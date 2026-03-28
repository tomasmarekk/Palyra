use std::{
    collections::{HashMap, VecDeque},
    fs,
    path::{Path, PathBuf},
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Digest;
use tokio::sync::oneshot;
use tonic::Status;
use ulid::Ulid;

use palyra_identity::{PairingClientKind, PairingMethod, PairingResult, VerifiedPairing};

const NODE_RUNTIME_STATE_FILE_NAME: &str = "node-runtime.v1.json";
const DEFAULT_PAIRING_CODE_TTL_MS: u64 = 10 * 60 * 1_000;
const MIN_PAIRING_CODE_TTL_MS: u64 = 30 * 1_000;
const MAX_PAIRING_CODE_TTL_MS: u64 = 24 * 60 * 60 * 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PairingCodeMethod {
    Pin,
    Qr,
}

impl PairingCodeMethod {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Pin => "pin",
            Self::Qr => "qr",
        }
    }

    pub(crate) fn to_pairing_method(self, code: String) -> PairingMethod {
        match self {
            Self::Pin => PairingMethod::Pin { code },
            Self::Qr => PairingMethod::Qr { token: code },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DevicePairingCodeRecord {
    pub(crate) code: String,
    pub(crate) method: PairingCodeMethod,
    pub(crate) issued_by: String,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) expires_at_unix_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DevicePairingRequestState {
    PendingApproval,
    Approved,
    Rejected,
    Completed,
    Expired,
}

impl DevicePairingRequestState {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::PendingApproval => "pending_approval",
            Self::Approved => "approved",
            Self::Rejected => "rejected",
            Self::Completed => "completed",
            Self::Expired => "expired",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DevicePairingMaterialRecord {
    pub(crate) identity_fingerprint: String,
    pub(crate) transcript_hash_hex: String,
    pub(crate) mtls_client_certificate_pem: String,
    pub(crate) mtls_client_private_key_pem: String,
    pub(crate) gateway_ca_certificate_pem: String,
    pub(crate) cert_expires_at_unix_ms: i64,
}

impl DevicePairingMaterialRecord {
    fn from_pairing_result(result: &PairingResult) -> Self {
        Self {
            identity_fingerprint: result.identity_fingerprint.clone(),
            transcript_hash_hex: result.transcript_hash_hex.clone(),
            mtls_client_certificate_pem: result.device.current_certificate.certificate_pem.clone(),
            mtls_client_private_key_pem: result.device.current_certificate.private_key_pem.clone(),
            gateway_ca_certificate_pem: result.gateway_ca_certificate_pem.clone(),
            cert_expires_at_unix_ms: i64::try_from(
                result.device.current_certificate.expires_at_unix_ms,
            )
            .unwrap_or(i64::MAX),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DevicePairingRequestRecord {
    pub(crate) request_id: String,
    pub(crate) session_id: String,
    pub(crate) device_id: String,
    pub(crate) client_kind: PairingClientKind,
    pub(crate) method: PairingCodeMethod,
    pub(crate) code_issued_by: String,
    pub(crate) requested_at_unix_ms: i64,
    pub(crate) expires_at_unix_ms: i64,
    pub(crate) approval_id: String,
    pub(crate) state: DevicePairingRequestState,
    pub(crate) decision_reason: Option<String>,
    pub(crate) decision_scope_ttl_ms: Option<i64>,
    pub(crate) verified_pairing: VerifiedPairing,
    pub(crate) material: Option<DevicePairingMaterialRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DeviceCapabilityView {
    pub(crate) name: String,
    pub(crate) available: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RegisteredNodeRecord {
    pub(crate) device_id: String,
    pub(crate) platform: String,
    pub(crate) capabilities: Vec<DeviceCapabilityView>,
    pub(crate) registered_at_unix_ms: i64,
    pub(crate) last_seen_at_unix_ms: i64,
    pub(crate) last_event_name: Option<String>,
    pub(crate) last_event_at_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PersistedNodeRuntimeState {
    #[serde(default)]
    active_pairing_codes: HashMap<String, DevicePairingCodeRecord>,
    #[serde(default)]
    pairing_requests: HashMap<String, DevicePairingRequestRecord>,
    #[serde(default)]
    nodes: HashMap<String, RegisteredNodeRecord>,
}

#[derive(Debug, Clone)]
pub(crate) struct CapabilityDispatchRecord {
    pub(crate) request_id: String,
    pub(crate) capability: String,
    pub(crate) input_json: Vec<u8>,
    pub(crate) max_payload_bytes: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct CapabilityExecutionResult {
    pub(crate) success: bool,
    pub(crate) output_json: Vec<u8>,
    pub(crate) error: String,
}

#[derive(Default)]
struct CapabilityRuntimeState {
    queued_by_device: HashMap<String, VecDeque<CapabilityDispatchRecord>>,
    inflight_by_request_id: HashMap<String, CapabilityDispatchRecord>,
    waiters_by_request_id: HashMap<String, oneshot::Sender<CapabilityExecutionResult>>,
}

#[derive(Default)]
struct ReservedPairingCodeState {
    by_session_id: HashMap<String, DevicePairingCodeRecord>,
}

pub(crate) struct NodeRuntimeState {
    state_path: PathBuf,
    persisted: Mutex<PersistedNodeRuntimeState>,
    reserved_codes: Mutex<ReservedPairingCodeState>,
    capabilities: Mutex<CapabilityRuntimeState>,
}

impl std::fmt::Debug for NodeRuntimeState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("NodeRuntimeState").field("state_path", &self.state_path).finish()
    }
}

impl NodeRuntimeState {
    pub(crate) fn load(state_root: &Path) -> Result<Self> {
        let state_path = state_root.join(NODE_RUNTIME_STATE_FILE_NAME);
        if let Some(parent) = state_path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("failed to create node runtime state dir {}", parent.display())
            })?;
        }
        let persisted = if state_path.is_file() {
            let raw = fs::read(&state_path).with_context(|| {
                format!("failed to read node runtime state {}", state_path.display())
            })?;
            serde_json::from_slice::<PersistedNodeRuntimeState>(raw.as_slice()).with_context(
                || format!("failed to parse node runtime state {}", state_path.display()),
            )?
        } else {
            PersistedNodeRuntimeState::default()
        };
        Ok(Self {
            state_path,
            persisted: Mutex::new(persisted),
            reserved_codes: Mutex::new(ReservedPairingCodeState::default()),
            capabilities: Mutex::new(CapabilityRuntimeState::default()),
        })
    }

    pub(crate) fn mint_pairing_code(
        &self,
        method: PairingCodeMethod,
        issued_by: &str,
        ttl_ms: Option<u64>,
    ) -> Result<DevicePairingCodeRecord, Status> {
        let now = current_unix_ms()?;
        let ttl_ms = normalize_pairing_code_ttl_ms(ttl_ms);
        let mut persisted = lock_mutex(&self.persisted, "node runtime state")?;
        prune_persisted_state(&mut persisted, now);
        let record = DevicePairingCodeRecord {
            code: generate_pairing_code(method),
            method,
            issued_by: issued_by.trim().to_owned(),
            created_at_unix_ms: now,
            expires_at_unix_ms: now.saturating_add(i64::try_from(ttl_ms).unwrap_or(i64::MAX)),
        };
        persisted.active_pairing_codes.insert(record.code.clone(), record.clone());
        self.persist_locked(&persisted)?;
        Ok(record)
    }

    pub(crate) fn pairing_codes(&self) -> Result<Vec<DevicePairingCodeRecord>, Status> {
        let now = current_unix_ms()?;
        let mut persisted = lock_mutex(&self.persisted, "node runtime state")?;
        prune_persisted_state(&mut persisted, now);
        self.persist_locked(&persisted)?;
        let mut records = persisted.active_pairing_codes.values().cloned().collect::<Vec<_>>();
        records.sort_by(|left, right| {
            left.created_at_unix_ms
                .cmp(&right.created_at_unix_ms)
                .then_with(|| left.code.cmp(&right.code))
        });
        Ok(records)
    }

    pub(crate) fn reserve_pairing_code(
        &self,
        method: PairingCodeMethod,
        code: &str,
    ) -> Result<DevicePairingCodeRecord, Status> {
        let now = current_unix_ms()?;
        let mut persisted = lock_mutex(&self.persisted, "node runtime state")?;
        prune_persisted_state(&mut persisted, now);
        let Some(record) = persisted.active_pairing_codes.remove(code) else {
            return Err(Status::failed_precondition("pairing code is missing or expired"));
        };
        if record.method != method {
            persisted.active_pairing_codes.insert(record.code.clone(), record);
            return Err(Status::failed_precondition("pairing code method does not match request"));
        }
        self.persist_locked(&persisted)?;
        Ok(record)
    }

    pub(crate) fn restore_pairing_code(
        &self,
        record: DevicePairingCodeRecord,
    ) -> Result<(), Status> {
        let now = current_unix_ms()?;
        if record.expires_at_unix_ms <= now {
            return Ok(());
        }
        let mut persisted = lock_mutex(&self.persisted, "node runtime state")?;
        persisted.active_pairing_codes.insert(record.code.clone(), record);
        self.persist_locked(&persisted)
    }

    pub(crate) fn bind_reserved_pairing_code(
        &self,
        session_id: &str,
        record: DevicePairingCodeRecord,
    ) -> Result<(), Status> {
        let mut reserved = lock_mutex(&self.reserved_codes, "reserved pairing code state")?;
        reserved.by_session_id.insert(session_id.to_owned(), record);
        Ok(())
    }

    pub(crate) fn take_reserved_pairing_code(
        &self,
        session_id: &str,
    ) -> Result<Option<DevicePairingCodeRecord>, Status> {
        let mut reserved = lock_mutex(&self.reserved_codes, "reserved pairing code state")?;
        Ok(reserved.by_session_id.remove(session_id))
    }

    pub(crate) fn create_pairing_request(
        &self,
        session_id: &str,
        verified: VerifiedPairing,
        code: DevicePairingCodeRecord,
        approval_id: &str,
    ) -> Result<DevicePairingRequestRecord, Status> {
        let now = current_unix_ms()?;
        let record = DevicePairingRequestRecord {
            request_id: session_id.to_owned(),
            session_id: session_id.to_owned(),
            device_id: verified.device_id.clone(),
            client_kind: verified.client_kind,
            method: code.method,
            code_issued_by: code.issued_by.clone(),
            requested_at_unix_ms: now,
            expires_at_unix_ms: code.expires_at_unix_ms,
            approval_id: approval_id.to_owned(),
            state: DevicePairingRequestState::PendingApproval,
            decision_reason: None,
            decision_scope_ttl_ms: None,
            verified_pairing: verified,
            material: None,
        };
        let mut persisted = lock_mutex(&self.persisted, "node runtime state")?;
        persisted.pairing_requests.insert(record.request_id.clone(), record.clone());
        self.persist_locked(&persisted)?;
        Ok(record)
    }

    pub(crate) fn pairing_requests(&self) -> Result<Vec<DevicePairingRequestRecord>, Status> {
        let now = current_unix_ms()?;
        let mut persisted = lock_mutex(&self.persisted, "node runtime state")?;
        prune_persisted_state(&mut persisted, now);
        self.persist_locked(&persisted)?;
        let mut records = persisted.pairing_requests.values().cloned().collect::<Vec<_>>();
        records.sort_by(|left, right| {
            right
                .requested_at_unix_ms
                .cmp(&left.requested_at_unix_ms)
                .then_with(|| left.request_id.cmp(&right.request_id))
        });
        Ok(records)
    }

    pub(crate) fn pairing_request(
        &self,
        request_id: &str,
    ) -> Result<Option<DevicePairingRequestRecord>, Status> {
        let now = current_unix_ms()?;
        let mut persisted = lock_mutex(&self.persisted, "node runtime state")?;
        prune_persisted_state(&mut persisted, now);
        self.persist_locked(&persisted)?;
        Ok(persisted.pairing_requests.get(request_id).cloned())
    }

    pub(crate) fn apply_pairing_approval(
        &self,
        approval_id: &str,
        approved: bool,
        reason: &str,
        decision_scope_ttl_ms: Option<i64>,
    ) -> Result<Option<DevicePairingRequestRecord>, Status> {
        let now = current_unix_ms()?;
        let mut persisted = lock_mutex(&self.persisted, "node runtime state")?;
        prune_persisted_state(&mut persisted, now);
        let request = persisted
            .pairing_requests
            .values_mut()
            .find(|record| record.approval_id == approval_id);
        let Some(request) = request else {
            self.persist_locked(&persisted)?;
            return Ok(None);
        };
        request.state = if approved {
            DevicePairingRequestState::Approved
        } else {
            DevicePairingRequestState::Rejected
        };
        request.decision_reason = Some(reason.to_owned());
        request.decision_scope_ttl_ms = decision_scope_ttl_ms;
        let updated = request.clone();
        self.persist_locked(&persisted)?;
        Ok(Some(updated))
    }

    pub(crate) fn complete_pairing_request(
        &self,
        request_id: &str,
        result: &PairingResult,
    ) -> Result<Option<DevicePairingRequestRecord>, Status> {
        let now = current_unix_ms()?;
        let mut persisted = lock_mutex(&self.persisted, "node runtime state")?;
        prune_persisted_state(&mut persisted, now);
        let Some(request) = persisted.pairing_requests.get_mut(request_id) else {
            self.persist_locked(&persisted)?;
            return Ok(None);
        };
        request.state = DevicePairingRequestState::Completed;
        request.material = Some(DevicePairingMaterialRecord::from_pairing_result(result));
        let updated = request.clone();
        self.persist_locked(&persisted)?;
        Ok(Some(updated))
    }

    pub(crate) fn register_node(
        &self,
        device_id: &str,
        platform: &str,
        capabilities: Vec<DeviceCapabilityView>,
    ) -> Result<RegisteredNodeRecord, Status> {
        let now = current_unix_ms()?;
        let mut persisted = lock_mutex(&self.persisted, "node runtime state")?;
        prune_persisted_state(&mut persisted, now);
        let record =
            persisted.nodes.entry(device_id.to_owned()).or_insert_with(|| RegisteredNodeRecord {
                device_id: device_id.to_owned(),
                platform: platform.to_owned(),
                capabilities: capabilities.clone(),
                registered_at_unix_ms: now,
                last_seen_at_unix_ms: now,
                last_event_name: None,
                last_event_at_unix_ms: None,
            });
        record.platform = platform.to_owned();
        record.capabilities = capabilities;
        record.last_seen_at_unix_ms = now;
        let updated = record.clone();
        self.persist_locked(&persisted)?;
        Ok(updated)
    }

    pub(crate) fn touch_node_event(
        &self,
        device_id: &str,
        event_name: &str,
    ) -> Result<Option<RegisteredNodeRecord>, Status> {
        let now = current_unix_ms()?;
        let mut persisted = lock_mutex(&self.persisted, "node runtime state")?;
        let Some(record) = persisted.nodes.get_mut(device_id) else {
            return Ok(None);
        };
        record.last_seen_at_unix_ms = now;
        record.last_event_name = Some(event_name.to_owned());
        record.last_event_at_unix_ms = Some(now);
        let updated = record.clone();
        self.persist_locked(&persisted)?;
        Ok(Some(updated))
    }

    pub(crate) fn nodes(&self) -> Result<Vec<RegisteredNodeRecord>, Status> {
        let persisted = lock_mutex(&self.persisted, "node runtime state")?;
        let mut nodes = persisted.nodes.values().cloned().collect::<Vec<_>>();
        nodes.sort_by(|left, right| left.device_id.cmp(&right.device_id));
        Ok(nodes)
    }

    pub(crate) fn node(&self, device_id: &str) -> Result<Option<RegisteredNodeRecord>, Status> {
        let persisted = lock_mutex(&self.persisted, "node runtime state")?;
        Ok(persisted.nodes.get(device_id).cloned())
    }

    pub(crate) fn remove_node(&self, device_id: &str) -> Result<bool, Status> {
        let mut persisted = lock_mutex(&self.persisted, "node runtime state")?;
        let removed = persisted.nodes.remove(device_id).is_some();
        if removed {
            self.persist_locked(&persisted)?;
        }
        Ok(removed)
    }

    pub(crate) fn enqueue_capability_request(
        &self,
        device_id: &str,
        capability: &str,
        input_json: Vec<u8>,
        max_payload_bytes: u64,
        _timeout_ms: Option<u64>,
    ) -> Result<(String, oneshot::Receiver<CapabilityExecutionResult>), Status> {
        let request_id = Ulid::new().to_string();
        let dispatch = CapabilityDispatchRecord {
            request_id: request_id.clone(),
            capability: capability.to_owned(),
            input_json,
            max_payload_bytes,
        };
        let (sender, receiver) = oneshot::channel();
        let mut capabilities = lock_mutex(&self.capabilities, "node capability runtime")?;
        capabilities.queued_by_device.entry(device_id.to_owned()).or_default().push_back(dispatch);
        capabilities.waiters_by_request_id.insert(request_id.clone(), sender);
        Ok((request_id, receiver))
    }

    pub(crate) fn next_capability_dispatch(
        &self,
        device_id: &str,
    ) -> Result<Option<CapabilityDispatchRecord>, Status> {
        let mut capabilities = lock_mutex(&self.capabilities, "node capability runtime")?;
        let Some(queue) = capabilities.queued_by_device.get_mut(device_id) else {
            return Ok(None);
        };
        let Some(dispatch) = queue.pop_front() else {
            return Ok(None);
        };
        capabilities.inflight_by_request_id.insert(dispatch.request_id.clone(), dispatch.clone());
        Ok(Some(dispatch))
    }

    pub(crate) fn complete_capability_request(
        &self,
        request_id: &str,
        result: CapabilityExecutionResult,
    ) -> Result<bool, Status> {
        let mut capabilities = lock_mutex(&self.capabilities, "node capability runtime")?;
        capabilities.inflight_by_request_id.remove(request_id);
        let Some(waiter) = capabilities.waiters_by_request_id.remove(request_id) else {
            return Ok(false);
        };
        let _ = waiter.send(result);
        Ok(true)
    }

    fn persist_locked(&self, persisted: &PersistedNodeRuntimeState) -> Result<(), Status> {
        let encoded = serde_json::to_vec_pretty(persisted).map_err(|error| {
            Status::internal(format!("failed to encode node runtime state: {error}"))
        })?;
        fs::write(&self.state_path, encoded).map_err(|error| {
            Status::internal(format!(
                "failed to write node runtime state {}: {error}",
                self.state_path.display()
            ))
        })
    }
}

fn generate_pairing_code(method: PairingCodeMethod) -> String {
    match method {
        PairingCodeMethod::Pin => {
            let digest = sha2::Sha256::digest(Ulid::new().to_string().as_bytes());
            let value =
                u32::from_be_bytes([digest[0], digest[1], digest[2], digest[3]]) % 1_000_000;
            format!("{value:06}")
        }
        PairingCodeMethod::Qr => Ulid::new().to_string(),
    }
}

fn normalize_pairing_code_ttl_ms(value: Option<u64>) -> u64 {
    value
        .unwrap_or(DEFAULT_PAIRING_CODE_TTL_MS)
        .clamp(MIN_PAIRING_CODE_TTL_MS, MAX_PAIRING_CODE_TTL_MS)
}

fn prune_persisted_state(state: &mut PersistedNodeRuntimeState, now_unix_ms: i64) {
    state.active_pairing_codes.retain(|_, record| record.expires_at_unix_ms > now_unix_ms);
    for request in state.pairing_requests.values_mut() {
        if request.expires_at_unix_ms <= now_unix_ms
            && matches!(
                request.state,
                DevicePairingRequestState::PendingApproval | DevicePairingRequestState::Approved
            )
        {
            request.state = DevicePairingRequestState::Expired;
            if request.decision_reason.is_none() {
                request.decision_reason = Some("pairing request expired".to_owned());
            }
        }
    }
}

fn lock_mutex<'a, T>(
    mutex: &'a Mutex<T>,
    label: &str,
) -> Result<std::sync::MutexGuard<'a, T>, Status> {
    mutex.lock().map_err(|_| Status::internal(format!("{label} lock poisoned")))
}

pub(crate) fn current_unix_ms() -> Result<i64, Status> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| Status::internal(format!("system clock error: {error}")))?;
    i64::try_from(duration.as_millis()).map_err(|_| Status::internal("timestamp overflow"))
}

pub(crate) fn parse_capability_result_payload(
    payload_json: &[u8],
) -> Result<(String, CapabilityExecutionResult), Status> {
    let value: Value = serde_json::from_slice(payload_json).map_err(|error| {
        Status::invalid_argument(format!("invalid capability result payload: {error}"))
    })?;
    let request_id = value
        .get("request_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Status::invalid_argument("capability result payload missing request_id"))?
        .to_owned();
    let success = value.get("success").and_then(Value::as_bool).unwrap_or(false);
    let error = value.get("error").and_then(Value::as_str).unwrap_or_default().to_owned();
    let output_json = value
        .get("output_json")
        .map(|inner| serde_json::to_vec(inner).unwrap_or_default())
        .unwrap_or_default();
    Ok((request_id, CapabilityExecutionResult { success, output_json, error }))
}
