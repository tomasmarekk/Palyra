//! Persistent node runtime state: pairing codes, device pairing requests,
//! registered nodes, and the per-device capability dispatch queue.
//!
//! [`NodeRuntimeState`] is the single owner of `node-runtime.v1.json` under
//! the daemon state root; every mutation is written back synchronously so a
//! daemon restart cannot resurrect consumed pairing codes or lose request
//! states. Volatile coordination (reserved codes mid-handshake, queued
//! dispatches, bounded result channels) deliberately lives only in memory.
//! Consumed by `node_rpc` (gRPC surface) and the realtime `command_router`.
//! Summaries persisted from payloads are redacted before storage.

use std::{
    collections::{HashMap, VecDeque},
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use palyra_common::{
    redaction::{redact_auth_error, redact_url_segments_in_text},
    runtime_contracts::RuntimeGeneration,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Digest;
use tokio::sync::Notify;
use tonic::Status;
use ulid::Ulid;

use palyra_identity::{PairingClientKind, PairingMethod, PairingResult, VerifiedPairing};
use palyra_workerd::{WorkerRemoteToolRequestEnvelope, WorkerRemoteToolResultEnvelope};

use crate::journal::{
    NetworkedWorkerDeliveryReservationOutcome, NetworkedWorkerDeliveryReservationRequest,
    NetworkedWorkerDispatchAbortBeforeReleaseOutcome, NetworkedWorkerDispatchCancelOutcome,
    NetworkedWorkerDispatchClaim, NetworkedWorkerPayloadAcknowledgementOutcome,
    NetworkedWorkerPayloadAcknowledgementRequest, NetworkedWorkerPayloadReleaseOutcome,
    NetworkedWorkerPayloadReleaseRequest,
};

const NODE_RUNTIME_STATE_FILE_NAME: &str = "node-runtime.v1.json";
const DEFAULT_PAIRING_CODE_TTL_MS: u64 = 10 * 60 * 1_000;
const MIN_PAIRING_CODE_TTL_MS: u64 = 30 * 1_000;
const MAX_PAIRING_CODE_TTL_MS: u64 = 24 * 60 * 60 * 1_000;
pub(crate) const NETWORKED_WORKER_DELIVERY_FENCE_CAPABILITY: &str =
    "protocol:palyra.networked_worker.delivery_fence.v2";
pub(crate) const NETWORKED_WORKER_DELIVERY_FENCE_PROTOCOL: &str =
    "palyra.networked_worker.delivery_fence.v2";
const NETWORKED_WORKER_DELIVERY_TOKEN_BYTES: usize = 32;

/// How a pairing code is presented to the device being paired.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PairingCodeMethod {
    Pin,
    Qr,
}

impl PairingCodeMethod {
    /// Stable lowercase label used in journal payloads and RPC responses.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Pin => "pin",
            Self::Qr => "qr",
        }
    }

    /// Wraps the raw `code` in the matching `palyra-identity` pairing method.
    pub(crate) fn to_pairing_method(self, code: String) -> PairingMethod {
        match self {
            Self::Pin => PairingMethod::Pin { code },
            Self::Qr => PairingMethod::Qr { token: code },
        }
    }
}

/// A single-use pairing code minted by an operator; expires at
/// `expires_at_unix_ms` and is consumed by [`NodeRuntimeState::reserve_pairing_code`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DevicePairingCodeRecord {
    pub(crate) code: String,
    pub(crate) method: PairingCodeMethod,
    pub(crate) issued_by: String,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) expires_at_unix_ms: i64,
}

/// Lifecycle of a verified pairing request awaiting and following operator
/// approval; only `PendingApproval` and `Approved` can still expire.
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
    /// Stable snake_case label exposed through RPC status responses.
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

/// Certificate material handed to a device after pairing completes.
///
/// Invariant: `mtls_client_private_key_pem` is `skip_serializing` so the
/// private key is never written into node runtime state. It deserializes only
/// to migrate legacy records into sealed identity storage (see
/// `node_rpc::resolve_pairing_private_key`); a unit test pins this.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DevicePairingMaterialRecord {
    pub(crate) identity_fingerprint: String,
    pub(crate) transcript_hash_hex: String,
    pub(crate) mtls_client_certificate_pem: String,
    #[serde(default, skip_serializing)]
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
            mtls_client_private_key_pem: String::new(),
            gateway_ca_certificate_pem: result.gateway_ca_certificate_pem.clone(),
            cert_expires_at_unix_ms: i64::try_from(
                result.device.current_certificate.expires_at_unix_ms,
            )
            .unwrap_or(i64::MAX),
        }
    }
}

/// Persisted pairing request keyed by `request_id` (the pairing session id),
/// carrying the verified handshake plus the operator decision trail.
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

/// One advertised node capability and whether it is currently available.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DeviceCapabilityView {
    pub(crate) name: String,
    pub(crate) available: bool,
}

/// A node that has registered with the daemon, including its capability set
/// and last-seen/last-event presence data.
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
    #[serde(default)]
    capability_requests: HashMap<String, CapabilityRequestRecord>,
}

/// Work item handed to a node over the event stream; the raw `input_json`
/// stays in memory only (the persisted record keeps a redacted summary).
#[derive(Debug, Clone)]
pub(crate) struct CapabilityDispatchRecord {
    pub(crate) request_id: String,
    pub(crate) capability: String,
    pub(crate) input_json: Vec<u8>,
    pub(crate) max_payload_bytes: u64,
    pub(crate) networked_worker_reservation: Option<NetworkedWorkerDeliveryReservation>,
    networked_worker_result_commit_context: Option<NetworkedWorkerResultCommitContext>,
    authority: CapabilityDispatchAuthority,
}

/// Metadata-only reservation emitted before a networked worker may fetch its raw payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NetworkedWorkerDeliveryReservation {
    pub(crate) request_id: String,
    pub(crate) delivery_attempt_id: String,
    pub(crate) fetch_token: String,
    pub(crate) request_sha256: String,
    pub(crate) worker_id: String,
    pub(crate) lease_id: String,
    pub(crate) run_id: String,
    pub(crate) fleet_generation: u64,
    pub(crate) run_generation: RuntimeGeneration,
    pub(crate) expires_at_unix_ms: i64,
}

/// Raw payload returned only after the exact durable release transaction commits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NetworkedWorkerFetchedPayload {
    pub(crate) request_id: String,
    pub(crate) delivery_attempt_id: String,
    pub(crate) input_json: Vec<u8>,
    pub(crate) max_payload_bytes: u64,
    pub(crate) request_sha256: String,
}

/// Durable authority required before a queued networked-worker payload may leave the daemon.
#[derive(Debug, Clone, PartialEq, Eq)]
enum CapabilityDispatchAuthority {
    Generic,
    NetworkedWorker {
        remote_request_id: String,
        request_sha256: String,
        lease_id: String,
        session_id: String,
        run_id: String,
        run_generation: RuntimeGeneration,
        lease_expires_at_unix_ms: i64,
    },
}

/// Payload-redacted authority marker persisted in the node request audit ledger.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum CapabilityRequestAuthorityRecord {
    #[default]
    Generic,
    NetworkedWorker {
        remote_request_id: String,
    },
}

/// Whether a node-returned result matches active durable networked-worker authority.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NetworkedWorkerResultAuthorizationOutcome {
    Authorized,
    Rejected,
}

/// Host-owned attribution retained beside a raw remote-worker request, never sent on the wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NetworkedWorkerHostReceiptContext {
    pub(crate) principal: String,
    pub(crate) device_id: String,
    pub(crate) channel: Option<String>,
}

/// Volatile callback authority that binds a result to its original request and host attribution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NetworkedWorkerResultCommitContext {
    pub(crate) request: WorkerRemoteToolRequestEnvelope,
    pub(crate) host: NetworkedWorkerHostReceiptContext,
}

/// Fully parsed callback supplied to the durable worker-result commit boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NetworkedWorkerResultCommitRequest {
    pub(crate) context: NetworkedWorkerResultCommitContext,
    pub(crate) result: WorkerRemoteToolResultEnvelope,
    pub(crate) node_request_id: String,
    pub(crate) delivery_attempt_id: String,
    pub(crate) reporting_worker_id: String,
    pub(crate) callback_run_generation: RuntimeGeneration,
    pub(crate) observed_at_unix_ms: i64,
}

/// Durable disposition returned before Node state may project a worker callback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NetworkedWorkerResultCommitDisposition {
    ActiveCompletion,
    LateReconciliation,
    ExactReplay,
}

/// Result of the generation-fenced durable worker callback boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum NetworkedWorkerResultCommitOutcome {
    Committed {
        disposition: NetworkedWorkerResultCommitDisposition,
        canonical_observed_at_unix_ms: i64,
        validated_result_sha256: String,
    },
    StaleSuppressed,
    Rejected,
}

/// Boundary used by the node queue to cross durable networked-worker dispatch authority.
pub(crate) trait CapabilityDispatchAuthorizer: Send + Sync {
    /// Reserves an exact metadata-only delivery attempt for the queued claim.
    ///
    /// # Errors
    /// Returns a gRPC status when durable authority cannot be checked or updated.
    fn reserve_networked_worker_delivery(
        &self,
        request: &NetworkedWorkerDeliveryReservationRequest,
    ) -> Result<NetworkedWorkerDeliveryReservationOutcome, Status>;

    /// Commits the exact one-time payload-release boundary before bytes leave the daemon.
    ///
    /// # Errors
    /// Returns a gRPC status when durable authority cannot be checked or updated.
    fn release_networked_worker_payload(
        &self,
        request: &NetworkedWorkerPayloadReleaseRequest,
    ) -> Result<NetworkedWorkerPayloadReleaseOutcome, Status>;

    /// Records one exact payload acknowledgement idempotently.
    ///
    /// # Errors
    /// Returns a gRPC status when durable evidence cannot be checked or updated.
    fn acknowledge_networked_worker_payload(
        &self,
        request: &NetworkedWorkerPayloadAcknowledgementRequest,
    ) -> Result<NetworkedWorkerPayloadAcknowledgementOutcome, Status>;

    /// Rolls back an exact dispatch whose payload has not left the daemon.
    ///
    /// # Errors
    /// Returns a gRPC status when durable evidence cannot be checked or committed.
    fn abort_networked_worker_dispatch_before_payload_release(
        &self,
        remote_request_id: &str,
        node_request_id: &str,
        request_sha256: &str,
        dispatch_fleet_generation: u64,
        observed_at_unix_ms: i64,
    ) -> Result<NetworkedWorkerDispatchAbortBeforeReleaseOutcome, Status>;

    /// Cancels the exact durable claim while it remains queued.
    ///
    /// # Errors
    /// Returns a gRPC status when durable claim state cannot be checked or updated.
    fn cancel_networked_worker_dispatch(
        &self,
        remote_request_id: &str,
        node_request_id: &str,
        reason_code: &str,
        observed_at_unix_ms: i64,
    ) -> Result<NetworkedWorkerDispatchCancelOutcome, Status>;

    /// Verifies that an authenticated worker owns the exact released attempt returning this result.
    ///
    /// # Errors
    /// Returns a gRPC status when durable claim authority cannot be checked.
    #[cfg(test)]
    fn authorize_networked_worker_result(
        &self,
        remote_request_id: &str,
        node_request_id: &str,
        delivery_attempt_id: &str,
        run_generation: RuntimeGeneration,
        reporting_worker_id: &str,
        observed_at_unix_ms: i64,
    ) -> Result<NetworkedWorkerResultAuthorizationOutcome, Status>;

    /// Validates and durably settles an exact worker result before Node state is mutated.
    ///
    /// # Errors
    /// Returns a gRPC status when callback evidence is malformed or durable state cannot commit.
    fn commit_networked_worker_result(
        &self,
        request: &NetworkedWorkerResultCommitRequest,
    ) -> Result<NetworkedWorkerResultCommitOutcome, Status>;
}

/// Result a node reports back for a dispatched capability request.
#[derive(Debug, Clone)]
pub(crate) struct CapabilityExecutionResult {
    pub(crate) success: bool,
    pub(crate) output_json: Vec<u8>,
    pub(crate) error: String,
}

/// One authenticated result notification plus host-owned receipt evidence.
#[derive(Debug, Clone)]
pub(crate) struct CapabilityExecutionNotification {
    pub(crate) result: CapabilityExecutionResult,
    pub(crate) delivery_attempt_id: Option<String>,
    pub(crate) run_generation: Option<RuntimeGeneration>,
    pub(crate) networked_worker_commit_disposition: Option<NetworkedWorkerResultCommitDisposition>,
    pub(crate) observed_at_unix_ms: i64,
}

#[derive(Debug, Default)]
struct CapabilityExecutionSlot {
    notification: Mutex<Option<CapabilityExecutionNotification>>,
    ready: Notify,
}

/// Single-consumer result handle backed by a runtime-owned one-item slot.
#[derive(Debug)]
pub(crate) struct CapabilityExecutionReceiver {
    slot: Arc<CapabilityExecutionSlot>,
}

impl CapabilityExecutionReceiver {
    /// Receives the authenticated result once it has been durably committed and published.
    pub(crate) async fn recv(&mut self) -> CapabilityExecutionNotification {
        loop {
            let notified = self.slot.ready.notified();
            if let Some(notification) =
                lock_mutex(&self.slot.notification, "node capability result notification")
                    .expect("capability result notification lock must remain usable")
                    .take()
            {
                return notification;
            }
            notified.await;
        }
    }

    /// Tries to receive the published result without waiting.
    #[cfg(test)]
    pub(crate) fn try_recv(&mut self) -> Option<CapabilityExecutionNotification> {
        lock_mutex(&self.slot.notification, "node capability result notification")
            .expect("capability result notification lock must remain usable")
            .take()
    }
}

/// Lifecycle of a capability request from enqueue to terminal outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CapabilityRequestState {
    Queued,
    Dispatched,
    AwaitingLocalMediation,
    Succeeded,
    Failed,
    TimedOut,
    Rejected,
    Cancelled,
}

/// Outcome of attempting to stop one capability request without recalling released work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CapabilityRequestStopOutcome {
    CancelledBeforeRelease,
    ReleasedReconciliationOwned,
    AlreadyTerminal,
    Missing,
}

/// Outcome of recording a caller deadline after work may already have completed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CapabilityRequestTimeoutOutcome {
    MarkedTimedOut,
    ResultCommitted,
    AlreadyTerminal,
    Missing,
}

/// Persisted, payload-redacted audit record for one capability request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CapabilityRequestRecord {
    pub(crate) request_id: String,
    pub(crate) device_id: String,
    pub(crate) capability: String,
    pub(crate) state: CapabilityRequestState,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) updated_at_unix_ms: i64,
    pub(crate) dispatched_at_unix_ms: Option<i64>,
    pub(crate) completed_at_unix_ms: Option<i64>,
    pub(crate) max_payload_bytes: u64,
    #[serde(default)]
    authority: CapabilityRequestAuthorityRecord,
    pub(crate) input_summary: Option<String>,
    pub(crate) output_summary: Option<String>,
    pub(crate) error: Option<String>,
}

// In-memory only: queues and result channels reference live process ownership and raw payload
// bytes, neither of which would survive (or should reach) disk.
#[derive(Debug, Clone, PartialEq, Eq)]
struct NetworkedWorkerInFlightMetadata {
    device_id: String,
    remote_request_id: String,
    delivery_attempt_id: String,
    run_generation: RuntimeGeneration,
    request_sha256: String,
    payload_released: bool,
    result_commit_context: Option<NetworkedWorkerResultCommitContext>,
    committed_observed_at_unix_ms: Option<i64>,
    committed_result_sha256: Option<String>,
}

#[derive(Default)]
struct CapabilityRuntimeState {
    queued_by_device: HashMap<String, VecDeque<CapabilityDispatchRecord>>,
    reserved_worker_payloads_by_request_id: HashMap<String, CapabilityDispatchRecord>,
    generic_inflight_by_request_id: HashMap<String, CapabilityDispatchRecord>,
    networked_worker_inflight_by_request_id: HashMap<String, NetworkedWorkerInFlightMetadata>,
    result_slots_by_request_id: HashMap<String, Arc<CapabilityExecutionSlot>>,
}

#[derive(Default)]
struct ReservedPairingCodeState {
    by_session_id: HashMap<String, DevicePairingCodeRecord>,
}

/// Thread-safe owner of node runtime state.
///
/// Persistent data (`persisted`) is flushed to `node-runtime.v1.json` on every
/// mutation; `reserved_codes` and `capabilities` are volatile coordination
/// state. Methods that must atomically cancel a queued request take
/// `capabilities` before `persisted`; no method may acquire these locks in the
/// reverse order.
pub(crate) struct NodeRuntimeState {
    state_root: PathBuf,
    persisted: Mutex<PersistedNodeRuntimeState>,
    reserved_codes: Mutex<ReservedPairingCodeState>,
    capabilities: Mutex<CapabilityRuntimeState>,
    #[cfg(test)]
    fail_next_persist: std::sync::atomic::AtomicBool,
}

impl std::fmt::Debug for NodeRuntimeState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("NodeRuntimeState").field("state_root", &self.state_root).finish()
    }
}

impl NodeRuntimeState {
    /// Loads (or initializes) node runtime state under `state_root`.
    ///
    /// # Errors
    /// Fails when the state root is empty or cannot be created/canonicalized,
    /// or when an existing state file cannot be read or parsed.
    pub(crate) fn load(state_root: &Path) -> Result<Self> {
        let state_root = resolve_canonical_state_root(state_root)?;
        let state_path = state_root.join(NODE_RUNTIME_STATE_FILE_NAME);
        let persisted = if state_path.as_path().is_file() {
            let raw = fs::read(state_path.as_path()).with_context(|| {
                format!("failed to read node runtime state {}", state_path.as_path().display())
            })?;
            serde_json::from_slice::<PersistedNodeRuntimeState>(raw.as_slice()).with_context(
                || format!("failed to parse node runtime state {}", state_path.as_path().display()),
            )?
        } else {
            PersistedNodeRuntimeState::default()
        };
        Ok(Self {
            state_root,
            persisted: Mutex::new(persisted),
            reserved_codes: Mutex::new(ReservedPairingCodeState::default()),
            capabilities: Mutex::new(CapabilityRuntimeState::default()),
            #[cfg(test)]
            fail_next_persist: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Mints and persists a new single-use pairing code; `ttl_ms` is clamped
    /// to the supported window (30s..=24h, default 10min).
    ///
    /// # Errors
    /// Returns `Status::internal` on clock, lock, or persistence failure.
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

    /// Lists active (unexpired, unconsumed) pairing codes, oldest first.
    ///
    /// # Errors
    /// Returns `Status::internal` on clock, lock, or persistence failure.
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

    /// Atomically consumes an active pairing code so two concurrent pairing
    /// sessions can never share one code; use [`Self::restore_pairing_code`]
    /// to put it back if the session fails to start.
    ///
    /// # Errors
    /// Returns `Status::failed_precondition` when the code is missing,
    /// expired, or was minted for a different method; `Status::internal` on
    /// clock, lock, or persistence failure.
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

    /// Returns a reserved code to the active pool after a failed session
    /// start; silently drops it when it has already expired.
    ///
    /// # Errors
    /// Returns `Status::internal` on clock, lock, or persistence failure.
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

    /// Associates a reserved code with a pairing session so the completion
    /// RPC can recover it (in memory only; a restart aborts the handshake).
    ///
    /// # Errors
    /// Returns `Status::internal` when the reserved-code lock is poisoned.
    pub(crate) fn bind_reserved_pairing_code(
        &self,
        session_id: &str,
        record: DevicePairingCodeRecord,
    ) -> Result<(), Status> {
        let mut reserved = lock_mutex(&self.reserved_codes, "reserved pairing code state")?;
        reserved.by_session_id.insert(session_id.to_owned(), record);
        Ok(())
    }

    /// Removes and returns the code bound to `session_id`, if any.
    ///
    /// # Errors
    /// Returns `Status::internal` when the reserved-code lock is poisoned.
    pub(crate) fn take_reserved_pairing_code(
        &self,
        session_id: &str,
    ) -> Result<Option<DevicePairingCodeRecord>, Status> {
        let mut reserved = lock_mutex(&self.reserved_codes, "reserved pairing code state")?;
        Ok(reserved.by_session_id.remove(session_id))
    }

    /// Persists a verified pairing handshake as a `PendingApproval` request
    /// that inherits the consumed code's expiry deadline.
    ///
    /// # Errors
    /// Returns `Status::internal` on clock, lock, or persistence failure.
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

    /// Lists pairing requests, newest first; expired ones are marked (not
    /// removed) so the decision trail stays auditable.
    ///
    /// # Errors
    /// Returns `Status::internal` on clock, lock, or persistence failure.
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

    /// Returns the pairing request with `request_id` after expiry pruning.
    ///
    /// # Errors
    /// Returns `Status::internal` on clock, lock, or persistence failure.
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

    /// Applies an operator approval decision to the pairing request bound to
    /// `approval_id`; returns `None` when no live request references it
    /// (already expired or never created).
    ///
    /// # Errors
    /// Returns `Status::internal` on clock, lock, or persistence failure.
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

    /// Marks an approved request `Completed` and attaches the issued
    /// certificate material (private key excluded; it is sealed separately).
    ///
    /// # Errors
    /// Returns `Status::internal` on clock, lock, or persistence failure.
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

    /// Registers (or re-registers) a node; platform and capability set are
    /// replaced wholesale and presence is refreshed.
    ///
    /// # Errors
    /// Returns `Status::internal` on clock, lock, or persistence failure.
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

    /// Updates node presence from an inbound event; returns `None` for nodes
    /// that never registered.
    ///
    /// # Errors
    /// Returns `Status::internal` on clock, lock, or persistence failure.
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

    /// Lists registered nodes ordered by device id.
    ///
    /// # Errors
    /// Returns `Status::internal` when the state lock is poisoned.
    pub(crate) fn nodes(&self) -> Result<Vec<RegisteredNodeRecord>, Status> {
        let persisted = lock_mutex(&self.persisted, "node runtime state")?;
        let mut nodes = persisted.nodes.values().cloned().collect::<Vec<_>>();
        nodes.sort_by(|left, right| left.device_id.cmp(&right.device_id));
        Ok(nodes)
    }

    /// Returns the registered node with `device_id`, if any.
    ///
    /// # Errors
    /// Returns `Status::internal` when the state lock is poisoned.
    pub(crate) fn node(&self, device_id: &str) -> Result<Option<RegisteredNodeRecord>, Status> {
        let persisted = lock_mutex(&self.persisted, "node runtime state")?;
        Ok(persisted.nodes.get(device_id).cloned())
    }

    /// Grants or revokes a single capability on a registered node, recording
    /// the change as the node's last event.
    ///
    /// # Errors
    /// Returns `Status::invalid_argument` for an empty capability name,
    /// `Status::not_found` for unknown nodes, and `Status::internal` on
    /// clock, lock, or persistence failure.
    pub(crate) fn set_node_capability_availability(
        &self,
        device_id: &str,
        capability: &str,
        available: bool,
    ) -> Result<RegisteredNodeRecord, Status> {
        let capability = capability.trim();
        if capability.is_empty() {
            return Err(Status::invalid_argument("node capability must not be empty"));
        }
        let now = current_unix_ms()?;
        let mut persisted = lock_mutex(&self.persisted, "node runtime state")?;
        let Some(record) = persisted.nodes.get_mut(device_id) else {
            return Err(Status::not_found(format!("node not found: {device_id}")));
        };
        if let Some(existing) =
            record.capabilities.iter_mut().find(|candidate| candidate.name == capability)
        {
            existing.available = available;
        } else {
            record
                .capabilities
                .push(DeviceCapabilityView { name: capability.to_owned(), available });
            record.capabilities.sort_by(|left, right| left.name.cmp(&right.name));
        }
        record.last_event_name = Some(if available {
            "capability_granted".to_owned()
        } else {
            "capability_revoked".to_owned()
        });
        record.last_event_at_unix_ms = Some(now);
        let updated = record.clone();
        self.persist_locked(&persisted)?;
        Ok(updated)
    }

    /// Removes a registered node; returns whether anything was removed.
    ///
    /// # Errors
    /// Returns `Status::internal` on lock or persistence failure.
    pub(crate) fn remove_node(&self, device_id: &str) -> Result<bool, Status> {
        let mut persisted = lock_mutex(&self.persisted, "node runtime state")?;
        let removed = persisted.nodes.remove(device_id).is_some();
        if removed {
            self.persist_locked(&persisted)?;
        }
        Ok(removed)
    }

    /// Queues a capability request for `device_id` and returns its id plus a
    /// bounded receiver that resolves when the node reports a result.
    ///
    /// `_timeout_ms` is accepted for API symmetry but deliberately unused:
    /// the deadline is enforced by the caller racing the receiver (see
    /// `node_rpc::execute_capability`), which keeps the queue free of timers.
    ///
    /// # Errors
    /// Returns `Status::internal` on clock, lock, or persistence failure.
    pub(crate) fn enqueue_capability_request(
        &self,
        device_id: &str,
        capability: &str,
        input_json: Vec<u8>,
        max_payload_bytes: u64,
        _timeout_ms: Option<u64>,
    ) -> Result<(String, CapabilityExecutionReceiver), Status> {
        let now = current_unix_ms()?;
        let request_id = Ulid::generate().to_string();
        let dispatch = CapabilityDispatchRecord {
            request_id: request_id.clone(),
            capability: capability.to_owned(),
            input_json: input_json.clone(),
            max_payload_bytes,
            networked_worker_reservation: None,
            networked_worker_result_commit_context: None,
            authority: CapabilityDispatchAuthority::Generic,
        };
        let slot = Arc::new(CapabilityExecutionSlot::default());
        let request = CapabilityRequestRecord {
            request_id: request_id.clone(),
            device_id: device_id.to_owned(),
            capability: capability.to_owned(),
            state: CapabilityRequestState::Queued,
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
            dispatched_at_unix_ms: None,
            completed_at_unix_ms: None,
            max_payload_bytes,
            authority: CapabilityRequestAuthorityRecord::Generic,
            input_summary: summarize_payload_bytes(input_json.as_slice()),
            output_summary: None,
            error: None,
        };
        {
            let mut persisted = lock_mutex(&self.persisted, "node runtime state")?;
            persisted.capability_requests.insert(request_id.clone(), request);
            self.persist_locked(&persisted)?;
        }
        let mut capabilities = lock_mutex(&self.capabilities, "node capability runtime")?;
        capabilities.queued_by_device.entry(device_id.to_owned()).or_default().push_back(dispatch);
        capabilities.result_slots_by_request_id.insert(request_id.clone(), Arc::clone(&slot));
        Ok((request_id, CapabilityExecutionReceiver { slot }))
    }

    /// Enqueues a networked-worker payload only after its exact durable claim exists.
    ///
    /// `claim.node_request_id` is caller-generated and shared by the durable claim and volatile
    /// queue record, preventing queue presence from becoming an independent authority source.
    ///
    /// # Errors
    /// Returns `Status::internal` on clock, lock, or persistence failure, or
    /// `Status::failed_precondition` when the claim metadata does not match the payload.
    pub(crate) fn enqueue_claimed_capability_request(
        &self,
        device_id: &str,
        capability: &str,
        input_json: Vec<u8>,
        max_payload_bytes: u64,
        claim: &NetworkedWorkerDispatchClaim,
        result_commit_context: NetworkedWorkerResultCommitContext,
    ) -> Result<CapabilityExecutionReceiver, Status> {
        self.enqueue_claimed_capability_request_inner(
            device_id,
            capability,
            input_json,
            max_payload_bytes,
            claim,
            Some(result_commit_context),
        )
    }

    #[cfg(test)]
    fn enqueue_claimed_capability_request_for_test(
        &self,
        device_id: &str,
        capability: &str,
        input_json: Vec<u8>,
        max_payload_bytes: u64,
        claim: &NetworkedWorkerDispatchClaim,
    ) -> Result<CapabilityExecutionReceiver, Status> {
        self.enqueue_claimed_capability_request_inner(
            device_id,
            capability,
            input_json,
            max_payload_bytes,
            claim,
            None,
        )
    }

    fn enqueue_claimed_capability_request_inner(
        &self,
        device_id: &str,
        capability: &str,
        input_json: Vec<u8>,
        max_payload_bytes: u64,
        claim: &NetworkedWorkerDispatchClaim,
        result_commit_context: Option<NetworkedWorkerResultCommitContext>,
    ) -> Result<CapabilityExecutionReceiver, Status> {
        let now = current_unix_ms()?;
        if claim.node_request_id.is_empty()
            || claim.remote_request_id.is_empty()
            || claim.worker_id != device_id
            || claim.capability != capability
            || claim.request_sha256 != sha256_hex(input_json.as_slice())
            || !matches!(claim.state, crate::journal::NetworkedWorkerDispatchClaimState::Queued)
        {
            return Err(Status::failed_precondition(
                "networked worker dispatch claim does not match queued payload",
            ));
        }
        let session_id = claim.session_id.clone().ok_or_else(|| {
            Status::failed_precondition(
                "networked worker dispatch claim is missing session generation authority",
            )
        })?;
        let run_generation = claim.run_generation.ok_or_else(|| {
            Status::failed_precondition(
                "networked worker dispatch claim is missing run generation authority",
            )
        })?;
        let request_id = claim.node_request_id.clone();
        let dispatch = CapabilityDispatchRecord {
            request_id: request_id.clone(),
            capability: capability.to_owned(),
            input_json: input_json.clone(),
            max_payload_bytes,
            networked_worker_reservation: None,
            networked_worker_result_commit_context: result_commit_context,
            authority: CapabilityDispatchAuthority::NetworkedWorker {
                remote_request_id: claim.remote_request_id.clone(),
                request_sha256: claim.request_sha256.clone(),
                lease_id: claim.lease_id.clone(),
                session_id,
                run_id: claim.run_id.clone(),
                run_generation,
                lease_expires_at_unix_ms: claim.lease_expires_at_unix_ms,
            },
        };
        let slot = Arc::new(CapabilityExecutionSlot::default());
        let request = CapabilityRequestRecord {
            request_id: request_id.clone(),
            device_id: device_id.to_owned(),
            capability: capability.to_owned(),
            state: CapabilityRequestState::Queued,
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
            dispatched_at_unix_ms: None,
            completed_at_unix_ms: None,
            max_payload_bytes,
            authority: CapabilityRequestAuthorityRecord::NetworkedWorker {
                remote_request_id: claim.remote_request_id.clone(),
            },
            input_summary: summarize_payload_bytes(input_json.as_slice()),
            output_summary: None,
            error: None,
        };
        let mut capabilities = lock_mutex(&self.capabilities, "node capability runtime")?;
        if capabilities.result_slots_by_request_id.contains_key(request_id.as_str())
            || capabilities.generic_inflight_by_request_id.contains_key(request_id.as_str())
            || capabilities
                .networked_worker_inflight_by_request_id
                .contains_key(request_id.as_str())
            || capabilities.reserved_worker_payloads_by_request_id.contains_key(request_id.as_str())
            || capabilities
                .queued_by_device
                .values()
                .any(|queue| queue.iter().any(|item| item.request_id == request_id))
        {
            return Err(Status::already_exists("networked worker node request is already queued"));
        }
        let mut persisted = lock_mutex(&self.persisted, "node runtime state")?;
        if persisted.capability_requests.contains_key(request_id.as_str()) {
            return Err(Status::already_exists(
                "networked worker node request audit record already exists",
            ));
        }
        persisted.capability_requests.insert(request_id.clone(), request);
        self.persist_locked(&persisted)?;
        capabilities.queued_by_device.entry(device_id.to_owned()).or_default().push_back(dispatch);
        capabilities.result_slots_by_request_id.insert(request_id, Arc::clone(&slot));
        Ok(CapabilityExecutionReceiver { slot })
    }

    /// Returns the next generic dispatch or metadata-only worker reservation after durable writes.
    ///
    /// A fenced worker payload remains process-local until [`Self::fetch_networked_worker_payload`]
    /// commits the exact release transaction. The event stream never receives its raw bytes.
    ///
    /// # Errors
    /// Returns a gRPC status on clock, lock, persistence, randomness, or durable authorization
    /// failure.
    pub(crate) fn next_capability_dispatch(
        &self,
        device_id: &str,
        authorizer: &dyn CapabilityDispatchAuthorizer,
    ) -> Result<Option<CapabilityDispatchRecord>, Status> {
        loop {
            let now = current_unix_ms()?;
            let mut capabilities = lock_mutex(&self.capabilities, "node capability runtime")?;
            let Some(mut dispatch) = capabilities
                .queued_by_device
                .get(device_id)
                .and_then(|queue| queue.front())
                .cloned()
            else {
                return Ok(None);
            };

            let mut reserved_delivery = None;
            let rejected_reason = match &dispatch.authority {
                CapabilityDispatchAuthority::Generic => None,
                CapabilityDispatchAuthority::NetworkedWorker {
                    remote_request_id,
                    request_sha256,
                    lease_id,
                    session_id: _,
                    run_id,
                    run_generation,
                    lease_expires_at_unix_ms,
                } => {
                    let observed_sha256 = sha256_hex(dispatch.input_json.as_slice());
                    if observed_sha256 != *request_sha256 {
                        match authorizer.cancel_networked_worker_dispatch(
                            remote_request_id,
                            dispatch.request_id.as_str(),
                            "worker.dispatch.payload_digest_mismatch",
                            now,
                        )? {
                            NetworkedWorkerDispatchCancelOutcome::Cancelled
                            | NetworkedWorkerDispatchCancelOutcome::AlreadyCancelled => {}
                            NetworkedWorkerDispatchCancelOutcome::InFlight => {
                                return Err(Status::failed_precondition(
                                    "networked worker payload changed after dispatch authority began",
                                ));
                            }
                            NetworkedWorkerDispatchCancelOutcome::Missing => {
                                return Err(Status::failed_precondition(
                                    "networked worker payload mismatch claim is missing",
                                ));
                            }
                        }
                        Some("networked worker queued payload digest mismatch")
                    } else {
                        let delivery_attempt_id = Ulid::generate().to_string();
                        let fetch_token = mint_networked_worker_delivery_token();
                        let reservation_request = NetworkedWorkerDeliveryReservationRequest {
                            remote_request_id: remote_request_id.clone(),
                            node_request_id: dispatch.request_id.clone(),
                            request_sha256: request_sha256.clone(),
                            delivery_attempt_id: delivery_attempt_id.clone(),
                            delivery_token_sha256: sha256_hex(fetch_token.as_bytes()),
                            observed_at_unix_ms: now,
                        };
                        match authorizer.reserve_networked_worker_delivery(&reservation_request)? {
                            NetworkedWorkerDeliveryReservationOutcome::Authorized {
                                fleet_generation,
                            } => {
                                reserved_delivery = Some(NetworkedWorkerDeliveryReservation {
                                    request_id: dispatch.request_id.clone(),
                                    delivery_attempt_id,
                                    fetch_token,
                                    request_sha256: request_sha256.clone(),
                                    worker_id: device_id.to_owned(),
                                    lease_id: lease_id.clone(),
                                    run_id: run_id.clone(),
                                    fleet_generation,
                                    run_generation: *run_generation,
                                    expires_at_unix_ms: *lease_expires_at_unix_ms,
                                });
                                None
                            }
                            NetworkedWorkerDeliveryReservationOutcome::Rejected => {
                                Some("networked worker durable delivery reservation rejected")
                            }
                        }
                    }
                }
            };

            let mut persisted = lock_mutex(&self.persisted, "node runtime state")?;
            let Some(request) = persisted.capability_requests.get_mut(dispatch.request_id.as_str())
            else {
                return Err(Status::failed_precondition(
                    "queued capability request has no persisted audit record",
                ));
            };
            if let Some(reason) = rejected_reason {
                request.state = CapabilityRequestState::Rejected;
                request.updated_at_unix_ms = now;
                request.completed_at_unix_ms = Some(now);
                request.error = Some(reason.to_owned());
                self.persist_locked(&persisted)?;
                remove_queued_capability(
                    &mut capabilities,
                    device_id,
                    dispatch.request_id.as_str(),
                )?;
                capabilities.result_slots_by_request_id.remove(dispatch.request_id.as_str());
                continue;
            }

            let request_before_dispatch = request.clone();
            request.state = CapabilityRequestState::Dispatched;
            request.updated_at_unix_ms = now;
            request.dispatched_at_unix_ms = Some(now);
            if let Err(persist_error) = self.persist_locked(&persisted) {
                persisted
                    .capability_requests
                    .insert(dispatch.request_id.clone(), request_before_dispatch);
                enum RollbackOutcome {
                    Exact,
                    Inexact,
                    CheckFailed(Status),
                }

                let rollback_outcome = match (&dispatch.authority, &reserved_delivery) {
                    (
                        CapabilityDispatchAuthority::NetworkedWorker {
                            remote_request_id,
                            request_sha256,
                            ..
                        },
                        Some(reservation),
                    ) => match authorizer.abort_networked_worker_dispatch_before_payload_release(
                        remote_request_id.as_str(),
                        dispatch.request_id.as_str(),
                        request_sha256.as_str(),
                        reservation.fleet_generation,
                        now,
                    ) {
                        Ok(
                            NetworkedWorkerDispatchAbortBeforeReleaseOutcome::Aborted
                            | NetworkedWorkerDispatchAbortBeforeReleaseOutcome::AlreadyAborted,
                        ) => RollbackOutcome::Exact,
                        Ok(
                            NetworkedWorkerDispatchAbortBeforeReleaseOutcome::NotAbortable
                            | NetworkedWorkerDispatchAbortBeforeReleaseOutcome::Missing,
                        ) => RollbackOutcome::Inexact,
                        Err(error) => RollbackOutcome::CheckFailed(error),
                    },
                    _ => return Err(persist_error),
                };

                let request = persisted
                    .capability_requests
                    .get_mut(dispatch.request_id.as_str())
                    .expect("queued capability request was restored before recovery");
                request.state = if matches!(&rollback_outcome, RollbackOutcome::Exact) {
                    CapabilityRequestState::Cancelled
                } else {
                    CapabilityRequestState::Failed
                };
                request.updated_at_unix_ms = now.max(request.updated_at_unix_ms);
                request.dispatched_at_unix_ms = None;
                request.completed_at_unix_ms = Some(request.updated_at_unix_ms);
                request.error = Some(
                    if matches!(&rollback_outcome, RollbackOutcome::Exact) {
                        "networked worker payload withheld after local audit persistence failure; durable dispatch authority cancelled"
                    } else {
                        "networked worker payload withheld after local audit persistence failure; durable dispatch authority requires reconciliation"
                    }
                    .to_owned(),
                );
                let recovery_persist_error = self.persist_locked(&persisted).err();

                remove_queued_capability(
                    &mut capabilities,
                    device_id,
                    dispatch.request_id.as_str(),
                )?;
                capabilities.result_slots_by_request_id.remove(dispatch.request_id.as_str());

                let recovery_failure = recovery_persist_error
                    .as_ref()
                    .map(|error| {
                        format!("; terminal local audit recovery also failed: {}", error.message())
                    })
                    .unwrap_or_default();
                return match rollback_outcome {
                    RollbackOutcome::Exact => {
                        if recovery_persist_error.is_some() {
                            Err(Status::internal(format!(
                                "node audit persistence failed before payload release and durable dispatch was cancelled{recovery_failure}; initial failure: {}",
                                persist_error.message()
                            )))
                        } else {
                            Err(persist_error)
                        }
                    }
                    RollbackOutcome::Inexact => Err(Status::failed_precondition(format!(
                        "node audit persistence failed before payload release and durable dispatch rollback was not exact: {}{recovery_failure}",
                        persist_error.message()
                    ))),
                    RollbackOutcome::CheckFailed(rollback_error) => {
                        Err(Status::failed_precondition(format!(
                            "node audit persistence failed before payload release and durable dispatch rollback could not be confirmed: {}; rollback check failed: {}{recovery_failure}",
                            persist_error.message(),
                            rollback_error.message()
                        )))
                    }
                };
            }

            remove_queued_capability(&mut capabilities, device_id, dispatch.request_id.as_str())?;
            if let Some(reservation) = reserved_delivery {
                let mut reserved_dispatch = dispatch.clone();
                reserved_dispatch.networked_worker_reservation = Some(reservation.clone());
                capabilities
                    .reserved_worker_payloads_by_request_id
                    .insert(dispatch.request_id.clone(), reserved_dispatch);
                capabilities.networked_worker_inflight_by_request_id.insert(
                    dispatch.request_id.clone(),
                    NetworkedWorkerInFlightMetadata {
                        device_id: device_id.to_owned(),
                        remote_request_id: match &dispatch.authority {
                            CapabilityDispatchAuthority::NetworkedWorker {
                                remote_request_id,
                                ..
                            } => remote_request_id.clone(),
                            CapabilityDispatchAuthority::Generic => unreachable!(
                                "worker reservation requires networked-worker dispatch authority"
                            ),
                        },
                        delivery_attempt_id: reservation.delivery_attempt_id.clone(),
                        run_generation: reservation.run_generation,
                        request_sha256: reservation.request_sha256.clone(),
                        payload_released: false,
                        result_commit_context: dispatch
                            .networked_worker_result_commit_context
                            .clone(),
                        committed_observed_at_unix_ms: None,
                        committed_result_sha256: None,
                    },
                );
                dispatch.input_json.clear();
                dispatch.networked_worker_reservation = Some(reservation);
            } else {
                capabilities
                    .generic_inflight_by_request_id
                    .insert(dispatch.request_id.clone(), dispatch.clone());
            }
            return Ok(Some(dispatch));
        }
    }

    /// Recovers a metadata reservation that could not be delivered to the node event stream.
    ///
    /// The event sender reports failure only when the response was not accepted by its bounded
    /// channel, so an unreleased reservation can be durably cancelled without claiming recall of
    /// bytes. Generic dispatches and already-released worker payloads remain reconciliation-owned.
    ///
    /// # Errors
    /// Returns a gRPC status when durable authority or local audit evidence cannot be checked or
    /// committed.
    pub(crate) fn recover_undelivered_capability_dispatch(
        &self,
        dispatch: &CapabilityDispatchRecord,
        authorizer: &dyn CapabilityDispatchAuthorizer,
    ) -> Result<CapabilityRequestStopOutcome, Status> {
        self.stop_capability_request(
            dispatch.request_id.as_str(),
            "node event stream closed before dispatch delivery",
            Some(authorizer),
        )
    }

    /// Releases the exact reserved worker payload after durable authority commits.
    ///
    /// No bytes are returned for a mismatched device, request, attempt, token, digest, or durable
    /// release denial. The raw payload is removed from reserved ownership after the release commit;
    /// a lost RPC response is therefore treated as released and reconciliation-owned.
    ///
    /// # Errors
    /// Returns a gRPC status when volatile reservation state is missing/inconsistent or durable
    /// release authority cannot be checked or committed.
    pub(crate) fn fetch_networked_worker_payload(
        &self,
        reporting_device_id: &str,
        request_id: &str,
        delivery_attempt_id: &str,
        fetch_token: &str,
        authorizer: &dyn CapabilityDispatchAuthorizer,
    ) -> Result<NetworkedWorkerFetchedPayload, Status> {
        let now = current_unix_ms()?;
        let mut capabilities = lock_mutex(&self.capabilities, "node capability runtime")?;
        let metadata = capabilities
            .networked_worker_inflight_by_request_id
            .get(request_id)
            .cloned()
            .ok_or_else(|| {
                Status::failed_precondition("networked worker delivery reservation is not active")
            })?;
        if metadata.device_id != reporting_device_id
            || metadata.delivery_attempt_id != delivery_attempt_id
            || metadata.payload_released
        {
            return Err(Status::failed_precondition(
                "networked worker delivery reservation does not match the authenticated request",
            ));
        }
        let dispatch = capabilities
            .reserved_worker_payloads_by_request_id
            .get(request_id)
            .cloned()
            .ok_or_else(|| {
                Status::failed_precondition("networked worker reserved payload is unavailable")
            })?;
        if sha256_hex(dispatch.input_json.as_slice()) != metadata.request_sha256 {
            return Err(Status::failed_precondition(
                "networked worker reserved payload digest is inconsistent",
            ));
        }
        match authorizer.release_networked_worker_payload(
            &NetworkedWorkerPayloadReleaseRequest {
                node_request_id: request_id.to_owned(),
                delivery_attempt_id: delivery_attempt_id.to_owned(),
                delivery_token: fetch_token.to_owned(),
                reporting_worker_id: reporting_device_id.to_owned(),
                observed_at_unix_ms: now,
            },
        )? {
            NetworkedWorkerPayloadReleaseOutcome::Released => {}
            NetworkedWorkerPayloadReleaseOutcome::AlreadyReleased => {
                return Err(Status::failed_precondition(
                    "networked worker payload was already released",
                ));
            }
            NetworkedWorkerPayloadReleaseOutcome::Rejected => {
                return Err(Status::failed_precondition(
                    "networked worker payload release authority was rejected",
                ));
            }
        }
        capabilities.reserved_worker_payloads_by_request_id.remove(request_id);
        let metadata = capabilities
            .networked_worker_inflight_by_request_id
            .get_mut(request_id)
            .expect("networked worker in-flight metadata was checked before durable release");
        metadata.payload_released = true;
        Ok(NetworkedWorkerFetchedPayload {
            request_id: request_id.to_owned(),
            delivery_attempt_id: delivery_attempt_id.to_owned(),
            input_json: dispatch.input_json,
            max_payload_bytes: dispatch.max_payload_bytes,
            request_sha256: metadata.request_sha256.clone(),
        })
    }

    /// Records one exact worker payload acknowledgement.
    ///
    /// # Errors
    /// Returns a gRPC status when the reservation does not belong to the authenticated node or
    /// durable acknowledgement evidence cannot be checked or committed.
    pub(crate) fn acknowledge_networked_worker_payload(
        &self,
        reporting_device_id: &str,
        request_id: &str,
        delivery_attempt_id: &str,
        fetch_token: &str,
        authorizer: &dyn CapabilityDispatchAuthorizer,
    ) -> Result<NetworkedWorkerPayloadAcknowledgementOutcome, Status> {
        let now = current_unix_ms()?;
        let capabilities = lock_mutex(&self.capabilities, "node capability runtime")?;
        let metadata = capabilities
            .networked_worker_inflight_by_request_id
            .get(request_id)
            .ok_or_else(|| {
                Status::failed_precondition("networked worker delivery reservation is not active")
            })?;
        if metadata.device_id != reporting_device_id
            || metadata.delivery_attempt_id != delivery_attempt_id
            || !metadata.payload_released
        {
            return Err(Status::failed_precondition(
                "networked worker acknowledgement does not match a released payload",
            ));
        }
        let outcome = authorizer.acknowledge_networked_worker_payload(
            &NetworkedWorkerPayloadAcknowledgementRequest {
                node_request_id: request_id.to_owned(),
                delivery_attempt_id: delivery_attempt_id.to_owned(),
                delivery_token: fetch_token.to_owned(),
                reporting_worker_id: reporting_device_id.to_owned(),
                observed_at_unix_ms: now,
            },
        )?;
        if matches!(outcome, NetworkedWorkerPayloadAcknowledgementOutcome::Rejected) {
            return Err(Status::failed_precondition(
                "networked worker payload acknowledgement was rejected",
            ));
        }
        Ok(outcome)
    }

    /// Records a node-reported result after authenticating request ownership and wakes the caller.
    ///
    /// A valid late result overwrites a `TimedOut` record only when its runtime-owned one-item slot
    /// is empty. The slot survives dropped waiting futures, so caller cancellation cannot create an
    /// ownerless retry gap.
    ///
    /// # Errors
    /// Returns `permission_denied` for the wrong reporting node, `failed_precondition` for missing
    /// or inactive request/claim authority, and `internal` on clock, lock, or persistence failure.
    pub(crate) fn complete_capability_request(
        &self,
        reporting_device_id: &str,
        request_id: &str,
        delivery_attempt_id: Option<&str>,
        run_generation: Option<RuntimeGeneration>,
        result: CapabilityExecutionResult,
        authorizer: &dyn CapabilityDispatchAuthorizer,
    ) -> Result<bool, Status> {
        let now = current_unix_ms()?;
        let mut capabilities = lock_mutex(&self.capabilities, "node capability runtime")?;
        let mut persisted = lock_mutex(&self.persisted, "node runtime state")?;
        let request = persisted.capability_requests.get_mut(request_id).ok_or_else(|| {
            Status::failed_precondition("capability result request is not active")
        })?;
        authorize_capability_request_owner(request, reporting_device_id)?;
        if !matches!(
            request.state,
            CapabilityRequestState::Dispatched
                | CapabilityRequestState::AwaitingLocalMediation
                | CapabilityRequestState::TimedOut
        ) {
            return Err(Status::failed_precondition(
                "capability result request is not awaiting a result",
            ));
        }
        let Some(slot) = capabilities.result_slots_by_request_id.get(request_id).cloned() else {
            return Ok(false);
        };
        let mut notification_slot =
            lock_mutex(&slot.notification, "node capability result notification")?;
        if notification_slot.is_some() {
            return Err(Status::unavailable(
                "capability result notification slot is already occupied",
            ));
        }

        let mut canonical_observed_at_unix_ms = now;
        let mut networked_worker_commit_disposition = None;
        let (authenticated_delivery_attempt_id, authenticated_run_generation) =
            if let CapabilityRequestAuthorityRecord::NetworkedWorker { remote_request_id } =
                &request.authority
            {
                let delivery_attempt_id = delivery_attempt_id.ok_or_else(|| {
                    Status::invalid_argument("networked worker result missing delivery_attempt_id")
                })?;
                let run_generation = run_generation.ok_or_else(|| {
                    Status::invalid_argument("networked worker result missing run_generation")
                })?;
                let metadata = capabilities
                    .networked_worker_inflight_by_request_id
                    .get(request_id)
                    .cloned()
                    .ok_or_else(|| {
                        Status::failed_precondition(
                            "networked worker result delivery reservation is not active",
                        )
                    })?;
                if metadata.remote_request_id != *remote_request_id
                    || metadata.device_id != reporting_device_id
                    || metadata.delivery_attempt_id != delivery_attempt_id
                    || metadata.run_generation != run_generation
                    || !metadata.payload_released
                {
                    return Err(Status::failed_precondition(
                        "networked worker result does not match the released delivery attempt",
                    ));
                }
                if let Some(result_commit_context) = metadata.result_commit_context {
                    if !result.success {
                        return Err(Status::failed_precondition(
                            "networked worker callback did not carry a result envelope",
                        ));
                    }
                    let worker_result = serde_json::from_slice::<WorkerRemoteToolResultEnvelope>(
                        result.output_json.as_slice(),
                    )
                    .map_err(|error| {
                        Status::invalid_argument(format!(
                            "networked worker callback result envelope is malformed: {error}"
                        ))
                    })?;
                    if worker_result.run_generation != run_generation {
                        return Err(Status::failed_precondition(
                            "networked worker callback generations do not match",
                        ));
                    }
                    let validation_observed_at_unix_ms =
                        metadata.committed_observed_at_unix_ms.unwrap_or(now);
                    let validated_result_sha256 = worker_result
                        .validated_receipt_sha256(
                            &result_commit_context.request,
                            validation_observed_at_unix_ms,
                        )
                        .map_err(|error| {
                            Status::failed_precondition(format!(
                                "networked worker callback failed contract validation: {error}"
                            ))
                        })?;
                    if metadata
                        .committed_result_sha256
                        .as_deref()
                        .is_some_and(|committed| committed != validated_result_sha256)
                    {
                        return Err(Status::failed_precondition(
                            "networked worker callback conflicts with the committed result",
                        ));
                    }
                    match authorizer.commit_networked_worker_result(
                        &NetworkedWorkerResultCommitRequest {
                            context: result_commit_context,
                            result: worker_result,
                            node_request_id: request_id.to_owned(),
                            delivery_attempt_id: delivery_attempt_id.to_owned(),
                            reporting_worker_id: reporting_device_id.to_owned(),
                            callback_run_generation: run_generation,
                            observed_at_unix_ms: validation_observed_at_unix_ms,
                        },
                    )? {
                        NetworkedWorkerResultCommitOutcome::Committed {
                            disposition,
                            canonical_observed_at_unix_ms: committed_at,
                            validated_result_sha256: committed_sha256,
                        } => {
                            if committed_sha256 != validated_result_sha256
                                || metadata
                                    .committed_observed_at_unix_ms
                                    .is_some_and(|observed| observed != committed_at)
                            {
                                return Err(Status::failed_precondition(
                                    "networked worker callback commit returned conflicting evidence",
                                ));
                            }
                            let active_metadata = capabilities
                                .networked_worker_inflight_by_request_id
                                .get_mut(request_id)
                                .ok_or_else(|| {
                                    Status::failed_precondition(
                                        "networked worker callback authority disappeared during commit",
                                    )
                                })?;
                            active_metadata.committed_observed_at_unix_ms = Some(committed_at);
                            active_metadata.committed_result_sha256 = Some(committed_sha256);
                            canonical_observed_at_unix_ms = committed_at;
                            networked_worker_commit_disposition = Some(disposition);
                        }
                        NetworkedWorkerResultCommitOutcome::StaleSuppressed => {
                            return Err(Status::failed_precondition(
                                "networked worker callback belongs to a superseded run generation",
                            ));
                        }
                        NetworkedWorkerResultCommitOutcome::Rejected => {
                            return Err(Status::failed_precondition(
                                "networked worker result claim is not active for this node",
                            ));
                        }
                    }
                } else {
                    #[cfg(test)]
                    {
                        if !matches!(
                            authorizer.authorize_networked_worker_result(
                                remote_request_id,
                                request_id,
                                delivery_attempt_id,
                                run_generation,
                                reporting_device_id,
                                now,
                            )?,
                            NetworkedWorkerResultAuthorizationOutcome::Authorized
                        ) {
                            return Err(Status::failed_precondition(
                                "networked worker result claim is not active for this node",
                            ));
                        }
                    }
                    #[cfg(not(test))]
                    {
                        return Err(Status::internal(
                            "networked worker callback is missing durable commit context",
                        ));
                    }
                }
                (Some(delivery_attempt_id.to_owned()), Some(run_generation))
            } else {
                if delivery_attempt_id.is_some() || run_generation.is_some() {
                    return Err(Status::invalid_argument(
                        "generic capability result must not include delivery_attempt_id or run_generation",
                    ));
                }
                (None, None)
            };

        let result_state = if result.success {
            CapabilityRequestState::Succeeded
        } else {
            CapabilityRequestState::Failed
        };
        let output_summary = summarize_payload_bytes(result.output_json.as_slice());
        let error = normalize_summary_text(result.error.as_str());

        let request_before_completion = request.clone();
        request.state = result_state;
        request.updated_at_unix_ms = canonical_observed_at_unix_ms;
        request.completed_at_unix_ms = Some(canonical_observed_at_unix_ms);
        request.output_summary = output_summary;
        request.error = error;
        if let Err(persist_error) = self.persist_locked(&persisted) {
            persisted.capability_requests.insert(request_id.to_owned(), request_before_completion);
            return Err(persist_error);
        }

        *notification_slot = Some(CapabilityExecutionNotification {
            result,
            delivery_attempt_id: authenticated_delivery_attempt_id,
            run_generation: authenticated_run_generation,
            networked_worker_commit_disposition,
            observed_at_unix_ms: canonical_observed_at_unix_ms,
        });
        drop(notification_slot);
        slot.ready.notify_one();
        capabilities.result_slots_by_request_id.remove(request_id);
        capabilities.generic_inflight_by_request_id.remove(request_id);
        capabilities.networked_worker_inflight_by_request_id.remove(request_id);
        capabilities.reserved_worker_payloads_by_request_id.remove(request_id);
        Ok(true)
    }

    /// Marks a pending request `TimedOut` without overwriting a committed result.
    ///
    /// Dropping a waiting future does not remove the runtime-owned result slot while reconciliation
    /// owns it. A late node result clears volatile dispatch state only after publication.
    /// Callers must treat [`CapabilityRequestTimeoutOutcome::ResultCommitted`] as authoritative and
    /// drain their existing result receiver before returning a deadline.
    ///
    /// # Errors
    /// Returns `Status::internal` on clock, lock, or persistence failure.
    pub(crate) fn mark_capability_timeout(
        &self,
        request_id: &str,
    ) -> Result<CapabilityRequestTimeoutOutcome, Status> {
        let now = current_unix_ms()?;
        let mut persisted = lock_mutex(&self.persisted, "node runtime state")?;
        let Some(request) = persisted.capability_requests.get_mut(request_id) else {
            return Ok(CapabilityRequestTimeoutOutcome::Missing);
        };
        let outcome = match request.state {
            CapabilityRequestState::Queued
            | CapabilityRequestState::Dispatched
            | CapabilityRequestState::AwaitingLocalMediation => {
                request.state = CapabilityRequestState::TimedOut;
                request.updated_at_unix_ms = now;
                request.completed_at_unix_ms = Some(now);
                request.error = Some("timed out waiting for node capability result".to_owned());
                self.persist_locked(&persisted)?;
                CapabilityRequestTimeoutOutcome::MarkedTimedOut
            }
            CapabilityRequestState::Succeeded | CapabilityRequestState::Failed => {
                CapabilityRequestTimeoutOutcome::ResultCommitted
            }
            CapabilityRequestState::TimedOut
            | CapabilityRequestState::Rejected
            | CapabilityRequestState::Cancelled => CapabilityRequestTimeoutOutcome::AlreadyTerminal,
        };
        Ok(outcome)
    }

    /// Test-only hook that simulates loss of runtime-owned result delivery after dispatch.
    #[cfg(test)]
    pub(crate) fn drop_capability_result_owner(&self, request_id: &str) -> Result<bool, Status> {
        let mut capabilities = lock_mutex(&self.capabilities, "node capability runtime")?;
        Ok(capabilities.result_slots_by_request_id.remove(request_id).is_some())
    }

    /// Test-only hook that fails the result audit write after ownership checks.
    #[cfg(test)]
    pub(crate) fn fail_next_result_persist_for_test(&self) {
        self.fail_next_persist_for_test();
    }

    /// Stops queued or unreleased work without claiming recall after payload release.
    ///
    /// # Errors
    /// Returns a gRPC status when durable authority or local audit evidence cannot be checked or
    /// committed.
    pub(crate) fn stop_capability_request(
        &self,
        request_id: &str,
        reason: &str,
        authorizer: Option<&dyn CapabilityDispatchAuthorizer>,
    ) -> Result<CapabilityRequestStopOutcome, Status> {
        let now = current_unix_ms()?;
        let mut capabilities = lock_mutex(&self.capabilities, "node capability runtime")?;
        if capabilities.generic_inflight_by_request_id.contains_key(request_id) {
            return Ok(CapabilityRequestStopOutcome::ReleasedReconciliationOwned);
        }
        if let Some(metadata) =
            capabilities.networked_worker_inflight_by_request_id.get(request_id).cloned()
        {
            if metadata.payload_released {
                return Ok(CapabilityRequestStopOutcome::ReleasedReconciliationOwned);
            }
            let Some(authorizer) = authorizer else {
                return Err(Status::failed_precondition(
                    "networked worker reservation cancellation requires durable authorizer",
                ));
            };
            let dispatch = capabilities
                .reserved_worker_payloads_by_request_id
                .get(request_id)
                .ok_or_else(|| {
                    Status::failed_precondition(
                        "networked worker unreleased reservation is missing its payload",
                    )
                })?;
            let dispatch_fleet_generation = match &dispatch.networked_worker_reservation {
                Some(reservation) => reservation.fleet_generation,
                None => {
                    return Err(Status::failed_precondition(
                        "networked worker unreleased reservation is missing delivery metadata",
                    ));
                }
            };
            match authorizer.abort_networked_worker_dispatch_before_payload_release(
                metadata.remote_request_id.as_str(),
                request_id,
                metadata.request_sha256.as_str(),
                dispatch_fleet_generation,
                now,
            )? {
                NetworkedWorkerDispatchAbortBeforeReleaseOutcome::Aborted
                | NetworkedWorkerDispatchAbortBeforeReleaseOutcome::AlreadyAborted => {}
                NetworkedWorkerDispatchAbortBeforeReleaseOutcome::NotAbortable => {
                    return Ok(CapabilityRequestStopOutcome::ReleasedReconciliationOwned);
                }
                NetworkedWorkerDispatchAbortBeforeReleaseOutcome::Missing => {
                    return Err(Status::failed_precondition(
                        "networked worker reservation cancellation claim is missing",
                    ));
                }
            }
            let mut persisted = lock_mutex(&self.persisted, "node runtime state")?;
            let request = persisted.capability_requests.get_mut(request_id).ok_or_else(|| {
                Status::failed_precondition(
                    "networked worker reservation cancellation audit record is missing",
                )
            })?;
            if !matches!(request.state, CapabilityRequestState::Dispatched) {
                return Ok(CapabilityRequestStopOutcome::AlreadyTerminal);
            }
            request.state = CapabilityRequestState::Cancelled;
            request.updated_at_unix_ms = now;
            request.completed_at_unix_ms = Some(now);
            request.error = normalize_summary_text(reason)
                .or_else(|| Some("capability request cancelled before payload release".to_owned()));
            self.persist_locked(&persisted)?;
            capabilities.reserved_worker_payloads_by_request_id.remove(request_id);
            capabilities.networked_worker_inflight_by_request_id.remove(request_id);
            capabilities.result_slots_by_request_id.remove(request_id);
            return Ok(CapabilityRequestStopOutcome::CancelledBeforeRelease);
        }
        let queued_device = capabilities.queued_by_device.iter().find_map(|(device_id, queue)| {
            queue
                .iter()
                .any(|dispatch| dispatch.request_id == request_id)
                .then(|| device_id.clone())
        });
        let Some(queued_device) = queued_device else {
            let persisted = lock_mutex(&self.persisted, "node runtime state")?;
            return Ok(match persisted.capability_requests.get(request_id) {
                Some(request)
                    if matches!(
                        request.state,
                        CapabilityRequestState::Succeeded
                            | CapabilityRequestState::Failed
                            | CapabilityRequestState::TimedOut
                            | CapabilityRequestState::Rejected
                            | CapabilityRequestState::Cancelled
                    ) =>
                {
                    CapabilityRequestStopOutcome::AlreadyTerminal
                }
                Some(_) => CapabilityRequestStopOutcome::ReleasedReconciliationOwned,
                None => CapabilityRequestStopOutcome::Missing,
            });
        };
        let mut persisted = lock_mutex(&self.persisted, "node runtime state")?;
        let Some(request) = persisted.capability_requests.get_mut(request_id) else {
            return Ok(CapabilityRequestStopOutcome::Missing);
        };
        if let CapabilityRequestAuthorityRecord::NetworkedWorker { remote_request_id } =
            &request.authority
        {
            let Some(authorizer) = authorizer else {
                return Err(Status::failed_precondition(
                    "networked worker queued cancellation requires durable authorizer",
                ));
            };
            match authorizer.cancel_networked_worker_dispatch(
                remote_request_id,
                request_id,
                "worker.dispatch.cancelled_before_dispatch",
                now,
            )? {
                NetworkedWorkerDispatchCancelOutcome::Cancelled
                | NetworkedWorkerDispatchCancelOutcome::AlreadyCancelled => {}
                NetworkedWorkerDispatchCancelOutcome::InFlight => {
                    return Ok(CapabilityRequestStopOutcome::ReleasedReconciliationOwned);
                }
                NetworkedWorkerDispatchCancelOutcome::Missing => {
                    return Err(Status::failed_precondition(
                        "networked worker queued cancellation claim is missing",
                    ));
                }
            }
        }
        request.state = CapabilityRequestState::Cancelled;
        request.updated_at_unix_ms = now;
        request.completed_at_unix_ms = Some(now);
        request.error = normalize_summary_text(reason)
            .or_else(|| Some("capability request cancelled before dispatch".to_owned()));
        self.persist_locked(&persisted)?;
        if let Some(queue) = capabilities.queued_by_device.get_mut(queued_device.as_str()) {
            queue.retain(|dispatch| dispatch.request_id != request_id);
            if queue.is_empty() {
                capabilities.queued_by_device.remove(queued_device.as_str());
            }
        }
        capabilities.result_slots_by_request_id.remove(request_id);
        Ok(CapabilityRequestStopOutcome::CancelledBeforeRelease)
    }

    /// Compatibility wrapper returning whether [`Self::stop_capability_request`] proved recall.
    ///
    /// # Errors
    /// Returns a gRPC status when durable authority or local audit evidence cannot be checked or
    /// committed.
    #[cfg(test)]
    pub(crate) fn cancel_queued_capability_request(
        &self,
        request_id: &str,
        reason: &str,
        authorizer: Option<&dyn CapabilityDispatchAuthorizer>,
    ) -> Result<bool, Status> {
        self.stop_capability_request(request_id, reason, authorizer)
            .map(|outcome| matches!(outcome, CapabilityRequestStopOutcome::CancelledBeforeRelease))
    }

    /// Marks an owned dispatched request as blocked on local (on-device) mediation.
    ///
    /// # Errors
    /// Returns `permission_denied` for the wrong reporting node, `failed_precondition` when the
    /// request is missing or not dispatched, and `internal` on clock, lock, or persistence failure.
    pub(crate) fn mark_capability_awaiting_local_mediation(
        &self,
        reporting_device_id: &str,
        request_id: &str,
    ) -> Result<bool, Status> {
        let now = current_unix_ms()?;
        let mut persisted = lock_mutex(&self.persisted, "node runtime state")?;
        let request = persisted.capability_requests.get_mut(request_id).ok_or_else(|| {
            Status::failed_precondition("capability mediation request is not active")
        })?;
        authorize_capability_request_owner(request, reporting_device_id)?;
        if !matches!(
            request.state,
            CapabilityRequestState::Dispatched | CapabilityRequestState::AwaitingLocalMediation
        ) {
            return Err(Status::failed_precondition(
                "capability mediation request is not dispatched",
            ));
        }
        request.state = CapabilityRequestState::AwaitingLocalMediation;
        request.updated_at_unix_ms = now;
        request.dispatched_at_unix_ms = Some(request.dispatched_at_unix_ms.unwrap_or(now));
        self.persist_locked(&persisted)?;
        Ok(true)
    }

    /// Lists capability request audit records (optionally filtered by
    /// device), newest first.
    ///
    /// # Errors
    /// Returns `Status::internal` when the state lock is poisoned.
    pub(crate) fn capability_requests(
        &self,
        device_id: Option<&str>,
    ) -> Result<Vec<CapabilityRequestRecord>, Status> {
        let persisted = lock_mutex(&self.persisted, "node runtime state")?;
        let mut requests = persisted
            .capability_requests
            .values()
            .filter(|record| {
                device_id.is_none_or(|candidate| candidate == record.device_id.as_str())
            })
            .cloned()
            .collect::<Vec<_>>();
        requests.sort_by(|left, right| {
            right
                .created_at_unix_ms
                .cmp(&left.created_at_unix_ms)
                .then_with(|| left.request_id.cmp(&right.request_id))
        });
        Ok(requests)
    }

    /// Test-only hook that fails the next node runtime persistence attempt.
    #[cfg(test)]
    pub(crate) fn fail_next_persist_for_test(&self) {
        self.fail_next_persist.store(true, std::sync::atomic::Ordering::Release);
    }

    // Re-canonicalizes the root and rejects any join that escapes it on every
    // write: defense in depth against the state root being swapped for a
    // symlink between load and persist.
    fn persist_locked(&self, persisted: &PersistedNodeRuntimeState) -> Result<(), Status> {
        #[cfg(test)]
        if self.fail_next_persist.swap(false, std::sync::atomic::Ordering::AcqRel) {
            return Err(Status::internal("injected node runtime persistence failure"));
        }

        let encoded = serde_json::to_vec_pretty(persisted).map_err(|error| {
            Status::internal(format!("failed to encode node runtime state: {error}"))
        })?;
        let canonical_state_root = fs::canonicalize(&self.state_root).map_err(|error| {
            Status::internal(format!(
                "failed to canonicalize node runtime state dir {}: {error}",
                self.state_root.display()
            ))
        })?;
        let state_path = canonical_state_root.join(NODE_RUNTIME_STATE_FILE_NAME);
        let parent = state_path
            .parent()
            .ok_or_else(|| Status::internal("node runtime state path has no parent"))?;
        if parent != canonical_state_root {
            return Err(Status::internal(
                "node runtime state path escapes the canonical state root",
            ));
        }
        fs::write(&state_path, encoded).map_err(|error| {
            Status::internal(format!(
                "failed to write node runtime state {}: {error}",
                state_path.display()
            ))
        })
    }
}

fn resolve_canonical_state_root(state_root: &Path) -> Result<PathBuf> {
    anyhow::ensure!(
        !state_root.as_os_str().is_empty(),
        "node runtime state root must not be empty"
    );
    fs::create_dir_all(state_root).with_context(|| {
        format!("failed to create node runtime state dir {}", state_root.display())
    })?;
    fs::canonicalize(state_root).with_context(|| {
        format!("failed to canonicalize node runtime state dir {}", state_root.display())
    })
}

fn generate_pairing_code(method: PairingCodeMethod) -> String {
    match method {
        PairingCodeMethod::Pin => {
            // Six digits derived by hashing a fresh random ULID; the modulo
            // bias of 2^32 % 1_000_000 is negligible for a short-lived,
            // single-use code that still requires operator approval.
            let digest = sha2::Sha256::digest(Ulid::generate().to_string().as_bytes());
            let value =
                u32::from_be_bytes([digest[0], digest[1], digest[2], digest[3]]) % 1_000_000;
            format!("{value:06}")
        }
        PairingCodeMethod::Qr => Ulid::generate().to_string(),
    }
}

fn normalize_pairing_code_ttl_ms(value: Option<u64>) -> u64 {
    value
        .unwrap_or(DEFAULT_PAIRING_CODE_TTL_MS)
        .clamp(MIN_PAIRING_CODE_TTL_MS, MAX_PAIRING_CODE_TTL_MS)
}

// Expired codes are deleted, but expired pairing requests are only flipped to
// `Expired` so the operator-facing decision trail remains auditable.
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

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = sha2::Sha256::digest(bytes);
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

fn mint_networked_worker_delivery_token() -> String {
    let token_bytes: [u8; NETWORKED_WORKER_DELIVERY_TOKEN_BYTES] = rand::random();
    URL_SAFE_NO_PAD.encode(token_bytes)
}

fn remove_queued_capability(
    capabilities: &mut CapabilityRuntimeState,
    device_id: &str,
    request_id: &str,
) -> Result<(), Status> {
    let queue = capabilities
        .queued_by_device
        .get_mut(device_id)
        .ok_or_else(|| Status::aborted("capability queue changed during dispatch"))?;
    let removed =
        queue.pop_front().ok_or_else(|| Status::aborted("capability queue head disappeared"))?;
    if removed.request_id != request_id {
        return Err(Status::aborted("capability queue head changed during dispatch"));
    }
    if queue.is_empty() {
        capabilities.queued_by_device.remove(device_id);
    }
    Ok(())
}

/// Current wall-clock time as Unix milliseconds.
///
/// # Errors
/// Returns `Status::internal` when the system clock reports a time before the
/// Unix epoch or the value overflows `i64`.
pub(crate) fn current_unix_ms() -> Result<i64, Status> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| Status::internal(format!("system clock error: {error}")))?;
    i64::try_from(duration.as_millis()).map_err(|_| Status::internal("timestamp overflow"))
}

// Persisted summaries pass through auth/URL redaction and are capped at 240
// chars so node payloads can never leak credentials into runtime state.
fn normalize_summary_text(raw: &str) -> Option<String> {
    let trimmed = redact_url_segments_in_text(&redact_auth_error(raw)).trim().to_owned();
    if trimmed.is_empty() {
        None
    } else if trimmed.chars().count() > 240 {
        Some(format!("{}...", trimmed.chars().take(237).collect::<String>()))
    } else {
        Some(trimmed)
    }
}

fn summarize_payload_bytes(payload_json: &[u8]) -> Option<String> {
    if payload_json.is_empty() {
        return None;
    }
    let redacted = crate::journal::redact_payload_json(payload_json)
        .unwrap_or_else(|_| String::from_utf8_lossy(payload_json).into_owned());
    normalize_summary_text(redacted.as_str())
}

/// Parses a `capability.result` node event payload into its request id and
/// execution result; absent `success`/`error`/`output_json` fields default to
/// a failed, empty result.
///
/// # Errors
/// Returns `Status::invalid_argument` for malformed JSON or a missing/empty
/// `request_id`.
pub(crate) fn parse_capability_result_payload(
    payload_json: &[u8],
) -> Result<(String, Option<String>, Option<RuntimeGeneration>, CapabilityExecutionResult), Status>
{
    let value: Value = serde_json::from_slice(payload_json).map_err(|error| {
        Status::invalid_argument(format!("invalid capability result payload: {error}"))
    })?;
    let request_id = parse_capability_request_id(&value)?;
    let delivery_attempt_id = value
        .get("delivery_attempt_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|candidate| !candidate.is_empty())
        .map(ToOwned::to_owned);
    let run_generation = value
        .get("run_generation")
        .map(|generation| {
            let generation = generation.as_u64().ok_or_else(|| {
                Status::invalid_argument(
                    "capability result run_generation must be an unsigned integer",
                )
            })?;
            RuntimeGeneration::new(generation).map_err(|_| {
                Status::invalid_argument(
                    "capability result run_generation must be greater than zero",
                )
            })
        })
        .transpose()?;
    let success = value.get("success").and_then(Value::as_bool).unwrap_or(false);
    let error = value.get("error").and_then(Value::as_str).unwrap_or_default().to_owned();
    let output_json = value
        .get("output_json")
        .map(|inner| serde_json::to_vec(inner).unwrap_or_default())
        .unwrap_or_default();
    Ok((
        request_id,
        delivery_attempt_id,
        run_generation,
        CapabilityExecutionResult { success, output_json, error },
    ))
}

/// Extracts the `request_id` from a capability lifecycle event payload.
///
/// # Errors
/// Returns `Status::invalid_argument` for malformed JSON or a missing/empty
/// `request_id`.
pub(crate) fn parse_capability_request_id_payload(payload_json: &[u8]) -> Result<String, Status> {
    let value: Value = serde_json::from_slice(payload_json).map_err(|error| {
        Status::invalid_argument(format!("invalid capability lifecycle payload: {error}"))
    })?;
    parse_capability_request_id(&value)
}

fn authorize_capability_request_owner(
    request: &CapabilityRequestRecord,
    reporting_device_id: &str,
) -> Result<(), Status> {
    if request.device_id == reporting_device_id {
        Ok(())
    } else {
        Err(Status::permission_denied("capability request is not authorized for this node"))
    }
}

fn parse_capability_request_id(value: &Value) -> Result<String, Status> {
    value
        .get("request_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|candidate| !candidate.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| Status::invalid_argument("capability payload missing request_id"))
}

#[cfg(test)]
mod tests {
    use super::{
        resolve_canonical_state_root, CapabilityDispatchAuthorizer, DevicePairingMaterialRecord,
        NetworkedWorkerResultAuthorizationOutcome, NodeRuntimeState, NODE_RUNTIME_STATE_FILE_NAME,
    };
    use crate::journal::{
        NetworkedWorkerDeliveryReservationOutcome,
        NetworkedWorkerDispatchAbortBeforeReleaseOutcome, NetworkedWorkerDispatchCancelOutcome,
        NetworkedWorkerPayloadAcknowledgementOutcome, NetworkedWorkerPayloadReleaseOutcome,
    };
    use palyra_common::runtime_contracts::RuntimeGeneration;
    use tonic::{Code, Status};
    use ulid::Ulid;

    struct TestClaimAuthorizer {
        result_outcome: NetworkedWorkerResultAuthorizationOutcome,
        begin_outcome: NetworkedWorkerDeliveryReservationOutcome,
        abort_outcome: NetworkedWorkerDispatchAbortBeforeReleaseOutcome,
    }

    impl TestClaimAuthorizer {
        const REJECTING: Self = Self {
            result_outcome: NetworkedWorkerResultAuthorizationOutcome::Rejected,
            begin_outcome: NetworkedWorkerDeliveryReservationOutcome::Rejected,
            abort_outcome: NetworkedWorkerDispatchAbortBeforeReleaseOutcome::Aborted,
        };

        const fn dispatching(
            abort_outcome: NetworkedWorkerDispatchAbortBeforeReleaseOutcome,
        ) -> Self {
            Self {
                result_outcome: NetworkedWorkerResultAuthorizationOutcome::Rejected,
                begin_outcome: NetworkedWorkerDeliveryReservationOutcome::Authorized {
                    fleet_generation: 7,
                },
                abort_outcome,
            }
        }
    }

    impl CapabilityDispatchAuthorizer for TestClaimAuthorizer {
        fn reserve_networked_worker_delivery(
            &self,
            _request: &crate::journal::NetworkedWorkerDeliveryReservationRequest,
        ) -> Result<NetworkedWorkerDeliveryReservationOutcome, Status> {
            Ok(self.begin_outcome)
        }

        fn release_networked_worker_payload(
            &self,
            _request: &crate::journal::NetworkedWorkerPayloadReleaseRequest,
        ) -> Result<NetworkedWorkerPayloadReleaseOutcome, Status> {
            Ok(NetworkedWorkerPayloadReleaseOutcome::Released)
        }

        fn acknowledge_networked_worker_payload(
            &self,
            _request: &crate::journal::NetworkedWorkerPayloadAcknowledgementRequest,
        ) -> Result<NetworkedWorkerPayloadAcknowledgementOutcome, Status> {
            Ok(NetworkedWorkerPayloadAcknowledgementOutcome::Acknowledged)
        }

        fn abort_networked_worker_dispatch_before_payload_release(
            &self,
            _remote_request_id: &str,
            _node_request_id: &str,
            _request_sha256: &str,
            _dispatch_fleet_generation: u64,
            _observed_at_unix_ms: i64,
        ) -> Result<NetworkedWorkerDispatchAbortBeforeReleaseOutcome, Status> {
            Ok(self.abort_outcome)
        }

        fn cancel_networked_worker_dispatch(
            &self,
            _remote_request_id: &str,
            _node_request_id: &str,
            _reason_code: &str,
            _observed_at_unix_ms: i64,
        ) -> Result<NetworkedWorkerDispatchCancelOutcome, Status> {
            Ok(NetworkedWorkerDispatchCancelOutcome::Cancelled)
        }

        fn authorize_networked_worker_result(
            &self,
            _remote_request_id: &str,
            _node_request_id: &str,
            _delivery_attempt_id: &str,
            _run_generation: RuntimeGeneration,
            _reporting_worker_id: &str,
            _observed_at_unix_ms: i64,
        ) -> Result<NetworkedWorkerResultAuthorizationOutcome, Status> {
            Ok(self.result_outcome)
        }

        fn commit_networked_worker_result(
            &self,
            _request: &super::NetworkedWorkerResultCommitRequest,
        ) -> Result<super::NetworkedWorkerResultCommitOutcome, Status> {
            Ok(super::NetworkedWorkerResultCommitOutcome::Rejected)
        }
    }
    use tempfile::tempdir;

    #[test]
    fn canonical_state_path_resolves_inside_canonical_root() {
        let tempdir = tempdir().expect("temp dir should be created");
        let state_root = tempdir.path().join("runtime");
        std::fs::create_dir_all(&state_root).expect("state root should be created");

        let canonical_root =
            resolve_canonical_state_root(state_root.as_path()).expect("root should resolve");
        let state_path = canonical_root.join(NODE_RUNTIME_STATE_FILE_NAME);

        assert_eq!(state_path, canonical_root.join(NODE_RUNTIME_STATE_FILE_NAME));
    }

    #[test]
    fn node_runtime_load_rejects_empty_state_root() {
        let error = NodeRuntimeState::load(std::path::Path::new(""))
            .expect_err("empty state root must fail");

        assert!(error.to_string().contains("must not be empty"), "unexpected error: {error}");
    }

    #[test]
    fn pairing_material_omits_private_key_when_serialized() {
        let raw = serde_json::json!({
            "identity_fingerprint": "fingerprint",
            "transcript_hash_hex": "transcript",
            "mtls_client_certificate_pem": "CERT",
            "mtls_client_private_key_pem": "PRIVATE KEY",
            "gateway_ca_certificate_pem": "CA",
            "cert_expires_at_unix_ms": 42
        });

        let material: DevicePairingMaterialRecord =
            serde_json::from_value(raw).expect("legacy pairing material should deserialize");
        assert_eq!(material.mtls_client_private_key_pem, "PRIVATE KEY");

        let encoded =
            serde_json::to_value(&material).expect("pairing material should serialize safely");
        assert!(
            encoded.get("mtls_client_private_key_pem").is_none(),
            "runtime state serialization must not persist private keys"
        );
    }

    #[test]
    fn capability_request_lifecycle_tracks_queue_dispatch_completion_and_timeout() {
        let tempdir = tempdir().expect("temp dir should be created");
        let runtime =
            NodeRuntimeState::load(tempdir.path()).expect("node runtime should initialize cleanly");
        runtime
            .register_node(
                "01ARZ3NDEKTSV4RRFFQ69G5FAZ",
                "windows-x86_64",
                vec![super::DeviceCapabilityView {
                    name: "system.health".to_owned(),
                    available: true,
                }],
            )
            .expect("node should register");

        let (request_id, _receiver) = runtime
            .enqueue_capability_request(
                "01ARZ3NDEKTSV4RRFFQ69G5FAZ",
                "system.health",
                br#"{"secret":"value","ok":true}"#.to_vec(),
                4096,
                Some(30_000),
            )
            .expect("capability request should queue");
        let queued = runtime.capability_requests(Some("01ARZ3NDEKTSV4RRFFQ69G5FAZ")).unwrap();
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].request_id, request_id);
        assert!(matches!(queued[0].state, super::CapabilityRequestState::Queued));
        assert_eq!(queued[0].capability, "system.health");
        assert_eq!(queued[0].max_payload_bytes, 4096);
        assert!(queued[0].input_summary.is_some());
        assert!(
            !queued[0].input_summary.as_deref().unwrap_or_default().contains("value"),
            "payload summary must remain redacted"
        );

        let dispatched = runtime
            .next_capability_dispatch("01ARZ3NDEKTSV4RRFFQ69G5FAZ", &TestClaimAuthorizer::REJECTING)
            .expect("dispatch should succeed")
            .expect("request should dispatch");
        assert_eq!(dispatched.request_id, request_id);
        let dispatched_state =
            runtime.capability_requests(Some("01ARZ3NDEKTSV4RRFFQ69G5FAZ")).unwrap();
        assert!(matches!(dispatched_state[0].state, super::CapabilityRequestState::Dispatched));
        assert!(dispatched_state[0].dispatched_at_unix_ms.is_some());

        runtime
            .complete_capability_request(
                "01ARZ3NDEKTSV4RRFFQ69G5FAZ",
                request_id.as_str(),
                None,
                None,
                super::CapabilityExecutionResult {
                    success: true,
                    output_json: br#"{"status":"ok"}"#.to_vec(),
                    error: String::new(),
                },
                &TestClaimAuthorizer::REJECTING,
            )
            .expect("request completion should succeed");
        let completed = runtime.capability_requests(Some("01ARZ3NDEKTSV4RRFFQ69G5FAZ")).unwrap();
        assert!(matches!(completed[0].state, super::CapabilityRequestState::Succeeded));
        assert_eq!(completed[0].output_summary.as_deref(), Some("{\"status\":\"ok\"}"));
        assert!(completed[0].completed_at_unix_ms.is_some());

        let (timeout_request_id, _timeout_receiver) = runtime
            .enqueue_capability_request(
                "01ARZ3NDEKTSV4RRFFQ69G5FAZ",
                "desktop.open_url",
                br#"{"url":"https://example.com/secret/path"}"#.to_vec(),
                4096,
                Some(30_000),
            )
            .expect("timeout request should queue");
        assert_eq!(
            runtime
                .mark_capability_timeout(timeout_request_id.as_str())
                .expect("timeout should mark request"),
            super::CapabilityRequestTimeoutOutcome::MarkedTimedOut
        );
        let requests = runtime.capability_requests(Some("01ARZ3NDEKTSV4RRFFQ69G5FAZ")).unwrap();
        let timeout = requests
            .iter()
            .find(|record| record.request_id == timeout_request_id)
            .expect("timed-out request should remain visible");
        assert!(matches!(timeout.state, super::CapabilityRequestState::TimedOut));
        assert_eq!(timeout.error.as_deref(), Some("timed out waiting for node capability result"));
        assert!(timeout.completed_at_unix_ms.is_some());

        let (cancelled_request_id, _cancelled_receiver) = runtime
            .enqueue_capability_request(
                "01ARZ3NDEKTSV4RRFFQ69G5FAZ",
                "desktop.open_url",
                br#"{"url":"https://example.com/cancelled"}"#.to_vec(),
                4096,
                Some(30_000),
            )
            .expect("cancelled request should queue");
        assert!(runtime
            .cancel_queued_capability_request(
                cancelled_request_id.as_str(),
                "cancelled by parent run",
                None,
            )
            .expect("queued cancellation should succeed"));
        assert!(runtime
            .next_capability_dispatch("01ARZ3NDEKTSV4RRFFQ69G5FAZ", &TestClaimAuthorizer::REJECTING)
            .expect("dispatch poll should succeed")
            .is_some_and(|dispatch| dispatch.request_id == timeout_request_id));
        assert!(runtime
            .next_capability_dispatch("01ARZ3NDEKTSV4RRFFQ69G5FAZ", &TestClaimAuthorizer::REJECTING)
            .expect("dispatch poll should succeed")
            .is_none());
        let requests = runtime.capability_requests(Some("01ARZ3NDEKTSV4RRFFQ69G5FAZ")).unwrap();
        let cancelled = requests
            .iter()
            .find(|record| record.request_id == cancelled_request_id)
            .expect("cancelled request should remain visible");
        assert!(matches!(cancelled.state, super::CapabilityRequestState::Cancelled));
        assert_eq!(cancelled.error.as_deref(), Some("cancelled by parent run"));

        let (mediation_request_id, _mediation_receiver) = runtime
            .enqueue_capability_request(
                "01ARZ3NDEKTSV4RRFFQ69G5FAZ",
                "desktop.open_url",
                br#"{"url":"https://example.com/open"}"#.to_vec(),
                4096,
                Some(30_000),
            )
            .expect("mediation request should queue");
        runtime
            .next_capability_dispatch("01ARZ3NDEKTSV4RRFFQ69G5FAZ", &TestClaimAuthorizer::REJECTING)
            .expect("dispatch should work")
            .expect("mediation request should dispatch");
        runtime
            .mark_capability_awaiting_local_mediation(
                "01ARZ3NDEKTSV4RRFFQ69G5FAZ",
                mediation_request_id.as_str(),
            )
            .expect("mediation state should persist");
        let requests = runtime.capability_requests(Some("01ARZ3NDEKTSV4RRFFQ69G5FAZ")).unwrap();
        let mediation = requests
            .iter()
            .find(|record| record.request_id == mediation_request_id)
            .expect("mediation request should remain visible");
        assert!(matches!(mediation.state, super::CapabilityRequestState::AwaitingLocalMediation));
        assert!(mediation.dispatched_at_unix_ms.is_some());
    }

    fn queued_worker_claim(
        worker_id: &str,
        capability: &str,
        input_json: &[u8],
    ) -> crate::journal::NetworkedWorkerDispatchClaim {
        let now = super::current_unix_ms().expect("clock should be available");
        crate::journal::NetworkedWorkerDispatchClaim {
            schema_version: 3,
            remote_request_id: Ulid::generate().to_string(),
            node_request_id: Ulid::generate().to_string(),
            worker_id: worker_id.to_owned(),
            lease_id: Ulid::generate().to_string(),
            session_id: Some(Ulid::generate().to_string()),
            run_id: Ulid::generate().to_string(),
            run_generation: Some(
                RuntimeGeneration::new(1).expect("test generation should be valid"),
            ),
            issued_fleet_generation: 7,
            dispatch_fleet_generation: None,
            revoked_fleet_generation: None,
            lease_expires_at_unix_ms: now.saturating_add(30_000),
            capability: capability.to_owned(),
            request_sha256: super::sha256_hex(input_json),
            state: crate::journal::NetworkedWorkerDispatchClaimState::Queued,
            delivery_attempt_id: None,
            delivery_token_sha256: None,
            delivery_reserved_at_unix_ms: None,
            payload_released_at_unix_ms: None,
            payload_release_fleet_generation: None,
            payload_acknowledged_at_unix_ms: None,
            delivery_disposition: None,
            delivery_payload_present: Some(true),
            validated_result_sha256: None,
            result_observed_at_unix_ms: None,
            reconciliation_disposition: None,
            terminal_reason_code: None,
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
            completed_at_unix_ms: None,
        }
    }

    #[test]
    fn released_worker_result_and_timeout_are_linearized_by_capability_lock() {
        const WORKER_ID: &str = "worker-result-timeout-linearization";
        const CAPABILITY: &str = "tool:palyra.echo";

        let tempdir = tempdir().expect("temp dir should be created");
        let runtime =
            NodeRuntimeState::load(tempdir.path()).expect("node runtime should initialize cleanly");
        runtime
            .register_node(
                WORKER_ID,
                "test-worker",
                vec![super::DeviceCapabilityView { name: CAPABILITY.to_owned(), available: true }],
            )
            .expect("worker node should register");
        let input_json = br#"{"message":"linearize"}"#.to_vec();
        let claim = queued_worker_claim(WORKER_ID, CAPABILITY, input_json.as_slice());
        let mut receiver = runtime
            .enqueue_claimed_capability_request_for_test(
                WORKER_ID, CAPABILITY, input_json, 4_096, &claim,
            )
            .expect("claimed payload should queue");
        let authorizer = TestClaimAuthorizer {
            result_outcome: NetworkedWorkerResultAuthorizationOutcome::Authorized,
            begin_outcome: NetworkedWorkerDeliveryReservationOutcome::Authorized {
                fleet_generation: 7,
            },
            abort_outcome: NetworkedWorkerDispatchAbortBeforeReleaseOutcome::NotAbortable,
        };
        let dispatch = runtime
            .next_capability_dispatch(WORKER_ID, &authorizer)
            .expect("dispatch poll should succeed")
            .expect("worker request should dispatch");
        let reservation =
            dispatch.networked_worker_reservation.expect("worker dispatch should reserve delivery");
        runtime
            .fetch_networked_worker_payload(
                WORKER_ID,
                dispatch.request_id.as_str(),
                reservation.delivery_attempt_id.as_str(),
                reservation.fetch_token.as_str(),
                &authorizer,
            )
            .expect("worker payload should release");
        runtime
            .complete_capability_request(
                WORKER_ID,
                dispatch.request_id.as_str(),
                Some(reservation.delivery_attempt_id.as_str()),
                Some(reservation.run_generation),
                super::CapabilityExecutionResult {
                    success: true,
                    output_json: br#"{"status":"ok"}"#.to_vec(),
                    error: String::new(),
                },
                &authorizer,
            )
            .expect("worker result should commit");

        assert_eq!(
            runtime
                .stop_capability_request(
                    dispatch.request_id.as_str(),
                    "caller cancelled after result commit",
                    Some(&authorizer),
                )
                .expect("stop classification should load committed state"),
            super::CapabilityRequestStopOutcome::AlreadyTerminal
        );
        assert_eq!(
            runtime
                .mark_capability_timeout(dispatch.request_id.as_str())
                .expect("timeout classification should load committed state"),
            super::CapabilityRequestTimeoutOutcome::ResultCommitted
        );
        let notification = receiver.try_recv().expect("committed result should remain deliverable");
        assert!(notification.result.success);
        assert_eq!(
            notification.delivery_attempt_id.as_deref(),
            Some(reservation.delivery_attempt_id.as_str())
        );
        let request = runtime
            .capability_requests(Some(WORKER_ID))
            .expect("request audit should load")
            .into_iter()
            .find(|request| request.request_id == dispatch.request_id)
            .expect("completed request should remain auditable");
        assert_eq!(notification.observed_at_unix_ms, request.completed_at_unix_ms.unwrap());
    }

    #[test]
    fn undelivered_worker_reservation_is_cancelled_before_payload_release() {
        const WORKER_ID: &str = "worker-undelivered-reservation";
        const CAPABILITY: &str = "tool:palyra.echo";

        let tempdir = tempdir().expect("temp dir should be created");
        let runtime =
            NodeRuntimeState::load(tempdir.path()).expect("node runtime should initialize cleanly");
        runtime
            .register_node(
                WORKER_ID,
                "test-worker",
                vec![super::DeviceCapabilityView { name: CAPABILITY.to_owned(), available: true }],
            )
            .expect("worker node should register");
        let input_json = br#"{"message":"withhold on closed stream"}"#.to_vec();
        let claim = queued_worker_claim(WORKER_ID, CAPABILITY, input_json.as_slice());
        let mut receiver = runtime
            .enqueue_claimed_capability_request_for_test(
                WORKER_ID, CAPABILITY, input_json, 4_096, &claim,
            )
            .expect("claimed payload should queue");
        let authorizer = TestClaimAuthorizer::dispatching(
            NetworkedWorkerDispatchAbortBeforeReleaseOutcome::Aborted,
        );
        let dispatch = runtime
            .next_capability_dispatch(WORKER_ID, &authorizer)
            .expect("dispatch poll should succeed")
            .expect("worker reservation should be produced");
        let reservation = dispatch
            .networked_worker_reservation
            .as_ref()
            .expect("worker dispatch should contain a reservation");

        assert_eq!(
            runtime
                .recover_undelivered_capability_dispatch(&dispatch, &authorizer)
                .expect("undelivered reservation should recover"),
            super::CapabilityRequestStopOutcome::CancelledBeforeRelease
        );
        let fetch_error = runtime
            .fetch_networked_worker_payload(
                WORKER_ID,
                dispatch.request_id.as_str(),
                reservation.delivery_attempt_id.as_str(),
                reservation.fetch_token.as_str(),
                &authorizer,
            )
            .expect_err("cancelled undelivered reservation must release no bytes");
        assert_eq!(fetch_error.code(), Code::FailedPrecondition);
        assert!(receiver.try_recv().is_none());
        let request = runtime
            .capability_requests(Some(WORKER_ID))
            .expect("request audit should load")
            .into_iter()
            .find(|request| request.request_id == claim.node_request_id)
            .expect("request audit should remain visible");
        assert!(matches!(request.state, super::CapabilityRequestState::Cancelled));
        assert!(request
            .error
            .as_deref()
            .is_some_and(|error| error.contains("closed before dispatch delivery")));
    }

    #[test]
    fn claimed_dispatch_persistence_failure_withholds_payload_and_drops_volatile_authority() {
        const WORKER_ID: &str = "worker-local-audit-failure";

        let tempdir = tempdir().expect("temp dir should be created");
        let runtime =
            NodeRuntimeState::load(tempdir.path()).expect("node runtime should initialize cleanly");
        runtime
            .register_node(
                WORKER_ID,
                "test-worker",
                vec![super::DeviceCapabilityView {
                    name: "tool:palyra.echo".to_owned(),
                    available: true,
                }],
            )
            .expect("worker node should register");
        let input_json = br#"{"message":"never release"}"#.to_vec();
        let claim = queued_worker_claim(WORKER_ID, "tool:palyra.echo", input_json.as_slice());
        let mut receiver = runtime
            .enqueue_claimed_capability_request_for_test(
                WORKER_ID,
                claim.capability.as_str(),
                input_json,
                4_096,
                &claim,
            )
            .expect("claimed payload should queue");

        runtime.fail_next_persist_for_test();
        let error = runtime
            .next_capability_dispatch(
                WORKER_ID,
                &TestClaimAuthorizer::dispatching(
                    NetworkedWorkerDispatchAbortBeforeReleaseOutcome::Aborted,
                ),
            )
            .expect_err("failed local audit persistence must withhold the payload");
        assert_eq!(error.code(), Code::Internal);
        assert!(error.message().contains("injected node runtime persistence failure"));
        assert!(runtime
            .next_capability_dispatch(WORKER_ID, &TestClaimAuthorizer::REJECTING)
            .expect("subsequent dispatch poll should succeed")
            .is_none());
        assert!(receiver.try_recv().is_none());
        let request = runtime
            .capability_requests(Some(WORKER_ID))
            .expect("request audit should load")
            .into_iter()
            .find(|request| request.request_id == claim.node_request_id)
            .expect("request audit should remain visible");
        assert!(matches!(request.state, super::CapabilityRequestState::Cancelled));
        assert!(request.dispatched_at_unix_ms.is_none());
        assert!(request.completed_at_unix_ms.is_some());
        assert!(request
            .error
            .as_deref()
            .is_some_and(|error| error.contains("durable dispatch authority cancelled")));
    }

    #[test]
    fn claimed_dispatch_inexact_rollback_drops_payload_and_marks_reconciliation_required() {
        const WORKER_ID: &str = "worker-local-audit-uncertain";

        let tempdir = tempdir().expect("temp dir should be created");
        let runtime =
            NodeRuntimeState::load(tempdir.path()).expect("node runtime should initialize cleanly");
        runtime
            .register_node(
                WORKER_ID,
                "test-worker",
                vec![super::DeviceCapabilityView {
                    name: "tool:palyra.echo".to_owned(),
                    available: true,
                }],
            )
            .expect("worker node should register");
        let input_json = br#"{"message":"uncertain authority"}"#.to_vec();
        let claim = queued_worker_claim(WORKER_ID, "tool:palyra.echo", input_json.as_slice());
        let mut receiver = runtime
            .enqueue_claimed_capability_request_for_test(
                WORKER_ID,
                claim.capability.as_str(),
                input_json,
                4_096,
                &claim,
            )
            .expect("claimed payload should queue");

        runtime.fail_next_persist_for_test();
        let error = runtime
            .next_capability_dispatch(
                WORKER_ID,
                &TestClaimAuthorizer::dispatching(
                    NetworkedWorkerDispatchAbortBeforeReleaseOutcome::NotAbortable,
                ),
            )
            .expect_err("inexact rollback must fail closed");
        assert_eq!(error.code(), Code::FailedPrecondition);
        assert!(error.message().contains("rollback was not exact"));
        assert!(runtime
            .next_capability_dispatch(WORKER_ID, &TestClaimAuthorizer::REJECTING)
            .expect("subsequent dispatch poll should succeed")
            .is_none());
        assert!(receiver.try_recv().is_none());
        let request = runtime
            .capability_requests(Some(WORKER_ID))
            .expect("request audit should load")
            .into_iter()
            .find(|request| request.request_id == claim.node_request_id)
            .expect("request audit should remain visible");
        assert!(matches!(request.state, super::CapabilityRequestState::Failed));
        assert!(request.dispatched_at_unix_ms.is_none());
        assert!(request.completed_at_unix_ms.is_some());
        assert!(request
            .error
            .as_deref()
            .is_some_and(|error| error.contains("requires reconciliation")));
    }

    #[test]
    fn capability_result_and_mediation_require_authenticated_request_owner() {
        const OWNER_DEVICE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAZ";
        const OTHER_DEVICE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAY";

        let tempdir = tempdir().expect("temp dir should be created");
        let runtime =
            NodeRuntimeState::load(tempdir.path()).expect("node runtime should initialize cleanly");
        for device_id in [OWNER_DEVICE_ID, OTHER_DEVICE_ID] {
            runtime
                .register_node(
                    device_id,
                    "windows-x86_64",
                    vec![super::DeviceCapabilityView {
                        name: "system.health".to_owned(),
                        available: true,
                    }],
                )
                .expect("node should register");
        }

        let (request_id, mut receiver) = runtime
            .enqueue_capability_request(
                OWNER_DEVICE_ID,
                "system.health",
                br#"{"ok":true}"#.to_vec(),
                4096,
                Some(30_000),
            )
            .expect("capability request should queue");
        runtime
            .next_capability_dispatch(OWNER_DEVICE_ID, &TestClaimAuthorizer::REJECTING)
            .expect("dispatch should succeed")
            .expect("request should dispatch");

        let denial = runtime
            .complete_capability_request(
                OTHER_DEVICE_ID,
                request_id.as_str(),
                None,
                None,
                super::CapabilityExecutionResult {
                    success: true,
                    output_json: br#"{"status":"forged"}"#.to_vec(),
                    error: String::new(),
                },
                &TestClaimAuthorizer::REJECTING,
            )
            .expect_err("another node must not complete the request");
        assert_eq!(denial.code(), Code::PermissionDenied);
        assert!(matches!(
            runtime.capability_requests(Some(OWNER_DEVICE_ID)).expect("request audit should load")
                [0]
            .state,
            super::CapabilityRequestState::Dispatched
        ));
        assert!(receiver.try_recv().is_none());

        let mediation_denial = runtime
            .mark_capability_awaiting_local_mediation(OTHER_DEVICE_ID, request_id.as_str())
            .expect_err("another node must not alter mediation state");
        assert_eq!(mediation_denial.code(), Code::PermissionDenied);
        assert!(matches!(
            runtime.capability_requests(Some(OWNER_DEVICE_ID)).expect("request audit should load")
                [0]
            .state,
            super::CapabilityRequestState::Dispatched
        ));

        runtime
            .mark_capability_awaiting_local_mediation(OWNER_DEVICE_ID, request_id.as_str())
            .expect("request owner should enter mediation");
        runtime
            .complete_capability_request(
                OWNER_DEVICE_ID,
                request_id.as_str(),
                None,
                None,
                super::CapabilityExecutionResult {
                    success: true,
                    output_json: br#"{"status":"ok"}"#.to_vec(),
                    error: String::new(),
                },
                &TestClaimAuthorizer::REJECTING,
            )
            .expect("request owner should complete the request");
        let notification = receiver.try_recv().expect("owner result should reach original waiter");
        assert_eq!(notification.result.output_json, br#"{"status":"ok"}"#);
        assert!(notification.delivery_attempt_id.is_none());
        assert!(matches!(
            runtime.capability_requests(Some(OWNER_DEVICE_ID)).expect("request audit should load")
                [0]
            .state,
            super::CapabilityRequestState::Succeeded
        ));
    }

    #[test]
    fn committed_capability_result_wins_timeout_marking() {
        const OWNER_DEVICE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAZ";

        let tempdir = tempdir().expect("temp dir should be created");
        let runtime =
            NodeRuntimeState::load(tempdir.path()).expect("node runtime should initialize cleanly");
        runtime
            .register_node(
                OWNER_DEVICE_ID,
                "windows-x86_64",
                vec![super::DeviceCapabilityView {
                    name: "system.health".to_owned(),
                    available: true,
                }],
            )
            .expect("node should register");
        let (request_id, mut receiver) = runtime
            .enqueue_capability_request(
                OWNER_DEVICE_ID,
                "system.health",
                br#"{"ok":true}"#.to_vec(),
                4096,
                Some(30_000),
            )
            .expect("capability request should queue");
        runtime
            .next_capability_dispatch(OWNER_DEVICE_ID, &TestClaimAuthorizer::REJECTING)
            .expect("dispatch should succeed")
            .expect("request should dispatch");
        runtime
            .complete_capability_request(
                OWNER_DEVICE_ID,
                request_id.as_str(),
                None,
                None,
                super::CapabilityExecutionResult {
                    success: true,
                    output_json: br#"{"status":"ok"}"#.to_vec(),
                    error: String::new(),
                },
                &TestClaimAuthorizer::REJECTING,
            )
            .expect("request owner should complete the request");

        assert_eq!(
            runtime
                .mark_capability_timeout(request_id.as_str())
                .expect("timeout classification should load committed state"),
            super::CapabilityRequestTimeoutOutcome::ResultCommitted
        );
        let notification = receiver.try_recv().expect("committed result should remain deliverable");
        assert!(notification.result.success);
        assert_eq!(notification.result.output_json, br#"{"status":"ok"}"#);
        let request = runtime
            .capability_requests(Some(OWNER_DEVICE_ID))
            .expect("request audit should load")
            .into_iter()
            .find(|request| request.request_id == request_id)
            .expect("completed request should remain auditable");
        assert!(matches!(request.state, super::CapabilityRequestState::Succeeded));
        assert_eq!(notification.observed_at_unix_ms, request.completed_at_unix_ms.unwrap());
    }

    #[test]
    fn capability_request_accepts_owned_late_result_after_timeout() {
        const OWNER_DEVICE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAZ";

        let tempdir = tempdir().expect("temp dir should be created");
        let runtime =
            NodeRuntimeState::load(tempdir.path()).expect("node runtime should initialize cleanly");
        runtime
            .register_node(
                OWNER_DEVICE_ID,
                "windows-x86_64",
                vec![super::DeviceCapabilityView {
                    name: "system.health".to_owned(),
                    available: true,
                }],
            )
            .expect("node should register");
        let (request_id, mut receiver) = runtime
            .enqueue_capability_request(
                OWNER_DEVICE_ID,
                "system.health",
                br#"{"ok":true}"#.to_vec(),
                4096,
                Some(30_000),
            )
            .expect("capability request should queue");
        runtime
            .next_capability_dispatch(OWNER_DEVICE_ID, &TestClaimAuthorizer::REJECTING)
            .expect("dispatch should succeed")
            .expect("request should dispatch");
        assert_eq!(
            runtime
                .mark_capability_timeout(request_id.as_str())
                .expect("request timeout should persist"),
            super::CapabilityRequestTimeoutOutcome::MarkedTimedOut
        );

        runtime
            .complete_capability_request(
                OWNER_DEVICE_ID,
                request_id.as_str(),
                None,
                None,
                super::CapabilityExecutionResult {
                    success: false,
                    output_json: Vec::new(),
                    error: "worker failed after timeout".to_owned(),
                },
                &TestClaimAuthorizer::REJECTING,
            )
            .expect("owned late result should update audit evidence");
        let notification = receiver.try_recv().expect("late result should reach retained waiter");
        assert!(!notification.result.success);
        assert!(notification.delivery_attempt_id.is_none());
        let request = runtime
            .capability_requests(Some(OWNER_DEVICE_ID))
            .expect("request audit should load")
            .into_iter()
            .find(|request| request.request_id == request_id)
            .expect("late-result request should remain auditable");
        assert!(matches!(request.state, super::CapabilityRequestState::Failed));
        assert_eq!(request.error.as_deref(), Some("worker failed after timeout"));
        assert_eq!(notification.observed_at_unix_ms, request.completed_at_unix_ms.unwrap());
    }

    #[test]
    fn capability_result_persistence_failure_preserves_owner_for_exact_retry() {
        const OWNER_DEVICE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAZ";

        let tempdir = tempdir().expect("temp dir should be created");
        let runtime =
            NodeRuntimeState::load(tempdir.path()).expect("node runtime should initialize cleanly");
        runtime
            .register_node(
                OWNER_DEVICE_ID,
                "windows-x86_64",
                vec![super::DeviceCapabilityView {
                    name: "system.health".to_owned(),
                    available: true,
                }],
            )
            .expect("node should register");
        let (request_id, mut receiver) = runtime
            .enqueue_capability_request(
                OWNER_DEVICE_ID,
                "system.health",
                br#"{"ok":true}"#.to_vec(),
                4096,
                Some(30_000),
            )
            .expect("capability request should queue");
        runtime
            .next_capability_dispatch(OWNER_DEVICE_ID, &TestClaimAuthorizer::REJECTING)
            .expect("dispatch should succeed")
            .expect("request should dispatch");
        let result = super::CapabilityExecutionResult {
            success: true,
            output_json: br#"{"status":"retry"}"#.to_vec(),
            error: String::new(),
        };

        runtime.fail_next_result_persist_for_test();
        let error = runtime
            .complete_capability_request(
                OWNER_DEVICE_ID,
                request_id.as_str(),
                None,
                None,
                result.clone(),
                &TestClaimAuthorizer::REJECTING,
            )
            .expect_err("injected persistence failure should reject the result");
        assert_eq!(error.code(), Code::Internal);
        assert!(receiver.try_recv().is_none());
        let request = runtime
            .capability_requests(Some(OWNER_DEVICE_ID))
            .expect("request audit should load")
            .into_iter()
            .find(|request| request.request_id == request_id)
            .expect("request should remain auditable");
        assert!(matches!(request.state, super::CapabilityRequestState::Dispatched));
        assert!(request.output_summary.is_none());

        assert!(runtime
            .complete_capability_request(
                OWNER_DEVICE_ID,
                request_id.as_str(),
                None,
                None,
                result,
                &TestClaimAuthorizer::REJECTING,
            )
            .expect("exact retry should use the retained owner"));
        let notification = receiver.try_recv().expect("retried result should be delivered");
        assert_eq!(notification.result.output_json, br#"{"status":"retry"}"#);
    }

    #[test]
    fn capability_result_remains_deliverable_when_no_future_is_polling() {
        const OWNER_DEVICE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAZ";

        let tempdir = tempdir().expect("temp dir should be created");
        let runtime =
            NodeRuntimeState::load(tempdir.path()).expect("node runtime should initialize cleanly");
        runtime
            .register_node(
                OWNER_DEVICE_ID,
                "windows-x86_64",
                vec![super::DeviceCapabilityView {
                    name: "system.health".to_owned(),
                    available: true,
                }],
            )
            .expect("node should register");
        let (request_id, mut receiver) = runtime
            .enqueue_capability_request(
                OWNER_DEVICE_ID,
                "system.health",
                br#"{"ok":true}"#.to_vec(),
                4096,
                Some(30_000),
            )
            .expect("capability request should queue");
        runtime
            .next_capability_dispatch(OWNER_DEVICE_ID, &TestClaimAuthorizer::REJECTING)
            .expect("dispatch should succeed")
            .expect("request should dispatch");
        runtime
            .mark_capability_timeout(request_id.as_str())
            .expect("request timeout should persist");

        assert!(runtime
            .complete_capability_request(
                OWNER_DEVICE_ID,
                request_id.as_str(),
                None,
                None,
                super::CapabilityExecutionResult {
                    success: true,
                    output_json: br#"{"status":"late"}"#.to_vec(),
                    error: String::new(),
                },
                &TestClaimAuthorizer::REJECTING,
            )
            .expect("runtime-owned channel should accept the result"));
        let notification = receiver.try_recv().expect("buffered result should remain deliverable");
        assert!(notification.result.success);
        assert_eq!(notification.result.output_json, br#"{"status":"late"}"#);
        let request = runtime
            .capability_requests(Some(OWNER_DEVICE_ID))
            .expect("request audit should load")
            .into_iter()
            .find(|request| request.request_id == request_id)
            .expect("request should remain auditable");
        assert!(matches!(request.state, super::CapabilityRequestState::Succeeded));
        assert!(request.completed_at_unix_ms.is_some());
        assert!(request.output_summary.is_some());
        assert!(request.error.is_none());
    }

    #[tokio::test]
    async fn capability_result_wakeup_is_retained_before_wait_registration() {
        const OWNER_DEVICE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAZ";

        let tempdir = tempdir().expect("temp dir should be created");
        let runtime =
            NodeRuntimeState::load(tempdir.path()).expect("node runtime should initialize cleanly");
        runtime
            .register_node(
                OWNER_DEVICE_ID,
                "windows-x86_64",
                vec![super::DeviceCapabilityView {
                    name: "system.health".to_owned(),
                    available: true,
                }],
            )
            .expect("node should register");
        let (request_id, mut receiver) = runtime
            .enqueue_capability_request(
                OWNER_DEVICE_ID,
                "system.health",
                br#"{"ok":true}"#.to_vec(),
                4096,
                Some(30_000),
            )
            .expect("capability request should queue");
        runtime
            .next_capability_dispatch(OWNER_DEVICE_ID, &TestClaimAuthorizer::REJECTING)
            .expect("dispatch should succeed")
            .expect("request should dispatch");

        let slot = std::sync::Arc::clone(&receiver.slot);
        let notified = slot.ready.notified();
        assert!(receiver.try_recv().is_none());
        runtime
            .complete_capability_request(
                OWNER_DEVICE_ID,
                request_id.as_str(),
                None,
                None,
                super::CapabilityExecutionResult {
                    success: true,
                    output_json: br#"{"status":"between-check-and-await"}"#.to_vec(),
                    error: String::new(),
                },
                &TestClaimAuthorizer::REJECTING,
            )
            .expect("request owner should publish the result");

        tokio::time::timeout(std::time::Duration::from_secs(1), notified)
            .await
            .expect("publication should retain a wakeup until the receiver registers");
        let notification = receiver.try_recv().expect("published result should remain deliverable");
        assert_eq!(notification.result.output_json, br#"{"status":"between-check-and-await"}"#);
    }

    #[test]
    fn capability_grant_and_revoke_update_node_presence_immediately() {
        let tempdir = tempdir().expect("temp dir should be created");
        let runtime =
            NodeRuntimeState::load(tempdir.path()).expect("node runtime should initialize cleanly");
        runtime
            .register_node("01ARZ3NDEKTSV4RRFFQ69G5FAA", "windows-x86_64", Vec::new())
            .expect("node should register");

        let granted = runtime
            .set_node_capability_availability(
                "01ARZ3NDEKTSV4RRFFQ69G5FAA",
                "desktop.open_url",
                true,
            )
            .expect("grant should update node capability");
        assert_eq!(granted.capabilities.len(), 1);
        assert!(granted.capabilities[0].available);
        assert_eq!(granted.last_event_name.as_deref(), Some("capability_granted"));

        let revoked = runtime
            .set_node_capability_availability(
                "01ARZ3NDEKTSV4RRFFQ69G5FAA",
                "desktop.open_url",
                false,
            )
            .expect("revoke should update node capability");
        assert!(!revoked.capabilities[0].available);
        assert_eq!(revoked.last_event_name.as_deref(), Some("capability_revoked"));
    }
}
