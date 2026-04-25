use std::collections::{BTreeMap, BTreeSet, VecDeque};

pub use palyra_common::runtime_contracts::WorkerLifecycleState;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

const MAX_WORKER_ID_BYTES: usize = 128;
const MAX_GRANT_ID_BYTES: usize = 128;
const MAX_RECENT_LIFECYCLE_EVENTS: usize = 64;
const DEFAULT_WORKER_SDK_PROTOCOL_VERSION: u32 = 1;
const DEFAULT_WORKER_WIT_ABI_VERSION: &str = "palyra-worker-abi/v1";

fn default_worker_sdk_protocol_version() -> u32 {
    DEFAULT_WORKER_SDK_PROTOCOL_VERSION
}

fn default_worker_wit_abi_version() -> String {
    DEFAULT_WORKER_WIT_ABI_VERSION.to_owned()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerAttestation {
    pub worker_id: String,
    pub image_digest_sha256: String,
    pub build_digest_sha256: String,
    pub artifact_digest_sha256: String,
    pub egress_proxy_attested: bool,
    #[serde(default)]
    pub supported_capabilities: Vec<String>,
    #[serde(default)]
    pub capability_authority_sha256: Option<String>,
    #[serde(default = "default_worker_sdk_protocol_version")]
    pub sdk_protocol_version: u32,
    #[serde(default = "default_worker_wit_abi_version")]
    pub wit_abi_version: String,
    #[serde(default)]
    pub heartbeat_unix_ms: i64,
    pub issued_at_unix_ms: i64,
    pub expires_at_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerAttestationExpectation {
    pub require_egress_proxy: bool,
    pub image_digest_sha256: Option<String>,
    pub build_digest_sha256: Option<String>,
    pub artifact_digest_sha256: Option<String>,
}

impl Default for WorkerAttestationExpectation {
    fn default() -> Self {
        Self {
            require_egress_proxy: true,
            image_digest_sha256: None,
            build_digest_sha256: None,
            artifact_digest_sha256: None,
        }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum WorkerAttestationError {
    #[error("worker attestation missing worker identifier")]
    MissingWorkerId,
    #[error("worker attestation is expired")]
    Expired,
    #[error("worker attestation is not yet valid")]
    NotYetValid,
    #[error("worker attestation does not include an attested egress proxy binding")]
    MissingEgressProxyBinding,
    #[error("worker attestation {field} digest did not match the expected value")]
    DigestMismatch { field: &'static str },
}

impl WorkerAttestation {
    pub fn validate(
        &self,
        expected: &WorkerAttestationExpectation,
        now_unix_ms: i64,
    ) -> Result<(), WorkerAttestationError> {
        if self.worker_id.trim().is_empty() || self.worker_id.len() > MAX_WORKER_ID_BYTES {
            return Err(WorkerAttestationError::MissingWorkerId);
        }
        if self.issued_at_unix_ms > now_unix_ms {
            return Err(WorkerAttestationError::NotYetValid);
        }
        if self.expires_at_unix_ms <= now_unix_ms {
            return Err(WorkerAttestationError::Expired);
        }
        if expected.require_egress_proxy && !self.egress_proxy_attested {
            return Err(WorkerAttestationError::MissingEgressProxyBinding);
        }
        if expected
            .image_digest_sha256
            .as_deref()
            .is_some_and(|expected_digest| expected_digest != self.image_digest_sha256)
        {
            return Err(WorkerAttestationError::DigestMismatch { field: "image" });
        }
        if expected
            .build_digest_sha256
            .as_deref()
            .is_some_and(|expected_digest| expected_digest != self.build_digest_sha256)
        {
            return Err(WorkerAttestationError::DigestMismatch { field: "build" });
        }
        if expected
            .artifact_digest_sha256
            .as_deref()
            .is_some_and(|expected_digest| expected_digest != self.artifact_digest_sha256)
        {
            return Err(WorkerAttestationError::DigestMismatch { field: "artifact" });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerWorkspaceScope {
    pub workspace_root: String,
    #[serde(default)]
    pub allowed_paths: Vec<String>,
    pub read_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerArtifactTransport {
    pub input_manifest_sha256: String,
    pub output_manifest_sha256: String,
    pub log_stream_id: String,
    pub scratch_directory_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerRunGrant {
    pub grant_id: String,
    pub run_id: String,
    pub tool_name: String,
    pub expires_at_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerLeaseRequest {
    pub run_id: String,
    pub ttl_ms: u64,
    #[serde(default)]
    pub required_capabilities: Vec<String>,
    pub workspace_scope: WorkerWorkspaceScope,
    pub artifact_transport: WorkerArtifactTransport,
    pub grant: WorkerRunGrant,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerLease {
    pub lease_id: String,
    pub worker_id: String,
    pub run_id: String,
    pub expires_at_unix_ms: i64,
    pub required_capabilities: Vec<String>,
    pub workspace_scope: WorkerWorkspaceScope,
    pub artifact_transport: WorkerArtifactTransport,
    pub grant: WorkerRunGrant,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerCleanupReport {
    pub removed_workspace_scope: bool,
    pub removed_artifacts: bool,
    pub removed_logs: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerLifecycleEvent {
    pub worker_id: String,
    pub state: WorkerLifecycleState,
    pub run_id: Option<String>,
    pub reason_code: String,
    pub timestamp_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerCleanupOutcome {
    pub event: WorkerLifecycleEvent,
    pub cleanup_report: WorkerCleanupReport,
    pub cleanup_succeeded: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct WorkerFleetSnapshot {
    pub registered_workers: usize,
    pub attested_workers: usize,
    pub active_leases: usize,
    pub available_workers: usize,
    pub busy_workers: usize,
    pub degraded_workers: usize,
    pub draining_workers: usize,
    pub offline_workers: usize,
    pub orphaned_workers: usize,
    pub failed_closed_workers: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerFleetPolicy {
    pub max_ttl_ms: u64,
    pub heartbeat_timeout_ms: u64,
    pub trusted_capabilities: Vec<String>,
    pub required_capability_authority_sha256: Option<String>,
    pub required_sdk_protocol_version: Option<u32>,
    pub required_wit_abi_version: Option<String>,
    pub attestation: WorkerAttestationExpectation,
}

impl Default for WorkerFleetPolicy {
    fn default() -> Self {
        Self {
            max_ttl_ms: 15 * 60 * 1_000,
            heartbeat_timeout_ms: 30_000,
            trusted_capabilities: vec![
                "tool:palyra.echo".to_owned(),
                "tool:palyra.sleep".to_owned(),
            ],
            required_capability_authority_sha256: None,
            required_sdk_protocol_version: Some(DEFAULT_WORKER_SDK_PROTOCOL_VERSION),
            required_wit_abi_version: Some(DEFAULT_WORKER_WIT_ABI_VERSION.to_owned()),
            attestation: WorkerAttestationExpectation::default(),
        }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum WorkerLifecycleError {
    #[error(transparent)]
    Attestation(#[from] WorkerAttestationError),
    #[error("worker '{0}' is already registered")]
    AlreadyRegistered(String),
    #[error("worker '{0}' is not registered")]
    UnknownWorker(String),
    #[error("requested worker lease ttl exceeds the configured maximum")]
    TtlExceeded,
    #[error("worker '{0}' already has an active lease")]
    LeaseAlreadyActive(String),
    #[error("worker '{0}' is fail-closed and cannot accept work")]
    WorkerFailClosed(String),
    #[error("worker '{0}' is draining and cannot accept new work")]
    WorkerDraining(String),
    #[error("worker '{0}' heartbeat is stale")]
    WorkerOffline(String),
    #[error("no attested worker is available for the requested capabilities")]
    NoAvailableWorker,
    #[error("worker compatibility check failed: {0}")]
    CompatibilityMismatch(String),
    #[error("worker lease request is invalid: {0}")]
    InvalidLeaseRequest(String),
    #[error("worker cleanup failed and the worker stayed fail-closed")]
    CleanupFailed,
}

#[derive(Debug, Clone)]
struct WorkerRecord {
    attestation: WorkerAttestation,
    state: WorkerLifecycleState,
    lease: Option<WorkerLease>,
    last_heartbeat_unix_ms: i64,
}

#[derive(Debug, Default)]
pub struct WorkerFleetManager {
    workers: BTreeMap<String, WorkerRecord>,
    recent_events: VecDeque<WorkerLifecycleEvent>,
}

impl WorkerFleetManager {
    #[must_use]
    pub fn snapshot(&self) -> WorkerFleetSnapshot {
        let registered_workers = self.workers.len();
        let attested_workers = self
            .workers
            .values()
            .filter(|worker| {
                worker.attestation.egress_proxy_attested
                    && !matches!(worker.state, WorkerLifecycleState::Failed)
            })
            .count();
        let active_leases = self.workers.values().filter(|worker| worker.lease.is_some()).count();
        let available_workers = self
            .workers
            .values()
            .filter(|worker| {
                worker.lease.is_none()
                    && matches!(
                        worker.state,
                        WorkerLifecycleState::Registered
                            | WorkerLifecycleState::Available
                            | WorkerLifecycleState::Completed
                    )
            })
            .count();
        let busy_workers = self
            .workers
            .values()
            .filter(|worker| {
                worker.lease.is_some()
                    || matches!(
                        worker.state,
                        WorkerLifecycleState::Assigned | WorkerLifecycleState::Busy
                    )
            })
            .count();
        let degraded_workers = self
            .workers
            .values()
            .filter(|worker| {
                matches!(
                    worker.state,
                    WorkerLifecycleState::Degraded
                        | WorkerLifecycleState::Failed
                        | WorkerLifecycleState::Orphaned
                )
            })
            .count();
        let draining_workers = self
            .workers
            .values()
            .filter(|worker| matches!(worker.state, WorkerLifecycleState::Draining))
            .count();
        let offline_workers = self
            .workers
            .values()
            .filter(|worker| matches!(worker.state, WorkerLifecycleState::Offline))
            .count();
        let orphaned_workers = self
            .workers
            .values()
            .filter(|worker| matches!(worker.state, WorkerLifecycleState::Orphaned))
            .count();
        let failed_closed_workers = self
            .workers
            .values()
            .filter(|worker| matches!(worker.state, WorkerLifecycleState::Failed))
            .count();
        WorkerFleetSnapshot {
            registered_workers,
            attested_workers,
            active_leases,
            available_workers,
            busy_workers,
            degraded_workers,
            draining_workers,
            offline_workers,
            orphaned_workers,
            failed_closed_workers,
        }
    }

    #[must_use]
    pub fn recent_events(&self) -> Vec<WorkerLifecycleEvent> {
        self.recent_events.iter().cloned().collect()
    }

    pub fn register_worker(
        &mut self,
        attestation: WorkerAttestation,
        policy: &WorkerFleetPolicy,
        now_unix_ms: i64,
    ) -> Result<WorkerLifecycleEvent, WorkerLifecycleError> {
        attestation.validate(&policy.attestation, now_unix_ms)?;
        validate_worker_compatibility(&attestation, policy)?;
        if self.workers.contains_key(attestation.worker_id.as_str()) {
            return Err(WorkerLifecycleError::AlreadyRegistered(attestation.worker_id));
        }
        let worker_id = attestation.worker_id.clone();
        let last_heartbeat_unix_ms = if attestation.heartbeat_unix_ms > 0 {
            attestation.heartbeat_unix_ms
        } else {
            now_unix_ms
        };
        self.workers.insert(
            worker_id.clone(),
            WorkerRecord {
                attestation,
                state: WorkerLifecycleState::Registered,
                lease: None,
                last_heartbeat_unix_ms,
            },
        );
        let event = WorkerLifecycleEvent {
            worker_id,
            state: WorkerLifecycleState::Registered,
            run_id: None,
            reason_code: "worker.registered".to_owned(),
            timestamp_unix_ms: now_unix_ms,
        };
        self.push_recent_event(event.clone());
        Ok(event)
    }

    pub fn assign_work(
        &mut self,
        worker_id: &str,
        request: WorkerLeaseRequest,
        policy: &WorkerFleetPolicy,
        now_unix_ms: i64,
    ) -> Result<(WorkerLease, WorkerLifecycleEvent), WorkerLifecycleError> {
        validate_lease_request(&request, policy, now_unix_ms)?;
        let worker = self
            .workers
            .get_mut(worker_id)
            .ok_or_else(|| WorkerLifecycleError::UnknownWorker(worker_id.to_owned()))?;
        let (lease, event) = assign_worker_record(worker_id, worker, request, policy, now_unix_ms)?;
        self.push_recent_event(event.clone());
        Ok((lease, event))
    }

    pub fn assign_next_work(
        &mut self,
        request: WorkerLeaseRequest,
        policy: &WorkerFleetPolicy,
        now_unix_ms: i64,
    ) -> Result<(WorkerLease, WorkerLifecycleEvent), WorkerLifecycleError> {
        validate_lease_request(&request, policy, now_unix_ms)?;
        let Some(worker_id) = self.workers.iter().find_map(|(worker_id, worker)| {
            worker_record_can_accept(worker, &request, policy, now_unix_ms)
                .then(|| worker_id.clone())
        }) else {
            return Err(WorkerLifecycleError::NoAvailableWorker);
        };
        let worker = self
            .workers
            .get_mut(worker_id.as_str())
            .ok_or_else(|| WorkerLifecycleError::UnknownWorker(worker_id.clone()))?;
        let (lease, event) =
            assign_worker_record(worker_id.as_str(), worker, request, policy, now_unix_ms)?;
        self.push_recent_event(event.clone());
        Ok((lease, event))
    }

    pub fn complete_work(
        &mut self,
        worker_id: &str,
        cleanup: &WorkerCleanupReport,
        now_unix_ms: i64,
    ) -> Result<WorkerLifecycleEvent, WorkerLifecycleError> {
        let outcome = self.finalize_work(worker_id, cleanup.clone(), now_unix_ms)?;
        if outcome.cleanup_succeeded {
            Ok(outcome.event)
        } else {
            Err(WorkerLifecycleError::CleanupFailed)
        }
    }

    pub fn finalize_work(
        &mut self,
        worker_id: &str,
        cleanup: WorkerCleanupReport,
        now_unix_ms: i64,
    ) -> Result<WorkerCleanupOutcome, WorkerLifecycleError> {
        let worker = self
            .workers
            .get_mut(worker_id)
            .ok_or_else(|| WorkerLifecycleError::UnknownWorker(worker_id.to_owned()))?;
        let run_id = worker.lease.as_ref().map(|lease| lease.run_id.clone());
        let cleanup_succeeded = cleanup.failure_reason.is_none()
            && cleanup.removed_workspace_scope
            && cleanup.removed_artifacts
            && cleanup.removed_logs;
        let event = if cleanup_succeeded {
            worker.state = WorkerLifecycleState::Completed;
            worker.lease = None;
            WorkerLifecycleEvent {
                worker_id: worker_id.to_owned(),
                state: WorkerLifecycleState::Completed,
                run_id,
                reason_code: "worker.completed".to_owned(),
                timestamp_unix_ms: now_unix_ms,
            }
        } else {
            worker.state = WorkerLifecycleState::Failed;
            worker.lease = None;
            WorkerLifecycleEvent {
                worker_id: worker_id.to_owned(),
                state: WorkerLifecycleState::Failed,
                run_id,
                reason_code: "worker.cleanup_failed".to_owned(),
                timestamp_unix_ms: now_unix_ms,
            }
        };
        self.push_recent_event(event.clone());
        Ok(WorkerCleanupOutcome { event, cleanup_report: cleanup, cleanup_succeeded })
    }

    pub fn quarantine_worker(
        &mut self,
        worker_id: &str,
        reason_code: &str,
        now_unix_ms: i64,
    ) -> Result<WorkerLifecycleEvent, WorkerLifecycleError> {
        let worker = self
            .workers
            .get_mut(worker_id)
            .ok_or_else(|| WorkerLifecycleError::UnknownWorker(worker_id.to_owned()))?;
        let run_id = worker.lease.as_ref().map(|lease| lease.run_id.clone());
        worker.state = WorkerLifecycleState::Failed;
        worker.lease = None;
        let event = WorkerLifecycleEvent {
            worker_id: worker_id.to_owned(),
            state: WorkerLifecycleState::Failed,
            run_id,
            reason_code: normalize_operator_reason_code(
                reason_code,
                "worker.quarantined_by_operator",
            ),
            timestamp_unix_ms: now_unix_ms,
        };
        self.push_recent_event(event.clone());
        Ok(event)
    }

    pub fn quarantine_all_workers(
        &mut self,
        reason_code: &str,
        now_unix_ms: i64,
    ) -> Vec<WorkerLifecycleEvent> {
        let reason_code = normalize_operator_reason_code(reason_code, "worker.drained_by_operator");
        let mut events = Vec::new();
        for (worker_id, worker) in &mut self.workers {
            if matches!(worker.state, WorkerLifecycleState::Failed) && worker.lease.is_none() {
                continue;
            }
            let run_id = worker.lease.as_ref().map(|lease| lease.run_id.clone());
            worker.state = WorkerLifecycleState::Failed;
            worker.lease = None;
            events.push(WorkerLifecycleEvent {
                worker_id: worker_id.clone(),
                state: WorkerLifecycleState::Failed,
                run_id,
                reason_code: reason_code.clone(),
                timestamp_unix_ms: now_unix_ms,
            });
        }
        for event in &events {
            self.push_recent_event(event.clone());
        }
        events
    }

    pub fn reverify_worker(
        &mut self,
        worker_id: &str,
        policy: &WorkerFleetPolicy,
        now_unix_ms: i64,
    ) -> Result<WorkerLifecycleEvent, WorkerLifecycleError> {
        let worker = self
            .workers
            .get_mut(worker_id)
            .ok_or_else(|| WorkerLifecycleError::UnknownWorker(worker_id.to_owned()))?;
        if worker.lease.is_some() {
            return Err(WorkerLifecycleError::LeaseAlreadyActive(worker_id.to_owned()));
        }
        worker.attestation.validate(&policy.attestation, now_unix_ms)?;
        validate_worker_compatibility(&worker.attestation, policy)?;
        worker.state = WorkerLifecycleState::Registered;
        worker.last_heartbeat_unix_ms = now_unix_ms;
        let event = WorkerLifecycleEvent {
            worker_id: worker_id.to_owned(),
            state: WorkerLifecycleState::Registered,
            run_id: None,
            reason_code: "worker.reverified_by_operator".to_owned(),
            timestamp_unix_ms: now_unix_ms,
        };
        self.push_recent_event(event.clone());
        Ok(event)
    }

    pub fn heartbeat_worker(
        &mut self,
        worker_id: &str,
        policy: &WorkerFleetPolicy,
        now_unix_ms: i64,
    ) -> Result<WorkerLifecycleEvent, WorkerLifecycleError> {
        let worker = self
            .workers
            .get_mut(worker_id)
            .ok_or_else(|| WorkerLifecycleError::UnknownWorker(worker_id.to_owned()))?;
        worker.attestation.validate(&policy.attestation, now_unix_ms)?;
        validate_worker_compatibility(&worker.attestation, policy)?;
        worker.last_heartbeat_unix_ms = now_unix_ms;
        if matches!(worker.state, WorkerLifecycleState::Offline) {
            worker.state = WorkerLifecycleState::Registered;
        }
        let event = WorkerLifecycleEvent {
            worker_id: worker_id.to_owned(),
            state: worker.state,
            run_id: worker.lease.as_ref().map(|lease| lease.run_id.clone()),
            reason_code: "worker.heartbeat".to_owned(),
            timestamp_unix_ms: now_unix_ms,
        };
        self.push_recent_event(event.clone());
        Ok(event)
    }

    pub fn drain_worker(
        &mut self,
        worker_id: &str,
        reason_code: &str,
        now_unix_ms: i64,
    ) -> Result<WorkerLifecycleEvent, WorkerLifecycleError> {
        let worker = self
            .workers
            .get_mut(worker_id)
            .ok_or_else(|| WorkerLifecycleError::UnknownWorker(worker_id.to_owned()))?;
        worker.state = WorkerLifecycleState::Draining;
        let event = WorkerLifecycleEvent {
            worker_id: worker_id.to_owned(),
            state: WorkerLifecycleState::Draining,
            run_id: worker.lease.as_ref().map(|lease| lease.run_id.clone()),
            reason_code: normalize_operator_reason_code(reason_code, "worker.draining"),
            timestamp_unix_ms: now_unix_ms,
        };
        self.push_recent_event(event.clone());
        Ok(event)
    }

    pub fn revoke_lease(
        &mut self,
        worker_id: &str,
        reason_code: &str,
        now_unix_ms: i64,
    ) -> Result<WorkerLifecycleEvent, WorkerLifecycleError> {
        let worker = self
            .workers
            .get_mut(worker_id)
            .ok_or_else(|| WorkerLifecycleError::UnknownWorker(worker_id.to_owned()))?;
        let run_id = worker.lease.as_ref().map(|lease| lease.run_id.clone());
        worker.lease = None;
        worker.state = WorkerLifecycleState::Orphaned;
        let event = WorkerLifecycleEvent {
            worker_id: worker_id.to_owned(),
            state: WorkerLifecycleState::Orphaned,
            run_id,
            reason_code: normalize_operator_reason_code(reason_code, "worker.lease_revoked"),
            timestamp_unix_ms: now_unix_ms,
        };
        self.push_recent_event(event.clone());
        Ok(event)
    }

    pub fn force_cleanup_worker(
        &mut self,
        worker_id: &str,
        cleanup: WorkerCleanupReport,
        now_unix_ms: i64,
    ) -> Result<WorkerCleanupOutcome, WorkerLifecycleError> {
        self.finalize_work(worker_id, cleanup, now_unix_ms)
    }

    pub fn reap_expired_workers(&mut self, now_unix_ms: i64) -> Vec<WorkerLifecycleEvent> {
        let mut events = Vec::new();
        for (worker_id, worker) in &mut self.workers {
            let expired =
                worker.lease.as_ref().is_some_and(|lease| lease.expires_at_unix_ms <= now_unix_ms);
            if expired {
                let run_id = worker.lease.as_ref().map(|lease| lease.run_id.clone());
                worker.state = WorkerLifecycleState::Orphaned;
                worker.lease = None;
                events.push(WorkerLifecycleEvent {
                    worker_id: worker_id.clone(),
                    state: WorkerLifecycleState::Orphaned,
                    run_id,
                    reason_code: "worker.ttl_expired".to_owned(),
                    timestamp_unix_ms: now_unix_ms,
                });
            }
        }
        for event in &events {
            self.push_recent_event(event.clone());
        }
        events
    }

    pub fn mark_stale_heartbeat_workers(
        &mut self,
        policy: &WorkerFleetPolicy,
        now_unix_ms: i64,
    ) -> Vec<WorkerLifecycleEvent> {
        let mut events = Vec::new();
        for (worker_id, worker) in &mut self.workers {
            if matches!(
                worker.state,
                WorkerLifecycleState::Failed
                    | WorkerLifecycleState::Offline
                    | WorkerLifecycleState::Orphaned
            ) {
                continue;
            }
            if worker_heartbeat_is_fresh(worker, policy, now_unix_ms) {
                continue;
            }
            let run_id = worker.lease.as_ref().map(|lease| lease.run_id.clone());
            worker.state = WorkerLifecycleState::Offline;
            worker.lease = None;
            events.push(WorkerLifecycleEvent {
                worker_id: worker_id.clone(),
                state: WorkerLifecycleState::Offline,
                run_id,
                reason_code: "worker.heartbeat_stale".to_owned(),
                timestamp_unix_ms: now_unix_ms,
            });
        }
        for event in &events {
            self.push_recent_event(event.clone());
        }
        events
    }

    fn push_recent_event(&mut self, event: WorkerLifecycleEvent) {
        self.recent_events.push_front(event);
        while self.recent_events.len() > MAX_RECENT_LIFECYCLE_EVENTS {
            self.recent_events.pop_back();
        }
    }
}

fn normalize_operator_reason_code(raw: &str, fallback: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.len() > 128 {
        return fallback.to_owned();
    }
    if trimmed.chars().all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-')) {
        trimmed.to_owned()
    } else {
        fallback.to_owned()
    }
}

fn validate_lease_request(
    request: &WorkerLeaseRequest,
    policy: &WorkerFleetPolicy,
    now_unix_ms: i64,
) -> Result<(), WorkerLifecycleError> {
    if request.run_id.trim().is_empty() {
        return Err(WorkerLifecycleError::InvalidLeaseRequest(
            "run_id must not be empty".to_owned(),
        ));
    }
    if request.ttl_ms == 0 {
        return Err(WorkerLifecycleError::InvalidLeaseRequest(
            "ttl_ms must be positive".to_owned(),
        ));
    }
    if request.ttl_ms > policy.max_ttl_ms {
        return Err(WorkerLifecycleError::TtlExceeded);
    }
    if request.grant.grant_id.trim().is_empty() || request.grant.grant_id.len() > MAX_GRANT_ID_BYTES
    {
        return Err(WorkerLifecycleError::InvalidLeaseRequest(
            "grant_id must be present and bounded".to_owned(),
        ));
    }
    if request.grant.run_id != request.run_id {
        return Err(WorkerLifecycleError::InvalidLeaseRequest(
            "grant run_id must match lease run_id".to_owned(),
        ));
    }
    if request.grant.expires_at_unix_ms <= now_unix_ms {
        return Err(WorkerLifecycleError::InvalidLeaseRequest("grant is expired".to_owned()));
    }
    Ok(())
}

fn validate_worker_compatibility(
    attestation: &WorkerAttestation,
    policy: &WorkerFleetPolicy,
) -> Result<(), WorkerLifecycleError> {
    if let Some(expected) = policy.required_capability_authority_sha256.as_deref() {
        let Some(actual) = attestation.capability_authority_sha256.as_deref() else {
            return Err(WorkerLifecycleError::CompatibilityMismatch(
                "capability authority digest is required".to_owned(),
            ));
        };
        if actual != expected {
            return Err(WorkerLifecycleError::CompatibilityMismatch(
                "capability authority digest mismatch".to_owned(),
            ));
        }
    }
    if let Some(expected) = policy.required_sdk_protocol_version {
        if attestation.sdk_protocol_version != expected {
            return Err(WorkerLifecycleError::CompatibilityMismatch(format!(
                "sdk_protocol_version={} expected={expected}",
                attestation.sdk_protocol_version
            )));
        }
    }
    if let Some(expected) = policy.required_wit_abi_version.as_deref() {
        if attestation.wit_abi_version != expected {
            return Err(WorkerLifecycleError::CompatibilityMismatch(format!(
                "wit_abi_version={} expected={expected}",
                attestation.wit_abi_version
            )));
        }
    }
    Ok(())
}

fn worker_heartbeat_is_fresh(
    worker: &WorkerRecord,
    policy: &WorkerFleetPolicy,
    now_unix_ms: i64,
) -> bool {
    now_unix_ms.saturating_sub(worker.last_heartbeat_unix_ms)
        <= i64::try_from(policy.heartbeat_timeout_ms).unwrap_or(i64::MAX)
}

fn assign_worker_record(
    worker_id: &str,
    worker: &mut WorkerRecord,
    request: WorkerLeaseRequest,
    policy: &WorkerFleetPolicy,
    now_unix_ms: i64,
) -> Result<(WorkerLease, WorkerLifecycleEvent), WorkerLifecycleError> {
    worker.attestation.validate(&policy.attestation, now_unix_ms)?;
    validate_worker_compatibility(&worker.attestation, policy)?;
    if worker.lease.is_some() {
        return Err(WorkerLifecycleError::LeaseAlreadyActive(worker_id.to_owned()));
    }
    if matches!(worker.state, WorkerLifecycleState::Failed | WorkerLifecycleState::Orphaned) {
        return Err(WorkerLifecycleError::WorkerFailClosed(worker_id.to_owned()));
    }
    if matches!(worker.state, WorkerLifecycleState::Draining) {
        return Err(WorkerLifecycleError::WorkerDraining(worker_id.to_owned()));
    }
    if !worker_heartbeat_is_fresh(worker, policy, now_unix_ms) {
        worker.state = WorkerLifecycleState::Offline;
        return Err(WorkerLifecycleError::WorkerOffline(worker_id.to_owned()));
    }
    if !worker_supports_capabilities(worker, request.required_capabilities.as_slice(), policy) {
        return Err(WorkerLifecycleError::NoAvailableWorker);
    }
    let lease = WorkerLease {
        lease_id: Ulid::new().to_string(),
        worker_id: worker_id.to_owned(),
        run_id: request.run_id.clone(),
        expires_at_unix_ms: now_unix_ms.saturating_add(request.ttl_ms as i64),
        required_capabilities: request.required_capabilities,
        workspace_scope: request.workspace_scope,
        artifact_transport: request.artifact_transport,
        grant: request.grant,
    };
    worker.state = WorkerLifecycleState::Assigned;
    worker.lease = Some(lease.clone());
    Ok((
        lease.clone(),
        WorkerLifecycleEvent {
            worker_id: worker_id.to_owned(),
            state: WorkerLifecycleState::Assigned,
            run_id: Some(lease.run_id.clone()),
            reason_code: "worker.assigned".to_owned(),
            timestamp_unix_ms: now_unix_ms,
        },
    ))
}

fn worker_record_can_accept(
    worker: &WorkerRecord,
    request: &WorkerLeaseRequest,
    policy: &WorkerFleetPolicy,
    now_unix_ms: i64,
) -> bool {
    worker.lease.is_none()
        && !matches!(
            worker.state,
            WorkerLifecycleState::Failed
                | WorkerLifecycleState::Orphaned
                | WorkerLifecycleState::Draining
                | WorkerLifecycleState::Offline
        )
        && worker.attestation.validate(&policy.attestation, now_unix_ms).is_ok()
        && validate_worker_compatibility(&worker.attestation, policy).is_ok()
        && worker_heartbeat_is_fresh(worker, policy, now_unix_ms)
        && worker_supports_capabilities(worker, request.required_capabilities.as_slice(), policy)
}

fn worker_supports_capabilities(
    worker: &WorkerRecord,
    required_capabilities: &[String],
    policy: &WorkerFleetPolicy,
) -> bool {
    if required_capabilities.is_empty() {
        return true;
    }
    let trusted_capabilities = policy
        .trusted_capabilities
        .iter()
        .map(|capability| capability.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    required_capabilities.iter().all(|required| {
        let normalized_required = required.to_ascii_lowercase();
        trusted_capabilities.contains(normalized_required.as_str())
            && worker
                .attestation
                .supported_capabilities
                .iter()
                .any(|available| available.eq_ignore_ascii_case(required))
    })
}

#[cfg(test)]
mod tests {
    use super::{
        WorkerArtifactTransport, WorkerAttestation, WorkerCleanupReport, WorkerFleetManager,
        WorkerFleetPolicy, WorkerLeaseRequest, WorkerLifecycleError, WorkerLifecycleState,
        WorkerRunGrant, WorkerWorkspaceScope,
    };

    fn attestation(worker_id: &str) -> WorkerAttestation {
        WorkerAttestation {
            worker_id: worker_id.to_owned(),
            image_digest_sha256: "img".repeat(16),
            build_digest_sha256: "bld".repeat(16),
            artifact_digest_sha256: "art".repeat(16),
            egress_proxy_attested: true,
            supported_capabilities: vec!["tool:palyra.echo".to_owned()],
            capability_authority_sha256: None,
            sdk_protocol_version: 1,
            wit_abi_version: "palyra-worker-abi/v1".to_owned(),
            heartbeat_unix_ms: 2_000,
            issued_at_unix_ms: 1_000,
            expires_at_unix_ms: 10_000,
        }
    }

    fn lease_request(run_id: &str, ttl_ms: u64) -> WorkerLeaseRequest {
        WorkerLeaseRequest {
            run_id: run_id.to_owned(),
            ttl_ms,
            required_capabilities: Vec::new(),
            workspace_scope: WorkerWorkspaceScope {
                workspace_root: "/workspace".to_owned(),
                allowed_paths: vec!["src".to_owned()],
                read_only: false,
            },
            artifact_transport: WorkerArtifactTransport {
                input_manifest_sha256: "in".repeat(32),
                output_manifest_sha256: "out".repeat(32),
                log_stream_id: "log-stream".to_owned(),
                scratch_directory_id: "scratch".to_owned(),
            },
            grant: WorkerRunGrant {
                grant_id: format!("grant-{run_id}"),
                run_id: run_id.to_owned(),
                tool_name: "palyra.echo".to_owned(),
                expires_at_unix_ms: 9_000,
            },
        }
    }

    #[test]
    fn worker_lifecycle_supports_successful_handshake_assignment_and_cleanup() {
        let mut manager = WorkerFleetManager::default();
        let policy = WorkerFleetPolicy::default();

        let register = manager
            .register_worker(attestation("worker-a"), &policy, 2_000)
            .expect("worker should register");
        assert_eq!(register.reason_code, "worker.registered");

        let (lease, assign) = manager
            .assign_work("worker-a", lease_request("run-1", 500), &policy, 2_500)
            .expect("worker should accept a lease");
        assert_eq!(lease.run_id, "run-1");
        assert_eq!(assign.state, WorkerLifecycleState::Assigned);

        let complete = manager
            .complete_work(
                "worker-a",
                &WorkerCleanupReport {
                    removed_workspace_scope: true,
                    removed_artifacts: true,
                    removed_logs: true,
                    failure_reason: None,
                },
                3_000,
            )
            .expect("cleanup should succeed");
        assert_eq!(complete.state, WorkerLifecycleState::Completed);
        assert_eq!(manager.snapshot().active_leases, 0);
    }

    #[test]
    fn worker_registration_rejects_missing_egress_proxy_attestation() {
        let mut manager = WorkerFleetManager::default();
        let policy = WorkerFleetPolicy::default();
        let mut worker_attestation = attestation("worker-b");
        worker_attestation.egress_proxy_attested = false;

        let error = manager
            .register_worker(worker_attestation, &policy, 2_000)
            .expect_err("egress proxy binding should be required");
        assert!(matches!(
            error,
            WorkerLifecycleError::Attestation(
                super::WorkerAttestationError::MissingEgressProxyBinding
            )
        ));
    }

    #[test]
    fn worker_cleanup_failure_stays_fail_closed() {
        let mut manager = WorkerFleetManager::default();
        let policy = WorkerFleetPolicy::default();
        manager.register_worker(attestation("worker-c"), &policy, 2_000).unwrap();
        manager.assign_work("worker-c", lease_request("run-2", 500), &policy, 2_500).unwrap();

        let error = manager
            .complete_work(
                "worker-c",
                &WorkerCleanupReport {
                    removed_workspace_scope: false,
                    removed_artifacts: true,
                    removed_logs: true,
                    failure_reason: Some("artifact cleanup failure".to_owned()),
                },
                3_000,
            )
            .expect_err("cleanup failure should not be ignored");
        assert_eq!(error, WorkerLifecycleError::CleanupFailed);
    }

    #[test]
    fn worker_ttl_reap_marks_orphaned_instances() {
        let mut manager = WorkerFleetManager::default();
        let policy = WorkerFleetPolicy::default();
        manager.register_worker(attestation("worker-d"), &policy, 2_000).unwrap();
        manager.assign_work("worker-d", lease_request("run-3", 250), &policy, 2_500).unwrap();

        let events = manager.reap_expired_workers(2_751);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].state, WorkerLifecycleState::Orphaned);
        assert_eq!(manager.snapshot().orphaned_workers, 1);
    }

    #[test]
    fn worker_auto_assignment_matches_required_capabilities() {
        let mut manager = WorkerFleetManager::default();
        let policy = WorkerFleetPolicy::default();
        manager.register_worker(attestation("worker-e"), &policy, 2_000).unwrap();

        let mut request = lease_request("run-4", 500);
        request.required_capabilities = vec!["tool:palyra.echo".to_owned()];
        let (lease, event) = manager
            .assign_next_work(request, &policy, 2_500)
            .expect("matching worker should accept the lease");

        assert_eq!(lease.worker_id, "worker-e");
        assert_eq!(lease.required_capabilities, vec!["tool:palyra.echo"]);
        assert_eq!(event.state, WorkerLifecycleState::Assigned);
        assert_eq!(manager.recent_events().len(), 2);
    }

    #[test]
    fn worker_auto_assignment_rejects_missing_capability() {
        let mut manager = WorkerFleetManager::default();
        let policy = WorkerFleetPolicy::default();
        manager.register_worker(attestation("worker-f"), &policy, 2_000).unwrap();

        let mut request = lease_request("run-5", 500);
        request.required_capabilities = vec!["tool:palyra.sleep".to_owned()];
        let error = manager
            .assign_next_work(request, &policy, 2_500)
            .expect_err("missing worker capability should fail closed");

        assert_eq!(error, WorkerLifecycleError::NoAvailableWorker);
        assert_eq!(manager.snapshot().active_leases, 0);
    }

    #[test]
    fn worker_cleanup_failure_records_failed_event_for_journal_surfaces() {
        let mut manager = WorkerFleetManager::default();
        let policy = WorkerFleetPolicy::default();
        manager.register_worker(attestation("worker-g"), &policy, 2_000).unwrap();
        manager.assign_work("worker-g", lease_request("run-6", 500), &policy, 2_500).unwrap();

        let outcome = manager
            .finalize_work(
                "worker-g",
                WorkerCleanupReport {
                    removed_workspace_scope: true,
                    removed_artifacts: false,
                    removed_logs: true,
                    failure_reason: Some("artifact cleanup failure".to_owned()),
                },
                3_000,
            )
            .expect("cleanup outcome should be returned for journal emission");

        assert!(!outcome.cleanup_succeeded);
        assert_eq!(outcome.event.state, WorkerLifecycleState::Failed);
        assert_eq!(outcome.event.reason_code, "worker.cleanup_failed");
        assert_eq!(manager.snapshot().failed_closed_workers, 1);
        let error = manager
            .assign_work("worker-g", lease_request("run-7", 500), &policy, 3_100)
            .expect_err("failed worker must stay fail closed");
        assert!(matches!(error, WorkerLifecycleError::WorkerFailClosed(_)));
    }

    #[test]
    fn operator_quarantine_and_drain_fail_closed() {
        let mut manager = WorkerFleetManager::default();
        let policy = WorkerFleetPolicy::default();
        manager.register_worker(attestation("worker-h"), &policy, 2_000).unwrap();
        manager.assign_work("worker-h", lease_request("run-8", 500), &policy, 2_500).unwrap();

        let quarantine = manager
            .quarantine_worker("worker-h", "worker.operator.quarantine", 2_750)
            .expect("operator quarantine should be recorded");
        assert_eq!(quarantine.state, WorkerLifecycleState::Failed);
        assert_eq!(quarantine.run_id.as_deref(), Some("run-8"));
        assert_eq!(manager.snapshot().failed_closed_workers, 1);

        manager.register_worker(attestation("worker-i"), &policy, 2_800).unwrap();
        let drain = manager.quarantine_all_workers("worker.operator.drain", 3_000);
        assert_eq!(drain.len(), 1);
        assert_eq!(drain[0].reason_code, "worker.operator.drain");
        assert_eq!(manager.snapshot().failed_closed_workers, 2);
    }

    #[test]
    fn operator_reverify_requires_fresh_attestation_and_no_active_lease() {
        let mut manager = WorkerFleetManager::default();
        let policy = WorkerFleetPolicy::default();
        manager.register_worker(attestation("worker-j"), &policy, 2_000).unwrap();
        manager.quarantine_worker("worker-j", "worker.operator.quarantine", 2_100).unwrap();

        let event = manager
            .reverify_worker("worker-j", &policy, 2_200)
            .expect("fresh attestation should restore the worker to registered");
        assert_eq!(event.state, WorkerLifecycleState::Registered);
        assert_eq!(event.reason_code, "worker.reverified_by_operator");
        manager.assign_work("worker-j", lease_request("run-9", 500), &policy, 2_500).unwrap();

        let error = manager
            .reverify_worker("worker-j", &policy, 2_600)
            .expect_err("active lease must not be reverified in place");
        assert!(matches!(error, WorkerLifecycleError::LeaseAlreadyActive(_)));
    }

    #[test]
    fn force_cleanup_promotes_only_verified_cleanup_reports() {
        let mut manager = WorkerFleetManager::default();
        let policy = WorkerFleetPolicy::default();
        manager.register_worker(attestation("worker-k"), &policy, 2_000).unwrap();
        manager.assign_work("worker-k", lease_request("run-10", 500), &policy, 2_500).unwrap();

        let failed = manager
            .force_cleanup_worker(
                "worker-k",
                WorkerCleanupReport {
                    removed_workspace_scope: true,
                    removed_artifacts: false,
                    removed_logs: true,
                    failure_reason: Some("operator could not remove artifact".to_owned()),
                },
                2_700,
            )
            .expect("cleanup report should be recorded");
        assert!(!failed.cleanup_succeeded);
        assert_eq!(failed.event.state, WorkerLifecycleState::Failed);

        let recovered = manager
            .force_cleanup_worker(
                "worker-k",
                WorkerCleanupReport {
                    removed_workspace_scope: true,
                    removed_artifacts: true,
                    removed_logs: true,
                    failure_reason: None,
                },
                2_900,
            )
            .expect("verified cleanup should be accepted");
        assert!(recovered.cleanup_succeeded);
        assert_eq!(recovered.event.state, WorkerLifecycleState::Completed);
        assert_eq!(manager.snapshot().failed_closed_workers, 0);
    }

    #[test]
    fn capability_matching_requires_worker_self_report_and_policy_trust() {
        let mut manager = WorkerFleetManager::default();
        let mut policy = WorkerFleetPolicy::default();
        policy.trusted_capabilities = vec!["tool:palyra.sleep".to_owned()];
        let mut attestation = attestation("worker-l");
        attestation.supported_capabilities = vec!["tool:palyra.sleep".to_owned()];
        manager.register_worker(attestation, &policy, 2_000).unwrap();

        let mut request = lease_request("run-11", 500);
        request.required_capabilities = vec!["tool:palyra.echo".to_owned()];
        let error = manager
            .assign_next_work(request, &policy, 2_500)
            .expect_err("untrusted capability must fail closed even if another tool is trusted");

        assert_eq!(error, WorkerLifecycleError::NoAvailableWorker);
        assert_eq!(manager.snapshot().active_leases, 0);
    }

    #[test]
    fn stale_heartbeat_marks_worker_offline_and_clears_active_lease() {
        let mut manager = WorkerFleetManager::default();
        let mut policy =
            WorkerFleetPolicy { heartbeat_timeout_ms: 100, ..WorkerFleetPolicy::default() };
        policy.trusted_capabilities = vec!["tool:palyra.echo".to_owned()];
        manager.register_worker(attestation("worker-m"), &policy, 2_000).unwrap();
        manager.assign_work("worker-m", lease_request("run-12", 500), &policy, 2_050).unwrap();

        let events = manager.mark_stale_heartbeat_workers(&policy, 2_250);

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].state, WorkerLifecycleState::Offline);
        assert_eq!(events[0].run_id.as_deref(), Some("run-12"));
        assert_eq!(manager.snapshot().offline_workers, 1);
        assert_eq!(manager.snapshot().active_leases, 0);
    }

    #[test]
    fn draining_worker_rejects_new_leases_without_quarantine() {
        let mut manager = WorkerFleetManager::default();
        let policy = WorkerFleetPolicy::default();
        manager.register_worker(attestation("worker-n"), &policy, 2_000).unwrap();

        let drain = manager
            .drain_worker("worker-n", "worker.operator.drain", 2_100)
            .expect("drain should be recorded");
        assert_eq!(drain.state, WorkerLifecycleState::Draining);

        let error = manager
            .assign_work("worker-n", lease_request("run-13", 500), &policy, 2_200)
            .expect_err("draining worker must not accept a new lease");
        assert!(matches!(error, WorkerLifecycleError::WorkerDraining(_)));
        assert_eq!(manager.snapshot().draining_workers, 1);
        assert_eq!(manager.snapshot().failed_closed_workers, 0);
    }

    #[test]
    fn compatibility_matrix_rejects_unversioned_worker_abi() {
        let mut manager = WorkerFleetManager::default();
        let policy = WorkerFleetPolicy {
            required_sdk_protocol_version: Some(2),
            required_wit_abi_version: Some("palyra-worker-abi/v2".to_owned()),
            ..WorkerFleetPolicy::default()
        };

        let error = manager
            .register_worker(attestation("worker-o"), &policy, 2_000)
            .expect_err("worker ABI mismatch must fail closed");

        assert!(matches!(error, WorkerLifecycleError::CompatibilityMismatch(_)));
        assert_eq!(manager.snapshot().registered_workers, 0);
    }
}
