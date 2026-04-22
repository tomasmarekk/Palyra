use std::collections::{BTreeMap, VecDeque};

pub use palyra_common::runtime_contracts::WorkerLifecycleState;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

const MAX_WORKER_ID_BYTES: usize = 128;
const MAX_GRANT_ID_BYTES: usize = 128;
const MAX_RECENT_LIFECYCLE_EVENTS: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerAttestation {
    pub worker_id: String,
    pub image_digest_sha256: String,
    pub build_digest_sha256: String,
    pub artifact_digest_sha256: String,
    pub egress_proxy_attested: bool,
    #[serde(default)]
    pub supported_capabilities: Vec<String>,
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
    pub orphaned_workers: usize,
    pub failed_closed_workers: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerFleetPolicy {
    pub max_ttl_ms: u64,
    pub attestation: WorkerAttestationExpectation,
}

impl Default for WorkerFleetPolicy {
    fn default() -> Self {
        Self { max_ttl_ms: 15 * 60 * 1_000, attestation: WorkerAttestationExpectation::default() }
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
    #[error("no attested worker is available for the requested capabilities")]
    NoAvailableWorker,
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
        if self.workers.contains_key(attestation.worker_id.as_str()) {
            return Err(WorkerLifecycleError::AlreadyRegistered(attestation.worker_id));
        }
        let worker_id = attestation.worker_id.clone();
        self.workers.insert(
            worker_id.clone(),
            WorkerRecord { attestation, state: WorkerLifecycleState::Registered, lease: None },
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

    fn push_recent_event(&mut self, event: WorkerLifecycleEvent) {
        self.recent_events.push_front(event);
        while self.recent_events.len() > MAX_RECENT_LIFECYCLE_EVENTS {
            self.recent_events.pop_back();
        }
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

fn assign_worker_record(
    worker_id: &str,
    worker: &mut WorkerRecord,
    request: WorkerLeaseRequest,
    policy: &WorkerFleetPolicy,
    now_unix_ms: i64,
) -> Result<(WorkerLease, WorkerLifecycleEvent), WorkerLifecycleError> {
    worker.attestation.validate(&policy.attestation, now_unix_ms)?;
    if worker.lease.is_some() {
        return Err(WorkerLifecycleError::LeaseAlreadyActive(worker_id.to_owned()));
    }
    if matches!(worker.state, WorkerLifecycleState::Failed | WorkerLifecycleState::Orphaned) {
        return Err(WorkerLifecycleError::WorkerFailClosed(worker_id.to_owned()));
    }
    if !worker_supports_capabilities(worker, request.required_capabilities.as_slice()) {
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
        && !matches!(worker.state, WorkerLifecycleState::Failed | WorkerLifecycleState::Orphaned)
        && worker.attestation.validate(&policy.attestation, now_unix_ms).is_ok()
        && worker_supports_capabilities(worker, request.required_capabilities.as_slice())
}

fn worker_supports_capabilities(worker: &WorkerRecord, required_capabilities: &[String]) -> bool {
    if required_capabilities.is_empty() {
        return true;
    }
    required_capabilities.iter().all(|required| {
        worker
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
}
