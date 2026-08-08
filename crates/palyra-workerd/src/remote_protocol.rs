//! Canonical transport-neutral task and outcome contracts for remote workers.
//! The protocol binds policy, workspace, idempotency, cancellation, generations,
//! artifacts, resource usage, cleanup, and mutually authenticated transport evidence.

use std::path::{Component, Path};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    RuntimeGeneration, WorkerCleanupReport, WorkerRemoteToolRequestEnvelope,
    WorkerRemoteToolResultEnvelope,
};

/// Canonical protocol identifier for all remote worker transports.
pub const REMOTE_WORKER_PROTOCOL_V1: &str = "palyra.remote-worker/v1";
/// Current schema version of the canonical protocol.
pub const REMOTE_WORKER_PROTOCOL_SCHEMA_VERSION: u32 = 1;
const MAX_IDENTITY_BYTES: usize = 256;
const MAX_ARTIFACTS: usize = 128;
const MAX_PATCH_PATHS: usize = 1_024;
const MAX_CLOCK_SKEW_MS: i64 = 60_000;

/// Hash-addressed artifact transferred into or out of a remote task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentAddressedArtifact {
    /// Stable artifact identifier.
    pub artifact_id: String,
    /// SHA-256 digest of the artifact bytes.
    pub sha256: String,
    /// Bounded artifact size.
    pub size_bytes: u64,
    /// Non-secret media type used for dispatch and diagnostics.
    pub media_type: String,
}

/// Short-lived encrypted secret lease descriptor.
///
/// The encrypted bytes travel through the artifact channel identified by
/// `ciphertext_sha256`; workers must never persist the decrypted value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EncryptedWorkerSecretLease {
    /// Host-issued lease identity.
    pub lease_id: String,
    /// Digest of the encrypted payload.
    pub ciphertext_sha256: String,
    /// Digest of the recipient encryption key.
    pub recipient_key_sha256: String,
    /// Lease expiry enforced by host and worker.
    pub expires_at_unix_ms: i64,
    /// Must remain false; persistence is never delegated to a worker.
    pub persistence_allowed: bool,
}

/// Reviewed patch bundle returned by a mutating remote task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemotePatchBundle {
    /// SHA-256 digest of canonical patch bytes.
    pub patch_sha256: String,
    /// Workspace-relative paths touched by the patch.
    pub touched_paths: Vec<String>,
    /// Whether host review is required before authoritative mutation.
    pub review_required: bool,
}

/// Bounded worker resource accounting for one terminal outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteResourceUsage {
    /// Wall time consumed by the task.
    pub duration_ms: u64,
    /// Peak resident memory reported by the isolated worker.
    pub peak_memory_bytes: u64,
    /// CPU time consumed by the task.
    pub cpu_time_ms: u64,
    /// Bytes read from input artifacts.
    pub input_bytes: u64,
    /// Bytes written to output artifacts.
    pub output_bytes: u64,
}

/// Cleanup settlement for all task-scoped worker state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteCleanupAttestation {
    /// Whether the workspace scope was removed.
    pub workspace_removed: bool,
    /// Whether task artifacts were removed from mutable scratch storage.
    pub scratch_artifacts_removed: bool,
    /// Whether task logs were removed from worker-local storage.
    pub logs_removed: bool,
    /// Whether decrypted secret material was absent after cleanup.
    pub secret_material_removed: bool,
    /// Stable cleanup result reason.
    pub reason_code: String,
}

impl RemoteCleanupAttestation {
    /// Returns whether every required cleanup dimension was verified.
    #[must_use]
    pub const fn is_verified(&self) -> bool {
        self.workspace_removed
            && self.scratch_artifacts_removed
            && self.logs_removed
            && self.secret_material_removed
    }
}

/// Canonical host-to-worker task envelope shared by network, SSH, and desktop adapters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerTaskEnvelope {
    /// Stable task identity.
    pub task_id: String,
    /// Host request identity.
    pub request_id: String,
    /// Duplicate-suppression key retained across retries.
    pub idempotency_key: String,
    /// Stable identifier used for out-of-band cancellation.
    pub cancellation_id: String,
    /// Host issue timestamp.
    pub issued_at_unix_ms: i64,
    /// Hard terminal deadline.
    pub deadline_unix_ms: i64,
    /// SHA-256 digest of the exact policy and approval grant.
    pub policy_sha256: String,
    /// SHA-256 digest of the workspace manifest.
    pub workspace_manifest_sha256: String,
    /// SHA-256 digest of the canonical tool input.
    pub input_sha256: String,
    /// Canonical tool name.
    pub tool_name: String,
    /// Canonical JSON input.
    pub input_json: String,
    /// Content-addressed inputs available to the worker.
    pub input_artifacts: Vec<ContentAddressedArtifact>,
    /// Optional encrypted secret lease.
    pub secret_lease: Option<EncryptedWorkerSecretLease>,
    /// Host-issued runtime generation.
    pub run_generation: RuntimeGeneration,
    /// Side-effect fence generation.
    pub fence_generation: u64,
    /// WorkGraph claim identity when the task belongs to a graph.
    pub work_graph_claim_id: Option<String>,
    /// Bounded output budget.
    pub max_output_bytes: u64,
}

impl WorkerTaskEnvelope {
    /// Projects the established worker RPC envelope into the canonical task contract.
    #[must_use]
    pub fn from_remote_request(request: &WorkerRemoteToolRequestEnvelope) -> Self {
        let policy_sha256 = canonical_sha256(&(
            request.lease.grant_id.as_str(),
            request.lease.grant_tool_name.as_str(),
            request.lease.required_capabilities.as_slice(),
        ));
        Self {
            task_id: request.proposal_id.clone(),
            request_id: request.request_id.clone(),
            idempotency_key: canonical_sha256(&(
                request.lease.run_id.as_str(),
                request.proposal_id.as_str(),
                request.input_json_sha256.as_str(),
            )),
            cancellation_id: canonical_sha256(&(
                "cancel",
                request.lease.lease_id.as_str(),
                request.request_id.as_str(),
            )),
            issued_at_unix_ms: request.lease.expires_at_unix_ms.saturating_sub(60_000),
            deadline_unix_ms: request.lease.expires_at_unix_ms,
            policy_sha256,
            workspace_manifest_sha256: request.workspace_transfer.workspace_manifest_sha256.clone(),
            input_sha256: request.input_json_sha256.clone(),
            tool_name: request.tool_name.clone(),
            input_json: request.input_json.clone(),
            input_artifacts: vec![ContentAddressedArtifact {
                artifact_id: "workspace-input-manifest".to_owned(),
                sha256: request.lease.artifact_transport.input_manifest_sha256.clone(),
                size_bytes: 0,
                media_type: "application/vnd.palyra.workspace-manifest+json".to_owned(),
            }],
            secret_lease: None,
            run_generation: request.lease.run_generation,
            fence_generation: request.lease.run_generation.get(),
            work_graph_claim_id: None,
            max_output_bytes: 512 * 1_024,
        }
    }

    /// Validates all authority, integrity, deadline, and boundedness invariants.
    ///
    /// # Errors
    /// Returns a typed fail-closed error for malformed identities, digests,
    /// clocks, duplicate keys, secret leases, artifact bounds, or patch authority.
    pub fn validate(&self, observed_at_unix_ms: i64) -> Result<(), RemoteWorkerProtocolError> {
        validate_identity(self.task_id.as_str(), "task_id")?;
        validate_identity(self.request_id.as_str(), "request_id")?;
        validate_identity(self.idempotency_key.as_str(), "idempotency_key")?;
        validate_identity(self.cancellation_id.as_str(), "cancellation_id")?;
        validate_identity(self.tool_name.as_str(), "tool_name")?;
        validate_sha256(self.idempotency_key.as_str(), "idempotency_key")?;
        validate_sha256(self.cancellation_id.as_str(), "cancellation_id")?;
        validate_sha256(self.policy_sha256.as_str(), "policy_sha256")?;
        validate_sha256(self.workspace_manifest_sha256.as_str(), "workspace_manifest_sha256")?;
        validate_sha256(self.input_sha256.as_str(), "input_sha256")?;
        if self.input_sha256 != sha256_hex(self.input_json.as_bytes()) {
            return Err(RemoteWorkerProtocolError::DigestMismatch { field: "input_sha256" });
        }
        if self.issued_at_unix_ms > observed_at_unix_ms.saturating_add(MAX_CLOCK_SKEW_MS) {
            return Err(RemoteWorkerProtocolError::ClockSkew);
        }
        if self.deadline_unix_ms <= observed_at_unix_ms
            || self.deadline_unix_ms <= self.issued_at_unix_ms
        {
            return Err(RemoteWorkerProtocolError::DeadlineExpired);
        }
        if self.run_generation.get() == 0
            || self.fence_generation == 0
            || self.fence_generation != self.run_generation.get()
        {
            return Err(RemoteWorkerProtocolError::GenerationMismatch);
        }
        if self.max_output_bytes == 0 || self.input_artifacts.len() > MAX_ARTIFACTS {
            return Err(RemoteWorkerProtocolError::ResourceBoundsInvalid);
        }
        for artifact in &self.input_artifacts {
            artifact.validate()?;
        }
        if let Some(secret) = self.secret_lease.as_ref() {
            secret.validate(observed_at_unix_ms, self.deadline_unix_ms)?;
        }
        Ok(())
    }
}

impl ContentAddressedArtifact {
    fn validate(&self) -> Result<(), RemoteWorkerProtocolError> {
        validate_identity(self.artifact_id.as_str(), "artifact_id")?;
        validate_identity(self.media_type.as_str(), "media_type")?;
        validate_sha256(self.sha256.as_str(), "artifact_sha256")
    }
}

impl EncryptedWorkerSecretLease {
    fn validate(
        &self,
        observed_at_unix_ms: i64,
        task_deadline_unix_ms: i64,
    ) -> Result<(), RemoteWorkerProtocolError> {
        validate_identity(self.lease_id.as_str(), "secret_lease_id")?;
        validate_sha256(self.ciphertext_sha256.as_str(), "ciphertext_sha256")?;
        validate_sha256(self.recipient_key_sha256.as_str(), "recipient_key_sha256")?;
        if self.persistence_allowed {
            return Err(RemoteWorkerProtocolError::SecretPersistenceRequested);
        }
        if self.expires_at_unix_ms <= observed_at_unix_ms
            || self.expires_at_unix_ms > task_deadline_unix_ms
        {
            return Err(RemoteWorkerProtocolError::SecretLeaseInvalid);
        }
        Ok(())
    }
}

/// Worker-to-host terminal outcome bound to one canonical task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteTaskOutcome {
    /// Task identity copied from the request.
    pub task_id: String,
    /// Idempotency key copied from the request.
    pub idempotency_key: String,
    /// Attested worker identity.
    pub worker_id: String,
    /// Monotonic response sequence; terminal response is exactly sequence one.
    pub response_sequence: u64,
    /// Host-issued runtime generation.
    pub run_generation: RuntimeGeneration,
    /// Side-effect fence generation.
    pub fence_generation: u64,
    /// Whether the tool itself succeeded.
    pub success: bool,
    /// SHA-256 digest of the bounded output document.
    pub output_sha256: String,
    /// Content-addressed output artifacts.
    pub output_artifacts: Vec<ContentAddressedArtifact>,
    /// Optional reviewed patch bundle.
    pub patch_bundle: Option<RemotePatchBundle>,
    /// Bounded resource accounting.
    pub usage: RemoteResourceUsage,
    /// Verified cleanup evidence.
    pub cleanup: RemoteCleanupAttestation,
    /// Worker completion timestamp.
    pub completed_at_unix_ms: i64,
    /// Stable terminal reason.
    pub reason_code: String,
}

impl RemoteTaskOutcome {
    /// Projects an established worker RPC result into the canonical outcome contract.
    #[must_use]
    pub fn from_remote_result(
        request: &WorkerRemoteToolRequestEnvelope,
        result: &WorkerRemoteToolResultEnvelope,
    ) -> Self {
        Self {
            task_id: request.proposal_id.clone(),
            idempotency_key: WorkerTaskEnvelope::from_remote_request(request).idempotency_key,
            worker_id: result.worker_id.clone(),
            response_sequence: 1,
            run_generation: result.run_generation,
            fence_generation: result.run_generation.get(),
            success: result.success,
            output_sha256: result.output_json_sha256.clone(),
            output_artifacts: vec![ContentAddressedArtifact {
                artifact_id: "output-manifest".to_owned(),
                sha256: result.output_manifest_sha256.clone(),
                size_bytes: 0,
                media_type: "application/vnd.palyra.worker-output-manifest+json".to_owned(),
            }],
            patch_bundle: None,
            usage: RemoteResourceUsage {
                duration_ms: 0,
                peak_memory_bytes: 0,
                cpu_time_ms: 0,
                input_bytes: u64::try_from(request.input_json.len()).unwrap_or(u64::MAX),
                output_bytes: u64::try_from(result.output_json.len()).unwrap_or(u64::MAX),
            },
            cleanup: RemoteCleanupAttestation::from(&result.cleanup_report),
            completed_at_unix_ms: result.completed_at_unix_ms,
            reason_code: if result.success {
                "worker.task.succeeded"
            } else {
                "worker.task.failed"
            }
            .to_owned(),
        }
    }

    /// Validates exact request binding, sequence, artifact, patch, and cleanup evidence.
    ///
    /// # Errors
    /// Returns a typed error when a late, duplicate, stale, malicious, or
    /// incompletely cleaned result attempts to settle.
    pub fn validate_against(
        &self,
        task: &WorkerTaskEnvelope,
        observed_at_unix_ms: i64,
    ) -> Result<(), RemoteWorkerProtocolError> {
        task.validate(observed_at_unix_ms.min(task.deadline_unix_ms.saturating_sub(1)))?;
        validate_identity(self.worker_id.as_str(), "worker_id")?;
        validate_identity(self.reason_code.as_str(), "reason_code")?;
        validate_sha256(self.idempotency_key.as_str(), "idempotency_key")?;
        validate_sha256(self.output_sha256.as_str(), "output_sha256")?;
        if self.task_id != task.task_id || self.idempotency_key != task.idempotency_key {
            return Err(RemoteWorkerProtocolError::TaskBindingMismatch);
        }
        if self.response_sequence != 1 {
            return Err(RemoteWorkerProtocolError::ResponseSequenceInvalid);
        }
        if self.run_generation != task.run_generation
            || self.fence_generation != task.fence_generation
        {
            return Err(RemoteWorkerProtocolError::GenerationMismatch);
        }
        if observed_at_unix_ms > task.deadline_unix_ms.saturating_add(MAX_CLOCK_SKEW_MS)
            || self.completed_at_unix_ms > task.deadline_unix_ms
        {
            return Err(RemoteWorkerProtocolError::LateOutcome);
        }
        if self.output_artifacts.len() > MAX_ARTIFACTS {
            return Err(RemoteWorkerProtocolError::ResourceBoundsInvalid);
        }
        for artifact in &self.output_artifacts {
            artifact.validate()?;
        }
        if let Some(patch) = self.patch_bundle.as_ref() {
            patch.validate()?;
        }
        if !self.cleanup.is_verified() {
            return Err(RemoteWorkerProtocolError::CleanupIncomplete);
        }
        Ok(())
    }
}

impl From<&WorkerCleanupReport> for RemoteCleanupAttestation {
    fn from(report: &WorkerCleanupReport) -> Self {
        let verified = report.is_verified();
        Self {
            workspace_removed: report.removed_workspace_scope,
            scratch_artifacts_removed: report.removed_artifacts,
            logs_removed: report.removed_logs,
            secret_material_removed: verified,
            reason_code: report.failure_reason.clone().unwrap_or_else(|| {
                if verified { "worker.cleanup.ok" } else { "worker.cleanup.incomplete" }.to_owned()
            }),
        }
    }
}

impl RemotePatchBundle {
    fn validate(&self) -> Result<(), RemoteWorkerProtocolError> {
        validate_sha256(self.patch_sha256.as_str(), "patch_sha256")?;
        if !self.review_required || self.touched_paths.len() > MAX_PATCH_PATHS {
            return Err(RemoteWorkerProtocolError::PatchAuthorityInvalid);
        }
        for raw in &self.touched_paths {
            let path = Path::new(raw);
            if raw.trim().is_empty()
                || path.is_absolute()
                || path.components().any(|component| {
                    matches!(
                        component,
                        Component::ParentDir | Component::RootDir | Component::Prefix(_)
                    )
                })
            {
                return Err(RemoteWorkerProtocolError::PatchPathEscapesWorkspace {
                    path: raw.clone(),
                });
            }
        }
        Ok(())
    }
}

/// Trust wrapper binding a canonical task to the authenticated transport and worker claims.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteWorkerProtocolV1 {
    /// Canonical protocol identifier.
    pub protocol: String,
    /// Contract schema version.
    pub schema_version: u32,
    /// SHA-256 digest of the mutually authenticated transport identity.
    pub mutual_auth_binding_sha256: String,
    /// SHA-256 digest of worker attestation claims.
    pub worker_attestation_sha256: String,
    /// Canonical task envelope.
    pub task: WorkerTaskEnvelope,
}

impl RemoteWorkerProtocolV1 {
    /// Creates a protocol wrapper from the established mTLS-bound request.
    #[must_use]
    pub fn from_remote_request(request: &WorkerRemoteToolRequestEnvelope) -> Self {
        Self {
            protocol: REMOTE_WORKER_PROTOCOL_V1.to_owned(),
            schema_version: REMOTE_WORKER_PROTOCOL_SCHEMA_VERSION,
            mutual_auth_binding_sha256: canonical_sha256(&(
                request.lease.worker_id.as_str(),
                request.lease.session_id.as_str(),
                request.lease.run_generation.get(),
            )),
            worker_attestation_sha256: canonical_sha256(&request.worker_identity),
            task: WorkerTaskEnvelope::from_remote_request(request),
        }
    }

    /// Validates protocol identity, trust bindings, and the nested task.
    ///
    /// # Errors
    /// Returns a typed error when any transport, attestation, or task invariant fails.
    pub fn validate(&self, observed_at_unix_ms: i64) -> Result<(), RemoteWorkerProtocolError> {
        if self.protocol != REMOTE_WORKER_PROTOCOL_V1
            || self.schema_version != REMOTE_WORKER_PROTOCOL_SCHEMA_VERSION
        {
            return Err(RemoteWorkerProtocolError::ProtocolMismatch);
        }
        validate_sha256(self.mutual_auth_binding_sha256.as_str(), "mutual_auth_binding_sha256")?;
        validate_sha256(self.worker_attestation_sha256.as_str(), "worker_attestation_sha256")?;
        self.task.validate(observed_at_unix_ms)
    }
}

/// Fail-closed canonical remote worker contract errors.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum RemoteWorkerProtocolError {
    /// Protocol identifier or schema is unsupported.
    #[error("remote worker protocol or schema mismatch")]
    ProtocolMismatch,
    /// Required identity is missing or exceeds its bound.
    #[error("remote worker field {field} is missing or oversized")]
    InvalidIdentity { field: &'static str },
    /// SHA-256 field is malformed.
    #[error("remote worker field {field} is not a lowercase SHA-256 digest")]
    InvalidDigest { field: &'static str },
    /// Payload bytes do not match their declared digest.
    #[error("remote worker digest mismatch for {field}")]
    DigestMismatch { field: &'static str },
    /// Host and worker clocks exceed the accepted issuance skew.
    #[error("remote worker task clock skew exceeds the accepted bound")]
    ClockSkew,
    /// Task deadline is absent or expired.
    #[error("remote worker task deadline is expired")]
    DeadlineExpired,
    /// Runtime or side-effect fence generations differ.
    #[error("remote worker generation or fence mismatch")]
    GenerationMismatch,
    /// Artifact or output bounds are invalid.
    #[error("remote worker resource bounds are invalid")]
    ResourceBoundsInvalid,
    /// A worker was asked to persist secret material.
    #[error("remote worker secret persistence is forbidden")]
    SecretPersistenceRequested,
    /// Secret lease expiry is invalid.
    #[error("remote worker secret lease is expired or outlives the task")]
    SecretLeaseInvalid,
    /// Outcome does not bind to the exact task and idempotency key.
    #[error("remote worker outcome task binding mismatch")]
    TaskBindingMismatch,
    /// A duplicate or out-of-order terminal response was received.
    #[error("remote worker response sequence is invalid")]
    ResponseSequenceInvalid,
    /// Outcome arrived after its authoritative deadline.
    #[error("remote worker outcome arrived after its deadline")]
    LateOutcome,
    /// Cleanup evidence is incomplete.
    #[error("remote worker cleanup evidence is incomplete")]
    CleanupIncomplete,
    /// Patch lacks mandatory review or exceeds path bounds.
    #[error("remote worker patch authority is invalid")]
    PatchAuthorityInvalid,
    /// Patch path escapes the leased workspace.
    #[error("remote worker patch path escapes the workspace: {path}")]
    PatchPathEscapesWorkspace { path: String },
}

fn validate_identity(value: &str, field: &'static str) -> Result<(), RemoteWorkerProtocolError> {
    if value.trim().is_empty() || value.len() > MAX_IDENTITY_BYTES {
        return Err(RemoteWorkerProtocolError::InvalidIdentity { field });
    }
    Ok(())
}

fn validate_sha256(value: &str, field: &'static str) -> Result<(), RemoteWorkerProtocolError> {
    if value.len() != 64
        || value
            .chars()
            .any(|character| !character.is_ascii_hexdigit() || character.is_ascii_uppercase())
    {
        return Err(RemoteWorkerProtocolError::InvalidDigest { field });
    }
    Ok(())
}

fn canonical_sha256<T: Serialize>(value: &T) -> String {
    let bytes = serde_json::to_vec(value).unwrap_or_default();
    sha256_hex(bytes.as_slice())
}

fn sha256_hex(value: &[u8]) -> String {
    hex::encode(Sha256::digest(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        WorkerArtifactTransport, WorkerRemoteIdentity, WorkerRemoteLeaseBinding,
        WorkerRemoteToolKind, WorkerRemoteWorkspaceTransfer, WorkerWorkspaceScope,
        WORKER_REMOTE_TOOL_PROTOCOL, WORKER_REMOTE_TOOL_SCHEMA_VERSION,
    };

    fn remote_request() -> WorkerRemoteToolRequestEnvelope {
        let input_json = r#"{"path":"README.md"}"#.to_owned();
        WorkerRemoteToolRequestEnvelope {
            protocol: WORKER_REMOTE_TOOL_PROTOCOL.to_owned(),
            schema_version: WORKER_REMOTE_TOOL_SCHEMA_VERSION,
            request_id: "request-1".to_owned(),
            proposal_id: "proposal-1".to_owned(),
            tool_name: "palyra.fs.read_file".to_owned(),
            tool_kind: WorkerRemoteToolKind::FsRead,
            input_json_sha256: sha256_hex(input_json.as_bytes()),
            input_json,
            lease: WorkerRemoteLeaseBinding {
                lease_id: "lease-1".to_owned(),
                worker_id: "worker-1".to_owned(),
                session_id: "session-1".to_owned(),
                run_id: "run-1".to_owned(),
                run_generation: RuntimeGeneration::new(7).expect("generation"),
                grant_id: "grant-1".to_owned(),
                grant_tool_name: "palyra.fs.read_file".to_owned(),
                expires_at_unix_ms: 120_000,
                required_capabilities: vec!["tool:palyra.fs.read_file".to_owned()],
                workspace_scope: WorkerWorkspaceScope {
                    workspace_root: "/workspace".to_owned(),
                    allowed_paths: vec!["README.md".to_owned()],
                    read_only: true,
                },
                artifact_transport: WorkerArtifactTransport {
                    input_manifest_sha256: "1".repeat(64),
                    output_manifest_sha256: "2".repeat(64),
                    log_stream_id: "logs-1".to_owned(),
                    scratch_directory_id: "scratch-1".to_owned(),
                },
            },
            worker_identity: WorkerRemoteIdentity {
                worker_id: "worker-1".to_owned(),
                image_digest_sha256: "3".repeat(64),
                build_digest_sha256: "4".repeat(64),
                artifact_digest_sha256: "5".repeat(64),
                capability_authority_sha256: Some("6".repeat(64)),
                sdk_protocol_version: 1,
                wit_abi_version: "palyra-worker-abi/v1".to_owned(),
            },
            workspace_transfer: WorkerRemoteWorkspaceTransfer::manifest("7".repeat(64)),
            canonical_protocol: None,
        }
    }

    #[test]
    fn canonical_task_binds_policy_deadline_idempotency_and_fence() {
        let protocol = RemoteWorkerProtocolV1::from_remote_request(&remote_request());
        protocol.validate(60_000).expect("canonical task should validate");
        assert_eq!(protocol.task.deadline_unix_ms, 120_000);
        assert_eq!(protocol.task.run_generation.get(), protocol.task.fence_generation);
        assert_eq!(protocol.task.idempotency_key.len(), 64);
        assert_eq!(protocol.task.cancellation_id.len(), 64);
    }

    #[test]
    fn canonical_task_rejects_clock_skew_and_persistent_secret() {
        let mut protocol = RemoteWorkerProtocolV1::from_remote_request(&remote_request());
        protocol.task.issued_at_unix_ms = 200_001;
        assert_eq!(protocol.validate(60_000), Err(RemoteWorkerProtocolError::ClockSkew));

        protocol.task.issued_at_unix_ms = 60_000;
        protocol.task.secret_lease = Some(EncryptedWorkerSecretLease {
            lease_id: "secret-1".to_owned(),
            ciphertext_sha256: "8".repeat(64),
            recipient_key_sha256: "9".repeat(64),
            expires_at_unix_ms: 100_000,
            persistence_allowed: true,
        });
        assert_eq!(
            protocol.validate(60_000),
            Err(RemoteWorkerProtocolError::SecretPersistenceRequested)
        );
    }

    #[test]
    fn remote_outcome_rejects_duplicate_sequence_and_malicious_patch_path() {
        let request = remote_request();
        let task = WorkerTaskEnvelope::from_remote_request(&request);
        let mut outcome = RemoteTaskOutcome {
            task_id: task.task_id.clone(),
            idempotency_key: task.idempotency_key.clone(),
            worker_id: "worker-1".to_owned(),
            response_sequence: 2,
            run_generation: task.run_generation,
            fence_generation: task.fence_generation,
            success: true,
            output_sha256: "a".repeat(64),
            output_artifacts: Vec::new(),
            patch_bundle: None,
            usage: RemoteResourceUsage {
                duration_ms: 1,
                peak_memory_bytes: 1,
                cpu_time_ms: 1,
                input_bytes: 1,
                output_bytes: 1,
            },
            cleanup: RemoteCleanupAttestation {
                workspace_removed: true,
                scratch_artifacts_removed: true,
                logs_removed: true,
                secret_material_removed: true,
                reason_code: "worker.cleanup.ok".to_owned(),
            },
            completed_at_unix_ms: 80_000,
            reason_code: "worker.task.succeeded".to_owned(),
        };
        assert_eq!(
            outcome.validate_against(&task, 80_000),
            Err(RemoteWorkerProtocolError::ResponseSequenceInvalid)
        );

        outcome.response_sequence = 1;
        outcome.patch_bundle = Some(RemotePatchBundle {
            patch_sha256: "b".repeat(64),
            touched_paths: vec!["../escape.txt".to_owned()],
            review_required: true,
        });
        assert_eq!(
            outcome.validate_against(&task, 80_000),
            Err(RemoteWorkerProtocolError::PatchPathEscapesWorkspace {
                path: "../escape.txt".to_owned()
            })
        );
    }
}
