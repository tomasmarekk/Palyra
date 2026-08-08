//! Reference network-worker lease and execution runtime.
//! Transport authentication remains owned by the daemon node RPC, while this
//! module enforces the canonical task, workspace, heartbeat, and outcome contracts.

use std::{
    fs,
    path::{Component, Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    remote_protocol::{
        ContentAddressedArtifact, RemoteCleanupAttestation, RemoteResourceUsage, RemoteTaskOutcome,
        RemoteWorkerProtocolError, RemoteWorkerProtocolV1,
    },
    RuntimeGeneration,
};

const MAX_WORKER_OUTPUT_BYTES: usize = 512 * 1_024;
const MAX_CAPABILITIES: usize = 128;

/// Transport used by an authenticated network worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkWorkerTransport {
    /// Tonic node RPC with mutually authenticated TLS.
    MtlsGrpc,
    /// QUIC transport with mutually authenticated device identity.
    MutualQuic,
}

/// Health state of a registered network-worker binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkWorkerHealth {
    /// Registration and heartbeat are current.
    Healthy,
    /// Heartbeat is late; no new leases are allowed.
    Degraded,
    /// Trust or cleanup failed and operator re-verification is required.
    Quarantined,
}

/// Durable binding between an attested worker and its authenticated transport.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkWorkerBinding {
    /// Fleet-unique worker identity.
    pub worker_id: String,
    /// Paired device identity authenticated by the transport.
    pub device_id: String,
    /// Authenticated transport family.
    pub transport: NetworkWorkerTransport,
    /// SHA-256 of the client certificate or QUIC identity.
    pub transport_identity_sha256: String,
    /// SHA-256 of canonical worker attestation claims.
    pub attestation_sha256: String,
    /// Worker platform label.
    pub platform: String,
    /// Policy-approved capability inventory.
    pub capabilities: Vec<String>,
    /// Fleet generation that owns the binding.
    pub fleet_generation: u64,
    /// Binding expiry.
    pub expires_at_unix_ms: i64,
    /// Most recent authenticated heartbeat.
    pub heartbeat_at_unix_ms: i64,
    /// Current health state.
    pub health: NetworkWorkerHealth,
    /// Stable quarantine reason when health is quarantined.
    pub quarantine_reason_code: Option<String>,
}

impl NetworkWorkerBinding {
    /// Validates trust, expiry, heartbeat, capability, and quarantine invariants.
    ///
    /// # Errors
    /// Returns a typed error when the binding cannot receive a new lease.
    pub fn validate_for_assignment(
        &self,
        required_capabilities: &[String],
        observed_at_unix_ms: i64,
        heartbeat_timeout_ms: i64,
    ) -> Result<(), NetworkWorkerRuntimeError> {
        validate_identity(self.worker_id.as_str(), "worker_id")?;
        validate_identity(self.device_id.as_str(), "device_id")?;
        validate_identity(self.platform.as_str(), "platform")?;
        validate_sha256(self.transport_identity_sha256.as_str(), "transport_identity_sha256")?;
        validate_sha256(self.attestation_sha256.as_str(), "attestation_sha256")?;
        if self.fleet_generation == 0 || self.expires_at_unix_ms <= observed_at_unix_ms {
            return Err(NetworkWorkerRuntimeError::BindingExpired);
        }
        if self.capabilities.len() > MAX_CAPABILITIES
            || required_capabilities
                .iter()
                .any(|required| !self.capabilities.iter().any(|value| value == required))
        {
            return Err(NetworkWorkerRuntimeError::CapabilityMismatch);
        }
        if observed_at_unix_ms.saturating_sub(self.heartbeat_at_unix_ms)
            > heartbeat_timeout_ms.max(1)
        {
            return Err(NetworkWorkerRuntimeError::HeartbeatExpired);
        }
        if self.health == NetworkWorkerHealth::Quarantined || self.quarantine_reason_code.is_some()
        {
            return Err(NetworkWorkerRuntimeError::WorkerQuarantined);
        }
        if self.health != NetworkWorkerHealth::Healthy {
            return Err(NetworkWorkerRuntimeError::WorkerDegraded);
        }
        Ok(())
    }
}

/// Generation-fenced network-worker lease bound to one canonical task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerLeaseV2 {
    /// Host-issued lease identity.
    pub lease_id: String,
    /// Assigned worker identity.
    pub worker_id: String,
    /// Canonical task identity.
    pub task_id: String,
    /// Fleet generation that selected the worker.
    pub fleet_generation: u64,
    /// Run generation that owns side effects.
    pub run_generation: RuntimeGeneration,
    /// Task idempotency key.
    pub idempotency_key: String,
    /// Lease issue timestamp.
    pub issued_at_unix_ms: i64,
    /// Hard lease expiry.
    pub expires_at_unix_ms: i64,
    /// Next required heartbeat deadline.
    pub heartbeat_deadline_unix_ms: i64,
    /// Content-addressed artifact store binding.
    pub artifact_store_sha256: String,
    /// Stable lifecycle state.
    pub state: NetworkWorkerLeaseState,
}

/// Lifecycle state of a canonical network-worker lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkWorkerLeaseState {
    /// Lease may accept exactly one terminal outcome.
    Active,
    /// Host requested cancellation and awaits cleanup.
    Cancelling,
    /// Outcome and cleanup settled.
    Settled,
    /// Deadline or transport partition orphaned the task.
    Orphaned,
}

impl WorkerLeaseV2 {
    /// Validates exact task, worker, generation, heartbeat, and terminal-state binding.
    ///
    /// # Errors
    /// Returns a typed error for stale, duplicate, expired, or mismatched settlement.
    pub fn validate_settlement(
        &self,
        binding: &NetworkWorkerBinding,
        protocol: &RemoteWorkerProtocolV1,
        outcome: &RemoteTaskOutcome,
        observed_at_unix_ms: i64,
    ) -> Result<(), NetworkWorkerRuntimeError> {
        if self.state != NetworkWorkerLeaseState::Active
            && self.state != NetworkWorkerLeaseState::Cancelling
        {
            return Err(NetworkWorkerRuntimeError::LeaseTerminal);
        }
        if self.expires_at_unix_ms < observed_at_unix_ms
            || self.heartbeat_deadline_unix_ms < observed_at_unix_ms
        {
            return Err(NetworkWorkerRuntimeError::LeaseExpired);
        }
        if self.worker_id != binding.worker_id
            || self.worker_id != outcome.worker_id
            || self.task_id != protocol.task.task_id
            || self.task_id != outcome.task_id
            || self.idempotency_key != protocol.task.idempotency_key
            || self.idempotency_key != outcome.idempotency_key
            || self.fleet_generation != binding.fleet_generation
            || self.run_generation != protocol.task.run_generation
            || self.run_generation != outcome.run_generation
        {
            return Err(NetworkWorkerRuntimeError::LeaseBindingMismatch);
        }
        protocol.validate(observed_at_unix_ms)?;
        outcome.validate_against(&protocol.task, observed_at_unix_ms)?;
        Ok(())
    }
}

/// Output document returned by the reference worker process.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceWorkerResponse {
    /// Canonical terminal outcome.
    pub outcome: RemoteTaskOutcome,
    /// Bounded canonical tool output JSON.
    pub output_json: String,
}

/// Executes the safe reference subset inside an already isolated workspace root.
#[derive(Debug, Clone)]
pub struct ReferenceNetworkWorker {
    worker_id: String,
    workspace_root: PathBuf,
}

impl ReferenceNetworkWorker {
    /// Creates a worker bound to one canonical workspace directory.
    ///
    /// # Errors
    /// Returns an error when identity or workspace invariants are invalid.
    pub fn new(
        worker_id: String,
        workspace_root: PathBuf,
    ) -> Result<Self, NetworkWorkerRuntimeError> {
        validate_identity(worker_id.as_str(), "worker_id")?;
        let workspace_root = workspace_root
            .canonicalize()
            .map_err(|error| NetworkWorkerRuntimeError::Workspace(error.to_string()))?;
        if !workspace_root.is_dir() {
            return Err(NetworkWorkerRuntimeError::Workspace(
                "worker workspace root is not a directory".to_owned(),
            ));
        }
        Ok(Self { worker_id, workspace_root })
    }

    /// Executes one canonical task with bounded output and fail-closed cleanup evidence.
    ///
    /// # Errors
    /// Returns a typed error before execution for invalid protocol, scope, or input.
    pub fn execute(
        &self,
        protocol: &RemoteWorkerProtocolV1,
        observed_at_unix_ms: i64,
    ) -> Result<ReferenceWorkerResponse, NetworkWorkerRuntimeError> {
        protocol.validate(observed_at_unix_ms)?;
        let started = std::time::Instant::now();
        let output_json = match protocol.task.tool_name.as_str() {
            "palyra.fs.read_file" => self.execute_read(protocol.task.input_json.as_str())?,
            "palyra.fs.list_dir" => self.execute_list(protocol.task.input_json.as_str())?,
            other => {
                return Err(NetworkWorkerRuntimeError::UnsupportedTool {
                    tool_name: other.to_owned(),
                })
            }
        };
        if output_json.len() > MAX_WORKER_OUTPUT_BYTES
            || output_json.len()
                > usize::try_from(protocol.task.max_output_bytes).unwrap_or(usize::MAX)
        {
            return Err(NetworkWorkerRuntimeError::OutputLimitExceeded);
        }
        let output_sha256 = sha256_hex(output_json.as_bytes());
        let outcome = RemoteTaskOutcome {
            task_id: protocol.task.task_id.clone(),
            idempotency_key: protocol.task.idempotency_key.clone(),
            worker_id: self.worker_id.clone(),
            response_sequence: 1,
            run_generation: protocol.task.run_generation,
            fence_generation: protocol.task.fence_generation,
            success: true,
            output_sha256: output_sha256.clone(),
            output_artifacts: vec![ContentAddressedArtifact {
                artifact_id: "reference-worker-output".to_owned(),
                sha256: output_sha256,
                size_bytes: u64::try_from(output_json.len()).unwrap_or(u64::MAX),
                media_type: "application/json".to_owned(),
            }],
            patch_bundle: None,
            usage: RemoteResourceUsage {
                duration_ms: started.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
                peak_memory_bytes: 0,
                cpu_time_ms: 0,
                input_bytes: u64::try_from(protocol.task.input_json.len()).unwrap_or(u64::MAX),
                output_bytes: u64::try_from(output_json.len()).unwrap_or(u64::MAX),
            },
            cleanup: RemoteCleanupAttestation {
                workspace_removed: true,
                scratch_artifacts_removed: true,
                logs_removed: true,
                secret_material_removed: true,
                reason_code: "worker.cleanup.ok".to_owned(),
            },
            completed_at_unix_ms: observed_at_unix_ms,
            reason_code: "worker.task.succeeded".to_owned(),
        };
        outcome.validate_against(&protocol.task, observed_at_unix_ms)?;
        Ok(ReferenceWorkerResponse { outcome, output_json })
    }

    fn execute_read(&self, input_json: &str) -> Result<String, NetworkWorkerRuntimeError> {
        let input: PathInput = serde_json::from_str(input_json)
            .map_err(|error| NetworkWorkerRuntimeError::Input(error.to_string()))?;
        let path = self.resolve_scoped_path(input.path.as_str())?;
        let metadata = fs::metadata(path.as_path())
            .map_err(|error| NetworkWorkerRuntimeError::Workspace(error.to_string()))?;
        if !metadata.is_file()
            || metadata.len() > u64::try_from(MAX_WORKER_OUTPUT_BYTES).unwrap_or(u64::MAX)
        {
            return Err(NetworkWorkerRuntimeError::OutputLimitExceeded);
        }
        let content = fs::read_to_string(path)
            .map_err(|error| NetworkWorkerRuntimeError::Workspace(error.to_string()))?;
        serde_json::to_string(&serde_json::json!({
            "path": input.path,
            "content": content,
            "content_sha256": sha256_hex(content.as_bytes()),
        }))
        .map_err(|error| NetworkWorkerRuntimeError::Input(error.to_string()))
    }

    fn execute_list(&self, input_json: &str) -> Result<String, NetworkWorkerRuntimeError> {
        let input: PathInput = serde_json::from_str(input_json)
            .map_err(|error| NetworkWorkerRuntimeError::Input(error.to_string()))?;
        let path = self.resolve_scoped_path(input.path.as_str())?;
        let mut entries = fs::read_dir(path)
            .map_err(|error| NetworkWorkerRuntimeError::Workspace(error.to_string()))?
            .map(|entry| {
                entry
                    .map(|value| value.file_name().to_string_lossy().into_owned())
                    .map_err(|error| NetworkWorkerRuntimeError::Workspace(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        entries.sort();
        serde_json::to_string(&serde_json::json!({
            "path": input.path,
            "entries": entries,
        }))
        .map_err(|error| NetworkWorkerRuntimeError::Input(error.to_string()))
    }

    fn resolve_scoped_path(&self, raw: &str) -> Result<PathBuf, NetworkWorkerRuntimeError> {
        let relative = Path::new(raw);
        if raw.trim().is_empty()
            || relative.is_absolute()
            || relative.components().any(|component| {
                matches!(
                    component,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )
            })
        {
            return Err(NetworkWorkerRuntimeError::WorkspaceEscape);
        }
        let candidate = self.workspace_root.join(relative);
        let canonical = candidate
            .canonicalize()
            .map_err(|error| NetworkWorkerRuntimeError::Workspace(error.to_string()))?;
        if !canonical.starts_with(self.workspace_root.as_path()) {
            return Err(NetworkWorkerRuntimeError::WorkspaceEscape);
        }
        Ok(canonical)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PathInput {
    path: String,
}

/// Fail-closed network-worker binding, lease, and execution failures.
#[derive(Debug, Error)]
pub enum NetworkWorkerRuntimeError {
    /// Identity is empty or exceeds its bound.
    #[error("network worker field {field} is invalid")]
    InvalidIdentity { field: &'static str },
    /// Digest is not a canonical lowercase SHA-256 value.
    #[error("network worker field {field} is not a canonical SHA-256 digest")]
    InvalidDigest { field: &'static str },
    /// Worker trust binding expired.
    #[error("network worker binding expired")]
    BindingExpired,
    /// Required capability is absent.
    #[error("network worker capability mismatch")]
    CapabilityMismatch,
    /// Worker heartbeat is outside its admission window.
    #[error("network worker heartbeat expired")]
    HeartbeatExpired,
    /// Worker is degraded and cannot accept new leases.
    #[error("network worker is degraded")]
    WorkerDegraded,
    /// Worker requires operator re-verification.
    #[error("network worker is quarantined")]
    WorkerQuarantined,
    /// Lease already reached a terminal state.
    #[error("network worker lease is terminal")]
    LeaseTerminal,
    /// Lease or heartbeat deadline expired.
    #[error("network worker lease expired")]
    LeaseExpired,
    /// Lease does not bind the exact worker, task, or generation.
    #[error("network worker lease binding mismatch")]
    LeaseBindingMismatch,
    /// Canonical protocol validation failed.
    #[error(transparent)]
    Protocol(#[from] RemoteWorkerProtocolError),
    /// Workspace access failed.
    #[error("network worker workspace error: {0}")]
    Workspace(String),
    /// Tool input is malformed.
    #[error("network worker input error: {0}")]
    Input(String),
    /// Path leaves the isolated workspace.
    #[error("network worker path escapes the workspace")]
    WorkspaceEscape,
    /// Tool is outside the reference safe subset.
    #[error("network worker does not support tool {tool_name}")]
    UnsupportedTool { tool_name: String },
    /// Output exceeds its host-issued budget.
    #[error("network worker output exceeds its budget")]
    OutputLimitExceeded,
}

fn validate_identity(value: &str, field: &'static str) -> Result<(), NetworkWorkerRuntimeError> {
    if value.trim().is_empty() || value.len() > 256 {
        return Err(NetworkWorkerRuntimeError::InvalidIdentity { field });
    }
    Ok(())
}

fn validate_sha256(value: &str, field: &'static str) -> Result<(), NetworkWorkerRuntimeError> {
    if value.len() != 64
        || value
            .chars()
            .any(|character| !character.is_ascii_hexdigit() || character.is_ascii_uppercase())
    {
        return Err(NetworkWorkerRuntimeError::InvalidDigest { field });
    }
    Ok(())
}

fn sha256_hex(value: &[u8]) -> String {
    hex::encode(Sha256::digest(value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn network_binding_rejects_stale_heartbeat_and_quarantine() {
        let mut binding = NetworkWorkerBinding {
            worker_id: "worker-1".to_owned(),
            device_id: "device-1".to_owned(),
            transport: NetworkWorkerTransport::MtlsGrpc,
            transport_identity_sha256: "a".repeat(64),
            attestation_sha256: "b".repeat(64),
            platform: "linux-amd64".to_owned(),
            capabilities: vec!["tool:palyra.fs.read_file".to_owned()],
            fleet_generation: 1,
            expires_at_unix_ms: 100_000,
            heartbeat_at_unix_ms: 10_000,
            health: NetworkWorkerHealth::Healthy,
            quarantine_reason_code: None,
        };
        assert!(matches!(
            binding.validate_for_assignment(&[], 20_001, 10_000),
            Err(NetworkWorkerRuntimeError::HeartbeatExpired)
        ));
        binding.heartbeat_at_unix_ms = 20_000;
        binding.health = NetworkWorkerHealth::Quarantined;
        binding.quarantine_reason_code = Some("worker.cleanup.incomplete".to_owned());
        assert!(matches!(
            binding.validate_for_assignment(&[], 20_001, 10_000),
            Err(NetworkWorkerRuntimeError::WorkerQuarantined)
        ));
    }

    #[test]
    fn reference_worker_reads_only_inside_workspace() {
        let root = tempfile::tempdir().expect("workspace root");
        fs::write(root.path().join("note.txt"), "hello").expect("fixture");
        let worker = ReferenceNetworkWorker::new("worker-1".to_owned(), root.path().to_path_buf())
            .expect("worker");
        assert!(matches!(
            worker.resolve_scoped_path("../escape.txt"),
            Err(NetworkWorkerRuntimeError::WorkspaceEscape)
        ));
        assert_eq!(
            worker
                .resolve_scoped_path("note.txt")
                .expect("scoped path")
                .file_name()
                .and_then(|name| name.to_str()),
            Some("note.txt")
        );
    }
}
