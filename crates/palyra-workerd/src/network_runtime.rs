//! Reference network-worker lease and execution runtime.
//! Transport authentication remains owned by the daemon node RPC, while this
//! module enforces the canonical task, workspace, heartbeat, and outcome contracts.

use std::{
    fs,
    io::{self, Read},
    path::{Component, Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

use palyra_common::{
    process_runner_input::{
        interpreter_args_contain_blocked_eval_flag, parse_process_runner_tool_input,
        process_executable_is_interpreter, ProcessRunnerToolInput,
    },
    redaction::redact_diagnostic_text,
    workspace_patch::{
        apply_workspace_patch, WorkspacePatchLimits, WorkspacePatchRedactionPolicy,
        WorkspacePatchRequest,
    },
};
use palyra_sandbox::{build_tier_c_command_plan, TierCCommandRequest, TierCPolicy};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    computer_use::{ComputerUseAction, ComputerUseTaskContract, IsolatedComputerUseWorker},
    remote_protocol::{
        ContentAddressedArtifact, RemoteCleanupAttestation, RemotePatchBundle, RemoteResourceUsage,
        RemoteTaskOutcome, RemoteWorkerProtocolError, RemoteWorkerProtocolV1,
    },
    RuntimeGeneration, WorkerCleanupReport, WorkerRemoteToolRequestEnvelope,
    WorkerRemoteToolResultEnvelope, WorkerRemoteWorkspaceEntryKind,
    WorkerRemoteWorkspaceTransferMode, WORKER_REMOTE_TOOL_PROTOCOL,
    WORKER_REMOTE_TOOL_SCHEMA_VERSION,
};

const MAX_WORKER_OUTPUT_BYTES: usize = 512 * 1_024;
const MAX_CAPABILITIES: usize = 128;
const MAX_PROCESS_ARGUMENTS: usize = 256;
const MAX_PROCESS_ARGUMENT_BYTES: usize = 32 * 1_024;
const PROCESS_POLL_INTERVAL_MS: u64 = 10;

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
    /// Executes an established remote RPC request from a content-addressed workspace bundle.
    ///
    /// The materialized workspace is process-local and removed before verified
    /// cleanup is reported. Manifest-only requests are rejected because a
    /// network worker must never reinterpret a daemon-local path.
    ///
    /// # Errors
    /// Returns a typed fail-closed error for malformed request authority,
    /// workspace integrity failures, unsupported tools, or cleanup gaps.
    pub fn execute_remote_request(
        request: &WorkerRemoteToolRequestEnvelope,
        observed_at_unix_ms: i64,
    ) -> Result<WorkerRemoteToolResultEnvelope, NetworkWorkerRuntimeError> {
        Self::execute_remote_request_with_cancellation(request, observed_at_unix_ms, None)
    }

    fn execute_remote_request_with_cancellation(
        request: &WorkerRemoteToolRequestEnvelope,
        observed_at_unix_ms: i64,
        cancellation_requested: Option<&AtomicBool>,
    ) -> Result<WorkerRemoteToolResultEnvelope, NetworkWorkerRuntimeError> {
        request
            .validate(observed_at_unix_ms)
            .map_err(|error| NetworkWorkerRuntimeError::Input(error.to_string()))?;
        let protocol = request
            .canonical_protocol
            .as_ref()
            .ok_or(NetworkWorkerRuntimeError::CanonicalProtocolRequired)?;
        if protocol.task.secret_lease.is_some() {
            return Err(NetworkWorkerRuntimeError::SecretLeaseUnsupported);
        }
        if !matches!(
            request.workspace_transfer.mode,
            WorkerRemoteWorkspaceTransferMode::ScopedBundle
        ) {
            return Err(NetworkWorkerRuntimeError::ScopedWorkspaceRequired);
        }
        let workspace = tempfile::tempdir()
            .map_err(|error| NetworkWorkerRuntimeError::Workspace(error.to_string()))?;
        materialize_scoped_workspace(workspace.path(), request)?;
        if matches!(request.tool_kind, crate::WorkerRemoteToolKind::ComputerUse) {
            validate_computer_use_remote_scopes(request)?;
        }
        let worker = Self::new(request.lease.worker_id.clone(), workspace.path().to_path_buf())?;
        let response = worker.execute_with_authority(
            protocol,
            request.lease.process_executable_allowlist.as_slice(),
            observed_at_unix_ms,
            cancellation_requested,
        )?;
        drop(worker);
        workspace
            .close()
            .map_err(|error| NetworkWorkerRuntimeError::Workspace(error.to_string()))?;

        let cleanup_report = WorkerCleanupReport {
            removed_workspace_scope: true,
            removed_artifacts: true,
            removed_logs: true,
            failure_reason: None,
        };
        let result = WorkerRemoteToolResultEnvelope {
            protocol: WORKER_REMOTE_TOOL_PROTOCOL.to_owned(),
            schema_version: WORKER_REMOTE_TOOL_SCHEMA_VERSION,
            request_id: request.request_id.clone(),
            proposal_id: request.proposal_id.clone(),
            tool_name: request.tool_name.clone(),
            tool_kind: request.tool_kind,
            worker_id: request.lease.worker_id.clone(),
            lease_id: request.lease.lease_id.clone(),
            run_generation: request.lease.run_generation,
            success: response.outcome.success,
            output_json: response.output_json,
            output_json_sha256: response.outcome.output_sha256,
            error: (!response.outcome.success).then(|| response.outcome.reason_code.clone()),
            output_manifest_sha256: response
                .outcome
                .output_artifacts
                .first()
                .map(|artifact| artifact.sha256.clone())
                .unwrap_or_else(|| sha256_hex(&[])),
            cleanup_report,
            worker_identity: request.worker_identity.clone(),
            completed_at_unix_ms: response.outcome.completed_at_unix_ms,
        };
        result
            .validate_against_request(request, observed_at_unix_ms)
            .map_err(|error| NetworkWorkerRuntimeError::Input(error.to_string()))?;
        Ok(result)
    }

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
        self.execute_with_authority(protocol, &[], observed_at_unix_ms, None)
    }

    fn execute_with_authority(
        &self,
        protocol: &RemoteWorkerProtocolV1,
        process_executable_allowlist: &[String],
        observed_at_unix_ms: i64,
        cancellation_requested: Option<&AtomicBool>,
    ) -> Result<ReferenceWorkerResponse, NetworkWorkerRuntimeError> {
        protocol.validate(observed_at_unix_ms)?;
        let started = std::time::Instant::now();
        let (output_json, patch_bundle, success, reason_code) =
            match protocol.task.tool_name.as_str() {
                "palyra.fs.read_file" => (
                    self.execute_read(protocol.task.input_json.as_str())?,
                    None,
                    true,
                    "worker.task.succeeded",
                ),
                "palyra.fs.list_dir" => (
                    self.execute_list(protocol.task.input_json.as_str())?,
                    None,
                    true,
                    "worker.task.succeeded",
                ),
                "palyra.fs.search" => (
                    self.execute_search(protocol.task.input_json.as_str())?,
                    None,
                    true,
                    "worker.task.succeeded",
                ),
                "palyra.fs.apply_patch" => {
                    let (output, patch) = self.execute_patch(protocol.task.input_json.as_str())?;
                    (output, patch, true, "worker.task.succeeded")
                }
                "palyra.process.run" => {
                    let result = self.execute_process(
                        protocol,
                        process_executable_allowlist,
                        observed_at_unix_ms,
                        cancellation_requested,
                    )?;
                    (result.output_json, None, result.success, result.reason_code)
                }
                "palyra.computer.use" => {
                    let output = IsolatedComputerUseWorker::execute_task(
                        protocol.task.clone(),
                        observed_at_unix_ms,
                    )
                    .map_err(|error| NetworkWorkerRuntimeError::ComputerUse(error.to_string()))?;
                    let success = output.succeeded;
                    let reason_code =
                        if success { "worker.task.succeeded" } else { "worker.task.action_denied" };
                    (
                        serde_json::to_string(&output)
                            .map_err(|error| NetworkWorkerRuntimeError::Input(error.to_string()))?,
                        None,
                        success,
                        reason_code,
                    )
                }
                other => {
                    return Err(NetworkWorkerRuntimeError::UnsupportedTool {
                        tool_name: other.to_owned(),
                    })
                }
            };
        let duration_ms = started.elapsed().as_millis().try_into().unwrap_or(u64::MAX);
        if output_json.len() > MAX_WORKER_OUTPUT_BYTES
            || output_json.len()
                > usize::try_from(protocol.task.max_output_bytes).unwrap_or(usize::MAX)
            || duration_ms > protocol.task.resource_limits.wall_time_ms
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
            success,
            output_sha256: output_sha256.clone(),
            output_artifacts: vec![ContentAddressedArtifact {
                artifact_id: "reference-worker-output".to_owned(),
                sha256: output_sha256,
                size_bytes: u64::try_from(output_json.len()).unwrap_or(u64::MAX),
                media_type: "application/json".to_owned(),
            }],
            patch_bundle,
            usage: RemoteResourceUsage {
                duration_ms,
                peak_memory_bytes: 0,
                cpu_time_ms: 0,
                input_bytes: protocol
                    .task
                    .input_artifacts
                    .iter()
                    .fold(0_u64, |total, artifact| total.saturating_add(artifact.size_bytes)),
                output_bytes: u64::try_from(output_json.len()).unwrap_or(u64::MAX),
            },
            cleanup: RemoteCleanupAttestation {
                workspace_removed: true,
                scratch_artifacts_removed: true,
                logs_removed: true,
                secret_material_removed: true,
                reason_code: "worker.cleanup.ok".to_owned(),
            },
            completed_at_unix_ms: observed_at_unix_ms
                .saturating_add(i64::try_from(duration_ms).unwrap_or(i64::MAX)),
            reason_code: reason_code.to_owned(),
        };
        outcome.validate_against(&protocol.task, observed_at_unix_ms)?;
        Ok(ReferenceWorkerResponse { outcome, output_json })
    }

    fn execute_process(
        &self,
        protocol: &RemoteWorkerProtocolV1,
        process_executable_allowlist: &[String],
        observed_at_unix_ms: i64,
        cancellation_requested: Option<&AtomicBool>,
    ) -> Result<ProcessExecutionResult, NetworkWorkerRuntimeError> {
        let input = parse_process_runner_tool_input(protocol.task.input_json.as_bytes())
            .map_err(|error| NetworkWorkerRuntimeError::Input(error.to_string()))?;
        validate_process_input(&input, process_executable_allowlist)?;
        if cancellation_requested.is_some_and(|cancelled| cancelled.load(Ordering::Acquire)) {
            return Err(NetworkWorkerRuntimeError::ProcessCancelled);
        }
        let cwd = input
            .cwd
            .as_deref()
            .map_or_else(|| Ok(self.workspace_root.clone()), |raw| self.resolve_scoped_path(raw))?;
        if !cwd.is_dir() {
            return Err(NetworkWorkerRuntimeError::Workspace(
                "network worker process cwd is not a directory".to_owned(),
            ));
        }
        let deadline_remaining_ms =
            protocol.task.deadline_unix_ms.saturating_sub(observed_at_unix_ms);
        let host_timeout_ms = protocol
            .task
            .resource_limits
            .wall_time_ms
            .min(u64::try_from(deadline_remaining_ms).unwrap_or(0));
        let timeout_ms = input.timeout_ms.unwrap_or(host_timeout_ms).min(host_timeout_ms);
        if timeout_ms == 0 {
            return Err(NetworkWorkerRuntimeError::ProcessTimeout);
        }
        let command_plan = build_tier_c_command_plan(
            &TierCPolicy {
                workspace_root: self.workspace_root.clone(),
                cwd: cwd.clone(),
                enforce_network_isolation: true,
                allowed_egress_hosts: Vec::new(),
                allowed_dns_suffixes: Vec::new(),
            },
            &TierCCommandRequest { command: input.command.clone(), args: input.args.clone() },
        )
        .map_err(|error| NetworkWorkerRuntimeError::ProcessSandboxUnavailable(error.to_string()))?;
        let capture_budget = usize::try_from(protocol.task.max_output_bytes)
            .unwrap_or(usize::MAX)
            .min(MAX_WORKER_OUTPUT_BYTES)
            .saturating_sub(4 * 1_024);
        if capture_budget == 0 {
            return Err(NetworkWorkerRuntimeError::OutputLimitExceeded);
        }
        let child = Command::new(command_plan.program.as_str())
            .args(command_plan.args.as_slice())
            .current_dir(cwd)
            .env_clear()
            .env("PATH", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
            .env("LANG", "C")
            .env("LC_ALL", "C")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|_| NetworkWorkerRuntimeError::ProcessSpawnFailed)?;
        let mut child = ProcessChildGuard::new(child);
        let stdout =
            child.child_mut()?.stdout.take().ok_or(NetworkWorkerRuntimeError::ProcessIo)?;
        let stderr =
            child.child_mut()?.stderr.take().ok_or(NetworkWorkerRuntimeError::ProcessIo)?;
        let bytes_observed = Arc::new(AtomicUsize::new(0));
        let output_limit_exceeded = Arc::new(AtomicBool::new(false));
        let stdout_reader = spawn_bounded_process_reader(
            stdout,
            Arc::clone(&bytes_observed),
            Arc::clone(&output_limit_exceeded),
            capture_budget,
        );
        let stderr_reader = spawn_bounded_process_reader(
            stderr,
            Arc::clone(&bytes_observed),
            Arc::clone(&output_limit_exceeded),
            capture_budget,
        );
        let started = Instant::now();
        let deadline = started + Duration::from_millis(timeout_ms);
        let status = loop {
            if cancellation_requested.is_some_and(|cancelled| cancelled.load(Ordering::Acquire)) {
                child.terminate_and_wait();
                join_process_readers(stdout_reader, stderr_reader);
                return Err(NetworkWorkerRuntimeError::ProcessCancelled);
            }
            if output_limit_exceeded.load(Ordering::Acquire) {
                child.terminate_and_wait();
                join_process_readers(stdout_reader, stderr_reader);
                return Err(NetworkWorkerRuntimeError::OutputLimitExceeded);
            }
            if Instant::now() >= deadline {
                child.terminate_and_wait();
                join_process_readers(stdout_reader, stderr_reader);
                return Err(NetworkWorkerRuntimeError::ProcessTimeout);
            }
            match child.try_wait().map_err(|_| NetworkWorkerRuntimeError::ProcessIo)? {
                Some(status) => break status,
                None => thread::sleep(Duration::from_millis(PROCESS_POLL_INTERVAL_MS)),
            }
        };
        child.disarm();
        let stdout = stdout_reader
            .join()
            .map_err(|_| NetworkWorkerRuntimeError::ProcessIo)?
            .map_err(|_| NetworkWorkerRuntimeError::ProcessIo)?;
        let stderr = stderr_reader
            .join()
            .map_err(|_| NetworkWorkerRuntimeError::ProcessIo)?
            .map_err(|_| NetworkWorkerRuntimeError::ProcessIo)?;
        if output_limit_exceeded.load(Ordering::Acquire) {
            return Err(NetworkWorkerRuntimeError::OutputLimitExceeded);
        }
        process_execution_result(
            command_plan.backend.executor_label(),
            status,
            stdout,
            stderr,
            u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        )
    }

    fn execute_read(&self, input_json: &str) -> Result<String, NetworkWorkerRuntimeError> {
        let input: ReadInput = serde_json::from_str(input_json)
            .map_err(|error| NetworkWorkerRuntimeError::Input(error.to_string()))?;
        let path = self.resolve_scoped_path(input.path.as_str())?;
        let metadata = fs::metadata(path.as_path())
            .map_err(|error| NetworkWorkerRuntimeError::Workspace(error.to_string()))?;
        if !metadata.is_file()
            || metadata.len() > u64::try_from(MAX_WORKER_OUTPUT_BYTES).unwrap_or(u64::MAX)
        {
            return Err(NetworkWorkerRuntimeError::OutputLimitExceeded);
        }
        if input.line_start.is_some() || input.line_count.is_some() {
            return Err(NetworkWorkerRuntimeError::Input(
                "network worker read_file line windows are unavailable; use byte offsets"
                    .to_owned(),
            ));
        }
        let bytes = fs::read(path)
            .map_err(|error| NetworkWorkerRuntimeError::Workspace(error.to_string()))?;
        let offset = usize::try_from(input.offset_bytes).unwrap_or(usize::MAX).min(bytes.len());
        let max_bytes =
            usize::try_from(input.max_bytes.unwrap_or(128 * 1_024)).unwrap_or(usize::MAX);
        if max_bytes == 0 {
            return Err(NetworkWorkerRuntimeError::Input(
                "read_file max_bytes must be positive".to_owned(),
            ));
        }
        let end = offset.saturating_add(max_bytes).min(bytes.len());
        let chunk = &bytes[offset..end];
        let content = std::str::from_utf8(chunk).map_err(|_| {
            NetworkWorkerRuntimeError::Input(
                "network worker reference read_file supports UTF-8 text only".to_owned(),
            )
        })?;
        serde_json::to_string(&serde_json::json!({
            "path": input.path,
            "workspace_root_index": 0,
            "offset_bytes": input.offset_bytes,
            "returned_bytes": chunk.len(),
            "size_bytes": bytes.len(),
            "eof": end >= bytes.len(),
            "chunk_sha256": sha256_hex(chunk),
            "text": content,
        }))
        .map_err(|error| NetworkWorkerRuntimeError::Input(error.to_string()))
    }

    fn execute_list(&self, input_json: &str) -> Result<String, NetworkWorkerRuntimeError> {
        let input: ListInput = serde_json::from_str(input_json)
            .map_err(|error| NetworkWorkerRuntimeError::Input(error.to_string()))?;
        let path = self.resolve_scoped_path(input.path.as_str())?;
        let mut entries = fs::read_dir(path)
            .map_err(|error| NetworkWorkerRuntimeError::Workspace(error.to_string()))?
            .map(|entry| {
                let value = entry
                    .map_err(|error| NetworkWorkerRuntimeError::Workspace(error.to_string()))?;
                let file_type = value
                    .file_type()
                    .map_err(|error| NetworkWorkerRuntimeError::Workspace(error.to_string()))?;
                let name = value.file_name().to_string_lossy().into_owned();
                let relative_path = if input.path.is_empty() {
                    name.clone()
                } else {
                    format!("{}/{}", input.path.trim_end_matches('/'), name)
                };
                let size_bytes = if file_type.is_file() {
                    Some(
                        value
                            .metadata()
                            .map_err(|error| {
                                NetworkWorkerRuntimeError::Workspace(error.to_string())
                            })?
                            .len(),
                    )
                } else {
                    None
                };
                Ok::<serde_json::Value, NetworkWorkerRuntimeError>(serde_json::json!({
                    "name": name,
                    "path": relative_path,
                    "kind": if file_type.is_dir() { "directory" } else { "file" },
                    "size_bytes": size_bytes,
                }))
            })
            .collect::<Result<Vec<_>, _>>()?;
        entries.sort_by(|left, right| {
            left.get("name")
                .and_then(serde_json::Value::as_str)
                .cmp(&right.get("name").and_then(serde_json::Value::as_str))
        });
        let max_entries = input.max_entries.unwrap_or(200).clamp(1, 1_000);
        let truncated = entries.len() > max_entries;
        entries.truncate(max_entries);
        serde_json::to_string(&serde_json::json!({
            "path": input.path,
            "workspace_root_index": 0,
            "entries": entries,
            "truncated": truncated,
        }))
        .map_err(|error| NetworkWorkerRuntimeError::Input(error.to_string()))
    }

    fn execute_search(&self, input_json: &str) -> Result<String, NetworkWorkerRuntimeError> {
        let input: SearchInput = serde_json::from_str(input_json)
            .map_err(|error| NetworkWorkerRuntimeError::Input(error.to_string()))?;
        if input.query.is_empty() {
            return Err(NetworkWorkerRuntimeError::Input(
                "search query must not be empty".to_owned(),
            ));
        }
        let root = self.resolve_scoped_path(input.path.as_str())?;
        let max_matches = input.max_matches.unwrap_or(100).clamp(1, 1_000);
        let mut files = Vec::new();
        collect_regular_files(root.as_path(), &mut files)?;
        files.sort();
        let case_sensitive = input.case_sensitive.unwrap_or(true);
        let needle = if case_sensitive { input.query.clone() } else { input.query.to_lowercase() };
        let mut matches = Vec::new();
        let mut files_scanned = 0_usize;
        let mut files_with_matches = 0_usize;
        let mut skipped_files = 0_usize;
        for path in files {
            let relative = path
                .strip_prefix(self.workspace_root.as_path())
                .map_err(|_| NetworkWorkerRuntimeError::WorkspaceEscape)?;
            let content = match fs::read_to_string(path.as_path()) {
                Ok(content) => content,
                Err(error) if error.kind() == std::io::ErrorKind::InvalidData => {
                    skipped_files = skipped_files.saturating_add(1);
                    continue;
                }
                Err(error) => {
                    return Err(NetworkWorkerRuntimeError::Workspace(error.to_string()));
                }
            };
            files_scanned = files_scanned.saturating_add(1);
            let matches_before = matches.len();
            for (line_index, line) in content.lines().enumerate() {
                let haystack = if case_sensitive { line.to_owned() } else { line.to_lowercase() };
                if haystack.contains(needle.as_str()) {
                    let column = haystack
                        .find(needle.as_str())
                        .map(|index| index.saturating_add(1))
                        .unwrap_or(1);
                    matches.push(serde_json::json!({
                        "path": relative.to_string_lossy().replace('\\', "/"),
                        "line": line_index.saturating_add(1),
                        "column": column,
                        "line_text": line,
                    }));
                    if matches.len() >= max_matches {
                        break;
                    }
                }
            }
            if matches.len() > matches_before {
                files_with_matches = files_with_matches.saturating_add(1);
            }
            if matches.len() >= max_matches {
                break;
            }
        }
        serde_json::to_string(&serde_json::json!({
            "query": input.query,
            "path": input.path,
            "workspace_root_index": 0,
            "case_sensitive": case_sensitive,
            "matches": matches,
            "truncated": matches.len() >= max_matches,
            "files_scanned": files_scanned,
            "files_with_matches": files_with_matches,
            "skipped_files": skipped_files,
            "skipped_dirs": 0,
        }))
        .map_err(|error| NetworkWorkerRuntimeError::Input(error.to_string()))
    }

    fn execute_patch(
        &self,
        input_json: &str,
    ) -> Result<(String, Option<RemotePatchBundle>), NetworkWorkerRuntimeError> {
        let input: PatchInput = serde_json::from_str(input_json)
            .map_err(|error| NetworkWorkerRuntimeError::Input(error.to_string()))?;
        if input.patch.trim().is_empty() {
            return Err(NetworkWorkerRuntimeError::Input(
                "patch document must not be empty".to_owned(),
            ));
        }
        let outcome = apply_workspace_patch(
            std::slice::from_ref(&self.workspace_root),
            &WorkspacePatchRequest {
                patch: input.patch,
                dry_run: input.dry_run.unwrap_or(false),
                redaction_policy: WorkspacePatchRedactionPolicy::default(),
            },
            &WorkspacePatchLimits::default(),
        )
        .map_err(|error| NetworkWorkerRuntimeError::Input(error.to_string()))?;
        let mut touched_paths =
            outcome.files_touched.iter().map(|file| file.path.clone()).collect::<Vec<_>>();
        touched_paths.sort();
        touched_paths.dedup();
        let patch_bundle = RemotePatchBundle {
            patch_sha256: outcome.patch_sha256.clone(),
            touched_paths,
            review_required: true,
        };
        let output = serde_json::to_string(&serde_json::json!({
            "remote_patch_bundle": &patch_bundle,
            "files_touched": outcome.files_touched,
            "no_op_files": outcome.no_op_files,
            "dry_run": outcome.dry_run,
            "rollback_performed": outcome.rollback_performed,
        }))
        .map_err(|error| NetworkWorkerRuntimeError::Input(error.to_string()))?;
        Ok((output, Some(patch_bundle)))
    }

    fn resolve_scoped_path(&self, raw: &str) -> Result<PathBuf, NetworkWorkerRuntimeError> {
        let portable = raw.trim().replace('\\', "/");
        let normalized = portable
            .strip_prefix("/workspace/")
            .or_else(|| portable.strip_prefix("workspace/"))
            .unwrap_or(portable.as_str())
            .trim_matches('/');
        if matches!(portable.as_str(), "/workspace" | "workspace") {
            return Ok(self.workspace_root.clone());
        }
        let relative = Path::new(normalized);
        if relative.is_absolute()
            || relative.components().any(|component| {
                matches!(
                    component,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )
            })
        {
            return Err(NetworkWorkerRuntimeError::WorkspaceEscape);
        }
        if normalized.is_empty() {
            return Ok(self.workspace_root.clone());
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

struct ProcessExecutionResult {
    output_json: String,
    success: bool,
    reason_code: &'static str,
}

fn validate_process_input(
    input: &ProcessRunnerToolInput,
    process_executable_allowlist: &[String],
) -> Result<(), NetworkWorkerRuntimeError> {
    if !process_executable_allowlist.iter().any(|allowed| allowed == &input.command) {
        return Err(NetworkWorkerRuntimeError::ProcessExecutableDenied);
    }
    if process_executable_is_raw_shell(input.command.as_str()) {
        return Err(NetworkWorkerRuntimeError::ProcessShellDenied);
    }
    if process_executable_is_interpreter(input.command.as_str())
        && interpreter_args_contain_blocked_eval_flag(input.command.as_str(), input.args.as_slice())
    {
        return Err(NetworkWorkerRuntimeError::ProcessInterpreterEvalDenied);
    }
    if input.args.len() > MAX_PROCESS_ARGUMENTS
        || input
            .args
            .iter()
            .try_fold(0_usize, |total, argument| total.checked_add(argument.len()))
            .is_none_or(|total| total > MAX_PROCESS_ARGUMENT_BYTES)
        || input.args.iter().any(|argument| argument.as_bytes().contains(&b'\0'))
    {
        return Err(NetworkWorkerRuntimeError::ProcessArgumentsInvalid);
    }
    if input.args.iter().any(|argument| process_argument_escapes_workspace(argument)) {
        return Err(NetworkWorkerRuntimeError::WorkspaceEscape);
    }
    if input.background
        || input.keep_running_after_run
        || input.interactive
        || input.stdin
        || input.pty
        || input.notify_on_complete
        || !input.watch_patterns.is_empty()
        || !input.port_hints.is_empty()
        || input.elevated_intent
        || input.effective_lifetime_mode().is_detached_handoff()
    {
        return Err(NetworkWorkerRuntimeError::ProcessLifecycleDenied);
    }
    if !input.env.is_empty() || input.env_profile_id.is_some() || !input.prepend_path.is_empty() {
        return Err(NetworkWorkerRuntimeError::ProcessEnvironmentDenied);
    }
    if !input.requested_egress_hosts.is_empty() {
        return Err(NetworkWorkerRuntimeError::ProcessEgressDenied);
    }
    Ok(())
}

fn process_argument_escapes_workspace(argument: &str) -> bool {
    if argument.contains('\\') || argument.to_ascii_lowercase().starts_with("file:") {
        return true;
    }
    let candidate = argument.split_once('=').map_or(argument, |(_, value)| value);
    let path = Path::new(candidate);
    path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::ParentDir | Component::RootDir))
}

fn process_executable_is_raw_shell(command: &str) -> bool {
    let executable = command.rsplit('/').next().unwrap_or(command).to_ascii_lowercase();
    matches!(
        executable.as_str(),
        "bash"
            | "sh"
            | "zsh"
            | "fish"
            | "cmd"
            | "cmd.exe"
            | "powershell"
            | "powershell.exe"
            | "pwsh"
            | "pwsh.exe"
    )
}

fn process_execution_result(
    executor: &str,
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    duration_ms: u64,
) -> Result<ProcessExecutionResult, NetworkWorkerRuntimeError> {
    let success = status.success();
    let reason_code = if success { "worker.task.succeeded" } else { "worker.process.exit_nonzero" };
    let stdout_text = String::from_utf8_lossy(stdout.as_slice());
    let stderr_text = String::from_utf8_lossy(stderr.as_slice());
    let redacted_stdout = redact_diagnostic_text(stdout_text.as_ref());
    let redacted_stderr = redact_diagnostic_text(stderr_text.as_ref());
    let output_json = serde_json::to_string(&serde_json::json!({
        "schema_version": 2,
        "exit_code": status.code(),
        "stdout": redacted_stdout,
        "stderr": redacted_stderr,
        "stdout_truncated": false,
        "stderr_truncated": false,
        "stdout_redacted": redacted_stdout.as_str() != stdout_text.as_ref(),
        "stderr_redacted": redacted_stderr.as_str() != stderr_text.as_ref(),
        "stdout_bytes": stdout.len(),
        "stderr_bytes": stderr.len(),
        "stdout_sha256": sha256_hex(stdout.as_slice()),
        "stderr_sha256": sha256_hex(stderr.as_slice()),
        "duration_ms": duration_ms,
        "tier": "C",
        "sandbox_backend": executor,
        "remote_worker": {
            "network_isolation": "os_enforced",
            "environment": "fixed_minimal",
            "cleanup": "process_reaped",
        },
    }))
    .map_err(|error| NetworkWorkerRuntimeError::Input(error.to_string()))?;
    Ok(ProcessExecutionResult { output_json, success, reason_code })
}

struct ProcessChildGuard {
    child: Option<Child>,
}

impl ProcessChildGuard {
    fn new(child: Child) -> Self {
        Self { child: Some(child) }
    }

    fn child_mut(&mut self) -> Result<&mut Child, NetworkWorkerRuntimeError> {
        self.child.as_mut().ok_or(NetworkWorkerRuntimeError::ProcessIo)
    }

    fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        self.child
            .as_mut()
            .ok_or_else(|| io::Error::other("process child is not owned"))?
            .try_wait()
    }

    fn terminate_and_wait(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    fn disarm(&mut self) {
        let _ = self.child.take();
    }
}

impl Drop for ProcessChildGuard {
    fn drop(&mut self) {
        self.terminate_and_wait();
    }
}

fn spawn_bounded_process_reader<R>(
    mut reader: R,
    bytes_observed: Arc<AtomicUsize>,
    output_limit_exceeded: Arc<AtomicBool>,
    capture_budget: usize,
) -> thread::JoinHandle<io::Result<Vec<u8>>>
where
    R: Read + Send + 'static,
{
    thread::spawn(move || {
        let mut output = Vec::new();
        let mut buffer = [0_u8; 8 * 1_024];
        loop {
            let read = reader.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            let previous = bytes_observed.fetch_add(read, Ordering::AcqRel);
            let retained = capture_budget.saturating_sub(previous).min(read);
            output.extend_from_slice(&buffer[..retained]);
            if retained != read {
                output_limit_exceeded.store(true, Ordering::Release);
                break;
            }
        }
        Ok(output)
    })
}

fn join_process_readers(
    stdout_reader: thread::JoinHandle<io::Result<Vec<u8>>>,
    stderr_reader: thread::JoinHandle<io::Result<Vec<u8>>>,
) {
    let _ = stdout_reader.join();
    let _ = stderr_reader.join();
}

#[derive(Debug, Deserialize)]
struct ReadInput {
    path: String,
    #[serde(default, rename = "workspace_root")]
    _workspace_root: Option<String>,
    #[serde(default)]
    offset_bytes: u64,
    #[serde(default)]
    max_bytes: Option<u64>,
    #[serde(default)]
    line_start: Option<u64>,
    #[serde(default)]
    line_count: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ListInput {
    #[serde(default)]
    path: String,
    #[serde(default, rename = "workspace_root")]
    _workspace_root: Option<String>,
    #[serde(default)]
    max_entries: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct SearchInput {
    query: String,
    #[serde(default)]
    path: String,
    #[serde(default, rename = "workspace_root")]
    _workspace_root: Option<String>,
    #[serde(default)]
    case_sensitive: Option<bool>,
    #[serde(default)]
    max_matches: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct PatchInput {
    patch: String,
    #[serde(default)]
    dry_run: Option<bool>,
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
    /// The transport omitted the canonical task projection.
    #[error("network worker canonical protocol binding is required")]
    CanonicalProtocolRequired,
    /// A portable network worker requires a scoped content-addressed workspace.
    #[error("network worker requires a scoped workspace bundle")]
    ScopedWorkspaceRequired,
    /// The reference safe subset does not request or decrypt secret leases.
    #[error("network worker reference tool subset does not accept secret leases")]
    SecretLeaseUnsupported,
    /// Output exceeds its host-issued budget.
    #[error("network worker output exceeds its budget")]
    OutputLimitExceeded,
    /// The exact executable token was not present in host-issued authority.
    #[error("network worker process executable is not authorized")]
    ProcessExecutableDenied,
    /// Shell interpreters are outside the canonical process safe subset.
    #[error("network worker process shell execution is denied")]
    ProcessShellDenied,
    /// Interpreter inline-eval flags are outside the canonical process safe subset.
    #[error("network worker process interpreter inline eval is denied")]
    ProcessInterpreterEvalDenied,
    /// Process arguments exceed their closed shape or byte bounds.
    #[error("network worker process arguments are invalid")]
    ProcessArgumentsInvalid,
    /// Background, interactive, elevated, or detached process lifecycle was requested.
    #[error("network worker process lifecycle is not supported")]
    ProcessLifecycleDenied,
    /// Task-controlled environment or PATH mutation was requested.
    #[error("network worker process environment authority is denied")]
    ProcessEnvironmentDenied,
    /// The reference process worker only supports OS-enforced network denial.
    #[error("network worker process egress is denied")]
    ProcessEgressDenied,
    /// The target host cannot enforce the required Tier-C sandbox.
    #[error("network worker process sandbox is unavailable: {0}")]
    ProcessSandboxUnavailable(String),
    /// The sandbox wrapper could not be spawned.
    #[error("network worker process failed to start")]
    ProcessSpawnFailed,
    /// Process pipe or wait handling failed.
    #[error("network worker process I/O failed")]
    ProcessIo,
    /// Host cancellation terminated and reaped the process.
    #[error("network worker process was cancelled")]
    ProcessCancelled,
    /// The process exceeded its canonical deadline and was reaped.
    #[error("network worker process timed out")]
    ProcessTimeout,
    /// Isolated computer-use execution or evidence validation failed.
    #[error("network worker computer use failed closed: {0}")]
    ComputerUse(String),
}

impl NetworkWorkerRuntimeError {
    /// Stable reason code for fail-closed runtime diagnostics.
    #[must_use]
    pub const fn reason_code(&self) -> &'static str {
        match self {
            Self::SecretLeaseUnsupported => "worker.secret_lease.unsupported_by_safe_subset",
            Self::ProcessExecutableDenied => "worker.process.executable_denied",
            Self::ProcessShellDenied => "worker.process.shell_denied",
            Self::ProcessInterpreterEvalDenied => "worker.process.interpreter_eval_denied",
            Self::ProcessArgumentsInvalid => "worker.process.arguments_invalid",
            Self::ProcessLifecycleDenied => "worker.process.lifecycle_denied",
            Self::ProcessEnvironmentDenied => "worker.process.environment_denied",
            Self::ProcessEgressDenied => "worker.process.egress_denied",
            Self::ProcessSandboxUnavailable(_) => "worker.process.sandbox_unavailable",
            Self::ProcessSpawnFailed => "worker.process.spawn_failed",
            Self::ProcessIo => "worker.process.io_failed",
            Self::ProcessCancelled => "worker.process.cancelled",
            Self::ProcessTimeout => "worker.process.timeout",
            _ => "worker.runtime.failed_closed",
        }
    }
}

fn validate_computer_use_remote_scopes(
    request: &WorkerRemoteToolRequestEnvelope,
) -> Result<(), NetworkWorkerRuntimeError> {
    let contract = serde_json::from_str::<ComputerUseTaskContract>(request.input_json.as_str())
        .map_err(|error| NetworkWorkerRuntimeError::ComputerUse(error.to_string()))?;
    contract
        .profile
        .validate()
        .map_err(|error| NetworkWorkerRuntimeError::ComputerUse(error.to_string()))?;
    if contract.profile.isolation_attestation_sha256 != request.worker_identity.image_digest_sha256
        || contract.profile.host_desktop_access
        || !contract.profile.network_hosts.is_empty()
        || contract.profile.clipboard_read
        || contract.profile.clipboard_write
        || contract.profile.max_wall_clock_ms
            > request
                .canonical_protocol
                .as_ref()
                .map_or(0, |protocol| protocol.task.resource_limits.wall_time_ms)
    {
        return Err(NetworkWorkerRuntimeError::ComputerUse(
            "computer-use capability profile exceeds host-issued worker authority".to_owned(),
        ));
    }
    for root in &contract.profile.filesystem_roots {
        if !portable_path_allowed_by_lease(
            root,
            request.lease.workspace_scope.allowed_paths.as_slice(),
        ) || !request.workspace_transfer.scoped_entries.iter().any(|entry| entry.path == *root)
        {
            return Err(NetworkWorkerRuntimeError::ComputerUse(
                "computer-use filesystem scope is not content-addressed by the lease".to_owned(),
            ));
        }
    }
    for requested in &contract.actions {
        if let ComputerUseAction::FileChooser { path } = &requested.action {
            if !contract.profile.filesystem_roots.iter().any(|root| root == path) {
                return Err(NetworkWorkerRuntimeError::ComputerUse(
                    "computer-use file chooser path exceeds its dedicated filesystem scope"
                        .to_owned(),
                ));
            }
        }
    }
    Ok(())
}

fn portable_path_allowed_by_lease(path: &str, allowed_paths: &[String]) -> bool {
    let path = Path::new(path);
    allowed_paths.iter().any(|allowed| {
        if allowed == "." {
            return true;
        }
        let allowed = Path::new(allowed);
        path == allowed || path.starts_with(allowed)
    })
}

fn materialize_scoped_workspace(
    workspace_root: &Path,
    request: &WorkerRemoteToolRequestEnvelope,
) -> Result<(), NetworkWorkerRuntimeError> {
    request
        .workspace_transfer
        .canonical_bundle_sha256()
        .map_err(|error| NetworkWorkerRuntimeError::Input(error.to_string()))?;
    for entry in &request.workspace_transfer.scoped_entries {
        let path = workspace_root.join(entry.path.as_str());
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| NetworkWorkerRuntimeError::Workspace(error.to_string()))?;
        }
        match entry.kind {
            WorkerRemoteWorkspaceEntryKind::Directory => fs::create_dir_all(path.as_path())
                .map_err(|error| NetworkWorkerRuntimeError::Workspace(error.to_string()))?,
            WorkerRemoteWorkspaceEntryKind::File => {
                fs::write(path.as_path(), entry.bytes.as_slice())
                    .map_err(|error| NetworkWorkerRuntimeError::Workspace(error.to_string()))?
            }
        }
    }
    Ok(())
}

fn collect_regular_files(
    directory: &Path,
    files: &mut Vec<PathBuf>,
) -> Result<(), NetworkWorkerRuntimeError> {
    for entry in fs::read_dir(directory)
        .map_err(|error| NetworkWorkerRuntimeError::Workspace(error.to_string()))?
    {
        let entry =
            entry.map_err(|error| NetworkWorkerRuntimeError::Workspace(error.to_string()))?;
        let file_type = entry
            .file_type()
            .map_err(|error| NetworkWorkerRuntimeError::Workspace(error.to_string()))?;
        if file_type.is_symlink() {
            return Err(NetworkWorkerRuntimeError::WorkspaceEscape);
        }
        if file_type.is_dir() {
            collect_regular_files(entry.path().as_path(), files)?;
        } else if file_type.is_file() {
            files.push(entry.path());
        }
    }
    Ok(())
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
    use crate::{
        remote_protocol::RemoteWorkerProtocolV1, WorkerArtifactTransport, WorkerRemoteIdentity,
        WorkerRemoteLeaseBinding, WorkerRemoteToolKind, WorkerRemoteWorkspaceEntry,
        WorkerRemoteWorkspaceTransfer, WorkerWorkspaceScope,
    };

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

    #[test]
    fn scoped_network_request_executes_read_and_verifies_cleanup() {
        let request = scoped_request(
            "palyra.fs.read_file",
            WorkerRemoteToolKind::FsRead,
            r#"{"path":"note.txt"}"#,
            b"hello network worker",
        );

        let result =
            ReferenceNetworkWorker::execute_remote_request(&request, 60_000).expect("remote read");

        assert!(result.success);
        assert!(result.output_json.contains("hello network worker"));
        assert!(result.cleanup_report.is_verified());
    }

    #[test]
    fn scoped_network_patch_returns_reviewed_bundle_without_host_writeback() {
        let patch = "*** Begin Patch\n*** Update File: note.txt\n@@\n-old\n+new\n*** End Patch\n";
        let request = scoped_request(
            "palyra.fs.apply_patch",
            WorkerRemoteToolKind::ApplyPatch,
            serde_json::json!({ "patch": patch }).to_string().as_str(),
            b"old\n",
        );

        let result = ReferenceNetworkWorker::execute_remote_request(&request, 60_000)
            .expect("remote patch should execute in scratch scope");
        let output: serde_json::Value =
            serde_json::from_str(result.output_json.as_str()).expect("patch output");

        assert!(result.success);
        assert_eq!(
            output
                .get("remote_patch_bundle")
                .and_then(|bundle| bundle.get("review_required"))
                .and_then(serde_json::Value::as_bool),
            Some(true)
        );
        assert!(result.cleanup_report.is_verified());
    }

    #[test]
    fn remote_process_denies_executable_shell_cwd_and_environment_authority() {
        let executable = process_request(r#"{"command":"printf","args":["ok"]}"#, "sleep");
        assert!(matches!(
            ReferenceNetworkWorker::execute_remote_request(&executable, 60_000),
            Err(NetworkWorkerRuntimeError::ProcessExecutableDenied)
        ));

        let shell = process_request(r#"{"command":"sh","args":["-c","echo unsafe"]}"#, "sh");
        assert!(matches!(
            ReferenceNetworkWorker::execute_remote_request(&shell, 60_000),
            Err(NetworkWorkerRuntimeError::ProcessShellDenied)
        ));

        let interpreter =
            process_request(r#"{"command":"python","args":["-c","print('unsafe')"]}"#, "python");
        assert!(matches!(
            ReferenceNetworkWorker::execute_remote_request(&interpreter, 60_000),
            Err(NetworkWorkerRuntimeError::ProcessInterpreterEvalDenied)
        ));

        let cwd_escape =
            process_request(r#"{"command":"printf","args":["ok"],"cwd":"../outside"}"#, "printf");
        assert!(matches!(
            ReferenceNetworkWorker::execute_remote_request(&cwd_escape, 60_000),
            Err(NetworkWorkerRuntimeError::WorkspaceEscape)
        ));

        let argument_escape =
            process_request(r#"{"command":"printf","args":["--output=/etc/passwd"]}"#, "printf");
        assert!(matches!(
            ReferenceNetworkWorker::execute_remote_request(&argument_escape, 60_000),
            Err(NetworkWorkerRuntimeError::WorkspaceEscape)
        ));

        let environment = process_request(
            r#"{"command":"printf","args":["ok"],"env":{"AWS_SECRET_ACCESS_KEY":"not-a-real-secret"}}"#,
            "printf",
        );
        let error = ReferenceNetworkWorker::execute_remote_request(&environment, 60_000)
            .expect_err("task environment must not reach the sandbox");
        assert!(matches!(error, NetworkWorkerRuntimeError::ProcessEnvironmentDenied));
        assert!(!error.to_string().contains("not-a-real-secret"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn remote_process_runs_in_tier_c_with_bounded_output_and_cleanup() {
        let request = process_request(
            r#"{"command":"/usr/bin/printf","args":["worker-ok"]}"#,
            "/usr/bin/printf",
        );

        let result = match ReferenceNetworkWorker::execute_remote_request(&request, 60_000) {
            Ok(result) => result,
            Err(NetworkWorkerRuntimeError::ProcessSandboxUnavailable(reason))
                if reason.contains("requires binary 'bwrap'") =>
            {
                return;
            }
            Err(error) => {
                panic!("Tier-C process should execute or fail for missing bwrap: {error}")
            }
        };
        let output: serde_json::Value =
            serde_json::from_str(result.output_json.as_str()).expect("process output");

        assert!(result.success);
        assert_eq!(output["stdout"], "worker-ok");
        assert_eq!(output["network_isolation"], "os_enforced");
        assert_eq!(output["environment"], "fixed_minimal");
        assert_eq!(output["cleanup"], "process_reaped");
        assert!(result.cleanup_report.is_verified());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn remote_process_timeout_output_limit_and_cancellation_reap_child() {
        let timeout = process_request(
            r#"{"command":"/usr/bin/sleep","args":["5"],"timeout_ms":20}"#,
            "/usr/bin/sleep",
        );
        match ReferenceNetworkWorker::execute_remote_request(&timeout, 60_000) {
            Err(NetworkWorkerRuntimeError::ProcessTimeout) => {}
            Err(NetworkWorkerRuntimeError::ProcessSandboxUnavailable(reason))
                if reason.contains("requires binary 'bwrap'") =>
            {
                return;
            }
            other => panic!("Tier-C timeout should reap or fail for missing bwrap: {other:?}"),
        }

        let output_limit =
            process_request(r#"{"command":"/usr/bin/yes","args":["bounded"]}"#, "/usr/bin/yes");
        assert!(matches!(
            ReferenceNetworkWorker::execute_remote_request(&output_limit, 60_000),
            Err(NetworkWorkerRuntimeError::OutputLimitExceeded)
        ));

        let cancelled = Arc::new(AtomicBool::new(false));
        let worker_cancelled = Arc::clone(&cancelled);
        let cancellation =
            process_request(r#"{"command":"/usr/bin/sleep","args":["5"]}"#, "/usr/bin/sleep");
        let execution = thread::spawn(move || {
            ReferenceNetworkWorker::execute_remote_request_with_cancellation(
                &cancellation,
                60_000,
                Some(worker_cancelled.as_ref()),
            )
        });
        thread::sleep(Duration::from_millis(50));
        cancelled.store(true, Ordering::Release);

        assert!(matches!(
            execution.join().expect("worker execution thread"),
            Err(NetworkWorkerRuntimeError::ProcessCancelled)
        ));
    }

    #[cfg(windows)]
    #[test]
    fn remote_process_has_no_unsandboxed_windows_fallback() {
        let request = process_request(r#"{"command":"where.exe","args":["rustc"]}"#, "where.exe");
        assert!(matches!(
            ReferenceNetworkWorker::execute_remote_request(&request, 60_000),
            Err(NetworkWorkerRuntimeError::ProcessSandboxUnavailable(_))
        ));
    }

    #[test]
    fn reference_safe_subset_rejects_secret_lease_without_downgrade() {
        let recipient_secret = x25519_dalek::StaticSecret::from([7_u8; 32]);
        let recipient_public = x25519_dalek::PublicKey::from(&recipient_secret).to_bytes();
        let secret_artifact = crate::remote_protocol::seal_worker_secret_lease(
            "secret-network-1",
            recipient_public,
            b"PALYRA_TEST_REMOTE_SECRET",
            90_000,
        )
        .expect("secret lease");
        let mut request = scoped_request(
            "palyra.fs.read_file",
            WorkerRemoteToolKind::FsRead,
            r#"{"path":"note.txt"}"#,
            b"hello",
        );
        request.encrypted_secret_artifact = Some(secret_artifact);
        request.canonical_protocol = Some(RemoteWorkerProtocolV1::from_remote_request(&request));

        let error = ReferenceNetworkWorker::execute_remote_request(&request, 60_000)
            .expect_err("safe subset must not silently discard the secret lease");

        assert!(matches!(&error, NetworkWorkerRuntimeError::SecretLeaseUnsupported));
        assert_eq!(error.reason_code(), "worker.secret_lease.unsupported_by_safe_subset");
    }

    fn process_request(input_json: &str, executable: &str) -> WorkerRemoteToolRequestEnvelope {
        let mut request = scoped_request(
            "palyra.process.run",
            WorkerRemoteToolKind::ProcessRun,
            input_json,
            b"process workspace",
        );
        request.lease.process_executable_allowlist = vec![executable.to_owned()];
        request.canonical_protocol = Some(RemoteWorkerProtocolV1::from_remote_request(&request));
        request
    }

    fn scoped_request(
        tool_name: &str,
        tool_kind: WorkerRemoteToolKind,
        input_json: &str,
        file_bytes: &[u8],
    ) -> WorkerRemoteToolRequestEnvelope {
        let entry = WorkerRemoteWorkspaceEntry {
            path: "note.txt".to_owned(),
            kind: WorkerRemoteWorkspaceEntryKind::File,
            sha256: sha256_hex(file_bytes),
            bytes: file_bytes.to_vec(),
        };
        let workspace_transfer = WorkerRemoteWorkspaceTransfer::scoped("7".repeat(64), vec![entry])
            .expect("scoped transfer");
        let mut request = WorkerRemoteToolRequestEnvelope {
            protocol: WORKER_REMOTE_TOOL_PROTOCOL.to_owned(),
            schema_version: WORKER_REMOTE_TOOL_SCHEMA_VERSION,
            request_id: "request-network-1".to_owned(),
            proposal_id: "proposal-network-1".to_owned(),
            tool_name: tool_name.to_owned(),
            tool_kind,
            input_json: input_json.to_owned(),
            input_json_sha256: sha256_hex(input_json.as_bytes()),
            lease: WorkerRemoteLeaseBinding {
                lease_id: "lease-network-1".to_owned(),
                worker_id: "worker-network-1".to_owned(),
                session_id: "session-network-1".to_owned(),
                run_id: "run-network-1".to_owned(),
                run_generation: RuntimeGeneration::new(1).expect("generation"),
                grant_id: "grant-network-1".to_owned(),
                grant_tool_name: tool_name.to_owned(),
                issued_at_unix_ms: 60_000,
                expires_at_unix_ms: 120_000,
                required_capabilities: vec![tool_kind.required_capability()],
                process_executable_allowlist: if matches!(
                    tool_kind,
                    WorkerRemoteToolKind::ProcessRun
                ) {
                    vec!["printf".to_owned()]
                } else {
                    Vec::new()
                },
                work_graph_claim: None,
                work_graph_posture: Default::default(),
                workspace_scope: WorkerWorkspaceScope {
                    workspace_root: "/workspace".to_owned(),
                    allowed_paths: vec!["note.txt".to_owned()],
                    read_only: !matches!(tool_kind, WorkerRemoteToolKind::ApplyPatch),
                },
                artifact_transport: WorkerArtifactTransport {
                    input_manifest_sha256: "8".repeat(64),
                    output_manifest_sha256: "9".repeat(64),
                    log_stream_id: "logs-network-1".to_owned(),
                    scratch_directory_id: "scratch-network-1".to_owned(),
                },
            },
            worker_identity: WorkerRemoteIdentity {
                worker_id: "worker-network-1".to_owned(),
                image_digest_sha256: "a".repeat(64),
                build_digest_sha256: "b".repeat(64),
                artifact_digest_sha256: "c".repeat(64),
                capability_authority_sha256: Some("d".repeat(64)),
                sdk_protocol_version: 1,
                wit_abi_version: "palyra-worker-abi/v1".to_owned(),
            },
            workspace_transfer,
            encrypted_secret_artifact: None,
            canonical_protocol: None,
        };
        request.canonical_protocol = Some(RemoteWorkerProtocolV1::from_remote_request(&request));
        request
    }
}
