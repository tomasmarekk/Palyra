//! Execution backend inventory, selection, and preflight for tool jobs.
//!
//! Models where tool work may run -- local sandbox, paired desktop node,
//! attested networked worker, or operator SSH tunnel -- and resolves a
//! requested [`ExecutionBackendPreference`] against the live inventory.
//! Preview backends stay disabled until their `PALYRA_EXPERIMENTAL_*` rollout
//! flags opt in; the local sandbox is the conservative default. Also hosts
//! fail-closed container/SSH profile validation and recovery planning for
//! stuck tool jobs ([`plan_stuck_tool_job_recovery`]).

#![allow(dead_code)]

use std::{
    collections::BTreeSet,
    env,
    future::Future,
    path::Path,
    pin::Pin,
    sync::{atomic::AtomicBool, Arc},
    time::Instant,
};

use palyra_common::runtime_preview::RuntimePreviewMode;
use palyra_common::{
    feature_rollouts::{
        FeatureRolloutSetting, FeatureRolloutSource, EXECUTION_BACKEND_DOCKER_ROLLOUT_ENV,
        EXECUTION_BACKEND_NETWORKED_WORKER_ROLLOUT_ENV, EXECUTION_BACKEND_REMOTE_NODE_ROLLOUT_ENV,
        EXECUTION_BACKEND_SSH_TUNNEL_ROLLOUT_ENV,
    },
    process_risk::{classify_process_run, ProcessRiskContext},
    process_runner_input::{parse_process_runner_tool_input, ProcessRunnerToolInput},
    redaction::{is_sensitive_key, redact_diagnostic_text},
};
use palyra_sandbox::{current_backend_capabilities, current_backend_kind};
use palyra_workerd::{WorkerFleetPolicy, WorkerFleetSnapshot};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::{
    config::{
        ExecutionBackendContainerEnvBindingConfig, ExecutionBackendContainerProfileConfig,
        ExecutionBackendProfileConfig, ExecutionBackendProfilesConfig, FeatureRolloutsConfig,
        NetworkedWorkersConfig,
    },
    journal::{ToolJobRecord, ToolJobState},
    node_runtime::RegisteredNodeRecord,
    sandbox_runner::{
        process_runner_executor_name, ProcessProgressSink, SandboxProcessRunnerPolicy,
    },
    tool_protocol::{
        build_tool_execution_outcome, execute_tool_call_with_cancellation_and_progress,
        ToolCallConfig, ToolExecutionOutcome,
    },
};

/// A desktop node counts as healthy only when seen within this window.
const NODE_HEALTHY_AFTER_MS: i64 = 5 * 60 * 1_000;

/// Operator/runtime preference for where tool work should execute.
///
/// `Automatic` keeps the conservative default (local sandbox) until a
/// preview backend is explicitly selected. Serialized snake_case values are
/// persisted in tool job records; do not rename variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExecutionBackendPreference {
    #[default]
    Automatic,
    LocalSandbox,
    DesktopNode,
    Docker,
    NetworkedWorker,
    SshTunnel,
}

impl ExecutionBackendPreference {
    /// Stable snake_case identifier; doubles as the inventory `backend_id`.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Automatic => "automatic",
            Self::LocalSandbox => "local_sandbox",
            Self::DesktopNode => "desktop_node",
            Self::Docker => "docker",
            Self::NetworkedWorker => "networked_worker",
            Self::SshTunnel => "ssh_tunnel",
        }
    }

    /// Human-readable label for operator surfaces.
    #[must_use]
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Automatic => "Automatic",
            Self::LocalSandbox => "Local sandbox",
            Self::DesktopNode => "Desktop node",
            Self::Docker => "Docker",
            Self::NetworkedWorker => "Networked worker",
            Self::SshTunnel => "SSH tunnel",
        }
    }

    /// One-sentence operator description of the backend's posture.
    #[must_use]
    pub(crate) const fn description(self) -> &'static str {
        match self {
            Self::Automatic => {
                "Keep the current default behavior and stay on the daemon host unless a preview backend is explicitly selected."
            }
            Self::LocalSandbox => {
                "Run work on the daemon host and keep sandbox/process-runner guardrails local."
            }
            Self::DesktopNode => {
                "Hand work off to a paired first-party desktop node when a healthy node is available."
            }
            Self::Docker => {
                "Run work inside an explicit container profile with workspace changes returning as a reviewed patch bundle."
            }
            Self::NetworkedWorker => {
                "Run work on an attested ephemeral worker with proxy-mediated egress and scoped artifact transport."
            }
            Self::SshTunnel => {
                "Use an operator-established SSH tunnel for remote control-plane access and remote operator workflows."
            }
        }
    }
}

/// Inventory health of a backend; only `Available` backends are selectable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExecutionBackendState {
    Available,
    Degraded,
    Disabled,
}

impl ExecutionBackendState {
    /// Stable snake_case identifier matching the serde representation.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::Degraded => "degraded",
            Self::Disabled => "disabled",
        }
    }
}

/// One advertised execution backend with its live state, rollout posture,
/// declared capabilities, and workspace/cleanup contract. This is what the
/// console renders and what the resolvers select from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ExecutionBackendInventoryRecord {
    pub(crate) backend_id: String,
    pub(crate) label: String,
    pub(crate) state: ExecutionBackendState,
    pub(crate) selectable: bool,
    pub(crate) selected_by_default: bool,
    pub(crate) description: String,
    pub(crate) operator_summary: String,
    pub(crate) executor_label: Option<String>,
    pub(crate) rollout_flag: Option<String>,
    pub(crate) rollout_source: Option<FeatureRolloutSource>,
    pub(crate) rollout_enabled: bool,
    pub(crate) capabilities: Vec<String>,
    pub(crate) tradeoffs: Vec<String>,
    pub(crate) requires_attestation: bool,
    pub(crate) requires_egress_proxy: bool,
    pub(crate) attestation_mode: BackendAttestationMode,
    pub(crate) workspace_strategy: WorkspaceStrategyDescriptor,
    pub(crate) workspace_scope_mode: String,
    pub(crate) artifact_transport: String,
    pub(crate) cleanup_strategy: String,
    pub(crate) supports_cancellation: bool,
    pub(crate) supports_cleanup: bool,
    pub(crate) health_probe: String,
    pub(crate) active_node_count: usize,
    pub(crate) total_node_count: usize,
}

/// Result of resolving a backend preference: what was requested, what was
/// chosen, whether a fallback happened, and whether approval is required.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ExecutionBackendResolution {
    pub(crate) requested: ExecutionBackendPreference,
    pub(crate) resolved: ExecutionBackendPreference,
    pub(crate) fallback_used: bool,
    pub(crate) reason_code: String,
    pub(crate) approval_required: bool,
    pub(crate) reason: String,
}

/// How a backend materializes the workspace a tool job runs against.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WorkspaceStrategyKind {
    DaemonWorkspaceRoot,
    GitWorktree,
    EphemeralCopy,
    ContainerVolume,
    RemoteLeaseWorkspace,
    OperatorManagedRemote,
}

impl WorkspaceStrategyKind {
    /// Stable snake_case identifier matching the serde representation.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::DaemonWorkspaceRoot => "daemon_workspace_root",
            Self::GitWorktree => "git_worktree",
            Self::EphemeralCopy => "ephemeral_copy",
            Self::ContainerVolume => "container_volume",
            Self::RemoteLeaseWorkspace => "remote_lease_workspace",
            Self::OperatorManagedRemote => "operator_managed_remote",
        }
    }
}

/// How results flow back into the primary workspace, if at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WorkspaceWritebackMode {
    None,
    PatchBundle,
    GitCommit,
    LeaseCommit,
}

impl WorkspaceWritebackMode {
    /// Stable snake_case identifier matching the serde representation.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::PatchBundle => "patch_bundle",
            Self::GitCommit => "git_commit",
            Self::LeaseCommit => "lease_commit",
        }
    }
}

/// Full workspace contract of a backend: lifecycle, isolation, cleanup,
/// writeback mode, and preconditions. Hashable into an attestation digest via
/// [`WorkspaceStrategyDescriptor::attestation_digest_sha256`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WorkspaceStrategyDescriptor {
    pub(crate) kind: WorkspaceStrategyKind,
    pub(crate) lifecycle: String,
    pub(crate) isolation: String,
    pub(crate) cleanup: String,
    pub(crate) writeback: WorkspaceWritebackMode,
    pub(crate) requires_clean_git_state: bool,
    pub(crate) requires_lease: bool,
    pub(crate) digest_required: bool,
}

impl WorkspaceStrategyDescriptor {
    /// Contract for running directly in the validated daemon workspace root.
    #[must_use]
    pub(crate) fn daemon_workspace_root() -> Self {
        Self {
            kind: WorkspaceStrategyKind::DaemonWorkspaceRoot,
            lifecycle: "validated daemon workspace root for the current run".to_owned(),
            isolation: "workspace scope checks plus sandbox process policy".to_owned(),
            cleanup: "process exit and scoped artifact cleanup".to_owned(),
            writeback: WorkspaceWritebackMode::PatchBundle,
            requires_clean_git_state: false,
            requires_lease: false,
            digest_required: true,
        }
    }

    /// Contract for a scoped git worktree created from a clean base ref.
    #[must_use]
    pub(crate) fn git_worktree() -> Self {
        Self {
            kind: WorkspaceStrategyKind::GitWorktree,
            lifecycle: "create scoped git worktree from a clean base ref".to_owned(),
            isolation: "dedicated worktree path with dirty-state guard".to_owned(),
            cleanup: "remove worktree after attested writeback or cancellation".to_owned(),
            writeback: WorkspaceWritebackMode::GitCommit,
            requires_clean_git_state: true,
            requires_lease: false,
            digest_required: true,
        }
    }

    /// Contract for a throwaway per-run copy of the scoped workspace.
    #[must_use]
    pub(crate) fn ephemeral_copy() -> Self {
        Self {
            kind: WorkspaceStrategyKind::EphemeralCopy,
            lifecycle: "copy scoped workspace into a per-run temporary root".to_owned(),
            isolation: "copy-on-run workspace with no ambient host writeback".to_owned(),
            cleanup: "delete temporary workspace on completion or cancellation".to_owned(),
            writeback: WorkspaceWritebackMode::PatchBundle,
            requires_clean_git_state: false,
            requires_lease: false,
            digest_required: true,
        }
    }

    /// Contract for a workspace volume mounted into a container profile.
    #[must_use]
    pub(crate) fn container_volume() -> Self {
        Self {
            kind: WorkspaceStrategyKind::ContainerVolume,
            lifecycle: "mount a scoped workspace volume into a declared container profile"
                .to_owned(),
            isolation: "container namespace plus explicit mount policy".to_owned(),
            cleanup: "remove container volume and upload attested artifacts".to_owned(),
            writeback: WorkspaceWritebackMode::PatchBundle,
            requires_clean_git_state: false,
            requires_lease: false,
            digest_required: true,
        }
    }

    /// Contract for a leased remote worker workspace (networked workers).
    #[must_use]
    pub(crate) fn remote_lease_workspace() -> Self {
        Self {
            kind: WorkspaceStrategyKind::RemoteLeaseWorkspace,
            lifecycle: "lease a remote worker workspace for one run-scoped grant".to_owned(),
            isolation: "remote lease boundary with attested allowed paths".to_owned(),
            cleanup: "lease TTL reap plus verified workspace/artifact/log cleanup".to_owned(),
            writeback: WorkspaceWritebackMode::LeaseCommit,
            requires_clean_git_state: false,
            requires_lease: true,
            digest_required: true,
        }
    }

    /// Contract for an operator-managed remote scope (SSH tunnel backend).
    #[must_use]
    pub(crate) fn operator_managed_remote() -> Self {
        Self {
            kind: WorkspaceStrategyKind::OperatorManagedRemote,
            lifecycle: "operator-established remote scope".to_owned(),
            isolation: "manual tunnel boundary with identity-gated control plane".to_owned(),
            cleanup: "operator teardown plus runtime audit event".to_owned(),
            writeback: WorkspaceWritebackMode::None,
            requires_clean_git_state: false,
            requires_lease: false,
            digest_required: true,
        }
    }

    /// SHA-256 over the serialized descriptor, binding the exact workspace
    /// contract into job attestations.
    #[must_use]
    pub(crate) fn attestation_digest_sha256(&self) -> String {
        // Serialization of this plain struct cannot realistically fail; the
        // kind string fallback keeps the digest stable rather than panicking.
        let encoded =
            serde_json::to_vec(self).unwrap_or_else(|_| self.kind.as_str().as_bytes().to_vec());
        let mut hasher = Sha256::new();
        hasher.update(encoded);
        hex::encode(hasher.finalize())
    }
}

/// Trust mechanism a backend uses to attest its execution environment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum BackendAttestationMode {
    None,
    LocalExecutor,
    ContainerProfile,
    VaultIdentity,
    WorkerLease,
}

/// Capability- and workspace-aware backend request used by
/// [`resolve_execution_backend_for_request`] and the preflight report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExecutionBackendResolutionRequest {
    pub(crate) preference: ExecutionBackendPreference,
    pub(crate) required_capabilities: Vec<String>,
    pub(crate) workspace_strategy: Option<WorkspaceStrategyKind>,
}

/// Coarse environment profile derived from a backend's declared capability
/// strings; advisory data for operators, not an enforcement surface.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct ExecutionEnvironmentCapabilities {
    pub(crate) filesystem_read: bool,
    pub(crate) filesystem_write: bool,
    pub(crate) network_egress: bool,
    pub(crate) secrets: bool,
    pub(crate) process_spawn: bool,
    pub(crate) persistent_workspace: bool,
    pub(crate) gpu: bool,
    pub(crate) timeout_ms: Option<u64>,
    pub(crate) cpu_time_limit_ms: Option<u64>,
    pub(crate) memory_limit_bytes: Option<u64>,
}

/// Health verdict of a backend preflight check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExecutionBackendHealthStatus {
    Healthy,
    Degraded,
    Unavailable,
}

/// Outcome of preflighting one backend against a resolution request:
/// status, stable reason code, repair hint, and capability gap.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ExecutionBackendPreflightRecord {
    pub(crate) backend_id: String,
    pub(crate) status: ExecutionBackendHealthStatus,
    pub(crate) reason_code: String,
    pub(crate) repair_hint: Option<String>,
    pub(crate) checked_at_unix_ms: i64,
    pub(crate) declared_capabilities: Vec<String>,
    pub(crate) missing_capabilities: Vec<String>,
    pub(crate) environment: ExecutionEnvironmentCapabilities,
}

/// Recovery action recommended for a tool job whose heartbeat went stale.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum StuckToolJobRecoveryAction {
    Attach,
    MarkFailed,
    Cancel,
    Cleanup,
    RepairRequired,
}

/// Recovery recommendation for one stuck tool job; produced by
/// [`plan_stuck_tool_job_recovery`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StuckToolJobRecoveryPlan {
    pub(crate) job_id: String,
    pub(crate) backend_id: String,
    pub(crate) action: StuckToolJobRecoveryAction,
    pub(crate) reason_code: String,
    pub(crate) repair_hint: Option<String>,
    pub(crate) stale_for_ms: i64,
}

/// Read-only contract every execution backend exposes for selection and
/// preflight. Implemented by [`ExecutionBackendInventoryRecord`] so inventory
/// entries can be preflighted uniformly.
pub(crate) trait ExecutionBackend {
    fn backend_id(&self) -> &str;
    fn capabilities(&self) -> &[String];
    fn workspace_strategy(&self) -> &WorkspaceStrategyDescriptor;
    fn attestation_mode(&self) -> BackendAttestationMode;
    fn artifact_transport(&self) -> &str;
    fn cleanup_strategy(&self) -> &str;
    fn supports_cancellation(&self) -> bool;
    fn supports_cleanup(&self) -> bool;
    fn health_probe(&self) -> &str;
    fn preflight(
        &self,
        request: &ExecutionBackendResolutionRequest,
        now_unix_ms: i64,
    ) -> ExecutionBackendPreflightRecord {
        build_execution_backend_preflight(self, request, now_unix_ms)
    }
}

impl ExecutionBackend for ExecutionBackendInventoryRecord {
    fn backend_id(&self) -> &str {
        self.backend_id.as_str()
    }

    fn capabilities(&self) -> &[String] {
        self.capabilities.as_slice()
    }

    fn workspace_strategy(&self) -> &WorkspaceStrategyDescriptor {
        &self.workspace_strategy
    }

    fn attestation_mode(&self) -> BackendAttestationMode {
        self.attestation_mode
    }

    fn artifact_transport(&self) -> &str {
        self.artifact_transport.as_str()
    }

    fn cleanup_strategy(&self) -> &str {
        self.cleanup_strategy.as_str()
    }

    fn supports_cancellation(&self) -> bool {
        self.supports_cancellation
    }

    fn supports_cleanup(&self) -> bool {
        self.supports_cleanup
    }

    fn health_probe(&self) -> &str {
        self.health_probe.as_str()
    }
}

/// Discrete runtime operations a backend runner may implement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExecutionBackendRunnerCapability {
    RunProcess,
    RunToolProgram,
    ReadArtifact,
    WriteArtifact,
    OpenWorkspace,
    CommitOrPatchBundle,
    Cancel,
    Cleanup,
    HealthProbe,
    AttestationManifest,
}

impl ExecutionBackendRunnerCapability {
    /// Stable snake_case identifier used in selection diagnostics.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::RunProcess => "run_process",
            Self::RunToolProgram => "run_tool_program",
            Self::ReadArtifact => "read_artifact",
            Self::WriteArtifact => "write_artifact",
            Self::OpenWorkspace => "open_workspace",
            Self::CommitOrPatchBundle => "commit_or_patch_bundle",
            Self::Cancel => "cancel",
            Self::Cleanup => "cleanup",
            Self::HealthProbe => "health_probe",
            Self::AttestationManifest => "attestation_manifest",
        }
    }
}

/// Registry selection record suitable for journal and trajectory metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ExecutionBackendRunnerSelection {
    pub(crate) event: String,
    pub(crate) requested_backend: String,
    pub(crate) resolved_backend: String,
    pub(crate) runner_id: String,
    pub(crate) runner_version: String,
    pub(crate) reason_code: String,
    pub(crate) capabilities: Vec<String>,
}

/// Minimal runner manifest used before the full backend attestation milestone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ExecutionBackendRunnerManifest {
    pub(crate) backend_id: String,
    pub(crate) runner_id: String,
    pub(crate) runner_version: String,
    pub(crate) workspace_strategy_digest: String,
    pub(crate) capabilities: Vec<String>,
}

/// Health probe result for a concrete execution runner implementation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ExecutionBackendRunnerHealth {
    pub(crate) backend_id: String,
    pub(crate) status: ExecutionBackendHealthStatus,
    pub(crate) reason_code: String,
    pub(crate) summary: String,
}

/// Process-run dispatch request passed from gateway runtime into a runner.
pub(crate) struct ExecutionBackendProcessRunRequest<'a> {
    pub(crate) config: &'a ToolCallConfig,
    pub(crate) proposal_id: &'a str,
    pub(crate) tool_name: &'a str,
    pub(crate) input_json: &'a [u8],
    pub(crate) cancellation_requested: Option<Arc<AtomicBool>>,
    pub(crate) process_progress_sink: Option<ProcessProgressSink>,
}

/// Tool-program dispatch placeholder for runner implementations.
pub(crate) struct ExecutionBackendToolProgramRequest<'a> {
    pub(crate) proposal_id: &'a str,
    pub(crate) tool_name: &'a str,
    pub(crate) input_json: &'a [u8],
}

/// Artifact dispatch placeholder for runner implementations.
pub(crate) struct ExecutionBackendArtifactRequest<'a> {
    pub(crate) proposal_id: &'a str,
    pub(crate) tool_name: &'a str,
    pub(crate) input_json: &'a [u8],
}

/// Workspace dispatch placeholder for runner implementations.
pub(crate) struct ExecutionBackendWorkspaceRequest<'a> {
    pub(crate) proposal_id: &'a str,
    pub(crate) tool_name: &'a str,
    pub(crate) input_json: &'a [u8],
}

/// Cancellation/cleanup dispatch placeholder for runner implementations.
pub(crate) struct ExecutionBackendLifecycleRequest<'a> {
    pub(crate) proposal_id: &'a str,
    pub(crate) tool_name: &'a str,
    pub(crate) input_json: &'a [u8],
}

type RunnerExecutionFuture<'a> = Pin<Box<dyn Future<Output = ToolExecutionOutcome> + Send + 'a>>;

/// Executable backend contract used by the gateway runtime dispatcher.
///
/// Inventory still describes what a backend could do; this trait is the
/// execution boundary that actually owns dispatch. Unsupported operations
/// return explicit unavailable outcomes, so a selected backend can never
/// silently fall back to host-local execution.
pub(crate) trait ExecutionBackendRunner: Send + Sync {
    fn backend_preference(&self) -> ExecutionBackendPreference;
    fn runner_id(&self) -> &'static str;
    fn runner_version(&self) -> &'static str;
    fn capabilities(&self) -> &'static [ExecutionBackendRunnerCapability];

    fn run_process<'a>(
        &'a self,
        request: ExecutionBackendProcessRunRequest<'a>,
    ) -> RunnerExecutionFuture<'a> {
        Box::pin(async move {
            unavailable_runner_operation_outcome(
                self,
                ExecutionBackendRunnerCapability::RunProcess,
                request.proposal_id,
                request.tool_name,
                request.input_json,
            )
        })
    }

    fn run_tool_program<'a>(
        &'a self,
        request: ExecutionBackendToolProgramRequest<'a>,
    ) -> RunnerExecutionFuture<'a> {
        Box::pin(async move {
            unavailable_runner_operation_outcome(
                self,
                ExecutionBackendRunnerCapability::RunToolProgram,
                request.proposal_id,
                request.tool_name,
                request.input_json,
            )
        })
    }

    fn read_artifact<'a>(
        &'a self,
        request: ExecutionBackendArtifactRequest<'a>,
    ) -> RunnerExecutionFuture<'a> {
        Box::pin(async move {
            unavailable_runner_operation_outcome(
                self,
                ExecutionBackendRunnerCapability::ReadArtifact,
                request.proposal_id,
                request.tool_name,
                request.input_json,
            )
        })
    }

    fn write_artifact<'a>(
        &'a self,
        request: ExecutionBackendArtifactRequest<'a>,
    ) -> RunnerExecutionFuture<'a> {
        Box::pin(async move {
            unavailable_runner_operation_outcome(
                self,
                ExecutionBackendRunnerCapability::WriteArtifact,
                request.proposal_id,
                request.tool_name,
                request.input_json,
            )
        })
    }

    fn open_workspace<'a>(
        &'a self,
        request: ExecutionBackendWorkspaceRequest<'a>,
    ) -> RunnerExecutionFuture<'a> {
        Box::pin(async move {
            unavailable_runner_operation_outcome(
                self,
                ExecutionBackendRunnerCapability::OpenWorkspace,
                request.proposal_id,
                request.tool_name,
                request.input_json,
            )
        })
    }

    fn commit_or_patch_bundle<'a>(
        &'a self,
        request: ExecutionBackendWorkspaceRequest<'a>,
    ) -> RunnerExecutionFuture<'a> {
        Box::pin(async move {
            unavailable_runner_operation_outcome(
                self,
                ExecutionBackendRunnerCapability::CommitOrPatchBundle,
                request.proposal_id,
                request.tool_name,
                request.input_json,
            )
        })
    }

    fn cancel<'a>(
        &'a self,
        request: ExecutionBackendLifecycleRequest<'a>,
    ) -> RunnerExecutionFuture<'a> {
        Box::pin(async move {
            unavailable_runner_operation_outcome(
                self,
                ExecutionBackendRunnerCapability::Cancel,
                request.proposal_id,
                request.tool_name,
                request.input_json,
            )
        })
    }

    fn cleanup<'a>(
        &'a self,
        request: ExecutionBackendLifecycleRequest<'a>,
    ) -> RunnerExecutionFuture<'a> {
        Box::pin(async move {
            unavailable_runner_operation_outcome(
                self,
                ExecutionBackendRunnerCapability::Cleanup,
                request.proposal_id,
                request.tool_name,
                request.input_json,
            )
        })
    }

    fn health_probe(&self) -> ExecutionBackendRunnerHealth;

    fn attestation_manifest(
        &self,
        workspace_strategy: &WorkspaceStrategyDescriptor,
    ) -> ExecutionBackendRunnerManifest {
        ExecutionBackendRunnerManifest {
            backend_id: self.backend_preference().as_str().to_owned(),
            runner_id: self.runner_id().to_owned(),
            runner_version: self.runner_version().to_owned(),
            workspace_strategy_digest: workspace_strategy.attestation_digest_sha256(),
            capabilities: runner_capability_strings(self.capabilities()),
        }
    }
}

/// Adapter from the new runner contract to the existing local sandbox runtime.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct LocalSandboxRunner;

impl ExecutionBackendRunner for LocalSandboxRunner {
    fn backend_preference(&self) -> ExecutionBackendPreference {
        ExecutionBackendPreference::LocalSandbox
    }

    fn runner_id(&self) -> &'static str {
        "local_sandbox_runner"
    }

    fn runner_version(&self) -> &'static str {
        "v1"
    }

    fn capabilities(&self) -> &'static [ExecutionBackendRunnerCapability] {
        &[
            ExecutionBackendRunnerCapability::RunProcess,
            ExecutionBackendRunnerCapability::RunToolProgram,
            ExecutionBackendRunnerCapability::ReadArtifact,
            ExecutionBackendRunnerCapability::WriteArtifact,
            ExecutionBackendRunnerCapability::OpenWorkspace,
            ExecutionBackendRunnerCapability::CommitOrPatchBundle,
            ExecutionBackendRunnerCapability::Cancel,
            ExecutionBackendRunnerCapability::Cleanup,
            ExecutionBackendRunnerCapability::HealthProbe,
            ExecutionBackendRunnerCapability::AttestationManifest,
        ]
    }

    fn run_process<'a>(
        &'a self,
        request: ExecutionBackendProcessRunRequest<'a>,
    ) -> RunnerExecutionFuture<'a> {
        Box::pin(async move {
            execute_tool_call_with_cancellation_and_progress(
                request.config,
                request.proposal_id,
                request.tool_name,
                request.input_json,
                request.cancellation_requested,
                request.process_progress_sink,
            )
            .await
        })
    }

    fn health_probe(&self) -> ExecutionBackendRunnerHealth {
        ExecutionBackendRunnerHealth {
            backend_id: ExecutionBackendPreference::LocalSandbox.as_str().to_owned(),
            status: ExecutionBackendHealthStatus::Healthy,
            reason_code: "runner.health.local_sandbox.ready".to_owned(),
            summary: "local sandbox runner is available in-process".to_owned(),
        }
    }
}

/// Registry of concrete runners available to the daemon process.
#[derive(Default)]
pub(crate) struct ExecutionBackendRunnerRegistry {
    local_sandbox: LocalSandboxRunner,
    docker: Option<Box<dyn ExecutionBackendRunner>>,
}

impl std::fmt::Debug for ExecutionBackendRunnerRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExecutionBackendRunnerRegistry")
            .field("local_sandbox", &self.local_sandbox.runner_id())
            .field("docker", &self.docker.as_ref().map(|runner| runner.runner_id()))
            .finish()
    }
}

impl ExecutionBackendRunnerRegistry {
    /// Builds a registry with an explicitly configured Docker runner.
    pub(crate) fn with_docker_runner(docker: Box<dyn ExecutionBackendRunner>) -> Self {
        Self { local_sandbox: LocalSandboxRunner, docker: Some(docker) }
    }

    /// Builds a runner registry from validated execution backend profiles.
    ///
    /// # Errors
    /// Returns an error when more than one Docker profile is enabled or when
    /// the selected Docker profile violates container safety invariants.
    pub(crate) fn from_execution_backend_profiles(
        profiles: &ExecutionBackendProfilesConfig,
    ) -> Result<Self, String> {
        if profiles.mode == RuntimePreviewMode::Disabled {
            return Ok(Self::default());
        }
        let docker_profiles = profiles
            .profiles
            .iter()
            .filter(|profile| {
                profile.enabled
                    && profile
                        .kind
                        .eq_ignore_ascii_case(ExecutionBackendPreference::Docker.as_str())
            })
            .collect::<Vec<_>>();
        match docker_profiles.as_slice() {
            [] => Ok(Self::default()),
            [profile] => {
                let container_profile = container_backend_profile_from_config(profile)?;
                let docker =
                    DockerRunner::new(container_profile, DockerCliEngine).map_err(|error| {
                        format!(
                            "failed to build Docker execution backend profile '{}': {error}",
                            profile.id
                        )
                    })?;
                Ok(Self::with_docker_runner(Box::new(docker)))
            }
            _ => {
                Err("execution_backend_profiles must enable at most one Docker profile".to_owned())
            }
        }
    }

    /// Selects a runner for the resolved backend and required operation.
    pub(crate) fn select_runner(
        &self,
        backend: ExecutionBackendPreference,
        required_capability: ExecutionBackendRunnerCapability,
    ) -> Result<&dyn ExecutionBackendRunner, ExecutionBackendRunnerSelectionError> {
        match backend {
            ExecutionBackendPreference::Automatic | ExecutionBackendPreference::LocalSandbox => {
                let runner = &self.local_sandbox;
                if runner.capabilities().contains(&required_capability) {
                    Ok(runner)
                } else {
                    Err(ExecutionBackendRunnerSelectionError::missing_capability(
                        backend,
                        runner.runner_id(),
                        required_capability,
                    ))
                }
            }
            ExecutionBackendPreference::Docker => match self.docker.as_deref() {
                Some(runner) if runner.capabilities().contains(&required_capability) => Ok(runner),
                Some(runner) => Err(ExecutionBackendRunnerSelectionError::missing_capability(
                    backend,
                    runner.runner_id(),
                    required_capability,
                )),
                None => Err(ExecutionBackendRunnerSelectionError::unavailable_backend(
                    backend,
                    required_capability,
                )),
            },
            ExecutionBackendPreference::DesktopNode
            | ExecutionBackendPreference::NetworkedWorker
            | ExecutionBackendPreference::SshTunnel => {
                Err(ExecutionBackendRunnerSelectionError::unavailable_backend(
                    backend,
                    required_capability,
                ))
            }
        }
    }

    /// Returns the runner metadata emitted into backend selection reports.
    pub(crate) fn selection_event(
        &self,
        requested_backend: ExecutionBackendPreference,
        resolved_backend: ExecutionBackendPreference,
    ) -> ExecutionBackendRunnerSelection {
        match self.select_runner(resolved_backend, ExecutionBackendRunnerCapability::OpenWorkspace)
        {
            Ok(runner) => ExecutionBackendRunnerSelection {
                event: "execution_backend.runner_selected".to_owned(),
                requested_backend: requested_backend.as_str().to_owned(),
                resolved_backend: resolved_backend.as_str().to_owned(),
                runner_id: runner.runner_id().to_owned(),
                runner_version: runner.runner_version().to_owned(),
                reason_code: format!("runner.selected.{}", runner.runner_id()),
                capabilities: runner_capability_strings(runner.capabilities()),
            },
            Err(error) => error.selection_event(requested_backend),
        }
    }
}

/// Fail-closed runner selection failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExecutionBackendRunnerSelectionError {
    backend: ExecutionBackendPreference,
    runner_id: Option<String>,
    required_capability: ExecutionBackendRunnerCapability,
    reason_code: String,
    message: String,
}

impl ExecutionBackendRunnerSelectionError {
    fn unavailable_backend(
        backend: ExecutionBackendPreference,
        required_capability: ExecutionBackendRunnerCapability,
    ) -> Self {
        Self {
            backend,
            runner_id: None,
            required_capability,
            reason_code: format!("runner.unavailable.{}", backend.as_str()),
            message: format!(
                "execution backend {} has no registered runner for {}; local fallback is denied",
                backend.as_str(),
                required_capability.as_str()
            ),
        }
    }

    fn missing_capability(
        backend: ExecutionBackendPreference,
        runner_id: &str,
        required_capability: ExecutionBackendRunnerCapability,
    ) -> Self {
        Self {
            backend,
            runner_id: Some(runner_id.to_owned()),
            required_capability,
            reason_code: format!("runner.capability_missing.{}", backend.as_str()),
            message: format!(
                "execution backend {} runner {} does not support {}; local fallback is denied",
                backend.as_str(),
                runner_id,
                required_capability.as_str()
            ),
        }
    }

    pub(crate) fn to_tool_execution_outcome(
        &self,
        proposal_id: &str,
        tool_name: &str,
        input_json: &[u8],
    ) -> ToolExecutionOutcome {
        let output = serde_json::json!({
            "success": false,
            "event": "execution_backend.runner_selection",
            "status": "unavailable",
            "backend": self.backend.as_str(),
            "runner_id": self.runner_id.as_deref(),
            "required_capability": self.required_capability.as_str(),
            "reason_code": self.reason_code.as_str(),
            "repair_hint": "Enable a runner for the selected backend or switch the agent backend preference to automatic/local_sandbox.",
        });
        build_tool_execution_outcome(
            proposal_id,
            tool_name,
            input_json,
            false,
            serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
            self.message.clone(),
            false,
            self.backend.as_str().to_owned(),
            "runner_selection".to_owned(),
        )
    }

    fn selection_event(
        &self,
        requested_backend: ExecutionBackendPreference,
    ) -> ExecutionBackendRunnerSelection {
        ExecutionBackendRunnerSelection {
            event: "execution_backend.runner_unavailable".to_owned(),
            requested_backend: requested_backend.as_str().to_owned(),
            resolved_backend: self.backend.as_str().to_owned(),
            runner_id: self.runner_id.clone().unwrap_or_else(|| "unregistered".to_owned()),
            runner_version: "unavailable".to_owned(),
            reason_code: self.reason_code.clone(),
            capabilities: Vec::new(),
        }
    }
}

fn unavailable_runner_operation_outcome<R: ExecutionBackendRunner + ?Sized>(
    runner: &R,
    required_capability: ExecutionBackendRunnerCapability,
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
) -> ToolExecutionOutcome {
    ExecutionBackendRunnerSelectionError::missing_capability(
        runner.backend_preference(),
        runner.runner_id(),
        required_capability,
    )
    .to_tool_execution_outcome(proposal_id, tool_name, input_json)
}

fn runner_capability_strings(capabilities: &[ExecutionBackendRunnerCapability]) -> Vec<String> {
    capabilities.iter().map(|capability| capability.as_str().to_owned()).collect()
}

/// Default preflight: checks declared capabilities and workspace strategy
/// against the request and classifies the backend's health.
pub(crate) fn build_execution_backend_preflight<B: ExecutionBackend + ?Sized>(
    backend: &B,
    request: &ExecutionBackendResolutionRequest,
    now_unix_ms: i64,
) -> ExecutionBackendPreflightRecord {
    let missing_capabilities = request
        .required_capabilities
        .iter()
        .filter(|required| !backend.capabilities().iter().any(|capability| capability == *required))
        .cloned()
        .collect::<Vec<_>>();
    let workspace_mismatch = request
        .workspace_strategy
        .is_some_and(|required| backend.workspace_strategy().kind != required);
    let status = if !missing_capabilities.is_empty() || workspace_mismatch {
        ExecutionBackendHealthStatus::Unavailable
    } else if backend.health_probe().contains("degraded") {
        // Substring sniffing on the probe label: none of the built-in probe
        // names contain "degraded", so this branch only fires for custom
        // ExecutionBackend impls that encode degradation in their probe.
        // Inventory-level degradation is layered on in
        // build_execution_backend_preflight_report instead.
        ExecutionBackendHealthStatus::Degraded
    } else {
        ExecutionBackendHealthStatus::Healthy
    };
    let reason_code = match status {
        ExecutionBackendHealthStatus::Healthy => "backend.preflight.healthy",
        ExecutionBackendHealthStatus::Degraded => "backend.preflight.degraded",
        ExecutionBackendHealthStatus::Unavailable if !missing_capabilities.is_empty() => {
            "backend.preflight.missing_capabilities"
        }
        ExecutionBackendHealthStatus::Unavailable => "backend.preflight.workspace_mismatch",
    }
    .to_owned();
    let repair_hint = match status {
        ExecutionBackendHealthStatus::Healthy => None,
        ExecutionBackendHealthStatus::Degraded => {
            Some(format!("Inspect backend health probe '{}'.", backend.health_probe()))
        }
        ExecutionBackendHealthStatus::Unavailable if !missing_capabilities.is_empty() => {
            Some(format!("Select a backend that declares {:?}.", missing_capabilities))
        }
        ExecutionBackendHealthStatus::Unavailable => Some(format!(
            "Select a backend with workspace strategy '{}'.",
            request.workspace_strategy.map(WorkspaceStrategyKind::as_str).unwrap_or("unspecified")
        )),
    };
    ExecutionBackendPreflightRecord {
        backend_id: backend.backend_id().to_owned(),
        status,
        reason_code,
        repair_hint,
        checked_at_unix_ms: now_unix_ms,
        declared_capabilities: backend.capabilities().to_vec(),
        missing_capabilities,
        environment: capabilities_to_environment(
            backend.capabilities(),
            backend.workspace_strategy(),
            backend.supports_cancellation(),
            backend.supports_cleanup(),
        ),
    }
}

/// Preflights every inventory backend against one request, overlaying
/// inventory-level disabled/degraded state on the per-backend verdicts.
pub(crate) fn build_execution_backend_preflight_report(
    inventory: &[ExecutionBackendInventoryRecord],
    request: &ExecutionBackendResolutionRequest,
    now_unix_ms: i64,
) -> Vec<ExecutionBackendPreflightRecord> {
    inventory
        .iter()
        .map(|backend| {
            let mut record = backend.preflight(request, now_unix_ms);
            // Inventory state overrides the capability verdict: a disabled
            // backend stays unavailable even when capabilities would match.
            if !backend.selectable || backend.state == ExecutionBackendState::Disabled {
                record.status = ExecutionBackendHealthStatus::Unavailable;
                record.reason_code = "backend.preflight.disabled".to_owned();
                record.repair_hint = Some(backend.operator_summary.clone());
            } else if backend.state == ExecutionBackendState::Degraded
                && record.status == ExecutionBackendHealthStatus::Healthy
            {
                record.status = ExecutionBackendHealthStatus::Degraded;
                record.reason_code = "backend.preflight.inventory_degraded".to_owned();
                record.repair_hint = Some(backend.operator_summary.clone());
            }
            apply_docker_cli_preflight_probe(&mut record, backend, docker_cli_available());
            record
        })
        .collect()
}

fn apply_docker_cli_preflight_probe(
    record: &mut ExecutionBackendPreflightRecord,
    backend: &ExecutionBackendInventoryRecord,
    docker_available: bool,
) {
    if backend.backend_id != ExecutionBackendPreference::Docker.as_str()
        || record.status != ExecutionBackendHealthStatus::Healthy
        || docker_available
    {
        return;
    }
    record.status = ExecutionBackendHealthStatus::Unavailable;
    record.reason_code = "backend.preflight.docker_unavailable".to_owned();
    record.repair_hint = Some(
        "Install Docker CLI or select local_sandbox; Docker target will not fall back to host execution."
            .to_owned(),
    );
}

fn docker_cli_available() -> bool {
    let Some(paths) = env::var_os("PATH") else {
        return false;
    };
    env::split_paths(&paths)
        .any(|directory| docker_cli_candidates().iter().any(|name| directory.join(name).is_file()))
}

fn docker_cli_candidates() -> &'static [&'static str] {
    if cfg!(windows) {
        &["docker.exe", "docker.cmd", "docker.bat", "docker"]
    } else {
        &["docker"]
    }
}

/// Plans recovery for an in-flight tool job whose heartbeat is stale, or
/// returns `None` for jobs that are terminal or still fresh.
///
/// Orphaned jobs always get a plan regardless of staleness. The action
/// depends on what the owning backend still supports: cancel for cancelling
/// jobs, cleanup for orphans, attach when the backend is healthy, otherwise
/// mark-failed or repair-required.
pub(crate) fn plan_stuck_tool_job_recovery(
    job: &ToolJobRecord,
    inventory: &[ExecutionBackendInventoryRecord],
    now_unix_ms: i64,
    heartbeat_timeout_ms: i64,
) -> Option<StuckToolJobRecoveryPlan> {
    if !matches!(
        job.state,
        ToolJobState::Starting
            | ToolJobState::Running
            | ToolJobState::Draining
            | ToolJobState::Cancelling
            | ToolJobState::Orphaned
    ) {
        return None;
    }
    // An expired lease caps last_seen: a job whose lease lapsed is treated as
    // stale from the lease deadline even if a later heartbeat arrived.
    let last_seen = job
        .heartbeat_at_unix_ms
        .unwrap_or(job.updated_at_unix_ms)
        .min(job.lease_expires_at_unix_ms.unwrap_or(i64::MAX));
    let stale_for_ms = now_unix_ms.saturating_sub(last_seen);
    if job.state != ToolJobState::Orphaned && stale_for_ms < heartbeat_timeout_ms.max(1) {
        return None;
    }
    let backend = inventory.iter().find(|record| record.backend_id == job.backend);
    let (action, reason_code, repair_hint) = match backend {
        Some(record) if !record.selectable || record.state == ExecutionBackendState::Disabled => (
            StuckToolJobRecoveryAction::RepairRequired,
            "tool_job.recovery.backend_unavailable",
            Some(record.operator_summary.clone()),
        ),
        Some(record) if record.supports_cancellation && job.state == ToolJobState::Cancelling => {
            (StuckToolJobRecoveryAction::Cancel, "tool_job.recovery.cancel_via_backend", None)
        }
        Some(record) if record.supports_cleanup && job.state == ToolJobState::Orphaned => {
            (StuckToolJobRecoveryAction::Cleanup, "tool_job.recovery.cleanup_orphan", None)
        }
        Some(record) if record.state == ExecutionBackendState::Available => {
            (StuckToolJobRecoveryAction::Attach, "tool_job.recovery.attach", None)
        }
        Some(record) => (
            StuckToolJobRecoveryAction::MarkFailed,
            "tool_job.recovery.mark_failed",
            Some(record.operator_summary.clone()),
        ),
        None => (
            StuckToolJobRecoveryAction::RepairRequired,
            "tool_job.recovery.unknown_backend",
            Some("Backend no longer exists in the runtime inventory.".to_owned()),
        ),
    };
    Some(StuckToolJobRecoveryPlan {
        job_id: job.job_id.clone(),
        backend_id: job.backend.clone(),
        action,
        reason_code: reason_code.to_owned(),
        repair_hint,
        stale_for_ms,
    })
}

// Maps free-form capability strings onto the coarse environment profile.
// The timeout/cpu/memory numbers are advisory defaults for display, not
// enforced limits; enforcement lives in the per-backend runner policies.
fn capabilities_to_environment(
    capabilities: &[String],
    workspace_strategy: &WorkspaceStrategyDescriptor,
    supports_cancellation: bool,
    supports_cleanup: bool,
) -> ExecutionEnvironmentCapabilities {
    let has = |needle: &str| capabilities.iter().any(|capability| capability == needle);
    ExecutionEnvironmentCapabilities {
        filesystem_read: true,
        filesystem_write: matches!(
            workspace_strategy.writeback,
            WorkspaceWritebackMode::PatchBundle
                | WorkspaceWritebackMode::GitCommit
                | WorkspaceWritebackMode::LeaseCommit
        ),
        network_egress: has("egress_proxy")
            || has("proxy_mediated_egress")
            || has("networked_worker_pool"),
        secrets: has("vault_scoped_secret_delivery"),
        process_spawn: has("sandbox_process_runner") || has("daemon_host_execution"),
        persistent_workspace: matches!(
            workspace_strategy.kind,
            WorkspaceStrategyKind::DaemonWorkspaceRoot
                | WorkspaceStrategyKind::GitWorktree
                | WorkspaceStrategyKind::RemoteLeaseWorkspace
                | WorkspaceStrategyKind::OperatorManagedRemote
        ),
        gpu: has("gpu"),
        timeout_ms: supports_cancellation.then_some(30_000),
        cpu_time_limit_ms: has("sandbox_process_runner").then_some(30_000),
        memory_limit_bytes: supports_cleanup.then_some(512 * 1_024 * 1_024),
    }
}

/// Supported container engines for the container-volume backend profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ContainerRuntimeKind {
    Docker,
    Podman,
}

/// Container network posture: fully isolated or proxy-mediated egress only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ContainerNetworkPolicy {
    None,
    EgressProxy,
}

/// One host-to-container mount; must stay workspace-scoped (validated).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ContainerMountPolicy {
    pub(crate) host_path: String,
    pub(crate) container_path: String,
    pub(crate) read_only: bool,
    pub(crate) workspace_scoped: bool,
}

/// Hard resource ceilings for a container run; all must be positive.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ContainerResourceLimits {
    pub(crate) cpu_time_limit_ms: u64,
    pub(crate) memory_limit_bytes: u64,
    pub(crate) max_output_bytes: u64,
}

/// Where a container env value comes from: an inline non-secret literal or a
/// vault reference resolved at launch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ContainerEnvSourceKind {
    LiteralSafeValue,
    VaultRef,
}

/// One environment variable binding inside a container profile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ContainerEnvBinding {
    pub(crate) name: String,
    pub(crate) source_kind: ContainerEnvSourceKind,
    pub(crate) value: String,
}

/// Declarative container execution profile; `validate` enforces the
/// fail-closed security invariants before any container is launched.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ContainerBackendProfile {
    pub(crate) profile_id: String,
    pub(crate) runtime: ContainerRuntimeKind,
    pub(crate) image: String,
    pub(crate) mounts: Vec<ContainerMountPolicy>,
    pub(crate) network: ContainerNetworkPolicy,
    pub(crate) user: String,
    pub(crate) readonly_rootfs: bool,
    pub(crate) privileged: bool,
    pub(crate) limits: ContainerResourceLimits,
    pub(crate) env: Vec<ContainerEnvBinding>,
    pub(crate) cleanup_strategy: String,
}

impl ContainerBackendProfile {
    /// Validates the fail-closed container invariants: no privileged
    /// containers, explicit non-root user, positive resource limits,
    /// workspace-scoped mounts only, and secret env values only via
    /// `vault://` references.
    ///
    /// # Errors
    /// Returns a human-readable message naming the first violated invariant.
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.profile_id.trim().is_empty() {
            return Err("container backend profile_id must not be empty".to_owned());
        }
        if self.image.trim().is_empty() {
            return Err("container backend image must not be empty".to_owned());
        }
        if docker_image_digest_sha256(self.image.as_str()).is_none() {
            return Err("container backend image must be pinned by sha256 digest".to_owned());
        }
        if self.privileged {
            return Err(
                "container backend profiles are fail-closed for privileged containers".to_owned()
            );
        }
        if container_user_is_root(self.user.as_str()) {
            return Err("container backend user must be an explicit non-root user".to_owned());
        }
        if !self.readonly_rootfs {
            return Err("container backend root filesystem must be read-only".to_owned());
        }
        if self.limits.cpu_time_limit_ms == 0
            || self.limits.memory_limit_bytes == 0
            || self.limits.max_output_bytes == 0
        {
            return Err("container backend limits must be positive".to_owned());
        }
        if self.mounts.is_empty() {
            return Err("container backend requires a workspace-scoped mount".to_owned());
        }
        if self.mounts.iter().any(|mount| !mount.workspace_scoped) {
            return Err("container backend mounts must be workspace-scoped".to_owned());
        }
        if self.mounts.iter().any(|mount| {
            mount.host_path.trim().is_empty()
                || mount.container_path.trim().is_empty()
                || !mount.container_path.starts_with('/')
        }) {
            return Err(
                "container backend mounts require non-empty host paths and absolute container paths"
                    .to_owned(),
            );
        }
        // A literal value under a sensitive-looking env name is treated as a
        // leaked secret regardless of its content: secret material may only
        // travel as a vault reference.
        if self.env.iter().any(|binding| {
            matches!(binding.source_kind, ContainerEnvSourceKind::LiteralSafeValue)
                && palyra_common::redaction::is_sensitive_key(binding.name.as_str())
        }) {
            return Err("container backend env secrets must use Vault refs".to_owned());
        }
        if self.env.iter().any(|binding| {
            matches!(binding.source_kind, ContainerEnvSourceKind::VaultRef)
                && !binding.value.starts_with("vault://")
        }) {
            return Err("container backend Vault env bindings must use vault:// handles".to_owned());
        }
        Ok(())
    }

    fn primary_workspace_mount(&self) -> Option<&ContainerMountPolicy> {
        self.mounts.iter().find(|mount| mount.workspace_scoped)
    }
}

fn container_backend_profile_from_config(
    profile: &ExecutionBackendProfileConfig,
) -> Result<ContainerBackendProfile, String> {
    let container = profile.container.as_ref().ok_or_else(|| {
        format!("Docker execution backend profile '{}' requires a container block", profile.id)
    })?;
    Ok(ContainerBackendProfile {
        profile_id: profile.id.clone(),
        runtime: ContainerRuntimeKind::Docker,
        image: container.image.clone(),
        mounts: vec![container_workspace_mount_from_config(container)],
        network: container_network_policy_from_config(container.network.as_str())?,
        user: container.user.clone(),
        readonly_rootfs: container.readonly_rootfs,
        privileged: container.privileged,
        limits: ContainerResourceLimits {
            cpu_time_limit_ms: container.resource_limits.cpu_time_limit_ms,
            memory_limit_bytes: container.resource_limits.memory_limit_bytes,
            max_output_bytes: container.resource_limits.max_output_bytes,
        },
        env: container
            .env
            .iter()
            .map(container_env_binding_from_config)
            .collect::<Result<Vec<_>, _>>()?,
        cleanup_strategy: container.cleanup_strategy.clone(),
    })
}

fn container_workspace_mount_from_config(
    container: &ExecutionBackendContainerProfileConfig,
) -> ContainerMountPolicy {
    ContainerMountPolicy {
        host_path: container.workspace_mount.host_path.clone(),
        container_path: container.workspace_mount.container_path.clone(),
        read_only: container.workspace_mount.read_only,
        workspace_scoped: true,
    }
}

fn container_network_policy_from_config(raw: &str) -> Result<ContainerNetworkPolicy, String> {
    match raw.trim().to_ascii_lowercase().replace('-', "_").as_str() {
        "none" => Ok(ContainerNetworkPolicy::None),
        "egress_proxy" => Ok(ContainerNetworkPolicy::EgressProxy),
        other => {
            Err(format!("container backend network must be none or egress_proxy, got {other}"))
        }
    }
}

fn container_env_binding_from_config(
    binding: &ExecutionBackendContainerEnvBindingConfig,
) -> Result<ContainerEnvBinding, String> {
    Ok(ContainerEnvBinding {
        name: binding.name.clone(),
        source_kind: container_env_source_kind_from_config(binding.source_kind.as_str())?,
        value: binding.value.clone(),
    })
}

fn container_env_source_kind_from_config(raw: &str) -> Result<ContainerEnvSourceKind, String> {
    match raw.trim().to_ascii_lowercase().replace('-', "_").as_str() {
        "literal_safe_value" => Ok(ContainerEnvSourceKind::LiteralSafeValue),
        "vault_ref" => Ok(ContainerEnvSourceKind::VaultRef),
        other => Err(format!(
            "container backend env source_kind must be literal_safe_value or vault_ref, got {other}"
        )),
    }
}

const DOCKER_WORKSPACE_ROOT: &str = "/workspace";
const DOCKER_EGRESS_PROXY_NETWORK: &str = "palyra-egress-proxy";

/// Runtime plan passed to a Docker engine implementation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DockerRunPlan {
    pub(crate) profile_id: String,
    pub(crate) image: String,
    pub(crate) image_digest_sha256: String,
    pub(crate) user: String,
    pub(crate) readonly_rootfs: bool,
    pub(crate) network: ContainerNetworkPolicy,
    pub(crate) mounts: Vec<ContainerMountPolicy>,
    pub(crate) env: Vec<ContainerEnvBinding>,
    pub(crate) command: String,
    pub(crate) args: Vec<String>,
    pub(crate) working_dir: String,
    pub(crate) limits: ContainerResourceLimits,
    pub(crate) workspace_writeback: WorkspaceWritebackMode,
    pub(crate) cleanup_strategy: String,
}

/// Container cleanup evidence emitted by a Docker engine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DockerCleanupAttestation {
    pub(crate) strategy: String,
    pub(crate) container_removed: bool,
    pub(crate) volume_removed: bool,
    pub(crate) success: bool,
    pub(crate) reason_code: String,
}

/// Container resource usage summary attached to process output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DockerResourceUsage {
    pub(crate) duration_ms: u64,
    pub(crate) memory_limit_bytes: u64,
    pub(crate) cpu_time_limit_ms: u64,
}

/// Reviewed patch-bundle writeback produced from container workspace changes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DockerPatchBundle {
    pub(crate) schema_version: u8,
    pub(crate) reviewed: bool,
    pub(crate) patch_sha256: String,
    pub(crate) file_count: usize,
}

/// Result returned by a Docker engine after running a container.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DockerRunReport {
    pub(crate) exit_code: i32,
    pub(crate) stdout: Vec<u8>,
    pub(crate) stderr: Vec<u8>,
    pub(crate) resource_usage: DockerResourceUsage,
    pub(crate) cleanup: DockerCleanupAttestation,
    pub(crate) patch_bundle: Option<DockerPatchBundle>,
}

/// Classified Docker engine failure before a container result exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DockerEngineError {
    pub(crate) reason_code: String,
    pub(crate) message: String,
}

type DockerEngineFuture<'a> =
    Pin<Box<dyn Future<Output = Result<DockerRunReport, DockerEngineError>> + Send + 'a>>;

/// Narrow execution boundary for Docker backends.
pub(crate) trait DockerEngine: Send + Sync {
    fn run<'a>(&'a self, plan: DockerRunPlan) -> DockerEngineFuture<'a>;
}

/// Docker CLI implementation used outside tests when an explicit profile is configured.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct DockerCliEngine;

impl DockerEngine for DockerCliEngine {
    fn run<'a>(&'a self, plan: DockerRunPlan) -> DockerEngineFuture<'a> {
        Box::pin(async move {
            if plan.mounts.iter().any(|mount| !mount.read_only) {
                return Err(DockerEngineError {
                    reason_code: "docker.writeback.capture_unavailable".to_owned(),
                    message: format!(
                        "Docker profile {} declares a writable workspace mount; patch-bundle capture is required before host writeback is allowed",
                        plan.profile_id
                    ),
                });
            }
            let started = Instant::now();
            let mut command = tokio::process::Command::new("docker");
            command.arg("run").arg("--rm");
            if plan.readonly_rootfs {
                command.arg("--read-only");
            }
            command.arg("--user").arg(plan.user.as_str());
            command.arg("--network").arg(docker_network_arg(plan.network));
            command.arg("--workdir").arg(plan.working_dir.as_str());
            command.arg("--memory").arg(format!("{}b", plan.limits.memory_limit_bytes));
            for mount in &plan.mounts {
                command.arg("--mount").arg(docker_mount_arg(mount));
            }
            for binding in &plan.env {
                match binding.source_kind {
                    ContainerEnvSourceKind::LiteralSafeValue => {
                        command.arg("--env").arg(format!("{}={}", binding.name, binding.value));
                    }
                    ContainerEnvSourceKind::VaultRef => {
                        return Err(DockerEngineError {
                            reason_code: "docker.env.vault_resolution_unavailable".to_owned(),
                            message: format!(
                                "Docker profile {} declares Vault env binding {}; vault resolution is not wired into DockerRunner yet",
                                plan.profile_id, binding.name
                            ),
                        });
                    }
                }
            }
            command.arg(plan.image.as_str());
            command.arg(plan.command.as_str());
            command.args(plan.args.iter().map(String::as_str));
            let output = command.output().await.map_err(|error| DockerEngineError {
                reason_code: "docker.spawn_failed".to_owned(),
                message: format!(
                    "failed to launch Docker CLI for profile {}: {error}",
                    plan.profile_id
                ),
            })?;
            Ok(DockerRunReport {
                exit_code: output.status.code().unwrap_or(1),
                stdout: output.stdout,
                stderr: output.stderr,
                resource_usage: DockerResourceUsage {
                    duration_ms: started.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
                    memory_limit_bytes: plan.limits.memory_limit_bytes,
                    cpu_time_limit_ms: plan.limits.cpu_time_limit_ms,
                },
                cleanup: DockerCleanupAttestation {
                    strategy: plan.cleanup_strategy,
                    container_removed: true,
                    volume_removed: true,
                    success: true,
                    reason_code: "docker.cleanup.run_rm".to_owned(),
                },
                patch_bundle: None,
            })
        })
    }
}

/// Docker runner adapter. Profile validation happens before any engine call.
#[derive(Debug)]
pub(crate) struct DockerRunner<E: DockerEngine> {
    profile: ContainerBackendProfile,
    engine: E,
}

impl<E: DockerEngine> DockerRunner<E> {
    /// Builds a Docker runner from an already parsed container profile.
    ///
    /// # Errors
    /// Returns the first profile invariant violation.
    pub(crate) fn new(profile: ContainerBackendProfile, engine: E) -> Result<Self, String> {
        profile.validate()?;
        if !matches!(profile.runtime, ContainerRuntimeKind::Docker) {
            return Err("DockerRunner requires a docker container profile".to_owned());
        }
        Ok(Self { profile, engine })
    }
}

impl<E: DockerEngine> ExecutionBackendRunner for DockerRunner<E> {
    fn backend_preference(&self) -> ExecutionBackendPreference {
        ExecutionBackendPreference::Docker
    }

    fn runner_id(&self) -> &'static str {
        "docker_runner"
    }

    fn runner_version(&self) -> &'static str {
        "v1"
    }

    fn capabilities(&self) -> &'static [ExecutionBackendRunnerCapability] {
        &[
            ExecutionBackendRunnerCapability::RunProcess,
            ExecutionBackendRunnerCapability::OpenWorkspace,
            ExecutionBackendRunnerCapability::CommitOrPatchBundle,
            ExecutionBackendRunnerCapability::Cancel,
            ExecutionBackendRunnerCapability::Cleanup,
            ExecutionBackendRunnerCapability::HealthProbe,
            ExecutionBackendRunnerCapability::AttestationManifest,
        ]
    }

    fn run_process<'a>(
        &'a self,
        request: ExecutionBackendProcessRunRequest<'a>,
    ) -> RunnerExecutionFuture<'a> {
        Box::pin(async move {
            let plan =
                match docker_process_run_plan(&self.profile, request.config, request.input_json) {
                    Ok(plan) => plan,
                    Err(error) => {
                        return docker_error_outcome(
                            request.proposal_id,
                            request.tool_name,
                            request.input_json,
                            error.reason_code.as_str(),
                            error.message,
                        );
                    }
                };
            match self.engine.run(plan.clone()).await {
                Ok(report) => docker_process_run_outcome(
                    request.proposal_id,
                    request.tool_name,
                    request.input_json,
                    &plan,
                    report,
                ),
                Err(error) => docker_error_outcome(
                    request.proposal_id,
                    request.tool_name,
                    request.input_json,
                    error.reason_code.as_str(),
                    error.message,
                ),
            }
        })
    }

    fn health_probe(&self) -> ExecutionBackendRunnerHealth {
        ExecutionBackendRunnerHealth {
            backend_id: ExecutionBackendPreference::Docker.as_str().to_owned(),
            status: ExecutionBackendHealthStatus::Healthy,
            reason_code: "runner.health.docker.profile_valid".to_owned(),
            summary: format!(
                "Docker profile {} passed fail-closed validation",
                self.profile.profile_id
            ),
        }
    }
}

fn docker_process_run_plan(
    profile: &ContainerBackendProfile,
    config: &ToolCallConfig,
    input_json: &[u8],
) -> Result<DockerRunPlan, DockerEngineError> {
    profile.validate().map_err(|message| DockerEngineError {
        reason_code: "docker.profile.invalid".to_owned(),
        message,
    })?;
    let input = parse_process_runner_tool_input(input_json).map_err(|error| DockerEngineError {
        reason_code: "docker.process.invalid_input".to_owned(),
        message: format!("DockerRunner rejected process input: {error}"),
    })?;
    validate_docker_process_input(config, &input)?;
    let image_digest_sha256 =
        docker_image_digest_sha256(profile.image.as_str()).ok_or_else(|| DockerEngineError {
            reason_code: "docker.profile.image_digest_missing".to_owned(),
            message: "DockerRunner requires image references pinned by sha256 digest".to_owned(),
        })?;
    let Some(workspace_mount) = profile.primary_workspace_mount() else {
        return Err(DockerEngineError {
            reason_code: "docker.profile.workspace_mount_missing".to_owned(),
            message: "DockerRunner requires a workspace-scoped mount".to_owned(),
        });
    };
    Ok(DockerRunPlan {
        profile_id: profile.profile_id.clone(),
        image: profile.image.clone(),
        image_digest_sha256,
        user: profile.user.clone(),
        readonly_rootfs: profile.readonly_rootfs,
        network: profile.network,
        mounts: vec![workspace_mount.clone()],
        env: profile.env.clone(),
        command: input.command,
        args: input.args,
        working_dir: docker_container_working_dir(input.cwd.as_deref())?,
        limits: profile.limits.clone(),
        workspace_writeback: WorkspaceWritebackMode::PatchBundle,
        cleanup_strategy: profile.cleanup_strategy.clone(),
    })
}

fn validate_docker_process_input(
    config: &ToolCallConfig,
    input: &ProcessRunnerToolInput,
) -> Result<(), DockerEngineError> {
    if input.background || input.keep_running_after_run {
        return Err(DockerEngineError {
            reason_code: "docker.process.background_unsupported".to_owned(),
            message: "DockerRunner does not support background process handles yet".to_owned(),
        });
    }
    if !input.prepend_path.is_empty() {
        return Err(DockerEngineError {
            reason_code: "docker.process.prepend_path_unsupported".to_owned(),
            message: "DockerRunner does not accept host PATH injection".to_owned(),
        });
    }
    if input.env.keys().any(|name| is_sensitive_key(name.as_str())) {
        return Err(DockerEngineError {
            reason_code: "docker.process.secret_env_denied".to_owned(),
            message: "DockerRunner rejects sensitive env names in tool input".to_owned(),
        });
    }
    let command = input.command.trim();
    if command.is_empty() || command.chars().any(char::is_whitespace) {
        return Err(DockerEngineError {
            reason_code: "docker.process.invalid_command".to_owned(),
            message: "DockerRunner requires a single executable token in command".to_owned(),
        });
    }
    let allowed = &config.process_runner.allowed_executables;
    if !allowed.iter().any(|entry| docker_command_allowlist_matches(entry, command)) {
        return Err(DockerEngineError {
            reason_code: "docker.process.executable_denied".to_owned(),
            message: format!("DockerRunner command {command:?} is not in the process allowlist"),
        });
    }
    Ok(())
}

fn docker_process_run_outcome(
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    plan: &DockerRunPlan,
    report: DockerRunReport,
) -> ToolExecutionOutcome {
    let cleanup_success = report.cleanup.success;
    let success = report.exit_code == 0 && cleanup_success;
    let stdout_view = docker_stream_output_view(&report.stdout);
    let stderr_view = docker_stream_output_view(&report.stderr);
    let process_risk = parse_process_runner_tool_input(input_json)
        .map(|input| {
            classify_process_run(
                &input,
                ProcessRiskContext {
                    workspace_root: Some(Path::new(plan.mounts[0].host_path.as_str())),
                    resolved_cwd: None,
                },
            )
        })
        .ok();
    let output_manifest = json!({
        "schema_version": 1,
        "profile_id": plan.profile_id,
        "image_digest_sha256": plan.image_digest_sha256,
        "stdout_sha256": sha256_hex(report.stdout.as_slice()),
        "stderr_sha256": sha256_hex(report.stderr.as_slice()),
        "workspace_writeback": plan.workspace_writeback.as_str(),
        "cleanup_success": cleanup_success,
        "patch_bundle_sha256": report.patch_bundle.as_ref().map(|bundle| bundle.patch_sha256.as_str()),
    });
    let output_manifest_sha256 =
        sha256_hex(serde_json::to_vec(&output_manifest).unwrap_or_default().as_slice());
    let output = json!({
        "schema_version": 2,
        "exit_code": report.exit_code,
        "stdout": stdout_view.model_text,
        "stderr": stderr_view.model_text,
        "stdout_truncated": false,
        "stderr_truncated": false,
        "stdout_redacted": stdout_view.redacted,
        "stderr_redacted": stderr_view.redacted,
        "stdout_bytes": report.stdout.len(),
        "stderr_bytes": report.stderr.len(),
        "duration_ms": report.resource_usage.duration_ms,
        "tier": "container_profile",
        "sandbox_backend": "docker",
        "process_risk": process_risk,
        "streams": {
            "stdout": stdout_view.metadata,
            "stderr": stderr_view.metadata,
        },
        "resource_usage": report.resource_usage,
        "workspace_writeback": {
            "mode": plan.workspace_writeback.as_str(),
            "authoritative_workspace_mutation": false,
            "patch_bundle": report.patch_bundle,
        },
        "cleanup": report.cleanup,
        "output_manifest": output_manifest,
        "output_manifest_sha256": output_manifest_sha256,
    });
    let error = if success {
        String::new()
    } else if !cleanup_success {
        "DockerRunner cleanup failed after container execution".to_owned()
    } else {
        format!("DockerRunner process exited unsuccessfully with code {}", report.exit_code)
    };
    build_tool_execution_outcome(
        proposal_id,
        tool_name,
        input_json,
        success,
        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
        error,
        false,
        "docker".to_owned(),
        "container_profile".to_owned(),
    )
}

fn docker_error_outcome(
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    reason_code: &str,
    message: String,
) -> ToolExecutionOutcome {
    let output = json!({
        "success": false,
        "event": "execution_backend.docker_runner",
        "status": "unavailable",
        "reason_code": reason_code,
        "repair_hint": "Configure an allowlisted non-root Docker profile with a sha256-pinned image, read-only root filesystem, workspace-scoped mount, and patch-bundle writeback.",
    });
    build_tool_execution_outcome(
        proposal_id,
        tool_name,
        input_json,
        false,
        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
        message,
        false,
        "docker".to_owned(),
        "container_profile_preflight".to_owned(),
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DockerStreamOutputView {
    model_text: String,
    redacted: bool,
    metadata: serde_json::Value,
}

fn docker_stream_output_view(output: &[u8]) -> DockerStreamOutputView {
    let raw = String::from_utf8_lossy(output).into_owned();
    let model_text = redact_diagnostic_text(raw.as_str());
    DockerStreamOutputView {
        redacted: model_text != raw,
        metadata: json!({
            "size_bytes": output.len(),
            "captured_bytes": output.len(),
            "truncated": false,
            "binary": false,
            "encoding": "utf-8-lossy",
            "sha256": sha256_hex(output),
        }),
        model_text,
    }
}

fn docker_network_arg(network: ContainerNetworkPolicy) -> &'static str {
    match network {
        ContainerNetworkPolicy::None => "none",
        ContainerNetworkPolicy::EgressProxy => DOCKER_EGRESS_PROXY_NETWORK,
    }
}

fn docker_mount_arg(mount: &ContainerMountPolicy) -> String {
    let mut value = format!(
        "type=bind,src={},dst={}",
        mount.host_path.replace(',', "\\,"),
        mount.container_path.replace(',', "\\,")
    );
    if mount.read_only {
        value.push_str(",readonly");
    }
    value
}

fn docker_container_working_dir(cwd: Option<&str>) -> Result<String, DockerEngineError> {
    let Some(cwd) = cwd.map(str::trim).filter(|cwd| !cwd.is_empty()) else {
        return Ok(DOCKER_WORKSPACE_ROOT.to_owned());
    };
    let normalized = cwd.replace('\\', "/");
    if normalized == "/workspace" || normalized == "workspace" {
        return Ok(DOCKER_WORKSPACE_ROOT.to_owned());
    }
    let relative = normalized
        .strip_prefix("/workspace/")
        .or_else(|| normalized.strip_prefix("workspace/"))
        .unwrap_or(normalized.as_str());
    if relative.starts_with('/') || relative.split('/').any(|part| part == "..") {
        return Err(DockerEngineError {
            reason_code: "docker.process.cwd_denied".to_owned(),
            message: "DockerRunner cwd must stay inside /workspace".to_owned(),
        });
    }
    Ok(if relative == "." {
        DOCKER_WORKSPACE_ROOT.to_owned()
    } else {
        format!("{DOCKER_WORKSPACE_ROOT}/{relative}")
    })
}

fn docker_command_allowlist_matches(allowed: &str, command: &str) -> bool {
    let allowed = allowed.trim();
    if allowed == "*" {
        return true;
    }
    let command_name =
        Path::new(command).file_name().and_then(|name| name.to_str()).unwrap_or(command);
    allowed.eq_ignore_ascii_case(command) || allowed.eq_ignore_ascii_case(command_name)
}

fn docker_image_digest_sha256(image: &str) -> Option<String> {
    let (_, digest) = image.rsplit_once("@sha256:")?;
    let digest = digest.trim();
    (digest.len() == 64 && digest.chars().all(|ch| ch.is_ascii_hexdigit()))
        .then(|| digest.to_ascii_lowercase())
}

fn container_user_is_root(user: &str) -> bool {
    let user = user.trim();
    if user.is_empty() || user.eq_ignore_ascii_case("root") {
        return true;
    }
    let Some(first) = user.split(':').next() else {
        return true;
    };
    first == "0"
}

fn sha256_hex(input: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input);
    hex::encode(hasher.finalize())
}

/// Declarative SSH worker profile. All connection material is referenced via
/// `vault://` or `identity://` handles -- never plaintext (validated).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SshWorkerBackendProfile {
    pub(crate) profile_id: String,
    pub(crate) host_handle: String,
    pub(crate) user_handle: String,
    pub(crate) identity_handle: String,
    pub(crate) host_trust_handle: String,
    pub(crate) worker_protocol: String,
    pub(crate) workspace_strategy: WorkspaceStrategyDescriptor,
}

impl SshWorkerBackendProfile {
    /// Validates the fail-closed SSH worker invariants: handle-only
    /// credentials, the versioned worker RPC envelope (no raw shell), and a
    /// remote-lease workspace strategy.
    ///
    /// # Errors
    /// Returns a human-readable message naming the first violated invariant.
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.profile_id.trim().is_empty() {
            return Err("ssh worker profile_id must not be empty".to_owned());
        }
        for (field_name, value) in [
            ("host_handle", self.host_handle.as_str()),
            ("user_handle", self.user_handle.as_str()),
            ("identity_handle", self.identity_handle.as_str()),
            ("host_trust_handle", self.host_trust_handle.as_str()),
        ] {
            if !(value.starts_with("vault://") || value.starts_with("identity://")) {
                return Err(format!(
                    "ssh worker {field_name} must be a Vault or identity handle, not plaintext"
                ));
            }
        }
        if self.worker_protocol != "palyra-worker-rpc/v1" {
            return Err("ssh worker backend must use palyra-worker-rpc/v1 envelope".to_owned());
        }
        if !matches!(self.workspace_strategy.kind, WorkspaceStrategyKind::RemoteLeaseWorkspace) {
            return Err("ssh worker backend requires a remote lease workspace strategy".to_owned());
        }
        Ok(())
    }
}

/// Parses an operator-supplied backend preference, accepting short aliases
/// (`auto`, `local`, `node`, `worker`, `ssh`, ...). Empty input means
/// `Automatic`.
///
/// # Errors
/// Returns a message listing the canonical values when `raw` is unknown.
pub(crate) fn parse_execution_backend_preference(
    raw: &str,
    field_name: &str,
) -> Result<ExecutionBackendPreference, String> {
    let normalized = raw.trim().to_ascii_lowercase();
    let preference = match normalized.as_str() {
        "" | "automatic" | "auto" => ExecutionBackendPreference::Automatic,
        "local_sandbox" | "local" | "sandbox" => ExecutionBackendPreference::LocalSandbox,
        "desktop_node" | "node" | "remote_node" => ExecutionBackendPreference::DesktopNode,
        "docker" | "container" | "container_sandbox" => ExecutionBackendPreference::Docker,
        "networked_worker" | "networked" | "worker" | "remote_worker" => {
            ExecutionBackendPreference::NetworkedWorker
        }
        "ssh_tunnel" | "ssh" | "tunnel" => ExecutionBackendPreference::SshTunnel,
        _ => {
            return Err(format!(
                "{field_name} must be one of automatic, local_sandbox, desktop_node, docker, networked_worker, ssh_tunnel"
            ));
        }
    };
    Ok(preference)
}

/// Optional-input wrapper around [`parse_execution_backend_preference`].
///
/// # Errors
/// Propagates the parse error for a present-but-invalid value.
pub(crate) fn parse_optional_execution_backend_preference(
    raw: Option<&str>,
    field_name: &str,
) -> Result<Option<ExecutionBackendPreference>, String> {
    raw.map(|value| parse_execution_backend_preference(value, field_name)).transpose()
}

/// Builds the backend inventory without live worker-fleet state (defaults to
/// an empty fleet); see
/// [`build_execution_backend_inventory_with_worker_state`].
#[allow(dead_code)]
#[must_use]
pub(crate) fn build_execution_backend_inventory(
    policy: &SandboxProcessRunnerPolicy,
    nodes: &[RegisteredNodeRecord],
    now_unix_ms: i64,
    feature_rollouts: &FeatureRolloutsConfig,
    networked_workers: &NetworkedWorkersConfig,
) -> Vec<ExecutionBackendInventoryRecord> {
    build_execution_backend_inventory_with_worker_state(
        policy,
        nodes,
        now_unix_ms,
        feature_rollouts,
        networked_workers,
        WorkerFleetSnapshot::default(),
        &WorkerFleetPolicy::default(),
    )
}

/// Builds the full backend inventory from the sandbox policy, registered
/// desktop nodes, rollout flags, and the live worker-fleet snapshot. Node
/// health is judged by heartbeat recency (`NODE_HEALTHY_AFTER_MS`).
#[must_use]
pub(crate) fn build_execution_backend_inventory_with_worker_state(
    policy: &SandboxProcessRunnerPolicy,
    nodes: &[RegisteredNodeRecord],
    now_unix_ms: i64,
    feature_rollouts: &FeatureRolloutsConfig,
    networked_workers: &NetworkedWorkersConfig,
    worker_snapshot: WorkerFleetSnapshot,
    worker_policy: &WorkerFleetPolicy,
) -> Vec<ExecutionBackendInventoryRecord> {
    let healthy_nodes = nodes
        .iter()
        .filter(|node| {
            now_unix_ms.saturating_sub(node.last_seen_at_unix_ms.max(0)) <= NODE_HEALTHY_AFTER_MS
        })
        .collect::<Vec<_>>();
    build_execution_backend_inventory_with_docker_rollout(
        policy,
        nodes.len(),
        healthy_nodes.as_slice(),
        feature_rollouts.execution_backend_remote_node,
        feature_rollouts.execution_backend_networked_worker,
        feature_rollouts.execution_backend_docker,
        feature_rollouts.networked_workers,
        feature_rollouts.execution_backend_ssh_tunnel,
        networked_workers,
        worker_snapshot,
        worker_policy,
    )
}

#[allow(clippy::too_many_arguments)]
fn build_execution_backend_inventory_with_rollout(
    policy: &SandboxProcessRunnerPolicy,
    total_nodes: usize,
    healthy_nodes: &[&RegisteredNodeRecord],
    remote_node_rollout: FeatureRolloutSetting,
    networked_worker_rollout: FeatureRolloutSetting,
    networked_workers_runtime_rollout: FeatureRolloutSetting,
    ssh_tunnel_rollout: FeatureRolloutSetting,
    networked_workers: &NetworkedWorkersConfig,
    worker_snapshot: WorkerFleetSnapshot,
    worker_policy: &WorkerFleetPolicy,
) -> Vec<ExecutionBackendInventoryRecord> {
    build_execution_backend_inventory_with_docker_rollout(
        policy,
        total_nodes,
        healthy_nodes,
        remote_node_rollout,
        networked_worker_rollout,
        FeatureRolloutSetting::default(),
        networked_workers_runtime_rollout,
        ssh_tunnel_rollout,
        networked_workers,
        worker_snapshot,
        worker_policy,
    )
}

#[allow(clippy::too_many_arguments)]
fn build_execution_backend_inventory_with_docker_rollout(
    policy: &SandboxProcessRunnerPolicy,
    total_nodes: usize,
    healthy_nodes: &[&RegisteredNodeRecord],
    remote_node_rollout: FeatureRolloutSetting,
    networked_worker_rollout: FeatureRolloutSetting,
    docker_rollout: FeatureRolloutSetting,
    networked_workers_runtime_rollout: FeatureRolloutSetting,
    ssh_tunnel_rollout: FeatureRolloutSetting,
    networked_workers: &NetworkedWorkersConfig,
    worker_snapshot: WorkerFleetSnapshot,
    worker_policy: &WorkerFleetPolicy,
) -> Vec<ExecutionBackendInventoryRecord> {
    vec![
        local_sandbox_inventory_record(policy),
        desktop_node_inventory_record(total_nodes, healthy_nodes, remote_node_rollout),
        docker_inventory_record(docker_rollout),
        networked_worker_inventory_record(
            networked_worker_rollout,
            networked_workers_runtime_rollout,
            networked_workers,
            worker_snapshot,
            worker_policy,
        ),
        ssh_tunnel_inventory_record(ssh_tunnel_rollout),
    ]
}

/// Rejects an explicit backend selection that the current inventory cannot
/// honor; `Automatic` is always accepted.
///
/// # Errors
/// Returns an operator-facing message when the backend is missing from the
/// inventory or not selectable.
pub(crate) fn validate_execution_backend_selection(
    preference: ExecutionBackendPreference,
    inventory: &[ExecutionBackendInventoryRecord],
) -> Result<(), String> {
    if matches!(preference, ExecutionBackendPreference::Automatic) {
        return Ok(());
    }
    let record = inventory
        .iter()
        .find(|entry| entry.backend_id == preference.as_str())
        .ok_or_else(|| format!("execution backend '{}' is not available", preference.as_str()))?;
    if record.selectable {
        return Ok(());
    }
    Err(format!(
        "execution backend '{}' cannot be selected: {}",
        preference.as_str(),
        record.operator_summary
    ))
}

/// Resolves a capability-aware backend request without silent substitution.
///
/// `Automatic` picks the default-selected match first, then any match. An
/// explicit preference that cannot satisfy the request fails closed (the
/// resolution keeps the requested backend and an `unsatisfied` reason code)
/// rather than falling back to another backend.
#[must_use]
pub(crate) fn resolve_execution_backend_for_request(
    request: &ExecutionBackendResolutionRequest,
    inventory: &[ExecutionBackendInventoryRecord],
) -> ExecutionBackendResolution {
    if matches!(request.preference, ExecutionBackendPreference::Automatic) {
        let selected = inventory
            .iter()
            .filter(|entry| execution_backend_matches_request(entry, request))
            .find(|entry| entry.selected_by_default)
            .or_else(|| {
                inventory.iter().find(|entry| execution_backend_matches_request(entry, request))
            });
        if let Some(record) = selected {
            let resolved =
                parse_execution_backend_preference(record.backend_id.as_str(), "backend_id")
                    .unwrap_or(ExecutionBackendPreference::Automatic);
            return ExecutionBackendResolution {
                requested: request.preference,
                resolved,
                fallback_used: false,
                reason_code: format!("backend.available.{}", record.backend_id),
                approval_required: !matches!(resolved, ExecutionBackendPreference::LocalSandbox),
                reason: format!(
                    "Backend '{}' satisfies required capabilities and workspace strategy '{}'. {}",
                    record.backend_id,
                    record.workspace_strategy.kind.as_str(),
                    record.operator_summary
                ),
            };
        }
        return ExecutionBackendResolution {
            requested: request.preference,
            resolved: ExecutionBackendPreference::Automatic,
            fallback_used: false,
            reason_code: "backend.policy.no_matching_backend".to_owned(),
            approval_required: false,
            reason: "No selectable execution backend satisfies the requested capabilities and workspace strategy."
                .to_owned(),
        };
    }

    let Some(record) =
        inventory.iter().find(|entry| entry.backend_id == request.preference.as_str())
    else {
        return ExecutionBackendResolution {
            requested: request.preference,
            resolved: request.preference,
            fallback_used: false,
            reason_code: format!("backend.unavailable.{}", request.preference.as_str()),
            approval_required: !matches!(
                request.preference,
                ExecutionBackendPreference::LocalSandbox
            ),
            reason: format!(
                "Requested backend '{}' is missing from inventory.",
                request.preference.as_str()
            ),
        };
    };
    if execution_backend_matches_request(record, request) {
        return ExecutionBackendResolution {
            requested: request.preference,
            resolved: request.preference,
            fallback_used: false,
            reason_code: format!("backend.available.{}", request.preference.as_str()),
            approval_required: !matches!(
                request.preference,
                ExecutionBackendPreference::LocalSandbox
            ),
            reason: record.operator_summary.clone(),
        };
    }

    ExecutionBackendResolution {
        requested: request.preference,
        resolved: request.preference,
        fallback_used: false,
        reason_code: format!("backend.policy.unsatisfied.{}", request.preference.as_str()),
        approval_required: !matches!(request.preference, ExecutionBackendPreference::LocalSandbox),
        reason: backend_request_mismatch_reason(record, request),
    }
}

/// Resolves a plain backend preference with fallback semantics.
///
/// `Automatic` pins to the local sandbox. Unselectable preview backends fall
/// back to the local sandbox (flagged via `fallback_used`) -- except Docker
/// and `NetworkedWorker`, where falling back would silently downgrade an
/// explicit isolation or worker grant onto the daemon host, so those requests
/// fail closed instead. Every non-local resolution requires approval.
#[must_use]
pub(crate) fn resolve_execution_backend(
    preference: ExecutionBackendPreference,
    inventory: &[ExecutionBackendInventoryRecord],
) -> ExecutionBackendResolution {
    let local_record = inventory
        .iter()
        .find(|entry| entry.backend_id == ExecutionBackendPreference::LocalSandbox.as_str());
    let requested_record = inventory.iter().find(|entry| entry.backend_id == preference.as_str());
    if matches!(preference, ExecutionBackendPreference::Automatic) {
        if let Some(record) = local_record {
            return ExecutionBackendResolution {
                requested: preference,
                resolved: ExecutionBackendPreference::LocalSandbox,
                fallback_used: false,
                reason_code: "backend.default.local_sandbox".to_owned(),
                approval_required: false,
                reason: if record.selectable {
                    "Automatic keeps execution on the daemon host until an operator explicitly opts into a preview backend."
                        .to_owned()
                } else {
                    format!(
                        "Automatic prefers the daemon-host backend; the current local posture is degraded: {}",
                        record.operator_summary
                    )
                },
            };
        }
        return ExecutionBackendResolution {
            requested: preference,
            resolved: ExecutionBackendPreference::Automatic,
            fallback_used: false,
            reason_code: "backend.inventory.missing".to_owned(),
            approval_required: false,
            reason: "No execution backend inventory is available.".to_owned(),
        };
    }

    if let Some(record) = requested_record {
        if record.selectable {
            return ExecutionBackendResolution {
                requested: preference,
                resolved: preference,
                fallback_used: false,
                reason_code: format!("backend.available.{}", preference.as_str()),
                approval_required: !matches!(preference, ExecutionBackendPreference::LocalSandbox),
                reason: record.operator_summary.clone(),
            };
        }
    }

    // Deny local fallback for explicit isolation backends: the caller asked
    // for a container or attested worker grant and must not silently end up
    // on the daemon host (pinned by tests).
    if matches!(
        preference,
        ExecutionBackendPreference::Docker | ExecutionBackendPreference::NetworkedWorker
    ) {
        return ExecutionBackendResolution {
            requested: preference,
            resolved: preference,
            fallback_used: false,
            reason_code: format!("backend.unavailable.{}", preference.as_str()),
            approval_required: true,
            reason: requested_record
                .map(|record| {
                    format!(
                        "Requested backend '{}' is not selectable and local fallback is denied for explicit isolation grants. {}",
                        preference.as_str(),
                        record.operator_summary
                    )
                })
                .unwrap_or_else(|| {
                    format!(
                        "Requested backend '{}' is missing from inventory and local fallback is denied for explicit isolation grants.",
                        preference.as_str()
                    )
                }),
        };
    }

    if let Some(record) = local_record.filter(|entry| entry.selectable) {
        return ExecutionBackendResolution {
            requested: preference,
            resolved: ExecutionBackendPreference::LocalSandbox,
            fallback_used: true,
            reason_code: format!("backend.fallback.{}", preference.as_str()),
            approval_required: false,
            reason: format!(
                "Requested backend '{}' is not selectable right now; falling back to local_sandbox. {}",
                preference.as_str(),
                record.operator_summary
            ),
        };
    }

    let fallback = inventory.iter().find(|entry| entry.selectable);
    if let Some(record) = fallback {
        let resolved = parse_execution_backend_preference(record.backend_id.as_str(), "backend_id")
            .unwrap_or(ExecutionBackendPreference::Automatic);
        return ExecutionBackendResolution {
            requested: preference,
            resolved,
            fallback_used: true,
            reason_code: format!("backend.fallback.{}", record.backend_id),
            approval_required: !matches!(resolved, ExecutionBackendPreference::LocalSandbox),
            reason: format!(
                "Requested backend '{}' is not selectable; falling back to '{}'. {}",
                preference.as_str(),
                record.backend_id,
                record.operator_summary
            ),
        };
    }

    ExecutionBackendResolution {
        requested: preference,
        resolved: preference,
        fallback_used: false,
        reason_code: format!("backend.unavailable.{}", preference.as_str()),
        approval_required: !matches!(preference, ExecutionBackendPreference::LocalSandbox),
        reason: format!(
            "Requested backend '{}' is currently unavailable and no fallback backend is selectable.",
            preference.as_str()
        ),
    }
}

fn execution_backend_matches_request(
    record: &ExecutionBackendInventoryRecord,
    request: &ExecutionBackendResolutionRequest,
) -> bool {
    record.selectable
        && capabilities_satisfy(
            record.capabilities.as_slice(),
            request.required_capabilities.as_slice(),
        )
        && request
            .workspace_strategy
            .is_none_or(|strategy| record.workspace_strategy.kind == strategy)
}

// Capability names are matched case-insensitively because they arrive from
// config files and remote node registrations with varying casing.
fn capabilities_satisfy(available: &[String], required: &[String]) -> bool {
    required.iter().all(|required| {
        available.iter().any(|available| available.eq_ignore_ascii_case(required.as_str()))
    })
}

fn backend_request_mismatch_reason(
    record: &ExecutionBackendInventoryRecord,
    request: &ExecutionBackendResolutionRequest,
) -> String {
    if !record.selectable {
        return format!(
            "Requested backend '{}' is not selectable: {}",
            record.backend_id, record.operator_summary
        );
    }
    if !capabilities_satisfy(
        record.capabilities.as_slice(),
        request.required_capabilities.as_slice(),
    ) {
        return format!(
            "Requested backend '{}' does not satisfy required capabilities: {:?}",
            record.backend_id, request.required_capabilities
        );
    }
    if let Some(strategy) = request.workspace_strategy {
        if record.workspace_strategy.kind != strategy {
            return format!(
                "Requested backend '{}' uses workspace strategy '{}' but '{}' was required.",
                record.backend_id,
                record.workspace_strategy.kind.as_str(),
                strategy.as_str()
            );
        }
    }
    "Requested backend did not satisfy the execution policy request.".to_owned()
}

fn local_sandbox_inventory_record(
    policy: &SandboxProcessRunnerPolicy,
) -> ExecutionBackendInventoryRecord {
    let backend_kind = current_backend_kind();
    let backend_capabilities = current_backend_capabilities();
    let process_runner_summary = if policy.enabled {
        format!(
            "Process runner is enabled with executor '{}' and tier '{}'.",
            process_runner_executor_name(policy),
            policy.tier.as_str()
        )
    } else {
        "Process runner is disabled by runtime policy; local daemon-host execution remains the conservative default."
            .to_owned()
    };
    let operator_summary = if matches!(backend_kind.as_str(), "unsupported") {
        format!(
            "{} Tier-C isolation is unavailable on this platform, so preview backends should be treated conservatively.",
            process_runner_summary
        )
    } else {
        format!(
            "{} Tier-C backend '{}', runtime_network_isolation={}, host_allowlists={}.",
            process_runner_summary,
            backend_kind.as_str(),
            backend_capabilities.runtime_network_isolation,
            backend_capabilities.host_allowlists
        )
    };
    let mut capabilities = vec!["daemon_host_execution".to_owned(), "workspace_patch".to_owned()];
    if policy.enabled {
        capabilities.push("sandbox_process_runner".to_owned());
    }
    ExecutionBackendInventoryRecord {
        backend_id: ExecutionBackendPreference::LocalSandbox.as_str().to_owned(),
        label: ExecutionBackendPreference::LocalSandbox.label().to_owned(),
        state: if matches!(backend_kind.as_str(), "unsupported") {
            ExecutionBackendState::Degraded
        } else {
            ExecutionBackendState::Available
        },
        selectable: true,
        selected_by_default: true,
        description: ExecutionBackendPreference::LocalSandbox.description().to_owned(),
        operator_summary,
        executor_label: Some(process_runner_executor_name(policy)),
        rollout_flag: None,
        rollout_source: None,
        rollout_enabled: true,
        capabilities,
        tradeoffs: vec![
            "Most conservative default posture".to_owned(),
            "Cannot satisfy first-party desktop-native capability requests by itself".to_owned(),
        ],
        requires_attestation: false,
        requires_egress_proxy: false,
        attestation_mode: BackendAttestationMode::LocalExecutor,
        workspace_strategy: WorkspaceStrategyDescriptor::daemon_workspace_root(),
        workspace_scope_mode: "daemon_workspace_root".to_owned(),
        artifact_transport: "direct_local_filesystem".to_owned(),
        cleanup_strategy: "process_exit_and_workspace_scope_validation".to_owned(),
        supports_cancellation: true,
        supports_cleanup: true,
        health_probe: "sandbox_policy_and_tier_c_capability_probe".to_owned(),
        active_node_count: 0,
        total_node_count: 0,
    }
}

fn desktop_node_inventory_record(
    total_nodes: usize,
    healthy_nodes: &[&RegisteredNodeRecord],
    rollout: FeatureRolloutSetting,
) -> ExecutionBackendInventoryRecord {
    let capabilities = aggregate_node_capabilities(healthy_nodes);
    let (state, selectable, operator_summary) = if !rollout.enabled {
        (
            ExecutionBackendState::Disabled,
            false,
            format!(
                "Preview backend is disabled. Set {}=1 and keep at least one paired desktop node healthy before selecting it.",
                EXECUTION_BACKEND_REMOTE_NODE_ROLLOUT_ENV
            ),
        )
    } else if !healthy_nodes.is_empty() {
        (
            ExecutionBackendState::Available,
            true,
            format!(
                "{} healthy desktop node(s) are available for first-party node handoff.",
                healthy_nodes.len()
            ),
        )
    } else if total_nodes > 0 {
        (
            ExecutionBackendState::Degraded,
            false,
            format!(
                "{} desktop node(s) are registered but none are healthy enough for selection.",
                total_nodes
            ),
        )
    } else {
        (
            ExecutionBackendState::Disabled,
            false,
            "Preview backend is enabled, but no paired desktop node has registered yet.".to_owned(),
        )
    };
    ExecutionBackendInventoryRecord {
        backend_id: ExecutionBackendPreference::DesktopNode.as_str().to_owned(),
        label: ExecutionBackendPreference::DesktopNode.label().to_owned(),
        state,
        selectable,
        selected_by_default: false,
        description: ExecutionBackendPreference::DesktopNode.description().to_owned(),
        operator_summary,
        executor_label: None,
        rollout_flag: Some(EXECUTION_BACKEND_REMOTE_NODE_ROLLOUT_ENV.to_owned()),
        rollout_source: Some(rollout.source),
        rollout_enabled: rollout.enabled,
        capabilities,
        tradeoffs: vec![
            "Supports first-party desktop capabilities and local mediation flows".to_owned(),
            "Depends on node heartbeat, pairing trust, and explicit rollout opt-in".to_owned(),
        ],
        requires_attestation: true,
        requires_egress_proxy: false,
        attestation_mode: BackendAttestationMode::VaultIdentity,
        workspace_strategy: WorkspaceStrategyDescriptor::git_worktree(),
        workspace_scope_mode: "paired_node_workspace_contract".to_owned(),
        artifact_transport: "node_rpc_transfer".to_owned(),
        cleanup_strategy: "node_disconnect_or_run_completion_cleanup".to_owned(),
        supports_cancellation: true,
        supports_cleanup: true,
        health_probe: "paired_node_heartbeat_and_capability_snapshot".to_owned(),
        active_node_count: healthy_nodes.len(),
        total_node_count: total_nodes,
    }
}

fn docker_inventory_record(rollout: FeatureRolloutSetting) -> ExecutionBackendInventoryRecord {
    let (state, selectable, operator_summary) = if rollout.enabled {
        (
            ExecutionBackendState::Available,
            true,
            "Preview backend is enabled. Docker runs require an allowlisted image, workspace-scoped mount, explicit network policy, and patch-bundle writeback."
                .to_owned(),
        )
    } else {
        (
            ExecutionBackendState::Disabled,
            false,
            format!(
                "Preview backend is disabled. Set {}=1 before selecting Docker execution.",
                EXECUTION_BACKEND_DOCKER_ROLLOUT_ENV
            ),
        )
    };
    ExecutionBackendInventoryRecord {
        backend_id: ExecutionBackendPreference::Docker.as_str().to_owned(),
        label: ExecutionBackendPreference::Docker.label().to_owned(),
        state,
        selectable,
        selected_by_default: false,
        description: ExecutionBackendPreference::Docker.description().to_owned(),
        operator_summary,
        executor_label: Some("docker".to_owned()),
        rollout_flag: Some(EXECUTION_BACKEND_DOCKER_ROLLOUT_ENV.to_owned()),
        rollout_source: Some(rollout.source),
        rollout_enabled: rollout.enabled,
        capabilities: vec![
            "containerized_execution".to_owned(),
            "workspace_patch".to_owned(),
            "scoped_artifact_transport".to_owned(),
            "sandbox_process_runner".to_owned(),
        ],
        tradeoffs: vec![
            "Requires an allowlisted image and non-privileged container profile".to_owned(),
            "Authoritative workspace writes return as patch bundles for review".to_owned(),
        ],
        requires_attestation: true,
        requires_egress_proxy: true,
        attestation_mode: BackendAttestationMode::ContainerProfile,
        workspace_strategy: WorkspaceStrategyDescriptor::container_volume(),
        workspace_scope_mode: "workspace_scoped_container_mount".to_owned(),
        artifact_transport: "container_patch_bundle_transfer".to_owned(),
        cleanup_strategy: "container_and_volume_removal_attestation".to_owned(),
        supports_cancellation: true,
        supports_cleanup: true,
        health_probe: "docker_cli_profile_preflight".to_owned(),
        active_node_count: 0,
        total_node_count: 0,
    }
}

fn networked_worker_inventory_record(
    rollout: FeatureRolloutSetting,
    runtime_rollout: FeatureRolloutSetting,
    networked_workers: &NetworkedWorkersConfig,
    worker_snapshot: WorkerFleetSnapshot,
    worker_policy: &WorkerFleetPolicy,
) -> ExecutionBackendInventoryRecord {
    let (state, selectable, operator_summary) = if matches!(
        networked_workers.mode,
        RuntimePreviewMode::Disabled
    ) {
        (
            ExecutionBackendState::Disabled,
            false,
            "Networked workers runtime is disabled. Set networked_workers.mode to preview_only or enabled before advertising remote execution."
                .to_owned(),
        )
    } else if !rollout.enabled {
        (
            ExecutionBackendState::Disabled,
            false,
            format!(
                "Preview backend is disabled. Set {}=1 before attested worker registration can advertise networked execution.",
                EXECUTION_BACKEND_NETWORKED_WORKER_ROLLOUT_ENV
            ),
        )
    } else if matches!(networked_workers.mode, RuntimePreviewMode::Enabled)
        && !runtime_rollout.enabled
    {
        (
            ExecutionBackendState::Disabled,
            false,
            "Networked workers runtime is pinned to enabled mode, but its dedicated rollout flag is still off."
                .to_owned(),
        )
    } else if worker_snapshot.attested_workers > 0 {
        (
            ExecutionBackendState::Available,
            true,
            format!(
                "{} attested worker(s) are registered with proxy-bound egress and ephemeral lease support.",
                worker_snapshot.attested_workers
            ),
        )
    } else if worker_snapshot.registered_workers > 0 {
        (
            ExecutionBackendState::Degraded,
            false,
            format!(
                "{} worker(s) registered, but none passed attestation requirements for execution.",
                worker_snapshot.registered_workers
            ),
        )
    } else {
        (
            ExecutionBackendState::Degraded,
            false,
            "Preview backend is enabled, but no attested worker has registered yet.".to_owned(),
        )
    };
    ExecutionBackendInventoryRecord {
        backend_id: ExecutionBackendPreference::NetworkedWorker.as_str().to_owned(),
        label: ExecutionBackendPreference::NetworkedWorker.label().to_owned(),
        state,
        selectable,
        selected_by_default: false,
        description: ExecutionBackendPreference::NetworkedWorker.description().to_owned(),
        operator_summary,
        executor_label: Some("networked_worker".to_owned()),
        rollout_flag: Some(EXECUTION_BACKEND_NETWORKED_WORKER_ROLLOUT_ENV.to_owned()),
        rollout_source: Some(rollout.source),
        rollout_enabled: rollout.enabled,
        capabilities: vec![
            "attested_remote_execution".to_owned(),
            "proxy_mediated_egress".to_owned(),
            "scoped_artifact_transport".to_owned(),
        ],
        tradeoffs: vec![
            "Requires explicit worker attestation plus cleanup verification before use".to_owned(),
            format!(
                "Worker leases stay ephemeral with ttl<={}ms and fail closed on cleanup gaps",
                worker_policy.max_ttl_ms
            ),
        ],
        requires_attestation: true,
        requires_egress_proxy: worker_policy.attestation.require_egress_proxy,
        attestation_mode: BackendAttestationMode::WorkerLease,
        workspace_strategy: WorkspaceStrategyDescriptor::remote_lease_workspace(),
        workspace_scope_mode: "ephemeral_scoped_mount".to_owned(),
        artifact_transport: "manifest_attested_bundle_transfer".to_owned(),
        cleanup_strategy: "lease_ttl_reap_with_fail_closed_cleanup".to_owned(),
        supports_cancellation: true,
        supports_cleanup: true,
        health_probe: "worker_lease_heartbeat_and_cleanup_snapshot".to_owned(),
        active_node_count: worker_snapshot.attested_workers,
        total_node_count: worker_snapshot.registered_workers,
    }
}

fn ssh_tunnel_inventory_record(rollout: FeatureRolloutSetting) -> ExecutionBackendInventoryRecord {
    ExecutionBackendInventoryRecord {
        backend_id: ExecutionBackendPreference::SshTunnel.as_str().to_owned(),
        label: ExecutionBackendPreference::SshTunnel.label().to_owned(),
        state: if rollout.enabled {
            ExecutionBackendState::Available
        } else {
            ExecutionBackendState::Disabled
        },
        selectable: rollout.enabled,
        selected_by_default: false,
        description: ExecutionBackendPreference::SshTunnel.description().to_owned(),
        operator_summary: if rollout.enabled {
            "Preview backend is enabled. Operators must still establish an explicit SSH forward before relying on remote control-plane flows."
                .to_owned()
        } else {
            format!(
                "Preview backend is disabled. Set {}=1 before advertising SSH tunnel workflows.",
                EXECUTION_BACKEND_SSH_TUNNEL_ROLLOUT_ENV
            )
        },
        executor_label: None,
        rollout_flag: Some(EXECUTION_BACKEND_SSH_TUNNEL_ROLLOUT_ENV.to_owned()),
        rollout_source: Some(rollout.source),
        rollout_enabled: rollout.enabled,
        capabilities: vec![
            "verified_remote_dashboard_access".to_owned(),
            "operator_handoff".to_owned(),
        ],
        tradeoffs: vec![
            "Useful for explicit remote operator access and controlled handoff".to_owned(),
            "Requires manual tunnel setup and does not replace sandbox or node trust boundaries"
                .to_owned(),
        ],
        requires_attestation: false,
        requires_egress_proxy: false,
        attestation_mode: BackendAttestationMode::VaultIdentity,
        workspace_strategy: WorkspaceStrategyDescriptor::operator_managed_remote(),
        workspace_scope_mode: "operator_managed_remote_scope".to_owned(),
        artifact_transport: "out_of_band_operator_tunnel".to_owned(),
        cleanup_strategy: "operator_managed_tunnel_teardown".to_owned(),
        supports_cancellation: false,
        supports_cleanup: false,
        health_probe: "operator_tunnel_connectivity_check".to_owned(),
        active_node_count: 0,
        total_node_count: 0,
    }
}

// Union of available capabilities across healthy nodes, deduplicated and
// capped at 6 entries to keep the operator-facing inventory readable; the
// placeholder keeps the backend describable when no node reported anything.
fn aggregate_node_capabilities(nodes: &[&RegisteredNodeRecord]) -> Vec<String> {
    let mut capabilities = BTreeSet::<String>::new();
    for node in nodes {
        for capability in &node.capabilities {
            if capability.available {
                capabilities.insert(capability.name.clone());
            }
        }
    }
    if capabilities.is_empty() {
        return vec!["paired_desktop_capabilities".to_owned()];
    }
    capabilities.into_iter().take(6).collect()
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        sync::{Arc, Mutex},
    };

    use palyra_common::feature_rollouts::FeatureRolloutSource;
    use palyra_common::runtime_preview::RuntimePreviewMode;
    use palyra_workerd::{WorkerFleetPolicy, WorkerFleetSnapshot};

    use crate::config::{
        ExecutionBackendContainerEnvBindingConfig, ExecutionBackendContainerProfileConfig,
        ExecutionBackendContainerResourceLimitsConfig,
        ExecutionBackendContainerWorkspaceMountConfig, ExecutionBackendProfileConfig,
        ExecutionBackendProfilesConfig, NetworkedWorkersConfig,
    };
    use crate::journal::{ToolJobRecord, ToolJobState};
    use crate::sandbox_runner::{
        process_runner_executor_name, process_runner_sandbox_enforcement_label,
        EgressEnforcementMode, SandboxProcessRunnerPolicy, SandboxProcessRunnerTier,
    };
    use crate::tool_protocol::ToolCallConfig;
    use crate::wasm_plugin_runner::WasmPluginRunnerPolicy;

    use super::{
        apply_docker_cli_preflight_probe, build_execution_backend_inventory_with_docker_rollout,
        build_execution_backend_inventory_with_rollout, build_execution_backend_preflight_report,
        parse_execution_backend_preference, plan_stuck_tool_job_recovery,
        resolve_execution_backend, resolve_execution_backend_for_request,
        validate_execution_backend_selection, ContainerBackendProfile, ContainerEnvBinding,
        ContainerEnvSourceKind, ContainerMountPolicy, ContainerNetworkPolicy,
        ContainerResourceLimits, ContainerRuntimeKind, DockerCleanupAttestation, DockerCliEngine,
        DockerEngine, DockerEngineError, DockerEngineFuture, DockerPatchBundle,
        DockerResourceUsage, DockerRunPlan, DockerRunReport, DockerRunner, ExecutionBackend,
        ExecutionBackendHealthStatus, ExecutionBackendPreference,
        ExecutionBackendProcessRunRequest, ExecutionBackendResolutionRequest,
        ExecutionBackendRunner, ExecutionBackendRunnerCapability, ExecutionBackendRunnerRegistry,
        ExecutionBackendState, FeatureRolloutSetting, SshWorkerBackendProfile,
        StuckToolJobRecoveryAction, WorkspaceStrategyDescriptor, WorkspaceStrategyKind,
        WorkspaceWritebackMode,
    };

    const SAFE_DOCKER_IMAGE: &str =
        "ghcr.io/palyra/worker@sha256:1111111111111111111111111111111111111111111111111111111111111111";

    fn test_policy() -> SandboxProcessRunnerPolicy {
        SandboxProcessRunnerPolicy {
            enabled: true,
            tier: SandboxProcessRunnerTier::C,
            workspace_root: PathBuf::from("."),
            path_access_mode: crate::sandbox_runner::PathAccessMode::WorkspaceOnly,
            allowed_executables: vec!["cargo".to_owned()],
            allow_interpreters: false,
            egress_enforcement_mode: EgressEnforcementMode::Preflight,
            allowed_egress_hosts: Vec::new(),
            allowed_dns_suffixes: Vec::new(),
            cpu_time_limit_ms: 1_000,
            memory_limit_bytes: 1_048_576,
            max_output_bytes: 1_048_576,
        }
    }

    fn test_wasm_policy() -> WasmPluginRunnerPolicy {
        WasmPluginRunnerPolicy {
            enabled: false,
            allow_inline_modules: false,
            max_module_size_bytes: 256 * 1024,
            fuel_budget: 10_000_000,
            max_memory_bytes: 64 * 1024 * 1024,
            max_table_elements: 100_000,
            max_instances: 256,
            allowed_http_hosts: Vec::new(),
            allowed_secrets: Vec::new(),
            allowed_storage_prefixes: Vec::new(),
            allowed_channels: Vec::new(),
        }
    }

    fn test_tool_call_config(process_runner: SandboxProcessRunnerPolicy) -> ToolCallConfig {
        ToolCallConfig {
            allowed_tools: vec!["palyra.process.run".to_owned()],
            max_calls_per_run: 10,
            execution_timeout_ms: 1_000,
            process_runner,
            wasm_runtime: test_wasm_policy(),
        }
    }

    fn safe_container_profile() -> ContainerBackendProfile {
        ContainerBackendProfile {
            profile_id: "docker-safe".to_owned(),
            runtime: ContainerRuntimeKind::Docker,
            image: SAFE_DOCKER_IMAGE.to_owned(),
            mounts: vec![ContainerMountPolicy {
                host_path: "workspace".to_owned(),
                container_path: "/workspace".to_owned(),
                read_only: false,
                workspace_scoped: true,
            }],
            network: ContainerNetworkPolicy::None,
            user: "1000:1000".to_owned(),
            readonly_rootfs: true,
            privileged: false,
            limits: ContainerResourceLimits {
                cpu_time_limit_ms: 1_000,
                memory_limit_bytes: 128 * 1024 * 1024,
                max_output_bytes: 64 * 1024,
            },
            env: Vec::new(),
            cleanup_strategy: "remove_container_and_volume".to_owned(),
        }
    }

    fn safe_container_profile_config(
        id: &str,
        workspace_mount_read_only: bool,
    ) -> ExecutionBackendProfileConfig {
        ExecutionBackendProfileConfig {
            id: id.to_owned(),
            enabled: true,
            kind: "docker".to_owned(),
            container: Some(ExecutionBackendContainerProfileConfig {
                image: SAFE_DOCKER_IMAGE.to_owned(),
                user: "1000:1000".to_owned(),
                network: "none".to_owned(),
                readonly_rootfs: true,
                privileged: false,
                workspace_mount: ExecutionBackendContainerWorkspaceMountConfig {
                    host_path: "workspace".to_owned(),
                    container_path: "/workspace".to_owned(),
                    read_only: workspace_mount_read_only,
                },
                resource_limits: ExecutionBackendContainerResourceLimitsConfig {
                    cpu_time_limit_ms: 1_000,
                    memory_limit_bytes: 128 * 1024 * 1024,
                    max_output_bytes: 64 * 1024,
                },
                env: vec![ExecutionBackendContainerEnvBindingConfig {
                    name: "API_TOKEN".to_owned(),
                    source_kind: "vault_ref".to_owned(),
                    value: "vault://worker/api-token".to_owned(),
                }],
                cleanup_strategy: "remove_container_and_volume".to_owned(),
            }),
        }
    }

    fn docker_report_success() -> DockerRunReport {
        DockerRunReport {
            exit_code: 0,
            stdout: b"runner-ok\n".to_vec(),
            stderr: Vec::new(),
            resource_usage: DockerResourceUsage {
                duration_ms: 42,
                memory_limit_bytes: 128 * 1024 * 1024,
                cpu_time_limit_ms: 1_000,
            },
            cleanup: DockerCleanupAttestation {
                strategy: "remove_container_and_volume".to_owned(),
                container_removed: true,
                volume_removed: true,
                success: true,
                reason_code: "docker.cleanup.ok".to_owned(),
            },
            patch_bundle: None,
        }
    }

    #[derive(Debug, Clone)]
    struct FakeDockerEngine {
        result: Result<DockerRunReport, DockerEngineError>,
        plans: Arc<Mutex<Vec<DockerRunPlan>>>,
    }

    impl FakeDockerEngine {
        fn new(
            result: Result<DockerRunReport, DockerEngineError>,
        ) -> (Self, Arc<Mutex<Vec<DockerRunPlan>>>) {
            let plans = Arc::new(Mutex::new(Vec::new()));
            (Self { result, plans: Arc::clone(&plans) }, plans)
        }
    }

    impl DockerEngine for FakeDockerEngine {
        fn run<'a>(&'a self, plan: DockerRunPlan) -> DockerEngineFuture<'a> {
            let result = self.result.clone();
            let plans = Arc::clone(&self.plans);
            Box::pin(async move {
                plans.lock().expect("fake docker plan lock").push(plan);
                result
            })
        }
    }

    fn test_tool_job(state: ToolJobState, backend: ExecutionBackendPreference) -> ToolJobRecord {
        ToolJobRecord {
            job_id: "job-1".to_owned(),
            owner_principal: "user:ops".to_owned(),
            device_id: "device:local".to_owned(),
            channel: Some("cli".to_owned()),
            session_id: "session-1".to_owned(),
            run_id: "run-1".to_owned(),
            tool_call_id: "call-1".to_owned(),
            tool_name: "palyra.process.run".to_owned(),
            backend: backend.as_str().to_owned(),
            backend_reason_code: Some("backend.test".to_owned()),
            command_sha256: "sha256-command".to_owned(),
            program_sha256: None,
            state,
            attempt_count: 1,
            max_attempts: 1,
            retry_allowed: false,
            idempotency_key: None,
            cancellation_handle: Some("cancel:job-1".to_owned()),
            artifact_refs_json: None,
            tail_preview: String::new(),
            stdout_artifact_id: None,
            stderr_artifact_id: None,
            last_error: None,
            state_reason: None,
            created_at_unix_ms: 1_000,
            updated_at_unix_ms: 2_000,
            started_at_unix_ms: Some(1_500),
            heartbeat_at_unix_ms: Some(2_000),
            completed_at_unix_ms: None,
            expires_at_unix_ms: None,
            legal_hold: false,
            active_ref_count: 0,
            lease_expires_at_unix_ms: Some(2_500),
        }
    }

    #[test]
    fn automatic_resolution_prefers_local_sandbox() {
        let networked_workers = NetworkedWorkersConfig::default();
        let inventory = build_execution_backend_inventory_with_rollout(
            &test_policy(),
            0,
            &[],
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            &networked_workers,
            WorkerFleetSnapshot::default(),
            &WorkerFleetPolicy::default(),
        );
        let resolution =
            resolve_execution_backend(ExecutionBackendPreference::Automatic, &inventory);
        assert_eq!(resolution.resolved, ExecutionBackendPreference::LocalSandbox);
        assert!(!resolution.fallback_used);
    }

    #[test]
    fn runner_registry_selects_local_sandbox_runner() {
        let registry = ExecutionBackendRunnerRegistry::default();

        let runner = registry
            .select_runner(
                ExecutionBackendPreference::LocalSandbox,
                ExecutionBackendRunnerCapability::RunProcess,
            )
            .expect("local sandbox runner should implement process runs");
        assert_eq!(runner.runner_id(), "local_sandbox_runner");
        assert!(runner.capabilities().contains(&ExecutionBackendRunnerCapability::RunProcess));

        let selection = registry.selection_event(
            ExecutionBackendPreference::Automatic,
            ExecutionBackendPreference::LocalSandbox,
        );
        assert_eq!(selection.event, "execution_backend.runner_selected");
        assert_eq!(selection.requested_backend, "automatic");
        assert_eq!(selection.resolved_backend, "local_sandbox");
        assert!(selection.capabilities.iter().any(|capability| capability == "run_process"));
    }

    #[test]
    fn runner_registry_denies_unregistered_backend_without_local_fallback() {
        let registry = ExecutionBackendRunnerRegistry::default();
        let error = match registry.select_runner(
            ExecutionBackendPreference::SshTunnel,
            ExecutionBackendRunnerCapability::OpenWorkspace,
        ) {
            Ok(_) => panic!("ssh tunnel must not fall back to local execution"),
            Err(error) => error,
        };

        let outcome = error.to_tool_execution_outcome("proposal-ssh", "palyra.fs.read_file", b"{}");
        assert!(!outcome.success);
        assert!(outcome.error.contains("local fallback is denied"));
        assert_eq!(outcome.attestation.executor, "ssh_tunnel");
        assert_eq!(outcome.attestation.sandbox_enforcement, "runner_selection");

        let payload: serde_json::Value =
            serde_json::from_slice(&outcome.output_json).expect("selection payload is valid JSON");
        assert_eq!(payload["event"], "execution_backend.runner_selection");
        assert_eq!(payload["status"], "unavailable");
        assert_eq!(payload["backend"], "ssh_tunnel");
        assert_eq!(payload["required_capability"], "open_workspace");
        assert_eq!(payload["reason_code"], "runner.unavailable.ssh_tunnel");
    }

    #[test]
    fn runner_registry_selects_configured_docker_runner() {
        let (engine, _) = FakeDockerEngine::new(Ok(docker_report_success()));
        let docker = DockerRunner::new(safe_container_profile(), engine)
            .expect("safe Docker profile should build runner");
        let registry = ExecutionBackendRunnerRegistry::with_docker_runner(Box::new(docker));

        let runner = registry
            .select_runner(
                ExecutionBackendPreference::Docker,
                ExecutionBackendRunnerCapability::RunProcess,
            )
            .expect("configured Docker runner should be selectable");
        assert_eq!(runner.runner_id(), "docker_runner");

        let selection = registry.selection_event(
            ExecutionBackendPreference::Docker,
            ExecutionBackendPreference::Docker,
        );
        assert_eq!(selection.event, "execution_backend.runner_selected");
        assert_eq!(selection.resolved_backend, "docker");
        assert!(selection.capabilities.iter().any(|capability| capability == "run_process"));
    }

    #[test]
    fn runner_registry_builds_docker_runner_from_profile_config() {
        let profiles = ExecutionBackendProfilesConfig {
            mode: RuntimePreviewMode::PreviewOnly,
            profiles: vec![safe_container_profile_config("docker-safe", true)],
        };

        let registry = ExecutionBackendRunnerRegistry::from_execution_backend_profiles(&profiles)
            .expect("valid Docker profile should build a registry");

        let runner = registry
            .select_runner(
                ExecutionBackendPreference::Docker,
                ExecutionBackendRunnerCapability::RunProcess,
            )
            .expect("configured Docker runner should be selectable");
        assert_eq!(runner.runner_id(), "docker_runner");
    }

    #[test]
    fn runner_registry_rejects_multiple_enabled_docker_profiles() {
        let profiles = ExecutionBackendProfilesConfig {
            mode: RuntimePreviewMode::PreviewOnly,
            profiles: vec![
                safe_container_profile_config("docker-a", true),
                safe_container_profile_config("docker-b", true),
            ],
        };

        let error = ExecutionBackendRunnerRegistry::from_execution_backend_profiles(&profiles)
            .expect_err("multiple enabled Docker profiles must fail closed");

        assert!(error.contains("at most one Docker profile"), "{error}");
    }

    #[tokio::test]
    async fn docker_cli_engine_rejects_writable_mount_without_patch_capture() {
        let profile = safe_container_profile();
        let plan = DockerRunPlan {
            profile_id: profile.profile_id,
            image: profile.image,
            image_digest_sha256: "1111111111111111111111111111111111111111111111111111111111111111"
                .to_owned(),
            user: profile.user,
            readonly_rootfs: profile.readonly_rootfs,
            network: profile.network,
            mounts: profile.mounts,
            env: profile.env,
            command: "echo".to_owned(),
            args: vec!["runner-ok".to_owned()],
            working_dir: "/workspace".to_owned(),
            limits: profile.limits,
            workspace_writeback: WorkspaceWritebackMode::PatchBundle,
            cleanup_strategy: profile.cleanup_strategy,
        };

        let error = DockerCliEngine
            .run(plan)
            .await
            .expect_err("writable workspace mounts require patch capture before Docker CLI launch");

        assert_eq!(error.reason_code, "docker.writeback.capture_unavailable");
    }

    #[tokio::test]
    async fn local_sandbox_runner_preserves_process_run_schema() {
        let mut policy = test_policy();
        policy.allowed_executables = vec!["echo".to_owned()];
        let config = test_tool_call_config(policy);
        let registry = ExecutionBackendRunnerRegistry::default();
        let runner = registry
            .select_runner(
                ExecutionBackendPreference::LocalSandbox,
                ExecutionBackendRunnerCapability::RunProcess,
            )
            .expect("local sandbox runner should implement process runs");

        let outcome = runner
            .run_process(ExecutionBackendProcessRunRequest {
                config: &config,
                proposal_id: "proposal-local-process",
                tool_name: "palyra.process.run",
                input_json: br#"{"command":"echo","args":["runner-ok"]}"#,
                cancellation_requested: None,
                process_progress_sink: None,
            })
            .await;

        assert!(outcome.success, "{}", outcome.error);
        let payload: serde_json::Value =
            serde_json::from_slice(&outcome.output_json).expect("process output is valid JSON");
        assert_eq!(payload["schema_version"], 2);
        assert_eq!(payload["exit_code"], 0);
        assert!(payload["stdout"].as_str().unwrap_or_default().contains("runner-ok"));
        assert_eq!(
            outcome.attestation.executor,
            process_runner_executor_name(&config.process_runner)
        );
        assert_eq!(
            outcome.attestation.sandbox_enforcement,
            process_runner_sandbox_enforcement_label(&config.process_runner)
        );
    }

    #[test]
    fn docker_profile_requires_digest_non_root_and_readonly_rootfs() {
        let profile = safe_container_profile();
        assert!(profile.validate().is_ok());

        let mut missing_digest = safe_container_profile();
        missing_digest.image = "ubuntu:latest".to_owned();
        assert!(missing_digest
            .validate()
            .expect_err("Docker image tags without digests must fail")
            .contains("sha256 digest"));

        let mut root_user = safe_container_profile();
        root_user.user = "0:0".to_owned();
        assert!(root_user.validate().expect_err("root user must fail").contains("non-root user"));

        let mut writable_root = safe_container_profile();
        writable_root.readonly_rootfs = false;
        assert!(writable_root
            .validate()
            .expect_err("writable rootfs must fail")
            .contains("read-only"));
    }

    #[tokio::test]
    async fn docker_runner_fake_process_run_matches_local_output_schema() {
        let (engine, plans) = FakeDockerEngine::new(Ok(docker_report_success()));
        let runner = DockerRunner::new(safe_container_profile(), engine)
            .expect("safe Docker profile should build runner");
        let mut policy = test_policy();
        policy.allowed_executables = vec!["echo".to_owned()];
        let config = test_tool_call_config(policy);

        let outcome = runner
            .run_process(ExecutionBackendProcessRunRequest {
                config: &config,
                proposal_id: "proposal-docker-process",
                tool_name: "palyra.process.run",
                input_json: br#"{"command":"echo","args":["runner-ok"],"cwd":"workspace/subdir"}"#,
                cancellation_requested: None,
                process_progress_sink: None,
            })
            .await;

        assert!(outcome.success, "{}", outcome.error);
        assert_eq!(outcome.attestation.executor, "docker");
        assert_eq!(outcome.attestation.sandbox_enforcement, "container_profile");
        let payload: serde_json::Value =
            serde_json::from_slice(&outcome.output_json).expect("Docker output should be JSON");
        assert_eq!(payload["schema_version"], 2);
        assert_eq!(payload["exit_code"], 0);
        assert!(payload["stdout"].as_str().unwrap_or_default().contains("runner-ok"));
        assert_eq!(payload["workspace_writeback"]["mode"], "patch_bundle");
        assert_eq!(payload["workspace_writeback"]["authoritative_workspace_mutation"], false);
        assert!(payload["output_manifest_sha256"].as_str().is_some_and(|hash| hash.len() == 64));

        let plans = plans.lock().expect("fake Docker plans");
        assert_eq!(plans.len(), 1);
        let plan = &plans[0];
        assert_eq!(plan.command, "echo");
        assert_eq!(plan.args, vec!["runner-ok"]);
        assert_eq!(plan.working_dir, "/workspace/subdir");
        assert!(plan.readonly_rootfs);
        assert_eq!(plan.network, ContainerNetworkPolicy::None);
        assert_eq!(plan.workspace_writeback, WorkspaceWritebackMode::PatchBundle);
        assert_eq!(
            plan.image_digest_sha256,
            "1111111111111111111111111111111111111111111111111111111111111111"
        );
    }

    #[tokio::test]
    async fn docker_runner_output_carries_reviewed_patch_bundle_writeback() {
        let mut report = docker_report_success();
        report.patch_bundle = Some(DockerPatchBundle {
            schema_version: 1,
            reviewed: true,
            patch_sha256: "2222222222222222222222222222222222222222222222222222222222222222"
                .to_owned(),
            file_count: 2,
        });
        let (engine, _) = FakeDockerEngine::new(Ok(report));
        let runner = DockerRunner::new(safe_container_profile(), engine)
            .expect("safe Docker profile should build runner");
        let mut policy = test_policy();
        policy.allowed_executables = vec!["echo".to_owned()];
        let config = test_tool_call_config(policy);

        let outcome = runner
            .run_process(ExecutionBackendProcessRunRequest {
                config: &config,
                proposal_id: "proposal-docker-patch",
                tool_name: "palyra.process.run",
                input_json: br#"{"command":"echo","args":["runner-ok"]}"#,
                cancellation_requested: None,
                process_progress_sink: None,
            })
            .await;

        assert!(outcome.success, "{}", outcome.error);
        let payload: serde_json::Value =
            serde_json::from_slice(&outcome.output_json).expect("Docker output should be JSON");
        assert_eq!(
            payload["workspace_writeback"]["patch_bundle"]["patch_sha256"],
            "2222222222222222222222222222222222222222222222222222222222222222"
        );
        assert_eq!(payload["workspace_writeback"]["patch_bundle"]["reviewed"], true);
        assert_eq!(payload["workspace_writeback"]["patch_bundle"]["file_count"], 2);
    }

    #[tokio::test]
    async fn docker_runner_cleanup_failure_is_fail_closed() {
        let mut report = docker_report_success();
        report.cleanup.success = false;
        report.cleanup.reason_code = "docker.cleanup.remove_failed".to_owned();
        report.cleanup.container_removed = false;
        let (engine, _) = FakeDockerEngine::new(Ok(report));
        let runner = DockerRunner::new(safe_container_profile(), engine)
            .expect("safe Docker profile should build runner");
        let mut policy = test_policy();
        policy.allowed_executables = vec!["echo".to_owned()];
        let config = test_tool_call_config(policy);

        let outcome = runner
            .run_process(ExecutionBackendProcessRunRequest {
                config: &config,
                proposal_id: "proposal-docker-cleanup",
                tool_name: "palyra.process.run",
                input_json: br#"{"command":"echo","args":["runner-ok"]}"#,
                cancellation_requested: None,
                process_progress_sink: None,
            })
            .await;

        assert!(!outcome.success);
        assert!(outcome.error.contains("cleanup failed"));
        let payload: serde_json::Value =
            serde_json::from_slice(&outcome.output_json).expect("Docker output should be JSON");
        assert_eq!(payload["cleanup"]["success"], false);
        assert_eq!(payload["cleanup"]["reason_code"], "docker.cleanup.remove_failed");
    }

    #[test]
    fn preview_backend_selection_rejects_disabled_rollout() {
        let networked_workers = NetworkedWorkersConfig::default();
        let inventory = build_execution_backend_inventory_with_rollout(
            &test_policy(),
            0,
            &[],
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            &networked_workers,
            WorkerFleetSnapshot::default(),
            &WorkerFleetPolicy::default(),
        );
        let error = validate_execution_backend_selection(
            ExecutionBackendPreference::DesktopNode,
            &inventory,
        )
        .expect_err("disabled preview backend should be rejected");
        assert!(error.contains("desktop_node"), "unexpected error: {error}");
    }

    #[test]
    fn preview_backend_resolution_falls_back_to_local_sandbox() {
        let networked_workers = NetworkedWorkersConfig::default();
        let inventory = build_execution_backend_inventory_with_rollout(
            &test_policy(),
            0,
            &[],
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::from_config(true),
            &networked_workers,
            WorkerFleetSnapshot::default(),
            &WorkerFleetPolicy::default(),
        );
        let resolution =
            resolve_execution_backend(ExecutionBackendPreference::DesktopNode, &inventory);
        assert_eq!(resolution.resolved, ExecutionBackendPreference::LocalSandbox);
        assert!(resolution.fallback_used);
    }

    #[test]
    fn networked_worker_resolution_denies_local_fallback_without_attestation() {
        let networked_workers = NetworkedWorkersConfig::default();
        let inventory = build_execution_backend_inventory_with_rollout(
            &test_policy(),
            0,
            &[],
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            &networked_workers,
            WorkerFleetSnapshot::default(),
            &WorkerFleetPolicy::default(),
        );
        let resolution =
            resolve_execution_backend(ExecutionBackendPreference::NetworkedWorker, &inventory);

        assert_eq!(resolution.resolved, ExecutionBackendPreference::NetworkedWorker);
        assert!(!resolution.fallback_used);
        assert_eq!(resolution.reason_code, "backend.unavailable.networked_worker");
        assert!(resolution.reason.contains("local fallback is denied"));
    }

    #[test]
    fn docker_backend_parser_accepts_container_aliases() {
        for raw in ["docker", "container", "container_sandbox"] {
            let parsed = parse_execution_backend_preference(raw, "execution_backend")
                .expect("docker alias should parse");
            assert_eq!(parsed, ExecutionBackendPreference::Docker);
        }
    }

    #[test]
    fn docker_inventory_requires_rollout_and_exposes_patch_bundle_contract() {
        let networked_workers = NetworkedWorkersConfig::default();
        let disabled_inventory = build_execution_backend_inventory_with_docker_rollout(
            &test_policy(),
            0,
            &[],
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            &networked_workers,
            WorkerFleetSnapshot::default(),
            &WorkerFleetPolicy::default(),
        );
        let disabled = disabled_inventory
            .iter()
            .find(|entry| entry.backend_id == ExecutionBackendPreference::Docker.as_str())
            .expect("docker backend should exist");
        assert_eq!(disabled.state, ExecutionBackendState::Disabled);
        assert!(!disabled.selectable);

        let enabled_inventory = build_execution_backend_inventory_with_docker_rollout(
            &test_policy(),
            0,
            &[],
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::from_config(true),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            &networked_workers,
            WorkerFleetSnapshot::default(),
            &WorkerFleetPolicy::default(),
        );
        let docker = enabled_inventory
            .iter()
            .find(|entry| entry.backend_id == ExecutionBackendPreference::Docker.as_str())
            .expect("docker backend should exist");
        assert_eq!(docker.state, ExecutionBackendState::Available);
        assert!(docker.selectable);
        assert_eq!(docker.workspace_strategy.kind, WorkspaceStrategyKind::ContainerVolume);
        assert_eq!(docker.workspace_strategy.writeback, WorkspaceWritebackMode::PatchBundle);
        assert_eq!(docker.artifact_transport, "container_patch_bundle_transfer");
        assert!(docker.requires_attestation);
        assert!(docker.requires_egress_proxy);
    }

    #[test]
    fn docker_resolution_denies_local_fallback_when_unavailable() {
        let networked_workers = NetworkedWorkersConfig::default();
        let inventory = build_execution_backend_inventory_with_docker_rollout(
            &test_policy(),
            0,
            &[],
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            &networked_workers,
            WorkerFleetSnapshot::default(),
            &WorkerFleetPolicy::default(),
        );
        let resolution = resolve_execution_backend(ExecutionBackendPreference::Docker, &inventory);

        assert_eq!(resolution.resolved, ExecutionBackendPreference::Docker);
        assert!(!resolution.fallback_used);
        assert_eq!(resolution.reason_code, "backend.unavailable.docker");
        assert!(resolution.reason.contains("local fallback is denied"));
    }

    #[test]
    fn docker_preflight_reports_missing_cli_with_repair_hint() {
        let networked_workers = NetworkedWorkersConfig::default();
        let inventory = build_execution_backend_inventory_with_docker_rollout(
            &test_policy(),
            0,
            &[],
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::from_config(true),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            &networked_workers,
            WorkerFleetSnapshot::default(),
            &WorkerFleetPolicy::default(),
        );
        let docker = inventory
            .iter()
            .find(|entry| entry.backend_id == ExecutionBackendPreference::Docker.as_str())
            .expect("docker backend should exist");
        let mut record = docker.preflight(
            &ExecutionBackendResolutionRequest {
                preference: ExecutionBackendPreference::Docker,
                required_capabilities: vec!["containerized_execution".to_owned()],
                workspace_strategy: Some(WorkspaceStrategyKind::ContainerVolume),
            },
            42_000,
        );

        apply_docker_cli_preflight_probe(&mut record, docker, false);

        assert_eq!(record.status, ExecutionBackendHealthStatus::Unavailable);
        assert_eq!(record.reason_code, "backend.preflight.docker_unavailable");
        assert!(record.repair_hint.as_deref().is_some_and(|hint| hint.contains("Docker CLI")));
    }

    #[test]
    fn preview_backend_inventory_is_degraded_without_healthy_nodes() {
        let networked_workers = NetworkedWorkersConfig::default();
        let inventory = build_execution_backend_inventory_with_rollout(
            &test_policy(),
            1,
            &[],
            FeatureRolloutSetting::from_config(true),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            &networked_workers,
            WorkerFleetSnapshot::default(),
            &WorkerFleetPolicy::default(),
        );
        let desktop_node = inventory
            .iter()
            .find(|entry| entry.backend_id == ExecutionBackendPreference::DesktopNode.as_str())
            .expect("desktop node backend should exist");
        assert_eq!(desktop_node.state, ExecutionBackendState::Degraded);
        assert!(desktop_node.rollout_enabled);
        assert_eq!(desktop_node.rollout_source, Some(FeatureRolloutSource::Config));
        assert!(!desktop_node.selectable);
    }

    #[test]
    fn networked_worker_inventory_is_available_only_with_attested_workers() {
        let networked_workers = NetworkedWorkersConfig {
            mode: RuntimePreviewMode::PreviewOnly,
            ..NetworkedWorkersConfig::default()
        };
        let inventory = build_execution_backend_inventory_with_rollout(
            &test_policy(),
            0,
            &[],
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::from_config(true),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            &networked_workers,
            WorkerFleetSnapshot {
                registered_workers: 1,
                attested_workers: 1,
                active_leases: 0,
                ..WorkerFleetSnapshot::default()
            },
            &WorkerFleetPolicy::default(),
        );
        let networked_worker = inventory
            .iter()
            .find(|entry| entry.backend_id == ExecutionBackendPreference::NetworkedWorker.as_str())
            .expect("networked worker backend should exist");
        assert_eq!(networked_worker.state, ExecutionBackendState::Available);
        assert!(networked_worker.selectable);
        assert!(networked_worker.requires_attestation);
        assert!(networked_worker.requires_egress_proxy);
    }

    #[test]
    fn networked_worker_inventory_requires_runtime_rollout_when_enabled_mode_is_pinned() {
        let networked_workers = NetworkedWorkersConfig {
            mode: RuntimePreviewMode::Enabled,
            ..NetworkedWorkersConfig::default()
        };
        let inventory = build_execution_backend_inventory_with_rollout(
            &test_policy(),
            0,
            &[],
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::from_config(true),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            &networked_workers,
            WorkerFleetSnapshot {
                registered_workers: 1,
                attested_workers: 1,
                active_leases: 0,
                ..WorkerFleetSnapshot::default()
            },
            &WorkerFleetPolicy::default(),
        );
        let networked_worker = inventory
            .iter()
            .find(|entry| entry.backend_id == ExecutionBackendPreference::NetworkedWorker.as_str())
            .expect("networked worker backend should exist");
        assert_eq!(networked_worker.state, ExecutionBackendState::Disabled);
        assert!(!networked_worker.selectable);
        assert!(networked_worker.operator_summary.contains("dedicated rollout flag"));
    }

    #[test]
    fn backend_contract_exposes_workspace_attestation_and_cleanup_flags() {
        let networked_workers = NetworkedWorkersConfig {
            mode: RuntimePreviewMode::PreviewOnly,
            ..NetworkedWorkersConfig::default()
        };
        let inventory = build_execution_backend_inventory_with_rollout(
            &test_policy(),
            0,
            &[],
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::from_config(true),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            &networked_workers,
            WorkerFleetSnapshot {
                registered_workers: 1,
                attested_workers: 1,
                ..WorkerFleetSnapshot::default()
            },
            &WorkerFleetPolicy::default(),
        );
        let backend = inventory
            .iter()
            .find(|entry| entry.backend_id == ExecutionBackendPreference::NetworkedWorker.as_str())
            .expect("networked worker backend should exist");

        assert_eq!(backend.backend_id(), "networked_worker");
        assert!(backend.supports_cancellation());
        assert!(backend.supports_cleanup());
        assert_eq!(backend.workspace_strategy().kind, WorkspaceStrategyKind::RemoteLeaseWorkspace);
        assert!(!backend.workspace_strategy().attestation_digest_sha256().is_empty());
        assert_eq!(backend.artifact_transport(), "manifest_attested_bundle_transfer");
        assert_eq!(backend.cleanup_strategy(), "lease_ttl_reap_with_fail_closed_cleanup");
        assert_eq!(backend.health_probe(), "worker_lease_heartbeat_and_cleanup_snapshot");
        assert!(backend
            .capabilities()
            .iter()
            .any(|capability| capability == "scoped_artifact_transport"));
    }

    #[test]
    fn request_resolver_matches_capabilities_and_workspace_strategy() {
        let networked_workers = NetworkedWorkersConfig {
            mode: RuntimePreviewMode::PreviewOnly,
            ..NetworkedWorkersConfig::default()
        };
        let inventory = build_execution_backend_inventory_with_rollout(
            &test_policy(),
            0,
            &[],
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::from_config(true),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            &networked_workers,
            WorkerFleetSnapshot {
                registered_workers: 1,
                attested_workers: 1,
                ..WorkerFleetSnapshot::default()
            },
            &WorkerFleetPolicy::default(),
        );
        let resolution = resolve_execution_backend_for_request(
            &ExecutionBackendResolutionRequest {
                preference: ExecutionBackendPreference::Automatic,
                required_capabilities: vec!["scoped_artifact_transport".to_owned()],
                workspace_strategy: Some(WorkspaceStrategyKind::RemoteLeaseWorkspace),
            },
            &inventory,
        );

        assert_eq!(resolution.resolved, ExecutionBackendPreference::NetworkedWorker);
        assert_eq!(resolution.reason_code, "backend.available.networked_worker");
    }

    #[test]
    fn request_resolver_fails_closed_on_workspace_strategy_mismatch() {
        let networked_workers = NetworkedWorkersConfig::default();
        let inventory = build_execution_backend_inventory_with_rollout(
            &test_policy(),
            0,
            &[],
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            &networked_workers,
            WorkerFleetSnapshot::default(),
            &WorkerFleetPolicy::default(),
        );
        let resolution = resolve_execution_backend_for_request(
            &ExecutionBackendResolutionRequest {
                preference: ExecutionBackendPreference::LocalSandbox,
                required_capabilities: vec!["sandbox_process_runner".to_owned()],
                workspace_strategy: Some(WorkspaceStrategyKind::RemoteLeaseWorkspace),
            },
            &inventory,
        );

        assert_eq!(resolution.resolved, ExecutionBackendPreference::LocalSandbox);
        assert_eq!(resolution.reason_code, "backend.policy.unsatisfied.local_sandbox");
        assert!(resolution.reason.contains("workspace strategy"));
    }

    #[test]
    fn backend_preflight_reports_missing_capabilities_and_environment() {
        let networked_workers = NetworkedWorkersConfig {
            mode: RuntimePreviewMode::PreviewOnly,
            ..NetworkedWorkersConfig::default()
        };
        let inventory = build_execution_backend_inventory_with_rollout(
            &test_policy(),
            0,
            &[],
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::from_config(true),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            &networked_workers,
            WorkerFleetSnapshot {
                registered_workers: 1,
                attested_workers: 1,
                ..WorkerFleetSnapshot::default()
            },
            &WorkerFleetPolicy::default(),
        );
        let report = build_execution_backend_preflight_report(
            &inventory,
            &ExecutionBackendResolutionRequest {
                preference: ExecutionBackendPreference::Automatic,
                required_capabilities: vec!["scoped_artifact_transport".to_owned()],
                workspace_strategy: Some(WorkspaceStrategyKind::RemoteLeaseWorkspace),
            },
            42_000,
        );
        let worker = report
            .iter()
            .find(|entry| entry.backend_id == ExecutionBackendPreference::NetworkedWorker.as_str())
            .expect("worker preflight should exist");
        assert_eq!(worker.status, ExecutionBackendHealthStatus::Healthy);
        assert!(worker.environment.network_egress);
        assert!(worker.environment.persistent_workspace);

        let local = report
            .iter()
            .find(|entry| entry.backend_id == ExecutionBackendPreference::LocalSandbox.as_str())
            .expect("local preflight should exist");
        assert_eq!(local.status, ExecutionBackendHealthStatus::Unavailable);
        assert!(local.missing_capabilities.contains(&"scoped_artifact_transport".to_owned()));
    }

    #[test]
    fn stuck_tool_job_recovery_plans_attach_cancel_cleanup_and_repair() {
        let networked_workers = NetworkedWorkersConfig {
            mode: RuntimePreviewMode::PreviewOnly,
            ..NetworkedWorkersConfig::default()
        };
        let inventory = build_execution_backend_inventory_with_rollout(
            &test_policy(),
            0,
            &[],
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::from_config(true),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            &networked_workers,
            WorkerFleetSnapshot {
                registered_workers: 1,
                attested_workers: 1,
                ..WorkerFleetSnapshot::default()
            },
            &WorkerFleetPolicy::default(),
        );

        let running =
            test_tool_job(ToolJobState::Running, ExecutionBackendPreference::LocalSandbox);
        let plan = plan_stuck_tool_job_recovery(&running, &inventory, 10_000, 1_000)
            .expect("stale running job should plan recovery");
        assert_eq!(plan.action, StuckToolJobRecoveryAction::Attach);

        let cancelling =
            test_tool_job(ToolJobState::Cancelling, ExecutionBackendPreference::LocalSandbox);
        let plan = plan_stuck_tool_job_recovery(&cancelling, &inventory, 10_000, 1_000)
            .expect("stale cancelling job should plan cancellation");
        assert_eq!(plan.action, StuckToolJobRecoveryAction::Cancel);

        let orphaned =
            test_tool_job(ToolJobState::Orphaned, ExecutionBackendPreference::NetworkedWorker);
        let plan = plan_stuck_tool_job_recovery(&orphaned, &inventory, 10_000, 1_000)
            .expect("orphaned job should plan cleanup");
        assert_eq!(plan.action, StuckToolJobRecoveryAction::Cleanup);

        let unknown = test_tool_job(ToolJobState::Running, ExecutionBackendPreference::SshTunnel);
        let disabled_inventory = build_execution_backend_inventory_with_rollout(
            &test_policy(),
            0,
            &[],
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            &NetworkedWorkersConfig::default(),
            WorkerFleetSnapshot::default(),
            &WorkerFleetPolicy::default(),
        );
        let plan = plan_stuck_tool_job_recovery(&unknown, &disabled_inventory, 10_000, 1_000)
            .expect("disabled backend should require repair");
        assert_eq!(plan.action, StuckToolJobRecoveryAction::RepairRequired);
    }

    #[test]
    fn container_backend_profile_rejects_privileged_and_plaintext_secret_env() {
        let mut profile = safe_container_profile();
        profile.network = ContainerNetworkPolicy::EgressProxy;
        profile.env = vec![ContainerEnvBinding {
            name: "API_TOKEN".to_owned(),
            source_kind: ContainerEnvSourceKind::VaultRef,
            value: "vault://worker/api-token".to_owned(),
        }];
        assert!(profile.validate().is_ok());

        profile.privileged = true;
        assert!(profile
            .validate()
            .expect_err("privileged containers must fail closed")
            .contains("privileged"));
        profile.privileged = false;
        profile.env[0].source_kind = ContainerEnvSourceKind::LiteralSafeValue;
        profile.env[0].value = "raw-secret".to_owned();
        assert!(profile.validate().expect_err("secret env must use Vault").contains("Vault"));
    }

    #[test]
    fn ssh_worker_profile_requires_vault_identity_handles_and_worker_rpc() {
        let mut profile = SshWorkerBackendProfile {
            profile_id: "ssh-worker".to_owned(),
            host_handle: "vault://ssh/host".to_owned(),
            user_handle: "identity://ssh/user".to_owned(),
            identity_handle: "vault://ssh/key".to_owned(),
            host_trust_handle: "vault://ssh/known-host".to_owned(),
            worker_protocol: "palyra-worker-rpc/v1".to_owned(),
            workspace_strategy: WorkspaceStrategyDescriptor::remote_lease_workspace(),
        };
        assert!(profile.validate().is_ok());

        profile.identity_handle = "-----BEGIN PRIVATE KEY-----".to_owned();
        assert!(profile
            .validate()
            .expect_err("plaintext identity material must fail closed")
            .contains("identity_handle"));
        profile.identity_handle = "vault://ssh/key".to_owned();
        profile.worker_protocol = "raw-shell".to_owned();
        assert!(profile
            .validate()
            .expect_err("raw shell protocol must fail closed")
            .contains("worker-rpc"));
    }
}
