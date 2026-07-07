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
    collections::{BTreeMap, BTreeSet},
    env, fs,
    future::Future,
    io::Write,
    path::{Component, Path, PathBuf},
    pin::Pin,
    sync::{atomic::AtomicBool, Arc},
    time::Instant,
};

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

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
    secret_refs::SecretRef,
    workspace_patch::{
        apply_workspace_patch, compute_patch_sha256, redact_patch_preview,
        WorkspacePatchFileAttestation, WorkspacePatchLimits, WorkspacePatchOutcome,
        WorkspacePatchRedactionPolicy, WorkspacePatchRequest,
    },
};
use palyra_sandbox::{current_backend_capabilities, current_backend_kind};
use palyra_vault::{SecretResolver, Vault};
use palyra_workerd::{
    WorkerCleanupReport, WorkerFleetPolicy, WorkerFleetSnapshot, WorkerRemoteToolKind,
    WORKER_REMOTE_TOOL_PROTOCOL, WORKER_REMOTE_TOOL_SCHEMA_VERSION,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use ulid::Ulid;

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
        build_tool_execution_outcome, build_tool_execution_outcome_with_manifest,
        execute_tool_call_with_cancellation_and_progress, ExecutionAttestationManifest,
        ExecutionCleanupEvidence, ExecutionCleanupResourceEvidence, ToolCallConfig,
        ToolExecutionOutcome,
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
            writeback: WorkspaceWritebackMode::PatchBundle,
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

/// Capability state combining inventory declarations with live runner support.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ExecutionBackendCapabilityStatus {
    pub(crate) capability: String,
    pub(crate) declared_by_inventory: bool,
    pub(crate) supported_by_runner: bool,
}

/// Cleanup evidence surfaced for status pages and support bundles.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ExecutionBackendCleanupEvidenceReport {
    pub(crate) cleanup_supported: bool,
    pub(crate) cleanup_strategy: String,
    pub(crate) evidence_kind: String,
    pub(crate) reason_code: String,
}

/// Status report for one backend, including capability mismatch evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ExecutionBackendStatusReport {
    pub(crate) backend_id: String,
    pub(crate) state: ExecutionBackendState,
    pub(crate) selectable: bool,
    pub(crate) health_status: ExecutionBackendHealthStatus,
    pub(crate) reason_code: String,
    pub(crate) declared_capabilities: Vec<String>,
    pub(crate) runner_capabilities: Vec<String>,
    pub(crate) capability_status: Vec<ExecutionBackendCapabilityStatus>,
    pub(crate) cleanup: ExecutionBackendCleanupEvidenceReport,
}

/// Redacted environment inventory exposed to models and operator diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct EnvironmentInventoryRecord {
    pub(crate) schema_version: u8,
    pub(crate) backend_id: String,
    pub(crate) backend_type: String,
    pub(crate) state: ExecutionBackendState,
    pub(crate) selected_by_default: bool,
    pub(crate) workspace_root: String,
    pub(crate) persistence: String,
    pub(crate) writeback_mode: WorkspaceWritebackMode,
    pub(crate) cleanup_strategy: String,
    pub(crate) egress_posture: String,
    pub(crate) env_posture: String,
    pub(crate) environment_epoch: String,
    pub(crate) model_guidance: String,
    pub(crate) operator_detail: String,
    pub(crate) redaction_level: String,
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

struct ExecutionAttestationManifestInput<'a> {
    backend_id: &'a str,
    runner_id: &'a str,
    runner_version: &'a str,
    workspace_strategy_digest: String,
    input_manifest_sha256: String,
    output_manifest_sha256: String,
    cleanup: ExecutionCleanupEvidence,
    egress_posture: String,
}

fn execution_attestation_manifest(
    input: ExecutionAttestationManifestInput<'_>,
) -> ExecutionAttestationManifest {
    ExecutionAttestationManifest {
        schema_version: 1,
        backend_id: input.backend_id.to_owned(),
        runner_id: input.runner_id.to_owned(),
        runner_version: input.runner_version.to_owned(),
        workspace_strategy_digest: input.workspace_strategy_digest,
        input_manifest_sha256: input.input_manifest_sha256,
        output_manifest_sha256: input.output_manifest_sha256,
        cleanup: input.cleanup,
        egress_posture: input.egress_posture,
        policy_decision_id: None,
        approval_id: None,
    }
}

fn cleanup_resource(
    kind: &str,
    status: &str,
    cleanup_required: bool,
    cleanup_verified: bool,
) -> ExecutionCleanupResourceEvidence {
    ExecutionCleanupResourceEvidence {
        kind: kind.to_owned(),
        status: status.to_owned(),
        cleanup_required,
        cleanup_verified,
        identifier_sha256: None,
    }
}

fn local_sandbox_process_manifest(
    runner: &dyn ExecutionBackendRunner,
    policy: &SandboxProcessRunnerPolicy,
    input_json: &[u8],
    outcome: &ToolExecutionOutcome,
) -> ExecutionAttestationManifest {
    execution_attestation_manifest(ExecutionAttestationManifestInput {
        backend_id: runner.backend_preference().as_str(),
        runner_id: runner.runner_id(),
        runner_version: runner.runner_version(),
        workspace_strategy_digest: WorkspaceStrategyDescriptor::daemon_workspace_root()
            .attestation_digest_sha256(),
        input_manifest_sha256: sha256_hex(input_json),
        output_manifest_sha256: sha256_hex(outcome.output_json.as_slice()),
        cleanup: local_sandbox_cleanup_evidence(outcome),
        egress_posture: format!(
            "process_runner_egress:{}",
            policy.egress_enforcement_mode.as_str()
        ),
    })
}

fn local_sandbox_cleanup_evidence(outcome: &ToolExecutionOutcome) -> ExecutionCleanupEvidence {
    let payload = serde_json::from_slice::<serde_json::Value>(outcome.output_json.as_slice()).ok();
    let background = payload
        .as_ref()
        .and_then(|value| value.get("background"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let cleanup_is_null = payload
        .as_ref()
        .and_then(|value| value.get("cleanup"))
        .is_some_and(serde_json::Value::is_null);
    let process_status = if outcome.attestation.timed_out {
        "process_timeout_terminated"
    } else if background && cleanup_is_null {
        "background_launcher_tree_terminated"
    } else if background {
        "background_lifecycle_bounded"
    } else {
        "foreground_process_reaped"
    };
    let process_cleanup_verified = !background || cleanup_is_null || outcome.attestation.timed_out;
    ExecutionCleanupEvidence {
        strategy: "local_sandbox_process_lifecycle".to_owned(),
        success: process_cleanup_verified,
        reason_code: if process_cleanup_verified {
            "local_sandbox.cleanup.ok"
        } else {
            "local_sandbox.cleanup.background_lifecycle_pending"
        }
        .to_owned(),
        resources: vec![
            cleanup_resource("process_tree", process_status, true, process_cleanup_verified),
            cleanup_resource("temporary_files", "scoped_runtime_temp_released", true, true),
        ],
    }
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
    pub(crate) vault: Option<&'a Vault>,
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
            let mut outcome = execute_tool_call_with_cancellation_and_progress(
                request.config,
                request.proposal_id,
                request.tool_name,
                request.input_json,
                request.cancellation_requested,
                request.process_progress_sink,
            )
            .await;
            outcome.attestation.execution_manifest =
                Some(Box::new(local_sandbox_process_manifest(
                    self,
                    &request.config.process_runner,
                    request.input_json,
                    &outcome,
                )));
            outcome
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
    ssh_worker: Option<Box<dyn ExecutionBackendRunner>>,
}

impl std::fmt::Debug for ExecutionBackendRunnerRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExecutionBackendRunnerRegistry")
            .field("local_sandbox", &self.local_sandbox.runner_id())
            .field("docker", &self.docker.as_ref().map(|runner| runner.runner_id()))
            .field("ssh_worker", &self.ssh_worker.as_ref().map(|runner| runner.runner_id()))
            .finish()
    }
}

impl ExecutionBackendRunnerRegistry {
    /// Builds a registry with an explicitly configured Docker runner.
    pub(crate) fn with_docker_runner(docker: Box<dyn ExecutionBackendRunner>) -> Self {
        Self { local_sandbox: LocalSandboxRunner, docker: Some(docker), ssh_worker: None }
    }

    /// Builds a registry with an explicitly configured SSH worker runner.
    pub(crate) fn with_ssh_worker_runner(ssh_worker: Box<dyn ExecutionBackendRunner>) -> Self {
        Self { local_sandbox: LocalSandboxRunner, docker: None, ssh_worker: Some(ssh_worker) }
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
        let docker_profiles =
            enabled_profiles_for_backend(profiles, ExecutionBackendPreference::Docker);
        let ssh_profiles =
            enabled_profiles_for_backend(profiles, ExecutionBackendPreference::SshTunnel);
        if docker_profiles.len() > 1 {
            return Err(
                "execution_backend_profiles must enable at most one Docker profile".to_owned()
            );
        }
        if ssh_profiles.len() > 1 {
            return Err(
                "execution_backend_profiles must enable at most one SSH worker profile".to_owned()
            );
        }

        let docker = match docker_profiles.as_slice() {
            [] => None,
            [profile] => {
                let container_profile = container_backend_profile_from_config(profile)?;
                let docker =
                    DockerRunner::new(container_profile, DockerCliEngine).map_err(|error| {
                        format!(
                            "failed to build Docker execution backend profile '{}': {error}",
                            profile.id
                        )
                    })?;
                Some(Box::new(docker) as Box<dyn ExecutionBackendRunner>)
            }
            _ => unreachable!("multiple Docker profiles were rejected above"),
        };

        let ssh_worker = match ssh_profiles.as_slice() {
            [] => None,
            [profile] => {
                let ssh_profile = ssh_worker_backend_profile_from_config(profile)?;
                let runner = SshWorkerRunner::new(ssh_profile, OperatorManagedSshTunnelTransport)
                    .map_err(|error| {
                    format!(
                        "failed to build SSH worker execution backend profile '{}': {error}",
                        profile.id
                    )
                })?;
                Some(Box::new(runner) as Box<dyn ExecutionBackendRunner>)
            }
            _ => unreachable!("multiple SSH worker profiles were rejected above"),
        };

        Ok(Self { local_sandbox: LocalSandboxRunner, docker, ssh_worker })
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
            ExecutionBackendPreference::SshTunnel => match self.ssh_worker.as_deref() {
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
            | ExecutionBackendPreference::NetworkedWorker => {
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

    fn runner_for_backend(
        &self,
        backend: ExecutionBackendPreference,
    ) -> Option<&dyn ExecutionBackendRunner> {
        match backend {
            ExecutionBackendPreference::Automatic | ExecutionBackendPreference::LocalSandbox => {
                Some(&self.local_sandbox)
            }
            ExecutionBackendPreference::Docker => self.docker.as_deref(),
            ExecutionBackendPreference::SshTunnel => self.ssh_worker.as_deref(),
            ExecutionBackendPreference::DesktopNode
            | ExecutionBackendPreference::NetworkedWorker => None,
        }
    }
}

/// Builds status reports that join configured inventory with live runner
/// capability and cleanup evidence.
#[must_use]
pub(crate) fn build_execution_backend_status_reports(
    inventory: &[ExecutionBackendInventoryRecord],
    runner_registry: &ExecutionBackendRunnerRegistry,
) -> Vec<ExecutionBackendStatusReport> {
    inventory
        .iter()
        .map(|record| execution_backend_status_report(record, runner_registry))
        .collect()
}

fn execution_backend_status_report(
    record: &ExecutionBackendInventoryRecord,
    runner_registry: &ExecutionBackendRunnerRegistry,
) -> ExecutionBackendStatusReport {
    let preference = parse_execution_backend_preference(record.backend_id.as_str(), "backend_id")
        .unwrap_or(ExecutionBackendPreference::Automatic);
    let runner = runner_registry.runner_for_backend(preference);
    let runner_capabilities =
        runner.map(|runner| runner_capability_strings(runner.capabilities())).unwrap_or_default();
    let capability_status = execution_backend_capability_status(
        record.capabilities.as_slice(),
        runner_capabilities.as_slice(),
    );
    let run_process_supported = runner_capabilities
        .iter()
        .any(|capability| capability == ExecutionBackendRunnerCapability::RunProcess.as_str());
    let cleanup_supported = record.supports_cleanup
        && runner_capabilities
            .iter()
            .any(|capability| capability == ExecutionBackendRunnerCapability::Cleanup.as_str());
    let health_status = if record.state == ExecutionBackendState::Disabled
        || (record.selectable && !run_process_supported)
    {
        ExecutionBackendHealthStatus::Unavailable
    } else if record.state == ExecutionBackendState::Degraded {
        ExecutionBackendHealthStatus::Degraded
    } else {
        runner
            .map(|runner| runner.health_probe())
            .map(|health| health.status)
            .unwrap_or(ExecutionBackendHealthStatus::Unavailable)
    };
    let reason_code = if record.state == ExecutionBackendState::Disabled {
        format!("backend.status.disabled.{}", record.backend_id)
    } else if record.selectable && !run_process_supported {
        format!("backend.status.runner_missing_run_process.{}", record.backend_id)
    } else if !cleanup_supported {
        format!("backend.status.cleanup_unverified.{}", record.backend_id)
    } else {
        format!("backend.status.ready.{}", record.backend_id)
    };

    ExecutionBackendStatusReport {
        backend_id: record.backend_id.clone(),
        state: record.state,
        selectable: record.selectable,
        health_status,
        reason_code,
        declared_capabilities: record.capabilities.clone(),
        runner_capabilities,
        capability_status,
        cleanup: ExecutionBackendCleanupEvidenceReport {
            cleanup_supported,
            cleanup_strategy: record.cleanup_strategy.clone(),
            evidence_kind: if cleanup_supported {
                "runner_cleanup_capability"
            } else {
                "inventory_cleanup_only"
            }
            .to_owned(),
            reason_code: if cleanup_supported {
                "execution_backend.cleanup.supported"
            } else {
                "execution_backend.cleanup.unverified"
            }
            .to_owned(),
        },
    }
}

fn execution_backend_capability_status(
    declared_capabilities: &[String],
    runner_capabilities: &[String],
) -> Vec<ExecutionBackendCapabilityStatus> {
    let mut names = declared_capabilities.iter().cloned().collect::<BTreeSet<_>>();
    names.extend(runner_capabilities.iter().cloned());
    names
        .into_iter()
        .map(|capability| ExecutionBackendCapabilityStatus {
            declared_by_inventory: declared_capabilities
                .iter()
                .any(|declared| declared == &capability),
            supported_by_runner: runner_capabilities.iter().any(|runner| runner == &capability),
            capability,
        })
        .collect()
}

fn enabled_profiles_for_backend(
    profiles: &ExecutionBackendProfilesConfig,
    backend: ExecutionBackendPreference,
) -> Vec<&ExecutionBackendProfileConfig> {
    profiles
        .profiles
        .iter()
        .filter(|profile| profile.enabled && profile.kind.eq_ignore_ascii_case(backend.as_str()))
        .collect()
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

fn materialize_docker_vault_env_file(
    profile_id: &str,
    bindings: &[ContainerEnvBinding],
    vault: Option<&Vault>,
    working_dir: &Path,
) -> Result<Option<DockerEnvFileMaterialization>, DockerEngineError> {
    let vault_bindings = bindings
        .iter()
        .filter(|binding| matches!(binding.source_kind, ContainerEnvSourceKind::VaultRef))
        .collect::<Vec<_>>();
    if vault_bindings.is_empty() {
        return Ok(None);
    }
    let Some(vault) = vault else {
        return Err(DockerEngineError {
            reason_code: "docker.env.vault_resolution_unavailable".to_owned(),
            message: format!(
                "Docker profile {profile_id} declares vault-backed env bindings but no vault runtime is available"
            ),
        });
    };
    let path = env::temp_dir().join(format!("palyra-docker-env-{}.env", Ulid::new()));
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        options.mode(0o600);
    }
    let mut file = options.open(path.as_path()).map_err(|error| DockerEngineError {
        reason_code: "docker.env.env_file_create_failed".to_owned(),
        message: format!("failed to create Docker env file for profile {profile_id}: {error}"),
    })?;
    let resolver = SecretResolver::with_working_dir(Some(vault), working_dir);
    let mut env_names = Vec::new();
    for binding in vault_bindings {
        let vault_ref = normalize_docker_vault_ref(binding.value.as_str())?;
        let secret_ref = SecretRef::from_legacy_vault_ref(vault_ref);
        let value = resolver
            .resolve(&secret_ref)
            .and_then(|resolution| {
                resolution.decode_utf8(format!("Docker env binding {}", binding.name).as_str())
            })
            .map_err(|error| DockerEngineError {
                reason_code: "docker.env.vault_resolution_failed".to_owned(),
                message: format!(
                    "Docker profile {profile_id} could not resolve vault-backed env binding {}: {}",
                    binding.name, error.message
                ),
            })?;
        if value.contains(['\n', '\r', '\0']) {
            return Err(DockerEngineError {
                reason_code: "docker.env.value_shape_invalid".to_owned(),
                message: format!(
                    "Docker profile {profile_id} env binding {} resolved to a value that cannot be written to Docker env-file format",
                    binding.name
                ),
            });
        }
        writeln!(file, "{}={}", binding.name, value).map_err(|error| DockerEngineError {
            reason_code: "docker.env.env_file_write_failed".to_owned(),
            message: format!("failed to write Docker env file for profile {profile_id}: {error}"),
        })?;
        env_names.push(binding.name.clone());
    }
    drop(file);
    let cleanup_guard = Arc::new(DockerEnvFileCleanupGuard { path: path.clone() });
    Ok(Some(DockerEnvFileMaterialization {
        path,
        env_names,
        vault_ref_count: bindings
            .iter()
            .filter(|binding| matches!(binding.source_kind, ContainerEnvSourceKind::VaultRef))
            .count(),
        cleanup_guard,
    }))
}

fn normalize_docker_vault_ref(raw: &str) -> Result<String, DockerEngineError> {
    let trimmed = raw.trim();
    let Some(vault_ref) = trimmed.strip_prefix("vault://") else {
        return Err(DockerEngineError {
            reason_code: "docker.env.vault_ref_invalid".to_owned(),
            message: "Docker env vault references must use vault:// handles".to_owned(),
        });
    };
    if vault_ref.trim().is_empty() {
        return Err(DockerEngineError {
            reason_code: "docker.env.vault_ref_invalid".to_owned(),
            message: "Docker env vault reference handle is empty".to_owned(),
        });
    }
    Ok(vault_ref.to_owned())
}

const DOCKER_WORKSPACE_ROOT: &str = "/workspace";
const DOCKER_EGRESS_PROXY_NETWORK: &str = "palyra-egress-proxy";

/// Runtime plan passed to a Docker engine implementation.
#[derive(Debug, Clone)]
pub(crate) struct DockerRunPlan {
    pub(crate) profile_id: String,
    pub(crate) image: String,
    pub(crate) image_digest_sha256: String,
    pub(crate) workspace_strategy_digest: String,
    pub(crate) user: String,
    pub(crate) readonly_rootfs: bool,
    pub(crate) network: ContainerNetworkPolicy,
    pub(crate) mounts: Vec<ContainerMountPolicy>,
    pub(crate) env: Vec<ContainerEnvBinding>,
    pub(crate) env_file: Option<DockerEnvFileMaterialization>,
    pub(crate) background: bool,
    pub(crate) command: String,
    pub(crate) args: Vec<String>,
    pub(crate) working_dir: String,
    pub(crate) limits: ContainerResourceLimits,
    pub(crate) workspace_writeback: WorkspaceWritebackMode,
    pub(crate) cleanup_strategy: String,
}

/// Temporary env-file containing resolved vault-backed Docker env values.
#[derive(Debug, Clone)]
pub(crate) struct DockerEnvFileMaterialization {
    pub(crate) path: PathBuf,
    pub(crate) env_names: Vec<String>,
    pub(crate) vault_ref_count: usize,
    cleanup_guard: Arc<DockerEnvFileCleanupGuard>,
}

#[derive(Debug)]
struct DockerEnvFileCleanupGuard {
    path: PathBuf,
}

impl Drop for DockerEnvFileCleanupGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(self.path.as_path());
    }
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

fn docker_cleanup_evidence(cleanup: &DockerCleanupAttestation) -> ExecutionCleanupEvidence {
    ExecutionCleanupEvidence {
        strategy: cleanup.strategy.clone(),
        success: cleanup.success,
        reason_code: cleanup.reason_code.clone(),
        resources: vec![
            cleanup_resource(
                "container",
                if cleanup.container_removed { "removed" } else { "remove_failed" },
                true,
                cleanup.container_removed,
            ),
            cleanup_resource(
                "workspace_volume",
                if cleanup.volume_removed { "removed" } else { "remove_failed" },
                true,
                cleanup.volume_removed,
            ),
        ],
    }
}

/// Container resource usage summary attached to process output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DockerResourceUsage {
    pub(crate) duration_ms: u64,
    pub(crate) memory_limit_bytes: u64,
    pub(crate) cpu_time_limit_ms: u64,
}

/// Backend-neutral patch-bundle writeback produced from isolated workspace changes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WorkspacePatchBundle {
    pub(crate) schema_version: u8,
    pub(crate) backend_id: String,
    pub(crate) source_manifest: WorkspacePatchBundleSourceManifest,
    pub(crate) reviewed: bool,
    pub(crate) patch_sha256: String,
    pub(crate) file_count: usize,
    pub(crate) files: Vec<String>,
    pub(crate) touched_paths: Vec<WorkspacePatchBundleTouchedPath>,
    pub(crate) symlink_guard_result: WorkspacePatchBundleSymlinkGuardResult,
    pub(crate) binary_file_policy: WorkspacePatchBundleBinaryFilePolicy,
    pub(crate) conflict_summary: WorkspacePatchBundleConflictSummary,
    pub(crate) verification_stale_state: WorkspacePatchBundleVerificationState,
    pub(crate) merge_preview: WorkspacePatchBundleMergePreview,
    pub(crate) rollback_plan: WorkspacePatchBundleRollbackPlan,
    pub(crate) checkpoint_pair: WorkspacePatchBundleCheckpointPair,
    pub(crate) redacted_preview: String,
    #[serde(default, skip_serializing)]
    pub(crate) patch_document: String,
}

pub(crate) type DockerPatchBundle = WorkspacePatchBundle;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WorkspacePatchBundleSourceManifest {
    pub(crate) source_kind: String,
    pub(crate) source_id: String,
    pub(crate) source_digest_sha256: String,
    pub(crate) workspace_strategy_digest: String,
    pub(crate) artifact_transport: String,
    pub(crate) writeback_mode: String,
    pub(crate) authoritative_workspace_mutation: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WorkspacePatchBundleTouchedPath {
    pub(crate) path: String,
    pub(crate) workspace_root_index: usize,
    pub(crate) operation: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) moved_from: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WorkspacePatchBundleSymlinkGuardResult {
    pub(crate) checked: bool,
    pub(crate) status: String,
    pub(crate) rejected_paths: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WorkspacePatchBundleBinaryFilePolicy {
    pub(crate) mode: String,
    pub(crate) text_only: bool,
    pub(crate) rejected_paths: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WorkspacePatchBundleConflictSummary {
    pub(crate) status: String,
    pub(crate) stale_view_possible: bool,
    pub(crate) conflicting_paths: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WorkspacePatchBundleVerificationState {
    pub(crate) status: String,
    pub(crate) reason_codes: Vec<String>,
    pub(crate) changed_paths: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WorkspacePatchBundleMergePreview {
    pub(crate) mode: String,
    pub(crate) apply_tool: String,
    pub(crate) dry_run_success: bool,
    pub(crate) review_required_before_apply: bool,
    pub(crate) authoritative_workspace_mutation: bool,
    pub(crate) files_changed: usize,
    pub(crate) patch_sha256: String,
    pub(crate) redacted_preview: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WorkspacePatchBundleRollbackPlan {
    pub(crate) mode: String,
    pub(crate) checkpoint_pair_required: bool,
    pub(crate) preflight_checkpoint_required: bool,
    pub(crate) restore_report_required: bool,
    pub(crate) restore_scope_kind: String,
    pub(crate) target_paths: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WorkspacePatchBundleCheckpointPair {
    pub(crate) status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) tool_job_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) mutation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) preflight_checkpoint_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) post_change_checkpoint_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) restore_report_id: Option<String>,
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
            let started = Instant::now();
            let (run_plan, writeback_capture) = prepare_docker_run_plan(&plan)?;
            let mut command = tokio::process::Command::new("docker");
            command.arg("run").arg("--rm");
            if run_plan.readonly_rootfs {
                command.arg("--read-only");
            }
            command.arg("--user").arg(run_plan.user.as_str());
            command.arg("--network").arg(docker_network_arg(run_plan.network));
            command.arg("--workdir").arg(run_plan.working_dir.as_str());
            command.arg("--memory").arg(format!("{}b", run_plan.limits.memory_limit_bytes));
            for mount in &run_plan.mounts {
                command.arg("--mount").arg(docker_mount_arg(mount));
            }
            if let Some(env_file) = run_plan.env_file.as_ref() {
                command.arg("--env-file").arg(env_file.path.as_os_str());
            }
            for binding in &run_plan.env {
                match binding.source_kind {
                    ContainerEnvSourceKind::LiteralSafeValue => {
                        command.arg("--env").arg(format!("{}={}", binding.name, binding.value));
                    }
                    ContainerEnvSourceKind::VaultRef => {}
                }
            }
            command.arg(run_plan.image.as_str());
            command.arg(run_plan.command.as_str());
            command.args(run_plan.args.iter().map(String::as_str));
            let output = command.output().await.map_err(|error| DockerEngineError {
                reason_code: "docker.spawn_failed".to_owned(),
                message: format!(
                    "failed to launch Docker CLI for profile {}: {error}",
                    run_plan.profile_id
                ),
            })?;
            let patch_bundle = match writeback_capture {
                Some(capture) => capture.finish()?,
                None => None,
            };
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
                    reason_code: "docker.cleanup.ok".to_owned(),
                },
                patch_bundle,
            })
        })
    }
}

#[derive(Debug)]
struct DockerWorkspaceWritebackCapture {
    original_root: PathBuf,
    temp_workspace: PathBuf,
    source_manifest: WorkspacePatchBundleSourceManifest,
    tempdir: tempfile::TempDir,
}

impl DockerWorkspaceWritebackCapture {
    fn finish(self) -> Result<Option<DockerPatchBundle>, DockerEngineError> {
        let patch_bundle = docker_patch_bundle_from_workspace_diff(
            &self.original_root,
            &self.temp_workspace,
            self.source_manifest,
        )?;
        self.tempdir.close().map_err(|error| DockerEngineError {
            reason_code: "docker.cleanup.volume_remove_failed".to_owned(),
            message: format!("failed to remove Docker writeback temp workspace: {error}"),
        })?;
        Ok(patch_bundle)
    }
}

fn prepare_docker_run_plan(
    plan: &DockerRunPlan,
) -> Result<(DockerRunPlan, Option<DockerWorkspaceWritebackCapture>), DockerEngineError> {
    let mut run_plan = plan.clone();
    let writable_mount_indexes = run_plan
        .mounts
        .iter()
        .enumerate()
        .filter_map(|(index, mount)| (!mount.read_only).then_some(index))
        .collect::<Vec<_>>();
    if writable_mount_indexes.len() > 1 {
        return Err(DockerEngineError {
            reason_code: "docker.writeback.multiple_mounts_unsupported".to_owned(),
            message: format!(
                "Docker profile {} declares multiple writable workspace mounts; patch-bundle capture supports exactly one",
                plan.profile_id
            ),
        });
    }

    for (index, mount) in run_plan.mounts.iter_mut().enumerate() {
        let canonical_host_path = canonical_docker_mount_host_path(mount.host_path.as_str())?;
        if writable_mount_indexes.first().copied() == Some(index) {
            let tempdir = tempfile::Builder::new()
                .prefix("palyra-docker-writeback-")
                .tempdir()
                .map_err(|error| DockerEngineError {
                    reason_code: "docker.writeback.temp_workspace_failed".to_owned(),
                    message: format!("failed to create Docker writeback temp workspace: {error}"),
                })?;
            let temp_workspace = tempdir.path().join("workspace");
            copy_workspace_tree(canonical_host_path.as_path(), temp_workspace.as_path())?;
            mount.host_path = temp_workspace.display().to_string();
            let source_manifest = docker_patch_bundle_source_manifest(&run_plan);
            return Ok((
                run_plan,
                Some(DockerWorkspaceWritebackCapture {
                    original_root: canonical_host_path,
                    temp_workspace,
                    source_manifest,
                    tempdir,
                }),
            ));
        }
        mount.host_path = canonical_host_path.display().to_string();
    }
    Ok((run_plan, None))
}

fn canonical_docker_mount_host_path(raw: &str) -> Result<PathBuf, DockerEngineError> {
    if raw.contains('\0') {
        return Err(DockerEngineError {
            reason_code: "docker.mount.path_invalid".to_owned(),
            message: "Docker mount host path contains an embedded NUL byte".to_owned(),
        });
    }
    let raw_path = Path::new(raw);
    let candidate = if raw_path.is_absolute() {
        raw_path.to_path_buf()
    } else {
        env::current_dir()
            .map_err(|error| DockerEngineError {
                reason_code: "docker.mount.cwd_unavailable".to_owned(),
                message: format!("failed to resolve current directory for Docker mount: {error}"),
            })?
            .join(raw_path)
    };
    let canonical = candidate.canonicalize().map_err(|error| DockerEngineError {
        reason_code: "docker.mount.path_unavailable".to_owned(),
        message: format!("Docker mount host path {} is unavailable: {error}", candidate.display()),
    })?;
    if !canonical.is_dir() {
        return Err(DockerEngineError {
            reason_code: "docker.mount.not_directory".to_owned(),
            message: format!("Docker mount host path {} is not a directory", canonical.display()),
        });
    }
    Ok(canonical)
}

fn copy_workspace_tree(source: &Path, destination: &Path) -> Result<(), DockerEngineError> {
    fs::create_dir_all(destination).map_err(|error| DockerEngineError {
        reason_code: "docker.writeback.copy_failed".to_owned(),
        message: format!(
            "failed to create Docker writeback workspace {}: {error}",
            destination.display()
        ),
    })?;
    let mut entries = fs::read_dir(source)
        .map_err(|error| DockerEngineError {
            reason_code: "docker.writeback.copy_failed".to_owned(),
            message: format!("failed to read workspace {}: {error}", source.display()),
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| DockerEngineError {
            reason_code: "docker.writeback.copy_failed".to_owned(),
            message: format!("failed to enumerate workspace {}: {error}", source.display()),
        })?;
    entries.sort_by_key(|entry| entry.path());
    for entry in entries {
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let metadata =
            fs::symlink_metadata(source_path.as_path()).map_err(|error| DockerEngineError {
                reason_code: "docker.writeback.copy_failed".to_owned(),
                message: format!(
                    "failed to inspect workspace path {}: {error}",
                    source_path.display()
                ),
            })?;
        let file_type = metadata.file_type();
        if file_type.is_symlink() {
            return Err(DockerEngineError {
                reason_code: "docker.writeback.symlink_unsupported".to_owned(),
                message: format!("Docker writeback refuses symlink path {}", source_path.display()),
            });
        }
        if file_type.is_dir() {
            copy_workspace_tree(source_path.as_path(), destination_path.as_path())?;
        } else if file_type.is_file() {
            fs::copy(source_path.as_path(), destination_path.as_path()).map_err(|error| {
                DockerEngineError {
                    reason_code: "docker.writeback.copy_failed".to_owned(),
                    message: format!(
                        "failed to copy workspace file {}: {error}",
                        source_path.display()
                    ),
                }
            })?;
        } else {
            return Err(DockerEngineError {
                reason_code: "docker.writeback.special_file_unsupported".to_owned(),
                message: format!(
                    "Docker writeback refuses non-regular workspace path {}",
                    source_path.display()
                ),
            });
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DockerWorkspaceFileSnapshot {
    bytes: Vec<u8>,
}

fn docker_patch_bundle_from_workspace_diff(
    original_root: &Path,
    mutated_root: &Path,
    source_manifest: WorkspacePatchBundleSourceManifest,
) -> Result<Option<DockerPatchBundle>, DockerEngineError> {
    let before = collect_workspace_file_snapshots(original_root, original_root)?;
    let after = collect_workspace_file_snapshots(mutated_root, mutated_root)?;
    let Some((patch_document, files)) = docker_patch_document_from_snapshots(&before, &after)?
    else {
        return Ok(None);
    };
    let limits = WorkspacePatchLimits::default();
    let redaction_policy = WorkspacePatchRedactionPolicy::default();
    let request = WorkspacePatchRequest {
        patch: patch_document.clone(),
        dry_run: true,
        redaction_policy: redaction_policy.clone(),
    };
    let planned_outcome = apply_workspace_patch(&[original_root.to_path_buf()], &request, &limits)
        .map_err(|error| DockerEngineError {
            reason_code: "docker.writeback.patch_validation_failed".to_owned(),
            message: format!("generated Docker writeback patch failed dry-run validation: {error}"),
        })?;
    Ok(Some(workspace_patch_bundle_from_planned_patch(
        ExecutionBackendPreference::Docker.as_str(),
        source_manifest,
        patch_document,
        files,
        &planned_outcome,
        &redaction_policy,
        &limits,
    )))
}

fn docker_patch_bundle_source_manifest(plan: &DockerRunPlan) -> WorkspacePatchBundleSourceManifest {
    let source_descriptor = json!({
        "schema_version": 1,
        "source_kind": "docker_container_workspace",
        "source_id": plan.profile_id,
        "image_digest_sha256": plan.image_digest_sha256,
        "workspace_strategy_digest": plan.workspace_strategy_digest,
        "workspace_writeback": plan.workspace_writeback.as_str(),
        "artifact_transport": "container_patch_bundle_transfer",
    });
    WorkspacePatchBundleSourceManifest {
        source_kind: "docker_container_workspace".to_owned(),
        source_id: plan.profile_id.clone(),
        source_digest_sha256: sha256_hex(
            serde_json::to_vec(&source_descriptor).unwrap_or_default().as_slice(),
        ),
        workspace_strategy_digest: plan.workspace_strategy_digest.clone(),
        artifact_transport: "container_patch_bundle_transfer".to_owned(),
        writeback_mode: plan.workspace_writeback.as_str().to_owned(),
        authoritative_workspace_mutation: false,
    }
}

fn workspace_patch_bundle_from_planned_patch(
    backend_id: &str,
    source_manifest: WorkspacePatchBundleSourceManifest,
    patch_document: String,
    files: Vec<String>,
    planned_outcome: &WorkspacePatchOutcome,
    redaction_policy: &WorkspacePatchRedactionPolicy,
    limits: &WorkspacePatchLimits,
) -> WorkspacePatchBundle {
    let patch_sha256 = compute_patch_sha256(patch_document.as_str());
    let redacted_preview =
        redact_patch_preview(patch_document.as_str(), redaction_policy, limits.max_preview_bytes);
    let touched_paths =
        workspace_patch_bundle_touched_paths(planned_outcome.files_touched.as_slice());
    let changed_paths = touched_paths
        .iter()
        .map(|path| path.path.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    WorkspacePatchBundle {
        schema_version: 2,
        backend_id: backend_id.to_owned(),
        source_manifest,
        reviewed: true,
        patch_sha256: patch_sha256.clone(),
        file_count: files.len(),
        files: files.clone(),
        touched_paths,
        symlink_guard_result: WorkspacePatchBundleSymlinkGuardResult {
            checked: true,
            status: "passed".to_owned(),
            rejected_paths: Vec::new(),
        },
        binary_file_policy: WorkspacePatchBundleBinaryFilePolicy {
            mode: "reject_binary_text_patch_only".to_owned(),
            text_only: true,
            rejected_paths: Vec::new(),
        },
        conflict_summary: WorkspacePatchBundleConflictSummary {
            status: "clean".to_owned(),
            stale_view_possible: false,
            conflicting_paths: Vec::new(),
        },
        verification_stale_state: WorkspacePatchBundleVerificationState {
            status: "pending_after_bundle_apply".to_owned(),
            reason_codes: vec!["verification.pending_after_patch_bundle_apply".to_owned()],
            changed_paths,
        },
        merge_preview: WorkspacePatchBundleMergePreview {
            mode: "dry_run_apply_patch".to_owned(),
            apply_tool: "palyra.fs.apply_patch".to_owned(),
            dry_run_success: true,
            review_required_before_apply: true,
            authoritative_workspace_mutation: false,
            files_changed: files.len(),
            patch_sha256: patch_sha256.clone(),
            redacted_preview: redacted_preview.clone(),
        },
        rollback_plan: WorkspacePatchBundleRollbackPlan {
            mode: "workspace_checkpoint_restore".to_owned(),
            checkpoint_pair_required: true,
            preflight_checkpoint_required: true,
            restore_report_required: true,
            restore_scope_kind: "workspace".to_owned(),
            target_paths: files,
        },
        checkpoint_pair: WorkspacePatchBundleCheckpointPair {
            status: "required_on_apply".to_owned(),
            tool_job_ref: None,
            mutation_id: None,
            preflight_checkpoint_id: None,
            post_change_checkpoint_id: None,
            restore_report_id: None,
        },
        redacted_preview,
        patch_document,
    }
}

fn workspace_patch_bundle_touched_paths(
    files_touched: &[WorkspacePatchFileAttestation],
) -> Vec<WorkspacePatchBundleTouchedPath> {
    files_touched
        .iter()
        .map(|file| WorkspacePatchBundleTouchedPath {
            path: file.path.clone(),
            workspace_root_index: file.workspace_root_index,
            operation: file.operation.clone(),
            moved_from: file.moved_from.clone(),
        })
        .collect()
}

fn workspace_patch_bundle_for_tool_job(
    bundle: &WorkspacePatchBundle,
    tool_job_ref: &str,
) -> WorkspacePatchBundle {
    let mut scoped = bundle.clone();
    scoped.checkpoint_pair.tool_job_ref = Some(tool_job_ref.to_owned());
    scoped
}

fn workspace_patch_bundle_manifest_projection(bundle: &WorkspacePatchBundle) -> serde_json::Value {
    json!({
        "schema_version": bundle.schema_version,
        "backend_id": bundle.backend_id,
        "source_manifest": bundle.source_manifest,
        "patch_sha256": bundle.patch_sha256,
        "file_count": bundle.file_count,
        "files": bundle.files,
        "touched_paths": bundle.touched_paths,
        "symlink_guard_result": bundle.symlink_guard_result,
        "binary_file_policy": bundle.binary_file_policy,
        "conflict_summary": bundle.conflict_summary,
        "verification_stale_state": bundle.verification_stale_state,
        "merge_preview": bundle.merge_preview,
        "rollback_plan": bundle.rollback_plan,
        "checkpoint_pair": bundle.checkpoint_pair,
    })
}

fn collect_workspace_file_snapshots(
    root: &Path,
    current: &Path,
) -> Result<BTreeMap<String, DockerWorkspaceFileSnapshot>, DockerEngineError> {
    let mut snapshots = BTreeMap::new();
    collect_workspace_file_snapshots_into(root, current, &mut snapshots)?;
    Ok(snapshots)
}

fn collect_workspace_file_snapshots_into(
    root: &Path,
    current: &Path,
    snapshots: &mut BTreeMap<String, DockerWorkspaceFileSnapshot>,
) -> Result<(), DockerEngineError> {
    let mut entries = fs::read_dir(current)
        .map_err(|error| DockerEngineError {
            reason_code: "docker.writeback.diff_failed".to_owned(),
            message: format!("failed to read workspace {}: {error}", current.display()),
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| DockerEngineError {
            reason_code: "docker.writeback.diff_failed".to_owned(),
            message: format!("failed to enumerate workspace {}: {error}", current.display()),
        })?;
    entries.sort_by_key(|entry| entry.path());
    for entry in entries {
        let path = entry.path();
        let metadata = fs::symlink_metadata(path.as_path()).map_err(|error| DockerEngineError {
            reason_code: "docker.writeback.diff_failed".to_owned(),
            message: format!("failed to inspect workspace path {}: {error}", path.display()),
        })?;
        let file_type = metadata.file_type();
        if file_type.is_symlink() {
            return Err(DockerEngineError {
                reason_code: "docker.writeback.symlink_unsupported".to_owned(),
                message: format!("Docker writeback refuses symlink path {}", path.display()),
            });
        }
        if file_type.is_dir() {
            collect_workspace_file_snapshots_into(root, path.as_path(), snapshots)?;
        } else if file_type.is_file() {
            let relative_path = workspace_relative_patch_path(root, path.as_path())?;
            let bytes = fs::read(path.as_path()).map_err(|error| DockerEngineError {
                reason_code: "docker.writeback.diff_failed".to_owned(),
                message: format!("failed to read workspace file {}: {error}", path.display()),
            })?;
            snapshots.insert(relative_path, DockerWorkspaceFileSnapshot { bytes });
        } else {
            return Err(DockerEngineError {
                reason_code: "docker.writeback.special_file_unsupported".to_owned(),
                message: format!(
                    "Docker writeback refuses non-regular workspace path {}",
                    path.display()
                ),
            });
        }
    }
    Ok(())
}

fn workspace_relative_patch_path(root: &Path, path: &Path) -> Result<String, DockerEngineError> {
    let relative = path.strip_prefix(root).map_err(|error| DockerEngineError {
        reason_code: "docker.writeback.diff_failed".to_owned(),
        message: format!(
            "workspace path {} is outside root {}: {error}",
            path.display(),
            root.display()
        ),
    })?;
    let mut parts = Vec::new();
    for component in relative.components() {
        match component {
            Component::Normal(part) => {
                let Some(part) = part.to_str() else {
                    return Err(DockerEngineError {
                        reason_code: "docker.writeback.non_utf8_path".to_owned(),
                        message: format!("Docker writeback path {} is not UTF-8", path.display()),
                    });
                };
                if part.is_empty() {
                    return Err(DockerEngineError {
                        reason_code: "docker.writeback.path_invalid".to_owned(),
                        message: format!("Docker writeback path {} is empty", path.display()),
                    });
                }
                parts.push(part.to_owned());
            }
            Component::CurDir => {}
            Component::ParentDir | Component::Prefix(_) | Component::RootDir => {
                return Err(DockerEngineError {
                    reason_code: "docker.writeback.path_invalid".to_owned(),
                    message: format!(
                        "Docker writeback path {} is not workspace-relative",
                        path.display()
                    ),
                });
            }
        }
    }
    if parts.is_empty() {
        return Err(DockerEngineError {
            reason_code: "docker.writeback.path_invalid".to_owned(),
            message: format!("Docker writeback path {} has no relative file name", path.display()),
        });
    }
    Ok(parts.join("/"))
}

fn docker_patch_document_from_snapshots(
    before: &BTreeMap<String, DockerWorkspaceFileSnapshot>,
    after: &BTreeMap<String, DockerWorkspaceFileSnapshot>,
) -> Result<Option<(String, Vec<String>)>, DockerEngineError> {
    let mut paths = BTreeSet::new();
    paths.extend(before.keys().cloned());
    paths.extend(after.keys().cloned());

    let mut changed_files = Vec::new();
    let mut patch = String::from("*** Begin Patch\n");
    for path in paths {
        match (before.get(path.as_str()), after.get(path.as_str())) {
            (None, Some(after_file)) => {
                append_docker_full_file_patch(
                    &mut patch,
                    "*** Add File",
                    path.as_str(),
                    after_file,
                )?;
                changed_files.push(path);
            }
            (Some(_), None) => {
                patch.push_str("*** Delete File: ");
                patch.push_str(path.as_str());
                patch.push('\n');
                changed_files.push(path);
            }
            (Some(before_file), Some(after_file)) if before_file.bytes != after_file.bytes => {
                append_docker_full_file_patch(
                    &mut patch,
                    "*** Replace File",
                    path.as_str(),
                    after_file,
                )?;
                changed_files.push(path);
            }
            _ => {}
        }
    }
    if changed_files.is_empty() {
        return Ok(None);
    }
    patch.push_str("*** End Patch\n");
    Ok(Some((patch, changed_files)))
}

fn append_docker_full_file_patch(
    patch: &mut String,
    operation: &str,
    path: &str,
    file: &DockerWorkspaceFileSnapshot,
) -> Result<(), DockerEngineError> {
    let text = std::str::from_utf8(file.bytes.as_slice()).map_err(|error| DockerEngineError {
        reason_code: "docker.writeback.binary_unsupported".to_owned(),
        message: format!("Docker writeback file {path} is not UTF-8 text: {error}"),
    })?;
    if text.is_empty() || !text.ends_with('\n') {
        return Err(DockerEngineError {
            reason_code: "docker.writeback.patch_unsupported".to_owned(),
            message: format!(
                "Docker writeback file {path} cannot be represented as a full-file Palyra patch because it is empty or lacks a trailing newline"
            ),
        });
    }
    patch.push_str(operation);
    patch.push_str(": ");
    patch.push_str(path);
    patch.push('\n');
    let without_final_newline = text.strip_suffix('\n').unwrap_or(text);
    for line in without_final_newline.split('\n') {
        append_docker_full_file_patch_line(patch, line);
    }
    Ok(())
}

fn append_docker_full_file_patch_line(patch: &mut String, line: &str) {
    if docker_patch_line_needs_body_prefix(line) {
        patch.push('+');
    }
    patch.push_str(line);
    patch.push('\n');
}

fn docker_patch_line_needs_body_prefix(line: &str) -> bool {
    let control_line = line.trim_end_matches([' ', '\t']);
    let trimmed = line.trim_start();
    line.starts_with('+')
        || control_line == "*** End Patch"
        || control_line.starts_with("*** ")
        || trimmed.starts_with("diff --git ")
        || trimmed.starts_with("index ")
        || docker_patch_line_is_unified_header(trimmed, "---")
        || docker_patch_line_is_unified_header(trimmed, "+++")
        || trimmed.starts_with("@@")
        || trimmed.starts_with("<<<<<<<")
        || trimmed.starts_with("=======")
        || trimmed.starts_with(">>>>>>>")
}

fn docker_patch_line_is_unified_header(line: &str, prefix: &str) -> bool {
    let Some(rest) = line.strip_prefix(prefix) else {
        return false;
    };
    rest.starts_with([' ', '\t']) && !rest.trim().is_empty()
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
            let plan = match docker_process_run_plan(
                &self.profile,
                request.config,
                request.input_json,
                request.vault,
            ) {
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
    request_vault: Option<&Vault>,
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
    let env_file = materialize_docker_vault_env_file(
        profile.profile_id.as_str(),
        profile.env.as_slice(),
        request_vault,
        Path::new(workspace_mount.host_path.as_str()),
    )?;
    Ok(DockerRunPlan {
        profile_id: profile.profile_id.clone(),
        image: profile.image.clone(),
        image_digest_sha256,
        workspace_strategy_digest: WorkspaceStrategyDescriptor::container_volume()
            .attestation_digest_sha256(),
        user: profile.user.clone(),
        readonly_rootfs: profile.readonly_rootfs,
        network: profile.network,
        mounts: vec![workspace_mount.clone()],
        env: docker_plan_env_bindings(profile.env.as_slice()),
        env_file,
        background: input.background || input.keep_running_after_run,
        command: input.command,
        args: input.args,
        working_dir: docker_container_working_dir(input.cwd.as_deref())?,
        limits: profile.limits.clone(),
        workspace_writeback: WorkspaceWritebackMode::PatchBundle,
        cleanup_strategy: profile.cleanup_strategy.clone(),
    })
}

fn docker_plan_env_bindings(bindings: &[ContainerEnvBinding]) -> Vec<ContainerEnvBinding> {
    bindings
        .iter()
        .map(|binding| match binding.source_kind {
            ContainerEnvSourceKind::LiteralSafeValue => binding.clone(),
            ContainerEnvSourceKind::VaultRef => ContainerEnvBinding {
                name: binding.name.clone(),
                source_kind: ContainerEnvSourceKind::VaultRef,
                value: format!("vault_ref_sha256:{}", sha256_hex(binding.value.as_bytes())),
            },
        })
        .collect()
}

fn validate_docker_process_input(
    config: &ToolCallConfig,
    input: &ProcessRunnerToolInput,
) -> Result<(), DockerEngineError> {
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
    let cleanup = report.cleanup.clone();
    let cleanup_success = cleanup.success;
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
    let patch_bundle = report
        .patch_bundle
        .as_ref()
        .map(|bundle| workspace_patch_bundle_for_tool_job(bundle, proposal_id));
    let output_manifest = json!({
        "schema_version": 1,
        "profile_id": plan.profile_id,
        "image_digest_sha256": plan.image_digest_sha256,
        "stdout_sha256": sha256_hex(report.stdout.as_slice()),
        "stderr_sha256": sha256_hex(report.stderr.as_slice()),
        "workspace_writeback": plan.workspace_writeback.as_str(),
        "env_file": plan.env_file.as_ref().map(|env_file| json!({
            "materialized": true,
            "env_names": env_file.env_names.as_slice(),
            "vault_ref_count": env_file.vault_ref_count,
            "path_sha256": sha256_hex(env_file.path.to_string_lossy().as_bytes()),
        })),
        "background": {
            "requested": plan.background,
            "handle_kind": if plan.background { "docker_attached_run" } else { "none" },
            "status": if plan.background { "completed_or_timed_out_with_tool_call" } else { "not_requested" },
            "reason_code": if plan.background {
                "docker.background.attached_handle"
            } else {
                "docker.background.not_requested"
            },
        },
        "cleanup_success": cleanup_success,
        "patch_bundle_sha256": patch_bundle.as_ref().map(|bundle| bundle.patch_sha256.as_str()),
        "patch_bundle": patch_bundle.as_ref().map(workspace_patch_bundle_manifest_projection),
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
            "patch_bundle": patch_bundle,
        },
        "background_handle": {
            "requested": plan.background,
            "handle_kind": if plan.background { "docker_attached_run" } else { "none" },
            "status_command": if plan.background { Some("palyra.process.status") } else { None },
            "tail_command": if plan.background { Some("palyra.process.status") } else { None },
            "stop_command": if plan.background { Some("palyra.process.stop") } else { None },
            "cleanup_registered": false,
            "reason_code": if plan.background {
                "docker.background.attached_handle"
            } else {
                "docker.background.not_requested"
            },
        },
        "cleanup": cleanup,
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
    let manifest = execution_attestation_manifest(ExecutionAttestationManifestInput {
        backend_id: ExecutionBackendPreference::Docker.as_str(),
        runner_id: "docker_runner",
        runner_version: "v1",
        workspace_strategy_digest: plan.workspace_strategy_digest.clone(),
        input_manifest_sha256: sha256_hex(input_json),
        output_manifest_sha256,
        cleanup: docker_cleanup_evidence(&report.cleanup),
        egress_posture: docker_egress_posture(plan.network).to_owned(),
    });
    build_tool_execution_outcome_with_manifest(
        proposal_id,
        tool_name,
        input_json,
        success,
        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
        error,
        false,
        "docker".to_owned(),
        "container_profile".to_owned(),
        manifest,
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
    let output_json = serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec());
    let manifest = execution_attestation_manifest(ExecutionAttestationManifestInput {
        backend_id: ExecutionBackendPreference::Docker.as_str(),
        runner_id: "docker_runner",
        runner_version: "v1",
        workspace_strategy_digest: WorkspaceStrategyDescriptor::container_volume()
            .attestation_digest_sha256(),
        input_manifest_sha256: sha256_hex(input_json),
        output_manifest_sha256: sha256_hex(output_json.as_slice()),
        cleanup: ExecutionCleanupEvidence {
            strategy: "container_profile_preflight".to_owned(),
            success: false,
            reason_code: reason_code.to_owned(),
            resources: vec![
                cleanup_resource("container", "not_started", false, false),
                cleanup_resource("workspace_volume", "not_started", false, false),
            ],
        },
        egress_posture: "container_network:preflight_not_started".to_owned(),
    });
    build_tool_execution_outcome_with_manifest(
        proposal_id,
        tool_name,
        input_json,
        false,
        output_json,
        message,
        false,
        "docker".to_owned(),
        "container_profile_preflight".to_owned(),
        manifest,
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

fn docker_egress_posture(network: ContainerNetworkPolicy) -> &'static str {
    match network {
        ContainerNetworkPolicy::None => "container_network:none",
        ContainerNetworkPolicy::EgressProxy => "container_network:egress_proxy",
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
    pub(crate) tunnel_endpoint: String,
    pub(crate) host_handle: String,
    pub(crate) user_handle: String,
    pub(crate) identity_handle: String,
    pub(crate) host_trust_handle: String,
    pub(crate) worker_protocol: String,
    pub(crate) health_probe: String,
    pub(crate) capabilities: Vec<String>,
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
        if !ssh_worker_tunnel_endpoint_is_safe(self.tunnel_endpoint.as_str()) {
            return Err(
                "ssh worker tunnel_endpoint must be a non-empty endpoint label, not a shell command"
                    .to_owned(),
            );
        }
        for (field_name, value) in [
            ("host_handle", self.host_handle.as_str()),
            ("user_handle", self.user_handle.as_str()),
            ("identity_handle", self.identity_handle.as_str()),
            ("host_trust_handle", self.host_trust_handle.as_str()),
        ] {
            if !ssh_worker_handle_is_reference(value) {
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
        if self.health_probe.trim().is_empty() {
            return Err("ssh worker health_probe must not be empty".to_owned());
        }
        if self.capabilities.is_empty() {
            return Err("ssh worker backend requires negotiated worker RPC capabilities".to_owned());
        }
        for capability in &self.capabilities {
            if !ssh_worker_capability_is_safe(capability) {
                return Err(format!(
                    "ssh worker capability {capability:?} must be a worker RPC capability, not raw shell"
                ));
            }
        }
        Ok(())
    }
}

fn ssh_worker_backend_profile_from_config(
    profile: &ExecutionBackendProfileConfig,
) -> Result<SshWorkerBackendProfile, String> {
    let ssh_worker = profile.ssh_worker.as_ref().ok_or_else(|| {
        format!("SSH execution backend profile '{}' requires an ssh_worker block", profile.id)
    })?;
    let backend_profile = SshWorkerBackendProfile {
        profile_id: profile.id.clone(),
        tunnel_endpoint: ssh_worker.tunnel_endpoint.clone(),
        host_handle: ssh_worker.host_handle.clone(),
        user_handle: ssh_worker.user_handle.clone(),
        identity_handle: ssh_worker.identity_handle.clone(),
        host_trust_handle: ssh_worker.host_trust_handle.clone(),
        worker_protocol: ssh_worker.worker_protocol.clone(),
        health_probe: ssh_worker.health_probe.clone(),
        capabilities: ssh_worker.capabilities.clone(),
        workspace_strategy: WorkspaceStrategyDescriptor::remote_lease_workspace(),
    };
    backend_profile.validate()?;
    Ok(backend_profile)
}

fn ssh_worker_handle_is_reference(value: &str) -> bool {
    let value = value.trim();
    (value.starts_with("vault://") || value.starts_with("identity://"))
        && !value.contains(char::is_whitespace)
}

fn ssh_worker_tunnel_endpoint_is_safe(value: &str) -> bool {
    let value = value.trim();
    !value.is_empty()
        && !value.contains(['\0', '\n', '\r'])
        && !value.contains("&&")
        && !value.contains("||")
        && !value.contains(';')
        && !value.contains('|')
        && !value.contains('`')
        && !value.contains("$(")
}

fn ssh_worker_capability_is_safe(capability: &str) -> bool {
    let capability = capability.trim();
    !capability.is_empty()
        && capability
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, ':' | '.' | '_' | '-' | '/'))
        && !matches!(
            capability.to_ascii_lowercase().as_str(),
            "shell" | "raw_shell" | "ssh.shell" | "ssh.raw_shell" | "tool:shell"
        )
}

/// SSH worker RPC request sent through an operator-managed tunnel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SshWorkerRpcRequestEnvelope {
    pub(crate) protocol: String,
    pub(crate) schema_version: u32,
    pub(crate) request_id: String,
    pub(crate) profile_id: String,
    pub(crate) tunnel_endpoint_sha256: String,
    pub(crate) tool_name: String,
    pub(crate) tool_kind: WorkerRemoteToolKind,
    pub(crate) input_json: String,
    pub(crate) input_json_sha256: String,
    pub(crate) worker_protocol: String,
    pub(crate) health_probe: String,
    pub(crate) negotiated_capabilities: Vec<String>,
    pub(crate) workspace_strategy_digest: String,
    pub(crate) artifact_transport: String,
    pub(crate) writeback_mode: String,
}

/// SSH worker RPC result returned by the remote worker envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SshWorkerRpcResultEnvelope {
    pub(crate) protocol: String,
    pub(crate) schema_version: u32,
    pub(crate) request_id: String,
    pub(crate) success: bool,
    pub(crate) output_json: String,
    pub(crate) output_json_sha256: String,
    pub(crate) error: Option<String>,
    pub(crate) output_manifest_sha256: String,
    pub(crate) cleanup_report: WorkerCleanupReport,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) patch_bundle: Option<WorkspacePatchBundle>,
}

fn worker_cleanup_evidence(
    strategy: &str,
    cleanup_report: &WorkerCleanupReport,
) -> ExecutionCleanupEvidence {
    let success = cleanup_report.is_verified();
    ExecutionCleanupEvidence {
        strategy: strategy.to_owned(),
        success,
        reason_code: cleanup_report.failure_reason.clone().unwrap_or_else(|| {
            if success {
                "worker.cleanup.ok".to_owned()
            } else {
                "worker.cleanup.incomplete".to_owned()
            }
        }),
        resources: vec![
            cleanup_resource(
                "remote_workspace",
                if cleanup_report.removed_workspace_scope { "removed" } else { "remove_failed" },
                true,
                cleanup_report.removed_workspace_scope,
            ),
            cleanup_resource(
                "remote_artifacts",
                if cleanup_report.removed_artifacts { "removed" } else { "remove_failed" },
                true,
                cleanup_report.removed_artifacts,
            ),
            cleanup_resource(
                "remote_logs",
                if cleanup_report.removed_logs { "removed" } else { "remove_failed" },
                true,
                cleanup_report.removed_logs,
            ),
        ],
    }
}

fn worker_not_started_cleanup_evidence(
    strategy: &str,
    reason_code: &str,
) -> ExecutionCleanupEvidence {
    ExecutionCleanupEvidence {
        strategy: strategy.to_owned(),
        success: false,
        reason_code: reason_code.to_owned(),
        resources: vec![
            cleanup_resource("remote_workspace", "not_started", false, false),
            cleanup_resource("remote_artifacts", "not_started", false, false),
            cleanup_resource("remote_logs", "not_started", false, false),
        ],
    }
}

/// SSH worker transport failures converted to fail-closed tool outcomes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SshWorkerTransportError {
    pub(crate) reason_code: String,
    pub(crate) message: String,
}

type SshWorkerRpcFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<SshWorkerRpcResultEnvelope, SshWorkerTransportError>>
            + Send
            + 'a,
    >,
>;

/// Narrow transport seam for SSH worker RPC envelopes.
pub(crate) trait SshWorkerRpcTransport: Send + Sync + std::fmt::Debug {
    fn health_probe(&self, profile: &SshWorkerBackendProfile) -> ExecutionBackendRunnerHealth;

    fn execute<'a>(
        &'a self,
        profile: &'a SshWorkerBackendProfile,
        request: SshWorkerRpcRequestEnvelope,
    ) -> SshWorkerRpcFuture<'a>;
}

/// Production placeholder for operator-managed SSH tunnels.
///
/// The daemon never opens a raw SSH shell here. Until a tunnel transport is
/// explicitly attached, every dispatch fails closed with a repair hint.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct OperatorManagedSshTunnelTransport;

impl SshWorkerRpcTransport for OperatorManagedSshTunnelTransport {
    fn health_probe(&self, profile: &SshWorkerBackendProfile) -> ExecutionBackendRunnerHealth {
        ExecutionBackendRunnerHealth {
            backend_id: ExecutionBackendPreference::SshTunnel.as_str().to_owned(),
            status: ExecutionBackendHealthStatus::Unavailable,
            reason_code: "runner.health.ssh_worker.tunnel_unavailable".to_owned(),
            summary: format!(
                "SSH worker profile {} is valid but no operator-managed RPC tunnel is attached",
                profile.profile_id
            ),
        }
    }

    fn execute<'a>(
        &'a self,
        profile: &'a SshWorkerBackendProfile,
        _request: SshWorkerRpcRequestEnvelope,
    ) -> SshWorkerRpcFuture<'a> {
        Box::pin(async move {
            Err(SshWorkerTransportError {
                reason_code: "runner.unavailable.ssh_tunnel".to_owned(),
                message: format!(
                    "SSH worker profile {} has no active palyra-worker-rpc/v1 tunnel; local fallback is denied",
                    profile.profile_id
                ),
            })
        })
    }
}

const SSH_WORKER_RUNNER_CAPABILITIES: &[ExecutionBackendRunnerCapability] = &[
    ExecutionBackendRunnerCapability::RunProcess,
    ExecutionBackendRunnerCapability::RunToolProgram,
    ExecutionBackendRunnerCapability::ReadArtifact,
    ExecutionBackendRunnerCapability::HealthProbe,
    ExecutionBackendRunnerCapability::AttestationManifest,
];

/// Runner for worker RPC over an operator-managed SSH tunnel.
#[derive(Debug)]
pub(crate) struct SshWorkerRunner<T: SshWorkerRpcTransport> {
    profile: SshWorkerBackendProfile,
    transport: T,
}

impl<T: SshWorkerRpcTransport> SshWorkerRunner<T> {
    /// Builds an SSH worker runner from an already parsed profile.
    ///
    /// # Errors
    /// Returns the first profile invariant violation.
    pub(crate) fn new(profile: SshWorkerBackendProfile, transport: T) -> Result<Self, String> {
        profile.validate()?;
        Ok(Self { profile, transport })
    }

    fn dispatch_remote_tool<'a>(
        &'a self,
        proposal_id: &'a str,
        tool_name: &'a str,
        input_json: &'a [u8],
    ) -> RunnerExecutionFuture<'a> {
        Box::pin(async move {
            let request = match build_ssh_worker_rpc_request(&self.profile, tool_name, input_json) {
                Ok(request) => request,
                Err(error) => {
                    return ssh_worker_error_outcome(
                        proposal_id,
                        tool_name,
                        input_json,
                        error.reason_code.as_str(),
                        error.message,
                        &self.profile,
                    );
                }
            };
            let result = match self.transport.execute(&self.profile, request.clone()).await {
                Ok(result) => result,
                Err(error) => {
                    return ssh_worker_error_outcome(
                        proposal_id,
                        tool_name,
                        input_json,
                        error.reason_code.as_str(),
                        error.message,
                        &self.profile,
                    );
                }
            };
            ssh_worker_outcome_from_rpc_result(
                proposal_id,
                tool_name,
                input_json,
                &self.profile,
                &request,
                result,
            )
        })
    }
}

impl<T: SshWorkerRpcTransport> ExecutionBackendRunner for SshWorkerRunner<T> {
    fn backend_preference(&self) -> ExecutionBackendPreference {
        ExecutionBackendPreference::SshTunnel
    }

    fn runner_id(&self) -> &'static str {
        "ssh_worker_runner"
    }

    fn runner_version(&self) -> &'static str {
        "v1"
    }

    fn capabilities(&self) -> &'static [ExecutionBackendRunnerCapability] {
        SSH_WORKER_RUNNER_CAPABILITIES
    }

    fn run_process<'a>(
        &'a self,
        request: ExecutionBackendProcessRunRequest<'a>,
    ) -> RunnerExecutionFuture<'a> {
        if let Err(error) = validate_ssh_worker_process_input(request.config, request.input_json) {
            return Box::pin(async move {
                ssh_worker_error_outcome(
                    request.proposal_id,
                    request.tool_name,
                    request.input_json,
                    error.reason_code.as_str(),
                    error.message,
                    &self.profile,
                )
            });
        }
        self.dispatch_remote_tool(request.proposal_id, request.tool_name, request.input_json)
    }

    fn run_tool_program<'a>(
        &'a self,
        request: ExecutionBackendToolProgramRequest<'a>,
    ) -> RunnerExecutionFuture<'a> {
        self.dispatch_remote_tool(request.proposal_id, request.tool_name, request.input_json)
    }

    fn read_artifact<'a>(
        &'a self,
        request: ExecutionBackendArtifactRequest<'a>,
    ) -> RunnerExecutionFuture<'a> {
        self.dispatch_remote_tool(request.proposal_id, request.tool_name, request.input_json)
    }

    fn health_probe(&self) -> ExecutionBackendRunnerHealth {
        self.transport.health_probe(&self.profile)
    }
}

fn build_ssh_worker_rpc_request(
    profile: &SshWorkerBackendProfile,
    tool_name: &str,
    input_json: &[u8],
) -> Result<SshWorkerRpcRequestEnvelope, SshWorkerTransportError> {
    let tool_kind =
        WorkerRemoteToolKind::from_tool_name(tool_name).ok_or_else(|| SshWorkerTransportError {
            reason_code: "ssh_worker.tool_unsupported".to_owned(),
            message: format!("SSH worker RPC does not support tool {tool_name}"),
        })?;
    let input_json = std::str::from_utf8(input_json).map_err(|error| SshWorkerTransportError {
        reason_code: "ssh_worker.input.not_utf8".to_owned(),
        message: format!("SSH worker RPC input must be UTF-8 JSON: {error}"),
    })?;
    let required_capability = tool_kind.required_capability();
    if !profile.capabilities.iter().any(|capability| capability == required_capability.as_str()) {
        return Err(SshWorkerTransportError {
            reason_code: "ssh_worker.capability_missing".to_owned(),
            message: format!(
                "SSH worker profile {} does not advertise {required_capability}",
                profile.profile_id
            ),
        });
    }
    Ok(SshWorkerRpcRequestEnvelope {
        protocol: WORKER_REMOTE_TOOL_PROTOCOL.to_owned(),
        schema_version: WORKER_REMOTE_TOOL_SCHEMA_VERSION,
        request_id: Ulid::new().to_string(),
        profile_id: profile.profile_id.clone(),
        tunnel_endpoint_sha256: sha256_hex(profile.tunnel_endpoint.as_bytes()),
        tool_name: tool_name.to_owned(),
        tool_kind,
        input_json: input_json.to_owned(),
        input_json_sha256: sha256_hex(input_json.as_bytes()),
        worker_protocol: profile.worker_protocol.clone(),
        health_probe: profile.health_probe.clone(),
        negotiated_capabilities: profile.capabilities.clone(),
        workspace_strategy_digest: profile.workspace_strategy.attestation_digest_sha256(),
        artifact_transport: "ssh_worker_rpc_manifest_bundle_transfer".to_owned(),
        writeback_mode: profile.workspace_strategy.writeback.as_str().to_owned(),
    })
}

fn validate_ssh_worker_process_input(
    config: &ToolCallConfig,
    input_json: &[u8],
) -> Result<(), SshWorkerTransportError> {
    let input =
        parse_process_runner_tool_input(input_json).map_err(|error| SshWorkerTransportError {
            reason_code: "ssh_worker.process.invalid_input".to_owned(),
            message: format!("SshWorkerRunner rejected process input: {error}"),
        })?;
    if input.background || input.keep_running_after_run {
        return Err(SshWorkerTransportError {
            reason_code: "ssh_worker.process.background_unsupported".to_owned(),
            message: "SshWorkerRunner does not support background process handles yet".to_owned(),
        });
    }
    if !input.prepend_path.is_empty() {
        return Err(SshWorkerTransportError {
            reason_code: "ssh_worker.process.prepend_path_unsupported".to_owned(),
            message: "SshWorkerRunner does not accept host PATH injection".to_owned(),
        });
    }
    if input.env.keys().any(|name| is_sensitive_key(name.as_str())) {
        return Err(SshWorkerTransportError {
            reason_code: "ssh_worker.process.secret_env_denied".to_owned(),
            message: "SshWorkerRunner rejects sensitive env names in tool input".to_owned(),
        });
    }
    let command = input.command.trim();
    if command.is_empty() || command.chars().any(char::is_whitespace) {
        return Err(SshWorkerTransportError {
            reason_code: "ssh_worker.process.invalid_command".to_owned(),
            message: "SshWorkerRunner requires a single executable token in command".to_owned(),
        });
    }
    if ssh_worker_command_is_raw_shell(command) {
        return Err(SshWorkerTransportError {
            reason_code: "ssh_worker.process.raw_shell_denied".to_owned(),
            message: "SshWorkerRunner uses worker RPC envelopes and refuses raw shell dispatch"
                .to_owned(),
        });
    }
    if !config
        .process_runner
        .allowed_executables
        .iter()
        .any(|entry| docker_command_allowlist_matches(entry, command))
    {
        return Err(SshWorkerTransportError {
            reason_code: "ssh_worker.process.executable_denied".to_owned(),
            message: format!("SshWorkerRunner command {command:?} is not in the process allowlist"),
        });
    }
    Ok(())
}

fn ssh_worker_command_is_raw_shell(command: &str) -> bool {
    let command_name =
        Path::new(command).file_name().and_then(|name| name.to_str()).unwrap_or(command);
    matches!(
        command_name.to_ascii_lowercase().as_str(),
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

fn ssh_worker_outcome_from_rpc_result(
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    profile: &SshWorkerBackendProfile,
    request: &SshWorkerRpcRequestEnvelope,
    result: SshWorkerRpcResultEnvelope,
) -> ToolExecutionOutcome {
    if result.protocol != WORKER_REMOTE_TOOL_PROTOCOL
        || result.schema_version != WORKER_REMOTE_TOOL_SCHEMA_VERSION
        || result.request_id != request.request_id
    {
        return ssh_worker_error_outcome(
            proposal_id,
            tool_name,
            input_json,
            "ssh_worker.result.binding_mismatch",
            "SSH worker RPC result did not match the request envelope".to_owned(),
            profile,
        );
    }
    if sha256_hex(result.output_json.as_bytes()) != result.output_json_sha256 {
        return ssh_worker_error_outcome(
            proposal_id,
            tool_name,
            input_json,
            "ssh_worker.result.digest_mismatch",
            "SSH worker RPC output digest mismatch".to_owned(),
            profile,
        );
    }
    if let Some(bundle) = result.patch_bundle.as_ref() {
        if let Err(message) = validate_remote_workspace_patch_bundle(bundle, request) {
            return ssh_worker_bundle_contract_error_outcome(
                proposal_id,
                tool_name,
                input_json,
                profile,
                request,
                &result,
                message,
            );
        }
    }
    if !result.cleanup_report.is_verified() {
        let cleanup_report = result.cleanup_report.clone();
        let output = json!({
            "success": false,
            "event": "execution_backend.ssh_worker_runner",
            "status": "cleanup_failed",
            "backend": ExecutionBackendPreference::SshTunnel.as_str(),
            "profile_id": profile.profile_id,
            "protocol": WORKER_REMOTE_TOOL_PROTOCOL,
            "reason_code": "ssh_worker.cleanup.incomplete",
            "cleanup_report": cleanup_report.clone(),
        });
        let output_json = serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec());
        let manifest = execution_attestation_manifest(ExecutionAttestationManifestInput {
            backend_id: ExecutionBackendPreference::SshTunnel.as_str(),
            runner_id: "ssh_worker_runner",
            runner_version: "v1",
            workspace_strategy_digest: request.workspace_strategy_digest.clone(),
            input_manifest_sha256: request.input_json_sha256.clone(),
            output_manifest_sha256: result.output_manifest_sha256,
            cleanup: worker_cleanup_evidence("ssh_worker_rpc_cleanup", &cleanup_report),
            egress_posture: "operator_managed_ssh_tunnel_worker_rpc".to_owned(),
        });
        return build_tool_execution_outcome_with_manifest(
            proposal_id,
            tool_name,
            input_json,
            false,
            output_json,
            "SSH worker RPC cleanup report was incomplete".to_owned(),
            false,
            "ssh_tunnel".to_owned(),
            "ssh_worker_rpc_cleanup_failed".to_owned(),
            manifest,
        );
    }
    let output_json = ssh_worker_output_json_with_writeback(proposal_id, request, &result);
    let output_manifest_sha256 =
        ssh_worker_output_manifest_sha256(request, &result, output_json.as_slice());
    let trajectory_label =
        ssh_worker_trajectory_label(profile, request, &result, output_manifest_sha256.as_str());
    let error = result.error.clone().unwrap_or_default();
    let cleanup_report = result.cleanup_report.clone();
    let manifest = execution_attestation_manifest(ExecutionAttestationManifestInput {
        backend_id: ExecutionBackendPreference::SshTunnel.as_str(),
        runner_id: "ssh_worker_runner",
        runner_version: "v1",
        workspace_strategy_digest: request.workspace_strategy_digest.clone(),
        input_manifest_sha256: request.input_json_sha256.clone(),
        output_manifest_sha256: output_manifest_sha256.clone(),
        cleanup: worker_cleanup_evidence("ssh_worker_rpc_cleanup", &cleanup_report),
        egress_posture: "operator_managed_ssh_tunnel_worker_rpc".to_owned(),
    });
    build_tool_execution_outcome_with_manifest(
        proposal_id,
        tool_name,
        input_json,
        result.success,
        output_json,
        error,
        false,
        "ssh_tunnel".to_owned(),
        trajectory_label,
        manifest,
    )
}

fn validate_remote_workspace_patch_bundle(
    bundle: &WorkspacePatchBundle,
    request: &SshWorkerRpcRequestEnvelope,
) -> Result<(), String> {
    if bundle.schema_version < 2 {
        return Err("remote patch bundle schema_version must be at least 2".to_owned());
    }
    if bundle.backend_id != ExecutionBackendPreference::SshTunnel.as_str()
        && bundle.backend_id != ExecutionBackendPreference::NetworkedWorker.as_str()
    {
        return Err(format!(
            "remote patch bundle backend_id is unsupported: {}",
            bundle.backend_id
        ));
    }
    if !bundle.reviewed || !bundle.merge_preview.review_required_before_apply {
        return Err("remote patch bundle must be marked review-required before apply".to_owned());
    }
    if bundle.source_manifest.authoritative_workspace_mutation
        || bundle.merge_preview.authoritative_workspace_mutation
    {
        return Err(
            "remote patch bundle must not claim direct authoritative workspace mutation".to_owned()
        );
    }
    if bundle.source_manifest.writeback_mode != WorkspaceWritebackMode::PatchBundle.as_str()
        || request.writeback_mode != WorkspaceWritebackMode::PatchBundle.as_str()
    {
        return Err("remote patch bundle writeback_mode must be patch_bundle".to_owned());
    }
    if bundle.source_manifest.workspace_strategy_digest != request.workspace_strategy_digest {
        return Err(
            "remote patch bundle workspace strategy digest does not match request".to_owned()
        );
    }
    if !bundle.symlink_guard_result.checked || bundle.symlink_guard_result.status != "passed" {
        return Err("remote patch bundle symlink guard did not pass".to_owned());
    }
    if !bundle.binary_file_policy.text_only || !bundle.binary_file_policy.rejected_paths.is_empty()
    {
        return Err("remote patch bundle contains unsupported binary file changes".to_owned());
    }
    if bundle.conflict_summary.status != "clean" || bundle.conflict_summary.stale_view_possible {
        return Err("remote patch bundle conflict summary is not clean".to_owned());
    }
    if bundle.patch_sha256 != bundle.merge_preview.patch_sha256 {
        return Err("remote patch bundle merge preview hash does not match patch hash".to_owned());
    }
    if bundle.file_count != bundle.files.len() {
        return Err("remote patch bundle file_count does not match file list".to_owned());
    }
    if !bundle.rollback_plan.checkpoint_pair_required
        || !bundle.rollback_plan.preflight_checkpoint_required
        || !bundle.rollback_plan.restore_report_required
    {
        return Err("remote patch bundle rollback plan must require checkpoint restore".to_owned());
    }
    Ok(())
}

fn ssh_worker_output_json_with_writeback(
    proposal_id: &str,
    request: &SshWorkerRpcRequestEnvelope,
    result: &SshWorkerRpcResultEnvelope,
) -> Vec<u8> {
    let Some(bundle) = result.patch_bundle.as_ref() else {
        return result.output_json.as_bytes().to_vec();
    };
    let mut output = match serde_json::from_str::<serde_json::Value>(result.output_json.as_str()) {
        Ok(serde_json::Value::Object(map)) => serde_json::Value::Object(map),
        _ => return result.output_json.as_bytes().to_vec(),
    };
    let Some(object) = output.as_object_mut() else {
        return result.output_json.as_bytes().to_vec();
    };
    let tool_job_ref = format!("{proposal_id}:{}", request.request_id);
    let scoped_bundle = workspace_patch_bundle_for_tool_job(bundle, tool_job_ref.as_str());
    object.insert(
        "workspace_writeback".to_owned(),
        json!({
            "mode": WorkspaceWritebackMode::PatchBundle.as_str(),
            "authoritative_workspace_mutation": false,
            "remote_backend": request.profile_id,
            "artifact_transport": request.artifact_transport,
            "patch_bundle": scoped_bundle,
        }),
    );
    serde_json::to_vec(&output).unwrap_or_else(|_| result.output_json.as_bytes().to_vec())
}

fn ssh_worker_output_manifest_sha256(
    request: &SshWorkerRpcRequestEnvelope,
    result: &SshWorkerRpcResultEnvelope,
    final_output_json: &[u8],
) -> String {
    let manifest = json!({
        "schema_version": 1,
        "worker_output_manifest_sha256": result.output_manifest_sha256,
        "worker_output_json_sha256": result.output_json_sha256,
        "final_output_json_sha256": sha256_hex(final_output_json),
        "workspace_strategy_digest": request.workspace_strategy_digest,
        "writeback_mode": request.writeback_mode,
        "patch_bundle_sha256": result.patch_bundle.as_ref().map(|bundle| bundle.patch_sha256.as_str()),
        "patch_bundle_source": result.patch_bundle.as_ref().map(|bundle| &bundle.source_manifest),
    });
    sha256_hex(serde_json::to_vec(&manifest).unwrap_or_default().as_slice())
}

fn ssh_worker_trajectory_label(
    profile: &SshWorkerBackendProfile,
    request: &SshWorkerRpcRequestEnvelope,
    result: &SshWorkerRpcResultEnvelope,
    output_manifest_sha256: &str,
) -> String {
    let mut label = format!(
        "ssh_worker_rpc;profile_id={};workspace_strategy_sha256={};output_manifest_sha256={}",
        profile.profile_id, request.workspace_strategy_digest, output_manifest_sha256
    );
    if let Some(bundle) = result.patch_bundle.as_ref() {
        label.push_str(";workspace_writeback=patch_bundle;patch_bundle_sha256=");
        label.push_str(bundle.patch_sha256.as_str());
    }
    label
}

fn ssh_worker_bundle_contract_error_outcome(
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    profile: &SshWorkerBackendProfile,
    request: &SshWorkerRpcRequestEnvelope,
    result: &SshWorkerRpcResultEnvelope,
    message: String,
) -> ToolExecutionOutcome {
    let output = json!({
        "success": false,
        "event": "execution_backend.ssh_worker_runner",
        "status": "patch_bundle_rejected",
        "backend": ExecutionBackendPreference::SshTunnel.as_str(),
        "profile_id": profile.profile_id,
        "protocol": WORKER_REMOTE_TOOL_PROTOCOL,
        "reason_code": "ssh_worker.patch_bundle.contract_invalid",
        "repair_hint": "Return a reviewable patch_bundle writeback with no direct authoritative workspace mutation.",
    });
    let output_json = serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec());
    let manifest = execution_attestation_manifest(ExecutionAttestationManifestInput {
        backend_id: ExecutionBackendPreference::SshTunnel.as_str(),
        runner_id: "ssh_worker_runner",
        runner_version: "v1",
        workspace_strategy_digest: request.workspace_strategy_digest.clone(),
        input_manifest_sha256: request.input_json_sha256.clone(),
        output_manifest_sha256: sha256_hex(output_json.as_slice()),
        cleanup: worker_cleanup_evidence("ssh_worker_rpc_cleanup", &result.cleanup_report),
        egress_posture: "operator_managed_ssh_tunnel_worker_rpc".to_owned(),
    });
    build_tool_execution_outcome_with_manifest(
        proposal_id,
        tool_name,
        input_json,
        false,
        output_json,
        redact_diagnostic_text(message.as_str()),
        false,
        "ssh_tunnel".to_owned(),
        "ssh_worker_rpc_patch_bundle_rejected".to_owned(),
        manifest,
    )
}

fn ssh_worker_error_outcome(
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    reason_code: &str,
    message: String,
    profile: &SshWorkerBackendProfile,
) -> ToolExecutionOutcome {
    let output = json!({
        "success": false,
        "event": "execution_backend.ssh_worker_runner",
        "status": "unavailable",
        "backend": ExecutionBackendPreference::SshTunnel.as_str(),
        "profile_id": profile.profile_id,
        "protocol": WORKER_REMOTE_TOOL_PROTOCOL,
        "tunnel_endpoint_sha256": sha256_hex(profile.tunnel_endpoint.as_bytes()),
        "reason_code": reason_code,
        "repair_hint": "Attach an operator-managed palyra-worker-rpc/v1 SSH tunnel profile or select a different execution backend.",
    });
    let redacted_message = redact_diagnostic_text(message.as_str());
    let output_json = serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec());
    let manifest = execution_attestation_manifest(ExecutionAttestationManifestInput {
        backend_id: ExecutionBackendPreference::SshTunnel.as_str(),
        runner_id: "ssh_worker_runner",
        runner_version: "v1",
        workspace_strategy_digest: profile.workspace_strategy.attestation_digest_sha256(),
        input_manifest_sha256: sha256_hex(input_json),
        output_manifest_sha256: sha256_hex(output_json.as_slice()),
        cleanup: worker_not_started_cleanup_evidence("ssh_worker_rpc_preflight", reason_code),
        egress_posture: "operator_managed_ssh_tunnel_worker_rpc".to_owned(),
    });
    build_tool_execution_outcome_with_manifest(
        proposal_id,
        tool_name,
        input_json,
        false,
        output_json,
        redacted_message,
        false,
        "ssh_tunnel".to_owned(),
        "ssh_worker_rpc_unavailable".to_owned(),
        manifest,
    )
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

/// Projects backend inventory into model/operator environment guidance.
#[must_use]
pub(crate) fn build_environment_inventory(
    inventory: &[ExecutionBackendInventoryRecord],
    active_workspace_root: Option<&Path>,
) -> Vec<EnvironmentInventoryRecord> {
    inventory
        .iter()
        .map(|record| environment_inventory_record(record, active_workspace_root))
        .collect()
}

fn environment_inventory_record(
    record: &ExecutionBackendInventoryRecord,
    active_workspace_root: Option<&Path>,
) -> EnvironmentInventoryRecord {
    let workspace_root = model_workspace_root_label(record, active_workspace_root);
    let persistence = backend_workspace_persistence(&record.workspace_strategy);
    let egress_posture = backend_egress_posture(record);
    let env_posture = backend_env_posture(record);
    let environment_epoch = environment_epoch(record, workspace_root.as_str());
    EnvironmentInventoryRecord {
        schema_version: 1,
        backend_id: record.backend_id.clone(),
        backend_type: record.workspace_strategy.kind.as_str().to_owned(),
        state: record.state,
        selected_by_default: record.selected_by_default,
        workspace_root: workspace_root.clone(),
        persistence: persistence.to_owned(),
        writeback_mode: record.workspace_strategy.writeback,
        cleanup_strategy: record.cleanup_strategy.clone(),
        egress_posture: egress_posture.to_owned(),
        env_posture: env_posture.to_owned(),
        environment_epoch,
        model_guidance: format!(
            "Tools run on backend={} with workspace={} persistence={} writeback={} egress={} env={}.",
            record.backend_id,
            workspace_root,
            persistence,
            record.workspace_strategy.writeback.as_str(),
            egress_posture,
            env_posture,
        ),
        operator_detail: format!(
            "backend={} state={} selectable={} cleanup={} artifact_transport={} workspace_strategy_sha256={}",
            record.backend_id,
            record.state.as_str(),
            record.selectable,
            record.cleanup_strategy,
            record.artifact_transport,
            record.workspace_strategy.attestation_digest_sha256(),
        ),
        redaction_level: "metadata_only".to_owned(),
    }
}

fn model_workspace_root_label(
    record: &ExecutionBackendInventoryRecord,
    active_workspace_root: Option<&Path>,
) -> String {
    match record.workspace_strategy.kind {
        WorkspaceStrategyKind::DaemonWorkspaceRoot => active_workspace_root
            .and_then(Path::file_name)
            .and_then(|name| name.to_str())
            .map(|name| format!("/workspace/{name}"))
            .unwrap_or_else(|| "/workspace".to_owned()),
        WorkspaceStrategyKind::GitWorktree => "/workspace/worktree".to_owned(),
        WorkspaceStrategyKind::EphemeralCopy => "/workspace/ephemeral-copy".to_owned(),
        WorkspaceStrategyKind::ContainerVolume => "/workspace".to_owned(),
        WorkspaceStrategyKind::RemoteLeaseWorkspace => "remote://lease/workspace".to_owned(),
        WorkspaceStrategyKind::OperatorManagedRemote => {
            "remote://operator-managed/workspace".to_owned()
        }
    }
}

fn backend_workspace_persistence(strategy: &WorkspaceStrategyDescriptor) -> &'static str {
    match strategy.kind {
        WorkspaceStrategyKind::DaemonWorkspaceRoot => "persistent_host_workspace",
        WorkspaceStrategyKind::GitWorktree => "persistent_until_worktree_cleanup",
        WorkspaceStrategyKind::EphemeralCopy | WorkspaceStrategyKind::ContainerVolume => {
            "ephemeral_with_patch_bundle_writeback"
        }
        WorkspaceStrategyKind::RemoteLeaseWorkspace => "lease_scoped_remote_workspace",
        WorkspaceStrategyKind::OperatorManagedRemote => "operator_managed_remote_workspace",
    }
}

fn backend_egress_posture(record: &ExecutionBackendInventoryRecord) -> &'static str {
    if record.requires_egress_proxy {
        "proxy_required"
    } else if record.capabilities.iter().any(|capability| capability == "no_network") {
        "blocked"
    } else {
        "backend_policy"
    }
}

fn backend_env_posture(record: &ExecutionBackendInventoryRecord) -> &'static str {
    if record.capabilities.iter().any(|capability| capability == "vault_scoped_secret_delivery") {
        "vault_refs_only"
    } else {
        "no_secret_material"
    }
}

fn environment_epoch(record: &ExecutionBackendInventoryRecord, workspace_root: &str) -> String {
    let payload = json!({
        "backend_id": record.backend_id,
        "state": record.state.as_str(),
        "workspace_root": workspace_root,
        "workspace_strategy": &record.workspace_strategy,
        "writeback": record.workspace_strategy.writeback.as_str(),
        "cleanup": record.cleanup_strategy.as_str(),
        "egress_proxy": record.requires_egress_proxy,
    });
    sha256_hex(serde_json::to_vec(&payload).unwrap_or_default().as_slice())
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
            "Preview backend is enabled. Operators must configure an SSH worker RPC profile and attach the tunnel before remote dispatch can run."
                .to_owned()
        } else {
            format!(
                "Preview backend is disabled. Set {}=1 before advertising SSH worker RPC workflows.",
                EXECUTION_BACKEND_SSH_TUNNEL_ROLLOUT_ENV
            )
        },
        executor_label: None,
        rollout_flag: Some(EXECUTION_BACKEND_SSH_TUNNEL_ROLLOUT_ENV.to_owned()),
        rollout_source: Some(rollout.source),
        rollout_enabled: rollout.enabled,
        capabilities: vec![
            "ssh_worker_rpc_envelope".to_owned(),
            "worker_capability_negotiation".to_owned(),
            "vault_scoped_secret_delivery".to_owned(),
            "manifest_attested_artifact_transport".to_owned(),
            "patch_bundle_writeback".to_owned(),
        ],
        tradeoffs: vec![
            "Uses only the palyra-worker-rpc/v1 envelope over an operator-managed tunnel"
                .to_owned(),
            "Requires vault-backed SSH identity handles and explicit tunnel health negotiation"
                .to_owned(),
        ],
        requires_attestation: true,
        requires_egress_proxy: false,
        attestation_mode: BackendAttestationMode::VaultIdentity,
        workspace_strategy: WorkspaceStrategyDescriptor::remote_lease_workspace(),
        workspace_scope_mode: "ssh_worker_remote_lease_scope".to_owned(),
        artifact_transport: "ssh_worker_rpc_manifest_bundle_transfer".to_owned(),
        cleanup_strategy: "worker_rpc_cleanup_attestation".to_owned(),
        supports_cancellation: true,
        supports_cleanup: true,
        health_probe: "ssh_worker_rpc_health_and_capability_negotiation".to_owned(),
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
        fs,
        path::PathBuf,
        sync::{Arc, Mutex},
    };

    use palyra_common::feature_rollouts::FeatureRolloutSource;
    use palyra_common::runtime_preview::RuntimePreviewMode;
    use palyra_common::workspace_patch::{
        apply_workspace_patch, WorkspacePatchLimits, WorkspacePatchRedactionPolicy,
        WorkspacePatchRequest,
    };
    use palyra_vault::{
        BackendPreference as VaultBackendPreference, Vault, VaultConfig, VaultScope,
    };
    use palyra_workerd::{
        WorkerCleanupReport, WorkerFleetPolicy, WorkerFleetSnapshot, WorkerRemoteToolKind,
        WORKER_REMOTE_TOOL_PROTOCOL, WORKER_REMOTE_TOOL_SCHEMA_VERSION,
    };

    use crate::config::{
        ExecutionBackendContainerEnvBindingConfig, ExecutionBackendContainerProfileConfig,
        ExecutionBackendContainerResourceLimitsConfig,
        ExecutionBackendContainerWorkspaceMountConfig, ExecutionBackendProfileConfig,
        ExecutionBackendProfilesConfig, ExecutionBackendSshWorkerProfileConfig,
        NetworkedWorkersConfig,
    };
    use crate::journal::{ToolJobRecord, ToolJobState};
    use crate::sandbox_runner::{
        process_runner_executor_name, process_runner_sandbox_enforcement_label,
        EgressEnforcementMode, SandboxProcessRunnerPolicy, SandboxProcessRunnerTier,
    };
    use crate::tool_protocol::ToolCallConfig;
    use crate::wasm_plugin_runner::WasmPluginRunnerPolicy;

    use super::{
        apply_docker_cli_preflight_probe, build_environment_inventory,
        build_execution_backend_inventory_with_docker_rollout,
        build_execution_backend_inventory_with_rollout, build_execution_backend_preflight_report,
        build_execution_backend_status_reports, parse_execution_backend_preference,
        plan_stuck_tool_job_recovery, prepare_docker_run_plan, resolve_execution_backend,
        resolve_execution_backend_for_request, sha256_hex, validate_execution_backend_selection,
        ContainerBackendProfile, ContainerEnvBinding, ContainerEnvSourceKind, ContainerMountPolicy,
        ContainerNetworkPolicy, ContainerResourceLimits, ContainerRuntimeKind,
        DockerCleanupAttestation, DockerEngine, DockerEngineError, DockerEngineFuture,
        DockerResourceUsage, DockerRunPlan, DockerRunReport, DockerRunner, ExecutionBackend,
        ExecutionBackendHealthStatus, ExecutionBackendPreference,
        ExecutionBackendProcessRunRequest, ExecutionBackendResolutionRequest,
        ExecutionBackendRunner, ExecutionBackendRunnerCapability, ExecutionBackendRunnerHealth,
        ExecutionBackendRunnerRegistry, ExecutionBackendState, FeatureRolloutSetting,
        LocalSandboxRunner, OperatorManagedSshTunnelTransport, SshWorkerBackendProfile,
        SshWorkerRpcFuture, SshWorkerRpcRequestEnvelope, SshWorkerRpcResultEnvelope,
        SshWorkerRpcTransport, SshWorkerRunner, StuckToolJobRecoveryAction, WorkspacePatchBundle,
        WorkspacePatchBundleBinaryFilePolicy, WorkspacePatchBundleCheckpointPair,
        WorkspacePatchBundleConflictSummary, WorkspacePatchBundleMergePreview,
        WorkspacePatchBundleRollbackPlan, WorkspacePatchBundleSourceManifest,
        WorkspacePatchBundleSymlinkGuardResult, WorkspacePatchBundleTouchedPath,
        WorkspacePatchBundleVerificationState, WorkspaceStrategyDescriptor, WorkspaceStrategyKind,
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

    fn temp_vault_with_secret(
        scope: VaultScope,
        key: &str,
        value: &[u8],
    ) -> (tempfile::TempDir, Vault) {
        let tempdir = tempfile::tempdir().expect("vault tempdir should be created");
        let vault = Vault::open_with_config(VaultConfig {
            root: Some(tempdir.path().join("vault")),
            identity_store_root: Some(tempdir.path().join("identity")),
            backend_preference: VaultBackendPreference::EncryptedFile,
            max_secret_bytes: 64 * 1024,
        })
        .expect("test vault should open");
        vault.put_secret(&scope, key, value).expect("test secret should be stored");
        (tempdir, vault)
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
            ssh_worker: None,
        }
    }

    fn safe_ssh_worker_profile() -> SshWorkerBackendProfile {
        SshWorkerBackendProfile {
            profile_id: "ssh-worker".to_owned(),
            tunnel_endpoint: "127.0.0.1:7142".to_owned(),
            host_handle: "vault://ssh/host".to_owned(),
            user_handle: "identity://ssh/user".to_owned(),
            identity_handle: "vault://ssh/key".to_owned(),
            host_trust_handle: "vault://ssh/known-host".to_owned(),
            worker_protocol: "palyra-worker-rpc/v1".to_owned(),
            health_probe: "ssh_worker_rpc_health".to_owned(),
            capabilities: vec![
                "tool:palyra.process.run".to_owned(),
                "tool:palyra.artifact.read".to_owned(),
            ],
            workspace_strategy: WorkspaceStrategyDescriptor::remote_lease_workspace(),
        }
    }

    fn safe_ssh_worker_profile_config(id: &str, enabled: bool) -> ExecutionBackendProfileConfig {
        ExecutionBackendProfileConfig {
            id: id.to_owned(),
            enabled,
            kind: "ssh_tunnel".to_owned(),
            container: None,
            ssh_worker: Some(ExecutionBackendSshWorkerProfileConfig {
                tunnel_endpoint: "127.0.0.1:7142".to_owned(),
                host_handle: "vault://ssh/host".to_owned(),
                user_handle: "identity://ssh/user".to_owned(),
                identity_handle: "vault://ssh/key".to_owned(),
                host_trust_handle: "vault://ssh/known-host".to_owned(),
                worker_protocol: "palyra-worker-rpc/v1".to_owned(),
                health_probe: "ssh_worker_rpc_health".to_owned(),
                capabilities: vec!["tool:palyra.process.run".to_owned()],
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

    fn test_workspace_patch_bundle(
        backend_id: &str,
        workspace_strategy_digest: String,
    ) -> WorkspacePatchBundle {
        let patch_sha256 =
            "2222222222222222222222222222222222222222222222222222222222222222".to_owned();
        WorkspacePatchBundle {
            schema_version: 2,
            backend_id: backend_id.to_owned(),
            source_manifest: WorkspacePatchBundleSourceManifest {
                source_kind: format!("{backend_id}_workspace"),
                source_id: "test-source".to_owned(),
                source_digest_sha256:
                    "3333333333333333333333333333333333333333333333333333333333333333".to_owned(),
                workspace_strategy_digest,
                artifact_transport: "test_patch_bundle_transfer".to_owned(),
                writeback_mode: WorkspaceWritebackMode::PatchBundle.as_str().to_owned(),
                authoritative_workspace_mutation: false,
            },
            reviewed: true,
            patch_sha256: patch_sha256.clone(),
            file_count: 2,
            files: vec!["a.txt".to_owned(), "b.txt".to_owned()],
            touched_paths: vec![
                WorkspacePatchBundleTouchedPath {
                    path: "a.txt".to_owned(),
                    workspace_root_index: 0,
                    operation: "replace".to_owned(),
                    moved_from: None,
                },
                WorkspacePatchBundleTouchedPath {
                    path: "b.txt".to_owned(),
                    workspace_root_index: 0,
                    operation: "create".to_owned(),
                    moved_from: None,
                },
            ],
            symlink_guard_result: WorkspacePatchBundleSymlinkGuardResult {
                checked: true,
                status: "passed".to_owned(),
                rejected_paths: Vec::new(),
            },
            binary_file_policy: WorkspacePatchBundleBinaryFilePolicy {
                mode: "reject_binary_text_patch_only".to_owned(),
                text_only: true,
                rejected_paths: Vec::new(),
            },
            conflict_summary: WorkspacePatchBundleConflictSummary {
                status: "clean".to_owned(),
                stale_view_possible: false,
                conflicting_paths: Vec::new(),
            },
            verification_stale_state: WorkspacePatchBundleVerificationState {
                status: "pending_after_bundle_apply".to_owned(),
                reason_codes: vec!["verification.pending_after_patch_bundle_apply".to_owned()],
                changed_paths: vec!["a.txt".to_owned(), "b.txt".to_owned()],
            },
            merge_preview: WorkspacePatchBundleMergePreview {
                mode: "dry_run_apply_patch".to_owned(),
                apply_tool: "palyra.fs.apply_patch".to_owned(),
                dry_run_success: true,
                review_required_before_apply: true,
                authoritative_workspace_mutation: false,
                files_changed: 2,
                patch_sha256: patch_sha256.clone(),
                redacted_preview: "*** Begin Patch\n*** End Patch\n".to_owned(),
            },
            rollback_plan: WorkspacePatchBundleRollbackPlan {
                mode: "workspace_checkpoint_restore".to_owned(),
                checkpoint_pair_required: true,
                preflight_checkpoint_required: true,
                restore_report_required: true,
                restore_scope_kind: "workspace".to_owned(),
                target_paths: vec!["a.txt".to_owned(), "b.txt".to_owned()],
            },
            checkpoint_pair: WorkspacePatchBundleCheckpointPair {
                status: "required_on_apply".to_owned(),
                tool_job_ref: None,
                mutation_id: None,
                preflight_checkpoint_id: None,
                post_change_checkpoint_id: None,
                restore_report_id: None,
            },
            redacted_preview: "*** Begin Patch\n*** End Patch\n".to_owned(),
            patch_document: String::new(),
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

    #[derive(Debug, Clone)]
    struct FakeSshWorkerTransport {
        requests: Arc<Mutex<Vec<SshWorkerRpcRequestEnvelope>>>,
        result_json: serde_json::Value,
        patch_bundle: Option<WorkspacePatchBundle>,
    }

    impl FakeSshWorkerTransport {
        fn new(
            result_json: serde_json::Value,
        ) -> (Self, Arc<Mutex<Vec<SshWorkerRpcRequestEnvelope>>>) {
            let requests = Arc::new(Mutex::new(Vec::new()));
            (Self { requests: Arc::clone(&requests), result_json, patch_bundle: None }, requests)
        }

        fn new_with_patch_bundle(
            result_json: serde_json::Value,
            patch_bundle: WorkspacePatchBundle,
        ) -> (Self, Arc<Mutex<Vec<SshWorkerRpcRequestEnvelope>>>) {
            let requests = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    requests: Arc::clone(&requests),
                    result_json,
                    patch_bundle: Some(patch_bundle),
                },
                requests,
            )
        }
    }

    impl SshWorkerRpcTransport for FakeSshWorkerTransport {
        fn health_probe(&self, _profile: &SshWorkerBackendProfile) -> ExecutionBackendRunnerHealth {
            ExecutionBackendRunnerHealth {
                backend_id: ExecutionBackendPreference::SshTunnel.as_str().to_owned(),
                status: ExecutionBackendHealthStatus::Healthy,
                reason_code: "runner.health.ssh_worker.fake_ready".to_owned(),
                summary: "fake SSH worker transport is ready".to_owned(),
            }
        }

        fn execute<'a>(
            &'a self,
            _profile: &'a SshWorkerBackendProfile,
            request: SshWorkerRpcRequestEnvelope,
        ) -> SshWorkerRpcFuture<'a> {
            let requests = Arc::clone(&self.requests);
            let result_json = self.result_json.clone();
            let patch_bundle = self.patch_bundle.clone();
            Box::pin(async move {
                requests.lock().expect("fake ssh requests").push(request.clone());
                let output_json =
                    serde_json::to_string(&result_json).expect("fake ssh output should serialize");
                Ok(SshWorkerRpcResultEnvelope {
                    protocol: WORKER_REMOTE_TOOL_PROTOCOL.to_owned(),
                    schema_version: WORKER_REMOTE_TOOL_SCHEMA_VERSION,
                    request_id: request.request_id,
                    success: true,
                    output_json: output_json.clone(),
                    output_json_sha256: sha256_hex(output_json.as_bytes()),
                    error: None,
                    output_manifest_sha256: sha256_hex(b"ssh-worker-output-manifest"),
                    cleanup_report: WorkerCleanupReport {
                        removed_workspace_scope: true,
                        removed_artifacts: true,
                        removed_logs: true,
                        failure_reason: None,
                    },
                    patch_bundle,
                })
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
    fn execution_backend_parity_matrix_covers_runner_contracts() {
        let remote_tool_cases = [
            ("palyra.fs.read_file", WorkerRemoteToolKind::FsRead),
            ("palyra.fs.list_dir", WorkerRemoteToolKind::FsList),
            ("palyra.fs.search", WorkerRemoteToolKind::FsSearch),
            ("palyra.process.run", WorkerRemoteToolKind::ProcessRun),
            ("palyra.fs.apply_patch", WorkerRemoteToolKind::ApplyPatch),
            ("palyra.artifact.read", WorkerRemoteToolKind::ArtifactRead),
            ("palyra.tool_program.run", WorkerRemoteToolKind::ToolProgramRun),
        ];
        for (tool_name, expected_kind) in remote_tool_cases {
            assert_eq!(WorkerRemoteToolKind::from_tool_name(tool_name), Some(expected_kind));
            assert_eq!(expected_kind.required_capability(), format!("tool:{tool_name}"));
        }

        let local = LocalSandboxRunner;
        assert_runner_exposes(
            &local,
            &[
                ExecutionBackendRunnerCapability::RunProcess,
                ExecutionBackendRunnerCapability::RunToolProgram,
                ExecutionBackendRunnerCapability::ReadArtifact,
                ExecutionBackendRunnerCapability::CommitOrPatchBundle,
                ExecutionBackendRunnerCapability::Cancel,
                ExecutionBackendRunnerCapability::Cleanup,
                ExecutionBackendRunnerCapability::AttestationManifest,
            ],
        );
        assert_eq!(
            WorkspaceStrategyDescriptor::daemon_workspace_root().writeback,
            WorkspaceWritebackMode::PatchBundle
        );

        let (engine, _) = FakeDockerEngine::new(Ok(docker_report_success()));
        let docker = DockerRunner::new(safe_container_profile(), engine)
            .expect("safe Docker profile should build runner");
        assert_runner_exposes(
            &docker,
            &[
                ExecutionBackendRunnerCapability::RunProcess,
                ExecutionBackendRunnerCapability::OpenWorkspace,
                ExecutionBackendRunnerCapability::CommitOrPatchBundle,
                ExecutionBackendRunnerCapability::Cancel,
                ExecutionBackendRunnerCapability::Cleanup,
                ExecutionBackendRunnerCapability::AttestationManifest,
            ],
        );
        assert_eq!(
            WorkspaceStrategyDescriptor::container_volume().writeback,
            WorkspaceWritebackMode::PatchBundle
        );

        let (transport, _) = FakeSshWorkerTransport::new(serde_json::json!({"exit_code": 0}));
        let ssh = SshWorkerRunner::new(safe_ssh_worker_profile(), transport)
            .expect("safe SSH worker profile should build runner");
        assert_runner_exposes(
            &ssh,
            &[
                ExecutionBackendRunnerCapability::RunProcess,
                ExecutionBackendRunnerCapability::RunToolProgram,
                ExecutionBackendRunnerCapability::ReadArtifact,
                ExecutionBackendRunnerCapability::AttestationManifest,
            ],
        );
        assert_eq!(
            WorkspaceStrategyDescriptor::remote_lease_workspace().writeback,
            WorkspaceWritebackMode::PatchBundle
        );

        let patch_bundle = test_workspace_patch_bundle(
            ExecutionBackendPreference::Docker.as_str(),
            WorkspaceStrategyDescriptor::container_volume().attestation_digest_sha256(),
        );
        assert!(patch_bundle.reviewed);
        assert!(patch_bundle.rollback_plan.restore_report_required);
        assert_eq!(patch_bundle.checkpoint_pair.status, "required_on_apply");
        assert_eq!(patch_bundle.source_manifest.writeback_mode, "patch_bundle");
        assert!(!patch_bundle.source_manifest.authoritative_workspace_mutation);
    }

    #[test]
    fn backend_status_reports_runner_capability_and_cleanup_evidence() {
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
        let default_registry = ExecutionBackendRunnerRegistry::default();
        let reports = build_execution_backend_status_reports(&inventory, &default_registry);
        let local = reports
            .iter()
            .find(|report| report.backend_id == ExecutionBackendPreference::LocalSandbox.as_str())
            .expect("local sandbox status should exist");

        assert_eq!(local.health_status, ExecutionBackendHealthStatus::Healthy);
        assert!(local.cleanup.cleanup_supported);
        assert!(local.runner_capabilities.iter().any(|capability| capability == "run_process"));

        let docker_without_runner = reports
            .iter()
            .find(|report| report.backend_id == ExecutionBackendPreference::Docker.as_str())
            .expect("docker status should exist");

        assert_eq!(docker_without_runner.health_status, ExecutionBackendHealthStatus::Unavailable);
        assert!(
            docker_without_runner.reason_code.contains("runner_missing_run_process"),
            "{}",
            docker_without_runner.reason_code
        );
        assert!(!docker_without_runner.cleanup.cleanup_supported);

        let (engine, _) = FakeDockerEngine::new(Ok(docker_report_success()));
        let docker_runner = DockerRunner::new(safe_container_profile(), engine)
            .expect("safe Docker runner should build");
        let registry = ExecutionBackendRunnerRegistry::with_docker_runner(Box::new(docker_runner));
        let reports = build_execution_backend_status_reports(&inventory, &registry);
        let docker = reports
            .iter()
            .find(|report| report.backend_id == ExecutionBackendPreference::Docker.as_str())
            .expect("docker status should exist");

        assert!(docker.runner_capabilities.iter().any(|capability| capability == "cleanup"));
        assert!(docker.cleanup.cleanup_supported);
    }

    fn assert_runner_exposes(
        runner: &dyn ExecutionBackendRunner,
        required_capabilities: &[ExecutionBackendRunnerCapability],
    ) {
        for capability in required_capabilities {
            assert!(
                runner.capabilities().contains(capability),
                "{} should expose {}",
                runner.runner_id(),
                capability.as_str()
            );
        }
        let manifest =
            runner.attestation_manifest(&WorkspaceStrategyDescriptor::daemon_workspace_root());
        assert_eq!(manifest.runner_id, runner.runner_id());
        assert!(manifest.capabilities.iter().any(|capability| capability
            == ExecutionBackendRunnerCapability::AttestationManifest.as_str()));
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

    #[test]
    fn runner_registry_builds_ssh_worker_runner_from_profile_config() {
        let profiles = ExecutionBackendProfilesConfig {
            mode: RuntimePreviewMode::PreviewOnly,
            profiles: vec![safe_ssh_worker_profile_config("ssh-worker", true)],
        };

        let registry = ExecutionBackendRunnerRegistry::from_execution_backend_profiles(&profiles)
            .expect("valid SSH worker profile should build a registry");

        let runner = registry
            .select_runner(
                ExecutionBackendPreference::SshTunnel,
                ExecutionBackendRunnerCapability::RunProcess,
            )
            .expect("configured SSH worker runner should be selectable");
        assert_eq!(runner.runner_id(), "ssh_worker_runner");
        let health = runner.health_probe();
        assert_eq!(health.status, ExecutionBackendHealthStatus::Unavailable);
        assert_eq!(health.reason_code, "runner.health.ssh_worker.tunnel_unavailable");
    }

    #[test]
    fn runner_registry_rejects_multiple_enabled_ssh_worker_profiles() {
        let profiles = ExecutionBackendProfilesConfig {
            mode: RuntimePreviewMode::PreviewOnly,
            profiles: vec![
                safe_ssh_worker_profile_config("ssh-a", true),
                safe_ssh_worker_profile_config("ssh-b", true),
            ],
        };

        let error = ExecutionBackendRunnerRegistry::from_execution_backend_profiles(&profiles)
            .expect_err("multiple enabled SSH worker profiles must fail closed");

        assert!(error.contains("at most one SSH worker profile"), "{error}");
    }

    #[test]
    fn docker_writeback_capture_returns_patch_bundle_without_host_mutation() {
        let temp = tempfile::tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(workspace.as_path()).expect("workspace should exist");
        fs::write(workspace.join("notes.txt"), "alpha\n").expect("seed file should be written");
        let mut profile = safe_container_profile();
        profile.mounts[0].host_path = workspace.display().to_string();
        let plan = DockerRunPlan {
            profile_id: profile.profile_id,
            image: profile.image,
            image_digest_sha256: "1111111111111111111111111111111111111111111111111111111111111111"
                .to_owned(),
            workspace_strategy_digest: WorkspaceStrategyDescriptor::container_volume()
                .attestation_digest_sha256(),
            user: profile.user,
            readonly_rootfs: profile.readonly_rootfs,
            network: profile.network,
            mounts: profile.mounts,
            env: profile.env,
            env_file: None,
            background: false,
            command: "echo".to_owned(),
            args: vec!["runner-ok".to_owned()],
            working_dir: "/workspace".to_owned(),
            limits: profile.limits,
            workspace_writeback: WorkspaceWritebackMode::PatchBundle,
            cleanup_strategy: profile.cleanup_strategy,
        };

        let (run_plan, capture) = prepare_docker_run_plan(&plan)
            .expect("writable mount should be copied into a writeback capture workspace");
        let capture = capture.expect("writable mount should create a writeback capture");
        let temp_workspace = PathBuf::from(run_plan.mounts[0].host_path.as_str());
        fs::write(temp_workspace.join("notes.txt"), "beta\n")
            .expect("container workspace mutation should be simulated");
        fs::write(temp_workspace.join("new.txt"), "created\n")
            .expect("container workspace add should be simulated");

        let bundle = capture
            .finish()
            .expect("writeback capture should finish")
            .expect("workspace mutation should produce a patch bundle");

        assert_eq!(
            fs::read_to_string(workspace.join("notes.txt")).expect("host workspace should read"),
            "alpha\n",
            "host workspace must not be mutated by Docker writeback capture"
        );
        assert_eq!(bundle.file_count, 2);
        assert_eq!(bundle.schema_version, 2);
        assert_eq!(bundle.backend_id, "docker");
        assert!(bundle.reviewed);
        assert_eq!(bundle.source_manifest.writeback_mode, "patch_bundle");
        assert!(!bundle.source_manifest.authoritative_workspace_mutation);
        assert!(bundle.symlink_guard_result.checked);
        assert_eq!(bundle.symlink_guard_result.status, "passed");
        assert!(bundle.binary_file_policy.text_only);
        assert_eq!(bundle.conflict_summary.status, "clean");
        assert_eq!(bundle.verification_stale_state.status, "pending_after_bundle_apply");
        assert_eq!(bundle.merge_preview.mode, "dry_run_apply_patch");
        assert!(bundle.merge_preview.review_required_before_apply);
        assert!(!bundle.merge_preview.authoritative_workspace_mutation);
        assert_eq!(bundle.rollback_plan.mode, "workspace_checkpoint_restore");
        assert!(bundle.rollback_plan.checkpoint_pair_required);
        assert!(bundle.rollback_plan.restore_report_required);
        assert_eq!(bundle.checkpoint_pair.status, "required_on_apply");
        assert!(bundle.files.iter().any(|path| path == "notes.txt"));
        assert!(bundle.files.iter().any(|path| path == "new.txt"));
        assert!(bundle.touched_paths.iter().any(|path| path.path == "notes.txt"));
        assert_eq!(bundle.patch_sha256, bundle.merge_preview.patch_sha256);
        assert_eq!(bundle.patch_sha256, sha256_hex(bundle.patch_document.as_bytes()));
        assert!(bundle.redacted_preview.contains("*** Replace File: notes.txt"));
        assert!(bundle.redacted_preview.contains("*** Add File: new.txt"));

        let dry_run = apply_workspace_patch(
            std::slice::from_ref(&workspace),
            &WorkspacePatchRequest {
                patch: bundle.patch_document.clone(),
                dry_run: true,
                redaction_policy: WorkspacePatchRedactionPolicy::default(),
            },
            &WorkspacePatchLimits::default(),
        )
        .expect("captured patch bundle should dry-run apply to authoritative workspace");
        assert_eq!(dry_run.files_touched.len(), 2);
        assert_eq!(dry_run.patch_sha256, bundle.patch_sha256);
    }

    #[cfg(unix)]
    #[test]
    fn docker_writeback_capture_rejects_symlink_paths() {
        let temp = tempfile::tempdir().expect("tempdir should be created");
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(workspace.as_path()).expect("workspace should exist");
        fs::write(workspace.join("target.txt"), "alpha\n").expect("seed file should be written");
        std::os::unix::fs::symlink("target.txt", workspace.join("link.txt"))
            .expect("symlink should be created");
        let mut profile = safe_container_profile();
        profile.mounts[0].host_path = workspace.display().to_string();
        let plan = DockerRunPlan {
            profile_id: profile.profile_id,
            image: profile.image,
            image_digest_sha256: "1111111111111111111111111111111111111111111111111111111111111111"
                .to_owned(),
            workspace_strategy_digest: WorkspaceStrategyDescriptor::container_volume()
                .attestation_digest_sha256(),
            user: profile.user,
            readonly_rootfs: profile.readonly_rootfs,
            network: profile.network,
            mounts: profile.mounts,
            env: profile.env,
            env_file: None,
            background: false,
            command: "echo".to_owned(),
            args: vec!["runner-ok".to_owned()],
            working_dir: "/workspace".to_owned(),
            limits: profile.limits,
            workspace_writeback: WorkspaceWritebackMode::PatchBundle,
            cleanup_strategy: profile.cleanup_strategy,
        };

        let error = prepare_docker_run_plan(&plan)
            .expect_err("symlink paths must be rejected before Docker writeback starts");

        assert_eq!(error.reason_code, "docker.writeback.symlink_unsupported");
        assert!(error.message.contains("link.txt"), "{}", error.message);
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
                vault: None,
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
        let manifest = outcome
            .attestation
            .execution_manifest
            .as_ref()
            .expect("local process outcome should carry an execution manifest");
        assert_eq!(manifest.backend_id, "local_sandbox");
        assert_eq!(manifest.runner_id, "local_sandbox_runner");
        assert_eq!(
            manifest.input_manifest_sha256,
            sha256_hex(br#"{"command":"echo","args":["runner-ok"]}"#)
        );
        assert_eq!(manifest.output_manifest_sha256, sha256_hex(outcome.output_json.as_slice()));
        assert!(manifest.cleanup.success);
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
                vault: None,
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
        let manifest = outcome
            .attestation
            .execution_manifest
            .as_ref()
            .expect("Docker outcome should carry an execution manifest");
        assert_eq!(manifest.backend_id, "docker");
        assert_eq!(manifest.runner_id, "docker_runner");
        assert_eq!(
            manifest.output_manifest_sha256,
            payload["output_manifest_sha256"].as_str().expect("manifest hash")
        );
        assert_eq!(manifest.cleanup.reason_code, "docker.cleanup.ok");
        assert!(manifest.cleanup.success);

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
    async fn docker_runner_materializes_vault_env_file_without_leaking_secret() {
        let (_vault_dir, vault) =
            temp_vault_with_secret(VaultScope::Global, "api-token", b"secret-token");
        let (engine, plans) = FakeDockerEngine::new(Ok(docker_report_success()));
        let mut profile = safe_container_profile();
        profile.env = vec![ContainerEnvBinding {
            name: "API_TOKEN".to_owned(),
            source_kind: ContainerEnvSourceKind::VaultRef,
            value: "vault://global/api-token".to_owned(),
        }];
        let runner = DockerRunner::new(profile, engine).expect("safe Docker profile should build");
        let mut policy = test_policy();
        policy.allowed_executables = vec!["echo".to_owned()];
        let config = test_tool_call_config(policy);

        let outcome = runner
            .run_process(ExecutionBackendProcessRunRequest {
                config: &config,
                proposal_id: "proposal-docker-vault-env",
                tool_name: "palyra.process.run",
                input_json: br#"{"command":"echo","args":["runner-ok"]}"#,
                vault: Some(&vault),
                cancellation_requested: None,
                process_progress_sink: None,
            })
            .await;

        assert!(outcome.success, "{}", outcome.error);
        let plans = plans.lock().expect("fake Docker plans");
        let plan = plans.first().expect("Docker plan should be recorded");
        let env_file = plan.env_file.as_ref().expect("vault env should materialize env-file");
        assert_eq!(env_file.env_names, vec!["API_TOKEN"]);
        assert_eq!(env_file.vault_ref_count, 1);
        let env_file_content =
            fs::read_to_string(env_file.path.as_path()).expect("env file should exist");
        assert!(env_file_content.contains("API_TOKEN=secret-token"));

        let debug_plan = format!("{plan:?}");
        assert!(!debug_plan.contains("secret-token"));
        assert!(!debug_plan.contains("vault://global/api-token"));
        let output = String::from_utf8(outcome.output_json).expect("output JSON should be UTF-8");
        assert!(!output.contains("secret-token"));
        assert!(!output.contains("vault://global/api-token"));
        assert!(output.contains("\"vault_ref_count\":1"));
    }

    #[tokio::test]
    async fn docker_runner_fails_closed_when_vault_env_has_no_vault_runtime() {
        let (engine, plans) = FakeDockerEngine::new(Ok(docker_report_success()));
        let mut profile = safe_container_profile();
        profile.env = vec![ContainerEnvBinding {
            name: "API_TOKEN".to_owned(),
            source_kind: ContainerEnvSourceKind::VaultRef,
            value: "vault://global/api-token".to_owned(),
        }];
        let runner = DockerRunner::new(profile, engine).expect("safe Docker profile should build");
        let mut policy = test_policy();
        policy.allowed_executables = vec!["echo".to_owned()];
        let config = test_tool_call_config(policy);

        let outcome = runner
            .run_process(ExecutionBackendProcessRunRequest {
                config: &config,
                proposal_id: "proposal-docker-vault-missing",
                tool_name: "palyra.process.run",
                input_json: br#"{"command":"echo","args":["runner-ok"]}"#,
                vault: None,
                cancellation_requested: None,
                process_progress_sink: None,
            })
            .await;

        assert!(!outcome.success);
        assert_eq!(plans.lock().expect("fake Docker plans").len(), 0);
        assert!(outcome.error.contains("no vault runtime is available"));
        assert!(!outcome.error.contains("vault://global/api-token"));
        let payload: serde_json::Value =
            serde_json::from_slice(&outcome.output_json).expect("Docker error should be JSON");
        assert_eq!(payload["reason_code"], "docker.env.vault_resolution_unavailable");
    }

    #[tokio::test]
    async fn docker_runner_background_request_returns_attached_handle_metadata() {
        let (engine, plans) = FakeDockerEngine::new(Ok(docker_report_success()));
        let runner = DockerRunner::new(safe_container_profile(), engine)
            .expect("safe Docker profile should build runner");
        let mut policy = test_policy();
        policy.allowed_executables = vec!["echo".to_owned()];
        let config = test_tool_call_config(policy);

        let outcome = runner
            .run_process(ExecutionBackendProcessRunRequest {
                config: &config,
                proposal_id: "proposal-docker-background",
                tool_name: "palyra.process.run",
                input_json: br#"{"command":"echo","args":["runner-ok"],"background":true}"#,
                vault: None,
                cancellation_requested: None,
                process_progress_sink: None,
            })
            .await;

        assert!(outcome.success, "{}", outcome.error);
        let plans = plans.lock().expect("fake Docker plans");
        assert!(plans.first().expect("plan should be recorded").background);
        let payload: serde_json::Value =
            serde_json::from_slice(&outcome.output_json).expect("Docker output should be JSON");
        assert_eq!(payload["background_handle"]["requested"], true);
        assert_eq!(payload["background_handle"]["handle_kind"], "docker_attached_run");
        assert_eq!(payload["background_handle"]["cleanup_registered"], false);
    }

    #[tokio::test]
    async fn docker_runner_output_carries_reviewed_patch_bundle_writeback() {
        let mut report = docker_report_success();
        report.patch_bundle = Some(test_workspace_patch_bundle(
            ExecutionBackendPreference::Docker.as_str(),
            WorkspaceStrategyDescriptor::container_volume().attestation_digest_sha256(),
        ));
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
                vault: None,
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
        assert_eq!(
            payload["workspace_writeback"]["patch_bundle"]["merge_preview"]
                ["review_required_before_apply"],
            true
        );
        assert_eq!(
            payload["workspace_writeback"]["patch_bundle"]["rollback_plan"]
                ["restore_report_required"],
            true
        );
        assert_eq!(
            payload["workspace_writeback"]["patch_bundle"]["checkpoint_pair"]["tool_job_ref"],
            "proposal-docker-patch"
        );
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
                vault: None,
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
        let manifest = outcome
            .attestation
            .execution_manifest
            .as_ref()
            .expect("Docker cleanup failure should carry an execution manifest");
        assert_eq!(manifest.backend_id, "docker");
        assert!(!manifest.cleanup.success);
        assert_eq!(manifest.cleanup.reason_code, "docker.cleanup.remove_failed");
    }

    #[tokio::test]
    async fn ssh_worker_runner_fake_process_run_uses_worker_rpc_envelope() {
        let (transport, requests) = FakeSshWorkerTransport::new(
            serde_json::json!({"exit_code": 0, "stdout": "runner-ok\n"}),
        );
        let runner = SshWorkerRunner::new(safe_ssh_worker_profile(), transport)
            .expect("safe SSH worker profile should build runner");
        let mut policy = test_policy();
        policy.allowed_executables = vec!["echo".to_owned()];
        let config = test_tool_call_config(policy);

        let outcome = runner
            .run_process(ExecutionBackendProcessRunRequest {
                config: &config,
                proposal_id: "proposal-ssh-process",
                tool_name: "palyra.process.run",
                input_json: br#"{"command":"echo","args":["runner-ok"]}"#,
                vault: None,
                cancellation_requested: None,
                process_progress_sink: None,
            })
            .await;

        assert!(outcome.success, "{}", outcome.error);
        assert_eq!(outcome.attestation.executor, "ssh_tunnel");
        assert!(outcome
            .attestation
            .sandbox_enforcement
            .contains("ssh_worker_rpc;profile_id=ssh-worker"));
        let payload: serde_json::Value =
            serde_json::from_slice(&outcome.output_json).expect("SSH worker output should be JSON");
        assert_eq!(payload["exit_code"], 0);
        assert_eq!(payload["stdout"], "runner-ok\n");
        let manifest = outcome
            .attestation
            .execution_manifest
            .as_ref()
            .expect("SSH worker outcome should carry an execution manifest");
        assert_eq!(manifest.backend_id, "ssh_tunnel");
        assert_eq!(manifest.runner_id, "ssh_worker_runner");
        assert_eq!(manifest.cleanup.reason_code, "worker.cleanup.ok");
        assert!(manifest.cleanup.success);

        let requests = requests.lock().expect("fake SSH requests");
        assert_eq!(requests.len(), 1);
        let request = &requests[0];
        assert_eq!(request.protocol, WORKER_REMOTE_TOOL_PROTOCOL);
        assert_eq!(request.schema_version, WORKER_REMOTE_TOOL_SCHEMA_VERSION);
        assert_eq!(request.tool_name, "palyra.process.run");
        assert_eq!(request.tool_kind, WorkerRemoteToolKind::ProcessRun);
        assert_eq!(request.worker_protocol, "palyra-worker-rpc/v1");
        assert!(request
            .negotiated_capabilities
            .iter()
            .any(|capability| capability == "tool:palyra.process.run"));
        assert!(!serde_json::to_string(request)
            .expect("request should serialize")
            .contains("vault://ssh/key"));
    }

    #[tokio::test]
    async fn ssh_worker_runner_projects_remote_patch_bundle_for_review() {
        let workspace_strategy_digest =
            WorkspaceStrategyDescriptor::remote_lease_workspace().attestation_digest_sha256();
        let patch_bundle = test_workspace_patch_bundle(
            ExecutionBackendPreference::SshTunnel.as_str(),
            workspace_strategy_digest.clone(),
        );
        let (transport, requests) = FakeSshWorkerTransport::new_with_patch_bundle(
            serde_json::json!({"exit_code": 0, "stdout": "runner-ok\n"}),
            patch_bundle,
        );
        let runner = SshWorkerRunner::new(safe_ssh_worker_profile(), transport)
            .expect("safe SSH worker profile should build runner");
        let mut policy = test_policy();
        policy.allowed_executables = vec!["echo".to_owned()];
        let config = test_tool_call_config(policy);

        let outcome = runner
            .run_process(ExecutionBackendProcessRunRequest {
                config: &config,
                proposal_id: "proposal-ssh-patch",
                tool_name: "palyra.process.run",
                input_json: br#"{"command":"echo","args":["runner-ok"]}"#,
                vault: None,
                cancellation_requested: None,
                process_progress_sink: None,
            })
            .await;

        assert!(outcome.success, "{}", outcome.error);
        assert!(outcome
            .attestation
            .sandbox_enforcement
            .contains("workspace_writeback=patch_bundle"));
        assert!(outcome.attestation.sandbox_enforcement.contains("patch_bundle_sha256="));
        let payload: serde_json::Value =
            serde_json::from_slice(&outcome.output_json).expect("SSH worker output should be JSON");
        assert_eq!(payload["workspace_writeback"]["mode"], "patch_bundle");
        assert_eq!(payload["workspace_writeback"]["authoritative_workspace_mutation"], false);
        assert_eq!(payload["workspace_writeback"]["patch_bundle"]["backend_id"], "ssh_tunnel");
        assert_eq!(
            payload["workspace_writeback"]["patch_bundle"]["source_manifest"]["writeback_mode"],
            "patch_bundle"
        );
        assert_eq!(
            payload["workspace_writeback"]["patch_bundle"]["merge_preview"]
                ["review_required_before_apply"],
            true
        );
        assert_eq!(
            payload["workspace_writeback"]["patch_bundle"]["checkpoint_pair"]["tool_job_ref"],
            format!(
                "proposal-ssh-patch:{}",
                requests.lock().expect("fake SSH requests")[0].request_id
            )
        );
        let manifest = outcome
            .attestation
            .execution_manifest
            .as_ref()
            .expect("SSH worker outcome should carry an execution manifest");
        assert_eq!(manifest.workspace_strategy_digest, workspace_strategy_digest);
        assert_ne!(manifest.output_manifest_sha256, sha256_hex(b"ssh-worker-output-manifest"));

        let requests = requests.lock().expect("fake SSH requests");
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].writeback_mode, "patch_bundle");
    }

    #[tokio::test]
    async fn ssh_worker_runner_rejects_direct_remote_workspace_mutation() {
        let mut patch_bundle = test_workspace_patch_bundle(
            ExecutionBackendPreference::SshTunnel.as_str(),
            WorkspaceStrategyDescriptor::remote_lease_workspace().attestation_digest_sha256(),
        );
        patch_bundle.source_manifest.authoritative_workspace_mutation = true;
        let (transport, _) = FakeSshWorkerTransport::new_with_patch_bundle(
            serde_json::json!({"exit_code": 0, "stdout": "runner-ok\n"}),
            patch_bundle,
        );
        let runner = SshWorkerRunner::new(safe_ssh_worker_profile(), transport)
            .expect("safe SSH worker profile should build runner");
        let mut policy = test_policy();
        policy.allowed_executables = vec!["echo".to_owned()];
        let config = test_tool_call_config(policy);

        let outcome = runner
            .run_process(ExecutionBackendProcessRunRequest {
                config: &config,
                proposal_id: "proposal-ssh-direct-mutation",
                tool_name: "palyra.process.run",
                input_json: br#"{"command":"echo","args":["runner-ok"]}"#,
                vault: None,
                cancellation_requested: None,
                process_progress_sink: None,
            })
            .await;

        assert!(!outcome.success);
        assert!(outcome.error.contains("direct authoritative workspace mutation"));
        assert_eq!(outcome.attestation.sandbox_enforcement, "ssh_worker_rpc_patch_bundle_rejected");
        let payload: serde_json::Value =
            serde_json::from_slice(&outcome.output_json).expect("SSH worker output should be JSON");
        assert_eq!(payload["reason_code"], "ssh_worker.patch_bundle.contract_invalid");
    }

    #[tokio::test]
    async fn ssh_worker_runner_unavailable_tunnel_fails_closed_without_shell() {
        let runner =
            SshWorkerRunner::new(safe_ssh_worker_profile(), OperatorManagedSshTunnelTransport)
                .expect("safe SSH worker profile should build runner");
        let mut policy = test_policy();
        policy.allowed_executables = vec!["echo".to_owned()];
        let config = test_tool_call_config(policy);

        let outcome = runner
            .run_process(ExecutionBackendProcessRunRequest {
                config: &config,
                proposal_id: "proposal-ssh-unavailable",
                tool_name: "palyra.process.run",
                input_json: br#"{"command":"echo","args":["runner-ok"]}"#,
                vault: None,
                cancellation_requested: None,
                process_progress_sink: None,
            })
            .await;

        assert!(!outcome.success);
        assert!(outcome.error.contains("local fallback is denied"));
        assert!(!outcome.error.contains("vault://ssh/key"));
        assert_eq!(outcome.attestation.executor, "ssh_tunnel");
        assert_eq!(outcome.attestation.sandbox_enforcement, "ssh_worker_rpc_unavailable");
        let payload: serde_json::Value =
            serde_json::from_slice(&outcome.output_json).expect("SSH worker error should be JSON");
        assert_eq!(payload["reason_code"], "runner.unavailable.ssh_tunnel");
        assert_eq!(payload["protocol"], WORKER_REMOTE_TOOL_PROTOCOL);
        assert!(payload
            .get("tunnel_endpoint_sha256")
            .and_then(serde_json::Value::as_str)
            .is_some());
        let manifest = outcome
            .attestation
            .execution_manifest
            .as_ref()
            .expect("SSH worker unavailable outcome should carry an execution manifest");
        assert_eq!(manifest.backend_id, "ssh_tunnel");
        assert!(!manifest.cleanup.success);
        assert_eq!(manifest.cleanup.reason_code, "runner.unavailable.ssh_tunnel");
    }

    #[tokio::test]
    async fn ssh_worker_runner_rejects_raw_shell_process_input() {
        let (transport, requests) =
            FakeSshWorkerTransport::new(serde_json::json!({"exit_code": 0}));
        let runner = SshWorkerRunner::new(safe_ssh_worker_profile(), transport)
            .expect("safe SSH worker profile should build runner");
        let mut policy = test_policy();
        policy.allowed_executables = vec!["bash".to_owned()];
        let config = test_tool_call_config(policy);

        let outcome = runner
            .run_process(ExecutionBackendProcessRunRequest {
                config: &config,
                proposal_id: "proposal-ssh-shell",
                tool_name: "palyra.process.run",
                input_json: br#"{"command":"bash","args":["-lc","echo unsafe"]}"#,
                vault: None,
                cancellation_requested: None,
                process_progress_sink: None,
            })
            .await;

        assert!(!outcome.success);
        assert!(outcome.error.contains("refuses raw shell dispatch"));
        assert!(requests.lock().expect("fake SSH requests").is_empty());
        let payload: serde_json::Value =
            serde_json::from_slice(&outcome.output_json).expect("SSH worker error should be JSON");
        assert_eq!(payload["reason_code"], "ssh_worker.process.raw_shell_denied");
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
    fn environment_inventory_is_redacted_and_epoch_changes_with_backend_posture() {
        let networked_workers = NetworkedWorkersConfig::default();
        let disabled_inventory = build_execution_backend_inventory_with_docker_rollout(
            &test_policy(),
            0,
            &[],
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::from_config(false),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            &networked_workers,
            WorkerFleetSnapshot::default(),
            &WorkerFleetPolicy::default(),
        );
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

        let active_root = PathBuf::from("/private/project");
        let disabled =
            build_environment_inventory(disabled_inventory.as_slice(), Some(active_root.as_path()));
        let enabled =
            build_environment_inventory(enabled_inventory.as_slice(), Some(active_root.as_path()));
        let docker_disabled = disabled
            .iter()
            .find(|record| record.backend_id == "docker")
            .expect("Docker environment inventory should exist");
        let docker_enabled = enabled
            .iter()
            .find(|record| record.backend_id == "docker")
            .expect("Docker environment inventory should exist");

        assert_eq!(docker_disabled.workspace_root, "/workspace");
        assert_eq!(docker_disabled.writeback_mode, WorkspaceWritebackMode::PatchBundle);
        assert_eq!(docker_disabled.egress_posture, "proxy_required");
        assert_eq!(docker_disabled.redaction_level, "metadata_only");
        assert!(!docker_disabled.model_guidance.contains("/private/project"));
        assert!(!docker_disabled.model_guidance.to_ascii_lowercase().contains("token"));
        assert_ne!(docker_disabled.environment_epoch, docker_enabled.environment_epoch);

        let local = disabled
            .iter()
            .find(|record| record.backend_id == "local_sandbox")
            .expect("local environment inventory should exist");
        assert_eq!(local.workspace_root, "/workspace/project");
        assert_eq!(local.persistence, "persistent_host_workspace");
    }

    #[test]
    fn execution_backend_security_matrix_pins_required_negative_cases() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/golden/execution_backend_security_matrix.json");
        let raw = fs::read_to_string(path.as_path()).expect("security matrix fixture should load");
        let matrix: serde_json::Value =
            serde_json::from_str(raw.as_str()).expect("security matrix fixture should parse");

        assert_eq!(matrix["production_gate"], "blocked_until_suite_passed");
        let commands =
            matrix["required_commands"].as_array().expect("required_commands should be an array");
        assert!(commands.iter().any(|command| command.as_str()
            == Some("palyra qa validate --path qa/scenarios/execution_backends --json")));

        let backends = matrix["backends"].as_array().expect("backends should be an array");
        for backend_id in ["local_sandbox", "docker", "networked_worker", "ssh_tunnel"] {
            let record = backends
                .iter()
                .find(|record| record["backend_id"].as_str() == Some(backend_id))
                .unwrap_or_else(|| panic!("backend {backend_id} should be covered"));
            let cases = record["required_cases"]
                .as_array()
                .expect("required_cases should be an array")
                .iter()
                .filter_map(serde_json::Value::as_str)
                .collect::<Vec<_>>();
            assert!(
                cases.contains(&"cleanup_failure_reported"),
                "backend {backend_id} should cover cleanup failure reporting"
            );
            assert!(
                cases.iter().any(|case| {
                    matches!(
                        *case,
                        "egress_blocked_audited"
                            | "egress_proxy_posture_audited"
                            | "egress_proxy_attestation_required"
                    )
                }) || backend_id == "ssh_tunnel",
                "backend {backend_id} should cover egress posture"
            );
        }
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
            tunnel_endpoint: "127.0.0.1:7142".to_owned(),
            host_handle: "vault://ssh/host".to_owned(),
            user_handle: "identity://ssh/user".to_owned(),
            identity_handle: "vault://ssh/key".to_owned(),
            host_trust_handle: "vault://ssh/known-host".to_owned(),
            worker_protocol: "palyra-worker-rpc/v1".to_owned(),
            health_probe: "ssh_worker_rpc_health".to_owned(),
            capabilities: vec!["tool:palyra.process.run".to_owned()],
            workspace_strategy: WorkspaceStrategyDescriptor::remote_lease_workspace(),
        };
        assert!(profile.validate().is_ok());

        profile.identity_handle = "-----BEGIN PRIVATE KEY-----".to_owned();
        let error = profile.validate().expect_err("plaintext identity material must fail closed");
        assert!(error.contains("identity_handle"));
        assert!(!error.contains("BEGIN PRIVATE KEY"));
        profile.identity_handle = "vault://ssh/key".to_owned();
        profile.worker_protocol = "raw-shell".to_owned();
        assert!(profile
            .validate()
            .expect_err("raw shell protocol must fail closed")
            .contains("worker-rpc"));
    }
}
