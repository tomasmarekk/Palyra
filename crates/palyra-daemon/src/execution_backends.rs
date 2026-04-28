#![allow(dead_code)]

use std::collections::BTreeSet;

use palyra_common::feature_rollouts::{
    FeatureRolloutSetting, FeatureRolloutSource, EXECUTION_BACKEND_NETWORKED_WORKER_ROLLOUT_ENV,
    EXECUTION_BACKEND_REMOTE_NODE_ROLLOUT_ENV, EXECUTION_BACKEND_SSH_TUNNEL_ROLLOUT_ENV,
};
use palyra_common::runtime_preview::RuntimePreviewMode;
use palyra_sandbox::{current_backend_capabilities, current_backend_kind};
use palyra_workerd::{WorkerFleetPolicy, WorkerFleetSnapshot};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    config::{FeatureRolloutsConfig, NetworkedWorkersConfig},
    journal::{ToolJobRecord, ToolJobState},
    node_runtime::RegisteredNodeRecord,
    sandbox_runner::{process_runner_executor_name, SandboxProcessRunnerPolicy},
};

const NODE_HEALTHY_AFTER_MS: i64 = 5 * 60 * 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExecutionBackendPreference {
    #[default]
    Automatic,
    LocalSandbox,
    DesktopNode,
    NetworkedWorker,
    SshTunnel,
}

impl ExecutionBackendPreference {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Automatic => "automatic",
            Self::LocalSandbox => "local_sandbox",
            Self::DesktopNode => "desktop_node",
            Self::NetworkedWorker => "networked_worker",
            Self::SshTunnel => "ssh_tunnel",
        }
    }

    #[must_use]
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Automatic => "Automatic",
            Self::LocalSandbox => "Local sandbox",
            Self::DesktopNode => "Desktop node",
            Self::NetworkedWorker => "Networked worker",
            Self::SshTunnel => "SSH tunnel",
        }
    }

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
            Self::NetworkedWorker => {
                "Run work on an attested ephemeral worker with proxy-mediated egress and scoped artifact transport."
            }
            Self::SshTunnel => {
                "Use an operator-established SSH tunnel for remote control-plane access and remote operator workflows."
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExecutionBackendState {
    Available,
    Degraded,
    Disabled,
}

impl ExecutionBackendState {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::Degraded => "degraded",
            Self::Disabled => "disabled",
        }
    }
}

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ExecutionBackendResolution {
    pub(crate) requested: ExecutionBackendPreference,
    pub(crate) resolved: ExecutionBackendPreference,
    pub(crate) fallback_used: bool,
    pub(crate) reason_code: String,
    pub(crate) approval_required: bool,
    pub(crate) reason: String,
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WorkspaceWritebackMode {
    None,
    PatchBundle,
    GitCommit,
    LeaseCommit,
}

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

    #[must_use]
    pub(crate) fn attestation_digest_sha256(&self) -> String {
        let encoded =
            serde_json::to_vec(self).unwrap_or_else(|_| self.kind.as_str().as_bytes().to_vec());
        let mut hasher = Sha256::new();
        hasher.update(encoded);
        hex::encode(hasher.finalize())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum BackendAttestationMode {
    None,
    LocalExecutor,
    ContainerProfile,
    VaultIdentity,
    WorkerLease,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExecutionBackendResolutionRequest {
    pub(crate) preference: ExecutionBackendPreference,
    pub(crate) required_capabilities: Vec<String>,
    pub(crate) workspace_strategy: Option<WorkspaceStrategyKind>,
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExecutionBackendHealthStatus {
    Healthy,
    Degraded,
    Unavailable,
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum StuckToolJobRecoveryAction {
    Attach,
    MarkFailed,
    Cancel,
    Cleanup,
    RepairRequired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StuckToolJobRecoveryPlan {
    pub(crate) job_id: String,
    pub(crate) backend_id: String,
    pub(crate) action: StuckToolJobRecoveryAction,
    pub(crate) reason_code: String,
    pub(crate) repair_hint: Option<String>,
    pub(crate) stale_for_ms: i64,
}

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

pub(crate) fn build_execution_backend_preflight_report(
    inventory: &[ExecutionBackendInventoryRecord],
    request: &ExecutionBackendResolutionRequest,
    now_unix_ms: i64,
) -> Vec<ExecutionBackendPreflightRecord> {
    inventory
        .iter()
        .map(|backend| {
            let mut record = backend.preflight(request, now_unix_ms);
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
            record
        })
        .collect()
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ContainerRuntimeKind {
    Docker,
    Podman,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ContainerNetworkPolicy {
    None,
    EgressProxy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ContainerMountPolicy {
    pub(crate) host_path: String,
    pub(crate) container_path: String,
    pub(crate) read_only: bool,
    pub(crate) workspace_scoped: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ContainerResourceLimits {
    pub(crate) cpu_time_limit_ms: u64,
    pub(crate) memory_limit_bytes: u64,
    pub(crate) max_output_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ContainerEnvSourceKind {
    LiteralSafeValue,
    VaultRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ContainerEnvBinding {
    pub(crate) name: String,
    pub(crate) source_kind: ContainerEnvSourceKind,
    pub(crate) value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ContainerBackendProfile {
    pub(crate) profile_id: String,
    pub(crate) runtime: ContainerRuntimeKind,
    pub(crate) image: String,
    pub(crate) mounts: Vec<ContainerMountPolicy>,
    pub(crate) network: ContainerNetworkPolicy,
    pub(crate) user: String,
    pub(crate) privileged: bool,
    pub(crate) limits: ContainerResourceLimits,
    pub(crate) env: Vec<ContainerEnvBinding>,
    pub(crate) cleanup_strategy: String,
}

impl ContainerBackendProfile {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.profile_id.trim().is_empty() {
            return Err("container backend profile_id must not be empty".to_owned());
        }
        if self.image.trim().is_empty() {
            return Err("container backend image must not be empty".to_owned());
        }
        if self.privileged {
            return Err(
                "container backend profiles are fail-closed for privileged containers".to_owned()
            );
        }
        if self.user.trim().is_empty() || self.user.eq_ignore_ascii_case("root") {
            return Err("container backend user must be an explicit non-root user".to_owned());
        }
        if self.limits.cpu_time_limit_ms == 0
            || self.limits.memory_limit_bytes == 0
            || self.limits.max_output_bytes == 0
        {
            return Err("container backend limits must be positive".to_owned());
        }
        if self.mounts.iter().any(|mount| !mount.workspace_scoped) {
            return Err("container backend mounts must be workspace-scoped".to_owned());
        }
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
}

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

pub(crate) fn parse_execution_backend_preference(
    raw: &str,
    field_name: &str,
) -> Result<ExecutionBackendPreference, String> {
    let normalized = raw.trim().to_ascii_lowercase();
    let preference = match normalized.as_str() {
        "" | "automatic" | "auto" => ExecutionBackendPreference::Automatic,
        "local_sandbox" | "local" | "sandbox" => ExecutionBackendPreference::LocalSandbox,
        "desktop_node" | "node" | "remote_node" => ExecutionBackendPreference::DesktopNode,
        "networked_worker" | "networked" | "worker" | "remote_worker" => {
            ExecutionBackendPreference::NetworkedWorker
        }
        "ssh_tunnel" | "ssh" | "tunnel" => ExecutionBackendPreference::SshTunnel,
        _ => {
            return Err(format!(
                "{field_name} must be one of automatic, local_sandbox, desktop_node, networked_worker, ssh_tunnel"
            ));
        }
    };
    Ok(preference)
}

pub(crate) fn parse_optional_execution_backend_preference(
    raw: Option<&str>,
    field_name: &str,
) -> Result<Option<ExecutionBackendPreference>, String> {
    raw.map(|value| parse_execution_backend_preference(value, field_name)).transpose()
}

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
    build_execution_backend_inventory_with_rollout(
        policy,
        nodes.len(),
        healthy_nodes.as_slice(),
        feature_rollouts.execution_backend_remote_node,
        feature_rollouts.execution_backend_networked_worker,
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
    vec![
        local_sandbox_inventory_record(policy),
        desktop_node_inventory_record(total_nodes, healthy_nodes, remote_node_rollout),
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

    if matches!(preference, ExecutionBackendPreference::NetworkedWorker) {
        return ExecutionBackendResolution {
            requested: preference,
            resolved: preference,
            fallback_used: false,
            reason_code: "backend.unavailable.networked_worker".to_owned(),
            approval_required: true,
            reason: requested_record
                .map(|record| {
                    format!(
                        "Requested backend 'networked_worker' is not selectable and local fallback is denied for run-scoped worker grants. {}",
                        record.operator_summary
                    )
                })
                .unwrap_or_else(|| {
                    "Requested backend 'networked_worker' is missing from inventory and local fallback is denied for run-scoped worker grants."
                        .to_owned()
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
    use std::path::PathBuf;

    use palyra_common::feature_rollouts::FeatureRolloutSource;
    use palyra_common::runtime_preview::RuntimePreviewMode;
    use palyra_workerd::{WorkerFleetPolicy, WorkerFleetSnapshot};

    use crate::config::NetworkedWorkersConfig;
    use crate::journal::{ToolJobRecord, ToolJobState};
    use crate::sandbox_runner::{
        EgressEnforcementMode, SandboxProcessRunnerPolicy, SandboxProcessRunnerTier,
    };

    use super::{
        build_execution_backend_inventory_with_rollout, build_execution_backend_preflight_report,
        plan_stuck_tool_job_recovery, resolve_execution_backend,
        resolve_execution_backend_for_request, validate_execution_backend_selection,
        ContainerBackendProfile, ContainerEnvBinding, ContainerEnvSourceKind, ContainerMountPolicy,
        ContainerNetworkPolicy, ContainerResourceLimits, ContainerRuntimeKind, ExecutionBackend,
        ExecutionBackendHealthStatus, ExecutionBackendPreference,
        ExecutionBackendResolutionRequest, ExecutionBackendState, FeatureRolloutSetting,
        SshWorkerBackendProfile, StuckToolJobRecoveryAction, WorkspaceStrategyDescriptor,
        WorkspaceStrategyKind,
    };

    fn test_policy() -> SandboxProcessRunnerPolicy {
        SandboxProcessRunnerPolicy {
            enabled: true,
            tier: SandboxProcessRunnerTier::C,
            workspace_root: PathBuf::from("."),
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
        let mut profile = ContainerBackendProfile {
            profile_id: "docker-safe".to_owned(),
            runtime: ContainerRuntimeKind::Docker,
            image: "ghcr.io/palyra/worker:sha256-deadbeef".to_owned(),
            mounts: vec![ContainerMountPolicy {
                host_path: "workspace".to_owned(),
                container_path: "/workspace".to_owned(),
                read_only: false,
                workspace_scoped: true,
            }],
            network: ContainerNetworkPolicy::EgressProxy,
            user: "1000:1000".to_owned(),
            privileged: false,
            limits: ContainerResourceLimits {
                cpu_time_limit_ms: 1_000,
                memory_limit_bytes: 128 * 1024 * 1024,
                max_output_bytes: 64 * 1024,
            },
            env: vec![ContainerEnvBinding {
                name: "API_TOKEN".to_owned(),
                source_kind: ContainerEnvSourceKind::VaultRef,
                value: "vault://worker/api-token".to_owned(),
            }],
            cleanup_strategy: "remove_container_and_volume".to_owned(),
        };
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
