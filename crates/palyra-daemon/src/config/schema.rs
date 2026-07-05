//! Validated runtime configuration types for the daemon plus their secure
//! defaults (loopback binds, deny-by-default allowlists, disabled preview
//! features).
//!
//! These types are produced exclusively by [`super::load::load_config`],
//! which parses the raw `File*` serde shapes from
//! `palyra_common::daemon_config_schema` and applies `PALYRA_*` environment
//! overrides on top. Code elsewhere in the daemon consumes only these
//! validated types, never the file-level ones.
//!
//! The `DEFAULT_*` constants below are contract surface: integration tests
//! and config import/export fixtures pin them, so changing a value here is a
//! behavioral (and security-posture) change, not a refactor.

use std::path::PathBuf;

use anyhow::Result;
use palyra_common::{
    default_identity_store_root, feature_rollouts::FeatureRolloutSetting,
    runtime_preview::RuntimePreviewMode, secret_refs::SecretRef,
};

use crate::channel_router::ChannelRouterConfig;
use crate::cron::CronTimezoneMode;
use crate::media::MediaRuntimeConfig;
use crate::model_provider::ModelProviderConfig;
use crate::retrieval::RetrievalRuntimeConfig;
use crate::sandbox_runner::{EgressEnforcementMode, PathAccessMode, SandboxProcessRunnerTier};

const DEFAULT_BIND_ADDR: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 7142;
const DEFAULT_GRPC_BIND_ADDR: &str = "127.0.0.1";
const DEFAULT_GRPC_PORT: u16 = 7443;
const DEFAULT_QUIC_BIND_ADDR: &str = "127.0.0.1";
const DEFAULT_QUIC_PORT: u16 = 7444;
const DEFAULT_QUIC_ENABLED: bool = true;
const DEFAULT_GATEWAY_ALLOW_INSECURE_REMOTE: bool = false;
const DEFAULT_GATEWAY_MAX_TAPE_ENTRIES_PER_RESPONSE: usize = 1_000;
const DEFAULT_GATEWAY_MAX_TAPE_BYTES_PER_RESPONSE: usize = 2 * 1024 * 1024;
const DEFAULT_GATEWAY_TLS_ENABLED: bool = false;
const DEFAULT_GATEWAY_VAULT_GET_APPROVAL_REQUIRED_REFS: &[&str] =
    &["global/openai_api_key", "global/anthropic_api_key"];
const DEFAULT_CRON_TIMEZONE_MODE: CronTimezoneMode = CronTimezoneMode::Utc;
const DEFAULT_ORCHESTRATOR_RUNLOOP_V1_ENABLED: bool = false;
const DEFAULT_SESSION_QUEUE_MODE: RuntimePreviewMode = RuntimePreviewMode::PreviewOnly;
const DEFAULT_SESSION_QUEUE_MAX_DEPTH: usize = 8;
const DEFAULT_SESSION_QUEUE_MERGE_WINDOW_MS: u64 = 1_500;
const DEFAULT_PRUNING_POLICY_MODE: RuntimePreviewMode = RuntimePreviewMode::PreviewOnly;
const DEFAULT_PRUNING_MANUAL_APPLY_ENABLED: bool = true;
const DEFAULT_PRUNING_MIN_TOKEN_SAVINGS: u64 = 128;
const DEFAULT_RETRIEVAL_DUAL_PATH_MODE: RuntimePreviewMode = RuntimePreviewMode::PreviewOnly;
const DEFAULT_RETRIEVAL_BRANCH_TIMEOUT_MS: u64 = 2_000;
const DEFAULT_RETRIEVAL_PROMPT_BUDGET_TOKENS: u64 = 1_800;
const DEFAULT_AUXILIARY_EXECUTOR_MODE: RuntimePreviewMode = RuntimePreviewMode::PreviewOnly;
const DEFAULT_AUXILIARY_MAX_TASKS_PER_SESSION: usize = 4;
const DEFAULT_AUXILIARY_DEFAULT_BUDGET_TOKENS: u64 = 1_024;
const DEFAULT_FLOW_ORCHESTRATION_MODE: RuntimePreviewMode = RuntimePreviewMode::PreviewOnly;
const DEFAULT_FLOW_CANCELLATION_GATE_ENABLED: bool = true;
const DEFAULT_FLOW_MAX_RETRY_COUNT: u32 = 1;
const DEFAULT_DELIVERY_ARBITRATION_MODE: RuntimePreviewMode = RuntimePreviewMode::Disabled;
const DEFAULT_DELIVERY_DESCENDANT_PREFERENCE: bool = true;
const DEFAULT_DELIVERY_SUPPRESSION_LIMIT: u32 = 2;
const DEFAULT_REPLAY_CAPTURE_MODE: RuntimePreviewMode = RuntimePreviewMode::PreviewOnly;
const DEFAULT_REPLAY_CAPTURE_RUNTIME_DECISIONS: bool = true;
const DEFAULT_REPLAY_MAX_EVENTS_PER_RUN: usize = 128;
const DEFAULT_NETWORKED_WORKERS_MODE: RuntimePreviewMode = RuntimePreviewMode::Disabled;
const DEFAULT_NETWORKED_WORKERS_LEASE_TTL_MS: u64 = 15 * 60 * 1_000;
const DEFAULT_NETWORKED_WORKERS_REQUIRE_ATTESTATION: bool = true;
const DEFAULT_ROADMAP_PREVIEW_SECTION_MODE: RuntimePreviewMode = RuntimePreviewMode::Disabled;
const DEFAULT_MEMORY_MAX_ITEM_BYTES: usize = 16 * 1024;
const DEFAULT_MEMORY_MAX_ITEM_TOKENS: usize = 2_048;
const DEFAULT_MEMORY_DEFAULT_TTL_MS: i64 = 30 * 24 * 60 * 60 * 1_000;
const DEFAULT_MEMORY_AUTO_INJECT_ENABLED: bool = true;
const DEFAULT_MEMORY_AUTO_INJECT_MAX_ITEMS: usize = 3;
const DEFAULT_MEMORY_RETENTION_VACUUM_SCHEDULE: &str = "0 0 * * 0";
const DEFAULT_ADMIN_REQUIRE_AUTH: bool = true;
const DEFAULT_ALLOW_INSECURE_NODE_RPC_WITHOUT_MTLS: bool = false;
const DEFAULT_JOURNAL_DB_PATH: &str = "data/journal.sqlite3";
const DEFAULT_JOURNAL_HASH_CHAIN_ENABLED: bool = true;
const DEFAULT_MAX_JOURNAL_PAYLOAD_BYTES: usize = 256 * 1024;
const DEFAULT_MAX_JOURNAL_EVENTS: usize = 1_000_000;
const DEFAULT_TOOL_CALL_MAX_CALLS_PER_RUN: u32 = 0;
const DEFAULT_TOOL_CALL_EXECUTION_TIMEOUT_MS: u64 = 750;
const DEFAULT_PROCESS_RUNNER_ENABLED: bool = false;
const DEFAULT_PROCESS_RUNNER_TIER: SandboxProcessRunnerTier = SandboxProcessRunnerTier::B;
const DEFAULT_PROCESS_RUNNER_WORKSPACE_ROOT: &str = ".";
const DEFAULT_PROCESS_RUNNER_PATH_ACCESS_MODE: PathAccessMode = PathAccessMode::WorkspaceOnly;
const DEFAULT_PROCESS_RUNNER_ALLOW_INTERPRETERS: bool = false;
const DEFAULT_PROCESS_RUNNER_EGRESS_ENFORCEMENT_MODE: EgressEnforcementMode =
    EgressEnforcementMode::Strict;
const DEFAULT_PROCESS_RUNNER_CPU_TIME_LIMIT_MS: u64 = 2_000;
const DEFAULT_PROCESS_RUNNER_MEMORY_LIMIT_BYTES: u64 = 256 * 1024 * 1024;
const DEFAULT_PROCESS_RUNNER_MAX_OUTPUT_BYTES: u64 = 64 * 1024;
const DEFAULT_WASM_RUNTIME_ENABLED: bool = false;
const DEFAULT_WASM_RUNTIME_ALLOW_INLINE_MODULES: bool = false;
const DEFAULT_WASM_RUNTIME_MAX_MODULE_SIZE_BYTES: u64 = 256 * 1024;
const DEFAULT_WASM_RUNTIME_FUEL_BUDGET: u64 = 10_000_000;
const DEFAULT_WASM_RUNTIME_MAX_MEMORY_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_WASM_RUNTIME_MAX_TABLE_ELEMENTS: u64 = 100_000;
const DEFAULT_WASM_RUNTIME_MAX_INSTANCES: u64 = 256;
const DEFAULT_HTTP_FETCH_ALLOW_PRIVATE_TARGETS: bool = false;
const DEFAULT_HTTP_FETCH_CONNECT_TIMEOUT_MS: u64 = 1_500;
const DEFAULT_HTTP_FETCH_REQUEST_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_HTTP_FETCH_MAX_RESPONSE_BYTES: u64 = 512 * 1024;
const DEFAULT_HTTP_FETCH_ALLOW_REDIRECTS: bool = true;
const DEFAULT_HTTP_FETCH_MAX_REDIRECTS: u32 = 3;
const DEFAULT_HTTP_FETCH_ALLOWED_CONTENT_TYPES: &[&str] = &[
    "text/html",
    "text/plain",
    "text/markdown",
    "application/json",
    "text/css",
    "text/javascript",
    "text/ecmascript",
    "application/javascript",
    "application/x-javascript",
    "application/ecmascript",
];
const DEFAULT_HTTP_FETCH_ALLOWED_REQUEST_HEADERS: &[&str] = &[
    "accept",
    "accept-language",
    "content-type",
    "if-none-match",
    "if-modified-since",
    "user-agent",
    "x-client-version",
];
const DEFAULT_HTTP_FETCH_CACHE_ENABLED: bool = true;
const DEFAULT_HTTP_FETCH_CACHE_TTL_MS: u64 = 30_000;
const DEFAULT_HTTP_FETCH_MAX_CACHE_ENTRIES: u64 = 256;
const DEFAULT_BROWSER_SERVICE_ENABLED: bool = false;
const DEFAULT_BROWSER_SERVICE_ENDPOINT: &str = "http://127.0.0.1:7543";
const DEFAULT_BROWSER_SERVICE_CONNECT_TIMEOUT_MS: u64 = 1_500;
const DEFAULT_BROWSER_SERVICE_REQUEST_TIMEOUT_MS: u64 = 15_000;
const DEFAULT_BROWSER_SERVICE_MAX_SCREENSHOT_BYTES: u64 = 1024 * 1024;
const DEFAULT_BROWSER_SERVICE_MAX_TITLE_BYTES: u64 = 4 * 1024;
const DEFAULT_CANVAS_HOST_ENABLED: bool = false;
const DEFAULT_CANVAS_HOST_PUBLIC_BASE_URL: &str = "http://127.0.0.1:7142";
const DEFAULT_CANVAS_HOST_TOKEN_TTL_MS: u64 = 15 * 60 * 1_000;
const DEFAULT_CANVAS_HOST_MAX_STATE_BYTES: u64 = 64 * 1024;
const DEFAULT_CANVAS_HOST_MAX_BUNDLE_BYTES: u64 = 512 * 1024;
const DEFAULT_CANVAS_HOST_MAX_ASSETS_PER_BUNDLE: u32 = 32;
const DEFAULT_CANVAS_HOST_MAX_UPDATES_PER_MINUTE: u32 = 120;
const DEFAULT_DEPLOYMENT_MODE: DeploymentMode = DeploymentMode::LocalDesktop;
const DEFAULT_GATEWAY_BIND_PROFILE: GatewayBindProfile = GatewayBindProfile::LoopbackOnly;
const DEFAULT_DANGEROUS_REMOTE_BIND_ACK: bool = false;

/// Fully merged and validated daemon configuration, the single source of
/// truth handed to the runtime at startup.
///
/// Built only by [`super::load::load_config`]; all section values have
/// already passed parsing and cross-field validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedConfig {
    /// Human-readable provenance: the config file path (or `"defaults"`)
    /// followed by one ` +env(NAME)` marker per applied override.
    pub source: String,
    /// Schema version of the loaded document after migration.
    pub config_version: u32,
    /// Original schema version when a legacy document was migrated; `None`
    /// when no migration ran.
    pub migrated_from_version: Option<u32>,
    pub deployment: DeploymentConfig,
    pub daemon: DaemonConfig,
    pub gateway: GatewayConfig,
    pub feature_rollouts: FeatureRolloutsConfig,
    pub session_queue_policy: SessionQueuePolicyConfig,
    pub pruning_policy_matrix: PruningPolicyMatrixConfig,
    pub retrieval_dual_path: RetrievalDualPathConfig,
    pub auxiliary_executor: AuxiliaryExecutorConfig,
    pub flow_orchestration: FlowOrchestrationConfig,
    pub delivery_arbitration: DeliveryArbitrationConfig,
    pub replay_capture: ReplayCaptureConfig,
    pub networked_workers: NetworkedWorkersConfig,
    pub api_facade: RoadmapPreviewSectionConfig,
    pub mcp_servers: McpServersConfig,
    pub execution_backend_profiles: ExecutionBackendProfilesConfig,
    pub qa_lab: RoadmapPreviewSectionConfig,
    pub observability_exporters: ObservabilityExportersConfig,
    pub hook_policy: RoadmapPreviewSectionConfig,
    pub agent_harness_registry: AgentHarnessRegistryConfig,
    pub doctor_check_registry: DoctorCheckRegistryConfig,
    pub cron: CronConfig,
    pub orchestrator: OrchestratorConfig,
    pub memory: MemoryConfig,
    pub media: MediaRuntimeConfig,
    pub model_provider: ModelProviderConfig,
    pub tool_call: ToolCallConfig,
    pub channel_router: ChannelRouterConfig,
    pub canvas_host: CanvasHostConfig,
    pub admin: AdminConfig,
    pub identity: IdentityConfig,
    pub storage: StorageConfig,
}

/// Deployment posture: which profile/mode the daemon runs under and whether
/// the operator acknowledged a dangerous remote bind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentConfig {
    /// Deployment profile id; derived from mode and worker settings unless
    /// set explicitly in the file or via `PALYRA_DEPLOYMENT_PROFILE`.
    pub profile: String,
    pub mode: DeploymentMode,
    /// Explicit operator acknowledgement required before non-loopback binds
    /// are permitted in remote deployments. Defaults to `false`.
    pub dangerous_remote_bind_ack: bool,
}

/// Where the daemon is deployed; gates bind-address and TLS policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeploymentMode {
    LocalDesktop,
    RemoteVps,
}

impl DeploymentMode {
    /// Returns the canonical snake_case config value for this mode.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LocalDesktop => "local_desktop",
            Self::RemoteVps => "remote_vps",
        }
    }

    /// Parses a config/env value, accepting hyphenated and short aliases.
    ///
    /// # Errors
    /// Fails when `raw` is not a recognized mode; the message names
    /// `source_name` so the operator can find the offending key.
    pub fn parse(raw: &str, source_name: &str) -> Result<Self> {
        let normalized = raw.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "local_desktop" | "local-desktop" | "local" => Ok(Self::LocalDesktop),
            "remote_vps" | "remote-vps" | "remote" | "vps" => Ok(Self::RemoteVps),
            _ => anyhow::bail!("{source_name} must be one of: local_desktop | remote_vps"),
        }
    }
}

/// How the gateway may expose its listeners: loopback-only (default) or
/// public with TLS required.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayBindProfile {
    LoopbackOnly,
    PublicTls,
}

impl GatewayBindProfile {
    /// Returns the canonical snake_case config value for this profile.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LoopbackOnly => "loopback_only",
            Self::PublicTls => "public_tls",
        }
    }

    /// Parses a config/env value, accepting hyphenated and short aliases.
    ///
    /// # Errors
    /// Fails when `raw` is not a recognized profile; the message names
    /// `source_name` so the operator can find the offending key.
    pub fn parse(raw: &str, source_name: &str) -> Result<Self> {
        let normalized = raw.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "loopback_only" | "loopback-only" | "loopback" => Ok(Self::LoopbackOnly),
            "public_tls" | "public-tls" | "public" => Ok(Self::PublicTls),
            _ => anyhow::bail!("{source_name} must be one of: loopback_only | public_tls"),
        }
    }
}

/// HTTP listener address for the daemon's console/admin API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonConfig {
    pub bind_addr: String,
    pub port: u16,
}

/// Gateway transport (gRPC/QUIC) binds, exposure profile, and response
/// budgets. Defaults to loopback-only listeners with QUIC enabled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayConfig {
    pub grpc_bind_addr: String,
    pub grpc_port: u16,
    pub quic_bind_addr: String,
    pub quic_port: u16,
    pub quic_enabled: bool,
    pub bind_profile: GatewayBindProfile,
    /// Escape hatch allowing remote exposure without TLS; defaults to
    /// `false` and must be opted into explicitly.
    pub allow_insecure_remote: bool,
    /// Identity/pairing store location; also used to derive the runtime
    /// state root when `PALYRA_STATE_ROOT` is unset.
    pub identity_store_dir: Option<PathBuf>,
    /// Vault refs (`<scope>/<key>`, case-sensitive key preserved) whose
    /// reads require explicit operator approval through the gateway.
    pub vault_get_approval_required_refs: Vec<String>,
    pub max_tape_entries_per_response: usize,
    pub max_tape_bytes_per_response: usize,
    pub tls: GatewayTlsConfig,
}

/// Gateway TLS material; when `enabled`, the loader requires both
/// `cert_path` and `key_path` to be set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayTlsConfig {
    pub enabled: bool,
    pub cert_path: Option<PathBuf>,
    pub key_path: Option<PathBuf>,
    /// Optional client CA bundle enabling mutual TLS verification.
    pub client_ca_path: Option<PathBuf>,
}

/// Cron scheduler settings; defaults to UTC for deterministic cross-host
/// behavior.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronConfig {
    pub timezone: CronTimezoneMode,
}

/// Per-feature rollout switches (all disabled by default). Runtime preview
/// sections may only be set to `enabled` mode when the matching rollout
/// flag here is on; the loader enforces that pairing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FeatureRolloutsConfig {
    pub dynamic_tool_builder: FeatureRolloutSetting,
    pub context_engine: FeatureRolloutSetting,
    pub execution_backend_remote_node: FeatureRolloutSetting,
    pub execution_backend_networked_worker: FeatureRolloutSetting,
    pub execution_backend_docker: FeatureRolloutSetting,
    pub execution_backend_ssh_tunnel: FeatureRolloutSetting,
    pub safety_boundary: FeatureRolloutSetting,
    pub execution_gate_pipeline_v2: FeatureRolloutSetting,
    pub agent_harness_runtime: FeatureRolloutSetting,
    pub inline_runtime_hooks: FeatureRolloutSetting,
    pub tool_result_middleware: FeatureRolloutSetting,
    pub session_queue_policy: FeatureRolloutSetting,
    pub pruning_policy_matrix: FeatureRolloutSetting,
    pub retrieval_dual_path: FeatureRolloutSetting,
    pub auxiliary_executor: FeatureRolloutSetting,
    pub flow_orchestration: FeatureRolloutSetting,
    pub delivery_arbitration: FeatureRolloutSetting,
    pub replay_capture: FeatureRolloutSetting,
    pub networked_workers: FeatureRolloutSetting,
    pub tool_repair: FeatureRolloutSetting,
    pub provider_stream_normalizer: FeatureRolloutSetting,
    pub provider_recovery: FeatureRolloutSetting,
    pub terminal_sessions: FeatureRolloutSetting,
    pub browser_rescue: FeatureRolloutSetting,
    pub lsp_service: FeatureRolloutSetting,
    pub advisor_fanout: FeatureRolloutSetting,
    pub acp_runtime: FeatureRolloutSetting,
    pub channel_turn_kernel: FeatureRolloutSetting,
    pub agent_plan_state: FeatureRolloutSetting,
    pub objective_judge: FeatureRolloutSetting,
    pub verification_runtime: FeatureRolloutSetting,
    pub progress_drafts: FeatureRolloutSetting,
    pub compaction_safeguard: FeatureRolloutSetting,
    pub provider_backed_evidence_compaction: FeatureRolloutSetting,
    pub attack_surface_audit: FeatureRolloutSetting,
}

/// Orchestrator feature toggles; the v1 run loop defaults to disabled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrchestratorConfig {
    pub runloop_v1_enabled: bool,
}

/// Session queue policy preview: queueing depth and message merge window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionQueuePolicyConfig {
    pub mode: RuntimePreviewMode,
    pub max_depth: usize,
    pub merge_window_ms: u64,
}

impl Default for SessionQueuePolicyConfig {
    fn default() -> Self {
        Self {
            mode: DEFAULT_SESSION_QUEUE_MODE,
            max_depth: DEFAULT_SESSION_QUEUE_MAX_DEPTH,
            merge_window_ms: DEFAULT_SESSION_QUEUE_MERGE_WINDOW_MS,
        }
    }
}

/// Context pruning policy preview: manual-apply gate and minimum token
/// savings before a prune is proposed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PruningPolicyMatrixConfig {
    pub mode: RuntimePreviewMode,
    pub manual_apply_enabled: bool,
    pub min_token_savings: u64,
}

impl Default for PruningPolicyMatrixConfig {
    fn default() -> Self {
        Self {
            mode: DEFAULT_PRUNING_POLICY_MODE,
            manual_apply_enabled: DEFAULT_PRUNING_MANUAL_APPLY_ENABLED,
            min_token_savings: DEFAULT_PRUNING_MIN_TOKEN_SAVINGS,
        }
    }
}

/// Dual-path retrieval preview: per-branch timeout and prompt token budget.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetrievalDualPathConfig {
    pub mode: RuntimePreviewMode,
    pub branch_timeout_ms: u64,
    pub prompt_budget_tokens: u64,
}

impl Default for RetrievalDualPathConfig {
    fn default() -> Self {
        Self {
            mode: DEFAULT_RETRIEVAL_DUAL_PATH_MODE,
            branch_timeout_ms: DEFAULT_RETRIEVAL_BRANCH_TIMEOUT_MS,
            prompt_budget_tokens: DEFAULT_RETRIEVAL_PROMPT_BUDGET_TOKENS,
        }
    }
}

/// Auxiliary executor preview: per-session task cap and default token
/// budget for spawned auxiliary tasks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuxiliaryExecutorConfig {
    pub mode: RuntimePreviewMode,
    pub max_tasks_per_session: usize,
    pub default_budget_tokens: u64,
}

impl Default for AuxiliaryExecutorConfig {
    fn default() -> Self {
        Self {
            mode: DEFAULT_AUXILIARY_EXECUTOR_MODE,
            max_tasks_per_session: DEFAULT_AUXILIARY_MAX_TASKS_PER_SESSION,
            default_budget_tokens: DEFAULT_AUXILIARY_DEFAULT_BUDGET_TOKENS,
        }
    }
}

/// Flow orchestration preview: cancellation gating and step retry budget.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowOrchestrationConfig {
    pub mode: RuntimePreviewMode,
    pub cancellation_gate_enabled: bool,
    pub max_retry_count: u32,
}

impl Default for FlowOrchestrationConfig {
    fn default() -> Self {
        Self {
            mode: DEFAULT_FLOW_ORCHESTRATION_MODE,
            cancellation_gate_enabled: DEFAULT_FLOW_CANCELLATION_GATE_ENABLED,
            max_retry_count: DEFAULT_FLOW_MAX_RETRY_COUNT,
        }
    }
}

/// Delivery arbitration preview: descendant preference and suppression cap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryArbitrationConfig {
    pub mode: RuntimePreviewMode,
    pub descendant_preference: bool,
    pub suppression_limit: u32,
}

impl Default for DeliveryArbitrationConfig {
    fn default() -> Self {
        Self {
            mode: DEFAULT_DELIVERY_ARBITRATION_MODE,
            descendant_preference: DEFAULT_DELIVERY_DESCENDANT_PREFERENCE,
            suppression_limit: DEFAULT_DELIVERY_SUPPRESSION_LIMIT,
        }
    }
}

/// Replay capture preview: whether runtime decisions are recorded and the
/// per-run event cap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayCaptureConfig {
    pub mode: RuntimePreviewMode,
    pub capture_runtime_decisions: bool,
    pub max_events_per_run: usize,
}

impl Default for ReplayCaptureConfig {
    fn default() -> Self {
        Self {
            mode: DEFAULT_REPLAY_CAPTURE_MODE,
            capture_runtime_decisions: DEFAULT_REPLAY_CAPTURE_RUNTIME_DECISIONS,
            max_events_per_run: DEFAULT_REPLAY_MAX_EVENTS_PER_RUN,
        }
    }
}

/// Networked worker fleet preview: lease TTL, attestation requirement, and
/// expected artifact digests (lowercase 64-char hex SHA-256 when set).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkedWorkersConfig {
    pub mode: RuntimePreviewMode,
    pub lease_ttl_ms: u64,
    pub require_attestation: bool,
    pub expected_image_digest_sha256: Option<String>,
    pub expected_build_digest_sha256: Option<String>,
    pub expected_artifact_digest_sha256: Option<String>,
}

impl Default for NetworkedWorkersConfig {
    fn default() -> Self {
        Self {
            mode: DEFAULT_NETWORKED_WORKERS_MODE,
            lease_ttl_ms: DEFAULT_NETWORKED_WORKERS_LEASE_TTL_MS,
            require_attestation: DEFAULT_NETWORKED_WORKERS_REQUIRE_ATTESTATION,
            expected_image_digest_sha256: None,
            expected_build_digest_sha256: None,
            expected_artifact_digest_sha256: None,
        }
    }
}

/// Minimal preview section used for roadmap areas that are declared in config
/// before their runtime implementation is allowed to run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoadmapPreviewSectionConfig {
    pub mode: RuntimePreviewMode,
}

impl Default for RoadmapPreviewSectionConfig {
    fn default() -> Self {
        Self { mode: DEFAULT_ROADMAP_PREVIEW_SECTION_MODE }
    }
}

/// MCP server registry preview consumed by the runtime supervisor and catalog
/// projection when MCP import is enabled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpServersConfig {
    pub mode: RuntimePreviewMode,
    pub servers: Vec<McpServerConfig>,
}

impl Default for McpServersConfig {
    fn default() -> Self {
        Self { mode: DEFAULT_ROADMAP_PREVIEW_SECTION_MODE, servers: Vec::new() }
    }
}

/// Transport type for a declared MCP server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpServerTransport {
    Stdio,
    Http,
    Sse,
}

impl McpServerTransport {
    /// Returns the canonical config value for this transport.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Stdio => "stdio",
            Self::Http => "http",
            Self::Sse => "sse",
        }
    }
}

/// One declared MCP server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpServerConfig {
    pub id: String,
    pub enabled: bool,
    pub namespace: String,
    pub transport: McpServerTransport,
    pub command: Option<Vec<String>>,
    pub url: Option<String>,
    pub env_vault_refs: Vec<McpServerEnvVaultRef>,
    pub trust_level: McpServerTrustLevel,
    pub approval_profile: McpServerApprovalProfile,
    pub egress_policy: McpServerEgressPolicy,
    pub egress_allowlist: Vec<String>,
    pub oauth_required: bool,
    pub oauth_grant: Option<McpServerOAuthGrant>,
    pub sampling_policy: McpServerSamplingPolicy,
    pub tool_allowlist: Vec<String>,
    pub tool_denylist: Vec<String>,
}

/// Vault-backed environment binding for an MCP stdio server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpServerEnvVaultRef {
    pub name: String,
    pub vault_ref: String,
}

/// Vault-backed OAuth grant metadata for an MCP server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpServerOAuthGrant {
    pub grant_id: String,
    pub access_token_vault_ref: String,
    pub refresh_token_vault_ref: Option<String>,
    pub metadata_vault_ref: String,
    pub scopes: Vec<String>,
    pub expires_at_unix_ms: Option<i64>,
    pub rotation_id: Option<String>,
    pub issued_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
    pub revoked_at_unix_ms: Option<i64>,
}

/// Sampling posture for a configured MCP server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpServerSamplingPolicy {
    pub mode: McpServerSamplingMode,
    pub allowed_model_capabilities: Vec<String>,
}

impl Default for McpServerSamplingPolicy {
    fn default() -> Self {
        Self { mode: McpServerSamplingMode::Deny, allowed_model_capabilities: Vec::new() }
    }
}

/// Sampling request decision mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpServerSamplingMode {
    Deny,
    Allowlist,
}

impl McpServerSamplingMode {
    /// Returns the canonical config value for this sampling mode.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Deny => "deny",
            Self::Allowlist => "allowlist",
        }
    }
}

/// Operator-assigned trust tier for an external MCP server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpServerTrustLevel {
    Local,
    Workspace,
    External,
}

/// Default approval posture for tools imported from an MCP server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpServerApprovalProfile {
    Safe,
    RequireApproval,
}

/// Egress posture for an MCP server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpServerEgressPolicy {
    DenyAll,
    Allowlist,
}

/// Execution backend profile registry preview.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionBackendProfilesConfig {
    pub mode: RuntimePreviewMode,
    pub profiles: Vec<ExecutionBackendProfileConfig>,
}

impl Default for ExecutionBackendProfilesConfig {
    fn default() -> Self {
        Self { mode: DEFAULT_ROADMAP_PREVIEW_SECTION_MODE, profiles: Vec::new() }
    }
}

/// One declared execution backend profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionBackendProfileConfig {
    pub id: String,
    pub enabled: bool,
    pub kind: String,
    pub container: Option<ExecutionBackendContainerProfileConfig>,
    pub ssh_worker: Option<ExecutionBackendSshWorkerProfileConfig>,
}

/// Container-specific execution backend profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionBackendContainerProfileConfig {
    pub image: String,
    pub user: String,
    pub network: String,
    pub readonly_rootfs: bool,
    pub privileged: bool,
    pub workspace_mount: ExecutionBackendContainerWorkspaceMountConfig,
    pub resource_limits: ExecutionBackendContainerResourceLimitsConfig,
    pub env: Vec<ExecutionBackendContainerEnvBindingConfig>,
    pub cleanup_strategy: String,
}

/// Workspace mount for a container execution backend profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionBackendContainerWorkspaceMountConfig {
    pub host_path: String,
    pub container_path: String,
    pub read_only: bool,
}

/// Resource limits for a container execution backend profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionBackendContainerResourceLimitsConfig {
    pub cpu_time_limit_ms: u64,
    pub memory_limit_bytes: u64,
    pub max_output_bytes: u64,
}

/// Environment variable binding for a container execution backend profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionBackendContainerEnvBindingConfig {
    pub name: String,
    pub source_kind: String,
    pub value: String,
}

/// SSH worker RPC execution backend profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionBackendSshWorkerProfileConfig {
    pub tunnel_endpoint: String,
    pub host_handle: String,
    pub user_handle: String,
    pub identity_handle: String,
    pub host_trust_handle: String,
    pub worker_protocol: String,
    pub health_probe: String,
    pub capabilities: Vec<String>,
}

/// Observability exporter registry preview.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservabilityExportersConfig {
    pub mode: RuntimePreviewMode,
    pub exporters: Vec<ObservabilityExporterConfig>,
}

impl Default for ObservabilityExportersConfig {
    fn default() -> Self {
        Self { mode: DEFAULT_ROADMAP_PREVIEW_SECTION_MODE, exporters: Vec::new() }
    }
}

/// One declared observability exporter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservabilityExporterConfig {
    pub id: String,
    pub enabled: bool,
    pub kind: String,
}

/// Native agent harness registry preview.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentHarnessRegistryConfig {
    pub mode: RuntimePreviewMode,
    pub harnesses: Vec<AgentHarnessConfig>,
}

impl Default for AgentHarnessRegistryConfig {
    fn default() -> Self {
        Self { mode: DEFAULT_ROADMAP_PREVIEW_SECTION_MODE, harnesses: Vec::new() }
    }
}

/// One declared agent harness.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentHarnessConfig {
    pub id: String,
    pub enabled: bool,
    pub kind: String,
}

/// Doctor check registry preview.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DoctorCheckRegistryConfig {
    pub mode: RuntimePreviewMode,
    pub checks: Vec<DoctorCheckConfig>,
}

impl Default for DoctorCheckRegistryConfig {
    fn default() -> Self {
        Self { mode: DEFAULT_ROADMAP_PREVIEW_SECTION_MODE, checks: Vec::new() }
    }
}

/// One declared doctor check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DoctorCheckConfig {
    pub id: String,
    pub enabled: bool,
}

/// Memory subsystem limits, auto-injection, retention, and retrieval tuning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryConfig {
    pub max_item_bytes: usize,
    pub max_item_tokens: usize,
    /// Default lifetime for new memory items; `None` means items never
    /// expire by default (configured as `0`).
    pub default_ttl_ms: Option<i64>,
    pub auto_inject: MemoryAutoInjectConfig,
    pub retention: MemoryRetentionConfig,
    pub retrieval: RetrievalRuntimeConfig,
}

/// Automatic memory injection into prompts: on/off and per-prompt item cap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryAutoInjectConfig {
    pub enabled: bool,
    pub max_items: usize,
}

/// Memory retention bounds; `None` fields mean the bound is not enforced.
/// `vacuum_schedule` is a normalized 5- or 6-field cron expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryRetentionConfig {
    pub max_entries: Option<usize>,
    pub max_bytes: Option<u64>,
    pub ttl_days: Option<u32>,
    pub vacuum_schedule: String,
}

/// Tool execution broker policy: the tool allowlist (empty = deny all),
/// per-call time budget, legacy per-run call key, and per-backend sub-configs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCallConfig {
    pub allowed_tools: Vec<String>,
    pub explicit_allowed_tools: Vec<String>,
    pub toolset_profiles: Vec<String>,
    pub extra_tools: Vec<String>,
    pub disabled_tools: Vec<String>,
    pub catalog_exposure_mode: palyra_common::tool_catalog::ToolCatalogExposureMode,
    pub compact_tool_threshold: usize,
    /// Legacy config key accepted for existing installations.
    ///
    /// Count-based agent-run limiting is disabled; `0` means unlimited and
    /// non-zero historical values are not terminal step budgets.
    pub max_calls_per_run: u32,
    pub execution_timeout_ms: u64,
    pub process_runner: ProcessRunnerConfig,
    pub code_intel: CodeIntelConfig,
    pub wasm_runtime: WasmRuntimeConfig,
    pub http_fetch: HttpFetchConfig,
    pub browser_service: BrowserServiceConfig,
}

/// Code diagnostics adapter policy (disabled by default): workspace scope,
/// provider binaries, time budget, and output caps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodeIntelConfig {
    pub enabled: bool,
    pub workspace_root: Option<PathBuf>,
    pub rust_analyzer_binary: String,
    pub typescript_server_binary: String,
    pub pyright_binary: String,
    pub timeout_ms: u64,
    pub max_output_bytes: u64,
    pub max_items: usize,
    pub idle_reap_ms: u64,
}

/// Sandboxed process runner policy (disabled by default): tier, executable
/// allowlist, egress enforcement, and resource limits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessRunnerConfig {
    pub enabled: bool,
    pub tier: SandboxProcessRunnerTier,
    pub workspace_root: PathBuf,
    pub path_access_mode: PathAccessMode,
    /// Executable names permitted to run; a literal `"*"` entry grants host
    /// access to any executable.
    pub allowed_executables: Vec<String>,
    pub allow_interpreters: bool,
    pub egress_enforcement_mode: EgressEnforcementMode,
    pub allowed_egress_hosts: Vec<String>,
    /// Allowed DNS suffixes, normalized with a leading dot.
    pub allowed_dns_suffixes: Vec<String>,
    pub cpu_time_limit_ms: u64,
    pub memory_limit_bytes: u64,
    pub max_output_bytes: u64,
}

/// Wasm plugin runtime policy (disabled by default): module size/fuel/memory
/// limits and capability allowlists (empty = deny).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmRuntimeConfig {
    pub enabled: bool,
    pub allow_inline_modules: bool,
    pub max_module_size_bytes: u64,
    pub fuel_budget: u64,
    pub max_memory_bytes: u64,
    pub max_table_elements: u64,
    pub max_instances: u64,
    pub allowed_http_hosts: Vec<String>,
    /// Secret handles (not secret values) plugins may request.
    pub allowed_secrets: Vec<String>,
    pub allowed_storage_prefixes: Vec<String>,
    pub allowed_channels: Vec<String>,
}

/// HTTP fetch tool policy: private-target blocking (on by default),
/// timeouts, response/redirect budgets, content-type and header allowlists,
/// and the vault refs eligible for credential injection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpFetchConfig {
    pub allow_private_targets: bool,
    pub connect_timeout_ms: u64,
    pub request_timeout_ms: u64,
    pub max_response_bytes: u64,
    pub allow_redirects: bool,
    pub max_redirects: u32,
    pub allowed_content_types: Vec<String>,
    pub allowed_request_headers: Vec<String>,
    /// Exact vault refs (scope case preserved) that fetch requests may bind
    /// as credentials; empty means credential injection is disabled.
    pub allowed_credential_vault_refs: Vec<String>,
    pub cache_enabled: bool,
    pub cache_ttl_ms: u64,
    pub max_cache_entries: u64,
}

/// Browser daemon (browserd) broker settings (disabled by default).
///
/// Secrets follow the loader's mutual-exclusion rule: at most one of the
/// inline value, structured `*_secret_ref`, or legacy `*_vault_ref` may be
/// set per credential.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrowserServiceConfig {
    pub enabled: bool,
    pub endpoint: String,
    pub auth_token: Option<String>,
    pub auth_token_secret_ref: Option<SecretRef>,
    pub state_dir: Option<PathBuf>,
    pub state_key_secret_ref: Option<SecretRef>,
    /// Legacy `<scope>/<key>` vault ref for the state encryption key; kept
    /// as-is (not converted to a [`SecretRef`]) for config round-tripping.
    pub state_key_vault_ref: Option<String>,
    pub connect_timeout_ms: u64,
    pub request_timeout_ms: u64,
    pub max_screenshot_bytes: u64,
    pub max_title_bytes: u64,
}

/// Canvas hosting (disabled by default): public base URL, token TTL, and
/// per-bundle/update rate budgets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanvasHostConfig {
    pub enabled: bool,
    pub public_base_url: String,
    pub token_ttl_ms: u64,
    pub max_state_bytes: u64,
    pub max_bundle_bytes: u64,
    pub max_assets_per_bundle: u32,
    pub max_updates_per_minute: u32,
}

/// Admin/connector authentication. Auth is required by default; tokens left
/// as `None` mean access is denied rather than open.
///
/// Each token follows the loader's mutual-exclusion rule: inline value and
/// `*_secret_ref` cannot both be set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminConfig {
    pub require_auth: bool,
    pub auth_token: Option<String>,
    pub auth_token_secret_ref: Option<SecretRef>,
    pub connector_token: Option<String>,
    pub connector_token_secret_ref: Option<SecretRef>,
    /// When set, the admin token is only honored for this principal.
    pub bound_principal: Option<String>,
}

/// Identity-plane toggles; insecure node RPC without mTLS defaults to off.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityConfig {
    pub allow_insecure_node_rpc_without_mtls: bool,
}

/// Journal and vault storage locations and journal limits.
///
/// `journal_db_path` and `vault_dir` are resolved against the runtime state
/// root by the loader when configured as relative paths.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageConfig {
    pub journal_db_path: PathBuf,
    pub journal_hash_chain_enabled: bool,
    pub max_journal_payload_bytes: usize,
    pub max_journal_events: usize,
    pub vault_dir: PathBuf,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self { bind_addr: DEFAULT_BIND_ADDR.to_owned(), port: DEFAULT_PORT }
    }
}

impl Default for DeploymentConfig {
    fn default() -> Self {
        Self {
            profile: palyra_common::deployment_profiles::DeploymentProfileId::Local
                .as_str()
                .to_owned(),
            mode: DEFAULT_DEPLOYMENT_MODE,
            dangerous_remote_bind_ack: DEFAULT_DANGEROUS_REMOTE_BIND_ACK,
        }
    }
}

impl Default for IdentityConfig {
    fn default() -> Self {
        Self { allow_insecure_node_rpc_without_mtls: DEFAULT_ALLOW_INSECURE_NODE_RPC_WITHOUT_MTLS }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            journal_db_path: PathBuf::from(DEFAULT_JOURNAL_DB_PATH),
            journal_hash_chain_enabled: DEFAULT_JOURNAL_HASH_CHAIN_ENABLED,
            max_journal_payload_bytes: DEFAULT_MAX_JOURNAL_PAYLOAD_BYTES,
            max_journal_events: DEFAULT_MAX_JOURNAL_EVENTS,
            vault_dir: default_vault_dir(),
        }
    }
}

/// Returns the default vault directory: a `vault` sibling of the identity
/// store (i.e. `<state-root>/vault`), so vault and identity material live
/// under the same state root by default.
pub(super) fn default_vault_dir() -> PathBuf {
    // Fall back to a relative .palyra/identity root when the platform state
    // root cannot be resolved; the loader later re-anchors relative paths
    // against the runtime state root.
    let identity_root =
        default_identity_store_root().unwrap_or_else(|_| PathBuf::from(".palyra/identity"));
    if let Some(parent) = identity_root.parent() {
        parent.join("vault")
    } else {
        identity_root.join("vault")
    }
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            grpc_bind_addr: DEFAULT_GRPC_BIND_ADDR.to_owned(),
            grpc_port: DEFAULT_GRPC_PORT,
            quic_bind_addr: DEFAULT_QUIC_BIND_ADDR.to_owned(),
            quic_port: DEFAULT_QUIC_PORT,
            quic_enabled: DEFAULT_QUIC_ENABLED,
            bind_profile: DEFAULT_GATEWAY_BIND_PROFILE,
            allow_insecure_remote: DEFAULT_GATEWAY_ALLOW_INSECURE_REMOTE,
            identity_store_dir: None,
            vault_get_approval_required_refs: DEFAULT_GATEWAY_VAULT_GET_APPROVAL_REQUIRED_REFS
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
            max_tape_entries_per_response: DEFAULT_GATEWAY_MAX_TAPE_ENTRIES_PER_RESPONSE,
            max_tape_bytes_per_response: DEFAULT_GATEWAY_MAX_TAPE_BYTES_PER_RESPONSE,
            tls: GatewayTlsConfig::default(),
        }
    }
}

impl Default for GatewayTlsConfig {
    fn default() -> Self {
        Self {
            enabled: DEFAULT_GATEWAY_TLS_ENABLED,
            cert_path: None,
            key_path: None,
            client_ca_path: None,
        }
    }
}

impl Default for CronConfig {
    fn default() -> Self {
        Self { timezone: DEFAULT_CRON_TIMEZONE_MODE }
    }
}

impl Default for OrchestratorConfig {
    fn default() -> Self {
        Self { runloop_v1_enabled: DEFAULT_ORCHESTRATOR_RUNLOOP_V1_ENABLED }
    }
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            max_item_bytes: DEFAULT_MEMORY_MAX_ITEM_BYTES,
            max_item_tokens: DEFAULT_MEMORY_MAX_ITEM_TOKENS,
            default_ttl_ms: Some(DEFAULT_MEMORY_DEFAULT_TTL_MS),
            auto_inject: MemoryAutoInjectConfig::default(),
            retention: MemoryRetentionConfig::default(),
            retrieval: RetrievalRuntimeConfig::default(),
        }
    }
}

impl Default for MemoryAutoInjectConfig {
    fn default() -> Self {
        Self {
            enabled: DEFAULT_MEMORY_AUTO_INJECT_ENABLED,
            max_items: DEFAULT_MEMORY_AUTO_INJECT_MAX_ITEMS,
        }
    }
}

impl Default for MemoryRetentionConfig {
    fn default() -> Self {
        Self {
            max_entries: None,
            max_bytes: None,
            ttl_days: None,
            vacuum_schedule: DEFAULT_MEMORY_RETENTION_VACUUM_SCHEDULE.to_owned(),
        }
    }
}

impl Default for ToolCallConfig {
    fn default() -> Self {
        Self {
            allowed_tools: Vec::new(),
            explicit_allowed_tools: Vec::new(),
            toolset_profiles: Vec::new(),
            extra_tools: Vec::new(),
            disabled_tools: Vec::new(),
            catalog_exposure_mode: palyra_common::tool_catalog::ToolCatalogExposureMode::Direct,
            compact_tool_threshold: 16,
            max_calls_per_run: DEFAULT_TOOL_CALL_MAX_CALLS_PER_RUN,
            execution_timeout_ms: DEFAULT_TOOL_CALL_EXECUTION_TIMEOUT_MS,
            process_runner: ProcessRunnerConfig::default(),
            code_intel: CodeIntelConfig::default(),
            wasm_runtime: WasmRuntimeConfig::default(),
            http_fetch: HttpFetchConfig::default(),
            browser_service: BrowserServiceConfig::default(),
        }
    }
}

impl Default for CodeIntelConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            workspace_root: None,
            rust_analyzer_binary: "rust-analyzer".to_owned(),
            typescript_server_binary: "typescript-language-server".to_owned(),
            pyright_binary: "pyright-langserver".to_owned(),
            timeout_ms: 2_000,
            max_output_bytes: 64 * 1024,
            max_items: 64,
            idle_reap_ms: 5 * 60 * 1_000,
        }
    }
}

impl Default for ProcessRunnerConfig {
    fn default() -> Self {
        Self {
            enabled: DEFAULT_PROCESS_RUNNER_ENABLED,
            tier: DEFAULT_PROCESS_RUNNER_TIER,
            workspace_root: PathBuf::from(DEFAULT_PROCESS_RUNNER_WORKSPACE_ROOT),
            path_access_mode: DEFAULT_PROCESS_RUNNER_PATH_ACCESS_MODE,
            allowed_executables: Vec::new(),
            allow_interpreters: DEFAULT_PROCESS_RUNNER_ALLOW_INTERPRETERS,
            egress_enforcement_mode: DEFAULT_PROCESS_RUNNER_EGRESS_ENFORCEMENT_MODE,
            allowed_egress_hosts: Vec::new(),
            allowed_dns_suffixes: Vec::new(),
            cpu_time_limit_ms: DEFAULT_PROCESS_RUNNER_CPU_TIME_LIMIT_MS,
            memory_limit_bytes: DEFAULT_PROCESS_RUNNER_MEMORY_LIMIT_BYTES,
            max_output_bytes: DEFAULT_PROCESS_RUNNER_MAX_OUTPUT_BYTES,
        }
    }
}

impl Default for WasmRuntimeConfig {
    fn default() -> Self {
        Self {
            enabled: DEFAULT_WASM_RUNTIME_ENABLED,
            allow_inline_modules: DEFAULT_WASM_RUNTIME_ALLOW_INLINE_MODULES,
            max_module_size_bytes: DEFAULT_WASM_RUNTIME_MAX_MODULE_SIZE_BYTES,
            fuel_budget: DEFAULT_WASM_RUNTIME_FUEL_BUDGET,
            max_memory_bytes: DEFAULT_WASM_RUNTIME_MAX_MEMORY_BYTES,
            max_table_elements: DEFAULT_WASM_RUNTIME_MAX_TABLE_ELEMENTS,
            max_instances: DEFAULT_WASM_RUNTIME_MAX_INSTANCES,
            allowed_http_hosts: Vec::new(),
            allowed_secrets: Vec::new(),
            allowed_storage_prefixes: Vec::new(),
            allowed_channels: Vec::new(),
        }
    }
}

impl Default for HttpFetchConfig {
    fn default() -> Self {
        Self {
            allow_private_targets: DEFAULT_HTTP_FETCH_ALLOW_PRIVATE_TARGETS,
            connect_timeout_ms: DEFAULT_HTTP_FETCH_CONNECT_TIMEOUT_MS,
            request_timeout_ms: DEFAULT_HTTP_FETCH_REQUEST_TIMEOUT_MS,
            max_response_bytes: DEFAULT_HTTP_FETCH_MAX_RESPONSE_BYTES,
            allow_redirects: DEFAULT_HTTP_FETCH_ALLOW_REDIRECTS,
            max_redirects: DEFAULT_HTTP_FETCH_MAX_REDIRECTS,
            allowed_content_types: DEFAULT_HTTP_FETCH_ALLOWED_CONTENT_TYPES
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
            allowed_request_headers: DEFAULT_HTTP_FETCH_ALLOWED_REQUEST_HEADERS
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
            allowed_credential_vault_refs: Vec::new(),
            cache_enabled: DEFAULT_HTTP_FETCH_CACHE_ENABLED,
            cache_ttl_ms: DEFAULT_HTTP_FETCH_CACHE_TTL_MS,
            max_cache_entries: DEFAULT_HTTP_FETCH_MAX_CACHE_ENTRIES,
        }
    }
}

impl Default for BrowserServiceConfig {
    fn default() -> Self {
        Self {
            enabled: DEFAULT_BROWSER_SERVICE_ENABLED,
            endpoint: DEFAULT_BROWSER_SERVICE_ENDPOINT.to_owned(),
            auth_token: None,
            auth_token_secret_ref: None,
            state_dir: None,
            state_key_secret_ref: None,
            state_key_vault_ref: None,
            connect_timeout_ms: DEFAULT_BROWSER_SERVICE_CONNECT_TIMEOUT_MS,
            request_timeout_ms: DEFAULT_BROWSER_SERVICE_REQUEST_TIMEOUT_MS,
            max_screenshot_bytes: DEFAULT_BROWSER_SERVICE_MAX_SCREENSHOT_BYTES,
            max_title_bytes: DEFAULT_BROWSER_SERVICE_MAX_TITLE_BYTES,
        }
    }
}

impl Default for CanvasHostConfig {
    fn default() -> Self {
        Self {
            enabled: DEFAULT_CANVAS_HOST_ENABLED,
            public_base_url: DEFAULT_CANVAS_HOST_PUBLIC_BASE_URL.to_owned(),
            token_ttl_ms: DEFAULT_CANVAS_HOST_TOKEN_TTL_MS,
            max_state_bytes: DEFAULT_CANVAS_HOST_MAX_STATE_BYTES,
            max_bundle_bytes: DEFAULT_CANVAS_HOST_MAX_BUNDLE_BYTES,
            max_assets_per_bundle: DEFAULT_CANVAS_HOST_MAX_ASSETS_PER_BUNDLE,
            max_updates_per_minute: DEFAULT_CANVAS_HOST_MAX_UPDATES_PER_MINUTE,
        }
    }
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            require_auth: DEFAULT_ADMIN_REQUIRE_AUTH,
            auth_token: None,
            auth_token_secret_ref: None,
            connector_token: None,
            connector_token_secret_ref: None,
            bound_principal: None,
        }
    }
}
