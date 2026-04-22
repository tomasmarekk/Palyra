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
use crate::sandbox_runner::{EgressEnforcementMode, SandboxProcessRunnerTier};

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
const DEFAULT_GATEWAY_VAULT_GET_APPROVAL_REQUIRED_REFS: &[&str] = &["global/openai_api_key"];
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
const DEFAULT_MEMORY_MAX_ITEM_BYTES: usize = 16 * 1024;
const DEFAULT_MEMORY_MAX_ITEM_TOKENS: usize = 2_048;
const DEFAULT_MEMORY_DEFAULT_TTL_MS: i64 = 30 * 24 * 60 * 60 * 1_000;
const DEFAULT_MEMORY_AUTO_INJECT_ENABLED: bool = false;
const DEFAULT_MEMORY_AUTO_INJECT_MAX_ITEMS: usize = 3;
const DEFAULT_MEMORY_RETENTION_VACUUM_SCHEDULE: &str = "0 0 * * 0";
const DEFAULT_ADMIN_REQUIRE_AUTH: bool = true;
const DEFAULT_ALLOW_INSECURE_NODE_RPC_WITHOUT_MTLS: bool = false;
const DEFAULT_JOURNAL_DB_PATH: &str = "data/journal.sqlite3";
const DEFAULT_JOURNAL_HASH_CHAIN_ENABLED: bool = true;
const DEFAULT_MAX_JOURNAL_PAYLOAD_BYTES: usize = 256 * 1024;
const DEFAULT_MAX_JOURNAL_EVENTS: usize = 10_000;
const DEFAULT_TOOL_CALL_MAX_CALLS_PER_RUN: u32 = 4;
const DEFAULT_TOOL_CALL_EXECUTION_TIMEOUT_MS: u64 = 750;
const DEFAULT_PROCESS_RUNNER_ENABLED: bool = false;
const DEFAULT_PROCESS_RUNNER_TIER: SandboxProcessRunnerTier = SandboxProcessRunnerTier::B;
const DEFAULT_PROCESS_RUNNER_WORKSPACE_ROOT: &str = ".";
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
const DEFAULT_HTTP_FETCH_ALLOWED_CONTENT_TYPES: &[&str] =
    &["text/html", "text/plain", "application/json"];
const DEFAULT_HTTP_FETCH_ALLOWED_REQUEST_HEADERS: &[&str] =
    &["accept", "accept-language", "if-none-match", "if-modified-since", "user-agent"];
const DEFAULT_HTTP_FETCH_CACHE_ENABLED: bool = true;
const DEFAULT_HTTP_FETCH_CACHE_TTL_MS: u64 = 30_000;
const DEFAULT_HTTP_FETCH_MAX_CACHE_ENTRIES: u64 = 256;
const DEFAULT_BROWSER_SERVICE_ENABLED: bool = false;
const DEFAULT_BROWSER_SERVICE_ENDPOINT: &str = "http://127.0.0.1:7543";
const DEFAULT_BROWSER_SERVICE_CONNECT_TIMEOUT_MS: u64 = 1_500;
const DEFAULT_BROWSER_SERVICE_REQUEST_TIMEOUT_MS: u64 = 15_000;
const DEFAULT_BROWSER_SERVICE_MAX_SCREENSHOT_BYTES: u64 = 256 * 1024;
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedConfig {
    pub source: String,
    pub config_version: u32,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentConfig {
    pub mode: DeploymentMode,
    pub dangerous_remote_bind_ack: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeploymentMode {
    LocalDesktop,
    RemoteVps,
}

impl DeploymentMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LocalDesktop => "local_desktop",
            Self::RemoteVps => "remote_vps",
        }
    }

    pub fn parse(raw: &str, source_name: &str) -> Result<Self> {
        let normalized = raw.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "local_desktop" | "local-desktop" | "local" => Ok(Self::LocalDesktop),
            "remote_vps" | "remote-vps" | "remote" | "vps" => Ok(Self::RemoteVps),
            _ => anyhow::bail!("{source_name} must be one of: local_desktop | remote_vps"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayBindProfile {
    LoopbackOnly,
    PublicTls,
}

impl GatewayBindProfile {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LoopbackOnly => "loopback_only",
            Self::PublicTls => "public_tls",
        }
    }

    pub fn parse(raw: &str, source_name: &str) -> Result<Self> {
        let normalized = raw.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "loopback_only" | "loopback-only" | "loopback" => Ok(Self::LoopbackOnly),
            "public_tls" | "public-tls" | "public" => Ok(Self::PublicTls),
            _ => anyhow::bail!("{source_name} must be one of: loopback_only | public_tls"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonConfig {
    pub bind_addr: String,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayConfig {
    pub grpc_bind_addr: String,
    pub grpc_port: u16,
    pub quic_bind_addr: String,
    pub quic_port: u16,
    pub quic_enabled: bool,
    pub bind_profile: GatewayBindProfile,
    pub allow_insecure_remote: bool,
    pub identity_store_dir: Option<PathBuf>,
    pub vault_get_approval_required_refs: Vec<String>,
    pub max_tape_entries_per_response: usize,
    pub max_tape_bytes_per_response: usize,
    pub tls: GatewayTlsConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayTlsConfig {
    pub enabled: bool,
    pub cert_path: Option<PathBuf>,
    pub key_path: Option<PathBuf>,
    pub client_ca_path: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronConfig {
    pub timezone: CronTimezoneMode,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FeatureRolloutsConfig {
    pub dynamic_tool_builder: FeatureRolloutSetting,
    pub context_engine: FeatureRolloutSetting,
    pub execution_backend_remote_node: FeatureRolloutSetting,
    pub execution_backend_networked_worker: FeatureRolloutSetting,
    pub execution_backend_ssh_tunnel: FeatureRolloutSetting,
    pub safety_boundary: FeatureRolloutSetting,
    pub execution_gate_pipeline_v2: FeatureRolloutSetting,
    pub session_queue_policy: FeatureRolloutSetting,
    pub pruning_policy_matrix: FeatureRolloutSetting,
    pub retrieval_dual_path: FeatureRolloutSetting,
    pub auxiliary_executor: FeatureRolloutSetting,
    pub flow_orchestration: FeatureRolloutSetting,
    pub delivery_arbitration: FeatureRolloutSetting,
    pub replay_capture: FeatureRolloutSetting,
    pub networked_workers: FeatureRolloutSetting,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrchestratorConfig {
    pub runloop_v1_enabled: bool,
}

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryConfig {
    pub max_item_bytes: usize,
    pub max_item_tokens: usize,
    pub default_ttl_ms: Option<i64>,
    pub auto_inject: MemoryAutoInjectConfig,
    pub retention: MemoryRetentionConfig,
    pub retrieval: RetrievalRuntimeConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryAutoInjectConfig {
    pub enabled: bool,
    pub max_items: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryRetentionConfig {
    pub max_entries: Option<usize>,
    pub max_bytes: Option<u64>,
    pub ttl_days: Option<u32>,
    pub vacuum_schedule: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCallConfig {
    pub allowed_tools: Vec<String>,
    pub max_calls_per_run: u32,
    pub execution_timeout_ms: u64,
    pub process_runner: ProcessRunnerConfig,
    pub wasm_runtime: WasmRuntimeConfig,
    pub http_fetch: HttpFetchConfig,
    pub browser_service: BrowserServiceConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessRunnerConfig {
    pub enabled: bool,
    pub tier: SandboxProcessRunnerTier,
    pub workspace_root: PathBuf,
    pub allowed_executables: Vec<String>,
    pub allow_interpreters: bool,
    pub egress_enforcement_mode: EgressEnforcementMode,
    pub allowed_egress_hosts: Vec<String>,
    pub allowed_dns_suffixes: Vec<String>,
    pub cpu_time_limit_ms: u64,
    pub memory_limit_bytes: u64,
    pub max_output_bytes: u64,
}

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
    pub allowed_secrets: Vec<String>,
    pub allowed_storage_prefixes: Vec<String>,
    pub allowed_channels: Vec<String>,
}

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
    pub cache_enabled: bool,
    pub cache_ttl_ms: u64,
    pub max_cache_entries: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrowserServiceConfig {
    pub enabled: bool,
    pub endpoint: String,
    pub auth_token: Option<String>,
    pub auth_token_secret_ref: Option<SecretRef>,
    pub state_dir: Option<PathBuf>,
    pub state_key_secret_ref: Option<SecretRef>,
    pub state_key_vault_ref: Option<String>,
    pub connect_timeout_ms: u64,
    pub request_timeout_ms: u64,
    pub max_screenshot_bytes: u64,
    pub max_title_bytes: u64,
}

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminConfig {
    pub require_auth: bool,
    pub auth_token: Option<String>,
    pub auth_token_secret_ref: Option<SecretRef>,
    pub connector_token: Option<String>,
    pub connector_token_secret_ref: Option<SecretRef>,
    pub bound_principal: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityConfig {
    pub allow_insecure_node_rpc_without_mtls: bool,
}

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

pub(super) fn default_vault_dir() -> PathBuf {
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
            max_calls_per_run: DEFAULT_TOOL_CALL_MAX_CALLS_PER_RUN,
            execution_timeout_ms: DEFAULT_TOOL_CALL_EXECUTION_TIMEOUT_MS,
            process_runner: ProcessRunnerConfig::default(),
            wasm_runtime: WasmRuntimeConfig::default(),
            http_fetch: HttpFetchConfig::default(),
            browser_service: BrowserServiceConfig::default(),
        }
    }
}

impl Default for ProcessRunnerConfig {
    fn default() -> Self {
        Self {
            enabled: DEFAULT_PROCESS_RUNNER_ENABLED,
            tier: DEFAULT_PROCESS_RUNNER_TIER,
            workspace_root: PathBuf::from(DEFAULT_PROCESS_RUNNER_WORKSPACE_ROOT),
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
