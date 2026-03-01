use std::{
    env, fs,
    path::{Component, PathBuf},
};

use anyhow::{Context, Result};
use palyra_common::{
    config_system::{
        parse_document_with_migration, serialize_document_pretty, ConfigMigrationInfo,
    },
    daemon_config_schema::RootFileConfig,
    default_config_search_paths, default_identity_store_root, parse_config_path,
};
use palyra_vault::VaultRef;

use crate::channel_router::{BroadcastStrategy, ChannelRouterConfig, ChannelRoutingRule};
use crate::cron::CronTimezoneMode;
use crate::model_provider::{
    validate_openai_base_url_network_policy, ModelProviderAuthProviderKind, ModelProviderConfig,
    ModelProviderKind,
};
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
const DEFAULT_MEMORY_MAX_ITEM_BYTES: usize = 16 * 1024;
const DEFAULT_MEMORY_MAX_ITEM_TOKENS: usize = 2_048;
const DEFAULT_MEMORY_DEFAULT_TTL_MS: i64 = 30 * 24 * 60 * 60 * 1_000;
const DEFAULT_MEMORY_AUTO_INJECT_ENABLED: bool = false;
const DEFAULT_MEMORY_AUTO_INJECT_MAX_ITEMS: usize = 3;
const DEFAULT_ADMIN_REQUIRE_AUTH: bool = true;
const DEFAULT_ALLOW_INSECURE_NODE_RPC_WITHOUT_MTLS: bool = false;
const DEFAULT_JOURNAL_DB_PATH: &str = "data/journal.sqlite3";
const DEFAULT_JOURNAL_HASH_CHAIN_ENABLED: bool = true;
const DEFAULT_MAX_JOURNAL_PAYLOAD_BYTES: usize = 256 * 1024;
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedConfig {
    pub source: String,
    pub config_version: u32,
    pub migrated_from_version: Option<u32>,
    pub daemon: DaemonConfig,
    pub gateway: GatewayConfig,
    pub cron: CronConfig,
    pub orchestrator: OrchestratorConfig,
    pub memory: MemoryConfig,
    pub model_provider: ModelProviderConfig,
    pub tool_call: ToolCallConfig,
    pub channel_router: ChannelRouterConfig,
    pub canvas_host: CanvasHostConfig,
    pub admin: AdminConfig,
    pub identity: IdentityConfig,
    pub storage: StorageConfig,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrchestratorConfig {
    pub runloop_v1_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryConfig {
    pub max_item_bytes: usize,
    pub max_item_tokens: usize,
    pub default_ttl_ms: Option<i64>,
    pub auto_inject: MemoryAutoInjectConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryAutoInjectConfig {
    pub enabled: bool,
    pub max_items: usize,
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
    pub state_dir: Option<PathBuf>,
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
    pub vault_dir: PathBuf,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self { bind_addr: DEFAULT_BIND_ADDR.to_owned(), port: DEFAULT_PORT }
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
            vault_dir: default_vault_dir(),
        }
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
            state_dir: None,
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
        Self { require_auth: DEFAULT_ADMIN_REQUIRE_AUTH, auth_token: None, bound_principal: None }
    }
}

pub fn load_config() -> Result<LoadedConfig> {
    let mut daemon = DaemonConfig::default();
    let mut gateway = GatewayConfig::default();
    let mut cron = CronConfig::default();
    let mut orchestrator = OrchestratorConfig::default();
    let mut memory = MemoryConfig::default();
    let mut model_provider = ModelProviderConfig::default();
    let mut tool_call = ToolCallConfig::default();
    let mut channel_router = ChannelRouterConfig::default();
    let mut canvas_host = CanvasHostConfig::default();
    let mut admin = AdminConfig::default();
    let mut identity = IdentityConfig::default();
    let mut storage = StorageConfig::default();
    let mut source = "defaults".to_owned();
    let mut config_version = 1_u32;
    let mut migrated_from_version = None;

    if let Some(path) = find_config_path()? {
        let content = fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let (parsed, migration) = parse_root_file_config(&content)
            .with_context(|| format!("failed to parse config {}", path.display()))?;
        config_version = migration.target_version;
        if migration.migrated {
            migrated_from_version = Some(migration.source_version);
        }
        if let Some(file_daemon) = parsed.daemon {
            if let Some(bind_addr) = file_daemon.bind_addr {
                daemon.bind_addr = bind_addr;
            }
            if let Some(port) = file_daemon.port {
                daemon.port = port;
            }
        }
        if let Some(file_gateway) = parsed.gateway {
            if let Some(grpc_bind_addr) = file_gateway.grpc_bind_addr {
                gateway.grpc_bind_addr = grpc_bind_addr;
            }
            if let Some(grpc_port) = file_gateway.grpc_port {
                gateway.grpc_port = grpc_port;
            }
            if let Some(quic_bind_addr) = file_gateway.quic_bind_addr {
                gateway.quic_bind_addr = quic_bind_addr;
            }
            if let Some(quic_port) = file_gateway.quic_port {
                gateway.quic_port = quic_port;
            }
            if let Some(quic_enabled) = file_gateway.quic_enabled {
                gateway.quic_enabled = quic_enabled;
            }
            if let Some(allow_insecure_remote) = file_gateway.allow_insecure_remote {
                gateway.allow_insecure_remote = allow_insecure_remote;
            }
            if let Some(identity_store_dir) = file_gateway.identity_store_dir {
                gateway.identity_store_dir =
                    Some(parse_identity_store_dir(identity_store_dir.as_str())?);
            }
            if let Some(vault_get_approval_required_refs) =
                file_gateway.vault_get_approval_required_refs
            {
                gateway.vault_get_approval_required_refs = parse_vault_ref_allowlist(
                    vault_get_approval_required_refs.join(",").as_str(),
                    "gateway.vault_get_approval_required_refs",
                )?;
            }
            if let Some(max_tape_entries_per_response) = file_gateway.max_tape_entries_per_response
            {
                gateway.max_tape_entries_per_response = parse_positive_usize(
                    max_tape_entries_per_response,
                    "gateway.max_tape_entries_per_response",
                )?;
            }
            if let Some(max_tape_bytes_per_response) = file_gateway.max_tape_bytes_per_response {
                gateway.max_tape_bytes_per_response = parse_positive_usize(
                    max_tape_bytes_per_response,
                    "gateway.max_tape_bytes_per_response",
                )?;
            }
            if let Some(file_tls) = file_gateway.tls {
                if let Some(enabled) = file_tls.enabled {
                    gateway.tls.enabled = enabled;
                }
                if let Some(cert_path) = file_tls.cert_path {
                    gateway.tls.cert_path = Some(parse_gateway_tls_path(cert_path.as_str())?);
                }
                if let Some(key_path) = file_tls.key_path {
                    gateway.tls.key_path = Some(parse_gateway_tls_path(key_path.as_str())?);
                }
                if let Some(client_ca_path) = file_tls.client_ca_path {
                    gateway.tls.client_ca_path =
                        Some(parse_gateway_tls_path(client_ca_path.as_str())?);
                }
            }
        }
        if let Some(file_cron) = parsed.cron {
            if let Some(timezone) = file_cron.timezone {
                cron.timezone = parse_cron_timezone_mode(timezone.as_str(), "cron.timezone")?;
            }
        }
        if let Some(file_orchestrator) = parsed.orchestrator {
            if let Some(runloop_v1_enabled) = file_orchestrator.runloop_v1_enabled {
                orchestrator.runloop_v1_enabled = runloop_v1_enabled;
            }
        }
        if let Some(file_memory) = parsed.memory {
            if let Some(max_item_bytes) = file_memory.max_item_bytes {
                memory.max_item_bytes =
                    parse_positive_usize(max_item_bytes, "memory.max_item_bytes")?;
            }
            if let Some(max_item_tokens) = file_memory.max_item_tokens {
                memory.max_item_tokens =
                    parse_positive_usize(max_item_tokens, "memory.max_item_tokens")?;
            }
            if let Some(default_ttl_ms) = file_memory.default_ttl_ms {
                memory.default_ttl_ms =
                    parse_default_memory_ttl_ms(default_ttl_ms, "memory.default_ttl_ms")?;
            }
            if let Some(file_auto_inject) = file_memory.auto_inject {
                if let Some(enabled) = file_auto_inject.enabled {
                    memory.auto_inject.enabled = enabled;
                }
                if let Some(max_items) = file_auto_inject.max_items {
                    memory.auto_inject.max_items =
                        parse_positive_usize(max_items, "memory.auto_inject.max_items")?;
                }
            }
        }
        if let Some(file_model_provider) = parsed.model_provider {
            if let Some(kind) = file_model_provider.kind {
                model_provider.kind = ModelProviderKind::parse(kind.as_str())
                    .context("model_provider.kind must be deterministic or openai_compatible")?;
            }
            if let Some(openai_base_url) = file_model_provider.openai_base_url {
                model_provider.openai_base_url = parse_openai_base_url(openai_base_url.as_str())?;
            }
            if let Some(allow_private_base_url) = file_model_provider.allow_private_base_url {
                model_provider.allow_private_base_url = allow_private_base_url;
            }
            if let Some(openai_model) = file_model_provider.openai_model {
                model_provider.openai_model = parse_openai_model(openai_model.as_str())?;
            }
            if let Some(openai_embeddings_model) = file_model_provider.openai_embeddings_model {
                model_provider.openai_embeddings_model =
                    parse_optional_openai_embeddings_model(openai_embeddings_model.as_str())?;
            }
            if let Some(openai_embeddings_dims) = file_model_provider.openai_embeddings_dims {
                model_provider.openai_embeddings_dims = Some(parse_openai_embeddings_dims(
                    openai_embeddings_dims,
                    "model_provider.openai_embeddings_dims",
                )?);
            }
            if let Some(openai_api_key) = file_model_provider.openai_api_key {
                model_provider.openai_api_key =
                    if openai_api_key.trim().is_empty() { None } else { Some(openai_api_key) };
            }
            if let Some(openai_api_key_vault_ref) = file_model_provider.openai_api_key_vault_ref {
                model_provider.openai_api_key_vault_ref =
                    if openai_api_key_vault_ref.trim().is_empty() {
                        None
                    } else {
                        Some(openai_api_key_vault_ref)
                    };
            }
            if let Some(auth_profile_ref) = file_model_provider.auth_profile_ref {
                model_provider.auth_profile_id = parse_optional_auth_profile_id(
                    auth_profile_ref.as_str(),
                    "model_provider.auth_profile_ref",
                )?;
            }
            if let Some(auth_profile_id) = file_model_provider.auth_profile_id {
                model_provider.auth_profile_id = parse_optional_auth_profile_id(
                    auth_profile_id.as_str(),
                    "model_provider.auth_profile_id",
                )?;
            }
            if let Some(auth_provider_kind) = file_model_provider.auth_provider_kind {
                model_provider.auth_profile_provider_kind =
                    Some(parse_model_provider_auth_provider_kind(
                        auth_provider_kind.as_str(),
                        "model_provider.auth_provider_kind",
                    )?);
            }
            if let Some(request_timeout_ms) = file_model_provider.request_timeout_ms {
                model_provider.request_timeout_ms =
                    parse_positive_u64(request_timeout_ms, "model_provider.request_timeout_ms")?;
            }
            if let Some(max_retries) = file_model_provider.max_retries {
                model_provider.max_retries =
                    parse_retries(max_retries, "model_provider.max_retries")?;
            }
            if let Some(retry_backoff_ms) = file_model_provider.retry_backoff_ms {
                model_provider.retry_backoff_ms =
                    parse_positive_u64(retry_backoff_ms, "model_provider.retry_backoff_ms")?;
            }
            if let Some(failure_threshold) = file_model_provider.circuit_breaker_failure_threshold {
                model_provider.circuit_breaker_failure_threshold = parse_positive_u32(
                    failure_threshold,
                    "model_provider.circuit_breaker_failure_threshold",
                )?;
            }
            if let Some(cooldown_ms) = file_model_provider.circuit_breaker_cooldown_ms {
                model_provider.circuit_breaker_cooldown_ms =
                    parse_positive_u64(cooldown_ms, "model_provider.circuit_breaker_cooldown_ms")?;
            }
        }
        if let Some(file_tool_call) = parsed.tool_call {
            if let Some(allowed_tools) = file_tool_call.allowed_tools {
                tool_call.allowed_tools = parse_tool_allowlist(
                    allowed_tools.join(",").as_str(),
                    "tool_call.allowed_tools",
                )?;
            }
            if let Some(max_calls_per_run) = file_tool_call.max_calls_per_run {
                tool_call.max_calls_per_run =
                    parse_positive_u32(max_calls_per_run, "tool_call.max_calls_per_run")?;
            }
            if let Some(execution_timeout_ms) = file_tool_call.execution_timeout_ms {
                tool_call.execution_timeout_ms =
                    parse_positive_u64(execution_timeout_ms, "tool_call.execution_timeout_ms")?;
            }
            if let Some(file_process_runner) = file_tool_call.process_runner {
                if let Some(enabled) = file_process_runner.enabled {
                    tool_call.process_runner.enabled = enabled;
                }
                if let Some(tier) = file_process_runner.tier {
                    tool_call.process_runner.tier =
                        parse_process_runner_tier(tier.as_str(), "tool_call.process_runner.tier")?;
                }
                if let Some(workspace_root) = file_process_runner.workspace_root {
                    tool_call.process_runner.workspace_root =
                        parse_workspace_root(workspace_root.as_str())?;
                }
                if let Some(allowed_executables) = file_process_runner.allowed_executables {
                    tool_call.process_runner.allowed_executables =
                        parse_process_executable_allowlist(
                            allowed_executables.join(",").as_str(),
                            "tool_call.process_runner.allowed_executables",
                        )?;
                }
                if let Some(allow_interpreters) = file_process_runner.allow_interpreters {
                    tool_call.process_runner.allow_interpreters = allow_interpreters;
                }
                if let Some(egress_enforcement_mode) = file_process_runner.egress_enforcement_mode {
                    tool_call.process_runner.egress_enforcement_mode =
                        parse_process_runner_egress_enforcement_mode(
                            egress_enforcement_mode.as_str(),
                            "tool_call.process_runner.egress_enforcement_mode",
                        )?;
                }
                if let Some(allowed_egress_hosts) = file_process_runner.allowed_egress_hosts {
                    tool_call.process_runner.allowed_egress_hosts = parse_host_allowlist(
                        allowed_egress_hosts.join(",").as_str(),
                        "tool_call.process_runner.allowed_egress_hosts",
                    )?;
                }
                if let Some(allowed_dns_suffixes) = file_process_runner.allowed_dns_suffixes {
                    tool_call.process_runner.allowed_dns_suffixes = parse_dns_suffix_allowlist(
                        allowed_dns_suffixes.join(",").as_str(),
                        "tool_call.process_runner.allowed_dns_suffixes",
                    )?;
                }
                if let Some(cpu_time_limit_ms) = file_process_runner.cpu_time_limit_ms {
                    tool_call.process_runner.cpu_time_limit_ms = parse_positive_u64(
                        cpu_time_limit_ms,
                        "tool_call.process_runner.cpu_time_limit_ms",
                    )?;
                }
                if let Some(memory_limit_bytes) = file_process_runner.memory_limit_bytes {
                    tool_call.process_runner.memory_limit_bytes = parse_positive_u64(
                        memory_limit_bytes,
                        "tool_call.process_runner.memory_limit_bytes",
                    )?;
                }
                if let Some(max_output_bytes) = file_process_runner.max_output_bytes {
                    tool_call.process_runner.max_output_bytes = parse_positive_u64(
                        max_output_bytes,
                        "tool_call.process_runner.max_output_bytes",
                    )?;
                }
            }
            if let Some(file_wasm_runtime) = file_tool_call.wasm_runtime {
                if let Some(enabled) = file_wasm_runtime.enabled {
                    tool_call.wasm_runtime.enabled = enabled;
                }
                if let Some(allow_inline_modules) = file_wasm_runtime.allow_inline_modules {
                    tool_call.wasm_runtime.allow_inline_modules = allow_inline_modules;
                }
                if let Some(max_module_size_bytes) = file_wasm_runtime.max_module_size_bytes {
                    tool_call.wasm_runtime.max_module_size_bytes = parse_positive_u64(
                        max_module_size_bytes,
                        "tool_call.wasm_runtime.max_module_size_bytes",
                    )?;
                }
                if let Some(fuel_budget) = file_wasm_runtime.fuel_budget {
                    tool_call.wasm_runtime.fuel_budget =
                        parse_positive_u64(fuel_budget, "tool_call.wasm_runtime.fuel_budget")?;
                }
                if let Some(max_memory_bytes) = file_wasm_runtime.max_memory_bytes {
                    tool_call.wasm_runtime.max_memory_bytes = parse_positive_u64(
                        max_memory_bytes,
                        "tool_call.wasm_runtime.max_memory_bytes",
                    )?;
                }
                if let Some(max_table_elements) = file_wasm_runtime.max_table_elements {
                    tool_call.wasm_runtime.max_table_elements = parse_positive_u64(
                        max_table_elements,
                        "tool_call.wasm_runtime.max_table_elements",
                    )?;
                }
                if let Some(max_instances) = file_wasm_runtime.max_instances {
                    tool_call.wasm_runtime.max_instances =
                        parse_positive_u64(max_instances, "tool_call.wasm_runtime.max_instances")?;
                }
                if let Some(allowed_http_hosts) = file_wasm_runtime.allowed_http_hosts {
                    tool_call.wasm_runtime.allowed_http_hosts = parse_host_allowlist(
                        allowed_http_hosts.join(",").as_str(),
                        "tool_call.wasm_runtime.allowed_http_hosts",
                    )?;
                }
                if let Some(allowed_secrets) = file_wasm_runtime.allowed_secrets {
                    tool_call.wasm_runtime.allowed_secrets = parse_identifier_allowlist(
                        allowed_secrets.join(",").as_str(),
                        "tool_call.wasm_runtime.allowed_secrets",
                        "secret handle",
                    )?;
                }
                if let Some(allowed_storage_prefixes) = file_wasm_runtime.allowed_storage_prefixes {
                    tool_call.wasm_runtime.allowed_storage_prefixes =
                        parse_storage_prefix_allowlist(
                            allowed_storage_prefixes.join(",").as_str(),
                            "tool_call.wasm_runtime.allowed_storage_prefixes",
                        )?;
                }
                if let Some(allowed_channels) = file_wasm_runtime.allowed_channels {
                    tool_call.wasm_runtime.allowed_channels = parse_identifier_allowlist(
                        allowed_channels.join(",").as_str(),
                        "tool_call.wasm_runtime.allowed_channels",
                        "channel handle",
                    )?;
                }
            }
            if let Some(file_http_fetch) = file_tool_call.http_fetch {
                if let Some(allow_private_targets) = file_http_fetch.allow_private_targets {
                    tool_call.http_fetch.allow_private_targets = allow_private_targets;
                }
                if let Some(connect_timeout_ms) = file_http_fetch.connect_timeout_ms {
                    tool_call.http_fetch.connect_timeout_ms = parse_positive_u64(
                        connect_timeout_ms,
                        "tool_call.http_fetch.connect_timeout_ms",
                    )?;
                }
                if let Some(request_timeout_ms) = file_http_fetch.request_timeout_ms {
                    tool_call.http_fetch.request_timeout_ms = parse_positive_u64(
                        request_timeout_ms,
                        "tool_call.http_fetch.request_timeout_ms",
                    )?;
                }
                if let Some(max_response_bytes) = file_http_fetch.max_response_bytes {
                    tool_call.http_fetch.max_response_bytes = parse_positive_u64(
                        max_response_bytes,
                        "tool_call.http_fetch.max_response_bytes",
                    )?;
                }
                if let Some(allow_redirects) = file_http_fetch.allow_redirects {
                    tool_call.http_fetch.allow_redirects = allow_redirects;
                }
                if let Some(max_redirects) = file_http_fetch.max_redirects {
                    tool_call.http_fetch.max_redirects =
                        parse_positive_u32(max_redirects, "tool_call.http_fetch.max_redirects")?;
                }
                if let Some(allowed_content_types) = file_http_fetch.allowed_content_types {
                    tool_call.http_fetch.allowed_content_types = parse_content_type_allowlist(
                        allowed_content_types.join(",").as_str(),
                        "tool_call.http_fetch.allowed_content_types",
                    )?;
                }
                if let Some(allowed_request_headers) = file_http_fetch.allowed_request_headers {
                    tool_call.http_fetch.allowed_request_headers = parse_http_header_allowlist(
                        allowed_request_headers.join(",").as_str(),
                        "tool_call.http_fetch.allowed_request_headers",
                    )?;
                }
                if let Some(cache_enabled) = file_http_fetch.cache_enabled {
                    tool_call.http_fetch.cache_enabled = cache_enabled;
                }
                if let Some(cache_ttl_ms) = file_http_fetch.cache_ttl_ms {
                    tool_call.http_fetch.cache_ttl_ms =
                        parse_positive_u64(cache_ttl_ms, "tool_call.http_fetch.cache_ttl_ms")?;
                }
                if let Some(max_cache_entries) = file_http_fetch.max_cache_entries {
                    tool_call.http_fetch.max_cache_entries = parse_positive_u64(
                        max_cache_entries,
                        "tool_call.http_fetch.max_cache_entries",
                    )?;
                }
            }
            if let Some(file_browser_service) = file_tool_call.browser_service {
                if let Some(enabled) = file_browser_service.enabled {
                    tool_call.browser_service.enabled = enabled;
                }
                if let Some(endpoint) = file_browser_service.endpoint {
                    tool_call.browser_service.endpoint = parse_browser_service_endpoint(
                        endpoint.as_str(),
                        "tool_call.browser_service.endpoint",
                    )?;
                }
                if let Some(auth_token) = file_browser_service.auth_token {
                    let trimmed = auth_token.trim();
                    tool_call.browser_service.auth_token =
                        if trimmed.is_empty() { None } else { Some(trimmed.to_owned()) };
                }
                if let Some(state_dir) = file_browser_service.state_dir {
                    tool_call.browser_service.state_dir = parse_optional_browser_state_dir(
                        state_dir.as_str(),
                        "tool_call.browser_service.state_dir",
                    )?;
                }
                if let Some(state_key_vault_ref) = file_browser_service.state_key_vault_ref {
                    tool_call.browser_service.state_key_vault_ref = parse_optional_vault_ref_field(
                        state_key_vault_ref.as_str(),
                        "tool_call.browser_service.state_key_vault_ref",
                    )?;
                }
                if let Some(connect_timeout_ms) = file_browser_service.connect_timeout_ms {
                    tool_call.browser_service.connect_timeout_ms = parse_positive_u64(
                        connect_timeout_ms,
                        "tool_call.browser_service.connect_timeout_ms",
                    )?;
                }
                if let Some(request_timeout_ms) = file_browser_service.request_timeout_ms {
                    tool_call.browser_service.request_timeout_ms = parse_positive_u64(
                        request_timeout_ms,
                        "tool_call.browser_service.request_timeout_ms",
                    )?;
                }
                if let Some(max_screenshot_bytes) = file_browser_service.max_screenshot_bytes {
                    tool_call.browser_service.max_screenshot_bytes = parse_positive_u64(
                        max_screenshot_bytes,
                        "tool_call.browser_service.max_screenshot_bytes",
                    )?;
                }
                if let Some(max_title_bytes) = file_browser_service.max_title_bytes {
                    tool_call.browser_service.max_title_bytes = parse_positive_u64(
                        max_title_bytes,
                        "tool_call.browser_service.max_title_bytes",
                    )?;
                }
            }
        }
        if let Some(file_channel_router) = parsed.channel_router {
            if let Some(enabled) = file_channel_router.enabled {
                channel_router.enabled = enabled;
            }
            if let Some(max_message_bytes) = file_channel_router.max_message_bytes {
                channel_router.max_message_bytes =
                    parse_positive_usize(max_message_bytes, "channel_router.max_message_bytes")?;
            }
            if let Some(max_retry_queue_depth_per_channel) =
                file_channel_router.max_retry_queue_depth_per_channel
            {
                channel_router.max_retry_queue_depth_per_channel = parse_positive_usize(
                    max_retry_queue_depth_per_channel,
                    "channel_router.max_retry_queue_depth_per_channel",
                )?;
            }
            if let Some(max_retry_attempts) = file_channel_router.max_retry_attempts {
                channel_router.max_retry_attempts =
                    parse_positive_u32(max_retry_attempts, "channel_router.max_retry_attempts")?;
            }
            if let Some(retry_backoff_ms) = file_channel_router.retry_backoff_ms {
                channel_router.retry_backoff_ms =
                    parse_positive_u64(retry_backoff_ms, "channel_router.retry_backoff_ms")?;
            }
            if let Some(default_response_prefix) = file_channel_router.default_response_prefix {
                channel_router.default_response_prefix = parse_optional_text_field(
                    default_response_prefix,
                    "channel_router.default_response_prefix",
                    256,
                )?;
            }
            if let Some(file_routing) = file_channel_router.routing {
                if let Some(default_channel_enabled) = file_routing.default_channel_enabled {
                    channel_router.default_channel_enabled = default_channel_enabled;
                }
                if let Some(default_allow_direct_messages) =
                    file_routing.default_allow_direct_messages
                {
                    channel_router.default_allow_direct_messages = default_allow_direct_messages;
                }
                if let Some(default_isolate_session_by_sender) =
                    file_routing.default_isolate_session_by_sender
                {
                    channel_router.default_isolate_session_by_sender =
                        default_isolate_session_by_sender;
                }
                if let Some(default_broadcast_strategy) = file_routing.default_broadcast_strategy {
                    channel_router.default_broadcast_strategy = parse_broadcast_strategy(
                        default_broadcast_strategy.as_str(),
                        "channel_router.routing.default_broadcast_strategy",
                    )?;
                }
                if let Some(default_concurrency_limit) = file_routing.default_concurrency_limit {
                    channel_router.default_concurrency_limit = parse_positive_usize(
                        default_concurrency_limit,
                        "channel_router.routing.default_concurrency_limit",
                    )?;
                }
                if let Some(channels) = file_routing.channels {
                    let mut parsed_channels = Vec::with_capacity(channels.len());
                    for (index, channel) in channels.into_iter().enumerate() {
                        let source_name = format!("channel_router.routing.channels[{index}]");
                        parsed_channels.push(parse_channel_routing_rule(
                            channel,
                            source_name.as_str(),
                            &channel_router,
                        )?);
                    }
                    channel_router.channels = parsed_channels;
                }
            }
        }
        if let Some(file_canvas_host) = parsed.canvas_host {
            if let Some(enabled) = file_canvas_host.enabled {
                canvas_host.enabled = enabled;
            }
            if let Some(public_base_url) = file_canvas_host.public_base_url {
                canvas_host.public_base_url = parse_canvas_host_public_base_url(
                    public_base_url.as_str(),
                    "canvas_host.public_base_url",
                )?;
            }
            if let Some(token_ttl_ms) = file_canvas_host.token_ttl_ms {
                canvas_host.token_ttl_ms =
                    parse_positive_u64(token_ttl_ms, "canvas_host.token_ttl_ms")?;
            }
            if let Some(max_state_bytes) = file_canvas_host.max_state_bytes {
                canvas_host.max_state_bytes =
                    parse_positive_u64(max_state_bytes, "canvas_host.max_state_bytes")?;
            }
            if let Some(max_bundle_bytes) = file_canvas_host.max_bundle_bytes {
                canvas_host.max_bundle_bytes =
                    parse_positive_u64(max_bundle_bytes, "canvas_host.max_bundle_bytes")?;
            }
            if let Some(max_assets_per_bundle) = file_canvas_host.max_assets_per_bundle {
                canvas_host.max_assets_per_bundle =
                    parse_positive_u32(max_assets_per_bundle, "canvas_host.max_assets_per_bundle")?;
            }
            if let Some(max_updates_per_minute) = file_canvas_host.max_updates_per_minute {
                canvas_host.max_updates_per_minute = parse_positive_u32(
                    max_updates_per_minute,
                    "canvas_host.max_updates_per_minute",
                )?;
            }
        }
        if let Some(file_admin) = parsed.admin {
            if let Some(require_auth) = file_admin.require_auth {
                admin.require_auth = require_auth;
            }
            if let Some(auth_token) = file_admin.auth_token {
                admin.auth_token =
                    if auth_token.trim().is_empty() { None } else { Some(auth_token) };
            }
            if let Some(bound_principal) = file_admin.bound_principal {
                let trimmed = bound_principal.trim();
                admin.bound_principal =
                    if trimmed.is_empty() { None } else { Some(trimmed.to_owned()) };
            }
        }
        if let Some(file_identity) = parsed.identity {
            if let Some(allow_insecure) = file_identity.allow_insecure_node_rpc_without_mtls {
                identity.allow_insecure_node_rpc_without_mtls = allow_insecure;
            }
        }
        if let Some(file_storage) = parsed.storage {
            if let Some(path) = file_storage.journal_db_path {
                storage.journal_db_path = parse_journal_db_path(&path)?;
            }
            if let Some(hash_chain_enabled) = file_storage.journal_hash_chain_enabled {
                storage.journal_hash_chain_enabled = hash_chain_enabled;
            }
            if let Some(max_journal_payload_bytes) = file_storage.max_journal_payload_bytes {
                storage.max_journal_payload_bytes = parse_positive_usize(
                    max_journal_payload_bytes,
                    "storage.max_journal_payload_bytes",
                )?;
            }
            if let Some(vault_dir) = file_storage.vault_dir {
                storage.vault_dir = parse_vault_dir(&vault_dir)?;
            }
        }
        source = path.to_string_lossy().into_owned();
        if migration.migrated {
            source.push_str(" +migration(v0->v1)");
        }
    }

    if let Ok(bind_addr) = env::var("PALYRA_DAEMON_BIND_ADDR") {
        daemon.bind_addr = bind_addr;
        source.push_str(" +env(PALYRA_DAEMON_BIND_ADDR)");
    }

    if let Ok(port) = env::var("PALYRA_DAEMON_PORT") {
        daemon.port = port.parse::<u16>().context("PALYRA_DAEMON_PORT must be a valid u16")?;
        source.push_str(" +env(PALYRA_DAEMON_PORT)");
    }

    if let Ok(grpc_bind_addr) = env::var("PALYRA_GATEWAY_GRPC_BIND_ADDR") {
        gateway.grpc_bind_addr = grpc_bind_addr;
        source.push_str(" +env(PALYRA_GATEWAY_GRPC_BIND_ADDR)");
    }

    if let Ok(grpc_port) = env::var("PALYRA_GATEWAY_GRPC_PORT") {
        gateway.grpc_port =
            grpc_port.parse::<u16>().context("PALYRA_GATEWAY_GRPC_PORT must be a valid u16")?;
        source.push_str(" +env(PALYRA_GATEWAY_GRPC_PORT)");
    }

    if let Ok(quic_bind_addr) = env::var("PALYRA_GATEWAY_QUIC_BIND_ADDR") {
        gateway.quic_bind_addr = quic_bind_addr;
        source.push_str(" +env(PALYRA_GATEWAY_QUIC_BIND_ADDR)");
    }

    if let Ok(quic_port) = env::var("PALYRA_GATEWAY_QUIC_PORT") {
        gateway.quic_port =
            quic_port.parse::<u16>().context("PALYRA_GATEWAY_QUIC_PORT must be a valid u16")?;
        source.push_str(" +env(PALYRA_GATEWAY_QUIC_PORT)");
    }

    if let Ok(quic_enabled) = env::var("PALYRA_GATEWAY_QUIC_ENABLED") {
        gateway.quic_enabled = quic_enabled
            .parse::<bool>()
            .context("PALYRA_GATEWAY_QUIC_ENABLED must be true or false")?;
        source.push_str(" +env(PALYRA_GATEWAY_QUIC_ENABLED)");
    }

    if let Ok(allow_insecure_remote) = env::var("PALYRA_GATEWAY_ALLOW_INSECURE_REMOTE") {
        gateway.allow_insecure_remote = allow_insecure_remote
            .parse::<bool>()
            .context("PALYRA_GATEWAY_ALLOW_INSECURE_REMOTE must be true or false")?;
        source.push_str(" +env(PALYRA_GATEWAY_ALLOW_INSECURE_REMOTE)");
    }

    if let Ok(identity_store_dir) = env::var("PALYRA_GATEWAY_IDENTITY_STORE_DIR") {
        gateway.identity_store_dir = Some(parse_identity_store_dir(identity_store_dir.as_str())?);
        source.push_str(" +env(PALYRA_GATEWAY_IDENTITY_STORE_DIR)");
    }

    if let Ok(vault_get_approval_required_refs) =
        env::var("PALYRA_VAULT_GET_APPROVAL_REQUIRED_REFS")
    {
        gateway.vault_get_approval_required_refs = parse_vault_ref_allowlist(
            vault_get_approval_required_refs.as_str(),
            "PALYRA_VAULT_GET_APPROVAL_REQUIRED_REFS",
        )?;
        source.push_str(" +env(PALYRA_VAULT_GET_APPROVAL_REQUIRED_REFS)");
    }

    if let Ok(max_tape_entries_per_response) =
        env::var("PALYRA_GATEWAY_MAX_TAPE_ENTRIES_PER_RESPONSE")
    {
        gateway.max_tape_entries_per_response = parse_positive_usize(
            max_tape_entries_per_response
                .parse::<u64>()
                .context("PALYRA_GATEWAY_MAX_TAPE_ENTRIES_PER_RESPONSE must be a valid u64")?,
            "PALYRA_GATEWAY_MAX_TAPE_ENTRIES_PER_RESPONSE",
        )?;
        source.push_str(" +env(PALYRA_GATEWAY_MAX_TAPE_ENTRIES_PER_RESPONSE)");
    }

    if let Ok(max_tape_bytes_per_response) = env::var("PALYRA_GATEWAY_MAX_TAPE_BYTES_PER_RESPONSE")
    {
        gateway.max_tape_bytes_per_response = parse_positive_usize(
            max_tape_bytes_per_response
                .parse::<u64>()
                .context("PALYRA_GATEWAY_MAX_TAPE_BYTES_PER_RESPONSE must be a valid u64")?,
            "PALYRA_GATEWAY_MAX_TAPE_BYTES_PER_RESPONSE",
        )?;
        source.push_str(" +env(PALYRA_GATEWAY_MAX_TAPE_BYTES_PER_RESPONSE)");
    }

    if let Ok(tls_enabled) = env::var("PALYRA_GATEWAY_TLS_ENABLED") {
        gateway.tls.enabled = tls_enabled
            .parse::<bool>()
            .context("PALYRA_GATEWAY_TLS_ENABLED must be true or false")?;
        source.push_str(" +env(PALYRA_GATEWAY_TLS_ENABLED)");
    }

    if let Ok(tls_cert_path) = env::var("PALYRA_GATEWAY_TLS_CERT_PATH") {
        gateway.tls.cert_path = Some(parse_gateway_tls_path(tls_cert_path.as_str())?);
        source.push_str(" +env(PALYRA_GATEWAY_TLS_CERT_PATH)");
    }

    if let Ok(tls_key_path) = env::var("PALYRA_GATEWAY_TLS_KEY_PATH") {
        gateway.tls.key_path = Some(parse_gateway_tls_path(tls_key_path.as_str())?);
        source.push_str(" +env(PALYRA_GATEWAY_TLS_KEY_PATH)");
    }

    if let Ok(tls_client_ca_path) = env::var("PALYRA_GATEWAY_TLS_CLIENT_CA_PATH") {
        gateway.tls.client_ca_path = Some(parse_gateway_tls_path(tls_client_ca_path.as_str())?);
        source.push_str(" +env(PALYRA_GATEWAY_TLS_CLIENT_CA_PATH)");
    }

    if let Ok(cron_timezone) = env::var("PALYRA_CRON_TIMEZONE") {
        cron.timezone = parse_cron_timezone_mode(cron_timezone.as_str(), "PALYRA_CRON_TIMEZONE")?;
        source.push_str(" +env(PALYRA_CRON_TIMEZONE)");
    }

    if let Ok(runloop_v1_enabled) = env::var("PALYRA_ORCHESTRATOR_RUNLOOP_V1_ENABLED") {
        orchestrator.runloop_v1_enabled = runloop_v1_enabled
            .parse::<bool>()
            .context("PALYRA_ORCHESTRATOR_RUNLOOP_V1_ENABLED must be true or false")?;
        source.push_str(" +env(PALYRA_ORCHESTRATOR_RUNLOOP_V1_ENABLED)");
    }

    if let Ok(max_item_bytes) = env::var("PALYRA_MEMORY_MAX_ITEM_BYTES") {
        memory.max_item_bytes = parse_positive_usize(
            max_item_bytes
                .parse::<u64>()
                .context("PALYRA_MEMORY_MAX_ITEM_BYTES must be a valid u64")?,
            "PALYRA_MEMORY_MAX_ITEM_BYTES",
        )?;
        source.push_str(" +env(PALYRA_MEMORY_MAX_ITEM_BYTES)");
    }

    if let Ok(max_item_tokens) = env::var("PALYRA_MEMORY_MAX_ITEM_TOKENS") {
        memory.max_item_tokens = parse_positive_usize(
            max_item_tokens
                .parse::<u64>()
                .context("PALYRA_MEMORY_MAX_ITEM_TOKENS must be a valid u64")?,
            "PALYRA_MEMORY_MAX_ITEM_TOKENS",
        )?;
        source.push_str(" +env(PALYRA_MEMORY_MAX_ITEM_TOKENS)");
    }

    if let Ok(default_ttl_ms) = env::var("PALYRA_MEMORY_DEFAULT_TTL_MS") {
        memory.default_ttl_ms = parse_default_memory_ttl_ms(
            default_ttl_ms
                .parse::<i64>()
                .context("PALYRA_MEMORY_DEFAULT_TTL_MS must be a valid i64")?,
            "PALYRA_MEMORY_DEFAULT_TTL_MS",
        )?;
        source.push_str(" +env(PALYRA_MEMORY_DEFAULT_TTL_MS)");
    }

    if let Ok(auto_inject_enabled) = env::var("PALYRA_MEMORY_AUTO_INJECT_ENABLED") {
        memory.auto_inject.enabled = auto_inject_enabled
            .parse::<bool>()
            .context("PALYRA_MEMORY_AUTO_INJECT_ENABLED must be true or false")?;
        source.push_str(" +env(PALYRA_MEMORY_AUTO_INJECT_ENABLED)");
    }

    if let Ok(auto_inject_max_items) = env::var("PALYRA_MEMORY_AUTO_INJECT_MAX_ITEMS") {
        memory.auto_inject.max_items = parse_positive_usize(
            auto_inject_max_items
                .parse::<u64>()
                .context("PALYRA_MEMORY_AUTO_INJECT_MAX_ITEMS must be a valid u64")?,
            "PALYRA_MEMORY_AUTO_INJECT_MAX_ITEMS",
        )?;
        source.push_str(" +env(PALYRA_MEMORY_AUTO_INJECT_MAX_ITEMS)");
    }

    if let Ok(kind) = env::var("PALYRA_MODEL_PROVIDER_KIND") {
        model_provider.kind = ModelProviderKind::parse(kind.as_str())
            .context("PALYRA_MODEL_PROVIDER_KIND must be deterministic or openai_compatible")?;
        source.push_str(" +env(PALYRA_MODEL_PROVIDER_KIND)");
    }

    if let Ok(openai_base_url) = env::var("PALYRA_MODEL_PROVIDER_OPENAI_BASE_URL") {
        model_provider.openai_base_url = parse_openai_base_url(openai_base_url.as_str())?;
        source.push_str(" +env(PALYRA_MODEL_PROVIDER_OPENAI_BASE_URL)");
    }

    if let Ok(allow_private_base_url) = env::var("PALYRA_MODEL_PROVIDER_ALLOW_PRIVATE_BASE_URL") {
        model_provider.allow_private_base_url = allow_private_base_url
            .parse::<bool>()
            .context("PALYRA_MODEL_PROVIDER_ALLOW_PRIVATE_BASE_URL must be true or false")?;
        source.push_str(" +env(PALYRA_MODEL_PROVIDER_ALLOW_PRIVATE_BASE_URL)");
    }

    if let Ok(openai_model) = env::var("PALYRA_MODEL_PROVIDER_OPENAI_MODEL") {
        model_provider.openai_model = parse_openai_model(openai_model.as_str())?;
        source.push_str(" +env(PALYRA_MODEL_PROVIDER_OPENAI_MODEL)");
    }
    if let Ok(openai_embeddings_model) = env::var("PALYRA_MODEL_PROVIDER_OPENAI_EMBEDDINGS_MODEL") {
        model_provider.openai_embeddings_model =
            parse_optional_openai_embeddings_model(openai_embeddings_model.as_str())?;
        source.push_str(" +env(PALYRA_MODEL_PROVIDER_OPENAI_EMBEDDINGS_MODEL)");
    }
    if let Ok(openai_embeddings_dims) = env::var("PALYRA_MODEL_PROVIDER_OPENAI_EMBEDDINGS_DIMS") {
        model_provider.openai_embeddings_dims = Some(parse_openai_embeddings_dims(
            openai_embeddings_dims
                .parse::<u32>()
                .context("PALYRA_MODEL_PROVIDER_OPENAI_EMBEDDINGS_DIMS must be a valid u32")?,
            "PALYRA_MODEL_PROVIDER_OPENAI_EMBEDDINGS_DIMS",
        )?);
        source.push_str(" +env(PALYRA_MODEL_PROVIDER_OPENAI_EMBEDDINGS_DIMS)");
    }

    if let Ok(openai_api_key) = env::var("PALYRA_MODEL_PROVIDER_OPENAI_API_KEY") {
        model_provider.openai_api_key =
            if openai_api_key.trim().is_empty() { None } else { Some(openai_api_key) };
        source.push_str(" +env(PALYRA_MODEL_PROVIDER_OPENAI_API_KEY)");
    }

    if let Ok(openai_api_key_vault_ref) = env::var("PALYRA_MODEL_PROVIDER_OPENAI_API_KEY_VAULT_REF")
    {
        model_provider.openai_api_key_vault_ref = if openai_api_key_vault_ref.trim().is_empty() {
            None
        } else {
            Some(openai_api_key_vault_ref)
        };
        source.push_str(" +env(PALYRA_MODEL_PROVIDER_OPENAI_API_KEY_VAULT_REF)");
    }
    if let Ok(auth_profile_ref) = env::var("PALYRA_MODEL_PROVIDER_AUTH_PROFILE_REF") {
        model_provider.auth_profile_id = parse_optional_auth_profile_id(
            auth_profile_ref.as_str(),
            "PALYRA_MODEL_PROVIDER_AUTH_PROFILE_REF",
        )?;
        source.push_str(" +env(PALYRA_MODEL_PROVIDER_AUTH_PROFILE_REF)");
    }
    if let Ok(auth_profile_id) = env::var("PALYRA_MODEL_PROVIDER_AUTH_PROFILE_ID") {
        model_provider.auth_profile_id = parse_optional_auth_profile_id(
            auth_profile_id.as_str(),
            "PALYRA_MODEL_PROVIDER_AUTH_PROFILE_ID",
        )?;
        source.push_str(" +env(PALYRA_MODEL_PROVIDER_AUTH_PROFILE_ID)");
    }
    if let Ok(auth_provider_kind) = env::var("PALYRA_MODEL_PROVIDER_AUTH_PROVIDER_KIND") {
        model_provider.auth_profile_provider_kind = Some(parse_model_provider_auth_provider_kind(
            auth_provider_kind.as_str(),
            "PALYRA_MODEL_PROVIDER_AUTH_PROVIDER_KIND",
        )?);
        source.push_str(" +env(PALYRA_MODEL_PROVIDER_AUTH_PROVIDER_KIND)");
    }

    if let Ok(request_timeout_ms) = env::var("PALYRA_MODEL_PROVIDER_REQUEST_TIMEOUT_MS") {
        model_provider.request_timeout_ms = parse_positive_u64(
            request_timeout_ms
                .parse::<u64>()
                .context("PALYRA_MODEL_PROVIDER_REQUEST_TIMEOUT_MS must be a valid u64")?,
            "PALYRA_MODEL_PROVIDER_REQUEST_TIMEOUT_MS",
        )?;
        source.push_str(" +env(PALYRA_MODEL_PROVIDER_REQUEST_TIMEOUT_MS)");
    }

    if let Ok(max_retries) = env::var("PALYRA_MODEL_PROVIDER_MAX_RETRIES") {
        model_provider.max_retries = parse_retries(
            max_retries
                .parse::<u32>()
                .context("PALYRA_MODEL_PROVIDER_MAX_RETRIES must be a valid u32")?,
            "PALYRA_MODEL_PROVIDER_MAX_RETRIES",
        )?;
        source.push_str(" +env(PALYRA_MODEL_PROVIDER_MAX_RETRIES)");
    }

    if let Ok(retry_backoff_ms) = env::var("PALYRA_MODEL_PROVIDER_RETRY_BACKOFF_MS") {
        model_provider.retry_backoff_ms = parse_positive_u64(
            retry_backoff_ms
                .parse::<u64>()
                .context("PALYRA_MODEL_PROVIDER_RETRY_BACKOFF_MS must be a valid u64")?,
            "PALYRA_MODEL_PROVIDER_RETRY_BACKOFF_MS",
        )?;
        source.push_str(" +env(PALYRA_MODEL_PROVIDER_RETRY_BACKOFF_MS)");
    }

    if let Ok(failure_threshold) =
        env::var("PALYRA_MODEL_PROVIDER_CIRCUIT_BREAKER_FAILURE_THRESHOLD")
    {
        model_provider.circuit_breaker_failure_threshold = parse_positive_u32(
            failure_threshold.parse::<u32>().context(
                "PALYRA_MODEL_PROVIDER_CIRCUIT_BREAKER_FAILURE_THRESHOLD must be a valid u32",
            )?,
            "PALYRA_MODEL_PROVIDER_CIRCUIT_BREAKER_FAILURE_THRESHOLD",
        )?;
        source.push_str(" +env(PALYRA_MODEL_PROVIDER_CIRCUIT_BREAKER_FAILURE_THRESHOLD)");
    }

    if let Ok(cooldown_ms) = env::var("PALYRA_MODEL_PROVIDER_CIRCUIT_BREAKER_COOLDOWN_MS") {
        model_provider.circuit_breaker_cooldown_ms = parse_positive_u64(
            cooldown_ms
                .parse::<u64>()
                .context("PALYRA_MODEL_PROVIDER_CIRCUIT_BREAKER_COOLDOWN_MS must be a valid u64")?,
            "PALYRA_MODEL_PROVIDER_CIRCUIT_BREAKER_COOLDOWN_MS",
        )?;
        source.push_str(" +env(PALYRA_MODEL_PROVIDER_CIRCUIT_BREAKER_COOLDOWN_MS)");
    }

    if let Ok(allowed_tools) = env::var("PALYRA_TOOL_CALL_ALLOWED_TOOLS") {
        tool_call.allowed_tools =
            parse_tool_allowlist(allowed_tools.as_str(), "PALYRA_TOOL_CALL_ALLOWED_TOOLS")?;
        source.push_str(" +env(PALYRA_TOOL_CALL_ALLOWED_TOOLS)");
    }

    if let Ok(max_calls_per_run) = env::var("PALYRA_TOOL_CALL_MAX_CALLS_PER_RUN") {
        tool_call.max_calls_per_run = parse_positive_u32(
            max_calls_per_run
                .parse::<u32>()
                .context("PALYRA_TOOL_CALL_MAX_CALLS_PER_RUN must be a valid u32")?,
            "PALYRA_TOOL_CALL_MAX_CALLS_PER_RUN",
        )?;
        source.push_str(" +env(PALYRA_TOOL_CALL_MAX_CALLS_PER_RUN)");
    }

    if let Ok(execution_timeout_ms) = env::var("PALYRA_TOOL_CALL_TIMEOUT_MS") {
        tool_call.execution_timeout_ms = parse_positive_u64(
            execution_timeout_ms
                .parse::<u64>()
                .context("PALYRA_TOOL_CALL_TIMEOUT_MS must be a valid u64")?,
            "PALYRA_TOOL_CALL_TIMEOUT_MS",
        )?;
        source.push_str(" +env(PALYRA_TOOL_CALL_TIMEOUT_MS)");
    }
    if let Ok(allow_private_targets) = env::var("PALYRA_HTTP_FETCH_ALLOW_PRIVATE_TARGETS") {
        tool_call.http_fetch.allow_private_targets = allow_private_targets
            .parse::<bool>()
            .context("PALYRA_HTTP_FETCH_ALLOW_PRIVATE_TARGETS must be true or false")?;
        source.push_str(" +env(PALYRA_HTTP_FETCH_ALLOW_PRIVATE_TARGETS)");
    }
    if let Ok(connect_timeout_ms) = env::var("PALYRA_HTTP_FETCH_CONNECT_TIMEOUT_MS") {
        tool_call.http_fetch.connect_timeout_ms = parse_positive_u64(
            connect_timeout_ms
                .parse::<u64>()
                .context("PALYRA_HTTP_FETCH_CONNECT_TIMEOUT_MS must be a valid u64")?,
            "PALYRA_HTTP_FETCH_CONNECT_TIMEOUT_MS",
        )?;
        source.push_str(" +env(PALYRA_HTTP_FETCH_CONNECT_TIMEOUT_MS)");
    }
    if let Ok(request_timeout_ms) = env::var("PALYRA_HTTP_FETCH_REQUEST_TIMEOUT_MS") {
        tool_call.http_fetch.request_timeout_ms = parse_positive_u64(
            request_timeout_ms
                .parse::<u64>()
                .context("PALYRA_HTTP_FETCH_REQUEST_TIMEOUT_MS must be a valid u64")?,
            "PALYRA_HTTP_FETCH_REQUEST_TIMEOUT_MS",
        )?;
        source.push_str(" +env(PALYRA_HTTP_FETCH_REQUEST_TIMEOUT_MS)");
    }
    if let Ok(max_response_bytes) = env::var("PALYRA_HTTP_FETCH_MAX_RESPONSE_BYTES") {
        tool_call.http_fetch.max_response_bytes = parse_positive_u64(
            max_response_bytes
                .parse::<u64>()
                .context("PALYRA_HTTP_FETCH_MAX_RESPONSE_BYTES must be a valid u64")?,
            "PALYRA_HTTP_FETCH_MAX_RESPONSE_BYTES",
        )?;
        source.push_str(" +env(PALYRA_HTTP_FETCH_MAX_RESPONSE_BYTES)");
    }
    if let Ok(allow_redirects) = env::var("PALYRA_HTTP_FETCH_ALLOW_REDIRECTS") {
        tool_call.http_fetch.allow_redirects = allow_redirects
            .parse::<bool>()
            .context("PALYRA_HTTP_FETCH_ALLOW_REDIRECTS must be true or false")?;
        source.push_str(" +env(PALYRA_HTTP_FETCH_ALLOW_REDIRECTS)");
    }
    if let Ok(max_redirects) = env::var("PALYRA_HTTP_FETCH_MAX_REDIRECTS") {
        tool_call.http_fetch.max_redirects = parse_positive_u32(
            max_redirects
                .parse::<u32>()
                .context("PALYRA_HTTP_FETCH_MAX_REDIRECTS must be a valid u32")?,
            "PALYRA_HTTP_FETCH_MAX_REDIRECTS",
        )?;
        source.push_str(" +env(PALYRA_HTTP_FETCH_MAX_REDIRECTS)");
    }
    if let Ok(allowed_content_types) = env::var("PALYRA_HTTP_FETCH_ALLOWED_CONTENT_TYPES") {
        tool_call.http_fetch.allowed_content_types = parse_content_type_allowlist(
            allowed_content_types.as_str(),
            "PALYRA_HTTP_FETCH_ALLOWED_CONTENT_TYPES",
        )?;
        source.push_str(" +env(PALYRA_HTTP_FETCH_ALLOWED_CONTENT_TYPES)");
    }
    if let Ok(allowed_headers) = env::var("PALYRA_HTTP_FETCH_ALLOWED_HEADERS") {
        tool_call.http_fetch.allowed_request_headers = parse_http_header_allowlist(
            allowed_headers.as_str(),
            "PALYRA_HTTP_FETCH_ALLOWED_HEADERS",
        )?;
        source.push_str(" +env(PALYRA_HTTP_FETCH_ALLOWED_HEADERS)");
    }
    if let Ok(cache_enabled) = env::var("PALYRA_HTTP_FETCH_CACHE_ENABLED") {
        tool_call.http_fetch.cache_enabled = cache_enabled
            .parse::<bool>()
            .context("PALYRA_HTTP_FETCH_CACHE_ENABLED must be true or false")?;
        source.push_str(" +env(PALYRA_HTTP_FETCH_CACHE_ENABLED)");
    }
    if let Ok(cache_ttl_ms) = env::var("PALYRA_HTTP_FETCH_CACHE_TTL_MS") {
        tool_call.http_fetch.cache_ttl_ms = parse_positive_u64(
            cache_ttl_ms
                .parse::<u64>()
                .context("PALYRA_HTTP_FETCH_CACHE_TTL_MS must be a valid u64")?,
            "PALYRA_HTTP_FETCH_CACHE_TTL_MS",
        )?;
        source.push_str(" +env(PALYRA_HTTP_FETCH_CACHE_TTL_MS)");
    }
    if let Ok(max_cache_entries) = env::var("PALYRA_HTTP_FETCH_MAX_CACHE_ENTRIES") {
        tool_call.http_fetch.max_cache_entries = parse_positive_u64(
            max_cache_entries
                .parse::<u64>()
                .context("PALYRA_HTTP_FETCH_MAX_CACHE_ENTRIES must be a valid u64")?,
            "PALYRA_HTTP_FETCH_MAX_CACHE_ENTRIES",
        )?;
        source.push_str(" +env(PALYRA_HTTP_FETCH_MAX_CACHE_ENTRIES)");
    }
    if let Ok(browser_service_enabled) = env::var("PALYRA_BROWSER_SERVICE_ENABLED") {
        tool_call.browser_service.enabled = browser_service_enabled
            .parse::<bool>()
            .context("PALYRA_BROWSER_SERVICE_ENABLED must be true or false")?;
        source.push_str(" +env(PALYRA_BROWSER_SERVICE_ENABLED)");
    }
    if let Ok(browser_service_endpoint) = env::var("PALYRA_BROWSER_SERVICE_ENDPOINT") {
        tool_call.browser_service.endpoint = parse_browser_service_endpoint(
            browser_service_endpoint.as_str(),
            "PALYRA_BROWSER_SERVICE_ENDPOINT",
        )?;
        source.push_str(" +env(PALYRA_BROWSER_SERVICE_ENDPOINT)");
    }
    if let Ok(browser_service_auth_token) = env::var("PALYRA_BROWSER_SERVICE_AUTH_TOKEN") {
        let trimmed = browser_service_auth_token.trim();
        tool_call.browser_service.auth_token =
            if trimmed.is_empty() { None } else { Some(trimmed.to_owned()) };
        source.push_str(" +env(PALYRA_BROWSER_SERVICE_AUTH_TOKEN)");
    }
    if let Ok(browserd_state_dir) = env::var("PALYRA_BROWSERD_STATE_DIR") {
        tool_call.browser_service.state_dir = parse_optional_browser_state_dir(
            browserd_state_dir.as_str(),
            "PALYRA_BROWSERD_STATE_DIR",
        )?;
        source.push_str(" +env(PALYRA_BROWSERD_STATE_DIR)");
    }
    if let Ok(browserd_state_key_vault_ref) =
        env::var("PALYRA_BROWSERD_STATE_ENCRYPTION_KEY_VAULT_REF")
    {
        tool_call.browser_service.state_key_vault_ref = parse_optional_vault_ref_field(
            browserd_state_key_vault_ref.as_str(),
            "PALYRA_BROWSERD_STATE_ENCRYPTION_KEY_VAULT_REF",
        )?;
        source.push_str(" +env(PALYRA_BROWSERD_STATE_ENCRYPTION_KEY_VAULT_REF)");
    }
    if let Ok(connect_timeout_ms) = env::var("PALYRA_BROWSER_SERVICE_CONNECT_TIMEOUT_MS") {
        tool_call.browser_service.connect_timeout_ms = parse_positive_u64(
            connect_timeout_ms
                .parse::<u64>()
                .context("PALYRA_BROWSER_SERVICE_CONNECT_TIMEOUT_MS must be a valid u64")?,
            "PALYRA_BROWSER_SERVICE_CONNECT_TIMEOUT_MS",
        )?;
        source.push_str(" +env(PALYRA_BROWSER_SERVICE_CONNECT_TIMEOUT_MS)");
    }
    if let Ok(request_timeout_ms) = env::var("PALYRA_BROWSER_SERVICE_REQUEST_TIMEOUT_MS") {
        tool_call.browser_service.request_timeout_ms = parse_positive_u64(
            request_timeout_ms
                .parse::<u64>()
                .context("PALYRA_BROWSER_SERVICE_REQUEST_TIMEOUT_MS must be a valid u64")?,
            "PALYRA_BROWSER_SERVICE_REQUEST_TIMEOUT_MS",
        )?;
        source.push_str(" +env(PALYRA_BROWSER_SERVICE_REQUEST_TIMEOUT_MS)");
    }
    if let Ok(max_screenshot_bytes) = env::var("PALYRA_BROWSER_SERVICE_MAX_SCREENSHOT_BYTES") {
        tool_call.browser_service.max_screenshot_bytes = parse_positive_u64(
            max_screenshot_bytes
                .parse::<u64>()
                .context("PALYRA_BROWSER_SERVICE_MAX_SCREENSHOT_BYTES must be a valid u64")?,
            "PALYRA_BROWSER_SERVICE_MAX_SCREENSHOT_BYTES",
        )?;
        source.push_str(" +env(PALYRA_BROWSER_SERVICE_MAX_SCREENSHOT_BYTES)");
    }
    if let Ok(max_title_bytes) = env::var("PALYRA_BROWSER_SERVICE_MAX_TITLE_BYTES") {
        tool_call.browser_service.max_title_bytes = parse_positive_u64(
            max_title_bytes
                .parse::<u64>()
                .context("PALYRA_BROWSER_SERVICE_MAX_TITLE_BYTES must be a valid u64")?,
            "PALYRA_BROWSER_SERVICE_MAX_TITLE_BYTES",
        )?;
        source.push_str(" +env(PALYRA_BROWSER_SERVICE_MAX_TITLE_BYTES)");
    }

    if let Ok(channel_router_enabled) = env::var("PALYRA_CHANNEL_ROUTER_ENABLED") {
        channel_router.enabled = channel_router_enabled
            .parse::<bool>()
            .context("PALYRA_CHANNEL_ROUTER_ENABLED must be true or false")?;
        source.push_str(" +env(PALYRA_CHANNEL_ROUTER_ENABLED)");
    }

    if let Ok(max_message_bytes) = env::var("PALYRA_CHANNEL_ROUTER_MAX_MESSAGE_BYTES") {
        channel_router.max_message_bytes = parse_positive_usize(
            max_message_bytes
                .parse::<u64>()
                .context("PALYRA_CHANNEL_ROUTER_MAX_MESSAGE_BYTES must be a valid u64")?,
            "PALYRA_CHANNEL_ROUTER_MAX_MESSAGE_BYTES",
        )?;
        source.push_str(" +env(PALYRA_CHANNEL_ROUTER_MAX_MESSAGE_BYTES)");
    }

    if let Ok(max_retry_queue_depth_per_channel) =
        env::var("PALYRA_CHANNEL_ROUTER_MAX_RETRY_QUEUE_DEPTH_PER_CHANNEL")
    {
        channel_router.max_retry_queue_depth_per_channel = parse_positive_usize(
            max_retry_queue_depth_per_channel.parse::<u64>().context(
                "PALYRA_CHANNEL_ROUTER_MAX_RETRY_QUEUE_DEPTH_PER_CHANNEL must be a valid u64",
            )?,
            "PALYRA_CHANNEL_ROUTER_MAX_RETRY_QUEUE_DEPTH_PER_CHANNEL",
        )?;
        source.push_str(" +env(PALYRA_CHANNEL_ROUTER_MAX_RETRY_QUEUE_DEPTH_PER_CHANNEL)");
    }

    if let Ok(max_retry_attempts) = env::var("PALYRA_CHANNEL_ROUTER_MAX_RETRY_ATTEMPTS") {
        channel_router.max_retry_attempts = parse_positive_u32(
            max_retry_attempts
                .parse::<u32>()
                .context("PALYRA_CHANNEL_ROUTER_MAX_RETRY_ATTEMPTS must be a valid u32")?,
            "PALYRA_CHANNEL_ROUTER_MAX_RETRY_ATTEMPTS",
        )?;
        source.push_str(" +env(PALYRA_CHANNEL_ROUTER_MAX_RETRY_ATTEMPTS)");
    }

    if let Ok(retry_backoff_ms) = env::var("PALYRA_CHANNEL_ROUTER_RETRY_BACKOFF_MS") {
        channel_router.retry_backoff_ms = parse_positive_u64(
            retry_backoff_ms
                .parse::<u64>()
                .context("PALYRA_CHANNEL_ROUTER_RETRY_BACKOFF_MS must be a valid u64")?,
            "PALYRA_CHANNEL_ROUTER_RETRY_BACKOFF_MS",
        )?;
        source.push_str(" +env(PALYRA_CHANNEL_ROUTER_RETRY_BACKOFF_MS)");
    }

    if let Ok(canvas_host_enabled) = env::var("PALYRA_CANVAS_HOST_ENABLED") {
        canvas_host.enabled = canvas_host_enabled
            .parse::<bool>()
            .context("PALYRA_CANVAS_HOST_ENABLED must be true or false")?;
        source.push_str(" +env(PALYRA_CANVAS_HOST_ENABLED)");
    }

    if let Ok(public_base_url) = env::var("PALYRA_CANVAS_HOST_PUBLIC_BASE_URL") {
        canvas_host.public_base_url = parse_canvas_host_public_base_url(
            public_base_url.as_str(),
            "PALYRA_CANVAS_HOST_PUBLIC_BASE_URL",
        )?;
        source.push_str(" +env(PALYRA_CANVAS_HOST_PUBLIC_BASE_URL)");
    }

    if let Ok(token_ttl_ms) = env::var("PALYRA_CANVAS_HOST_TOKEN_TTL_MS") {
        canvas_host.token_ttl_ms = parse_positive_u64(
            token_ttl_ms
                .parse::<u64>()
                .context("PALYRA_CANVAS_HOST_TOKEN_TTL_MS must be a valid u64")?,
            "PALYRA_CANVAS_HOST_TOKEN_TTL_MS",
        )?;
        source.push_str(" +env(PALYRA_CANVAS_HOST_TOKEN_TTL_MS)");
    }

    if let Ok(max_state_bytes) = env::var("PALYRA_CANVAS_HOST_MAX_STATE_BYTES") {
        canvas_host.max_state_bytes = parse_positive_u64(
            max_state_bytes
                .parse::<u64>()
                .context("PALYRA_CANVAS_HOST_MAX_STATE_BYTES must be a valid u64")?,
            "PALYRA_CANVAS_HOST_MAX_STATE_BYTES",
        )?;
        source.push_str(" +env(PALYRA_CANVAS_HOST_MAX_STATE_BYTES)");
    }

    if let Ok(max_bundle_bytes) = env::var("PALYRA_CANVAS_HOST_MAX_BUNDLE_BYTES") {
        canvas_host.max_bundle_bytes = parse_positive_u64(
            max_bundle_bytes
                .parse::<u64>()
                .context("PALYRA_CANVAS_HOST_MAX_BUNDLE_BYTES must be a valid u64")?,
            "PALYRA_CANVAS_HOST_MAX_BUNDLE_BYTES",
        )?;
        source.push_str(" +env(PALYRA_CANVAS_HOST_MAX_BUNDLE_BYTES)");
    }

    if let Ok(max_assets_per_bundle) = env::var("PALYRA_CANVAS_HOST_MAX_ASSETS_PER_BUNDLE") {
        canvas_host.max_assets_per_bundle = parse_positive_u32(
            max_assets_per_bundle
                .parse::<u32>()
                .context("PALYRA_CANVAS_HOST_MAX_ASSETS_PER_BUNDLE must be a valid u32")?,
            "PALYRA_CANVAS_HOST_MAX_ASSETS_PER_BUNDLE",
        )?;
        source.push_str(" +env(PALYRA_CANVAS_HOST_MAX_ASSETS_PER_BUNDLE)");
    }

    if let Ok(max_updates_per_minute) = env::var("PALYRA_CANVAS_HOST_MAX_UPDATES_PER_MINUTE") {
        canvas_host.max_updates_per_minute = parse_positive_u32(
            max_updates_per_minute
                .parse::<u32>()
                .context("PALYRA_CANVAS_HOST_MAX_UPDATES_PER_MINUTE must be a valid u32")?,
            "PALYRA_CANVAS_HOST_MAX_UPDATES_PER_MINUTE",
        )?;
        source.push_str(" +env(PALYRA_CANVAS_HOST_MAX_UPDATES_PER_MINUTE)");
    }

    if let Ok(require_auth) = env::var("PALYRA_ADMIN_REQUIRE_AUTH") {
        admin.require_auth = require_auth
            .parse::<bool>()
            .context("PALYRA_ADMIN_REQUIRE_AUTH must be true or false")?;
        source.push_str(" +env(PALYRA_ADMIN_REQUIRE_AUTH)");
    }

    if let Ok(admin_token) = env::var("PALYRA_ADMIN_TOKEN") {
        admin.auth_token = if admin_token.trim().is_empty() { None } else { Some(admin_token) };
        source.push_str(" +env(PALYRA_ADMIN_TOKEN)");
    }

    if let Ok(bound_principal) = env::var("PALYRA_ADMIN_BOUND_PRINCIPAL") {
        let trimmed = bound_principal.trim();
        admin.bound_principal = if trimmed.is_empty() { None } else { Some(trimmed.to_owned()) };
        source.push_str(" +env(PALYRA_ADMIN_BOUND_PRINCIPAL)");
    }

    if let Ok(allow_insecure) = env::var("PALYRA_ALLOW_INSECURE_NODE_RPC_WITHOUT_MTLS") {
        identity.allow_insecure_node_rpc_without_mtls = allow_insecure
            .parse::<bool>()
            .context("PALYRA_ALLOW_INSECURE_NODE_RPC_WITHOUT_MTLS must be true or false")?;
        source.push_str(" +env(PALYRA_ALLOW_INSECURE_NODE_RPC_WITHOUT_MTLS)");
    }

    if let Ok(path) = env::var("PALYRA_JOURNAL_DB_PATH") {
        storage.journal_db_path = parse_journal_db_path(&path)?;
        source.push_str(" +env(PALYRA_JOURNAL_DB_PATH)");
    }

    if let Ok(hash_chain_enabled) = env::var("PALYRA_JOURNAL_HASH_CHAIN_ENABLED") {
        storage.journal_hash_chain_enabled = hash_chain_enabled
            .parse::<bool>()
            .context("PALYRA_JOURNAL_HASH_CHAIN_ENABLED must be true or false")?;
        source.push_str(" +env(PALYRA_JOURNAL_HASH_CHAIN_ENABLED)");
    }

    if let Ok(max_journal_payload_bytes) = env::var("PALYRA_JOURNAL_MAX_PAYLOAD_BYTES") {
        storage.max_journal_payload_bytes = parse_positive_usize(
            max_journal_payload_bytes
                .parse::<u64>()
                .context("PALYRA_JOURNAL_MAX_PAYLOAD_BYTES must be a valid u64")?,
            "PALYRA_JOURNAL_MAX_PAYLOAD_BYTES",
        )?;
        source.push_str(" +env(PALYRA_JOURNAL_MAX_PAYLOAD_BYTES)");
    }

    if let Ok(vault_dir) = env::var("PALYRA_VAULT_DIR") {
        storage.vault_dir = parse_vault_dir(&vault_dir)?;
        source.push_str(" +env(PALYRA_VAULT_DIR)");
    }

    if gateway.tls.enabled && (gateway.tls.cert_path.is_none() || gateway.tls.key_path.is_none()) {
        anyhow::bail!(
            "gateway.tls.enabled=true requires both gateway.tls.cert_path and gateway.tls.key_path"
        );
    }
    if model_provider.auth_profile_id.is_some()
        && model_provider.auth_profile_provider_kind.is_none()
    {
        model_provider.auth_profile_provider_kind = Some(ModelProviderAuthProviderKind::Openai);
    }
    if model_provider.kind == ModelProviderKind::OpenAiCompatible {
        validate_openai_base_url_network_policy(
            model_provider.openai_base_url.as_str(),
            model_provider.allow_private_base_url,
        )?;
    }

    Ok(LoadedConfig {
        source,
        config_version,
        migrated_from_version,
        daemon,
        gateway,
        cron,
        orchestrator,
        memory,
        model_provider,
        tool_call,
        channel_router,
        canvas_host,
        admin,
        identity,
        storage,
    })
}

fn parse_root_file_config(content: &str) -> Result<(RootFileConfig, ConfigMigrationInfo)> {
    let (document, migration) =
        parse_document_with_migration(content).context("failed to migrate config document")?;
    let normalized =
        serialize_document_pretty(&document).context("failed to serialize normalized config")?;
    let parsed: RootFileConfig =
        toml::from_str(&normalized).context("invalid daemon config schema")?;
    Ok((parsed, migration))
}

fn find_config_path() -> Result<Option<PathBuf>> {
    if let Ok(path) = env::var("PALYRA_CONFIG") {
        let path =
            parse_config_path(&path).context("PALYRA_CONFIG contains an invalid config path")?;
        if !path.exists() {
            anyhow::bail!("PALYRA_CONFIG points to a missing file: {}", path.to_string_lossy());
        }
        return Ok(Some(path));
    }

    for candidate in default_config_search_paths() {
        if candidate.exists() {
            return Ok(Some(candidate));
        }
    }

    Ok(None)
}

fn parse_journal_db_path(raw: &str) -> Result<PathBuf> {
    if raw.trim().is_empty() {
        anyhow::bail!("journal db path cannot be empty");
    }
    if raw.contains('\0') {
        anyhow::bail!("journal db path cannot contain embedded NUL byte");
    }
    let path = PathBuf::from(raw);
    if path.components().any(|component| matches!(component, Component::ParentDir)) {
        anyhow::bail!("journal db path cannot contain parent traversal ('..')");
    }
    Ok(path)
}

fn parse_vault_dir(raw: &str) -> Result<PathBuf> {
    if raw.trim().is_empty() {
        anyhow::bail!("vault directory cannot be empty");
    }
    if raw.contains('\0') {
        anyhow::bail!("vault directory cannot contain embedded NUL byte");
    }
    Ok(PathBuf::from(raw))
}

fn parse_optional_browser_state_dir(raw: &str, source_name: &str) -> Result<Option<PathBuf>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed.contains('\0') {
        anyhow::bail!("{source_name} cannot contain embedded NUL byte");
    }
    Ok(Some(PathBuf::from(trimmed)))
}

fn parse_optional_vault_ref_field(raw: &str, source_name: &str) -> Result<Option<String>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let refs = parse_vault_ref_allowlist(trimmed, source_name)?;
    if refs.len() != 1 {
        anyhow::bail!("{source_name} must contain exactly one <scope>/<key> entry");
    }
    Ok(refs.into_iter().next())
}

fn parse_optional_auth_profile_id(raw: &str, source_name: &str) -> Result<Option<String>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed.len() > 128 {
        anyhow::bail!("{source_name} exceeds maximum bytes ({} > 128)", trimmed.len());
    }
    let normalized = trimmed.to_ascii_lowercase();
    if !normalized
        .chars()
        .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '.' | '_' | '-'))
    {
        anyhow::bail!("{source_name} contains unsupported characters");
    }
    Ok(Some(normalized))
}

fn parse_model_provider_auth_provider_kind(
    raw: &str,
    source_name: &str,
) -> Result<ModelProviderAuthProviderKind> {
    ModelProviderAuthProviderKind::parse(raw)
        .with_context(|| format!("{source_name} must be one of: openai, openai_compatible"))
}

fn parse_identity_store_dir(raw: &str) -> Result<PathBuf> {
    if raw.trim().is_empty() {
        anyhow::bail!("identity store directory cannot be empty");
    }
    if raw.contains('\0') {
        anyhow::bail!("identity store directory cannot contain embedded NUL byte");
    }
    Ok(PathBuf::from(raw))
}

fn default_vault_dir() -> PathBuf {
    let identity_root =
        default_identity_store_root().unwrap_or_else(|_| PathBuf::from(".palyra/identity"));
    if let Some(parent) = identity_root.parent() {
        parent.join("vault")
    } else {
        identity_root.join("vault")
    }
}

fn parse_gateway_tls_path(raw: &str) -> Result<PathBuf> {
    if raw.trim().is_empty() {
        anyhow::bail!("gateway tls path cannot be empty");
    }
    if raw.contains('\0') {
        anyhow::bail!("gateway tls path cannot contain embedded NUL byte");
    }
    Ok(PathBuf::from(raw))
}

fn parse_openai_base_url(raw: &str) -> Result<String> {
    if raw.trim().is_empty() {
        anyhow::bail!("openai base URL cannot be empty");
    }
    let normalized = raw.trim();
    let parsed =
        reqwest::Url::parse(normalized).context("openai base URL must be a valid absolute URL")?;
    let host =
        parsed.host_str().ok_or_else(|| anyhow::anyhow!("openai base URL must include a host"))?;
    let loopback_http_allowed = host.eq_ignore_ascii_case("localhost")
        || host.parse::<std::net::IpAddr>().is_ok_and(|ip| ip.is_loopback());
    if parsed.scheme() != "https" && !(parsed.scheme() == "http" && loopback_http_allowed) {
        anyhow::bail!("openai base URL must use https (http is only allowed for loopback hosts)");
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        anyhow::bail!("openai base URL must not embed credentials");
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        anyhow::bail!("openai base URL must not include query or fragment");
    }
    Ok(parsed.as_str().trim_end_matches('/').to_owned())
}

fn parse_openai_model(raw: &str) -> Result<String> {
    if raw.trim().is_empty() {
        anyhow::bail!("openai model cannot be empty");
    }
    Ok(raw.trim().to_owned())
}

fn parse_optional_openai_embeddings_model(raw: &str) -> Result<Option<String>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    Ok(Some(trimmed.to_owned()))
}

fn parse_openai_embeddings_dims(raw: u32, source_name: &str) -> Result<u32> {
    parse_positive_u32(raw, source_name)
}

fn parse_vault_ref_allowlist(raw: &str, source_name: &str) -> Result<Vec<String>> {
    let mut refs = Vec::new();
    for candidate in raw.split(',').map(str::trim).filter(|value| !value.is_empty()) {
        let parsed = VaultRef::parse(candidate).map_err(|error| {
            anyhow::anyhow!("{source_name} contains invalid vault ref '{candidate}': {error}")
        })?;
        let normalized = format!("{}/{}", parsed.scope, parsed.key).to_ascii_lowercase();
        if !refs.iter().any(|existing| existing == &normalized) {
            refs.push(normalized);
        }
    }
    if refs.is_empty() {
        anyhow::bail!("{source_name} must include at least one <scope>/<key> entry");
    }
    Ok(refs)
}

fn parse_tool_allowlist(raw: &str, source_name: &str) -> Result<Vec<String>> {
    parse_identifier_allowlist(raw, source_name, "tool name")
}

fn parse_process_executable_allowlist(raw: &str, source_name: &str) -> Result<Vec<String>> {
    parse_identifier_allowlist(raw, source_name, "executable name")
}

fn parse_process_runner_tier(raw: &str, source_name: &str) -> Result<SandboxProcessRunnerTier> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "b" | "tier_b" => Ok(SandboxProcessRunnerTier::B),
        "c" | "tier_c" => Ok(SandboxProcessRunnerTier::C),
        _ => anyhow::bail!("{source_name} must be one of: b, c"),
    }
}

fn parse_process_runner_egress_enforcement_mode(
    raw: &str,
    source_name: &str,
) -> Result<EgressEnforcementMode> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "none" => Ok(EgressEnforcementMode::None),
        "preflight" => Ok(EgressEnforcementMode::Preflight),
        "strict" => Ok(EgressEnforcementMode::Strict),
        _ => anyhow::bail!("{source_name} must be one of: none, preflight, strict"),
    }
}

fn parse_cron_timezone_mode(raw: &str, source_name: &str) -> Result<CronTimezoneMode> {
    CronTimezoneMode::from_str(raw)
        .ok_or_else(|| anyhow::anyhow!("{source_name} must be one of: utc, local"))
}

fn parse_identifier_allowlist(raw: &str, source_name: &str, label: &str) -> Result<Vec<String>> {
    let mut allowlist = Vec::new();
    for candidate in raw.split(',').map(str::trim).filter(|value| !value.is_empty()) {
        if !candidate.chars().all(|ch| {
            ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '.' | '_' | '-')
        }) {
            anyhow::bail!("{source_name} contains invalid {label} '{candidate}'");
        }
        if !allowlist.iter().any(|existing| existing == candidate) {
            allowlist.push(candidate.to_owned());
        }
    }
    Ok(allowlist)
}

fn parse_workspace_root(raw: &str) -> Result<PathBuf> {
    if raw.trim().is_empty() {
        anyhow::bail!("process runner workspace root cannot be empty");
    }
    if raw.contains('\0') {
        anyhow::bail!("process runner workspace root cannot contain embedded NUL byte");
    }
    Ok(PathBuf::from(raw))
}

fn parse_host_allowlist(raw: &str, source_name: &str) -> Result<Vec<String>> {
    let mut allowlist = Vec::new();
    for candidate in raw.split(',').map(str::trim).filter(|value| !value.is_empty()) {
        let normalized = normalize_host_candidate(candidate)
            .with_context(|| format!("{source_name} contains invalid host '{candidate}'"))?;
        if !allowlist.iter().any(|existing| existing == &normalized) {
            allowlist.push(normalized);
        }
    }
    Ok(allowlist)
}

fn parse_dns_suffix_allowlist(raw: &str, source_name: &str) -> Result<Vec<String>> {
    let mut allowlist = Vec::new();
    for candidate in raw.split(',').map(str::trim).filter(|value| !value.is_empty()) {
        let normalized = normalize_dns_suffix_candidate(candidate)
            .with_context(|| format!("{source_name} contains invalid dns suffix '{candidate}'"))?;
        if !allowlist.iter().any(|existing| existing == &normalized) {
            allowlist.push(normalized);
        }
    }
    Ok(allowlist)
}

fn parse_content_type_allowlist(raw: &str, source_name: &str) -> Result<Vec<String>> {
    let mut allowlist = Vec::new();
    for candidate in raw.split(',').map(str::trim).filter(|value| !value.is_empty()) {
        let normalized = candidate.to_ascii_lowercase();
        if normalized.len() > 128 {
            anyhow::bail!("{source_name} contains oversized content type '{candidate}'");
        }
        if !normalized.contains('/') || normalized.starts_with('/') || normalized.ends_with('/') {
            anyhow::bail!("{source_name} contains invalid content type '{candidate}'");
        }
        if normalized.contains(';') || normalized.chars().any(|ch| ch.is_ascii_whitespace()) {
            anyhow::bail!(
                "{source_name} content type entries must not include parameters or whitespace"
            );
        }
        if !normalized.chars().all(|ch| {
            ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '/' | '+' | '.' | '-')
        }) {
            anyhow::bail!("{source_name} contains invalid content type '{candidate}'");
        }
        if !allowlist.iter().any(|existing| existing == &normalized) {
            allowlist.push(normalized);
        }
    }
    if allowlist.is_empty() {
        anyhow::bail!("{source_name} must include at least one content type");
    }
    Ok(allowlist)
}

fn parse_http_header_allowlist(raw: &str, source_name: &str) -> Result<Vec<String>> {
    let mut allowlist = Vec::new();
    for candidate in raw.split(',').map(str::trim).filter(|value| !value.is_empty()) {
        let normalized = candidate.to_ascii_lowercase();
        if normalized.len() > 128 {
            anyhow::bail!("{source_name} contains oversized header name '{candidate}'");
        }
        if normalized.starts_with('-')
            || normalized.ends_with('-')
            || !normalized
                .chars()
                .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-')
        {
            anyhow::bail!("{source_name} contains invalid header name '{candidate}'");
        }
        if !allowlist.iter().any(|existing| existing == &normalized) {
            allowlist.push(normalized);
        }
    }
    if allowlist.is_empty() {
        anyhow::bail!("{source_name} must include at least one header name");
    }
    Ok(allowlist)
}

fn parse_storage_prefix_allowlist(raw: &str, source_name: &str) -> Result<Vec<String>> {
    let mut allowlist = Vec::new();
    for candidate in raw.split(',').map(str::trim).filter(|value| !value.is_empty()) {
        if candidate.contains('\0')
            || candidate.contains("..")
            || candidate.starts_with('/')
            || candidate.starts_with('\\')
            || !candidate.chars().all(|ch| {
                ch.is_ascii_lowercase()
                    || ch.is_ascii_digit()
                    || matches!(ch, '/' | '.' | '_' | '-')
            })
        {
            anyhow::bail!("{source_name} contains invalid storage prefix '{candidate}'");
        }
        if !allowlist.iter().any(|existing| existing == candidate) {
            allowlist.push(candidate.to_owned());
        }
    }
    Ok(allowlist)
}

fn parse_broadcast_strategy(raw: &str, source_name: &str) -> Result<BroadcastStrategy> {
    BroadcastStrategy::parse(raw)
        .ok_or_else(|| anyhow::anyhow!("{source_name} must be one of: deny, mention_only, allow"))
}

fn parse_optional_text_field(
    raw: String,
    source_name: &str,
    max_bytes: usize,
) -> Result<Option<String>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed.len() > max_bytes {
        anyhow::bail!("{source_name} exceeds maximum bytes ({} > {max_bytes})", trimmed.len());
    }
    Ok(Some(trimmed.to_owned()))
}

fn parse_browser_service_endpoint(raw: &str, source_name: &str) -> Result<String> {
    if raw.trim().is_empty() {
        anyhow::bail!("{source_name} cannot be empty");
    }
    let parsed = reqwest::Url::parse(raw.trim())
        .with_context(|| format!("{source_name} must be a valid absolute URL"))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        anyhow::bail!("{source_name} must use http or https scheme");
    }
    if parsed.host_str().is_none() {
        anyhow::bail!("{source_name} must include a host");
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        anyhow::bail!("{source_name} must not embed credentials");
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        anyhow::bail!("{source_name} must not include query or fragment");
    }
    if parsed.path() != "/" && !parsed.path().is_empty() {
        anyhow::bail!("{source_name} must not include a path");
    }
    Ok(parsed.as_str().trim_end_matches('/').to_owned())
}

fn parse_canvas_host_public_base_url(raw: &str, source_name: &str) -> Result<String> {
    if raw.trim().is_empty() {
        anyhow::bail!("{source_name} cannot be empty");
    }
    let parsed = reqwest::Url::parse(raw.trim())
        .with_context(|| format!("{source_name} must be a valid absolute URL"))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        anyhow::bail!("{source_name} must use http or https scheme");
    }
    if parsed.host_str().is_none() {
        anyhow::bail!("{source_name} must include a host");
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        anyhow::bail!("{source_name} must not embed credentials");
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        anyhow::bail!("{source_name} must not include query or fragment");
    }
    Ok(parsed.as_str().trim_end_matches('/').to_owned())
}

fn parse_channel_identifier(raw: &str, source_name: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        anyhow::bail!("{source_name} cannot be empty");
    }
    if !trimmed.chars().all(|ch| {
        ch.is_ascii_lowercase()
            || ch.is_ascii_uppercase()
            || ch.is_ascii_digit()
            || matches!(ch, '.' | '_' | '-' | ':')
    }) {
        anyhow::bail!("{source_name} contains invalid channel identifier '{trimmed}'");
    }
    Ok(trimmed.to_ascii_lowercase())
}

fn parse_sender_identifier_list(raw: &[String], source_name: &str) -> Result<Vec<String>> {
    let mut values = Vec::new();
    for candidate in raw.iter().map(String::as_str).map(str::trim).filter(|value| !value.is_empty())
    {
        if !candidate.chars().all(|ch| {
            ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-' | '@' | ':' | '/' | '#')
        }) {
            anyhow::bail!("{source_name} contains invalid sender identifier '{candidate}'");
        }
        let normalized = candidate.to_ascii_lowercase();
        if !values.iter().any(|existing| existing == &normalized) {
            values.push(normalized);
        }
    }
    Ok(values)
}

fn parse_mention_patterns(raw: &[String], source_name: &str) -> Result<Vec<String>> {
    if raw.len() > 64 {
        anyhow::bail!("{source_name} exceeds maximum entries ({} > 64)", raw.len());
    }
    let mut patterns = Vec::new();
    for candidate in raw.iter().map(String::as_str) {
        let trimmed = candidate.trim();
        if trimmed.is_empty() {
            anyhow::bail!("{source_name} cannot contain empty mention patterns");
        }
        if trimmed.len() > 128 {
            anyhow::bail!(
                "{source_name} contains oversized mention pattern ({} > 128)",
                trimmed.len()
            );
        }
        let normalized = trimmed.to_ascii_lowercase();
        if !patterns.iter().any(|existing| existing == &normalized) {
            patterns.push(normalized);
        }
    }
    Ok(patterns)
}

fn parse_channel_routing_rule(
    raw: palyra_common::daemon_config_schema::FileChannelRoutingRule,
    source_name: &str,
    defaults: &ChannelRouterConfig,
) -> Result<ChannelRoutingRule> {
    let channel = parse_channel_identifier(
        raw.channel.unwrap_or_default().as_str(),
        format!("{source_name}.channel").as_str(),
    )?;
    let mention_patterns = parse_mention_patterns(
        raw.mention_patterns.unwrap_or_default().as_slice(),
        format!("{source_name}.mention_patterns").as_str(),
    )?;
    let allow_from = parse_sender_identifier_list(
        raw.allow_from.unwrap_or_default().as_slice(),
        format!("{source_name}.allow_from").as_str(),
    )?;
    let deny_from = parse_sender_identifier_list(
        raw.deny_from.unwrap_or_default().as_slice(),
        format!("{source_name}.deny_from").as_str(),
    )?;
    let response_prefix = parse_optional_text_field(
        raw.response_prefix.unwrap_or_default(),
        format!("{source_name}.response_prefix").as_str(),
        256,
    )?;
    let auto_ack_text = parse_optional_text_field(
        raw.auto_ack_text.unwrap_or_default(),
        format!("{source_name}.auto_ack_text").as_str(),
        256,
    )?;
    let auto_reaction = parse_optional_text_field(
        raw.auto_reaction.unwrap_or_default(),
        format!("{source_name}.auto_reaction").as_str(),
        64,
    )?;
    let broadcast_strategy = if let Some(value) = raw.broadcast_strategy {
        parse_broadcast_strategy(
            value.as_str(),
            format!("{source_name}.broadcast_strategy").as_str(),
        )?
    } else {
        defaults.default_broadcast_strategy
    };
    let concurrency_limit = if let Some(value) = raw.concurrency_limit {
        Some(parse_positive_usize(value, format!("{source_name}.concurrency_limit").as_str())?)
    } else {
        Some(defaults.default_concurrency_limit)
    };

    Ok(ChannelRoutingRule {
        channel,
        enabled: raw.enabled.unwrap_or(defaults.default_channel_enabled),
        mention_patterns,
        allow_from,
        deny_from,
        allow_direct_messages: raw
            .allow_direct_messages
            .unwrap_or(defaults.default_allow_direct_messages),
        isolate_session_by_sender: raw
            .isolate_session_by_sender
            .unwrap_or(defaults.default_isolate_session_by_sender),
        response_prefix,
        auto_ack_text,
        auto_reaction,
        broadcast_strategy,
        concurrency_limit,
    })
}

fn normalize_host_candidate(raw: &str) -> Result<String> {
    let trimmed = raw.trim().trim_end_matches('.').to_ascii_lowercase();
    if trimmed.is_empty() {
        anyhow::bail!("host cannot be empty");
    }
    if !trimmed
        .chars()
        .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '.' | '-'))
    {
        anyhow::bail!("host must contain only ASCII letters, digits, dots, and hyphens");
    }
    if trimmed.starts_with('-')
        || trimmed.ends_with('-')
        || trimmed.starts_with('.')
        || trimmed.ends_with('.')
        || trimmed.contains("..")
    {
        anyhow::bail!("host has invalid dot/hyphen placement");
    }
    Ok(trimmed)
}

fn normalize_dns_suffix_candidate(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        anyhow::bail!("dns suffix cannot be empty");
    }
    if trimmed.contains("..") {
        anyhow::bail!("dns suffix cannot contain empty labels");
    }
    let normalized_host = normalize_host_candidate(trimmed.trim_start_matches('.'))?;
    Ok(format!(".{normalized_host}"))
}

fn parse_positive_u64(value: u64, name: &str) -> Result<u64> {
    if value == 0 {
        anyhow::bail!("{name} must be greater than 0");
    }
    Ok(value)
}

fn parse_positive_u32(value: u32, name: &str) -> Result<u32> {
    if value == 0 {
        anyhow::bail!("{name} must be greater than 0");
    }
    Ok(value)
}

fn parse_positive_usize(value: u64, name: &str) -> Result<usize> {
    if value == 0 {
        anyhow::bail!("{name} must be greater than 0");
    }
    usize::try_from(value).with_context(|| format!("{name} exceeds platform usize range"))
}

fn parse_default_memory_ttl_ms(value: i64, name: &str) -> Result<Option<i64>> {
    if value < 0 {
        anyhow::bail!("{name} must be >= 0");
    }
    if value == 0 {
        return Ok(None);
    }
    Ok(Some(value))
}

fn parse_retries(value: u32, name: &str) -> Result<u32> {
    const MAX_RETRIES: u32 = 10;
    if value > MAX_RETRIES {
        anyhow::bail!("{name} must be <= {MAX_RETRIES}");
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{
        parse_broadcast_strategy, parse_browser_service_endpoint,
        parse_canvas_host_public_base_url, parse_content_type_allowlist, parse_cron_timezone_mode,
        parse_default_memory_ttl_ms, parse_dns_suffix_allowlist, parse_host_allowlist,
        parse_http_header_allowlist, parse_journal_db_path,
        parse_model_provider_auth_provider_kind, parse_openai_base_url,
        parse_openai_embeddings_dims, parse_optional_auth_profile_id,
        parse_optional_browser_state_dir, parse_optional_openai_embeddings_model,
        parse_optional_vault_ref_field, parse_positive_usize, parse_process_executable_allowlist,
        parse_process_runner_egress_enforcement_mode, parse_process_runner_tier,
        parse_root_file_config, parse_storage_prefix_allowlist, parse_tool_allowlist,
        parse_vault_dir, parse_vault_ref_allowlist, AdminConfig, BrowserServiceConfig,
        CanvasHostConfig, ChannelRouterConfig, CronConfig, GatewayConfig, GatewayTlsConfig,
        HttpFetchConfig, IdentityConfig, MemoryConfig, ModelProviderConfig, OrchestratorConfig,
        StorageConfig, ToolCallConfig,
    };
    use crate::channel_router::BroadcastStrategy;
    use crate::model_provider::{ModelProviderAuthProviderKind, ModelProviderKind};
    use crate::sandbox_runner::{EgressEnforcementMode, SandboxProcessRunnerTier};
    use palyra_common::daemon_config_schema::RootFileConfig;

    #[test]
    fn identity_config_defaults_to_secure_mode() {
        let config = IdentityConfig::default();
        assert!(!config.allow_insecure_node_rpc_without_mtls);
    }

    #[test]
    fn identity_config_parses_file_override() {
        let parsed: RootFileConfig = toml::from_str(
            r#"
            [identity]
            allow_insecure_node_rpc_without_mtls = true
            "#,
        )
        .expect("toml should parse");
        let identity = parsed.identity.expect("identity config should be present");
        assert_eq!(identity.allow_insecure_node_rpc_without_mtls, Some(true));
    }

    #[test]
    fn gateway_config_defaults_to_quic_and_grpc_loopback() {
        let config = GatewayConfig::default();
        assert_eq!(config.grpc_bind_addr, "127.0.0.1");
        assert_eq!(config.grpc_port, 7443);
        assert_eq!(config.quic_bind_addr, "127.0.0.1");
        assert_eq!(config.quic_port, 7444);
        assert!(config.quic_enabled, "gateway transport should default to QUIC-enabled mode");
        assert!(
            !config.allow_insecure_remote,
            "remote exposure must require explicit insecure opt-in"
        );
        assert_eq!(
            config.vault_get_approval_required_refs,
            vec!["global/openai_api_key".to_owned()],
            "sensitive vault reads should require explicit approval by default"
        );
        assert_eq!(config.max_tape_entries_per_response, 1_000);
        assert_eq!(config.max_tape_bytes_per_response, 2 * 1024 * 1024);
        assert_eq!(config.tls, GatewayTlsConfig::default());
    }

    #[test]
    fn gateway_config_parses_vault_get_approval_required_refs() {
        let (parsed, _) = parse_root_file_config(
            r#"
            [gateway]
            vault_get_approval_required_refs = [
                "global/openai_api_key",
                "principal:user/openai_api_key",
            ]
            "#,
        )
        .expect("gateway vault approval refs should parse");
        let gateway = parsed.gateway.expect("gateway section should exist");
        assert_eq!(
            gateway.vault_get_approval_required_refs,
            Some(vec![
                "global/openai_api_key".to_owned(),
                "principal:user/openai_api_key".to_owned(),
            ])
        );
    }

    #[test]
    fn cron_config_defaults_to_utc_timezone() {
        let config = CronConfig::default();
        assert_eq!(
            config.timezone,
            crate::cron::CronTimezoneMode::Utc,
            "cron scheduler should default to UTC for deterministic cross-host behavior"
        );
    }

    #[test]
    fn cron_config_parses_timezone_override() {
        let (parsed, _) = parse_root_file_config(
            r#"
            [cron]
            timezone = "local"
            "#,
        )
        .expect("cron timezone override should parse");
        let cron = parsed.cron.expect("cron section should be present");
        assert_eq!(cron.timezone.as_deref(), Some("local"));
    }

    #[test]
    fn orchestrator_config_defaults_to_disabled_runloop() {
        let config = OrchestratorConfig::default();
        assert!(
            !config.runloop_v1_enabled,
            "orchestrator run loop should default disabled until explicitly enabled"
        );
    }

    #[test]
    fn memory_config_defaults_to_constrained_ingestion_with_auto_inject_disabled() {
        let config = MemoryConfig::default();
        assert_eq!(config.max_item_bytes, 16 * 1024);
        assert_eq!(config.max_item_tokens, 2_048);
        assert_eq!(config.default_ttl_ms, Some(30 * 24 * 60 * 60 * 1_000));
        assert!(!config.auto_inject.enabled, "memory auto-inject must default to disabled");
        assert_eq!(config.auto_inject.max_items, 3);
    }

    #[test]
    fn channel_router_defaults_to_disabled_deny_by_default() {
        let config = ChannelRouterConfig::default();
        assert!(!config.enabled, "channel router must require explicit opt-in");
        assert_eq!(config.max_message_bytes, 32 * 1024);
        assert_eq!(config.max_retry_queue_depth_per_channel, 64);
        assert_eq!(config.max_retry_attempts, 3);
        assert_eq!(config.retry_backoff_ms, 250);
        assert!(
            !config.default_channel_enabled,
            "per-channel routing should default disabled until explicitly configured"
        );
        assert_eq!(config.default_broadcast_strategy, BroadcastStrategy::Deny);
        assert_eq!(config.default_concurrency_limit, 2);
    }

    #[test]
    fn channel_router_config_parses_routing_rules() {
        let (parsed, _) = parse_root_file_config(
            r#"
            [channel_router]
            enabled = true
            max_message_bytes = 2048
            max_retry_queue_depth_per_channel = 8
            max_retry_attempts = 2
            retry_backoff_ms = 150
            default_response_prefix = "Palyra: "

            [channel_router.routing]
            default_channel_enabled = false
            default_allow_direct_messages = false
            default_isolate_session_by_sender = true
            default_broadcast_strategy = "mention_only"
            default_concurrency_limit = 3
            channels = [
                { channel = "slack", enabled = true, mention_patterns = ["@palyra"], allow_from = ["U123"], allow_direct_messages = true, broadcast_strategy = "allow", concurrency_limit = 1 }
            ]
            "#,
        )
        .expect("channel router config should parse");
        let channel_router = parsed.channel_router.expect("channel_router section should exist");
        assert_eq!(channel_router.enabled, Some(true));
        assert_eq!(channel_router.max_message_bytes, Some(2048));
        let routing = channel_router.routing.expect("routing section should exist");
        assert_eq!(routing.default_concurrency_limit, Some(3));
        let channels = routing.channels.expect("channels list should exist");
        assert_eq!(channels.len(), 1);
        assert_eq!(channels[0].channel.as_deref(), Some("slack"));
        assert_eq!(channels[0].broadcast_strategy.as_deref(), Some("allow"));
    }

    #[test]
    fn parse_broadcast_strategy_accepts_and_rejects_expected_values() {
        assert_eq!(
            parse_broadcast_strategy("deny", "channel_router.routing.default_broadcast_strategy")
                .expect("deny should parse"),
            BroadcastStrategy::Deny
        );
        assert_eq!(
            parse_broadcast_strategy(
                "mention_only",
                "channel_router.routing.default_broadcast_strategy",
            )
            .expect("mention_only should parse"),
            BroadcastStrategy::MentionOnly
        );
        assert_eq!(
            parse_broadcast_strategy("allow", "channel_router.routing.default_broadcast_strategy")
                .expect("allow should parse"),
            BroadcastStrategy::Allow
        );
        assert!(
            parse_broadcast_strategy("always", "channel_router.routing.default_broadcast_strategy")
                .is_err(),
            "unsupported broadcast strategy should be rejected"
        );
    }

    #[test]
    fn tool_call_config_defaults_to_deny_by_default_with_execution_limits() {
        let config = ToolCallConfig::default();
        assert!(
            config.allowed_tools.is_empty(),
            "tool call allowlist must default empty to enforce deny-by-default"
        );
        assert_eq!(config.max_calls_per_run, 4);
        assert_eq!(config.execution_timeout_ms, 750);
        assert!(!config.process_runner.enabled, "sandbox process runner must default to disabled");
        assert_eq!(
            config.process_runner.tier,
            SandboxProcessRunnerTier::B,
            "process runner tier must default to tier b until operator opts into tier c"
        );
        assert!(
            config.process_runner.allowed_executables.is_empty(),
            "sandbox process runner executable allowlist must default empty"
        );
        assert!(
            !config.process_runner.allow_interpreters,
            "interpreter execution must default to explicit opt-in"
        );
        assert_eq!(
            config.process_runner.egress_enforcement_mode,
            EgressEnforcementMode::Strict,
            "process runner egress enforcement must default to strict"
        );
        assert!(!config.wasm_runtime.enabled, "wasm plugin runtime must default to disabled");
        assert!(
            !config.wasm_runtime.allow_inline_modules,
            "inline wasm module payloads must default to explicit opt-in"
        );
        assert_eq!(config.wasm_runtime.max_module_size_bytes, 256 * 1024);
        assert!(
            config.wasm_runtime.allowed_http_hosts.is_empty(),
            "wasm runtime http allowlist must default empty"
        );
        assert!(
            !config.http_fetch.allow_private_targets,
            "http fetch must default to private-target denial"
        );
        assert_eq!(config.http_fetch.max_response_bytes, 512 * 1024);
        assert_eq!(config.http_fetch.max_redirects, 3);
        assert!(
            config.http_fetch.allowed_content_types.iter().any(|value| value == "text/html"),
            "http fetch default content-type allowlist should include text/html"
        );
        assert!(
            !config.browser_service.enabled,
            "browser service broker must default to explicit opt-in"
        );
        assert_eq!(config.browser_service.endpoint, "http://127.0.0.1:7543");
    }

    #[test]
    fn http_fetch_config_defaults_enforce_safe_limits() {
        let config = HttpFetchConfig::default();
        assert!(!config.allow_private_targets);
        assert_eq!(config.connect_timeout_ms, 1_500);
        assert_eq!(config.request_timeout_ms, 10_000);
        assert_eq!(config.max_response_bytes, 512 * 1024);
        assert!(config.allow_redirects);
        assert_eq!(config.max_redirects, 3);
        assert!(config.cache_enabled);
        assert_eq!(config.cache_ttl_ms, 30_000);
        assert_eq!(config.max_cache_entries, 256);
    }

    #[test]
    fn browser_service_config_defaults_are_local_and_bounded() {
        let config = BrowserServiceConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.endpoint, "http://127.0.0.1:7543");
        assert!(config.auth_token.is_none());
        assert!(
            config.state_dir.is_none(),
            "browser service state_dir should default to unset unless explicitly configured"
        );
        assert!(
            config.state_key_vault_ref.is_none(),
            "browser service state key vault ref should default to unset unless explicitly configured"
        );
        assert_eq!(config.connect_timeout_ms, 1_500);
        assert_eq!(config.request_timeout_ms, 15_000);
        assert_eq!(config.max_screenshot_bytes, 256 * 1024);
        assert_eq!(config.max_title_bytes, 4 * 1024);
    }

    #[test]
    fn canvas_host_config_defaults_to_disabled_with_bounded_limits() {
        let config = CanvasHostConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.public_base_url, "http://127.0.0.1:7142");
        assert_eq!(config.token_ttl_ms, 15 * 60 * 1_000);
        assert_eq!(config.max_state_bytes, 64 * 1024);
        assert_eq!(config.max_bundle_bytes, 512 * 1024);
        assert_eq!(config.max_assets_per_bundle, 32);
        assert_eq!(config.max_updates_per_minute, 120);
    }

    #[test]
    fn canvas_host_config_parses_overrides() {
        let (parsed, _) = parse_root_file_config(
            r#"
            [canvas_host]
            enabled = true
            public_base_url = "https://console.example.com/palyra"
            token_ttl_ms = 120000
            max_state_bytes = 8192
            max_bundle_bytes = 131072
            max_assets_per_bundle = 8
            max_updates_per_minute = 30
            "#,
        )
        .expect("canvas_host override should parse");
        let canvas_host = parsed.canvas_host.expect("canvas_host section should be present");
        assert_eq!(canvas_host.enabled, Some(true));
        assert_eq!(
            canvas_host.public_base_url,
            Some("https://console.example.com/palyra".to_owned())
        );
        assert_eq!(canvas_host.token_ttl_ms, Some(120_000));
        assert_eq!(canvas_host.max_state_bytes, Some(8_192));
        assert_eq!(canvas_host.max_bundle_bytes, Some(131_072));
        assert_eq!(canvas_host.max_assets_per_bundle, Some(8));
        assert_eq!(canvas_host.max_updates_per_minute, Some(30));
    }

    #[test]
    fn wasm_runtime_config_parses_allow_inline_modules_override() {
        let (parsed, _) = parse_root_file_config(
            r#"
            [tool_call.wasm_runtime]
            allow_inline_modules = true
            "#,
        )
        .expect("wasm runtime override should parse");
        let tool_call = parsed.tool_call.expect("tool_call section should be present");
        let wasm_runtime = tool_call.wasm_runtime.expect("wasm_runtime section should be present");
        assert_eq!(wasm_runtime.allow_inline_modules, Some(true));
    }

    #[test]
    fn process_runner_config_parses_tier_override() {
        let (parsed, _) = parse_root_file_config(
            r#"
            [tool_call.process_runner]
            tier = "c"
            "#,
        )
        .expect("process runner tier override should parse");
        let tool_call = parsed.tool_call.expect("tool_call section should be present");
        let process_runner =
            tool_call.process_runner.expect("process_runner section should be present");
        assert_eq!(process_runner.tier.as_deref(), Some("c"));
    }

    #[test]
    fn tool_call_config_parses_http_fetch_and_browser_service_overrides() {
        let (parsed, _) = parse_root_file_config(
            r#"
            [tool_call.http_fetch]
            request_timeout_ms = 22000
            max_response_bytes = 12345
            allowed_content_types = ["application/json"]
            allowed_request_headers = ["accept"]
            cache_enabled = false

            [tool_call.browser_service]
            enabled = true
            endpoint = "http://127.0.0.1:7543"
            state_dir = "data/browserd-state"
            state_key_vault_ref = "global/browserd_state_key"
            connect_timeout_ms = 2000
            request_timeout_ms = 18000
            max_screenshot_bytes = 131072
            max_title_bytes = 2048
            "#,
        )
        .expect("http fetch + browser service override should parse");
        let tool_call = parsed.tool_call.expect("tool_call section should be present");
        let http_fetch = tool_call.http_fetch.expect("http_fetch section should be present");
        assert_eq!(http_fetch.request_timeout_ms, Some(22_000));
        assert_eq!(http_fetch.max_response_bytes, Some(12_345));
        assert_eq!(http_fetch.allowed_content_types, Some(vec!["application/json".to_owned()]));
        assert_eq!(http_fetch.allowed_request_headers, Some(vec!["accept".to_owned()]));
        assert_eq!(http_fetch.cache_enabled, Some(false));

        let browser_service =
            tool_call.browser_service.expect("browser_service section should be present");
        assert_eq!(browser_service.enabled, Some(true));
        assert_eq!(browser_service.endpoint, Some("http://127.0.0.1:7543".to_owned()));
        assert_eq!(browser_service.state_dir, Some("data/browserd-state".to_owned()));
        assert_eq!(
            browser_service.state_key_vault_ref,
            Some("global/browserd_state_key".to_owned())
        );
        assert_eq!(browser_service.connect_timeout_ms, Some(2_000));
        assert_eq!(browser_service.request_timeout_ms, Some(18_000));
        assert_eq!(browser_service.max_screenshot_bytes, Some(131_072));
        assert_eq!(browser_service.max_title_bytes, Some(2_048));
    }

    #[test]
    fn model_provider_defaults_to_deterministic_with_safe_retry_policy() {
        let config = ModelProviderConfig::default();
        assert_eq!(config.kind, ModelProviderKind::Deterministic);
        assert_eq!(config.openai_base_url, "https://api.openai.com/v1");
        assert!(
            !config.allow_private_base_url,
            "model provider private-network base URLs must require explicit opt-in"
        );
        assert_eq!(config.openai_model, "gpt-4o-mini");
        assert!(
            config.openai_embeddings_model.is_none(),
            "openai embeddings model should default to unset"
        );
        assert!(
            config.openai_embeddings_dims.is_none(),
            "openai embeddings dims should default to unset"
        );
        assert!(config.openai_api_key.is_none(), "openai API key should default to unset");
        assert!(
            config.openai_api_key_vault_ref.is_none(),
            "openai API key vault ref should default to unset"
        );
        assert!(config.auth_profile_id.is_none(), "auth profile id should default to unset");
        assert!(
            config.auth_profile_provider_kind.is_none(),
            "auth provider kind should default to unset"
        );
        assert_eq!(config.max_retries, 2);
    }

    #[test]
    fn admin_config_defaults_to_deny_when_token_missing() {
        let config = AdminConfig::default();
        assert!(config.require_auth, "admin auth should default to required");
        assert!(config.auth_token.is_none(), "admin token should default to missing");
        assert!(
            config.bound_principal.is_none(),
            "admin token principal binding should default to missing until explicitly configured"
        );
    }

    #[test]
    fn storage_config_defaults_to_safe_journal_mode() {
        let config = StorageConfig::default();
        assert_eq!(config.journal_db_path, PathBuf::from("data/journal.sqlite3"));
        assert!(
            config.journal_hash_chain_enabled,
            "hash chain must default to enabled for tamper-evident audit journaling"
        );
        assert_eq!(
            config.max_journal_payload_bytes,
            256 * 1024,
            "journal payload limit should default to 256 KiB"
        );
        assert!(
            config.vault_dir.ends_with("vault"),
            "default vault directory should be rooted under state/vault"
        );
    }

    #[test]
    fn config_rejects_unknown_top_level_key() {
        let result: Result<RootFileConfig, _> =
            toml::from_str("unexpected=true\n[daemon]\nport=7142\n");
        assert!(result.is_err(), "unknown top-level keys must be rejected");
    }

    #[test]
    fn config_migrates_legacy_documents_without_explicit_version() {
        let (parsed, migration) =
            parse_root_file_config("[daemon]\nport=7142\n").expect("legacy config should parse");
        assert_eq!(parsed.version, Some(1));
        assert!(migration.migrated, "legacy config should trigger migration");
        assert_eq!(migration.source_version, 0);
        assert_eq!(migration.target_version, 1);
    }

    #[test]
    fn config_rejects_unsupported_future_version() {
        let error =
            parse_root_file_config("version=2\n[daemon]\nport=7142\n").expect_err("must fail");
        let rendered = format!("{error:#}");
        assert!(rendered.contains("unsupported config version 2"), "unexpected error: {rendered}");
    }

    #[test]
    fn config_rejects_unknown_daemon_key() {
        let result: Result<RootFileConfig, _> =
            toml::from_str("[daemon]\nport=7142\nunexpected=true\n");
        assert!(result.is_err(), "unknown daemon keys must be rejected");
    }

    #[test]
    fn config_rejects_unknown_identity_key() {
        let result: Result<RootFileConfig, _> = toml::from_str(
            "[identity]\nallow_insecure_node_rpc_without_mtls=true\nunexpected=true\n",
        );
        assert!(result.is_err(), "unknown identity keys must be rejected");
    }

    #[test]
    fn config_rejects_unknown_gateway_key() {
        let result: Result<RootFileConfig, _> =
            toml::from_str("[gateway]\ngrpc_port=7443\nunexpected=true\n");
        assert!(result.is_err(), "unknown gateway keys must be rejected");
    }

    #[test]
    fn config_rejects_unknown_orchestrator_key() {
        let result: Result<RootFileConfig, _> =
            toml::from_str("[orchestrator]\nrunloop_v1_enabled=true\nunexpected=true\n");
        assert!(result.is_err(), "unknown orchestrator keys must be rejected");
    }

    #[test]
    fn config_rejects_unknown_cron_key() {
        let result: Result<RootFileConfig, _> =
            toml::from_str("[cron]\ntimezone='utc'\nunexpected=true\n");
        assert!(result.is_err(), "unknown cron keys must be rejected");
    }

    #[test]
    fn config_rejects_unknown_memory_key() {
        let result: Result<RootFileConfig, _> =
            toml::from_str("[memory]\nmax_item_bytes=4096\nunexpected=true\n");
        assert!(result.is_err(), "unknown memory keys must be rejected");
    }

    #[test]
    fn config_rejects_unknown_memory_auto_inject_key() {
        let result: Result<RootFileConfig, _> =
            toml::from_str("[memory.auto_inject]\nenabled=true\nunexpected=true\n");
        assert!(result.is_err(), "unknown memory.auto_inject keys must be rejected");
    }

    #[test]
    fn config_rejects_unknown_model_provider_key() {
        let result: Result<RootFileConfig, _> =
            toml::from_str("[model_provider]\nkind='deterministic'\nunexpected=true\n");
        assert!(result.is_err(), "unknown model_provider keys must be rejected");
    }

    #[test]
    fn config_rejects_unknown_admin_key() {
        let result: Result<RootFileConfig, _> =
            toml::from_str("[admin]\nrequire_auth=true\nunexpected=true\n");
        assert!(result.is_err(), "unknown admin keys must be rejected");
    }

    #[test]
    fn config_rejects_unknown_storage_key() {
        let result: Result<RootFileConfig, _> =
            toml::from_str("[storage]\njournal_db_path='data/journal.sqlite3'\nunexpected=true\n");
        assert!(result.is_err(), "unknown storage keys must be rejected");
    }

    #[test]
    fn config_rejects_unknown_process_runner_key() {
        let result: Result<RootFileConfig, _> =
            toml::from_str("[tool_call.process_runner]\nenabled=true\nunexpected=true\n");
        assert!(result.is_err(), "unknown process runner keys must be rejected");
    }

    #[test]
    fn config_rejects_unknown_wasm_runtime_key() {
        let result: Result<RootFileConfig, _> =
            toml::from_str("[tool_call.wasm_runtime]\nenabled=true\nunexpected=true\n");
        assert!(result.is_err(), "unknown wasm runtime keys must be rejected");
    }

    #[test]
    fn config_rejects_unknown_http_fetch_key() {
        let result: Result<RootFileConfig, _> = toml::from_str(
            "[tool_call.http_fetch]\nallow_private_targets=false\nunexpected=true\n",
        );
        assert!(result.is_err(), "unknown http fetch keys must be rejected");
    }

    #[test]
    fn config_rejects_unknown_browser_service_key() {
        let result: Result<RootFileConfig, _> =
            toml::from_str("[tool_call.browser_service]\nenabled=true\nunexpected=true\n");
        assert!(result.is_err(), "unknown browser service keys must be rejected");
    }

    #[test]
    fn config_rejects_unknown_channel_router_key() {
        let result: Result<RootFileConfig, _> =
            toml::from_str("[channel_router]\nenabled=true\nunexpected=true\n");
        assert!(result.is_err(), "unknown channel_router keys must be rejected");
    }

    #[test]
    fn config_rejects_unknown_channel_router_routing_key() {
        let result: Result<RootFileConfig, _> = toml::from_str(
            "[channel_router.routing]\ndefault_channel_enabled=true\nunexpected=true\n",
        );
        assert!(result.is_err(), "unknown channel_router.routing keys must be rejected");
    }

    #[test]
    fn config_rejects_unknown_channel_router_channel_rule_key() {
        let result: Result<RootFileConfig, _> = toml::from_str(
            "[channel_router.routing]\nchannels = [{ channel = 'slack', enabled = true, unexpected = true }]\n",
        );
        assert!(
            result.is_err(),
            "unknown channel_router.routing.channels[*] keys must be rejected"
        );
    }

    #[test]
    fn config_rejects_unknown_canvas_host_key() {
        let result: Result<RootFileConfig, _> =
            toml::from_str("[canvas_host]\nenabled=true\nunexpected=true\n");
        assert!(result.is_err(), "unknown canvas_host keys must be rejected");
    }

    #[test]
    fn journal_db_path_rejects_parent_traversal() {
        let result = parse_journal_db_path("../secrets/journal.sqlite3");
        assert!(result.is_err(), "journal db path must reject parent traversal");
    }

    #[test]
    fn vault_dir_rejects_empty_and_nul() {
        assert!(parse_vault_dir("").is_err(), "vault dir must reject empty values");
        assert!(parse_vault_dir("vault\0dir").is_err(), "vault dir must reject embedded NUL");
    }

    #[test]
    fn openai_base_url_requires_https_scheme() {
        let result = parse_openai_base_url("file:///tmp/openai");
        assert!(result.is_err(), "openai base URL without https scheme must fail");
    }

    #[test]
    fn openai_base_url_rejects_non_loopback_http_url() {
        let result = parse_openai_base_url("http://example.com/v1");
        assert!(result.is_err(), "openai base URL over non-loopback HTTP must be rejected");
    }

    #[test]
    fn openai_base_url_accepts_loopback_http_url() {
        let parsed = parse_openai_base_url("http://127.0.0.1:8080/v1")
            .expect("loopback HTTP should be allowed for local testing");
        assert_eq!(parsed, "http://127.0.0.1:8080/v1");
    }

    #[test]
    fn openai_base_url_rejects_embedded_credentials() {
        let result = parse_openai_base_url("https://user:pass@example.com/v1");
        assert!(result.is_err(), "openai base URL with embedded credentials must be rejected");
    }

    #[test]
    fn openai_base_url_rejects_query_and_fragment() {
        let result = parse_openai_base_url("https://example.com/v1?api_key=secret#anchor");
        assert!(result.is_err(), "openai base URL with query or fragment must be rejected");
    }

    #[test]
    fn openai_base_url_accepts_clean_https_url() {
        let parsed =
            parse_openai_base_url("https://api.openai.com/v1").expect("base URL should parse");
        assert_eq!(parsed, "https://api.openai.com/v1");
    }

    #[test]
    fn model_provider_config_parses_private_base_url_opt_in_flag() {
        let (parsed, _) = parse_root_file_config(
            r#"
            [model_provider]
            allow_private_base_url = true
            "#,
        )
        .expect("model provider private-base-url opt-in should parse");
        let model_provider = parsed.model_provider.expect("model_provider section should exist");
        assert_eq!(model_provider.allow_private_base_url, Some(true));
    }

    #[test]
    fn model_provider_config_parses_embeddings_fields() {
        let (parsed, _) = parse_root_file_config(
            r#"
            [model_provider]
            kind = "openai_compatible"
            openai_embeddings_model = "text-embedding-3-small"
            openai_embeddings_dims = 1536
            "#,
        )
        .expect("model provider embeddings fields should parse");
        let model_provider = parsed.model_provider.expect("model_provider section should exist");
        assert_eq!(
            model_provider.openai_embeddings_model.as_deref(),
            Some("text-embedding-3-small")
        );
        assert_eq!(model_provider.openai_embeddings_dims, Some(1536));
    }

    #[test]
    fn parse_optional_openai_embeddings_model_trims_and_allows_clear() {
        let parsed = parse_optional_openai_embeddings_model(" text-embedding-3-large ")
            .expect("embeddings model should parse");
        assert_eq!(parsed, Some("text-embedding-3-large".to_owned()));

        let cleared = parse_optional_openai_embeddings_model("   ")
            .expect("blank embeddings model should clear configuration");
        assert!(cleared.is_none());
    }

    #[test]
    fn parse_openai_embeddings_dims_rejects_zero() {
        let error = parse_openai_embeddings_dims(0, "model_provider.openai_embeddings_dims")
            .expect_err("zero embeddings dims must be rejected");
        assert!(
            error.to_string().contains("greater than 0"),
            "error should explain positive dimensions requirement: {error}"
        );
    }

    #[test]
    fn parse_optional_auth_profile_id_normalizes_and_validates_values() {
        let parsed =
            parse_optional_auth_profile_id(" OpenAI.Default_1 ", "model_provider.auth_profile_id")
                .expect("auth profile id should parse");
        assert_eq!(parsed, Some("openai.default_1".to_owned()));

        let cleared = parse_optional_auth_profile_id("   ", "model_provider.auth_profile_id")
            .expect("empty auth profile id should clear value");
        assert!(cleared.is_none());

        let invalid =
            parse_optional_auth_profile_id("bad profile", "model_provider.auth_profile_id");
        assert!(invalid.is_err(), "invalid auth profile id should fail validation");
    }

    #[test]
    fn parse_model_provider_auth_provider_kind_accepts_aliases() {
        assert_eq!(
            parse_model_provider_auth_provider_kind("openai", "model_provider.auth_provider_kind",)
                .expect("openai kind should parse"),
            ModelProviderAuthProviderKind::Openai
        );
        assert_eq!(
            parse_model_provider_auth_provider_kind(
                "openai_compatible",
                "model_provider.auth_provider_kind",
            )
            .expect("openai_compatible kind should parse"),
            ModelProviderAuthProviderKind::Openai
        );
    }

    #[test]
    fn parse_model_provider_auth_provider_kind_rejects_unknown_values() {
        let result = parse_model_provider_auth_provider_kind(
            "anthropic",
            "model_provider.auth_provider_kind",
        );
        assert!(result.is_err(), "unsupported auth provider kind should fail validation");
    }

    #[test]
    fn parse_tool_allowlist_normalizes_and_deduplicates_values() {
        let parsed = parse_tool_allowlist(
            "palyra.echo, palyra.sleep ,palyra.echo,,",
            "PALYRA_TOOL_CALL_ALLOWED_TOOLS",
        )
        .expect("allowlist should parse");
        assert_eq!(parsed, vec!["palyra.echo".to_owned(), "palyra.sleep".to_owned()]);
    }

    #[test]
    fn parse_tool_allowlist_rejects_invalid_characters() {
        let result = parse_tool_allowlist("palyra.echo,../shell", "tool_call.allowed_tools");
        assert!(result.is_err(), "allowlist parser must reject invalid tool names");
    }

    #[test]
    fn parse_vault_ref_allowlist_normalizes_and_deduplicates_values() {
        let parsed = parse_vault_ref_allowlist(
            "GLOBAL/openai_api_key,global/openai_api_key,principal:user/openai_api_key",
            "gateway.vault_get_approval_required_refs",
        )
        .expect("vault ref allowlist should parse");
        assert_eq!(
            parsed,
            vec!["global/openai_api_key".to_owned(), "principal:user/openai_api_key".to_owned(),]
        );
    }

    #[test]
    fn parse_vault_ref_allowlist_rejects_invalid_entries() {
        let result = parse_vault_ref_allowlist(
            "global/not valid",
            "gateway.vault_get_approval_required_refs",
        );
        assert!(result.is_err(), "vault ref allowlist must reject invalid entries");
    }

    #[test]
    fn parse_optional_vault_ref_field_requires_single_entry_when_set() {
        let parsed = parse_optional_vault_ref_field(
            "GLOBAL/browserd_state_key",
            "tool_call.browser_service.state_key_vault_ref",
        )
        .expect("single optional vault ref should parse");
        assert_eq!(parsed, Some("global/browserd_state_key".to_owned()));

        let empty =
            parse_optional_vault_ref_field("   ", "tool_call.browser_service.state_key_vault_ref")
                .expect("empty optional vault ref should clear value");
        assert!(empty.is_none());

        let multiple = parse_optional_vault_ref_field(
            "global/key_a,global/key_b",
            "tool_call.browser_service.state_key_vault_ref",
        );
        assert!(multiple.is_err(), "multiple refs must be rejected for single-value field");
    }

    #[test]
    fn parse_optional_browser_state_dir_accepts_empty_and_rejects_nul() {
        let parsed = parse_optional_browser_state_dir(
            "data/browserd-state",
            "tool_call.browser_service.state_dir",
        )
        .expect("browser state dir should parse");
        assert_eq!(parsed, Some(PathBuf::from("data/browserd-state")));

        let empty = parse_optional_browser_state_dir("   ", "tool_call.browser_service.state_dir")
            .expect("empty browser state dir should clear value");
        assert!(empty.is_none());

        let invalid =
            parse_optional_browser_state_dir("state\0dir", "tool_call.browser_service.state_dir");
        assert!(invalid.is_err(), "embedded NUL must be rejected");
    }

    #[test]
    fn parse_process_executable_allowlist_normalizes_and_deduplicates_values() {
        let parsed = parse_process_executable_allowlist(
            "rustc, cargo ,rustc,,",
            "tool_call.process_runner.allowed_executables",
        )
        .expect("allowlist should parse");
        assert_eq!(parsed, vec!["rustc".to_owned(), "cargo".to_owned()]);
    }

    #[test]
    fn parse_process_runner_tier_accepts_supported_values() {
        assert_eq!(
            parse_process_runner_tier("b", "tool_call.process_runner.tier")
                .expect("tier b should parse"),
            SandboxProcessRunnerTier::B
        );
        assert_eq!(
            parse_process_runner_tier("tier_b", "tool_call.process_runner.tier")
                .expect("tier_b alias should parse"),
            SandboxProcessRunnerTier::B
        );
        assert_eq!(
            parse_process_runner_tier("c", "tool_call.process_runner.tier")
                .expect("tier c should parse"),
            SandboxProcessRunnerTier::C
        );
        assert_eq!(
            parse_process_runner_tier("tier_c", "tool_call.process_runner.tier")
                .expect("tier_c alias should parse"),
            SandboxProcessRunnerTier::C
        );
    }

    #[test]
    fn parse_process_runner_tier_rejects_unknown_values() {
        let result = parse_process_runner_tier("strict", "tool_call.process_runner.tier");
        assert!(result.is_err(), "unsupported process runner tier must fail parsing");
    }

    #[test]
    fn parse_process_runner_egress_enforcement_mode_accepts_supported_values() {
        assert_eq!(
            parse_process_runner_egress_enforcement_mode(
                "none",
                "tool_call.process_runner.egress_enforcement_mode",
            )
            .expect("none mode should parse"),
            EgressEnforcementMode::None
        );
        assert_eq!(
            parse_process_runner_egress_enforcement_mode(
                "preflight",
                "tool_call.process_runner.egress_enforcement_mode",
            )
            .expect("preflight mode should parse"),
            EgressEnforcementMode::Preflight
        );
        assert_eq!(
            parse_process_runner_egress_enforcement_mode(
                "strict",
                "tool_call.process_runner.egress_enforcement_mode",
            )
            .expect("strict mode should parse"),
            EgressEnforcementMode::Strict
        );
    }

    #[test]
    fn parse_process_runner_egress_enforcement_mode_rejects_unknown_values() {
        let result = parse_process_runner_egress_enforcement_mode(
            "best_effort",
            "tool_call.process_runner.egress_enforcement_mode",
        );
        assert!(result.is_err(), "unsupported egress enforcement mode must fail parsing");
    }

    #[test]
    fn parse_cron_timezone_mode_accepts_supported_values() {
        assert_eq!(
            parse_cron_timezone_mode("utc", "cron.timezone").expect("utc should parse"),
            crate::cron::CronTimezoneMode::Utc
        );
        assert_eq!(
            parse_cron_timezone_mode("local", "cron.timezone").expect("local should parse"),
            crate::cron::CronTimezoneMode::Local
        );
    }

    #[test]
    fn parse_cron_timezone_mode_rejects_unknown_values() {
        let result = parse_cron_timezone_mode("Europe/Prague", "cron.timezone");
        assert!(result.is_err(), "unsupported cron timezone mode must be rejected");
    }

    #[test]
    fn parse_host_allowlist_normalizes_and_deduplicates_values() {
        let parsed = parse_host_allowlist(
            "EXAMPLE.COM, api.example.com.,example.com",
            "tool_call.process_runner.allowed_egress_hosts",
        )
        .expect("host allowlist should parse");
        assert_eq!(parsed, vec!["example.com".to_owned(), "api.example.com".to_owned()]);
    }

    #[test]
    fn parse_dns_suffix_allowlist_normalizes_and_deduplicates_values() {
        let parsed = parse_dns_suffix_allowlist(
            "example.com,.corp.local,example.com",
            "tool_call.process_runner.allowed_dns_suffixes",
        )
        .expect("dns suffix allowlist should parse");
        assert_eq!(parsed, vec![".example.com".to_owned(), ".corp.local".to_owned()]);
    }

    #[test]
    fn parse_dns_suffix_allowlist_rejects_invalid_values() {
        let result = parse_dns_suffix_allowlist(
            "..example.com",
            "tool_call.process_runner.allowed_dns_suffixes",
        );
        assert!(result.is_err(), "dns suffix allowlist must reject malformed entries");
    }

    #[test]
    fn parse_content_type_allowlist_normalizes_and_deduplicates_values() {
        let parsed = parse_content_type_allowlist(
            "TEXT/HTML, application/json,text/html",
            "tool_call.http_fetch.allowed_content_types",
        )
        .expect("content-type allowlist should parse");
        assert_eq!(parsed, vec!["text/html".to_owned(), "application/json".to_owned()]);
    }

    #[test]
    fn parse_content_type_allowlist_rejects_parameters_and_whitespace() {
        let result = parse_content_type_allowlist(
            "text/html; charset=utf-8",
            "tool_call.http_fetch.allowed_content_types",
        );
        assert!(result.is_err(), "content-type allowlist must reject parameters");
    }

    #[test]
    fn parse_http_header_allowlist_normalizes_and_deduplicates_values() {
        let parsed = parse_http_header_allowlist(
            "User-Agent,accept,user-agent",
            "tool_call.http_fetch.allowed_request_headers",
        )
        .expect("header allowlist should parse");
        assert_eq!(parsed, vec!["user-agent".to_owned(), "accept".to_owned()]);
    }

    #[test]
    fn parse_http_header_allowlist_rejects_invalid_header_names() {
        let result = parse_http_header_allowlist(
            "x-custom, bad header",
            "tool_call.http_fetch.allowed_request_headers",
        );
        assert!(result.is_err(), "header allowlist must reject invalid header names");
    }

    #[test]
    fn parse_browser_service_endpoint_requires_http_or_https_without_path() {
        assert!(
            parse_browser_service_endpoint(
                "grpc://127.0.0.1:7543",
                "tool_call.browser_service.endpoint",
            )
            .is_err(),
            "unsupported scheme must fail"
        );
        assert!(
            parse_browser_service_endpoint(
                "http://127.0.0.1:7543/browser",
                "tool_call.browser_service.endpoint",
            )
            .is_err(),
            "path segments must fail"
        );
        let parsed = parse_browser_service_endpoint(
            "https://browserd.internal:7443/",
            "tool_call.browser_service.endpoint",
        )
        .expect("valid endpoint should parse");
        assert_eq!(parsed, "https://browserd.internal:7443");
    }

    #[test]
    fn parse_canvas_host_public_base_url_requires_http_or_https_without_query_or_fragment() {
        assert!(
            parse_canvas_host_public_base_url(
                "grpc://127.0.0.1:7142",
                "canvas_host.public_base_url",
            )
            .is_err(),
            "unsupported scheme must fail"
        );
        assert!(
            parse_canvas_host_public_base_url(
                "https://console.example.com/base?debug=true",
                "canvas_host.public_base_url",
            )
            .is_err(),
            "query component must fail"
        );
        let parsed = parse_canvas_host_public_base_url(
            "https://console.example.com/base/",
            "canvas_host.public_base_url",
        )
        .expect("valid base URL should parse");
        assert_eq!(parsed, "https://console.example.com/base");
    }

    #[test]
    fn parse_storage_prefix_allowlist_normalizes_and_deduplicates_values() {
        let parsed = parse_storage_prefix_allowlist(
            "plugins/cache, plugins/cache ,plugins/artifacts",
            "tool_call.wasm_runtime.allowed_storage_prefixes",
        )
        .expect("storage prefix allowlist should parse");
        assert_eq!(parsed, vec!["plugins/cache".to_owned(), "plugins/artifacts".to_owned()]);
    }

    #[test]
    fn parse_storage_prefix_allowlist_rejects_parent_traversal() {
        let result = parse_storage_prefix_allowlist(
            "plugins/../escape",
            "tool_call.wasm_runtime.allowed_storage_prefixes",
        );
        assert!(result.is_err(), "storage prefix allowlist must reject parent traversal");
    }

    #[test]
    fn parse_positive_usize_rejects_zero() {
        let result = parse_positive_usize(0, "gateway.max_tape_entries_per_response");
        assert!(result.is_err(), "zero should not be accepted for positive usize fields");
    }

    #[test]
    fn parse_default_memory_ttl_zero_disables_default_ttl() {
        let parsed =
            parse_default_memory_ttl_ms(0, "memory.default_ttl_ms").expect("ttl should parse");
        assert_eq!(parsed, None);
    }

    #[test]
    fn parse_default_memory_ttl_rejects_negative_values() {
        let result = parse_default_memory_ttl_ms(-1, "memory.default_ttl_ms");
        assert!(result.is_err(), "negative ttl should be rejected");
    }
}
