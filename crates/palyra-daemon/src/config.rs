use std::{
    env, fs,
    path::{Component, PathBuf},
};

use anyhow::{Context, Result};
use palyra_common::{
    daemon_config_schema::RootFileConfig, default_config_search_paths, parse_config_path,
};

use crate::model_provider::{ModelProviderConfig, ModelProviderKind};

const DEFAULT_BIND_ADDR: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 7142;
const DEFAULT_GRPC_BIND_ADDR: &str = "127.0.0.1";
const DEFAULT_GRPC_PORT: u16 = 7443;
const DEFAULT_QUIC_BIND_ADDR: &str = "127.0.0.1";
const DEFAULT_QUIC_PORT: u16 = 7444;
const DEFAULT_QUIC_ENABLED: bool = true;
const DEFAULT_ORCHESTRATOR_RUNLOOP_V1_ENABLED: bool = false;
const DEFAULT_ADMIN_REQUIRE_AUTH: bool = true;
const DEFAULT_ALLOW_INSECURE_NODE_RPC_WITHOUT_MTLS: bool = false;
const DEFAULT_JOURNAL_DB_PATH: &str = "data/journal.sqlite3";
const DEFAULT_JOURNAL_HASH_CHAIN_ENABLED: bool = false;
const DEFAULT_TOOL_CALL_MAX_CALLS_PER_RUN: u32 = 4;
const DEFAULT_TOOL_CALL_EXECUTION_TIMEOUT_MS: u64 = 750;
const DEFAULT_PROCESS_RUNNER_ENABLED: bool = false;
const DEFAULT_PROCESS_RUNNER_WORKSPACE_ROOT: &str = ".";
const DEFAULT_PROCESS_RUNNER_CPU_TIME_LIMIT_MS: u64 = 2_000;
const DEFAULT_PROCESS_RUNNER_MEMORY_LIMIT_BYTES: u64 = 256 * 1024 * 1024;
const DEFAULT_PROCESS_RUNNER_MAX_OUTPUT_BYTES: u64 = 64 * 1024;
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedConfig {
    pub source: String,
    pub daemon: DaemonConfig,
    pub gateway: GatewayConfig,
    pub orchestrator: OrchestratorConfig,
    pub model_provider: ModelProviderConfig,
    pub tool_call: ToolCallConfig,
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrchestratorConfig {
    pub runloop_v1_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCallConfig {
    pub allowed_tools: Vec<String>,
    pub max_calls_per_run: u32,
    pub execution_timeout_ms: u64,
    pub process_runner: ProcessRunnerConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessRunnerConfig {
    pub enabled: bool,
    pub workspace_root: PathBuf,
    pub allowed_executables: Vec<String>,
    pub allowed_egress_hosts: Vec<String>,
    pub allowed_dns_suffixes: Vec<String>,
    pub cpu_time_limit_ms: u64,
    pub memory_limit_bytes: u64,
    pub max_output_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminConfig {
    pub require_auth: bool,
    pub auth_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityConfig {
    pub allow_insecure_node_rpc_without_mtls: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageConfig {
    pub journal_db_path: PathBuf,
    pub journal_hash_chain_enabled: bool,
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
        }
    }
}

impl Default for OrchestratorConfig {
    fn default() -> Self {
        Self { runloop_v1_enabled: DEFAULT_ORCHESTRATOR_RUNLOOP_V1_ENABLED }
    }
}

impl Default for ToolCallConfig {
    fn default() -> Self {
        Self {
            allowed_tools: Vec::new(),
            max_calls_per_run: DEFAULT_TOOL_CALL_MAX_CALLS_PER_RUN,
            execution_timeout_ms: DEFAULT_TOOL_CALL_EXECUTION_TIMEOUT_MS,
            process_runner: ProcessRunnerConfig::default(),
        }
    }
}

impl Default for ProcessRunnerConfig {
    fn default() -> Self {
        Self {
            enabled: DEFAULT_PROCESS_RUNNER_ENABLED,
            workspace_root: PathBuf::from(DEFAULT_PROCESS_RUNNER_WORKSPACE_ROOT),
            allowed_executables: Vec::new(),
            allowed_egress_hosts: Vec::new(),
            allowed_dns_suffixes: Vec::new(),
            cpu_time_limit_ms: DEFAULT_PROCESS_RUNNER_CPU_TIME_LIMIT_MS,
            memory_limit_bytes: DEFAULT_PROCESS_RUNNER_MEMORY_LIMIT_BYTES,
            max_output_bytes: DEFAULT_PROCESS_RUNNER_MAX_OUTPUT_BYTES,
        }
    }
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self { require_auth: DEFAULT_ADMIN_REQUIRE_AUTH, auth_token: None }
    }
}

pub fn load_config() -> Result<LoadedConfig> {
    let mut daemon = DaemonConfig::default();
    let mut gateway = GatewayConfig::default();
    let mut orchestrator = OrchestratorConfig::default();
    let mut model_provider = ModelProviderConfig::default();
    let mut tool_call = ToolCallConfig::default();
    let mut admin = AdminConfig::default();
    let mut identity = IdentityConfig::default();
    let mut storage = StorageConfig::default();
    let mut source = "defaults".to_owned();

    if let Some(path) = find_config_path()? {
        let content = fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let parsed: RootFileConfig = toml::from_str(&content)
            .with_context(|| format!("failed to parse config {}", path.display()))?;
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
        }
        if let Some(file_orchestrator) = parsed.orchestrator {
            if let Some(runloop_v1_enabled) = file_orchestrator.runloop_v1_enabled {
                orchestrator.runloop_v1_enabled = runloop_v1_enabled;
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
            if let Some(openai_model) = file_model_provider.openai_model {
                model_provider.openai_model = parse_openai_model(openai_model.as_str())?;
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
        }
        if let Some(file_admin) = parsed.admin {
            if let Some(require_auth) = file_admin.require_auth {
                admin.require_auth = require_auth;
            }
            if let Some(auth_token) = file_admin.auth_token {
                admin.auth_token =
                    if auth_token.trim().is_empty() { None } else { Some(auth_token) };
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
        }
        source = path.to_string_lossy().into_owned();
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

    if let Ok(runloop_v1_enabled) = env::var("PALYRA_ORCHESTRATOR_RUNLOOP_V1_ENABLED") {
        orchestrator.runloop_v1_enabled = runloop_v1_enabled
            .parse::<bool>()
            .context("PALYRA_ORCHESTRATOR_RUNLOOP_V1_ENABLED must be true or false")?;
        source.push_str(" +env(PALYRA_ORCHESTRATOR_RUNLOOP_V1_ENABLED)");
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

    if let Ok(openai_model) = env::var("PALYRA_MODEL_PROVIDER_OPENAI_MODEL") {
        model_provider.openai_model = parse_openai_model(openai_model.as_str())?;
        source.push_str(" +env(PALYRA_MODEL_PROVIDER_OPENAI_MODEL)");
    }

    if let Ok(openai_api_key) = env::var("PALYRA_MODEL_PROVIDER_OPENAI_API_KEY") {
        model_provider.openai_api_key =
            if openai_api_key.trim().is_empty() { None } else { Some(openai_api_key) };
        source.push_str(" +env(PALYRA_MODEL_PROVIDER_OPENAI_API_KEY)");
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

    Ok(LoadedConfig {
        source,
        daemon,
        gateway,
        orchestrator,
        model_provider,
        tool_call,
        admin,
        identity,
        storage,
    })
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

fn parse_openai_base_url(raw: &str) -> Result<String> {
    if raw.trim().is_empty() {
        anyhow::bail!("openai base URL cannot be empty");
    }
    let normalized = raw.trim();
    let parsed =
        reqwest::Url::parse(normalized).context("openai base URL must be a valid absolute URL")?;
    if parsed.scheme() != "http" && parsed.scheme() != "https" {
        anyhow::bail!("openai base URL must use http or https");
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

fn parse_tool_allowlist(raw: &str, source_name: &str) -> Result<Vec<String>> {
    parse_identifier_allowlist(raw, source_name, "tool name")
}

fn parse_process_executable_allowlist(raw: &str, source_name: &str) -> Result<Vec<String>> {
    parse_identifier_allowlist(raw, source_name, "executable name")
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
        parse_dns_suffix_allowlist, parse_host_allowlist, parse_journal_db_path,
        parse_openai_base_url, parse_process_executable_allowlist, parse_tool_allowlist,
        AdminConfig, GatewayConfig, IdentityConfig, ModelProviderConfig, OrchestratorConfig,
        StorageConfig, ToolCallConfig,
    };
    use crate::model_provider::ModelProviderKind;
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
    fn tool_call_config_defaults_to_deny_by_default_with_execution_limits() {
        let config = ToolCallConfig::default();
        assert!(
            config.allowed_tools.is_empty(),
            "tool call allowlist must default empty to enforce deny-by-default"
        );
        assert_eq!(config.max_calls_per_run, 4);
        assert_eq!(config.execution_timeout_ms, 750);
        assert!(!config.process_runner.enabled, "sandbox process runner must default to disabled");
        assert!(
            config.process_runner.allowed_executables.is_empty(),
            "sandbox process runner executable allowlist must default empty"
        );
    }

    #[test]
    fn model_provider_defaults_to_deterministic_with_safe_retry_policy() {
        let config = ModelProviderConfig::default();
        assert_eq!(config.kind, ModelProviderKind::Deterministic);
        assert_eq!(config.openai_base_url, "https://api.openai.com/v1");
        assert_eq!(config.openai_model, "gpt-4o-mini");
        assert!(config.openai_api_key.is_none(), "openai API key should default to unset");
        assert_eq!(config.max_retries, 2);
    }

    #[test]
    fn admin_config_defaults_to_deny_when_token_missing() {
        let config = AdminConfig::default();
        assert!(config.require_auth, "admin auth should default to required");
        assert!(config.auth_token.is_none(), "admin token should default to missing");
    }

    #[test]
    fn storage_config_defaults_to_safe_journal_mode() {
        let config = StorageConfig::default();
        assert_eq!(config.journal_db_path, PathBuf::from("data/journal.sqlite3"));
        assert!(
            !config.journal_hash_chain_enabled,
            "hash chain must default to disabled until audit mode is explicitly enabled"
        );
    }

    #[test]
    fn config_rejects_unknown_top_level_key() {
        let result: Result<RootFileConfig, _> =
            toml::from_str("unexpected=true\n[daemon]\nport=7142\n");
        assert!(result.is_err(), "unknown top-level keys must be rejected");
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
    fn journal_db_path_rejects_parent_traversal() {
        let result = parse_journal_db_path("../secrets/journal.sqlite3");
        assert!(result.is_err(), "journal db path must reject parent traversal");
    }

    #[test]
    fn openai_base_url_requires_http_scheme() {
        let result = parse_openai_base_url("file:///tmp/openai");
        assert!(result.is_err(), "openai base URL without http/https scheme must fail");
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
    fn parse_process_executable_allowlist_normalizes_and_deduplicates_values() {
        let parsed = parse_process_executable_allowlist(
            "rustc, cargo ,rustc,,",
            "tool_call.process_runner.allowed_executables",
        )
        .expect("allowlist should parse");
        assert_eq!(parsed, vec!["rustc".to_owned(), "cargo".to_owned()]);
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
}
