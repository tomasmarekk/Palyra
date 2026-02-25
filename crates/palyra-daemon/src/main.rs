mod agents;
mod channel_router;
mod config;
mod cron;
mod gateway;
mod journal;
mod model_provider;
mod node_rpc;
mod orchestrator;
mod quic_runtime;
mod sandbox_runner;
mod tool_protocol;
mod wasm_plugin_runner;

use std::{
    collections::{HashMap, HashSet},
    fs,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::{Path as FsPath, PathBuf},
    sync::{Arc, Mutex},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use axum::{
    extract::{ConnectInfo, DefaultBodyLimit, Path, Query, Request, State},
    http::{
        header::{
            AUTHORIZATION, CACHE_CONTROL, CONTENT_SECURITY_POLICY, CONTENT_TYPE, COOKIE, SET_COOKIE,
        },
        HeaderMap, HeaderName, HeaderValue, StatusCode,
    },
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use config::load_config;
use cron::spawn_scheduler_loop;
use gateway::{
    authorize_headers, request_context_from_headers, AuthError, CanvasAssetResponse,
    GatewayAuthConfig, GatewayJournalConfigSnapshot, GatewayRuntimeConfigSnapshot,
    GatewayRuntimeState, MemoryRuntimeConfig,
};
use journal::{
    ApprovalDecision, ApprovalDecisionScope, ApprovalSubjectType, CronJobUpdatePatch,
    JournalAppendRequest, JournalConfig, JournalStore, MemoryPurgeRequest,
    OrchestratorCancelRequest, OrchestratorRunStatusSnapshot, SkillExecutionStatus,
    SkillStatusRecord, SkillStatusUpsertRequest,
};
use model_provider::{build_model_provider, ModelProviderKind};
use palyra_auth::{AuthProfileRegistry, HttpOAuthRefreshAdapter, OAuthRefreshAdapter};
use palyra_common::default_identity_store_root;
use palyra_common::{
    build_metadata, health_response, parse_daemon_bind_socket, validate_canonical_id,
    HealthResponse,
};
use palyra_identity::IdentityManager;
use palyra_identity::{FilesystemSecretStore, SecretStore};
use palyra_policy::{evaluate_with_config, PolicyDecision, PolicyEvaluationConfig, PolicyRequest};
use palyra_skills::{
    audit_skill_artifact_security, inspect_skill_artifact, verify_skill_artifact,
    SkillSecurityAuditPolicy, SkillTrustStore,
};
use palyra_transport_quic::QuicTransportLimits;
use palyra_vault::{Vault, VaultConfig as VaultConfigOptions, VaultRef};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::sync::Notify;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{
    metadata::MetadataValue,
    transport::{Certificate, Identity, Server, ServerTlsConfig},
    Request as TonicRequest,
};
use tracing::info;
use tracing_subscriber::EnvFilter;
use ulid::Ulid;

use crate::gateway::proto::palyra::{common::v1 as common_v1, cron::v1 as cron_v1};

const DANGEROUS_REMOTE_BIND_ACK_ENV: &str = "PALYRA_GATEWAY_DANGEROUS_REMOTE_BIND_ACK";
const SYSTEM_DAEMON_PRINCIPAL: &str = "system:daemon";
const SYSTEM_VAULT_CHANNEL: &str = "system:vault";
const SYSTEM_DAEMON_DEVICE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
const GRPC_MAX_DECODING_MESSAGE_SIZE_BYTES: usize = 4 * 1024 * 1024;
const GRPC_MAX_ENCODING_MESSAGE_SIZE_BYTES: usize = 4 * 1024 * 1024;
const ADMIN_RATE_LIMIT_WINDOW_MS: u64 = 1_000;
const ADMIN_RATE_LIMIT_MAX_REQUESTS_PER_WINDOW: u32 = 30;
const ADMIN_RATE_LIMIT_MAX_IP_BUCKETS: usize = 4_096;
const HTTP_MAX_REQUEST_BODY_BYTES: usize = 64 * 1024;
const CONSOLE_SESSION_COOKIE_NAME: &str = "palyra_console_session";
const CONSOLE_CSRF_HEADER_NAME: &str = "x-palyra-csrf-token";
const CONSOLE_SESSION_TTL_SECONDS: u64 = 30 * 60;
const CONSOLE_MAX_ACTIVE_SESSIONS: usize = 1_024;
const SKILLS_LAYOUT_VERSION: u32 = 1;
const SKILLS_INDEX_FILE_NAME: &str = "installed-index.json";
const SKILL_ARTIFACT_FILE_NAME: &str = "artifact.palyra-skill";

#[derive(Debug, Clone, Parser)]
#[command(name = "palyrad", about = "Palyra gateway skeleton daemon")]
struct Args {
    #[arg(long)]
    bind: Option<String>,
    #[arg(long)]
    port: Option<u16>,
    #[arg(long)]
    grpc_bind: Option<String>,
    #[arg(long)]
    grpc_port: Option<u16>,
    #[arg(long, default_value_t = false)]
    journal_migrate_only: bool,
}

#[derive(Clone)]
struct AppState {
    started_at: Instant,
    runtime: Arc<GatewayRuntimeState>,
    auth_runtime: Arc<gateway::AuthRuntimeState>,
    auth: GatewayAuthConfig,
    admin_rate_limit: Arc<Mutex<HashMap<IpAddr, AdminRateLimitEntry>>>,
    cron_timezone_mode: cron::CronTimezoneMode,
    grpc_url: String,
    scheduler_wake: Arc<Notify>,
    console_sessions: Arc<Mutex<HashMap<String, ConsoleSession>>>,
}

#[derive(Debug, Clone, Copy)]
struct AdminRateLimitEntry {
    window_started_at: Instant,
    requests_in_window: u32,
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
}

#[derive(Debug, Deserialize)]
struct JournalRecentQuery {
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct RunTapeQuery {
    after_seq: Option<i64>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct RunCancelRequest {
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SkillStatusRequest {
    version: String,
    reason: Option<String>,
    #[serde(rename = "override")]
    override_enabled: Option<bool>,
}

#[derive(Debug, Serialize)]
struct SkillStatusResponse {
    skill_id: String,
    version: String,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    detected_at_ms: i64,
    operator_principal: String,
}

#[derive(Debug, Clone)]
struct ConsoleSession {
    session_id: String,
    csrf_token: String,
    context: gateway::RequestContext,
    issued_at_unix_ms: i64,
    expires_at_unix_ms: i64,
}

#[derive(Debug, Serialize)]
struct ConsoleSessionResponse {
    principal: String,
    device_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    channel: Option<String>,
    csrf_token: String,
    issued_at_unix_ms: i64,
    expires_at_unix_ms: i64,
}

#[derive(Debug, Deserialize)]
struct ConsoleLoginRequest {
    admin_token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ConsoleApprovalsQuery {
    after_approval_id: Option<String>,
    limit: Option<usize>,
    since_unix_ms: Option<i64>,
    until_unix_ms: Option<i64>,
    subject_id: Option<String>,
    principal: Option<String>,
    decision: Option<String>,
    subject_type: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ConsoleApprovalDecisionRequest {
    approved: bool,
    reason: Option<String>,
    decision_scope: Option<String>,
    decision_scope_ttl_ms: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct ConsoleCronCreateRequest {
    name: String,
    prompt: String,
    #[serde(default)]
    owner_principal: Option<String>,
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    session_key: Option<String>,
    #[serde(default)]
    session_label: Option<String>,
    schedule_type: String,
    #[serde(default)]
    cron_expression: Option<String>,
    #[serde(default)]
    every_interval_ms: Option<u64>,
    #[serde(default)]
    at_timestamp_rfc3339: Option<String>,
    #[serde(default)]
    enabled: Option<bool>,
    #[serde(default)]
    jitter_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ConsoleCronEnabledRequest {
    enabled: bool,
}

#[derive(Debug, Deserialize)]
struct ConsoleMemorySearchQuery {
    query: String,
    top_k: Option<usize>,
    min_score: Option<f64>,
    #[serde(default)]
    tags_csv: Option<String>,
    #[serde(default)]
    sources_csv: Option<String>,
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ConsoleMemoryPurgeRequest {
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    purge_all_principal: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ConsoleSkillsListQuery {
    skill_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ConsoleSkillInstallRequest {
    artifact_path: String,
    #[serde(default)]
    allow_tofu: Option<bool>,
    #[serde(default)]
    allow_untrusted: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ConsoleSkillActionRequest {
    #[serde(default)]
    version: Option<String>,
    #[serde(default)]
    allow_tofu: Option<bool>,
    #[serde(default)]
    quarantine_on_fail: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ConsoleAuditEventsQuery {
    limit: Option<usize>,
    kind: Option<i32>,
    principal: Option<String>,
    channel: Option<String>,
    contains: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InstalledSkillsIndex {
    schema_version: u32,
    updated_at_unix_ms: i64,
    #[serde(default)]
    entries: Vec<InstalledSkillRecord>,
}

impl Default for InstalledSkillsIndex {
    fn default() -> Self {
        Self { schema_version: SKILLS_LAYOUT_VERSION, updated_at_unix_ms: 0, entries: Vec::new() }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InstalledSkillRecord {
    skill_id: String,
    version: String,
    publisher: String,
    current: bool,
    installed_at_unix_ms: i64,
    artifact_sha256: String,
    payload_sha256: String,
    signature_key_id: String,
    trust_decision: String,
    source: InstalledSkillSource,
    #[serde(default)]
    missing_secrets: Vec<MissingSkillSecret>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InstalledSkillSource {
    kind: String,
    reference: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct MissingSkillSecret {
    scope: String,
    key: String,
}

#[derive(Debug, Deserialize)]
struct CanvasTokenQuery {
    token: String,
}

#[derive(Debug, Deserialize)]
struct CanvasRuntimeQuery {
    canvas_id: String,
    token: String,
}

#[derive(Debug, Deserialize)]
struct CanvasStateQuery {
    token: String,
    after_version: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct PolicyExplainQuery {
    principal: String,
    action: String,
    resource: String,
}

#[derive(Debug, Serialize)]
struct PolicyExplainResponse {
    principal: String,
    action: String,
    resource: String,
    decision: String,
    approval_required: bool,
    reason: String,
    matched_policies: Vec<String>,
}

#[derive(Debug, Clone)]
struct IdentityRuntime {
    store_root: PathBuf,
    revoked_certificate_count: usize,
    revoked_certificate_fingerprints: HashSet<String>,
    gateway_ca_certificate_pem: String,
    node_server_certificate: palyra_identity::IssuedCertificate,
}

#[derive(Debug, Clone)]
struct SecretAccessAuditRecord {
    scope: String,
    key: String,
    action: String,
    value_bytes: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let args = Args::parse();
    let mut loaded = load_config()?;
    if let Some(bind) = args.bind {
        loaded.daemon.bind_addr = bind;
        loaded.source.push_str(" +cli(--bind)");
    }
    if let Some(port) = args.port {
        loaded.daemon.port = port;
        loaded.source.push_str(" +cli(--port)");
    }
    if let Some(grpc_bind) = args.grpc_bind {
        loaded.gateway.grpc_bind_addr = grpc_bind;
        loaded.source.push_str(" +cli(--grpc-bind)");
    }
    if let Some(grpc_port) = args.grpc_port {
        loaded.gateway.grpc_port = grpc_port;
        loaded.source.push_str(" +cli(--grpc-port)");
    }
    validate_process_runner_backend_policy(
        loaded.tool_call.process_runner.enabled,
        loaded.tool_call.process_runner.tier,
        loaded.tool_call.process_runner.egress_enforcement_mode,
        !loaded.tool_call.process_runner.allowed_egress_hosts.is_empty()
            || !loaded.tool_call.process_runner.allowed_dns_suffixes.is_empty(),
    )?;
    let node_rpc_mtls_required = !loaded.identity.allow_insecure_node_rpc_without_mtls;

    let identity_runtime = load_identity_runtime(loaded.gateway.identity_store_dir.clone())
        .context("failed to initialize gateway identity runtime")?;
    let auth = GatewayAuthConfig {
        require_auth: loaded.admin.require_auth,
        admin_token: loaded.admin.auth_token.clone(),
        bound_principal: loaded.admin.bound_principal.clone(),
    };
    validate_admin_auth_config(&auth)?;
    let journal_store = JournalStore::open(JournalConfig {
        db_path: loaded.storage.journal_db_path.clone(),
        hash_chain_enabled: loaded.storage.journal_hash_chain_enabled,
        max_payload_bytes: loaded.storage.max_journal_payload_bytes,
    })
    .context("failed to initialize event journal storage")?;
    let vault = Arc::new(
        Vault::open_with_config(VaultConfigOptions {
            root: Some(loaded.storage.vault_dir.clone()),
            identity_store_root: Some(identity_runtime.store_root.clone()),
            ..VaultConfigOptions::default()
        })
        .context("failed to initialize vault runtime")?,
    );
    if let Some(access_audit) =
        resolve_model_provider_secret_from_vault(&mut loaded, vault.as_ref())?
    {
        record_secret_access_journal_event(&journal_store, &access_audit)
            .context("failed to audit model provider secret access")?;
    }
    let model_provider = build_model_provider(&loaded.model_provider)
        .context("failed to initialize model provider runtime")?;
    let agent_registry = agents::AgentRegistry::open(identity_runtime.store_root.as_path())
        .context("failed to initialize agent registry state")?;
    let auth_registry = Arc::new(
        AuthProfileRegistry::open(identity_runtime.store_root.as_path())
            .context("failed to initialize auth profile registry state")?,
    );
    let auth_runtime = Arc::new(gateway::AuthRuntimeState::new(
        Arc::clone(&auth_registry),
        Arc::new(HttpOAuthRefreshAdapter::default()) as Arc<dyn OAuthRefreshAdapter>,
    ));
    let runtime = GatewayRuntimeState::new_with_provider(
        GatewayRuntimeConfigSnapshot {
            grpc_bind_addr: loaded.gateway.grpc_bind_addr.clone(),
            grpc_port: loaded.gateway.grpc_port,
            quic_bind_addr: loaded.gateway.quic_bind_addr.clone(),
            quic_port: loaded.gateway.quic_port,
            quic_enabled: loaded.gateway.quic_enabled,
            orchestrator_runloop_v1_enabled: loaded.orchestrator.runloop_v1_enabled,
            node_rpc_mtls_required,
            admin_auth_required: loaded.admin.require_auth,
            vault_get_approval_required_refs: loaded
                .gateway
                .vault_get_approval_required_refs
                .clone(),
            max_tape_entries_per_response: loaded.gateway.max_tape_entries_per_response,
            max_tape_bytes_per_response: loaded.gateway.max_tape_bytes_per_response,
            channel_router: loaded.channel_router.clone(),
            tool_call: tool_protocol::ToolCallConfig {
                allowed_tools: loaded.tool_call.allowed_tools.clone(),
                max_calls_per_run: loaded.tool_call.max_calls_per_run,
                execution_timeout_ms: loaded.tool_call.execution_timeout_ms,
                process_runner: sandbox_runner::SandboxProcessRunnerPolicy {
                    enabled: loaded.tool_call.process_runner.enabled,
                    tier: loaded.tool_call.process_runner.tier,
                    workspace_root: loaded.tool_call.process_runner.workspace_root.clone(),
                    allowed_executables: loaded
                        .tool_call
                        .process_runner
                        .allowed_executables
                        .clone(),
                    allow_interpreters: loaded.tool_call.process_runner.allow_interpreters,
                    egress_enforcement_mode: loaded
                        .tool_call
                        .process_runner
                        .egress_enforcement_mode,
                    allowed_egress_hosts: loaded
                        .tool_call
                        .process_runner
                        .allowed_egress_hosts
                        .clone(),
                    allowed_dns_suffixes: loaded
                        .tool_call
                        .process_runner
                        .allowed_dns_suffixes
                        .clone(),
                    cpu_time_limit_ms: loaded.tool_call.process_runner.cpu_time_limit_ms,
                    memory_limit_bytes: loaded.tool_call.process_runner.memory_limit_bytes,
                    max_output_bytes: loaded.tool_call.process_runner.max_output_bytes,
                },
                wasm_runtime: wasm_plugin_runner::WasmPluginRunnerPolicy {
                    enabled: loaded.tool_call.wasm_runtime.enabled,
                    allow_inline_modules: loaded.tool_call.wasm_runtime.allow_inline_modules,
                    max_module_size_bytes: loaded.tool_call.wasm_runtime.max_module_size_bytes,
                    fuel_budget: loaded.tool_call.wasm_runtime.fuel_budget,
                    max_memory_bytes: loaded.tool_call.wasm_runtime.max_memory_bytes,
                    max_table_elements: loaded.tool_call.wasm_runtime.max_table_elements,
                    max_instances: loaded.tool_call.wasm_runtime.max_instances,
                    allowed_http_hosts: loaded.tool_call.wasm_runtime.allowed_http_hosts.clone(),
                    allowed_secrets: loaded.tool_call.wasm_runtime.allowed_secrets.clone(),
                    allowed_storage_prefixes: loaded
                        .tool_call
                        .wasm_runtime
                        .allowed_storage_prefixes
                        .clone(),
                    allowed_channels: loaded.tool_call.wasm_runtime.allowed_channels.clone(),
                },
            },
            http_fetch: gateway::HttpFetchRuntimeConfig {
                allow_private_targets: loaded.tool_call.http_fetch.allow_private_targets,
                connect_timeout_ms: loaded.tool_call.http_fetch.connect_timeout_ms,
                request_timeout_ms: loaded.tool_call.http_fetch.request_timeout_ms,
                max_response_bytes: usize::try_from(loaded.tool_call.http_fetch.max_response_bytes)
                    .unwrap_or(usize::MAX),
                allow_redirects: loaded.tool_call.http_fetch.allow_redirects,
                max_redirects: usize::try_from(loaded.tool_call.http_fetch.max_redirects)
                    .unwrap_or(usize::MAX),
                allowed_content_types: loaded.tool_call.http_fetch.allowed_content_types.clone(),
                allowed_request_headers: loaded
                    .tool_call
                    .http_fetch
                    .allowed_request_headers
                    .clone(),
                cache_enabled: loaded.tool_call.http_fetch.cache_enabled,
                cache_ttl_ms: loaded.tool_call.http_fetch.cache_ttl_ms,
                max_cache_entries: usize::try_from(loaded.tool_call.http_fetch.max_cache_entries)
                    .unwrap_or(usize::MAX),
            },
            browser_service: gateway::BrowserServiceRuntimeConfig {
                enabled: loaded.tool_call.browser_service.enabled,
                endpoint: loaded.tool_call.browser_service.endpoint.clone(),
                auth_token: loaded.tool_call.browser_service.auth_token.clone(),
                connect_timeout_ms: loaded.tool_call.browser_service.connect_timeout_ms,
                request_timeout_ms: loaded.tool_call.browser_service.request_timeout_ms,
                max_screenshot_bytes: usize::try_from(
                    loaded.tool_call.browser_service.max_screenshot_bytes,
                )
                .unwrap_or(usize::MAX),
                max_title_bytes: usize::try_from(loaded.tool_call.browser_service.max_title_bytes)
                    .unwrap_or(usize::MAX),
            },
            canvas_host: gateway::CanvasHostRuntimeConfig {
                enabled: loaded.canvas_host.enabled,
                public_base_url: loaded.canvas_host.public_base_url.clone(),
                token_ttl_ms: loaded.canvas_host.token_ttl_ms,
                max_state_bytes: usize::try_from(loaded.canvas_host.max_state_bytes)
                    .unwrap_or(usize::MAX),
                max_bundle_bytes: usize::try_from(loaded.canvas_host.max_bundle_bytes)
                    .unwrap_or(usize::MAX),
                max_assets_per_bundle: usize::try_from(loaded.canvas_host.max_assets_per_bundle)
                    .unwrap_or(usize::MAX),
                max_updates_per_minute: usize::try_from(loaded.canvas_host.max_updates_per_minute)
                    .unwrap_or(usize::MAX),
            },
        },
        GatewayJournalConfigSnapshot {
            db_path: loaded.storage.journal_db_path.clone(),
            hash_chain_enabled: loaded.storage.journal_hash_chain_enabled,
        },
        journal_store,
        identity_runtime.revoked_certificate_count,
        model_provider,
        Arc::clone(&vault),
        agent_registry,
    )
    .context("failed to initialize gateway runtime state")?;
    runtime.configure_memory(MemoryRuntimeConfig {
        max_item_bytes: loaded.memory.max_item_bytes,
        max_item_tokens: loaded.memory.max_item_tokens,
        auto_inject_enabled: loaded.memory.auto_inject.enabled,
        auto_inject_max_items: loaded.memory.auto_inject.max_items,
        default_ttl_ms: loaded.memory.default_ttl_ms,
    });

    if args.journal_migrate_only {
        info!(
            journal_db_path = %loaded.storage.journal_db_path.display(),
            hash_chain_enabled = loaded.storage.journal_hash_chain_enabled,
            "journal migrations applied; exiting due to --journal-migrate-only"
        );
        println!(
            "journal.migration=ok db_path={} hash_chain_enabled={}",
            loaded.storage.journal_db_path.display(),
            loaded.storage.journal_hash_chain_enabled
        );
        return Ok(());
    }

    let build = build_metadata();
    info!(
        service = "palyrad",
        version = build.version,
        git_hash = build.git_hash,
        build_profile = build.build_profile,
        config_source = %loaded.source,
        config_version = loaded.config_version,
        config_migrated_from_version = ?loaded.migrated_from_version,
        admin_bind_addr = %loaded.daemon.bind_addr,
        admin_port = loaded.daemon.port,
        grpc_bind_addr = %loaded.gateway.grpc_bind_addr,
        grpc_port = loaded.gateway.grpc_port,
        quic_bind_addr = %loaded.gateway.quic_bind_addr,
        quic_port = loaded.gateway.quic_port,
        quic_enabled = loaded.gateway.quic_enabled,
        allow_insecure_remote = loaded.gateway.allow_insecure_remote,
        gateway_identity_store_dir = ?loaded.gateway.identity_store_dir.as_ref().map(|path| path.display().to_string()),
        gateway_vault_get_approval_required_refs = ?loaded.gateway.vault_get_approval_required_refs,
        gateway_max_tape_entries_per_response = loaded.gateway.max_tape_entries_per_response,
        gateway_max_tape_bytes_per_response = loaded.gateway.max_tape_bytes_per_response,
        gateway_tls_enabled = loaded.gateway.tls.enabled,
        gateway_tls_cert_path = ?loaded.gateway.tls.cert_path.as_ref().map(|path| path.display().to_string()),
        gateway_tls_key_path = ?loaded.gateway.tls.key_path.as_ref().map(|path| path.display().to_string()),
        gateway_tls_client_ca_path = ?loaded.gateway.tls.client_ca_path.as_ref().map(|path| path.display().to_string()),
        cron_timezone_mode = loaded.cron.timezone.as_str(),
        orchestrator_runloop_v1_enabled = loaded.orchestrator.runloop_v1_enabled,
        memory_max_item_bytes = loaded.memory.max_item_bytes,
        memory_max_item_tokens = loaded.memory.max_item_tokens,
        memory_default_ttl_ms = ?loaded.memory.default_ttl_ms,
        memory_auto_inject_enabled = loaded.memory.auto_inject.enabled,
        memory_auto_inject_max_items = loaded.memory.auto_inject.max_items,
        model_provider_kind = loaded.model_provider.kind.as_str(),
        model_provider_openai_base_url = %loaded.model_provider.openai_base_url,
        model_provider_allow_private_base_url = loaded.model_provider.allow_private_base_url,
        model_provider_openai_model = %loaded.model_provider.openai_model,
        model_provider_api_key_configured = loaded.model_provider.openai_api_key.is_some(),
        model_provider_openai_api_key_vault_ref_configured =
            loaded.model_provider.openai_api_key_vault_ref.is_some(),
        vault_backend = vault.backend_kind().as_str(),
        tool_call_allowed_tools = ?loaded.tool_call.allowed_tools,
        tool_call_max_calls_per_run = loaded.tool_call.max_calls_per_run,
        tool_call_execution_timeout_ms = loaded.tool_call.execution_timeout_ms,
        tool_call_process_runner_enabled = loaded.tool_call.process_runner.enabled,
        tool_call_process_runner_tier = loaded.tool_call.process_runner.tier.as_str(),
        tool_call_process_runner_workspace_root = %loaded.tool_call.process_runner.workspace_root.display(),
        tool_call_process_runner_allowed_executables = ?loaded.tool_call.process_runner.allowed_executables,
        tool_call_process_runner_allow_interpreters = loaded.tool_call.process_runner.allow_interpreters,
        tool_call_process_runner_egress_enforcement_mode =
            loaded.tool_call.process_runner.egress_enforcement_mode.as_str(),
        tool_call_process_runner_allowed_egress_hosts = ?loaded.tool_call.process_runner.allowed_egress_hosts,
        tool_call_process_runner_allowed_dns_suffixes = ?loaded.tool_call.process_runner.allowed_dns_suffixes,
        tool_call_process_runner_cpu_time_limit_ms = loaded.tool_call.process_runner.cpu_time_limit_ms,
        tool_call_process_runner_memory_limit_bytes = loaded.tool_call.process_runner.memory_limit_bytes,
        tool_call_process_runner_max_output_bytes = loaded.tool_call.process_runner.max_output_bytes,
        tool_call_wasm_runtime_enabled = loaded.tool_call.wasm_runtime.enabled,
        tool_call_wasm_runtime_allow_inline_modules =
            loaded.tool_call.wasm_runtime.allow_inline_modules,
        tool_call_wasm_runtime_max_module_size_bytes = loaded.tool_call.wasm_runtime.max_module_size_bytes,
        tool_call_wasm_runtime_fuel_budget = loaded.tool_call.wasm_runtime.fuel_budget,
        tool_call_wasm_runtime_max_memory_bytes = loaded.tool_call.wasm_runtime.max_memory_bytes,
        tool_call_wasm_runtime_max_table_elements = loaded.tool_call.wasm_runtime.max_table_elements,
        tool_call_wasm_runtime_max_instances = loaded.tool_call.wasm_runtime.max_instances,
        tool_call_wasm_runtime_allowed_http_hosts = ?loaded.tool_call.wasm_runtime.allowed_http_hosts,
        tool_call_wasm_runtime_allowed_secrets = ?loaded.tool_call.wasm_runtime.allowed_secrets,
        tool_call_wasm_runtime_allowed_storage_prefixes = ?loaded.tool_call.wasm_runtime.allowed_storage_prefixes,
        tool_call_wasm_runtime_allowed_channels = ?loaded.tool_call.wasm_runtime.allowed_channels,
        tool_call_http_fetch_allow_private_targets = loaded.tool_call.http_fetch.allow_private_targets,
        tool_call_http_fetch_connect_timeout_ms = loaded.tool_call.http_fetch.connect_timeout_ms,
        tool_call_http_fetch_request_timeout_ms = loaded.tool_call.http_fetch.request_timeout_ms,
        tool_call_http_fetch_max_response_bytes = loaded.tool_call.http_fetch.max_response_bytes,
        tool_call_http_fetch_allow_redirects = loaded.tool_call.http_fetch.allow_redirects,
        tool_call_http_fetch_max_redirects = loaded.tool_call.http_fetch.max_redirects,
        tool_call_http_fetch_allowed_content_types = ?loaded.tool_call.http_fetch.allowed_content_types,
        tool_call_http_fetch_allowed_headers = ?loaded.tool_call.http_fetch.allowed_request_headers,
        tool_call_http_fetch_cache_enabled = loaded.tool_call.http_fetch.cache_enabled,
        tool_call_http_fetch_cache_ttl_ms = loaded.tool_call.http_fetch.cache_ttl_ms,
        tool_call_http_fetch_max_cache_entries = loaded.tool_call.http_fetch.max_cache_entries,
        tool_call_browser_service_enabled = loaded.tool_call.browser_service.enabled,
        tool_call_browser_service_endpoint = %loaded.tool_call.browser_service.endpoint,
        tool_call_browser_service_auth_token_configured =
            loaded.tool_call.browser_service.auth_token.is_some(),
        tool_call_browser_service_connect_timeout_ms =
            loaded.tool_call.browser_service.connect_timeout_ms,
        tool_call_browser_service_request_timeout_ms =
            loaded.tool_call.browser_service.request_timeout_ms,
        tool_call_browser_service_max_screenshot_bytes =
            loaded.tool_call.browser_service.max_screenshot_bytes,
        tool_call_browser_service_max_title_bytes = loaded.tool_call.browser_service.max_title_bytes,
        canvas_host_enabled = loaded.canvas_host.enabled,
        canvas_host_public_base_url = %loaded.canvas_host.public_base_url,
        canvas_host_token_ttl_ms = loaded.canvas_host.token_ttl_ms,
        canvas_host_max_state_bytes = loaded.canvas_host.max_state_bytes,
        canvas_host_max_bundle_bytes = loaded.canvas_host.max_bundle_bytes,
        canvas_host_max_assets_per_bundle = loaded.canvas_host.max_assets_per_bundle,
        canvas_host_max_updates_per_minute = loaded.canvas_host.max_updates_per_minute,
        channel_router_enabled = loaded.channel_router.enabled,
        channel_router_max_message_bytes = loaded.channel_router.max_message_bytes,
        channel_router_max_retry_queue_depth_per_channel =
            loaded.channel_router.max_retry_queue_depth_per_channel,
        channel_router_max_retry_attempts = loaded.channel_router.max_retry_attempts,
        channel_router_retry_backoff_ms = loaded.channel_router.retry_backoff_ms,
        channel_router_default_channel_enabled = loaded.channel_router.default_channel_enabled,
        channel_router_default_allow_direct_messages =
            loaded.channel_router.default_allow_direct_messages,
        channel_router_default_isolate_session_by_sender =
            loaded.channel_router.default_isolate_session_by_sender,
        channel_router_default_broadcast_strategy =
            loaded.channel_router.default_broadcast_strategy.as_str(),
        channel_router_default_concurrency_limit =
            loaded.channel_router.default_concurrency_limit,
        channel_router_channels = ?loaded
            .channel_router
            .channels
            .iter()
            .map(|rule| rule.channel.clone())
            .collect::<Vec<_>>(),
        admin_auth_required = loaded.admin.require_auth,
        admin_token_configured = loaded.admin.auth_token.is_some(),
        admin_rate_limit_window_ms = ADMIN_RATE_LIMIT_WINDOW_MS,
        admin_rate_limit_max_requests_per_window = ADMIN_RATE_LIMIT_MAX_REQUESTS_PER_WINDOW,
        grpc_max_decoding_message_size_bytes = GRPC_MAX_DECODING_MESSAGE_SIZE_BYTES,
        grpc_max_encoding_message_size_bytes = GRPC_MAX_ENCODING_MESSAGE_SIZE_BYTES,
        node_rpc_mtls_required,
        journal_db_path = %loaded.storage.journal_db_path.display(),
        journal_hash_chain_enabled = loaded.storage.journal_hash_chain_enabled,
        journal_max_payload_bytes = loaded.storage.max_journal_payload_bytes,
        storage_vault_dir = %loaded.storage.vault_dir.display(),
        identity_store_root = %identity_runtime.store_root.display(),
        revoked_certificate_count = identity_runtime.revoked_certificate_count,
        "gateway startup"
    );

    let admin_address = parse_daemon_bind_socket(&loaded.daemon.bind_addr, loaded.daemon.port)
        .context("invalid admin bind address or port")?;
    let grpc_address =
        parse_daemon_bind_socket(&loaded.gateway.grpc_bind_addr, loaded.gateway.grpc_port)
            .context("invalid gRPC bind address or port")?;
    let quic_address = if loaded.gateway.quic_enabled {
        Some(
            parse_daemon_bind_socket(&loaded.gateway.quic_bind_addr, loaded.gateway.quic_port)
                .context("invalid QUIC bind address or port")?,
        )
    } else {
        None
    };
    enforce_remote_bind_guard(
        admin_address,
        grpc_address,
        quic_address,
        loaded.gateway.allow_insecure_remote,
        loaded.gateway.tls.enabled,
        node_rpc_mtls_required,
        dangerous_remote_bind_acknowledged()?,
    )?;

    let admin_listener = tokio::net::TcpListener::bind(admin_address)
        .await
        .context("failed to bind palyrad admin listener")?;
    let admin_bound =
        admin_listener.local_addr().context("failed to resolve palyrad admin listen address")?;
    let grpc_listener = tokio::net::TcpListener::bind(grpc_address)
        .await
        .context("failed to bind palyrad gRPC listener")?;
    let grpc_bound =
        grpc_listener.local_addr().context("failed to resolve palyrad gRPC listen address")?;
    let node_rpc_port =
        if loaded.gateway.grpc_port == 0 { 0 } else { loaded.gateway.grpc_port.saturating_add(1) };
    let node_rpc_address = parse_daemon_bind_socket(&loaded.gateway.grpc_bind_addr, node_rpc_port)
        .context("invalid node RPC bind address or port")?;
    let node_rpc_listener = tokio::net::TcpListener::bind(node_rpc_address)
        .await
        .context("failed to bind palyrad node RPC listener")?;
    let node_rpc_bound = node_rpc_listener
        .local_addr()
        .context("failed to resolve palyrad node RPC listen address")?;

    info!(listen_addr = %admin_bound, "daemon listening");
    info!(grpc_listen_addr = %grpc_bound, "gateway gRPC listening");
    info!(
        node_rpc_listen_addr = %node_rpc_bound,
        node_rpc_mtls_required,
        "node RPC listener initialized"
    );

    let scheduler_wake = Arc::new(Notify::new());
    let grpc_url = loopback_grpc_url(grpc_bound, loaded.gateway.tls.enabled);
    let _cron_scheduler_task = spawn_scheduler_loop(
        runtime.clone(),
        auth.clone(),
        grpc_url.clone(),
        Arc::clone(&scheduler_wake),
    );

    let started_at = Instant::now();
    let state = AppState {
        started_at,
        runtime: runtime.clone(),
        auth_runtime: Arc::clone(&auth_runtime),
        auth: auth.clone(),
        admin_rate_limit: Arc::new(Mutex::new(HashMap::new())),
        cron_timezone_mode: loaded.cron.timezone,
        grpc_url: grpc_url.clone(),
        scheduler_wake: Arc::clone(&scheduler_wake),
        console_sessions: Arc::new(Mutex::new(HashMap::new())),
    };
    let admin_routes = Router::new()
        .route("/admin/v1/status", get(admin_status_handler))
        .route("/admin/v1/journal/recent", get(admin_journal_recent_handler))
        .route("/admin/v1/policy/explain", get(admin_policy_explain_handler))
        .route("/admin/v1/runs/{run_id}", get(admin_run_status_handler))
        .route("/admin/v1/runs/{run_id}/tape", get(admin_run_tape_handler))
        .route("/admin/v1/runs/{run_id}/cancel", post(admin_run_cancel_handler))
        .route("/admin/v1/skills/{skill_id}/quarantine", post(admin_skill_quarantine_handler))
        .route("/admin/v1/skills/{skill_id}/enable", post(admin_skill_enable_handler))
        .layer(DefaultBodyLimit::max(HTTP_MAX_REQUEST_BODY_BYTES))
        .route_layer(middleware::from_fn_with_state(state.clone(), admin_rate_limit_middleware))
        .route_layer(middleware::from_fn(admin_console_security_headers_middleware));
    let console_routes = Router::new()
        .route("/console/v1/auth/login", post(console_login_handler))
        .route("/console/v1/auth/logout", post(console_logout_handler))
        .route("/console/v1/auth/session", get(console_session_handler))
        .route("/console/v1/approvals", get(console_approvals_list_handler))
        .route("/console/v1/approvals/{approval_id}", get(console_approval_get_handler))
        .route(
            "/console/v1/approvals/{approval_id}/decision",
            post(console_approval_decision_handler),
        )
        .route("/console/v1/cron/jobs", get(console_cron_list_handler))
        .route("/console/v1/cron/jobs", post(console_cron_create_handler))
        .route("/console/v1/cron/jobs/{job_id}/enabled", post(console_cron_set_enabled_handler))
        .route("/console/v1/cron/jobs/{job_id}/run-now", post(console_cron_run_now_handler))
        .route("/console/v1/cron/jobs/{job_id}/runs", get(console_cron_runs_handler))
        .route("/console/v1/memory/search", get(console_memory_search_handler))
        .route("/console/v1/memory/purge", post(console_memory_purge_handler))
        .route("/console/v1/skills", get(console_skills_list_handler))
        .route("/console/v1/skills/install", post(console_skills_install_handler))
        .route("/console/v1/skills/{skill_id}/verify", post(console_skills_verify_handler))
        .route("/console/v1/skills/{skill_id}/audit", post(console_skills_audit_handler))
        .route("/console/v1/skills/{skill_id}/quarantine", post(console_skill_quarantine_handler))
        .route("/console/v1/skills/{skill_id}/enable", post(console_skill_enable_handler))
        .route("/console/v1/audit/events", get(console_audit_events_handler))
        .layer(DefaultBodyLimit::max(HTTP_MAX_REQUEST_BODY_BYTES))
        .route_layer(middleware::from_fn_with_state(state.clone(), admin_rate_limit_middleware))
        .route_layer(middleware::from_fn(admin_console_security_headers_middleware));
    let canvas_routes = Router::new()
        .route("/canvas/v1/frame/{canvas_id}", get(canvas_frame_handler))
        .route("/canvas/v1/runtime.js", get(canvas_runtime_js_handler))
        .route("/canvas/v1/runtime.css", get(canvas_runtime_css_handler))
        .route("/canvas/v1/bundle/{canvas_id}/{*asset_path}", get(canvas_bundle_asset_handler))
        .route("/canvas/v1/state/{canvas_id}", get(canvas_state_handler))
        .route_layer(middleware::from_fn(canvas_security_headers_middleware));
    let app = Router::new()
        .route("/healthz", get(health_handler))
        .merge(canvas_routes)
        .merge(admin_routes)
        .merge(console_routes)
        .with_state(state);

    let gateway_service = gateway::GatewayServiceImpl::new(runtime.clone(), auth.clone());
    let grpc_gateway_server =
        gateway::proto::palyra::gateway::v1::gateway_service_server::GatewayServiceServer::new(
            gateway_service,
        )
        .max_decoding_message_size(GRPC_MAX_DECODING_MESSAGE_SIZE_BYTES)
        .max_encoding_message_size(GRPC_MAX_ENCODING_MESSAGE_SIZE_BYTES);
    let cron_service = gateway::CronServiceImpl::new(
        runtime.clone(),
        auth.clone(),
        grpc_url,
        Arc::clone(&scheduler_wake),
        loaded.cron.timezone,
    );
    let grpc_cron_server =
        gateway::proto::palyra::cron::v1::cron_service_server::CronServiceServer::new(cron_service)
            .max_decoding_message_size(GRPC_MAX_DECODING_MESSAGE_SIZE_BYTES)
            .max_encoding_message_size(GRPC_MAX_ENCODING_MESSAGE_SIZE_BYTES);
    let approvals_service = gateway::ApprovalsServiceImpl::new(runtime.clone(), auth.clone());
    let grpc_approvals_server =
        gateway::proto::palyra::gateway::v1::approvals_service_server::ApprovalsServiceServer::new(
            approvals_service,
        )
        .max_decoding_message_size(GRPC_MAX_DECODING_MESSAGE_SIZE_BYTES)
        .max_encoding_message_size(GRPC_MAX_ENCODING_MESSAGE_SIZE_BYTES);
    let memory_service = gateway::MemoryServiceImpl::new(runtime.clone(), auth.clone());
    let grpc_memory_server =
        gateway::proto::palyra::memory::v1::memory_service_server::MemoryServiceServer::new(
            memory_service,
        )
        .max_decoding_message_size(GRPC_MAX_DECODING_MESSAGE_SIZE_BYTES)
        .max_encoding_message_size(GRPC_MAX_ENCODING_MESSAGE_SIZE_BYTES);
    let vault_service = gateway::VaultServiceImpl::new(runtime.clone(), auth.clone());
    let grpc_vault_server =
        gateway::proto::palyra::gateway::v1::vault_service_server::VaultServiceServer::new(
            vault_service,
        )
        .max_decoding_message_size(GRPC_MAX_DECODING_MESSAGE_SIZE_BYTES)
        .max_encoding_message_size(GRPC_MAX_ENCODING_MESSAGE_SIZE_BYTES);
    let auth_service =
        gateway::AuthServiceImpl::new(runtime.clone(), auth.clone(), Arc::clone(&auth_runtime));
    let grpc_auth_server =
        gateway::proto::palyra::auth::v1::auth_service_server::AuthServiceServer::new(auth_service)
            .max_decoding_message_size(GRPC_MAX_DECODING_MESSAGE_SIZE_BYTES)
            .max_encoding_message_size(GRPC_MAX_ENCODING_MESSAGE_SIZE_BYTES);
    let canvas_service = gateway::CanvasServiceImpl::new(runtime.clone(), auth.clone());
    let grpc_canvas_server =
        gateway::proto::palyra::gateway::v1::canvas_service_server::CanvasServiceServer::new(
            canvas_service,
        )
        .max_decoding_message_size(GRPC_MAX_DECODING_MESSAGE_SIZE_BYTES)
        .max_encoding_message_size(GRPC_MAX_ENCODING_MESSAGE_SIZE_BYTES);
    let node_rpc_service = node_rpc::NodeRpcServiceImpl::new(
        identity_runtime.revoked_certificate_fingerprints.clone(),
        node_rpc_mtls_required,
    );
    let node_rpc_server =
        gateway::proto::palyra::node::v1::node_service_server::NodeServiceServer::new(
            node_rpc_service,
        )
        .max_decoding_message_size(GRPC_MAX_DECODING_MESSAGE_SIZE_BYTES)
        .max_encoding_message_size(GRPC_MAX_ENCODING_MESSAGE_SIZE_BYTES);
    let mut grpc_server_builder = Server::builder();
    if loaded.gateway.tls.enabled {
        grpc_server_builder = grpc_server_builder
            .tls_config(build_gateway_tls_config(&loaded.gateway.tls)?)
            .context("failed to apply gRPC TLS configuration")?;
    }
    let mut node_rpc_server_builder = Server::builder();
    node_rpc_server_builder = node_rpc_server_builder
        .tls_config(build_node_rpc_tls_config(&identity_runtime, node_rpc_mtls_required))
        .context("failed to apply node RPC TLS configuration")?;

    let quic_endpoint = if let Some(quic_address) = quic_address {
        let endpoint = quic_runtime::bind_endpoint(
            quic_address,
            &quic_runtime::QuicRuntimeTlsMaterial {
                ca_cert_pem: identity_runtime.gateway_ca_certificate_pem.clone(),
                cert_pem: identity_runtime.node_server_certificate.certificate_pem.clone(),
                key_pem: identity_runtime.node_server_certificate.private_key_pem.clone(),
                require_client_auth: node_rpc_mtls_required,
            },
            &QuicTransportLimits::default(),
        )
        .context("failed to bind palyrad QUIC listener")?;
        let quic_bound =
            endpoint.local_addr().context("failed to resolve palyrad QUIC listen address")?;
        info!(
            quic_listen_addr = %quic_bound,
            node_rpc_mtls_required,
            "gateway QUIC listener initialized"
        );
        Some(endpoint)
    } else {
        None
    };

    let admin_server = async move {
        axum::serve(admin_listener, app.into_make_service_with_connect_info::<SocketAddr>())
            .with_graceful_shutdown(shutdown_signal())
            .await
            .context("palyrad admin server failed")
    };
    let grpc_server = async move {
        grpc_server_builder
            .add_service(grpc_gateway_server)
            .add_service(grpc_cron_server)
            .add_service(grpc_approvals_server)
            .add_service(grpc_memory_server)
            .add_service(grpc_vault_server)
            .add_service(grpc_auth_server)
            .add_service(grpc_canvas_server)
            .serve_with_incoming_shutdown(TcpListenerStream::new(grpc_listener), shutdown_signal())
            .await
            .context("palyrad gRPC server failed")
    };
    let node_rpc_server = async move {
        node_rpc_server_builder
            .add_service(node_rpc_server)
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(node_rpc_listener),
                shutdown_signal(),
            )
            .await
            .context("palyrad node RPC server failed")
    };
    if let Some(quic_endpoint) = quic_endpoint {
        tokio::try_join!(admin_server, grpc_server, node_rpc_server, async move {
            quic_runtime::serve(quic_endpoint, node_rpc_mtls_required)
                .await
                .context("palyrad QUIC server failed")
        },)?;
    } else {
        tokio::try_join!(admin_server, grpc_server, node_rpc_server)?;
    }

    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().json().with_env_filter(filter).init();
}

fn loopback_grpc_url(socket: SocketAddr, tls_enabled: bool) -> String {
    let normalized = match socket {
        SocketAddr::V4(v4) if v4.ip().is_unspecified() => {
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), v4.port())
        }
        SocketAddr::V6(v6) if v6.ip().is_unspecified() => {
            SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), v6.port())
        }
        other => other,
    };
    let scheme = if tls_enabled { "https" } else { "http" };
    format!("{scheme}://{normalized}")
}

async fn health_handler(State(state): State<AppState>) -> impl IntoResponse {
    Json::<HealthResponse>(health_response("palyrad", state.started_at))
}

async fn canvas_frame_handler(
    State(state): State<AppState>,
    Path(canvas_id): Path<String>,
    Query(query): Query<CanvasTokenQuery>,
) -> Result<Response, Response> {
    let frame = state
        .runtime
        .canvas_frame_document(canvas_id.as_str(), query.token.as_str())
        .map_err(runtime_status_response)?;
    let mut response = frame.html.into_response();
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("text/html; charset=utf-8"));
    apply_canvas_security_headers(response.headers_mut(), frame.csp.as_str())?;
    Ok(response)
}

async fn canvas_runtime_js_handler(
    State(state): State<AppState>,
    Query(query): Query<CanvasRuntimeQuery>,
) -> Result<Response, Response> {
    let asset = state
        .runtime
        .canvas_runtime_script(query.canvas_id.as_str(), query.token.as_str())
        .map_err(runtime_status_response)?;
    canvas_asset_response(asset)
}

async fn canvas_runtime_css_handler(
    State(state): State<AppState>,
    Query(query): Query<CanvasRuntimeQuery>,
) -> Result<Response, Response> {
    let asset = state
        .runtime
        .canvas_runtime_stylesheet(query.canvas_id.as_str(), query.token.as_str())
        .map_err(runtime_status_response)?;
    canvas_asset_response(asset)
}

async fn canvas_bundle_asset_handler(
    State(state): State<AppState>,
    Path((canvas_id, asset_path)): Path<(String, String)>,
    Query(query): Query<CanvasTokenQuery>,
) -> Result<Response, Response> {
    let normalized_asset_path = asset_path.trim_start_matches('/').to_owned();
    let asset = state
        .runtime
        .canvas_bundle_asset(
            canvas_id.as_str(),
            normalized_asset_path.as_str(),
            query.token.as_str(),
        )
        .map_err(runtime_status_response)?;
    canvas_asset_response(asset)
}

async fn canvas_state_handler(
    State(state): State<AppState>,
    Path(canvas_id): Path<String>,
    Query(query): Query<CanvasStateQuery>,
) -> Result<Response, Response> {
    let payload = state
        .runtime
        .canvas_state(canvas_id.as_str(), query.token.as_str(), query.after_version)
        .map_err(runtime_status_response)?;
    if let Some(payload) = payload {
        let mut response = Json(payload).into_response();
        response.headers_mut().insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
        response.headers_mut().insert(
            HeaderName::from_static("x-content-type-options"),
            HeaderValue::from_static("nosniff"),
        );
        Ok(response)
    } else {
        Ok(StatusCode::NO_CONTENT.into_response())
    }
}

#[allow(clippy::result_large_err)]
fn canvas_asset_response(asset: CanvasAssetResponse) -> Result<Response, Response> {
    let mut response = asset.body.into_response();
    let content_type = HeaderValue::from_str(asset.content_type.as_str()).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to encode canvas content-type header: {error}"
        )))
    })?;
    response.headers_mut().insert(CONTENT_TYPE, content_type);
    apply_canvas_security_headers(response.headers_mut(), asset.csp.as_str())?;
    Ok(response)
}

#[allow(clippy::result_large_err)]
fn apply_canvas_security_headers(headers: &mut HeaderMap, csp: &str) -> Result<(), Response> {
    let csp_header = HeaderValue::from_str(csp).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to encode canvas csp header: {error}"
        )))
    })?;
    headers.insert(CONTENT_SECURITY_POLICY, csp_header);
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        HeaderName::from_static("x-content-type-options"),
        HeaderValue::from_static("nosniff"),
    );
    Ok(())
}

async fn admin_console_security_headers_middleware(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        HeaderName::from_static("x-content-type-options"),
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(HeaderName::from_static("x-frame-options"), HeaderValue::from_static("DENY"));
    headers.insert(
        HeaderName::from_static("referrer-policy"),
        HeaderValue::from_static("no-referrer"),
    );
    response
}

async fn canvas_security_headers_middleware(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        HeaderName::from_static("x-content-type-options"),
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        HeaderName::from_static("referrer-policy"),
        HeaderValue::from_static("no-referrer"),
    );
    response
}

fn consume_admin_rate_limit(state: &AppState, remote_addr: SocketAddr) -> bool {
    consume_admin_rate_limit_with_now(&state.admin_rate_limit, remote_addr.ip(), Instant::now())
}

fn consume_admin_rate_limit_with_now(
    buckets: &Mutex<HashMap<IpAddr, AdminRateLimitEntry>>,
    remote_ip: IpAddr,
    now: Instant,
) -> bool {
    let mut buckets = match buckets.lock() {
        Ok(guard) => guard,
        Err(_) => return false,
    };
    if !buckets.contains_key(&remote_ip) && buckets.len() >= ADMIN_RATE_LIMIT_MAX_IP_BUCKETS {
        buckets.retain(|_, entry| {
            now.duration_since(entry.window_started_at).as_millis() as u64
                <= ADMIN_RATE_LIMIT_WINDOW_MS
        });
        if buckets.len() >= ADMIN_RATE_LIMIT_MAX_IP_BUCKETS {
            let evicted_ip =
                buckets.iter().min_by_key(|(_, entry)| entry.window_started_at).map(|(ip, _)| *ip);
            let Some(evicted_ip) = evicted_ip else {
                return false;
            };
            buckets.remove(&evicted_ip);
        }
    }
    let entry = buckets
        .entry(remote_ip)
        .or_insert(AdminRateLimitEntry { window_started_at: now, requests_in_window: 0 });
    if now.duration_since(entry.window_started_at).as_millis() as u64 > ADMIN_RATE_LIMIT_WINDOW_MS {
        entry.window_started_at = now;
        entry.requests_in_window = 0;
    }
    if entry.requests_in_window >= ADMIN_RATE_LIMIT_MAX_REQUESTS_PER_WINDOW {
        return false;
    }
    entry.requests_in_window = entry.requests_in_window.saturating_add(1);
    true
}

async fn admin_rate_limit_middleware(
    State(state): State<AppState>,
    ConnectInfo(remote_addr): ConnectInfo<SocketAddr>,
    request: Request,
    next: Next,
) -> Response {
    if !consume_admin_rate_limit(&state, remote_addr) {
        state.runtime.record_denied();
        return runtime_status_response(tonic::Status::resource_exhausted(format!(
            "admin API rate limit exceeded for {}",
            remote_addr.ip()
        )));
    }
    next.run(request).await
}

async fn admin_status_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let snapshot = state
        .runtime
        .status_snapshot_async(context, state.auth.clone())
        .await
        .map_err(runtime_status_response)?;
    let auth_snapshot = state
        .auth_runtime
        .admin_status_snapshot(Arc::clone(&state.runtime))
        .await
        .map_err(runtime_status_response)?;
    let mut payload = serde_json::to_value(snapshot).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to serialize admin status snapshot: {error}"
        )))
    })?;
    let auth_payload = serde_json::to_value(auth_snapshot).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to serialize auth status snapshot: {error}"
        )))
    })?;
    if let Value::Object(ref mut map) = payload {
        map.insert("auth".to_owned(), auth_payload);
    }
    Ok(Json(payload))
}

async fn admin_journal_recent_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<JournalRecentQuery>,
) -> Result<Json<gateway::JournalRecentSnapshot>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let limit = query.limit.unwrap_or(20);
    let snapshot =
        state.runtime.recent_journal_snapshot(limit).await.map_err(runtime_status_response)?;
    Ok(Json(snapshot))
}

async fn admin_policy_explain_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<PolicyExplainQuery>,
) -> Result<Json<PolicyExplainResponse>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();

    let request = PolicyRequest {
        principal: query.principal,
        action: query.action,
        resource: query.resource,
    };
    let evaluation =
        evaluate_with_config(&request, &PolicyEvaluationConfig::default()).map_err(|error| {
            runtime_status_response(tonic::Status::internal(format!(
                "failed to evaluate policy with Cedar engine: {error}"
            )))
        })?;
    let (decision, approval_required, reason) = match evaluation.decision {
        PolicyDecision::Allow => ("allow".to_owned(), false, evaluation.explanation.reason),
        PolicyDecision::DenyByDefault { reason } => ("deny_by_default".to_owned(), true, reason),
    };

    Ok(Json(PolicyExplainResponse {
        principal: request.principal,
        action: request.action,
        resource: request.resource,
        decision,
        approval_required,
        reason,
        matched_policies: evaluation.explanation.matched_policy_ids,
    }))
}

async fn admin_run_status_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
) -> Result<Json<OrchestratorRunStatusSnapshot>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    validate_canonical_id(run_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument("run_id must be a canonical ULID"))
    })?;
    state.runtime.record_admin_status_request();
    let snapshot = state
        .runtime
        .orchestrator_run_status_snapshot(run_id.clone())
        .await
        .map_err(runtime_status_response)?;
    let Some(snapshot) = snapshot else {
        return Err(runtime_status_response(tonic::Status::not_found(format!(
            "orchestrator run not found: {run_id}"
        ))));
    };
    Ok(Json(snapshot))
}

async fn admin_run_tape_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
    Query(query): Query<RunTapeQuery>,
) -> Result<Json<gateway::RunTapeSnapshot>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    validate_canonical_id(run_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument("run_id must be a canonical ULID"))
    })?;
    state.runtime.record_admin_status_request();
    let snapshot = state
        .runtime
        .orchestrator_tape_snapshot(run_id, query.after_seq, query.limit)
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(snapshot))
}

async fn admin_run_cancel_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
    payload: Option<Json<RunCancelRequest>>,
) -> Result<Json<gateway::RunCancelSnapshot>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    validate_canonical_id(run_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument("run_id must be a canonical ULID"))
    })?;
    state.runtime.record_admin_status_request();
    let reason = payload
        .and_then(|body| body.0.reason)
        .and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_owned())
            }
        })
        .unwrap_or_else(|| "admin_cancel_requested".to_owned());
    let response = state
        .runtime
        .request_orchestrator_cancel(OrchestratorCancelRequest { run_id, reason })
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(response))
}

async fn admin_skill_quarantine_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(skill_id): Path<String>,
    Json(payload): Json<SkillStatusRequest>,
) -> Result<Json<SkillStatusResponse>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let skill_id = normalize_non_empty_field(skill_id, "skill_id")?;
    let version = normalize_non_empty_field(payload.version, "version")?;
    let record = state
        .runtime
        .upsert_skill_status(SkillStatusUpsertRequest {
            skill_id,
            version,
            status: SkillExecutionStatus::Quarantined,
            reason: payload.reason.and_then(|value| {
                let trimmed = value.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_owned())
                }
            }),
            detected_at_ms: unix_ms_now().map_err(|error| {
                runtime_status_response(tonic::Status::internal(format!(
                    "failed to read system clock: {error}"
                )))
            })?,
            operator_principal: context.principal.clone(),
        })
        .await
        .map_err(runtime_status_response)?;
    state
        .runtime
        .record_skill_status_event(&context, "skill.quarantined", &record)
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(skill_status_response(record)))
}

async fn admin_skill_enable_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(skill_id): Path<String>,
    Json(payload): Json<SkillStatusRequest>,
) -> Result<Json<SkillStatusResponse>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    if !payload.override_enabled.unwrap_or(false) {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "enable requires explicit override=true acknowledgment",
        )));
    }
    let skill_id = normalize_non_empty_field(skill_id, "skill_id")?;
    let version = normalize_non_empty_field(payload.version, "version")?;
    let record = state
        .runtime
        .upsert_skill_status(SkillStatusUpsertRequest {
            skill_id,
            version,
            status: SkillExecutionStatus::Active,
            reason: payload.reason.and_then(|value| {
                let trimmed = value.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_owned())
                }
            }),
            detected_at_ms: unix_ms_now().map_err(|error| {
                runtime_status_response(tonic::Status::internal(format!(
                    "failed to read system clock: {error}"
                )))
            })?,
            operator_principal: context.principal.clone(),
        })
        .await
        .map_err(runtime_status_response)?;
    state
        .runtime
        .record_skill_status_event(&context, "skill.enabled", &record)
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(skill_status_response(record)))
}

fn skill_status_response(record: SkillStatusRecord) -> SkillStatusResponse {
    SkillStatusResponse {
        skill_id: record.skill_id,
        version: record.version,
        status: record.status.as_str().to_owned(),
        reason: record.reason,
        detected_at_ms: record.detected_at_ms,
        operator_principal: record.operator_principal,
    }
}

#[derive(Debug, Deserialize)]
struct ConsoleCronListQuery {
    after_job_id: Option<String>,
    limit: Option<usize>,
    enabled: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ConsoleCronRunsQuery {
    after_run_id: Option<String>,
    limit: Option<usize>,
}

async fn console_login_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleLoginRequest>,
) -> Result<(HeaderMap, Json<ConsoleSessionResponse>), Response> {
    let principal = payload.principal.trim();
    let device_id = payload.device_id.trim();
    if principal.is_empty() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "principal cannot be empty",
        )));
    }
    if device_id.is_empty() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "device_id cannot be empty",
        )));
    }

    let mut auth_headers = HeaderMap::new();
    if let Some(token) = payload.admin_token.as_deref() {
        let token = token.trim();
        if token.is_empty() {
            return Err(runtime_status_response(tonic::Status::invalid_argument(
                "admin_token cannot be empty when provided",
            )));
        }
        let authorization =
            HeaderValue::from_str(format!("Bearer {token}").as_str()).map_err(|_| {
                runtime_status_response(tonic::Status::invalid_argument(
                    "admin_token contains unsupported characters",
                ))
            })?;
        auth_headers.insert(AUTHORIZATION, authorization);
    }
    let principal_header = HeaderValue::from_str(principal).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "principal contains unsupported characters",
        ))
    })?;
    let device_header = HeaderValue::from_str(device_id).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "device_id contains unsupported characters",
        ))
    })?;
    auth_headers.insert(gateway::HEADER_PRINCIPAL, principal_header);
    auth_headers.insert(gateway::HEADER_DEVICE_ID, device_header);
    if let Some(channel) = payload.channel.as_deref() {
        let channel = channel.trim();
        if !channel.is_empty() {
            let channel_header = HeaderValue::from_str(channel).map_err(|_| {
                runtime_status_response(tonic::Status::invalid_argument(
                    "channel contains unsupported characters",
                ))
            })?;
            auth_headers.insert(gateway::HEADER_CHANNEL, channel_header);
        }
    }

    authorize_headers(&auth_headers, &state.auth).map_err(auth_error_response)?;
    let context = request_context_from_headers(&auth_headers).map_err(auth_error_response)?;
    if !context.principal.starts_with("admin:") {
        return Err(runtime_status_response(tonic::Status::permission_denied(
            "web console login requires an admin:* principal",
        )));
    }

    let now = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock: {error}"
        )))
    })?;
    let expires_at_unix_ms =
        now.saturating_add(i64::try_from(CONSOLE_SESSION_TTL_SECONDS).unwrap_or(i64::MAX) * 1_000);
    let session = ConsoleSession {
        session_id: Ulid::new().to_string(),
        csrf_token: Ulid::new().to_string(),
        context,
        issued_at_unix_ms: now,
        expires_at_unix_ms,
    };

    {
        let mut sessions = lock_console_sessions(&state.console_sessions);
        sessions.retain(|_, existing| existing.expires_at_unix_ms > now);
        if sessions.len() >= CONSOLE_MAX_ACTIVE_SESSIONS {
            let mut oldest: Option<(String, i64)> = None;
            for (session_id, existing) in sessions.iter() {
                if oldest
                    .as_ref()
                    .is_none_or(|(_, issued_at)| existing.issued_at_unix_ms < *issued_at)
                {
                    oldest = Some((session_id.clone(), existing.issued_at_unix_ms));
                }
            }
            if let Some((session_id, _)) = oldest {
                sessions.remove(session_id.as_str());
            }
        }
        sessions.insert(session.session_id.clone(), session.clone());
    }

    let secure_cookie = request_uses_tls(&headers);
    let mut response_headers = HeaderMap::new();
    response_headers.insert(
        SET_COOKIE,
        build_console_session_cookie(session.session_id.as_str(), secure_cookie)?,
    );
    Ok((response_headers, Json(build_console_session_response(&session))))
}

async fn console_logout_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<(HeaderMap, Json<Value>), Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    {
        let mut sessions = lock_console_sessions(&state.console_sessions);
        sessions.remove(session.session_id.as_str());
    }
    let mut response_headers = HeaderMap::new();
    response_headers.insert(SET_COOKIE, clear_console_session_cookie(request_uses_tls(&headers))?);
    Ok((response_headers, Json(json!({ "signed_out": true }))))
}

async fn console_session_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<ConsoleSessionResponse>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    Ok(Json(build_console_session_response(&session)))
}

fn build_console_session_response(session: &ConsoleSession) -> ConsoleSessionResponse {
    ConsoleSessionResponse {
        principal: session.context.principal.clone(),
        device_id: session.context.device_id.clone(),
        channel: session.context.channel.clone(),
        csrf_token: session.csrf_token.clone(),
        issued_at_unix_ms: session.issued_at_unix_ms,
        expires_at_unix_ms: session.expires_at_unix_ms,
    }
}

#[allow(clippy::result_large_err)]
fn authorize_console_session(
    state: &AppState,
    headers: &HeaderMap,
    require_csrf: bool,
) -> Result<ConsoleSession, Response> {
    let session_id = cookie_value(headers, CONSOLE_SESSION_COOKIE_NAME).ok_or_else(|| {
        runtime_status_response(tonic::Status::permission_denied(
            "console session cookie is missing",
        ))
    })?;
    let now = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock: {error}"
        )))
    })?;
    let mut sessions = lock_console_sessions(&state.console_sessions);
    sessions.retain(|_, session| session.expires_at_unix_ms > now);
    let session = sessions.get_mut(session_id.as_str()).ok_or_else(|| {
        runtime_status_response(tonic::Status::permission_denied(
            "console session is missing or expired",
        ))
    })?;
    if require_csrf {
        let csrf_candidate = headers
            .get(CONSOLE_CSRF_HEADER_NAME)
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                runtime_status_response(tonic::Status::permission_denied(
                    "missing CSRF token for console request",
                ))
            })?;
        if csrf_candidate != session.csrf_token {
            return Err(runtime_status_response(tonic::Status::permission_denied(
                "CSRF token is invalid",
            )));
        }
    }
    session.expires_at_unix_ms =
        now.saturating_add(i64::try_from(CONSOLE_SESSION_TTL_SECONDS).unwrap_or(i64::MAX) * 1_000);
    Ok(session.clone())
}

fn lock_console_sessions<'a>(
    sessions: &'a Arc<Mutex<HashMap<String, ConsoleSession>>>,
) -> std::sync::MutexGuard<'a, HashMap<String, ConsoleSession>> {
    match sessions.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            tracing::warn!("console session map lock poisoned; recovering");
            poisoned.into_inner()
        }
    }
}

fn request_uses_tls(headers: &HeaderMap) -> bool {
    headers
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
        .map(|value| value.trim().eq_ignore_ascii_case("https"))
        .unwrap_or(false)
}

#[allow(clippy::result_large_err)]
fn build_console_session_cookie(session_id: &str, secure: bool) -> Result<HeaderValue, Response> {
    let mut cookie = format!(
        "{CONSOLE_SESSION_COOKIE_NAME}={session_id}; Max-Age={CONSOLE_SESSION_TTL_SECONDS}; Path=/; HttpOnly; SameSite=Strict"
    );
    if secure {
        cookie.push_str("; Secure");
    }
    HeaderValue::from_str(cookie.as_str()).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to encode Set-Cookie header: {error}"
        )))
    })
}

#[allow(clippy::result_large_err)]
fn clear_console_session_cookie(secure: bool) -> Result<HeaderValue, Response> {
    let mut cookie = format!(
        "{CONSOLE_SESSION_COOKIE_NAME}=deleted; Max-Age=0; Path=/; HttpOnly; SameSite=Strict"
    );
    if secure {
        cookie.push_str("; Secure");
    }
    HeaderValue::from_str(cookie.as_str()).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to encode Set-Cookie header: {error}"
        )))
    })
}

fn cookie_value(headers: &HeaderMap, name: &str) -> Option<String> {
    let raw = headers.get(COOKIE)?.to_str().ok()?;
    for part in raw.split(';') {
        let trimmed = part.trim();
        let mut pair = trimmed.splitn(2, '=');
        let key = pair.next()?.trim();
        let value = pair.next().unwrap_or("").trim();
        if key == name && !value.is_empty() {
            return Some(value.to_owned());
        }
    }
    None
}

async fn console_approvals_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleApprovalsQuery>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let decision = parse_console_approval_decision(query.decision.as_deref())?;
    let subject_type = parse_console_approval_subject_type(query.subject_type.as_deref())?;
    let (approvals, next_after_approval_id) = state
        .runtime
        .list_approval_records(
            query.after_approval_id,
            query.limit,
            query.since_unix_ms,
            query.until_unix_ms,
            query.subject_id,
            query.principal,
            decision,
            subject_type,
        )
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "approvals": approvals,
        "next_after_approval_id": next_after_approval_id,
    })))
}

async fn console_approval_get_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(approval_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    validate_canonical_id(approval_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "approval_id must be a canonical ULID",
        ))
    })?;
    let record = state
        .runtime
        .approval_record(approval_id.clone())
        .await
        .map_err(runtime_status_response)?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found(format!(
                "approval record not found: {approval_id}"
            )))
        })?;
    Ok(Json(json!({ "approval": record })))
}

async fn console_approval_decision_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(approval_id): Path<String>,
    Json(payload): Json<ConsoleApprovalDecisionRequest>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, true)?;
    validate_canonical_id(approval_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "approval_id must be a canonical ULID",
        ))
    })?;
    let decision_scope = parse_console_decision_scope(payload.decision_scope.as_deref())?;
    let reason = payload
        .reason
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| {
            if payload.approved {
                "approved_by_console".to_owned()
            } else {
                "denied_by_console".to_owned()
            }
        });
    let resolved = state
        .runtime
        .resolve_approval_record(journal::ApprovalResolveRequest {
            approval_id,
            decision: if payload.approved {
                ApprovalDecision::Allow
            } else {
                ApprovalDecision::Deny
            },
            decision_scope,
            decision_reason: reason,
            decision_scope_ttl_ms: payload.decision_scope_ttl_ms,
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({ "approval": resolved })))
}

#[allow(clippy::result_large_err)]
fn parse_console_approval_decision(
    value: Option<&str>,
) -> Result<Option<ApprovalDecision>, Response> {
    let Some(raw) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    match raw.to_ascii_lowercase().as_str() {
        "allow" => Ok(Some(ApprovalDecision::Allow)),
        "deny" => Ok(Some(ApprovalDecision::Deny)),
        "timeout" => Ok(Some(ApprovalDecision::Timeout)),
        "error" => Ok(Some(ApprovalDecision::Error)),
        _ => Err(runtime_status_response(tonic::Status::invalid_argument(
            "decision must be one of allow|deny|timeout|error",
        ))),
    }
}

#[allow(clippy::result_large_err)]
fn parse_console_approval_subject_type(
    value: Option<&str>,
) -> Result<Option<ApprovalSubjectType>, Response> {
    let Some(raw) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    match raw.to_ascii_lowercase().as_str() {
        "tool" => Ok(Some(ApprovalSubjectType::Tool)),
        "channel_send" => Ok(Some(ApprovalSubjectType::ChannelSend)),
        "secret_access" => Ok(Some(ApprovalSubjectType::SecretAccess)),
        "browser_action" => Ok(Some(ApprovalSubjectType::BrowserAction)),
        "node_capability" => Ok(Some(ApprovalSubjectType::NodeCapability)),
        _ => Err(runtime_status_response(tonic::Status::invalid_argument(
            "subject_type must be one of tool|channel_send|secret_access|browser_action|node_capability",
        ))),
    }
}

#[allow(clippy::result_large_err)]
fn parse_console_decision_scope(value: Option<&str>) -> Result<ApprovalDecisionScope, Response> {
    let Some(raw) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(ApprovalDecisionScope::Once);
    };
    match raw.to_ascii_lowercase().as_str() {
        "once" => Ok(ApprovalDecisionScope::Once),
        "session" => Ok(ApprovalDecisionScope::Session),
        "timeboxed" => Ok(ApprovalDecisionScope::Timeboxed),
        _ => Err(runtime_status_response(tonic::Status::invalid_argument(
            "decision_scope must be one of once|session|timeboxed",
        ))),
    }
}

async fn console_cron_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleCronListQuery>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let (jobs, next_after_job_id) = state
        .runtime
        .list_cron_jobs(
            query.after_job_id,
            query.limit,
            query.enabled,
            Some(session.context.principal),
            session.context.channel,
        )
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "jobs": jobs,
        "next_after_job_id": next_after_job_id,
    })))
}

async fn console_cron_create_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleCronCreateRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let name = payload.name.trim();
    if name.is_empty() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "name cannot be empty",
        )));
    }
    let prompt = payload.prompt.trim();
    if prompt.is_empty() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "prompt cannot be empty",
        )));
    }
    let owner_principal = match payload.owner_principal.as_deref().map(str::trim) {
        Some("") | None => session.context.principal.clone(),
        Some(owner_principal) if owner_principal == session.context.principal => {
            owner_principal.to_owned()
        }
        Some(_) => {
            return Err(runtime_status_response(tonic::Status::permission_denied(
                "owner_principal must match authenticated session principal",
            )))
        }
    };
    let channel = payload
        .channel
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| session.context.channel.clone())
        .unwrap_or_default();
    let session_key = payload.session_key.clone().and_then(trim_to_option).unwrap_or_default();
    let session_label = payload.session_label.clone().and_then(trim_to_option).unwrap_or_default();
    let schedule = build_console_schedule(payload.schedule_type.as_str(), &payload)?;
    let mut request = TonicRequest::new(cron_v1::CreateJobRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        name: name.to_owned(),
        prompt: prompt.to_owned(),
        owner_principal,
        channel,
        session_key,
        session_label,
        schedule: Some(schedule),
        enabled: payload.enabled.unwrap_or(true),
        concurrency_policy: cron_v1::ConcurrencyPolicy::Forbid as i32,
        retry_policy: Some(cron_v1::RetryPolicy { max_attempts: 1, backoff_ms: 1_000 }),
        misfire_policy: cron_v1::MisfirePolicy::Skip as i32,
        jitter_ms: payload.jitter_ms.unwrap_or(0),
    });
    apply_console_rpc_context(&state, &session, request.metadata_mut())?;
    let service = build_console_cron_service(&state);
    let response =
        <gateway::CronServiceImpl as cron_v1::cron_service_server::CronService>::create_job(
            &service, request,
        )
        .await
        .map_err(runtime_status_response)?;
    let job_id =
        response.into_inner().job.and_then(|job| job.job_id).map(|value| value.ulid).ok_or_else(
            || {
                runtime_status_response(tonic::Status::internal(
                    "cron create response did not include job_id",
                ))
            },
        )?;
    let job =
        state.runtime.cron_job(job_id.clone()).await.map_err(runtime_status_response)?.ok_or_else(
            || {
                runtime_status_response(tonic::Status::internal(format!(
                    "created cron job not found: {job_id}"
                )))
            },
        )?;
    Ok(Json(json!({ "job": job })))
}

async fn console_cron_set_enabled_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(job_id): Path<String>,
    Json(payload): Json<ConsoleCronEnabledRequest>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, true)?;
    validate_canonical_id(job_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument("job_id must be a canonical ULID"))
    })?;
    let updated = state
        .runtime
        .update_cron_job(
            job_id.clone(),
            CronJobUpdatePatch { enabled: Some(payload.enabled), ..CronJobUpdatePatch::default() },
        )
        .await
        .map_err(runtime_status_response)?;
    state.scheduler_wake.notify_one();
    Ok(Json(json!({ "job": updated })))
}

async fn console_cron_run_now_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(job_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    validate_canonical_id(job_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument("job_id must be a canonical ULID"))
    })?;
    let mut request = TonicRequest::new(cron_v1::RunJobNowRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        job_id: Some(common_v1::CanonicalId { ulid: job_id }),
    });
    apply_console_rpc_context(&state, &session, request.metadata_mut())?;
    let service = build_console_cron_service(&state);
    let response =
        <gateway::CronServiceImpl as cron_v1::cron_service_server::CronService>::run_job_now(
            &service, request,
        )
        .await
        .map_err(runtime_status_response)?
        .into_inner();
    let status = cron_v1::JobRunStatus::try_from(response.status)
        .unwrap_or(cron_v1::JobRunStatus::Unspecified)
        .as_str_name()
        .to_ascii_lowercase();
    Ok(Json(json!({
        "run_id": response.run_id.map(|value| value.ulid),
        "status": status,
        "message": response.message,
    })))
}

async fn console_cron_runs_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(job_id): Path<String>,
    Query(query): Query<ConsoleCronRunsQuery>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    validate_canonical_id(job_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument("job_id must be a canonical ULID"))
    })?;
    let (runs, next_after_run_id) = state
        .runtime
        .list_cron_runs(Some(job_id), query.after_run_id, query.limit)
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "runs": runs,
        "next_after_run_id": next_after_run_id,
    })))
}

#[allow(clippy::result_large_err)]
fn build_console_schedule(
    schedule_type_raw: &str,
    payload: &ConsoleCronCreateRequest,
) -> Result<cron_v1::Schedule, Response> {
    match schedule_type_raw.trim().to_ascii_lowercase().as_str() {
        "cron" => {
            let expression = payload
                .cron_expression
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    runtime_status_response(tonic::Status::invalid_argument(
                        "cron_expression is required for schedule_type=cron",
                    ))
                })?;
            Ok(cron_v1::Schedule {
                r#type: cron_v1::ScheduleType::Cron as i32,
                spec: Some(cron_v1::schedule::Spec::Cron(cron_v1::CronSchedule {
                    expression: expression.to_owned(),
                })),
            })
        }
        "every" => {
            let interval_ms =
                payload.every_interval_ms.filter(|value| *value > 0).ok_or_else(|| {
                    runtime_status_response(tonic::Status::invalid_argument(
                        "every_interval_ms must be greater than zero for schedule_type=every",
                    ))
                })?;
            Ok(cron_v1::Schedule {
                r#type: cron_v1::ScheduleType::Every as i32,
                spec: Some(cron_v1::schedule::Spec::Every(cron_v1::EverySchedule { interval_ms })),
            })
        }
        "at" => {
            let timestamp = payload
                .at_timestamp_rfc3339
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    runtime_status_response(tonic::Status::invalid_argument(
                        "at_timestamp_rfc3339 is required for schedule_type=at",
                    ))
                })?;
            Ok(cron_v1::Schedule {
                r#type: cron_v1::ScheduleType::At as i32,
                spec: Some(cron_v1::schedule::Spec::At(cron_v1::AtSchedule {
                    timestamp_rfc3339: timestamp.to_owned(),
                })),
            })
        }
        _ => Err(runtime_status_response(tonic::Status::invalid_argument(
            "schedule_type must be one of cron|every|at",
        ))),
    }
}

fn build_console_cron_service(state: &AppState) -> gateway::CronServiceImpl {
    gateway::CronServiceImpl::new(
        Arc::clone(&state.runtime),
        state.auth.clone(),
        state.grpc_url.clone(),
        Arc::clone(&state.scheduler_wake),
        state.cron_timezone_mode,
    )
}

#[allow(clippy::result_large_err)]
fn apply_console_rpc_context(
    state: &AppState,
    session: &ConsoleSession,
    metadata: &mut tonic::metadata::MetadataMap,
) -> Result<(), Response> {
    if state.auth.require_auth {
        let token = state.auth.admin_token.as_deref().ok_or_else(|| {
            runtime_status_response(tonic::Status::failed_precondition(
                "admin token is not configured for authenticated console RPC dispatch",
            ))
        })?;
        let bearer = MetadataValue::try_from(format!("Bearer {token}").as_str()).map_err(|_| {
            runtime_status_response(tonic::Status::internal(
                "failed to encode authorization metadata",
            ))
        })?;
        metadata.insert("authorization", bearer);
    }
    let principal = MetadataValue::try_from(session.context.principal.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::internal("failed to encode principal metadata"))
    })?;
    metadata.insert(gateway::HEADER_PRINCIPAL, principal);
    let device_id = MetadataValue::try_from(session.context.device_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::internal("failed to encode device metadata"))
    })?;
    metadata.insert(gateway::HEADER_DEVICE_ID, device_id);
    if let Some(channel) = session.context.channel.as_deref() {
        let channel = MetadataValue::try_from(channel).map_err(|_| {
            runtime_status_response(tonic::Status::internal("failed to encode channel metadata"))
        })?;
        metadata.insert(gateway::HEADER_CHANNEL, channel);
    }
    Ok(())
}

async fn console_memory_search_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleMemorySearchQuery>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let search_query = query.query.trim();
    if search_query.is_empty() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "query cannot be empty",
        )));
    }
    let min_score = query.min_score.unwrap_or(0.0);
    if !min_score.is_finite() || !(0.0..=1.0).contains(&min_score) {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "min_score must be in range 0.0..=1.0",
        )));
    }
    let session_scope = query.session_id.clone().and_then(trim_to_option);
    if let Some(session_scope) = session_scope.as_deref() {
        validate_canonical_id(session_scope).map_err(|_| {
            runtime_status_response(tonic::Status::invalid_argument(
                "session_id must be a canonical ULID",
            ))
        })?;
    }

    let sources = parse_memory_sources_csv(query.sources_csv.as_deref())?;
    let hits = state
        .runtime
        .search_memory(journal::MemorySearchRequest {
            principal: session.context.principal,
            channel: query.channel.or(session.context.channel),
            session_id: session_scope,
            query: search_query.to_owned(),
            top_k: query.top_k.unwrap_or(8).clamp(1, 50),
            min_score,
            tags: parse_csv_values(query.tags_csv.as_deref()),
            sources,
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({ "hits": hits })))
}

async fn console_memory_purge_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleMemoryPurgeRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let session_scope = payload.session_id.clone().and_then(trim_to_option);
    if let Some(session_scope) = session_scope.as_deref() {
        validate_canonical_id(session_scope).map_err(|_| {
            runtime_status_response(tonic::Status::invalid_argument(
                "session_id must be a canonical ULID",
            ))
        })?;
    }
    let purge_all_principal = payload.purge_all_principal.unwrap_or(false);
    if !purge_all_principal
        && payload.channel.as_deref().is_none_or(|value| value.trim().is_empty())
        && session_scope.is_none()
    {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "purge request requires purge_all_principal=true or channel/session scope",
        )));
    }

    let deleted_count = state
        .runtime
        .purge_memory(MemoryPurgeRequest {
            principal: session.context.principal,
            channel: payload.channel,
            session_id: session_scope,
            purge_all_principal,
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({ "deleted_count": deleted_count })))
}

fn parse_csv_values(raw: Option<&str>) -> Vec<String> {
    raw.map(|value| {
        value
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>()
    })
    .unwrap_or_default()
}

#[allow(clippy::result_large_err)]
fn parse_memory_sources_csv(raw: Option<&str>) -> Result<Vec<journal::MemorySource>, Response> {
    let mut parsed = Vec::new();
    for value in parse_csv_values(raw) {
        let source = journal::MemorySource::from_str(value.as_str()).ok_or_else(|| {
            runtime_status_response(tonic::Status::invalid_argument(format!(
                "unsupported memory source value: {value}"
            )))
        })?;
        parsed.push(source);
    }
    Ok(parsed)
}

async fn console_skills_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleSkillsListQuery>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let skills_root = resolve_skills_root()?;
    let mut index = load_installed_skills_index(skills_root.as_path())?;
    if let Some(skill_id) =
        query.skill_id.as_deref().map(str::trim).filter(|value| !value.is_empty())
    {
        let skill_id = skill_id.to_ascii_lowercase();
        index.entries.retain(|entry| entry.skill_id == skill_id);
    }

    let mut entries = Vec::with_capacity(index.entries.len());
    for entry in index.entries {
        let status = state
            .runtime
            .skill_status(entry.skill_id.clone(), entry.version.clone())
            .await
            .map_err(runtime_status_response)?;
        entries.push(json!({
            "record": entry,
            "status": status,
        }));
    }
    Ok(Json(json!({
        "skills_root": skills_root,
        "count": entries.len(),
        "entries": entries,
    })))
}

async fn console_skills_install_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleSkillInstallRequest>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, true)?;
    let artifact_path_raw = payload.artifact_path.trim();
    if artifact_path_raw.is_empty() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "artifact_path cannot be empty",
        )));
    }
    let artifact_path = PathBuf::from(artifact_path_raw);
    let artifact_bytes = fs::read(artifact_path.as_path()).map_err(|error| {
        runtime_status_response(tonic::Status::invalid_argument(format!(
            "failed to read artifact {}: {error}",
            artifact_path.display()
        )))
    })?;
    let inspection = inspect_skill_artifact(artifact_bytes.as_slice()).map_err(|error| {
        runtime_status_response(tonic::Status::invalid_argument(format!(
            "skill artifact inspection failed: {error}"
        )))
    })?;

    let skills_root = resolve_skills_root()?;
    fs::create_dir_all(skills_root.as_path()).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to create skills root {}: {error}",
            skills_root.display()
        )))
    })?;
    let trust_store_path = resolve_skills_trust_store_path(skills_root.as_path());
    let mut trust_store = load_trust_store(trust_store_path.as_path())?;
    let allow_tofu = payload.allow_tofu.unwrap_or(true);
    let verification =
        match verify_skill_artifact(artifact_bytes.as_slice(), &mut trust_store, allow_tofu) {
            Ok(report) => Some(report),
            Err(error) if payload.allow_untrusted.unwrap_or(false) => {
                tracing::warn!(
                    error = %error,
                    artifact_path = %artifact_path.display(),
                    "console skill install proceeding with allow_untrusted override"
                );
                None
            }
            Err(error) => {
                return Err(runtime_status_response(tonic::Status::invalid_argument(format!(
                    "skill artifact verification failed: {error}"
                ))));
            }
        };
    save_trust_store(trust_store_path.as_path(), &trust_store)?;

    let skill_id = inspection.manifest.skill_id.clone();
    let version = inspection.manifest.version.clone();
    let managed_artifact_path =
        managed_skill_artifact_path(skills_root.as_path(), skill_id.as_str(), version.as_str());
    if let Some(parent) = managed_artifact_path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            runtime_status_response(tonic::Status::internal(format!(
                "failed to create managed skill directory {}: {error}",
                parent.display()
            )))
        })?;
    }
    fs::write(managed_artifact_path.as_path(), artifact_bytes.as_slice()).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to persist managed artifact {}: {error}",
            managed_artifact_path.display()
        )))
    })?;

    let mut index = load_installed_skills_index(skills_root.as_path())?;
    index.entries.retain(|entry| !(entry.skill_id == skill_id && entry.version == version));
    for entry in &mut index.entries {
        if entry.skill_id == skill_id {
            entry.current = false;
        }
    }
    let record = InstalledSkillRecord {
        skill_id: skill_id.clone(),
        version: version.clone(),
        publisher: inspection.manifest.publisher.clone(),
        current: true,
        installed_at_unix_ms: unix_ms_now().map_err(|error| {
            runtime_status_response(tonic::Status::internal(format!(
                "failed to read system clock: {error}"
            )))
        })?,
        artifact_sha256: sha256_hex(artifact_bytes.as_slice()),
        payload_sha256: verification
            .as_ref()
            .map(|report| report.payload_sha256.clone())
            .unwrap_or_else(|| inspection.payload_sha256.clone()),
        signature_key_id: inspection.signature.key_id.clone(),
        trust_decision: verification
            .as_ref()
            .map(|report| trust_decision_label(report.trust_decision))
            .unwrap_or_else(|| "untrusted_override".to_owned()),
        source: InstalledSkillSource {
            kind: "managed_artifact".to_owned(),
            reference: artifact_path.to_string_lossy().into_owned(),
        },
        missing_secrets: Vec::new(),
    };
    index.entries.push(record.clone());
    save_installed_skills_index(skills_root.as_path(), &index)?;
    Ok(Json(json!({
        "installed": true,
        "record": record,
        "skills_root": skills_root,
        "trust_store": trust_store_path,
    })))
}

async fn console_skills_verify_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(skill_id): Path<String>,
    Json(payload): Json<ConsoleSkillActionRequest>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, true)?;
    let skill_id = normalize_non_empty_field(skill_id, "skill_id")?;
    let skills_root = resolve_skills_root()?;
    let mut index = load_installed_skills_index(skills_root.as_path())?;
    let version = resolve_skill_version(&index, skill_id.as_str(), payload.version.as_deref())?;
    let artifact_path =
        managed_skill_artifact_path(skills_root.as_path(), skill_id.as_str(), version.as_str());
    let artifact_bytes = fs::read(artifact_path.as_path()).map_err(|error| {
        runtime_status_response(tonic::Status::invalid_argument(format!(
            "failed to read managed artifact {}: {error}",
            artifact_path.display()
        )))
    })?;

    let trust_store_path = resolve_skills_trust_store_path(skills_root.as_path());
    let mut trust_store = load_trust_store(trust_store_path.as_path())?;
    let report = verify_skill_artifact(
        artifact_bytes.as_slice(),
        &mut trust_store,
        payload.allow_tofu.unwrap_or(false),
    )
    .map_err(|error| {
        runtime_status_response(tonic::Status::invalid_argument(format!(
            "skill verification failed: {error}"
        )))
    })?;
    save_trust_store(trust_store_path.as_path(), &trust_store)?;
    if let Some(entry) = index
        .entries
        .iter_mut()
        .find(|entry| entry.skill_id == skill_id && entry.version == version)
    {
        entry.payload_sha256 = report.payload_sha256.clone();
        entry.publisher = report.manifest.publisher.clone();
        entry.trust_decision = trust_decision_label(report.trust_decision);
    }
    save_installed_skills_index(skills_root.as_path(), &index)?;
    Ok(Json(json!({ "report": report })))
}

async fn console_skills_audit_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(skill_id): Path<String>,
    Json(payload): Json<ConsoleSkillActionRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let skill_id = normalize_non_empty_field(skill_id, "skill_id")?;
    let skills_root = resolve_skills_root()?;
    let index = load_installed_skills_index(skills_root.as_path())?;
    let version = resolve_skill_version(&index, skill_id.as_str(), payload.version.as_deref())?;
    let artifact_path =
        managed_skill_artifact_path(skills_root.as_path(), skill_id.as_str(), version.as_str());
    let artifact_bytes = fs::read(artifact_path.as_path()).map_err(|error| {
        runtime_status_response(tonic::Status::invalid_argument(format!(
            "failed to read managed artifact {}: {error}",
            artifact_path.display()
        )))
    })?;

    let trust_store_path = resolve_skills_trust_store_path(skills_root.as_path());
    let mut trust_store = load_trust_store(trust_store_path.as_path())?;
    let report = audit_skill_artifact_security(
        artifact_bytes.as_slice(),
        &mut trust_store,
        payload.allow_tofu.unwrap_or(false),
        &SkillSecurityAuditPolicy::default(),
    )
    .map_err(|error| {
        runtime_status_response(tonic::Status::invalid_argument(format!(
            "skill security audit failed: {error}"
        )))
    })?;
    save_trust_store(trust_store_path.as_path(), &trust_store)?;

    let quarantined = if report.should_quarantine && payload.quarantine_on_fail.unwrap_or(true) {
        let record = state
            .runtime
            .upsert_skill_status(SkillStatusUpsertRequest {
                skill_id: report.skill_id.clone(),
                version: report.version.clone(),
                status: SkillExecutionStatus::Quarantined,
                reason: Some(format!("console_audit: {}", report.quarantine_reasons.join(" | "))),
                detected_at_ms: unix_ms_now().map_err(|error| {
                    runtime_status_response(tonic::Status::internal(format!(
                        "failed to read system clock: {error}"
                    )))
                })?,
                operator_principal: session.context.principal.clone(),
            })
            .await
            .map_err(runtime_status_response)?;
        state
            .runtime
            .record_skill_status_event(&session.context, "skill.quarantined", &record)
            .await
            .map_err(runtime_status_response)?;
        true
    } else {
        false
    };
    Ok(Json(json!({
        "report": report,
        "quarantined": quarantined,
    })))
}

async fn console_skill_quarantine_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(skill_id): Path<String>,
    Json(payload): Json<SkillStatusRequest>,
) -> Result<Json<SkillStatusResponse>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let skill_id = normalize_non_empty_field(skill_id, "skill_id")?;
    let version = normalize_non_empty_field(payload.version, "version")?;
    let record = state
        .runtime
        .upsert_skill_status(SkillStatusUpsertRequest {
            skill_id,
            version,
            status: SkillExecutionStatus::Quarantined,
            reason: payload.reason.and_then(trim_to_option),
            detected_at_ms: unix_ms_now().map_err(|error| {
                runtime_status_response(tonic::Status::internal(format!(
                    "failed to read system clock: {error}"
                )))
            })?,
            operator_principal: session.context.principal.clone(),
        })
        .await
        .map_err(runtime_status_response)?;
    state
        .runtime
        .record_skill_status_event(&session.context, "skill.quarantined", &record)
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(skill_status_response(record)))
}

async fn console_skill_enable_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(skill_id): Path<String>,
    Json(payload): Json<SkillStatusRequest>,
) -> Result<Json<SkillStatusResponse>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    if !payload.override_enabled.unwrap_or(false) {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "enable requires explicit override=true acknowledgment",
        )));
    }
    let skill_id = normalize_non_empty_field(skill_id, "skill_id")?;
    let version = normalize_non_empty_field(payload.version, "version")?;
    let record = state
        .runtime
        .upsert_skill_status(SkillStatusUpsertRequest {
            skill_id,
            version,
            status: SkillExecutionStatus::Active,
            reason: payload.reason.and_then(trim_to_option),
            detected_at_ms: unix_ms_now().map_err(|error| {
                runtime_status_response(tonic::Status::internal(format!(
                    "failed to read system clock: {error}"
                )))
            })?,
            operator_principal: session.context.principal.clone(),
        })
        .await
        .map_err(runtime_status_response)?;
    state
        .runtime
        .record_skill_status_event(&session.context, "skill.enabled", &record)
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(skill_status_response(record)))
}

async fn console_audit_events_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleAuditEventsQuery>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let limit = query.limit.unwrap_or(200).clamp(1, 2_000);
    let snapshot =
        state.runtime.recent_journal_snapshot(limit).await.map_err(runtime_status_response)?;
    let contains = query
        .contains
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_ascii_lowercase());
    let events = snapshot
        .events
        .into_iter()
        .filter(|event| query.kind.is_none_or(|kind| event.kind == kind))
        .filter(|event| {
            query
                .principal
                .as_deref()
                .is_none_or(|principal| event.principal.eq_ignore_ascii_case(principal.trim()))
        })
        .filter(|event| {
            query.channel.as_deref().is_none_or(|channel| {
                event
                    .channel
                    .as_deref()
                    .is_some_and(|value| value.eq_ignore_ascii_case(channel.trim()))
            })
        })
        .filter(|event| {
            contains.as_ref().is_none_or(|needle| {
                event.payload_json.to_ascii_lowercase().contains(needle.as_str())
            })
        })
        .collect::<Vec<_>>();
    Ok(Json(json!({
        "hash_chain_enabled": snapshot.hash_chain_enabled,
        "total_events": snapshot.total_events,
        "returned_events": events.len(),
        "events": events,
    })))
}

#[allow(clippy::result_large_err)]
fn resolve_skills_root() -> Result<PathBuf, Response> {
    let identity_root = default_identity_store_root().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to resolve default identity root: {error}"
        )))
    })?;
    let state_root =
        identity_root.parent().map(FsPath::to_path_buf).unwrap_or_else(|| identity_root.clone());
    Ok(state_root.join("skills"))
}

fn resolve_skills_trust_store_path(skills_root: &FsPath) -> PathBuf {
    match std::env::var("PALYRA_SKILLS_TRUST_STORE") {
        Ok(raw) if !raw.trim().is_empty() => PathBuf::from(raw),
        _ => skills_root.join("trust-store.json"),
    }
}

#[allow(clippy::result_large_err)]
fn load_trust_store(path: &FsPath) -> Result<SkillTrustStore, Response> {
    if !path.exists() {
        return Ok(SkillTrustStore::default());
    }
    SkillTrustStore::load(path).map_err(|error| {
        runtime_status_response(tonic::Status::invalid_argument(format!(
            "failed to load trust store {}: {error}",
            path.display()
        )))
    })
}

#[allow(clippy::result_large_err)]
fn save_trust_store(path: &FsPath, store: &SkillTrustStore) -> Result<(), Response> {
    store.save(path).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to persist trust store {}: {error}",
            path.display()
        )))
    })
}

#[allow(clippy::result_large_err)]
fn load_installed_skills_index(skills_root: &FsPath) -> Result<InstalledSkillsIndex, Response> {
    let index_path = skills_root.join(SKILLS_INDEX_FILE_NAME);
    if !index_path.exists() {
        return Ok(InstalledSkillsIndex::default());
    }
    let payload = fs::read(index_path.as_path()).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read installed skills index {}: {error}",
            index_path.display()
        )))
    })?;
    let mut index: InstalledSkillsIndex =
        serde_json::from_slice(payload.as_slice()).map_err(|error| {
            runtime_status_response(tonic::Status::invalid_argument(format!(
                "failed to parse installed skills index {}: {error}",
                index_path.display()
            )))
        })?;
    if index.schema_version != SKILLS_LAYOUT_VERSION {
        return Err(runtime_status_response(tonic::Status::invalid_argument(format!(
            "unsupported installed skills index schema version {}",
            index.schema_version
        ))));
    }
    normalize_installed_skills_index(&mut index);
    Ok(index)
}

#[allow(clippy::result_large_err)]
fn save_installed_skills_index(
    skills_root: &FsPath,
    index: &InstalledSkillsIndex,
) -> Result<(), Response> {
    fs::create_dir_all(skills_root).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to create skills root {}: {error}",
            skills_root.display()
        )))
    })?;
    let mut normalized = index.clone();
    normalized.schema_version = SKILLS_LAYOUT_VERSION;
    normalized.updated_at_unix_ms = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock: {error}"
        )))
    })?;
    normalize_installed_skills_index(&mut normalized);
    let payload = serde_json::to_vec_pretty(&normalized).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to serialize installed skills index: {error}"
        )))
    })?;
    fs::write(skills_root.join(SKILLS_INDEX_FILE_NAME), payload).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to write installed skills index {}: {error}",
            skills_root.join(SKILLS_INDEX_FILE_NAME).display()
        )))
    })
}

fn normalize_installed_skills_index(index: &mut InstalledSkillsIndex) {
    index.entries.sort_by(|left, right| {
        left.skill_id
            .cmp(&right.skill_id)
            .then_with(|| left.version.cmp(&right.version))
            .then_with(|| right.installed_at_unix_ms.cmp(&left.installed_at_unix_ms))
    });
    let mut current_by_skill = HashMap::<String, bool>::new();
    for entry in &mut index.entries {
        if current_by_skill.get(entry.skill_id.as_str()).copied().unwrap_or(false) {
            entry.current = false;
        } else if entry.current {
            current_by_skill.insert(entry.skill_id.clone(), true);
        }
    }
    for entry in &mut index.entries {
        current_by_skill.entry(entry.skill_id.clone()).or_insert_with(|| {
            entry.current = true;
            true
        });
    }
}

#[allow(clippy::result_large_err)]
fn resolve_skill_version(
    index: &InstalledSkillsIndex,
    skill_id: &str,
    version: Option<&str>,
) -> Result<String, Response> {
    if let Some(version) = version.map(str::trim).filter(|value| !value.is_empty()) {
        return Ok(version.to_owned());
    }
    let current = index
        .entries
        .iter()
        .find(|entry| entry.skill_id == skill_id && entry.current)
        .or_else(|| index.entries.iter().find(|entry| entry.skill_id == skill_id))
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found(format!(
                "installed skill not found: {skill_id}"
            )))
        })?;
    Ok(current.version.clone())
}

fn managed_skill_artifact_path(skills_root: &FsPath, skill_id: &str, version: &str) -> PathBuf {
    skills_root.join(skill_id).join(version).join(SKILL_ARTIFACT_FILE_NAME)
}

fn trust_decision_label(decision: palyra_skills::TrustDecision) -> String {
    match decision {
        palyra_skills::TrustDecision::Allowlisted => "allowlisted".to_owned(),
        palyra_skills::TrustDecision::TofuPinned => "tofu_pinned".to_owned(),
        palyra_skills::TrustDecision::TofuNewlyPinned => "tofu_newly_pinned".to_owned(),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn trim_to_option(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

#[allow(clippy::result_large_err)]
fn normalize_non_empty_field(value: String, field_name: &'static str) -> Result<String, Response> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(format!(
            "{field_name} cannot be empty"
        ))));
    }
    if field_name == "skill_id" {
        return Ok(trimmed.to_ascii_lowercase());
    }
    Ok(trimmed.to_owned())
}

fn auth_error_response(error: AuthError) -> Response {
    let status = match error {
        AuthError::MissingConfiguredToken => StatusCode::SERVICE_UNAVAILABLE,
        AuthError::InvalidAuthorizationHeader | AuthError::InvalidToken => StatusCode::UNAUTHORIZED,
        AuthError::MissingContext(_) | AuthError::EmptyContext(_) | AuthError::InvalidDeviceId => {
            StatusCode::BAD_REQUEST
        }
    };
    (status, Json(ErrorBody { error: error.to_string() })).into_response()
}

fn runtime_status_response(status: tonic::Status) -> Response {
    let http_status = match status.code() {
        tonic::Code::PermissionDenied => StatusCode::FORBIDDEN,
        tonic::Code::InvalidArgument => StatusCode::BAD_REQUEST,
        tonic::Code::FailedPrecondition => StatusCode::PRECONDITION_FAILED,
        tonic::Code::NotFound => StatusCode::NOT_FOUND,
        tonic::Code::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (http_status, Json(ErrorBody { error: status.message().to_owned() })).into_response()
}

fn validate_admin_auth_config(auth: &GatewayAuthConfig) -> Result<()> {
    if auth.require_auth && auth.admin_token.as_deref().is_none_or(|value| value.trim().is_empty())
    {
        anyhow::bail!(
            "admin auth is enabled but no admin token is configured; set PALYRA_ADMIN_TOKEN or admin.auth_token in config"
        );
    }
    Ok(())
}

fn validate_process_runner_backend_policy(
    enabled: bool,
    tier: sandbox_runner::SandboxProcessRunnerTier,
    egress_enforcement_mode: sandbox_runner::EgressEnforcementMode,
    has_host_allowlists: bool,
) -> Result<()> {
    if enabled && matches!(tier, sandbox_runner::SandboxProcessRunnerTier::C) && cfg!(windows) {
        anyhow::bail!(
            "tool_call.process_runner.tier='c' is unsupported on windows until Tier-C backend isolation is OS-enforced"
        );
    }
    if enabled
        && matches!(egress_enforcement_mode, sandbox_runner::EgressEnforcementMode::Strict)
        && has_host_allowlists
    {
        anyhow::bail!(
            "tool_call.process_runner.egress_enforcement_mode='strict' does not support host allowlists; clear allowlists or switch to preflight mode with dedicated network tools"
        );
    }
    Ok(())
}

fn resolve_model_provider_secret_from_vault(
    loaded: &mut config::LoadedConfig,
    vault: &Vault,
) -> Result<Option<SecretAccessAuditRecord>> {
    if loaded.model_provider.kind != ModelProviderKind::OpenAiCompatible {
        return Ok(None);
    }
    if loaded.model_provider.openai_api_key.is_some() {
        return Ok(None);
    }
    let Some(vault_ref_raw) = loaded.model_provider.openai_api_key_vault_ref.clone() else {
        return Ok(None);
    };
    let vault_ref = VaultRef::parse(vault_ref_raw.as_str()).with_context(|| {
        format!("invalid model_provider.openai_api_key_vault_ref: {vault_ref_raw}")
    })?;
    let value = vault.get_secret(&vault_ref.scope, vault_ref.key.as_str()).with_context(|| {
        format!("failed to load model provider API key from vault ref {}", vault_ref_raw)
    })?;
    if value.is_empty() {
        anyhow::bail!("vault ref {} resolved to an empty secret value", vault_ref_raw);
    }
    let decoded = String::from_utf8(value.clone())
        .context("model provider API key from vault must be valid UTF-8 text")?;
    if decoded.trim().is_empty() {
        anyhow::bail!(
            "model provider API key from vault ref {} cannot be whitespace only",
            vault_ref_raw
        );
    }
    loaded.model_provider.openai_api_key = Some(decoded);
    Ok(Some(SecretAccessAuditRecord {
        scope: vault_ref.scope.to_string(),
        key: vault_ref.key,
        action: "model_provider.openai_api_key.resolve".to_owned(),
        value_bytes: value.len(),
    }))
}

fn record_secret_access_journal_event(
    journal_store: &JournalStore,
    audit: &SecretAccessAuditRecord,
) -> Result<()> {
    journal_store
        .append(&JournalAppendRequest {
            event_id: Ulid::new().to_string(),
            session_id: Ulid::new().to_string(),
            run_id: Ulid::new().to_string(),
            kind: gateway::proto::palyra::common::v1::journal_event::EventKind::ToolExecuted as i32,
            actor: gateway::proto::palyra::common::v1::journal_event::EventActor::System as i32,
            timestamp_unix_ms: unix_ms_now()?,
            payload_json: json!({
                "event": "secret.accessed",
                "action": audit.action,
                "scope": audit.scope,
                "key": audit.key,
                "value_bytes": audit.value_bytes,
            })
            .to_string()
            .into_bytes(),
            principal: SYSTEM_DAEMON_PRINCIPAL.to_owned(),
            device_id: SYSTEM_DAEMON_DEVICE_ID.to_owned(),
            channel: Some(SYSTEM_VAULT_CHANNEL.to_owned()),
        })
        .context("failed to append secret.accessed journal event")?;
    Ok(())
}

fn unix_ms_now() -> Result<i64> {
    let elapsed =
        SystemTime::now().duration_since(UNIX_EPOCH).context("system clock before UNIX epoch")?;
    Ok(elapsed.as_millis() as i64)
}

fn load_identity_runtime(configured_store_root: Option<PathBuf>) -> Result<IdentityRuntime> {
    let store_root = if let Some(configured_store_root) = configured_store_root {
        configured_store_root
    } else {
        default_identity_store_root().context("failed to resolve default identity store path")?
    };
    let store = FilesystemSecretStore::new(&store_root).with_context(|| {
        format!("failed to initialize identity store at {}", store_root.display())
    })?;
    let store: std::sync::Arc<dyn SecretStore> = std::sync::Arc::new(store);
    let mut manager =
        IdentityManager::with_store(store).context("failed to initialize identity manager")?;
    let revoked_certificate_fingerprints = manager.revoked_certificate_fingerprints();
    let gateway_ca_certificate_pem = manager.gateway_ca_certificate_pem();
    let node_server_certificate = manager
        .issue_gateway_server_certificate("palyrad-node-rpc")
        .context("failed to issue node RPC gateway certificate")?;
    Ok(IdentityRuntime {
        store_root,
        revoked_certificate_count: revoked_certificate_fingerprints.len(),
        revoked_certificate_fingerprints,
        gateway_ca_certificate_pem,
        node_server_certificate,
    })
}

fn build_gateway_tls_config(tls: &config::GatewayTlsConfig) -> Result<ServerTlsConfig> {
    let cert_path =
        tls.cert_path.as_ref().context("gateway TLS enabled but cert path is missing")?;
    let key_path = tls.key_path.as_ref().context("gateway TLS enabled but key path is missing")?;
    let cert_pem = std::fs::read(cert_path)
        .with_context(|| format!("failed to read gateway TLS cert {}", cert_path.display()))?;
    let key_pem = std::fs::read(key_path)
        .with_context(|| format!("failed to read gateway TLS key {}", key_path.display()))?;

    let mut tls_config = ServerTlsConfig::new().identity(Identity::from_pem(cert_pem, key_pem));
    if let Some(client_ca_path) = tls.client_ca_path.as_ref() {
        let client_ca_pem = std::fs::read(client_ca_path).with_context(|| {
            format!("failed to read gateway TLS client CA {}", client_ca_path.display())
        })?;
        tls_config = tls_config.client_ca_root(Certificate::from_pem(client_ca_pem));
    }
    Ok(tls_config)
}

fn build_node_rpc_tls_config(
    identity_runtime: &IdentityRuntime,
    mtls_required: bool,
) -> ServerTlsConfig {
    let mut tls_config = ServerTlsConfig::new().identity(Identity::from_pem(
        identity_runtime.node_server_certificate.certificate_pem.clone(),
        identity_runtime.node_server_certificate.private_key_pem.clone(),
    ));
    if mtls_required {
        tls_config = tls_config.client_ca_root(Certificate::from_pem(
            identity_runtime.gateway_ca_certificate_pem.clone(),
        ));
    }
    tls_config
}

fn enforce_remote_bind_guard(
    admin_address: SocketAddr,
    grpc_address: SocketAddr,
    quic_address: Option<SocketAddr>,
    allow_insecure_remote: bool,
    gateway_tls_enabled: bool,
    node_rpc_mtls_required: bool,
    dangerous_remote_bind_acknowledged: bool,
) -> Result<()> {
    let admin_remote = !admin_address.ip().is_loopback();
    let grpc_remote = !grpc_address.ip().is_loopback();
    let quic_remote = quic_address.is_some_and(|address| !address.ip().is_loopback());
    let quic_display =
        quic_address.map(|address| address.to_string()).unwrap_or_else(|| "disabled".to_owned());
    if (admin_remote || grpc_remote || quic_remote) && !allow_insecure_remote {
        anyhow::bail!(
            "refusing non-loopback bind without explicit insecure opt-in: admin={} grpc={} quic={} (set gateway.allow_insecure_remote=true or PALYRA_GATEWAY_ALLOW_INSECURE_REMOTE=true to override)",
            admin_address,
            grpc_address,
            quic_display,
        );
    }
    let requires_danger_ack = admin_remote
        || (grpc_remote && (!gateway_tls_enabled || !node_rpc_mtls_required))
        || (quic_remote && !node_rpc_mtls_required);
    if requires_danger_ack && !dangerous_remote_bind_acknowledged {
        anyhow::bail!(
            "refusing insecure remote bind without explicit danger acknowledgement: admin={} grpc={} quic={} gateway_tls_enabled={} node_rpc_mtls_required={} (set {}=true to acknowledge risk, or keep admin loopback and enable gateway TLS + node RPC mTLS)",
            admin_address,
            grpc_address,
            quic_display,
            gateway_tls_enabled,
            node_rpc_mtls_required,
            DANGEROUS_REMOTE_BIND_ACK_ENV,
        );
    }
    Ok(())
}

fn dangerous_remote_bind_acknowledged() -> Result<bool> {
    match std::env::var(DANGEROUS_REMOTE_BIND_ACK_ENV) {
        Ok(raw) => raw
            .parse::<bool>()
            .with_context(|| format!("{DANGEROUS_REMOTE_BIND_ACK_ENV} must be true or false")),
        Err(std::env::VarError::NotPresent) => Ok(false),
        Err(std::env::VarError::NotUnicode(_)) => {
            anyhow::bail!("{DANGEROUS_REMOTE_BIND_ACK_ENV} must contain valid UTF-8")
        }
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use std::{
        collections::HashMap,
        net::IpAddr,
        str::FromStr,
        sync::Mutex,
        time::{Duration, Instant},
    };

    use axum::http::StatusCode;

    use super::{
        consume_admin_rate_limit_with_now, enforce_remote_bind_guard, loopback_grpc_url,
        runtime_status_response, validate_admin_auth_config,
        validate_process_runner_backend_policy, ADMIN_RATE_LIMIT_MAX_IP_BUCKETS,
        ADMIN_RATE_LIMIT_MAX_REQUESTS_PER_WINDOW,
    };
    use crate::gateway::GatewayAuthConfig;
    use crate::sandbox_runner::{EgressEnforcementMode, SandboxProcessRunnerTier};

    #[test]
    fn remote_bind_guard_allows_loopback_without_opt_in() {
        let result = enforce_remote_bind_guard(
            "127.0.0.1:7142".parse().expect("loopback endpoint should parse"),
            "127.0.0.1:7443".parse().expect("loopback endpoint should parse"),
            None,
            false,
            false,
            true,
            false,
        );
        assert!(result.is_ok(), "loopback bind should not require insecure opt-in");
    }

    #[test]
    fn remote_bind_guard_rejects_non_loopback_without_opt_in() {
        let result = enforce_remote_bind_guard(
            "0.0.0.0:7142".parse().expect("remote endpoint should parse"),
            "127.0.0.1:7443".parse().expect("loopback endpoint should parse"),
            None,
            false,
            false,
            true,
            false,
        );
        assert!(result.is_err(), "non-loopback bind should require explicit opt-in");
    }

    #[test]
    fn remote_bind_guard_allows_tls_grpc_remote_with_explicit_opt_in() {
        let result = enforce_remote_bind_guard(
            "127.0.0.1:7142".parse().expect("loopback endpoint should parse"),
            "0.0.0.0:7443".parse().expect("remote endpoint should parse"),
            None,
            true,
            true,
            true,
            false,
        );
        assert!(
            result.is_ok(),
            "TLS-enabled remote gRPC bind should be allowed with explicit opt-in"
        );
    }

    #[test]
    fn remote_bind_guard_requires_danger_ack_for_non_tls_grpc_remote() {
        let result = enforce_remote_bind_guard(
            "127.0.0.1:7142".parse().expect("loopback endpoint should parse"),
            "0.0.0.0:7443".parse().expect("remote endpoint should parse"),
            None,
            true,
            false,
            true,
            false,
        );
        assert!(
            result.is_err(),
            "non-TLS remote gRPC bind should require explicit danger acknowledgement"
        );
    }

    #[test]
    fn remote_bind_guard_allows_non_tls_grpc_remote_with_danger_ack() {
        let result = enforce_remote_bind_guard(
            "127.0.0.1:7142".parse().expect("loopback endpoint should parse"),
            "0.0.0.0:7443".parse().expect("remote endpoint should parse"),
            None,
            true,
            false,
            true,
            true,
        );
        assert!(result.is_ok(), "danger acknowledgement should allow non-TLS remote gRPC bind");
    }

    #[test]
    fn remote_bind_guard_requires_danger_ack_for_remote_admin_bind() {
        let result = enforce_remote_bind_guard(
            "0.0.0.0:7142".parse().expect("remote endpoint should parse"),
            "0.0.0.0:7443".parse().expect("remote endpoint should parse"),
            None,
            true,
            true,
            true,
            false,
        );
        assert!(
            result.is_err(),
            "remote admin bind should require explicit danger acknowledgement"
        );
    }

    #[test]
    fn loopback_grpc_url_matches_gateway_tls_mode() {
        let plain_url =
            loopback_grpc_url("0.0.0.0:7443".parse().expect("socket address should parse"), false);
        let tls_url =
            loopback_grpc_url("0.0.0.0:7443".parse().expect("socket address should parse"), true);
        assert_eq!(plain_url, "http://127.0.0.1:7443");
        assert_eq!(tls_url, "https://127.0.0.1:7443");
    }

    #[test]
    fn remote_bind_guard_requires_danger_ack_for_remote_grpc_when_node_rpc_mtls_disabled() {
        let result = enforce_remote_bind_guard(
            "127.0.0.1:7142".parse().expect("loopback endpoint should parse"),
            "0.0.0.0:7443".parse().expect("remote endpoint should parse"),
            None,
            true,
            true,
            false,
            false,
        );
        assert!(
            result.is_err(),
            "remote gRPC bind should require danger acknowledgement when node RPC mTLS is disabled"
        );
    }

    #[test]
    fn remote_bind_guard_allows_remote_grpc_with_node_rpc_mtls_disabled_and_danger_ack() {
        let result = enforce_remote_bind_guard(
            "127.0.0.1:7142".parse().expect("loopback endpoint should parse"),
            "0.0.0.0:7443".parse().expect("remote endpoint should parse"),
            None,
            true,
            true,
            false,
            true,
        );
        assert!(
            result.is_ok(),
            "danger acknowledgement should allow remote gRPC bind when node RPC mTLS is disabled"
        );
    }

    #[test]
    fn remote_bind_guard_rejects_quic_remote_without_opt_in() {
        let result = enforce_remote_bind_guard(
            "127.0.0.1:7142".parse().expect("loopback endpoint should parse"),
            "127.0.0.1:7443".parse().expect("loopback endpoint should parse"),
            Some("0.0.0.0:7444".parse().expect("remote QUIC endpoint should parse")),
            false,
            true,
            true,
            false,
        );
        assert!(result.is_err(), "remote QUIC bind should require explicit insecure opt-in");
    }

    #[test]
    fn remote_bind_guard_requires_danger_ack_for_remote_quic_when_node_rpc_mtls_disabled() {
        let result = enforce_remote_bind_guard(
            "127.0.0.1:7142".parse().expect("loopback endpoint should parse"),
            "127.0.0.1:7443".parse().expect("loopback endpoint should parse"),
            Some("0.0.0.0:7444".parse().expect("remote QUIC endpoint should parse")),
            true,
            true,
            false,
            false,
        );
        assert!(
            result.is_err(),
            "remote QUIC bind should require danger acknowledgement when node RPC mTLS is disabled"
        );
    }

    #[test]
    fn runtime_status_response_maps_resource_exhausted_to_too_many_requests() {
        let response = runtime_status_response(tonic::Status::resource_exhausted("rate limited"));
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[test]
    fn admin_auth_config_validation_fails_when_token_missing() {
        let error = validate_admin_auth_config(&GatewayAuthConfig {
            require_auth: true,
            admin_token: None,
            bound_principal: Some("user:ops".to_owned()),
        })
        .expect_err("missing admin token should fail preflight validation");
        assert!(
            error.to_string().contains("admin auth is enabled but no admin token is configured"),
            "error should explain admin token preflight requirement"
        );
    }

    #[test]
    fn admin_auth_config_validation_allows_disabled_auth_without_token() {
        let result = validate_admin_auth_config(&GatewayAuthConfig {
            require_auth: false,
            admin_token: None,
            bound_principal: None,
        });
        assert!(result.is_ok(), "disabled auth should allow missing token");
    }

    #[test]
    fn admin_rate_limit_rejects_after_window_budget_is_exhausted() {
        let buckets = Mutex::new(HashMap::new());
        let ip = IpAddr::from_str("127.0.0.1").expect("IP literal should parse");
        let now = Instant::now();
        for attempt in 0..ADMIN_RATE_LIMIT_MAX_REQUESTS_PER_WINDOW {
            let allowed = consume_admin_rate_limit_with_now(&buckets, ip, now);
            assert!(allowed, "attempt {attempt} should remain within the request budget");
        }
        assert!(
            !consume_admin_rate_limit_with_now(&buckets, ip, now),
            "request after budget exhaustion should be rejected"
        );
    }

    #[test]
    fn admin_rate_limit_resets_budget_after_window_elapses() {
        let buckets = Mutex::new(HashMap::new());
        let ip = IpAddr::from_str("127.0.0.1").expect("IP literal should parse");
        let now = Instant::now();
        for _ in 0..ADMIN_RATE_LIMIT_MAX_REQUESTS_PER_WINDOW {
            let _ = consume_admin_rate_limit_with_now(&buckets, ip, now);
        }
        assert!(
            !consume_admin_rate_limit_with_now(&buckets, ip, now),
            "budget should be exhausted within the same window"
        );
        let advanced = now + Duration::from_millis(1_200);
        assert!(
            consume_admin_rate_limit_with_now(&buckets, ip, advanced),
            "request should be allowed after the fixed window expires"
        );
    }

    #[test]
    fn admin_rate_limit_bucket_count_is_bounded() {
        let buckets = Mutex::new(HashMap::new());
        let now = Instant::now();
        for offset in 0..ADMIN_RATE_LIMIT_MAX_IP_BUCKETS {
            let ip = IpAddr::from([10, 0, (offset / 256) as u8, (offset % 256) as u8]);
            let allowed = consume_admin_rate_limit_with_now(&buckets, ip, now);
            assert!(allowed, "filling bucket {offset} should succeed");
        }
        let overflow_ip = IpAddr::from([10, 250, 0, 1]);
        assert!(
            consume_admin_rate_limit_with_now(&buckets, overflow_ip, now),
            "overflow principal should still be accepted after oldest-bucket eviction"
        );
        let bucket_count = buckets.lock().expect("bucket mutex should be available").len();
        assert_eq!(
            bucket_count, ADMIN_RATE_LIMIT_MAX_IP_BUCKETS,
            "bucket count must remain bounded to avoid unbounded memory growth"
        );
    }

    #[test]
    #[cfg(not(windows))]
    fn process_runner_backend_policy_allows_tier_c_on_supported_platforms() {
        let result = validate_process_runner_backend_policy(
            true,
            SandboxProcessRunnerTier::C,
            EgressEnforcementMode::Strict,
            false,
        );
        assert!(result.is_ok(), "tier-c should remain configurable on non-windows platforms");
    }

    #[test]
    #[cfg(windows)]
    fn process_runner_backend_policy_rejects_tier_c_on_windows() {
        let error = validate_process_runner_backend_policy(
            true,
            SandboxProcessRunnerTier::C,
            EgressEnforcementMode::Strict,
            false,
        )
        .expect_err("tier-c must fail closed on windows until backend isolation is implemented");
        assert!(
            error.to_string().contains("unsupported on windows"),
            "error should explain unsupported tier-c backend policy"
        );
    }

    #[test]
    fn process_runner_backend_policy_allows_tier_b() {
        let result = validate_process_runner_backend_policy(
            true,
            SandboxProcessRunnerTier::B,
            EgressEnforcementMode::Strict,
            false,
        );
        assert!(result.is_ok(), "tier-b should remain allowed");
    }

    #[test]
    fn process_runner_backend_policy_rejects_strict_mode_host_allowlists() {
        let error = validate_process_runner_backend_policy(
            true,
            SandboxProcessRunnerTier::B,
            EgressEnforcementMode::Strict,
            true,
        )
        .expect_err("strict mode host allowlists should fail closed");
        assert!(
            error
                .to_string()
                .contains("egress_enforcement_mode='strict' does not support host allowlists"),
            "error should explain strict-mode host allowlist policy restrictions"
        );
    }
}

async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::error!(error = %error, "failed to register Ctrl+C handler");
        std::future::pending::<()>().await;
    }
}
