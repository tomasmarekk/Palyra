mod agents;
mod channel_router;
mod config;
mod cron;
mod gateway;
mod journal;
mod model_provider;
mod node_rpc;
mod orchestrator;
mod sandbox_runner;
mod tool_protocol;
mod wasm_plugin_runner;

use std::{
    collections::{HashMap, HashSet},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use axum::{
    extract::{ConnectInfo, Path, Query, Request, State},
    http::{HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use config::load_config;
use cron::spawn_scheduler_loop;
use gateway::{
    authorize_headers, request_context_from_headers, AuthError, GatewayAuthConfig,
    GatewayJournalConfigSnapshot, GatewayRuntimeConfigSnapshot, GatewayRuntimeState,
    MemoryRuntimeConfig,
};
use journal::{
    JournalAppendRequest, JournalConfig, JournalStore, OrchestratorCancelRequest,
    OrchestratorRunStatusSnapshot, SkillExecutionStatus, SkillStatusRecord,
    SkillStatusUpsertRequest,
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
use palyra_vault::{Vault, VaultConfig as VaultConfigOptions, VaultRef};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::Notify;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tracing::info;
use tracing_subscriber::EnvFilter;
use ulid::Ulid;

const DANGEROUS_REMOTE_BIND_ACK_ENV: &str = "PALYRA_GATEWAY_DANGEROUS_REMOTE_BIND_ACK";
const SYSTEM_DAEMON_PRINCIPAL: &str = "system:daemon";
const SYSTEM_VAULT_CHANNEL: &str = "system:vault";
const SYSTEM_DAEMON_DEVICE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
const GRPC_MAX_DECODING_MESSAGE_SIZE_BYTES: usize = 4 * 1024 * 1024;
const GRPC_MAX_ENCODING_MESSAGE_SIZE_BYTES: usize = 4 * 1024 * 1024;
const ADMIN_RATE_LIMIT_WINDOW_MS: u64 = 1_000;
const ADMIN_RATE_LIMIT_MAX_REQUESTS_PER_WINDOW: u32 = 30;
const ADMIN_RATE_LIMIT_MAX_IP_BUCKETS: usize = 4_096;

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
            node_rpc_mtls_required: !loaded.identity.allow_insecure_node_rpc_without_mtls,
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
        node_rpc_mtls_required = !loaded.identity.allow_insecure_node_rpc_without_mtls,
        journal_db_path = %loaded.storage.journal_db_path.display(),
        journal_hash_chain_enabled = loaded.storage.journal_hash_chain_enabled,
        journal_max_payload_bytes = loaded.storage.max_journal_payload_bytes,
        storage_vault_dir = %loaded.storage.vault_dir.display(),
        identity_store_root = %identity_runtime.store_root.display(),
        revoked_certificate_count = identity_runtime.revoked_certificate_count,
        "gateway startup"
    );

    let started_at = Instant::now();
    let state = AppState {
        started_at,
        runtime: runtime.clone(),
        auth_runtime: Arc::clone(&auth_runtime),
        auth: auth.clone(),
        admin_rate_limit: Arc::new(Mutex::new(HashMap::new())),
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
        .route_layer(middleware::from_fn_with_state(state.clone(), admin_rate_limit_middleware));
    let app =
        Router::new().route("/healthz", get(health_handler)).merge(admin_routes).with_state(state);

    let admin_address = parse_daemon_bind_socket(&loaded.daemon.bind_addr, loaded.daemon.port)
        .context("invalid admin bind address or port")?;
    let grpc_address =
        parse_daemon_bind_socket(&loaded.gateway.grpc_bind_addr, loaded.gateway.grpc_port)
            .context("invalid gRPC bind address or port")?;
    enforce_remote_bind_guard(
        admin_address,
        grpc_address,
        loaded.gateway.allow_insecure_remote,
        loaded.gateway.tls.enabled,
        !loaded.identity.allow_insecure_node_rpc_without_mtls,
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
        node_rpc_mtls_required = !loaded.identity.allow_insecure_node_rpc_without_mtls,
        "node RPC listener initialized"
    );
    if loaded.gateway.quic_enabled {
        info!(
            quic_bind_addr = %loaded.gateway.quic_bind_addr,
            quic_port = loaded.gateway.quic_port,
            "gateway QUIC transport configured for upcoming runtime integration"
        );
    }

    let scheduler_wake = Arc::new(Notify::new());
    let grpc_url = loopback_grpc_url(grpc_bound, loaded.gateway.tls.enabled);
    let _cron_scheduler_task = spawn_scheduler_loop(
        runtime.clone(),
        auth.clone(),
        grpc_url.clone(),
        Arc::clone(&scheduler_wake),
    );

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
    let node_rpc_service = node_rpc::NodeRpcServiceImpl::new(
        identity_runtime.revoked_certificate_fingerprints.clone(),
        !loaded.identity.allow_insecure_node_rpc_without_mtls,
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
        .tls_config(build_node_rpc_tls_config(
            &identity_runtime,
            !loaded.identity.allow_insecure_node_rpc_without_mtls,
        ))
        .context("failed to apply node RPC TLS configuration")?;

    tokio::try_join!(
        async move {
            axum::serve(admin_listener, app.into_make_service_with_connect_info::<SocketAddr>())
                .with_graceful_shutdown(shutdown_signal())
                .await
                .context("palyrad admin server failed")
        },
        async move {
            grpc_server_builder
                .add_service(grpc_gateway_server)
                .add_service(grpc_cron_server)
                .add_service(grpc_approvals_server)
                .add_service(grpc_memory_server)
                .add_service(grpc_vault_server)
                .add_service(grpc_auth_server)
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(grpc_listener),
                    shutdown_signal(),
                )
                .await
                .context("palyrad gRPC server failed")
        },
        async move {
            node_rpc_server_builder
                .add_service(node_rpc_server)
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(node_rpc_listener),
                    shutdown_signal(),
                )
                .await
                .context("palyrad node RPC server failed")
        },
    )?;

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
    allow_insecure_remote: bool,
    gateway_tls_enabled: bool,
    node_rpc_mtls_required: bool,
    dangerous_remote_bind_acknowledged: bool,
) -> Result<()> {
    let admin_remote = !admin_address.ip().is_loopback();
    let grpc_remote = !grpc_address.ip().is_loopback();
    if (admin_remote || grpc_remote) && !allow_insecure_remote {
        anyhow::bail!(
            "refusing non-loopback bind without explicit insecure opt-in: admin={} grpc={} (set gateway.allow_insecure_remote=true or PALYRA_GATEWAY_ALLOW_INSECURE_REMOTE=true to override)",
            admin_address,
            grpc_address
        );
    }
    let requires_danger_ack =
        admin_remote || (grpc_remote && (!gateway_tls_enabled || !node_rpc_mtls_required));
    if requires_danger_ack && !dangerous_remote_bind_acknowledged {
        anyhow::bail!(
            "refusing insecure remote bind without explicit danger acknowledgement: admin={} grpc={} gateway_tls_enabled={} node_rpc_mtls_required={} (set {}=true to acknowledge risk, or keep admin loopback and enable gateway TLS and node RPC mTLS)",
            admin_address,
            grpc_address,
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
        runtime_status_response, validate_admin_auth_config, ADMIN_RATE_LIMIT_MAX_IP_BUCKETS,
        ADMIN_RATE_LIMIT_MAX_REQUESTS_PER_WINDOW,
    };
    use crate::gateway::GatewayAuthConfig;

    #[test]
    fn remote_bind_guard_allows_loopback_without_opt_in() {
        let result = enforce_remote_bind_guard(
            "127.0.0.1:7142".parse().expect("loopback endpoint should parse"),
            "127.0.0.1:7443".parse().expect("loopback endpoint should parse"),
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
}

async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::error!(error = %error, "failed to register Ctrl+C handler");
        std::future::pending::<()>().await;
    }
}
