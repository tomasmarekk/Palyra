//! Palyra daemon (`palyrad`): the gateway runtime that hosts the admin/console
//! HTTP surface, the gateway and node-RPC gRPC listeners, optional QUIC
//! transport, and every background subsystem in a single process.
//!
//! # Entry points
//!
//! [`run`] is the normal daemon entry point. Before constructing Tokio, the `palyrad` binary
//! invokes a hidden exact-argv dispatcher used by the trusted Unix process supervisor and its gated
//! target launcher. `run` loads config (`app::bootstrap`), wires the subsystems below, enforces the remote-bind security
//! guard, then serves HTTP and gRPC until Ctrl+C.
//!
//! # Module map
//!
//! - [`app`]: bootstrap, logging, shared `app::state::AppState`, shutdown.
//! - `transport`: HTTP router/handlers/middleware and gRPC/QUIC servers.
//! - `application` / [`domain`]: use-case services and domain invariants.
//! - `gateway`: gateway runtime state, runtime config snapshots, auth config.
//! - `journal`: SQLite event journal, memory/workspace storage, approvals.
//! - `orchestrator`: run lifecycle state machine shared by streaming surfaces.
//! - `channels`, `channel_router`, `webhooks`, `routines`, `objectives`:
//!   connector platform and scheduled/queued work.
//! - `model_provider`, `openai_auth`, `openai_surface`, `provider_leases`:
//!   model provider runtime and auth-profile credential flows.
//! - `sandbox_runner`, `wasm_plugin_runner`, `tool_protocol`, `tool_posture`:
//!   tool execution backends and their fail-closed policies.
//! - `access_control`, `acp`, `node_runtime`, `quic_runtime`, `maintenance`,
//!   `observability`, `self_healing`, `usage_governance`: supporting runtimes.
//!
//! Besides [`run`], this crate root hosts the startup helpers (identity/vault/
//! secret resolution, bind guard) plus the serde DTOs for the console/admin
//! HTTP surface, which are crate-private but referenced from the `transport`
//! handler modules.

#![recursion_limit = "256"]

#[cfg(test)]
pub(crate) mod test_env {
    use std::sync::{Mutex, MutexGuard, OnceLock};

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    pub(crate) fn lock() -> MutexGuard<'static, ()> {
        ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("test env lock should not be poisoned")
    }
}

#[cfg(test)]
mod planning_objective_regressions;

mod acceptance;
mod access_control;
mod acp;
mod agents;
pub mod app;
pub mod application;
mod automation;
mod auxiliary_executor;
mod background_queue;
mod channel_router;
mod channels;
mod command_router;
mod commitments;
mod config;
mod config_watcher;
mod cron;
mod delegation;
pub mod domain;
mod execution_backends;
mod feature_rollout_maturity;
mod feature_usage;
mod flows;
mod gateway;
mod hooks;
pub mod infra;
mod journal;
mod maintenance;
mod media;
mod media_derived;
mod metadata_trace;
mod method_registry;
mod model_provider;
mod node_rpc;
mod node_runtime;
mod objective_judge;
mod objectives;
mod observability;
mod openai_auth;
mod openai_model_discovery;
mod openai_surface;
mod orchestrator;
mod plugins;
mod provider_leases;
mod qa_fault_injection;
mod quic_runtime;
mod realtime;
#[allow(dead_code)]
mod replay_capture;
mod retrieval;
mod routines;
mod runtime_diagnostics;
mod runtime_preview_controls;
mod sandbox_runner;
mod self_healing;
pub mod support;
mod task_runtime;
mod tool_posture;
mod tool_protocol;
pub mod transport;
#[cfg(unix)]
mod unix_process_supervisor;
mod usage_governance;
mod wasm_plugin_runner;
mod webhooks;

use std::{
    collections::{BTreeMap, HashMap},
    convert::Infallible,
    fs,
    net::{IpAddr, SocketAddr},
    path::{Path as FsPath, PathBuf},
    process::Stdio,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};

/// Dispatches the exact hidden Unix process modes before any async runtime initialization.
///
/// Normal daemon invocations return immediately. A matching hidden invocation never returns.
#[doc(hidden)]
pub fn dispatch_internal_process_supervisor() {
    #[cfg(unix)]
    unix_process_supervisor::dispatch_if_requested();
}

/// Dispatches the exact hidden Codex bridge before normal daemon initialization.
///
/// Normal daemon invocations return immediately. A matching hidden invocation never returns.
#[doc(hidden)]
pub fn dispatch_internal_codex_app_server_bridge() {
    application::codex_app_server_bridge::dispatch_internal_codex_app_server_bridge();
}
use app::{
    bootstrap::load_runtime_bootstrap,
    logging::init_tracing,
    runtime::{build_app_state, loopback_grpc_url, AppStateBuildContext},
    state::{
        AppState, ConsoleActionContext, ConsoleChatRunStream, ConsoleRelayToken, ConsoleSession,
        OpenAiOAuthAttempt,
    },
};
use application::auth::record_auth_refresh_journal_event;
use axum::{
    body::{Body, Bytes},
    extract::{Path, Query, State},
    http::{
        header::{AUTHORIZATION, CACHE_CONTROL, CONTENT_TYPE, COOKIE, SET_COOKIE},
        HeaderMap, HeaderValue, StatusCode,
    },
    response::{Html, IntoResponse, Response},
    Json,
};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use cron::{spawn_scheduler_loop, MEMORY_MAINTENANCE_INTERVAL};
use gateway::{
    GatewayJournalConfigSnapshot, GatewayRuntimeConfigSnapshot, GatewayRuntimeState,
    LearningRuntimeConfig, MemoryRuntimeConfig, RoutinesRuntimeConfig,
};
use journal::{
    ApprovalDecision, ApprovalDecisionScope, ApprovalSubjectType, CronJobUpdatePatch,
    JournalAppendRequest, JournalConfig, JournalError, JournalStore, MemoryPurgeRequest,
    OrchestratorCancelRequest, OrchestratorRunStatusSnapshot, RuntimeHealthProbeReconciliationMode,
    SkillExecutionStatus, SkillStatusRecord, SkillStatusUpsertRequest,
};
use model_provider::{
    build_model_provider, ModelProviderAuthProviderKind, ModelProviderConfig,
    ModelProviderCredentialSource, ModelProviderKind, ProviderRegistryEntryConfig,
};
use observability::{
    CorrelationSnapshot as ObservabilityCorrelationSnapshot, FailureClass, ObservabilityState,
};
use openai_auth::{
    build_authorization_url, exchange_authorization_code, generate_pkce_verifier, normalize_scopes,
    oauth_endpoint_config_from_env, pkce_challenge, render_callback_page, revoke_openai_token,
    validate_openai_bearer_token, OpenAiCredentialValidationError, OpenAiOAuthAttemptStateRecord,
    OPENAI_OAUTH_ATTEMPT_TTL_MS, OPENAI_OAUTH_CALLBACK_EVENT_TYPE,
};
use openai_surface::{
    clear_model_provider_auth_profile_selection_if_matches, complete_openai_oauth_callback,
    connect_anthropic_api_key, connect_anthropic_oauth_tokens, connect_minimax_api_key,
    connect_openai_api_key, connect_xai_oauth_tokens, load_minimax_oauth_callback_state,
    load_openai_oauth_callback_state, reconnect_minimax_oauth_attempt,
    reconnect_openai_oauth_attempt, refresh_anthropic_oauth_profile, refresh_minimax_oauth_profile,
    refresh_openai_oauth_profile, refresh_xai_oauth_profile, revoke_anthropic_auth_profile,
    revoke_minimax_auth_profile, revoke_openai_auth_profile, revoke_xai_auth_profile,
    select_default_anthropic_auth_profile, select_default_minimax_auth_profile,
    select_default_openai_auth_profile, select_default_xai_auth_profile,
    start_minimax_oauth_attempt_from_request, start_openai_oauth_attempt_from_request,
};
use palyra_auth::{
    AuthCredential, AuthProfileError, AuthProfileRecord, AuthProfileRegistry, AuthProfileScope,
    AuthProviderKind, HttpOAuthRefreshAdapter, OAuthRefreshAdapter, OAuthRefreshOutcomeKind,
    OAuthRefreshState,
};
use palyra_common::{
    build_metadata,
    config_system::{
        backup_path, get_value_at_path, parse_document_with_migration, parse_toml_value_literal,
        recover_config_from_backup, serialize_document_pretty, set_value_at_path,
        unset_value_at_path, write_document_with_backups, ConfigMigrationInfo,
    },
    daemon_config_schema::{redact_secret_config_values, RootFileConfig},
    default_config_search_paths, parse_config_path, parse_daemon_bind_socket,
    redaction::{
        is_sensitive_key as redaction_key_is_sensitive, redact_auth_error, redact_url,
        redact_url_segments_in_text,
    },
    runtime_contracts::{PalyraErrorCategory, PalyraErrorEnvelope, PalyraValidationIssue},
    secret_refs::{SecretRef, SecretSource},
    validate_canonical_id,
};
use palyra_common::{
    default_identity_store_root, default_state_root,
    versioned_json::{
        migrate_updated_at_metadata_v0_to_v1, parse_versioned_json, VersionedJsonFormat,
    },
};
use palyra_connectors::providers::discord::{
    discord_min_invite_permissions, discord_required_permission_labels,
    resolve_discord_intents_from_flags, DiscordPrivilegedIntentStatus,
    DISCORD_PERMISSION_ATTACH_FILES, DISCORD_PERMISSION_EMBED_LINKS,
    DISCORD_PERMISSION_READ_MESSAGE_HISTORY, DISCORD_PERMISSION_SEND_MESSAGES,
    DISCORD_PERMISSION_SEND_MESSAGES_IN_THREADS, DISCORD_PERMISSION_VIEW_CHANNEL,
};
#[cfg(test)]
use palyra_connectors::providers::discord::{
    discord_required_permissions, DISCORD_APP_FLAG_GATEWAY_GUILD_MEMBERS,
    DISCORD_APP_FLAG_GATEWAY_MESSAGE_CONTENT, DISCORD_APP_FLAG_GATEWAY_PRESENCE,
};
use palyra_connectors::{
    ConnectorMessageDeleteRequest, ConnectorMessageEditRequest, ConnectorMessageLocator,
    ConnectorMessageReactionRequest, ConnectorMessageReadRequest, ConnectorMessageSearchRequest,
};
use palyra_control_plane as control_plane;
use palyra_identity::{FilesystemSecretStore, IdentityManager, SecretStore};
use palyra_policy::{
    evaluate_with_config, evaluate_with_context, PolicyDecision, PolicyEvaluationConfig,
    PolicyRequest, PolicyRequestContext,
};
use palyra_skills::{
    audit_skill_artifact_security, inspect_skill_artifact, verify_skill_artifact,
    SkillAuditCheckStatus, SkillCapabilities, SkillCompat, SkillEntrypoints,
    SkillFilesystemCapabilities, SkillIntegrity, SkillManifest, SkillQuotaConfig,
    SkillSecurityAuditPolicy, SkillToolEntrypoint, SkillToolRisk, SkillTrustStore,
    SKILL_MANIFEST_VERSION,
};
use palyra_vault::{
    SecretResolutionStatus, SecretResolveErrorKind, SecretResolver, Vault,
    VaultConfig as VaultConfigOptions, VaultRef, VaultScope,
};
use reqwest::{Client as ReqwestClient, Url};
use retrieval::{
    build_memory_embedding_runtime_selection, ExternalDerivedRetrievalBackend,
    ExternalRetrievalRuntime,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::process::Command as TokioCommand;
use tokio::sync::{mpsc, Notify};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{
    metadata::MetadataValue,
    transport::{Certificate, Identity, ServerTlsConfig},
    Request as TonicRequest,
};
use tracing::{info, warn};
use transport::grpc::auth::{
    authorize_headers, request_context_from_headers, AuthError, GatewayAuthConfig, RequestContext,
};
use ulid::Ulid;

#[cfg(test)]
pub(crate) use crate::application::channels::providers::discord::{
    build_discord_channel_permission_warnings, build_discord_inbound_monitor_warnings,
    build_discord_onboarding_plan, build_discord_onboarding_security_defaults,
    discord_inbound_monitor_is_alive, finalize_discord_onboarding_plan, normalize_discord_token,
    normalize_optional_discord_channel_id, summarize_discord_inbound_monitor,
};
use crate::gateway::proto::palyra::{
    browser::v1 as browser_v1, common::v1 as common_v1, cron::v1 as cron_v1,
    gateway::v1 as gateway_v1,
};
pub(crate) use crate::transport::http::contracts::channels::discord::{
    DiscordAccountLifecycleActionRequest, DiscordAccountLifecycleRequest,
    DiscordOnboardingPreflightResponse, DiscordOnboardingRequest,
};
#[cfg(test)]
pub(crate) use crate::transport::http::contracts::channels::discord::{
    DiscordBotIdentitySummary, DiscordChannelPermissionCheck, DiscordChannelPermissionCheckStatus,
    DiscordOnboardingScope,
};
pub(crate) use crate::transport::http::handlers::admin::skills::skill_status_response;
#[cfg(test)]
pub(crate) use crate::transport::http::handlers::canvas::{
    validate_canvas_http_canvas_id, validate_canvas_http_token_query,
};
pub(crate) use crate::transport::http::handlers::console::browser::{
    apply_browser_service_auth, build_console_browser_client, constant_time_eq_bytes,
    find_hashed_secret_map_key, mint_console_secret_token,
};
#[cfg(test)]
pub(crate) use crate::transport::http::handlers::console::browser::{
    clamp_console_relay_token_ttl_ms, mint_console_relay_token, prune_console_relay_tokens,
};
pub(crate) use crate::transport::http::handlers::console::chat::{
    lock_console_chat_streams, sync_console_chat_approval_to_stream,
};
pub(crate) use crate::transport::http::handlers::console::cron::{
    apply_console_request_context, apply_console_rpc_context,
};
pub(crate) use crate::transport::http::handlers::console::diagnostics::*;
#[cfg(test)]
pub(crate) use crate::transport::http::middleware::{
    consume_admin_auth_failure_rate_limit_with_now, consume_admin_rate_limit_with_now,
    consume_canvas_rate_limit_with_now,
};

const DANGEROUS_REMOTE_BIND_ACK_ENV: &str = "PALYRA_GATEWAY_DANGEROUS_REMOTE_BIND_ACK";
const SYSTEM_DAEMON_PRINCIPAL: &str = "system:daemon";
const SYSTEM_VAULT_CHANNEL: &str = "system:vault";
const SYSTEM_DAEMON_DEVICE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
const GRPC_MAX_DECODING_MESSAGE_SIZE_BYTES: usize = 4 * 1024 * 1024;
const GRPC_MAX_ENCODING_MESSAGE_SIZE_BYTES: usize = 4 * 1024 * 1024;
/// Fixed-window length for per-IP admin API rate limiting.
pub(crate) const ADMIN_RATE_LIMIT_WINDOW_MS: u64 = 1_000;
/// Admin API requests allowed per window for non-loopback clients.
pub(crate) const ADMIN_RATE_LIMIT_MAX_REQUESTS_PER_WINDOW: u32 = 30;
/// Looser admin budget for loopback clients (local desktop/CLI bursts).
pub(crate) const ADMIN_RATE_LIMIT_LOOPBACK_MAX_REQUESTS_PER_WINDOW: u32 = 1_000;
/// Failed admin auth attempts allowed per IP per window before lockout.
pub(crate) const ADMIN_AUTH_FAILURE_RATE_LIMIT_MAX_REQUESTS_PER_WINDOW: u32 = 30;
/// Cap on tracked admin rate-limit IP buckets (oldest evicted beyond this).
pub(crate) const ADMIN_RATE_LIMIT_MAX_IP_BUCKETS: usize = 4_096;
/// Fixed-window length for per-IP canvas HTTP rate limiting.
pub(crate) const CANVAS_RATE_LIMIT_WINDOW_MS: u64 = 1_000;
/// Canvas HTTP requests allowed per IP per window.
pub(crate) const CANVAS_RATE_LIMIT_MAX_REQUESTS_PER_WINDOW: u32 = 90;
/// Cap on tracked canvas rate-limit IP buckets (oldest evicted beyond this).
pub(crate) const CANVAS_RATE_LIMIT_MAX_IP_BUCKETS: usize = 4_096;
/// Maximum accepted canvas token query parameter size.
pub(crate) const CANVAS_HTTP_MAX_TOKEN_BYTES: usize = 8 * 1024;
/// Maximum accepted canvas id length (canonical ULIDs are 26 bytes).
pub(crate) const CANVAS_HTTP_MAX_CANVAS_ID_BYTES: usize = 64;
/// Request body cap applied across the admin/console HTTP surface.
pub(crate) const HTTP_MAX_REQUEST_BODY_BYTES: usize = 64 * 1024;
/// Larger bounded body cap for atomic flow dependency repair batches.
pub(crate) const FLOW_DEPENDENCY_REPAIR_MAX_REQUEST_BODY_BYTES: usize = 2 * 1024 * 1024;
const DISCORD_API_BASE: &str = "https://discord.com/api/v10";
const DISCORD_ONBOARDING_HTTP_TIMEOUT_MS: u64 = 5_000;
const DISCORD_ONBOARDING_CONFIG_BACKUPS: usize = 2;
const DISCORD_ONBOARDING_INBOUND_RECENT_WINDOW_MS: i64 = 15 * 60 * 1_000;
const DISCORD_ONBOARDING_MONITOR_WAIT_TIMEOUT_MS: u64 = 5_000;
const SMART_ROUTING_ENABLED_ENV: &str = "PALYRA_SMART_ROUTING_ENABLED";
const SMART_ROUTING_MODE_ENV: &str = "PALYRA_SMART_ROUTING_MODE";
const SMART_ROUTING_AUXILIARY_ENABLED_ENV: &str = "PALYRA_SMART_ROUTING_AUXILIARY_ENABLED";

/// Reads the smart-routing env toggles; unset or unrecognized values fall back
/// to the enabled/suggest defaults rather than failing startup.
fn load_smart_routing_runtime_config() -> usage_governance::SmartRoutingRuntimeConfig {
    let enabled = std::env::var(SMART_ROUTING_ENABLED_ENV)
        .ok()
        .and_then(|value| match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
        .unwrap_or(true);
    let default_mode = std::env::var(SMART_ROUTING_MODE_ENV)
        .ok()
        .and_then(|value| {
            usage_governance::RoutingMode::parse(value.as_str())
                .map(|mode| mode.as_str().to_owned())
        })
        .unwrap_or_else(|| usage_governance::RoutingMode::Suggest.as_str().to_owned());
    let auxiliary_routing_enabled = std::env::var(SMART_ROUTING_AUXILIARY_ENABLED_ENV)
        .ok()
        .and_then(|value| match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
        .unwrap_or(true);
    usage_governance::SmartRoutingRuntimeConfig { enabled, default_mode, auxiliary_routing_enabled }
}
const DISCORD_ONBOARDING_MONITOR_WAIT_POLL_MS: u64 = 250;
const CONSOLE_SESSION_COOKIE_NAME: &str = "palyra_console_session";
const CONSOLE_CSRF_HEADER_NAME: &str = "x-palyra-csrf-token";
const CONSOLE_SESSION_TTL_SECONDS: u64 = 30 * 60;
const CONSOLE_MAX_ACTIVE_SESSIONS: usize = 1_024;
const CONSOLE_RELAY_TOKEN_DEFAULT_TTL_MS: u64 = 5 * 60 * 1_000;
const CONSOLE_RELAY_TOKEN_MIN_TTL_MS: u64 = 30 * 1_000;
const CONSOLE_RELAY_TOKEN_MAX_TTL_MS: u64 = 30 * 60 * 1_000;
const CONSOLE_MAX_RELAY_TOKENS: usize = 4_096;
const CONSOLE_MAX_RELAY_EXTENSION_ID_BYTES: usize = 96;
const CONSOLE_MAX_RELAY_ACTION_PAYLOAD_BYTES: u64 = 32 * 1_024;
const SKILLS_LAYOUT_VERSION: u32 = 1;
const SKILLS_INDEX_FILE_NAME: &str = "installed-index.json";
const SKILL_ARTIFACT_FILE_NAME: &str = "artifact.palyra-skill";
const INSTALLED_SKILLS_INDEX_FORMAT: VersionedJsonFormat =
    VersionedJsonFormat::new("installed skills index", SKILLS_LAYOUT_VERSION);

// Serde DTOs for the console/admin HTTP surface follow. They are crate-root
// private on purpose: every `transport::http` handler module can reach them
// as `crate::<Name>` while nothing leaks into the public API. Field shapes
// mirror the `/console/v1` JSON contract; renaming fields is a breaking
// contract change.
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
struct RunWaitRequest {
    timeout_ms: Option<u64>,
    return_on_waiting: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct RunControlRequest {
    command: crate::application::turn_control::ControlCommand,
    active_phase: Option<crate::application::turn_control::ControlActivePhase>,
    session_id: Option<String>,
    queued_input_id: Option<String>,
    priority_lane: Option<String>,
    instruction: Option<String>,
    reason: Option<String>,
    dry_run: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct SkillStatusRequest {
    version: String,
    reason: Option<String>,
    #[serde(rename = "override")]
    override_enabled: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ConsoleProcedureSkillPromotionRequest {
    #[serde(default)]
    skill_id: Option<String>,
    #[serde(default)]
    version: Option<String>,
    #[serde(default)]
    publisher: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    accept_candidate: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ConsoleSkillBuilderCandidatesQuery {
    #[serde(default)]
    source_kind: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ConsoleSkillBuilderCapabilityRequest {
    #[serde(default)]
    http_hosts: Vec<String>,
    #[serde(default)]
    secrets: Vec<String>,
    #[serde(default)]
    storage_prefixes: Vec<String>,
    #[serde(default)]
    channels: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ConsoleSkillBuilderCreateRequest {
    #[serde(default)]
    learning_candidate_id: Option<String>,
    #[serde(default)]
    prompt: Option<String>,
    #[serde(default)]
    skill_id: Option<String>,
    #[serde(default)]
    version: Option<String>,
    #[serde(default)]
    publisher: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    tool_id: Option<String>,
    #[serde(default)]
    tool_name: Option<String>,
    #[serde(default)]
    tool_description: Option<String>,
    #[serde(default)]
    review_notes: Option<String>,
    #[serde(default)]
    capabilities: Option<ConsoleSkillBuilderCapabilityRequest>,
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

#[derive(Debug, Serialize)]
struct ConsoleSessionResponse {
    principal: String,
    device_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    channel: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    profile: Option<control_plane::ConsoleProfileContext>,
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
struct ConsoleAuthProfilesQuery {
    after_profile_id: Option<String>,
    limit: Option<u32>,
    provider_kind: Option<String>,
    provider_custom_name: Option<String>,
    scope_kind: Option<String>,
    scope_agent_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ConsoleAuthHealthQuery {
    agent_id: Option<String>,
    include_profiles: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ConsoleAuthRuntimeQuery {
    agent_id: Option<String>,
    provider_kind: Option<String>,
    provider_custom_name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ConsoleAuthSelectionExplainRequest {
    agent_id: Option<String>,
    provider_kind: Option<String>,
    provider_custom_name: Option<String>,
    #[serde(default)]
    explicit_profile_order: Vec<String>,
    #[serde(default)]
    allowed_credential_types: Vec<String>,
    #[serde(default)]
    policy_denied_profile_ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ConsoleAuthProfileOrderSetRequest {
    agent_id: Option<String>,
    provider_kind: Option<String>,
    provider_custom_name: Option<String>,
    profile_ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ConsoleOpenAiCallbackStateQuery {
    attempt_id: String,
}

#[derive(Debug, Deserialize)]
struct ConsoleOpenAiCallbackQuery {
    state: String,
    #[serde(default)]
    code: Option<String>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    error_description: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ConsoleSecretsListQuery {
    scope: String,
}

#[derive(Debug, Deserialize)]
struct ConsoleSecretMetadataQuery {
    scope: String,
    key: String,
}

#[derive(Debug, Deserialize)]
struct ConsoleSupportBundleJobsQuery {
    after_job_id: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct ConsoleDoctorJobsQuery {
    after_job_id: Option<String>,
    limit: Option<usize>,
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
struct ConsoleToolPermissionsQuery {
    #[serde(default)]
    scope_kind: Option<String>,
    #[serde(default)]
    scope_id: Option<String>,
    #[serde(default)]
    q: Option<String>,
    #[serde(default)]
    category: Option<String>,
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    locked_only: Option<bool>,
    #[serde(default)]
    high_friction_only: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ConsoleToolPostureOverrideRequest {
    scope_kind: String,
    #[serde(default)]
    scope_id: Option<String>,
    state: String,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    expires_at_unix_ms: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct ConsoleToolPostureResetRequest {
    scope_kind: String,
    #[serde(default)]
    scope_id: Option<String>,
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ConsoleToolPostureScopeResetRequest {
    scope_kind: String,
    #[serde(default)]
    scope_id: Option<String>,
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ConsoleToolPosturePresetPreviewRequest {
    preset_id: String,
    scope_kind: String,
    #[serde(default)]
    scope_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ConsoleToolPosturePresetApplyRequest {
    preset_id: String,
    scope_kind: String,
    #[serde(default)]
    scope_id: Option<String>,
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ConsoleToolPostureRecommendationActionRequest {
    recommendation_id: String,
    tool_name: String,
    scope_kind: String,
    #[serde(default)]
    scope_id: Option<String>,
    action: String,
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
struct ConsoleMemoryProviderExplainQuery {
    query: String,
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    agent_id: Option<String>,
    #[serde(default)]
    workspace_prefix: Option<String>,
    #[serde(default)]
    top_k: Option<usize>,
    #[serde(default)]
    min_score: Option<f64>,
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
struct ConsoleMemoryIndexRequest {
    #[serde(default)]
    batch_size: Option<usize>,
    #[serde(default)]
    until_complete: Option<bool>,
    #[serde(default)]
    cancel_after_batches: Option<u64>,
    #[serde(default)]
    run_maintenance: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ConsoleLearningCandidatesQuery {
    #[serde(default)]
    candidate_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    candidate_kind: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    risk_level: Option<String>,
    #[serde(default)]
    scope_kind: Option<String>,
    #[serde(default)]
    scope_id: Option<String>,
    #[serde(default)]
    source_task_id: Option<String>,
    #[serde(default)]
    min_confidence: Option<f64>,
    #[serde(default)]
    max_confidence: Option<f64>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct ConsoleLearningGraphQuery {
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    include_artifacts: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ConsoleLearningCandidateReviewRequest {
    status: String,
    #[serde(default)]
    action_summary: Option<String>,
    #[serde(default)]
    action_payload_json: Option<String>,
    #[serde(default)]
    apply_preference: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ConsoleLearningCandidateEvalRequest {
    eval_suite: String,
    result: String,
    threshold: f64,
    score: f64,
    decision: String,
    #[serde(default)]
    policy_decision: Option<String>,
    #[serde(default)]
    evidence_refs_json: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ConsoleLearningCandidateApplyRequest {
    #[serde(default)]
    action_summary: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ConsoleLearningPreferencesQuery {
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    scope_kind: Option<String>,
    #[serde(default)]
    scope_id: Option<String>,
    #[serde(default)]
    key: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct ConsoleWorkspaceDocumentsQuery {
    #[serde(default)]
    prefix: Option<String>,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    agent_id: Option<String>,
    #[serde(default)]
    include_deleted: Option<bool>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct ConsoleWorkspaceDocumentQuery {
    path: String,
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    agent_id: Option<String>,
    #[serde(default)]
    include_deleted: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ConsoleWorkspaceDocumentWriteRequest {
    #[serde(default)]
    document_id: Option<String>,
    path: String,
    #[serde(default)]
    title: Option<String>,
    content_text: String,
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    agent_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    template_id: Option<String>,
    #[serde(default)]
    template_version: Option<i64>,
    #[serde(default)]
    template_content_hash: Option<String>,
    #[serde(default)]
    source_memory_id: Option<String>,
    #[serde(default)]
    manual_override: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ConsoleWorkspaceDocumentMoveRequest {
    path: String,
    next_path: String,
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    agent_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ConsoleWorkspaceDocumentDeleteRequest {
    path: String,
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    agent_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ConsoleWorkspaceDocumentPinRequest {
    path: String,
    pinned: bool,
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    agent_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ConsoleWorkspaceDocumentVersionsQuery {
    path: String,
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    agent_id: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct ConsoleWorkspaceBootstrapRequest {
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    agent_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    force_repair: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ConsoleWorkspaceSearchQuery {
    query: String,
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    agent_id: Option<String>,
    #[serde(default)]
    prefix: Option<String>,
    #[serde(default)]
    top_k: Option<usize>,
    #[serde(default)]
    min_score: Option<f64>,
    #[serde(default)]
    include_historical: Option<bool>,
    #[serde(default)]
    include_quarantined: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ConsoleRecallPreviewRequest {
    query: String,
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    agent_id: Option<String>,
    #[serde(default)]
    memory_top_k: Option<usize>,
    #[serde(default)]
    workspace_top_k: Option<usize>,
    #[serde(default)]
    min_score: Option<f64>,
    #[serde(default)]
    workspace_prefix: Option<String>,
    #[serde(default)]
    include_workspace_historical: Option<bool>,
    #[serde(default)]
    include_workspace_quarantined: Option<bool>,
    #[serde(default)]
    max_candidates: Option<usize>,
    #[serde(default)]
    prompt_budget_tokens: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct ConsoleSearchAllQuery {
    q: String,
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    agent_id: Option<String>,
    #[serde(default)]
    top_k: Option<usize>,
    #[serde(default)]
    min_score: Option<f64>,
    #[serde(default)]
    workspace_prefix: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ConsoleSessionSearchQuery {
    q: String,
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    top_k: Option<usize>,
    #[serde(default)]
    min_score: Option<f64>,
    #[serde(default)]
    window_before: Option<usize>,
    #[serde(default)]
    window_after: Option<usize>,
    #[serde(default)]
    max_windows_per_session: Option<usize>,
    #[serde(default)]
    include_archived: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ConsoleRecallArtifactsQuery {
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct ChannelLogsQuery {
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct ChannelIngressQuery {
    limit: Option<usize>,
    status: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ChannelIngressPath {
    connector_id: String,
    ingress_event_id: i64,
}

#[derive(Debug, Deserialize)]
struct ChannelDeliveryQuery {
    limit: Option<usize>,
    status: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ChannelDeliveryPath {
    intent_id: String,
}

#[derive(Debug, Deserialize)]
struct ChannelLogsRequest {
    connector_id: String,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct ChannelEnabledRequest {
    enabled: bool,
}

#[derive(Debug, Deserialize)]
struct ChannelTestRequest {
    text: String,
    #[serde(default)]
    conversation_id: Option<String>,
    #[serde(default)]
    sender_id: Option<String>,
    #[serde(default)]
    sender_display: Option<String>,
    #[serde(default)]
    simulate_crash_once: Option<bool>,
    #[serde(default)]
    is_direct_message: Option<bool>,
    #[serde(default)]
    requested_broadcast: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ChannelTestSendRequest {
    target: String,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    confirm: Option<bool>,
    #[serde(default)]
    auto_reaction: Option<String>,
    #[serde(default)]
    thread_id: Option<String>,
    #[serde(default)]
    reply_to_message_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ChannelHealthRefreshRequest {
    #[serde(default)]
    verify_channel_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ChannelMessageReadBody {
    #[serde(flatten)]
    request: ConnectorMessageReadRequest,
}

#[derive(Debug, Deserialize)]
struct ChannelMessageSearchBody {
    #[serde(flatten)]
    request: ConnectorMessageSearchRequest,
}

#[derive(Debug, Deserialize)]
struct ChannelMessageEditBody {
    #[serde(flatten)]
    request: ConnectorMessageEditRequest,
    #[serde(default)]
    approval_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ChannelMessageDeleteBody {
    #[serde(flatten)]
    request: ConnectorMessageDeleteRequest,
    #[serde(default)]
    approval_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ChannelMessageReactionBody {
    #[serde(flatten)]
    request: ConnectorMessageReactionRequest,
    #[serde(default)]
    approval_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DeadLetterActionPath {
    connector_id: String,
    dead_letter_id: i64,
}

#[derive(Debug, Deserialize)]
struct ChannelRouterPreviewRequest {
    channel: String,
    text: String,
    #[serde(default)]
    conversation_id: Option<String>,
    #[serde(default)]
    sender_identity: Option<String>,
    #[serde(default)]
    sender_display: Option<String>,
    #[serde(default)]
    sender_verified: Option<bool>,
    #[serde(default)]
    sender_roles: Option<Vec<String>>,
    #[serde(default)]
    is_direct_message: Option<bool>,
    #[serde(default)]
    requested_broadcast: Option<bool>,
    #[serde(default)]
    adapter_message_id: Option<String>,
    #[serde(default)]
    adapter_thread_id: Option<String>,
    #[serde(default)]
    max_payload_bytes: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ChannelRouterPairingCodeMintRequest {
    channel: String,
    #[serde(default)]
    issued_by: Option<String>,
    #[serde(default)]
    ttl_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ChannelRouterPairingsQuery {
    #[serde(default)]
    channel: Option<String>,
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

#[derive(Debug, Deserialize)]
struct ConsoleBrowserProfilesQuery {
    principal: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ConsoleBrowserCreateProfileRequest {
    principal: Option<String>,
    name: String,
    #[serde(default)]
    theme_color: Option<String>,
    #[serde(default)]
    persistence_enabled: Option<bool>,
    #[serde(default)]
    private_profile: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ConsoleBrowserRenameProfileRequest {
    principal: Option<String>,
    name: String,
}

#[derive(Debug, Deserialize)]
struct ConsoleBrowserProfileScopeRequest {
    principal: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ConsoleBrowserCreateSessionRequest {
    principal: Option<String>,
    #[serde(default)]
    idle_ttl_ms: Option<u64>,
    #[serde(default)]
    budget: Option<control_plane::BrowserSessionBudget>,
    #[serde(default)]
    allow_private_targets: Option<bool>,
    #[serde(default)]
    allow_downloads: Option<bool>,
    #[serde(default)]
    action_allowed_domains: Vec<String>,
    #[serde(default)]
    persistence_enabled: Option<bool>,
    #[serde(default)]
    persistence_id: Option<String>,
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    profile_id: Option<String>,
    #[serde(default)]
    private_profile: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ConsoleBrowserNavigateRequest {
    url: String,
    #[serde(default)]
    timeout_ms: Option<u64>,
    #[serde(default)]
    allow_redirects: Option<bool>,
    #[serde(default)]
    max_redirects: Option<u32>,
    #[serde(default)]
    allow_private_targets: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ConsoleBrowserClickRequest {
    selector: String,
    #[serde(default)]
    max_retries: Option<u32>,
    #[serde(default)]
    timeout_ms: Option<u64>,
    #[serde(default)]
    capture_failure_screenshot: Option<bool>,
    #[serde(default)]
    max_failure_screenshot_bytes: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ConsoleBrowserTypeRequest {
    selector: String,
    #[serde(default)]
    text: String,
    #[serde(default)]
    clear_existing: Option<bool>,
    #[serde(default)]
    timeout_ms: Option<u64>,
    #[serde(default)]
    capture_failure_screenshot: Option<bool>,
    #[serde(default)]
    max_failure_screenshot_bytes: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ConsoleBrowserPressRequest {
    key: String,
    #[serde(default)]
    timeout_ms: Option<u64>,
    #[serde(default)]
    capture_failure_screenshot: Option<bool>,
    #[serde(default)]
    max_failure_screenshot_bytes: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ConsoleBrowserSelectRequest {
    selector: String,
    value: String,
    #[serde(default)]
    timeout_ms: Option<u64>,
    #[serde(default)]
    capture_failure_screenshot: Option<bool>,
    #[serde(default)]
    max_failure_screenshot_bytes: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ConsoleBrowserHighlightRequest {
    selector: String,
    #[serde(default)]
    timeout_ms: Option<u64>,
    #[serde(default)]
    duration_ms: Option<u64>,
    #[serde(default)]
    capture_failure_screenshot: Option<bool>,
    #[serde(default)]
    max_failure_screenshot_bytes: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ConsoleBrowserScrollRequest {
    #[serde(default)]
    delta_x: Option<i64>,
    #[serde(default)]
    delta_y: Option<i64>,
    #[serde(default)]
    capture_failure_screenshot: Option<bool>,
    #[serde(default)]
    max_failure_screenshot_bytes: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ConsoleBrowserWaitForRequest {
    #[serde(default)]
    selector: Option<String>,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    timeout_ms: Option<u64>,
    #[serde(default)]
    poll_interval_ms: Option<u64>,
    #[serde(default)]
    capture_failure_screenshot: Option<bool>,
    #[serde(default)]
    max_failure_screenshot_bytes: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ConsoleBrowserTitleQuery {
    max_title_bytes: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ConsoleBrowserScreenshotQuery {
    max_bytes: Option<u64>,
    format: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ConsoleBrowserPdfQuery {
    max_bytes: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ConsoleBrowserSessionsQuery {
    #[serde(default)]
    principal: Option<String>,
    limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct ConsoleBrowserObserveQuery {
    include_dom_snapshot: Option<bool>,
    include_accessibility_tree: Option<bool>,
    include_visible_text: Option<bool>,
    max_dom_snapshot_bytes: Option<u64>,
    max_accessibility_tree_bytes: Option<u64>,
    max_visible_text_bytes: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ConsoleBrowserNetworkLogQuery {
    limit: Option<u32>,
    include_headers: Option<bool>,
    max_payload_bytes: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ConsoleBrowserConsoleLogQuery {
    limit: Option<u32>,
    minimum_severity: Option<control_plane::BrowserDiagnosticSeverity>,
    include_page_diagnostics: Option<bool>,
    max_payload_bytes: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ConsoleBrowserInspectSessionQuery {
    #[serde(default)]
    include_cookies: Option<bool>,
    #[serde(default)]
    include_storage: Option<bool>,
    #[serde(default)]
    include_action_log: Option<bool>,
    #[serde(default)]
    include_network_log: Option<bool>,
    #[serde(default)]
    include_page_snapshot: Option<bool>,
    #[serde(default)]
    include_console_log: Option<bool>,
    #[serde(default)]
    include_page_diagnostics: Option<bool>,
    max_cookie_bytes: Option<u64>,
    max_storage_bytes: Option<u64>,
    max_action_log_entries: Option<u32>,
    max_network_log_entries: Option<u32>,
    max_network_log_bytes: Option<u64>,
    max_dom_snapshot_bytes: Option<u64>,
    max_visible_text_bytes: Option<u64>,
    max_console_log_entries: Option<u32>,
    max_console_log_bytes: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ConsoleBrowserOpenTabRequest {
    url: String,
    #[serde(default)]
    activate: Option<bool>,
    #[serde(default)]
    timeout_ms: Option<u64>,
    #[serde(default)]
    allow_redirects: Option<bool>,
    #[serde(default)]
    max_redirects: Option<u32>,
    #[serde(default)]
    allow_private_targets: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ConsoleBrowserTabMutationRequest {
    tab_id: String,
}

#[derive(Debug, Deserialize)]
struct ConsoleBrowserTabCloseRequest {
    #[serde(default)]
    tab_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ConsoleBrowserSetPermissionsRequest {
    #[serde(default)]
    camera: Option<control_plane::BrowserPermissionSetting>,
    #[serde(default)]
    microphone: Option<control_plane::BrowserPermissionSetting>,
    #[serde(default)]
    location: Option<control_plane::BrowserPermissionSetting>,
    #[serde(default)]
    reset_to_default: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ConsoleBrowserResetStateRequest {
    #[serde(default)]
    clear_cookies: Option<bool>,
    #[serde(default)]
    clear_storage: Option<bool>,
    #[serde(default)]
    reset_tabs: Option<bool>,
    #[serde(default)]
    reset_permissions: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ConsoleBrowserRelayTokenRequest {
    session_id: String,
    extension_id: String,
    ttl_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ConsoleBrowserRelayOpenTabPayload {
    url: String,
    #[serde(default)]
    activate: Option<bool>,
    #[serde(default)]
    timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ConsoleBrowserRelayCaptureSelectionPayload {
    selector: String,
    #[serde(default)]
    max_selection_bytes: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ConsoleBrowserRelayPageSnapshotPayload {
    #[serde(default)]
    include_dom_snapshot: Option<bool>,
    #[serde(default)]
    include_visible_text: Option<bool>,
    #[serde(default)]
    max_dom_snapshot_bytes: Option<u64>,
    #[serde(default)]
    max_visible_text_bytes: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ConsoleBrowserRelayActionRequest {
    session_id: String,
    extension_id: String,
    action: String,
    #[serde(default)]
    open_tab: Option<ConsoleBrowserRelayOpenTabPayload>,
    #[serde(default)]
    capture_selection: Option<ConsoleBrowserRelayCaptureSelectionPayload>,
    #[serde(default)]
    page_snapshot: Option<ConsoleBrowserRelayPageSnapshotPayload>,
    #[serde(default)]
    max_payload_bytes: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ConsoleBrowserDownloadsQuery {
    session_id: String,
    limit: Option<u32>,
    #[serde(default)]
    quarantined_only: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ConsoleChatSessionsQuery {
    after_session_key: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct ConsoleChatSessionResolveRequest {
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    session_key: Option<String>,
    #[serde(default)]
    session_label: Option<String>,
    #[serde(default)]
    require_existing: Option<bool>,
    #[serde(default)]
    reset_session: Option<bool>,
}

#[derive(Debug, Deserialize, Default)]
struct ConsoleChatRenameSessionRequest {
    #[serde(default)]
    session_label: Option<String>,
    #[serde(default)]
    manual_title_locked: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ConsoleChatMessageRequest {
    text: String,
    #[serde(default)]
    session_label: Option<String>,
    #[serde(default)]
    allow_sensitive_tools: Option<bool>,
    #[serde(default)]
    origin_kind: Option<String>,
    #[serde(default)]
    origin_run_id: Option<String>,
    #[serde(default)]
    parameter_delta: Option<Value>,
    #[serde(default)]
    queued_input_id: Option<String>,
    #[serde(default)]
    attachments: Vec<ConsoleChatAttachmentReference>,
}

#[derive(Debug, Deserialize)]
struct ConsoleChatContextReferencePreviewRequest {
    text: String,
}

#[derive(Debug, Deserialize, Default)]
struct ConsoleChatProjectContextPreviewRequest {
    #[serde(default)]
    text: String,
}

#[derive(Debug, Deserialize)]
struct ConsoleChatRunEventsQuery {
    after_seq: Option<i64>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize, Default)]
struct ConsoleChatRunWorkspaceQuery {
    #[serde(default)]
    q: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize, Default)]
struct ConsoleChatWorkspaceArtifactQuery {
    #[serde(default)]
    include_content: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ConsoleChatQueueRequest {
    text: String,
    #[serde(default)]
    queue_mode: Option<String>,
    #[serde(default)]
    attachments: Vec<ConsoleChatAttachmentReference>,
}

#[derive(Debug, Deserialize, Default)]
struct ConsoleChatQueueControlRequest {
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    priority_lane: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ConsoleChatRetryRequest {
    #[serde(default)]
    parameter_delta: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct ConsoleChatBranchRequest {
    #[serde(default)]
    session_label: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct ConsoleSessionProjectContextScaffoldRequest {
    #[serde(default)]
    project_name: Option<String>,
    #[serde(default)]
    force: Option<bool>,
}

#[derive(Debug, Deserialize, Default)]
struct ConsoleChatCompactionRequest {
    #[serde(default)]
    trigger_reason: Option<String>,
    #[serde(default)]
    trigger_policy: Option<String>,
    #[serde(default)]
    operator_instruction: Option<String>,
    #[serde(default)]
    accept_candidate_ids: Vec<String>,
    #[serde(default)]
    reject_candidate_ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ConsoleChatCheckpointRequest {
    name: String,
    #[serde(default)]
    note: Option<String>,
    #[serde(default)]
    tags: Vec<String>,
}

#[derive(Debug, Deserialize, Default)]
struct ConsoleChatCheckpointRestoreRequest {
    #[serde(default)]
    session_label: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct ConsoleChatWorkspaceCompareRequest {
    #[serde(default)]
    left_run_id: Option<String>,
    #[serde(default)]
    right_run_id: Option<String>,
    #[serde(default)]
    left_checkpoint_id: Option<String>,
    #[serde(default)]
    right_checkpoint_id: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize, Default)]
struct ConsoleChatWorkspaceRestoreRequest {
    #[serde(default)]
    session_label: Option<String>,
    #[serde(default)]
    scope_kind: Option<String>,
    #[serde(default)]
    target_path: Option<String>,
    #[serde(default)]
    target_workspace_root_index: Option<u32>,
    #[serde(default)]
    branch_session: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ConsoleChatCanvasRestoreRequest {
    state_version: u64,
}

#[derive(Debug, Deserialize)]
struct ConsoleChatPinRequest {
    run_id: String,
    tape_seq: i64,
    title: String,
    #[serde(default)]
    note: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ConsoleChatTranscriptSearchQuery {
    q: String,
}

#[derive(Debug, Deserialize, Default)]
struct ConsoleChatDerivedArtifactsQuery {
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    state: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct ConsoleMemoryDerivedArtifactsQuery {
    #[serde(default)]
    workspace_document_id: Option<String>,
    #[serde(default)]
    memory_item_id: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize, Default)]
struct ConsoleDerivedArtifactLifecycleRequest {
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ConsoleChatTranscriptExportQuery {
    format: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ConsoleChatBackgroundTasksQuery {
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    include_completed: Option<bool>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize, Default)]
struct ConsoleChatBackgroundTaskCreateRequest {
    text: String,
    #[serde(default)]
    task_kind: Option<String>,
    #[serde(default)]
    priority: Option<i64>,
    #[serde(default)]
    max_attempts: Option<u64>,
    #[serde(default)]
    budget_tokens: Option<u64>,
    #[serde(default)]
    not_before_unix_ms: Option<i64>,
    #[serde(default)]
    expires_at_unix_ms: Option<i64>,
    #[serde(default)]
    notification_target: Option<Value>,
    #[serde(default)]
    parameter_delta: Option<Value>,
    #[serde(default)]
    delegation: Option<crate::delegation::DelegationRequestInput>,
}

#[derive(Debug, Deserialize)]
struct ConsoleChatAttachmentUploadRequest {
    filename: String,
    content_type: String,
    bytes_base64: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ConsoleChatAttachmentReference {
    artifact_id: String,
}

/// Persisted index of installed skills (`skills/installed-index.json`).
///
/// On-disk schema: changes must bump `SKILLS_LAYOUT_VERSION` and ship a
/// migration for `parse_versioned_json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InstalledSkillsIndex {
    schema_version: u32,
    updated_at_unix_ms: i64,
    #[serde(default)]
    entries: Vec<InstalledSkillRecord>,
}

const SKILL_BUILDER_CANDIDATE_LAYOUT_VERSION: u32 = 1;
const SKILL_BUILDER_CANDIDATE_INDEX_FORMAT: VersionedJsonFormat = VersionedJsonFormat::new(
    "skill builder candidate index",
    SKILL_BUILDER_CANDIDATE_LAYOUT_VERSION,
);

/// Persisted index of skill-builder candidates awaiting artifact/eval/review
/// gates; an on-disk schema like `InstalledSkillsIndex`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SkillBuilderCandidateIndex {
    schema_version: u32,
    updated_at_unix_ms: i64,
    #[serde(default)]
    entries: Vec<SkillBuilderCandidateRecord>,
}

impl Default for SkillBuilderCandidateIndex {
    fn default() -> Self {
        Self {
            schema_version: SKILL_BUILDER_CANDIDATE_LAYOUT_VERSION,
            updated_at_unix_ms: 0,
            entries: Vec::new(),
        }
    }
}

/// One generated-skill candidate: scaffold/manifest paths, gate statuses, and
/// the capability profile it would be granted if promoted.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SkillBuilderCandidateRecord {
    candidate_id: String,
    skill_id: String,
    version: String,
    publisher: String,
    name: String,
    source_kind: String,
    source_ref: String,
    summary: String,
    status: String,
    rollout_flag: String,
    rollout_enabled: bool,
    scaffold_root: String,
    manifest_path: String,
    capability_declaration_path: String,
    provenance_path: String,
    test_harness_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    artifact_plan_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    eval_outcome_path: Option<String>,
    #[serde(default = "default_builder_artifact_status")]
    artifact_status: String,
    #[serde(default = "default_builder_eval_status")]
    eval_status: String,
    #[serde(default = "default_builder_quarantine_reason")]
    quarantine_reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reproducibility_key: Option<String>,
    capability_profile: crate::plugins::PluginCapabilityProfile,
    generated_at_unix_ms: i64,
    updated_at_unix_ms: i64,
}

fn default_builder_artifact_status() -> String {
    "legacy_untracked".to_owned()
}

fn default_builder_eval_status() -> String {
    "legacy_untracked".to_owned()
}

fn default_builder_quarantine_reason() -> String {
    "generated skill remains quarantined until signed artifact, eval, and review pass".to_owned()
}

impl Default for InstalledSkillsIndex {
    fn default() -> Self {
        Self { schema_version: SKILLS_LAYOUT_VERSION, updated_at_unix_ms: 0, entries: Vec::new() }
    }
}

/// One installed skill version with its trust decision, hashes, security scan
/// snapshot, and optional rollback pointer. At most one record per `skill_id`
/// has `current == true` (enforced by `normalize_installed_skills_index`).
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
    #[serde(default)]
    security_scan: InstalledSkillSecuritySnapshot,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    rollback_snapshot: Option<InstalledSkillRollbackSnapshot>,
}

/// Where a skill artifact came from (for example a local path or registry ref).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InstalledSkillSource {
    kind: String,
    reference: String,
}

/// Persisted result of the install-time security audit for one skill payload.
/// The `Default` is deliberately fail-closed (`should_quarantine: true`) so
/// legacy records without a scan stay quarantined until re-audited.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct InstalledSkillSecuritySnapshot {
    schema_version: u32,
    accepted: bool,
    passed: bool,
    should_quarantine: bool,
    generated_at_unix_ms: i64,
    payload_sha256: String,
    trust_decision: String,
    check_count: usize,
    failed_checks: Vec<String>,
    warning_checks: Vec<String>,
    quarantine_reasons: Vec<String>,
    policy: SkillSecurityAuditPolicy,
}

impl Default for InstalledSkillSecuritySnapshot {
    fn default() -> Self {
        Self {
            schema_version: 1,
            accepted: false,
            passed: false,
            should_quarantine: true,
            generated_at_unix_ms: 0,
            payload_sha256: String::new(),
            trust_decision: "unknown".to_owned(),
            check_count: 0,
            failed_checks: Vec::new(),
            warning_checks: Vec::new(),
            quarantine_reasons: Vec::new(),
            policy: SkillSecurityAuditPolicy::default(),
        }
    }
}

/// Pointer to the previously current version, captured before an upgrade so
/// the operator can roll back.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct InstalledSkillRollbackSnapshot {
    schema_version: u32,
    previous_version: String,
    previous_artifact_sha256: String,
    previous_payload_sha256: String,
    captured_at_unix_ms: i64,
}

/// A vault secret a skill declares but that is not provisioned yet.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct MissingSkillSecret {
    scope: String,
    key: String,
}

/// Query carrying a canvas access token.
#[derive(Debug, Deserialize)]
pub(crate) struct CanvasTokenQuery {
    token: String,
}

/// Query addressing a specific canvas with its access token.
#[derive(Debug, Deserialize)]
pub(crate) struct CanvasRuntimeQuery {
    canvas_id: String,
    token: String,
}

/// Canvas state poll: returns state newer than `after_version` when set.
#[derive(Debug, Deserialize)]
pub(crate) struct CanvasStateQuery {
    token: String,
    after_version: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct PolicyExplainQuery {
    principal: String,
    action: String,
    resource: String,
    channel: Option<String>,
    session_id: Option<String>,
    run_id: Option<String>,
    device_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct PolicyExplainResponse {
    principal: String,
    action: String,
    resource: String,
    decision: String,
    approval_required: bool,
    runtime_approval_required: bool,
    runtime_approval_tool: Option<String>,
    reason: String,
    matched_policies: Vec<String>,
    diagnostics: serde_json::Value,
}

/// Identity material initialized at startup: the store root, gateway CA, the
/// node-RPC server certificate, and the shared [`IdentityManager`] handle.
#[derive(Clone)]
struct IdentityRuntime {
    store_root: PathBuf,
    revoked_certificate_count: usize,
    gateway_ca_certificate_pem: String,
    node_server_certificate: palyra_identity::IssuedCertificate,
    manager: Arc<Mutex<IdentityManager>>,
}

/// Journal payload describing one secret resolution; identifies the secret by
/// config path and fingerprint only, never by value.
#[derive(Debug, Clone)]
pub(crate) struct SecretAccessAuditRecord {
    action: String,
    config_path: String,
    secret_id: String,
    source_kind: String,
    resolved_at_unix_ms: i64,
}

/// Parses a boolean-like env value (`1/0`, `true/false`, `yes/no`, `on/off`;
/// empty means `false`); anything else is a startup error, not a default.
fn parse_offline_env_flag(raw: &str) -> Result<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" | "" => Ok(false),
        other => Err(anyhow::anyhow!(
            "PALYRA_OFFLINE must be a boolean-like value (accepted: 1/0, true/false, yes/no, on/off), got '{other}'"
        )),
    }
}

/// Reads `PALYRA_OFFLINE`; absent means online, malformed values fail startup.
fn offline_mode_enabled() -> Result<bool> {
    match std::env::var("PALYRA_OFFLINE") {
        Ok(raw) => parse_offline_env_flag(raw.as_str()),
        Err(std::env::VarError::NotPresent) => Ok(false),
        Err(std::env::VarError::NotUnicode(_)) => {
            Err(anyhow::anyhow!("PALYRA_OFFLINE contains non-unicode data"))
        }
    }
}

/// Builds the learning runtime config from defaults plus `PALYRA_LEARNING_*`
/// env overrides; out-of-range or non-numeric values fail startup loudly.
fn build_learning_runtime_config() -> Result<LearningRuntimeConfig> {
    let mut config = LearningRuntimeConfig::default();

    fn parse_bps_env(name: &str, raw: &str) -> Result<u16> {
        let value = raw
            .trim()
            .parse::<u16>()
            .with_context(|| format!("{name} must be integer 0-10000, got '{raw}'"))?;
        if value > 10_000 {
            return Err(anyhow::anyhow!("{name} must be between 0 and 10000, got {}", value));
        }
        Ok(value)
    }

    if let Ok(raw) = std::env::var("PALYRA_LEARNING_ENABLED") {
        config.enabled = parse_offline_env_flag(raw.as_str())
            .context("PALYRA_LEARNING_ENABLED must be boolean-like")?;
    }
    if let Ok(raw) = std::env::var("PALYRA_LEARNING_SAMPLING_PERCENT") {
        let value = raw.trim().parse::<u16>().with_context(|| {
            format!("PALYRA_LEARNING_SAMPLING_PERCENT must be integer 0-100, got '{raw}'")
        })?;
        if value > 100 {
            return Err(anyhow::anyhow!(
                "PALYRA_LEARNING_SAMPLING_PERCENT must be between 0 and 100, got {}",
                value
            ));
        }
        config.sampling_percent = value as u8;
    }
    if let Ok(raw) = std::env::var("PALYRA_LEARNING_COOLDOWN_MS") {
        config.cooldown_ms = raw.trim().parse::<i64>().with_context(|| {
            format!("PALYRA_LEARNING_COOLDOWN_MS must be integer milliseconds, got '{raw}'")
        })?;
    }
    if let Ok(raw) = std::env::var("PALYRA_LEARNING_BUDGET_TOKENS") {
        config.budget_tokens = raw.trim().parse::<u64>().with_context(|| {
            format!("PALYRA_LEARNING_BUDGET_TOKENS must be integer, got '{raw}'")
        })?;
    }
    if let Ok(raw) = std::env::var("PALYRA_LEARNING_MAX_CANDIDATES_PER_RUN") {
        config.max_candidates_per_run = raw.trim().parse::<usize>().with_context(|| {
            format!("PALYRA_LEARNING_MAX_CANDIDATES_PER_RUN must be integer, got '{raw}'")
        })?;
    }
    if let Ok(raw) = std::env::var("PALYRA_LEARNING_DURABLE_FACT_REVIEW_MIN_CONFIDENCE_BPS") {
        config.durable_fact_review_min_confidence_bps =
            parse_bps_env("PALYRA_LEARNING_DURABLE_FACT_REVIEW_MIN_CONFIDENCE_BPS", raw.as_str())?;
    }
    if let Ok(raw) = std::env::var("PALYRA_LEARNING_DURABLE_FACT_AUTO_WRITE_THRESHOLD_BPS") {
        config.durable_fact_auto_write_threshold_bps =
            parse_bps_env("PALYRA_LEARNING_DURABLE_FACT_AUTO_WRITE_THRESHOLD_BPS", raw.as_str())?;
    }
    if let Ok(raw) = std::env::var("PALYRA_LEARNING_PREFERENCE_REVIEW_MIN_CONFIDENCE_BPS") {
        config.preference_review_min_confidence_bps =
            parse_bps_env("PALYRA_LEARNING_PREFERENCE_REVIEW_MIN_CONFIDENCE_BPS", raw.as_str())?;
    }
    if let Ok(raw) = std::env::var("PALYRA_LEARNING_PROCEDURE_MIN_OCCURRENCES") {
        config.procedure_min_occurrences = raw.trim().parse::<usize>().with_context(|| {
            format!("PALYRA_LEARNING_PROCEDURE_MIN_OCCURRENCES must be integer, got '{raw}'")
        })?;
    }
    if let Ok(raw) = std::env::var("PALYRA_LEARNING_PROCEDURE_REVIEW_MIN_CONFIDENCE_BPS") {
        config.procedure_review_min_confidence_bps =
            parse_bps_env("PALYRA_LEARNING_PROCEDURE_REVIEW_MIN_CONFIDENCE_BPS", raw.as_str())?;
    }

    Ok(config)
}

const PROCESS_LEASE_RECONCILIATION_INTERVAL: Duration = Duration::from_secs(60);
const RUNTIME_HEALTH_RECONCILIATION_INTERVAL: Duration = Duration::from_secs(30);
const NETWORKED_WORKER_EXPIRY_INTERVAL: Duration = Duration::from_secs(15);

fn supervise_lifecycle_subsystem_task(
    runtime: &Arc<GatewayRuntimeState>,
    subsystem: application::daemon_lifecycle::LifecycleSubsystem,
    task: tokio::task::JoinHandle<()>,
) -> Result<tokio::task::JoinHandle<()>> {
    runtime
        .daemon_lifecycle
        .register_subsystem_task(subsystem, task.abort_handle())
        .map_err(|error| anyhow::anyhow!(error))
        .context("failed to register daemon lifecycle subsystem")?;
    let runtime = Arc::clone(runtime);
    Ok(tokio::spawn(async move {
        let outcome = task.await;
        if let Err(error) = &outcome {
            if !error.is_cancelled() {
                tracing::warn!(
                    subsystem = subsystem.as_str(),
                    message = %error,
                    "daemon lifecycle subsystem task failed"
                );
            }
        }
        if let Err(error) = runtime.daemon_lifecycle.acknowledge_subsystem_drained(subsystem) {
            tracing::warn!(
                subsystem = subsystem.as_str(),
                message = %error,
                "failed to acknowledge daemon lifecycle subsystem drain"
            );
        }
    }))
}

fn spawn_managed_coding_lifecycle(
    runtime: Arc<GatewayRuntimeState>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut lifecycle = runtime.daemon_lifecycle.subscribe();
        loop {
            let phase = lifecycle.borrow().phase;
            if matches!(
                phase,
                application::daemon_lifecycle::DaemonLifecyclePhase::DrainingSubsystems
                    | application::daemon_lifecycle::DaemonLifecyclePhase::ShutdownRequested
            ) {
                break;
            }
            if lifecycle.changed().await.is_err() {
                return;
            }
        }
        let shutdown_runtime = Arc::clone(&runtime);
        match tokio::task::spawn_blocking(move || {
            shutdown_runtime.shutdown_managed_coding_services()
        })
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::warn!(error = %error, "managed coding services did not drain cleanly");
            }
            Err(error) => {
                tracing::warn!(error = %error, "managed coding drain worker failed");
            }
        }
    })
}

fn managed_coding_command_environment() -> BTreeMap<String, String> {
    [
        "PATH",
        "HOME",
        "USERPROFILE",
        "APPDATA",
        "LOCALAPPDATA",
        "CARGO_HOME",
        "RUSTUP_HOME",
        "TMP",
        "TEMP",
        "SYSTEMROOT",
        "COMSPEC",
    ]
    .into_iter()
    .filter_map(|name| std::env::var(name).ok().map(|value| (name.to_owned(), value)))
    .collect()
}

fn managed_coding_command_policies() -> Vec<application::coding_runtime::CodingCommandPolicyV2> {
    use application::local_resource_governor::ResourceUnitsV1;

    let environment = managed_coding_command_environment();
    let resources = ResourceUnitsV1 {
        processes: 1,
        memory_bytes: 2 * 1024 * 1024 * 1024,
        file_descriptors: 256,
        sockets: 0,
        spool_bytes: 16 * 1024 * 1024,
        concurrency: 1,
    };
    let mut policies = Vec::new();
    if let Some(cargo) = application::managed_coding_services::resolve_trusted_executable("cargo") {
        for (command_id, args) in [
            ("rust-check", vec!["check", "--workspace", "--locked"]),
            ("rust-test", vec!["test", "--workspace", "--locked"]),
        ] {
            policies.push(application::coding_runtime::CodingCommandPolicyV2 {
                command_id: command_id.to_owned(),
                executable: cargo.clone(),
                args: args.into_iter().map(str::to_owned).collect(),
                env: environment.clone(),
                requires_terminal: false,
                timeout: Duration::from_secs(30 * 60),
                no_output_timeout: Some(Duration::from_secs(5 * 60)),
                resource_units: resources,
            });
        }
    }
    if let Some(npm) = application::managed_coding_services::resolve_trusted_executable("npm") {
        policies.push(application::coding_runtime::CodingCommandPolicyV2 {
            command_id: "typescript-check".to_owned(),
            executable: npm,
            args: vec!["run".to_owned(), "check".to_owned(), "--if-present".to_owned()],
            env: environment.clone(),
            requires_terminal: false,
            timeout: Duration::from_secs(15 * 60),
            no_output_timeout: Some(Duration::from_secs(5 * 60)),
            resource_units: resources,
        });
    }
    let python = application::managed_coding_services::resolve_trusted_executable("python")
        .or_else(|| application::managed_coding_services::resolve_trusted_executable("python3"));
    if let Some(python) = python {
        policies.push(application::coding_runtime::CodingCommandPolicyV2 {
            command_id: "python-check".to_owned(),
            executable: python.clone(),
            args: vec!["-m".to_owned(), "compileall".to_owned(), "-q".to_owned(), ".".to_owned()],
            env: environment.clone(),
            requires_terminal: false,
            timeout: Duration::from_secs(10 * 60),
            no_output_timeout: Some(Duration::from_secs(2 * 60)),
            resource_units: resources,
        });
        policies.push(application::coding_runtime::CodingCommandPolicyV2 {
            command_id: "python-repl".to_owned(),
            executable: python,
            args: vec!["-q".to_owned()],
            env: environment,
            requires_terminal: true,
            timeout: Duration::from_secs(30 * 60),
            no_output_timeout: None,
            resource_units: resources,
        });
    }
    policies
}

fn managed_coding_lsp_policies(
    code_intel: &config::CodeIntelConfig,
) -> Vec<application::lsp_workspace_supervisor::LspServerCommandPolicyV2> {
    use application::lsp_workspace_supervisor::{LspLanguageV2, LspServerCommandPolicyV2};

    if !code_intel.enabled {
        return Vec::new();
    }
    [
        (LspLanguageV2::Rust, code_intel.rust_analyzer_binary.as_str(), Vec::new()),
        (
            LspLanguageV2::TypeScript,
            code_intel.typescript_server_binary.as_str(),
            vec!["--stdio".to_owned()],
        ),
        (LspLanguageV2::Python, code_intel.pyright_binary.as_str(), vec!["--stdio".to_owned()]),
    ]
    .into_iter()
    .filter_map(|(language, configured, args)| {
        let executable =
            application::managed_coding_services::resolve_trusted_configured_executable(
                configured,
            )?;
        let toolchain_fingerprint =
            application::managed_coding_services::executable_fingerprint(executable.as_path())
                .ok()?;
        Some(LspServerCommandPolicyV2 {
            language,
            executable,
            args,
            env: managed_coding_command_environment(),
            toolchain_fingerprint,
            network_allowed: code_intel.allow_network,
        })
    })
    .collect()
}

fn spawn_networked_worker_expiry_loop(
    runtime: Arc<GatewayRuntimeState>,
) -> tokio::task::JoinHandle<()> {
    spawn_networked_worker_expiry_loop_with_interval(runtime, NETWORKED_WORKER_EXPIRY_INTERVAL)
}

fn spawn_networked_worker_expiry_loop_with_interval(
    runtime: Arc<GatewayRuntimeState>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut lifecycle = runtime.daemon_lifecycle.subscribe();
        let first_tick = tokio::time::Instant::now() + interval;
        let mut ticker = tokio::time::interval_at(first_tick, interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                changed = lifecycle.changed() => {
                    if changed.is_err() || lifecycle.borrow().phase.stops_subsystems() {
                        break;
                    }
                    continue;
                }
            }
            if lifecycle.borrow().phase.stops_subsystems() {
                break;
            }
            match runtime.reap_expired_networked_workers().await {
                Ok(events) if !events.is_empty() => {
                    tracing::warn!(
                        networked_workers_reaped = events.len(),
                        "reclaimed expired networked worker leases"
                    );
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(
                        status_code = ?error.code(),
                        status_message = %error.message(),
                        "periodic networked worker expiry reconciliation failed"
                    );
                }
            }
        }
    })
}

fn spawn_runtime_health_reconciliation_loop(
    runtime: Arc<GatewayRuntimeState>,
) -> tokio::task::JoinHandle<()> {
    spawn_runtime_health_reconciliation_loop_with_interval(
        runtime,
        RUNTIME_HEALTH_RECONCILIATION_INTERVAL,
    )
}

fn spawn_runtime_health_reconciliation_loop_with_interval(
    runtime: Arc<GatewayRuntimeState>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut lifecycle = runtime.daemon_lifecycle.subscribe();
        // Startup reconciles inherited probes before runtime activation; delay the first
        // periodic pass so it cannot race the startup inventory transaction.
        let first_tick = tokio::time::Instant::now() + interval;
        let mut ticker = tokio::time::interval_at(first_tick, interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                changed = lifecycle.changed() => {
                    if changed.is_err() || lifecycle.borrow().phase.stops_subsystems() {
                        break;
                    }
                    continue;
                }
            }
            if lifecycle.borrow().phase.stops_subsystems() {
                break;
            }
            match runtime.reconcile_runtime_health_probes_async().await {
                Ok(outcome)
                    if outcome.examined > 0
                        || outcome.repaired_stranded_health > 0
                        || outcome.retired_orphan_leases > 0 =>
                {
                    tracing::info!(
                        health_probes_examined = outcome.examined,
                        health_probes_settled_inconclusive = outcome.settled_inconclusive,
                        health_probes_stranded_repaired = outcome.repaired_stranded_health,
                        health_probe_orphan_leases_retired = outcome.retired_orphan_leases,
                        health_probe_generation_mismatches = outcome.skipped_generation_mismatches,
                        health_probe_reconciliation_remaining = outcome.remaining,
                        "completed periodic runtime health reconciliation"
                    );
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(
                        status_code = ?error.code(),
                        status_message = %error.message(),
                        "periodic runtime health reconciliation failed"
                    );
                }
            }
        }
    })
}

fn spawn_process_lease_reconciliation_loop(
    runtime: Arc<GatewayRuntimeState>,
) -> tokio::task::JoinHandle<()> {
    spawn_process_lease_reconciliation_loop_with_interval(
        runtime,
        PROCESS_LEASE_RECONCILIATION_INTERVAL,
    )
}

fn spawn_process_lease_reconciliation_loop_with_interval(
    runtime: Arc<GatewayRuntimeState>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut lifecycle = runtime.daemon_lifecycle.subscribe();
        // Startup performs the first pass synchronously before listeners bind. `interval_at`
        // prevents an immediate periodic duplicate while retaining delayed missed-tick behavior.
        let first_tick = tokio::time::Instant::now() + interval;
        let mut ticker = tokio::time::interval_at(first_tick, interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                changed = lifecycle.changed() => {
                    if changed.is_err() || lifecycle.borrow().phase.stops_subsystems() {
                        break;
                    }
                    continue;
                }
            }
            if lifecycle.borrow().phase.stops_subsystems() {
                break;
            }
            match runtime.reconcile_persisted_process_leases_async().await {
                Ok(report)
                    if report.inspected_count > 0 || report.pending_cleanup_inspected_count > 0 =>
                {
                    tracing::info!(
                        process_leases_inspected = report.inspected_count,
                        process_leases_closed = report.closed_count,
                        process_leases_orphaned = report.orphaned_count,
                        process_leases_quarantined = report.quarantined_count,
                        process_leases_expired = report.expired_count,
                        pending_process_cleanups_inspected = report.pending_cleanup_inspected_count,
                        pending_process_cleanups_completed = report.pending_cleanup_completed_count,
                        pending_process_cleanups_remaining = report.pending_cleanup_count,
                        "completed periodic process lease reconciliation"
                    );
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(
                        status_code = ?error.code(),
                        status_message = %error.message(),
                        "periodic process lease reconciliation failed"
                    );
                }
            }
        }
    })
}

/// Runs the daemon until shutdown: the single entry point used by `palyrad`.
///
/// Startup sequence: install tracing, load config and CLI overrides, open the
/// identity store, journal, vault, and registries, resolve configured secret
/// references (auditing each access to the journal), enforce the fail-closed
/// remote-bind guard, bind the admin HTTP, gateway gRPC, and node-RPC
/// listeners, spawn the scheduler/channel/hook/background/self-healing/process-reconciliation
/// and networked-worker expiry loops, then serve until the lifecycle controller
/// completes a SIGINT, SIGTERM, admin, or restart drain.
///
/// With `--journal-migrate-only` it applies journal migrations and returns
/// before binding any listener.
///
/// # Errors
///
/// Returns an error when any startup stage fails - invalid config or env
/// values, storage/identity/vault initialization failures, secret resolution
/// failures, rejected security posture (bind guard, process-runner policy,
/// missing admin token), unbindable listeners - or when either server exits
/// with a failure at runtime.
pub async fn run() -> Result<()> {
    init_tracing();
    let bootstrap = load_runtime_bootstrap()?;
    let mut loaded = bootstrap.loaded;
    let journal_migrate_only = bootstrap.journal_migrate_only;
    let node_rpc_mtls_required = bootstrap.node_rpc_mtls_required;

    let identity_runtime = load_identity_runtime(loaded.gateway.identity_store_dir.clone())
        .context("failed to initialize gateway identity runtime")?;
    let runtime_state_root = resolve_runtime_state_root(identity_runtime.store_root.as_path())
        .context("failed to resolve daemon runtime state root")?;
    let qa_fault_runtime = qa_fault_injection::load_fault_injection(runtime_state_root.as_path())
        .context("failed QA fault-injection startup preflight")?;
    let offline_mode = offline_mode_enabled()?;
    let memory_embedding_selection =
        build_memory_embedding_runtime_selection(&loaded.model_provider, offline_mode)
            .context("failed to resolve retrieval embeddings runtime")?;
    let journal_store = JournalStore::open_with_memory_embedding_runtime_and_fault_injection(
        JournalConfig {
            db_path: loaded.storage.journal_db_path.clone(),
            hash_chain_enabled: loaded.storage.journal_hash_chain_enabled,
            max_payload_bytes: loaded.storage.max_journal_payload_bytes,
            max_events: loaded.storage.max_journal_events,
        },
        Arc::clone(&memory_embedding_selection.provider),
        memory_embedding_selection.profile.clone(),
        qa_fault_runtime.clone(),
    )
    .map_err(|error| match &error {
        JournalError::RuntimeStateCompatibilityBlocked { report } => anyhow::anyhow!(
            "failed to initialize event journal storage: {}",
            report.redacted_reason_summary()
        ),
        _ => anyhow::anyhow!("failed to initialize event journal storage: {error}"),
    })?;
    let startup_compatibility = journal_store.startup_runtime_state_compatibility_report();
    info!(
        compatibility_summary = %startup_compatibility.redacted_reason_summary(),
        "journal startup compatibility preflight completed"
    );
    let runtime_state_compatibility = journal_store
        .runtime_state_compatibility_report()
        .context("failed to inspect shared runtime state compatibility")?;
    journal_store
        .persist_runtime_state_quarantine_findings(&runtime_state_compatibility)
        .context("failed to persist shared runtime state quarantine evidence")?;
    if !runtime_state_compatibility.permits_admission() {
        anyhow::bail!(
            "shared runtime state blocks admission: {}",
            runtime_state_compatibility.redacted_reason_summary(),
        );
    }
    if journal_migrate_only {
        info!(
            journal_db_path = %loaded.storage.journal_db_path.display(),
            hash_chain_enabled = loaded.storage.journal_hash_chain_enabled,
            compatibility_admission = runtime_state_compatibility.admission.as_str(),
            compatibility_findings = runtime_state_compatibility.findings.len(),
            startup_compatibility_summary = %startup_compatibility.redacted_reason_summary(),
            "journal migrations applied; exiting due to --journal-migrate-only"
        );
        println!(
            "journal.migration=ok db_path={} hash_chain_enabled={} compatibility_admission={} compatibility_findings={}",
            loaded.storage.journal_db_path.display(),
            loaded.storage.journal_hash_chain_enabled,
            runtime_state_compatibility.admission.as_str(),
            runtime_state_compatibility.findings.len(),
        );
        return Ok(());
    }
    loop {
        let outcome = journal_store
            .reconcile_runtime_health_probes(
                RuntimeHealthProbeReconciliationMode::Startup,
                unix_ms_now()?,
            )
            .context("failed to reconcile inherited runtime health probes")?;
        if !outcome.remaining {
            break;
        }
    }

    let vault = Arc::new(
        Vault::open_with_config(VaultConfigOptions {
            root: Some(loaded.storage.vault_dir.clone()),
            identity_store_root: Some(identity_runtime.store_root.clone()),
            ..VaultConfigOptions::default()
        })
        .context("failed to initialize vault runtime")?,
    );
    let auth_registry = Arc::new(
        AuthProfileRegistry::open(identity_runtime.store_root.as_path())
            .context("failed to initialize auth profile registry state")?,
    );
    let secret_resolver = SecretResolver::with_working_dir(
        Some(vault.as_ref()),
        secret_resolution_working_dir(&loaded)?,
    );
    for access_audit in resolve_model_provider_secret(
        &mut loaded.model_provider,
        auth_registry.as_ref(),
        vault.as_ref(),
        &secret_resolver,
    )? {
        record_secret_access_journal_event(&journal_store, &access_audit)
            .context("failed to audit model provider secret access")?;
    }
    for access_audit in resolve_admin_and_browser_secret_refs(&mut loaded, &secret_resolver)? {
        record_secret_access_journal_event(&journal_store, &access_audit)
            .context("failed to audit runtime secret access")?;
    }
    let configured_secrets =
        build_configured_secrets_state(&loaded, &secret_resolver, 1, "startup")?;
    let auth = GatewayAuthConfig {
        require_auth: loaded.admin.require_auth,
        admin_token: loaded.admin.auth_token.clone(),
        connector_token: loaded.admin.connector_token.clone(),
        bound_principal: loaded.admin.bound_principal.clone(),
    };
    validate_admin_auth_config(&auth)?;
    let model_provider = build_model_provider(&loaded.model_provider)
        .context("failed to initialize model provider runtime")?;
    let v2_availability =
        production_runtime_kernel_v2_availability(&model_provider.status_snapshot());
    let (runtime_kernel_dispatcher, runtime_kernel_secret_audit) =
        build_runtime_kernel_dispatcher(&loaded, &secret_resolver, v2_availability)?;
    if let Some(access_audit) = runtime_kernel_secret_audit {
        record_secret_access_journal_event(&journal_store, &access_audit)
            .context("failed to audit runtime-kernel sampling secret access")?;
    }
    if matches!(
        loaded.daemon.runtime_kernel.profile,
        config::RuntimeKernelProfile::Legacy | config::RuntimeKernelProfile::V2Shadow
    ) {
        let report: journal::runtime_kernel::RuntimeRollbackActuationReportV1 = journal_store
            .request_runtime_kernel_profile_downgrade(loaded.daemon.runtime_kernel.rollback_policy)
            .context("failed to apply runtime-kernel profile downgrade policy")?;
        info!(
            profile = loaded.daemon.runtime_kernel.profile.as_str(),
            rollback_policy = loaded.daemon.runtime_kernel.rollback_policy.as_str(),
            evaluated = report.evaluated,
            finish_allowed = report.finish_allowed,
            suspension_pending = report.suspension_pending,
            suspended = report.suspended,
            replayed = report.replayed,
            "runtime-kernel profile downgrade scan completed"
        );
    }
    let agent_registry = agents::AgentRegistry::open(identity_runtime.store_root.as_path())
        .context("failed to initialize agent registry state")?;
    ensure_local_default_agent(&agent_registry, &loaded)
        .context("failed to ensure local default agent state")?;
    let acp_runtime = Arc::new(
        acp::AcpRuntime::open_with_live_runtime(
            acp::acp_root_from_state_root(runtime_state_root.as_path()),
            loaded.feature_rollouts.acp_runtime.enabled,
            loaded.acp_runtime.clone(),
        )
        .context("failed to initialize ACP runtime state")?,
    );
    let webhook_registry = Arc::new(
        webhooks::WebhookRegistry::open(runtime_state_root.as_path())
            .context("failed to initialize webhook registry state")?,
    );
    let routine_registry = Arc::new(
        routines::RoutineRegistry::open(runtime_state_root.as_path())
            .context("failed to initialize routine registry state")?,
    );
    let objective_registry = Arc::new(
        objectives::ObjectiveRegistry::open(runtime_state_root.as_path())
            .context("failed to initialize objective registry state")?,
    );
    let access_registry = Arc::new(Mutex::new(
        access_control::AccessRegistry::open(runtime_state_root.as_path())
            .context("failed to initialize access registry state")?,
    ));
    let tool_posture_registry =
        tool_posture::ToolPostureRegistry::open(runtime_state_root.as_path())
            .context("failed to initialize tool posture state")?;
    let plugin_bindings =
        plugins::load_plugin_bindings_index(runtime_state_root.join("plugins").as_path())
            .context("failed to load plugin bindings for managed runtime health")?;
    let plugin_binding_ids = plugin_bindings
        .entries
        .iter()
        .filter(|binding| binding.enabled)
        .map(|binding| binding.plugin_id.clone())
        .collect::<Vec<_>>();
    let auth_runtime = Arc::new(gateway::AuthRuntimeState::new(
        Arc::clone(&auth_registry),
        Arc::new(HttpOAuthRefreshAdapter::default()) as Arc<dyn OAuthRefreshAdapter>,
    ));
    let node_runtime = Arc::new(
        node_runtime::NodeRuntimeState::load(runtime_state_root.as_path())
            .context("failed to initialize node runtime state")?,
    );
    let conversation_bindings = application::conversation_bindings::ConversationBindingStore::open(
        runtime_state_root.join("conversation-bindings.json"),
    )
    .context("failed to initialize conversation binding state")?;
    let conversation_binding_startup_report = conversation_bindings
        .reconcile_on_startup(unix_ms_now().context("failed to read system clock")?)
        .context("failed to reconcile conversation binding state")?;
    if conversation_binding_startup_report.expired_count > 0
        || conversation_binding_startup_report.conflict_count > 0
    {
        warn!(
            expired_count = conversation_binding_startup_report.expired_count,
            conflict_count = conversation_binding_startup_report.conflict_count,
            "conversation binding startup reconcile found stale state"
        );
    }
    #[rustfmt::skip]
    let external_retrieval_index = Arc::new(ExternalRetrievalRuntime::default());
    let trusted_git = application::managed_coding_services::resolve_trusted_executable("git");
    let trusted_git_missing = trusted_git.is_none();
    let managed_coding_services = trusted_git.and_then(|git_executable| {
        let state_parent = loaded.storage.journal_db_path.parent()?;
        let state_root = state_parent.join("managed-coding-runtime");
        let lsp_policies = managed_coding_lsp_policies(&loaded.tool_call.code_intel);
        match application::managed_coding_services::ManagedCodingRuntimeServices::open(
            application::managed_coding_services::ManagedCodingServicesConfig {
                managed_worktree_root: state_root.join("worktrees"),
                state_root,
                git_executable,
                profile: application::coding_runtime::CodingExecutionProfileV2 {
                    managed_worktree_enabled: true,
                    in_place_workspace_fallback_allowed: false,
                    persistent_lsp_enabled: loaded.tool_call.code_intel.enabled,
                    cli_diagnostics_fallback_allowed: true,
                    native_pty_enabled: true,
                    process_fallback_without_pty_allowed: true,
                    retain_dirty_worktrees: true,
                },
                command_policies: managed_coding_command_policies(),
                lsp_policies,
                // Denied-network policies remain configured but degrade to
                // CLI verification until a host isolation backend is proven.
                lsp_network_isolation_verified: false,
                lsp_idle_ttl: Duration::from_millis(loaded.tool_call.code_intel.idle_reap_ms),
            },
        ) {
            Ok(services) => Some(Arc::new(services)),
            Err(error) => {
                warn!(error = %error, "managed coding runtime is unavailable");
                None
            }
        }
    });
    if managed_coding_services.is_none() && trusted_git_missing {
        warn!("managed coding runtime is unavailable because trusted Git was not resolved");
    }
    let runtime = GatewayRuntimeState::new_with_provider(
        GatewayRuntimeConfigSnapshot {
            grpc_bind_addr: loaded.gateway.grpc_bind_addr.clone(),
            grpc_port: loaded.gateway.grpc_port,
            quic_bind_addr: loaded.gateway.quic_bind_addr.clone(),
            quic_port: loaded.gateway.quic_port,
            quic_enabled: loaded.gateway.quic_enabled,
            orchestrator_runloop_v1_enabled: loaded.orchestrator.runloop_v1_enabled,
            model_provider_request_timeout_ms: loaded.model_provider.request_timeout_ms,
            qa_execution_key_digest: loaded.model_provider.qa_execution_key_digest.clone(),
            qa_provider_binding_sha256: loaded.model_provider.qa_provider_binding_sha256.clone(),
            node_rpc_mtls_required,
            admin_auth_required: loaded.admin.require_auth,
            vault_get_approval_required_refs: loaded
                .gateway
                .vault_get_approval_required_refs
                .clone(),
            max_tape_entries_per_response: loaded.gateway.max_tape_entries_per_response,
            max_tape_bytes_per_response: loaded.gateway.max_tape_bytes_per_response,
            feature_rollouts: loaded.feature_rollouts.clone(),
            session_queue_policy: loaded.session_queue_policy.clone(),
            pruning_policy_matrix: loaded.pruning_policy_matrix.clone(),
            retrieval_dual_path: loaded.retrieval_dual_path.clone(),
            auxiliary_executor: loaded.auxiliary_executor.clone(),
            flow_orchestration: loaded.flow_orchestration.clone(),
            delivery_arbitration: loaded.delivery_arbitration.clone(),
            replay_capture: loaded.replay_capture.clone(),
            networked_workers: loaded.networked_workers.clone(),
            mcp_servers: loaded.mcp_servers.clone(),
            plugin_binding_ids,
            execution_backend_profiles: loaded.execution_backend_profiles.clone(),
            agent_harness_registry: loaded.agent_harness_registry.clone(),
            channel_router: loaded.channel_router.clone(),
            media: loaded.media.clone(),
            code_intel: loaded.tool_call.code_intel.clone(),
            tool_call: tool_protocol::ToolCallConfig {
                allowed_tools: loaded.tool_call.allowed_tools.clone(),
                max_calls_per_run: loaded.tool_call.max_calls_per_run,
                execution_timeout_ms: loaded.tool_call.execution_timeout_ms,
                process_runner: sandbox_runner::SandboxProcessRunnerPolicy {
                    enabled: loaded.tool_call.process_runner.enabled,
                    tier: loaded.tool_call.process_runner.tier,
                    workspace_root: loaded.tool_call.process_runner.workspace_root.clone(),
                    path_access_mode: loaded.tool_call.process_runner.path_access_mode,
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
            tool_catalog_policy:
                application::tool_registry::ToolCatalogPolicySnapshot::from_loaded_tool_call_config(
                    &loaded.tool_call,
                ),
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
                allowed_credential_vault_refs: loaded
                    .tool_call
                    .http_fetch
                    .allowed_credential_vault_refs
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
            smart_routing: load_smart_routing_runtime_config(),
        },
        GatewayJournalConfigSnapshot {
            db_path: loaded.storage.journal_db_path.clone(),
            hash_chain_enabled: loaded.storage.journal_hash_chain_enabled,
        },
        journal_store,
        identity_runtime.revoked_certificate_count,
        gateway::GatewayRuntimeDependencies {
            model_provider,
            vault: Arc::clone(&vault),
            auth_profile_registry: Some(Arc::clone(&auth_registry)),
            auth_runtime: Some(Arc::clone(&auth_runtime)),
            agent_registry,
            tool_posture_registry,
            retrieval_backend: Arc::new(ExternalDerivedRetrievalBackend::new(
                external_retrieval_index.clone(),
            )),
            external_retrieval_index,
            conversation_bindings,
            fault_injection: qa_fault_runtime,
            runtime_kernel_dispatcher,
            managed_coding_services,
        },
    )
    .context("failed to initialize gateway runtime state")?;
    runtime.configure_networked_worker_remote_dispatcher(Arc::new(
        application::tool_runtime::networked_worker::NodeRuntimeNetworkedWorkerDispatcher::new(
            Arc::clone(&node_runtime),
        ),
    ));
    runtime.configure_memory(MemoryRuntimeConfig {
        max_item_bytes: loaded.memory.max_item_bytes,
        max_item_tokens: loaded.memory.max_item_tokens,
        auto_inject_enabled: loaded.memory.auto_inject.enabled,
        auto_inject_max_items: loaded.memory.auto_inject.max_items,
        default_ttl_ms: loaded.memory.default_ttl_ms,
        retention_max_entries: loaded.memory.retention.max_entries,
        retention_max_bytes: loaded.memory.retention.max_bytes,
        retention_ttl_days: loaded.memory.retention.ttl_days,
        retention_vacuum_schedule: loaded.memory.retention.vacuum_schedule.clone(),
    });
    runtime.configure_retrieval(loaded.memory.retrieval.clone());
    runtime.configure_learning(build_learning_runtime_config()?);

    let startup_flow_dependency_audit = runtime
        .audit_flow_dependencies_on_startup()
        .await
        .context("failed to audit durable flow dependencies during startup")?;
    if startup_flow_dependency_audit.invalid_flow_count > 0 {
        warn!(
            inspected_flow_count = startup_flow_dependency_audit.inspected_flow_count,
            invalid_flow_count = startup_flow_dependency_audit.invalid_flow_count,
            newly_recorded_invalid_count =
                startup_flow_dependency_audit.newly_recorded_invalid_count,
            "startup flow dependency audit found invalid durable graphs"
        );
    }

    let startup_run_recovery = runtime
        .terminalize_orphaned_orchestrator_runs_on_startup(
            "daemon startup detected an interrupted active run from a previous runtime",
        )
        .await
        .context("failed to actuate interrupted orchestrator runs during startup")?;
    let startup_background_task_recovery = runtime
        .reconcile_orphaned_background_tasks_on_startup(
            "daemon startup detected an orphaned in-process background task; automatic replay is blocked and explicit operator retry is required",
        )
        .await
        .context("failed to reconcile orphaned background tasks during startup")?;
    let startup_child_completion_recovery = runtime
        .reconcile_child_completions()
        .await
        .context("failed to reconcile durable child completions during startup")?;
    let startup_parent_suspension_recovery = runtime
        .reconcile_parent_suspensions()
        .await
        .context("failed to reconcile durable parent suspensions during startup")?;
    let startup_process_lease_reconciliation = runtime
        .reconcile_persisted_process_leases_async()
        .await
        .context("failed to reconcile persisted process leases during startup")?;
    let mut startup_cleanup_traces_finalized = 0usize;
    for run_id in &startup_run_recovery.deferred_metadata_trace_run_ids {
        if runtime.journal_store.finalize_startup_recovery_metadata_trace(run_id).with_context(
            || {
                format!(
                    "failed to finalize startup-recovery metadata trace for run digest {}",
                    crate::metadata_trace::hash_metadata_trace_run_id(run_id)
                        .unwrap_or_else(|| "invalid".to_owned())
                )
            },
        )? {
            startup_cleanup_traces_finalized = startup_cleanup_traces_finalized.saturating_add(1);
        }
    }
    let startup_cleanup_traces_pending = startup_run_recovery
        .deferred_metadata_trace_run_ids
        .len()
        .saturating_sub(startup_cleanup_traces_finalized);
    let startup_networked_worker_expiry = runtime
        .reap_expired_networked_workers()
        .await
        .context("failed to reconcile expired networked worker leases during startup")?;
    let recovered_journal_fault_activations = runtime
        .journal_store
        .reconcile_pending_qa_fault_recoveries()
        .context("failed to reconcile committed journal effects after QA fault restart")?;

    let build = build_metadata();
    info!(
        service = "palyrad",
        version = build.version,
        git_hash = build.git_hash,
        build_profile = build.build_profile,
        config_source = %loaded.source,
        config_version = loaded.config_version,
        config_migrated_from_version = ?loaded.migrated_from_version,
        deployment_mode = loaded.deployment.mode.as_str(),
        deployment_dangerous_remote_bind_ack = loaded.deployment.dangerous_remote_bind_ack,
        admin_bind_addr = %loaded.daemon.bind_addr,
        admin_port = loaded.daemon.port,
        grpc_bind_addr = %loaded.gateway.grpc_bind_addr,
        grpc_port = loaded.gateway.grpc_port,
        quic_bind_addr = %loaded.gateway.quic_bind_addr,
        quic_port = loaded.gateway.quic_port,
        quic_enabled = loaded.gateway.quic_enabled,
        gateway_bind_profile = loaded.gateway.bind_profile.as_str(),
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
        model_provider_auth_profile_id = ?loaded.model_provider.auth_profile_id,
        model_provider_auth_profile_provider_kind = ?loaded.model_provider.auth_profile_provider_kind.map(|kind| kind.as_str()),
        model_provider_credential_source = ?loaded.model_provider.credential_source.map(|source| source.as_str()),
        vault_backend = vault.backend_kind().as_str(),
        tool_call_allowed_tools = ?loaded.tool_call.allowed_tools,
        tool_call_legacy_max_calls_per_run = loaded.tool_call.max_calls_per_run,
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
        tool_call_browser_service_state_dir =
            ?loaded.tool_call.browser_service.state_dir.as_ref().map(|p| p.display().to_string()),
        tool_call_browser_service_state_key_vault_ref_configured =
            loaded.tool_call.browser_service.state_key_vault_ref.is_some(),
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
        channel_router_default_direct_message_policy =
            loaded.channel_router.default_direct_message_policy.as_str(),
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
        canvas_rate_limit_window_ms = CANVAS_RATE_LIMIT_WINDOW_MS,
        canvas_rate_limit_max_requests_per_window = CANVAS_RATE_LIMIT_MAX_REQUESTS_PER_WINDOW,
        grpc_max_decoding_message_size_bytes = GRPC_MAX_DECODING_MESSAGE_SIZE_BYTES,
        grpc_max_encoding_message_size_bytes = GRPC_MAX_ENCODING_MESSAGE_SIZE_BYTES,
        node_rpc_mtls_required,
        journal_db_path = %loaded.storage.journal_db_path.display(),
        journal_hash_chain_enabled = loaded.storage.journal_hash_chain_enabled,
        journal_max_payload_bytes = loaded.storage.max_journal_payload_bytes,
        journal_max_events = loaded.storage.max_journal_events,
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
    let dangerous_remote_bind_ack_env = dangerous_remote_bind_acknowledged()?;
    enforce_remote_bind_guard(
        RemoteBindEndpoints { admin_address, grpc_address, quic_address },
        RemoteBindGuardConfig {
            bind_profile: loaded.gateway.bind_profile,
            allow_insecure_remote: loaded.gateway.allow_insecure_remote,
            gateway_tls_enabled: loaded.gateway.tls.enabled,
            admin_auth_required: loaded.admin.require_auth,
            admin_token_configured: loaded.admin.auth_token.is_some(),
            node_rpc_mtls_required,
            config_dangerous_remote_bind_ack: loaded.deployment.dangerous_remote_bind_ack,
            env_dangerous_remote_bind_ack: dangerous_remote_bind_ack_env,
        },
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
    let connectors_db_path =
        connector_db_path_from_journal_path(loaded.storage.journal_db_path.as_path());
    #[cfg(feature = "qa-fault-injection")]
    let channels = channels::ChannelPlatform::initialize_with_qa_fault_probe(
        grpc_url.clone(),
        auth.clone(),
        connectors_db_path,
        loaded.media.clone(),
        runtime.fault_injection.probe_handle(),
    );
    #[cfg(not(feature = "qa-fault-injection"))]
    let channels = channels::ChannelPlatform::initialize(
        grpc_url.clone(),
        auth.clone(),
        connectors_db_path,
        loaded.media.clone(),
    );
    let channels = Arc::new(channels.context("failed to initialize channel connector platform")?);
    #[cfg(feature = "qa-fault-injection")]
    let recovered_connector_fault_activations = channels
        .reconcile_pending_qa_fault_recoveries(&runtime.fault_injection)
        .context("failed to reconcile connector outbox effects after QA fault restart")?;
    #[cfg(not(feature = "qa-fault-injection"))]
    let recovered_connector_fault_activations = 0usize;
    let pending_final_recovery =
        application::runtime_kernel_v2::finalization::recover_pending_final_deliveries(
            &runtime.journal_store,
            channels.as_ref(),
        )
        .context("failed to reconcile pending final deliveries during startup")?;
    if !pending_final_recovery.parent_wake_run_ids.is_empty() {
        scheduler_wake.notify_waiters();
    }
    let recovered_generic_fault_activations = runtime
        .fault_injection
        .record_startup_orphan_recoveries()
        .context("failed to record QA fault recovery after subsystem reconciliation")?;
    let recovered_fault_activations = recovered_journal_fault_activations
        .saturating_add(recovered_connector_fault_activations)
        .saturating_add(recovered_generic_fault_activations);
    runtime
        .complete_daemon_startup_recovery()
        .context("failed to release daemon startup recovery barrier")?;
    if startup_run_recovery.scanned_count > 0
        || startup_background_task_recovery.failed_count > 0
        || startup_parent_suspension_recovery.matched_child_count > 0
        || startup_parent_suspension_recovery.timed_out_count > 0
        || startup_child_completion_recovery.delivered_announcements > 0
        || startup_child_completion_recovery.stale_announcements > 0
        || startup_child_completion_recovery.manual_review_announcements > 0
        || startup_process_lease_reconciliation.inspected_count > 0
        || startup_process_lease_reconciliation.pending_cleanup_inspected_count > 0
        || !startup_networked_worker_expiry.is_empty()
        || pending_final_recovery.scanned_count > 0
        || recovered_fault_activations > 0
    {
        warn!(
            scanned_run_count = startup_run_recovery.scanned_count,
            terminalized_count = startup_run_recovery.terminalized_count,
            terminalized_run_ids = ?startup_run_recovery.terminalized_run_ids,
            continuation_queued_count = startup_run_recovery.continuation_queued_count,
            continuation_run_ids = ?startup_run_recovery
                .continuation_descriptors
                .iter()
                .map(|descriptor| descriptor.continuation_run_id.as_str())
                .collect::<Vec<_>>(),
            confirmation_required_count = startup_run_recovery.confirmation_required_count,
            failed_background_task_count = startup_background_task_recovery.failed_count,
            failed_background_task_ids = ?startup_background_task_recovery.failed_task_ids,
            parent_suspension_children_matched =
                startup_parent_suspension_recovery.matched_child_count,
            parent_suspension_continuations_queued =
                startup_parent_suspension_recovery.continuation_queued_count,
            parent_suspensions_timed_out = startup_parent_suspension_recovery.timed_out_count,
            child_orphans_classified = startup_child_completion_recovery.classified_orphans,
            child_announcements_delivered =
                startup_child_completion_recovery.delivered_announcements,
            child_announcements_nested_deferred =
                startup_child_completion_recovery.deferred_for_nested_children,
            child_announcements_stale =
                startup_child_completion_recovery.stale_announcements,
            child_announcements_cancelled =
                startup_child_completion_recovery.cancelled_announcements,
            child_announcements_manual_review =
                startup_child_completion_recovery.manual_review_announcements,
            process_leases_inspected = startup_process_lease_reconciliation.inspected_count,
            process_leases_closed = startup_process_lease_reconciliation.closed_count,
            process_leases_orphaned = startup_process_lease_reconciliation.orphaned_count,
            process_leases_quarantined = startup_process_lease_reconciliation.quarantined_count,
            process_leases_expired = startup_process_lease_reconciliation.expired_count,
            pending_process_cleanups_inspected =
                startup_process_lease_reconciliation.pending_cleanup_inspected_count,
            pending_final_deliveries_scanned = pending_final_recovery.scanned_count,
            final_artifacts_without_intent =
                pending_final_recovery.artifact_without_intent_count,
            final_delivery_intents_pending = pending_final_recovery.intent_pending_count,
            final_delivery_outcomes_unknown = pending_final_recovery.outcome_unknown_count,
            final_deliveries_acknowledged = pending_final_recovery.acknowledged_count,
            final_delivery_dead_letters = pending_final_recovery.dead_letter_count,
            final_delivery_parent_wake_digests = ?pending_final_recovery
                .parent_wake_run_ids
                .iter()
                .filter_map(|run_id| crate::metadata_trace::hash_metadata_trace_run_id(run_id))
                .collect::<Vec<_>>(),
            pending_process_cleanups_completed =
                startup_process_lease_reconciliation.pending_cleanup_completed_count,
            pending_process_cleanups_remaining =
                startup_process_lease_reconciliation.pending_cleanup_count,
            startup_cleanup_traces_finalized,
            startup_cleanup_traces_pending,
            networked_workers_reaped = startup_networked_worker_expiry.len(),
            recovered_journal_fault_activations,
            recovered_connector_fault_activations,
            recovered_generic_fault_activations,
            recovered_fault_activations,
            "completed startup recovery for orphaned runtime work and pending QA fault activations"
        );
    }
    runtime.configure_routines_runtime(RoutinesRuntimeConfig {
        registry: Arc::clone(&routine_registry),
        objectives: Arc::clone(&objective_registry),
        auth: auth.clone(),
        grpc_url: grpc_url.clone(),
        scheduler_wake: Arc::clone(&scheduler_wake),
        timezone_mode: loaded.cron.timezone,
    });
    let objective_continuation_recovery =
        application::objective_continuation::reconcile_startup(&runtime).await?;
    tracing::info!(
        objective_attempts_scanned = objective_continuation_recovery.scanned,
        objective_judge_tasks_repaired = objective_continuation_recovery.judge_tasks_repaired,
        objective_decisions_applied = objective_continuation_recovery.decisions_applied,
        objective_continuations_repaired = objective_continuation_recovery.continuations_repaired,
        objective_attempts_paused = objective_continuation_recovery.paused,
        objective_recovery_errors = objective_continuation_recovery.errors,
        reason_code = "objective.continuation.startup_reconciled",
        "completed bounded objective continuation recovery"
    );
    let _wake_coordinator_task =
        application::wake_coordinator::spawn_wake_coordinator(runtime.clone());
    let _cron_scheduler_task = supervise_lifecycle_subsystem_task(
        &runtime,
        application::daemon_lifecycle::LifecycleSubsystem::Scheduler,
        spawn_scheduler_loop(
            runtime.clone(),
            auth.clone(),
            grpc_url.clone(),
            Arc::clone(&scheduler_wake),
            loaded.memory.retention.clone(),
            Arc::clone(&access_registry),
        ),
    )?;

    let state = build_app_state(
        &loaded,
        dangerous_remote_bind_ack_env,
        configured_secrets,
        AppStateBuildContext {
            acp_runtime: Arc::clone(&acp_runtime),
            runtime: runtime.clone(),
            node_runtime: Arc::clone(&node_runtime),
            identity_manager: Arc::clone(&identity_runtime.manager),
            channels: Arc::clone(&channels),
            webhooks: Arc::clone(&webhook_registry),
            routines: Arc::clone(&routine_registry),
            objectives: Arc::clone(&objective_registry),
            vault: Arc::clone(&vault),
            auth_runtime: Arc::clone(&auth_runtime),
            auth: auth.clone(),
            grpc_url: grpc_url.clone(),
            scheduler_wake: Arc::clone(&scheduler_wake),
            access_registry: Arc::clone(&access_registry),
        },
    );
    let hook_runtime_policy = wasm_plugin_runner::WasmPluginRunnerPolicy {
        enabled: loaded.tool_call.wasm_runtime.enabled,
        allow_inline_modules: loaded.tool_call.wasm_runtime.allow_inline_modules,
        max_module_size_bytes: loaded.tool_call.wasm_runtime.max_module_size_bytes,
        fuel_budget: loaded.tool_call.wasm_runtime.fuel_budget,
        max_memory_bytes: loaded.tool_call.wasm_runtime.max_memory_bytes,
        max_table_elements: loaded.tool_call.wasm_runtime.max_table_elements,
        max_instances: loaded.tool_call.wasm_runtime.max_instances,
        allowed_http_hosts: loaded.tool_call.wasm_runtime.allowed_http_hosts.clone(),
        allowed_secrets: loaded.tool_call.wasm_runtime.allowed_secrets.clone(),
        allowed_storage_prefixes: loaded.tool_call.wasm_runtime.allowed_storage_prefixes.clone(),
        allowed_channels: loaded.tool_call.wasm_runtime.allowed_channels.clone(),
    };
    let hook_execution_timeout = Duration::from_millis(loaded.tool_call.execution_timeout_ms);
    let app = transport::http::router::build_router(state.clone());
    let _channel_worker_task = supervise_lifecycle_subsystem_task(
        &runtime,
        application::daemon_lifecycle::LifecycleSubsystem::Channels,
        Arc::clone(&channels).spawn_worker(),
    )?;
    let _hook_runtime_task = supervise_lifecycle_subsystem_task(
        &runtime,
        application::daemon_lifecycle::LifecycleSubsystem::Hooks,
        hooks::spawn_hook_runtime(runtime.clone(), hook_runtime_policy, hook_execution_timeout),
    )?;
    let _background_queue_task = supervise_lifecycle_subsystem_task(
        &runtime,
        application::daemon_lifecycle::LifecycleSubsystem::BackgroundQueue,
        background_queue::spawn_background_queue_loop(
            runtime.clone(),
            auth.clone(),
            grpc_url.clone(),
        ),
    )?;
    let _self_healing_task = supervise_lifecycle_subsystem_task(
        &runtime,
        application::daemon_lifecycle::LifecycleSubsystem::SelfHealing,
        self_healing::spawn_self_healing_loop(state.clone()),
    )?;
    let _runtime_health_reconciliation_task = supervise_lifecycle_subsystem_task(
        &runtime,
        application::daemon_lifecycle::LifecycleSubsystem::RuntimeHealth,
        spawn_runtime_health_reconciliation_loop(runtime.clone()),
    )?;
    let _managed_coding_lifecycle_task = supervise_lifecycle_subsystem_task(
        &runtime,
        application::daemon_lifecycle::LifecycleSubsystem::ManagedCoding,
        spawn_managed_coding_lifecycle(runtime.clone()),
    )?;
    let _process_lease_reconciliation_task = supervise_lifecycle_subsystem_task(
        &runtime,
        application::daemon_lifecycle::LifecycleSubsystem::ProcessLeases,
        spawn_process_lease_reconciliation_loop(runtime.clone()),
    )?;
    let _networked_worker_expiry_task = supervise_lifecycle_subsystem_task(
        &runtime,
        application::daemon_lifecycle::LifecycleSubsystem::NetworkedWorkers,
        spawn_networked_worker_expiry_loop(runtime.clone()),
    )?;
    let _config_watcher_task = config_watcher::path_from_loaded_source(&loaded.source)
        .map(|path| config_watcher::spawn_config_watcher(state.clone(), path))
        .transpose()
        .context("failed to initialize config watcher")?;
    let _shutdown_signal_task = app::shutdown::spawn_shutdown_signal_listener(Arc::clone(&runtime));

    let admin_shutdown_runtime = Arc::clone(&runtime);
    let admin_server = async move {
        axum::serve(admin_listener, app.into_make_service_with_connect_info::<SocketAddr>())
            .with_graceful_shutdown(async move {
                admin_shutdown_runtime.wait_for_daemon_shutdown().await;
            })
            .await
            .context("palyrad admin server failed")
    };
    let grpc_transport = transport::grpc::server::serve(
        &loaded,
        &identity_runtime,
        runtime.clone(),
        Arc::clone(&channels),
        auth.clone(),
        Arc::clone(&auth_runtime),
        grpc_url,
        Arc::clone(&scheduler_wake),
        grpc_listener,
        node_rpc_listener,
        quic_address,
        Arc::clone(&node_runtime),
        node_rpc_mtls_required,
    );
    tokio::try_join!(admin_server, grpc_transport)?;

    Ok(())
}

/// In local-desktop deployments, guarantees a usable default agent exists
/// (creating or selecting one); skipped entirely for other deployment modes,
/// and left to the operator when multiple agents exist without a default.
fn ensure_local_default_agent(
    agent_registry: &agents::AgentRegistry,
    loaded: &config::LoadedConfig,
) -> Result<()> {
    if loaded.deployment.mode != config::DeploymentMode::LocalDesktop {
        return Ok(());
    }

    let workspace_root =
        default_agent_workspace_root(loaded.tool_call.process_runner.workspace_root.as_path())?;
    let outcome = agent_registry.ensure_local_default_agent(
        workspace_root.as_path(),
        loaded.model_provider.default_chat_model_id(),
    )?;

    match outcome {
        agents::AgentDefaultEnsureOutcome::AlreadyConfigured { .. } => {}
        agents::AgentDefaultEnsureOutcome::Created { agent_id } => {
            info!(
                agent_id = agent_id.as_str(),
                workspace_root = %workspace_root.display(),
                "created local default agent during startup"
            );
        }
        agents::AgentDefaultEnsureOutcome::SelectedExisting { agent_id } => {
            info!(
                agent_id = agent_id.as_str(),
                "selected existing sole agent as local default during startup"
            );
        }
        agents::AgentDefaultEnsureOutcome::Updated { agent_id } => {
            info!(
                agent_id = agent_id.as_str(),
                workspace_root = %workspace_root.display(),
                "updated local default agent workspace root during startup"
            );
        }
        agents::AgentDefaultEnsureOutcome::SkippedMultipleAgents { observed_agent_count } => {
            warn!(
                observed_agent_count,
                "local agent registry has no default agent and multiple agents are present; leaving explicit selection to the operator"
            );
        }
    }
    Ok(())
}

/// Anchors a relative configured workspace root to the current directory so
/// the persisted agent record holds an absolute path.
fn default_agent_workspace_root(configured_workspace_root: &FsPath) -> Result<PathBuf> {
    if configured_workspace_root.is_absolute() {
        return Ok(configured_workspace_root.to_path_buf());
    }
    Ok(std::env::current_dir()
        .context("failed to resolve current directory for default agent workspace root")?
        .join(configured_workspace_root))
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

/// Resolves the daemon state root with precedence: `PALYRA_STATE_ROOT` env
/// override, then the identity store's parent directory, then the platform
/// default state root.
#[allow(clippy::result_large_err)]
fn resolve_runtime_state_root(identity_store_root: &FsPath) -> Result<PathBuf> {
    resolve_runtime_state_root_with_override(
        std::env::var_os("PALYRA_STATE_ROOT").map(PathBuf::from),
        identity_store_root,
    )
}

/// Env-free core of `resolve_runtime_state_root`, split out for tests.
fn resolve_runtime_state_root_with_override(
    state_root_override: Option<PathBuf>,
    identity_store_root: &FsPath,
) -> Result<PathBuf> {
    if let Some(state_root_override) = state_root_override {
        anyhow::ensure!(
            !state_root_override.as_os_str().is_empty(),
            "PALYRA_STATE_ROOT must not be empty"
        );
        return Ok(state_root_override);
    }
    if let Some(parent) = identity_store_root.parent() {
        return Ok(parent.to_path_buf());
    }
    default_state_root().context("failed to resolve default state root")
}

/// Resolves the skills storage root under the runtime state root.
///
/// Returns a ready-to-send HTTP error `Response` on failure because every
/// caller is an HTTP handler (hence the `result_large_err` allowance here and
/// on the sibling skills helpers).
#[allow(clippy::result_large_err)]
fn resolve_skills_root() -> Result<PathBuf, Response> {
    let identity_root = match std::env::var_os("PALYRA_GATEWAY_IDENTITY_STORE_DIR") {
        Some(raw) if raw.is_empty() => {
            return Err(runtime_status_response(tonic::Status::internal(
                "PALYRA_GATEWAY_IDENTITY_STORE_DIR must not be empty",
            )));
        }
        Some(raw) => PathBuf::from(raw),
        None => default_identity_store_root().map_err(|error| {
            runtime_status_response(tonic::Status::internal(format!(
                "failed to resolve default identity root: {error}"
            )))
        })?,
    };
    let state_root = resolve_runtime_state_root(identity_root.as_path()).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to resolve runtime state root for skills: {error}"
        )))
    })?;
    Ok(state_root.join("skills"))
}

/// Trust store path: `PALYRA_SKILLS_TRUST_STORE` override or the default
/// `trust-store.json` under the skills root.
fn resolve_skills_trust_store_path(skills_root: &FsPath) -> PathBuf {
    match std::env::var("PALYRA_SKILLS_TRUST_STORE") {
        Ok(raw) if !raw.trim().is_empty() => PathBuf::from(raw),
        _ => skills_root.join("trust-store.json"),
    }
}

/// Loads the skill trust store; a missing file yields the empty default
/// (first install runs under TOFU rules rather than failing).
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

/// Persists the skill trust store, mapping failures to an HTTP error response.
#[allow(clippy::result_large_err)]
fn save_trust_store(path: &FsPath, store: &SkillTrustStore) -> Result<(), Response> {
    store.save(path).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to persist trust store {}: {error}",
            path.display()
        )))
    })
}

/// Loads and normalizes the installed-skills index; a missing file yields the
/// empty default, and legacy layouts are migrated on read.
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
    let mut index: InstalledSkillsIndex = parse_versioned_json(
        payload.as_slice(),
        INSTALLED_SKILLS_INDEX_FORMAT,
        &[(0, migrate_updated_at_metadata_v0_to_v1)],
    )
    .map_err(|error| {
        runtime_status_response(tonic::Status::invalid_argument(format!(
            "failed to parse installed skills index {}: {error}",
            index_path.display()
        )))
    })?;
    normalize_installed_skills_index(&mut index);
    Ok(index)
}

/// Writes the installed-skills index after re-normalizing and stamping the
/// current schema version and timestamp.
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

/// Restores the index invariants: deterministic ordering and exactly one
/// `current` entry per skill (the first marked one wins; if none is marked,
/// the first entry in sort order is promoted).
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
    // Second pass: skills whose entries were all unmarked get their first
    // entry promoted. The or_insert_with closure deliberately mutates `entry`
    // as a side effect; it only runs for a skill_id not seen above.
    for entry in &mut index.entries {
        current_by_skill.entry(entry.skill_id.clone()).or_insert_with(|| {
            entry.current = true;
            true
        });
    }
}

/// Picks the effective version for a skill action: an explicit non-blank
/// `version` wins, otherwise the current (or first known) installed version.
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

/// Canonical on-disk location of a managed skill artifact:
/// `<skills_root>/<skill_id>/<version>/artifact.palyra-skill`.
fn managed_skill_artifact_path(skills_root: &FsPath, skill_id: &str, version: &str) -> PathBuf {
    skills_root.join(skill_id).join(version).join(SKILL_ARTIFACT_FILE_NAME)
}

/// Stable `snake_case` label persisted in the installed-skills index.
fn trust_decision_label(decision: palyra_skills::TrustDecision) -> String {
    match decision {
        palyra_skills::TrustDecision::Allowlisted => "allowlisted".to_owned(),
        palyra_skills::TrustDecision::TofuPinned => "tofu_pinned".to_owned(),
        palyra_skills::TrustDecision::TofuNewlyPinned => "tofu_newly_pinned".to_owned(),
    }
}

/// Lowercase hex SHA-256 digest; the canonical fingerprint format for token
/// hashes and artifact checksums across the daemon.
pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

/// Trims `value`, mapping blank input to `None`.
fn trim_to_option(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

/// Trims a required request field, rejecting blank values with a 400 response.
#[allow(clippy::result_large_err)]
fn normalize_non_empty_field(value: String, field_name: &'static str) -> Result<String, Response> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(format!(
            "{field_name} cannot be empty"
        ))));
    }
    // Skill ids are case-insensitive; lowercase here so index lookups and
    // on-disk paths share one canonical form.
    if field_name == "skill_id" {
        return Ok(trimmed.to_ascii_lowercase());
    }
    Ok(trimmed.to_owned())
}

/// Maps gateway auth failures onto HTTP status, error code, and category,
/// sanitizing the message before it leaves the process.
fn auth_error_response(error: AuthError) -> Response {
    let status = match error {
        AuthError::MissingConfiguredToken => StatusCode::SERVICE_UNAVAILABLE,
        AuthError::InvalidAuthorizationHeader | AuthError::InvalidToken => StatusCode::UNAUTHORIZED,
        AuthError::MissingContext(_) | AuthError::EmptyContext(_) | AuthError::InvalidDeviceId => {
            StatusCode::BAD_REQUEST
        }
    };
    let raw_error = error.to_string();
    let sanitized_error = sanitize_http_error_message(raw_error.as_str());
    let redacted = sanitized_error != raw_error;
    let (code, category, retryable) = match error {
        AuthError::MissingConfiguredToken => {
            ("service_unavailable", control_plane::ErrorCategory::Dependency, true)
        }
        AuthError::InvalidAuthorizationHeader | AuthError::InvalidToken => {
            ("unauthorized", control_plane::ErrorCategory::Auth, false)
        }
        AuthError::MissingContext(_) | AuthError::EmptyContext(_) | AuthError::InvalidDeviceId => {
            ("validation_error", control_plane::ErrorCategory::Validation, false)
        }
    };
    build_error_response(status, sanitized_error, code, category, retryable, Vec::new(), redacted)
}

/// Redacts secret-like values from an error message before it is returned to
/// HTTP clients, reusing the journal redaction rules with the provider
/// sanitizer as fallback when the round-trip through JSON fails.
fn sanitize_http_error_message(raw: &str) -> String {
    if raw.trim().is_empty() {
        return String::new();
    }
    let payload = json!({ "error": raw });
    match crate::journal::redact_payload_json(payload.to_string().as_bytes())
        .ok()
        .and_then(|redacted| serde_json::from_str::<Value>(&redacted).ok())
        .and_then(|parsed| parsed.get("error").and_then(Value::as_str).map(str::to_owned))
    {
        Some(value) => value,
        None => crate::model_provider::sanitize_remote_error(raw),
    }
}

/// Maps channel platform errors onto gRPC statuses (and through them HTTP
/// responses), distinguishing validation, precondition, not-found, and
/// availability failures.
fn channel_platform_error_response(error: channels::ChannelPlatformError) -> Response {
    let status = match &error {
        channels::ChannelPlatformError::InvalidInput(message) => {
            tonic::Status::invalid_argument(message.clone())
        }
        channels::ChannelPlatformError::Precondition(message) => {
            tonic::Status::failed_precondition(message.clone())
        }
        channels::ChannelPlatformError::UnsupportedConnector(message) => {
            tonic::Status::failed_precondition(message.clone())
        }
        channels::ChannelPlatformError::Supervisor(
            palyra_connectors::ConnectorSupervisorError::NotFound(message),
        ) => tonic::Status::not_found(message.clone()),
        channels::ChannelPlatformError::Supervisor(
            palyra_connectors::ConnectorSupervisorError::Validation(message),
        ) => tonic::Status::invalid_argument(message.clone()),
        channels::ChannelPlatformError::Supervisor(
            palyra_connectors::ConnectorSupervisorError::Router(message),
        ) if message.contains(
            "connector_token is required for RouteMessage when gateway auth is enabled",
        ) =>
        {
            tonic::Status::failed_precondition(message.clone())
        }
        channels::ChannelPlatformError::Supervisor(
            palyra_connectors::ConnectorSupervisorError::Router(message),
        ) => tonic::Status::unavailable(message.clone()),
        channels::ChannelPlatformError::Supervisor(
            palyra_connectors::ConnectorSupervisorError::Adapter(message),
        ) => tonic::Status::unavailable(message.clone()),
        _ => tonic::Status::internal(error.to_string()),
    };
    runtime_status_response(status)
}

/// Converts a `tonic::Status` into the daemon's HTTP error envelope, mapping
/// gRPC codes to HTTP statuses/categories and sanitizing the message.
///
/// This is the single choke point for handler errors on the HTTP surface, so
/// redaction and the error contract stay uniform.
pub(crate) fn runtime_status_response(status: tonic::Status) -> Response {
    let (http_status, code, category, retryable) = match status.code() {
        tonic::Code::Unauthenticated => {
            (StatusCode::UNAUTHORIZED, "unauthorized", control_plane::ErrorCategory::Auth, false)
        }
        tonic::Code::PermissionDenied => {
            (StatusCode::FORBIDDEN, "forbidden", control_plane::ErrorCategory::Policy, false)
        }
        tonic::Code::InvalidArgument => (
            StatusCode::BAD_REQUEST,
            "validation_error",
            control_plane::ErrorCategory::Validation,
            false,
        ),
        tonic::Code::FailedPrecondition => (
            StatusCode::PRECONDITION_FAILED,
            "failed_precondition",
            control_plane::ErrorCategory::Dependency,
            false,
        ),
        tonic::Code::Aborted => {
            (StatusCode::CONFLICT, "conflict", control_plane::ErrorCategory::Conflict, false)
        }
        tonic::Code::NotFound => {
            (StatusCode::NOT_FOUND, "not_found", control_plane::ErrorCategory::NotFound, false)
        }
        tonic::Code::ResourceExhausted => (
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limited",
            control_plane::ErrorCategory::Availability,
            true,
        ),
        tonic::Code::Unavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            "service_unavailable",
            control_plane::ErrorCategory::Availability,
            true,
        ),
        _ => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            control_plane::ErrorCategory::Internal,
            false,
        ),
    };
    let raw_message = status.message().to_owned();
    let sanitized_error = sanitize_http_error_message(raw_message.as_str());
    let redacted = sanitized_error != raw_message;
    build_error_response(
        http_status,
        sanitized_error,
        code,
        category,
        retryable,
        Vec::new(),
        redacted,
    )
}

/// Builds a 400 response carrying a single structured validation issue.
fn validation_error_response(field: &str, code: &str, message: &str) -> Response {
    build_error_response(
        StatusCode::BAD_REQUEST,
        sanitize_http_error_message(message),
        "validation_error",
        control_plane::ErrorCategory::Validation,
        false,
        vec![control_plane::ValidationIssue {
            field: field.to_owned(),
            code: code.to_owned(),
            message: message.to_owned(),
        }],
        false,
    )
}

/// Serializes the shared control-plane error envelope; `message` must already
/// be sanitized by the caller.
fn build_error_response(
    status: StatusCode,
    message: String,
    code: &str,
    category: control_plane::ErrorCategory,
    retryable: bool,
    validation_errors: Vec<control_plane::ValidationIssue>,
    redacted: bool,
) -> Response {
    let recovery_hint = recovery_hint_for_error_category(&category, retryable);
    let canonical = PalyraErrorEnvelope::new(
        palyra_error_category_from_control_plane(&category),
        code,
        message,
        recovery_hint,
        retryable,
        redacted,
    )
    .with_validation_errors(
        validation_errors
            .into_iter()
            .map(|issue| PalyraValidationIssue {
                field: issue.field,
                code: issue.code,
                message: issue.message,
            })
            .collect(),
    );
    (status, Json(control_plane::ErrorEnvelope::from(canonical))).into_response()
}

fn palyra_error_category_from_control_plane(
    category: &control_plane::ErrorCategory,
) -> PalyraErrorCategory {
    match category {
        control_plane::ErrorCategory::Auth => PalyraErrorCategory::Auth,
        control_plane::ErrorCategory::Validation => PalyraErrorCategory::Validation,
        control_plane::ErrorCategory::Policy => PalyraErrorCategory::Policy,
        control_plane::ErrorCategory::NotFound => PalyraErrorCategory::NotFound,
        control_plane::ErrorCategory::Conflict => PalyraErrorCategory::Conflict,
        control_plane::ErrorCategory::Dependency => PalyraErrorCategory::Dependency,
        control_plane::ErrorCategory::Availability => PalyraErrorCategory::Availability,
        control_plane::ErrorCategory::Internal => PalyraErrorCategory::Internal,
    }
}

fn recovery_hint_for_error_category(
    category: &control_plane::ErrorCategory,
    retryable: bool,
) -> &'static str {
    if retryable {
        return "retry the same request after the dependency or rate limit recovers";
    }
    match category {
        control_plane::ErrorCategory::Auth => {
            "refresh credentials or send a valid authorization header"
        }
        control_plane::ErrorCategory::Validation => "fix the request fields and retry",
        control_plane::ErrorCategory::Policy => {
            "request a permitted action or change policy through an authorized path"
        }
        control_plane::ErrorCategory::NotFound => {
            "refresh state and retry with an existing resource"
        }
        control_plane::ErrorCategory::Conflict => "refresh state and retry with a new version",
        control_plane::ErrorCategory::Dependency => {
            "inspect dependency health before retrying this operation"
        }
        control_plane::ErrorCategory::Availability => {
            "retry after the runtime reports the dependency is available"
        }
        control_plane::ErrorCategory::Internal => {
            "inspect daemon logs and retry only after the fault is resolved"
        }
    }
}

/// Startup preflight: admin auth may not be enabled without a usable token,
/// so misconfiguration fails the boot instead of yielding 503s at runtime.
fn validate_admin_auth_config(auth: &GatewayAuthConfig) -> Result<()> {
    if auth.require_auth && auth.admin_token.as_deref().is_none_or(|value| value.trim().is_empty())
    {
        anyhow::bail!(
            "admin auth is enabled but no admin token is configured; set PALYRA_ADMIN_TOKEN or admin.auth_token in config"
        );
    }
    Ok(())
}

/// Fail-closed preflight for the process-runner sandbox configuration.
///
/// Rejects combinations whose isolation guarantees cannot be honored: Tier C
/// on Windows (no OS-enforced backend yet), Tier B with strict egress (Tier B
/// cannot enforce it), and strict egress combined with host allowlists
/// (strict mode blocks all egress, so allowlists would silently lie).
///
/// # Errors
///
/// Returns a descriptive error naming the offending config keys for each of
/// the rejected combinations above.
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
        && matches!(tier, sandbox_runner::SandboxProcessRunnerTier::B)
        && matches!(egress_enforcement_mode, sandbox_runner::EgressEnforcementMode::Strict)
    {
        anyhow::bail!(
            "tool_call.process_runner.tier='b' does not support egress_enforcement_mode='strict'; use egress_enforcement_mode='preflight' or 'none', or opt into tier='c'"
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

/// Hydrates API keys for the primary model provider and every registry entry,
/// returning one audit record per secret actually resolved.
///
/// Credential precedence per provider: inline config key, then auth profile,
/// then `*_secret_ref`, then legacy `*_vault_ref` (upgraded to a secret ref
/// in place). `credential_source` is set to record which path won.
pub(crate) fn resolve_model_provider_secret(
    model_provider: &mut ModelProviderConfig,
    auth_registry: &AuthProfileRegistry,
    vault: &Vault,
    resolver: &SecretResolver<'_>,
) -> Result<Vec<SecretAccessAuditRecord>> {
    let mut audits = Vec::new();

    if let Some(access_audit) =
        resolve_primary_model_provider_secret(model_provider, auth_registry, vault, resolver)?
    {
        audits.push(access_audit);
    }

    for entry in &mut model_provider.registry.providers {
        if let Some(access_audit) =
            resolve_registry_provider_secret(entry, auth_registry, vault, resolver)?
        {
            audits.push(access_audit);
        }
    }

    Ok(audits)
}

/// Resolves the primary provider's API key following the precedence described
/// on `resolve_model_provider_secret`; returns `None` when nothing needed
/// resolving (inline key, deterministic provider, or no credential configured).
fn resolve_primary_model_provider_secret(
    model_provider: &mut ModelProviderConfig,
    auth_registry: &AuthProfileRegistry,
    _vault: &Vault,
    resolver: &SecretResolver<'_>,
) -> Result<Option<SecretAccessAuditRecord>> {
    let Some(expected_provider) = auth_provider_kind_for_model_provider(model_provider.kind) else {
        return Ok(None);
    };

    let inline_api_key = match model_provider.kind {
        ModelProviderKind::OpenAiCompatible => &mut model_provider.openai_api_key,
        ModelProviderKind::Anthropic => &mut model_provider.anthropic_api_key,
        ModelProviderKind::Deterministic => return Ok(None),
    };
    if inline_api_key.is_some() {
        model_provider.credential_source = Some(ModelProviderCredentialSource::InlineConfig);
        return Ok(None);
    }

    if let Some(auth_profile_id) = model_provider.auth_profile_id.clone() {
        let (decoded, access_audit, credential_source, resolved_profile_id) =
            resolve_provider_secret_from_auth_profile(
                auth_registry,
                resolver,
                auth_profile_id.as_str(),
                model_provider.auth_profile_provider_kind.unwrap_or(expected_provider),
                "model provider runtime",
                "model_provider.auth_profile",
            )?;
        *inline_api_key = Some(decoded);
        model_provider.auth_profile_id = Some(resolved_profile_id);
        model_provider.credential_source = Some(credential_source);
        return Ok(Some(access_audit));
    }

    let (secret_ref, config_path) = match model_provider.kind {
        ModelProviderKind::OpenAiCompatible => {
            if let Some(secret_ref) = model_provider.openai_api_key_secret_ref.clone() {
                (secret_ref, "model_provider.openai_api_key_secret_ref")
            } else if let Some(vault_ref) = model_provider.openai_api_key_vault_ref.clone() {
                let secret_ref = SecretRef::from_legacy_vault_ref(vault_ref);
                model_provider.openai_api_key_secret_ref = Some(secret_ref.clone());
                (secret_ref, "model_provider.openai_api_key_vault_ref")
            } else {
                return Ok(None);
            }
        }
        ModelProviderKind::Anthropic => {
            if let Some(secret_ref) = model_provider.anthropic_api_key_secret_ref.clone() {
                (secret_ref, "model_provider.anthropic_api_key_secret_ref")
            } else if let Some(vault_ref) = model_provider.anthropic_api_key_vault_ref.clone() {
                let secret_ref = SecretRef::from_legacy_vault_ref(vault_ref);
                model_provider.anthropic_api_key_secret_ref = Some(secret_ref.clone());
                (secret_ref, "model_provider.anthropic_api_key_vault_ref")
            } else {
                return Ok(None);
            }
        }
        ModelProviderKind::Deterministic => return Ok(None),
    };
    let action = match model_provider.kind {
        ModelProviderKind::OpenAiCompatible => "model_provider.openai_api_key.resolve",
        ModelProviderKind::Anthropic => "model_provider.anthropic_api_key.resolve",
        ModelProviderKind::Deterministic => "model_provider.api_key.resolve",
    };
    let (value, access_audit) = resolve_provider_secret_from_secret_ref(
        resolver,
        &secret_ref,
        "model provider API key",
        action,
        config_path,
    )?;
    *inline_api_key = Some(value);
    model_provider.credential_source = Some(secret_ref_credential_source(&secret_ref));
    Ok(Some(access_audit))
}

/// Registry-entry counterpart of `resolve_primary_model_provider_secret`,
/// with the same credential precedence and audit semantics.
fn resolve_registry_provider_secret(
    entry: &mut ProviderRegistryEntryConfig,
    auth_registry: &AuthProfileRegistry,
    _vault: &Vault,
    resolver: &SecretResolver<'_>,
) -> Result<Option<SecretAccessAuditRecord>> {
    let Some(expected_provider) = auth_provider_kind_for_model_provider(entry.kind) else {
        return Ok(None);
    };

    if entry.api_key.is_some() {
        entry.credential_source = Some(ModelProviderCredentialSource::InlineConfig);
        return Ok(None);
    }

    if let Some(auth_profile_id) = entry.auth_profile_id.clone() {
        let context_label = format!("provider registry entry '{}'", entry.provider_id);
        let action_prefix =
            format!("model_provider.registry.providers[{}].auth_profile", entry.provider_id);
        let (decoded, access_audit, credential_source, resolved_profile_id) =
            resolve_provider_secret_from_auth_profile(
                auth_registry,
                resolver,
                auth_profile_id.as_str(),
                entry.auth_profile_provider_kind.unwrap_or(expected_provider),
                context_label.as_str(),
                action_prefix.as_str(),
            )?;
        entry.api_key = Some(decoded);
        entry.auth_profile_id = Some(resolved_profile_id);
        entry.credential_source = Some(credential_source);
        return Ok(Some(access_audit));
    }

    let (secret_ref, config_path) = if let Some(secret_ref) = entry.api_key_secret_ref.clone() {
        (
            secret_ref,
            format!("model_provider.registry.providers[{}].api_key_secret_ref", entry.provider_id),
        )
    } else if let Some(vault_ref) = entry.api_key_vault_ref.clone() {
        let secret_ref = SecretRef::from_legacy_vault_ref(vault_ref);
        entry.api_key_secret_ref = Some(secret_ref.clone());
        (
            secret_ref,
            format!("model_provider.registry.providers[{}].api_key_vault_ref", entry.provider_id),
        )
    } else {
        return Ok(None);
    };
    let action_prefix = format!("model_provider.registry.providers[{}].api_key", entry.provider_id);
    let (value, access_audit) = resolve_provider_secret_from_secret_ref(
        resolver,
        &secret_ref,
        format!("provider registry entry '{}' API key", entry.provider_id).as_str(),
        action_prefix.as_str(),
        config_path.as_str(),
    )?;
    entry.api_key = Some(value);
    entry.credential_source = Some(secret_ref_credential_source(&secret_ref));
    Ok(Some(access_audit))
}

/// Auth-profile provider expected for a model provider kind; `None` for the
/// deterministic provider, which needs no credentials.
fn auth_provider_kind_for_model_provider(
    kind: ModelProviderKind,
) -> Option<ModelProviderAuthProviderKind> {
    match kind {
        ModelProviderKind::OpenAiCompatible => Some(ModelProviderAuthProviderKind::Openai),
        ModelProviderKind::Anthropic => Some(ModelProviderAuthProviderKind::Anthropic),
        ModelProviderKind::Deterministic => None,
    }
}

/// Loads the credential behind an auth profile (API key or OAuth access
/// token), rejecting profiles whose provider kind does not match the
/// configured expectation. Returns the decoded secret, the audit record, the
/// credential-source classification, and the resolved profile id.
fn resolve_provider_secret_from_auth_profile(
    auth_registry: &AuthProfileRegistry,
    resolver: &SecretResolver<'_>,
    auth_profile_id: &str,
    expected_provider: ModelProviderAuthProviderKind,
    context_label: &str,
    action_prefix: &str,
) -> Result<(String, SecretAccessAuditRecord, ModelProviderCredentialSource, String)> {
    let profile = auth_registry
        .get_profile(auth_profile_id)
        .with_context(|| {
            format!("failed to resolve auth profile '{}' for {}", auth_profile_id, context_label)
        })?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "{} auth profile '{}' was not found in auth registry",
                context_label,
                auth_profile_id
            )
        })?;

    let provider_matches = match expected_provider {
        ModelProviderAuthProviderKind::Openai => {
            matches!(profile.provider.kind, AuthProviderKind::Openai)
        }
        ModelProviderAuthProviderKind::Anthropic => {
            matches!(profile.provider.kind, AuthProviderKind::Anthropic)
        }
        ModelProviderAuthProviderKind::Minimax
        | ModelProviderAuthProviderKind::Xai
        | ModelProviderAuthProviderKind::GoogleGemini
        | ModelProviderAuthProviderKind::GoogleGeminiCli
        | ModelProviderAuthProviderKind::Openrouter => {
            matches!(profile.provider.kind, AuthProviderKind::Custom)
                && profile
                    .provider
                    .custom_name
                    .as_deref()
                    .is_some_and(|name| name.eq_ignore_ascii_case(expected_provider.as_str()))
        }
    };
    if !provider_matches {
        anyhow::bail!(
            "{} auth profile '{}' provider mismatch: expected '{}', got '{}'",
            context_label,
            profile.profile_id,
            expected_provider.as_str(),
            profile.provider.label()
        );
    }

    let (secret_ref, action, credential_source) = match &profile.credential {
        AuthCredential::ApiKey { api_key_vault_ref } => (
            SecretRef::from_legacy_vault_ref(api_key_vault_ref.clone()),
            format!("{action_prefix}.api_key.resolve"),
            ModelProviderCredentialSource::AuthProfileApiKey,
        ),
        AuthCredential::Oauth { access_token_vault_ref, .. } => (
            SecretRef::from_legacy_vault_ref(access_token_vault_ref.clone()),
            format!("{action_prefix}.oauth_access_token.resolve"),
            ModelProviderCredentialSource::AuthProfileOauthAccessToken,
        ),
    };
    let (value, access_audit) = resolve_provider_secret_from_secret_ref(
        resolver,
        &secret_ref,
        format!("{} credential from auth profile '{}'", context_label, profile.profile_id).as_str(),
        action.as_str(),
        format!("{action_prefix}.resolved_secret").as_str(),
    )?;
    Ok((value, access_audit, credential_source, profile.profile_id))
}

/// Resolves one secret reference to UTF-8 text plus its audit record; the
/// error path carries `context_label`, never the secret value.
fn resolve_provider_secret_from_secret_ref(
    resolver: &SecretResolver<'_>,
    secret_ref: &SecretRef,
    context_label: &str,
    action: &str,
    config_path: &str,
) -> Result<(String, SecretAccessAuditRecord)> {
    let resolution = resolver
        .resolve(secret_ref)
        .map_err(|error| anyhow::anyhow!("failed to resolve {context_label}: {}", error.message))?;
    let resolved_at_unix_ms = resolution.metadata.resolved_at_unix_ms;
    let decoded = resolution.decode_utf8(context_label)?;
    Ok((
        decoded,
        SecretAccessAuditRecord {
            action: action.to_owned(),
            config_path: config_path.to_owned(),
            secret_id: secret_ref.fingerprint(),
            source_kind: secret_ref.source_kind().to_owned(),
            resolved_at_unix_ms,
        },
    ))
}

/// Builds the host-owned runtime dispatcher from merged config and audited key material.
///
/// Secret-reference bytes are consumed only by the dispatcher constructor and
/// never copied into `LoadedConfig`, logs, diagnostics, or journal payloads.
/// Inline key material keeps the config layer's existing redacted-debug posture.
fn build_runtime_kernel_dispatcher(
    loaded: &config::LoadedConfig,
    resolver: &SecretResolver<'_>,
    v2_availability: application::runtime_kernel_v2::selection::V2RuntimeAvailability,
) -> Result<(
    Arc<application::runtime_kernel_v2::dispatcher::RuntimeKernelDispatcher>,
    Option<SecretAccessAuditRecord>,
)> {
    use application::runtime_kernel_v2::dispatcher::RuntimeKernelDispatcher;

    let mut resolved_secret = None;
    let mut audit = None;
    if let Some(config::RuntimeKernelSamplingKeySource::SecretRef(secret_ref)) =
        loaded.daemon.runtime_kernel.sampling_key_source.as_ref()
    {
        let resolution = resolver.resolve(secret_ref).map_err(|error| {
            anyhow::anyhow!("failed to resolve runtime-kernel sampling key: {}", error.message)
        })?;
        let resolved_at_unix_ms = resolution.metadata.resolved_at_unix_ms;
        resolved_secret = Some(resolution.require_bytes().map_err(|error| {
            anyhow::anyhow!(
                "failed to resolve runtime-kernel sampling key bytes: {}",
                error.message
            )
        })?);
        audit = Some(SecretAccessAuditRecord {
            action: "runtime_kernel.sampling_key.resolve".to_owned(),
            config_path: "runtime_kernel.sampling_key_secret_ref".to_owned(),
            secret_id: secret_ref.fingerprint(),
            source_kind: secret_ref.source_kind().to_owned(),
            resolved_at_unix_ms,
        });
    }

    let dispatcher = RuntimeKernelDispatcher::resolve(
        &loaded.daemon.runtime_kernel,
        &loaded.feature_rollouts,
        resolved_secret.as_ref().map(|secret| secret.as_ref()),
        runtime_kernel_explicit_shadow_enrollment(loaded)?,
        v2_availability,
    )
    .context("failed to initialize runtime-kernel dispatcher")?;
    Ok((Arc::new(dispatcher), audit))
}

fn production_runtime_kernel_v2_availability(
    provider_status: &model_provider::ProviderStatusSnapshot,
) -> application::runtime_kernel_v2::selection::V2RuntimeAvailability {
    use application::runtime_kernel_v2::selection::{
        V2RuntimeAvailability, V2UnavailabilityReason,
    };

    let mut selected_chat_routes = provider_status
        .route_selection
        .candidates
        .iter()
        .filter(|candidate| candidate.role == "chat" && candidate.selected);
    let provider_ready = selected_chat_routes.next().is_some_and(|candidate| {
        candidate.capability_state == "eligible"
            && !candidate.provider_id.trim().is_empty()
            && !candidate.model_id.trim().is_empty()
            && !candidate.credential_id.trim().is_empty()
    }) && selected_chat_routes.next().is_none();
    let harness_ready = application::agent_harness::AgentHarnessRegistry::with_embedded_default()
        .is_ok_and(|registry| registry.list().iter().any(|descriptor| descriptor.embedded_default));
    let context_snapshot =
        application::context_engine::ContextEngineRegistry::production_default().snapshot();
    let context_ready = context_snapshot.engines.iter().any(|descriptor| {
        descriptor.engine_id == context_snapshot.selected_engine_id
            && !descriptor.version.trim().is_empty()
    });

    if provider_ready && harness_ready && context_ready {
        V2RuntimeAvailability::Ready
    } else {
        warn!(
            provider_ready,
            harness_ready,
            context_ready,
            "runtime-kernel V2 dependencies are not ready; authoritative V2 admission remains blocked"
        );
        V2RuntimeAvailability::Unavailable(V2UnavailabilityReason::NotReady)
    }
}

fn runtime_kernel_explicit_shadow_enrollment(loaded: &config::LoadedConfig) -> Result<bool> {
    const ENV_NAME: &str = "PALYRA_QA_RUNTIME_KERNEL_SHADOW_EXPLICIT_BINDING";

    let Some(binding) = std::env::var_os(ENV_NAME) else {
        return Ok(false);
    };
    let binding = binding
        .into_string()
        .map_err(|_| anyhow::anyhow!("{ENV_NAME} contains non-unicode data"))?;
    let execution_key_digest = loaded
        .model_provider
        .qa_execution_key_digest
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("{ENV_NAME} requires QA execution metadata"))?;
    if loaded.qa_lab.mode != palyra_common::runtime_preview::RuntimePreviewMode::PreviewOnly
        || loaded.model_provider.qa_provider_binding_sha256.is_none()
        || loaded.daemon.runtime_kernel.profile != config::RuntimeKernelProfile::V2Shadow
        || binding != execution_key_digest
    {
        anyhow::bail!(
            "{ENV_NAME} requires qa_lab preview_only, bound QA provider metadata, and the v2_shadow profile"
        );
    }
    Ok(true)
}

/// Hydrates the admin auth/connector tokens and the browser-service token
/// from their secret refs, returning an audit record per resolution.
fn resolve_admin_and_browser_secret_refs(
    loaded: &mut config::LoadedConfig,
    resolver: &SecretResolver<'_>,
) -> Result<Vec<SecretAccessAuditRecord>> {
    let mut audits = Vec::new();

    if let Some(secret_ref) = loaded.admin.auth_token_secret_ref.as_ref() {
        let (decoded, audit) = resolve_provider_secret_from_secret_ref(
            resolver,
            secret_ref,
            "admin auth token",
            "admin.auth_token.resolve",
            "admin.auth_token_secret_ref",
        )?;
        loaded.admin.auth_token = Some(decoded);
        audits.push(audit);
    }
    if let Some(secret_ref) = loaded.admin.connector_token_secret_ref.as_ref() {
        let (decoded, audit) = resolve_provider_secret_from_secret_ref(
            resolver,
            secret_ref,
            "admin connector token",
            "admin.connector_token.resolve",
            "admin.connector_token_secret_ref",
        )?;
        loaded.admin.connector_token = Some(decoded);
        audits.push(audit);
    }
    if let Some(secret_ref) = loaded.tool_call.browser_service.auth_token_secret_ref.as_ref() {
        let (decoded, audit) = resolve_provider_secret_from_secret_ref(
            resolver,
            secret_ref,
            "browser service auth token",
            "tool_call.browser_service.auth_token.resolve",
            "tool_call.browser_service.auth_token_secret_ref",
        )?;
        loaded.tool_call.browser_service.auth_token = Some(decoded);
        audits.push(audit);
    }

    Ok(audits)
}

/// Probes every configured secret reference and records its health (without
/// retaining values) for the console secrets diagnostics view.
///
/// Individual resolution failures are captured in the records rather than
/// failing the snapshot.
///
/// # Errors
///
/// Returns an error only if the system clock reads before the UNIX epoch.
pub(crate) fn build_configured_secrets_state(
    loaded: &config::LoadedConfig,
    resolver: &SecretResolver<'_>,
    snapshot_generation: u64,
    resolution_scope: &str,
) -> Result<app::state::ConfiguredSecretsState> {
    let mut secrets = Vec::new();
    collect_configured_secret_record(
        &mut secrets,
        loaded.model_provider.openai_api_key_secret_ref.as_ref(),
        "model_provider",
        "model_provider.openai_api_key_secret_ref",
        resolver,
        snapshot_generation,
        resolution_scope,
    )?;
    collect_configured_secret_record(
        &mut secrets,
        loaded.model_provider.anthropic_api_key_secret_ref.as_ref(),
        "model_provider",
        "model_provider.anthropic_api_key_secret_ref",
        resolver,
        snapshot_generation,
        resolution_scope,
    )?;
    for provider in &loaded.model_provider.registry.providers {
        collect_configured_secret_record(
            &mut secrets,
            provider.api_key_secret_ref.as_ref(),
            "model_provider_registry",
            format!(
                "model_provider.registry.providers[{}].api_key_secret_ref",
                provider.provider_id
            )
            .as_str(),
            resolver,
            snapshot_generation,
            resolution_scope,
        )?;
    }
    collect_configured_secret_record(
        &mut secrets,
        loaded.tool_call.browser_service.auth_token_secret_ref.as_ref(),
        "browser_service",
        "tool_call.browser_service.auth_token_secret_ref",
        resolver,
        snapshot_generation,
        resolution_scope,
    )?;
    collect_configured_secret_record(
        &mut secrets,
        loaded.tool_call.browser_service.state_key_secret_ref.as_ref(),
        "browser_service",
        "tool_call.browser_service.state_key_secret_ref",
        resolver,
        snapshot_generation,
        resolution_scope,
    )?;
    collect_configured_secret_record(
        &mut secrets,
        loaded.admin.auth_token_secret_ref.as_ref(),
        "admin_auth",
        "admin.auth_token_secret_ref",
        resolver,
        snapshot_generation,
        resolution_scope,
    )?;
    collect_configured_secret_record(
        &mut secrets,
        loaded.admin.connector_token_secret_ref.as_ref(),
        "admin_auth",
        "admin.connector_token_secret_ref",
        resolver,
        snapshot_generation,
        resolution_scope,
    )?;
    let runtime_kernel_sampling_secret =
        match loaded.daemon.runtime_kernel.sampling_key_source.as_ref() {
            Some(config::RuntimeKernelSamplingKeySource::SecretRef(secret_ref)) => Some(secret_ref),
            Some(config::RuntimeKernelSamplingKeySource::Inline(_)) | None => None,
        };
    collect_configured_secret_record(
        &mut secrets,
        runtime_kernel_sampling_secret,
        "runtime_kernel",
        "runtime_kernel.sampling_key_secret_ref",
        resolver,
        snapshot_generation,
        resolution_scope,
    )?;

    Ok(app::state::ConfiguredSecretsState {
        generated_at_unix_ms: unix_ms_now()?,
        snapshot_generation,
        secrets,
    })
}

/// Appends the diagnostics record for one optional secret ref; resolution
/// failures become record fields (`last_error*`), not function errors.
fn collect_configured_secret_record(
    records: &mut Vec<control_plane::ConfiguredSecretRecord>,
    secret_ref: Option<&SecretRef>,
    component: &str,
    config_path: &str,
    resolver: &SecretResolver<'_>,
    snapshot_generation: u64,
    resolution_scope: &str,
) -> Result<()> {
    let Some(secret_ref) = secret_ref else {
        return Ok(());
    };
    let source = configured_secret_source_view(secret_ref);
    match resolver.resolve(secret_ref) {
        Ok(resolution) => {
            records.push(control_plane::ConfiguredSecretRecord {
                secret_id: format!("{}:{}", config_path, secret_ref.fingerprint()),
                component: component.to_owned(),
                config_path: config_path.to_owned(),
                status: match resolution.metadata.status {
                    SecretResolutionStatus::Resolved => "healthy",
                    SecretResolutionStatus::Missing => "missing",
                    SecretResolutionStatus::Blocked => "blocked",
                    SecretResolutionStatus::Failed => "failed",
                }
                .to_owned(),
                resolution_scope: resolution_scope.to_owned(),
                reload_action: reload_action_for_secret_path(config_path).to_owned(),
                snapshot_generation,
                source,
                affected_components: vec![component.to_owned()],
                last_resolved_at_unix_ms: Some(resolution.metadata.resolved_at_unix_ms),
                last_error_kind: None,
                last_error: None,
                value_bytes: None,
            });
        }
        Err(error) => {
            records.push(control_plane::ConfiguredSecretRecord {
                secret_id: format!("{}:{}", config_path, secret_ref.fingerprint()),
                component: component.to_owned(),
                config_path: config_path.to_owned(),
                status: match error.metadata.status {
                    SecretResolutionStatus::Resolved => "healthy",
                    SecretResolutionStatus::Missing => "missing",
                    SecretResolutionStatus::Blocked => "blocked",
                    SecretResolutionStatus::Failed => "failed",
                }
                .to_owned(),
                resolution_scope: resolution_scope.to_owned(),
                reload_action: reload_action_for_secret_path(config_path).to_owned(),
                snapshot_generation,
                source,
                affected_components: vec![component.to_owned()],
                last_resolved_at_unix_ms: Some(error.metadata.resolved_at_unix_ms),
                last_error_kind: Some(secret_resolve_error_kind_label(error.kind).to_owned()),
                last_error: Some(error.message),
                value_bytes: None,
            });
        }
    }
    Ok(())
}

/// Projects a secret ref's redacted metadata into the control-plane view type.
fn configured_secret_source_view(
    secret_ref: &SecretRef,
) -> control_plane::ConfiguredSecretSourceView {
    let redacted = secret_ref.redacted_view();
    control_plane::ConfiguredSecretSourceView {
        kind: redacted.kind,
        fingerprint: redacted.fingerprint,
        required: redacted.required,
        refresh_policy: redacted.refresh_policy,
        snapshot_policy: redacted.snapshot_policy,
        description: redacted.source.description,
        display_name: redacted.display_name,
        redaction_label: redacted.redaction_label,
        max_bytes: redacted.max_bytes,
        exec_timeout_ms: redacted.exec_timeout_ms,
        trusted_dir_count: redacted
            .source
            .trusted_dir_count
            .map(|value| u32::try_from(value).unwrap_or(u32::MAX)),
        inherited_env_count: redacted
            .source
            .inherited_env_count
            .map(|value| u32::try_from(value).unwrap_or(u32::MAX)),
        allow_symlinks: redacted.source.allow_symlinks,
    }
}

/// Stable `snake_case` label for a secret resolution failure kind.
fn secret_resolve_error_kind_label(kind: SecretResolveErrorKind) -> &'static str {
    match kind {
        SecretResolveErrorKind::Missing => "missing",
        SecretResolveErrorKind::InvalidReference => "invalid_reference",
        SecretResolveErrorKind::PolicyBlocked => "policy_blocked",
        SecretResolveErrorKind::Io => "io",
        SecretResolveErrorKind::TooLarge => "too_large",
        SecretResolveErrorKind::Timeout => "timeout",
        SecretResolveErrorKind::ExecFailed => "exec_failed",
        SecretResolveErrorKind::DecodeFailed => "decode_failed",
    }
}

/// What it takes to pick up a rotated secret at this config path: model
/// provider secrets reload once runs drain, browser/admin tokens need a
/// restart, everything else needs manual review.
fn reload_action_for_secret_path(config_path: &str) -> &'static str {
    if config_path.starts_with("model_provider.") {
        "blocked_while_runs_active"
    } else if config_path.starts_with("tool_call.browser_service.")
        || config_path.starts_with("admin.")
    {
        "restart_required"
    } else {
        "manual_review"
    }
}

/// Classifies a secret ref as the legacy vault path or the generic secret-ref
/// path for `credential_source` reporting.
fn secret_ref_credential_source(secret_ref: &SecretRef) -> ModelProviderCredentialSource {
    match secret_ref.source {
        SecretSource::Vault { .. } => ModelProviderCredentialSource::VaultRef,
        _ => ModelProviderCredentialSource::SecretRef,
    }
}

/// Directory used to resolve relative file/exec secret sources: the loaded
/// config file's directory, or the current directory when running on defaults.
///
/// # Errors
///
/// Returns an error if the current working directory cannot be resolved.
pub(crate) fn secret_resolution_working_dir(loaded: &config::LoadedConfig) -> Result<PathBuf> {
    // `loaded.source` is a provenance string like "<path> +env(...) +cli(...)";
    // the leading token is the config file path (or the literal "defaults").
    let source_path = loaded.source.split(" +env(").next().map(str::trim).unwrap_or("defaults");
    if source_path.eq_ignore_ascii_case("defaults") {
        return std::env::current_dir().context("failed to resolve current working directory");
    }
    let path = PathBuf::from(source_path);
    if path.is_dir() {
        Ok(path)
    } else if let Some(parent) = path.parent() {
        Ok(parent.to_path_buf())
    } else {
        std::env::current_dir().context("failed to resolve current working directory")
    }
}

/// Appends a `secret.accessed` audit event for one startup secret resolution,
/// attributed to the system daemon principal.
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
                "config_path": audit.config_path,
                "secret_id": audit.secret_id,
                "source_kind": audit.source_kind,
                "resolved_at_unix_ms": audit.resolved_at_unix_ms,
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

/// Current wall-clock time as UNIX milliseconds.
///
/// # Errors
///
/// Returns an error if the system clock reads before the UNIX epoch.
pub(crate) fn unix_ms_now() -> Result<i64> {
    let elapsed =
        SystemTime::now().duration_since(UNIX_EPOCH).context("system clock before UNIX epoch")?;
    Ok(elapsed.as_millis() as i64)
}

// Thin delegation shims: the implementations moved into the transport handler
// tree, but crate-root call sites and tests still use these short names.
fn parse_csv_values(raw: Option<&str>) -> Vec<String> {
    transport::http::handlers::console::channels::connectors::discord::parse_csv_values(raw)
}

fn connector_db_path_from_journal_path(journal_db_path: &FsPath) -> PathBuf {
    transport::http::handlers::console::channels::connectors::discord::connector_db_path_from_journal_path(
        journal_db_path,
    )
}

#[allow(clippy::result_large_err)]
fn parse_memory_sources_csv(raw: Option<&str>) -> Result<Vec<journal::MemorySource>, Response> {
    transport::http::handlers::console::channels::connectors::discord::parse_memory_sources_csv(raw)
}

/// Opens (or initializes) the identity store and issues the node-RPC server
/// certificate, producing the [`IdentityRuntime`] shared across transports.
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
    let gateway_ca_certificate_pem = manager.gateway_ca_certificate_pem();
    let node_server_certificate = manager
        .issue_gateway_server_certificate("palyrad-node-rpc")
        .context("failed to issue node RPC gateway certificate")?;
    let revoked_certificate_count = manager.revoked_certificate_fingerprints().len();
    Ok(IdentityRuntime {
        store_root,
        revoked_certificate_count,
        gateway_ca_certificate_pem,
        node_server_certificate,
        manager: Arc::new(Mutex::new(manager)),
    })
}

/// Builds the gateway TLS config from operator-provided cert/key paths, with
/// optional client-CA verification when `client_ca_path` is set.
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

/// Builds the node-RPC TLS config from daemon-issued identity material,
/// requiring client certificates signed by the gateway CA when mTLS is on.
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

/// The three listener addresses checked by the remote-bind guard.
#[derive(Debug, Clone, Copy)]
struct RemoteBindEndpoints {
    admin_address: SocketAddr,
    grpc_address: SocketAddr,
    quic_address: Option<SocketAddr>,
}

/// Security posture inputs evaluated by [`enforce_remote_bind_guard`].
#[derive(Debug, Clone, Copy)]
struct RemoteBindGuardConfig {
    bind_profile: config::GatewayBindProfile,
    allow_insecure_remote: bool,
    gateway_tls_enabled: bool,
    admin_auth_required: bool,
    admin_token_configured: bool,
    node_rpc_mtls_required: bool,
    config_dangerous_remote_bind_ack: bool,
    env_dangerous_remote_bind_ack: bool,
}

/// Fail-closed gate for exposing any listener beyond loopback.
///
/// Loopback-only binds always pass. A non-loopback bind requires all of: a
/// bind profile that allows remote exposure, gateway TLS, an authenticated
/// admin surface, node-RPC mTLS for remote gRPC/QUIC, active admin rate
/// limits, and the dual (config + env) dangerous-remote-bind acknowledgement.
///
/// # Errors
///
/// Returns an error naming the first unmet requirement; startup must abort.
fn enforce_remote_bind_guard(
    endpoints: RemoteBindEndpoints,
    config: RemoteBindGuardConfig,
) -> Result<()> {
    let admin_address = endpoints.admin_address;
    let grpc_address = endpoints.grpc_address;
    let quic_address = endpoints.quic_address;
    let admin_remote = !admin_address.ip().is_loopback();
    let grpc_remote = !grpc_address.ip().is_loopback();
    let quic_remote = quic_address.is_some_and(|address| !address.ip().is_loopback());
    let quic_display =
        quic_address.map(|address| address.to_string()).unwrap_or_else(|| "disabled".to_owned());
    let remote_bind_detected = admin_remote || grpc_remote || quic_remote;
    if !remote_bind_detected {
        return Ok(());
    }

    let bind_profile_allows_remote =
        matches!(config.bind_profile, config::GatewayBindProfile::PublicTls)
            || config.allow_insecure_remote;
    if !bind_profile_allows_remote {
        anyhow::bail!(
            "refusing non-loopback bind while gateway.bind_profile=loopback_only: admin={} grpc={} quic={} (set gateway.bind_profile=public_tls for hardened remote exposure, or keep loopback-only and use SSH tunnel/reverse proxy)",
            admin_address,
            grpc_address,
            quic_display,
        );
    }

    if !config.gateway_tls_enabled {
        anyhow::bail!(
            "refusing remote bind without TLS: admin={} grpc={} quic={} (set gateway.tls.enabled=true and configure cert/key paths)",
            admin_address,
            grpc_address,
            quic_display,
        );
    }

    if !config.admin_auth_required || !config.admin_token_configured {
        anyhow::bail!(
            "refusing remote bind without authenticated admin surface: admin.require_auth={} admin_token_configured={} (configure admin.require_auth=true with admin.auth_token or PALYRA_ADMIN_TOKEN)",
            config.admin_auth_required,
            config.admin_token_configured,
        );
    }

    if !config.node_rpc_mtls_required && (grpc_remote || quic_remote) {
        anyhow::bail!(
            "refusing remote gRPC/QUIC bind without node RPC mTLS: grpc={} quic={} (enable mTLS by keeping identity.allow_insecure_node_rpc_without_mtls=false)",
            grpc_address,
            quic_display,
        );
    }

    if !admin_rate_limiting_enabled() {
        anyhow::bail!(
            "refusing remote bind because admin API rate limits are disabled (window_ms={} max_requests={})",
            ADMIN_RATE_LIMIT_WINDOW_MS,
            ADMIN_RATE_LIMIT_MAX_REQUESTS_PER_WINDOW,
        );
    }

    if !(config.config_dangerous_remote_bind_ack && config.env_dangerous_remote_bind_ack) {
        anyhow::bail!(
            "refusing remote bind without explicit dual acknowledgement: deployment.dangerous_remote_bind_ack=true and {}=true are both required",
            DANGEROUS_REMOTE_BIND_ACK_ENV,
        );
    }

    Ok(())
}

/// True when the compile-time admin rate-limit constants are non-zero; the
/// remote-bind guard refuses remote exposure if limits are ever disabled.
fn admin_rate_limiting_enabled() -> bool {
    ADMIN_RATE_LIMIT_WINDOW_MS > 0 && ADMIN_RATE_LIMIT_MAX_REQUESTS_PER_WINDOW > 0
}

/// Reads the env half of the dual remote-bind acknowledgement; absent means
/// not acknowledged, malformed values fail startup.
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
        collections::{HashMap, HashSet},
        fs,
        net::IpAddr,
        path::PathBuf,
        str::FromStr,
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    };

    use axum::http::StatusCode;
    use palyra_auth::{
        AuthCredential, AuthProfileRegistry, AuthProfileScope, AuthProfileSetRequest, AuthProvider,
        AuthProviderKind,
    };
    use palyra_vault::{
        BackendPreference, SecretResolver, Vault, VaultConfig as VaultConfigOptions, VaultRef,
    };
    use serde_json::json;
    use tempfile::TempDir;

    use super::{
        build_discord_inbound_monitor_warnings, build_discord_onboarding_plan,
        build_discord_onboarding_security_defaults, build_memory_embedding_runtime_selection,
        clamp_console_relay_token_ttl_ms, connector_db_path_from_journal_path,
        constant_time_eq_bytes, consume_admin_auth_failure_rate_limit_with_now,
        consume_admin_rate_limit_with_now, consume_canvas_rate_limit_with_now,
        enforce_remote_bind_guard, finalize_discord_onboarding_plan, find_hashed_secret_map_key,
        load_installed_skills_index, loopback_grpc_url, mint_console_relay_token,
        mint_console_secret_token, normalize_discord_token, parse_offline_env_flag,
        production_runtime_kernel_v2_availability, prune_console_relay_tokens,
        redact_console_diagnostics_value, resolve_discord_intents_from_flags,
        resolve_model_provider_secret, resolve_runtime_state_root_with_override,
        runtime_status_response, sanitize_http_error_message, sha256_hex,
        spawn_networked_worker_expiry_loop_with_interval,
        spawn_process_lease_reconciliation_loop_with_interval,
        spawn_runtime_health_reconciliation_loop_with_interval, summarize_discord_inbound_monitor,
        validate_admin_auth_config, validate_canvas_http_canvas_id,
        validate_canvas_http_token_query, validate_process_runner_backend_policy,
        ConsoleRelayToken, DiscordBotIdentitySummary, DiscordOnboardingRequest,
        DiscordOnboardingScope, DiscordPrivilegedIntentStatus, RemoteBindEndpoints,
        RemoteBindGuardConfig, ADMIN_AUTH_FAILURE_RATE_LIMIT_MAX_REQUESTS_PER_WINDOW,
        ADMIN_RATE_LIMIT_LOOPBACK_MAX_REQUESTS_PER_WINDOW, ADMIN_RATE_LIMIT_MAX_IP_BUCKETS,
        ADMIN_RATE_LIMIT_MAX_REQUESTS_PER_WINDOW, CANVAS_HTTP_MAX_TOKEN_BYTES,
        CANVAS_RATE_LIMIT_MAX_IP_BUCKETS, CANVAS_RATE_LIMIT_MAX_REQUESTS_PER_WINDOW,
        CONSOLE_RELAY_TOKEN_DEFAULT_TTL_MS, CONSOLE_RELAY_TOKEN_MAX_TTL_MS,
        CONSOLE_RELAY_TOKEN_MIN_TTL_MS, DISCORD_APP_FLAG_GATEWAY_GUILD_MEMBERS,
        DISCORD_APP_FLAG_GATEWAY_MESSAGE_CONTENT, DISCORD_APP_FLAG_GATEWAY_PRESENCE,
        SKILLS_INDEX_FILE_NAME, SKILLS_LAYOUT_VERSION,
    };
    use crate::gateway::{tests::build_test_runtime_state, GatewayAuthConfig};
    use crate::model_provider::{
        ModelProviderAuthProviderKind, ModelProviderConfig, ModelProviderCredentialSource,
        ModelProviderKind, ProviderRegistryEntryConfig, ProviderRouteCandidateTrace,
        ProviderRouteSelectionTrace,
    };
    use crate::sandbox_runner::{EgressEnforcementMode, SandboxProcessRunnerTier};

    fn setup_auth_registry_and_vault() -> (TempDir, AuthProfileRegistry, Vault) {
        let tempdir = tempfile::tempdir().expect("temporary test directory should be created");
        let identity_store_root = tempdir.path().join("identity");
        fs::create_dir_all(&identity_store_root)
            .expect("identity store root should be created for auth/vault tests");
        let vault = Vault::open_with_config(VaultConfigOptions {
            root: Some(tempdir.path().join("vault")),
            identity_store_root: Some(identity_store_root.clone()),
            backend_preference: BackendPreference::EncryptedFile,
            ..VaultConfigOptions::default()
        })
        .expect("vault runtime should initialize");
        let auth_registry = AuthProfileRegistry::open(identity_store_root.as_path())
            .expect("auth profile registry should initialize");
        (tempdir, auth_registry, vault)
    }

    fn ready_v2_provider_status() -> crate::model_provider::ProviderStatusSnapshot {
        let runtime = build_test_runtime_state(false);
        let mut status = runtime.model_provider_status_snapshot();
        status.route_selection = ProviderRouteSelectionTrace {
            default_model_id: Some("deterministic-v2".to_owned()),
            failover_enabled: false,
            generated_at_unix_ms: 1,
            selected_provider_id: Some("deterministic".to_owned()),
            selected_model_id: Some("deterministic-v2".to_owned()),
            candidates: vec![ProviderRouteCandidateTrace {
                provider_id: "deterministic".to_owned(),
                credential_id: "credential:deterministic".to_owned(),
                model_id: "deterministic-v2".to_owned(),
                role: "chat".to_owned(),
                capability_state: "eligible".to_owned(),
                health_state: "healthy".to_owned(),
                selected: true,
                reason_code: "selected".to_owned(),
            }],
        };
        status
    }

    #[test]
    fn runtime_kernel_v2_becomes_ready_only_for_complete_production_dependencies() {
        assert_eq!(
            production_runtime_kernel_v2_availability(&ready_v2_provider_status()),
            crate::application::runtime_kernel_v2::selection::V2RuntimeAvailability::Ready
        );
    }

    #[test]
    fn runtime_kernel_v2_stays_not_ready_without_unique_selected_provider_route() {
        let mut status = ready_v2_provider_status();
        status.route_selection.candidates[0].selected = false;

        assert_eq!(
            production_runtime_kernel_v2_availability(&status),
            crate::application::runtime_kernel_v2::selection::V2RuntimeAvailability::Unavailable(
                crate::application::runtime_kernel_v2::selection::V2UnavailabilityReason::NotReady,
            )
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn networked_worker_expiry_loop_delays_first_periodic_pass() {
        let runtime = crate::gateway::tests::build_test_runtime_state_with_networked_worker_ttl(40);
        runtime
            .register_networked_worker(crate::gateway::tests::test_worker_attestation_for_lib(
                "worker-periodic-delay",
            ))
            .await
            .expect("worker registration should succeed");
        runtime
            .assign_networked_worker_lease(
                "worker-periodic-delay",
                crate::gateway::tests::test_worker_lease_request_for_lib(
                    "run-worker-periodic-delay",
                    40,
                ),
            )
            .await
            .expect("worker lease assignment should succeed");
        let handle = spawn_networked_worker_expiry_loop_with_interval(
            Arc::clone(&runtime),
            Duration::from_millis(200),
        );
        tokio::time::sleep(Duration::from_millis(80)).await;
        let before_tick = runtime.worker_fleet_snapshot();
        assert_eq!(before_tick.active_leases, 1);
        assert_eq!(before_tick.orphaned_workers, 0);

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let snapshot = runtime.worker_fleet_snapshot();
                if snapshot.orphaned_workers == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the first delayed expiry tick should run");
        handle.abort();
        let _ = handle.await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn networked_worker_expiry_loop_continues_after_journal_failure() {
        let runtime = crate::gateway::tests::build_test_runtime_state_with_networked_worker_ttl(30);
        runtime
            .register_networked_worker(crate::gateway::tests::test_worker_attestation_for_lib(
                "worker-periodic-retry",
            ))
            .await
            .expect("worker registration should succeed");
        runtime
            .assign_networked_worker_lease(
                "worker-periodic-retry",
                crate::gateway::tests::test_worker_lease_request_for_lib(
                    "run-worker-periodic-retry",
                    30,
                ),
            )
            .await
            .expect("worker lease assignment should succeed");
        let connection = rusqlite::Connection::open(&runtime.journal_config.db_path)
            .expect("test journal database should reopen");
        connection
            .execute_batch(
                r#"
                    CREATE TRIGGER fail_periodic_worker_expiry_journal
                    BEFORE INSERT ON journal_events
                    WHEN NEW.payload_json LIKE '%worker.ttl_expired%'
                    BEGIN
                        SELECT RAISE(ABORT, 'forced periodic worker expiry journal failure');
                    END;
                "#,
            )
            .expect("failure trigger should install");
        drop(connection);
        let handle = spawn_networked_worker_expiry_loop_with_interval(
            Arc::clone(&runtime),
            Duration::from_millis(60),
        );
        tokio::time::sleep(Duration::from_millis(110)).await;
        assert_eq!(runtime.worker_fleet_snapshot().orphaned_workers, 1);
        assert!(!handle.is_finished(), "one failed pass must not stop the periodic loop");

        let connection = rusqlite::Connection::open(&runtime.journal_config.db_path)
            .expect("test journal database should reopen");
        connection
            .execute_batch("DROP TRIGGER fail_periodic_worker_expiry_journal;")
            .expect("failure trigger should drop");
        drop(connection);
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let snapshot = runtime
                    .recent_journal_snapshot(100)
                    .await
                    .expect("journal snapshot should load");
                if snapshot.events.iter().any(|event| {
                    serde_json::from_str::<serde_json::Value>(event.payload_json.as_str())
                        .ok()
                        .and_then(|payload| {
                            payload
                                .pointer("/payload/details/reason_code")
                                .and_then(serde_json::Value::as_str)
                                .map(str::to_owned)
                        })
                        .as_deref()
                        == Some("worker.ttl_expired")
                }) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("a later periodic pass should persist retained expiry evidence");
        handle.abort();
        let _ = handle.await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn runtime_health_reconciliation_loop_delays_first_periodic_pass() {
        let runtime = build_test_runtime_state(false);
        let mut health = palyra_common::runtime_contracts::RuntimeComponentHealthV1 {
            schema_version: 1,
            component_id: palyra_common::runtime_contracts::RuntimeInstanceId::parse(
                "provider_periodic_health_probe",
            )
            .expect("component identity should validate"),
            generation: palyra_common::runtime_contracts::RuntimeGeneration::new(1)
                .expect("generation should validate"),
            state: palyra_common::runtime_contracts::RuntimeHealthState::Cooldown,
            authority_class:
                palyra_common::runtime_contracts::RuntimeAuthorityClass::ScopedMutation,
            strike_count: 3,
            reason_code: "runtime.health.cooldown".to_owned(),
            first_failure_at_unix_ms: Some(1),
            last_failure_at_unix_ms: Some(2),
            expires_at_unix_ms: Some(0),
            fallback_component_id: None,
            fallback_authority_class: None,
            security_quarantine: false,
            policy: palyra_common::runtime_contracts::CircuitBreakerPolicy {
                strike_threshold: 3,
                cooldown_ms: 1_000,
                max_probe_concurrency: 1,
                security_quarantine_auto_clear: false,
            },
            updated_at_unix_ms: 2,
        };
        runtime
            .journal_store
            .upsert_runtime_component_health(&health)
            .expect("health should persist");
        let lease = palyra_common::runtime_contracts::HealthProbeLeaseV1 {
            schema_version: 1,
            lease_id: palyra_common::runtime_contracts::RuntimeLeaseId::parse(
                "provider_periodic_health_lease",
            )
            .expect("lease identity should validate"),
            component_id: health.component_id.clone(),
            expected_generation: health.generation,
            authority_class: health.authority_class,
            issued_at_unix_ms: 10,
            expires_at_unix_ms: 20,
            non_mutating: true,
        };
        runtime
            .journal_store
            .begin_runtime_health_probe(&crate::journal::RuntimeHealthProbeBeginRequest {
                lease,
                reason_code: "runtime.health.probe_started".to_owned(),
                authorization_evidence_sha256: None,
                authorized_actor_id_sha256: None,
            })
            .expect("probe should begin");
        health.state = palyra_common::runtime_contracts::RuntimeHealthState::Probing;

        let handle = spawn_runtime_health_reconciliation_loop_with_interval(
            Arc::clone(&runtime),
            Duration::from_millis(200),
        );
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert_eq!(
            runtime
                .journal_store
                .runtime_component_health(health.component_id.as_str())
                .expect("health should load before first tick")
                .expect("health should remain present")
                .state,
            palyra_common::runtime_contracts::RuntimeHealthState::Probing
        );

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let current = runtime
                    .journal_store
                    .runtime_component_health(health.component_id.as_str())
                    .expect("health should load after periodic tick")
                    .expect("health should remain present");
                if current.state
                    == palyra_common::runtime_contracts::RuntimeHealthState::Quarantined
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the first delayed runtime health reconciliation tick should run");
        handle.abort();
        let _ = handle.await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn process_lease_reconciliation_loop_delays_first_periodic_pass() {
        let runtime = build_test_runtime_state(false);
        let generation = palyra_common::runtime_contracts::RuntimeGeneration::new(1)
            .expect("generation should validate");
        let instance_id = palyra_common::runtime_contracts::RuntimeInstanceId::parse(
            "process-periodic-delay-test",
        )
        .expect("instance id should validate");
        runtime
            .journal_store
            .register_process_handle_and_lease(
                &palyra_common::runtime_contracts::RuntimeHandleDescriptorV1 {
                    schema_version: 1,
                    instance_id: instance_id.clone(),
                    kind: palyra_common::runtime_contracts::RuntimeHandleKind::Process,
                    session_id: None,
                    run_id: None,
                    generation,
                    owner: "process-periodic-delay-test".to_owned(),
                    state: palyra_common::runtime_contracts::RuntimeHandleState::Running,
                    resume_metadata_json: None,
                    created_at_unix_ms: 1,
                    updated_at_unix_ms: 1,
                },
                &palyra_common::runtime_contracts::ProcessLeaseV1 {
                    schema_version: 1,
                    lease_id: palyra_common::runtime_contracts::RuntimeLeaseId::parse(
                        "process-periodic-delay-lease",
                    )
                    .expect("lease id should validate"),
                    instance_id,
                    generation,
                    pid: 4_500_000_u32.saturating_add(std::process::id()),
                    provenance: palyra_common::runtime_contracts::ProcessProvenance {
                        ownership_kind: palyra_common::runtime_contracts::ProcessOwnershipKind::RemoteExecutionInstance,
                        start_token: "process-periodic-delay-start".to_owned(),
                        executable_sha256: "a".repeat(64),
                        owner_nonce: "process-periodic-delay-owner".to_owned(),
                        ownership_identity_sha256: "b".repeat(64),
                    },
                    issued_at_unix_ms: 1,
                    expires_at_unix_ms: i64::MAX,
                    verified_at_unix_ms: 1,
                },
            )
            .expect("process lease should persist");
        let handle = spawn_process_lease_reconciliation_loop_with_interval(
            Arc::clone(&runtime),
            Duration::from_millis(200),
        );
        tokio::time::sleep(Duration::from_millis(40)).await;
        let before_tick = runtime
            .journal_store
            .shared_runtime_diagnostics()
            .expect("diagnostics should load before first tick");
        assert_eq!(before_tick.handles_by_state.get("running"), Some(&1));
        assert_eq!(before_tick.active_process_leases, 1);

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let diagnostics = runtime
                    .journal_store
                    .shared_runtime_diagnostics()
                    .expect("diagnostics should load after first tick");
                if diagnostics.handles_by_state.get("orphaned") == Some(&1) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("first delayed reconciliation tick should run");
        handle.abort();
        let _ = handle.await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn process_lease_reconciliation_passes_never_overlap() {
        let runtime = build_test_runtime_state(false);
        let first = {
            let runtime = Arc::clone(&runtime);
            tokio::spawn(async move { runtime.reconcile_persisted_process_leases_async().await })
        };
        let second = {
            let runtime = Arc::clone(&runtime);
            tokio::spawn(async move { runtime.reconcile_persisted_process_leases_async().await })
        };

        first
            .await
            .expect("first reconciliation task should join")
            .expect("first reconciliation should succeed");
        second
            .await
            .expect("second reconciliation task should join")
            .expect("second reconciliation should succeed");
        assert_eq!(runtime.process_lease_reconciliation_max_active(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn process_lease_reconciliation_loop_continues_after_failure() {
        let runtime = build_test_runtime_state(false);
        let generation = palyra_common::runtime_contracts::RuntimeGeneration::new(1)
            .expect("generation should validate");
        let instance_id = palyra_common::runtime_contracts::RuntimeInstanceId::parse(
            "process-periodic-failure-test",
        )
        .expect("instance id should validate");
        runtime
            .journal_store
            .register_process_handle_and_lease(
                &palyra_common::runtime_contracts::RuntimeHandleDescriptorV1 {
                    schema_version: 1,
                    instance_id: instance_id.clone(),
                    kind: palyra_common::runtime_contracts::RuntimeHandleKind::Process,
                    session_id: None,
                    run_id: None,
                    generation,
                    owner: "process-periodic-failure-test".to_owned(),
                    state: palyra_common::runtime_contracts::RuntimeHandleState::Running,
                    resume_metadata_json: None,
                    created_at_unix_ms: 1,
                    updated_at_unix_ms: 1,
                },
                &palyra_common::runtime_contracts::ProcessLeaseV1 {
                    schema_version: 1,
                    lease_id: palyra_common::runtime_contracts::RuntimeLeaseId::parse(
                        "process-periodic-failure-lease",
                    )
                    .expect("lease id should validate"),
                    instance_id: instance_id.clone(),
                    generation,
                    pid: 4_600_000_u32.saturating_add(std::process::id()),
                    provenance: palyra_common::runtime_contracts::ProcessProvenance {
                        ownership_kind: palyra_common::runtime_contracts::ProcessOwnershipKind::RemoteExecutionInstance,
                        start_token: "process-periodic-failure-start".to_owned(),
                        executable_sha256: "a".repeat(64),
                        owner_nonce: "process-periodic-failure-owner".to_owned(),
                        ownership_identity_sha256: "b".repeat(64),
                    },
                    issued_at_unix_ms: 1,
                    expires_at_unix_ms: i64::MAX,
                    verified_at_unix_ms: 1,
                },
            )
            .expect("process lease should persist");
        let connection = rusqlite::Connection::open(&runtime.journal_config.db_path)
            .expect("test journal database should reopen");
        connection
            .execute_batch(
                format!(
                    r#"
                        CREATE TRIGGER fail_periodic_process_reconciliation
                        BEFORE UPDATE ON runtime_handles
                        WHEN NEW.instance_ulid = '{}'
                        BEGIN
                            SELECT RAISE(ABORT, 'forced periodic reconciliation failure');
                        END;
                    "#,
                    instance_id.as_str()
                )
                .as_str(),
            )
            .expect("failure trigger should install");
        drop(connection);
        let handle = spawn_process_lease_reconciliation_loop_with_interval(
            Arc::clone(&runtime),
            Duration::from_millis(80),
        );
        tokio::time::sleep(Duration::from_millis(140)).await;
        let failed_pass = runtime
            .journal_store
            .shared_runtime_diagnostics()
            .expect("diagnostics should load after failed pass");
        assert_eq!(failed_pass.handles_by_state.get("running"), Some(&1));
        assert!(!handle.is_finished(), "one failed pass must not stop the periodic loop");

        let connection = rusqlite::Connection::open(&runtime.journal_config.db_path)
            .expect("test journal database should reopen");
        connection
            .execute_batch("DROP TRIGGER fail_periodic_process_reconciliation;")
            .expect("failure trigger should drop");
        drop(connection);
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let diagnostics = runtime
                    .journal_store
                    .shared_runtime_diagnostics()
                    .expect("diagnostics should load after recovery pass");
                if diagnostics.handles_by_state.get("orphaned") == Some(&1) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("a later periodic pass should recover after the failure is removed");
        handle.abort();
        let _ = handle.await;
    }

    #[test]
    fn load_installed_skills_index_migrates_legacy_metadata() {
        let tempdir = tempfile::tempdir().expect("temporary directory should be created");
        fs::write(tempdir.path().join(SKILLS_INDEX_FILE_NAME), br#"{"entries":[]}"#)
            .expect("legacy installed skills index should be written");
        let index = load_installed_skills_index(tempdir.path())
            .expect("legacy installed skills index should load");
        assert_eq!(index.schema_version, SKILLS_LAYOUT_VERSION);
        assert_eq!(index.updated_at_unix_ms, 0);
        assert!(index.entries.is_empty());
    }

    fn openai_model_provider_config() -> ModelProviderConfig {
        ModelProviderConfig {
            kind: ModelProviderKind::OpenAiCompatible,
            ..ModelProviderConfig::default()
        }
    }

    #[test]
    fn resolve_runtime_state_root_prefers_explicit_override() {
        let tempdir = tempfile::tempdir().expect("temporary test directory should be created");
        let state_root = tempdir.path().join("state-root");
        let identity_store_root = tempdir.path().join("custom").join("identity");

        let resolved = resolve_runtime_state_root_with_override(
            Some(state_root.clone()),
            identity_store_root.as_path(),
        )
        .expect("state root override should be accepted");

        assert_eq!(
            resolved, state_root,
            "explicit PALYRA_STATE_ROOT should take precedence over identity layout"
        );
    }

    #[test]
    fn resolve_runtime_state_root_falls_back_to_identity_parent() {
        let tempdir = tempfile::tempdir().expect("temporary test directory should be created");
        let state_root = tempdir.path().join("state-root");
        let identity_store_root = state_root.join("identity");

        let resolved =
            resolve_runtime_state_root_with_override(None, identity_store_root.as_path())
                .expect("identity parent should provide a state root");

        assert_eq!(
            resolved, state_root,
            "identity parent should back the daemon state root when no override is set"
        );
    }

    #[test]
    fn normalize_discord_token_strips_optional_bot_prefix() {
        assert_eq!(
            normalize_discord_token(" Bot abc.def "),
            Some("abc.def".to_owned()),
            "Bot prefix should be stripped"
        );
        assert_eq!(
            normalize_discord_token("token-only"),
            Some("token-only".to_owned()),
            "plain token should remain unchanged"
        );
        assert_eq!(normalize_discord_token("   "), None, "blank token should be rejected");
    }

    #[test]
    fn discord_intent_flags_map_to_enabled_statuses() {
        let flags = DISCORD_APP_FLAG_GATEWAY_MESSAGE_CONTENT
            | DISCORD_APP_FLAG_GATEWAY_GUILD_MEMBERS
            | DISCORD_APP_FLAG_GATEWAY_PRESENCE;
        let intents = resolve_discord_intents_from_flags(flags);
        assert!(
            matches!(intents.message_content, DiscordPrivilegedIntentStatus::Enabled),
            "message content flag should map to enabled"
        );
        assert!(
            matches!(intents.guild_members, DiscordPrivilegedIntentStatus::Enabled),
            "guild members flag should map to enabled"
        );
        assert!(
            matches!(intents.presence, DiscordPrivilegedIntentStatus::Enabled),
            "presence flag should map to enabled"
        );
    }

    #[test]
    fn discord_required_permissions_include_thread_send_permission() {
        let labels = super::discord_required_permission_labels();
        assert!(
            labels.iter().any(|label| label == "Send Messages in Threads"),
            "required permissions list should include thread reply capability"
        );
        let mask = super::discord_min_invite_permissions();
        assert_ne!(
            mask & super::DISCORD_PERMISSION_SEND_MESSAGES_IN_THREADS,
            0,
            "invite permissions mask should include Send Messages in Threads bit"
        );
    }

    #[test]
    fn discord_invite_permissions_mask_matches_required_baseline() {
        let mask = super::discord_min_invite_permissions();
        for (name, bit) in super::discord_required_permissions() {
            assert_ne!(
                mask & bit,
                0,
                "invite permissions mask should include required permission '{name}'"
            );
        }
        let mention_everyone_bit = 1_u64 << 17;
        let use_external_emojis_bit = 1_u64 << 18;
        assert_eq!(
            mask & mention_everyone_bit,
            0,
            "invite permissions mask should not include Mention Everyone by default"
        );
        assert_eq!(
            mask & use_external_emojis_bit,
            0,
            "invite permissions mask should not include Use External Emojis by default"
        );
    }

    #[test]
    fn normalize_optional_discord_channel_id_accepts_valid_values() {
        assert_eq!(
            super::normalize_optional_discord_channel_id(None).expect("none should be accepted"),
            None
        );
        assert_eq!(
            super::normalize_optional_discord_channel_id(Some("   "))
                .expect("blank should normalize to none"),
            None
        );
        assert_eq!(
            super::normalize_optional_discord_channel_id(Some("123456789012345678"))
                .expect("valid snowflake should normalize"),
            Some("123456789012345678".to_owned())
        );
    }

    #[test]
    fn normalize_optional_discord_channel_id_rejects_invalid_shapes() {
        let invalid_non_digit = super::normalize_optional_discord_channel_id(Some("abc123"))
            .expect_err("non-digit channel id should be rejected");
        assert_eq!(
            invalid_non_digit.status(),
            StatusCode::BAD_REQUEST,
            "non-digit verify_channel_id should map to 400"
        );
        let invalid_short = super::normalize_optional_discord_channel_id(Some("12345"))
            .expect_err("short channel id should be rejected");
        assert_eq!(
            invalid_short.status(),
            StatusCode::BAD_REQUEST,
            "short verify_channel_id should map to 400"
        );
    }

    #[test]
    fn discord_channel_permission_warnings_include_missing_permission_details() {
        let warnings = super::build_discord_channel_permission_warnings(Some(
            &super::DiscordChannelPermissionCheck {
                channel_id: "123456789012345678".to_owned(),
                status: super::DiscordChannelPermissionCheckStatus::Ok,
                can_view_channel: true,
                can_send_messages: false,
                can_read_message_history: false,
                can_embed_links: false,
                can_attach_files: false,
                can_send_messages_in_threads: false,
            },
        ));
        assert!(
            warnings.iter().any(|entry| entry.contains("Send Messages")),
            "warnings should include missing send messages permission"
        );
        assert!(
            warnings.iter().any(|entry| entry.contains("Send Messages in Threads")),
            "warnings should include missing thread send permission"
        );
    }

    #[test]
    fn discord_onboarding_plan_defaults_to_dm_only_safe_baseline() {
        let payload = DiscordOnboardingRequest {
            account_id: None,
            token: "token".to_owned(),
            mode: None,
            inbound_scope: None,
            allow_from: None,
            deny_from: None,
            require_mention: None,
            mention_patterns: None,
            concurrency_limit: None,
            direct_message_policy: None,
            broadcast_strategy: None,
            confirm_open_guild_channels: None,
            verify_channel_id: None,
        };
        let plan = build_discord_onboarding_plan(&payload)
            .expect("default onboarding payload should parse");
        assert!(
            matches!(plan.inbound_scope, DiscordOnboardingScope::DmOnly),
            "onboarding should default to DM-only scope"
        );
        assert!(plan.require_mention, "safe baseline should require mention by default");
        assert!(
            matches!(
                plan.direct_message_policy,
                crate::channel_router::DirectMessagePolicy::Pairing
            ),
            "safe baseline should default to DM pairing policy"
        );
        assert_eq!(plan.connector_id, "discord:default");
    }

    #[test]
    fn discord_onboarding_security_defaults_include_attachment_and_auth_posture() {
        let payload = DiscordOnboardingRequest {
            account_id: Some("default".to_owned()),
            token: "token".to_owned(),
            mode: None,
            inbound_scope: None,
            allow_from: None,
            deny_from: None,
            require_mention: None,
            mention_patterns: None,
            concurrency_limit: None,
            direct_message_policy: None,
            broadcast_strategy: None,
            confirm_open_guild_channels: None,
            verify_channel_id: None,
        };
        let plan = build_discord_onboarding_plan(&payload).expect("plan should parse");
        let defaults = build_discord_onboarding_security_defaults(&plan);
        assert!(
            defaults.iter().any(|entry| entry.contains("metadata only")),
            "security defaults should mention metadata-only attachment posture"
        );
        assert!(
            defaults.iter().any(|entry| entry.contains("connector_token")),
            "security defaults should mention connector-scoped auth posture"
        );
    }

    #[test]
    fn finalize_discord_onboarding_plan_preserves_custom_mentions() {
        let payload = DiscordOnboardingRequest {
            account_id: Some("default".to_owned()),
            token: "token".to_owned(),
            mode: None,
            inbound_scope: None,
            allow_from: None,
            deny_from: None,
            require_mention: Some(true),
            mention_patterns: Some(vec!["@ops".to_owned()]),
            concurrency_limit: None,
            direct_message_policy: None,
            broadcast_strategy: None,
            confirm_open_guild_channels: None,
            verify_channel_id: None,
        };
        let plan = build_discord_onboarding_plan(&payload).expect("plan should parse");
        let finalized = finalize_discord_onboarding_plan(
            plan,
            &DiscordBotIdentitySummary {
                id: "123456".to_owned(),
                username: "Palyra-Bot".to_owned(),
            },
        );
        assert_eq!(
            finalized.mention_patterns,
            vec!["@ops".to_owned()],
            "custom mention patterns should be preserved without appending default bot aliases"
        );
    }

    #[test]
    fn finalize_discord_onboarding_plan_adds_required_bot_mentions_when_missing() {
        let payload = DiscordOnboardingRequest {
            account_id: Some("default".to_owned()),
            token: "token".to_owned(),
            mode: None,
            inbound_scope: None,
            allow_from: None,
            deny_from: None,
            require_mention: Some(true),
            mention_patterns: None,
            concurrency_limit: None,
            direct_message_policy: None,
            broadcast_strategy: None,
            confirm_open_guild_channels: None,
            verify_channel_id: None,
        };
        let plan = build_discord_onboarding_plan(&payload).expect("plan should parse");
        let finalized = finalize_discord_onboarding_plan(
            plan,
            &DiscordBotIdentitySummary {
                id: "123456".to_owned(),
                username: "Palyra-Bot".to_owned(),
            },
        );
        assert!(
            finalized.mention_patterns.iter().any(|value| value == "<@123456>"),
            "canonical <@bot_id> mention should be present"
        );
        assert!(
            finalized.mention_patterns.iter().any(|value| value == "<@!123456>"),
            "canonical <@!bot_id> mention should be present"
        );
        assert!(
            finalized.mention_patterns.iter().any(|value| value == "@palyra-bot"),
            "bot username alias should be present"
        );
    }

    #[test]
    fn summarize_discord_inbound_monitor_marks_recent_inbound() {
        let now = super::unix_ms_now().expect("current unix ms should resolve");
        let runtime = json!({
            "inbound": {
                "gateway_connected": true,
                "last_inbound_unix_ms": now - 1_000,
                "last_connect_unix_ms": now - 10_000,
                "last_disconnect_unix_ms": null,
                "last_event_type": "MESSAGE_CREATE"
            }
        });
        let summary = summarize_discord_inbound_monitor(true, Some(&runtime));
        assert!(summary.connector_registered, "connector registration should be preserved");
        assert!(summary.gateway_connected, "gateway_connected should parse from runtime snapshot");
        assert!(summary.recent_inbound, "fresh inbound event should be marked as recent");
        assert!(
            super::discord_inbound_monitor_is_alive(&summary),
            "connected monitor with recent inbound should be marked alive"
        );
        assert_eq!(summary.last_event_type.as_deref(), Some("MESSAGE_CREATE"));
    }

    #[test]
    fn inbound_monitor_warnings_report_unconnected_gateway() {
        let runtime = json!({
            "inbound": {
                "gateway_connected": false
            }
        });
        let summary = summarize_discord_inbound_monitor(true, Some(&runtime));
        let warnings = build_discord_inbound_monitor_warnings(&summary);
        assert!(
            !super::discord_inbound_monitor_is_alive(&summary),
            "disconnected monitor must not be marked alive"
        );
        assert!(
            warnings.iter().any(|warning| warning.contains("not connected")),
            "unconnected monitor should emit actionable warning"
        );
    }

    #[test]
    fn inbound_monitor_is_not_alive_when_last_event_is_stale() {
        let now = super::unix_ms_now().expect("current unix ms should resolve");
        let runtime = json!({
            "inbound": {
                "gateway_connected": true,
                "last_inbound_unix_ms": now - (super::DISCORD_ONBOARDING_INBOUND_RECENT_WINDOW_MS + 1_000),
                "last_event_type": "MESSAGE_CREATE"
            }
        });
        let summary = summarize_discord_inbound_monitor(true, Some(&runtime));
        assert!(!summary.recent_inbound, "stale inbound timestamp should not be recent");
        assert!(
            !super::discord_inbound_monitor_is_alive(&summary),
            "connected monitor with stale inbound should not be marked alive"
        );
        let warnings = build_discord_inbound_monitor_warnings(&summary);
        assert!(
            warnings.iter().any(|warning| warning.contains("stale")),
            "stale inbound should surface actionable warning"
        );
    }

    #[test]
    fn parse_offline_env_flag_accepts_common_boolean_values() {
        assert!(parse_offline_env_flag("1").expect("1 should parse"));
        assert!(parse_offline_env_flag(" true ").expect("true should parse"));
        assert!(parse_offline_env_flag("YES").expect("yes should parse"));
        assert!(!parse_offline_env_flag("0").expect("0 should parse"));
        assert!(!parse_offline_env_flag("off").expect("off should parse"));
        assert!(!parse_offline_env_flag("  ").expect("blank should parse as false"));
    }

    #[test]
    fn parse_offline_env_flag_rejects_invalid_value() {
        let error =
            parse_offline_env_flag("sometimes").expect_err("invalid offline value should fail");
        assert!(
            error.to_string().contains("PALYRA_OFFLINE"),
            "error should mention PALYRA_OFFLINE"
        );
    }

    #[test]
    fn connector_db_path_uses_journal_file_stem_for_uniqueness() {
        let path =
            connector_db_path_from_journal_path(std::path::Path::new("C:/tmp/journal-a.sqlite3"));
        assert!(
            path.ends_with("journal-a.connectors.sqlite3"),
            "connector db path should derive from journal filename stem"
        );
    }

    #[test]
    fn connector_db_path_falls_back_to_data_default_when_parent_is_missing() {
        let path = connector_db_path_from_journal_path(std::path::Path::new("journal.sqlite3"));
        assert_eq!(path, PathBuf::from("data").join("connectors.sqlite3"));
    }

    #[test]
    fn build_memory_embedding_runtime_selection_marks_unknown_dims_as_degraded_fallback() {
        let mut config = openai_model_provider_config();
        config.openai_embeddings_model = Some("custom-embedding-model".to_owned());
        config.openai_embeddings_dims = None;

        let selection = build_memory_embedding_runtime_selection(&config, false)
            .expect("embedding runtime selection should succeed");
        assert!(
            !selection.profile.production_default_active,
            "unknown embedding dimensions should keep retrieval in degraded mode"
        );
        assert_eq!(
            selection.profile.degraded_reason_code.as_deref(),
            Some("embeddings_dimensions_unknown"),
            "degraded selection should explain that embedding dimensions are missing"
        );
    }

    #[test]
    fn build_memory_embedding_runtime_selection_uses_hash_fallback_in_explicit_offline_mode() {
        let mut config = openai_model_provider_config();
        config.openai_embeddings_model = Some("text-embedding-3-small".to_owned());
        config.openai_embeddings_dims = Some(8);

        let selection = build_memory_embedding_runtime_selection(&config, true)
            .expect("offline mode should allow hash fallback");
        assert_eq!(selection.provider.model_name(), "hash-embedding-v1");
        assert_eq!(selection.provider.dimensions(), 8);
        assert_eq!(
            selection.profile.degraded_reason_code.as_deref(),
            Some("offline_mode_enabled"),
            "offline mode should be surfaced explicitly in degraded retrieval posture"
        );
    }

    #[test]
    fn remote_bind_guard_allows_loopback_without_opt_in() {
        let result = enforce_remote_bind_guard(
            RemoteBindEndpoints {
                admin_address: "127.0.0.1:7142".parse().expect("loopback endpoint should parse"),
                grpc_address: "127.0.0.1:7443".parse().expect("loopback endpoint should parse"),
                quic_address: None,
            },
            RemoteBindGuardConfig {
                bind_profile: crate::config::GatewayBindProfile::LoopbackOnly,
                allow_insecure_remote: false,
                gateway_tls_enabled: false,
                admin_auth_required: true,
                admin_token_configured: true,
                node_rpc_mtls_required: true,
                config_dangerous_remote_bind_ack: false,
                env_dangerous_remote_bind_ack: false,
            },
        );
        assert!(result.is_ok(), "loopback bind should always be allowed");
    }

    #[test]
    fn remote_bind_guard_rejects_non_loopback_when_bind_profile_is_loopback_only() {
        let result = enforce_remote_bind_guard(
            RemoteBindEndpoints {
                admin_address: "0.0.0.0:7142".parse().expect("remote endpoint should parse"),
                grpc_address: "127.0.0.1:7443".parse().expect("loopback endpoint should parse"),
                quic_address: None,
            },
            RemoteBindGuardConfig {
                bind_profile: crate::config::GatewayBindProfile::LoopbackOnly,
                allow_insecure_remote: false,
                gateway_tls_enabled: true,
                admin_auth_required: true,
                admin_token_configured: true,
                node_rpc_mtls_required: true,
                config_dangerous_remote_bind_ack: true,
                env_dangerous_remote_bind_ack: true,
            },
        );
        assert!(result.is_err(), "loopback bind profile should block remote exposure");
    }

    #[test]
    fn remote_bind_guard_rejects_remote_bind_without_tls() {
        let result = enforce_remote_bind_guard(
            RemoteBindEndpoints {
                admin_address: "127.0.0.1:7142".parse().expect("loopback endpoint should parse"),
                grpc_address: "0.0.0.0:7443".parse().expect("remote endpoint should parse"),
                quic_address: None,
            },
            RemoteBindGuardConfig {
                bind_profile: crate::config::GatewayBindProfile::PublicTls,
                allow_insecure_remote: true,
                gateway_tls_enabled: false,
                admin_auth_required: true,
                admin_token_configured: true,
                node_rpc_mtls_required: true,
                config_dangerous_remote_bind_ack: true,
                env_dangerous_remote_bind_ack: true,
            },
        );
        assert!(result.is_err(), "remote bind without TLS must fail closed");
    }

    #[test]
    fn remote_bind_guard_rejects_remote_bind_without_admin_auth() {
        let result = enforce_remote_bind_guard(
            RemoteBindEndpoints {
                admin_address: "0.0.0.0:7142".parse().expect("remote endpoint should parse"),
                grpc_address: "0.0.0.0:7443".parse().expect("remote endpoint should parse"),
                quic_address: None,
            },
            RemoteBindGuardConfig {
                bind_profile: crate::config::GatewayBindProfile::PublicTls,
                allow_insecure_remote: true,
                gateway_tls_enabled: true,
                admin_auth_required: false,
                admin_token_configured: true,
                node_rpc_mtls_required: true,
                config_dangerous_remote_bind_ack: true,
                env_dangerous_remote_bind_ack: true,
            },
        );
        assert!(
            result.is_err(),
            "remote bind without authenticated admin surface must fail closed"
        );
    }

    #[test]
    fn remote_bind_guard_requires_dual_ack_for_remote_exposure() {
        let result = enforce_remote_bind_guard(
            RemoteBindEndpoints {
                admin_address: "127.0.0.1:7142".parse().expect("loopback endpoint should parse"),
                grpc_address: "0.0.0.0:7443".parse().expect("remote endpoint should parse"),
                quic_address: None,
            },
            RemoteBindGuardConfig {
                bind_profile: crate::config::GatewayBindProfile::PublicTls,
                allow_insecure_remote: true,
                gateway_tls_enabled: true,
                admin_auth_required: true,
                admin_token_configured: true,
                node_rpc_mtls_required: true,
                config_dangerous_remote_bind_ack: false,
                env_dangerous_remote_bind_ack: true,
            },
        );
        assert!(result.is_err(), "both config and env acknowledgements must be required");
    }

    #[test]
    fn remote_bind_guard_rejects_remote_grpc_without_node_rpc_mtls() {
        let result = enforce_remote_bind_guard(
            RemoteBindEndpoints {
                admin_address: "127.0.0.1:7142".parse().expect("loopback endpoint should parse"),
                grpc_address: "0.0.0.0:7443".parse().expect("remote endpoint should parse"),
                quic_address: None,
            },
            RemoteBindGuardConfig {
                bind_profile: crate::config::GatewayBindProfile::PublicTls,
                allow_insecure_remote: true,
                gateway_tls_enabled: true,
                admin_auth_required: true,
                admin_token_configured: true,
                node_rpc_mtls_required: false,
                config_dangerous_remote_bind_ack: true,
                env_dangerous_remote_bind_ack: true,
            },
        );
        assert!(result.is_err(), "remote gRPC should require node RPC mTLS");
    }

    #[test]
    fn remote_bind_guard_rejects_remote_quic_without_node_rpc_mtls() {
        let result = enforce_remote_bind_guard(
            RemoteBindEndpoints {
                admin_address: "127.0.0.1:7142".parse().expect("loopback endpoint should parse"),
                grpc_address: "127.0.0.1:7443".parse().expect("loopback endpoint should parse"),
                quic_address: Some(
                    "0.0.0.0:7444".parse().expect("remote QUIC endpoint should parse"),
                ),
            },
            RemoteBindGuardConfig {
                bind_profile: crate::config::GatewayBindProfile::PublicTls,
                allow_insecure_remote: true,
                gateway_tls_enabled: true,
                admin_auth_required: true,
                admin_token_configured: true,
                node_rpc_mtls_required: false,
                config_dangerous_remote_bind_ack: true,
                env_dangerous_remote_bind_ack: true,
            },
        );
        assert!(result.is_err(), "remote QUIC should require node RPC mTLS");
    }

    #[test]
    fn remote_bind_guard_allows_hardened_remote_profile() {
        let result = enforce_remote_bind_guard(
            RemoteBindEndpoints {
                admin_address: "0.0.0.0:7142".parse().expect("remote endpoint should parse"),
                grpc_address: "0.0.0.0:7443".parse().expect("remote endpoint should parse"),
                quic_address: None,
            },
            RemoteBindGuardConfig {
                bind_profile: crate::config::GatewayBindProfile::PublicTls,
                allow_insecure_remote: true,
                gateway_tls_enabled: true,
                admin_auth_required: true,
                admin_token_configured: true,
                node_rpc_mtls_required: true,
                config_dangerous_remote_bind_ack: true,
                env_dangerous_remote_bind_ack: true,
            },
        );
        assert!(result.is_ok(), "hardened public TLS profile should allow remote bind");
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
    fn model_provider_secret_resolver_prefers_auth_profile_over_legacy_vault_ref() {
        let (tempdir, auth_registry, vault) = setup_auth_registry_and_vault();
        let resolver = SecretResolver::with_working_dir(Some(&vault), tempdir.path());
        let legacy_ref =
            VaultRef::parse("global/openai_legacy_key").expect("legacy vault ref should parse");
        vault
            .put_secret(&legacy_ref.scope, legacy_ref.key.as_str(), b"sk-legacy")
            .expect("legacy model provider key should be written");
        let auth_ref =
            VaultRef::parse("global/openai_auth_key").expect("auth profile vault ref should parse");
        vault
            .put_secret(&auth_ref.scope, auth_ref.key.as_str(), b"sk-auth-profile")
            .expect("auth profile API key should be written");
        auth_registry
            .set_profile(AuthProfileSetRequest {
                profile_id: "openai-default".to_owned(),
                provider: AuthProvider::known(AuthProviderKind::Openai),
                profile_name: "OpenAI Default".to_owned(),
                scope: AuthProfileScope::Global,
                credential: AuthCredential::ApiKey {
                    api_key_vault_ref: "global/openai_auth_key".to_owned(),
                },
            })
            .expect("auth profile should be persisted");

        let mut model_provider = openai_model_provider_config();
        model_provider.auth_profile_id = Some("openai-default".to_owned());
        model_provider.auth_profile_provider_kind = Some(ModelProviderAuthProviderKind::Openai);
        model_provider.openai_api_key_vault_ref = Some("global/openai_legacy_key".to_owned());

        let audits =
            resolve_model_provider_secret(&mut model_provider, &auth_registry, &vault, &resolver)
                .expect("auth profile resolution should succeed");
        let audit =
            audits.into_iter().next().expect("audit record should be emitted for resolved secret");
        assert_eq!(
            model_provider.openai_api_key,
            Some("sk-auth-profile".to_owned()),
            "auth profile credential should override legacy model_provider vault ref"
        );
        assert_eq!(
            model_provider.credential_source,
            Some(ModelProviderCredentialSource::AuthProfileApiKey),
            "credential source should reflect auth profile API key path"
        );
        assert_eq!(audit.action, "model_provider.auth_profile.api_key.resolve");
        assert_eq!(audit.source_kind, "vault");
        assert_eq!(audit.config_path, "model_provider.auth_profile.resolved_secret");
    }

    #[test]
    fn model_provider_secret_resolver_uses_legacy_vault_ref_when_auth_profile_is_unset() {
        let (tempdir, auth_registry, vault) = setup_auth_registry_and_vault();
        let resolver = SecretResolver::with_working_dir(Some(&vault), tempdir.path());
        let legacy_ref =
            VaultRef::parse("global/openai_legacy_key").expect("legacy vault ref should parse");
        vault
            .put_secret(&legacy_ref.scope, legacy_ref.key.as_str(), b"sk-legacy")
            .expect("legacy model provider key should be written");

        let mut model_provider = openai_model_provider_config();
        model_provider.openai_api_key_vault_ref = Some("global/openai_legacy_key".to_owned());

        let audits =
            resolve_model_provider_secret(&mut model_provider, &auth_registry, &vault, &resolver)
                .expect("legacy vault-ref resolution should succeed");
        let audit =
            audits.into_iter().next().expect("audit record should be emitted for resolved secret");
        assert_eq!(
            model_provider.openai_api_key,
            Some("sk-legacy".to_owned()),
            "resolver should populate model provider API key from legacy vault ref"
        );
        assert_eq!(
            model_provider.credential_source,
            Some(ModelProviderCredentialSource::VaultRef),
            "credential source should reflect legacy vault-ref path"
        );
        assert_eq!(audit.action, "model_provider.openai_api_key.resolve");
        assert_eq!(audit.source_kind, "vault");
        assert_eq!(audit.config_path, "model_provider.openai_api_key_vault_ref");
    }

    #[test]
    fn model_provider_secret_resolver_rejects_auth_profile_provider_mismatch() {
        let (tempdir, auth_registry, vault) = setup_auth_registry_and_vault();
        let resolver = SecretResolver::with_working_dir(Some(&vault), tempdir.path());
        auth_registry
            .set_profile(AuthProfileSetRequest {
                profile_id: "anthropic-default".to_owned(),
                provider: AuthProvider::known(AuthProviderKind::Anthropic),
                profile_name: "Anthropic Default".to_owned(),
                scope: AuthProfileScope::Global,
                credential: AuthCredential::ApiKey {
                    api_key_vault_ref: "global/anthropic_api_key".to_owned(),
                },
            })
            .expect("anthropic profile should be persisted");

        let mut model_provider = openai_model_provider_config();
        model_provider.auth_profile_id = Some("anthropic-default".to_owned());
        model_provider.auth_profile_provider_kind = Some(ModelProviderAuthProviderKind::Openai);

        let error =
            resolve_model_provider_secret(&mut model_provider, &auth_registry, &vault, &resolver)
                .expect_err("provider mismatch should fail closed");
        assert!(
            error.to_string().contains("provider mismatch"),
            "resolver should explain provider mismatch when auth profile kind is incompatible"
        );
    }

    #[test]
    fn model_provider_secret_resolver_accepts_minimax_custom_auth_profile() {
        let (tempdir, auth_registry, vault) = setup_auth_registry_and_vault();
        let resolver = SecretResolver::with_working_dir(Some(&vault), tempdir.path());
        let auth_ref =
            VaultRef::parse("global/minimax_api_key").expect("minimax vault ref should parse");
        vault
            .put_secret(&auth_ref.scope, auth_ref.key.as_str(), b"minimax-secret")
            .expect("minimax auth profile API key should be written");
        auth_registry
            .set_profile(AuthProfileSetRequest {
                profile_id: "minimax-default".to_owned(),
                provider: AuthProvider {
                    kind: AuthProviderKind::Custom,
                    custom_name: Some("minimax".to_owned()),
                },
                profile_name: "MiniMax Default".to_owned(),
                scope: AuthProfileScope::Global,
                credential: AuthCredential::ApiKey {
                    api_key_vault_ref: "global/minimax_api_key".to_owned(),
                },
            })
            .expect("minimax auth profile should be persisted");

        let mut model_provider = ModelProviderConfig {
            kind: ModelProviderKind::Anthropic,
            auth_profile_id: Some("minimax-default".to_owned()),
            auth_profile_provider_kind: Some(ModelProviderAuthProviderKind::Minimax),
            ..ModelProviderConfig::default()
        };

        let audits =
            resolve_model_provider_secret(&mut model_provider, &auth_registry, &vault, &resolver)
                .expect("minimax custom auth profile resolution should succeed");
        let audit =
            audits.into_iter().next().expect("audit record should be emitted for resolved secret");
        assert_eq!(
            model_provider.anthropic_api_key.as_deref(),
            Some("minimax-secret"),
            "MiniMax should hydrate the Anthropic-compatible transport credential"
        );
        assert_eq!(
            model_provider.credential_source,
            Some(ModelProviderCredentialSource::AuthProfileApiKey)
        );
        assert_eq!(audit.action, "model_provider.auth_profile.api_key.resolve");
    }

    #[test]
    fn model_provider_secret_resolver_accepts_xai_custom_auth_profile() {
        let (tempdir, auth_registry, vault) = setup_auth_registry_and_vault();
        let resolver = SecretResolver::with_working_dir(Some(&vault), tempdir.path());
        let auth_ref = VaultRef::parse("global/xai_api_key").expect("xAI vault ref should parse");
        vault
            .put_secret(&auth_ref.scope, auth_ref.key.as_str(), b"xai-secret")
            .expect("xAI auth profile API key should be written");
        auth_registry
            .set_profile(AuthProfileSetRequest {
                profile_id: "xai-default".to_owned(),
                provider: AuthProvider {
                    kind: AuthProviderKind::Custom,
                    custom_name: Some("xai".to_owned()),
                },
                profile_name: "xAI Default".to_owned(),
                scope: AuthProfileScope::Global,
                credential: AuthCredential::ApiKey {
                    api_key_vault_ref: "global/xai_api_key".to_owned(),
                },
            })
            .expect("xAI auth profile should be persisted");

        let mut model_provider = openai_model_provider_config();
        model_provider.auth_profile_id = Some("xai-default".to_owned());
        model_provider.auth_profile_provider_kind = Some(ModelProviderAuthProviderKind::Xai);

        let audits =
            resolve_model_provider_secret(&mut model_provider, &auth_registry, &vault, &resolver)
                .expect("xAI custom auth profile resolution should succeed");
        let audit =
            audits.into_iter().next().expect("audit record should be emitted for resolved secret");

        assert_eq!(
            model_provider.openai_api_key.as_deref(),
            Some("xai-secret"),
            "xAI should hydrate the OpenAI-compatible transport credential"
        );
        assert_eq!(
            model_provider.credential_source,
            Some(ModelProviderCredentialSource::AuthProfileApiKey)
        );
        assert_eq!(audit.action, "model_provider.auth_profile.api_key.resolve");
    }

    #[test]
    fn model_provider_secret_resolver_loads_oauth_access_token_from_auth_profile() {
        let (tempdir, auth_registry, vault) = setup_auth_registry_and_vault();
        let resolver = SecretResolver::with_working_dir(Some(&vault), tempdir.path());
        let access_ref = VaultRef::parse("global/openai_access_token")
            .expect("oauth access-token vault ref should parse");
        vault
            .put_secret(&access_ref.scope, access_ref.key.as_str(), b"oauth-access-token")
            .expect("oauth access token should be written");
        auth_registry
            .set_profile(AuthProfileSetRequest {
                profile_id: "openai-oauth".to_owned(),
                provider: AuthProvider::known(AuthProviderKind::Openai),
                profile_name: "OpenAI OAuth".to_owned(),
                scope: AuthProfileScope::Global,
                credential: AuthCredential::Oauth {
                    access_token_vault_ref: "global/openai_access_token".to_owned(),
                    refresh_token_vault_ref: "global/openai_refresh_token".to_owned(),
                    token_endpoint: "https://oauth.example.com/token".to_owned(),
                    client_id: None,
                    client_secret_vault_ref: None,
                    scopes: Vec::new(),
                    expires_at_unix_ms: None,
                    refresh_state: Default::default(),
                },
            })
            .expect("openai oauth profile should be persisted");

        let mut model_provider = openai_model_provider_config();
        model_provider.auth_profile_id = Some("openai-oauth".to_owned());
        model_provider.auth_profile_provider_kind = Some(ModelProviderAuthProviderKind::Openai);

        let audits =
            resolve_model_provider_secret(&mut model_provider, &auth_registry, &vault, &resolver)
                .expect("oauth auth profile resolution should succeed");
        let audit = audits
            .into_iter()
            .next()
            .expect("audit record should be emitted for resolved oauth token");
        assert_eq!(
            model_provider.openai_api_key,
            Some("oauth-access-token".to_owned()),
            "resolver should hydrate provider API key from oauth access token vault ref"
        );
        assert_eq!(
            model_provider.credential_source,
            Some(ModelProviderCredentialSource::AuthProfileOauthAccessToken),
            "credential source should identify oauth access-token path"
        );
        assert_eq!(
            audit.action, "model_provider.auth_profile.oauth_access_token.resolve",
            "audit action should capture oauth credential source"
        );
    }

    #[test]
    fn model_provider_secret_resolver_hydrates_registry_provider_auth_profile() {
        let (tempdir, auth_registry, vault) = setup_auth_registry_and_vault();
        let resolver = SecretResolver::with_working_dir(Some(&vault), tempdir.path());
        let auth_ref = VaultRef::parse("global/anthropic_auth_key")
            .expect("auth profile vault ref should parse");
        vault
            .put_secret(&auth_ref.scope, auth_ref.key.as_str(), b"anthropic-secret")
            .expect("anthropic auth profile API key should be written");
        auth_registry
            .set_profile(AuthProfileSetRequest {
                profile_id: "anthropic-default".to_owned(),
                provider: AuthProvider::known(AuthProviderKind::Anthropic),
                profile_name: "Anthropic Default".to_owned(),
                scope: AuthProfileScope::Global,
                credential: AuthCredential::ApiKey {
                    api_key_vault_ref: "global/anthropic_auth_key".to_owned(),
                },
            })
            .expect("anthropic auth profile should be persisted");

        let mut model_provider = ModelProviderConfig::default();
        model_provider.registry.providers = vec![ProviderRegistryEntryConfig {
            provider_id: "anthropic-primary".to_owned(),
            display_name: Some("Anthropic".to_owned()),
            kind: ModelProviderKind::Anthropic,
            base_url: Some("https://api.anthropic.com".to_owned()),
            allow_private_base_url: false,
            enabled: true,
            auth_profile_id: Some("anthropic-default".to_owned()),
            auth_profile_provider_kind: Some(ModelProviderAuthProviderKind::Anthropic),
            api_key: None,
            api_key_secret_ref: None,
            api_key_vault_ref: None,
            credential_source: None,
            request_timeout_ms: 15_000,
            max_retries: 2,
            retry_backoff_ms: 100,
            circuit_breaker_failure_threshold: 3,
            circuit_breaker_cooldown_ms: 30_000,
        }];

        let audits =
            resolve_model_provider_secret(&mut model_provider, &auth_registry, &vault, &resolver)
                .expect("registry auth profile resolution should succeed");

        assert_eq!(audits.len(), 1);
        assert_eq!(
            model_provider.registry.providers[0].api_key.as_deref(),
            Some("anthropic-secret")
        );
        assert_eq!(
            model_provider.registry.providers[0].credential_source,
            Some(ModelProviderCredentialSource::AuthProfileApiKey)
        );
        assert_eq!(
            audits[0].action,
            "model_provider.registry.providers[anthropic-primary].auth_profile.api_key.resolve"
        );
    }

    #[test]
    fn model_provider_secret_resolver_hydrates_registry_provider_legacy_vault_ref() {
        let (tempdir, auth_registry, vault) = setup_auth_registry_and_vault();
        let resolver = SecretResolver::with_working_dir(Some(&vault), tempdir.path());
        let legacy_ref = VaultRef::parse("global/anthropic_legacy_key")
            .expect("legacy provider vault ref should parse");
        vault
            .put_secret(&legacy_ref.scope, legacy_ref.key.as_str(), b"anthropic-legacy")
            .expect("legacy registry provider key should be written");

        let mut model_provider = ModelProviderConfig::default();
        model_provider.registry.providers = vec![ProviderRegistryEntryConfig {
            provider_id: "anthropic-primary".to_owned(),
            display_name: Some("Anthropic".to_owned()),
            kind: ModelProviderKind::Anthropic,
            base_url: Some("https://api.anthropic.com".to_owned()),
            allow_private_base_url: false,
            enabled: true,
            auth_profile_id: None,
            auth_profile_provider_kind: Some(ModelProviderAuthProviderKind::Anthropic),
            api_key: None,
            api_key_secret_ref: None,
            api_key_vault_ref: Some("global/anthropic_legacy_key".to_owned()),
            credential_source: None,
            request_timeout_ms: 15_000,
            max_retries: 2,
            retry_backoff_ms: 100,
            circuit_breaker_failure_threshold: 3,
            circuit_breaker_cooldown_ms: 30_000,
        }];

        let audits =
            resolve_model_provider_secret(&mut model_provider, &auth_registry, &vault, &resolver)
                .expect("legacy registry provider vault-ref resolution should succeed");

        assert_eq!(audits.len(), 1);
        assert_eq!(
            model_provider.registry.providers[0].api_key.as_deref(),
            Some("anthropic-legacy")
        );
        assert_eq!(
            model_provider.registry.providers[0].credential_source,
            Some(ModelProviderCredentialSource::VaultRef)
        );
        assert!(model_provider.registry.providers[0].api_key_secret_ref.is_some());
        assert_eq!(
            audits[0].config_path,
            "model_provider.registry.providers[anthropic-primary].api_key_vault_ref"
        );
    }

    #[test]
    fn runtime_status_response_maps_resource_exhausted_to_too_many_requests() {
        let response = runtime_status_response(tonic::Status::resource_exhausted("rate limited"));
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn runtime_status_response_maps_aborted_to_non_retryable_conflict() {
        let response = runtime_status_response(tonic::Status::aborted(
            "flow revision conflict for flow-1: expected 1, found 2",
        ));
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("conflict response body should be readable");
        let envelope = serde_json::from_slice::<serde_json::Value>(body.as_ref())
            .expect("conflict response should be JSON");

        assert_eq!(envelope["code"], "conflict");
        assert_eq!(envelope["category"], "conflict");
        assert_eq!(envelope["retryable"], false);
    }

    #[test]
    fn sanitize_http_error_message_redacts_secret_like_values() {
        let sanitized = sanitize_http_error_message(
            "provider failed: bearer topsecret token=abc123 cookie: sessionid=xyz",
        );
        assert!(
            sanitized.contains("<redacted>"),
            "sanitized error text should include redaction marker"
        );
        assert!(
            !sanitized.contains("topsecret")
                && !sanitized.contains("token=abc123")
                && !sanitized.contains("sessionid=xyz"),
            "sanitized error text must not leak secret-like values: {sanitized}"
        );
    }

    #[test]
    fn diagnostics_redaction_masks_sensitive_keys_and_query_values() {
        let mut payload = serde_json::json!({
            "authorization": "Bearer topsecret",
            "endpoint": "https://example.test/callback?access_token=alpha&mode=ok",
            "error_message": "provider failure https://example.test/callback?state=ok&access_token=abc123",
            "error_detail": "provider detail https://example.test/callback?state=ok#refresh_token=refresh-secret&mode=ok",
            "browserd": {
                "relay_token": "relay-secret",
                "downloads_endpoint": "https://example.test/downloads?token=browser-secret&mode=ok",
                "last_error": "Bearer browser-secret"
            },
            "channels": {
                "discord:default": {
                    "runtime": {
                        "last_error": "authorization=discord-secret"
                    },
                    "webhook_url": "https://discord.test/api/webhooks/1?token=hook-secret&mode=ok"
                }
            },
            "nested": {
                "refresh_token": "beta"
            }
        });
        redact_console_diagnostics_value(&mut payload, None);
        assert_eq!(
            payload.get("authorization").and_then(serde_json::Value::as_str),
            Some("<redacted>")
        );
        assert_eq!(
            payload.pointer("/nested/refresh_token").and_then(serde_json::Value::as_str),
            Some("<redacted>")
        );
        assert_eq!(
            payload.get("endpoint").and_then(serde_json::Value::as_str),
            Some("https://example.test/callback?access_token=<redacted>&mode=ok")
        );
        assert_eq!(
            payload.pointer("/browserd/relay_token").and_then(serde_json::Value::as_str),
            Some("<redacted>")
        );
        assert_eq!(
            payload.pointer("/browserd/downloads_endpoint").and_then(serde_json::Value::as_str),
            Some("https://example.test/downloads?token=<redacted>&mode=ok")
        );
        assert_eq!(
            payload
                .pointer("/channels/discord:default/webhook_url")
                .and_then(serde_json::Value::as_str),
            Some("https://discord.test/api/webhooks/1?token=<redacted>&mode=ok")
        );
        let redacted_error = payload
            .get("error_message")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned();
        assert!(
            redacted_error.contains("state=ok")
                && redacted_error.contains("access_token=<redacted>")
                && !redacted_error.contains("abc123"),
            "error message should hide secret token values: {redacted_error}"
        );
        let redacted_detail = payload
            .get("error_detail")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned();
        assert!(
            redacted_detail.contains("refresh_token=<redacted>")
                && redacted_detail.contains("mode=ok")
                && !redacted_detail.contains("refresh-secret"),
            "error detail should hide fragment token values: {redacted_detail}"
        );
        let browser_error = payload
            .pointer("/browserd/last_error")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        assert!(
            browser_error.contains("<redacted>") && !browser_error.contains("browser-secret"),
            "browser diagnostics error should hide secret values: {browser_error}"
        );
        let connector_error = payload
            .pointer("/channels/discord:default/runtime/last_error")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        assert!(
            connector_error.contains("<redacted>") && !connector_error.contains("discord-secret"),
            "connector diagnostics error should hide secret values: {connector_error}"
        );
    }

    #[test]
    fn diagnostics_redaction_masks_flow_delivery_progress_details() {
        let mut payload = serde_json::json!({
            "recent": [{
                "delivery_progress": {
                    "items": [{
                        "detail": "provider failed: Authorization: Bearer sk-live-secret https://api.example.test/v1/run?access_token=tok123&ok=1"
                    }]
                }
            }]
        });

        redact_console_diagnostics_value(&mut payload, None);

        let detail = payload
            .pointer("/recent/0/delivery_progress/items/0/detail")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        assert!(
            detail.contains("Bearer <redacted>")
                && detail.contains("access_token=<redacted>")
                && detail.contains("https://api.example.test/v1/run"),
            "flow diagnostics detail should preserve useful context while redacting secrets: {detail}"
        );
        assert!(
            !detail.contains("sk-live-secret")
                && !detail.contains("tok123")
                && !detail.contains("access_token=tok123"),
            "flow diagnostics detail must not leak raw secret values: {detail}"
        );
    }

    #[test]
    fn console_secret_token_is_urlsafe_and_unpadded() {
        let token = mint_console_secret_token();
        assert_eq!(
            token.len(),
            43,
            "32 random bytes encoded as base64url without padding should be 43 chars"
        );
        assert!(!token.contains('='), "console secret token should never include base64 padding");
        assert!(
            token.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')),
            "console secret token should remain URL-safe base64 alphabet"
        );
    }

    #[test]
    fn console_secret_token_generation_has_no_duplicates_in_small_batch() {
        let mut seen = HashSet::new();
        for index in 0..512 {
            let token = mint_console_secret_token();
            assert!(
                seen.insert(token),
                "unexpected duplicate console secret token at sample index {index}"
            );
        }
    }

    #[test]
    fn console_relay_token_uses_same_secret_token_format() {
        let token = mint_console_relay_token();
        assert_eq!(token.len(), 43, "relay token should use 32-byte CSPRNG base64url encoding");
        assert!(!token.contains('='), "relay token should never include base64 padding");
    }

    #[test]
    fn constant_time_comparator_requires_exact_match() {
        assert!(
            constant_time_eq_bytes(b"same-value", b"same-value"),
            "comparator should accept equal byte sequences"
        );
        assert!(
            !constant_time_eq_bytes(b"same-value", b"same-valuf"),
            "comparator should reject different byte sequences"
        );
        assert!(
            !constant_time_eq_bytes(b"short", b"longer"),
            "comparator should reject inputs with different lengths"
        );
    }

    #[test]
    fn hashed_secret_lookup_matches_only_exact_hash() {
        let relay_token = mint_console_relay_token();
        let relay_hash = sha256_hex(relay_token.as_bytes());
        let mut tokens = HashMap::new();
        tokens.insert(
            relay_hash.clone(),
            ConsoleRelayToken {
                token_hash_sha256: relay_hash.clone(),
                principal: "admin:ops".to_owned(),
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                channel: Some("web".to_owned()),
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                extension_id: "ext-1".to_owned(),
                issued_at_unix_ms: 1_000,
                expires_at_unix_ms: 2_000,
            },
        );

        let matched = find_hashed_secret_map_key(&tokens, relay_hash.as_str());
        assert_eq!(matched.as_deref(), Some(relay_hash.as_str()));

        let non_matching = find_hashed_secret_map_key(&tokens, "not-a-valid-sha256-hash");
        assert!(non_matching.is_none(), "unexpected match for unrelated hash candidate");
    }

    #[test]
    fn relay_token_pruning_evicts_expired_entries() {
        let now = 1_000_i64;
        let mut tokens = HashMap::new();
        tokens.insert(
            "expired".to_owned(),
            ConsoleRelayToken {
                token_hash_sha256: "expired".to_owned(),
                principal: "admin:ops".to_owned(),
                device_id: "dev".to_owned(),
                channel: None,
                session_id: "session-1".to_owned(),
                extension_id: "ext".to_owned(),
                issued_at_unix_ms: 500,
                expires_at_unix_ms: now,
            },
        );
        tokens.insert(
            "active".to_owned(),
            ConsoleRelayToken {
                token_hash_sha256: "active".to_owned(),
                principal: "admin:ops".to_owned(),
                device_id: "dev".to_owned(),
                channel: None,
                session_id: "session-2".to_owned(),
                extension_id: "ext".to_owned(),
                issued_at_unix_ms: 900,
                expires_at_unix_ms: now + 1,
            },
        );

        prune_console_relay_tokens(&mut tokens, now);
        assert!(
            !tokens.contains_key("expired"),
            "expired relay token record should be removed during prune"
        );
        assert!(
            tokens.contains_key("active"),
            "non-expired relay token record should remain after prune"
        );
    }

    #[test]
    fn relay_token_ttl_clamp_enforces_policy_bounds() {
        assert_eq!(
            clamp_console_relay_token_ttl_ms(None),
            CONSOLE_RELAY_TOKEN_DEFAULT_TTL_MS,
            "default relay token TTL should apply when caller does not provide value"
        );
        assert_eq!(
            clamp_console_relay_token_ttl_ms(Some(1)),
            CONSOLE_RELAY_TOKEN_MIN_TTL_MS,
            "relay token TTL should clamp below minimum bound"
        );
        assert_eq!(
            clamp_console_relay_token_ttl_ms(Some(CONSOLE_RELAY_TOKEN_MAX_TTL_MS + 1)),
            CONSOLE_RELAY_TOKEN_MAX_TTL_MS,
            "relay token TTL should clamp above maximum bound"
        );
    }

    #[test]
    fn admin_auth_config_validation_fails_when_token_missing() {
        let error = validate_admin_auth_config(&GatewayAuthConfig {
            require_auth: true,
            admin_token: None,
            connector_token: None,
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
            connector_token: None,
            bound_principal: None,
        });
        assert!(result.is_ok(), "disabled auth should allow missing token");
    }

    #[test]
    fn admin_rate_limit_rejects_after_window_budget_is_exhausted() {
        let buckets = Mutex::new(HashMap::new());
        let ip = IpAddr::from_str("127.0.0.1").expect("IP literal should parse");
        let now = Instant::now();
        for attempt in 0..ADMIN_RATE_LIMIT_LOOPBACK_MAX_REQUESTS_PER_WINDOW {
            let allowed = consume_admin_rate_limit_with_now(&buckets, ip, now);
            assert!(allowed, "attempt {attempt} should remain within the request budget");
        }
        assert!(
            !consume_admin_rate_limit_with_now(&buckets, ip, now),
            "request after budget exhaustion should be rejected"
        );
    }

    #[test]
    fn admin_auth_failure_rate_limit_rejects_after_failure_budget_is_exhausted() {
        let buckets = Mutex::new(HashMap::new());
        let ip = IpAddr::from_str("127.0.0.1").expect("IP literal should parse");
        let now = Instant::now();
        for attempt in 0..ADMIN_AUTH_FAILURE_RATE_LIMIT_MAX_REQUESTS_PER_WINDOW {
            let allowed = consume_admin_auth_failure_rate_limit_with_now(&buckets, ip, now);
            assert!(allowed, "failed auth attempt {attempt} should remain within the budget");
        }
        assert!(
            !consume_admin_auth_failure_rate_limit_with_now(&buckets, ip, now),
            "failed auth request after budget exhaustion should be rejected"
        );
    }

    fn admin_rate_limit_window_budgets_for_test() -> (u32, u32) {
        (
            ADMIN_RATE_LIMIT_LOOPBACK_MAX_REQUESTS_PER_WINDOW,
            ADMIN_RATE_LIMIT_MAX_REQUESTS_PER_WINDOW,
        )
    }

    #[test]
    fn admin_rate_limit_keeps_remote_budget_tighter_than_loopback() {
        let buckets = Mutex::new(HashMap::new());
        let ip = IpAddr::from_str("203.0.113.10").expect("IP literal should parse");
        let now = Instant::now();
        let (loopback_budget, remote_budget) = admin_rate_limit_window_budgets_for_test();
        assert!(
            loopback_budget > remote_budget,
            "loopback budget should preserve local desktop/CLI bursts while remote exposure remains tighter"
        );
        assert!(
            loopback_budget >= 1_000,
            "loopback budget should not throttle normal local desktop plus CLI automation bursts"
        );
        for attempt in 0..remote_budget {
            let allowed = consume_admin_rate_limit_with_now(&buckets, ip, now);
            assert!(allowed, "remote attempt {attempt} should remain within the request budget");
        }
        assert!(
            !consume_admin_rate_limit_with_now(&buckets, ip, now),
            "remote request after budget exhaustion should be rejected"
        );
    }

    #[test]
    fn admin_rate_limit_resets_budget_after_window_elapses() {
        let buckets = Mutex::new(HashMap::new());
        let ip = IpAddr::from_str("127.0.0.1").expect("IP literal should parse");
        let now = Instant::now();
        for _ in 0..ADMIN_RATE_LIMIT_LOOPBACK_MAX_REQUESTS_PER_WINDOW {
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
    fn canvas_rate_limit_rejects_after_window_budget_is_exhausted() {
        let buckets = Mutex::new(HashMap::new());
        let ip = IpAddr::from_str("127.0.0.1").expect("IP literal should parse");
        let now = Instant::now();
        for attempt in 0..CANVAS_RATE_LIMIT_MAX_REQUESTS_PER_WINDOW {
            let allowed = consume_canvas_rate_limit_with_now(&buckets, ip, now);
            assert!(allowed, "attempt {attempt} should remain within the request budget");
        }
        assert!(
            !consume_canvas_rate_limit_with_now(&buckets, ip, now),
            "request after budget exhaustion should be rejected"
        );
    }

    #[test]
    fn canvas_rate_limit_resets_budget_after_window_elapses() {
        let buckets = Mutex::new(HashMap::new());
        let ip = IpAddr::from_str("127.0.0.1").expect("IP literal should parse");
        let now = Instant::now();
        for _ in 0..CANVAS_RATE_LIMIT_MAX_REQUESTS_PER_WINDOW {
            let _ = consume_canvas_rate_limit_with_now(&buckets, ip, now);
        }
        assert!(
            !consume_canvas_rate_limit_with_now(&buckets, ip, now),
            "budget should be exhausted within the same window"
        );
        let advanced = now + Duration::from_millis(1_200);
        assert!(
            consume_canvas_rate_limit_with_now(&buckets, ip, advanced),
            "request should be allowed after the fixed window expires"
        );
    }

    #[test]
    fn canvas_rate_limit_bucket_count_is_bounded() {
        let buckets = Mutex::new(HashMap::new());
        let now = Instant::now();
        for offset in 0..CANVAS_RATE_LIMIT_MAX_IP_BUCKETS {
            let ip = IpAddr::from([100, 64, (offset / 256) as u8, (offset % 256) as u8]);
            let allowed = consume_canvas_rate_limit_with_now(&buckets, ip, now);
            assert!(allowed, "filling bucket {offset} should succeed");
        }
        let overflow_ip = IpAddr::from([100, 127, 0, 1]);
        assert!(
            consume_canvas_rate_limit_with_now(&buckets, overflow_ip, now),
            "overflow principal should still be accepted after oldest-bucket eviction"
        );
        let bucket_count = buckets.lock().expect("bucket mutex should be available").len();
        assert_eq!(
            bucket_count, CANVAS_RATE_LIMIT_MAX_IP_BUCKETS,
            "bucket count must remain bounded to avoid unbounded memory growth"
        );
    }

    #[test]
    fn canvas_http_token_query_rejects_empty_and_oversized_values() {
        let empty = validate_canvas_http_token_query("")
            .expect_err("empty token query should fail closed at HTTP boundary");
        assert_eq!(empty.status(), StatusCode::BAD_REQUEST);

        let oversized = "a".repeat(CANVAS_HTTP_MAX_TOKEN_BYTES.saturating_add(1));
        let oversized_error = validate_canvas_http_token_query(oversized.as_str())
            .expect_err("oversized token query should fail closed at HTTP boundary");
        assert_eq!(oversized_error.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn canvas_http_canvas_id_validation_enforces_canonical_ulid_shape() {
        validate_canvas_http_canvas_id("01ARZ3NDEKTSV4RRFFQ69G5FAV")
            .expect("canonical ULID canvas id should be accepted");
        let invalid = validate_canvas_http_canvas_id("not-a-canonical-id")
            .expect_err("invalid canvas id should be rejected at HTTP boundary");
        assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
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
    fn process_runner_backend_policy_allows_tier_b_preflight_mode() {
        let result = validate_process_runner_backend_policy(
            true,
            SandboxProcessRunnerTier::B,
            EgressEnforcementMode::Preflight,
            false,
        );
        assert!(result.is_ok(), "tier-b should remain allowed in preflight mode");
    }

    #[test]
    fn process_runner_backend_policy_rejects_tier_b_strict_mode() {
        let error = validate_process_runner_backend_policy(
            true,
            SandboxProcessRunnerTier::B,
            EgressEnforcementMode::Strict,
            false,
        )
        .expect_err("tier-b strict mode should fail closed");
        assert!(
            error
                .to_string()
                .contains("tier='b' does not support egress_enforcement_mode='strict'"),
            "error should explain strict-mode requirement to use preflight/none or tier-c"
        );
    }

    #[test]
    #[cfg(not(windows))]
    fn process_runner_backend_policy_rejects_strict_mode_host_allowlists() {
        let error = validate_process_runner_backend_policy(
            true,
            SandboxProcessRunnerTier::C,
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
