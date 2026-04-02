#![recursion_limit = "256"]

mod agents;
pub mod app;
pub mod application;
mod background_queue;
mod channel_router;
mod channels;
mod config;
mod cron;
pub mod domain;
mod gateway;
mod hooks;
pub mod infra;
mod journal;
mod media;
mod media_derived;
mod model_provider;
mod node_rpc;
mod node_runtime;
mod observability;
mod openai_auth;
mod openai_surface;
mod orchestrator;
mod plugins;
mod quic_runtime;
mod sandbox_runner;
pub mod support;
mod tool_protocol;
pub mod transport;
mod wasm_plugin_runner;
mod webhooks;

use std::{
    collections::HashMap,
    convert::Infallible,
    fs,
    net::{IpAddr, SocketAddr},
    path::{Path as FsPath, PathBuf},
    process::Stdio,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use app::{
    bootstrap::load_runtime_bootstrap,
    logging::init_tracing,
    runtime::{build_app_state, loopback_grpc_url, AppStateBuildContext},
    shutdown::shutdown_signal,
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
    MemoryRuntimeConfig,
};
use journal::{
    ApprovalDecision, ApprovalDecisionScope, ApprovalSubjectType, CronJobUpdatePatch,
    HashMemoryEmbeddingProvider, JournalAppendRequest, JournalConfig, JournalStore,
    MemoryEmbeddingProvider, MemoryPurgeRequest, OrchestratorCancelRequest,
    OrchestratorRunStatusSnapshot, SkillExecutionStatus, SkillStatusRecord,
    SkillStatusUpsertRequest,
};
use model_provider::{
    build_embeddings_provider, build_model_provider, EmbeddingsProvider, EmbeddingsRequest,
    ModelProviderAuthProviderKind, ModelProviderConfig, ModelProviderCredentialSource,
    ModelProviderKind,
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
    connect_openai_api_key, load_openai_oauth_callback_state, reconnect_openai_oauth_attempt,
    refresh_openai_oauth_profile, revoke_openai_auth_profile, select_default_openai_auth_profile,
    start_openai_oauth_attempt_from_request,
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
    validate_canonical_id,
};
use palyra_common::{default_identity_store_root, default_state_root};
use palyra_connector_discord::{
    discord_min_invite_permissions, discord_required_permission_labels,
    resolve_discord_intents_from_flags, DiscordPrivilegedIntentStatus,
    DiscordPrivilegedIntentsSummary, DISCORD_PERMISSION_ATTACH_FILES,
    DISCORD_PERMISSION_EMBED_LINKS, DISCORD_PERMISSION_READ_MESSAGE_HISTORY,
    DISCORD_PERMISSION_SEND_MESSAGES, DISCORD_PERMISSION_SEND_MESSAGES_IN_THREADS,
    DISCORD_PERMISSION_VIEW_CHANNEL,
};
#[cfg(test)]
use palyra_connector_discord::{
    discord_required_permissions, DISCORD_APP_FLAG_GATEWAY_GUILD_MEMBERS,
    DISCORD_APP_FLAG_GATEWAY_MESSAGE_CONTENT, DISCORD_APP_FLAG_GATEWAY_PRESENCE,
};
use palyra_control_plane as control_plane;
use palyra_identity::{FilesystemSecretStore, IdentityManager, SecretStore};
use palyra_policy::{
    evaluate_with_config, evaluate_with_context, PolicyDecision, PolicyEvaluationConfig,
    PolicyRequest, PolicyRequestContext,
};
use palyra_skills::{
    audit_skill_artifact_security, inspect_skill_artifact, verify_skill_artifact,
    SkillSecurityAuditPolicy, SkillTrustStore,
};
use palyra_vault::{Vault, VaultConfig as VaultConfigOptions, VaultRef, VaultScope};
use reqwest::{Client as ReqwestClient, Url};
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

use crate::gateway::proto::palyra::{
    browser::v1 as browser_v1, common::v1 as common_v1, cron::v1 as cron_v1,
    gateway::v1 as gateway_v1,
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
#[cfg(test)]
pub(crate) use crate::transport::http::handlers::console::channels::connectors::discord::{
    build_discord_channel_permission_warnings, build_discord_inbound_monitor_warnings,
    build_discord_onboarding_plan, build_discord_onboarding_security_defaults,
    discord_inbound_monitor_is_alive, finalize_discord_onboarding_plan, normalize_discord_token,
    normalize_optional_discord_channel_id, summarize_discord_inbound_monitor,
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
    consume_admin_rate_limit_with_now, consume_canvas_rate_limit_with_now,
};

const DANGEROUS_REMOTE_BIND_ACK_ENV: &str = "PALYRA_GATEWAY_DANGEROUS_REMOTE_BIND_ACK";
const SYSTEM_DAEMON_PRINCIPAL: &str = "system:daemon";
const SYSTEM_VAULT_CHANNEL: &str = "system:vault";
const SYSTEM_DAEMON_DEVICE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
const GRPC_MAX_DECODING_MESSAGE_SIZE_BYTES: usize = 4 * 1024 * 1024;
const GRPC_MAX_ENCODING_MESSAGE_SIZE_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const ADMIN_RATE_LIMIT_WINDOW_MS: u64 = 1_000;
pub(crate) const ADMIN_RATE_LIMIT_MAX_REQUESTS_PER_WINDOW: u32 = 30;
pub(crate) const ADMIN_RATE_LIMIT_LOOPBACK_MAX_REQUESTS_PER_WINDOW: u32 = 120;
pub(crate) const ADMIN_RATE_LIMIT_MAX_IP_BUCKETS: usize = 4_096;
pub(crate) const CANVAS_RATE_LIMIT_WINDOW_MS: u64 = 1_000;
pub(crate) const CANVAS_RATE_LIMIT_MAX_REQUESTS_PER_WINDOW: u32 = 90;
pub(crate) const CANVAS_RATE_LIMIT_MAX_IP_BUCKETS: usize = 4_096;
pub(crate) const CANVAS_HTTP_MAX_TOKEN_BYTES: usize = 8 * 1024;
pub(crate) const CANVAS_HTTP_MAX_CANVAS_ID_BYTES: usize = 64;
pub(crate) const HTTP_MAX_REQUEST_BODY_BYTES: usize = 64 * 1024;
const DISCORD_API_BASE: &str = "https://discord.com/api/v10";
const DISCORD_ONBOARDING_HTTP_TIMEOUT_MS: u64 = 5_000;
const DISCORD_ONBOARDING_CONFIG_BACKUPS: usize = 2;
const DISCORD_ONBOARDING_INBOUND_RECENT_WINDOW_MS: i64 = 15 * 60 * 1_000;
const DISCORD_ONBOARDING_MONITOR_WAIT_TIMEOUT_MS: u64 = 5_000;
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
struct ConsoleMemoryIndexRequest {
    #[serde(default)]
    batch_size: Option<usize>,
    #[serde(default)]
    until_complete: Option<bool>,
    #[serde(default)]
    run_maintenance: Option<bool>,
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
struct ChannelLogsQuery {
    limit: Option<usize>,
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

#[derive(Debug, Default, Deserialize)]
struct DiscordAccountLifecycleRequest {
    #[serde(default)]
    keep_credential: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct DiscordAccountLifecycleActionRequest {
    account_id: String,
    #[serde(default)]
    keep_credential: Option<bool>,
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
struct DiscordOnboardingRequest {
    #[serde(default)]
    account_id: Option<String>,
    token: String,
    #[serde(default)]
    mode: Option<String>,
    #[serde(default)]
    inbound_scope: Option<String>,
    #[serde(default)]
    allow_from: Option<Vec<String>>,
    #[serde(default)]
    deny_from: Option<Vec<String>>,
    #[serde(default)]
    require_mention: Option<bool>,
    #[serde(default)]
    mention_patterns: Option<Vec<String>>,
    #[serde(default)]
    concurrency_limit: Option<u64>,
    #[serde(default)]
    direct_message_policy: Option<String>,
    #[serde(default)]
    broadcast_strategy: Option<String>,
    #[serde(default)]
    confirm_open_guild_channels: Option<bool>,
    #[serde(default)]
    verify_channel_id: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum DiscordOnboardingMode {
    Local,
    RemoteVps,
}

impl DiscordOnboardingMode {
    fn parse(raw: Option<&str>) -> Option<Self> {
        let normalized = raw?.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "local" => Some(Self::Local),
            "remote_vps" | "remote-vps" | "remote" | "vps" => Some(Self::RemoteVps),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum DiscordOnboardingScope {
    DmOnly,
    AllowlistedGuildChannels,
    OpenGuildChannels,
}

impl DiscordOnboardingScope {
    fn parse(raw: Option<&str>) -> Option<Self> {
        let normalized = raw?.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "dm_only" | "dm-only" | "dm" => Some(Self::DmOnly),
            "allowlisted_guild_channels" | "allowlisted-guild-channels" | "allowlisted" => {
                Some(Self::AllowlistedGuildChannels)
            }
            "open_guild_channels" | "open-guild-channels" | "open" => Some(Self::OpenGuildChannels),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct DiscordApplicationSummary {
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    flags: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    intents: Option<DiscordPrivilegedIntentsSummary>,
}

#[derive(Debug, Clone, Serialize)]
struct DiscordBotIdentitySummary {
    id: String,
    username: String,
}

#[derive(Debug, Clone, Serialize)]
struct DiscordRoutingPreview {
    connector_id: String,
    mode: DiscordOnboardingMode,
    inbound_scope: DiscordOnboardingScope,
    require_mention: bool,
    mention_patterns: Vec<String>,
    allow_from: Vec<String>,
    deny_from: Vec<String>,
    allow_direct_messages: bool,
    direct_message_policy: String,
    broadcast_strategy: String,
    concurrency_limit: u64,
}

#[derive(Debug, Clone, Serialize)]
struct DiscordInboundMonitorSummary {
    connector_registered: bool,
    gateway_connected: bool,
    recent_inbound: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_inbound_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_connect_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_disconnect_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_event_type: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum DiscordChannelPermissionCheckStatus {
    Ok,
    Forbidden,
    NotFound,
    Unavailable,
    ParseError,
}

#[derive(Debug, Clone, Serialize)]
struct DiscordChannelPermissionCheck {
    channel_id: String,
    status: DiscordChannelPermissionCheckStatus,
    can_view_channel: bool,
    can_send_messages: bool,
    can_read_message_history: bool,
    can_embed_links: bool,
    can_attach_files: bool,
    can_send_messages_in_threads: bool,
}

#[derive(Debug, Clone, Serialize)]
struct DiscordOnboardingPreflightResponse {
    connector_id: String,
    account_id: String,
    mode: DiscordOnboardingMode,
    inbound_scope: DiscordOnboardingScope,
    bot: DiscordBotIdentitySummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    application: Option<DiscordApplicationSummary>,
    invite_url_template: String,
    required_permissions: Vec<String>,
    egress_allowlist: Vec<String>,
    security_defaults: Vec<String>,
    routing_preview: DiscordRoutingPreview,
    #[serde(skip_serializing_if = "Option::is_none")]
    channel_permission_check: Option<DiscordChannelPermissionCheck>,
    inbound_monitor: DiscordInboundMonitorSummary,
    inbound_alive: bool,
    warnings: Vec<String>,
    policy_warnings: Vec<String>,
}

#[derive(Debug, Clone)]
struct DiscordOnboardingPlan {
    connector_id: String,
    account_id: String,
    mode: DiscordOnboardingMode,
    inbound_scope: DiscordOnboardingScope,
    require_mention: bool,
    mention_patterns: Vec<String>,
    allow_from: Vec<String>,
    deny_from: Vec<String>,
    allow_direct_messages: bool,
    direct_message_policy: channel_router::DirectMessagePolicy,
    broadcast_strategy: channel_router::BroadcastStrategy,
    concurrency_limit: u64,
    confirm_open_guild_channels: bool,
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

#[derive(Debug, Deserialize)]
struct ConsoleChatRenameSessionRequest {
    session_label: String,
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
struct ConsoleChatRunEventsQuery {
    after_seq: Option<i64>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct ConsoleChatQueueRequest {
    text: String,
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
struct ConsoleChatCompactionRequest {
    #[serde(default)]
    trigger_reason: Option<String>,
    #[serde(default)]
    trigger_policy: Option<String>,
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
}

#[derive(Debug, Deserialize)]
struct ConsoleChatAttachmentUploadRequest {
    filename: String,
    content_type: String,
    bytes_base64: String,
}

#[derive(Debug, Clone, Deserialize)]
struct ConsoleChatAttachmentReference {
    artifact_id: String,
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
pub(crate) struct CanvasTokenQuery {
    token: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct CanvasRuntimeQuery {
    canvas_id: String,
    token: String,
}

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

#[derive(Clone)]
struct IdentityRuntime {
    store_root: PathBuf,
    revoked_certificate_count: usize,
    gateway_ca_certificate_pem: String,
    node_server_certificate: palyra_identity::IssuedCertificate,
    manager: Arc<Mutex<IdentityManager>>,
}

#[derive(Debug, Clone)]
struct SecretAccessAuditRecord {
    scope: String,
    key: String,
    action: String,
    value_bytes: usize,
}

struct ModelProviderMemoryEmbeddingAdapter {
    provider: Arc<dyn EmbeddingsProvider>,
    model_name: String,
    dimensions: usize,
}

impl ModelProviderMemoryEmbeddingAdapter {
    fn new(provider: Arc<dyn EmbeddingsProvider>, model_name: String, dimensions: usize) -> Self {
        Self { provider, model_name, dimensions: dimensions.max(1) }
    }

    fn zero_vector(&self) -> Vec<f32> {
        vec![0.0_f32; self.dimensions]
    }
}

impl MemoryEmbeddingProvider for ModelProviderMemoryEmbeddingAdapter {
    fn model_name(&self) -> &str {
        self.model_name.as_str()
    }

    fn dimensions(&self) -> usize {
        self.dimensions
    }

    fn embed_text(&self, text: &str) -> Vec<f32> {
        let request = EmbeddingsRequest { inputs: vec![text.to_owned()] };
        let result = match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                tokio::task::block_in_place(|| handle.block_on(self.provider.embed(request)))
            }
            Err(_) => {
                warn!(
                    "tokio runtime unavailable for model-provider embeddings adapter; using zero vector fallback"
                );
                return self.zero_vector();
            }
        };

        match result {
            Ok(response) => {
                let Some(vector) = response.vectors.into_iter().next() else {
                    warn!(
                        "model-provider embeddings response did not include vector payload; using zero vector fallback"
                    );
                    return self.zero_vector();
                };
                normalize_memory_embedding_vector(vector, self.dimensions)
            }
            Err(error) => {
                warn!(
                    error = %error,
                    "model-provider embeddings request failed; using zero vector fallback"
                );
                self.zero_vector()
            }
        }
    }
}

fn normalize_memory_embedding_vector(mut vector: Vec<f32>, expected_dims: usize) -> Vec<f32> {
    if expected_dims == 0 {
        return Vec::new();
    }
    if vector.len() < expected_dims {
        vector.resize(expected_dims, 0.0);
    } else if vector.len() > expected_dims {
        vector.truncate(expected_dims);
    }
    vector
}

fn parse_offline_env_flag(raw: &str) -> Result<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" | "" => Ok(false),
        other => Err(anyhow::anyhow!(
            "PALYRA_OFFLINE must be a boolean-like value (accepted: 1/0, true/false, yes/no, on/off), got '{other}'"
        )),
    }
}

fn offline_mode_enabled() -> Result<bool> {
    match std::env::var("PALYRA_OFFLINE") {
        Ok(raw) => parse_offline_env_flag(raw.as_str()),
        Err(std::env::VarError::NotPresent) => Ok(false),
        Err(std::env::VarError::NotUnicode(_)) => {
            Err(anyhow::anyhow!("PALYRA_OFFLINE contains non-unicode data"))
        }
    }
}

fn build_memory_embedding_provider(
    config: &ModelProviderConfig,
    offline_mode: bool,
) -> Result<Arc<dyn MemoryEmbeddingProvider>> {
    if config.kind != ModelProviderKind::OpenAiCompatible {
        return Ok(Arc::new(HashMemoryEmbeddingProvider::default()));
    }

    match (&config.openai_embeddings_model, config.openai_embeddings_dims) {
        (Some(model_name), Some(dimensions)) => {
            if offline_mode {
                warn!(
                    model = %model_name,
                    dimensions,
                    "PALYRA_OFFLINE is enabled; using hash embeddings fallback for memory vectors"
                );
                return Ok(Arc::new(HashMemoryEmbeddingProvider::with_dimensions(
                    dimensions as usize,
                )));
            }
            let embeddings_provider = build_embeddings_provider(config)
                .context("failed to initialize model-provider embeddings runtime")?;
            Ok(Arc::new(ModelProviderMemoryEmbeddingAdapter::new(
                embeddings_provider,
                model_name.clone(),
                dimensions as usize,
            )))
        }
        (Some(_), None) => Err(anyhow::anyhow!(
            "openai embeddings model is configured but model_provider.openai_embeddings_dims is missing"
        )),
        (None, Some(dimensions)) => {
            warn!(
                dimensions,
                "openai embeddings dimensions are configured without model; hash embeddings fallback remains active"
            );
            Ok(Arc::new(HashMemoryEmbeddingProvider::default()))
        }
        (None, None) => Ok(Arc::new(HashMemoryEmbeddingProvider::default())),
    }
}

pub async fn run() -> Result<()> {
    init_tracing();
    let bootstrap = load_runtime_bootstrap()?;
    let mut loaded = bootstrap.loaded;
    let journal_migrate_only = bootstrap.journal_migrate_only;
    let node_rpc_mtls_required = bootstrap.node_rpc_mtls_required;

    let identity_runtime = load_identity_runtime(loaded.gateway.identity_store_dir.clone())
        .context("failed to initialize gateway identity runtime")?;
    let auth = GatewayAuthConfig {
        require_auth: loaded.admin.require_auth,
        admin_token: loaded.admin.auth_token.clone(),
        connector_token: loaded.admin.connector_token.clone(),
        bound_principal: loaded.admin.bound_principal.clone(),
    };
    validate_admin_auth_config(&auth)?;
    let offline_mode = offline_mode_enabled()?;
    let memory_embedding_provider =
        build_memory_embedding_provider(&loaded.model_provider, offline_mode)?;
    let journal_store = JournalStore::open_with_memory_embedding_provider(
        JournalConfig {
            db_path: loaded.storage.journal_db_path.clone(),
            hash_chain_enabled: loaded.storage.journal_hash_chain_enabled,
            max_payload_bytes: loaded.storage.max_journal_payload_bytes,
            max_events: loaded.storage.max_journal_events,
        },
        memory_embedding_provider,
    )
    .context("failed to initialize event journal storage")?;
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
    if let Some(access_audit) = resolve_model_provider_secret(
        &mut loaded.model_provider,
        auth_registry.as_ref(),
        vault.as_ref(),
    )? {
        record_secret_access_journal_event(&journal_store, &access_audit)
            .context("failed to audit model provider secret access")?;
    }
    let model_provider = build_model_provider(&loaded.model_provider)
        .context("failed to initialize model provider runtime")?;
    let agent_registry = agents::AgentRegistry::open(identity_runtime.store_root.as_path())
        .context("failed to initialize agent registry state")?;
    let runtime_state_root = resolve_runtime_state_root(identity_runtime.store_root.as_path())
        .context("failed to resolve webhook registry state root")?;
    let webhook_registry = Arc::new(
        webhooks::WebhookRegistry::open(runtime_state_root.as_path())
            .context("failed to initialize webhook registry state")?,
    );
    let auth_runtime = Arc::new(gateway::AuthRuntimeState::new(
        Arc::clone(&auth_registry),
        Arc::new(HttpOAuthRefreshAdapter::default()) as Arc<dyn OAuthRefreshAdapter>,
    ));
    let node_runtime = Arc::new(
        node_runtime::NodeRuntimeState::load(runtime_state_root.as_path())
            .context("failed to initialize node runtime state")?,
    );
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
            media: loaded.media.clone(),
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
        retention_max_entries: loaded.memory.retention.max_entries,
        retention_max_bytes: loaded.memory.retention.max_bytes,
        retention_ttl_days: loaded.memory.retention.ttl_days,
        retention_vacuum_schedule: loaded.memory.retention.vacuum_schedule.clone(),
    });

    if journal_migrate_only {
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
    let channels = Arc::new(
        channels::ChannelPlatform::initialize(
            grpc_url.clone(),
            auth.clone(),
            connectors_db_path,
            loaded.media.clone(),
        )
        .context("failed to initialize channel connector platform")?,
    );
    let _cron_scheduler_task = spawn_scheduler_loop(
        runtime.clone(),
        auth.clone(),
        grpc_url.clone(),
        Arc::clone(&scheduler_wake),
        loaded.memory.retention.clone(),
    );

    let state = build_app_state(
        &loaded,
        dangerous_remote_bind_ack_env,
        AppStateBuildContext {
            runtime: runtime.clone(),
            node_runtime: Arc::clone(&node_runtime),
            identity_manager: Arc::clone(&identity_runtime.manager),
            channels: Arc::clone(&channels),
            webhooks: Arc::clone(&webhook_registry),
            vault: Arc::clone(&vault),
            auth_runtime: Arc::clone(&auth_runtime),
            auth: auth.clone(),
            grpc_url: grpc_url.clone(),
            scheduler_wake: Arc::clone(&scheduler_wake),
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
    let _channel_worker_task = Arc::clone(&channels).spawn_worker();
    let _hook_runtime_task =
        hooks::spawn_hook_runtime(runtime.clone(), hook_runtime_policy, hook_execution_timeout);
    let _background_queue_task = background_queue::spawn_background_queue_loop(
        runtime.clone(),
        auth.clone(),
        grpc_url.clone(),
    );

    let admin_server = async move {
        axum::serve(admin_listener, app.into_make_service_with_connect_info::<SocketAddr>())
            .with_graceful_shutdown(shutdown_signal())
            .await
            .context("palyrad admin server failed")
    };
    let grpc_transport = transport::grpc::server::serve(
        &loaded,
        &identity_runtime,
        runtime.clone(),
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

#[allow(clippy::result_large_err)]
fn resolve_runtime_state_root(identity_store_root: &FsPath) -> Result<PathBuf> {
    resolve_runtime_state_root_with_override(
        std::env::var_os("PALYRA_STATE_ROOT").map(PathBuf::from),
        identity_store_root,
    )
}

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

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
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

fn channel_platform_error_response(error: channels::ChannelPlatformError) -> Response {
    let status = match &error {
        channels::ChannelPlatformError::InvalidInput(message) => {
            tonic::Status::invalid_argument(message.clone())
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

fn build_error_response(
    status: StatusCode,
    message: String,
    code: &str,
    category: control_plane::ErrorCategory,
    retryable: bool,
    validation_errors: Vec<control_plane::ValidationIssue>,
    redacted: bool,
) -> Response {
    (
        status,
        Json(control_plane::ErrorEnvelope {
            error: message,
            code: code.to_owned(),
            category,
            retryable,
            redacted,
            validation_errors,
        }),
    )
        .into_response()
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

fn resolve_model_provider_secret(
    model_provider: &mut ModelProviderConfig,
    auth_registry: &AuthProfileRegistry,
    vault: &Vault,
) -> Result<Option<SecretAccessAuditRecord>> {
    if model_provider.kind != ModelProviderKind::OpenAiCompatible {
        return Ok(None);
    }
    if model_provider.openai_api_key.is_some() {
        model_provider.credential_source = Some(ModelProviderCredentialSource::InlineConfig);
        return Ok(None);
    }
    if model_provider.auth_profile_id.is_some() {
        return resolve_model_provider_secret_from_auth_profile(
            model_provider,
            auth_registry,
            vault,
        );
    }
    let Some(vault_ref_raw) = model_provider.openai_api_key_vault_ref.clone() else {
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
    model_provider.openai_api_key = Some(decoded);
    model_provider.credential_source = Some(ModelProviderCredentialSource::VaultRef);
    Ok(Some(SecretAccessAuditRecord {
        scope: vault_ref.scope.to_string(),
        key: vault_ref.key,
        action: "model_provider.openai_api_key.resolve".to_owned(),
        value_bytes: value.len(),
    }))
}

fn resolve_model_provider_secret_from_auth_profile(
    model_provider: &mut ModelProviderConfig,
    auth_registry: &AuthProfileRegistry,
    vault: &Vault,
) -> Result<Option<SecretAccessAuditRecord>> {
    let Some(auth_profile_id) = model_provider.auth_profile_id.clone() else {
        return Ok(None);
    };
    let profile = auth_registry
        .get_profile(auth_profile_id.as_str())
        .with_context(|| {
            format!(
                "failed to resolve auth profile '{}' for model provider runtime",
                auth_profile_id
            )
        })?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "model provider auth profile '{}' was not found in auth registry",
                auth_profile_id
            )
        })?;

    let expected_provider =
        model_provider.auth_profile_provider_kind.unwrap_or(ModelProviderAuthProviderKind::Openai);
    if matches!(expected_provider, ModelProviderAuthProviderKind::Openai)
        && !matches!(profile.provider.kind, AuthProviderKind::Openai)
    {
        anyhow::bail!(
            "model provider auth profile '{}' provider mismatch: expected openai-compatible profile, got '{}'",
            profile.profile_id,
            profile.provider.label()
        );
    }

    let (vault_ref_raw, action, credential_source) = match &profile.credential {
        AuthCredential::ApiKey { api_key_vault_ref } => (
            api_key_vault_ref.clone(),
            "model_provider.auth_profile.api_key.resolve",
            ModelProviderCredentialSource::AuthProfileApiKey,
        ),
        AuthCredential::Oauth { access_token_vault_ref, .. } => (
            access_token_vault_ref.clone(),
            "model_provider.auth_profile.oauth_access_token.resolve",
            ModelProviderCredentialSource::AuthProfileOauthAccessToken,
        ),
    };
    let vault_ref = VaultRef::parse(vault_ref_raw.as_str()).with_context(|| {
        format!(
            "invalid vault ref '{}' in model provider auth profile '{}'",
            vault_ref_raw, profile.profile_id
        )
    })?;
    let value = vault.get_secret(&vault_ref.scope, vault_ref.key.as_str()).with_context(|| {
        format!(
            "failed to load model provider credential from auth profile '{}' vault ref '{}'",
            profile.profile_id, vault_ref_raw
        )
    })?;
    if value.is_empty() {
        anyhow::bail!(
            "auth profile '{}' vault ref '{}' resolved to an empty secret value",
            profile.profile_id,
            vault_ref_raw
        );
    }
    let decoded = String::from_utf8(value.clone()).with_context(|| {
        format!(
            "model provider credential from auth profile '{}' must be valid UTF-8 text",
            profile.profile_id
        )
    })?;
    if decoded.trim().is_empty() {
        anyhow::bail!(
            "model provider credential from auth profile '{}' vault ref '{}' cannot be whitespace only",
            profile.profile_id,
            vault_ref_raw
        );
    }

    model_provider.openai_api_key = Some(decoded);
    model_provider.auth_profile_id = Some(profile.profile_id);
    model_provider.credential_source = Some(credential_source);
    Ok(Some(SecretAccessAuditRecord {
        scope: vault_ref.scope.to_string(),
        key: vault_ref.key,
        action: action.to_owned(),
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

pub(crate) fn unix_ms_now() -> Result<i64> {
    let elapsed =
        SystemTime::now().duration_since(UNIX_EPOCH).context("system clock before UNIX epoch")?;
    Ok(elapsed.as_millis() as i64)
}

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

#[derive(Debug, Clone, Copy)]
struct RemoteBindEndpoints {
    admin_address: SocketAddr,
    grpc_address: SocketAddr,
    quic_address: Option<SocketAddr>,
}

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

fn admin_rate_limiting_enabled() -> bool {
    ADMIN_RATE_LIMIT_WINDOW_MS > 0 && ADMIN_RATE_LIMIT_MAX_REQUESTS_PER_WINDOW > 0
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
        collections::{HashMap, HashSet},
        fs,
        net::IpAddr,
        path::PathBuf,
        str::FromStr,
        sync::Mutex,
        time::{Duration, Instant},
    };

    use axum::http::StatusCode;
    use palyra_auth::{
        AuthCredential, AuthProfileRegistry, AuthProfileScope, AuthProfileSetRequest, AuthProvider,
        AuthProviderKind,
    };
    use palyra_vault::{BackendPreference, Vault, VaultConfig as VaultConfigOptions, VaultRef};
    use serde_json::json;
    use tempfile::TempDir;

    use super::{
        build_discord_inbound_monitor_warnings, build_discord_onboarding_plan,
        build_discord_onboarding_security_defaults, build_memory_embedding_provider,
        clamp_console_relay_token_ttl_ms, connector_db_path_from_journal_path,
        constant_time_eq_bytes, consume_admin_rate_limit_with_now,
        consume_canvas_rate_limit_with_now, enforce_remote_bind_guard,
        finalize_discord_onboarding_plan, find_hashed_secret_map_key, loopback_grpc_url,
        mint_console_relay_token, mint_console_secret_token, normalize_discord_token,
        parse_offline_env_flag, prune_console_relay_tokens, redact_console_diagnostics_value,
        resolve_discord_intents_from_flags, resolve_model_provider_secret,
        resolve_runtime_state_root_with_override, runtime_status_response,
        sanitize_http_error_message, sha256_hex, summarize_discord_inbound_monitor,
        validate_admin_auth_config, validate_canvas_http_canvas_id,
        validate_canvas_http_token_query, validate_process_runner_backend_policy,
        ConsoleRelayToken, DiscordBotIdentitySummary, DiscordOnboardingRequest,
        DiscordOnboardingScope, DiscordPrivilegedIntentStatus, RemoteBindEndpoints,
        RemoteBindGuardConfig, ADMIN_RATE_LIMIT_LOOPBACK_MAX_REQUESTS_PER_WINDOW,
        ADMIN_RATE_LIMIT_MAX_IP_BUCKETS, ADMIN_RATE_LIMIT_MAX_REQUESTS_PER_WINDOW,
        CANVAS_HTTP_MAX_TOKEN_BYTES, CANVAS_RATE_LIMIT_MAX_IP_BUCKETS,
        CANVAS_RATE_LIMIT_MAX_REQUESTS_PER_WINDOW, CONSOLE_RELAY_TOKEN_DEFAULT_TTL_MS,
        CONSOLE_RELAY_TOKEN_MAX_TTL_MS, CONSOLE_RELAY_TOKEN_MIN_TTL_MS,
        DISCORD_APP_FLAG_GATEWAY_GUILD_MEMBERS, DISCORD_APP_FLAG_GATEWAY_MESSAGE_CONTENT,
        DISCORD_APP_FLAG_GATEWAY_PRESENCE,
    };
    use crate::gateway::GatewayAuthConfig;
    use crate::model_provider::{
        ModelProviderAuthProviderKind, ModelProviderConfig, ModelProviderCredentialSource,
        ModelProviderKind,
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
            "M45 onboarding should default to DM-only scope"
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
    fn build_memory_embedding_provider_requires_dims_when_model_is_configured() {
        let mut config = openai_model_provider_config();
        config.openai_embeddings_model = Some("text-embedding-3-small".to_owned());
        config.openai_embeddings_dims = None;

        let error = match build_memory_embedding_provider(&config, false) {
            Ok(_) => panic!("configured model without dims should fail"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("openai_embeddings_dims"),
            "error should explain missing dimensions requirement"
        );
    }

    #[test]
    fn build_memory_embedding_provider_uses_hash_fallback_in_explicit_offline_mode() {
        let mut config = openai_model_provider_config();
        config.openai_embeddings_model = Some("text-embedding-3-small".to_owned());
        config.openai_embeddings_dims = Some(8);

        let provider = build_memory_embedding_provider(&config, true)
            .expect("offline mode should allow hash fallback");
        assert_eq!(provider.model_name(), "hash-embedding-v1");
        assert_eq!(provider.dimensions(), 8);
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
        let (_tempdir, auth_registry, vault) = setup_auth_registry_and_vault();
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

        let audit = resolve_model_provider_secret(&mut model_provider, &auth_registry, &vault)
            .expect("auth profile resolution should succeed")
            .expect("audit record should be emitted for resolved secret");
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
        assert_eq!(audit.scope, "global");
        assert_eq!(audit.key, "openai_auth_key");
        assert_eq!(audit.action, "model_provider.auth_profile.api_key.resolve");
    }

    #[test]
    fn model_provider_secret_resolver_uses_legacy_vault_ref_when_auth_profile_is_unset() {
        let (_tempdir, auth_registry, vault) = setup_auth_registry_and_vault();
        let legacy_ref =
            VaultRef::parse("global/openai_legacy_key").expect("legacy vault ref should parse");
        vault
            .put_secret(&legacy_ref.scope, legacy_ref.key.as_str(), b"sk-legacy")
            .expect("legacy model provider key should be written");

        let mut model_provider = openai_model_provider_config();
        model_provider.openai_api_key_vault_ref = Some("global/openai_legacy_key".to_owned());

        let audit = resolve_model_provider_secret(&mut model_provider, &auth_registry, &vault)
            .expect("legacy vault-ref resolution should succeed")
            .expect("audit record should be emitted for resolved secret");
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
    }

    #[test]
    fn model_provider_secret_resolver_rejects_auth_profile_provider_mismatch() {
        let (_tempdir, auth_registry, vault) = setup_auth_registry_and_vault();
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

        let error = resolve_model_provider_secret(&mut model_provider, &auth_registry, &vault)
            .expect_err("provider mismatch should fail closed");
        assert!(
            error.to_string().contains("provider mismatch"),
            "resolver should explain provider mismatch when auth profile kind is incompatible"
        );
    }

    #[test]
    fn model_provider_secret_resolver_loads_oauth_access_token_from_auth_profile() {
        let (_tempdir, auth_registry, vault) = setup_auth_registry_and_vault();
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

        let audit = resolve_model_provider_secret(&mut model_provider, &auth_registry, &vault)
            .expect("oauth auth profile resolution should succeed")
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
    fn runtime_status_response_maps_resource_exhausted_to_too_many_requests() {
        let response = runtime_status_response(tonic::Status::resource_exhausted("rate limited"));
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
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
    fn admin_rate_limit_keeps_remote_budget_tighter_than_loopback() {
        let buckets = Mutex::new(HashMap::new());
        let ip = IpAddr::from_str("203.0.113.10").expect("IP literal should parse");
        let now = Instant::now();
        for attempt in 0..ADMIN_RATE_LIMIT_MAX_REQUESTS_PER_WINDOW {
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
