use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, RwLock,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::http::{header::AUTHORIZATION, HeaderMap};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use palyra_a2ui::{
    apply_patch_document, build_replace_root_patch, parse_patch_document, patch_document_to_bytes,
};
use palyra_auth::{
    AuthCredential, AuthCredentialType, AuthExpiryDistribution, AuthHealthSummary,
    AuthProfileError, AuthProfileHealthState, AuthProfileListFilter, AuthProfileRecord,
    AuthProfileRegistry, AuthProfileScope, AuthProfileSetRequest, AuthProvider, AuthProviderKind,
    AuthScopeFilter, OAuthRefreshAdapter, OAuthRefreshOutcome,
};
use palyra_common::{
    build_metadata, validate_canonical_id,
    workspace_patch::{
        apply_workspace_patch, compute_patch_sha256, redact_patch_preview, WorkspacePatchLimits,
        WorkspacePatchRedactionPolicy, WorkspacePatchRequest,
    },
    CANONICAL_PROTOCOL_MAJOR,
};
use palyra_policy::{
    evaluate_with_config, evaluate_with_context, PolicyDecision, PolicyEvaluationConfig,
    PolicyRequest, PolicyRequestContext,
};
#[cfg(test)]
use palyra_vault::{
    BackendPreference as VaultBackendPreference, VaultConfig as VaultConfigOptions,
};
use palyra_vault::{SecretMetadata as VaultSecretMetadata, Vault, VaultError, VaultScope};
use reqwest::{redirect::Policy, Url};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, Notify};
use tokio::time::{interval, timeout, MissedTickBehavior};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{metadata::MetadataMap, Request, Response, Status, Streaming};
use tracing::{info, warn};
use ulid::Ulid;

use crate::{
    agents::{
        AgentCreateOutcome, AgentCreateRequest, AgentListPage, AgentRecord, AgentRegistry,
        AgentRegistryError, AgentResolutionSource, AgentResolveOutcome, AgentResolveRequest,
        AgentSetDefaultOutcome,
    },
    channel_router::{
        ChannelRouter, ChannelRouterConfig, InboundMessage as ChannelInboundMessage,
        RetryDisposition, RouteOutcome, RoutedMessage as ChannelRoutedMessage,
    },
    cron::{normalize_schedule, schedule_to_proto, trigger_job_now, CronTimezoneMode},
    journal::{
        ApprovalCreateRequest, ApprovalDecision, ApprovalDecisionScope, ApprovalPolicySnapshot,
        ApprovalPromptOption, ApprovalPromptRecord, ApprovalRecord, ApprovalResolveRequest,
        ApprovalRiskLevel, ApprovalSubjectType, ApprovalsListFilter, CanvasStatePatchRecord,
        CanvasStateSnapshotRecord, CanvasStateTransitionRequest, CronConcurrencyPolicy,
        CronJobCreateRequest, CronJobRecord, CronJobUpdatePatch, CronJobsListFilter,
        CronRunFinalizeRequest, CronRunRecord, CronRunStartRequest, CronRunStatus,
        CronRunsListFilter, JournalAppendRequest, JournalError, JournalEventRecord, JournalStore,
        MemoryItemCreateRequest, MemoryItemRecord, MemoryItemsListFilter, MemoryPurgeRequest,
        MemorySearchHit, MemorySearchRequest, MemorySource, OrchestratorCancelRequest,
        OrchestratorRunStartRequest, OrchestratorRunStatusSnapshot, OrchestratorSessionRecord,
        OrchestratorSessionResolveOutcome, OrchestratorSessionResolveRequest,
        OrchestratorTapeAppendRequest, OrchestratorTapeRecord, OrchestratorUsageDelta,
        SkillExecutionStatus, SkillStatusRecord, SkillStatusUpsertRequest,
    },
    model_provider::{
        ModelProvider, ProviderError, ProviderEvent, ProviderRequest, ProviderStatusSnapshot,
    },
    orchestrator::{is_cancel_command, RunLifecycleState, RunStateMachine, RunTransition},
    tool_protocol::{
        decide_tool_call, denied_execution_outcome, execute_tool_call, tool_policy_snapshot,
        tool_requires_approval, ToolAttestation, ToolCallConfig, ToolCallPolicySnapshot,
        ToolDecision, ToolExecutionOutcome, ToolRequestContext,
    },
};

pub mod proto {
    pub mod palyra {
        pub mod common {
            pub mod v1 {
                tonic::include_proto!("palyra.common.v1");
            }
        }

        pub mod gateway {
            pub mod v1 {
                tonic::include_proto!("palyra.gateway.v1");
            }
        }

        pub mod cron {
            pub mod v1 {
                tonic::include_proto!("palyra.cron.v1");
            }
        }

        pub mod memory {
            pub mod v1 {
                tonic::include_proto!("palyra.memory.v1");
            }
        }

        pub mod auth {
            pub mod v1 {
                tonic::include_proto!("palyra.auth.v1");
            }
        }

        pub mod node {
            pub mod v1 {
                tonic::include_proto!("palyra.node.v1");
            }
        }

        pub mod browser {
            pub mod v1 {
                tonic::include_proto!("palyra.browser.v1");
            }
        }
    }
}

use proto::palyra::{
    auth::v1 as auth_v1, browser::v1 as browser_v1, common::v1 as common_v1, cron::v1 as cron_v1,
    gateway::v1 as gateway_v1, memory::v1 as memory_v1,
};

pub const HEADER_PRINCIPAL: &str = "x-palyra-principal";
pub const HEADER_DEVICE_ID: &str = "x-palyra-device-id";
pub const HEADER_CHANNEL: &str = "x-palyra-channel";
pub const HEADER_VAULT_READ_APPROVAL: &str = "x-palyra-vault-read-approval";
const MAX_JOURNAL_RECENT_EVENTS: usize = 100;
const MAX_SESSIONS_PAGE_LIMIT: usize = 500;
const MAX_AGENTS_PAGE_LIMIT: usize = 500;
const JOURNAL_WRITE_LATENCY_BUDGET_MS: u128 = 25;
const TOOL_EXECUTION_LATENCY_BUDGET_MS: u128 = 200;
const MIN_TAPE_PAGE_LIMIT: usize = 1;
const SENSITIVE_TOOLS_DENY_REASON: &str =
    "allow_sensitive_tools=true is denied by default and requires explicit approvals";
const CANCELLED_REASON: &str = "cancelled by request";
const APPROVAL_CHANNEL_UNAVAILABLE_REASON: &str =
    "approval required but no interactive approval channel is available for this run";
const APPROVAL_DENIED_REASON: &str = "tool execution denied by explicit client approval response";
const APPROVAL_DECISION_CACHE_CAPACITY: usize = 1_024;
const MAX_MODEL_TOKEN_TAPE_EVENTS_PER_RUN: usize = 1_024;
const MAX_CRON_JOB_NAME_BYTES: usize = 128;
const MAX_CRON_PROMPT_BYTES: usize = 16 * 1024;
const MAX_CRON_JITTER_MS: u64 = 60_000;
const MAX_CRON_PAGE_LIMIT: usize = 500;
const MAX_APPROVAL_PAGE_LIMIT: usize = 500;
const MAX_APPROVAL_EXPORT_LIMIT: usize = 5_000;
const MAX_APPROVAL_EXPORT_CHUNK_BYTES: usize = 64 * 1024;
const APPROVAL_EXPORT_NDJSON_SCHEMA_ID: &str = "palyra.approvals.export.ndjson.v1";
const APPROVAL_EXPORT_NDJSON_RECORD_TYPE_ENTRY: &str = "approval_record";
const APPROVAL_EXPORT_NDJSON_RECORD_TYPE_TRAILER: &str = "export_trailer";
const APPROVAL_EXPORT_CHAIN_SEED_HEX: &str =
    "0000000000000000000000000000000000000000000000000000000000000000";
const MAX_MEMORY_PAGE_LIMIT: usize = 500;
const MAX_MEMORY_SEARCH_TOP_K: usize = 64;
const MAX_MEMORY_ITEM_BYTES: usize = 16 * 1024;
const MAX_MEMORY_ITEM_TOKENS: usize = 2_048;
const MAX_MEMORY_TOOL_QUERY_BYTES: usize = 4 * 1024;
const MAX_MEMORY_TOOL_TAGS: usize = 32;
const MAX_WORKSPACE_PATCH_TOOL_INPUT_BYTES: usize = 256 * 1024;
const MAX_HTTP_FETCH_TOOL_INPUT_BYTES: usize = 64 * 1024;
const MAX_HTTP_FETCH_BODY_BYTES: usize = 512 * 1024;
const MAX_HTTP_FETCH_REDIRECTS: usize = 10;
const MAX_HTTP_FETCH_CACHE_KEY_BYTES: usize = 4 * 1024;
const MAX_BROWSER_TOOL_INPUT_BYTES: usize = 128 * 1024;
const MAX_CANVAS_ID_BYTES: usize = 64;
const MAX_CANVAS_BUNDLE_ID_BYTES: usize = 128;
const MAX_CANVAS_ASSET_PATH_BYTES: usize = 256;
const MAX_CANVAS_ASSET_CONTENT_TYPE_BYTES: usize = 128;
const MAX_CANVAS_ALLOWED_PARENT_ORIGINS: usize = 16;
const MAX_CANVAS_ORIGIN_BYTES: usize = 256;
const MAX_CANVAS_TOKEN_TTL_MS: u64 = 24 * 60 * 60 * 1_000;
const MIN_CANVAS_TOKEN_TTL_MS: u64 = 30 * 1_000;
const MAX_CANVAS_RECOVERY_SNAPSHOTS: usize = 10_000;
const MAX_CANVAS_STREAM_PATCH_BATCH: usize = 64;
const CANVAS_STREAM_POLL_INTERVAL: Duration = Duration::from_millis(250);
const MAX_PATCH_TOOL_REDACTION_PATTERNS: usize = 64;
const MAX_PATCH_TOOL_SECRET_FILE_MARKERS: usize = 64;
const MAX_PATCH_TOOL_PATTERN_BYTES: usize = 256;
const MAX_PATCH_TOOL_MARKER_BYTES: usize = 256;
const MAX_AGENT_STATUS_BINDINGS: usize = 128;
const MAX_VAULT_SECRET_BYTES: usize = 64 * 1024;
const MAX_VAULT_LIST_RESULTS: usize = 1_000;
const VAULT_RATE_LIMIT_WINDOW_MS: u64 = 1_000;
const VAULT_RATE_LIMIT_MAX_REQUESTS_PER_WINDOW: u32 = 30;
const VAULT_RATE_LIMIT_MAX_PRINCIPAL_BUCKETS: usize = 4_096;
const VAULT_READ_APPROVAL_ALLOW_VALUE: &str = "allow";
const MEMORY_SEARCH_LATENCY_BUDGET_MS: u128 = 75;
const MEMORY_SEARCH_CACHE_CAPACITY: usize = 128;
const MEMORY_AUTO_INJECT_MIN_SCORE: f64 = 0.2;
const APPROVAL_POLICY_ID: &str = "tool_call_policy.v1";
const APPROVAL_PROMPT_TIMEOUT_SECONDS: u32 = 60;
const APPROVAL_REQUEST_SUMMARY_MAX_BYTES: usize = 1024;
const TOOL_APPROVAL_RESPONSE_TIMEOUT: Duration = Duration::from_secs(60);
const SKILL_EXECUTION_DENY_REASON_PREFIX: &str = "skill execution blocked by security gate";
const WORKSPACE_PATCH_TOOL_NAME: &str = "palyra.fs.apply_patch";
const PROCESS_RUNNER_TOOL_NAME: &str = "palyra.process.run";
const HTTP_FETCH_TOOL_NAME: &str = "palyra.http.fetch";
const BROWSER_SESSION_CREATE_TOOL_NAME: &str = "palyra.browser.session.create";
const BROWSER_SESSION_CLOSE_TOOL_NAME: &str = "palyra.browser.session.close";
const BROWSER_NAVIGATE_TOOL_NAME: &str = "palyra.browser.navigate";
const BROWSER_CLICK_TOOL_NAME: &str = "palyra.browser.click";
const BROWSER_TYPE_TOOL_NAME: &str = "palyra.browser.type";
const BROWSER_SCROLL_TOOL_NAME: &str = "palyra.browser.scroll";
const BROWSER_WAIT_FOR_TOOL_NAME: &str = "palyra.browser.wait_for";
const BROWSER_TITLE_TOOL_NAME: &str = "palyra.browser.title";
const BROWSER_SCREENSHOT_TOOL_NAME: &str = "palyra.browser.screenshot";
const BROWSER_OBSERVE_TOOL_NAME: &str = "palyra.browser.observe";
const BROWSER_NETWORK_LOG_TOOL_NAME: &str = "palyra.browser.network_log";
const BROWSER_RESET_STATE_TOOL_NAME: &str = "palyra.browser.reset_state";
const BROWSER_TABS_LIST_TOOL_NAME: &str = "palyra.browser.tabs.list";
const BROWSER_TABS_OPEN_TOOL_NAME: &str = "palyra.browser.tabs.open";
const BROWSER_TABS_SWITCH_TOOL_NAME: &str = "palyra.browser.tabs.switch";
const BROWSER_TABS_CLOSE_TOOL_NAME: &str = "palyra.browser.tabs.close";
const BROWSER_PERMISSIONS_GET_TOOL_NAME: &str = "palyra.browser.permissions.get";
const BROWSER_PERMISSIONS_SET_TOOL_NAME: &str = "palyra.browser.permissions.set";

#[derive(Debug, Clone)]
pub struct GatewayRuntimeConfigSnapshot {
    pub grpc_bind_addr: String,
    pub grpc_port: u16,
    pub quic_bind_addr: String,
    pub quic_port: u16,
    pub quic_enabled: bool,
    pub orchestrator_runloop_v1_enabled: bool,
    pub node_rpc_mtls_required: bool,
    pub admin_auth_required: bool,
    pub vault_get_approval_required_refs: Vec<String>,
    pub max_tape_entries_per_response: usize,
    pub max_tape_bytes_per_response: usize,
    pub channel_router: ChannelRouterConfig,
    pub tool_call: ToolCallConfig,
    pub http_fetch: HttpFetchRuntimeConfig,
    pub browser_service: BrowserServiceRuntimeConfig,
    pub canvas_host: CanvasHostRuntimeConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MemoryRuntimeConfig {
    pub max_item_bytes: usize,
    pub max_item_tokens: usize,
    pub auto_inject_enabled: bool,
    pub auto_inject_max_items: usize,
    pub default_ttl_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HttpFetchRuntimeConfig {
    pub allow_private_targets: bool,
    pub connect_timeout_ms: u64,
    pub request_timeout_ms: u64,
    pub max_response_bytes: usize,
    pub allow_redirects: bool,
    pub max_redirects: usize,
    pub allowed_content_types: Vec<String>,
    pub allowed_request_headers: Vec<String>,
    pub cache_enabled: bool,
    pub cache_ttl_ms: u64,
    pub max_cache_entries: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BrowserServiceRuntimeConfig {
    pub enabled: bool,
    pub endpoint: String,
    pub auth_token: Option<String>,
    pub connect_timeout_ms: u64,
    pub request_timeout_ms: u64,
    pub max_screenshot_bytes: usize,
    pub max_title_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CanvasHostRuntimeConfig {
    pub enabled: bool,
    pub public_base_url: String,
    pub token_ttl_ms: u64,
    pub max_state_bytes: usize,
    pub max_bundle_bytes: usize,
    pub max_assets_per_bundle: usize,
    pub max_updates_per_minute: usize,
}

impl Default for MemoryRuntimeConfig {
    fn default() -> Self {
        Self {
            max_item_bytes: MAX_MEMORY_ITEM_BYTES,
            max_item_tokens: MAX_MEMORY_ITEM_TOKENS,
            auto_inject_enabled: false,
            auto_inject_max_items: 3,
            default_ttl_ms: Some(30 * 24 * 60 * 60 * 1_000),
        }
    }
}

#[derive(Debug, Clone)]
struct ToolApprovalOutcome {
    approval_id: String,
    approved: bool,
    reason: String,
    decision: ApprovalDecision,
    decision_scope: ApprovalDecisionScope,
    decision_scope_ttl_ms: Option<i64>,
}

#[derive(Debug, Clone)]
struct CachedToolApprovalDecision {
    approval_id: String,
    approved: bool,
    reason: String,
    decision: ApprovalDecision,
    decision_scope: ApprovalDecisionScope,
    expires_at_unix_ms: Option<i64>,
}

#[derive(Debug, Clone)]
struct CachedHttpFetchEntry {
    expires_at_unix_ms: i64,
    output_json: Vec<u8>,
}

#[derive(Debug, Clone)]
struct PendingToolApproval {
    approval_id: String,
    request_summary: String,
    policy_snapshot: ApprovalPolicySnapshot,
    prompt: ApprovalPromptRecord,
}

#[derive(Debug, Clone)]
struct ToolSkillContext {
    skill_id: String,
    version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CanvasAssetRecord {
    content_type: String,
    body: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CanvasBundleRecord {
    bundle_id: String,
    entrypoint_path: String,
    assets: HashMap<String, CanvasAssetRecord>,
    sha256: String,
    signature: String,
}

#[derive(Debug, Clone)]
struct CanvasRecord {
    canvas_id: String,
    session_id: String,
    principal: String,
    state_version: u64,
    state_schema_version: u64,
    state_json: Vec<u8>,
    bundle: CanvasBundleRecord,
    allowed_parent_origins: Vec<String>,
    created_at_unix_ms: i64,
    updated_at_unix_ms: i64,
    expires_at_unix_ms: i64,
    closed: bool,
    close_reason: Option<String>,
    update_timestamps_unix_ms: VecDeque<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CanvasFrameDocument {
    pub canvas_id: String,
    pub html: String,
    pub csp: String,
    pub expires_at_unix_ms: i64,
}

#[derive(Debug, Clone)]
pub struct CanvasAssetResponse {
    pub content_type: String,
    pub body: Vec<u8>,
    pub csp: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct CanvasStateResponse {
    pub canvas_id: String,
    pub state_version: u64,
    pub state_schema_version: u64,
    pub state: Value,
    pub closed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub close_reason: Option<String>,
    pub expires_at_unix_ms: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct CanvasRuntimeDescriptor {
    pub canvas_id: String,
    pub frame_url: String,
    pub runtime_url: String,
    pub auth_token: String,
    pub expires_at_unix_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CanvasTokenPayload {
    canvas_id: String,
    principal: String,
    session_id: String,
    issued_at_unix_ms: i64,
    expires_at_unix_ms: i64,
    nonce: String,
}

#[derive(Debug, Clone, Copy)]
struct VaultRateLimitEntry {
    window_started_at: Instant,
    requests_in_window: u32,
}

#[derive(Debug, Clone)]
pub struct GatewayJournalConfigSnapshot {
    pub db_path: PathBuf,
    pub hash_chain_enabled: bool,
}

#[derive(Debug, Clone)]
pub struct GatewayAuthConfig {
    pub require_auth: bool,
    pub admin_token: Option<String>,
    pub bound_principal: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RequestContext {
    pub principal: String,
    pub device_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
}

pub struct GatewayRuntimeState {
    started_at: Instant,
    build: BuildSnapshot,
    config: GatewayRuntimeConfigSnapshot,
    journal_config: GatewayJournalConfigSnapshot,
    counters: RuntimeCounters,
    journal_store: JournalStore,
    revoked_certificate_count: usize,
    model_provider: Arc<dyn ModelProvider>,
    vault: Arc<Vault>,
    memory_config: RwLock<MemoryRuntimeConfig>,
    memory_search_cache: Mutex<HashMap<String, Vec<MemorySearchHit>>>,
    http_fetch_cache: Mutex<HashMap<String, CachedHttpFetchEntry>>,
    tool_approval_cache: Mutex<HashMap<String, CachedToolApprovalDecision>>,
    vault_rate_limit: Mutex<HashMap<String, VaultRateLimitEntry>>,
    canvas_records: Mutex<HashMap<String, CanvasRecord>>,
    canvas_signing_secret: [u8; 32],
    agent_registry: AgentRegistry,
    channel_router: ChannelRouter,
}

#[derive(Debug)]
struct RuntimeCounters {
    run_stream_requests: AtomicU64,
    append_event_requests: AtomicU64,
    admin_status_requests: AtomicU64,
    denied_requests: AtomicU64,
    journal_events: AtomicU64,
    journal_persist_failures: AtomicU64,
    journal_redacted_events: AtomicU64,
    orchestrator_runs_started: AtomicU64,
    orchestrator_runs_completed: AtomicU64,
    orchestrator_runs_cancelled: AtomicU64,
    orchestrator_cancel_requests: AtomicU64,
    orchestrator_tape_events: AtomicU64,
    model_provider_requests: AtomicU64,
    model_provider_failures: AtomicU64,
    model_provider_retry_attempts: AtomicU64,
    model_provider_circuit_open_rejections: AtomicU64,
    tool_proposals: AtomicU64,
    tool_decisions_allowed: AtomicU64,
    tool_decisions_denied: AtomicU64,
    tool_execution_attempts: AtomicU64,
    tool_execution_failures: AtomicU64,
    tool_execution_timeouts: AtomicU64,
    tool_attestations_emitted: AtomicU64,
    sandbox_launches: AtomicU64,
    sandbox_policy_denies: AtomicU64,
    sandbox_escape_attempts_blocked_workspace: AtomicU64,
    sandbox_escape_attempts_blocked_egress: AtomicU64,
    sandbox_escape_attempts_blocked_executable: AtomicU64,
    sandbox_backend_selected_tier_b: AtomicU64,
    sandbox_backend_selected_tier_c_linux_bubblewrap: AtomicU64,
    sandbox_backend_selected_tier_c_macos_sandbox_exec: AtomicU64,
    sandbox_backend_selected_tier_c_windows_job_object: AtomicU64,
    patches_applied: AtomicU64,
    patches_rejected: AtomicU64,
    patch_files_touched: AtomicU64,
    patch_rollbacks: AtomicU64,
    cron_jobs_created: AtomicU64,
    cron_jobs_updated: AtomicU64,
    cron_jobs_deleted: AtomicU64,
    cron_triggers_fired: AtomicU64,
    cron_runs_started: AtomicU64,
    cron_runs_completed: AtomicU64,
    cron_runs_failed: AtomicU64,
    cron_runs_skipped: AtomicU64,
    memory_items_ingested: AtomicU64,
    memory_items_rejected: AtomicU64,
    memory_search_requests: AtomicU64,
    memory_search_cache_hits: AtomicU64,
    memory_auto_inject_events: AtomicU64,
    vault_put_requests: AtomicU64,
    vault_get_requests: AtomicU64,
    vault_delete_requests: AtomicU64,
    vault_list_requests: AtomicU64,
    vault_rate_limited_requests: AtomicU64,
    vault_access_audit_events: AtomicU64,
    skill_status_updates: AtomicU64,
    skill_execution_denied: AtomicU64,
    approvals_tool_requested: AtomicU64,
    approvals_tool_resolved_allow: AtomicU64,
    approvals_tool_resolved_deny: AtomicU64,
    approvals_tool_resolved_timeout: AtomicU64,
    approvals_tool_resolved_error: AtomicU64,
    agent_mutations: AtomicU64,
    agent_resolution_hits: AtomicU64,
    agent_resolution_misses: AtomicU64,
    agent_validation_failures: AtomicU64,
    channel_messages_inbound: AtomicU64,
    channel_messages_routed: AtomicU64,
    channel_messages_replied: AtomicU64,
    channel_messages_rejected: AtomicU64,
    channel_messages_queued: AtomicU64,
    channel_messages_quarantined: AtomicU64,
    channel_router_queue_depth: AtomicU64,
    channel_reply_failures: AtomicU64,
    canvas_created: AtomicU64,
    canvas_updated: AtomicU64,
    canvas_closed: AtomicU64,
    canvas_denied: AtomicU64,
}

#[derive(Debug, Clone, Serialize)]
struct BuildSnapshot {
    version: String,
    git_hash: String,
    build_profile: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct GatewayStatusSnapshot {
    pub service: &'static str,
    pub status: &'static str,
    pub version: String,
    pub git_hash: String,
    pub build_profile: String,
    pub uptime_seconds: u64,
    pub transport: TransportSnapshot,
    pub security: SecuritySnapshot,
    pub storage: StorageSnapshot,
    pub model_provider: ProviderStatusSnapshot,
    pub tool_call_policy: ToolCallPolicySnapshot,
    pub counters: CountersSnapshot,
    pub agents: AgentRuntimeSnapshot,
    pub request_context: RequestContext,
}

#[derive(Debug, Clone, Serialize)]
pub struct TransportSnapshot {
    pub grpc_bind_addr: String,
    pub grpc_port: u16,
    pub quic_bind_addr: String,
    pub quic_port: u16,
    pub quic_enabled: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct SecuritySnapshot {
    pub deny_by_default: bool,
    pub admin_auth_required: bool,
    pub admin_token_configured: bool,
    pub orchestrator_runloop_v1_enabled: bool,
    pub node_rpc_mtls_required: bool,
    pub revoked_certificate_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct StorageSnapshot {
    pub journal_db_path: String,
    pub journal_hash_chain_enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_event_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CountersSnapshot {
    pub run_stream_requests: u64,
    pub append_event_requests: u64,
    pub admin_status_requests: u64,
    pub denied_requests: u64,
    pub journal_events: u64,
    pub journal_persist_failures: u64,
    pub journal_redacted_events: u64,
    pub orchestrator_runs_started: u64,
    pub orchestrator_runs_completed: u64,
    pub orchestrator_runs_cancelled: u64,
    pub orchestrator_cancel_requests: u64,
    pub orchestrator_tape_events: u64,
    pub model_provider_requests: u64,
    pub model_provider_failures: u64,
    pub model_provider_retry_attempts: u64,
    pub model_provider_circuit_open_rejections: u64,
    pub tool_proposals: u64,
    pub tool_decisions_allowed: u64,
    pub tool_decisions_denied: u64,
    pub tool_execution_attempts: u64,
    pub tool_execution_failures: u64,
    pub tool_execution_timeouts: u64,
    pub tool_attestations_emitted: u64,
    pub sandbox_launches: u64,
    pub sandbox_policy_denies: u64,
    pub sandbox_escape_attempts_blocked_workspace: u64,
    pub sandbox_escape_attempts_blocked_egress: u64,
    pub sandbox_escape_attempts_blocked_executable: u64,
    pub sandbox_backend_selected_tier_b: u64,
    pub sandbox_backend_selected_tier_c_linux_bubblewrap: u64,
    pub sandbox_backend_selected_tier_c_macos_sandbox_exec: u64,
    pub sandbox_backend_selected_tier_c_windows_job_object: u64,
    pub patches_applied: u64,
    pub patches_rejected: u64,
    pub patch_files_touched: u64,
    pub patch_rollbacks: u64,
    pub cron_jobs_created: u64,
    pub cron_jobs_updated: u64,
    pub cron_jobs_deleted: u64,
    pub cron_triggers_fired: u64,
    pub cron_runs_started: u64,
    pub cron_runs_completed: u64,
    pub cron_runs_failed: u64,
    pub cron_runs_skipped: u64,
    pub memory_items_ingested: u64,
    pub memory_items_rejected: u64,
    pub memory_search_requests: u64,
    pub memory_search_cache_hits: u64,
    pub memory_auto_inject_events: u64,
    pub vault_put_requests: u64,
    pub vault_get_requests: u64,
    pub vault_delete_requests: u64,
    pub vault_list_requests: u64,
    pub vault_rate_limited_requests: u64,
    pub vault_access_audit_events: u64,
    pub skill_status_updates: u64,
    pub skill_execution_denied: u64,
    pub approvals_tool_requested: u64,
    pub approvals_tool_resolved_allow: u64,
    pub approvals_tool_resolved_deny: u64,
    pub approvals_tool_resolved_timeout: u64,
    pub approvals_tool_resolved_error: u64,
    pub agent_mutations: u64,
    pub agent_resolution_hits: u64,
    pub agent_resolution_misses: u64,
    pub agent_validation_failures: u64,
    pub channel_messages_inbound: u64,
    pub channel_messages_routed: u64,
    pub channel_messages_replied: u64,
    pub channel_messages_rejected: u64,
    pub channel_messages_queued: u64,
    pub channel_messages_quarantined: u64,
    pub channel_router_queue_depth: u64,
    pub channel_reply_failures: u64,
    pub canvas_created: u64,
    pub canvas_updated: u64,
    pub canvas_closed: u64,
    pub canvas_denied: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct AgentRuntimeSnapshot {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_agent_id: Option<String>,
    pub agent_count: usize,
    pub active_session_bindings: Vec<AgentSessionBindingSnapshot>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AgentSessionBindingSnapshot {
    pub session_id_redacted: String,
    pub agent_id: String,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct AuthProviderRefreshMetricsSnapshot {
    pub provider: String,
    pub attempts: u64,
    pub successes: u64,
    pub failures: u64,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct AuthRefreshMetricsSnapshot {
    pub attempts: u64,
    pub successes: u64,
    pub failures: u64,
    pub by_provider: Vec<AuthProviderRefreshMetricsSnapshot>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct AuthAdminStatusSnapshot {
    pub summary: AuthHealthSummary,
    pub expiry_distribution: AuthExpiryDistribution,
    pub refresh_metrics: AuthRefreshMetricsSnapshot,
}

#[derive(Debug, Clone, Serialize)]
pub struct JournalRecentSnapshot {
    pub total_events: u64,
    pub hash_chain_enabled: bool,
    pub events: Vec<JournalEventRecord>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunTapeSnapshot {
    pub run_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requested_after_seq: Option<i64>,
    pub limit: usize,
    pub max_response_bytes: usize,
    pub returned_bytes: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_after_seq: Option<i64>,
    pub events: Vec<OrchestratorTapeRecord>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunCancelSnapshot {
    pub run_id: String,
    pub cancel_requested: bool,
    pub reason: String,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AuthError {
    #[error("admin auth token is required but no token is configured")]
    MissingConfiguredToken,
    #[error("authorization header is missing or malformed")]
    InvalidAuthorizationHeader,
    #[error("authorization token is invalid")]
    InvalidToken,
    #[error("request context field '{0}' is required")]
    MissingContext(&'static str),
    #[error("request context field '{0}' cannot be empty")]
    EmptyContext(&'static str),
    #[error("request context device_id must be a canonical ULID")]
    InvalidDeviceId,
}

#[derive(Debug, Clone, Copy, Default)]
struct AuthProviderRefreshCounters {
    attempts: u64,
    successes: u64,
    failures: u64,
}

#[derive(Debug, Default)]
struct AuthRefreshMetricsState {
    attempts: AtomicU64,
    successes: AtomicU64,
    failures: AtomicU64,
    by_provider: Mutex<HashMap<String, AuthProviderRefreshCounters>>,
}

impl AuthRefreshMetricsState {
    fn record_outcome(&self, outcome: &OAuthRefreshOutcome) {
        if !outcome.kind.attempted() {
            return;
        }
        self.attempts.fetch_add(1, Ordering::Relaxed);
        if outcome.kind.success() {
            self.successes.fetch_add(1, Ordering::Relaxed);
        } else {
            self.failures.fetch_add(1, Ordering::Relaxed);
        }
        if let Ok(mut guard) = self.by_provider.lock() {
            let provider_key = outcome.provider.to_ascii_lowercase();
            let entry = guard.entry(provider_key).or_default();
            entry.attempts = entry.attempts.saturating_add(1);
            if outcome.kind.success() {
                entry.successes = entry.successes.saturating_add(1);
            } else {
                entry.failures = entry.failures.saturating_add(1);
            }
        }
    }

    fn snapshot(&self) -> AuthRefreshMetricsSnapshot {
        let by_provider = if let Ok(guard) = self.by_provider.lock() {
            let mut rows = guard
                .iter()
                .map(|(provider, counters)| AuthProviderRefreshMetricsSnapshot {
                    provider: provider.clone(),
                    attempts: counters.attempts,
                    successes: counters.successes,
                    failures: counters.failures,
                })
                .collect::<Vec<_>>();
            rows.sort_by(|left, right| left.provider.cmp(&right.provider));
            rows
        } else {
            Vec::new()
        };
        AuthRefreshMetricsSnapshot {
            attempts: self.attempts.load(Ordering::Relaxed),
            successes: self.successes.load(Ordering::Relaxed),
            failures: self.failures.load(Ordering::Relaxed),
            by_provider,
        }
    }
}

#[derive(Clone)]
pub struct AuthRuntimeState {
    registry: Arc<AuthProfileRegistry>,
    refresh_adapter: Arc<dyn OAuthRefreshAdapter>,
    refresh_metrics: Arc<AuthRefreshMetricsState>,
}

impl AuthRuntimeState {
    #[must_use]
    pub fn new(
        registry: Arc<AuthProfileRegistry>,
        refresh_adapter: Arc<dyn OAuthRefreshAdapter>,
    ) -> Self {
        Self {
            registry,
            refresh_adapter,
            refresh_metrics: Arc::new(AuthRefreshMetricsState::default()),
        }
    }

    pub fn registry(&self) -> &AuthProfileRegistry {
        self.registry.as_ref()
    }

    pub fn refresh_metrics_snapshot(&self) -> AuthRefreshMetricsSnapshot {
        self.refresh_metrics.snapshot()
    }

    pub fn record_refresh_outcome(&self, outcome: &OAuthRefreshOutcome) {
        self.refresh_metrics.record_outcome(outcome);
    }

    #[allow(clippy::result_large_err)]
    pub async fn admin_status_snapshot(
        self: &Arc<Self>,
        runtime_state: Arc<GatewayRuntimeState>,
    ) -> Result<AuthAdminStatusSnapshot, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let report = state
                .registry
                .health_report(runtime_state.vault.as_ref(), None)
                .map_err(map_auth_profile_error)?;
            Ok(AuthAdminStatusSnapshot {
                summary: report.summary,
                expiry_distribution: report.expiry_distribution,
                refresh_metrics: state.refresh_metrics.snapshot(),
            })
        })
        .await
        .map_err(|_| Status::internal("auth status worker panicked"))?
    }
}

impl RuntimeCounters {
    fn snapshot(&self) -> CountersSnapshot {
        CountersSnapshot {
            run_stream_requests: self.run_stream_requests.load(Ordering::Relaxed),
            append_event_requests: self.append_event_requests.load(Ordering::Relaxed),
            admin_status_requests: self.admin_status_requests.load(Ordering::Relaxed),
            denied_requests: self.denied_requests.load(Ordering::Relaxed),
            journal_events: self.journal_events.load(Ordering::Relaxed),
            journal_persist_failures: self.journal_persist_failures.load(Ordering::Relaxed),
            journal_redacted_events: self.journal_redacted_events.load(Ordering::Relaxed),
            orchestrator_runs_started: self.orchestrator_runs_started.load(Ordering::Relaxed),
            orchestrator_runs_completed: self.orchestrator_runs_completed.load(Ordering::Relaxed),
            orchestrator_runs_cancelled: self.orchestrator_runs_cancelled.load(Ordering::Relaxed),
            orchestrator_cancel_requests: self.orchestrator_cancel_requests.load(Ordering::Relaxed),
            orchestrator_tape_events: self.orchestrator_tape_events.load(Ordering::Relaxed),
            model_provider_requests: self.model_provider_requests.load(Ordering::Relaxed),
            model_provider_failures: self.model_provider_failures.load(Ordering::Relaxed),
            model_provider_retry_attempts: self
                .model_provider_retry_attempts
                .load(Ordering::Relaxed),
            model_provider_circuit_open_rejections: self
                .model_provider_circuit_open_rejections
                .load(Ordering::Relaxed),
            tool_proposals: self.tool_proposals.load(Ordering::Relaxed),
            tool_decisions_allowed: self.tool_decisions_allowed.load(Ordering::Relaxed),
            tool_decisions_denied: self.tool_decisions_denied.load(Ordering::Relaxed),
            tool_execution_attempts: self.tool_execution_attempts.load(Ordering::Relaxed),
            tool_execution_failures: self.tool_execution_failures.load(Ordering::Relaxed),
            tool_execution_timeouts: self.tool_execution_timeouts.load(Ordering::Relaxed),
            tool_attestations_emitted: self.tool_attestations_emitted.load(Ordering::Relaxed),
            sandbox_launches: self.sandbox_launches.load(Ordering::Relaxed),
            sandbox_policy_denies: self.sandbox_policy_denies.load(Ordering::Relaxed),
            sandbox_escape_attempts_blocked_workspace: self
                .sandbox_escape_attempts_blocked_workspace
                .load(Ordering::Relaxed),
            sandbox_escape_attempts_blocked_egress: self
                .sandbox_escape_attempts_blocked_egress
                .load(Ordering::Relaxed),
            sandbox_escape_attempts_blocked_executable: self
                .sandbox_escape_attempts_blocked_executable
                .load(Ordering::Relaxed),
            sandbox_backend_selected_tier_b: self
                .sandbox_backend_selected_tier_b
                .load(Ordering::Relaxed),
            sandbox_backend_selected_tier_c_linux_bubblewrap: self
                .sandbox_backend_selected_tier_c_linux_bubblewrap
                .load(Ordering::Relaxed),
            sandbox_backend_selected_tier_c_macos_sandbox_exec: self
                .sandbox_backend_selected_tier_c_macos_sandbox_exec
                .load(Ordering::Relaxed),
            sandbox_backend_selected_tier_c_windows_job_object: self
                .sandbox_backend_selected_tier_c_windows_job_object
                .load(Ordering::Relaxed),
            patches_applied: self.patches_applied.load(Ordering::Relaxed),
            patches_rejected: self.patches_rejected.load(Ordering::Relaxed),
            patch_files_touched: self.patch_files_touched.load(Ordering::Relaxed),
            patch_rollbacks: self.patch_rollbacks.load(Ordering::Relaxed),
            cron_jobs_created: self.cron_jobs_created.load(Ordering::Relaxed),
            cron_jobs_updated: self.cron_jobs_updated.load(Ordering::Relaxed),
            cron_jobs_deleted: self.cron_jobs_deleted.load(Ordering::Relaxed),
            cron_triggers_fired: self.cron_triggers_fired.load(Ordering::Relaxed),
            cron_runs_started: self.cron_runs_started.load(Ordering::Relaxed),
            cron_runs_completed: self.cron_runs_completed.load(Ordering::Relaxed),
            cron_runs_failed: self.cron_runs_failed.load(Ordering::Relaxed),
            cron_runs_skipped: self.cron_runs_skipped.load(Ordering::Relaxed),
            memory_items_ingested: self.memory_items_ingested.load(Ordering::Relaxed),
            memory_items_rejected: self.memory_items_rejected.load(Ordering::Relaxed),
            memory_search_requests: self.memory_search_requests.load(Ordering::Relaxed),
            memory_search_cache_hits: self.memory_search_cache_hits.load(Ordering::Relaxed),
            memory_auto_inject_events: self.memory_auto_inject_events.load(Ordering::Relaxed),
            vault_put_requests: self.vault_put_requests.load(Ordering::Relaxed),
            vault_get_requests: self.vault_get_requests.load(Ordering::Relaxed),
            vault_delete_requests: self.vault_delete_requests.load(Ordering::Relaxed),
            vault_list_requests: self.vault_list_requests.load(Ordering::Relaxed),
            vault_rate_limited_requests: self.vault_rate_limited_requests.load(Ordering::Relaxed),
            vault_access_audit_events: self.vault_access_audit_events.load(Ordering::Relaxed),
            skill_status_updates: self.skill_status_updates.load(Ordering::Relaxed),
            skill_execution_denied: self.skill_execution_denied.load(Ordering::Relaxed),
            approvals_tool_requested: self.approvals_tool_requested.load(Ordering::Relaxed),
            approvals_tool_resolved_allow: self
                .approvals_tool_resolved_allow
                .load(Ordering::Relaxed),
            approvals_tool_resolved_deny: self.approvals_tool_resolved_deny.load(Ordering::Relaxed),
            approvals_tool_resolved_timeout: self
                .approvals_tool_resolved_timeout
                .load(Ordering::Relaxed),
            approvals_tool_resolved_error: self
                .approvals_tool_resolved_error
                .load(Ordering::Relaxed),
            agent_mutations: self.agent_mutations.load(Ordering::Relaxed),
            agent_resolution_hits: self.agent_resolution_hits.load(Ordering::Relaxed),
            agent_resolution_misses: self.agent_resolution_misses.load(Ordering::Relaxed),
            agent_validation_failures: self.agent_validation_failures.load(Ordering::Relaxed),
            channel_messages_inbound: self.channel_messages_inbound.load(Ordering::Relaxed),
            channel_messages_routed: self.channel_messages_routed.load(Ordering::Relaxed),
            channel_messages_replied: self.channel_messages_replied.load(Ordering::Relaxed),
            channel_messages_rejected: self.channel_messages_rejected.load(Ordering::Relaxed),
            channel_messages_queued: self.channel_messages_queued.load(Ordering::Relaxed),
            channel_messages_quarantined: self.channel_messages_quarantined.load(Ordering::Relaxed),
            channel_router_queue_depth: self.channel_router_queue_depth.load(Ordering::Relaxed),
            channel_reply_failures: self.channel_reply_failures.load(Ordering::Relaxed),
            canvas_created: self.canvas_created.load(Ordering::Relaxed),
            canvas_updated: self.canvas_updated.load(Ordering::Relaxed),
            canvas_closed: self.canvas_closed.load(Ordering::Relaxed),
            canvas_denied: self.canvas_denied.load(Ordering::Relaxed),
        }
    }
}

impl GatewayRuntimeState {
    #[cfg(test)]
    pub fn new(
        config: GatewayRuntimeConfigSnapshot,
        journal_config: GatewayJournalConfigSnapshot,
        journal_store: JournalStore,
        revoked_certificate_count: usize,
        agent_registry: AgentRegistry,
    ) -> Result<Arc<Self>, JournalError> {
        let default_provider = crate::model_provider::build_model_provider(
            &crate::model_provider::ModelProviderConfig::default(),
        )
        .expect("default deterministic model provider should initialize");
        let default_vault = build_test_vault();
        Self::new_with_provider(
            config,
            journal_config,
            journal_store,
            revoked_certificate_count,
            default_provider,
            default_vault,
            agent_registry,
        )
    }

    pub fn new_with_provider(
        config: GatewayRuntimeConfigSnapshot,
        journal_config: GatewayJournalConfigSnapshot,
        journal_store: JournalStore,
        revoked_certificate_count: usize,
        model_provider: Arc<dyn ModelProvider>,
        vault: Arc<Vault>,
        agent_registry: AgentRegistry,
    ) -> Result<Arc<Self>, JournalError> {
        let build = build_metadata();
        let existing_events = journal_store.total_events()? as u64;
        let canvas_snapshots =
            journal_store.list_canvas_state_snapshots(MAX_CANVAS_RECOVERY_SNAPSHOTS)?;
        for snapshot in &canvas_snapshots {
            let replayed = journal_store.replay_canvas_state(snapshot.canvas_id.as_str())?.ok_or(
                JournalError::InvalidCanvasReplay {
                    canvas_id: snapshot.canvas_id.clone(),
                    reason: "snapshot exists but replay produced no state".to_owned(),
                },
            )?;
            let replay_state: Value =
                serde_json::from_str(replayed.state_json.as_str()).map_err(|error| {
                    JournalError::InvalidCanvasReplay {
                        canvas_id: snapshot.canvas_id.clone(),
                        reason: format!("replay state JSON is invalid: {error}"),
                    }
                })?;
            let snapshot_state: Value = serde_json::from_str(snapshot.state_json.as_str())
                .map_err(|error| JournalError::InvalidCanvasReplay {
                    canvas_id: snapshot.canvas_id.clone(),
                    reason: format!("snapshot state JSON is invalid: {error}"),
                })?;
            if replayed.state_version != snapshot.state_version
                || replayed.state_schema_version != snapshot.state_schema_version
                || replay_state != snapshot_state
            {
                return Err(JournalError::InvalidCanvasReplay {
                    canvas_id: snapshot.canvas_id.clone(),
                    reason: "replay outcome does not match latest snapshot".to_owned(),
                });
            }
        }
        let recovered_canvas_records =
            load_canvas_records_from_snapshots(canvas_snapshots.as_slice())?;
        let channel_router = ChannelRouter::new(config.channel_router.clone());
        Ok(Arc::new(Self {
            started_at: Instant::now(),
            build: BuildSnapshot {
                version: build.version.to_owned(),
                git_hash: build.git_hash.to_owned(),
                build_profile: build.build_profile.to_owned(),
            },
            config,
            journal_config,
            counters: RuntimeCounters {
                run_stream_requests: AtomicU64::new(0),
                append_event_requests: AtomicU64::new(0),
                admin_status_requests: AtomicU64::new(0),
                denied_requests: AtomicU64::new(0),
                journal_events: AtomicU64::new(existing_events),
                journal_persist_failures: AtomicU64::new(0),
                journal_redacted_events: AtomicU64::new(0),
                orchestrator_runs_started: AtomicU64::new(0),
                orchestrator_runs_completed: AtomicU64::new(0),
                orchestrator_runs_cancelled: AtomicU64::new(0),
                orchestrator_cancel_requests: AtomicU64::new(0),
                orchestrator_tape_events: AtomicU64::new(0),
                model_provider_requests: AtomicU64::new(0),
                model_provider_failures: AtomicU64::new(0),
                model_provider_retry_attempts: AtomicU64::new(0),
                model_provider_circuit_open_rejections: AtomicU64::new(0),
                tool_proposals: AtomicU64::new(0),
                tool_decisions_allowed: AtomicU64::new(0),
                tool_decisions_denied: AtomicU64::new(0),
                tool_execution_attempts: AtomicU64::new(0),
                tool_execution_failures: AtomicU64::new(0),
                tool_execution_timeouts: AtomicU64::new(0),
                tool_attestations_emitted: AtomicU64::new(0),
                sandbox_launches: AtomicU64::new(0),
                sandbox_policy_denies: AtomicU64::new(0),
                sandbox_escape_attempts_blocked_workspace: AtomicU64::new(0),
                sandbox_escape_attempts_blocked_egress: AtomicU64::new(0),
                sandbox_escape_attempts_blocked_executable: AtomicU64::new(0),
                sandbox_backend_selected_tier_b: AtomicU64::new(0),
                sandbox_backend_selected_tier_c_linux_bubblewrap: AtomicU64::new(0),
                sandbox_backend_selected_tier_c_macos_sandbox_exec: AtomicU64::new(0),
                sandbox_backend_selected_tier_c_windows_job_object: AtomicU64::new(0),
                patches_applied: AtomicU64::new(0),
                patches_rejected: AtomicU64::new(0),
                patch_files_touched: AtomicU64::new(0),
                patch_rollbacks: AtomicU64::new(0),
                cron_jobs_created: AtomicU64::new(0),
                cron_jobs_updated: AtomicU64::new(0),
                cron_jobs_deleted: AtomicU64::new(0),
                cron_triggers_fired: AtomicU64::new(0),
                cron_runs_started: AtomicU64::new(0),
                cron_runs_completed: AtomicU64::new(0),
                cron_runs_failed: AtomicU64::new(0),
                cron_runs_skipped: AtomicU64::new(0),
                memory_items_ingested: AtomicU64::new(0),
                memory_items_rejected: AtomicU64::new(0),
                memory_search_requests: AtomicU64::new(0),
                memory_search_cache_hits: AtomicU64::new(0),
                memory_auto_inject_events: AtomicU64::new(0),
                vault_put_requests: AtomicU64::new(0),
                vault_get_requests: AtomicU64::new(0),
                vault_delete_requests: AtomicU64::new(0),
                vault_list_requests: AtomicU64::new(0),
                vault_rate_limited_requests: AtomicU64::new(0),
                vault_access_audit_events: AtomicU64::new(0),
                skill_status_updates: AtomicU64::new(0),
                skill_execution_denied: AtomicU64::new(0),
                approvals_tool_requested: AtomicU64::new(0),
                approvals_tool_resolved_allow: AtomicU64::new(0),
                approvals_tool_resolved_deny: AtomicU64::new(0),
                approvals_tool_resolved_timeout: AtomicU64::new(0),
                approvals_tool_resolved_error: AtomicU64::new(0),
                agent_mutations: AtomicU64::new(0),
                agent_resolution_hits: AtomicU64::new(0),
                agent_resolution_misses: AtomicU64::new(0),
                agent_validation_failures: AtomicU64::new(0),
                channel_messages_inbound: AtomicU64::new(0),
                channel_messages_routed: AtomicU64::new(0),
                channel_messages_replied: AtomicU64::new(0),
                channel_messages_rejected: AtomicU64::new(0),
                channel_messages_queued: AtomicU64::new(0),
                channel_messages_quarantined: AtomicU64::new(0),
                channel_router_queue_depth: AtomicU64::new(0),
                channel_reply_failures: AtomicU64::new(0),
                canvas_created: AtomicU64::new(0),
                canvas_updated: AtomicU64::new(0),
                canvas_closed: AtomicU64::new(0),
                canvas_denied: AtomicU64::new(0),
            },
            journal_store,
            revoked_certificate_count,
            model_provider,
            vault,
            memory_config: RwLock::new(MemoryRuntimeConfig::default()),
            memory_search_cache: Mutex::new(HashMap::new()),
            http_fetch_cache: Mutex::new(HashMap::new()),
            tool_approval_cache: Mutex::new(HashMap::new()),
            vault_rate_limit: Mutex::new(HashMap::new()),
            canvas_records: Mutex::new(recovered_canvas_records),
            canvas_signing_secret: generate_canvas_signing_secret(),
            agent_registry,
            channel_router,
        }))
    }

    pub fn record_denied(&self) {
        self.counters.denied_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_admin_status_request(&self) {
        self.counters.admin_status_requests.fetch_add(1, Ordering::Relaxed);
    }

    #[allow(clippy::result_large_err)]
    fn ensure_canvas_host_enabled(&self) -> Result<(), Status> {
        if self.config.canvas_host.enabled {
            Ok(())
        } else {
            Err(Status::failed_precondition("canvas host is disabled (canvas_host.enabled=false)"))
        }
    }

    #[allow(clippy::result_large_err)]
    #[allow(clippy::too_many_arguments)]
    fn create_canvas(
        &self,
        context: &RequestContext,
        requested_canvas_id: Option<String>,
        session_id: String,
        initial_state_json: &[u8],
        initial_state_version: u64,
        requested_state_schema_version: Option<u64>,
        bundle: gateway_v1::CanvasBundle,
        allowed_parent_origins: Vec<String>,
        requested_token_ttl_seconds: Option<u32>,
    ) -> Result<(CanvasRecord, CanvasRuntimeDescriptor), Status> {
        self.ensure_canvas_host_enabled()?;
        if initial_state_json.len() > self.config.canvas_host.max_state_bytes {
            return Err(Status::resource_exhausted(format!(
                "canvas state payload exceeds limit ({} > {})",
                initial_state_json.len(),
                self.config.canvas_host.max_state_bytes
            )));
        }
        let validated_initial_state =
            serde_json::from_slice::<Value>(initial_state_json).map_err(|error| {
                Status::invalid_argument(format!("initial_state_json must be valid JSON: {error}"))
            })?;
        let state_schema_version = resolve_canvas_state_schema_version(
            requested_state_schema_version,
            &validated_initial_state,
            None,
        )?;
        let canonical_initial_state_json =
            serde_json::to_vec(&validated_initial_state).map_err(|error| {
                Status::internal(format!("failed to encode initial state JSON: {error}"))
            })?;
        let initial_patch = build_replace_root_patch(&validated_initial_state);
        let initial_patch_json = patch_document_to_bytes(&initial_patch).map_err(|error| {
            Status::internal(format!("failed to encode initial canvas patch payload: {error}"))
        })?;
        let now_unix_ms = unix_ms_now_for_status()?;
        let canvas_id = match requested_canvas_id {
            Some(value) => normalize_canvas_identifier(value.as_str(), "canvas_id")?,
            None => Ulid::new().to_string(),
        };
        let state_version = if initial_state_version == 0 { 1 } else { initial_state_version };
        let allowed_parent_origins =
            parse_canvas_allowed_parent_origins(allowed_parent_origins.as_slice())?;
        let mut bundle = self.parse_canvas_bundle(bundle)?;
        let token_ttl_ms =
            self.resolve_canvas_token_ttl_ms(requested_token_ttl_seconds.unwrap_or_default())?;
        let expires_at_unix_ms = now_unix_ms.saturating_add(token_ttl_ms as i64);
        bundle.signature = self.sign_canvas_bundle(
            canvas_id.as_str(),
            bundle.sha256.as_str(),
            context.principal.as_str(),
            session_id.as_str(),
        );

        let mut records = self
            .canvas_records
            .lock()
            .map_err(|_| Status::internal("canvas registry lock poisoned"))?;
        if records.contains_key(canvas_id.as_str()) {
            return Err(Status::already_exists(format!("canvas already exists: {canvas_id}")));
        }

        let record = CanvasRecord {
            canvas_id: canvas_id.clone(),
            session_id: session_id.clone(),
            principal: context.principal.clone(),
            state_version,
            state_schema_version,
            state_json: canonical_initial_state_json.clone(),
            bundle,
            allowed_parent_origins,
            created_at_unix_ms: now_unix_ms,
            updated_at_unix_ms: now_unix_ms,
            expires_at_unix_ms,
            closed: false,
            close_reason: None,
            update_timestamps_unix_ms: VecDeque::new(),
        };
        let transition = CanvasStateTransitionRequest {
            canvas_id: record.canvas_id.clone(),
            session_id: record.session_id.clone(),
            principal: record.principal.clone(),
            state_version: record.state_version,
            base_state_version: 0,
            state_schema_version: record.state_schema_version,
            state_json: String::from_utf8(record.state_json.clone()).map_err(|error| {
                Status::internal(format!("failed to encode initial state JSON as UTF-8: {error}"))
            })?,
            patch_json: String::from_utf8(initial_patch_json).map_err(|error| {
                Status::internal(format!("failed to encode initial patch JSON as UTF-8: {error}"))
            })?,
            bundle_json: serde_json::to_string(&record.bundle).map_err(|error| {
                Status::internal(format!("failed to encode canvas bundle for persistence: {error}"))
            })?,
            allowed_parent_origins_json: serde_json::to_string(&record.allowed_parent_origins)
                .map_err(|error| {
                    Status::internal(format!(
                        "failed to encode canvas origin allowlist for persistence: {error}"
                    ))
                })?,
            created_at_unix_ms: record.created_at_unix_ms,
            updated_at_unix_ms: record.updated_at_unix_ms,
            expires_at_unix_ms: record.expires_at_unix_ms,
            closed: record.closed,
            close_reason: record.close_reason.clone(),
            actor_principal: context.principal.clone(),
            actor_device_id: context.device_id.clone(),
        };
        self.journal_store
            .record_canvas_state_transition(&transition)
            .map_err(|error| map_canvas_store_error("record_canvas_state_transition", error))?;
        records.insert(canvas_id.clone(), record.clone());
        self.counters.canvas_created.fetch_add(1, Ordering::Relaxed);

        let auth_token = self.issue_canvas_token(
            canvas_id.as_str(),
            context.principal.as_str(),
            session_id.as_str(),
            now_unix_ms,
            expires_at_unix_ms,
        )?;
        let descriptor = CanvasRuntimeDescriptor {
            canvas_id: canvas_id.clone(),
            frame_url: format!(
                "{}/canvas/v1/frame/{}",
                self.config.canvas_host.public_base_url, canvas_id
            ),
            runtime_url: format!(
                "{}/canvas/v1/runtime.js",
                self.config.canvas_host.public_base_url
            ),
            auth_token,
            expires_at_unix_ms,
        };
        Ok((record, descriptor))
    }

    #[allow(clippy::result_large_err)]
    fn update_canvas_state(
        &self,
        context: &RequestContext,
        canvas_id: &str,
        state_json: Option<&[u8]>,
        patch_json: Option<&[u8]>,
        expected_state_version: Option<u64>,
        expected_state_schema_version: Option<u64>,
    ) -> Result<CanvasRecord, Status> {
        self.ensure_canvas_host_enabled()?;
        let has_state_payload = state_json.is_some_and(|payload| !payload.is_empty());
        let has_patch_payload = patch_json.is_some_and(|payload| !payload.is_empty());
        if !has_state_payload && !has_patch_payload {
            return Err(Status::invalid_argument(
                "canvas update requires non-empty state_json or patch_json payload",
            ));
        }
        if has_state_payload && has_patch_payload {
            return Err(Status::invalid_argument(
                "canvas update accepts either state_json or patch_json, not both",
            ));
        }
        if let Some(payload) = state_json {
            if payload.len() > self.config.canvas_host.max_state_bytes {
                return Err(Status::resource_exhausted(format!(
                    "canvas state payload exceeds limit ({} > {})",
                    payload.len(),
                    self.config.canvas_host.max_state_bytes
                )));
            }
        }
        if let Some(payload) = patch_json {
            if payload.len() > self.config.canvas_host.max_state_bytes {
                return Err(Status::resource_exhausted(format!(
                    "canvas patch payload exceeds limit ({} > {})",
                    payload.len(),
                    self.config.canvas_host.max_state_bytes
                )));
            }
        }
        let normalized_canvas_id = normalize_canvas_identifier(canvas_id, "canvas_id")?;
        let now_unix_ms = unix_ms_now_for_status()?;
        let mut records = self
            .canvas_records
            .lock()
            .map_err(|_| Status::internal("canvas registry lock poisoned"))?;
        let Some(record) = records.get_mut(normalized_canvas_id.as_str()) else {
            return Err(Status::not_found(format!("canvas not found: {normalized_canvas_id}")));
        };
        if record.principal != context.principal {
            self.counters.canvas_denied.fetch_add(1, Ordering::Relaxed);
            return Err(Status::permission_denied("canvas access denied: principal mismatch"));
        }
        if record.closed {
            return Err(Status::failed_precondition("canvas is closed and cannot be updated"));
        }
        if record.expires_at_unix_ms <= now_unix_ms {
            self.counters.canvas_denied.fetch_add(1, Ordering::Relaxed);
            return Err(Status::permission_denied("canvas token/session expired"));
        }
        if let Some(expected_state_version) = expected_state_version {
            if expected_state_version > 0 && record.state_version != expected_state_version {
                return Err(Status::failed_precondition(format!(
                    "canvas version mismatch (expected {}, current {})",
                    expected_state_version, record.state_version
                )));
            }
        }
        if let Some(expected_state_schema_version) = expected_state_schema_version {
            if expected_state_schema_version > 0
                && record.state_schema_version != expected_state_schema_version
            {
                return Err(Status::failed_precondition(format!(
                    "canvas schema mismatch (expected {}, current {})",
                    expected_state_schema_version, record.state_schema_version
                )));
            }
        }

        let current_state: Value =
            serde_json::from_slice(record.state_json.as_slice()).map_err(|error| {
                Status::internal(format!("persisted canvas state JSON is invalid: {error}"))
            })?;
        let (next_state, patch_document) = if let Some(payload) = state_json {
            let next_state = serde_json::from_slice::<Value>(payload).map_err(|error| {
                Status::invalid_argument(format!("state_json must be valid JSON: {error}"))
            })?;
            (next_state.clone(), build_replace_root_patch(&next_state))
        } else {
            let payload = patch_json.ok_or_else(|| {
                Status::invalid_argument("canvas patch update requires non-empty patch_json")
            })?;
            let patch_document = parse_patch_document(payload).map_err(|error| {
                Status::invalid_argument(format!("patch_json is invalid: {error}"))
            })?;
            let next_state =
                apply_patch_document(&current_state, &patch_document).map_err(|error| {
                    Status::failed_precondition(format!("patch application failed: {error}"))
                })?;
            (next_state, patch_document)
        };
        let next_state_schema_version = resolve_canvas_state_schema_version(
            None,
            &next_state,
            Some(record.state_schema_version),
        )?;
        let canonical_next_state_json = serde_json::to_vec(&next_state).map_err(|error| {
            Status::internal(format!("failed to encode next state JSON: {error}"))
        })?;
        if canonical_next_state_json.len() > self.config.canvas_host.max_state_bytes {
            return Err(Status::resource_exhausted(format!(
                "canvas state payload exceeds limit after patch apply ({} > {})",
                canonical_next_state_json.len(),
                self.config.canvas_host.max_state_bytes
            )));
        }
        let canonical_patch_json = patch_document_to_bytes(&patch_document)
            .map_err(|error| Status::internal(format!("failed to encode patch JSON: {error}")))?;
        let next_state_version = record.state_version.saturating_add(1);

        while record
            .update_timestamps_unix_ms
            .front()
            .is_some_and(|value| now_unix_ms.saturating_sub(*value) > 60_000)
        {
            let _ = record.update_timestamps_unix_ms.pop_front();
        }
        if record.update_timestamps_unix_ms.len() >= self.config.canvas_host.max_updates_per_minute
        {
            return Err(Status::resource_exhausted(format!(
                "canvas update rate limit exceeded (>{} updates/minute)",
                self.config.canvas_host.max_updates_per_minute
            )));
        }
        let transition = CanvasStateTransitionRequest {
            canvas_id: record.canvas_id.clone(),
            session_id: record.session_id.clone(),
            principal: record.principal.clone(),
            state_version: next_state_version,
            base_state_version: record.state_version,
            state_schema_version: next_state_schema_version,
            state_json: String::from_utf8(canonical_next_state_json.clone()).map_err(|error| {
                Status::internal(format!("failed to encode state JSON as UTF-8: {error}"))
            })?,
            patch_json: String::from_utf8(canonical_patch_json).map_err(|error| {
                Status::internal(format!("failed to encode patch JSON as UTF-8: {error}"))
            })?,
            bundle_json: serde_json::to_string(&record.bundle).map_err(|error| {
                Status::internal(format!("failed to encode canvas bundle for persistence: {error}"))
            })?,
            allowed_parent_origins_json: serde_json::to_string(&record.allowed_parent_origins)
                .map_err(|error| {
                    Status::internal(format!(
                        "failed to encode canvas origin allowlist for persistence: {error}"
                    ))
                })?,
            created_at_unix_ms: record.created_at_unix_ms,
            updated_at_unix_ms: now_unix_ms,
            expires_at_unix_ms: record.expires_at_unix_ms,
            closed: record.closed,
            close_reason: record.close_reason.clone(),
            actor_principal: context.principal.clone(),
            actor_device_id: context.device_id.clone(),
        };
        self.journal_store
            .record_canvas_state_transition(&transition)
            .map_err(|error| map_canvas_store_error("record_canvas_state_transition", error))?;

        record.update_timestamps_unix_ms.push_back(now_unix_ms);
        record.state_version = next_state_version;
        record.state_schema_version = next_state_schema_version;
        record.state_json = canonical_next_state_json;
        record.updated_at_unix_ms = now_unix_ms;
        self.counters.canvas_updated.fetch_add(1, Ordering::Relaxed);
        Ok(record.clone())
    }

    #[allow(clippy::result_large_err)]
    fn close_canvas(
        &self,
        context: &RequestContext,
        canvas_id: &str,
        reason: Option<String>,
    ) -> Result<CanvasRecord, Status> {
        self.ensure_canvas_host_enabled()?;
        let normalized_canvas_id = normalize_canvas_identifier(canvas_id, "canvas_id")?;
        let now_unix_ms = unix_ms_now_for_status()?;
        let mut records = self
            .canvas_records
            .lock()
            .map_err(|_| Status::internal("canvas registry lock poisoned"))?;
        let Some(record) = records.get_mut(normalized_canvas_id.as_str()) else {
            return Err(Status::not_found(format!("canvas not found: {normalized_canvas_id}")));
        };
        if record.principal != context.principal {
            self.counters.canvas_denied.fetch_add(1, Ordering::Relaxed);
            return Err(Status::permission_denied("canvas access denied: principal mismatch"));
        }
        if !record.closed {
            let resolved_reason =
                reason.and_then(non_empty).or_else(|| Some("closed_by_operator".to_owned()));
            let current_state: Value = serde_json::from_slice(record.state_json.as_slice())
                .map_err(|error| {
                    Status::internal(format!("persisted canvas state JSON is invalid: {error}"))
                })?;
            let close_patch = build_replace_root_patch(&current_state);
            let close_patch_json = patch_document_to_bytes(&close_patch).map_err(|error| {
                Status::internal(format!("failed to encode close patch: {error}"))
            })?;
            let next_state_version = record.state_version.saturating_add(1);
            let transition = CanvasStateTransitionRequest {
                canvas_id: record.canvas_id.clone(),
                session_id: record.session_id.clone(),
                principal: record.principal.clone(),
                state_version: next_state_version,
                base_state_version: record.state_version,
                state_schema_version: record.state_schema_version,
                state_json: String::from_utf8(record.state_json.clone()).map_err(|error| {
                    Status::internal(format!("failed to encode close state as UTF-8: {error}"))
                })?,
                patch_json: String::from_utf8(close_patch_json).map_err(|error| {
                    Status::internal(format!("failed to encode close patch as UTF-8: {error}"))
                })?,
                bundle_json: serde_json::to_string(&record.bundle).map_err(|error| {
                    Status::internal(format!(
                        "failed to encode canvas bundle for persistence: {error}"
                    ))
                })?,
                allowed_parent_origins_json: serde_json::to_string(&record.allowed_parent_origins)
                    .map_err(|error| {
                        Status::internal(format!(
                            "failed to encode canvas origin allowlist for persistence: {error}"
                        ))
                    })?,
                created_at_unix_ms: record.created_at_unix_ms,
                updated_at_unix_ms: now_unix_ms,
                expires_at_unix_ms: record.expires_at_unix_ms,
                closed: true,
                close_reason: resolved_reason.clone(),
                actor_principal: context.principal.clone(),
                actor_device_id: context.device_id.clone(),
            };
            self.journal_store
                .record_canvas_state_transition(&transition)
                .map_err(|error| map_canvas_store_error("record_canvas_state_transition", error))?;
            record.state_version = next_state_version;
            record.close_reason = resolved_reason;
            record.closed = true;
            record.updated_at_unix_ms = now_unix_ms;
            self.counters.canvas_closed.fetch_add(1, Ordering::Relaxed);
        }
        Ok(record.clone())
    }

    #[allow(clippy::result_large_err)]
    fn get_canvas(
        &self,
        context: &RequestContext,
        canvas_id: &str,
    ) -> Result<CanvasRecord, Status> {
        self.ensure_canvas_host_enabled()?;
        let normalized_canvas_id = normalize_canvas_identifier(canvas_id, "canvas_id")?;
        let records = self
            .canvas_records
            .lock()
            .map_err(|_| Status::internal("canvas registry lock poisoned"))?;
        let Some(record) = records.get(normalized_canvas_id.as_str()) else {
            return Err(Status::not_found(format!("canvas not found: {normalized_canvas_id}")));
        };
        if record.principal != context.principal {
            self.counters.canvas_denied.fetch_add(1, Ordering::Relaxed);
            return Err(Status::permission_denied("canvas access denied: principal mismatch"));
        }
        Ok(record.clone())
    }

    #[allow(clippy::result_large_err)]
    fn list_canvas_state_patches(
        &self,
        context: &RequestContext,
        canvas_id: &str,
        after_state_version: u64,
        limit: usize,
    ) -> Result<Vec<CanvasStatePatchRecord>, Status> {
        let record = self.get_canvas(context, canvas_id)?;
        let limited = limit.clamp(1, MAX_CANVAS_STREAM_PATCH_BATCH);
        self.journal_store
            .list_canvas_state_patches(record.canvas_id.as_str(), after_state_version, limited)
            .map_err(|error| map_canvas_store_error("list_canvas_state_patches", error))
    }

    #[allow(clippy::result_large_err)]
    pub fn canvas_frame_document(
        &self,
        canvas_id: &str,
        token: &str,
    ) -> Result<CanvasFrameDocument, Status> {
        let record = self.authorize_canvas_http_request(canvas_id, token)?;
        let csp = build_canvas_csp_header(record.allowed_parent_origins.as_slice());
        let encoded_canvas_id = url_encode_component(record.canvas_id.as_str());
        let encoded_entrypoint = url_encode_path_component(record.bundle.entrypoint_path.as_str());
        let encoded_token = url_encode_component(token);
        let mut origins_meta = String::new();
        for origin in record.allowed_parent_origins.iter() {
            origins_meta.push_str("<meta name=\"palyra-canvas-origin\" content=\"");
            origins_meta.push_str(escape_html_attribute(origin).as_str());
            origins_meta.push_str("\" />\n");
        }
        let html = format!(
            concat!(
                "<!doctype html>\n",
                "<html lang=\"en\">\n",
                "<head>\n",
                "<meta charset=\"utf-8\" />\n",
                "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />\n",
                "<title>Palyra Canvas</title>\n",
                "{origins_meta}",
                "<link rel=\"stylesheet\" href=\"/canvas/v1/runtime.css?canvas_id={canvas_id}&token={token}\" />\n",
                "</head>\n",
                "<body>\n",
                "<main id=\"palyra-canvas-root\" data-canvas-id=\"{canvas_id}\"></main>\n",
                "<pre id=\"palyra-canvas-state\" hidden></pre>\n",
                "<script src=\"/canvas/v1/runtime.js?canvas_id={canvas_id}&token={token}\" defer></script>\n",
                "<script src=\"/canvas/v1/bundle/{canvas_id}/{entrypoint}?token={token}\" defer></script>\n",
                "</body>\n",
                "</html>\n"
            ),
            origins_meta = origins_meta,
            canvas_id = encoded_canvas_id,
            entrypoint = encoded_entrypoint,
            token = encoded_token
        );
        Ok(CanvasFrameDocument {
            canvas_id: record.canvas_id,
            html,
            csp,
            expires_at_unix_ms: record.expires_at_unix_ms,
        })
    }

    #[allow(clippy::result_large_err)]
    pub fn canvas_runtime_script(
        &self,
        canvas_id: &str,
        token: &str,
    ) -> Result<CanvasAssetResponse, Status> {
        let record = self.authorize_canvas_http_request(canvas_id, token)?;
        let script = format!(
            concat!(
                "(function () {{\n",
                "  'use strict';\n",
                "  const root = document.getElementById('palyra-canvas-root');\n",
                "  const statePreview = document.getElementById('palyra-canvas-state');\n",
                "  const params = new URLSearchParams(window.location.search);\n",
                "  const canvasId = params.get('canvas_id') || {canvas_id_json};\n",
                "  const token = params.get('token') || '';\n",
                "  const allowedOrigins = new Set(Array.from(document.querySelectorAll('meta[name=\"palyra-canvas-origin\"]')).map((node) => node.content));\n",
                "  let stateVersion = 0;\n",
                "  function renderState(state) {{\n",
                "    if (statePreview) {{\n",
                "      statePreview.hidden = false;\n",
                "      statePreview.textContent = JSON.stringify(state, null, 2);\n",
                "    }}\n",
                "    window.dispatchEvent(new CustomEvent('palyra:canvas-state', {{ detail: state }}));\n",
                "  }}\n",
                "  async function pollState() {{\n",
                "    if (!canvasId || !token) return;\n",
                "    const url = new URL('/canvas/v1/state/' + encodeURIComponent(canvasId), window.location.origin);\n",
                "    url.searchParams.set('token', token);\n",
                "    url.searchParams.set('after_version', String(stateVersion));\n",
                "    const response = await fetch(url.toString(), {{ method: 'GET', cache: 'no-store', credentials: 'omit' }});\n",
                "    if (response.status === 204) return;\n",
                "    if (!response.ok) return;\n",
                "    const payload = await response.json();\n",
                "    if (typeof payload.state_version !== 'number') return;\n",
                "    if (payload.state_version <= stateVersion) return;\n",
                "    stateVersion = payload.state_version;\n",
                "    renderState(payload.state);\n",
                "  }}\n",
                "  window.addEventListener('message', (event) => {{\n",
                "    if (!allowedOrigins.has(event.origin)) return;\n",
                "    const message = event.data;\n",
                "    if (!message || typeof message !== 'object') return;\n",
                "    if (message.type !== 'palyra.canvas.state') return;\n",
                "    if (message.token !== token) return;\n",
                "    if (typeof message.version !== 'number' || message.version <= stateVersion) return;\n",
                "    stateVersion = message.version;\n",
                "    renderState(message.state);\n",
                "  }});\n",
                "  if (window.parent && window.parent !== window) {{\n",
                "    for (const origin of allowedOrigins) {{\n",
                "      window.parent.postMessage({{ type: 'palyra.canvas.ready', canvas_id: canvasId }}, origin);\n",
                "    }}\n",
                "  }}\n",
                "  setInterval(() => {{ void pollState(); }}, 750);\n",
                "  void pollState();\n",
                "  if (root) {{\n",
                "    root.setAttribute('data-canvas-ready', 'true');\n",
                "  }}\n",
                "}})();\n"
            ),
            canvas_id_json = serde_json::to_string(&record.canvas_id).map_err(|error| {
                Status::internal(format!("failed to encode canvas runtime identifier: {error}"))
            })?
        );
        Ok(CanvasAssetResponse {
            content_type: "application/javascript; charset=utf-8".to_owned(),
            body: script.into_bytes(),
            csp: build_canvas_csp_header(record.allowed_parent_origins.as_slice()),
        })
    }

    #[allow(clippy::result_large_err)]
    pub fn canvas_runtime_stylesheet(
        &self,
        canvas_id: &str,
        token: &str,
    ) -> Result<CanvasAssetResponse, Status> {
        let record = self.authorize_canvas_http_request(canvas_id, token)?;
        let stylesheet = concat!(
            ":root { color-scheme: light; font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }\n",
            "html, body { margin: 0; padding: 0; background: #f5f7fb; color: #111827; }\n",
            "#palyra-canvas-root { min-height: 2rem; }\n",
            "#palyra-canvas-state { margin: 0; padding: 1rem; white-space: pre-wrap; word-break: break-word; }\n"
        );
        Ok(CanvasAssetResponse {
            content_type: "text/css; charset=utf-8".to_owned(),
            body: stylesheet.as_bytes().to_vec(),
            csp: build_canvas_csp_header(record.allowed_parent_origins.as_slice()),
        })
    }

    #[allow(clippy::result_large_err)]
    pub fn canvas_bundle_asset(
        &self,
        canvas_id: &str,
        asset_path: &str,
        token: &str,
    ) -> Result<CanvasAssetResponse, Status> {
        let record = self.authorize_canvas_http_request(canvas_id, token)?;
        let normalized_asset_path = normalize_canvas_asset_path(asset_path, "asset_path")?;
        let Some(asset) = record.bundle.assets.get(normalized_asset_path.as_str()) else {
            return Err(Status::not_found(format!(
                "canvas asset not found: {}",
                normalized_asset_path
            )));
        };
        Ok(CanvasAssetResponse {
            content_type: asset.content_type.clone(),
            body: asset.body.clone(),
            csp: build_canvas_csp_header(record.allowed_parent_origins.as_slice()),
        })
    }

    #[allow(clippy::result_large_err)]
    pub fn canvas_state(
        &self,
        canvas_id: &str,
        token: &str,
        after_version: Option<u64>,
    ) -> Result<Option<CanvasStateResponse>, Status> {
        let record = self.authorize_canvas_http_request(canvas_id, token)?;
        if after_version.is_some_and(|value| value >= record.state_version) {
            return Ok(None);
        }
        let state = serde_json::from_slice::<Value>(&record.state_json).map_err(|error| {
            Status::internal(format!("persisted canvas state JSON is invalid: {error}"))
        })?;
        Ok(Some(CanvasStateResponse {
            canvas_id: record.canvas_id,
            state_version: record.state_version,
            state_schema_version: record.state_schema_version,
            state,
            closed: record.closed,
            close_reason: record.close_reason,
            expires_at_unix_ms: record.expires_at_unix_ms,
        }))
    }

    #[allow(clippy::result_large_err)]
    fn authorize_canvas_http_request(
        &self,
        canvas_id: &str,
        token: &str,
    ) -> Result<CanvasRecord, Status> {
        self.ensure_canvas_host_enabled()?;
        let normalized_canvas_id = normalize_canvas_identifier(canvas_id, "canvas_id")?;
        let token_payload = self.verify_canvas_token(token)?;
        if token_payload.canvas_id != normalized_canvas_id {
            self.counters.canvas_denied.fetch_add(1, Ordering::Relaxed);
            return Err(Status::permission_denied("canvas token does not match canvas id"));
        }
        let now_unix_ms = unix_ms_now_for_status()?;
        if token_payload.expires_at_unix_ms <= now_unix_ms {
            self.counters.canvas_denied.fetch_add(1, Ordering::Relaxed);
            return Err(Status::permission_denied("canvas token expired"));
        }

        let records = self
            .canvas_records
            .lock()
            .map_err(|_| Status::internal("canvas registry lock poisoned"))?;
        let Some(record) = records.get(normalized_canvas_id.as_str()) else {
            return Err(Status::not_found(format!("canvas not found: {normalized_canvas_id}")));
        };
        if record.expires_at_unix_ms <= now_unix_ms {
            self.counters.canvas_denied.fetch_add(1, Ordering::Relaxed);
            return Err(Status::permission_denied("canvas session expired"));
        }
        if record.principal != token_payload.principal
            || record.session_id != token_payload.session_id
        {
            self.counters.canvas_denied.fetch_add(1, Ordering::Relaxed);
            return Err(Status::permission_denied("canvas token scope mismatch"));
        }
        Ok(record.clone())
    }

    #[allow(clippy::result_large_err)]
    fn parse_canvas_bundle(
        &self,
        bundle: gateway_v1::CanvasBundle,
    ) -> Result<CanvasBundleRecord, Status> {
        let bundle_id = normalize_canvas_bundle_identifier(bundle.bundle_id.as_str())?;
        let entrypoint_path =
            normalize_canvas_asset_path(bundle.entrypoint_path.as_str(), "bundle.entrypoint_path")?;
        if bundle.assets.is_empty() {
            return Err(Status::invalid_argument("bundle.assets must include at least one asset"));
        }
        if bundle.assets.len() > self.config.canvas_host.max_assets_per_bundle {
            return Err(Status::resource_exhausted(format!(
                "bundle.assets exceeds limit ({} > {})",
                bundle.assets.len(),
                self.config.canvas_host.max_assets_per_bundle
            )));
        }
        let mut assets = HashMap::new();
        let mut total_bytes = 0usize;
        for (index, asset) in bundle.assets.iter().enumerate() {
            let source = format!("bundle.assets[{index}]");
            let normalized_path =
                normalize_canvas_asset_path(asset.path.as_str(), source.as_str())?;
            if assets.contains_key(normalized_path.as_str()) {
                return Err(Status::invalid_argument(format!(
                    "{source}.path duplicates asset path '{normalized_path}'"
                )));
            }
            let content_type =
                normalize_canvas_asset_content_type(asset.content_type.as_str(), source.as_str())?;
            total_bytes = total_bytes.saturating_add(asset.body.len());
            if total_bytes > self.config.canvas_host.max_bundle_bytes {
                return Err(Status::resource_exhausted(format!(
                    "bundle byte size exceeds limit ({} > {})",
                    total_bytes, self.config.canvas_host.max_bundle_bytes
                )));
            }
            assets.insert(
                normalized_path,
                CanvasAssetRecord { content_type, body: asset.body.clone() },
            );
        }
        let Some(entrypoint_asset) = assets.get(entrypoint_path.as_str()) else {
            return Err(Status::invalid_argument(
                "bundle.entrypoint_path must reference an existing asset",
            ));
        };
        if !is_canvas_javascript_content_type(entrypoint_asset.content_type.as_str()) {
            return Err(Status::failed_precondition(
                "bundle.entrypoint_path asset must use javascript content type",
            ));
        }
        let sha256 = compute_canvas_bundle_sha256(&assets);
        Ok(CanvasBundleRecord {
            bundle_id,
            entrypoint_path,
            assets,
            sha256,
            signature: String::new(),
        })
    }

    #[allow(clippy::result_large_err)]
    fn resolve_canvas_token_ttl_ms(&self, requested_ttl_seconds: u32) -> Result<u64, Status> {
        let requested_ttl_ms = if requested_ttl_seconds == 0 {
            self.config.canvas_host.token_ttl_ms
        } else {
            u64::from(requested_ttl_seconds).saturating_mul(1_000)
        };
        let bounded = requested_ttl_ms.clamp(MIN_CANVAS_TOKEN_TTL_MS, MAX_CANVAS_TOKEN_TTL_MS);
        if bounded == 0 {
            return Err(Status::invalid_argument("canvas auth token ttl must be positive"));
        }
        Ok(bounded)
    }

    fn sign_canvas_bundle(
        &self,
        canvas_id: &str,
        bundle_sha256: &str,
        principal: &str,
        session_id: &str,
    ) -> String {
        let mut hasher = Sha256::new();
        hasher.update(self.canvas_signing_secret);
        hasher.update(b"\n");
        hasher.update(canvas_id.as_bytes());
        hasher.update(b"\n");
        hasher.update(bundle_sha256.as_bytes());
        hasher.update(b"\n");
        hasher.update(principal.as_bytes());
        hasher.update(b"\n");
        hasher.update(session_id.as_bytes());
        URL_SAFE_NO_PAD.encode(hasher.finalize())
    }

    #[allow(clippy::result_large_err)]
    fn issue_canvas_token(
        &self,
        canvas_id: &str,
        principal: &str,
        session_id: &str,
        issued_at_unix_ms: i64,
        expires_at_unix_ms: i64,
    ) -> Result<String, Status> {
        let payload = CanvasTokenPayload {
            canvas_id: canvas_id.to_owned(),
            principal: principal.to_owned(),
            session_id: session_id.to_owned(),
            issued_at_unix_ms,
            expires_at_unix_ms,
            nonce: Ulid::new().to_string(),
        };
        let payload_json = serde_json::to_vec(&payload).map_err(|error| {
            Status::internal(format!("failed to serialize canvas token payload: {error}"))
        })?;
        let payload_b64 = URL_SAFE_NO_PAD.encode(payload_json);
        let mut hasher = Sha256::new();
        hasher.update(self.canvas_signing_secret);
        hasher.update(b".");
        hasher.update(payload_b64.as_bytes());
        let signature = URL_SAFE_NO_PAD.encode(hasher.finalize());
        Ok(format!("{payload_b64}.{signature}"))
    }

    #[allow(clippy::result_large_err)]
    fn verify_canvas_token(&self, token: &str) -> Result<CanvasTokenPayload, Status> {
        if token.trim().is_empty() {
            return Err(Status::invalid_argument("canvas token is required"));
        }
        let Some((payload_b64, signature_b64)) = token.split_once('.') else {
            return Err(Status::invalid_argument("canvas token format is invalid"));
        };
        let mut hasher = Sha256::new();
        hasher.update(self.canvas_signing_secret);
        hasher.update(b".");
        hasher.update(payload_b64.as_bytes());
        let expected_signature = URL_SAFE_NO_PAD.encode(hasher.finalize());
        if !constant_time_eq(expected_signature.as_bytes(), signature_b64.as_bytes()) {
            return Err(Status::permission_denied("canvas token signature is invalid"));
        }
        let payload_json = URL_SAFE_NO_PAD.decode(payload_b64).map_err(|error| {
            Status::invalid_argument(format!("canvas token payload encoding is invalid: {error}"))
        })?;
        let payload =
            serde_json::from_slice::<CanvasTokenPayload>(&payload_json).map_err(|error| {
                Status::invalid_argument(format!("canvas token payload is invalid JSON: {error}"))
            })?;
        Ok(payload)
    }

    #[allow(clippy::result_large_err)]
    fn record_journal_event_blocking(
        &self,
        request: &JournalAppendRequest,
    ) -> Result<crate::journal::JournalAppendOutcome, Status> {
        let outcome = match self.journal_store.append(request) {
            Ok(outcome) => outcome,
            Err(JournalError::DuplicateEventId { event_id }) => {
                return Err(Status::already_exists(format!(
                    "journal event already exists: {event_id}"
                )));
            }
            Err(error) => {
                self.counters.journal_persist_failures.fetch_add(1, Ordering::Relaxed);
                return Err(Status::internal(format!(
                    "failed to persist journal event '{}': {error}",
                    request.event_id
                )));
            }
        };
        self.counters.journal_events.fetch_add(1, Ordering::Relaxed);
        if outcome.redacted {
            self.counters.journal_redacted_events.fetch_add(1, Ordering::Relaxed);
        }
        if outcome.write_duration.as_millis() > JOURNAL_WRITE_LATENCY_BUDGET_MS {
            warn!(
                event_id = %request.event_id,
                write_duration_ms = outcome.write_duration.as_millis(),
                budget_ms = JOURNAL_WRITE_LATENCY_BUDGET_MS,
                "journal write exceeded latency budget"
            );
        }
        Ok(outcome)
    }

    #[allow(clippy::result_large_err)]
    async fn record_journal_event(
        self: &Arc<Self>,
        request: JournalAppendRequest,
    ) -> Result<crate::journal::JournalAppendOutcome, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.record_journal_event_blocking(&request))
            .await
            .map_err(|_| Status::internal("journal write worker panicked"))?
    }

    fn consume_vault_rate_limit(&self, principal: &str) -> bool {
        let now = Instant::now();
        let mut buckets = match self.vault_rate_limit.lock() {
            Ok(guard) => guard,
            Err(_) => return false,
        };
        if !buckets.contains_key(principal)
            && buckets.len() >= VAULT_RATE_LIMIT_MAX_PRINCIPAL_BUCKETS
        {
            buckets.retain(|_, entry| {
                now.duration_since(entry.window_started_at).as_millis() as u64
                    <= VAULT_RATE_LIMIT_WINDOW_MS
            });
            if buckets.len() >= VAULT_RATE_LIMIT_MAX_PRINCIPAL_BUCKETS {
                let evicted = buckets
                    .iter()
                    .min_by(|(left_principal, left_entry), (right_principal, right_entry)| {
                        left_entry
                            .window_started_at
                            .cmp(&right_entry.window_started_at)
                            .then_with(|| left_principal.cmp(right_principal))
                    })
                    .map(|(oldest_principal, _)| oldest_principal.clone());
                let Some(oldest_principal) = evicted else {
                    return false;
                };
                buckets.remove(oldest_principal.as_str());
            }
        }
        let entry = buckets
            .entry(principal.to_owned())
            .or_insert(VaultRateLimitEntry { window_started_at: now, requests_in_window: 0 });
        if now.duration_since(entry.window_started_at).as_millis() as u64
            > VAULT_RATE_LIMIT_WINDOW_MS
        {
            entry.window_started_at = now;
            entry.requests_in_window = 0;
        }
        if entry.requests_in_window >= VAULT_RATE_LIMIT_MAX_REQUESTS_PER_WINDOW {
            return false;
        }
        entry.requests_in_window = entry.requests_in_window.saturating_add(1);
        true
    }

    #[allow(clippy::result_large_err)]
    async fn vault_put_secret(
        self: &Arc<Self>,
        scope: VaultScope,
        key: String,
        value: Vec<u8>,
    ) -> Result<VaultSecretMetadata, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.vault.put_secret(&scope, key.as_str(), value.as_slice())
        })
        .await
        .map_err(|_| Status::internal("vault write worker panicked"))?
        .map_err(|error| map_vault_error("put secret", error))
    }

    #[allow(clippy::result_large_err)]
    async fn vault_get_secret(
        self: &Arc<Self>,
        scope: VaultScope,
        key: String,
    ) -> Result<Vec<u8>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.vault.get_secret(&scope, key.as_str()))
            .await
            .map_err(|_| Status::internal("vault read worker panicked"))?
            .map_err(|error| map_vault_error("get secret", error))
    }

    #[allow(clippy::result_large_err)]
    async fn vault_delete_secret(
        self: &Arc<Self>,
        scope: VaultScope,
        key: String,
    ) -> Result<bool, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.vault.delete_secret(&scope, key.as_str()))
            .await
            .map_err(|_| Status::internal("vault delete worker panicked"))?
            .map_err(|error| map_vault_error("delete secret", error))
    }

    #[allow(clippy::result_large_err)]
    async fn vault_list_secrets(
        self: &Arc<Self>,
        scope: VaultScope,
    ) -> Result<Vec<VaultSecretMetadata>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.vault.list_secrets(&scope))
            .await
            .map_err(|_| Status::internal("vault list worker panicked"))?
            .map_err(|error| map_vault_error("list secrets", error))
    }

    pub fn status_snapshot(
        &self,
        context: RequestContext,
        auth_config: &GatewayAuthConfig,
    ) -> GatewayStatusSnapshot {
        let latest_event_hash = self.journal_store.latest_hash().ok().flatten();
        let agents_runtime = self
            .agent_registry
            .status_snapshot()
            .map(|snapshot| AgentRuntimeSnapshot {
                default_agent_id: snapshot.default_agent_id,
                agent_count: snapshot.agent_count,
                active_session_bindings: snapshot
                    .session_bindings
                    .into_iter()
                    .take(MAX_AGENT_STATUS_BINDINGS)
                    .map(|binding| AgentSessionBindingSnapshot {
                        session_id_redacted: redact_session_id(binding.session_id.as_str()),
                        agent_id: binding.agent_id,
                    })
                    .collect(),
            })
            .unwrap_or_else(|_| AgentRuntimeSnapshot {
                default_agent_id: None,
                agent_count: 0,
                active_session_bindings: Vec::new(),
            });
        self.counters
            .channel_router_queue_depth
            .store(self.channel_router.queue_depth() as u64, Ordering::Relaxed);
        GatewayStatusSnapshot {
            service: "palyrad",
            status: "ok",
            version: self.build.version.clone(),
            git_hash: self.build.git_hash.clone(),
            build_profile: self.build.build_profile.clone(),
            uptime_seconds: self.started_at.elapsed().as_secs(),
            transport: TransportSnapshot {
                grpc_bind_addr: self.config.grpc_bind_addr.clone(),
                grpc_port: self.config.grpc_port,
                quic_bind_addr: self.config.quic_bind_addr.clone(),
                quic_port: self.config.quic_port,
                quic_enabled: self.config.quic_enabled,
            },
            security: SecuritySnapshot {
                deny_by_default: true,
                admin_auth_required: self.config.admin_auth_required,
                admin_token_configured: auth_config.admin_token.is_some(),
                orchestrator_runloop_v1_enabled: self.config.orchestrator_runloop_v1_enabled,
                node_rpc_mtls_required: self.config.node_rpc_mtls_required,
                revoked_certificate_count: self.revoked_certificate_count,
            },
            storage: StorageSnapshot {
                journal_db_path: self.journal_config.db_path.to_string_lossy().into_owned(),
                journal_hash_chain_enabled: self.journal_config.hash_chain_enabled,
                latest_event_hash,
            },
            model_provider: self.model_provider.status_snapshot(),
            tool_call_policy: tool_policy_snapshot(&self.config.tool_call),
            counters: self.counters.snapshot(),
            agents: agents_runtime,
            request_context: context,
        }
    }

    #[allow(clippy::result_large_err)]
    pub async fn status_snapshot_async(
        self: &Arc<Self>,
        context: RequestContext,
        auth_config: GatewayAuthConfig,
    ) -> Result<GatewayStatusSnapshot, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.status_snapshot(context, &auth_config))
            .await
            .map_err(|_| Status::internal("status snapshot worker panicked"))
    }

    #[allow(clippy::result_large_err)]
    fn recent_journal_snapshot_blocking(
        &self,
        limit: usize,
    ) -> Result<JournalRecentSnapshot, Status> {
        let limit = limit.clamp(1, MAX_JOURNAL_RECENT_EVENTS);
        let events = self.journal_store.recent(limit).map_err(|error| {
            Status::internal(format!("failed to load recent journal events: {error}"))
        })?;
        let total_events =
            self.journal_store.total_events().map_err(|error| {
                Status::internal(format!("failed to count journal events: {error}"))
            })? as u64;
        Ok(JournalRecentSnapshot {
            total_events,
            hash_chain_enabled: self.journal_config.hash_chain_enabled,
            events,
        })
    }

    #[allow(clippy::result_large_err)]
    pub async fn recent_journal_snapshot(
        self: &Arc<Self>,
        limit: usize,
    ) -> Result<JournalRecentSnapshot, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.recent_journal_snapshot_blocking(limit))
            .await
            .map_err(|_| Status::internal("journal read worker panicked"))?
    }

    #[must_use]
    pub const fn is_orchestrator_runloop_enabled(&self) -> bool {
        self.config.orchestrator_runloop_v1_enabled
    }

    #[allow(clippy::result_large_err)]
    pub async fn execute_model_provider(
        self: &Arc<Self>,
        request: ProviderRequest,
    ) -> Result<crate::model_provider::ProviderResponse, Status> {
        self.counters.model_provider_requests.fetch_add(1, Ordering::Relaxed);
        match self.model_provider.complete(request).await {
            Ok(response) => {
                if response.retry_count > 0 {
                    self.counters
                        .model_provider_retry_attempts
                        .fetch_add(response.retry_count as u64, Ordering::Relaxed);
                }
                Ok(response)
            }
            Err(error) => {
                self.counters.model_provider_failures.fetch_add(1, Ordering::Relaxed);
                if error.retry_count() > 0 {
                    self.counters
                        .model_provider_retry_attempts
                        .fetch_add(error.retry_count() as u64, Ordering::Relaxed);
                }
                if error.is_circuit_open() {
                    self.counters
                        .model_provider_circuit_open_rejections
                        .fetch_add(1, Ordering::Relaxed);
                }
                Err(map_provider_error(error))
            }
        }
    }

    #[allow(clippy::result_large_err)]
    fn resolve_orchestrator_session_blocking(
        &self,
        request: &OrchestratorSessionResolveRequest,
    ) -> Result<OrchestratorSessionResolveOutcome, Status> {
        self.journal_store
            .resolve_orchestrator_session(request)
            .map_err(|error| map_orchestrator_store_error("resolve orchestrator session", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn resolve_orchestrator_session(
        self: &Arc<Self>,
        request: OrchestratorSessionResolveRequest,
    ) -> Result<OrchestratorSessionResolveOutcome, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.resolve_orchestrator_session_blocking(&request))
            .await
            .map_err(|_| Status::internal("orchestrator session resolve worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_orchestrator_sessions_blocking(
        &self,
        after_session_key: Option<String>,
        requested_limit: Option<usize>,
    ) -> Result<(Vec<OrchestratorSessionRecord>, Option<String>), Status> {
        let limit = requested_limit.unwrap_or(100).clamp(1, MAX_SESSIONS_PAGE_LIMIT);
        let mut sessions = self
            .journal_store
            .list_orchestrator_sessions(after_session_key.as_deref(), limit.saturating_add(1))
            .map_err(|error| map_orchestrator_store_error("list orchestrator sessions", error))?;
        let has_more = sessions.len() > limit;
        if has_more {
            sessions.truncate(limit);
        }
        let next_after_session_key = if has_more {
            sessions.last().map(|session| session.session_key.clone())
        } else {
            None
        };
        Ok((sessions, next_after_session_key))
    }

    #[allow(clippy::result_large_err)]
    pub async fn list_orchestrator_sessions(
        self: &Arc<Self>,
        after_session_key: Option<String>,
        requested_limit: Option<usize>,
    ) -> Result<(Vec<OrchestratorSessionRecord>, Option<String>), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.list_orchestrator_sessions_blocking(after_session_key, requested_limit)
        })
        .await
        .map_err(|_| Status::internal("orchestrator session list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_agents_blocking(
        &self,
        after_agent_id: Option<String>,
        requested_limit: Option<usize>,
    ) -> Result<AgentListPage, Status> {
        self.agent_registry
            .list_agents(after_agent_id.as_deref(), requested_limit.or(Some(MAX_AGENTS_PAGE_LIMIT)))
            .map_err(|error| map_agent_registry_error("list agents", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn list_agents(
        self: &Arc<Self>,
        after_agent_id: Option<String>,
        requested_limit: Option<usize>,
    ) -> Result<AgentListPage, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.list_agents_blocking(after_agent_id, requested_limit)
        })
        .await
        .map_err(|_| Status::internal("agent list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn get_agent_blocking(&self, agent_id: &str) -> Result<(AgentRecord, bool), Status> {
        self.agent_registry
            .get_agent(agent_id)
            .map_err(|error| map_agent_registry_error("get agent", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn get_agent(
        self: &Arc<Self>,
        agent_id: String,
    ) -> Result<(AgentRecord, bool), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.get_agent_blocking(agent_id.as_str()))
            .await
            .map_err(|_| Status::internal("agent get worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn create_agent_blocking(
        &self,
        request: &AgentCreateRequest,
    ) -> Result<AgentCreateOutcome, Status> {
        self.agent_registry
            .create_agent(request.clone())
            .map_err(|error| map_agent_registry_error("create agent", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn create_agent(
        self: &Arc<Self>,
        request: AgentCreateRequest,
    ) -> Result<AgentCreateOutcome, Status> {
        let state = Arc::clone(self);
        let result = tokio::task::spawn_blocking(move || state.create_agent_blocking(&request))
            .await
            .map_err(|_| Status::internal("agent create worker panicked"))?;
        if let Err(status) = &result {
            if status.code() == tonic::Code::InvalidArgument {
                self.counters.agent_validation_failures.fetch_add(1, Ordering::Relaxed);
            }
        } else {
            self.counters.agent_mutations.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    #[allow(clippy::result_large_err)]
    fn set_default_agent_blocking(&self, agent_id: &str) -> Result<AgentSetDefaultOutcome, Status> {
        self.agent_registry
            .set_default_agent(agent_id)
            .map_err(|error| map_agent_registry_error("set default agent", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn set_default_agent(
        self: &Arc<Self>,
        agent_id: String,
    ) -> Result<AgentSetDefaultOutcome, Status> {
        let state = Arc::clone(self);
        let result = tokio::task::spawn_blocking(move || {
            state.set_default_agent_blocking(agent_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("agent default worker panicked"))?;
        if let Err(status) = &result {
            if status.code() == tonic::Code::InvalidArgument {
                self.counters.agent_validation_failures.fetch_add(1, Ordering::Relaxed);
            }
        } else {
            self.counters.agent_mutations.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    #[allow(clippy::result_large_err)]
    fn resolve_agent_for_context_blocking(
        &self,
        request: &AgentResolveRequest,
    ) -> Result<AgentResolveOutcome, Status> {
        self.agent_registry
            .resolve_agent_for_context(request.clone())
            .map_err(|error| map_agent_registry_error("resolve agent for context", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn resolve_agent_for_context(
        self: &Arc<Self>,
        request: AgentResolveRequest,
    ) -> Result<AgentResolveOutcome, Status> {
        let state = Arc::clone(self);
        let result =
            tokio::task::spawn_blocking(move || state.resolve_agent_for_context_blocking(&request))
                .await
                .map_err(|_| Status::internal("agent resolve worker panicked"))?;
        match &result {
            Ok(outcome) => {
                if matches!(outcome.source, AgentResolutionSource::SessionBinding) {
                    self.counters.agent_resolution_hits.fetch_add(1, Ordering::Relaxed);
                } else {
                    self.counters.agent_resolution_misses.fetch_add(1, Ordering::Relaxed);
                }
                if outcome.binding_created {
                    self.counters.agent_mutations.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(status) => {
                if status.code() == tonic::Code::InvalidArgument {
                    self.counters.agent_validation_failures.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        result
    }

    #[allow(clippy::result_large_err)]
    fn start_orchestrator_run_blocking(
        &self,
        request: &OrchestratorRunStartRequest,
    ) -> Result<(), Status> {
        self.journal_store
            .start_orchestrator_run(request)
            .map_err(|error| map_orchestrator_store_error("start orchestrator run", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn start_orchestrator_run(
        self: &Arc<Self>,
        request: OrchestratorRunStartRequest,
    ) -> Result<(), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.start_orchestrator_run_blocking(&request))
            .await
            .map_err(|_| Status::internal("orchestrator run worker panicked"))??;
        self.counters.orchestrator_runs_started.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    fn update_orchestrator_run_state_blocking(
        &self,
        run_id: &str,
        state: RunLifecycleState,
        error_message: Option<&str>,
    ) -> Result<(), Status> {
        self.journal_store
            .update_orchestrator_run_state(run_id, state, error_message)
            .map_err(|error| map_orchestrator_store_error("update orchestrator run state", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn update_orchestrator_run_state(
        self: &Arc<Self>,
        run_id: String,
        state: RunLifecycleState,
        error_message: Option<String>,
    ) -> Result<(), Status> {
        let state_ref = Arc::clone(self);
        let error_message_ref = error_message.clone();
        tokio::task::spawn_blocking(move || {
            state_ref.update_orchestrator_run_state_blocking(
                run_id.as_str(),
                state,
                error_message_ref.as_deref(),
            )
        })
        .await
        .map_err(|_| Status::internal("orchestrator run state worker panicked"))??;
        if state == RunLifecycleState::Done {
            self.counters.orchestrator_runs_completed.fetch_add(1, Ordering::Relaxed);
        } else if state == RunLifecycleState::Cancelled {
            self.counters.orchestrator_runs_cancelled.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    fn add_orchestrator_usage_blocking(
        &self,
        delta: &OrchestratorUsageDelta,
    ) -> Result<(), Status> {
        self.journal_store
            .add_orchestrator_usage(delta)
            .map_err(|error| map_orchestrator_store_error("update orchestrator usage", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn add_orchestrator_usage(
        self: &Arc<Self>,
        delta: OrchestratorUsageDelta,
    ) -> Result<(), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.add_orchestrator_usage_blocking(&delta))
            .await
            .map_err(|_| Status::internal("orchestrator usage worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn append_orchestrator_tape_event_blocking(
        &self,
        request: &OrchestratorTapeAppendRequest,
    ) -> Result<(), Status> {
        self.journal_store
            .append_orchestrator_tape_event(request)
            .map_err(|error| map_orchestrator_store_error("append orchestrator tape event", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn append_orchestrator_tape_event(
        self: &Arc<Self>,
        request: OrchestratorTapeAppendRequest,
    ) -> Result<(), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.append_orchestrator_tape_event_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("orchestrator tape worker panicked"))??;
        self.counters.orchestrator_tape_events.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    fn request_orchestrator_cancel_blocking(
        &self,
        request: &OrchestratorCancelRequest,
    ) -> Result<(), Status> {
        self.journal_store
            .request_orchestrator_cancel(request)
            .map_err(|error| map_orchestrator_store_error("request orchestrator cancel", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn request_orchestrator_cancel(
        self: &Arc<Self>,
        request: OrchestratorCancelRequest,
    ) -> Result<RunCancelSnapshot, Status> {
        let state = Arc::clone(self);
        let run_id = request.run_id.clone();
        let reason = request.reason.clone();
        tokio::task::spawn_blocking(move || state.request_orchestrator_cancel_blocking(&request))
            .await
            .map_err(|_| Status::internal("orchestrator cancel worker panicked"))??;
        self.counters.orchestrator_cancel_requests.fetch_add(1, Ordering::Relaxed);
        Ok(RunCancelSnapshot { run_id, cancel_requested: true, reason })
    }

    #[allow(clippy::result_large_err)]
    fn is_orchestrator_cancel_requested_blocking(&self, run_id: &str) -> Result<bool, Status> {
        self.journal_store
            .is_orchestrator_cancel_requested(run_id)
            .map_err(|error| map_orchestrator_store_error("load orchestrator cancel flag", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn is_orchestrator_cancel_requested(
        self: &Arc<Self>,
        run_id: String,
    ) -> Result<bool, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.is_orchestrator_cancel_requested_blocking(run_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("orchestrator cancel read worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn orchestrator_run_status_snapshot_blocking(
        &self,
        run_id: &str,
    ) -> Result<Option<OrchestratorRunStatusSnapshot>, Status> {
        self.journal_store
            .orchestrator_run_status_snapshot(run_id)
            .map_err(|error| map_orchestrator_store_error("load orchestrator run snapshot", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn orchestrator_run_status_snapshot(
        self: &Arc<Self>,
        run_id: String,
    ) -> Result<Option<OrchestratorRunStatusSnapshot>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.orchestrator_run_status_snapshot_blocking(run_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("orchestrator snapshot worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn orchestrator_tape_snapshot_blocking(
        &self,
        run_id: &str,
        after_seq: Option<i64>,
        requested_limit: Option<usize>,
    ) -> Result<RunTapeSnapshot, Status> {
        let run_exists = self
            .journal_store
            .orchestrator_run_status_snapshot(run_id)
            .map_err(|error| map_orchestrator_store_error("load orchestrator run snapshot", error))?
            .is_some();
        if !run_exists {
            return Err(Status::not_found(format!("orchestrator run not found: {run_id}")));
        }
        let limit = requested_limit
            .unwrap_or(self.config.max_tape_entries_per_response)
            .clamp(MIN_TAPE_PAGE_LIMIT, self.config.max_tape_entries_per_response);
        let fetched_events = self
            .journal_store
            .orchestrator_tape_page(run_id, after_seq, limit.saturating_add(1))
            .map_err(|error| map_orchestrator_store_error("load orchestrator tape", error))?;
        let mut events = Vec::with_capacity(limit);
        let mut returned_bytes = 0_usize;
        let mut has_more = false;

        for record in fetched_events {
            if events.len() >= limit {
                has_more = true;
                break;
            }
            let sanitized_payload =
                crate::journal::redact_payload_json(record.payload_json.as_bytes()).map_err(
                    |error| map_orchestrator_store_error("redact orchestrator tape payload", error),
                )?;
            let payload_bytes = sanitized_payload.len();
            if events.is_empty() && payload_bytes > self.config.max_tape_bytes_per_response {
                return Err(Status::resource_exhausted(format!(
                    "single orchestrator tape event exceeds response byte limit ({payload_bytes} > {})",
                    self.config.max_tape_bytes_per_response
                )));
            }
            if returned_bytes.saturating_add(payload_bytes)
                > self.config.max_tape_bytes_per_response
            {
                has_more = true;
                break;
            }
            returned_bytes = returned_bytes.saturating_add(payload_bytes);
            events.push(OrchestratorTapeRecord {
                seq: record.seq,
                event_type: record.event_type,
                payload_json: sanitized_payload,
            });
        }

        let next_after_seq = if has_more { events.last().map(|event| event.seq) } else { None };
        Ok(RunTapeSnapshot {
            run_id: run_id.to_owned(),
            requested_after_seq: after_seq,
            limit,
            max_response_bytes: self.config.max_tape_bytes_per_response,
            returned_bytes,
            next_after_seq,
            events,
        })
    }

    #[allow(clippy::result_large_err)]
    pub async fn orchestrator_tape_snapshot(
        self: &Arc<Self>,
        run_id: String,
        after_seq: Option<i64>,
        limit: Option<usize>,
    ) -> Result<RunTapeSnapshot, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.orchestrator_tape_snapshot_blocking(run_id.as_str(), after_seq, limit)
        })
        .await
        .map_err(|_| Status::internal("orchestrator tape snapshot worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn create_cron_job_blocking(
        &self,
        request: &CronJobCreateRequest,
    ) -> Result<CronJobRecord, Status> {
        self.journal_store
            .create_cron_job(request)
            .map_err(|error| map_cron_store_error("create cron job", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn create_cron_job(
        self: &Arc<Self>,
        request: CronJobCreateRequest,
    ) -> Result<CronJobRecord, Status> {
        let state = Arc::clone(self);
        let result = tokio::task::spawn_blocking(move || state.create_cron_job_blocking(&request))
            .await
            .map_err(|_| Status::internal("cron create worker panicked"))??;
        self.counters.cron_jobs_created.fetch_add(1, Ordering::Relaxed);
        Ok(result)
    }

    #[allow(clippy::result_large_err)]
    fn update_cron_job_blocking(
        &self,
        job_id: &str,
        patch: &CronJobUpdatePatch,
    ) -> Result<CronJobRecord, Status> {
        self.journal_store
            .update_cron_job(job_id, patch)
            .map_err(|error| map_cron_store_error("update cron job", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn update_cron_job(
        self: &Arc<Self>,
        job_id: String,
        patch: CronJobUpdatePatch,
    ) -> Result<CronJobRecord, Status> {
        let state = Arc::clone(self);
        let result = tokio::task::spawn_blocking(move || {
            state.update_cron_job_blocking(job_id.as_str(), &patch)
        })
        .await
        .map_err(|_| Status::internal("cron update worker panicked"))??;
        self.counters.cron_jobs_updated.fetch_add(1, Ordering::Relaxed);
        Ok(result)
    }

    #[allow(clippy::result_large_err)]
    fn delete_cron_job_blocking(&self, job_id: &str) -> Result<bool, Status> {
        self.journal_store
            .delete_cron_job(job_id)
            .map_err(|error| map_cron_store_error("delete cron job", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn delete_cron_job(self: &Arc<Self>, job_id: String) -> Result<bool, Status> {
        let state = Arc::clone(self);
        let deleted =
            tokio::task::spawn_blocking(move || state.delete_cron_job_blocking(job_id.as_str()))
                .await
                .map_err(|_| Status::internal("cron delete worker panicked"))??;
        if deleted {
            self.counters.cron_jobs_deleted.fetch_add(1, Ordering::Relaxed);
        }
        Ok(deleted)
    }

    #[allow(clippy::result_large_err)]
    fn cron_job_blocking(&self, job_id: &str) -> Result<Option<CronJobRecord>, Status> {
        self.journal_store
            .cron_job(job_id)
            .map_err(|error| map_cron_store_error("load cron job", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn cron_job(
        self: &Arc<Self>,
        job_id: String,
    ) -> Result<Option<CronJobRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.cron_job_blocking(job_id.as_str()))
            .await
            .map_err(|_| Status::internal("cron read worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    pub async fn list_cron_jobs(
        self: &Arc<Self>,
        after_job_id: Option<String>,
        requested_limit: Option<usize>,
        enabled: Option<bool>,
        owner_principal: Option<String>,
        channel: Option<String>,
    ) -> Result<(Vec<CronJobRecord>, Option<String>), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let limit = requested_limit.unwrap_or(100).clamp(1, MAX_CRON_PAGE_LIMIT);
            state
                .journal_store
                .list_cron_jobs(CronJobsListFilter {
                    after_job_id: after_job_id.as_deref(),
                    limit: limit.saturating_add(1),
                    enabled,
                    owner_principal: owner_principal.as_deref(),
                    channel: channel.as_deref(),
                })
                .map_err(|error| map_cron_store_error("list cron jobs", error))
        })
        .await
        .map_err(|_| Status::internal("cron list worker panicked"))?
        .map(|mut jobs| {
            let limit = requested_limit.unwrap_or(100).clamp(1, MAX_CRON_PAGE_LIMIT);
            let has_more = jobs.len() > limit;
            if has_more {
                jobs.truncate(limit);
            }
            let next_after =
                if has_more { jobs.last().map(|job| job.job_id.clone()) } else { None };
            (jobs, next_after)
        })
    }

    #[allow(clippy::result_large_err)]
    pub async fn list_due_cron_jobs(
        self: &Arc<Self>,
        now_unix_ms: i64,
        limit: usize,
    ) -> Result<Vec<CronJobRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .list_due_cron_jobs(now_unix_ms, limit)
                .map_err(|error| map_cron_store_error("list due cron jobs", error))
        })
        .await
        .map_err(|_| Status::internal("cron due-list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    pub async fn first_due_cron_job_time(self: &Arc<Self>) -> Result<Option<i64>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .first_due_cron_job_time()
                .map_err(|error| map_cron_store_error("load first due cron job time", error))
        })
        .await
        .map_err(|_| Status::internal("cron next due worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    pub async fn set_cron_job_next_run(
        self: &Arc<Self>,
        job_id: String,
        next_run_at_unix_ms: Option<i64>,
        last_run_at_unix_ms: Option<i64>,
    ) -> Result<(), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .set_cron_job_next_run(job_id.as_str(), next_run_at_unix_ms, last_run_at_unix_ms)
                .map_err(|error| map_cron_store_error("update cron job next run", error))
        })
        .await
        .map_err(|_| Status::internal("cron next-run worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    pub async fn set_cron_job_queue_state(
        self: &Arc<Self>,
        job_id: String,
        queued_run: bool,
    ) -> Result<(), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .set_cron_job_queue_state(job_id.as_str(), queued_run)
                .map_err(|error| map_cron_store_error("update cron job queue state", error))
        })
        .await
        .map_err(|_| Status::internal("cron queue worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    pub async fn start_cron_run(
        self: &Arc<Self>,
        request: CronRunStartRequest,
    ) -> Result<(), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .start_cron_run(&request)
                .map_err(|error| map_cron_store_error("start cron run", error))
        })
        .await
        .map_err(|_| Status::internal("cron run start worker panicked"))??;
        self.counters.cron_runs_started.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    pub async fn finalize_cron_run(
        self: &Arc<Self>,
        request: CronRunFinalizeRequest,
    ) -> Result<(), Status> {
        let terminal_status = request.status;
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .finalize_cron_run(&request)
                .map_err(|error| map_cron_store_error("finalize cron run", error))
        })
        .await
        .map_err(|_| Status::internal("cron run finalize worker panicked"))??;
        match terminal_status {
            CronRunStatus::Succeeded => {
                self.counters.cron_runs_completed.fetch_add(1, Ordering::Relaxed);
            }
            CronRunStatus::Failed | CronRunStatus::Denied => {
                self.counters.cron_runs_failed.fetch_add(1, Ordering::Relaxed);
            }
            CronRunStatus::Skipped => {
                self.counters.cron_runs_skipped.fetch_add(1, Ordering::Relaxed);
            }
            CronRunStatus::Accepted | CronRunStatus::Running => {}
        }
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    pub async fn cron_run(
        self: &Arc<Self>,
        run_id: String,
    ) -> Result<Option<CronRunRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .cron_run(run_id.as_str())
                .map_err(|error| map_cron_store_error("load cron run", error))
        })
        .await
        .map_err(|_| Status::internal("cron run read worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    pub async fn active_cron_run_for_job(
        self: &Arc<Self>,
        job_id: String,
    ) -> Result<Option<CronRunRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .active_cron_run_for_job(job_id.as_str())
                .map_err(|error| map_cron_store_error("load active cron run", error))
        })
        .await
        .map_err(|_| Status::internal("active cron run worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    pub async fn list_cron_runs(
        self: &Arc<Self>,
        job_id: Option<String>,
        after_run_id: Option<String>,
        requested_limit: Option<usize>,
    ) -> Result<(Vec<CronRunRecord>, Option<String>), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let limit = requested_limit.unwrap_or(100).clamp(1, MAX_CRON_PAGE_LIMIT);
            state
                .journal_store
                .list_cron_runs(CronRunsListFilter {
                    job_id: job_id.as_deref(),
                    after_run_id: after_run_id.as_deref(),
                    limit: limit.saturating_add(1),
                })
                .map_err(|error| map_cron_store_error("list cron runs", error))
        })
        .await
        .map_err(|_| Status::internal("cron runs list worker panicked"))?
        .map(|mut runs| {
            let limit = requested_limit.unwrap_or(100).clamp(1, MAX_CRON_PAGE_LIMIT);
            let has_more = runs.len() > limit;
            if has_more {
                runs.truncate(limit);
            }
            let next_after =
                if has_more { runs.last().map(|run| run.run_id.clone()) } else { None };
            (runs, next_after)
        })
    }

    #[allow(clippy::result_large_err)]
    pub async fn create_approval_record(
        self: &Arc<Self>,
        request: ApprovalCreateRequest,
    ) -> Result<ApprovalRecord, Status> {
        let subject_type = request.subject_type;
        let state = Arc::clone(self);
        let result = tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .create_approval(&request)
                .map_err(|error| map_approval_store_error("create approval", error))
        })
        .await
        .map_err(|_| Status::internal("approval create worker panicked"))??;
        if subject_type == ApprovalSubjectType::Tool {
            self.counters.approvals_tool_requested.fetch_add(1, Ordering::Relaxed);
        }
        Ok(result)
    }

    #[allow(clippy::result_large_err)]
    pub async fn resolve_approval_record(
        self: &Arc<Self>,
        request: ApprovalResolveRequest,
    ) -> Result<ApprovalRecord, Status> {
        let decision = request.decision;
        let state = Arc::clone(self);
        let result = tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .resolve_approval(&request)
                .map_err(|error| map_approval_store_error("resolve approval", error))
        })
        .await
        .map_err(|_| Status::internal("approval resolve worker panicked"))??;
        if result.subject_type == ApprovalSubjectType::Tool {
            match decision {
                ApprovalDecision::Allow => {
                    self.counters.approvals_tool_resolved_allow.fetch_add(1, Ordering::Relaxed);
                }
                ApprovalDecision::Deny => {
                    self.counters.approvals_tool_resolved_deny.fetch_add(1, Ordering::Relaxed);
                }
                ApprovalDecision::Timeout => {
                    self.counters.approvals_tool_resolved_timeout.fetch_add(1, Ordering::Relaxed);
                }
                ApprovalDecision::Error => {
                    self.counters.approvals_tool_resolved_error.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        Ok(result)
    }

    #[allow(clippy::result_large_err)]
    pub async fn approval_record(
        self: &Arc<Self>,
        approval_id: String,
    ) -> Result<Option<ApprovalRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .approval(approval_id.as_str())
                .map_err(|error| map_approval_store_error("load approval", error))
        })
        .await
        .map_err(|_| Status::internal("approval read worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    #[allow(clippy::too_many_arguments)]
    pub async fn list_approval_records(
        self: &Arc<Self>,
        after_approval_id: Option<String>,
        requested_limit: Option<usize>,
        since_unix_ms: Option<i64>,
        until_unix_ms: Option<i64>,
        subject_id: Option<String>,
        principal: Option<String>,
        decision: Option<ApprovalDecision>,
        subject_type: Option<ApprovalSubjectType>,
    ) -> Result<(Vec<ApprovalRecord>, Option<String>), Status> {
        let effective_limit = requested_limit
            .filter(|value| *value > 0)
            .unwrap_or(100)
            .clamp(1, MAX_APPROVAL_PAGE_LIMIT);
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .list_approvals(ApprovalsListFilter {
                    after_approval_id: after_approval_id.as_deref(),
                    limit: effective_limit.saturating_add(1),
                    since_unix_ms,
                    until_unix_ms,
                    subject_id: subject_id.as_deref(),
                    principal: principal.as_deref(),
                    decision,
                    subject_type,
                })
                .map_err(|error| map_approval_store_error("list approvals", error))
        })
        .await
        .map_err(|_| Status::internal("approvals list worker panicked"))?
        .map(|mut approvals| {
            let has_more = approvals.len() > effective_limit;
            if has_more {
                approvals.truncate(effective_limit);
            }
            let next_after = if has_more {
                approvals.last().map(|approval| approval.approval_id.clone())
            } else {
                None
            };
            (approvals, next_after)
        })
    }

    #[allow(clippy::result_large_err)]
    pub async fn upsert_skill_status(
        self: &Arc<Self>,
        request: SkillStatusUpsertRequest,
    ) -> Result<SkillStatusRecord, Status> {
        let state = Arc::clone(self);
        let record = tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .upsert_skill_status(&request)
                .map_err(|error| map_skill_store_error("upsert skill status", error))
        })
        .await
        .map_err(|_| Status::internal("skill status update worker panicked"))??;
        self.counters.skill_status_updates.fetch_add(1, Ordering::Relaxed);
        Ok(record)
    }

    #[allow(clippy::result_large_err)]
    pub async fn skill_status(
        self: &Arc<Self>,
        skill_id: String,
        version: String,
    ) -> Result<Option<SkillStatusRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .skill_status(skill_id.as_str(), version.as_str())
                .map_err(|error| map_skill_store_error("load skill status", error))
        })
        .await
        .map_err(|_| Status::internal("skill status read worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    pub async fn latest_skill_status(
        self: &Arc<Self>,
        skill_id: String,
    ) -> Result<Option<SkillStatusRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .latest_skill_status(skill_id.as_str())
                .map_err(|error| map_skill_store_error("load latest skill status", error))
        })
        .await
        .map_err(|_| Status::internal("latest skill status read worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    pub async fn record_skill_status_event(
        self: &Arc<Self>,
        context: &RequestContext,
        event: &str,
        record: &SkillStatusRecord,
    ) -> Result<(), Status> {
        self.record_journal_event(JournalAppendRequest {
            event_id: Ulid::new().to_string(),
            session_id: Ulid::new().to_string(),
            run_id: Ulid::new().to_string(),
            kind: common_v1::journal_event::EventKind::ToolExecuted as i32,
            actor: common_v1::journal_event::EventActor::System as i32,
            timestamp_unix_ms: current_unix_ms(),
            payload_json: json!({
                "event": event,
                "skill_id": record.skill_id,
                "version": record.version,
                "status": record.status.as_str(),
                "reason": record.reason,
                "detected_at_ms": record.detected_at_ms,
                "operator_principal": record.operator_principal,
            })
            .to_string()
            .into_bytes(),
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
        })
        .await
        .map(|_| ())
    }

    pub fn configure_memory(&self, config: MemoryRuntimeConfig) {
        match self.memory_config.write() {
            Ok(mut guard) => {
                *guard = config;
            }
            Err(poisoned) => {
                warn!("memory config lock poisoned while applying runtime config");
                let mut guard = poisoned.into_inner();
                *guard = config;
            }
        }
        self.clear_memory_search_cache();
    }

    #[must_use]
    pub fn memory_config_snapshot(&self) -> MemoryRuntimeConfig {
        match self.memory_config.read() {
            Ok(config) => config.clone(),
            Err(poisoned) => {
                warn!("memory config lock poisoned while reading runtime config");
                poisoned.into_inner().clone()
            }
        }
    }

    pub fn clear_memory_search_cache(&self) {
        match self.memory_search_cache.lock() {
            Ok(mut cache) => {
                cache.clear();
            }
            Err(poisoned) => {
                warn!("memory search cache lock poisoned while clearing cache");
                let mut cache = poisoned.into_inner();
                cache.clear();
            }
        }
    }

    fn clear_tool_approval_cache_for_session(&self, context: &RequestContext, session_id: &str) {
        let key_prefix = tool_approval_cache_key_prefix(context, session_id);
        match self.tool_approval_cache.lock() {
            Ok(mut cache) => {
                cache.retain(|key, _| !key.starts_with(key_prefix.as_str()));
            }
            Err(poisoned) => {
                warn!("tool approval cache lock poisoned while clearing session cache");
                let mut cache = poisoned.into_inner();
                cache.retain(|key, _| !key.starts_with(key_prefix.as_str()));
            }
        }
    }

    fn resolve_cached_tool_approval(
        &self,
        context: &RequestContext,
        session_id: &str,
        subject_id: &str,
    ) -> Option<ToolApprovalOutcome> {
        let now_unix_ms = current_unix_ms();
        let cache_key = tool_approval_cache_key(context, session_id, subject_id);
        let resolve_from_cache =
            |cache: &mut HashMap<String, CachedToolApprovalDecision>| -> Option<ToolApprovalOutcome> {
                cache.retain(|_, entry| match entry.expires_at_unix_ms {
                    Some(expires_at_unix_ms) => expires_at_unix_ms > now_unix_ms,
                    None => true,
                });
                let cached = cache.get(cache_key.as_str())?.clone();
                let remaining_ttl_ms = cached
                    .expires_at_unix_ms
                    .map(|expires_at_unix_ms| expires_at_unix_ms.saturating_sub(now_unix_ms))
                    .filter(|remaining| *remaining > 0);
                Some(ToolApprovalOutcome {
                    approval_id: cached.approval_id,
                    approved: cached.approved,
                    reason: format!(
                        "cached_approval(scope={}): {}",
                        cached.decision_scope.as_str(),
                        cached.reason
                    ),
                    decision: cached.decision,
                    decision_scope: cached.decision_scope,
                    decision_scope_ttl_ms: remaining_ttl_ms,
                })
            };
        match self.tool_approval_cache.lock() {
            Ok(mut cache) => resolve_from_cache(&mut cache),
            Err(poisoned) => {
                warn!("tool approval cache lock poisoned while resolving cached decision");
                let mut cache = poisoned.into_inner();
                resolve_from_cache(&mut cache)
            }
        }
    }

    fn remember_tool_approval(
        &self,
        context: &RequestContext,
        session_id: &str,
        subject_id: &str,
        outcome: &ToolApprovalOutcome,
    ) {
        if !matches!(outcome.decision, ApprovalDecision::Allow | ApprovalDecision::Deny) {
            return;
        }
        let now_unix_ms = current_unix_ms();
        let expires_at_unix_ms = match outcome.decision_scope {
            ApprovalDecisionScope::Once => return,
            ApprovalDecisionScope::Session => outcome
                .decision_scope_ttl_ms
                .filter(|ttl_ms| *ttl_ms > 0)
                .map(|ttl_ms| now_unix_ms.saturating_add(ttl_ms)),
            ApprovalDecisionScope::Timeboxed => {
                let Some(ttl_ms) = outcome.decision_scope_ttl_ms.filter(|ttl_ms| *ttl_ms > 0)
                else {
                    warn!(
                        approval_id = %outcome.approval_id,
                        "ignoring timeboxed approval memory entry without positive ttl"
                    );
                    return;
                };
                Some(now_unix_ms.saturating_add(ttl_ms))
            }
        };
        let cache_key = tool_approval_cache_key(context, session_id, subject_id);
        let cache_entry = CachedToolApprovalDecision {
            approval_id: outcome.approval_id.clone(),
            approved: outcome.approved,
            reason: outcome.reason.clone(),
            decision: outcome.decision,
            decision_scope: outcome.decision_scope,
            expires_at_unix_ms,
        };
        let remember_in_cache = |cache: &mut HashMap<String, CachedToolApprovalDecision>| {
            cache.retain(|_, entry| match entry.expires_at_unix_ms {
                Some(entry_expires_at_unix_ms) => entry_expires_at_unix_ms > now_unix_ms,
                None => true,
            });
            if cache.len() >= APPROVAL_DECISION_CACHE_CAPACITY {
                if let Some(first_key) = cache.keys().next().cloned() {
                    cache.remove(first_key.as_str());
                }
            }
            cache.insert(cache_key.clone(), cache_entry.clone());
        };
        match self.tool_approval_cache.lock() {
            Ok(mut cache) => remember_in_cache(&mut cache),
            Err(poisoned) => {
                warn!("tool approval cache lock poisoned while recording decision");
                let mut cache = poisoned.into_inner();
                remember_in_cache(&mut cache);
            }
        }
    }

    #[allow(clippy::result_large_err)]
    pub async fn ingest_memory_item(
        self: &Arc<Self>,
        mut request: MemoryItemCreateRequest,
    ) -> Result<MemoryItemRecord, Status> {
        let config = self.memory_config_snapshot();
        let payload_bytes = request.content_text.len();
        let token_count = request.content_text.split_whitespace().count();
        if payload_bytes > config.max_item_bytes {
            self.counters.memory_items_rejected.fetch_add(1, Ordering::Relaxed);
            return Err(Status::invalid_argument(format!(
                "memory content exceeds byte limit ({payload_bytes} > {})",
                config.max_item_bytes
            )));
        }
        if token_count > config.max_item_tokens {
            self.counters.memory_items_rejected.fetch_add(1, Ordering::Relaxed);
            return Err(Status::invalid_argument(format!(
                "memory content exceeds token limit ({token_count} > {})",
                config.max_item_tokens
            )));
        }
        if request.ttl_unix_ms.is_none() {
            if let Some(default_ttl_ms) = config.default_ttl_ms {
                let now = current_unix_ms_status()?;
                request.ttl_unix_ms = Some(now.saturating_add(default_ttl_ms));
            }
        }

        let state = Arc::clone(self);
        let created = tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .create_memory_item(&request)
                .map_err(|error| map_memory_store_error("ingest memory item", error))
        })
        .await
        .map_err(|_| Status::internal("memory ingest worker panicked"))??;
        self.counters.memory_items_ingested.fetch_add(1, Ordering::Relaxed);
        self.clear_memory_search_cache();
        Ok(created)
    }

    #[allow(clippy::result_large_err)]
    pub async fn memory_item(
        self: &Arc<Self>,
        memory_id: String,
    ) -> Result<Option<MemoryItemRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .memory_item(memory_id.as_str())
                .map_err(|error| map_memory_store_error("load memory item", error))
        })
        .await
        .map_err(|_| Status::internal("memory read worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    pub async fn delete_memory_item(
        self: &Arc<Self>,
        memory_id: String,
        principal: String,
        channel: Option<String>,
    ) -> Result<bool, Status> {
        let state = Arc::clone(self);
        let deleted = tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .delete_memory_item(memory_id.as_str(), principal.as_str(), channel.as_deref())
                .map_err(|error| map_memory_store_error("delete memory item", error))
        })
        .await
        .map_err(|_| Status::internal("memory delete worker panicked"))??;
        if deleted {
            self.clear_memory_search_cache();
        }
        Ok(deleted)
    }

    #[allow(clippy::result_large_err, clippy::too_many_arguments)]
    pub async fn list_memory_items(
        self: &Arc<Self>,
        after_memory_id: Option<String>,
        requested_limit: Option<usize>,
        principal: String,
        channel: Option<String>,
        session_id: Option<String>,
        tags: Vec<String>,
        sources: Vec<MemorySource>,
    ) -> Result<(Vec<MemoryItemRecord>, Option<String>), Status> {
        let effective_limit = requested_limit.unwrap_or(100).clamp(1, MAX_MEMORY_PAGE_LIMIT);
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .list_memory_items(&MemoryItemsListFilter {
                    after_memory_id,
                    principal,
                    channel,
                    session_id,
                    limit: effective_limit.saturating_add(1),
                    tags,
                    sources,
                })
                .map_err(|error| map_memory_store_error("list memory items", error))
        })
        .await
        .map_err(|_| Status::internal("memory list worker panicked"))?
        .map(|mut items| {
            let has_more = items.len() > effective_limit;
            if has_more {
                items.truncate(effective_limit);
            }
            let next_after =
                if has_more { items.last().map(|item| item.memory_id.clone()) } else { None };
            (items, next_after)
        })
    }

    #[allow(clippy::result_large_err)]
    pub async fn purge_memory(
        self: &Arc<Self>,
        request: MemoryPurgeRequest,
    ) -> Result<u64, Status> {
        let state = Arc::clone(self);
        let deleted = tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .purge_memory(&request)
                .map_err(|error| map_memory_store_error("purge memory items", error))
        })
        .await
        .map_err(|_| Status::internal("memory purge worker panicked"))??;
        if deleted > 0 {
            self.clear_memory_search_cache();
        }
        Ok(deleted)
    }

    #[allow(clippy::result_large_err)]
    pub async fn search_memory(
        self: &Arc<Self>,
        request: MemorySearchRequest,
    ) -> Result<Vec<MemorySearchHit>, Status> {
        self.counters.memory_search_requests.fetch_add(1, Ordering::Relaxed);
        let cache_key = memory_search_cache_key(&request);
        let cached_hits = match self.memory_search_cache.lock() {
            Ok(cache) => cache.get(cache_key.as_str()).cloned(),
            Err(poisoned) => {
                warn!("memory search cache lock poisoned while reading cache");
                let cache = poisoned.into_inner();
                cache.get(cache_key.as_str()).cloned()
            }
        };
        if let Some(cached) = cached_hits {
            self.counters.memory_search_cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(cached);
        }

        let started_at = Instant::now();
        let state = Arc::clone(self);
        let results = tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .search_memory(&request)
                .map_err(|error| map_memory_store_error("search memory items", error))
        })
        .await
        .map_err(|_| Status::internal("memory search worker panicked"))??;
        if started_at.elapsed().as_millis() > MEMORY_SEARCH_LATENCY_BUDGET_MS {
            warn!(
                elapsed_ms = started_at.elapsed().as_millis(),
                budget_ms = MEMORY_SEARCH_LATENCY_BUDGET_MS,
                "memory search exceeded latency budget"
            );
        }

        match self.memory_search_cache.lock() {
            Ok(mut cache) => {
                if cache.len() >= MEMORY_SEARCH_CACHE_CAPACITY {
                    if let Some(first_key) = cache.keys().next().cloned() {
                        cache.remove(first_key.as_str());
                    }
                }
                cache.insert(cache_key, results.clone());
            }
            Err(poisoned) => {
                warn!("memory search cache lock poisoned while writing cache");
                let mut cache = poisoned.into_inner();
                if cache.len() >= MEMORY_SEARCH_CACHE_CAPACITY {
                    if let Some(first_key) = cache.keys().next().cloned() {
                        cache.remove(first_key.as_str());
                    }
                }
                cache.insert(cache_key, results.clone());
            }
        }
        Ok(results)
    }

    pub fn record_cron_trigger_fired(&self) {
        self.counters.cron_triggers_fired.fetch_add(1, Ordering::Relaxed);
    }
}

fn map_orchestrator_store_error(operation: &str, error: JournalError) -> Status {
    match error {
        JournalError::DuplicateRunId { run_id } => {
            Status::already_exists(format!("orchestrator run already exists: {run_id}"))
        }
        JournalError::DuplicateTapeSequence { run_id, seq } => Status::already_exists(format!(
            "orchestrator tape already contains seq={seq} for run {run_id}"
        )),
        JournalError::RunNotFound { run_id } => {
            Status::not_found(format!("orchestrator run not found: {run_id}"))
        }
        JournalError::PayloadTooLarge { payload_kind, actual_bytes, max_bytes } => {
            Status::invalid_argument(format!(
                "{payload_kind} payload exceeds maximum size ({actual_bytes} > {max_bytes})"
            ))
        }
        JournalError::SessionIdentityMismatch { session_id } => Status::failed_precondition(
            format!("orchestrator session identity mismatch for session: {session_id}"),
        ),
        JournalError::SessionNotFound { selector } => {
            Status::not_found(format!("orchestrator session not found for selector: {selector}"))
        }
        JournalError::InvalidSessionSelector { reason } => {
            Status::invalid_argument(format!("invalid orchestrator session selector: {reason}"))
        }
        other => Status::internal(format!("{operation} failed: {other}")),
    }
}

fn map_agent_registry_error(operation: &str, error: AgentRegistryError) -> Status {
    match error {
        AgentRegistryError::AgentNotFound(agent_id) => {
            Status::not_found(format!("agent not found: {agent_id}"))
        }
        AgentRegistryError::DuplicateAgentId(agent_id) => {
            Status::already_exists(format!("agent already exists: {agent_id}"))
        }
        AgentRegistryError::AgentDirCollision(agent_id) => Status::already_exists(format!(
            "agent directory overlaps with existing agent {agent_id}"
        )),
        AgentRegistryError::WorkspaceRootEscape(path)
        | AgentRegistryError::DuplicateWorkspaceRoot(path)
        | AgentRegistryError::InvalidSessionId(path) => Status::invalid_argument(path),
        AgentRegistryError::DefaultAgentNotConfigured => {
            Status::failed_precondition("default agent is not configured")
        }
        AgentRegistryError::InvalidPath { field, message } => {
            Status::invalid_argument(format!("{field}: {message}"))
        }
        AgentRegistryError::RegistryLimitExceeded => {
            Status::resource_exhausted("agent registry limits exceeded")
        }
        other => Status::internal(format!("{operation} failed: {other}")),
    }
}

fn map_cron_store_error(operation: &str, error: JournalError) -> Status {
    match error {
        JournalError::CronJobNotFound { job_id } => {
            Status::not_found(format!("cron job not found: {job_id}"))
        }
        JournalError::CronRunNotFound { run_id } => {
            Status::not_found(format!("cron run not found: {run_id}"))
        }
        JournalError::DuplicateCronJobId { job_id } => {
            Status::already_exists(format!("cron job already exists: {job_id}"))
        }
        JournalError::DuplicateCronRunId { run_id } => {
            Status::already_exists(format!("cron run already exists: {run_id}"))
        }
        JournalError::PayloadTooLarge { payload_kind, actual_bytes, max_bytes } => {
            Status::invalid_argument(format!(
                "{payload_kind} payload exceeds maximum size ({actual_bytes} > {max_bytes})"
            ))
        }
        other => Status::internal(format!("{operation} failed: {other}")),
    }
}

fn map_approval_store_error(operation: &str, error: JournalError) -> Status {
    match error {
        JournalError::ApprovalNotFound { approval_id } => {
            Status::not_found(format!("approval record not found: {approval_id}"))
        }
        JournalError::DuplicateApprovalId { approval_id } => {
            Status::already_exists(format!("approval record already exists: {approval_id}"))
        }
        JournalError::PayloadTooLarge { payload_kind, actual_bytes, max_bytes } => {
            Status::invalid_argument(format!(
                "{payload_kind} payload exceeds maximum size ({actual_bytes} > {max_bytes})"
            ))
        }
        other => Status::internal(format!("{operation} failed: {other}")),
    }
}

fn map_memory_store_error(operation: &str, error: JournalError) -> Status {
    match error {
        JournalError::MemoryNotFound { memory_id } => {
            Status::not_found(format!("memory item not found: {memory_id}"))
        }
        JournalError::DuplicateMemoryId { memory_id } => {
            Status::already_exists(format!("memory item already exists: {memory_id}"))
        }
        JournalError::PayloadTooLarge { payload_kind, actual_bytes, max_bytes } => {
            Status::invalid_argument(format!(
                "{payload_kind} payload exceeds maximum size ({actual_bytes} > {max_bytes})"
            ))
        }
        other => Status::internal(format!("{operation} failed: {other}")),
    }
}

fn map_skill_store_error(operation: &str, error: JournalError) -> Status {
    match error {
        JournalError::PayloadTooLarge { payload_kind, actual_bytes, max_bytes } => {
            Status::invalid_argument(format!(
                "{payload_kind} payload exceeds maximum size ({actual_bytes} > {max_bytes})"
            ))
        }
        other => Status::internal(format!("{operation} failed: {other}")),
    }
}

fn map_canvas_store_error(operation: &str, error: JournalError) -> Status {
    match error {
        JournalError::DuplicateCanvasStateVersion { canvas_id, state_version } => {
            Status::already_exists(format!(
                "canvas state already exists for canvas {canvas_id} at version {state_version}"
            ))
        }
        JournalError::CanvasStateNotFound { canvas_id } => {
            Status::not_found(format!("canvas state not found: {canvas_id}"))
        }
        JournalError::InvalidCanvasReplay { canvas_id, reason } => Status::failed_precondition(
            format!("invalid canvas replay state for {canvas_id}: {reason}"),
        ),
        JournalError::PayloadTooLarge { payload_kind, actual_bytes, max_bytes } => {
            Status::invalid_argument(format!(
                "{payload_kind} payload exceeds maximum size ({actual_bytes} > {max_bytes})"
            ))
        }
        other => Status::internal(format!("{operation} failed: {other}")),
    }
}

fn map_vault_error(operation: &str, error: VaultError) -> Status {
    match error {
        VaultError::NotFound => Status::not_found("secret not found"),
        VaultError::InvalidScope(message)
        | VaultError::InvalidKey(message)
        | VaultError::InvalidObjectId(message)
        | VaultError::Crypto(message) => Status::invalid_argument(message),
        VaultError::ValueTooLarge { actual, max } => {
            Status::invalid_argument(format!("secret value exceeds limit ({actual} > {max})"))
        }
        VaultError::BackendUnavailable(message) => Status::failed_precondition(message),
        VaultError::Io(message) => Status::internal(format!("{operation} failed: {message}")),
    }
}

#[allow(clippy::result_large_err)]
fn parse_vault_scope(raw: &str) -> Result<VaultScope, Status> {
    raw.parse::<VaultScope>()
        .map_err(|error| Status::invalid_argument(format!("invalid vault scope: {error}")))
}

#[allow(clippy::result_large_err)]
fn enforce_vault_scope_access(scope: &VaultScope, context: &RequestContext) -> Result<(), Status> {
    match scope {
        VaultScope::Global => Ok(()),
        VaultScope::Principal { principal_id } => {
            if principal_id == &context.principal {
                Ok(())
            } else {
                Err(Status::permission_denied(
                    "vault principal scope must match authenticated principal context",
                ))
            }
        }
        VaultScope::Channel { channel_name, account_id } => {
            let context_channel = context.channel.as_deref().ok_or_else(|| {
                Status::permission_denied(
                    "vault channel scope requires authenticated channel context",
                )
            })?;
            let expected_with_account = format!("{channel_name}:{account_id}");
            if context_channel == expected_with_account {
                Ok(())
            } else {
                Err(Status::permission_denied(
                    "vault channel scope must match authenticated channel context",
                ))
            }
        }
        VaultScope::Skill { .. } => Err(Status::permission_denied(
            "vault skill scope is not allowed over external RPC context",
        )),
    }
}

fn vault_secret_metadata_message(
    metadata: &VaultSecretMetadata,
) -> gateway_v1::VaultSecretMetadata {
    gateway_v1::VaultSecretMetadata {
        scope: metadata.scope.to_string(),
        key: metadata.key.clone(),
        created_at_unix_ms: metadata.created_at_unix_ms,
        updated_at_unix_ms: metadata.updated_at_unix_ms,
        value_bytes: metadata.value_bytes as u32,
    }
}

fn memory_search_cache_key(request: &MemorySearchRequest) -> String {
    json!({
        "principal": request.principal,
        "channel": request.channel,
        "session_id": request.session_id,
        "query": request.query,
        "top_k": request.top_k,
        "min_score": request.min_score,
        "tags": request.tags,
        "sources": request.sources.iter().map(|source| source.as_str()).collect::<Vec<_>>(),
    })
    .to_string()
}

fn build_tool_approval_subject_id(
    tool_name: &str,
    skill_context: Option<&ToolSkillContext>,
) -> String {
    if let Some(skill_context) = skill_context {
        format!("tool:{tool_name}|skill:{}", skill_context.skill_id)
    } else {
        format!("tool:{tool_name}")
    }
}

fn tool_approval_cache_key_prefix(context: &RequestContext, session_id: &str) -> String {
    format!(
        "principal={}|device_id={}|channel={}|session={}|",
        context.principal,
        context.device_id,
        context.channel.as_deref().unwrap_or_default(),
        session_id
    )
}

fn tool_approval_cache_key(context: &RequestContext, session_id: &str, subject_id: &str) -> String {
    format!("{}subject={subject_id}", tool_approval_cache_key_prefix(context, session_id))
}

#[allow(clippy::result_large_err)]
fn require_supported_version(v: u32) -> Result<(), Status> {
    if v != CANONICAL_PROTOCOL_MAJOR {
        return Err(Status::failed_precondition("unsupported protocol major version"));
    }
    Ok(())
}

#[allow(clippy::result_large_err)]
fn authorize_cron_action(principal: &str, action: &str, resource: &str) -> Result<(), Status> {
    let evaluation = evaluate_with_config(
        &PolicyRequest {
            principal: principal.to_owned(),
            action: action.to_owned(),
            resource: resource.to_owned(),
        },
        &PolicyEvaluationConfig::default(),
    )
    .map_err(|error| Status::internal(format!("failed to evaluate cron policy: {error}")))?;
    match evaluation.decision {
        PolicyDecision::Allow => Ok(()),
        PolicyDecision::DenyByDefault { reason } => Err(Status::permission_denied(format!(
            "policy denied action '{action}' on '{resource}': {reason}"
        ))),
    }
}

#[allow(clippy::result_large_err)]
fn authorize_message_action(
    principal: &str,
    action: &str,
    resource: &str,
    channel: Option<&str>,
    _session_id: Option<&str>,
    _run_id: Option<&str>,
) -> Result<(), Status> {
    let evaluation = evaluate_with_context(
        &PolicyRequest {
            principal: principal.to_owned(),
            action: action.to_owned(),
            resource: resource.to_owned(),
        },
        &PolicyRequestContext {
            // Message-routing policy currently gates by principal/channel semantics only.
            // Keep session/run correlation identifiers out of policy error surfaces.
            channel: channel.map(str::to_owned),
            ..PolicyRequestContext::default()
        },
        &PolicyEvaluationConfig::default(),
    )
    .map_err(|error| {
        Status::internal(format!("failed to evaluate message routing policy: {error}"))
    })?;
    match evaluation.decision {
        PolicyDecision::Allow => Ok(()),
        PolicyDecision::DenyByDefault { reason } => Err(Status::permission_denied(format!(
            "policy denied action '{action}' on '{resource}': {reason}"
        ))),
    }
}

#[allow(clippy::result_large_err)]
fn authorize_memory_action(principal: &str, action: &str, resource: &str) -> Result<(), Status> {
    let evaluation = evaluate_with_config(
        &PolicyRequest {
            principal: principal.to_owned(),
            action: action.to_owned(),
            resource: resource.to_owned(),
        },
        &PolicyEvaluationConfig::default(),
    )
    .map_err(|error| Status::internal(format!("failed to evaluate memory policy: {error}")))?;
    match evaluation.decision {
        PolicyDecision::Allow => Ok(()),
        PolicyDecision::DenyByDefault { reason } => Err(Status::permission_denied(format!(
            "policy denied action '{action}' on '{resource}': {reason}"
        ))),
    }
}

#[allow(clippy::result_large_err)]
fn authorize_vault_action(principal: &str, action: &str, resource: &str) -> Result<(), Status> {
    let evaluation = evaluate_with_config(
        &PolicyRequest {
            principal: principal.to_owned(),
            action: action.to_owned(),
            resource: resource.to_owned(),
        },
        &PolicyEvaluationConfig::default(),
    )
    .map_err(|error| Status::internal(format!("failed to evaluate vault policy: {error}")))?;
    match evaluation.decision {
        PolicyDecision::Allow => Ok(()),
        PolicyDecision::DenyByDefault { reason } => Err(Status::permission_denied(format!(
            "policy denied action '{action}' on '{resource}': {reason}"
        ))),
    }
}

#[derive(Clone, Copy)]
enum SensitiveServiceRole {
    AdminOnly,
    AdminOrSystem,
}

fn principal_has_sensitive_service_role(principal: &str, role: SensitiveServiceRole) -> bool {
    let normalized_principal = principal.to_ascii_lowercase();
    match role {
        SensitiveServiceRole::AdminOnly => normalized_principal.starts_with("admin:"),
        SensitiveServiceRole::AdminOrSystem => {
            normalized_principal.starts_with("admin:")
                || normalized_principal.starts_with("system:")
        }
    }
}

#[allow(clippy::result_large_err)]
fn authorize_agent_management_action(
    principal: &str,
    action: &str,
    resource: &str,
) -> Result<(), Status> {
    let evaluation = evaluate_with_config(
        &PolicyRequest {
            principal: principal.to_owned(),
            action: action.to_owned(),
            resource: resource.to_owned(),
        },
        &PolicyEvaluationConfig::default(),
    )
    .map_err(|error| Status::internal(format!("failed to evaluate agent policy: {error}")))?;
    if principal_has_sensitive_service_role(principal, SensitiveServiceRole::AdminOnly) {
        return Ok(());
    }
    let reason = match evaluation.decision {
        PolicyDecision::Allow => {
            "agent management requires admin principal prefix 'admin:'".to_owned()
        }
        PolicyDecision::DenyByDefault { reason } => reason,
    };
    Err(Status::permission_denied(format!(
        "policy denied action '{action}' on '{resource}': {reason}"
    )))
}

#[allow(clippy::result_large_err)]
fn authorize_auth_profile_action(
    principal: &str,
    action: &str,
    resource: &str,
) -> Result<(), Status> {
    let evaluation = evaluate_with_config(
        &PolicyRequest {
            principal: principal.to_owned(),
            action: action.to_owned(),
            resource: resource.to_owned(),
        },
        &PolicyEvaluationConfig::default(),
    )
    .map_err(|error| {
        Status::internal(format!("failed to evaluate auth profile policy: {error}"))
    })?;
    if principal_has_sensitive_service_role(principal, SensitiveServiceRole::AdminOrSystem) {
        return Ok(());
    }
    let reason = match evaluation.decision {
        PolicyDecision::Allow => {
            "auth profile management requires admin/system principal prefix".to_owned()
        }
        PolicyDecision::DenyByDefault { reason } => reason,
    };
    Err(Status::permission_denied(format!(
        "policy denied action '{action}' on '{resource}': {reason}"
    )))
}

#[allow(clippy::result_large_err)]
fn authorize_approvals_action(principal: &str, action: &str, resource: &str) -> Result<(), Status> {
    let evaluation = evaluate_with_config(
        &PolicyRequest {
            principal: principal.to_owned(),
            action: action.to_owned(),
            resource: resource.to_owned(),
        },
        &PolicyEvaluationConfig::default(),
    )
    .map_err(|error| Status::internal(format!("failed to evaluate approvals policy: {error}")))?;
    if principal_has_sensitive_service_role(principal, SensitiveServiceRole::AdminOrSystem) {
        return Ok(());
    }
    let reason = match evaluation.decision {
        PolicyDecision::Allow => "approvals APIs require admin/system principal prefix".to_owned(),
        PolicyDecision::DenyByDefault { reason } => reason,
    };
    Err(Status::permission_denied(format!(
        "policy denied action '{action}' on '{resource}': {reason}"
    )))
}

fn map_auth_profile_error(error: AuthProfileError) -> Status {
    match error {
        AuthProfileError::InvalidField { .. } | AuthProfileError::InvalidPath { .. } => {
            Status::invalid_argument(error.to_string())
        }
        AuthProfileError::UnsupportedVersion(_) => Status::failed_precondition(error.to_string()),
        AuthProfileError::ProfileNotFound(_) => Status::not_found(error.to_string()),
        AuthProfileError::RegistryLimitExceeded => Status::resource_exhausted(error.to_string()),
        AuthProfileError::ReadRegistry { .. }
        | AuthProfileError::ParseRegistry { .. }
        | AuthProfileError::WriteRegistry { .. }
        | AuthProfileError::SerializeRegistry(_)
        | AuthProfileError::LockPoisoned
        | AuthProfileError::InvalidSystemTime(_) => Status::internal(error.to_string()),
    }
}

#[allow(clippy::result_large_err)]
fn auth_list_filter_from_proto(
    payload: auth_v1::ListAuthProfilesRequest,
) -> Result<AuthProfileListFilter, Status> {
    let provider_kind = auth_v1::AuthProviderKind::try_from(payload.provider_kind)
        .unwrap_or(auth_v1::AuthProviderKind::Unspecified);
    let provider = match provider_kind {
        auth_v1::AuthProviderKind::Unspecified => None,
        auth_v1::AuthProviderKind::Custom => {
            let custom_name = payload.provider_custom_name.trim();
            if custom_name.is_empty() {
                return Err(Status::invalid_argument(
                    "provider_custom_name is required when provider_kind=custom",
                ));
            }
            Some(AuthProvider {
                kind: AuthProviderKind::Custom,
                custom_name: Some(custom_name.to_owned()),
            })
        }
        _ => Some(AuthProvider {
            kind: auth_provider_kind_from_proto(provider_kind)?,
            custom_name: None,
        }),
    };
    let scope_kind = auth_v1::AuthScopeKind::try_from(payload.scope_kind)
        .unwrap_or(auth_v1::AuthScopeKind::Unspecified);
    let scope = match scope_kind {
        auth_v1::AuthScopeKind::Unspecified => None,
        auth_v1::AuthScopeKind::Global => Some(AuthScopeFilter::Global),
        auth_v1::AuthScopeKind::Agent => {
            let agent_id = payload.scope_agent_id.trim();
            if agent_id.is_empty() {
                return Err(Status::invalid_argument(
                    "scope_agent_id is required when scope_kind=agent",
                ));
            }
            Some(AuthScopeFilter::Agent { agent_id: agent_id.to_owned() })
        }
    };
    Ok(AuthProfileListFilter {
        after_profile_id: non_empty(payload.after_profile_id),
        limit: if payload.limit == 0 { None } else { Some(payload.limit as usize) },
        provider,
        scope,
    })
}

#[allow(clippy::result_large_err)]
fn auth_set_request_from_proto(
    profile: auth_v1::AuthProfile,
) -> Result<AuthProfileSetRequest, Status> {
    let provider = auth_provider_from_proto(
        profile.provider.ok_or_else(|| Status::invalid_argument("profile.provider is required"))?,
    )?;
    let scope = auth_scope_from_proto(
        profile.scope.ok_or_else(|| Status::invalid_argument("profile.scope is required"))?,
    )?;
    let credential = auth_credential_from_proto(
        profile
            .credential
            .ok_or_else(|| Status::invalid_argument("profile.credential is required"))?,
    )?;
    Ok(AuthProfileSetRequest {
        profile_id: profile.profile_id,
        provider,
        profile_name: profile.profile_name,
        scope,
        credential,
    })
}

#[allow(clippy::result_large_err)]
fn auth_provider_from_proto(provider: auth_v1::AuthProvider) -> Result<AuthProvider, Status> {
    let kind = auth_v1::AuthProviderKind::try_from(provider.kind)
        .unwrap_or(auth_v1::AuthProviderKind::Unspecified);
    if kind == auth_v1::AuthProviderKind::Unspecified {
        return Err(Status::invalid_argument("profile.provider.kind must be specified"));
    }
    if kind == auth_v1::AuthProviderKind::Custom {
        let custom_name = provider.custom_name.trim();
        if custom_name.is_empty() {
            return Err(Status::invalid_argument(
                "profile.provider.custom_name is required for custom providers",
            ));
        }
        return Ok(AuthProvider {
            kind: AuthProviderKind::Custom,
            custom_name: Some(custom_name.to_owned()),
        });
    }
    Ok(AuthProvider { kind: auth_provider_kind_from_proto(kind)?, custom_name: None })
}

#[allow(clippy::result_large_err)]
fn auth_scope_from_proto(scope: auth_v1::AuthScope) -> Result<AuthProfileScope, Status> {
    match auth_v1::AuthScopeKind::try_from(scope.kind)
        .unwrap_or(auth_v1::AuthScopeKind::Unspecified)
    {
        auth_v1::AuthScopeKind::Global => Ok(AuthProfileScope::Global),
        auth_v1::AuthScopeKind::Agent => {
            let agent_id = scope.agent_id.trim();
            if agent_id.is_empty() {
                return Err(Status::invalid_argument(
                    "profile.scope.agent_id is required for agent scope",
                ));
            }
            Ok(AuthProfileScope::Agent { agent_id: agent_id.to_owned() })
        }
        auth_v1::AuthScopeKind::Unspecified => {
            Err(Status::invalid_argument("profile.scope.kind must be specified"))
        }
    }
}

#[allow(clippy::result_large_err)]
fn auth_credential_from_proto(
    credential: auth_v1::AuthCredential,
) -> Result<AuthCredential, Status> {
    match credential.kind {
        Some(auth_v1::auth_credential::Kind::ApiKey(value)) => {
            Ok(AuthCredential::ApiKey { api_key_vault_ref: value.api_key_vault_ref })
        }
        Some(auth_v1::auth_credential::Kind::Oauth(value)) => Ok(AuthCredential::Oauth {
            access_token_vault_ref: value.access_token_vault_ref,
            refresh_token_vault_ref: value.refresh_token_vault_ref,
            token_endpoint: value.token_endpoint,
            client_id: non_empty(value.client_id),
            client_secret_vault_ref: non_empty(value.client_secret_vault_ref),
            scopes: value.scopes,
            expires_at_unix_ms: if value.expires_at_unix_ms > 0 {
                Some(value.expires_at_unix_ms)
            } else {
                None
            },
            refresh_state: if let Some(refresh_state) = value.refresh_state {
                palyra_auth::OAuthRefreshState {
                    failure_count: refresh_state.failure_count,
                    last_error: non_empty(refresh_state.last_error),
                    last_attempt_unix_ms: if refresh_state.last_attempt_unix_ms > 0 {
                        Some(refresh_state.last_attempt_unix_ms)
                    } else {
                        None
                    },
                    last_success_unix_ms: if refresh_state.last_success_unix_ms > 0 {
                        Some(refresh_state.last_success_unix_ms)
                    } else {
                        None
                    },
                    next_allowed_refresh_unix_ms: if refresh_state.next_allowed_refresh_unix_ms > 0
                    {
                        Some(refresh_state.next_allowed_refresh_unix_ms)
                    } else {
                        None
                    },
                }
            } else {
                palyra_auth::OAuthRefreshState::default()
            },
        }),
        None => Err(Status::invalid_argument("profile.credential.kind is required")),
    }
}

#[allow(clippy::result_large_err)]
fn auth_provider_kind_from_proto(
    kind: auth_v1::AuthProviderKind,
) -> Result<AuthProviderKind, Status> {
    match kind {
        auth_v1::AuthProviderKind::Openai => Ok(AuthProviderKind::Openai),
        auth_v1::AuthProviderKind::Anthropic => Ok(AuthProviderKind::Anthropic),
        auth_v1::AuthProviderKind::Telegram => Ok(AuthProviderKind::Telegram),
        auth_v1::AuthProviderKind::Slack => Ok(AuthProviderKind::Slack),
        auth_v1::AuthProviderKind::Discord => Ok(AuthProviderKind::Discord),
        auth_v1::AuthProviderKind::Webhook => Ok(AuthProviderKind::Webhook),
        auth_v1::AuthProviderKind::Custom => Ok(AuthProviderKind::Custom),
        auth_v1::AuthProviderKind::Unspecified => {
            Err(Status::invalid_argument("provider kind must be specified"))
        }
    }
}

fn auth_profile_to_proto(profile: &AuthProfileRecord) -> auth_v1::AuthProfile {
    auth_v1::AuthProfile {
        profile_id: profile.profile_id.clone(),
        provider: Some(auth_provider_to_proto(&profile.provider)),
        profile_name: profile.profile_name.clone(),
        scope: Some(auth_scope_to_proto(&profile.scope)),
        credential: Some(auth_credential_to_proto(&profile.credential)),
        created_at_unix_ms: profile.created_at_unix_ms,
        updated_at_unix_ms: profile.updated_at_unix_ms,
    }
}

fn auth_provider_to_proto(provider: &AuthProvider) -> auth_v1::AuthProvider {
    auth_v1::AuthProvider {
        kind: match provider.kind {
            AuthProviderKind::Openai => auth_v1::AuthProviderKind::Openai as i32,
            AuthProviderKind::Anthropic => auth_v1::AuthProviderKind::Anthropic as i32,
            AuthProviderKind::Telegram => auth_v1::AuthProviderKind::Telegram as i32,
            AuthProviderKind::Slack => auth_v1::AuthProviderKind::Slack as i32,
            AuthProviderKind::Discord => auth_v1::AuthProviderKind::Discord as i32,
            AuthProviderKind::Webhook => auth_v1::AuthProviderKind::Webhook as i32,
            AuthProviderKind::Custom => auth_v1::AuthProviderKind::Custom as i32,
        },
        custom_name: provider.custom_name.clone().unwrap_or_default(),
    }
}

fn auth_scope_to_proto(scope: &AuthProfileScope) -> auth_v1::AuthScope {
    match scope {
        AuthProfileScope::Global => auth_v1::AuthScope {
            kind: auth_v1::AuthScopeKind::Global as i32,
            agent_id: String::new(),
        },
        AuthProfileScope::Agent { agent_id } => auth_v1::AuthScope {
            kind: auth_v1::AuthScopeKind::Agent as i32,
            agent_id: agent_id.clone(),
        },
    }
}

fn auth_credential_to_proto(credential: &AuthCredential) -> auth_v1::AuthCredential {
    match credential {
        AuthCredential::ApiKey { api_key_vault_ref } => auth_v1::AuthCredential {
            kind: Some(auth_v1::auth_credential::Kind::ApiKey(auth_v1::ApiKeyCredential {
                api_key_vault_ref: api_key_vault_ref.clone(),
            })),
        },
        AuthCredential::Oauth {
            access_token_vault_ref,
            refresh_token_vault_ref,
            token_endpoint,
            client_id,
            client_secret_vault_ref,
            scopes,
            expires_at_unix_ms,
            refresh_state,
        } => auth_v1::AuthCredential {
            kind: Some(auth_v1::auth_credential::Kind::Oauth(auth_v1::OAuthCredential {
                access_token_vault_ref: access_token_vault_ref.clone(),
                refresh_token_vault_ref: refresh_token_vault_ref.clone(),
                token_endpoint: token_endpoint.clone(),
                client_id: client_id.clone().unwrap_or_default(),
                client_secret_vault_ref: client_secret_vault_ref.clone().unwrap_or_default(),
                scopes: scopes.clone(),
                expires_at_unix_ms: expires_at_unix_ms.unwrap_or_default(),
                refresh_state: Some(auth_v1::OAuthRefreshState {
                    failure_count: refresh_state.failure_count,
                    last_error: refresh_state.last_error.clone().unwrap_or_default(),
                    last_attempt_unix_ms: refresh_state.last_attempt_unix_ms.unwrap_or_default(),
                    last_success_unix_ms: refresh_state.last_success_unix_ms.unwrap_or_default(),
                    next_allowed_refresh_unix_ms: refresh_state
                        .next_allowed_refresh_unix_ms
                        .unwrap_or_default(),
                }),
            })),
        },
    }
}

fn auth_health_summary_to_proto(summary: &AuthHealthSummary) -> auth_v1::AuthHealthSummary {
    auth_v1::AuthHealthSummary {
        total: summary.total,
        ok: summary.ok,
        expiring: summary.expiring,
        expired: summary.expired,
        missing: summary.missing,
        static_count: summary.static_count,
    }
}

fn auth_expiry_distribution_to_proto(
    distribution: &AuthExpiryDistribution,
) -> auth_v1::AuthExpiryDistribution {
    auth_v1::AuthExpiryDistribution {
        expired: distribution.expired,
        under_5m: distribution.under_5m,
        between_5m_15m: distribution.between_5m_15m,
        between_15m_60m: distribution.between_15m_60m,
        between_1h_24h: distribution.between_1h_24h,
        over_24h: distribution.over_24h,
        unknown: distribution.unknown,
        static_count: distribution.static_count,
        missing: distribution.missing,
    }
}

fn auth_health_profile_to_proto(
    health: &palyra_auth::AuthProfileHealthRecord,
) -> auth_v1::AuthProfileHealth {
    auth_v1::AuthProfileHealth {
        profile_id: health.profile_id.clone(),
        provider: health.provider.clone(),
        profile_name: health.profile_name.clone(),
        scope: health.scope.clone(),
        credential_type: match health.credential_type {
            AuthCredentialType::ApiKey => "api_key".to_owned(),
            AuthCredentialType::Oauth => "oauth".to_owned(),
        },
        state: auth_health_state_to_proto(health.state),
        reason: health.reason.clone(),
        expires_at_unix_ms: health.expires_at_unix_ms.unwrap_or_default(),
    }
}

fn auth_health_state_to_proto(state: AuthProfileHealthState) -> i32 {
    match state {
        AuthProfileHealthState::Ok => auth_v1::AuthHealthState::Ok as i32,
        AuthProfileHealthState::Expiring => auth_v1::AuthHealthState::Expiring as i32,
        AuthProfileHealthState::Expired => auth_v1::AuthHealthState::Expired as i32,
        AuthProfileHealthState::Missing => auth_v1::AuthHealthState::Missing as i32,
        AuthProfileHealthState::Static => auth_v1::AuthHealthState::Static as i32,
    }
}

fn auth_refresh_metrics_to_proto(
    metrics: &AuthRefreshMetricsSnapshot,
) -> auth_v1::AuthRefreshMetrics {
    auth_v1::AuthRefreshMetrics {
        attempts: metrics.attempts,
        successes: metrics.successes,
        failures: metrics.failures,
        by_provider: metrics
            .by_provider
            .iter()
            .map(|provider| auth_v1::ProviderRefreshMetric {
                provider: provider.provider.clone(),
                attempts: provider.attempts,
                successes: provider.successes,
                failures: provider.failures,
            })
            .collect(),
    }
}

fn normalize_vault_ref_literal(scope: &VaultScope, key: &str) -> String {
    format!("{scope}/{key}").to_ascii_lowercase()
}

fn vault_get_requires_approval(
    scope: &VaultScope,
    key: &str,
    approval_required_refs: &[String],
) -> bool {
    if approval_required_refs.is_empty() {
        return false;
    }
    let candidate = normalize_vault_ref_literal(scope, key);
    approval_required_refs
        .iter()
        .any(|configured| configured.eq_ignore_ascii_case(candidate.as_str()))
}

#[allow(clippy::result_large_err)]
fn enforce_vault_get_approval_policy(
    principal: &str,
    scope: &VaultScope,
    key: &str,
    approval_required_refs: &[String],
    approval_header: Option<&str>,
) -> Result<(), Status> {
    if !vault_get_requires_approval(scope, key, approval_required_refs) {
        return Ok(());
    }
    let approved = match approval_header {
        Some(value) if value.eq_ignore_ascii_case(VAULT_READ_APPROVAL_ALLOW_VALUE) => true,
        Some(_) => {
            return Err(Status::permission_denied(format!(
                "vault read approval header must be '{}' when approval is required",
                VAULT_READ_APPROVAL_ALLOW_VALUE
            )));
        }
        None => false,
    };
    let evaluation = evaluate_with_config(
        &PolicyRequest {
            principal: principal.to_owned(),
            action: "vault.get".to_owned(),
            resource: format!("secrets:{scope}:{key}"),
        },
        &PolicyEvaluationConfig {
            allow_sensitive_tools: approved,
            sensitive_actions: vec!["vault.get".to_owned()],
            ..PolicyEvaluationConfig::default()
        },
    )
    .map_err(|error| {
        Status::internal(format!("failed to evaluate vault approval policy: {error}"))
    })?;
    match evaluation.decision {
        PolicyDecision::Allow => Ok(()),
        PolicyDecision::DenyByDefault { reason } => Err(Status::permission_denied(format!(
            "vault read requires explicit approval for {scope}/{key}: {reason}"
        ))),
    }
}

#[allow(clippy::result_large_err)]
fn current_unix_ms_status() -> Result<i64, Status> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| Status::internal(format!("system time before unix epoch: {error}")))?;
    Ok(elapsed.as_millis() as i64)
}

#[allow(clippy::result_large_err)]
fn validate_cron_job_name(name: String) -> Result<String, Status> {
    let value = name.trim();
    if value.is_empty() {
        return Err(Status::invalid_argument("cron job name cannot be empty"));
    }
    if value.len() > MAX_CRON_JOB_NAME_BYTES {
        return Err(Status::invalid_argument(format!(
            "cron job name exceeds maximum bytes ({} > {MAX_CRON_JOB_NAME_BYTES})",
            value.len()
        )));
    }
    Ok(value.to_owned())
}

#[allow(clippy::result_large_err)]
fn validate_cron_job_prompt(prompt: String) -> Result<String, Status> {
    let value = prompt.trim();
    if value.is_empty() {
        return Err(Status::invalid_argument("cron job prompt cannot be empty"));
    }
    if value.len() > MAX_CRON_PROMPT_BYTES {
        return Err(Status::invalid_argument(format!(
            "cron job prompt exceeds maximum bytes ({} > {MAX_CRON_PROMPT_BYTES})",
            value.len()
        )));
    }
    Ok(value.to_owned())
}

#[allow(clippy::result_large_err)]
fn validate_cron_jitter_ms(jitter_ms: u64) -> Result<u64, Status> {
    if jitter_ms > MAX_CRON_JITTER_MS {
        return Err(Status::invalid_argument(format!(
            "jitter_ms exceeds maximum ({MAX_CRON_JITTER_MS})"
        )));
    }
    Ok(jitter_ms)
}

#[allow(clippy::result_large_err)]
fn validate_cron_job_owner_principal(
    authenticated_principal: &str,
    requested_owner_principal: String,
) -> Result<String, Status> {
    match non_empty(requested_owner_principal) {
        Some(owner_principal) if owner_principal == authenticated_principal => Ok(owner_principal),
        Some(_) => {
            Err(Status::permission_denied("owner_principal must match authenticated principal"))
        }
        None => Ok(authenticated_principal.to_owned()),
    }
}

#[allow(clippy::result_large_err)]
fn validate_cron_job_owner_principal_for_update(
    authenticated_principal: &str,
    requested_owner_principal: String,
) -> Result<String, Status> {
    let owner_principal = non_empty(requested_owner_principal)
        .ok_or_else(|| Status::invalid_argument("owner_principal cannot be empty"))?;
    if owner_principal != authenticated_principal {
        return Err(Status::permission_denied(
            "owner_principal must match authenticated principal",
        ));
    }
    Ok(owner_principal)
}

#[allow(clippy::result_large_err)]
fn validate_cron_job_channel_context(
    context_channel: Option<&str>,
    requested_channel: Option<&str>,
) -> Result<(), Status> {
    let Some(requested_channel) = requested_channel else {
        return Ok(());
    };
    let Some(context_channel) = context_channel else {
        return Ok(());
    };
    if context_channel != requested_channel && requested_channel != "system:cron" {
        return Err(Status::permission_denied(
            "cron channel must match authenticated channel context",
        ));
    }
    Ok(())
}

#[allow(clippy::result_large_err)]
fn resolve_cron_job_channel_for_create(
    context_channel: Option<&str>,
    requested_channel: String,
) -> Result<String, Status> {
    let requested_channel = non_empty(requested_channel);
    validate_cron_job_channel_context(context_channel, requested_channel.as_deref())?;
    Ok(requested_channel
        .or_else(|| context_channel.map(str::to_owned))
        .unwrap_or_else(|| "system:cron".to_owned()))
}

#[allow(clippy::result_large_err)]
fn validate_cron_job_channel_for_update(
    context_channel: Option<&str>,
    requested_channel: String,
) -> Result<Option<String>, Status> {
    let requested_channel = non_empty(requested_channel);
    validate_cron_job_channel_context(context_channel, requested_channel.as_deref())?;
    Ok(requested_channel)
}

#[allow(clippy::result_large_err)]
fn enforce_cron_job_owner(
    authenticated_principal: &str,
    job_owner_principal: &str,
) -> Result<(), Status> {
    if authenticated_principal == job_owner_principal {
        return Ok(());
    }
    Err(Status::permission_denied("cron job owner mismatch for authenticated principal"))
}

#[allow(clippy::result_large_err)]
fn cron_concurrency_from_proto(raw: i32) -> Result<CronConcurrencyPolicy, Status> {
    match cron_v1::ConcurrencyPolicy::try_from(raw)
        .unwrap_or(cron_v1::ConcurrencyPolicy::Unspecified)
    {
        cron_v1::ConcurrencyPolicy::Forbid => Ok(CronConcurrencyPolicy::Forbid),
        cron_v1::ConcurrencyPolicy::Replace => Ok(CronConcurrencyPolicy::Replace),
        cron_v1::ConcurrencyPolicy::QueueOne => Ok(CronConcurrencyPolicy::QueueOne),
        cron_v1::ConcurrencyPolicy::Unspecified => {
            Err(Status::invalid_argument("concurrency_policy must be specified"))
        }
    }
}

fn cron_concurrency_to_proto(policy: CronConcurrencyPolicy) -> i32 {
    match policy {
        CronConcurrencyPolicy::Forbid => cron_v1::ConcurrencyPolicy::Forbid as i32,
        CronConcurrencyPolicy::Replace => cron_v1::ConcurrencyPolicy::Replace as i32,
        CronConcurrencyPolicy::QueueOne => cron_v1::ConcurrencyPolicy::QueueOne as i32,
    }
}

#[allow(clippy::result_large_err)]
fn cron_misfire_from_proto(raw: i32) -> Result<crate::journal::CronMisfirePolicy, Status> {
    match cron_v1::MisfirePolicy::try_from(raw).unwrap_or(cron_v1::MisfirePolicy::Unspecified) {
        cron_v1::MisfirePolicy::Skip => Ok(crate::journal::CronMisfirePolicy::Skip),
        cron_v1::MisfirePolicy::CatchUp => Ok(crate::journal::CronMisfirePolicy::CatchUp),
        cron_v1::MisfirePolicy::Unspecified => {
            Err(Status::invalid_argument("misfire_policy must be specified"))
        }
    }
}

fn cron_misfire_to_proto(policy: crate::journal::CronMisfirePolicy) -> i32 {
    match policy {
        crate::journal::CronMisfirePolicy::Skip => cron_v1::MisfirePolicy::Skip as i32,
        crate::journal::CronMisfirePolicy::CatchUp => cron_v1::MisfirePolicy::CatchUp as i32,
    }
}

#[allow(clippy::result_large_err)]
fn cron_retry_from_proto(
    value: Option<cron_v1::RetryPolicy>,
) -> Result<crate::journal::CronRetryPolicy, Status> {
    let value = value.ok_or_else(|| Status::invalid_argument("retry_policy is required"))?;
    let max_attempts = value.max_attempts.clamp(1, 16);
    let backoff_ms = value.backoff_ms.clamp(1, 60_000);
    Ok(crate::journal::CronRetryPolicy { max_attempts, backoff_ms })
}

#[allow(clippy::result_large_err)]
fn cron_job_message(job: &CronJobRecord) -> Result<cron_v1::Job, Status> {
    let schedule = schedule_to_proto(job.schedule_type, job.schedule_payload_json.as_str())?;
    Ok(cron_v1::Job {
        v: CANONICAL_PROTOCOL_MAJOR,
        job_id: Some(common_v1::CanonicalId { ulid: job.job_id.clone() }),
        name: job.name.clone(),
        prompt: job.prompt.clone(),
        owner_principal: job.owner_principal.clone(),
        channel: job.channel.clone(),
        session_key: job.session_key.clone().unwrap_or_default(),
        session_label: job.session_label.clone().unwrap_or_default(),
        schedule: Some(schedule),
        enabled: job.enabled,
        concurrency_policy: cron_concurrency_to_proto(job.concurrency_policy),
        retry_policy: Some(cron_v1::RetryPolicy {
            max_attempts: job.retry_policy.max_attempts,
            backoff_ms: job.retry_policy.backoff_ms,
        }),
        misfire_policy: cron_misfire_to_proto(job.misfire_policy),
        jitter_ms: job.jitter_ms,
        next_run_at_unix_ms: job.next_run_at_unix_ms.unwrap_or_default(),
        last_run_at_unix_ms: job.last_run_at_unix_ms.unwrap_or_default(),
        created_at_unix_ms: job.created_at_unix_ms,
        updated_at_unix_ms: job.updated_at_unix_ms,
    })
}

fn cron_run_message(run: &CronRunRecord) -> cron_v1::JobRun {
    cron_v1::JobRun {
        v: CANONICAL_PROTOCOL_MAJOR,
        run_id: Some(common_v1::CanonicalId { ulid: run.run_id.clone() }),
        job_id: Some(common_v1::CanonicalId { ulid: run.job_id.clone() }),
        session_id: run
            .session_id
            .as_ref()
            .map(|value| common_v1::CanonicalId { ulid: value.clone() }),
        orchestrator_run_id: run
            .orchestrator_run_id
            .as_ref()
            .map(|value| common_v1::CanonicalId { ulid: value.clone() }),
        attempt: run.attempt,
        started_at_unix_ms: run.started_at_unix_ms,
        finished_at_unix_ms: run.finished_at_unix_ms.unwrap_or_default(),
        status: cron_run_status_to_proto(run.status),
        error_kind: run.error_kind.clone().unwrap_or_default(),
        error_message_redacted: run.error_message_redacted.clone().unwrap_or_default(),
        model_tokens_in: run.model_tokens_in,
        model_tokens_out: run.model_tokens_out,
        tool_calls: run.tool_calls,
        tool_denies: run.tool_denies,
    }
}

fn cron_run_status_to_proto(status: CronRunStatus) -> i32 {
    match status {
        CronRunStatus::Accepted => cron_v1::JobRunStatus::Accepted as i32,
        CronRunStatus::Running => cron_v1::JobRunStatus::Running as i32,
        CronRunStatus::Succeeded => cron_v1::JobRunStatus::Succeeded as i32,
        CronRunStatus::Failed => cron_v1::JobRunStatus::Failed as i32,
        CronRunStatus::Skipped => cron_v1::JobRunStatus::Skipped as i32,
        CronRunStatus::Denied => cron_v1::JobRunStatus::Denied as i32,
    }
}

#[allow(clippy::result_large_err)]
fn resolve_memory_channel_scope(
    context_channel: Option<&str>,
    requested_channel: Option<String>,
) -> Result<Option<String>, Status> {
    let normalized_requested = requested_channel.and_then(non_empty);
    if let (Some(context_channel), Some(requested_channel)) =
        (context_channel, normalized_requested.as_deref())
    {
        if context_channel != requested_channel {
            return Err(Status::permission_denied(
                "memory scope channel must match authenticated channel context",
            ));
        }
    }
    Ok(normalized_requested.or_else(|| context_channel.map(str::to_owned)))
}

#[allow(clippy::result_large_err)]
fn memory_source_from_proto(raw: i32) -> Result<MemorySource, Status> {
    match memory_v1::MemorySource::try_from(raw).unwrap_or(memory_v1::MemorySource::Unspecified) {
        memory_v1::MemorySource::TapeUserMessage => Ok(MemorySource::TapeUserMessage),
        memory_v1::MemorySource::TapeToolResult => Ok(MemorySource::TapeToolResult),
        memory_v1::MemorySource::Summary => Ok(MemorySource::Summary),
        memory_v1::MemorySource::Manual => Ok(MemorySource::Manual),
        memory_v1::MemorySource::Import => Ok(MemorySource::Import),
        memory_v1::MemorySource::Unspecified => {
            Err(Status::invalid_argument("memory source must be specified"))
        }
    }
}

fn memory_source_to_proto(source: MemorySource) -> i32 {
    match source {
        MemorySource::TapeUserMessage => memory_v1::MemorySource::TapeUserMessage as i32,
        MemorySource::TapeToolResult => memory_v1::MemorySource::TapeToolResult as i32,
        MemorySource::Summary => memory_v1::MemorySource::Summary as i32,
        MemorySource::Manual => memory_v1::MemorySource::Manual as i32,
        MemorySource::Import => memory_v1::MemorySource::Import as i32,
    }
}

#[allow(clippy::result_large_err)]
fn enforce_memory_item_scope(
    item: &MemoryItemRecord,
    principal: &str,
    channel: Option<&str>,
) -> Result<(), Status> {
    if item.principal != principal {
        return Err(Status::permission_denied("memory item principal does not match context"));
    }
    match (channel, item.channel.as_deref()) {
        (Some(context_channel), Some(item_channel)) => {
            if context_channel != item_channel {
                return Err(Status::permission_denied(
                    "memory item channel does not match context",
                ));
            }
        }
        (None, Some(_)) => {
            return Err(Status::permission_denied(
                "memory item is channel-scoped and requires authenticated channel context",
            ));
        }
        _ => {}
    }
    Ok(())
}

fn redact_memory_text_for_output(raw: &str) -> String {
    if raw.is_empty() {
        return String::new();
    }

    let payload = json!({ "value": raw });
    let redacted_payload = match crate::journal::redact_payload_json(payload.to_string().as_bytes())
    {
        Ok(redacted) => redacted,
        Err(_) => return raw.to_owned(),
    };
    match serde_json::from_str::<Value>(redacted_payload.as_str()) {
        Ok(Value::Object(fields)) => fields
            .get("value")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| raw.to_owned()),
        _ => raw.to_owned(),
    }
}

fn memory_item_message(item: &MemoryItemRecord) -> memory_v1::MemoryItem {
    memory_v1::MemoryItem {
        v: CANONICAL_PROTOCOL_MAJOR,
        memory_id: Some(common_v1::CanonicalId { ulid: item.memory_id.clone() }),
        principal: item.principal.clone(),
        channel: item.channel.clone().unwrap_or_default(),
        session_id: item
            .session_id
            .as_ref()
            .map(|value| common_v1::CanonicalId { ulid: value.clone() }),
        source: memory_source_to_proto(item.source),
        content_text: redact_memory_text_for_output(item.content_text.as_str()),
        content_hash: item.content_hash.clone(),
        tags: item.tags.clone(),
        confidence: item.confidence.unwrap_or_default(),
        ttl_unix_ms: item.ttl_unix_ms.unwrap_or_default(),
        created_at_unix_ms: item.created_at_unix_ms,
        updated_at_unix_ms: item.updated_at_unix_ms,
    }
}

fn memory_search_hit_message(
    hit: &MemorySearchHit,
    include_score_breakdown: bool,
) -> memory_v1::MemorySearchHit {
    memory_v1::MemorySearchHit {
        item: Some(memory_item_message(&hit.item)),
        snippet: redact_memory_text_for_output(hit.snippet.as_str()),
        score: hit.score,
        breakdown: if include_score_breakdown {
            Some(memory_v1::MemoryScoreBreakdown {
                lexical_score: hit.breakdown.lexical_score,
                vector_score: hit.breakdown.vector_score,
                recency_score: hit.breakdown.recency_score,
                final_score: hit.breakdown.final_score,
            })
        } else {
            None
        },
    }
}

fn apply_tool_approval_outcome(
    mut decision: crate::tool_protocol::ToolDecision,
    tool_name: &str,
    approval: Option<&ToolApprovalOutcome>,
) -> crate::tool_protocol::ToolDecision {
    if !(decision.allowed && decision.approval_required) {
        return decision;
    }

    let Some(approval) = approval else {
        decision.allowed = false;
        decision.reason = format!(
            "{APPROVAL_CHANNEL_UNAVAILABLE_REASON}; tool={tool_name}; original_reason={}",
            decision.reason
        );
        return decision;
    };

    if approval.approved {
        decision.reason = format!(
            "explicit approval granted for tool={tool_name}; approval_reason={}; original_reason={}",
            approval.reason, decision.reason
        );
        return decision;
    }

    decision.allowed = false;
    decision.reason = format!(
        "{APPROVAL_DENIED_REASON}; tool={tool_name}; approval_reason={}; original_reason={}",
        approval.reason, decision.reason
    );
    decision
}

#[allow(clippy::result_large_err)]
async fn await_tool_approval_response(
    stream: &mut Streaming<common_v1::RunStreamRequest>,
    expected_session_id: &str,
    expected_run_id: &str,
    proposal_id: &str,
    approval_id: &str,
) -> Result<ToolApprovalOutcome, Status> {
    while let Some(item) = stream.next().await {
        let message = item.map_err(|error| {
            Status::internal(format!("failed to read approval stream item: {error}"))
        })?;
        if message.v != CANONICAL_PROTOCOL_MAJOR {
            return Err(Status::failed_precondition("unsupported protocol major version"));
        }

        let message_session_id = canonical_id(message.session_id, "session_id")?;
        if message_session_id != expected_session_id {
            return Err(Status::invalid_argument(
                "run stream cannot switch session_id while awaiting tool approval response",
            ));
        }
        let message_run_id = canonical_id(message.run_id, "run_id")?;
        if message_run_id != expected_run_id {
            return Err(Status::invalid_argument(
                "run stream cannot switch run_id while awaiting tool approval response",
            ));
        }
        if message.input.is_some() {
            return Err(Status::invalid_argument(
                "received prompt payload while waiting for tool approval response",
            ));
        }

        let Some(response) = message.tool_approval_response else {
            continue;
        };
        let response_proposal_id =
            canonical_id(response.proposal_id, "tool_approval_response.proposal_id")?;
        if response_proposal_id != proposal_id {
            return Err(Status::invalid_argument(
                "tool approval response proposal_id does not match pending tool proposal",
            ));
        }
        let response_approval_id = if let Some(response_approval_id) =
            response.approval_id.and_then(|value| non_empty(value.ulid))
        {
            validate_canonical_id(response_approval_id.as_str()).map_err(|_| {
                Status::invalid_argument(
                    "tool_approval_response.approval_id must be a canonical ULID",
                )
            })?;
            if response_approval_id != approval_id {
                return Err(Status::invalid_argument(
                    "tool approval response approval_id does not match pending approval record",
                ));
            }
            response_approval_id
        } else {
            approval_id.to_owned()
        };

        let reason = non_empty(response.reason).unwrap_or_else(|| {
            if response.approved {
                "approved_by_client".to_owned()
            } else {
                "denied_by_client".to_owned()
            }
        });
        return Ok(ToolApprovalOutcome {
            approval_id: response_approval_id,
            approved: response.approved,
            reason,
            decision: if response.approved {
                ApprovalDecision::Allow
            } else {
                ApprovalDecision::Deny
            },
            decision_scope: approval_scope_from_proto(response.decision_scope),
            decision_scope_ttl_ms: if response.decision_scope_ttl_ms > 0 {
                Some(response.decision_scope_ttl_ms)
            } else {
                None
            },
        });
    }

    Ok(ToolApprovalOutcome {
        approval_id: approval_id.to_owned(),
        approved: false,
        reason: APPROVAL_CHANNEL_UNAVAILABLE_REASON.to_owned(),
        decision: ApprovalDecision::Error,
        decision_scope: ApprovalDecisionScope::Once,
        decision_scope_ttl_ms: None,
    })
}

fn map_provider_error(error: ProviderError) -> Status {
    match error {
        ProviderError::CircuitOpen { retry_after_ms } => Status::unavailable(format!(
            "model provider circuit breaker is open; retry after {retry_after_ms}ms"
        )),
        ProviderError::MissingApiKey => {
            Status::failed_precondition("model provider API key is missing")
        }
        ProviderError::VisionUnsupported { provider } => {
            Status::failed_precondition(format!("provider '{provider}' does not support vision"))
        }
        ProviderError::RequestFailed { message, retryable, retry_count } => {
            let status_message = format!(
                "model provider request failed after {retry_count} retries (retryable={retryable}): {message}"
            );
            if retryable {
                Status::unavailable(status_message)
            } else {
                Status::internal(status_message)
            }
        }
        ProviderError::InvalidResponse { message, retry_count } => Status::internal(format!(
            "model provider response invalid after {retry_count} retries: {message}"
        )),
        ProviderError::StatePoisoned => Status::internal("model provider state lock poisoned"),
    }
}

fn session_summary_message(session: &OrchestratorSessionRecord) -> gateway_v1::SessionSummary {
    gateway_v1::SessionSummary {
        session_id: Some(common_v1::CanonicalId { ulid: session.session_id.clone() }),
        session_key: session.session_key.clone(),
        session_label: session.session_label.clone().unwrap_or_default(),
        created_at_unix_ms: session.created_at_unix_ms,
        updated_at_unix_ms: session.updated_at_unix_ms,
        last_run_id: session
            .last_run_id
            .as_ref()
            .map(|run_id| common_v1::CanonicalId { ulid: run_id.clone() }),
    }
}

fn agent_message(agent: &AgentRecord) -> gateway_v1::Agent {
    gateway_v1::Agent {
        agent_id: agent.agent_id.clone(),
        display_name: agent.display_name.clone(),
        agent_dir: agent.agent_dir.clone(),
        workspace_roots: agent.workspace_roots.clone(),
        default_model_profile: agent.default_model_profile.clone(),
        default_tool_allowlist: agent.default_tool_allowlist.clone(),
        default_skill_allowlist: agent.default_skill_allowlist.clone(),
        created_at_unix_ms: agent.created_at_unix_ms,
        updated_at_unix_ms: agent.updated_at_unix_ms,
    }
}

fn agent_resolution_source_to_proto(source: AgentResolutionSource) -> i32 {
    match source {
        AgentResolutionSource::SessionBinding => {
            gateway_v1::AgentResolutionSource::SessionBinding as i32
        }
        AgentResolutionSource::Default => gateway_v1::AgentResolutionSource::Default as i32,
        AgentResolutionSource::Fallback => gateway_v1::AgentResolutionSource::Fallback as i32,
    }
}

fn approval_option_messages(options: &[ApprovalPromptOption]) -> Vec<common_v1::ApprovalOption> {
    options
        .iter()
        .map(|option| common_v1::ApprovalOption {
            option_id: option.option_id.clone(),
            label: option.label.clone(),
            description: option.description.clone(),
            default_selected: option.default_selected,
            decision_scope: approval_scope_to_proto(option.decision_scope),
            timebox_ttl_ms: option.timebox_ttl_ms.unwrap_or_default(),
        })
        .collect()
}

fn approval_prompt_message(prompt: &ApprovalPromptRecord) -> common_v1::ApprovalPrompt {
    common_v1::ApprovalPrompt {
        title: prompt.title.clone(),
        risk_level: approval_risk_to_proto(prompt.risk_level),
        subject_id: prompt.subject_id.clone(),
        summary: prompt.summary.clone(),
        options: approval_option_messages(prompt.options.as_slice()),
        timeout_seconds: prompt.timeout_seconds,
        details_json: prompt.details_json.as_bytes().to_vec(),
        policy_explanation: prompt.policy_explanation.clone(),
    }
}

fn approval_policy_snapshot_message(
    value: &ApprovalPolicySnapshot,
) -> gateway_v1::ApprovalPolicySnapshot {
    gateway_v1::ApprovalPolicySnapshot {
        policy_id: value.policy_id.clone(),
        policy_hash: value.policy_hash.clone(),
        evaluation_summary: value.evaluation_summary.clone(),
    }
}

fn approval_record_message(record: &ApprovalRecord) -> gateway_v1::ApprovalRecord {
    gateway_v1::ApprovalRecord {
        v: CANONICAL_PROTOCOL_MAJOR,
        approval_id: Some(common_v1::CanonicalId { ulid: record.approval_id.clone() }),
        session_id: Some(common_v1::CanonicalId { ulid: record.session_id.clone() }),
        run_id: Some(common_v1::CanonicalId { ulid: record.run_id.clone() }),
        principal: record.principal.clone(),
        device_id: record.device_id.clone(),
        channel: record.channel.clone().unwrap_or_default(),
        requested_at_unix_ms: record.requested_at_unix_ms,
        resolved_at_unix_ms: record.resolved_at_unix_ms.unwrap_or_default(),
        subject_type: approval_subject_type_to_proto(record.subject_type),
        subject_id: record.subject_id.clone(),
        request_summary: record.request_summary.clone(),
        decision: record
            .decision
            .map(approval_decision_to_proto)
            .unwrap_or(gateway_v1::ApprovalDecision::Unspecified as i32),
        decision_scope: record
            .decision_scope
            .map(approval_scope_to_proto)
            .unwrap_or(common_v1::ApprovalDecisionScope::Unspecified as i32),
        policy_snapshot: Some(approval_policy_snapshot_message(&record.policy_snapshot)),
        prompt: Some(approval_prompt_message(&record.prompt)),
        decision_reason: record.decision_reason.clone().unwrap_or_default(),
        decision_scope_ttl_ms: record.decision_scope_ttl_ms.unwrap_or_default(),
    }
}

fn canvas_bundle_message(bundle: &CanvasBundleRecord) -> gateway_v1::CanvasBundle {
    let mut assets = bundle.assets.iter().collect::<Vec<_>>();
    assets.sort_by(|left, right| left.0.cmp(right.0));
    gateway_v1::CanvasBundle {
        bundle_id: bundle.bundle_id.clone(),
        entrypoint_path: bundle.entrypoint_path.clone(),
        assets: assets
            .into_iter()
            .map(|(path, asset)| gateway_v1::CanvasAsset {
                path: path.clone(),
                content_type: asset.content_type.clone(),
                body: asset.body.clone(),
            })
            .collect(),
        sha256: bundle.sha256.clone(),
        signature: bundle.signature.clone(),
    }
}

fn canvas_message(record: &CanvasRecord) -> gateway_v1::Canvas {
    gateway_v1::Canvas {
        v: CANONICAL_PROTOCOL_MAJOR,
        canvas_id: Some(common_v1::CanonicalId { ulid: record.canvas_id.clone() }),
        session_id: Some(common_v1::CanonicalId { ulid: record.session_id.clone() }),
        principal: record.principal.clone(),
        state_version: record.state_version,
        state_json: record.state_json.clone(),
        bundle: Some(canvas_bundle_message(&record.bundle)),
        allowed_parent_origins: record.allowed_parent_origins.clone(),
        created_at_unix_ms: record.created_at_unix_ms,
        updated_at_unix_ms: record.updated_at_unix_ms,
        expires_at_unix_ms: record.expires_at_unix_ms,
        closed: record.closed,
        close_reason: record.close_reason.clone().unwrap_or_default(),
        state_schema_version: record.state_schema_version,
    }
}

fn canvas_patch_update_message(
    patch: &CanvasStatePatchRecord,
    include_snapshot_state: bool,
) -> gateway_v1::SubscribeCanvasUpdatesResponse {
    gateway_v1::SubscribeCanvasUpdatesResponse {
        v: CANONICAL_PROTOCOL_MAJOR,
        canvas_id: Some(common_v1::CanonicalId { ulid: patch.canvas_id.clone() }),
        state_version: patch.state_version,
        base_state_version: patch.base_state_version,
        state_schema_version: patch.state_schema_version,
        patch_json: patch.patch_json.as_bytes().to_vec(),
        state_json: if include_snapshot_state {
            patch.resulting_state_json.as_bytes().to_vec()
        } else {
            Vec::new()
        },
        closed: patch.closed,
        close_reason: patch.close_reason.clone().unwrap_or_default(),
        applied_at_unix_ms: patch.applied_at_unix_ms,
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

fn approval_export_chain_checksum(
    sequence: u64,
    previous_chain_checksum_sha256: &str,
    record_checksum_sha256: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(APPROVAL_EXPORT_NDJSON_SCHEMA_ID.as_bytes());
    hasher.update(b"\n");
    hasher.update(sequence.to_string().as_bytes());
    hasher.update(b"\n");
    hasher.update(previous_chain_checksum_sha256.as_bytes());
    hasher.update(b"\n");
    hasher.update(record_checksum_sha256.as_bytes());
    format!("{:x}", hasher.finalize())
}

#[allow(clippy::result_large_err)]
fn approval_export_ndjson_record_line(
    record: &ApprovalRecord,
    sequence: u64,
    previous_chain_checksum_sha256: &str,
) -> Result<(Vec<u8>, String), Status> {
    let record_payload = serde_json::to_value(record).map_err(|error| {
        Status::internal(format!("failed to serialize approval export record payload: {error}"))
    })?;
    let record_payload_bytes = serde_json::to_vec(&record_payload).map_err(|error| {
        Status::internal(format!("failed to encode approval export record payload bytes: {error}"))
    })?;
    let record_checksum_sha256 = sha256_hex(record_payload_bytes.as_slice());
    let chain_checksum_sha256 = approval_export_chain_checksum(
        sequence,
        previous_chain_checksum_sha256,
        record_checksum_sha256.as_str(),
    );
    let mut line = serde_json::to_vec(&json!({
        "schema": APPROVAL_EXPORT_NDJSON_SCHEMA_ID,
        "record_type": APPROVAL_EXPORT_NDJSON_RECORD_TYPE_ENTRY,
        "sequence": sequence,
        "prev_checksum_sha256": previous_chain_checksum_sha256,
        "record_checksum_sha256": record_checksum_sha256,
        "chain_checksum_sha256": chain_checksum_sha256,
        "record": record_payload,
    }))
    .map_err(|error| {
        Status::internal(format!("failed to encode approval export NDJSON record line: {error}"))
    })?;
    line.push(b'\n');
    Ok((line, chain_checksum_sha256))
}

#[allow(clippy::result_large_err)]
fn approval_export_ndjson_trailer_line(
    exported_records: usize,
    final_chain_checksum_sha256: &str,
) -> Result<Vec<u8>, Status> {
    let mut line = serde_json::to_vec(&json!({
        "schema": APPROVAL_EXPORT_NDJSON_SCHEMA_ID,
        "record_type": APPROVAL_EXPORT_NDJSON_RECORD_TYPE_TRAILER,
        "exported_records": exported_records,
        "final_chain_checksum_sha256": final_chain_checksum_sha256,
    }))
    .map_err(|error| {
        Status::internal(format!("failed to encode approval export NDJSON trailer line: {error}"))
    })?;
    line.push(b'\n');
    Ok(line)
}

fn approval_subject_type_to_proto(value: ApprovalSubjectType) -> i32 {
    match value {
        ApprovalSubjectType::Tool => gateway_v1::ApprovalSubjectType::Tool as i32,
        ApprovalSubjectType::ChannelSend => gateway_v1::ApprovalSubjectType::ChannelSend as i32,
        ApprovalSubjectType::SecretAccess => gateway_v1::ApprovalSubjectType::SecretAccess as i32,
        ApprovalSubjectType::BrowserAction => gateway_v1::ApprovalSubjectType::BrowserAction as i32,
        ApprovalSubjectType::NodeCapability => {
            gateway_v1::ApprovalSubjectType::NodeCapability as i32
        }
    }
}

fn approval_subject_type_from_proto(value: i32) -> Option<ApprovalSubjectType> {
    match gateway_v1::ApprovalSubjectType::try_from(value)
        .unwrap_or(gateway_v1::ApprovalSubjectType::Unspecified)
    {
        gateway_v1::ApprovalSubjectType::Unspecified => None,
        gateway_v1::ApprovalSubjectType::Tool => Some(ApprovalSubjectType::Tool),
        gateway_v1::ApprovalSubjectType::ChannelSend => Some(ApprovalSubjectType::ChannelSend),
        gateway_v1::ApprovalSubjectType::SecretAccess => Some(ApprovalSubjectType::SecretAccess),
        gateway_v1::ApprovalSubjectType::BrowserAction => Some(ApprovalSubjectType::BrowserAction),
        gateway_v1::ApprovalSubjectType::NodeCapability => {
            Some(ApprovalSubjectType::NodeCapability)
        }
    }
}

fn approval_decision_to_proto(value: ApprovalDecision) -> i32 {
    match value {
        ApprovalDecision::Allow => gateway_v1::ApprovalDecision::Allow as i32,
        ApprovalDecision::Deny => gateway_v1::ApprovalDecision::Deny as i32,
        ApprovalDecision::Timeout => gateway_v1::ApprovalDecision::Timeout as i32,
        ApprovalDecision::Error => gateway_v1::ApprovalDecision::Error as i32,
    }
}

fn approval_decision_from_proto(value: i32) -> Option<ApprovalDecision> {
    match gateway_v1::ApprovalDecision::try_from(value)
        .unwrap_or(gateway_v1::ApprovalDecision::Unspecified)
    {
        gateway_v1::ApprovalDecision::Unspecified => None,
        gateway_v1::ApprovalDecision::Allow => Some(ApprovalDecision::Allow),
        gateway_v1::ApprovalDecision::Deny => Some(ApprovalDecision::Deny),
        gateway_v1::ApprovalDecision::Timeout => Some(ApprovalDecision::Timeout),
        gateway_v1::ApprovalDecision::Error => Some(ApprovalDecision::Error),
    }
}

fn approval_scope_to_proto(value: ApprovalDecisionScope) -> i32 {
    match value {
        ApprovalDecisionScope::Once => common_v1::ApprovalDecisionScope::Once as i32,
        ApprovalDecisionScope::Session => common_v1::ApprovalDecisionScope::Session as i32,
        ApprovalDecisionScope::Timeboxed => common_v1::ApprovalDecisionScope::Timeboxed as i32,
    }
}

fn approval_scope_from_proto(value: i32) -> ApprovalDecisionScope {
    match common_v1::ApprovalDecisionScope::try_from(value)
        .unwrap_or(common_v1::ApprovalDecisionScope::Unspecified)
    {
        common_v1::ApprovalDecisionScope::Unspecified => ApprovalDecisionScope::Once,
        common_v1::ApprovalDecisionScope::Once => ApprovalDecisionScope::Once,
        common_v1::ApprovalDecisionScope::Session => ApprovalDecisionScope::Session,
        common_v1::ApprovalDecisionScope::Timeboxed => ApprovalDecisionScope::Timeboxed,
    }
}

fn approval_risk_to_proto(value: ApprovalRiskLevel) -> i32 {
    match value {
        ApprovalRiskLevel::Low => common_v1::ApprovalRiskLevel::Low as i32,
        ApprovalRiskLevel::Medium => common_v1::ApprovalRiskLevel::Medium as i32,
        ApprovalRiskLevel::High => common_v1::ApprovalRiskLevel::High as i32,
        ApprovalRiskLevel::Critical => common_v1::ApprovalRiskLevel::Critical as i32,
    }
}

#[derive(Clone)]
pub struct GatewayServiceImpl {
    state: Arc<GatewayRuntimeState>,
    auth: GatewayAuthConfig,
}

impl GatewayServiceImpl {
    #[must_use]
    pub fn new(state: Arc<GatewayRuntimeState>, auth: GatewayAuthConfig) -> Self {
        Self { state, auth }
    }

    #[allow(clippy::result_large_err)]
    fn authorize_rpc(
        &self,
        metadata: &MetadataMap,
        method: &'static str,
    ) -> Result<RequestContext, Status> {
        authorize_metadata(metadata, &self.auth).map_err(|error| {
            self.state.record_denied();
            warn!(method, error = %error, "gateway rpc authorization denied");
            Status::permission_denied(error.to_string())
        })
    }
}

#[derive(Clone)]
pub struct CronServiceImpl {
    state: Arc<GatewayRuntimeState>,
    auth: GatewayAuthConfig,
    grpc_url: String,
    scheduler_wake: Arc<Notify>,
    cron_timezone_mode: CronTimezoneMode,
}

impl CronServiceImpl {
    #[must_use]
    pub fn new(
        state: Arc<GatewayRuntimeState>,
        auth: GatewayAuthConfig,
        grpc_url: String,
        scheduler_wake: Arc<Notify>,
        cron_timezone_mode: CronTimezoneMode,
    ) -> Self {
        Self { state, auth, grpc_url, scheduler_wake, cron_timezone_mode }
    }

    #[allow(clippy::result_large_err)]
    fn authorize_rpc(
        &self,
        metadata: &MetadataMap,
        method: &'static str,
    ) -> Result<RequestContext, Status> {
        authorize_metadata(metadata, &self.auth).map_err(|error| {
            self.state.record_denied();
            warn!(method, error = %error, "cron rpc authorization denied");
            Status::permission_denied(error.to_string())
        })
    }
}

#[derive(Clone)]
pub struct ApprovalsServiceImpl {
    state: Arc<GatewayRuntimeState>,
    auth: GatewayAuthConfig,
}

impl ApprovalsServiceImpl {
    #[must_use]
    pub fn new(state: Arc<GatewayRuntimeState>, auth: GatewayAuthConfig) -> Self {
        Self { state, auth }
    }

    #[allow(clippy::result_large_err)]
    fn authorize_rpc(
        &self,
        metadata: &MetadataMap,
        method: &'static str,
    ) -> Result<RequestContext, Status> {
        authorize_metadata(metadata, &self.auth).map_err(|error| {
            self.state.record_denied();
            warn!(method, error = %error, "approvals rpc authorization denied");
            Status::permission_denied(error.to_string())
        })
    }
}

#[derive(Clone)]
pub struct MemoryServiceImpl {
    state: Arc<GatewayRuntimeState>,
    auth: GatewayAuthConfig,
}

impl MemoryServiceImpl {
    #[must_use]
    pub fn new(state: Arc<GatewayRuntimeState>, auth: GatewayAuthConfig) -> Self {
        Self { state, auth }
    }

    #[allow(clippy::result_large_err)]
    fn authorize_rpc(
        &self,
        metadata: &MetadataMap,
        method: &'static str,
    ) -> Result<RequestContext, Status> {
        authorize_metadata(metadata, &self.auth).map_err(|error| {
            self.state.record_denied();
            warn!(method, error = %error, "memory rpc authorization denied");
            Status::permission_denied(error.to_string())
        })
    }
}

#[derive(Clone)]
pub struct VaultServiceImpl {
    state: Arc<GatewayRuntimeState>,
    auth: GatewayAuthConfig,
}

impl VaultServiceImpl {
    #[must_use]
    pub fn new(state: Arc<GatewayRuntimeState>, auth: GatewayAuthConfig) -> Self {
        Self { state, auth }
    }

    #[allow(clippy::result_large_err)]
    fn authorize_rpc(
        &self,
        metadata: &MetadataMap,
        method: &'static str,
    ) -> Result<RequestContext, Status> {
        authorize_metadata(metadata, &self.auth).map_err(|error| {
            self.state.record_denied();
            warn!(method, error = %error, "vault rpc authorization denied");
            Status::permission_denied(error.to_string())
        })
    }
}

#[derive(Clone)]
pub struct AuthServiceImpl {
    state: Arc<GatewayRuntimeState>,
    auth: GatewayAuthConfig,
    auth_runtime: Arc<AuthRuntimeState>,
}

impl AuthServiceImpl {
    #[must_use]
    pub fn new(
        state: Arc<GatewayRuntimeState>,
        auth: GatewayAuthConfig,
        auth_runtime: Arc<AuthRuntimeState>,
    ) -> Self {
        Self { state, auth, auth_runtime }
    }

    #[allow(clippy::result_large_err)]
    fn authorize_rpc(
        &self,
        metadata: &MetadataMap,
        method: &'static str,
    ) -> Result<RequestContext, Status> {
        authorize_metadata(metadata, &self.auth).map_err(|error| {
            self.state.record_denied();
            warn!(method, error = %error, "auth rpc authorization denied");
            Status::permission_denied(error.to_string())
        })
    }
}

#[derive(Clone)]
pub struct CanvasServiceImpl {
    state: Arc<GatewayRuntimeState>,
    auth: GatewayAuthConfig,
}

impl CanvasServiceImpl {
    #[must_use]
    pub fn new(state: Arc<GatewayRuntimeState>, auth: GatewayAuthConfig) -> Self {
        Self { state, auth }
    }

    #[allow(clippy::result_large_err)]
    fn authorize_rpc(
        &self,
        metadata: &MetadataMap,
        method: &'static str,
    ) -> Result<RequestContext, Status> {
        authorize_metadata(metadata, &self.auth).map_err(|error| {
            self.state.record_denied();
            warn!(method, error = %error, "canvas rpc authorization denied");
            Status::permission_denied(error.to_string())
        })
    }
}

#[tonic::async_trait]
impl gateway_v1::canvas_service_server::CanvasService for CanvasServiceImpl {
    type SubscribeCanvasUpdatesStream =
        ReceiverStream<Result<gateway_v1::SubscribeCanvasUpdatesResponse, Status>>;

    async fn create_canvas(
        &self,
        request: Request<gateway_v1::CreateCanvasRequest>,
    ) -> Result<Response<gateway_v1::CreateCanvasResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "CreateCanvas")?;
        let payload = request.into_inner();
        if payload.v != CANONICAL_PROTOCOL_MAJOR {
            return Err(Status::failed_precondition("unsupported protocol major version"));
        }
        let requested_canvas_id = optional_canonical_id(payload.canvas_id, "canvas_id")?;
        let session_id = canonical_id(payload.session_id, "session_id")?;
        let bundle =
            payload.bundle.ok_or_else(|| Status::invalid_argument("bundle is required"))?;
        let (record, descriptor) = self.state.create_canvas(
            &context,
            requested_canvas_id,
            session_id,
            payload.initial_state_json.as_slice(),
            payload.initial_state_version,
            if payload.state_schema_version == 0 {
                None
            } else {
                Some(payload.state_schema_version)
            },
            bundle,
            payload.allowed_parent_origins,
            if payload.auth_token_ttl_seconds == 0 {
                None
            } else {
                Some(payload.auth_token_ttl_seconds)
            },
        )?;
        Ok(Response::new(gateway_v1::CreateCanvasResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            canvas: Some(canvas_message(&record)),
            frame_url: descriptor.frame_url,
            runtime_url: descriptor.runtime_url,
            auth_token: descriptor.auth_token,
        }))
    }

    async fn update_canvas(
        &self,
        request: Request<gateway_v1::UpdateCanvasRequest>,
    ) -> Result<Response<gateway_v1::UpdateCanvasResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "UpdateCanvas")?;
        let payload = request.into_inner();
        if payload.v != CANONICAL_PROTOCOL_MAJOR {
            return Err(Status::failed_precondition("unsupported protocol major version"));
        }
        let canvas_id = canonical_id(payload.canvas_id, "canvas_id")?;
        let record = self.state.update_canvas_state(
            &context,
            canvas_id.as_str(),
            if payload.state_json.is_empty() { None } else { Some(payload.state_json.as_slice()) },
            if payload.patch_json.is_empty() { None } else { Some(payload.patch_json.as_slice()) },
            if payload.expected_state_version == 0 {
                None
            } else {
                Some(payload.expected_state_version)
            },
            if payload.expected_state_schema_version == 0 {
                None
            } else {
                Some(payload.expected_state_schema_version)
            },
        )?;
        Ok(Response::new(gateway_v1::UpdateCanvasResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            canvas: Some(canvas_message(&record)),
        }))
    }

    async fn close_canvas(
        &self,
        request: Request<gateway_v1::CloseCanvasRequest>,
    ) -> Result<Response<gateway_v1::CloseCanvasResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "CloseCanvas")?;
        let payload = request.into_inner();
        if payload.v != CANONICAL_PROTOCOL_MAJOR {
            return Err(Status::failed_precondition("unsupported protocol major version"));
        }
        let canvas_id = canonical_id(payload.canvas_id, "canvas_id")?;
        let record =
            self.state.close_canvas(&context, canvas_id.as_str(), non_empty(payload.reason))?;
        Ok(Response::new(gateway_v1::CloseCanvasResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            canvas_id: Some(common_v1::CanonicalId { ulid: record.canvas_id }),
            closed: record.closed,
            close_reason: record.close_reason.unwrap_or_default(),
        }))
    }

    async fn get_canvas(
        &self,
        request: Request<gateway_v1::GetCanvasRequest>,
    ) -> Result<Response<gateway_v1::GetCanvasResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "GetCanvas")?;
        let payload = request.into_inner();
        if payload.v != CANONICAL_PROTOCOL_MAJOR {
            return Err(Status::failed_precondition("unsupported protocol major version"));
        }
        let canvas_id = canonical_id(payload.canvas_id, "canvas_id")?;
        let record = self.state.get_canvas(&context, canvas_id.as_str())?;
        Ok(Response::new(gateway_v1::GetCanvasResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            canvas: Some(canvas_message(&record)),
        }))
    }

    async fn subscribe_canvas_updates(
        &self,
        request: Request<gateway_v1::SubscribeCanvasUpdatesRequest>,
    ) -> Result<Response<Self::SubscribeCanvasUpdatesStream>, Status> {
        let context = self.authorize_rpc(request.metadata(), "SubscribeCanvasUpdates")?;
        let payload = request.into_inner();
        if payload.v != CANONICAL_PROTOCOL_MAJOR {
            return Err(Status::failed_precondition("unsupported protocol major version"));
        }
        let canvas_id = canonical_id(payload.canvas_id, "canvas_id")?;
        let include_snapshot_state = payload.include_snapshot_state;

        let _existing_canvas = self.state.get_canvas(&context, canvas_id.as_str())?;
        let state = Arc::clone(&self.state);
        let context_for_stream = context.clone();
        let canvas_id_for_stream = canvas_id.clone();
        let mut after_state_version = payload.after_state_version;
        let (tx, rx) = mpsc::channel::<Result<gateway_v1::SubscribeCanvasUpdatesResponse, Status>>(
            MAX_CANVAS_STREAM_PATCH_BATCH,
        );

        tokio::spawn(async move {
            loop {
                let patches = match state.list_canvas_state_patches(
                    &context_for_stream,
                    canvas_id_for_stream.as_str(),
                    after_state_version,
                    MAX_CANVAS_STREAM_PATCH_BATCH,
                ) {
                    Ok(records) => records,
                    Err(error) => {
                        let _ = tx.send(Err(error)).await;
                        break;
                    }
                };
                if patches.is_empty() {
                    match state.get_canvas(&context_for_stream, canvas_id_for_stream.as_str()) {
                        Ok(record)
                            if record.closed && after_state_version >= record.state_version =>
                        {
                            return;
                        }
                        Ok(_) => {}
                        Err(error) => {
                            let _ = tx.send(Err(error)).await;
                            return;
                        }
                    }
                    tokio::time::sleep(CANVAS_STREAM_POLL_INTERVAL).await;
                    continue;
                }

                for patch in patches {
                    after_state_version = patch.state_version;
                    if tx
                        .send(Ok(canvas_patch_update_message(&patch, include_snapshot_state)))
                        .await
                        .is_err()
                    {
                        return;
                    }
                    if patch.closed {
                        return;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[tonic::async_trait]
impl gateway_v1::gateway_service_server::GatewayService for GatewayServiceImpl {
    type RunStreamStream = ReceiverStream<Result<common_v1::RunStreamEvent, Status>>;

    async fn get_health(
        &self,
        _request: Request<gateway_v1::HealthRequest>,
    ) -> Result<Response<gateway_v1::HealthResponse>, Status> {
        Ok(Response::new(gateway_v1::HealthResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            service: "palyrad".to_owned(),
            status: "ok".to_owned(),
            version: self.state.build.version.clone(),
            git_hash: self.state.build.git_hash.clone(),
            build_profile: self.state.build.build_profile.clone(),
            uptime_seconds: self.state.started_at.elapsed().as_secs(),
        }))
    }

    async fn append_event(
        &self,
        request: Request<gateway_v1::AppendEventRequest>,
    ) -> Result<Response<gateway_v1::AppendEventResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "AppendEvent")?;
        self.state.counters.append_event_requests.fetch_add(1, Ordering::Relaxed);

        let payload = request.into_inner();
        if payload.v != CANONICAL_PROTOCOL_MAJOR {
            return Err(Status::failed_precondition("unsupported protocol major version"));
        }
        let event = payload.event.ok_or_else(|| Status::invalid_argument("event is required"))?;
        if event.v != CANONICAL_PROTOCOL_MAJOR {
            return Err(Status::failed_precondition(
                "event uses an unsupported protocol major version",
            ));
        }
        let event_id = if let Some(id) = event.event_id.and_then(|value| non_empty(value.ulid)) {
            validate_canonical_id(&id)
                .map_err(|_| Status::invalid_argument("event.event_id must be a canonical ULID"))?;
            id
        } else {
            Ulid::new().to_string()
        };
        let session_id = canonical_id(event.session_id, "event.session_id")?;
        let run_id = canonical_id(event.run_id, "event.run_id")?;
        if event.timestamp_unix_ms <= 0 {
            return Err(Status::invalid_argument(
                "event.timestamp_unix_ms must be a unix timestamp",
            ));
        }
        if event.kind == common_v1::journal_event::EventKind::Unspecified as i32 {
            return Err(Status::invalid_argument("event.kind must be specified"));
        }
        if event.actor == common_v1::journal_event::EventActor::Unspecified as i32 {
            return Err(Status::invalid_argument("event.actor must be specified"));
        }

        let journal_outcome = self
            .state
            .record_journal_event(JournalAppendRequest {
                event_id: event_id.clone(),
                session_id,
                run_id,
                kind: event.kind,
                actor: event.actor,
                timestamp_unix_ms: event.timestamp_unix_ms,
                payload_json: event.payload_json,
                principal: context.principal.clone(),
                device_id: context.device_id.clone(),
                channel: context.channel.clone(),
            })
            .await?;

        info!(
            method = "AppendEvent",
            principal = %context.principal,
            device_id = %context.device_id,
            channel = context.channel.as_deref().unwrap_or("n/a"),
            event_id = %event_id,
            redacted_payload = journal_outcome.redacted,
            hash_chain_enabled = self.state.journal_config.hash_chain_enabled,
            write_duration_ms = journal_outcome.write_duration.as_millis(),
            event_hash = journal_outcome.hash.as_deref().unwrap_or("disabled"),
            "gateway event appended"
        );

        Ok(Response::new(gateway_v1::AppendEventResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            event_id: Some(common_v1::CanonicalId { ulid: event_id }),
            accepted: true,
        }))
    }

    async fn abort_run(
        &self,
        request: Request<gateway_v1::AbortRunRequest>,
    ) -> Result<Response<gateway_v1::AbortRunResponse>, Status> {
        let _context = self.authorize_rpc(request.metadata(), "AbortRun")?;
        let payload = request.into_inner();
        if payload.v != CANONICAL_PROTOCOL_MAJOR {
            return Err(Status::failed_precondition("unsupported protocol major version"));
        }
        let run_id = canonical_id(payload.run_id, "run_id")?;
        let reason = non_empty(payload.reason).unwrap_or_else(|| "grpc_abort_requested".to_owned());
        let snapshot = self
            .state
            .request_orchestrator_cancel(OrchestratorCancelRequest {
                run_id: run_id.clone(),
                reason: reason.clone(),
            })
            .await?;
        Ok(Response::new(gateway_v1::AbortRunResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            run_id: Some(common_v1::CanonicalId { ulid: snapshot.run_id }),
            cancel_requested: snapshot.cancel_requested,
            reason: snapshot.reason,
        }))
    }

    async fn list_sessions(
        &self,
        request: Request<gateway_v1::ListSessionsRequest>,
    ) -> Result<Response<gateway_v1::ListSessionsResponse>, Status> {
        let _context = self.authorize_rpc(request.metadata(), "ListSessions")?;
        let payload = request.into_inner();
        if payload.v != CANONICAL_PROTOCOL_MAJOR {
            return Err(Status::failed_precondition("unsupported protocol major version"));
        }
        let after_session_key = non_empty(payload.after_session_key);
        let requested_limit = if payload.limit == 0 { None } else { Some(payload.limit as usize) };
        let (sessions, next_after_session_key) =
            self.state.list_orchestrator_sessions(after_session_key, requested_limit).await?;
        Ok(Response::new(gateway_v1::ListSessionsResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            sessions: sessions.iter().map(session_summary_message).collect(),
            next_after_session_key: next_after_session_key.unwrap_or_default(),
        }))
    }

    async fn resolve_session(
        &self,
        request: Request<gateway_v1::ResolveSessionRequest>,
    ) -> Result<Response<gateway_v1::ResolveSessionResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "ResolveSession")?;
        let payload = request.into_inner();
        if payload.v != CANONICAL_PROTOCOL_MAJOR {
            return Err(Status::failed_precondition("unsupported protocol major version"));
        }
        let session_id = optional_canonical_id(payload.session_id, "session_id")?;
        let session_key = non_empty(payload.session_key);
        let session_label = non_empty(payload.session_label);
        let outcome = self
            .state
            .resolve_orchestrator_session(OrchestratorSessionResolveRequest {
                session_id,
                session_key,
                session_label,
                principal: context.principal,
                device_id: context.device_id,
                channel: context.channel,
                require_existing: payload.require_existing,
                reset_session: payload.reset_session,
            })
            .await?;
        Ok(Response::new(gateway_v1::ResolveSessionResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            session: Some(session_summary_message(&outcome.session)),
            created: outcome.created,
            reset_applied: outcome.reset_applied,
        }))
    }

    async fn route_message(
        &self,
        request: Request<gateway_v1::RouteMessageRequest>,
    ) -> Result<Response<gateway_v1::RouteMessageResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "RouteMessage")?;
        let payload = request.into_inner();
        let retry_attempt = payload.retry_attempt;
        let requested_session_label = non_empty(payload.session_label.clone());
        require_supported_version(payload.v)?;
        let envelope =
            payload.envelope.ok_or_else(|| Status::invalid_argument("envelope is required"))?;
        if envelope.v != CANONICAL_PROTOCOL_MAJOR {
            return Err(Status::failed_precondition(
                "envelope uses an unsupported protocol major version",
            ));
        }
        let origin = envelope.origin.unwrap_or_default();
        let content = envelope.content.unwrap_or_default();
        let channel = if let Some(value) = non_empty(origin.channel.clone()) {
            value
        } else if let Some(value) = context.channel.clone() {
            value
        } else {
            return Err(Status::invalid_argument(
                "route message requires origin.channel or authenticated channel context",
            ));
        };
        if let Some(context_channel) = context.channel.as_deref() {
            if !context_channel.eq_ignore_ascii_case(channel.as_str()) {
                self.state.record_denied();
                return Err(Status::permission_denied(
                    "authenticated channel context does not match message channel",
                ));
            }
        }
        let envelope_id = if let Some(value) = envelope.envelope_id {
            validate_canonical_id(value.ulid.as_str()).map_err(|_| {
                Status::invalid_argument("envelope.envelope_id must be a canonical ULID")
            })?;
            value.ulid
        } else {
            Ulid::new().to_string()
        };
        let input = ChannelInboundMessage {
            envelope_id: envelope_id.clone(),
            channel: channel.clone(),
            conversation_id: non_empty(origin.conversation_id),
            sender_handle: non_empty(origin.sender_handle),
            sender_display: non_empty(origin.sender_display),
            sender_verified: origin.sender_verified,
            text: content.text.clone(),
            max_payload_bytes: envelope.max_payload_bytes,
            is_direct_message: payload.is_direct_message,
            requested_broadcast: payload.request_broadcast,
            adapter_message_id: non_empty(payload.adapter_message_id),
            adapter_thread_id: non_empty(payload.adapter_thread_id),
            retry_attempt,
        };
        self.state.counters.channel_messages_inbound.fetch_add(1, Ordering::Relaxed);

        match self.state.channel_router.begin_route(&input) {
            RouteOutcome::Rejected(rejection) => {
                let rejection_reason = rejection.reason.clone();
                self.state.counters.channel_messages_rejected.fetch_add(1, Ordering::Relaxed);
                if rejection.quarantined {
                    self.state
                        .counters
                        .channel_messages_quarantined
                        .fetch_add(1, Ordering::Relaxed);
                }
                self.state
                    .counters
                    .channel_router_queue_depth
                    .store(self.state.channel_router.queue_depth() as u64, Ordering::Relaxed);
                let journal_session_id = Ulid::new().to_string();
                let journal_run_id = Ulid::new().to_string();
                let _ = record_message_router_journal_event(
                    &self.state,
                    &context,
                    journal_session_id.as_str(),
                    journal_run_id.as_str(),
                    "message.received",
                    common_v1::journal_event::EventActor::User as i32,
                    json!({
                        "event": "message.received",
                        "envelope_id": envelope_id,
                        "channel": channel,
                        "requested_broadcast": input.requested_broadcast,
                        "is_direct_message": input.is_direct_message,
                    }),
                )
                .await;
                let _ = record_message_router_journal_event(
                    &self.state,
                    &context,
                    journal_session_id.as_str(),
                    journal_run_id.as_str(),
                    "message.rejected",
                    common_v1::journal_event::EventActor::System as i32,
                    json!({
                        "event": "message.rejected",
                        "envelope_id": input.envelope_id.clone(),
                        "channel": input.channel.clone(),
                        "reason": rejection_reason.clone(),
                        "quarantined": rejection.quarantined,
                    }),
                )
                .await;
                return Ok(Response::new(gateway_v1::RouteMessageResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    accepted: false,
                    queued_for_retry: false,
                    decision_reason: rejection_reason,
                    session_id: None,
                    run_id: None,
                    outputs: Vec::new(),
                    route_key: String::new(),
                    retry_attempt,
                    queue_depth: self.state.channel_router.queue_depth() as u32,
                }));
            }
            RouteOutcome::Queued(queued) => {
                let queue_reason = queued.reason.clone();
                self.state.counters.channel_messages_queued.fetch_add(1, Ordering::Relaxed);
                self.state
                    .counters
                    .channel_router_queue_depth
                    .store(self.state.channel_router.queue_depth() as u64, Ordering::Relaxed);
                let journal_session_id = Ulid::new().to_string();
                let journal_run_id = Ulid::new().to_string();
                let _ = record_message_router_journal_event(
                    &self.state,
                    &context,
                    journal_session_id.as_str(),
                    journal_run_id.as_str(),
                    "message.received",
                    common_v1::journal_event::EventActor::User as i32,
                    json!({
                        "event": "message.received",
                        "envelope_id": input.envelope_id.clone(),
                        "channel": input.channel.clone(),
                        "requested_broadcast": input.requested_broadcast,
                        "is_direct_message": input.is_direct_message,
                    }),
                )
                .await;
                let _ = record_message_router_journal_event(
                    &self.state,
                    &context,
                    journal_session_id.as_str(),
                    journal_run_id.as_str(),
                    "message.rejected",
                    common_v1::journal_event::EventActor::System as i32,
                    json!({
                        "event": "message.rejected",
                        "envelope_id": input.envelope_id.clone(),
                        "channel": input.channel.clone(),
                        "reason": queue_reason.clone(),
                        "queued_for_retry": true,
                        "retry_after_ms": queued.retry_after_ms,
                    }),
                )
                .await;
                return Ok(Response::new(gateway_v1::RouteMessageResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    accepted: false,
                    queued_for_retry: true,
                    decision_reason: queue_reason,
                    session_id: None,
                    run_id: None,
                    outputs: Vec::new(),
                    route_key: String::new(),
                    retry_attempt: retry_attempt.saturating_add(1),
                    queue_depth: queued.queue_depth as u32,
                }));
            }
            RouteOutcome::Routed(routed) => {
                let ChannelRoutedMessage { plan, lease: _route_lease } = *routed;
                let route_action =
                    if plan.is_broadcast { "message.broadcast" } else { "message.reply" };
                let policy_resource = format!("channel:{}", plan.channel);
                if let Err(error) = authorize_message_action(
                    context.principal.as_str(),
                    route_action,
                    policy_resource.as_str(),
                    Some(plan.channel.as_str()),
                    None,
                    None,
                ) {
                    self.state.record_denied();
                    self.state.counters.channel_messages_rejected.fetch_add(1, Ordering::Relaxed);
                    let journal_session_id = Ulid::new().to_string();
                    let journal_run_id = Ulid::new().to_string();
                    let _ = record_message_router_journal_event(
                        &self.state,
                        &context,
                        journal_session_id.as_str(),
                        journal_run_id.as_str(),
                        "message.rejected",
                        common_v1::journal_event::EventActor::System as i32,
                        json!({
                            "event": "message.rejected",
                            "envelope_id": input.envelope_id.clone(),
                            "channel": input.channel.clone(),
                            "reason": error.message(),
                            "policy_action": route_action,
                        }),
                    )
                    .await;
                    return Ok(Response::new(gateway_v1::RouteMessageResponse {
                        v: CANONICAL_PROTOCOL_MAJOR,
                        accepted: false,
                        queued_for_retry: false,
                        decision_reason: error.message().to_owned(),
                        session_id: None,
                        run_id: None,
                        outputs: Vec::new(),
                        route_key: plan.route_key.clone(),
                        retry_attempt,
                        queue_depth: self.state.channel_router.queue_depth() as u32,
                    }));
                }

                let resolved_session = self
                    .state
                    .resolve_orchestrator_session(OrchestratorSessionResolveRequest {
                        session_id: None,
                        session_key: Some(plan.session_key.clone()),
                        session_label: requested_session_label
                            .clone()
                            .or(plan.session_label.clone()),
                        principal: context.principal.clone(),
                        device_id: context.device_id.clone(),
                        channel: Some(plan.channel.clone()),
                        require_existing: false,
                        reset_session: false,
                    })
                    .await?;
                let session_id = resolved_session.session.session_id;
                let run_id = Ulid::new().to_string();
                self.state
                    .start_orchestrator_run(OrchestratorRunStartRequest {
                        run_id: run_id.clone(),
                        session_id: session_id.clone(),
                    })
                    .await?;
                self.state
                    .update_orchestrator_run_state(
                        run_id.clone(),
                        RunLifecycleState::InProgress,
                        None,
                    )
                    .await?;
                self.state.counters.channel_messages_routed.fetch_add(1, Ordering::Relaxed);

                let _ = record_message_router_journal_event(
                    &self.state,
                    &context,
                    session_id.as_str(),
                    run_id.as_str(),
                    "message.received",
                    common_v1::journal_event::EventActor::User as i32,
                    json!({
                        "event": "message.received",
                        "envelope_id": input.envelope_id.clone(),
                        "channel": input.channel.clone(),
                        "session_key": plan.session_key.clone(),
                        "route_key": plan.route_key.clone(),
                    }),
                )
                .await;

                let provider_response = self
                    .state
                    .execute_model_provider(ProviderRequest {
                        input_text: input.text.clone(),
                        json_mode: false,
                        vision_requested: content.attachments.iter().any(|attachment| {
                            attachment.kind
                                == common_v1::message_attachment::AttachmentKind::Image as i32
                        }),
                    })
                    .await;

                let provider_response = match provider_response {
                    Ok(response) => response,
                    Err(error) => {
                        let error_message = error.message().to_owned();
                        let retry_disposition = self
                            .state
                            .channel_router
                            .record_processing_failure(&input, "provider_error");
                        if matches!(retry_disposition, RetryDisposition::Quarantined) {
                            self.state
                                .counters
                                .channel_messages_quarantined
                                .fetch_add(1, Ordering::Relaxed);
                        } else {
                            self.state
                                .counters
                                .channel_messages_queued
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        self.state
                            .counters
                            .channel_messages_rejected
                            .fetch_add(1, Ordering::Relaxed);
                        self.state.counters.channel_reply_failures.fetch_add(1, Ordering::Relaxed);
                        self.state
                            .update_orchestrator_run_state(
                                run_id.clone(),
                                RunLifecycleState::Failed,
                                Some(error_message.clone()),
                            )
                            .await?;
                        let _ = record_message_router_journal_event(
                            &self.state,
                            &context,
                            session_id.as_str(),
                            run_id.as_str(),
                            "message.rejected",
                            common_v1::journal_event::EventActor::System as i32,
                            json!({
                                "event": "message.rejected",
                                "envelope_id": input.envelope_id.clone(),
                                "channel": input.channel.clone(),
                                "reason": error_message,
                                "retry_disposition": match retry_disposition {
                                    RetryDisposition::Queued => "queued",
                                    RetryDisposition::Quarantined => "quarantined",
                                },
                            }),
                        )
                        .await;
                        self.state.counters.channel_router_queue_depth.store(
                            self.state.channel_router.queue_depth() as u64,
                            Ordering::Relaxed,
                        );
                        return Ok(Response::new(gateway_v1::RouteMessageResponse {
                            v: CANONICAL_PROTOCOL_MAJOR,
                            accepted: false,
                            queued_for_retry: matches!(retry_disposition, RetryDisposition::Queued),
                            decision_reason: "model_provider_failed".to_owned(),
                            session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                            run_id: Some(common_v1::CanonicalId { ulid: run_id }),
                            outputs: Vec::new(),
                            route_key: plan.route_key.clone(),
                            retry_attempt: retry_attempt.saturating_add(1),
                            queue_depth: self.state.channel_router.queue_depth() as u32,
                        }));
                    }
                };

                let mut reply_text = String::new();
                for event in provider_response.events {
                    match event {
                        ProviderEvent::ModelToken { token, .. } => {
                            if !reply_text.is_empty() {
                                reply_text.push(' ');
                            }
                            reply_text.push_str(token.as_str());
                        }
                        ProviderEvent::ToolProposal { tool_name, .. } => {
                            if !reply_text.is_empty() {
                                reply_text.push('\n');
                            }
                            reply_text.push_str(
                                format!(
                                    "[tool proposal blocked by channel router v1: {tool_name}]"
                                )
                                .as_str(),
                            );
                        }
                    }
                }
                if reply_text.trim().is_empty() {
                    reply_text = "ack".to_owned();
                }
                if let Some(prefix) = plan.response_prefix.as_deref() {
                    reply_text = format!("{prefix}{reply_text}");
                }
                if let Err(error) = authorize_message_action(
                    context.principal.as_str(),
                    "channel.send",
                    policy_resource.as_str(),
                    Some(plan.channel.as_str()),
                    Some(session_id.as_str()),
                    Some(run_id.as_str()),
                ) {
                    self.state.record_denied();
                    self.state.counters.channel_messages_rejected.fetch_add(1, Ordering::Relaxed);
                    self.state.counters.channel_reply_failures.fetch_add(1, Ordering::Relaxed);
                    self.state
                        .update_orchestrator_run_state(
                            run_id.clone(),
                            RunLifecycleState::Failed,
                            Some(error.message().to_owned()),
                        )
                        .await?;
                    let _ = record_message_router_journal_event(
                        &self.state,
                        &context,
                        session_id.as_str(),
                        run_id.as_str(),
                        "message.rejected",
                        common_v1::journal_event::EventActor::System as i32,
                        json!({
                            "event": "message.rejected",
                            "envelope_id": envelope_id,
                            "channel": plan.channel.clone(),
                            "reason": error.message(),
                            "policy_action": "channel.send",
                        }),
                    )
                    .await;
                    return Ok(Response::new(gateway_v1::RouteMessageResponse {
                        v: CANONICAL_PROTOCOL_MAJOR,
                        accepted: false,
                        queued_for_retry: false,
                        decision_reason: error.message().to_owned(),
                        session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                        run_id: Some(common_v1::CanonicalId { ulid: run_id }),
                        outputs: Vec::new(),
                        route_key: plan.route_key.clone(),
                        retry_attempt,
                        queue_depth: self.state.channel_router.queue_depth() as u32,
                    }));
                }

                self.state
                    .add_orchestrator_usage(OrchestratorUsageDelta {
                        run_id: run_id.clone(),
                        prompt_tokens_delta: provider_response.prompt_tokens,
                        completion_tokens_delta: provider_response.completion_tokens,
                    })
                    .await?;
                self.state
                    .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
                        run_id: run_id.clone(),
                        seq: 1,
                        event_type: "message.received".to_owned(),
                        payload_json: json!({
                            "envelope_id": input.envelope_id.clone(),
                            "text": input.text.clone(),
                            "channel": input.channel.clone(),
                        })
                        .to_string(),
                    })
                    .await?;
                self.state
                    .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
                        run_id: run_id.clone(),
                        seq: 2,
                        event_type: "message.replied".to_owned(),
                        payload_json: json!({
                            "reply_text": reply_text,
                            "route_key": plan.route_key.clone(),
                        })
                        .to_string(),
                    })
                    .await?;
                self.state
                    .update_orchestrator_run_state(run_id.clone(), RunLifecycleState::Done, None)
                    .await?;

                let _ = record_message_router_journal_event(
                    &self.state,
                    &context,
                    session_id.as_str(),
                    run_id.as_str(),
                    "message.routed",
                    common_v1::journal_event::EventActor::System as i32,
                    json!({
                        "event": "message.routed",
                        "envelope_id": envelope_id,
                        "channel": plan.channel.clone(),
                        "route_key": plan.route_key.clone(),
                        "session_id": session_id,
                        "run_id": run_id,
                        "broadcast": plan.is_broadcast,
                    }),
                )
                .await;
                let _ = record_message_router_journal_event(
                    &self.state,
                    &context,
                    session_id.as_str(),
                    run_id.as_str(),
                    "message.replied",
                    common_v1::journal_event::EventActor::System as i32,
                    json!({
                        "event": "message.replied",
                        "envelope_id": envelope_id,
                        "channel": plan.channel.clone(),
                        "reply_preview": truncate_with_ellipsis(reply_text.clone(), 256),
                    }),
                )
                .await;

                self.state.counters.channel_messages_replied.fetch_add(1, Ordering::Relaxed);
                self.state
                    .counters
                    .channel_router_queue_depth
                    .store(self.state.channel_router.queue_depth() as u64, Ordering::Relaxed);
                return Ok(Response::new(gateway_v1::RouteMessageResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    accepted: true,
                    queued_for_retry: false,
                    decision_reason: "routed".to_owned(),
                    session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                    run_id: Some(common_v1::CanonicalId { ulid: run_id }),
                    outputs: vec![gateway_v1::OutboundMessage {
                        text: reply_text,
                        attachments: Vec::new(),
                        thread_id: plan.reply_thread_id.unwrap_or_default(),
                        in_reply_to_message_id: plan.in_reply_to_message_id.unwrap_or_default(),
                        broadcast: plan.is_broadcast,
                        auto_ack_text: plan.auto_ack_text.unwrap_or_default(),
                        auto_reaction: plan.auto_reaction.unwrap_or_default(),
                    }],
                    route_key: plan.route_key,
                    retry_attempt,
                    queue_depth: self.state.channel_router.queue_depth() as u32,
                }));
            }
        }
    }

    async fn list_agents(
        &self,
        request: Request<gateway_v1::ListAgentsRequest>,
    ) -> Result<Response<gateway_v1::ListAgentsResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "ListAgents")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        authorize_agent_management_action(
            context.principal.as_str(),
            "agent.list",
            "agent:registry",
        )
        .inspect_err(|_error| {
            self.state.record_denied();
        })?;
        let page = self
            .state
            .list_agents(non_empty(payload.after_agent_id), Some(payload.limit as usize))
            .await?;
        Ok(Response::new(gateway_v1::ListAgentsResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            agents: page.agents.iter().map(agent_message).collect(),
            default_agent_id: page.default_agent_id.unwrap_or_default(),
            next_after_agent_id: page.next_after_agent_id.unwrap_or_default(),
        }))
    }

    async fn get_agent(
        &self,
        request: Request<gateway_v1::GetAgentRequest>,
    ) -> Result<Response<gateway_v1::GetAgentResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "GetAgent")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        authorize_agent_management_action(
            context.principal.as_str(),
            "agent.get",
            "agent:registry",
        )
        .inspect_err(|_error| {
            self.state.record_denied();
        })?;
        let agent_id = normalize_agent_identifier(payload.agent_id.as_str(), "agent_id")
            .inspect_err(|_error| {
                self.state.counters.agent_validation_failures.fetch_add(1, Ordering::Relaxed);
            })?;
        let (agent, is_default) = self.state.get_agent(agent_id).await?;
        Ok(Response::new(gateway_v1::GetAgentResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            agent: Some(agent_message(&agent)),
            is_default,
        }))
    }

    async fn create_agent(
        &self,
        request: Request<gateway_v1::CreateAgentRequest>,
    ) -> Result<Response<gateway_v1::CreateAgentResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "CreateAgent")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        authorize_agent_management_action(
            context.principal.as_str(),
            "agent.create",
            "agent:registry",
        )
        .inspect_err(|_error| {
            self.state.record_denied();
        })?;
        let outcome = self
            .state
            .create_agent(AgentCreateRequest {
                agent_id: payload.agent_id,
                display_name: payload.display_name,
                agent_dir: non_empty(payload.agent_dir),
                workspace_roots: payload.workspace_roots,
                default_model_profile: non_empty(payload.default_model_profile),
                default_tool_allowlist: payload.default_tool_allowlist,
                default_skill_allowlist: payload.default_skill_allowlist,
                set_default: payload.set_default,
                allow_absolute_paths: payload.allow_absolute_paths,
            })
            .await?;
        let journal_payload = json!({
            "event": "agent.created",
            "agent_id": outcome.agent.agent_id,
            "display_name": outcome.agent.display_name,
            "agent_dir": outcome.agent.agent_dir,
            "workspace_roots": outcome.agent.workspace_roots,
            "default_model_profile": outcome.agent.default_model_profile,
            "default_changed": outcome.default_changed,
            "default_agent_id": outcome.default_agent_id,
        });
        let _ = record_agent_journal_event(&self.state, &context, journal_payload).await;
        if outcome.default_changed {
            let _ = record_agent_journal_event(
                &self.state,
                &context,
                json!({
                    "event": "agent.default_changed",
                    "previous_default_agent_id": outcome.previous_default_agent_id,
                    "default_agent_id": outcome.default_agent_id,
                }),
            )
            .await;
        }
        Ok(Response::new(gateway_v1::CreateAgentResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            agent: Some(agent_message(&outcome.agent)),
            default_changed: outcome.default_changed,
            default_agent_id: outcome.default_agent_id.unwrap_or_default(),
        }))
    }

    async fn set_default_agent(
        &self,
        request: Request<gateway_v1::SetDefaultAgentRequest>,
    ) -> Result<Response<gateway_v1::SetDefaultAgentResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "SetDefaultAgent")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        authorize_agent_management_action(
            context.principal.as_str(),
            "agent.set_default",
            "agent:registry",
        )
        .inspect_err(|_error| {
            self.state.record_denied();
        })?;
        let agent_id = normalize_agent_identifier(payload.agent_id.as_str(), "agent_id")
            .inspect_err(|_error| {
                self.state.counters.agent_validation_failures.fetch_add(1, Ordering::Relaxed);
            })?;
        let outcome = self.state.set_default_agent(agent_id).await?;
        let _ = record_agent_journal_event(
            &self.state,
            &context,
            json!({
                "event": "agent.default_changed",
                "previous_default_agent_id": outcome.previous_default_agent_id,
                "default_agent_id": outcome.default_agent_id,
            }),
        )
        .await;
        Ok(Response::new(gateway_v1::SetDefaultAgentResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            previous_agent_id: outcome.previous_default_agent_id.unwrap_or_default(),
            default_agent_id: outcome.default_agent_id,
        }))
    }

    async fn resolve_agent_for_context(
        &self,
        request: Request<gateway_v1::ResolveAgentForContextRequest>,
    ) -> Result<Response<gateway_v1::ResolveAgentForContextResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "ResolveAgentForContext")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        authorize_agent_management_action(
            context.principal.as_str(),
            "agent.resolve",
            "agent:registry",
        )
        .inspect_err(|_error| {
            self.state.record_denied();
        })?;
        let principal = if let Some(value) = non_empty(payload.principal) {
            if value != context.principal {
                self.state.record_denied();
                return Err(Status::permission_denied(
                    "resolve agent principal must match authenticated principal",
                ));
            }
            value
        } else {
            context.principal.clone()
        };
        let session_id =
            optional_canonical_id(payload.session_id, "session_id").inspect_err(|_error| {
                self.state.counters.agent_validation_failures.fetch_add(1, Ordering::Relaxed);
            })?;
        let outcome = self
            .state
            .resolve_agent_for_context(AgentResolveRequest {
                principal,
                channel: non_empty(payload.channel),
                session_id,
                preferred_agent_id: non_empty(payload.preferred_agent_id),
                persist_session_binding: payload.persist_session_binding,
            })
            .await?;
        if outcome.binding_created {
            let _ = record_agent_journal_event(
                &self.state,
                &context,
                json!({
                    "event": "agent.updated",
                    "agent_id": outcome.agent.agent_id,
                    "binding_created": true,
                }),
            )
            .await;
        }
        Ok(Response::new(gateway_v1::ResolveAgentForContextResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            agent: Some(agent_message(&outcome.agent)),
            source: agent_resolution_source_to_proto(outcome.source),
            binding_created: outcome.binding_created,
            is_default: outcome.is_default,
        }))
    }

    async fn run_stream(
        &self,
        request: Request<Streaming<common_v1::RunStreamRequest>>,
    ) -> Result<Response<Self::RunStreamStream>, Status> {
        if !self.state.is_orchestrator_runloop_enabled() {
            self.state.record_denied();
            return Err(Status::failed_precondition(
                "orchestrator run loop v1 is disabled; set PALYRA_ORCHESTRATOR_RUNLOOP_V1_ENABLED=true",
            ));
        }
        let context = self.authorize_rpc(request.metadata(), "RunStream")?;
        self.state.counters.run_stream_requests.fetch_add(1, Ordering::Relaxed);

        let mut stream = request.into_inner();
        let (sender, receiver) = mpsc::channel(16);
        let context_for_stream = context.clone();
        let state_for_stream = self.state.clone();

        tokio::spawn(async move {
            let mut active_session_id = None::<String>;
            let mut active_run_id = None::<String>;
            let mut run_state = RunStateMachine::default();
            let mut tape_seq = 0_i64;
            let mut model_token_tape_events = 0_usize;
            let mut model_token_compaction_emitted = false;
            let mut in_progress_emitted = false;
            let mut remaining_tool_budget = state_for_stream.config.tool_call.max_calls_per_run;

            while let Some(item) = stream.next().await {
                let message = match item {
                    Ok(value) => value,
                    Err(error) => {
                        let status =
                            Status::internal(format!("failed to read run stream request: {error}"));
                        finalize_run_failure(
                            &sender,
                            &state_for_stream,
                            &mut run_state,
                            active_run_id.as_deref(),
                            &mut tape_seq,
                            status.message(),
                        )
                        .await;
                        let _ = sender.send(Err(status)).await;
                        return;
                    }
                };
                if message.v != CANONICAL_PROTOCOL_MAJOR {
                    let status = Status::failed_precondition("unsupported protocol major version");
                    finalize_run_failure(
                        &sender,
                        &state_for_stream,
                        &mut run_state,
                        active_run_id.as_deref(),
                        &mut tape_seq,
                        status.message(),
                    )
                    .await;
                    let _ = sender.send(Err(status)).await;
                    return;
                }
                if message.allow_sensitive_tools {
                    state_for_stream.record_denied();
                    let status = Status::permission_denied(format!(
                        "decision=deny_by_default approval_required=true reason={SENSITIVE_TOOLS_DENY_REASON}",
                    ));
                    finalize_run_failure(
                        &sender,
                        &state_for_stream,
                        &mut run_state,
                        active_run_id.as_deref(),
                        &mut tape_seq,
                        SENSITIVE_TOOLS_DENY_REASON,
                    )
                    .await;
                    let _ = sender.send(Err(status)).await;
                    return;
                }

                let session_id = match canonical_id(message.session_id, "session_id") {
                    Ok(value) => value,
                    Err(error) => {
                        finalize_run_failure(
                            &sender,
                            &state_for_stream,
                            &mut run_state,
                            active_run_id.as_deref(),
                            &mut tape_seq,
                            error.message(),
                        )
                        .await;
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                };
                let run_id = match canonical_id(message.run_id, "run_id") {
                    Ok(value) => value,
                    Err(error) => {
                        finalize_run_failure(
                            &sender,
                            &state_for_stream,
                            &mut run_state,
                            active_run_id.as_deref(),
                            &mut tape_seq,
                            error.message(),
                        )
                        .await;
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                };

                if let Some(expected_session) = active_session_id.as_ref() {
                    if expected_session != &session_id {
                        let status = Status::invalid_argument(
                            "run stream cannot switch session_id mid-stream",
                        );
                        finalize_run_failure(
                            &sender,
                            &state_for_stream,
                            &mut run_state,
                            active_run_id.as_deref(),
                            &mut tape_seq,
                            status.message(),
                        )
                        .await;
                        let _ = sender.send(Err(status)).await;
                        return;
                    }
                }
                if let Some(expected_run) = active_run_id.as_ref() {
                    if expected_run != &run_id {
                        let status =
                            Status::invalid_argument("run stream cannot switch run_id mid-stream");
                        finalize_run_failure(
                            &sender,
                            &state_for_stream,
                            &mut run_state,
                            active_run_id.as_deref(),
                            &mut tape_seq,
                            status.message(),
                        )
                        .await;
                        let _ = sender.send(Err(status)).await;
                        return;
                    }
                }

                if active_run_id.is_none() {
                    if let Err(error) = run_state.transition(RunTransition::Accept) {
                        let status = Status::internal(error.to_string());
                        finalize_run_failure(
                            &sender,
                            &state_for_stream,
                            &mut run_state,
                            active_run_id.as_deref(),
                            &mut tape_seq,
                            status.message(),
                        )
                        .await;
                        let _ = sender.send(Err(status)).await;
                        return;
                    }
                    let resolved_session = state_for_stream
                        .resolve_orchestrator_session(OrchestratorSessionResolveRequest {
                            session_id: Some(session_id.clone()),
                            session_key: non_empty(message.session_key.clone()),
                            session_label: non_empty(message.session_label.clone()),
                            principal: context_for_stream.principal.clone(),
                            device_id: context_for_stream.device_id.clone(),
                            channel: context_for_stream.channel.clone(),
                            require_existing: message.require_existing,
                            reset_session: message.reset_session,
                        })
                        .await;
                    let resolved_session = match resolved_session {
                        Ok(outcome) => outcome,
                        Err(error) => {
                            finalize_run_failure(
                                &sender,
                                &state_for_stream,
                                &mut run_state,
                                active_run_id.as_deref(),
                                &mut tape_seq,
                                error.message(),
                            )
                            .await;
                            let _ = sender.send(Err(error)).await;
                            return;
                        }
                    };
                    if message.reset_session {
                        state_for_stream.clear_tool_approval_cache_for_session(
                            &context_for_stream,
                            session_id.as_str(),
                        );
                    }
                    if resolved_session.session.session_id != session_id {
                        let status = Status::failed_precondition(
                            "resolved session_id does not match RunStream session_id",
                        );
                        finalize_run_failure(
                            &sender,
                            &state_for_stream,
                            &mut run_state,
                            active_run_id.as_deref(),
                            &mut tape_seq,
                            status.message(),
                        )
                        .await;
                        let _ = sender.send(Err(status)).await;
                        return;
                    }
                    if let Err(error) = state_for_stream
                        .start_orchestrator_run(OrchestratorRunStartRequest {
                            run_id: run_id.clone(),
                            session_id: session_id.clone(),
                        })
                        .await
                    {
                        finalize_run_failure(
                            &sender,
                            &state_for_stream,
                            &mut run_state,
                            active_run_id.as_deref(),
                            &mut tape_seq,
                            error.message(),
                        )
                        .await;
                        let _ = sender.send(Err(error)).await;
                        return;
                    }

                    active_session_id = Some(session_id.clone());
                    active_run_id = Some(run_id.clone());

                    let accepted_message = format!(
                        "accepted session={session_id} principal={}",
                        context_for_stream.principal
                    );
                    if let Err(error) = send_status_with_tape(
                        &sender,
                        &state_for_stream,
                        run_id.as_str(),
                        &mut tape_seq,
                        common_v1::stream_status::StatusKind::Accepted,
                        accepted_message.as_str(),
                    )
                    .await
                    {
                        finalize_run_failure(
                            &sender,
                            &state_for_stream,
                            &mut run_state,
                            active_run_id.as_deref(),
                            &mut tape_seq,
                            error.message(),
                        )
                        .await;
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                }

                let input_envelope = message.input.unwrap_or_default();
                let input_content = input_envelope.content.unwrap_or_default();
                let input_text = input_content.text;
                let vision_requested = input_content.attachments.iter().any(|attachment| {
                    attachment.kind == common_v1::message_attachment::AttachmentKind::Image as i32
                });
                let json_mode_requested = input_envelope
                    .security
                    .as_ref()
                    .map(|security| {
                        security.labels.iter().any(|label| label.eq_ignore_ascii_case("json_mode"))
                    })
                    .unwrap_or(false);
                let session_id_for_message = if let Some(session_id) = active_session_id.as_deref()
                {
                    session_id.to_owned()
                } else {
                    let status = Status::internal(
                        "run stream internal invariant violated: missing session_id for message",
                    );
                    finalize_run_failure(
                        &sender,
                        &state_for_stream,
                        &mut run_state,
                        active_run_id.as_deref(),
                        &mut tape_seq,
                        status.message(),
                    )
                    .await;
                    let _ = sender.send(Err(status)).await;
                    return;
                };

                ingest_memory_best_effort(
                    &state_for_stream,
                    context_for_stream.principal.as_str(),
                    context_for_stream.channel.as_deref(),
                    Some(session_id_for_message.as_str()),
                    MemorySource::TapeUserMessage,
                    input_text.as_str(),
                    Vec::new(),
                    Some(0.9),
                    "run_stream_user_input",
                )
                .await;

                let provider_input_text = match build_memory_augmented_prompt(
                    &state_for_stream,
                    &context_for_stream,
                    run_id.as_str(),
                    &mut tape_seq,
                    session_id_for_message.as_str(),
                    input_text.as_str(),
                )
                .await
                {
                    Ok(value) => value,
                    Err(error) => {
                        finalize_run_failure(
                            &sender,
                            &state_for_stream,
                            &mut run_state,
                            active_run_id.as_deref(),
                            &mut tape_seq,
                            error.message(),
                        )
                        .await;
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                };

                if is_cancel_command(input_text.as_str()) {
                    if let Err(error) = state_for_stream
                        .request_orchestrator_cancel(OrchestratorCancelRequest {
                            run_id: run_id.clone(),
                            reason: "stream_cancel_command".to_owned(),
                        })
                        .await
                    {
                        finalize_run_failure(
                            &sender,
                            &state_for_stream,
                            &mut run_state,
                            active_run_id.as_deref(),
                            &mut tape_seq,
                            error.message(),
                        )
                        .await;
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                }

                match state_for_stream.is_orchestrator_cancel_requested(run_id.clone()).await {
                    Ok(true) => {
                        if let Err(error) = run_state.transition(RunTransition::Cancel) {
                            let status = Status::internal(error.to_string());
                            finalize_run_failure(
                                &sender,
                                &state_for_stream,
                                &mut run_state,
                                active_run_id.as_deref(),
                                &mut tape_seq,
                                status.message(),
                            )
                            .await;
                            let _ = sender.send(Err(status)).await;
                            return;
                        }
                        if let Err(error) = state_for_stream
                            .update_orchestrator_run_state(
                                run_id.clone(),
                                RunLifecycleState::Cancelled,
                                Some(CANCELLED_REASON.to_owned()),
                            )
                            .await
                        {
                            finalize_run_failure(
                                &sender,
                                &state_for_stream,
                                &mut run_state,
                                active_run_id.as_deref(),
                                &mut tape_seq,
                                error.message(),
                            )
                            .await;
                            let _ = sender.send(Err(error)).await;
                            return;
                        }
                        if let Err(error) = send_status_with_tape(
                            &sender,
                            &state_for_stream,
                            run_id.as_str(),
                            &mut tape_seq,
                            common_v1::stream_status::StatusKind::Failed,
                            CANCELLED_REASON,
                        )
                        .await
                        {
                            let _ = sender.send(Err(error)).await;
                        }
                        return;
                    }
                    Ok(false) => {}
                    Err(error) => {
                        finalize_run_failure(
                            &sender,
                            &state_for_stream,
                            &mut run_state,
                            active_run_id.as_deref(),
                            &mut tape_seq,
                            error.message(),
                        )
                        .await;
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                }

                if !in_progress_emitted {
                    if let Err(error) = run_state.transition(RunTransition::StartStreaming) {
                        let status = Status::internal(error.to_string());
                        finalize_run_failure(
                            &sender,
                            &state_for_stream,
                            &mut run_state,
                            active_run_id.as_deref(),
                            &mut tape_seq,
                            status.message(),
                        )
                        .await;
                        let _ = sender.send(Err(status)).await;
                        return;
                    }
                    if let Err(error) = state_for_stream
                        .update_orchestrator_run_state(
                            run_id.clone(),
                            RunLifecycleState::InProgress,
                            None,
                        )
                        .await
                    {
                        finalize_run_failure(
                            &sender,
                            &state_for_stream,
                            &mut run_state,
                            active_run_id.as_deref(),
                            &mut tape_seq,
                            error.message(),
                        )
                        .await;
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                    if let Err(error) = send_status_with_tape(
                        &sender,
                        &state_for_stream,
                        run_id.as_str(),
                        &mut tape_seq,
                        common_v1::stream_status::StatusKind::InProgress,
                        "streaming",
                    )
                    .await
                    {
                        finalize_run_failure(
                            &sender,
                            &state_for_stream,
                            &mut run_state,
                            active_run_id.as_deref(),
                            &mut tape_seq,
                            error.message(),
                        )
                        .await;
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                    in_progress_emitted = true;
                }

                let mut provider_future =
                    Box::pin(state_for_stream.execute_model_provider(ProviderRequest {
                        input_text: provider_input_text,
                        json_mode: json_mode_requested,
                        vision_requested,
                    }));
                let mut cancel_poll = interval(Duration::from_millis(100));
                cancel_poll.set_missed_tick_behavior(MissedTickBehavior::Delay);

                let provider_response = loop {
                    tokio::select! {
                        provider_result = &mut provider_future => {
                            match provider_result {
                                Ok(response) => break response,
                                Err(error) => {
                                    finalize_run_failure(
                                        &sender,
                                        &state_for_stream,
                                        &mut run_state,
                                        active_run_id.as_deref(),
                                        &mut tape_seq,
                                        error.message(),
                                    )
                                    .await;
                                    let _ = sender.send(Err(error)).await;
                                    return;
                                }
                            }
                        }
                        _ = cancel_poll.tick() => {
                            match state_for_stream.is_orchestrator_cancel_requested(run_id.clone()).await {
                                Ok(true) => {
                                    if let Err(error) = run_state.transition(RunTransition::Cancel) {
                                        let status = Status::internal(error.to_string());
                                        finalize_run_failure(
                                            &sender,
                                            &state_for_stream,
                                            &mut run_state,
                                            active_run_id.as_deref(),
                                            &mut tape_seq,
                                            status.message(),
                                        )
                                        .await;
                                        let _ = sender.send(Err(status)).await;
                                        return;
                                    }
                                    if let Err(error) = state_for_stream
                                        .update_orchestrator_run_state(
                                            run_id.clone(),
                                            RunLifecycleState::Cancelled,
                                            Some(CANCELLED_REASON.to_owned()),
                                        )
                                        .await
                                    {
                                        finalize_run_failure(
                                            &sender,
                                            &state_for_stream,
                                            &mut run_state,
                                            active_run_id.as_deref(),
                                            &mut tape_seq,
                                            error.message(),
                                        )
                                        .await;
                                        let _ = sender.send(Err(error)).await;
                                        return;
                                    }
                                    if let Err(error) = send_status_with_tape(
                                        &sender,
                                        &state_for_stream,
                                        run_id.as_str(),
                                        &mut tape_seq,
                                        common_v1::stream_status::StatusKind::Failed,
                                        CANCELLED_REASON,
                                    )
                                    .await
                                    {
                                        let _ = sender.send(Err(error)).await;
                                    }
                                    return;
                                }
                                Ok(false) => {}
                                Err(error) => {
                                    finalize_run_failure(
                                        &sender,
                                        &state_for_stream,
                                        &mut run_state,
                                        active_run_id.as_deref(),
                                        &mut tape_seq,
                                        error.message(),
                                    )
                                    .await;
                                    let _ = sender.send(Err(error)).await;
                                    return;
                                }
                            }
                        }
                    }
                };

                if let Err(error) = state_for_stream
                    .add_orchestrator_usage(OrchestratorUsageDelta {
                        run_id: run_id.clone(),
                        prompt_tokens_delta: provider_response.prompt_tokens,
                        completion_tokens_delta: 0,
                    })
                    .await
                {
                    finalize_run_failure(
                        &sender,
                        &state_for_stream,
                        &mut run_state,
                        active_run_id.as_deref(),
                        &mut tape_seq,
                        error.message(),
                    )
                    .await;
                    let _ = sender.send(Err(error)).await;
                    return;
                }

                let mut summary_tokens = Vec::new();
                for provider_event in provider_response.events {
                    match state_for_stream.is_orchestrator_cancel_requested(run_id.clone()).await {
                        Ok(true) => {
                            if let Err(error) = run_state.transition(RunTransition::Cancel) {
                                let status = Status::internal(error.to_string());
                                finalize_run_failure(
                                    &sender,
                                    &state_for_stream,
                                    &mut run_state,
                                    active_run_id.as_deref(),
                                    &mut tape_seq,
                                    status.message(),
                                )
                                .await;
                                let _ = sender.send(Err(status)).await;
                                return;
                            }
                            if let Err(error) = state_for_stream
                                .update_orchestrator_run_state(
                                    run_id.clone(),
                                    RunLifecycleState::Cancelled,
                                    Some(CANCELLED_REASON.to_owned()),
                                )
                                .await
                            {
                                finalize_run_failure(
                                    &sender,
                                    &state_for_stream,
                                    &mut run_state,
                                    active_run_id.as_deref(),
                                    &mut tape_seq,
                                    error.message(),
                                )
                                .await;
                                let _ = sender.send(Err(error)).await;
                                return;
                            }
                            if let Err(error) = send_status_with_tape(
                                &sender,
                                &state_for_stream,
                                run_id.as_str(),
                                &mut tape_seq,
                                common_v1::stream_status::StatusKind::Failed,
                                CANCELLED_REASON,
                            )
                            .await
                            {
                                let _ = sender.send(Err(error)).await;
                            }
                            return;
                        }
                        Ok(false) => {}
                        Err(error) => {
                            finalize_run_failure(
                                &sender,
                                &state_for_stream,
                                &mut run_state,
                                active_run_id.as_deref(),
                                &mut tape_seq,
                                error.message(),
                            )
                            .await;
                            let _ = sender.send(Err(error)).await;
                            return;
                        }
                    }

                    match provider_event {
                        ProviderEvent::ModelToken { token, is_final } => {
                            if !token.trim().is_empty() {
                                summary_tokens.push(token.clone());
                            }
                            if let Err(error) = send_model_token_with_tape(
                                &sender,
                                &state_for_stream,
                                run_id.as_str(),
                                &mut tape_seq,
                                &mut model_token_tape_events,
                                &mut model_token_compaction_emitted,
                                token.as_str(),
                                is_final,
                            )
                            .await
                            {
                                finalize_run_failure(
                                    &sender,
                                    &state_for_stream,
                                    &mut run_state,
                                    active_run_id.as_deref(),
                                    &mut tape_seq,
                                    error.message(),
                                )
                                .await;
                                let _ = sender.send(Err(error)).await;
                                return;
                            }
                        }
                        ProviderEvent::ToolProposal { proposal_id, tool_name, input_json } => {
                            let mut skill_gate_decision: Option<ToolDecision> = None;
                            let skill_context = match parse_tool_skill_context(
                                tool_name.as_str(),
                                input_json.as_slice(),
                            ) {
                                Ok(context) => context,
                                Err(error) => {
                                    warn!(
                                        run_id = %run_id,
                                        proposal_id = %proposal_id,
                                        tool_name = %tool_name,
                                        error = %error.message(),
                                        "skill context parsing failed; proposal will be denied safely"
                                    );
                                    skill_gate_decision = Some(ToolDecision {
                                        allowed: false,
                                        reason: format!(
                                            "{SKILL_EXECUTION_DENY_REASON_PREFIX}: invalid skill context: {}",
                                            error.message()
                                        ),
                                        approval_required: false,
                                        policy_enforced: true,
                                    });
                                    None
                                }
                            };
                            if skill_gate_decision.is_none() {
                                if let Some(skill_context) = skill_context.as_ref() {
                                    skill_gate_decision = match evaluate_skill_execution_gate(
                                        &state_for_stream,
                                        &context_for_stream,
                                        skill_context,
                                    )
                                    .await
                                    {
                                        Ok(value) => value,
                                        Err(error) => Some(ToolDecision {
                                            allowed: false,
                                            reason: format!(
                                                "{SKILL_EXECUTION_DENY_REASON_PREFIX}: skill={} evaluation_error={}",
                                                skill_context.skill_id,
                                                error.message()
                                            ),
                                            approval_required: false,
                                            policy_enforced: true,
                                        }),
                                    };
                                }
                            }
                            let proposal_approval_required = skill_gate_decision
                                .as_ref()
                                .map(|decision| {
                                    decision.allowed && tool_requires_approval(tool_name.as_str())
                                })
                                .unwrap_or_else(|| tool_requires_approval(tool_name.as_str()));
                            let approval_subject_id = build_tool_approval_subject_id(
                                tool_name.as_str(),
                                skill_context.as_ref(),
                            );
                            state_for_stream
                                .counters
                                .tool_proposals
                                .fetch_add(1, Ordering::Relaxed);
                            if let Err(error) = send_tool_proposal_with_tape(
                                &sender,
                                &state_for_stream,
                                run_id.as_str(),
                                &mut tape_seq,
                                proposal_id.as_str(),
                                tool_name.as_str(),
                                input_json.as_slice(),
                                proposal_approval_required,
                            )
                            .await
                            {
                                finalize_run_failure(
                                    &sender,
                                    &state_for_stream,
                                    &mut run_state,
                                    active_run_id.as_deref(),
                                    &mut tape_seq,
                                    error.message(),
                                )
                                .await;
                                let _ = sender.send(Err(error)).await;
                                return;
                            }
                            let mut cached_approval_outcome = if proposal_approval_required {
                                state_for_stream.resolve_cached_tool_approval(
                                    &context_for_stream,
                                    session_id.as_str(),
                                    approval_subject_id.as_str(),
                                )
                            } else {
                                None
                            };

                            let approval_outcome = if proposal_approval_required {
                                if let Some(cached_outcome) = cached_approval_outcome.take() {
                                    info!(
                                        run_id = %run_id,
                                        proposal_id = %proposal_id,
                                        approval_id = %cached_outcome.approval_id,
                                        subject_id = %approval_subject_id,
                                        decision = %cached_outcome.decision.as_str(),
                                        decision_scope = %cached_outcome.decision_scope.as_str(),
                                        "reusing cached tool approval decision"
                                    );
                                    if let Err(error) = send_tool_approval_response_with_tape(
                                        &sender,
                                        &state_for_stream,
                                        run_id.as_str(),
                                        &mut tape_seq,
                                        proposal_id.as_str(),
                                        cached_outcome.approval_id.as_str(),
                                        cached_outcome.approved,
                                        cached_outcome.reason.as_str(),
                                        cached_outcome.decision_scope,
                                        cached_outcome.decision_scope_ttl_ms,
                                    )
                                    .await
                                    {
                                        finalize_run_failure(
                                            &sender,
                                            &state_for_stream,
                                            &mut run_state,
                                            active_run_id.as_deref(),
                                            &mut tape_seq,
                                            error.message(),
                                        )
                                        .await;
                                        let _ = sender.send(Err(error)).await;
                                        return;
                                    }
                                    Some(cached_outcome)
                                } else {
                                    let pending_approval = build_pending_tool_approval(
                                        tool_name.as_str(),
                                        skill_context.as_ref(),
                                        input_json.as_slice(),
                                        &state_for_stream.config.tool_call,
                                    );
                                    if let Err(error) = state_for_stream
                                        .create_approval_record(ApprovalCreateRequest {
                                            approval_id: pending_approval.approval_id.clone(),
                                            session_id: session_id.clone(),
                                            run_id: run_id.clone(),
                                            principal: context_for_stream.principal.clone(),
                                            device_id: context_for_stream.device_id.clone(),
                                            channel: context_for_stream.channel.clone(),
                                            subject_type: ApprovalSubjectType::Tool,
                                            subject_id: pending_approval.prompt.subject_id.clone(),
                                            request_summary: pending_approval
                                                .request_summary
                                                .clone(),
                                            policy_snapshot: pending_approval
                                                .policy_snapshot
                                                .clone(),
                                            prompt: pending_approval.prompt.clone(),
                                        })
                                        .await
                                    {
                                        finalize_run_failure(
                                            &sender,
                                            &state_for_stream,
                                            &mut run_state,
                                            active_run_id.as_deref(),
                                            &mut tape_seq,
                                            error.message(),
                                        )
                                        .await;
                                        let _ = sender.send(Err(error)).await;
                                        return;
                                    }
                                    info!(
                                        run_id = %run_id,
                                        proposal_id = %proposal_id,
                                        approval_id = %pending_approval.approval_id,
                                        subject_id = %pending_approval.prompt.subject_id,
                                        "approval requested"
                                    );

                                    if let Err(error) = send_tool_approval_request_with_tape(
                                        &sender,
                                        &state_for_stream,
                                        run_id.as_str(),
                                        &mut tape_seq,
                                        proposal_id.as_str(),
                                        pending_approval.approval_id.as_str(),
                                        tool_name.as_str(),
                                        input_json.as_slice(),
                                        true,
                                        pending_approval.request_summary.as_str(),
                                        &pending_approval.prompt,
                                    )
                                    .await
                                    {
                                        best_effort_mark_approval_error(
                                            &state_for_stream,
                                            pending_approval.approval_id.as_str(),
                                            format!(
                                                "approval_request_dispatch_error: {}",
                                                error.message()
                                            ),
                                        )
                                        .await;
                                        finalize_run_failure(
                                            &sender,
                                            &state_for_stream,
                                            &mut run_state,
                                            active_run_id.as_deref(),
                                            &mut tape_seq,
                                            error.message(),
                                        )
                                        .await;
                                        let _ = sender.send(Err(error)).await;
                                        return;
                                    }
                                    if let Err(error) = record_approval_requested_journal_event(
                                        &state_for_stream,
                                        &context_for_stream,
                                        session_id.as_str(),
                                        run_id.as_str(),
                                        proposal_id.as_str(),
                                        pending_approval.approval_id.as_str(),
                                        tool_name.as_str(),
                                        pending_approval.prompt.subject_id.as_str(),
                                        pending_approval.request_summary.as_str(),
                                        &pending_approval.policy_snapshot,
                                        &pending_approval.prompt,
                                    )
                                    .await
                                    {
                                        best_effort_mark_approval_error(
                                            &state_for_stream,
                                            pending_approval.approval_id.as_str(),
                                            format!(
                                                "approval_request_journal_error: {}",
                                                error.message()
                                            ),
                                        )
                                        .await;
                                        finalize_run_failure(
                                            &sender,
                                            &state_for_stream,
                                            &mut run_state,
                                            active_run_id.as_deref(),
                                            &mut tape_seq,
                                            error.message(),
                                        )
                                        .await;
                                        let _ = sender.send(Err(error)).await;
                                        return;
                                    }

                                    let response = match timeout(
                                        TOOL_APPROVAL_RESPONSE_TIMEOUT,
                                        await_tool_approval_response(
                                            &mut stream,
                                            session_id.as_str(),
                                            run_id.as_str(),
                                            proposal_id.as_str(),
                                            pending_approval.approval_id.as_str(),
                                        ),
                                    )
                                    .await
                                    {
                                        Ok(Ok(value)) => value,
                                        Ok(Err(error)) => ToolApprovalOutcome {
                                            approval_id: pending_approval.approval_id.clone(),
                                            approved: false,
                                            reason: format!(
                                                "approval_response_error: {}",
                                                error.message()
                                            ),
                                            decision: ApprovalDecision::Error,
                                            decision_scope: ApprovalDecisionScope::Once,
                                            decision_scope_ttl_ms: None,
                                        },
                                        Err(_) => ToolApprovalOutcome {
                                            approval_id: pending_approval.approval_id.clone(),
                                            approved: false,
                                            reason: "approval_response_timeout".to_owned(),
                                            decision: ApprovalDecision::Timeout,
                                            decision_scope: ApprovalDecisionScope::Once,
                                            decision_scope_ttl_ms: None,
                                        },
                                    };

                                    let resolved = match state_for_stream
                                        .resolve_approval_record(ApprovalResolveRequest {
                                            approval_id: pending_approval.approval_id.clone(),
                                            decision: response.decision,
                                            decision_scope: response.decision_scope,
                                            decision_reason: response.reason.clone(),
                                            decision_scope_ttl_ms: response.decision_scope_ttl_ms,
                                        })
                                        .await
                                    {
                                        Ok(value) => value,
                                        Err(error) => {
                                            finalize_run_failure(
                                                &sender,
                                                &state_for_stream,
                                                &mut run_state,
                                                active_run_id.as_deref(),
                                                &mut tape_seq,
                                                error.message(),
                                            )
                                            .await;
                                            let _ = sender.send(Err(error)).await;
                                            return;
                                        }
                                    };
                                    info!(
                                        run_id = %run_id,
                                        proposal_id = %proposal_id,
                                        approval_id = %resolved.approval_id,
                                        decision = %response.decision.as_str(),
                                        decision_scope = %response.decision_scope.as_str(),
                                        "approval resolved"
                                    );
                                    if let Err(error) = record_approval_resolved_journal_event(
                                        &state_for_stream,
                                        &context_for_stream,
                                        session_id.as_str(),
                                        run_id.as_str(),
                                        proposal_id.as_str(),
                                        response.approval_id.as_str(),
                                        response.decision,
                                        response.decision_scope,
                                        response.decision_scope_ttl_ms,
                                        response.reason.as_str(),
                                    )
                                    .await
                                    {
                                        finalize_run_failure(
                                            &sender,
                                            &state_for_stream,
                                            &mut run_state,
                                            active_run_id.as_deref(),
                                            &mut tape_seq,
                                            error.message(),
                                        )
                                        .await;
                                        let _ = sender.send(Err(error)).await;
                                        return;
                                    }

                                    if let Err(error) = send_tool_approval_response_with_tape(
                                        &sender,
                                        &state_for_stream,
                                        run_id.as_str(),
                                        &mut tape_seq,
                                        proposal_id.as_str(),
                                        response.approval_id.as_str(),
                                        response.approved,
                                        response.reason.as_str(),
                                        response.decision_scope,
                                        response.decision_scope_ttl_ms,
                                    )
                                    .await
                                    {
                                        finalize_run_failure(
                                            &sender,
                                            &state_for_stream,
                                            &mut run_state,
                                            active_run_id.as_deref(),
                                            &mut tape_seq,
                                            error.message(),
                                        )
                                        .await;
                                        let _ = sender.send(Err(error)).await;
                                        return;
                                    }
                                    state_for_stream.remember_tool_approval(
                                        &context_for_stream,
                                        session_id.as_str(),
                                        approval_subject_id.as_str(),
                                        &response,
                                    );
                                    Some(response)
                                }
                            } else {
                                None
                            };

                            let decision = if let Some(skill_gate_decision) = skill_gate_decision {
                                skill_gate_decision
                            } else {
                                let policy_request_context = ToolRequestContext {
                                    principal: context_for_stream.principal.clone(),
                                    device_id: Some(context_for_stream.device_id.clone()),
                                    channel: context_for_stream.channel.clone(),
                                    session_id: active_session_id.clone(),
                                    run_id: Some(run_id.clone()),
                                    skill_id: skill_context
                                        .as_ref()
                                        .map(|context| context.skill_id.clone()),
                                };
                                let decision = decide_tool_call(
                                    &state_for_stream.config.tool_call,
                                    &mut remaining_tool_budget,
                                    &policy_request_context,
                                    tool_name.as_str(),
                                    approval_outcome
                                        .as_ref()
                                        .map(|response| response.approved)
                                        .unwrap_or(false),
                                );
                                apply_tool_approval_outcome(
                                    decision,
                                    tool_name.as_str(),
                                    approval_outcome.as_ref(),
                                )
                            };
                            if decision.allowed {
                                state_for_stream
                                    .counters
                                    .tool_decisions_allowed
                                    .fetch_add(1, Ordering::Relaxed);
                            } else {
                                state_for_stream
                                    .counters
                                    .tool_decisions_denied
                                    .fetch_add(1, Ordering::Relaxed);
                                state_for_stream.record_denied();
                                if tool_name == PROCESS_RUNNER_TOOL_NAME {
                                    state_for_stream
                                        .counters
                                        .sandbox_policy_denies
                                        .fetch_add(1, Ordering::Relaxed);
                                }
                            }

                            if let Err(error) = send_tool_decision_with_tape(
                                &sender,
                                &state_for_stream,
                                run_id.as_str(),
                                &mut tape_seq,
                                proposal_id.as_str(),
                                decision.allowed,
                                decision.reason.as_str(),
                                decision.approval_required,
                                decision.policy_enforced,
                            )
                            .await
                            {
                                finalize_run_failure(
                                    &sender,
                                    &state_for_stream,
                                    &mut run_state,
                                    active_run_id.as_deref(),
                                    &mut tape_seq,
                                    error.message(),
                                )
                                .await;
                                let _ = sender.send(Err(error)).await;
                                return;
                            }
                            let session_id = if let Some(session_id) = active_session_id.as_deref()
                            {
                                session_id
                            } else {
                                let status = Status::internal(
                                    "run stream internal invariant violated: missing session_id while recording policy decision",
                                );
                                finalize_run_failure(
                                    &sender,
                                    &state_for_stream,
                                    &mut run_state,
                                    active_run_id.as_deref(),
                                    &mut tape_seq,
                                    status.message(),
                                )
                                .await;
                                let _ = sender.send(Err(status)).await;
                                return;
                            };
                            if let Err(error) = record_policy_decision_journal_event(
                                &state_for_stream,
                                &context_for_stream,
                                session_id,
                                run_id.as_str(),
                                proposal_id.as_str(),
                                tool_name.as_str(),
                                decision.allowed,
                                decision.reason.as_str(),
                                decision.approval_required,
                                decision.policy_enforced,
                            )
                            .await
                            {
                                finalize_run_failure(
                                    &sender,
                                    &state_for_stream,
                                    &mut run_state,
                                    active_run_id.as_deref(),
                                    &mut tape_seq,
                                    error.message(),
                                )
                                .await;
                                let _ = sender.send(Err(error)).await;
                                return;
                            }
                            if !decision.allowed
                                && decision.reason.contains(SKILL_EXECUTION_DENY_REASON_PREFIX)
                            {
                                if let Some(skill_context) = skill_context.as_ref() {
                                    state_for_stream
                                        .counters
                                        .skill_execution_denied
                                        .fetch_add(1, Ordering::Relaxed);
                                    if let Err(error) = record_skill_execution_denied_journal_event(
                                        &state_for_stream,
                                        &context_for_stream,
                                        session_id,
                                        run_id.as_str(),
                                        proposal_id.as_str(),
                                        tool_name.as_str(),
                                        skill_context,
                                        decision.reason.as_str(),
                                    )
                                    .await
                                    {
                                        finalize_run_failure(
                                            &sender,
                                            &state_for_stream,
                                            &mut run_state,
                                            active_run_id.as_deref(),
                                            &mut tape_seq,
                                            error.message(),
                                        )
                                        .await;
                                        let _ = sender.send(Err(error)).await;
                                        return;
                                    }
                                }
                            }

                            let execution_outcome = if decision.allowed {
                                state_for_stream
                                    .counters
                                    .tool_execution_attempts
                                    .fetch_add(1, Ordering::Relaxed);
                                let started_at = Instant::now();
                                let mut cancel_poll = interval(Duration::from_millis(100));
                                cancel_poll.set_missed_tick_behavior(MissedTickBehavior::Delay);
                                let mut execution_future = Box::pin(async {
                                    if tool_name == "palyra.memory.search" {
                                        execute_memory_search_tool(
                                            &state_for_stream,
                                            context_for_stream.principal.as_str(),
                                            context_for_stream.channel.as_deref(),
                                            session_id,
                                            proposal_id.as_str(),
                                            input_json.as_slice(),
                                        )
                                        .await
                                    } else if tool_name == HTTP_FETCH_TOOL_NAME {
                                        execute_http_fetch_tool(
                                            &state_for_stream,
                                            proposal_id.as_str(),
                                            input_json.as_slice(),
                                        )
                                        .await
                                    } else if tool_name.starts_with("palyra.browser.") {
                                        execute_browser_tool(
                                            &state_for_stream,
                                            context_for_stream.principal.as_str(),
                                            context_for_stream.channel.as_deref(),
                                            tool_name.as_str(),
                                            proposal_id.as_str(),
                                            input_json.as_slice(),
                                        )
                                        .await
                                    } else if tool_name == WORKSPACE_PATCH_TOOL_NAME {
                                        execute_workspace_patch_tool(
                                            &state_for_stream,
                                            context_for_stream.principal.as_str(),
                                            context_for_stream.channel.as_deref(),
                                            session_id,
                                            proposal_id.as_str(),
                                            input_json.as_slice(),
                                        )
                                        .await
                                    } else {
                                        execute_tool_call(
                                            &state_for_stream.config.tool_call,
                                            proposal_id.as_str(),
                                            tool_name.as_str(),
                                            input_json.as_slice(),
                                        )
                                        .await
                                    }
                                });
                                let outcome = loop {
                                    tokio::select! {
                                        result = &mut execution_future => {
                                            break result;
                                        }
                                        _ = cancel_poll.tick() => {
                                            match state_for_stream.is_orchestrator_cancel_requested(run_id.clone()).await {
                                                Ok(true) => {
                                                    if let Err(error) = run_state.transition(RunTransition::Cancel) {
                                                        let status = Status::internal(error.to_string());
                                                        finalize_run_failure(
                                                            &sender,
                                                            &state_for_stream,
                                                            &mut run_state,
                                                            active_run_id.as_deref(),
                                                            &mut tape_seq,
                                                            status.message(),
                                                        )
                                                        .await;
                                                        let _ = sender.send(Err(status)).await;
                                                        return;
                                                    }
                                                    if let Err(error) = state_for_stream
                                                        .update_orchestrator_run_state(
                                                            run_id.clone(),
                                                            RunLifecycleState::Cancelled,
                                                            Some(CANCELLED_REASON.to_owned()),
                                                        )
                                                        .await
                                                    {
                                                        finalize_run_failure(
                                                            &sender,
                                                            &state_for_stream,
                                                            &mut run_state,
                                                            active_run_id.as_deref(),
                                                            &mut tape_seq,
                                                            error.message(),
                                                        )
                                                        .await;
                                                        let _ = sender.send(Err(error)).await;
                                                        return;
                                                    }
                                                    if let Err(error) = send_status_with_tape(
                                                        &sender,
                                                        &state_for_stream,
                                                        run_id.as_str(),
                                                        &mut tape_seq,
                                                        common_v1::stream_status::StatusKind::Failed,
                                                        CANCELLED_REASON,
                                                    )
                                                    .await
                                                    {
                                                        let _ = sender.send(Err(error)).await;
                                                    }
                                                    return;
                                                }
                                                Ok(false) => {}
                                                Err(error) => {
                                                    finalize_run_failure(
                                                        &sender,
                                                        &state_for_stream,
                                                        &mut run_state,
                                                        active_run_id.as_deref(),
                                                        &mut tape_seq,
                                                        error.message(),
                                                    )
                                                    .await;
                                                    let _ = sender.send(Err(error)).await;
                                                    return;
                                                }
                                            }
                                        }
                                    }
                                };
                                if started_at.elapsed().as_millis()
                                    > TOOL_EXECUTION_LATENCY_BUDGET_MS
                                {
                                    warn!(
                                        run_id = %run_id,
                                        proposal_id = %proposal_id,
                                        tool_name = %tool_name,
                                        execution_duration_ms = started_at.elapsed().as_millis(),
                                        budget_ms = TOOL_EXECUTION_LATENCY_BUDGET_MS,
                                        "tool execution exceeded latency budget"
                                    );
                                }
                                if !outcome.success {
                                    state_for_stream
                                        .counters
                                        .tool_execution_failures
                                        .fetch_add(1, Ordering::Relaxed);
                                }
                                if outcome.attestation.timed_out {
                                    state_for_stream
                                        .counters
                                        .tool_execution_timeouts
                                        .fetch_add(1, Ordering::Relaxed);
                                }
                                outcome
                            } else {
                                denied_execution_outcome(
                                    proposal_id.as_str(),
                                    tool_name.as_str(),
                                    input_json.as_slice(),
                                    decision.reason.as_str(),
                                )
                            };

                            if tool_name == WORKSPACE_PATCH_TOOL_NAME {
                                if execution_outcome.success {
                                    state_for_stream
                                        .counters
                                        .patches_applied
                                        .fetch_add(1, Ordering::Relaxed);
                                } else {
                                    state_for_stream
                                        .counters
                                        .patches_rejected
                                        .fetch_add(1, Ordering::Relaxed);
                                }

                                let (files_touched, rollback_performed) =
                                    workspace_patch_metrics_from_output(
                                        execution_outcome.output_json.as_slice(),
                                    );
                                if files_touched > 0 {
                                    state_for_stream
                                        .counters
                                        .patch_files_touched
                                        .fetch_add(files_touched as u64, Ordering::Relaxed);
                                }
                                if rollback_performed {
                                    state_for_stream
                                        .counters
                                        .patch_rollbacks
                                        .fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            if tool_name == PROCESS_RUNNER_TOOL_NAME {
                                record_process_runner_execution_metrics(
                                    &state_for_stream.counters,
                                    decision.allowed,
                                    &execution_outcome,
                                );
                            }

                            if let Err(error) = send_tool_result_with_tape(
                                &sender,
                                &state_for_stream,
                                run_id.as_str(),
                                &mut tape_seq,
                                proposal_id.as_str(),
                                execution_outcome.success,
                                execution_outcome.output_json.as_slice(),
                                execution_outcome.error.as_str(),
                            )
                            .await
                            {
                                finalize_run_failure(
                                    &sender,
                                    &state_for_stream,
                                    &mut run_state,
                                    active_run_id.as_deref(),
                                    &mut tape_seq,
                                    error.message(),
                                )
                                .await;
                                let _ = sender.send(Err(error)).await;
                                return;
                            }

                            if let Err(error) = send_tool_attestation_with_tape(
                                &sender,
                                &state_for_stream,
                                run_id.as_str(),
                                &mut tape_seq,
                                proposal_id.as_str(),
                                execution_outcome.attestation.attestation_id.as_str(),
                                execution_outcome.attestation.execution_sha256.as_str(),
                                execution_outcome.attestation.executed_at_unix_ms,
                                execution_outcome.attestation.timed_out,
                                execution_outcome.attestation.executor.as_str(),
                                execution_outcome.attestation.sandbox_enforcement.as_str(),
                            )
                            .await
                            {
                                finalize_run_failure(
                                    &sender,
                                    &state_for_stream,
                                    &mut run_state,
                                    active_run_id.as_deref(),
                                    &mut tape_seq,
                                    error.message(),
                                )
                                .await;
                                let _ = sender.send(Err(error)).await;
                                return;
                            }
                            state_for_stream
                                .counters
                                .tool_attestations_emitted
                                .fetch_add(1, Ordering::Relaxed);

                            if decision.allowed || execution_outcome.success {
                                let tool_memory_text = build_tool_result_memory_text(
                                    tool_name.as_str(),
                                    execution_outcome.success,
                                    execution_outcome.output_json.as_slice(),
                                    execution_outcome.error.as_str(),
                                );
                                ingest_memory_best_effort(
                                    &state_for_stream,
                                    context_for_stream.principal.as_str(),
                                    context_for_stream.channel.as_deref(),
                                    Some(session_id),
                                    MemorySource::TapeToolResult,
                                    tool_memory_text.as_str(),
                                    vec![format!("tool:{tool_name}")],
                                    Some(if execution_outcome.success { 0.85 } else { 0.55 }),
                                    "run_stream_tool_result",
                                )
                                .await;
                            }
                        }
                    }
                }

                if provider_response.completion_tokens > 0 {
                    if let Err(error) = state_for_stream
                        .add_orchestrator_usage(OrchestratorUsageDelta {
                            run_id: run_id.clone(),
                            prompt_tokens_delta: 0,
                            completion_tokens_delta: provider_response.completion_tokens,
                        })
                        .await
                    {
                        finalize_run_failure(
                            &sender,
                            &state_for_stream,
                            &mut run_state,
                            active_run_id.as_deref(),
                            &mut tape_seq,
                            error.message(),
                        )
                        .await;
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                }

                if !summary_tokens.is_empty() {
                    let summary_text = summary_tokens.join(" ");
                    ingest_memory_best_effort(
                        &state_for_stream,
                        context_for_stream.principal.as_str(),
                        context_for_stream.channel.as_deref(),
                        Some(session_id_for_message.as_str()),
                        MemorySource::Summary,
                        summary_text.as_str(),
                        vec!["summary:model_output".to_owned()],
                        Some(0.75),
                        "run_stream_model_summary",
                    )
                    .await;
                }
            }

            if let Some(run_id) = active_run_id {
                match state_for_stream.is_orchestrator_cancel_requested(run_id.clone()).await {
                    Ok(true) => {
                        if let Err(error) = run_state.transition(RunTransition::Cancel) {
                            let status = Status::internal(error.to_string());
                            finalize_run_failure(
                                &sender,
                                &state_for_stream,
                                &mut run_state,
                                Some(run_id.as_str()),
                                &mut tape_seq,
                                status.message(),
                            )
                            .await;
                            let _ = sender.send(Err(status)).await;
                            return;
                        }
                        if let Err(error) = state_for_stream
                            .update_orchestrator_run_state(
                                run_id.clone(),
                                RunLifecycleState::Cancelled,
                                Some(CANCELLED_REASON.to_owned()),
                            )
                            .await
                        {
                            finalize_run_failure(
                                &sender,
                                &state_for_stream,
                                &mut run_state,
                                Some(run_id.as_str()),
                                &mut tape_seq,
                                error.message(),
                            )
                            .await;
                            let _ = sender.send(Err(error)).await;
                            return;
                        }
                        if let Err(error) = send_status_with_tape(
                            &sender,
                            &state_for_stream,
                            run_id.as_str(),
                            &mut tape_seq,
                            common_v1::stream_status::StatusKind::Failed,
                            CANCELLED_REASON,
                        )
                        .await
                        {
                            let _ = sender.send(Err(error)).await;
                        }
                        return;
                    }
                    Ok(false) => {}
                    Err(error) => {
                        finalize_run_failure(
                            &sender,
                            &state_for_stream,
                            &mut run_state,
                            Some(run_id.as_str()),
                            &mut tape_seq,
                            error.message(),
                        )
                        .await;
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                }

                if run_state.state() == RunLifecycleState::InProgress {
                    if let Err(error) = run_state.transition(RunTransition::Complete) {
                        let status = Status::internal(error.to_string());
                        finalize_run_failure(
                            &sender,
                            &state_for_stream,
                            &mut run_state,
                            Some(run_id.as_str()),
                            &mut tape_seq,
                            status.message(),
                        )
                        .await;
                        let _ = sender.send(Err(status)).await;
                        return;
                    }
                    if let Err(error) = state_for_stream
                        .update_orchestrator_run_state(
                            run_id.clone(),
                            RunLifecycleState::Done,
                            None,
                        )
                        .await
                    {
                        finalize_run_failure(
                            &sender,
                            &state_for_stream,
                            &mut run_state,
                            Some(run_id.as_str()),
                            &mut tape_seq,
                            error.message(),
                        )
                        .await;
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                    if let Err(error) = send_status_with_tape(
                        &sender,
                        &state_for_stream,
                        run_id.as_str(),
                        &mut tape_seq,
                        common_v1::stream_status::StatusKind::Done,
                        "completed",
                    )
                    .await
                    {
                        finalize_run_failure(
                            &sender,
                            &state_for_stream,
                            &mut run_state,
                            Some(run_id.as_str()),
                            &mut tape_seq,
                            error.message(),
                        )
                        .await;
                        let _ = sender.send(Err(error)).await;
                    }
                }
            }
        });

        info!(
            method = "RunStream",
            principal = %context.principal,
            device_id = %context.device_id,
            channel = context.channel.as_deref().unwrap_or("n/a"),
            "gateway run stream opened"
        );

        Ok(Response::new(ReceiverStream::new(receiver)))
    }
}

#[tonic::async_trait]
impl cron_v1::cron_service_server::CronService for CronServiceImpl {
    async fn create_job(
        &self,
        request: Request<cron_v1::CreateJobRequest>,
    ) -> Result<Response<cron_v1::CreateJobResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "CreateJob")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        authorize_cron_action(context.principal.as_str(), "cron.create", "cron:job")?;

        let now_unix_ms = current_unix_ms_status()?;
        let schedule = normalize_schedule(payload.schedule, now_unix_ms, self.cron_timezone_mode)?;
        let name = validate_cron_job_name(payload.name)?;
        let prompt = validate_cron_job_prompt(payload.prompt)?;
        let owner_principal =
            validate_cron_job_owner_principal(context.principal.as_str(), payload.owner_principal)?;
        let channel =
            resolve_cron_job_channel_for_create(context.channel.as_deref(), payload.channel)?;
        let session_key = non_empty(payload.session_key);
        let session_label = non_empty(payload.session_label);
        let concurrency_policy = cron_concurrency_from_proto(payload.concurrency_policy)?;
        let retry_policy = cron_retry_from_proto(payload.retry_policy)?;
        let misfire_policy = cron_misfire_from_proto(payload.misfire_policy)?;
        let jitter_ms = validate_cron_jitter_ms(payload.jitter_ms)?;

        let job = self
            .state
            .create_cron_job(CronJobCreateRequest {
                job_id: Ulid::new().to_string(),
                name,
                prompt,
                owner_principal,
                channel,
                session_key,
                session_label,
                schedule_type: schedule.schedule_type,
                schedule_payload_json: schedule.schedule_payload_json,
                enabled: payload.enabled,
                concurrency_policy,
                retry_policy,
                misfire_policy,
                jitter_ms,
                next_run_at_unix_ms: schedule.next_run_at_unix_ms,
            })
            .await?;
        self.scheduler_wake.notify_one();
        Ok(Response::new(cron_v1::CreateJobResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            job: Some(cron_job_message(&job)?),
        }))
    }

    async fn update_job(
        &self,
        request: Request<cron_v1::UpdateJobRequest>,
    ) -> Result<Response<cron_v1::UpdateJobResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "UpdateJob")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        let job_id = canonical_id(payload.job_id, "job_id")?;
        authorize_cron_action(
            context.principal.as_str(),
            "cron.update",
            format!("cron:{job_id}").as_str(),
        )?;
        let existing_job = self
            .state
            .cron_job(job_id.clone())
            .await?
            .ok_or_else(|| Status::not_found(format!("cron job not found: {job_id}")))?;
        enforce_cron_job_owner(context.principal.as_str(), existing_job.owner_principal.as_str())?;

        let mut patch = CronJobUpdatePatch::default();
        if let Some(name) = payload.name {
            patch.name = Some(validate_cron_job_name(name)?);
        }
        if let Some(prompt) = payload.prompt {
            patch.prompt = Some(validate_cron_job_prompt(prompt)?);
        }
        if let Some(owner_principal) = payload.owner_principal {
            patch.owner_principal = Some(validate_cron_job_owner_principal_for_update(
                context.principal.as_str(),
                owner_principal,
            )?);
        }
        if let Some(channel) = payload.channel {
            patch.channel =
                validate_cron_job_channel_for_update(context.channel.as_deref(), channel)?;
        }
        if let Some(session_key) = payload.session_key {
            patch.session_key = Some(non_empty(session_key));
        }
        if let Some(session_label) = payload.session_label {
            patch.session_label = Some(non_empty(session_label));
        }
        if payload.schedule.is_some() {
            let schedule = normalize_schedule(
                payload.schedule,
                current_unix_ms_status()?,
                self.cron_timezone_mode,
            )?;
            patch.schedule_type = Some(schedule.schedule_type);
            patch.schedule_payload_json = Some(schedule.schedule_payload_json);
            patch.next_run_at_unix_ms = Some(schedule.next_run_at_unix_ms);
        }
        if let Some(enabled) = payload.enabled {
            patch.enabled = Some(enabled);
        }
        if let Some(concurrency_policy) = payload.concurrency_policy {
            patch.concurrency_policy = Some(cron_concurrency_from_proto(concurrency_policy)?);
        }
        if let Some(retry_policy) = payload.retry_policy {
            patch.retry_policy = Some(cron_retry_from_proto(Some(retry_policy))?);
        }
        if let Some(misfire_policy) = payload.misfire_policy {
            patch.misfire_policy = Some(cron_misfire_from_proto(misfire_policy)?);
        }
        if let Some(jitter_ms) = payload.jitter_ms {
            patch.jitter_ms = Some(validate_cron_jitter_ms(jitter_ms)?);
        }

        let updated = self.state.update_cron_job(job_id, patch).await?;
        self.scheduler_wake.notify_one();
        Ok(Response::new(cron_v1::UpdateJobResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            job: Some(cron_job_message(&updated)?),
        }))
    }

    async fn delete_job(
        &self,
        request: Request<cron_v1::DeleteJobRequest>,
    ) -> Result<Response<cron_v1::DeleteJobResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "DeleteJob")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        let job_id = canonical_id(payload.job_id, "job_id")?;
        authorize_cron_action(
            context.principal.as_str(),
            "cron.delete",
            format!("cron:{job_id}").as_str(),
        )?;
        let job = self
            .state
            .cron_job(job_id.clone())
            .await?
            .ok_or_else(|| Status::not_found(format!("cron job not found: {job_id}")))?;
        enforce_cron_job_owner(context.principal.as_str(), job.owner_principal.as_str())?;
        let deleted = self.state.delete_cron_job(job_id).await?;
        self.scheduler_wake.notify_one();
        Ok(Response::new(cron_v1::DeleteJobResponse { v: CANONICAL_PROTOCOL_MAJOR, deleted }))
    }

    async fn get_job(
        &self,
        request: Request<cron_v1::GetJobRequest>,
    ) -> Result<Response<cron_v1::GetJobResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "GetJob")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        let job_id = canonical_id(payload.job_id, "job_id")?;
        authorize_cron_action(
            context.principal.as_str(),
            "cron.get",
            format!("cron:{job_id}").as_str(),
        )?;
        let job = self
            .state
            .cron_job(job_id.clone())
            .await?
            .ok_or_else(|| Status::not_found(format!("cron job not found: {job_id}")))?;
        enforce_cron_job_owner(context.principal.as_str(), job.owner_principal.as_str())?;
        Ok(Response::new(cron_v1::GetJobResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            job: Some(cron_job_message(&job)?),
        }))
    }

    async fn list_jobs(
        &self,
        request: Request<cron_v1::ListJobsRequest>,
    ) -> Result<Response<cron_v1::ListJobsResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "ListJobs")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        authorize_cron_action(context.principal.as_str(), "cron.list", "cron:jobs")?;
        if let Some(owner_principal) = payload.owner_principal.as_deref() {
            if owner_principal != context.principal.as_str() {
                return Err(Status::permission_denied(
                    "owner_principal must match authenticated principal",
                ));
            }
        }

        let (jobs, next_after_job_ulid) = self
            .state
            .list_cron_jobs(
                non_empty(payload.after_job_ulid),
                Some(payload.limit as usize),
                payload.enabled,
                Some(context.principal.clone()),
                payload.channel,
            )
            .await?;
        let jobs = jobs.iter().map(cron_job_message).collect::<Result<Vec<_>, _>>()?;
        Ok(Response::new(cron_v1::ListJobsResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            jobs,
            next_after_job_ulid: next_after_job_ulid.unwrap_or_default(),
        }))
    }

    async fn run_job_now(
        &self,
        request: Request<cron_v1::RunJobNowRequest>,
    ) -> Result<Response<cron_v1::RunJobNowResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "RunJobNow")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        let job_id = canonical_id(payload.job_id, "job_id")?;
        authorize_cron_action(
            context.principal.as_str(),
            "cron.run",
            format!("cron:{job_id}").as_str(),
        )?;
        let job = self
            .state
            .cron_job(job_id.clone())
            .await?
            .ok_or_else(|| Status::not_found(format!("cron job not found: {job_id}")))?;
        enforce_cron_job_owner(context.principal.as_str(), job.owner_principal.as_str())?;
        let outcome = trigger_job_now(
            Arc::clone(&self.state),
            self.auth.clone(),
            self.grpc_url.clone(),
            job,
            Arc::clone(&self.scheduler_wake),
        )
        .await?;
        Ok(Response::new(cron_v1::RunJobNowResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            run_id: outcome.run_id.map(|ulid| common_v1::CanonicalId { ulid }),
            status: cron_run_status_to_proto(outcome.status),
            message: outcome.message,
        }))
    }

    async fn list_job_runs(
        &self,
        request: Request<cron_v1::ListJobRunsRequest>,
    ) -> Result<Response<cron_v1::ListJobRunsResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "ListJobRuns")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        let job_id = canonical_id(payload.job_id, "job_id")?;
        authorize_cron_action(
            context.principal.as_str(),
            "cron.logs",
            format!("cron:{job_id}").as_str(),
        )?;
        let job = self
            .state
            .cron_job(job_id.clone())
            .await?
            .ok_or_else(|| Status::not_found(format!("cron job not found: {job_id}")))?;
        enforce_cron_job_owner(context.principal.as_str(), job.owner_principal.as_str())?;
        let (runs, next_after_run_ulid) = self
            .state
            .list_cron_runs(
                Some(job_id),
                non_empty(payload.after_run_ulid),
                Some(payload.limit as usize),
            )
            .await?;
        Ok(Response::new(cron_v1::ListJobRunsResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            runs: runs.iter().map(cron_run_message).collect(),
            next_after_run_ulid: next_after_run_ulid.unwrap_or_default(),
        }))
    }

    async fn get_job_run(
        &self,
        request: Request<cron_v1::GetJobRunRequest>,
    ) -> Result<Response<cron_v1::GetJobRunResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "GetJobRun")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        let run_id = canonical_id(payload.run_id, "run_id")?;
        authorize_cron_action(
            context.principal.as_str(),
            "cron.logs",
            format!("cron:run:{run_id}").as_str(),
        )?;
        let run = self
            .state
            .cron_run(run_id.clone())
            .await?
            .ok_or_else(|| Status::not_found(format!("cron run not found: {run_id}")))?;
        let job = self
            .state
            .cron_job(run.job_id.clone())
            .await?
            .ok_or_else(|| Status::internal("cron job for run not found"))?;
        enforce_cron_job_owner(context.principal.as_str(), job.owner_principal.as_str())?;
        Ok(Response::new(cron_v1::GetJobRunResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            run: Some(cron_run_message(&run)),
        }))
    }
}

#[tonic::async_trait]
impl gateway_v1::approvals_service_server::ApprovalsService for ApprovalsServiceImpl {
    type ExportApprovalsStream =
        ReceiverStream<Result<gateway_v1::ExportApprovalsResponse, Status>>;

    async fn list_approvals(
        &self,
        request: Request<gateway_v1::ListApprovalsRequest>,
    ) -> Result<Response<gateway_v1::ListApprovalsResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "ListApprovals")?;
        authorize_approvals_action(
            context.principal.as_str(),
            "approvals.list",
            "approvals:records",
        )?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        let decision = approval_decision_from_proto(payload.decision);
        let subject_type = approval_subject_type_from_proto(payload.subject_type);
        let since_unix_ms =
            if payload.since_unix_ms > 0 { Some(payload.since_unix_ms) } else { None };
        let until_unix_ms =
            if payload.until_unix_ms > 0 { Some(payload.until_unix_ms) } else { None };
        if let (Some(since), Some(until)) = (since_unix_ms, until_unix_ms) {
            if since > until {
                return Err(Status::invalid_argument(
                    "since_unix_ms cannot be greater than until_unix_ms",
                ));
            }
        }

        let (records, next_after_approval_ulid) = self
            .state
            .list_approval_records(
                non_empty(payload.after_approval_ulid),
                Some(payload.limit as usize),
                since_unix_ms,
                until_unix_ms,
                non_empty(payload.subject_id),
                non_empty(payload.principal),
                decision,
                subject_type,
            )
            .await?;
        Ok(Response::new(gateway_v1::ListApprovalsResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            approvals: records.iter().map(approval_record_message).collect(),
            next_after_approval_ulid: next_after_approval_ulid.unwrap_or_default(),
        }))
    }

    async fn get_approval(
        &self,
        request: Request<gateway_v1::GetApprovalRequest>,
    ) -> Result<Response<gateway_v1::GetApprovalResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "GetApproval")?;
        authorize_approvals_action(
            context.principal.as_str(),
            "approvals.get",
            "approvals:record",
        )?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        let approval_id = canonical_id(payload.approval_id, "approval_id")?;
        let record = self.state.approval_record(approval_id.clone()).await?.ok_or_else(|| {
            Status::not_found(format!("approval record not found: {approval_id}"))
        })?;
        Ok(Response::new(gateway_v1::GetApprovalResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            approval: Some(approval_record_message(&record)),
        }))
    }

    async fn export_approvals(
        &self,
        request: Request<gateway_v1::ExportApprovalsRequest>,
    ) -> Result<Response<Self::ExportApprovalsStream>, Status> {
        let context = self.authorize_rpc(request.metadata(), "ExportApprovals")?;
        authorize_approvals_action(
            context.principal.as_str(),
            "approvals.export",
            "approvals:records",
        )?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        let decision = approval_decision_from_proto(payload.decision);
        let subject_type = approval_subject_type_from_proto(payload.subject_type);
        let since_unix_ms =
            if payload.since_unix_ms > 0 { Some(payload.since_unix_ms) } else { None };
        let until_unix_ms =
            if payload.until_unix_ms > 0 { Some(payload.until_unix_ms) } else { None };
        if let (Some(since), Some(until)) = (since_unix_ms, until_unix_ms) {
            if since > until {
                return Err(Status::invalid_argument(
                    "since_unix_ms cannot be greater than until_unix_ms",
                ));
            }
        }
        let export_format = match gateway_v1::ApprovalExportFormat::try_from(payload.format)
            .unwrap_or(gateway_v1::ApprovalExportFormat::Unspecified)
        {
            gateway_v1::ApprovalExportFormat::Unspecified => {
                gateway_v1::ApprovalExportFormat::Ndjson
            }
            other => other,
        };
        let export_limit = if payload.limit == 0 { 1_000_usize } else { payload.limit as usize }
            .clamp(1, MAX_APPROVAL_EXPORT_LIMIT);

        let state = Arc::clone(&self.state);
        let subject_id = non_empty(payload.subject_id);
        let principal = non_empty(payload.principal);
        let (sender, receiver) = mpsc::channel(8);
        tokio::spawn(async move {
            let mut after_approval_id: Option<String> = None;
            let mut exported = 0_usize;
            let mut chunk_seq = 0_u32;
            let mut json_array_started = false;
            let mut json_first_item = true;
            let mut ndjson_sequence = 0_u64;
            let mut ndjson_last_chain_checksum = APPROVAL_EXPORT_CHAIN_SEED_HEX.to_owned();

            loop {
                if exported >= export_limit {
                    break;
                }
                let page_limit =
                    export_limit.saturating_sub(exported).clamp(1, MAX_APPROVAL_PAGE_LIMIT);
                let (records, next_after) = match state
                    .list_approval_records(
                        after_approval_id.clone(),
                        Some(page_limit),
                        since_unix_ms,
                        until_unix_ms,
                        subject_id.clone(),
                        principal.clone(),
                        decision,
                        subject_type,
                    )
                    .await
                {
                    Ok(value) => value,
                    Err(error) => {
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                };
                if records.is_empty() {
                    break;
                }

                for record in records {
                    if exported >= export_limit {
                        break;
                    }
                    match export_format {
                        gateway_v1::ApprovalExportFormat::Ndjson => {
                            ndjson_sequence = ndjson_sequence.saturating_add(1);
                            let (line, chain_checksum) = match approval_export_ndjson_record_line(
                                &record,
                                ndjson_sequence,
                                ndjson_last_chain_checksum.as_str(),
                            ) {
                                Ok(value) => value,
                                Err(error) => {
                                    let _ = sender.send(Err(error)).await;
                                    return;
                                }
                            };
                            ndjson_last_chain_checksum = chain_checksum;
                            for chunk in line.chunks(MAX_APPROVAL_EXPORT_CHUNK_BYTES) {
                                chunk_seq = chunk_seq.saturating_add(1);
                                if sender
                                    .send(Ok(gateway_v1::ExportApprovalsResponse {
                                        v: CANONICAL_PROTOCOL_MAJOR,
                                        chunk: chunk.to_vec(),
                                        chunk_seq,
                                        done: false,
                                    }))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                            }
                        }
                        gateway_v1::ApprovalExportFormat::Json => {
                            if !json_array_started {
                                json_array_started = true;
                                chunk_seq = chunk_seq.saturating_add(1);
                                if sender
                                    .send(Ok(gateway_v1::ExportApprovalsResponse {
                                        v: CANONICAL_PROTOCOL_MAJOR,
                                        chunk: b"[".to_vec(),
                                        chunk_seq,
                                        done: false,
                                    }))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                            }
                            if !json_first_item {
                                chunk_seq = chunk_seq.saturating_add(1);
                                if sender
                                    .send(Ok(gateway_v1::ExportApprovalsResponse {
                                        v: CANONICAL_PROTOCOL_MAJOR,
                                        chunk: b",".to_vec(),
                                        chunk_seq,
                                        done: false,
                                    }))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                            }
                            json_first_item = false;
                            let payload = match serde_json::to_vec(&record) {
                                Ok(value) => value,
                                Err(error) => {
                                    let _ = sender
                                        .send(Err(Status::internal(format!(
                                            "failed to serialize approvals JSON export record: {error}"
                                        ))))
                                        .await;
                                    return;
                                }
                            };
                            for chunk in payload.chunks(MAX_APPROVAL_EXPORT_CHUNK_BYTES) {
                                chunk_seq = chunk_seq.saturating_add(1);
                                if sender
                                    .send(Ok(gateway_v1::ExportApprovalsResponse {
                                        v: CANONICAL_PROTOCOL_MAJOR,
                                        chunk: chunk.to_vec(),
                                        chunk_seq,
                                        done: false,
                                    }))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                            }
                        }
                        gateway_v1::ApprovalExportFormat::Unspecified => {}
                    }
                    exported = exported.saturating_add(1);
                }

                let Some(next_after) = next_after else {
                    break;
                };
                after_approval_id = Some(next_after);
            }

            let export_suffix = if export_format == gateway_v1::ApprovalExportFormat::Json {
                Some(if json_array_started { b"]".to_vec() } else { b"[]".to_vec() })
            } else if export_format == gateway_v1::ApprovalExportFormat::Ndjson {
                match approval_export_ndjson_trailer_line(
                    exported,
                    ndjson_last_chain_checksum.as_str(),
                ) {
                    Ok(value) => Some(value),
                    Err(error) => {
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                }
            } else {
                None
            };
            if let Some(suffix) = export_suffix {
                for chunk in suffix.chunks(MAX_APPROVAL_EXPORT_CHUNK_BYTES) {
                    chunk_seq = chunk_seq.saturating_add(1);
                    if sender
                        .send(Ok(gateway_v1::ExportApprovalsResponse {
                            v: CANONICAL_PROTOCOL_MAJOR,
                            chunk: chunk.to_vec(),
                            chunk_seq,
                            done: false,
                        }))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }

            chunk_seq = chunk_seq.saturating_add(1);
            let _ = sender
                .send(Ok(gateway_v1::ExportApprovalsResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    chunk: Vec::new(),
                    chunk_seq,
                    done: true,
                }))
                .await;
        });

        Ok(Response::new(ReceiverStream::new(receiver)))
    }
}

#[tonic::async_trait]
impl memory_v1::memory_service_server::MemoryService for MemoryServiceImpl {
    async fn ingest_memory(
        &self,
        request: Request<memory_v1::IngestMemoryRequest>,
    ) -> Result<Response<memory_v1::IngestMemoryResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "IngestMemory")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        authorize_memory_action(context.principal.as_str(), "memory.ingest", "memory:item")?;

        let source = memory_source_from_proto(payload.source)?;
        let channel =
            resolve_memory_channel_scope(context.channel.as_deref(), non_empty(payload.channel))?;
        let session_id = optional_canonical_id(payload.session_id, "session_id")?;
        let confidence = if payload.confidence == 0.0 {
            None
        } else if payload.confidence.is_finite() && (0.0..=1.0).contains(&payload.confidence) {
            Some(payload.confidence)
        } else {
            return Err(Status::invalid_argument(
                "memory confidence must be a finite value in range 0.0..=1.0",
            ));
        };
        let ttl_unix_ms = if payload.ttl_unix_ms > 0 { Some(payload.ttl_unix_ms) } else { None };

        let created = self
            .state
            .ingest_memory_item(MemoryItemCreateRequest {
                memory_id: Ulid::new().to_string(),
                principal: context.principal,
                channel,
                session_id,
                source,
                content_text: payload.content_text,
                tags: payload.tags,
                confidence,
                ttl_unix_ms,
            })
            .await?;
        Ok(Response::new(memory_v1::IngestMemoryResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            item: Some(memory_item_message(&created)),
        }))
    }

    async fn search_memory(
        &self,
        request: Request<memory_v1::SearchMemoryRequest>,
    ) -> Result<Response<memory_v1::SearchMemoryResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "SearchMemory")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;

        let channel =
            resolve_memory_channel_scope(context.channel.as_deref(), non_empty(payload.channel))?;
        let session_id = optional_canonical_id(payload.session_id, "session_id")?;
        let resource = if let Some(session_id) = session_id.as_deref() {
            format!("memory:session:{session_id}")
        } else if let Some(channel) = channel.as_deref() {
            format!("memory:channel:{channel}")
        } else {
            "memory:principal".to_owned()
        };
        authorize_memory_action(context.principal.as_str(), "memory.search", resource.as_str())?;

        if !payload.min_score.is_finite() || payload.min_score < 0.0 || payload.min_score > 1.0 {
            return Err(Status::invalid_argument(
                "memory min_score must be a finite value in range 0.0..=1.0",
            ));
        }
        let sources = payload
            .sources
            .into_iter()
            .map(memory_source_from_proto)
            .collect::<Result<Vec<_>, _>>()?;
        let top_k = if payload.top_k == 0 {
            None
        } else {
            Some((payload.top_k as usize).clamp(1, MAX_MEMORY_SEARCH_TOP_K))
        };

        let hits = self
            .state
            .search_memory(MemorySearchRequest {
                principal: context.principal,
                channel,
                session_id,
                query: payload.query,
                top_k: top_k.unwrap_or(8),
                min_score: payload.min_score,
                tags: payload.tags,
                sources,
            })
            .await?;
        Ok(Response::new(memory_v1::SearchMemoryResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            hits: hits
                .iter()
                .map(|hit| memory_search_hit_message(hit, payload.include_score_breakdown))
                .collect(),
        }))
    }

    async fn get_memory_item(
        &self,
        request: Request<memory_v1::GetMemoryItemRequest>,
    ) -> Result<Response<memory_v1::GetMemoryItemResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "GetMemoryItem")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        let memory_id = canonical_id(payload.memory_id, "memory_id")?;
        authorize_memory_action(
            context.principal.as_str(),
            "memory.get",
            format!("memory:{memory_id}").as_str(),
        )?;
        let item = self
            .state
            .memory_item(memory_id.clone())
            .await?
            .ok_or_else(|| Status::not_found(format!("memory item not found: {memory_id}")))?;
        enforce_memory_item_scope(&item, context.principal.as_str(), context.channel.as_deref())?;
        Ok(Response::new(memory_v1::GetMemoryItemResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            item: Some(memory_item_message(&item)),
        }))
    }

    async fn delete_memory_item(
        &self,
        request: Request<memory_v1::DeleteMemoryItemRequest>,
    ) -> Result<Response<memory_v1::DeleteMemoryItemResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "DeleteMemoryItem")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        let memory_id = canonical_id(payload.memory_id, "memory_id")?;
        authorize_memory_action(
            context.principal.as_str(),
            "memory.delete",
            format!("memory:{memory_id}").as_str(),
        )?;
        if let Some(item) = self.state.memory_item(memory_id.clone()).await? {
            enforce_memory_item_scope(
                &item,
                context.principal.as_str(),
                context.channel.as_deref(),
            )?;
        }
        let deleted =
            self.state.delete_memory_item(memory_id, context.principal, context.channel).await?;
        Ok(Response::new(memory_v1::DeleteMemoryItemResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            deleted,
        }))
    }

    async fn list_memory_items(
        &self,
        request: Request<memory_v1::ListMemoryItemsRequest>,
    ) -> Result<Response<memory_v1::ListMemoryItemsResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "ListMemoryItems")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        authorize_memory_action(context.principal.as_str(), "memory.list", "memory:items")?;
        let after_memory_id = non_empty(payload.after_memory_ulid);
        if let Some(after) = after_memory_id.as_deref() {
            validate_canonical_id(after).map_err(|_| {
                Status::invalid_argument("after_memory_ulid must be a canonical ULID")
            })?;
        }
        let channel =
            resolve_memory_channel_scope(context.channel.as_deref(), non_empty(payload.channel))?;
        let session_id = optional_canonical_id(payload.session_id, "session_id")?;
        let sources = payload
            .sources
            .into_iter()
            .map(memory_source_from_proto)
            .collect::<Result<Vec<_>, _>>()?;
        let (items, next_after_memory_id) = self
            .state
            .list_memory_items(
                after_memory_id,
                if payload.limit == 0 { None } else { Some(payload.limit as usize) },
                context.principal,
                channel,
                session_id,
                payload.tags,
                sources,
            )
            .await?;
        Ok(Response::new(memory_v1::ListMemoryItemsResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            items: items.iter().map(memory_item_message).collect(),
            next_after_memory_ulid: next_after_memory_id.unwrap_or_default(),
        }))
    }

    async fn purge_memory(
        &self,
        request: Request<memory_v1::PurgeMemoryRequest>,
    ) -> Result<Response<memory_v1::PurgeMemoryResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "PurgeMemory")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        authorize_memory_action(context.principal.as_str(), "memory.purge", "memory:items")?;
        let channel =
            resolve_memory_channel_scope(context.channel.as_deref(), non_empty(payload.channel))?;
        let session_id = optional_canonical_id(payload.session_id, "session_id")?;
        if !payload.purge_all_principal && channel.is_none() && session_id.is_none() {
            return Err(Status::invalid_argument(
                "purge request requires purge_all_principal=true or a channel/session scope",
            ));
        }

        let deleted_count = self
            .state
            .purge_memory(MemoryPurgeRequest {
                principal: context.principal,
                channel,
                session_id,
                purge_all_principal: payload.purge_all_principal,
            })
            .await?;
        Ok(Response::new(memory_v1::PurgeMemoryResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            deleted_count,
        }))
    }
}

#[tonic::async_trait]
impl gateway_v1::vault_service_server::VaultService for VaultServiceImpl {
    async fn put_secret(
        &self,
        request: Request<gateway_v1::PutSecretRequest>,
    ) -> Result<Response<gateway_v1::PutSecretResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "PutSecret")?;
        if !self.state.consume_vault_rate_limit(context.principal.as_str()) {
            self.state.counters.vault_rate_limited_requests.fetch_add(1, Ordering::Relaxed);
            return Err(Status::resource_exhausted("vault rate limit exceeded"));
        }
        self.state.counters.vault_put_requests.fetch_add(1, Ordering::Relaxed);

        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        if payload.value.len() > MAX_VAULT_SECRET_BYTES {
            return Err(Status::invalid_argument(format!(
                "secret value exceeds maximum bytes ({} > {MAX_VAULT_SECRET_BYTES})",
                payload.value.len()
            )));
        }
        let scope = parse_vault_scope(payload.scope.as_str())?;
        enforce_vault_scope_access(&scope, &context)?;
        let key = payload.key.trim().to_owned();
        authorize_vault_action(
            context.principal.as_str(),
            "vault.put",
            format!("secrets:{scope}:{key}").as_str(),
        )?;
        let metadata =
            self.state.vault_put_secret(scope.clone(), key.clone(), payload.value).await?;
        record_vault_journal_event(
            &self.state,
            &context,
            "secret.updated",
            "vault.put",
            &scope,
            Some(key.as_str()),
            Some(metadata.value_bytes),
        )
        .await?;
        Ok(Response::new(gateway_v1::PutSecretResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            secret: Some(vault_secret_metadata_message(&metadata)),
        }))
    }

    async fn get_secret(
        &self,
        request: Request<gateway_v1::GetSecretRequest>,
    ) -> Result<Response<gateway_v1::GetSecretResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "GetSecret")?;
        if !self.state.consume_vault_rate_limit(context.principal.as_str()) {
            self.state.counters.vault_rate_limited_requests.fetch_add(1, Ordering::Relaxed);
            return Err(Status::resource_exhausted("vault rate limit exceeded"));
        }
        self.state.counters.vault_get_requests.fetch_add(1, Ordering::Relaxed);

        let approval_header = request
            .metadata()
            .get(HEADER_VAULT_READ_APPROVAL)
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);

        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        let scope = parse_vault_scope(payload.scope.as_str())?;
        enforce_vault_scope_access(&scope, &context)?;
        let key = payload.key.trim().to_owned();
        enforce_vault_get_approval_policy(
            context.principal.as_str(),
            &scope,
            key.as_str(),
            self.state.config.vault_get_approval_required_refs.as_slice(),
            approval_header.as_deref(),
        )?;
        authorize_vault_action(
            context.principal.as_str(),
            "vault.get",
            format!("secrets:{scope}:{key}").as_str(),
        )?;
        let value = self.state.vault_get_secret(scope.clone(), key.clone()).await?;
        record_vault_journal_event(
            &self.state,
            &context,
            "secret.accessed",
            "vault.get",
            &scope,
            Some(key.as_str()),
            Some(value.len()),
        )
        .await?;
        Ok(Response::new(gateway_v1::GetSecretResponse { v: CANONICAL_PROTOCOL_MAJOR, value }))
    }

    async fn delete_secret(
        &self,
        request: Request<gateway_v1::DeleteSecretRequest>,
    ) -> Result<Response<gateway_v1::DeleteSecretResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "DeleteSecret")?;
        if !self.state.consume_vault_rate_limit(context.principal.as_str()) {
            self.state.counters.vault_rate_limited_requests.fetch_add(1, Ordering::Relaxed);
            return Err(Status::resource_exhausted("vault rate limit exceeded"));
        }
        self.state.counters.vault_delete_requests.fetch_add(1, Ordering::Relaxed);

        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        let scope = parse_vault_scope(payload.scope.as_str())?;
        enforce_vault_scope_access(&scope, &context)?;
        let key = payload.key.trim().to_owned();
        authorize_vault_action(
            context.principal.as_str(),
            "vault.delete",
            format!("secrets:{scope}:{key}").as_str(),
        )?;
        let deleted = self.state.vault_delete_secret(scope.clone(), key.clone()).await?;
        if deleted {
            record_vault_journal_event(
                &self.state,
                &context,
                "secret.deleted",
                "vault.delete",
                &scope,
                Some(key.as_str()),
                None,
            )
            .await?;
        }
        Ok(Response::new(gateway_v1::DeleteSecretResponse { v: CANONICAL_PROTOCOL_MAJOR, deleted }))
    }

    async fn list_secrets(
        &self,
        request: Request<gateway_v1::ListSecretsRequest>,
    ) -> Result<Response<gateway_v1::ListSecretsResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "ListSecrets")?;
        if !self.state.consume_vault_rate_limit(context.principal.as_str()) {
            self.state.counters.vault_rate_limited_requests.fetch_add(1, Ordering::Relaxed);
            return Err(Status::resource_exhausted("vault rate limit exceeded"));
        }
        self.state.counters.vault_list_requests.fetch_add(1, Ordering::Relaxed);

        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        let scope = parse_vault_scope(payload.scope.as_str())?;
        enforce_vault_scope_access(&scope, &context)?;
        authorize_vault_action(
            context.principal.as_str(),
            "vault.list",
            format!("secrets:{scope}").as_str(),
        )?;
        let mut secrets = self.state.vault_list_secrets(scope.clone()).await?;
        if secrets.len() > MAX_VAULT_LIST_RESULTS {
            secrets.truncate(MAX_VAULT_LIST_RESULTS);
        }
        record_vault_journal_event(
            &self.state,
            &context,
            "secret.listed",
            "vault.list",
            &scope,
            None,
            Some(secrets.len()),
        )
        .await?;
        Ok(Response::new(gateway_v1::ListSecretsResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            secrets: secrets.iter().map(vault_secret_metadata_message).collect(),
        }))
    }
}

#[tonic::async_trait]
impl auth_v1::auth_service_server::AuthService for AuthServiceImpl {
    async fn list_profiles(
        &self,
        request: Request<auth_v1::ListAuthProfilesRequest>,
    ) -> Result<Response<auth_v1::ListAuthProfilesResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "ListProfiles")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        authorize_auth_profile_action(
            context.principal.as_str(),
            "auth.profile.list",
            "auth:profiles",
        )?;
        let filter = auth_list_filter_from_proto(payload)?;

        let auth_runtime = Arc::clone(&self.auth_runtime);
        let page = tokio::task::spawn_blocking(move || {
            auth_runtime.registry().list_profiles(filter).map_err(map_auth_profile_error)
        })
        .await
        .map_err(|_| Status::internal("auth list worker panicked"))??;

        Ok(Response::new(auth_v1::ListAuthProfilesResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            profiles: page.profiles.iter().map(auth_profile_to_proto).collect(),
            next_after_profile_id: page.next_after_profile_id.unwrap_or_default(),
        }))
    }

    async fn get_profile(
        &self,
        request: Request<auth_v1::GetAuthProfileRequest>,
    ) -> Result<Response<auth_v1::GetAuthProfileResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "GetProfile")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        let profile_id = payload.profile_id.trim().to_owned();
        if profile_id.is_empty() {
            return Err(Status::invalid_argument("profile_id is required"));
        }
        authorize_auth_profile_action(
            context.principal.as_str(),
            "auth.profile.get",
            format!("auth:profile:{profile_id}").as_str(),
        )?;
        let auth_runtime = Arc::clone(&self.auth_runtime);
        let profile = tokio::task::spawn_blocking(move || {
            auth_runtime.registry().get_profile(profile_id.as_str()).map_err(map_auth_profile_error)
        })
        .await
        .map_err(|_| Status::internal("auth get worker panicked"))??;
        let profile = profile.ok_or_else(|| Status::not_found("auth profile not found"))?;

        Ok(Response::new(auth_v1::GetAuthProfileResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            profile: Some(auth_profile_to_proto(&profile)),
        }))
    }

    async fn set_profile(
        &self,
        request: Request<auth_v1::SetAuthProfileRequest>,
    ) -> Result<Response<auth_v1::SetAuthProfileResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "SetProfile")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        let profile =
            payload.profile.ok_or_else(|| Status::invalid_argument("profile is required"))?;
        let set_request = auth_set_request_from_proto(profile)?;
        authorize_auth_profile_action(
            context.principal.as_str(),
            "auth.profile.set",
            format!("auth:profile:{}", set_request.profile_id).as_str(),
        )?;
        let auth_runtime = Arc::clone(&self.auth_runtime);
        let saved = tokio::task::spawn_blocking(move || {
            auth_runtime.registry().set_profile(set_request).map_err(map_auth_profile_error)
        })
        .await
        .map_err(|_| Status::internal("auth set worker panicked"))??;
        record_auth_profile_saved_journal_event(&self.state, &context, &saved).await?;

        Ok(Response::new(auth_v1::SetAuthProfileResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            profile: Some(auth_profile_to_proto(&saved)),
        }))
    }

    async fn delete_profile(
        &self,
        request: Request<auth_v1::DeleteAuthProfileRequest>,
    ) -> Result<Response<auth_v1::DeleteAuthProfileResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "DeleteProfile")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        let profile_id = payload.profile_id.trim().to_owned();
        if profile_id.is_empty() {
            return Err(Status::invalid_argument("profile_id is required"));
        }
        authorize_auth_profile_action(
            context.principal.as_str(),
            "auth.profile.delete",
            format!("auth:profile:{profile_id}").as_str(),
        )?;
        let auth_runtime = Arc::clone(&self.auth_runtime);
        let deleted = tokio::task::spawn_blocking(move || {
            auth_runtime
                .registry()
                .delete_profile(profile_id.as_str())
                .map_err(map_auth_profile_error)
        })
        .await
        .map_err(|_| Status::internal("auth delete worker panicked"))??;

        Ok(Response::new(auth_v1::DeleteAuthProfileResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            deleted,
        }))
    }

    async fn get_health(
        &self,
        request: Request<auth_v1::GetAuthHealthRequest>,
    ) -> Result<Response<auth_v1::GetAuthHealthResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "GetHealth")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        authorize_auth_profile_action(
            context.principal.as_str(),
            "auth.profile.health",
            "auth:health",
        )?;
        let agent_id_filter = non_empty(payload.agent_id);
        let include_profiles = payload.include_profiles;
        let auth_runtime = Arc::clone(&self.auth_runtime);
        let vault = Arc::clone(&self.state.vault);
        let (report, outcomes, refresh_metrics) = tokio::task::spawn_blocking(move || {
            let outcomes = auth_runtime
                .registry()
                .refresh_due_oauth_profiles(
                    vault.as_ref(),
                    auth_runtime.refresh_adapter.as_ref(),
                    agent_id_filter.as_deref(),
                )
                .map_err(map_auth_profile_error)?;
            for outcome in &outcomes {
                auth_runtime.record_refresh_outcome(outcome);
            }
            let report = auth_runtime
                .registry()
                .health_report(vault.as_ref(), agent_id_filter.as_deref())
                .map_err(map_auth_profile_error)?;
            Ok::<_, Status>((report, outcomes, auth_runtime.refresh_metrics_snapshot()))
        })
        .await
        .map_err(|_| Status::internal("auth health worker panicked"))??;

        for outcome in outcomes {
            record_auth_refresh_journal_event(&self.state, &context, &outcome).await?;
        }

        Ok(Response::new(auth_v1::GetAuthHealthResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            summary: Some(auth_health_summary_to_proto(&report.summary)),
            expiry_distribution: Some(auth_expiry_distribution_to_proto(
                &report.expiry_distribution,
            )),
            profiles: if include_profiles {
                report.profiles.iter().map(auth_health_profile_to_proto).collect()
            } else {
                Vec::new()
            },
            refresh_metrics: Some(auth_refresh_metrics_to_proto(&refresh_metrics)),
        }))
    }
}

#[allow(clippy::too_many_arguments)]
async fn ingest_memory_best_effort(
    runtime_state: &Arc<GatewayRuntimeState>,
    principal: &str,
    channel: Option<&str>,
    session_id: Option<&str>,
    source: MemorySource,
    content_text: &str,
    tags: Vec<String>,
    confidence: Option<f64>,
    reason: &str,
) {
    if content_text.trim().is_empty() {
        return;
    }
    if let Err(error) = runtime_state
        .ingest_memory_item(MemoryItemCreateRequest {
            memory_id: Ulid::new().to_string(),
            principal: principal.to_owned(),
            channel: channel.map(str::to_owned),
            session_id: session_id.map(str::to_owned),
            source,
            content_text: content_text.to_owned(),
            tags,
            confidence,
            ttl_unix_ms: None,
        })
        .await
    {
        warn!(
            reason,
            status_code = ?error.code(),
            status_message = %error.message(),
            "memory ingest best-effort path rejected candidate"
        );
    }
}

#[allow(clippy::result_large_err)]
async fn build_memory_augmented_prompt(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    run_id: &str,
    tape_seq: &mut i64,
    session_id: &str,
    input_text: &str,
) -> Result<String, Status> {
    let trimmed_input = input_text.trim();
    if trimmed_input.is_empty() {
        return Ok(input_text.to_owned());
    }
    let memory_config = runtime_state.memory_config_snapshot();
    if !memory_config.auto_inject_enabled || memory_config.auto_inject_max_items == 0 {
        return Ok(input_text.to_owned());
    }
    let resource = format!("memory:session:{session_id}");
    if let Err(error) =
        authorize_memory_action(context.principal.as_str(), "memory.search", resource.as_str())
    {
        warn!(
            run_id,
            principal = %context.principal,
            session_id,
            status_message = %error.message(),
            "memory auto-inject skipped because policy denied access"
        );
        return Ok(input_text.to_owned());
    }

    let search_hits = match runtime_state
        .search_memory(MemorySearchRequest {
            principal: context.principal.clone(),
            channel: context.channel.clone(),
            session_id: Some(session_id.to_owned()),
            query: input_text.to_owned(),
            top_k: memory_config.auto_inject_max_items,
            min_score: MEMORY_AUTO_INJECT_MIN_SCORE,
            tags: Vec::new(),
            sources: Vec::new(),
        })
        .await
    {
        Ok(hits) => hits,
        Err(error) => {
            warn!(
                run_id,
                principal = %context.principal,
                session_id,
                status_code = ?error.code(),
                status_message = %error.message(),
                "memory auto-inject search failed"
            );
            return Ok(input_text.to_owned());
        }
    };
    if search_hits.is_empty() {
        return Ok(input_text.to_owned());
    }

    let selected_hits =
        search_hits.into_iter().take(memory_config.auto_inject_max_items).collect::<Vec<_>>();
    let mut context_lines = Vec::new();
    for (index, hit) in selected_hits.iter().enumerate() {
        let snippet = hit.snippet.replace(['\r', '\n'], " ").trim().to_owned();
        context_lines.push(format!(
            "{}. id={} source={} score={:.4} created_at_unix_ms={} snippet={}",
            index + 1,
            hit.item.memory_id,
            hit.item.source.as_str(),
            hit.score,
            hit.item.created_at_unix_ms,
            truncate_with_ellipsis(snippet, 256),
        ));
    }

    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: "memory_auto_inject".to_owned(),
            payload_json: memory_auto_inject_tape_payload(input_text, selected_hits.as_slice()),
        })
        .await?;
    *tape_seq = tape_seq.saturating_add(1);
    runtime_state.counters.memory_auto_inject_events.fetch_add(1, Ordering::Relaxed);

    let mut block = String::from("<memory_context>\n");
    block.push_str(context_lines.join("\n").as_str());
    block.push_str("\n</memory_context>");
    Ok(format!("{block}\n\n{input_text}"))
}

fn memory_auto_inject_tape_payload(query: &str, hits: &[MemorySearchHit]) -> String {
    let payload = json!({
        "query": truncate_with_ellipsis(query.to_owned(), 512),
        "injected_count": hits.len(),
        "hits": hits.iter().map(|hit| {
            json!({
                "memory_id": hit.item.memory_id,
                "source": hit.item.source.as_str(),
                "score": hit.score,
                "created_at_unix_ms": hit.item.created_at_unix_ms,
                "snippet": truncate_with_ellipsis(hit.snippet.clone(), 256),
            })
        }).collect::<Vec<_>>(),
    })
    .to_string();
    crate::journal::redact_payload_json(payload.as_bytes()).unwrap_or(payload)
}

fn memory_search_tool_output_payload(search_hits: &[MemorySearchHit]) -> Value {
    json!({
        "hits": search_hits.iter().map(|hit| {
            json!({
                "memory_id": hit.item.memory_id,
                "source": hit.item.source.as_str(),
                "snippet": redact_memory_text_for_output(hit.snippet.as_str()),
                "score": hit.score,
                "created_at_unix_ms": hit.item.created_at_unix_ms,
                "content_text": redact_memory_text_for_output(hit.item.content_text.as_str()),
                "content_hash": hit.item.content_hash,
                "tags": hit.item.tags,
                "confidence": hit.item.confidence,
                "breakdown": {
                    "lexical_score": hit.breakdown.lexical_score,
                    "vector_score": hit.breakdown.vector_score,
                    "recency_score": hit.breakdown.recency_score,
                    "final_score": hit.breakdown.final_score,
                }
            })
        }).collect::<Vec<_>>()
    })
}

fn build_tool_result_memory_text(
    tool_name: &str,
    success: bool,
    output_json: &[u8],
    error: &str,
) -> String {
    let output_preview = truncate_with_ellipsis(
        String::from_utf8_lossy(output_json).replace(['\r', '\n'], " "),
        512,
    );
    let error_preview = truncate_with_ellipsis(error.replace(['\r', '\n'], " "), 256);
    if success {
        format!("tool={tool_name} success=true output={output_preview}")
    } else {
        format!("tool={tool_name} success=false output={} error={error_preview}", output_preview)
    }
}

async fn execute_memory_search_tool(
    runtime_state: &Arc<GatewayRuntimeState>,
    principal: &str,
    channel: Option<&str>,
    session_id: &str,
    proposal_id: &str,
    input_json: &[u8],
) -> ToolExecutionOutcome {
    let parsed = match serde_json::from_slice::<Value>(input_json) {
        Ok(Value::Object(map)) => map,
        Ok(_) => {
            return memory_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                "palyra.memory.search requires JSON object input".to_owned(),
            );
        }
        Err(error) => {
            return memory_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.memory.search invalid JSON input: {error}"),
            );
        }
    };

    let query = match parsed.get("query").and_then(Value::as_str).map(str::trim) {
        Some(value) if !value.is_empty() => value.to_owned(),
        _ => {
            return memory_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                "palyra.memory.search requires non-empty string field 'query'".to_owned(),
            );
        }
    };
    if query.len() > MAX_MEMORY_TOOL_QUERY_BYTES {
        return memory_tool_execution_outcome(
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!("palyra.memory.search query exceeds {MAX_MEMORY_TOOL_QUERY_BYTES} bytes"),
        );
    }

    let scope = parsed.get("scope").and_then(Value::as_str).unwrap_or("session");
    let (channel_scope, session_scope, resource) = match scope {
        "principal" => {
            let channel_scope = channel.map(str::to_owned);
            let resource = channel_scope
                .as_deref()
                .map(|value| format!("memory:channel:{value}"))
                .unwrap_or_else(|| "memory:principal".to_owned());
            (channel_scope, None, resource)
        }
        "channel" => {
            let Some(channel) = channel.map(str::to_owned) else {
                return memory_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    "palyra.memory.search scope=channel requires authenticated channel context"
                        .to_owned(),
                );
            };
            let resource = format!("memory:channel:{channel}");
            (Some(channel), None, resource)
        }
        "session" => {
            let channel = channel.map(str::to_owned);
            let session = Some(session_id.to_owned());
            (channel, session, format!("memory:session:{session_id}"))
        }
        _ => {
            return memory_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                "palyra.memory.search scope must be one of: session|channel|principal".to_owned(),
            );
        }
    };

    if let Err(error) = authorize_memory_action(principal, "memory.search", resource.as_str()) {
        return memory_tool_execution_outcome(
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!("memory policy denied tool search request: {}", error.message()),
        );
    }

    let min_score = parsed.get("min_score").and_then(Value::as_f64).unwrap_or(0.0);
    if !min_score.is_finite() || !(0.0..=1.0).contains(&min_score) {
        return memory_tool_execution_outcome(
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            "palyra.memory.search min_score must be in range 0.0..=1.0".to_owned(),
        );
    }
    let top_k = parsed
        .get("top_k")
        .and_then(Value::as_u64)
        .map(|value| (value as usize).clamp(1, MAX_MEMORY_SEARCH_TOP_K))
        .unwrap_or(8);
    let tags = match parsed.get("tags") {
        Some(Value::Array(values)) => {
            if values.len() > MAX_MEMORY_TOOL_TAGS {
                return memory_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    format!("palyra.memory.search tags exceeds limit ({})", MAX_MEMORY_TOOL_TAGS),
                );
            }
            let mut parsed_tags = Vec::new();
            for value in values {
                let Some(tag) = value.as_str() else {
                    return memory_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        "palyra.memory.search tags must be strings".to_owned(),
                    );
                };
                if !tag.trim().is_empty() {
                    parsed_tags.push(tag.trim().to_owned());
                }
            }
            parsed_tags
        }
        Some(_) => {
            return memory_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                "palyra.memory.search tags must be an array of strings".to_owned(),
            );
        }
        None => Vec::new(),
    };
    let sources = match parsed.get("sources") {
        Some(Value::Array(values)) => {
            let mut parsed_sources = Vec::new();
            for value in values {
                let Some(source) = value.as_str() else {
                    return memory_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        "palyra.memory.search sources must be an array of strings".to_owned(),
                    );
                };
                let Some(memory_source) = parse_memory_source_literal(source) else {
                    return memory_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        format!("palyra.memory.search unknown source value: {source}"),
                    );
                };
                parsed_sources.push(memory_source);
            }
            parsed_sources
        }
        Some(_) => {
            return memory_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                "palyra.memory.search sources must be an array of strings".to_owned(),
            );
        }
        None => Vec::new(),
    };

    let search_hits = match runtime_state
        .search_memory(MemorySearchRequest {
            principal: principal.to_owned(),
            channel: channel_scope,
            session_id: session_scope,
            query,
            top_k,
            min_score,
            tags,
            sources,
        })
        .await
    {
        Ok(hits) => hits,
        Err(error) => {
            return memory_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.memory.search failed: {}", error.message()),
            );
        }
    };

    let payload = memory_search_tool_output_payload(search_hits.as_slice());
    match serde_json::to_vec(&payload) {
        Ok(output_json) => {
            memory_tool_execution_outcome(proposal_id, input_json, true, output_json, String::new())
        }
        Err(error) => memory_tool_execution_outcome(
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!("palyra.memory.search failed to serialize output: {error}"),
        ),
    }
}

async fn execute_http_fetch_tool(
    runtime_state: &Arc<GatewayRuntimeState>,
    proposal_id: &str,
    input_json: &[u8],
) -> ToolExecutionOutcome {
    if input_json.len() > MAX_HTTP_FETCH_TOOL_INPUT_BYTES {
        return http_fetch_tool_execution_outcome(
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!("palyra.http.fetch input exceeds {MAX_HTTP_FETCH_TOOL_INPUT_BYTES} bytes"),
        );
    }

    let payload = match serde_json::from_slice::<Value>(input_json) {
        Ok(Value::Object(map)) => map,
        Ok(_) => {
            return http_fetch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                "palyra.http.fetch requires JSON object input".to_owned(),
            );
        }
        Err(error) => {
            return http_fetch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.http.fetch invalid JSON input: {error}"),
            );
        }
    };

    let url_raw = match payload.get("url").and_then(Value::as_str).map(str::trim) {
        Some(value) if !value.is_empty() => value.to_owned(),
        _ => {
            return http_fetch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                "palyra.http.fetch requires non-empty string field 'url'".to_owned(),
            );
        }
    };
    let method = payload
        .get("method")
        .and_then(Value::as_str)
        .map(|value| value.trim().to_ascii_uppercase())
        .unwrap_or_else(|| "GET".to_owned());
    if !matches!(method.as_str(), "GET" | "HEAD" | "POST") {
        return http_fetch_tool_execution_outcome(
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            "palyra.http.fetch method must be one of: GET|HEAD|POST".to_owned(),
        );
    }

    let body = match payload.get("body") {
        Some(Value::String(value)) => value.clone(),
        Some(_) => {
            return http_fetch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                "palyra.http.fetch body must be a string".to_owned(),
            );
        }
        None => String::new(),
    };

    let request_headers = match payload.get("headers") {
        Some(Value::Object(values)) => {
            let mut headers = Vec::new();
            for (name, value) in values {
                let Value::String(raw_value) = value else {
                    return http_fetch_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        format!("palyra.http.fetch header '{name}' must be a string"),
                    );
                };
                let normalized_name = name.trim().to_ascii_lowercase();
                if normalized_name.is_empty() {
                    return http_fetch_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        "palyra.http.fetch header names cannot be empty".to_owned(),
                    );
                }
                if !runtime_state
                    .config
                    .http_fetch
                    .allowed_request_headers
                    .iter()
                    .any(|allowed| allowed == &normalized_name)
                {
                    return http_fetch_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        format!(
                            "palyra.http.fetch header '{normalized_name}' is not allowed by policy"
                        ),
                    );
                }
                headers.push((normalized_name, raw_value.clone()));
            }
            headers
        }
        Some(_) => {
            return http_fetch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                "palyra.http.fetch headers must be an object map".to_owned(),
            );
        }
        None => Vec::new(),
    };

    let allow_redirects = payload
        .get("allow_redirects")
        .and_then(Value::as_bool)
        .unwrap_or(runtime_state.config.http_fetch.allow_redirects);
    let max_redirects = payload
        .get("max_redirects")
        .and_then(Value::as_u64)
        .map(|value| value as usize)
        .unwrap_or(runtime_state.config.http_fetch.max_redirects)
        .clamp(1, MAX_HTTP_FETCH_REDIRECTS);
    let allow_private_targets =
        payload.get("allow_private_targets").and_then(Value::as_bool).unwrap_or(false)
            || runtime_state.config.http_fetch.allow_private_targets;
    let max_response_bytes = payload
        .get("max_response_bytes")
        .and_then(Value::as_u64)
        .map(|value| value as usize)
        .unwrap_or(runtime_state.config.http_fetch.max_response_bytes)
        .clamp(1, MAX_HTTP_FETCH_BODY_BYTES);
    let cache_enabled = payload
        .get("cache")
        .and_then(Value::as_bool)
        .unwrap_or(runtime_state.config.http_fetch.cache_enabled)
        && matches!(method.as_str(), "GET" | "HEAD");
    let cache_ttl_ms = payload
        .get("cache_ttl_ms")
        .and_then(Value::as_u64)
        .unwrap_or(runtime_state.config.http_fetch.cache_ttl_ms)
        .max(1);
    let allowed_content_types = match payload.get("allowed_content_types") {
        Some(Value::Array(values)) => {
            let mut parsed = Vec::new();
            for value in values {
                let Some(content_type) = value.as_str() else {
                    return http_fetch_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        "palyra.http.fetch allowed_content_types must be strings".to_owned(),
                    );
                };
                let normalized =
                    content_type.split(';').next().unwrap_or_default().trim().to_ascii_lowercase();
                if normalized.is_empty() {
                    continue;
                }
                if !runtime_state
                    .config
                    .http_fetch
                    .allowed_content_types
                    .iter()
                    .any(|allowed| allowed == &normalized)
                {
                    return http_fetch_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        format!(
                            "palyra.http.fetch content type '{normalized}' is not allowed by policy"
                        ),
                    );
                }
                if !parsed.iter().any(|existing| existing == &normalized) {
                    parsed.push(normalized);
                }
            }
            if parsed.is_empty() {
                runtime_state.config.http_fetch.allowed_content_types.clone()
            } else {
                parsed
            }
        }
        Some(_) => {
            return http_fetch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                "palyra.http.fetch allowed_content_types must be an array of strings".to_owned(),
            );
        }
        None => runtime_state.config.http_fetch.allowed_content_types.clone(),
    };

    let url = match Url::parse(url_raw.as_str()) {
        Ok(value) => value,
        Err(error) => {
            return http_fetch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.http.fetch URL is invalid: {error}"),
            );
        }
    };
    if !matches!(url.scheme(), "http" | "https") {
        return http_fetch_tool_execution_outcome(
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!("palyra.http.fetch blocked URL scheme '{}'", url.scheme()),
        );
    }
    if !url.username().is_empty() || url.password().is_some() {
        return http_fetch_tool_execution_outcome(
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            "palyra.http.fetch URL credentials are not allowed".to_owned(),
        );
    }

    let cache_key = http_fetch_cache_key(
        method.as_str(),
        url.as_str(),
        request_headers.as_slice(),
        body.as_str(),
    );
    if cache_enabled {
        let now = current_unix_ms();
        if let Ok(mut cache) = runtime_state.http_fetch_cache.lock() {
            cache.retain(|_, entry| entry.expires_at_unix_ms > now);
            if let Some(cached) = cache.get(cache_key.as_str()) {
                return http_fetch_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    true,
                    cached.output_json.clone(),
                    String::new(),
                );
            }
        }
    }

    let started_at = Instant::now();
    let mut current_url = url;
    let mut redirects_followed = 0_usize;
    loop {
        let resolved_addrs =
            match resolve_fetch_target_addresses(&current_url, allow_private_targets).await {
                Ok(value) => value,
                Err(error) => {
                    return http_fetch_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        format!("palyra.http.fetch target blocked: {error}"),
                    );
                }
            };

        let host = current_url.host_str().unwrap_or_default().to_owned();
        let mut client_builder = reqwest::Client::builder()
            .redirect(Policy::none())
            .connect_timeout(Duration::from_millis(
                runtime_state.config.http_fetch.connect_timeout_ms,
            ))
            .timeout(Duration::from_millis(runtime_state.config.http_fetch.request_timeout_ms));
        if !host.is_empty() && host.parse::<IpAddr>().is_err() {
            for address in resolved_addrs {
                client_builder = client_builder.resolve(host.as_str(), address);
            }
        }
        let client = match client_builder.build() {
            Ok(value) => value,
            Err(error) => {
                return http_fetch_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    format!("palyra.http.fetch failed to build HTTP client: {error}"),
                );
            }
        };

        let method_value = match method.parse::<reqwest::Method>() {
            Ok(value) => value,
            Err(error) => {
                return http_fetch_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    format!("palyra.http.fetch invalid method: {error}"),
                );
            }
        };
        let mut request = client.request(method_value, current_url.clone());
        for (name, value) in request_headers.as_slice() {
            request = request.header(name, value);
        }
        if method == "POST" && !body.is_empty() {
            request = request.body(body.clone());
        }
        let mut response = match request.send().await {
            Ok(value) => value,
            Err(error) => {
                return http_fetch_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    format!("palyra.http.fetch request failed: {error}"),
                );
            }
        };

        if response.status().is_redirection() {
            if !allow_redirects {
                return http_fetch_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    "palyra.http.fetch redirect blocked by policy".to_owned(),
                );
            }
            if redirects_followed >= max_redirects {
                return http_fetch_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    format!("palyra.http.fetch redirect limit exceeded ({max_redirects})"),
                );
            }
            let Some(location) = response.headers().get(reqwest::header::LOCATION) else {
                return http_fetch_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    "palyra.http.fetch redirect response missing Location header".to_owned(),
                );
            };
            let location_str = match location.to_str() {
                Ok(value) => value,
                Err(_) => {
                    return http_fetch_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        "palyra.http.fetch redirect Location header is invalid UTF-8".to_owned(),
                    );
                }
            };
            current_url = match current_url.join(location_str) {
                Ok(value) => value,
                Err(error) => {
                    return http_fetch_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        format!("palyra.http.fetch redirect URL is invalid: {error}"),
                    );
                }
            };
            redirects_followed = redirects_followed.saturating_add(1);
            continue;
        }

        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(|value| value.split(';').next().unwrap_or_default().trim().to_ascii_lowercase())
            .unwrap_or_default();
        if !content_type.is_empty()
            && !allowed_content_types.iter().any(|allowed| allowed == &content_type)
        {
            return http_fetch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.http.fetch content type '{content_type}' is blocked by policy"),
            );
        }

        let mut body_bytes = Vec::new();
        if method != "HEAD" {
            loop {
                let chunk = match response.chunk().await {
                    Ok(value) => value,
                    Err(error) => {
                        return http_fetch_tool_execution_outcome(
                            proposal_id,
                            input_json,
                            false,
                            b"{}".to_vec(),
                            format!("palyra.http.fetch failed to stream response body: {error}"),
                        );
                    }
                };
                let Some(chunk) = chunk else {
                    break;
                };
                if body_bytes.len().saturating_add(chunk.len()) > max_response_bytes {
                    return http_fetch_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        format!(
                            "palyra.http.fetch response exceeds max_response_bytes ({max_response_bytes})"
                        ),
                    );
                }
                body_bytes.extend_from_slice(chunk.as_ref());
            }
        }

        let status_code = response.status().as_u16();
        let success = response.status().is_success();
        let output_json = json!({
            "url": current_url.as_str(),
            "method": method,
            "status_code": status_code,
            "redirects_followed": redirects_followed,
            "content_type": content_type,
            "body_bytes": body_bytes.len(),
            "body_text": String::from_utf8_lossy(body_bytes.as_slice()).to_string(),
            "latency_ms": started_at.elapsed().as_millis() as u64,
            "request_headers": redacted_http_headers(request_headers.as_slice()),
        });
        let serialized = serde_json::to_vec(&output_json).unwrap_or_else(|_| b"{}".to_vec());
        if cache_enabled && success {
            if let Ok(mut cache) = runtime_state.http_fetch_cache.lock() {
                let now = current_unix_ms();
                cache.retain(|_, entry| entry.expires_at_unix_ms > now);
                while cache.len() >= runtime_state.config.http_fetch.max_cache_entries {
                    let Some(first_key) = cache.keys().next().cloned() else {
                        break;
                    };
                    cache.remove(first_key.as_str());
                }
                cache.insert(
                    cache_key.clone(),
                    CachedHttpFetchEntry {
                        expires_at_unix_ms: now.saturating_add(cache_ttl_ms as i64),
                        output_json: serialized.clone(),
                    },
                );
            }
        }
        return http_fetch_tool_execution_outcome(
            proposal_id,
            input_json,
            success,
            serialized,
            if success {
                String::new()
            } else {
                format!("palyra.http.fetch returned HTTP {status_code}")
            },
        );
    }
}

fn http_fetch_cache_key(
    method: &str,
    url: &str,
    headers: &[(String, String)],
    body: &str,
) -> String {
    let mut normalized_headers =
        headers.iter().map(|(name, value)| format!("{name}:{value}")).collect::<Vec<_>>();
    normalized_headers.sort();
    let mut key =
        format!("{method}|{url}|{}|{}", normalized_headers.join("&"), sha256_hex(body.as_bytes()));
    if key.len() > MAX_HTTP_FETCH_CACHE_KEY_BYTES {
        key = format!("sha256:{}", sha256_hex(key.as_bytes()));
    }
    key
}

async fn resolve_fetch_target_addresses(
    url: &Url,
    allow_private_targets: bool,
) -> Result<Vec<SocketAddr>, String> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err(format!("blocked URL scheme '{}'", url.scheme()));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err("URL credentials are not allowed".to_owned());
    }
    let host = url.host_str().ok_or_else(|| "URL host is required".to_owned())?;
    let port =
        url.port_or_known_default().ok_or_else(|| "URL port could not be resolved".to_owned())?;

    let addrs = if let Ok(ip) = host.parse::<IpAddr>() {
        vec![SocketAddr::new(ip, port)]
    } else {
        let resolved = tokio::net::lookup_host((host, port))
            .await
            .map_err(|error| format!("DNS resolution failed for host '{host}': {error}"))?;
        resolved.collect::<Vec<_>>()
    };
    if addrs.is_empty() {
        return Err(format!("DNS resolution returned no addresses for host '{host}'"));
    }
    validate_resolved_fetch_addresses(addrs.as_slice(), allow_private_targets)?;
    Ok(addrs)
}

fn validate_resolved_fetch_addresses(
    addrs: &[SocketAddr],
    allow_private_targets: bool,
) -> Result<(), String> {
    if addrs.is_empty() {
        return Err("DNS resolution returned no addresses".to_owned());
    }
    if !allow_private_targets && addrs.iter().any(|address| is_private_or_local_ip(address.ip())) {
        return Err("target resolves to private/local address and is blocked by policy".to_owned());
    }
    Ok(())
}

fn is_private_or_local_ip(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(ipv4) => is_private_or_local_ipv4(ipv4),
        IpAddr::V6(ipv6) => is_private_or_local_ipv6(ipv6),
    }
}

fn is_private_or_local_ipv4(address: Ipv4Addr) -> bool {
    address.is_private()
        || address.is_loopback()
        || address.is_link_local()
        || address.is_unspecified()
        || address.is_multicast()
        || is_special_ipv4_ssrf_range(address)
}

fn is_private_or_local_ipv6(address: Ipv6Addr) -> bool {
    if let Some(mapped_ipv4) = address.to_ipv4_mapped() {
        return is_private_or_local_ipv4(mapped_ipv4);
    }
    address.is_loopback()
        || address.is_unicast_link_local()
        || address.is_unique_local()
        || address.is_unspecified()
        || address.is_multicast()
        || is_documentation_ipv6(address)
        || is_site_local_ipv6(address)
}

fn is_special_ipv4_ssrf_range(address: Ipv4Addr) -> bool {
    let octets = address.octets();
    let first = octets[0];
    let second = octets[1];
    let third = octets[2];

    first == 0
        || (first == 100 && (64..=127).contains(&second))
        || (first == 192 && second == 0 && third == 0)
        || (first == 192 && second == 0 && third == 2)
        || (first == 198 && second == 18)
        || (first == 198 && second == 19)
        || (first == 198 && second == 51 && third == 100)
        || (first == 203 && second == 0 && third == 113)
        || first >= 240
}

fn is_documentation_ipv6(address: Ipv6Addr) -> bool {
    let segments = address.segments();
    segments[0] == 0x2001 && segments[1] == 0x0db8
}

fn is_site_local_ipv6(address: Ipv6Addr) -> bool {
    (address.segments()[0] & 0xffc0) == 0xfec0
}

fn redacted_http_headers(headers: &[(String, String)]) -> Vec<serde_json::Value> {
    headers
        .iter()
        .map(|(name, value)| {
            let sensitive = name.contains("authorization")
                || name.contains("cookie")
                || name.contains("token")
                || name.contains("api-key")
                || name.contains("apikey");
            json!({
                "name": name,
                "value": if sensitive { "<redacted>" } else { value.as_str() }
            })
        })
        .collect()
}

fn http_fetch_tool_execution_outcome(
    proposal_id: &str,
    input_json: &[u8],
    success: bool,
    output_json: Vec<u8>,
    error: String,
) -> ToolExecutionOutcome {
    let executed_at_unix_ms = current_unix_ms();
    let mut hasher = Sha256::new();
    hasher.update(b"palyra.http.fetch.attestation.v1");
    hasher.update((proposal_id.len() as u64).to_be_bytes());
    hasher.update(proposal_id.as_bytes());
    hasher.update((input_json.len() as u64).to_be_bytes());
    hasher.update(input_json);
    hasher.update([u8::from(success)]);
    hasher.update((output_json.len() as u64).to_be_bytes());
    hasher.update(output_json.as_slice());
    hasher.update((error.len() as u64).to_be_bytes());
    hasher.update(error.as_bytes());
    hasher.update(executed_at_unix_ms.to_be_bytes());
    let execution_sha256 = format!("{:x}", hasher.finalize());

    ToolExecutionOutcome {
        success,
        output_json,
        error,
        attestation: ToolAttestation {
            attestation_id: Ulid::new().to_string(),
            execution_sha256,
            executed_at_unix_ms,
            timed_out: false,
            executor: "gateway_http_fetch".to_owned(),
            sandbox_enforcement: "ssrf_guard".to_owned(),
        },
    }
}

async fn execute_browser_tool(
    runtime_state: &Arc<GatewayRuntimeState>,
    principal: &str,
    channel: Option<&str>,
    tool_name: &str,
    proposal_id: &str,
    input_json: &[u8],
) -> ToolExecutionOutcome {
    if input_json.len() > MAX_BROWSER_TOOL_INPUT_BYTES {
        return browser_tool_execution_outcome(
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!("palyra.browser.* input exceeds {MAX_BROWSER_TOOL_INPUT_BYTES} bytes"),
        );
    }
    if !runtime_state.config.browser_service.enabled {
        return browser_tool_execution_outcome(
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            "palyra.browser.* is disabled by runtime config (tool_call.browser_service.enabled=false)"
                .to_owned(),
        );
    }

    let payload = match serde_json::from_slice::<Value>(input_json) {
        Ok(Value::Object(map)) => map,
        Ok(_) => {
            return browser_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                "palyra.browser.* requires JSON object input".to_owned(),
            );
        }
        Err(error) => {
            return browser_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.browser.* invalid JSON input: {error}"),
            );
        }
    };

    let mut client =
        match connect_browser_service(runtime_state.config.browser_service.clone()).await {
            Ok(value) => value,
            Err(error) => {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
        };

    let outcome = match tool_name {
        BROWSER_SESSION_CREATE_TOOL_NAME => {
            let idle_ttl_ms = payload.get("idle_ttl_ms").and_then(Value::as_u64).unwrap_or(0);
            let allow_private_targets =
                payload.get("allow_private_targets").and_then(Value::as_bool).unwrap_or(false);
            let budget = payload.get("budget").and_then(Value::as_object).map(|value| {
                browser_v1::SessionBudget {
                    max_navigation_timeout_ms: value
                        .get("max_navigation_timeout_ms")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                    max_session_lifetime_ms: value
                        .get("max_session_lifetime_ms")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                    max_screenshot_bytes: value
                        .get("max_screenshot_bytes")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                    max_response_bytes: value
                        .get("max_response_bytes")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                    max_action_timeout_ms: value
                        .get("max_action_timeout_ms")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                    max_type_input_bytes: value
                        .get("max_type_input_bytes")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                    max_actions_per_session: value
                        .get("max_actions_per_session")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                    max_actions_per_window: value
                        .get("max_actions_per_window")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                    action_rate_window_ms: value
                        .get("action_rate_window_ms")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                    max_action_log_entries: value
                        .get("max_action_log_entries")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                    max_observe_snapshot_bytes: value
                        .get("max_observe_snapshot_bytes")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                    max_visible_text_bytes: value
                        .get("max_visible_text_bytes")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                    max_network_log_entries: value
                        .get("max_network_log_entries")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                    max_network_log_bytes: value
                        .get("max_network_log_bytes")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                }
            });
            let mut request = Request::new(browser_v1::CreateSessionRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                principal: principal.to_owned(),
                idle_ttl_ms,
                budget,
                allow_private_targets,
                allow_downloads: payload
                    .get("allow_downloads")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                action_allowed_domains: payload
                    .get("action_allowed_domains")
                    .and_then(Value::as_array)
                    .map(|entries| {
                        entries
                            .iter()
                            .filter_map(Value::as_str)
                            .map(str::trim)
                            .filter(|value| !value.is_empty())
                            .map(str::to_owned)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default(),
                persistence_enabled: payload
                    .get("persistence_enabled")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                persistence_id: payload
                    .get("persistence_id")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .unwrap_or_default()
                    .to_owned(),
                channel: channel.unwrap_or_default().to_owned(),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                runtime_state.config.browser_service.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.create_session(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let session_id =
                        if let Some(value) = response.session_id { Some(value.ulid) } else { None };
                    let output = json!({
                        "session_id": session_id,
                        "created_at_unix_ms": response.created_at_unix_ms,
                        "effective_budget": response.effective_budget.map(|value| json!({
                            "max_navigation_timeout_ms": value.max_navigation_timeout_ms,
                            "max_session_lifetime_ms": value.max_session_lifetime_ms,
                            "max_screenshot_bytes": value.max_screenshot_bytes,
                            "max_response_bytes": value.max_response_bytes,
                            "max_action_timeout_ms": value.max_action_timeout_ms,
                            "max_type_input_bytes": value.max_type_input_bytes,
                            "max_actions_per_session": value.max_actions_per_session,
                            "max_actions_per_window": value.max_actions_per_window,
                            "action_rate_window_ms": value.action_rate_window_ms,
                            "max_action_log_entries": value.max_action_log_entries,
                            "max_observe_snapshot_bytes": value.max_observe_snapshot_bytes,
                            "max_visible_text_bytes": value.max_visible_text_bytes,
                            "max_network_log_entries": value.max_network_log_entries,
                            "max_network_log_bytes": value.max_network_log_bytes,
                        })),
                        "downloads_enabled": response.downloads_enabled,
                        "action_allowed_domains": response.action_allowed_domains,
                        "persistence_enabled": response.persistence_enabled,
                        "persistence_id": response.persistence_id,
                        "state_restored": response.state_restored,
                    });
                    (
                        true,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        String::new(),
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!(
                        "palyra.browser.session.create failed: {}",
                        sanitize_status_message(&error)
                    ),
                ),
            }
        }
        BROWSER_SESSION_CLOSE_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let mut request = Request::new(browser_v1::CloseSessionRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                runtime_state.config.browser_service.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.close_session(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let output = json!({
                        "closed": response.closed,
                        "reason": response.reason,
                    });
                    (
                        response.closed,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if response.closed {
                            String::new()
                        } else {
                            "browser session was not closed".to_owned()
                        },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!(
                        "palyra.browser.session.close failed: {}",
                        sanitize_status_message(&error)
                    ),
                ),
            }
        }
        BROWSER_NAVIGATE_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let Some(url) = payload.get("url").and_then(Value::as_str).map(str::trim) else {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    "palyra.browser.navigate requires non-empty string field 'url'".to_owned(),
                );
            };
            if url.is_empty() {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    "palyra.browser.navigate requires non-empty string field 'url'".to_owned(),
                );
            }
            let mut request = Request::new(browser_v1::NavigateRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                url: url.to_owned(),
                timeout_ms: payload.get("timeout_ms").and_then(Value::as_u64).unwrap_or(0),
                allow_redirects: payload
                    .get("allow_redirects")
                    .and_then(Value::as_bool)
                    .unwrap_or(true),
                max_redirects: payload.get("max_redirects").and_then(Value::as_u64).unwrap_or(3)
                    as u32,
                allow_private_targets: payload
                    .get("allow_private_targets")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                runtime_state.config.browser_service.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.navigate(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let output = json!({
                        "success": response.success,
                        "final_url": response.final_url,
                        "status_code": response.status_code,
                        "title": response.title,
                        "body_bytes": response.body_bytes,
                        "latency_ms": response.latency_ms,
                        "error": response.error,
                    });
                    (
                        response.success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if response.success { String::new() } else { response.error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!("palyra.browser.navigate failed: {}", sanitize_status_message(&error)),
                ),
            }
        }
        BROWSER_CLICK_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let Some(selector) = payload.get("selector").and_then(Value::as_str).map(str::trim)
            else {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    "palyra.browser.click requires non-empty string field 'selector'".to_owned(),
                );
            };
            if selector.is_empty() {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    "palyra.browser.click requires non-empty string field 'selector'".to_owned(),
                );
            }
            let mut request = Request::new(browser_v1::ClickRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                selector: selector.to_owned(),
                max_retries: payload.get("max_retries").and_then(Value::as_u64).unwrap_or(0) as u32,
                timeout_ms: payload.get("timeout_ms").and_then(Value::as_u64).unwrap_or(0),
                capture_failure_screenshot: payload
                    .get("capture_failure_screenshot")
                    .and_then(Value::as_bool)
                    .unwrap_or(true),
                max_failure_screenshot_bytes: payload
                    .get("max_failure_screenshot_bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or(runtime_state.config.browser_service.max_screenshot_bytes as u64),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                runtime_state.config.browser_service.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.click(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let output = json!({
                        "success": response.success,
                        "error": response.error,
                        "action_log": response.action_log.map(browser_action_log_to_json),
                        "failure_screenshot_mime_type": response.failure_screenshot_mime_type,
                        "failure_screenshot_base64": base64::engine::general_purpose::STANDARD
                            .encode(response.failure_screenshot_bytes.as_slice()),
                    });
                    (
                        response.success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if response.success { String::new() } else { response.error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!("palyra.browser.click failed: {}", sanitize_status_message(&error)),
                ),
            }
        }
        BROWSER_TYPE_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let Some(selector) = payload.get("selector").and_then(Value::as_str).map(str::trim)
            else {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    "palyra.browser.type requires non-empty string field 'selector'".to_owned(),
                );
            };
            if selector.is_empty() {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    "palyra.browser.type requires non-empty string field 'selector'".to_owned(),
                );
            }
            let text = payload.get("text").and_then(Value::as_str).unwrap_or_default();
            let mut request = Request::new(browser_v1::TypeRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                selector: selector.to_owned(),
                text: text.to_owned(),
                clear_existing: payload
                    .get("clear_existing")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                timeout_ms: payload.get("timeout_ms").and_then(Value::as_u64).unwrap_or(0),
                capture_failure_screenshot: payload
                    .get("capture_failure_screenshot")
                    .and_then(Value::as_bool)
                    .unwrap_or(true),
                max_failure_screenshot_bytes: payload
                    .get("max_failure_screenshot_bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or(runtime_state.config.browser_service.max_screenshot_bytes as u64),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                runtime_state.config.browser_service.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.r#type(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let output = json!({
                        "success": response.success,
                        "typed_bytes": response.typed_bytes,
                        "error": response.error,
                        "action_log": response.action_log.map(browser_action_log_to_json),
                        "failure_screenshot_mime_type": response.failure_screenshot_mime_type,
                        "failure_screenshot_base64": base64::engine::general_purpose::STANDARD
                            .encode(response.failure_screenshot_bytes.as_slice()),
                    });
                    (
                        response.success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if response.success { String::new() } else { response.error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!("palyra.browser.type failed: {}", sanitize_status_message(&error)),
                ),
            }
        }
        BROWSER_SCROLL_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let mut request = Request::new(browser_v1::ScrollRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                delta_x: payload.get("delta_x").and_then(Value::as_i64).unwrap_or(0),
                delta_y: payload.get("delta_y").and_then(Value::as_i64).unwrap_or(0),
                capture_failure_screenshot: payload
                    .get("capture_failure_screenshot")
                    .and_then(Value::as_bool)
                    .unwrap_or(true),
                max_failure_screenshot_bytes: payload
                    .get("max_failure_screenshot_bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or(runtime_state.config.browser_service.max_screenshot_bytes as u64),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                runtime_state.config.browser_service.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.scroll(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let output = json!({
                        "success": response.success,
                        "scroll_x": response.scroll_x,
                        "scroll_y": response.scroll_y,
                        "error": response.error,
                        "action_log": response.action_log.map(browser_action_log_to_json),
                        "failure_screenshot_mime_type": response.failure_screenshot_mime_type,
                        "failure_screenshot_base64": base64::engine::general_purpose::STANDARD
                            .encode(response.failure_screenshot_bytes.as_slice()),
                    });
                    (
                        response.success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if response.success { String::new() } else { response.error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!("palyra.browser.scroll failed: {}", sanitize_status_message(&error)),
                ),
            }
        }
        BROWSER_WAIT_FOR_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let mut request = Request::new(browser_v1::WaitForRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                selector: payload
                    .get("selector")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                text: payload.get("text").and_then(Value::as_str).unwrap_or_default().to_owned(),
                timeout_ms: payload.get("timeout_ms").and_then(Value::as_u64).unwrap_or(0),
                poll_interval_ms: payload
                    .get("poll_interval_ms")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
                capture_failure_screenshot: payload
                    .get("capture_failure_screenshot")
                    .and_then(Value::as_bool)
                    .unwrap_or(true),
                max_failure_screenshot_bytes: payload
                    .get("max_failure_screenshot_bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or(runtime_state.config.browser_service.max_screenshot_bytes as u64),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                runtime_state.config.browser_service.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.wait_for(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let output = json!({
                        "success": response.success,
                        "waited_ms": response.waited_ms,
                        "error": response.error,
                        "matched_selector": response.matched_selector,
                        "matched_text": response.matched_text,
                        "action_log": response.action_log.map(browser_action_log_to_json),
                        "failure_screenshot_mime_type": response.failure_screenshot_mime_type,
                        "failure_screenshot_base64": base64::engine::general_purpose::STANDARD
                            .encode(response.failure_screenshot_bytes.as_slice()),
                    });
                    (
                        response.success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if response.success { String::new() } else { response.error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!("palyra.browser.wait_for failed: {}", sanitize_status_message(&error)),
                ),
            }
        }
        BROWSER_TITLE_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let mut request = Request::new(browser_v1::GetTitleRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                max_title_bytes: payload
                    .get("max_title_bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or(runtime_state.config.browser_service.max_title_bytes as u64),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                runtime_state.config.browser_service.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.get_title(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let output = json!({
                        "success": response.success,
                        "title": response.title,
                        "error": response.error,
                    });
                    (
                        response.success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if response.success { String::new() } else { response.error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!("palyra.browser.title failed: {}", sanitize_status_message(&error)),
                ),
            }
        }
        BROWSER_SCREENSHOT_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let mut request = Request::new(browser_v1::ScreenshotRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                max_bytes: payload
                    .get("max_bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or(runtime_state.config.browser_service.max_screenshot_bytes as u64),
                format: payload.get("format").and_then(Value::as_str).unwrap_or("png").to_owned(),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                runtime_state.config.browser_service.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.screenshot(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let output = json!({
                        "success": response.success,
                        "mime_type": response.mime_type,
                        "image_base64": base64::engine::general_purpose::STANDARD
                            .encode(response.image_bytes.as_slice()),
                        "error": response.error,
                    });
                    (
                        response.success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if response.success { String::new() } else { response.error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!(
                        "palyra.browser.screenshot failed: {}",
                        sanitize_status_message(&error)
                    ),
                ),
            }
        }
        BROWSER_OBSERVE_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let mut request = Request::new(browser_v1::ObserveRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                include_dom_snapshot: payload
                    .get("include_dom_snapshot")
                    .and_then(Value::as_bool)
                    .unwrap_or(true),
                include_accessibility_tree: payload
                    .get("include_accessibility_tree")
                    .and_then(Value::as_bool)
                    .unwrap_or(true),
                include_visible_text: payload
                    .get("include_visible_text")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                max_dom_snapshot_bytes: payload
                    .get("max_dom_snapshot_bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
                max_accessibility_tree_bytes: payload
                    .get("max_accessibility_tree_bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
                max_visible_text_bytes: payload
                    .get("max_visible_text_bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                runtime_state.config.browser_service.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.observe(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let output = json!({
                        "success": response.success,
                        "dom_snapshot": response.dom_snapshot,
                        "accessibility_tree": response.accessibility_tree,
                        "visible_text": response.visible_text,
                        "dom_truncated": response.dom_truncated,
                        "accessibility_tree_truncated": response.accessibility_tree_truncated,
                        "visible_text_truncated": response.visible_text_truncated,
                        "page_url": response.page_url,
                        "error": response.error,
                    });
                    (
                        response.success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if response.success { String::new() } else { response.error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!("palyra.browser.observe failed: {}", sanitize_status_message(&error)),
                ),
            }
        }
        BROWSER_NETWORK_LOG_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let mut request = Request::new(browser_v1::NetworkLogRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                limit: payload.get("limit").and_then(Value::as_u64).unwrap_or(0) as u32,
                include_headers: payload
                    .get("include_headers")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                max_payload_bytes: payload
                    .get("max_payload_bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                runtime_state.config.browser_service.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.network_log(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let output = json!({
                        "success": response.success,
                        "entries": response
                            .entries
                            .into_iter()
                            .map(browser_network_log_entry_to_json)
                            .collect::<Vec<_>>(),
                        "truncated": response.truncated,
                        "error": response.error,
                    });
                    (
                        response.success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if response.success { String::new() } else { response.error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!(
                        "palyra.browser.network_log failed: {}",
                        sanitize_status_message(&error)
                    ),
                ),
            }
        }
        BROWSER_RESET_STATE_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let mut request = Request::new(browser_v1::ResetStateRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                clear_cookies: payload
                    .get("clear_cookies")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                clear_storage: payload
                    .get("clear_storage")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                reset_tabs: payload.get("reset_tabs").and_then(Value::as_bool).unwrap_or(false),
                reset_permissions: payload
                    .get("reset_permissions")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                runtime_state.config.browser_service.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.reset_state(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let output = json!({
                        "success": response.success,
                        "cookies_cleared": response.cookies_cleared,
                        "storage_entries_cleared": response.storage_entries_cleared,
                        "tabs_closed": response.tabs_closed,
                        "permissions": response.permissions.map(browser_permissions_to_json),
                        "error": response.error,
                    });
                    (
                        response.success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if response.success { String::new() } else { response.error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!(
                        "palyra.browser.reset_state failed: {}",
                        sanitize_status_message(&error)
                    ),
                ),
            }
        }
        BROWSER_TABS_LIST_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let mut request = Request::new(browser_v1::ListTabsRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                runtime_state.config.browser_service.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.list_tabs(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let output = json!({
                        "success": response.success,
                        "tabs": response.tabs.into_iter().map(browser_tab_to_json).collect::<Vec<_>>(),
                        "active_tab_id": response.active_tab_id.map(|value| value.ulid),
                        "error": response.error,
                    });
                    (
                        response.success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if response.success { String::new() } else { response.error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!("palyra.browser.tabs.list failed: {}", sanitize_status_message(&error)),
                ),
            }
        }
        BROWSER_TABS_OPEN_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let mut request = Request::new(browser_v1::OpenTabRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                url: payload.get("url").and_then(Value::as_str).unwrap_or_default().to_owned(),
                activate: payload.get("activate").and_then(Value::as_bool).unwrap_or(true),
                timeout_ms: payload.get("timeout_ms").and_then(Value::as_u64).unwrap_or(0),
                allow_redirects: payload
                    .get("allow_redirects")
                    .and_then(Value::as_bool)
                    .unwrap_or(true),
                max_redirects: payload.get("max_redirects").and_then(Value::as_u64).unwrap_or(3)
                    as u32,
                allow_private_targets: payload
                    .get("allow_private_targets")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                runtime_state.config.browser_service.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.open_tab(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let output = json!({
                        "success": response.success,
                        "tab": response.tab.map(browser_tab_to_json),
                        "navigated": response.navigated,
                        "status_code": response.status_code,
                        "error": response.error,
                    });
                    (
                        response.success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if response.success { String::new() } else { response.error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!("palyra.browser.tabs.open failed: {}", sanitize_status_message(&error)),
                ),
            }
        }
        BROWSER_TABS_SWITCH_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let tab_id = match parse_browser_tool_tab_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let mut request = Request::new(browser_v1::SwitchTabRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                tab_id: Some(common_v1::CanonicalId { ulid: tab_id }),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                runtime_state.config.browser_service.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.switch_tab(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let output = json!({
                        "success": response.success,
                        "active_tab": response.active_tab.map(browser_tab_to_json),
                        "error": response.error,
                    });
                    (
                        response.success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if response.success { String::new() } else { response.error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!(
                        "palyra.browser.tabs.switch failed: {}",
                        sanitize_status_message(&error)
                    ),
                ),
            }
        }
        BROWSER_TABS_CLOSE_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let tab_id = match payload.get("tab_id") {
                Some(Value::String(raw)) => {
                    let trimmed = raw.trim();
                    if trimmed.is_empty() {
                        None
                    } else {
                        match validate_canonical_id(trimmed) {
                            Ok(_) => Some(common_v1::CanonicalId { ulid: trimmed.to_owned() }),
                            Err(error) => {
                                return browser_tool_execution_outcome(
                                    proposal_id,
                                    input_json,
                                    false,
                                    b"{}".to_vec(),
                                    format!("palyra.browser.tabs.close tab_id is invalid: {error}"),
                                );
                            }
                        }
                    }
                }
                Some(_) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        "palyra.browser.tabs.close field 'tab_id' must be a string".to_owned(),
                    );
                }
                None => None,
            };
            let mut request = Request::new(browser_v1::CloseTabRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                tab_id,
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                runtime_state.config.browser_service.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.close_tab(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let output = json!({
                        "success": response.success,
                        "closed_tab_id": response.closed_tab_id.map(|value| value.ulid),
                        "active_tab": response.active_tab.map(browser_tab_to_json),
                        "tabs_remaining": response.tabs_remaining,
                        "error": response.error,
                    });
                    (
                        response.success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if response.success { String::new() } else { response.error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!(
                        "palyra.browser.tabs.close failed: {}",
                        sanitize_status_message(&error)
                    ),
                ),
            }
        }
        BROWSER_PERMISSIONS_GET_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let mut request = Request::new(browser_v1::GetPermissionsRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                runtime_state.config.browser_service.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.get_permissions(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let output = json!({
                        "success": response.success,
                        "permissions": response.permissions.map(browser_permissions_to_json),
                        "error": response.error,
                    });
                    (
                        response.success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if response.success { String::new() } else { response.error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!(
                        "palyra.browser.permissions.get failed: {}",
                        sanitize_status_message(&error)
                    ),
                ),
            }
        }
        BROWSER_PERMISSIONS_SET_TOOL_NAME => {
            let session_id = match parse_browser_tool_session_id(&payload) {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let camera = match parse_browser_permission_setting(&payload, "camera") {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let microphone = match parse_browser_permission_setting(&payload, "microphone") {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let location = match parse_browser_permission_setting(&payload, "location") {
                Ok(value) => value,
                Err(error) => {
                    return browser_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            };
            let mut request = Request::new(browser_v1::SetPermissionsRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                camera,
                microphone,
                location,
                reset_to_default: payload
                    .get("reset_to_default")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            });
            if let Err(error) = attach_browser_auth_metadata(
                &mut request,
                runtime_state.config.browser_service.auth_token.as_deref(),
            ) {
                return browser_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
            match client.set_permissions(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let output = json!({
                        "success": response.success,
                        "permissions": response.permissions.map(browser_permissions_to_json),
                        "error": response.error,
                    });
                    (
                        response.success,
                        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
                        if response.success { String::new() } else { response.error },
                    )
                }
                Err(error) => (
                    false,
                    b"{}".to_vec(),
                    format!(
                        "palyra.browser.permissions.set failed: {}",
                        sanitize_status_message(&error)
                    ),
                ),
            }
        }
        _ => (false, b"{}".to_vec(), "palyra.browser.* unsupported tool name".to_owned()),
    };

    browser_tool_execution_outcome(proposal_id, input_json, outcome.0, outcome.1, outcome.2)
}

async fn connect_browser_service(
    config: BrowserServiceRuntimeConfig,
) -> Result<
    browser_v1::browser_service_client::BrowserServiceClient<tonic::transport::Channel>,
    String,
> {
    let endpoint = tonic::transport::Endpoint::from_shared(config.endpoint.clone())
        .map_err(|error| {
            format!("invalid browser service endpoint '{}': {error}", config.endpoint)
        })?
        .connect_timeout(Duration::from_millis(config.connect_timeout_ms))
        .timeout(Duration::from_millis(config.request_timeout_ms));
    let channel = endpoint.connect().await.map_err(|error| {
        format!("failed to connect to browser service '{}': {error}", config.endpoint)
    })?;
    Ok(browser_v1::browser_service_client::BrowserServiceClient::new(channel))
}

fn parse_browser_tool_session_id(
    payload: &serde_json::Map<String, Value>,
) -> Result<String, String> {
    let Some(session_id) = payload.get("session_id").and_then(Value::as_str).map(str::trim) else {
        return Err("palyra.browser.* requires non-empty string field 'session_id'".to_owned());
    };
    if session_id.is_empty() {
        return Err("palyra.browser.* requires non-empty string field 'session_id'".to_owned());
    }
    validate_canonical_id(session_id)
        .map_err(|error| format!("palyra.browser.* session_id is invalid: {error}"))?;
    Ok(session_id.to_owned())
}

fn parse_browser_tool_tab_id(payload: &serde_json::Map<String, Value>) -> Result<String, String> {
    let Some(tab_id) = payload.get("tab_id").and_then(Value::as_str).map(str::trim) else {
        return Err("palyra.browser.tabs.* requires non-empty string field 'tab_id'".to_owned());
    };
    if tab_id.is_empty() {
        return Err("palyra.browser.tabs.* requires non-empty string field 'tab_id'".to_owned());
    }
    validate_canonical_id(tab_id)
        .map_err(|error| format!("palyra.browser.tabs.* tab_id is invalid: {error}"))?;
    Ok(tab_id.to_owned())
}

fn parse_browser_permission_setting(
    payload: &serde_json::Map<String, Value>,
    field: &str,
) -> Result<i32, String> {
    let Some(value) = payload.get(field) else {
        return Ok(0);
    };
    match value {
        Value::Number(number) => number
            .as_i64()
            .filter(|candidate| (0..=2).contains(candidate))
            .map(|candidate| candidate as i32)
            .ok_or_else(|| {
                format!("palyra.browser.permissions.set field '{field}' must be 0, 1, or 2")
            }),
        Value::String(raw) => {
            let normalized = raw.trim().to_ascii_lowercase();
            match normalized.as_str() {
                "" | "unspecified" => Ok(0),
                "deny" => Ok(1),
                "allow" => Ok(2),
                _ => Err(format!(
                    "palyra.browser.permissions.set field '{field}' must be one of: allow|deny|unspecified"
                )),
            }
        }
        _ => Err(format!(
            "palyra.browser.permissions.set field '{field}' must be a string or integer"
        )),
    }
}

fn attach_browser_auth_metadata<T>(
    request: &mut Request<T>,
    auth_token: Option<&str>,
) -> Result<(), String> {
    let Some(token) = auth_token.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(());
    };
    let value = tonic::metadata::MetadataValue::try_from(format!("Bearer {token}"))
        .map_err(|error| format!("invalid browser service auth token metadata: {error}"))?;
    request.metadata_mut().insert("authorization", value);
    Ok(())
}

fn sanitize_status_message(status: &Status) -> String {
    truncate_with_ellipsis(status.message().to_owned(), 512)
}

fn browser_action_log_to_json(entry: browser_v1::BrowserActionLogEntry) -> Value {
    json!({
        "action_id": entry.action_id,
        "action_name": entry.action_name,
        "selector": entry.selector,
        "success": entry.success,
        "outcome": entry.outcome,
        "error": entry.error,
        "started_at_unix_ms": entry.started_at_unix_ms,
        "completed_at_unix_ms": entry.completed_at_unix_ms,
        "attempts": entry.attempts,
        "page_url": entry.page_url,
    })
}

fn browser_network_log_entry_to_json(entry: browser_v1::NetworkLogEntry) -> Value {
    let mut headers = entry
        .headers
        .into_iter()
        .map(|header| json!({ "name": header.name, "value": header.value }))
        .collect::<Vec<_>>();
    headers.sort_by(|left, right| {
        let left_name = left.get("name").and_then(Value::as_str).unwrap_or_default();
        let right_name = right.get("name").and_then(Value::as_str).unwrap_or_default();
        left_name.cmp(right_name)
    });
    json!({
        "request_url": entry.request_url,
        "status_code": entry.status_code,
        "timing_bucket": entry.timing_bucket,
        "latency_ms": entry.latency_ms,
        "captured_at_unix_ms": entry.captured_at_unix_ms,
        "headers": headers,
    })
}

fn browser_tab_to_json(tab: browser_v1::BrowserTab) -> Value {
    json!({
        "tab_id": tab.tab_id.map(|value| value.ulid),
        "url": tab.url,
        "title": tab.title,
        "active": tab.active,
    })
}

fn browser_permission_setting_label(value: i32) -> &'static str {
    match value {
        1 => "deny",
        2 => "allow",
        _ => "unspecified",
    }
}

fn browser_permissions_to_json(permissions: browser_v1::SessionPermissions) -> Value {
    json!({
        "camera": browser_permission_setting_label(permissions.camera),
        "microphone": browser_permission_setting_label(permissions.microphone),
        "location": browser_permission_setting_label(permissions.location),
    })
}

fn browser_tool_execution_outcome(
    proposal_id: &str,
    input_json: &[u8],
    success: bool,
    output_json: Vec<u8>,
    error: String,
) -> ToolExecutionOutcome {
    let executed_at_unix_ms = current_unix_ms();
    let mut hasher = Sha256::new();
    hasher.update(b"palyra.browser.tool.attestation.v1");
    hasher.update((proposal_id.len() as u64).to_be_bytes());
    hasher.update(proposal_id.as_bytes());
    hasher.update((input_json.len() as u64).to_be_bytes());
    hasher.update(input_json);
    hasher.update([u8::from(success)]);
    hasher.update((output_json.len() as u64).to_be_bytes());
    hasher.update(output_json.as_slice());
    hasher.update((error.len() as u64).to_be_bytes());
    hasher.update(error.as_bytes());
    hasher.update(executed_at_unix_ms.to_be_bytes());
    let execution_sha256 = format!("{:x}", hasher.finalize());

    ToolExecutionOutcome {
        success,
        output_json,
        error,
        attestation: ToolAttestation {
            attestation_id: Ulid::new().to_string(),
            execution_sha256,
            executed_at_unix_ms,
            timed_out: false,
            executor: "browser_broker".to_owned(),
            sandbox_enforcement: "browser_service".to_owned(),
        },
    }
}

async fn execute_workspace_patch_tool(
    runtime_state: &Arc<GatewayRuntimeState>,
    principal: &str,
    channel: Option<&str>,
    session_id: &str,
    proposal_id: &str,
    input_json: &[u8],
) -> ToolExecutionOutcome {
    if input_json.len() > MAX_WORKSPACE_PATCH_TOOL_INPUT_BYTES {
        return workspace_patch_tool_execution_outcome(
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!(
                "palyra.fs.apply_patch input exceeds {MAX_WORKSPACE_PATCH_TOOL_INPUT_BYTES} bytes"
            ),
        );
    }

    let parsed = match serde_json::from_slice::<Value>(input_json) {
        Ok(Value::Object(map)) => map,
        Ok(_) => {
            return workspace_patch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                "palyra.fs.apply_patch requires JSON object input".to_owned(),
            );
        }
        Err(error) => {
            return workspace_patch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.fs.apply_patch invalid JSON input: {error}"),
            );
        }
    };

    let patch = match parsed.get("patch").and_then(Value::as_str) {
        Some(value) if !value.trim().is_empty() => value.to_owned(),
        _ => {
            return workspace_patch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                "palyra.fs.apply_patch requires non-empty string field 'patch'".to_owned(),
            );
        }
    };

    let dry_run = match parsed.get("dry_run") {
        Some(Value::Bool(value)) => *value,
        Some(_) => {
            return workspace_patch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                "palyra.fs.apply_patch dry_run must be a boolean".to_owned(),
            );
        }
        None => false,
    };

    let mut redaction_policy = WorkspacePatchRedactionPolicy::default();
    match parse_patch_string_array_field(
        &parsed,
        "redaction_patterns",
        MAX_PATCH_TOOL_REDACTION_PATTERNS,
        MAX_PATCH_TOOL_PATTERN_BYTES,
    ) {
        Ok(Some(patterns)) => {
            extend_patch_string_defaults(&mut redaction_policy.redaction_patterns, patterns);
        }
        Ok(None) => {}
        Err(message) => {
            return workspace_patch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                message,
            );
        }
    }
    match parse_patch_string_array_field(
        &parsed,
        "secret_file_markers",
        MAX_PATCH_TOOL_SECRET_FILE_MARKERS,
        MAX_PATCH_TOOL_MARKER_BYTES,
    ) {
        Ok(Some(markers)) => {
            extend_patch_string_defaults(&mut redaction_policy.secret_file_markers, markers);
        }
        Ok(None) => {}
        Err(message) => {
            return workspace_patch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                message,
            );
        }
    }

    let agent_outcome = match runtime_state
        .resolve_agent_for_context(AgentResolveRequest {
            principal: principal.to_owned(),
            channel: channel.map(str::to_owned),
            session_id: Some(session_id.to_owned()),
            preferred_agent_id: None,
            persist_session_binding: false,
        })
        .await
    {
        Ok(outcome) => outcome,
        Err(error) => {
            return workspace_patch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!(
                    "palyra.fs.apply_patch failed to resolve agent workspace: {}",
                    error.message()
                ),
            );
        }
    };
    let workspace_roots =
        agent_outcome.agent.workspace_roots.iter().map(PathBuf::from).collect::<Vec<_>>();
    let limits = WorkspacePatchLimits::default();
    let request = WorkspacePatchRequest {
        patch: patch.clone(),
        dry_run,
        redaction_policy: redaction_policy.clone(),
    };

    match apply_workspace_patch(workspace_roots.as_slice(), &request, &limits) {
        Ok(outcome) => match serde_json::to_vec(&outcome) {
            Ok(output_json) => workspace_patch_tool_execution_outcome(
                proposal_id,
                input_json,
                true,
                output_json,
                String::new(),
            ),
            Err(error) => workspace_patch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.fs.apply_patch failed to serialize output: {error}"),
            ),
        },
        Err(error) => {
            if let Some((line, column)) = error.parse_location() {
                warn!(
                    proposal_id = %proposal_id,
                    line,
                    column,
                    error = %error,
                    "workspace patch parse failed"
                );
            } else {
                warn!(
                    proposal_id = %proposal_id,
                    error = %error,
                    "workspace patch execution failed"
                );
            }
            let failure_payload = json!({
                "patch_sha256": compute_patch_sha256(patch.as_str()),
                "dry_run": dry_run,
                "files_touched": [],
                "rollback_performed": error.rollback_performed(),
                "redacted_preview": redact_patch_preview(
                    patch.as_str(),
                    &redaction_policy,
                    limits.max_preview_bytes
                ),
                "parse_error": error
                    .parse_location()
                    .map(|(line, column)| json!({ "line": line, "column": column })),
                "error": error.to_string(),
            });
            let output_json =
                serde_json::to_vec(&failure_payload).unwrap_or_else(|_| b"{}".to_vec());
            workspace_patch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                output_json,
                format!("palyra.fs.apply_patch failed: {error}"),
            )
        }
    }
}

fn extend_patch_string_defaults(defaults: &mut Vec<String>, additions: Vec<String>) {
    for addition in additions {
        if !defaults.iter().any(|existing| existing == &addition) {
            defaults.push(addition);
        }
    }
}

fn parse_patch_string_array_field(
    payload: &serde_json::Map<String, Value>,
    field_name: &str,
    max_items: usize,
    max_item_bytes: usize,
) -> Result<Option<Vec<String>>, String> {
    let Some(value) = payload.get(field_name) else {
        return Ok(None);
    };
    let Value::Array(values) = value else {
        return Err(format!("palyra.fs.apply_patch {field_name} must be an array of strings"));
    };
    if values.len() > max_items {
        return Err(format!("palyra.fs.apply_patch {field_name} exceeds limit ({max_items})"));
    }
    let mut parsed = Vec::with_capacity(values.len());
    for value in values {
        let Some(raw) = value.as_str() else {
            return Err(format!("palyra.fs.apply_patch {field_name} must be an array of strings"));
        };
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed.len() > max_item_bytes {
            return Err(format!(
                "palyra.fs.apply_patch {field_name} entries must be <= {max_item_bytes} bytes"
            ));
        }
        parsed.push(trimmed.to_owned());
    }
    Ok(Some(parsed))
}

fn workspace_patch_tool_execution_outcome(
    proposal_id: &str,
    input_json: &[u8],
    success: bool,
    output_json: Vec<u8>,
    error: String,
) -> ToolExecutionOutcome {
    let executed_at_unix_ms = current_unix_ms();
    let mut hasher = Sha256::new();
    hasher.update(b"palyra.fs.apply_patch.attestation.v1");
    hasher.update((proposal_id.len() as u64).to_be_bytes());
    hasher.update(proposal_id.as_bytes());
    hasher.update((input_json.len() as u64).to_be_bytes());
    hasher.update(input_json);
    hasher.update([u8::from(success)]);
    hasher.update((output_json.len() as u64).to_be_bytes());
    hasher.update(output_json.as_slice());
    hasher.update((error.len() as u64).to_be_bytes());
    hasher.update(error.as_bytes());
    hasher.update(executed_at_unix_ms.to_be_bytes());
    let execution_sha256 = format!("{:x}", hasher.finalize());

    ToolExecutionOutcome {
        success,
        output_json,
        error,
        attestation: ToolAttestation {
            attestation_id: Ulid::new().to_string(),
            execution_sha256,
            executed_at_unix_ms,
            timed_out: false,
            executor: "workspace_patch".to_owned(),
            sandbox_enforcement: "workspace_roots".to_owned(),
        },
    }
}

fn parse_memory_source_literal(raw: &str) -> Option<MemorySource> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "tape:user_message" | "tape_user_message" | "user_message" => {
            Some(MemorySource::TapeUserMessage)
        }
        "tape:tool_result" | "tape_tool_result" | "tool_result" => {
            Some(MemorySource::TapeToolResult)
        }
        "summary" => Some(MemorySource::Summary),
        "manual" => Some(MemorySource::Manual),
        "import" => Some(MemorySource::Import),
        _ => None,
    }
}

fn memory_tool_execution_outcome(
    proposal_id: &str,
    input_json: &[u8],
    success: bool,
    output_json: Vec<u8>,
    error: String,
) -> ToolExecutionOutcome {
    let executed_at_unix_ms = current_unix_ms();
    let mut hasher = Sha256::new();
    hasher.update(b"palyra.memory.search.attestation.v1");
    hasher.update((proposal_id.len() as u64).to_be_bytes());
    hasher.update(proposal_id.as_bytes());
    hasher.update((input_json.len() as u64).to_be_bytes());
    hasher.update(input_json);
    hasher.update([u8::from(success)]);
    hasher.update((output_json.len() as u64).to_be_bytes());
    hasher.update(output_json.as_slice());
    hasher.update((error.len() as u64).to_be_bytes());
    hasher.update(error.as_bytes());
    hasher.update(executed_at_unix_ms.to_be_bytes());
    let execution_sha256 = format!("{:x}", hasher.finalize());

    ToolExecutionOutcome {
        success,
        output_json,
        error,
        attestation: ToolAttestation {
            attestation_id: Ulid::new().to_string(),
            execution_sha256,
            executed_at_unix_ms,
            timed_out: false,
            executor: "memory_runtime".to_owned(),
            sandbox_enforcement: "none".to_owned(),
        },
    }
}

#[allow(clippy::result_large_err)]
async fn best_effort_mark_approval_error(
    runtime_state: &Arc<GatewayRuntimeState>,
    approval_id: &str,
    reason: String,
) {
    if let Err(error) = runtime_state
        .resolve_approval_record(ApprovalResolveRequest {
            approval_id: approval_id.to_owned(),
            decision: ApprovalDecision::Error,
            decision_scope: ApprovalDecisionScope::Once,
            decision_reason: reason,
            decision_scope_ttl_ms: None,
        })
        .await
    {
        warn!(approval_id, error = %error, "failed to mark approval record as error");
    }
}

async fn finalize_run_failure(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &Arc<GatewayRuntimeState>,
    run_state: &mut RunStateMachine,
    active_run_id: Option<&str>,
    tape_seq: &mut i64,
    reason: &str,
) {
    let Some(run_id) = active_run_id else {
        return;
    };
    if run_state.state().is_terminal() {
        return;
    }
    if run_state.transition(RunTransition::Fail).is_err() {
        return;
    }
    let _ = runtime_state
        .update_orchestrator_run_state(
            run_id.to_owned(),
            RunLifecycleState::Failed,
            Some(reason.to_owned()),
        )
        .await;
    let _ = send_status_with_tape(
        sender,
        runtime_state,
        run_id,
        tape_seq,
        common_v1::stream_status::StatusKind::Failed,
        reason,
    )
    .await;
}

fn status_event(
    run_id: String,
    kind: common_v1::stream_status::StatusKind,
    message: impl Into<String>,
) -> common_v1::RunStreamEvent {
    common_v1::RunStreamEvent {
        v: CANONICAL_PROTOCOL_MAJOR,
        run_id: Some(common_v1::CanonicalId { ulid: run_id }),
        body: Some(common_v1::run_stream_event::Body::Status(common_v1::StreamStatus {
            kind: kind as i32,
            message: message.into(),
        })),
    }
}

fn model_token_event(
    run_id: String,
    token: impl Into<String>,
    is_final: bool,
) -> common_v1::RunStreamEvent {
    common_v1::RunStreamEvent {
        v: CANONICAL_PROTOCOL_MAJOR,
        run_id: Some(common_v1::CanonicalId { ulid: run_id }),
        body: Some(common_v1::run_stream_event::Body::ModelToken(common_v1::ModelToken {
            token: token.into(),
            is_final,
        })),
    }
}

fn tool_proposal_event(
    run_id: String,
    proposal_id: impl Into<String>,
    tool_name: impl Into<String>,
    input_json: Vec<u8>,
    approval_required: bool,
) -> common_v1::RunStreamEvent {
    common_v1::RunStreamEvent {
        v: CANONICAL_PROTOCOL_MAJOR,
        run_id: Some(common_v1::CanonicalId { ulid: run_id }),
        body: Some(common_v1::run_stream_event::Body::ToolProposal(common_v1::ToolProposal {
            proposal_id: Some(common_v1::CanonicalId { ulid: proposal_id.into() }),
            tool_name: tool_name.into(),
            input_json,
            approval_required,
        })),
    }
}

#[allow(clippy::too_many_arguments)]
fn tool_approval_request_event(
    run_id: String,
    proposal_id: impl Into<String>,
    approval_id: impl Into<String>,
    tool_name: impl Into<String>,
    input_json: Vec<u8>,
    approval_required: bool,
    request_summary: impl Into<String>,
    prompt: &ApprovalPromptRecord,
) -> common_v1::RunStreamEvent {
    common_v1::RunStreamEvent {
        v: CANONICAL_PROTOCOL_MAJOR,
        run_id: Some(common_v1::CanonicalId { ulid: run_id }),
        body: Some(common_v1::run_stream_event::Body::ToolApprovalRequest(
            common_v1::ToolApprovalRequest {
                proposal_id: Some(common_v1::CanonicalId { ulid: proposal_id.into() }),
                tool_name: tool_name.into(),
                input_json,
                approval_required,
                approval_id: Some(common_v1::CanonicalId { ulid: approval_id.into() }),
                prompt: Some(approval_prompt_message(prompt)),
                request_summary: request_summary.into(),
            },
        )),
    }
}

fn tool_approval_response_event(
    run_id: String,
    proposal_id: impl Into<String>,
    approval_id: impl Into<String>,
    approved: bool,
    reason: impl Into<String>,
    decision_scope: ApprovalDecisionScope,
    decision_scope_ttl_ms: Option<i64>,
) -> common_v1::RunStreamEvent {
    common_v1::RunStreamEvent {
        v: CANONICAL_PROTOCOL_MAJOR,
        run_id: Some(common_v1::CanonicalId { ulid: run_id }),
        body: Some(common_v1::run_stream_event::Body::ToolApprovalResponse(
            common_v1::ToolApprovalResponse {
                proposal_id: Some(common_v1::CanonicalId { ulid: proposal_id.into() }),
                approved,
                reason: reason.into(),
                approval_id: Some(common_v1::CanonicalId { ulid: approval_id.into() }),
                decision_scope: approval_scope_to_proto(decision_scope),
                decision_scope_ttl_ms: decision_scope_ttl_ms.unwrap_or_default(),
            },
        )),
    }
}

fn tool_decision_event(
    run_id: String,
    proposal_id: impl Into<String>,
    allowed: bool,
    reason: impl Into<String>,
    approval_required: bool,
    policy_enforced: bool,
) -> common_v1::RunStreamEvent {
    let kind = if allowed {
        common_v1::tool_decision::DecisionKind::Allow
    } else {
        common_v1::tool_decision::DecisionKind::Deny
    };
    common_v1::RunStreamEvent {
        v: CANONICAL_PROTOCOL_MAJOR,
        run_id: Some(common_v1::CanonicalId { ulid: run_id }),
        body: Some(common_v1::run_stream_event::Body::ToolDecision(common_v1::ToolDecision {
            proposal_id: Some(common_v1::CanonicalId { ulid: proposal_id.into() }),
            kind: kind as i32,
            reason: reason.into(),
            approval_required,
            policy_enforced,
        })),
    }
}

fn tool_result_event(
    run_id: String,
    proposal_id: impl Into<String>,
    success: bool,
    output_json: Vec<u8>,
    error: impl Into<String>,
) -> common_v1::RunStreamEvent {
    common_v1::RunStreamEvent {
        v: CANONICAL_PROTOCOL_MAJOR,
        run_id: Some(common_v1::CanonicalId { ulid: run_id }),
        body: Some(common_v1::run_stream_event::Body::ToolResult(common_v1::ToolResult {
            proposal_id: Some(common_v1::CanonicalId { ulid: proposal_id.into() }),
            success,
            output_json,
            error: error.into(),
        })),
    }
}

fn tool_attestation_event(
    run_id: String,
    proposal_id: impl Into<String>,
    attestation_id: impl Into<String>,
    execution_sha256: impl Into<String>,
    executed_at_unix_ms: i64,
    timed_out: bool,
    executor: impl Into<String>,
) -> common_v1::RunStreamEvent {
    common_v1::RunStreamEvent {
        v: CANONICAL_PROTOCOL_MAJOR,
        run_id: Some(common_v1::CanonicalId { ulid: run_id }),
        body: Some(common_v1::run_stream_event::Body::ToolAttestation(
            common_v1::ToolAttestation {
                proposal_id: Some(common_v1::CanonicalId { ulid: proposal_id.into() }),
                attestation_id: Some(common_v1::CanonicalId { ulid: attestation_id.into() }),
                execution_sha256: execution_sha256.into(),
                executed_at_unix_ms,
                timed_out,
                executor: executor.into(),
            },
        )),
    }
}

#[allow(clippy::result_large_err)]
async fn send_status_with_tape(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    kind: common_v1::stream_status::StatusKind,
    message: &str,
) -> Result<(), Status> {
    let event = status_event(run_id.to_owned(), kind, message.to_owned());
    sender
        .send(Ok(event))
        .await
        .map_err(|_| Status::cancelled("run stream response channel closed"))?;
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: "status".to_owned(),
            payload_json: status_tape_payload(kind, message),
        })
        .await?;
    *tape_seq += 1;
    Ok(())
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn send_model_token_with_tape(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    token_tape_events: &mut usize,
    compaction_emitted: &mut bool,
    token: &str,
    is_final: bool,
) -> Result<(), Status> {
    let event = model_token_event(run_id.to_owned(), token.to_owned(), is_final);
    sender
        .send(Ok(event))
        .await
        .map_err(|_| Status::cancelled("run stream response channel closed"))?;
    if !is_final && *token_tape_events >= MAX_MODEL_TOKEN_TAPE_EVENTS_PER_RUN {
        if !*compaction_emitted {
            compact_model_token_tape_stub(runtime_state, run_id, tape_seq).await?;
            *compaction_emitted = true;
        }
        return Ok(());
    }
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: "model_token".to_owned(),
            payload_json: model_token_tape_payload(token, is_final),
        })
        .await?;
    *tape_seq += 1;
    *token_tape_events += 1;
    Ok(())
}

#[allow(clippy::result_large_err)]
async fn compact_model_token_tape_stub(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
) -> Result<(), Status> {
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: "model_token_compaction".to_owned(),
            payload_json: model_token_compaction_tape_payload(MAX_MODEL_TOKEN_TAPE_EVENTS_PER_RUN),
        })
        .await?;
    *tape_seq += 1;
    Ok(())
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn send_tool_proposal_with_tape(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    approval_required: bool,
) -> Result<(), Status> {
    let event = tool_proposal_event(
        run_id.to_owned(),
        proposal_id.to_owned(),
        tool_name.to_owned(),
        input_json.to_vec(),
        approval_required,
    );
    sender
        .send(Ok(event))
        .await
        .map_err(|_| Status::cancelled("run stream response channel closed"))?;
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: "tool_proposal".to_owned(),
            payload_json: tool_proposal_tape_payload(
                proposal_id,
                tool_name,
                input_json,
                approval_required,
            ),
        })
        .await?;
    *tape_seq += 1;
    Ok(())
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn send_tool_approval_request_with_tape(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    proposal_id: &str,
    approval_id: &str,
    tool_name: &str,
    input_json: &[u8],
    approval_required: bool,
    request_summary: &str,
    prompt: &ApprovalPromptRecord,
) -> Result<(), Status> {
    let event = tool_approval_request_event(
        run_id.to_owned(),
        proposal_id.to_owned(),
        approval_id.to_owned(),
        tool_name.to_owned(),
        input_json.to_vec(),
        approval_required,
        request_summary.to_owned(),
        prompt,
    );
    sender
        .send(Ok(event))
        .await
        .map_err(|_| Status::cancelled("run stream response channel closed"))?;
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: "tool_approval_request".to_owned(),
            payload_json: tool_approval_request_tape_payload(
                proposal_id,
                approval_id,
                tool_name,
                input_json,
                approval_required,
                request_summary,
                prompt,
            ),
        })
        .await?;
    *tape_seq += 1;
    Ok(())
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn send_tool_approval_response_with_tape(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    proposal_id: &str,
    approval_id: &str,
    approved: bool,
    reason: &str,
    decision_scope: ApprovalDecisionScope,
    decision_scope_ttl_ms: Option<i64>,
) -> Result<(), Status> {
    let event = tool_approval_response_event(
        run_id.to_owned(),
        proposal_id.to_owned(),
        approval_id.to_owned(),
        approved,
        reason.to_owned(),
        decision_scope,
        decision_scope_ttl_ms,
    );
    sender
        .send(Ok(event))
        .await
        .map_err(|_| Status::cancelled("run stream response channel closed"))?;
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: "tool_approval_response".to_owned(),
            payload_json: tool_approval_response_tape_payload(
                proposal_id,
                approval_id,
                approved,
                reason,
                decision_scope,
                decision_scope_ttl_ms,
            ),
        })
        .await?;
    *tape_seq += 1;
    Ok(())
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn send_tool_decision_with_tape(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    proposal_id: &str,
    allowed: bool,
    reason: &str,
    approval_required: bool,
    policy_enforced: bool,
) -> Result<(), Status> {
    let event = tool_decision_event(
        run_id.to_owned(),
        proposal_id.to_owned(),
        allowed,
        reason.to_owned(),
        approval_required,
        policy_enforced,
    );
    sender
        .send(Ok(event))
        .await
        .map_err(|_| Status::cancelled("run stream response channel closed"))?;
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: "tool_decision".to_owned(),
            payload_json: tool_decision_tape_payload(
                proposal_id,
                allowed,
                reason,
                approval_required,
                policy_enforced,
            ),
        })
        .await?;
    *tape_seq += 1;
    Ok(())
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn send_tool_result_with_tape(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    proposal_id: &str,
    success: bool,
    output_json: &[u8],
    error: &str,
) -> Result<(), Status> {
    let event = tool_result_event(
        run_id.to_owned(),
        proposal_id.to_owned(),
        success,
        output_json.to_vec(),
        error.to_owned(),
    );
    sender
        .send(Ok(event))
        .await
        .map_err(|_| Status::cancelled("run stream response channel closed"))?;
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: "tool_result".to_owned(),
            payload_json: tool_result_tape_payload(proposal_id, success, output_json, error),
        })
        .await?;
    *tape_seq += 1;
    Ok(())
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn send_tool_attestation_with_tape(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    proposal_id: &str,
    attestation_id: &str,
    execution_sha256: &str,
    executed_at_unix_ms: i64,
    timed_out: bool,
    executor: &str,
    sandbox_enforcement: &str,
) -> Result<(), Status> {
    let event = tool_attestation_event(
        run_id.to_owned(),
        proposal_id.to_owned(),
        attestation_id.to_owned(),
        execution_sha256.to_owned(),
        executed_at_unix_ms,
        timed_out,
        executor.to_owned(),
    );
    sender
        .send(Ok(event))
        .await
        .map_err(|_| Status::cancelled("run stream response channel closed"))?;
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: "tool_attestation".to_owned(),
            payload_json: tool_attestation_tape_payload(
                proposal_id,
                attestation_id,
                execution_sha256,
                executed_at_unix_ms,
                timed_out,
                executor,
                sandbox_enforcement,
            ),
        })
        .await?;
    *tape_seq += 1;
    Ok(())
}

fn status_tape_payload(kind: common_v1::stream_status::StatusKind, message: &str) -> String {
    json!({
        "kind": status_kind_name(kind),
        "message": message,
    })
    .to_string()
}

fn model_token_tape_payload(token: &str, is_final: bool) -> String {
    json!({
        "is_final": is_final,
        "token": token,
    })
    .to_string()
}

fn model_token_compaction_tape_payload(max_model_token_events: usize) -> String {
    json!({
        "kind": "token_cap_reached",
        "max_model_token_tape_events": max_model_token_events,
        "compaction_hook": "stub",
    })
    .to_string()
}

fn tool_proposal_tape_payload(
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    approval_required: bool,
) -> String {
    let normalized_input = serde_json::from_slice::<Value>(input_json)
        .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(input_json).to_string() }));
    json!({
        "proposal_id": proposal_id,
        "tool_name": tool_name,
        "input_json": normalized_input,
        "approval_required": approval_required,
    })
    .to_string()
}

fn tool_approval_request_tape_payload(
    proposal_id: &str,
    approval_id: &str,
    tool_name: &str,
    input_json: &[u8],
    approval_required: bool,
    request_summary: &str,
    prompt: &ApprovalPromptRecord,
) -> String {
    let normalized_input = serde_json::from_slice::<Value>(input_json)
        .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(input_json).to_string() }));
    let prompt_details_json = serde_json::from_str::<Value>(prompt.details_json.as_str())
        .unwrap_or_else(|_| json!({ "raw": prompt.details_json }));
    json!({
        "proposal_id": proposal_id,
        "approval_id": approval_id,
        "tool_name": tool_name,
        "input_json": normalized_input,
        "approval_required": approval_required,
        "request_summary": request_summary,
        "prompt": {
            "title": prompt.title,
            "risk_level": prompt.risk_level.as_str(),
            "subject_id": prompt.subject_id,
            "summary": prompt.summary,
            "timeout_seconds": prompt.timeout_seconds,
            "policy_explanation": prompt.policy_explanation,
            "options": prompt.options.iter().map(|option| json!({
                "option_id": option.option_id,
                "label": option.label,
                "description": option.description,
                "default_selected": option.default_selected,
                "decision_scope": option.decision_scope.as_str(),
                "timebox_ttl_ms": option.timebox_ttl_ms,
            })).collect::<Vec<_>>(),
            "details_json": prompt_details_json,
        },
    })
    .to_string()
}

fn tool_approval_response_tape_payload(
    proposal_id: &str,
    approval_id: &str,
    approved: bool,
    reason: &str,
    decision_scope: ApprovalDecisionScope,
    decision_scope_ttl_ms: Option<i64>,
) -> String {
    json!({
        "proposal_id": proposal_id,
        "approval_id": approval_id,
        "approved": approved,
        "reason": reason,
        "decision_scope": decision_scope.as_str(),
        "decision_scope_ttl_ms": decision_scope_ttl_ms,
    })
    .to_string()
}

fn tool_decision_tape_payload(
    proposal_id: &str,
    allowed: bool,
    reason: &str,
    approval_required: bool,
    policy_enforced: bool,
) -> String {
    json!({
        "proposal_id": proposal_id,
        "kind": if allowed { "allow" } else { "deny" },
        "reason": reason,
        "approval_required": approval_required,
        "policy_enforced": policy_enforced,
    })
    .to_string()
}

fn default_approval_prompt_options() -> Vec<ApprovalPromptOption> {
    vec![
        ApprovalPromptOption {
            option_id: "allow_once".to_owned(),
            label: "Allow once".to_owned(),
            description: "Approve this single action".to_owned(),
            default_selected: true,
            decision_scope: ApprovalDecisionScope::Once,
            timebox_ttl_ms: None,
        },
        ApprovalPromptOption {
            option_id: "allow_session".to_owned(),
            label: "Allow for session".to_owned(),
            description: "Remember approval for this session".to_owned(),
            default_selected: false,
            decision_scope: ApprovalDecisionScope::Session,
            timebox_ttl_ms: None,
        },
        ApprovalPromptOption {
            option_id: "deny_once".to_owned(),
            label: "Deny".to_owned(),
            description: "Reject this action".to_owned(),
            default_selected: false,
            decision_scope: ApprovalDecisionScope::Once,
            timebox_ttl_ms: None,
        },
    ]
}

fn truncate_with_ellipsis(input: String, max_bytes: usize) -> String {
    if input.len() <= max_bytes {
        return input;
    }
    let cutoff = max_bytes.saturating_sub(3);
    let mut output = String::new();
    for character in input.chars() {
        if output.len().saturating_add(character.len_utf8()) > cutoff {
            break;
        }
        output.push(character);
    }
    output.push_str("...");
    output
}

fn build_tool_request_summary(
    tool_name: &str,
    skill_context: Option<&ToolSkillContext>,
    input_json: &[u8],
) -> String {
    let normalized_input = serde_json::from_slice::<Value>(input_json)
        .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(input_json).to_string() }));
    truncate_with_ellipsis(
        json!({
            "tool_name": tool_name,
            "skill_id": skill_context.map(|context| context.skill_id.as_str()),
            "skill_version": skill_context.and_then(|context| context.version.as_deref()),
            "input_json": normalized_input,
        })
        .to_string(),
        APPROVAL_REQUEST_SUMMARY_MAX_BYTES,
    )
}

fn build_tool_policy_snapshot(config: &ToolCallConfig, tool_name: &str) -> ApprovalPolicySnapshot {
    let snapshot = tool_policy_snapshot(config);
    let policy_snapshot_json = serde_json::to_vec(&snapshot).unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(policy_snapshot_json.as_slice());
    let policy_hash = format!("{:x}", hasher.finalize());
    ApprovalPolicySnapshot {
        policy_id: APPROVAL_POLICY_ID.to_owned(),
        policy_hash,
        evaluation_summary: format!(
            "action=tool.execute resource=tool:{tool_name} approval_required=true deny_by_default=true"
        ),
    }
}

#[allow(clippy::result_large_err)]
fn parse_tool_skill_context(
    tool_name: &str,
    input_json: &[u8],
) -> Result<Option<ToolSkillContext>, Status> {
    if tool_name != "palyra.plugin.run" {
        return Ok(None);
    }
    let payload = serde_json::from_slice::<Value>(input_json)
        .map_err(|error| Status::invalid_argument(format!("invalid tool input JSON: {error}")))?;
    let object = payload
        .as_object()
        .ok_or_else(|| Status::invalid_argument("tool input must be a JSON object"))?;
    let skill_id_value = object.get("skill_id").ok_or_else(|| {
        Status::invalid_argument("palyra.plugin.run requires non-empty skill_id for security gate")
    })?;
    let skill_id = skill_id_value
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            Status::invalid_argument("palyra.plugin.run skill_id must be a non-empty string")
        })?
        .to_ascii_lowercase();
    let version = object
        .get("skill_version")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    Ok(Some(ToolSkillContext { skill_id, version }))
}

#[allow(clippy::result_large_err)]
async fn evaluate_skill_execution_gate(
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    context: &ToolSkillContext,
) -> Result<Option<ToolDecision>, Status> {
    let status_record = if let Some(version) = context.version.as_deref() {
        runtime_state.skill_status(context.skill_id.clone(), version.to_owned()).await?
    } else {
        runtime_state.latest_skill_status(context.skill_id.clone()).await?
    };
    let Some(status_record) = status_record else {
        let version = context.version.as_deref().unwrap_or("latest");
        return Ok(Some(ToolDecision {
            allowed: false,
            reason: format!(
                "{SKILL_EXECUTION_DENY_REASON_PREFIX}: skill={} version={} status=missing",
                context.skill_id, version
            ),
            approval_required: false,
            policy_enforced: true,
        }));
    };
    if !matches!(status_record.status, SkillExecutionStatus::Active) {
        return Ok(Some(ToolDecision {
            allowed: false,
            reason: format!(
                "{SKILL_EXECUTION_DENY_REASON_PREFIX}: skill={} version={} status={} reason={}",
                status_record.skill_id,
                status_record.version,
                status_record.status.as_str(),
                status_record.reason.unwrap_or_else(|| "none".to_owned())
            ),
            approval_required: false,
            policy_enforced: true,
        }));
    }

    let evaluation = evaluate_with_context(
        &PolicyRequest {
            principal: request_context.principal.clone(),
            action: "skill.execute".to_owned(),
            resource: format!("skill:{}", context.skill_id),
        },
        &PolicyRequestContext {
            device_id: Some(request_context.device_id.clone()),
            channel: request_context.channel.clone(),
            skill_id: Some(context.skill_id.clone()),
            ..PolicyRequestContext::default()
        },
        &PolicyEvaluationConfig {
            allowlisted_skills: vec![status_record.skill_id.clone()],
            ..PolicyEvaluationConfig::default()
        },
    )
    .map_err(|error| Status::internal(format!("failed to evaluate skill policy: {error}")))?;

    match evaluation.decision {
        PolicyDecision::Allow => Ok(None),
        PolicyDecision::DenyByDefault { reason } => Ok(Some(ToolDecision {
            allowed: false,
            reason: format!(
                "{SKILL_EXECUTION_DENY_REASON_PREFIX}: skill={} reason={reason}",
                context.skill_id
            ),
            approval_required: false,
            policy_enforced: true,
        })),
    }
}

fn build_pending_tool_approval(
    tool_name: &str,
    skill_context: Option<&ToolSkillContext>,
    input_json: &[u8],
    config: &ToolCallConfig,
) -> PendingToolApproval {
    let subject_id = build_tool_approval_subject_id(tool_name, skill_context);
    let request_summary = build_tool_request_summary(tool_name, skill_context, input_json);
    let policy_snapshot = build_tool_policy_snapshot(config, tool_name);
    let details = serde_json::from_slice::<Value>(input_json)
        .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(input_json).to_string() }));
    let prompt = ApprovalPromptRecord {
        title: format!("Approve {}", tool_name),
        risk_level: approval_risk_for_tool(tool_name, input_json, config),
        subject_id: subject_id.clone(),
        summary: format!("Tool `{tool_name}` requested explicit approval"),
        options: default_approval_prompt_options(),
        timeout_seconds: APPROVAL_PROMPT_TIMEOUT_SECONDS,
        details_json: json!({
            "tool_name": tool_name,
            "subject_id": subject_id,
            "skill_id": skill_context.map(|context| context.skill_id.as_str()),
            "skill_version": skill_context.and_then(|context| context.version.as_deref()),
            "input_json": details,
        })
        .to_string(),
        policy_explanation: "Sensitive tool actions are deny-by-default until explicitly approved"
            .to_owned(),
    };
    PendingToolApproval {
        approval_id: Ulid::new().to_string(),
        request_summary,
        policy_snapshot,
        prompt,
    }
}

fn approval_risk_for_tool(
    tool_name: &str,
    input_json: &[u8],
    config: &ToolCallConfig,
) -> ApprovalRiskLevel {
    if tool_name != PROCESS_RUNNER_TOOL_NAME {
        return ApprovalRiskLevel::High;
    }
    if !matches!(config.process_runner.tier, crate::sandbox_runner::SandboxProcessRunnerTier::C) {
        return ApprovalRiskLevel::High;
    }
    if process_runner_command_is_read_only(input_json) {
        ApprovalRiskLevel::Medium
    } else {
        ApprovalRiskLevel::High
    }
}

fn process_runner_command_is_read_only(input_json: &[u8]) -> bool {
    const READ_ONLY_COMMANDS: &[&str] = &[
        "cat", "find", "grep", "head", "id", "ls", "pwd", "rg", "stat", "tail", "uname", "wc",
        "whoami",
    ];

    let parsed = match serde_json::from_slice::<Value>(input_json) {
        Ok(value) => value,
        Err(_) => return false,
    };
    let Some(payload) = parsed.as_object() else {
        return false;
    };
    let Some(command) = payload.get("command").and_then(Value::as_str).map(str::trim) else {
        return false;
    };
    READ_ONLY_COMMANDS.iter().any(|candidate| candidate.eq_ignore_ascii_case(command))
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn record_approval_requested_journal_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    session_id: &str,
    run_id: &str,
    proposal_id: &str,
    approval_id: &str,
    tool_name: &str,
    subject_id: &str,
    request_summary: &str,
    policy_snapshot: &ApprovalPolicySnapshot,
    prompt: &ApprovalPromptRecord,
) -> Result<(), Status> {
    runtime_state
        .record_journal_event(JournalAppendRequest {
            event_id: Ulid::new().to_string(),
            session_id: session_id.to_owned(),
            run_id: run_id.to_owned(),
            kind: common_v1::journal_event::EventKind::ToolProposed as i32,
            actor: common_v1::journal_event::EventActor::System as i32,
            timestamp_unix_ms: current_unix_ms(),
            payload_json: approval_requested_journal_payload(
                proposal_id,
                approval_id,
                tool_name,
                subject_id,
                request_summary,
                policy_snapshot,
                prompt,
            ),
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
        })
        .await
        .map(|_| ())
}

fn approval_requested_journal_payload(
    proposal_id: &str,
    approval_id: &str,
    tool_name: &str,
    subject_id: &str,
    request_summary: &str,
    policy_snapshot: &ApprovalPolicySnapshot,
    prompt: &ApprovalPromptRecord,
) -> Vec<u8> {
    let prompt_details_json = serde_json::from_str::<Value>(prompt.details_json.as_str())
        .unwrap_or_else(|_| json!({ "raw": prompt.details_json }));
    json!({
        "event": "approval.requested",
        "proposal_id": proposal_id,
        "approval_id": approval_id,
        "subject_type": "tool",
        "subject_id": subject_id,
        "tool_name": tool_name,
        "request_summary": request_summary,
        "policy_snapshot": policy_snapshot,
        "prompt": {
            "title": prompt.title,
            "risk_level": prompt.risk_level.as_str(),
            "subject_id": prompt.subject_id,
            "summary": prompt.summary,
            "timeout_seconds": prompt.timeout_seconds,
            "policy_explanation": prompt.policy_explanation,
            "options": prompt.options.iter().map(|option| json!({
                "option_id": option.option_id,
                "label": option.label,
                "description": option.description,
                "default_selected": option.default_selected,
                "decision_scope": option.decision_scope.as_str(),
                "timebox_ttl_ms": option.timebox_ttl_ms,
            })).collect::<Vec<_>>(),
            "details_json": prompt_details_json,
        },
    })
    .to_string()
    .into_bytes()
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn record_approval_resolved_journal_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    session_id: &str,
    run_id: &str,
    proposal_id: &str,
    approval_id: &str,
    decision: ApprovalDecision,
    decision_scope: ApprovalDecisionScope,
    decision_scope_ttl_ms: Option<i64>,
    reason: &str,
) -> Result<(), Status> {
    runtime_state
        .record_journal_event(JournalAppendRequest {
            event_id: Ulid::new().to_string(),
            session_id: session_id.to_owned(),
            run_id: run_id.to_owned(),
            kind: common_v1::journal_event::EventKind::ToolExecuted as i32,
            actor: common_v1::journal_event::EventActor::System as i32,
            timestamp_unix_ms: current_unix_ms(),
            payload_json: approval_resolved_journal_payload(
                proposal_id,
                approval_id,
                decision,
                decision_scope,
                decision_scope_ttl_ms,
                reason,
            ),
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
        })
        .await
        .map(|_| ())
}

fn approval_resolved_journal_payload(
    proposal_id: &str,
    approval_id: &str,
    decision: ApprovalDecision,
    decision_scope: ApprovalDecisionScope,
    decision_scope_ttl_ms: Option<i64>,
    reason: &str,
) -> Vec<u8> {
    json!({
        "event": "approval.resolved",
        "proposal_id": proposal_id,
        "approval_id": approval_id,
        "decision": decision.as_str(),
        "decision_scope": decision_scope.as_str(),
        "decision_scope_ttl_ms": decision_scope_ttl_ms,
        "reason": reason,
    })
    .to_string()
    .into_bytes()
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn record_policy_decision_journal_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    session_id: &str,
    run_id: &str,
    proposal_id: &str,
    tool_name: &str,
    allowed: bool,
    reason: &str,
    approval_required: bool,
    policy_enforced: bool,
) -> Result<(), Status> {
    runtime_state
        .record_journal_event(JournalAppendRequest {
            event_id: Ulid::new().to_string(),
            session_id: session_id.to_owned(),
            run_id: run_id.to_owned(),
            kind: common_v1::journal_event::EventKind::ToolProposed as i32,
            actor: common_v1::journal_event::EventActor::System as i32,
            timestamp_unix_ms: current_unix_ms(),
            payload_json: tool_decision_journal_payload(
                proposal_id,
                tool_name,
                allowed,
                reason,
                approval_required,
                policy_enforced,
            ),
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
        })
        .await
        .map(|_| ())
}

fn tool_decision_journal_payload(
    proposal_id: &str,
    tool_name: &str,
    allowed: bool,
    reason: &str,
    approval_required: bool,
    policy_enforced: bool,
) -> Vec<u8> {
    json!({
        "event": "policy_decision",
        "proposal_id": proposal_id,
        "tool_name": tool_name,
        "kind": if allowed { "allow" } else { "deny" },
        "reason": reason,
        "approval_required": approval_required,
        "policy_enforced": policy_enforced,
    })
    .to_string()
    .into_bytes()
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn record_skill_execution_denied_journal_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    session_id: &str,
    run_id: &str,
    proposal_id: &str,
    tool_name: &str,
    skill_context: &ToolSkillContext,
    reason: &str,
) -> Result<(), Status> {
    runtime_state
        .record_journal_event(JournalAppendRequest {
            event_id: Ulid::new().to_string(),
            session_id: session_id.to_owned(),
            run_id: run_id.to_owned(),
            kind: common_v1::journal_event::EventKind::ToolProposed as i32,
            actor: common_v1::journal_event::EventActor::System as i32,
            timestamp_unix_ms: current_unix_ms(),
            payload_json: json!({
                "event": "skill.execution_denied",
                "proposal_id": proposal_id,
                "tool_name": tool_name,
                "skill_id": skill_context.skill_id,
                "skill_version": skill_context.version,
                "reason": reason,
            })
            .to_string()
            .into_bytes(),
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
        })
        .await
        .map(|_| ())
}

#[allow(clippy::result_large_err)]
async fn record_message_router_journal_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    session_id: &str,
    run_id: &str,
    event_name: &str,
    actor: i32,
    payload: Value,
) -> Result<(), Status> {
    let mut payload = payload;
    if let Some(map) = payload.as_object_mut() {
        map.entry("event".to_owned()).or_insert(Value::String(event_name.to_owned()));
    }
    runtime_state
        .record_journal_event(JournalAppendRequest {
            event_id: Ulid::new().to_string(),
            session_id: session_id.to_owned(),
            run_id: run_id.to_owned(),
            kind: common_v1::journal_event::EventKind::MessageReceived as i32,
            actor,
            timestamp_unix_ms: current_unix_ms(),
            payload_json: payload.to_string().into_bytes(),
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
        })
        .await
        .map(|_| ())
}

#[allow(clippy::result_large_err)]
async fn record_vault_journal_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    event: &str,
    action: &str,
    scope: &VaultScope,
    key: Option<&str>,
    value_size: Option<usize>,
) -> Result<(), Status> {
    runtime_state
        .record_journal_event(JournalAppendRequest {
            event_id: Ulid::new().to_string(),
            session_id: Ulid::new().to_string(),
            run_id: Ulid::new().to_string(),
            kind: common_v1::journal_event::EventKind::ToolExecuted as i32,
            actor: common_v1::journal_event::EventActor::System as i32,
            timestamp_unix_ms: current_unix_ms(),
            payload_json: json!({
                "event": event,
                "action": action,
                "scope": scope.to_string(),
                "key": key.unwrap_or_default(),
                "value_bytes": value_size,
                "vault_backend": runtime_state.vault.backend_kind().as_str(),
            })
            .to_string()
            .into_bytes(),
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
        })
        .await?;
    runtime_state.counters.vault_access_audit_events.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

#[allow(clippy::result_large_err)]
async fn record_agent_journal_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    payload: Value,
) -> Result<(), Status> {
    runtime_state
        .record_journal_event(JournalAppendRequest {
            event_id: Ulid::new().to_string(),
            session_id: Ulid::new().to_string(),
            run_id: Ulid::new().to_string(),
            kind: common_v1::journal_event::EventKind::ToolExecuted as i32,
            actor: common_v1::journal_event::EventActor::System as i32,
            timestamp_unix_ms: current_unix_ms(),
            payload_json: payload.to_string().into_bytes(),
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
        })
        .await
        .map(|_| ())
}

#[allow(clippy::result_large_err)]
async fn record_auth_profile_saved_journal_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    profile: &AuthProfileRecord,
) -> Result<(), Status> {
    runtime_state
        .record_journal_event(JournalAppendRequest {
            event_id: Ulid::new().to_string(),
            session_id: Ulid::new().to_string(),
            run_id: Ulid::new().to_string(),
            kind: common_v1::journal_event::EventKind::ToolExecuted as i32,
            actor: common_v1::journal_event::EventActor::System as i32,
            timestamp_unix_ms: current_unix_ms(),
            payload_json: json!({
                "event": "auth.profile.saved",
                "profile_id": profile.profile_id,
                "provider": profile.provider.label(),
                "scope": profile.scope.scope_key(),
                "credential_type": match profile.credential.credential_type() {
                    AuthCredentialType::ApiKey => "api_key",
                    AuthCredentialType::Oauth => "oauth",
                },
            })
            .to_string()
            .into_bytes(),
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
        })
        .await
        .map(|_| ())
}

#[allow(clippy::result_large_err)]
async fn record_auth_refresh_journal_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    outcome: &OAuthRefreshOutcome,
) -> Result<(), Status> {
    if !outcome.kind.attempted() {
        return Ok(());
    }
    let event_name =
        if outcome.kind.success() { "auth.token.refreshed" } else { "auth.refresh.failed" };
    let redacted_reason = crate::model_provider::sanitize_remote_error(outcome.reason.as_str());
    runtime_state
        .record_journal_event(JournalAppendRequest {
            event_id: Ulid::new().to_string(),
            session_id: Ulid::new().to_string(),
            run_id: Ulid::new().to_string(),
            kind: common_v1::journal_event::EventKind::ToolExecuted as i32,
            actor: common_v1::journal_event::EventActor::System as i32,
            timestamp_unix_ms: current_unix_ms(),
            payload_json: json!({
                "event": event_name,
                "profile_id": outcome.profile_id,
                "provider": outcome.provider,
                "reason": redacted_reason,
                "next_allowed_refresh_unix_ms": outcome.next_allowed_refresh_unix_ms,
                "expires_at_unix_ms": outcome.expires_at_unix_ms,
            })
            .to_string()
            .into_bytes(),
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
        })
        .await
        .map(|_| ())
}

fn workspace_patch_metrics_from_output(output_json: &[u8]) -> (usize, bool) {
    let parsed = serde_json::from_slice::<Value>(output_json).ok();
    let Some(Value::Object(payload)) = parsed else {
        return (0, false);
    };
    let files_touched =
        payload.get("files_touched").and_then(Value::as_array).map_or(0, std::vec::Vec::len);
    let rollback_performed =
        payload.get("rollback_performed").and_then(Value::as_bool).unwrap_or(false);
    (files_touched, rollback_performed)
}

fn record_process_runner_execution_metrics(
    counters: &RuntimeCounters,
    decision_allowed: bool,
    outcome: &ToolExecutionOutcome,
) {
    if !decision_allowed {
        return;
    }

    counters.sandbox_launches.fetch_add(1, Ordering::Relaxed);
    match outcome.attestation.executor.as_str() {
        "sandbox_tier_b" => {
            counters.sandbox_backend_selected_tier_b.fetch_add(1, Ordering::Relaxed);
        }
        "sandbox_tier_c_linux_bubblewrap" => {
            counters
                .sandbox_backend_selected_tier_c_linux_bubblewrap
                .fetch_add(1, Ordering::Relaxed);
        }
        "sandbox_tier_c_macos_sandbox_exec" => {
            counters
                .sandbox_backend_selected_tier_c_macos_sandbox_exec
                .fetch_add(1, Ordering::Relaxed);
        }
        "sandbox_tier_c_windows_job_object" => {
            counters
                .sandbox_backend_selected_tier_c_windows_job_object
                .fetch_add(1, Ordering::Relaxed);
        }
        _ => {}
    }

    if !outcome.success {
        if outcome.error.contains("sandbox denied") {
            counters.sandbox_policy_denies.fetch_add(1, Ordering::Relaxed);
        }
        match classify_sandbox_escape_attempt(outcome.error.as_str()) {
            Some(SandboxEscapeAttemptType::Workspace) => {
                counters.sandbox_escape_attempts_blocked_workspace.fetch_add(1, Ordering::Relaxed);
            }
            Some(SandboxEscapeAttemptType::Egress) => {
                counters.sandbox_escape_attempts_blocked_egress.fetch_add(1, Ordering::Relaxed);
            }
            Some(SandboxEscapeAttemptType::Executable) => {
                counters.sandbox_escape_attempts_blocked_executable.fetch_add(1, Ordering::Relaxed);
            }
            None => {}
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SandboxEscapeAttemptType {
    Workspace,
    Egress,
    Executable,
}

fn classify_sandbox_escape_attempt(error: &str) -> Option<SandboxEscapeAttemptType> {
    let normalized = error.to_ascii_lowercase();
    if normalized.contains("path traversal")
        || normalized.contains("workspace scope")
        || normalized.contains("escapes workspace")
        || normalized.contains("absolute path")
    {
        return Some(SandboxEscapeAttemptType::Workspace);
    }
    if normalized.contains("egress")
        || normalized.contains("host-level egress")
        || normalized.contains("network isolation")
    {
        return Some(SandboxEscapeAttemptType::Egress);
    }
    if normalized.contains("not allowlisted")
        || normalized.contains("allow_interpreters")
        || normalized.contains("bare executable")
        || normalized.contains("shell-eval")
    {
        return Some(SandboxEscapeAttemptType::Executable);
    }
    None
}

fn tool_result_tape_payload(
    proposal_id: &str,
    success: bool,
    output_json: &[u8],
    error: &str,
) -> String {
    let normalized_output = serde_json::from_slice::<Value>(output_json)
        .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(output_json).to_string() }));
    json!({
        "proposal_id": proposal_id,
        "success": success,
        "output_json": normalized_output,
        "error": error,
    })
    .to_string()
}

fn tool_attestation_tape_payload(
    proposal_id: &str,
    attestation_id: &str,
    execution_sha256: &str,
    executed_at_unix_ms: i64,
    timed_out: bool,
    executor: &str,
    sandbox_enforcement: &str,
) -> String {
    json!({
        "proposal_id": proposal_id,
        "attestation_id": attestation_id,
        "execution_sha256": execution_sha256,
        "executed_at_unix_ms": executed_at_unix_ms,
        "timed_out": timed_out,
        "executor": executor,
        "sandbox_enforcement": sandbox_enforcement,
    })
    .to_string()
}

fn current_unix_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64
}

fn redact_session_id(session_id: &str) -> String {
    if session_id.len() <= 8 {
        return "***".to_owned();
    }
    let prefix = &session_id[..4];
    let suffix = &session_id[session_id.len().saturating_sub(4)..];
    format!("{prefix}***{suffix}")
}

const fn status_kind_name(kind: common_v1::stream_status::StatusKind) -> &'static str {
    match kind {
        common_v1::stream_status::StatusKind::Unspecified => "unspecified",
        common_v1::stream_status::StatusKind::Accepted => "accepted",
        common_v1::stream_status::StatusKind::InProgress => "in_progress",
        common_v1::stream_status::StatusKind::Done => "done",
        common_v1::stream_status::StatusKind::Failed => "failed",
    }
}

#[allow(clippy::result_large_err)]
fn unix_ms_now_for_status() -> Result<i64, Status> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| Status::internal(format!("failed to read system clock: {error}")))?;
    Ok(i64::try_from(now.as_millis()).unwrap_or(i64::MAX))
}

#[allow(clippy::result_large_err)]
fn resolve_canvas_state_schema_version(
    requested_state_schema_version: Option<u64>,
    state: &Value,
    fallback_state_schema_version: Option<u64>,
) -> Result<u64, Status> {
    if let Some(value) = requested_state_schema_version {
        if value == 0 {
            return Err(Status::invalid_argument("state_schema_version must be greater than 0"));
        }
    }
    let embedded_state_schema_version =
        state.as_object().and_then(|value| value.get("schema_version")).and_then(Value::as_u64);
    if let Some(value) = embedded_state_schema_version {
        if value == 0 {
            return Err(Status::invalid_argument("embedded schema_version must be greater than 0"));
        }
    }
    if let (Some(requested), Some(embedded)) =
        (requested_state_schema_version, embedded_state_schema_version)
    {
        if requested != embedded {
            return Err(Status::invalid_argument(format!(
                "state_schema_version mismatch between request ({requested}) and state payload ({embedded})"
            )));
        }
    }
    Ok(requested_state_schema_version
        .or(embedded_state_schema_version)
        .or(fallback_state_schema_version)
        .unwrap_or(1))
}

fn load_canvas_records_from_snapshots(
    snapshots: &[CanvasStateSnapshotRecord],
) -> Result<HashMap<String, CanvasRecord>, JournalError> {
    let mut records = HashMap::with_capacity(snapshots.len());
    for snapshot in snapshots {
        serde_json::from_str::<Value>(snapshot.state_json.as_str()).map_err(|error| {
            JournalError::InvalidCanvasReplay {
                canvas_id: snapshot.canvas_id.clone(),
                reason: format!("snapshot state_json is invalid: {error}"),
            }
        })?;
        let bundle: CanvasBundleRecord = serde_json::from_str(snapshot.bundle_json.as_str())
            .map_err(|error| JournalError::InvalidCanvasReplay {
                canvas_id: snapshot.canvas_id.clone(),
                reason: format!("snapshot bundle_json is invalid: {error}"),
            })?;
        let allowed_parent_origins: Vec<String> = serde_json::from_str(
            snapshot.allowed_parent_origins_json.as_str(),
        )
        .map_err(|error| JournalError::InvalidCanvasReplay {
            canvas_id: snapshot.canvas_id.clone(),
            reason: format!("snapshot allowed_parent_origins_json is invalid: {error}"),
        })?;
        if snapshot.state_version == 0 {
            return Err(JournalError::InvalidCanvasReplay {
                canvas_id: snapshot.canvas_id.clone(),
                reason: "snapshot state_version must be greater than 0".to_owned(),
            });
        }
        if snapshot.state_schema_version == 0 {
            return Err(JournalError::InvalidCanvasReplay {
                canvas_id: snapshot.canvas_id.clone(),
                reason: "snapshot state_schema_version must be greater than 0".to_owned(),
            });
        }
        records.insert(
            snapshot.canvas_id.clone(),
            CanvasRecord {
                canvas_id: snapshot.canvas_id.clone(),
                session_id: snapshot.session_id.clone(),
                principal: snapshot.principal.clone(),
                state_version: snapshot.state_version,
                state_schema_version: snapshot.state_schema_version,
                state_json: snapshot.state_json.as_bytes().to_vec(),
                bundle,
                allowed_parent_origins,
                created_at_unix_ms: snapshot.created_at_unix_ms,
                updated_at_unix_ms: snapshot.updated_at_unix_ms,
                expires_at_unix_ms: snapshot.expires_at_unix_ms,
                closed: snapshot.closed,
                close_reason: snapshot.close_reason.clone(),
                update_timestamps_unix_ms: VecDeque::new(),
            },
        );
    }
    Ok(records)
}

fn generate_canvas_signing_secret() -> [u8; 32] {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
    let mut hasher = Sha256::new();
    hasher.update(Ulid::new().to_string().as_bytes());
    hasher.update(now.to_string().as_bytes());
    let digest = hasher.finalize();
    let mut secret = [0_u8; 32];
    secret.copy_from_slice(&digest[..32]);
    secret
}

#[allow(clippy::result_large_err)]
fn normalize_canvas_identifier(raw: &str, field_name: &'static str) -> Result<String, Status> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(Status::invalid_argument(format!("{field_name} cannot be empty")));
    }
    if trimmed.len() > MAX_CANVAS_ID_BYTES {
        return Err(Status::invalid_argument(format!(
            "{field_name} exceeds maximum bytes ({} > {MAX_CANVAS_ID_BYTES})",
            trimmed.len()
        )));
    }
    validate_canonical_id(trimmed).map_err(|_| {
        Status::invalid_argument(format!("{field_name} must be a canonical ULID identifier"))
    })?;
    Ok(trimmed.to_owned())
}

#[allow(clippy::result_large_err)]
fn normalize_canvas_bundle_identifier(raw: &str) -> Result<String, Status> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(format!("bundle-{}", Ulid::new().to_string().to_ascii_lowercase()));
    }
    if trimmed.len() > MAX_CANVAS_BUNDLE_ID_BYTES {
        return Err(Status::invalid_argument(format!(
            "bundle.bundle_id exceeds maximum bytes ({} > {MAX_CANVAS_BUNDLE_ID_BYTES})",
            trimmed.len()
        )));
    }
    if !trimmed.chars().all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-')) {
        return Err(Status::invalid_argument("bundle.bundle_id contains unsupported characters"));
    }
    Ok(trimmed.to_ascii_lowercase())
}

#[allow(clippy::result_large_err)]
fn normalize_canvas_asset_path(raw: &str, field_name: &str) -> Result<String, Status> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(Status::invalid_argument(format!("{field_name} cannot be empty")));
    }
    if trimmed.len() > MAX_CANVAS_ASSET_PATH_BYTES {
        return Err(Status::invalid_argument(format!(
            "{field_name} exceeds maximum bytes ({} > {MAX_CANVAS_ASSET_PATH_BYTES})",
            trimmed.len()
        )));
    }
    if trimmed.starts_with('/') || trimmed.starts_with('\\') || trimmed.contains('\\') {
        return Err(Status::invalid_argument(format!(
            "{field_name} must be a relative forward-slash path"
        )));
    }
    if trimmed.contains("..") {
        return Err(Status::invalid_argument(format!(
            "{field_name} cannot contain parent traversal"
        )));
    }
    if !trimmed
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '/' | '-' | '_' | '.' | '~'))
    {
        return Err(Status::invalid_argument(format!(
            "{field_name} contains unsupported characters"
        )));
    }
    if trimmed.split('/').any(|segment| segment.is_empty() || segment == "." || segment == "..") {
        return Err(Status::invalid_argument(format!(
            "{field_name} contains invalid path segment"
        )));
    }
    Ok(trimmed.to_owned())
}

#[allow(clippy::result_large_err)]
fn normalize_canvas_asset_content_type(raw: &str, field_name: &str) -> Result<String, Status> {
    let trimmed = raw.trim().to_ascii_lowercase();
    if trimmed.is_empty() {
        return Err(Status::invalid_argument(format!("{field_name}.content_type cannot be empty")));
    }
    if trimmed.len() > MAX_CANVAS_ASSET_CONTENT_TYPE_BYTES {
        return Err(Status::invalid_argument(format!(
            "{field_name}.content_type exceeds maximum bytes ({} > {MAX_CANVAS_ASSET_CONTENT_TYPE_BYTES})",
            trimmed.len()
        )));
    }
    if trimmed.contains(';') || trimmed.contains(char::is_whitespace) {
        return Err(Status::invalid_argument(format!(
            "{field_name}.content_type must not include parameters or whitespace"
        )));
    }
    if !matches!(
        trimmed.as_str(),
        "application/javascript"
            | "text/javascript"
            | "text/css"
            | "application/json"
            | "text/plain"
            | "image/svg+xml"
    ) {
        return Err(Status::failed_precondition(format!(
            "{field_name}.content_type '{trimmed}' is not allowed by canvas host policy"
        )));
    }
    Ok(trimmed)
}

fn is_canvas_javascript_content_type(content_type: &str) -> bool {
    matches!(content_type, "application/javascript" | "text/javascript")
}

fn compute_canvas_bundle_sha256(assets: &HashMap<String, CanvasAssetRecord>) -> String {
    let mut ordered = BTreeMap::new();
    for (path, asset) in assets.iter() {
        ordered.insert(path, asset);
    }
    let mut hasher = Sha256::new();
    for (path, asset) in ordered {
        hasher.update(path.as_bytes());
        hasher.update(b"\n");
        hasher.update(asset.content_type.as_bytes());
        hasher.update(b"\n");
        hasher.update(asset.body.as_slice());
        hasher.update(b"\n--\n");
    }
    format!("{:x}", hasher.finalize())
}

#[allow(clippy::result_large_err)]
fn parse_canvas_allowed_parent_origins(origins: &[String]) -> Result<Vec<String>, Status> {
    if origins.is_empty() {
        return Err(Status::invalid_argument(
            "allowed_parent_origins must include at least one origin",
        ));
    }
    if origins.len() > MAX_CANVAS_ALLOWED_PARENT_ORIGINS {
        return Err(Status::invalid_argument(format!(
            "allowed_parent_origins exceeds limit ({} > {MAX_CANVAS_ALLOWED_PARENT_ORIGINS})",
            origins.len()
        )));
    }
    let mut normalized = Vec::new();
    for (index, origin) in origins.iter().enumerate() {
        let source = format!("allowed_parent_origins[{index}]");
        let parsed = normalize_canvas_origin(origin.as_str(), source.as_str())?;
        if !normalized.iter().any(|existing| existing == &parsed) {
            normalized.push(parsed);
        }
    }
    Ok(normalized)
}

#[allow(clippy::result_large_err)]
fn normalize_canvas_origin(raw: &str, field_name: &str) -> Result<String, Status> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(Status::invalid_argument(format!("{field_name} cannot be empty")));
    }
    if trimmed.len() > MAX_CANVAS_ORIGIN_BYTES {
        return Err(Status::invalid_argument(format!(
            "{field_name} exceeds maximum bytes ({} > {MAX_CANVAS_ORIGIN_BYTES})",
            trimmed.len()
        )));
    }
    let parsed = Url::parse(trimmed).map_err(|error| {
        Status::invalid_argument(format!("{field_name} must be a valid URL: {error}"))
    })?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(Status::invalid_argument(format!(
            "{field_name} must use http or https scheme"
        )));
    }
    if parsed.host_str().is_none() {
        return Err(Status::invalid_argument(format!("{field_name} must include host")));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(Status::invalid_argument(format!("{field_name} must not include credentials")));
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(Status::invalid_argument(format!(
            "{field_name} must not include query or fragment"
        )));
    }
    if parsed.path() != "/" && !parsed.path().is_empty() {
        return Err(Status::invalid_argument(format!(
            "{field_name} must not include path segments"
        )));
    }
    Ok(parsed.origin().ascii_serialization())
}

fn build_canvas_csp_header(allowed_parent_origins: &[String]) -> String {
    let frame_ancestors = if allowed_parent_origins.is_empty() {
        "'none'".to_owned()
    } else {
        allowed_parent_origins.join(" ")
    };
    format!(
        "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; object-src 'none'; base-uri 'none'; form-action 'none'; frame-ancestors {frame_ancestors}; sandbox allow-scripts"
    )
}

fn url_encode_component(raw: &str) -> String {
    percent_encode_canvas(raw, false)
}

fn url_encode_path_component(raw: &str) -> String {
    percent_encode_canvas(raw, true)
}

fn percent_encode_canvas(raw: &str, allow_slash: bool) -> String {
    let mut encoded = String::with_capacity(raw.len());
    for byte in raw.bytes() {
        let is_unreserved =
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~');
        if is_unreserved || (allow_slash && byte == b'/') {
            encoded.push(byte as char);
        } else {
            encoded.push('%');
            encoded.push_str(format!("{byte:02X}").as_str());
        }
    }
    encoded
}

fn escape_html_attribute(raw: &str) -> String {
    raw.replace('&', "&amp;").replace('"', "&quot;").replace('<', "&lt;").replace('>', "&gt;")
}

#[allow(clippy::result_large_err)]
fn canonical_id(
    value: Option<common_v1::CanonicalId>,
    field_name: &'static str,
) -> Result<String, Status> {
    let id = value
        .and_then(|id| non_empty(id.ulid))
        .ok_or_else(|| Status::invalid_argument(format!("{field_name} is required")))?;
    validate_canonical_id(id.as_str())
        .map_err(|_| Status::invalid_argument(format!("{field_name} must be a canonical ULID")))?;
    Ok(id)
}

#[allow(clippy::result_large_err)]
fn optional_canonical_id(
    value: Option<common_v1::CanonicalId>,
    field_name: &'static str,
) -> Result<Option<String>, Status> {
    let Some(value) = value else {
        return Ok(None);
    };
    let id = non_empty(value.ulid)
        .ok_or_else(|| Status::invalid_argument(format!("{field_name} must be non-empty")))?;
    validate_canonical_id(id.as_str())
        .map_err(|_| Status::invalid_argument(format!("{field_name} must be a canonical ULID")))?;
    Ok(Some(id))
}

#[allow(clippy::result_large_err)]
fn normalize_agent_identifier(raw: &str, field_name: &'static str) -> Result<String, Status> {
    let value = raw.trim();
    if value.is_empty() {
        return Err(Status::invalid_argument(format!("{field_name} cannot be empty")));
    }
    if value.len() > 64 {
        return Err(Status::invalid_argument(format!("{field_name} cannot exceed 64 bytes")));
    }
    for character in value.chars() {
        if !(character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.')) {
            return Err(Status::invalid_argument(format!(
                "{field_name} contains unsupported character '{character}'"
            )));
        }
    }
    Ok(value.to_ascii_lowercase())
}

pub fn authorize_headers(headers: &HeaderMap, auth: &GatewayAuthConfig) -> Result<(), AuthError> {
    if !auth.require_auth {
        return Ok(());
    }
    let token = auth.admin_token.as_ref().ok_or(AuthError::MissingConfiguredToken)?;
    let candidate =
        extract_bearer_token(headers.get(AUTHORIZATION).and_then(|value| value.to_str().ok()))
            .ok_or(AuthError::InvalidAuthorizationHeader)?;
    if constant_time_eq(token.as_bytes(), candidate.as_bytes()) {
        let principal = require_context_value(
            &|name| headers.get(name).and_then(|value| value.to_str().ok()).map(ToOwned::to_owned),
            HEADER_PRINCIPAL,
        )?;
        enforce_token_principal_binding(principal.as_str(), auth)
    } else {
        Err(AuthError::InvalidToken)
    }
}

pub fn request_context_from_headers(headers: &HeaderMap) -> Result<RequestContext, AuthError> {
    request_context_from_header_resolver(|name| {
        headers.get(name).and_then(|value| value.to_str().ok()).map(ToOwned::to_owned)
    })
}

fn authorize_metadata(
    metadata: &MetadataMap,
    auth: &GatewayAuthConfig,
) -> Result<RequestContext, AuthError> {
    if auth.require_auth {
        let token = auth.admin_token.as_ref().ok_or(AuthError::MissingConfiguredToken)?;
        let candidate = extract_bearer_token(
            metadata.get(AUTHORIZATION.as_str()).and_then(|value| value.to_str().ok()),
        )
        .ok_or(AuthError::InvalidAuthorizationHeader)?;
        if !constant_time_eq(token.as_bytes(), candidate.as_bytes()) {
            return Err(AuthError::InvalidToken);
        }
    }

    let context = request_context_from_header_resolver(|name| {
        metadata.get(name).and_then(|value| value.to_str().ok()).map(ToOwned::to_owned)
    })?;
    enforce_token_principal_binding(context.principal.as_str(), auth)?;
    Ok(context)
}

fn enforce_token_principal_binding(
    principal: &str,
    auth: &GatewayAuthConfig,
) -> Result<(), AuthError> {
    if !auth.require_auth {
        return Ok(());
    }
    let Some(expected_principal) = auth.bound_principal.as_ref() else {
        return Ok(());
    };
    if principal == expected_principal {
        Ok(())
    } else {
        Err(AuthError::InvalidToken)
    }
}

fn request_context_from_header_resolver<F>(resolver: F) -> Result<RequestContext, AuthError>
where
    F: Fn(&'static str) -> Option<String>,
{
    let principal = require_context_value(&resolver, HEADER_PRINCIPAL)?;
    let device_id = require_context_value(&resolver, HEADER_DEVICE_ID)?;
    validate_canonical_id(device_id.as_str()).map_err(|_| AuthError::InvalidDeviceId)?;
    let channel = optional_context_value(&resolver, HEADER_CHANNEL)?;

    Ok(RequestContext { principal, device_id, channel })
}

fn require_context_value<F>(resolver: &F, key: &'static str) -> Result<String, AuthError>
where
    F: Fn(&'static str) -> Option<String>,
{
    let value = resolver(key).ok_or(AuthError::MissingContext(key))?;
    let value = value.trim();
    if value.is_empty() {
        return Err(AuthError::EmptyContext(key));
    }
    Ok(value.to_owned())
}

fn optional_context_value<F>(resolver: &F, key: &'static str) -> Result<Option<String>, AuthError>
where
    F: Fn(&'static str) -> Option<String>,
{
    let Some(value) = resolver(key) else {
        return Ok(None);
    };
    let value = value.trim();
    if value.is_empty() {
        return Err(AuthError::EmptyContext(key));
    }
    Ok(Some(value.to_owned()))
}

#[cfg(test)]
fn build_test_vault() -> Arc<Vault> {
    let nonce = Ulid::new();
    let root = std::env::temp_dir().join(format!("palyra-gateway-test-vault-{nonce}"));
    let identity_root =
        std::env::temp_dir().join(format!("palyra-gateway-test-vault-identity-{nonce}"));
    Arc::new(
        Vault::open_with_config(VaultConfigOptions {
            root: Some(root),
            identity_store_root: Some(identity_root),
            backend_preference: VaultBackendPreference::EncryptedFile,
            max_secret_bytes: MAX_VAULT_SECRET_BYTES,
        })
        .expect("test vault should initialize"),
    )
}

fn extract_bearer_token(raw: Option<&str>) -> Option<&str> {
    let value = raw?.trim();
    let separator_index = value.find(char::is_whitespace)?;
    let (scheme, remainder) = value.split_at(separator_index);
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = remainder.trim();
    if token.is_empty() {
        return None;
    }
    Some(token)
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let max_len = left.len().max(right.len());
    let mut diff = left.len() ^ right.len();
    for index in 0..max_len {
        let lhs = left.get(index).copied().unwrap_or_default();
        let rhs = right.get(index).copied().unwrap_or_default();
        diff |= usize::from(lhs ^ rhs);
    }
    diff == 0
}

fn non_empty(input: String) -> Option<String> {
    if input.trim().is_empty() {
        None
    } else {
        Some(input)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        io::{Read, Write},
        net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener, TcpStream},
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
        thread,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use axum::http::{header::AUTHORIZATION, HeaderMap, HeaderValue};
    use serde_json::{json, Value};

    use crate::agents::AgentCreateRequest;
    use crate::journal::{
        ApprovalCreateRequest, ApprovalDecision, ApprovalDecisionScope, ApprovalPolicySnapshot,
        ApprovalPromptOption, ApprovalPromptRecord, ApprovalRiskLevel, ApprovalSubjectType,
        JournalAppendRequest, JournalConfig, JournalStore, MemoryItemRecord, MemoryScoreBreakdown,
        MemorySearchHit, MemorySource, OrchestratorRunStartRequest,
        OrchestratorSessionUpsertRequest, OrchestratorTapeAppendRequest,
    };
    use tonic::Code;
    use ulid::Ulid;

    use super::{
        apply_tool_approval_outcome, authorize_approvals_action, authorize_headers,
        best_effort_mark_approval_error, constant_time_eq, enforce_memory_item_scope,
        enforce_vault_get_approval_policy, enforce_vault_scope_access, execute_http_fetch_tool,
        execute_memory_search_tool, execute_workspace_patch_tool, extend_patch_string_defaults,
        parse_patch_string_array_field, principal_has_sensitive_service_role,
        record_auth_refresh_journal_event, request_context_from_headers,
        resolve_cron_job_channel_for_create, validate_resolved_fetch_addresses,
        vault_get_requires_approval, workspace_patch_metrics_from_output, AuthError,
        GatewayAuthConfig, GatewayJournalConfigSnapshot, GatewayRuntimeConfigSnapshot,
        GatewayRuntimeState, MemoryRuntimeConfig, ProviderRequest, RequestContext,
        SensitiveServiceRole, ToolApprovalOutcome, HEADER_CHANNEL, HEADER_DEVICE_ID,
        HEADER_PRINCIPAL, MAX_APPROVAL_PAGE_LIMIT, VAULT_RATE_LIMIT_MAX_PRINCIPAL_BUCKETS,
        VAULT_RATE_LIMIT_MAX_REQUESTS_PER_WINDOW,
    };

    static TEMP_JOURNAL_COUNTER: AtomicU64 = AtomicU64::new(0);
    const PARITY_REDIRECT_CREDENTIALS_URL: &str =
        include_str!("../../../fixtures/parity/redirect-credentials-url.txt");
    const PARITY_TRICKY_DOM_HTML: &str = include_str!("../../../fixtures/parity/tricky-dom.html");

    fn unique_temp_journal_path() -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        let counter = TEMP_JOURNAL_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir()
            .join(format!("palyra-gateway-unit-{nonce}-{}-{counter}.sqlite3", std::process::id()))
    }

    fn read_http_request(stream: &mut TcpStream) {
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("request read timeout should be configured");
        let mut buffer = [0_u8; 1024];
        let _ = stream.read(&mut buffer);
    }

    fn spawn_redirect_loop_http_server(
        expected_requests: usize,
    ) -> (String, thread::JoinHandle<()>) {
        let listener =
            TcpListener::bind("127.0.0.1:0").expect("redirect test listener should bind");
        let address = listener.local_addr().expect("redirect test listener address should resolve");
        let handle = thread::spawn(move || {
            for _ in 0..expected_requests {
                let (mut stream, _) =
                    listener.accept().expect("redirect test listener should accept request");
                read_http_request(&mut stream);
                let response = "HTTP/1.1 302 Found\r\nLocation: /loop\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                stream.write_all(response.as_bytes()).expect("redirect test response should write");
                stream.flush().expect("redirect test response should flush");
            }
        });
        (format!("http://{address}/loop"), handle)
    }

    fn spawn_redirect_http_server(location: &str) -> (String, thread::JoinHandle<()>) {
        let listener =
            TcpListener::bind("127.0.0.1:0").expect("redirect test listener should bind");
        let address = listener.local_addr().expect("redirect test listener address should resolve");
        let redirect_location = location.to_owned();
        let handle = thread::spawn(move || {
            let (mut stream, _) =
                listener.accept().expect("redirect test listener should accept request");
            read_http_request(&mut stream);
            let response = format!(
                "HTTP/1.1 302 Found\r\nLocation: {redirect_location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            );
            stream.write_all(response.as_bytes()).expect("redirect test response should write");
            stream.flush().expect("redirect test response should flush");
        });
        (format!("http://{address}/redirect"), handle)
    }

    fn spawn_static_http_server(body: &str) -> (String, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("static test listener should bind");
        let address = listener.local_addr().expect("static test listener address should resolve");
        let response_body = body.to_owned();
        let handle = thread::spawn(move || {
            let (mut stream, _) =
                listener.accept().expect("static test listener should accept request");
            read_http_request(&mut stream);
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                response_body.len(),
                response_body
            );
            stream.write_all(response.as_bytes()).expect("static test response should write");
            stream.flush().expect("static test response should flush");
        });
        (format!("http://{address}/"), handle)
    }

    fn build_test_runtime_state(hash_chain_enabled: bool) -> std::sync::Arc<GatewayRuntimeState> {
        let db_path = unique_temp_journal_path();
        let state_root = std::env::temp_dir().join(format!(
            "palyra-gateway-unit-state-{}-{}",
            std::process::id(),
            TEMP_JOURNAL_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        let identity_root = state_root.join("identity");
        let agent_registry = crate::agents::AgentRegistry::open(identity_root.as_path())
            .expect("agent registry should initialize");
        let journal_store = JournalStore::open(JournalConfig {
            db_path: db_path.clone(),
            hash_chain_enabled,
            max_payload_bytes: 256 * 1024,
        })
        .expect("journal store should initialize");
        GatewayRuntimeState::new(
            GatewayRuntimeConfigSnapshot {
                grpc_bind_addr: "127.0.0.1".to_owned(),
                grpc_port: 7443,
                quic_bind_addr: "127.0.0.1".to_owned(),
                quic_port: 7444,
                quic_enabled: true,
                orchestrator_runloop_v1_enabled: true,
                node_rpc_mtls_required: true,
                admin_auth_required: true,
                vault_get_approval_required_refs: vec!["global/openai_api_key".to_owned()],
                max_tape_entries_per_response: 1_000,
                max_tape_bytes_per_response: 2 * 1024 * 1024,
                channel_router: crate::channel_router::ChannelRouterConfig::default(),
                tool_call: crate::tool_protocol::ToolCallConfig {
                    allowed_tools: vec!["palyra.echo".to_owned()],
                    max_calls_per_run: 4,
                    execution_timeout_ms: 250,
                    process_runner: crate::sandbox_runner::SandboxProcessRunnerPolicy {
                        enabled: false,
                        tier: crate::sandbox_runner::SandboxProcessRunnerTier::B,
                        workspace_root: PathBuf::from("."),
                        allowed_executables: Vec::new(),
                        allow_interpreters: false,
                        egress_enforcement_mode:
                            crate::sandbox_runner::EgressEnforcementMode::Strict,
                        allowed_egress_hosts: Vec::new(),
                        allowed_dns_suffixes: Vec::new(),
                        cpu_time_limit_ms: 2_000,
                        memory_limit_bytes: 256 * 1024 * 1024,
                        max_output_bytes: 64 * 1024,
                    },
                    wasm_runtime: crate::wasm_plugin_runner::WasmPluginRunnerPolicy {
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
                    },
                },
                http_fetch: super::HttpFetchRuntimeConfig {
                    allow_private_targets: false,
                    connect_timeout_ms: 1_500,
                    request_timeout_ms: 10_000,
                    max_response_bytes: 512 * 1024,
                    allow_redirects: true,
                    max_redirects: 3,
                    allowed_content_types: vec![
                        "text/html".to_owned(),
                        "text/plain".to_owned(),
                        "application/json".to_owned(),
                    ],
                    allowed_request_headers: vec![
                        "accept".to_owned(),
                        "accept-language".to_owned(),
                        "if-none-match".to_owned(),
                        "if-modified-since".to_owned(),
                        "user-agent".to_owned(),
                    ],
                    cache_enabled: true,
                    cache_ttl_ms: 30_000,
                    max_cache_entries: 256,
                },
                browser_service: super::BrowserServiceRuntimeConfig {
                    enabled: false,
                    endpoint: "http://127.0.0.1:7543".to_owned(),
                    auth_token: None,
                    connect_timeout_ms: 1_500,
                    request_timeout_ms: 15_000,
                    max_screenshot_bytes: 256 * 1024,
                    max_title_bytes: 4 * 1024,
                },
                canvas_host: super::CanvasHostRuntimeConfig {
                    enabled: true,
                    public_base_url: "http://127.0.0.1:7142".to_owned(),
                    token_ttl_ms: 15 * 60 * 1_000,
                    max_state_bytes: 64 * 1024,
                    max_bundle_bytes: 512 * 1024,
                    max_assets_per_bundle: 32,
                    max_updates_per_minute: 120,
                },
            },
            GatewayJournalConfigSnapshot { db_path, hash_chain_enabled },
            journal_store,
            0,
            agent_registry,
        )
        .expect("runtime state should initialize")
    }

    fn build_test_approval_request(subject_suffix: usize) -> ApprovalCreateRequest {
        ApprovalCreateRequest {
            approval_id: Ulid::new().to_string(),
            session_id: Ulid::new().to_string(),
            run_id: Ulid::new().to_string(),
            principal: "user:ops".to_owned(),
            device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            channel: Some("cli".to_owned()),
            subject_type: ApprovalSubjectType::Tool,
            subject_id: format!("tool:test-{subject_suffix}"),
            request_summary: format!("test summary {subject_suffix}"),
            policy_snapshot: ApprovalPolicySnapshot {
                policy_id: "tool_call_policy.v1".to_owned(),
                policy_hash: "sha256:test".to_owned(),
                evaluation_summary: "approval_required=true".to_owned(),
            },
            prompt: ApprovalPromptRecord {
                title: "Approve tool execution".to_owned(),
                risk_level: ApprovalRiskLevel::High,
                subject_id: format!("tool:test-{subject_suffix}"),
                summary: "Tool requires approval".to_owned(),
                options: vec![
                    ApprovalPromptOption {
                        option_id: "allow_once".to_owned(),
                        label: "Allow once".to_owned(),
                        description: "Approve once".to_owned(),
                        default_selected: true,
                        decision_scope: ApprovalDecisionScope::Once,
                        timebox_ttl_ms: None,
                    },
                    ApprovalPromptOption {
                        option_id: "deny_once".to_owned(),
                        label: "Deny".to_owned(),
                        description: "Reject".to_owned(),
                        default_selected: false,
                        decision_scope: ApprovalDecisionScope::Once,
                        timebox_ttl_ms: None,
                    },
                ],
                timeout_seconds: 60,
                details_json: r#"{"tool_name":"test"}"#.to_owned(),
                policy_explanation: "Policy requires explicit approval".to_owned(),
            },
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn http_fetch_rejects_blocked_scheme() {
        let state = build_test_runtime_state(false);
        let input = serde_json::to_vec(&json!({
            "url": "file:///tmp/secret.txt"
        }))
        .expect("input should serialize");
        let outcome =
            execute_http_fetch_tool(&state, "proposal-http-fetch-1", input.as_slice()).await;
        assert!(!outcome.success, "blocked scheme should be rejected");
        assert!(
            outcome.error.contains("blocked URL scheme"),
            "error should explain blocked scheme: {}",
            outcome.error
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn http_fetch_rejects_private_targets_by_default() {
        let state = build_test_runtime_state(false);
        let input = serde_json::to_vec(&json!({
            "url": "http://127.0.0.1:8080/"
        }))
        .expect("input should serialize");
        let outcome =
            execute_http_fetch_tool(&state, "proposal-http-fetch-2", input.as_slice()).await;
        assert!(!outcome.success, "private targets must be denied by default");
        assert!(
            outcome.error.contains("target blocked") && outcome.error.contains("private/local"),
            "error should explain private target block: {}",
            outcome.error
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn http_fetch_rejects_url_credentials() {
        let state = build_test_runtime_state(false);
        let input = serde_json::to_vec(&json!({
            "url": PARITY_REDIRECT_CREDENTIALS_URL.trim()
        }))
        .expect("input should serialize");
        let outcome =
            execute_http_fetch_tool(&state, "proposal-http-fetch-credentials", input.as_slice())
                .await;
        assert!(!outcome.success, "URL credentials must be denied");
        assert!(
            outcome.error.contains("URL credentials are not allowed"),
            "error should explain credential rejection: {}",
            outcome.error
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn http_fetch_rejects_redirect_hop_with_url_credentials() {
        let state = build_test_runtime_state(false);
        let (url, handle) = spawn_redirect_http_server(PARITY_REDIRECT_CREDENTIALS_URL.trim());
        let input = serde_json::to_vec(&json!({
            "url": url,
            "allow_private_targets": true,
            "allow_redirects": true
        }))
        .expect("input should serialize");
        let outcome = execute_http_fetch_tool(
            &state,
            "proposal-http-fetch-redirect-credentials",
            input.as_slice(),
        )
        .await;
        assert!(!outcome.success, "redirect hop URLs with credentials must be denied");
        assert!(
            outcome.error.contains("URL credentials are not allowed"),
            "error should explain credential rejection on redirect hops: {}",
            outcome.error
        );
        handle.join().expect("redirect test server should complete after one request");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn http_fetch_parity_fixture_exposes_deterministic_body_text() {
        let state = build_test_runtime_state(false);
        let (url, handle) = spawn_static_http_server(PARITY_TRICKY_DOM_HTML);
        let input = serde_json::to_vec(&json!({
            "url": url,
            "allow_private_targets": true
        }))
        .expect("input should serialize");
        let outcome =
            execute_http_fetch_tool(&state, "proposal-http-fetch-parity-fixture", input.as_slice())
                .await;
        assert!(outcome.success, "parity fixture HTML should be fetched successfully");
        let payload: Value = serde_json::from_slice(outcome.output_json.as_slice())
            .expect("http.fetch output JSON should parse");
        let body_text = payload
            .get("body_text")
            .and_then(Value::as_str)
            .expect("http.fetch output should include response body text");
        assert!(
            body_text.contains("Observe Fixture"),
            "fixture body should include canonical title marker"
        );
        assert!(
            body_text.contains("access_token=secret"),
            "fixture body should include sensitive query token fixture payload"
        );
        handle.join().expect("static fixture server should complete after one request");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn http_fetch_detects_redirect_loop_limit() {
        let state = build_test_runtime_state(false);
        let (url, handle) = spawn_redirect_loop_http_server(3);
        let input = serde_json::to_vec(&json!({
            "url": url,
            "allow_private_targets": true,
            "allow_redirects": true,
            "max_redirects": 2
        }))
        .expect("input should serialize");
        let outcome =
            execute_http_fetch_tool(&state, "proposal-http-fetch-3", input.as_slice()).await;
        assert!(!outcome.success, "redirect loops should be bounded");
        assert!(
            outcome.error.contains("redirect limit exceeded (2)"),
            "error should include redirect limit context: {}",
            outcome.error
        );
        handle.join().expect("redirect loop server should process expected request count");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn http_fetch_enforces_response_size_cutoff() {
        let state = build_test_runtime_state(false);
        let (url, handle) = spawn_static_http_server(&"X".repeat(256));
        let input = serde_json::to_vec(&json!({
            "url": url,
            "allow_private_targets": true,
            "max_response_bytes": 64
        }))
        .expect("input should serialize");
        let outcome =
            execute_http_fetch_tool(&state, "proposal-http-fetch-4", input.as_slice()).await;
        assert!(!outcome.success, "oversized response should be rejected");
        assert!(
            outcome.error.contains("max_response_bytes (64)"),
            "error should include cutoff details: {}",
            outcome.error
        );
        handle.join().expect("static server should complete after single request");
    }

    #[test]
    fn http_fetch_rebinding_simulation_rejects_mixed_public_private_answers() {
        let addresses = vec![
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34)), 443),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 443),
        ];
        let blocked = validate_resolved_fetch_addresses(addresses.as_slice(), false);
        assert!(
            blocked.is_err(),
            "mixed public/private DNS answers must be denied to prevent rebinding"
        );
        let allowed = validate_resolved_fetch_addresses(addresses.as_slice(), true);
        assert!(
            allowed.is_ok(),
            "explicit private-target override should permit mixed DNS answers"
        );
    }

    #[test]
    fn validate_resolved_fetch_addresses_blocks_ssrf_sensitive_ipv4_ranges() {
        let blocked = [
            Ipv4Addr::new(100, 64, 0, 1),
            Ipv4Addr::new(169, 254, 169, 254),
            Ipv4Addr::new(198, 18, 0, 1),
            Ipv4Addr::new(192, 0, 2, 42),
            Ipv4Addr::new(198, 51, 100, 42),
            Ipv4Addr::new(203, 0, 113, 42),
            Ipv4Addr::new(240, 1, 2, 3),
        ];
        for ip in blocked {
            let result =
                validate_resolved_fetch_addresses(&[SocketAddr::new(IpAddr::V4(ip), 443)], false);
            assert!(
                result.is_err(),
                "address {ip} must be treated as non-public and denied by default"
            );
        }
    }

    #[test]
    fn validate_resolved_fetch_addresses_blocks_ssrf_sensitive_ipv6_ranges() {
        let blocked = [
            Ipv6Addr::LOCALHOST,
            Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, 1),
            Ipv6Addr::new(0xfec0, 0, 0, 0, 0, 0, 0, 1),
            Ipv6Addr::new(0xff02, 0, 0, 0, 0, 0, 0, 1),
        ];
        for ip in blocked {
            let result =
                validate_resolved_fetch_addresses(&[SocketAddr::new(IpAddr::V6(ip), 443)], false);
            assert!(
                result.is_err(),
                "address {ip} must be treated as non-public and denied by default"
            );
        }
    }

    #[test]
    fn authorize_headers_rejects_missing_token_when_required() {
        let auth = GatewayAuthConfig {
            require_auth: true,
            admin_token: Some("secret".to_owned()),
            bound_principal: Some("user:ops".to_owned()),
        };
        let headers = HeaderMap::new();
        let result = authorize_headers(&headers, &auth);
        assert_eq!(result, Err(AuthError::InvalidAuthorizationHeader));
    }

    #[test]
    fn authorize_headers_accepts_matching_bearer_token() {
        let auth = GatewayAuthConfig {
            require_auth: true,
            admin_token: Some("secret".to_owned()),
            bound_principal: Some("user:ops".to_owned()),
        };
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer secret"));
        headers.insert(HEADER_PRINCIPAL, HeaderValue::from_static("user:ops"));
        let result = authorize_headers(&headers, &auth);
        assert!(result.is_ok(), "matching bearer token should be accepted");
    }

    #[test]
    fn authorize_headers_accepts_case_insensitive_bearer_scheme() {
        let auth = GatewayAuthConfig {
            require_auth: true,
            admin_token: Some("secret".to_owned()),
            bound_principal: Some("user:ops".to_owned()),
        };
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_static("bEaReR secret"));
        headers.insert(HEADER_PRINCIPAL, HeaderValue::from_static("user:ops"));
        let result = authorize_headers(&headers, &auth);
        assert!(result.is_ok(), "bearer auth scheme should be parsed case-insensitively");
    }

    fn test_memory_item(channel: Option<&str>) -> MemoryItemRecord {
        MemoryItemRecord {
            memory_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            principal: "user:ops".to_owned(),
            channel: channel.map(str::to_owned),
            session_id: None,
            source: MemorySource::Manual,
            content_text: "test memory".to_owned(),
            content_hash: "sha256:test".to_owned(),
            tags: vec!["test".to_owned()],
            confidence: None,
            ttl_unix_ms: None,
            created_at_unix_ms: 1,
            updated_at_unix_ms: 1,
        }
    }

    #[test]
    fn memory_auto_inject_tape_payload_redacts_secret_like_values() {
        let hit = MemorySearchHit {
            item: test_memory_item(None),
            snippet: "token=abc123 should never leak".to_owned(),
            score: 0.87,
            breakdown: MemoryScoreBreakdown {
                lexical_score: 0.5,
                vector_score: 0.2,
                recency_score: 0.17,
                final_score: 0.87,
            },
        };
        let payload = super::memory_auto_inject_tape_payload(
            "Bearer topsecret123 access_token=supersecret",
            &[hit],
        );
        assert!(
            payload.contains("<redacted>"),
            "memory auto-inject tape payload should include redaction marker"
        );
        assert!(
            !payload.contains("topsecret123")
                && !payload.contains("access_token=supersecret")
                && !payload.contains("token=abc123"),
            "secret-like values must be redacted before tape persistence: {payload}"
        );
    }

    #[test]
    fn memory_item_message_redacts_legacy_secret_like_content_text() {
        let mut item = test_memory_item(None);
        item.content_text =
            "legacy payload bearer topsecret refresh_token=shh cookie: sessionid=abc".to_owned();
        let message = super::memory_item_message(&item);
        assert!(
            message.content_text.contains("<redacted>"),
            "memory item response should include redaction marker"
        );
        assert!(
            !message.content_text.contains("topsecret")
                && !message.content_text.contains("refresh_token=shh")
                && !message.content_text.contains("sessionid=abc"),
            "memory item response must not leak secret-like values: {}",
            message.content_text
        );
    }

    #[test]
    fn memory_search_hit_message_redacts_legacy_secret_like_snippet() {
        let hit = MemorySearchHit {
            item: test_memory_item(None),
            snippet: "url token=abc123 and api_key=qwerty must be hidden".to_owned(),
            score: 0.42,
            breakdown: MemoryScoreBreakdown {
                lexical_score: 0.2,
                vector_score: 0.1,
                recency_score: 0.12,
                final_score: 0.42,
            },
        };
        let message = super::memory_search_hit_message(&hit, false);
        assert!(
            message.snippet.contains("<redacted>"),
            "search hit snippet should include redaction marker"
        );
        assert!(
            !message.snippet.contains("token=abc123")
                && !message.snippet.contains("api_key=qwerty"),
            "search hit snippet must not leak secret-like values: {}",
            message.snippet
        );
    }

    #[test]
    fn redact_memory_text_for_output_keeps_non_secret_text_stable() {
        let safe = "release train rollback checklist";
        assert_eq!(
            super::redact_memory_text_for_output(safe),
            safe,
            "safe memory text should remain unchanged"
        );
    }

    #[test]
    fn memory_search_tool_output_payload_redacts_secret_like_values() {
        let mut item = test_memory_item(None);
        item.content_text = "legacy row bearer topsecret token=abc123".to_owned();
        let hit = MemorySearchHit {
            item,
            snippet: "url refresh_token=hidden should be redacted".to_owned(),
            score: 0.66,
            breakdown: MemoryScoreBreakdown {
                lexical_score: 0.3,
                vector_score: 0.2,
                recency_score: 0.16,
                final_score: 0.66,
            },
        };

        let payload = super::memory_search_tool_output_payload(&[hit]);
        let encoded = serde_json::to_string(&payload).expect("payload should serialize");
        assert!(
            encoded.contains("<redacted>"),
            "tool output payload should include redaction marker"
        );
        assert!(
            !encoded.contains("topsecret")
                && !encoded.contains("token=abc123")
                && !encoded.contains("refresh_token=hidden"),
            "tool output payload must not leak secret-like values: {encoded}"
        );
    }

    #[test]
    fn sensitive_service_role_guard_matches_expected_principals() {
        assert!(
            principal_has_sensitive_service_role("admin:ops", SensitiveServiceRole::AdminOnly),
            "admin principal should satisfy admin-only guard"
        );
        assert!(
            !principal_has_sensitive_service_role("system:cron", SensitiveServiceRole::AdminOnly),
            "system principal should not satisfy admin-only guard"
        );
        assert!(
            principal_has_sensitive_service_role(
                "system:cron",
                SensitiveServiceRole::AdminOrSystem
            ),
            "system principal should satisfy admin-or-system guard"
        );
        assert!(
            !principal_has_sensitive_service_role("user:ops", SensitiveServiceRole::AdminOrSystem),
            "regular user principal should not satisfy elevated guard"
        );
    }

    #[test]
    fn approvals_authorization_requires_admin_or_system_principal() {
        let denied = authorize_approvals_action("user:ops", "approvals.list", "approvals:records")
            .expect_err("non-admin principal should be denied");
        assert_eq!(denied.code(), Code::PermissionDenied);
        assert!(
            authorize_approvals_action("admin:ops", "approvals.list", "approvals:records").is_ok(),
            "admin principal should pass approvals guard"
        );
        assert!(
            authorize_approvals_action("system:cron", "approvals.list", "approvals:records")
                .is_ok(),
            "system principal should pass approvals guard"
        );
    }

    #[test]
    fn memory_scope_requires_channel_context_for_channel_scoped_item() {
        let item = test_memory_item(Some("discord"));
        let denied = enforce_memory_item_scope(&item, "user:ops", None)
            .expect_err("channel-scoped memory should require channel context");
        assert_eq!(denied.code(), Code::PermissionDenied);
        assert_eq!(
            denied.message(),
            "memory item is channel-scoped and requires authenticated channel context"
        );
    }

    #[test]
    fn memory_scope_allows_global_item_without_channel_context() {
        let item = test_memory_item(None);
        enforce_memory_item_scope(&item, "user:ops", None)
            .expect("global memory item should be accessible without channel context");
    }

    #[test]
    fn authorize_headers_rejects_principal_mismatch_with_bound_principal() {
        let auth = GatewayAuthConfig {
            require_auth: true,
            admin_token: Some("secret".to_owned()),
            bound_principal: Some("user:ops".to_owned()),
        };
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer secret"));
        headers.insert(HEADER_PRINCIPAL, HeaderValue::from_static("user:finance"));
        let result = authorize_headers(&headers, &auth);
        assert_eq!(result, Err(AuthError::InvalidToken));
    }

    #[test]
    fn constant_time_eq_rejects_length_mismatch() {
        assert!(
            !constant_time_eq(b"secret", b"secret-longer"),
            "length mismatch should never compare as equal"
        );
    }

    #[test]
    fn request_context_from_headers_validates_device_id() {
        let mut headers = HeaderMap::new();
        headers.insert(HEADER_PRINCIPAL, HeaderValue::from_static("user:ops"));
        headers.insert(HEADER_DEVICE_ID, HeaderValue::from_static("invalid-id"));
        let result = request_context_from_headers(&headers);
        assert_eq!(result, Err(AuthError::InvalidDeviceId));
    }

    #[test]
    fn request_context_from_headers_extracts_expected_fields() {
        let mut headers = HeaderMap::new();
        headers.insert(HEADER_PRINCIPAL, HeaderValue::from_static("user:ops"));
        headers.insert(HEADER_DEVICE_ID, HeaderValue::from_static("01ARZ3NDEKTSV4RRFFQ69G5FAV"));
        headers.insert(HEADER_CHANNEL, HeaderValue::from_static("cli"));
        let context = request_context_from_headers(&headers).expect("context should parse");
        assert_eq!(
            context,
            RequestContext {
                principal: "user:ops".to_owned(),
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                channel: Some("cli".to_owned()),
            }
        );
    }

    #[test]
    fn vault_scope_enforcement_allows_matching_principal_scope() {
        let context = RequestContext {
            principal: "user:ops".to_owned(),
            device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            channel: Some("cli".to_owned()),
        };
        let scope = super::VaultScope::Principal { principal_id: "user:ops".to_owned() };
        assert!(
            enforce_vault_scope_access(&scope, &context).is_ok(),
            "principal scope should be allowed when it matches authenticated principal"
        );
    }

    #[test]
    fn vault_scope_enforcement_rejects_mismatched_principal_scope() {
        let context = RequestContext {
            principal: "user:ops".to_owned(),
            device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            channel: Some("cli".to_owned()),
        };
        let scope = super::VaultScope::Principal { principal_id: "user:finance".to_owned() };
        let error = enforce_vault_scope_access(&scope, &context)
            .expect_err("mismatched principal scope must be denied");
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn vault_scope_enforcement_rejects_missing_or_mismatched_channel_scope() {
        let missing_channel_context = RequestContext {
            principal: "user:ops".to_owned(),
            device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            channel: None,
        };
        let scope = super::VaultScope::Channel {
            channel_name: "cli".to_owned(),
            account_id: "acct-1".to_owned(),
        };
        let missing_channel_error = enforce_vault_scope_access(&scope, &missing_channel_context)
            .expect_err("channel scope without context channel must be denied");
        assert_eq!(missing_channel_error.code(), tonic::Code::PermissionDenied);

        let mismatched_channel_context = RequestContext {
            principal: "user:ops".to_owned(),
            device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            channel: Some("slack".to_owned()),
        };
        let mismatched_channel_error =
            enforce_vault_scope_access(&scope, &mismatched_channel_context)
                .expect_err("mismatched channel scope must be denied");
        assert_eq!(mismatched_channel_error.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn vault_scope_enforcement_accepts_channel_scope_with_exact_context_match() {
        let context = RequestContext {
            principal: "user:ops".to_owned(),
            device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            channel: Some("slack:acct-1".to_owned()),
        };
        let scope = super::VaultScope::Channel {
            channel_name: "slack".to_owned(),
            account_id: "acct-1".to_owned(),
        };
        assert!(
            enforce_vault_scope_access(&scope, &context).is_ok(),
            "channel scope should be allowed when authenticated channel context matches scope"
        );
    }

    #[test]
    fn vault_scope_enforcement_rejects_bare_channel_name_for_account_scope() {
        let context = RequestContext {
            principal: "user:ops".to_owned(),
            device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            channel: Some("slack".to_owned()),
        };
        let scope = super::VaultScope::Channel {
            channel_name: "slack".to_owned(),
            account_id: "acct-1".to_owned(),
        };
        let error = enforce_vault_scope_access(&scope, &context)
            .expect_err("bare channel context must not satisfy account-scoped vault access");
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn vault_get_approval_matcher_checks_selected_scope_key_refs() {
        let refs = vec!["global/openai_api_key".to_owned()];
        let matched =
            vault_get_requires_approval(&super::VaultScope::Global, "openai_api_key", &refs);
        let not_matched =
            vault_get_requires_approval(&super::VaultScope::Global, "non_sensitive", &refs);
        assert!(matched, "configured scope/key ref should require explicit approval");
        assert!(!not_matched, "unconfigured scope/key ref should not require explicit approval");
    }

    #[test]
    fn vault_get_approval_policy_denies_without_explicit_approval() {
        let refs = vec!["global/openai_api_key".to_owned()];
        let error = enforce_vault_get_approval_policy(
            "user:ops",
            &super::VaultScope::Global,
            "openai_api_key",
            refs.as_slice(),
            None,
        )
        .expect_err("selected sensitive vault ref must be denied without explicit approval");
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
        assert!(
            error.message().contains("explicit approval"),
            "deny reason should explain explicit approval requirement"
        );
    }

    #[test]
    fn vault_get_approval_policy_allows_with_explicit_approval_header() {
        let refs = vec!["global/openai_api_key".to_owned()];
        let result = enforce_vault_get_approval_policy(
            "user:ops",
            &super::VaultScope::Global,
            "openai_api_key",
            refs.as_slice(),
            Some("allow"),
        );
        assert!(result.is_ok(), "explicit approval header should allow configured sensitive ref");
    }

    #[test]
    fn cron_channel_create_allows_payload_channel_without_context() {
        let channel = resolve_cron_job_channel_for_create(None, "slack:acct-1".to_owned())
            .expect("payload channel should be accepted when no channel context is present");
        assert_eq!(channel, "slack:acct-1");
    }

    #[test]
    fn cron_channel_create_requires_context_match() {
        let error = resolve_cron_job_channel_for_create(Some("cli"), "slack:acct-1".to_owned())
            .expect_err("payload channel must match authenticated channel context");
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn cron_channel_create_allows_system_channel_with_context_mismatch() {
        let channel = resolve_cron_job_channel_for_create(Some("cli"), "system:cron".to_owned())
            .expect("system:cron channel should remain allowed for scheduler ownership");
        assert_eq!(channel, "system:cron");
    }

    #[test]
    fn cron_channel_create_defaults_to_system_when_context_and_payload_are_missing() {
        let channel = resolve_cron_job_channel_for_create(None, String::new())
            .expect("missing context and empty payload should default to system channel");
        assert_eq!(channel, "system:cron");
    }

    #[test]
    fn vault_scope_enforcement_rejects_skill_scope_for_external_rpc() {
        let context = RequestContext {
            principal: "user:ops".to_owned(),
            device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            channel: Some("cli".to_owned()),
        };
        let scope = super::VaultScope::Skill { skill_id: "skill.slack.bot".to_owned() };
        let error = enforce_vault_scope_access(&scope, &context)
            .expect_err("skill scope should not be exposed via external vault RPC");
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn vault_rate_limit_principal_bucket_count_is_bounded() {
        let state = build_test_runtime_state(false);
        for index in 0..VAULT_RATE_LIMIT_MAX_PRINCIPAL_BUCKETS {
            let allowed = state.consume_vault_rate_limit(format!("user:{index}").as_str());
            assert!(allowed, "initial request for unique principal should be allowed");
        }
        assert!(
            state.consume_vault_rate_limit("user:overflow"),
            "new principal should remain admissible via oldest-bucket eviction at cap"
        );
        let bucket_count = match state.vault_rate_limit.lock() {
            Ok(cache) => cache.len(),
            Err(poisoned) => poisoned.into_inner().len(),
        };
        assert_eq!(
            bucket_count, VAULT_RATE_LIMIT_MAX_PRINCIPAL_BUCKETS,
            "eviction should keep bucket map bounded to configured cap"
        );
    }

    #[test]
    fn vault_rate_limit_still_throttles_hot_principal_within_window() {
        let state = build_test_runtime_state(false);
        for attempt in 0..VAULT_RATE_LIMIT_MAX_REQUESTS_PER_WINDOW {
            assert!(
                state.consume_vault_rate_limit("user:hot"),
                "request {attempt} within per-window limit should be allowed"
            );
        }
        assert!(
            !state.consume_vault_rate_limit("user:hot"),
            "request above per-window limit should be throttled"
        );
    }

    #[test]
    fn memory_config_snapshot_recovers_from_poisoned_lock_without_default_fallback() {
        let state = build_test_runtime_state(false);
        let poisoned_state = std::sync::Arc::clone(&state);
        let panic_result = std::thread::spawn(move || {
            let _guard = poisoned_state
                .memory_config
                .write()
                .expect("memory config lock should be available before poisoning");
            panic!("intentional memory config lock poison");
        })
        .join();
        assert!(panic_result.is_err(), "poisoning helper thread should panic");

        let expected = MemoryRuntimeConfig {
            max_item_bytes: 4_096,
            max_item_tokens: 128,
            auto_inject_enabled: true,
            auto_inject_max_items: 2,
            default_ttl_ms: Some(60_000),
        };
        state.configure_memory(expected.clone());
        assert_eq!(
            state.memory_config_snapshot(),
            expected,
            "poisoned lock recovery should preserve configured runtime memory limits"
        );
    }

    #[test]
    fn clear_memory_search_cache_recovers_from_poisoned_lock() {
        let state = build_test_runtime_state(false);
        {
            let mut cache = state
                .memory_search_cache
                .lock()
                .expect("cache lock should be available before poisoning");
            cache.insert("seed".to_owned(), Vec::new());
        }

        let poisoned_state = std::sync::Arc::clone(&state);
        let panic_result = std::thread::spawn(move || {
            let _guard = poisoned_state
                .memory_search_cache
                .lock()
                .expect("cache lock should be available before poisoning");
            panic!("intentional memory cache lock poison");
        })
        .join();
        assert!(panic_result.is_err(), "poisoning helper thread should panic");

        state.clear_memory_search_cache();
        let cache_is_empty = match state.memory_search_cache.lock() {
            Ok(cache) => cache.is_empty(),
            Err(poisoned) => poisoned.into_inner().is_empty(),
        };
        assert!(cache_is_empty, "cache clear should succeed even when lock is poisoned");
    }

    #[test]
    fn status_snapshot_reports_journal_counters_and_storage_metadata() {
        let state = build_test_runtime_state(true);

        state
            .record_journal_event_blocking(&JournalAppendRequest {
                event_id: "01ARZ3NDEKTSV4RRFFQ69G5FB0".to_owned(),
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned(),
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
                kind: 1,
                actor: 1,
                timestamp_unix_ms: 1_730_000_000_000,
                payload_json: br#"{"token":"SECRET","safe":"ok"}"#.to_vec(),
                principal: "user:ops".to_owned(),
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                channel: Some("cli".to_owned()),
            })
            .expect("journal record should succeed");

        let status = state.status_snapshot(
            RequestContext {
                principal: "user:ops".to_owned(),
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                channel: Some("cli".to_owned()),
            },
            &GatewayAuthConfig {
                require_auth: true,
                admin_token: Some("token".to_owned()),
                bound_principal: Some("user:ops".to_owned()),
            },
        );
        assert_eq!(
            status.counters.journal_events, 1,
            "status should report persisted journal count"
        );
        assert_eq!(status.counters.journal_redacted_events, 1, "status should report redactions");
        assert!(status.storage.journal_hash_chain_enabled, "hash-chain flag should be surfaced");
        assert!(
            status.security.orchestrator_runloop_v1_enabled,
            "status should expose orchestrator runloop flag"
        );
        assert!(
            status.storage.latest_event_hash.is_some(),
            "latest hash should be available when hash-chain is enabled"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn status_snapshot_surfaces_model_provider_runtime_aggregates() {
        let state = build_test_runtime_state(false);

        state
            .execute_model_provider(ProviderRequest {
                input_text: "status snapshot provider metrics".to_owned(),
                json_mode: false,
                vision_requested: false,
            })
            .await
            .expect("deterministic provider request should succeed");
        let failed = state
            .execute_model_provider(ProviderRequest {
                input_text: "vision unsupported path".to_owned(),
                json_mode: false,
                vision_requested: true,
            })
            .await;
        assert!(
            failed.is_err(),
            "vision request should fail and contribute to provider error aggregates"
        );

        let status = state.status_snapshot(
            RequestContext {
                principal: "user:ops".to_owned(),
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                channel: Some("cli".to_owned()),
            },
            &GatewayAuthConfig {
                require_auth: true,
                admin_token: Some("token".to_owned()),
                bound_principal: Some("user:ops".to_owned()),
            },
        );
        assert_eq!(status.model_provider.runtime_metrics.request_count, 2);
        assert_eq!(status.model_provider.runtime_metrics.error_count, 1);
        assert_eq!(status.model_provider.runtime_metrics.error_rate_bps, 5_000);
        assert!(
            status.model_provider.runtime_metrics.total_prompt_tokens > 0,
            "status snapshot should expose accumulated prompt token usage"
        );
        assert!(
            status.model_provider.runtime_metrics.total_completion_tokens > 0,
            "status snapshot should expose accumulated completion token usage"
        );
        assert_eq!(
            status.counters.model_provider_requests, 2,
            "gateway counters should keep tracking provider request totals"
        );
        assert_eq!(
            status.counters.model_provider_failures, 1,
            "gateway counters should keep tracking provider failures"
        );
    }

    #[test]
    fn recent_journal_snapshot_returns_events_for_admin_surface() {
        let state = build_test_runtime_state(false);

        for index in 0..3 {
            state
                .record_journal_event_blocking(&JournalAppendRequest {
                    event_id: format!("01ARZ3NDEKTSV4RRFFQ69G5FD{index}"),
                    session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned(),
                    run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
                    kind: 1,
                    actor: 1,
                    timestamp_unix_ms: 1_730_000_000_000 + index,
                    payload_json: format!(r#"{{"index":{index}}}"#).into_bytes(),
                    principal: "user:ops".to_owned(),
                    device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                    channel: Some("cli".to_owned()),
                })
                .expect("journal record should succeed");
        }

        let snapshot = state
            .recent_journal_snapshot_blocking(1000)
            .expect("recent journal snapshot should be returned");
        assert_eq!(snapshot.total_events, 3);
        assert_eq!(snapshot.events.len(), 3);
        assert!(
            snapshot.events[0].event_id.ends_with('2'),
            "recent events should be returned in descending order"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn auth_refresh_journal_event_redacts_reason_text() {
        let state = build_test_runtime_state(false);
        let context = RequestContext {
            principal: "admin:ops".to_owned(),
            device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            channel: Some("cli".to_owned()),
        };
        let outcome = palyra_auth::OAuthRefreshOutcome {
            profile_id: "openai-default".to_owned(),
            provider: "openai".to_owned(),
            kind: palyra_auth::OAuthRefreshOutcomeKind::Failed,
            reason: "Bearer topsecret123 sk-test-secret-token token=qwe".to_owned(),
            next_allowed_refresh_unix_ms: Some(1_730_000_000_000),
            expires_at_unix_ms: None,
        };

        record_auth_refresh_journal_event(&state, &context, &outcome)
            .await
            .expect("auth refresh journal event should persist");

        let snapshot = state
            .recent_journal_snapshot_blocking(100)
            .expect("recent journal snapshot should be returned");
        let payload = snapshot
            .events
            .iter()
            .find_map(|event| {
                let parsed = serde_json::from_str::<Value>(event.payload_json.as_str()).ok()?;
                if parsed.get("event").and_then(Value::as_str) == Some("auth.refresh.failed") {
                    Some(parsed)
                } else {
                    None
                }
            })
            .expect("auth refresh event should be present in recent journal snapshot");
        let reason = payload.get("reason").and_then(Value::as_str).unwrap_or_default();
        assert!(reason.contains("<redacted>"), "auth refresh reason should be redacted");
        assert!(
            !reason.contains("topsecret123")
                && !reason.contains("sk-test-secret-token")
                && !reason.contains("token=qwe"),
            "auth refresh journal reason must not leak raw secret values"
        );
    }

    #[test]
    fn approval_required_decision_is_denied_without_interactive_channel() {
        let decision = crate::tool_protocol::ToolDecision {
            allowed: true,
            reason: "allowlisted by policy".to_owned(),
            approval_required: true,
            policy_enforced: true,
        };
        let enforced = apply_tool_approval_outcome(decision, "palyra.process.run", None);
        assert!(!enforced.allowed, "allowed decisions must be denied until approval is granted");
        assert!(
            enforced.reason.contains("approval required"),
            "denial reason should explain why execution was blocked"
        );
    }

    #[test]
    fn approval_required_decision_is_allowed_with_explicit_approval() {
        let decision = crate::tool_protocol::ToolDecision {
            allowed: true,
            reason: "allowlisted by policy".to_owned(),
            approval_required: true,
            policy_enforced: true,
        };
        let approval = ToolApprovalOutcome {
            approval_id: "01ARZ3NDEKTSV4RRFFQ69G5FB0".to_owned(),
            approved: true,
            reason: "allow_once".to_owned(),
            decision: crate::journal::ApprovalDecision::Allow,
            decision_scope: crate::journal::ApprovalDecisionScope::Once,
            decision_scope_ttl_ms: None,
        };
        let enforced = apply_tool_approval_outcome(decision, "palyra.process.run", Some(&approval));
        assert!(enforced.allowed, "explicit approval should keep allow decisions allowed");
        assert!(
            enforced.reason.contains("explicit approval granted"),
            "allow reason should preserve approval context"
        );
    }

    #[test]
    fn tool_approval_cache_does_not_store_once_scope_entries() {
        let state = build_test_runtime_state(false);
        let context = RequestContext {
            principal: "user:ops".to_owned(),
            device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            channel: Some("cli".to_owned()),
        };
        let outcome = ToolApprovalOutcome {
            approval_id: "01ARZ3NDEKTSV4RRFFQ69G5FB0".to_owned(),
            approved: true,
            reason: "allow_once".to_owned(),
            decision: ApprovalDecision::Allow,
            decision_scope: ApprovalDecisionScope::Once,
            decision_scope_ttl_ms: None,
        };
        state.remember_tool_approval(&context, "session-1", "tool:custom.noop", &outcome);
        let cached = state.resolve_cached_tool_approval(&context, "session-1", "tool:custom.noop");
        assert!(cached.is_none(), "allow-once decisions must not be remembered in cache");
    }

    #[test]
    fn tool_approval_cache_reuses_session_scope_and_clears_on_session_reset() {
        let state = build_test_runtime_state(false);
        let context = RequestContext {
            principal: "user:ops".to_owned(),
            device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            channel: Some("cli".to_owned()),
        };
        let outcome = ToolApprovalOutcome {
            approval_id: "01ARZ3NDEKTSV4RRFFQ69G5FB1".to_owned(),
            approved: false,
            reason: "deny_session".to_owned(),
            decision: ApprovalDecision::Deny,
            decision_scope: ApprovalDecisionScope::Session,
            decision_scope_ttl_ms: None,
        };
        state.remember_tool_approval(&context, "session-1", "tool:custom.noop", &outcome);
        let cached_before_reset =
            state.resolve_cached_tool_approval(&context, "session-1", "tool:custom.noop");
        assert!(
            cached_before_reset.is_some(),
            "session-scoped approval decision should be reused until session reset"
        );
        state.clear_tool_approval_cache_for_session(&context, "session-1");
        let cached_after_reset =
            state.resolve_cached_tool_approval(&context, "session-1", "tool:custom.noop");
        assert!(
            cached_after_reset.is_none(),
            "session reset should invalidate cached approval decisions"
        );
    }

    #[test]
    fn tool_approval_cache_expires_timeboxed_scope_entries() {
        let state = build_test_runtime_state(false);
        let context = RequestContext {
            principal: "user:ops".to_owned(),
            device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            channel: Some("cli".to_owned()),
        };
        let outcome = ToolApprovalOutcome {
            approval_id: "01ARZ3NDEKTSV4RRFFQ69G5FB2".to_owned(),
            approved: true,
            reason: "allow_timeboxed".to_owned(),
            decision: ApprovalDecision::Allow,
            decision_scope: ApprovalDecisionScope::Timeboxed,
            decision_scope_ttl_ms: Some(200),
        };
        state.remember_tool_approval(&context, "session-1", "tool:custom.noop", &outcome);
        assert!(
            state.resolve_cached_tool_approval(&context, "session-1", "tool:custom.noop").is_some(),
            "timeboxed approval should be immediately reusable before ttl expires"
        );
        std::thread::sleep(std::time::Duration::from_millis(250));
        assert!(
            state.resolve_cached_tool_approval(&context, "session-1", "tool:custom.noop").is_none(),
            "timeboxed approval should expire when ttl elapses"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn approval_list_pagination_keeps_next_cursor_at_page_limit() {
        let state = build_test_runtime_state(false);
        for index in 0..=MAX_APPROVAL_PAGE_LIMIT {
            state
                .create_approval_record(build_test_approval_request(index))
                .await
                .expect("approval create should succeed");
        }

        let (first_page, next_after) = state
            .list_approval_records(
                None,
                Some(MAX_APPROVAL_PAGE_LIMIT),
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .expect("first approvals page should succeed");
        assert_eq!(
            first_page.len(),
            MAX_APPROVAL_PAGE_LIMIT,
            "first page should respect requested page size"
        );
        let next_after =
            next_after.expect("pagination should expose next cursor when more records exist");

        let (second_page, second_next_after) = state
            .list_approval_records(
                Some(next_after),
                Some(MAX_APPROVAL_PAGE_LIMIT),
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .expect("second approvals page should succeed");
        assert_eq!(second_page.len(), 1, "sentinel pagination should return remaining records");
        assert!(
            second_next_after.is_none(),
            "second page should not expose a cursor after returning the final record"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn approval_list_zero_limit_uses_default_page_size() {
        let state = build_test_runtime_state(false);
        for index in 0..3 {
            state
                .create_approval_record(build_test_approval_request(index))
                .await
                .expect("approval create should succeed");
        }

        let (records, next_after) = state
            .list_approval_records(None, Some(0), None, None, None, None, None, None)
            .await
            .expect("list approvals with zero limit should succeed");
        assert_eq!(
            records.len(),
            3,
            "zero limit should use the default page size instead of returning a single record"
        );
        assert!(
            next_after.is_none(),
            "default page should not expose pagination cursor when all records are returned"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn best_effort_mark_approval_error_resolves_pending_record() {
        let state = build_test_runtime_state(false);
        let created = state
            .create_approval_record(build_test_approval_request(0))
            .await
            .expect("approval create should succeed");
        assert!(created.decision.is_none(), "freshly created approval should start unresolved");

        best_effort_mark_approval_error(
            &state,
            created.approval_id.as_str(),
            "approval_request_dispatch_error: response channel closed".to_owned(),
        )
        .await;

        let resolved = state
            .approval_record(created.approval_id.clone())
            .await
            .expect("approval lookup should succeed")
            .expect("approval should exist");
        assert_eq!(
            resolved.decision,
            Some(ApprovalDecision::Error),
            "best-effort error marking should close the approval lifecycle"
        );
        assert!(
            resolved.resolved_at_unix_ms.is_some(),
            "resolved approval should include resolved timestamp"
        );
        assert!(
            resolved
                .decision_reason
                .as_deref()
                .unwrap_or_default()
                .contains("approval_request_dispatch_error"),
            "resolved approval should retain reason context"
        );
    }

    #[test]
    fn orchestrator_tape_snapshot_paginates_and_redacts_payloads() {
        let state = build_test_runtime_state(false);
        state
            .journal_store
            .upsert_orchestrator_session(&OrchestratorSessionUpsertRequest {
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned(),
                session_key: "session:test".to_owned(),
                session_label: Some("Test session".to_owned()),
                principal: "user:ops".to_owned(),
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                channel: Some("cli".to_owned()),
            })
            .expect("orchestrator session should be upserted");
        state
            .journal_store
            .start_orchestrator_run(&OrchestratorRunStartRequest {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned(),
            })
            .expect("orchestrator run should start");
        state
            .journal_store
            .append_orchestrator_tape_event(&OrchestratorTapeAppendRequest {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
                seq: 0,
                event_type: "status".to_owned(),
                payload_json: r#"{"kind":"accepted"}"#.to_owned(),
            })
            .expect("first tape event should persist");
        state
            .journal_store
            .append_orchestrator_tape_event(&OrchestratorTapeAppendRequest {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
                seq: 1,
                event_type: "tool_result".to_owned(),
                payload_json: r#"{"token":"secret-value","ok":true}"#.to_owned(),
            })
            .expect("second tape event should persist");

        let first_page = state
            .orchestrator_tape_snapshot_blocking("01ARZ3NDEKTSV4RRFFQ69G5FAX", None, Some(1))
            .expect("first tape page should succeed");
        assert_eq!(first_page.events.len(), 1);
        assert_eq!(first_page.events[0].seq, 0);
        assert_eq!(first_page.next_after_seq, Some(0));

        let second_page = state
            .orchestrator_tape_snapshot_blocking(
                "01ARZ3NDEKTSV4RRFFQ69G5FAX",
                first_page.next_after_seq,
                Some(2),
            )
            .expect("second tape page should succeed");
        assert_eq!(second_page.events.len(), 1);
        assert_eq!(second_page.events[0].seq, 1);
        assert!(
            !second_page.events[0].payload_json.contains("secret-value"),
            "tape snapshots must redact sensitive token values"
        );
        assert!(
            second_page.events[0].payload_json.contains("<redacted>"),
            "redacted marker should be present in tape payloads"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn memory_search_tool_channel_scope_requires_authenticated_channel_context() {
        let state = build_test_runtime_state(false);
        let input_json = br#"{"query":"incident summary","scope":"channel"}"#;
        let outcome = execute_memory_search_tool(
            &state,
            "user:ops",
            None,
            "01ARZ3NDEKTSV4RRFFQ69G5FAW",
            "01ARZ3NDEKTSV4RRFFQ69G5FB0",
            input_json,
        )
        .await;
        assert!(!outcome.success, "tool call should fail closed without channel context");
        assert!(
            outcome.error.contains("scope=channel requires authenticated channel context"),
            "error should explain fail-closed channel scope behavior"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn model_token_tape_compaction_stub_emits_marker_event() {
        let state = build_test_runtime_state(false);
        state
            .journal_store
            .upsert_orchestrator_session(&OrchestratorSessionUpsertRequest {
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned(),
                session_key: "session:test".to_owned(),
                session_label: Some("Test session".to_owned()),
                principal: "user:ops".to_owned(),
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                channel: Some("cli".to_owned()),
            })
            .expect("orchestrator session should be upserted");
        state
            .journal_store
            .start_orchestrator_run(&OrchestratorRunStartRequest {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned(),
            })
            .expect("orchestrator run should start");

        let mut tape_seq = 0_i64;
        super::compact_model_token_tape_stub(&state, "01ARZ3NDEKTSV4RRFFQ69G5FAX", &mut tape_seq)
            .await
            .expect("compaction stub should append marker tape event");
        assert_eq!(tape_seq, 1);

        let tape = state
            .journal_store
            .orchestrator_tape("01ARZ3NDEKTSV4RRFFQ69G5FAX")
            .expect("orchestrator tape should be queryable");
        assert_eq!(tape.len(), 1);
        assert_eq!(tape[0].event_type, "model_token_compaction");
        assert!(
            tape[0].payload_json.contains("token_cap_reached"),
            "marker payload should describe compaction trigger"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn workspace_patch_tool_applies_patch_and_emits_attested_hashes() {
        let state = build_test_runtime_state(false);
        let created = state
            .create_agent(AgentCreateRequest {
                agent_id: "patcher".to_owned(),
                display_name: "Patcher".to_owned(),
                agent_dir: None,
                workspace_roots: Vec::new(),
                default_model_profile: None,
                default_tool_allowlist: Vec::new(),
                default_skill_allowlist: Vec::new(),
                set_default: true,
                allow_absolute_paths: false,
            })
            .await
            .expect("agent should be created");
        let workspace = PathBuf::from(&created.agent.workspace_roots[0]);
        fs::write(workspace.join("notes.txt"), "alpha\nbeta\n")
            .expect("seed file should be written");

        let patch = "*** Begin Patch\n*** Update File: notes.txt\n@@\n-beta\n+beta-updated\n*** Add File: new.txt\n+hello\n*** End Patch\n";
        let input_json =
            serde_json::to_vec(&json!({ "patch": patch })).expect("patch input should serialize");
        let outcome = execute_workspace_patch_tool(
            &state,
            "user:ops",
            Some("cli"),
            "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "01ARZ3NDEKTSV4RRFFQ69G5FB1",
            input_json.as_slice(),
        )
        .await;
        assert!(outcome.success, "patch tool should apply valid patch");

        let payload: Value =
            serde_json::from_slice(&outcome.output_json).expect("output should parse as JSON");
        let files = payload
            .get("files_touched")
            .and_then(Value::as_array)
            .expect("files_touched must be present");
        assert_eq!(files.len(), 2, "update + add should emit two file attestations");

        let notes = files
            .iter()
            .find(|entry| entry.get("path").and_then(Value::as_str) == Some("notes.txt"))
            .expect("notes.txt attestation should be present");
        let before_notes_hash = super::sha256_hex(b"alpha\nbeta\n");
        let after_notes_hash = super::sha256_hex(
            fs::read(workspace.join("notes.txt"))
                .expect("updated notes file should exist")
                .as_slice(),
        );
        assert_eq!(
            notes.get("before_sha256").and_then(Value::as_str),
            Some(before_notes_hash.as_str()),
            "before hash should match original file bytes"
        );
        assert_eq!(
            notes.get("after_sha256").and_then(Value::as_str),
            Some(after_notes_hash.as_str()),
            "after hash should match updated file bytes"
        );

        let created_file = files
            .iter()
            .find(|entry| entry.get("path").and_then(Value::as_str) == Some("new.txt"))
            .expect("new.txt attestation should be present");
        let created_file_hash = super::sha256_hex(
            fs::read(workspace.join("new.txt")).expect("new file should exist").as_slice(),
        );
        assert_eq!(
            created_file.get("before_sha256").and_then(Value::as_str),
            None,
            "new file attestation must not include before hash"
        );
        assert_eq!(
            created_file.get("after_sha256").and_then(Value::as_str),
            Some(created_file_hash.as_str()),
            "after hash should match newly created file"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn workspace_patch_tool_rejects_oversized_input_payload() {
        let state = build_test_runtime_state(false);
        let oversized = vec![b'a'; super::MAX_WORKSPACE_PATCH_TOOL_INPUT_BYTES + 1];
        let outcome = execute_workspace_patch_tool(
            &state,
            "user:ops",
            Some("cli"),
            "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "01ARZ3NDEKTSV4RRFFQ69G5FB2",
            oversized.as_slice(),
        )
        .await;
        assert!(!outcome.success, "oversized payload must be rejected");
        assert!(
            outcome.error.contains("input exceeds"),
            "error should describe payload size limit enforcement"
        );
    }

    #[test]
    fn parse_patch_string_array_field_validates_shape_limits_and_sizes() {
        let payload = json!({
            "redaction_patterns": ["token", "  ", "password"],
            "secret_file_markers": "invalid"
        });
        let object = payload.as_object().expect("payload should be object");

        let parsed = parse_patch_string_array_field(object, "redaction_patterns", 4, 16)
            .expect("string array should parse")
            .expect("field should be present");
        assert_eq!(
            parsed,
            vec!["token".to_owned(), "password".to_owned()],
            "blank entries should be ignored"
        );

        let type_error = parse_patch_string_array_field(object, "secret_file_markers", 4, 16)
            .expect_err("non-array field must be rejected");
        assert!(
            type_error.contains("must be an array of strings"),
            "error should explain expected array type"
        );

        let too_many = json!({ "redaction_patterns": ["a", "b", "c"] });
        let too_many_err = parse_patch_string_array_field(
            too_many.as_object().expect("payload should be object"),
            "redaction_patterns",
            2,
            16,
        )
        .expect_err("item count above limit must fail");
        assert!(too_many_err.contains("exceeds limit"));

        let too_large = json!({ "redaction_patterns": ["123456"] });
        let too_large_err = parse_patch_string_array_field(
            too_large.as_object().expect("payload should be object"),
            "redaction_patterns",
            4,
            4,
        )
        .expect_err("oversized entry must fail");
        assert!(too_large_err.contains("must be <="));
    }

    #[test]
    fn workspace_patch_redaction_policy_merge_preserves_defaults_for_empty_overrides() {
        let mut policy = super::WorkspacePatchRedactionPolicy::default();
        let original_patterns = policy.redaction_patterns.clone();
        let original_markers = policy.secret_file_markers.clone();

        extend_patch_string_defaults(&mut policy.redaction_patterns, Vec::new());
        extend_patch_string_defaults(&mut policy.secret_file_markers, Vec::new());

        assert_eq!(
            policy.redaction_patterns, original_patterns,
            "empty redaction pattern overrides must not disable default patterns"
        );
        assert_eq!(
            policy.secret_file_markers, original_markers,
            "empty secret marker overrides must not disable default markers"
        );
    }

    #[test]
    fn workspace_patch_redaction_policy_merge_adds_only_unique_values() {
        let mut policy = super::WorkspacePatchRedactionPolicy::default();
        let original_pattern_len = policy.redaction_patterns.len();
        let original_marker_len = policy.secret_file_markers.len();

        extend_patch_string_defaults(
            &mut policy.redaction_patterns,
            vec!["token".to_owned(), "custom-pattern".to_owned(), "custom-pattern".to_owned()],
        );
        extend_patch_string_defaults(
            &mut policy.secret_file_markers,
            vec![".env".to_owned(), "custom.marker".to_owned(), "custom.marker".to_owned()],
        );

        assert_eq!(
            policy.redaction_patterns.len(),
            original_pattern_len + 1,
            "only one unique redaction pattern should be appended"
        );
        assert_eq!(
            policy.secret_file_markers.len(),
            original_marker_len + 1,
            "only one unique secret marker should be appended"
        );
        assert_eq!(
            policy
                .redaction_patterns
                .iter()
                .filter(|value| value.as_str() == "custom-pattern")
                .count(),
            1,
            "custom redaction pattern should appear once"
        );
        assert_eq!(
            policy
                .secret_file_markers
                .iter()
                .filter(|value| value.as_str() == "custom.marker")
                .count(),
            1,
            "custom secret marker should appear once"
        );
    }

    #[test]
    fn workspace_patch_metrics_from_output_extracts_files_and_rollback() {
        let output = json!({
            "files_touched": [{"path": "a.txt"}, {"path": "b.txt"}],
            "rollback_performed": true
        });
        let serialized = serde_json::to_vec(&output).expect("metrics payload should serialize");
        assert_eq!(workspace_patch_metrics_from_output(&serialized), (2, true));
        assert_eq!(
            workspace_patch_metrics_from_output(b"{\"files_touched\":\"invalid\"}"),
            (0, false)
        );
    }

    #[test]
    fn classify_sandbox_escape_attempt_identifies_expected_categories() {
        assert_eq!(
            super::classify_sandbox_escape_attempt(
                "sandbox denied: path traversal is blocked for '../outside.txt'"
            ),
            Some(super::SandboxEscapeAttemptType::Workspace)
        );
        assert_eq!(
            super::classify_sandbox_escape_attempt(
                "sandbox denied: egress host 'blocked.example' is not allowlisted"
            ),
            Some(super::SandboxEscapeAttemptType::Egress)
        );
        assert_eq!(
            super::classify_sandbox_escape_attempt(
                "sandbox denied: executable 'cargo' is not allowlisted for process runner"
            ),
            Some(super::SandboxEscapeAttemptType::Executable)
        );
        assert_eq!(
            super::classify_sandbox_escape_attempt("sandbox process exited unsuccessfully"),
            None
        );
    }

    #[test]
    fn approval_risk_for_tier_c_read_only_process_command_is_reduced() {
        let config = crate::tool_protocol::ToolCallConfig {
            allowed_tools: vec![super::PROCESS_RUNNER_TOOL_NAME.to_owned()],
            max_calls_per_run: 1,
            execution_timeout_ms: 250,
            process_runner: crate::sandbox_runner::SandboxProcessRunnerPolicy {
                enabled: true,
                tier: crate::sandbox_runner::SandboxProcessRunnerTier::C,
                workspace_root: PathBuf::from("."),
                allowed_executables: vec!["uname".to_owned()],
                allow_interpreters: false,
                egress_enforcement_mode: crate::sandbox_runner::EgressEnforcementMode::Strict,
                allowed_egress_hosts: Vec::new(),
                allowed_dns_suffixes: Vec::new(),
                cpu_time_limit_ms: 2_000,
                memory_limit_bytes: 128 * 1024 * 1024,
                max_output_bytes: 64 * 1024,
            },
            wasm_runtime: crate::wasm_plugin_runner::WasmPluginRunnerPolicy {
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
            },
        };
        let risk = super::approval_risk_for_tool(
            super::PROCESS_RUNNER_TOOL_NAME,
            br#"{"command":"uname","args":["-a"]}"#,
            &config,
        );
        assert_eq!(risk, ApprovalRiskLevel::Medium);
    }

    #[test]
    fn approval_risk_for_tier_b_process_command_remains_high() {
        let config = crate::tool_protocol::ToolCallConfig {
            allowed_tools: vec![super::PROCESS_RUNNER_TOOL_NAME.to_owned()],
            max_calls_per_run: 1,
            execution_timeout_ms: 250,
            process_runner: crate::sandbox_runner::SandboxProcessRunnerPolicy {
                enabled: true,
                tier: crate::sandbox_runner::SandboxProcessRunnerTier::B,
                workspace_root: PathBuf::from("."),
                allowed_executables: vec!["uname".to_owned()],
                allow_interpreters: false,
                egress_enforcement_mode: crate::sandbox_runner::EgressEnforcementMode::Strict,
                allowed_egress_hosts: Vec::new(),
                allowed_dns_suffixes: Vec::new(),
                cpu_time_limit_ms: 2_000,
                memory_limit_bytes: 128 * 1024 * 1024,
                max_output_bytes: 64 * 1024,
            },
            wasm_runtime: crate::wasm_plugin_runner::WasmPluginRunnerPolicy {
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
            },
        };
        let risk = super::approval_risk_for_tool(
            super::PROCESS_RUNNER_TOOL_NAME,
            br#"{"command":"uname","args":["-a"]}"#,
            &config,
        );
        assert_eq!(risk, ApprovalRiskLevel::High);
    }

    fn canvas_test_context() -> super::RequestContext {
        super::RequestContext {
            principal: "user:ops".to_owned(),
            device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            channel: Some("cli".to_owned()),
        }
    }

    fn canvas_test_bundle(entrypoint_source: &[u8]) -> super::gateway_v1::CanvasBundle {
        super::gateway_v1::CanvasBundle {
            bundle_id: "demo".to_owned(),
            entrypoint_path: "app.js".to_owned(),
            assets: vec![super::gateway_v1::CanvasAsset {
                path: "app.js".to_owned(),
                content_type: "application/javascript".to_owned(),
                body: entrypoint_source.to_vec(),
            }],
            sha256: String::new(),
            signature: String::new(),
        }
    }

    #[test]
    fn canvas_lifecycle_supports_secure_render_and_state_updates() {
        let state = build_test_runtime_state(false);
        let context = canvas_test_context();
        let malicious_state = br#"{"content":"<img src=x onerror=alert('xss')>"}"#;
        let (created, descriptor) = state
            .create_canvas(
                &context,
                None,
                "01ARZ3NDEKTSV4RRFFQ69G5FAA".to_owned(),
                malicious_state,
                1,
                None,
                canvas_test_bundle(br#"window.addEventListener('palyra:canvas-state', () => {});"#),
                vec!["https://console.example.com".to_owned()],
                Some(600),
            )
            .expect("canvas create should succeed");

        let frame = state
            .canvas_frame_document(created.canvas_id.as_str(), descriptor.auth_token.as_str())
            .expect("frame render should succeed");
        assert!(
            frame.csp.contains("sandbox allow-scripts"),
            "canvas frame must enforce CSP sandbox restrictions"
        );
        assert!(
            frame.csp.contains("frame-ancestors https://console.example.com"),
            "canvas frame must enforce strict frame-ancestors origin policy"
        );
        assert!(
            !frame.html.contains("<img src=x onerror=alert('xss')>"),
            "frame template must not render state payload as raw HTML"
        );
        let runtime_script = state
            .canvas_runtime_script(created.canvas_id.as_str(), descriptor.auth_token.as_str())
            .expect("runtime script render should succeed");
        let runtime_body =
            String::from_utf8(runtime_script.body).expect("runtime JS should be utf8");
        assert!(
            runtime_body.contains("textContent = JSON.stringify"),
            "runtime script must render state via textContent to avoid script execution"
        );
        assert!(
            !runtime_body.contains("innerHTML"),
            "runtime script must not use innerHTML for untrusted state"
        );

        let updated = state
            .update_canvas_state(
                &context,
                created.canvas_id.as_str(),
                Some(br#"{"content":"updated"}"#.as_slice()),
                None,
                Some(created.state_version),
                None,
            )
            .expect("canvas update should succeed");
        assert_eq!(
            updated.state_version,
            created.state_version + 1,
            "canvas update should advance state version"
        );
        let refreshed = state
            .canvas_state(
                updated.canvas_id.as_str(),
                descriptor.auth_token.as_str(),
                Some(created.state_version),
            )
            .expect("state lookup should succeed")
            .expect("state lookup should return newer state");
        assert_eq!(
            refreshed.state.get("content").and_then(Value::as_str),
            Some("updated"),
            "refreshed state should expose latest JSON payload"
        );
        assert!(
            state
                .canvas_state(
                    updated.canvas_id.as_str(),
                    descriptor.auth_token.as_str(),
                    Some(updated.state_version),
                )
                .expect("state poll should succeed")
                .is_none(),
            "state polling should return no payload when caller already has latest version"
        );

        let closed = state
            .close_canvas(&context, updated.canvas_id.as_str(), Some("operator_close".to_owned()))
            .expect("canvas close should succeed");
        assert!(closed.closed, "canvas close should mark canvas as closed");
        let close_update_error = state
            .update_canvas_state(
                &context,
                updated.canvas_id.as_str(),
                Some(br#"{"content":"late"}"#.as_slice()),
                None,
                None,
                None,
            )
            .expect_err("closed canvas should reject updates");
        assert_eq!(close_update_error.code(), Code::FailedPrecondition);
    }

    #[test]
    fn canvas_rejects_out_of_bounds_payloads() {
        let state = build_test_runtime_state(false);
        let context = canvas_test_context();
        let oversized_state = vec![b'a'; state.config.canvas_host.max_state_bytes + 1];
        let create_error = state
            .create_canvas(
                &context,
                None,
                "01ARZ3NDEKTSV4RRFFQ69G5FAB".to_owned(),
                oversized_state.as_slice(),
                1,
                None,
                canvas_test_bundle(br#"console.log("ok");"#),
                vec!["https://console.example.com".to_owned()],
                Some(600),
            )
            .expect_err("oversized create payload should fail");
        assert_eq!(create_error.code(), Code::ResourceExhausted);

        let (created, _descriptor) = state
            .create_canvas(
                &context,
                None,
                "01ARZ3NDEKTSV4RRFFQ69G5FAC".to_owned(),
                br#"{"content":"ok"}"#,
                1,
                None,
                canvas_test_bundle(br#"console.log("ok");"#),
                vec!["https://console.example.com".to_owned()],
                Some(600),
            )
            .expect("baseline canvas create should succeed");
        let oversized_update = vec![b'a'; state.config.canvas_host.max_state_bytes + 1];
        let update_error = state
            .update_canvas_state(
                &context,
                created.canvas_id.as_str(),
                Some(oversized_update.as_slice()),
                None,
                None,
                None,
            )
            .expect_err("oversized update payload should fail");
        assert_eq!(update_error.code(), Code::ResourceExhausted);
    }

    #[test]
    fn canvas_rejects_oversized_bundle_and_missing_origin_allowlist() {
        let state = build_test_runtime_state(false);
        let context = canvas_test_context();
        let mut oversized_bundle = canvas_test_bundle(br#"console.log("ok");"#);
        oversized_bundle.assets = vec![super::gateway_v1::CanvasAsset {
            path: "app.js".to_owned(),
            content_type: "application/javascript".to_owned(),
            body: vec![b'a'; state.config.canvas_host.max_bundle_bytes + 1],
        }];
        let oversized_bundle_error = state
            .create_canvas(
                &context,
                None,
                "01ARZ3NDEKTSV4RRFFQ69G5FAD".to_owned(),
                br#"{"content":"ok"}"#,
                1,
                None,
                oversized_bundle,
                vec!["https://console.example.com".to_owned()],
                Some(600),
            )
            .expect_err("oversized bundle should fail");
        assert_eq!(oversized_bundle_error.code(), Code::ResourceExhausted);

        let missing_origin_error = state
            .create_canvas(
                &context,
                None,
                "01ARZ3NDEKTSV4RRFFQ69G5FAE".to_owned(),
                br#"{"content":"ok"}"#,
                1,
                None,
                canvas_test_bundle(br#"console.log("ok");"#),
                Vec::new(),
                Some(600),
            )
            .expect_err("missing origin allowlist should fail");
        assert_eq!(missing_origin_error.code(), Code::InvalidArgument);
    }

    #[test]
    fn canvas_patch_updates_are_replayable_and_deterministic() {
        let state = build_test_runtime_state(false);
        let context = canvas_test_context();
        let (created, _descriptor) = state
            .create_canvas(
                &context,
                None,
                "01ARZ3NDEKTSV4RRFFQ69G5FAF".to_owned(),
                br#"{"counter":1,"items":[]}"#,
                1,
                None,
                canvas_test_bundle(br#"console.log("ok");"#),
                vec!["https://console.example.com".to_owned()],
                Some(600),
            )
            .expect("canvas create should succeed");

        let patched = state
            .update_canvas_state(
                &context,
                created.canvas_id.as_str(),
                None,
                Some(
                    br#"{"v":1,"ops":[{"op":"replace","path":"/counter","value":2},{"op":"add","path":"/items/0","value":"alpha"}]}"#
                        .as_slice(),
                ),
                Some(created.state_version),
                Some(created.state_schema_version),
            )
            .expect("patch update should succeed");
        assert_eq!(patched.state_version, created.state_version + 1);

        let replayed = state
            .journal_store
            .replay_canvas_state(created.canvas_id.as_str())
            .expect("canvas replay should succeed")
            .expect("canvas replay should return state");
        assert_eq!(
            replayed.state_json, r#"{"counter":2,"items":["alpha"]}"#,
            "replay should reconstruct deterministic final state"
        );
        assert_eq!(replayed.state_version, patched.state_version);
    }

    #[test]
    fn canvas_update_rejects_version_conflict() {
        let state = build_test_runtime_state(false);
        let context = canvas_test_context();
        let (created, _descriptor) = state
            .create_canvas(
                &context,
                None,
                "01ARZ3NDEKTSV4RRFFQ69G5FAG".to_owned(),
                br#"{"content":"ok"}"#,
                1,
                None,
                canvas_test_bundle(br#"console.log("ok");"#),
                vec!["https://console.example.com".to_owned()],
                Some(600),
            )
            .expect("canvas create should succeed");

        let conflict = state
            .update_canvas_state(
                &context,
                created.canvas_id.as_str(),
                Some(br#"{"content":"next"}"#.as_slice()),
                None,
                Some(created.state_version + 7),
                None,
            )
            .expect_err("stale expected state version should be rejected");
        assert_eq!(conflict.code(), Code::FailedPrecondition);
    }

    #[test]
    fn canvas_update_rejects_oversized_patch_payload() {
        let state = build_test_runtime_state(false);
        let context = canvas_test_context();
        let (created, _descriptor) = state
            .create_canvas(
                &context,
                None,
                "01ARZ3NDEKTSV4RRFFQ69G5FAH".to_owned(),
                br#"{"content":"ok"}"#,
                1,
                None,
                canvas_test_bundle(br#"console.log("ok");"#),
                vec!["https://console.example.com".to_owned()],
                Some(600),
            )
            .expect("canvas create should succeed");
        let oversized_patch = vec![b'a'; state.config.canvas_host.max_state_bytes + 1];
        let error = state
            .update_canvas_state(
                &context,
                created.canvas_id.as_str(),
                None,
                Some(oversized_patch.as_slice()),
                Some(created.state_version),
                Some(created.state_schema_version),
            )
            .expect_err("oversized patch payload must be rejected");
        assert_eq!(error.code(), Code::ResourceExhausted);
    }
}
