use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, RwLock,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::http::{header::AUTHORIZATION, HeaderMap};
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
use serde::Serialize;
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
    cron::{normalize_schedule, schedule_to_proto, trigger_job_now, CronTimezoneMode},
    journal::{
        ApprovalCreateRequest, ApprovalDecision, ApprovalDecisionScope, ApprovalPolicySnapshot,
        ApprovalPromptOption, ApprovalPromptRecord, ApprovalRecord, ApprovalResolveRequest,
        ApprovalRiskLevel, ApprovalSubjectType, ApprovalsListFilter, CronConcurrencyPolicy,
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
    }
}

use proto::palyra::{
    auth::v1 as auth_v1, common::v1 as common_v1, cron::v1 as cron_v1, gateway::v1 as gateway_v1,
    memory::v1 as memory_v1,
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
    pub tool_call: ToolCallConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MemoryRuntimeConfig {
    pub max_item_bytes: usize,
    pub max_item_tokens: usize,
    pub auto_inject_enabled: bool,
    pub auto_inject_max_items: usize,
    pub default_ttl_ms: Option<i64>,
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
    tool_approval_cache: Mutex<HashMap<String, CachedToolApprovalDecision>>,
    vault_rate_limit: Mutex<HashMap<String, VaultRateLimitEntry>>,
    agent_registry: AgentRegistry,
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
            },
            journal_store,
            revoked_certificate_count,
            model_provider,
            vault,
            memory_config: RwLock::new(MemoryRuntimeConfig::default()),
            memory_search_cache: Mutex::new(HashMap::new()),
            tool_approval_cache: Mutex::new(HashMap::new()),
            vault_rate_limit: Mutex::new(HashMap::new()),
            agent_registry,
        }))
    }

    pub fn record_denied(&self) {
        self.counters.denied_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_admin_status_request(&self) {
        self.counters.admin_status_requests.fetch_add(1, Ordering::Relaxed);
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
    if principal.to_ascii_lowercase().starts_with("admin:") {
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
    let normalized_principal = principal.to_ascii_lowercase();
    if normalized_principal.starts_with("admin:") || normalized_principal.starts_with("system:") {
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
    if let (Some(context_channel), Some(item_channel)) = (channel, item.channel.as_deref()) {
        if context_channel != item_channel {
            return Err(Status::permission_denied("memory item channel does not match context"));
        }
    }
    Ok(())
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
        content_text: item.content_text.clone(),
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
        snippet: hit.snippet.clone(),
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
        let _context = self.authorize_rpc(request.metadata(), "ListApprovals")?;
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
        let _context = self.authorize_rpc(request.metadata(), "GetApproval")?;
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
        let _context = self.authorize_rpc(request.metadata(), "ExportApprovals")?;
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
    json!({
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
    .to_string()
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

    let payload = json!({
        "hits": search_hits.iter().map(|hit| {
            json!({
                "memory_id": hit.item.memory_id,
                "source": hit.item.source.as_str(),
                "snippet": hit.snippet,
                "score": hit.score,
                "created_at_unix_ms": hit.item.created_at_unix_ms,
                "content_text": hit.item.content_text,
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
    });
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
            redaction_policy.redaction_patterns = patterns;
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
            redaction_policy.secret_file_markers = markers;
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
        risk_level: ApprovalRiskLevel::High,
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
                "reason": outcome.reason,
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
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use axum::http::{header::AUTHORIZATION, HeaderMap, HeaderValue};
    use serde_json::{json, Value};

    use crate::agents::AgentCreateRequest;
    use crate::journal::{
        ApprovalCreateRequest, ApprovalDecision, ApprovalDecisionScope, ApprovalPolicySnapshot,
        ApprovalPromptOption, ApprovalPromptRecord, ApprovalRiskLevel, ApprovalSubjectType,
        JournalAppendRequest, JournalConfig, JournalStore, OrchestratorRunStartRequest,
        OrchestratorSessionUpsertRequest, OrchestratorTapeAppendRequest,
    };
    use ulid::Ulid;

    use super::{
        apply_tool_approval_outcome, authorize_headers, best_effort_mark_approval_error,
        constant_time_eq, enforce_vault_get_approval_policy, enforce_vault_scope_access,
        execute_memory_search_tool, execute_workspace_patch_tool, parse_patch_string_array_field,
        request_context_from_headers, resolve_cron_job_channel_for_create,
        vault_get_requires_approval, workspace_patch_metrics_from_output, AuthError,
        GatewayAuthConfig, GatewayJournalConfigSnapshot, GatewayRuntimeConfigSnapshot,
        GatewayRuntimeState, MemoryRuntimeConfig, ProviderRequest, RequestContext,
        ToolApprovalOutcome, HEADER_CHANNEL, HEADER_DEVICE_ID, HEADER_PRINCIPAL,
        MAX_APPROVAL_PAGE_LIMIT, VAULT_RATE_LIMIT_MAX_PRINCIPAL_BUCKETS,
        VAULT_RATE_LIMIT_MAX_REQUESTS_PER_WINDOW,
    };

    static TEMP_JOURNAL_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn unique_temp_journal_path() -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        let counter = TEMP_JOURNAL_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir()
            .join(format!("palyra-gateway-unit-{nonce}-{}-{counter}.sqlite3", std::process::id()))
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
                tool_call: crate::tool_protocol::ToolCallConfig {
                    allowed_tools: vec!["palyra.echo".to_owned()],
                    max_calls_per_run: 4,
                    execution_timeout_ms: 250,
                    process_runner: crate::sandbox_runner::SandboxProcessRunnerPolicy {
                        enabled: false,
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
}
