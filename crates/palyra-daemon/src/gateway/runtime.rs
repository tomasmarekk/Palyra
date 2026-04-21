use super::*;
use crate::agents::{
    AgentBindingOutcome, AgentBindingQuery, AgentBindingRequest, AgentDeleteOutcome, AgentListPage,
    AgentRecord, AgentResolveOutcome, AgentResolveRequest, AgentSetDefaultOutcome,
    AgentUnbindOutcome, AgentUnbindRequest, SessionAgentBinding,
};
use crate::application::auth::map_auth_profile_error;
use crate::journal::{
    LearningCandidateCreateRequest, LearningCandidateHistoryRecord, LearningCandidateListFilter,
    LearningCandidateRecord, LearningCandidateReviewRequest, LearningPreferenceListFilter,
    LearningPreferenceRecord, LearningPreferenceUpsertRequest, MemoryEmbeddingsStatus,
    MemoryItemRecord, OrchestratorBackgroundTaskCreateRequest,
    OrchestratorBackgroundTaskListFilter, OrchestratorBackgroundTaskRecord,
    OrchestratorBackgroundTaskUpdateRequest, OrchestratorCheckpointCreateRequest,
    OrchestratorCheckpointRecord, OrchestratorCheckpointRestoreMarkRequest,
    OrchestratorCompactionArtifactCreateRequest, OrchestratorCompactionArtifactRecord,
    OrchestratorQueuedInputCreateRequest, OrchestratorQueuedInputRecord,
    OrchestratorQueuedInputUpdateRequest, OrchestratorRunMetadataUpdateRequest,
    OrchestratorSessionCleanupOutcome, OrchestratorSessionCleanupRequest,
    OrchestratorSessionLineageUpdateRequest, OrchestratorSessionPinCreateRequest,
    OrchestratorSessionPinRecord, OrchestratorSessionQueueControlRecord,
    OrchestratorSessionQueueControlUpdateRequest, OrchestratorSessionRecord,
    OrchestratorSessionTitleUpdateRequest, OrchestratorSessionTranscriptRecord,
    OrchestratorUsageQuery, OrchestratorUsageRunRecord, OrchestratorUsageSessionRecord,
    OrchestratorUsageSummary, RetrievalBranchDiagnostics, SessionProjectContextStateCopyRequest,
    SessionProjectContextStateRecord, SessionProjectContextStateUpsertRequest,
    WorkspaceBootstrapOutcome, WorkspaceBootstrapRequest, WorkspaceCheckpointCreateRequest,
    WorkspaceCheckpointFilePayload, WorkspaceCheckpointFileRecord, WorkspaceCheckpointListFilter,
    WorkspaceCheckpointRecord, WorkspaceCheckpointRestoreMarkRequest,
    WorkspaceDocumentDeleteRequest, WorkspaceDocumentListFilter, WorkspaceDocumentMoveRequest,
    WorkspaceDocumentRecord, WorkspaceDocumentVersionRecord, WorkspaceDocumentWriteRequest,
    WorkspaceRestoreActivityFilter, WorkspaceRestoreActivitySummary,
    WorkspaceRestoreReportCreateRequest, WorkspaceRestoreReportListFilter,
    WorkspaceRestoreReportRecord, WorkspaceSearchHit, WorkspaceSearchRequest,
};
use crate::provider_leases::{
    ProviderLeaseAcquireError, ProviderLeaseAcquireRequest, ProviderLeaseExecutionContext,
    ProviderLeaseManager, ProviderLeaseManagerSnapshot, ProviderLeasePreviewRequest,
    ProviderLeasePreviewSnapshot,
};
use crate::retrieval::{
    score_memory_candidates, score_workspace_candidates, RetrievalBackend,
    RetrievalBackendSnapshot, RetrievalRuntimeConfig,
};
use crate::self_healing::{
    IncidentDomain, RemediationAttemptStatus, RuntimeIncidentHistoryEntry,
    RuntimeIncidentObservation, RuntimeIncidentRecord, RuntimeIncidentSummary,
    RuntimeRemediationAttemptRecord, SelfHealingFeature, SelfHealingSettingsSnapshot,
    SelfHealingState, WorkHeartbeatKind, WorkHeartbeatRecord, WorkHeartbeatUpdate,
};
use crate::tool_posture::{
    ToolPostureAuditEventRecord, ToolPostureOverrideClearRequest, ToolPostureOverrideRecord,
    ToolPostureOverrideUpsertRequest, ToolPostureRecommendationActionRecord,
    ToolPostureRecommendationActionRequest, ToolPostureRegistry, ToolPostureScopeResetRequest,
};
use crate::usage_governance::SmartRoutingRuntimeConfig;
use palyra_auth::AuthHealthReport;
use palyra_common::runtime_preview::{
    RuntimeDecisionActor, RuntimeDecisionActorKind, RuntimeDecisionPayload,
};
use palyra_workerd::{
    WorkerAttestation, WorkerCleanupReport, WorkerFleetManager, WorkerFleetPolicy,
    WorkerFleetSnapshot, WorkerLease, WorkerLeaseRequest, WorkerLifecycleEvent,
};
use std::path::PathBuf;
use tokio::sync::Notify;

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
    pub feature_rollouts: crate::config::FeatureRolloutsConfig,
    pub session_queue_policy: crate::config::SessionQueuePolicyConfig,
    pub pruning_policy_matrix: crate::config::PruningPolicyMatrixConfig,
    pub retrieval_dual_path: crate::config::RetrievalDualPathConfig,
    pub auxiliary_executor: crate::config::AuxiliaryExecutorConfig,
    pub flow_orchestration: crate::config::FlowOrchestrationConfig,
    pub delivery_arbitration: crate::config::DeliveryArbitrationConfig,
    pub replay_capture: crate::config::ReplayCaptureConfig,
    pub networked_workers: crate::config::NetworkedWorkersConfig,
    pub channel_router: ChannelRouterConfig,
    pub media: MediaRuntimeConfig,
    pub tool_call: ToolCallConfig,
    pub http_fetch: HttpFetchRuntimeConfig,
    pub browser_service: BrowserServiceRuntimeConfig,
    pub canvas_host: CanvasHostRuntimeConfig,
    pub smart_routing: SmartRoutingRuntimeConfig,
}

#[derive(Debug, Clone)]
pub struct ListOrchestratorSessionsRequest {
    pub after_session_key: Option<String>,
    pub principal: String,
    pub device_id: String,
    pub channel: Option<String>,
    pub include_archived: bool,
    pub requested_limit: Option<usize>,
    pub search_query: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ListPrincipalOrchestratorSessionsRequest {
    pub after_session_key: Option<String>,
    pub principal: String,
    pub include_archived: bool,
    pub requested_limit: Option<usize>,
    pub search_query: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MemoryRuntimeConfig {
    pub max_item_bytes: usize,
    pub max_item_tokens: usize,
    pub auto_inject_enabled: bool,
    pub auto_inject_max_items: usize,
    pub default_ttl_ms: Option<i64>,
    pub retention_max_entries: Option<usize>,
    pub retention_max_bytes: Option<u64>,
    pub retention_ttl_days: Option<u32>,
    pub retention_vacuum_schedule: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LearningRuntimeConfig {
    pub enabled: bool,
    pub sampling_percent: u8,
    pub cooldown_ms: i64,
    pub budget_tokens: u64,
    pub max_candidates_per_run: usize,
    pub durable_fact_review_min_confidence_bps: u16,
    pub durable_fact_auto_write_threshold_bps: u16,
    pub preference_review_min_confidence_bps: u16,
    pub procedure_min_occurrences: usize,
    pub procedure_review_min_confidence_bps: u16,
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
            retention_max_entries: None,
            retention_max_bytes: None,
            retention_ttl_days: None,
            retention_vacuum_schedule: "0 0 * * 0".to_owned(),
        }
    }
}

impl Default for LearningRuntimeConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            sampling_percent: 100,
            cooldown_ms: 5 * 60 * 1_000,
            budget_tokens: 1_200,
            max_candidates_per_run: 24,
            durable_fact_review_min_confidence_bps: 7_500,
            durable_fact_auto_write_threshold_bps: 9_000,
            preference_review_min_confidence_bps: 8_000,
            procedure_min_occurrences: 2,
            procedure_review_min_confidence_bps: 8_500,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ToolApprovalOutcome {
    pub(crate) approval_id: String,
    pub(crate) approved: bool,
    pub(crate) reason: String,
    pub(crate) decision: ApprovalDecision,
    pub(crate) decision_scope: ApprovalDecisionScope,
    pub(crate) decision_scope_ttl_ms: Option<i64>,
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
pub(crate) struct CachedHttpFetchEntry {
    pub(crate) expires_at_unix_ms: i64,
    pub(crate) output_json: Vec<u8>,
}

#[derive(Debug, Clone)]
pub(crate) struct CachedMemorySearchEntry {
    pub(crate) hits: Vec<MemorySearchHit>,
    pub(crate) expires_at_unix_ms: Option<i64>,
}

#[derive(Debug, Clone)]
pub(crate) struct MemorySearchOutcome {
    pub(crate) hits: Vec<MemorySearchHit>,
    pub(crate) diagnostics: RetrievalBranchDiagnostics,
}

#[derive(Debug, Clone)]
pub(crate) struct WorkspaceSearchOutcome {
    pub(crate) hits: Vec<WorkspaceSearchHit>,
    pub(crate) diagnostics: RetrievalBranchDiagnostics,
}

#[derive(Debug, Clone)]
pub(crate) struct ToolSkillContext {
    pub(crate) skill_id: String,
    pub(crate) version: Option<String>,
}

impl ToolSkillContext {
    pub(crate) fn new(skill_id: String, version: Option<String>) -> Self {
        Self { skill_id, version }
    }

    pub(crate) fn skill_id(&self) -> &str {
        self.skill_id.as_str()
    }

    pub(crate) fn version(&self) -> Option<&str> {
        self.version.as_deref()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RunStreamToolExecutionOutcome {
    Completed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CanvasAssetRecord {
    pub(crate) content_type: String,
    pub(crate) body: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CanvasBundleRecord {
    pub(crate) bundle_id: String,
    pub(crate) entrypoint_path: String,
    pub(crate) assets: HashMap<String, CanvasAssetRecord>,
    pub(crate) sha256: String,
    pub(crate) signature: String,
}

#[derive(Debug, Clone)]
pub(crate) struct CanvasRecord {
    pub(crate) canvas_id: String,
    pub(crate) session_id: String,
    pub(crate) principal: String,
    pub(crate) state_version: u64,
    pub(crate) state_schema_version: u64,
    pub(crate) state_json: Vec<u8>,
    pub(crate) bundle: CanvasBundleRecord,
    pub(crate) allowed_parent_origins: Vec<String>,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) updated_at_unix_ms: i64,
    pub(crate) expires_at_unix_ms: i64,
    pub(crate) closed: bool,
    pub(crate) close_reason: Option<String>,
    pub(crate) update_timestamps_unix_ms: VecDeque<i64>,
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

const CANVAS_PATCH_HISTORY_BATCH_LIMIT: usize = 1_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CanvasTokenPayload {
    pub(crate) canvas_id: String,
    pub(crate) principal: String,
    pub(crate) session_id: String,
    issued_at_unix_ms: i64,
    pub(crate) expires_at_unix_ms: i64,
    nonce: String,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct VaultRateLimitEntry {
    window_started_at: Instant,
    requests_in_window: u32,
}

#[derive(Debug, Clone)]
pub struct GatewayJournalConfigSnapshot {
    pub db_path: PathBuf,
    pub hash_chain_enabled: bool,
}

#[rustfmt::skip]
pub struct GatewayRuntimeDependencies { pub model_provider: Arc<dyn ModelProvider>, pub vault: Arc<Vault>, pub agent_registry: AgentRegistry, pub tool_posture_registry: ToolPostureRegistry, pub retrieval_backend: Arc<dyn RetrievalBackend> }

#[derive(Clone)]
pub(crate) struct RoutinesRuntimeConfig {
    pub registry: Arc<crate::routines::RoutineRegistry>,
    pub auth: GatewayAuthConfig,
    pub grpc_url: String,
    pub scheduler_wake: Arc<Notify>,
    pub timezone_mode: crate::cron::CronTimezoneMode,
}

pub struct GatewayRuntimeState {
    pub(crate) started_at: Instant,
    pub(crate) build: BuildSnapshot,
    pub(crate) config: GatewayRuntimeConfigSnapshot,
    pub(crate) journal_config: GatewayJournalConfigSnapshot,
    pub(crate) counters: RuntimeCounters,
    pub(crate) journal_store: JournalStore,
    revoked_certificate_count: usize,
    model_provider: Arc<dyn ModelProvider>,
    pub(crate) vault: Arc<Vault>,
    pub(crate) memory_config: RwLock<MemoryRuntimeConfig>,
    pub(crate) retrieval_config: RwLock<RetrievalRuntimeConfig>,
    pub(crate) learning_config: RwLock<LearningRuntimeConfig>,
    pub(crate) memory_search_cache: Mutex<HashMap<String, CachedMemorySearchEntry>>,
    pub(crate) http_fetch_cache: Mutex<HashMap<String, CachedHttpFetchEntry>>,
    tool_approval_cache: Mutex<HashMap<String, CachedToolApprovalDecision>>,
    worker_fleet: RwLock<WorkerFleetManager>,
    pub(crate) provider_leases: ProviderLeaseManager,
    pub(crate) retrieval_backend: Arc<dyn RetrievalBackend>,
    pub(crate) tool_posture_registry: ToolPostureRegistry,
    pub(crate) routines_runtime: RwLock<Option<RoutinesRuntimeConfig>>,
    pub(crate) vault_rate_limit: Mutex<HashMap<String, VaultRateLimitEntry>>,
    canvas_records: Mutex<HashMap<String, CanvasRecord>>,
    canvas_signing_secret: [u8; 32],
    agent_registry: AgentRegistry,
    pub(crate) channel_router: ChannelRouter,
    pub(crate) observability: Arc<crate::observability::ObservabilityState>,
    pub(crate) self_healing: Arc<SelfHealingState>,
}

#[derive(Debug)]
pub(crate) struct RuntimeCounters {
    pub(crate) run_stream_requests: AtomicU64,
    pub(crate) append_event_requests: AtomicU64,
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
    pub(crate) tool_decisions_allowed: AtomicU64,
    pub(crate) tool_decisions_denied: AtomicU64,
    tool_execution_attempts: AtomicU64,
    pub(crate) tool_execution_failures: AtomicU64,
    pub(crate) tool_execution_timeouts: AtomicU64,
    tool_attestations_emitted: AtomicU64,
    pub(crate) sandbox_launches: AtomicU64,
    pub(crate) sandbox_policy_denies: AtomicU64,
    pub(crate) sandbox_escape_attempts_blocked_workspace: AtomicU64,
    pub(crate) sandbox_escape_attempts_blocked_egress: AtomicU64,
    pub(crate) sandbox_escape_attempts_blocked_executable: AtomicU64,
    pub(crate) sandbox_backend_selected_tier_b: AtomicU64,
    pub(crate) sandbox_backend_selected_tier_c_linux_bubblewrap: AtomicU64,
    pub(crate) sandbox_backend_selected_tier_c_macos_sandbox_exec: AtomicU64,
    pub(crate) sandbox_backend_selected_tier_c_windows_job_object: AtomicU64,
    pub(crate) patches_applied: AtomicU64,
    pub(crate) patches_rejected: AtomicU64,
    pub(crate) patch_files_touched: AtomicU64,
    pub(crate) patch_rollbacks: AtomicU64,
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
    learning_reflections_scheduled: AtomicU64,
    learning_reflections_completed: AtomicU64,
    learning_candidates_created: AtomicU64,
    learning_candidates_auto_applied: AtomicU64,
    vault_put_requests: AtomicU64,
    vault_get_requests: AtomicU64,
    vault_delete_requests: AtomicU64,
    vault_list_requests: AtomicU64,
    vault_rate_limited_requests: AtomicU64,
    pub(crate) vault_access_audit_events: AtomicU64,
    skill_status_updates: AtomicU64,
    pub(crate) skill_execution_denied: AtomicU64,
    approvals_tool_requested: AtomicU64,
    approvals_tool_resolved_allow: AtomicU64,
    approvals_tool_resolved_deny: AtomicU64,
    approvals_tool_resolved_timeout: AtomicU64,
    approvals_tool_resolved_error: AtomicU64,
    agent_mutations: AtomicU64,
    agent_resolution_hits: AtomicU64,
    agent_resolution_misses: AtomicU64,
    pub(crate) agent_validation_failures: AtomicU64,
    pub(crate) channel_messages_inbound: AtomicU64,
    channel_messages_routed: AtomicU64,
    channel_messages_replied: AtomicU64,
    pub(crate) channel_messages_rejected: AtomicU64,
    pub(crate) channel_messages_queued: AtomicU64,
    pub(crate) channel_messages_quarantined: AtomicU64,
    pub(crate) channel_router_queue_depth: AtomicU64,
    channel_reply_failures: AtomicU64,
    canvas_created: AtomicU64,
    canvas_updated: AtomicU64,
    canvas_closed: AtomicU64,
    canvas_denied: AtomicU64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct BuildSnapshot {
    pub(crate) version: String,
    pub(crate) git_hash: String,
    pub(crate) build_profile: String,
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
    pub smart_routing_enabled: bool,
    pub smart_routing_default_mode: String,
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
    pub learning_reflections_scheduled: u64,
    pub learning_reflections_completed: u64,
    pub learning_candidates_created: u64,
    pub learning_candidates_auto_applied: u64,
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
    pub async fn refresh_oauth_profile(
        self: &Arc<Self>,
        profile_id: String,
        vault: Arc<Vault>,
    ) -> Result<OAuthRefreshOutcome, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let outcome = state
                .registry
                .refresh_oauth_profile(
                    profile_id.as_str(),
                    vault.as_ref(),
                    state.refresh_adapter.as_ref(),
                )
                .map_err(map_auth_profile_error)?;
            state.record_refresh_outcome(&outcome);
            Ok(outcome)
        })
        .await
        .map_err(|_| Status::internal("auth refresh worker panicked"))?
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

    #[allow(clippy::result_large_err)]
    pub(crate) async fn refresh_health_report(
        self: &Arc<Self>,
        runtime_state: Arc<GatewayRuntimeState>,
        agent_id: String,
    ) -> Result<(AuthHealthReport, Vec<OAuthRefreshOutcome>, AuthRefreshMetricsSnapshot), Status>
    {
        let state = Arc::clone(self);
        let agent_id_filter = non_empty(agent_id);
        tokio::task::spawn_blocking(move || {
            let outcomes = state
                .registry()
                .refresh_due_oauth_profiles(
                    runtime_state.vault.as_ref(),
                    state.refresh_adapter.as_ref(),
                    agent_id_filter.as_deref(),
                )
                .map_err(map_auth_profile_error)?;
            for outcome in &outcomes {
                state.record_refresh_outcome(outcome);
            }
            let report = state
                .registry()
                .health_report(runtime_state.vault.as_ref(), agent_id_filter.as_deref())
                .map_err(map_auth_profile_error)?;
            Ok::<_, Status>((report, outcomes, state.refresh_metrics_snapshot()))
        })
        .await
        .map_err(|_| Status::internal("auth health worker panicked"))?
    }
}

impl RuntimeCounters {
    pub(crate) fn snapshot(&self) -> CountersSnapshot {
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
            learning_reflections_scheduled: self
                .learning_reflections_scheduled
                .load(Ordering::Relaxed),
            learning_reflections_completed: self
                .learning_reflections_completed
                .load(Ordering::Relaxed),
            learning_candidates_created: self.learning_candidates_created.load(Ordering::Relaxed),
            learning_candidates_auto_applied: self
                .learning_candidates_auto_applied
                .load(Ordering::Relaxed),
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

fn complete_retrieval_diagnostics(
    mut diagnostics: RetrievalBranchDiagnostics,
    fusion_latency_ms: u64,
    fused_hit_count: u64,
    total_latency_ms: u64,
) -> RetrievalBranchDiagnostics {
    let latency_budget_ms = u64::try_from(MEMORY_SEARCH_LATENCY_BUDGET_MS).unwrap_or(u64::MAX);
    diagnostics.fusion_latency_ms = fusion_latency_ms;
    diagnostics.fused_hit_count = fused_hit_count;
    diagnostics.total_latency_ms = total_latency_ms;
    diagnostics.latency_budget_ms = latency_budget_ms;
    diagnostics.latency_budget_exceeded = total_latency_ms > latency_budget_ms;
    diagnostics
}

fn elapsed_millis(started_at: Instant) -> u64 {
    u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX)
}

impl GatewayRuntimeState {
    fn cached_memory_search_expires_at(hits: &[MemorySearchHit]) -> Option<i64> {
        hits.iter().filter_map(|hit| hit.item.ttl_unix_ms).min()
    }

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
        let tool_posture_root =
            std::env::temp_dir().join(format!("palyra-tool-posture-{}", Ulid::new()));
        let tool_posture_registry = ToolPostureRegistry::open(tool_posture_root.as_path())
            .expect("test tool posture registry should initialize");
        #[rustfmt::skip]
        let dependencies = GatewayRuntimeDependencies { model_provider: default_provider, vault: default_vault, agent_registry, tool_posture_registry, retrieval_backend: Arc::new(crate::retrieval::JournalRetrievalBackend) };
        Self::new_with_provider(
            config,
            journal_config,
            journal_store,
            revoked_certificate_count,
            dependencies,
        )
    }

    pub fn new_with_provider(
        config: GatewayRuntimeConfigSnapshot,
        journal_config: GatewayJournalConfigSnapshot,
        journal_store: JournalStore,
        revoked_certificate_count: usize,
        dependencies: GatewayRuntimeDependencies,
    ) -> Result<Arc<Self>, JournalError> {
        #[rustfmt::skip]
        let GatewayRuntimeDependencies { model_provider, vault, agent_registry, tool_posture_registry, retrieval_backend } = dependencies;
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
                learning_reflections_scheduled: AtomicU64::new(0),
                learning_reflections_completed: AtomicU64::new(0),
                learning_candidates_created: AtomicU64::new(0),
                learning_candidates_auto_applied: AtomicU64::new(0),
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
            retrieval_config: RwLock::new(RetrievalRuntimeConfig::default()),
            learning_config: RwLock::new(LearningRuntimeConfig::default()),
            memory_search_cache: Mutex::new(HashMap::new()),
            http_fetch_cache: Mutex::new(HashMap::new()),
            tool_approval_cache: Mutex::new(HashMap::new()),
            worker_fleet: RwLock::new(WorkerFleetManager::default()),
            provider_leases: ProviderLeaseManager::default(),
            retrieval_backend,
            tool_posture_registry,
            routines_runtime: RwLock::new(None),
            vault_rate_limit: Mutex::new(HashMap::new()),
            canvas_records: Mutex::new(recovered_canvas_records),
            canvas_signing_secret: generate_canvas_signing_secret(),
            agent_registry,
            channel_router,
            observability: Arc::new(crate::observability::ObservabilityState::default()),
            self_healing: Arc::new(SelfHealingState::new()),
        }))
    }

    pub fn record_denied(&self) {
        self.counters.denied_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_admin_status_request(&self) {
        self.counters.admin_status_requests.fetch_add(1, Ordering::Relaxed);
    }

    #[must_use]
    pub(crate) fn self_healing_settings_snapshot(&self) -> SelfHealingSettingsSnapshot {
        self.self_healing.settings_snapshot()
    }

    #[must_use]
    pub(crate) fn self_healing_incident_summary(&self) -> RuntimeIncidentSummary {
        self.self_healing.incident_summary()
    }

    #[must_use]
    pub(crate) fn self_healing_active_incidents(&self, limit: usize) -> Vec<RuntimeIncidentRecord> {
        self.self_healing.active_incidents(limit)
    }

    #[must_use]
    pub(crate) fn self_healing_recent_history(
        &self,
        limit: usize,
    ) -> Vec<RuntimeIncidentHistoryEntry> {
        self.self_healing.recent_incident_history(limit)
    }

    #[must_use]
    pub(crate) fn self_healing_recent_remediation_attempts(
        &self,
        limit: usize,
    ) -> Vec<RuntimeRemediationAttemptRecord> {
        self.self_healing.recent_remediation_attempts(limit)
    }

    #[must_use]
    pub(crate) fn self_healing_heartbeats(&self) -> Vec<WorkHeartbeatRecord> {
        self.self_healing.list_heartbeats()
    }

    pub(crate) fn record_self_healing_heartbeat(&self, update: WorkHeartbeatUpdate) {
        self.self_healing.record_heartbeat(update);
    }

    pub(crate) fn clear_self_healing_heartbeat(&self, kind: WorkHeartbeatKind, object_id: &str) {
        self.self_healing.clear_heartbeat(kind, object_id);
    }

    #[must_use]
    pub(crate) fn observe_self_healing_incident(
        &self,
        observation: RuntimeIncidentObservation,
    ) -> RuntimeIncidentRecord {
        self.self_healing.observe_incident(observation)
    }

    pub(crate) fn resolve_self_healing_incident(
        &self,
        domain: IncidentDomain,
        dedupe_key: &str,
        summary: &str,
    ) {
        self.self_healing.resolve_incident(domain, dedupe_key, summary);
    }

    #[must_use]
    pub(crate) fn record_self_healing_remediation_attempt(
        &self,
        incident_id: &str,
        remediation_id: &str,
        feature: SelfHealingFeature,
        status: RemediationAttemptStatus,
        detail: impl Into<String>,
    ) -> RuntimeRemediationAttemptRecord {
        self.self_healing.record_remediation_attempt(
            incident_id,
            remediation_id,
            feature,
            status,
            detail,
        )
    }

    pub(crate) fn record_channel_message_routed(&self) {
        self.counters.channel_messages_routed.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_channel_message_replied(&self) {
        self.counters.channel_messages_replied.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_channel_reply_failure(&self) {
        self.counters.channel_reply_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn refresh_channel_router_queue_depth(&self) {
        self.counters
            .channel_router_queue_depth
            .store(self.channel_router.queue_depth() as u64, Ordering::Relaxed);
    }

    pub(crate) fn record_tool_proposal(&self) {
        self.counters.tool_proposals.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_tool_execution_attempt(&self) {
        self.counters.tool_execution_attempts.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_tool_attestation_emitted(&self) {
        self.counters.tool_attestations_emitted.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_memory_auto_inject_event(&self) {
        self.counters.memory_auto_inject_events.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_learning_reflection_scheduled(&self) {
        self.counters.learning_reflections_scheduled.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_learning_reflection_completed(&self) {
        self.counters.learning_reflections_completed.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_learning_candidate_created(&self) {
        self.counters.learning_candidates_created.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_learning_candidate_auto_applied(&self) {
        self.counters.learning_candidates_auto_applied.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_tool_decision(&self, tool_name: &str, decision_allowed: bool) {
        if decision_allowed {
            self.counters.tool_decisions_allowed.fetch_add(1, Ordering::Relaxed);
            return;
        }

        self.counters.tool_decisions_denied.fetch_add(1, Ordering::Relaxed);
        self.record_denied();
        if tool_name == PROCESS_RUNNER_TOOL_NAME {
            self.counters.sandbox_policy_denies.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn record_skill_execution_denied(&self) {
        self.counters.skill_execution_denied.fetch_add(1, Ordering::Relaxed);
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
    pub(crate) fn create_canvas(
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
        ensure_canvas_version_fits_sqlite("state_version", state_version)?;
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
    pub(crate) fn update_canvas_state(
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
        if record.state_version >= MAX_CANVAS_SQLITE_VERSION {
            return Err(Status::failed_precondition(format!(
                "canvas state_version cannot advance beyond maximum supported value {MAX_CANVAS_SQLITE_VERSION}"
            )));
        }
        let next_state_version = record.state_version + 1;

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
    pub(crate) fn close_canvas(
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
            if record.state_version >= MAX_CANVAS_SQLITE_VERSION {
                return Err(Status::failed_precondition(format!(
                    "canvas state_version cannot advance beyond maximum supported value {MAX_CANVAS_SQLITE_VERSION}"
                )));
            }
            let next_state_version = record.state_version + 1;
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
    pub(crate) fn get_canvas(
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
    pub(crate) fn list_canvas_state_patches(
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
    pub(crate) fn list_session_canvases(
        &self,
        context: &RequestContext,
        session_id: &str,
    ) -> Result<Vec<CanvasRecord>, Status> {
        self.ensure_canvas_host_enabled()?;
        validate_canonical_id(session_id).map_err(|_| {
            Status::invalid_argument("session_id must be a canonical ULID identifier")
        })?;
        let records = self
            .canvas_records
            .lock()
            .map_err(|_| Status::internal("canvas registry lock poisoned"))?;
        let mut scoped = records
            .values()
            .filter(|record| {
                record.principal == context.principal && record.session_id.as_str() == session_id
            })
            .cloned()
            .collect::<Vec<_>>();
        scoped.sort_by(|left, right| {
            right
                .updated_at_unix_ms
                .cmp(&left.updated_at_unix_ms)
                .then_with(|| left.canvas_id.cmp(&right.canvas_id))
        });
        Ok(scoped)
    }

    #[allow(clippy::result_large_err)]
    pub(crate) fn issue_canvas_runtime_descriptor(
        &self,
        context: &RequestContext,
        canvas_id: &str,
        requested_token_ttl_seconds: Option<u32>,
    ) -> Result<CanvasRuntimeDescriptor, Status> {
        self.ensure_canvas_host_enabled()?;
        let record = self.get_canvas(context, canvas_id)?;
        let now_unix_ms = unix_ms_now_for_status()?;
        if record.expires_at_unix_ms <= now_unix_ms {
            self.counters.canvas_denied.fetch_add(1, Ordering::Relaxed);
            return Err(Status::failed_precondition("canvas session expired"));
        }
        let token_ttl_ms =
            self.resolve_canvas_token_ttl_ms(requested_token_ttl_seconds.unwrap_or_default())?;
        let expires_at_unix_ms =
            now_unix_ms.saturating_add(token_ttl_ms as i64).min(record.expires_at_unix_ms);
        let auth_token = self.issue_canvas_token(
            record.canvas_id.as_str(),
            context.principal.as_str(),
            record.session_id.as_str(),
            now_unix_ms,
            expires_at_unix_ms,
        )?;
        Ok(CanvasRuntimeDescriptor {
            canvas_id: record.canvas_id.clone(),
            frame_url: format!(
                "{}/canvas/v1/frame/{}",
                self.config.canvas_host.public_base_url, record.canvas_id
            ),
            runtime_url: format!(
                "{}/canvas/v1/runtime.js",
                self.config.canvas_host.public_base_url
            ),
            auth_token,
            expires_at_unix_ms,
        })
    }

    #[allow(clippy::result_large_err)]
    pub(crate) fn restore_canvas_state(
        &self,
        context: &RequestContext,
        canvas_id: &str,
        target_state_version: u64,
    ) -> Result<CanvasRecord, Status> {
        if target_state_version == 0 {
            return Err(Status::invalid_argument("target_state_version must be greater than 0"));
        }
        let record = self.get_canvas(context, canvas_id)?;
        if record.closed {
            return Err(Status::failed_precondition("canvas is closed and cannot be restored"));
        }
        if record.state_version == target_state_version {
            return Ok(record);
        }
        let target_patch = self
            .load_canvas_patch_history(record.canvas_id.as_str())?
            .into_iter()
            .find(|patch| patch.state_version == target_state_version)
            .ok_or_else(|| {
                Status::not_found(format!(
                    "canvas state version not found: {}@{}",
                    record.canvas_id, target_state_version
                ))
            })?;
        if target_patch.closed {
            return Err(Status::failed_precondition("closed canvas revisions cannot be restored"));
        }
        self.update_canvas_state(
            context,
            record.canvas_id.as_str(),
            Some(target_patch.resulting_state_json.as_bytes()),
            None,
            Some(record.state_version),
            Some(record.state_schema_version),
        )
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
    pub(crate) fn load_canvas_patch_history(
        &self,
        canvas_id: &str,
    ) -> Result<Vec<CanvasStatePatchRecord>, Status> {
        let mut history = Vec::new();
        let mut next_after = 0_u64;
        loop {
            let batch = self
                .journal_store
                .list_canvas_state_patches(canvas_id, next_after, CANVAS_PATCH_HISTORY_BATCH_LIMIT)
                .map_err(|error| map_canvas_store_error("list_canvas_state_patches", error))?;
            if batch.is_empty() {
                break;
            }
            next_after = batch.last().map(|record| record.state_version).unwrap_or(next_after);
            let completed_batch = batch.len() < CANVAS_PATCH_HISTORY_BATCH_LIMIT;
            history.extend(batch);
            if completed_batch {
                break;
            }
        }
        Ok(history)
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
    pub(crate) fn record_journal_event_blocking(
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
            Err(JournalError::JournalCapacityExceeded { current_events, max_events }) => {
                self.counters.journal_persist_failures.fetch_add(1, Ordering::Relaxed);
                return Err(Status::resource_exhausted(format!(
                    "journal capacity reached ({current_events} >= {max_events})"
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
    pub(crate) async fn record_journal_event(
        self: &Arc<Self>,
        request: JournalAppendRequest,
    ) -> Result<crate::journal::JournalAppendOutcome, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.record_journal_event_blocking(&request))
            .await
            .map_err(|_| Status::internal("journal write worker panicked"))?
    }

    pub(crate) fn consume_vault_rate_limit(&self, principal: &str) -> bool {
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

    pub(crate) fn record_vault_rate_limited_request(&self) {
        self.counters.vault_rate_limited_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_vault_put_request(&self) {
        self.counters.vault_put_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_vault_get_request(&self) {
        self.counters.vault_get_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_vault_delete_request(&self) {
        self.counters.vault_delete_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_vault_list_request(&self) {
        self.counters.vault_list_requests.fetch_add(1, Ordering::Relaxed);
    }

    #[allow(clippy::result_large_err)]
    pub(crate) async fn vault_put_secret(
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
    pub(crate) async fn vault_get_secret(
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
    pub(crate) async fn vault_delete_secret(
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
    pub(crate) async fn vault_list_secrets(
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
                smart_routing_enabled: self.config.smart_routing.enabled,
                smart_routing_default_mode: self.config.smart_routing.default_mode.clone(),
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
    pub(crate) fn recent_journal_snapshot_blocking(
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

    #[must_use]
    pub fn model_provider_status_snapshot(&self) -> ProviderStatusSnapshot {
        self.model_provider.status_snapshot()
    }

    #[must_use]
    pub fn provider_lease_snapshot(&self) -> ProviderLeaseManagerSnapshot {
        self.provider_leases.snapshot()
    }

    #[allow(clippy::result_large_err)]
    pub fn retrieval_backend_snapshot(&self) -> Result<RetrievalBackendSnapshot, Status> {
        let embeddings_status = self
            .journal_store
            .memory_embeddings_status()
            .map_err(|error| map_memory_store_error("load retrieval backend snapshot", error))?;
        Ok(self.retrieval_backend.snapshot(&self.retrieval_config_snapshot(), &embeddings_status))
    }

    #[must_use]
    pub fn preview_provider_lease(
        &self,
        provider_id: &str,
        credential_id: &str,
        priority: crate::provider_leases::LeasePriority,
        task_label: &str,
        max_wait_ms: u64,
    ) -> ProviderLeasePreviewSnapshot {
        let _ = task_label;
        self.provider_leases.preview(ProviderLeasePreviewRequest {
            provider_id,
            credential_id,
            priority,
            max_wait_ms,
        })
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
    pub async fn execute_model_provider_with_lease(
        self: &Arc<Self>,
        request: ProviderRequest,
        lease_context: ProviderLeaseExecutionContext,
    ) -> Result<crate::model_provider::ProviderResponse, Status> {
        let _lease = self
            .provider_leases
            .acquire(ProviderLeaseAcquireRequest {
                provider_id: lease_context.provider_id.as_str(),
                credential_id: lease_context.credential_id.as_str(),
                priority: lease_context.priority,
                task_label: lease_context.task_label.as_str(),
                max_wait_ms: lease_context.max_wait_ms,
                session_id: lease_context.session_id.as_deref(),
                run_id: lease_context.run_id.as_deref(),
            })
            .await
            .map_err(|error| match error {
                ProviderLeaseAcquireError::Deferred(preview) => {
                    Status::resource_exhausted(format!(
                        "shared provider lease deferred {} for {}:{} ({})",
                        lease_context.task_label,
                        lease_context.provider_id,
                        lease_context.credential_id,
                        preview.reason.unwrap_or_else(|| "foreground capacity reserved".to_owned()),
                    ))
                }
                ProviderLeaseAcquireError::TimedOut { waited_ms, preview } => {
                    Status::unavailable(format!(
                        "shared provider lease wait exceeded {} ms for {}:{} ({})",
                        waited_ms,
                        lease_context.provider_id,
                        lease_context.credential_id,
                        preview.reason.unwrap_or_else(|| "shared capacity exhausted".to_owned()),
                    ))
                }
            })?;
        self.execute_model_provider(request).await
    }

    #[allow(clippy::result_large_err)]
    pub async fn execute_audio_transcription(
        self: &Arc<Self>,
        request: AudioTranscriptionRequest,
    ) -> Result<AudioTranscriptionResponse, Status> {
        self.counters.model_provider_requests.fetch_add(1, Ordering::Relaxed);
        match self.model_provider.transcribe_audio(request).await {
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
    fn update_orchestrator_session_title_blocking(
        &self,
        request: &OrchestratorSessionTitleUpdateRequest,
    ) -> Result<OrchestratorSessionRecord, Status> {
        self.journal_store.update_orchestrator_session_title(request).map_err(|error| {
            map_orchestrator_store_error("update orchestrator session title", error)
        })
    }

    #[allow(clippy::result_large_err)]
    pub async fn update_orchestrator_session_title(
        self: &Arc<Self>,
        request: OrchestratorSessionTitleUpdateRequest,
    ) -> Result<OrchestratorSessionRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.update_orchestrator_session_title_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("orchestrator session title worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn orchestrator_session_by_id_blocking(
        &self,
        session_id: &str,
    ) -> Result<Option<OrchestratorSessionRecord>, Status> {
        self.journal_store
            .orchestrator_session_by_id(session_id)
            .map_err(|error| map_orchestrator_store_error("load orchestrator session by id", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn orchestrator_session_by_id(
        self: &Arc<Self>,
        session_id: String,
    ) -> Result<Option<OrchestratorSessionRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.orchestrator_session_by_id_blocking(&session_id))
            .await
            .map_err(|_| Status::internal("orchestrator session lookup worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn session_project_context_state_blocking(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionProjectContextStateRecord>, Status> {
        self.journal_store.session_project_context_state(session_id).map_err(|error| {
            map_orchestrator_store_error("load session project context state", error)
        })
    }

    #[allow(clippy::result_large_err)]
    pub async fn session_project_context_state(
        self: &Arc<Self>,
        session_id: String,
    ) -> Result<Option<SessionProjectContextStateRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.session_project_context_state_blocking(session_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("session project context state worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn upsert_session_project_context_state_blocking(
        &self,
        request: &SessionProjectContextStateUpsertRequest,
    ) -> Result<SessionProjectContextStateRecord, Status> {
        self.journal_store.upsert_session_project_context_state(request).map_err(|error| {
            map_orchestrator_store_error("upsert session project context state", error)
        })
    }

    #[allow(clippy::result_large_err)]
    pub async fn upsert_session_project_context_state(
        self: &Arc<Self>,
        request: SessionProjectContextStateUpsertRequest,
    ) -> Result<SessionProjectContextStateRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.upsert_session_project_context_state_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("session project context upsert worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn copy_session_project_context_state_blocking(
        &self,
        request: &SessionProjectContextStateCopyRequest,
    ) -> Result<Option<SessionProjectContextStateRecord>, Status> {
        self.journal_store.copy_session_project_context_state(request).map_err(|error| {
            map_orchestrator_store_error("copy session project context state", error)
        })
    }

    #[allow(clippy::result_large_err)]
    pub async fn copy_session_project_context_state(
        self: &Arc<Self>,
        request: SessionProjectContextStateCopyRequest,
    ) -> Result<Option<SessionProjectContextStateRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.copy_session_project_context_state_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("session project context copy worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn update_orchestrator_session_quick_controls_blocking(
        &self,
        request: &crate::journal::OrchestratorSessionQuickControlsUpdateRequest,
    ) -> Result<OrchestratorSessionRecord, Status> {
        self.journal_store.update_orchestrator_session_quick_controls(request).map_err(|error| {
            map_orchestrator_store_error("update orchestrator session quick controls", error)
        })
    }

    #[allow(clippy::result_large_err)]
    pub async fn update_orchestrator_session_quick_controls(
        self: &Arc<Self>,
        request: crate::journal::OrchestratorSessionQuickControlsUpdateRequest,
    ) -> Result<OrchestratorSessionRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.update_orchestrator_session_quick_controls_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("orchestrator session quick controls worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_orchestrator_sessions_blocking(
        &self,
        request: &ListOrchestratorSessionsRequest,
    ) -> Result<(Vec<OrchestratorSessionRecord>, Option<String>), Status> {
        let limit = request.requested_limit.unwrap_or(100).clamp(1, MAX_SESSIONS_PAGE_LIMIT);
        let normalized_search = request
            .search_query
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| value.to_ascii_lowercase());
        let mut sessions = if let Some(search) = normalized_search.as_deref() {
            let mut matched = Vec::new();
            let mut cursor = request.after_session_key.clone();
            loop {
                let page = self
                    .journal_store
                    .list_orchestrator_sessions(
                        cursor.as_deref(),
                        request.principal.as_str(),
                        request.device_id.as_str(),
                        request.channel.as_deref(),
                        request.include_archived,
                        MAX_SESSIONS_PAGE_LIMIT,
                    )
                    .map_err(|error| {
                        map_orchestrator_store_error("list orchestrator sessions", error)
                    })?;
                if page.is_empty() {
                    break;
                }
                cursor = page.last().map(|session| session.session_key.clone());
                for mut session in page {
                    let matched_field = [
                        Some(session.title.as_str()),
                        session.preview.as_deref(),
                        session.last_intent.as_deref(),
                        session.last_summary.as_deref(),
                        session.last_run_state.as_deref(),
                    ]
                    .into_iter()
                    .flatten()
                    .find(|value| value.to_ascii_lowercase().contains(search))
                    .map(ToOwned::to_owned);
                    if let Some(snippet) = matched_field {
                        session.match_snippet = Some(snippet);
                        matched.push(session);
                        if matched.len() > limit {
                            break;
                        }
                    }
                }
                if matched.len() > limit || cursor.is_none() {
                    break;
                }
            }
            matched
        } else {
            self.journal_store
                .list_orchestrator_sessions(
                    request.after_session_key.as_deref(),
                    request.principal.as_str(),
                    request.device_id.as_str(),
                    request.channel.as_deref(),
                    request.include_archived,
                    limit.saturating_add(1),
                )
                .map_err(|error| {
                    map_orchestrator_store_error("list orchestrator sessions", error)
                })?
        };
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
        request: ListOrchestratorSessionsRequest,
    ) -> Result<(Vec<OrchestratorSessionRecord>, Option<String>), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.list_orchestrator_sessions_blocking(&request))
            .await
            .map_err(|_| Status::internal("orchestrator session list worker panicked"))?
    }

    fn list_orchestrator_sessions_for_principal_blocking(
        &self,
        request: &ListPrincipalOrchestratorSessionsRequest,
    ) -> Result<(Vec<OrchestratorSessionRecord>, Option<String>), Status> {
        let limit = request.requested_limit.unwrap_or(100).clamp(1, MAX_SESSIONS_PAGE_LIMIT);
        let normalized_search = request
            .search_query
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| value.to_ascii_lowercase());
        let mut sessions = if let Some(search) = normalized_search.as_deref() {
            let mut matched = Vec::new();
            let mut cursor = request.after_session_key.clone();
            loop {
                let page = self
                    .journal_store
                    .list_orchestrator_sessions_for_principal(
                        cursor.as_deref(),
                        request.principal.as_str(),
                        request.include_archived,
                        MAX_SESSIONS_PAGE_LIMIT,
                    )
                    .map_err(|error| {
                        map_orchestrator_store_error(
                            "list orchestrator sessions for principal",
                            error,
                        )
                    })?;
                if page.is_empty() {
                    break;
                }
                cursor = page.last().map(|session| session.session_key.clone());
                for mut session in page {
                    let matched_field = [
                        Some(session.title.as_str()),
                        session.preview.as_deref(),
                        session.last_intent.as_deref(),
                        session.last_summary.as_deref(),
                        session.last_run_state.as_deref(),
                    ]
                    .into_iter()
                    .flatten()
                    .find(|value| value.to_ascii_lowercase().contains(search))
                    .map(ToOwned::to_owned);
                    if let Some(snippet) = matched_field {
                        session.match_snippet = Some(snippet);
                        matched.push(session);
                        if matched.len() > limit {
                            break;
                        }
                    }
                }
                if matched.len() > limit || cursor.is_none() {
                    break;
                }
            }
            matched
        } else {
            self.journal_store
                .list_orchestrator_sessions_for_principal(
                    request.after_session_key.as_deref(),
                    request.principal.as_str(),
                    request.include_archived,
                    limit.saturating_add(1),
                )
                .map_err(|error| {
                    map_orchestrator_store_error("list orchestrator sessions for principal", error)
                })?
        };
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
    pub async fn list_orchestrator_sessions_for_principal(
        self: &Arc<Self>,
        request: ListPrincipalOrchestratorSessionsRequest,
    ) -> Result<(Vec<OrchestratorSessionRecord>, Option<String>), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.list_orchestrator_sessions_for_principal_blocking(&request)
        })
        .await
        .map_err(|_| {
            Status::internal("principal-scoped orchestrator session list worker panicked")
        })?
    }

    #[allow(clippy::result_large_err)]
    fn summarize_orchestrator_usage_blocking(
        &self,
        query: &OrchestratorUsageQuery,
    ) -> Result<OrchestratorUsageSummary, Status> {
        self.journal_store
            .summarize_orchestrator_usage(query)
            .map_err(|error| map_orchestrator_store_error("summarize orchestrator usage", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn summarize_orchestrator_usage(
        self: &Arc<Self>,
        query: OrchestratorUsageQuery,
    ) -> Result<OrchestratorUsageSummary, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.summarize_orchestrator_usage_blocking(&query))
            .await
            .map_err(|_| Status::internal("orchestrator usage summary worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_orchestrator_usage_sessions_blocking(
        &self,
        query: &OrchestratorUsageQuery,
    ) -> Result<Vec<OrchestratorUsageSessionRecord>, Status> {
        self.journal_store.list_orchestrator_usage_sessions(query).map_err(|error| {
            map_orchestrator_store_error("list orchestrator usage sessions", error)
        })
    }

    #[allow(clippy::result_large_err)]
    pub async fn list_orchestrator_usage_sessions(
        self: &Arc<Self>,
        query: OrchestratorUsageQuery,
    ) -> Result<Vec<OrchestratorUsageSessionRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.list_orchestrator_usage_sessions_blocking(&query))
            .await
            .map_err(|_| Status::internal("orchestrator usage session list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn get_orchestrator_usage_session_blocking(
        &self,
        query: &OrchestratorUsageQuery,
        session_id: &str,
        run_limit: usize,
    ) -> Result<Option<(OrchestratorUsageSessionRecord, Vec<OrchestratorUsageRunRecord>)>, Status>
    {
        self.journal_store
            .get_orchestrator_usage_session(query, session_id, run_limit)
            .map_err(|error| map_orchestrator_store_error("get orchestrator usage session", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn get_orchestrator_usage_session(
        self: &Arc<Self>,
        query: OrchestratorUsageQuery,
        session_id: String,
        run_limit: usize,
    ) -> Result<Option<(OrchestratorUsageSessionRecord, Vec<OrchestratorUsageRunRecord>)>, Status>
    {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.get_orchestrator_usage_session_blocking(&query, session_id.as_str(), run_limit)
        })
        .await
        .map_err(|_| Status::internal("orchestrator usage session detail worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn cleanup_orchestrator_session_blocking(
        &self,
        request: &OrchestratorSessionCleanupRequest,
    ) -> Result<OrchestratorSessionCleanupOutcome, Status> {
        self.journal_store
            .cleanup_orchestrator_session(request)
            .map_err(|error| map_orchestrator_store_error("cleanup orchestrator session", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn cleanup_orchestrator_session(
        self: &Arc<Self>,
        request: OrchestratorSessionCleanupRequest,
    ) -> Result<OrchestratorSessionCleanupOutcome, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.cleanup_orchestrator_session_blocking(&request))
            .await
            .map_err(|_| Status::internal("orchestrator session cleanup worker panicked"))?
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
    fn delete_agent_blocking(&self, agent_id: &str) -> Result<AgentDeleteOutcome, Status> {
        self.agent_registry
            .delete_agent(agent_id)
            .map_err(|error| map_agent_registry_error("delete agent", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn delete_agent(
        self: &Arc<Self>,
        agent_id: String,
    ) -> Result<AgentDeleteOutcome, Status> {
        let state = Arc::clone(self);
        let result =
            tokio::task::spawn_blocking(move || state.delete_agent_blocking(agent_id.as_str()))
                .await
                .map_err(|_| Status::internal("agent delete worker panicked"))?;
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
    fn list_agent_bindings_blocking(
        &self,
        query: &AgentBindingQuery,
    ) -> Result<Vec<SessionAgentBinding>, Status> {
        self.agent_registry
            .list_bindings(query.clone())
            .map_err(|error| map_agent_registry_error("list agent bindings", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn list_agent_bindings(
        self: &Arc<Self>,
        query: AgentBindingQuery,
    ) -> Result<Vec<SessionAgentBinding>, Status> {
        let state = Arc::clone(self);
        let result =
            tokio::task::spawn_blocking(move || state.list_agent_bindings_blocking(&query))
                .await
                .map_err(|_| Status::internal("agent binding list worker panicked"))?;
        if let Err(status) = &result {
            if status.code() == tonic::Code::InvalidArgument {
                self.counters.agent_validation_failures.fetch_add(1, Ordering::Relaxed);
            }
        }
        result
    }

    #[allow(clippy::result_large_err)]
    fn bind_agent_for_context_blocking(
        &self,
        request: &AgentBindingRequest,
    ) -> Result<AgentBindingOutcome, Status> {
        self.agent_registry
            .bind_agent_for_context(request.clone())
            .map_err(|error| map_agent_registry_error("bind agent for context", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn bind_agent_for_context(
        self: &Arc<Self>,
        request: AgentBindingRequest,
    ) -> Result<AgentBindingOutcome, Status> {
        let state = Arc::clone(self);
        let result =
            tokio::task::spawn_blocking(move || state.bind_agent_for_context_blocking(&request))
                .await
                .map_err(|_| Status::internal("agent bind worker panicked"))?;
        match &result {
            Ok(_) => {
                self.counters.agent_mutations.fetch_add(1, Ordering::Relaxed);
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
    fn unbind_agent_for_context_blocking(
        &self,
        request: &AgentUnbindRequest,
    ) -> Result<AgentUnbindOutcome, Status> {
        self.agent_registry
            .unbind_agent_for_context(request.clone())
            .map_err(|error| map_agent_registry_error("unbind agent for context", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn unbind_agent_for_context(
        self: &Arc<Self>,
        request: AgentUnbindRequest,
    ) -> Result<AgentUnbindOutcome, Status> {
        let state = Arc::clone(self);
        let result =
            tokio::task::spawn_blocking(move || state.unbind_agent_for_context_blocking(&request))
                .await
                .map_err(|_| Status::internal("agent unbind worker panicked"))?;
        match &result {
            Ok(outcome) => {
                if outcome.removed {
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
    fn update_orchestrator_run_metadata_blocking(
        &self,
        request: &OrchestratorRunMetadataUpdateRequest,
    ) -> Result<(), Status> {
        self.journal_store.update_orchestrator_run_metadata(request).map_err(|error| {
            map_orchestrator_store_error("update orchestrator run metadata", error)
        })
    }

    #[allow(clippy::result_large_err)]
    pub async fn update_orchestrator_run_metadata(
        self: &Arc<Self>,
        request: OrchestratorRunMetadataUpdateRequest,
    ) -> Result<(), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.update_orchestrator_run_metadata_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("orchestrator run metadata worker panicked"))?
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
    fn list_orchestrator_usage_runs_blocking(
        &self,
        query: &OrchestratorUsageQuery,
        limit: usize,
    ) -> Result<Vec<crate::journal::OrchestratorUsageInsightsRunRecord>, Status> {
        self.journal_store
            .list_orchestrator_usage_runs(query, limit)
            .map_err(|error| map_orchestrator_store_error("list orchestrator usage runs", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn list_orchestrator_usage_runs(
        self: &Arc<Self>,
        query: OrchestratorUsageQuery,
        limit: usize,
    ) -> Result<Vec<crate::journal::OrchestratorUsageInsightsRunRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.list_orchestrator_usage_runs_blocking(&query, limit)
        })
        .await
        .map_err(|_| Status::internal("orchestrator usage runs worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_usage_pricing_records_blocking(
        &self,
    ) -> Result<Vec<crate::journal::UsagePricingRecord>, Status> {
        self.journal_store
            .list_usage_pricing_records()
            .map_err(|error| map_orchestrator_store_error("list usage pricing records", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn list_usage_pricing_records(
        self: &Arc<Self>,
    ) -> Result<Vec<crate::journal::UsagePricingRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.list_usage_pricing_records_blocking())
            .await
            .map_err(|_| Status::internal("usage pricing list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn upsert_usage_pricing_record_blocking(
        &self,
        request: &crate::journal::UsagePricingUpsertRequest,
    ) -> Result<crate::journal::UsagePricingRecord, Status> {
        self.journal_store
            .upsert_usage_pricing_record(request)
            .map_err(|error| map_orchestrator_store_error("upsert usage pricing record", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn upsert_usage_pricing_record(
        self: &Arc<Self>,
        request: crate::journal::UsagePricingUpsertRequest,
    ) -> Result<crate::journal::UsagePricingRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.upsert_usage_pricing_record_blocking(&request))
            .await
            .map_err(|_| Status::internal("usage pricing upsert worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn upsert_usage_budget_policy_blocking(
        &self,
        request: &crate::journal::UsageBudgetPolicyUpsertRequest,
    ) -> Result<crate::journal::UsageBudgetPolicyRecord, Status> {
        self.journal_store
            .upsert_usage_budget_policy(request)
            .map_err(|error| map_orchestrator_store_error("upsert usage budget policy", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn upsert_usage_budget_policy(
        self: &Arc<Self>,
        request: crate::journal::UsageBudgetPolicyUpsertRequest,
    ) -> Result<crate::journal::UsageBudgetPolicyRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.upsert_usage_budget_policy_blocking(&request))
            .await
            .map_err(|_| Status::internal("usage budget upsert worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_usage_budget_policies_blocking(
        &self,
        filter: &crate::journal::UsageBudgetPoliciesFilter,
    ) -> Result<Vec<crate::journal::UsageBudgetPolicyRecord>, Status> {
        self.journal_store
            .list_usage_budget_policies(filter)
            .map_err(|error| map_orchestrator_store_error("list usage budget policies", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn list_usage_budget_policies(
        self: &Arc<Self>,
        filter: crate::journal::UsageBudgetPoliciesFilter,
    ) -> Result<Vec<crate::journal::UsageBudgetPolicyRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.list_usage_budget_policies_blocking(&filter))
            .await
            .map_err(|_| Status::internal("usage budget list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn create_usage_routing_decision_blocking(
        &self,
        request: &crate::journal::UsageRoutingDecisionCreateRequest,
    ) -> Result<crate::journal::UsageRoutingDecisionRecord, Status> {
        self.journal_store
            .create_usage_routing_decision(request)
            .map_err(|error| map_orchestrator_store_error("create usage routing decision", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn create_usage_routing_decision(
        self: &Arc<Self>,
        request: crate::journal::UsageRoutingDecisionCreateRequest,
    ) -> Result<crate::journal::UsageRoutingDecisionRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.create_usage_routing_decision_blocking(&request))
            .await
            .map_err(|_| Status::internal("usage routing decision worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_usage_routing_decisions_blocking(
        &self,
        filter: &crate::journal::UsageRoutingDecisionsFilter,
    ) -> Result<Vec<crate::journal::UsageRoutingDecisionRecord>, Status> {
        self.journal_store
            .list_usage_routing_decisions(filter)
            .map_err(|error| map_orchestrator_store_error("list usage routing decisions", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn list_usage_routing_decisions(
        self: &Arc<Self>,
        filter: crate::journal::UsageRoutingDecisionsFilter,
    ) -> Result<Vec<crate::journal::UsageRoutingDecisionRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.list_usage_routing_decisions_blocking(&filter))
            .await
            .map_err(|_| Status::internal("usage routing decision list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn upsert_usage_alert_blocking(
        &self,
        request: &crate::journal::UsageAlertUpsertRequest,
    ) -> Result<crate::journal::UsageAlertRecord, Status> {
        self.journal_store
            .upsert_usage_alert(request)
            .map_err(|error| map_orchestrator_store_error("upsert usage alert", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn upsert_usage_alert(
        self: &Arc<Self>,
        request: crate::journal::UsageAlertUpsertRequest,
    ) -> Result<crate::journal::UsageAlertRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.upsert_usage_alert_blocking(&request))
            .await
            .map_err(|_| Status::internal("usage alert upsert worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_usage_alerts_blocking(
        &self,
        filter: &crate::journal::UsageAlertsFilter,
    ) -> Result<Vec<crate::journal::UsageAlertRecord>, Status> {
        self.journal_store
            .list_usage_alerts(filter)
            .map_err(|error| map_orchestrator_store_error("list usage alerts", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn list_usage_alerts(
        self: &Arc<Self>,
        filter: crate::journal::UsageAlertsFilter,
    ) -> Result<Vec<crate::journal::UsageAlertRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.list_usage_alerts_blocking(&filter))
            .await
            .map_err(|_| Status::internal("usage alerts list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn latest_approval_by_subject_blocking(
        &self,
        subject_id: &str,
    ) -> Result<Option<crate::journal::ApprovalRecord>, Status> {
        self.journal_store
            .latest_approval_by_subject(subject_id)
            .map_err(|error| map_orchestrator_store_error("load latest approval by subject", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn latest_approval_by_subject(
        self: &Arc<Self>,
        subject_id: String,
    ) -> Result<Option<crate::journal::ApprovalRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.latest_approval_by_subject_blocking(subject_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("usage approval lookup worker panicked"))?
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
    fn list_orchestrator_session_runs_blocking(
        &self,
        session_id: &str,
    ) -> Result<Vec<OrchestratorRunStatusSnapshot>, Status> {
        self.journal_store
            .list_orchestrator_session_runs(session_id)
            .map_err(|error| map_orchestrator_store_error("list orchestrator session runs", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn list_orchestrator_session_runs(
        self: &Arc<Self>,
        session_id: String,
    ) -> Result<Vec<OrchestratorRunStatusSnapshot>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.list_orchestrator_session_runs_blocking(session_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("orchestrator session runs worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn update_orchestrator_session_lineage_blocking(
        &self,
        request: &OrchestratorSessionLineageUpdateRequest,
    ) -> Result<(), Status> {
        self.journal_store.update_orchestrator_session_lineage(request).map_err(|error| {
            map_orchestrator_store_error("update orchestrator session lineage", error)
        })
    }

    #[allow(clippy::result_large_err)]
    pub async fn update_orchestrator_session_lineage(
        self: &Arc<Self>,
        request: OrchestratorSessionLineageUpdateRequest,
    ) -> Result<(), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.update_orchestrator_session_lineage_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("orchestrator session lineage worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_orchestrator_session_transcript_blocking(
        &self,
        session_id: &str,
    ) -> Result<Vec<OrchestratorSessionTranscriptRecord>, Status> {
        self.journal_store.list_orchestrator_session_transcript(session_id).map_err(|error| {
            map_orchestrator_store_error("load orchestrator session transcript", error)
        })
    }

    #[allow(clippy::result_large_err)]
    pub async fn list_orchestrator_session_transcript(
        self: &Arc<Self>,
        session_id: String,
    ) -> Result<Vec<OrchestratorSessionTranscriptRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.list_orchestrator_session_transcript_blocking(session_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("orchestrator transcript worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn create_orchestrator_queued_input_blocking(
        &self,
        request: &OrchestratorQueuedInputCreateRequest,
    ) -> Result<OrchestratorQueuedInputRecord, Status> {
        self.journal_store.create_orchestrator_queued_input(request).map_err(|error| {
            map_orchestrator_store_error("create queued orchestrator input", error)
        })
    }

    #[allow(clippy::result_large_err)]
    pub async fn create_orchestrator_queued_input(
        self: &Arc<Self>,
        request: OrchestratorQueuedInputCreateRequest,
    ) -> Result<OrchestratorQueuedInputRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.create_orchestrator_queued_input_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("orchestrator queued input worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn update_orchestrator_queued_input_state_blocking(
        &self,
        request: &OrchestratorQueuedInputUpdateRequest,
    ) -> Result<(), Status> {
        self.journal_store.update_orchestrator_queued_input_state(request).map_err(|error| {
            map_orchestrator_store_error("update queued orchestrator input", error)
        })
    }

    #[allow(clippy::result_large_err)]
    pub async fn update_orchestrator_queued_input_state(
        self: &Arc<Self>,
        request: OrchestratorQueuedInputUpdateRequest,
    ) -> Result<(), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.update_orchestrator_queued_input_state_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("orchestrator queued input state worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn get_orchestrator_session_queue_control_blocking(
        &self,
        session_id: &str,
    ) -> Result<Option<OrchestratorSessionQueueControlRecord>, Status> {
        self.journal_store.get_orchestrator_session_queue_control(session_id).map_err(|error| {
            map_orchestrator_store_error("load orchestrator session queue control", error)
        })
    }

    #[allow(clippy::result_large_err)]
    pub async fn get_orchestrator_session_queue_control(
        self: &Arc<Self>,
        session_id: String,
    ) -> Result<Option<OrchestratorSessionQueueControlRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.get_orchestrator_session_queue_control_blocking(session_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("orchestrator queue control worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn upsert_orchestrator_session_queue_control_blocking(
        &self,
        request: &OrchestratorSessionQueueControlUpdateRequest,
    ) -> Result<OrchestratorSessionQueueControlRecord, Status> {
        self.journal_store.upsert_orchestrator_session_queue_control(request).map_err(|error| {
            map_orchestrator_store_error("upsert orchestrator session queue control", error)
        })
    }

    #[allow(clippy::result_large_err)]
    pub async fn upsert_orchestrator_session_queue_control(
        self: &Arc<Self>,
        request: OrchestratorSessionQueueControlUpdateRequest,
    ) -> Result<OrchestratorSessionQueueControlRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.upsert_orchestrator_session_queue_control_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("orchestrator queue control upsert worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_orchestrator_queued_inputs_blocking(
        &self,
        session_id: &str,
    ) -> Result<Vec<OrchestratorQueuedInputRecord>, Status> {
        self.journal_store
            .list_orchestrator_queued_inputs(session_id)
            .map_err(|error| map_orchestrator_store_error("load queued orchestrator inputs", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn list_orchestrator_queued_inputs(
        self: &Arc<Self>,
        session_id: String,
    ) -> Result<Vec<OrchestratorQueuedInputRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.list_orchestrator_queued_inputs_blocking(session_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("orchestrator queued input list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn create_orchestrator_session_pin_blocking(
        &self,
        request: &OrchestratorSessionPinCreateRequest,
    ) -> Result<OrchestratorSessionPinRecord, Status> {
        self.journal_store
            .create_orchestrator_session_pin(request)
            .map_err(|error| map_orchestrator_store_error("create orchestrator session pin", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn create_orchestrator_session_pin(
        self: &Arc<Self>,
        request: OrchestratorSessionPinCreateRequest,
    ) -> Result<OrchestratorSessionPinRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.create_orchestrator_session_pin_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("orchestrator session pin worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_orchestrator_session_pins_blocking(
        &self,
        session_id: &str,
    ) -> Result<Vec<OrchestratorSessionPinRecord>, Status> {
        self.journal_store
            .list_orchestrator_session_pins(session_id)
            .map_err(|error| map_orchestrator_store_error("load orchestrator session pins", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn list_orchestrator_session_pins(
        self: &Arc<Self>,
        session_id: String,
    ) -> Result<Vec<OrchestratorSessionPinRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.list_orchestrator_session_pins_blocking(session_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("orchestrator session pin list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn delete_orchestrator_session_pin_blocking(&self, pin_id: &str) -> Result<bool, Status> {
        self.journal_store
            .delete_orchestrator_session_pin(pin_id)
            .map_err(|error| map_orchestrator_store_error("delete orchestrator session pin", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn delete_orchestrator_session_pin(
        self: &Arc<Self>,
        pin_id: String,
    ) -> Result<bool, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.delete_orchestrator_session_pin_blocking(pin_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("orchestrator session pin delete worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn create_orchestrator_compaction_artifact_blocking(
        &self,
        request: &OrchestratorCompactionArtifactCreateRequest,
    ) -> Result<OrchestratorCompactionArtifactRecord, Status> {
        self.journal_store.create_orchestrator_compaction_artifact(request).map_err(|error| {
            map_orchestrator_store_error("create orchestrator compaction artifact", error)
        })
    }

    #[allow(clippy::result_large_err)]
    pub async fn create_orchestrator_compaction_artifact(
        self: &Arc<Self>,
        request: OrchestratorCompactionArtifactCreateRequest,
    ) -> Result<OrchestratorCompactionArtifactRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.create_orchestrator_compaction_artifact_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("orchestrator compaction artifact worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_orchestrator_compaction_artifacts_blocking(
        &self,
        session_id: &str,
    ) -> Result<Vec<OrchestratorCompactionArtifactRecord>, Status> {
        self.journal_store.list_orchestrator_compaction_artifacts(session_id).map_err(|error| {
            map_orchestrator_store_error("list orchestrator compaction artifacts", error)
        })
    }

    #[allow(clippy::result_large_err)]
    pub async fn list_orchestrator_compaction_artifacts(
        self: &Arc<Self>,
        session_id: String,
    ) -> Result<Vec<OrchestratorCompactionArtifactRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.list_orchestrator_compaction_artifacts_blocking(session_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("orchestrator compaction artifact list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn get_orchestrator_compaction_artifact_blocking(
        &self,
        artifact_id: &str,
    ) -> Result<Option<OrchestratorCompactionArtifactRecord>, Status> {
        self.journal_store.get_orchestrator_compaction_artifact(artifact_id).map_err(|error| {
            map_orchestrator_store_error("load orchestrator compaction artifact", error)
        })
    }

    #[allow(clippy::result_large_err)]
    pub async fn get_orchestrator_compaction_artifact(
        self: &Arc<Self>,
        artifact_id: String,
    ) -> Result<Option<OrchestratorCompactionArtifactRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.get_orchestrator_compaction_artifact_blocking(artifact_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("orchestrator compaction artifact detail worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn create_orchestrator_checkpoint_blocking(
        &self,
        request: &OrchestratorCheckpointCreateRequest,
    ) -> Result<OrchestratorCheckpointRecord, Status> {
        self.journal_store
            .create_orchestrator_checkpoint(request)
            .map_err(|error| map_orchestrator_store_error("create orchestrator checkpoint", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn create_orchestrator_checkpoint(
        self: &Arc<Self>,
        request: OrchestratorCheckpointCreateRequest,
    ) -> Result<OrchestratorCheckpointRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.create_orchestrator_checkpoint_blocking(&request))
            .await
            .map_err(|_| Status::internal("orchestrator checkpoint worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_orchestrator_checkpoints_blocking(
        &self,
        session_id: &str,
    ) -> Result<Vec<OrchestratorCheckpointRecord>, Status> {
        self.journal_store
            .list_orchestrator_checkpoints(session_id)
            .map_err(|error| map_orchestrator_store_error("list orchestrator checkpoints", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn list_orchestrator_checkpoints(
        self: &Arc<Self>,
        session_id: String,
    ) -> Result<Vec<OrchestratorCheckpointRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.list_orchestrator_checkpoints_blocking(session_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("orchestrator checkpoint list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn get_orchestrator_checkpoint_blocking(
        &self,
        checkpoint_id: &str,
    ) -> Result<Option<OrchestratorCheckpointRecord>, Status> {
        self.journal_store
            .get_orchestrator_checkpoint(checkpoint_id)
            .map_err(|error| map_orchestrator_store_error("load orchestrator checkpoint", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn get_orchestrator_checkpoint(
        self: &Arc<Self>,
        checkpoint_id: String,
    ) -> Result<Option<OrchestratorCheckpointRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.get_orchestrator_checkpoint_blocking(checkpoint_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("orchestrator checkpoint detail worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn mark_orchestrator_checkpoint_restored_blocking(
        &self,
        request: &OrchestratorCheckpointRestoreMarkRequest,
    ) -> Result<(), Status> {
        self.journal_store.mark_orchestrator_checkpoint_restored(request).map_err(|error| {
            map_orchestrator_store_error("mark orchestrator checkpoint restored", error)
        })
    }

    #[allow(clippy::result_large_err)]
    pub async fn mark_orchestrator_checkpoint_restored(
        self: &Arc<Self>,
        request: OrchestratorCheckpointRestoreMarkRequest,
    ) -> Result<(), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.mark_orchestrator_checkpoint_restored_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("orchestrator checkpoint restore worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn create_workspace_checkpoint_blocking(
        &self,
        request: &WorkspaceCheckpointCreateRequest,
    ) -> Result<WorkspaceCheckpointRecord, Status> {
        self.journal_store
            .create_workspace_checkpoint(request)
            .map_err(|error| map_orchestrator_store_error("create workspace checkpoint", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn create_workspace_checkpoint(
        self: &Arc<Self>,
        request: WorkspaceCheckpointCreateRequest,
    ) -> Result<WorkspaceCheckpointRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.create_workspace_checkpoint_blocking(&request))
            .await
            .map_err(|_| Status::internal("workspace checkpoint worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_workspace_checkpoints_blocking(
        &self,
        filter: &WorkspaceCheckpointListFilter,
    ) -> Result<Vec<WorkspaceCheckpointRecord>, Status> {
        self.journal_store
            .list_workspace_checkpoints(filter)
            .map_err(|error| map_orchestrator_store_error("list workspace checkpoints", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn list_workspace_checkpoints(
        self: &Arc<Self>,
        filter: WorkspaceCheckpointListFilter,
    ) -> Result<Vec<WorkspaceCheckpointRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.list_workspace_checkpoints_blocking(&filter))
            .await
            .map_err(|_| Status::internal("workspace checkpoint list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn get_workspace_checkpoint_blocking(
        &self,
        checkpoint_id: &str,
    ) -> Result<Option<WorkspaceCheckpointRecord>, Status> {
        self.journal_store
            .get_workspace_checkpoint(checkpoint_id)
            .map_err(|error| map_orchestrator_store_error("load workspace checkpoint", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn get_workspace_checkpoint(
        self: &Arc<Self>,
        checkpoint_id: String,
    ) -> Result<Option<WorkspaceCheckpointRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.get_workspace_checkpoint_blocking(checkpoint_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("workspace checkpoint detail worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_workspace_checkpoint_files_blocking(
        &self,
        checkpoint_id: &str,
    ) -> Result<Vec<WorkspaceCheckpointFileRecord>, Status> {
        self.journal_store
            .list_workspace_checkpoint_files(checkpoint_id)
            .map_err(|error| map_orchestrator_store_error("list workspace checkpoint files", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn list_workspace_checkpoint_files(
        self: &Arc<Self>,
        checkpoint_id: String,
    ) -> Result<Vec<WorkspaceCheckpointFileRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.list_workspace_checkpoint_files_blocking(checkpoint_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("workspace checkpoint file list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn get_workspace_checkpoint_file_payload_blocking(
        &self,
        artifact_id: &str,
    ) -> Result<Option<WorkspaceCheckpointFilePayload>, Status> {
        self.journal_store.get_workspace_checkpoint_file_payload(artifact_id).map_err(|error| {
            map_orchestrator_store_error("get workspace checkpoint file payload", error)
        })
    }

    #[allow(clippy::result_large_err)]
    pub async fn get_workspace_checkpoint_file_payload(
        self: &Arc<Self>,
        artifact_id: String,
    ) -> Result<Option<WorkspaceCheckpointFilePayload>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.get_workspace_checkpoint_file_payload_blocking(artifact_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("workspace checkpoint file payload worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn get_workspace_restore_report_blocking(
        &self,
        report_id: &str,
    ) -> Result<Option<WorkspaceRestoreReportRecord>, Status> {
        self.journal_store
            .get_workspace_restore_report(report_id)
            .map_err(|error| map_orchestrator_store_error("load workspace restore report", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn get_workspace_restore_report(
        self: &Arc<Self>,
        report_id: String,
    ) -> Result<Option<WorkspaceRestoreReportRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.get_workspace_restore_report_blocking(report_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("workspace restore report detail worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_workspace_restore_reports_blocking(
        &self,
        filter: &WorkspaceRestoreReportListFilter,
    ) -> Result<Vec<WorkspaceRestoreReportRecord>, Status> {
        self.journal_store
            .list_workspace_restore_reports(filter)
            .map_err(|error| map_orchestrator_store_error("list workspace restore reports", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn list_workspace_restore_reports(
        self: &Arc<Self>,
        filter: WorkspaceRestoreReportListFilter,
    ) -> Result<Vec<WorkspaceRestoreReportRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.list_workspace_restore_reports_blocking(&filter))
            .await
            .map_err(|_| Status::internal("workspace restore report list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn summarize_workspace_restore_activity_blocking(
        &self,
        filter: &WorkspaceRestoreActivityFilter,
    ) -> Result<WorkspaceRestoreActivitySummary, Status> {
        self.journal_store.summarize_workspace_restore_activity(filter).map_err(|error| {
            map_orchestrator_store_error("summarize workspace restore activity", error)
        })
    }

    #[allow(clippy::result_large_err)]
    pub async fn summarize_workspace_restore_activity(
        self: &Arc<Self>,
        filter: WorkspaceRestoreActivityFilter,
    ) -> Result<WorkspaceRestoreActivitySummary, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.summarize_workspace_restore_activity_blocking(&filter)
        })
        .await
        .map_err(|_| Status::internal("workspace restore activity worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn create_workspace_restore_report_blocking(
        &self,
        request: &WorkspaceRestoreReportCreateRequest,
    ) -> Result<WorkspaceRestoreReportRecord, Status> {
        self.journal_store
            .create_workspace_restore_report(request)
            .map_err(|error| map_orchestrator_store_error("create workspace restore report", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn create_workspace_restore_report(
        self: &Arc<Self>,
        request: WorkspaceRestoreReportCreateRequest,
    ) -> Result<WorkspaceRestoreReportRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.create_workspace_restore_report_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("workspace restore report worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn mark_workspace_checkpoint_restored_blocking(
        &self,
        request: &WorkspaceCheckpointRestoreMarkRequest,
    ) -> Result<(), Status> {
        self.journal_store.mark_workspace_checkpoint_restored(request).map_err(|error| {
            map_orchestrator_store_error("mark workspace checkpoint restored", error)
        })
    }

    #[allow(clippy::result_large_err)]
    pub async fn mark_workspace_checkpoint_restored(
        self: &Arc<Self>,
        request: WorkspaceCheckpointRestoreMarkRequest,
    ) -> Result<(), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.mark_workspace_checkpoint_restored_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("workspace checkpoint restore worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn create_orchestrator_background_task_blocking(
        &self,
        request: &OrchestratorBackgroundTaskCreateRequest,
    ) -> Result<OrchestratorBackgroundTaskRecord, Status> {
        self.journal_store.create_orchestrator_background_task(request).map_err(|error| {
            map_orchestrator_store_error("create orchestrator background task", error)
        })
    }

    #[allow(clippy::result_large_err)]
    pub async fn create_orchestrator_background_task(
        self: &Arc<Self>,
        request: OrchestratorBackgroundTaskCreateRequest,
    ) -> Result<OrchestratorBackgroundTaskRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.create_orchestrator_background_task_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("orchestrator background task worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn update_orchestrator_background_task_blocking(
        &self,
        request: &OrchestratorBackgroundTaskUpdateRequest,
    ) -> Result<(), Status> {
        self.journal_store.update_orchestrator_background_task(request).map_err(|error| {
            map_orchestrator_store_error("update orchestrator background task", error)
        })
    }

    #[allow(clippy::result_large_err)]
    pub async fn update_orchestrator_background_task(
        self: &Arc<Self>,
        request: OrchestratorBackgroundTaskUpdateRequest,
    ) -> Result<(), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.update_orchestrator_background_task_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("orchestrator background task update worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_orchestrator_background_tasks_blocking(
        &self,
        filter: &OrchestratorBackgroundTaskListFilter,
    ) -> Result<Vec<OrchestratorBackgroundTaskRecord>, Status> {
        self.journal_store.list_orchestrator_background_tasks(filter).map_err(|error| {
            map_orchestrator_store_error("list orchestrator background tasks", error)
        })
    }

    #[allow(clippy::result_large_err)]
    pub async fn list_orchestrator_background_tasks(
        self: &Arc<Self>,
        filter: OrchestratorBackgroundTaskListFilter,
    ) -> Result<Vec<OrchestratorBackgroundTaskRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.list_orchestrator_background_tasks_blocking(&filter)
        })
        .await
        .map_err(|_| Status::internal("orchestrator background task list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn get_orchestrator_background_task_blocking(
        &self,
        task_id: &str,
    ) -> Result<Option<OrchestratorBackgroundTaskRecord>, Status> {
        self.journal_store.get_orchestrator_background_task(task_id).map_err(|error| {
            map_orchestrator_store_error("load orchestrator background task", error)
        })
    }

    #[allow(clippy::result_large_err)]
    pub async fn get_orchestrator_background_task(
        self: &Arc<Self>,
        task_id: String,
    ) -> Result<Option<OrchestratorBackgroundTaskRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.get_orchestrator_background_task_blocking(task_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("orchestrator background task detail worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn upsert_learning_candidate_blocking(
        &self,
        request: &LearningCandidateCreateRequest,
    ) -> Result<LearningCandidateRecord, Status> {
        self.journal_store
            .upsert_learning_candidate(request)
            .map_err(|error| map_orchestrator_store_error("upsert learning candidate", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn upsert_learning_candidate(
        self: &Arc<Self>,
        request: LearningCandidateCreateRequest,
    ) -> Result<LearningCandidateRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.upsert_learning_candidate_blocking(&request))
            .await
            .map_err(|_| Status::internal("learning candidate upsert worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn review_learning_candidate_blocking(
        &self,
        request: &LearningCandidateReviewRequest,
    ) -> Result<LearningCandidateRecord, Status> {
        self.journal_store
            .review_learning_candidate(request)
            .map_err(|error| map_orchestrator_store_error("review learning candidate", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn review_learning_candidate(
        self: &Arc<Self>,
        request: LearningCandidateReviewRequest,
    ) -> Result<LearningCandidateRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.review_learning_candidate_blocking(&request))
            .await
            .map_err(|_| Status::internal("learning candidate review worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_learning_candidates_blocking(
        &self,
        filter: &LearningCandidateListFilter,
    ) -> Result<Vec<LearningCandidateRecord>, Status> {
        self.journal_store
            .list_learning_candidates(filter)
            .map_err(|error| map_orchestrator_store_error("list learning candidates", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn list_learning_candidates(
        self: &Arc<Self>,
        filter: LearningCandidateListFilter,
    ) -> Result<Vec<LearningCandidateRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.list_learning_candidates_blocking(&filter))
            .await
            .map_err(|_| Status::internal("learning candidate list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn learning_candidate_history_blocking(
        &self,
        candidate_id: &str,
    ) -> Result<Vec<LearningCandidateHistoryRecord>, Status> {
        self.journal_store
            .learning_candidate_history(candidate_id)
            .map_err(|error| map_orchestrator_store_error("list learning candidate history", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn learning_candidate_history(
        self: &Arc<Self>,
        candidate_id: String,
    ) -> Result<Vec<LearningCandidateHistoryRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.learning_candidate_history_blocking(candidate_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("learning candidate history worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn upsert_learning_preference_blocking(
        &self,
        request: &LearningPreferenceUpsertRequest,
    ) -> Result<LearningPreferenceRecord, Status> {
        self.journal_store
            .upsert_learning_preference(request)
            .map_err(|error| map_orchestrator_store_error("upsert learning preference", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn upsert_learning_preference(
        self: &Arc<Self>,
        request: LearningPreferenceUpsertRequest,
    ) -> Result<LearningPreferenceRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.upsert_learning_preference_blocking(&request))
            .await
            .map_err(|_| Status::internal("learning preference upsert worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_learning_preferences_blocking(
        &self,
        filter: &LearningPreferenceListFilter,
    ) -> Result<Vec<LearningPreferenceRecord>, Status> {
        self.journal_store
            .list_learning_preferences(filter)
            .map_err(|error| map_orchestrator_store_error("list learning preferences", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn list_learning_preferences(
        self: &Arc<Self>,
        filter: LearningPreferenceListFilter,
    ) -> Result<Vec<LearningPreferenceRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.list_learning_preferences_blocking(&filter))
            .await
            .map_err(|_| Status::internal("learning preference list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    pub(crate) fn orchestrator_tape_snapshot_blocking(
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
            let cache_context = RequestContext {
                principal: result.principal.clone(),
                device_id: result.device_id.clone(),
                channel: result.channel.clone(),
            };
            let cached_outcome = tool_approval_outcome_from_record(&result, decision);
            self.remember_tool_approval(
                &cache_context,
                result.session_id.as_str(),
                result.subject_id.as_str(),
                &cached_outcome,
            );
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

    pub fn list_tool_posture_overrides(&self) -> Result<Vec<ToolPostureOverrideRecord>, Status> {
        self.tool_posture_registry.list_overrides().map_err(|error| {
            Status::internal(format!("failed to list tool posture overrides: {error}"))
        })
    }

    pub fn list_tool_posture_recommendation_actions(
        &self,
    ) -> Result<Vec<ToolPostureRecommendationActionRecord>, Status> {
        self.tool_posture_registry.list_recommendation_actions().map_err(|error| {
            Status::internal(format!("failed to list tool posture recommendation actions: {error}"))
        })
    }

    pub fn list_tool_posture_audit_events(
        &self,
    ) -> Result<Vec<ToolPostureAuditEventRecord>, Status> {
        self.tool_posture_registry.list_audit_events().map_err(|error| {
            Status::internal(format!("failed to list tool posture audit events: {error}"))
        })
    }

    pub fn upsert_tool_posture_override(
        &self,
        request: ToolPostureOverrideUpsertRequest,
    ) -> Result<ToolPostureOverrideRecord, Status> {
        self.tool_posture_registry.upsert_override(request).map_err(|error| {
            Status::internal(format!("failed to persist tool posture override: {error}"))
        })
    }

    pub fn clear_tool_posture_override(
        &self,
        request: ToolPostureOverrideClearRequest,
    ) -> Result<bool, Status> {
        self.tool_posture_registry.clear_override(request).map_err(|error| {
            Status::internal(format!("failed to clear tool posture override: {error}"))
        })
    }

    pub fn reset_tool_posture_scope(
        &self,
        request: ToolPostureScopeResetRequest,
    ) -> Result<Vec<ToolPostureOverrideRecord>, Status> {
        self.tool_posture_registry.reset_scope(request).map_err(|error| {
            Status::internal(format!("failed to reset tool posture scope: {error}"))
        })
    }

    pub fn record_tool_posture_recommendation_action(
        &self,
        request: ToolPostureRecommendationActionRequest,
    ) -> Result<ToolPostureRecommendationActionRecord, Status> {
        self.tool_posture_registry.record_recommendation_action(request).map_err(|error| {
            Status::internal(format!(
                "failed to persist tool posture recommendation action: {error}"
            ))
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

    #[allow(clippy::result_large_err)]
    pub async fn record_console_event(
        self: &Arc<Self>,
        context: &RequestContext,
        event: &str,
        details: Value,
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
                "details": details,
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
    async fn append_runtime_decision_event(
        self: &Arc<Self>,
        principal: String,
        device_id: String,
        channel: Option<String>,
        session_id: Option<String>,
        run_id: Option<String>,
        payload: RuntimeDecisionPayload,
    ) -> Result<(), Status> {
        let session_id = session_id.unwrap_or_else(|| Ulid::new().to_string());
        let run_id = run_id.unwrap_or_else(|| session_id.clone());
        self.record_journal_event(JournalAppendRequest {
            event_id: Ulid::new().to_string(),
            session_id,
            run_id,
            kind: common_v1::journal_event::EventKind::ToolExecuted as i32,
            actor: common_v1::journal_event::EventActor::System as i32,
            timestamp_unix_ms: current_unix_ms(),
            payload_json: json!({
                "event": payload.event_type.journal_event(),
                "payload": payload,
            })
            .to_string()
            .into_bytes(),
            principal,
            device_id,
            channel,
        })
        .await?;
        self.observability.record_runtime_decision_event(&payload);
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    pub async fn record_runtime_decision_event(
        self: &Arc<Self>,
        context: &RequestContext,
        session_id: Option<&str>,
        run_id: Option<&str>,
        payload: RuntimeDecisionPayload,
    ) -> Result<(), Status> {
        self.append_runtime_decision_event(
            context.principal.clone(),
            context.device_id.clone(),
            context.channel.clone(),
            session_id.map(ToOwned::to_owned),
            run_id.map(ToOwned::to_owned),
            payload,
        )
        .await
    }

    #[allow(clippy::result_large_err)]
    pub async fn record_system_runtime_decision_event(
        self: &Arc<Self>,
        principal: &str,
        device_id: &str,
        channel: Option<&str>,
        session_id: Option<&str>,
        run_id: Option<&str>,
        payload: RuntimeDecisionPayload,
    ) -> Result<(), Status> {
        self.append_runtime_decision_event(
            principal.to_owned(),
            device_id.to_owned(),
            channel.map(ToOwned::to_owned),
            session_id.map(ToOwned::to_owned),
            run_id.map(ToOwned::to_owned),
            payload,
        )
        .await
    }

    #[must_use]
    pub fn runtime_decision_actor_from_context(
        &self,
        context: &RequestContext,
        kind: RuntimeDecisionActorKind,
    ) -> RuntimeDecisionActor {
        RuntimeDecisionActor::new(
            kind,
            context.principal.clone(),
            context.device_id.clone(),
            context.channel.clone(),
        )
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

    pub fn configure_retrieval(&self, config: RetrievalRuntimeConfig) {
        match self.retrieval_config.write() {
            Ok(mut guard) => {
                *guard = config;
            }
            Err(poisoned) => {
                warn!("retrieval config lock poisoned while applying runtime config");
                let mut guard = poisoned.into_inner();
                *guard = config;
            }
        }
        self.clear_memory_search_cache();
    }

    pub fn configure_routines_runtime(&self, config: RoutinesRuntimeConfig) {
        match self.routines_runtime.write() {
            Ok(mut guard) => {
                *guard = Some(config);
            }
            Err(poisoned) => {
                warn!("routines runtime lock poisoned while applying runtime config");
                let mut guard = poisoned.into_inner();
                *guard = Some(config);
            }
        }
    }

    #[allow(clippy::result_large_err)]
    pub fn routines_runtime_config(&self) -> Result<RoutinesRuntimeConfig, Status> {
        match self.routines_runtime.read() {
            Ok(config) => config
                .clone()
                .ok_or_else(|| Status::failed_precondition("routines runtime is not configured")),
            Err(poisoned) => poisoned
                .into_inner()
                .clone()
                .ok_or_else(|| Status::failed_precondition("routines runtime is not configured")),
        }
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

    #[must_use]
    pub fn retrieval_config_snapshot(&self) -> RetrievalRuntimeConfig {
        match self.retrieval_config.read() {
            Ok(config) => config.clone(),
            Err(poisoned) => {
                warn!("retrieval config lock poisoned while reading runtime config");
                poisoned.into_inner().clone()
            }
        }
    }

    pub fn configure_learning(&self, config: LearningRuntimeConfig) {
        match self.learning_config.write() {
            Ok(mut guard) => {
                *guard = config;
            }
            Err(poisoned) => {
                warn!("learning config lock poisoned while applying runtime config");
                let mut guard = poisoned.into_inner();
                *guard = config;
            }
        }
    }

    #[must_use]
    pub fn learning_config_snapshot(&self) -> LearningRuntimeConfig {
        match self.learning_config.read() {
            Ok(config) => config.clone(),
            Err(poisoned) => {
                warn!("learning config lock poisoned while reading runtime config");
                poisoned.into_inner().clone()
            }
        }
    }

    #[must_use]
    pub fn channel_router_config_snapshot(&self) -> ChannelRouterConfig {
        self.config.channel_router.clone()
    }

    #[must_use]
    pub fn channel_router_config_hash(&self) -> String {
        self.channel_router.config_hash()
    }

    #[must_use]
    pub fn runtime_config_snapshot(&self) -> GatewayRuntimeConfigSnapshot {
        self.config.clone()
    }

    #[must_use]
    pub fn channel_router_validation_warnings(&self) -> Vec<String> {
        self.channel_router.validation_warnings()
    }

    #[must_use]
    pub fn channel_router_preview(&self, message: &ChannelInboundMessage) -> ChannelRoutePreview {
        self.channel_router.preview_route(message)
    }

    #[must_use]
    pub fn channel_router_pairing_snapshot(
        &self,
        channel: Option<&str>,
    ) -> Vec<ChannelPairingSnapshot> {
        self.channel_router.pairing_snapshot(channel)
    }

    pub fn channel_router_mint_pairing_code(
        &self,
        channel: &str,
        issued_by: &str,
        ttl_ms: Option<u64>,
    ) -> Result<PairingCodeRecord, Status> {
        self.channel_router
            .mint_pairing_code(channel, issued_by, ttl_ms)
            .map_err(|reason| Status::failed_precondition(reason.as_str()))
    }

    #[must_use]
    pub fn channel_router_consume_pairing_code(
        &self,
        channel: &str,
        sender_identity: Option<&str>,
        code: &str,
        pending_ttl_ms: Option<u64>,
    ) -> PairingConsumeOutcome {
        self.channel_router.consume_pairing_code(channel, sender_identity, code, pending_ttl_ms)
    }

    #[must_use]
    pub fn channel_router_attach_pairing_pending_approval(
        &self,
        channel: &str,
        sender_identity: &str,
        approval_id: &str,
    ) -> bool {
        self.channel_router
            .attach_pairing_pending_approval(channel, sender_identity, approval_id)
            .is_some()
    }

    #[must_use]
    pub fn channel_router_apply_pairing_approval(
        &self,
        approval_id: &str,
        approved: bool,
        decision_scope_ttl_ms: Option<i64>,
    ) -> PairingApprovalOutcome {
        self.channel_router.apply_pairing_approval(approval_id, approved, decision_scope_ttl_ms)
    }

    #[must_use]
    pub fn worker_fleet_policy(&self) -> WorkerFleetPolicy {
        WorkerFleetPolicy::default()
    }

    #[must_use]
    pub fn worker_fleet_snapshot(&self) -> WorkerFleetSnapshot {
        match self.worker_fleet.read() {
            Ok(manager) => manager.snapshot(),
            Err(poisoned) => {
                warn!("worker fleet lock poisoned while reading snapshot");
                poisoned.into_inner().snapshot()
            }
        }
    }

    #[allow(clippy::result_large_err)]
    #[allow(dead_code)]
    pub async fn register_networked_worker(
        self: &Arc<Self>,
        attestation: WorkerAttestation,
    ) -> Result<WorkerLifecycleEvent, Status> {
        let policy = self.worker_fleet_policy();
        let now_unix_ms = current_unix_ms();
        let event = match self.worker_fleet.write() {
            Ok(mut manager) => {
                manager.register_worker(attestation, &policy, now_unix_ms).map_err(|error| {
                    Status::failed_precondition(format!(
                        "networked worker registration failed: {error}"
                    ))
                })?
            }
            Err(poisoned) => {
                warn!("worker fleet lock poisoned while registering worker");
                poisoned.into_inner().register_worker(attestation, &policy, now_unix_ms).map_err(
                    |error| {
                        Status::failed_precondition(format!(
                            "networked worker registration failed: {error}"
                        ))
                    },
                )?
            }
        };
        self.record_networked_worker_lifecycle_event(&event).await?;
        Ok(event)
    }

    #[allow(clippy::result_large_err)]
    #[allow(dead_code)]
    pub async fn assign_networked_worker_lease(
        self: &Arc<Self>,
        worker_id: &str,
        request: WorkerLeaseRequest,
    ) -> Result<(WorkerLease, WorkerLifecycleEvent), Status> {
        let policy = self.worker_fleet_policy();
        let now_unix_ms = current_unix_ms();
        let assign_work = |manager: &mut WorkerFleetManager| {
            manager.assign_work(worker_id, request.clone(), &policy, now_unix_ms).map_err(|error| {
                Status::failed_precondition(format!(
                    "networked worker lease assignment failed: {error}"
                ))
            })
        };
        let (lease, event) = match self.worker_fleet.write() {
            Ok(mut manager) => assign_work(&mut manager)?,
            Err(poisoned) => {
                warn!("worker fleet lock poisoned while assigning lease");
                let mut manager = poisoned.into_inner();
                assign_work(&mut manager)?
            }
        };
        self.record_networked_worker_lifecycle_event(&event).await?;
        Ok((lease, event))
    }

    #[allow(clippy::result_large_err)]
    #[allow(dead_code)]
    pub async fn complete_networked_worker_lease(
        self: &Arc<Self>,
        worker_id: &str,
        cleanup_report: WorkerCleanupReport,
    ) -> Result<WorkerLifecycleEvent, Status> {
        let now_unix_ms = current_unix_ms();
        let event = match self.worker_fleet.write() {
            Ok(mut manager) => {
                manager.complete_work(worker_id, &cleanup_report, now_unix_ms).map_err(|error| {
                    Status::failed_precondition(format!("networked worker cleanup failed: {error}"))
                })?
            }
            Err(poisoned) => {
                warn!("worker fleet lock poisoned while completing worker lease");
                poisoned
                    .into_inner()
                    .complete_work(worker_id, &cleanup_report, now_unix_ms)
                    .map_err(|error| {
                        Status::failed_precondition(format!(
                            "networked worker cleanup failed: {error}"
                        ))
                    })?
            }
        };
        self.record_networked_worker_lifecycle_event(&event).await?;
        Ok(event)
    }

    #[allow(clippy::result_large_err)]
    #[allow(dead_code)]
    pub async fn reap_expired_networked_workers(
        self: &Arc<Self>,
    ) -> Result<Vec<WorkerLifecycleEvent>, Status> {
        let now_unix_ms = current_unix_ms();
        let events = match self.worker_fleet.write() {
            Ok(mut manager) => manager.reap_expired_workers(now_unix_ms),
            Err(poisoned) => {
                warn!("worker fleet lock poisoned while reaping expired workers");
                poisoned.into_inner().reap_expired_workers(now_unix_ms)
            }
        };
        for event in &events {
            self.record_networked_worker_lifecycle_event(event).await?;
        }
        Ok(events)
    }

    #[allow(clippy::result_large_err)]
    #[allow(dead_code)]
    async fn record_networked_worker_lifecycle_event(
        self: &Arc<Self>,
        event: &WorkerLifecycleEvent,
    ) -> Result<(), Status> {
        use palyra_common::runtime_preview::{
            RuntimeDecisionEventType, RuntimeDecisionTiming, RuntimeEntityRef,
            RuntimeResourceBudget,
        };

        self.record_system_runtime_decision_event(
            "system:networked-worker",
            "networked-worker",
            Some("system"),
            event.run_id.as_deref(),
            event.run_id.as_deref(),
            RuntimeDecisionPayload::new(
                RuntimeDecisionEventType::WorkerLeaseLifecycle,
                RuntimeDecisionActor::new(
                    RuntimeDecisionActorKind::Worker,
                    "system:networked-worker",
                    "networked-worker",
                    Some("system".to_owned()),
                ),
                event.reason_code.clone(),
                "networked_workers.lease.preview",
                RuntimeDecisionTiming::observed(event.timestamp_unix_ms),
            )
            .with_input(RuntimeEntityRef::new("worker", "worker", event.worker_id.clone()))
            .with_output(
                RuntimeEntityRef::new("worker_lifecycle", "worker", event.worker_id.clone())
                    .with_state(event.state.as_str()),
            )
            .with_resource_budget(RuntimeResourceBudget::default())
            .with_details(json!({
                "run_id": event.run_id,
                "reason_code": event.reason_code,
            })),
        )
        .await
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

    pub(crate) fn clear_tool_approval_cache_for_session(
        &self,
        context: &RequestContext,
        session_id: &str,
    ) {
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

    pub(crate) fn resolve_cached_tool_approval(
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

    pub(crate) fn remember_tool_approval(
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
    pub async fn memory_maintenance_status(
        self: &Arc<Self>,
    ) -> Result<MemoryMaintenanceStatus, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .memory_maintenance_status()
                .map_err(|error| map_memory_store_error("load memory maintenance status", error))
        })
        .await
        .map_err(|_| Status::internal("memory maintenance status worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    pub async fn memory_embeddings_status(
        self: &Arc<Self>,
    ) -> Result<MemoryEmbeddingsStatus, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .memory_embeddings_status()
                .map_err(|error| map_memory_store_error("load memory embeddings status", error))
        })
        .await
        .map_err(|_| Status::internal("memory embeddings status worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    pub async fn run_memory_maintenance(
        self: &Arc<Self>,
        now_unix_ms: i64,
        retention: MemoryRetentionPolicy,
        next_vacuum_due_at_unix_ms: Option<i64>,
        next_maintenance_run_at_unix_ms: Option<i64>,
    ) -> Result<crate::journal::MemoryMaintenanceOutcome, Status> {
        let state = Arc::clone(self);
        let outcome = tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .run_memory_maintenance(&MemoryMaintenanceRequest {
                    now_unix_ms,
                    retention,
                    next_vacuum_due_at_unix_ms,
                    next_maintenance_run_at_unix_ms,
                })
                .map_err(|error| map_memory_store_error("run memory maintenance", error))
        })
        .await
        .map_err(|_| Status::internal("memory maintenance worker panicked"))??;
        if outcome.deleted_total_count > 0 {
            self.clear_memory_search_cache();
        }
        Ok(outcome)
    }

    #[allow(clippy::result_large_err)]
    pub async fn run_memory_embeddings_backfill(
        self: &Arc<Self>,
        batch_size: usize,
    ) -> Result<MemoryEmbeddingsBackfillOutcome, Status> {
        let state = Arc::clone(self);
        let outcome = tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .run_memory_embeddings_backfill(batch_size)
                .map_err(|error| map_memory_store_error("run memory embeddings backfill", error))
        })
        .await
        .map_err(|_| Status::internal("memory embeddings backfill worker panicked"))??;
        if outcome.updated_count > 0 {
            self.clear_memory_search_cache();
        }
        Ok(outcome)
    }

    #[allow(clippy::result_large_err)]
    pub async fn search_memory_with_diagnostics(
        self: &Arc<Self>,
        request: MemorySearchRequest,
    ) -> Result<MemorySearchOutcome, Status> {
        self.counters.memory_search_requests.fetch_add(1, Ordering::Relaxed);
        let total_started = Instant::now();
        let state = Arc::clone(self);
        let outcome = tokio::task::spawn_blocking(move || {
            let candidate_outcome = state
                .retrieval_backend
                .search_memory_candidate_outcome(&state.journal_store, &request)
                .map_err(|error| map_memory_store_error("search memory items", error))?;
            let fusion_started = Instant::now();
            let hits = score_memory_candidates(
                candidate_outcome.candidates,
                request.min_score,
                &state.retrieval_config_snapshot(),
            );
            let diagnostics = complete_retrieval_diagnostics(
                candidate_outcome.diagnostics,
                elapsed_millis(fusion_started),
                hits.len() as u64,
                elapsed_millis(total_started),
            );
            Ok::<_, Status>(MemorySearchOutcome { hits, diagnostics })
        })
        .await
        .map_err(|_| Status::internal("memory search worker panicked"))??;
        if outcome.diagnostics.latency_budget_exceeded {
            warn!(
                elapsed_ms = outcome.diagnostics.total_latency_ms,
                budget_ms = outcome.diagnostics.latency_budget_ms,
                "memory search exceeded latency budget"
            );
        }
        Ok(outcome)
    }

    #[allow(clippy::result_large_err)]
    pub async fn search_memory(
        self: &Arc<Self>,
        request: MemorySearchRequest,
    ) -> Result<Vec<MemorySearchHit>, Status> {
        self.counters.memory_search_requests.fetch_add(1, Ordering::Relaxed);
        let cache_key = memory_search_cache_key(&request);
        let now_unix_ms = current_unix_ms();
        let cached_hits = match self.memory_search_cache.lock() {
            Ok(mut cache) => match cache.get(cache_key.as_str()) {
                Some(entry)
                    if entry
                        .expires_at_unix_ms
                        .is_some_and(|expires_at| expires_at <= now_unix_ms) =>
                {
                    cache.remove(cache_key.as_str());
                    None
                }
                Some(entry) => Some(entry.hits.clone()),
                None => None,
            },
            Err(poisoned) => {
                warn!("memory search cache lock poisoned while reading cache");
                let mut cache = poisoned.into_inner();
                match cache.get(cache_key.as_str()) {
                    Some(entry)
                        if entry
                            .expires_at_unix_ms
                            .is_some_and(|expires_at| expires_at <= now_unix_ms) =>
                    {
                        cache.remove(cache_key.as_str());
                        None
                    }
                    Some(entry) => Some(entry.hits.clone()),
                    None => None,
                }
            }
        };
        if let Some(cached) = cached_hits {
            self.counters.memory_search_cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(cached);
        }

        let started_at = Instant::now();
        let state = Arc::clone(self);
        let results = tokio::task::spawn_blocking(move || {
            let candidates = state
                .retrieval_backend
                .search_memory_candidates(&state.journal_store, &request)
                .map_err(|error| map_memory_store_error("search memory items", error))?;
            Ok::<_, Status>(score_memory_candidates(
                candidates,
                request.min_score,
                &state.retrieval_config_snapshot(),
            ))
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
                cache.insert(
                    cache_key,
                    CachedMemorySearchEntry {
                        hits: results.clone(),
                        expires_at_unix_ms: Self::cached_memory_search_expires_at(&results),
                    },
                );
            }
            Err(poisoned) => {
                warn!("memory search cache lock poisoned while writing cache");
                let mut cache = poisoned.into_inner();
                if cache.len() >= MEMORY_SEARCH_CACHE_CAPACITY {
                    if let Some(first_key) = cache.keys().next().cloned() {
                        cache.remove(first_key.as_str());
                    }
                }
                cache.insert(
                    cache_key,
                    CachedMemorySearchEntry {
                        hits: results.clone(),
                        expires_at_unix_ms: Self::cached_memory_search_expires_at(&results),
                    },
                );
            }
        }
        Ok(results)
    }

    #[allow(clippy::result_large_err)]
    pub async fn workspace_document_by_path(
        self: &Arc<Self>,
        principal: String,
        channel: Option<String>,
        agent_id: Option<String>,
        path: String,
        include_deleted: bool,
    ) -> Result<Option<WorkspaceDocumentRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .workspace_document_by_path(
                    principal.as_str(),
                    channel.as_deref(),
                    agent_id.as_deref(),
                    path.as_str(),
                    include_deleted,
                )
                .map_err(|error| map_memory_store_error("load workspace document", error))
        })
        .await
        .map_err(|_| Status::internal("workspace document worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    pub async fn list_workspace_documents(
        self: &Arc<Self>,
        filter: WorkspaceDocumentListFilter,
    ) -> Result<Vec<WorkspaceDocumentRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .list_workspace_documents(&filter)
                .map_err(|error| map_memory_store_error("list workspace documents", error))
        })
        .await
        .map_err(|_| Status::internal("workspace document list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    pub async fn upsert_workspace_document(
        self: &Arc<Self>,
        request: WorkspaceDocumentWriteRequest,
    ) -> Result<WorkspaceDocumentRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .upsert_workspace_document(&request)
                .map_err(|error| map_memory_store_error("upsert workspace document", error))
        })
        .await
        .map_err(|_| Status::internal("workspace document write worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    pub async fn move_workspace_document(
        self: &Arc<Self>,
        request: WorkspaceDocumentMoveRequest,
    ) -> Result<WorkspaceDocumentRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .move_workspace_document(&request)
                .map_err(|error| map_memory_store_error("move workspace document", error))
        })
        .await
        .map_err(|_| Status::internal("workspace document move worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    pub async fn soft_delete_workspace_document(
        self: &Arc<Self>,
        request: WorkspaceDocumentDeleteRequest,
    ) -> Result<WorkspaceDocumentRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .soft_delete_workspace_document(&request)
                .map_err(|error| map_memory_store_error("delete workspace document", error))
        })
        .await
        .map_err(|_| Status::internal("workspace document delete worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    pub async fn list_workspace_document_versions(
        self: &Arc<Self>,
        document_id: String,
        limit: usize,
    ) -> Result<Vec<WorkspaceDocumentVersionRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .list_workspace_document_versions(document_id.as_str(), limit)
                .map_err(|error| map_memory_store_error("list workspace document versions", error))
        })
        .await
        .map_err(|_| Status::internal("workspace document versions worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    pub async fn set_workspace_document_pinned(
        self: &Arc<Self>,
        principal: String,
        channel: Option<String>,
        agent_id: Option<String>,
        path: String,
        pinned: bool,
    ) -> Result<Option<WorkspaceDocumentRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .set_workspace_document_pinned(
                    principal.as_str(),
                    channel.as_deref(),
                    agent_id.as_deref(),
                    path.as_str(),
                    pinned,
                )
                .map_err(|error| map_memory_store_error("pin workspace document", error))
        })
        .await
        .map_err(|_| Status::internal("workspace document pin worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    pub async fn record_workspace_document_recall(
        self: &Arc<Self>,
        document_id: String,
        recalled_at_unix_ms: i64,
    ) -> Result<(), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .record_workspace_document_recall(document_id.as_str(), recalled_at_unix_ms)
                .map_err(|error| map_memory_store_error("record workspace recall", error))
        })
        .await
        .map_err(|_| Status::internal("workspace recall worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    pub async fn bootstrap_workspace(
        self: &Arc<Self>,
        request: WorkspaceBootstrapRequest,
    ) -> Result<WorkspaceBootstrapOutcome, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .bootstrap_workspace(&request)
                .map_err(|error| map_memory_store_error("bootstrap workspace", error))
        })
        .await
        .map_err(|_| Status::internal("workspace bootstrap worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    pub async fn search_workspace_documents_with_diagnostics(
        self: &Arc<Self>,
        request: WorkspaceSearchRequest,
    ) -> Result<WorkspaceSearchOutcome, Status> {
        let total_started = Instant::now();
        let state = Arc::clone(self);
        let outcome = tokio::task::spawn_blocking(move || {
            let candidate_outcome = state
                .retrieval_backend
                .search_workspace_candidate_outcome(&state.journal_store, &request)
                .map_err(|error| map_memory_store_error("search workspace documents", error))?;
            let fusion_started = Instant::now();
            let hits = score_workspace_candidates(
                candidate_outcome.candidates,
                request.min_score,
                &state.retrieval_config_snapshot(),
            );
            let diagnostics = complete_retrieval_diagnostics(
                candidate_outcome.diagnostics,
                elapsed_millis(fusion_started),
                hits.len() as u64,
                elapsed_millis(total_started),
            );
            Ok::<_, Status>(WorkspaceSearchOutcome { hits, diagnostics })
        })
        .await
        .map_err(|_| Status::internal("workspace search worker panicked"))??;
        if outcome.diagnostics.latency_budget_exceeded {
            warn!(
                elapsed_ms = outcome.diagnostics.total_latency_ms,
                budget_ms = outcome.diagnostics.latency_budget_ms,
                "workspace search exceeded latency budget"
            );
        }
        Ok(outcome)
    }

    #[allow(clippy::result_large_err)]
    pub async fn search_workspace_documents(
        self: &Arc<Self>,
        request: WorkspaceSearchRequest,
    ) -> Result<Vec<WorkspaceSearchHit>, Status> {
        Ok(self.search_workspace_documents_with_diagnostics(request).await?.hits)
    }

    pub fn record_cron_trigger_fired(&self) {
        self.counters.cron_triggers_fired.fetch_add(1, Ordering::Relaxed);
    }
}
