//! Gateway runtime state: [`GatewayRuntimeState`] is the daemon-wide hub that
//! every transport (gRPC, HTTP console, QUIC) shares, together with the
//! config/snapshot types it exposes.
//!
//! Relationship to its neighbours: `gateway.rs` (the parent module) owns tool
//! dispatch, approval prompting, and run cleanup; `application::run_stream`
//! drives the streaming agent loop. Both lean on this file for persistence and
//! policy: orchestrator run lifecycle records (queued -> running -> waiting ->
//! terminal, parsed via `RunLifecyclePhase`), tape pagination, cancel flags and
//! run-completion wakeups (`orchestrator_run_notify`), approval decision
//! caching, provider lease admission, and the journal-backed stores for
//! sessions, memory, workspace, cron, flows, skills, and workers.
//!
//! Conventions used throughout this file:
//! - Journal/SQLite access is synchronous. Each `*_blocking` method performs
//!   the query and maps `JournalError` into a gRPC `Status`; its `pub async`
//!   twin moves that call onto `tokio::task::spawn_blocking` so transports
//!   never block the runtime. The async wrappers fail with `Status::internal`
//!   when the worker panics.
//! - `std::sync` locks guard in-memory state only and are never held across an
//!   `.await`. Poisoned locks are recovered with `into_inner()` (warn and
//!   continue) because the guarded data remains structurally valid.
//! - Counters are relaxed atomics; they feed status snapshots, never control
//!   flow.

use super::*;
use crate::agents::{
    AgentBindingOutcome, AgentBindingQuery, AgentBindingRequest, AgentDeleteOutcome, AgentListPage,
    AgentRecord, AgentResolveOutcome, AgentResolveRequest, AgentSetDefaultOutcome,
    AgentUnbindOutcome, AgentUnbindRequest, SessionAgentBinding,
};
use crate::application::auth::map_auth_profile_error;
use crate::journal::state_health::{
    JournalHashChainVerificationReport, JournalHashVerificationScope, JournalHealthReport,
    JournalStateRepairReport, JournalStateRepairRequest, JournalWalCheckpointMode,
    JournalWalCheckpointReport, SidecarIndexDescriptor,
};
use crate::journal::{
    CommitmentCreateRequest, CommitmentDeliveryAttemptCreateRequest,
    CommitmentDeliveryAttemptRecord, CommitmentEventRecord, CommitmentListFilter, CommitmentRecord,
    CommitmentSourceRecord, CommitmentUpdateRequest, FlowBundleRecord, FlowCreateRequest,
    FlowListFilter, FlowRecord, FlowStepRecord, FlowStepUpdateRequest, FlowTransitionRequest,
    IdempotencyBeginRequest, IdempotencyCompleteRequest, IdempotencyFailRequest,
    LearningCandidateCreateRequest, LearningCandidateEvalCreateRequest,
    LearningCandidateEvalRecord, LearningCandidateHistoryRecord, LearningCandidateListFilter,
    LearningCandidateRecord, LearningCandidateReviewRequest, LearningCandidateRolloutCreateRequest,
    LearningCandidateRolloutRecord, LearningPreferenceListFilter, LearningPreferenceRecord,
    LearningPreferenceUpsertRequest, MemoryEmbeddingsStatus, MemoryItemLifecycleUpdateRequest,
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
    OrchestratorStartupRunRecoveryReport, OrchestratorUsageQuery, OrchestratorUsageRunRecord,
    OrchestratorUsageSessionRecord, OrchestratorUsageSummary, RecallArtifactCreateRequest,
    RecallArtifactListFilter, RecallArtifactRecord, RetrievalBranchDiagnostics,
    SessionProjectContextStateCopyRequest, SessionProjectContextStateRecord,
    SessionProjectContextStateUpsertRequest, SessionSearchOutcome, SessionSearchRequest,
    ToolJobAttachRequest, ToolJobCreateRequest, ToolJobRecord, ToolJobRetryRequest,
    ToolJobTailAppendRequest, ToolJobTailPage, ToolJobTailReadRequest, ToolJobTransitionRequest,
    ToolJobsListFilter, ToolResultArtifactCreateRequest, ToolResultArtifactReadRequest,
    WorkItemCreateRequest, WorkItemEventRecord, WorkItemListFilter, WorkItemRecord,
    WorkItemUpdateRequest, WorkspaceBootstrapOutcome, WorkspaceBootstrapRequest,
    WorkspaceCheckpointCreateRequest, WorkspaceCheckpointFilePayload,
    WorkspaceCheckpointFileRecord, WorkspaceCheckpointListFilter,
    WorkspaceCheckpointPairLinkRequest, WorkspaceCheckpointRecord,
    WorkspaceCheckpointRestoreMarkRequest, WorkspaceDocumentDeleteRequest,
    WorkspaceDocumentListFilter, WorkspaceDocumentMoveRequest, WorkspaceDocumentRecord,
    WorkspaceDocumentVersionRecord, WorkspaceDocumentWriteRequest, WorkspaceRestoreActivityFilter,
    WorkspaceRestoreActivitySummary, WorkspaceRestoreReportCreateRequest,
    WorkspaceRestoreReportListFilter, WorkspaceRestoreReportRecord, WorkspaceScoreBreakdown,
    WorkspaceSearchHit, WorkspaceSearchRequest,
};
use crate::provider_leases::{
    ProviderCredentialFeedbackKind, ProviderCredentialFeedbackRequest, ProviderLeaseAcquireError,
    ProviderLeaseAcquireRequest, ProviderLeaseExecutionContext, ProviderLeaseManager,
    ProviderLeaseManagerSnapshot, ProviderLeasePreviewRequest, ProviderLeasePreviewSnapshot,
};
use crate::retrieval::{
    lexical_overlap_score, recency_score as retrieval_recency_score, score_memory_candidates,
    score_with_profile, score_workspace_candidates, workspace_source_quality,
    ExternalRetrievalRuntime, RetrievalBackend, RetrievalBackendSnapshot, RetrievalRuntimeConfig,
    RetrievalSourceProfileKind,
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
use palyra_auth::{AuthHealthReport, AuthProfileFailureKind};
use palyra_common::replay_bundle::ReplayBundle;
use palyra_common::runtime_contracts::{
    ArtifactReadResponse, IdempotencyReplayDecision, RunLifecyclePhase, StableErrorEnvelope,
    ToolResultArtifactRef,
};
use palyra_common::runtime_preview::{
    RuntimeDecisionActor, RuntimeDecisionActorKind, RuntimeDecisionPayload,
};
use palyra_workerd::{
    WorkerAttestation, WorkerCleanupReport, WorkerFleetManager, WorkerFleetPolicy,
    WorkerFleetSnapshot, WorkerLease, WorkerLeaseRequest, WorkerLifecycleEvent,
};
use ring::hmac;
use std::path::PathBuf;
use tokio::sync::{Mutex as AsyncMutex, Notify};

mod external_retrieval;

fn sign_canvas_hmac_sha256(secret: &[u8], domain: &str, parts: &[&[u8]]) -> String {
    let key = hmac::Key::new(hmac::HMAC_SHA256, secret);
    let mut context = hmac::Context::with_key(&key);
    context.update(domain.as_bytes());
    for part in parts {
        context.update(&(part.len() as u64).to_be_bytes());
        context.update(part);
    }
    URL_SAFE_NO_PAD.encode(context.sign().as_ref())
}

/// Immutable copy of the daemon configuration the gateway runtime was started with.
///
/// Captured once at startup and shared read-only. Settings that can be retuned
/// at runtime (memory, retrieval, learning, routines) live separately in
/// [`GatewayRuntimeState`] behind locks.
#[derive(Debug, Clone)]
pub struct GatewayRuntimeConfigSnapshot {
    pub grpc_bind_addr: String,
    pub grpc_port: u16,
    pub quic_bind_addr: String,
    pub quic_port: u16,
    pub quic_enabled: bool,
    pub orchestrator_runloop_v1_enabled: bool,
    pub model_provider_request_timeout_ms: u64,
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
    pub code_intel: crate::config::CodeIntelConfig,
    pub tool_call: ToolCallConfig,
    pub tool_catalog_policy: crate::application::tool_registry::ToolCatalogPolicySnapshot,
    pub http_fetch: HttpFetchRuntimeConfig,
    pub browser_service: BrowserServiceRuntimeConfig,
    pub canvas_host: CanvasHostRuntimeConfig,
    pub smart_routing: SmartRoutingRuntimeConfig,
}

/// Cursor-paginated filter for listing sessions scoped to a principal and
/// device (plus optional channel and case-insensitive search).
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

/// Cursor-paginated filter for listing sessions across all devices of one
/// principal.
#[derive(Debug, Clone)]
pub struct ListPrincipalOrchestratorSessionsRequest {
    pub after_session_key: Option<String>,
    pub principal: String,
    pub include_archived: bool,
    pub requested_limit: Option<usize>,
    pub search_query: Option<String>,
}

/// Parameters for [`GatewayRuntimeState::wait_for_orchestrator_run`].
///
/// `return_on_waiting` makes the wait resolve as soon as the run parks in a
/// waiting phase (for example pending approval) instead of only on terminal
/// phases.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrchestratorRunWaitRequest {
    pub run_id: String,
    pub timeout: Duration,
    pub poll_interval: Duration,
    pub return_on_waiting: bool,
}

/// Final run snapshot observed by a wait, plus its parsed canonical phase.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrchestratorRunWaitOutcome {
    pub snapshot: OrchestratorRunStatusSnapshot,
    pub canonical_state: RunLifecyclePhase,
}

/// Live-tunable memory subsystem limits, auto-injection policy, and retention
/// policy.
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

/// Live-tunable learning/reflection pipeline settings; confidence thresholds
/// are expressed in basis points (10000 = 1.0).
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

/// Egress policy for the `http_fetch` tool: timeouts, size caps,
/// redirect/header/content-type allowlists, credential vault-ref allowlist,
/// and response caching.
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
    pub allowed_credential_vault_refs: Vec<String>,
    pub cache_enabled: bool,
    pub cache_ttl_ms: u64,
    pub max_cache_entries: usize,
}

/// Connection settings and response size caps for the external browser
/// service (`palyra-browserd`).
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

/// Canvas host limits: enablement, public base URL, token TTL, and
/// state/bundle/update-rate caps.
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
            auto_inject_enabled: true,
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

/// Resolved tool approval decision as consumed by the run stream, including
/// the scope and TTL that drive decision caching.
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

/// Session-scoped cache of resolved approval decisions.
///
/// `generations` counts cache invalidations per session prefix so that a
/// decision resolved before an invalidation cannot be written back afterwards
/// (see [`GatewayRuntimeState::remember_tool_approval_if_generation`]).
#[derive(Debug, Default)]
struct ToolApprovalCacheState {
    decisions: HashMap<String, CachedToolApprovalDecision>,
    generations: HashMap<String, u64>,
}

fn tool_approval_cache_generation(cache: &ToolApprovalCacheState, key_prefix: &str) -> u64 {
    cache.generations.get(key_prefix).copied().unwrap_or(0)
}

fn bump_tool_approval_cache_generation(cache: &mut ToolApprovalCacheState, key_prefix: &str) {
    let generation = cache.generations.entry(key_prefix.to_owned()).or_insert(0);
    *generation = generation.saturating_add(1);
}

/// Cached `http_fetch` tool response with an absolute expiry.
#[derive(Debug, Clone)]
pub(crate) struct CachedHttpFetchEntry {
    pub(crate) expires_at_unix_ms: i64,
    pub(crate) output_json: Vec<u8>,
}

/// Cached memory search hits; `expires_at_unix_ms` is the earliest TTL among
/// the hits so the cache never serves an already-expired item.
#[derive(Debug, Clone)]
pub(crate) struct CachedMemorySearchEntry {
    pub(crate) hits: Vec<MemorySearchHit>,
    pub(crate) expires_at_unix_ms: Option<i64>,
}

/// Memory search hits plus retrieval branch diagnostics for observability.
#[derive(Debug, Clone)]
pub(crate) struct MemorySearchOutcome {
    pub(crate) hits: Vec<MemorySearchHit>,
    pub(crate) diagnostics: RetrievalBranchDiagnostics,
}

/// Workspace document search hits plus retrieval branch diagnostics.
#[derive(Debug, Clone)]
pub(crate) struct WorkspaceSearchOutcome {
    pub(crate) hits: Vec<WorkspaceSearchHit>,
    pub(crate) diagnostics: RetrievalBranchDiagnostics,
}

/// Skill attribution attached to a tool call (skill id plus optional version).
#[derive(Debug, Clone)]
pub(crate) struct ToolSkillContext {
    pub(crate) skill_id: String,
    pub(crate) version: Option<String>,
}

impl ToolSkillContext {
    /// Creates a skill attribution from id and optional version.
    pub(crate) fn new(skill_id: String, version: Option<String>) -> Self {
        Self { skill_id, version }
    }

    /// The skill identifier.
    pub(crate) fn skill_id(&self) -> &str {
        self.skill_id.as_str()
    }

    /// The skill version, when one was specified.
    pub(crate) fn version(&self) -> Option<&str> {
        self.version.as_deref()
    }
}

/// Result of executing one approved tool proposal inside the run stream;
/// `Cancelled` means the run was cancelled while the tool was in flight.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum RunStreamToolExecutionOutcome {
    Completed {
        proposal_id: String,
        tool_name: String,
        outcome: crate::tool_protocol::ToolExecutionOutcome,
    },
    Cancelled,
}

/// Single canvas bundle asset: content type plus raw body bytes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CanvasAssetRecord {
    pub(crate) content_type: String,
    pub(crate) body: Vec<u8>,
}

/// Validated canvas bundle: assets keyed by normalized path, a content hash,
/// and the gateway-issued signature binding it to canvas/principal/session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CanvasBundleRecord {
    pub(crate) bundle_id: String,
    pub(crate) entrypoint_path: String,
    pub(crate) assets: HashMap<String, CanvasAssetRecord>,
    pub(crate) sha256: String,
    pub(crate) signature: String,
}

/// In-memory canvas state: current state JSON/version, bundle, parent-origin
/// allowlist, expiry, and the rolling per-minute update timestamps that back
/// the update rate limit.
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

/// Rendered HTML shell served at `/canvas/v1/frame/{canvas_id}` together with
/// the CSP header to attach to the response.
#[derive(Debug, Clone, Serialize)]
pub struct CanvasFrameDocument {
    pub canvas_id: String,
    pub html: String,
    pub csp: String,
    pub expires_at_unix_ms: i64,
}

/// Canvas asset payload plus the CSP header to serve with it.
#[derive(Debug, Clone)]
pub struct CanvasAssetResponse {
    pub content_type: String,
    pub body: Vec<u8>,
    pub csp: String,
}

/// Canvas state document returned by the HTTP polling endpoint.
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

/// Client-facing handle to an active canvas: frame/runtime URLs plus a signed
/// auth token bound to the canvas, principal, and session.
#[derive(Debug, Clone, Serialize)]
pub struct CanvasRuntimeDescriptor {
    pub canvas_id: String,
    pub frame_url: String,
    pub runtime_url: String,
    pub auth_token: String,
    pub expires_at_unix_ms: i64,
}

/// Maximum patch records fetched when assembling a canvas patch history
/// response (further trimmed by the byte budget below).
pub(crate) const CANVAS_PATCH_HISTORY_RESPONSE_ROW_LIMIT: usize = 100;
const CANVAS_PATCH_HISTORY_RESPONSE_BYTE_LIMIT: usize = 4 * 1024 * 1024;
const CANVAS_PATCH_HISTORY_RESPONSE_RECORD_OVERHEAD: usize = 256;

/// Signed canvas token claims: canvas/principal/session scope, validity
/// window, and a nonce making every issued token unique.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CanvasTokenPayload {
    pub(crate) canvas_id: String,
    pub(crate) principal: String,
    pub(crate) session_id: String,
    issued_at_unix_ms: i64,
    pub(crate) expires_at_unix_ms: i64,
    nonce: String,
}

/// Fixed-window request counter for one principal's vault operations.
#[derive(Debug, Clone, Copy)]
pub(crate) struct VaultRateLimitEntry {
    window_started_at: Instant,
    requests_in_window: u32,
}

/// Journal storage settings surfaced in status snapshots.
#[derive(Debug, Clone)]
pub struct GatewayJournalConfigSnapshot {
    pub db_path: PathBuf,
    pub hash_chain_enabled: bool,
}

/// Externally constructed collaborators injected into
/// [`GatewayRuntimeState::new_with_provider`].
#[rustfmt::skip]
pub struct GatewayRuntimeDependencies { pub model_provider: Arc<dyn ModelProvider>, pub vault: Arc<Vault>, pub auth_profile_registry: Option<Arc<AuthProfileRegistry>>, pub agent_registry: AgentRegistry, pub tool_posture_registry: ToolPostureRegistry, pub retrieval_backend: Arc<dyn RetrievalBackend>, pub external_retrieval_index: Arc<ExternalRetrievalRuntime>, pub conversation_bindings: ConversationBindingStore }

/// Maps internal lease preview reason codes to operator-facing wording.
/// Internal codes must not leak into user-visible errors (pinned by tests).
fn provider_lease_pressure_reason(preview: &ProviderLeasePreviewSnapshot) -> &'static str {
    match preview.reason.as_deref() {
        Some("shared_capacity_exhausted") => "shared provider capacity is exhausted",
        Some("foreground_waiters_present") => "foreground work is already queued",
        Some("foreground_capacity_reserved") => "foreground provider capacity is reserved",
        Some(reason) if reason.starts_with("credential_feedback:") => {
            "provider credential is cooling down after a provider error"
        }
        _ => "provider capacity is busy",
    }
}

fn provider_lease_deferred_status(
    lease_context: &ProviderLeaseExecutionContext,
    preview: ProviderLeasePreviewSnapshot,
) -> Status {
    let reason = provider_lease_pressure_reason(&preview);
    Status::resource_exhausted(format!(
        "model provider capacity is busy for {} on provider '{}' ({reason}); retry shortly or reduce concurrent agent runs",
        lease_context.task_label,
        lease_context.provider_id,
    ))
}

fn provider_lease_timeout_status(
    waited_ms: u64,
    lease_context: &ProviderLeaseExecutionContext,
    preview: ProviderLeasePreviewSnapshot,
) -> Status {
    let reason = provider_lease_pressure_reason(&preview);
    Status::resource_exhausted(format!(
        "model provider capacity is busy for {} on provider '{}' ({reason}); queued for {waited_ms} ms before timing out; retry shortly or reduce concurrent agent runs",
        lease_context.task_label,
        lease_context.provider_id,
    ))
}

/// Routines/objectives wiring installed late, once the scheduler is running
/// (see [`GatewayRuntimeState::configure_routines_runtime`]).
#[derive(Clone)]
pub(crate) struct RoutinesRuntimeConfig {
    pub registry: Arc<crate::routines::RoutineRegistry>,
    pub objectives: Arc<crate::objectives::ObjectiveRegistry>,
    pub auth: GatewayAuthConfig,
    pub grpc_url: String,
    pub scheduler_wake: Arc<Notify>,
    pub timezone_mode: crate::cron::CronTimezoneMode,
}

/// Live resources tied to a run (browser sessions, background process PIDs)
/// that terminal-run cleanup must release.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RunCleanupResources {
    pub(crate) browser_session_ids: Vec<String>,
    pub(crate) background_process_pids: Vec<u32>,
}

impl RunCleanupResources {
    /// True when no resources remain registered for the run.
    pub(crate) fn is_empty(&self) -> bool {
        self.browser_session_ids.is_empty() && self.background_process_pids.is_empty()
    }
}

/// Detached background resources created by a run and intentionally left out
/// of terminal run cleanup. They are reported in the terminal cleanup summary
/// so post-run verifiers and users get explicit stop/status commands.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct RunDetachedResources {
    pub(crate) background_processes: Vec<DetachedBackgroundProcessResource>,
}

impl RunDetachedResources {
    /// True when the run did not create any detached handoff resources.
    pub(crate) fn is_empty(&self) -> bool {
        self.background_processes.is_empty()
    }
}

/// One detached background-process handoff record.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DetachedBackgroundProcessResource {
    pub(crate) pid: u32,
    pub(crate) lifetime_mode: String,
    pub(crate) ports: Vec<u16>,
    pub(crate) lifetime_ms: Option<u64>,
    pub(crate) max_lifetime_ms: Option<u64>,
    pub(crate) start_command: Value,
    pub(crate) cleanup: Value,
}

/// Bounded dedupe ledger of browser sessions that already closed, so terminal
/// run cleanup does not try to close them again. Insertion order is kept so
/// the oldest entries can be evicted once capacity is reached.
#[derive(Debug, Default)]
struct ClosedBrowserSessionLedger {
    ids: HashSet<String>,
    order: VecDeque<String>,
}

impl ClosedBrowserSessionLedger {
    fn insert(&mut self, session_id: String) {
        if !self.ids.insert(session_id.clone()) {
            return;
        }
        self.order.push_back(session_id);
        while self.ids.len() > CLOSED_BROWSER_SESSION_LEDGER_CAPACITY {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            self.ids.remove(oldest.as_str());
        }
    }

    fn remove(&mut self, session_id: &str) {
        self.ids.remove(session_id);
        self.order.retain(|existing| existing != session_id);
    }

    fn contains(&self, session_id: &str) -> bool {
        self.ids.contains(session_id)
    }
}

#[derive(Debug, Default)]
struct RunParameterDeltaCache {
    entries: HashMap<String, String>,
    order: VecDeque<String>,
}

impl RunParameterDeltaCache {
    fn insert(&mut self, run_id: &str, parameter_delta_json: &str) {
        let run_id = run_id.trim();
        let parameter_delta_json = parameter_delta_json.trim();
        if run_id.is_empty() || parameter_delta_json.is_empty() {
            return;
        }
        if let Some(existing) = self.entries.get_mut(run_id) {
            *existing = parameter_delta_json.to_owned();
            return;
        }

        self.entries.insert(run_id.to_owned(), parameter_delta_json.to_owned());
        self.order.push_back(run_id.to_owned());
        while self.entries.len() > RUN_PARAMETER_DELTA_CACHE_CAPACITY {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            self.entries.remove(oldest.as_str());
        }
    }

    fn get(&self, run_id: &str) -> Option<String> {
        let run_id = run_id.trim();
        if run_id.is_empty() {
            return None;
        }
        self.entries.get(run_id).cloned()
    }
}

/// Daemon-wide gateway state shared (via `Arc`) by every transport surface.
///
/// Owns the journal store and all in-memory runtime state: counters, caches
/// (memory search, http fetch, tool approvals, run launch context), run cleanup bookkeeping,
/// canvas records, provider leases, the channel router, and the worker fleet.
/// All mutation goes through interior mutability; methods take `&self` (or
/// `&Arc<Self>` for the `spawn_blocking` wrappers).
pub struct GatewayRuntimeState {
    pub(crate) started_at: Instant,
    pub(crate) build: BuildSnapshot,
    pub(crate) config: GatewayRuntimeConfigSnapshot,
    pub(crate) journal_config: GatewayJournalConfigSnapshot,
    pub(crate) counters: RuntimeCounters,
    pub(crate) journal_store: JournalStore,
    revoked_certificate_count: usize,
    model_provider: RwLock<Arc<dyn ModelProvider>>,
    model_provider_generation: AtomicU64,
    auth_profile_registry: Option<Arc<AuthProfileRegistry>>,
    pub(crate) vault: Arc<Vault>,
    pub(crate) memory_config: RwLock<MemoryRuntimeConfig>,
    pub(crate) retrieval_config: RwLock<RetrievalRuntimeConfig>,
    pub(crate) learning_config: RwLock<LearningRuntimeConfig>,
    pub(crate) memory_search_cache: Mutex<HashMap<String, CachedMemorySearchEntry>>,
    pub(crate) http_fetch_cache: Mutex<HashMap<String, CachedHttpFetchEntry>>,
    recent_context_assembly_traces: Mutex<Vec<Value>>,
    tool_approval_cache: Mutex<ToolApprovalCacheState>,
    run_parameter_delta_cache: Mutex<RunParameterDeltaCache>,
    run_cleanup_resources: Mutex<HashMap<String, RunCleanupResources>>,
    run_detached_resources: Mutex<HashMap<String, RunDetachedResources>>,
    closed_browser_sessions: Mutex<ClosedBrowserSessionLedger>,
    worker_fleet: RwLock<WorkerFleetManager>,
    pub(crate) provider_leases: ProviderLeaseManager,
    pub(crate) retrieval_backend: Arc<dyn RetrievalBackend>,
    pub(crate) external_retrieval_index: Arc<ExternalRetrievalRuntime>,
    pub(crate) tool_posture_registry: ToolPostureRegistry,
    pub(crate) routines_runtime: RwLock<Option<RoutinesRuntimeConfig>>,
    pub(crate) vault_rate_limit: Mutex<HashMap<String, VaultRateLimitEntry>>,
    pub(crate) orchestrator_run_notify: Arc<Notify>,
    canvas_records: Mutex<HashMap<String, CanvasRecord>>,
    canvas_signing_secret: [u8; 32],
    agent_registry: AgentRegistry,
    pub(crate) channel_router: ChannelRouter,
    pub(crate) inbound_coalescer: InboundCoalescer,
    pub(crate) conversation_bindings: ConversationBindingStore,
    pub(crate) observability: Arc<crate::observability::ObservabilityState>,
    pub(crate) self_healing: Arc<SelfHealingState>,
}

/// Relaxed atomic counters backing [`CountersSnapshot`]. All values count
/// since process start except `journal_events`, which is seeded from the
/// persisted journal total.
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

/// Build metadata reported in status responses.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct BuildSnapshot {
    pub(crate) version: String,
    pub(crate) git_hash: String,
    pub(crate) build_profile: String,
}

/// Top-level admin status document returned by the gateway status endpoints.
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

/// Listener addresses and transport toggles in the status document.
#[derive(Debug, Clone, Serialize)]
pub struct TransportSnapshot {
    pub grpc_bind_addr: String,
    pub grpc_port: u16,
    pub quic_bind_addr: String,
    pub quic_port: u16,
    pub quic_enabled: bool,
}

/// Security posture flags in the status document.
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

/// Journal storage details in the status document.
#[derive(Debug, Clone, Serialize)]
pub struct StorageSnapshot {
    pub journal_db_path: String,
    pub journal_hash_chain_enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_event_hash: Option<String>,
}

/// Point-in-time copy of [`RuntimeCounters`] for status responses.
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

/// Agent registry summary in the status document; session ids are redacted.
#[derive(Debug, Clone, Serialize)]
pub struct AgentRuntimeSnapshot {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_agent_id: Option<String>,
    pub agent_count: usize,
    pub active_session_bindings: Vec<AgentSessionBindingSnapshot>,
}

/// One active session-to-agent binding (redacted session id).
#[derive(Debug, Clone, Serialize)]
pub struct AgentSessionBindingSnapshot {
    pub session_id_redacted: String,
    pub agent_id: String,
}

/// OAuth refresh attempt counts for a single provider.
#[derive(Debug, Clone, Serialize, Default)]
pub struct AuthProviderRefreshMetricsSnapshot {
    pub provider: String,
    pub attempts: u64,
    pub successes: u64,
    pub failures: u64,
}

/// Aggregate OAuth refresh metrics, broken down per provider.
#[derive(Debug, Clone, Serialize, Default)]
pub struct AuthRefreshMetricsSnapshot {
    pub attempts: u64,
    pub successes: u64,
    pub failures: u64,
    pub by_provider: Vec<AuthProviderRefreshMetricsSnapshot>,
}

/// Admin-facing auth health: profile summary, expiry buckets, and refresh
/// metrics.
#[derive(Debug, Clone, Serialize, Default)]
pub struct AuthAdminStatusSnapshot {
    pub summary: AuthHealthSummary,
    pub expiry_distribution: AuthExpiryDistribution,
    pub refresh_metrics: AuthRefreshMetricsSnapshot,
}

/// Most recent journal events plus chain metadata for admin inspection.
#[derive(Debug, Clone, Serialize)]
pub struct JournalRecentSnapshot {
    pub total_events: u64,
    pub hash_chain_enabled: bool,
    pub events: Vec<JournalEventRecord>,
}

/// Page of orchestrator tape events with entry/byte budget bookkeeping;
/// `next_after_seq` is set when more events remain.
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

/// Run state right after a cancel request was recorded.
#[derive(Debug, Clone, Serialize)]
pub struct RunCancelSnapshot {
    pub run_id: String,
    pub state: String,
    pub cancel_requested: bool,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, Default)]
struct AuthProviderRefreshCounters {
    attempts: u64,
    successes: u64,
    failures: u64,
}

/// Mutable refresh-metrics accumulator behind [`AuthRuntimeState`].
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

/// OAuth profile refresh runtime: serializes refreshes per profile, records
/// refresh metrics, and produces auth health snapshots for admin surfaces.
#[derive(Clone)]
pub struct AuthRuntimeState {
    registry: Arc<AuthProfileRegistry>,
    refresh_adapter: Arc<dyn OAuthRefreshAdapter>,
    refresh_metrics: Arc<AuthRefreshMetricsState>,
    refresh_locks: Arc<Mutex<HashMap<String, Arc<AsyncMutex<()>>>>>,
}

impl AuthRuntimeState {
    /// Creates the auth runtime over a profile registry and refresh adapter.
    #[must_use]
    pub fn new(
        registry: Arc<AuthProfileRegistry>,
        refresh_adapter: Arc<dyn OAuthRefreshAdapter>,
    ) -> Self {
        Self {
            registry,
            refresh_adapter,
            refresh_metrics: Arc::new(AuthRefreshMetricsState::default()),
            refresh_locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Returns the underlying auth profile registry.
    pub fn registry(&self) -> &AuthProfileRegistry {
        self.registry.as_ref()
    }

    /// Returns the current refresh metrics, sorted per provider.
    pub fn refresh_metrics_snapshot(&self) -> AuthRefreshMetricsSnapshot {
        self.refresh_metrics.snapshot()
    }

    /// Records a refresh outcome into the metrics (no-op when nothing was
    /// attempted).
    pub fn record_refresh_outcome(&self, outcome: &OAuthRefreshOutcome) {
        self.refresh_metrics.record_outcome(outcome);
    }

    // One async mutex per profile id: refreshes for the same profile must not
    // race each other (token rotation in the vault), while different profiles
    // may refresh concurrently.
    fn refresh_lock(&self, profile_id: &str) -> Arc<AsyncMutex<()>> {
        let mut guard = self.refresh_locks.lock().unwrap_or_else(|error| error.into_inner());
        guard.entry(profile_id.to_owned()).or_insert_with(|| Arc::new(AsyncMutex::new(()))).clone()
    }

    /// Refreshes one OAuth profile on a blocking worker, serialized per
    /// profile id, and records the outcome in the refresh metrics.
    ///
    /// # Errors
    /// Returns the mapped auth profile error, or `Status::internal` if the
    /// worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn refresh_oauth_profile(
        self: &Arc<Self>,
        profile_id: String,
        vault: Arc<Vault>,
    ) -> Result<OAuthRefreshOutcome, Status> {
        let refresh_lock = self.refresh_lock(profile_id.as_str());
        let _refresh_guard = refresh_lock.lock().await;
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

    /// Builds the admin auth status (health report plus refresh metrics) on a
    /// blocking worker.
    ///
    /// # Errors
    /// Returns the mapped auth profile error, or `Status::internal` if the
    /// worker panicked.
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

    /// Refreshes all due OAuth profiles (optionally filtered by agent) and
    /// returns the resulting health report, refresh outcomes, and metrics.
    ///
    /// # Errors
    /// Returns the mapped auth profile error, or `Status::internal` if the
    /// worker panicked.
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
    /// Copies every counter into a serializable snapshot (relaxed loads; the
    /// values are not guaranteed to be mutually consistent).
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

/// Extracts the profile id from an `auth-profile:<provider>:<profile>`
/// credential id; `None` for other credential id shapes.
fn auth_profile_id_from_credential_id(credential_id: &str) -> Option<&str> {
    let mut parts = credential_id.splitn(3, ':');
    let prefix = parts.next()?;
    let provider_id = parts.next()?;
    let profile_id = parts.next()?;
    if prefix == "auth-profile" && !provider_id.is_empty() && !profile_id.is_empty() {
        Some(profile_id)
    } else {
        None
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProviderCredentialAttribution {
    provider_id: String,
    credential_id: String,
    auth_profile_id: Option<String>,
}

fn provider_credential_attribution_from_parts(
    provider_id: &str,
    credential_id: &str,
) -> ProviderCredentialAttribution {
    ProviderCredentialAttribution {
        provider_id: provider_id.to_owned(),
        credential_id: credential_id.to_owned(),
        auth_profile_id: auth_profile_id_from_credential_id(credential_id).map(str::to_owned),
    }
}

/// Resolves which credential actually served `provider_id`. The provider that
/// answered may differ from the leased one when registry failover routed the
/// request elsewhere, so the live snapshot is consulted before falling back to
/// the lease context.
fn provider_credential_attribution_for_provider(
    snapshot: &ProviderStatusSnapshot,
    lease_context: &ProviderLeaseExecutionContext,
    provider_id: &str,
) -> Option<ProviderCredentialAttribution> {
    if snapshot.provider_id == provider_id {
        return Some(provider_credential_attribution_from_parts(
            snapshot.provider_id.as_str(),
            snapshot.credential_id.as_str(),
        ));
    }
    if let Some(provider) =
        snapshot.registry.providers.iter().find(|provider| provider.provider_id == provider_id)
    {
        return Some(provider_credential_attribution_from_parts(
            provider.provider_id.as_str(),
            provider.credential_id.as_str(),
        ));
    }
    if let Some(credential) = snapshot
        .registry
        .credentials
        .iter()
        .find(|credential| credential.provider_id == provider_id)
    {
        return Some(provider_credential_attribution_from_parts(
            credential.provider_id.as_str(),
            credential.credential_id.as_str(),
        ));
    }
    (lease_context.provider_id == provider_id).then(|| {
        provider_credential_attribution_from_parts(
            lease_context.provider_id.as_str(),
            lease_context.credential_id.as_str(),
        )
    })
}

fn auth_profile_failure_kind_for_provider_error(
    error: &ProviderError,
) -> Option<AuthProfileFailureKind> {
    if matches!(error, ProviderError::MissingApiKey | ProviderError::MissingAnthropicApiKey) {
        return Some(AuthProfileFailureKind::ConfigMissing);
    }
    let failure = error.failure_snapshot();
    match failure.class.as_str() {
        "auth_expired" => Some(AuthProfileFailureKind::RefreshDue),
        "auth_invalid" | "permission_denied" => Some(AuthProfileFailureKind::AuthInvalid),
        "quota_exceeded" => Some(AuthProfileFailureKind::Quota),
        "rate_limited" => Some(AuthProfileFailureKind::RateLimit),
        "network_unavailable"
        | "provider_timeout"
        | "transient_upstream"
        | "malformed_response" => Some(AuthProfileFailureKind::Transient),
        _ => None,
    }
}

/// Canonical fingerprint of a run start request, used to detect conflicting
/// payloads behind the same idempotency key.
fn orchestrator_run_start_payload_sha256(
    request: &OrchestratorRunStartRequest,
) -> Result<String, Status> {
    let payload = json!({
        "run_id": request.run_id,
        "session_id": request.session_id,
        "origin_kind": request.origin_kind,
        "origin_run_id": request.origin_run_id,
        "triggered_by_principal": request.triggered_by_principal,
        "parameter_delta_json": request.parameter_delta_json,
    });
    let encoded = serde_json::to_vec(&payload).map_err(|error| {
        Status::internal(format!("failed to encode run start payload: {error}"))
    })?;
    let mut hasher = Sha256::new();
    hasher.update(encoded.as_slice());
    Ok(hex::encode(hasher.finalize()))
}

fn stable_error_from_journal(code: &str, error: &JournalError) -> StableErrorEnvelope {
    StableErrorEnvelope::new(
        code,
        error.to_string(),
        "inspect the journal error and retry only after the underlying state is corrected",
    )
}

fn journal_state_error_status(error: JournalError) -> Status {
    match &error {
        JournalError::InvalidArgument(message) => Status::invalid_argument(message.clone()),
        JournalError::WriteBlockedByHashChainMismatch { .. } => {
            Status::failed_precondition(error.to_string())
        }
        JournalError::EmptyPath | JournalError::ParentTraversalPath { .. } => {
            Status::invalid_argument(error.to_string())
        }
        _ => Status::internal(format!("journal state operation failed: {error}")),
    }
}

/// Parses the stored run state into the canonical lifecycle phase, failing
/// closed on states this build does not know.
fn canonical_phase_from_snapshot(
    snapshot: &OrchestratorRunStatusSnapshot,
) -> Result<RunLifecyclePhase, Status> {
    RunLifecyclePhase::parse(snapshot.state.as_str()).ok_or_else(|| {
        Status::failed_precondition(format!(
            "orchestrator run {} has unknown lifecycle state {}",
            snapshot.run_id, snapshot.state
        ))
    })
}

impl GatewayRuntimeState {
    // Cache entries expire when the earliest hit TTL lapses, so a cached
    // result can never include an item the journal already considers expired.
    fn cached_memory_search_expires_at(hits: &[MemorySearchHit]) -> Option<i64> {
        hits.iter().filter_map(|hit| hit.item.ttl_unix_ms).min()
    }

    // Construction. `new` is the test-only convenience constructor with
    // deterministic defaults; production wiring goes through
    // `new_with_provider`.

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
        let dependencies = GatewayRuntimeDependencies { model_provider: default_provider, vault: default_vault, auth_profile_registry: None, agent_registry, tool_posture_registry, retrieval_backend: Arc::new(crate::retrieval::JournalRetrievalBackend), external_retrieval_index: Arc::new(crate::retrieval::ExternalRetrievalRuntime::default()), conversation_bindings: ConversationBindingStore::open_temp() };
        Self::new_with_provider(
            config,
            journal_config,
            journal_store,
            revoked_certificate_count,
            dependencies,
        )
    }

    /// Builds the runtime state, recovering canvas records from journal
    /// snapshots and seeding the journal event counter from the stored total.
    ///
    /// Canvas recovery is verified by replaying the patch chain and comparing
    /// it against the latest snapshot; construction fails closed on any
    /// divergence rather than serving a canvas whose history cannot be
    /// reproduced.
    ///
    /// # Errors
    /// Returns [`JournalError`] when journal reads fail or canvas replay
    /// verification does not match the persisted snapshots.
    pub fn new_with_provider(
        config: GatewayRuntimeConfigSnapshot,
        journal_config: GatewayJournalConfigSnapshot,
        journal_store: JournalStore,
        revoked_certificate_count: usize,
        dependencies: GatewayRuntimeDependencies,
    ) -> Result<Arc<Self>, JournalError> {
        #[rustfmt::skip]
        let GatewayRuntimeDependencies { model_provider, vault, auth_profile_registry, agent_registry, tool_posture_registry, retrieval_backend, external_retrieval_index, conversation_bindings } = dependencies;
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
        let inbound_coalescer =
            InboundCoalescer::new(config.channel_router.inbound_coalescing.clone());
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
            model_provider: RwLock::new(model_provider),
            model_provider_generation: AtomicU64::new(1),
            auth_profile_registry,
            vault,
            memory_config: RwLock::new(MemoryRuntimeConfig::default()),
            retrieval_config: RwLock::new(RetrievalRuntimeConfig::default()),
            learning_config: RwLock::new(LearningRuntimeConfig::default()),
            memory_search_cache: Mutex::new(HashMap::new()),
            http_fetch_cache: Mutex::new(HashMap::new()),
            recent_context_assembly_traces: Mutex::new(Vec::new()),
            tool_approval_cache: Mutex::new(ToolApprovalCacheState::default()),
            run_parameter_delta_cache: Mutex::new(RunParameterDeltaCache::default()),
            run_cleanup_resources: Mutex::new(HashMap::new()),
            run_detached_resources: Mutex::new(HashMap::new()),
            closed_browser_sessions: Mutex::new(ClosedBrowserSessionLedger::default()),
            worker_fleet: RwLock::new(WorkerFleetManager::default()),
            provider_leases: ProviderLeaseManager::default(),
            retrieval_backend,
            external_retrieval_index,
            tool_posture_registry,
            routines_runtime: RwLock::new(None),
            vault_rate_limit: Mutex::new(HashMap::new()),
            orchestrator_run_notify: Arc::new(Notify::new()),
            canvas_records: Mutex::new(recovered_canvas_records),
            canvas_signing_secret: generate_canvas_signing_secret(),
            agent_registry,
            channel_router,
            inbound_coalescer,
            conversation_bindings,
            observability: Arc::new(crate::observability::ObservabilityState::default()),
            self_healing: Arc::new(SelfHealingState::new()),
        }))
    }

    // Counter recorders and run cleanup bookkeeping. The `record_*` methods
    // are fire-and-forget; identifier arguments are trimmed and empty values
    // ignored so malformed callers cannot poison the cleanup maps.

    /// Counts a denied request.
    pub fn record_denied(&self) {
        self.counters.denied_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// Counts an admin status request.
    pub fn record_admin_status_request(&self) {
        self.counters.admin_status_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// Registers a browser session for cleanup when the run terminates,
    /// clearing any stale closed-marker for the same session first.
    pub(crate) fn record_run_browser_session(&self, run_id: &str, session_id: &str) {
        let run_id = run_id.trim();
        let session_id = session_id.trim();
        if run_id.is_empty() || session_id.is_empty() {
            return;
        }

        self.forget_closed_browser_session(session_id);
        match self.run_cleanup_resources.lock() {
            Ok(mut resources_by_run) => {
                let resources = resources_by_run.entry(run_id.to_owned()).or_default();
                if !resources.browser_session_ids.iter().any(|existing| existing == session_id) {
                    resources.browser_session_ids.push(session_id.to_owned());
                }
            }
            Err(error) => {
                warn!(
                    run_id,
                    error = %error,
                    "failed to record browser session for run cleanup"
                );
            }
        }
    }

    /// Marks a browser session as already closed so terminal-run cleanup
    /// skips it.
    pub(crate) fn record_closed_browser_session(&self, session_id: &str) {
        let session_id = session_id.trim();
        if session_id.is_empty() {
            return;
        }

        match self.closed_browser_sessions.lock() {
            Ok(mut ledger) => ledger.insert(session_id.to_owned()),
            Err(error) => {
                warn!(
                    session_id,
                    error = %error,
                    "failed to record closed browser session"
                );
            }
        }
    }

    /// Clears the closed-marker for a browser session (it is live again).
    pub(crate) fn forget_closed_browser_session(&self, session_id: &str) {
        let session_id = session_id.trim();
        if session_id.is_empty() {
            return;
        }

        match self.closed_browser_sessions.lock() {
            Ok(mut ledger) => ledger.remove(session_id),
            Err(error) => {
                warn!(
                    session_id,
                    error = %error,
                    "failed to clear closed browser session marker"
                );
            }
        }
    }

    /// Whether a browser session is marked as already closed.
    pub(crate) fn is_browser_session_closed(&self, session_id: &str) -> bool {
        let session_id = session_id.trim();
        if session_id.is_empty() {
            return false;
        }

        match self.closed_browser_sessions.lock() {
            Ok(ledger) => ledger.contains(session_id),
            Err(error) => {
                warn!(
                    session_id,
                    error = %error,
                    "failed to inspect closed browser session marker"
                );
                false
            }
        }
    }

    fn remember_run_parameter_delta_json(&self, run_id: &str, parameter_delta_json: Option<&str>) {
        let Some(parameter_delta_json) = parameter_delta_json else {
            return;
        };
        match self.run_parameter_delta_cache.lock() {
            Ok(mut cache) => cache.insert(run_id, parameter_delta_json),
            Err(error) => {
                warn!(
                    run_id,
                    error = %error,
                    "failed to lock run launch context cache"
                );
                error.into_inner().insert(run_id, parameter_delta_json);
            }
        }
    }

    /// Returns the in-process start-time parameter delta for a run, when this
    /// daemon instance started or replayed it.
    pub(crate) fn cached_run_parameter_delta_json(&self, run_id: &str) -> Option<String> {
        match self.run_parameter_delta_cache.lock() {
            Ok(cache) => cache.get(run_id),
            Err(error) => {
                warn!(
                    run_id,
                    error = %error,
                    "failed to read run launch context cache"
                );
                error.into_inner().get(run_id)
            }
        }
    }

    /// Unregisters a browser session from the run's cleanup set, dropping the
    /// run entry when nothing remains.
    pub(crate) fn forget_run_browser_session(&self, run_id: &str, session_id: &str) {
        let run_id = run_id.trim();
        let session_id = session_id.trim();
        if run_id.is_empty() || session_id.is_empty() {
            return;
        }

        match self.run_cleanup_resources.lock() {
            Ok(mut resources_by_run) => {
                if let Some(resources) = resources_by_run.get_mut(run_id) {
                    resources.browser_session_ids.retain(|existing| existing != session_id);
                    if resources.is_empty() {
                        resources_by_run.remove(run_id);
                    }
                }
            }
            Err(error) => {
                warn!(
                    run_id,
                    error = %error,
                    "failed to forget browser session for run cleanup"
                );
            }
        }
    }

    /// Registers a background process PID for cleanup when the run
    /// terminates.
    pub(crate) fn record_run_background_process(&self, run_id: &str, pid: u32) {
        let run_id = run_id.trim();
        if run_id.is_empty() || pid == 0 {
            return;
        }

        match self.run_cleanup_resources.lock() {
            Ok(mut resources_by_run) => {
                let resources = resources_by_run.entry(run_id.to_owned()).or_default();
                if !resources.background_process_pids.contains(&pid) {
                    resources.background_process_pids.push(pid);
                }
            }
            Err(error) => {
                warn!(
                    run_id,
                    pid,
                    error = %error,
                    "failed to record background process for run cleanup"
                );
            }
        }
    }

    /// Unregisters a background process PID from the run's cleanup set,
    /// dropping the run entry when nothing remains.
    pub(crate) fn forget_run_background_process(&self, run_id: &str, pid: u32) {
        let run_id = run_id.trim();
        if run_id.is_empty() || pid == 0 {
            return;
        }

        match self.run_cleanup_resources.lock() {
            Ok(mut resources_by_run) => {
                if let Some(resources) = resources_by_run.get_mut(run_id) {
                    resources.background_process_pids.retain(|existing| *existing != pid);
                    if resources.is_empty() {
                        resources_by_run.remove(run_id);
                    }
                }
            }
            Err(error) => {
                warn!(
                    run_id,
                    pid,
                    error = %error,
                    "failed to forget background process for run cleanup"
                );
            }
        }
    }

    /// Lists the background process PIDs currently registered for a run.
    pub(crate) fn list_run_background_processes(&self, run_id: &str) -> Vec<u32> {
        let run_id = run_id.trim();
        if run_id.is_empty() {
            return Vec::new();
        }

        match self.run_cleanup_resources.lock() {
            Ok(resources_by_run) => resources_by_run
                .get(run_id)
                .map(|resources| resources.background_process_pids.clone())
                .unwrap_or_default(),
            Err(error) => {
                warn!(
                    run_id,
                    error = %error,
                    "failed to list background processes for run cleanup"
                );
                Vec::new()
            }
        }
    }

    /// Registers a detached background process for terminal handoff reporting
    /// without adding it to terminal cleanup.
    pub(crate) fn record_run_detached_background_process(
        &self,
        run_id: &str,
        resource: DetachedBackgroundProcessResource,
    ) {
        let run_id = run_id.trim();
        if run_id.is_empty() || resource.pid == 0 {
            return;
        }

        match self.run_detached_resources.lock() {
            Ok(mut resources_by_run) => {
                let resources = resources_by_run.entry(run_id.to_owned()).or_default();
                if let Some(existing) = resources
                    .background_processes
                    .iter_mut()
                    .find(|existing| existing.pid == resource.pid)
                {
                    *existing = resource;
                } else {
                    resources.background_processes.push(resource);
                }
            }
            Err(error) => {
                warn!(
                    run_id,
                    pid = resource.pid,
                    error = %error,
                    "failed to record detached background process for run handoff"
                );
            }
        }
    }

    /// Removes and returns detached resources for a terminal-run handoff
    /// summary. The resources are not stopped here.
    pub(crate) fn take_run_detached_resources(&self, run_id: &str) -> RunDetachedResources {
        let run_id = run_id.trim();
        if run_id.is_empty() {
            return RunDetachedResources::default();
        }

        match self.run_detached_resources.lock() {
            Ok(mut resources_by_run) => resources_by_run.remove(run_id).unwrap_or_default(),
            Err(error) => {
                warn!(
                    run_id,
                    error = %error,
                    "failed to take detached background resources"
                );
                RunDetachedResources::default()
            }
        }
    }

    /// Removes and returns everything still registered for the run; cleanup
    /// consumes the entry so a second cleanup pass finds nothing to do.
    pub(crate) fn take_run_cleanup_resources(&self, run_id: &str) -> RunCleanupResources {
        let run_id = run_id.trim();
        if run_id.is_empty() {
            return RunCleanupResources::default();
        }

        match self.run_cleanup_resources.lock() {
            Ok(mut resources_by_run) => resources_by_run.remove(run_id).unwrap_or_default(),
            Err(error) => {
                warn!(
                    run_id,
                    error = %error,
                    "failed to take run cleanup resources"
                );
                RunCleanupResources::default()
            }
        }
    }

    // Self-healing: thin delegates to `SelfHealingState`; the names mirror
    // the methods they forward to.

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

    /// Prepends a context assembly trace, keeping only the most recent few
    /// for the diagnostics endpoint.
    pub(crate) fn record_context_assembly_trace(&self, trace: Value) {
        const MAX_RECENT_CONTEXT_ASSEMBLY_TRACES: usize = 16;

        let mut traces = match self.recent_context_assembly_traces.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!("context assembly trace buffer lock poisoned while recording trace");
                poisoned.into_inner()
            }
        };
        traces.insert(0, trace);
        traces.truncate(MAX_RECENT_CONTEXT_ASSEMBLY_TRACES);
    }

    /// Copies the retained context assembly traces, newest first.
    #[must_use]
    pub(crate) fn context_assembly_traces_snapshot(&self) -> Vec<Value> {
        match self.recent_context_assembly_traces.lock() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => {
                warn!("context assembly trace buffer lock poisoned while reading trace snapshot");
                poisoned.into_inner().clone()
            }
        }
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

    /// Counts an allow/deny tool decision. Denials also count as denied
    /// requests, and process-runner denials additionally count as sandbox
    /// policy denies.
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

    // Canvas host. In-memory records live behind `canvas_records`; every
    // state transition is journaled while that lock is held so the persisted
    // patch sequence can never diverge from the in-memory version counter.
    // Public HTTP access (frame, runtime assets, state polling) is authorized
    // exclusively through signed canvas tokens.

    #[allow(clippy::result_large_err)]
    fn ensure_canvas_host_enabled(&self) -> Result<(), Status> {
        if self.config.canvas_host.enabled {
            Ok(())
        } else {
            Err(Status::failed_precondition("canvas host is disabled (canvas_host.enabled=false)"))
        }
    }

    /// Creates a canvas: validates state/bundle limits, signs the bundle,
    /// journals the initial transition, and issues the first runtime
    /// descriptor with a signed auth token.
    ///
    /// # Errors
    /// `failed_precondition` when the canvas host is disabled,
    /// `invalid_argument`/`resource_exhausted` for malformed or oversized
    /// payloads, and `already_exists` for duplicate canvas ids.
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

        // The registry lock stays held through the journal write below so the
        // duplicate-id check and the persisted transition are atomic with the
        // in-memory insert.
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

    /// Applies a full-state replacement or JSON patch to a canvas, enforcing
    /// version/schema preconditions and the per-minute update rate limit, and
    /// journals the resulting transition.
    ///
    /// # Errors
    /// `invalid_argument` for malformed payloads, `failed_precondition` for
    /// version/schema mismatches or closed canvases, `permission_denied` for
    /// principal mismatch or expiry, and `resource_exhausted` for size or
    /// rate limits.
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

        // Sliding-window rate limit: drop timestamps older than one minute,
        // then reject the update if the window is already at capacity.
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

    /// Closes a canvas (idempotent), journaling one final transition with the
    /// close reason.
    ///
    /// # Errors
    /// `not_found` for unknown ids, `permission_denied` for principal
    /// mismatch, and journal mapping errors from persisting the transition.
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

    /// Loads a canvas record, enforcing principal ownership.
    ///
    /// # Errors
    /// `not_found` for unknown ids, `permission_denied` for principal
    /// mismatch.
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

    /// Lists journal-backed state patches after a version (limit clamped to
    /// the streaming batch maximum).
    ///
    /// # Errors
    /// Ownership errors from [`Self::get_canvas`] plus mapped journal errors.
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

    /// Lists the principal's canvases for one session, newest update first.
    ///
    /// # Errors
    /// `invalid_argument` for non-canonical session ids; `failed_precondition`
    /// when the canvas host is disabled.
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

    /// Issues a fresh runtime descriptor (and token) for an existing canvas;
    /// the token expiry never outlives the canvas session itself.
    ///
    /// # Errors
    /// Ownership errors from [`Self::get_canvas`]; `failed_precondition` when
    /// the canvas session already expired.
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

    /// Restores a canvas to an earlier persisted state version by replaying
    /// that version's resulting state as a new update (history only ever
    /// moves forward; no versions are rewritten).
    ///
    /// # Errors
    /// `invalid_argument` for version 0, `not_found` for unknown versions,
    /// `failed_precondition` for closed canvases or closed target revisions.
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
            .journal_store
            .get_canvas_state_patch(record.canvas_id.as_str(), target_state_version)
            .map_err(|error| map_canvas_store_error("get_canvas_state_patch", error))?
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

    /// Renders the HTML shell for the canvas iframe; access is authorized by
    /// the signed token in the URL, not by request principal.
    ///
    /// # Errors
    /// Token/ownership errors from `Self::authorize_canvas_http_request`.
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

    /// Serves the generated canvas runtime script (state polling plus
    /// postMessage bridge restricted to the allowed parent origins).
    ///
    /// # Errors
    /// Token/ownership errors from `Self::authorize_canvas_http_request`.
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

    /// Serves the static canvas stylesheet.
    ///
    /// # Errors
    /// Token/ownership errors from `Self::authorize_canvas_http_request`.
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

    /// Serves one asset from the canvas bundle by normalized path.
    ///
    /// # Errors
    /// Token/ownership errors from `Self::authorize_canvas_http_request`;
    /// `not_found` for unknown asset paths.
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

    /// Returns the current canvas state, or `None` when `after_version` is
    /// already current (the HTTP layer turns that into 204 No Content).
    ///
    /// # Errors
    /// Token/ownership errors from `Self::authorize_canvas_http_request`.
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

    /// Loads recent patch history for a canvas, trimmed to the response row
    /// and byte budgets (newest records win).
    ///
    /// # Errors
    /// Mapped journal errors from the patch history read.
    #[allow(clippy::result_large_err)]
    pub(crate) fn load_canvas_patch_history(
        &self,
        canvas_id: &str,
    ) -> Result<Vec<CanvasStatePatchRecord>, Status> {
        let history = self
            .journal_store
            .list_recent_canvas_state_patches(canvas_id, CANVAS_PATCH_HISTORY_RESPONSE_ROW_LIMIT)
            .map_err(|error| map_canvas_store_error("list_recent_canvas_state_patches", error))?;
        Ok(Self::limit_canvas_patch_history_response(
            history,
            CANVAS_PATCH_HISTORY_RESPONSE_BYTE_LIMIT,
        ))
    }

    // Walks history newest-first so the byte budget keeps the most recent
    // contiguous records; oversized records at the newest edge are skipped
    // until something fits (pinned by a unit test below).
    fn limit_canvas_patch_history_response(
        history: Vec<CanvasStatePatchRecord>,
        max_response_bytes: usize,
    ) -> Vec<CanvasStatePatchRecord> {
        let mut selected = Vec::new();
        let mut selected_bytes = 0_usize;
        for record in history.into_iter().rev() {
            let record_bytes = Self::canvas_patch_history_response_bytes(&record);
            if record_bytes > max_response_bytes {
                if selected.is_empty() {
                    continue;
                }
                break;
            }
            if selected_bytes.saturating_add(record_bytes) > max_response_bytes {
                break;
            }
            selected_bytes = selected_bytes.saturating_add(record_bytes);
            selected.push(record);
        }
        selected.reverse();
        selected
    }

    fn canvas_patch_history_response_bytes(record: &CanvasStatePatchRecord) -> usize {
        CANVAS_PATCH_HISTORY_RESPONSE_RECORD_OVERHEAD
            .saturating_add(record.canvas_id.len())
            .saturating_add(record.patch_json.len())
            .saturating_add(record.resulting_state_json.len())
            .saturating_add(record.close_reason.as_deref().map_or(0, str::len))
            .saturating_add(record.actor_principal.len())
            .saturating_add(record.actor_device_id.len())
    }

    /// Verifies a canvas token and checks that its scope (canvas, principal,
    /// session) matches the live record and that neither token nor canvas has
    /// expired. Every rejection increments the `canvas_denied` counter.
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
        sign_canvas_hmac_sha256(
            &self.canvas_signing_secret,
            "canvas_bundle.v1",
            &[
                canvas_id.as_bytes(),
                bundle_sha256.as_bytes(),
                principal.as_bytes(),
                session_id.as_bytes(),
            ],
        )
    }

    /// Issues a `payload_b64.signature` token scoped to canvas, principal,
    /// and session. The signing secret is generated per process, so tokens do
    /// not survive a daemon restart.
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
        let signature = sign_canvas_hmac_sha256(
            &self.canvas_signing_secret,
            "canvas_token.v1",
            &[payload_b64.as_bytes()],
        );
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
        let expected_signature = sign_canvas_hmac_sha256(
            &self.canvas_signing_secret,
            "canvas_token.v1",
            &[payload_b64.as_bytes()],
        );
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

    // Journal event recording and vault access (rate limiting, counters, and
    // spawn_blocking wrappers around the synchronous vault backends).

    /// Appends a journal event, updating event/redaction counters and warning
    /// when the write exceeds the latency budget.
    ///
    /// # Errors
    /// `already_exists` for duplicate event ids, `resource_exhausted` when the
    /// journal capacity is reached, `internal` for other persistence failures
    /// (the persist-failure counter is bumped for the latter two).
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

    /// Async wrapper for [`Self::record_journal_event_blocking`] on a
    /// blocking worker.
    ///
    /// # Errors
    /// Same as the blocking variant, plus `internal` if the worker panicked.
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

    /// Consumes one slot from the principal's fixed-window vault rate limit;
    /// returns `false` when the budget is exhausted.
    ///
    /// Fails closed: a poisoned lock denies the request. The bucket map is
    /// bounded; when full, stale windows are pruned and, if still full, the
    /// principal with the oldest window is evicted so an attacker cannot grow
    /// the map without bound by rotating principals.
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

    /// Stores a secret in the vault scope on a blocking worker.
    ///
    /// # Errors
    /// Returns the mapped vault error, or `internal` if the worker panicked.
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

    /// Reads a secret from the vault scope on a blocking worker.
    ///
    /// # Errors
    /// Returns the mapped vault error, or `internal` if the worker panicked.
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

    /// Deletes a secret from the vault scope on a blocking worker; returns
    /// whether it existed.
    ///
    /// # Errors
    /// Returns the mapped vault error, or `internal` if the worker panicked.
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

    /// Lists secret metadata in the vault scope on a blocking worker (values
    /// are never returned by this call).
    ///
    /// # Errors
    /// Returns the mapped vault error, or `internal` if the worker panicked.
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

    // Status snapshots and model provider execution.

    /// Assembles the full gateway status document. Performs synchronous
    /// journal reads; call [`Self::status_snapshot_async`] from async code.
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
            model_provider: self.current_model_provider().status_snapshot(),
            tool_call_policy: tool_policy_snapshot(&self.config.tool_call),
            counters: self.counters.snapshot(),
            agents: agents_runtime,
            request_context: context,
        }
    }

    /// Builds [`Self::status_snapshot`] on a blocking worker.
    ///
    /// # Errors
    /// `internal` if the worker panicked.
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

    /// Loads the most recent journal events plus totals (limit clamped to
    /// `MAX_JOURNAL_RECENT_EVENTS`).
    ///
    /// # Errors
    /// `internal` when journal reads fail.
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

    /// Async wrapper for `Self::recent_journal_snapshot_blocking`.
    ///
    /// # Errors
    /// Same as the blocking variant, plus `internal` if the worker panicked.
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

    /// Builds the journal state doctor report on the blocking journal worker.
    ///
    /// # Errors
    /// `internal` when state probes fail, `failed_precondition` when writes are
    /// already blocked by a known hash-chain mismatch.
    #[allow(clippy::result_large_err)]
    pub(crate) fn journal_state_health_report_blocking(
        &self,
        fast_window: Option<usize>,
    ) -> Result<JournalHealthReport, Status> {
        self.journal_store.state_health_report(fast_window).map_err(journal_state_error_status)
    }

    /// Async wrapper for [`Self::journal_state_health_report_blocking`].
    ///
    /// # Errors
    /// Same as the blocking variant, plus `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn journal_state_health_report(
        self: &Arc<Self>,
        fast_window: Option<usize>,
    ) -> Result<JournalHealthReport, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.journal_state_health_report_blocking(fast_window))
            .await
            .map_err(|_| Status::internal("journal state doctor worker panicked"))?
    }

    /// Applies or previews journal state repair on the blocking journal worker.
    ///
    /// # Errors
    /// `invalid_argument` when the requested repair is outside the supported
    /// Phase 1 FTS-only contract, or `internal` when the repair fails.
    #[allow(clippy::result_large_err)]
    pub(crate) fn repair_journal_state_blocking(
        &self,
        request: &JournalStateRepairRequest,
    ) -> Result<JournalStateRepairReport, Status> {
        self.journal_store.repair_state(request).map_err(journal_state_error_status)
    }

    /// Async wrapper for [`Self::repair_journal_state_blocking`].
    ///
    /// # Errors
    /// Same as the blocking variant, plus `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn repair_journal_state(
        self: &Arc<Self>,
        request: JournalStateRepairRequest,
    ) -> Result<JournalStateRepairReport, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.repair_journal_state_blocking(&request))
            .await
            .map_err(|_| Status::internal("journal state repair worker panicked"))?
    }

    /// Runs a WAL checkpoint on the blocking journal worker.
    ///
    /// # Errors
    /// `internal` when SQLite rejects the checkpoint.
    #[allow(clippy::result_large_err)]
    pub(crate) fn checkpoint_journal_wal_blocking(
        &self,
        mode: JournalWalCheckpointMode,
    ) -> Result<JournalWalCheckpointReport, Status> {
        self.journal_store.checkpoint_wal(mode).map_err(journal_state_error_status)
    }

    /// Async wrapper for [`Self::checkpoint_journal_wal_blocking`].
    ///
    /// # Errors
    /// Same as the blocking variant, plus `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn checkpoint_journal_wal(
        self: &Arc<Self>,
        mode: JournalWalCheckpointMode,
    ) -> Result<JournalWalCheckpointReport, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.checkpoint_journal_wal_blocking(mode))
            .await
            .map_err(|_| Status::internal("journal WAL checkpoint worker panicked"))?
    }

    /// Verifies the journal hash chain on the blocking journal worker.
    ///
    /// # Errors
    /// `internal` when SQLite verification fails.
    #[allow(clippy::result_large_err)]
    pub(crate) fn verify_journal_hash_chain_blocking(
        &self,
        scope: JournalHashVerificationScope,
    ) -> Result<JournalHashChainVerificationReport, Status> {
        self.journal_store.verify_hash_chain(scope).map_err(journal_state_error_status)
    }

    /// Async wrapper for [`Self::verify_journal_hash_chain_blocking`].
    ///
    /// # Errors
    /// Same as the blocking variant, plus `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn verify_journal_hash_chain(
        self: &Arc<Self>,
        scope: JournalHashVerificationScope,
    ) -> Result<JournalHashChainVerificationReport, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.verify_journal_hash_chain_blocking(scope))
            .await
            .map_err(|_| Status::internal("journal hash-chain verifier worker panicked"))?
    }

    /// Creates rebuildable journal sidecar directories on the blocking worker.
    ///
    /// # Errors
    /// `internal` when directory creation or permission hardening fails.
    #[allow(clippy::result_large_err)]
    pub(crate) fn prepare_journal_sidecar_storage_blocking(
        &self,
    ) -> Result<Vec<SidecarIndexDescriptor>, Status> {
        self.journal_store.prepare_sidecar_storage().map_err(journal_state_error_status)
    }

    /// Async wrapper for [`Self::prepare_journal_sidecar_storage_blocking`].
    ///
    /// # Errors
    /// Same as the blocking variant, plus `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn prepare_journal_sidecar_storage(
        self: &Arc<Self>,
    ) -> Result<Vec<SidecarIndexDescriptor>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.prepare_journal_sidecar_storage_blocking())
            .await
            .map_err(|_| Status::internal("journal sidecar prepare worker panicked"))?
    }

    /// Whether the v1 orchestrator run loop is enabled by configuration.
    #[must_use]
    pub const fn is_orchestrator_runloop_enabled(&self) -> bool {
        self.config.orchestrator_runloop_v1_enabled
    }

    fn current_model_provider(&self) -> Arc<dyn ModelProvider> {
        Arc::clone(&self.model_provider.read().unwrap_or_else(|error| error.into_inner()))
    }

    /// Replaces the model provider used by new requests and returns the new generation.
    #[must_use]
    pub fn configure_model_provider(&self, model_provider: Arc<dyn ModelProvider>) -> u64 {
        let mut guard = self.model_provider.write().unwrap_or_else(|error| error.into_inner());
        *guard = model_provider;
        self.model_provider_generation.fetch_add(1, Ordering::Relaxed).saturating_add(1)
    }

    /// Monotonic generation of the live model-provider runtime.
    #[must_use]
    #[cfg(test)]
    pub fn model_provider_generation(&self) -> u64 {
        self.model_provider_generation.load(Ordering::Relaxed)
    }

    /// Current model provider/registry status.
    #[must_use]
    pub fn model_provider_status_snapshot(&self) -> ProviderStatusSnapshot {
        self.current_model_provider().status_snapshot()
    }

    /// Current provider lease manager state (active leases and waiters).
    #[must_use]
    pub fn provider_lease_snapshot(&self) -> ProviderLeaseManagerSnapshot {
        self.provider_leases.snapshot()
    }

    /// Feeds a credential health signal (success, rate limit, quota, auth,
    /// transient) into the lease manager's cooldown logic.
    pub fn record_provider_credential_feedback(&self, request: ProviderCredentialFeedbackRequest) {
        self.provider_leases.record_credential_feedback(request);
    }

    /// Snapshot of the retrieval backend including memory embeddings status.
    ///
    /// # Errors
    /// Returns the mapped memory store error when the embeddings status read
    /// fails.
    #[allow(clippy::result_large_err)]
    pub fn retrieval_backend_snapshot(&self) -> Result<RetrievalBackendSnapshot, Status> {
        let embeddings_status = self
            .journal_store
            .memory_embeddings_status()
            .map_err(|error| map_memory_store_error("load retrieval backend snapshot", error))?;
        Ok(self.retrieval_backend.snapshot(&self.retrieval_config_snapshot(), &embeddings_status))
    }

    /// Previews lease admission for a provider/credential without acquiring
    /// or queueing anything. `task_label` is accepted for signature parity
    /// with acquisition but does not influence the preview.
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

    /// Test-only completion path that skips lease admission while still
    /// updating provider counters.
    ///
    /// # Errors
    /// Returns the mapped provider error.
    #[cfg(test)]
    #[allow(clippy::result_large_err)]
    pub async fn execute_model_provider(
        self: &Arc<Self>,
        request: ProviderRequest,
    ) -> Result<crate::model_provider::ProviderResponse, Status> {
        self.counters.model_provider_requests.fetch_add(1, Ordering::Relaxed);
        let model_provider = self.current_model_provider();
        match model_provider.complete(request).await {
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

    /// Completes a model request under provider lease admission.
    ///
    /// Waits for (or is refused) a lease first, then runs the request and
    /// feeds credential-health and auth-profile signals back from the
    /// outcome. The lease guard is held for the whole provider call so
    /// concurrency limits hold.
    ///
    /// # Errors
    /// `resource_exhausted` when capacity is busy or the lease wait times
    /// out, otherwise the mapped provider error.
    #[allow(clippy::result_large_err)]
    pub async fn execute_model_provider_with_lease(
        self: &Arc<Self>,
        request: ProviderRequest,
        lease_context: ProviderLeaseExecutionContext,
    ) -> Result<crate::model_provider::ProviderResponse, Status> {
        let model_provider = self.current_model_provider();
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
                    provider_lease_deferred_status(&lease_context, preview)
                }
                ProviderLeaseAcquireError::TimedOut { waited_ms, preview } => {
                    provider_lease_timeout_status(waited_ms, &lease_context, preview)
                }
            })?;
        self.counters.model_provider_requests.fetch_add(1, Ordering::Relaxed);
        match model_provider.complete(request).await {
            Ok(response) => {
                let provider_status = model_provider.status_snapshot();
                if let Some(attribution) = provider_credential_attribution_for_provider(
                    &provider_status,
                    &lease_context,
                    response.provider_id.as_str(),
                ) {
                    self.record_provider_credential_feedback(ProviderCredentialFeedbackRequest {
                        provider_id: attribution.provider_id.clone(),
                        credential_id: attribution.credential_id.clone(),
                        kind: ProviderCredentialFeedbackKind::Success,
                        retry_after_ms: None,
                        reason: "provider call succeeded".to_owned(),
                        observed_at_unix_ms: current_unix_ms(),
                    });
                    self.record_auth_profile_success_for_attribution(&attribution);
                }
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
                self.record_auth_profile_failure_for_lease(&lease_context, &error);
                self.record_provider_lease_feedback_for_error(&lease_context, &error);
                Err(map_provider_error(error))
            }
        }
    }

    fn record_auth_profile_success_for_attribution(
        &self,
        attribution: &ProviderCredentialAttribution,
    ) {
        let Some(profile_id) = attribution.auth_profile_id.as_deref() else {
            return;
        };
        let Some(registry) = self.auth_profile_registry.as_ref() else {
            return;
        };
        if let Err(error) = registry.record_profile_success(profile_id) {
            warn!(
                profile_id,
                provider_id = attribution.provider_id.as_str(),
                error = %error,
                "failed to record auth profile provider success"
            );
        }
    }

    fn record_auth_profile_failure_for_lease(
        &self,
        lease_context: &ProviderLeaseExecutionContext,
        error: &ProviderError,
    ) {
        let Some(profile_id) =
            auth_profile_id_from_credential_id(lease_context.credential_id.as_str())
        else {
            return;
        };
        let Some(kind) = auth_profile_failure_kind_for_provider_error(error) else {
            return;
        };
        let Some(registry) = self.auth_profile_registry.as_ref() else {
            return;
        };
        if let Err(record_error) = registry.record_profile_failure(profile_id, kind) {
            warn!(
                profile_id,
                provider_id = lease_context.provider_id.as_str(),
                failure_kind = kind.as_str(),
                error = %record_error,
                "failed to record auth profile provider failure"
            );
        }
    }

    fn record_provider_lease_feedback_for_error(
        &self,
        lease_context: &ProviderLeaseExecutionContext,
        error: &ProviderError,
    ) {
        let failure = error.failure_snapshot();
        let kind = match failure.recovery.category.as_str() {
            "rate_limit" => ProviderCredentialFeedbackKind::RateLimited,
            "quota" => ProviderCredentialFeedbackKind::QuotaExhausted,
            "auth" => ProviderCredentialFeedbackKind::AuthFailed,
            "transient" => ProviderCredentialFeedbackKind::TransientFailure,
            _ => return,
        };
        self.record_provider_credential_feedback(ProviderCredentialFeedbackRequest {
            provider_id: lease_context.provider_id.clone(),
            credential_id: lease_context.credential_id.clone(),
            kind,
            retry_after_ms: failure.recovery.retry_after_ms,
            reason: format!(
                "class={} category={} action={}",
                failure.class, failure.recovery.category, failure.recovery.action
            ),
            observed_at_unix_ms: current_unix_ms(),
        });
    }

    /// Transcribes audio through the model provider, updating provider
    /// counters (no lease admission; transcription is not lease-governed).
    ///
    /// # Errors
    /// Returns the mapped provider error.
    #[allow(clippy::result_large_err)]
    pub async fn execute_audio_transcription(
        self: &Arc<Self>,
        request: AudioTranscriptionRequest,
    ) -> Result<AudioTranscriptionResponse, Status> {
        self.counters.model_provider_requests.fetch_add(1, Ordering::Relaxed);
        let model_provider = self.current_model_provider();
        match model_provider.transcribe_audio(request).await {
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

    // Orchestrator sessions, usage accounting, and agent registry access.
    // From here to the end of the impl, most methods follow the
    // blocking/async delegate pattern described in the module header; the
    // async wrapper is the documented public surface.

    #[allow(clippy::result_large_err)]
    fn resolve_orchestrator_session_blocking(
        &self,
        request: &OrchestratorSessionResolveRequest,
    ) -> Result<OrchestratorSessionResolveOutcome, Status> {
        self.journal_store
            .resolve_orchestrator_session(request)
            .map_err(|error| map_orchestrator_store_error("resolve orchestrator session", error))
    }

    /// Resolves (or creates) the orchestrator session for a conversation context.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Updates a session title.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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
    fn orchestrator_session_by_id_snapshot_blocking(
        &self,
        session_id: &str,
    ) -> Result<Option<OrchestratorSessionRecord>, Status> {
        self.journal_store.orchestrator_session_by_id_snapshot(session_id).map_err(|error| {
            map_orchestrator_store_error("load orchestrator session snapshot by id", error)
        })
    }

    /// Loads a session by id.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Loads a read-only snapshot of a session by id.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn orchestrator_session_by_id_snapshot(
        self: &Arc<Self>,
        session_id: String,
    ) -> Result<Option<OrchestratorSessionRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.orchestrator_session_by_id_snapshot_blocking(&session_id)
        })
        .await
        .map_err(|_| Status::internal("orchestrator session snapshot worker panicked"))?
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

    /// Loads the per-session project context state, if any.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Creates or updates the per-session project context state.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Copies project context state between sessions; `None` when the source has no state.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Updates a session's quick-control settings.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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
        // Search cannot be pushed into the store query, so matching pages are
        // scanned in store order until limit+1 hits are collected; the extra
        // hit only signals has_more and is truncated below.
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

    /// Lists sessions for a principal/device with cursor pagination and optional case-insensitive
    /// search over title/preview/intent/summary/run-state.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    // Same pagination/search shape as list_orchestrator_sessions_blocking,
    // but scoped to the principal across devices.
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

    /// Like [`Self::list_orchestrator_sessions`] but scoped to the principal across all devices.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Aggregates orchestrator usage for the query window.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Lists per-session usage rows for the query window.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Loads one session's usage plus up to `run_limit` of its run records.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Archives/cleans a session per the cleanup request.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Lists agents with cursor pagination.
    ///
    /// # Errors
    /// Returns the mapped agent registry error, or `internal` if the worker panicked.
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

    /// Loads an agent record; the bool flags whether it is the default agent.
    ///
    /// # Errors
    /// Returns the mapped agent registry error, or `internal` if the worker panicked.
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

    /// Creates an agent, defaulting its model profile from the provider registry when unset;
    /// updates mutation/validation counters.
    ///
    /// # Errors
    /// Returns the mapped agent registry error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn create_agent(
        self: &Arc<Self>,
        request: AgentCreateRequest,
    ) -> Result<AgentCreateOutcome, Status> {
        let request = self.create_agent_request_with_runtime_defaults(request);
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

    fn create_agent_request_with_runtime_defaults(
        &self,
        mut request: AgentCreateRequest,
    ) -> AgentCreateRequest {
        if request
            .default_model_profile
            .as_deref()
            .and_then(normalize_optional_agent_model_profile)
            .is_none()
        {
            request.default_model_profile = self.default_agent_model_profile();
        }
        request
    }

    fn default_agent_model_profile(&self) -> Option<String> {
        let snapshot = self.current_model_provider().status_snapshot();
        select_default_agent_model_profile(
            snapshot.registry.default_chat_model_id.as_deref(),
            snapshot.model_id.as_deref(),
            snapshot.openai_model.as_deref(),
            snapshot.anthropic_model.as_deref(),
        )
    }

    #[allow(clippy::result_large_err)]
    fn delete_agent_blocking(&self, agent_id: &str) -> Result<AgentDeleteOutcome, Status> {
        self.agent_registry
            .delete_agent(agent_id)
            .map_err(|error| map_agent_registry_error("delete agent", error))
    }

    /// Deletes an agent; updates mutation/validation counters.
    ///
    /// # Errors
    /// Returns the mapped agent registry error, or `internal` if the worker panicked.
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

    /// Marks an agent as the default; updates mutation/validation counters.
    ///
    /// # Errors
    /// Returns the mapped agent registry error, or `internal` if the worker panicked.
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

    /// Lists session-agent bindings matching the query.
    ///
    /// # Errors
    /// Returns the mapped agent registry error, or `internal` if the worker panicked.
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

    /// Binds an agent to a conversation context; updates mutation/validation counters.
    ///
    /// # Errors
    /// Returns the mapped agent registry error, or `internal` if the worker panicked.
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

    /// Removes an agent binding; counts a mutation only when something was actually removed.
    ///
    /// # Errors
    /// Returns the mapped agent registry error, or `internal` if the worker panicked.
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

    /// Resolves the agent for a context, recording binding hit/miss counters and any binding
    /// created on the way.
    ///
    /// # Errors
    /// Returns the mapped agent registry error, or `internal` if the worker panicked.
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

    // Run lifecycle: idempotent start, metadata/state updates, tape, cancel
    // flags, startup recovery, and run-completion waiting.

    // Returns Ok(true) only when this call actually inserted the run, so the
    // async wrapper counts each run exactly once across idempotent retries.
    #[allow(clippy::result_large_err)]
    fn start_orchestrator_run_blocking(
        &self,
        request: &OrchestratorRunStartRequest,
    ) -> Result<bool, Status> {
        let idempotency_key = format!("run:start:{}", request.run_id);
        let payload_sha256 = orchestrator_run_start_payload_sha256(request)?;
        let begin = self
            .journal_store
            .begin_idempotency_operation(&IdempotencyBeginRequest {
                key: idempotency_key.clone(),
                scope: "orchestrator_run".to_owned(),
                operation_kind: "start_orchestrator_run".to_owned(),
                payload_sha256,
                expires_at_unix_ms: Some(
                    current_unix_ms().saturating_add(24_i64 * 60 * 60 * 1_000),
                ),
            })
            .map_err(|error| map_orchestrator_store_error("begin idempotent run start", error))?;

        match begin.decision {
            IdempotencyReplayDecision::CompletedReplayResult => return Ok(false),
            IdempotencyReplayDecision::ConflictingPayload => {
                return Err(Status::already_exists(format!(
                    "conflicting idempotent start request for run {}",
                    request.run_id
                )));
            }
            IdempotencyReplayDecision::SamePayloadRetry
            | IdempotencyReplayDecision::Reserved
            | IdempotencyReplayDecision::ExpiredRetry => {}
        }

        match self.journal_store.start_orchestrator_run(request) {
            Ok(()) => {
                self.complete_run_start_idempotency(idempotency_key.as_str(), request)?;
                Ok(true)
            }
            // A duplicate run id on a retry/reserved decision means an
            // earlier attempt inserted the run but crashed before completing
            // the idempotency record: finish that record and report replay.
            Err(JournalError::DuplicateRunId { .. })
                if matches!(
                    begin.decision,
                    IdempotencyReplayDecision::SamePayloadRetry
                        | IdempotencyReplayDecision::Reserved
                        | IdempotencyReplayDecision::ExpiredRetry
                ) =>
            {
                self.complete_run_start_idempotency(idempotency_key.as_str(), request)?;
                Ok(false)
            }
            Err(error) => {
                let _ = self.journal_store.fail_idempotency_operation(&IdempotencyFailRequest {
                    key: idempotency_key,
                    error: stable_error_from_journal("run_start_failed", &error),
                });
                Err(map_orchestrator_store_error("start orchestrator run", error))
            }
        }
    }

    /// Starts a run idempotently: replays of the same start payload are
    /// accepted without effect, conflicting payloads under the same run id
    /// are rejected. Wakes run waiters either way.
    ///
    /// # Errors
    /// `already_exists` for conflicting idempotent payloads, otherwise the
    /// mapped journal error or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn start_orchestrator_run(
        self: &Arc<Self>,
        request: OrchestratorRunStartRequest,
    ) -> Result<(), Status> {
        let run_id = request.run_id.clone();
        let parameter_delta_json = request.parameter_delta_json.clone();
        let state = Arc::clone(self);
        let inserted =
            tokio::task::spawn_blocking(move || state.start_orchestrator_run_blocking(&request))
                .await
                .map_err(|_| Status::internal("orchestrator run worker panicked"))??;
        self.remember_run_parameter_delta_json(run_id.as_str(), parameter_delta_json.as_deref());
        if inserted {
            self.counters.orchestrator_runs_started.fetch_add(1, Ordering::Relaxed);
        }
        self.orchestrator_run_notify.notify_waiters();
        Ok(())
    }

    fn complete_run_start_idempotency(
        &self,
        idempotency_key: &str,
        request: &OrchestratorRunStartRequest,
    ) -> Result<(), Status> {
        let result_json = json!({
            "run_id": request.run_id,
            "session_id": request.session_id,
            "state": RunLifecyclePhase::Queued.as_str(),
        })
        .to_string();
        self.journal_store
            .complete_idempotency_operation(&IdempotencyCompleteRequest {
                key: idempotency_key.to_owned(),
                result_json,
            })
            .map(|_| ())
            .map_err(|error| map_orchestrator_store_error("complete idempotent run start", error))
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

    /// Updates run metadata (intent, summary, parameters).
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Transitions a run's lifecycle state, bumping completed/cancelled
    /// counters on terminal states, and wakes run waiters.
    ///
    /// # Errors
    /// Returns the mapped journal error, or `internal` if the worker
    /// panicked.
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
        self.orchestrator_run_notify.notify_waiters();
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    fn terminalize_orphaned_orchestrator_runs_on_startup_blocking(
        &self,
        reason: &str,
    ) -> Result<OrchestratorStartupRunRecoveryReport, Status> {
        self.journal_store.terminalize_orphaned_orchestrator_runs_on_startup(reason).map_err(
            |error| map_orchestrator_store_error("terminalize orphaned orchestrator runs", error),
        )
    }

    /// Marks runs left non-terminal by a previous daemon process as failed
    /// with `reason`; wakes run waiters when anything changed.
    ///
    /// # Errors
    /// Returns the mapped journal error, or `internal` if the worker
    /// panicked.
    #[allow(clippy::result_large_err)]
    pub async fn terminalize_orphaned_orchestrator_runs_on_startup(
        self: &Arc<Self>,
        reason: impl Into<String>,
    ) -> Result<OrchestratorStartupRunRecoveryReport, Status> {
        let state = Arc::clone(self);
        let reason = reason.into();
        let report = tokio::task::spawn_blocking(move || {
            state.terminalize_orphaned_orchestrator_runs_on_startup_blocking(reason.as_str())
        })
        .await
        .map_err(|_| Status::internal("orchestrator startup recovery worker panicked"))??;
        if report.terminalized_count > 0 {
            self.orchestrator_run_notify.notify_waiters();
        }
        Ok(report)
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

    /// Accumulates a usage delta onto run and session totals.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Lists usage-insight run records for the query window.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Lists model pricing records.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Creates or updates a model pricing record.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Creates or updates a usage budget policy.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Lists usage budget policies matching the filter.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Records a smart-routing decision for auditability.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Lists recorded smart-routing decisions matching the filter.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Creates or updates a usage alert.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Lists usage alerts matching the filter.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Loads the most recent approval recorded for a subject id.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Appends one tape event for a run and bumps the tape counter.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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
    ) -> Result<crate::journal::OrchestratorCancelSnapshot, Status> {
        self.journal_store
            .request_orchestrator_cancel(request)
            .map_err(|error| map_orchestrator_store_error("request orchestrator cancel", error))
    }

    /// Records a cancel request for a run, bumps the cancel counter, and returns the resulting
    /// cancel snapshot. The run loop observes the flag at its next checkpoint; cancellation is
    /// cooperative.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn request_orchestrator_cancel(
        self: &Arc<Self>,
        request: OrchestratorCancelRequest,
    ) -> Result<RunCancelSnapshot, Status> {
        let state = Arc::clone(self);
        let snapshot = tokio::task::spawn_blocking(move || {
            state.request_orchestrator_cancel_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("orchestrator cancel worker panicked"))??;
        self.counters.orchestrator_cancel_requests.fetch_add(1, Ordering::Relaxed);
        Ok(RunCancelSnapshot {
            run_id: snapshot.run_id,
            state: snapshot.state,
            cancel_requested: snapshot.cancel_requested,
            reason: snapshot.reason,
        })
    }

    #[allow(clippy::result_large_err)]
    fn is_orchestrator_cancel_requested_blocking(&self, run_id: &str) -> Result<bool, Status> {
        self.journal_store
            .is_orchestrator_cancel_requested(run_id)
            .map_err(|error| map_orchestrator_store_error("load orchestrator cancel flag", error))
    }

    /// Reads the cancel-requested flag for a run.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Loads the current run status snapshot, if the run exists.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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
    fn list_orchestrator_run_status_snapshots_blocking(
        &self,
        run_ids: &[String],
    ) -> Result<Vec<OrchestratorRunStatusSnapshot>, Status> {
        self.journal_store
            .list_orchestrator_run_status_snapshots(run_ids)
            .map_err(|error| map_orchestrator_store_error("list orchestrator run snapshots", error))
    }

    /// Loads current run status snapshots for the supplied run ids.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_orchestrator_run_status_snapshots(
        self: &Arc<Self>,
        run_ids: Vec<String>,
    ) -> Result<Vec<OrchestratorRunStatusSnapshot>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.list_orchestrator_run_status_snapshots_blocking(run_ids.as_slice())
        })
        .await
        .map_err(|_| Status::internal("orchestrator snapshot list worker panicked"))?
    }

    // Diagnostics accept either an orchestrator run id or a cron run id;
    // cron runs are followed to the orchestrator run they spawned.
    #[allow(clippy::result_large_err)]
    fn resolve_orchestrator_diagnostics_run_id_blocking(
        &self,
        run_id: &str,
    ) -> Result<Option<String>, Status> {
        let run_exists = self
            .journal_store
            .orchestrator_run_status_snapshot(run_id)
            .map_err(|error| map_orchestrator_store_error("load orchestrator run snapshot", error))?
            .is_some();
        if run_exists {
            return Ok(Some(run_id.to_owned()));
        }

        let cron_run = self
            .journal_store
            .cron_run(run_id)
            .map_err(|error| map_cron_store_error("load cron run", error))?;
        Ok(cron_run.and_then(|run| run.orchestrator_run_id))
    }

    /// Maps an operator-supplied id to an orchestrator run id, following cron run linkage when
    /// needed.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn resolve_orchestrator_diagnostics_run_id(
        self: &Arc<Self>,
        run_id: String,
    ) -> Result<Option<String>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.resolve_orchestrator_diagnostics_run_id_blocking(run_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("orchestrator diagnostics run-id resolver worker panicked"))?
    }

    /// Waits until the run reaches a terminal phase (or also a waiting phase
    /// when `return_on_waiting` is set), up to `request.timeout`.
    ///
    /// # Errors
    /// `not_found` for unknown runs, `failed_precondition` for unknown
    /// lifecycle states, `deadline_exceeded` on timeout, plus mapped journal
    /// errors from the snapshot reads.
    #[allow(clippy::result_large_err)]
    pub async fn wait_for_orchestrator_run(
        self: &Arc<Self>,
        request: OrchestratorRunWaitRequest,
    ) -> Result<OrchestratorRunWaitOutcome, Status> {
        let poll_interval = request.poll_interval.max(Duration::from_millis(25));
        let wait = async {
            loop {
                let snapshot = self
                    .orchestrator_run_status_snapshot(request.run_id.clone())
                    .await?
                    .ok_or_else(|| {
                        Status::not_found(format!("orchestrator run not found: {}", request.run_id))
                    })?;
                let canonical_state = canonical_phase_from_snapshot(&snapshot)?;
                if canonical_state.is_terminal()
                    || (request.return_on_waiting && canonical_state.is_waiting())
                {
                    return Ok(OrchestratorRunWaitOutcome { snapshot, canonical_state });
                }
                // Hybrid notify/poll: `notified()` is registered only after
                // the snapshot read, so a state change in that gap would be
                // missed; bounding the wait by poll_interval guarantees the
                // loop re-reads the snapshot regardless.
                let notified = self.orchestrator_run_notify.notified();
                let _ = tokio::time::timeout(poll_interval, notified).await;
            }
        };
        tokio::time::timeout(request.timeout, wait).await.map_err(|_| {
            Status::deadline_exceeded(format!(
                "timed out waiting for orchestrator run {}",
                request.run_id
            ))
        })?
    }

    // Tool result artifacts and tool jobs (background tool execution).

    #[allow(clippy::result_large_err)]
    fn create_tool_result_artifact_blocking(
        &self,
        request: &ToolResultArtifactCreateRequest,
    ) -> Result<ToolResultArtifactRef, Status> {
        self.journal_store
            .create_tool_result_artifact(request)
            .map_err(|error| map_orchestrator_store_error("create tool result artifact", error))
    }

    /// Persists a tool result artifact and returns its reference.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn create_tool_result_artifact(
        self: &Arc<Self>,
        request: ToolResultArtifactCreateRequest,
    ) -> Result<ToolResultArtifactRef, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.create_tool_result_artifact_blocking(&request))
            .await
            .map_err(|_| Status::internal("tool result artifact create worker panicked"))?
    }

    /// Maximum artifact payload size accepted by the journal store.
    pub(crate) fn tool_result_artifact_max_payload_bytes(&self) -> usize {
        self.journal_store.max_payload_bytes()
    }

    #[allow(clippy::result_large_err)]
    fn read_tool_result_artifact_blocking(
        &self,
        request: &ToolResultArtifactReadRequest,
    ) -> Result<ArtifactReadResponse, Status> {
        self.journal_store
            .read_tool_result_artifact(request)
            .map_err(|error| map_orchestrator_store_error("read tool result artifact", error))
    }

    /// Reads a tool result artifact range by reference.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn read_tool_result_artifact(
        self: &Arc<Self>,
        request: ToolResultArtifactReadRequest,
    ) -> Result<ArtifactReadResponse, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.read_tool_result_artifact_blocking(&request))
            .await
            .map_err(|_| Status::internal("tool result artifact read worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn create_tool_job_blocking(
        &self,
        request: &ToolJobCreateRequest,
    ) -> Result<ToolJobRecord, Status> {
        self.journal_store
            .create_tool_job(request)
            .map_err(|error| map_orchestrator_store_error("create tool job", error))
    }

    /// Creates a background tool job record.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn create_tool_job(
        self: &Arc<Self>,
        request: ToolJobCreateRequest,
    ) -> Result<ToolJobRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.create_tool_job_blocking(&request))
            .await
            .map_err(|_| Status::internal("tool job create worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn get_tool_job_blocking(&self, job_id: &str) -> Result<Option<ToolJobRecord>, Status> {
        self.journal_store
            .get_tool_job(job_id)
            .map_err(|error| map_orchestrator_store_error("get tool job", error))
    }

    /// Loads a tool job by id.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn get_tool_job(
        self: &Arc<Self>,
        job_id: String,
    ) -> Result<Option<ToolJobRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.get_tool_job_blocking(job_id.as_str()))
            .await
            .map_err(|_| Status::internal("tool job get worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_tool_jobs_blocking(
        &self,
        filter: &ToolJobsListFilter,
    ) -> Result<Vec<ToolJobRecord>, Status> {
        self.journal_store
            .list_tool_jobs(filter)
            .map_err(|error| map_orchestrator_store_error("list tool jobs", error))
    }

    /// Lists tool jobs matching the filter.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_tool_jobs(
        self: &Arc<Self>,
        filter: ToolJobsListFilter,
    ) -> Result<Vec<ToolJobRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.list_tool_jobs_blocking(&filter))
            .await
            .map_err(|_| Status::internal("tool job list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn transition_tool_job_blocking(
        &self,
        request: &ToolJobTransitionRequest,
    ) -> Result<ToolJobRecord, Status> {
        self.journal_store
            .transition_tool_job(request)
            .map_err(|error| map_orchestrator_store_error("transition tool job", error))
    }

    /// Applies a state transition to a tool job.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn transition_tool_job(
        self: &Arc<Self>,
        request: ToolJobTransitionRequest,
    ) -> Result<ToolJobRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.transition_tool_job_blocking(&request))
            .await
            .map_err(|_| Status::internal("tool job transition worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn append_tool_job_tail_blocking(
        &self,
        request: &ToolJobTailAppendRequest,
    ) -> Result<crate::journal::ToolJobTailEntry, Status> {
        self.journal_store
            .append_tool_job_tail(request)
            .map_err(|error| map_orchestrator_store_error("append tool job tail", error))
    }

    /// Appends an output tail entry to a tool job.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn append_tool_job_tail(
        self: &Arc<Self>,
        request: ToolJobTailAppendRequest,
    ) -> Result<crate::journal::ToolJobTailEntry, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.append_tool_job_tail_blocking(&request))
            .await
            .map_err(|_| Status::internal("tool job tail append worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn tail_tool_job_blocking(
        &self,
        request: &ToolJobTailReadRequest,
    ) -> Result<ToolJobTailPage, Status> {
        self.journal_store
            .tail_tool_job(request)
            .map_err(|error| map_orchestrator_store_error("tail tool job", error))
    }

    /// Reads a page of a tool job's output tail.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn tail_tool_job(
        self: &Arc<Self>,
        request: ToolJobTailReadRequest,
    ) -> Result<ToolJobTailPage, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.tail_tool_job_blocking(&request))
            .await
            .map_err(|_| Status::internal("tool job tail worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn attach_tool_job_blocking(
        &self,
        request: &ToolJobAttachRequest,
    ) -> Result<ToolJobRecord, Status> {
        self.journal_store
            .attach_tool_job(request)
            .map_err(|error| map_orchestrator_store_error("attach tool job", error))
    }

    /// Attaches a consumer to a tool job.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn attach_tool_job(
        self: &Arc<Self>,
        request: ToolJobAttachRequest,
    ) -> Result<ToolJobRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.attach_tool_job_blocking(&request))
            .await
            .map_err(|_| Status::internal("tool job attach worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn release_tool_job_attachment_blocking(&self, job_id: &str) -> Result<ToolJobRecord, Status> {
        self.journal_store
            .release_tool_job_attachment(job_id)
            .map_err(|error| map_orchestrator_store_error("release tool job attachment", error))
    }

    /// Releases a tool job's consumer attachment.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn release_tool_job_attachment(
        self: &Arc<Self>,
        job_id: String,
    ) -> Result<ToolJobRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.release_tool_job_attachment_blocking(job_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("tool job attachment release worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn retry_tool_job_blocking(
        &self,
        request: &ToolJobRetryRequest,
    ) -> Result<ToolJobRecord, Status> {
        self.journal_store
            .retry_tool_job(request)
            .map_err(|error| map_orchestrator_store_error("retry tool job", error))
    }

    /// Requeues a failed tool job for retry.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn retry_tool_job(
        self: &Arc<Self>,
        request: ToolJobRetryRequest,
    ) -> Result<ToolJobRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.retry_tool_job_blocking(&request))
            .await
            .map_err(|_| Status::internal("tool job retry worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn sweep_expired_tool_jobs_blocking(
        &self,
        now_unix_ms: i64,
        limit: usize,
    ) -> Result<Vec<ToolJobRecord>, Status> {
        self.journal_store
            .sweep_expired_tool_jobs(now_unix_ms, limit)
            .map_err(|error| map_orchestrator_store_error("sweep expired tool jobs", error))
    }

    /// Expires tool jobs past their deadline, bounded by `limit`.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn sweep_expired_tool_jobs(
        self: &Arc<Self>,
        now_unix_ms: i64,
        limit: usize,
    ) -> Result<Vec<ToolJobRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.sweep_expired_tool_jobs_blocking(now_unix_ms, limit)
        })
        .await
        .map_err(|_| Status::internal("tool job sweep worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn recover_stale_tool_jobs_blocking(
        &self,
        now_unix_ms: i64,
        stale_after_ms: i64,
        limit: usize,
    ) -> Result<Vec<ToolJobRecord>, Status> {
        self.journal_store
            .recover_stale_tool_jobs(now_unix_ms, stale_after_ms, limit)
            .map_err(|error| map_orchestrator_store_error("recover stale tool jobs", error))
    }

    /// Recovers jobs whose owner stopped heartbeating for `stale_after_ms`.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn recover_stale_tool_jobs(
        self: &Arc<Self>,
        now_unix_ms: i64,
        stale_after_ms: i64,
        limit: usize,
    ) -> Result<Vec<ToolJobRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.recover_stale_tool_jobs_blocking(now_unix_ms, stale_after_ms, limit)
        })
        .await
        .map_err(|_| Status::internal("tool job recovery worker panicked"))?
    }

    // Session history and context: runs, lineage, transcripts, window
    // search, recall artifacts, queued inputs, queue controls, pins,
    // compaction artifacts, checkpoints, workspace checkpoints/restore
    // reports, flows, background tasks, and learning records.

    #[allow(clippy::result_large_err)]
    fn list_orchestrator_session_runs_blocking(
        &self,
        session_id: &str,
    ) -> Result<Vec<OrchestratorRunStatusSnapshot>, Status> {
        self.journal_store
            .list_orchestrator_session_runs(session_id)
            .map_err(|error| map_orchestrator_store_error("list orchestrator session runs", error))
    }

    /// Lists run snapshots belonging to a session.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Updates a session's fork/branch lineage pointers.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Loads the transcript records for a session.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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
    fn search_orchestrator_session_windows_blocking(
        &self,
        request: &SessionSearchRequest,
    ) -> Result<SessionSearchOutcome, Status> {
        self.journal_store.search_orchestrator_session_windows(request).map_err(|error| {
            map_orchestrator_store_error("search orchestrator session windows", error)
        })
    }

    /// Searches transcript windows of a session.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn search_orchestrator_session_windows(
        self: &Arc<Self>,
        request: SessionSearchRequest,
    ) -> Result<SessionSearchOutcome, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.search_orchestrator_session_windows_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("orchestrator session search worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn create_recall_artifact_blocking(
        &self,
        request: &RecallArtifactCreateRequest,
    ) -> Result<RecallArtifactRecord, Status> {
        self.journal_store
            .create_recall_artifact(request)
            .map_err(|error| map_memory_store_error("create recall artifact", error))
    }

    /// Persists a recall artifact.
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn create_recall_artifact(
        self: &Arc<Self>,
        request: RecallArtifactCreateRequest,
    ) -> Result<RecallArtifactRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.create_recall_artifact_blocking(&request))
            .await
            .map_err(|_| Status::internal("recall artifact create worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_recall_artifacts_blocking(
        &self,
        filter: &RecallArtifactListFilter,
    ) -> Result<Vec<RecallArtifactRecord>, Status> {
        self.journal_store
            .list_recall_artifacts(filter)
            .map_err(|error| map_memory_store_error("list recall artifacts", error))
    }

    /// Lists recall artifacts matching the filter.
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_recall_artifacts(
        self: &Arc<Self>,
        filter: RecallArtifactListFilter,
    ) -> Result<Vec<RecallArtifactRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.list_recall_artifacts_blocking(&filter))
            .await
            .map_err(|_| Status::internal("recall artifact list worker panicked"))?
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

    /// Queues an input for a busy session.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Updates the state of a queued input.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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
    fn prioritize_orchestrator_queued_input_blocking(
        &self,
        queued_input_id: &str,
        priority_lane: &str,
        decision_reason: &str,
        explain_json: &str,
    ) -> Result<(), Status> {
        self.journal_store
            .prioritize_orchestrator_queued_input(
                queued_input_id,
                priority_lane,
                decision_reason,
                explain_json,
            )
            .map_err(|error| {
                map_orchestrator_store_error("prioritize queued orchestrator input", error)
            })
    }

    /// Moves a queued input to a priority lane, recording the decision reason and explanation for
    /// audit.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn prioritize_orchestrator_queued_input(
        self: &Arc<Self>,
        queued_input_id: String,
        priority_lane: String,
        decision_reason: String,
        explain_json: String,
    ) -> Result<(), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.prioritize_orchestrator_queued_input_blocking(
                queued_input_id.as_str(),
                priority_lane.as_str(),
                decision_reason.as_str(),
                explain_json.as_str(),
            )
        })
        .await
        .map_err(|_| Status::internal("orchestrator queued input priority worker panicked"))?
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

    /// Loads a session's queue control record, if any.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Creates or updates a session's queue control record.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Lists the queued inputs of a session.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Pins a transcript item in a session.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Lists the pins of a session.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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
    fn delete_orchestrator_session_pin_blocking(
        &self,
        session_id: &str,
        pin_id: &str,
    ) -> Result<bool, Status> {
        self.journal_store
            .delete_orchestrator_session_pin(session_id, pin_id)
            .map_err(|error| map_orchestrator_store_error("delete orchestrator session pin", error))
    }

    /// Deletes a session pin; returns whether it existed in the supplied session.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn delete_orchestrator_session_pin(
        self: &Arc<Self>,
        session_id: String,
        pin_id: String,
    ) -> Result<bool, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.delete_orchestrator_session_pin_blocking(session_id.as_str(), pin_id.as_str())
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

    /// Persists a context-compaction artifact for a session.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Lists the compaction artifacts of a session.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Loads a compaction artifact by id.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Persists a conversation checkpoint.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Lists the checkpoints of a session.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Loads a checkpoint by id.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Marks a checkpoint as restored.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Persists a workspace (file-state) checkpoint.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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
    fn link_workspace_checkpoint_pair_blocking(
        &self,
        request: &WorkspaceCheckpointPairLinkRequest,
    ) -> Result<(), Status> {
        self.journal_store
            .link_workspace_checkpoint_pair(request)
            .map_err(|error| map_orchestrator_store_error("link workspace checkpoint pair", error))
    }

    /// Links the pre/post workspace checkpoints of one operation.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn link_workspace_checkpoint_pair(
        self: &Arc<Self>,
        request: WorkspaceCheckpointPairLinkRequest,
    ) -> Result<(), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.link_workspace_checkpoint_pair_blocking(&request))
            .await
            .map_err(|_| Status::internal("workspace checkpoint pair worker panicked"))?
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

    /// Lists workspace checkpoints matching the filter.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Loads a workspace checkpoint by id.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Lists the file records captured by a workspace checkpoint.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Loads one captured file payload by artifact id.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Loads a workspace restore report by id.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Lists workspace restore reports matching the filter.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Aggregates workspace restore activity for the filter window.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Persists a workspace restore report.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Marks a workspace checkpoint as restored.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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
    fn create_flow_blocking(&self, request: &FlowCreateRequest) -> Result<FlowRecord, Status> {
        self.journal_store
            .create_flow(request)
            .map_err(|error| map_orchestrator_store_error("create flow", error))
    }

    /// Creates a flow record.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn create_flow(
        self: &Arc<Self>,
        request: FlowCreateRequest,
    ) -> Result<FlowRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.create_flow_blocking(&request))
            .await
            .map_err(|_| Status::internal("flow create worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_flows_blocking(&self, filter: &FlowListFilter) -> Result<Vec<FlowRecord>, Status> {
        self.journal_store
            .list_flows(filter)
            .map_err(|error| map_orchestrator_store_error("list flows", error))
    }

    /// Lists flows matching the filter.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_flows(
        self: &Arc<Self>,
        filter: FlowListFilter,
    ) -> Result<Vec<FlowRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.list_flows_blocking(&filter))
            .await
            .map_err(|_| Status::internal("flow list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn get_flow_bundle_blocking(
        &self,
        flow_id: &str,
        event_limit: usize,
    ) -> Result<Option<FlowBundleRecord>, Status> {
        self.journal_store
            .get_flow_bundle(flow_id, event_limit)
            .map_err(|error| map_orchestrator_store_error("load flow", error))
    }

    /// Loads a flow with up to `event_limit` of its events.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn get_flow_bundle(
        self: &Arc<Self>,
        flow_id: String,
        event_limit: usize,
    ) -> Result<Option<FlowBundleRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.get_flow_bundle_blocking(flow_id.as_str(), event_limit)
        })
        .await
        .map_err(|_| Status::internal("flow detail worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn transition_flow_blocking(
        &self,
        request: &FlowTransitionRequest,
    ) -> Result<FlowRecord, Status> {
        self.journal_store
            .transition_flow(request)
            .map_err(|error| map_orchestrator_store_error("transition flow", error))
    }

    /// Applies a state transition to a flow.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn transition_flow(
        self: &Arc<Self>,
        request: FlowTransitionRequest,
    ) -> Result<FlowRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.transition_flow_blocking(&request))
            .await
            .map_err(|_| Status::internal("flow transition worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn update_flow_step_blocking(
        &self,
        request: &FlowStepUpdateRequest,
    ) -> Result<FlowStepRecord, Status> {
        self.journal_store
            .update_flow_step(request)
            .map_err(|error| map_orchestrator_store_error("update flow step", error))
    }

    /// Updates one step of a flow.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn update_flow_step(
        self: &Arc<Self>,
        request: FlowStepUpdateRequest,
    ) -> Result<FlowStepRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.update_flow_step_blocking(&request))
            .await
            .map_err(|_| Status::internal("flow step update worker panicked"))?
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

    /// Creates a background task record.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Updates a background task record.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Lists background tasks matching the filter.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Loads a background task by id.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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
    fn create_work_item_blocking(
        &self,
        request: &WorkItemCreateRequest,
    ) -> Result<WorkItemRecord, Status> {
        self.journal_store
            .create_work_item(request)
            .map_err(|error| map_orchestrator_store_error("create work item", error))
    }

    /// Creates a WorkBoard item.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn create_work_item(
        self: &Arc<Self>,
        request: WorkItemCreateRequest,
    ) -> Result<WorkItemRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.create_work_item_blocking(&request))
            .await
            .map_err(|_| Status::internal("work item create worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn update_work_item_blocking(
        &self,
        request: &WorkItemUpdateRequest,
    ) -> Result<WorkItemRecord, Status> {
        self.journal_store
            .update_work_item(request)
            .map_err(|error| map_orchestrator_store_error("update work item", error))
    }

    /// Updates a WorkBoard item.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn update_work_item(
        self: &Arc<Self>,
        request: WorkItemUpdateRequest,
    ) -> Result<WorkItemRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.update_work_item_blocking(&request))
            .await
            .map_err(|_| Status::internal("work item update worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_work_items_blocking(
        &self,
        filter: &WorkItemListFilter,
    ) -> Result<Vec<WorkItemRecord>, Status> {
        self.journal_store
            .list_work_items(filter)
            .map_err(|error| map_orchestrator_store_error("list work items", error))
    }

    /// Lists WorkBoard items matching the filter.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_work_items(
        self: &Arc<Self>,
        filter: WorkItemListFilter,
    ) -> Result<Vec<WorkItemRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.list_work_items_blocking(&filter))
            .await
            .map_err(|_| Status::internal("work item list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn get_work_item_blocking(&self, work_item_id: &str) -> Result<Option<WorkItemRecord>, Status> {
        self.journal_store
            .get_work_item(work_item_id)
            .map_err(|error| map_orchestrator_store_error("load work item", error))
    }

    /// Loads a WorkBoard item by id.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn get_work_item(
        self: &Arc<Self>,
        work_item_id: String,
    ) -> Result<Option<WorkItemRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.get_work_item_blocking(work_item_id.as_str()))
            .await
            .map_err(|_| Status::internal("work item detail worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_work_item_events_blocking(
        &self,
        work_item_id: &str,
        limit: usize,
    ) -> Result<Vec<WorkItemEventRecord>, Status> {
        self.journal_store
            .list_work_item_events(work_item_id, limit)
            .map_err(|error| map_orchestrator_store_error("list work item events", error))
    }

    /// Lists a WorkBoard item's audit events.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_work_item_events(
        self: &Arc<Self>,
        work_item_id: String,
        limit: usize,
    ) -> Result<Vec<WorkItemEventRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.list_work_item_events_blocking(work_item_id.as_str(), limit)
        })
        .await
        .map_err(|_| Status::internal("work item event list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn create_commitment_blocking(
        &self,
        request: &CommitmentCreateRequest,
    ) -> Result<CommitmentRecord, Status> {
        self.journal_store
            .create_commitment(request)
            .map_err(|error| map_orchestrator_store_error("create commitment", error))
    }

    /// Creates a commitment record.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn create_commitment(
        self: &Arc<Self>,
        request: CommitmentCreateRequest,
    ) -> Result<CommitmentRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.create_commitment_blocking(&request))
            .await
            .map_err(|_| Status::internal("commitment create worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn update_commitment_blocking(
        &self,
        request: &CommitmentUpdateRequest,
    ) -> Result<CommitmentRecord, Status> {
        self.journal_store
            .update_commitment(request)
            .map_err(|error| map_orchestrator_store_error("update commitment", error))
    }

    /// Updates a commitment record.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn update_commitment(
        self: &Arc<Self>,
        request: CommitmentUpdateRequest,
    ) -> Result<CommitmentRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.update_commitment_blocking(&request))
            .await
            .map_err(|_| Status::internal("commitment update worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_commitments_blocking(
        &self,
        filter: &CommitmentListFilter,
    ) -> Result<Vec<CommitmentRecord>, Status> {
        self.journal_store
            .list_commitments(filter)
            .map_err(|error| map_orchestrator_store_error("list commitments", error))
    }

    /// Lists commitments matching the filter.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_commitments(
        self: &Arc<Self>,
        filter: CommitmentListFilter,
    ) -> Result<Vec<CommitmentRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.list_commitments_blocking(&filter))
            .await
            .map_err(|_| Status::internal("commitment list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn get_commitment_blocking(
        &self,
        commitment_id: &str,
    ) -> Result<Option<CommitmentRecord>, Status> {
        self.journal_store
            .get_commitment(commitment_id)
            .map_err(|error| map_orchestrator_store_error("load commitment", error))
    }

    /// Loads a commitment by id.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn get_commitment(
        self: &Arc<Self>,
        commitment_id: String,
    ) -> Result<Option<CommitmentRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.get_commitment_blocking(commitment_id.as_str()))
            .await
            .map_err(|_| Status::internal("commitment detail worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_commitment_sources_blocking(
        &self,
        commitment_id: &str,
    ) -> Result<Vec<CommitmentSourceRecord>, Status> {
        self.journal_store
            .list_commitment_sources(commitment_id)
            .map_err(|error| map_orchestrator_store_error("list commitment sources", error))
    }

    /// Lists source evidence for a commitment.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_commitment_sources(
        self: &Arc<Self>,
        commitment_id: String,
    ) -> Result<Vec<CommitmentSourceRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.list_commitment_sources_blocking(commitment_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("commitment source list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_commitment_events_blocking(
        &self,
        commitment_id: &str,
        limit: usize,
    ) -> Result<Vec<CommitmentEventRecord>, Status> {
        self.journal_store
            .list_commitment_events(commitment_id, limit)
            .map_err(|error| map_orchestrator_store_error("list commitment events", error))
    }

    /// Lists commitment audit events.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_commitment_events(
        self: &Arc<Self>,
        commitment_id: String,
        limit: usize,
    ) -> Result<Vec<CommitmentEventRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.list_commitment_events_blocking(commitment_id.as_str(), limit)
        })
        .await
        .map_err(|_| Status::internal("commitment event list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn create_commitment_delivery_attempt_blocking(
        &self,
        request: &CommitmentDeliveryAttemptCreateRequest,
    ) -> Result<CommitmentDeliveryAttemptRecord, Status> {
        self.journal_store.create_commitment_delivery_attempt(request).map_err(|error| {
            map_orchestrator_store_error("create commitment delivery attempt", error)
        })
    }

    /// Records a commitment delivery attempt.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn create_commitment_delivery_attempt(
        self: &Arc<Self>,
        request: CommitmentDeliveryAttemptCreateRequest,
    ) -> Result<CommitmentDeliveryAttemptRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.create_commitment_delivery_attempt_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("commitment delivery attempt worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_commitment_delivery_attempts_blocking(
        &self,
        commitment_id: &str,
        limit: usize,
    ) -> Result<Vec<CommitmentDeliveryAttemptRecord>, Status> {
        self.journal_store.list_commitment_delivery_attempts(commitment_id, limit).map_err(
            |error| map_orchestrator_store_error("list commitment delivery attempts", error),
        )
    }

    /// Lists commitment delivery attempts.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_commitment_delivery_attempts(
        self: &Arc<Self>,
        commitment_id: String,
        limit: usize,
    ) -> Result<Vec<CommitmentDeliveryAttemptRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.list_commitment_delivery_attempts_blocking(commitment_id.as_str(), limit)
        })
        .await
        .map_err(|_| Status::internal("commitment delivery attempt list worker panicked"))?
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

    /// Creates or updates a learning candidate.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Applies a review decision to a learning candidate.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Lists learning candidates matching the filter.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Loads the review history of a learning candidate.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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
    fn record_learning_candidate_eval_blocking(
        &self,
        request: &LearningCandidateEvalCreateRequest,
    ) -> Result<LearningCandidateEvalRecord, Status> {
        self.journal_store
            .record_learning_candidate_eval(request)
            .map_err(|error| map_orchestrator_store_error("record learning candidate eval", error))
    }

    /// Appends an evaluation gate result for a learning candidate.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn record_learning_candidate_eval(
        self: &Arc<Self>,
        request: LearningCandidateEvalCreateRequest,
    ) -> Result<LearningCandidateEvalRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.record_learning_candidate_eval_blocking(&request))
            .await
            .map_err(|_| Status::internal("learning candidate eval worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_learning_candidate_evals_blocking(
        &self,
        candidate_id: &str,
        limit: usize,
    ) -> Result<Vec<LearningCandidateEvalRecord>, Status> {
        self.journal_store
            .list_learning_candidate_evals(candidate_id, limit)
            .map_err(|error| map_orchestrator_store_error("list learning candidate evals", error))
    }

    /// Lists evaluation gate results for a learning candidate.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_learning_candidate_evals(
        self: &Arc<Self>,
        candidate_id: String,
        limit: usize,
    ) -> Result<Vec<LearningCandidateEvalRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.list_learning_candidate_evals_blocking(candidate_id.as_str(), limit)
        })
        .await
        .map_err(|_| Status::internal("learning candidate eval list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn record_learning_candidate_rollout_blocking(
        &self,
        request: &LearningCandidateRolloutCreateRequest,
    ) -> Result<LearningCandidateRolloutRecord, Status> {
        self.journal_store.record_learning_candidate_rollout(request).map_err(|error| {
            map_orchestrator_store_error("record learning candidate rollout", error)
        })
    }

    /// Appends a rollout, monitoring, or rollback event for a learning candidate.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn record_learning_candidate_rollout(
        self: &Arc<Self>,
        request: LearningCandidateRolloutCreateRequest,
    ) -> Result<LearningCandidateRolloutRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.record_learning_candidate_rollout_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("learning candidate rollout worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_learning_candidate_rollouts_blocking(
        &self,
        candidate_id: &str,
        limit: usize,
    ) -> Result<Vec<LearningCandidateRolloutRecord>, Status> {
        self.journal_store.list_learning_candidate_rollouts(candidate_id, limit).map_err(|error| {
            map_orchestrator_store_error("list learning candidate rollouts", error)
        })
    }

    /// Lists rollout, monitoring, and rollback events for a learning candidate.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_learning_candidate_rollouts(
        self: &Arc<Self>,
        candidate_id: String,
        limit: usize,
    ) -> Result<Vec<LearningCandidateRolloutRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.list_learning_candidate_rollouts_blocking(candidate_id.as_str(), limit)
        })
        .await
        .map_err(|_| Status::internal("learning candidate rollout list worker panicked"))?
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

    /// Creates or updates a learned preference.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Lists learned preferences matching the filter.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
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

    /// Assembles a redacted, budgeted page of tape events for a run.
    ///
    /// Payloads are redacted before byte accounting, the page is capped by
    /// both the entry and byte budgets, and `next_after_seq` is set when more
    /// events remain.
    ///
    /// # Errors
    /// `not_found` for unknown runs, `resource_exhausted` when a single event
    /// alone exceeds the byte budget (otherwise pagination could never make
    /// progress), plus mapped journal errors.
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

    /// Async wrapper for `Self::orchestrator_tape_snapshot_blocking`.
    ///
    /// # Errors
    /// Same as the blocking variant, plus `internal` if the worker panicked.
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
    #[allow(dead_code)]
    fn incident_replay_bundle_blocking(
        &self,
        run_id: &str,
        requested_limit: Option<usize>,
    ) -> Result<ReplayBundle, Status> {
        let max_events = requested_limit
            .unwrap_or(self.config.replay_capture.max_events_per_run)
            .clamp(MIN_TAPE_PAGE_LIMIT, self.config.replay_capture.max_events_per_run);
        crate::replay_capture::capture_incident_replay_bundle(
            crate::replay_capture::IncidentReplayCaptureRequest {
                journal_store: &self.journal_store,
                replay_capture: &self.config.replay_capture,
                feature_rollouts: &self.config.feature_rollouts,
                run_id,
                generated_at_unix_ms: current_unix_ms(),
                max_events,
            },
        )
        .map_err(|error| Status::internal(format!("failed to capture replay bundle: {error:#}")))
    }

    /// Captures a deterministic replay bundle for an incident run, bounded by
    /// the replay-capture event budget.
    ///
    /// # Errors
    /// `internal` when capture fails or the worker panicked.
    #[allow(clippy::result_large_err)]
    #[allow(dead_code)]
    pub async fn incident_replay_bundle(
        self: &Arc<Self>,
        run_id: String,
        limit: Option<usize>,
    ) -> Result<ReplayBundle, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.incident_replay_bundle_blocking(run_id.as_str(), limit)
        })
        .await
        .map_err(|_| Status::internal("incident replay capture worker panicked"))?
    }

    // Cron jobs and cron runs.

    #[allow(clippy::result_large_err)]
    fn create_cron_job_blocking(
        &self,
        request: &CronJobCreateRequest,
    ) -> Result<CronJobRecord, Status> {
        self.journal_store
            .create_cron_job(request)
            .map_err(|error| map_cron_store_error("create cron job", error))
    }

    /// Creates a cron job and counts the creation.
    ///
    /// # Errors
    /// Returns the mapped cron store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn create_cron_job(
        self: &Arc<Self>,
        request: CronJobCreateRequest,
    ) -> Result<CronJobRecord, Status> {
        if request.enabled {
            crate::cron::ensure_archived_objective_allows_cron_job_enable(
                self.as_ref(),
                request.job_id.as_str(),
            )?;
        }
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

    /// Applies a patch to a cron job and counts the update.
    ///
    /// # Errors
    /// Returns the mapped cron store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn update_cron_job(
        self: &Arc<Self>,
        job_id: String,
        patch: CronJobUpdatePatch,
    ) -> Result<CronJobRecord, Status> {
        if patch.enabled == Some(true) {
            crate::cron::ensure_archived_objective_allows_cron_job_enable(
                self.as_ref(),
                job_id.as_str(),
            )?;
        }
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

    /// Deletes a cron job; returns whether it existed and counts the deletion.
    ///
    /// # Errors
    /// Returns the mapped cron store error, or `internal` if the worker panicked.
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

    /// Loads a cron job by id.
    ///
    /// # Errors
    /// Returns the mapped cron store error, or `internal` if the worker panicked.
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

    /// Lists cron jobs with cursor pagination and optional enabled/owner/channel filters.
    ///
    /// # Errors
    /// Returns the mapped cron store error, or `internal` if the worker panicked.
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

    /// Lists enabled cron jobs due at or before `now_unix_ms`.
    ///
    /// # Errors
    /// Returns the mapped cron store error, or `internal` if the worker panicked.
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

    /// Earliest `next_run_at` across cron jobs, if any (scheduler wake time).
    ///
    /// # Errors
    /// Returns the mapped cron store error, or `internal` if the worker panicked.
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

    /// Updates a cron job's next/last run timestamps.
    ///
    /// # Errors
    /// Returns the mapped cron store error, or `internal` if the worker panicked.
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

    /// Sets whether a cron job has a queued run pending.
    ///
    /// # Errors
    /// Returns the mapped cron store error, or `internal` if the worker panicked.
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

    /// Records the start of a cron run and counts it.
    ///
    /// # Errors
    /// Returns the mapped cron store error, or `internal` if the worker panicked.
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

    /// Finalizes a cron run and bumps the counter matching its terminal status.
    ///
    /// # Errors
    /// Returns the mapped cron store error, or `internal` if the worker panicked.
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

    /// Loads a cron run by id.
    ///
    /// # Errors
    /// Returns the mapped cron store error, or `internal` if the worker panicked.
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

    /// Loads the active (non-terminal) cron run of a job, if any.
    ///
    /// # Errors
    /// Returns the mapped cron store error, or `internal` if the worker panicked.
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

    /// Lists cron runs (optionally for one job) with cursor pagination.
    ///
    /// # Errors
    /// Returns the mapped cron store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_cron_runs(
        self: &Arc<Self>,
        job_id: Option<String>,
        after_run_id: Option<String>,
        requested_limit: Option<usize>,
    ) -> Result<(Vec<CronRunRecord>, Option<String>), Status> {
        self.list_cron_runs_filtered(job_id, None, after_run_id, requested_limit).await
    }

    /// Lists cron runs across all jobs owned by a principal, with cursor pagination.
    ///
    /// # Errors
    /// Returns the mapped cron store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_cron_runs_for_owner(
        self: &Arc<Self>,
        owner_principal: String,
        after_run_id: Option<String>,
        requested_limit: Option<usize>,
    ) -> Result<(Vec<CronRunRecord>, Option<String>), Status> {
        self.list_cron_runs_filtered(None, Some(owner_principal), after_run_id, requested_limit)
            .await
    }

    #[allow(clippy::result_large_err)]
    async fn list_cron_runs_filtered(
        self: &Arc<Self>,
        job_id: Option<String>,
        owner_principal: Option<String>,
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
                    owner_principal: owner_principal.as_deref(),
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

    // Approvals: persistence plus the session-scoped decision cache that lets
    // identical tool proposals reuse a prior Allow/Deny within its scope.

    /// Persists an approval request; tool-subject requests bump the request
    /// counter.
    ///
    /// # Errors
    /// Returns the mapped approval store error, or `internal` if the worker
    /// panicked.
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

    /// Resolves an approval. For tool subjects this also bumps the matching
    /// decision counter and seeds the session approval cache so later
    /// identical proposals can reuse the decision within its scope.
    ///
    /// # Errors
    /// Returns the mapped approval store error, or `internal` if the worker
    /// panicked.
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

    /// Loads one approval by id.
    ///
    /// # Errors
    /// Returns the mapped approval store error, or `internal` if the worker
    /// panicked.
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

    /// Atomically consumes an allowed once-scoped approval.
    ///
    /// Returns `false` when the approval was already consumed or is not
    /// currently an allowed once-scoped approval.
    ///
    /// # Errors
    /// Returns the mapped approval store error, or `internal` if the worker
    /// panicked.
    #[allow(clippy::result_large_err)]
    pub async fn consume_approval_once(
        self: &Arc<Self>,
        approval_id: String,
        consume_reason: String,
    ) -> Result<bool, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .consume_approval_once(approval_id.as_str(), consume_reason.as_str())
                .map_err(|error| map_approval_store_error("consume approval", error))
        })
        .await
        .map_err(|_| Status::internal("approval consume worker panicked"))?
    }

    /// Lists approvals with cursor pagination and optional time, subject,
    /// principal, decision, and subject-type filters.
    ///
    /// # Errors
    /// Returns the mapped approval store error, or `internal` if the worker
    /// panicked.
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

    // Tool posture registry delegates (synchronous; the registry has its own
    // storage). All of them map registry failures to `Status::internal`.

    /// Lists all tool posture overrides.
    ///
    /// # Errors
    /// `internal` when the posture registry read fails.
    pub fn list_tool_posture_overrides(&self) -> Result<Vec<ToolPostureOverrideRecord>, Status> {
        self.tool_posture_registry.list_overrides().map_err(|error| {
            Status::internal(format!("failed to list tool posture overrides: {error}"))
        })
    }

    /// Lists recorded posture recommendation actions.
    ///
    /// # Errors
    /// `internal` when the posture registry read fails.
    pub fn list_tool_posture_recommendation_actions(
        &self,
    ) -> Result<Vec<ToolPostureRecommendationActionRecord>, Status> {
        self.tool_posture_registry.list_recommendation_actions().map_err(|error| {
            Status::internal(format!("failed to list tool posture recommendation actions: {error}"))
        })
    }

    /// Lists posture audit events.
    ///
    /// # Errors
    /// `internal` when the posture registry read fails.
    pub fn list_tool_posture_audit_events(
        &self,
    ) -> Result<Vec<ToolPostureAuditEventRecord>, Status> {
        self.tool_posture_registry.list_audit_events().map_err(|error| {
            Status::internal(format!("failed to list tool posture audit events: {error}"))
        })
    }

    /// Creates or updates a posture override.
    ///
    /// # Errors
    /// `internal` when persisting the override fails.
    pub fn upsert_tool_posture_override(
        &self,
        request: ToolPostureOverrideUpsertRequest,
    ) -> Result<ToolPostureOverrideRecord, Status> {
        self.tool_posture_registry.upsert_override(request).map_err(|error| {
            Status::internal(format!("failed to persist tool posture override: {error}"))
        })
    }

    /// Clears a posture override; returns whether one existed.
    ///
    /// # Errors
    /// `internal` when clearing the override fails.
    pub fn clear_tool_posture_override(
        &self,
        request: ToolPostureOverrideClearRequest,
    ) -> Result<bool, Status> {
        self.tool_posture_registry.clear_override(request).map_err(|error| {
            Status::internal(format!("failed to clear tool posture override: {error}"))
        })
    }

    /// Resets all overrides in a scope, returning the removed records.
    ///
    /// # Errors
    /// `internal` when the scope reset fails.
    pub fn reset_tool_posture_scope(
        &self,
        request: ToolPostureScopeResetRequest,
    ) -> Result<Vec<ToolPostureOverrideRecord>, Status> {
        self.tool_posture_registry.reset_scope(request).map_err(|error| {
            Status::internal(format!("failed to reset tool posture scope: {error}"))
        })
    }

    /// Records the action taken on a posture recommendation.
    ///
    /// # Errors
    /// `internal` when persisting the action fails.
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

    // Skill status records and journaled runtime/console events.

    /// Persists a skill status record and counts the update.
    ///
    /// # Errors
    /// Returns the mapped skill store error, or `internal` if the worker
    /// panicked.
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

    /// Loads the status record for one skill version.
    ///
    /// # Errors
    /// Returns the mapped skill store error, or `internal` if the worker
    /// panicked.
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

    /// Loads the most recent status record for a skill across versions.
    ///
    /// # Errors
    /// Returns the mapped skill store error, or `internal` if the worker
    /// panicked.
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

    /// Journals a skill lifecycle event under fresh synthetic session/run ids
    /// (these events are not tied to a conversation).
    ///
    /// # Errors
    /// Same as `Self::record_journal_event`.
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

    /// Journals an operator console event under fresh synthetic session/run
    /// ids.
    ///
    /// # Errors
    /// Same as `Self::record_journal_event`.
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

    /// Journals a runtime decision event for an authenticated request context
    /// and feeds it into observability.
    ///
    /// # Errors
    /// Same as `Self::record_journal_event`.
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

    /// Journals a runtime decision event attributed to a system principal
    /// (background work with no request context).
    ///
    /// # Errors
    /// Same as `Self::record_journal_event`.
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

    /// Builds the decision actor for an authenticated request context.
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

    // Live runtime configuration (memory/retrieval/learning/routines) and
    // channel router delegates.

    /// Replaces the memory config and invalidates the memory search cache
    /// (cached scores depend on the old limits).
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

    /// Replaces the retrieval config and invalidates the memory search cache.
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

    /// Installs the routines/objectives wiring once the scheduler is up.
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

    /// Returns the routines wiring.
    ///
    /// # Errors
    /// `failed_precondition` until [`Self::configure_routines_runtime`] has
    /// run.
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

    /// Current memory config (cloned).
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

    /// Current retrieval config (cloned).
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

    /// Replaces the learning config (no caches depend on it).
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

    /// Current learning config (cloned).
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

    /// Channel router config as loaded at startup (cloned).
    #[must_use]
    pub fn channel_router_config_snapshot(&self) -> ChannelRouterConfig {
        self.config.channel_router.clone()
    }

    /// Stable hash of the active channel router config (drift detection).
    #[must_use]
    pub fn channel_router_config_hash(&self) -> String {
        self.channel_router.config_hash()
    }

    /// Full startup config snapshot (cloned).
    #[must_use]
    pub fn runtime_config_snapshot(&self) -> GatewayRuntimeConfigSnapshot {
        self.config.clone()
    }

    /// Validation warnings produced when the router config was loaded.
    #[must_use]
    pub fn channel_router_validation_warnings(&self) -> Vec<String> {
        self.channel_router.validation_warnings()
    }

    /// Dry-runs routing for an inbound message without delivering it.
    #[must_use]
    pub fn channel_router_preview(&self, message: &ChannelInboundMessage) -> ChannelRoutePreview {
        self.channel_router.preview_route(message)
    }

    /// Pairing state per channel (optionally filtered to one channel).
    #[must_use]
    pub fn channel_router_pairing_snapshot(
        &self,
        channel: Option<&str>,
    ) -> Vec<ChannelPairingSnapshot> {
        self.channel_router.pairing_snapshot(channel)
    }

    /// Mints a pairing code for a channel.
    ///
    /// # Errors
    /// `failed_precondition` with the router's refusal reason.
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

    /// Attempts to consume a pairing code for a sender identity.
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

    /// Associates an approval id with a pending pairing; `false` when no
    /// matching pending entry exists.
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

    /// Applies an operator approval/denial to a pending pairing.
    #[must_use]
    pub fn channel_router_apply_pairing_approval(
        &self,
        approval_id: &str,
        approved: bool,
        decision_scope_ttl_ms: Option<i64>,
    ) -> PairingApprovalOutcome {
        self.channel_router.apply_pairing_approval(approval_id, approved, decision_scope_ttl_ms)
    }

    // Networked worker fleet. The fleet manager lives behind an RwLock; each
    // mutation journals its lifecycle event afterwards, and cleanup paths
    // fail closed when a worker's cleanup report shows leftover scoped data.

    /// Builds the fleet admission policy from networked-worker config. The
    /// trusted capability list is currently a fixed built-in allowlist.
    #[must_use]
    pub fn worker_fleet_policy(&self) -> WorkerFleetPolicy {
        WorkerFleetPolicy {
            max_ttl_ms: self.config.networked_workers.lease_ttl_ms,
            heartbeat_timeout_ms: 30_000,
            trusted_capabilities: vec![
                "tool:palyra.echo".to_owned(),
                "tool:palyra.sleep".to_owned(),
            ],
            required_capability_authority_sha256: None,
            required_sdk_protocol_version: Some(1),
            required_wit_abi_version: Some("palyra-worker-abi/v1".to_owned()),
            attestation: palyra_workerd::WorkerAttestationExpectation {
                require_egress_proxy: self.config.networked_workers.require_attestation,
                image_digest_sha256: self
                    .config
                    .networked_workers
                    .expected_image_digest_sha256
                    .clone(),
                build_digest_sha256: self
                    .config
                    .networked_workers
                    .expected_build_digest_sha256
                    .clone(),
                artifact_digest_sha256: self
                    .config
                    .networked_workers
                    .expected_artifact_digest_sha256
                    .clone(),
            },
        }
    }

    /// Current worker fleet state.
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

    /// Recent worker lifecycle events retained by the fleet manager.
    #[must_use]
    pub fn worker_fleet_recent_events(&self) -> Vec<WorkerLifecycleEvent> {
        match self.worker_fleet.read() {
            Ok(manager) => manager.recent_events(),
            Err(poisoned) => {
                warn!("worker fleet lock poisoned while reading recent events");
                poisoned.into_inner().recent_events()
            }
        }
    }

    /// Admits a worker after attestation checks and journals the event.
    ///
    /// # Errors
    /// `failed_precondition` on policy rejection, plus journaling errors.
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

    /// Assigns a lease to a specific worker and journals the event.
    ///
    /// # Errors
    /// `failed_precondition` when assignment is refused, plus journaling
    /// errors.
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

    /// Assigns a lease to the next eligible worker and journals the event.
    ///
    /// # Errors
    /// `failed_precondition` when no worker can take the lease, plus
    /// journaling errors.
    #[allow(clippy::result_large_err)]
    #[allow(dead_code)]
    pub async fn assign_next_networked_worker_lease(
        self: &Arc<Self>,
        request: WorkerLeaseRequest,
    ) -> Result<(WorkerLease, WorkerLifecycleEvent), Status> {
        let policy = self.worker_fleet_policy();
        let now_unix_ms = current_unix_ms();
        let assign_work = |manager: &mut WorkerFleetManager| {
            manager.assign_next_work(request.clone(), &policy, now_unix_ms).map_err(|error| {
                Status::failed_precondition(format!(
                    "networked worker lease assignment failed: {error}"
                ))
            })
        };
        let (lease, event) = match self.worker_fleet.write() {
            Ok(mut manager) => assign_work(&mut manager)?,
            Err(poisoned) => {
                warn!("worker fleet lock poisoned while assigning next lease");
                let mut manager = poisoned.into_inner();
                assign_work(&mut manager)?
            }
        };
        self.record_networked_worker_lifecycle_event(&event).await?;
        Ok((lease, event))
    }

    /// Finalizes a worker's lease against its cleanup report. Fails closed:
    /// incomplete cleanup is journaled as a non-recoverable orphan and
    /// surfaced as an error so scoped data leaks need operator action.
    ///
    /// # Errors
    /// `failed_precondition` when finalization is refused or cleanup left
    /// scoped data behind, plus journaling errors.
    #[allow(clippy::result_large_err)]
    #[allow(dead_code)]
    pub async fn complete_networked_worker_lease(
        self: &Arc<Self>,
        worker_id: &str,
        cleanup_report: WorkerCleanupReport,
    ) -> Result<WorkerLifecycleEvent, Status> {
        let now_unix_ms = current_unix_ms();
        let outcome = match self.worker_fleet.write() {
            Ok(mut manager) => {
                manager.finalize_work(worker_id, cleanup_report, now_unix_ms).map_err(|error| {
                    Status::failed_precondition(format!("networked worker cleanup failed: {error}"))
                })?
            }
            Err(poisoned) => {
                warn!("worker fleet lock poisoned while completing worker lease");
                poisoned
                    .into_inner()
                    .finalize_work(worker_id, cleanup_report, now_unix_ms)
                    .map_err(|error| {
                        Status::failed_precondition(format!(
                            "networked worker cleanup failed: {error}"
                        ))
                    })?
            }
        };
        self.record_networked_worker_lifecycle_event_with_details(
            &outcome.event,
            json!({
                "cleanup_report": outcome.cleanup_report,
                "cleanup_succeeded": outcome.cleanup_succeeded,
                "orphan_classification": if outcome.cleanup_succeeded {
                    "resolved"
                } else {
                    "non_recoverable_requires_operator_cleanup"
                },
            }),
        )
        .await?;
        if outcome.cleanup_succeeded {
            Ok(outcome.event)
        } else {
            Err(Status::failed_precondition(outcome.cleanup_report.failure_reason.unwrap_or_else(
                || "networked worker cleanup did not remove all scoped data".to_owned(),
            )))
        }
    }

    /// Expires workers past their lease TTL, journaling each event with
    /// recovery guidance for operators.
    ///
    /// # Errors
    /// Journaling errors from recording the lifecycle events.
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
            self.record_networked_worker_lifecycle_event_with_details(
                event,
                json!({
                    "orphan_classification": "recoverable_requires_force_cleanup",
                    "recommended_actions": [
                        "force_cleanup",
                        "reverify",
                        "quarantine"
                    ],
                }),
            )
            .await?;
        }
        Ok(events)
    }

    /// Quarantines every worker (operator drain) and journals the events.
    ///
    /// # Errors
    /// Journaling errors from recording the lifecycle events.
    #[allow(clippy::result_large_err)]
    #[allow(dead_code)]
    pub async fn drain_networked_workers(
        self: &Arc<Self>,
    ) -> Result<Vec<WorkerLifecycleEvent>, Status> {
        let now_unix_ms = current_unix_ms();
        let events = match self.worker_fleet.write() {
            Ok(mut manager) => {
                manager.quarantine_all_workers("worker.drained_by_operator", now_unix_ms)
            }
            Err(poisoned) => {
                warn!("worker fleet lock poisoned while draining workers");
                poisoned
                    .into_inner()
                    .quarantine_all_workers("worker.drained_by_operator", now_unix_ms)
            }
        };
        for event in &events {
            self.record_networked_worker_lifecycle_event_with_details(
                event,
                json!({
                    "operator_action": "drain",
                    "cleanup_required": true,
                }),
            )
            .await?;
        }
        Ok(events)
    }

    /// Quarantines one worker by operator action and journals the event.
    ///
    /// # Errors
    /// `failed_precondition` when the worker cannot be quarantined, plus
    /// journaling errors.
    #[allow(clippy::result_large_err)]
    #[allow(dead_code)]
    pub async fn quarantine_networked_worker(
        self: &Arc<Self>,
        worker_id: &str,
    ) -> Result<WorkerLifecycleEvent, Status> {
        let now_unix_ms = current_unix_ms();
        let event = match self.worker_fleet.write() {
            Ok(mut manager) => manager
                .quarantine_worker(worker_id, "worker.quarantined_by_operator", now_unix_ms)
                .map_err(|error| {
                    Status::failed_precondition(format!(
                        "networked worker quarantine failed: {error}"
                    ))
                })?,
            Err(poisoned) => {
                warn!("worker fleet lock poisoned while quarantining worker");
                poisoned
                    .into_inner()
                    .quarantine_worker(worker_id, "worker.quarantined_by_operator", now_unix_ms)
                    .map_err(|error| {
                        Status::failed_precondition(format!(
                            "networked worker quarantine failed: {error}"
                        ))
                    })?
            }
        };
        self.record_networked_worker_lifecycle_event_with_details(
            &event,
            json!({ "operator_action": "quarantine" }),
        )
        .await?;
        Ok(event)
    }

    /// Re-runs attestation checks for a quarantined worker and journals the
    /// outcome.
    ///
    /// # Errors
    /// `failed_precondition` when re-verification fails, plus journaling
    /// errors.
    #[allow(clippy::result_large_err)]
    #[allow(dead_code)]
    pub async fn reverify_networked_worker(
        self: &Arc<Self>,
        worker_id: &str,
    ) -> Result<WorkerLifecycleEvent, Status> {
        let policy = self.worker_fleet_policy();
        let now_unix_ms = current_unix_ms();
        let event = match self.worker_fleet.write() {
            Ok(mut manager) => {
                manager.reverify_worker(worker_id, &policy, now_unix_ms).map_err(|error| {
                    Status::failed_precondition(format!(
                        "networked worker re-verification failed: {error}"
                    ))
                })?
            }
            Err(poisoned) => {
                warn!("worker fleet lock poisoned while re-verifying worker");
                poisoned.into_inner().reverify_worker(worker_id, &policy, now_unix_ms).map_err(
                    |error| {
                        Status::failed_precondition(format!(
                            "networked worker re-verification failed: {error}"
                        ))
                    },
                )?
            }
        };
        self.record_networked_worker_lifecycle_event_with_details(
            &event,
            json!({ "operator_action": "reverify" }),
        )
        .await?;
        Ok(event)
    }

    /// Operator-forced cleanup of an orphaned worker; fails closed when the
    /// cleanup report shows leftover scoped data.
    ///
    /// # Errors
    /// `failed_precondition` when cleanup is refused or incomplete, plus
    /// journaling errors.
    #[allow(clippy::result_large_err)]
    #[allow(dead_code)]
    pub async fn force_cleanup_networked_worker(
        self: &Arc<Self>,
        worker_id: &str,
        cleanup_report: WorkerCleanupReport,
    ) -> Result<WorkerLifecycleEvent, Status> {
        let now_unix_ms = current_unix_ms();
        let force_cleanup = |manager: &mut WorkerFleetManager| {
            manager.force_cleanup_worker(worker_id, cleanup_report.clone(), now_unix_ms).map_err(
                |error| {
                    Status::failed_precondition(format!(
                        "networked worker force cleanup failed: {error}"
                    ))
                },
            )
        };
        let outcome = match self.worker_fleet.write() {
            Ok(mut manager) => force_cleanup(&mut manager)?,
            Err(poisoned) => {
                warn!("worker fleet lock poisoned while force-cleaning worker");
                let mut manager = poisoned.into_inner();
                force_cleanup(&mut manager)?
            }
        };
        self.record_networked_worker_lifecycle_event_with_details(
            &outcome.event,
            json!({
                "operator_action": "force_cleanup",
                "cleanup_report": outcome.cleanup_report,
                "cleanup_succeeded": outcome.cleanup_succeeded,
                "orphan_classification": if outcome.cleanup_succeeded {
                    "resolved"
                } else {
                    "non_recoverable_requires_operator_cleanup"
                },
            }),
        )
        .await?;
        if outcome.cleanup_succeeded {
            Ok(outcome.event)
        } else {
            Err(Status::failed_precondition(outcome.cleanup_report.failure_reason.unwrap_or_else(
                || "networked worker force cleanup did not remove all scoped data".to_owned(),
            )))
        }
    }

    #[allow(clippy::result_large_err)]
    #[allow(dead_code)]
    async fn record_networked_worker_lifecycle_event(
        self: &Arc<Self>,
        event: &WorkerLifecycleEvent,
    ) -> Result<(), Status> {
        self.record_networked_worker_lifecycle_event_with_details(event, Value::Null).await
    }

    #[allow(clippy::result_large_err)]
    async fn record_networked_worker_lifecycle_event_with_details(
        self: &Arc<Self>,
        event: &WorkerLifecycleEvent,
        extra_details: Value,
    ) -> Result<(), Status> {
        use palyra_common::runtime_preview::{
            RuntimeDecisionEventType, RuntimeDecisionTiming, RuntimeEntityRef,
            RuntimeResourceBudget,
        };
        let mut details = json!({
            "run_id": event.run_id,
            "reason_code": event.reason_code,
            "state": event.state.as_str(),
        });
        if let (Some(details), Value::Object(extra)) = (details.as_object_mut(), extra_details) {
            for (key, value) in extra {
                details.insert(key, value);
            }
        }

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
            .with_details(details),
        )
        .await
    }

    // In-memory caches: memory search results and tool approval decisions.

    /// Drops every cached memory search result. Called on any write that can
    /// change search outcomes (config changes, item mutations, maintenance).
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

    /// Removes cached approval decisions for one session scope and bumps the
    /// scope's generation so in-flight `remember_*_if_generation` writes that
    /// raced this invalidation are discarded.
    pub(crate) fn clear_tool_approval_cache_for_session(
        &self,
        context: &RequestContext,
        session_id: &str,
    ) {
        let key_prefix = tool_approval_cache_key_prefix(context, session_id);
        match self.tool_approval_cache.lock() {
            Ok(mut cache) => {
                cache.decisions.retain(|key, _| !key.starts_with(key_prefix.as_str()));
                bump_tool_approval_cache_generation(&mut cache, key_prefix.as_str());
            }
            Err(poisoned) => {
                warn!("tool approval cache lock poisoned while clearing session cache");
                let mut cache = poisoned.into_inner();
                cache.decisions.retain(|key, _| !key.starts_with(key_prefix.as_str()));
                bump_tool_approval_cache_generation(&mut cache, key_prefix.as_str());
            }
        }
    }

    /// Current cache generation for the session scope. Read this before an
    /// approval wait and pass it to
    /// [`Self::remember_tool_approval_if_generation`] afterwards.
    pub(crate) fn tool_approval_cache_generation_for_session(
        &self,
        context: &RequestContext,
        session_id: &str,
    ) -> u64 {
        let key_prefix = tool_approval_cache_key_prefix(context, session_id);
        match self.tool_approval_cache.lock() {
            Ok(cache) => tool_approval_cache_generation(&cache, key_prefix.as_str()),
            Err(poisoned) => {
                warn!("tool approval cache lock poisoned while reading session cache generation");
                let cache = poisoned.into_inner();
                tool_approval_cache_generation(&cache, key_prefix.as_str())
            }
        }
    }

    /// Returns a previously remembered decision for the subject, pruning
    /// expired entries first; the returned TTL is the remaining lifetime.
    pub(crate) fn resolve_cached_tool_approval(
        &self,
        context: &RequestContext,
        session_id: &str,
        subject_id: &str,
    ) -> Option<ToolApprovalOutcome> {
        let now_unix_ms = current_unix_ms();
        let cache_key = tool_approval_cache_key(context, session_id, subject_id);
        let resolve_from_cache =
            |cache: &mut ToolApprovalCacheState| -> Option<ToolApprovalOutcome> {
                cache.decisions.retain(|_, entry| match entry.expires_at_unix_ms {
                    Some(expires_at_unix_ms) => expires_at_unix_ms > now_unix_ms,
                    None => true,
                });
                let cached = cache.decisions.get(cache_key.as_str())?.clone();
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

    /// Caches a decision unconditionally (no generation check); used when the
    /// decision is fresh and no invalidation race is possible.
    pub(crate) fn remember_tool_approval(
        &self,
        context: &RequestContext,
        session_id: &str,
        subject_id: &str,
        outcome: &ToolApprovalOutcome,
    ) {
        let _remembered = self
            .remember_tool_approval_if_generation(context, session_id, subject_id, outcome, None);
    }

    /// Caches an Allow/Deny decision for `Session`/`Timeboxed` scopes
    /// (`Once` decisions and timeboxed entries without a positive TTL are
    /// never cached). When `expected_generation` is set, the write is dropped
    /// if the session cache was invalidated while the approval was pending,
    /// so a stale decision cannot resurrect after a cache clear. Returns
    /// whether the decision was stored.
    pub(crate) fn remember_tool_approval_if_generation(
        &self,
        context: &RequestContext,
        session_id: &str,
        subject_id: &str,
        outcome: &ToolApprovalOutcome,
        expected_generation: Option<u64>,
    ) -> bool {
        if !matches!(outcome.decision, ApprovalDecision::Allow | ApprovalDecision::Deny) {
            return false;
        }
        let now_unix_ms = current_unix_ms();
        let expires_at_unix_ms = match outcome.decision_scope {
            ApprovalDecisionScope::Once => return false,
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
                    return false;
                };
                Some(now_unix_ms.saturating_add(ttl_ms))
            }
        };
        let cache_key = tool_approval_cache_key(context, session_id, subject_id);
        let generation_key = tool_approval_cache_key_prefix(context, session_id);
        let cache_entry = CachedToolApprovalDecision {
            approval_id: outcome.approval_id.clone(),
            approved: outcome.approved,
            reason: outcome.reason.clone(),
            decision: outcome.decision,
            decision_scope: outcome.decision_scope,
            expires_at_unix_ms,
        };
        let remember_in_cache = |cache: &mut ToolApprovalCacheState| -> bool {
            if let Some(expected_generation) = expected_generation {
                let current_generation =
                    tool_approval_cache_generation(cache, generation_key.as_str());
                if current_generation != expected_generation {
                    return false;
                }
            }
            cache.decisions.retain(|_, entry| match entry.expires_at_unix_ms {
                Some(entry_expires_at_unix_ms) => entry_expires_at_unix_ms > now_unix_ms,
                None => true,
            });
            // Capacity eviction removes an arbitrary entry (HashMap iteration
            // order), not LRU: this is a size bound, not a hit-rate strategy.
            if cache.decisions.len() >= APPROVAL_DECISION_CACHE_CAPACITY {
                if let Some(first_key) = cache.decisions.keys().next().cloned() {
                    cache.decisions.remove(first_key.as_str());
                }
            }
            cache.decisions.insert(cache_key.clone(), cache_entry.clone());
            true
        };
        match self.tool_approval_cache.lock() {
            Ok(mut cache) => remember_in_cache(&mut cache),
            Err(poisoned) => {
                warn!("tool approval cache lock poisoned while recording decision");
                let mut cache = poisoned.into_inner();
                remember_in_cache(&mut cache)
            }
        }
    }

    // Memory items, memory search (cached and diagnostic variants), and
    // workspace documents.

    /// Validates content limits, applies the default TTL when none is set,
    /// persists the item, and invalidates the memory search cache.
    ///
    /// # Errors
    /// `invalid_argument` when content exceeds the configured byte/token
    /// limits (the rejection counter is bumped), otherwise the mapped memory
    /// store error or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn ingest_memory_item(
        self: &Arc<Self>,
        mut request: MemoryItemCreateRequest,
    ) -> Result<MemoryItemRecord, Status> {
        let config = self.memory_config_snapshot();
        if let Err(status) =
            validate_memory_item_content_limits(request.content_text.as_str(), &config)
        {
            self.counters.memory_items_rejected.fetch_add(1, Ordering::Relaxed);
            return Err(status);
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

    /// Loads a memory item by id.
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker panicked.
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

    /// Applies a lifecycle update, revalidating content limits when the text
    /// changes, and invalidates the search cache when something was updated.
    ///
    /// # Errors
    /// `invalid_argument` for content over limits, otherwise the mapped
    /// memory store error or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn update_memory_item_lifecycle(
        self: &Arc<Self>,
        request: MemoryItemLifecycleUpdateRequest,
    ) -> Result<Option<MemoryItemRecord>, Status> {
        if let Some(content_text) = request.content_text.as_deref() {
            let config = self.memory_config_snapshot();
            if let Err(status) = validate_memory_item_content_limits(content_text, &config) {
                self.counters.memory_items_rejected.fetch_add(1, Ordering::Relaxed);
                return Err(status);
            }
        }
        let state = Arc::clone(self);
        let updated = tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .update_memory_item_lifecycle(&request)
                .map_err(|error| map_memory_store_error("update memory item lifecycle", error))
        })
        .await
        .map_err(|_| Status::internal("memory lifecycle update worker panicked"))??;
        if updated.is_some() {
            self.clear_memory_search_cache();
        }
        Ok(updated)
    }

    /// Deletes a memory item scoped to principal/channel; invalidates the search cache when
    /// something was removed.
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker panicked.
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

    /// Lists memory items with cursor pagination and tag/source filters.
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker panicked.
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

    /// Bulk-deletes memory matching the request and returns the count; invalidates the search
    /// cache when anything was removed.
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker panicked.
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

    /// Loads the memory maintenance bookkeeping (last/next runs).
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker panicked.
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

    /// Loads embedding coverage statistics for memory items.
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker panicked.
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

    /// Runs retention/vacuum maintenance; invalidates the search cache when anything was deleted.
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker panicked.
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

    /// Backfills missing memory embeddings in batches; invalidates the search cache when rows
    /// changed.
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker panicked.
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

    /// Searches memory and returns hits plus full retrieval diagnostics.
    /// Never served from (nor written to) the search cache; logs when the
    /// latency budget is exceeded.
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker
    /// panicked.
    #[allow(clippy::result_large_err)]
    pub async fn search_memory_with_diagnostics(
        self: &Arc<Self>,
        request: MemorySearchRequest,
    ) -> Result<MemorySearchOutcome, Status> {
        self.counters.memory_search_requests.fetch_add(1, Ordering::Relaxed);
        let total_started = Instant::now();
        let state = Arc::clone(self);
        let outcome = tokio::task::spawn_blocking(move || {
            let retrieval_config = state.retrieval_config_snapshot();
            let candidate_outcome = state
                .retrieval_backend
                .search_memory_candidate_outcome(&state.journal_store, &request, &retrieval_config)
                .map_err(|error| map_memory_store_error("search memory items", error))?;
            let fusion_started = Instant::now();
            let hits = score_memory_candidates(
                candidate_outcome.candidates,
                request.min_score,
                &retrieval_config,
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

    /// Cached memory search: a hit is served until the earliest TTL among its
    /// items lapses; misses run the backend search and repopulate the cache.
    /// Concurrent misses for the same key may compute twice (last write
    /// wins), which is acceptable for a read-through cache.
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker
    /// panicked.
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
            let retrieval_config = state.retrieval_config_snapshot();
            let candidates = state
                .retrieval_backend
                .search_memory_candidates(&state.journal_store, &request, &retrieval_config)
                .map_err(|error| map_memory_store_error("search memory items", error))?;
            Ok::<_, Status>(score_memory_candidates(
                candidates,
                request.min_score,
                &retrieval_config,
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

        // Capacity eviction removes an arbitrary entry (HashMap iteration
        // order), not LRU: this is a size bound, not a hit-rate strategy.
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

    /// Loads a workspace document by path within the principal/channel/agent scope.
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker panicked.
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

    /// Loads a workspace document by id within the principal/channel/agent scope.
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn workspace_document_by_id(
        self: &Arc<Self>,
        principal: String,
        channel: Option<String>,
        agent_id: Option<String>,
        document_id: String,
        include_deleted: bool,
    ) -> Result<Option<WorkspaceDocumentRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .workspace_document_by_id(
                    principal.as_str(),
                    channel.as_deref(),
                    agent_id.as_deref(),
                    document_id.as_str(),
                    include_deleted,
                )
                .map_err(|error| map_memory_store_error("load workspace document", error))
        })
        .await
        .map_err(|_| Status::internal("workspace document worker panicked"))?
    }

    /// Lists workspace documents matching the filter.
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker panicked.
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

    /// Creates or updates a workspace document (new version on change).
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker panicked.
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

    /// Moves/renames a workspace document.
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker panicked.
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

    /// Soft-deletes a workspace document (history is retained).
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker panicked.
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

    /// Lists the stored versions of a workspace document.
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker panicked.
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

    /// Sets or clears the pinned flag on a workspace document by path.
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker panicked.
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

    /// Records that a workspace document was recalled into context.
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker panicked.
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

    /// Seeds the default workspace documents for a principal.
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker panicked.
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

    /// Searches workspace documents. When the retrieval index returns no
    /// hits, falls back to lexical scoring over journal documents so content
    /// that has not been indexed yet stays findable.
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker
    /// panicked.
    #[allow(clippy::result_large_err)]
    pub async fn search_workspace_documents_with_diagnostics(
        self: &Arc<Self>,
        request: WorkspaceSearchRequest,
    ) -> Result<WorkspaceSearchOutcome, Status> {
        let total_started = Instant::now();
        let state = Arc::clone(self);
        let outcome = tokio::task::spawn_blocking(move || {
            let retrieval_config = state.retrieval_config_snapshot();
            let candidate_outcome = state
                .retrieval_backend
                .search_workspace_candidate_outcome(
                    &state.journal_store,
                    &request,
                    &retrieval_config,
                )
                .map_err(|error| map_memory_store_error("search workspace documents", error))?;
            let fusion_started = Instant::now();
            let mut hits = score_workspace_candidates(
                candidate_outcome.candidates,
                request.min_score,
                &retrieval_config,
            );
            if hits.is_empty() {
                let documents = state
                    .journal_store
                    .list_workspace_documents(&WorkspaceDocumentListFilter {
                        principal: request.principal.clone(),
                        channel: request.channel.clone(),
                        agent_id: request.agent_id.clone(),
                        prefix: request.prefix.clone(),
                        include_deleted: false,
                        limit: request.top_k.clamp(1, MAX_MEMORY_SEARCH_TOP_K),
                    })
                    .map_err(|error| {
                        map_memory_store_error("fallback search workspace documents", error)
                    })?;
                hits = fallback_workspace_document_search_hits(
                    documents,
                    &request,
                    &retrieval_config,
                    current_unix_ms(),
                );
            }
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

    /// Hits-only variant of
    /// [`Self::search_workspace_documents_with_diagnostics`].
    ///
    /// # Errors
    /// Same as the diagnostics variant.
    #[allow(clippy::result_large_err)]
    pub async fn search_workspace_documents(
        self: &Arc<Self>,
        request: WorkspaceSearchRequest,
    ) -> Result<Vec<WorkspaceSearchHit>, Status> {
        Ok(self.search_workspace_documents_with_diagnostics(request).await?.hits)
    }

    /// Counts a cron trigger firing.
    pub fn record_cron_trigger_fired(&self) {
        self.counters.cron_triggers_fired.fetch_add(1, Ordering::Relaxed);
    }
}

/// Lexical-only fallback scoring over journal workspace documents, used when
/// the retrieval backend returned no hits (for example, content not yet
/// indexed).
fn fallback_workspace_document_search_hits(
    documents: Vec<WorkspaceDocumentRecord>,
    request: &WorkspaceSearchRequest,
    retrieval_config: &RetrievalRuntimeConfig,
    now_unix_ms: i64,
) -> Vec<WorkspaceSearchHit> {
    let profile =
        retrieval_config.scoring.profile_for(RetrievalSourceProfileKind::WorkspaceDocument);
    let query_variants = vec![request.query.clone()];
    let mut hits = documents
        .into_iter()
        .filter(|document| request.include_quarantined || document.risk_state != "quarantined")
        .filter_map(|document| {
            let searchable_text =
                format!("{}\n{}\n{}", document.title, document.path, document.content_text);
            let lexical_score = lexical_overlap_score(
                searchable_text.as_str(),
                query_variants.as_slice(),
                retrieval_config.scoring.phrase_match_bonus_bps,
            );
            if lexical_score <= 0.0 {
                return None;
            }
            let recency = retrieval_recency_score(
                document.updated_at_unix_ms,
                now_unix_ms,
                profile.min_recency_bps,
            );
            let source_quality = workspace_source_quality(
                document.pinned,
                document.manual_override,
                document.prompt_binding.as_str(),
                document.risk_state.as_str(),
                profile,
            );
            let breakdown = score_with_profile(
                lexical_score,
                0.0,
                recency,
                source_quality,
                document.pinned,
                profile,
            );
            if breakdown.final_score < request.min_score {
                return None;
            }
            let snippet = fallback_workspace_document_snippet(
                document.content_text.as_str(),
                request.query.as_str(),
            );
            Some(WorkspaceSearchHit {
                version: document.latest_version,
                chunk_index: 0,
                chunk_count: 1,
                score: breakdown.final_score,
                reason: format!(
                    "journal_document_fallback(lexical={:.2},recency={:.2},quality={:.2})",
                    breakdown.lexical_score,
                    breakdown.recency_score,
                    breakdown.source_quality_score,
                ),
                breakdown: WorkspaceScoreBreakdown {
                    lexical_score: breakdown.lexical_score,
                    vector_score: 0.0,
                    recency_score: breakdown.recency_score,
                    source_quality_score: breakdown.source_quality_score,
                    final_score: breakdown.final_score,
                },
                snippet,
                document,
            })
        })
        .collect::<Vec<_>>();
    hits.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| right.document.updated_at_unix_ms.cmp(&left.document.updated_at_unix_ms))
            .then_with(|| left.document.document_id.cmp(&right.document.document_id))
    });
    hits.truncate(request.top_k.clamp(1, MAX_MEMORY_SEARCH_TOP_K));
    hits
}

/// Extracts a query-anchored snippet: finds the earliest token match, then
/// backs the start up by a quarter of the snippet length (on a char boundary)
/// so the match appears with leading context rather than at position zero.
fn fallback_workspace_document_snippet(content: &str, query: &str) -> String {
    const SNIPPET_CHARS: usize = 512;
    let query_tokens = query
        .split(|ch: char| !ch.is_alphanumeric() && ch != '_' && ch != '-')
        .map(str::trim)
        .filter(|token| token.chars().count() >= 3)
        .map(str::to_ascii_lowercase)
        .collect::<Vec<_>>();
    let content_lower = content.to_ascii_lowercase();
    let match_index = query_tokens
        .iter()
        .filter_map(|token| content_lower.find(token.as_str()))
        .min()
        .unwrap_or(0);
    let start = content[..match_index.min(content.len())]
        .char_indices()
        .rev()
        .nth(SNIPPET_CHARS / 4)
        .map(|(index, _)| index)
        .unwrap_or(0);
    content[start..].chars().take(SNIPPET_CHARS).collect::<String>()
}

/// Enforces the configured memory content byte and whitespace-token limits.
///
/// # Errors
/// `invalid_argument` naming the limit that was exceeded.
fn validate_memory_item_content_limits(
    content_text: &str,
    config: &MemoryRuntimeConfig,
) -> Result<(), Status> {
    let payload_bytes = content_text.len();
    if payload_bytes > config.max_item_bytes {
        return Err(Status::invalid_argument(format!(
            "memory content exceeds byte limit ({payload_bytes} > {})",
            config.max_item_bytes
        )));
    }
    let token_count = content_text.split_whitespace().count();
    if token_count > config.max_item_tokens {
        return Err(Status::invalid_argument(format!(
            "memory content exceeds token limit ({token_count} > {})",
            config.max_item_tokens
        )));
    }
    Ok(())
}

/// Picks the first non-blank model profile in priority order: registry
/// default chat model, active model, OpenAI model, Anthropic model.
fn select_default_agent_model_profile(
    registry_default_chat_model_id: Option<&str>,
    active_model_id: Option<&str>,
    openai_model: Option<&str>,
    anthropic_model: Option<&str>,
) -> Option<String> {
    [registry_default_chat_model_id, active_model_id, openai_model, anthropic_model]
        .into_iter()
        .find_map(|value| value.and_then(normalize_optional_agent_model_profile))
}

fn normalize_optional_agent_model_profile(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        fallback_workspace_document_search_hits, provider_credential_attribution_for_provider,
        provider_lease_timeout_status, select_default_agent_model_profile, sign_canvas_hmac_sha256,
        validate_memory_item_content_limits, BrowserServiceRuntimeConfig, CanvasHostRuntimeConfig,
        GatewayJournalConfigSnapshot, GatewayRuntimeConfigSnapshot, GatewayRuntimeState,
        HttpFetchRuntimeConfig, MemoryRuntimeConfig,
    };
    use crate::agents::AgentRegistry;
    use crate::gateway::RUN_PARAMETER_DELTA_CACHE_CAPACITY;
    use crate::journal::{
        CanvasStatePatchRecord, JournalConfig, JournalStore, WorkspaceDocumentRecord,
        WorkspaceSearchRequest,
    };
    use crate::media::MediaRuntimeConfig;
    use crate::model_provider::{
        AudioTranscriptionRequest, AudioTranscriptionResponse, ModelProvider,
        ProviderCapabilitiesSnapshot, ProviderCircuitBreakerSnapshot, ProviderDiscoverySnapshot,
        ProviderError, ProviderHealthProbeSnapshot, ProviderRegistryModelSnapshot,
        ProviderRegistryProviderSnapshot, ProviderRegistrySnapshot, ProviderRequest,
        ProviderResponse, ProviderResponseCacheSnapshot, ProviderRetryPolicySnapshot,
        ProviderRouteSelectionTrace, ProviderRuntimeMetricsSnapshot, ProviderStatusSnapshot,
    };
    use crate::provider_leases::{
        LeasePreviewState, LeasePriority, ProviderLeaseExecutionContext,
        ProviderLeasePreviewSnapshot,
    };
    use crate::retrieval::RetrievalRuntimeConfig;
    use palyra_model_providers::classify_http_provider_failure;
    use std::{future::Future, pin::Pin, sync::Arc};
    use tonic::Code;

    #[test]
    fn provider_lease_timeout_status_surfaces_backpressure_without_internal_reason_codes() {
        let status = provider_lease_timeout_status(
            763,
            &ProviderLeaseExecutionContext {
                provider_id: "openai".to_owned(),
                credential_id: "cred-a".to_owned(),
                priority: LeasePriority::Foreground,
                task_label: "primary_interactive".to_owned(),
                max_wait_ms: 30_000,
                session_id: Some("session-1".to_owned()),
                run_id: Some("run-1".to_owned()),
            },
            ProviderLeasePreviewSnapshot {
                state: LeasePreviewState::Waiting,
                priority: LeasePriority::Foreground,
                estimated_wait_ms: Some(25),
                retry_after_ms: None,
                active_provider_leases: 2,
                active_credential_leases: 2,
                foreground_waiters: 1,
                background_waiters: 0,
                credential_state: None,
                reason: Some("shared_capacity_exhausted".to_owned()),
                queue_position: Some(2),
                wait_reason: Some("shared_capacity_exhausted".to_owned()),
                priority_class: "foreground".to_owned(),
                selected_provider_candidate: Some("openai:cred-a".to_owned()),
                timeout_ms: Some(30_000),
            },
        );

        assert_eq!(status.code(), Code::ResourceExhausted);
        assert!(
            status.message().contains("model provider capacity is busy"),
            "lease timeout should be reported as explicit backpressure"
        );
        assert!(
            status.message().contains("queued for 763 ms"),
            "lease timeout should include the observed queue wait"
        );
        assert!(
            !status.message().contains("shared_capacity_exhausted"),
            "internal lease reason codes should not leak into user-facing errors"
        );
    }

    #[test]
    fn run_parameter_delta_cache_updates_and_evicts_oldest() {
        let mut cache = super::RunParameterDeltaCache::default();

        cache.insert(" run-1 ", r#"{"cli_context":{"launch_cwd":"/tmp/one"}}"#);
        cache.insert("run-1", r#"{"cli_context":{"launch_cwd":"/tmp/two"}}"#);

        assert_eq!(
            cache.get("run-1").as_deref(),
            Some(r#"{"cli_context":{"launch_cwd":"/tmp/two"}}"#)
        );

        for index in 2..=(RUN_PARAMETER_DELTA_CACHE_CAPACITY + 1) {
            cache.insert(format!("run-{index}").as_str(), "{}");
        }

        assert_eq!(cache.get("run-1"), None);
        assert_eq!(cache.entries.len(), RUN_PARAMETER_DELTA_CACHE_CAPACITY);
        let newest_run_id = format!("run-{}", RUN_PARAMETER_DELTA_CACHE_CAPACITY + 1);
        assert_eq!(cache.get(newest_run_id.as_str()).as_deref(), Some("{}"));
    }

    #[test]
    fn canvas_patch_history_response_budget_keeps_recent_records() {
        let records = (1..=5)
            .map(|state_version| test_canvas_patch_record(state_version, 128))
            .collect::<Vec<_>>();

        let limited = GatewayRuntimeState::limit_canvas_patch_history_response(records, 1_100);

        assert_eq!(
            limited.iter().map(|record| record.state_version).collect::<Vec<_>>(),
            vec![4, 5],
            "response budgeting should retain the newest contiguous revisions"
        );
    }

    #[test]
    fn canvas_hmac_signature_frames_parts_unambiguously() {
        let secret = [7_u8; 32];

        let left = sign_canvas_hmac_sha256(&secret, "canvas_bundle.v1", &[b"ab", b"c"]);
        let right = sign_canvas_hmac_sha256(&secret, "canvas_bundle.v1", &[b"a", b"bc"]);

        assert_ne!(left, right);
    }

    #[test]
    fn default_agent_model_profile_prefers_registry_default_model() {
        let selected = select_default_agent_model_profile(
            Some("MiniMax-M2.7"),
            Some("gpt-4o-mini"),
            Some("gpt-4o-mini"),
            Some("claude-3-5-sonnet-latest"),
        );

        assert_eq!(selected.as_deref(), Some("MiniMax-M2.7"));
    }

    fn test_canvas_patch_record(
        state_version: u64,
        payload_bytes: usize,
    ) -> CanvasStatePatchRecord {
        let payload = "x".repeat(payload_bytes);
        let sqlite_version = i64::try_from(state_version).expect("test version fits i64");
        CanvasStatePatchRecord {
            seq: sqlite_version,
            canvas_id: "canvas-test".to_owned(),
            state_version,
            base_state_version: state_version.saturating_sub(1),
            state_schema_version: 1,
            patch_json: payload.clone(),
            resulting_state_json: payload,
            closed: false,
            close_reason: None,
            actor_principal: "admin:local".to_owned(),
            actor_device_id: "device:test".to_owned(),
            applied_at_unix_ms: sqlite_version,
        }
    }

    #[test]
    fn provider_credential_attribution_uses_actual_failover_provider() {
        let lease_context =
            provider_lease_context("openai-primary", "auth-profile:openai-primary:primary-profile");
        let snapshot = provider_status_snapshot(true);

        let attribution = provider_credential_attribution_for_provider(
            &snapshot,
            &lease_context,
            "anthropic-primary",
        )
        .expect("fallback provider should resolve to a credential attribution");

        assert_eq!(attribution.provider_id, "anthropic-primary");
        assert_eq!(attribution.credential_id, "auth-profile:anthropic-primary:fallback-profile");
        assert_eq!(attribution.auth_profile_id.as_deref(), Some("fallback-profile"));
    }

    #[tokio::test]
    async fn failover_provider_errors_still_record_credential_feedback() {
        let state = test_runtime_state();
        let _ = state.configure_model_provider(Arc::new(RateLimitedFailoverModelProvider));
        let lease_context =
            provider_lease_context("openai-primary", "auth-profile:openai-primary:primary-profile");

        let result = state
            .execute_model_provider_with_lease(
                ProviderRequest::from_input_text(
                    "exercise rate-limit feedback".to_owned(),
                    false,
                    Vec::new(),
                    None,
                ),
                lease_context,
            )
            .await;

        assert!(result.is_err(), "fake provider should return the configured rate-limit error");
        let snapshot = state.provider_lease_snapshot();
        let feedback = snapshot
            .credential_feedback
            .iter()
            .find(|entry| entry.credential_id == "auth-profile:openai-primary:primary-profile")
            .expect("provider error should record credential feedback even when failover exists");
        assert_eq!(feedback.provider_id, "openai-primary");
        assert_eq!(feedback.state, "rate_limited");
        assert!(
            feedback.reason.contains("category=rate_limit"),
            "feedback should preserve the provider recovery category"
        );
    }

    #[test]
    fn memory_content_limit_validation_rejects_oversized_updates() {
        let byte_config =
            MemoryRuntimeConfig { max_item_bytes: 12, max_item_tokens: 32, ..Default::default() };

        let byte_error = validate_memory_item_content_limits("0123456789abc", &byte_config)
            .expect_err("content above byte limit should be rejected");
        assert_eq!(byte_error.code(), Code::InvalidArgument);
        assert!(
            byte_error.message().contains("exceeds byte limit"),
            "unexpected byte-limit error: {byte_error}"
        );

        let token_config =
            MemoryRuntimeConfig { max_item_bytes: 128, max_item_tokens: 3, ..Default::default() };
        let token_error = validate_memory_item_content_limits("one two three four", &token_config)
            .expect_err("content above token limit should be rejected");
        assert_eq!(token_error.code(), Code::InvalidArgument);
        assert!(
            token_error.message().contains("exceeds token limit"),
            "unexpected token-limit error: {token_error}"
        );
    }

    #[test]
    fn fallback_workspace_document_search_finds_unindexed_active_documents() {
        let document = WorkspaceDocumentRecord {
            document_id: "workspace-doc-1".to_owned(),
            principal: "principal-1".to_owned(),
            channel: None,
            agent_id: None,
            latest_session_id: Some("session-1".to_owned()),
            path: "projects/S033/MEMORY.md".to_owned(),
            parent_path: Some("projects/S033".to_owned()),
            title: "Project Memory".to_owned(),
            kind: "memory".to_owned(),
            document_class: "workspace_memory".to_owned(),
            state: "active".to_owned(),
            prompt_binding: "system_candidate".to_owned(),
            risk_state: "clean".to_owned(),
            risk_reasons: Vec::new(),
            pinned: false,
            manual_override: false,
            template_id: None,
            template_version: None,
            source_memory_id: None,
            latest_version: 1,
            content_text: "- remembered_at_unix_ms=1780430000000 source=manual\n  S033-PREF-20260602 prefers TypeScript, Vitest, and short concise reports.".to_owned(),
            content_hash: "hash-1".to_owned(),
            created_at_unix_ms: 1_780_430_000_000,
            updated_at_unix_ms: 1_780_430_000_000,
            deleted_at_unix_ms: None,
            last_recalled_at_unix_ms: None,
        };
        let request = WorkspaceSearchRequest {
            principal: "principal-1".to_owned(),
            channel: None,
            agent_id: None,
            query: "S033-PREF-20260602 Vitest".to_owned(),
            prefix: Some("projects/S033".to_owned()),
            top_k: 4,
            min_score: 0.0,
            include_historical: false,
            include_quarantined: false,
        };

        let hits = fallback_workspace_document_search_hits(
            vec![document],
            &request,
            &RetrievalRuntimeConfig::default(),
            1_780_430_000_000,
        );

        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].document.path, "projects/S033/MEMORY.md");
        assert!(hits[0].snippet.contains("S033-PREF-20260602"));
        assert!(hits[0].reason.contains("journal_document_fallback"));
    }

    #[test]
    fn default_agent_model_profile_uses_active_model_when_registry_default_is_blank() {
        let selected = select_default_agent_model_profile(
            Some("   "),
            Some("MiniMax-M2.7"),
            Some("gpt-4o-mini"),
            Some("claude-3-5-sonnet-latest"),
        );

        assert_eq!(selected.as_deref(), Some("MiniMax-M2.7"));
    }

    fn provider_lease_context(
        provider_id: &str,
        credential_id: &str,
    ) -> ProviderLeaseExecutionContext {
        ProviderLeaseExecutionContext {
            provider_id: provider_id.to_owned(),
            credential_id: credential_id.to_owned(),
            priority: LeasePriority::Foreground,
            task_label: "primary_interactive".to_owned(),
            max_wait_ms: 30_000,
            session_id: Some("session-1".to_owned()),
            run_id: Some("run-1".to_owned()),
        }
    }

    struct RateLimitedFailoverModelProvider;

    impl ModelProvider for RateLimitedFailoverModelProvider {
        fn complete<'a>(
            &'a self,
            _request: ProviderRequest,
        ) -> Pin<Box<dyn Future<Output = Result<ProviderResponse, ProviderError>> + Send + 'a>>
        {
            Box::pin(async { Err(rate_limit_provider_error()) })
        }

        fn transcribe_audio<'a>(
            &'a self,
            _request: AudioTranscriptionRequest,
        ) -> Pin<
            Box<dyn Future<Output = Result<AudioTranscriptionResponse, ProviderError>> + Send + 'a>,
        > {
            Box::pin(async { Err(ProviderError::MissingApiKey) })
        }

        fn status_snapshot(&self) -> ProviderStatusSnapshot {
            provider_status_snapshot(true)
        }
    }

    fn rate_limit_provider_error() -> ProviderError {
        ProviderError::RequestFailed {
            message: "rate limit exceeded".to_owned(),
            retryable: true,
            retry_count: 0,
            classification: classify_http_provider_failure(
                429,
                true,
                "openai_chat_http",
                "rate limit exceeded",
            ),
        }
    }

    fn test_runtime_state() -> Arc<GatewayRuntimeState> {
        let db_path =
            unique_runtime_test_root("palyra-runtime-feedback-journal").join("events.sqlite3");
        let state_root = unique_runtime_test_root("palyra-runtime-feedback-state");
        let agent_registry = AgentRegistry::open_for_test_state_root(state_root.as_path())
            .expect("test agent registry should initialize");
        let journal_store = JournalStore::open(JournalConfig {
            db_path: db_path.clone(),
            hash_chain_enabled: false,
            max_payload_bytes: 256 * 1024,
            max_events: 10_000,
        })
        .expect("test journal store should initialize");

        GatewayRuntimeState::new(
            test_runtime_config(),
            GatewayJournalConfigSnapshot { db_path, hash_chain_enabled: false },
            journal_store,
            0,
            agent_registry,
        )
        .expect("test runtime state should initialize")
    }

    fn unique_runtime_test_root(prefix: &str) -> std::path::PathBuf {
        let root = std::env::temp_dir().join(format!(
            "{prefix}-{}-{}",
            std::process::id(),
            ulid::Ulid::new()
        ));
        std::fs::create_dir_all(root.as_path()).expect("test runtime temp root should exist");
        root
    }

    fn test_runtime_config() -> GatewayRuntimeConfigSnapshot {
        let model_provider_request_timeout_ms =
            crate::model_provider::ModelProviderConfig::default().request_timeout_ms;

        GatewayRuntimeConfigSnapshot {
            grpc_bind_addr: "127.0.0.1".to_owned(),
            grpc_port: 7443,
            quic_bind_addr: "127.0.0.1".to_owned(),
            quic_port: 7444,
            quic_enabled: true,
            orchestrator_runloop_v1_enabled: true,
            model_provider_request_timeout_ms,
            node_rpc_mtls_required: true,
            admin_auth_required: true,
            vault_get_approval_required_refs: vec!["global/openai_api_key".to_owned()],
            max_tape_entries_per_response: 1_000,
            max_tape_bytes_per_response: 2 * 1024 * 1024,
            feature_rollouts: crate::config::FeatureRolloutsConfig::default(),
            session_queue_policy: crate::config::SessionQueuePolicyConfig::default(),
            pruning_policy_matrix: crate::config::PruningPolicyMatrixConfig::default(),
            retrieval_dual_path: crate::config::RetrievalDualPathConfig::default(),
            auxiliary_executor: crate::config::AuxiliaryExecutorConfig::default(),
            flow_orchestration: crate::config::FlowOrchestrationConfig::default(),
            delivery_arbitration: crate::config::DeliveryArbitrationConfig::default(),
            replay_capture: crate::config::ReplayCaptureConfig::default(),
            networked_workers: crate::config::NetworkedWorkersConfig::default(),
            channel_router: crate::channel_router::ChannelRouterConfig::default(),
            media: MediaRuntimeConfig::default(),
            code_intel: crate::config::CodeIntelConfig::default(),
            tool_catalog_policy:
                crate::application::tool_registry::ToolCatalogPolicySnapshot::direct_from_allowed_tools(
                    &["palyra.echo".to_owned()],
                ),
            tool_call: test_tool_call_config(),
            http_fetch: HttpFetchRuntimeConfig {
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
                    "content-type".to_owned(),
                    "if-none-match".to_owned(),
                    "if-modified-since".to_owned(),
                    "user-agent".to_owned(),
                    "x-client-version".to_owned(),
                ],
                allowed_credential_vault_refs: Vec::new(),
                cache_enabled: true,
                cache_ttl_ms: 30_000,
                max_cache_entries: 256,
            },
            browser_service: BrowserServiceRuntimeConfig {
                enabled: false,
                endpoint: "http://127.0.0.1:7543".to_owned(),
                auth_token: None,
                connect_timeout_ms: 1_500,
                request_timeout_ms: 15_000,
                max_screenshot_bytes: 256 * 1024,
                max_title_bytes: 4 * 1024,
            },
            canvas_host: CanvasHostRuntimeConfig {
                enabled: true,
                public_base_url: "http://127.0.0.1:7142".to_owned(),
                token_ttl_ms: 15 * 60 * 1_000,
                max_state_bytes: 64 * 1024,
                max_bundle_bytes: 512 * 1024,
                max_assets_per_bundle: 32,
                max_updates_per_minute: 120,
            },
            smart_routing: crate::usage_governance::SmartRoutingRuntimeConfig {
                enabled: true,
                default_mode: "suggest".to_owned(),
                auxiliary_routing_enabled: true,
            },
        }
    }

    fn test_tool_call_config() -> crate::tool_protocol::ToolCallConfig {
        crate::tool_protocol::ToolCallConfig {
            allowed_tools: vec!["palyra.echo".to_owned()],
            max_calls_per_run: 4,
            execution_timeout_ms: 250,
            process_runner: crate::sandbox_runner::SandboxProcessRunnerPolicy {
                enabled: false,
                tier: crate::sandbox_runner::SandboxProcessRunnerTier::B,
                workspace_root: std::path::PathBuf::from("."),
                path_access_mode: crate::sandbox_runner::PathAccessMode::WorkspaceOnly,
                allowed_executables: Vec::new(),
                allow_interpreters: false,
                egress_enforcement_mode: crate::sandbox_runner::EgressEnforcementMode::Strict,
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
        }
    }

    fn provider_status_snapshot(failover_enabled: bool) -> ProviderStatusSnapshot {
        ProviderStatusSnapshot {
            kind: "registry".to_owned(),
            provider_id: "openai-primary".to_owned(),
            credential_id: "auth-profile:openai-primary:primary-profile".to_owned(),
            model_id: Some("gpt-4o-mini".to_owned()),
            capabilities: provider_capabilities(),
            openai_base_url: None,
            anthropic_base_url: None,
            openai_model: Some("gpt-4o-mini".to_owned()),
            anthropic_model: None,
            openai_embeddings_model: None,
            openai_embeddings_dims: None,
            auth_profile_id: Some("primary-profile".to_owned()),
            auth_profile_provider_kind: Some("openai".to_owned()),
            credential_source: Some("auth_profile_api_key".to_owned()),
            api_key_configured: true,
            retry_policy: retry_policy(),
            circuit_breaker: circuit_breaker(),
            runtime_metrics: runtime_metrics(),
            response_cache: ProviderResponseCacheSnapshot {
                enabled: false,
                entry_count: 0,
                hit_count: 0,
                miss_count: 0,
            },
            health: health("ok"),
            discovery: discovery(),
            registry: ProviderRegistrySnapshot {
                default_chat_model_id: Some("gpt-4o-mini".to_owned()),
                default_embeddings_model_id: None,
                default_audio_transcription_model_id: None,
                failover_enabled,
                response_cache_enabled: false,
                providers: vec![
                    provider_snapshot(
                        "openai-primary",
                        "auth-profile:openai-primary:primary-profile",
                        "primary-profile",
                        "openai",
                    ),
                    provider_snapshot(
                        "anthropic-primary",
                        "auth-profile:anthropic-primary:fallback-profile",
                        "fallback-profile",
                        "anthropic",
                    ),
                ],
                credentials: Vec::new(),
                models: vec![
                    model_snapshot("gpt-4o-mini", "openai-primary"),
                    model_snapshot("claude-3-5-sonnet-latest", "anthropic-primary"),
                ],
            },
            route_selection: ProviderRouteSelectionTrace::empty(),
        }
    }

    fn provider_snapshot(
        provider_id: &str,
        credential_id: &str,
        auth_profile_id: &str,
        kind: &str,
    ) -> ProviderRegistryProviderSnapshot {
        ProviderRegistryProviderSnapshot {
            provider_id: provider_id.to_owned(),
            credential_id: credential_id.to_owned(),
            display_name: provider_id.to_owned(),
            kind: kind.to_owned(),
            enabled: true,
            endpoint_base_url: None,
            auth_profile_id: Some(auth_profile_id.to_owned()),
            auth_profile_provider_kind: Some(kind.to_owned()),
            credential_source: Some("auth_profile_api_key".to_owned()),
            api_key_configured: true,
            retry_policy: retry_policy(),
            circuit_breaker: circuit_breaker(),
            runtime_metrics: runtime_metrics(),
            health: health("ok"),
            discovery: discovery(),
        }
    }

    fn model_snapshot(model_id: &str, provider_id: &str) -> ProviderRegistryModelSnapshot {
        ProviderRegistryModelSnapshot {
            model_id: model_id.to_owned(),
            provider_id: provider_id.to_owned(),
            role: "chat".to_owned(),
            enabled: true,
            capabilities: provider_capabilities(),
        }
    }

    fn provider_capabilities() -> ProviderCapabilitiesSnapshot {
        ProviderCapabilitiesSnapshot {
            streaming_tokens: true,
            tool_calls: true,
            json_mode: true,
            vision: false,
            audio_transcribe: false,
            embeddings: false,
            reasoning: false,
            reasoning_efforts: Vec::new(),
            service_tier: false,
            service_tiers: Vec::new(),
            max_context_tokens: Some(128_000),
            cost_tier: "standard".to_owned(),
            latency_tier: "standard".to_owned(),
            recommended_use_cases: Vec::new(),
            known_limitations: Vec::new(),
            operator_override: false,
            metadata_source: "static".to_owned(),
        }
    }

    fn retry_policy() -> ProviderRetryPolicySnapshot {
        ProviderRetryPolicySnapshot { max_retries: 0, retry_backoff_ms: 0 }
    }

    fn circuit_breaker() -> ProviderCircuitBreakerSnapshot {
        ProviderCircuitBreakerSnapshot {
            failure_threshold: 1,
            cooldown_ms: 60_000,
            consecutive_failures: 0,
            open: false,
        }
    }

    fn runtime_metrics() -> ProviderRuntimeMetricsSnapshot {
        ProviderRuntimeMetricsSnapshot {
            request_count: 0,
            error_count: 0,
            error_rate_bps: 0,
            total_retry_attempts: 0,
            total_prompt_tokens: 0,
            total_completion_tokens: 0,
            avg_prompt_tokens_per_run: 0,
            avg_completion_tokens_per_run: 0,
            last_latency_ms: 0,
            avg_latency_ms: 0,
            max_latency_ms: 0,
            last_used_at_unix_ms: None,
            last_success_at_unix_ms: None,
            last_error_at_unix_ms: None,
            last_error: None,
        }
    }

    fn health(state: &str) -> ProviderHealthProbeSnapshot {
        ProviderHealthProbeSnapshot {
            state: state.to_owned(),
            message: "test".to_owned(),
            checked_at_unix_ms: None,
            latency_ms: None,
            source: "test".to_owned(),
        }
    }

    fn discovery() -> ProviderDiscoverySnapshot {
        ProviderDiscoverySnapshot {
            status: "unknown".to_owned(),
            checked_at_unix_ms: None,
            expires_at_unix_ms: None,
            discovered_model_ids: Vec::new(),
            source: "test".to_owned(),
            message: None,
        }
    }
}
