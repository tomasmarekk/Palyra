//! Console usage, cost, and operator-insights reporting handlers.
//!
//! Serves the `/console/v1/usage/*` surface: `summary`, `sessions`,
//! `sessions/{session_id}`, `agents`, `models`, `insights`, and `export`
//! reads, plus the `pricing`, `budgets`, and per-policy `override-request`
//! writes. Aggregates journal usage records with
//! `usage_governance` pricing/budget evaluation and samples run tapes, cron
//! history, and plugin bindings for the operator-insights snapshot. All
//! envelope shapes and error strings here are a wire contract consumed by
//! the web console (`apps/web`).

use std::{
    collections::{HashMap, HashSet},
    time::{SystemTime, UNIX_EPOCH},
};

use axum::http::{header::CONTENT_TYPE, HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::diagnostics::{
    build_config_ref_health_observability, build_page_info, build_provider_auth_observability,
    contract_descriptor,
};
use crate::agents::{AgentBindingQuery, AgentRecord, SessionAgentBinding};
use crate::journal::{self, OrchestratorUsageQuery};
use crate::plugins::{load_plugin_bindings_index, resolve_plugins_root};
use crate::usage_governance::{
    build_alert_candidates, build_model_mix, build_scope_mix, build_tool_mix,
    estimate_cost_for_model, evaluate_budget_policies, load_budget_override_approvals,
    request_usage_budget_override, PricingEstimate, UsageBudgetEvaluation, UsageModelMixRecord,
    UsageScopeMixRecord, UsageToolMixRecord,
};
use crate::*;

// Usage queries default to the trailing 30 days and are capped at 366 days
// (one leap year) because every endpoint scans the journal window on demand.
const DEFAULT_USAGE_LOOKBACK_MS: i64 = 30 * 24 * 60 * 60 * 1_000;
const MAX_USAGE_LOOKBACK_MS: i64 = 366 * 24 * 60 * 60 * 1_000;
const HOUR_BUCKET_MS: i64 = 60 * 60 * 1_000;
const DAY_BUCKET_MS: i64 = 24 * 60 * 60 * 1_000;
const DEFAULT_USAGE_BREAKDOWN_LIMIT: usize = 10;
const MAX_USAGE_BREAKDOWN_LIMIT: usize = 50;
const DEFAULT_USAGE_RUN_LIMIT: usize = 12;
const MAX_USAGE_RUN_LIMIT: usize = 25;
// Sampling caps for the operator-insights snapshot; the response reports the
// observed-vs-sampled counts so operators can tell when data was truncated.
const OPERATOR_INSIGHTS_RUN_SAMPLE_LIMIT: usize = 24;
const OPERATOR_INSIGHTS_TAPE_SAMPLE_LIMIT: usize = 128;
const OPERATOR_INSIGHTS_CRON_RUN_LIMIT: usize = 32;
const OPERATOR_INSIGHTS_PLUGIN_LIMIT: usize = 32;
const OPERATOR_INSIGHTS_QUERY_PREVIEW_BYTES: usize = 160;

/// Query string for the usage summary and insights endpoints: an optional
/// time window, bucket selector (`auto|hour|day`), and archived-session flag.
#[derive(Debug, Deserialize)]
pub(crate) struct ConsoleUsageSummaryQuery {
    #[serde(default)]
    start_at_unix_ms: Option<i64>,
    #[serde(default)]
    end_at_unix_ms: Option<i64>,
    #[serde(default)]
    bucket: Option<String>,
    #[serde(default)]
    include_archived: Option<bool>,
}

/// Query string for the paginated breakdown endpoints (sessions, agents,
/// models): the summary parameters plus a page `limit` and offset `cursor`.
#[derive(Debug, Deserialize)]
pub(crate) struct ConsoleUsageBreakdownQuery {
    #[serde(default)]
    start_at_unix_ms: Option<i64>,
    #[serde(default)]
    end_at_unix_ms: Option<i64>,
    #[serde(default)]
    bucket: Option<String>,
    #[serde(default)]
    include_archived: Option<bool>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    cursor: Option<String>,
}

/// Query string for the per-session detail endpoint; `run_limit` caps how
/// many recent runs are embedded in the response.
#[derive(Debug, Deserialize)]
pub(crate) struct ConsoleUsageSessionDetailQuery {
    #[serde(default)]
    start_at_unix_ms: Option<i64>,
    #[serde(default)]
    end_at_unix_ms: Option<i64>,
    #[serde(default)]
    bucket: Option<String>,
    #[serde(default)]
    include_archived: Option<bool>,
    #[serde(default)]
    run_limit: Option<usize>,
}

/// Query string for the usage export endpoint; `dataset` selects
/// timeline/sessions/agents/models and `format` selects json/csv.
#[derive(Debug, Deserialize)]
pub(crate) struct ConsoleUsageExportQuery {
    dataset: String,
    format: String,
    #[serde(default)]
    start_at_unix_ms: Option<i64>,
    #[serde(default)]
    end_at_unix_ms: Option<i64>,
    #[serde(default)]
    bucket: Option<String>,
    #[serde(default)]
    include_archived: Option<bool>,
}

/// Request body for upserting one model pricing record (per-million token
/// costs plus an effective time range).
#[derive(Debug, Deserialize)]
pub(crate) struct ConsoleUsagePricingUpsertRequest {
    pricing_id: String,
    provider_id: String,
    provider_kind: String,
    model_id: String,
    effective_from_unix_ms: i64,
    #[serde(default)]
    effective_to_unix_ms: Option<i64>,
    #[serde(default)]
    input_cost_per_million_usd: Option<f64>,
    #[serde(default)]
    output_cost_per_million_usd: Option<f64>,
    #[serde(default)]
    fixed_request_cost_usd: Option<f64>,
    source: String,
    precision: String,
    #[serde(default = "default_usage_currency")]
    currency: String,
}

/// Request body for upserting one usage budget policy (scope, metric,
/// interval, soft/hard limits, and the action taken on breach).
#[derive(Debug, Deserialize)]
pub(crate) struct ConsoleUsageBudgetPolicyUpsertRequest {
    policy_id: String,
    scope_kind: String,
    scope_id: String,
    metric_kind: String,
    interval_kind: String,
    #[serde(default)]
    soft_limit_value: Option<f64>,
    #[serde(default)]
    hard_limit_value: Option<f64>,
    action: String,
    #[serde(default)]
    routing_mode_override: Option<String>,
    #[serde(default = "default_usage_policy_enabled")]
    enabled: bool,
}

/// Request body for asking approval to override a budget policy.
#[derive(Debug, Deserialize)]
pub(crate) struct ConsoleUsageBudgetOverrideRequest {
    #[serde(default)]
    reason: Option<String>,
}

/// Resolved query parameters echoed back so clients see the effective
/// window/bucket after defaulting and clamping.
#[derive(Debug, Clone, Serialize)]
struct UsageQueryEcho {
    start_at_unix_ms: i64,
    end_at_unix_ms: i64,
    bucket: String,
    bucket_width_ms: i64,
    include_archived: bool,
}

#[derive(Debug, Clone, Serialize)]
struct UsagePaginationQueryEcho {
    start_at_unix_ms: i64,
    end_at_unix_ms: i64,
    bucket: String,
    bucket_width_ms: i64,
    include_archived: bool,
    limit: usize,
    cursor: usize,
}

#[derive(Debug, Clone, Serialize)]
struct UsageSessionDetailQueryEcho {
    start_at_unix_ms: i64,
    end_at_unix_ms: i64,
    bucket: String,
    bucket_width_ms: i64,
    include_archived: bool,
    run_limit: usize,
}

/// Response envelope for `GET /console/v1/usage/summary`.
#[derive(Debug, Serialize)]
pub(crate) struct UsageSummaryEnvelope {
    contract: control_plane::ContractDescriptor,
    query: UsageQueryEcho,
    totals: journal::OrchestratorUsageTotals,
    timeline: Vec<journal::OrchestratorUsageTimelineBucket>,
    cost_tracking_available: bool,
}

/// Response envelope for `GET /console/v1/usage/sessions`.
#[derive(Debug, Serialize)]
pub(crate) struct UsageSessionsEnvelope {
    contract: control_plane::ContractDescriptor,
    query: UsagePaginationQueryEcho,
    sessions: Vec<journal::OrchestratorUsageSessionRecord>,
    page: control_plane::PageInfo,
    cost_tracking_available: bool,
}

/// Response envelope for `GET /console/v1/usage/sessions/{session_id}`.
#[derive(Debug, Serialize)]
pub(crate) struct UsageSessionDetailEnvelope {
    contract: control_plane::ContractDescriptor,
    query: UsageSessionDetailQueryEcho,
    session: journal::OrchestratorUsageSessionRecord,
    totals: journal::OrchestratorUsageTotals,
    timeline: Vec<journal::OrchestratorUsageTimelineBucket>,
    runs: Vec<journal::OrchestratorUsageRunRecord>,
    cost_tracking_available: bool,
}

/// Usage rollup for one agent (sessions attributed via session bindings).
#[derive(Debug, Clone, Serialize)]
pub(crate) struct UsageAgentRecord {
    agent_id: String,
    display_name: String,
    binding_source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    default_model_profile: Option<String>,
    session_count: u64,
    runs: u64,
    active_runs: u64,
    completed_runs: u64,
    prompt_tokens: u64,
    completion_tokens: u64,
    total_tokens: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    average_latency_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    latest_started_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    estimated_cost_usd: Option<f64>,
}

/// Response envelope for `GET /console/v1/usage/agents`.
#[derive(Debug, Serialize)]
pub(crate) struct UsageAgentsEnvelope {
    contract: control_plane::ContractDescriptor,
    query: UsagePaginationQueryEcho,
    agents: Vec<UsageAgentRecord>,
    page: control_plane::PageInfo,
    cost_tracking_available: bool,
}

/// Usage rollup for one model profile (sessions attributed via the bound
/// agent's default model profile, not the per-run routed model).
#[derive(Debug, Clone, Serialize)]
pub(crate) struct UsageModelRecord {
    model_id: String,
    display_name: String,
    model_source: String,
    agent_count: u64,
    session_count: u64,
    runs: u64,
    active_runs: u64,
    completed_runs: u64,
    prompt_tokens: u64,
    completion_tokens: u64,
    total_tokens: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    average_latency_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    latest_started_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    estimated_cost_usd: Option<f64>,
}

/// Response envelope for `GET /console/v1/usage/models`.
#[derive(Debug, Serialize)]
pub(crate) struct UsageModelsEnvelope {
    contract: control_plane::ContractDescriptor,
    query: UsagePaginationQueryEcho,
    models: Vec<UsageModelRecord>,
    page: control_plane::PageInfo,
    cost_tracking_available: bool,
}

/// Pricing-table coverage shown in the insights envelope.
#[derive(Debug, Serialize)]
pub(crate) struct UsageInsightsPricingSummary {
    known_entries: usize,
    estimated_models: usize,
    estimate_only: bool,
}

/// Model-provider health snapshot embedded in the insights envelope.
#[derive(Debug, Serialize)]
pub(crate) struct UsageInsightsHealthSummary {
    provider_state: String,
    provider_kind: String,
    error_rate_bps: u32,
    circuit_open: bool,
    cooldown_ms: u64,
    avg_latency_ms: u64,
    recent_routing_overrides: usize,
}

/// Smart-routing activity rollup (mode counts and recent decisions) for the
/// queried window.
#[derive(Debug, Serialize)]
pub(crate) struct UsageInsightsRoutingSummary {
    default_mode: String,
    suggest_runs: usize,
    dry_run_runs: usize,
    enforced_runs: usize,
    overrides: usize,
    recent_decisions: Vec<journal::UsageRoutingDecisionRecord>,
}

/// Budget policies and their evaluation results for the queried window.
#[derive(Debug, Serialize)]
pub(crate) struct UsageInsightsBudgetsEnvelope {
    policies: Vec<journal::UsageBudgetPolicyRecord>,
    evaluations: Vec<UsageBudgetEvaluation>,
}

/// Declares where operator-insight data comes from and that derived metrics
/// are recomputed per request rather than persisted.
#[derive(Debug, Serialize, Clone)]
pub(crate) struct OperatorInsightsRetentionPolicy {
    source_of_truth: String,
    aggregation_mode: String,
    derived_metrics_persisted: bool,
    support_bundle_embeds_latest_snapshot: bool,
    window_start_at_unix_ms: i64,
    window_end_at_unix_ms: i64,
}

/// Sampling caps applied while building the snapshot, with observed-vs-sampled
/// counts and human-readable truncation notes.
#[derive(Debug, Serialize, Clone)]
pub(crate) struct OperatorInsightsSamplingPolicy {
    run_sample_limit: usize,
    tape_event_limit_per_run: usize,
    cron_run_limit: usize,
    plugin_limit: usize,
    observed_runs: usize,
    sampled_runs: usize,
    observed_cron_runs: usize,
    sampled_cron_runs: usize,
    observed_plugins: usize,
    sampled_plugins: usize,
    notes: Vec<String>,
}

/// Declares the redaction guarantees of the snapshot (no raw queries, error
/// messages, or config values; secret-like values redacted).
#[derive(Debug, Serialize, Clone)]
pub(crate) struct OperatorInsightsPrivacyPolicy {
    redaction_mode: String,
    raw_queries_included: bool,
    raw_error_messages_included: bool,
    raw_config_values_included: bool,
    secret_like_values_redacted: bool,
}

/// Link pair (console API path plus web console route) for drilling into an
/// insight's underlying data.
#[derive(Debug, Serialize, Clone)]
pub(crate) struct OperatorInsightDrillDown {
    label: String,
    section: String,
    api_path: String,
    console_path: String,
}

/// One non-info subsystem finding surfaced at the top of the snapshot.
#[derive(Debug, Serialize, Clone)]
pub(crate) struct OperatorInsightHotspot {
    hotspot_id: String,
    subsystem: String,
    state: String,
    severity: String,
    summary: String,
    detail: String,
    recommended_action: String,
    drill_down: OperatorInsightDrillDown,
}

/// Roll-up verdict over all hotspots (blocking beats warning beats ok).
#[derive(Debug, Serialize, Clone)]
pub(crate) struct OperatorInsightsSummary {
    state: String,
    severity: String,
    hotspot_count: usize,
    blocking_hotspots: usize,
    warning_hotspots: usize,
    recommendation: String,
}

/// Provider auth, error-rate, latency, and response-cache health insight.
#[derive(Debug, Serialize, Clone)]
pub(crate) struct OperatorProviderHealthInsight {
    state: String,
    severity: String,
    summary: String,
    provider_kind: String,
    error_rate_bps: u32,
    avg_latency_ms: u64,
    circuit_open: bool,
    auth_state: String,
    refresh_failures: u64,
    response_cache_enabled: bool,
    response_cache_entries: usize,
    response_cache_hit_rate_bps: u32,
    recommended_action: String,
    drill_down: OperatorInsightDrillDown,
}

/// One sampled recall event; `query_preview` is whitespace-collapsed,
/// truncated, and redacted before serialization.
#[derive(Debug, Serialize, Clone)]
pub(crate) struct OperatorRecallSample {
    run_id: String,
    session_id: Option<String>,
    kind: String,
    query_preview: String,
    total_hits: usize,
    memory_hits: usize,
    workspace_hits: usize,
    transcript_hits: usize,
    checkpoint_hits: usize,
    compaction_hits: usize,
}

/// Memory recall quality insight (explicit recall and auto-inject hit rates).
#[derive(Debug, Serialize, Clone)]
pub(crate) struct OperatorRecallInsight {
    state: String,
    severity: String,
    summary: String,
    explicit_recall_events: usize,
    explicit_recall_zero_hit_events: usize,
    explicit_recall_zero_hit_rate_bps: u32,
    auto_inject_events: usize,
    auto_inject_zero_hit_events: usize,
    auto_inject_avg_hits: f64,
    samples: Vec<OperatorRecallSample>,
    recommended_action: String,
    drill_down: OperatorInsightDrillDown,
}

/// One sampled session-compaction event.
#[derive(Debug, Serialize, Clone)]
pub(crate) struct OperatorCompactionSample {
    run_id: String,
    session_id: Option<String>,
    trigger: String,
    token_delta: i64,
    estimated_input_tokens: u64,
    estimated_output_tokens: u64,
    artifact_id: Option<String>,
}

/// Session-compaction effectiveness insight (preview/created/blocked counts
/// and token-reduction averages).
#[derive(Debug, Serialize, Clone)]
pub(crate) struct OperatorCompactionInsight {
    state: String,
    severity: String,
    summary: String,
    preview_events: usize,
    created_events: usize,
    actionable_blocked_events: usize,
    dry_run_events: usize,
    avg_token_delta: i64,
    avg_reduction_bps: u32,
    samples: Vec<OperatorCompactionSample>,
    recommended_action: String,
    drill_down: OperatorInsightDrillDown,
}

/// One sampled denied tool decision; `reason` is sanitized before
/// serialization.
#[derive(Debug, Serialize, Clone)]
pub(crate) struct OperatorSafetySample {
    run_id: String,
    tool_name: String,
    reason: String,
    approval_required: bool,
}

/// Tool-decision policy insight (deny/approval rates over sampled runs).
#[derive(Debug, Serialize, Clone)]
pub(crate) struct OperatorSafetyBoundaryInsight {
    state: String,
    severity: String,
    summary: String,
    inspected_tool_decisions: usize,
    denied_tool_decisions: usize,
    policy_enforced_denies: usize,
    approval_required_decisions: usize,
    deny_rate_bps: u32,
    samples: Vec<OperatorSafetySample>,
    recommended_action: String,
    drill_down: OperatorInsightDrillDown,
}

/// One sampled unhealthy plugin binding with its failure reasons.
#[derive(Debug, Serialize, Clone)]
pub(crate) struct OperatorPluginSample {
    plugin_id: String,
    discovery_state: Option<String>,
    config_state: Option<String>,
    contracts_mode: Option<String>,
    reasons: Vec<String>,
}

/// Plugin-bindings operability insight (contract, config, and discovery
/// failure counts).
#[derive(Debug, Serialize, Clone)]
pub(crate) struct OperatorPluginInsight {
    state: String,
    severity: String,
    summary: String,
    total_bindings: usize,
    ready_bindings: usize,
    unhealthy_bindings: usize,
    typed_contract_failures: usize,
    config_failures: usize,
    discovery_failures: usize,
    samples: Vec<OperatorPluginSample>,
    recommended_action: String,
    drill_down: OperatorInsightDrillDown,
}

/// One sampled cron run outcome.
#[derive(Debug, Serialize, Clone)]
pub(crate) struct OperatorCronRunSample {
    run_id: String,
    job_id: String,
    status: String,
    error_kind: Option<String>,
    tool_denies: u64,
}

/// Cron delivery insight (failure rate and tool denies over recent runs).
#[derive(Debug, Serialize, Clone)]
pub(crate) struct OperatorCronInsight {
    state: String,
    severity: String,
    summary: String,
    total_runs: usize,
    failed_runs: usize,
    success_rate_bps: u32,
    total_tool_denies: u64,
    samples: Vec<OperatorCronRunSample>,
    recommended_action: String,
    drill_down: OperatorInsightDrillDown,
}

/// One config ref whose reload state needs operator attention.
#[derive(Debug, Serialize, Clone)]
pub(crate) struct OperatorReloadHotspot {
    ref_id: String,
    config_path: String,
    state: String,
    severity: String,
    reload_mode: String,
    advice: Option<String>,
}

/// Config-ref reload health insight (blocking/warning refs and hotspots).
#[derive(Debug, Serialize, Clone)]
pub(crate) struct OperatorReloadInsight {
    state: String,
    severity: String,
    summary: String,
    blocking_refs: usize,
    warning_refs: usize,
    hotspots: Vec<OperatorReloadHotspot>,
    recommended_action: String,
    drill_down: OperatorInsightDrillDown,
}

/// Cross-subsystem operations rollup (stuck runs, queue backlog, cooldowns,
/// worker fleet anomalies).
#[derive(Debug, Serialize, Clone)]
pub(crate) struct OperatorOperationsOverviewInsight {
    state: String,
    severity: String,
    summary: String,
    stuck_runs: u64,
    provider_cooldowns: usize,
    queue_backlog: u64,
    routine_failures: usize,
    plugin_errors: usize,
    worker_orphaned: u64,
    worker_failed_closed: u64,
    recommended_action: String,
    drill_down: OperatorInsightDrillDown,
}

/// Security posture insight (policy/approval denies, redactions, sandbox
/// violations).
#[derive(Debug, Serialize, Clone)]
pub(crate) struct OperatorSecurityInsight {
    state: String,
    severity: String,
    summary: String,
    approval_denies: u64,
    policy_denies: u64,
    redaction_events: u64,
    sandbox_violations: u64,
    skill_execution_denies: u64,
    sampled_denied_tool_decisions: usize,
    recommended_action: String,
    drill_down: OperatorInsightDrillDown,
}

/// Routine (scheduled job) delivery insight.
#[derive(Debug, Serialize, Clone)]
pub(crate) struct OperatorRoutineInsight {
    state: String,
    severity: String,
    summary: String,
    total_runs: usize,
    failed_runs: usize,
    skipped_runs: u64,
    policy_denies: u64,
    success_rate_bps: u32,
    recommended_action: String,
    drill_down: OperatorInsightDrillDown,
}

/// Learning-candidate lifecycle insight (review queue, rollbacks, and
/// prompt-injection conflicts).
#[derive(Debug, Serialize, Clone)]
pub(crate) struct OperatorMemoryLearningInsight {
    state: String,
    severity: String,
    summary: String,
    total_candidates: usize,
    proposed_candidates: usize,
    needs_review_candidates: usize,
    approved_candidates: usize,
    rejected_candidates: usize,
    deployed_candidates: usize,
    rolled_back_candidates: usize,
    auto_applied_candidates: usize,
    memory_rejections: u64,
    injection_conflicts: usize,
    rollback_events: usize,
    recommended_action: String,
    drill_down: OperatorInsightDrillDown,
}

/// Full operator-insights snapshot; embedded in the usage insights envelope
/// and in support bundles.
#[derive(Debug, Serialize, Clone)]
pub(crate) struct OperatorInsightsEnvelope {
    generated_at_unix_ms: i64,
    summary: OperatorInsightsSummary,
    hotspots: Vec<OperatorInsightHotspot>,
    retention: OperatorInsightsRetentionPolicy,
    sampling: OperatorInsightsSamplingPolicy,
    privacy: OperatorInsightsPrivacyPolicy,
    operations: OperatorOperationsOverviewInsight,
    provider_health: OperatorProviderHealthInsight,
    security: OperatorSecurityInsight,
    recall: OperatorRecallInsight,
    compaction: OperatorCompactionInsight,
    safety_boundary: OperatorSafetyBoundaryInsight,
    plugins: OperatorPluginInsight,
    cron: OperatorCronInsight,
    routines: OperatorRoutineInsight,
    memory_learning: OperatorMemoryLearningInsight,
    reload: OperatorReloadInsight,
}

/// Response envelope for `GET /console/v1/usage/insights`.
#[derive(Debug, Serialize)]
pub(crate) struct UsageInsightsEnvelope {
    contract: control_plane::ContractDescriptor,
    query: UsageQueryEcho,
    totals: journal::OrchestratorUsageTotals,
    timeline: Vec<journal::OrchestratorUsageTimelineBucket>,
    pricing: UsageInsightsPricingSummary,
    health: UsageInsightsHealthSummary,
    routing: UsageInsightsRoutingSummary,
    budgets: UsageInsightsBudgetsEnvelope,
    alerts: Vec<journal::UsageAlertRecord>,
    model_mix: Vec<UsageModelMixRecord>,
    scope_mix: Vec<UsageScopeMixRecord>,
    tool_mix: Vec<UsageToolMixRecord>,
    operator: OperatorInsightsEnvelope,
    cost_tracking_available: bool,
}

/// Returns window totals and a bucketed timeline of orchestrator usage.
///
/// # Errors
/// Returns an error response when the console session is invalid, the query
/// window/bucket fails validation, or the journal query fails.
pub(crate) async fn console_usage_summary_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleUsageSummaryQuery>,
) -> Result<Json<UsageSummaryEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let resolved = resolve_usage_query(
        query.start_at_unix_ms,
        query.end_at_unix_ms,
        query.bucket.as_deref(),
        query.include_archived.unwrap_or(false),
        &session.context,
        None,
    )?;
    let summary = state
        .runtime
        .summarize_orchestrator_usage(resolved.query.clone())
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(UsageSummaryEnvelope {
        contract: contract_descriptor(),
        query: resolved.echo,
        totals: summary.totals,
        timeline: summary.timeline,
        cost_tracking_available: summary.cost_tracking_available,
    }))
}

/// Lists per-session usage rollups, paginated with an offset cursor over the
/// in-memory result list.
///
/// # Errors
/// Returns an error response when the console session is invalid, the cursor
/// or query window fails validation, or the journal query fails.
pub(crate) async fn console_usage_sessions_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleUsageBreakdownQuery>,
) -> Result<Json<UsageSessionsEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let limit =
        query.limit.unwrap_or(DEFAULT_USAGE_BREAKDOWN_LIMIT).clamp(1, MAX_USAGE_BREAKDOWN_LIMIT);
    let cursor = parse_usage_cursor(query.cursor.as_deref())?;
    let resolved = resolve_usage_query(
        query.start_at_unix_ms,
        query.end_at_unix_ms,
        query.bucket.as_deref(),
        query.include_archived.unwrap_or(false),
        &session.context,
        None,
    )?;
    let sessions = state
        .runtime
        .list_orchestrator_usage_sessions(resolved.query.clone())
        .await
        .map_err(runtime_status_response)?;
    let cost_tracking_available =
        usage_cost_tracking_available(sessions.iter().map(|entry| entry.estimated_cost_usd));
    let next_cursor =
        (cursor.saturating_add(limit) < sessions.len()).then(|| (cursor + limit).to_string());
    let page =
        build_page_info(limit, sessions.len().saturating_sub(cursor).min(limit), next_cursor);
    let sessions = sessions.into_iter().skip(cursor).take(limit).collect::<Vec<_>>();

    Ok(Json(UsageSessionsEnvelope {
        contract: contract_descriptor(),
        query: UsagePaginationQueryEcho {
            start_at_unix_ms: resolved.echo.start_at_unix_ms,
            end_at_unix_ms: resolved.echo.end_at_unix_ms,
            bucket: resolved.echo.bucket,
            bucket_width_ms: resolved.echo.bucket_width_ms,
            include_archived: resolved.echo.include_archived,
            limit,
            cursor,
        },
        sessions,
        page,
        cost_tracking_available,
    }))
}

/// Returns one session's usage rollup, its window timeline, and its most
/// recent runs (capped by `run_limit`).
///
/// # Errors
/// Returns an error response when the console session is invalid, the session
/// id is not a canonical ULID, the query fails validation, the journal query
/// fails, or the session does not exist in the window.
pub(crate) async fn console_usage_session_detail_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Query(query): Query<ConsoleUsageSessionDetailQuery>,
) -> Result<Json<UsageSessionDetailEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    validate_canonical_id(session_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "session_id must be a canonical ULID",
        ))
    })?;
    let run_limit =
        query.run_limit.unwrap_or(DEFAULT_USAGE_RUN_LIMIT).clamp(1, MAX_USAGE_RUN_LIMIT);
    let resolved = resolve_usage_query(
        query.start_at_unix_ms,
        query.end_at_unix_ms,
        query.bucket.as_deref(),
        query.include_archived.unwrap_or(false),
        &session.context,
        Some(session_id.clone()),
    )?;
    let summary = state
        .runtime
        .summarize_orchestrator_usage(resolved.query.clone())
        .await
        .map_err(runtime_status_response)?;
    let (session_record, session_runs) = state
        .runtime
        .get_orchestrator_usage_session(resolved.query.clone(), session_id, run_limit)
        .await
        .map_err(runtime_status_response)?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found("session was not found"))
        })?;

    Ok(Json(UsageSessionDetailEnvelope {
        contract: contract_descriptor(),
        query: UsageSessionDetailQueryEcho {
            start_at_unix_ms: resolved.echo.start_at_unix_ms,
            end_at_unix_ms: resolved.echo.end_at_unix_ms,
            bucket: resolved.echo.bucket,
            bucket_width_ms: resolved.echo.bucket_width_ms,
            include_archived: resolved.echo.include_archived,
            run_limit,
        },
        session: session_record,
        totals: summary.totals,
        timeline: summary.timeline,
        runs: session_runs,
        cost_tracking_available: summary.cost_tracking_available,
    }))
}

/// Lists per-agent usage rollups built by grouping sessions through their
/// agent bindings; unbound sessions fall into an "unassigned" row.
///
/// # Errors
/// Returns an error response when the console session is invalid, the cursor
/// or query window fails validation, or journal/agent lookups fail.
pub(crate) async fn console_usage_agents_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleUsageBreakdownQuery>,
) -> Result<Json<UsageAgentsEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let limit =
        query.limit.unwrap_or(DEFAULT_USAGE_BREAKDOWN_LIMIT).clamp(1, MAX_USAGE_BREAKDOWN_LIMIT);
    let cursor = parse_usage_cursor(query.cursor.as_deref())?;
    let resolved = resolve_usage_query(
        query.start_at_unix_ms,
        query.end_at_unix_ms,
        query.bucket.as_deref(),
        query.include_archived.unwrap_or(false),
        &session.context,
        None,
    )?;
    let sessions = state
        .runtime
        .list_orchestrator_usage_sessions(resolved.query.clone())
        .await
        .map_err(runtime_status_response)?;
    let usage_metadata = load_usage_metadata(&state, &session.context).await?;
    let rows = build_usage_agent_rows(sessions.as_slice(), &usage_metadata);
    let cost_tracking_available =
        usage_cost_tracking_available(rows.iter().map(|entry| entry.estimated_cost_usd));
    let next_cursor =
        (cursor.saturating_add(limit) < rows.len()).then(|| (cursor + limit).to_string());
    let page = build_page_info(limit, rows.len().saturating_sub(cursor).min(limit), next_cursor);
    let agents = rows.into_iter().skip(cursor).take(limit).collect::<Vec<_>>();

    Ok(Json(UsageAgentsEnvelope {
        contract: contract_descriptor(),
        query: UsagePaginationQueryEcho {
            start_at_unix_ms: resolved.echo.start_at_unix_ms,
            end_at_unix_ms: resolved.echo.end_at_unix_ms,
            bucket: resolved.echo.bucket,
            bucket_width_ms: resolved.echo.bucket_width_ms,
            include_archived: resolved.echo.include_archived,
            limit,
            cursor,
        },
        agents,
        page,
        cost_tracking_available,
    }))
}

/// Lists per-model usage rollups keyed by each bound agent's default model
/// profile (the journal does not record a per-run model id at this level).
///
/// # Errors
/// Returns an error response when the console session is invalid, the cursor
/// or query window fails validation, or journal/agent lookups fail.
pub(crate) async fn console_usage_models_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleUsageBreakdownQuery>,
) -> Result<Json<UsageModelsEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let limit =
        query.limit.unwrap_or(DEFAULT_USAGE_BREAKDOWN_LIMIT).clamp(1, MAX_USAGE_BREAKDOWN_LIMIT);
    let cursor = parse_usage_cursor(query.cursor.as_deref())?;
    let resolved = resolve_usage_query(
        query.start_at_unix_ms,
        query.end_at_unix_ms,
        query.bucket.as_deref(),
        query.include_archived.unwrap_or(false),
        &session.context,
        None,
    )?;
    let sessions = state
        .runtime
        .list_orchestrator_usage_sessions(resolved.query.clone())
        .await
        .map_err(runtime_status_response)?;
    let usage_metadata = load_usage_metadata(&state, &session.context).await?;
    let rows = build_usage_model_rows(sessions.as_slice(), &usage_metadata);
    let cost_tracking_available =
        usage_cost_tracking_available(rows.iter().map(|entry| entry.estimated_cost_usd));
    let next_cursor =
        (cursor.saturating_add(limit) < rows.len()).then(|| (cursor + limit).to_string());
    let page = build_page_info(limit, rows.len().saturating_sub(cursor).min(limit), next_cursor);
    let models = rows.into_iter().skip(cursor).take(limit).collect::<Vec<_>>();

    Ok(Json(UsageModelsEnvelope {
        contract: contract_descriptor(),
        query: UsagePaginationQueryEcho {
            start_at_unix_ms: resolved.echo.start_at_unix_ms,
            end_at_unix_ms: resolved.echo.end_at_unix_ms,
            bucket: resolved.echo.bucket,
            bucket_width_ms: resolved.echo.bucket_width_ms,
            include_archived: resolved.echo.include_archived,
            limit,
            cursor,
        },
        models,
        page,
        cost_tracking_available,
    }))
}

fn usage_cost_tracking_available(costs: impl IntoIterator<Item = Option<f64>>) -> bool {
    costs.into_iter().any(|cost| cost.is_some())
}

/// Builds the full usage-insights view: window totals, cost estimation,
/// routing and budget evaluation, alert generation, and the operator
/// insights snapshot.
///
/// # Errors
/// Returns an error response when the console session is invalid, the query
/// window fails validation, or any of the journal/runtime/auth snapshot
/// queries fail. Alert persistence failures are intentionally swallowed.
pub(crate) async fn console_usage_insights_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleUsageSummaryQuery>,
) -> Result<Json<UsageInsightsEnvelope>, Response> {
    // Although this endpoint returns an insight snapshot, it also persists and may reopen alerts.
    let session = authorize_console_session(&state, &headers, true)?;
    let resolved = resolve_usage_query(
        query.start_at_unix_ms,
        query.end_at_unix_ms,
        query.bucket.as_deref(),
        query.include_archived.unwrap_or(false),
        &session.context,
        None,
    )?;
    let summary = state
        .runtime
        .summarize_orchestrator_usage(resolved.query.clone())
        .await
        .map_err(runtime_status_response)?;
    let runs = state
        .runtime
        .list_orchestrator_usage_runs(resolved.query.clone(), 500)
        .await
        .map_err(runtime_status_response)?;
    let pricing =
        state.runtime.list_usage_pricing_records().await.map_err(runtime_status_response)?;
    let routing_decisions = state
        .runtime
        .list_usage_routing_decisions(journal::UsageRoutingDecisionsFilter {
            since_unix_ms: Some(resolved.query.start_at_unix_ms),
            until_unix_ms: Some(resolved.query.end_at_unix_ms),
            session_id: None,
            run_id: None,
            limit: 50,
        })
        .await
        .map_err(runtime_status_response)?;
    let routing_summary_recent_decisions = routing_decisions.clone();
    let budget_policies = state
        .runtime
        .list_usage_budget_policies(journal::UsageBudgetPoliciesFilter {
            enabled_only: false,
            scope_kind: None,
            scope_id: None,
        })
        .await
        .map_err(runtime_status_response)?;
    let status_snapshot = state
        .runtime
        .status_snapshot_async(session.context.clone(), state.auth.clone())
        .await
        .map_err(runtime_status_response)?;
    let auth_snapshot = state
        .auth_runtime
        .admin_status_snapshot(std::sync::Arc::clone(&state.runtime))
        .await
        .map_err(runtime_status_response)?;
    let auth_payload = serde_json::to_value(auth_snapshot).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to serialize usage auth status snapshot: {error}"
        )))
    })?;
    let provider_auth_observability =
        build_provider_auth_observability(&auth_payload, state.observability.as_ref());
    let provider_snapshot = &status_snapshot.model_provider;
    let provider_kind = provider_snapshot.kind.clone();
    // Fallback chain mirrors the model runtime's default-model resolution;
    // "deterministic" is the offline provider used when nothing is configured.
    let default_model_id = provider_snapshot
        .registry
        .default_chat_model_id
        .clone()
        .or_else(|| provider_snapshot.model_id.clone())
        .or_else(|| provider_snapshot.openai_model.clone())
        .or_else(|| provider_snapshot.anthropic_model.clone())
        .unwrap_or_else(|| "deterministic".to_owned());

    let routing_by_run = routing_decisions
        .iter()
        .map(|record| (record.run_id.clone(), record))
        .collect::<HashMap<_, _>>();
    // Cost attribution precedence: a recorded routing decision pins the exact
    // model and provider for the run; otherwise infer the provider from the
    // registry entry for the default chat model, falling back to the
    // top-level provider snapshot.
    let enriched_runs = runs
        .iter()
        .map(|run| {
            let routing = routing_by_run.get(run.run_id.as_str()).copied();
            let model_id = routing
                .map(|record| record.actual_model_id.as_str())
                .unwrap_or(default_model_id.as_str());
            let (fallback_provider_id, fallback_provider_kind) =
                resolve_provider_for_model(provider_snapshot, model_id);
            let provider_kind_value = routing
                .map(|record| record.provider_kind.as_str())
                .unwrap_or(fallback_provider_kind);
            let provider_id_value =
                routing.map(|record| record.provider_id.as_str()).unwrap_or(fallback_provider_id);
            let cost_estimate = estimate_cost_for_model(
                pricing.as_slice(),
                provider_kind_value,
                provider_id_value,
                model_id,
                run.started_at_unix_ms,
                run.prompt_tokens,
                run.completion_tokens,
            );
            crate::usage_governance::UsageEnrichedRun {
                run,
                routing,
                inferred_model_id: model_id,
                inferred_provider_kind: provider_kind_value,
                cost_estimate,
            }
        })
        .collect::<Vec<_>>();
    let approved_subjects = load_budget_override_approvals(
        &state.runtime,
        &session.context,
        None,
        None,
        budget_policies.as_slice(),
    )
    .await;
    // Budget evaluation deliberately uses the upper cost estimate for both
    // projection bounds: budgets should trip on the conservative figure.
    let total_estimated_cost =
        enriched_runs.iter().filter_map(|entry| entry.cost_estimate.upper_usd).sum::<f64>();
    let cost_projection = PricingEstimate {
        lower_usd: Some(total_estimated_cost),
        upper_usd: Some(total_estimated_cost),
        estimate_only: true,
        source: "usage_window".to_owned(),
        precision: "estimate_only".to_owned(),
    };
    let budget_evaluations = evaluate_budget_policies(
        budget_policies.as_slice(),
        enriched_runs.as_slice(),
        summary.totals.total_tokens,
        &cost_projection,
        &approved_subjects,
    );
    let model_mix = build_model_mix(enriched_runs.as_slice());
    let scope_mix = build_scope_mix(enriched_runs.as_slice());
    let tool_mix = load_usage_tool_mix(&state, runs.as_slice()).await;
    let alert_candidates = build_alert_candidates(
        enriched_runs.as_slice(),
        routing_decisions.as_slice(),
        budget_evaluations.as_slice(),
        model_mix.as_slice(),
        model_provider_health_state(&status_snapshot.model_provider),
    );
    // Alert persistence is best-effort: a failed upsert must not break the
    // insights read path. Each candidate gets a fresh id; the journal
    // deduplicates recurring alerts through dedupe_key.
    for (index, candidate) in alert_candidates.iter().enumerate() {
        let _ = state
            .runtime
            .upsert_usage_alert(journal::UsageAlertUpsertRequest {
                alert_id: format!("{}-{index}", Ulid::generate()),
                alert_kind: candidate.alert_kind.clone(),
                severity: candidate.severity.clone(),
                scope_kind: candidate.scope_kind.clone(),
                scope_id: candidate.scope_id.clone(),
                summary: candidate.summary.clone(),
                reason: candidate.reason.clone(),
                recommended_action: candidate.recommended_action.clone(),
                source: "usage_insights".to_owned(),
                dedupe_key: candidate.dedupe_key.clone(),
                payload_json: candidate.payload.to_string(),
                observed_at_unix_ms: resolved.query.end_at_unix_ms,
                resolved: false,
            })
            .await;
    }
    let alerts = state
        .runtime
        .list_usage_alerts(journal::UsageAlertsFilter {
            active_only: true,
            limit: 12,
            scope_kind: None,
            scope_id: None,
        })
        .await
        .map_err(runtime_status_response)?;
    let routing_summary = UsageInsightsRoutingSummary {
        default_mode: if state.runtime.config.smart_routing.enabled {
            state.runtime.config.smart_routing.default_mode.clone()
        } else {
            "disabled".to_owned()
        },
        suggest_runs: routing_decisions.iter().filter(|entry| entry.mode == "suggest").count(),
        dry_run_runs: routing_decisions.iter().filter(|entry| entry.mode == "dry_run").count(),
        enforced_runs: routing_decisions.iter().filter(|entry| entry.mode == "enforced").count(),
        overrides: routing_decisions
            .iter()
            .filter(|entry| entry.actual_model_id != entry.default_model_id)
            .count(),
        recent_decisions: routing_summary_recent_decisions,
    };
    let cost_tracking_available =
        enriched_runs.iter().any(|entry| entry.cost_estimate.upper_usd.is_some());
    let operator = build_operator_insights_snapshot(
        &state,
        &resolved,
        runs.as_slice(),
        provider_snapshot,
        &provider_auth_observability,
    )
    .await?;

    Ok(Json(UsageInsightsEnvelope {
        contract: contract_descriptor(),
        query: resolved.echo,
        totals: summary.totals,
        timeline: summary.timeline,
        pricing: UsageInsightsPricingSummary {
            known_entries: pricing.len(),
            estimated_models: pricing
                .iter()
                .map(|entry| entry.model_id.as_str())
                .collect::<HashSet<_>>()
                .len(),
            estimate_only: pricing.iter().any(|entry| entry.precision != "exact"),
        },
        health: UsageInsightsHealthSummary {
            provider_state: model_provider_health_state(&status_snapshot.model_provider).to_owned(),
            provider_kind,
            error_rate_bps: status_snapshot.model_provider.runtime_metrics.error_rate_bps,
            circuit_open: status_snapshot.model_provider.circuit_breaker.open,
            cooldown_ms: status_snapshot.model_provider.circuit_breaker.cooldown_ms,
            avg_latency_ms: status_snapshot.model_provider.runtime_metrics.avg_latency_ms,
            recent_routing_overrides: routing_summary.overrides,
        },
        routing: routing_summary,
        budgets: UsageInsightsBudgetsEnvelope {
            policies: budget_policies,
            evaluations: budget_evaluations,
        },
        alerts,
        model_mix,
        scope_mix,
        tool_mix,
        operator,
        cost_tracking_available,
    }))
}

/// Upserts a model pricing record (CSRF-checked console session required).
///
/// # Errors
/// Returns an error response when the session/CSRF check fails or the journal
/// rejects the pricing record.
pub(crate) async fn console_usage_pricing_upsert_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleUsagePricingUpsertRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let record = state
        .runtime
        .upsert_usage_pricing_record(journal::UsagePricingUpsertRequest {
            pricing_id: payload.pricing_id,
            provider_id: payload.provider_id,
            provider_kind: payload.provider_kind,
            model_id: payload.model_id,
            effective_from_unix_ms: payload.effective_from_unix_ms,
            effective_to_unix_ms: payload.effective_to_unix_ms,
            input_cost_per_million_usd: payload.input_cost_per_million_usd,
            output_cost_per_million_usd: payload.output_cost_per_million_usd,
            fixed_request_cost_usd: payload.fixed_request_cost_usd,
            source: payload.source,
            precision: payload.precision,
            currency: payload.currency,
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "operator_principal": session.context.principal,
        "pricing": record,
    })))
}

/// Upserts a usage budget policy, attributing the change to the operator
/// principal (CSRF-checked console session required).
///
/// # Errors
/// Returns an error response when the session/CSRF check fails or the journal
/// rejects the policy.
pub(crate) async fn console_usage_budget_policy_upsert_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleUsageBudgetPolicyUpsertRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let record = state
        .runtime
        .upsert_usage_budget_policy(journal::UsageBudgetPolicyUpsertRequest {
            policy_id: payload.policy_id,
            scope_kind: payload.scope_kind,
            scope_id: payload.scope_id,
            metric_kind: payload.metric_kind,
            interval_kind: payload.interval_kind,
            soft_limit_value: payload.soft_limit_value,
            hard_limit_value: payload.hard_limit_value,
            action: payload.action,
            routing_mode_override: payload.routing_mode_override,
            enabled: payload.enabled,
            operator_principal: session.context.principal.clone(),
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "operator_principal": session.context.principal,
        "policy": record,
    })))
}

/// Files an approval request to override one budget policy and records a
/// console audit event (best-effort) for the request.
///
/// # Errors
/// Returns an error response when the session/CSRF check fails, the policy id
/// is unknown, or the override request cannot be persisted.
pub(crate) async fn console_usage_budget_override_request_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(policy_id): Path<String>,
    Json(payload): Json<ConsoleUsageBudgetOverrideRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let policy = state
        .runtime
        .list_usage_budget_policies(journal::UsageBudgetPoliciesFilter {
            enabled_only: false,
            scope_kind: None,
            scope_id: None,
        })
        .await
        .map_err(runtime_status_response)?
        .into_iter()
        .find(|entry| entry.policy_id == policy_id)
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found("budget policy was not found"))
        })?;
    let approval = request_usage_budget_override(
        &state.runtime,
        &session.context,
        &policy,
        None,
        None,
        None,
        payload.reason.as_deref(),
    )
    .await
    .map_err(runtime_status_response)?;
    let _ = state
        .runtime
        .record_console_event(
            &session.context,
            "usage_budget_override.requested",
            json!({
                "policy_id": policy.policy_id,
                "scope_kind": policy.scope_kind,
                "scope_id": policy.scope_id,
                "approval_id": approval.approval_id,
            }),
        )
        .await;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "operator_principal": session.context.principal,
        "policy": policy,
        "approval": approval,
    })))
}

/// Streams a usage dataset as a JSON or CSV file-download response.
///
/// CSV cells are quoted and formula-prefixed defensively so exports cannot
/// trigger spreadsheet formula execution.
///
/// # Errors
/// Returns an error response when the console session is invalid, the
/// dataset/format/window parameters fail validation, or the journal queries
/// or serialization fail.
pub(crate) async fn console_usage_export_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleUsageExportQuery>,
) -> Result<Response, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let resolved = resolve_usage_query(
        query.start_at_unix_ms,
        query.end_at_unix_ms,
        query.bucket.as_deref(),
        query.include_archived.unwrap_or(false),
        &session.context,
        None,
    )?;
    let dataset = normalize_usage_export_dataset(query.dataset.as_str())?;
    let format = normalize_usage_export_format(query.format.as_str())?;

    let response = match dataset {
        UsageExportDataset::Timeline => {
            let summary = state
                .runtime
                .summarize_orchestrator_usage(resolved.query.clone())
                .await
                .map_err(runtime_status_response)?;
            match format {
                UsageExportFormat::Json => usage_json_export_response(
                    "timeline",
                    json!({
                        "contract": contract_descriptor(),
                        "query": resolved.echo,
                        "rows": summary.timeline,
                        "cost_tracking_available": summary.cost_tracking_available,
                    }),
                )?,
                UsageExportFormat::Csv => usage_csv_export_response(
                    "timeline",
                    build_timeline_csv(summary.timeline.as_slice()),
                )?,
            }
        }
        UsageExportDataset::Sessions => {
            let sessions = state
                .runtime
                .list_orchestrator_usage_sessions(resolved.query.clone())
                .await
                .map_err(runtime_status_response)?;
            match format {
                UsageExportFormat::Json => usage_json_export_response(
                    "sessions",
                    json!({
                        "contract": contract_descriptor(),
                        "query": resolved.echo,
                        "rows": sessions,
                        "cost_tracking_available": sessions
                            .iter()
                            .any(|entry| entry.estimated_cost_usd.is_some()),
                    }),
                )?,
                UsageExportFormat::Csv => {
                    usage_csv_export_response("sessions", build_sessions_csv(sessions.as_slice()))?
                }
            }
        }
        UsageExportDataset::Agents => {
            let sessions = state
                .runtime
                .list_orchestrator_usage_sessions(resolved.query.clone())
                .await
                .map_err(runtime_status_response)?;
            let usage_metadata = load_usage_metadata(&state, &session.context).await?;
            let agents = build_usage_agent_rows(sessions.as_slice(), &usage_metadata);
            match format {
                UsageExportFormat::Json => usage_json_export_response(
                    "agents",
                    json!({
                        "contract": contract_descriptor(),
                        "query": resolved.echo,
                        "rows": agents,
                        "cost_tracking_available": agents
                            .iter()
                            .any(|entry| entry.estimated_cost_usd.is_some()),
                    }),
                )?,
                UsageExportFormat::Csv => {
                    usage_csv_export_response("agents", build_agents_csv(agents.as_slice()))?
                }
            }
        }
        UsageExportDataset::Models => {
            let sessions = state
                .runtime
                .list_orchestrator_usage_sessions(resolved.query.clone())
                .await
                .map_err(runtime_status_response)?;
            let usage_metadata = load_usage_metadata(&state, &session.context).await?;
            let models = build_usage_model_rows(sessions.as_slice(), &usage_metadata);
            match format {
                UsageExportFormat::Json => usage_json_export_response(
                    "models",
                    json!({
                        "contract": contract_descriptor(),
                        "query": resolved.echo,
                        "rows": models,
                        "cost_tracking_available": models
                            .iter()
                            .any(|entry| entry.estimated_cost_usd.is_some()),
                    }),
                )?,
                UsageExportFormat::Csv => {
                    usage_csv_export_response("models", build_models_csv(models.as_slice()))?
                }
            }
        }
    };

    Ok(response)
}

/// Validated usage query plus the echo payload describing the effective
/// window after defaulting and clamping.
#[derive(Debug, Clone)]
struct ResolvedUsageQuery {
    query: OrchestratorUsageQuery,
    echo: UsageQueryEcho,
}

/// Lookup tables used to attribute sessions to agents and model profiles.
#[derive(Debug, Clone)]
struct UsageMetadata {
    bindings_by_session: HashMap<String, SessionAgentBinding>,
    agents_by_id: HashMap<String, AgentRecord>,
}

/// Datasets accepted by the export endpoint (`dataset` query parameter).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UsageExportDataset {
    Timeline,
    Sessions,
    Agents,
    Models,
}

/// Output encodings accepted by the export endpoint (`format` query
/// parameter).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UsageExportFormat {
    Json,
    Csv,
}

const fn default_usage_policy_enabled() -> bool {
    true
}

fn default_usage_currency() -> String {
    "USD".to_owned()
}

/// Grades provider health for the insights view: any open circuit breaker or
/// recorded error wins over auth state, and missing credentials are reported
/// distinctly so the console can prompt for setup rather than show "degraded".
fn model_provider_health_state(
    snapshot: &crate::model_provider::ProviderStatusSnapshot,
) -> &'static str {
    if snapshot.circuit_breaker.open || snapshot.runtime_metrics.error_count > 0 {
        "degraded"
    } else if snapshot.api_key_configured || snapshot.auth_profile_id.is_some() {
        "ok"
    } else {
        "missing_auth"
    }
}

/// Resolves `(provider_id, provider_kind)` for a model id via the provider
/// registry, falling back to the snapshot's primary provider when the model
/// is not registered.
fn resolve_provider_for_model<'a>(
    snapshot: &'a crate::model_provider::ProviderStatusSnapshot,
    model_id: &str,
) -> (&'a str, &'a str) {
    if let Some(model) = snapshot.registry.models.iter().find(|entry| entry.model_id == model_id) {
        if let Some(provider) =
            snapshot.registry.providers.iter().find(|entry| entry.provider_id == model.provider_id)
        {
            return (provider.provider_id.as_str(), provider.kind.as_str());
        }
    }
    (snapshot.provider_id.as_str(), snapshot.kind.as_str())
}

/// Counts tool usage by scanning session transcripts for the runs in the
/// window. Best-effort: sessions whose transcript cannot be loaded are
/// skipped rather than failing the whole insights response.
async fn load_usage_tool_mix(
    state: &AppState,
    runs: &[journal::OrchestratorUsageInsightsRunRecord],
) -> Vec<UsageToolMixRecord> {
    let mut run_ids_by_session = HashMap::<String, HashSet<String>>::new();
    for run in runs {
        run_ids_by_session.entry(run.session_id.clone()).or_default().insert(run.run_id.clone());
    }

    let mut tool_counts = HashMap::<String, u64>::new();
    for (session_id, run_ids) in run_ids_by_session {
        let transcript = match state.runtime.list_orchestrator_session_transcript(session_id).await
        {
            Ok(records) => records,
            Err(_) => continue,
        };
        for record in transcript {
            if !run_ids.contains(record.run_id.as_str()) {
                continue;
            }
            if !matches!(record.event_type.as_str(), "tool_proposal" | "tool.executed") {
                continue;
            }
            let tool_name = serde_json::from_str::<Value>(record.payload_json.as_str())
                .ok()
                .and_then(|payload| {
                    payload.get("tool_name").and_then(Value::as_str).map(ToOwned::to_owned)
                });
            let Some(tool_name) = tool_name else {
                continue;
            };
            *tool_counts.entry(tool_name).or_default() += 1;
        }
    }

    build_tool_mix(&tool_counts)
}

/// Counters and bounded sample lists accumulated from sampled run tapes
/// (recall, compaction, and tool-decision events).
#[derive(Debug, Default)]
struct OperatorTapeAggregation {
    explicit_recall_events: usize,
    explicit_recall_zero_hit_events: usize,
    auto_inject_events: usize,
    auto_inject_zero_hit_events: usize,
    auto_inject_total_hits: u64,
    compaction_preview_events: usize,
    compaction_created_events: usize,
    compaction_blocked_events: usize,
    compaction_actionable_blocked_events: usize,
    compaction_dry_run_events: usize,
    compaction_events: usize,
    compaction_total_token_delta: i128,
    compaction_total_input_tokens: u128,
    inspected_tool_decisions: usize,
    denied_tool_decisions: usize,
    policy_enforced_denies: usize,
    approval_required_decisions: usize,
    recall_samples: Vec<OperatorRecallSample>,
    compaction_samples: Vec<OperatorCompactionSample>,
    safety_samples: Vec<OperatorSafetySample>,
    truncated_tape_runs: usize,
}

/// Builds an operator-insights snapshot for a request context using the
/// default usage window; used by support bundles and the system insights
/// endpoint in addition to the usage insights handler.
///
/// # Errors
/// Returns an error response when the usage window cannot be resolved or any
/// underlying journal/runtime query fails.
pub(crate) async fn build_operator_insights_for_context(
    state: &AppState,
    context: &gateway::RequestContext,
    provider_snapshot: &crate::model_provider::ProviderStatusSnapshot,
    provider_auth_observability: &Value,
) -> Result<OperatorInsightsEnvelope, Response> {
    let resolved = resolve_usage_query(None, None, None, false, context, None)?;
    let runs = state
        .runtime
        .list_orchestrator_usage_runs(resolved.query.clone(), 500)
        .await
        .map_err(runtime_status_response)?;
    build_operator_insights_snapshot(
        state,
        &resolved,
        runs.as_slice(),
        provider_snapshot,
        provider_auth_observability,
    )
    .await
}

async fn build_operator_insights_snapshot(
    state: &AppState,
    resolved: &ResolvedUsageQuery,
    runs: &[journal::OrchestratorUsageInsightsRunRecord],
    provider_snapshot: &crate::model_provider::ProviderStatusSnapshot,
    provider_auth_observability: &Value,
) -> Result<OperatorInsightsEnvelope, Response> {
    let generated_at_unix_ms = current_unix_ms().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to resolve operator insights timestamp: {error}"
        )))
    })?;
    // Sample the newest runs first; the run-id tiebreak keeps the order
    // deterministic for runs sharing a start timestamp.
    let mut sampled_runs = runs.iter().collect::<Vec<_>>();
    sampled_runs.sort_by(|left, right| {
        right
            .started_at_unix_ms
            .cmp(&left.started_at_unix_ms)
            .then_with(|| right.run_id.cmp(&left.run_id))
    });
    let observed_runs = sampled_runs.len();
    if sampled_runs.len() > OPERATOR_INSIGHTS_RUN_SAMPLE_LIMIT {
        sampled_runs.truncate(OPERATOR_INSIGHTS_RUN_SAMPLE_LIMIT);
    }
    let tape_aggregation =
        collect_operator_tape_aggregation(state, sampled_runs.as_slice()).await?;

    let (cron_runs, cron_next_after) = state
        .runtime
        .list_cron_runs_for_owner(
            resolved.query.principal.clone(),
            None,
            Some(OPERATOR_INSIGHTS_CRON_RUN_LIMIT),
        )
        .await
        .map_err(runtime_status_response)?;

    let (plugin_index, plugin_error) = load_operator_plugin_index();
    let provider_health =
        build_operator_provider_health_insight(provider_snapshot, provider_auth_observability);
    let recall = build_operator_recall_insight(&tape_aggregation);
    let compaction = build_operator_compaction_insight(&tape_aggregation);
    let safety_boundary = build_operator_safety_boundary_insight(&tape_aggregation);
    let plugins = build_operator_plugin_insight(plugin_index.as_ref(), plugin_error.as_deref());
    let cron = build_operator_cron_insight(cron_runs.as_slice());
    let reload = build_operator_reload_insight(build_config_ref_health_observability(state));
    let counters = state.runtime.counters.snapshot();
    let runtime_decision = state.observability.runtime_decision_snapshot();
    let lease_snapshot = state.runtime.provider_lease_snapshot();
    let worker_snapshot = state.runtime.worker_fleet_snapshot();
    let operations = build_operator_operations_overview_insight(
        &counters,
        &runtime_decision,
        &lease_snapshot,
        &worker_snapshot,
        &plugins,
        &cron,
    );
    let security = build_operator_security_insight(&counters, &tape_aggregation);
    let routines = build_operator_routine_insight(cron_runs.as_slice(), &counters);
    let memory_learning = build_operator_memory_learning_insight(state, resolved).await?;
    let hotspots = build_operator_hotspots(OperatorHotspotSources {
        operations: &operations,
        provider_health: &provider_health,
        security: &security,
        recall: &recall,
        compaction: &compaction,
        safety_boundary: &safety_boundary,
        plugins: &plugins,
        cron: &cron,
        routines: &routines,
        memory_learning: &memory_learning,
        reload: &reload,
    });
    let blocking_hotspots =
        hotspots.iter().filter(|hotspot| hotspot.severity == "blocking").count();
    let warning_hotspots = hotspots.iter().filter(|hotspot| hotspot.severity == "warning").count();
    let (summary_state, summary_severity) = build_operator_summary(hotspots.as_slice());

    // The cron pager only reveals whether more history exists, not how much;
    // "+1" simply signals truncation in the observed-vs-sampled counts.
    let observed_cron_runs = cron_runs.len().saturating_add(usize::from(cron_next_after.is_some()));
    let sampled_cron_runs = cron_runs.len();
    let observed_plugins = plugin_index.as_ref().map(|index| index.entries.len()).unwrap_or(0);
    let sampled_plugins = observed_plugins.min(OPERATOR_INSIGHTS_PLUGIN_LIMIT);
    let mut sampling_notes = vec![
        "window is scoped to the authorized operator principal and request context".to_owned(),
    ];
    if observed_runs > sampled_runs.len() {
        sampling_notes.push(format!(
            "run sampling capped at {} of {} observed runs",
            sampled_runs.len(),
            observed_runs
        ));
    }
    if tape_aggregation.truncated_tape_runs > 0 {
        sampling_notes.push(format!(
            "{} sampled runs exceeded the per-run tape event cap",
            tape_aggregation.truncated_tape_runs
        ));
    }
    if cron_next_after.is_some() {
        sampling_notes.push(format!(
            "cron sampling stopped after {} runs; additional history is available",
            sampled_cron_runs
        ));
    }
    if observed_plugins > sampled_plugins {
        sampling_notes.push(format!(
            "plugin samples capped at {} of {} bindings",
            sampled_plugins, observed_plugins
        ));
    }
    if plugin_error.is_some() {
        sampling_notes.push(
            "plugin operability data is degraded because bindings could not be loaded".to_owned(),
        );
    }

    Ok(OperatorInsightsEnvelope {
        generated_at_unix_ms,
        summary: OperatorInsightsSummary {
            state: summary_state.to_owned(),
            severity: summary_severity.to_owned(),
            hotspot_count: hotspots.len(),
            blocking_hotspots,
            warning_hotspots,
            recommendation: if let Some(first) = hotspots.first() {
                first.recommended_action.clone()
            } else {
                "No blocking operator hotspots were detected in the sampled window.".to_owned()
            },
        },
        hotspots,
        retention: OperatorInsightsRetentionPolicy {
            source_of_truth: "append_only_journal_and_runtime_snapshots".to_owned(),
            aggregation_mode: "on_demand_window_scan".to_owned(),
            derived_metrics_persisted: false,
            support_bundle_embeds_latest_snapshot: true,
            window_start_at_unix_ms: resolved.query.start_at_unix_ms,
            window_end_at_unix_ms: resolved.query.end_at_unix_ms,
        },
        sampling: OperatorInsightsSamplingPolicy {
            run_sample_limit: OPERATOR_INSIGHTS_RUN_SAMPLE_LIMIT,
            tape_event_limit_per_run: OPERATOR_INSIGHTS_TAPE_SAMPLE_LIMIT,
            cron_run_limit: OPERATOR_INSIGHTS_CRON_RUN_LIMIT,
            plugin_limit: OPERATOR_INSIGHTS_PLUGIN_LIMIT,
            observed_runs,
            sampled_runs: sampled_runs.len(),
            observed_cron_runs,
            sampled_cron_runs,
            observed_plugins,
            sampled_plugins,
            notes: sampling_notes,
        },
        privacy: OperatorInsightsPrivacyPolicy {
            redaction_mode: "query_previews_and_sanitized_operator_samples".to_owned(),
            raw_queries_included: false,
            raw_error_messages_included: false,
            raw_config_values_included: false,
            secret_like_values_redacted: true,
        },
        operations,
        provider_health,
        security,
        recall,
        compaction,
        safety_boundary,
        plugins,
        cron,
        routines,
        memory_learning,
        reload,
    })
}

/// Fetches a bounded tape snapshot per sampled run and folds the events into
/// one aggregation; runs whose tape exceeds the per-run event cap are counted
/// as truncated for the sampling notes.
#[allow(clippy::result_large_err)]
async fn collect_operator_tape_aggregation(
    state: &AppState,
    sampled_runs: &[&journal::OrchestratorUsageInsightsRunRecord],
) -> Result<OperatorTapeAggregation, Response> {
    let mut aggregation = OperatorTapeAggregation::default();
    for run in sampled_runs {
        let tape = state
            .runtime
            .orchestrator_tape_snapshot(
                run.run_id.clone(),
                None,
                Some(OPERATOR_INSIGHTS_TAPE_SAMPLE_LIMIT),
            )
            .await
            .map_err(runtime_status_response)?;
        if tape.next_after_seq.is_some() {
            aggregation.truncated_tape_runs = aggregation.truncated_tape_runs.saturating_add(1);
        }
        for event in tape.events {
            accumulate_operator_tape_event(run, &event, &mut aggregation);
        }
    }
    Ok(aggregation)
}

/// Folds one tape event into the aggregation. Sample lists are capped at 8
/// entries each; counters keep accumulating past the cap. Events with
/// unparseable payloads are skipped (best-effort sampling).
fn accumulate_operator_tape_event(
    run: &journal::OrchestratorUsageInsightsRunRecord,
    event: &journal::OrchestratorTapeRecord,
    aggregation: &mut OperatorTapeAggregation,
) {
    let Ok(payload) = serde_json::from_str::<Value>(event.payload_json.as_str()) else {
        return;
    };

    match event.event_type.as_str() {
        "explicit_recall" => {
            let memory_hits = json_usize(payload.get("memory_hits"));
            let workspace_hits = json_usize(payload.get("workspace_hits"));
            let transcript_hits = json_usize(payload.get("transcript_hits"));
            let checkpoint_hits = json_usize(payload.get("checkpoint_hits"));
            let compaction_hits = json_usize(payload.get("compaction_hits"));
            let total_hits = memory_hits
                .saturating_add(workspace_hits)
                .saturating_add(transcript_hits)
                .saturating_add(checkpoint_hits)
                .saturating_add(compaction_hits);
            aggregation.explicit_recall_events =
                aggregation.explicit_recall_events.saturating_add(1);
            if total_hits == 0 {
                aggregation.explicit_recall_zero_hit_events =
                    aggregation.explicit_recall_zero_hit_events.saturating_add(1);
            }
            if aggregation.recall_samples.len() < 8 {
                aggregation.recall_samples.push(OperatorRecallSample {
                    run_id: run.run_id.clone(),
                    session_id: Some(run.session_id.clone()),
                    kind: "explicit_recall".to_owned(),
                    query_preview: operator_query_preview(
                        payload.get("query").and_then(Value::as_str).unwrap_or_default(),
                    ),
                    total_hits,
                    memory_hits,
                    workspace_hits,
                    transcript_hits,
                    checkpoint_hits,
                    compaction_hits,
                });
            }
        }
        "memory_auto_inject" => {
            let total_hits = payload
                .get("hits")
                .and_then(Value::as_array)
                .map(std::vec::Vec::len)
                .unwrap_or_else(|| json_usize(payload.get("injected_count")));
            aggregation.auto_inject_events = aggregation.auto_inject_events.saturating_add(1);
            aggregation.auto_inject_total_hits =
                aggregation.auto_inject_total_hits.saturating_add(total_hits as u64);
            if total_hits == 0 {
                aggregation.auto_inject_zero_hit_events =
                    aggregation.auto_inject_zero_hit_events.saturating_add(1);
            }
            if aggregation.recall_samples.len() < 8 {
                aggregation.recall_samples.push(OperatorRecallSample {
                    run_id: run.run_id.clone(),
                    session_id: Some(run.session_id.clone()),
                    kind: "memory_auto_inject".to_owned(),
                    query_preview: operator_query_preview(
                        payload.get("query").and_then(Value::as_str).unwrap_or_default(),
                    ),
                    total_hits,
                    memory_hits: total_hits,
                    workspace_hits: 0,
                    transcript_hits: 0,
                    checkpoint_hits: 0,
                    compaction_hits: 0,
                });
            }
        }
        "session.compaction.auto_preview"
        | "session.compaction.auto_created"
        | "session.compaction" => {
            // Generic "session.compaction" records carry the specific phase in
            // the payload's "event" field; classification below is substring
            // based so blocked previews count toward both preview and blocked.
            let event_name =
                payload.get("event").and_then(Value::as_str).unwrap_or(event.event_type.as_str());
            let estimated_input_tokens = json_u64(payload.get("estimated_input_tokens"));
            let estimated_output_tokens = json_u64(payload.get("estimated_output_tokens"));
            // Older tape payloads lack token_delta; reconstruct it from the
            // estimates. The clamp makes the u64 -> i64 narrowing lossless.
            let token_delta =
                payload.get("token_delta").and_then(Value::as_i64).unwrap_or_else(|| {
                    estimated_input_tokens
                        .saturating_sub(estimated_output_tokens)
                        .min(i64::MAX as u64) as i64
                });
            aggregation.compaction_events = aggregation.compaction_events.saturating_add(1);
            aggregation.compaction_total_input_tokens = aggregation
                .compaction_total_input_tokens
                .saturating_add(u128::from(estimated_input_tokens));
            aggregation.compaction_total_token_delta =
                aggregation.compaction_total_token_delta.saturating_add(i128::from(token_delta));
            if event_name.contains("preview") || event_name.contains("blocked") {
                aggregation.compaction_preview_events =
                    aggregation.compaction_preview_events.saturating_add(1);
            }
            if event_name.contains("created") || event_name.contains("applied") {
                aggregation.compaction_created_events =
                    aggregation.compaction_created_events.saturating_add(1);
            }
            if event_name.contains("blocked") {
                aggregation.compaction_blocked_events =
                    aggregation.compaction_blocked_events.saturating_add(1);
                if compaction_blocked_reason_requires_action(
                    payload.get("blocked_reason").and_then(Value::as_str),
                ) {
                    aggregation.compaction_actionable_blocked_events =
                        aggregation.compaction_actionable_blocked_events.saturating_add(1);
                }
            }
            if payload.get("dry_run").and_then(Value::as_bool).unwrap_or(false) {
                aggregation.compaction_dry_run_events =
                    aggregation.compaction_dry_run_events.saturating_add(1);
            }
            if aggregation.compaction_samples.len() < 8 {
                aggregation.compaction_samples.push(OperatorCompactionSample {
                    run_id: run.run_id.clone(),
                    session_id: Some(run.session_id.clone()),
                    trigger: event_name.to_owned(),
                    token_delta,
                    estimated_input_tokens,
                    estimated_output_tokens,
                    artifact_id: payload
                        .get("artifact_id")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned),
                });
            }
        }
        "tool_decision" => {
            aggregation.inspected_tool_decisions =
                aggregation.inspected_tool_decisions.saturating_add(1);
            let denied = payload.get("kind").and_then(Value::as_str) == Some("deny");
            let approval_required =
                payload.get("approval_required").and_then(Value::as_bool).unwrap_or(false);
            let policy_enforced =
                payload.get("policy_enforced").and_then(Value::as_bool).unwrap_or(false);
            if denied {
                aggregation.denied_tool_decisions =
                    aggregation.denied_tool_decisions.saturating_add(1);
                if policy_enforced {
                    aggregation.policy_enforced_denies =
                        aggregation.policy_enforced_denies.saturating_add(1);
                }
            }
            if approval_required {
                aggregation.approval_required_decisions =
                    aggregation.approval_required_decisions.saturating_add(1);
            }
            if denied && aggregation.safety_samples.len() < 8 {
                aggregation.safety_samples.push(OperatorSafetySample {
                    run_id: run.run_id.clone(),
                    tool_name: payload
                        .get("tool_name")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown")
                        .to_owned(),
                    reason: sanitize_http_error_message(
                        payload.get("reason").and_then(Value::as_str).unwrap_or("policy deny"),
                    ),
                    approval_required,
                });
            }
        }
        _ => {}
    }
}

fn build_operator_provider_health_insight(
    provider_snapshot: &crate::model_provider::ProviderStatusSnapshot,
    provider_auth_observability: &Value,
) -> OperatorProviderHealthInsight {
    let auth_state =
        provider_auth_observability.get("state").and_then(Value::as_str).unwrap_or("unknown");
    let cache_total = provider_snapshot
        .response_cache
        .hit_count
        .saturating_add(provider_snapshot.response_cache.miss_count);
    let response_cache_hit_rate_bps =
        ratio_bps_u64(provider_snapshot.response_cache.hit_count, cache_total);
    let (state, severity, recommended_action) = if auth_state != "ok"
        || model_provider_health_state(provider_snapshot) == "missing_auth"
    {
        (
            "blocking",
            "blocking",
            "Inspect auth profiles and provider credential wiring before trusting degraded model output."
                .to_owned(),
        )
    } else if provider_snapshot.circuit_breaker.open
        || provider_snapshot.runtime_metrics.error_rate_bps > 0
    {
        (
            "degraded",
            "warning",
            "Review provider health, failover posture, and cache coverage before widening usage."
                .to_owned(),
        )
    } else {
        ("ok", "info", "Provider health is within the expected operating envelope.".to_owned())
    };

    OperatorProviderHealthInsight {
        state: state.to_owned(),
        severity: severity.to_owned(),
        summary: format!(
            "{} provider auth={} error_rate_bps={} avg_latency_ms={} cache_hit_rate_bps={}",
            provider_snapshot.kind,
            auth_state,
            provider_snapshot.runtime_metrics.error_rate_bps,
            provider_snapshot.runtime_metrics.avg_latency_ms,
            response_cache_hit_rate_bps
        ),
        provider_kind: provider_snapshot.kind.clone(),
        error_rate_bps: provider_snapshot.runtime_metrics.error_rate_bps,
        avg_latency_ms: provider_snapshot.runtime_metrics.avg_latency_ms,
        circuit_open: provider_snapshot.circuit_breaker.open,
        auth_state: auth_state.to_owned(),
        refresh_failures: provider_auth_observability
            .get("refresh_failures")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        response_cache_enabled: provider_snapshot.response_cache.enabled,
        response_cache_entries: provider_snapshot.response_cache.entry_count,
        response_cache_hit_rate_bps,
        recommended_action,
        drill_down: operator_drill_down(
            "Provider and auth diagnostics",
            "operations",
            "/console/v1/diagnostics",
            "/control/operations",
        ),
    }
}

fn build_operator_recall_insight(aggregation: &OperatorTapeAggregation) -> OperatorRecallInsight {
    let explicit_recall_zero_hit_rate_bps =
        ratio_bps(aggregation.explicit_recall_zero_hit_events, aggregation.explicit_recall_events);
    let auto_inject_avg_hits = if aggregation.auto_inject_events == 0 {
        0.0
    } else {
        aggregation.auto_inject_total_hits as f64 / aggregation.auto_inject_events as f64
    };
    let (state, severity, recommended_action) = if aggregation.explicit_recall_events > 0
        && explicit_recall_zero_hit_rate_bps >= 5_000
    {
        (
            "degraded",
            "warning",
            "Inspect recall samples, workspace indexing, and durable memory coverage before trusting follow-up answers."
                .to_owned(),
        )
    } else if aggregation.auto_inject_events > 0 && auto_inject_avg_hits < 1.0 {
        (
            "degraded",
            "warning",
            "Memory auto-inject is finding too little context; review retrieval tuning and scoped memory coverage."
                .to_owned(),
        )
    } else {
        ("ok", "info", "Recall hit quality is stable in the sampled run window.".to_owned())
    };

    OperatorRecallInsight {
        state: state.to_owned(),
        severity: severity.to_owned(),
        summary: format!(
            "explicit_recall={} zero_hit_rate_bps={} memory_auto_inject={} avg_hits={:.2}",
            aggregation.explicit_recall_events,
            explicit_recall_zero_hit_rate_bps,
            aggregation.auto_inject_events,
            auto_inject_avg_hits
        ),
        explicit_recall_events: aggregation.explicit_recall_events,
        explicit_recall_zero_hit_events: aggregation.explicit_recall_zero_hit_events,
        explicit_recall_zero_hit_rate_bps,
        auto_inject_events: aggregation.auto_inject_events,
        auto_inject_zero_hit_events: aggregation.auto_inject_zero_hit_events,
        auto_inject_avg_hits,
        samples: aggregation.recall_samples.clone(),
        recommended_action,
        drill_down: operator_drill_down(
            "Recall diagnostics",
            "usage",
            "/console/v1/usage/insights",
            "/control/usage",
        ),
    }
}

fn build_operator_compaction_insight(
    aggregation: &OperatorTapeAggregation,
) -> OperatorCompactionInsight {
    let avg_token_delta = if aggregation.compaction_events == 0 {
        0
    } else {
        (aggregation.compaction_total_token_delta / aggregation.compaction_events as i128)
            .clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
    };
    // Reduction is reported in basis points of the estimated input tokens;
    // negative total deltas (compaction grew the context) clamp to zero.
    let avg_reduction_bps = ((aggregation.compaction_total_token_delta.max(0) as u128) * 10_000)
        .checked_div(aggregation.compaction_total_input_tokens)
        .unwrap_or(0)
        .min(u128::from(u32::MAX)) as u32;
    let unresolved_preview_without_outcome = aggregation.compaction_preview_events > 0
        && aggregation.compaction_created_events == 0
        && aggregation.compaction_dry_run_events == 0
        && aggregation.compaction_blocked_events == 0;
    let (state, severity, recommended_action) = if aggregation.compaction_actionable_blocked_events
        > 0
        || unresolved_preview_without_outcome
    {
        (
            "degraded",
            "warning",
            "Inspect compaction blockers and preview samples before relying on long-running session continuity."
                .to_owned(),
        )
    } else {
        (
            "ok",
            "info",
            "Automatic compaction is either quiet or behaving within the sampled window."
                .to_owned(),
        )
    };

    OperatorCompactionInsight {
        state: state.to_owned(),
        severity: severity.to_owned(),
        summary: format!(
            "preview_events={} created_events={} blocked_events={} actionable_blocked_events={} avg_reduction_bps={}",
            aggregation.compaction_preview_events,
            aggregation.compaction_created_events,
            aggregation.compaction_blocked_events,
            aggregation.compaction_actionable_blocked_events,
            avg_reduction_bps
        ),
        preview_events: aggregation.compaction_preview_events,
        created_events: aggregation.compaction_created_events,
        actionable_blocked_events: aggregation.compaction_actionable_blocked_events,
        dry_run_events: aggregation.compaction_dry_run_events,
        avg_token_delta,
        avg_reduction_bps,
        samples: aggregation.compaction_samples.clone(),
        recommended_action,
        drill_down: operator_drill_down(
            "Compaction diagnostics",
            "usage",
            "/console/v1/usage/insights",
            "/control/usage",
        ),
    }
}

fn compaction_blocked_reason_requires_action(reason: Option<&str>) -> bool {
    !matches!(reason.map(str::trim), Some("not_enough_history"))
}

fn build_operator_safety_boundary_insight(
    aggregation: &OperatorTapeAggregation,
) -> OperatorSafetyBoundaryInsight {
    let deny_rate_bps =
        ratio_bps(aggregation.denied_tool_decisions, aggregation.inspected_tool_decisions);
    let (state, severity, recommended_action) = if aggregation.denied_tool_decisions > 0 {
        (
            "degraded",
            "warning",
            "Review denied tool decisions and approval-required branches to confirm policy behavior matches operator intent."
                .to_owned(),
        )
    } else {
        ("ok", "info", "Sampled tool decisions did not surface policy-enforced denies.".to_owned())
    };

    OperatorSafetyBoundaryInsight {
        state: state.to_owned(),
        severity: severity.to_owned(),
        summary: format!(
            "inspected={} denied={} approval_required={} policy_enforced_denies={}",
            aggregation.inspected_tool_decisions,
            aggregation.denied_tool_decisions,
            aggregation.approval_required_decisions,
            aggregation.policy_enforced_denies
        ),
        inspected_tool_decisions: aggregation.inspected_tool_decisions,
        denied_tool_decisions: aggregation.denied_tool_decisions,
        policy_enforced_denies: aggregation.policy_enforced_denies,
        approval_required_decisions: aggregation.approval_required_decisions,
        deny_rate_bps,
        samples: aggregation.safety_samples.clone(),
        recommended_action,
        drill_down: operator_drill_down(
            "Safety and approval diagnostics",
            "operations",
            "/console/v1/diagnostics",
            "/control/operations",
        ),
    }
}

fn build_operator_plugin_insight(
    index: Option<&crate::plugins::PluginBindingsIndex>,
    load_error: Option<&str>,
) -> OperatorPluginInsight {
    let Some(index) = index else {
        return OperatorPluginInsight {
            state: "degraded".to_owned(),
            severity: "warning".to_owned(),
            summary: load_error
                .map(|error| format!("plugin operability is unavailable: {error}"))
                .unwrap_or_else(|| "plugin operability bindings are unavailable".to_owned()),
            total_bindings: 0,
            ready_bindings: 0,
            unhealthy_bindings: 0,
            typed_contract_failures: 0,
            config_failures: 0,
            discovery_failures: 0,
            samples: Vec::new(),
            recommended_action:
                "Rebuild or restore the plugin bindings index before relying on plugin insights."
                    .to_owned(),
            drill_down: operator_drill_down(
                "Plugin operability",
                "operations",
                "/console/v1/plugins",
                "/control/operations",
            ),
        };
    };

    let mut typed_contract_failures = 0_usize;
    let mut config_failures = 0_usize;
    let mut discovery_failures = 0_usize;
    let mut ready_bindings = 0_usize;
    let mut unhealthy_bindings = 0_usize;
    let mut samples = Vec::new();
    for entry in &index.entries {
        let discovery_state = plugin_discovery_state_label(entry);
        let config_state = plugin_config_state_label(entry);
        let typed_failed = entry.typed_contracts.mode
            == palyra_plugins_runtime::TypedPluginContractMode::Typed
            && (!entry.typed_contracts.ready
                || entry.typed_contracts.entries.iter().any(|contract| {
                    contract.status == palyra_plugins_runtime::TypedPluginContractStatus::Rejected
                }));
        let config_failed = !matches!(config_state.as_deref(), Some("valid") | Some("unknown"))
            && entry.config.is_some();
        let discovery_failed =
            entry.enabled && !matches!(discovery_state.as_str(), "installed" | "unknown");
        let capability_drift = !entry.capability_diff.valid;
        let ready = entry.enabled
            && !typed_failed
            && !config_failed
            && !discovery_failed
            && !capability_drift;
        if ready {
            ready_bindings = ready_bindings.saturating_add(1);
        } else {
            unhealthy_bindings = unhealthy_bindings.saturating_add(1);
        }
        if typed_failed {
            typed_contract_failures = typed_contract_failures.saturating_add(1);
        }
        if config_failed {
            config_failures = config_failures.saturating_add(1);
        }
        if discovery_failed {
            discovery_failures = discovery_failures.saturating_add(1);
        }

        if !ready && samples.len() < 8 {
            samples.push(OperatorPluginSample {
                plugin_id: entry.plugin_id.clone(),
                discovery_state: Some(discovery_state),
                config_state,
                contracts_mode: Some(
                    format!("{:?}", entry.typed_contracts.mode).to_ascii_lowercase(),
                ),
                reasons: collect_plugin_sample_reasons(entry),
            });
        }
    }

    let (state, severity, recommended_action) = if typed_contract_failures > 0
        || config_failures > 0
    {
        (
            "blocking",
            "blocking",
            "Fix typed contract or plugin config failures before enabling additional operator automation."
                .to_owned(),
        )
    } else if unhealthy_bindings > 0 || discovery_failures > 0 {
        (
            "degraded",
            "warning",
            "Review discovery and capability drift before assuming plugin routes are healthy."
                .to_owned(),
        )
    } else {
        ("ok", "info", "Plugin bindings look operable in the current bindings index.".to_owned())
    };

    OperatorPluginInsight {
        state: state.to_owned(),
        severity: severity.to_owned(),
        summary: format!(
            "bindings={} ready={} unhealthy={} typed_failures={} config_failures={}",
            index.entries.len(),
            ready_bindings,
            unhealthy_bindings,
            typed_contract_failures,
            config_failures
        ),
        total_bindings: index.entries.len(),
        ready_bindings,
        unhealthy_bindings,
        typed_contract_failures,
        config_failures,
        discovery_failures,
        samples,
        recommended_action,
        drill_down: operator_drill_down(
            "Plugin operability",
            "operations",
            "/console/v1/plugins",
            "/control/operations",
        ),
    }
}

fn build_operator_cron_insight(cron_runs: &[journal::CronRunRecord]) -> OperatorCronInsight {
    let total_runs = cron_runs.len();
    let failed_runs = cron_runs
        .iter()
        .filter(|run| {
            matches!(run.status, journal::CronRunStatus::Failed | journal::CronRunStatus::Denied)
        })
        .count();
    let succeeded_runs =
        cron_runs.iter().filter(|run| run.status == journal::CronRunStatus::Succeeded).count();
    let total_tool_denies = cron_runs.iter().map(|run| run.tool_denies).sum::<u64>();
    let success_rate_bps = ratio_bps(succeeded_runs, total_runs);
    let (state, severity, recommended_action) = if failed_runs > 0 || total_tool_denies > 0 {
        (
            "degraded",
            "warning",
            "Inspect routine runs and cron delivery history before widening unattended automation."
                .to_owned(),
        )
    } else {
        ("ok", "info", "Recent cron runs are not showing delivery or policy failures.".to_owned())
    };

    OperatorCronInsight {
        state: state.to_owned(),
        severity: severity.to_owned(),
        summary: format!(
            "total_runs={} failed_runs={} success_rate_bps={} tool_denies={}",
            total_runs, failed_runs, success_rate_bps, total_tool_denies
        ),
        total_runs,
        failed_runs,
        success_rate_bps,
        total_tool_denies,
        samples: cron_runs
            .iter()
            .take(8)
            .map(|run| OperatorCronRunSample {
                run_id: run.run_id.clone(),
                job_id: run.job_id.clone(),
                status: format!("{:?}", run.status).to_ascii_lowercase(),
                error_kind: run.error_kind.clone(),
                tool_denies: run.tool_denies,
            })
            .collect(),
        recommended_action,
        drill_down: operator_drill_down(
            "Cron delivery history",
            "cron",
            "/console/v1/routines",
            "/control/cron",
        ),
    }
}

fn build_operator_reload_insight(config_ref_health: Value) -> OperatorReloadInsight {
    let state = config_ref_health.get("state").and_then(Value::as_str).unwrap_or("unknown");
    let severity = config_ref_health
        .get("severity")
        .and_then(Value::as_str)
        .unwrap_or(if state == "ok" { "info" } else { "warning" });
    let summary = config_ref_health.get("summary").unwrap_or(&Value::Null);
    let hotspots = config_ref_health
        .get("items")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|item| {
            let item_severity = item.get("severity").and_then(Value::as_str).unwrap_or("info");
            if item_severity == "info" {
                return None;
            }
            Some(OperatorReloadHotspot {
                ref_id: item.get("ref_id").and_then(Value::as_str).unwrap_or("unknown").to_owned(),
                config_path: item
                    .get("config_path")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
                    .to_owned(),
                state: item.get("state").and_then(Value::as_str).unwrap_or("unknown").to_owned(),
                severity: item_severity.to_owned(),
                reload_mode: item
                    .get("reload_mode")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
                    .to_owned(),
                advice: item.get("advice").and_then(Value::as_str).map(ToOwned::to_owned),
            })
        })
        .take(8)
        .collect::<Vec<_>>();

    OperatorReloadInsight {
        state: state.to_owned(),
        severity: severity.to_owned(),
        summary: format!(
            "blocking_refs={} warning_refs={} active_runs={}",
            json_u64(summary.get("blocking_refs")),
            json_u64(summary.get("warning_refs")),
            json_u64(summary.get("active_runs"))
        ),
        blocking_refs: json_usize(summary.get("blocking_refs")),
        warning_refs: json_usize(summary.get("warning_refs")),
        hotspots,
        recommended_action: config_ref_health
            .get("recommendations")
            .and_then(Value::as_array)
            .and_then(|values| values.first())
            .and_then(Value::as_str)
            .unwrap_or("Inspect config ref health before applying runtime reload changes.")
            .to_owned(),
        drill_down: operator_drill_down(
            "Config ref health",
            "operations",
            "/console/v1/diagnostics",
            "/control/operations",
        ),
    }
}

fn build_operator_operations_overview_insight(
    counters: &crate::gateway::CountersSnapshot,
    runtime_decision: &crate::observability::RuntimeDecisionObservabilitySnapshot,
    lease_snapshot: &crate::provider_leases::ProviderLeaseManagerSnapshot,
    worker_snapshot: &palyra_workerd::WorkerFleetSnapshot,
    plugins: &OperatorPluginInsight,
    cron: &OperatorCronInsight,
) -> OperatorOperationsOverviewInsight {
    let stuck_runs = counters.active_orchestrator_runs();
    let queue_backlog =
        counters.channel_router_queue_depth.saturating_add(runtime_decision.metrics.queue_depth);
    let provider_cooldowns = lease_snapshot.credential_feedback.len();
    let routine_failures = cron.failed_runs;
    let plugin_errors = plugins.unhealthy_bindings;
    let worker_orphaned = usize_to_u64(worker_snapshot.orphaned_workers);
    let worker_failed_closed = usize_to_u64(worker_snapshot.failed_closed_workers);
    let (state, severity, recommended_action) = if worker_failed_closed > 0 {
        (
            "blocking",
            "blocking",
            "Quarantine or force-clean failed-closed workers before accepting remote execution.",
        )
    } else if stuck_runs > 0
        || provider_cooldowns > 0
        || queue_backlog > 0
        || routine_failures > 0
        || plugin_errors > 0
        || worker_orphaned > 0
    {
        (
            "degraded",
            "warning",
            "Inspect the busiest degraded subsystem before expanding unattended automation.",
        )
    } else {
        ("ok", "info", "Operations overview has no sampled degraded subsystems.")
    };
    OperatorOperationsOverviewInsight {
        state: state.to_owned(),
        severity: severity.to_owned(),
        summary: format!(
            "stuck_runs={} provider_cooldowns={} queue_backlog={} routine_failures={} plugin_errors={} worker_orphaned={} worker_failed_closed={}",
            stuck_runs,
            provider_cooldowns,
            queue_backlog,
            routine_failures,
            plugin_errors,
            worker_orphaned,
            worker_failed_closed
        ),
        stuck_runs,
        provider_cooldowns,
        queue_backlog,
        routine_failures,
        plugin_errors,
        worker_orphaned,
        worker_failed_closed,
        recommended_action: recommended_action.to_owned(),
        drill_down: operator_drill_down(
            "Operations overview",
            "operations",
            "/console/v1/system/insights",
            "/control/operations",
        ),
    }
}

fn build_operator_security_insight(
    counters: &crate::gateway::CountersSnapshot,
    aggregation: &OperatorTapeAggregation,
) -> OperatorSecurityInsight {
    let sandbox_violations = counters
        .sandbox_escape_attempts_blocked_workspace
        .saturating_add(counters.sandbox_escape_attempts_blocked_egress)
        .saturating_add(counters.sandbox_escape_attempts_blocked_executable);
    let policy_denies = counters
        .tool_decisions_denied
        .saturating_add(counters.sandbox_policy_denies)
        .saturating_add(counters.skill_execution_denied)
        .saturating_add(u64::try_from(aggregation.policy_enforced_denies).unwrap_or(u64::MAX));
    let approval_denies = counters.approvals_tool_resolved_deny;
    let (state, severity, recommended_action) = if sandbox_violations > 0 {
        (
            "blocking",
            "blocking",
            "Review sandbox violations and blocked escape attempts before continuing automation.",
        )
    } else if policy_denies > 0 || approval_denies > 0 || counters.journal_redacted_events > 0 {
        (
            "degraded",
            "warning",
            "Review policy denies, approval denies, and redaction events for policy drift or abuse.",
        )
    } else {
        ("ok", "info", "Security insights did not find sampled policy or sandbox anomalies.")
    };
    OperatorSecurityInsight {
        state: state.to_owned(),
        severity: severity.to_owned(),
        summary: format!(
            "approval_denies={} policy_denies={} redaction_events={} sandbox_violations={} skill_execution_denies={}",
            approval_denies,
            policy_denies,
            counters.journal_redacted_events,
            sandbox_violations,
            counters.skill_execution_denied
        ),
        approval_denies,
        policy_denies,
        redaction_events: counters.journal_redacted_events,
        sandbox_violations,
        skill_execution_denies: counters.skill_execution_denied,
        sampled_denied_tool_decisions: aggregation.denied_tool_decisions,
        recommended_action: recommended_action.to_owned(),
        drill_down: operator_drill_down(
            "Security and approvals",
            "operations",
            "/console/v1/diagnostics",
            "/control/operations",
        ),
    }
}

fn build_operator_routine_insight(
    cron_runs: &[journal::CronRunRecord],
    counters: &crate::gateway::CountersSnapshot,
) -> OperatorRoutineInsight {
    let total_runs = cron_runs.len();
    let failed_runs = cron_runs
        .iter()
        .filter(|run| {
            matches!(run.status, journal::CronRunStatus::Failed | journal::CronRunStatus::Denied)
        })
        .count();
    let succeeded_runs =
        cron_runs.iter().filter(|run| run.status == journal::CronRunStatus::Succeeded).count();
    let success_rate_bps = ratio_bps(succeeded_runs, total_runs);
    let policy_denies = cron_runs.iter().map(|run| run.tool_denies).sum::<u64>();
    let (state, severity, recommended_action) = if failed_runs > 0 || policy_denies > 0 {
        (
            "degraded",
            "warning",
            "Inspect routine failures and policy denies before widening scheduled automation.",
        )
    } else if counters.cron_runs_skipped > 0 {
        (
            "watching",
            "info",
            "Routine skips are present but no sampled failures require intervention.",
        )
    } else {
        ("ok", "info", "Routine delivery is healthy in the sampled window.")
    };
    OperatorRoutineInsight {
        state: state.to_owned(),
        severity: severity.to_owned(),
        summary: format!(
            "total_runs={} failed_runs={} skipped_runs={} policy_denies={} success_rate_bps={}",
            total_runs, failed_runs, counters.cron_runs_skipped, policy_denies, success_rate_bps
        ),
        total_runs,
        failed_runs,
        skipped_runs: counters.cron_runs_skipped,
        policy_denies,
        success_rate_bps,
        recommended_action: recommended_action.to_owned(),
        drill_down: operator_drill_down(
            "Routine delivery",
            "cron",
            "/console/v1/routines",
            "/control/cron",
        ),
    }
}

#[allow(clippy::result_large_err)]
async fn build_operator_memory_learning_insight(
    state: &AppState,
    resolved: &ResolvedUsageQuery,
) -> Result<OperatorMemoryLearningInsight, Response> {
    let counters = state.runtime.counters.snapshot();
    let candidates = state
        .runtime
        .list_learning_candidates(journal::LearningCandidateListFilter {
            candidate_id: None,
            owner_principal: Some(resolved.query.principal.clone()),
            device_id: Some(resolved.query.device_id.clone()),
            channel: resolved.query.channel.clone(),
            session_id: resolved.query.session_id.clone(),
            scope_kind: None,
            scope_id: None,
            candidate_kind: None,
            status: None,
            risk_level: None,
            source_task_id: None,
            min_confidence: None,
            max_confidence: None,
            limit: 256,
        })
        .await
        .map_err(runtime_status_response)?;

    let mut proposed_candidates = 0_usize;
    let mut needs_review_candidates = 0_usize;
    let mut approved_candidates = 0_usize;
    let mut rejected_candidates = 0_usize;
    let mut deployed_candidates = 0_usize;
    let mut rolled_back_candidates = 0_usize;
    let mut auto_applied_candidates = 0_usize;
    let mut injection_conflicts = 0_usize;
    let mut rollback_events = 0_usize;

    for candidate in &candidates {
        match learning_candidate_lifecycle_status(candidate.status.as_str(), candidate.auto_applied)
        {
            "proposed" => proposed_candidates = proposed_candidates.saturating_add(1),
            "needs_review" => needs_review_candidates = needs_review_candidates.saturating_add(1),
            "approved" => approved_candidates = approved_candidates.saturating_add(1),
            "rejected" => rejected_candidates = rejected_candidates.saturating_add(1),
            "deployed" => deployed_candidates = deployed_candidates.saturating_add(1),
            "rolled_back" => rolled_back_candidates = rolled_back_candidates.saturating_add(1),
            _ => {}
        }
        if candidate.auto_applied {
            auto_applied_candidates = auto_applied_candidates.saturating_add(1);
        }
        if candidate_mentions_injection_conflict(candidate) {
            injection_conflicts = injection_conflicts.saturating_add(1);
        }
        if candidate.status.eq_ignore_ascii_case("rolled-back")
            || candidate.status.eq_ignore_ascii_case("rolled_back")
            || candidate
                .last_action_summary
                .as_deref()
                .is_some_and(|summary| summary.to_ascii_lowercase().contains("rollback"))
        {
            rollback_events = rollback_events.saturating_add(1);
        }
    }

    let (state_label, severity, recommended_action) = if rolled_back_candidates > 0
        || rollback_events > 0
    {
        (
            "degraded",
            "warning",
            "Review rolled-back learning candidates and confirm the previous memory or routine state was restored.",
        )
    } else if needs_review_candidates > 0 || injection_conflicts > 0 {
        (
            "needs_review",
            "warning",
            "Review pending learning candidates and prompt-injection conflicts before deployment.",
        )
    } else {
        ("ok", "info", "Learning candidates are either quiet or already handled within policy.")
    };

    Ok(OperatorMemoryLearningInsight {
        state: state_label.to_owned(),
        severity: severity.to_owned(),
        summary: format!(
            "candidates={} proposed={} needs_review={} approved={} rejected={} deployed={} rolled_back={} auto_applied={} injection_conflicts={}",
            candidates.len(),
            proposed_candidates,
            needs_review_candidates,
            approved_candidates,
            rejected_candidates,
            deployed_candidates,
            rolled_back_candidates,
            auto_applied_candidates,
            injection_conflicts
        ),
        total_candidates: candidates.len(),
        proposed_candidates,
        needs_review_candidates,
        approved_candidates,
        rejected_candidates,
        deployed_candidates,
        rolled_back_candidates,
        auto_applied_candidates,
        memory_rejections: counters.memory_items_rejected,
        injection_conflicts,
        rollback_events,
        recommended_action: recommended_action.to_owned(),
        drill_down: operator_drill_down(
            "Memory learning review",
            "memory",
            "/console/v1/memory/learning/candidates",
            "/control/memory",
        ),
    })
}

/// Normalizes the free-form journal status into one lifecycle bucket.
fn learning_candidate_lifecycle_status(status: &str, auto_applied: bool) -> &'static str {
    let normalized = status.trim().to_ascii_lowercase().replace('_', "-");
    // An auto-applied candidate is live regardless of its recorded status.
    if auto_applied {
        return "deployed";
    }
    match normalized.as_str() {
        "" | "queued" | "proposed" => "proposed",
        "needs-review" | "review" | "pending-review" => "needs_review",
        "approved" | "accepted" => "approved",
        "rejected" | "suppressed" => "rejected",
        "deployed" | "auto-applied" => "deployed",
        "rolled-back" | "rollback" => "rolled_back",
        _ => "other",
    }
}

// Heuristic text scan: injection conflicts are not a structured field on
// learning candidates, so look for the known marker phrases anywhere in the
// candidate's descriptive and provenance text.
fn candidate_mentions_injection_conflict(candidate: &journal::LearningCandidateRecord) -> bool {
    [
        candidate.risk_level.as_str(),
        candidate.title.as_str(),
        candidate.summary.as_str(),
        candidate.content_json.as_str(),
        candidate.provenance_json.as_str(),
    ]
    .iter()
    .any(|value| {
        let normalized = value.to_ascii_lowercase();
        normalized.contains("prompt_injection")
            || normalized.contains("prompt-injection")
            || normalized.contains("injection conflict")
    })
}

/// Borrowed per-subsystem insights consumed by [`build_operator_hotspots`].
struct OperatorHotspotSources<'a> {
    operations: &'a OperatorOperationsOverviewInsight,
    provider_health: &'a OperatorProviderHealthInsight,
    security: &'a OperatorSecurityInsight,
    recall: &'a OperatorRecallInsight,
    compaction: &'a OperatorCompactionInsight,
    safety_boundary: &'a OperatorSafetyBoundaryInsight,
    plugins: &'a OperatorPluginInsight,
    cron: &'a OperatorCronInsight,
    routines: &'a OperatorRoutineInsight,
    memory_learning: &'a OperatorMemoryLearningInsight,
    reload: &'a OperatorReloadInsight,
}

/// Flattens the per-subsystem insights into the hotspot list, keeping only
/// warning/blocking entries; the iteration order below fixes the display
/// order in the console.
fn build_operator_hotspots(sources: OperatorHotspotSources<'_>) -> Vec<OperatorInsightHotspot> {
    let OperatorHotspotSources {
        operations,
        provider_health,
        security,
        recall,
        compaction,
        safety_boundary,
        plugins,
        cron,
        routines,
        memory_learning,
        reload,
    } = sources;
    let mut hotspots = Vec::new();
    for (hotspot_id, subsystem, state, severity, summary, detail, recommended_action, drill_down) in [
        (
            "operations_overview",
            "operations_overview",
            operations.state.as_str(),
            operations.severity.as_str(),
            operations.summary.as_str(),
            format!(
                "stuck_runs={} provider_cooldowns={} queue_backlog={} worker_failed_closed={}",
                operations.stuck_runs,
                operations.provider_cooldowns,
                operations.queue_backlog,
                operations.worker_failed_closed
            ),
            operations.recommended_action.clone(),
            operations.drill_down.clone(),
        ),
        (
            "provider_health",
            "provider_health",
            provider_health.state.as_str(),
            provider_health.severity.as_str(),
            provider_health.summary.as_str(),
            format!(
                "auth_state={} error_rate_bps={} avg_latency_ms={} cache_hit_rate_bps={}",
                provider_health.auth_state,
                provider_health.error_rate_bps,
                provider_health.avg_latency_ms,
                provider_health.response_cache_hit_rate_bps
            ),
            provider_health.recommended_action.clone(),
            provider_health.drill_down.clone(),
        ),
        (
            "security_posture",
            "security_posture",
            security.state.as_str(),
            security.severity.as_str(),
            security.summary.as_str(),
            format!(
                "approval_denies={} policy_denies={} sandbox_violations={}",
                security.approval_denies, security.policy_denies, security.sandbox_violations
            ),
            security.recommended_action.clone(),
            security.drill_down.clone(),
        ),
        (
            "recall_quality",
            "recall_quality",
            recall.state.as_str(),
            recall.severity.as_str(),
            recall.summary.as_str(),
            format!(
                "explicit_zero_hit_rate_bps={} auto_inject_avg_hits={:.2}",
                recall.explicit_recall_zero_hit_rate_bps, recall.auto_inject_avg_hits
            ),
            recall.recommended_action.clone(),
            recall.drill_down.clone(),
        ),
        (
            "compaction_efficiency",
            "compaction_efficiency",
            compaction.state.as_str(),
            compaction.severity.as_str(),
            compaction.summary.as_str(),
            format!(
                "preview_events={} created_events={} actionable_blocked_events={} avg_reduction_bps={}",
                compaction.preview_events,
                compaction.created_events,
                compaction.actionable_blocked_events,
                compaction.avg_reduction_bps
            ),
            compaction.recommended_action.clone(),
            compaction.drill_down.clone(),
        ),
        (
            "safety_boundary",
            "safety_boundary",
            safety_boundary.state.as_str(),
            safety_boundary.severity.as_str(),
            safety_boundary.summary.as_str(),
            format!(
                "deny_rate_bps={} approval_required={}",
                safety_boundary.deny_rate_bps, safety_boundary.approval_required_decisions
            ),
            safety_boundary.recommended_action.clone(),
            safety_boundary.drill_down.clone(),
        ),
        (
            "plugin_operability",
            "plugin_operability",
            plugins.state.as_str(),
            plugins.severity.as_str(),
            plugins.summary.as_str(),
            format!(
                "typed_failures={} config_failures={} discovery_failures={}",
                plugins.typed_contract_failures,
                plugins.config_failures,
                plugins.discovery_failures
            ),
            plugins.recommended_action.clone(),
            plugins.drill_down.clone(),
        ),
        (
            "cron_delivery",
            "cron_delivery",
            cron.state.as_str(),
            cron.severity.as_str(),
            cron.summary.as_str(),
            format!(
                "failed_runs={} success_rate_bps={} tool_denies={}",
                cron.failed_runs, cron.success_rate_bps, cron.total_tool_denies
            ),
            cron.recommended_action.clone(),
            cron.drill_down.clone(),
        ),
        (
            "routine_delivery",
            "routine_delivery",
            routines.state.as_str(),
            routines.severity.as_str(),
            routines.summary.as_str(),
            format!(
                "failed_runs={} skipped_runs={} policy_denies={}",
                routines.failed_runs, routines.skipped_runs, routines.policy_denies
            ),
            routines.recommended_action.clone(),
            routines.drill_down.clone(),
        ),
        (
            "memory_learning",
            "memory_learning",
            memory_learning.state.as_str(),
            memory_learning.severity.as_str(),
            memory_learning.summary.as_str(),
            format!(
                "needs_review={} deployed={} rolled_back={} injection_conflicts={}",
                memory_learning.needs_review_candidates,
                memory_learning.deployed_candidates,
                memory_learning.rolled_back_candidates,
                memory_learning.injection_conflicts
            ),
            memory_learning.recommended_action.clone(),
            memory_learning.drill_down.clone(),
        ),
        (
            "reload_health",
            "reload_health",
            reload.state.as_str(),
            reload.severity.as_str(),
            reload.summary.as_str(),
            format!("blocking_refs={} warning_refs={}", reload.blocking_refs, reload.warning_refs),
            reload.recommended_action.clone(),
            reload.drill_down.clone(),
        ),
    ] {
        if severity == "info" {
            continue;
        }
        hotspots.push(OperatorInsightHotspot {
            hotspot_id: hotspot_id.to_owned(),
            subsystem: subsystem.to_owned(),
            state: state.to_owned(),
            severity: severity.to_owned(),
            summary: summary.to_owned(),
            detail,
            recommended_action,
            drill_down,
        });
    }
    hotspots
}

/// Returns the overall `(state, severity)` pair: any blocking hotspot makes
/// the whole snapshot blocking, otherwise any warning makes it degraded.
fn build_operator_summary(hotspots: &[OperatorInsightHotspot]) -> (&'static str, &'static str) {
    if hotspots.iter().any(|hotspot| hotspot.severity == "blocking") {
        ("blocking", "blocking")
    } else if hotspots.iter().any(|hotspot| hotspot.severity == "warning") {
        ("degraded", "warning")
    } else {
        ("ok", "info")
    }
}

/// Loads the plugin bindings index, returning a sanitized error string
/// instead of failing so plugin insights can degrade gracefully.
fn load_operator_plugin_index() -> (Option<crate::plugins::PluginBindingsIndex>, Option<String>) {
    let plugins_root = match resolve_plugins_root() {
        Ok(path) => path,
        Err(error) => return (None, Some(sanitize_http_error_message(error.to_string().as_str()))),
    };
    match load_plugin_bindings_index(plugins_root.as_path()) {
        Ok(index) => (Some(index), None),
        Err(error) => (None, Some(sanitize_http_error_message(error.to_string().as_str()))),
    }
}

fn plugin_discovery_state_label(entry: &crate::plugins::PluginBindingRecord) -> String {
    serde_json::to_value(entry.discovery.state)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| "unknown".to_owned())
}

fn plugin_config_state_label(entry: &crate::plugins::PluginBindingRecord) -> Option<String> {
    entry.config.as_ref().and_then(|config| {
        serde_json::to_value(config.validation.state)
            .ok()
            .and_then(|value| value.as_str().map(str::to_owned))
    })
}

fn collect_plugin_sample_reasons(entry: &crate::plugins::PluginBindingRecord) -> Vec<String> {
    let mut reasons = Vec::new();
    if !entry.enabled {
        reasons.push("binding is disabled".to_owned());
    }
    reasons.extend(entry.discovery.reasons.iter().cloned());
    if let Some(config) = entry.config.as_ref() {
        reasons.extend(config.validation.issues.iter().cloned());
    }
    reasons.extend(entry.capability_diff.entries.iter().map(|issue| issue.message.clone()));
    if entry.typed_contracts.mode == palyra_plugins_runtime::TypedPluginContractMode::Typed {
        reasons.extend(
            entry
                .typed_contracts
                .entries
                .iter()
                .flat_map(|contract| contract.reasons.iter().cloned()),
        );
    }
    if reasons.is_empty() {
        reasons.push("binding did not satisfy the sampled operability checks".to_owned());
    }
    reasons.truncate(4);
    reasons
}

fn operator_drill_down(
    label: &str,
    section: &str,
    api_path: &str,
    console_path: &str,
) -> OperatorInsightDrillDown {
    OperatorInsightDrillDown {
        label: label.to_owned(),
        section: section.to_owned(),
        api_path: api_path.to_owned(),
        console_path: console_path.to_owned(),
    }
}

/// Produces a privacy-safe preview of a recall query: whitespace collapsed,
/// truncated to the preview byte budget, then secret patterns redacted.
fn operator_query_preview(raw: &str) -> String {
    let flattened = raw.replace(['\r', '\n', '\t'], " ");
    let collapsed = flattened.split_whitespace().collect::<Vec<_>>().join(" ");
    let truncated =
        crate::gateway::truncate_with_ellipsis(collapsed, OPERATOR_INSIGHTS_QUERY_PREVIEW_BYTES);
    sanitize_http_error_message(truncated.as_str())
}

// Ratio helpers report basis points (1/10_000) and treat a zero denominator
// as zero rather than an error, since "no events" is a normal quiet window.
fn ratio_bps(numerator: usize, denominator: usize) -> u32 {
    if denominator == 0 {
        0
    } else {
        ((numerator as u128) * 10_000 / (denominator as u128)).min(u128::from(u32::MAX)) as u32
    }
}

fn ratio_bps_u64(numerator: u64, denominator: u64) -> u32 {
    if denominator == 0 {
        0
    } else {
        ((numerator as u128) * 10_000 / (denominator as u128)).min(u128::from(u32::MAX)) as u32
    }
}

// Saturating usize -> u64 conversion (lossless on every supported target;
// the guard only matters for hypothetical >64-bit usize).
const fn usize_to_u64(value: usize) -> u64 {
    if value > u64::MAX as usize {
        u64::MAX
    } else {
        value as u64
    }
}

fn json_usize(value: Option<&Value>) -> usize {
    value.and_then(Value::as_u64).unwrap_or(0).min(usize::MAX as u64) as usize
}

fn json_u64(value: Option<&Value>) -> u64 {
    value.and_then(Value::as_u64).unwrap_or(0)
}

/// Applies the usage-window defaults: `end` falls back to now, `start` to
/// `end` minus the 30-day default lookback, and the span is capped at the
/// supported maximum. The query is always scoped to the requesting principal,
/// device, and channel.
#[allow(clippy::result_large_err)]
fn resolve_usage_query(
    start_at_unix_ms: Option<i64>,
    end_at_unix_ms: Option<i64>,
    bucket: Option<&str>,
    include_archived: bool,
    context: &gateway::RequestContext,
    session_id: Option<String>,
) -> Result<ResolvedUsageQuery, Response> {
    let now = current_unix_ms().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to resolve current time for usage query: {error}"
        )))
    })?;
    let end_at_unix_ms = end_at_unix_ms.unwrap_or(now);
    let start_at_unix_ms =
        start_at_unix_ms.unwrap_or(end_at_unix_ms.saturating_sub(DEFAULT_USAGE_LOOKBACK_MS));
    if end_at_unix_ms <= start_at_unix_ms {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "end_at_unix_ms must be greater than start_at_unix_ms",
        )));
    }
    if end_at_unix_ms.saturating_sub(start_at_unix_ms) > MAX_USAGE_LOOKBACK_MS {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "usage query lookback exceeds the maximum supported range",
        )));
    }
    let (bucket_label, bucket_width_ms) =
        normalize_usage_bucket(bucket, start_at_unix_ms, end_at_unix_ms)?;
    Ok(ResolvedUsageQuery {
        query: OrchestratorUsageQuery {
            start_at_unix_ms,
            end_at_unix_ms,
            bucket_width_ms,
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
            include_archived,
            session_id,
        },
        echo: UsageQueryEcho {
            start_at_unix_ms,
            end_at_unix_ms,
            bucket: bucket_label.to_owned(),
            bucket_width_ms,
            include_archived,
        },
    })
}

/// Resolves the bucket selector to a `(label, width_ms)` pair. `auto` picks
/// hour buckets for windows up to 72 hours (at most 72 timeline points) and
/// day buckets beyond that.
#[allow(clippy::result_large_err)]
fn normalize_usage_bucket(
    raw: Option<&str>,
    start_at_unix_ms: i64,
    end_at_unix_ms: i64,
) -> Result<(&'static str, i64), Response> {
    let lookback = end_at_unix_ms.saturating_sub(start_at_unix_ms);
    match raw.unwrap_or("auto").trim() {
        "" | "auto" => {
            if lookback <= 72 * HOUR_BUCKET_MS {
                Ok(("hour", HOUR_BUCKET_MS))
            } else {
                Ok(("day", DAY_BUCKET_MS))
            }
        }
        "hour" => Ok(("hour", HOUR_BUCKET_MS)),
        "day" => Ok(("day", DAY_BUCKET_MS)),
        _ => Err(runtime_status_response(tonic::Status::invalid_argument(
            "bucket must be one of auto|hour|day",
        ))),
    }
}

#[allow(clippy::result_large_err)]
fn normalize_usage_export_dataset(raw: &str) -> Result<UsageExportDataset, Response> {
    match raw.trim() {
        "timeline" => Ok(UsageExportDataset::Timeline),
        "sessions" => Ok(UsageExportDataset::Sessions),
        "agents" => Ok(UsageExportDataset::Agents),
        "models" => Ok(UsageExportDataset::Models),
        _ => Err(runtime_status_response(tonic::Status::invalid_argument(
            "dataset must be one of timeline|sessions|agents|models",
        ))),
    }
}

#[allow(clippy::result_large_err)]
fn normalize_usage_export_format(raw: &str) -> Result<UsageExportFormat, Response> {
    match raw.trim() {
        "json" => Ok(UsageExportFormat::Json),
        "csv" => Ok(UsageExportFormat::Csv),
        _ => Err(runtime_status_response(tonic::Status::invalid_argument(
            "format must be one of json|csv",
        ))),
    }
}

// The cursor is a plain numeric offset into the recomputed result list, not
// an opaque token; a missing or empty cursor means "start at the beginning".
#[allow(clippy::result_large_err)]
fn parse_usage_cursor(raw: Option<&str>) -> Result<usize, Response> {
    let Some(raw) = raw.map(str::trim) else {
        return Ok(0);
    };
    if raw.is_empty() {
        return Ok(0);
    }
    raw.parse::<usize>().map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "cursor must be an unsigned integer offset",
        ))
    })
}

/// Loads the agent bindings for the requesting principal plus the full agent
/// registry (paged at 100 per request) for session attribution.
async fn load_usage_metadata(
    state: &AppState,
    context: &gateway::RequestContext,
) -> Result<UsageMetadata, Response> {
    let bindings = state
        .runtime
        .list_agent_bindings(AgentBindingQuery {
            agent_id: None,
            principal: Some(context.principal.clone()),
            channel: context.channel.clone(),
            session_id: None,
            limit: Some(1_000),
        })
        .await
        .map_err(runtime_status_response)?;
    let mut agents = Vec::new();
    let mut after_agent_id = None::<String>;
    loop {
        let page = state
            .runtime
            .list_agents(after_agent_id.clone(), Some(100))
            .await
            .map_err(runtime_status_response)?;
        agents.extend(page.agents);
        let Some(next_after) = page.next_after_agent_id else {
            break;
        };
        after_agent_id = Some(next_after);
    }

    Ok(UsageMetadata {
        bindings_by_session: bindings
            .into_iter()
            .map(|binding| (binding.session_id.clone(), binding))
            .collect(),
        agents_by_id: agents.into_iter().map(|agent| (agent.agent_id.clone(), agent)).collect(),
    })
}

/// Groups session rollups by bound agent and sorts by total tokens, then
/// runs, then agent id, so the heaviest consumers list first.
fn build_usage_agent_rows(
    sessions: &[journal::OrchestratorUsageSessionRecord],
    metadata: &UsageMetadata,
) -> Vec<UsageAgentRecord> {
    let mut aggregates = HashMap::<String, UsageAgentAccumulator>::new();
    for session in sessions {
        let (agent_id, display_name, binding_source, default_model_profile) =
            resolve_usage_agent_identity(session, metadata);
        let entry = aggregates.entry(agent_id.clone()).or_insert_with(|| UsageAgentAccumulator {
            record: UsageAgentRecord {
                agent_id,
                display_name,
                binding_source,
                default_model_profile,
                session_count: 0,
                runs: 0,
                active_runs: 0,
                completed_runs: 0,
                prompt_tokens: 0,
                completion_tokens: 0,
                total_tokens: 0,
                average_latency_ms: None,
                latest_started_at_unix_ms: None,
                estimated_cost_usd: None,
            },
            latency_weighted_total_ms: 0,
        });
        entry.record.session_count += 1;
        entry.record.runs += session.runs;
        entry.record.active_runs += session.active_runs;
        entry.record.completed_runs += session.completed_runs;
        entry.record.prompt_tokens += session.prompt_tokens;
        entry.record.completion_tokens += session.completion_tokens;
        entry.record.total_tokens += session.total_tokens;
        entry.record.latest_started_at_unix_ms = latest_unix_ms(
            entry.record.latest_started_at_unix_ms,
            session.latest_started_at_unix_ms,
        );
        if let Some(average_latency_ms) = session.average_latency_ms {
            entry.latency_weighted_total_ms +=
                u128::from(average_latency_ms) * u128::from(session.completed_runs);
        }
    }

    let mut rows = aggregates.into_values().map(finalize_agent_accumulator).collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        right
            .total_tokens
            .cmp(&left.total_tokens)
            .then_with(|| right.runs.cmp(&left.runs))
            .then_with(|| left.agent_id.cmp(&right.agent_id))
    });
    rows
}

/// Groups session rollups by model profile with the same ordering as the
/// agent rows; the model key comes from the bound agent's default profile.
fn build_usage_model_rows(
    sessions: &[journal::OrchestratorUsageSessionRecord],
    metadata: &UsageMetadata,
) -> Vec<UsageModelRecord> {
    let mut aggregates = HashMap::<String, UsageModelAccumulator>::new();
    for session in sessions {
        let (model_id, display_name, model_source, agent_id) =
            resolve_usage_model_identity(session, metadata);
        let entry = aggregates.entry(model_id.clone()).or_insert_with(|| UsageModelAccumulator {
            record: UsageModelRecord {
                model_id,
                display_name,
                model_source,
                agent_count: 0,
                session_count: 0,
                runs: 0,
                active_runs: 0,
                completed_runs: 0,
                prompt_tokens: 0,
                completion_tokens: 0,
                total_tokens: 0,
                average_latency_ms: None,
                latest_started_at_unix_ms: None,
                estimated_cost_usd: None,
            },
            latency_weighted_total_ms: 0,
            agent_ids: HashSet::new(),
        });
        entry.record.session_count += 1;
        entry.record.runs += session.runs;
        entry.record.active_runs += session.active_runs;
        entry.record.completed_runs += session.completed_runs;
        entry.record.prompt_tokens += session.prompt_tokens;
        entry.record.completion_tokens += session.completion_tokens;
        entry.record.total_tokens += session.total_tokens;
        entry.record.latest_started_at_unix_ms = latest_unix_ms(
            entry.record.latest_started_at_unix_ms,
            session.latest_started_at_unix_ms,
        );
        if let Some(agent_id) = agent_id {
            entry.agent_ids.insert(agent_id);
        }
        if let Some(average_latency_ms) = session.average_latency_ms {
            entry.latency_weighted_total_ms +=
                u128::from(average_latency_ms) * u128::from(session.completed_runs);
        }
    }

    let mut rows = aggregates.into_values().map(finalize_model_accumulator).collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        right
            .total_tokens
            .cmp(&left.total_tokens)
            .then_with(|| right.runs.cmp(&left.runs))
            .then_with(|| left.model_id.cmp(&right.model_id))
    });
    rows
}

/// Returns `(agent_id, display_name, binding_source, default_model_profile)`
/// for a session. A binding whose agent record is gone (deleted agent) still
/// attributes usage to the agent id; unbound sessions collapse into the
/// shared "unassigned" row.
fn resolve_usage_agent_identity(
    session: &journal::OrchestratorUsageSessionRecord,
    metadata: &UsageMetadata,
) -> (String, String, String, Option<String>) {
    let binding = metadata.bindings_by_session.get(session.session_id.as_str());
    let agent = binding.and_then(|record| metadata.agents_by_id.get(record.agent_id.as_str()));
    match (binding, agent) {
        (Some(binding), Some(agent)) => (
            binding.agent_id.clone(),
            agent.display_name.clone(),
            "session_binding".to_owned(),
            normalize_usage_model_profile(agent.default_model_profile.as_str()),
        ),
        (Some(binding), None) => {
            (binding.agent_id.clone(), binding.agent_id.clone(), "session_binding".to_owned(), None)
        }
        (None, _) => {
            ("unassigned".to_owned(), "Unassigned".to_owned(), "unassigned".to_owned(), None)
        }
    }
}

/// Returns `(model_id, display_name, model_source, agent_id)` for a session.
/// Sessions without a resolvable agent (and therefore no default model
/// profile) collapse into the shared "unassigned" row.
fn resolve_usage_model_identity(
    session: &journal::OrchestratorUsageSessionRecord,
    metadata: &UsageMetadata,
) -> (String, String, String, Option<String>) {
    let binding = metadata.bindings_by_session.get(session.session_id.as_str());
    let agent = binding.and_then(|record| metadata.agents_by_id.get(record.agent_id.as_str()));
    match (binding, agent) {
        (Some(binding), Some(agent)) => {
            normalize_usage_model_profile(agent.default_model_profile.as_str())
                .map(|model_profile| {
                    (
                        model_profile.clone(),
                        model_profile,
                        "agent_default_model_profile".to_owned(),
                        Some(binding.agent_id.clone()),
                    )
                })
                .unwrap_or_else(|| {
                    (
                        "unassigned".to_owned(),
                        "Unassigned".to_owned(),
                        "unassigned".to_owned(),
                        None,
                    )
                })
        }
        _ => ("unassigned".to_owned(), "Unassigned".to_owned(), "unassigned".to_owned(), None),
    }
}

fn normalize_usage_model_profile(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

/// In-progress agent rollup; latency is accumulated as a completed-run
/// weighted total so the final average is run-weighted, not session-weighted.
#[derive(Debug)]
struct UsageAgentAccumulator {
    record: UsageAgentRecord,
    latency_weighted_total_ms: u128,
}

/// In-progress model rollup; tracks distinct agent ids for `agent_count`.
#[derive(Debug)]
struct UsageModelAccumulator {
    record: UsageModelRecord,
    latency_weighted_total_ms: u128,
    agent_ids: HashSet<String>,
}

fn finalize_agent_accumulator(mut aggregate: UsageAgentAccumulator) -> UsageAgentRecord {
    aggregate.record.average_latency_ms =
        weighted_latency(aggregate.latency_weighted_total_ms, aggregate.record.completed_runs);
    aggregate.record
}

fn finalize_model_accumulator(mut aggregate: UsageModelAccumulator) -> UsageModelRecord {
    aggregate.record.agent_count = aggregate.agent_ids.len() as u64;
    aggregate.record.average_latency_ms =
        weighted_latency(aggregate.latency_weighted_total_ms, aggregate.record.completed_runs);
    aggregate.record
}

// None (rather than zero) when nothing completed, so the JSON field is
// omitted instead of implying a measured zero-latency average.
fn weighted_latency(weighted_total_ms: u128, completed_runs: u64) -> Option<u64> {
    if completed_runs == 0 {
        return None;
    }
    Some((weighted_total_ms / u128::from(completed_runs)) as u64)
}

fn latest_unix_ms(current: Option<i64>, candidate: Option<i64>) -> Option<i64> {
    match (current, candidate) {
        (Some(current), Some(candidate)) => Some(current.max(candidate)),
        (Some(current), None) => Some(current),
        (None, Some(candidate)) => Some(candidate),
        (None, None) => None,
    }
}

#[allow(clippy::result_large_err)]
fn usage_json_export_response(
    dataset: &str,
    payload: serde_json::Value,
) -> Result<Response, Response> {
    let filename = format!("usage-{dataset}.json");
    let body = serde_json::to_string_pretty(&payload).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to serialize usage export JSON: {error}"
        )))
    })?;
    Ok((
        [
            (CONTENT_TYPE, HeaderValue::from_static("application/json; charset=utf-8")),
            (
                axum::http::header::CONTENT_DISPOSITION,
                HeaderValue::from_str(format!("attachment; filename=\"{filename}\"").as_str())
                    .map_err(|_| {
                        runtime_status_response(tonic::Status::internal(
                            "failed to build usage export content-disposition header",
                        ))
                    })?,
            ),
        ],
        body,
    )
        .into_response())
}

#[allow(clippy::result_large_err)]
fn usage_csv_export_response(dataset: &str, body: String) -> Result<Response, Response> {
    let filename = format!("usage-{dataset}.csv");
    Ok((
        [
            (CONTENT_TYPE, HeaderValue::from_static("text/csv; charset=utf-8")),
            (
                axum::http::header::CONTENT_DISPOSITION,
                HeaderValue::from_str(format!("attachment; filename=\"{filename}\"").as_str())
                    .map_err(|_| {
                        runtime_status_response(tonic::Status::internal(
                            "failed to build usage export content-disposition header",
                        ))
                    })?,
            ),
        ],
        body,
    )
        .into_response())
}

fn build_timeline_csv(rows: &[journal::OrchestratorUsageTimelineBucket]) -> String {
    let mut csv = String::from(
        "bucket_start_unix_ms,bucket_end_unix_ms,runs,session_count,active_runs,completed_runs,prompt_tokens,completion_tokens,total_tokens,average_latency_ms,estimated_cost_usd\n",
    );
    for row in rows {
        push_csv_row(
            &mut csv,
            &[
                row.bucket_start_unix_ms.to_string(),
                row.bucket_end_unix_ms.to_string(),
                row.runs.to_string(),
                row.session_count.to_string(),
                row.active_runs.to_string(),
                row.completed_runs.to_string(),
                row.prompt_tokens.to_string(),
                row.completion_tokens.to_string(),
                row.total_tokens.to_string(),
                optional_u64(row.average_latency_ms),
                optional_f64(row.estimated_cost_usd),
            ],
        );
    }
    csv
}

fn build_sessions_csv(rows: &[journal::OrchestratorUsageSessionRecord]) -> String {
    let mut csv = String::from(
        "session_id,session_key,session_label,archived,archived_at_unix_ms,last_run_id,runs,active_runs,completed_runs,prompt_tokens,completion_tokens,total_tokens,average_latency_ms,latest_started_at_unix_ms,estimated_cost_usd\n",
    );
    for row in rows {
        push_csv_row(
            &mut csv,
            &[
                row.session_id.clone(),
                row.session_key.clone(),
                row.session_label.clone().unwrap_or_default(),
                row.archived.to_string(),
                optional_i64(row.archived_at_unix_ms),
                row.last_run_id.clone().unwrap_or_default(),
                row.runs.to_string(),
                row.active_runs.to_string(),
                row.completed_runs.to_string(),
                row.prompt_tokens.to_string(),
                row.completion_tokens.to_string(),
                row.total_tokens.to_string(),
                optional_u64(row.average_latency_ms),
                optional_i64(row.latest_started_at_unix_ms),
                optional_f64(row.estimated_cost_usd),
            ],
        );
    }
    csv
}

fn build_agents_csv(rows: &[UsageAgentRecord]) -> String {
    let mut csv = String::from(
        "agent_id,display_name,binding_source,default_model_profile,session_count,runs,active_runs,completed_runs,prompt_tokens,completion_tokens,total_tokens,average_latency_ms,latest_started_at_unix_ms,estimated_cost_usd\n",
    );
    for row in rows {
        push_csv_row(
            &mut csv,
            &[
                row.agent_id.clone(),
                row.display_name.clone(),
                row.binding_source.clone(),
                row.default_model_profile.clone().unwrap_or_default(),
                row.session_count.to_string(),
                row.runs.to_string(),
                row.active_runs.to_string(),
                row.completed_runs.to_string(),
                row.prompt_tokens.to_string(),
                row.completion_tokens.to_string(),
                row.total_tokens.to_string(),
                optional_u64(row.average_latency_ms),
                optional_i64(row.latest_started_at_unix_ms),
                optional_f64(row.estimated_cost_usd),
            ],
        );
    }
    csv
}

fn build_models_csv(rows: &[UsageModelRecord]) -> String {
    let mut csv = String::from(
        "model_id,display_name,model_source,agent_count,session_count,runs,active_runs,completed_runs,prompt_tokens,completion_tokens,total_tokens,average_latency_ms,latest_started_at_unix_ms,estimated_cost_usd\n",
    );
    for row in rows {
        push_csv_row(
            &mut csv,
            &[
                row.model_id.clone(),
                row.display_name.clone(),
                row.model_source.clone(),
                row.agent_count.to_string(),
                row.session_count.to_string(),
                row.runs.to_string(),
                row.active_runs.to_string(),
                row.completed_runs.to_string(),
                row.prompt_tokens.to_string(),
                row.completion_tokens.to_string(),
                row.total_tokens.to_string(),
                optional_u64(row.average_latency_ms),
                optional_i64(row.latest_started_at_unix_ms),
                optional_f64(row.estimated_cost_usd),
            ],
        );
    }
    csv
}

fn push_csv_row(buffer: &mut String, values: &[String]) {
    let encoded = values.iter().map(|value| csv_escape(value.as_str())).collect::<Vec<_>>();
    buffer.push_str(encoded.join(",").as_str());
    buffer.push('\n');
}

fn csv_escape(value: &str) -> String {
    let escaped = neutralize_csv_formula(value).replace('"', "\"\"");
    format!("\"{escaped}\"")
}

// CSV-injection defense: cells starting with a formula trigger character get
// a leading apostrophe so spreadsheet apps treat them as text, not formulas.
fn neutralize_csv_formula(value: &str) -> String {
    if matches!(value.chars().next(), Some('=' | '+' | '-' | '@' | '\t' | '\r' | '\n')) {
        format!("'{value}")
    } else {
        value.to_owned()
    }
}

fn optional_u64(value: Option<u64>) -> String {
    value.map(|entry| entry.to_string()).unwrap_or_default()
}

fn optional_i64(value: Option<i64>) -> String {
    value.map(|entry| entry.to_string()).unwrap_or_default()
}

fn optional_f64(value: Option<f64>) -> String {
    value.map(|entry| format!("{entry:.6}")).unwrap_or_default()
}

fn current_unix_ms() -> Result<i64, std::time::SystemTimeError> {
    // Intentional narrowing: milliseconds since the epoch fit in i64 far
    // beyond any realistic clock value.
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64)
}

#[cfg(test)]
mod tests {
    use super::{
        build_operator_compaction_insight, build_operator_hotspots, build_operator_summary,
        csv_escape, operator_drill_down, operator_query_preview, usage_cost_tracking_available,
        OperatorCompactionInsight, OperatorCronInsight, OperatorHotspotSources,
        OperatorMemoryLearningInsight, OperatorOperationsOverviewInsight, OperatorPluginInsight,
        OperatorProviderHealthInsight, OperatorRecallInsight, OperatorReloadInsight,
        OperatorRoutineInsight, OperatorSafetyBoundaryInsight, OperatorSecurityInsight,
        OperatorTapeAggregation,
    };

    #[test]
    fn csv_escape_neutralizes_formula_prefixes() {
        assert_eq!(csv_escape("=SUM(A1:A2)"), "\"'=SUM(A1:A2)\"");
        assert_eq!(csv_escape("+cmd"), "\"'+cmd\"");
        assert_eq!(csv_escape("-10"), "\"'-10\"");
        assert_eq!(csv_escape("@user"), "\"'@user\"");
        assert_eq!(csv_escape("safe"), "\"safe\"");
    }

    #[test]
    fn operator_summary_prioritizes_blocking_hotspots() {
        let warning = test_provider_health("degraded", "warning", "provider warning");
        let blocking = test_reload("blocking", "blocking", "reload blocking");
        let operations = test_operations("ok", "info", "operations ok");
        let security = test_security("ok", "info", "security ok");
        let recall = test_recall("ok", "info", "recall ok");
        let compaction = test_compaction("ok", "info", "compaction ok");
        let safety_boundary = test_safety("ok", "info", "safety ok");
        let plugins = test_plugins("ok", "info", "plugins ok");
        let cron = test_cron("ok", "info", "cron ok");
        let routines = test_routines("ok", "info", "routines ok");
        let memory_learning = test_memory_learning("ok", "info", "memory learning ok");
        let hotspots = build_operator_hotspots(OperatorHotspotSources {
            operations: &operations,
            provider_health: &warning,
            security: &security,
            recall: &recall,
            compaction: &compaction,
            safety_boundary: &safety_boundary,
            plugins: &plugins,
            cron: &cron,
            routines: &routines,
            memory_learning: &memory_learning,
            reload: &blocking,
        });
        assert_eq!(build_operator_summary(hotspots.as_slice()), ("blocking", "blocking"));
    }

    #[test]
    fn operator_query_preview_redacts_and_truncates() {
        let preview = operator_query_preview(
            "Authorization: Bearer sk-live-super-secret\nwith several words to force truncation after normalization.",
        );
        assert!(!preview.contains("sk-live-super-secret"));
        assert!(!preview.contains('\n'));
        assert!(preview.len() <= 240);
    }

    #[test]
    fn cost_tracking_available_accepts_any_window_cost() {
        assert!(usage_cost_tracking_available([None, Some(0.0), None]));
        assert!(!usage_cost_tracking_available([None, None]));
    }

    #[test]
    fn compaction_average_uses_observed_event_count() {
        let aggregation = OperatorTapeAggregation {
            compaction_events: 3,
            compaction_total_token_delta: 90,
            compaction_total_input_tokens: 300,
            ..OperatorTapeAggregation::default()
        };

        let insight = build_operator_compaction_insight(&aggregation);

        assert_eq!(insight.avg_token_delta, 30);
    }

    #[test]
    fn compaction_not_enough_history_blockers_do_not_degrade_insight() {
        let aggregation = OperatorTapeAggregation {
            compaction_preview_events: 8,
            compaction_blocked_events: 8,
            compaction_actionable_blocked_events: 0,
            compaction_events: 8,
            ..OperatorTapeAggregation::default()
        };

        let insight = build_operator_compaction_insight(&aggregation);

        assert_eq!(insight.state, "ok");
        assert_eq!(insight.severity, "info");
        assert!(insight.summary.contains("actionable_blocked_events=0"));
    }

    #[test]
    fn compaction_actionable_blockers_degrade_insight() {
        let aggregation = OperatorTapeAggregation {
            compaction_preview_events: 1,
            compaction_blocked_events: 1,
            compaction_actionable_blocked_events: 1,
            compaction_events: 1,
            ..OperatorTapeAggregation::default()
        };

        let insight = build_operator_compaction_insight(&aggregation);

        assert_eq!(insight.state, "degraded");
        assert_eq!(insight.severity, "warning");
    }

    fn test_operations(
        state: &str,
        severity: &str,
        summary: &str,
    ) -> OperatorOperationsOverviewInsight {
        OperatorOperationsOverviewInsight {
            state: state.to_owned(),
            severity: severity.to_owned(),
            summary: summary.to_owned(),
            stuck_runs: 0,
            provider_cooldowns: 0,
            queue_backlog: 0,
            routine_failures: 0,
            plugin_errors: 0,
            worker_orphaned: 0,
            worker_failed_closed: 0,
            recommended_action: "Inspect operations overview.".to_owned(),
            drill_down: operator_drill_down(
                "Operations overview",
                "operations",
                "/console/v1/system/insights",
                "/control/operations",
            ),
        }
    }

    fn test_provider_health(
        state: &str,
        severity: &str,
        summary: &str,
    ) -> OperatorProviderHealthInsight {
        OperatorProviderHealthInsight {
            state: state.to_owned(),
            severity: severity.to_owned(),
            summary: summary.to_owned(),
            provider_kind: "deterministic".to_owned(),
            error_rate_bps: 0,
            avg_latency_ms: 0,
            circuit_open: false,
            auth_state: "ok".to_owned(),
            refresh_failures: 0,
            response_cache_enabled: true,
            response_cache_entries: 1,
            response_cache_hit_rate_bps: 0,
            recommended_action: "Inspect provider diagnostics.".to_owned(),
            drill_down: operator_drill_down(
                "Provider and auth diagnostics",
                "operations",
                "/console/v1/diagnostics",
                "/control/operations",
            ),
        }
    }

    fn test_security(state: &str, severity: &str, summary: &str) -> OperatorSecurityInsight {
        OperatorSecurityInsight {
            state: state.to_owned(),
            severity: severity.to_owned(),
            summary: summary.to_owned(),
            approval_denies: 0,
            policy_denies: 0,
            redaction_events: 0,
            sandbox_violations: 0,
            skill_execution_denies: 0,
            sampled_denied_tool_decisions: 0,
            recommended_action: "Inspect security posture.".to_owned(),
            drill_down: operator_drill_down(
                "Security and approvals",
                "operations",
                "/console/v1/diagnostics",
                "/control/operations",
            ),
        }
    }

    fn test_recall(state: &str, severity: &str, summary: &str) -> OperatorRecallInsight {
        OperatorRecallInsight {
            state: state.to_owned(),
            severity: severity.to_owned(),
            summary: summary.to_owned(),
            explicit_recall_events: 0,
            explicit_recall_zero_hit_events: 0,
            explicit_recall_zero_hit_rate_bps: 0,
            auto_inject_events: 0,
            auto_inject_zero_hit_events: 0,
            auto_inject_avg_hits: 0.0,
            samples: Vec::new(),
            recommended_action: "Inspect recall diagnostics.".to_owned(),
            drill_down: operator_drill_down(
                "Recall diagnostics",
                "usage",
                "/console/v1/usage/insights",
                "/control/usage",
            ),
        }
    }

    fn test_compaction(state: &str, severity: &str, summary: &str) -> OperatorCompactionInsight {
        OperatorCompactionInsight {
            state: state.to_owned(),
            severity: severity.to_owned(),
            summary: summary.to_owned(),
            preview_events: 0,
            created_events: 0,
            actionable_blocked_events: 0,
            dry_run_events: 0,
            avg_token_delta: 0,
            avg_reduction_bps: 0,
            samples: Vec::new(),
            recommended_action: "Inspect compaction diagnostics.".to_owned(),
            drill_down: operator_drill_down(
                "Compaction diagnostics",
                "usage",
                "/console/v1/usage/insights",
                "/control/usage",
            ),
        }
    }

    fn test_safety(state: &str, severity: &str, summary: &str) -> OperatorSafetyBoundaryInsight {
        OperatorSafetyBoundaryInsight {
            state: state.to_owned(),
            severity: severity.to_owned(),
            summary: summary.to_owned(),
            inspected_tool_decisions: 0,
            denied_tool_decisions: 0,
            policy_enforced_denies: 0,
            approval_required_decisions: 0,
            deny_rate_bps: 0,
            samples: Vec::new(),
            recommended_action: "Inspect safety diagnostics.".to_owned(),
            drill_down: operator_drill_down(
                "Safety and approval diagnostics",
                "operations",
                "/console/v1/diagnostics",
                "/control/operations",
            ),
        }
    }

    fn test_plugins(state: &str, severity: &str, summary: &str) -> OperatorPluginInsight {
        OperatorPluginInsight {
            state: state.to_owned(),
            severity: severity.to_owned(),
            summary: summary.to_owned(),
            total_bindings: 0,
            ready_bindings: 0,
            unhealthy_bindings: 0,
            typed_contract_failures: 0,
            config_failures: 0,
            discovery_failures: 0,
            samples: Vec::new(),
            recommended_action: "Inspect plugin operability.".to_owned(),
            drill_down: operator_drill_down(
                "Plugin operability",
                "operations",
                "/console/v1/plugins",
                "/control/operations",
            ),
        }
    }

    fn test_cron(state: &str, severity: &str, summary: &str) -> OperatorCronInsight {
        OperatorCronInsight {
            state: state.to_owned(),
            severity: severity.to_owned(),
            summary: summary.to_owned(),
            total_runs: 0,
            failed_runs: 0,
            success_rate_bps: 0,
            total_tool_denies: 0,
            samples: Vec::new(),
            recommended_action: "Inspect cron delivery.".to_owned(),
            drill_down: operator_drill_down(
                "Cron delivery history",
                "cron",
                "/console/v1/routines",
                "/control/cron",
            ),
        }
    }

    fn test_routines(state: &str, severity: &str, summary: &str) -> OperatorRoutineInsight {
        OperatorRoutineInsight {
            state: state.to_owned(),
            severity: severity.to_owned(),
            summary: summary.to_owned(),
            total_runs: 0,
            failed_runs: 0,
            skipped_runs: 0,
            policy_denies: 0,
            success_rate_bps: 0,
            recommended_action: "Inspect routine delivery.".to_owned(),
            drill_down: operator_drill_down(
                "Routine delivery",
                "cron",
                "/console/v1/routines",
                "/control/cron",
            ),
        }
    }

    fn test_memory_learning(
        state: &str,
        severity: &str,
        summary: &str,
    ) -> OperatorMemoryLearningInsight {
        OperatorMemoryLearningInsight {
            state: state.to_owned(),
            severity: severity.to_owned(),
            summary: summary.to_owned(),
            total_candidates: 0,
            proposed_candidates: 0,
            needs_review_candidates: 0,
            approved_candidates: 0,
            rejected_candidates: 0,
            deployed_candidates: 0,
            rolled_back_candidates: 0,
            auto_applied_candidates: 0,
            memory_rejections: 0,
            injection_conflicts: 0,
            rollback_events: 0,
            recommended_action: "Inspect memory learning.".to_owned(),
            drill_down: operator_drill_down(
                "Memory learning review",
                "memory",
                "/console/v1/memory/learning/candidates",
                "/control/memory",
            ),
        }
    }

    fn test_reload(state: &str, severity: &str, summary: &str) -> OperatorReloadInsight {
        OperatorReloadInsight {
            state: state.to_owned(),
            severity: severity.to_owned(),
            summary: summary.to_owned(),
            blocking_refs: 0,
            warning_refs: 0,
            hotspots: Vec::new(),
            recommended_action: "Inspect config ref health.".to_owned(),
            drill_down: operator_drill_down(
                "Config ref health",
                "operations",
                "/console/v1/diagnostics",
                "/control/operations",
            ),
        }
    }
}
