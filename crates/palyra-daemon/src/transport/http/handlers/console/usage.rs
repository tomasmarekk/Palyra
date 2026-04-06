use std::{
    collections::{HashMap, HashSet},
    time::{SystemTime, UNIX_EPOCH},
};

use axum::http::{header::CONTENT_TYPE, HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::diagnostics::{build_page_info, contract_descriptor};
use crate::agents::{AgentBindingQuery, AgentRecord, SessionAgentBinding};
use crate::journal::{self, OrchestratorUsageQuery};
use crate::usage_governance::{
    build_alert_candidates, build_model_mix, build_scope_mix, build_tool_mix,
    estimate_cost_for_model, evaluate_budget_policies, load_budget_override_approvals,
    request_usage_budget_override, PricingEstimate, UsageBudgetEvaluation, UsageModelMixRecord,
    UsageScopeMixRecord, UsageToolMixRecord,
};
use crate::*;

const DEFAULT_USAGE_LOOKBACK_MS: i64 = 30 * 24 * 60 * 60 * 1_000;
const MAX_USAGE_LOOKBACK_MS: i64 = 366 * 24 * 60 * 60 * 1_000;
const HOUR_BUCKET_MS: i64 = 60 * 60 * 1_000;
const DAY_BUCKET_MS: i64 = 24 * 60 * 60 * 1_000;
const DEFAULT_USAGE_BREAKDOWN_LIMIT: usize = 10;
const MAX_USAGE_BREAKDOWN_LIMIT: usize = 50;
const DEFAULT_USAGE_RUN_LIMIT: usize = 12;
const MAX_USAGE_RUN_LIMIT: usize = 25;

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

#[derive(Debug, Deserialize)]
pub(crate) struct ConsoleUsageBudgetOverrideRequest {
    #[serde(default)]
    reason: Option<String>,
}

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

#[derive(Debug, Serialize)]
pub(crate) struct UsageSummaryEnvelope {
    contract: control_plane::ContractDescriptor,
    query: UsageQueryEcho,
    totals: journal::OrchestratorUsageTotals,
    timeline: Vec<journal::OrchestratorUsageTimelineBucket>,
    cost_tracking_available: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct UsageSessionsEnvelope {
    contract: control_plane::ContractDescriptor,
    query: UsagePaginationQueryEcho,
    sessions: Vec<journal::OrchestratorUsageSessionRecord>,
    page: control_plane::PageInfo,
    cost_tracking_available: bool,
}

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

#[derive(Debug, Serialize)]
pub(crate) struct UsageAgentsEnvelope {
    contract: control_plane::ContractDescriptor,
    query: UsagePaginationQueryEcho,
    agents: Vec<UsageAgentRecord>,
    page: control_plane::PageInfo,
    cost_tracking_available: bool,
}

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

#[derive(Debug, Serialize)]
pub(crate) struct UsageModelsEnvelope {
    contract: control_plane::ContractDescriptor,
    query: UsagePaginationQueryEcho,
    models: Vec<UsageModelRecord>,
    page: control_plane::PageInfo,
    cost_tracking_available: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct UsageInsightsPricingSummary {
    known_entries: usize,
    estimated_models: usize,
    estimate_only: bool,
}

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

#[derive(Debug, Serialize)]
pub(crate) struct UsageInsightsRoutingSummary {
    default_mode: String,
    suggest_runs: usize,
    dry_run_runs: usize,
    enforced_runs: usize,
    overrides: usize,
    recent_decisions: Vec<journal::UsageRoutingDecisionRecord>,
}

#[derive(Debug, Serialize)]
pub(crate) struct UsageInsightsBudgetsEnvelope {
    policies: Vec<journal::UsageBudgetPolicyRecord>,
    evaluations: Vec<UsageBudgetEvaluation>,
}

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
    cost_tracking_available: bool,
}

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
    let next_cursor =
        (cursor.saturating_add(limit) < sessions.len()).then(|| (cursor + limit).to_string());
    let page =
        build_page_info(limit, sessions.len().saturating_sub(cursor).min(limit), next_cursor);
    let sessions = sessions.into_iter().skip(cursor).take(limit).collect::<Vec<_>>();
    let cost_tracking_available = sessions.iter().any(|entry| entry.estimated_cost_usd.is_some());

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
    let detail = state
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
        session: detail.0,
        totals: summary.totals,
        timeline: summary.timeline,
        runs: detail.1,
        cost_tracking_available: summary.cost_tracking_available,
    }))
}

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
    let next_cursor =
        (cursor.saturating_add(limit) < rows.len()).then(|| (cursor + limit).to_string());
    let page = build_page_info(limit, rows.len().saturating_sub(cursor).min(limit), next_cursor);
    let agents = rows.into_iter().skip(cursor).take(limit).collect::<Vec<_>>();
    let cost_tracking_available = agents.iter().any(|entry| entry.estimated_cost_usd.is_some());

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
    let next_cursor =
        (cursor.saturating_add(limit) < rows.len()).then(|| (cursor + limit).to_string());
    let page = build_page_info(limit, rows.len().saturating_sub(cursor).min(limit), next_cursor);
    let models = rows.into_iter().skip(cursor).take(limit).collect::<Vec<_>>();
    let cost_tracking_available = models.iter().any(|entry| entry.estimated_cost_usd.is_some());

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

pub(crate) async fn console_usage_insights_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleUsageSummaryQuery>,
) -> Result<Json<UsageInsightsEnvelope>, Response> {
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
    let provider_kind = status_snapshot.model_provider.kind.clone();
    let provider_id = if provider_kind == "openai_compatible" {
        "openai".to_owned()
    } else {
        "palyra".to_owned()
    };
    let default_model_id = status_snapshot
        .model_provider
        .openai_model
        .clone()
        .unwrap_or_else(|| "deterministic".to_owned());

    let routing_by_run = routing_decisions
        .iter()
        .map(|record| (record.run_id.clone(), record))
        .collect::<HashMap<_, _>>();
    let enriched_runs = runs
        .iter()
        .map(|run| {
            let routing = routing_by_run.get(run.run_id.as_str()).copied();
            let model_id = routing
                .map(|record| record.actual_model_id.as_str())
                .unwrap_or(default_model_id.as_str());
            let provider_kind_value = routing
                .map(|record| record.provider_kind.as_str())
                .unwrap_or(provider_kind.as_str());
            let provider_id_value =
                routing.map(|record| record.provider_id.as_str()).unwrap_or(provider_id.as_str());
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
    let approved_subjects =
        load_budget_override_approvals(&state.runtime, budget_policies.as_slice()).await;
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
    for (index, candidate) in alert_candidates.iter().enumerate() {
        let _ = state
            .runtime
            .upsert_usage_alert(journal::UsageAlertUpsertRequest {
                alert_id: format!("{}-{index}", Ulid::new()),
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
        cost_tracking_available,
    }))
}

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

#[derive(Debug, Clone)]
struct ResolvedUsageQuery {
    query: OrchestratorUsageQuery,
    echo: UsageQueryEcho,
}

#[derive(Debug, Clone)]
struct UsageMetadata {
    bindings_by_session: HashMap<String, SessionAgentBinding>,
    agents_by_id: HashMap<String, AgentRecord>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UsageExportDataset {
    Timeline,
    Sessions,
    Agents,
    Models,
}

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
            Some(agent.default_model_profile.clone()),
        ),
        (Some(binding), None) => {
            (binding.agent_id.clone(), binding.agent_id.clone(), "session_binding".to_owned(), None)
        }
        (None, _) => {
            ("unassigned".to_owned(), "Unassigned".to_owned(), "unassigned".to_owned(), None)
        }
    }
}

fn resolve_usage_model_identity(
    session: &journal::OrchestratorUsageSessionRecord,
    metadata: &UsageMetadata,
) -> (String, String, String, Option<String>) {
    let binding = metadata.bindings_by_session.get(session.session_id.as_str());
    let agent = binding.and_then(|record| metadata.agents_by_id.get(record.agent_id.as_str()));
    match (binding, agent) {
        (Some(binding), Some(agent)) => (
            agent.default_model_profile.clone(),
            agent.default_model_profile.clone(),
            "agent_default_model_profile".to_owned(),
            Some(binding.agent_id.clone()),
        ),
        _ => ("unassigned".to_owned(), "Unassigned".to_owned(), "unassigned".to_owned(), None),
    }
}

#[derive(Debug)]
struct UsageAgentAccumulator {
    record: UsageAgentRecord,
    latency_weighted_total_ms: u128,
}

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
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64)
}

#[cfg(test)]
mod tests {
    use super::csv_escape;

    #[test]
    fn csv_escape_neutralizes_formula_prefixes() {
        assert_eq!(csv_escape("=SUM(A1:A2)"), "\"'=SUM(A1:A2)\"");
        assert_eq!(csv_escape("+cmd"), "\"'+cmd\"");
        assert_eq!(csv_escape("-10"), "\"'-10\"");
        assert_eq!(csv_escape("@user"), "\"'@user\"");
        assert_eq!(csv_escape("safe"), "\"safe\"");
    }
}
