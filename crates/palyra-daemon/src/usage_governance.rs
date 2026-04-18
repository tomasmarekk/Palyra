use std::{collections::HashMap, sync::Arc};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tonic::Status;
use ulid::Ulid;

use crate::journal::{
    ApprovalCreateRequest, ApprovalDecision, ApprovalDecisionScope, ApprovalPolicySnapshot,
    ApprovalPromptOption, ApprovalPromptRecord, ApprovalRecord, ApprovalRiskLevel,
    ApprovalSubjectType, OrchestratorUsageInsightsRunRecord, UsageBudgetPolicyRecord,
    UsagePricingRecord, UsageRoutingDecisionCreateRequest, UsageRoutingDecisionRecord,
};
use crate::{
    gateway::GatewayRuntimeState,
    model_provider::{
        ProviderRegistryModelSnapshot, ProviderRegistryProviderSnapshot, ProviderStatusSnapshot,
    },
    orchestrator::estimate_token_count,
    transport::grpc::auth::RequestContext,
};

const ALERT_MIN_COST_SPIKE_USD: f64 = 0.50;
pub(crate) const USAGE_BUDGET_SUBJECT_PREFIX: &str = "usage-budget:";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RoutingMode {
    Suggest,
    DryRun,
    Enforced,
}

impl RoutingMode {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Suggest => "suggest",
            Self::DryRun => "dry_run",
            Self::Enforced => "enforced",
        }
    }

    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "suggest" => Some(Self::Suggest),
            "dry_run" | "dry-run" => Some(Self::DryRun),
            "enforced" => Some(Self::Enforced),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct SmartRoutingRuntimeConfig {
    pub enabled: bool,
    pub default_mode: String,
}

impl SmartRoutingRuntimeConfig {
    pub(crate) fn effective_mode(&self) -> RoutingMode {
        if !self.enabled {
            return RoutingMode::Suggest;
        }
        RoutingMode::parse(self.default_mode.as_str()).unwrap_or(RoutingMode::Suggest)
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct PricingEstimate {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lower_usd: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upper_usd: Option<f64>,
    pub estimate_only: bool,
    pub source: String,
    pub precision: String,
}

impl PricingEstimate {
    pub(crate) fn unavailable() -> Self {
        Self {
            lower_usd: None,
            upper_usd: None,
            estimate_only: true,
            source: "unavailable".to_owned(),
            precision: "unknown".to_owned(),
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct UsageBudgetEvaluation {
    pub policy_id: String,
    pub scope_kind: String,
    pub scope_id: String,
    pub metric_kind: String,
    pub interval_kind: String,
    pub action: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consumed_value: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub projected_value: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub soft_limit_value: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hard_limit_value: Option<f64>,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct RoutingDecision {
    pub mode: String,
    pub scope_kind: String,
    pub scope_id: String,
    pub default_model_id: String,
    pub recommended_model_id: String,
    pub actual_model_id: String,
    pub provider_id: String,
    pub provider_kind: String,
    pub complexity_score: f64,
    pub health_state: String,
    pub explanation: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_cost: Option<PricingEstimate>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub budget_outcome: Option<String>,
    pub blocked: bool,
    pub approval_required: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct RoutingDecisionContext<'a> {
    pub scope_kind: &'a str,
    pub scope_id: &'a str,
    pub mode: RoutingMode,
    pub default_model_id: &'a str,
    pub prompt_text: &'a str,
    pub prompt_tokens_estimate: u64,
    pub json_mode: bool,
    pub vision_inputs: usize,
    pub provider_health_state: &'a str,
    pub provider_snapshot: &'a ProviderStatusSnapshot,
    pub pricing: &'a [UsagePricingRecord],
    pub budgets: &'a [UsageBudgetEvaluation],
}

#[derive(Debug, Clone, PartialEq)]
struct RoutingModelSelection {
    complexity_score: f64,
    explanation: Vec<String>,
    recommended_model_id: String,
    actual_model_id: String,
    provider_id: String,
    provider_kind: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RoutingCandidate {
    model_id: String,
    provider_id: String,
    provider_kind: String,
    health_state: String,
    cost_rank: u8,
    latency_rank: u8,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct UsageModelMixRecord {
    pub model_id: String,
    pub provider_kind: String,
    pub runs: u64,
    pub total_tokens: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_cost_usd: Option<f64>,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct UsageToolMixRecord {
    pub tool_name: String,
    pub proposals: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct UsageScopeMixRecord {
    pub scope: String,
    pub runs: u64,
    pub total_tokens: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_cost_usd: Option<f64>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct UsageAlertCandidate {
    pub alert_kind: String,
    pub severity: String,
    pub scope_kind: String,
    pub scope_id: String,
    pub summary: String,
    pub reason: String,
    pub recommended_action: String,
    pub dedupe_key: String,
    pub payload: Value,
}

#[derive(Debug, Clone)]
pub(crate) struct UsageEnrichedRun<'a> {
    pub run: &'a OrchestratorUsageInsightsRunRecord,
    pub routing: Option<&'a UsageRoutingDecisionRecord>,
    pub inferred_model_id: &'a str,
    pub inferred_provider_kind: &'a str,
    pub cost_estimate: PricingEstimate,
}

pub(crate) struct UsageRoutingPlanRequest<'a> {
    pub runtime_state: &'a Arc<GatewayRuntimeState>,
    pub request_context: &'a RequestContext,
    pub run_id: &'a str,
    pub session_id: &'a str,
    pub parameter_delta_json: Option<&'a str>,
    pub prompt_text: &'a str,
    pub json_mode: bool,
    pub vision_inputs: usize,
    pub scope_kind: &'a str,
    pub scope_id: &'a str,
    pub provider_snapshot: &'a ProviderStatusSnapshot,
}

pub(crate) fn parse_routing_mode_override(
    parameter_delta_json: Option<&str>,
) -> Option<RoutingMode> {
    let raw = parameter_delta_json?.trim();
    if raw.is_empty() {
        return None;
    }
    let parsed = serde_json::from_str::<Value>(raw).ok()?;
    let routing = parsed.get("routing")?;
    let mode = routing.get("mode")?.as_str()?;
    RoutingMode::parse(mode)
}

pub(crate) fn usage_budget_subject_id(policy_id: &str) -> String {
    format!("{USAGE_BUDGET_SUBJECT_PREFIX}{policy_id}")
}

#[allow(clippy::result_large_err)]
pub(crate) async fn plan_usage_routing(
    request: UsageRoutingPlanRequest<'_>,
) -> Result<RoutingDecision, Status> {
    let pricing = request.runtime_state.list_usage_pricing_records().await?;
    let default_model_id = request
        .provider_snapshot
        .registry
        .default_chat_model_id
        .clone()
        .or_else(|| request.provider_snapshot.model_id.clone())
        .or_else(|| request.provider_snapshot.openai_model.clone())
        .or_else(|| request.provider_snapshot.anthropic_model.clone())
        .unwrap_or_else(|| "deterministic".to_owned());
    let mode = if request.runtime_state.config.smart_routing.enabled {
        parse_routing_mode_override(request.parameter_delta_json)
            .unwrap_or_else(|| request.runtime_state.config.smart_routing.effective_mode())
    } else {
        RoutingMode::Suggest
    };
    let budget_policies = request
        .runtime_state
        .list_usage_budget_policies(crate::journal::UsageBudgetPoliciesFilter {
            enabled_only: true,
            scope_kind: None,
            scope_id: None,
        })
        .await?;
    let historical_runs = request
        .runtime_state
        .list_orchestrator_usage_runs(
            crate::journal::OrchestratorUsageQuery {
                start_at_unix_ms: 0,
                end_at_unix_ms: crate::gateway::current_unix_ms(),
                bucket_width_ms: 24 * 60 * 60 * 1_000,
                principal: request.request_context.principal.clone(),
                device_id: request.request_context.device_id.clone(),
                channel: request.request_context.channel.clone(),
                include_archived: true,
                session_id: Some(request.session_id.to_owned()),
            },
            250,
        )
        .await
        .unwrap_or_default();
    let approved_subjects =
        load_budget_override_approvals(request.runtime_state, &budget_policies).await;
    let provider_health_state = provider_health_state(request.provider_snapshot);
    let prompt_tokens_estimate = estimate_token_count(request.prompt_text);
    let projected_selection = select_routing_models(&RoutingDecisionContext {
        scope_kind: request.scope_kind,
        scope_id: request.scope_id,
        mode,
        default_model_id: default_model_id.as_str(),
        prompt_text: request.prompt_text,
        prompt_tokens_estimate,
        json_mode: request.json_mode,
        vision_inputs: request.vision_inputs,
        provider_health_state,
        provider_snapshot: request.provider_snapshot,
        pricing: pricing.as_slice(),
        budgets: &[],
    });
    let routing_decisions = request
        .runtime_state
        .list_usage_routing_decisions(crate::journal::UsageRoutingDecisionsFilter {
            since_unix_ms: None,
            until_unix_ms: None,
            session_id: Some(request.session_id.to_owned()),
            run_id: None,
            limit: historical_runs.len().saturating_mul(2).max(128),
        })
        .await
        .unwrap_or_default();
    let routing_by_run = latest_routing_decisions_by_run_id(routing_decisions.as_slice());
    let enriched_runs = historical_runs
        .iter()
        .map(|run| {
            let routing = routing_by_run.get(run.run_id.as_str()).copied();
            let inferred_model_id = routing
                .map(|decision| decision.actual_model_id.as_str())
                .unwrap_or(default_model_id.as_str());
            let (fallback_provider_id, fallback_provider_kind) =
                resolve_provider_for_model(request.provider_snapshot, inferred_model_id);
            let inferred_provider_kind = routing
                .map(|decision| decision.provider_kind.as_str())
                .unwrap_or(fallback_provider_kind);
            let inferred_provider_id = routing
                .map(|decision| decision.provider_id.as_str())
                .unwrap_or(fallback_provider_id);
            let cost_estimate = estimate_cost_for_model(
                pricing.as_slice(),
                inferred_provider_kind,
                inferred_provider_id,
                inferred_model_id,
                run.started_at_unix_ms,
                run.prompt_tokens,
                run.completion_tokens,
            );
            UsageEnrichedRun {
                run,
                routing,
                inferred_model_id,
                inferred_provider_kind,
                cost_estimate,
            }
        })
        .collect::<Vec<_>>();
    let projected_tokens = historical_runs
        .iter()
        .map(|entry| entry.total_tokens)
        .sum::<u64>()
        .saturating_add(prompt_tokens_estimate);
    let projected_cost = estimate_cost_for_model(
        pricing.as_slice(),
        projected_selection.provider_kind.as_str(),
        projected_selection.provider_id.as_str(),
        projected_selection.actual_model_id.as_str(),
        crate::gateway::current_unix_ms(),
        prompt_tokens_estimate,
        prompt_tokens_estimate / 2,
    );
    let budget_evaluations = evaluate_budget_policies(
        budget_policies.as_slice(),
        enriched_runs.as_slice(),
        projected_tokens,
        &projected_cost,
        &approved_subjects,
    );
    let decision = decide_routing(RoutingDecisionContext {
        scope_kind: request.scope_kind,
        scope_id: request.scope_id,
        mode,
        default_model_id: default_model_id.as_str(),
        prompt_text: request.prompt_text,
        prompt_tokens_estimate,
        json_mode: request.json_mode,
        vision_inputs: request.vision_inputs,
        provider_health_state,
        provider_snapshot: request.provider_snapshot,
        pricing: pricing.as_slice(),
        budgets: budget_evaluations.as_slice(),
    });
    let _ = request
        .runtime_state
        .create_usage_routing_decision(UsageRoutingDecisionCreateRequest {
            decision_id: Ulid::new().to_string(),
            run_id: request.run_id.to_owned(),
            session_id: request.session_id.to_owned(),
            principal: request.request_context.principal.clone(),
            device_id: request.request_context.device_id.clone(),
            channel: request.request_context.channel.clone(),
            scope_kind: request.scope_kind.to_owned(),
            scope_id: request.scope_id.to_owned(),
            mode: decision.mode.clone(),
            default_model_id: decision.default_model_id.clone(),
            recommended_model_id: decision.recommended_model_id.clone(),
            actual_model_id: decision.actual_model_id.clone(),
            provider_id: decision.provider_id.clone(),
            provider_kind: decision.provider_kind.clone(),
            complexity_score: decision.complexity_score,
            health_state: decision.health_state.clone(),
            explanation_json: json!({
                "explanation": decision.explanation,
                "budget_outcome": decision.budget_outcome,
            })
            .to_string(),
            estimated_cost_lower_usd: decision
                .estimated_cost
                .as_ref()
                .and_then(|entry| entry.lower_usd),
            estimated_cost_upper_usd: decision
                .estimated_cost
                .as_ref()
                .and_then(|entry| entry.upper_usd),
            budget_outcome: decision.budget_outcome.clone(),
        })
        .await;

    if decision.approval_required {
        let policy_id = budget_evaluations
            .iter()
            .find(|entry| entry.status == "approval_required")
            .map(|entry| entry.policy_id.clone())
            .unwrap_or_else(|| "unknown".to_owned());
        if let Some(policy) = budget_policies.iter().find(|entry| entry.policy_id == policy_id) {
            let _ = request_usage_budget_override(
                request.runtime_state,
                request.request_context,
                policy,
                Some(request.session_id),
                Some(request.run_id),
                Some(decision.recommended_model_id.as_str()),
                Some("routing plan exceeded an approval-gated hard budget limit"),
            )
            .await;
        }
        return Err(Status::failed_precondition(
            "usage budget override approval required before routing can continue",
        ));
    }
    if decision.blocked {
        return Err(Status::failed_precondition(
            "usage budget hard limit blocked the requested run",
        ));
    }
    Ok(decision)
}

fn latest_routing_decisions_by_run_id<'a>(
    decisions: &'a [UsageRoutingDecisionRecord],
) -> HashMap<&'a str, &'a UsageRoutingDecisionRecord> {
    let mut by_run: HashMap<&'a str, &'a UsageRoutingDecisionRecord> = HashMap::new();
    for decision in decisions {
        match by_run.get(decision.run_id.as_str()) {
            Some(existing) if existing.created_at_unix_ms >= decision.created_at_unix_ms => {}
            _ => {
                by_run.insert(decision.run_id.as_str(), decision);
            }
        }
    }
    by_run
}

pub(crate) fn estimate_cost_for_model(
    pricing: &[UsagePricingRecord],
    provider_kind: &str,
    provider_id: &str,
    model_id: &str,
    observed_at_unix_ms: i64,
    prompt_tokens: u64,
    completion_tokens: u64,
) -> PricingEstimate {
    let Some(record) = pricing.iter().find(|entry| {
        entry.provider_kind == provider_kind
            && entry.provider_id == provider_id
            && entry.model_id == model_id
            && entry.effective_from_unix_ms <= observed_at_unix_ms
            && entry.effective_to_unix_ms.is_none_or(|value| value > observed_at_unix_ms)
    }) else {
        return PricingEstimate::unavailable();
    };

    let prompt_component =
        record.input_cost_per_million_usd.map(|rate| (prompt_tokens as f64 / 1_000_000.0) * rate);
    let completion_component = record
        .output_cost_per_million_usd
        .map(|rate| (completion_tokens as f64 / 1_000_000.0) * rate);
    let fixed_component = record.fixed_request_cost_usd.unwrap_or(0.0);
    let total =
        prompt_component.unwrap_or(0.0) + completion_component.unwrap_or(0.0) + fixed_component;
    let total = (total * 100_000.0).round() / 100_000.0;

    PricingEstimate {
        lower_usd: Some(total),
        upper_usd: Some(total),
        estimate_only: record.precision != "exact",
        source: record.source.clone(),
        precision: record.precision.clone(),
    }
}

pub(crate) fn evaluate_budget_policies(
    policies: &[UsageBudgetPolicyRecord],
    runs: &[UsageEnrichedRun<'_>],
    projected_total_tokens: u64,
    projected_cost_estimate: &PricingEstimate,
    approved_subjects: &HashMap<String, bool>,
) -> Vec<UsageBudgetEvaluation> {
    policies
        .iter()
        .filter(|policy| policy.enabled)
        .map(|policy| {
            let consumed_value = match policy.metric_kind.as_str() {
                "total_tokens" => {
                    Some(runs.iter().map(|entry| entry.run.total_tokens).sum::<u64>() as f64)
                }
                "estimated_cost_usd" => {
                    let total =
                        runs.iter().filter_map(|entry| entry.cost_estimate.upper_usd).sum::<f64>();
                    Some(total)
                }
                _ => None,
            };
            let projected_value = match policy.metric_kind.as_str() {
                "total_tokens" => Some(projected_total_tokens as f64),
                "estimated_cost_usd" => projected_cost_estimate.upper_usd,
                _ => None,
            };
            let subject_id = usage_budget_subject_id(policy.policy_id.as_str());
            let approved_override =
                approved_subjects.get(subject_id.as_str()).copied().unwrap_or(false);

            let status = if let (Some(projected), Some(hard_limit)) =
                (projected_value, policy.hard_limit_value)
            {
                if projected > hard_limit {
                    if approved_override {
                        "override_applied"
                    } else if policy.action == "block" {
                        "blocked"
                    } else if policy.action == "approval_required" {
                        "approval_required"
                    } else {
                        "hard_limit"
                    }
                } else if let Some(soft_limit) = policy.soft_limit_value {
                    if projected > soft_limit {
                        "soft_limit"
                    } else {
                        "ok"
                    }
                } else {
                    "ok"
                }
            } else if let (Some(projected), Some(soft_limit)) =
                (projected_value, policy.soft_limit_value)
            {
                if projected > soft_limit {
                    "soft_limit"
                } else {
                    "ok"
                }
            } else {
                "unknown"
            };

            let message = match status {
                "override_applied" => format!(
                    "Budget override already approved for {} {}.",
                    policy.scope_kind, policy.scope_id
                ),
                "blocked" => format!(
                    "Projected {} exceeds hard limit for {} {}.",
                    policy.metric_kind, policy.scope_kind, policy.scope_id
                ),
                "approval_required" => format!(
                    "Projected {} exceeds approval-gated hard limit for {} {}.",
                    policy.metric_kind, policy.scope_kind, policy.scope_id
                ),
                "hard_limit" => format!(
                    "Projected {} exceeds hard limit for {} {}.",
                    policy.metric_kind, policy.scope_kind, policy.scope_id
                ),
                "soft_limit" => format!(
                    "Projected {} is above warning threshold for {} {}.",
                    policy.metric_kind, policy.scope_kind, policy.scope_id
                ),
                "unknown" => format!(
                    "Policy {} cannot be evaluated because {} is unavailable.",
                    policy.policy_id, policy.metric_kind
                ),
                _ => format!("Budget policy {} is within limits.", policy.policy_id),
            };

            UsageBudgetEvaluation {
                policy_id: policy.policy_id.clone(),
                scope_kind: policy.scope_kind.clone(),
                scope_id: policy.scope_id.clone(),
                metric_kind: policy.metric_kind.clone(),
                interval_kind: policy.interval_kind.clone(),
                action: policy.action.clone(),
                status: status.to_owned(),
                consumed_value,
                projected_value,
                soft_limit_value: policy.soft_limit_value,
                hard_limit_value: policy.hard_limit_value,
                message,
            }
        })
        .collect()
}

pub(crate) fn decide_routing(context: RoutingDecisionContext<'_>) -> RoutingDecision {
    let selection = select_routing_models(&context);
    let mut explanation = selection.explanation.clone();

    let mut blocked = false;
    let mut approval_required = false;
    let mut budget_outcome = None;
    for evaluation in context.budgets {
        if matches!(
            evaluation.status.as_str(),
            "soft_limit" | "hard_limit" | "blocked" | "approval_required" | "override_applied"
        ) {
            explanation.push(evaluation.message.clone());
            budget_outcome = Some(evaluation.status.clone());
        }
        if evaluation.status == "blocked" {
            blocked = true;
        }
        if evaluation.status == "approval_required" {
            approval_required = true;
        }
    }

    let estimate = estimate_cost_for_model(
        context.pricing,
        selection.provider_kind.as_str(),
        selection.provider_id.as_str(),
        selection.actual_model_id.as_str(),
        0,
        context.prompt_tokens_estimate,
        context.prompt_tokens_estimate / 2,
    );

    RoutingDecision {
        mode: context.mode.as_str().to_owned(),
        scope_kind: context.scope_kind.to_owned(),
        scope_id: context.scope_id.to_owned(),
        default_model_id: context.default_model_id.to_owned(),
        recommended_model_id: selection.recommended_model_id,
        actual_model_id: selection.actual_model_id,
        provider_id: selection.provider_id,
        provider_kind: selection.provider_kind,
        complexity_score: selection.complexity_score,
        health_state: context.provider_health_state.to_owned(),
        explanation,
        estimated_cost: Some(estimate),
        budget_outcome,
        blocked,
        approval_required,
    }
}

fn select_routing_models(context: &RoutingDecisionContext<'_>) -> RoutingModelSelection {
    let complexity_score = complexity_score(
        context.prompt_text,
        context.prompt_tokens_estimate,
        context.json_mode,
        context.vision_inputs,
    );
    let mut explanation = vec![format!(
        "Complexity {:.2} derived from prompt length, token estimate, JSON mode, and vision inputs.",
        complexity_score
    )];
    if context.provider_health_state != "ok" {
        explanation.push(format!(
            "Provider health is {}, so routing stays conservative.",
            context.provider_health_state
        ));
    }

    let mut candidates = build_routing_candidates(context);
    let default_candidate = default_routing_candidate(context);
    let default_candidate_in_registry =
        candidates.iter().any(|candidate| candidate.model_id == default_candidate.model_id);
    if !default_candidate_in_registry {
        candidates.push(default_candidate.clone());
    }

    let capability_filtered = context.provider_snapshot.registry.models.iter().any(|model| {
        model.role == "chat"
            && model.enabled
            && (context.json_mode && !model.capabilities.json_mode
                || context.vision_inputs > 0 && !model.capabilities.vision)
    });
    if capability_filtered && (context.json_mode || context.vision_inputs > 0) {
        explanation.push(
            "Capability filters removed one or more chat models that could not satisfy the request."
                .to_owned(),
        );
    }

    let mut recommendation_pool = candidates.clone();
    if context.provider_snapshot.registry.failover_enabled
        && default_candidate.health_state != "ok"
        && candidates.iter().any(|candidate| candidate.health_state == "ok")
    {
        recommendation_pool.retain(|candidate| candidate.health_state == "ok");
        explanation.push(format!(
            "Default provider {} is {}, so smart routing prefers a healthy registry fallback.",
            default_candidate.provider_id, default_candidate.health_state
        ));
    }

    let recommended_candidate = choose_routing_candidate(
        recommendation_pool.as_slice(),
        &default_candidate,
        complexity_score,
    );
    let actual_candidate = match context.mode {
        RoutingMode::Suggest | RoutingMode::DryRun => default_candidate.clone(),
        RoutingMode::Enforced => recommended_candidate.clone(),
    };
    if context.mode != RoutingMode::Enforced
        && (recommended_candidate.model_id != actual_candidate.model_id
            || recommended_candidate.provider_id != actual_candidate.provider_id)
    {
        explanation.push(format!(
            "Mode {} keeps the default model active while still publishing the recommendation.",
            context.mode.as_str()
        ));
    }

    RoutingModelSelection {
        complexity_score,
        explanation,
        recommended_model_id: recommended_candidate.model_id,
        actual_model_id: actual_candidate.model_id,
        provider_id: actual_candidate.provider_id,
        provider_kind: actual_candidate.provider_kind,
    }
}

fn build_routing_candidates(context: &RoutingDecisionContext<'_>) -> Vec<RoutingCandidate> {
    let mut candidates = Vec::new();
    for model in &context.provider_snapshot.registry.models {
        if model.role != "chat" || !model.enabled {
            continue;
        }
        if context.json_mode && !model.capabilities.json_mode {
            continue;
        }
        if context.vision_inputs > 0 && !model.capabilities.vision {
            continue;
        }
        let Some(provider) =
            find_registry_provider(context.provider_snapshot, model.provider_id.as_str())
        else {
            continue;
        };
        if !provider.enabled {
            continue;
        }
        candidates.push(RoutingCandidate {
            model_id: model.model_id.clone(),
            provider_id: provider.provider_id.clone(),
            provider_kind: provider.kind.clone(),
            health_state: registry_provider_health_state(provider).to_owned(),
            cost_rank: provider_cost_rank(model),
            latency_rank: provider_latency_rank(model),
        });
    }
    candidates
}

fn default_routing_candidate(context: &RoutingDecisionContext<'_>) -> RoutingCandidate {
    if let Some(model) = find_registry_model(context.provider_snapshot, context.default_model_id) {
        if let Some(provider) =
            find_registry_provider(context.provider_snapshot, model.provider_id.as_str())
        {
            return RoutingCandidate {
                model_id: model.model_id.clone(),
                provider_id: provider.provider_id.clone(),
                provider_kind: provider.kind.clone(),
                health_state: registry_provider_health_state(provider).to_owned(),
                cost_rank: provider_cost_rank(model),
                latency_rank: provider_latency_rank(model),
            };
        }
    }

    RoutingCandidate {
        model_id: context.default_model_id.to_owned(),
        provider_id: context.provider_snapshot.provider_id.clone(),
        provider_kind: context.provider_snapshot.kind.clone(),
        health_state: context.provider_health_state.to_owned(),
        cost_rank: cost_tier_rank(context.provider_snapshot.capabilities.cost_tier.as_str()),
        latency_rank: latency_tier_rank(
            context.provider_snapshot.capabilities.latency_tier.as_str(),
        ),
    }
}

fn choose_routing_candidate(
    candidates: &[RoutingCandidate],
    default_candidate: &RoutingCandidate,
    complexity_score: f64,
) -> RoutingCandidate {
    if candidates.is_empty() {
        return default_candidate.clone();
    }

    if complexity_score >= 0.75 {
        return candidates
            .iter()
            .max_by(|left, right| {
                left.cost_rank
                    .cmp(&right.cost_rank)
                    .then_with(|| right.latency_rank.cmp(&left.latency_rank))
                    .then_with(|| left.model_id.cmp(&right.model_id))
            })
            .cloned()
            .unwrap_or_else(|| default_candidate.clone());
    }

    if complexity_score <= 0.35 {
        return candidates
            .iter()
            .min_by(|left, right| {
                left.cost_rank
                    .cmp(&right.cost_rank)
                    .then_with(|| left.latency_rank.cmp(&right.latency_rank))
                    .then_with(|| left.model_id.cmp(&right.model_id))
            })
            .cloned()
            .unwrap_or_else(|| default_candidate.clone());
    }

    candidates
        .iter()
        .find(|candidate| candidate.model_id == default_candidate.model_id)
        .cloned()
        .or_else(|| {
            candidates
                .iter()
                .min_by(|left, right| {
                    left.latency_rank
                        .cmp(&right.latency_rank)
                        .then_with(|| left.cost_rank.cmp(&right.cost_rank))
                        .then_with(|| left.model_id.cmp(&right.model_id))
                })
                .cloned()
        })
        .unwrap_or_else(|| default_candidate.clone())
}

fn find_registry_provider<'a>(
    snapshot: &'a ProviderStatusSnapshot,
    provider_id: &str,
) -> Option<&'a ProviderRegistryProviderSnapshot> {
    snapshot.registry.providers.iter().find(|provider| provider.provider_id == provider_id)
}

fn find_registry_model<'a>(
    snapshot: &'a ProviderStatusSnapshot,
    model_id: &str,
) -> Option<&'a ProviderRegistryModelSnapshot> {
    snapshot.registry.models.iter().find(|model| model.model_id == model_id)
}

fn resolve_provider_for_model<'a>(
    snapshot: &'a ProviderStatusSnapshot,
    model_id: &str,
) -> (&'a str, &'a str) {
    if let Some(model) = find_registry_model(snapshot, model_id) {
        if let Some(provider) = find_registry_provider(snapshot, model.provider_id.as_str()) {
            return (provider.provider_id.as_str(), provider.kind.as_str());
        }
    }
    (snapshot.provider_id.as_str(), snapshot.kind.as_str())
}

fn registry_provider_health_state(provider: &ProviderRegistryProviderSnapshot) -> &'static str {
    if provider.circuit_breaker.open || provider.runtime_metrics.error_count > 0 {
        "degraded"
    } else {
        match provider.health.state.as_str() {
            "ok" | "static" => "ok",
            "missing_auth" => "missing_auth",
            _ if provider.api_key_configured || provider.auth_profile_id.is_some() => "ok",
            _ => "degraded",
        }
    }
}

fn provider_cost_rank(model: &ProviderRegistryModelSnapshot) -> u8 {
    cost_tier_rank(model.capabilities.cost_tier.as_str())
}

fn provider_latency_rank(model: &ProviderRegistryModelSnapshot) -> u8 {
    latency_tier_rank(model.capabilities.latency_tier.as_str())
}

fn cost_tier_rank(value: &str) -> u8 {
    match value {
        "low" => 0,
        "standard" => 1,
        "premium" => 2,
        _ => 1,
    }
}

fn latency_tier_rank(value: &str) -> u8 {
    match value {
        "low" => 0,
        "standard" => 1,
        "high" => 2,
        _ => 1,
    }
}

pub(crate) fn build_model_mix(runs: &[UsageEnrichedRun<'_>]) -> Vec<UsageModelMixRecord> {
    let mut groups = HashMap::<(String, String, String), UsageModelMixRecord>::new();
    for entry in runs {
        let model_id = entry
            .routing
            .map(|decision| decision.actual_model_id.clone())
            .unwrap_or_else(|| entry.inferred_model_id.to_owned());
        let provider_kind = entry
            .routing
            .map(|decision| decision.provider_kind.clone())
            .unwrap_or_else(|| entry.inferred_provider_kind.to_owned());
        let source = if entry.routing.is_some() { "routing_decision" } else { "inferred_default" };
        let key = (model_id.clone(), provider_kind.clone(), source.to_owned());
        let row = groups.entry(key).or_insert_with(|| UsageModelMixRecord {
            model_id: model_id.clone(),
            provider_kind: provider_kind.clone(),
            runs: 0,
            total_tokens: 0,
            estimated_cost_usd: Some(0.0),
            source: source.to_owned(),
        });
        row.runs = row.runs.saturating_add(1);
        row.total_tokens = row.total_tokens.saturating_add(entry.run.total_tokens);
        if let Some(cost) = entry.cost_estimate.upper_usd {
            row.estimated_cost_usd = Some(row.estimated_cost_usd.unwrap_or(0.0) + cost);
        } else {
            row.estimated_cost_usd = None;
        }
    }
    let mut rows = groups.into_values().collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        right.total_tokens.cmp(&left.total_tokens).then_with(|| left.model_id.cmp(&right.model_id))
    });
    rows
}

pub(crate) fn build_scope_mix(runs: &[UsageEnrichedRun<'_>]) -> Vec<UsageScopeMixRecord> {
    let mut groups = HashMap::<String, UsageScopeMixRecord>::new();
    for entry in runs {
        let scope =
            if entry.run.origin_kind == "background" || entry.run.background_task_id.is_some() {
                "background".to_owned()
            } else {
                "foreground".to_owned()
            };
        let row = groups.entry(scope.clone()).or_insert_with(|| UsageScopeMixRecord {
            scope,
            runs: 0,
            total_tokens: 0,
            estimated_cost_usd: Some(0.0),
        });
        row.runs = row.runs.saturating_add(1);
        row.total_tokens = row.total_tokens.saturating_add(entry.run.total_tokens);
        if let Some(cost) = entry.cost_estimate.upper_usd {
            row.estimated_cost_usd = Some(row.estimated_cost_usd.unwrap_or(0.0) + cost);
        } else {
            row.estimated_cost_usd = None;
        }
    }
    let mut rows = groups.into_values().collect::<Vec<_>>();
    rows.sort_by(|left, right| right.runs.cmp(&left.runs));
    rows
}

pub(crate) fn build_tool_mix(tool_counts: &HashMap<String, u64>) -> Vec<UsageToolMixRecord> {
    let mut rows = tool_counts
        .iter()
        .map(|(tool_name, proposals)| UsageToolMixRecord {
            tool_name: tool_name.clone(),
            proposals: *proposals,
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        right.proposals.cmp(&left.proposals).then_with(|| left.tool_name.cmp(&right.tool_name))
    });
    rows
}

pub(crate) fn build_alert_candidates(
    runs: &[UsageEnrichedRun<'_>],
    routing_decisions: &[UsageRoutingDecisionRecord],
    budget_evaluations: &[UsageBudgetEvaluation],
    model_mix: &[UsageModelMixRecord],
    provider_health_state: &str,
) -> Vec<UsageAlertCandidate> {
    let total_cost = runs.iter().filter_map(|entry| entry.cost_estimate.upper_usd).sum::<f64>();
    let first_half_cost = runs
        .iter()
        .skip(runs.len() / 2)
        .filter_map(|entry| entry.cost_estimate.upper_usd)
        .sum::<f64>();
    let second_half_cost = runs
        .iter()
        .take(runs.len() / 2)
        .filter_map(|entry| entry.cost_estimate.upper_usd)
        .sum::<f64>();

    let mut alerts = Vec::new();
    if first_half_cost > ALERT_MIN_COST_SPIKE_USD
        && second_half_cost > 0.0
        && first_half_cost > second_half_cost * 2.0
    {
        alerts.push(UsageAlertCandidate {
            alert_kind: "cost_spike".to_owned(),
            severity: "warning".to_owned(),
            scope_kind: "environment".to_owned(),
            scope_id: "default".to_owned(),
            summary: "Recent usage cost spiked sharply.".to_owned(),
            reason: format!(
                "The newer half of the selected interval is {:.2}x more expensive than the older half.",
                first_half_cost / second_half_cost
            ),
            recommended_action: "Inspect routing decisions and the model mix before the spike continues.".to_owned(),
            dedupe_key: "cost_spike:environment:default".to_owned(),
            payload: json!({
                "older_half_cost_usd": second_half_cost,
                "newer_half_cost_usd": first_half_cost,
                "total_cost_usd": total_cost,
            }),
        });
    }

    if let Some(primary_model) = model_mix.first() {
        if primary_model.runs > 0 && (primary_model.runs as f64 / runs.len().max(1) as f64) > 0.75 {
            alerts.push(UsageAlertCandidate {
                alert_kind: "unusual_model_mix".to_owned(),
                severity: "warning".to_owned(),
                scope_kind: "environment".to_owned(),
                scope_id: "default".to_owned(),
                summary: "One model dominates the current workload.".to_owned(),
                reason: format!("Model `{}` accounts for most of the selected runs.", primary_model.model_id),
                recommended_action: "Check whether routing mode or policy overrides are pinning the same model too often.".to_owned(),
                dedupe_key: format!("model_mix:{}", primary_model.model_id),
                payload: json!({
                    "model_id": primary_model.model_id,
                    "runs": primary_model.runs,
                    "total_runs": runs.len(),
                }),
            });
        }
    }

    if provider_health_state != "ok" {
        alerts.push(UsageAlertCandidate {
            alert_kind: "provider_health".to_owned(),
            severity: "danger".to_owned(),
            scope_kind: "provider".to_owned(),
            scope_id: provider_health_state.to_owned(),
            summary: "Provider health is degraded.".to_owned(),
            reason: format!("Model provider health is currently `{provider_health_state}`."),
            recommended_action: "Open diagnostics and confirm whether circuit breaker or auth posture explains the degradation.".to_owned(),
            dedupe_key: format!("provider_health:{provider_health_state}"),
            payload: json!({ "provider_health_state": provider_health_state }),
        });
    }

    if routing_decisions
        .iter()
        .filter(|entry| entry.mode == "enforced" && entry.actual_model_id != entry.default_model_id)
        .count()
        > 0
    {
        alerts.push(UsageAlertCandidate {
            alert_kind: "routing_regression".to_owned(),
            severity: "warning".to_owned(),
            scope_kind: "routing".to_owned(),
            scope_id: "default".to_owned(),
            summary: "Enforced routing is overriding the default model.".to_owned(),
            reason: "Recent enforced routing decisions differ from the default model selection.".to_owned(),
            recommended_action: "Review routing explanations and confirm the override matches current latency and budget goals.".to_owned(),
            dedupe_key: "routing_regression:default".to_owned(),
            payload: json!({
                "enforced_override_count": routing_decisions
                    .iter()
                    .filter(|entry| entry.mode == "enforced" && entry.actual_model_id != entry.default_model_id)
                    .count(),
            }),
        });
    }

    for evaluation in budget_evaluations.iter().filter(|entry| {
        matches!(
            entry.status.as_str(),
            "soft_limit" | "hard_limit" | "blocked" | "approval_required"
        )
    }) {
        alerts.push(UsageAlertCandidate {
            alert_kind: "budget_alert".to_owned(),
            severity: if matches!(evaluation.status.as_str(), "blocked" | "approval_required") {
                "danger".to_owned()
            } else {
                "warning".to_owned()
            },
            scope_kind: evaluation.scope_kind.clone(),
            scope_id: evaluation.scope_id.clone(),
            summary: format!("Budget policy {} needs attention.", evaluation.policy_id),
            reason: evaluation.message.clone(),
            recommended_action: "Review budget policy, routing mode, and approvals before more expensive runs are queued.".to_owned(),
            dedupe_key: format!("budget:{}:{}", evaluation.policy_id, evaluation.status),
            payload: json!({
                "policy_id": evaluation.policy_id,
                "status": evaluation.status,
                "metric_kind": evaluation.metric_kind,
                "projected_value": evaluation.projected_value,
                "hard_limit_value": evaluation.hard_limit_value,
                "soft_limit_value": evaluation.soft_limit_value,
            }),
        });
    }

    alerts
}

pub(crate) async fn request_usage_budget_override(
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    policy: &UsageBudgetPolicyRecord,
    session_id: Option<&str>,
    run_id: Option<&str>,
    recommended_model_id: Option<&str>,
    requested_reason: Option<&str>,
) -> Result<ApprovalRecord, Status> {
    let subject_id = usage_budget_subject_id(policy.policy_id.as_str());
    if let Some(existing) = runtime_state.latest_approval_by_subject(subject_id.clone()).await? {
        if existing.decision.is_none() || is_active_budget_override_allow(&existing) {
            return Ok(existing);
        }
    }

    let session_id = session_id.map(ToOwned::to_owned).unwrap_or_else(|| Ulid::new().to_string());
    let run_id = run_id.map(ToOwned::to_owned).unwrap_or_else(|| Ulid::new().to_string());
    let request_summary =
        if let Some(reason) = requested_reason.filter(|value| !value.trim().is_empty()) {
            format!("Budget override requested for policy {}: {}", policy.policy_id, reason.trim())
        } else {
            format!("Budget override requested for policy {}", policy.policy_id)
        };

    runtime_state
        .create_approval_record(ApprovalCreateRequest {
            approval_id: Ulid::new().to_string(),
            session_id,
            run_id,
            principal: request_context.principal.clone(),
            device_id: request_context.device_id.clone(),
            channel: request_context.channel.clone(),
            subject_type: ApprovalSubjectType::Tool,
            subject_id: subject_id.clone(),
            request_summary,
            policy_snapshot: ApprovalPolicySnapshot {
                policy_id: "usage_budget_policy.v1".to_owned(),
                policy_hash: "usage-governance-v1".to_owned(),
                evaluation_summary: "budget hard limit exceeded; explicit override required"
                    .to_owned(),
            },
            prompt: ApprovalPromptRecord {
                title: "Approve usage budget override".to_owned(),
                risk_level: ApprovalRiskLevel::High,
                subject_id,
                summary: "A run exceeded a hard budget limit and needs an explicit override."
                    .to_owned(),
                options: vec![
                    ApprovalPromptOption {
                        option_id: "allow_once".to_owned(),
                        label: "Allow once".to_owned(),
                        description: "Approve this single override.".to_owned(),
                        default_selected: true,
                        decision_scope: ApprovalDecisionScope::Once,
                        timebox_ttl_ms: None,
                    },
                    ApprovalPromptOption {
                        option_id: "deny".to_owned(),
                        label: "Deny".to_owned(),
                        description: "Keep the hard limit enforced.".to_owned(),
                        default_selected: false,
                        decision_scope: ApprovalDecisionScope::Once,
                        timebox_ttl_ms: None,
                    },
                ],
                timeout_seconds: 60,
                details_json: json!({
                    "policy_id": policy.policy_id,
                    "scope_kind": policy.scope_kind,
                    "scope_id": policy.scope_id,
                    "metric_kind": policy.metric_kind,
                    "interval_kind": policy.interval_kind,
                    "action": policy.action,
                    "soft_limit_value": policy.soft_limit_value,
                    "hard_limit_value": policy.hard_limit_value,
                    "routing_mode_override": policy.routing_mode_override,
                    "recommended_model_id": recommended_model_id,
                    "requested_reason": requested_reason,
                })
                .to_string(),
                policy_explanation:
                    "Hard budget limits require an operator override before the run can continue."
                        .to_owned(),
            },
        })
        .await
}

pub(crate) async fn load_budget_override_approvals(
    runtime_state: &Arc<GatewayRuntimeState>,
    policies: &[UsageBudgetPolicyRecord],
) -> HashMap<String, bool> {
    let mut approvals = HashMap::new();
    for policy in policies {
        let subject_id = usage_budget_subject_id(policy.policy_id.as_str());
        let approved = runtime_state
            .latest_approval_by_subject(subject_id.clone())
            .await
            .ok()
            .flatten()
            .is_some_and(|record| is_active_budget_override_allow(&record));
        approvals.insert(subject_id, approved);
    }
    approvals
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::{
        latest_routing_decisions_by_run_id, select_routing_models, RoutingDecisionContext,
        RoutingMode,
    };
    use crate::journal::{UsagePricingRecord, UsageRoutingDecisionRecord};
    use crate::model_provider::{
        ProviderCapabilitiesSnapshot, ProviderCircuitBreakerSnapshot, ProviderDiscoverySnapshot,
        ProviderHealthProbeSnapshot, ProviderRegistryModelSnapshot,
        ProviderRegistryProviderSnapshot, ProviderRegistrySnapshot, ProviderResponseCacheSnapshot,
        ProviderRetryPolicySnapshot, ProviderRuntimeMetricsSnapshot, ProviderStatusSnapshot,
    };

    fn capabilities(
        cost_tier: &str,
        latency_tier: &str,
        json_mode: bool,
        vision: bool,
    ) -> ProviderCapabilitiesSnapshot {
        ProviderCapabilitiesSnapshot {
            streaming_tokens: true,
            tool_calls: true,
            json_mode,
            vision,
            audio_transcribe: false,
            embeddings: false,
            max_context_tokens: Some(128_000),
            cost_tier: cost_tier.to_owned(),
            latency_tier: latency_tier.to_owned(),
            recommended_use_cases: vec![],
            known_limitations: vec![],
            operator_override: false,
            metadata_source: "static".to_owned(),
        }
    }

    fn provider_runtime_metrics(error_count: u64) -> ProviderRuntimeMetricsSnapshot {
        ProviderRuntimeMetricsSnapshot {
            request_count: 0,
            error_count,
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

    fn registry_provider(
        provider_id: &str,
        kind: &str,
        health_state: &str,
        error_count: u64,
    ) -> ProviderRegistryProviderSnapshot {
        ProviderRegistryProviderSnapshot {
            provider_id: provider_id.to_owned(),
            credential_id: format!("credential-{provider_id}"),
            display_name: provider_id.to_owned(),
            kind: kind.to_owned(),
            enabled: true,
            endpoint_base_url: None,
            auth_profile_id: Some(format!("auth-{provider_id}")),
            auth_profile_provider_kind: Some(kind.to_owned()),
            credential_source: Some("auth_profile_api_key".to_owned()),
            api_key_configured: true,
            retry_policy: ProviderRetryPolicySnapshot { max_retries: 2, retry_backoff_ms: 100 },
            circuit_breaker: ProviderCircuitBreakerSnapshot {
                failure_threshold: 3,
                cooldown_ms: 30_000,
                consecutive_failures: error_count as u32,
                open: false,
            },
            runtime_metrics: provider_runtime_metrics(error_count),
            health: ProviderHealthProbeSnapshot {
                state: health_state.to_owned(),
                message: health_state.to_owned(),
                checked_at_unix_ms: Some(0),
                latency_ms: Some(50),
                source: "test".to_owned(),
            },
            discovery: ProviderDiscoverySnapshot {
                status: "static".to_owned(),
                checked_at_unix_ms: Some(0),
                expires_at_unix_ms: None,
                discovered_model_ids: vec![],
                source: "test".to_owned(),
                message: None,
            },
        }
    }

    fn registry_model(
        model_id: &str,
        provider_id: &str,
        cost_tier: &str,
        latency_tier: &str,
        json_mode: bool,
        vision: bool,
    ) -> ProviderRegistryModelSnapshot {
        ProviderRegistryModelSnapshot {
            model_id: model_id.to_owned(),
            provider_id: provider_id.to_owned(),
            role: "chat".to_owned(),
            enabled: true,
            capabilities: capabilities(cost_tier, latency_tier, json_mode, vision),
        }
    }

    fn provider_snapshot(
        default_model_id: &str,
        providers: Vec<ProviderRegistryProviderSnapshot>,
        models: Vec<ProviderRegistryModelSnapshot>,
    ) -> ProviderStatusSnapshot {
        let default_model =
            models.iter().find(|model| model.model_id == default_model_id).cloned().unwrap_or_else(
                || registry_model(default_model_id, "openai", "low", "low", true, false),
            );
        let default_provider = providers
            .iter()
            .find(|provider| provider.provider_id == default_model.provider_id)
            .cloned()
            .unwrap_or_else(|| {
                registry_provider(default_model.provider_id.as_str(), "openai_compatible", "ok", 0)
            });
        ProviderStatusSnapshot {
            kind: default_provider.kind.clone(),
            provider_id: default_provider.provider_id.clone(),
            credential_id: default_provider.credential_id.clone(),
            model_id: Some(default_model.model_id.clone()),
            capabilities: default_model.capabilities.clone(),
            openai_base_url: Some("https://api.openai.test/v1".to_owned()),
            anthropic_base_url: Some("https://api.anthropic.test".to_owned()),
            openai_model: Some(default_model.model_id.clone()),
            anthropic_model: None,
            openai_embeddings_model: None,
            openai_embeddings_dims: None,
            auth_profile_id: default_provider.auth_profile_id.clone(),
            auth_profile_provider_kind: default_provider.auth_profile_provider_kind.clone(),
            credential_source: default_provider.credential_source.clone(),
            api_key_configured: default_provider.api_key_configured,
            retry_policy: default_provider.retry_policy.clone(),
            circuit_breaker: default_provider.circuit_breaker.clone(),
            runtime_metrics: default_provider.runtime_metrics.clone(),
            response_cache: ProviderResponseCacheSnapshot {
                enabled: true,
                entry_count: 0,
                hit_count: 0,
                miss_count: 0,
            },
            health: default_provider.health.clone(),
            discovery: default_provider.discovery.clone(),
            registry: ProviderRegistrySnapshot {
                default_chat_model_id: Some(default_model_id.to_owned()),
                default_embeddings_model_id: None,
                default_audio_transcription_model_id: None,
                failover_enabled: true,
                response_cache_enabled: true,
                providers,
                credentials: Vec::new(),
                models,
            },
        }
    }

    fn pricing_record(model_id: &str, input_cost: f64, output_cost: f64) -> UsagePricingRecord {
        UsagePricingRecord {
            pricing_id: format!("pricing-{model_id}"),
            provider_kind: "openai_compatible".to_owned(),
            provider_id: "openai".to_owned(),
            model_id: model_id.to_owned(),
            input_cost_per_million_usd: Some(input_cost),
            output_cost_per_million_usd: Some(output_cost),
            fixed_request_cost_usd: Some(0.0),
            currency: "USD".to_owned(),
            precision: "exact".to_owned(),
            source: "test".to_owned(),
            effective_from_unix_ms: 0,
            effective_to_unix_ms: None,
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        }
    }

    fn routing_record(
        decision_id: &str,
        run_id: &str,
        actual_model_id: &str,
        created_at_unix_ms: i64,
    ) -> UsageRoutingDecisionRecord {
        UsageRoutingDecisionRecord {
            decision_id: decision_id.to_owned(),
            run_id: run_id.to_owned(),
            session_id: "session-1".to_owned(),
            principal: "user:test".to_owned(),
            device_id: "device-1".to_owned(),
            channel: Some("console".to_owned()),
            scope_kind: "session".to_owned(),
            scope_id: "session-1".to_owned(),
            mode: "enforced".to_owned(),
            default_model_id: "cheap".to_owned(),
            recommended_model_id: actual_model_id.to_owned(),
            actual_model_id: actual_model_id.to_owned(),
            provider_id: "openai".to_owned(),
            provider_kind: "openai_compatible".to_owned(),
            complexity_score: 0.9,
            health_state: "ok".to_owned(),
            explanation_json: "{}".to_owned(),
            estimated_cost_lower_usd: Some(1.0),
            estimated_cost_upper_usd: Some(1.0),
            budget_outcome: None,
            created_at_unix_ms,
        }
    }

    #[test]
    fn select_routing_models_uses_enforced_premium_model() {
        let pricing = vec![pricing_record("cheap", 0.1, 0.2), pricing_record("premium", 2.0, 4.0)];
        let snapshot = provider_snapshot(
            "cheap",
            vec![registry_provider("openai", "openai_compatible", "ok", 0)],
            vec![
                registry_model("cheap", "openai", "low", "low", true, false),
                registry_model("premium", "openai", "premium", "high", true, true),
            ],
        );
        let selection = select_routing_models(&RoutingDecisionContext {
            scope_kind: "session",
            scope_id: "session-1",
            mode: RoutingMode::Enforced,
            default_model_id: "cheap",
            prompt_text: &"complex request ".repeat(400),
            prompt_tokens_estimate: 2_400,
            json_mode: true,
            vision_inputs: 2,
            provider_health_state: "ok",
            provider_snapshot: &snapshot,
            pricing: pricing.as_slice(),
            budgets: &[],
        });
        assert_eq!(selection.recommended_model_id, "premium");
        assert_eq!(selection.actual_model_id, "premium");
        assert_eq!(selection.provider_id, "openai");
        assert_eq!(selection.provider_kind, "openai_compatible");
    }

    #[test]
    fn select_routing_models_prefers_healthy_registry_fallback_when_default_is_degraded() {
        let snapshot = provider_snapshot(
            "cheap",
            vec![
                registry_provider("openai", "openai_compatible", "ok", 3),
                registry_provider("anthropic", "anthropic", "ok", 0),
            ],
            vec![
                registry_model("cheap", "openai", "low", "low", true, false),
                registry_model("claude-sonnet", "anthropic", "premium", "standard", true, true),
            ],
        );
        let selection = select_routing_models(&RoutingDecisionContext {
            scope_kind: "session",
            scope_id: "session-1",
            mode: RoutingMode::Enforced,
            default_model_id: "cheap",
            prompt_text: "summarize this request",
            prompt_tokens_estimate: 180,
            json_mode: false,
            vision_inputs: 0,
            provider_health_state: "degraded",
            provider_snapshot: &snapshot,
            pricing: &[],
            budgets: &[],
        });
        assert_eq!(selection.recommended_model_id, "claude-sonnet");
        assert_eq!(selection.actual_model_id, "claude-sonnet");
        assert_eq!(selection.provider_id, "anthropic");
        assert_eq!(selection.provider_kind, "anthropic");
    }

    #[test]
    fn latest_routing_decisions_by_run_id_prefers_newest_decision() {
        let latest = routing_record("decision-new", "run-1", "premium", 20);
        let oldest = routing_record("decision-old", "run-1", "cheap", 10);
        let decisions = [oldest, latest.clone()];
        let by_run = latest_routing_decisions_by_run_id(&decisions);
        assert_eq!(
            by_run.get("run-1").map(|record| record.actual_model_id.as_str()),
            Some("premium")
        );
        assert_eq!(
            by_run.get("run-1").map(|record| record.decision_id.as_str()),
            Some(latest.decision_id.as_str())
        );
    }
}

fn is_active_budget_override_allow(record: &ApprovalRecord) -> bool {
    record.decision == Some(ApprovalDecision::Allow)
        && !record.decision_scope_ttl_ms.zip(record.resolved_at_unix_ms).is_some_and(
            |(ttl_ms, resolved_at)| {
                resolved_at.saturating_add(ttl_ms) <= crate::gateway::current_unix_ms()
            },
        )
}

fn provider_health_state(snapshot: &ProviderStatusSnapshot) -> &'static str {
    if snapshot.circuit_breaker.open || snapshot.runtime_metrics.error_count > 0 {
        "degraded"
    } else if snapshot.api_key_configured || snapshot.auth_profile_id.is_some() {
        "ok"
    } else {
        "missing_auth"
    }
}

fn complexity_score(
    prompt_text: &str,
    prompt_tokens_estimate: u64,
    json_mode: bool,
    vision_inputs: usize,
) -> f64 {
    let length_component = (prompt_text.len() as f64 / 4_000.0).clamp(0.0, 1.0);
    let token_component = (prompt_tokens_estimate as f64 / 3_000.0).clamp(0.0, 1.0);
    let json_component = if json_mode { 0.2 } else { 0.0 };
    let vision_component = (vision_inputs as f64 * 0.15).clamp(0.0, 0.3);
    let keyword_component = ["analyze", "architecture", "investigate", "regression", "security"]
        .iter()
        .filter(|keyword| prompt_text.to_ascii_lowercase().contains(**keyword))
        .count() as f64
        * 0.05;
    (length_component * 0.35
        + token_component * 0.35
        + json_component
        + vision_component
        + keyword_component)
        .clamp(0.0, 1.0)
}
