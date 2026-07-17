//! Executor for bounded auxiliary model tasks (summary, recall search,
//! classification, extraction, vision) that run beside the main orchestrator
//! loop.
//!
//! Each task type carries a fixed [`AuxiliaryTaskContract`] (input/output
//! shape, token-budget ceiling, model preference, fallback posture) that is
//! also surfaced verbatim in lifecycle events and results. Execution routes
//! through `usage_governance` planning and provider leases, and every phase
//! (started/completed/failed) is recorded as a runtime decision event.

use std::sync::Arc;

use palyra_common::runtime_contracts::AuxiliaryTaskKind;
use palyra_common::runtime_preview::{
    RuntimeDecisionActorKind, RuntimeDecisionEventType, RuntimeDecisionPayload,
    RuntimeDecisionTiming, RuntimeEntityRef, RuntimeResourceBudget,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tonic::Status;

use crate::{
    gateway::{is_provider_reconfigured_status, GatewayRuntimeState, RequestContext},
    model_provider::{
        ProviderImageInput, ProviderRequest, ProviderResponse, ProviderStatusSnapshot,
    },
    objective_judge::{
        OBJECTIVE_JUDGE_COMPLETED_EVENT, OBJECTIVE_JUDGE_FAILED_EVENT,
        OBJECTIVE_JUDGE_STARTED_EVENT,
    },
    provider_leases::ProviderLeaseExecutionContext,
    usage_governance::{
        plan_usage_routing, resolve_provider_binding_for_model, RoutingDecision, RoutingTaskClass,
        UsageRoutingPlanRequest,
    },
};

const SUMMARY_DEFAULT_BUDGET_TOKENS: u64 = 1_200;
const RECALL_SEARCH_DEFAULT_BUDGET_TOKENS: u64 = 1_600;
const CLASSIFICATION_DEFAULT_BUDGET_TOKENS: u64 = 600;
const EXTRACTION_DEFAULT_BUDGET_TOKENS: u64 = 1_200;
const OBJECTIVE_JUDGE_DEFAULT_BUDGET_TOKENS: u64 = 900;
const VISION_DEFAULT_BUDGET_TOKENS: u64 = 2_000;
const AUXILIARY_OUTPUT_TEXT_LIMIT: usize = 4_000;
const MAX_AUXILIARY_PROVIDER_SUPERSESSION_RETRIES: u8 = 1;
const PROVIDER_RECONFIGURED_REASON_CODE: &str = "runtime.generation.provider_reconfigured";

/// Auxiliary task families this executor can run; a strict subset of
/// `AuxiliaryTaskKind` (queue-only kinds are handled elsewhere).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AuxiliaryTaskType {
    Summary,
    RecallSearch,
    Classification,
    Extraction,
    ObjectiveJudge,
    Vision,
}

/// Authority envelope enforced for an auxiliary task family.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AuxiliaryAuthorityProfile {
    ReadOnlyEvidence,
    FinalizationReview,
    BrowserRescuePreview,
}

impl AuxiliaryAuthorityProfile {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ReadOnlyEvidence => "read_only_evidence",
            Self::FinalizationReview => "finalization_review",
            Self::BrowserRescuePreview => "browser_rescue_preview",
        }
    }

    const fn tool_execution_allowed(self) -> bool {
        false
    }
}

impl AuxiliaryTaskType {
    /// Maps a runtime-contract task-kind string (including aliases) to an
    /// executor task type; returns `None` for kinds this executor does not
    /// run (background/delegation prompts, attachment work, reflection).
    pub(crate) fn from_task_kind_str(value: &str) -> Option<Self> {
        match AuxiliaryTaskKind::from_str(value)? {
            AuxiliaryTaskKind::Summary => Some(Self::Summary),
            AuxiliaryTaskKind::RecallSearch => Some(Self::RecallSearch),
            AuxiliaryTaskKind::Classification => Some(Self::Classification),
            AuxiliaryTaskKind::Extraction => Some(Self::Extraction),
            AuxiliaryTaskKind::ObjectiveJudge => Some(Self::ObjectiveJudge),
            AuxiliaryTaskKind::Vision => Some(Self::Vision),
            AuxiliaryTaskKind::BackgroundPrompt
            | AuxiliaryTaskKind::DelegationPrompt
            | AuxiliaryTaskKind::AttachmentDerivation
            | AuxiliaryTaskKind::AttachmentRecompute
            | AuxiliaryTaskKind::PostRunReflection => None,
        }
    }

    /// Returns the stable wire identifier used in lifecycle events and
    /// result JSON.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Summary => "summary",
            Self::RecallSearch => "recall_search",
            Self::Classification => "classification",
            Self::Extraction => "extraction",
            Self::ObjectiveJudge => "objective_judge",
            Self::Vision => "vision",
        }
    }

    /// Returns the fixed execution contract for this task type. Contracts
    /// are compile-time constants: budgets, fallback posture, and routing
    /// class are policy, not caller-tunable knobs.
    pub(crate) const fn contract(self) -> AuxiliaryTaskContract {
        match self {
            Self::Summary => AuxiliaryTaskContract {
                task_type: self,
                authority_profile: AuxiliaryAuthorityProfile::ReadOnlyEvidence,
                input_contract: "plain_text_context",
                output_contract: "bounded_plain_text_summary",
                default_budget_tokens: SUMMARY_DEFAULT_BUDGET_TOKENS,
                model_preference: AuxiliaryModelPreference::LowCost,
                fallback_policy: AuxiliaryFallbackPolicy::DegradeToDefaultModel,
                routing_task_class: RoutingTaskClass::AuxiliarySummary,
                json_mode: false,
                accepts_vision: false,
            },
            Self::RecallSearch => AuxiliaryTaskContract {
                task_type: self,
                authority_profile: AuxiliaryAuthorityProfile::ReadOnlyEvidence,
                input_contract: "query_plus_optional_context",
                output_contract: "ranked_recall_evidence_json",
                default_budget_tokens: RECALL_SEARCH_DEFAULT_BUDGET_TOKENS,
                model_preference: AuxiliaryModelPreference::LowCost,
                fallback_policy: AuxiliaryFallbackPolicy::DegradeToDefaultModel,
                routing_task_class: RoutingTaskClass::AuxiliaryRecall,
                json_mode: true,
                accepts_vision: false,
            },
            Self::Classification => AuxiliaryTaskContract {
                task_type: self,
                authority_profile: AuxiliaryAuthorityProfile::ReadOnlyEvidence,
                input_contract: "plain_text_or_structured_payload",
                output_contract: "single_label_json",
                default_budget_tokens: CLASSIFICATION_DEFAULT_BUDGET_TOKENS,
                model_preference: AuxiliaryModelPreference::LowLatency,
                fallback_policy: AuxiliaryFallbackPolicy::FailClosed,
                routing_task_class: RoutingTaskClass::AuxiliaryClassification,
                json_mode: true,
                accepts_vision: false,
            },
            Self::Extraction => AuxiliaryTaskContract {
                task_type: self,
                authority_profile: AuxiliaryAuthorityProfile::ReadOnlyEvidence,
                input_contract: "plain_text_or_structured_payload",
                output_contract: "bounded_extracted_fields_json",
                default_budget_tokens: EXTRACTION_DEFAULT_BUDGET_TOKENS,
                model_preference: AuxiliaryModelPreference::LowCost,
                fallback_policy: AuxiliaryFallbackPolicy::DegradeToDefaultModel,
                routing_task_class: RoutingTaskClass::AuxiliaryExtraction,
                json_mode: true,
                accepts_vision: false,
            },
            Self::ObjectiveJudge => AuxiliaryTaskContract {
                task_type: self,
                authority_profile: AuxiliaryAuthorityProfile::FinalizationReview,
                input_contract: "objective_judge_input_json",
                output_contract: "objective_judge_strict_json",
                default_budget_tokens: OBJECTIVE_JUDGE_DEFAULT_BUDGET_TOKENS,
                model_preference: AuxiliaryModelPreference::LowLatency,
                fallback_policy: AuxiliaryFallbackPolicy::FailClosed,
                routing_task_class: RoutingTaskClass::AuxiliaryClassification,
                json_mode: true,
                accepts_vision: false,
            },
            Self::Vision => AuxiliaryTaskContract {
                task_type: self,
                authority_profile: AuxiliaryAuthorityProfile::BrowserRescuePreview,
                input_contract: "prompt_plus_bounded_images",
                output_contract: "bounded_visual_observation_json",
                default_budget_tokens: VISION_DEFAULT_BUDGET_TOKENS,
                model_preference: AuxiliaryModelPreference::VisionCapable,
                fallback_policy: AuxiliaryFallbackPolicy::FailClosed,
                routing_task_class: RoutingTaskClass::AuxiliaryVision,
                json_mode: true,
                accepts_vision: true,
            },
        }
    }
}

/// Model-selection hint advertised by a task contract to routing.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AuxiliaryModelPreference {
    LowCost,
    LowLatency,
    VisionCapable,
}

impl AuxiliaryModelPreference {
    const fn as_str(self) -> &'static str {
        match self {
            Self::LowCost => "low_cost",
            Self::LowLatency => "low_latency",
            Self::VisionCapable => "vision_capable",
        }
    }
}

/// What happens when the preferred model is unavailable: degrade to the
/// default model, or fail the task outright (for correctness-critical kinds).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AuxiliaryFallbackPolicy {
    DegradeToDefaultModel,
    FailClosed,
}

impl AuxiliaryFallbackPolicy {
    const fn as_str(self) -> &'static str {
        match self {
            Self::DegradeToDefaultModel => "degrade_to_default_model",
            Self::FailClosed => "fail_closed",
        }
    }
}

/// Immutable execution contract for one auxiliary task type.
///
/// Serialized into lifecycle events and results, so field names are part of
/// the observable event shape. `default_budget_tokens` is both the default
/// and the ceiling for caller-supplied budgets.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
pub(crate) struct AuxiliaryTaskContract {
    pub task_type: AuxiliaryTaskType,
    pub authority_profile: AuxiliaryAuthorityProfile,
    pub input_contract: &'static str,
    pub output_contract: &'static str,
    pub default_budget_tokens: u64,
    pub model_preference: AuxiliaryModelPreference,
    pub fallback_policy: AuxiliaryFallbackPolicy,
    #[serde(skip)]
    pub routing_task_class: RoutingTaskClass,
    pub json_mode: bool,
    pub accepts_vision: bool,
}

/// One auxiliary task to execute; `run_id` is optional because tasks may be
/// triggered outside an orchestrator run (the task id then keys routing).
#[derive(Debug, Clone)]
pub(crate) struct AuxiliaryExecutionRequest {
    pub task_id: String,
    pub session_id: String,
    pub run_id: Option<String>,
    pub context: RequestContext,
    pub task_type: AuxiliaryTaskType,
    pub input_text: String,
    pub parameter_delta_json: Option<String>,
    pub token_budget: Option<u64>,
    pub vision_inputs: Vec<ProviderImageInput>,
}

/// Successful task outcome: provider output plus usage, provenance, and the
/// contract/routing decision that produced it.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct AuxiliaryExecutionResult {
    pub task_id: String,
    pub task_type: AuxiliaryTaskType,
    pub output_text: String,
    pub output_truncated: bool,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    pub provider_id: String,
    pub model_id: String,
    pub served_from_cache: bool,
    pub retry_count: u32,
    pub failover_count: u32,
    pub contract: AuxiliaryTaskContract,
    pub routing: RoutingDecision,
    pub lineage: Value,
}

impl AuxiliaryExecutionResult {
    /// Serializes the result into the stable JSON shape consumed by task
    /// records and lifecycle event details.
    pub(crate) fn to_result_json(&self) -> Value {
        let output_text = bounded_output_text(self.output_text.as_str());
        json!({
            "status": "succeeded",
            "task_id": self.task_id,
            "task_type": self.task_type.as_str(),
            "output_text": output_text,
            "output_truncated": self.output_truncated,
            "usage": {
                "prompt_tokens": self.prompt_tokens,
                "completion_tokens": self.completion_tokens,
                "total_tokens": self.total_tokens,
            },
            "provider": {
                "provider_id": self.provider_id,
                "model_id": self.model_id,
                "served_from_cache": self.served_from_cache,
                "retry_count": self.retry_count,
                "failover_count": self.failover_count,
            },
            "contract": self.contract,
            "authority": {
                "profile": self.contract.authority_profile.as_str(),
                "tool_execution_allowed": self.contract.authority_profile.tool_execution_allowed(),
                "output_projection": "bounded",
            },
            "lineage": self.lineage,
            "routing": self.routing,
        })
    }
}

/// Validates, routes, and executes one auxiliary task end to end, recording
/// started/completed/failed lifecycle events along the way.
///
/// # Errors
/// Returns `Status::invalid_argument` for empty input text or vision inputs
/// on a non-vision task, and propagates routing, lifecycle-recording, and
/// provider execution errors unchanged.
#[allow(clippy::result_large_err)]
pub(crate) async fn execute_auxiliary_task(
    runtime_state: &Arc<GatewayRuntimeState>,
    request: AuxiliaryExecutionRequest,
) -> Result<AuxiliaryExecutionResult, Status> {
    let contract = request.task_type.contract();
    if !contract.accepts_vision && !request.vision_inputs.is_empty() {
        return Err(Status::invalid_argument(format!(
            "auxiliary task '{}' does not accept vision inputs",
            request.task_type.as_str()
        )));
    }
    let input_text = request.input_text.trim();
    if input_text.is_empty() {
        return Err(Status::invalid_argument("auxiliary task input_text cannot be empty"));
    }
    // The contract budget is a ceiling, not just a default: callers may only
    // shrink the spend, never raise it above the per-task-type policy.
    let effective_budget = request
        .token_budget
        .unwrap_or(contract.default_budget_tokens)
        .clamp(1, contract.default_budget_tokens);
    let routing_run_id = request.run_id.as_deref().unwrap_or(request.task_id.as_str());
    let provider_snapshot = runtime_state.model_provider_status_snapshot();
    let routing = plan_usage_routing(UsageRoutingPlanRequest {
        runtime_state,
        request_context: &request.context,
        run_id: routing_run_id,
        session_id: request.session_id.as_str(),
        parameter_delta_json: request.parameter_delta_json.as_deref(),
        prompt_text: input_text,
        json_mode: contract.json_mode,
        vision_inputs: request.vision_inputs.len(),
        scope_kind: "auxiliary_task",
        scope_id: request.task_id.as_str(),
        task_class: contract.routing_task_class,
        provider_snapshot: &provider_snapshot,
        model_profile_override: None,
    })
    .await?;
    let started_reason = if request.task_type == AuxiliaryTaskType::ObjectiveJudge {
        OBJECTIVE_JUDGE_STARTED_EVENT
    } else {
        "auxiliary executor acquired usage routing plan"
    };
    record_auxiliary_lifecycle_event(
        runtime_state,
        &request.context,
        Some(request.session_id.as_str()),
        request.run_id.as_deref(),
        AuxiliaryLifecycleEventInput {
            task_id: request.task_id.as_str(),
            task_type: request.task_type.as_str(),
            phase: "started",
            reason: started_reason,
            token_budget: Some(effective_budget),
            details: json!({
                "contract": contract,
                "authority": {
                    "profile": contract.authority_profile.as_str(),
                    "tool_execution_allowed": contract.authority_profile.tool_execution_allowed(),
                },
                "lineage": auxiliary_lineage_projection(
                    request.task_id.as_str(),
                    request.session_id.as_str(),
                    request.run_id.as_deref(),
                    request.task_type,
                    contract.authority_profile,
                ),
                "model_preference": contract.model_preference.as_str(),
                "fallback_policy": contract.fallback_policy.as_str(),
                "routing": routing.clone(),
            }),
        },
    )
    .await?;

    // Only an "enforced" routing decision pins the model; advisory modes let
    // the provider runtime pick, so the lease binding falls back to the
    // routing plan's provider/credential pair below.
    let mut provider_model_override =
        (routing.mode == "enforced").then(|| routing.actual_model_id.clone());
    let (mut lease_provider_id, mut lease_credential_id) = auxiliary_provider_binding(
        &provider_snapshot,
        &routing,
        provider_model_override.as_deref(),
    );
    let mut supersession_retries = 0_u8;

    let response = loop {
        let provider_request = ProviderRequest::from_input_text(
            input_text.to_owned(),
            contract.json_mode,
            request.vision_inputs.clone(),
            provider_model_override.clone(),
        );
        match runtime_state
            .execute_model_provider_with_lease(
                provider_request,
                ProviderLeaseExecutionContext {
                    provider_id: lease_provider_id.clone(),
                    credential_id: lease_credential_id.clone(),
                    priority: contract.routing_task_class.lease_priority(),
                    task_label: contract.routing_task_class.as_str().to_owned(),
                    max_wait_ms: contract.routing_task_class.max_lease_wait_ms(),
                    session_id: Some(request.session_id.clone()),
                    run_id: request.run_id.clone(),
                    diagnostic_scope_id: Some(request.task_id.clone()),
                },
            )
            .await
        {
            Ok(response) => break response,
            Err(error)
                if is_provider_reconfigured_status(&error)
                    && supersession_retries < MAX_AUXILIARY_PROVIDER_SUPERSESSION_RETRIES =>
            {
                supersession_retries = supersession_retries.saturating_add(1);
                let replacement_snapshot = runtime_state.model_provider_status_snapshot();
                provider_model_override = replacement_auxiliary_model_override(
                    &replacement_snapshot,
                    provider_model_override.as_deref(),
                );
                (lease_provider_id, lease_credential_id) = auxiliary_provider_binding(
                    &replacement_snapshot,
                    &routing,
                    provider_model_override.as_deref(),
                );
                let _ = record_auxiliary_lifecycle_event(
                    runtime_state,
                    &request.context,
                    Some(request.session_id.as_str()),
                    request.run_id.as_deref(),
                    AuxiliaryLifecycleEventInput {
                        task_id: request.task_id.as_str(),
                        task_type: request.task_type.as_str(),
                        phase: "provider_handover",
                        reason: PROVIDER_RECONFIGURED_REASON_CODE,
                        token_budget: Some(effective_budget),
                        details: json!({
                            "retry_attempt": supersession_retries,
                            "max_retries": MAX_AUXILIARY_PROVIDER_SUPERSESSION_RETRIES,
                            "model_override": provider_model_override,
                        }),
                    },
                )
                .await;
            }
            Err(error) => {
                // Best-effort by design: surfacing the provider error to the
                // caller matters more than the failure journal entry, so a
                // journaling error here is deliberately discarded.
                let failed_reason = if request.task_type == AuxiliaryTaskType::ObjectiveJudge {
                    OBJECTIVE_JUDGE_FAILED_EVENT
                } else {
                    "auxiliary executor provider request failed"
                };
                let _ = record_auxiliary_lifecycle_event(
                    runtime_state,
                    &request.context,
                    Some(request.session_id.as_str()),
                    request.run_id.as_deref(),
                    AuxiliaryLifecycleEventInput {
                        task_id: request.task_id.as_str(),
                        task_type: request.task_type.as_str(),
                        phase: "failed",
                        reason: failed_reason,
                        token_budget: Some(effective_budget),
                        details: json!({
                            "status_code": format!("{:?}", error.code()),
                            "error": error.message(),
                            "fallback_policy": contract.fallback_policy.as_str(),
                            "provider_supersession_retries": supersession_retries,
                        }),
                    },
                )
                .await;
                return Err(error);
            }
        }
    };

    let result = build_execution_result(
        request.task_id,
        request.session_id.clone(),
        request.run_id.clone(),
        request.task_type,
        contract,
        routing,
        response,
    );
    let completed_reason = if result.task_type == AuxiliaryTaskType::ObjectiveJudge {
        OBJECTIVE_JUDGE_COMPLETED_EVENT
    } else {
        "auxiliary executor completed provider request"
    };
    record_auxiliary_lifecycle_event(
        runtime_state,
        &request.context,
        Some(request.session_id.as_str()),
        request.run_id.as_deref(),
        AuxiliaryLifecycleEventInput {
            task_id: result.task_id.as_str(),
            task_type: result.task_type.as_str(),
            phase: "completed",
            reason: completed_reason,
            token_budget: Some(effective_budget),
            details: result.to_result_json(),
        },
    )
    .await?;
    Ok(result)
}

fn auxiliary_provider_binding(
    provider_snapshot: &ProviderStatusSnapshot,
    routing: &RoutingDecision,
    provider_model_override: Option<&str>,
) -> (String, String) {
    let (provider_id, _provider_kind, credential_id) = provider_model_override.map_or_else(
        || {
            (
                routing.provider_id.clone(),
                routing.provider_kind.clone(),
                routing.credential_id.clone(),
            )
        },
        |model_id| resolve_provider_binding_for_model(provider_snapshot, model_id),
    );
    (provider_id, credential_id)
}

fn replacement_auxiliary_model_override(
    provider_snapshot: &ProviderStatusSnapshot,
    previous_model_override: Option<&str>,
) -> Option<String> {
    previous_model_override
        .filter(|model_id| {
            provider_snapshot
                .registry
                .models
                .iter()
                .any(|model| model.model_id == *model_id && model.enabled)
        })
        .map(ToOwned::to_owned)
        .or_else(|| provider_snapshot.route_selection.selected_model_id.clone())
        .or_else(|| provider_snapshot.registry.default_chat_model_id.clone())
        .or_else(|| provider_snapshot.model_id.clone())
}

fn build_execution_result(
    task_id: String,
    session_id: String,
    run_id: Option<String>,
    task_type: AuxiliaryTaskType,
    contract: AuxiliaryTaskContract,
    routing: RoutingDecision,
    response: ProviderResponse,
) -> AuxiliaryExecutionResult {
    let output_truncated = response.output.full_text.chars().count() > AUXILIARY_OUTPUT_TEXT_LIMIT;
    let lineage = auxiliary_lineage_projection(
        task_id.as_str(),
        session_id.as_str(),
        run_id.as_deref(),
        task_type,
        contract.authority_profile,
    );
    AuxiliaryExecutionResult {
        task_id,
        task_type,
        output_text: response.output.full_text,
        output_truncated,
        prompt_tokens: response.prompt_tokens,
        completion_tokens: response.completion_tokens,
        total_tokens: response.prompt_tokens.saturating_add(response.completion_tokens),
        provider_id: response.provider_id,
        model_id: response.model_id,
        served_from_cache: response.served_from_cache,
        retry_count: response.retry_count,
        failover_count: response.failover_count,
        contract,
        routing,
        lineage,
    }
}

fn auxiliary_lineage_projection(
    task_id: &str,
    session_id: &str,
    run_id: Option<&str>,
    task_type: AuxiliaryTaskType,
    authority_profile: AuxiliaryAuthorityProfile,
) -> Value {
    json!({
        "task_id": task_id,
        "session_id": session_id,
        "run_id": run_id,
        "task_type": task_type.as_str(),
        "authority_profile": authority_profile.as_str(),
        "result_kind": match authority_profile {
            AuxiliaryAuthorityProfile::FinalizationReview => "finalization_review",
            AuxiliaryAuthorityProfile::BrowserRescuePreview => "evidence_segment",
            AuxiliaryAuthorityProfile::ReadOnlyEvidence => "evidence_segment",
        },
    })
}

fn bounded_output_text(value: &str) -> String {
    value.chars().take(AUXILIARY_OUTPUT_TEXT_LIMIT).collect()
}

/// Borrowed inputs for one auxiliary lifecycle event
/// (phase: `started`, `completed`, or `failed`).
pub(crate) struct AuxiliaryLifecycleEventInput<'a> {
    pub task_id: &'a str,
    pub task_type: &'a str,
    pub phase: &'a str,
    pub reason: &'a str,
    pub token_budget: Option<u64>,
    pub details: Value,
}

/// Records an auxiliary-task lifecycle phase as a runtime decision event so
/// task progress is observable in the journal/preview surfaces.
///
/// # Errors
/// Propagates the runtime's decision-event recording failure unchanged.
#[allow(clippy::result_large_err)]
pub(crate) async fn record_auxiliary_lifecycle_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    session_id: Option<&str>,
    run_id: Option<&str>,
    event: AuxiliaryLifecycleEventInput<'_>,
) -> Result<(), Status> {
    let budget = RuntimeResourceBudget {
        token_budget: event.token_budget,
        ..RuntimeResourceBudget::default()
    };
    let payload = RuntimeDecisionPayload::new(
        RuntimeDecisionEventType::AuxiliaryTaskLifecycle,
        runtime_state
            .runtime_decision_actor_from_context(context, RuntimeDecisionActorKind::System),
        event.reason,
        "auxiliary_executor.lifecycle",
        RuntimeDecisionTiming::observed(crate::gateway::current_unix_ms()),
    )
    .with_input(
        RuntimeEntityRef::new("task", "auxiliary_task", event.task_id)
            .with_state(event.phase.to_owned()),
    )
    .with_resource_budget(budget)
    .with_details(json!({
        "task_type": event.task_type,
        "phase": event.phase,
        "details": event.details,
    }));
    runtime_state.record_runtime_decision_event(context, session_id, run_id, payload).await
}

/// Evidence sufficiency input for stopping auxiliary fanout early.
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AuxiliaryStopConditionInput {
    pub required_evidence_count: u64,
    pub observed_evidence_count: u64,
    pub required_safety_warning_count: u64,
    pub observed_safety_warning_count: u64,
    pub outstanding_child_count: u64,
}

/// Decision describing whether additional auxiliary work should stop.
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AuxiliaryStopConditionDecision {
    pub stop: bool,
    pub reason_code: String,
    pub bounded: bool,
}

/// Decides whether auxiliary fanout can stop because the parent has enough evidence.
#[allow(dead_code)]
#[must_use]
pub(crate) fn auxiliary_stop_condition(
    input: AuxiliaryStopConditionInput,
) -> AuxiliaryStopConditionDecision {
    if input.observed_evidence_count < input.required_evidence_count {
        return AuxiliaryStopConditionDecision {
            stop: false,
            reason_code: "auxiliary_stop_condition.insufficient_evidence".to_owned(),
            bounded: true,
        };
    }
    if input.observed_safety_warning_count < input.required_safety_warning_count {
        return AuxiliaryStopConditionDecision {
            stop: false,
            reason_code: "auxiliary_stop_condition.safety_review_pending".to_owned(),
            bounded: true,
        };
    }
    if input.outstanding_child_count > 0 {
        return AuxiliaryStopConditionDecision {
            stop: false,
            reason_code: "auxiliary_stop_condition.children_still_running".to_owned(),
            bounded: true,
        };
    }
    AuxiliaryStopConditionDecision {
        stop: true,
        reason_code: "auxiliary_stop_condition.evidence_satisfied".to_owned(),
        bounded: true,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::{mpsc, Notify};
    use ulid::Ulid;

    use super::{
        auxiliary_stop_condition, execute_auxiliary_task, AuxiliaryAuthorityProfile,
        AuxiliaryExecutionRequest, AuxiliaryFallbackPolicy, AuxiliaryModelPreference,
        AuxiliaryStopConditionInput, AuxiliaryTaskType,
    };
    use crate::{
        gateway::{
            runtime::tests::{provider_status_snapshot, SuccessfulModelProvider},
            tests::build_test_runtime_state,
            RequestContext,
        },
        journal::OrchestratorSessionUpsertRequest,
        model_provider::{
            AudioTranscriptionRequest, AudioTranscriptionResponse, ModelProvider, ProviderError,
            ProviderRequest, ProviderResponse, ProviderStatusSnapshot,
        },
    };
    use std::{future::Future, pin::Pin};

    struct BlockingAuxiliaryProvider {
        started: mpsc::Sender<()>,
        release: Arc<Notify>,
        status: ProviderStatusSnapshot,
    }

    impl ModelProvider for BlockingAuxiliaryProvider {
        fn complete<'a>(
            &'a self,
            _request: ProviderRequest,
        ) -> Pin<Box<dyn Future<Output = Result<ProviderResponse, ProviderError>> + Send + 'a>>
        {
            Box::pin(async move {
                self.started
                    .send(())
                    .await
                    .expect("auxiliary supersession receiver should remain open");
                self.release.notified().await;
                Err(ProviderError::StatePoisoned)
            })
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
            self.status.clone()
        }
    }

    #[tokio::test]
    async fn auxiliary_provider_supersession_retries_without_failed_lifecycle() {
        let state = build_test_runtime_state(false);
        let session_id = Ulid::new().to_string();
        let task_id = Ulid::new().to_string();
        state
            .journal_store
            .upsert_orchestrator_session(&OrchestratorSessionUpsertRequest {
                session_id: session_id.clone(),
                session_key: format!("auxiliary:{session_id}"),
                session_label: Some("Auxiliary provider supersession".to_owned()),
                principal: "user:test".to_owned(),
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                channel: Some("test".to_owned()),
            })
            .expect("auxiliary test session should be created");

        let (started_tx, mut started_rx) = mpsc::channel(1);
        let release = Arc::new(Notify::new());
        let _ = state.configure_model_provider(Arc::new(BlockingAuxiliaryProvider {
            started: started_tx,
            release: Arc::clone(&release),
            status: state.model_provider_status_snapshot(),
        }));
        let execution_state = Arc::clone(&state);
        let execution_session_id = session_id.clone();
        let execution_task_id = task_id.clone();
        let execution = tokio::spawn(async move {
            execute_auxiliary_task(
                &execution_state,
                AuxiliaryExecutionRequest {
                    task_id: execution_task_id,
                    session_id: execution_session_id,
                    run_id: None,
                    context: RequestContext {
                        principal: "user:test".to_owned(),
                        device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                        channel: Some("test".to_owned()),
                    },
                    task_type: AuxiliaryTaskType::Summary,
                    input_text: "summarize through a replacement provider".to_owned(),
                    parameter_delta_json: None,
                    token_budget: Some(128),
                    vision_inputs: Vec::new(),
                },
            )
            .await
        });

        started_rx.recv().await.expect("initial auxiliary provider call should start");
        let (requests_tx, mut requests_rx) = mpsc::channel(1);
        let replacement_status = provider_status_snapshot(false);
        let _ = state.configure_model_provider(Arc::new(SuccessfulModelProvider {
            requests: requests_tx,
            response_text: "replacement auxiliary response",
            status: replacement_status,
        }));
        release.notify_one();

        let result = execution
            .await
            .expect("auxiliary execution task should join")
            .expect("auxiliary execution should retry through the replacement provider");
        assert_eq!(result.output_text, "replacement auxiliary response");
        assert!(!result.output_text.contains("stale"));
        let replacement_request =
            requests_rx.recv().await.expect("replacement auxiliary provider should run once");
        assert_eq!(replacement_request.model_override.as_deref(), Some("gpt-4o-mini"));
        assert!(requests_rx.try_recv().is_err());
        assert_eq!(
            state
                .journal_store
                .runtime_stale_event_diagnostic_count_for_scope(
                    session_id.as_str(),
                    task_id.as_str(),
                    "runtime.generation.provider_reconfigured",
                )
                .expect("auxiliary stale diagnostic count should load"),
            1
        );
        let lifecycle_events = state
            .journal_store
            .recent_for_run(session_id.as_str(), 16)
            .expect("auxiliary lifecycle events should load");
        let phases = lifecycle_events
            .iter()
            .filter_map(|event| serde_json::from_str::<serde_json::Value>(&event.payload_json).ok())
            .filter_map(|payload| {
                payload
                    .get("payload")?
                    .get("details")?
                    .get("phase")?
                    .as_str()
                    .map(ToOwned::to_owned)
            })
            .collect::<Vec<_>>();
        assert!(phases.iter().any(|phase| phase == "started"));
        assert!(phases.iter().any(|phase| phase == "provider_handover"));
        assert!(phases.iter().any(|phase| phase == "completed"));
        assert!(!phases.iter().any(|phase| phase == "failed"));
    }

    #[test]
    fn auxiliary_task_kind_aliases_resolve_to_executor_types() {
        assert_eq!(
            AuxiliaryTaskType::from_task_kind_str("auxiliary_summary"),
            Some(AuxiliaryTaskType::Summary)
        );
        assert_eq!(
            AuxiliaryTaskType::from_task_kind_str("recall_search"),
            Some(AuxiliaryTaskType::RecallSearch)
        );
        assert_eq!(
            AuxiliaryTaskType::from_task_kind_str("objective_judge"),
            Some(AuxiliaryTaskType::ObjectiveJudge)
        );
        assert_eq!(AuxiliaryTaskType::from_task_kind_str("background_prompt"), None);
    }

    #[test]
    fn auxiliary_contracts_define_budget_and_fallback_posture() {
        let summary = AuxiliaryTaskType::Summary.contract();
        assert_eq!(summary.authority_profile, AuxiliaryAuthorityProfile::ReadOnlyEvidence);
        assert_eq!(summary.default_budget_tokens, 1_200);
        assert_eq!(summary.model_preference, AuxiliaryModelPreference::LowCost);
        assert_eq!(summary.fallback_policy, AuxiliaryFallbackPolicy::DegradeToDefaultModel);
        assert!(!summary.json_mode);

        let classification = AuxiliaryTaskType::Classification.contract();
        assert_eq!(classification.default_budget_tokens, 600);
        assert_eq!(classification.model_preference, AuxiliaryModelPreference::LowLatency);
        assert_eq!(classification.fallback_policy, AuxiliaryFallbackPolicy::FailClosed);
        assert!(classification.json_mode);

        let objective_judge = AuxiliaryTaskType::ObjectiveJudge.contract();
        assert_eq!(
            objective_judge.authority_profile,
            AuxiliaryAuthorityProfile::FinalizationReview
        );
        assert_eq!(objective_judge.default_budget_tokens, 900);
        assert_eq!(objective_judge.model_preference, AuxiliaryModelPreference::LowLatency);
        assert_eq!(objective_judge.fallback_policy, AuxiliaryFallbackPolicy::FailClosed);
        assert!(objective_judge.json_mode);

        let vision = AuxiliaryTaskType::Vision.contract();
        assert!(vision.accepts_vision);
        assert_eq!(vision.authority_profile, AuxiliaryAuthorityProfile::BrowserRescuePreview);
        assert_eq!(vision.model_preference, AuxiliaryModelPreference::VisionCapable);
    }

    #[test]
    fn auxiliary_stop_condition_waits_for_evidence_and_children() {
        let insufficient = auxiliary_stop_condition(AuxiliaryStopConditionInput {
            required_evidence_count: 2,
            observed_evidence_count: 1,
            required_safety_warning_count: 0,
            observed_safety_warning_count: 0,
            outstanding_child_count: 0,
        });
        assert!(!insufficient.stop);
        assert_eq!(insufficient.reason_code, "auxiliary_stop_condition.insufficient_evidence");

        let child_running = auxiliary_stop_condition(AuxiliaryStopConditionInput {
            required_evidence_count: 2,
            observed_evidence_count: 2,
            required_safety_warning_count: 0,
            observed_safety_warning_count: 0,
            outstanding_child_count: 1,
        });
        assert!(!child_running.stop);
        assert_eq!(child_running.reason_code, "auxiliary_stop_condition.children_still_running");

        let satisfied = auxiliary_stop_condition(AuxiliaryStopConditionInput {
            required_evidence_count: 2,
            observed_evidence_count: 2,
            required_safety_warning_count: 1,
            observed_safety_warning_count: 1,
            outstanding_child_count: 0,
        });
        assert!(satisfied.stop);
        assert_eq!(satisfied.reason_code, "auxiliary_stop_condition.evidence_satisfied");
    }
}
