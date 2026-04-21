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
    gateway::{GatewayRuntimeState, RequestContext},
    model_provider::{ProviderEvent, ProviderImageInput, ProviderRequest, ProviderResponse},
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
const VISION_DEFAULT_BUDGET_TOKENS: u64 = 2_000;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AuxiliaryTaskType {
    Summary,
    RecallSearch,
    Classification,
    Extraction,
    Vision,
}

impl AuxiliaryTaskType {
    pub(crate) fn from_task_kind_str(value: &str) -> Option<Self> {
        match AuxiliaryTaskKind::from_str(value)? {
            AuxiliaryTaskKind::Summary => Some(Self::Summary),
            AuxiliaryTaskKind::RecallSearch => Some(Self::RecallSearch),
            AuxiliaryTaskKind::Classification => Some(Self::Classification),
            AuxiliaryTaskKind::Extraction => Some(Self::Extraction),
            AuxiliaryTaskKind::Vision => Some(Self::Vision),
            AuxiliaryTaskKind::BackgroundPrompt
            | AuxiliaryTaskKind::DelegationPrompt
            | AuxiliaryTaskKind::AttachmentDerivation
            | AuxiliaryTaskKind::AttachmentRecompute
            | AuxiliaryTaskKind::PostRunReflection => None,
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Summary => "summary",
            Self::RecallSearch => "recall_search",
            Self::Classification => "classification",
            Self::Extraction => "extraction",
            Self::Vision => "vision",
        }
    }

    pub(crate) const fn contract(self) -> AuxiliaryTaskContract {
        match self {
            Self::Summary => AuxiliaryTaskContract {
                task_type: self,
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
                input_contract: "plain_text_or_structured_payload",
                output_contract: "bounded_extracted_fields_json",
                default_budget_tokens: EXTRACTION_DEFAULT_BUDGET_TOKENS,
                model_preference: AuxiliaryModelPreference::LowCost,
                fallback_policy: AuxiliaryFallbackPolicy::DegradeToDefaultModel,
                routing_task_class: RoutingTaskClass::AuxiliaryExtraction,
                json_mode: true,
                accepts_vision: false,
            },
            Self::Vision => AuxiliaryTaskContract {
                task_type: self,
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

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
pub(crate) struct AuxiliaryTaskContract {
    pub task_type: AuxiliaryTaskType,
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

#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct AuxiliaryExecutionResult {
    pub task_id: String,
    pub task_type: AuxiliaryTaskType,
    pub output_text: String,
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
}

impl AuxiliaryExecutionResult {
    pub(crate) fn to_result_json(&self) -> Value {
        json!({
            "status": "succeeded",
            "task_id": self.task_id,
            "task_type": self.task_type.as_str(),
            "output_text": self.output_text,
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
            "routing": self.routing,
        })
    }
}

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
    let effective_budget = request
        .token_budget
        .unwrap_or(contract.default_budget_tokens)
        .max(1)
        .min(contract.default_budget_tokens);
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
    })
    .await?;
    record_auxiliary_lifecycle_event(
        runtime_state,
        &request.context,
        Some(request.session_id.as_str()),
        request.run_id.as_deref(),
        AuxiliaryLifecycleEventInput {
            task_id: request.task_id.as_str(),
            task_type: request.task_type.as_str(),
            phase: "started",
            reason: "auxiliary executor acquired usage routing plan",
            token_budget: Some(effective_budget),
            details: json!({
                "contract": contract,
                "model_preference": contract.model_preference.as_str(),
                "fallback_policy": contract.fallback_policy.as_str(),
                "routing": routing.clone(),
            }),
        },
    )
    .await?;

    let provider_model_override =
        (routing.mode == "enforced").then(|| routing.actual_model_id.clone());
    let (lease_provider_id, _lease_provider_kind, lease_credential_id) =
        provider_model_override.as_deref().map_or_else(
            || {
                (
                    routing.provider_id.clone(),
                    routing.provider_kind.clone(),
                    routing.credential_id.clone(),
                )
            },
            |model_id| resolve_provider_binding_for_model(&provider_snapshot, model_id),
        );
    match runtime_state
        .execute_model_provider_with_lease(
            ProviderRequest {
                input_text: input_text.to_owned(),
                json_mode: contract.json_mode,
                vision_inputs: request.vision_inputs.clone(),
                model_override: provider_model_override,
            },
            ProviderLeaseExecutionContext {
                provider_id: lease_provider_id,
                credential_id: lease_credential_id,
                priority: contract.routing_task_class.lease_priority(),
                task_label: contract.routing_task_class.as_str().to_owned(),
                max_wait_ms: contract.routing_task_class.max_lease_wait_ms(),
                session_id: Some(request.session_id.clone()),
                run_id: request.run_id.clone(),
            },
        )
        .await
    {
        Ok(response) => {
            let result = build_execution_result(
                request.task_id,
                request.task_type,
                contract,
                routing,
                response,
            );
            record_auxiliary_lifecycle_event(
                runtime_state,
                &request.context,
                Some(request.session_id.as_str()),
                request.run_id.as_deref(),
                AuxiliaryLifecycleEventInput {
                    task_id: result.task_id.as_str(),
                    task_type: result.task_type.as_str(),
                    phase: "completed",
                    reason: "auxiliary executor completed provider request",
                    token_budget: Some(effective_budget),
                    details: result.to_result_json(),
                },
            )
            .await?;
            Ok(result)
        }
        Err(error) => {
            let _ = record_auxiliary_lifecycle_event(
                runtime_state,
                &request.context,
                Some(request.session_id.as_str()),
                request.run_id.as_deref(),
                AuxiliaryLifecycleEventInput {
                    task_id: request.task_id.as_str(),
                    task_type: request.task_type.as_str(),
                    phase: "failed",
                    reason: "auxiliary executor provider request failed",
                    token_budget: Some(effective_budget),
                    details: json!({
                        "status_code": format!("{:?}", error.code()),
                        "error": error.message(),
                        "fallback_policy": contract.fallback_policy.as_str(),
                    }),
                },
            )
            .await;
            Err(error)
        }
    }
}

fn build_execution_result(
    task_id: String,
    task_type: AuxiliaryTaskType,
    contract: AuxiliaryTaskContract,
    routing: RoutingDecision,
    response: ProviderResponse,
) -> AuxiliaryExecutionResult {
    let output_text = response
        .events
        .iter()
        .filter_map(|event| match event {
            ProviderEvent::ModelToken { token, .. } => Some(token.as_str()),
            ProviderEvent::ToolProposal { .. } => None,
        })
        .collect::<String>();
    AuxiliaryExecutionResult {
        task_id,
        task_type,
        output_text,
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
    }
}

pub(crate) struct AuxiliaryLifecycleEventInput<'a> {
    pub task_id: &'a str,
    pub task_type: &'a str,
    pub phase: &'a str,
    pub reason: &'a str,
    pub token_budget: Option<u64>,
    pub details: Value,
}

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

#[cfg(test)]
mod tests {
    use super::{AuxiliaryFallbackPolicy, AuxiliaryModelPreference, AuxiliaryTaskType};

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
        assert_eq!(AuxiliaryTaskType::from_task_kind_str("background_prompt"), None);
    }

    #[test]
    fn auxiliary_contracts_define_budget_and_fallback_posture() {
        let summary = AuxiliaryTaskType::Summary.contract();
        assert_eq!(summary.default_budget_tokens, 1_200);
        assert_eq!(summary.model_preference, AuxiliaryModelPreference::LowCost);
        assert_eq!(summary.fallback_policy, AuxiliaryFallbackPolicy::DegradeToDefaultModel);
        assert!(!summary.json_mode);

        let classification = AuxiliaryTaskType::Classification.contract();
        assert_eq!(classification.default_budget_tokens, 600);
        assert_eq!(classification.model_preference, AuxiliaryModelPreference::LowLatency);
        assert_eq!(classification.fallback_policy, AuxiliaryFallbackPolicy::FailClosed);
        assert!(classification.json_mode);

        let vision = AuxiliaryTaskType::Vision.contract();
        assert!(vision.accepts_vision);
        assert_eq!(vision.model_preference, AuxiliaryModelPreference::VisionCapable);
    }
}
