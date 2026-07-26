//! Authoritative RuntimeKernelV2 driver for the gRPC RunStream host.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use palyra_common::{
    qa_runtime_path::{
        ProviderLaneAttestationEvent, CONTEXT_ENGINE_BINDING_EVENT, PROVIDER_LANE_ATTESTATION_EVENT,
    },
    runtime_contracts::{
        RuntimeAttemptId, RuntimeDeliveryIntentId, RuntimeErrorClass, RuntimeErrorEnvelopeV1,
        RuntimeErrorEnvelopeV1Input, RuntimeErrorPhase, RuntimeErrorSecurityClass,
        RuntimeErrorUserVisibility, RuntimeEventId, RuntimeGenerationLane, RuntimeOperationId,
        RuntimeRetryability, RuntimeSubsystem, RuntimeTerminalOutcome, RuntimeToolProposalId,
        RUNTIME_KERNEL_V2_PROVIDER_EFFECT_STARTED_MESSAGE,
    },
    CANONICAL_PROTOCOL_MAJOR,
};
use tonic::{Status, Streaming};
use ulid::Ulid;

use crate::{
    application::{
        context_recovery::{
            context_recovery_input_for_request, estimated_required_tokens_for_request,
            reduce_optional_context, truncate_old_tool_tails, ContextRecoveryAction,
            ContextRecoveryController, ContextRecoveryPlan, ContextRecoveryStep,
            TokenBreakdownCategory, CONTEXT_RECOVERY_EVENT,
        },
        provider_turn_recovery::{
            anomaly_from_terminal_validation, ProviderAttemptOutcome, ProviderAttemptPlan,
            ProviderAttemptStateMachine, ProviderRecoveryCommand, ProviderRecoverySideEffectState,
            ProviderTurnAnomaly, ProviderTurnRecoveryDecision, ProviderTurnRecoveryInput,
            RecoveryActionOutcome, RecoveryExecutorInput, PROVIDER_ATTEMPT_OUTCOME_EVENT,
            PROVIDER_ATTEMPT_PLAN_EVENT, PROVIDER_TURN_RECOVERY_EVENT,
            RECOVERY_ACTION_STARTED_EVENT,
        },
        run_admission::PersistedV2AdmissionToken,
        run_stream::{
            cancellation::request_persisted_run_interrupt,
            embedded_attempt::{
                EmbeddedDeliveryPlan, EmbeddedProviderTurn, ProductionEmbeddedAttemptFactory,
            },
            flow_control::RunStreamFlowControl,
            tape::{
                redact_run_stream_text, tool_result_event, tool_result_tape_payload,
                RUN_STREAM_RESPONSE_CHANNEL_CLOSED_MESSAGE,
            },
            tool_flow::{RunStreamLiveToolHost, RunStreamRetainedToolProposal},
        },
        runtime_kernel_v2::{
            context::{
                KernelAuthorityError, KernelLaneAuthority, RuntimeKernelContext,
                RuntimeKernelLifecycleServices, RuntimeKernelServices, RuntimeKernelTurnServices,
            },
            finalization::{
                DeliveryOutboxPort, JournalDeliveryService, JournalFinalizationService,
                RetainedFinalDelivery, RunFinalProjectionStore,
            },
            harness::{
                HarnessAttemptRequest, HarnessContractError, HarnessFuture, HarnessTerminalOutcome,
            },
            host_event_contract::{HarnessDeliveryBinding, HarnessTerminalReceipt},
            host_event_sink::HostHarnessEventSink,
            phases::{
                CompactionResult, DeliveryRequest, DeliveryResult, FinalizationReceipt,
                FinalizationRequest, ToolProposalRequest, ToolResultProjection,
            },
            production_flow::ProductionKernelFlowAuthorities,
            production_services::{
                compaction::{RunStreamCompactionInput, RunStreamCompactionProjectionStore},
                context_assembly::{
                    v2_context_retained_token_estimate, PreassembledContextAssemblyInput,
                },
                PreparedProductionProviderTurn, ProductionAttemptCallbacks,
                ProductionServiceBundle,
            },
            selection_host::select_production_runtime,
            KernelLaneAuthoritySet, KernelTerminalOutcome, RuntimeKernelV2,
        },
        tool_registry::{snapshot_to_provider_request_value, ModelVisibleToolCatalogSnapshot},
    },
    gateway::{
        cleanup_run_resources, GatewayRuntimeState, CANCELLED_REASON,
        MAX_MODEL_TOKEN_TAPE_EVENTS_PER_RUN,
    },
    journal::{
        OrchestratorRunTerminalSettlement, OrchestratorRunTerminalSettlementRequest,
        OrchestratorTapeAppendRequest, OrchestratorTerminalTapeEvent, OrchestratorUsageDelta,
    },
    model_provider::{
        bounded_provider_turn_output_for_persistence, normalized_provider_stream_from_output_v2,
        provider_events_from_output, ProviderEvent, ProviderFinishReason, ProviderMessage,
        ProviderMessageRole, ProviderOutputContentPart, ProviderRawProviderRefs, ProviderRequest,
        ProviderResponse, ProviderTerminalDisposition, ProviderTerminalValidationOutcome,
        ProviderTurnOutput, ProviderUsage, TerminalOutcomeClass, TerminalOutcomeClassification,
        PROVIDER_TERMINAL_VALIDATION_AUDIT_EVENT,
    },
    orchestrator::{RunLifecycleState, RunStateMachine},
    provider_leases::ProviderLeaseExecutionContext,
    self_healing::WorkHeartbeatKind,
    tool_protocol::ToolExecutionOutcome,
    transport::grpc::{auth::RequestContext, proto::palyra::common::v1 as common_v1},
    usage_governance::RoutingTaskClass,
};

use super::{
    append_agent_loop_tape_event, persist_accepted_final_reply_side_effects,
    run_runtime_path_summary_payload, send_settled_final_status, status_tape_payload,
    user_requested_summary_only_closeout, RunStreamMessageProcessingOutcome,
    RUNTIME_SELECTED_METADATA_EVENT, RUNTIME_SELECTED_METADATA_SCHEMA_V1,
};

struct RunStreamV2Callbacks {
    state: Mutex<CallbackState>,
    sender: tokio::sync::mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    run_id: String,
    base_request: ProviderRequest,
    lease: ProviderLeaseExecutionContext,
    catalog: ModelVisibleToolCatalogSnapshot,
    proposal_retention: crate::application::run_stream::tool_flow::RunStreamToolProposalRetention,
    compaction_projections: Arc<RunStreamCompactionProjectionStore>,
    finalization: V2FinalizationProjection,
}

struct V2FinalizationProjection {
    projections: Arc<RunFinalProjectionStore>,
    delivery: Option<RetainedFinalDelivery>,
}

struct V2CallbackResources {
    proposal_retention: crate::application::run_stream::tool_flow::RunStreamToolProposalRetention,
    compaction_projections: Arc<RunStreamCompactionProjectionStore>,
    finalization: V2FinalizationProjection,
}

struct CallbackState {
    first_provider_turn: bool,
    messages: Vec<ProviderMessage>,
    compacted_input_text: Option<String>,
    model_override: Option<String>,
    max_output_tokens: Option<u64>,
    drop_vision_inputs: bool,
    final_text: Option<String>,
    final_projection: Option<crate::application::runtime_kernel_v2::phases::FinalProjectionRef>,
    deferred_tape_events: Vec<V2DeferredTapeEvent>,
    after_turn_observations: Vec<ContextAfterTurnObservation>,
    context_recovery: ContextRecoveryController,
    pending_context_recovery_step: Option<ContextRecoveryStep>,
    provider_recovery: ProviderAttemptStateMachine,
    current_attempt_plan: ProviderAttemptPlan,
}

#[derive(Debug, Clone)]
struct ContextAfterTurnObservation {
    prompt_tokens: u64,
    completion_tokens: u64,
    tool_exchange_count: u64,
    finish_reason: Option<ProviderFinishReason>,
}

enum V2ProviderRecoveryDisposition {
    Retry { reason_code: String, delay_ms: Option<u64> },
    CompactionRequired,
    Stop { reason_code: String },
}

enum V2DeferredTapeEvent {
    ProviderAttemptPlan(Box<ProviderAttemptPlan>),
    ProviderAttemptOutcome(Box<ProviderAttemptOutcome>),
    ProviderRecoveryDecision(Box<ProviderTurnRecoveryDecision>),
    ProviderRecoveryStarted(serde_json::Value),
    ProviderRecoveryOutcome(Box<RecoveryActionOutcome>),
    ProviderLaneAttested(Box<ProviderLaneAttestationEvent>),
    ProviderTerminalValidation(Box<ProviderTerminalValidationOutcome>),
    CompactionRequired,
    CompactionApplied { artifact_id_sha256: String },
    CompactionSkipped { reason_code: String },
    ContextRecoveryPlan(Box<ContextRecoveryPlan>),
    ContextRecoveryAction { event_type: &'static str, action: String, reason_code: String },
    ToolDenied { proposal_id: String, reason_code: &'static str },
}

impl RunStreamV2Callbacks {
    fn new(
        sender: tokio::sync::mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
        run_id: String,
        base_request: ProviderRequest,
        lease: ProviderLeaseExecutionContext,
        catalog: ModelVisibleToolCatalogSnapshot,
        context_recovery: ContextRecoveryController,
        resources: V2CallbackResources,
    ) -> Self {
        let network_authority = format!("{}:{}", lease.provider_id, lease.task_label);
        let tool_authority = serde_json::to_vec(&snapshot_to_provider_request_value(&catalog))
            .map(|value| crate::sha256_hex(value.as_slice()))
            .unwrap_or_else(|_| crate::sha256_hex(b"v2_tool_authority_unavailable"));
        let mut provider_recovery = ProviderAttemptStateMachine::for_request(
            &base_request,
            network_authority.as_str(),
            tool_authority.as_str(),
        );
        let initial_model_id =
            base_request.model_override.as_deref().unwrap_or("default").to_owned();
        let initial_attempt_plan = provider_recovery.plan_attempt(
            &base_request,
            lease.provider_id.as_str(),
            lease.credential_id.as_str(),
            initial_model_id.as_str(),
        );
        Self {
            state: Mutex::new(CallbackState {
                first_provider_turn: true,
                messages: base_request.effective_messages(),
                compacted_input_text: None,
                model_override: base_request.model_override.clone(),
                max_output_tokens: base_request.max_output_tokens,
                drop_vision_inputs: false,
                final_text: None,
                final_projection: None,
                deferred_tape_events: vec![V2DeferredTapeEvent::ProviderAttemptPlan(Box::new(
                    initial_attempt_plan.clone(),
                ))],
                after_turn_observations: Vec::new(),
                context_recovery,
                pending_context_recovery_step: None,
                provider_recovery,
                current_attempt_plan: initial_attempt_plan,
            }),
            sender,
            run_id,
            base_request,
            lease,
            catalog,
            proposal_retention: resources.proposal_retention,
            compaction_projections: resources.compaction_projections,
            finalization: resources.finalization,
        }
    }

    fn final_text(&self) -> Option<String> {
        self.state.lock().ok()?.final_text.clone()
    }

    fn take_deferred_tape_events(&self) -> Result<Vec<V2DeferredTapeEvent>, Status> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Status::internal("V2 provider observation state is unavailable"))?;
        Ok(std::mem::take(&mut state.deferred_tape_events))
    }

    fn take_after_turn_observations(&self) -> Result<Vec<ContextAfterTurnObservation>, Status> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Status::internal("V2 context lifecycle state is unavailable"))?;
        Ok(std::mem::take(&mut state.after_turn_observations))
    }

    fn retain_tool_proposal(
        &self,
        output: &ProviderTurnOutput,
    ) -> Result<Option<(ToolProposalRequest, RuntimeOperationId)>, RuntimeErrorEnvelopeV1> {
        let mut proposal = None;
        for part in &output.content_parts {
            let ProviderOutputContentPart::ToolCall { proposal_id, tool_name, input_json } = part
            else {
                continue;
            };
            if proposal.is_some() {
                return Err(
                    self.kernel_failure("runtime.provider.multiple_tool_calls_unsupported_v2")
                );
            }
            let retained = RunStreamRetainedToolProposal {
                proposal_id: proposal_id.clone(),
                tool_name: tool_name.clone(),
                input_json: serde_json::to_vec(input_json)
                    .map_err(|_| self.kernel_failure("runtime.provider.tool_arguments_invalid"))?,
                catalog: self.catalog.clone(),
            };
            let request = self
                .proposal_retention
                .retain_provider_proposal(retained)
                .map_err(|_| self.kernel_failure("runtime.provider.tool_proposal_invalid"))?;
            let operation_id =
                RuntimeOperationId::parse(format!("tool-operation:{}", Ulid::new()).as_str())
                    .map_err(|_| self.kernel_failure("runtime.provider.tool_operation_invalid"))?;
            proposal = Some((request, operation_id));
        }
        Ok(proposal)
    }

    fn project_tool_outcome(
        proposal_id: &str,
        outcome: ToolExecutionOutcome,
    ) -> Result<ProviderMessage, RuntimeErrorEnvelopeV1> {
        let output = serde_json::from_slice::<serde_json::Value>(&outcome.output_json)
            .unwrap_or_else(
                |_| serde_json::json!({ "raw": String::from_utf8_lossy(&outcome.output_json) }),
            );
        let serialized = serde_json::to_string(&serde_json::json!({
            "success": outcome.success,
            "error": outcome.error,
            "output": output,
        }))
        .map_err(|_| kernel_failure("runtime.tool.result_projection_invalid"))?;
        let redacted =
            crate::journal::redact_payload_json(serialized.as_bytes()).unwrap_or(serialized);
        Ok(ProviderMessage::tool_result(proposal_id.to_owned(), redacted))
    }

    fn projected_recovery_request(&self, state: &CallbackState) -> ProviderRequest {
        let mut request = self.base_request.clone();
        request.messages = state.messages.clone();
        if let Some(input_text) = state.compacted_input_text.as_ref() {
            request.input_text = input_text.clone();
        }
        request.model_override = state.model_override.clone();
        request.max_output_tokens = state.max_output_tokens;
        if state.drop_vision_inputs {
            request.vision_inputs.clear();
        }
        request.tool_catalog_snapshot = Some(snapshot_to_provider_request_value(&self.catalog));
        request
    }

    const fn recovery_action_label(action: ContextRecoveryAction) -> &'static str {
        match action {
            ContextRecoveryAction::Compact => "compact",
            ContextRecoveryAction::TruncateOldToolTails => "truncate_old_tool_tails",
            ContextRecoveryAction::ReduceOptionalContext => "reduce_optional_context",
            ContextRecoveryAction::RouteLargerWindow => "route_larger_window",
            ContextRecoveryAction::FailDeterministic => "fail_deterministic",
        }
    }

    fn continue_prompt_local_recovery(
        &self,
        state: &mut CallbackState,
    ) -> Result<(), RuntimeErrorEnvelopeV1> {
        loop {
            let step = state
                .context_recovery
                .next_step()
                .map_err(|_| self.kernel_failure("runtime.context_recovery.controller_failed"))?
                .ok_or_else(|| self.kernel_failure("runtime.context_recovery.budget_exhausted"))?;
            let action = Self::recovery_action_label(step.action);
            state.deferred_tape_events.push(V2DeferredTapeEvent::ContextRecoveryAction {
                event_type: "recovery.action.started",
                action: action.to_owned(),
                reason_code: step.reason_code.clone(),
            });

            let (after_tokens, removed_categories, evidence_retained) = match step.action {
                ContextRecoveryAction::Compact => {
                    return Err(
                        self.kernel_failure("runtime.context_recovery.compaction_reentered")
                    );
                }
                ContextRecoveryAction::TruncateOldToolTails => {
                    let mutation = truncate_old_tool_tails(state.messages.as_mut_slice());
                    let evidence_retained = mutation.removed_categories.is_empty()
                        || !mutation.evidence_refs.is_empty();
                    let request = self.projected_recovery_request(state);
                    (
                        estimated_required_tokens_for_request(&request, &self.catalog),
                        mutation.removed_categories,
                        evidence_retained,
                    )
                }
                ContextRecoveryAction::ReduceOptionalContext => {
                    let mutation = reduce_optional_context(&mut state.messages);
                    let request = self.projected_recovery_request(state);
                    (
                        estimated_required_tokens_for_request(&request, &self.catalog),
                        mutation.removed_categories,
                        true,
                    )
                }
                ContextRecoveryAction::RouteLargerWindow
                | ContextRecoveryAction::FailDeterministic => {
                    (state.context_recovery.current_tokens(), Vec::new(), true)
                }
            };
            let outcome = state
                .context_recovery
                .record_outcome(&step, after_tokens, removed_categories, evidence_retained)
                .map_err(|_| {
                    self.kernel_failure("runtime.context_recovery.executor_outcome_invalid")
                })?;
            if let Some(route) = outcome.route_fallback.as_ref() {
                state.model_override = Some(route.model_id.clone());
            }
            state.deferred_tape_events.push(V2DeferredTapeEvent::ContextRecoveryAction {
                event_type: "recovery.action.completed",
                action: action.to_owned(),
                reason_code: outcome.reason_code.clone(),
            });
            let plan = state.context_recovery.plan().clone();
            state
                .deferred_tape_events
                .push(V2DeferredTapeEvent::ContextRecoveryPlan(Box::new(plan)));

            if step.action == ContextRecoveryAction::FailDeterministic {
                return Err(self.kernel_failure("runtime.context_recovery.exhausted"));
            }
            if outcome.terminal {
                return Ok(());
            }
        }
    }

    fn prepare_provider_recovery(
        &self,
        state: &mut CallbackState,
        anomaly: ProviderTurnAnomaly,
        issue_summary: String,
        partial_user_visible_output: bool,
    ) -> Result<V2ProviderRecoveryDisposition, RuntimeErrorEnvelopeV1> {
        let decision =
            state.provider_recovery.decide(anomaly, ProviderTurnRecoveryInput::default());
        let completed_tool_calls = u32::try_from(
            state
                .messages
                .iter()
                .filter(|message| message.role == ProviderMessageRole::Tool)
                .count(),
        )
        .unwrap_or(u32::MAX);
        let prepared = state.provider_recovery.prepare_recovery(
            decision.clone(),
            &state.current_attempt_plan,
            RecoveryExecutorInput {
                issue_summary,
                completed_tool_calls,
                side_effect_state: if completed_tool_calls > 0 {
                    ProviderRecoverySideEffectState::ConfirmedWithReconciliation
                } else {
                    ProviderRecoverySideEffectState::None
                },
                partial_user_visible_output,
                summary_only_closeout: user_requested_summary_only_closeout(
                    state.messages.as_slice(),
                ),
            },
        );
        state
            .deferred_tape_events
            .push(V2DeferredTapeEvent::ProviderRecoveryDecision(Box::new(decision)));
        state
            .deferred_tape_events
            .push(V2DeferredTapeEvent::ProviderRecoveryStarted(prepared.started_payload()));
        if let Some(outcome) = prepared.immediate_outcome.clone() {
            let reason_code = outcome.reason_code.clone();
            state
                .deferred_tape_events
                .push(V2DeferredTapeEvent::ProviderRecoveryOutcome(Box::new(outcome)));
            return Ok(V2ProviderRecoveryDisposition::Stop { reason_code });
        }
        let command = prepared.command.clone().ok_or_else(|| {
            self.kernel_failure("runtime.provider.recovery_executor_missing_outcome")
        })?;
        let (outcome, disposition) = match command {
            ProviderRecoveryCommand::RetryCurrentRequest => {
                let outcome =
                    prepared.completed("provider.recovery.retry_current_request.completed");
                let reason_code = outcome.reason_code.clone();
                (outcome, V2ProviderRecoveryDisposition::Retry { reason_code, delay_ms: None })
            }
            ProviderRecoveryCommand::AppendGuidance { guidance } => {
                state.messages.push(ProviderMessage::user_text(guidance));
                let outcome = prepared.completed("provider.recovery.append_guidance.completed");
                let reason_code = outcome.reason_code.clone();
                (outcome, V2ProviderRecoveryDisposition::Retry { reason_code, delay_ms: None })
            }
            ProviderRecoveryCommand::RecoverContext => {
                let step = state
                    .context_recovery
                    .next_step()
                    .map_err(|_| self.kernel_failure("runtime.context_recovery.controller_failed"))?
                    .ok_or_else(|| {
                        self.kernel_failure("runtime.context_recovery.budget_exhausted")
                    })?;
                if step.action != ContextRecoveryAction::Compact {
                    let outcome = prepared.failed("provider.recovery.context.executor_required");
                    let reason_code = outcome.reason_code.clone();
                    (outcome, V2ProviderRecoveryDisposition::Stop { reason_code })
                } else {
                    let context_plan = state.context_recovery.plan().clone();
                    state.pending_context_recovery_step = Some(step);
                    state.deferred_tape_events.push(V2DeferredTapeEvent::ContextRecoveryAction {
                        event_type: "recovery.action.started",
                        action: "compact".to_owned(),
                        reason_code: "context.recovery.compact_requested".to_owned(),
                    });
                    state
                        .deferred_tape_events
                        .push(V2DeferredTapeEvent::ContextRecoveryPlan(Box::new(context_plan)));
                    state.deferred_tape_events.push(V2DeferredTapeEvent::CompactionRequired);
                    let outcome =
                        prepared.completed("provider.recovery.context.compaction_requested");
                    (outcome, V2ProviderRecoveryDisposition::CompactionRequired)
                }
            }
            ProviderRecoveryCommand::LowerOutputBudget => {
                let current = state.max_output_tokens.unwrap_or(4_096);
                state.max_output_tokens = Some(current.saturating_div(2).max(256));
                let outcome = prepared.completed("provider.recovery.output_budget_lowered");
                let reason_code = outcome.reason_code.clone();
                (outcome, V2ProviderRecoveryDisposition::Retry { reason_code, delay_ms: None })
            }
            ProviderRecoveryCommand::DropVisionInputs
            | ProviderRecoveryCommand::StripUnsupportedContent => {
                state.drop_vision_inputs = true;
                state.messages.push(ProviderMessage::user_text(
                    "Continue without provider-native image payloads. Use only retained textual metadata and explicitly report when visual evidence is unavailable.",
                ));
                let outcome = prepared.completed("provider.recovery.unsupported_content_stripped");
                let reason_code = outcome.reason_code.clone();
                (outcome, V2ProviderRecoveryDisposition::Retry { reason_code, delay_ms: None })
            }
            ProviderRecoveryCommand::RefreshCredential => {
                let outcome =
                    prepared.unsupported("provider.recovery.auth_refresh_port_unavailable");
                let reason_code = outcome.reason_code.clone();
                (outcome, V2ProviderRecoveryDisposition::Stop { reason_code })
            }
            ProviderRecoveryCommand::SelectFallbackRoute => {
                let outcome = prepared.blocked("provider.recovery.route_fallback_exhausted");
                let reason_code = outcome.reason_code.clone();
                (outcome, V2ProviderRecoveryDisposition::Stop { reason_code })
            }
            ProviderRecoveryCommand::Backoff { delay_ms } => {
                let outcome = prepared.completed("provider.recovery.backoff.completed");
                let reason_code = outcome.reason_code.clone();
                (
                    outcome,
                    V2ProviderRecoveryDisposition::Retry { reason_code, delay_ms: Some(delay_ms) },
                )
            }
            ProviderRecoveryCommand::FailDeterministic => {
                let outcome = prepared.completed("provider.recovery.fail_deterministic.completed");
                let reason_code = outcome.reason_code.clone();
                (outcome, V2ProviderRecoveryDisposition::Stop { reason_code })
            }
        };
        state
            .deferred_tape_events
            .push(V2DeferredTapeEvent::ProviderRecoveryOutcome(Box::new(outcome)));
        Ok(disposition)
    }
}

fn v2_provider_failure_anomaly(reason_code: &str) -> ProviderTurnAnomaly {
    if reason_code.contains("context_window") || reason_code.contains("context_overflow") {
        ProviderTurnAnomaly::ContextOverflow
    } else if reason_code.contains("auth_expired") {
        ProviderTurnAnomaly::AuthExpired
    } else if reason_code.contains("auth") {
        ProviderTurnAnomaly::AuthInvalid
    } else if reason_code.contains("rate_limit") || reason_code.contains("resource_exhausted") {
        ProviderTurnAnomaly::RateLimit
    } else if reason_code.contains("multimodal") || reason_code.contains("unsupported_image") {
        ProviderTurnAnomaly::MultimodalUnsupported
    } else if reason_code.contains("empty") {
        ProviderTurnAnomaly::EmptyFinalAnswer
    } else if reason_code.contains("timeout") || reason_code.contains("deadline") {
        ProviderTurnAnomaly::ProviderTimeout
    } else {
        ProviderTurnAnomaly::MalformedStream
    }
}

impl ProductionAttemptCallbacks for RunStreamV2Callbacks {
    fn provider_effect_started<'a>(
        &'a self,
    ) -> HarnessFuture<'a, Result<(), RuntimeErrorEnvelopeV1>> {
        Box::pin(async move {
            self.sender
                .send(Ok(common_v1::RunStreamEvent {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    run_id: Some(common_v1::CanonicalId { ulid: self.run_id.clone() }),
                    body: Some(common_v1::run_stream_event::Body::Status(
                        common_v1::StreamStatus {
                            kind: common_v1::stream_status::StatusKind::InProgress as i32,
                            message: RUNTIME_KERNEL_V2_PROVIDER_EFFECT_STARTED_MESSAGE.to_owned(),
                        },
                    )),
                }))
                .await
                .map_err(|_| {
                    self.kernel_failure("runtime.provider_call.start_observation_unavailable")
                })
        })
    }

    fn prepare_provider_turn<'a>(
        &'a self,
        _request: &'a HarnessAttemptRequest,
        context: &'a crate::application::runtime_kernel_v2::phases::ContextAssemblyResult,
    ) -> HarnessFuture<'a, Result<Option<PreparedProductionProviderTurn>, RuntimeErrorEnvelopeV1>>
    {
        Box::pin(async move {
            let mut state = self
                .state
                .lock()
                .map_err(|_| self.kernel_failure("runtime.provider.turn_state_unavailable"))?;
            if state.first_provider_turn {
                state.first_provider_turn = false;
                return Ok(None);
            }
            let request = self.projected_recovery_request(&state);
            let model_id = request.model_override.as_deref().unwrap_or("default").to_owned();
            let plan = state.provider_recovery.plan_attempt(
                &request,
                self.lease.provider_id.as_str(),
                self.lease.credential_id.as_str(),
                model_id.as_str(),
            );
            state.current_attempt_plan = plan.clone();
            state
                .deferred_tape_events
                .push(V2DeferredTapeEvent::ProviderAttemptPlan(Box::new(plan)));
            Ok(Some(PreparedProductionProviderTurn {
                projection_id: context.projection_id.clone(),
                request,
                lease: self.lease.clone(),
            }))
        })
    }

    fn project_provider_response<'a>(
        &'a self,
        _request: &'a HarnessAttemptRequest,
        response: ProviderResponse,
        terminal: TerminalOutcomeClassification,
    ) -> HarnessFuture<'a, Result<EmbeddedProviderTurn, RuntimeErrorEnvelopeV1>> {
        Box::pin(async move {
            let provider_lane_attestation = response.qa_lane_attestation.clone();
            if let Some(attestation) = provider_lane_attestation.as_ref() {
                attestation
                    .validate_shape()
                    .map_err(|_| self.kernel_failure("runtime.provider.qa_attestation_invalid"))?;
            }
            let terminal_validation =
                normalized_provider_stream_from_output_v2(&response.output).terminal_validation;
            {
                let mut state = self
                    .state
                    .lock()
                    .map_err(|_| self.kernel_failure("runtime.provider.turn_state_unavailable"))?;
                let plan = state.current_attempt_plan.clone();
                let attempt_outcome =
                    state.provider_recovery.record_completed_attempt(&plan, &response);
                state
                    .deferred_tape_events
                    .push(V2DeferredTapeEvent::ProviderAttemptOutcome(Box::new(attempt_outcome)));
                state.deferred_tape_events.push(V2DeferredTapeEvent::ProviderTerminalValidation(
                    Box::new(terminal_validation.clone()),
                ));
            }
            if terminal_validation.disposition != ProviderTerminalDisposition::Complete {
                let anomaly = anomaly_from_terminal_validation(&terminal_validation);
                let recovery = {
                    let mut state = self.state.lock().map_err(|_| {
                        self.kernel_failure("runtime.provider.turn_state_unavailable")
                    })?;
                    self.prepare_provider_recovery(
                        &mut state,
                        anomaly,
                        terminal_validation.reason_code.clone(),
                        terminal_validation.text_delta_count > 0,
                    )?
                };
                match recovery {
                    V2ProviderRecoveryDisposition::Retry { reason_code, delay_ms } => {
                        return Ok(EmbeddedProviderTurn::RetryRequired {
                            reason_code,
                            prompt_tokens: response.prompt_tokens,
                            completion_tokens: response.completion_tokens,
                            delay_ms,
                        });
                    }
                    V2ProviderRecoveryDisposition::CompactionRequired => {
                        return Ok(EmbeddedProviderTurn::CompactionRequired);
                    }
                    V2ProviderRecoveryDisposition::Stop { reason_code } => {
                        tracing::warn!(
                            run_id = self.run_id,
                            reason_code,
                            "authoritative provider recovery stopped"
                        );
                        return Err(self.kernel_failure("runtime.provider.recovery_blocked"));
                    }
                }
            }
            if terminal.finish_reason == Some(ProviderFinishReason::Cancelled) {
                self.state
                    .lock()
                    .map_err(|_| self.kernel_failure("runtime.provider.turn_state_unavailable"))?
                    .after_turn_observations
                    .push(ContextAfterTurnObservation {
                        prompt_tokens: response.prompt_tokens,
                        completion_tokens: response.completion_tokens,
                        tool_exchange_count: 0,
                        finish_reason: terminal.finish_reason,
                    });
                return Ok(EmbeddedProviderTurn::Cancelled {
                    reason_code: "runtime.provider_call.cancelled".to_owned(),
                });
            }
            let output = bounded_provider_turn_output_for_persistence(&response.output);
            let proposal = self.retain_tool_proposal(&output)?;
            let tool_exchange_count = u64::from(proposal.is_some());
            let mut state = self
                .state
                .lock()
                .map_err(|_| self.kernel_failure("runtime.provider.turn_state_unavailable"))?;
            if let Some(attestation) = provider_lane_attestation {
                state
                    .deferred_tape_events
                    .push(V2DeferredTapeEvent::ProviderLaneAttested(Box::new(attestation)));
            }
            state.after_turn_observations.push(ContextAfterTurnObservation {
                prompt_tokens: response.prompt_tokens,
                completion_tokens: response.completion_tokens,
                tool_exchange_count,
                finish_reason: terminal.finish_reason,
            });
            let final_output_unusable = proposal.is_none()
                && (terminal.class != TerminalOutcomeClass::VisibleText
                    || output.full_text.trim().is_empty());
            if final_output_unusable {
                let anomaly = match terminal.class {
                    TerminalOutcomeClass::ReasoningOnly | TerminalOutcomeClass::PlanningOnly => {
                        ProviderTurnAnomaly::ReasoningOnly
                    }
                    TerminalOutcomeClass::ProtocolError => {
                        ProviderTurnAnomaly::MalformedToolSequence
                    }
                    TerminalOutcomeClass::Empty
                    | TerminalOutcomeClass::VisibleText
                    | TerminalOutcomeClass::ToolOnly
                    | TerminalOutcomeClass::IntentionalSilent
                    | TerminalOutcomeClass::ProviderError => ProviderTurnAnomaly::EmptyFinalAnswer,
                };
                let recovery = self.prepare_provider_recovery(
                    &mut state,
                    anomaly,
                    "runtime.provider.final_output_unusable".to_owned(),
                    !output.full_text.trim().is_empty(),
                )?;
                drop(state);
                return match recovery {
                    V2ProviderRecoveryDisposition::Retry { reason_code, delay_ms } => {
                        Ok(EmbeddedProviderTurn::RetryRequired {
                            reason_code,
                            prompt_tokens: response.prompt_tokens,
                            completion_tokens: response.completion_tokens,
                            delay_ms,
                        })
                    }
                    V2ProviderRecoveryDisposition::CompactionRequired => {
                        Ok(EmbeddedProviderTurn::CompactionRequired)
                    }
                    V2ProviderRecoveryDisposition::Stop { reason_code } => {
                        tracing::warn!(
                            run_id = self.run_id,
                            reason_code,
                            "authoritative final-output recovery stopped"
                        );
                        Err(self.kernel_failure("runtime.provider.recovery_blocked"))
                    }
                };
            }
            let recovery_completed = !state.context_recovery.plan().steps.is_empty()
                && state.context_recovery.plan().terminal_reason_code.as_deref()
                    != Some("context.recovery.provider_retry_succeeded");
            if recovery_completed {
                state.context_recovery.record_provider_success();
                let plan = state.context_recovery.plan().clone();
                state
                    .deferred_tape_events
                    .push(V2DeferredTapeEvent::ContextRecoveryPlan(Box::new(plan)));
            }
            state.messages.push(ProviderMessage::assistant_from_output(&output));
            if let Some((proposal, operation_id)) = proposal {
                return Ok(EmbeddedProviderTurn::Tool { proposal, operation_id });
            }
            let text_utf8_bytes = u64::try_from(output.full_text.len()).unwrap_or(u64::MAX);
            state.final_text = Some(output.full_text);
            Ok(EmbeddedProviderTurn::Completed {
                text_utf8_bytes,
                prompt_tokens: response.prompt_tokens,
                completion_tokens: response.completion_tokens,
            })
        })
    }

    fn project_provider_failure<'a>(
        &'a self,
        _request: &'a HarnessAttemptRequest,
        reason_code: String,
        output_emitted: bool,
        qa_lane_attestation: Option<ProviderLaneAttestationEvent>,
    ) -> HarnessFuture<'a, Result<EmbeddedProviderTurn, RuntimeErrorEnvelopeV1>> {
        Box::pin(async move {
            if let Some(attestation) = qa_lane_attestation {
                attestation
                    .validate_shape()
                    .map_err(|_| self.kernel_failure("runtime.provider.qa_attestation_invalid"))?;
                self.state
                    .lock()
                    .map_err(|_| self.kernel_failure("runtime.provider.turn_state_unavailable"))?
                    .deferred_tape_events
                    .push(V2DeferredTapeEvent::ProviderLaneAttested(Box::new(attestation)));
            }
            {
                let mut state = self
                    .state
                    .lock()
                    .map_err(|_| self.kernel_failure("runtime.provider.turn_state_unavailable"))?;
                state.after_turn_observations.push(ContextAfterTurnObservation {
                    prompt_tokens: 0,
                    completion_tokens: 0,
                    tool_exchange_count: 0,
                    finish_reason: None,
                });
                let plan = state.current_attempt_plan.clone();
                let attempt_outcome = state.provider_recovery.record_failed_attempt(
                    &plan,
                    "failed",
                    reason_code.as_str(),
                );
                state
                    .deferred_tape_events
                    .push(V2DeferredTapeEvent::ProviderAttemptOutcome(Box::new(attempt_outcome)));
            }
            if reason_code == "runtime.provider_call.cancelled" {
                return Ok(EmbeddedProviderTurn::Cancelled { reason_code });
            }
            let anomaly = v2_provider_failure_anomaly(reason_code.as_str());
            let recovery = {
                let mut state = self
                    .state
                    .lock()
                    .map_err(|_| self.kernel_failure("runtime.provider.turn_state_unavailable"))?;
                self.prepare_provider_recovery(&mut state, anomaly, reason_code, output_emitted)?
            };
            match recovery {
                V2ProviderRecoveryDisposition::Retry { reason_code, delay_ms } => {
                    Ok(EmbeddedProviderTurn::RetryRequired {
                        reason_code,
                        prompt_tokens: 0,
                        completion_tokens: 0,
                        delay_ms,
                    })
                }
                V2ProviderRecoveryDisposition::CompactionRequired => {
                    Ok(EmbeddedProviderTurn::CompactionRequired)
                }
                V2ProviderRecoveryDisposition::Stop { reason_code } => {
                    tracing::warn!(
                        run_id = self.run_id,
                        reason_code,
                        "authoritative provider failure recovery stopped"
                    );
                    Err(self.kernel_failure("runtime.provider.recovery_blocked"))
                }
            }
        })
    }

    fn accept_compaction<'a>(
        &'a self,
        _request: &'a HarnessAttemptRequest,
        result: &'a CompactionResult,
    ) -> HarnessFuture<'a, Result<(), RuntimeErrorEnvelopeV1>> {
        Box::pin(async move {
            let mut state = self
                .state
                .lock()
                .map_err(|_| self.kernel_failure("runtime.provider.turn_state_unavailable"))?;
            let recovery_step = state.pending_context_recovery_step.take().ok_or_else(|| {
                self.kernel_failure("runtime.context_recovery.compaction_step_missing")
            })?;
            let recovery_outcome = match result {
                CompactionResult::Applied { .. } => {
                    let projection = self.compaction_projections.take().ok_or_else(|| {
                        self.kernel_failure("runtime.compaction.applied_projection_missing")
                    })?;
                    let current_input =
                        self.base_request.user_visible_input_text.as_deref().ok_or_else(|| {
                            self.kernel_failure("runtime.compaction.current_input_missing")
                        })?;
                    let block =
                        crate::application::session_compaction::render_compaction_prompt_block(
                            projection.artifact_id.as_str(),
                            projection.mode.as_str(),
                            projection.trigger_reason.as_str(),
                            projection.summary_text.as_str(),
                        );
                    let compacted_input_text = format!("{block}\n\n{current_input}");
                    let mut messages = self
                        .base_request
                        .effective_messages()
                        .into_iter()
                        .filter(|message| {
                            matches!(
                                message.role,
                                ProviderMessageRole::System | ProviderMessageRole::Developer
                            )
                        })
                        .collect::<Vec<_>>();
                    messages.push(ProviderMessage::user_text(compacted_input_text.clone()));
                    state.messages = messages;
                    state.compacted_input_text = Some(compacted_input_text);
                    let recovered_request = self.projected_recovery_request(&state);
                    let after_tokens =
                        estimated_required_tokens_for_request(&recovered_request, &self.catalog);
                    let outcome = state
                        .context_recovery
                        .record_outcome(
                            &recovery_step,
                            after_tokens,
                            vec![
                                TokenBreakdownCategory::SessionHistory,
                                TokenBreakdownCategory::ToolResults,
                            ],
                            true,
                        )
                        .map_err(|_| {
                            self.kernel_failure(
                                "runtime.context_recovery.compaction_outcome_invalid",
                            )
                        })?;
                    state.deferred_tape_events.push(V2DeferredTapeEvent::CompactionApplied {
                        artifact_id_sha256: crate::sha256_hex(projection.artifact_id.as_bytes()),
                    });
                    outcome
                }
                CompactionResult::Skipped { reason_code, .. } => {
                    let after_tokens = state.context_recovery.current_tokens();
                    let outcome = state
                        .context_recovery
                        .record_outcome_with_host_reason(
                            &recovery_step,
                            after_tokens,
                            Vec::new(),
                            true,
                            Some(reason_code.as_str()),
                        )
                        .map_err(|_| {
                            self.kernel_failure(
                                "runtime.context_recovery.compaction_outcome_invalid",
                            )
                        })?;
                    state.deferred_tape_events.push(V2DeferredTapeEvent::CompactionSkipped {
                        reason_code: reason_code.clone(),
                    });
                    outcome
                }
            };
            let completed_reason = recovery_outcome
                .host_reason_code
                .clone()
                .unwrap_or_else(|| recovery_outcome.reason_code.clone());
            state.deferred_tape_events.push(V2DeferredTapeEvent::ContextRecoveryAction {
                event_type: "recovery.action.completed",
                action: "compact".to_owned(),
                reason_code: completed_reason,
            });
            let plan = state.context_recovery.plan().clone();
            state
                .deferred_tape_events
                .push(V2DeferredTapeEvent::ContextRecoveryPlan(Box::new(plan)));
            if recovery_outcome.terminal {
                return Ok(());
            }
            self.continue_prompt_local_recovery(&mut state)
        })
    }

    fn accept_tool_projection<'a>(
        &'a self,
        _request: &'a HarnessAttemptRequest,
        projection: ToolResultProjection,
    ) -> HarnessFuture<'a, Result<(), RuntimeErrorEnvelopeV1>> {
        Box::pin(async move {
            if projection.evidence.is_empty() {
                return Err(self.kernel_failure("runtime.tool.projection_evidence_missing"));
            }
            let proposal_id = projection.proposal_id.as_str().to_owned();
            let outcome = self
                .proposal_retention
                .take_model_visible_result(
                    &projection.model_visible_result,
                    &projection.proposal_id,
                    &projection.execution_id,
                )
                .ok_or_else(|| self.kernel_failure("runtime.tool.result_projection_missing"))?;
            let message = Self::project_tool_outcome(proposal_id.as_str(), outcome)?;
            self.state
                .lock()
                .map_err(|_| self.kernel_failure("runtime.provider.turn_state_unavailable"))?
                .messages
                .push(message);
            Ok(())
        })
    }

    fn accept_tool_denial<'a>(
        &'a self,
        _request: &'a HarnessAttemptRequest,
        proposal_id: RuntimeToolProposalId,
        reason_code: &'static str,
    ) -> HarnessFuture<'a, Result<(), RuntimeErrorEnvelopeV1>> {
        Box::pin(async move {
            let result = serde_json::json!({
                "success": false,
                "error": reason_code,
            })
            .to_string();
            let proposal_id = proposal_id.as_str().to_owned();
            {
                let mut state = self
                    .state
                    .lock()
                    .map_err(|_| self.kernel_failure("runtime.provider.turn_state_unavailable"))?;
                state.messages.push(ProviderMessage::tool_result(proposal_id.clone(), result));
                state.deferred_tape_events.push(V2DeferredTapeEvent::ToolDenied {
                    proposal_id: proposal_id.clone(),
                    reason_code,
                });
            }
            self.sender
                .send(Ok(tool_result_event(
                    self.run_id.clone(),
                    proposal_id,
                    false,
                    b"{}".to_vec(),
                    reason_code,
                )))
                .await
                .map_err(|_| self.kernel_failure("runtime.tool.denial_projection_closed"))?;
            Ok(())
        })
    }

    fn verify<'a>(
        &'a self,
        _request: &'a HarnessAttemptRequest,
    ) -> HarnessFuture<'a, Result<(), RuntimeErrorEnvelopeV1>> {
        Box::pin(async move {
            let valid = self
                .state
                .lock()
                .ok()
                .and_then(|state| state.final_text.as_ref().map(|text| !text.trim().is_empty()))
                .unwrap_or(false);
            valid
                .then_some(())
                .ok_or_else(|| self.kernel_failure("runtime.verification.final_output_missing"))
        })
    }

    fn finalization_request<'a>(
        &'a self,
        _request: &'a HarnessAttemptRequest,
        outcome: &'a HarnessTerminalOutcome,
    ) -> HarnessFuture<'a, Result<FinalizationRequest, RuntimeErrorEnvelopeV1>> {
        Box::pin(async move {
            let (terminal, content) = match outcome {
                HarnessTerminalOutcome::Completed => {
                    let text = self.final_text().ok_or_else(|| {
                        self.kernel_failure("runtime.finalization.output_missing")
                    })?;
                    (RuntimeTerminalOutcome::Completed, text.into_bytes())
                }
                HarnessTerminalOutcome::Failed { error } => {
                    (RuntimeTerminalOutcome::Failed, error.reason_code().as_bytes().to_vec())
                }
                HarnessTerminalOutcome::Cancelled { reason_code } => {
                    (RuntimeTerminalOutcome::Cancelled, reason_code.as_bytes().to_vec())
                }
            };
            let final_projection = if terminal == RuntimeTerminalOutcome::Completed {
                match self.finalization.delivery.clone() {
                    Some(delivery) => self.finalization.projections.retain_visible(
                        content.as_slice(),
                        delivery,
                        Vec::new(),
                        Vec::new(),
                        Vec::new(),
                    ),
                    None => self.finalization.projections.retain_hidden(
                        content.as_slice(),
                        Vec::new(),
                        Vec::new(),
                        Vec::new(),
                    ),
                }
            } else {
                self.finalization.projections.retain_hidden(
                    content.as_slice(),
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                )
            }
            .map_err(|_| self.kernel_failure("runtime.finalization.projection_failed"))?;
            self.state
                .lock()
                .map_err(|_| self.kernel_failure("runtime.finalization.projection_state_failed"))?
                .final_projection = Some(final_projection.clone());
            Ok(FinalizationRequest { outcome: terminal, final_projection })
        })
    }

    fn accept_finalization<'a>(
        &'a self,
        _request: &'a HarnessAttemptRequest,
        _receipt: FinalizationReceipt,
    ) -> HarnessFuture<'a, Result<(), RuntimeErrorEnvelopeV1>> {
        Box::pin(async { Ok(()) })
    }

    fn delivery_plan<'a>(
        &'a self,
        _request: &'a HarnessAttemptRequest,
        _outcome: &'a HarnessTerminalOutcome,
    ) -> HarnessFuture<'a, Result<EmbeddedDeliveryPlan, RuntimeErrorEnvelopeV1>> {
        Box::pin(async move {
            if self.finalization.delivery.is_some() {
                let final_projection = self
                    .state
                    .lock()
                    .map_err(|_| self.kernel_failure("runtime.delivery.projection_state_failed"))?
                    .final_projection
                    .clone()
                    .ok_or_else(|| {
                        self.kernel_failure("runtime.delivery.final_projection_missing")
                    })?;
                let delivery_intent_id =
                    RuntimeDeliveryIntentId::parse(Ulid::new().to_string().as_str())
                        .map_err(|_| self.kernel_failure("runtime.delivery.intent_id_invalid"))?;
                return Ok(EmbeddedDeliveryPlan::Commit(DeliveryRequest {
                    delivery_intent_id,
                    final_projection,
                }));
            }
            let evidence = self
                .finalization
                .projections
                .retain_delivery_skip_evidence("runtime.delivery.grpc_stream_projection")
                .map_err(|_| self.kernel_failure("runtime.delivery.skip_evidence_failed"))?;
            Ok(EmbeddedDeliveryPlan::Skip { evidence })
        })
    }

    fn accept_delivery<'a>(
        &'a self,
        _request: &'a HarnessAttemptRequest,
        result: DeliveryResult,
    ) -> HarnessFuture<'a, Result<HarnessDeliveryBinding, RuntimeErrorEnvelopeV1>> {
        Box::pin(async move {
            let operation_id = RuntimeOperationId::parse(Ulid::new().to_string().as_str())
                .map_err(|_| self.kernel_failure("runtime.delivery.operation_id_invalid"))?;
            let output_event_id = RuntimeEventId::parse(Ulid::new().to_string().as_str())
                .map_err(|_| self.kernel_failure("runtime.delivery.output_event_id_invalid"))?;
            Ok(HarnessDeliveryBinding {
                delivery_intent_id: result.delivery_intent_id,
                operation_id,
                output_event_id,
            })
        })
    }

    fn kernel_failure(&self, reason_code: &'static str) -> RuntimeErrorEnvelopeV1 {
        kernel_failure(reason_code)
    }
}

pub(super) struct AuthoritativeV2DriverInput<'a> {
    pub(super) sender: &'a tokio::sync::mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    pub(super) stream: &'a mut Streaming<common_v1::RunStreamRequest>,
    pub(super) runtime_state: &'a Arc<GatewayRuntimeState>,
    pub(super) delivery_outbox: Arc<dyn DeliveryOutboxPort>,
    pub(super) request_context: &'a RequestContext,
    pub(super) run_state: &'a mut RunStateMachine,
    pub(super) session_id: &'a str,
    pub(super) run_id: &'a str,
    pub(super) base_provider_request: ProviderRequest,
    pub(super) tool_catalog: ModelVisibleToolCatalogSnapshot,
    pub(super) remaining_tool_budget: &'a mut u32,
    pub(super) allow_sensitive_tools: bool,
    pub(super) approval_cache_generation: Option<u64>,
    pub(super) flow_control: &'a RunStreamFlowControl,
    pub(super) tape_seq: &'a mut i64,
    pub(super) model_token_tape_events: &'a mut usize,
    pub(super) admission: PersistedV2AdmissionToken,
}

pub(super) async fn drive_authoritative_v2(
    input: AuthoritativeV2DriverInput<'_>,
) -> Result<RunStreamMessageProcessingOutcome, Status> {
    let AuthoritativeV2DriverInput {
        sender,
        stream,
        runtime_state,
        delivery_outbox,
        request_context,
        run_state,
        session_id,
        run_id,
        mut base_provider_request,
        tool_catalog,
        remaining_tool_budget,
        allow_sensitive_tools,
        approval_cache_generation,
        flow_control,
        tape_seq,
        model_token_tape_events,
        admission,
    } = input;
    let identities = admission.identities().clone();
    let run_lease = admission.run_lease().clone();
    let initial_attempt_id = RuntimeAttemptId::parse(admission.initial_attempt_id())
        .map_err(|error| Status::failed_precondition(format!("invalid V2 attempt id: {error}")))?;
    let gateway_snapshot = runtime_state
        .provider_selection_snapshot()
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    let configuration_epoch = gateway_snapshot.configuration_epoch;
    let selection = select_production_runtime(
        &runtime_state.journal_store,
        admission,
        &gateway_snapshot,
        &tool_catalog,
    )
    .map_err(|error| {
        Status::failed_precondition(format!("V2 runtime selection failed: {error}"))
    })?;
    let context_binding = selection.context.clone();
    append_agent_loop_tape_event(
        runtime_state,
        run_id,
        tape_seq,
        RUNTIME_SELECTED_METADATA_EVENT,
        runtime_kernel_v2_selection_metadata_payload(),
    )
    .await?;

    base_provider_request.model_override = Some(selection.provider.model_id.clone());
    base_provider_request.tool_catalog_snapshot =
        Some(snapshot_to_provider_request_value(&tool_catalog));
    let provider_snapshot = runtime_state.model_provider_status_snapshot();
    let context_recovery = ContextRecoveryController::new(context_recovery_input_for_request(
        &base_provider_request,
        &provider_snapshot,
        selection.provider.provider_id.as_str(),
        selection.provider.model_id.as_str(),
        &tool_catalog,
        true,
        0,
    ))
    .map_err(|reason| {
        Status::failed_precondition(format!("V2 context recovery input failed: {reason}"))
    })?;
    let lease = ProviderLeaseExecutionContext {
        provider_id: selection.provider.provider_id,
        credential_id: selection.provider.credential_id,
        priority: RoutingTaskClass::PrimaryInteractive.lease_priority(),
        task_label: RoutingTaskClass::PrimaryInteractive.as_str().to_owned(),
        max_wait_ms: RoutingTaskClass::PrimaryInteractive.max_lease_wait_ms(),
        session_id: Some(session_id.to_owned()),
        run_id: Some(run_id.to_owned()),
        runtime_authority: None,
        diagnostic_scope_id: Some(flow_control.root_context().scope_id.as_str().to_owned()),
    };
    let session = runtime_state
        .orchestrator_session_by_id(session_id.to_owned())
        .await?
        .ok_or_else(|| Status::failed_precondition("V2 compaction session is unavailable"))?;
    let context_work = Arc::new(PreassembledContextAssemblyInput::new(
        base_provider_request.clone(),
        lease.clone(),
        context_binding.clone(),
    ));
    let compaction_projections = Arc::new(RunStreamCompactionProjectionStore::default());
    let retained_context_tokens = v2_context_retained_token_estimate(&base_provider_request);
    let compaction_plan = crate::application::context_compaction::ContextCompactionPlanV2::host(
        crate::application::context_compaction::context_compaction_owner_registry()
            .next_generation(),
        context_binding.projection_epoch(),
        tool_catalog.catalog_hash.clone(),
        retained_context_tokens.max(1),
    );
    let compaction_work = Arc::new(RunStreamCompactionInput::new(
        Arc::clone(runtime_state),
        session,
        request_context.principal.clone(),
        request_context.clone(),
        run_id.to_owned(),
        Arc::clone(&compaction_projections),
        compaction_plan,
    ));
    let final_projections = Arc::new(RunFinalProjectionStore::default());

    let provider_authority = runtime_state
        .journal_store
        .acquire_runtime_provider_lane(
            &crate::journal::runtime_kernel::RuntimeKernelProviderLaneAcquireRequest::new(
                identities.clone(),
                run_lease.clone(),
                configuration_epoch,
            ),
        )
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    let flow_authorities = Arc::new(ProductionKernelFlowAuthorities::new(
        Arc::clone(runtime_state),
        identities.clone(),
        run_lease.clone(),
        provider_authority.clone(),
        flow_control.clone(),
    ));
    let tool_lane =
        flow_authorities.authority_for(RuntimeGenerationLane::Tool).map_err(authority_status)?;
    let attempt_factory = ProductionEmbeddedAttemptFactory::new(tool_lane)
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    let callbacks = Arc::new(RunStreamV2Callbacks::new(
        sender.clone(),
        run_id.to_owned(),
        base_provider_request.clone(),
        lease,
        tool_catalog,
        context_recovery,
        V2CallbackResources {
            proposal_retention: attempt_factory.proposal_retention(),
            compaction_projections,
            finalization: V2FinalizationProjection {
                projections: Arc::clone(&final_projections),
                delivery: None,
            },
        },
    ));
    let bundle = ProductionServiceBundle::new(
        Arc::clone(runtime_state),
        provider_authority,
        context_binding.clone(),
        context_work,
        retained_context_tokens.saturating_add(1),
        compaction_work,
        callbacks.clone(),
    )
    .map_err(|error| Status::failed_precondition(format!("V2 context binding failed: {error}")))?;

    let turn = RuntimeKernelTurnServices::new(
        bundle.context_assembly.clone(),
        bundle.provider_call.clone(),
    );
    let finalization = Arc::new(JournalFinalizationService::from_runtime_state(
        Arc::clone(runtime_state),
        final_projections.clone(),
    ));
    let delivery = Arc::new(JournalDeliveryService::from_runtime_state(
        Arc::clone(runtime_state),
        final_projections,
        delivery_outbox,
    ));
    let lifecycle =
        RuntimeKernelLifecycleServices::new(bundle.compaction.clone(), finalization, delivery);
    let services = RuntimeKernelServices::new(turn, lifecycle, attempt_factory.tool_authority());
    let context = Arc::new(
        RuntimeKernelContext::new(
            identities.clone(),
            selection.resolved,
            flow_authorities.clone(),
            flow_authorities.clone(),
            flow_authorities.clone(),
            flow_authorities.clone(),
            services,
        )
        .map_err(|error| Status::failed_precondition(error.to_string()))?,
    );
    let attempt_request = HarnessAttemptRequest::from_context(&context, initial_attempt_id)
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    let kernel_head = runtime_state
        .journal_store
        .load_runtime_kernel_head(run_id)
        .map_err(|error| Status::failed_precondition(error.to_string()))?
        .ok_or_else(|| Status::failed_precondition("V2 kernel head is unavailable"))?;
    let kernel = RuntimeKernelV2::restore_from_journal(kernel_head.snapshot)
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    let lane_authority = KernelLaneAuthoritySet::new(
        &identities,
        vec![
            flow_authorities.lane_lease(RuntimeGenerationLane::Run).map_err(authority_status)?,
            flow_authorities
                .lane_lease(RuntimeGenerationLane::Provider)
                .map_err(authority_status)?,
            flow_authorities
                .lane_lease(RuntimeGenerationLane::Harness)
                .map_err(authority_status)?,
            flow_authorities.lane_lease(RuntimeGenerationLane::Tool).map_err(authority_status)?,
            flow_authorities
                .lane_lease(RuntimeGenerationLane::Delivery)
                .map_err(authority_status)?,
        ],
    )
    .map_err(|error| Status::failed_precondition(error.to_string()))?;
    let mut sink = HostHarnessEventSink::from_runtime_state(
        attempt_request.clone(),
        kernel,
        lane_authority,
        Arc::clone(runtime_state),
    )
    .map_err(|error| Status::failed_precondition(error.to_string()))?;
    let mut attempt = attempt_factory.build(context, bundle.host_state);
    let receipt = {
        let mut live_tool_host = RunStreamLiveToolHost {
            sender,
            stream,
            runtime_state,
            request_context,
            active_session_id: Some(session_id),
            session_id,
            run_id,
            remaining_tool_budget,
            allow_sensitive_tools,
            approval_cache_generation,
            flow_control,
            tape_seq,
        };
        let attempt_future = attempt.drive(&mut live_tool_host, &attempt_request, &mut sink);
        tokio::pin!(attempt_future);
        let cancellation_future =
            bridge_persisted_v2_cancellation(runtime_state, run_id, flow_control);
        tokio::pin!(cancellation_future);
        let attempt_result = tokio::select! {
            result = &mut attempt_future => result,
            cancellation = &mut cancellation_future => {
                cancellation?;
                attempt_future.await
            }
        };
        match attempt_result {
            Ok(receipt) => receipt,
            Err(HarnessContractError::RollbackSuspended) => {
                runtime_state.clear_self_healing_heartbeat(WorkHeartbeatKind::Run, run_id);
                return Err(Status::aborted(
                    "runtime kernel V2 was suspended by configured rollback policy",
                ));
            }
            Err(error) if error.is_rollback_boundary_stale() => {
                runtime_state.clear_self_healing_heartbeat(WorkHeartbeatKind::Run, run_id);
                return Err(Status::aborted(
                    "runtime kernel V2 rollback boundary became stale; reload is required",
                ));
            }
            Err(error) => {
                return Err(Status::internal(format!("V2 embedded attempt failed: {error}")));
            }
        }
    };
    let context_binding_event = context_binding.evidence_event();
    context_binding_event.validate_shape().map_err(|error| {
        Status::internal(format!("invalid V2 context binding evidence: {error}"))
    })?;
    append_agent_loop_tape_event(
        runtime_state,
        run_id,
        tape_seq,
        CONTEXT_ENGINE_BINDING_EVENT,
        serde_json::to_string(&context_binding_event).map_err(|error| {
            Status::internal(format!("failed to serialize V2 context binding evidence: {error}"))
        })?,
    )
    .await?;
    append_v2_deferred_tape_events(
        runtime_state,
        run_id,
        tape_seq,
        callbacks.take_deferred_tape_events()?,
    )
    .await?;
    for observation in callbacks.take_after_turn_observations()? {
        crate::application::context_lifecycle::record_after_turn(
            runtime_state,
            run_id,
            session_id,
            tape_seq,
            observation.prompt_tokens,
            observation.completion_tokens,
            observation.tool_exchange_count,
            observation.finish_reason,
        )
        .await?;
    }
    settle_v2_receipt(V2ReceiptSettlement {
        sender,
        runtime_state,
        request_context,
        run_state,
        session_id,
        run_id,
        flow_control,
        tape_seq,
        model_token_tape_events,
        callbacks: callbacks.as_ref(),
        receipt,
    })
    .await
}

async fn bridge_persisted_v2_cancellation(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    flow_control: &RunStreamFlowControl,
) -> Result<(), Status> {
    let mut poll = tokio::time::interval(Duration::from_millis(25));
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        poll.tick().await;
        if runtime_state.is_orchestrator_cancel_requested(run_id.to_owned()).await? {
            request_persisted_run_interrupt(runtime_state, run_id, flow_control).await?;
            return Ok(());
        }
    }
}

fn runtime_kernel_v2_selection_metadata_payload() -> String {
    serde_json::json!({
        "schema_version": 1,
        "event": RUNTIME_SELECTED_METADATA_EVENT,
        "harness_id": "runtime_kernel_v2.embedded",
        "harness_version": env!("CARGO_PKG_VERSION"),
        "runtime_id": "runtime_kernel_v2",
        "runtime_version": env!("CARGO_PKG_VERSION"),
        "route_class": "primary",
        "schema_hashes": [{
            "schema_id": "metadata_trace.runtime_selected.v1",
            "sha256": crate::sha256_hex(RUNTIME_SELECTED_METADATA_SCHEMA_V1),
        }],
    })
    .to_string()
}

async fn append_v2_deferred_tape_events(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    events: Vec<V2DeferredTapeEvent>,
) -> Result<(), Status> {
    for event in events {
        let (event_type, payload_json) = match event {
            V2DeferredTapeEvent::ProviderAttemptPlan(plan) => {
                (PROVIDER_ATTEMPT_PLAN_EVENT.to_owned(), plan.tape_payload().to_string())
            }
            V2DeferredTapeEvent::ProviderAttemptOutcome(outcome) => {
                (PROVIDER_ATTEMPT_OUTCOME_EVENT.to_owned(), outcome.tape_payload().to_string())
            }
            V2DeferredTapeEvent::ProviderRecoveryDecision(decision) => {
                (PROVIDER_TURN_RECOVERY_EVENT.to_owned(), decision.tape_payload().to_string())
            }
            V2DeferredTapeEvent::ProviderRecoveryStarted(payload) => {
                (RECOVERY_ACTION_STARTED_EVENT.to_owned(), payload.to_string())
            }
            V2DeferredTapeEvent::ProviderRecoveryOutcome(outcome) => {
                (outcome.event_type.clone(), outcome.tape_payload().to_string())
            }
            V2DeferredTapeEvent::ProviderLaneAttested(attestation) => {
                attestation.validate_shape().map_err(|error| {
                    Status::internal(format!("invalid V2 provider lane attestation: {error}"))
                })?;
                let payload_json = serde_json::to_string(&attestation).map_err(|error| {
                    Status::internal(format!(
                        "failed to serialize V2 provider lane attestation: {error}"
                    ))
                })?;
                (PROVIDER_LANE_ATTESTATION_EVENT.to_owned(), payload_json)
            }
            V2DeferredTapeEvent::ProviderTerminalValidation(outcome) => (
                PROVIDER_TERMINAL_VALIDATION_AUDIT_EVENT.to_owned(),
                serde_json::to_string(&outcome).map_err(|error| {
                    Status::internal(format!(
                        "failed to serialize V2 provider terminal validation: {error}"
                    ))
                })?,
            ),
            V2DeferredTapeEvent::CompactionRequired => (
                "runtime.compaction.required".to_owned(),
                serde_json::json!({
                    "schema_version": 1,
                    "reason_code": "runtime.compaction.context_window_exceeded",
                })
                .to_string(),
            ),
            V2DeferredTapeEvent::CompactionApplied { artifact_id_sha256 } => (
                "runtime.compaction.applied".to_owned(),
                serde_json::json!({
                    "schema_version": 1,
                    "reason_code": "runtime.compaction.applied",
                    "artifact_id_sha256": artifact_id_sha256,
                })
                .to_string(),
            ),
            V2DeferredTapeEvent::CompactionSkipped { reason_code } => (
                "runtime.compaction.skipped".to_owned(),
                serde_json::json!({
                    "schema_version": 1,
                    "reason_code": reason_code,
                    "redaction_level": "metadata_only",
                })
                .to_string(),
            ),
            V2DeferredTapeEvent::ContextRecoveryPlan(plan) => {
                (CONTEXT_RECOVERY_EVENT.to_owned(), plan.tape_payload().to_string())
            }
            V2DeferredTapeEvent::ContextRecoveryAction { event_type, action, reason_code } => (
                event_type.to_owned(),
                serde_json::json!({
                    "schema_version": 1,
                    "action": action,
                    "reason_code": reason_code,
                    "redaction_level": "metadata_only",
                })
                .to_string(),
            ),
            V2DeferredTapeEvent::ToolDenied { proposal_id, reason_code } => (
                "tool_result".to_owned(),
                tool_result_tape_payload(
                    proposal_id.as_str(),
                    false,
                    b"{}",
                    redact_run_stream_text(reason_code).as_str(),
                ),
            ),
        };
        runtime_state
            .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
                run_id: run_id.to_owned(),
                seq: *tape_seq,
                event_type,
                payload_json,
            })
            .await?;
        *tape_seq = tape_seq.saturating_add(1);
    }
    Ok(())
}

struct V2ReceiptSettlement<'a> {
    sender: &'a tokio::sync::mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &'a Arc<GatewayRuntimeState>,
    request_context: &'a RequestContext,
    run_state: &'a mut RunStateMachine,
    session_id: &'a str,
    run_id: &'a str,
    flow_control: &'a RunStreamFlowControl,
    tape_seq: &'a mut i64,
    model_token_tape_events: &'a mut usize,
    callbacks: &'a RunStreamV2Callbacks,
    receipt: HarnessTerminalReceipt,
}

#[derive(Debug, Clone, Copy)]
struct V2TerminalProjection {
    lifecycle_state: RunLifecycleState,
    reason_code: &'static str,
    status_message: &'static str,
    status_kind: common_v1::stream_status::StatusKind,
    processing_outcome: RunStreamMessageProcessingOutcome,
}

impl V2TerminalProjection {
    fn from_kernel_outcome(outcome: KernelTerminalOutcome) -> Self {
        match outcome {
            KernelTerminalOutcome::Done => Self {
                lifecycle_state: RunLifecycleState::Done,
                reason_code: RuntimeTerminalOutcome::Completed.reason_code(),
                status_message: "completed",
                status_kind: common_v1::stream_status::StatusKind::Done,
                processing_outcome: RunStreamMessageProcessingOutcome::Terminate,
            },
            KernelTerminalOutcome::Failed => Self {
                lifecycle_state: RunLifecycleState::Failed,
                reason_code: RuntimeTerminalOutcome::Failed.reason_code(),
                status_message: "runtime kernel V2 run failed",
                status_kind: common_v1::stream_status::StatusKind::Failed,
                processing_outcome: RunStreamMessageProcessingOutcome::Terminate,
            },
            KernelTerminalOutcome::Cancelled => Self {
                lifecycle_state: RunLifecycleState::Cancelled,
                reason_code: RuntimeTerminalOutcome::Cancelled.reason_code(),
                status_message: CANCELLED_REASON,
                status_kind: common_v1::stream_status::StatusKind::Failed,
                processing_outcome: RunStreamMessageProcessingOutcome::Terminate,
            },
        }
    }
}

struct V2DeferredReplyProjection {
    wire_events: Vec<common_v1::RunStreamEvent>,
    terminal_tape_events: Vec<OrchestratorTerminalTapeEvent>,
    resulting_token_tape_events: usize,
}

impl V2DeferredReplyProjection {
    fn prepare(run_id: &str, reply_text: &str, token_tape_events: usize) -> Self {
        let output = ProviderTurnOutput::text(
            reply_text.to_owned(),
            ProviderFinishReason::Stop,
            ProviderUsage::new(0, 0, "run_stream_final_projection"),
            ProviderRawProviderRefs::default(),
        );
        let mut wire_events = Vec::new();
        let mut terminal_tape_events = Vec::new();
        let mut resulting_token_tape_events = token_tape_events;
        for event in provider_events_from_output(&output) {
            let ProviderEvent::ModelToken { token, is_final } = event else {
                continue;
            };
            let safe_token = redact_run_stream_text(token.as_str());
            wire_events.push(model_token_wire_event(run_id, safe_token.as_str(), is_final));
            if is_final || resulting_token_tape_events < MAX_MODEL_TOKEN_TAPE_EVENTS_PER_RUN {
                terminal_tape_events.push(OrchestratorTerminalTapeEvent {
                    event_type: "model_token".to_owned(),
                    payload_json: serde_json::json!({
                        "is_final": is_final,
                        "token": safe_token,
                    })
                    .to_string(),
                });
                resulting_token_tape_events = resulting_token_tape_events.saturating_add(1);
            }
        }
        if wire_events.is_empty() {
            wire_events.push(model_token_wire_event(run_id, "", true));
            terminal_tape_events.push(OrchestratorTerminalTapeEvent {
                event_type: "model_token".to_owned(),
                payload_json: serde_json::json!({
                    "is_final": true,
                    "token": "",
                })
                .to_string(),
            });
            resulting_token_tape_events = resulting_token_tape_events.saturating_add(1);
        }
        if !reply_text.trim().is_empty() {
            terminal_tape_events.push(OrchestratorTerminalTapeEvent {
                event_type: "message.replied".to_owned(),
                payload_json: serde_json::json!({
                    "reply_text": palyra_common::redaction::REDACTED,
                })
                .to_string(),
            });
        }
        Self { wire_events, terminal_tape_events, resulting_token_tape_events }
    }

    #[allow(clippy::result_large_err)]
    async fn emit(
        &self,
        sender: &tokio::sync::mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    ) -> Result<(), Status> {
        for event in &self.wire_events {
            sender
                .send(Ok(event.clone()))
                .await
                .map_err(|_| Status::cancelled(RUN_STREAM_RESPONSE_CHANNEL_CLOSED_MESSAGE))?;
        }
        Ok(())
    }
}

fn model_token_wire_event(run_id: &str, token: &str, is_final: bool) -> common_v1::RunStreamEvent {
    common_v1::RunStreamEvent {
        v: CANONICAL_PROTOCOL_MAJOR,
        run_id: Some(common_v1::CanonicalId { ulid: run_id.to_owned() }),
        body: Some(common_v1::run_stream_event::Body::ModelToken(common_v1::ModelToken {
            token: token.to_owned(),
            is_final,
        })),
    }
}

#[allow(clippy::result_large_err)]
async fn converge_authoritative_v2_lifecycle(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_state: &mut RunStateMachine,
    run_id: &str,
    outcome: KernelTerminalOutcome,
    terminal_tape_events: Vec<OrchestratorTerminalTapeEvent>,
) -> Result<(V2TerminalProjection, OrchestratorRunTerminalSettlement), Status> {
    let projection = V2TerminalProjection::from_kernel_outcome(outcome);
    let terminal = Ok(projection.processing_outcome);
    let summary = run_runtime_path_summary_payload(
        runtime_state,
        projection.lifecycle_state,
        &terminal,
        Some("runtime_kernel_v2.embedded"),
    )?;
    let settlement = runtime_state
        .settle_orchestrator_run_terminal_exact(OrchestratorRunTerminalSettlementRequest {
            run_id: run_id.to_owned(),
            requested_state: projection.lifecycle_state,
            reason_code: projection.reason_code.to_owned(),
            status_message: projection.status_message.to_owned(),
            actor: palyra_common::runtime_contracts::RuntimeActorRef {
                kind: palyra_common::runtime_contracts::RuntimeActorKind::System,
                id: "runtime_kernel_v2.embedded".to_owned(),
            },
            terminal_summary_payload_json: Some(summary),
            terminal_tape_events,
            terminal_status_payload_json: status_tape_payload(
                projection.status_kind,
                projection.status_message,
            ),
        })
        .await?;
    if settlement.effective_state != projection.lifecycle_state {
        return Err(Status::internal(
            "V2 outer lifecycle contradicted the authoritative kernel receipt",
        ));
    }
    if run_state.state() != projection.lifecycle_state {
        let transition = projection
            .lifecycle_state
            .terminal_transition()
            .ok_or_else(|| Status::internal("V2 kernel receipt selected a nonterminal state"))?;
        run_state.transition(transition).map_err(|error| Status::internal(error.to_string()))?;
    }
    Ok((projection, settlement))
}

async fn settle_v2_receipt(
    settlement: V2ReceiptSettlement<'_>,
) -> Result<RunStreamMessageProcessingOutcome, Status> {
    let V2ReceiptSettlement {
        sender,
        runtime_state,
        request_context,
        run_state,
        session_id,
        run_id,
        flow_control,
        tape_seq,
        model_token_tape_events,
        callbacks,
        receipt,
    } = settlement;
    let retained_text = (receipt.outcome() == KernelTerminalOutcome::Done)
        .then(|| callbacks.final_text())
        .flatten();
    let deferred_reply = retained_text
        .as_deref()
        .map(|text| V2DeferredReplyProjection::prepare(run_id, text, *model_token_tape_events));
    let terminal_tape_events = deferred_reply
        .as_ref()
        .map_or_else(Vec::new, |projection| projection.terminal_tape_events.clone());
    // The receipt acknowledges an already-durable kernel terminal event. Close
    // the outer lifecycle before host projection can fail; cancel cannot reclassify it.
    let (projection, lifecycle_settlement) = converge_authoritative_v2_lifecycle(
        runtime_state,
        run_state,
        run_id,
        receipt.outcome(),
        terminal_tape_events,
    )
    .await?;
    if lifecycle_settlement.changed {
        if let Some(deferred_reply) = deferred_reply.as_ref() {
            *model_token_tape_events = deferred_reply.resulting_token_tape_events;
        }
    }
    runtime_state.clear_self_healing_heartbeat(WorkHeartbeatKind::Run, run_id);
    cleanup_run_resources(runtime_state, run_id, projection.status_message).await;

    match receipt.outcome() {
        KernelTerminalOutcome::Done => {
            let text = retained_text
                .ok_or_else(|| Status::internal("V2 completion has no retained final text"))?;
            persist_accepted_final_reply_side_effects(
                runtime_state,
                request_context,
                session_id,
                run_id,
                text.as_str(),
            )
            .await;
            runtime_state
                .add_orchestrator_usage(OrchestratorUsageDelta {
                    run_id: run_id.to_owned(),
                    prompt_tokens_delta: receipt.prompt_tokens(),
                    completion_tokens_delta: receipt.completion_tokens(),
                })
                .await?;
            deferred_reply
                .as_ref()
                .ok_or_else(|| Status::internal("V2 completion projection is unavailable"))?
                .emit(sender)
                .await?;
            if let Some(settled_tape_sequence) = lifecycle_settlement.tape_sequence {
                let delivery = flow_control.delivery()?;
                send_settled_final_status(
                    sender,
                    runtime_state,
                    run_id,
                    tape_seq,
                    settled_tape_sequence,
                    projection.status_kind,
                    projection.status_message,
                    &delivery,
                )
                .await?;
            }
            Ok(projection.processing_outcome)
        }
        KernelTerminalOutcome::Failed | KernelTerminalOutcome::Cancelled => {
            if let Some(settled_tape_sequence) = lifecycle_settlement.tape_sequence {
                let delivery = flow_control.delivery()?;
                send_settled_final_status(
                    sender,
                    runtime_state,
                    run_id,
                    tape_seq,
                    settled_tape_sequence,
                    projection.status_kind,
                    projection.status_message,
                    &delivery,
                )
                .await?;
            }
            Ok(projection.processing_outcome)
        }
    }
}

fn authority_status(error: KernelAuthorityError) -> Status {
    Status::failed_precondition(error.to_string())
}

fn kernel_failure(reason_code: &'static str) -> RuntimeErrorEnvelopeV1 {
    RuntimeErrorEnvelopeV1::try_new(RuntimeErrorEnvelopeV1Input {
        class: RuntimeErrorClass::InternalInvariantViolation,
        reason_code: reason_code.to_owned(),
        subsystem: RuntimeSubsystem::RuntimeKernel,
        phase: RuntimeErrorPhase::Internal,
        retryability: RuntimeRetryability::NotRetryable,
        security_class: RuntimeErrorSecurityClass::Internal,
        user_visibility: RuntimeErrorUserVisibility::StatusOnly,
        output_emitted: false,
        side_effect_may_have_occurred: false,
        safe_message: "runtime kernel V2 execution failed".to_owned(),
        recovery_hint: "inspect retained runtime evidence before retrying".to_owned(),
    })
    .expect("static RuntimeKernelV2 failure envelope must remain valid")
}

#[cfg(test)]
mod production_wiring_tests {
    use std::time::Duration;

    use ulid::Ulid;

    use super::*;
    use crate::{
        application::run_admission::admit_test_v2_run, gateway::tests::build_test_runtime_state,
        journal::OrchestratorCancelRequest, orchestrator::RunTransition,
    };

    #[test]
    fn every_authoritative_terminal_receipt_stops_the_one_generation_host_stream() {
        for outcome in [
            KernelTerminalOutcome::Done,
            KernelTerminalOutcome::Failed,
            KernelTerminalOutcome::Cancelled,
        ] {
            assert_eq!(
                V2TerminalProjection::from_kernel_outcome(outcome).processing_outcome,
                RunStreamMessageProcessingOutcome::Terminate
            );
        }
    }

    #[test]
    fn authoritative_driver_uses_canonical_delivery_service_and_explicit_stream_skip() {
        let source = include_str!("v2_driver.rs");
        let embedded_source = include_str!("../embedded_attempt.rs");

        assert!(source.contains("JournalDeliveryService::from_runtime_state("));
        assert!(source.contains(
            "RuntimeKernelLifecycleServices::new(bundle.compaction.clone(), finalization, delivery)"
        ));
        let closed_delivery = [
            "RuntimeKernelLifecycleServices::new(",
            "bundle.compaction.clone(), finalization, closed)",
        ]
        .concat();
        assert!(!source.contains(closed_delivery.as_str()));
        assert!(source.contains("EmbeddedDeliveryPlan::Commit(DeliveryRequest {"));
        assert!(source.contains("runtime.delivery.grpc_stream_projection"));
        for variant in ["Commit(DeliveryRequest)", "Committed(HarnessDeliveryBinding)"] {
            let gated = ["#[cfg(test)]\n    ", variant].concat();
            assert!(
                !embedded_source.contains(gated.as_str()),
                "{variant} must remain reachable in production"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn exact_receipt_converges_before_closed_transport_despite_late_cancel() {
        let state = build_test_runtime_state(false);
        let session_id = Ulid::new().to_string();
        let run_id = Ulid::new().to_string();
        admit_test_v2_run(
            &state.journal_store,
            session_id.as_str(),
            run_id.as_str(),
            "user:ops",
            Ulid::new().to_string().as_str(),
        )
        .expect("test run should admit through the canonical V2 controller");
        let (_, generation) = state
            .runtime_generation_for_run(run_id.clone())
            .await
            .expect("run generation should load")
            .expect("run should retain active generation");
        let flow_control = RunStreamFlowControl::new(generation, Duration::from_secs(60))
            .expect("flow control should initialize");
        state
            .request_orchestrator_cancel(OrchestratorCancelRequest {
                run_id: run_id.clone(),
                reason: "cancel_after_kernel_terminal_commit".to_owned(),
            })
            .await
            .expect("late cancellation intent should persist");
        let mut run_state = RunStateMachine::default();
        run_state.transition(RunTransition::Accept).expect("run should accept");
        run_state.transition(RunTransition::StartStreaming).expect("run should enter progress");

        let (projection, first) = converge_authoritative_v2_lifecycle(
            &state,
            &mut run_state,
            run_id.as_str(),
            KernelTerminalOutcome::Done,
            Vec::new(),
        )
        .await
        .expect("exact kernel receipt should converge the outer lifecycle");
        assert!(first.changed);
        assert!(!first.cancellation_won);
        assert_eq!(projection.lifecycle_state, RunLifecycleState::Done);
        assert_eq!(run_state.state(), RunLifecycleState::Done);

        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        drop(receiver);
        let delivery =
            flow_control.delivery().expect("terminal delivery scope should remain valid");
        let mut tape_seq = 0;
        let transport_error = send_settled_final_status(
            &sender,
            &state,
            run_id.as_str(),
            &mut tape_seq,
            first.tape_sequence.expect("first convergence should commit terminal tape"),
            projection.status_kind,
            projection.status_message,
            &delivery,
        )
        .await
        .expect_err("closed transport should reject terminal projection");
        assert_eq!(transport_error.code(), tonic::Code::Cancelled);

        let snapshot = state
            .orchestrator_run_status_snapshot(run_id.clone())
            .await
            .expect("run snapshot should load")
            .expect("run should exist");
        assert_eq!(snapshot.state, RunLifecycleState::Done.as_str());
        assert!(
            snapshot.cancel_requested,
            "late cancellation remains evidence without replacing the kernel outcome"
        );
        let (_, replay) = converge_authoritative_v2_lifecycle(
            &state,
            &mut run_state,
            run_id.as_str(),
            KernelTerminalOutcome::Done,
            Vec::new(),
        )
        .await
        .expect("same exact receipt should converge idempotently");
        assert!(!replay.changed);
        assert_eq!(
            state
                .journal_store
                .orchestrator_tape(run_id.as_str())
                .expect("terminal tape should load")
                .len(),
            2,
            "idempotent convergence must not append duplicate summary or status rows"
        );
    }
}
