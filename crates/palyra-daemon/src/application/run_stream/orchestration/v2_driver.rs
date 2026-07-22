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
        bounded_provider_turn_output_for_persistence, provider_events_from_output, ProviderEvent,
        ProviderFinishReason, ProviderMessage, ProviderMessageRole, ProviderOutputContentPart,
        ProviderRawProviderRefs, ProviderRequest, ProviderResponse, ProviderTurnOutput,
        ProviderUsage, TerminalOutcomeClass, TerminalOutcomeClassification,
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
    RunStreamMessageProcessingOutcome, RUNTIME_SELECTED_METADATA_EVENT,
    RUNTIME_SELECTED_METADATA_SCHEMA_V1,
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
    final_text: Option<String>,
    final_projection: Option<crate::application::runtime_kernel_v2::phases::FinalProjectionRef>,
    deferred_tape_events: Vec<V2DeferredTapeEvent>,
}

enum V2DeferredTapeEvent {
    ProviderLaneAttested(Box<ProviderLaneAttestationEvent>),
    CompactionRequired,
    CompactionApplied { artifact_id_sha256: String },
    ToolDenied { proposal_id: String, reason_code: &'static str },
}

impl RunStreamV2Callbacks {
    fn new(
        sender: tokio::sync::mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
        run_id: String,
        base_request: ProviderRequest,
        lease: ProviderLeaseExecutionContext,
        catalog: ModelVisibleToolCatalogSnapshot,
        resources: V2CallbackResources,
    ) -> Self {
        Self {
            state: Mutex::new(CallbackState {
                first_provider_turn: true,
                messages: base_request.effective_messages(),
                compacted_input_text: None,
                final_text: None,
                final_projection: None,
                deferred_tape_events: Vec::new(),
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
            let mut request = self.base_request.clone();
            request.messages = state.messages.clone();
            if let Some(compacted_input_text) = state.compacted_input_text.as_ref() {
                request.input_text = compacted_input_text.clone();
            }
            request.tool_catalog_snapshot = Some(snapshot_to_provider_request_value(&self.catalog));
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
            if terminal.finish_reason == Some(ProviderFinishReason::Cancelled) {
                return Ok(EmbeddedProviderTurn::Cancelled {
                    reason_code: "runtime.provider_call.cancelled".to_owned(),
                });
            }
            let output = bounded_provider_turn_output_for_persistence(&response.output);
            let proposal = self.retain_tool_proposal(&output)?;
            let mut state = self
                .state
                .lock()
                .map_err(|_| self.kernel_failure("runtime.provider.turn_state_unavailable"))?;
            if let Some(attestation) = provider_lane_attestation {
                state
                    .deferred_tape_events
                    .push(V2DeferredTapeEvent::ProviderLaneAttested(Box::new(attestation)));
            }
            state.messages.push(ProviderMessage::assistant_from_output(&output));
            if let Some((proposal, operation_id)) = proposal {
                return Ok(EmbeddedProviderTurn::Tool { proposal, operation_id });
            }
            if terminal.class != TerminalOutcomeClass::VisibleText
                || output.full_text.trim().is_empty()
            {
                return Err(self.kernel_failure("runtime.provider.final_output_unusable"));
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
        _output_emitted: bool,
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
            match reason_code.as_str() {
                "runtime.provider_call.cancelled" => {
                    Ok(EmbeddedProviderTurn::Cancelled { reason_code })
                }
                "runtime.provider_call.context_window_exceeded" => {
                    self.state
                        .lock()
                        .map_err(|_| {
                            self.kernel_failure("runtime.provider.turn_state_unavailable")
                        })?
                        .deferred_tape_events
                        .push(V2DeferredTapeEvent::CompactionRequired);
                    Ok(EmbeddedProviderTurn::CompactionRequired)
                }
                _ => Err(self.kernel_failure("runtime.provider.call_failed")),
            }
        })
    }

    fn accept_compaction<'a>(
        &'a self,
        _request: &'a HarnessAttemptRequest,
        result: &'a CompactionResult,
    ) -> HarnessFuture<'a, Result<(), RuntimeErrorEnvelopeV1>> {
        Box::pin(async move {
            let CompactionResult::Applied { .. } = result;
            let projection = self.compaction_projections.take().ok_or_else(|| {
                self.kernel_failure("runtime.compaction.applied_projection_missing")
            })?;
            let current_input =
                self.base_request.user_visible_input_text.as_deref().ok_or_else(|| {
                    self.kernel_failure("runtime.compaction.current_input_missing")
                })?;
            let block = crate::application::session_compaction::render_compaction_prompt_block(
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
            let mut state = self
                .state
                .lock()
                .map_err(|_| self.kernel_failure("runtime.provider.turn_state_unavailable"))?;
            state.messages = messages;
            state.compacted_input_text = Some(compacted_input_text);
            state.deferred_tape_events.push(V2DeferredTapeEvent::CompactionApplied {
                artifact_id_sha256: crate::sha256_hex(projection.artifact_id.as_bytes()),
            });
            Ok(())
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
    let compaction_work = Arc::new(RunStreamCompactionInput::new(
        Arc::clone(runtime_state),
        session,
        request_context.principal.clone(),
        run_id.to_owned(),
        Arc::clone(&compaction_projections),
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
        v2_context_retained_token_estimate(&base_provider_request).saturating_add(1),
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
