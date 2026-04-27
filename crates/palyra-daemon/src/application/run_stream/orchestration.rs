use std::{sync::Arc, time::Duration};

use serde_json::{json, Value};
use tokio::{
    sync::mpsc,
    time::{interval, MissedTickBehavior},
};
use tonic::{Status, Streaming};
use tracing::warn;

use crate::{
    application::learning::schedule_post_run_reflection,
    application::provider_events::{
        process_run_stream_provider_events, RunStreamProviderEventsOutcome,
        RunStreamToolResultForModel,
    },
    application::provider_input::{
        prepare_model_provider_input, MemoryPromptFailureMode, PrepareModelProviderInputRequest,
    },
    application::tool_registry::{
        build_model_visible_tool_catalog_snapshot, snapshot_to_provider_request_value,
        tool_catalog_tape_payload, ModelVisibleToolCatalogSnapshot, ToolCatalogBuildRequest,
        ToolExposureSurface,
    },
    delegation::DelegationSnapshot,
    gateway::{
        canonical_id, current_unix_ms, ingest_memory_best_effort, non_empty,
        record_message_router_journal_event, security_requests_json_mode, truncate_with_ellipsis,
        GatewayRuntimeState,
    },
    journal::{
        MemorySource, OrchestratorCancelRequest, OrchestratorRunMetadataUpdateRequest,
        OrchestratorRunStartRequest, OrchestratorSessionResolveRequest,
        OrchestratorTapeAppendRequest, OrchestratorUsageDelta,
    },
    model_provider::{
        ProviderFinishReason, ProviderMessage, ProviderRequest, ProviderResponse,
        ProviderTurnOutput,
    },
    orchestrator::{is_cancel_command, RunLifecycleState, RunStateMachine, RunTransition},
    provider_leases::ProviderLeaseExecutionContext,
    self_healing::{WorkHeartbeatKind, WorkHeartbeatUpdate},
    tool_protocol::ToolRequestContext,
    transport::grpc::{auth::RequestContext, proto::palyra::common::v1 as common_v1},
    usage_governance::{
        plan_usage_routing, resolve_provider_binding_for_model, RoutingTaskClass,
        UsageRoutingPlanRequest,
    },
};

use super::{
    agent_loop::{
        AgentLoopTerminationReason, AgentRunLoopState, DEFAULT_AGENT_LOOP_WALL_CLOCK_BUDGET_MS,
    },
    cancellation::transition_run_stream_to_cancelled,
    tape::{maybe_compact_context_after_tool_results, send_status_with_tape},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RunStreamPostProviderOutcome {
    Completed,
    Cancelled,
}

#[derive(Debug, Clone)]
pub(crate) enum RunStreamProviderRequestOutcome {
    Completed(Box<ProviderResponse>),
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RunStreamProviderResponseOutcome {
    Completed {
        tool_result_messages: Vec<ProviderMessage>,
        provider_trace_ref: Option<String>,
        final_reply_text: Option<String>,
    },
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RunStreamMessageProcessingOutcome {
    Continue,
    Terminate,
}

async fn persist_run_stream_delegation_metadata(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    origin_run_id: Option<&common_v1::CanonicalId>,
    parameter_delta_json: Option<&str>,
) -> Result<(), Status> {
    let Some(parameter_delta_json) = parameter_delta_json else {
        return Ok(());
    };
    let parsed = match serde_json::from_str::<Value>(parameter_delta_json) {
        Ok(value) => value,
        Err(error) => {
            warn!(
                run_id = %run_id,
                error = %error,
                "ignoring non-JSON parameter_delta while inspecting delegation metadata"
            );
            return Ok(());
        }
    };
    let Some(delegation_json) = parsed.get("delegation") else {
        return Ok(());
    };
    let delegation = match serde_json::from_value::<DelegationSnapshot>(delegation_json.clone()) {
        Ok(value) => value,
        Err(error) => {
            warn!(
                run_id = %run_id,
                error = %error,
                "ignoring invalid delegation snapshot inside parameter_delta"
            );
            return Ok(());
        }
    };
    runtime_state
        .update_orchestrator_run_metadata(OrchestratorRunMetadataUpdateRequest {
            run_id: run_id.to_owned(),
            parent_run_id: Some(origin_run_id.map(|value| value.ulid.clone())),
            delegation: Some(Some(delegation)),
            merge_result: None,
        })
        .await
}

#[allow(clippy::result_large_err)]
pub(crate) async fn finalize_run_stream_after_provider_response(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &Arc<GatewayRuntimeState>,
    run_state: &mut RunStateMachine,
    run_id: &str,
    tape_seq: &mut i64,
) -> Result<RunStreamPostProviderOutcome, Status> {
    match runtime_state.is_orchestrator_cancel_requested(run_id.to_owned()).await {
        Ok(true) => {
            transition_run_stream_to_cancelled(sender, runtime_state, run_state, run_id, tape_seq)
                .await?;
            return Ok(RunStreamPostProviderOutcome::Cancelled);
        }
        Ok(false) => {}
        Err(error) => return Err(error),
    }

    if run_state.state() == RunLifecycleState::InProgress {
        run_state
            .transition(RunTransition::Complete)
            .map_err(|error| Status::internal(error.to_string()))?;
        runtime_state
            .update_orchestrator_run_state(run_id.to_owned(), RunLifecycleState::Done, None)
            .await?;
        runtime_state.clear_self_healing_heartbeat(WorkHeartbeatKind::Run, run_id);
        send_status_with_tape(
            sender,
            runtime_state,
            run_id,
            tape_seq,
            common_v1::stream_status::StatusKind::Done,
            "completed",
        )
        .await?;
    }

    Ok(RunStreamPostProviderOutcome::Completed)
}

#[allow(clippy::result_large_err)]
async fn execute_run_stream_provider_request(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &Arc<GatewayRuntimeState>,
    run_state: &mut RunStateMachine,
    run_id: &str,
    provider_request: ProviderRequest,
    lease_context: ProviderLeaseExecutionContext,
    tape_seq: &mut i64,
) -> Result<RunStreamProviderRequestOutcome, Status> {
    let mut provider_future =
        Box::pin(runtime_state.execute_model_provider_with_lease(provider_request, lease_context));
    let mut cancel_poll = interval(Duration::from_millis(100));
    cancel_poll.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            provider_result = &mut provider_future => {
                return provider_result
                    .map(Box::new)
                    .map(RunStreamProviderRequestOutcome::Completed);
            }
            _ = cancel_poll.tick() => {
                match runtime_state.is_orchestrator_cancel_requested(run_id.to_owned()).await {
                    Ok(true) => {
                        transition_run_stream_to_cancelled(
                            sender,
                            runtime_state,
                            run_state,
                            run_id,
                            tape_seq,
                        )
                        .await?;
                        return Ok(RunStreamProviderRequestOutcome::Cancelled);
                    }
                    Ok(false) => {}
                    Err(error) => return Err(error),
                }
            }
        }
    }
}

#[allow(clippy::result_large_err)]
async fn ensure_run_stream_in_progress(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &Arc<GatewayRuntimeState>,
    run_state: &mut RunStateMachine,
    run_id: &str,
    in_progress_emitted: &mut bool,
    tape_seq: &mut i64,
) -> Result<(), Status> {
    if *in_progress_emitted {
        return Ok(());
    }

    run_state
        .transition(RunTransition::StartStreaming)
        .map_err(|error| Status::internal(error.to_string()))?;
    runtime_state
        .update_orchestrator_run_state(run_id.to_owned(), RunLifecycleState::InProgress, None)
        .await?;
    send_status_with_tape(
        sender,
        runtime_state,
        run_id,
        tape_seq,
        common_v1::stream_status::StatusKind::InProgress,
        "streaming",
    )
    .await?;
    *in_progress_emitted = true;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn build_and_record_run_stream_tool_catalog_snapshot(
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    session_id: &str,
    run_id: &str,
    provider_kind: &str,
    provider_model_id: Option<&str>,
    remaining_tool_budget: u32,
    tape_seq: &mut i64,
) -> Result<ModelVisibleToolCatalogSnapshot, Status> {
    let snapshot = build_model_visible_tool_catalog_snapshot(ToolCatalogBuildRequest {
        config: &runtime_state.config.tool_call,
        browser_service_enabled: runtime_state.config.browser_service.enabled,
        request_context: &ToolRequestContext {
            principal: request_context.principal.clone(),
            device_id: Some(request_context.device_id.clone()),
            channel: request_context.channel.clone(),
            session_id: Some(session_id.to_owned()),
            run_id: Some(run_id.to_owned()),
            skill_id: None,
        },
        provider_kind,
        provider_model_id,
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget,
        created_at_unix_ms: current_unix_ms(),
    });
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: "tool_catalog_snapshot".to_owned(),
            payload_json: tool_catalog_tape_payload(&snapshot),
        })
        .await?;
    *tape_seq = (*tape_seq).saturating_add(1);
    Ok(snapshot)
}

#[allow(clippy::result_large_err)]
async fn append_agent_loop_tape_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    event_type: &str,
    payload_json: String,
) -> Result<(), Status> {
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: event_type.to_owned(),
            payload_json,
        })
        .await?;
    *tape_seq = (*tape_seq).saturating_add(1);
    Ok(())
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn terminate_run_stream_with_agent_loop_reason(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &Arc<GatewayRuntimeState>,
    run_state: &mut RunStateMachine,
    run_id: &str,
    tape_seq: &mut i64,
    loop_state: &AgentRunLoopState,
    reason: AgentLoopTerminationReason,
    message: &str,
    provider_trace_ref: Option<String>,
) -> Result<(), Status> {
    append_agent_loop_tape_event(
        runtime_state,
        run_id,
        tape_seq,
        "agent_loop.terminated",
        loop_state.termination_payload(run_id, reason, message, provider_trace_ref),
    )
    .await?;
    run_state
        .transition(RunTransition::Fail)
        .map_err(|error| Status::internal(error.to_string()))?;
    runtime_state
        .update_orchestrator_run_state(
            run_id.to_owned(),
            RunLifecycleState::Failed,
            Some(message.to_owned()),
        )
        .await?;
    runtime_state.clear_self_healing_heartbeat(WorkHeartbeatKind::Run, run_id);
    send_status_with_tape(
        sender,
        runtime_state,
        run_id,
        tape_seq,
        common_v1::stream_status::StatusKind::Failed,
        message,
    )
    .await
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn process_run_stream_message(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    stream: &mut Streaming<common_v1::RunStreamRequest>,
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    active_session_id: &mut Option<String>,
    active_run_id: &mut Option<String>,
    run_state: &mut RunStateMachine,
    tape_seq: &mut i64,
    model_token_tape_events: &mut usize,
    model_token_compaction_emitted: &mut bool,
    in_progress_emitted: &mut bool,
    remaining_tool_budget: &mut u32,
    previous_session_run_id: &mut Option<String>,
    message: common_v1::RunStreamRequest,
) -> Result<RunStreamMessageProcessingOutcome, Status> {
    let session_id = canonical_id(message.session_id, "session_id")?;
    let run_id = canonical_id(message.run_id, "run_id")?;

    if let Some(expected_session) = active_session_id.as_ref() {
        if expected_session != &session_id {
            return Err(Status::invalid_argument("run stream cannot switch session_id mid-stream"));
        }
    }
    if let Some(expected_run) = active_run_id.as_ref() {
        if expected_run != &run_id {
            return Err(Status::invalid_argument("run stream cannot switch run_id mid-stream"));
        }
    }

    let parameter_delta_json = (!message.parameter_delta_json.is_empty())
        .then(|| String::from_utf8_lossy(message.parameter_delta_json.as_slice()).into_owned());
    if active_run_id.is_none() {
        run_state
            .transition(RunTransition::Accept)
            .map_err(|error| Status::internal(error.to_string()))?;
        let resolved_session = runtime_state
            .resolve_orchestrator_session(OrchestratorSessionResolveRequest {
                session_id: Some(session_id.clone()),
                session_key: non_empty(message.session_key.clone()),
                session_label: non_empty(message.session_label.clone()),
                principal: request_context.principal.clone(),
                device_id: request_context.device_id.clone(),
                channel: request_context.channel.clone(),
                require_existing: message.require_existing,
                reset_session: message.reset_session,
            })
            .await?;
        if message.reset_session {
            runtime_state
                .clear_tool_approval_cache_for_session(request_context, session_id.as_str());
        }
        *previous_session_run_id = resolved_session
            .session
            .last_run_id
            .clone()
            .or(resolved_session.session.branch_origin_run_id.clone());
        if resolved_session.session.session_id != session_id {
            return Err(Status::failed_precondition(
                "resolved session_id does not match RunStream session_id",
            ));
        }
        runtime_state
            .start_orchestrator_run(OrchestratorRunStartRequest {
                run_id: run_id.clone(),
                session_id: session_id.clone(),
                origin_kind: non_empty(message.origin_kind.clone())
                    .unwrap_or_else(|| "manual".to_owned()),
                origin_run_id: message.origin_run_id.as_ref().map(|value| value.ulid.clone()),
                triggered_by_principal: Some(request_context.principal.clone()),
                parameter_delta_json: parameter_delta_json.clone(),
            })
            .await?;
        persist_run_stream_delegation_metadata(
            runtime_state,
            run_id.as_str(),
            message.origin_run_id.as_ref(),
            parameter_delta_json.as_deref(),
        )
        .await?;

        *active_session_id = Some(session_id.clone());
        *active_run_id = Some(run_id.clone());
        runtime_state.record_self_healing_heartbeat(WorkHeartbeatUpdate {
            kind: WorkHeartbeatKind::Run,
            object_id: run_id.clone(),
            summary: format!("run {run_id} for session {session_id}"),
        });

        let accepted_message =
            format!("accepted session={session_id} principal={}", request_context.principal);
        send_status_with_tape(
            sender,
            runtime_state,
            run_id.as_str(),
            tape_seq,
            common_v1::stream_status::StatusKind::Accepted,
            accepted_message.as_str(),
        )
        .await?;
    }

    let input_envelope = message.input.unwrap_or_default();
    let input_content = input_envelope.content.unwrap_or_default();
    let input_text = input_content.text;
    let json_mode_requested = security_requests_json_mode(input_envelope.security.as_ref());
    let session_id_for_message = active_session_id
        .as_deref()
        .ok_or_else(|| {
            Status::internal(
                "run stream internal invariant violated: missing session_id for message",
            )
        })?
        .to_owned();
    runtime_state.record_self_healing_heartbeat(WorkHeartbeatUpdate {
        kind: WorkHeartbeatKind::Run,
        object_id: run_id.clone(),
        summary: format!("run {run_id} for session {session_id_for_message}"),
    });

    let previous_run_id_for_context = previous_session_run_id.take();
    let prepared_provider_input = prepare_model_provider_input(
        runtime_state,
        request_context,
        PrepareModelProviderInputRequest {
            run_id: run_id.as_str(),
            tape_seq,
            session_id: session_id_for_message.as_str(),
            previous_run_id: previous_run_id_for_context.as_deref(),
            parameter_delta_json: parameter_delta_json.as_deref(),
            input_text: input_text.as_str(),
            attachments: input_content.attachments.as_slice(),
            memory_ingest_reason: "run_stream_user_input",
            memory_prompt_failure_mode: MemoryPromptFailureMode::Fail,
            channel_for_log: request_context.channel.as_deref().unwrap_or("n/a"),
        },
    )
    .await?;

    if is_cancel_command(input_text.as_str()) {
        runtime_state
            .request_orchestrator_cancel(OrchestratorCancelRequest {
                run_id: run_id.clone(),
                reason: "stream_cancel_command".to_owned(),
            })
            .await?;
    }

    match runtime_state.is_orchestrator_cancel_requested(run_id.clone()).await {
        Ok(true) => {
            transition_run_stream_to_cancelled(
                sender,
                runtime_state,
                run_state,
                run_id.as_str(),
                tape_seq,
            )
            .await?;
            return Ok(RunStreamMessageProcessingOutcome::Terminate);
        }
        Ok(false) => {}
        Err(error) => return Err(error),
    }

    ensure_run_stream_in_progress(
        sender,
        runtime_state,
        run_state,
        run_id.as_str(),
        in_progress_emitted,
        tape_seq,
    )
    .await?;

    let routing_decision = plan_usage_routing(UsageRoutingPlanRequest {
        runtime_state,
        request_context,
        run_id: run_id.as_str(),
        session_id: session_id.as_str(),
        parameter_delta_json: parameter_delta_json.as_deref(),
        prompt_text: input_text.as_str(),
        json_mode: json_mode_requested,
        vision_inputs: prepared_provider_input.vision_inputs.len(),
        scope_kind: "session",
        scope_id: session_id_for_message.as_str(),
        task_class: RoutingTaskClass::PrimaryInteractive,
        provider_snapshot: &runtime_state.model_provider_status_snapshot(),
    })
    .await?;

    let session_model_override = if routing_decision.mode == "enforced" {
        None
    } else {
        runtime_state
            .orchestrator_session_by_id(session_id.clone())
            .await?
            .and_then(|session| session.model_profile_override)
    };

    let provider_model_override = if routing_decision.mode == "enforced" {
        Some(routing_decision.actual_model_id.clone())
    } else {
        session_model_override
    };
    let (lease_provider_id, lease_provider_kind, lease_credential_id) =
        provider_model_override.as_deref().map_or_else(
            || {
                (
                    routing_decision.provider_id.clone(),
                    routing_decision.provider_kind.clone(),
                    routing_decision.credential_id.clone(),
                )
            },
            |model_id| {
                resolve_provider_binding_for_model(
                    &runtime_state.model_provider_status_snapshot(),
                    model_id,
                )
            },
        );
    let base_provider_request = ProviderRequest::from_input_text(
        prepared_provider_input.provider_input_text,
        json_mode_requested,
        prepared_provider_input.vision_inputs,
        provider_model_override.clone(),
    );
    let mut loop_state = AgentRunLoopState::new(
        base_provider_request.effective_messages(),
        AgentRunLoopState::default_model_turn_budget(
            runtime_state.config.tool_call.max_calls_per_run,
        ),
        *remaining_tool_budget,
        DEFAULT_AGENT_LOOP_WALL_CLOCK_BUDGET_MS,
    );
    append_agent_loop_tape_event(
        runtime_state,
        run_id.as_str(),
        tape_seq,
        "agent_loop.started",
        loop_state.start_payload(run_id.as_str()),
    )
    .await?;

    loop {
        let _turn_id = match loop_state.start_model_turn() {
            Ok(turn_id) => turn_id,
            Err(reason) => {
                let message = match reason {
                    AgentLoopTerminationReason::MaxTurns => "agent loop model turn limit reached",
                    AgentLoopTerminationReason::WallClock => {
                        "agent loop wall-clock budget exhausted"
                    }
                    _ => "agent loop budget exhausted",
                };
                terminate_run_stream_with_agent_loop_reason(
                    sender,
                    runtime_state,
                    run_state,
                    run_id.as_str(),
                    tape_seq,
                    &loop_state,
                    reason,
                    message,
                    None,
                )
                .await?;
                return Ok(RunStreamMessageProcessingOutcome::Terminate);
            }
        };
        append_agent_loop_tape_event(
            runtime_state,
            run_id.as_str(),
            tape_seq,
            "agent_loop.turn_started",
            loop_state.turn_payload(run_id.as_str(), "agent_loop.turn_started"),
        )
        .await?;

        let tool_catalog_snapshot = build_and_record_run_stream_tool_catalog_snapshot(
            runtime_state,
            request_context,
            session_id_for_message.as_str(),
            run_id.as_str(),
            lease_provider_kind.as_str(),
            provider_model_override.as_deref().or(Some(routing_decision.actual_model_id.as_str())),
            loop_state.remaining_tool_calls(),
            tape_seq,
        )
        .await?;
        let mut provider_request = ProviderRequest::from_input_text(
            base_provider_request.input_text.clone(),
            base_provider_request.json_mode,
            base_provider_request.vision_inputs.clone(),
            base_provider_request.model_override.clone(),
        );
        provider_request.messages = loop_state.messages();
        provider_request.tool_catalog_snapshot =
            Some(snapshot_to_provider_request_value(&tool_catalog_snapshot));
        let provider_response = match execute_run_stream_provider_request(
            sender,
            runtime_state,
            run_state,
            run_id.as_str(),
            provider_request,
            ProviderLeaseExecutionContext {
                provider_id: lease_provider_id.clone(),
                credential_id: lease_credential_id.clone(),
                priority: RoutingTaskClass::PrimaryInteractive.lease_priority(),
                task_label: RoutingTaskClass::PrimaryInteractive.as_str().to_owned(),
                max_wait_ms: RoutingTaskClass::PrimaryInteractive.max_lease_wait_ms(),
                session_id: Some(session_id_for_message.clone()),
                run_id: Some(run_id.clone()),
            },
            tape_seq,
        )
        .await
        {
            Ok(RunStreamProviderRequestOutcome::Completed(response)) => *response,
            Ok(RunStreamProviderRequestOutcome::Cancelled) => {
                return Ok(RunStreamMessageProcessingOutcome::Terminate);
            }
            Err(error) => {
                terminate_run_stream_with_agent_loop_reason(
                    sender,
                    runtime_state,
                    run_state,
                    run_id.as_str(),
                    tape_seq,
                    &loop_state,
                    AgentLoopTerminationReason::ProviderError,
                    error.message(),
                    None,
                )
                .await?;
                return Err(error);
            }
        };
        loop_state.record_provider_response(&provider_response);
        let provider_output = provider_response.output.clone();

        let response_outcome = process_run_stream_provider_response(
            sender,
            stream,
            runtime_state,
            request_context,
            active_session_id.as_deref(),
            run_state,
            session_id.as_str(),
            run_id.as_str(),
            session_id_for_message.as_str(),
            provider_response,
            &tool_catalog_snapshot,
            remaining_tool_budget,
            tape_seq,
            model_token_tape_events,
            model_token_compaction_emitted,
        )
        .await?;
        loop_state.sync_remaining_tool_calls(*remaining_tool_budget);
        loop_state.append_assistant_turn(&provider_output);

        match response_outcome {
            RunStreamProviderResponseOutcome::Completed {
                tool_result_messages,
                provider_trace_ref,
                final_reply_text,
            } => {
                let should_refeed_tool_results = !tool_result_messages.is_empty()
                    && provider_output.finish_reason == ProviderFinishReason::ToolCalls;
                if !should_refeed_tool_results {
                    append_agent_loop_tape_event(
                        runtime_state,
                        run_id.as_str(),
                        tape_seq,
                        "agent_loop.terminated",
                        loop_state.termination_payload(
                            run_id.as_str(),
                            AgentLoopTerminationReason::FinalAnswer,
                            final_reply_text.as_deref().unwrap_or("completed"),
                            provider_trace_ref,
                        ),
                    )
                    .await?;
                    return Ok(RunStreamMessageProcessingOutcome::Continue);
                }

                let tool_result_count = tool_result_messages.len();
                loop_state.append_tool_result_messages(tool_result_messages);
                maybe_compact_context_after_tool_results(
                    runtime_state,
                    request_context,
                    session_id.as_str(),
                    run_id.as_str(),
                    tape_seq,
                    tool_result_count,
                )
                .await?;
                append_agent_loop_tape_event(
                    runtime_state,
                    run_id.as_str(),
                    tape_seq,
                    "agent_loop.turn_completed",
                    loop_state.turn_payload(run_id.as_str(), "agent_loop.turn_completed"),
                )
                .await?;
            }
            RunStreamProviderResponseOutcome::Cancelled => {
                return Ok(RunStreamMessageProcessingOutcome::Terminate);
            }
        }
    }
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn process_run_stream_provider_response(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    stream: &mut Streaming<common_v1::RunStreamRequest>,
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    active_session_id: Option<&str>,
    run_state: &mut RunStateMachine,
    session_id: &str,
    run_id: &str,
    session_id_for_message: &str,
    provider_response: ProviderResponse,
    tool_catalog_snapshot: &ModelVisibleToolCatalogSnapshot,
    remaining_tool_budget: &mut u32,
    tape_seq: &mut i64,
    model_token_tape_events: &mut usize,
    model_token_compaction_emitted: &mut bool,
) -> Result<RunStreamProviderResponseOutcome, Status> {
    let provider_output = provider_response.output.clone();
    runtime_state
        .add_orchestrator_usage(OrchestratorUsageDelta {
            run_id: run_id.to_owned(),
            prompt_tokens_delta: provider_response.prompt_tokens,
            completion_tokens_delta: 0,
        })
        .await?;

    let (summary_tokens, tool_results) = match process_run_stream_provider_events(
        sender,
        stream,
        runtime_state,
        request_context,
        active_session_id,
        run_state,
        session_id,
        run_id,
        provider_response.events,
        tool_catalog_snapshot,
        remaining_tool_budget,
        tape_seq,
        model_token_tape_events,
        model_token_compaction_emitted,
    )
    .await?
    {
        RunStreamProviderEventsOutcome::Completed { summary_tokens, tool_results } => {
            (summary_tokens, tool_results)
        }
        RunStreamProviderEventsOutcome::Cancelled => {
            return Ok(RunStreamProviderResponseOutcome::Cancelled);
        }
    };
    persist_run_stream_provider_turn_output(runtime_state, run_id, tape_seq, &provider_output)
        .await?;
    let tool_result_messages =
        tool_results.iter().map(tool_result_to_provider_message).collect::<Result<Vec<_>, _>>()?;
    let has_pending_tool_results = !tool_result_messages.is_empty();
    let reply_text = if provider_output.full_text.trim().is_empty() {
        summary_tokens.concat()
    } else {
        provider_output.full_text.clone()
    };

    if provider_response.completion_tokens > 0 {
        runtime_state
            .add_orchestrator_usage(OrchestratorUsageDelta {
                run_id: run_id.to_owned(),
                prompt_tokens_delta: 0,
                completion_tokens_delta: provider_response.completion_tokens,
            })
            .await?;
    }

    if !has_pending_tool_results {
        persist_run_stream_reply_text(
            runtime_state,
            request_context,
            session_id_for_message,
            run_id,
            tape_seq,
            reply_text.as_str(),
        )
        .await?;
    }

    if !has_pending_tool_results && !summary_tokens.is_empty() {
        ingest_memory_best_effort(
            runtime_state,
            request_context.principal.as_str(),
            request_context.channel.as_deref(),
            Some(session_id_for_message),
            MemorySource::Summary,
            reply_text.as_str(),
            vec!["summary:model_output".to_owned()],
            Some(0.75),
            "run_stream_model_summary",
        )
        .await;
    }

    if let Ok(Some(run_snapshot)) =
        runtime_state.orchestrator_run_status_snapshot(run_id.to_owned()).await
    {
        if run_snapshot.state == RunLifecycleState::Done.as_str() {
            if let Err(error) =
                schedule_post_run_reflection(runtime_state, request_context, session_id, run_id)
                    .await
            {
                warn!(
                    run_id,
                    session_id,
                    status_code = ?error.code(),
                    status_message = %error.message(),
                    "failed to schedule post-run reflection"
                );
            }
        }
    }

    Ok(RunStreamProviderResponseOutcome::Completed {
        tool_result_messages,
        provider_trace_ref: provider_output.raw_provider_refs.provider_trace_ref.clone(),
        final_reply_text: (!has_pending_tool_results).then_some(reply_text),
    })
}

fn tool_result_to_provider_message(
    result: &RunStreamToolResultForModel,
) -> Result<ProviderMessage, Status> {
    let output = serde_json::from_slice::<Value>(result.outcome.output_json.as_slice())
        .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(&result.outcome.output_json) }));
    let content = if result.outcome.error.trim().is_empty() {
        output
    } else {
        json!({
            "success": result.outcome.success,
            "tool_name": result.tool_name.as_str(),
            "error": result.outcome.error.as_str(),
            "output": output,
        })
    };
    let serialized = serde_json::to_string(&content).map_err(|error| {
        Status::internal(format!("failed to serialize model-visible tool result: {error}"))
    })?;
    let redacted = crate::journal::redact_payload_json(serialized.as_bytes()).unwrap_or(serialized);
    Ok(ProviderMessage::tool_result(result.proposal_id.clone(), redacted))
}

#[allow(clippy::result_large_err)]
async fn persist_run_stream_reply_text(
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    session_id: &str,
    run_id: &str,
    tape_seq: &mut i64,
    reply_text: &str,
) -> Result<(), Status> {
    if reply_text.trim().is_empty() {
        return Ok(());
    }

    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: "message.replied".to_owned(),
            payload_json: json!({
                "reply_text": reply_text,
            })
            .to_string(),
        })
        .await?;
    *tape_seq += 1;

    let _ = record_message_router_journal_event(
        runtime_state,
        request_context,
        session_id,
        run_id,
        "message.replied",
        common_v1::journal_event::EventActor::System as i32,
        json!({
            "reply_preview": truncate_with_ellipsis(reply_text.to_owned(), 256),
        }),
    )
    .await;

    Ok(())
}

#[allow(clippy::result_large_err)]
async fn persist_run_stream_provider_turn_output(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    output: &ProviderTurnOutput,
) -> Result<(), Status> {
    let payload_json = serde_json::to_string(output).map_err(|error| {
        Status::internal(format!("failed to serialize provider turn output: {error}"))
    })?;
    let payload_json =
        crate::journal::redact_payload_json(payload_json.as_bytes()).unwrap_or(payload_json);
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: "provider_turn_output".to_owned(),
            payload_json,
        })
        .await?;
    *tape_seq += 1;
    Ok(())
}
