use std::{sync::Arc, time::Duration};

use serde_json::Value;
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
    },
    application::provider_input::{
        prepare_model_provider_input, MemoryPromptFailureMode, PrepareModelProviderInputRequest,
    },
    delegation::DelegationSnapshot,
    gateway::{
        canonical_id, ingest_memory_best_effort, non_empty, security_requests_json_mode,
        GatewayRuntimeState,
    },
    journal::{
        MemorySource, OrchestratorCancelRequest, OrchestratorRunMetadataUpdateRequest,
        OrchestratorRunStartRequest, OrchestratorSessionResolveRequest, OrchestratorUsageDelta,
    },
    model_provider::{ProviderRequest, ProviderResponse},
    orchestrator::{is_cancel_command, RunLifecycleState, RunStateMachine, RunTransition},
    self_healing::{WorkHeartbeatKind, WorkHeartbeatUpdate},
    transport::grpc::{auth::RequestContext, proto::palyra::common::v1 as common_v1},
    usage_governance::{plan_usage_routing, UsageRoutingPlanRequest},
};

use super::cancellation::transition_run_stream_to_cancelled;
use super::tape::send_status_with_tape;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RunStreamPostProviderOutcome {
    Completed,
    Cancelled,
}

#[derive(Debug, Clone)]
pub(crate) enum RunStreamProviderRequestOutcome {
    Completed(ProviderResponse),
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RunStreamProviderResponseOutcome {
    Completed,
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
    tape_seq: &mut i64,
) -> Result<RunStreamProviderRequestOutcome, Status> {
    let mut provider_future = Box::pin(runtime_state.execute_model_provider(provider_request));
    let mut cancel_poll = interval(Duration::from_millis(100));
    cancel_poll.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            provider_result = &mut provider_future => {
                return provider_result.map(RunStreamProviderRequestOutcome::Completed);
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
        provider_snapshot: &runtime_state.model_provider_status_snapshot(),
    })
    .await?;

    let provider_response = match execute_run_stream_provider_request(
        sender,
        runtime_state,
        run_state,
        run_id.as_str(),
        ProviderRequest {
            input_text: prepared_provider_input.provider_input_text,
            json_mode: json_mode_requested,
            vision_inputs: prepared_provider_input.vision_inputs,
            model_override: (routing_decision.mode == "enforced")
                .then(|| routing_decision.actual_model_id.clone()),
        },
        tape_seq,
    )
    .await?
    {
        RunStreamProviderRequestOutcome::Completed(response) => response,
        RunStreamProviderRequestOutcome::Cancelled => {
            return Ok(RunStreamMessageProcessingOutcome::Terminate);
        }
    };

    match process_run_stream_provider_response(
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
        remaining_tool_budget,
        tape_seq,
        model_token_tape_events,
        model_token_compaction_emitted,
    )
    .await?
    {
        RunStreamProviderResponseOutcome::Completed => {
            Ok(RunStreamMessageProcessingOutcome::Continue)
        }
        RunStreamProviderResponseOutcome::Cancelled => {
            Ok(RunStreamMessageProcessingOutcome::Terminate)
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
    remaining_tool_budget: &mut u32,
    tape_seq: &mut i64,
    model_token_tape_events: &mut usize,
    model_token_compaction_emitted: &mut bool,
) -> Result<RunStreamProviderResponseOutcome, Status> {
    runtime_state
        .add_orchestrator_usage(OrchestratorUsageDelta {
            run_id: run_id.to_owned(),
            prompt_tokens_delta: provider_response.prompt_tokens,
            completion_tokens_delta: 0,
        })
        .await?;

    let summary_tokens = match process_run_stream_provider_events(
        sender,
        stream,
        runtime_state,
        request_context,
        active_session_id,
        run_state,
        session_id,
        run_id,
        provider_response.events,
        remaining_tool_budget,
        tape_seq,
        model_token_tape_events,
        model_token_compaction_emitted,
    )
    .await?
    {
        RunStreamProviderEventsOutcome::Completed { summary_tokens } => summary_tokens,
        RunStreamProviderEventsOutcome::Cancelled => {
            return Ok(RunStreamProviderResponseOutcome::Cancelled);
        }
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

    if !summary_tokens.is_empty() {
        let summary_text = summary_tokens.join(" ");
        ingest_memory_best_effort(
            runtime_state,
            request_context.principal.as_str(),
            request_context.channel.as_deref(),
            Some(session_id_for_message),
            MemorySource::Summary,
            summary_text.as_str(),
            vec!["summary:model_output".to_owned()],
            Some(0.75),
            "run_stream_model_summary",
        )
        .await;
    }

    if let Ok(Some(run_snapshot)) = runtime_state
        .orchestrator_run_status_snapshot(run_id.to_owned())
        .await
    {
        if run_snapshot.state == RunLifecycleState::Done.as_str() {
            if let Err(error) = schedule_post_run_reflection(
                runtime_state,
                request_context,
                session_id,
                run_id,
            )
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

    Ok(RunStreamProviderResponseOutcome::Completed)
}
