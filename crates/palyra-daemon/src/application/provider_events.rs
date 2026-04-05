use std::sync::Arc;

use tokio::sync::mpsc;
use tonic::{Status, Streaming};

use crate::{
    application::{
        route_message::tool_flow::process_route_tool_proposal_event,
        run_stream::{
            cancellation::transition_run_stream_to_cancelled, tape::send_model_token_with_tape,
            tool_flow::process_run_stream_tool_proposal_event,
        },
    },
    gateway::{GatewayRuntimeState, RunStreamToolExecutionOutcome},
    model_provider::ProviderEvent,
    orchestrator::RunStateMachine,
    transport::grpc::{auth::RequestContext, proto::palyra::common::v1 as common_v1},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RunStreamProviderEventGateOutcome {
    Continue,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RunStreamProviderEventOutcome {
    Continue,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RunStreamProviderEventsOutcome {
    Completed { summary_tokens: Vec<String> },
    Cancelled,
}

pub(crate) struct RunStreamProviderEventSurface<'a> {
    pub(crate) sender: &'a mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    pub(crate) stream: &'a mut Streaming<common_v1::RunStreamRequest>,
    pub(crate) request_context: &'a RequestContext,
    pub(crate) active_session_id: Option<&'a str>,
    pub(crate) run_state: &'a mut RunStateMachine,
    pub(crate) model_token_tape_events: &'a mut usize,
    pub(crate) model_token_compaction_emitted: &'a mut bool,
}

pub(crate) struct RouteMessageProviderEventSurface<'a> {
    pub(crate) request_context: &'a RequestContext,
    pub(crate) reply_text: &'a mut String,
}

pub(crate) enum ProviderEventSurface<'a> {
    RunStream(RunStreamProviderEventSurface<'a>),
    RouteMessage(RouteMessageProviderEventSurface<'a>),
}

#[allow(clippy::result_large_err)]
async fn gate_run_stream_provider_event_on_cancellation(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &Arc<GatewayRuntimeState>,
    run_state: &mut RunStateMachine,
    run_id: &str,
    tape_seq: &mut i64,
) -> Result<RunStreamProviderEventGateOutcome, Status> {
    match runtime_state.is_orchestrator_cancel_requested(run_id.to_owned()).await {
        Ok(true) => {
            transition_run_stream_to_cancelled(sender, runtime_state, run_state, run_id, tape_seq)
                .await?;
            Ok(RunStreamProviderEventGateOutcome::Cancelled)
        }
        Ok(false) => Ok(RunStreamProviderEventGateOutcome::Continue),
        Err(error) => Err(error),
    }
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn process_provider_event_for_surface(
    runtime_state: &Arc<GatewayRuntimeState>,
    session_id: &str,
    run_id: &str,
    provider_event: ProviderEvent,
    summary_tokens: &mut Vec<String>,
    remaining_tool_budget: &mut u32,
    tape_seq: &mut i64,
    surface: ProviderEventSurface<'_>,
) -> Result<RunStreamProviderEventOutcome, Status> {
    match provider_event {
        ProviderEvent::ModelToken { token, is_final } => {
            if !token.trim().is_empty() {
                summary_tokens.push(token.clone());
            }
            match surface {
                ProviderEventSurface::RunStream(context) => {
                    send_model_token_with_tape(
                        context.sender,
                        runtime_state,
                        run_id,
                        tape_seq,
                        context.model_token_tape_events,
                        context.model_token_compaction_emitted,
                        token.as_str(),
                        is_final,
                    )
                    .await?;
                }
                ProviderEventSurface::RouteMessage(context) => {
                    if !context.reply_text.is_empty() {
                        context.reply_text.push(' ');
                    }
                    context.reply_text.push_str(token.as_str());
                }
            }
            Ok(RunStreamProviderEventOutcome::Continue)
        }
        ProviderEvent::ToolProposal { proposal_id, tool_name, input_json } => match surface {
            ProviderEventSurface::RunStream(context) => {
                match process_run_stream_tool_proposal_event(
                    context.sender,
                    context.stream,
                    runtime_state,
                    context.request_context,
                    context.active_session_id,
                    context.run_state,
                    session_id,
                    run_id,
                    proposal_id.as_str(),
                    tool_name.as_str(),
                    input_json.as_slice(),
                    remaining_tool_budget,
                    tape_seq,
                )
                .await?
                {
                    RunStreamToolExecutionOutcome::Completed => {
                        Ok(RunStreamProviderEventOutcome::Continue)
                    }
                    RunStreamToolExecutionOutcome::Cancelled => {
                        Ok(RunStreamProviderEventOutcome::Cancelled)
                    }
                }
            }
            ProviderEventSurface::RouteMessage(context) => {
                let tool_summary = process_route_tool_proposal_event(
                    runtime_state,
                    context.request_context,
                    session_id,
                    run_id,
                    proposal_id.as_str(),
                    tool_name.as_str(),
                    input_json.as_slice(),
                    remaining_tool_budget,
                    tape_seq,
                )
                .await?;
                if !context.reply_text.is_empty() {
                    context.reply_text.push('\n');
                }
                context.reply_text.push_str(tool_summary.as_str());
                Ok(RunStreamProviderEventOutcome::Continue)
            }
        },
    }
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn process_run_stream_provider_events(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    stream: &mut Streaming<common_v1::RunStreamRequest>,
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    active_session_id: Option<&str>,
    run_state: &mut RunStateMachine,
    session_id: &str,
    run_id: &str,
    provider_events: Vec<ProviderEvent>,
    remaining_tool_budget: &mut u32,
    tape_seq: &mut i64,
    model_token_tape_events: &mut usize,
    model_token_compaction_emitted: &mut bool,
) -> Result<RunStreamProviderEventsOutcome, Status> {
    let mut summary_tokens = Vec::new();
    for provider_event in provider_events {
        match gate_run_stream_provider_event_on_cancellation(
            sender,
            runtime_state,
            run_state,
            run_id,
            tape_seq,
        )
        .await?
        {
            RunStreamProviderEventGateOutcome::Continue => {}
            RunStreamProviderEventGateOutcome::Cancelled => {
                return Ok(RunStreamProviderEventsOutcome::Cancelled);
            }
        }

        match process_run_stream_provider_event(
            sender,
            stream,
            runtime_state,
            request_context,
            active_session_id,
            run_state,
            session_id,
            run_id,
            provider_event,
            &mut summary_tokens,
            remaining_tool_budget,
            tape_seq,
            model_token_tape_events,
            model_token_compaction_emitted,
        )
        .await?
        {
            RunStreamProviderEventOutcome::Continue => {}
            RunStreamProviderEventOutcome::Cancelled => {
                return Ok(RunStreamProviderEventsOutcome::Cancelled);
            }
        }
    }

    Ok(RunStreamProviderEventsOutcome::Completed { summary_tokens })
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn process_run_stream_provider_event(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    stream: &mut Streaming<common_v1::RunStreamRequest>,
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    active_session_id: Option<&str>,
    run_state: &mut RunStateMachine,
    session_id: &str,
    run_id: &str,
    provider_event: ProviderEvent,
    summary_tokens: &mut Vec<String>,
    remaining_tool_budget: &mut u32,
    tape_seq: &mut i64,
    model_token_tape_events: &mut usize,
    model_token_compaction_emitted: &mut bool,
) -> Result<RunStreamProviderEventOutcome, Status> {
    process_provider_event_for_surface(
        runtime_state,
        session_id,
        run_id,
        provider_event,
        summary_tokens,
        remaining_tool_budget,
        tape_seq,
        ProviderEventSurface::RunStream(RunStreamProviderEventSurface {
            sender,
            stream,
            request_context,
            active_session_id,
            run_state,
            model_token_tape_events,
            model_token_compaction_emitted,
        }),
    )
    .await
}
