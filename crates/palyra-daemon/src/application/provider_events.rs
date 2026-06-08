use std::sync::Arc;

use palyra_common::runtime_preview::{
    RuntimeDecisionActorKind, RuntimeDecisionEventType, RuntimeDecisionPayload,
    RuntimeDecisionTiming, RuntimeEntityRef, RuntimeResourceBudget,
};
use tokio::sync::mpsc;
use tonic::{Status, Streaming};

use crate::{
    application::{
        route_message::tool_flow::process_route_tool_proposal_event,
        run_stream::{
            cancellation::transition_run_stream_to_cancelled,
            tape::{append_runtime_decision_tape_event, send_model_token_with_tape},
            tool_flow::{
                execute_prepared_run_stream_tool_proposals_ordered,
                prepare_run_stream_tool_proposal_event, process_run_stream_tool_proposal_event,
                RunStreamPreparedToolExecution, RunStreamPreparedToolExecutionBatchOutcome,
                RunStreamToolProposalPreparationOutcome,
            },
        },
        tool_registry::ModelVisibleToolCatalogSnapshot,
    },
    gateway::{GatewayRuntimeState, RunStreamToolExecutionOutcome},
    model_provider::ProviderEvent,
    orchestrator::RunStateMachine,
    tool_protocol::ToolExecutionOutcome,
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
    Completed { summary_tokens: Vec<String>, tool_results: Vec<RunStreamToolResultForModel> },
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RunStreamToolResultForModel {
    pub(crate) proposal_id: String,
    pub(crate) tool_name: String,
    pub(crate) outcome: ToolExecutionOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingRunStreamToolProposal {
    proposal_id: String,
    tool_name: String,
    input_json: Vec<u8>,
}

pub(crate) struct RunStreamProviderEventSurface<'a> {
    pub(crate) sender: &'a mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    pub(crate) stream: &'a mut Streaming<common_v1::RunStreamRequest>,
    pub(crate) request_context: &'a RequestContext,
    pub(crate) active_session_id: Option<&'a str>,
    pub(crate) run_state: &'a mut RunStateMachine,
    pub(crate) model_token_tape_events: &'a mut usize,
    pub(crate) model_token_compaction_emitted: &'a mut bool,
    pub(crate) allow_sensitive_tools: bool,
    pub(crate) approval_cache_generation: Option<u64>,
    pub(crate) stream_model_tokens_immediately: bool,
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
    request_context: &RequestContext,
    run_state: &mut RunStateMachine,
    session_id: &str,
    run_id: &str,
    tape_seq: &mut i64,
) -> Result<RunStreamProviderEventGateOutcome, Status> {
    match runtime_state.is_orchestrator_cancel_requested(run_id.to_owned()).await {
        Ok(true) => {
            let event_payload = RuntimeDecisionPayload::new(
                RuntimeDecisionEventType::FlowLifecycle,
                runtime_state.runtime_decision_actor_from_context(
                    request_context,
                    RuntimeDecisionActorKind::RunStream,
                ),
                "run_cancel_requested",
                "flow_orchestration.preview.cancellation_gate",
                RuntimeDecisionTiming::observed(crate::gateway::current_unix_ms()),
            )
            .with_input(RuntimeEntityRef::new("run", "run", run_id).with_state("running"))
            .with_output(RuntimeEntityRef::new("run", "run", run_id).with_state("cancelled"))
            .with_resource_budget(RuntimeResourceBudget {
                queue_depth: None,
                token_budget: None,
                pruning_token_delta: None,
                retrieval_branch_latency_ms: None,
                retry_count: Some(0),
                suppression_count: None,
            });
            runtime_state
                .record_runtime_decision_event(
                    request_context,
                    Some(session_id),
                    Some(run_id),
                    event_payload.clone(),
                )
                .await?;
            append_runtime_decision_tape_event(runtime_state, run_id, tape_seq, &event_payload)
                .await?;
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
    tool_catalog_snapshot: &ModelVisibleToolCatalogSnapshot,
    summary_tokens: &mut Vec<String>,
    tool_results: &mut Vec<RunStreamToolResultForModel>,
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
                ProviderEventSurface::RunStream(context)
                    if context.stream_model_tokens_immediately =>
                {
                    send_model_token_with_tape(
                        context.sender,
                        runtime_state,
                        context.request_context,
                        session_id,
                        run_id,
                        tape_seq,
                        context.model_token_tape_events,
                        context.model_token_compaction_emitted,
                        token.as_str(),
                        is_final,
                    )
                    .await?;
                }
                ProviderEventSurface::RunStream(_) => {}
                ProviderEventSurface::RouteMessage(_) => {}
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
                    tool_catalog_snapshot,
                    remaining_tool_budget,
                    context.allow_sensitive_tools,
                    context.approval_cache_generation,
                    tape_seq,
                )
                .await?
                {
                    RunStreamToolExecutionOutcome::Completed {
                        proposal_id,
                        tool_name,
                        outcome,
                    } => {
                        tool_results.push(RunStreamToolResultForModel {
                            proposal_id,
                            tool_name,
                            outcome,
                        });
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
                    tool_catalog_snapshot,
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
    tool_catalog_snapshot: &ModelVisibleToolCatalogSnapshot,
    remaining_tool_budget: &mut u32,
    allow_sensitive_tools: bool,
    approval_cache_generation: Option<u64>,
    tape_seq: &mut i64,
    model_token_tape_events: &mut usize,
    model_token_compaction_emitted: &mut bool,
    stream_model_tokens_immediately: bool,
) -> Result<RunStreamProviderEventsOutcome, Status> {
    let mut summary_tokens = Vec::new();
    let mut tool_results = Vec::new();
    let mut pending_tool_proposals = Vec::new();
    for provider_event in provider_events {
        match gate_run_stream_provider_event_on_cancellation(
            sender,
            runtime_state,
            request_context,
            run_state,
            session_id,
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

        match provider_event {
            ProviderEvent::ToolProposal { proposal_id, tool_name, input_json } => {
                pending_tool_proposals.push(PendingRunStreamToolProposal {
                    proposal_id,
                    tool_name,
                    input_json,
                });
            }
            provider_event @ ProviderEvent::ModelToken { .. } => {
                match flush_pending_run_stream_tool_proposals(
                    sender,
                    stream,
                    runtime_state,
                    request_context,
                    active_session_id,
                    run_state,
                    session_id,
                    run_id,
                    &mut pending_tool_proposals,
                    tool_catalog_snapshot,
                    &mut tool_results,
                    remaining_tool_budget,
                    allow_sensitive_tools,
                    approval_cache_generation,
                    tape_seq,
                )
                .await?
                {
                    RunStreamProviderEventOutcome::Continue => {}
                    RunStreamProviderEventOutcome::Cancelled => {
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
                    tool_catalog_snapshot,
                    &mut summary_tokens,
                    &mut tool_results,
                    remaining_tool_budget,
                    allow_sensitive_tools,
                    approval_cache_generation,
                    tape_seq,
                    model_token_tape_events,
                    model_token_compaction_emitted,
                    stream_model_tokens_immediately,
                )
                .await?
                {
                    RunStreamProviderEventOutcome::Continue => {}
                    RunStreamProviderEventOutcome::Cancelled => {
                        return Ok(RunStreamProviderEventsOutcome::Cancelled);
                    }
                }
            }
        }
    }

    match flush_pending_run_stream_tool_proposals(
        sender,
        stream,
        runtime_state,
        request_context,
        active_session_id,
        run_state,
        session_id,
        run_id,
        &mut pending_tool_proposals,
        tool_catalog_snapshot,
        &mut tool_results,
        remaining_tool_budget,
        allow_sensitive_tools,
        approval_cache_generation,
        tape_seq,
    )
    .await?
    {
        RunStreamProviderEventOutcome::Continue => {}
        RunStreamProviderEventOutcome::Cancelled => {
            return Ok(RunStreamProviderEventsOutcome::Cancelled);
        }
    }

    Ok(RunStreamProviderEventsOutcome::Completed { summary_tokens, tool_results })
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn flush_pending_run_stream_tool_proposals(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    stream: &mut Streaming<common_v1::RunStreamRequest>,
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    active_session_id: Option<&str>,
    run_state: &mut RunStateMachine,
    session_id: &str,
    run_id: &str,
    pending_tool_proposals: &mut Vec<PendingRunStreamToolProposal>,
    tool_catalog_snapshot: &ModelVisibleToolCatalogSnapshot,
    tool_results: &mut Vec<RunStreamToolResultForModel>,
    remaining_tool_budget: &mut u32,
    allow_sensitive_tools: bool,
    approval_cache_generation: Option<u64>,
    tape_seq: &mut i64,
) -> Result<RunStreamProviderEventOutcome, Status> {
    if pending_tool_proposals.is_empty() {
        return Ok(RunStreamProviderEventOutcome::Continue);
    }

    let proposals = std::mem::take(pending_tool_proposals);
    let mut prepared_tools = Vec::new();
    for proposal in proposals {
        match gate_run_stream_provider_event_on_cancellation(
            sender,
            runtime_state,
            request_context,
            run_state,
            session_id,
            run_id,
            tape_seq,
        )
        .await?
        {
            RunStreamProviderEventGateOutcome::Continue => {}
            RunStreamProviderEventGateOutcome::Cancelled => {
                return Ok(RunStreamProviderEventOutcome::Cancelled);
            }
        }

        match prepare_run_stream_tool_proposal_event(
            sender,
            stream,
            runtime_state,
            request_context,
            active_session_id,
            session_id,
            run_id,
            proposal.proposal_id.as_str(),
            proposal.tool_name.as_str(),
            proposal.input_json.as_slice(),
            tool_catalog_snapshot,
            remaining_tool_budget,
            allow_sensitive_tools,
            approval_cache_generation,
            tape_seq,
        )
        .await?
        {
            RunStreamToolProposalPreparationOutcome::Prepared(prepared) => {
                prepared_tools.push(prepared);
            }
            RunStreamToolProposalPreparationOutcome::Completed(outcome) => {
                match flush_prepared_run_stream_tool_batch(
                    sender,
                    runtime_state,
                    request_context,
                    run_state,
                    run_id,
                    &mut prepared_tools,
                    tool_results,
                    remaining_tool_budget,
                    tape_seq,
                )
                .await?
                {
                    RunStreamProviderEventOutcome::Continue => {}
                    RunStreamProviderEventOutcome::Cancelled => {
                        return Ok(RunStreamProviderEventOutcome::Cancelled);
                    }
                }
                match push_run_stream_tool_execution_outcome(tool_results, outcome) {
                    RunStreamProviderEventOutcome::Continue => {}
                    RunStreamProviderEventOutcome::Cancelled => {
                        return Ok(RunStreamProviderEventOutcome::Cancelled);
                    }
                }
            }
        }
    }

    flush_prepared_run_stream_tool_batch(
        sender,
        runtime_state,
        request_context,
        run_state,
        run_id,
        &mut prepared_tools,
        tool_results,
        remaining_tool_budget,
        tape_seq,
    )
    .await
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn flush_prepared_run_stream_tool_batch(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    run_state: &mut RunStateMachine,
    run_id: &str,
    prepared_tools: &mut Vec<RunStreamPreparedToolExecution>,
    tool_results: &mut Vec<RunStreamToolResultForModel>,
    remaining_tool_budget: &mut u32,
    tape_seq: &mut i64,
) -> Result<RunStreamProviderEventOutcome, Status> {
    if prepared_tools.is_empty() {
        return Ok(RunStreamProviderEventOutcome::Continue);
    }

    match execute_prepared_run_stream_tool_proposals_ordered(
        sender,
        runtime_state,
        request_context,
        run_state,
        run_id,
        std::mem::take(prepared_tools),
        remaining_tool_budget,
        tape_seq,
    )
    .await?
    {
        RunStreamPreparedToolExecutionBatchOutcome::Completed(outcomes) => {
            for outcome in outcomes {
                match push_run_stream_tool_execution_outcome(tool_results, outcome) {
                    RunStreamProviderEventOutcome::Continue => {}
                    RunStreamProviderEventOutcome::Cancelled => {
                        return Ok(RunStreamProviderEventOutcome::Cancelled);
                    }
                }
            }
            Ok(RunStreamProviderEventOutcome::Continue)
        }
        RunStreamPreparedToolExecutionBatchOutcome::Cancelled => {
            Ok(RunStreamProviderEventOutcome::Cancelled)
        }
    }
}

fn push_run_stream_tool_execution_outcome(
    tool_results: &mut Vec<RunStreamToolResultForModel>,
    outcome: RunStreamToolExecutionOutcome,
) -> RunStreamProviderEventOutcome {
    match outcome {
        RunStreamToolExecutionOutcome::Completed { proposal_id, tool_name, outcome } => {
            tool_results.push(RunStreamToolResultForModel { proposal_id, tool_name, outcome });
            RunStreamProviderEventOutcome::Continue
        }
        RunStreamToolExecutionOutcome::Cancelled => RunStreamProviderEventOutcome::Cancelled,
    }
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
    tool_catalog_snapshot: &ModelVisibleToolCatalogSnapshot,
    summary_tokens: &mut Vec<String>,
    tool_results: &mut Vec<RunStreamToolResultForModel>,
    remaining_tool_budget: &mut u32,
    allow_sensitive_tools: bool,
    approval_cache_generation: Option<u64>,
    tape_seq: &mut i64,
    model_token_tape_events: &mut usize,
    model_token_compaction_emitted: &mut bool,
    stream_model_tokens_immediately: bool,
) -> Result<RunStreamProviderEventOutcome, Status> {
    process_provider_event_for_surface(
        runtime_state,
        session_id,
        run_id,
        provider_event,
        tool_catalog_snapshot,
        summary_tokens,
        tool_results,
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
            allow_sensitive_tools,
            approval_cache_generation,
            stream_model_tokens_immediately,
        }),
    )
    .await
}
