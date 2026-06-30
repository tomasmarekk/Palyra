//! Provider-event dispatch for run streams and routed channel messages.
//!
//! Takes the [`ProviderEvent`] batch produced by one model round and fans
//! each event out to the active surface: a gRPC run stream (tokens forwarded
//! to the client, tool proposals executed via `run_stream::tool_flow`) or
//! channel route-message handling (tool summaries appended to the reply
//! text). On the run-stream path, contiguous tool proposals are buffered and
//! flushed as one batch so approvals can be gathered up front while
//! execution stays in submission order, and orchestrator cancellation is
//! re-checked before every event so a cancel request wins promptly.

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

/// Result of the pre-event cancellation gate.
///
/// Same shape as [`RunStreamProviderEventOutcome`] but kept as a distinct
/// type: a gate `Cancelled` means the run was already transitioned to the
/// cancelled state before the event was processed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RunStreamProviderEventGateOutcome {
    Continue,
    Cancelled,
}

/// Result of processing a single provider event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RunStreamProviderEventOutcome {
    Continue,
    Cancelled,
}

/// Result of processing a whole provider-event batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RunStreamProviderEventsOutcome {
    /// All events processed; carries the accumulated non-empty model tokens
    /// and the tool results to feed back into the next model round.
    Completed {
        summary_tokens: Vec<String>,
        tool_results: Vec<RunStreamToolResultForModel>,
    },
    Cancelled,
}

/// A completed tool execution, keyed for the model's follow-up round.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RunStreamToolResultForModel {
    pub(crate) proposal_id: String,
    pub(crate) tool_name: String,
    pub(crate) input_json: Vec<u8>,
    pub(crate) outcome: ToolExecutionOutcome,
}

/// A tool proposal buffered until the contiguous proposal batch is flushed.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingRunStreamToolProposal {
    proposal_id: String,
    tool_name: String,
    input_json: Vec<u8>,
}

/// Mutable run-stream state a provider event may touch while being handled.
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

/// Route-message state a provider event may touch while being handled.
pub(crate) struct RouteMessageProviderEventSurface<'a> {
    pub(crate) request_context: &'a RequestContext,
    pub(crate) reply_text: &'a mut String,
}

/// The delivery surface a provider event is being processed for.
pub(crate) enum ProviderEventSurface<'a> {
    RunStream(RunStreamProviderEventSurface<'a>),
    RouteMessage(RouteMessageProviderEventSurface<'a>),
}

/// Checks for a pending cancel request and, if found, cancels the run.
///
/// A positive check records a `run_cancel_requested` runtime decision, tapes
/// it, and transitions the run state machine to cancelled before reporting
/// [`RunStreamProviderEventGateOutcome::Cancelled`] to the caller.
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

/// Handles one provider event for the given surface.
///
/// Model tokens are always accumulated into `summary_tokens` (whitespace-only
/// tokens are skipped) and streamed to the client only on run-stream surfaces
/// that opted into immediate token streaming. Tool proposals execute through
/// the surface-specific tool flow and land in `tool_results`.
///
/// # Errors
/// Returns `Status` when streaming a token or executing a tool proposal
/// fails.
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
                        input_json,
                        outcome,
                    } => {
                        tool_results.push(RunStreamToolResultForModel {
                            proposal_id,
                            tool_name,
                            input_json,
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

/// Processes one model round's provider events on the run-stream surface.
///
/// Tool proposals are buffered while contiguous and flushed as a batch when a
/// model token arrives or the batch ends, so approval prompts for the whole
/// batch can be raised before any tool runs while execution still happens in
/// submission order. Cancellation is re-checked before every event.
///
/// # Errors
/// Returns `Status` when cancellation gating, token streaming, or tool
/// execution fails.
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
                // A token ends the contiguous proposal run: execute the
                // buffered batch first so tool results precede later text.
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

/// Prepares and executes the buffered tool-proposal batch in order.
///
/// Preparation (validation, gating, approval collection) runs per proposal;
/// proposals that complete during preparation (for example denials) force the
/// already-prepared prefix to execute first so result order still matches
/// submission order.
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
                // Drain prepared-but-unexecuted proposals before recording
                // this short-circuited outcome to preserve submission order.
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

/// Executes a prepared tool batch and folds the outcomes into `tool_results`.
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

/// Folds one execution outcome into `tool_results`, mapping cancellation.
fn push_run_stream_tool_execution_outcome(
    tool_results: &mut Vec<RunStreamToolResultForModel>,
    outcome: RunStreamToolExecutionOutcome,
) -> RunStreamProviderEventOutcome {
    match outcome {
        RunStreamToolExecutionOutcome::Completed {
            proposal_id,
            tool_name,
            input_json,
            outcome,
        } => {
            tool_results.push(RunStreamToolResultForModel {
                proposal_id,
                tool_name,
                input_json,
                outcome,
            });
            RunStreamProviderEventOutcome::Continue
        }
        RunStreamToolExecutionOutcome::Cancelled => RunStreamProviderEventOutcome::Cancelled,
    }
}

/// Adapts loose run-stream arguments into the surface-based event handler.
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
