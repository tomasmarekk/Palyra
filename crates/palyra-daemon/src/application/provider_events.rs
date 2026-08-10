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
            orchestration::RunStreamHarnessLifecycle,
            tape::{
                append_runtime_decision_tape_event, redact_run_stream_text,
                send_model_token_with_tape,
            },
            tool_flow::{
                execute_prepared_run_stream_tool_proposals_ordered,
                prepare_run_stream_tool_proposal_event, process_run_stream_tool_proposal_event,
                RunStreamPreparedToolExecution, RunStreamPreparedToolExecutionBatchOutcome,
                RunStreamToolProposalPreparationOutcome,
            },
        },
        tool_registry::ModelVisibleToolCatalogSnapshot,
    },
    gateway::{
        current_unix_ms, GatewayRuntimeState, RunStreamToolExecutionOutcome, CANCELLED_REASON,
    },
    model_provider::ProviderEvent,
    orchestrator::{RunLifecycleState, RunStateMachine},
    tool_protocol::ToolExecutionOutcome,
    transport::grpc::{auth::RequestContext, proto::palyra::common::v1 as common_v1},
};
use serde_json::{json, Value};

const PARTIAL_ASSISTANT_ABORT_EVENT: &str = "partial_assistant.abort";
const MAX_PARTIAL_ASSISTANT_ABORT_CHARS: usize = 4_096;

/// Result of the pre-event cancellation gate.
///
/// Same shape as [`RunStreamProviderEventOutcome`] but kept as a distinct
/// type: a terminal gate means durable settlement already chose the state
/// before the event was processed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RunStreamProviderEventGateOutcome {
    Continue,
    Terminal(RunLifecycleState),
}

/// Result of processing a single provider event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RunStreamProviderEventOutcome {
    Continue,
    Suspended,
    Terminal(RunLifecycleState),
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
    Suspended,
    Terminal(RunLifecycleState),
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

#[derive(Debug, Clone, Copy)]
struct RunStreamPartialAbortContext<'a> {
    summary_tokens: &'a [String],
    stream_model_tokens_immediately: bool,
    model_token_tape_events: usize,
    model_token_compaction_emitted: bool,
}

struct RunStreamProviderCancellationGate<'a> {
    sender: &'a mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &'a Arc<GatewayRuntimeState>,
    request_context: &'a RequestContext,
    run_state: &'a mut RunStateMachine,
    session_id: &'a str,
    run_id: &'a str,
    flow_control: &'a crate::application::run_stream::flow_control::RunStreamFlowControl,
    tape_seq: &'a mut i64,
    partial_abort: Option<RunStreamPartialAbortContext<'a>>,
    harness_lifecycle: Option<&'a RunStreamHarnessLifecycle>,
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
    pub(crate) approval_cache_generation: Option<u64>,
    pub(crate) flow_control: &'a crate::application::run_stream::flow_control::RunStreamFlowControl,
    pub(crate) stream_model_tokens_immediately: bool,
    pub(crate) harness_lifecycle: Option<&'a RunStreamHarnessLifecycle>,
}

/// Route-message state a provider event may touch while being handled.
pub(crate) struct RouteMessageProviderEventSurface<'a> {
    pub(crate) request_context: &'a RequestContext,
    pub(crate) flow_control: &'a crate::application::run_stream::flow_control::RunStreamFlowControl,
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
/// it, and follows the effective durable terminal state before reporting it to
/// the caller.
#[allow(clippy::result_large_err)]
async fn gate_run_stream_provider_event_on_cancellation(
    context: RunStreamProviderCancellationGate<'_>,
) -> Result<RunStreamProviderEventGateOutcome, Status> {
    let RunStreamProviderCancellationGate {
        sender,
        runtime_state,
        request_context,
        run_state,
        session_id,
        run_id,
        flow_control,
        tape_seq,
        partial_abort,
        harness_lifecycle,
    } = context;
    match runtime_state.is_orchestrator_cancel_requested(run_id.to_owned()).await {
        Ok(true) => {
            append_partial_assistant_abort_tape_event_if_visible(
                runtime_state,
                request_context,
                run_id,
                tape_seq,
                partial_abort,
            )
            .await?;
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
            let effective_state = transition_run_stream_to_cancelled(
                sender,
                runtime_state,
                run_state,
                run_id,
                flow_control,
                tape_seq,
                harness_lifecycle,
            )
            .await?;
            Ok(RunStreamProviderEventGateOutcome::Terminal(effective_state))
        }
        Ok(false) => Ok(RunStreamProviderEventGateOutcome::Continue),
        Err(error) => Err(error),
    }
}

#[allow(clippy::result_large_err)]
async fn append_partial_assistant_abort_tape_event_if_visible(
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    run_id: &str,
    tape_seq: &mut i64,
    partial_abort: Option<RunStreamPartialAbortContext<'_>>,
) -> Result<(), Status> {
    let Some(partial_abort) = partial_abort else {
        return Ok(());
    };
    let abort_reason = runtime_state
        .orchestrator_run_status_snapshot(run_id.to_owned())
        .await?
        .and_then(|snapshot| snapshot.cancel_reason)
        .unwrap_or_else(|| CANCELLED_REASON.to_owned());
    let Some(payload) = partial_assistant_abort_payload(
        request_context,
        run_id,
        abort_reason.as_str(),
        partial_abort,
        current_unix_ms(),
    ) else {
        return Ok(());
    };
    runtime_state
        .append_orchestrator_tape_event(crate::journal::OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: PARTIAL_ASSISTANT_ABORT_EVENT.to_owned(),
            payload_json: payload.to_string(),
        })
        .await?;
    *tape_seq += 1;
    Ok(())
}

fn partial_assistant_abort_payload(
    request_context: &RequestContext,
    run_id: &str,
    abort_reason: &str,
    partial_abort: RunStreamPartialAbortContext<'_>,
    observed_at_unix_ms: i64,
) -> Option<Value> {
    if !partial_abort.stream_model_tokens_immediately || partial_abort.summary_tokens.is_empty() {
        return None;
    }
    let partial_text = partial_abort.summary_tokens.concat();
    if partial_text.trim().is_empty() {
        return None;
    }
    let redacted_partial_text = crate::model_provider::redact_remote_secret_fragments(
        redact_run_stream_text(partial_text.as_str()).as_str(),
    );
    let partial_char_count = redacted_partial_text.chars().count();
    if partial_char_count == 0 {
        return None;
    }
    let (bounded_partial_text, truncated) =
        bounded_partial_assistant_text(redacted_partial_text.as_str());
    let persisted_char_count = bounded_partial_text.chars().count();
    Some(json!({
        "schema_version": 1,
        "event": PARTIAL_ASSISTANT_ABORT_EVENT,
        "run_id": run_id,
        "observed_at_unix_ms": observed_at_unix_ms,
        "aborted_by": {
            "kind": "run_stream_cancel_request",
            "principal": request_context.principal,
            "device_id": request_context.device_id,
            "channel": request_context.channel,
        },
        "abort_reason": abort_reason,
        "partial_char_count": partial_char_count,
        "persisted_char_count": persisted_char_count,
        "truncated": truncated,
        "redaction_level": "run_stream_text_redaction",
        "content_visibility": "model_visible_streamed",
        "internal_reasoning_persisted": false,
        "source": {
            "model_token_events_seen": partial_abort.model_token_tape_events,
            "token_tape_compacted": partial_abort.model_token_compaction_emitted,
        },
        "partial_text": bounded_partial_text,
    }))
}

fn bounded_partial_assistant_text(input: &str) -> (String, bool) {
    let char_count = input.chars().count();
    if char_count <= MAX_PARTIAL_ASSISTANT_ABORT_CHARS {
        return (input.to_owned(), false);
    }
    let take_chars = MAX_PARTIAL_ASSISTANT_ABORT_CHARS.saturating_sub(3);
    let mut bounded = input.chars().take(take_chars).collect::<String>();
    bounded.push_str("...");
    (bounded, true)
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
                    context.approval_cache_generation,
                    context.flow_control,
                    tape_seq,
                    context.harness_lifecycle,
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
                    RunStreamToolExecutionOutcome::Terminal(state) => {
                        Ok(RunStreamProviderEventOutcome::Terminal(state))
                    }
                    RunStreamToolExecutionOutcome::Suspended => {
                        Ok(RunStreamProviderEventOutcome::Suspended)
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
                    context.flow_control,
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
    approval_cache_generation: Option<u64>,
    flow_control: &crate::application::run_stream::flow_control::RunStreamFlowControl,
    tape_seq: &mut i64,
    model_token_tape_events: &mut usize,
    model_token_compaction_emitted: &mut bool,
    stream_model_tokens_immediately: bool,
    harness_lifecycle: Option<&RunStreamHarnessLifecycle>,
) -> Result<RunStreamProviderEventsOutcome, Status> {
    let mut summary_tokens = Vec::new();
    let mut tool_results = Vec::new();
    let mut pending_tool_proposals = Vec::new();
    for provider_event in provider_events {
        match gate_run_stream_provider_event_on_cancellation(RunStreamProviderCancellationGate {
            sender,
            runtime_state,
            request_context,
            run_state,
            session_id,
            run_id,
            flow_control,
            tape_seq,
            partial_abort: Some(RunStreamPartialAbortContext {
                summary_tokens: summary_tokens.as_slice(),
                stream_model_tokens_immediately,
                model_token_tape_events: *model_token_tape_events,
                model_token_compaction_emitted: *model_token_compaction_emitted,
            }),
            harness_lifecycle,
        })
        .await?
        {
            RunStreamProviderEventGateOutcome::Continue => {}
            RunStreamProviderEventGateOutcome::Terminal(state) => {
                return Ok(RunStreamProviderEventsOutcome::Terminal(state));
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
                    approval_cache_generation,
                    flow_control,
                    tape_seq,
                    harness_lifecycle,
                )
                .await?
                {
                    RunStreamProviderEventOutcome::Continue => {}
                    RunStreamProviderEventOutcome::Suspended => {
                        return Ok(RunStreamProviderEventsOutcome::Suspended);
                    }
                    RunStreamProviderEventOutcome::Terminal(state) => {
                        return Ok(RunStreamProviderEventsOutcome::Terminal(state));
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
                    approval_cache_generation,
                    flow_control,
                    tape_seq,
                    model_token_tape_events,
                    model_token_compaction_emitted,
                    stream_model_tokens_immediately,
                    harness_lifecycle,
                )
                .await?
                {
                    RunStreamProviderEventOutcome::Continue => {}
                    RunStreamProviderEventOutcome::Suspended => {
                        return Ok(RunStreamProviderEventsOutcome::Suspended);
                    }
                    RunStreamProviderEventOutcome::Terminal(state) => {
                        return Ok(RunStreamProviderEventsOutcome::Terminal(state));
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
        approval_cache_generation,
        flow_control,
        tape_seq,
        harness_lifecycle,
    )
    .await?
    {
        RunStreamProviderEventOutcome::Continue => {}
        RunStreamProviderEventOutcome::Suspended => {
            return Ok(RunStreamProviderEventsOutcome::Suspended);
        }
        RunStreamProviderEventOutcome::Terminal(state) => {
            return Ok(RunStreamProviderEventsOutcome::Terminal(state));
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
    approval_cache_generation: Option<u64>,
    flow_control: &crate::application::run_stream::flow_control::RunStreamFlowControl,
    tape_seq: &mut i64,
    harness_lifecycle: Option<&RunStreamHarnessLifecycle>,
) -> Result<RunStreamProviderEventOutcome, Status> {
    if pending_tool_proposals.is_empty() {
        return Ok(RunStreamProviderEventOutcome::Continue);
    }

    let proposals = std::mem::take(pending_tool_proposals);
    let mut prepared_tools = Vec::new();
    for proposal in proposals {
        match gate_run_stream_provider_event_on_cancellation(RunStreamProviderCancellationGate {
            sender,
            runtime_state,
            request_context,
            run_state,
            session_id,
            run_id,
            flow_control,
            tape_seq,
            partial_abort: None,
            harness_lifecycle,
        })
        .await?
        {
            RunStreamProviderEventGateOutcome::Continue => {}
            RunStreamProviderEventGateOutcome::Terminal(state) => {
                return Ok(RunStreamProviderEventOutcome::Terminal(state));
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
            approval_cache_generation,
            flow_control,
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
                    flow_control,
                    tape_seq,
                    harness_lifecycle,
                )
                .await?
                {
                    RunStreamProviderEventOutcome::Continue => {}
                    RunStreamProviderEventOutcome::Suspended => {
                        return Ok(RunStreamProviderEventOutcome::Suspended);
                    }
                    RunStreamProviderEventOutcome::Terminal(state) => {
                        return Ok(RunStreamProviderEventOutcome::Terminal(state));
                    }
                }
                match push_run_stream_tool_execution_outcome(tool_results, outcome) {
                    RunStreamProviderEventOutcome::Continue => {}
                    RunStreamProviderEventOutcome::Suspended => {
                        return Ok(RunStreamProviderEventOutcome::Suspended);
                    }
                    RunStreamProviderEventOutcome::Terminal(state) => {
                        return Ok(RunStreamProviderEventOutcome::Terminal(state));
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
        flow_control,
        tape_seq,
        harness_lifecycle,
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
    flow_control: &crate::application::run_stream::flow_control::RunStreamFlowControl,
    tape_seq: &mut i64,
    harness_lifecycle: Option<&RunStreamHarnessLifecycle>,
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
        flow_control,
        tape_seq,
        harness_lifecycle,
    )
    .await?
    {
        RunStreamPreparedToolExecutionBatchOutcome::Completed(outcomes) => {
            for outcome in outcomes {
                match push_run_stream_tool_execution_outcome(tool_results, outcome) {
                    RunStreamProviderEventOutcome::Continue => {}
                    RunStreamProviderEventOutcome::Suspended => {
                        return Ok(RunStreamProviderEventOutcome::Suspended);
                    }
                    RunStreamProviderEventOutcome::Terminal(state) => {
                        return Ok(RunStreamProviderEventOutcome::Terminal(state));
                    }
                }
            }
            Ok(RunStreamProviderEventOutcome::Continue)
        }
        RunStreamPreparedToolExecutionBatchOutcome::Terminal(state) => {
            Ok(RunStreamProviderEventOutcome::Terminal(state))
        }
        RunStreamPreparedToolExecutionBatchOutcome::Suspended => {
            Ok(RunStreamProviderEventOutcome::Suspended)
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
        RunStreamToolExecutionOutcome::Terminal(state) => {
            RunStreamProviderEventOutcome::Terminal(state)
        }
        RunStreamToolExecutionOutcome::Suspended => RunStreamProviderEventOutcome::Suspended,
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
    approval_cache_generation: Option<u64>,
    flow_control: &crate::application::run_stream::flow_control::RunStreamFlowControl,
    tape_seq: &mut i64,
    model_token_tape_events: &mut usize,
    model_token_compaction_emitted: &mut bool,
    stream_model_tokens_immediately: bool,
    harness_lifecycle: Option<&RunStreamHarnessLifecycle>,
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
            approval_cache_generation,
            flow_control,
            stream_model_tokens_immediately,
            harness_lifecycle,
        }),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_request_context() -> RequestContext {
        RequestContext {
            principal: "user:ops".to_owned(),
            device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAA".to_owned(),
            channel: Some("cli".to_owned()),
        }
    }

    #[test]
    fn partial_assistant_abort_payload_redacts_visible_streamed_tokens() {
        let tokens = vec![
            "Visible partial before abort with secret ".to_owned(),
            "sk-test-secret-token".to_owned(),
        ];
        let payload = partial_assistant_abort_payload(
            &test_request_context(),
            "01ARZ3NDEKTSV4RRFFQ69G5FAB",
            "operator_requested",
            RunStreamPartialAbortContext {
                summary_tokens: tokens.as_slice(),
                stream_model_tokens_immediately: true,
                model_token_tape_events: 2,
                model_token_compaction_emitted: false,
            },
            1_700_000_000_000,
        )
        .expect("visible streamed partial should produce an abort payload");

        assert_eq!(payload["event"], PARTIAL_ASSISTANT_ABORT_EVENT);
        assert_eq!(payload["abort_reason"], "operator_requested");
        assert_eq!(payload["content_visibility"], "model_visible_streamed");
        assert_eq!(payload["internal_reasoning_persisted"], false);
        assert_eq!(payload["source"]["model_token_events_seen"], 2);
        let partial_text =
            payload["partial_text"].as_str().expect("partial text should be serialized");
        assert!(partial_text.contains("Visible partial before abort"));
        assert!(!partial_text.contains("sk-test-secret-token"));
    }

    #[test]
    fn partial_assistant_abort_payload_skips_unstreamed_tokens() {
        let tokens = vec!["candidate final answer".to_owned()];
        let payload = partial_assistant_abort_payload(
            &test_request_context(),
            "01ARZ3NDEKTSV4RRFFQ69G5FAC",
            "operator_requested",
            RunStreamPartialAbortContext {
                summary_tokens: tokens.as_slice(),
                stream_model_tokens_immediately: false,
                model_token_tape_events: 0,
                model_token_compaction_emitted: false,
            },
            1_700_000_000_000,
        );

        assert!(payload.is_none());
    }
}
