use std::{sync::Arc, time::Duration};

use serde_json::{json, Value};
use tokio::{
    sync::mpsc,
    time::{interval, interval_at, Instant as TokioInstant, MissedTickBehavior},
};
use tonic::{Code, Status, Streaming};
use tracing::{debug, warn, Instrument};

use crate::{
    application::learning::schedule_post_run_reflection,
    application::provider_events::{
        process_run_stream_provider_events, RunStreamProviderEventsOutcome,
        RunStreamToolResultForModel,
    },
    application::provider_input::{
        build_provider_image_inputs, prepare_model_provider_input, MemoryPromptFailureMode,
        PrepareModelProviderInputRequest,
    },
    application::tool_registry::{
        build_model_visible_tool_catalog_snapshot, snapshot_to_provider_request_value,
        tool_catalog_tape_payload, ModelVisibleToolCatalogSnapshot, ToolCatalogBuildRequest,
        ToolExposureSurface,
    },
    delegation::DelegationSnapshot,
    gateway::{
        canonical_id, cleanup_run_resources, current_unix_ms, ingest_memory_best_effort, non_empty,
        record_message_router_journal_event, security_requests_json_mode, truncate_with_ellipsis,
        GatewayRuntimeConfigSnapshot, GatewayRuntimeState, CANCELLED_REASON,
    },
    journal::{
        MemorySource, OrchestratorCancelRequest, OrchestratorRunMetadataUpdateRequest,
        OrchestratorRunStartRequest, OrchestratorSessionResolveRequest,
        OrchestratorTapeAppendRequest, OrchestratorUsageDelta,
    },
    model_provider::{
        bounded_provider_turn_output_for_persistence, provider_events_from_output, ProviderEvent,
        ProviderFinishReason, ProviderMessage, ProviderMessageContentPart, ProviderMessageRole,
        ProviderOutputContentPart, ProviderRawProviderRefs, ProviderRequest, ProviderResponse,
        ProviderRouteSelectionTrace, ProviderTurnOutput, ProviderUsage,
    },
    orchestrator::{
        estimate_token_count, is_cancel_command, RunLifecycleState, RunStateMachine, RunTransition,
    },
    provider_leases::ProviderLeaseExecutionContext,
    self_healing::{WorkHeartbeatKind, WorkHeartbeatUpdate},
    tool_protocol::ToolRequestContext,
    transport::grpc::{auth::RequestContext, proto::palyra::common::v1 as common_v1},
    usage_governance::{plan_usage_routing, RoutingTaskClass, UsageRoutingPlanRequest},
};

use super::{
    agent_loop::{
        AgentLoopTerminationReason, AgentRunLoopState, DEFAULT_AGENT_LOOP_WALL_CLOCK_BUDGET_MS,
    },
    cancellation::transition_run_stream_to_cancelled,
    tape::{
        maybe_compact_context_after_tool_results, send_model_token_with_tape,
        send_status_with_tape, RUN_STREAM_RESPONSE_CHANNEL_CLOSED_MESSAGE,
    },
};

const PROVIDER_PROGRESS_HEARTBEAT_MS: u64 = 20_000;
const PROVIDER_FAILOVER_DEADLINE_GRACE_MS: u64 = 5_000;
const MAX_LENGTH_RECOVERY_ATTEMPTS: u8 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BackgroundBudgetGuardDecision {
    budget_tokens: u64,
    consumed_tokens: u64,
    estimated_input_tokens: u64,
    max_output_tokens: u64,
}

impl BackgroundBudgetGuardDecision {
    fn tape_payload(self) -> String {
        json!({
            "schema_version": 1,
            "event": "agent_loop.background_budget_guard",
            "status": "applied",
            "budget_tokens": self.budget_tokens,
            "consumed_tokens": self.consumed_tokens,
            "estimated_input_tokens": self.estimated_input_tokens,
            "max_output_tokens": self.max_output_tokens,
        })
        .to_string()
    }
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RunStreamMessageProcessingOutcome {
    Continue,
    Terminate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RunStreamProviderResponseOutcome {
    Completed {
        tool_result_messages: Vec<ProviderMessage>,
        provider_trace_ref: Option<String>,
        final_reply_text: Option<String>,
        final_provider_output: Option<Box<ProviderTurnOutput>>,
        final_reply_tokens_deferred: bool,
    },
    Failed {
        message: String,
        provider_trace_ref: Option<String>,
        reason: AgentLoopTerminationReason,
    },
    Cancelled,
}

fn run_stream_attachment_metadata(attachments: &[common_v1::MessageAttachment]) -> Vec<Value> {
    attachments
        .iter()
        .map(|attachment| {
            let kind =
                match common_v1::message_attachment::AttachmentKind::try_from(attachment.kind).ok()
                {
                    Some(common_v1::message_attachment::AttachmentKind::Image) => "image",
                    Some(common_v1::message_attachment::AttachmentKind::File) => "file",
                    Some(common_v1::message_attachment::AttachmentKind::Audio) => "audio",
                    Some(common_v1::message_attachment::AttachmentKind::Video) => "video",
                    _ => "unspecified",
                };
            json!({
                "kind": kind,
                "artifact_id": attachment
                    .artifact_id
                    .as_ref()
                    .map(|value| value.ulid.clone()),
                "size_bytes": if attachment.size_bytes > 0 {
                    Some(attachment.size_bytes)
                } else {
                    None
                },
            })
        })
        .collect()
}

fn provider_request_timeout(config: &GatewayRuntimeConfigSnapshot) -> Duration {
    Duration::from_millis(config.model_provider_request_timeout_ms.max(1))
}

fn provider_request_deadline_timeout(
    base_timeout: Duration,
    route_selection: &ProviderRouteSelectionTrace,
    request: &ProviderRequest,
) -> Duration {
    let attempt_count = provider_request_deadline_attempt_count(route_selection, request);
    let multiplier = attempt_count.min(u32::MAX as usize) as u32;
    let mut deadline = base_timeout.saturating_mul(multiplier.max(1));
    if attempt_count > 1 {
        deadline =
            deadline.saturating_add(Duration::from_millis(PROVIDER_FAILOVER_DEADLINE_GRACE_MS));
    }
    deadline
}

fn provider_request_deadline_attempt_count(
    route_selection: &ProviderRouteSelectionTrace,
    request: &ProviderRequest,
) -> usize {
    if !route_selection.failover_enabled || request.model_override.is_some() {
        return 1;
    }

    let selected_provider_id = route_selection.selected_provider_id.as_deref().or_else(|| {
        route_selection
            .candidates
            .iter()
            .find(|candidate| candidate.selected)
            .map(|candidate| candidate.provider_id.as_str())
    });
    let Some(selected_provider_id) = selected_provider_id else {
        return 1;
    };

    let fallback_attempts = route_selection
        .candidates
        .iter()
        .filter(|candidate| {
            candidate.role == "chat"
                && candidate.capability_state == "eligible"
                && candidate.provider_id != selected_provider_id
        })
        .count();
    1 + fallback_attempts
}

fn duration_millis_u64(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

fn provider_request_timeout_status(run_id: &str, timeout: Duration) -> Status {
    let timeout_ms = duration_millis_u64(timeout);
    Status::deadline_exceeded(format!(
        "model provider turn timed out after {timeout_ms}ms for run {run_id}; no model tokens, tool proposals, or final answer arrived before the deadline. Retry the run, inspect provider connectivity, or increase model_provider.request_timeout_ms for providers that are expected to respond more slowly."
    ))
}

fn provider_model_override_for_routing(
    routing_mode: &str,
    actual_model_id: &str,
) -> Option<String> {
    (routing_mode == "enforced").then(|| actual_model_id.to_owned())
}

fn background_run_budget_tokens(parameter_delta_json: Option<&str>) -> Option<u64> {
    let parsed = serde_json::from_str::<Value>(parameter_delta_json?).ok()?;
    parsed
        .pointer("/background_task/budget_tokens")
        .and_then(Value::as_u64)
        .filter(|value| *value > 0)
}

fn apply_background_budget_guard(
    request: &mut ProviderRequest,
    budget_tokens: u64,
    consumed_tokens: u64,
) -> Result<BackgroundBudgetGuardDecision, String> {
    let estimated_input_tokens = estimate_provider_request_input_tokens(request);
    let committed_tokens = consumed_tokens.saturating_add(estimated_input_tokens);
    if committed_tokens >= budget_tokens {
        return Err(format!(
            "background task token budget exhausted before provider turn: budget_tokens={budget_tokens} consumed_tokens={consumed_tokens} estimated_input_tokens={estimated_input_tokens}"
        ));
    }
    let available_output_tokens = budget_tokens.saturating_sub(committed_tokens).max(1);
    let max_output_tokens = request
        .max_output_tokens
        .unwrap_or(available_output_tokens)
        .min(available_output_tokens)
        .max(1);
    request.max_output_tokens = Some(max_output_tokens);
    Ok(BackgroundBudgetGuardDecision {
        budget_tokens,
        consumed_tokens,
        estimated_input_tokens,
        max_output_tokens,
    })
}

fn background_budget_overrun_message(budget_tokens: u64, consumed_tokens: u64) -> Option<String> {
    (consumed_tokens > budget_tokens).then(|| {
        format!(
            "background task token budget exhausted after provider turn: budget_tokens={budget_tokens} consumed_tokens={consumed_tokens}"
        )
    })
}

fn estimate_provider_request_input_tokens(request: &ProviderRequest) -> u64 {
    let message_tokens = request
        .effective_messages()
        .iter()
        .map(estimate_provider_message_input_tokens)
        .fold(0_u64, u64::saturating_add);
    let vision_tokens =
        u64::try_from(request.vision_inputs.len()).unwrap_or(u64::MAX).saturating_mul(256);
    message_tokens.saturating_add(vision_tokens)
}

fn estimate_provider_message_input_tokens(message: &ProviderMessage) -> u64 {
    let content_tokens = message
        .content
        .iter()
        .map(|part| match part {
            ProviderMessageContentPart::Text { text } => {
                estimate_background_budget_text_tokens(text)
            }
            ProviderMessageContentPart::Image { .. } => 256,
        })
        .fold(0_u64, u64::saturating_add);
    let tool_call_tokens = message
        .tool_calls
        .iter()
        .map(|tool_call| {
            estimate_background_budget_text_tokens(tool_call.proposal_id.as_str())
                .saturating_add(estimate_background_budget_text_tokens(
                    tool_call.tool_name.as_str(),
                ))
                .saturating_add(estimate_background_budget_text_tokens(
                    tool_call.input_json.to_string().as_str(),
                ))
        })
        .fold(0_u64, u64::saturating_add);
    content_tokens.saturating_add(tool_call_tokens).saturating_add(4)
}

fn estimate_background_budget_text_tokens(value: &str) -> u64 {
    if value.is_empty() {
        return 0;
    }
    let whitespace_tokens = estimate_token_count(value);
    let character_tokens =
        u64::try_from(value.chars().count()).unwrap_or(u64::MAX).saturating_add(3) / 4;
    whitespace_tokens.max(character_tokens).max(1)
}

#[allow(clippy::result_large_err)]
async fn record_run_stream_provider_usage(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    provider_response: &ProviderResponse,
) -> Result<(), Status> {
    runtime_state
        .add_orchestrator_usage(OrchestratorUsageDelta {
            run_id: run_id.to_owned(),
            prompt_tokens_delta: provider_response.prompt_tokens,
            completion_tokens_delta: provider_response.completion_tokens,
        })
        .await
}

struct RunStreamUserMessage<'a> {
    run_id: &'a str,
    request_context: &'a RequestContext,
    envelope_id: Option<&'a common_v1::CanonicalId>,
    input_content: &'a common_v1::MessageContent,
    session_key: &'a str,
    json_mode_requested: bool,
}

#[allow(clippy::result_large_err)]
async fn append_run_stream_user_message(
    runtime_state: &Arc<GatewayRuntimeState>,
    tape_seq: &mut i64,
    message: RunStreamUserMessage<'_>,
) -> Result<(), Status> {
    if message.input_content.text.trim().is_empty() && message.input_content.attachments.is_empty()
    {
        return Ok(());
    }

    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: message.run_id.to_owned(),
            seq: *tape_seq,
            event_type: "message.received".to_owned(),
            payload_json: json!({
                "envelope_id": message.envelope_id.map(|value| value.ulid.clone()),
                "text": message.input_content.text.clone(),
                "channel": message.request_context.channel.clone(),
                "session_key": non_empty(message.session_key.to_owned()),
                "json_mode_requested": message.json_mode_requested,
                "attachments": run_stream_attachment_metadata(message.input_content.attachments.as_slice()),
            })
            .to_string(),
        })
        .await?;
    *tape_seq = tape_seq.saturating_add(1);
    Ok(())
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
        if matches!(
            runtime_state.orchestrator_run_status_snapshot(run_id.to_owned()).await,
            Ok(Some(snapshot)) if snapshot.state == RunLifecycleState::Cancelled.as_str()
        ) {
            cleanup_run_resources(runtime_state, run_id, CANCELLED_REASON).await;
            runtime_state.clear_self_healing_heartbeat(WorkHeartbeatKind::Run, run_id);
            return Ok(RunStreamPostProviderOutcome::Cancelled);
        }
        send_status_with_tape(
            sender,
            runtime_state,
            run_id,
            tape_seq,
            common_v1::stream_status::StatusKind::Done,
            "completed",
        )
        .await?;
        cleanup_run_resources(runtime_state, run_id, "completed").await;
        runtime_state.clear_self_healing_heartbeat(WorkHeartbeatKind::Run, run_id);
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
    let provider_timeout = provider_request_timeout(&runtime_state.config);
    let provider_status = runtime_state.model_provider_status_snapshot();
    let provider_deadline_timeout = provider_request_deadline_timeout(
        provider_timeout,
        &provider_status.route_selection,
        &provider_request,
    );
    let provider_span = tracing::info_span!(
        "provider.call",
        run_id = %run_id,
        trace_id = provider_request.context_trace_id.as_deref().unwrap_or("none"),
        has_tool_catalog = provider_request.tool_catalog_snapshot.is_some(),
        json_mode = provider_request.json_mode,
        status = tracing::field::Empty,
    );
    let mut provider_future = Box::pin(
        runtime_state
            .execute_model_provider_with_lease(provider_request, lease_context)
            .instrument(provider_span),
    );
    let provider_started_at = TokioInstant::now();
    let provider_deadline = tokio::time::sleep(provider_deadline_timeout);
    tokio::pin!(provider_deadline);
    let mut cancel_poll = interval(Duration::from_millis(100));
    cancel_poll.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut progress_heartbeat = interval_at(
        TokioInstant::now() + Duration::from_millis(PROVIDER_PROGRESS_HEARTBEAT_MS),
        Duration::from_millis(PROVIDER_PROGRESS_HEARTBEAT_MS),
    );
    progress_heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            provider_result = &mut provider_future => {
                return provider_result
                    .map(Box::new)
                    .map(RunStreamProviderRequestOutcome::Completed);
            }
            _ = &mut provider_deadline => {
                return Err(provider_request_timeout_status(run_id, provider_deadline_timeout));
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
            _ = progress_heartbeat.tick() => {
                if run_state.state() == RunLifecycleState::InProgress {
                    let elapsed_ms = duration_millis_u64(provider_started_at.elapsed());
                    let timeout_ms = duration_millis_u64(provider_deadline_timeout);
                    let message = if provider_deadline_timeout == provider_timeout {
                        format!(
                            "waiting for model provider response (elapsed_ms={elapsed_ms}, timeout_ms={timeout_ms})"
                        )
                    } else {
                        let provider_attempt_timeout_ms = duration_millis_u64(provider_timeout);
                        format!(
                            "waiting for model provider response (elapsed_ms={elapsed_ms}, timeout_ms={timeout_ms}, provider_attempt_timeout_ms={provider_attempt_timeout_ms}, failover_deadline_extended=true)"
                        )
                    };
                    send_status_with_tape(
                        sender,
                        runtime_state,
                        run_id,
                        tape_seq,
                        common_v1::stream_status::StatusKind::InProgress,
                        message.as_str(),
                    )
                    .await?;
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
async fn send_agent_loop_progress_status(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    phase: &str,
) -> Result<(), Status> {
    send_status_with_tape(
        sender,
        runtime_state,
        run_id,
        tape_seq,
        common_v1::stream_status::StatusKind::InProgress,
        format!("progress:{phase}").as_str(),
    )
    .await
}

#[allow(clippy::result_large_err)]
async fn send_terminal_agent_loop_progress_status(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    phase: &str,
) -> Result<(), Status> {
    match send_agent_loop_progress_status(sender, runtime_state, run_id, tape_seq, phase).await {
        Ok(()) => Ok(()),
        Err(error) if is_run_stream_response_channel_closed(&error) => {
            debug!(
                run_id,
                phase, "skipping terminal agent loop progress status after client stream closed"
            );
            Ok(())
        }
        Err(error) => Err(error),
    }
}

fn is_run_stream_response_channel_closed(error: &Status) -> bool {
    error.code() == Code::Cancelled && error.message() == RUN_STREAM_RESPONSE_CHANNEL_CLOSED_MESSAGE
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
    let message = loop_state.message_with_cleanup_guidance(message);
    append_agent_loop_tape_event(
        runtime_state,
        run_id,
        tape_seq,
        "agent_loop.terminated",
        loop_state.termination_payload(run_id, reason, message.as_str(), provider_trace_ref),
    )
    .await?;
    send_terminal_agent_loop_progress_status(
        sender,
        runtime_state,
        run_id,
        tape_seq,
        "agent_loop.terminated",
    )
    .await?;
    run_state
        .transition(RunTransition::Fail)
        .map_err(|error| Status::internal(error.to_string()))?;
    runtime_state
        .update_orchestrator_run_state(
            run_id.to_owned(),
            RunLifecycleState::Failed,
            Some(message.clone()),
        )
        .await?;
    runtime_state.clear_self_healing_heartbeat(WorkHeartbeatKind::Run, run_id);
    let status_result = send_status_with_tape(
        sender,
        runtime_state,
        run_id,
        tape_seq,
        common_v1::stream_status::StatusKind::Failed,
        message.as_str(),
    )
    .await;
    cleanup_run_resources(runtime_state, run_id, message.as_str()).await;
    status_result
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn send_budget_exhausted_partial_summary_tokens(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    session_id: &str,
    run_id: &str,
    tape_seq: &mut i64,
    model_token_tape_events: &mut usize,
    model_token_compaction_emitted: &mut bool,
    reason: AgentLoopTerminationReason,
    loop_state: &AgentRunLoopState,
    message: &str,
) -> Result<(), Status> {
    if !should_emit_budget_exhausted_partial_summary(reason, loop_state) {
        return Ok(());
    }
    let summary = loop_state.message_with_cleanup_guidance(message);
    send_deferred_final_reply_tokens(
        sender,
        runtime_state,
        request_context,
        session_id,
        run_id,
        tape_seq,
        model_token_tape_events,
        model_token_compaction_emitted,
        summary.as_str(),
    )
    .await
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn send_deferred_final_reply_tokens(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    session_id: &str,
    run_id: &str,
    tape_seq: &mut i64,
    model_token_tape_events: &mut usize,
    model_token_compaction_emitted: &mut bool,
    reply_text: &str,
) -> Result<(), Status> {
    let output = ProviderTurnOutput::text(
        reply_text.to_owned(),
        ProviderFinishReason::Stop,
        ProviderUsage::new(0, 0, "run_stream_final_projection"),
        ProviderRawProviderRefs::default(),
    );
    let mut emitted = false;
    for event in provider_events_from_output(&output) {
        let ProviderEvent::ModelToken { token, is_final } = event else {
            continue;
        };
        emitted = true;
        send_model_token_with_tape(
            sender,
            runtime_state,
            request_context,
            session_id,
            run_id,
            tape_seq,
            model_token_tape_events,
            model_token_compaction_emitted,
            token.as_str(),
            is_final,
        )
        .await?;
    }
    if !emitted {
        send_model_token_with_tape(
            sender,
            runtime_state,
            request_context,
            session_id,
            run_id,
            tape_seq,
            model_token_tape_events,
            model_token_compaction_emitted,
            "",
            true,
        )
        .await?;
    }
    Ok(())
}

#[allow(clippy::result_large_err)]
async fn persist_accepted_final_reply(
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    session_id_for_message: &str,
    run_id: &str,
    tape_seq: &mut i64,
    reply_text: &str,
) -> Result<(), Status> {
    persist_run_stream_reply_text(
        runtime_state,
        request_context,
        session_id_for_message,
        run_id,
        tape_seq,
        reply_text,
    )
    .await?;
    if !reply_text.trim().is_empty() {
        ingest_memory_best_effort(
            runtime_state,
            request_context.principal.as_str(),
            request_context.channel.as_deref(),
            Some(session_id_for_message),
            MemorySource::Summary,
            reply_text,
            vec!["summary:model_output".to_owned()],
            Some(0.75),
            "run_stream_model_summary",
        )
        .await;
    }
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
    tool_result_compaction_emitted: &mut bool,
    in_progress_emitted: &mut bool,
    remaining_tool_budget: &mut u32,
    previous_session_run_id: &mut Option<String>,
    active_background_budget_tokens: &mut Option<u64>,
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
    if let Some(budget_tokens) = background_run_budget_tokens(parameter_delta_json.as_deref()) {
        *active_background_budget_tokens = Some(budget_tokens);
    }
    let background_budget_tokens = *active_background_budget_tokens;
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
    let input_text = input_content.text.clone();
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

    append_run_stream_user_message(
        runtime_state,
        tape_seq,
        RunStreamUserMessage {
            run_id: run_id.as_str(),
            request_context,
            envelope_id: input_envelope.envelope_id.as_ref(),
            input_content: &input_content,
            session_key: message.session_key.as_str(),
            json_mode_requested,
        },
    )
    .await?;

    let provider_snapshot = runtime_state.model_provider_status_snapshot();
    let routing_vision_inputs = build_provider_image_inputs(
        input_content.attachments.as_slice(),
        &runtime_state.config.media,
    )
    .len();
    let session_model_override = runtime_state
        .orchestrator_session_by_id(session_id.clone())
        .await?
        .and_then(|session| session.model_profile_override);
    let routing_decision = plan_usage_routing(UsageRoutingPlanRequest {
        runtime_state,
        request_context,
        run_id: run_id.as_str(),
        session_id: session_id.as_str(),
        parameter_delta_json: parameter_delta_json.as_deref(),
        prompt_text: input_text.as_str(),
        json_mode: json_mode_requested,
        vision_inputs: routing_vision_inputs,
        scope_kind: "session",
        scope_id: session_id_for_message.as_str(),
        task_class: RoutingTaskClass::PrimaryInteractive,
        provider_snapshot: &provider_snapshot,
        model_profile_override: session_model_override.as_deref(),
    })
    .await?;

    let provider_model_override = provider_model_override_for_routing(
        routing_decision.mode.as_str(),
        routing_decision.actual_model_id.as_str(),
    );
    let lease_provider_id = routing_decision.provider_id.clone();
    let lease_provider_kind = routing_decision.provider_kind.clone();
    let lease_credential_id = routing_decision.credential_id.clone();
    let mut first_turn_tool_catalog_snapshot = Some(
        build_and_record_run_stream_tool_catalog_snapshot(
            runtime_state,
            request_context,
            session_id_for_message.as_str(),
            run_id.as_str(),
            lease_provider_kind.as_str(),
            provider_model_override.as_deref().or(Some(routing_decision.actual_model_id.as_str())),
            *remaining_tool_budget,
            tape_seq,
        )
        .await?,
    );
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
            provider_kind_hint: Some(lease_provider_kind.as_str()),
            provider_model_id_hint: provider_model_override
                .as_deref()
                .or(Some(routing_decision.actual_model_id.as_str())),
            tool_catalog_snapshot: first_turn_tool_catalog_snapshot.as_ref(),
            memory_ingest_reason: "run_stream_user_input",
            memory_prompt_failure_mode: MemoryPromptFailureMode::Fail,
            channel_for_log: request_context.channel.as_deref().unwrap_or("n/a"),
        },
    )
    .await?;
    let mut base_provider_request = ProviderRequest::from_input_text(
        prepared_provider_input.provider_input_text,
        json_mode_requested,
        prepared_provider_input.vision_inputs,
        provider_model_override.clone(),
    );
    base_provider_request.user_visible_input_text = Some(input_text.clone());
    base_provider_request.instruction_hash = prepared_provider_input.instruction_hash.clone();
    base_provider_request.context_trace_id = prepared_provider_input.context_trace_id.clone();
    base_provider_request.budget_profile = prepared_provider_input.budget_profile.clone();
    base_provider_request.max_output_tokens = prepared_provider_input.max_output_tokens;
    if !prepared_provider_input.provider_messages.is_empty() {
        let mut messages = prepared_provider_input.provider_messages.clone();
        messages.push(ProviderMessage::user_text(base_provider_request.input_text.clone()));
        base_provider_request.messages = messages;
    }
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
    send_agent_loop_progress_status(
        sender,
        runtime_state,
        run_id.as_str(),
        tape_seq,
        "agent_loop.started",
    )
    .await?;
    let mut length_recovery_attempts = 0u8;
    let mut final_answer_recovery_attempted = false;
    let mut repeated_tool_failure_tracker = RepeatedToolFailureTracker::default();

    loop {
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
        let _turn_id = match loop_state.start_model_turn() {
            Ok(turn_id) => turn_id,
            Err(reason) => {
                let message =
                    agent_loop_budget_exhausted_message(reason, &loop_state, run_id.as_str());
                send_budget_exhausted_partial_summary_tokens(
                    sender,
                    runtime_state,
                    request_context,
                    session_id_for_message.as_str(),
                    run_id.as_str(),
                    tape_seq,
                    model_token_tape_events,
                    model_token_compaction_emitted,
                    reason,
                    &loop_state,
                    message.as_str(),
                )
                .await?;
                terminate_run_stream_with_agent_loop_reason(
                    sender,
                    runtime_state,
                    run_state,
                    run_id.as_str(),
                    tape_seq,
                    &loop_state,
                    reason,
                    message.as_str(),
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
        send_agent_loop_progress_status(
            sender,
            runtime_state,
            run_id.as_str(),
            tape_seq,
            "agent_loop.turn_started",
        )
        .await?;

        let tool_catalog_snapshot = if let Some(snapshot) = first_turn_tool_catalog_snapshot.take()
        {
            snapshot
        } else {
            build_and_record_run_stream_tool_catalog_snapshot(
                runtime_state,
                request_context,
                session_id_for_message.as_str(),
                run_id.as_str(),
                lease_provider_kind.as_str(),
                provider_model_override
                    .as_deref()
                    .or(Some(routing_decision.actual_model_id.as_str())),
                loop_state.remaining_tool_calls(),
                tape_seq,
            )
            .await?
        };
        let mut provider_request = ProviderRequest::from_input_text(
            base_provider_request.input_text.clone(),
            base_provider_request.json_mode,
            base_provider_request.vision_inputs.clone(),
            base_provider_request.model_override.clone(),
        );
        provider_request.user_visible_input_text =
            base_provider_request.user_visible_input_text.clone();
        provider_request.messages = loop_state.messages();
        provider_request.tool_catalog_snapshot =
            Some(snapshot_to_provider_request_value(&tool_catalog_snapshot));
        provider_request.instruction_hash = base_provider_request.instruction_hash.clone();
        provider_request.context_trace_id = base_provider_request.context_trace_id.clone();
        provider_request.budget_profile = base_provider_request.budget_profile.clone();
        provider_request.max_output_tokens = base_provider_request.max_output_tokens;
        if let Some(budget_tokens) = background_budget_tokens {
            let consumed_tokens = loop_state.snapshot(run_id.as_str(), None).usage.total_tokens;
            match apply_background_budget_guard(
                &mut provider_request,
                budget_tokens,
                consumed_tokens,
            ) {
                Ok(decision) => {
                    append_agent_loop_tape_event(
                        runtime_state,
                        run_id.as_str(),
                        tape_seq,
                        "agent_loop.background_budget_guard",
                        decision.tape_payload(),
                    )
                    .await?;
                }
                Err(message) => {
                    terminate_run_stream_with_agent_loop_reason(
                        sender,
                        runtime_state,
                        run_state,
                        run_id.as_str(),
                        tape_seq,
                        &loop_state,
                        AgentLoopTerminationReason::ContextBudgetExhausted,
                        message.as_str(),
                        None,
                    )
                    .await?;
                    return Ok(RunStreamMessageProcessingOutcome::Terminate);
                }
            }
        }
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
                if loop_state.completed_tool_calls() > 0 {
                    let fallback_summary = if final_answer_recovery_attempted {
                        final_answer_recovery_fallback_summary(
                            error.message(),
                            &loop_state,
                            run_id.as_str(),
                        )
                    } else {
                        provider_error_partial_summary(
                            error.message(),
                            &loop_state,
                            run_id.as_str(),
                        )
                    };
                    send_deferred_final_reply_tokens(
                        sender,
                        runtime_state,
                        request_context,
                        session_id_for_message.as_str(),
                        run_id.as_str(),
                        tape_seq,
                        model_token_tape_events,
                        model_token_compaction_emitted,
                        fallback_summary.as_str(),
                    )
                    .await?;
                    terminate_run_stream_with_agent_loop_reason(
                        sender,
                        runtime_state,
                        run_state,
                        run_id.as_str(),
                        tape_seq,
                        &loop_state,
                        AgentLoopTerminationReason::ProviderError,
                        fallback_summary.as_str(),
                        None,
                    )
                    .await?;
                    return Ok(RunStreamMessageProcessingOutcome::Terminate);
                }
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
        if let Some(budget_tokens) = background_budget_tokens {
            let consumed_tokens = loop_state.snapshot(run_id.as_str(), None).usage.total_tokens;
            if let Some(message) = background_budget_overrun_message(budget_tokens, consumed_tokens)
            {
                record_run_stream_provider_usage(
                    runtime_state,
                    run_id.as_str(),
                    &provider_response,
                )
                .await?;
                terminate_run_stream_with_agent_loop_reason(
                    sender,
                    runtime_state,
                    run_state,
                    run_id.as_str(),
                    tape_seq,
                    &loop_state,
                    AgentLoopTerminationReason::ContextBudgetExhausted,
                    message.as_str(),
                    provider_response.output.raw_provider_refs.provider_trace_ref.clone(),
                )
                .await?;
                return Ok(RunStreamMessageProcessingOutcome::Terminate);
            }
        }
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
            provider_response,
            &tool_catalog_snapshot,
            remaining_tool_budget,
            message.allow_sensitive_tools,
            tape_seq,
            model_token_tape_events,
            model_token_compaction_emitted,
        )
        .await?;
        loop_state.sync_remaining_tool_calls(*remaining_tool_budget);

        match response_outcome {
            RunStreamProviderResponseOutcome::Completed {
                tool_result_messages,
                provider_trace_ref,
                final_reply_text,
                final_provider_output,
                final_reply_tokens_deferred,
            } => {
                loop_state.append_assistant_turn(&provider_output);
                let should_refeed_tool_results = !tool_result_messages.is_empty()
                    && provider_output.finish_reason == ProviderFinishReason::ToolCalls;
                if !should_refeed_tool_results {
                    if let Some(message) =
                        incomplete_terminal_final_answer(final_reply_text.as_deref(), &loop_state)
                    {
                        if let Some(recovery_prompt) = final_answer_recovery_prompt(
                            message.as_str(),
                            &loop_state,
                            final_answer_recovery_attempted,
                        ) {
                            final_answer_recovery_attempted = true;
                            loop_state.append_user_guidance(recovery_prompt);
                            append_agent_loop_tape_event(
                                runtime_state,
                                run_id.as_str(),
                                tape_seq,
                                "agent_loop.final_answer_recovery_requested",
                                loop_state.turn_payload(
                                    run_id.as_str(),
                                    "agent_loop.final_answer_recovery_requested",
                                ),
                            )
                            .await?;
                            send_agent_loop_progress_status(
                                sender,
                                runtime_state,
                                run_id.as_str(),
                                tape_seq,
                                "agent_loop.final_answer_recovery_requested",
                            )
                            .await?;
                            continue;
                        }
                        if loop_state.completed_tool_calls() > 0 {
                            let fallback_summary = final_answer_recovery_fallback_summary(
                                message.as_str(),
                                &loop_state,
                                run_id.as_str(),
                            );
                            append_agent_loop_tape_event(
                                runtime_state,
                                run_id.as_str(),
                                tape_seq,
                                "agent_loop.final_answer_fallback_used",
                                loop_state.turn_payload(
                                    run_id.as_str(),
                                    "agent_loop.final_answer_fallback_used",
                                ),
                            )
                            .await?;
                            send_agent_loop_progress_status(
                                sender,
                                runtime_state,
                                run_id.as_str(),
                                tape_seq,
                                "agent_loop.final_answer_fallback_used",
                            )
                            .await?;
                            send_deferred_final_reply_tokens(
                                sender,
                                runtime_state,
                                request_context,
                                session_id_for_message.as_str(),
                                run_id.as_str(),
                                tape_seq,
                                model_token_tape_events,
                                model_token_compaction_emitted,
                                fallback_summary.as_str(),
                            )
                            .await?;
                            persist_accepted_final_reply(
                                runtime_state,
                                request_context,
                                session_id_for_message.as_str(),
                                run_id.as_str(),
                                tape_seq,
                                fallback_summary.as_str(),
                            )
                            .await?;
                            append_agent_loop_tape_event(
                                runtime_state,
                                run_id.as_str(),
                                tape_seq,
                                "agent_loop.terminated",
                                loop_state.termination_payload(
                                    run_id.as_str(),
                                    AgentLoopTerminationReason::FinalAnswer,
                                    fallback_summary.as_str(),
                                    provider_trace_ref,
                                ),
                            )
                            .await?;
                            send_terminal_agent_loop_progress_status(
                                sender,
                                runtime_state,
                                run_id.as_str(),
                                tape_seq,
                                "agent_loop.terminated",
                            )
                            .await?;
                            return Ok(RunStreamMessageProcessingOutcome::Continue);
                        }
                        terminate_run_stream_with_agent_loop_reason(
                            sender,
                            runtime_state,
                            run_state,
                            run_id.as_str(),
                            tape_seq,
                            &loop_state,
                            AgentLoopTerminationReason::IncompleteFinalAnswer,
                            message.as_str(),
                            provider_trace_ref,
                        )
                        .await?;
                        return Ok(RunStreamMessageProcessingOutcome::Terminate);
                    }
                    if let Some(reply_text) = final_reply_text.as_deref() {
                        if final_reply_tokens_deferred {
                            send_deferred_final_reply_tokens(
                                sender,
                                runtime_state,
                                request_context,
                                session_id_for_message.as_str(),
                                run_id.as_str(),
                                tape_seq,
                                model_token_tape_events,
                                model_token_compaction_emitted,
                                reply_text,
                            )
                            .await?;
                        }
                        if let Some(provider_output) = final_provider_output.as_ref() {
                            persist_run_stream_provider_turn_output(
                                runtime_state,
                                run_id.as_str(),
                                tape_seq,
                                provider_output,
                            )
                            .await?;
                        }
                        persist_accepted_final_reply(
                            runtime_state,
                            request_context,
                            session_id_for_message.as_str(),
                            run_id.as_str(),
                            tape_seq,
                            reply_text,
                        )
                        .await?;
                    }
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
                    send_terminal_agent_loop_progress_status(
                        sender,
                        runtime_state,
                        run_id.as_str(),
                        tape_seq,
                        "agent_loop.terminated",
                    )
                    .await?;
                    return Ok(RunStreamMessageProcessingOutcome::Continue);
                }

                let repeated_tool_failure =
                    repeated_tool_failure_tracker.observe(tool_result_messages.as_slice());
                let tool_result_count = tool_result_messages.len();
                loop_state.append_tool_result_messages(tool_result_messages);
                if let Some(failure) = repeated_tool_failure {
                    terminate_run_stream_with_agent_loop_reason(
                        sender,
                        runtime_state,
                        run_state,
                        run_id.as_str(),
                        tape_seq,
                        &loop_state,
                        AgentLoopTerminationReason::RepeatedToolFailure,
                        failure.message.as_str(),
                        provider_trace_ref,
                    )
                    .await?;
                    return Ok(RunStreamMessageProcessingOutcome::Terminate);
                }
                if should_terminate_after_tool_budget_exhausted(&loop_state, tool_result_count) {
                    let message = agent_loop_budget_exhausted_message(
                        AgentLoopTerminationReason::MaxToolCalls,
                        &loop_state,
                        run_id.as_str(),
                    );
                    send_budget_exhausted_partial_summary_tokens(
                        sender,
                        runtime_state,
                        request_context,
                        session_id_for_message.as_str(),
                        run_id.as_str(),
                        tape_seq,
                        model_token_tape_events,
                        model_token_compaction_emitted,
                        AgentLoopTerminationReason::MaxToolCalls,
                        &loop_state,
                        message.as_str(),
                    )
                    .await?;
                    terminate_run_stream_with_agent_loop_reason(
                        sender,
                        runtime_state,
                        run_state,
                        run_id.as_str(),
                        tape_seq,
                        &loop_state,
                        AgentLoopTerminationReason::MaxToolCalls,
                        message.as_str(),
                        provider_trace_ref,
                    )
                    .await?;
                    return Ok(RunStreamMessageProcessingOutcome::Terminate);
                }
                let compaction_will_run = tool_result_count > 0 && !*tool_result_compaction_emitted;
                if compaction_will_run {
                    send_agent_loop_progress_status(
                        sender,
                        runtime_state,
                        run_id.as_str(),
                        tape_seq,
                        "session.compaction.tool_results.started",
                    )
                    .await?;
                }
                let compaction_outcome = maybe_compact_context_after_tool_results(
                    runtime_state,
                    request_context,
                    session_id.as_str(),
                    run_id.as_str(),
                    tape_seq,
                    tool_result_count,
                    tool_result_compaction_emitted,
                )
                .await?;
                if let Some(phase) = compaction_outcome.progress_phase() {
                    send_agent_loop_progress_status(
                        sender,
                        runtime_state,
                        run_id.as_str(),
                        tape_seq,
                        phase,
                    )
                    .await?;
                }
                append_agent_loop_tape_event(
                    runtime_state,
                    run_id.as_str(),
                    tape_seq,
                    "agent_loop.turn_completed",
                    loop_state.turn_payload(run_id.as_str(), "agent_loop.turn_completed"),
                )
                .await?;
                send_agent_loop_progress_status(
                    sender,
                    runtime_state,
                    run_id.as_str(),
                    tape_seq,
                    "agent_loop.turn_completed",
                )
                .await?;
            }
            RunStreamProviderResponseOutcome::Failed { message, provider_trace_ref, reason } => {
                if let Some(recovery_prompt) = length_recovery_prompt(
                    reason,
                    message.as_str(),
                    &loop_state,
                    length_recovery_attempts,
                ) {
                    length_recovery_attempts = length_recovery_attempts.saturating_add(1);
                    loop_state.append_user_guidance(recovery_prompt);
                    append_agent_loop_tape_event(
                        runtime_state,
                        run_id.as_str(),
                        tape_seq,
                        "agent_loop.length_recovery_requested",
                        loop_state
                            .turn_payload(run_id.as_str(), "agent_loop.length_recovery_requested"),
                    )
                    .await?;
                    send_agent_loop_progress_status(
                        sender,
                        runtime_state,
                        run_id.as_str(),
                        tape_seq,
                        "agent_loop.length_recovery_requested",
                    )
                    .await?;
                    continue;
                }
                loop_state.append_assistant_turn(&provider_output);
                terminate_run_stream_with_agent_loop_reason(
                    sender,
                    runtime_state,
                    run_state,
                    run_id.as_str(),
                    tape_seq,
                    &loop_state,
                    reason,
                    message.as_str(),
                    provider_trace_ref,
                )
                .await?;
                return Ok(RunStreamMessageProcessingOutcome::Terminate);
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
    provider_response: ProviderResponse,
    tool_catalog_snapshot: &ModelVisibleToolCatalogSnapshot,
    remaining_tool_budget: &mut u32,
    allow_sensitive_tools: bool,
    tape_seq: &mut i64,
    model_token_tape_events: &mut usize,
    model_token_compaction_emitted: &mut bool,
) -> Result<RunStreamProviderResponseOutcome, Status> {
    let provider_output = bounded_provider_turn_output_for_persistence(&provider_response.output);
    let stream_model_tokens_immediately = provider_response
        .events
        .iter()
        .any(|event| matches!(event, ProviderEvent::ToolProposal { .. }));
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
        allow_sensitive_tools,
        tape_seq,
        model_token_tape_events,
        model_token_compaction_emitted,
        stream_model_tokens_immediately,
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
    if stream_model_tokens_immediately {
        persist_run_stream_provider_turn_output(runtime_state, run_id, tape_seq, &provider_output)
            .await?;
    }
    let terminal_tool_failure = tool_results.iter().find_map(terminal_tool_authorization_failure);
    let tool_result_messages = if terminal_tool_failure.is_some() {
        Vec::new()
    } else {
        tool_results.iter().map(tool_result_to_provider_message).collect::<Result<Vec<_>, _>>()?
    };
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

    if let Some(message) = terminal_tool_failure {
        return Ok(RunStreamProviderResponseOutcome::Failed {
            message,
            provider_trace_ref: provider_output.raw_provider_refs.provider_trace_ref.clone(),
            reason: AgentLoopTerminationReason::ApprovalDenied,
        });
    }

    if !has_pending_tool_results {
        if let Some(message) = tool_calls_finish_without_tool_payload(&provider_output) {
            return Ok(RunStreamProviderResponseOutcome::Failed {
                message,
                provider_trace_ref: provider_output.raw_provider_refs.provider_trace_ref.clone(),
                reason: AgentLoopTerminationReason::ProviderError,
            });
        }
        if let Some(message) = truncated_final_answer_without_tools(&provider_output) {
            return Ok(RunStreamProviderResponseOutcome::Failed {
                message,
                provider_trace_ref: provider_output.raw_provider_refs.provider_trace_ref.clone(),
                reason: AgentLoopTerminationReason::IncompleteFinalAnswer,
            });
        }
    }

    if !has_pending_tool_results && contains_raw_provider_tool_call_markup(reply_text.as_str()) {
        return Ok(RunStreamProviderResponseOutcome::Failed {
            message:
                "model provider returned raw tool-call markup instead of a structured tool proposal"
                    .to_owned(),
            provider_trace_ref: provider_output.raw_provider_refs.provider_trace_ref.clone(),
            reason: AgentLoopTerminationReason::ProviderError,
        });
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
        final_provider_output: (!has_pending_tool_results && !stream_model_tokens_immediately)
            .then_some(Box::new(provider_output)),
        final_reply_tokens_deferred: !has_pending_tool_results && !stream_model_tokens_immediately,
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
        let mut content = json!({
            "success": result.outcome.success,
            "tool_name": result.tool_name.as_str(),
            "error": result.outcome.error.as_str(),
            "output": output,
        });
        if let Some(claim_boundary) = failed_tool_claim_boundary(result.tool_name.as_str()) {
            content["diagnostic_status"] = json!("unknown");
            content["claim_boundary"] = json!(claim_boundary);
        }
        content
    };
    let serialized = serde_json::to_string(&content).map_err(|error| {
        Status::internal(format!("failed to serialize model-visible tool result: {error}"))
    })?;
    let redacted = crate::journal::redact_payload_json(serialized.as_bytes()).unwrap_or(serialized);
    Ok(ProviderMessage::tool_result(result.proposal_id.clone(), redacted))
}

fn failed_tool_claim_boundary(tool_name: &str) -> Option<&'static str> {
    match tool_name {
        crate::gateway::BROWSER_CONSOLE_LOG_TOOL_NAME => Some(
            "browser console diagnostics failed; console status is unknown, so do not claim the page has no console errors or that the console is clean unless a later successful console diagnostic verifies it",
        ),
        crate::gateway::MEMORY_RETAIN_TOOL_NAME | crate::gateway::MEMORY_RETAIN_ALIAS_TOOL_NAME => Some(
            "memory retain did not complete as a durable write; do not claim the memory was stored or will be available for future recall unless a later successful retain or ingest verifies it",
        ),
        _ => None,
    }
}

const REPEATED_TOOL_FAILURE_LIMIT: u32 = 3;

#[derive(Debug, Clone, Default)]
struct RepeatedToolFailureTracker {
    last_signature: Option<RepeatedToolFailureSignature>,
    repeated_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RepeatedToolFailureSignature {
    tool_name: String,
    failure_kind: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RepeatedToolFailure {
    message: String,
}

impl RepeatedToolFailureTracker {
    fn observe(&mut self, tool_result_messages: &[ProviderMessage]) -> Option<RepeatedToolFailure> {
        let mut termination = None;
        for message in tool_result_messages {
            let Some(signature) = repeated_tool_failure_signature(message) else {
                continue;
            };
            if self.last_signature.as_ref() == Some(&signature) {
                self.repeated_count = self.repeated_count.saturating_add(1);
            } else {
                self.last_signature = Some(signature.clone());
                self.repeated_count = 1;
            }
            if self.repeated_count >= REPEATED_TOOL_FAILURE_LIMIT {
                termination = Some(RepeatedToolFailure {
                    message: repeated_tool_failure_message(&signature, self.repeated_count),
                });
            }
        }
        termination
    }
}

fn repeated_tool_failure_signature(
    message: &ProviderMessage,
) -> Option<RepeatedToolFailureSignature> {
    if message.role != crate::model_provider::ProviderMessageRole::Tool {
        return None;
    }
    let value = serde_json::from_str::<Value>(message.text_content().as_str()).ok()?;
    if value.get("success").and_then(Value::as_bool).unwrap_or(true) {
        return None;
    }
    let tool_name = value.get("tool_name").and_then(Value::as_str)?;
    if tool_name != crate::gateway::WORKSPACE_PATCH_TOOL_NAME {
        return None;
    }
    let error = value.get("error").and_then(Value::as_str).unwrap_or_default();
    let output = value.get("output").unwrap_or(&value);
    let has_parse_error =
        output.get("parse_error").is_some_and(|parse_error| !parse_error.is_null())
            || error.to_ascii_lowercase().contains("patch parse error");
    if !has_parse_error {
        return None;
    }
    let error_kind = normalize_repeated_tool_failure_kind(error);
    let recovery_hint = output
        .get("recovery_hint")
        .and_then(Value::as_str)
        .map(normalize_repeated_tool_failure_kind)
        .filter(|value| !value.is_empty())
        .unwrap_or_default();
    let failure_kind = if error_kind.starts_with("workspace_patch_parse.") {
        error_kind
    } else if !recovery_hint.is_empty() {
        recovery_hint
    } else {
        error_kind
    };
    Some(RepeatedToolFailureSignature { tool_name: tool_name.to_owned(), failure_kind })
}

fn normalize_repeated_tool_failure_kind(value: &str) -> String {
    let normalized = value.to_ascii_lowercase();
    if normalized.contains("expected '*** end patch'") {
        return "workspace_patch_parse.expected_end_patch".to_owned();
    }
    if normalized.contains("expected '*** begin patch'") {
        return "workspace_patch_parse.expected_begin_patch".to_owned();
    }
    if normalized.contains("unexpected content after '*** end patch'") {
        return "workspace_patch_parse.trailing_content".to_owned();
    }
    if normalized.contains("complete patch") || normalized.contains("patch parse error") {
        return "workspace_patch_parse.incomplete_or_malformed_patch".to_owned();
    }
    truncate_with_ellipsis(normalized.split_whitespace().collect::<Vec<_>>().join(" "), 160)
}

fn repeated_tool_failure_message(
    signature: &RepeatedToolFailureSignature,
    repeated_count: u32,
) -> String {
    format!(
        "agent loop stopped after {repeated_count} repeated malformed {tool} calls ({kind}). The patch was not applied. Read the current file before retrying and send one complete patch that starts with '*** Begin Patch' and ends with '*** End Patch'; if a valid artifact already exists, preserve it instead of deleting or replacing it.",
        tool = signature.tool_name,
        kind = signature.failure_kind,
    )
}

fn terminal_tool_authorization_failure(result: &RunStreamToolResultForModel) -> Option<String> {
    let error = result.outcome.error.trim();
    if result.outcome.success || error.is_empty() || !is_terminal_tool_authorization_error(error) {
        return None;
    }

    if is_noninteractive_cli_approval_denial(error) {
        return Some(format!(
            "tool execution requires approval, but the noninteractive CLI cannot prompt for it: tool={} proposal_id={} error={}. Rerun in an interactive terminal, use --approval-mode allow-once for per-request approval, or use --allow-sensitive-tools only after reviewing the requested tool risk.",
            result.tool_name,
            result.proposal_id,
            truncate_with_ellipsis(error.to_owned(), 512)
        ));
    }
    if is_cli_approval_mode_deny(error) {
        return Some(format!(
            "tool execution was blocked by --approval-mode deny: tool={} proposal_id={} error={}. No approval prompt is pending and the denied tool action was not executed. Rerun in an interactive terminal, use --approval-mode allow-once for per-request approval, or use --allow-sensitive-tools only after reviewing the requested tool risk.",
            result.tool_name,
            result.proposal_id,
            truncate_with_ellipsis(error.to_owned(), 512)
        ));
    }

    Some(format!(
        "tool execution blocked by approval or policy: tool={} proposal_id={} error={}",
        result.tool_name,
        result.proposal_id,
        truncate_with_ellipsis(error.to_owned(), 512)
    ))
}

fn is_terminal_tool_authorization_error(error: &str) -> bool {
    let normalized = error.to_ascii_lowercase();
    TERMINAL_TOOL_AUTHORIZATION_ERROR_MARKERS.iter().any(|marker| normalized.contains(marker))
}

fn is_noninteractive_cli_approval_denial(error: &str) -> bool {
    error.to_ascii_lowercase().contains("approval_required_non_interactive_cli")
}

fn is_cli_approval_mode_deny(error: &str) -> bool {
    error.to_ascii_lowercase().contains("denied_by_cli_approval_mode_deny")
}

fn contains_raw_provider_tool_call_markup(text: &str) -> bool {
    let normalized = text.to_ascii_lowercase();
    normalized.contains("<minimax:tool_call")
        || (normalized.contains("<tool_call") && normalized.contains("<invoke name="))
}

fn truncated_final_answer_without_tools(output: &ProviderTurnOutput) -> Option<String> {
    matches!(output.finish_reason, ProviderFinishReason::Length).then(|| {
        "model provider stopped because of an output token limit before returning a complete final answer or structured tool call (finish_reason=length)"
            .to_owned()
    })
}

fn tool_calls_finish_without_tool_payload(output: &ProviderTurnOutput) -> Option<String> {
    let provider_requested_tool_call =
        matches!(output.finish_reason, ProviderFinishReason::ToolCalls);
    let has_structured_tool_call = output
        .content_parts
        .iter()
        .any(|part| matches!(part, ProviderOutputContentPart::ToolCall { .. }));
    (provider_requested_tool_call && !has_structured_tool_call).then(|| {
        "model provider reported finish_reason=tool_calls without a structured tool call payload"
            .to_owned()
    })
}

fn agent_loop_budget_exhausted_message(
    reason: AgentLoopTerminationReason,
    loop_state: &AgentRunLoopState,
    run_id: &str,
) -> String {
    let snapshot = loop_state.snapshot(run_id, Some(reason));
    let base = match reason {
        AgentLoopTerminationReason::MaxTurns => "agent loop model turn limit reached",
        AgentLoopTerminationReason::MaxToolCalls => "agent loop tool call limit reached",
        AgentLoopTerminationReason::WallClock => "agent loop wall-clock budget exhausted",
        _ => "agent loop budget exhausted",
    };
    let tool_result_label =
        if snapshot.completed_tool_calls == 1 { "tool result" } else { "tool results" };
    let recovery_hint = match reason {
        AgentLoopTerminationReason::MaxToolCalls => {
            "Increase tool_call.max_calls_per_run only if the workflow genuinely needs more tool steps, or continue in the same session with a narrower resume prompt."
        }
        AgentLoopTerminationReason::WallClock => {
            "The tool-call limit was not the terminal condition; continue in the same session with a narrower resume prompt or increase the agent loop wall-clock budget for long process/browser workflows."
        }
        AgentLoopTerminationReason::MaxTurns => {
            "Continue in the same session with a narrower resume prompt or increase the model-turn budget for unusually long reasoning workflows."
        }
        _ => "Continue in the same session with a narrower resume prompt.",
    };
    format!(
        "{base} after {} model turns and {} {tool_result_label}; partial result summary: run tape for {run_id} contains the exact tool evidence, remaining_model_turns={}, remaining_tool_calls={}, elapsed_ms={}. Continue in the same session and ask to resume from run {run_id}. {recovery_hint}",
        snapshot.current_turn,
        snapshot.completed_tool_calls,
        snapshot.remaining_model_turns,
        snapshot.remaining_tool_calls,
        snapshot.elapsed_ms
    )
}

fn should_emit_budget_exhausted_partial_summary(
    reason: AgentLoopTerminationReason,
    loop_state: &AgentRunLoopState,
) -> bool {
    matches!(
        reason,
        AgentLoopTerminationReason::MaxTurns
            | AgentLoopTerminationReason::MaxToolCalls
            | AgentLoopTerminationReason::WallClock
    ) && loop_state.completed_tool_calls() > 0
}

fn should_terminate_after_tool_budget_exhausted(
    loop_state: &AgentRunLoopState,
    tool_result_count: usize,
) -> bool {
    tool_result_count > 0 && loop_state.remaining_tool_calls() == 0
}

fn length_recovery_prompt(
    reason: AgentLoopTerminationReason,
    message: &str,
    loop_state: &AgentRunLoopState,
    attempt_count: u8,
) -> Option<&'static str> {
    if attempt_count >= MAX_LENGTH_RECOVERY_ATTEMPTS
        || reason != AgentLoopTerminationReason::IncompleteFinalAnswer
        || loop_state.remaining_model_turns() == 0
        || !message.contains("finish_reason=length")
    {
        return None;
    }
    Some(match attempt_count {
        0 => {
            "The previous assistant output hit the provider output limit before a complete final answer or structured tool call. Continue now with no more explanatory prose. If the user requested files, code, tests, browser validation, research, or diagnostics, issue one concise tool call next using the available tool schema. Prefer palyra.fs.apply_patch for file writes and keep arguments minimal. If no tool is needed, answer in at most 120 words and do not claim unverified work."
        }
        1 => {
            "The previous length recovery also hit the provider output limit. Do not explain or restate prior work. Issue exactly one small structured tool call now, preferably palyra.fs.apply_patch with a single file or hunk. If a final answer is unavoidable, keep it under 60 words and mark any unfinished work as partial."
        }
        _ => {
            "Last length-recovery attempt. Produce only one minimal structured tool call with the smallest useful arguments. Do not include prose, file contents, markdown previews, or summaries before the tool call."
        }
    })
}

fn final_answer_recovery_prompt(
    message: &str,
    loop_state: &AgentRunLoopState,
    already_attempted: bool,
) -> Option<&'static str> {
    if already_attempted
        || loop_state.completed_tool_calls() == 0
        || loop_state.remaining_model_turns() == 0
    {
        return None;
    }

    let normalized = message.to_ascii_lowercase();
    if !(normalized.contains("empty final answer after tool execution")
        || normalized.contains("bare acknowledgement instead of a final answer")
        || normalized.contains("planning or intent statement")
        || normalized.contains("without matching tool evidence"))
    {
        return None;
    }

    Some(
        "The previous assistant turn did not provide a usable final answer after tool execution. Continue now using the existing tool evidence. If the requested work is complete, answer with a concise summary that lists changed files, validation results, and any unresolved partial state. If work is incomplete, issue the next minimal tool call needed to inspect, finish, validate, or clean up. Do not claim PASS or completion without direct successful tool evidence.",
    )
}

fn final_answer_recovery_fallback_summary(
    message: &str,
    loop_state: &AgentRunLoopState,
    run_id: &str,
) -> String {
    let tool_count = loop_state.completed_tool_calls();
    let tool_label = if tool_count == 1 { "tool call" } else { "tool calls" };
    format!(
        "Partial result: I ran {tool_count} {tool_label}, but the model did not produce a usable final answer after recovery. Last recovery issue: {}. The run tape for {run_id} contains the exact tool evidence. Resume this same session and reference run {run_id} if any requested artifact, validation, or cleanup is still missing.",
        truncate_with_ellipsis(message.trim().replace(['\r', '\n'], " "), 512)
    )
}

fn provider_error_partial_summary(
    message: &str,
    loop_state: &AgentRunLoopState,
    run_id: &str,
) -> String {
    let tool_count = loop_state.completed_tool_calls();
    let tool_label = if tool_count == 1 { "tool call" } else { "tool calls" };
    format!(
        "Partial result: I ran {tool_count} {tool_label}, but the next model provider turn failed before producing a final answer. Provider issue: {}. The run tape for {run_id} contains the exact tool evidence. Resume this same session and reference run {run_id} if any requested artifact, validation, or cleanup is still missing.",
        truncate_with_ellipsis(message.trim().replace(['\r', '\n'], " "), 512)
    )
}

fn incomplete_final_answer_without_tools(
    text: Option<&str>,
    messages: &[ProviderMessage],
) -> Option<String> {
    let text = text.unwrap_or_default().trim();
    if text.is_empty() {
        return Some(
            "model returned an empty final answer without executing any requested tools".to_owned(),
        );
    }
    if final_answer_is_minimal_ack(text) && !user_requested_exact_minimal_answer(text, messages) {
        return Some("model returned a bare acknowledgement as the final answer".to_owned());
    }
    if final_answer_is_deferred_tool_work(text) {
        return Some(
            "model returned a planning or intent statement as the final answer without executing any tools"
                .to_owned(),
        );
    }
    if final_answer_claims_tool_work_without_evidence(text) {
        return Some(
            "model claimed file, process, browser, or verification work without any successful tool results"
                .to_owned(),
        );
    }
    None
}

fn incomplete_terminal_final_answer(
    text: Option<&str>,
    loop_state: &AgentRunLoopState,
) -> Option<String> {
    let messages = loop_state.messages();
    if loop_state.completed_tool_calls() == 0 {
        return incomplete_final_answer_without_tools(text, messages.as_slice());
    }

    let text = text.unwrap_or_default().trim();
    if text.is_empty() {
        return Some("model returned an empty final answer after tool execution".to_owned());
    }

    if final_answer_is_minimal_ack(text)
        && !user_requested_exact_minimal_answer(text, messages.as_slice())
    {
        return Some(
            "model returned a bare acknowledgement instead of a final answer with tool evidence"
                .to_owned(),
        );
    }
    if final_answer_is_deferred_tool_work(text) {
        return Some(
            "model returned a planning or intent statement as the final answer after tool execution"
                .to_owned(),
        );
    }
    if final_answer_claims_tool_work_without_evidence(text)
        && !final_answer_has_matching_tool_evidence(text, messages.as_slice())
    {
        return Some(
            "model claimed file, process, browser, or verification work without matching tool evidence"
                .to_owned(),
        );
    }
    None
}

fn final_answer_is_deferred_tool_work(text: &str) -> bool {
    let normalized = normalize_final_answer_text(text);
    if DEFERRED_TOOL_WORK_NEGATED_MARKERS.iter().any(|marker| normalized.contains(marker)) {
        return false;
    }

    DEFERRED_TOOL_WORK_MARKERS.iter().any(|marker| normalized.contains(marker))
        && TOOL_WORK_ACTION_MARKERS
            .iter()
            .any(|marker| normalized_text_has_marker(normalized.as_str(), marker))
}

fn final_answer_claims_tool_work_without_evidence(text: &str) -> bool {
    let normalized = normalize_final_answer_text(text);
    UNSUPPORTED_TOOL_WORK_CLAIMS.iter().any(|marker| normalized.contains(marker))
}

fn normalized_text_has_marker(normalized: &str, marker: &str) -> bool {
    normalized
        .split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
        .any(|token| token == marker)
}

fn normalize_final_answer_text(text: &str) -> String {
    text.to_ascii_lowercase()
        .replace(['\u{2018}', '\u{2019}'], "'")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn final_answer_is_minimal_ack(text: &str) -> bool {
    let normalized = normalize_final_answer_text(text);
    matches!(normalized.as_str(), "ack" | "ok" | "okay" | "done" | "complete" | "completed")
        || is_ack_sentinel_token(normalized.as_str())
}

fn is_ack_sentinel_token(normalized: &str) -> bool {
    if normalized.len() > 64 || normalized.split_whitespace().count() != 1 {
        return false;
    }
    let Some((prefix, suffix)) = normalized.split_once('-') else {
        return false;
    };
    matches!(prefix, "ack" | "ok")
        && !suffix.is_empty()
        && suffix.chars().all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_'))
}

fn user_requested_exact_minimal_answer(answer: &str, messages: &[ProviderMessage]) -> bool {
    let normalized_answer = normalize_final_answer_text(answer);
    if !final_answer_is_minimal_ack(normalized_answer.as_str()) {
        return false;
    }

    current_user_message_for_exact_answer(messages).is_some_and(|message| {
        user_message_requests_exact_answer(
            message.text_content().as_str(),
            normalized_answer.as_str(),
        )
    })
}

fn current_user_message_for_exact_answer(messages: &[ProviderMessage]) -> Option<&ProviderMessage> {
    messages
        .iter()
        .take_while(|message| {
            !matches!(message.role, ProviderMessageRole::Assistant | ProviderMessageRole::Tool)
        })
        .filter(|message| message.role == ProviderMessageRole::User)
        .last()
}

fn user_message_requests_exact_answer(text: &str, normalized_answer: &str) -> bool {
    let normalized = normalize_final_answer_text(text);
    const EXACT_ANSWER_MARKERS: &[&str] = &[
        "acknowledge exactly",
        "answer exactly",
        "output exactly",
        "print exactly",
        "reply exactly",
        "respond exactly",
        "return exactly",
        "say exactly",
    ];

    if EXACT_ANSWER_MARKERS.iter().any(|marker| {
        let Some(marker_index) = normalized.find(marker) else {
            return false;
        };
        if exact_answer_marker_is_negated(normalized[..marker_index].trim_end()) {
            return false;
        }
        let requested = normalized[marker_index + marker.len()..]
            .trim_start_matches(|character: char| {
                character.is_whitespace()
                    || matches!(character, ':' | '-' | '"' | '\'' | '`' | '\u{201c}' | '\u{201d}')
            })
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .trim_matches(trim_exact_answer_token_character);
        requested == normalized_answer
    }) {
        return true;
    }

    user_message_requests_reply_only_answer(normalized.as_str(), normalized_answer)
}

fn user_message_requests_reply_only_answer(normalized: &str, normalized_answer: &str) -> bool {
    const REPLY_ONLY_MARKERS: &[&str] =
        &["reply", "respond", "answer", "return", "print", "output", "say"];

    for marker in REPLY_ONLY_MARKERS {
        let Some(marker_index) = normalized.find(marker) else {
            continue;
        };
        if exact_answer_marker_is_negated(normalized[..marker_index].trim_end()) {
            continue;
        }
        let mut tokens = normalized[marker_index + marker.len()..]
            .trim_start_matches(|character: char| {
                character.is_whitespace()
                    || matches!(character, ':' | '-' | '"' | '\'' | '`' | '\u{201c}' | '\u{201d}')
            })
            .split_whitespace();
        let requested =
            tokens.next().unwrap_or_default().trim_matches(trim_exact_answer_token_character);
        let limiter =
            tokens.next().unwrap_or_default().trim_matches(trim_exact_answer_token_character);
        if requested == normalized_answer && matches!(limiter, "only" | "exactly") {
            return true;
        }
    }
    false
}

fn trim_exact_answer_token_character(character: char) -> bool {
    matches!(
        character,
        '.' | ','
            | ';'
            | ':'
            | '!'
            | '?'
            | '"'
            | '\''
            | '`'
            | '\u{201c}'
            | '\u{201d}'
            | '('
            | ')'
            | '['
            | ']'
    )
}

fn exact_answer_marker_is_negated(prefix: &str) -> bool {
    prefix.ends_with("do not")
        || prefix.ends_with("don't")
        || prefix.ends_with("dont")
        || prefix.ends_with("never")
        || prefix.ends_with("not")
}

fn has_action_tool_evidence(messages: &[ProviderMessage]) -> bool {
    messages.iter().flat_map(|message| message.tool_calls.iter()).any(|call| {
        !matches!(
            call.tool_name.as_str(),
            "palyra.fs.list_dir"
                | "palyra.fs.read_file"
                | "palyra.fs.search"
                | "palyra.memory.status"
                | "palyra.memory.search"
                | "palyra.memory.recall"
        )
    })
}

fn final_answer_has_matching_tool_evidence(text: &str, messages: &[ProviderMessage]) -> bool {
    let normalized = normalize_final_answer_text(text);
    if normalized.contains("i read the file") {
        return has_tool_name(messages, "palyra.fs.read_file");
    }
    if normalized.contains("i navigated") || normalized.contains("i opened the browser") {
        return has_tool_name_prefix(messages, "palyra.browser.");
    }
    if normalized.contains("i ran the test")
        || normalized.contains("i ran tests")
        || normalized.contains("test passed")
        || normalized.contains("tests passed")
    {
        return has_tool_name(messages, "palyra.process.run");
    }
    if normalized.contains("i applied the patch")
        || normalized.contains("i created")
        || normalized.contains("i edited")
        || normalized.contains("i fixed")
        || normalized.contains("i implemented")
        || normalized.contains("i modified")
        || normalized.contains("i updated")
        || normalized.contains("i wrote")
    {
        return has_tool_name(messages, "palyra.fs.apply_patch");
    }
    has_action_tool_evidence(messages)
}

fn has_tool_name(messages: &[ProviderMessage], tool_name: &str) -> bool {
    messages
        .iter()
        .flat_map(|message| message.tool_calls.iter())
        .any(|call| call.tool_name == tool_name)
}

fn has_tool_name_prefix(messages: &[ProviderMessage], prefix: &str) -> bool {
    messages
        .iter()
        .flat_map(|message| message.tool_calls.iter())
        .any(|call| call.tool_name.starts_with(prefix))
}

const UNSUPPORTED_TOOL_WORK_CLAIMS: &[&str] = &[
    "i applied the patch",
    "i created the file",
    "i created files",
    "i edited the file",
    "i fixed the file",
    "i implemented",
    "i modified the file",
    "i navigated",
    "i opened the browser",
    "i ran the test",
    "i ran tests",
    "i read the file",
    "i updated the file",
    "i verified",
    "i wrote the file",
    "test passed",
    "tests passed",
];

const DEFERRED_TOOL_WORK_MARKERS: &[&str] = &[
    "let me ",
    "i will ",
    "i'll ",
    "i need to ",
    "i should ",
    "i am going to ",
    "i'm going to ",
    "next, i ",
];

const DEFERRED_TOOL_WORK_NEGATED_MARKERS: &[&str] = &[
    "i will not ",
    "i won't ",
    "i should not ",
    "i don't need to ",
    "i do not need to ",
    "i am not going to ",
    "i'm not going to ",
];

const TOOL_WORK_ACTION_MARKERS: &[&str] = &[
    "apply_patch",
    "browse",
    "browser",
    "build",
    "check",
    "create",
    "edit",
    "fix",
    "implement",
    "inspect",
    "list",
    "navigate",
    "open",
    "patch",
    "read",
    "research",
    "run",
    "search",
    "test",
    "update",
    "verify",
    "write",
];

const TERMINAL_TOOL_AUTHORIZATION_ERROR_MARKERS: &[&str] = &[
    "approval_response_error",
    "approval_response_timeout",
    "approval_required_non_interactive_cli",
    "denied_by_cli_approval_mode_deny",
];

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
    let bounded_output = bounded_provider_turn_output_for_persistence(output);
    let payload_json = serde_json::to_string(&bounded_output).map_err(|error| {
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

#[cfg(test)]
mod tests {
    use super::{
        agent_loop_budget_exhausted_message, apply_background_budget_guard,
        background_budget_overrun_message, background_run_budget_tokens,
        contains_raw_provider_tool_call_markup, final_answer_recovery_fallback_summary,
        final_answer_recovery_prompt, incomplete_final_answer_without_tools,
        incomplete_terminal_final_answer, is_run_stream_response_channel_closed,
        length_recovery_prompt, provider_model_override_for_routing,
        provider_request_deadline_timeout, provider_request_timeout_status,
        repeated_tool_failure_signature, should_emit_budget_exhausted_partial_summary,
        should_terminate_after_tool_budget_exhausted, terminal_tool_authorization_failure,
        tool_calls_finish_without_tool_payload, tool_result_to_provider_message,
        truncated_final_answer_without_tools, RepeatedToolFailureTracker,
        RunStreamToolResultForModel, MAX_LENGTH_RECOVERY_ATTEMPTS,
    };
    use super::{AgentLoopTerminationReason, AgentRunLoopState};
    use crate::application::run_stream::tape::RUN_STREAM_RESPONSE_CHANNEL_CLOSED_MESSAGE;
    use crate::model_provider::{
        ProviderFinishReason, ProviderMessage, ProviderMessageContentPart,
        ProviderOutputContentPart, ProviderRawProviderRefs, ProviderRequest,
        ProviderRouteCandidateTrace, ProviderRouteSelectionTrace, ProviderTurnOutput,
        ProviderUsage,
    };
    use serde_json::{json, Value};
    use std::time::Duration;
    use tonic::{Code, Status};

    fn loop_state_after_tool(prompt: &str, tool_name: &str) -> AgentRunLoopState {
        let mut state =
            AgentRunLoopState::new(vec![ProviderMessage::user_text(prompt)], 4, 8, 10_000);
        state.append_assistant_turn(&ProviderTurnOutput {
            full_text: String::new(),
            content_parts: vec![ProviderOutputContentPart::ToolCall {
                proposal_id: "toolu_test_01".to_owned(),
                tool_name: tool_name.to_owned(),
                input_json: json!({}),
            }],
            finish_reason: ProviderFinishReason::ToolCalls,
            usage: ProviderUsage::new(0, 0, "test"),
            raw_provider_refs: ProviderRawProviderRefs::default(),
            redaction_state: Default::default(),
        });
        state.append_tool_result_messages(vec![ProviderMessage::tool_result(
            "toolu_test_01",
            r#"{"success":true}"#,
        )]);
        state
    }

    fn route_selection_with_fallback(failover_enabled: bool) -> ProviderRouteSelectionTrace {
        ProviderRouteSelectionTrace {
            default_model_id: Some("gpt-4o-mini".to_owned()),
            failover_enabled,
            generated_at_unix_ms: 1,
            selected_provider_id: Some("openai-primary".to_owned()),
            selected_model_id: Some("gpt-4o-mini".to_owned()),
            candidates: vec![
                route_candidate("openai-primary", "gpt-4o-mini", true, "eligible"),
                route_candidate("anthropic-primary", "claude-3-5-sonnet-latest", false, "eligible"),
                route_candidate("disabled-provider", "disabled-chat", false, "provider_disabled"),
            ],
        }
    }

    fn route_candidate(
        provider_id: &str,
        model_id: &str,
        selected: bool,
        capability_state: &str,
    ) -> ProviderRouteCandidateTrace {
        ProviderRouteCandidateTrace {
            provider_id: provider_id.to_owned(),
            credential_id: format!("credential:{provider_id}"),
            model_id: model_id.to_owned(),
            role: "chat".to_owned(),
            capability_state: capability_state.to_owned(),
            health_state: "healthy".to_owned(),
            selected,
            reason_code: "test".to_owned(),
        }
    }

    #[test]
    fn run_stream_response_channel_closed_status_is_classified_narrowly() {
        let closed = Status::cancelled(RUN_STREAM_RESPONSE_CHANNEL_CLOSED_MESSAGE);
        assert!(is_run_stream_response_channel_closed(&closed));

        let different_cancel = Status::cancelled("caller cancelled before final answer");
        assert!(!is_run_stream_response_channel_closed(&different_cancel));

        let internal = Status::new(Code::Internal, RUN_STREAM_RESPONSE_CHANNEL_CLOSED_MESSAGE);
        assert!(!is_run_stream_response_channel_closed(&internal));
    }

    #[test]
    fn provider_request_timeout_status_is_actionable_deadline() {
        let status = provider_request_timeout_status(
            "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            Duration::from_millis(1_250),
        );

        assert_eq!(status.code(), Code::DeadlineExceeded);
        assert!(status.message().contains("model provider turn timed out after 1250ms"));
        assert!(status.message().contains("01ARZ3NDEKTSV4RRFFQ69G5FAV"));
        assert!(status.message().contains("model_provider.request_timeout_ms"));
    }

    #[test]
    fn provider_request_deadline_extends_for_failover_candidates() {
        let request = ProviderRequest::from_input_text("hello".to_owned(), false, Vec::new(), None);
        let deadline = provider_request_deadline_timeout(
            Duration::from_millis(10_000),
            &route_selection_with_fallback(true),
            &request,
        );

        assert_eq!(deadline, Duration::from_millis(25_000));
    }

    #[test]
    fn provider_request_deadline_does_not_extend_for_model_override() {
        let request = ProviderRequest::from_input_text(
            "hello".to_owned(),
            false,
            Vec::new(),
            Some("gpt-4o-mini".to_owned()),
        );
        let deadline = provider_request_deadline_timeout(
            Duration::from_millis(10_000),
            &route_selection_with_fallback(true),
            &request,
        );

        assert_eq!(deadline, Duration::from_millis(10_000));
    }

    #[test]
    fn provider_model_override_is_unset_for_publish_only_routing() {
        assert_eq!(provider_model_override_for_routing("suggest", "MiniMax-M3"), None);
        assert_eq!(provider_model_override_for_routing("dry_run", "MiniMax-M3"), None);
    }

    #[test]
    fn provider_model_override_is_set_for_enforced_routing() {
        assert_eq!(
            provider_model_override_for_routing("enforced", "MiniMax-M3"),
            Some("MiniMax-M3".to_owned())
        );
    }

    #[test]
    fn background_run_budget_tokens_reads_background_parameter_delta() {
        let parameter_delta = json!({
            "background_task": {
                "task_id": "task-01",
                "budget_tokens": 1_000
            }
        })
        .to_string();

        assert_eq!(background_run_budget_tokens(Some(parameter_delta.as_str())), Some(1_000));
        assert_eq!(background_run_budget_tokens(Some("{}")), None);
        assert_eq!(background_run_budget_tokens(Some("not-json")), None);
    }

    #[test]
    fn background_budget_guard_clamps_provider_output_tokens() {
        let mut request = ProviderRequest::from_input_text(
            "write a concise inventory report".to_owned(),
            false,
            Vec::new(),
            None,
        );
        request.max_output_tokens = Some(900);

        let decision = apply_background_budget_guard(&mut request, 1_000, 200)
            .expect("small background task should fit inside budget");

        assert_eq!(decision.budget_tokens, 1_000);
        assert!(decision.estimated_input_tokens > 0);
        assert_eq!(request.max_output_tokens, Some(decision.max_output_tokens));
        assert!(decision.max_output_tokens < 900);
    }

    #[test]
    fn background_budget_guard_rejects_over_budget_input() {
        let mut request = ProviderRequest::from_input_text(
            vec!["word"; 1_100].join(" "),
            false,
            Vec::new(),
            None,
        );

        let message = apply_background_budget_guard(&mut request, 1_000, 0)
            .expect_err("oversized background prompt must fail before provider execution");

        assert!(message.contains("background task token budget exhausted"));
        assert_eq!(request.max_output_tokens, None);
    }

    #[test]
    fn background_budget_overrun_detects_provider_usage_after_turn() {
        assert!(background_budget_overrun_message(1_000, 1_001)
            .expect("usage above budget must be rejected")
            .contains("budget_tokens=1000"));
        assert!(background_budget_overrun_message(1_000, 1_000).is_none());
    }

    #[test]
    fn terminal_tool_authorization_failure_detects_approval_errors() {
        let result = RunStreamToolResultForModel {
            proposal_id: "toolu_approval_01".to_owned(),
            tool_name: "palyra.process.run".to_owned(),
            outcome: crate::tool_protocol::denied_execution_outcome(
                "toolu_approval_01",
                "palyra.process.run",
                br#"{"command":"cmd","args":["/C","whoami"]}"#,
                "approval_response_error: tool_approval_response.proposal_id is required",
            ),
        };

        let message = terminal_tool_authorization_failure(&result)
            .expect("approval protocol failures must terminate the run");
        assert!(message.contains("palyra.process.run"));
        assert!(message.contains("toolu_approval_01"));
        assert!(message.contains("approval_response_error"));
    }

    #[test]
    fn repeated_tool_failure_tracker_stops_identical_workspace_patch_parse_errors() {
        let message = workspace_patch_parse_error_tool_message(
            "toolu_patch_01",
            "palyra.fs.apply_patch failed: patch parse error at line 3, column 1: expected '*** End Patch'",
            "Remove any duplicate terminator or text after the final '*** End Patch', then retry with one complete patch.",
        );
        let mut tracker = RepeatedToolFailureTracker::default();

        assert!(repeated_tool_failure_signature(&message).is_some());
        assert!(tracker.observe(std::slice::from_ref(&message)).is_none());
        assert!(tracker.observe(std::slice::from_ref(&message)).is_none());
        let failure = tracker
            .observe(std::slice::from_ref(&message))
            .expect("third identical patch parse failure should terminate");

        assert!(failure.message.contains("3 repeated malformed palyra.fs.apply_patch calls"));
        assert!(failure.message.contains("workspace_patch_parse.expected_end_patch"));
        assert!(failure.message.contains("Read the current file before retrying"));
    }

    #[test]
    fn repeated_tool_failure_tracker_resets_on_distinct_patch_parse_error() {
        let trailing = workspace_patch_parse_error_tool_message(
            "toolu_patch_01",
            "palyra.fs.apply_patch failed: patch parse error at line 3, column 1: expected '*** End Patch'",
            "Remove any duplicate terminator or text after the final '*** End Patch', then retry with one complete patch.",
        );
        let missing_begin = workspace_patch_parse_error_tool_message(
            "toolu_patch_02",
            "palyra.fs.apply_patch failed: patch parse error at line 1, column 1: expected '*** Begin Patch'",
            "Start the patch with exactly '*** Begin Patch' on its own line, not a Markdown-decorated variant.",
        );
        let mut tracker = RepeatedToolFailureTracker::default();

        assert!(tracker.observe(std::slice::from_ref(&trailing)).is_none());
        assert!(tracker.observe(std::slice::from_ref(&trailing)).is_none());
        assert!(tracker.observe(std::slice::from_ref(&missing_begin)).is_none());
        assert!(tracker.observe(std::slice::from_ref(&missing_begin)).is_none());
        let failure = tracker
            .observe(std::slice::from_ref(&missing_begin))
            .expect("third distinct-signature repetition should terminate");

        assert!(failure.message.contains("workspace_patch_parse.expected_begin_patch"));
    }

    fn workspace_patch_parse_error_tool_message(
        proposal_id: &str,
        error: &str,
        recovery_hint: &str,
    ) -> ProviderMessage {
        ProviderMessage::tool_result(
            proposal_id,
            json!({
                "success": false,
                "tool_name": crate::gateway::WORKSPACE_PATCH_TOOL_NAME,
                "error": error,
                "output": {
                    "parse_error": {
                        "line": 3,
                        "column": 1
                    },
                    "recovery_hint": recovery_hint
                }
            })
            .to_string(),
        )
    }

    #[test]
    fn budget_exhausted_message_includes_resume_context() {
        let state = loop_state_after_tool("build a browser app", "palyra.browser.navigate");

        let message = agent_loop_budget_exhausted_message(
            AgentLoopTerminationReason::MaxTurns,
            &state,
            "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        );

        assert!(message.contains("model turn limit reached"));
        assert!(message.contains("1 tool result"));
        assert!(message.contains("partial result summary"));
        assert!(message.contains("resume from run 01ARZ3NDEKTSV4RRFFQ69G5FAV"));
        assert!(message.contains("model-turn budget"));
    }

    #[test]
    fn tool_budget_exhausted_message_names_tool_call_limit() {
        let state = loop_state_after_tool("clean up generated files", "palyra.fs.apply_patch");

        let message = agent_loop_budget_exhausted_message(
            AgentLoopTerminationReason::MaxToolCalls,
            &state,
            "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        );

        assert!(message.contains("tool call limit reached"));
        assert!(message.contains("partial result summary"));
        assert!(message.contains("resume from run 01ARZ3NDEKTSV4RRFFQ69G5FAV"));
        assert!(message.contains("tool_call.max_calls_per_run"));
    }

    #[test]
    fn wall_clock_budget_exhausted_message_names_wall_clock_not_tool_limit() {
        let mut state = loop_state_after_tool("debug a browser app", "palyra.browser.observe");
        state.sync_remaining_tool_calls(16);

        let message = agent_loop_budget_exhausted_message(
            AgentLoopTerminationReason::WallClock,
            &state,
            "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        );

        assert!(message.contains("wall-clock budget exhausted"));
        assert!(message.contains("partial result summary"));
        assert!(message.contains("remaining_tool_calls=8"));
        assert!(message.contains("elapsed_ms="));
        assert!(message.contains("tool-call limit was not the terminal condition"));
        assert!(!message.contains("tool_call.max_calls_per_run"));
    }

    #[test]
    fn budget_partial_summary_requires_terminal_budget_with_tool_evidence() {
        let state = loop_state_after_tool("build a browser app", "palyra.browser.observe");
        let state_without_tools =
            AgentRunLoopState::new(vec![ProviderMessage::user_text("hello")], 4, 8, 10_000);

        assert!(should_emit_budget_exhausted_partial_summary(
            AgentLoopTerminationReason::WallClock,
            &state
        ));
        assert!(should_emit_budget_exhausted_partial_summary(
            AgentLoopTerminationReason::MaxToolCalls,
            &state
        ));
        assert!(!should_emit_budget_exhausted_partial_summary(
            AgentLoopTerminationReason::ProviderError,
            &state
        ));
        assert!(!should_emit_budget_exhausted_partial_summary(
            AgentLoopTerminationReason::WallClock,
            &state_without_tools
        ));
    }

    #[test]
    fn tool_budget_exhaustion_after_result_batch_terminates_loop() {
        let mut state = loop_state_after_tool("clean up generated files", "palyra.fs.apply_patch");
        state.sync_remaining_tool_calls(0);

        assert!(should_terminate_after_tool_budget_exhausted(&state, 1));
        assert!(!should_terminate_after_tool_budget_exhausted(&state, 0));

        state.sync_remaining_tool_calls(1);
        assert!(!should_terminate_after_tool_budget_exhausted(&state, 1));
    }

    #[test]
    fn terminal_tool_authorization_failure_refeeds_explicit_approval_denials() {
        let result = RunStreamToolResultForModel {
            proposal_id: "toolu_denied_01".to_owned(),
            tool_name: "palyra.process.run".to_owned(),
            outcome: crate::tool_protocol::denied_execution_outcome(
                "toolu_denied_01",
                "palyra.process.run",
                br#"{"command":"cmd","args":["/C","whoami"]}"#,
                "approval.denied: operator denied tool execution",
            ),
        };

        assert!(
            terminal_tool_authorization_failure(&result).is_none(),
            "explicit approval denials are tool observations the model can recover from"
        );
    }

    #[test]
    fn terminal_tool_authorization_failure_stops_noninteractive_cli_denials() {
        let result = RunStreamToolResultForModel {
            proposal_id: "toolu_noninteractive_01".to_owned(),
            tool_name: "palyra.process.run".to_owned(),
            outcome: crate::tool_protocol::denied_execution_outcome(
                "toolu_noninteractive_01",
                "palyra.process.run",
                br#"{"command":"node","args":["-e","console.log(1)"]}"#,
                "approval.denied: approval_required_non_interactive_cli",
            ),
        };

        let message = terminal_tool_authorization_failure(&result)
            .expect("noninteractive CLI approval denials should terminate the run");
        assert!(message.contains("noninteractive CLI"));
        assert!(message.contains("--approval-mode allow-once"));
        assert!(message.contains("--allow-sensitive-tools"));
        assert!(message.contains("toolu_noninteractive_01"));
    }

    #[test]
    fn terminal_tool_authorization_failure_stops_cli_deny_mode() {
        let result = RunStreamToolResultForModel {
            proposal_id: "toolu_deny_mode_01".to_owned(),
            tool_name: "palyra.fs.read_file".to_owned(),
            outcome: crate::tool_protocol::denied_execution_outcome(
                "toolu_deny_mode_01",
                "palyra.fs.read_file",
                br#"{"path":"generated/temp.txt"}"#,
                "tool execution denied by explicit client approval response; tool=palyra.fs.read_file; approval_reason=denied_by_cli_approval_mode_deny; original_reason=requires approval",
            ),
        };

        let message = terminal_tool_authorization_failure(&result)
            .expect("CLI deny mode approval denials should terminate the run");
        assert!(message.contains("--approval-mode deny"));
        assert!(message.contains("No approval prompt is pending"));
        assert!(message.contains("was not executed"));
        assert!(message.contains("--approval-mode allow-once"));
        assert!(message.contains("toolu_deny_mode_01"));
    }

    #[test]
    fn terminal_tool_authorization_failure_ignores_regular_tool_errors() {
        let result = RunStreamToolResultForModel {
            proposal_id: "toolu_regular_error_01".to_owned(),
            tool_name: "palyra.process.run".to_owned(),
            outcome: crate::tool_protocol::build_tool_execution_outcome(
                "toolu_regular_error_01",
                "palyra.process.run",
                br#"{"command":"cmd","args":["/C","exit","1"]}"#,
                false,
                b"{}".to_vec(),
                "process exited with status 1".to_owned(),
                false,
                "builtin".to_owned(),
                "none".to_owned(),
            ),
        };

        assert!(
            terminal_tool_authorization_failure(&result).is_none(),
            "ordinary runtime errors can still be re-fed to the model"
        );
    }

    #[test]
    fn failed_browser_console_result_marks_console_status_unknown_for_model() {
        let result = RunStreamToolResultForModel {
            proposal_id: "toolu_console_01".to_owned(),
            tool_name: crate::gateway::BROWSER_CONSOLE_LOG_TOOL_NAME.to_owned(),
            outcome: crate::tool_protocol::build_tool_execution_outcome(
                "toolu_console_01",
                crate::gateway::BROWSER_CONSOLE_LOG_TOOL_NAME,
                br#"{"session_id":"browser-session-1"}"#,
                false,
                b"{}".to_vec(),
                "missing caller principal".to_owned(),
                false,
                "builtin".to_owned(),
                "none".to_owned(),
            ),
        };

        let message = tool_result_to_provider_message(&result)
            .expect("failed console tool result should serialize for model");
        let content = match message.content.first() {
            Some(ProviderMessageContentPart::Text { text }) => text,
            _ => panic!("tool result should be serialized as text content"),
        };
        let value: Value =
            serde_json::from_str(content).expect("tool result content should be JSON");

        assert_eq!(value.get("success").and_then(Value::as_bool), Some(false));
        assert_eq!(value.get("diagnostic_status").and_then(Value::as_str), Some("unknown"));
        assert!(
            value.get("claim_boundary").and_then(Value::as_str).is_some_and(
                |boundary| boundary.contains("do not claim the page has no console errors")
            ),
            "{value}"
        );
    }

    #[test]
    fn failed_memory_retain_result_warns_model_not_to_claim_storage() {
        let result = RunStreamToolResultForModel {
            proposal_id: "toolu_memory_retain_01".to_owned(),
            tool_name: crate::gateway::MEMORY_RETAIN_TOOL_NAME.to_owned(),
            outcome: crate::tool_protocol::build_tool_execution_outcome(
                "toolu_memory_retain_01",
                crate::gateway::MEMORY_RETAIN_TOOL_NAME,
                br#"{"content_text":"remember this"}"#,
                false,
                br#"{"durable_memory_write":false,"review_state":"not_written_requires_review"}"#
                    .to_vec(),
                "palyra.memory.retain did not write memory".to_owned(),
                false,
                "gateway_runtime".to_owned(),
                "none".to_owned(),
            ),
        };

        let message = tool_result_to_provider_message(&result)
            .expect("failed memory retain result should serialize for model");
        let content = match message.content.first() {
            Some(ProviderMessageContentPart::Text { text }) => text,
            _ => panic!("tool result should be serialized as text content"),
        };
        let value: Value =
            serde_json::from_str(content).expect("tool result content should be JSON");

        assert_eq!(value.get("success").and_then(Value::as_bool), Some(false));
        assert!(
            value
                .get("claim_boundary")
                .and_then(Value::as_str)
                .is_some_and(|boundary| boundary.contains("do not claim the memory was stored")),
            "{value}"
        );
    }

    #[test]
    fn raw_provider_tool_call_markup_is_not_a_final_answer() {
        let raw_tool_call = r#"<minimax:tool_call>
<invoke name="palyra.fs.read_file">
{"path":"C:\\Users\\palo\\workspace\\calc.js"}
</invoke>
</minimax:tool_call>"#;

        assert!(contains_raw_provider_tool_call_markup(raw_tool_call));
        assert!(!contains_raw_provider_tool_call_markup(
            "The page had no tool calls and the final answer is complete."
        ));
    }

    #[test]
    fn incomplete_final_answer_without_tools_detects_bare_ack() {
        let message = incomplete_final_answer_without_tools(Some("done"), &[])
            .expect("bare acknowledgement must not be accepted as a final answer");

        assert!(message.contains("bare acknowledgement"));
    }

    #[test]
    fn incomplete_final_answer_without_tools_allows_requested_exact_ack() {
        let messages = vec![ProviderMessage::user_text("Acknowledge exactly OK.".to_owned())];

        assert!(incomplete_final_answer_without_tools(Some("OK"), messages.as_slice()).is_none());
    }

    #[test]
    fn incomplete_final_answer_without_tools_allows_requested_reply_only_ack_sentinel() {
        let messages = vec![ProviderMessage::user_text("Reply ACK-READY-4 only.".to_owned())];

        assert!(incomplete_final_answer_without_tools(Some("ACK-READY-4"), messages.as_slice())
            .is_none());
    }

    #[test]
    fn incomplete_final_answer_without_tools_rejects_unrequested_ack_sentinel() {
        let message = incomplete_final_answer_without_tools(Some("ACK-READY-4"), &[])
            .expect("unrequested ACK sentinel must not be accepted as a final answer");

        assert!(message.contains("bare acknowledgement"));
    }

    #[test]
    fn incomplete_final_answer_without_tools_detects_deferred_work() {
        let message = incomplete_final_answer_without_tools(
            Some(
                "The workspace is empty. I\u{2019}ll create the todo app files and run the tests.",
            ),
            &[],
        )
        .expect("deferred tool work must not be accepted as a final answer");

        assert!(message.contains("planning or intent statement"));
    }

    #[test]
    fn incomplete_final_answer_without_tools_allows_negated_deferred_work() {
        assert!(incomplete_final_answer_without_tools(
            Some("I will not edit files because you asked only for an explanation."),
            &[]
        )
        .is_none());
    }

    #[test]
    fn truncated_provider_output_is_not_a_final_answer_without_tools() {
        let output = ProviderTurnOutput::text(
            "Created fixtures/app and ran".to_owned(),
            ProviderFinishReason::Length,
            ProviderUsage::new(10, 20, "test"),
            ProviderRawProviderRefs::default(),
        );

        let message = truncated_final_answer_without_tools(&output)
            .expect("length-finished output must not be accepted as final");

        assert!(message.contains("finish_reason=length"));
    }

    #[test]
    fn tool_calls_finish_without_tool_payload_is_rejected() {
        let output = ProviderTurnOutput::text(
            "Workspace is empty. I will create the files next.".to_owned(),
            ProviderFinishReason::ToolCalls,
            ProviderUsage::new(10, 20, "test"),
            ProviderRawProviderRefs::default(),
        );

        let message = tool_calls_finish_without_tool_payload(&output)
            .expect("tool_calls finish without structured tool payload must be rejected");

        assert!(message.contains("finish_reason=tool_calls"));
        assert!(message.contains("without a structured tool call payload"));
    }

    #[test]
    fn tool_calls_finish_guard_allows_plain_final_answer() {
        let output = ProviderTurnOutput::text(
            "No changes needed.".to_owned(),
            ProviderFinishReason::Stop,
            ProviderUsage::new(10, 20, "test"),
            ProviderRawProviderRefs::default(),
        );

        assert!(tool_calls_finish_without_tool_payload(&output).is_none());
    }

    #[test]
    fn tool_calls_finish_guard_allows_structured_tool_payload() {
        let output = ProviderTurnOutput {
            full_text: String::new(),
            content_parts: vec![ProviderOutputContentPart::ToolCall {
                proposal_id: "toolu_test_01".to_owned(),
                tool_name: "palyra.fs.apply_patch".to_owned(),
                input_json: json!({"patch":"*** Begin Patch\n*** End Patch"}),
            }],
            finish_reason: ProviderFinishReason::ToolCalls,
            usage: ProviderUsage::new(10, 20, "test"),
            raw_provider_refs: ProviderRawProviderRefs::default(),
            redaction_state: Default::default(),
        };

        assert!(tool_calls_finish_without_tool_payload(&output).is_none());
    }

    #[test]
    fn final_answer_recovery_fallback_summary_points_to_run_evidence() {
        let state = loop_state_after_tool("Create a report", "palyra.fs.apply_patch");

        let summary = final_answer_recovery_fallback_summary(
            "model returned a planning or intent statement as the final answer after tool execution",
            &state,
            "01RUNFALLBACK000000000000",
        );

        assert!(summary.contains("Partial result"));
        assert!(summary.contains("1 tool call"));
        assert!(summary.contains("01RUNFALLBACK000000000000"));
        assert!(summary.contains("Resume this same session"));
    }

    #[test]
    fn length_finished_provider_output_gets_bounded_recovery_prompts() {
        let mut loop_state = AgentRunLoopState::new(
            vec![ProviderMessage::user_text("Create app files".to_owned())],
            2,
            8,
            10_000,
        );
        loop_state.start_model_turn().expect("first turn should start");

        let prompt = length_recovery_prompt(
            AgentLoopTerminationReason::IncompleteFinalAnswer,
            "model provider stopped because of an output token limit (finish_reason=length)",
            &loop_state,
            0,
        )
        .expect("first length failure with remaining turns should be recoverable");
        assert!(prompt.contains("one concise tool call next"));
        assert!(prompt.contains("palyra.fs.apply_patch"));

        let second_prompt = length_recovery_prompt(
            AgentLoopTerminationReason::IncompleteFinalAnswer,
            "model provider stopped because of an output token limit (finish_reason=length)",
            &loop_state,
            1,
        )
        .expect("second length failure should still be recoverable");
        assert!(second_prompt.contains("exactly one small structured tool call"));

        let final_prompt = length_recovery_prompt(
            AgentLoopTerminationReason::IncompleteFinalAnswer,
            "model provider stopped because of an output token limit (finish_reason=length)",
            &loop_state,
            2,
        )
        .expect("third length failure should get a last-chance recovery prompt");
        assert!(final_prompt.contains("Last length-recovery attempt"));

        assert!(
            length_recovery_prompt(
                AgentLoopTerminationReason::IncompleteFinalAnswer,
                "model provider stopped because of an output token limit (finish_reason=length)",
                &loop_state,
                MAX_LENGTH_RECOVERY_ATTEMPTS,
            )
            .is_none(),
            "length recovery must be bounded per run"
        );
    }

    #[test]
    fn empty_final_after_tool_execution_gets_one_recovery_prompt() {
        let state = loop_state_after_tool(
            "Refactor src/reporting.ts into smaller modules and summarize changed files.",
            "palyra.fs.apply_patch",
        );

        let prompt = final_answer_recovery_prompt(
            "model returned an empty final answer after tool execution",
            &state,
            false,
        )
        .expect("empty final answer after tool execution should be recoverable once");

        assert!(prompt.contains("changed files"));
        assert!(prompt.contains("partial state"));
        assert!(
            final_answer_recovery_prompt(
                "model returned an empty final answer after tool execution",
                &state,
                true,
            )
            .is_none(),
            "final-answer recovery must be attempted at most once per run"
        );
    }

    #[test]
    fn deferred_final_after_tool_execution_gets_one_recovery_prompt() {
        let state =
            loop_state_after_tool("Create fixtures/cz-validator with tests.", "palyra.fs.list_dir");

        let prompt = final_answer_recovery_prompt(
            "model returned a planning or intent statement as the final answer after tool execution",
            &state,
            false,
        )
        .expect("deferred work after tool execution should be recoverable once");

        assert!(prompt.contains("issue the next minimal tool call"));
    }

    #[test]
    fn stop_finished_provider_output_can_be_final_without_tools() {
        let output = ProviderTurnOutput::text(
            "Use cargo test to run the daemon tests.".to_owned(),
            ProviderFinishReason::Stop,
            ProviderUsage::new(10, 20, "test"),
            ProviderRawProviderRefs::default(),
        );

        assert!(truncated_final_answer_without_tools(&output).is_none());
    }

    #[test]
    fn incomplete_final_answer_without_tools_detects_unsupported_work_claims() {
        let message = incomplete_final_answer_without_tools(
            Some("I created the file and tests passed."),
            &[],
        )
        .expect("tool-work claims need tool evidence");

        assert!(message.contains("without any successful tool results"));
    }

    #[test]
    fn incomplete_final_answer_without_tools_allows_plain_answers() {
        assert!(incomplete_final_answer_without_tools(
            Some("Use `cargo test -p palyra-daemon` to run the daemon tests."),
            &[]
        )
        .is_none());
    }

    #[test]
    fn incomplete_terminal_final_answer_rejects_ack_for_requested_tool_work() {
        let state = loop_state_after_tool(
            "Create fixtures/landing-page and verify it.",
            "palyra.fs.list_dir",
        );
        let message = incomplete_terminal_final_answer(Some("ack"), &state)
            .expect("bare ack must not complete a requested tool workflow");

        assert!(message.contains("bare acknowledgement"));
    }

    #[test]
    fn incomplete_terminal_final_answer_rejects_deferred_work_after_read_only_tool() {
        let state =
            loop_state_after_tool("Create fixtures/cz-validator with tests.", "palyra.fs.list_dir");
        let message = incomplete_terminal_final_answer(
            Some("Good, the directory is absent. I'll create the files next."),
            &state,
        )
        .expect("deferred work after read-only discovery must not complete the run");

        assert!(message.contains("planning or intent statement"));
    }

    #[test]
    fn incomplete_terminal_final_answer_allows_requested_exact_ack_after_tool() {
        let state = loop_state_after_tool(
            "Create fixtures/landing-page and acknowledge exactly OK.",
            "palyra.fs.apply_patch",
        );

        assert!(incomplete_terminal_final_answer(Some("OK"), &state).is_none());
    }

    #[test]
    fn incomplete_terminal_final_answer_ignores_stale_exact_ack_context_after_tool() {
        let mut state = AgentRunLoopState::new(
            vec![
                ProviderMessage::user_text("Previous context says respond exactly OK.".to_owned()),
                ProviderMessage::user_text(
                    "Create fixtures/landing-page and verify it.".to_owned(),
                ),
            ],
            4,
            8,
            10_000,
        );
        state.append_assistant_turn(&ProviderTurnOutput {
            full_text: String::new(),
            content_parts: vec![ProviderOutputContentPart::ToolCall {
                proposal_id: "toolu_test_01".to_owned(),
                tool_name: "palyra.fs.apply_patch".to_owned(),
                input_json: json!({}),
            }],
            finish_reason: ProviderFinishReason::ToolCalls,
            usage: ProviderUsage::new(0, 0, "test"),
            raw_provider_refs: ProviderRawProviderRefs::default(),
            redaction_state: Default::default(),
        });
        state.append_tool_result_messages(vec![ProviderMessage::tool_result(
            "toolu_test_01",
            r#"{"success":true}"#,
        )]);

        let message = incomplete_terminal_final_answer(Some("OK"), &state)
            .expect("stale user-role context must not authorize a bare acknowledgement");

        assert!(message.contains("bare acknowledgement"));
    }

    #[test]
    fn incomplete_terminal_final_answer_allows_concrete_summary_after_action_tool() {
        let state = loop_state_after_tool(
            "Create fixtures/notes-api and run tests.",
            "palyra.fs.apply_patch",
        );

        assert!(incomplete_terminal_final_answer(
            Some("Created fixtures/notes-api and summarized the changed files."),
            &state,
        )
        .is_none());
    }

    #[test]
    fn incomplete_terminal_final_answer_allows_read_claim_after_read_tool() {
        let state =
            loop_state_after_tool("Read README.md and summarize it.", "palyra.fs.read_file");

        assert!(incomplete_terminal_final_answer(
            Some("I read the file. It describes the local development workflow."),
            &state,
        )
        .is_none());
    }
}
