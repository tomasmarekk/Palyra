//! Route-message orchestration: one inbound channel message to one reply.
//!
//! [`handle_routed_route_message`] runs the full pipeline for a message the
//! channel router already matched to a route plan: authorize the inbound
//! action, resolve session and conversation binding, coalesce duplicate
//! inbound text, run a single provider exchange (tool proposals handled
//! inline via `tool_flow` behind `response`), authorize the outbound send,
//! and assemble size-bounded outputs. In contrast to `run_stream` there is
//! no client to stream to: every outcome, including rejection, comes back as
//! one `RouteMessageResponse` while being mirrored to the orchestrator tape
//! and journal.

use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use palyra_common::runtime_contracts::{
    CancellationScopeKind, QueueMode, RuntimeActorKind, RuntimeActorRef, RuntimeSessionId,
    RuntimeTerminalOutcome,
};
use palyra_common::{runtime_preview::RuntimePreviewCapability, CANONICAL_PROTOCOL_MAJOR};
use serde_json::json;
use tonic::Status;
use tracing::warn;
use ulid::Ulid;

use crate::{
    agents::AgentResolveRequest,
    application::{
        channel_commands::{
            ChannelCommandName, ChannelCommandParseOutcome, ChannelCommandRegistry,
        },
        channel_turn::ChannelTurnEnvelope,
        conversation_bindings::{
            ConversationBindingCreateRequest, ConversationBindingKind, ConversationBindingLifecycle,
        },
        delivery_arbitration::resolve_delivery_policy,
        inbound_coalescer::{
            InboundCoalescingDecision, InboundCoalescingDecisionKind, InboundCoalescingRequest,
            INBOUND_COALESCING_BYPASSED_EVENT, INBOUND_COALESCING_CONSUMED_EVENT,
            INBOUND_COALESCING_PENDING_EVENT, INBOUND_COALESCING_READY_EVENT,
        },
        outbound_lifecycle::{
            ChannelOutboundCapabilities, OutboundLifecycle, OutboundLifecycleStart,
        },
        provider_input::{
            build_provider_image_inputs, prepare_model_provider_input,
            rematerialize_provider_input, MemoryPromptFailureMode,
            PrepareModelProviderInputRequest,
        },
        run_admission::{
            AdmissionCaller, RunAdmissionCommand, RunAdmissionController,
            RunAdmissionControllerOutcome,
        },
        run_stream::{
            admission_ingress::{admission_environment, channel_ingress},
            flow_control::RunStreamFlowControl,
        },
        runtime_kernel_v2::selection::{RuntimeAuthority, RuntimeAuthorityProgressEvidence},
        service_authorization::authorize_message_action,
        session_queue::SessionQueueSafeBoundary,
        tool_registry::{
            build_model_visible_tool_catalog_snapshot, tool_catalog_tape_payload,
            ModelVisibleToolCatalogSnapshot, ToolCatalogBuildRequest, ToolExposureSurface,
        },
    },
    channel_router::{
        InboundMessage as ChannelInboundMessage, RetryDisposition, RoutePlan as ChannelRoutePlan,
    },
    gateway::{
        agent_resolution_source_label, cleanup_run_resources, current_unix_ms,
        ingest_memory_best_effort, is_provider_reconfigured_status,
        record_message_router_journal_event, request_context_with_resolved_route_channel,
        truncate_with_ellipsis, GatewayRuntimeState, SessionQueueAdmissionRequest,
    },
    journal::{
        run_admission::JournalRunAdmissionSessionSelector, MemorySource,
        OrchestratorRunStartRequest, OrchestratorRunTerminalSettlement,
        OrchestratorRunTerminalSettlementRequest, OrchestratorSessionResolveRequest,
        OrchestratorTapeAppendRequest, OrchestratorUsageDelta,
    },
    orchestrator::RunLifecycleState,
    provider_leases::ProviderLeaseExecutionContext,
    self_healing::{WorkHeartbeatKind, WorkHeartbeatUpdate},
    tool_protocol::ToolRequestContext,
    transport::grpc::{
        auth::RequestContext,
        proto::palyra::{common::v1 as common_v1, gateway::v1 as gateway_v1},
    },
    usage_governance::{
        plan_usage_routing, resolve_provider_binding_for_model, RoutingTaskClass,
        UsageRoutingPlanRequest,
    },
};

use super::response::{
    build_route_message_outputs, process_route_provider_response, RouteMessageOutputTemplate,
    RouteProviderResponseProcessingOutcome,
};

const MAX_ROUTE_PROVIDER_SUPERSESSION_RETRIES: u8 = 1;
/// Route-message runs keep a surface-owned 15-minute wall-clock budget.
const ROUTE_MESSAGE_WALL_CLOCK_BUDGET_MS: u64 = 15 * 60 * 1_000;
const CHANNEL_V2_ADAPTER_UNAVAILABLE: &str = "runtime.channel_v2_adapter_unavailable";

fn route_message_status_tape_payload(state: RunLifecycleState, message: &str) -> String {
    match state {
        RunLifecycleState::Done => json!({
            "kind": "done",
            "message": message,
        })
        .to_string(),
        RunLifecycleState::Failed => json!({
            "kind": "failed",
            "message": message,
            "lifecycle_state": "failed",
        })
        .to_string(),
        RunLifecycleState::Cancelled => json!({
            "kind": "cancelled",
            "wire_kind": "failed",
            "message": crate::gateway::CANCELLED_REASON,
            "lifecycle_state": "cancelled",
            "reason_code": "cancelled_by_request",
            "controlled": true,
        })
        .to_string(),
        RunLifecycleState::Pending
        | RunLifecycleState::Accepted
        | RunLifecycleState::InProgress => {
            unreachable!("route-message terminal status requires a terminal state")
        }
    }
}

#[allow(clippy::result_large_err)]
async fn settle_route_message_run(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    requested_state: RunLifecycleState,
    reason_code: &str,
    status_message: &str,
) -> Result<OrchestratorRunTerminalSettlement, Status> {
    let settlement = runtime_state
        .settle_orchestrator_run_terminal(OrchestratorRunTerminalSettlementRequest {
            run_id: run_id.to_owned(),
            requested_state,
            reason_code: reason_code.to_owned(),
            status_message: status_message.to_owned(),
            actor: RuntimeActorRef {
                kind: RuntimeActorKind::System,
                id: "route_message".to_owned(),
            },
            terminal_summary_payload_json: None,
            terminal_tape_events: Vec::new(),
            terminal_status_payload_json: route_message_status_tape_payload(
                requested_state,
                status_message,
            ),
        })
        .await?;
    if settlement.changed {
        let cleanup_reason = match settlement.effective_state {
            RunLifecycleState::Cancelled => crate::gateway::CANCELLED_REASON,
            RunLifecycleState::Done => "completed",
            RunLifecycleState::Failed => status_message,
            RunLifecycleState::Pending
            | RunLifecycleState::Accepted
            | RunLifecycleState::InProgress => {
                return Err(Status::internal(
                    "route-message terminal settlement returned a nonterminal state",
                ));
            }
        };
        cleanup_run_resources(runtime_state, run_id, cleanup_reason).await;
        runtime_state.clear_self_healing_heartbeat(WorkHeartbeatKind::Run, run_id);
    }
    Ok(settlement)
}

fn inbound_coalescing_event_type(kind: InboundCoalescingDecisionKind) -> &'static str {
    match kind {
        InboundCoalescingDecisionKind::Bypassed => INBOUND_COALESCING_BYPASSED_EVENT,
        InboundCoalescingDecisionKind::Consumed => INBOUND_COALESCING_CONSUMED_EVENT,
        InboundCoalescingDecisionKind::Pending => INBOUND_COALESCING_PENDING_EVENT,
        InboundCoalescingDecisionKind::Ready => INBOUND_COALESCING_READY_EVENT,
    }
}

#[allow(clippy::too_many_arguments)]
async fn record_inbound_coalescing_journal_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    session_id: &str,
    run_id: &str,
    input: &ChannelInboundMessage,
    plan: &ChannelRoutePlan,
    route_config_hash: &str,
    coalescing_decision: &InboundCoalescingDecision,
) {
    let event_type = inbound_coalescing_event_type(coalescing_decision.kind);
    let _ = record_message_router_journal_event(
        runtime_state,
        request_context,
        session_id,
        run_id,
        event_type,
        common_v1::journal_event::EventActor::System as i32,
        json!({
            "event": event_type,
            "envelope_id": input.envelope_id.clone(),
            "channel": input.channel.clone(),
            "route_key": plan.route_key.clone(),
            "session_key": plan.session_key.clone(),
            "config_hash": route_config_hash,
            "coalescing": coalescing_decision
                .safe_snapshot_json(runtime_state.inbound_coalescer.policy()),
        }),
    )
    .await;
}

/// Builds the model-visible tool catalog for this routed run and records it
/// on the tape so replays see exactly what the model was offered.
#[allow(clippy::too_many_arguments)]
async fn build_and_record_route_tool_catalog_snapshot(
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    session_id: &str,
    run_id: &str,
    provider_kind: &str,
    provider_model_id: Option<&str>,
    _remaining_tool_budget: u32,
    tape_seq: &mut i64,
) -> Result<ModelVisibleToolCatalogSnapshot, Status> {
    let snapshot = build_model_visible_tool_catalog_snapshot(ToolCatalogBuildRequest {
        config: &runtime_state.config.tool_call,
        catalog_policy: &runtime_state.config.tool_catalog_policy,
        browser_service_enabled: runtime_state.config.browser_service.enabled,
        browser_service_configured: runtime_state.config.browser_service.enabled,
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
        surface: ToolExposureSurface::RouteMessage,
        remaining_tool_budget: None,
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

/// Processes one routed inbound message end to end and returns the final
/// `RouteMessageResponse`.
///
/// Policy denials and provider failures are not errors: they return
/// `accepted: false` with a decision reason (and a retry disposition for
/// provider failures) so the connector can ack, retry, or quarantine the
/// envelope. Journal writes along the way are best-effort and never fail the
/// route.
///
/// # Errors
/// Returns a status only for infrastructure failures: session resolution,
/// conversation-binding or run-state bookkeeping, tape appends, usage
/// accounting, or provider-response processing.
#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_routed_route_message(
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    input: &ChannelInboundMessage,
    content: &common_v1::MessageContent,
    plan: &ChannelRoutePlan,
    requested_session_label: Option<&str>,
    json_mode_requested: bool,
    envelope_id: &str,
    route_config_hash: &str,
    actor_connector: &str,
    actor_gateway_principal: &str,
    actor_gateway_device_id: &str,
    channel_turn_envelope: &ChannelTurnEnvelope,
    retry_attempt: u32,
) -> Result<gateway_v1::RouteMessageResponse, Status> {
    // Cloned because binding metadata is filled in below for journaling and
    // output assembly; the caller's plan must stay untouched for retries.
    let mut plan = plan.clone();
    let route_request_context =
        request_context_with_resolved_route_channel(request_context, plan.channel.as_str());
    let route_action = if plan.is_broadcast { "message.broadcast" } else { "message.reply" };
    let policy_resource = format!("channel:{}", plan.channel);
    if let Err(error) = authorize_message_action(
        route_request_context.principal.as_str(),
        route_action,
        policy_resource.as_str(),
        Some(plan.channel.as_str()),
        None,
        None,
    ) {
        runtime_state.record_denied();
        runtime_state.counters.channel_messages_rejected.fetch_add(1, Ordering::Relaxed);
        // The intake denial happens before any session/run exists, so fresh
        // ids are minted purely to give the journal event a stable shape.
        let journal_session_id = Ulid::new().to_string();
        let journal_run_id = Ulid::new().to_string();
        // Journal writes are best-effort throughout this flow: the routing
        // outcome must not change because an audit append failed.
        let _ = record_message_router_journal_event(
            runtime_state,
            &route_request_context,
            journal_session_id.as_str(),
            journal_run_id.as_str(),
            "message.rejected",
            common_v1::journal_event::EventActor::System as i32,
            json!({
                "event": "message.rejected",
                "envelope_id": input.envelope_id.clone(),
                "channel": input.channel.clone(),
                "reason": error.message(),
                "policy_action": route_action,
                "queued_for_retry": false,
                "quarantined": false,
                "config_hash": route_config_hash,
                "actor": {
                    "connector_channel": actor_connector,
                    "gateway_principal": actor_gateway_principal,
                    "gateway_device_id": actor_gateway_device_id,
                }
            }),
        )
        .await;
        return Ok(gateway_v1::RouteMessageResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            accepted: false,
            queued_for_retry: false,
            decision_reason: error.message().to_owned(),
            session_id: None,
            run_id: None,
            outputs: Vec::new(),
            route_key: plan.route_key.clone(),
            retry_attempt,
            queue_depth: runtime_state.channel_router.queue_depth() as u32,
        });
    }

    let resolved_session = runtime_state
        .resolve_orchestrator_session(OrchestratorSessionResolveRequest {
            session_id: None,
            session_key: Some(plan.session_key.clone()),
            session_label: requested_session_label
                .map(ToOwned::to_owned)
                .or_else(|| plan.session_label.clone()),
            principal: route_request_context.principal.clone(),
            device_id: route_request_context.device_id.clone(),
            channel: Some(plan.channel.clone()),
            require_existing: false,
            reset_session: false,
        })
        .await?;
    let session = resolved_session.session;
    let previous_run_id_for_context = session.last_run_id.clone();
    let session_id = session.session_id.clone();
    let run_id = Ulid::new().to_string();
    let binding_kind = if plan.reply_thread_id.as_deref().is_some_and(|value| !value.is_empty()) {
        ConversationBindingKind::Thread
    } else {
        ConversationBindingKind::Main
    };
    let binding_outcome = runtime_state
        .conversation_bindings
        .create_or_touch(ConversationBindingCreateRequest {
            binding_kind,
            channel: plan.channel.clone(),
            conversation_id: input.conversation_id.clone(),
            thread_id: plan.reply_thread_id.clone(),
            sender_identity: plan.sender_identity.clone(),
            principal: route_request_context.principal.clone(),
            session_id: session_id.clone(),
            workspace_id: None,
            policy_scope: policy_resource.clone(),
            parent_binding_id: None,
            lifecycle: ConversationBindingLifecycle::default(),
            now_unix_ms: current_unix_ms(),
        })
        .map_err(|error| {
            Status::internal(format!(
                "conversation binding update failed: {}",
                error.safe_message()
            ))
        })?;
    plan.binding_id = Some(binding_outcome.record.binding_id.clone());
    plan.binding_kind = Some(binding_outcome.record.binding_kind.as_str().to_owned());
    plan.binding_expires_at_unix_ms = binding_outcome.record.expires_at_unix_ms;
    plan.binding_reason = Some(binding_outcome.reason.clone());
    let (input_is_command, input_urgent_stop) =
        match ChannelCommandRegistry::builtin().parse_text(input.text.as_str()) {
            ChannelCommandParseOutcome::Parsed(invocation) => (
                true,
                matches!(
                    invocation.command,
                    ChannelCommandName::Stop | ChannelCommandName::DelegationInterrupt
                ),
            ),
            ChannelCommandParseOutcome::Malformed(_) => (true, false),
            ChannelCommandParseOutcome::NotCommand => (false, false),
        };
    let coalescing_request = InboundCoalescingRequest {
        message_id: input.adapter_message_id.clone().unwrap_or_else(|| input.envelope_id.clone()),
        principal: route_request_context.principal.clone(),
        device_id: route_request_context.device_id.clone(),
        session_id: session_id.clone(),
        policy_scope: policy_resource.clone(),
        binding_id: plan.binding_id.clone(),
        channel: plan.channel.clone(),
        conversation_id: input.conversation_id.clone(),
        thread_id: plan.reply_thread_id.clone().or_else(|| input.adapter_thread_id.clone()),
        sender_identity: plan.sender_identity.clone(),
        text: input.text.clone(),
        received_at_unix_ms: current_unix_ms(),
        has_media: !content.attachments.is_empty(),
        is_command: input_is_command,
        urgent_stop: input_urgent_stop,
    };
    let deferred_coalescing_enabled = runtime_state.inbound_coalescer.policy().active();
    let coalescing_decision = if deferred_coalescing_enabled {
        runtime_state.inbound_coalescer.submit_deferred(coalescing_request)
    } else {
        runtime_state.inbound_coalescer.submit_for_immediate_route(coalescing_request)
    }
    .map_err(|error| {
        Status::resource_exhausted(format!("{}: {}", error.code(), error.safe_message()))
    })?;
    let active_session_run = match previous_run_id_for_context.as_deref() {
        Some(previous_run_id) => runtime_state
            .orchestrator_run_status_snapshot(previous_run_id.to_owned())
            .await?
            .filter(|snapshot| {
                snapshot.session_id == session_id
                    && RunLifecycleState::from_str(snapshot.state.as_str())
                        .is_none_or(|state| !state.is_terminal())
            }),
        None => None,
    };
    let coalescing_run_id =
        active_session_run.as_ref().map_or(run_id.as_str(), |snapshot| snapshot.run_id.as_str());
    let inbound_coalescing_snapshot =
        coalescing_decision.safe_snapshot_json(runtime_state.inbound_coalescer.policy());
    record_inbound_coalescing_journal_event(
        runtime_state,
        &route_request_context,
        session_id.as_str(),
        coalescing_run_id,
        input,
        &plan,
        route_config_hash,
        &coalescing_decision,
    )
    .await;
    if coalescing_decision.kind == InboundCoalescingDecisionKind::Pending {
        // Connector-backed channel ingress persists the envelope and retries
        // this same RouteMessage path; the retry is what flushes a ready
        // bucket with the original route/session/lease context intact.
        return Ok(gateway_v1::RouteMessageResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            accepted: false,
            queued_for_retry: true,
            decision_reason: "inbound_coalescing_pending".to_owned(),
            session_id: Some(common_v1::CanonicalId { ulid: session_id }),
            run_id: None,
            outputs: Vec::new(),
            route_key: plan.route_key.clone(),
            retry_attempt: retry_attempt.saturating_add(1),
            queue_depth: runtime_state.channel_router.queue_depth() as u32,
        });
    }
    if coalescing_decision.kind == InboundCoalescingDecisionKind::Consumed {
        return Ok(gateway_v1::RouteMessageResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            accepted: false,
            queued_for_retry: false,
            decision_reason: "inbound_coalescing_already_dispatched".to_owned(),
            session_id: Some(common_v1::CanonicalId { ulid: session_id }),
            run_id: None,
            outputs: Vec::new(),
            route_key: plan.route_key.clone(),
            retry_attempt,
            queue_depth: runtime_state.channel_router.queue_depth() as u32,
        });
    }
    // When the coalescer merged rapid-fire messages, the merged text becomes
    // the model input; a blank merge falls back to the original message.
    let effective_input_text = coalescing_decision
        .coalesced
        .as_ref()
        .map(|coalesced| coalesced.text.trim().to_owned())
        .filter(|text| !text.is_empty())
        .unwrap_or_else(|| input.text.clone());
    if let Some(active_run) = active_session_run {
        let active_run_id = active_run.run_id;
        let requested_mode =
            if input_urgent_stop { QueueMode::Interrupt } else { QueueMode::Followup };
        let preferred_route_agent_id =
            plan.route_target.as_ref().and_then(|target| target.agent_id.clone());
        let queue_outcome = runtime_state
            .admit_session_queued_input(SessionQueueAdmissionRequest {
                queued_input_id: None,
                session_id: session_id.clone(),
                run_id: active_run_id.clone(),
                origin_run_id: Some(active_run_id.clone()),
                text: effective_input_text,
                requested_mode: Some(requested_mode),
                policy_channel: Some(plan.channel.clone()),
                policy_agent_id: preferred_route_agent_id,
                safe_boundary: SessionQueueSafeBoundary::active(true, false),
                actor_principal: route_request_context.principal.clone(),
                actor_device_id: route_request_context.device_id.clone(),
                actor_channel: Some(plan.channel.clone()),
                source: "route_message.active_session_run".to_owned(),
            })
            .await?;
        let _ = record_message_router_journal_event(
            runtime_state,
            &route_request_context,
            session_id.as_str(),
            active_run_id.as_str(),
            if binding_outcome.created {
                "conversation.binding.created"
            } else {
                "conversation.binding.touched"
            },
            common_v1::journal_event::EventActor::System as i32,
            json!({
                "event": if binding_outcome.created {
                    "conversation.binding.created"
                } else {
                    "conversation.binding.touched"
                },
                "binding": binding_outcome.record.safe_snapshot_json(),
                "reason": binding_outcome.reason,
                "route_key": plan.route_key.clone(),
                "config_hash": route_config_hash,
            }),
        )
        .await;
        let _ = record_message_router_journal_event(
            runtime_state,
            &route_request_context,
            session_id.as_str(),
            active_run_id.as_str(),
            "queued.input",
            common_v1::journal_event::EventActor::User as i32,
            json!({
                "event": "queued.input",
                "envelope_id": input.envelope_id.clone(),
                "channel": input.channel.clone(),
                "route_key": plan.route_key.clone(),
                "binding_id": plan.binding_id.clone(),
                "binding_kind": plan.binding_kind.clone(),
                "queued_input_id": queue_outcome.queued_input.queued_input_id.clone(),
                "queued_input_state": queue_outcome.queued_input.state.clone(),
                "active_run_id": active_run_id.clone(),
                "decision": queue_outcome.decision.decision.as_str(),
                "queue_mode": queue_outcome.decision.mode.as_str(),
                "reason": queue_outcome.decision.reason.clone(),
                "queued_for_retry": false,
                "config_hash": route_config_hash,
                "actor": {
                    "connector_channel": actor_connector,
                    "gateway_principal": actor_gateway_principal,
                    "gateway_device_id": actor_gateway_device_id,
                }
            }),
        )
        .await;
        runtime_state.record_channel_message_routed();
        if queue_outcome.decision.accepted {
            runtime_state.counters.channel_messages_queued.fetch_add(1, Ordering::Relaxed);
        } else {
            runtime_state.counters.channel_messages_rejected.fetch_add(1, Ordering::Relaxed);
        }
        runtime_state.refresh_channel_router_queue_depth();
        return Ok(gateway_v1::RouteMessageResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            accepted: queue_outcome.decision.accepted,
            queued_for_retry: false,
            decision_reason: queue_outcome.decision.reason,
            session_id: Some(common_v1::CanonicalId { ulid: session_id }),
            run_id: Some(common_v1::CanonicalId { ulid: active_run_id }),
            outputs: Vec::new(),
            route_key: plan.route_key.clone(),
            retry_attempt,
            queue_depth: queue_outcome.observed_queue_depth.min(u32::MAX as u64) as u32,
        });
    }
    let typed_session_id = RuntimeSessionId::parse(session_id.as_str()).map_err(|error| {
        Status::failed_precondition(format!(
            "route-message session_id is not a runtime identity: {error}"
        ))
    })?;
    let dispatcher = runtime_state.runtime_kernel_dispatcher();
    let authority_intent = dispatcher
        .resolve_authority_intent(
            &runtime_state.journal_store,
            &typed_session_id,
            Some(route_request_context.principal.as_str()),
            resolved_session.created,
            true,
            RuntimeAuthorityProgressEvidence::pristine(),
        )
        .map_err(|error| {
            Status::failed_precondition(format!(
                "route-message runtime authority could not be resolved: {error}"
            ))
        })?;
    match authority_intent.selected_runtime() {
        Some(RuntimeAuthority::Legacy) => {
            dispatcher
                .pin_non_v2_session_authority(
                    &runtime_state.journal_store,
                    &typed_session_id,
                    &authority_intent,
                )
                .map_err(|error| {
                    Status::failed_precondition(format!(
                        "route-message legacy runtime authority could not be pinned: {error}"
                    ))
                })?;
            runtime_state
                .start_orchestrator_run(OrchestratorRunStartRequest {
                    run_id: run_id.clone(),
                    session_id: session_id.clone(),
                    origin_kind: "manual".to_owned(),
                    origin_run_id: None,
                    triggered_by_principal: Some(route_request_context.principal.clone()),
                    parameter_delta_json: None,
                    delegated_admission: None,
                })
                .await?;
        }
        Some(RuntimeAuthority::V2) => {
            let environment =
                admission_environment(runtime_state, &route_request_context, &session)
                    .await?
                    .with_ingress_block(CHANNEL_V2_ADAPTER_UNAVAILABLE.to_owned());
            let verified = channel_ingress().issue(
                dispatcher,
                AdmissionCaller::authenticated(
                    route_request_context.principal.clone(),
                    route_request_context.device_id.clone(),
                    route_request_context.channel.clone(),
                ),
                environment,
                authority_intent,
                None,
            );
            let outcome = RunAdmissionController::new(&runtime_state.journal_store)
                .admit(RunAdmissionCommand::from_verified(
                    Ulid::new().to_string(),
                    format!("route_message:{session_id}"),
                    format!("route_message:{envelope_id}"),
                    run_id.clone(),
                    run_id.clone(),
                    Ulid::new().to_string(),
                    JournalRunAdmissionSessionSelector {
                        session_id: Some(session_id.clone()),
                        session_key: None,
                        session_label: None,
                        require_existing: true,
                        reset_session: false,
                    },
                    verified,
                ))
                .map_err(|error| {
                    Status::failed_precondition(format!(
                        "route-message runtime admission failed: {error}"
                    ))
                })?;
            let RunAdmissionControllerOutcome::Rejected { journal } = outcome else {
                return Err(Status::failed_precondition(
                    "route-message V2 adapter block did not reject admission",
                ));
            };
            if journal.reason_code != CHANNEL_V2_ADAPTER_UNAVAILABLE {
                return Err(Status::failed_precondition(
                    "route-message V2 adapter block returned unexpected admission evidence",
                ));
            }
            runtime_state.counters.channel_messages_rejected.fetch_add(1, Ordering::Relaxed);
            return Ok(gateway_v1::RouteMessageResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                accepted: false,
                queued_for_retry: false,
                decision_reason: journal.reason_code,
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                run_id: None,
                outputs: Vec::new(),
                route_key: plan.route_key.clone(),
                retry_attempt,
                queue_depth: runtime_state.channel_router.queue_depth() as u32,
            });
        }
        None => {
            runtime_state.counters.channel_messages_rejected.fetch_add(1, Ordering::Relaxed);
            return Ok(gateway_v1::RouteMessageResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                accepted: false,
                queued_for_retry: false,
                decision_reason: "runtime.channel_authority_blocked".to_owned(),
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                run_id: None,
                outputs: Vec::new(),
                route_key: plan.route_key.clone(),
                retry_attempt,
                queue_depth: runtime_state.channel_router.queue_depth() as u32,
            });
        }
    }
    let (generation_session_id, generation) =
        runtime_state.runtime_generation_for_run(run_id.clone()).await?.ok_or_else(|| {
            Status::failed_precondition(
                "route-message run admission did not activate a runtime generation",
            )
        })?;
    if generation_session_id != session_id {
        return Err(Status::failed_precondition(
            "route-message session does not own the admitted runtime generation",
        ));
    }
    let route_flow_control = RunStreamFlowControl::new(
        generation,
        Duration::from_millis(ROUTE_MESSAGE_WALL_CLOCK_BUDGET_MS),
    )?;
    runtime_state
        .update_orchestrator_run_state(run_id.clone(), RunLifecycleState::InProgress, None)
        .await?;
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.clone(),
            seq: 0,
            event_type: "status".to_owned(),
            payload_json: json!({
                "kind": "in_progress",
                "message": "route_message_processing",
                "lifecycle_state": "in_progress",
            })
            .to_string(),
        })
        .await?;
    runtime_state.record_self_healing_heartbeat(WorkHeartbeatUpdate {
        kind: WorkHeartbeatKind::Run,
        object_id: run_id.clone(),
        execution_generation: None,
        summary: format!("route-message run {run_id} in progress"),
    });
    let _ = record_message_router_journal_event(
        runtime_state,
        &route_request_context,
        session_id.as_str(),
        run_id.as_str(),
        if binding_outcome.created {
            "conversation.binding.created"
        } else {
            "conversation.binding.touched"
        },
        common_v1::journal_event::EventActor::System as i32,
        json!({
            "event": if binding_outcome.created {
                "conversation.binding.created"
            } else {
                "conversation.binding.touched"
            },
            "binding": binding_outcome.record.safe_snapshot_json(),
            "reason": binding_outcome.reason,
            "route_key": plan.route_key.clone(),
            "config_hash": route_config_hash,
        }),
    )
    .await;
    runtime_state.record_channel_message_routed();

    let preferred_route_agent_id =
        plan.route_target.as_ref().and_then(|target| target.agent_id.clone());
    // Default agent resolution only enriches journaling/routing metadata.
    // A route-target agent is an explicit policy choice, so resolution must
    // fail closed instead of silently falling back to the default agent.
    let route_agent = match runtime_state
        .resolve_agent_for_context(AgentResolveRequest {
            principal: route_request_context.principal.clone(),
            channel: Some(plan.channel.clone()),
            session_id: Some(session_id.clone()),
            preferred_agent_id: preferred_route_agent_id.clone(),
            persist_session_binding: true,
        })
        .await
    {
        Ok(outcome) => Some(outcome),
        Err(error) if preferred_route_agent_id.is_some() => {
            runtime_state.record_denied();
            runtime_state.counters.channel_messages_rejected.fetch_add(1, Ordering::Relaxed);
            runtime_state.record_channel_reply_failure();
            settle_route_message_run(
                runtime_state,
                run_id.as_str(),
                RunLifecycleState::Failed,
                "route_message.agent_resolution_failed",
                "route_target_agent_resolution_failed",
            )
            .await?;
            let _ = record_message_router_journal_event(
                runtime_state,
                &route_request_context,
                session_id.as_str(),
                run_id.as_str(),
                "message.rejected",
                common_v1::journal_event::EventActor::System as i32,
                json!({
                    "event": "message.rejected",
                    "envelope_id": input.envelope_id.clone(),
                    "channel": input.channel.clone(),
                    "reason": "route_target_agent_resolution_failed",
                    "agent_resolution_error": {
                        "code": format!("{:?}", error.code()),
                        "message": error.message(),
                    },
                    "route_target": plan.route_target.clone(),
                    "queued_for_retry": false,
                    "quarantined": false,
                    "binding_id": plan.binding_id.clone(),
                    "binding_kind": plan.binding_kind.clone(),
                    "binding_expires_at_unix_ms": plan.binding_expires_at_unix_ms,
                    "binding_reason": plan.binding_reason.clone(),
                    "config_hash": route_config_hash,
                    "actor": {
                        "connector_channel": actor_connector,
                        "gateway_principal": actor_gateway_principal,
                        "gateway_device_id": actor_gateway_device_id,
                    }
                }),
            )
            .await;
            return Ok(gateway_v1::RouteMessageResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                accepted: false,
                queued_for_retry: false,
                decision_reason: "route_target_agent_resolution_failed".to_owned(),
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                run_id: Some(common_v1::CanonicalId { ulid: run_id }),
                outputs: Vec::new(),
                route_key: plan.route_key.clone(),
                retry_attempt,
                queue_depth: runtime_state.channel_router.queue_depth() as u32,
            });
        }
        Err(error) => {
            warn!(
                session_id = %session_id,
                run_id = %run_id,
                principal = %route_request_context.principal,
                channel = %plan.channel,
                status_code = ?error.code(),
                status_message = %error.message(),
                "route message agent resolution failed; continuing without agent binding metadata"
            );
            None
        }
    };
    let route_agent_id = route_agent.as_ref().map(|outcome| outcome.agent.agent_id.clone());
    let route_agent_resolution_source = route_agent
        .as_ref()
        .map(|outcome| agent_resolution_source_label(outcome.source).to_owned());

    let _ = record_message_router_journal_event(
        runtime_state,
        &route_request_context,
        session_id.as_str(),
        run_id.as_str(),
        "message.received",
        common_v1::journal_event::EventActor::User as i32,
        json!({
            "event": "message.received",
            "envelope_id": input.envelope_id.clone(),
            "channel": input.channel.clone(),
            "session_key": plan.session_key.clone(),
            "route_key": plan.route_key.clone(),
            "binding_id": plan.binding_id.clone(),
            "binding_kind": plan.binding_kind.clone(),
            "binding_expires_at_unix_ms": plan.binding_expires_at_unix_ms,
            "binding_reason": plan.binding_reason.clone(),
            "inbound_coalescing": inbound_coalescing_snapshot.clone(),
            "json_mode_requested": json_mode_requested,
            "agent_id": route_agent_id.clone(),
            "agent_resolution_source": route_agent_resolution_source.clone(),
            "route_target": plan.route_target.clone(),
            "config_hash": route_config_hash,
            "actor": {
                "connector_channel": actor_connector,
                "gateway_principal": actor_gateway_principal,
                "gateway_device_id": actor_gateway_device_id,
            }
        }),
    )
    .await;

    let mut tape_seq = 1_i64;
    let route_attachment_metadata = content
        .attachments
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
        .collect::<Vec<_>>();
    let route_output_attachments = content.attachments.clone();
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.clone(),
            seq: tape_seq,
            event_type: "message.received".to_owned(),
            payload_json: json!({
                "envelope_id": input.envelope_id.clone(),
                "text": effective_input_text.clone(),
                "channel": input.channel.clone(),
                "route_key": plan.route_key.clone(),
                "binding_id": plan.binding_id.clone(),
                "binding_kind": plan.binding_kind.clone(),
                "binding_expires_at_unix_ms": plan.binding_expires_at_unix_ms,
                "inbound_coalescing": inbound_coalescing_snapshot.clone(),
                "json_mode_requested": json_mode_requested,
                "attachments": route_attachment_metadata.clone(),
                "agent_id": route_agent_id.clone(),
                "agent_resolution_source": route_agent_resolution_source.clone(),
                "route_target": plan.route_target.clone(),
            })
            .to_string(),
        })
        .await?;
    tape_seq = tape_seq.saturating_add(1);

    let routing_scope_kind = if route_agent_id.is_some() { "agent" } else { "session" };
    let routing_scope_id = route_agent_id.as_deref().unwrap_or(session_id.as_str());
    let provider_snapshot = runtime_state.model_provider_status_snapshot();
    let routing_vision_inputs =
        build_provider_image_inputs(content.attachments.as_slice(), &runtime_state.config.media)
            .len();
    let routing_decision = plan_usage_routing(UsageRoutingPlanRequest {
        runtime_state,
        request_context: &route_request_context,
        run_id: run_id.as_str(),
        session_id: session_id.as_str(),
        parameter_delta_json: None,
        prompt_text: effective_input_text.as_str(),
        json_mode: json_mode_requested,
        vision_inputs: routing_vision_inputs,
        scope_kind: routing_scope_kind,
        scope_id: routing_scope_id,
        task_class: RoutingTaskClass::PrimaryInteractive,
        provider_snapshot: &provider_snapshot,
        model_profile_override: None,
    })
    .await?;

    let mut remaining_tool_budget = 0_u32;
    let mut tool_catalog_snapshot = build_and_record_route_tool_catalog_snapshot(
        runtime_state,
        &route_request_context,
        session_id.as_str(),
        run_id.as_str(),
        routing_decision.provider_kind.as_str(),
        Some(routing_decision.actual_model_id.as_str()),
        remaining_tool_budget,
        &mut tape_seq,
    )
    .await?;
    let prepared_provider_input = prepare_model_provider_input(
        runtime_state,
        &route_request_context,
        PrepareModelProviderInputRequest {
            run_id: run_id.as_str(),
            tape_seq: &mut tape_seq,
            session_id: session_id.as_str(),
            previous_run_id: previous_run_id_for_context.as_deref(),
            parameter_delta_json: None,
            input_text: effective_input_text.as_str(),
            channel_turn_envelope: Some(channel_turn_envelope),
            attachments: content.attachments.as_slice(),
            provider_kind_hint: Some(routing_decision.provider_kind.as_str()),
            provider_model_id_hint: Some(routing_decision.actual_model_id.as_str()),
            tool_catalog_snapshot: Some(&tool_catalog_snapshot),
            memory_ingest_reason: "route_message_user_input",
            memory_prompt_failure_mode: MemoryPromptFailureMode::FallbackToRawInput {
                warn_message: "route message memory auto-inject failed; falling back to raw input",
            },
            channel_for_log: plan.channel.as_str(),
        },
    )
    .await?;
    let mut current_prepared_provider_input = prepared_provider_input.clone();
    let mut base_provider_request = prepared_provider_input.into_provider_request(
        effective_input_text.as_str(),
        json_mode_requested,
        (routing_decision.mode == "enforced").then(|| routing_decision.actual_model_id.clone()),
        &tool_catalog_snapshot,
    );

    let mut outbound_lifecycle = OutboundLifecycle::start(OutboundLifecycleStart {
        lifecycle_id: format!("out_{run_id}"),
        channel: plan.channel.clone(),
        run_id: run_id.clone(),
        binding_id: plan.binding_id.clone(),
        capabilities: ChannelOutboundCapabilities::for_channel(
            plan.channel.as_str(),
            input.max_payload_bytes,
        ),
        draft_requested: false,
        typing_requested: false,
        reaction_requested: plan.auto_reaction.as_deref().is_some_and(|value| !value.is_empty()),
        observed_at_unix_ms: current_unix_ms(),
    });

    let mut provider_id = routing_decision.provider_id.clone();
    let mut credential_id = routing_decision.credential_id.clone();
    let mut supersession_retries = 0_u8;
    let mut provider_terminal_state = None;
    let provider_response = loop {
        let provider_attempt = route_flow_control.child(
            CancellationScopeKind::ProviderAttempt,
            Duration::from_millis(ROUTE_MESSAGE_WALL_CLOCK_BUDGET_MS),
        )?;
        let remaining = RunStreamFlowControl::remaining_for_new_work(&provider_attempt)?;
        let provider_request = base_provider_request.clone();
        let mut provider_future = Box::pin(runtime_state.execute_model_provider_with_lease(
            provider_request,
            ProviderLeaseExecutionContext {
                provider_id: provider_id.clone(),
                credential_id: credential_id.clone(),
                priority: RoutingTaskClass::PrimaryInteractive.lease_priority(),
                task_label: RoutingTaskClass::PrimaryInteractive.as_str().to_owned(),
                max_wait_ms: RoutingTaskClass::PrimaryInteractive.max_lease_wait_ms(),
                session_id: Some(session_id.clone()),
                run_id: Some(run_id.clone()),
                runtime_authority: None,
                diagnostic_scope_id: Some(provider_attempt.scope_id.as_str().to_owned()),
            },
        ));
        let provider_deadline = tokio::time::sleep(remaining);
        tokio::pin!(provider_deadline);
        let mut cancel_poll = tokio::time::interval(Duration::from_millis(100));
        cancel_poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let provider_result = loop {
            tokio::select! {
                result = &mut provider_future => break result,
                _ = &mut provider_deadline => {
                    break Err(Status::deadline_exceeded(
                        "route-message provider deadline exceeded",
                    ));
                }
                _ = cancel_poll.tick() => {
                    if runtime_state.is_orchestrator_cancel_requested(run_id.clone()).await? {
                        provider_terminal_state = Some(RunLifecycleState::Cancelled);
                        break Err(Status::cancelled(crate::gateway::CANCELLED_REASON));
                    }
                }
            }
        };
        if provider_terminal_state.is_some() {
            break Err(Status::cancelled(crate::gateway::CANCELLED_REASON));
        }
        match provider_result {
            Ok(response) => break Ok(response),
            Err(error)
                if is_provider_reconfigured_status(&error)
                    && supersession_retries < MAX_ROUTE_PROVIDER_SUPERSESSION_RETRIES =>
            {
                if runtime_state.is_orchestrator_cancel_requested(run_id.clone()).await? {
                    provider_terminal_state = Some(RunLifecycleState::Cancelled);
                    break Err(Status::cancelled(crate::gateway::CANCELLED_REASON));
                }
                supersession_retries = supersession_retries.saturating_add(1);
                let provider_snapshot = runtime_state.model_provider_status_snapshot();
                let replacement_model_id = base_provider_request
                    .model_override
                    .as_deref()
                    .filter(|model_id| {
                        provider_snapshot
                            .registry
                            .models
                            .iter()
                            .any(|model| model.model_id == *model_id && model.enabled)
                    })
                    .map(ToOwned::to_owned)
                    .or_else(|| provider_snapshot.route_selection.selected_model_id.clone())
                    .or_else(|| provider_snapshot.registry.default_chat_model_id.clone())
                    .or_else(|| provider_snapshot.model_id.clone());
                let binding_model_id =
                    replacement_model_id.clone().unwrap_or_else(|| "default".to_owned());
                let replacement_binding = resolve_provider_binding_for_model(
                    &provider_snapshot,
                    binding_model_id.as_str(),
                );
                provider_id = replacement_binding.0;
                let provider_kind = replacement_binding.1;
                credential_id = replacement_binding.2;
                tool_catalog_snapshot = build_and_record_route_tool_catalog_snapshot(
                    runtime_state,
                    &route_request_context,
                    session_id.as_str(),
                    run_id.as_str(),
                    provider_kind.as_str(),
                    replacement_model_id.as_deref(),
                    remaining_tool_budget,
                    &mut tape_seq,
                )
                .await?;
                current_prepared_provider_input = rematerialize_provider_input(
                    &current_prepared_provider_input,
                    &provider_snapshot,
                    provider_kind.as_str(),
                    binding_model_id.as_str(),
                    effective_input_text.as_str(),
                    &tool_catalog_snapshot,
                );
                base_provider_request =
                    current_prepared_provider_input.clone().into_provider_request(
                        effective_input_text.as_str(),
                        json_mode_requested,
                        replacement_model_id,
                        &tool_catalog_snapshot,
                    );
                continue;
            }
            Err(error) => break Err(error),
        }
    };

    let provider_response = match provider_response {
        Ok(response) => response,
        Err(_) if provider_terminal_state.is_some() => {
            let settlement = settle_route_message_run(
                runtime_state,
                run_id.as_str(),
                provider_terminal_state.expect("provider terminal state should be present"),
                "route_message.provider_cancelled",
                crate::gateway::CANCELLED_REASON,
            )
            .await?;
            outbound_lifecycle
                .finalize_failure(crate::gateway::CANCELLED_REASON, current_unix_ms());
            runtime_state.refresh_channel_router_queue_depth();
            return Ok(gateway_v1::RouteMessageResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                accepted: false,
                queued_for_retry: false,
                decision_reason: match settlement.effective_state {
                    RunLifecycleState::Cancelled => "cancelled_by_request".to_owned(),
                    RunLifecycleState::Failed => "route_message_failed".to_owned(),
                    RunLifecycleState::Done => "already_completed".to_owned(),
                    RunLifecycleState::Pending
                    | RunLifecycleState::Accepted
                    | RunLifecycleState::InProgress => {
                        return Err(Status::internal(
                            "route-message terminal settlement returned a nonterminal state",
                        ));
                    }
                },
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                run_id: Some(common_v1::CanonicalId { ulid: run_id }),
                outputs: Vec::new(),
                route_key: plan.route_key.clone(),
                retry_attempt,
                queue_depth: runtime_state.channel_router.queue_depth() as u32,
            });
        }
        Err(error) if is_provider_reconfigured_status(&error) => {
            let error_message = error.message().to_owned();
            outbound_lifecycle.finalize_failure(error_message.as_str(), current_unix_ms());
            runtime_state.counters.channel_messages_rejected.fetch_add(1, Ordering::Relaxed);
            runtime_state.record_channel_reply_failure();
            settle_route_message_run(
                runtime_state,
                run_id.as_str(),
                RunLifecycleState::Failed,
                "route_message.provider_supersession_exhausted",
                error_message.as_str(),
            )
            .await?;
            let _ = record_message_router_journal_event(
                runtime_state,
                &route_request_context,
                session_id.as_str(),
                run_id.as_str(),
                "message.rejected",
                common_v1::journal_event::EventActor::System as i32,
                json!({
                    "event": "message.rejected",
                    "envelope_id": input.envelope_id.clone(),
                    "channel": input.channel.clone(),
                    "reason": error_message,
                    "reason_code": "runtime.generation.provider_reconfiguration_exhausted",
                    "provider_supersession_retries": supersession_retries,
                    "queued_for_retry": false,
                    "quarantined": false,
                    "binding_id": plan.binding_id.clone(),
                    "binding_kind": plan.binding_kind.clone(),
                    "outbound_lifecycle": outbound_lifecycle.safe_snapshot_json(),
                    "config_hash": route_config_hash,
                    "actor": {
                        "connector_channel": actor_connector,
                        "gateway_principal": actor_gateway_principal,
                        "gateway_device_id": actor_gateway_device_id,
                    }
                }),
            )
            .await;
            runtime_state.refresh_channel_router_queue_depth();
            return Ok(gateway_v1::RouteMessageResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                accepted: false,
                queued_for_retry: false,
                decision_reason: "model_provider_superseded".to_owned(),
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                run_id: Some(common_v1::CanonicalId { ulid: run_id }),
                outputs: Vec::new(),
                route_key: plan.route_key.clone(),
                retry_attempt,
                queue_depth: runtime_state.channel_router.queue_depth() as u32,
            });
        }
        Err(error) => {
            // Provider failure is a retryable outcome, not an RPC error: the
            // router decides whether the envelope is re-queued, quarantined,
            // or dropped, and the response reports that disposition.
            let error_message = error.message().to_owned();
            outbound_lifecycle.finalize_failure(error_message.as_str(), current_unix_ms());
            let outbound_lifecycle_snapshot = outbound_lifecycle.safe_snapshot_json();
            let retry_disposition =
                runtime_state.channel_router.record_processing_failure(input, "provider_error");
            match retry_disposition {
                RetryDisposition::Queued => {
                    runtime_state.counters.channel_messages_queued.fetch_add(1, Ordering::Relaxed);
                }
                RetryDisposition::Quarantined => {
                    runtime_state
                        .counters
                        .channel_messages_quarantined
                        .fetch_add(1, Ordering::Relaxed);
                }
                RetryDisposition::Dropped => {
                    warn!(
                        envelope_id = %input.envelope_id,
                        channel = %input.channel,
                        "channel router dropped failed message after quarantine persistence failure"
                    );
                }
            }
            runtime_state.counters.channel_messages_rejected.fetch_add(1, Ordering::Relaxed);
            runtime_state.record_channel_reply_failure();
            settle_route_message_run(
                runtime_state,
                run_id.as_str(),
                RunLifecycleState::Failed,
                "route_message.provider_failed",
                error_message.as_str(),
            )
            .await?;
            let _ = record_message_router_journal_event(
                runtime_state,
                &route_request_context,
                session_id.as_str(),
                run_id.as_str(),
                "message.rejected",
                common_v1::journal_event::EventActor::System as i32,
                json!({
                    "event": "message.rejected",
                    "envelope_id": input.envelope_id.clone(),
                    "channel": input.channel.clone(),
                    "reason": error_message,
                    "retry_disposition": match retry_disposition {
                        RetryDisposition::Queued => "queued",
                        RetryDisposition::Quarantined => "quarantined",
                        RetryDisposition::Dropped => "dropped",
                    },
                    "queued_for_retry": matches!(retry_disposition, RetryDisposition::Queued),
                    "quarantined": matches!(retry_disposition, RetryDisposition::Quarantined),
                    "binding_id": plan.binding_id.clone(),
                    "binding_kind": plan.binding_kind.clone(),
                    "outbound_lifecycle": outbound_lifecycle_snapshot,
                    "config_hash": route_config_hash,
                    "actor": {
                        "connector_channel": actor_connector,
                        "gateway_principal": actor_gateway_principal,
                        "gateway_device_id": actor_gateway_device_id,
                    }
                }),
            )
            .await;
            runtime_state.refresh_channel_router_queue_depth();
            return Ok(gateway_v1::RouteMessageResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                accepted: false,
                queued_for_retry: matches!(retry_disposition, RetryDisposition::Queued),
                decision_reason: "model_provider_failed".to_owned(),
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                run_id: Some(common_v1::CanonicalId { ulid: run_id }),
                outputs: Vec::new(),
                route_key: plan.route_key.clone(),
                retry_attempt: retry_attempt.saturating_add(1),
                queue_depth: runtime_state.channel_router.queue_depth() as u32,
            });
        }
    };

    let route_provider_response = match process_route_provider_response(
        runtime_state,
        &route_request_context,
        session_id.as_str(),
        run_id.as_str(),
        provider_response,
        &tool_catalog_snapshot,
        json_mode_requested,
        plan.response_prefix.as_deref(),
        &route_flow_control,
        &mut remaining_tool_budget,
        &mut tape_seq,
    )
    .await
    {
        Ok(RouteProviderResponseProcessingOutcome::Completed(outcome)) => outcome,
        Ok(RouteProviderResponseProcessingOutcome::Terminal {
            state,
            prompt_tokens,
            completion_tokens,
        }) => {
            runtime_state
                .add_orchestrator_usage(OrchestratorUsageDelta {
                    run_id: run_id.clone(),
                    prompt_tokens_delta: prompt_tokens,
                    completion_tokens_delta: completion_tokens,
                })
                .await?;
            let terminal_message = match state {
                RunLifecycleState::Cancelled => crate::gateway::CANCELLED_REASON,
                RunLifecycleState::Failed => "route-message provider event failed",
                RunLifecycleState::Done => "route-message provider event completed",
                RunLifecycleState::Pending
                | RunLifecycleState::Accepted
                | RunLifecycleState::InProgress => {
                    return Err(Status::internal(
                        "route-message provider event returned a nonterminal state",
                    ));
                }
            };
            let settlement = settle_route_message_run(
                runtime_state,
                run_id.as_str(),
                state,
                "route_message.provider_event_terminal",
                terminal_message,
            )
            .await?;
            outbound_lifecycle.finalize_failure(terminal_message, current_unix_ms());
            runtime_state.refresh_channel_router_queue_depth();
            return Ok(gateway_v1::RouteMessageResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                accepted: false,
                queued_for_retry: false,
                decision_reason: match settlement.effective_state {
                    RunLifecycleState::Cancelled => "cancelled_by_request".to_owned(),
                    RunLifecycleState::Failed => "route_message_failed".to_owned(),
                    RunLifecycleState::Done => "already_completed".to_owned(),
                    RunLifecycleState::Pending
                    | RunLifecycleState::Accepted
                    | RunLifecycleState::InProgress => {
                        return Err(Status::internal(
                            "route-message terminal settlement returned a nonterminal state",
                        ));
                    }
                },
                session_id: Some(common_v1::CanonicalId { ulid: session_id }),
                run_id: Some(common_v1::CanonicalId { ulid: run_id }),
                outputs: Vec::new(),
                route_key: plan.route_key.clone(),
                retry_attempt,
                queue_depth: runtime_state.channel_router.queue_depth() as u32,
            });
        }
        Err(error) => {
            outbound_lifecycle.finalize_failure(error.message(), current_unix_ms());
            settle_route_message_run(
                runtime_state,
                run_id.as_str(),
                RunLifecycleState::Failed,
                "route_message.response_processing_failed",
                error.message(),
            )
            .await?;
            let _ = record_message_router_journal_event(
                runtime_state,
                &route_request_context,
                session_id.as_str(),
                run_id.as_str(),
                "message.rejected",
                common_v1::journal_event::EventActor::System as i32,
                json!({
                    "event": "message.rejected",
                    "envelope_id": input.envelope_id.clone(),
                    "channel": input.channel.clone(),
                    "reason": error.message(),
                    "queued_for_retry": false,
                    "quarantined": false,
                    "binding_id": plan.binding_id.clone(),
                    "binding_kind": plan.binding_kind.clone(),
                    "outbound_lifecycle": outbound_lifecycle.safe_snapshot_json(),
                    "config_hash": route_config_hash,
                    "actor": {
                        "connector_channel": actor_connector,
                        "gateway_principal": actor_gateway_principal,
                        "gateway_device_id": actor_gateway_device_id,
                    }
                }),
            )
            .await;
            return Err(error);
        }
    };
    let reply_text = route_provider_response.reply_text;
    let route_structured_output = route_provider_response.structured_output;
    if runtime_state.is_orchestrator_cancel_requested(run_id.clone()).await? {
        let settlement = settle_route_message_run(
            runtime_state,
            run_id.as_str(),
            RunLifecycleState::Cancelled,
            "route_message.cancelled_before_delivery",
            crate::gateway::CANCELLED_REASON,
        )
        .await?;
        outbound_lifecycle.finalize_failure(crate::gateway::CANCELLED_REASON, current_unix_ms());
        runtime_state.refresh_channel_router_queue_depth();
        return Ok(gateway_v1::RouteMessageResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            accepted: false,
            queued_for_retry: false,
            decision_reason: match settlement.effective_state {
                RunLifecycleState::Cancelled => "cancelled_by_request".to_owned(),
                RunLifecycleState::Failed => "route_message_failed".to_owned(),
                RunLifecycleState::Done => "already_completed".to_owned(),
                RunLifecycleState::Pending
                | RunLifecycleState::Accepted
                | RunLifecycleState::InProgress => {
                    return Err(Status::internal(
                        "route-message terminal settlement returned a nonterminal state",
                    ));
                }
            },
            session_id: Some(common_v1::CanonicalId { ulid: session_id }),
            run_id: Some(common_v1::CanonicalId { ulid: run_id }),
            outputs: Vec::new(),
            route_key: plan.route_key.clone(),
            retry_attempt,
            queue_depth: runtime_state.channel_router.queue_depth() as u32,
        });
    }
    // Second policy gate: intake authorized receiving the message, but the
    // outbound send is re-checked closest to dispatch (now with session/run
    // context) so a policy change or session-scoped rule landing during
    // generation still blocks delivery.
    if let Err(error) = authorize_message_action(
        route_request_context.principal.as_str(),
        "channel.send",
        policy_resource.as_str(),
        Some(plan.channel.as_str()),
        Some(session_id.as_str()),
        Some(run_id.as_str()),
    ) {
        runtime_state.record_denied();
        runtime_state.counters.channel_messages_rejected.fetch_add(1, Ordering::Relaxed);
        runtime_state.record_channel_reply_failure();
        outbound_lifecycle.finalize_failure(error.message(), current_unix_ms());
        settle_route_message_run(
            runtime_state,
            run_id.as_str(),
            RunLifecycleState::Failed,
            "route_message.outbound_denied",
            error.message(),
        )
        .await?;
        let _ = record_message_router_journal_event(
            runtime_state,
            &route_request_context,
            session_id.as_str(),
            run_id.as_str(),
            "message.rejected",
            common_v1::journal_event::EventActor::System as i32,
            json!({
                "event": "message.rejected",
                "envelope_id": envelope_id,
                "channel": plan.channel.clone(),
                "reason": error.message(),
                "policy_action": "channel.send",
                "queued_for_retry": false,
                "quarantined": false,
                "binding_id": plan.binding_id.clone(),
                "binding_kind": plan.binding_kind.clone(),
                "outbound_lifecycle": outbound_lifecycle.safe_snapshot_json(),
                "config_hash": route_config_hash,
                "actor": {
                    "connector_channel": actor_connector,
                    "gateway_principal": actor_gateway_principal,
                    "gateway_device_id": actor_gateway_device_id,
                }
            }),
        )
        .await;
        return Ok(gateway_v1::RouteMessageResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            accepted: false,
            queued_for_retry: false,
            decision_reason: error.message().to_owned(),
            session_id: Some(common_v1::CanonicalId { ulid: session_id }),
            run_id: Some(common_v1::CanonicalId { ulid: run_id }),
            outputs: Vec::new(),
            route_key: plan.route_key.clone(),
            retry_attempt,
            queue_depth: runtime_state.channel_router.queue_depth() as u32,
        });
    }

    runtime_state
        .add_orchestrator_usage(OrchestratorUsageDelta {
            run_id: run_id.clone(),
            prompt_tokens_delta: route_provider_response.prompt_tokens,
            completion_tokens_delta: route_provider_response.completion_tokens,
        })
        .await?;

    if runtime_state.is_orchestrator_cancel_requested(run_id.clone()).await? {
        let settlement = settle_route_message_run(
            runtime_state,
            run_id.as_str(),
            RunLifecycleState::Cancelled,
            "route_message.cancelled_before_projection",
            crate::gateway::CANCELLED_REASON,
        )
        .await?;
        outbound_lifecycle.finalize_failure(crate::gateway::CANCELLED_REASON, current_unix_ms());
        runtime_state.refresh_channel_router_queue_depth();
        return Ok(gateway_v1::RouteMessageResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            accepted: false,
            queued_for_retry: false,
            decision_reason: match settlement.effective_state {
                RunLifecycleState::Cancelled => "cancelled_by_request".to_owned(),
                RunLifecycleState::Failed => "route_message_failed".to_owned(),
                RunLifecycleState::Done => "already_completed".to_owned(),
                RunLifecycleState::Pending
                | RunLifecycleState::Accepted
                | RunLifecycleState::InProgress => {
                    return Err(Status::internal(
                        "route-message terminal settlement returned a nonterminal state",
                    ));
                }
            },
            session_id: Some(common_v1::CanonicalId { ulid: session_id }),
            run_id: Some(common_v1::CanonicalId { ulid: run_id }),
            outputs: Vec::new(),
            route_key: plan.route_key.clone(),
            retry_attempt,
            queue_depth: runtime_state.channel_router.queue_depth() as u32,
        });
    }

    ingest_memory_best_effort(
        runtime_state,
        route_request_context.principal.as_str(),
        route_request_context.channel.as_deref(),
        Some(session_id.as_str()),
        MemorySource::Summary,
        reply_text.as_str(),
        vec!["summary:route_message".to_owned()],
        Some(0.75),
        "route_message_model_summary",
    )
    .await;
    outbound_lifecycle.finalize_success(current_unix_ms());
    let outbound_lifecycle_snapshot = outbound_lifecycle.safe_snapshot_json();
    let route_delivery_policy = resolve_delivery_policy(
        &runtime_state.config.delivery_arbitration,
        None,
        None,
        Some(plan.channel.as_str()),
    );
    let route_delivery_metadata = crate::runtime_preview_controls::capability_active(
        &runtime_state.config,
        RuntimePreviewCapability::DeliveryArbitration,
    )
    .then(|| {
        json!({
            "policy": route_delivery_policy.snapshot_json(),
            "decision": "deliver_interim_parent",
            "reason": "route_message_channel_default",
            "outbound_lifecycle": outbound_lifecycle_snapshot.clone(),
        })
    });
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.clone(),
            seq: tape_seq,
            event_type: "message.replied".to_owned(),
            payload_json: json!({
                "reply_text": reply_text.clone(),
                "route_key": plan.route_key.clone(),
                "json_mode_requested": json_mode_requested,
                "structured_output_present": !route_structured_output.structured_json.is_empty(),
                "a2ui_surface": route_structured_output
                    .a2ui_update
                    .as_ref()
                    .map(|value| value.surface.clone()),
                "attachments": route_attachment_metadata.clone(),
                "agent_id": route_agent_id.clone(),
                "agent_resolution_source": route_agent_resolution_source.clone(),
                "route_target": plan.route_target.clone(),
                "binding_id": plan.binding_id.clone(),
                "binding_kind": plan.binding_kind.clone(),
                "binding_expires_at_unix_ms": plan.binding_expires_at_unix_ms,
                "delivery_policy": route_delivery_policy.snapshot_json(),
                "outbound_lifecycle": outbound_lifecycle_snapshot.clone(),
            })
            .to_string(),
        })
        .await?;
    let settlement = settle_route_message_run(
        runtime_state,
        run_id.as_str(),
        RunLifecycleState::Done,
        RuntimeTerminalOutcome::Completed.reason_code(),
        "completed",
    )
    .await?;
    if settlement.effective_state != RunLifecycleState::Done {
        outbound_lifecycle.finalize_failure(crate::gateway::CANCELLED_REASON, current_unix_ms());
        runtime_state.refresh_channel_router_queue_depth();
        return Ok(gateway_v1::RouteMessageResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            accepted: false,
            queued_for_retry: false,
            decision_reason: match settlement.effective_state {
                RunLifecycleState::Cancelled => "cancelled_by_request".to_owned(),
                RunLifecycleState::Failed => "route_message_failed".to_owned(),
                RunLifecycleState::Done => unreachable!("done handled above"),
                RunLifecycleState::Pending
                | RunLifecycleState::Accepted
                | RunLifecycleState::InProgress => {
                    return Err(Status::internal(
                        "route-message terminal settlement returned a nonterminal state",
                    ));
                }
            },
            session_id: Some(common_v1::CanonicalId { ulid: session_id }),
            run_id: Some(common_v1::CanonicalId { ulid: run_id }),
            outputs: Vec::new(),
            route_key: plan.route_key.clone(),
            retry_attempt,
            queue_depth: runtime_state.channel_router.queue_depth() as u32,
        });
    }

    let _ = record_message_router_journal_event(
        runtime_state,
        &route_request_context,
        session_id.as_str(),
        run_id.as_str(),
        "message.routed",
        common_v1::journal_event::EventActor::System as i32,
        json!({
            "event": "message.routed",
            "envelope_id": envelope_id,
            "channel": plan.channel.clone(),
            "route_key": plan.route_key.clone(),
            "session_id": session_id.clone(),
            "run_id": run_id.clone(),
            "agent_id": route_agent_id.clone(),
            "agent_resolution_source": route_agent_resolution_source.clone(),
            "route_target": plan.route_target.clone(),
            "binding_id": plan.binding_id.clone(),
            "binding_kind": plan.binding_kind.clone(),
            "binding_expires_at_unix_ms": plan.binding_expires_at_unix_ms,
            "outbound_lifecycle": outbound_lifecycle_snapshot.clone(),
            "broadcast": plan.is_broadcast,
            "queued_for_retry": false,
            "quarantined": false,
            "config_hash": route_config_hash,
            "actor": {
                "connector_channel": actor_connector,
                "gateway_principal": actor_gateway_principal,
                "gateway_device_id": actor_gateway_device_id,
            }
        }),
    )
    .await;
    let _ = record_message_router_journal_event(
        runtime_state,
        &route_request_context,
        session_id.as_str(),
        run_id.as_str(),
        "message.replied",
        common_v1::journal_event::EventActor::System as i32,
        json!({
            "event": "message.replied",
            "envelope_id": envelope_id,
            "channel": plan.channel.clone(),
            "reply_preview": truncate_with_ellipsis(reply_text.clone(), 256),
            "json_mode_requested": json_mode_requested,
            "structured_output_present": !route_structured_output.structured_json.is_empty(),
            "a2ui_surface": route_structured_output
                .a2ui_update
                .as_ref()
                .map(|value| value.surface.clone()),
            "attachments": route_attachment_metadata,
            "agent_id": route_agent_id,
            "agent_resolution_source": route_agent_resolution_source,
            "route_target": plan.route_target.clone(),
            "binding_id": plan.binding_id.clone(),
            "binding_kind": plan.binding_kind.clone(),
            "binding_expires_at_unix_ms": plan.binding_expires_at_unix_ms,
            "delivery_policy": route_delivery_policy.snapshot_json(),
            "outbound_lifecycle": outbound_lifecycle_snapshot,
            "config_hash": route_config_hash,
            "actor": {
                "connector_channel": actor_connector,
                "gateway_principal": actor_gateway_principal,
                "gateway_device_id": actor_gateway_device_id,
            }
        }),
    )
    .await;

    if let Some(binding_id) = plan.binding_id.as_deref() {
        if let Err(error) = runtime_state.conversation_bindings.touch(binding_id, current_unix_ms())
        {
            warn!(
                binding_id,
                status_message = %error.safe_message(),
                "failed to touch conversation binding after channel reply"
            );
        }
    }
    runtime_state.record_channel_message_replied();
    runtime_state.refresh_channel_router_queue_depth();
    let route_output_template = RouteMessageOutputTemplate {
        thread_id: plan.reply_thread_id.as_deref().unwrap_or_default(),
        in_reply_to_message_id: plan.in_reply_to_message_id.as_deref().unwrap_or_default(),
        broadcast: plan.is_broadcast,
        auto_ack_text: plan.auto_ack_text.as_deref().unwrap_or_default(),
        auto_reaction: plan.auto_reaction.as_deref().unwrap_or_default(),
        attachments: route_output_attachments.as_slice(),
        structured_json: route_structured_output.structured_json.as_slice(),
        a2ui_update: route_structured_output.a2ui_update.as_ref(),
        delivery_metadata: route_delivery_metadata.as_ref(),
    };
    let route_outputs = build_route_message_outputs(
        reply_text.as_str(),
        input.max_payload_bytes,
        &route_output_template,
    );
    Ok(gateway_v1::RouteMessageResponse {
        v: CANONICAL_PROTOCOL_MAJOR,
        accepted: true,
        queued_for_retry: false,
        decision_reason: "routed".to_owned(),
        session_id: Some(common_v1::CanonicalId { ulid: session_id }),
        run_id: Some(common_v1::CanonicalId { ulid: run_id }),
        outputs: route_outputs,
        route_key: plan.route_key.clone(),
        retry_attempt,
        queue_depth: runtime_state.channel_router.queue_depth() as u32,
    })
}

#[cfg(test)]
mod tests;
