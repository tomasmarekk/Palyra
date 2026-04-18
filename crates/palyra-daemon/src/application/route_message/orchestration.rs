use std::sync::{atomic::Ordering, Arc};

use palyra_common::CANONICAL_PROTOCOL_MAJOR;
use serde_json::json;
use tonic::Status;
use tracing::warn;
use ulid::Ulid;

use crate::{
    agents::AgentResolveRequest,
    application::provider_input::{
        prepare_model_provider_input, MemoryPromptFailureMode, PrepareModelProviderInputRequest,
    },
    application::service_authorization::authorize_message_action,
    channel_router::{
        InboundMessage as ChannelInboundMessage, RetryDisposition, RoutePlan as ChannelRoutePlan,
    },
    gateway::{
        agent_resolution_source_label, ingest_memory_best_effort,
        record_message_router_journal_event, request_context_with_resolved_route_channel,
        truncate_with_ellipsis, GatewayRuntimeState,
    },
    journal::{
        MemorySource, OrchestratorRunStartRequest, OrchestratorSessionResolveRequest,
        OrchestratorTapeAppendRequest, OrchestratorUsageDelta,
    },
    model_provider::ProviderRequest,
    orchestrator::RunLifecycleState,
    provider_leases::ProviderLeaseExecutionContext,
    transport::grpc::{
        auth::RequestContext,
        proto::palyra::{common::v1 as common_v1, gateway::v1 as gateway_v1},
    },
    usage_governance::{plan_usage_routing, RoutingTaskClass, UsageRoutingPlanRequest},
};

use super::response::{
    build_route_message_outputs, process_route_provider_response, RouteMessageOutputTemplate,
};

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
    retry_attempt: u32,
) -> Result<gateway_v1::RouteMessageResponse, Status> {
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
        let journal_session_id = Ulid::new().to_string();
        let journal_run_id = Ulid::new().to_string();
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
                .or(plan.session_label.clone()),
            principal: route_request_context.principal.clone(),
            device_id: route_request_context.device_id.clone(),
            channel: Some(plan.channel.clone()),
            require_existing: false,
            reset_session: false,
        })
        .await?;
    let previous_run_id_for_context = resolved_session.session.last_run_id.clone();
    let session_id = resolved_session.session.session_id;
    let run_id = Ulid::new().to_string();
    runtime_state
        .start_orchestrator_run(OrchestratorRunStartRequest {
            run_id: run_id.clone(),
            session_id: session_id.clone(),
            origin_kind: "manual".to_owned(),
            origin_run_id: None,
            triggered_by_principal: Some(route_request_context.principal.clone()),
            parameter_delta_json: None,
        })
        .await?;
    runtime_state
        .update_orchestrator_run_state(run_id.clone(), RunLifecycleState::InProgress, None)
        .await?;
    runtime_state.record_channel_message_routed();

    let route_agent = match runtime_state
        .resolve_agent_for_context(AgentResolveRequest {
            principal: route_request_context.principal.clone(),
            channel: Some(plan.channel.clone()),
            session_id: Some(session_id.clone()),
            preferred_agent_id: None,
            persist_session_binding: true,
        })
        .await
    {
        Ok(outcome) => Some(outcome),
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
            "json_mode_requested": json_mode_requested,
            "agent_id": route_agent_id.clone(),
            "agent_resolution_source": route_agent_resolution_source.clone(),
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
                "text": input.text.clone(),
                "channel": input.channel.clone(),
                "route_key": plan.route_key.clone(),
                "json_mode_requested": json_mode_requested,
                "attachments": route_attachment_metadata.clone(),
                "agent_id": route_agent_id.clone(),
                "agent_resolution_source": route_agent_resolution_source.clone(),
            })
            .to_string(),
        })
        .await?;
    tape_seq = tape_seq.saturating_add(1);

    let prepared_provider_input = prepare_model_provider_input(
        runtime_state,
        &route_request_context,
        PrepareModelProviderInputRequest {
            run_id: run_id.as_str(),
            tape_seq: &mut tape_seq,
            session_id: session_id.as_str(),
            previous_run_id: previous_run_id_for_context.as_deref(),
            parameter_delta_json: None,
            input_text: input.text.as_str(),
            attachments: content.attachments.as_slice(),
            memory_ingest_reason: "route_message_user_input",
            memory_prompt_failure_mode: MemoryPromptFailureMode::FallbackToRawInput {
                warn_message: "route message memory auto-inject failed; falling back to raw input",
            },
            channel_for_log: plan.channel.as_str(),
        },
    )
    .await?;

    let routing_scope_kind = if route_agent_id.is_some() { "agent" } else { "session" };
    let routing_scope_id = route_agent_id.as_deref().unwrap_or(session_id.as_str());
    let routing_decision = plan_usage_routing(UsageRoutingPlanRequest {
        runtime_state,
        request_context: &route_request_context,
        run_id: run_id.as_str(),
        session_id: session_id.as_str(),
        parameter_delta_json: None,
        prompt_text: input.text.as_str(),
        json_mode: json_mode_requested,
        vision_inputs: prepared_provider_input.vision_inputs.len(),
        scope_kind: routing_scope_kind,
        scope_id: routing_scope_id,
        task_class: RoutingTaskClass::PrimaryInteractive,
        provider_snapshot: &runtime_state.model_provider_status_snapshot(),
    })
    .await?;

    let provider_response = runtime_state
        .execute_model_provider_with_lease(
            ProviderRequest {
                input_text: prepared_provider_input.provider_input_text,
                json_mode: json_mode_requested,
                vision_inputs: prepared_provider_input.vision_inputs,
                model_override: (routing_decision.mode == "enforced")
                    .then(|| routing_decision.actual_model_id.clone()),
            },
            ProviderLeaseExecutionContext {
                provider_id: routing_decision.provider_id.clone(),
                credential_id: routing_decision.credential_id.clone(),
                priority: RoutingTaskClass::PrimaryInteractive.lease_priority(),
                task_label: RoutingTaskClass::PrimaryInteractive.as_str().to_owned(),
                max_wait_ms: RoutingTaskClass::PrimaryInteractive.max_lease_wait_ms(),
                session_id: Some(session_id.clone()),
                run_id: Some(run_id.clone()),
            },
        )
        .await;

    let provider_response = match provider_response {
        Ok(response) => response,
        Err(error) => {
            let error_message = error.message().to_owned();
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
            runtime_state
                .update_orchestrator_run_state(
                    run_id.clone(),
                    RunLifecycleState::Failed,
                    Some(error_message.clone()),
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

    let mut remaining_tool_budget = runtime_state.config.tool_call.max_calls_per_run;
    let route_provider_response = process_route_provider_response(
        runtime_state,
        &route_request_context,
        session_id.as_str(),
        run_id.as_str(),
        provider_response,
        json_mode_requested,
        plan.response_prefix.as_deref(),
        &mut remaining_tool_budget,
        &mut tape_seq,
    )
    .await?;
    let reply_text = route_provider_response.reply_text;
    let route_structured_output = route_provider_response.structured_output;
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
        runtime_state
            .update_orchestrator_run_state(
                run_id.clone(),
                RunLifecycleState::Failed,
                Some(error.message().to_owned()),
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
            })
            .to_string(),
        })
        .await?;
    runtime_state
        .update_orchestrator_run_state(run_id.clone(), RunLifecycleState::Done, None)
        .await?;

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
            "config_hash": route_config_hash,
            "actor": {
                "connector_channel": actor_connector,
                "gateway_principal": actor_gateway_principal,
                "gateway_device_id": actor_gateway_device_id,
            }
        }),
    )
    .await;

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
