use std::sync::{atomic::Ordering, Arc};

use palyra_common::{validate_canonical_id, CANONICAL_PROTOCOL_MAJOR};
use serde_json::json;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{metadata::MetadataMap, Request, Response, Status, Streaming};
use tracing::{info, warn};
use ulid::Ulid;

use crate::{
    agents::{
        AgentBindingQuery, AgentBindingRequest, AgentCreateRequest, AgentResolveRequest,
        AgentUnbindRequest,
    },
    application::{
        route_message::orchestration::handle_routed_route_message,
        run_stream::orchestration::{
            finalize_run_stream_after_provider_response, process_run_stream_message,
            RunStreamMessageProcessingOutcome, RunStreamPostProviderOutcome,
        },
        service_authorization::authorize_agent_management_action,
    },
    channel_router::{
        InboundMessage as ChannelInboundMessage, PairingConsumeOutcome, RouteOutcome,
        RoutedMessage as ChannelRoutedMessage,
    },
    execution_backends::{
        build_execution_backend_inventory_with_worker_state,
        parse_optional_execution_backend_preference, resolve_execution_backend,
        validate_execution_backend_selection,
    },
    gateway::{
        agent_binding_message, agent_message, agent_resolution_source_to_proto, canonical_id,
        execution_backend_inventory_message, extract_pairing_code_command, finalize_run_failure,
        non_empty, normalize_agent_identifier, optional_canonical_id, record_agent_journal_event,
        record_message_router_journal_event, require_supported_version,
        security_requests_json_mode, session_summary_message, GatewayRuntimeState,
        ListOrchestratorSessionsRequest, RunFailureFinalization, APPROVAL_PROMPT_TIMEOUT_SECONDS,
        SENSITIVE_TOOLS_DENY_REASON,
    },
    journal::{
        ApprovalCreateRequest, ApprovalDecisionScope, ApprovalPolicySnapshot, ApprovalPromptOption,
        ApprovalPromptRecord, ApprovalRiskLevel, ApprovalSubjectType, JournalAppendRequest,
        OrchestratorCancelRequest, OrchestratorSessionCleanupRequest,
        OrchestratorSessionResolveRequest,
    },
    node_runtime::NodeRuntimeState,
    orchestrator::RunStateMachine,
    transport::grpc::{
        auth::{authorize_metadata, GatewayAuthConfig, RequestContext},
        proto::palyra::{common::v1 as common_v1, gateway::v1 as gateway_v1},
    },
};

#[derive(Clone)]
pub struct GatewayServiceImpl {
    state: Arc<GatewayRuntimeState>,
    auth: GatewayAuthConfig,
    node_runtime: Arc<NodeRuntimeState>,
}

impl GatewayServiceImpl {
    #[must_use]
    pub fn new(
        state: Arc<GatewayRuntimeState>,
        auth: GatewayAuthConfig,
        node_runtime: Arc<NodeRuntimeState>,
    ) -> Self {
        Self { state, auth, node_runtime }
    }

    #[allow(clippy::result_large_err)]
    fn authorize_rpc(
        &self,
        metadata: &MetadataMap,
        method: &'static str,
    ) -> Result<RequestContext, Status> {
        authorize_metadata(metadata, &self.auth, method).map_err(|error| {
            self.state.record_denied();
            warn!(method, error = %error, "gateway rpc authorization denied");
            Status::permission_denied(error.to_string())
        })
    }

    fn execution_backend_inventory(
        &self,
    ) -> Result<Vec<crate::execution_backends::ExecutionBackendInventoryRecord>, Status> {
        let now_unix_ms = crate::gateway::current_unix_ms_status()?;
        let nodes = self.node_runtime.nodes()?;
        Ok(build_execution_backend_inventory_with_worker_state(
            &self.state.config.tool_call.process_runner,
            nodes.as_slice(),
            now_unix_ms,
            &self.state.config.feature_rollouts,
            &self.state.config.networked_workers,
            self.state.worker_fleet_snapshot(),
            &self.state.worker_fleet_policy(),
        ))
    }
}

#[tonic::async_trait]
impl gateway_v1::gateway_service_server::GatewayService for GatewayServiceImpl {
    type RunStreamStream = ReceiverStream<Result<common_v1::RunStreamEvent, Status>>;

    async fn get_health(
        &self,
        _request: Request<gateway_v1::HealthRequest>,
    ) -> Result<Response<gateway_v1::HealthResponse>, Status> {
        Ok(Response::new(gateway_v1::HealthResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            service: "palyrad".to_owned(),
            status: "ok".to_owned(),
            version: self.state.build.version.clone(),
            git_hash: self.state.build.git_hash.clone(),
            build_profile: self.state.build.build_profile.clone(),
            uptime_seconds: self.state.started_at.elapsed().as_secs(),
        }))
    }

    async fn append_event(
        &self,
        request: Request<gateway_v1::AppendEventRequest>,
    ) -> Result<Response<gateway_v1::AppendEventResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "AppendEvent")?;
        self.state.counters.append_event_requests.fetch_add(1, Ordering::Relaxed);

        let payload = request.into_inner();
        if payload.v != CANONICAL_PROTOCOL_MAJOR {
            return Err(Status::failed_precondition("unsupported protocol major version"));
        }
        let event = payload.event.ok_or_else(|| Status::invalid_argument("event is required"))?;
        if event.v != CANONICAL_PROTOCOL_MAJOR {
            return Err(Status::failed_precondition(
                "event uses an unsupported protocol major version",
            ));
        }
        let event_id = if let Some(id) = event.event_id.and_then(|value| non_empty(value.ulid)) {
            validate_canonical_id(&id)
                .map_err(|_| Status::invalid_argument("event.event_id must be a canonical ULID"))?;
            id
        } else {
            Ulid::new().to_string()
        };
        let session_id = canonical_id(event.session_id, "event.session_id")?;
        let run_id = canonical_id(event.run_id, "event.run_id")?;
        if event.timestamp_unix_ms <= 0 {
            return Err(Status::invalid_argument(
                "event.timestamp_unix_ms must be a unix timestamp",
            ));
        }
        if event.kind == common_v1::journal_event::EventKind::Unspecified as i32 {
            return Err(Status::invalid_argument("event.kind must be specified"));
        }
        if event.actor == common_v1::journal_event::EventActor::Unspecified as i32 {
            return Err(Status::invalid_argument("event.actor must be specified"));
        }

        let journal_outcome = self
            .state
            .record_journal_event(JournalAppendRequest {
                event_id: event_id.clone(),
                session_id,
                run_id,
                kind: event.kind,
                actor: event.actor,
                timestamp_unix_ms: event.timestamp_unix_ms,
                payload_json: event.payload_json,
                principal: context.principal.clone(),
                device_id: context.device_id.clone(),
                channel: context.channel.clone(),
            })
            .await?;

        info!(
            method = "AppendEvent",
            principal = %context.principal,
            device_id = %context.device_id,
            channel = context.channel.as_deref().unwrap_or("n/a"),
            event_id = %event_id,
            redacted_payload = journal_outcome.redacted,
            hash_chain_enabled = self.state.journal_config.hash_chain_enabled,
            write_duration_ms = journal_outcome.write_duration.as_millis(),
            event_hash = journal_outcome.hash.as_deref().unwrap_or("disabled"),
            "gateway event appended"
        );

        Ok(Response::new(gateway_v1::AppendEventResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            event_id: Some(common_v1::CanonicalId { ulid: event_id }),
            accepted: true,
        }))
    }

    async fn abort_run(
        &self,
        request: Request<gateway_v1::AbortRunRequest>,
    ) -> Result<Response<gateway_v1::AbortRunResponse>, Status> {
        let _context = self.authorize_rpc(request.metadata(), "AbortRun")?;
        let payload = request.into_inner();
        if payload.v != CANONICAL_PROTOCOL_MAJOR {
            return Err(Status::failed_precondition("unsupported protocol major version"));
        }
        let run_id = canonical_id(payload.run_id, "run_id")?;
        let reason = non_empty(payload.reason).unwrap_or_else(|| "grpc_abort_requested".to_owned());
        let snapshot = self
            .state
            .request_orchestrator_cancel(OrchestratorCancelRequest {
                run_id: run_id.clone(),
                reason: reason.clone(),
            })
            .await?;
        Ok(Response::new(gateway_v1::AbortRunResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            run_id: Some(common_v1::CanonicalId { ulid: snapshot.run_id }),
            cancel_requested: snapshot.cancel_requested,
            reason: snapshot.reason,
        }))
    }

    async fn list_sessions(
        &self,
        request: Request<gateway_v1::ListSessionsRequest>,
    ) -> Result<Response<gateway_v1::ListSessionsResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "ListSessions")?;
        let payload = request.into_inner();
        if payload.v != CANONICAL_PROTOCOL_MAJOR {
            return Err(Status::failed_precondition("unsupported protocol major version"));
        }
        let after_session_key = non_empty(payload.after_session_key);
        let requested_limit = if payload.limit == 0 { None } else { Some(payload.limit as usize) };
        let (sessions, next_after_session_key) = self
            .state
            .list_orchestrator_sessions(ListOrchestratorSessionsRequest {
                after_session_key,
                principal: context.principal.clone(),
                device_id: context.device_id.clone(),
                channel: context.channel.clone(),
                include_archived: payload.include_archived,
                requested_limit,
                search_query: non_empty(payload.q),
            })
            .await?;
        Ok(Response::new(gateway_v1::ListSessionsResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            sessions: sessions.iter().map(session_summary_message).collect(),
            next_after_session_key: next_after_session_key.unwrap_or_default(),
        }))
    }

    async fn resolve_session(
        &self,
        request: Request<gateway_v1::ResolveSessionRequest>,
    ) -> Result<Response<gateway_v1::ResolveSessionResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "ResolveSession")?;
        let payload = request.into_inner();
        if payload.v != CANONICAL_PROTOCOL_MAJOR {
            return Err(Status::failed_precondition("unsupported protocol major version"));
        }
        let session_id = optional_canonical_id(payload.session_id, "session_id")?;
        let session_key = non_empty(payload.session_key);
        let session_label = non_empty(payload.session_label);
        let outcome = self
            .state
            .resolve_orchestrator_session(OrchestratorSessionResolveRequest {
                session_id,
                session_key,
                session_label,
                principal: context.principal.clone(),
                device_id: context.device_id.clone(),
                channel: context.channel.clone(),
                require_existing: payload.require_existing,
                reset_session: payload.reset_session,
            })
            .await?;
        if outcome.reset_applied {
            self.state.clear_tool_approval_cache_for_session(
                &context,
                outcome.session.session_id.as_str(),
            );
        }
        Ok(Response::new(gateway_v1::ResolveSessionResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            session: Some(session_summary_message(&outcome.session)),
            created: outcome.created,
            reset_applied: outcome.reset_applied,
        }))
    }

    async fn cleanup_session(
        &self,
        request: Request<gateway_v1::CleanupSessionRequest>,
    ) -> Result<Response<gateway_v1::CleanupSessionResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "CleanupSession")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        let session_id = optional_canonical_id(payload.session_id, "session_id")?;
        let session_key = non_empty(payload.session_key);
        let outcome = self
            .state
            .cleanup_orchestrator_session(OrchestratorSessionCleanupRequest {
                session_id,
                session_key,
                principal: context.principal.clone(),
                device_id: context.device_id.clone(),
                channel: context.channel.clone(),
            })
            .await?;
        self.state
            .clear_tool_approval_cache_for_session(&context, outcome.session.session_id.as_str());
        let _ = record_agent_journal_event(
            &self.state,
            &context,
            json!({
                "event": "session.cleaned",
                "session_id": outcome.session.session_id,
                "new_session_key": outcome.session.session_key,
                "previous_session_key": outcome.previous_session_key,
                "archived_at_unix_ms": outcome.session.archived_at_unix_ms,
                "run_count": outcome.run_count,
                "cleaned": outcome.cleaned,
                "newly_archived": outcome.newly_archived,
            }),
        )
        .await;
        Ok(Response::new(gateway_v1::CleanupSessionResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            session: Some(session_summary_message(&outcome.session)),
            cleaned: outcome.cleaned,
            newly_archived: outcome.newly_archived,
            previous_session_key: outcome.previous_session_key,
            run_count: outcome.run_count as u32,
        }))
    }

    async fn route_message(
        &self,
        request: Request<gateway_v1::RouteMessageRequest>,
    ) -> Result<Response<gateway_v1::RouteMessageResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "RouteMessage")?;
        let payload = request.into_inner();
        let retry_attempt = payload.retry_attempt;
        let requested_session_label = non_empty(payload.session_label.clone());
        require_supported_version(payload.v)?;
        let envelope =
            payload.envelope.ok_or_else(|| Status::invalid_argument("envelope is required"))?;
        if envelope.v != CANONICAL_PROTOCOL_MAJOR {
            return Err(Status::failed_precondition(
                "envelope uses an unsupported protocol major version",
            ));
        }
        let json_mode_requested = security_requests_json_mode(envelope.security.as_ref());
        let origin = envelope.origin.unwrap_or_default();
        let content = envelope.content.unwrap_or_default();
        let channel = if let Some(value) = non_empty(origin.channel.clone()) {
            value
        } else if let Some(value) = context.channel.clone() {
            value
        } else {
            return Err(Status::invalid_argument(
                "route message requires origin.channel or authenticated channel context",
            ));
        };
        if let Some(context_channel) = context.channel.as_deref() {
            if !context_channel.eq_ignore_ascii_case(channel.as_str()) {
                self.state.record_denied();
                return Err(Status::permission_denied(
                    "authenticated channel context does not match message channel",
                ));
            }
        }
        let envelope_id = if let Some(value) = envelope.envelope_id {
            validate_canonical_id(value.ulid.as_str()).map_err(|_| {
                Status::invalid_argument("envelope.envelope_id must be a canonical ULID")
            })?;
            value.ulid
        } else {
            Ulid::new().to_string()
        };
        let input = ChannelInboundMessage {
            envelope_id: envelope_id.clone(),
            channel: channel.clone(),
            conversation_id: non_empty(origin.conversation_id),
            sender_handle: non_empty(origin.sender_handle),
            sender_display: non_empty(origin.sender_display),
            sender_verified: origin.sender_verified,
            text: content.text.clone(),
            max_payload_bytes: envelope.max_payload_bytes,
            is_direct_message: payload.is_direct_message,
            requested_broadcast: payload.request_broadcast,
            adapter_message_id: non_empty(payload.adapter_message_id),
            adapter_thread_id: non_empty(payload.adapter_thread_id),
            retry_attempt,
        };
        self.state.counters.channel_messages_inbound.fetch_add(1, Ordering::Relaxed);
        let route_config_hash = self.state.channel_router_config_hash();
        let actor_connector = input.channel.clone();
        let actor_gateway_principal = context.principal.clone();
        let actor_gateway_device_id = context.device_id.clone();

        if input.is_direct_message {
            if let Some(pairing_code) = extract_pairing_code_command(input.text.as_str()) {
                let pairing_result = self.state.channel_router_consume_pairing_code(
                    input.channel.as_str(),
                    input.sender_handle.as_deref(),
                    pairing_code.as_str(),
                    None,
                );
                match pairing_result {
                    PairingConsumeOutcome::Pending(pending) => {
                        let session_id = Ulid::new().to_string();
                        let run_id = Ulid::new().to_string();
                        let approval_record = self
                            .state
                            .create_approval_record(ApprovalCreateRequest {
                                approval_id: Ulid::new().to_string(),
                                session_id: session_id.clone(),
                                run_id: run_id.clone(),
                                principal: context.principal.clone(),
                                device_id: context.device_id.clone(),
                                channel: Some(input.channel.clone()),
                                subject_type: ApprovalSubjectType::ChannelSend,
                                subject_id: format!(
                                    "dm_pairing:{}:{}",
                                    pending.channel, pending.sender_identity
                                ),
                                request_summary: format!(
                                    "Approve DM pairing for sender '{}' on channel '{}'",
                                    pending.sender_identity, pending.channel
                                ),
                                policy_snapshot: ApprovalPolicySnapshot {
                                    policy_id: "channel_router.dm_pairing.v1".to_owned(),
                                    policy_hash: route_config_hash.clone(),
                                    evaluation_summary:
                                        "direct_message_policy=pairing approval_required=true"
                                            .to_owned(),
                                },
                                prompt: ApprovalPromptRecord {
                                    title: "Approve DM pairing request".to_owned(),
                                    risk_level: ApprovalRiskLevel::Medium,
                                    subject_id: format!(
                                        "dm_pairing:{}:{}",
                                        pending.channel, pending.sender_identity
                                    ),
                                    summary: format!(
                                        "Sender '{}' requested DM pairing for '{}'",
                                        pending.sender_identity, pending.channel
                                    ),
                                    options: vec![
                                        ApprovalPromptOption {
                                            option_id: "allow_session".to_owned(),
                                            label: "Allow session".to_owned(),
                                            description:
                                                "Allow direct messages for the current operator session"
                                                    .to_owned(),
                                            default_selected: true,
                                            decision_scope: ApprovalDecisionScope::Session,
                                            timebox_ttl_ms: None,
                                        },
                                        ApprovalPromptOption {
                                            option_id: "allow_8h".to_owned(),
                                            label: "Allow 8 hours".to_owned(),
                                            description:
                                                "Approve DM pairing for a limited 8-hour window"
                                                    .to_owned(),
                                            default_selected: false,
                                            decision_scope: ApprovalDecisionScope::Timeboxed,
                                            timebox_ttl_ms: Some(8 * 60 * 60 * 1_000),
                                        },
                                        ApprovalPromptOption {
                                            option_id: "deny".to_owned(),
                                            label: "Deny".to_owned(),
                                            description:
                                                "Reject this pairing request and keep DM blocked"
                                                    .to_owned(),
                                            default_selected: false,
                                            decision_scope: ApprovalDecisionScope::Once,
                                            timebox_ttl_ms: None,
                                        },
                                    ],
                                    timeout_seconds: APPROVAL_PROMPT_TIMEOUT_SECONDS,
                                    details_json: json!({
                                        "channel": pending.channel,
                                        "sender_identity": pending.sender_identity,
                                        "pairing_code": pending.code,
                                        "expires_at_unix_ms": pending.expires_at_unix_ms,
                                    })
                                    .to_string(),
                                    policy_explanation:
                                        "DM pairing requires explicit operator approval before routing."
                                            .to_owned(),
                                },
                            })
                            .await?;
                        let attached = self.state.channel_router_attach_pairing_pending_approval(
                            pending.channel.as_str(),
                            pending.sender_identity.as_str(),
                            approval_record.approval_id.as_str(),
                        );
                        if !attached {
                            return Err(Status::internal(
                                "failed to attach DM pairing approval state",
                            ));
                        }
                        self.state
                            .counters
                            .channel_messages_rejected
                            .fetch_add(1, Ordering::Relaxed);
                        self.state.counters.channel_router_queue_depth.store(
                            self.state.channel_router.queue_depth() as u64,
                            Ordering::Relaxed,
                        );
                        let _ = record_message_router_journal_event(
                            &self.state,
                            &context,
                            session_id.as_str(),
                            run_id.as_str(),
                            "message.received",
                            common_v1::journal_event::EventActor::User as i32,
                            json!({
                                "event": "message.received",
                                "envelope_id": input.envelope_id.clone(),
                                "channel": input.channel.clone(),
                                "requested_broadcast": input.requested_broadcast,
                                "is_direct_message": input.is_direct_message,
                                "config_hash": route_config_hash.clone(),
                                "actor": {
                                    "connector_channel": actor_connector.clone(),
                                    "gateway_principal": actor_gateway_principal.clone(),
                                    "gateway_device_id": actor_gateway_device_id.clone(),
                                }
                            }),
                        )
                        .await;
                        let _ = record_message_router_journal_event(
                            &self.state,
                            &context,
                            session_id.as_str(),
                            run_id.as_str(),
                            "message.rejected",
                            common_v1::journal_event::EventActor::System as i32,
                            json!({
                                "event": "message.rejected",
                                "envelope_id": input.envelope_id.clone(),
                                "channel": input.channel.clone(),
                                "reason": "direct_message_pairing_pending_approval",
                                "approval_id": approval_record.approval_id,
                                "queued_for_retry": false,
                                "quarantined": false,
                                "config_hash": route_config_hash.clone(),
                                "actor": {
                                    "connector_channel": actor_connector.clone(),
                                    "gateway_principal": actor_gateway_principal.clone(),
                                    "gateway_device_id": actor_gateway_device_id.clone(),
                                }
                            }),
                        )
                        .await;
                        return Ok(Response::new(gateway_v1::RouteMessageResponse {
                            v: CANONICAL_PROTOCOL_MAJOR,
                            accepted: false,
                            queued_for_retry: false,
                            decision_reason: "direct_message_pairing_pending_approval".to_owned(),
                            session_id: None,
                            run_id: None,
                            outputs: Vec::new(),
                            route_key: String::new(),
                            retry_attempt,
                            queue_depth: self.state.channel_router.queue_depth() as u32,
                        }));
                    }
                    PairingConsumeOutcome::Rejected(reason) => {
                        self.state
                            .counters
                            .channel_messages_rejected
                            .fetch_add(1, Ordering::Relaxed);
                        self.state.counters.channel_router_queue_depth.store(
                            self.state.channel_router.queue_depth() as u64,
                            Ordering::Relaxed,
                        );
                        let session_id = Ulid::new().to_string();
                        let run_id = Ulid::new().to_string();
                        let reason_label = reason.as_str().to_owned();
                        let _ = record_message_router_journal_event(
                            &self.state,
                            &context,
                            session_id.as_str(),
                            run_id.as_str(),
                            "message.received",
                            common_v1::journal_event::EventActor::User as i32,
                            json!({
                                "event": "message.received",
                                "envelope_id": input.envelope_id.clone(),
                                "channel": input.channel.clone(),
                                "requested_broadcast": input.requested_broadcast,
                                "is_direct_message": input.is_direct_message,
                                "config_hash": route_config_hash.clone(),
                                "actor": {
                                    "connector_channel": actor_connector.clone(),
                                    "gateway_principal": actor_gateway_principal.clone(),
                                    "gateway_device_id": actor_gateway_device_id.clone(),
                                }
                            }),
                        )
                        .await;
                        let _ = record_message_router_journal_event(
                            &self.state,
                            &context,
                            session_id.as_str(),
                            run_id.as_str(),
                            "message.rejected",
                            common_v1::journal_event::EventActor::System as i32,
                            json!({
                                "event": "message.rejected",
                                "envelope_id": input.envelope_id.clone(),
                                "channel": input.channel.clone(),
                                "reason": reason_label.clone(),
                                "queued_for_retry": false,
                                "quarantined": false,
                                "config_hash": route_config_hash.clone(),
                                "actor": {
                                    "connector_channel": actor_connector.clone(),
                                    "gateway_principal": actor_gateway_principal.clone(),
                                    "gateway_device_id": actor_gateway_device_id.clone(),
                                }
                            }),
                        )
                        .await;
                        return Ok(Response::new(gateway_v1::RouteMessageResponse {
                            v: CANONICAL_PROTOCOL_MAJOR,
                            accepted: false,
                            queued_for_retry: false,
                            decision_reason: reason_label,
                            session_id: None,
                            run_id: None,
                            outputs: Vec::new(),
                            route_key: String::new(),
                            retry_attempt,
                            queue_depth: self.state.channel_router.queue_depth() as u32,
                        }));
                    }
                }
            }
        }

        match self.state.channel_router.begin_route(&input) {
            RouteOutcome::Rejected(rejection) => {
                let rejection_reason = rejection.reason.clone();
                self.state.counters.channel_messages_rejected.fetch_add(1, Ordering::Relaxed);
                if rejection.quarantined {
                    self.state
                        .counters
                        .channel_messages_quarantined
                        .fetch_add(1, Ordering::Relaxed);
                }
                self.state
                    .counters
                    .channel_router_queue_depth
                    .store(self.state.channel_router.queue_depth() as u64, Ordering::Relaxed);
                let journal_session_id = Ulid::new().to_string();
                let journal_run_id = Ulid::new().to_string();
                let _ = record_message_router_journal_event(
                    &self.state,
                    &context,
                    journal_session_id.as_str(),
                    journal_run_id.as_str(),
                    "message.received",
                    common_v1::journal_event::EventActor::User as i32,
                    json!({
                        "event": "message.received",
                        "envelope_id": envelope_id,
                        "channel": channel,
                        "requested_broadcast": input.requested_broadcast,
                        "is_direct_message": input.is_direct_message,
                        "config_hash": route_config_hash.clone(),
                        "actor": {
                            "connector_channel": actor_connector.clone(),
                            "gateway_principal": actor_gateway_principal.clone(),
                            "gateway_device_id": actor_gateway_device_id.clone(),
                        }
                    }),
                )
                .await;
                let _ = record_message_router_journal_event(
                    &self.state,
                    &context,
                    journal_session_id.as_str(),
                    journal_run_id.as_str(),
                    "message.rejected",
                    common_v1::journal_event::EventActor::System as i32,
                    json!({
                        "event": "message.rejected",
                        "envelope_id": input.envelope_id.clone(),
                        "channel": input.channel.clone(),
                        "reason": rejection_reason.clone(),
                        "queued_for_retry": false,
                        "quarantined": rejection.quarantined,
                        "config_hash": route_config_hash.clone(),
                        "actor": {
                            "connector_channel": actor_connector.clone(),
                            "gateway_principal": actor_gateway_principal.clone(),
                            "gateway_device_id": actor_gateway_device_id.clone(),
                        }
                    }),
                )
                .await;
                return Ok(Response::new(gateway_v1::RouteMessageResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    accepted: false,
                    queued_for_retry: false,
                    decision_reason: rejection_reason,
                    session_id: None,
                    run_id: None,
                    outputs: Vec::new(),
                    route_key: String::new(),
                    retry_attempt,
                    queue_depth: self.state.channel_router.queue_depth() as u32,
                }));
            }
            RouteOutcome::Queued(queued) => {
                let queue_reason = queued.reason.clone();
                self.state.counters.channel_messages_queued.fetch_add(1, Ordering::Relaxed);
                self.state
                    .counters
                    .channel_router_queue_depth
                    .store(self.state.channel_router.queue_depth() as u64, Ordering::Relaxed);
                let journal_session_id = Ulid::new().to_string();
                let journal_run_id = Ulid::new().to_string();
                let _ = record_message_router_journal_event(
                    &self.state,
                    &context,
                    journal_session_id.as_str(),
                    journal_run_id.as_str(),
                    "message.received",
                    common_v1::journal_event::EventActor::User as i32,
                    json!({
                        "event": "message.received",
                        "envelope_id": input.envelope_id.clone(),
                        "channel": input.channel.clone(),
                        "requested_broadcast": input.requested_broadcast,
                        "is_direct_message": input.is_direct_message,
                        "config_hash": route_config_hash.clone(),
                        "actor": {
                            "connector_channel": actor_connector.clone(),
                            "gateway_principal": actor_gateway_principal.clone(),
                            "gateway_device_id": actor_gateway_device_id.clone(),
                        }
                    }),
                )
                .await;
                let _ = record_message_router_journal_event(
                    &self.state,
                    &context,
                    journal_session_id.as_str(),
                    journal_run_id.as_str(),
                    "message.rejected",
                    common_v1::journal_event::EventActor::System as i32,
                    json!({
                        "event": "message.rejected",
                        "envelope_id": input.envelope_id.clone(),
                        "channel": input.channel.clone(),
                        "reason": queue_reason.clone(),
                        "queued_for_retry": true,
                        "quarantined": false,
                        "retry_after_ms": queued.retry_after_ms,
                        "config_hash": route_config_hash.clone(),
                        "actor": {
                            "connector_channel": actor_connector.clone(),
                            "gateway_principal": actor_gateway_principal.clone(),
                            "gateway_device_id": actor_gateway_device_id.clone(),
                        }
                    }),
                )
                .await;
                return Ok(Response::new(gateway_v1::RouteMessageResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    accepted: false,
                    queued_for_retry: true,
                    decision_reason: queue_reason,
                    session_id: None,
                    run_id: None,
                    outputs: Vec::new(),
                    route_key: String::new(),
                    retry_attempt: retry_attempt.saturating_add(1),
                    queue_depth: queued.queue_depth as u32,
                }));
            }
            RouteOutcome::Routed(routed) => {
                let ChannelRoutedMessage { plan, lease: route_lease } = *routed;
                let response = handle_routed_route_message(
                    &self.state,
                    &context,
                    &input,
                    &content,
                    &plan,
                    requested_session_label.as_deref(),
                    json_mode_requested,
                    envelope_id.as_str(),
                    route_config_hash.as_str(),
                    actor_connector.as_str(),
                    actor_gateway_principal.as_str(),
                    actor_gateway_device_id.as_str(),
                    retry_attempt,
                )
                .await?;
                drop(route_lease);
                return Ok(Response::new(response));
            }
        }
    }

    async fn list_agents(
        &self,
        request: Request<gateway_v1::ListAgentsRequest>,
    ) -> Result<Response<gateway_v1::ListAgentsResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "ListAgents")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        authorize_agent_management_action(
            context.principal.as_str(),
            "agent.list",
            "agent:registry",
        )
        .inspect_err(|_error| {
            self.state.record_denied();
        })?;
        let page = self
            .state
            .list_agents(non_empty(payload.after_agent_id), Some(payload.limit as usize))
            .await?;
        let inventory = self.execution_backend_inventory()?;
        Ok(Response::new(gateway_v1::ListAgentsResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            agents: page.agents.iter().map(agent_message).collect(),
            default_agent_id: page.default_agent_id.unwrap_or_default(),
            next_after_agent_id: page.next_after_agent_id.unwrap_or_default(),
            execution_backends: inventory.iter().map(execution_backend_inventory_message).collect(),
        }))
    }

    async fn get_agent(
        &self,
        request: Request<gateway_v1::GetAgentRequest>,
    ) -> Result<Response<gateway_v1::GetAgentResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "GetAgent")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        authorize_agent_management_action(
            context.principal.as_str(),
            "agent.get",
            "agent:registry",
        )
        .inspect_err(|_error| {
            self.state.record_denied();
        })?;
        let agent_id = normalize_agent_identifier(payload.agent_id.as_str(), "agent_id")
            .inspect_err(|_error| {
                self.state.counters.agent_validation_failures.fetch_add(1, Ordering::Relaxed);
            })?;
        let (agent, is_default) = self.state.get_agent(agent_id).await?;
        let inventory = self.execution_backend_inventory()?;
        let resolution = resolve_execution_backend(agent.execution_backend_preference, &inventory);
        Ok(Response::new(gateway_v1::GetAgentResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            agent: Some(agent_message(&agent)),
            is_default,
            execution_backends: inventory.iter().map(execution_backend_inventory_message).collect(),
            resolved_execution_backend: resolution.resolved.as_str().to_owned(),
            execution_backend_fallback_used: resolution.fallback_used,
            execution_backend_reason: resolution.reason,
        }))
    }

    async fn create_agent(
        &self,
        request: Request<gateway_v1::CreateAgentRequest>,
    ) -> Result<Response<gateway_v1::CreateAgentResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "CreateAgent")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        authorize_agent_management_action(
            context.principal.as_str(),
            "agent.create",
            "agent:registry",
        )
        .inspect_err(|_error| {
            self.state.record_denied();
        })?;
        let inventory = self.execution_backend_inventory()?;
        let execution_backend_preference = parse_optional_execution_backend_preference(
            non_empty(payload.execution_backend_preference).as_deref(),
            "execution_backend_preference",
        )
        .map_err(Status::invalid_argument)?;
        if let Some(preference) = execution_backend_preference {
            validate_execution_backend_selection(preference, &inventory)
                .map_err(Status::failed_precondition)?;
        }
        let outcome = self
            .state
            .create_agent(AgentCreateRequest {
                agent_id: payload.agent_id,
                display_name: payload.display_name,
                agent_dir: non_empty(payload.agent_dir),
                workspace_roots: payload.workspace_roots,
                default_model_profile: non_empty(payload.default_model_profile),
                execution_backend_preference,
                default_tool_allowlist: payload.default_tool_allowlist,
                default_skill_allowlist: payload.default_skill_allowlist,
                set_default: payload.set_default,
                allow_absolute_paths: payload.allow_absolute_paths,
            })
            .await?;
        let journal_payload = json!({
            "event": "agent.created",
            "agent_id": outcome.agent.agent_id,
            "display_name": outcome.agent.display_name,
            "agent_dir": outcome.agent.agent_dir,
            "workspace_roots": outcome.agent.workspace_roots,
            "default_model_profile": outcome.agent.default_model_profile,
            "default_changed": outcome.default_changed,
            "default_agent_id": outcome.default_agent_id,
        });
        let _ = record_agent_journal_event(&self.state, &context, journal_payload).await;
        if outcome.default_changed {
            let _ = record_agent_journal_event(
                &self.state,
                &context,
                json!({
                    "event": "agent.default_changed",
                    "previous_default_agent_id": outcome.previous_default_agent_id,
                    "default_agent_id": outcome.default_agent_id,
                }),
            )
            .await;
        }
        let resolution =
            resolve_execution_backend(outcome.agent.execution_backend_preference, &inventory);
        Ok(Response::new(gateway_v1::CreateAgentResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            agent: Some(agent_message(&outcome.agent)),
            default_changed: outcome.default_changed,
            default_agent_id: outcome.default_agent_id.unwrap_or_default(),
            execution_backends: inventory.iter().map(execution_backend_inventory_message).collect(),
            resolved_execution_backend: resolution.resolved.as_str().to_owned(),
            execution_backend_fallback_used: resolution.fallback_used,
            execution_backend_reason: resolution.reason,
        }))
    }

    async fn delete_agent(
        &self,
        request: Request<gateway_v1::DeleteAgentRequest>,
    ) -> Result<Response<gateway_v1::DeleteAgentResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "DeleteAgent")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        authorize_agent_management_action(
            context.principal.as_str(),
            "agent.delete",
            "agent:registry",
        )
        .inspect_err(|_error| {
            self.state.record_denied();
        })?;
        let agent_id = normalize_agent_identifier(payload.agent_id.as_str(), "agent_id")
            .inspect_err(|_error| {
                self.state.counters.agent_validation_failures.fetch_add(1, Ordering::Relaxed);
            })?;
        let outcome = self.state.delete_agent(agent_id.to_owned()).await?;
        let _ = record_agent_journal_event(
            &self.state,
            &context,
            json!({
                "event": "agent.deleted",
                "agent_id": outcome.deleted_agent_id,
                "agent_dir": outcome.agent_dir,
                "removed_bindings_count": outcome.removed_bindings_count,
                "previous_default_agent_id": outcome.previous_default_agent_id,
                "default_agent_id": outcome.default_agent_id,
            }),
        )
        .await;
        Ok(Response::new(gateway_v1::DeleteAgentResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            deleted_agent_id: outcome.deleted_agent_id,
            deleted: outcome.deleted,
            removed_bindings_count: outcome.removed_bindings_count as u32,
            previous_default_agent_id: outcome.previous_default_agent_id.unwrap_or_default(),
            default_agent_id: outcome.default_agent_id.unwrap_or_default(),
            agent_dir: outcome.agent_dir,
        }))
    }

    async fn set_default_agent(
        &self,
        request: Request<gateway_v1::SetDefaultAgentRequest>,
    ) -> Result<Response<gateway_v1::SetDefaultAgentResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "SetDefaultAgent")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        authorize_agent_management_action(
            context.principal.as_str(),
            "agent.set_default",
            "agent:registry",
        )
        .inspect_err(|_error| {
            self.state.record_denied();
        })?;
        let agent_id = normalize_agent_identifier(payload.agent_id.as_str(), "agent_id")
            .inspect_err(|_error| {
                self.state.counters.agent_validation_failures.fetch_add(1, Ordering::Relaxed);
            })?;
        let outcome = self.state.set_default_agent(agent_id).await?;
        let _ = record_agent_journal_event(
            &self.state,
            &context,
            json!({
                "event": "agent.default_changed",
                "previous_default_agent_id": outcome.previous_default_agent_id,
                "default_agent_id": outcome.default_agent_id,
            }),
        )
        .await;
        Ok(Response::new(gateway_v1::SetDefaultAgentResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            previous_agent_id: outcome.previous_default_agent_id.unwrap_or_default(),
            default_agent_id: outcome.default_agent_id,
        }))
    }

    async fn list_agent_bindings(
        &self,
        request: Request<gateway_v1::ListAgentBindingsRequest>,
    ) -> Result<Response<gateway_v1::ListAgentBindingsResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "ListAgentBindings")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        authorize_agent_management_action(
            context.principal.as_str(),
            "agent.bindings",
            "agent:registry",
        )
        .inspect_err(|_error| {
            self.state.record_denied();
        })?;
        let bindings =
            self.state
                .list_agent_bindings(AgentBindingQuery {
                    agent_id: non_empty(payload.agent_id),
                    principal: non_empty(payload.principal),
                    channel: non_empty(payload.channel),
                    session_id: optional_canonical_id(payload.session_id, "session_id")
                        .inspect_err(|_error| {
                            self.state
                                .counters
                                .agent_validation_failures
                                .fetch_add(1, Ordering::Relaxed);
                        })?,
                    limit: if payload.limit == 0 { None } else { Some(payload.limit as usize) },
                })
                .await?;
        Ok(Response::new(gateway_v1::ListAgentBindingsResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            bindings: bindings.iter().map(agent_binding_message).collect(),
        }))
    }

    async fn bind_agent_for_context(
        &self,
        request: Request<gateway_v1::BindAgentForContextRequest>,
    ) -> Result<Response<gateway_v1::BindAgentForContextResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "BindAgentForContext")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        authorize_agent_management_action(
            context.principal.as_str(),
            "agent.bind",
            "agent:registry",
        )
        .inspect_err(|_error| {
            self.state.record_denied();
        })?;
        let principal = if let Some(value) = non_empty(payload.principal) {
            if value != context.principal {
                self.state.record_denied();
                return Err(Status::permission_denied(
                    "bind agent principal must match authenticated principal",
                ));
            }
            value
        } else {
            context.principal.clone()
        };
        let agent_id = normalize_agent_identifier(payload.agent_id.as_str(), "agent_id")
            .inspect_err(|_error| {
                self.state.counters.agent_validation_failures.fetch_add(1, Ordering::Relaxed);
            })?;
        let session_id = optional_canonical_id(payload.session_id, "session_id")
            .inspect_err(|_error| {
                self.state.counters.agent_validation_failures.fetch_add(1, Ordering::Relaxed);
            })?
            .ok_or_else(|| Status::invalid_argument("session_id is required"))?;
        let outcome = self
            .state
            .bind_agent_for_context(AgentBindingRequest {
                agent_id: agent_id.to_owned(),
                principal,
                channel: non_empty(payload.channel),
                session_id,
            })
            .await?;
        let _ = record_agent_journal_event(
            &self.state,
            &context,
            json!({
                "event": "agent.binding_upserted",
                "agent_id": outcome.binding.agent_id,
                "session_id": outcome.binding.session_id,
                "channel": outcome.binding.channel,
                "created": outcome.created,
            }),
        )
        .await;
        Ok(Response::new(gateway_v1::BindAgentForContextResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            binding: Some(agent_binding_message(&outcome.binding)),
            created: outcome.created,
        }))
    }

    async fn unbind_agent_for_context(
        &self,
        request: Request<gateway_v1::UnbindAgentForContextRequest>,
    ) -> Result<Response<gateway_v1::UnbindAgentForContextResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "UnbindAgentForContext")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        authorize_agent_management_action(
            context.principal.as_str(),
            "agent.unbind",
            "agent:registry",
        )
        .inspect_err(|_error| {
            self.state.record_denied();
        })?;
        let principal = if let Some(value) = non_empty(payload.principal) {
            if value != context.principal {
                self.state.record_denied();
                return Err(Status::permission_denied(
                    "unbind agent principal must match authenticated principal",
                ));
            }
            value
        } else {
            context.principal.clone()
        };
        let session_id = optional_canonical_id(payload.session_id, "session_id")
            .inspect_err(|_error| {
                self.state.counters.agent_validation_failures.fetch_add(1, Ordering::Relaxed);
            })?
            .ok_or_else(|| Status::invalid_argument("session_id is required"))?;
        let outcome = self
            .state
            .unbind_agent_for_context(AgentUnbindRequest {
                principal,
                channel: non_empty(payload.channel),
                session_id,
            })
            .await?;
        if outcome.removed {
            let _ = record_agent_journal_event(
                &self.state,
                &context,
                json!({
                    "event": "agent.binding_removed",
                    "removed_agent_id": outcome.removed_agent_id,
                }),
            )
            .await;
        }
        Ok(Response::new(gateway_v1::UnbindAgentForContextResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            removed: outcome.removed,
            removed_agent_id: outcome.removed_agent_id.unwrap_or_default(),
        }))
    }

    async fn resolve_agent_for_context(
        &self,
        request: Request<gateway_v1::ResolveAgentForContextRequest>,
    ) -> Result<Response<gateway_v1::ResolveAgentForContextResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "ResolveAgentForContext")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        authorize_agent_management_action(
            context.principal.as_str(),
            "agent.resolve",
            "agent:registry",
        )
        .inspect_err(|_error| {
            self.state.record_denied();
        })?;
        let principal = if let Some(value) = non_empty(payload.principal) {
            if value != context.principal {
                self.state.record_denied();
                return Err(Status::permission_denied(
                    "resolve agent principal must match authenticated principal",
                ));
            }
            value
        } else {
            context.principal.clone()
        };
        let session_id =
            optional_canonical_id(payload.session_id, "session_id").inspect_err(|_error| {
                self.state.counters.agent_validation_failures.fetch_add(1, Ordering::Relaxed);
            })?;
        let outcome = self
            .state
            .resolve_agent_for_context(AgentResolveRequest {
                principal,
                channel: non_empty(payload.channel),
                session_id,
                preferred_agent_id: non_empty(payload.preferred_agent_id),
                persist_session_binding: payload.persist_session_binding,
            })
            .await?;
        let inventory = self.execution_backend_inventory()?;
        let resolution =
            resolve_execution_backend(outcome.agent.execution_backend_preference, &inventory);
        if outcome.binding_created {
            let _ = record_agent_journal_event(
                &self.state,
                &context,
                json!({
                    "event": "agent.updated",
                    "agent_id": outcome.agent.agent_id,
                    "binding_created": true,
                }),
            )
            .await;
        }
        Ok(Response::new(gateway_v1::ResolveAgentForContextResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            agent: Some(agent_message(&outcome.agent)),
            source: agent_resolution_source_to_proto(outcome.source),
            binding_created: outcome.binding_created,
            is_default: outcome.is_default,
            resolved_execution_backend: resolution.resolved.as_str().to_owned(),
            execution_backend_fallback_used: resolution.fallback_used,
            execution_backend_reason: resolution.reason,
            execution_backends: inventory.iter().map(execution_backend_inventory_message).collect(),
        }))
    }

    async fn run_stream(
        &self,
        request: Request<Streaming<common_v1::RunStreamRequest>>,
    ) -> Result<Response<Self::RunStreamStream>, Status> {
        if !self.state.is_orchestrator_runloop_enabled() {
            self.state.record_denied();
            return Err(Status::failed_precondition(
                "orchestrator run loop v1 is disabled; set PALYRA_ORCHESTRATOR_RUNLOOP_V1_ENABLED=true",
            ));
        }
        let context = self.authorize_rpc(request.metadata(), "RunStream")?;
        self.state.counters.run_stream_requests.fetch_add(1, Ordering::Relaxed);

        let mut stream = request.into_inner();
        let (sender, receiver) = mpsc::channel(16);
        let context_for_stream = context.clone();
        let state_for_stream = self.state.clone();

        tokio::spawn(async move {
            let mut active_session_id = None::<String>;
            let mut active_run_id = None::<String>;
            let mut run_state = RunStateMachine::default();
            let mut tape_seq = 0_i64;
            let mut model_token_tape_events = 0_usize;
            let mut model_token_compaction_emitted = false;
            let mut in_progress_emitted = false;
            let mut remaining_tool_budget = state_for_stream.config.tool_call.max_calls_per_run;
            let mut previous_session_run_id = None::<String>;

            while let Some(item) = stream.next().await {
                let message = match item {
                    Ok(value) => value,
                    Err(error) => {
                        let status =
                            Status::internal(format!("failed to read run stream request: {error}"));
                        finalize_run_failure(RunFailureFinalization {
                            sender: &sender,
                            runtime_state: &state_for_stream,
                            request_context: Some(&context_for_stream),
                            active_session_id: active_session_id.as_deref(),
                            run_state: &mut run_state,
                            active_run_id: active_run_id.as_deref(),
                            tape_seq: &mut tape_seq,
                            reason: status.message(),
                        })
                        .await;
                        let _ = sender.send(Err(status)).await;
                        return;
                    }
                };
                if message.v != CANONICAL_PROTOCOL_MAJOR {
                    let status = Status::failed_precondition("unsupported protocol major version");
                    finalize_run_failure(RunFailureFinalization {
                        sender: &sender,
                        runtime_state: &state_for_stream,
                        request_context: Some(&context_for_stream),
                        active_session_id: active_session_id.as_deref(),
                        run_state: &mut run_state,
                        active_run_id: active_run_id.as_deref(),
                        tape_seq: &mut tape_seq,
                        reason: status.message(),
                    })
                    .await;
                    let _ = sender.send(Err(status)).await;
                    return;
                }
                if message.allow_sensitive_tools {
                    state_for_stream.record_denied();
                    let status = Status::permission_denied(format!(
                        "decision=deny_by_default approval_required=true reason={SENSITIVE_TOOLS_DENY_REASON}",
                    ));
                    finalize_run_failure(RunFailureFinalization {
                        sender: &sender,
                        runtime_state: &state_for_stream,
                        request_context: Some(&context_for_stream),
                        active_session_id: active_session_id.as_deref(),
                        run_state: &mut run_state,
                        active_run_id: active_run_id.as_deref(),
                        tape_seq: &mut tape_seq,
                        reason: SENSITIVE_TOOLS_DENY_REASON,
                    })
                    .await;
                    let _ = sender.send(Err(status)).await;
                    return;
                }

                match process_run_stream_message(
                    &sender,
                    &mut stream,
                    &state_for_stream,
                    &context_for_stream,
                    &mut active_session_id,
                    &mut active_run_id,
                    &mut run_state,
                    &mut tape_seq,
                    &mut model_token_tape_events,
                    &mut model_token_compaction_emitted,
                    &mut in_progress_emitted,
                    &mut remaining_tool_budget,
                    &mut previous_session_run_id,
                    message,
                )
                .await
                {
                    Ok(RunStreamMessageProcessingOutcome::Continue) => {}
                    Ok(RunStreamMessageProcessingOutcome::Terminate) => {
                        return;
                    }
                    Err(error) => {
                        finalize_run_failure(RunFailureFinalization {
                            sender: &sender,
                            runtime_state: &state_for_stream,
                            request_context: Some(&context_for_stream),
                            active_session_id: active_session_id.as_deref(),
                            run_state: &mut run_state,
                            active_run_id: active_run_id.as_deref(),
                            tape_seq: &mut tape_seq,
                            reason: error.message(),
                        })
                        .await;
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                }
            }

            if let Some(run_id) = active_run_id {
                match finalize_run_stream_after_provider_response(
                    &sender,
                    &state_for_stream,
                    &mut run_state,
                    run_id.as_str(),
                    &mut tape_seq,
                )
                .await
                {
                    Ok(RunStreamPostProviderOutcome::Completed) => {}
                    Ok(RunStreamPostProviderOutcome::Cancelled) => {}
                    Err(error) => {
                        finalize_run_failure(RunFailureFinalization {
                            sender: &sender,
                            runtime_state: &state_for_stream,
                            request_context: Some(&context_for_stream),
                            active_session_id: active_session_id.as_deref(),
                            run_state: &mut run_state,
                            active_run_id: Some(run_id.as_str()),
                            tape_seq: &mut tape_seq,
                            reason: error.message(),
                        })
                        .await;
                        let _ = sender.send(Err(error)).await;
                    }
                }
            }
        });

        info!(
            method = "RunStream",
            principal = %context.principal,
            device_id = %context.device_id,
            channel = context.channel.as_deref().unwrap_or("n/a"),
            "gateway run stream opened"
        );

        Ok(Response::new(ReceiverStream::new(receiver)))
    }
}
