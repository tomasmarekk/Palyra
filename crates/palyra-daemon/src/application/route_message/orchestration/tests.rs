use std::{future::Future, pin::Pin, sync::Arc};

use tokio::sync::{mpsc, Notify};

use super::*;
use crate::{
    application::channel_turn::ChannelTurnEnvelopeInput,
    gateway::tests::{build_test_runtime_state, build_test_runtime_state_with_runtime_overrides},
    journal::{
        run_admission::{
            JournalInitialSessionAuthorityPinRequest, JournalRuntimeAuthority,
            JournalRuntimeAuthorityReason, JournalRuntimeProfile, JournalSessionAuthorityIntent,
        },
        OrchestratorCancelRequest, OrchestratorSessionUpsertRequest,
    },
    model_provider::{
        capability_defaults_for_kind, AudioTranscriptionRequest, AudioTranscriptionResponse,
        ModelProvider, ModelProviderKind, ProviderAttemptSummary, ProviderError,
        ProviderFinishReason, ProviderPromptSegmentKind, ProviderRawProviderRefs, ProviderRequest,
        ProviderResponse, ProviderStatusSnapshot, ProviderTurnOutput, ProviderUsage,
    },
};
use palyra_model_providers::{ProviderRegistryModelSnapshot, ProviderRegistryProviderSnapshot};

struct BlockingRouteProvider {
    started: mpsc::Sender<()>,
    release: Arc<Notify>,
    status: ProviderStatusSnapshot,
}

#[test]
fn attachment_context_is_added_only_after_route_admission() {
    let attachment = common_v1::MessageAttachment {
        kind: common_v1::message_attachment::AttachmentKind::File as i32,
        filename: "@palyra.txt".to_owned(),
        source_url: "https://example.test/@palyra.txt".to_owned(),
        ..Default::default()
    };

    let routed = with_routed_attachment_context("plain body", &[attachment]);

    assert!(routed.starts_with("plain body\n\n[attachment-metadata]"));
    assert!(routed.contains("filename=@palyra.txt"));
}

impl ModelProvider for BlockingRouteProvider {
    fn complete<'a>(
        &'a self,
        _request: ProviderRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ProviderResponse, ProviderError>> + Send + 'a>> {
        Box::pin(async move {
            self.started
                .send(())
                .await
                .expect("route supersession test receiver should remain open");
            self.release.notified().await;
            Ok(provider_response("stale route response", "stale-provider", "stale-model"))
        })
    }

    fn transcribe_audio<'a>(
        &'a self,
        _request: AudioTranscriptionRequest,
    ) -> Pin<Box<dyn Future<Output = Result<AudioTranscriptionResponse, ProviderError>> + Send + 'a>>
    {
        Box::pin(async { Err(ProviderError::MissingApiKey) })
    }

    fn status_snapshot(&self) -> ProviderStatusSnapshot {
        self.status.clone()
    }
}

struct BlockingRouteReplacementProvider {
    started: mpsc::Sender<()>,
    release: Arc<Notify>,
    status: ProviderStatusSnapshot,
}

impl ModelProvider for BlockingRouteReplacementProvider {
    fn complete<'a>(
        &'a self,
        _request: ProviderRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ProviderResponse, ProviderError>> + Send + 'a>> {
        Box::pin(async move {
            self.started
                .send(())
                .await
                .expect("second route supersession receiver should remain open");
            self.release.notified().await;
            Ok(provider_response(
                "second stale route response",
                "second-stale-provider",
                "second-stale-model",
            ))
        })
    }

    fn transcribe_audio<'a>(
        &'a self,
        _request: AudioTranscriptionRequest,
    ) -> Pin<Box<dyn Future<Output = Result<AudioTranscriptionResponse, ProviderError>> + Send + 'a>>
    {
        Box::pin(async { Err(ProviderError::MissingApiKey) })
    }

    fn status_snapshot(&self) -> ProviderStatusSnapshot {
        self.status.clone()
    }
}

struct CapturingRouteProvider {
    requests: mpsc::Sender<ProviderRequest>,
    status: ProviderStatusSnapshot,
    response_text: &'static str,
}

impl ModelProvider for CapturingRouteProvider {
    fn complete<'a>(
        &'a self,
        request: ProviderRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ProviderResponse, ProviderError>> + Send + 'a>> {
        Box::pin(async move {
            self.requests
                .send(request)
                .await
                .expect("route retry request receiver should remain open");
            Ok(provider_response(
                self.response_text,
                self.status.provider_id.as_str(),
                self.status.model_id.as_deref().unwrap_or("replacement-model"),
            ))
        })
    }

    fn transcribe_audio<'a>(
        &'a self,
        _request: AudioTranscriptionRequest,
    ) -> Pin<Box<dyn Future<Output = Result<AudioTranscriptionResponse, ProviderError>> + Send + 'a>>
    {
        Box::pin(async { Err(ProviderError::MissingApiKey) })
    }

    fn status_snapshot(&self) -> ProviderStatusSnapshot {
        self.status.clone()
    }
}

fn cross_kind_replacement_status(mut status: ProviderStatusSnapshot) -> ProviderStatusSnapshot {
    let anthropic_capabilities = capability_defaults_for_kind(
        ModelProviderKind::Anthropic,
        crate::model_provider::ProviderModelRole::Chat,
    );
    status.kind = "anthropic".to_owned();
    status.provider_id = "anthropic-replacement".to_owned();
    status.credential_id = "auth-profile:anthropic-replacement:test".to_owned();
    status.model_id = Some("claude-replacement".to_owned());
    status.capabilities = anthropic_capabilities.clone();
    status.registry.default_chat_model_id = Some("claude-replacement".to_owned());
    status.registry.failover_enabled = false;
    status.registry.response_cache_enabled = true;
    status.registry.providers = vec![ProviderRegistryProviderSnapshot {
        provider_id: "anthropic-replacement".to_owned(),
        credential_id: "auth-profile:anthropic-replacement:test".to_owned(),
        display_name: "Anthropic replacement".to_owned(),
        kind: "anthropic".to_owned(),
        enabled: true,
        endpoint_base_url: None,
        auth_profile_id: Some("test".to_owned()),
        auth_profile_provider_kind: Some("anthropic".to_owned()),
        credential_source: Some("auth_profile_api_key".to_owned()),
        api_key_configured: true,
        retry_policy: status.retry_policy.clone(),
        circuit_breaker: status.circuit_breaker.clone(),
        runtime_metrics: status.runtime_metrics.clone(),
        health: status.health.clone(),
        discovery: status.discovery.clone(),
    }];
    status.registry.credentials.clear();
    status.registry.models = vec![ProviderRegistryModelSnapshot {
        model_id: "claude-replacement".to_owned(),
        provider_id: "anthropic-replacement".to_owned(),
        role: "chat".to_owned(),
        enabled: true,
        capabilities: anthropic_capabilities,
    }];
    status.route_selection.selected_provider_id = Some("anthropic-replacement".to_owned());
    status.route_selection.selected_model_id = Some("claude-replacement".to_owned());
    status
}

fn provider_response(text: &str, provider_id: &str, model_id: &str) -> ProviderResponse {
    let output = ProviderTurnOutput::text(
        text.to_owned(),
        ProviderFinishReason::Stop,
        ProviderUsage::new(1, 2, "test"),
        ProviderRawProviderRefs::default(),
    );
    ProviderResponse {
        events: palyra_model_providers::provider_events_from_output(&output),
        prompt_tokens: output.usage.prompt_tokens,
        completion_tokens: output.usage.completion_tokens,
        output,
        retry_count: 0,
        provider_id: provider_id.to_owned(),
        model_id: model_id.to_owned(),
        served_from_cache: false,
        failover_count: 0,
        attempts: vec![ProviderAttemptSummary {
            provider_id: provider_id.to_owned(),
            model_id: model_id.to_owned(),
            outcome: "success".to_owned(),
            retryable: false,
            served_from_cache: false,
            reason_code: None,
            state: None,
        }],
        qa_lane_attestation: None,
    }
}

fn route_input() -> ChannelInboundMessage {
    ChannelInboundMessage {
        envelope_id: Ulid::new().to_string(),
        channel: "test".to_owned(),
        conversation_id: Some("route-provider-supersession".to_owned()),
        sender_handle: Some("user:test".to_owned()),
        sender_display: Some("Test User".to_owned()),
        sender_verified: true,
        sender_roles: Vec::new(),
        text: "use the replacement provider".to_owned(),
        max_payload_bytes: 16 * 1024,
        is_direct_message: true,
        requested_broadcast: false,
        adapter_message_id: Some(Ulid::new().to_string()),
        adapter_thread_id: None,
        retry_attempt: 0,
    }
}

fn route_plan(input: &ChannelInboundMessage) -> ChannelRoutePlan {
    ChannelRoutePlan {
        channel: input.channel.clone(),
        route_key: "route-provider-supersession".to_owned(),
        session_key: format!("route-provider-supersession:{}", input.envelope_id),
        session_label: Some("Provider supersession route".to_owned()),
        binding_id: None,
        binding_kind: None,
        binding_expires_at_unix_ms: None,
        binding_reason: None,
        sender_identity: input.sender_handle.clone(),
        message_route_authorized: true,
        is_broadcast: false,
        response_prefix: None,
        auto_ack_text: None,
        auto_reaction: None,
        in_reply_to_message_id: None,
        reply_thread_id: None,
        route_target: None,
    }
}

fn route_envelope(input: &ChannelInboundMessage, context: &RequestContext) -> ChannelTurnEnvelope {
    ChannelTurnEnvelope::from_input(ChannelTurnEnvelopeInput {
        envelope_id: input.envelope_id.clone(),
        channel: input.channel.clone(),
        conversation_id: input.conversation_id.clone(),
        thread_id: input.adapter_thread_id.clone(),
        sender_handle: input.sender_handle.clone(),
        sender_display: input.sender_display.clone(),
        sender_verified: input.sender_verified,
        gateway_principal: context.principal.clone(),
        gateway_device_id: context.device_id.clone(),
        text: input.text.clone(),
        max_payload_bytes: input.max_payload_bytes,
        is_direct_message: input.is_direct_message,
        requested_broadcast: input.requested_broadcast,
        adapter_message_id: input.adapter_message_id.clone(),
        retry_attempt: input.retry_attempt,
        attachment_count: 0,
        json_mode_requested: false,
        route_config_hash: "route-config-test".to_owned(),
        received_at_unix_ms: current_unix_ms(),
    })
}

fn route_context() -> RequestContext {
    RequestContext {
        principal: "user:test".to_owned(),
        device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
        channel: Some("test".to_owned()),
    }
}

fn pin_existing_legacy_route_session(
    state: &GatewayRuntimeState,
    plan: &ChannelRoutePlan,
    context: &RequestContext,
) {
    let session_id = Ulid::new().to_string();
    state
        .journal_store
        .upsert_orchestrator_session(&OrchestratorSessionUpsertRequest {
            session_id: session_id.clone(),
            session_key: plan.session_key.clone(),
            session_label: plan.session_label.clone(),
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
        })
        .expect("existing legacy route session should persist");
    state
        .journal_store
        .pin_initial_session_runtime_authority(&JournalInitialSessionAuthorityPinRequest {
            session_id,
            expected_revision: 0,
            intent: JournalSessionAuthorityIntent {
                configured_profile: JournalRuntimeProfile::Legacy,
                selected_runtime: JournalRuntimeAuthority::Legacy,
                reason: JournalRuntimeAuthorityReason::LegacyProfileSelected,
                shadow_evaluation_enabled: false,
            },
            migration_reason_code: "runtime.session_authority.route_provider_test".to_owned(),
        })
        .expect("existing legacy route session authority should pin");
}

#[tokio::test]
async fn default_v2_admits_new_connector_route_messages() {
    let state = build_test_runtime_state(false);
    let input = route_input();
    let plan = route_plan(&input);
    let context = route_context();
    let resolved_session = state
        .resolve_orchestrator_session(OrchestratorSessionResolveRequest {
            session_id: None,
            session_key: Some(plan.session_key),
            session_label: plan.session_label,
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
            require_existing: false,
            reset_session: false,
        })
        .await
        .expect("new route session should resolve");
    assert!(resolved_session.created);
    let session = resolved_session.session;
    let dispatcher =
        crate::application::runtime_kernel_v2::dispatcher::RuntimeKernelDispatcher::resolve(
            &crate::config::RuntimeKernelConfig::default(),
            &crate::config::FeatureRolloutsConfig::default(),
            None,
            false,
            crate::application::runtime_kernel_v2::selection::V2RuntimeAvailability::Ready,
        )
        .expect("default V2 runtime dispatcher should initialize");
    let typed_session_id = RuntimeSessionId::parse(session.session_id.as_str())
        .expect("route session id should parse");
    let authority_intent = dispatcher
        .resolve_authority_intent(
            &state.journal_store,
            &typed_session_id,
            Some(context.principal.as_str()),
            true,
            true,
            RuntimeAuthorityProgressEvidence::pristine(),
        )
        .expect("default V2 route authority should resolve");
    assert_eq!(authority_intent.selected_runtime(), Some(RuntimeAuthority::V2));
    let run_id = Ulid::new().to_string();
    let outcome = admit_v2_route_message_run(
        &state,
        &context,
        &session,
        &dispatcher,
        authority_intent,
        run_id.as_str(),
        input.envelope_id.as_str(),
    )
    .await
    .expect("default V2 route admission should execute");
    let token = match outcome {
        RunAdmissionControllerOutcome::Admitted { token, .. } => token,
        RunAdmissionControllerOutcome::Rejected { journal } => {
            panic!("default V2 route admission rejected: {}", journal.reason_code)
        }
        RunAdmissionControllerOutcome::Queued { journal } => {
            panic!("default V2 route admission queued: {}", journal.reason_code)
        }
    };
    assert_eq!(token.run_id(), run_id);
    assert_eq!(token.identities().session_id.as_str(), session.session_id);
    let pin = state
        .journal_store
        .load_session_runtime_authority(session.session_id.as_str())
        .expect("route session authority should load")
        .expect("route session authority should be pinned");
    assert_eq!(pin.configured_profile, JournalRuntimeProfile::V2);
    assert_eq!(pin.selected_runtime, JournalRuntimeAuthority::V2);
    assert!(
        state
            .journal_store
            .load_runtime_kernel_head(run_id.as_str())
            .expect("route V2 kernel state should load")
            .is_some(),
        "route V2 admission should initialize durable kernel state"
    );
}

#[tokio::test]
async fn route_provider_supersession_retries_with_replacement_and_settles_done() {
    let mut feature_rollouts = crate::config::FeatureRolloutsConfig::default();
    feature_rollouts.context_engine =
        palyra_common::feature_rollouts::FeatureRolloutSetting::from_config(true);
    let state = build_test_runtime_state_with_runtime_overrides(false, false, feature_rollouts);
    let input = route_input();
    let plan = route_plan(&input);
    let context = route_context();
    // Provider replacement belongs to the retained compatibility path, not new-session admission.
    pin_existing_legacy_route_session(&state, &plan, &context);
    let envelope = route_envelope(&input, &context);
    let content = common_v1::MessageContent { text: input.text.clone(), attachments: Vec::new() };

    let (started_tx, mut started_rx) = mpsc::channel(1);
    let release = Arc::new(Notify::new());
    let blocking_status = state.model_provider_status_snapshot();
    let _ = state.configure_model_provider(Arc::new(BlockingRouteProvider {
        started: started_tx,
        release: Arc::clone(&release),
        status: blocking_status,
    }));

    let request_state = Arc::clone(&state);
    let request_input = input.clone();
    let request_plan = plan.clone();
    let request_context = context.clone();
    let request_envelope = envelope.clone();
    let request_content = content.clone();
    let request = tokio::spawn(async move {
        handle_routed_route_message(
            &request_state,
            &request_context,
            &request_input,
            &request_content,
            &request_plan,
            None,
            false,
            request_input.envelope_id.as_str(),
            "route-config-test",
            "test-connector",
            request_context.principal.as_str(),
            request_context.device_id.as_str(),
            &request_envelope,
            0,
        )
        .await
    });

    started_rx.recv().await.expect("old provider call should start");
    let (requests_tx, mut requests_rx) = mpsc::channel(1);
    let replacement_status = cross_kind_replacement_status(state.model_provider_status_snapshot());
    let _ = state.configure_model_provider(Arc::new(CapturingRouteProvider {
        requests: requests_tx,
        status: replacement_status,
        response_text: "replacement route response",
    }));
    release.notify_one();

    let response = request
        .await
        .expect("route request task should join")
        .expect("route request should succeed through replacement provider");
    let replacement_request = requests_rx.recv().await.expect("replacement provider should run");
    assert_eq!(replacement_request.model_override.as_deref(), Some("claude-replacement"));
    assert!(
        replacement_request
            .messages
            .iter()
            .filter(|message| matches!(
                message.role,
                crate::model_provider::ProviderMessageRole::Developer
            ))
            .flat_map(|message| message.content.iter())
            .filter_map(|part| match part {
                crate::model_provider::ProviderMessageContentPart::Text { text } => {
                    Some(text.as_str())
                }
                crate::model_provider::ProviderMessageContentPart::Image { .. } => None,
            })
            .any(|text| text.contains("Provider kind: anthropic.")
                && text.contains("Model family: claude-replacement.")),
        "replacement developer instructions should target the replacement provider and model"
    );
    assert!(
        replacement_request
            .context_trace_id
            .as_deref()
            .is_some_and(|trace_id| { trace_id.starts_with("ctx_handover_") }),
        "replacement request should expose a versioned handover trace"
    );
    assert!(
        replacement_request
            .budget_profile
            .as_deref()
            .is_some_and(|profile| profile.starts_with("budget_")),
        "replacement request should recompute its provider budget profile"
    );
    assert_eq!(replacement_request.max_output_tokens, Some(8_192));
    assert_eq!(
        replacement_request.prompt_cache_policy.provider_compatibility,
        "anthropic_cache_control"
    );
    let current_turn_segment = replacement_request
        .prompt_segments
        .iter()
        .find(|segment| segment.kind == ProviderPromptSegmentKind::CurrentTurn)
        .expect("replacement request should preserve the current-turn prompt segment");
    assert_eq!(current_turn_segment.content_hash, crate::sha256_hex(input.text.as_bytes()));
    assert_eq!(current_turn_segment.byte_len, input.text.len());
    assert_eq!(
        replacement_request
            .prompt_cache_report
            .as_ref()
            .expect("replacement request should expose cache metadata")
            .provider_cache_strategy,
        "anthropic_cache_control"
    );
    let replacement_catalog = replacement_request
        .tool_catalog_snapshot
        .as_ref()
        .expect("replacement request should include a tool catalog");
    let replacement_catalog_hash = replacement_catalog
        .get("catalog_hash")
        .and_then(serde_json::Value::as_str)
        .expect("replacement catalog should expose its hash");
    let tool_segment = replacement_request
        .prompt_segments
        .iter()
        .find(|segment| segment.kind == ProviderPromptSegmentKind::Tool)
        .expect("replacement request should rematerialize the tool prompt segment");
    assert_eq!(tool_segment.content_hash, crate::sha256_hex(replacement_catalog_hash.as_bytes()));
    assert_eq!(tool_segment.byte_len, replacement_catalog_hash.len());
    assert!(response.accepted);
    assert_eq!(response.decision_reason, "routed");
    assert_eq!(response.retry_attempt, 0);
    assert_eq!(response.outputs.len(), 1);
    assert_eq!(response.outputs[0].text, "replacement route response");
    assert!(!response.outputs[0].text.contains("stale route response"));

    let run_id =
        response.run_id.as_ref().expect("route response should expose run id").ulid.as_str();
    let run = state
        .journal_store
        .orchestrator_run_status_snapshot(run_id)
        .expect("route run snapshot should load")
        .expect("route run should exist");
    assert_eq!(run.state, RunLifecycleState::Done.as_str());
    assert_eq!(
        state
            .journal_store
            .shared_runtime_diagnostics()
            .expect("runtime diagnostics should load")
            .stale_events_by_subsystem
            .get("provider"),
        Some(&1)
    );
}

#[tokio::test]
async fn route_provider_cancellation_during_call_suppresses_retry_and_output() {
    let state = build_test_runtime_state(false);
    let input = route_input();
    let plan = route_plan(&input);
    let context = route_context();
    pin_existing_legacy_route_session(&state, &plan, &context);
    let envelope = route_envelope(&input, &context);
    let content = common_v1::MessageContent { text: input.text.clone(), attachments: Vec::new() };

    let (started_tx, mut started_rx) = mpsc::channel(1);
    let release = Arc::new(Notify::new());
    let status = state.model_provider_status_snapshot();
    let _ = state.configure_model_provider(Arc::new(BlockingRouteProvider {
        started: started_tx,
        release,
        status,
    }));

    let request_state = Arc::clone(&state);
    let request_input = input.clone();
    let request_plan = plan.clone();
    let request_context = context.clone();
    let request_envelope = envelope.clone();
    let request_content = content.clone();
    let request = tokio::spawn(async move {
        handle_routed_route_message(
            &request_state,
            &request_context,
            &request_input,
            &request_content,
            &request_plan,
            None,
            false,
            request_input.envelope_id.as_str(),
            "route-config-test",
            "test-connector",
            request_context.principal.as_str(),
            request_context.device_id.as_str(),
            &request_envelope,
            0,
        )
        .await
    });

    started_rx.recv().await.expect("provider call should start");
    let run_id = loop {
        let connection = rusqlite::Connection::open(&state.journal_config.db_path)
            .expect("test journal database should reopen");
        let run_id = connection
            .query_row(
                "SELECT run_ulid FROM orchestrator_runs WHERE state = 'in_progress' ORDER BY created_at_unix_ms DESC LIMIT 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .ok();
        drop(connection);
        if let Some(run_id) = run_id {
            break run_id;
        }
        tokio::task::yield_now().await;
    };
    state
        .request_orchestrator_cancel(OrchestratorCancelRequest {
            run_id: run_id.clone(),
            reason: "route_cancel_test".to_owned(),
        })
        .await
        .expect("cancel intent should persist");

    let response = tokio::time::timeout(Duration::from_secs(5), request)
        .await
        .expect("route cancellation should settle promptly")
        .expect("route request task should join")
        .expect("route cancellation should return a route response");
    assert!(!response.accepted);
    assert!(!response.queued_for_retry);
    assert_eq!(response.decision_reason, "cancelled_by_request");
    assert!(response.outputs.is_empty());

    let run = state
        .journal_store
        .orchestrator_run_status_snapshot(run_id.as_str())
        .expect("route run snapshot should load")
        .expect("route run should exist");
    assert_eq!(run.state, RunLifecycleState::Cancelled.as_str());
    let tape =
        state.journal_store.orchestrator_tape(run_id.as_str()).expect("route tape should load");
    assert!(tape.iter().all(|event| event.event_type != "message.replied"));
    assert!(
        state.self_healing_heartbeats().into_iter().all(|heartbeat| heartbeat.object_id != run_id),
        "terminal route cancellation must clear its run heartbeat"
    );
}

#[tokio::test]
async fn route_provider_supersession_exhaustion_settles_failed_without_orphan() {
    let state = build_test_runtime_state(false);
    let input = route_input();
    let plan = route_plan(&input);
    let context = route_context();
    pin_existing_legacy_route_session(&state, &plan, &context);
    let envelope = route_envelope(&input, &context);
    let content = common_v1::MessageContent { text: input.text.clone(), attachments: Vec::new() };

    let (first_started_tx, mut first_started_rx) = mpsc::channel(1);
    let first_release = Arc::new(Notify::new());
    let initial_status = state.model_provider_status_snapshot();
    let _ = state.configure_model_provider(Arc::new(BlockingRouteProvider {
        started: first_started_tx,
        release: Arc::clone(&first_release),
        status: initial_status,
    }));

    let request_state = Arc::clone(&state);
    let request_input = input.clone();
    let request_plan = plan.clone();
    let request_context = context.clone();
    let request_envelope = envelope.clone();
    let request_content = content.clone();
    let request = tokio::spawn(async move {
        handle_routed_route_message(
            &request_state,
            &request_context,
            &request_input,
            &request_content,
            &request_plan,
            None,
            false,
            request_input.envelope_id.as_str(),
            "route-config-test",
            "test-connector",
            request_context.principal.as_str(),
            request_context.device_id.as_str(),
            &request_envelope,
            0,
        )
        .await
    });

    first_started_rx.recv().await.expect("first provider call should start");
    let (second_started_tx, mut second_started_rx) = mpsc::channel(1);
    let second_release = Arc::new(Notify::new());
    let second_status = state.model_provider_status_snapshot();
    let _ = state.configure_model_provider(Arc::new(BlockingRouteReplacementProvider {
        started: second_started_tx,
        release: Arc::clone(&second_release),
        status: second_status,
    }));
    first_release.notify_one();

    second_started_rx.recv().await.expect("replacement provider call should start");
    let (third_requests_tx, mut third_requests_rx) = mpsc::channel(1);
    let third_status = state.model_provider_status_snapshot();
    let _ = state.configure_model_provider(Arc::new(CapturingRouteProvider {
        requests: third_requests_tx,
        status: third_status,
        response_text: "third provider response must not run",
    }));
    second_release.notify_one();

    let response = request
        .await
        .expect("route request task should join")
        .expect("supersession exhaustion should return a route response");
    assert!(!response.accepted);
    assert!(!response.queued_for_retry);
    assert_eq!(response.decision_reason, "model_provider_superseded");
    assert_eq!(response.retry_attempt, 0);
    assert!(third_requests_rx.try_recv().is_err(), "third provider must not be called");

    let run_id =
        response.run_id.as_ref().expect("route response should expose run id").ulid.as_str();
    let run = state
        .journal_store
        .orchestrator_run_status_snapshot(run_id)
        .expect("route run snapshot should load")
        .expect("route run should exist");
    assert_eq!(run.state, RunLifecycleState::Failed.as_str());
    assert_eq!(
        state
            .journal_store
            .shared_runtime_diagnostics()
            .expect("runtime diagnostics should load")
            .stale_events_by_subsystem
            .get("provider"),
        Some(&2)
    );
}
