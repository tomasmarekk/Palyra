use super::{
    active_run_steering_guidance, advisor_synthesis_message, agent_loop_budget_exhausted_message,
    agent_loop_terminal_status_message, apply_background_budget_guard,
    background_budget_overrun_message, background_run_budget_tokens,
    bounded_provider_retry_evidence, bounded_provider_route_change_evidence,
    browser_followup_timeout_context, browser_followup_timeout_partial_summary,
    configured_run_stream_agent_harness_plugin_id, configured_run_stream_codex_harness_id,
    contains_raw_provider_tool_call_markup, delegated_run_admission,
    drain_active_run_steering_before_provider_call, effective_provider_request_deadline,
    embedded_run_stream_runtime_selection_payload, execute_run_stream_provider_request,
    final_answer_recovery_fallback_summary, final_answer_recovery_prompt,
    followup_timeout_recovery_prompt, incomplete_final_answer_without_tools,
    incomplete_terminal_final_answer, incomplete_terminal_outcome_message, is_browser_tool_name,
    is_run_stream_response_channel_closed, length_recovery_prompt,
    narrow_routine_tool_catalog_policy, normalized_provider_stream_from_output_v2,
    normalized_tool_output_evidence, phase_heartbeat_interval, provider_error_partial_summary,
    provider_model_override_for_routing, provider_output_needs_tool_repair_audit,
    provider_request_deadline_timeout, provider_request_timeout_message,
    provider_request_timeout_status, provider_status_recovery_decision_payload,
    provider_timeout_termination_reason, provider_turn_anomaly_from_response_failure,
    provider_turn_anomaly_from_status, provider_waiting_status_message,
    repeated_tool_failure_signature, revoke_inherited_tool_approvals_after_steering,
    run_loop_phase_timeout_message, run_loop_phase_timeout_partial_summary,
    run_loop_phase_timeout_payload, run_loop_phase_waiting_status_message,
    run_progress_attempt_from_tool_result, run_runtime_path_terminal_reason,
    run_stream_agent_harness_selection_mode, run_stream_harness_cancelled_tape_events,
    run_stream_harness_cleanup_payload, run_stream_harness_selection_payload,
    run_stream_harness_started_payload, run_stream_harness_terminal_event,
    run_stream_harness_terminal_from_outcome, run_stream_harness_terminal_from_state,
    run_stream_harness_terminal_payload, seed_orchestration_test_run,
    selected_v2_shadow_route_semantics, shadow_catalog_matches_selected_v2_route,
    should_emit_budget_exhausted_partial_summary, terminal_tool_authorization_failure,
    tool_calls_finish_without_tool_payload, tool_catalog_snapshot_phase_timeout,
    tool_followup_timeout_context, tool_followup_timeout_partial_summary,
    tool_result_to_provider_message, truncated_final_answer_without_tools,
    ProviderRequestTimeoutReason, RepeatedToolFailureTracker, RunLoopPhase,
    RunStreamHarnessLifecycle, RunStreamHarnessStartRequest, RunStreamHarnessTerminal,
    RunStreamMessageProcessingOutcome, RunStreamProviderRequestExecution,
    RunStreamProviderRequestOutcome, RunStreamToolResultForModel, ToolCatalogPolicySnapshot,
    CODEX_MANAGED_RUNTIME_ID, HARNESS_SELECTION_EVENT, MAX_LENGTH_RECOVERY_ATTEMPTS,
    RUNTIME_SELECTED_METADATA_EVENT, RUN_STREAM_HARNESS_RUNTIME_POLICY,
    TOOL_CATALOG_SNAPSHOT_PHASE_TIMEOUT_MS,
};
use super::{
    compare_shadow_comparison_plans_for_test, RuntimeDifferentialClassification,
    ShadowCandidatePlanInputsV1, ShadowCandidatePlannerV1, ShadowComparisonPlansV1,
    ShadowContextSegmentSemanticV1, ShadowPlanSemanticInputsV1, ShadowPolicySemanticV1,
    ShadowSelectionSemanticV1, ShadowToolCatalogSemanticV1, ShadowV2PreContextInputV1,
};
use super::{AgentLoopTerminationReason, AgentRunLoopState};
use crate::application::advisor_fanout::AdvisorRuntimeMode;
use crate::application::agent_harness::{
    AgentHarnessSelectionDiagnostics, EMBEDDED_PALYRA_HARNESS_ID,
};
use crate::application::agent_harness_lifecycle::{
    HARNESS_RUN_CANCELLED_EVENT, HARNESS_RUN_CLEANED_UP_EVENT, HARNESS_RUN_COMPLETED_EVENT,
    HARNESS_RUN_FAILED_EVENT, HARNESS_RUN_STARTED_EVENT,
};
use crate::application::provider_turn_recovery::ProviderTurnAnomaly;
use crate::application::run_stream::{
    cancellation::transition_run_stream_to_cancelled, flow_control::RunStreamFlowControl,
    tape::RUN_STREAM_RESPONSE_CHANNEL_CLOSED_MESSAGE,
};
use crate::config::{AgentHarnessConfig, AgentHarnessRegistryConfig};
use crate::gateway::{tests::build_test_runtime_state, RequestContext, ToolApprovalOutcome};
use crate::journal::{
    ApprovalDecision, ApprovalDecisionScope, OrchestratorQueuedInputCreateRequest,
    OrchestratorQueuedInputRecord, OrchestratorRunStartRequest, OrchestratorSessionUpsertRequest,
};
use crate::model_provider::{
    AudioTranscriptionRequest, AudioTranscriptionResponse, ModelProvider, ProviderAttemptState,
    ProviderAttemptSummary, ProviderError, ProviderFinishReason, ProviderMessage,
    ProviderMessageContentPart, ProviderOutputContentPart, ProviderRawProviderRefs,
    ProviderRequest, ProviderResponse, ProviderRouteCandidateTrace, ProviderRouteSelectionTrace,
    ProviderStatusSnapshot, ProviderTurnOutput, ProviderUsage, TerminalOutcomeClass,
    TerminalOutcomeClassification,
};
use crate::orchestrator::{RunLifecycleState, RunStateMachine, RunTransition};
use crate::provider_leases::{LeasePriority, ProviderLeaseExecutionContext};
use palyra_common::runtime_contracts::{
    AgentHarnessAttemptClassification, AgentHarnessAttemptReplaySafety,
    AgentHarnessAttemptTerminalStatus, AgentHarnessSelectionMode, AgentHarnessSupportOutcome,
    CancellationContextV1, CancellationReason, CancellationScopeKind, QueueMode,
    QueuedInputDeliveryBoundary, QueuedInputState, RuntimeErrorPhase, RuntimeGeneration,
    RuntimeGenerationLane, RuntimeGenerationTransitionKind, RuntimeOperationId,
    RUNTIME_FLOW_CONTROL_SCHEMA_VERSION,
};
use palyra_common::runtime_preview::RuntimePreviewMode;
use serde_json::{json, Value};
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};
use tokio::sync::{mpsc, Notify};
use tonic::{Code, Status};

#[test]
fn acting_request_accepts_manual_advisor_evidence_but_not_shadow_output() {
    let manual = advisor_synthesis_message(AdvisorRuntimeMode::Manual, Some("bounded finding"))
        .expect("manual mode should project advisor evidence");

    assert!(manual.contains("\"instruction_authority\":\"none\""));
    assert!(manual.contains("\"tool_authority\":false"));
    assert!(manual.contains("bounded finding"));
    assert!(advisor_synthesis_message(
        AdvisorRuntimeMode::Shadow,
        Some("must not reach acting request")
    )
    .is_none());
}

struct BlockingProvider {
    started: mpsc::Sender<()>,
    release: Arc<Notify>,
    status: ProviderStatusSnapshot,
}

#[test]
fn routine_tool_profile_only_narrows_the_global_catalog() {
    let base = ToolCatalogPolicySnapshot::direct_from_allowed_tools(&[
        "palyra.fs.read_file".to_owned(),
        "palyra.fs.list_dir".to_owned(),
    ]);

    let narrowed = narrow_routine_tool_catalog_policy(&base, &["PALYRA.FS.READ_FILE".to_owned()])
        .expect("global allowlisted tool should be accepted");

    assert_eq!(
        narrowed.profile_expansion.effective_allowed_tools,
        vec!["palyra.fs.read_file".to_owned()]
    );
    assert_eq!(
        narrowed.profile_expansion.explicit_allowed_tools,
        vec!["palyra.fs.read_file".to_owned()]
    );
    assert!(narrowed.profile_expansion.profiles.is_empty());

    let widened = narrow_routine_tool_catalog_policy(&base, &["palyra.fs.apply_patch".to_owned()])
        .expect_err("routine profile must not widen the global catalog");
    assert_eq!(widened.code(), Code::PermissionDenied);
}

#[test]
fn empty_global_tool_catalog_stays_deny_all_for_routines() {
    let base = ToolCatalogPolicySnapshot::direct_from_allowed_tools(&[]);

    let empty = narrow_routine_tool_catalog_policy(&base, &[])
        .expect("an empty routine profile may preserve deny-all");
    assert!(empty.profile_expansion.effective_allowed_tools.is_empty());

    let widened = narrow_routine_tool_catalog_policy(&base, &["palyra.fs.read_file".to_owned()])
        .expect_err("deny-all global policy must remain authoritative");
    assert_eq!(widened.code(), Code::PermissionDenied);
}

impl ModelProvider for BlockingProvider {
    fn complete<'a>(
        &'a self,
        _request: ProviderRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ProviderResponse, ProviderError>> + Send + 'a>> {
        Box::pin(async move {
            self.started
                .send(())
                .await
                .expect("provider supersession test receiver should remain open");
            self.release.notified().await;
            Err(ProviderError::MissingApiKey)
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

fn start_test_run(state: &crate::gateway::GatewayRuntimeState, session_id: &str, run_id: &str) {
    state
        .journal_store
        .upsert_orchestrator_session(&OrchestratorSessionUpsertRequest {
            session_id: session_id.to_owned(),
            session_key: session_id.to_owned(),
            session_label: None,
            principal: "user:test".to_owned(),
            device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            channel: Some("test".to_owned()),
        })
        .expect("test session should be created");
    seed_orchestration_test_run(
        state,
        &OrchestratorRunStartRequest {
            run_id: run_id.to_owned(),
            session_id: session_id.to_owned(),
            origin_kind: "test".to_owned(),
            origin_run_id: None,
            triggered_by_principal: Some("user:test".to_owned()),
            parameter_delta_json: None,

            delegated_admission: None,
        },
    )
    .expect("test run should be created");
    state
        .journal_store
        .update_orchestrator_run_state(run_id, RunLifecycleState::InProgress, None)
        .expect("test run should enter in-progress state");
}

fn loop_state_after_tool(prompt: &str, tool_name: &str) -> AgentRunLoopState {
    let mut state = AgentRunLoopState::new(vec![ProviderMessage::user_text(prompt)], 4, 8, 10_000);
    state.set_direct_user_input(prompt);
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

fn queued_input(
    queued_input_id: &str,
    text: &str,
    accepted_at_unix_ms: i64,
) -> OrchestratorQueuedInputRecord {
    OrchestratorQueuedInputRecord {
        queued_input_id: queued_input_id.to_owned(),
        run_id: "run_active".to_owned(),
        session_id: "session".to_owned(),
        state: "pending".to_owned(),
        queue_mode: "interrupt".to_owned(),
        delivery_boundary: "cancel_then_next_turn".to_owned(),
        expected_active_generation: Some(1),
        claimed_active_generation: None,
        lifecycle_revision: 0,
        priority_lane: "default".to_owned(),
        coalescing_group: None,
        overflow_summary_ref: None,
        safe_boundary_flags_json: "{}".to_owned(),
        decision_reason: "test".to_owned(),
        text: text.to_owned(),
        attachments_json: "[]".to_owned(),
        queue_outcome_json: "{}".to_owned(),
        accepted_at_unix_ms: Some(accepted_at_unix_ms),
        coalesced_at_unix_ms: None,
        forwarded_at_unix_ms: None,
        terminal_at_unix_ms: None,
        policy_snapshot_json: "{}".to_owned(),
        explain_json: "{}".to_owned(),
        created_at_unix_ms: accepted_at_unix_ms,
        updated_at_unix_ms: accepted_at_unix_ms,
        origin_run_id: Some("run_active".to_owned()),
    }
}

async fn persist_active_queued_input(
    state: &Arc<crate::gateway::GatewayRuntimeState>,
    session_id: &str,
    run_id: &str,
    queued_input_id: &str,
    mode: QueueMode,
    boundary: QueuedInputDeliveryBoundary,
    expected_generation: RuntimeGeneration,
) {
    state
        .create_orchestrator_queued_input(OrchestratorQueuedInputCreateRequest {
            queued_input_id: queued_input_id.to_owned(),
            run_id: run_id.to_owned(),
            session_id: session_id.to_owned(),
            state: QueuedInputState::Pending.as_str().to_owned(),
            text: format!("operator input for {mode:?}"),
            origin_run_id: Some(run_id.to_owned()),
            queue_mode: mode.as_str().to_owned(),
            delivery_boundary: boundary.as_str().to_owned(),
            expected_active_generation: Some(
                i64::try_from(expected_generation.get())
                    .expect("test generation should fit journal range"),
            ),
            priority_lane: "normal".to_owned(),
            coalescing_group: Some("active-run-test".to_owned()),
            overflow_summary_ref: None,
            safe_boundary_flags_json: "{}".to_owned(),
            decision_reason: "test.active_run_queue".to_owned(),
            attachments_json: "[]".to_owned(),
            queue_outcome_json: "{}".to_owned(),
            accepted_at_unix_ms: Some(crate::gateway::current_unix_ms()),
            policy_snapshot_json: "{}".to_owned(),
            explain_json: "{}".to_owned(),
        })
        .await
        .expect("queued input should persist");
}

#[tokio::test]
async fn provider_request_supersession_keeps_run_active() {
    let state = build_test_runtime_state(false);
    let session_id = "session-provider-supersession";
    let run_id = "run-provider-supersession";
    start_test_run(&state, session_id, run_id);

    let (started_tx, mut started_rx) = mpsc::channel(1);
    let release = Arc::new(Notify::new());
    let blocking_status = state.model_provider_status_snapshot();
    let _ = state.configure_model_provider(Arc::new(BlockingProvider {
        started: started_tx,
        release: Arc::clone(&release),
        status: blocking_status,
    }));
    let request_state = Arc::clone(&state);
    let (sender, _receiver) = mpsc::channel(8);
    let request_sender = sender.clone();
    let request = tokio::spawn(async move {
        let mut run_state = RunStateMachine::default();
        run_state.transition(RunTransition::Accept).expect("run should accept");
        run_state.transition(RunTransition::StartStreaming).expect("run should start");
        let mut tape_seq = 0;
        let generation = request_state
            .runtime_generation_for_tool(run_id.to_owned())
            .await
            .expect("generation lookup")
            .expect("active generation")
            .1;
        let flow_control = RunStreamFlowControl::new(generation, Duration::from_secs(60))
            .expect("run flow control");
        execute_run_stream_provider_request(
            &request_sender,
            &request_state,
            &mut run_state,
            run_id,
            RunStreamProviderRequestExecution {
                provider_request: ProviderRequest::from_input_text(
                    "blocked provider request".to_owned(),
                    false,
                    Vec::new(),
                    None,
                ),
                lease_context: ProviderLeaseExecutionContext {
                    provider_id: "blocking-provider".to_owned(),
                    credential_id: "blocking-credential".to_owned(),
                    priority: LeasePriority::Foreground,
                    task_label: "provider_supersession_test".to_owned(),
                    max_wait_ms: 30_000,
                    session_id: Some(session_id.to_owned()),
                    run_id: Some(run_id.to_owned()),
                    runtime_authority: None,
                    diagnostic_scope_id: None,
                },
                cancellation: flow_control
                    .live_child(
                        palyra_common::runtime_contracts::CancellationScopeKind::ProviderAttempt,
                        Duration::from_secs(30),
                    )
                    .expect("provider child scope"),
                timeout_context: None,
                harness_lifecycle: None,
            },
            &flow_control,
            &mut tape_seq,
        )
        .await
    });

    started_rx.recv().await.expect("blocked provider should start");
    let _ = state.configure_model_provider(
        crate::model_provider::build_model_provider(
            &crate::model_provider::ModelProviderConfig::default(),
        )
        .expect("replacement provider should build"),
    );
    release.notify_one();

    let outcome = request
        .await
        .expect("provider request task should join")
        .expect("supersession should be an orchestrator outcome");
    assert!(matches!(outcome, RunStreamProviderRequestOutcome::Superseded));
    let run = state
        .journal_store
        .orchestrator_run_status_snapshot(run_id)
        .expect("run snapshot should load")
        .expect("run should exist");
    assert_eq!(run.state, RunLifecycleState::InProgress.as_str());
    assert_eq!(run.tape_events, 0);
    assert_eq!(
        state
            .journal_store
            .shared_runtime_diagnostics()
            .expect("diagnostics should load")
            .stale_events_by_subsystem
            .get("provider"),
        Some(&1)
    );
}

#[test]
fn response_failure_classifies_unsupported_multimodal_errors() {
    let explicit = provider_turn_anomaly_from_response_failure(
        AgentLoopTerminationReason::ProviderError,
        "provider returned vision_unsupported for this model",
    );
    let content_type = provider_turn_anomaly_from_response_failure(
        AgentLoopTerminationReason::ProviderError,
        "unsupported content type image/webp in multimodal request",
    );

    assert_eq!(explicit, ProviderTurnAnomaly::MultimodalUnsupported);
    assert_eq!(content_type, ProviderTurnAnomaly::MultimodalUnsupported);
}

#[test]
fn provider_status_classifies_tool_sequence_rejection_without_retry() {
    let anomaly = provider_turn_anomaly_from_status(
        Code::Unavailable,
        "openai-compatible endpoint returned HTTP 400: tool call result does not follow tool call (2013)",
    );

    assert_eq!(anomaly, ProviderTurnAnomaly::MalformedToolSequence);
}

#[test]
fn active_run_steering_guidance_preserves_ordered_operator_text() {
    let inputs = vec![
        queued_input("queued_1", "first correction", 10),
        queued_input("queued_2", "second correction", 20),
    ];

    let guidance = active_run_steering_guidance(inputs.as_slice());

    assert!(guidance.starts_with("<operator_steering>"));
    assert!(guidance.contains("1. first correction"));
    assert!(guidance.contains("2. second correction"));
    assert!(guidance.ends_with("</operator_steering>"));
}

#[tokio::test]
async fn active_run_steering_supersedes_generation_and_replaces_flow_control() {
    let state = build_test_runtime_state(false);
    let session_id = "session-active-steer-generation";
    let run_id = "run-active-steer-generation";
    start_test_run(&state, session_id, run_id);
    state
        .create_orchestrator_queued_input(OrchestratorQueuedInputCreateRequest {
            queued_input_id: "queued-active-steer-generation".to_owned(),
            run_id: run_id.to_owned(),
            session_id: session_id.to_owned(),
            state: QueuedInputState::Pending.as_str().to_owned(),
            text: "use the corrected target".to_owned(),
            origin_run_id: Some(run_id.to_owned()),
            queue_mode: "interrupt".to_owned(),
            delivery_boundary: QueuedInputDeliveryBoundary::CancelThenNextTurn.as_str().to_owned(),
            expected_active_generation: Some(1),
            priority_lane: "normal".to_owned(),
            coalescing_group: Some("active-steer-generation".to_owned()),
            overflow_summary_ref: None,
            safe_boundary_flags_json: "{}".to_owned(),
            decision_reason: "test.active_steer".to_owned(),
            attachments_json: "[]".to_owned(),
            queue_outcome_json: "{}".to_owned(),
            accepted_at_unix_ms: Some(crate::gateway::current_unix_ms()),
            policy_snapshot_json: "{}".to_owned(),
            explain_json: "{}".to_owned(),
        })
        .await
        .expect("steering input should persist");
    let (_, initial_generation) = state
        .runtime_generation_for_run(run_id.to_owned())
        .await
        .expect("initial generation lookup should succeed")
        .expect("run should have an active generation");
    let initial_flow_control =
        RunStreamFlowControl::new(initial_generation, Duration::from_secs(60))
            .expect("initial flow control should initialize");
    let superseded_observer = initial_flow_control.clone();
    let original_root = initial_flow_control.root_context().clone();
    let mut active_flow_control = Some(initial_flow_control);
    let mut loop_state =
        AgentRunLoopState::new(vec![ProviderMessage::user_text("initial")], 4, 8, 10_000);
    let mut tape_seq = 0;

    let steering_injected = drain_active_run_steering_before_provider_call(
        &state,
        session_id,
        run_id,
        &mut tape_seq,
        &mut loop_state,
        &mut active_flow_control,
    )
    .await
    .expect("active steering should drain");
    assert!(steering_injected);

    let (_, replacement_generation) = state
        .runtime_generation_for_run(run_id.to_owned())
        .await
        .expect("replacement generation lookup should succeed")
        .expect("replacement generation should stay active");
    assert_eq!(
        replacement_generation,
        initial_generation.next().expect("next generation should exist")
    );
    let replacement = active_flow_control.expect("replacement flow control should exist");
    assert_eq!(replacement.root_context().generation, replacement_generation);
    assert_ne!(replacement.root_context().scope_id, original_root.scope_id);
    assert_eq!(replacement.root_context().deadline_unix_ms, original_root.deadline_unix_ms);
    assert_eq!(
        superseded_observer.current_cancellation_reason(),
        Some(CancellationReason::InterruptSupersede)
    );
    assert_eq!(replacement.current_cancellation_reason(), None);
    assert_eq!(tape_seq, 2);
    let queued = state
        .list_orchestrator_queued_inputs(session_id.to_owned())
        .await
        .expect("queued inputs should load");
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].state, QueuedInputState::Injected.as_str());
    let transition_count = state
        .journal_store
        .runtime_generation_transition_count_for_test(
            session_id,
            run_id,
            RuntimeGenerationLane::Run,
            RuntimeGenerationTransitionKind::SteerSuperseded,
        )
        .expect("steer transition count should query");
    assert_eq!(transition_count, 1);
}

#[tokio::test]
async fn active_run_steering_revokes_inherited_session_tool_approval() {
    let state = build_test_runtime_state(false);
    let context = RequestContext {
        principal: "user:ops".to_owned(),
        device_id: "device-active-steering".to_owned(),
        channel: Some("cli".to_owned()),
    };
    let session_id = "session-active-steer-approval";
    let approval = ToolApprovalOutcome {
        approval_id: "approval-active-steer".to_owned(),
        approved: true,
        reason: "allow_session".to_owned(),
        decision: ApprovalDecision::Allow,
        decision_scope: ApprovalDecisionScope::Session,
        decision_scope_ttl_ms: None,
    };
    state.remember_tool_approval(&context, session_id, "tool:palyra.process.run", &approval);
    assert!(state
        .resolve_cached_tool_approval(&context, session_id, "tool:palyra.process.run")
        .is_some());
    let mut approval_cache_generation =
        Some(state.tool_approval_cache_generation_for_session(&context, session_id));
    let inherited_generation = approval_cache_generation;

    revoke_inherited_tool_approvals_after_steering(
        &state,
        &context,
        session_id,
        &mut approval_cache_generation,
    );

    assert!(state
        .resolve_cached_tool_approval(&context, session_id, "tool:palyra.process.run")
        .is_none());
    assert_ne!(approval_cache_generation, inherited_generation);
}

#[tokio::test]
async fn active_run_drain_leaves_followup_for_next_turn() {
    let state = build_test_runtime_state(false);
    let session_id = "session-followup-boundary";
    let run_id = "run-followup-boundary";
    start_test_run(&state, session_id, run_id);
    let (_, initial_generation) = state
        .runtime_generation_for_run(run_id.to_owned())
        .await
        .expect("generation lookup should succeed")
        .expect("run should have an active generation");
    persist_active_queued_input(
        &state,
        session_id,
        run_id,
        "queued-followup-boundary",
        QueueMode::Followup,
        QueuedInputDeliveryBoundary::NextTurn,
        initial_generation,
    )
    .await;
    let mut active_flow_control = Some(
        RunStreamFlowControl::new(initial_generation, Duration::from_secs(60))
            .expect("flow control should initialize"),
    );
    let mut loop_state =
        AgentRunLoopState::new(vec![ProviderMessage::user_text("initial")], 4, 8, 10_000);
    let original_messages = loop_state.messages();
    let mut tape_seq = 0;

    let steering_injected = drain_active_run_steering_before_provider_call(
        &state,
        session_id,
        run_id,
        &mut tape_seq,
        &mut loop_state,
        &mut active_flow_control,
    )
    .await
    .expect("next-turn followup should be ignored by active drain");
    assert!(!steering_injected);

    let (_, observed_generation) = state
        .runtime_generation_for_run(run_id.to_owned())
        .await
        .expect("generation lookup should succeed")
        .expect("run should keep an active generation");
    let queued = state
        .list_orchestrator_queued_inputs(session_id.to_owned())
        .await
        .expect("queued inputs should load");
    assert_eq!(observed_generation, initial_generation);
    assert_eq!(queued[0].state, QueuedInputState::Pending.as_str());
    assert_eq!(loop_state.messages(), original_messages);
    assert_eq!(tape_seq, 0);
}

#[tokio::test]
async fn active_run_drain_supersedes_stale_generation_input() {
    let state = build_test_runtime_state(false);
    let session_id = "session-stale-steer";
    let run_id = "run-stale-steer";
    start_test_run(&state, session_id, run_id);
    let (_, initial_generation) = state
        .runtime_generation_for_run(run_id.to_owned())
        .await
        .expect("generation lookup should succeed")
        .expect("run should have an active generation");
    let stale_generation = initial_generation.next().expect("test generation should advance");
    persist_active_queued_input(
        &state,
        session_id,
        run_id,
        "queued-stale-steer",
        QueueMode::Steer,
        QueuedInputDeliveryBoundary::CurrentRunBeforeProvider,
        stale_generation,
    )
    .await;
    let mut active_flow_control = Some(
        RunStreamFlowControl::new(initial_generation, Duration::from_secs(60))
            .expect("flow control should initialize"),
    );
    let mut loop_state =
        AgentRunLoopState::new(vec![ProviderMessage::user_text("initial")], 4, 8, 10_000);
    let mut tape_seq = 0;

    let steering_injected = drain_active_run_steering_before_provider_call(
        &state,
        session_id,
        run_id,
        &mut tape_seq,
        &mut loop_state,
        &mut active_flow_control,
    )
    .await
    .expect("stale steering should settle without injection");
    assert!(!steering_injected);

    let queued = state
        .list_orchestrator_queued_inputs(session_id.to_owned())
        .await
        .expect("queued inputs should load");
    assert_eq!(queued[0].state, QueuedInputState::Superseded.as_str());
    assert_eq!(loop_state.messages().len(), 1);
    assert_eq!(tape_seq, 0);
}

#[tokio::test]
async fn terminal_run_wins_after_queue_claim_without_injection() {
    let state = build_test_runtime_state(false);
    let session_id = "session-terminal-queue-race";
    let run_id = "run-terminal-queue-race";
    start_test_run(&state, session_id, run_id);
    let (_, initial_generation) = state
        .runtime_generation_for_run(run_id.to_owned())
        .await
        .expect("generation lookup should succeed")
        .expect("run should have an active generation");
    persist_active_queued_input(
        &state,
        session_id,
        run_id,
        "queued-terminal-race",
        QueueMode::Steer,
        QueuedInputDeliveryBoundary::CurrentRunBeforeProvider,
        initial_generation,
    )
    .await;
    state
        .journal_store
        .update_orchestrator_run_state(run_id, RunLifecycleState::Done, None)
        .expect("terminal run should settle before the drain");
    let mut active_flow_control = Some(
        RunStreamFlowControl::new(initial_generation, Duration::from_secs(60))
            .expect("flow control should initialize"),
    );
    let mut loop_state =
        AgentRunLoopState::new(vec![ProviderMessage::user_text("initial")], 4, 8, 10_000);
    let mut tape_seq = 0;

    let steering_injected = drain_active_run_steering_before_provider_call(
        &state,
        session_id,
        run_id,
        &mut tape_seq,
        &mut loop_state,
        &mut active_flow_control,
    )
    .await
    .expect("terminal race should settle the queued input");
    assert!(!steering_injected);

    let queued = state
        .list_orchestrator_queued_inputs(session_id.to_owned())
        .await
        .expect("queued inputs should load");
    assert_eq!(queued[0].state, QueuedInputState::Superseded.as_str());
    assert_eq!(loop_state.messages().len(), 1);
    assert_eq!(tape_seq, 0);
}

#[test]
fn provider_output_projects_structured_tools_to_normalized_events() {
    let output = ProviderTurnOutput {
        full_text: String::new(),
        content_parts: vec![ProviderOutputContentPart::ToolCall {
            proposal_id: "call_1".to_owned(),
            tool_name: "palyra.fs.read".to_owned(),
            input_json: json!({"path": "Cargo.toml"}),
        }],
        finish_reason: ProviderFinishReason::ToolCalls,
        usage: ProviderUsage::new(0, 0, "test"),
        raw_provider_refs: ProviderRawProviderRefs::default(),
        redaction_state: Default::default(),
    };

    let stream = normalized_provider_stream_from_output_v2(&output);

    assert!(stream.events.iter().any(|event| {
        matches!(
            event,
            crate::model_provider::NormalizedProviderEventV2::ToolCallDelta {
                delta_kind:
                    palyra_model_providers::NormalizedProviderToolDeltaKind::Name,
                delta: Some(name_delta),
                ..
            } if name_delta == "palyra.fs.read"
        )
    }));
    assert!(matches!(
        stream.events.last(),
        Some(crate::model_provider::NormalizedProviderEventV2::Terminal {
            finish_reason: Some(ProviderFinishReason::ToolCalls),
            ..
        })
    ));
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
fn shadow_catalog_binding_rejects_legacy_provider_identity() {
    assert!(shadow_catalog_matches_selected_v2_route(
        "anthropic",
        Some("claude-sonnet"),
        "anthropic",
        "claude-sonnet",
    ));
    assert!(!shadow_catalog_matches_selected_v2_route(
        "openai",
        Some("gpt-5"),
        "anthropic",
        "claude-sonnet",
    ));
    assert!(!shadow_catalog_matches_selected_v2_route(
        "anthropic",
        Some("gpt-5"),
        "anthropic",
        "claude-sonnet",
    ));
}

#[test]
fn shadow_candidate_selection_route_difference_is_risky() {
    let generation = RuntimeGeneration::new(7).expect("test generation is non-zero");
    let authoritative_selection = ShadowSelectionSemanticV1::new(
        "legacy-provider".to_owned(),
        "legacy-model".to_owned(),
        "credential:legacy-provider".to_owned(),
        "healthy".to_owned(),
    )
    .expect("legacy selection semantics should validate");
    let candidate_selection =
        selected_v2_shadow_route_semantics(&route_selection_with_fallback(false))
            .expect("V2 selection semantics should validate");
    let authoritative_semantics = ShadowPlanSemanticInputsV1::new(
        authoritative_selection,
        vec![ShadowContextSegmentSemanticV1::new(
            "current_turn".to_owned(),
            "b".repeat(64),
            0,
            "untrusted".to_owned(),
            "volatile".to_owned(),
            None,
        )
        .expect("legacy context semantics should validate")],
        None,
        None,
        512,
        ShadowToolCatalogSemanticV1::new("a".repeat(64), "direct".to_owned(), 0)
            .expect("catalog semantics should validate"),
        ShadowPolicySemanticV1::new(false, false, 0, None, None)
            .expect("policy semantics should validate"),
    )
    .expect("authoritative semantics should validate");
    let authoritative = authoritative_semantics
        .into_authoritative_snapshot(
            generation,
            vec![
                RuntimeErrorPhase::Admission,
                RuntimeErrorPhase::RuntimeSelection,
                RuntimeErrorPhase::ContextAssembly,
                RuntimeErrorPhase::ProviderCall,
                RuntimeErrorPhase::Verification,
                RuntimeErrorPhase::Finalization,
                RuntimeErrorPhase::DeliveryIntent,
            ],
        )
        .expect("authoritative plan should validate");
    let candidate = ShadowCandidatePlannerV1::new(ShadowCandidatePlanInputsV1::from_pre_context(
        generation,
        candidate_selection,
        ShadowV2PreContextInputV1::new("b".repeat(64), 0, 0, 512, false, false, 0)
            .expect("candidate pre-context input should validate"),
        ShadowToolCatalogSemanticV1::new("a".repeat(64), "direct".to_owned(), 0)
            .expect("catalog semantics should validate"),
    ));
    let comparison =
        ShadowComparisonPlansV1::new(authoritative, candidate).expect("comparison should validate");
    let report =
        compare_shadow_comparison_plans_for_test(comparison).expect("comparison should execute");

    assert_eq!(report.classification(), RuntimeDifferentialClassification::Risky);
}

fn harness_lifecycle() -> RunStreamHarnessLifecycle {
    RunStreamHarnessLifecycle {
        diagnostics: AgentHarnessSelectionDiagnostics {
            harness_id: EMBEDDED_PALYRA_HARNESS_ID.to_owned(),
            descriptor_hash: "fnv1a64:test".to_owned(),
            selection_mode: AgentHarnessSelectionMode::Embedded,
            support_outcome: AgentHarnessSupportOutcome::Supported,
            reason_code: "harness.embedded_default".to_owned(),
            fallback_used: false,
            fallback_policy: "not_applicable".to_owned(),
            embedded_default: true,
        },
        trace_context: palyra_common::redaction::redact_diagnostic_text(
            "trace?access_token=secret-token",
        ),
        external: None,
    }
}

fn harness_start_request<'a>() -> RunStreamHarnessStartRequest<'a> {
    RunStreamHarnessStartRequest {
        session_id: "session-01J00000000000000000000000",
        provider_id: "openai-primary",
        model_id: "gpt-4o-mini",
        channel_kind: "operator_cli",
        trace_context: "trace-1",
        mutating: false,
    }
}

#[test]
fn run_stream_harness_config_resolves_plugin_only_when_preview_is_active() {
    let mut config = AgentHarnessRegistryConfig {
        mode: RuntimePreviewMode::PreviewOnly,
        harnesses: vec![
            AgentHarnessConfig {
                id: "disabled.plugin".to_owned(),
                enabled: false,
                kind: "plugin".to_owned(),
            },
            AgentHarnessConfig {
                id: "acme.agent_harness".to_owned(),
                enabled: true,
                kind: "agent_harness_plugin".to_owned(),
            },
        ],
    };

    assert_eq!(configured_run_stream_agent_harness_plugin_id(&config), Some("acme.agent_harness"));
    assert_eq!(
        run_stream_agent_harness_selection_mode(config.mode),
        AgentHarnessSelectionMode::PreferredPlugin
    );

    config.mode = RuntimePreviewMode::Enabled;
    assert_eq!(
        run_stream_agent_harness_selection_mode(config.mode),
        AgentHarnessSelectionMode::ExplicitPlugin
    );

    config.mode = RuntimePreviewMode::Disabled;
    assert_eq!(configured_run_stream_agent_harness_plugin_id(&config), None);
    assert_eq!(
        run_stream_agent_harness_selection_mode(config.mode),
        AgentHarnessSelectionMode::Embedded
    );
}

#[test]
fn run_stream_harness_config_resolves_only_the_canonical_codex_runtime() {
    let mut config = AgentHarnessRegistryConfig {
        mode: RuntimePreviewMode::Enabled,
        harnesses: vec![
            AgentHarnessConfig {
                id: "custom-codex".to_owned(),
                enabled: true,
                kind: "codex_app_server".to_owned(),
            },
            AgentHarnessConfig {
                id: CODEX_MANAGED_RUNTIME_ID.to_owned(),
                enabled: true,
                kind: "codex".to_owned(),
            },
        ],
    };

    assert_eq!(configured_run_stream_codex_harness_id(&config), Some(CODEX_MANAGED_RUNTIME_ID));
    config.mode = RuntimePreviewMode::Disabled;
    assert_eq!(configured_run_stream_codex_harness_id(&config), None);
}

#[test]
fn run_stream_harness_selection_payload_is_redacted_and_stable() {
    let lifecycle = harness_lifecycle();
    let selection: Value = serde_json::from_str(
        run_stream_harness_selection_payload(&lifecycle, harness_start_request()).as_str(),
    )
    .expect("selection payload should be JSON");
    let started: Value = serde_json::from_str(
        run_stream_harness_started_payload(&lifecycle, harness_start_request()).as_str(),
    )
    .expect("started payload should be JSON");
    let serialized_started =
        serde_json::to_string(&started).expect("started payload should serialize");

    assert_eq!(selection["event"], HARNESS_SELECTION_EVENT);
    assert_eq!(selection["harness_id"], EMBEDDED_PALYRA_HARNESS_ID);
    assert_eq!(selection["selection_mode"], "embedded");
    assert_eq!(selection["support_outcome"], "supported");
    assert_eq!(selection["selection_reason_code"], "harness.embedded_default");
    assert_eq!(selection["fallback_used"], false);
    assert_eq!(started["event"], HARNESS_RUN_STARTED_EVENT);
    assert!(!serialized_started.contains("secret-token"));
}

#[test]
fn embedded_runtime_selection_metadata_is_distinct_from_harness_rollout_evidence() {
    let selection: Value =
        serde_json::from_str(embedded_run_stream_runtime_selection_payload().as_str())
            .expect("metadata runtime selection payload should be JSON");

    assert_eq!(selection["event"], RUNTIME_SELECTED_METADATA_EVENT);
    assert_eq!(selection["harness_id"], "embedded_run_stream");
    assert_eq!(selection["runtime_id"], RUN_STREAM_HARNESS_RUNTIME_POLICY);
    assert_eq!(selection["route_class"], "primary");
    assert_eq!(selection["schema_hashes"].as_array().map(Vec::len), Some(1));
}

#[tokio::test]
async fn cancelled_settlement_commits_harness_terminal_evidence() {
    let state = build_test_runtime_state(false);
    let session_id = "session-harness-cancelled";
    let run_id = "run-harness-cancelled";
    start_test_run(&state, session_id, run_id);
    let (_, generation) = state
        .runtime_generation_for_run(run_id.to_owned())
        .await
        .expect("generation lookup should succeed")
        .expect("run generation should remain active");
    let flow_control = RunStreamFlowControl::new(generation, Duration::from_secs(60))
        .expect("run flow control should initialize");
    let (sender, _receiver) = mpsc::channel(4);
    let mut run_state = RunStateMachine::default();
    run_state.transition(RunTransition::Accept).expect("run should accept");
    run_state.transition(RunTransition::StartStreaming).expect("run should start");
    let mut tape_seq = 0;
    let lifecycle = harness_lifecycle();

    let effective_state = transition_run_stream_to_cancelled(
        &sender,
        &state,
        &mut run_state,
        run_id,
        &flow_control,
        &mut tape_seq,
        Some(&lifecycle),
    )
    .await
    .expect("cancelled run should settle atomically");

    assert_eq!(effective_state, RunLifecycleState::Cancelled);
    let tape = state.journal_store.orchestrator_tape(run_id).expect("tape should load");
    assert_eq!(
        tape.iter().map(|event| event.event_type.as_str()).collect::<Vec<_>>(),
        [
            "run.runtime_path_summary",
            HARNESS_RUN_CANCELLED_EVENT,
            HARNESS_RUN_CLEANED_UP_EVENT,
            "status",
        ]
    );
    let terminal: Value = serde_json::from_str(tape[1].payload_json.as_str())
        .expect("harness cancellation payload should decode");
    let cleanup: Value = serde_json::from_str(tape[2].payload_json.as_str())
        .expect("harness cleanup payload should decode");
    assert_eq!(terminal["terminal_status"], "cancelled");
    assert_eq!(cleanup["terminal_status"], "cancelled");
    assert_eq!(cleanup["cleanup_completed"], true);
}

#[test]
fn run_stream_harness_terminal_payload_classifies_outcomes() {
    let lifecycle = harness_lifecycle();
    let completed =
        run_stream_harness_terminal_from_outcome(&Ok(RunStreamMessageProcessingOutcome::Continue));
    let timeout = run_stream_harness_terminal_from_outcome(&Err(Status::deadline_exceeded("slow")));
    let bridge_failure = run_stream_harness_terminal_from_outcome(&Err(
        Status::failed_precondition("external bridge unavailable"),
    ));
    let cancelled = run_stream_harness_terminal_from_state(
        crate::orchestrator::RunLifecycleState::Cancelled,
        &Ok(RunStreamMessageProcessingOutcome::Terminate),
    );
    let failed = RunStreamHarnessTerminal {
        status: AgentHarnessAttemptTerminalStatus::Failed,
        classification: AgentHarnessAttemptClassification::ProviderError,
        replay_safety: AgentHarnessAttemptReplaySafety::NotReplaySafe,
    };
    let payload: Value =
        serde_json::from_str(run_stream_harness_terminal_payload(&lifecycle, failed).as_str())
            .expect("terminal payload should be JSON");
    let cleanup: Value =
        serde_json::from_str(run_stream_harness_cleanup_payload(&lifecycle, cancelled).as_str())
            .expect("cleanup payload should be JSON");
    let cancelled_events = run_stream_harness_cancelled_tape_events(Some(&lifecycle));

    assert_eq!(completed.status, AgentHarnessAttemptTerminalStatus::Completed);
    assert_eq!(completed.classification, AgentHarnessAttemptClassification::Ok);
    assert_eq!(timeout.status, AgentHarnessAttemptTerminalStatus::TimedOut);
    assert_eq!(timeout.classification, AgentHarnessAttemptClassification::ProviderError);
    assert_eq!(bridge_failure.status, AgentHarnessAttemptTerminalStatus::Failed);
    assert_eq!(bridge_failure.classification, AgentHarnessAttemptClassification::InternalError);
    assert_eq!(cancelled.status, AgentHarnessAttemptTerminalStatus::Cancelled);
    assert_eq!(run_stream_harness_terminal_event(completed.status), HARNESS_RUN_COMPLETED_EVENT);
    assert_eq!(run_stream_harness_terminal_event(timeout.status), HARNESS_RUN_FAILED_EVENT);
    assert_eq!(run_stream_harness_terminal_event(cancelled.status), HARNESS_RUN_CANCELLED_EVENT);
    assert_eq!(payload["event"], HARNESS_RUN_FAILED_EVENT);
    assert_eq!(payload["terminal_status"], "failed");
    assert_eq!(payload["terminal_classification"], "provider_error");
    assert_eq!(payload["replay_safety"], "not_replay_safe");
    assert!(payload.get("fallback_used").is_none());
    assert_eq!(cleanup["event"], HARNESS_RUN_CLEANED_UP_EVENT);
    assert_eq!(cleanup["terminal_status"], "cancelled");
    assert_eq!(cleanup["terminal_classification"], "cancelled");
    assert_eq!(cleanup["cleanup_completed"], true);
    assert_eq!(cancelled_events.len(), 2);
    assert_eq!(cancelled_events[0].event_type, HARNESS_RUN_CANCELLED_EVENT);
    assert_eq!(cancelled_events[1].event_type, HARNESS_RUN_CLEANED_UP_EVENT);
    assert!(run_stream_harness_cancelled_tape_events(None).is_empty());
}

#[test]
fn run_stream_harness_records_fallback_only_at_selection() {
    let mut lifecycle = harness_lifecycle();
    lifecycle.diagnostics.fallback_used = true;
    lifecycle.diagnostics.reason_code = "harness.preferred_plugin_unavailable".to_owned();
    let selection: Value = serde_json::from_str(
        run_stream_harness_selection_payload(&lifecycle, harness_start_request()).as_str(),
    )
    .expect("selection payload should be JSON");
    let terminal =
        run_stream_harness_terminal_from_outcome(&Ok(RunStreamMessageProcessingOutcome::Continue));
    let terminal_payload: Value =
        serde_json::from_str(run_stream_harness_terminal_payload(&lifecycle, terminal).as_str())
            .expect("terminal payload should be JSON");

    assert_eq!(selection["fallback_used"], true);
    assert!(terminal_payload.get("fallback_used").is_none());
}

#[test]
fn runtime_path_terminal_reason_is_stable_for_success_failure_cancel_and_timeout() {
    use crate::orchestrator::RunLifecycleState;

    assert_eq!(
        run_runtime_path_terminal_reason(
            RunLifecycleState::Done,
            &Ok(RunStreamMessageProcessingOutcome::Continue),
        ),
        "runtime.terminal.completed"
    );
    assert_eq!(
        run_runtime_path_terminal_reason(
            RunLifecycleState::Failed,
            &Ok(RunStreamMessageProcessingOutcome::Terminate),
        ),
        "runtime.terminal.failed"
    );
    assert_eq!(
        run_runtime_path_terminal_reason(
            RunLifecycleState::Cancelled,
            &Err(Status::cancelled("caller cancelled")),
        ),
        "runtime.terminal.cancelled"
    );
    assert_eq!(
        run_runtime_path_terminal_reason(
            RunLifecycleState::Failed,
            &Err(Status::deadline_exceeded("provider timeout")),
        ),
        "runtime.terminal.timed_out"
    );
    assert_eq!(
        run_runtime_path_terminal_reason(
            RunLifecycleState::Failed,
            &Err(Status::permission_denied("policy denied")),
        ),
        "run_stream.status.permission_denied"
    );
    assert_eq!(
        run_runtime_path_terminal_reason(
            RunLifecycleState::Done,
            &Err(Status::cancelled(RUN_STREAM_RESPONSE_CHANNEL_CLOSED_MESSAGE)),
        ),
        "runtime.terminal.completed"
    );
    assert_eq!(
        run_runtime_path_terminal_reason(
            RunLifecycleState::Failed,
            &Err(Status::cancelled(RUN_STREAM_RESPONSE_CHANNEL_CLOSED_MESSAGE)),
        ),
        "runtime.terminal.failed"
    );
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
    let status =
        provider_request_timeout_status("01ARZ3NDEKTSV4RRFFQ69G5FAV", Duration::from_millis(1_250));

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
fn browser_followup_uses_full_failover_aware_provider_deadline() {
    let request = ProviderRequest::from_input_text(
        "summarize browser result".to_owned(),
        false,
        Vec::new(),
        None,
    );
    let (deadline, reason) = effective_provider_request_deadline(
        Duration::from_millis(180_000),
        &route_selection_with_fallback(true),
        &request,
        browser_followup_timeout_context(true),
    );

    assert_eq!(deadline, Duration::from_millis(365_000));
    assert_eq!(reason, ProviderRequestTimeoutReason::BrowserFollowup);
}

#[test]
fn browser_followup_deadline_respects_smaller_provider_timeout() {
    let request = ProviderRequest::from_input_text(
        "summarize browser result".to_owned(),
        false,
        Vec::new(),
        None,
    );
    let (deadline, reason) = effective_provider_request_deadline(
        Duration::from_millis(5_000),
        &route_selection_with_fallback(false),
        &request,
        browser_followup_timeout_context(true),
    );

    assert_eq!(deadline, Duration::from_millis(5_000));
    assert_eq!(reason, ProviderRequestTimeoutReason::BrowserFollowup);
}

#[test]
fn tool_followup_uses_full_failover_aware_provider_deadline() {
    let request = ProviderRequest::from_input_text(
        "summarize file tool results".to_owned(),
        false,
        Vec::new(),
        None,
    );
    let (deadline, reason) = effective_provider_request_deadline(
        Duration::from_millis(180_000),
        &route_selection_with_fallback(true),
        &request,
        tool_followup_timeout_context(true),
    );

    assert_eq!(deadline, Duration::from_millis(365_000));
    assert_eq!(reason, ProviderRequestTimeoutReason::ToolFollowup);
}

#[test]
fn tool_followup_deadline_respects_smaller_provider_timeout() {
    let request = ProviderRequest::from_input_text(
        "summarize file tool results".to_owned(),
        false,
        Vec::new(),
        None,
    );
    let (deadline, reason) = effective_provider_request_deadline(
        Duration::from_millis(5_000),
        &route_selection_with_fallback(false),
        &request,
        tool_followup_timeout_context(true),
    );

    assert_eq!(deadline, Duration::from_millis(5_000));
    assert_eq!(reason, ProviderRequestTimeoutReason::ToolFollowup);
}

#[test]
fn browser_followup_timeout_status_is_actionable() {
    let message = provider_request_timeout_message(
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        Duration::from_millis(60_000),
        ProviderRequestTimeoutReason::BrowserFollowup,
    );

    assert!(message.contains("browser follow-up model turn timed out after 60000ms"));
    assert!(message.contains("browser tool results were already recorded"));
    assert!(message.contains("01ARZ3NDEKTSV4RRFFQ69G5FAV"));
    assert!(!message.contains("model_provider.request_timeout_ms"));
}

#[test]
fn tool_followup_timeout_status_is_actionable() {
    let message = provider_request_timeout_message(
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        Duration::from_millis(180_000),
        ProviderRequestTimeoutReason::ToolFollowup,
    );

    assert!(message.contains("tool follow-up model turn timed out after 180000ms"));
    assert!(message.contains("tool results were already recorded"));
    assert!(message.contains("next tool proposal or final answer"));
    assert!(message.contains("01ARZ3NDEKTSV4RRFFQ69G5FAV"));
    assert!(!message.contains("model_provider.request_timeout_ms"));
}

#[test]
fn browser_followup_waiting_status_names_followup_deadline() {
    let message = provider_waiting_status_message(
        ProviderRequestTimeoutReason::BrowserFollowup,
        20_000,
        180_000,
        180_000,
        Duration::from_millis(180_000),
        Duration::from_millis(180_000),
    );

    assert!(message.contains("waiting for browser follow-up model response"));
    assert!(message.contains("followup_context=true"));
    assert!(message.contains("provider_deadline_shared=true"));
    assert!(message.contains("provider_attempt_timeout_ms=180000"));
}

#[test]
fn tool_followup_waiting_status_names_followup_deadline() {
    let message = provider_waiting_status_message(
        ProviderRequestTimeoutReason::ToolFollowup,
        20_000,
        180_000,
        180_000,
        Duration::from_millis(180_000),
        Duration::from_millis(180_000),
    );

    assert!(message.contains("waiting for post-tool model response"));
    assert!(message.contains("tool_followup_context=true"));
    assert!(message.contains("provider_deadline_shared=true"));
    assert!(message.contains("provider_attempt_timeout_ms=180000"));
}

#[test]
fn tool_catalog_snapshot_phase_timeout_uses_bounded_default() {
    assert_eq!(
        tool_catalog_snapshot_phase_timeout(),
        Duration::from_millis(TOOL_CATALOG_SNAPSHOT_PHASE_TIMEOUT_MS)
    );
    assert_eq!(
        phase_heartbeat_interval(Duration::from_millis(30_000)),
        Duration::from_millis(15_000)
    );
    assert_eq!(
        phase_heartbeat_interval(Duration::from_millis(60_000)),
        Duration::from_millis(20_000)
    );
}

#[test]
fn run_loop_phase_waiting_status_is_machine_readable() {
    let message =
        run_loop_phase_waiting_status_message(RunLoopPhase::ToolCatalogSnapshot, 15_000, 30_000);

    assert_eq!(
        message,
        "progress:agent_loop.phase_waiting phase=tool_catalog_snapshot elapsed_ms=15000 timeout_ms=30000"
    );
}

#[test]
fn run_loop_phase_timeout_status_is_actionable() {
    let message = run_loop_phase_timeout_message(
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        RunLoopPhase::ToolCatalogSnapshot,
        30_001,
        30_000,
    );

    assert!(message.contains("agent loop phase timed out before provider response"));
    assert!(message.contains("phase=tool_catalog_snapshot"));
    assert!(message.contains("timeout_ms=30000"));
    assert!(message.contains("01ARZ3NDEKTSV4RRFFQ69G5FAV"));
    assert!(message.contains("Inspect run tape"));
}

#[test]
fn provider_model_override_is_unset_for_publish_only_routing() {
    let reason_codes = Vec::new();

    assert_eq!(
        provider_model_override_for_routing("suggest", "MiniMax-M3", reason_codes.as_slice()),
        None
    );
    assert_eq!(
        provider_model_override_for_routing("dry_run", "MiniMax-M3", reason_codes.as_slice()),
        None
    );
}

#[test]
fn provider_model_override_is_set_for_session_override_routing() {
    let reason_codes = vec!["session_model_override".to_owned()];

    assert_eq!(
        provider_model_override_for_routing("suggest", "MiniMax-M3", reason_codes.as_slice()),
        Some("MiniMax-M3".to_owned())
    );
    assert_eq!(
        provider_model_override_for_routing("dry_run", "MiniMax-M3", reason_codes.as_slice()),
        Some("MiniMax-M3".to_owned())
    );
}

#[test]
fn provider_model_override_is_set_for_enforced_routing() {
    let reason_codes = Vec::new();

    assert_eq!(
        provider_model_override_for_routing("enforced", "MiniMax-M3", reason_codes.as_slice()),
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
fn delegated_run_admission_requires_exact_child_authority() {
    let cancellation_context = CancellationContextV1 {
        schema_version: RUNTIME_FLOW_CONTROL_SCHEMA_VERSION,
        scope_id: RuntimeOperationId::parse("child_task:admission").expect("scope id"),
        scope: CancellationScopeKind::ChildTask,
        generation: RuntimeGeneration::new(4).expect("generation"),
        parent_scope_id: Some(
            RuntimeOperationId::parse("run:admission-parent").expect("parent scope id"),
        ),
        reason: None,
        deadline_unix_ms: Some(crate::gateway::current_unix_ms().saturating_add(60_000)),
        graceful_settle_ms: 500,
        hard_abort_after_ms: 2_000,
    };
    let parameter_delta = json!({
        "background_task": {
            "schema_version": 1,
            "task_id": "task-01",
            "task_kind": "delegation_prompt",
            "parent_session_id": "parent-session",
            "child_session_id": "child-session",
            "parent_run_id": "parent-run",
            "budget_tokens": 1_000,
            "cancellation_context": cancellation_context,
        }
    })
    .to_string();

    let admission = delegated_run_admission(
        "delegation",
        "child-session",
        Some("parent-run"),
        Some(parameter_delta.as_str()),
    )
    .expect("delegated authority should parse")
    .expect("delegated authority should be present");
    assert_eq!(admission.task_id, "task-01");
    assert_eq!(admission.parent_session_id, "parent-session");
    assert_eq!(admission.child_session_id, "child-session");

    assert!(delegated_run_admission(
        "delegation",
        "wrong-child-session",
        Some("parent-run"),
        Some(parameter_delta.as_str()),
    )
    .is_err());
    assert!(
        delegated_run_admission("delegation", "child-session", Some("parent-run"), None).is_err()
    );
    assert!(delegated_run_admission(
        "manual",
        "child-session",
        Some("parent-run"),
        Some(parameter_delta.as_str()),
    )
    .is_err());
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
    let mut request =
        ProviderRequest::from_input_text(vec!["word"; 1_100].join(" "), false, Vec::new(), None);

    let message = apply_background_budget_guard(&mut request, 1_000, 0)
        .expect_err("oversized background prompt must fail before provider execution");

    assert!(message.contains("background task token budget exhausted"));
    assert_eq!(request.max_output_tokens, None);
}

#[test]
fn background_budget_guard_counts_model_visible_tool_schemas() {
    let mut request = ProviderRequest::from_input_text(
        "short background task".to_owned(),
        false,
        Vec::new(),
        None,
    );
    request.tool_catalog_snapshot = Some(serde_json::json!({
        "tools": [{
            "name": "palyra.example",
            "description": vec!["schema"; 1_100].join(" "),
            "input_schema": {
                "type": "object",
                "properties": {}
            }
        }]
    }));

    let message = apply_background_budget_guard(&mut request, 1_000, 0)
        .expect_err("tool schema overhead must be included before provider execution");

    assert!(message.contains("background task token budget exhausted before provider turn"));
    assert!(message.contains("estimated_input_tokens="));
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
        input_json: br#"{"command":"cmd","args":["/C","whoami"]}"#.to_vec(),
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

    assert!(failure.message.contains("model_behavior_abort"));
    assert!(failure.message.contains("3 repeated malformed palyra.fs.apply_patch calls"));
    assert!(failure.message.contains("workspace_patch_parse.expected_end_patch"));
    assert!(failure.message.contains("Earlier successful tool calls"));
    assert!(!failure.message.contains("Read the current file before retrying"));
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

#[test]
fn repeated_tool_failure_tracker_resets_after_successful_patch_recovery() {
    let malformed = workspace_patch_parse_error_tool_message(
        "toolu_patch_01",
        "palyra.fs.apply_patch failed: patch parse error at line 3, column 1: expected '*** End Patch'",
        "Remove any duplicate terminator or text after the final '*** End Patch', then retry with one complete patch.",
    );
    let successful_patch = successful_tool_message(
        "toolu_patch_02",
        crate::gateway::WORKSPACE_PATCH_TOOL_NAME,
        json!({
            "files_touched": [
                {"path": "src/lib.rs"}
            ]
        }),
    );
    let mut tracker = RepeatedToolFailureTracker::default();

    assert!(tracker.observe(std::slice::from_ref(&malformed)).is_none());
    assert!(tracker.observe(std::slice::from_ref(&malformed)).is_none());
    assert!(tracker.observe(std::slice::from_ref(&successful_patch)).is_none());
    assert!(tracker.observe(std::slice::from_ref(&malformed)).is_none());
    assert!(tracker.observe(std::slice::from_ref(&malformed)).is_none());
    let failure = tracker
        .observe(std::slice::from_ref(&malformed))
        .expect("a new contiguous episode can still terminate after success reset");

    assert!(failure.message.contains("last_successful_tool:palyra.fs.apply_patch"));
    assert!(failure.message.contains("modified_files:[src/lib.rs]"));
    assert!(failure.message.contains("resume_hint:continue_same_session_with_narrow_patch"));
}

#[test]
fn repeated_tool_failure_tracker_resets_after_successful_os_file_write() {
    let malformed = workspace_patch_parse_error_tool_message(
        "toolu_patch_01",
        "palyra.fs.apply_patch failed: patch parse error at line 3, column 1: expected '*** End Patch'",
        "Remove any duplicate terminator or text after the final '*** End Patch', then retry with one complete patch.",
    );
    let successful_write = successful_tool_message(
        "toolu_os_file_01",
        crate::gateway::OS_FILE_TOOL_NAME,
        json!({"path": "C:/work/output.txt"}),
    );
    let mut tracker = RepeatedToolFailureTracker::default();

    assert!(tracker.observe(std::slice::from_ref(&malformed)).is_none());
    assert!(tracker.observe(std::slice::from_ref(&successful_write)).is_none());
    assert!(tracker.observe(std::slice::from_ref(&malformed)).is_none());
    assert!(tracker.observe(std::slice::from_ref(&malformed)).is_none());

    assert!(
        tracker.observe(std::slice::from_ref(&malformed)).is_some(),
        "third malformed patch after the os-file recovery starts a fresh episode"
    );
}

#[test]
fn repeated_tool_failure_tracker_does_not_count_noncontiguous_signatures() {
    let expected_end = workspace_patch_parse_error_tool_message(
        "toolu_patch_01",
        "palyra.fs.apply_patch failed: patch parse error at line 3, column 1: expected '*** End Patch'",
        "Remove any duplicate terminator or text after the final '*** End Patch', then retry with one complete patch.",
    );
    let expected_begin = workspace_patch_parse_error_tool_message(
        "toolu_patch_02",
        "palyra.fs.apply_patch failed: patch parse error at line 1, column 1: expected '*** Begin Patch'",
        "Start the patch with exactly '*** Begin Patch' on its own line, not a Markdown-decorated variant.",
    );
    let mut tracker = RepeatedToolFailureTracker::default();

    assert!(tracker.observe(std::slice::from_ref(&expected_end)).is_none());
    assert!(tracker.observe(std::slice::from_ref(&expected_begin)).is_none());
    assert!(tracker.observe(std::slice::from_ref(&expected_end)).is_none());
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

fn successful_tool_message(proposal_id: &str, tool_name: &str, output: Value) -> ProviderMessage {
    ProviderMessage::tool_result(
        proposal_id,
        json!({
            "success": true,
            "tool_name": tool_name,
            "error": "",
            "output": output
        })
        .to_string(),
    )
}

#[test]
fn wall_clock_budget_message_includes_resume_context() {
    let state = loop_state_after_tool("build a browser app", "palyra.browser.navigate");

    let message = agent_loop_budget_exhausted_message(
        AgentLoopTerminationReason::WallClock,
        &state,
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
    );

    assert!(message.contains("wall-clock budget exhausted"));
    assert!(message.contains("1 tool result"));
    assert!(message.contains("needs_continuation=true"));
    assert!(message.contains("reason_code=wall_clock"));
    assert!(message.contains("active_limits=wall_clock"));
    assert!(message.contains("wall_clock_budget_ms=10000"));
    assert!(message.contains("wall_clock_remaining_ms="));
    assert!(message.contains("model_turn_limit=unlimited"));
    assert!(message.contains("tool_call_limit=unlimited"));
    assert!(message.contains("partial result summary"));
    assert!(message.contains("resume from run 01ARZ3NDEKTSV4RRFFQ69G5FAV"));
    assert!(message.contains("Model turns and tool calls were not the active limit"));
}

#[test]
fn legacy_step_count_message_does_not_mark_needs_continuation() {
    let state = loop_state_after_tool("clean up generated files", "palyra.fs.apply_patch");

    let message = agent_loop_budget_exhausted_message(
        AgentLoopTerminationReason::MaxToolCalls,
        &state,
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
    );

    assert!(message.contains("legacy agent step-count limit observed"));
    assert!(!message.contains("needs_continuation=true"));
    assert!(!message.contains("reason_code=max_tool_calls"));
    assert!(message.contains("partial result summary"));
    assert!(message.contains("resume from run 01ARZ3NDEKTSV4RRFFQ69G5FAV"));
    assert!(message.contains("Step-count limits are disabled for agent runs"));
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
    assert!(message.contains("needs_continuation=true"));
    assert!(message.contains("reason_code=wall_clock"));
    assert!(message.contains("partial result summary"));
    assert!(message.contains("remaining_tool_calls=unlimited"));
    assert!(message.contains("active_limits=wall_clock"));
    assert!(message.contains("tool_call_limit=unlimited"));
    assert!(message.contains("elapsed_ms="));
    assert!(message.contains("Model turns and tool calls were not the active limit"));
    assert!(!message.contains("tool_call.max_calls_per_run"));
}

#[test]
fn browser_followup_timeout_partial_summary_includes_resume_context() {
    let state = loop_state_after_tool("click the local checkout button", "palyra.browser.click");
    let message = browser_followup_timeout_partial_summary(
        "browser follow-up model turn timed out after 60000ms",
        &state,
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
    );

    assert!(is_browser_tool_name("palyra.browser.click"));
    assert_eq!(
        provider_timeout_termination_reason(ProviderRequestTimeoutReason::BrowserFollowup),
        AgentLoopTerminationReason::BrowserFollowupTimeout
    );
    assert!(message.contains("Partial result: I ran 1 tool call"));
    assert!(message.contains("including browser work"));
    assert!(message.contains("follow-up timeout"));
    assert!(message.contains("exact browser tool evidence"));
    assert!(message.contains("Resume this same session"));
    assert!(message.contains("01ARZ3NDEKTSV4RRFFQ69G5FAV"));
}

#[test]
fn tool_followup_timeout_partial_summary_includes_resume_context() {
    let state = loop_state_after_tool("create files and run tests", "palyra.fs.apply_patch");
    let message = tool_followup_timeout_partial_summary(
        "tool follow-up model turn timed out after 120000ms",
        &state,
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
    );

    assert_eq!(
        provider_timeout_termination_reason(ProviderRequestTimeoutReason::ToolFollowup),
        AgentLoopTerminationReason::ToolFollowupTimeout
    );
    assert!(message.contains("Partial result: I ran 1 tool call"));
    assert!(message.contains("after the tool results"));
    assert!(message.contains("follow-up timeout"));
    assert!(message.contains("exact tool evidence"));
    assert!(message.contains("Resume this same session"));
    assert!(message.contains("01ARZ3NDEKTSV4RRFFQ69G5FAV"));
}

#[test]
fn phase_timeout_after_tool_evidence_emits_needs_continuation_checkpoint() {
    let state = loop_state_after_tool("create files and run tests", "palyra.fs.apply_patch");
    let partial = run_loop_phase_timeout_partial_summary(
        RunLoopPhase::ToolCatalogSnapshot,
        "agent loop phase timed out before provider response: phase=tool_catalog_snapshot",
        &state,
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
    );
    let message = agent_loop_terminal_status_message(
        AgentLoopTerminationReason::RunLoopPhaseTimeout,
        &state,
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        partial.as_str(),
    );

    assert!(message.contains("Partial result: I ran 1 tool call"));
    assert!(message.contains("run loop timed out in phase tool_catalog_snapshot"));
    assert!(message.contains("needs_continuation=true"));
    assert!(message.contains("reason_code=run_loop_phase_timeout"));
    assert!(message.contains("run_progress_checkpoint="));
}

#[test]
fn run_loop_phase_timeout_payload_includes_checkpoint_after_tool_evidence() {
    let state = loop_state_after_tool("create files and run tests", "palyra.fs.apply_patch");
    let payload = run_loop_phase_timeout_payload(
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        RunLoopPhase::ToolCatalogSnapshot,
        30_001,
        30_000,
        &state,
    );
    let parsed: Value =
        serde_json::from_str(payload.as_str()).expect("phase timeout payload should be JSON");

    assert_eq!(parsed["event"], "agent_loop.phase_timeout");
    assert_eq!(parsed["phase"], "tool_catalog_snapshot");
    assert_eq!(parsed["completed_tool_calls"], 1);
    assert_eq!(parsed["timeout_ms"], 30_000);
    assert_eq!(parsed["last_checkpoint"]["run_id"], "01ARZ3NDEKTSV4RRFFQ69G5FAV");
}

#[test]
fn followup_timeout_gets_one_recovery_prompt_after_tool_evidence() {
    let browser_state = loop_state_after_tool(
        "Open a local marketing page, capture screenshots, write a report, and patch CSS.",
        "palyra.browser.screenshot",
    );

    let browser_prompt = followup_timeout_recovery_prompt(
        ProviderRequestTimeoutReason::BrowserFollowup,
        "browser follow-up model turn timed out after 60000ms for run 01ARZ3NDEKTSV4RRFFQ69G5FAV",
        &browser_state,
        0,
    )
    .expect("first browser follow-up timeout after tool evidence should be recoverable");

    assert!(browser_prompt.contains("Continue from the existing browser evidence"));
    assert!(browser_prompt.contains("issue exactly one minimal tool call next"));
    assert!(browser_prompt.contains("patch, report, validation, or final summary"));
    assert!(
        followup_timeout_recovery_prompt(
            ProviderRequestTimeoutReason::BrowserFollowup,
            "browser follow-up model turn timed out after 60000ms",
            &browser_state,
            1,
        )
        .is_none(),
        "browser follow-up timeout recovery must be attempted at most once per run"
    );

    let tool_state = loop_state_after_tool(
        "Find current Node.js LTS releases and write reports/node-lts.md.",
        "palyra.process.run",
    );
    let tool_prompt = followup_timeout_recovery_prompt(
        ProviderRequestTimeoutReason::ToolFollowup,
        "tool follow-up model turn timed out after 120000ms",
        &tool_state,
        0,
    )
    .expect("first tool follow-up timeout after tool evidence should be recoverable");

    assert!(tool_prompt.contains("Continue from the existing tool evidence"));
    assert!(tool_prompt.contains("do not rerun completed tools unless"));
    assert!(tool_prompt.contains("requested artifact, report, validation, cleanup"));
    assert!(
        followup_timeout_recovery_prompt(
            ProviderRequestTimeoutReason::ToolFollowup,
            "tool follow-up model turn timed out after 120000ms",
            &tool_state,
            1,
        )
        .is_none(),
        "tool follow-up timeout recovery must be attempted at most once per run"
    );
    assert!(
        followup_timeout_recovery_prompt(
            ProviderRequestTimeoutReason::Provider,
            "model provider turn timed out after 60000ms",
            &tool_state,
            0,
        )
        .is_none(),
        "generic provider timeouts should keep the existing partial-continuation path"
    );
}

#[test]
fn provider_error_after_tool_work_gets_needs_continuation_status_marker() {
    let state = loop_state_after_tool("create a landing page", "palyra.fs.apply_patch");
    let partial = provider_error_partial_summary(
        "model provider response invalid after 2 retries (class=malformed_response)",
        &state,
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
    );

    let message = agent_loop_terminal_status_message(
        AgentLoopTerminationReason::ProviderError,
        &state,
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        partial.as_str(),
    );

    assert!(message.contains("Partial result: I ran 1 tool call"));
    assert!(message.contains("needs_continuation=true"));
    assert!(message.contains("reason_code=provider_error"));
    assert!(message.contains("Resume this same session"));
    assert!(message.contains("resume from run 01ARZ3NDEKTSV4RRFFQ69G5FAV"));
}

#[test]
fn provider_error_without_tool_work_omits_needs_continuation_status_marker() {
    let state = AgentRunLoopState::new(vec![ProviderMessage::user_text("hello")], 4, 8, 10_000);

    let message = agent_loop_terminal_status_message(
        AgentLoopTerminationReason::ProviderError,
        &state,
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "model provider failed before a tool call",
    );

    assert!(!message.contains("needs_continuation=true"));
    assert!(!message.contains("reason_code=provider_error"));
}

#[test]
fn provider_status_recovery_decision_payload_redacts_and_classifies_status() {
    let payload = provider_status_recovery_decision_payload(
        Code::InvalidArgument,
        "model provider request failed (class=context_window_exceeded): sk-secret-token",
        None,
    );
    let parsed: Value =
        serde_json::from_str(payload.as_str()).expect("recovery payload should be JSON");

    assert_eq!(parsed["event_type"], "provider.recovery.decision");
    assert_eq!(parsed["decision"], "compact_and_retry");
    assert_eq!(parsed["reason_code"], "provider.recovery.compact_and_retry");
    assert_eq!(parsed["redaction_level"], "status_message_redacted");
    assert!(!parsed["message"]
        .as_str()
        .expect("message should be a string")
        .contains("sk-secret-token"));
}

#[test]
fn retry_evidence_is_bounded_and_contains_only_stable_attempt_metadata() {
    let attempts = (0_u32..18)
        .map(|attempt_index| ProviderAttemptSummary {
            provider_id: "provider-a".to_owned(),
            model_id: "model-a".to_owned(),
            outcome: "error".to_owned(),
            retryable: true,
            served_from_cache: false,
            reason_code: Some("qa_mock_malformed_output".to_owned()),
            state: Some(ProviderAttemptState {
                attempt_index,
                provider_profile_id: "provider-a".to_owned(),
                credential_id: "credential-ref".to_owned(),
                model_id: "model-a".to_owned(),
                error_class: Some("malformed_response".to_owned()),
                retry_after_ms: None,
                cooldown_until_unix_ms: None,
                prompt_tokens: 0,
                output_tokens: 0,
                cache_tokens: 0,
                estimated_cost_microusd: None,
                final_disposition: "retry".to_owned(),
                repair_hint: None,
            }),
        })
        .collect::<Vec<_>>();

    let (evidence, truncated) = bounded_provider_retry_evidence(attempts.as_slice());

    assert!(truncated);
    assert_eq!(evidence.len(), 16);
    assert_eq!(evidence[0]["attempt_index"], 0);
    assert_eq!(evidence[0]["reason_code"], "qa_mock_malformed_output");
    assert_eq!(evidence[0]["error_class"], "malformed_response");
    assert!(evidence[0].get("credential_id").is_none());
}

#[test]
fn provider_route_evidence_counts_only_executed_identity_changes() {
    let attempt = |provider_id: &str, model_id: &str, outcome: &str| ProviderAttemptSummary {
        provider_id: provider_id.to_owned(),
        model_id: model_id.to_owned(),
        outcome: outcome.to_owned(),
        retryable: outcome == "error",
        served_from_cache: false,
        reason_code: None,
        state: None,
    };
    let attempts = vec![
        attempt("provider-a", "model-a", "error"),
        attempt("provider-a", "model-a", "error"),
        attempt("provider-b", "model-b", "skipped"),
        attempt("provider-c", "model-c", "failover_success"),
    ];

    let (events, truncated) = bounded_provider_route_change_evidence(attempts.as_slice());

    assert!(!truncated);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].from_provider_id, "provider-a");
    assert_eq!(events[0].from_model_id, "model-a");
    assert_eq!(events[0].to_provider_id, "provider-c");
    assert_eq!(events[0].to_model_id, "model-c");
    events[0].validate_shape().expect("route change evidence should validate");
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
    assert!(!should_emit_budget_exhausted_partial_summary(
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
fn budget_exhausted_message_without_tool_evidence_omits_continuation_marker() {
    let state = AgentRunLoopState::new(vec![ProviderMessage::user_text("hello")], 4, 8, 10_000);

    let message = agent_loop_budget_exhausted_message(
        AgentLoopTerminationReason::WallClock,
        &state,
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
    );

    assert!(message.contains("wall-clock budget exhausted"));
    assert!(!message.contains("needs_continuation=true"));
    assert!(!message.contains("reason_code=wall_clock"));
}

#[test]
fn terminal_tool_authorization_failure_refeeds_explicit_approval_denials() {
    let result = RunStreamToolResultForModel {
        proposal_id: "toolu_denied_01".to_owned(),
        tool_name: "palyra.process.run".to_owned(),
        input_json: br#"{"command":"cmd","args":["/C","whoami"]}"#.to_vec(),
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
        input_json: br#"{"command":"node","args":["-e","console.log(1)"]}"#.to_vec(),
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
        input_json: br#"{"path":"generated/temp.txt"}"#.to_vec(),
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
        input_json: br#"{"command":"cmd","args":["/C","exit","1"]}"#.to_vec(),
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
fn run_progress_attempt_canonicalizes_tool_arguments_before_hashing() {
    let first_input = br#"{"path":"src/lib.rs","options":{"b":2,"a":1}}"#.to_vec();
    let second_input = br#"{"options":{"a":1,"b":2},"path":"src/lib.rs"}"#.to_vec();
    let result = |proposal_id: &str, input_json: Vec<u8>| RunStreamToolResultForModel {
        proposal_id: proposal_id.to_owned(),
        tool_name: "palyra.fs.read_file".to_owned(),
        input_json: input_json.clone(),
        outcome: crate::tool_protocol::build_tool_execution_outcome(
            proposal_id,
            "palyra.fs.read_file",
            input_json.as_slice(),
            true,
            br#"{"content":"same"}"#.to_vec(),
            String::new(),
            false,
            "builtin".to_owned(),
            "none".to_owned(),
        ),
    };

    let first_attempt =
        run_progress_attempt_from_tool_result(&result("toolu_read_01", first_input));
    let second_attempt =
        run_progress_attempt_from_tool_result(&result("toolu_read_02", second_input));

    assert_eq!(first_attempt.normalized_input_json, second_attempt.normalized_input_json);
    assert_eq!(
        std::str::from_utf8(first_attempt.normalized_input_json.as_slice()).unwrap(),
        r#"{"options":{"a":1,"b":2},"path":"src/lib.rs"}"#
    );
}

#[test]
fn run_progress_output_normalization_strips_volatile_fields() {
    let first = normalized_tool_output_evidence(
        br#"{"request_id":"req-1","status":"running","timestamp":100}"#,
    );
    let second = normalized_tool_output_evidence(
        br#"{"timestamp":200,"status":"running","request_id":"req-2"}"#,
    );

    assert_eq!(first.hash, second.hash);
    assert_eq!(first.volatile_fields, vec!["request_id", "timestamp"]);
    assert_eq!(second.volatile_fields, vec!["request_id", "timestamp"]);
}

#[test]
fn run_progress_output_normalization_preserves_progress_signal() {
    let evidence = normalized_tool_output_evidence(
        br#"{"status":{"progress_percent":"42%","updated_at":"now"}}"#,
    );

    assert_eq!(evidence.progress_percent, Some(42));
    assert_eq!(evidence.volatile_fields, vec!["updated_at"]);
    assert_eq!(evidence.hash.len(), 64);
}

#[test]
fn failed_browser_console_result_marks_console_status_unknown_for_model() {
    let result = RunStreamToolResultForModel {
        proposal_id: "toolu_console_01".to_owned(),
        tool_name: crate::gateway::BROWSER_CONSOLE_LOG_TOOL_NAME.to_owned(),
        input_json: br#"{"session_id":"browser-session-1"}"#.to_vec(),
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
    let value: Value = serde_json::from_str(content).expect("tool result content should be JSON");

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
        input_json: br#"{"content_text":"remember this"}"#.to_vec(),
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
    let value: Value = serde_json::from_str(content).expect("tool result content should be JSON");

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
fn artifact_read_tool_result_withholds_content_from_provider_message() {
    let output = json!({
        "artifact": {
            "artifact_id": "01JARTIFACTREADTEST000000000",
            "digest_sha256": "b".repeat(64),
            "mime_type": "application/json",
            "size_bytes": 128,
            "sensitivity": "stdout_stderr",
            "tool_name": "palyra.process.run",
            "redacted_preview": "INTERNAL_PROJECT_CODENAME=BLUEJAY",
        },
        "offset_bytes": 0,
        "returned_bytes": 64,
        "eof": true,
        "visibility": "redacted_preview",
        "text": "{\"stdout\":\"INTERNAL_PROJECT_CODENAME=BLUEJAY\\n\"}",
        "bytes_base64": "SU5URVJOQUxfUFJPSkVDVF9DT0RFTkFNRT1CTFVFSkFZCg==",
    });
    let result = RunStreamToolResultForModel {
        proposal_id: "toolu_artifact_read_01".to_owned(),
        tool_name: crate::gateway::ARTIFACT_READ_TOOL_NAME.to_owned(),
        input_json: br#"{"artifact_id":"01JARTIFACTREADTEST000000000"}"#.to_vec(),
        outcome: crate::tool_protocol::build_tool_execution_outcome(
            "toolu_artifact_read_01",
            crate::gateway::ARTIFACT_READ_TOOL_NAME,
            br#"{"artifact_id":"01JARTIFACTREADTEST000000000"}"#,
            true,
            serde_json::to_vec(&output).expect("artifact read fixture should serialize"),
            String::new(),
            false,
            "gateway_artifacts".to_owned(),
            "artifact_scope".to_owned(),
        ),
    };

    let message = tool_result_to_provider_message(&result)
        .expect("artifact read result should serialize for provider");
    let content = match message.content.first() {
        Some(ProviderMessageContentPart::Text { text }) => text,
        _ => panic!("tool result should be serialized as text content"),
    };
    assert!(
        !content.contains("BLUEJAY"),
        "provider-visible artifact read message must not contain preview text: {content}"
    );
    let value: Value = serde_json::from_str(content).expect("tool result content should be JSON");

    assert_eq!(value.get("success").and_then(Value::as_bool), Some(false));
    assert_eq!(value.get("artifact_read_success").and_then(Value::as_bool), Some(true));
    assert_eq!(value.get("provider_visibility").and_then(Value::as_str), Some("withheld"));
    assert!(value.get("text").is_none(), "{value}");
    assert!(value.get("bytes_base64").is_none(), "{value}");
    assert!(value.pointer("/artifact/redacted_preview").is_none(), "{value}");
    assert_eq!(
        value.pointer("/artifact/artifact_id").and_then(Value::as_str),
        Some("01JARTIFACTREADTEST000000000")
    );
    assert_eq!(value.pointer("/read_window/returned_bytes").and_then(Value::as_u64), Some(64));
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
fn tool_repair_audit_runs_for_raw_markup_without_structured_tool() {
    let output = ProviderTurnOutput::text(
        r#"<tool_call name="palyra.fs.read">{"path":"Cargo.toml"}</tool_call>"#.to_owned(),
        ProviderFinishReason::ToolCalls,
        ProviderUsage::new(10, 20, "test"),
        ProviderRawProviderRefs::default(),
    );

    assert!(provider_output_needs_tool_repair_audit(&output));
}

#[test]
fn tool_repair_audit_skips_normal_final_answer_and_structured_tool() {
    let final_answer = ProviderTurnOutput::text(
        "Done.".to_owned(),
        ProviderFinishReason::Stop,
        ProviderUsage::new(10, 20, "test"),
        ProviderRawProviderRefs::default(),
    );
    let structured_tool = ProviderTurnOutput {
        full_text: String::new(),
        content_parts: vec![ProviderOutputContentPart::ToolCall {
            proposal_id: "toolu_test_01".to_owned(),
            tool_name: "palyra.fs.read".to_owned(),
            input_json: json!({"path":"Cargo.toml"}),
        }],
        finish_reason: ProviderFinishReason::ToolCalls,
        usage: ProviderUsage::new(10, 20, "test"),
        raw_provider_refs: ProviderRawProviderRefs::default(),
        redaction_state: Default::default(),
    };

    assert!(!provider_output_needs_tool_repair_audit(&final_answer));
    assert!(!provider_output_needs_tool_repair_audit(&structured_tool));
}

#[test]
fn incomplete_final_answer_without_tools_detects_bare_ack() {
    let message = incomplete_final_answer_without_tools(Some("done"), None)
        .expect("bare acknowledgement must not be accepted as a final answer");

    assert!(message.contains("bare acknowledgement"));
}

#[test]
fn incomplete_final_answer_without_tools_allows_requested_exact_ack() {
    assert!(incomplete_final_answer_without_tools(Some("OK"), Some("Acknowledge exactly OK."))
        .is_none());
}

#[test]
fn incomplete_final_answer_without_tools_allows_requested_reply_only_ack_sentinel() {
    assert!(incomplete_final_answer_without_tools(
        Some("ACK-READY-4"),
        Some("Reply ACK-READY-4 only.")
    )
    .is_none());
}

#[test]
fn incomplete_final_answer_without_tools_rejects_unrequested_ack_sentinel() {
    let message = incomplete_final_answer_without_tools(Some("ACK-READY-4"), None)
        .expect("unrequested ACK sentinel must not be accepted as a final answer");

    assert!(message.contains("bare acknowledgement"));
}

#[test]
fn incomplete_final_answer_without_tools_detects_deferred_work() {
    let message = incomplete_final_answer_without_tools(
        Some("The workspace is empty. I\u{2019}ll create the todo app files and run the tests."),
        None,
    )
    .expect("deferred tool work must not be accepted as a final answer");

    assert!(message.contains("planning or intent statement"));
}

#[test]
fn incomplete_final_answer_without_tools_allows_negated_deferred_work() {
    assert!(incomplete_final_answer_without_tools(
        Some("I will not edit files because you asked only for an explanation."),
        None
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
fn empty_final_without_tools_gets_one_recovery_prompt() {
    let state = AgentRunLoopState::new(
        vec![ProviderMessage::user_text("Explain the current status.".to_owned())],
        4,
        8,
        10_000,
    );

    let prompt = final_answer_recovery_prompt(
        "model returned an empty final answer without executing any requested tools",
        &state,
        false,
    )
    .expect("empty no-tool final answer should get one recovery turn");

    assert!(prompt.contains("user-visible final answer"));
    assert!(
        final_answer_recovery_prompt(
            "model returned an empty final answer without executing any requested tools",
            &state,
            true,
        )
        .is_none(),
        "empty no-tool recovery must remain bounded"
    );
}

#[test]
fn reasoning_only_terminal_outcome_gets_recovery_prompt() {
    let state = AgentRunLoopState::new(
        vec![ProviderMessage::user_text("Summarize the repo.".to_owned())],
        4,
        8,
        10_000,
    );
    let terminal_outcome = TerminalOutcomeClassification::runtime(
        TerminalOutcomeClass::ReasoningOnly,
        "terminal_outcome.reasoning_only",
    );

    let message = incomplete_terminal_outcome_message(
        &terminal_outcome,
        Some("Reasoning: I need to inspect the repo."),
        &state,
    )
    .expect("reasoning-only output must not be accepted as final");
    let prompt = final_answer_recovery_prompt(message.as_str(), &state, false)
        .expect("reasoning-only output should get one recovery turn");

    assert!(message.contains("reasoning-only"));
    assert!(prompt.contains("analysis-only text"));
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
fn summary_only_closeout_without_tools_gets_recovery_prompt() {
    let state = AgentRunLoopState::new(
        vec![ProviderMessage::user_text(
            "Stop the active run and provide a final-only summary without running any more tools.",
        )],
        4,
        8,
        10_000,
    );

    let prompt = final_answer_recovery_prompt(
        "model returned a planning or intent statement as the final answer without executing any tools",
        &state,
        false,
    )
    .expect("summary-only closeout should get one no-tool recovery turn");

    assert!(prompt.contains("summary-only closeout"));
    assert!(prompt.contains("do not call tools"));
    assert!(
        final_answer_recovery_prompt(
            "model returned a planning or intent statement as the final answer without executing any tools",
            &state,
            true,
        )
        .is_none(),
        "summary-only recovery must still be bounded"
    );
}

#[test]
fn ordinary_no_tool_deferred_work_does_not_get_recovery_prompt() {
    let state = AgentRunLoopState::new(
        vec![ProviderMessage::user_text("Create fixtures/cz-validator with tests.")],
        4,
        8,
        10_000,
    );

    assert!(
        final_answer_recovery_prompt(
            "model returned a planning or intent statement as the final answer without executing any tools",
            &state,
            false,
        )
        .is_none(),
        "ordinary implementation requests still need real tool evidence"
    );
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
    let message =
        incomplete_final_answer_without_tools(Some("I created the file and tests passed."), None)
            .expect("tool-work claims need tool evidence");

    assert!(message.contains("without any successful tool results"));
}

#[test]
fn incomplete_final_answer_without_tools_allows_plain_answers() {
    assert!(incomplete_final_answer_without_tools(
        Some("Use `cargo test -p palyra-daemon` to run the daemon tests."),
        None
    )
    .is_none());
}

#[test]
fn incomplete_terminal_final_answer_rejects_ack_for_requested_tool_work() {
    let state =
        loop_state_after_tool("Create fixtures/landing-page and verify it.", "palyra.fs.list_dir");
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
            ProviderMessage::user_text("Create fixtures/landing-page and verify it.".to_owned()),
        ],
        4,
        8,
        10_000,
    );
    state.set_direct_user_input("Create fixtures/landing-page and verify it.");
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
    let state =
        loop_state_after_tool("Create fixtures/notes-api and run tests.", "palyra.fs.apply_patch");

    assert!(incomplete_terminal_final_answer(
        Some("Created fixtures/notes-api and summarized the changed files."),
        &state,
    )
    .is_none());
}

#[test]
fn incomplete_terminal_final_answer_allows_read_claim_after_read_tool() {
    let state = loop_state_after_tool("Read README.md and summarize it.", "palyra.fs.read_file");

    assert!(incomplete_terminal_final_answer(
        Some("I read the file. It describes the local development workflow."),
        &state,
    )
    .is_none());
}
