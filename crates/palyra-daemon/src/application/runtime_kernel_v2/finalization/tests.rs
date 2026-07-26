use std::{
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use palyra_common::runtime_contracts::{
    BackpressureOverflowAction, BackpressurePolicy, CancellationContextV1, CancellationReason,
    CancellationScopeKind, RuntimeDeliveryIntentId, RuntimeGeneration, RuntimeGenerationLane,
    RuntimeGenerationTransitionKind, RuntimeIdentitySetV1, RuntimeLeaseId, RuntimeOperationId,
    RuntimeRunId, RuntimeSessionId, RuntimeTerminalOutcome, RuntimeTraceId,
    RUNTIME_FLOW_CONTROL_SCHEMA_VERSION,
};
use palyra_connectors::{AttachmentRef, OutboundA2uiUpdate, OutboundMessageRequest};

use crate::{
    application::runtime_kernel_v2::phases::{
        DeliveryDisposition, DeliveryPhase, DeliveryRequest, KernelCancellationScope,
        KernelCancellationSignal, KernelPhaseFuture, KernelPhaseInput, PhaseExecutionContext,
        PhaseLaneAuthority, RuntimePhaseService,
    },
    journal::{
        runtime_finalization::{
            runtime_finalization_now, DeliveryArbitrationActionV2, DeliveryArbitrationDecisionV2,
            FinalOutputArtifactDescriptor,
        },
        JournalConfig, JournalStore, OrchestratorRunStartRequest, OrchestratorSessionUpsertRequest,
        RuntimeGenerationActivateRequest,
    },
};

use super::{
    deterministic_final_envelope_id, digest_array, final_delivery_content_matches,
    final_delivery_destination_binding, final_delivery_request_sha256, reconcile_delivery_outbox,
    recover_pending_final_deliveries, DeliveryOutboxPort, DeliveryOutboxState,
    DeliveryReconciliation, FinalizationHostError, JournalDeliveryService, RetainedFinalDelivery,
    RunFinalProjectionStore, RuntimeDeliverySnapshot, RuntimeDeliveryState,
};

#[derive(Debug)]
struct CountingOutbox {
    inspect_calls: AtomicUsize,
    enqueue_calls: AtomicUsize,
    state: Mutex<DeliveryOutboxState>,
}

impl Default for CountingOutbox {
    fn default() -> Self {
        Self {
            inspect_calls: AtomicUsize::new(0),
            enqueue_calls: AtomicUsize::new(0),
            state: Mutex::new(DeliveryOutboxState::Missing),
        }
    }
}

#[test]
fn outbound_request_digest_binds_every_effect_bearing_payload_family() {
    let baseline = request();
    let baseline_digest =
        final_delivery_request_sha256(&baseline).expect("baseline request should hash");
    let mut variants = Vec::new();

    let mut envelope = baseline.clone();
    envelope.envelope_id = "other-envelope".to_owned();
    variants.push(envelope);
    let mut connector = baseline.clone();
    connector.connector_id = "other-connector".to_owned();
    variants.push(connector);
    let mut destination = baseline.clone();
    destination.conversation_id = "other-conversation".to_owned();
    variants.push(destination);
    let mut reply = baseline.clone();
    reply.reply_thread_id = Some("thread-1".to_owned());
    reply.in_reply_to_message_id = Some("message-1".to_owned());
    variants.push(reply);
    let mut body = baseline.clone();
    body.text = "different body".to_owned();
    variants.push(body);
    let mut broadcast = baseline.clone();
    broadcast.broadcast = true;
    variants.push(broadcast);
    let mut automatic_effects = baseline.clone();
    automatic_effects.auto_ack_text = Some("ack".to_owned());
    automatic_effects.auto_reaction = Some("check".to_owned());
    variants.push(automatic_effects);
    let mut attachment = baseline.clone();
    attachment.attachments.push(AttachmentRef {
        artifact_ref: Some("artifact:1".to_owned()),
        content_hash: Some("ab".repeat(32)),
        ..AttachmentRef::default()
    });
    variants.push(attachment);
    let mut structured = baseline.clone();
    structured.structured_json = Some(br#"{"result":"different"}"#.to_vec());
    variants.push(structured);
    let mut a2ui = baseline.clone();
    a2ui.a2ui_update = Some(OutboundA2uiUpdate {
        surface: "result".to_owned(),
        patch_json: br#"[{"op":"add","path":"/value","value":1}]"#.to_vec(),
    });
    variants.push(a2ui);
    let mut execution_bounds = baseline.clone();
    execution_bounds.timeout_ms += 1;
    execution_bounds.max_payload_bytes += 1;
    variants.push(execution_bounds);

    for variant in variants {
        assert_ne!(
            final_delivery_request_sha256(&variant).expect("variant request should hash"),
            baseline_digest
        );
    }
}

#[test]
fn final_artifact_content_must_match_the_exact_outbound_text() {
    let request = request();
    let matching = super::hex_sha256(request.text.as_bytes());
    assert!(final_delivery_content_matches(&matching, request.text.as_str()));
    assert!(!final_delivery_content_matches(&matching, "changed body"));
}

impl DeliveryOutboxPort for CountingOutbox {
    fn inspect(
        &self,
        _connector_id: &str,
        _envelope_id: &str,
    ) -> Result<DeliveryOutboxState, FinalizationHostError> {
        self.inspect_calls.fetch_add(1, Ordering::SeqCst);
        self.state
            .lock()
            .map(|state| state.clone())
            .map_err(|_| FinalizationHostError::InvalidDeliveryMetadata)
    }

    fn enqueue(&self, _request: &OutboundMessageRequest) -> Result<(), FinalizationHostError> {
        self.enqueue_calls.fetch_add(1, Ordering::SeqCst);
        *self.state.lock().map_err(|_| FinalizationHostError::InvalidDeliveryMetadata)? =
            DeliveryOutboxState::Queued;
        Ok(())
    }
}

fn request() -> OutboundMessageRequest {
    OutboundMessageRequest {
        envelope_id: "envelope-final".to_owned(),
        connector_id: "connector-final".to_owned(),
        conversation_id: "conversation-final".to_owned(),
        reply_thread_id: None,
        in_reply_to_message_id: None,
        text: "retained outside the kernel".to_owned(),
        broadcast: false,
        auto_ack_text: None,
        auto_reaction: None,
        attachments: Vec::new(),
        structured_json: None,
        a2ui_update: None,
        timeout_ms: 1_000,
        max_payload_bytes: 4_096,
    }
}

struct DeliveryFixture {
    journal: Arc<JournalStore>,
    run_generation: RuntimeGeneration,
    run_lease_id: String,
    delivery_generation: RuntimeGeneration,
    delivery_lease_id: String,
}

fn delivery_fixture(path: &Path) -> DeliveryFixture {
    let journal = Arc::new(
        JournalStore::open(JournalConfig {
            db_path: path.to_owned(),
            hash_chain_enabled: false,
            max_payload_bytes: 256 * 1024,
            max_events: 10_000,
        })
        .expect("journal should open"),
    );
    journal
        .upsert_orchestrator_session(&OrchestratorSessionUpsertRequest {
            session_id: "session-final".to_owned(),
            session_key: "session-final".to_owned(),
            session_label: None,
            principal: "user:test".to_owned(),
            device_id: "device-final".to_owned(),
            channel: Some("test".to_owned()),
        })
        .expect("session should persist");
    journal
        .start_orchestrator_run(&OrchestratorRunStartRequest {
            run_id: "run-final".to_owned(),
            session_id: "session-final".to_owned(),
            origin_kind: String::new(),
            origin_run_id: None,
            triggered_by_principal: None,
            parameter_delta_json: None,
            delegated_admission: None,
        })
        .expect("run should persist");
    let run_lease = journal
        .active_runtime_generation_for_run("run-final", RuntimeGenerationLane::Run)
        .expect("run generation should load")
        .expect("run generation should be active");
    let delivery_lease = journal
        .activate_runtime_generation(&RuntimeGenerationActivateRequest {
            session_id: "session-final".to_owned(),
            run_id: Some("run-final".to_owned()),
            lane: RuntimeGenerationLane::Delivery,
            owner: "runtime-finalization-service-test".to_owned(),
            ttl_ms: 60_000,
            transition_kind: RuntimeGenerationTransitionKind::Activated,
            reason_code: "runtime.delivery.service_test_activated".to_owned(),
        })
        .expect("delivery generation should activate");
    DeliveryFixture {
        journal,
        run_generation: run_lease.generation,
        run_lease_id: run_lease.lease_id.into_inner(),
        delivery_generation: delivery_lease.generation,
        delivery_lease_id: delivery_lease.lease_id.into_inner(),
    }
}

struct NeverCancelled;

impl KernelCancellationSignal for NeverCancelled {
    fn current_reason(&self) -> Option<CancellationReason> {
        None
    }

    fn cancelled(&self) -> KernelPhaseFuture<'_, CancellationReason> {
        Box::pin(std::future::pending())
    }
}

fn delivery_input(
    fixture: &DeliveryFixture,
    projection: super::FinalProjectionRef,
) -> KernelPhaseInput<DeliveryPhase, DeliveryRequest> {
    let identities = RuntimeIdentitySetV1::for_run(
        RuntimeTraceId::parse("trace-final").expect("trace id"),
        RuntimeSessionId::parse("session-final").expect("session id"),
        RuntimeRunId::parse("run-final").expect("run id"),
        fixture.run_generation,
    );
    let cancellation = KernelCancellationScope::new(
        CancellationContextV1 {
            schema_version: RUNTIME_FLOW_CONTROL_SCHEMA_VERSION,
            scope_id: RuntimeOperationId::parse("delivery-scope").expect("scope id"),
            scope: CancellationScopeKind::Delivery,
            generation: fixture.run_generation,
            parent_scope_id: None,
            reason: None,
            deadline_unix_ms: Some(i64::MAX),
            graceful_settle_ms: 100,
            hard_abort_after_ms: 1_000,
        },
        Arc::new(NeverCancelled),
    )
    .expect("delivery cancellation scope should validate");
    let execution = PhaseExecutionContext::new(
        cancellation,
        1_000,
        BackpressurePolicy {
            schema_version: RUNTIME_FLOW_CONTROL_SCHEMA_VERSION,
            capacity: 16,
            overflow_action: BackpressureOverflowAction::BlockProducer,
            preserve_terminal: true,
            preserve_approval: true,
            max_summary_bytes: 512,
        },
        PhaseLaneAuthority::from_host_leases(
            RuntimeSessionId::parse("session-final").expect("session id"),
            RuntimeRunId::parse("run-final").expect("run id"),
            fixture.run_generation,
            RuntimeLeaseId::parse(fixture.run_lease_id.as_str()).expect("run lease id"),
            RuntimeGenerationLane::Delivery,
            fixture.delivery_generation,
            RuntimeLeaseId::parse(fixture.delivery_lease_id.as_str()).expect("delivery lease id"),
        ),
        crate::application::runtime_kernel_v2::phases::PhaseAuthorityClass::TerminalDelivery,
        crate::application::runtime_kernel_v2::phases::DurableTracePolicy::IntentBeforeMutation,
    )
    .expect("delivery phase controls should validate");
    KernelPhaseInput::new(
        identities,
        fixture.run_generation,
        execution,
        DeliveryRequest {
            delivery_intent_id: RuntimeDeliveryIntentId::parse("delivery-final")
                .expect("delivery intent id"),
            final_projection: projection,
        },
    )
    .expect("delivery input should validate")
}

fn commit_pending_final_bundle(
    fixture: &DeliveryFixture,
    parent_run_id: Option<&str>,
) -> DeliveryArbitrationDecisionV2 {
    let content_sha256 = super::hex_sha256(request().text.as_bytes());
    let artifact_id = "pending-final-projection";
    let intent_id = "pending-final-projection";
    let mut durable_request = request();
    let destination_binding_sha256 = final_delivery_destination_binding(&durable_request);
    durable_request.envelope_id = deterministic_final_envelope_id(
        "run-final",
        fixture.run_generation,
        fixture.run_lease_id.as_str(),
        intent_id,
        artifact_id,
        content_sha256.as_str(),
        destination_binding_sha256.as_str(),
    );
    let artifact = FinalOutputArtifactDescriptor {
        artifact_id: artifact_id.to_owned(),
        session_id: "session-final".to_owned(),
        run_id: "run-final".to_owned(),
        run_generation: fixture.run_generation,
        run_lease_id: fixture.run_lease_id.clone(),
        terminal_outcome: RuntimeTerminalOutcome::Completed,
        content_sha256: content_sha256.clone(),
        projection_sha256: content_sha256.clone(),
        user_visible: true,
        verification_evidence: Vec::new(),
        missing_artifacts: Vec::new(),
        active_process_state: Vec::new(),
        reason_code: RuntimeTerminalOutcome::Completed.reason_code().to_owned(),
        committed_at_unix_ms: runtime_finalization_now().expect("clock should be available"),
    };
    let decision = DeliveryArbitrationDecisionV2 {
        artifact_id: artifact_id.to_owned(),
        session_id: "session-final".to_owned(),
        run_id: "run-final".to_owned(),
        run_generation: fixture.run_generation,
        parent_run_id: parent_run_id.map(str::to_owned),
        descendant_run_ids: Vec::new(),
        action: DeliveryArbitrationActionV2::Deliver,
        destination_binding_sha256: Some(destination_binding_sha256),
        delivery_intent_id: Some(intent_id.to_owned()),
        connector_id: Some(durable_request.connector_id.clone()),
        outbox_envelope_id: Some(durable_request.envelope_id.clone()),
        content_sha256,
        outbound_request_sha256: Some(
            final_delivery_request_sha256(&durable_request).expect("durable request should hash"),
        ),
        dedupe_key: Some(durable_request.delivery_idempotency_key()),
        outbound_request: Some(durable_request),
        reason_code: "runtime.delivery.arbitration_deliver".to_owned(),
        decided_at_unix_ms: runtime_finalization_now().expect("clock should be available"),
    };
    fixture
        .journal
        .commit_runtime_final_output_with_arbitration(&artifact, &decision)
        .expect("pending final bundle should commit");
    decision
}

#[tokio::test]
async fn canonical_delivery_phase_commits_once_and_replay_does_not_resend() {
    let directory = tempfile::tempdir().expect("temporary directory should create");
    let fixture = delivery_fixture(&directory.path().join("journal.sqlite3"));
    let content = b"retained outside the kernel";
    let content_digest = digest_array(content);
    let content_sha256 = hex::encode(content_digest);
    let projections = Arc::new(RunFinalProjectionStore::default());
    let outbound_request = request();
    let destination_binding_sha256 = final_delivery_destination_binding(&outbound_request);
    let projection = projections
        .retain_visible(
            content,
            RetainedFinalDelivery {
                destination_binding_sha256: destination_binding_sha256.clone(),
                request: outbound_request.clone(),
            },
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .expect("visible projection should be retained");
    let projection_id = projection.id().clone();
    let mut durable_request = outbound_request;
    durable_request.envelope_id = deterministic_final_envelope_id(
        "run-final",
        fixture.run_generation,
        fixture.run_lease_id.as_str(),
        "delivery-final",
        projection_id.as_str(),
        content_sha256.as_str(),
        destination_binding_sha256.as_str(),
    );
    let artifact = FinalOutputArtifactDescriptor {
        artifact_id: projection_id.as_str().to_owned(),
        session_id: "session-final".to_owned(),
        run_id: "run-final".to_owned(),
        run_generation: fixture.run_generation,
        run_lease_id: fixture.run_lease_id.clone(),
        terminal_outcome: RuntimeTerminalOutcome::Completed,
        content_sha256: content_sha256.clone(),
        projection_sha256: content_sha256.clone(),
        user_visible: true,
        verification_evidence: Vec::new(),
        missing_artifacts: Vec::new(),
        active_process_state: Vec::new(),
        reason_code: RuntimeTerminalOutcome::Completed.reason_code().to_owned(),
        committed_at_unix_ms: runtime_finalization_now().expect("clock should be available"),
    };
    let decision = DeliveryArbitrationDecisionV2 {
        artifact_id: projection_id.as_str().to_owned(),
        session_id: "session-final".to_owned(),
        run_id: "run-final".to_owned(),
        run_generation: fixture.run_generation,
        parent_run_id: None,
        descendant_run_ids: Vec::new(),
        action: DeliveryArbitrationActionV2::Deliver,
        destination_binding_sha256: Some(destination_binding_sha256),
        delivery_intent_id: Some("delivery-final".to_owned()),
        connector_id: Some(durable_request.connector_id.clone()),
        outbox_envelope_id: Some(durable_request.envelope_id.clone()),
        content_sha256: content_sha256.clone(),
        outbound_request_sha256: Some(
            final_delivery_request_sha256(&durable_request).expect("durable request should hash"),
        ),
        dedupe_key: Some(durable_request.delivery_idempotency_key()),
        outbound_request: Some(durable_request),
        reason_code: "runtime.delivery.arbitration_deliver".to_owned(),
        decided_at_unix_ms: runtime_finalization_now().expect("clock should be available"),
    };
    fixture
        .journal
        .commit_runtime_final_output_with_arbitration(&artifact, &decision)
        .expect("final artifact and arbitration should commit");
    let outbox = Arc::new(CountingOutbox::default());
    let service =
        JournalDeliveryService::new(Arc::clone(&fixture.journal), projections, outbox.clone());

    let first = service
        .execute(delivery_input(&fixture, projection.clone()))
        .await
        .expect("first delivery should enqueue");
    assert_eq!(
        first.boundary().execution().durable_trace_policy(),
        crate::application::runtime_kernel_v2::phases::DurableTracePolicy::IntentBeforeMutation
    );
    assert_eq!(first.payload().disposition, DeliveryDisposition::Queued);
    assert_eq!(outbox.enqueue_calls.load(Ordering::SeqCst), 1);

    let replay = service
        .execute(delivery_input(&fixture, projection))
        .await
        .expect("delivery replay should reconcile");
    assert_eq!(replay.payload().disposition, DeliveryDisposition::Queued);
    assert_eq!(
        outbox.enqueue_calls.load(Ordering::SeqCst),
        1,
        "a durable queued envelope must not be sent twice"
    );
    assert_eq!(
        fixture
            .journal
            .runtime_delivery_snapshot("delivery-final")
            .expect("delivery state should load")
            .expect("delivery intent should exist")
            .state,
        RuntimeDeliveryState::Queued
    );
}

#[test]
fn terminal_delivery_replay_preserves_evidence_without_touching_outbox() {
    let outbox = CountingOutbox::default();
    let evidence_sha256 = "ab".repeat(32);

    for state in [RuntimeDeliveryState::Delivered, RuntimeDeliveryState::OutcomeUnknown] {
        let reconciliation = reconcile_delivery_outbox(
            &outbox,
            &RuntimeDeliverySnapshot { state, evidence_sha256: Some(evidence_sha256.clone()) },
            &request(),
        )
        .expect("terminal replay should resolve from durable evidence");
        assert!(matches!(
            reconciliation,
            DeliveryReconciliation::Replay {
                state: observed,
                evidence_sha256: ref observed_evidence,
            } if observed == state && observed_evidence == &evidence_sha256
        ));
    }

    assert_eq!(outbox.inspect_calls.load(Ordering::SeqCst), 0);
    assert_eq!(outbox.enqueue_calls.load(Ordering::SeqCst), 0);
}

#[test]
fn startup_recovery_enqueues_the_exact_pending_final_once() {
    let directory = tempfile::tempdir().expect("temporary directory should create");
    let fixture = delivery_fixture(&directory.path().join("journal.sqlite3"));
    let decision = commit_pending_final_bundle(&fixture, Some("parent-run"));
    let outbox = CountingOutbox::default();

    let first = recover_pending_final_deliveries(&fixture.journal, &outbox)
        .expect("first startup recovery should enqueue");
    let second = recover_pending_final_deliveries(&fixture.journal, &outbox)
        .expect("duplicate startup recovery should reconcile");

    assert_eq!(first.artifact_without_intent_count, 1);
    assert_eq!(first.intent_pending_count, 1);
    assert_eq!(second.artifact_without_intent_count, 0);
    assert_eq!(second.intent_pending_count, 1);
    assert_eq!(outbox.enqueue_calls.load(Ordering::SeqCst), 1);
    assert_eq!(first.parent_wake_run_ids, vec!["parent-run".to_owned()]);
    let durable_intent = fixture
        .journal
        .runtime_delivery_intent(
            decision.delivery_intent_id.as_deref().expect("decision should have an intent"),
        )
        .expect("intent should load")
        .expect("intent should exist");
    assert_eq!(
        durable_intent.dedupe_key,
        decision.dedupe_key.expect("decision should have a dedupe key")
    );
    assert_eq!(
        durable_intent.content_sha256, decision.content_sha256,
        "recovery must reuse the original content digest"
    );
}

#[test]
fn startup_recovery_never_retries_an_unknown_delivery() {
    let directory = tempfile::tempdir().expect("temporary directory should create");
    let fixture = delivery_fixture(&directory.path().join("journal.sqlite3"));
    commit_pending_final_bundle(&fixture, None);
    let outbox = CountingOutbox::default();
    *outbox.state.lock().expect("outbox state should lock") = DeliveryOutboxState::OutcomeUnknown;

    let first = recover_pending_final_deliveries(&fixture.journal, &outbox)
        .expect("unknown outcome should reconcile");
    let second = recover_pending_final_deliveries(&fixture.journal, &outbox)
        .expect("unknown outcome replay should reconcile");

    assert_eq!(first.outcome_unknown_count, 1);
    assert_eq!(second.outcome_unknown_count, 1);
    assert_eq!(outbox.enqueue_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        fixture
            .journal
            .runtime_delivery_snapshot("pending-final-projection")
            .expect("snapshot should load")
            .expect("snapshot should exist")
            .state,
        RuntimeDeliveryState::OutcomeUnknown
    );
}

#[test]
fn dead_letter_operator_retry_advances_only_after_ack() {
    let directory = tempfile::tempdir().expect("temporary directory should create");
    let fixture = delivery_fixture(&directory.path().join("journal.sqlite3"));
    commit_pending_final_bundle(&fixture, None);
    let outbox = CountingOutbox::default();
    *outbox.state.lock().expect("outbox state should lock") = DeliveryOutboxState::DeadLetter;

    let dead = recover_pending_final_deliveries(&fixture.journal, &outbox)
        .expect("dead letter should remain operator-visible");
    assert_eq!(dead.dead_letter_count, 1);
    assert_eq!(outbox.enqueue_calls.load(Ordering::SeqCst), 0);

    *outbox.state.lock().expect("outbox state should lock") = DeliveryOutboxState::Queued;
    let pending = recover_pending_final_deliveries(&fixture.journal, &outbox)
        .expect("operator retry should remain pending");
    assert_eq!(pending.intent_pending_count, 1);
    assert_eq!(
        fixture
            .journal
            .runtime_delivery_snapshot("pending-final-projection")
            .expect("snapshot should load")
            .expect("snapshot should exist")
            .state,
        RuntimeDeliveryState::DeadLetter,
        "queued retry must not erase dead-letter evidence"
    );

    *outbox.state.lock().expect("outbox state should lock") =
        DeliveryOutboxState::Delivered { native_message_id: "provider-ack".to_owned() };
    let acknowledged = recover_pending_final_deliveries(&fixture.journal, &outbox)
        .expect("provider ack should advance the lineage");
    assert_eq!(acknowledged.acknowledged_count, 1);
    assert_eq!(
        fixture
            .journal
            .runtime_delivery_snapshot("pending-final-projection")
            .expect("snapshot should load")
            .expect("snapshot should exist")
            .state,
        RuntimeDeliveryState::Delivered
    );
    assert_eq!(outbox.enqueue_calls.load(Ordering::SeqCst), 0);
}
