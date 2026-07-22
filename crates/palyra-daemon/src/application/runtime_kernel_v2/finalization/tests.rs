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
        runtime_finalization::{runtime_finalization_now, FinalOutputArtifactDescriptor},
        JournalConfig, JournalStore, OrchestratorRunStartRequest, OrchestratorSessionUpsertRequest,
        RuntimeGenerationActivateRequest,
    },
};

use super::{
    digest_array, final_delivery_content_matches, final_delivery_destination_binding,
    final_delivery_request_sha256, reconcile_delivery_outbox, DeliveryOutboxPort,
    DeliveryOutboxState, DeliveryReconciliation, FinalizationHostError, JournalDeliveryService,
    RetainedFinalDelivery, RunFinalProjectionStore, RuntimeDeliverySnapshot, RuntimeDeliveryState,
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

#[tokio::test]
async fn canonical_delivery_phase_commits_once_and_replay_does_not_resend() {
    let directory = tempfile::tempdir().expect("temporary directory should create");
    let fixture = delivery_fixture(&directory.path().join("journal.sqlite3"));
    let content = b"retained outside the kernel";
    let content_digest = digest_array(content);
    let content_sha256 = hex::encode(content_digest);
    let projections = Arc::new(RunFinalProjectionStore::default());
    let outbound_request = request();
    let projection = projections
        .retain_visible(
            content,
            RetainedFinalDelivery {
                destination_binding_sha256: final_delivery_destination_binding(&outbound_request),
                request: outbound_request,
            },
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .expect("visible projection should be retained");
    let projection_id = projection.id().clone();
    fixture
        .journal
        .commit_runtime_final_output(&FinalOutputArtifactDescriptor {
            artifact_id: projection_id.as_str().to_owned(),
            session_id: "session-final".to_owned(),
            run_id: "run-final".to_owned(),
            run_generation: fixture.run_generation,
            run_lease_id: fixture.run_lease_id.clone(),
            terminal_outcome: RuntimeTerminalOutcome::Completed,
            content_sha256: content_sha256.clone(),
            projection_sha256: content_sha256,
            user_visible: true,
            verification_evidence: Vec::new(),
            missing_artifacts: Vec::new(),
            active_process_state: Vec::new(),
            reason_code: RuntimeTerminalOutcome::Completed.reason_code().to_owned(),
            committed_at_unix_ms: runtime_finalization_now().expect("clock should be available"),
        })
        .expect("final artifact should commit");
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
