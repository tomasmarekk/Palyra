//! Regression tests for wake coalescing, restart recovery, and user priority.

use super::*;

struct BarrierFixture {
    _root: tempfile::TempDir,
    db_path: PathBuf,
    store: JournalStore,
    session_id: String,
    run_id: String,
}

fn fixture() -> BarrierFixture {
    let root = tempfile::tempdir().expect("temporary journal root should create");
    let db_path = root.path().join("journal.db");
    let store = open_store(db_path.clone());
    let session_id = Ulid::new().to_string();
    let run_id = Ulid::new().to_string();
    store
        .upsert_orchestrator_session(&OrchestratorSessionUpsertRequest {
            session_id: session_id.clone(),
            session_key: format!("wait:{session_id}"),
            session_label: Some("Wait coordinator".to_owned()),
            principal: "user:wait".to_owned(),
            device_id: "device-wait".to_owned(),
            channel: Some("cli".to_owned()),
        })
        .expect("session should create");
    store
        .start_orchestrator_run(&OrchestratorRunStartRequest {
            run_id: run_id.clone(),
            session_id: session_id.clone(),
            origin_kind: "background_prompt".to_owned(),
            origin_run_id: None,
            triggered_by_principal: Some("user:wait".to_owned()),
            parameter_delta_json: None,
            delegated_admission: None,
        })
        .expect("run should create");
    BarrierFixture { _root: root, db_path, store, session_id, run_id }
}

fn open_store(db_path: PathBuf) -> JournalStore {
    JournalStore::open(JournalConfig {
        db_path,
        hash_chain_enabled: false,
        max_payload_bytes: 1024 * 1024,
        max_events: 10_000,
    })
    .expect("journal store should open")
}

fn barrier_request(
    fixture: &BarrierFixture,
    wake_at_unix_ms: Option<i64>,
) -> WaitBarrierCreateRequest {
    WaitBarrierCreateRequest {
        barrier_id: Ulid::new().to_string(),
        owner_kind: "objective_attempt".to_owned(),
        owner_id: Ulid::new().to_string(),
        session_id: fixture.session_id.clone(),
        root_run_id: Some(fixture.run_id.clone()),
        barrier_kind: if wake_at_unix_ms.is_some() {
            WaitBarrierKind::TimeDeadline
        } else {
            WaitBarrierKind::BackgroundTask
        },
        source_kind: if wake_at_unix_ms.is_some() {
            WaitBarrierKind::TimeDeadline.as_str().to_owned()
        } else {
            WaitBarrierKind::BackgroundTask.as_str().to_owned()
        },
        source_id: Ulid::new().to_string(),
        wake_decision: WakeDecision::Run,
        continuation_prompt: Some("Resume only after the durable event.".to_owned()),
        budget_tokens: 2_048,
        attempt_generation: 1,
        wake_at_unix_ms,
        expires_at_unix_ms: None,
        liveness_probe_json: r#"{"schema_version":1}"#.to_owned(),
        active_hours_json: None,
        stale_policy: "cancel".to_owned(),
        reason_code: "wait.test.registered".to_owned(),
    }
}

#[test]
fn repeated_source_events_coalesce_into_one_intent() {
    let fixture = fixture();
    let request = barrier_request(&fixture, None);
    fixture.store.register_wait_barrier(&request).expect("barrier should persist");
    let event = WakeEventRequest {
        source_event_id: Ulid::new().to_string(),
        source_kind: request.source_kind.clone(),
        source_id: request.source_id.clone(),
        source_generation: 1,
        reason_code: "wait.test.completed".to_owned(),
        evidence_json: r#"{"schema_version":1,"status":"completed"}"#.to_owned(),
        occurred_at_unix_ms: 10,
    };

    let first = fixture.store.emit_wake_event(&event).expect("first event should wake");
    let replay = fixture.store.emit_wake_event(&event).expect("replay should deduplicate");
    let second = fixture
        .store
        .emit_wake_event(&WakeEventRequest {
            source_event_id: Ulid::new().to_string(),
            occurred_at_unix_ms: 11,
            ..event
        })
        .expect("second event should coalesce");

    assert_eq!(first.len(), 1);
    assert_eq!(replay[0].source_event_count, 1);
    assert_eq!(second.len(), 1);
    assert_eq!(first[0].intent_id, second[0].intent_id);
    assert_eq!(second[0].source_event_count, 2);
    assert_eq!(second[0].decision, WakeDecision::Coalesce);
}

#[test]
fn due_deadline_survives_reopen_without_duplicate_intent() {
    let fixture = fixture();
    let request = barrier_request(&fixture, Some(10));
    fixture.store.register_wait_barrier(&request).expect("barrier should persist");
    let first =
        fixture.store.materialize_due_wait_barriers(10).expect("deadline should materialize");
    assert_eq!(first.len(), 1);

    let BarrierFixture { _root, db_path, store, .. } = fixture;
    drop(store);
    let reopened = open_store(db_path);
    let replay =
        reopened.materialize_due_wait_barriers(10).expect("restart scan should remain stable");
    assert_eq!(replay.len(), 1);
    assert_eq!(replay[0].intent_id, first[0].intent_id);
    assert_eq!(replay[0].source_event_count, 2);
}

#[test]
fn pending_user_input_preempts_wake_task_reservation() {
    let fixture = fixture();
    let request = barrier_request(&fixture, None);
    fixture.store.register_wait_barrier(&request).expect("barrier should persist");
    let intent = fixture
        .store
        .emit_wake_event(&WakeEventRequest {
            source_event_id: Ulid::new().to_string(),
            source_kind: request.source_kind,
            source_id: request.source_id,
            source_generation: 1,
            reason_code: "wait.test.completed".to_owned(),
            evidence_json: "{}".to_owned(),
            occurred_at_unix_ms: 10,
        })
        .expect("event should wake")
        .pop()
        .expect("matching intent should exist");
    fixture
        .store
        .create_orchestrator_queued_input(&OrchestratorQueuedInputCreateRequest {
            queued_input_id: Ulid::new().to_string(),
            run_id: fixture.run_id.clone(),
            session_id: fixture.session_id,
            state: QueuedInputState::Pending.as_str().to_owned(),
            text: "User input must win.".to_owned(),
            origin_run_id: Some(fixture.run_id),
            queue_mode: "steer".to_owned(),
            delivery_boundary: "current_run_before_provider".to_owned(),
            expected_active_generation: Some(1),
            priority_lane: "operator".to_owned(),
            coalescing_group: None,
            overflow_summary_ref: None,
            safe_boundary_flags_json: "{}".to_owned(),
            decision_reason: "queue.user_preemption".to_owned(),
            attachments_json: "[]".to_owned(),
            queue_outcome_json: r#"{"lifecycle_state":"pending"}"#.to_owned(),
            accepted_at_unix_ms: Some(1),
            policy_snapshot_json: "{}".to_owned(),
            explain_json: "{}".to_owned(),
        })
        .expect("user input should persist");

    let outcome = fixture
        .store
        .reserve_wake_task(intent.intent_id.as_str(), Ulid::new().to_string().as_str())
        .expect("preemption should be typed");
    let WakeTaskReserveOutcome::UserPreempted(cancelled) = outcome else {
        panic!("pending user input must preempt autonomous wake");
    };
    assert_eq!(cancelled.state, "cancelled");
    assert_eq!(cancelled.delivery_outcome, "user_preempted");
}

#[test]
fn source_event_before_registration_is_recovered_once() {
    let fixture = fixture();
    let request = barrier_request(&fixture, None);
    fixture
        .store
        .emit_wake_event(&WakeEventRequest {
            source_event_id: Ulid::new().to_string(),
            source_kind: request.source_kind.clone(),
            source_id: request.source_id.clone(),
            source_generation: 1,
            reason_code: "wait.test.early_completion".to_owned(),
            evidence_json: "{}".to_owned(),
            occurred_at_unix_ms: 10,
        })
        .expect("source event should persist without an active barrier");

    fixture.store.register_wait_barrier(&request).expect("barrier should recover source");
    fixture.store.register_wait_barrier(&request).expect("barrier replay should be idempotent");
    let intents = fixture.store.ready_wake_intents(10).expect("recovered wake should be ready");

    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].source_event_count, 1);
}

#[test]
fn every_typed_barrier_kind_round_trips() {
    let fixture = fixture();
    let kinds = [
        WaitBarrierKind::ProcessSession,
        WaitBarrierKind::TerminalPid,
        WaitBarrierKind::TimeDeadline,
        WaitBarrierKind::Approval,
        WaitBarrierKind::Webhook,
        WaitBarrierKind::FlowStep,
        WaitBarrierKind::DelegationChild,
        WaitBarrierKind::BackgroundTask,
        WaitBarrierKind::ExternalArtifact,
        WaitBarrierKind::UserInput,
    ];
    for (index, kind) in kinds.into_iter().enumerate() {
        let mut request = barrier_request(&fixture, None);
        request.owner_id = format!("owner-{index}");
        request.barrier_kind = kind;
        request.source_kind = kind.as_str().to_owned();
        request.source_id = format!("source-{index}");
        let record =
            fixture.store.register_wait_barrier(&request).expect("typed barrier should persist");
        assert_eq!(record.barrier_kind, kind);
        assert_eq!(record.source_kind, kind.as_str());
    }
}
