//! Regression tests for objective binding dedupe, decision replay, and
//! continuation task reservation across journal reopen.

use super::*;

struct AttemptFixture {
    _root: tempfile::TempDir,
    db_path: PathBuf,
    store: JournalStore,
    request: ObjectiveAttemptReserveRequest,
}

fn fixture() -> AttemptFixture {
    let root = tempfile::tempdir().expect("temporary journal root should create");
    let db_path = root.path().join("journal.db");
    let store = open_store(db_path.clone());
    let session_id = Ulid::new().to_string();
    let run_id = Ulid::new().to_string();
    store
        .upsert_orchestrator_session(&OrchestratorSessionUpsertRequest {
            session_id: session_id.clone(),
            session_key: format!("objective:{session_id}"),
            session_label: Some("Objective".to_owned()),
            principal: "user:objective".to_owned(),
            device_id: "device-objective".to_owned(),
            channel: Some("cli".to_owned()),
        })
        .expect("session should create");
    store
        .start_orchestrator_run(&OrchestratorRunStartRequest {
            run_id: run_id.clone(),
            session_id: session_id.clone(),
            origin_kind: "cron".to_owned(),
            origin_run_id: None,
            triggered_by_principal: Some("user:objective".to_owned()),
            parameter_delta_json: None,
            delegated_admission: None,
        })
        .expect("source run should create");
    AttemptFixture {
        _root: root,
        db_path,
        store,
        request: ObjectiveAttemptReserveRequest {
            attempt_id: Ulid::new().to_string(),
            objective_id: Ulid::new().to_string(),
            routine_id: Some(Ulid::new().to_string()),
            session_id,
            root_run_id: run_id.clone(),
            source_run_id: run_id,
            source_run_generation: 1,
            judge_task_id: Ulid::new().to_string(),
            owner_principal: "user:objective".to_owned(),
            device_id: "device-objective".to_owned(),
            channel: Some("cli".to_owned()),
            judge_payload_json: r#"{"schema_version":1,"objective_id":"test"}"#.to_owned(),
            contract_sha256: "a".repeat(64),
            budget_tokens: 1_024,
            workgraph_id: None,
        },
    }
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

fn guard_request(
    fixture: &AttemptFixture,
    decision: ObjectiveContinuationDecision,
) -> ObjectiveGuardEvaluationRequest {
    ObjectiveGuardEvaluationRequest {
        policy: ObjectiveGuardPolicy::default(),
        observation: ObjectiveProgressObservation {
            attempt_id: fixture.request.attempt_id.clone(),
            objective_id: fixture.request.objective_id.clone(),
            session_id: fixture.request.session_id.clone(),
            root_run_id: fixture.request.root_run_id.clone(),
            source_run_id: fixture.request.source_run_id.clone(),
            source_run_generation: fixture.request.source_run_generation,
            decision,
            runs_delta: 1,
            turns_delta: 1,
            provider_calls_delta: 1,
            tokens_delta: 1,
            cost_micros_delta: 0,
            wall_time_ms_delta: 1,
            progress_detected: true,
            progress_sha256: Some("b".repeat(64)),
            plan_sha256: None,
            tool_error_sha256: None,
            parse_failure: false,
            verification_status: ObjectiveVerificationStatus::Unknown,
            verification_reason_code: None,
            verification_evidence_json: r#"["artifact:one"]"#.to_owned(),
            missing_artifacts_json: "[]".to_owned(),
        },
    }
}

#[test]
fn objective_attempt_reservation_is_deduplicated_across_reopen() {
    let fixture = fixture();
    let reserved =
        fixture.store.reserve_objective_attempt(&fixture.request).expect("attempt should reserve");
    let duplicate = fixture
        .store
        .reserve_objective_attempt(&fixture.request)
        .expect("identical replay should load committed attempt");
    assert_eq!(duplicate, reserved);

    let AttemptFixture { _root, db_path, store, request } = fixture;
    drop(store);
    let reopened = open_store(db_path);
    let after_restart = reopened
        .reserve_objective_attempt(&request)
        .expect("restart replay should remain idempotent");
    assert_eq!(after_restart.attempt_id, request.attempt_id);
    assert_eq!(
        reopened
            .objective_attempt_transitions(request.attempt_id.as_str())
            .expect("transitions should load")
            .len(),
        1
    );
}

#[test]
fn judge_decision_is_first_writer_wins_and_replay_visible() {
    let fixture = fixture();
    fixture.store.reserve_objective_attempt(&fixture.request).expect("attempt should reserve");
    fixture
        .store
        .mark_objective_judge_enqueued(fixture.request.judge_task_id.as_str())
        .expect("judge should mark enqueued");
    let decision = ObjectiveJudgeDecisionRequest {
        judge_task_id: fixture.request.judge_task_id.clone(),
        decision: ObjectiveContinuationDecision::Continue,
        reason_code: "objective.continuation.continue".to_owned(),
        summary_text: "More evidence is required.".to_owned(),
        evidence_refs_json: r#"["artifact:one"]"#.to_owned(),
        next_action: Some("Collect the missing evidence.".to_owned()),
        retry_count: 0,
        next_eligible_at_unix_ms: None,
        guard: guard_request(&fixture, ObjectiveContinuationDecision::Continue),
    };
    let settled = fixture
        .store
        .settle_objective_judge_decision(&decision)
        .expect("judge decision should settle");
    assert_eq!(settled.decision, ObjectiveContinuationDecision::Continue);
    let replay = fixture
        .store
        .settle_objective_judge_decision(&decision)
        .expect("identical decision should replay");
    assert_eq!(replay, settled);

    let mut conflicting = decision;
    conflicting.decision = ObjectiveContinuationDecision::Blocked;
    assert!(fixture.store.settle_objective_judge_decision(&conflicting).is_err());
}

#[test]
fn continuation_task_reservation_survives_restart_without_duplication() {
    let fixture = fixture();
    fixture.store.reserve_objective_attempt(&fixture.request).expect("attempt should reserve");
    fixture
        .store
        .mark_objective_judge_enqueued(fixture.request.judge_task_id.as_str())
        .expect("judge should mark enqueued");
    fixture
        .store
        .settle_objective_judge_decision(&ObjectiveJudgeDecisionRequest {
            judge_task_id: fixture.request.judge_task_id.clone(),
            decision: ObjectiveContinuationDecision::Continue,
            reason_code: "objective.continuation.continue".to_owned(),
            summary_text: "Continue.".to_owned(),
            evidence_refs_json: "[]".to_owned(),
            next_action: Some("Continue safely.".to_owned()),
            retry_count: 0,
            next_eligible_at_unix_ms: None,
            guard: guard_request(&fixture, ObjectiveContinuationDecision::Continue),
        })
        .expect("decision should settle");
    let continuation_task_id = Ulid::new().to_string();
    let reserved = fixture
        .store
        .reserve_objective_continuation_task(
            fixture.request.attempt_id.as_str(),
            continuation_task_id.as_str(),
            "objective.continuation.task_reserved",
        )
        .expect("continuation task should reserve");
    let ObjectiveContinuationTaskReserveOutcome::Reserved(reserved) = reserved else {
        panic!("fixture has no user input that could preempt continuation");
    };
    assert_eq!(reserved.continuation_task_id.as_deref(), Some(continuation_task_id.as_str()));

    let AttemptFixture { _root, db_path, store, request } = fixture;
    drop(store);
    let reopened = open_store(db_path);
    let replay = reopened
        .reserve_objective_continuation_task(
            request.attempt_id.as_str(),
            continuation_task_id.as_str(),
            "objective.continuation.task_reserved",
        )
        .expect("restart should retain continuation identity");
    let ObjectiveContinuationTaskReserveOutcome::Reserved(replay) = replay else {
        panic!("replay should retain its committed continuation reservation");
    };
    assert_eq!(replay.continuation_task_id, Some(continuation_task_id));
}

#[test]
fn pending_user_input_preempts_continuation_reservation_atomically() {
    let fixture = fixture();
    fixture.store.reserve_objective_attempt(&fixture.request).expect("attempt should reserve");
    fixture
        .store
        .mark_objective_judge_enqueued(fixture.request.judge_task_id.as_str())
        .expect("judge should mark enqueued");
    fixture
        .store
        .settle_objective_judge_decision(&ObjectiveJudgeDecisionRequest {
            judge_task_id: fixture.request.judge_task_id.clone(),
            decision: ObjectiveContinuationDecision::Continue,
            reason_code: "objective.continuation.continue".to_owned(),
            summary_text: "Continue.".to_owned(),
            evidence_refs_json: "[]".to_owned(),
            next_action: Some("Continue safely.".to_owned()),
            retry_count: 0,
            next_eligible_at_unix_ms: None,
            guard: guard_request(&fixture, ObjectiveContinuationDecision::Continue),
        })
        .expect("decision should settle");
    fixture
        .store
        .create_orchestrator_queued_input(&OrchestratorQueuedInputCreateRequest {
            queued_input_id: Ulid::new().to_string(),
            run_id: fixture.request.source_run_id.clone(),
            session_id: fixture.request.session_id.clone(),
            state: QueuedInputState::Pending.as_str().to_owned(),
            text: "User correction takes priority.".to_owned(),
            origin_run_id: Some(fixture.request.source_run_id.clone()),
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
        .expect("pending user input should persist");

    let outcome = fixture
        .store
        .reserve_objective_continuation_task(
            fixture.request.attempt_id.as_str(),
            Ulid::new().to_string().as_str(),
            "objective.continuation.task_reserved",
        )
        .expect("preemption should be a typed outcome");

    let ObjectiveContinuationTaskReserveOutcome::UserPreempted(attempt) = outcome else {
        panic!("pending user input must preempt continuation");
    };
    assert_eq!(attempt.state, "decision_pending");
    assert!(attempt.continuation_task_id.is_none());
}
