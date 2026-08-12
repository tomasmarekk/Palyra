//! Regression tests for replay-safe objective guard charging, progress resets,
//! verification blocks, and authoritative-V2 automatic plan initialization.

use super::*;
use crate::journal::objective_continuation::ObjectiveAttemptReserveRequest;

struct GuardFixture {
    _root: tempfile::TempDir,
    db_path: PathBuf,
    store: JournalStore,
    objective_id: String,
    session_id: String,
    root_run_id: String,
}

impl GuardFixture {
    fn reserve_attempt(
        &self,
        generation: u64,
        decision: ObjectiveContinuationDecision,
    ) -> ObjectiveProgressObservation {
        let attempt_id = Ulid::generate().to_string();
        let judge_task_id = Ulid::generate().to_string();
        self.store
            .reserve_objective_attempt(&ObjectiveAttemptReserveRequest {
                attempt_id: attempt_id.clone(),
                objective_id: self.objective_id.clone(),
                routine_id: None,
                session_id: self.session_id.clone(),
                root_run_id: self.root_run_id.clone(),
                source_run_id: self.root_run_id.clone(),
                source_run_generation: generation,
                judge_task_id,
                owner_principal: "user:guard".to_owned(),
                device_id: "device-guard".to_owned(),
                channel: Some("cli".to_owned()),
                judge_payload_json: r#"{"schema_version":1}"#.to_owned(),
                contract_sha256: "a".repeat(64),
                budget_tokens: 1_024,
                workgraph_id: None,
            })
            .expect("objective attempt should reserve");
        let guard =
            self.store.connection.lock().expect("journal connection lock should remain healthy");
        guard
            .execute(
                r#"
                    UPDATE objective_continuation_attempts_v1
                    SET decision = ?2, state = 'decision_pending'
                    WHERE attempt_ulid = ?1
                "#,
                params![attempt_id, decision.as_str()],
            )
            .expect("test attempt decision should settle");
        drop(guard);
        ObjectiveProgressObservation {
            attempt_id,
            objective_id: self.objective_id.clone(),
            session_id: self.session_id.clone(),
            root_run_id: self.root_run_id.clone(),
            source_run_id: self.root_run_id.clone(),
            source_run_generation: generation,
            decision,
            runs_delta: 1,
            turns_delta: 1,
            provider_calls_delta: 1,
            tokens_delta: 10,
            cost_micros_delta: 20,
            wall_time_ms_delta: 30,
            progress_detected: false,
            progress_sha256: Some("b".repeat(64)),
            plan_sha256: Some("c".repeat(64)),
            tool_error_sha256: None,
            parse_failure: false,
            verification_status: ObjectiveVerificationStatus::NotRequired,
            verification_reason_code: None,
            verification_evidence_json: r#"{"guard":"not_required"}"#.to_owned(),
            missing_artifacts_json: "[]".to_owned(),
        }
    }
}

fn fixture() -> GuardFixture {
    let root = tempfile::tempdir().expect("temporary journal root should create");
    let db_path = root.path().join("journal.db");
    let store = open_store(db_path.clone());
    let session_id = Ulid::generate().to_string();
    let root_run_id = Ulid::generate().to_string();
    store
        .upsert_orchestrator_session(&OrchestratorSessionUpsertRequest {
            session_id: session_id.clone(),
            session_key: format!("objective-guard:{session_id}"),
            session_label: Some("Objective guard".to_owned()),
            principal: "user:guard".to_owned(),
            device_id: "device-guard".to_owned(),
            channel: Some("cli".to_owned()),
        })
        .expect("session should create");
    store
        .start_orchestrator_run(&OrchestratorRunStartRequest {
            run_id: root_run_id.clone(),
            session_id: session_id.clone(),
            origin_kind: "cron".to_owned(),
            origin_run_id: None,
            triggered_by_principal: Some("user:guard".to_owned()),
            parameter_delta_json: None,
            delegated_admission: None,
        })
        .expect("root run should create");
    GuardFixture {
        _root: root,
        db_path,
        store,
        objective_id: Ulid::generate().to_string(),
        session_id,
        root_run_id,
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

fn evaluate(
    fixture: &GuardFixture,
    observation: ObjectiveProgressObservation,
    policy: ObjectiveGuardPolicy,
) -> ObjectiveGuardEvaluation {
    fixture
        .store
        .evaluate_objective_guard(&ObjectiveGuardEvaluationRequest { policy, observation })
        .expect("objective guard should evaluate")
}

#[test]
fn objective_guard_migration_reopens_at_schema_89() {
    let fixture = fixture();
    let GuardFixture { _root, db_path, store, .. } = fixture;
    drop(store);
    let reopened = open_store(db_path);
    let guard = reopened.connection.lock().expect("journal connection lock should remain healthy");
    let migration: (i64, String) = guard
        .query_row("SELECT version, name FROM schema_migrations WHERE version = 89", [], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .expect("migration 89 marker should load");
    assert_eq!(migration, (89, "objective_guards_and_plan_links".to_owned()));
}

#[test]
fn identical_attempt_replay_does_not_double_charge_across_reopen() {
    let fixture = fixture();
    let observation = fixture.reserve_attempt(1, ObjectiveContinuationDecision::Continue);
    let request =
        ObjectiveGuardEvaluationRequest { policy: ObjectiveGuardPolicy::default(), observation };
    let first =
        fixture.store.evaluate_objective_guard(&request).expect("first evaluation should commit");
    assert!(!first.replayed);
    assert_eq!(first.ledger.turns_consumed, 1);

    let GuardFixture { _root, db_path, store, .. } = fixture;
    drop(store);
    let reopened = open_store(db_path);
    let replay =
        reopened.evaluate_objective_guard(&request).expect("replay should load committed result");
    assert!(replay.replayed);
    assert_eq!(replay.ledger.turns_consumed, 1);
    assert_eq!(
        reopened
            .objective_guard_evaluation_for_attempt(request.observation.attempt_id.as_str())
            .expect("evaluation should load")
            .expect("evaluation should exist")
            .fingerprint,
        first.fingerprint
    );
}

#[test]
fn unchanged_progress_and_plan_pause_bounded_objective() {
    let fixture = fixture();
    let policy = ObjectiveGuardPolicy {
        max_consecutive_no_progress: 1,
        max_consecutive_identical_plan: 1,
        ..ObjectiveGuardPolicy::default()
    };
    let first = fixture.reserve_attempt(1, ObjectiveContinuationDecision::Continue);
    let first = evaluate(&fixture, first, policy.clone());
    assert_eq!(first.disposition, ObjectiveGuardDisposition::Proceed);
    let second = fixture.reserve_attempt(2, ObjectiveContinuationDecision::Continue);
    let second = evaluate(&fixture, second, policy);
    assert_eq!(second.disposition, ObjectiveGuardDisposition::Pause);
    assert_eq!(second.reason_code, "objective.guard.no_progress");
    assert_eq!(second.ledger.consecutive_identical_plan, 1);
}

#[test]
fn repeated_parse_failures_and_budget_exhaustion_pause_with_stable_reasons() {
    let parse_fixture = fixture();
    let policy = ObjectiveGuardPolicy {
        max_consecutive_parse_failures: 2,
        ..ObjectiveGuardPolicy::default()
    };
    let mut first = parse_fixture.reserve_attempt(1, ObjectiveContinuationDecision::Continue);
    first.parse_failure = true;
    first.progress_detected = true;
    assert_eq!(
        evaluate(&parse_fixture, first, policy.clone()).disposition,
        ObjectiveGuardDisposition::Proceed
    );
    let mut second = parse_fixture.reserve_attempt(2, ObjectiveContinuationDecision::Continue);
    second.parse_failure = true;
    second.progress_detected = true;
    let parse_pause = evaluate(&parse_fixture, second, policy);
    assert_eq!(parse_pause.reason_code, "objective.guard.parse_failures");

    let budget_fixture = fixture();
    let mut budget = budget_fixture.reserve_attempt(1, ObjectiveContinuationDecision::Continue);
    budget.turns_delta = 2;
    let budget_pause = evaluate(
        &budget_fixture,
        budget,
        ObjectiveGuardPolicy { max_turns: Some(2), ..ObjectiveGuardPolicy::default() },
    );
    assert_eq!(budget_pause.reason_code, "objective.guard.budget.turns_exhausted");
}

#[test]
fn missing_artifact_verification_blocks_done() {
    let fixture = fixture();
    let mut observation = fixture.reserve_attempt(1, ObjectiveContinuationDecision::Done);
    observation.verification_status = ObjectiveVerificationStatus::MissingArtifacts;
    observation.verification_reason_code = Some("objective.verify.missing_artifacts".to_owned());
    observation.verification_evidence_json = r#"{"checked":true}"#.to_owned();
    observation.missing_artifacts_json = r#"["artifact:missing"]"#.to_owned();
    let evaluation = evaluate(&fixture, observation, ObjectiveGuardPolicy::default());
    assert_eq!(evaluation.disposition, ObjectiveGuardDisposition::Pause);
    assert_eq!(evaluation.reason_code, "objective.guard.verification_missing_artifacts");
}

#[test]
fn user_correction_resets_progress_without_refunding_consumption() {
    let fixture = fixture();
    let mut observation = fixture.reserve_attempt(1, ObjectiveContinuationDecision::Continue);
    observation.progress_sha256 = None;
    observation.plan_sha256 = None;
    let evaluated = evaluate(&fixture, observation, ObjectiveGuardPolicy::default());
    assert_eq!(evaluated.ledger.consecutive_no_progress, 1);
    let now = current_unix_ms().expect("clock should be available");
    let mut guard =
        fixture.store.connection.lock().expect("journal connection lock should remain healthy");
    let transaction = guard.transaction().expect("progress reset transaction should begin");
    let reset = objective_guard_reset_for_session_tx(
        &transaction,
        fixture.session_id.as_str(),
        "objective.guard.user_correction",
        now,
    )
    .expect("progress reset should succeed");
    transaction.commit().expect("progress reset should commit");
    drop(guard);
    assert_eq!(reset, 1);
    let ledger = fixture
        .store
        .objective_budget_ledger(fixture.objective_id.as_str())
        .expect("ledger should load")
        .expect("ledger should exist");
    assert_eq!(ledger.runs_consumed, 1);
    assert_eq!(ledger.turns_consumed, 1);
    assert_eq!(ledger.consecutive_no_progress, 0);
    assert_eq!(ledger.progress_epoch, 1);
    assert_eq!(ledger.progress_reset_count, 1);
    assert!(ledger.last_progress_sha256.is_none());
}

#[test]
fn authoritative_v2_plan_is_atomic_idempotent_and_queryable() {
    let fixture = fixture();
    fixture.reserve_attempt(1, ObjectiveContinuationDecision::Continue);
    let request = V2ComplexPlanEnsureRequest {
        plan_item_id: Ulid::generate().to_string(),
        objective_id: fixture.objective_id.clone(),
        session_id: fixture.session_id.clone(),
        root_run_id: fixture.root_run_id.clone(),
        source_run_id: fixture.root_run_id.clone(),
        owner_principal: "user:guard".to_owned(),
        device_id: "device-guard".to_owned(),
        channel: Some("cli".to_owned()),
        actor_principal: "runtime:v2".to_owned(),
        title: "Complete the objective and verify its success contract".to_owned(),
        focus: "objective.success_contract".to_owned(),
    };
    let created =
        fixture.store.ensure_v2_complex_plan(&request).expect("automatic plan should create");
    assert!(created.created);
    assert!(created.link.is_root);
    assert!(created.link.active);
    assert!(created.plan_item.run_id.is_none());
    assert_eq!(created.plan_item.reason_code, AUTO_PLAN_REASON);

    let mut replay_request = request;
    replay_request.plan_item_id = Ulid::generate().to_string();
    let replay = fixture
        .store
        .ensure_v2_complex_plan(&replay_request)
        .expect("automatic plan replay should return active root");
    assert!(!replay.created);
    assert_eq!(replay.plan_item.plan_item_id, created.plan_item.plan_item_id);
    let links = fixture
        .store
        .plan_objective_links(fixture.objective_id.as_str())
        .expect("objective links should load");
    assert_eq!(links, vec![created.link]);
    let events = fixture
        .store
        .list_agent_plan_events(created.plan_item.plan_item_id.as_str(), 10)
        .expect("plan events should load");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].reason_code, AUTO_PLAN_REASON);
    assert!(fixture
        .store
        .has_active_v2_complex_plan_for_session(fixture.session_id.as_str())
        .expect("active automatic plan should be queryable"));
    let reopened = open_store(fixture.db_path.clone());
    assert!(reopened
        .has_active_v2_complex_plan_for_session(fixture.session_id.as_str())
        .expect("automatic plan should restore after journal reopen"));
    assert_eq!(
        reopened
            .active_plan_item_ids_for_objective(fixture.objective_id.as_str())
            .expect("restored objective plan links should load"),
        vec![created.plan_item.plan_item_id]
    );
}

#[test]
fn unchanged_success_evidence_counts_as_no_progress() {
    let fixture = fixture();
    let policy =
        ObjectiveGuardPolicy { max_consecutive_no_progress: 1, ..ObjectiveGuardPolicy::default() };
    let mut first = fixture.reserve_attempt(1, ObjectiveContinuationDecision::Continue);
    first.progress_detected = true;
    assert_eq!(
        evaluate(&fixture, first, policy.clone()).disposition,
        ObjectiveGuardDisposition::Proceed
    );
    let mut second = fixture.reserve_attempt(2, ObjectiveContinuationDecision::Continue);
    second.progress_detected = true;
    let paused = evaluate(&fixture, second, policy);
    assert_eq!(paused.disposition, ObjectiveGuardDisposition::Pause);
    assert_eq!(paused.reason_code, "objective.guard.no_progress");
}

#[test]
fn non_objective_complex_plan_uses_validated_run_scope() {
    let fixture = fixture();
    let request = V2ComplexPlanEnsureRequest {
        plan_item_id: Ulid::generate().to_string(),
        objective_id: format!("run:{}", fixture.root_run_id),
        session_id: fixture.session_id.clone(),
        root_run_id: fixture.root_run_id.clone(),
        source_run_id: fixture.root_run_id.clone(),
        owner_principal: "user:guard".to_owned(),
        device_id: "device-guard".to_owned(),
        channel: Some("cli".to_owned()),
        actor_principal: "runtime:v2".to_owned(),
        title: "Track and verify this complex task".to_owned(),
        focus: "run.completion".to_owned(),
    };
    let outcome =
        fixture.store.ensure_v2_complex_plan(&request).expect("run-scoped plan should create");
    assert!(outcome.created);
    assert_eq!(outcome.link.objective_id, request.objective_id);
    assert!(fixture
        .store
        .has_active_v2_complex_plan_for_session(fixture.session_id.as_str())
        .expect("active run-scoped plan should be queryable"));
}

#[test]
fn diagnostics_aggregate_pause_reasons_and_completion_turns() {
    let fixture = fixture();
    let mut paused = fixture.reserve_attempt(1, ObjectiveContinuationDecision::Continue);
    paused.turns_delta = 2;
    evaluate(
        &fixture,
        paused,
        ObjectiveGuardPolicy { max_turns: Some(2), ..ObjectiveGuardPolicy::default() },
    );
    let diagnostics =
        fixture.store.objective_guard_diagnostics().expect("diagnostics should aggregate");
    assert_eq!(diagnostics.objectives_tracked, 1);
    assert_eq!(diagnostics.observations_total, 1);
    assert_eq!(diagnostics.pauses_total, 1);
    assert_eq!(diagnostics.turns_consumed, 2);
    assert_eq!(
        diagnostics.pause_reason_counts.get("objective.guard.budget.turns_exhausted"),
        Some(&1)
    );
    assert_eq!(
        fixture
            .store
            .objective_progress_fingerprints(fixture.objective_id.as_str(), 10)
            .expect("fingerprints should load")
            .len(),
        1
    );
}
