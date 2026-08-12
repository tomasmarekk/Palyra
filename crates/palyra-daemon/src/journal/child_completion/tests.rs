//! Regression tests for child completion transactionality, outbox dedupe, and
//! restart recovery. Fixtures intentionally use the public journal lifecycle
//! methods so the tested evidence matches production writes.

use super::*;
use crate::delegation::{
    DelegationExecutionMode, DelegationMemoryScopeKind, DelegationMergeContract,
    DelegationMergeStrategy, DelegationRole, DelegationRuntimeLimits,
};

struct CompletionFixture {
    _root: tempfile::TempDir,
    db_path: PathBuf,
    store: JournalStore,
    parent_run_id: String,
    child_session_id: String,
    task: OrchestratorBackgroundTaskRecord,
}

fn fixture() -> CompletionFixture {
    let root = tempfile::tempdir().expect("temporary journal root should create");
    let db_path = root.path().join("journal.db");
    let store = open_store(db_path.clone());
    let parent_session_id = Ulid::generate().to_string();
    let parent_run_id = Ulid::generate().to_string();
    let child_session_id = Ulid::generate().to_string();
    store
        .upsert_orchestrator_session(&OrchestratorSessionUpsertRequest {
            session_id: parent_session_id.clone(),
            session_key: format!("parent:{parent_session_id}"),
            session_label: Some("parent".to_owned()),
            principal: "user:child-completion".to_owned(),
            device_id: "device-child-completion".to_owned(),
            channel: Some("cli".to_owned()),
        })
        .expect("parent session should create");
    store
        .start_orchestrator_run(&OrchestratorRunStartRequest {
            run_id: parent_run_id.clone(),
            session_id: parent_session_id.clone(),
            origin_kind: "user".to_owned(),
            origin_run_id: None,
            triggered_by_principal: Some("user:child-completion".to_owned()),
            parameter_delta_json: None,
            delegated_admission: None,
        })
        .expect("parent run should start");
    store
        .update_orchestrator_run_state(parent_run_id.as_str(), RunLifecycleState::InProgress, None)
        .expect("parent run should enter progress");
    let task = store
        .create_orchestrator_background_task(&OrchestratorBackgroundTaskCreateRequest {
            task_id: Ulid::generate().to_string(),
            task_kind: AuxiliaryTaskKind::DelegationPrompt.as_str().to_owned(),
            session_id: parent_session_id.clone(),
            child_session_id: Some(child_session_id.clone()),
            parent_run_id: Some(parent_run_id.clone()),
            target_run_id: None,
            planned_child_run_id: Some(Ulid::generate().to_string()),
            queued_input_id: None,
            owner_principal: "user:child-completion".to_owned(),
            device_id: "device-child-completion".to_owned(),
            channel: Some("cli".to_owned()),
            state: AuxiliaryTaskState::Queued.as_str().to_owned(),
            priority: 0,
            max_attempts: 3,
            budget_tokens: 4_096,
            delegation: Some(test_delegation_snapshot()),
            cancellation_context: Some(test_child_task_cancellation_context()),
            not_before_unix_ms: None,
            expires_at_unix_ms: None,
            notification_target_json: None,
            input_text: Some("bounded child objective".to_owned()),
            payload_json: None,
        })
        .expect("child task should create");
    let task = store
        .claim_orchestrator_background_task(&OrchestratorBackgroundTaskClaimRequest {
            task_id: task.task_id,
            expected_revision: task.revision,
            started_at_unix_ms: current_unix_ms().expect("clock should be available"),
        })
        .expect("child task should claim");
    CompletionFixture { _root: root, db_path, store, parent_run_id, child_session_id, task }
}

fn test_delegation_snapshot() -> DelegationSnapshot {
    DelegationSnapshot {
        profile_id: "research".to_owned(),
        display_name: "Research".to_owned(),
        description: None,
        template_id: None,
        role: DelegationRole::Research,
        execution_mode: DelegationExecutionMode::Parallel,
        group_id: "default".to_owned(),
        model_profile: "deterministic".to_owned(),
        tool_allowlist: Vec::new(),
        skill_allowlist: Vec::new(),
        memory_scope: DelegationMemoryScopeKind::ParentSession,
        budget_tokens: 4_096,
        max_attempts: 3,
        merge_contract: DelegationMergeContract {
            strategy: DelegationMergeStrategy::Summarize,
            approval_required: false,
        },
        runtime_limits: DelegationRuntimeLimits::default(),
        agent_id: Some("main".to_owned()),
    }
}

fn test_child_task_cancellation_context() -> CancellationContextV1 {
    CancellationContextV1 {
        schema_version: 1,
        scope_id: RuntimeOperationId::parse("child_task:child-completion")
            .expect("child scope id should validate"),
        scope: CancellationScopeKind::ChildTask,
        generation: RuntimeGeneration::new(1).expect("generation should validate"),
        parent_scope_id: Some(
            RuntimeOperationId::parse("run:child-completion")
                .expect("parent scope id should validate"),
        ),
        reason: None,
        deadline_unix_ms: Some(i64::MAX),
        graceful_settle_ms: 500,
        hard_abort_after_ms: 2_000,
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

fn complete(fixture: &CompletionFixture, result_json: Value) {
    fixture
        .store
        .update_orchestrator_background_task_from_worker(
            &OrchestratorBackgroundTaskWorkerUpdateRequest {
                task_id: fixture.task.task_id.clone(),
                execution_generation: fixture.task.execution_generation,
                state: Some(AuxiliaryTaskState::Succeeded.as_str().to_owned()),
                target_run_id: None,
                last_error: Some(None),
                result_json: Some(Some(result_json.to_string())),
                started_at_unix_ms: None,
                completed_at_unix_ms: Some(Some(
                    current_unix_ms().expect("clock should be available"),
                )),
            },
        )
        .expect("terminal worker update should commit");
}

fn successful_result() -> Value {
    json!({
        "summary": "child finished safely",
        "verification_status": "passed",
        "evidence_refs": ["evidence:verified"],
        "transcript": "raw transcript must never cross the boundary",
        "run": {
            "merge_result": {
                "status": "merged",
                "strategy": "summarize",
                "summary_text": "bounded handoff",
                "warnings": [],
                "failure_category": null,
                "approval_required": false,
                "artifact_references": [{
                    "artifact_id": "artifact-1",
                    "sha256": "abcd"
                }],
                "provenance": []
            }
        }
    })
}

#[test]
fn reference_objects_bound_every_selected_field() {
    let oversized = "x".repeat(CHILD_COMPLETION_REF_FIELD_BYTES * 4);
    let bounded = bounded_reference_array(&json!([{
        "artifact_id": oversized,
        "path": ["nested", "values"],
        "evidence_id": 42,
        "ignored": "not projected"
    }]));
    let object = bounded
        .as_array()
        .and_then(|items| items.first())
        .and_then(Value::as_object)
        .expect("one bounded reference object should remain");

    assert_eq!(
        object.get("artifact_id").and_then(Value::as_str).map(str::len),
        Some(CHILD_COMPLETION_REF_FIELD_BYTES)
    );
    assert!(!object.contains_key("path"));
    assert!(!object.contains_key("evidence_id"));
    assert!(!object.contains_key("ignored"));
}

#[test]
fn terminal_commit_delivers_once_across_restart() {
    let fixture = fixture();
    complete(&fixture, successful_result());

    let envelope = fixture
        .store
        .child_completion_for_task(fixture.task.task_id.as_str())
        .expect("completion lookup should succeed")
        .expect("completion envelope should commit with terminal task");
    assert_eq!(envelope.summary_text, "bounded handoff");
    assert!(!envelope.structured_result_json.contains("raw transcript"));
    assert_eq!(envelope.verification_status, "passed");
    assert_eq!(envelope.merge_safety_verdict, "matched");

    let CompletionFixture { _root, db_path, store, parent_run_id, task, .. } = fixture;
    drop(store);
    let reopened = open_store(db_path.clone());
    let first = reopened
        .reconcile_child_completions()
        .expect("restart reconciliation should deliver the pending intent");
    assert_eq!(first.delivered_announcements, 1);
    let second = reopened
        .reconcile_child_completions()
        .expect("duplicate reconciliation should be idempotent");
    assert_eq!(second.delivered_announcements, 0);
    assert_eq!(completion_tape_count(&reopened, parent_run_id.as_str()), 1);
    let intent = reopened
        .child_announce_intent_for_task(task.task_id.as_str())
        .expect("announce intent lookup should succeed")
        .expect("announce intent should exist");
    assert_eq!(intent.state, "delivered");

    drop(reopened);
    let reopened_again = open_store(db_path);
    let after_restart = reopened_again
        .reconcile_child_completions()
        .expect("second restart reconciliation should succeed");
    assert_eq!(after_restart.delivered_announcements, 0);
}

#[test]
fn merge_conflict_and_parent_cancel_fail_closed() {
    let conflict = fixture();
    let mut conflict_result = successful_result();
    conflict_result["merge_preview_sha256"] = Value::String("different-preview".to_owned());
    complete(&conflict, conflict_result);
    let report =
        conflict.store.reconcile_child_completions().expect("merge conflict should classify");
    assert_eq!(report.manual_review_announcements, 1);
    assert_eq!(completion_tape_count(&conflict.store, conflict.parent_run_id.as_str()), 0);

    let cancelled = fixture();
    complete(&cancelled, successful_result());
    cancelled
        .store
        .connection
        .lock()
        .expect("journal lock should be available")
        .execute(
            "UPDATE orchestrator_runs SET state = 'cancelled' WHERE run_ulid = ?1",
            params![cancelled.parent_run_id],
        )
        .expect("parent should cancel");
    let report =
        cancelled.store.reconcile_child_completions().expect("cancelled parent should reconcile");
    assert_eq!(report.cancelled_announcements, 1);
    assert_eq!(completion_tape_count(&cancelled.store, cancelled.parent_run_id.as_str()), 0);
}

#[test]
fn nested_child_defers_parent_announcement_without_budget_growth() {
    let fixture = fixture();
    complete(&fixture, successful_result());
    fixture
        .store
        .create_orchestrator_background_task(&OrchestratorBackgroundTaskCreateRequest {
            task_id: Ulid::generate().to_string(),
            task_kind: AuxiliaryTaskKind::BackgroundPrompt.as_str().to_owned(),
            session_id: fixture.child_session_id.clone(),
            child_session_id: None,
            parent_run_id: None,
            target_run_id: None,
            planned_child_run_id: None,
            queued_input_id: None,
            owner_principal: "user:child-completion".to_owned(),
            device_id: "device-child-completion".to_owned(),
            channel: Some("cli".to_owned()),
            state: AuxiliaryTaskState::Queued.as_str().to_owned(),
            priority: 0,
            max_attempts: 1,
            budget_tokens: 512,
            delegation: None,
            cancellation_context: None,
            not_before_unix_ms: None,
            expires_at_unix_ms: None,
            notification_target_json: None,
            input_text: Some("nested work".to_owned()),
            payload_json: None,
        })
        .expect("nested task should create");

    let report =
        fixture.store.reconcile_child_completions().expect("nested reconciliation should defer");
    assert_eq!(report.deferred_for_nested_children, 1);
    let recovery = fixture
        .store
        .orphan_child_recovery_for_task(fixture.task.task_id.as_str())
        .expect("recovery lookup should succeed")
        .expect("recovery classification should persist");
    assert_eq!(recovery.budget_tokens, fixture.task.budget_tokens);
}

fn completion_tape_count(store: &JournalStore, parent_run_id: &str) -> usize {
    store
        .orchestrator_tape(parent_run_id)
        .expect("parent tape should load")
        .iter()
        .filter(|event| event.event_type == "child_completion.announced")
        .count()
}
