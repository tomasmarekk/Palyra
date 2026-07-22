//! Durable rollback-actuator regression tests.
//!
//! These fixtures use real Run-generation leases, canonical kernel events, and
//! the side-effect fence journal so rollback posture never comes from caller data.

use std::path::{Path, PathBuf};

use palyra_common::runtime_contracts::{
    ReconciliationStrategy, RuntimeEventEnvelopeV2, RuntimeEventId, RuntimeEventName,
    RuntimeEventPayloadRef, RuntimeGenerationLane, RuntimeIdempotencyClass, RuntimeIdentitySetV1,
    RuntimeOperationId, RuntimeRunId, RuntimeSessionId, RuntimeToolExecutionId, RuntimeTraceId,
    SideEffectFenceState, SideEffectFenceV1, SideEffectRestartPolicy, ToolExecutionSemantics,
    RUNTIME_EVENT_ENVELOPE_SCHEMA_VERSION, SIDE_EFFECT_FENCE_SCHEMA_VERSION,
};
use rusqlite::{params, Connection};
use serde_json::json;

use crate::{
    application::runtime_kernel_v2::{
        profile::{RuntimeKernelCompatibilityOverridesV1, RuntimeKernelProfileConfigV1},
        rollback::VerifiedRuntimeRollbackSafeBoundary,
        selection::{
            resolve_runtime_authority, RuntimeAuthorityProgressEvidence, V2RuntimeAvailability,
        },
        KernelLaneAuthoritySet, KernelState, KernelTransition, RuntimeKernelV2,
        RuntimeKernelVersion,
    },
    config::RuntimeKernelRollbackPolicy,
    journal::{
        JournalConfig, JournalError, JournalStore, OrchestratorRunStartRequest,
        OrchestratorSessionUpsertRequest,
    },
};

use super::{
    rollback::rollback_request_state_for_test, RuntimeKernelTransitionCommitOutcome,
    RuntimeRollbackBoundaryOutcome,
};

struct Fixture {
    store: JournalStore,
    kernel: RuntimeKernelV2,
    authority: KernelLaneAuthoritySet,
    session_id: String,
    run_id: String,
}

fn test_config(db_path: PathBuf) -> JournalConfig {
    JournalConfig {
        db_path,
        hash_chain_enabled: false,
        max_payload_bytes: 256 * 1024,
        max_events: 10_000,
    }
}

fn setup_fixture(db_path: &Path, suffix: &str) -> Fixture {
    let session_id = format!("session_rollback_{suffix}");
    let run_id = format!("run_rollback_{suffix}");
    let store = JournalStore::open(test_config(db_path.to_owned())).expect("journal should open");
    store
        .upsert_orchestrator_session(&OrchestratorSessionUpsertRequest {
            session_id: session_id.clone(),
            session_key: session_id.clone(),
            session_label: None,
            principal: "user:rollback-test".to_owned(),
            device_id: "device_rollback_test".to_owned(),
            channel: Some("cli".to_owned()),
        })
        .expect("session should persist");
    store
        .start_orchestrator_run(&OrchestratorRunStartRequest {
            run_id: run_id.clone(),
            session_id: session_id.clone(),
            origin_kind: String::new(),
            origin_run_id: None,
            triggered_by_principal: None,
            parameter_delta_json: None,
            delegated_admission: None,
        })
        .expect("run should persist");
    let lease = store
        .active_runtime_generation_for_run(run_id.as_str(), RuntimeGenerationLane::Run)
        .expect("generation should load")
        .expect("run generation should be active");
    let identities = RuntimeIdentitySetV1::for_run(
        RuntimeTraceId::parse(format!("trace_{run_id}").as_str())
            .expect("trace id should validate"),
        RuntimeSessionId::parse(session_id.as_str()).expect("session id should validate"),
        RuntimeRunId::parse(run_id.as_str()).expect("run id should validate"),
        lease.generation,
    );
    let profile = RuntimeKernelProfileConfigV1::new(
        RuntimeKernelVersion::V2,
        0,
        RuntimeKernelCompatibilityOverridesV1::none(),
    )
    .expect("V2 profile should validate");
    let decision = resolve_runtime_authority(
        &profile,
        &identities,
        V2RuntimeAvailability::Ready,
        RuntimeAuthorityProgressEvidence::pristine(),
        None,
    )
    .expect("V2 authority should resolve");
    let kernel = RuntimeKernelV2::admit_for_test(
        decision,
        identities.clone(),
        lease.clone(),
        lease.acquired_at_unix_ms,
    )
    .expect("kernel should admit");
    let authority =
        KernelLaneAuthoritySet::new(&identities, vec![lease]).expect("authority should validate");
    store.initialize_runtime_kernel_state(kernel.snapshot()).expect("head should initialize");
    let mut fixture = Fixture { store, kernel, authority, session_id, run_id };
    commit_transition(
        &mut fixture,
        RuntimeEventName::RunStarted,
        KernelTransition::BeginRuntimeSelection,
        "started",
    );
    fixture
}

fn commit_transition(
    fixture: &mut Fixture,
    event_name: RuntimeEventName,
    transition: KernelTransition,
    key_suffix: &str,
) {
    let descriptor = event_name.descriptor();
    let sequence = fixture.kernel.snapshot().revision().saturating_add(1);
    let event = RuntimeEventEnvelopeV2 {
        schema_version: RUNTIME_EVENT_ENVELOPE_SCHEMA_VERSION,
        event_id: RuntimeEventId::parse(
            format!("event_{}_{}", fixture.run_id, key_suffix).as_str(),
        )
        .expect("event id should validate"),
        identities: fixture.kernel.snapshot().base_identities().clone(),
        sequence,
        causal_parent_event_id: None,
        subsystem: descriptor.subsystem,
        phase: descriptor.phase,
        event_name,
        reason_code: format!("runtime.rollback.test.{key_suffix}"),
        actor_kind: descriptor.actor_kind,
        retryability: descriptor.retryability,
        redaction_class: descriptor.redaction_class,
        terminal: descriptor.terminal,
        payload: RuntimeEventPayloadRef::Inline { metadata: json!({"fixture": "rollback"}) },
        occurred_at_unix_ms: 1_700_000_000_000
            + i64::try_from(sequence).expect("test sequence should fit i64"),
        extensions: std::collections::BTreeMap::new(),
    };
    let prepared = fixture
        .kernel
        .prepare_transition(
            fixture.kernel.snapshot().run_generation(),
            &fixture.authority,
            format!("request.rollback.{key_suffix}").as_str(),
            event,
            transition,
        )
        .expect("transition should prepare");
    let snapshot = match fixture
        .store
        .commit_prepared_runtime_kernel_transition(&prepared)
        .expect("transition should commit")
    {
        RuntimeKernelTransitionCommitOutcome::Applied { snapshot, .. }
        | RuntimeKernelTransitionCommitOutcome::AlreadyApplied { snapshot, .. } => snapshot,
        RuntimeKernelTransitionCommitOutcome::StaleSuppressed { .. } => {
            panic!("fixture generation should retain authority")
        }
    };
    fixture.kernel =
        RuntimeKernelV2::restore_from_journal(snapshot).expect("committed snapshot should restore");
}

fn mutation_fence(fixture: &Fixture, suffix: &str) -> SideEffectFenceV1 {
    SideEffectFenceV1 {
        schema_version: SIDE_EFFECT_FENCE_SCHEMA_VERSION,
        operation_id: RuntimeOperationId::parse(format!("operation_rollback_{suffix}").as_str())
            .expect("operation id should validate"),
        tool_execution_id: RuntimeToolExecutionId::parse(
            format!("execution_rollback_{suffix}").as_str(),
        )
        .expect("tool execution id should validate"),
        intent_generation: fixture.kernel.snapshot().run_generation(),
        observed_generation: fixture.kernel.snapshot().run_generation(),
        intent_sha256: "a".repeat(64),
        state: SideEffectFenceState::IntentRecorded,
        semantics: ToolExecutionSemantics {
            schema_version: SIDE_EFFECT_FENCE_SCHEMA_VERSION,
            tool_name: "palyra.fs.apply_patch".to_owned(),
            idempotency_class: RuntimeIdempotencyClass::ReconciliableMutation,
            restart_policy: SideEffectRestartPolicy::ReconcileBeforeRetry,
            reconciliation_strategy: ReconciliationStrategy::WorkspaceDigest,
            external_idempotency_key_required: false,
        },
        external_idempotency_key_sha256: None,
        evidence_sha256: None,
        reason_code: "tool.effect.intent_recorded".to_owned(),
        updated_at_unix_ms: 1,
    }
}

fn table_count(store: &JournalStore, table: &str, run_id: &str) -> i64 {
    let connection = store.connection.lock().expect("journal lock should be available");
    connection
        .query_row(
            format!("SELECT COUNT(*) FROM {table} WHERE run_ulid = ?1").as_str(),
            params![run_id],
            |row| row.get(0),
        )
        .expect("table count should load")
}

fn fence_event_count(store: &JournalStore, run_id: &str) -> i64 {
    let connection = store.connection.lock().expect("journal lock should be available");
    connection
        .query_row(
            r#"
                SELECT COUNT(*)
                FROM runtime_side_effect_fence_events AS event
                INNER JOIN runtime_side_effect_fences AS fence
                    ON fence.operation_ulid = event.operation_ulid
                WHERE fence.run_ulid = ?1
            "#,
            params![run_id],
            |row| row.get(0),
        )
        .expect("fence-event count should load")
}

#[test]
fn read_only_generation_finishes_under_unchanged_authority_and_blocks_new_mutation() {
    let directory = tempfile::tempdir().expect("test directory should create");
    let db_path = directory.path().join("read-only.sqlite3");
    let fixture = setup_fixture(&db_path, "read_only");
    let authority_before = fixture.kernel.snapshot().runtime_authority_decision().clone();
    let report = fixture
        .store
        .request_runtime_kernel_profile_downgrade(
            RuntimeKernelRollbackPolicy::FinishReadOnlySuspendMutating,
        )
        .expect("rollback scan should succeed");
    assert_eq!(report.evaluated, 1);
    assert_eq!(report.finish_allowed, 1);
    let boundary = VerifiedRuntimeRollbackSafeBoundary::for_test(
        fixture.kernel.snapshot(),
        RuntimeEventName::RunStarted,
    );
    assert_eq!(
        fixture
            .store
            .apply_pending_runtime_rollback_at_safe_boundary(&boundary)
            .expect("read-only request should resolve"),
        RuntimeRollbackBoundaryOutcome::FinishAllowed
    );
    let head = fixture
        .store
        .load_runtime_kernel_head(fixture.run_id.as_str())
        .expect("head should load")
        .expect("head should exist");
    assert_eq!(head.revision, 1);
    assert_eq!(head.snapshot.runtime_authority_decision(), &authority_before);

    let fence = mutation_fence(&fixture, "read_only_denied");
    assert!(matches!(
        fixture
            .store
            .record_side_effect_fence(&fixture.session_id, &fixture.run_id, &fence)
            .expect_err("rollback request must fence new mutation"),
        JournalError::RuntimeRollbackNewSideEffectDenied { .. }
    ));
}

#[test]
fn mutating_generation_suspends_without_losing_effect_evidence() {
    let directory = tempfile::tempdir().expect("test directory should create");
    let db_path = directory.path().join("mutating.sqlite3");
    let fixture = setup_fixture(&db_path, "mutating");
    let fence = mutation_fence(&fixture, "mutating");
    fixture
        .store
        .record_side_effect_fence(&fixture.session_id, &fixture.run_id, &fence)
        .expect("mutation intent should persist");
    let ledger_before =
        table_count(&fixture.store, "runtime_kernel_transition_ledger", &fixture.run_id);
    let fences_before = table_count(&fixture.store, "runtime_side_effect_fences", &fixture.run_id);
    let events_before = fence_event_count(&fixture.store, &fixture.run_id);
    let authority_before = fixture.kernel.snapshot().runtime_authority_decision().clone();

    let report = fixture
        .store
        .request_runtime_kernel_profile_downgrade(
            RuntimeKernelRollbackPolicy::FinishReadOnlySuspendMutating,
        )
        .expect("rollback scan should succeed");
    assert_eq!(report.suspension_pending, 1);
    let boundary = VerifiedRuntimeRollbackSafeBoundary::for_test(
        fixture.kernel.snapshot(),
        RuntimeEventName::RunStarted,
    );
    let RuntimeRollbackBoundaryOutcome::Suspended { snapshot, replayed } = fixture
        .store
        .apply_pending_runtime_rollback_at_safe_boundary(&boundary)
        .expect("rollback suspension should commit")
    else {
        panic!("mutating run should suspend");
    };
    assert!(!replayed);
    assert_eq!(snapshot.state(), KernelState::Suspended);
    assert_eq!(snapshot.runtime_authority_decision(), &authority_before);
    assert_eq!(
        table_count(&fixture.store, "runtime_kernel_transition_ledger", &fixture.run_id),
        ledger_before + 1
    );
    assert_eq!(
        table_count(&fixture.store, "runtime_side_effect_fences", &fixture.run_id),
        fences_before
    );
    assert_eq!(fence_event_count(&fixture.store, &fixture.run_id), events_before);
}

#[test]
fn unknown_effect_generation_suspends_without_replaying_the_effect() {
    let directory = tempfile::tempdir().expect("test directory should create");
    let db_path = directory.path().join("unknown.sqlite3");
    let fixture = setup_fixture(&db_path, "unknown");
    let fence = mutation_fence(&fixture, "unknown");
    fixture
        .store
        .record_side_effect_fence(&fixture.session_id, &fixture.run_id, &fence)
        .expect("mutation intent should persist");
    fixture
        .store
        .transition_side_effect_fence(
            fence.operation_id.as_str(),
            SideEffectFenceState::EffectStarted,
            fence.observed_generation,
            "tool.effect.started",
            None,
        )
        .expect("effect start should persist");
    fixture
        .store
        .transition_side_effect_fence(
            fence.operation_id.as_str(),
            SideEffectFenceState::EffectUnknown,
            fence.observed_generation,
            "tool.effect.outcome_unknown",
            Some("b".repeat(64)),
        )
        .expect("unknown outcome should persist");
    let events_before = fence_event_count(&fixture.store, &fixture.run_id);

    let report = fixture
        .store
        .request_runtime_kernel_profile_downgrade(
            RuntimeKernelRollbackPolicy::FinishReadOnlySuspendMutating,
        )
        .expect("rollback scan should succeed");
    assert_eq!(report.suspension_pending, 1);
    let boundary = VerifiedRuntimeRollbackSafeBoundary::for_test(
        fixture.kernel.snapshot(),
        RuntimeEventName::RunStarted,
    );
    assert!(matches!(
        fixture
            .store
            .apply_pending_runtime_rollback_at_safe_boundary(&boundary)
            .expect("unknown effect should suspend"),
        RuntimeRollbackBoundaryOutcome::Suspended { replayed: false, .. }
    ));
    assert_eq!(fence_event_count(&fixture.store, &fixture.run_id), events_before);
}

#[test]
fn stale_safe_boundary_is_denied_by_head_revision_cas() {
    let directory = tempfile::tempdir().expect("test directory should create");
    let db_path = directory.path().join("stale.sqlite3");
    let mut fixture = setup_fixture(&db_path, "stale");
    fixture
        .store
        .request_runtime_kernel_profile_downgrade(
            RuntimeKernelRollbackPolicy::SuspendAllAtSafeBoundary,
        )
        .expect("rollback scan should succeed");
    let stale_boundary = VerifiedRuntimeRollbackSafeBoundary::for_test(
        fixture.kernel.snapshot(),
        RuntimeEventName::RunStarted,
    );
    commit_transition(&mut fixture, RuntimeEventName::RunFailed, KernelTransition::Fail, "failed");
    assert_eq!(
        fixture
            .store
            .apply_pending_runtime_rollback_at_safe_boundary(&stale_boundary)
            .expect("stale boundary should be rejected"),
        RuntimeRollbackBoundaryOutcome::StaleDenied {
            expected_revision: 1,
            actual_revision: Some(2),
        }
    );
    assert_eq!(
        rollback_request_state_for_test(&fixture.store, &fixture.run_id)
            .expect("request state should load")
            .as_deref(),
        Some("awaiting_safe_boundary")
    );
}

#[test]
fn restart_and_replay_reuse_one_suspension_transition() {
    let directory = tempfile::tempdir().expect("test directory should create");
    let db_path = directory.path().join("restart.sqlite3");
    let fixture = setup_fixture(&db_path, "restart");
    fixture
        .store
        .request_runtime_kernel_profile_downgrade(
            RuntimeKernelRollbackPolicy::SuspendAllAtSafeBoundary,
        )
        .expect("rollback scan should succeed");
    let boundary = VerifiedRuntimeRollbackSafeBoundary::for_test(
        fixture.kernel.snapshot(),
        RuntimeEventName::RunStarted,
    );
    let run_id = fixture.run_id.clone();
    drop(fixture);

    let reopened = JournalStore::open(test_config(db_path.clone())).expect("journal should reopen");
    assert!(matches!(
        reopened
            .apply_pending_runtime_rollback_at_safe_boundary(&boundary)
            .expect("pending request should survive restart"),
        RuntimeRollbackBoundaryOutcome::Suspended { replayed: false, .. }
    ));
    assert!(matches!(
        reopened
            .apply_pending_runtime_rollback_at_safe_boundary(&boundary)
            .expect("same suspension should replay"),
        RuntimeRollbackBoundaryOutcome::Suspended { replayed: true, .. }
    ));
    assert_eq!(table_count(&reopened, "runtime_kernel_transition_ledger", &run_id), 2);
    assert_eq!(
        rollback_request_state_for_test(&reopened, &run_id)
            .expect("request state should load")
            .as_deref(),
        Some("suspended")
    );
}

#[test]
fn migration_76_is_idempotent_and_rollback_evidence_is_immutable() {
    let directory = tempfile::tempdir().expect("test directory should create");
    let db_path = directory.path().join("migration.sqlite3");
    let fixture = setup_fixture(&db_path, "migration");
    fixture
        .store
        .request_runtime_kernel_profile_downgrade(
            RuntimeKernelRollbackPolicy::SuspendAllAtSafeBoundary,
        )
        .expect("rollback request should persist");
    drop(fixture);
    let reopened =
        JournalStore::open(test_config(db_path.clone())).expect("migration should replay safely");
    assert_eq!(
        rollback_request_state_for_test(&reopened, "run_rollback_migration")
            .expect("request state should load")
            .as_deref(),
        Some("awaiting_safe_boundary")
    );
    drop(reopened);

    let connection = Connection::open(db_path).expect("journal database should open");
    let version: i64 = connection
        .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| row.get(0))
        .expect("schema version should load");
    assert!(version >= 76);
    assert!(connection
        .execute(
            "UPDATE runtime_kernel_rollback_requests SET reason_code = 'tampered' WHERE run_ulid = ?1",
            params!["run_rollback_migration"],
        )
        .is_err());
    assert!(connection
        .execute(
            "DELETE FROM runtime_kernel_rollback_requests WHERE run_ulid = ?1",
            params!["run_rollback_migration"],
        )
        .is_err());
}
