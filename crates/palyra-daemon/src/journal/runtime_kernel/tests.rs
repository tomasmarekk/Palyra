//! Transaction, replay, fencing, rollback, and migration tests for the kernel journal.
//!
//! Fixtures exercise the real orchestrator Run generation and shared V2 event tables.

use std::{
    path::{Path, PathBuf},
    sync::{Arc, Barrier},
};

use palyra_common::runtime_contracts::{
    RuntimeEventEnvelopeV2, RuntimeEventId, RuntimeEventName, RuntimeEventPayloadRef,
    RuntimeGenerationLane, RuntimeIdentitySetV1, RuntimeRunId, RuntimeSessionId, RuntimeTraceId,
    RUNTIME_EVENT_ENVELOPE_SCHEMA_VERSION,
};
use rusqlite::{params, Connection};
use serde_json::json;

use crate::{
    application::runtime_kernel_v2::{
        profile::{RuntimeKernelCompatibilityOverridesV1, RuntimeKernelProfileConfigV1},
        selection::{
            resolve_runtime_authority, RuntimeAuthorityProgressEvidence, V2RuntimeAvailability,
        },
        KernelLaneAuthoritySet, KernelTransition, PreparedKernelTransition, RuntimeKernelV2,
        RuntimeKernelVersion,
    },
    journal::{
        JournalConfig, JournalError, JournalStore, OrchestratorRunStartRequest,
        OrchestratorRunTerminalSettlementRequest, OrchestratorSessionUpsertRequest,
    },
    orchestrator::RunLifecycleState,
};

use super::{
    RuntimeKernelChildLaneAcquireRequest, RuntimeKernelObservationCommitRequest,
    RuntimeKernelTransitionCommitOutcome, MIGRATION_72_SQL,
};

fn test_config(db_path: PathBuf) -> JournalConfig {
    JournalConfig {
        db_path,
        hash_chain_enabled: false,
        max_payload_bytes: 256 * 1024,
        max_events: 10_000,
    }
}

fn setup_store(
    db_path: &Path,
    session_id: &str,
    run_id: &str,
) -> (JournalStore, RuntimeKernelV2, KernelLaneAuthoritySet) {
    let store = JournalStore::open(test_config(db_path.to_owned())).expect("journal should open");
    store
        .upsert_orchestrator_session(&OrchestratorSessionUpsertRequest {
            session_id: session_id.to_owned(),
            session_key: session_id.to_owned(),
            session_label: None,
            principal: "user:test".to_owned(),
            device_id: "device_kernel_test".to_owned(),
            channel: Some("cli".to_owned()),
        })
        .expect("session should persist");
    store
        .start_orchestrator_run(&OrchestratorRunStartRequest {
            run_id: run_id.to_owned(),
            session_id: session_id.to_owned(),
            origin_kind: String::new(),
            origin_run_id: None,
            triggered_by_principal: None,
            parameter_delta_json: None,
            delegated_admission: None,
        })
        .expect("run should persist");
    let lease = store
        .active_runtime_generation_for_run(run_id, RuntimeGenerationLane::Run)
        .expect("generation should load")
        .expect("run generation should be active");
    let identities = RuntimeIdentitySetV1::for_run(
        RuntimeTraceId::parse(format!("trace_{run_id}").as_str())
            .expect("trace id should validate"),
        RuntimeSessionId::parse(session_id).expect("session id should validate"),
        RuntimeRunId::parse(run_id).expect("run id should validate"),
        lease.generation,
    );
    let profile = RuntimeKernelProfileConfigV1::new(
        RuntimeKernelVersion::V2,
        0,
        RuntimeKernelCompatibilityOverridesV1::none(),
    )
    .expect("V2 profile should validate");
    let authority_decision = resolve_runtime_authority(
        &profile,
        &identities,
        V2RuntimeAvailability::Ready,
        RuntimeAuthorityProgressEvidence::pristine(),
        None,
    )
    .expect("V2 authority should resolve");
    let kernel = RuntimeKernelV2::admit_for_test(
        authority_decision,
        identities.clone(),
        lease.clone(),
        lease.acquired_at_unix_ms,
    )
    .expect("kernel should admit");
    let authority = KernelLaneAuthoritySet::new(&identities, vec![lease])
        .expect("run authority should validate");
    (store, kernel, authority)
}

fn prepared_start(
    kernel: &RuntimeKernelV2,
    authority: &KernelLaneAuthoritySet,
    key: &str,
    event_id: &str,
) -> PreparedKernelTransition {
    let identities = kernel.snapshot().base_identities().clone();
    let descriptor = RuntimeEventName::RunStarted.descriptor();
    let event = RuntimeEventEnvelopeV2 {
        schema_version: RUNTIME_EVENT_ENVELOPE_SCHEMA_VERSION,
        event_id: RuntimeEventId::parse(event_id).expect("event id should validate"),
        identities,
        sequence: 1,
        causal_parent_event_id: None,
        subsystem: descriptor.subsystem,
        phase: descriptor.phase,
        event_name: RuntimeEventName::RunStarted,
        reason_code: "runtime.kernel.test_started".to_owned(),
        actor_kind: descriptor.actor_kind,
        retryability: descriptor.retryability,
        redaction_class: descriptor.redaction_class,
        terminal: descriptor.terminal,
        payload: RuntimeEventPayloadRef::Inline { metadata: json!({"fixture": "kernel"}) },
        occurred_at_unix_ms: 1_700_000_000_000,
        extensions: std::collections::BTreeMap::new(),
    };
    kernel
        .prepare_transition(
            kernel.snapshot().run_generation(),
            authority,
            key,
            event,
            KernelTransition::BeginRuntimeSelection,
        )
        .expect("transition should prepare")
}

fn observation_start(
    kernel: &RuntimeKernelV2,
    authority: &KernelLaneAuthoritySet,
    key: &str,
    event_id: &str,
    occurred_at_unix_ms: i64,
    metadata: serde_json::Value,
) -> RuntimeKernelObservationCommitRequest {
    let descriptor = RuntimeEventName::RunStarted.descriptor();
    RuntimeKernelObservationCommitRequest {
        expected_snapshot: kernel.snapshot().clone(),
        expected_run_generation: kernel.snapshot().run_generation(),
        lane_authority: authority.clone(),
        idempotency_key: key.to_owned(),
        event_template: RuntimeEventEnvelopeV2 {
            schema_version: RUNTIME_EVENT_ENVELOPE_SCHEMA_VERSION,
            event_id: RuntimeEventId::parse(event_id).expect("event id should validate"),
            identities: kernel.snapshot().base_identities().clone(),
            sequence: 0,
            causal_parent_event_id: None,
            subsystem: descriptor.subsystem,
            phase: descriptor.phase,
            event_name: RuntimeEventName::RunStarted,
            reason_code: "runtime.kernel.test_observation".to_owned(),
            actor_kind: descriptor.actor_kind,
            retryability: descriptor.retryability,
            redaction_class: descriptor.redaction_class,
            terminal: descriptor.terminal,
            payload: RuntimeEventPayloadRef::Inline { metadata },
            occurred_at_unix_ms,
            extensions: std::collections::BTreeMap::new(),
        },
        transition: KernelTransition::BeginRuntimeSelection,
    }
}

fn table_count(store: &JournalStore, table: &str, run_id: &str) -> i64 {
    let guard = store.connection.lock().expect("journal lock should be available");
    guard
        .query_row(
            format!("SELECT COUNT(*) FROM {table} WHERE run_ulid = ?1").as_str(),
            params![run_id],
            |row| row.get(0),
        )
        .expect("table count should load")
}

#[test]
fn terminal_settlement_releases_every_run_owned_kernel_lane() {
    let directory = tempfile::tempdir().expect("test directory should create");
    let db_path = directory.path().join("kernel-terminal-lanes.sqlite3");
    let session_id = "session_kernel_terminal_lanes";
    let run_id = "run_kernel_terminal_lanes";
    let (store, kernel, authority) = setup_store(&db_path, session_id, run_id);
    let identities = kernel.snapshot().base_identities().clone();
    let run_lease =
        authority.run_lease(&identities).expect("run authority should contain its lease").clone();

    for lane in [
        RuntimeGenerationLane::Harness,
        RuntimeGenerationLane::Tool,
        RuntimeGenerationLane::Delivery,
    ] {
        store
            .acquire_runtime_kernel_child_lane(&RuntimeKernelChildLaneAcquireRequest::new(
                identities.clone(),
                run_lease.clone(),
                lane,
                format!("test:{}", lane.as_str()),
            ))
            .expect("child lane should activate");
    }
    store
        .update_orchestrator_run_state(run_id, RunLifecycleState::InProgress, None)
        .expect("run should enter progress");
    store
        .settle_orchestrator_run_terminal(&OrchestratorRunTerminalSettlementRequest {
            run_id: run_id.to_owned(),
            requested_state: RunLifecycleState::Done,
            reason_code: "runtime.terminal.completed".to_owned(),
            status_message: "completed".to_owned(),
            actor: palyra_common::runtime_contracts::RuntimeActorRef {
                kind: palyra_common::runtime_contracts::RuntimeActorKind::System,
                id: "runtime-kernel-test".to_owned(),
            },
            terminal_summary_payload_json: None,
            terminal_tape_events: Vec::new(),
            terminal_status_payload_json: json!({
                "kind": "done",
                "message": "completed",
            })
            .to_string(),
        })
        .expect("terminal settlement should release generation authority");

    for lane in [
        RuntimeGenerationLane::Run,
        RuntimeGenerationLane::Harness,
        RuntimeGenerationLane::Tool,
        RuntimeGenerationLane::Delivery,
    ] {
        assert!(
            store
                .active_runtime_generation_for_run(run_id, lane)
                .expect("generation lookup should succeed")
                .is_none(),
            "{lane:?} lane must not outlive its terminal Run parent"
        );
    }
}

#[test]
fn initialization_and_transition_preserve_append_only_evidence() {
    let directory = tempfile::tempdir().expect("test directory should create");
    let db_path = directory.path().join("kernel.sqlite3");
    let (store, kernel, authority) =
        setup_store(&db_path, "session_kernel_init", "run_kernel_init");
    let initial =
        store.initialize_runtime_kernel_state(kernel.snapshot()).expect("head should initialize");
    assert_eq!(initial.revision, 0);
    assert_eq!(initial.snapshot.revision(), 0);

    let prepared = prepared_start(&kernel, &authority, "request.kernel.init", "event_kernel_init");
    let outcome = store
        .commit_prepared_runtime_kernel_transition(&prepared)
        .expect("transition should commit");
    assert!(matches!(
        outcome,
        RuntimeKernelTransitionCommitOutcome::Applied { revision: 1, event_sequence: 1, .. }
    ));
    let head = store
        .load_runtime_kernel_head("run_kernel_init")
        .expect("head should load")
        .expect("head should exist");
    assert_eq!(head.revision, 1);
    assert_eq!(head.revision, head.snapshot.revision());
    assert_eq!(head.snapshot, *prepared.next_snapshot());
    let ledger = store
        .list_runtime_kernel_transition_ledger_for_test("run_kernel_init")
        .expect("ledger should load");
    assert_eq!(ledger.len(), 1);
    assert_eq!(ledger[0].revision, ledger[0].next_snapshot.revision());
    assert_eq!(ledger[0].previous_snapshot, *prepared.previous_snapshot());
    assert_eq!(ledger[0].next_snapshot, *prepared.next_snapshot());

    let guard = store.connection.lock().expect("journal lock should be available");
    assert!(guard
        .execute(
            "UPDATE runtime_kernel_transition_ledger SET revision = 2 WHERE run_ulid = ?1",
            params!["run_kernel_init"],
        )
        .is_err());
    assert!(guard
        .execute(
            "DELETE FROM runtime_kernel_transition_ledger WHERE run_ulid = ?1",
            params!["run_kernel_init"],
        )
        .is_err());
    assert!(guard
            .execute(
                "UPDATE runtime_kernel_heads SET runtime_version = 'legacy', revision = revision + 1 WHERE run_ulid = ?1",
                params!["run_kernel_init"],
            )
            .is_err());
}

#[test]
fn exact_replay_after_reopen_reuses_one_ledger_row() {
    let directory = tempfile::tempdir().expect("test directory should create");
    let db_path = directory.path().join("replay.sqlite3");
    let prepared = {
        let (store, kernel, authority) =
            setup_store(&db_path, "session_kernel_replay", "run_kernel_replay");
        store.initialize_runtime_kernel_state(kernel.snapshot()).expect("head should initialize");
        let prepared =
            prepared_start(&kernel, &authority, "request.kernel.replay", "event_kernel_replay");
        assert!(matches!(
            store
                .commit_prepared_runtime_kernel_transition(&prepared)
                .expect("first transition should commit"),
            RuntimeKernelTransitionCommitOutcome::Applied { .. }
        ));
        prepared
    };
    let reopened = JournalStore::open(test_config(db_path)).expect("journal should reopen cleanly");
    assert!(matches!(
        reopened
            .commit_prepared_runtime_kernel_transition(&prepared)
            .expect("exact replay should resolve"),
        RuntimeKernelTransitionCommitOutcome::AlreadyApplied { revision: 1, event_sequence: 1, .. }
    ));
    assert_eq!(
        reopened
            .list_runtime_kernel_transition_ledger_for_test("run_kernel_replay")
            .expect("ledger should load")
            .len(),
        1
    );
    assert_eq!(table_count(&reopened, "runtime_events_v2", "run_kernel_replay"), 1);
}

#[test]
fn exact_replay_and_conflict_are_resolved_from_ledger_after_authority_expiry() {
    let directory = tempfile::tempdir().expect("test directory should create");
    let db_path = directory.path().join("expired-replay.sqlite3");
    let (store, kernel, authority) =
        setup_store(&db_path, "session_kernel_expired", "run_kernel_expired");
    store.initialize_runtime_kernel_state(kernel.snapshot()).expect("head should initialize");
    let first =
        prepared_start(&kernel, &authority, "request.kernel.expired", "event_kernel_expired_first");
    store
        .commit_prepared_runtime_kernel_transition(&first)
        .expect("first transition should commit");

    Connection::open(&db_path)
        .expect("journal connection should open")
        .execute(
            "UPDATE runtime_generation_leases SET expires_at_unix_ms = 0 WHERE run_ulid = ?1 AND lane = 'run'",
            params!["run_kernel_expired"],
        )
        .expect("run authority should expire");

    assert!(matches!(
        store
            .commit_prepared_runtime_kernel_transition(&first)
            .expect("exact immutable replay should ignore expired live authority"),
        RuntimeKernelTransitionCommitOutcome::AlreadyApplied { .. }
    ));

    let conflicting = prepared_start(
        &kernel,
        &authority,
        "request.kernel.expired",
        "event_kernel_expired_conflict",
    );
    assert!(matches!(
        store
            .commit_prepared_runtime_kernel_transition(&conflicting)
            .expect_err("conflicting immutable replay must fail after expiry"),
        JournalError::RuntimeKernelIdempotencyConflict { .. }
    ));
}

#[test]
fn semantic_observation_replay_ignores_fresh_host_stamp() {
    let directory = tempfile::tempdir().expect("test directory should create");
    let db_path = directory.path().join("observation-replay.sqlite3");
    let (store, kernel, authority) =
        setup_store(&db_path, "session_observation_replay", "run_observation_replay");
    store.initialize_runtime_kernel_state(kernel.snapshot()).expect("head should initialize");
    let first = observation_start(
        &kernel,
        &authority,
        "request.observation.replay",
        "event_observation_first",
        1_700_000_000_000,
        json!({"fixture": "same"}),
    );
    assert!(matches!(
        store.commit_runtime_kernel_observation(&first).expect("first observation should commit"),
        RuntimeKernelTransitionCommitOutcome::Applied { event_sequence: 1, .. }
    ));

    let replay = observation_start(
        &kernel,
        &authority,
        "request.observation.replay",
        "event_observation_retry",
        1_700_000_000_500,
        json!({"fixture": "same"}),
    );
    assert!(matches!(
        store
            .commit_runtime_kernel_observation(&replay)
            .expect("semantic replay should reuse durable evidence"),
        RuntimeKernelTransitionCommitOutcome::AlreadyApplied { event_sequence: 1, .. }
    ));
    assert_eq!(table_count(&store, "runtime_events_v2", "run_observation_replay"), 1);
}

#[test]
fn semantic_observation_replay_uses_ledger_after_authority_expiry() {
    let directory = tempfile::tempdir().expect("test directory should create");
    let db_path = directory.path().join("expired-observation-replay.sqlite3");
    let (store, kernel, authority) =
        setup_store(&db_path, "session_observation_expired", "run_observation_expired");
    store.initialize_runtime_kernel_state(kernel.snapshot()).expect("head should initialize");
    let first = observation_start(
        &kernel,
        &authority,
        "request.observation.expired",
        "event_observation_expired_first",
        1_700_000_000_000,
        json!({"fixture": "same"}),
    );
    store.commit_runtime_kernel_observation(&first).expect("first observation should commit");

    Connection::open(&db_path)
        .expect("journal connection should open")
        .execute(
            "UPDATE runtime_generation_leases SET expires_at_unix_ms = 0 WHERE run_ulid = ?1 AND lane = 'run'",
            params!["run_observation_expired"],
        )
        .expect("run authority should expire");

    let replay = observation_start(
        &kernel,
        &authority,
        "request.observation.expired",
        "event_observation_expired_retry",
        1_700_000_000_500,
        json!({"fixture": "same"}),
    );
    assert!(matches!(
        store
            .commit_runtime_kernel_observation(&replay)
            .expect("semantic immutable replay should ignore expired live authority"),
        RuntimeKernelTransitionCommitOutcome::AlreadyApplied { .. }
    ));
}

#[test]
fn semantic_observation_replay_rejects_changed_payload() {
    let directory = tempfile::tempdir().expect("test directory should create");
    let db_path = directory.path().join("observation-conflict.sqlite3");
    let (store, kernel, authority) =
        setup_store(&db_path, "session_observation_conflict", "run_observation_conflict");
    store.initialize_runtime_kernel_state(kernel.snapshot()).expect("head should initialize");
    let first = observation_start(
        &kernel,
        &authority,
        "request.observation.conflict",
        "event_observation_conflict_first",
        1_700_000_000_000,
        json!({"fixture": "first"}),
    );
    store.commit_runtime_kernel_observation(&first).expect("first observation should commit");
    let changed = observation_start(
        &kernel,
        &authority,
        "request.observation.conflict",
        "event_observation_conflict_retry",
        1_700_000_000_500,
        json!({"fixture": "changed"}),
    );
    assert!(matches!(
        store
            .commit_runtime_kernel_observation(&changed)
            .expect_err("changed semantic observation must conflict"),
        JournalError::RuntimeKernelIdempotencyConflict { .. }
    ));
}

#[test]
fn same_idempotency_key_with_different_digest_conflicts() {
    let directory = tempfile::tempdir().expect("test directory should create");
    let db_path = directory.path().join("idempotency.sqlite3");
    let (store, kernel, authority) = setup_store(&db_path, "session_kernel_key", "run_kernel_key");
    store.initialize_runtime_kernel_state(kernel.snapshot()).expect("head should initialize");
    let first = prepared_start(&kernel, &authority, "request.kernel.same", "event_kernel_same_a");
    let conflicting =
        prepared_start(&kernel, &authority, "request.kernel.same", "event_kernel_same_b");
    store.commit_prepared_runtime_kernel_transition(&first).expect("first request should commit");
    assert!(matches!(
        store
            .commit_prepared_runtime_kernel_transition(&conflicting)
            .expect_err("different request digest must conflict"),
        JournalError::RuntimeKernelIdempotencyConflict { .. }
    ));
    assert_eq!(
        store
            .list_runtime_kernel_transition_ledger_for_test("run_kernel_key")
            .expect("ledger should load")
            .len(),
        1
    );
}

#[test]
fn two_store_instances_allow_only_one_same_revision_writer() {
    let directory = tempfile::tempdir().expect("test directory should create");
    let db_path = directory.path().join("cas.sqlite3");
    let (store_a, kernel, authority) =
        setup_store(&db_path, "session_kernel_cas", "run_kernel_cas");
    store_a.initialize_runtime_kernel_state(kernel.snapshot()).expect("head should initialize");
    let store_b = JournalStore::open(test_config(db_path)).expect("second store should open");
    let first = prepared_start(&kernel, &authority, "request.kernel.cas_a", "event_kernel_cas_a");
    let second = prepared_start(&kernel, &authority, "request.kernel.cas_b", "event_kernel_cas_b");
    let barrier = Arc::new(Barrier::new(3));
    let first_barrier = Arc::clone(&barrier);
    let first_writer = std::thread::spawn(move || {
        first_barrier.wait();
        store_a.commit_prepared_runtime_kernel_transition(&first)
    });
    let second_barrier = Arc::clone(&barrier);
    let second_writer = std::thread::spawn(move || {
        second_barrier.wait();
        store_b.commit_prepared_runtime_kernel_transition(&second)
    });
    barrier.wait();
    let outcomes = [
        first_writer.join().expect("first writer should not panic"),
        second_writer.join().expect("second writer should not panic"),
    ];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| {
                matches!(outcome, Ok(RuntimeKernelTransitionCommitOutcome::Applied { .. }))
            })
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| {
                matches!(outcome, Err(JournalError::RuntimeKernelHeadConflict { .. }))
            })
            .count(),
        1
    );
}

#[test]
fn stale_run_generation_has_zero_kernel_or_event_effects() {
    let directory = tempfile::tempdir().expect("test directory should create");
    let db_path = directory.path().join("stale.sqlite3");
    let (store, kernel, authority) =
        setup_store(&db_path, "session_kernel_stale", "run_kernel_stale");
    store.initialize_runtime_kernel_state(kernel.snapshot()).expect("head should initialize");
    let prepared =
        prepared_start(&kernel, &authority, "request.kernel.stale", "event_kernel_stale");
    store
        .supersede_run_runtime_generation(
            "session_kernel_stale",
            "run_kernel_stale",
            "runtime.kernel.test_superseded",
        )
        .expect("generation should supersede");

    let before_head = store.load_runtime_kernel_head("run_kernel_stale").expect("head should load");
    let before_ledger = table_count(&store, "runtime_kernel_transition_ledger", "run_kernel_stale");
    let before_events = table_count(&store, "runtime_events_v2", "run_kernel_stale");
    assert!(matches!(
        store
            .commit_prepared_runtime_kernel_transition(&prepared)
            .expect("stale request should suppress"),
        RuntimeKernelTransitionCommitOutcome::StaleSuppressed { active_generation: Some(_) }
    ));
    assert_eq!(
        store.load_runtime_kernel_head("run_kernel_stale").expect("head should load"),
        before_head
    );
    assert_eq!(
        table_count(&store, "runtime_kernel_transition_ledger", "run_kernel_stale"),
        before_ledger
    );
    assert_eq!(table_count(&store, "runtime_events_v2", "run_kernel_stale"), before_events);
}

#[test]
fn insert_head_and_event_failures_roll_back_every_transition_effect() {
    for (case, trigger_sql) in [
        (
            "ledger",
            "CREATE TRIGGER force_kernel_ledger_failure BEFORE INSERT ON runtime_kernel_transition_ledger BEGIN SELECT RAISE(ABORT, 'forced ledger failure'); END;",
        ),
        (
            "head",
            "CREATE TRIGGER force_kernel_head_failure BEFORE UPDATE ON runtime_kernel_heads BEGIN SELECT RAISE(ABORT, 'forced head failure'); END;",
        ),
        (
            "event",
            "CREATE TRIGGER force_kernel_event_failure BEFORE INSERT ON runtime_events_v2 BEGIN SELECT RAISE(ABORT, 'forced event failure'); END;",
        ),
    ] {
        let directory = tempfile::tempdir().expect("test directory should create");
        let db_path = directory.path().join(format!("{case}.sqlite3"));
        let session_id = format!("session_kernel_failure_{case}");
        let run_id = format!("run_kernel_failure_{case}");
        let (store, kernel, authority) = setup_store(&db_path, &session_id, &run_id);
        store.initialize_runtime_kernel_state(kernel.snapshot()).expect("head should initialize");
        let prepared = prepared_start(
            &kernel,
            &authority,
            format!("request.kernel.failure_{case}").as_str(),
            format!("event_kernel_failure_{case}").as_str(),
        );
        store
            .connection
            .lock()
            .expect("journal lock should be available")
            .execute_batch(trigger_sql)
            .expect("failure trigger should install");
        assert!(store.commit_prepared_runtime_kernel_transition(&prepared).is_err());
        let head = store
            .load_runtime_kernel_head(&run_id)
            .expect("head should load")
            .expect("head should exist");
        assert_eq!(head.revision, 0, "{case} failure must retain initial head");
        assert_eq!(
            table_count(&store, "runtime_kernel_transition_ledger", &run_id),
            0,
            "{case} failure must roll back ledger"
        );
        assert_eq!(
            table_count(&store, "runtime_events_v2", &run_id),
            0,
            "{case} failure must roll back canonical event"
        );
    }
}

#[test]
fn migration_72_is_idempotent_and_pre_v2_fixture_bytes_remain_pinned() {
    const FIXTURE_SQL: &str =
        include_str!("../../../../../fixtures/golden/journal_migrations/pre_v2_v44.sql");
    const FIXTURE_MANIFEST: &str =
        include_str!("../../../../../fixtures/golden/journal_migrations/pre_v2_v44.sha256");

    let manifest_hash = FIXTURE_MANIFEST
        .trim_end_matches('\n')
        .split_once("  ")
        .expect("fixture manifest should use sha256sum format")
        .0;
    assert_eq!(super::sha256_hex(FIXTURE_SQL.as_bytes()), manifest_hash);

    let directory = tempfile::tempdir().expect("test directory should create");
    let db_path = directory.path().join("pre-v2.sqlite3");
    Connection::open(&db_path)
        .expect("fixture database should open")
        .execute_batch(FIXTURE_SQL)
        .expect("pre-V2 fixture should materialize");
    drop(JournalStore::open(test_config(db_path.clone())).expect("fixture should migrate"));
    drop(JournalStore::open(test_config(db_path.clone())).expect("fixture should reopen"));

    let connection = Connection::open(db_path).expect("migrated database should open");
    assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM schema_migrations WHERE version = 72 AND name = 'runtime_kernel_v2_journal_boundary'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .expect("migration marker should load"),
            1
        );
    assert!(MIGRATION_72_SQL.contains("runtime_kernel_transition_ledger"));
}
