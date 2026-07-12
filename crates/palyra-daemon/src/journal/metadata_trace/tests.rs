//! Journal-level regression tests for durable metadata traces.
//!
//! These tests exercise crash prefixes, immutable SQLite ledgers, hard caps,
//! legacy migration, and terminal outcomes through the real `JournalStore`.

use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use palyra_common::metadata_trace::{
    metadata_trace_id_sha256, MetadataTraceEntrypointV1, MetadataTraceEventDataV1,
    MetadataTraceEventV1, MetadataTraceIdDomainV1, MetadataTraceRecoveryStrategyV1,
    MetadataTraceSegmentStatusV1, MetadataTraceTerminalOutcomeV1, RecoveryMetadataV1,
    METADATA_TRACE_MAX_EVENTS, METADATA_TRACE_MAX_SEGMENTS,
};
use rusqlite::{params, Connection};

use crate::orchestrator::RunLifecycleState;

use super::super::{
    JournalConfig, JournalError, JournalStore, OrchestratorCancelRequest,
    OrchestratorRunStartRequest, OrchestratorSessionUpsertRequest, OrchestratorTapeRecord,
    MIGRATIONS,
};

static TEMP_DB_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_db_path() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("test clock should be after unix epoch")
        .as_nanos();
    let counter = TEMP_DB_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "palyra-metadata-trace-test-{nonce}-{}-{counter}.sqlite3",
        std::process::id()
    ))
}

fn journal_config(db_path: PathBuf) -> JournalConfig {
    JournalConfig {
        db_path,
        hash_chain_enabled: false,
        max_payload_bytes: 256 * 1024,
        max_events: 10_000,
    }
}

fn create_session(store: &JournalStore, session_id: &str) {
    store
        .upsert_orchestrator_session(&OrchestratorSessionUpsertRequest {
            session_id: session_id.to_owned(),
            session_key: session_id.to_owned(),
            session_label: None,
            principal: "user:metadata-trace-test".to_owned(),
            device_id: "device:metadata-trace-test".to_owned(),
            channel: Some("test".to_owned()),
        })
        .expect("test session should be created");
}

fn start_run(store: &JournalStore, session_id: &str, run_id: &str) {
    store
        .start_orchestrator_run(&OrchestratorRunStartRequest {
            run_id: run_id.to_owned(),
            session_id: session_id.to_owned(),
            origin_kind: "manual".to_owned(),
            origin_run_id: None,
            triggered_by_principal: Some("user:metadata-trace-test".to_owned()),
            parameter_delta_json: None,
        })
        .expect("test run should start");
}

fn recovery_event(
    sequence: u32,
    generation: u32,
    parent_event_id_sha256: &str,
) -> MetadataTraceEventV1 {
    MetadataTraceEventV1 {
        sequence,
        generation,
        recorded_at_unix_ms: 1_730_000_000_000_u64.saturating_add(u64::from(sequence)),
        event_id_sha256: metadata_trace_id_sha256(
            MetadataTraceIdDomainV1::Event,
            format!("metadata-trace-test-event-{generation}-{sequence}").as_str(),
        )
        .expect("test event identity should hash"),
        causal_parent_event_id_sha256: Some(parent_event_id_sha256.to_owned()),
        stage_duration_ms: Some(1),
        event: MetadataTraceEventDataV1::Recovery(RecoveryMetadataV1 {
            strategy: MetadataTraceRecoveryStrategyV1::OperatorReview,
            attempt: 1,
            reason_code: "test.recovery".to_owned(),
        }),
    }
}

#[test]
fn run_start_atomically_creates_root_segment_and_event() {
    let store =
        JournalStore::open(journal_config(temp_db_path())).expect("journal store should open");
    let session_id = "01ARZ3NDEKTSV4RRFFQ69G5M01";
    let run_id = "01ARZ3NDEKTSV4RRFFQ69G5M02";
    create_session(&store, session_id);
    start_run(&store, session_id, run_id);

    let trace = store
        .load_metadata_trace(run_id)
        .expect("metadata trace should load")
        .expect("new run should have a metadata trace");
    trace.validate_shape().expect("stored trace should satisfy the shared contract");
    assert_eq!(trace.segments.len(), 1);
    let root = &trace.segments[0];
    assert_eq!(root.segment_index, 0);
    assert_eq!(root.generation, 1);
    assert_eq!(root.status, MetadataTraceSegmentStatusV1::Interrupted);
    assert_eq!(root.events.len(), 1);
    assert!(matches!(
        &root.events[0].event,
        MetadataTraceEventDataV1::RunStarted(metadata)
            if metadata.entrypoint == MetadataTraceEntrypointV1::NewRun
    ));
}

#[test]
fn root_trace_failure_rolls_back_the_run_start() {
    let store =
        JournalStore::open(journal_config(temp_db_path())).expect("journal store should open");
    let session_id = "01ARZ3NDEKTSV4RRFFQ69G5M03";
    let run_id = "01ARZ3NDEKTSV4RRFFQ69G5M04";
    create_session(&store, session_id);
    {
        let guard = store.connection.lock().expect("connection lock should not be poisoned");
        guard
            .execute_batch(
                r#"
                    CREATE TRIGGER metadata_trace_test_fail_root
                    BEFORE INSERT ON metadata_trace_segments
                    BEGIN
                        SELECT RAISE(ABORT, 'injected metadata trace root failure');
                    END;
                "#,
            )
            .expect("failure trigger should install");
    }

    let error = store
        .start_orchestrator_run(&OrchestratorRunStartRequest {
            run_id: run_id.to_owned(),
            session_id: session_id.to_owned(),
            origin_kind: "manual".to_owned(),
            origin_run_id: None,
            triggered_by_principal: None,
            parameter_delta_json: None,
        })
        .expect_err("root trace failure should fail run admission");
    assert!(matches!(error, JournalError::Sqlite(_)));
    let guard = store.connection.lock().expect("connection lock should not be poisoned");
    let run_count: i64 = guard
        .query_row(
            "SELECT COUNT(*) FROM orchestrator_runs WHERE run_ulid = ?1",
            params![run_id],
            |row| row.get(0),
        )
        .expect("run count should query");
    assert_eq!(run_count, 0);
    let last_run_id: Option<String> = guard
        .query_row(
            "SELECT last_run_ulid FROM orchestrator_sessions WHERE session_ulid = ?1",
            params![session_id],
            |row| row.get(0),
        )
        .expect("session should remain readable");
    assert_eq!(last_run_id, None);
}

#[test]
fn recovery_continuation_preserves_crash_prefix_and_causal_order() {
    let db_path = temp_db_path();
    let store =
        JournalStore::open(journal_config(db_path.clone())).expect("journal store should open");
    let session_id = "01ARZ3NDEKTSV4RRFFQ69G5M05";
    let run_id = "01ARZ3NDEKTSV4RRFFQ69G5M06";
    create_session(&store, session_id);
    start_run(&store, session_id, run_id);
    let root_event_id = store
        .load_metadata_trace(run_id)
        .expect("trace should load")
        .expect("trace should exist")
        .segments[0]
        .events[0]
        .event_id_sha256
        .clone();

    let position = store
        .start_metadata_trace_recovery_continuation(run_id, "test.crash_recovery")
        .expect("recovery continuation should start");
    assert_eq!(position.segment_index, 1);
    assert_eq!(position.generation, 2);
    store
        .append_metadata_trace_terminalization(
            run_id,
            MetadataTraceTerminalOutcomeV1::ForcedAbort,
            "test.forced_abort",
            false,
            true,
        )
        .expect("continuation should terminalize");

    let trace =
        store.load_metadata_trace(run_id).expect("trace should load").expect("trace should exist");
    assert_eq!(trace.segments.len(), 2);
    assert_eq!(trace.segments[0].status, MetadataTraceSegmentStatusV1::Interrupted);
    assert_eq!(trace.segments[1].status, MetadataTraceSegmentStatusV1::Complete);
    let continuation = &trace.segments[1].events[0];
    assert_eq!(continuation.sequence, 1);
    assert_eq!(continuation.generation, 2);
    assert_eq!(continuation.causal_parent_event_id_sha256.as_deref(), Some(root_event_id.as_str()));
    assert!(matches!(&continuation.event, MetadataTraceEventDataV1::RecoveryContinuation(_)));
    drop(store);

    let reopened =
        JournalStore::open(journal_config(db_path)).expect("journal store should reopen");
    assert_eq!(reopened.load_metadata_trace(run_id).expect("trace should reload"), Some(trace));
}

#[test]
fn corrupt_event_suffix_is_excluded_without_exposing_raw_bytes() {
    let store =
        JournalStore::open(journal_config(temp_db_path())).expect("journal store should open");
    let session_id = "01ARZ3NDEKTSV4RRFFQ69G5M07";
    let run_id = "01ARZ3NDEKTSV4RRFFQ69G5M08";
    create_session(&store, session_id);
    start_run(&store, session_id, run_id);
    let root =
        store.load_metadata_trace(run_id).expect("trace should load").expect("trace should exist");
    let root_event_id = root.segments[0].events[0].event_id_sha256.clone();
    store
        .append_metadata_trace_event(run_id, &recovery_event(1, 1, root_event_id.as_str()))
        .expect("second event should append");
    {
        let guard = store.connection.lock().expect("connection lock should not be poisoned");
        guard
            .execute_batch("DROP TRIGGER trg_metadata_trace_events_prevent_update")
            .expect("test should remove the event update guard");
        guard
            .execute(
                "UPDATE metadata_trace_events SET event_json = ?2 WHERE run_ulid = ?1 AND sequence = 1",
                params![run_id, r#"{"prompt":"never expose this sentinel"}"#],
            )
            .expect("test should inject a corrupt suffix");
    }

    let trace = store
        .load_metadata_trace(run_id)
        .expect("valid prefix should remain readable")
        .expect("trace should exist");
    assert_eq!(trace.segments.len(), 1);
    assert_eq!(trace.segments[0].events.len(), 1);
    assert_eq!(trace.segments[0].status, MetadataTraceSegmentStatusV1::CorruptSuffixIsolated);
    let serialized = serde_json::to_string(&trace).expect("safe trace should serialize");
    assert!(!serialized.contains("never expose this sentinel"));
}

#[test]
fn projected_tape_append_allocates_identity_atomically_and_ignores_late_events() {
    let store =
        JournalStore::open(journal_config(temp_db_path())).expect("journal store should open");
    let session_id = "01ARZ3NDEKTSV4RRFFQ69G5M0S";
    let run_id = "01ARZ3NDEKTSV4RRFFQ69G5M0T";
    create_session(&store, session_id);
    start_run(&store, session_id, run_id);
    let recovery_record = OrchestratorTapeRecord {
        seq: 7,
        event_type: "run.recovery".to_owned(),
        payload_json: r#"{"recovery_kind":"startup_orphaned_active_run"}"#.to_owned(),
    };
    assert!(store
        .append_projected_metadata_trace_event(run_id, &recovery_record)
        .expect("supported tape event should project"));
    let trace =
        store.load_metadata_trace(run_id).expect("trace should load").expect("trace should exist");
    assert_eq!(trace.segments[0].events[1].sequence, 1);
    assert_eq!(
        trace.segments[0].events[1].causal_parent_event_id_sha256.as_deref(),
        Some(trace.segments[0].events[0].event_id_sha256.as_str())
    );
    store
        .append_metadata_trace_terminalization(
            run_id,
            MetadataTraceTerminalOutcomeV1::Failed,
            "test.failed",
            false,
            true,
        )
        .expect("trace should terminalize");
    let event_count_before = store
        .load_metadata_trace(run_id)
        .expect("trace should load")
        .expect("trace should exist")
        .segments[0]
        .events
        .len();
    assert!(!store
        .append_projected_metadata_trace_event(run_id, &recovery_record)
        .expect("late tape event should be an intentional no-op"));
    let event_count_after = store
        .load_metadata_trace(run_id)
        .expect("trace should load")
        .expect("trace should exist")
        .segments[0]
        .events
        .len();
    assert_eq!(event_count_after, event_count_before);
}

#[test]
fn metadata_trace_tables_reject_update_and_delete() {
    let store =
        JournalStore::open(journal_config(temp_db_path())).expect("journal store should open");
    let session_id = "01ARZ3NDEKTSV4RRFFQ69G5M09";
    let run_id = "01ARZ3NDEKTSV4RRFFQ69G5M0A";
    create_session(&store, session_id);
    start_run(&store, session_id, run_id);
    store
        .append_metadata_trace_terminalization(
            run_id,
            MetadataTraceTerminalOutcomeV1::Done,
            "test.done",
            false,
            true,
        )
        .expect("trace should terminalize");

    let guard = store.connection.lock().expect("connection lock should not be poisoned");
    for statement in [
        "UPDATE metadata_trace_segments SET generation = generation WHERE run_ulid = ?1",
        "DELETE FROM metadata_trace_segments WHERE run_ulid = ?1",
        "UPDATE metadata_trace_events SET event_kind = event_kind WHERE run_ulid = ?1",
        "DELETE FROM metadata_trace_events WHERE run_ulid = ?1",
        "UPDATE metadata_trace_segment_status_events SET status = status WHERE run_ulid = ?1",
        "DELETE FROM metadata_trace_segment_status_events WHERE run_ulid = ?1",
    ] {
        let error = guard
            .execute(statement, params![run_id])
            .expect_err("metadata trace ledgers must be immutable");
        assert!(error.to_string().contains("append-only"));
    }
}

#[test]
fn terminal_reserve_remains_available_after_non_terminal_cap() {
    let store =
        JournalStore::open(journal_config(temp_db_path())).expect("journal store should open");
    let session_id = "01ARZ3NDEKTSV4RRFFQ69G5M0B";
    let run_id = "01ARZ3NDEKTSV4RRFFQ69G5M0C";
    create_session(&store, session_id);
    start_run(&store, session_id, run_id);
    let mut parent = store
        .load_metadata_trace(run_id)
        .expect("trace should load")
        .expect("trace should exist")
        .segments[0]
        .events[0]
        .event_id_sha256
        .clone();
    for sequence in 1..u32::try_from(METADATA_TRACE_MAX_EVENTS - 2)
        .expect("metadata trace event cap should fit u32")
    {
        let event = recovery_event(sequence, 1, parent.as_str());
        parent.clone_from(&event.event_id_sha256);
        store
            .append_metadata_trace_event(run_id, &event)
            .expect("non-terminal event before reserve should append");
    }
    let blocked_sequence =
        u32::try_from(METADATA_TRACE_MAX_EVENTS - 2).expect("event cap should fit u32");
    let error = store
        .append_metadata_trace_event(run_id, &recovery_event(blocked_sequence, 1, parent.as_str()))
        .expect_err("non-terminal event must not consume the terminal reserve");
    assert!(matches!(
        error,
        JournalError::MetadataTraceCapacityExceeded { resource: "non_terminal_events", .. }
    ));
    let capped_record = OrchestratorTapeRecord {
        seq: 900,
        event_type: "run.recovery".to_owned(),
        payload_json: r#"{"recovery_kind":"startup_orphaned_active_run"}"#.to_owned(),
    };
    assert!(store
        .append_projected_metadata_trace_event(run_id, &capped_record)
        .expect("first over-cap projection should append the capacity marker"));
    assert!(!store
        .append_projected_metadata_trace_event(run_id, &capped_record)
        .expect("capacity marker retry should be an idempotent no-op"));
    let capped_trace = store
        .load_metadata_trace(run_id)
        .expect("capped trace should load")
        .expect("capped trace should exist");
    assert!(matches!(
        capped_trace.segments[0].events.last().map(|event| &event.event),
        Some(MetadataTraceEventDataV1::CapacityReached(metadata))
            if metadata.observed == u32::try_from(METADATA_TRACE_MAX_EVENTS)
                .expect("event cap should fit u32")
                && metadata.limit == metadata.observed
    ));
    store
        .append_metadata_trace_terminalization(
            run_id,
            MetadataTraceTerminalOutcomeV1::Done,
            "test.done_at_capacity",
            false,
            true,
        )
        .expect("terminal reserve should remain writable");
    let trace =
        store.load_metadata_trace(run_id).expect("trace should load").expect("trace should exist");
    assert_eq!(trace.segments[0].status, MetadataTraceSegmentStatusV1::Complete);
    assert_eq!(trace.segments[0].events.len(), METADATA_TRACE_MAX_EVENTS);
}

#[test]
fn segment_cap_is_enforced_deterministically() {
    let store =
        JournalStore::open(journal_config(temp_db_path())).expect("journal store should open");
    let session_id = "01ARZ3NDEKTSV4RRFFQ69G5M0D";
    let run_id = "01ARZ3NDEKTSV4RRFFQ69G5M0E";
    create_session(&store, session_id);
    start_run(&store, session_id, run_id);
    for generation in
        2..=u32::try_from(METADATA_TRACE_MAX_SEGMENTS).expect("segment cap should fit u32")
    {
        let position = store
            .start_metadata_trace_recovery_continuation(run_id, "test.repeated_crash")
            .expect("continuation below the segment cap should start");
        assert_eq!(position.generation, generation);
    }
    let error = store
        .start_metadata_trace_recovery_continuation(run_id, "test.one_crash_too_many")
        .expect_err("segment cap should reject another continuation");
    assert!(matches!(
        error,
        JournalError::MetadataTraceCapacityExceeded { resource: "segments", .. }
    ));
    let trace =
        store.load_metadata_trace(run_id).expect("trace should load").expect("trace should exist");
    assert_eq!(trace.segments.len(), METADATA_TRACE_MAX_SEGMENTS);
}

#[test]
fn direct_terminal_paths_emit_exactly_one_terminal_event() {
    let store =
        JournalStore::open(journal_config(temp_db_path())).expect("journal store should open");
    let cases = [
        (
            "01ARZ3NDEKTSV4RRFFQ69G5M0F",
            "01ARZ3NDEKTSV4RRFFQ69G5M0G",
            RunLifecycleState::Done,
            MetadataTraceTerminalOutcomeV1::Done,
            true,
        ),
        (
            "01ARZ3NDEKTSV4RRFFQ69G5M0H",
            "01ARZ3NDEKTSV4RRFFQ69G5M0J",
            RunLifecycleState::Failed,
            MetadataTraceTerminalOutcomeV1::Failed,
            false,
        ),
    ];
    for (session_id, run_id, state, expected_outcome, expected_output_emitted) in cases {
        create_session(&store, session_id);
        start_run(&store, session_id, run_id);
        store
            .update_orchestrator_run_state(run_id, RunLifecycleState::InProgress, None)
            .expect("run should enter in-progress state");
        if expected_output_emitted {
            assert!(store
                .append_projected_metadata_trace_event(
                    run_id,
                    &OrchestratorTapeRecord {
                        seq: 1,
                        event_type: "message.replied".to_owned(),
                        payload_json: "{}".to_owned(),
                    },
                )
                .expect("delivery intent should project"));
        }
        store.update_orchestrator_run_state(run_id, state, None).expect("run should terminalize");
        store
            .update_orchestrator_run_state(run_id, state, None)
            .expect("terminal retry should remain idempotent");
        let trace = store
            .load_metadata_trace(run_id)
            .expect("trace should load")
            .expect("trace should exist");
        let terminal_events = trace.segments[0]
            .events
            .iter()
            .filter_map(|event| match &event.event {
                MetadataTraceEventDataV1::Terminalization(metadata) => Some(metadata),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(terminal_events.len(), 1);
        assert_eq!(terminal_events[0].outcome, expected_outcome);
        assert_eq!(terminal_events[0].output_emitted, expected_output_emitted);
        assert_eq!(trace.segments[0].status, MetadataTraceSegmentStatusV1::Complete);
    }

    let session_id = "01ARZ3NDEKTSV4RRFFQ69G5M0K";
    let run_id = "01ARZ3NDEKTSV4RRFFQ69G5M0M";
    create_session(&store, session_id);
    start_run(&store, session_id, run_id);
    store
        .request_orchestrator_cancel(&OrchestratorCancelRequest {
            run_id: run_id.to_owned(),
            reason: "operator_cancel".to_owned(),
        })
        .expect("run should cancel");
    let trace =
        store.load_metadata_trace(run_id).expect("trace should load").expect("trace should exist");
    assert!(matches!(
        trace.segments[0].events.last().map(|event| &event.event),
        Some(MetadataTraceEventDataV1::Terminalization(metadata))
            if metadata.outcome == MetadataTraceTerminalOutcomeV1::Cancelled
    ));
}

#[test]
fn startup_recovery_records_interruption_continuation_and_forced_abort() {
    let store =
        JournalStore::open(journal_config(temp_db_path())).expect("journal store should open");
    let session_id = "01ARZ3NDEKTSV4RRFFQ69G5M0N";
    let run_id = "01ARZ3NDEKTSV4RRFFQ69G5M0P";
    create_session(&store, session_id);
    start_run(&store, session_id, run_id);
    store
        .update_orchestrator_run_state(run_id, RunLifecycleState::InProgress, None)
        .expect("run should enter in-progress state");

    let report = store
        .terminalize_orphaned_orchestrator_runs_on_startup("startup_test")
        .expect("startup recovery should finish");
    assert_eq!(report.terminalized_run_ids, vec![run_id.to_owned()]);
    let trace =
        store.load_metadata_trace(run_id).expect("trace should load").expect("trace should exist");
    assert_eq!(trace.segments.len(), 2);
    assert_eq!(trace.segments[0].status, MetadataTraceSegmentStatusV1::Interrupted);
    assert_eq!(trace.segments[1].status, MetadataTraceSegmentStatusV1::Complete);
    assert!(matches!(
        trace.segments[1].events.first().map(|event| &event.event),
        Some(MetadataTraceEventDataV1::RecoveryContinuation(_))
    ));
    assert!(matches!(
        trace.segments[1].events.last().map(|event| &event.event),
        Some(MetadataTraceEventDataV1::Terminalization(metadata))
            if metadata.outcome == MetadataTraceTerminalOutcomeV1::ForcedAbort
    ));
}

#[test]
fn migration_upgrades_v43_database_without_requiring_trace_rows() {
    let db_path = temp_db_path();
    let mut connection = Connection::open(db_path.as_path()).expect("legacy db should open");
    connection
        .execute_batch(
            r#"
                CREATE TABLE schema_migrations (
                    version INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    applied_at_unix_ms INTEGER NOT NULL
                );
            "#,
        )
        .expect("legacy migration ledger should be created");
    for migration in MIGRATIONS.iter().filter(|migration| migration.version <= 43) {
        let transaction = connection.transaction().expect("legacy migration should start");
        transaction.execute_batch(migration.sql).expect("legacy migration should apply");
        transaction
            .execute(
                "INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (?1, ?2, 0)",
                params![migration.version, migration.name],
            )
            .expect("legacy migration should be recorded");
        transaction.commit().expect("legacy migration should commit");
    }
    let session_id = "01ARZ3NDEKTSV4RRFFQ69G5M0Q";
    let run_id = "01ARZ3NDEKTSV4RRFFQ69G5M0R";
    connection
        .execute(
            r#"
                INSERT INTO orchestrator_sessions (
                    session_ulid, principal, device_id, channel,
                    created_at_unix_ms, updated_at_unix_ms, session_key
                ) VALUES (?1, 'legacy:user', 'legacy:device', 'test', 1, 1, ?1)
            "#,
            params![session_id],
        )
        .expect("legacy session should insert");
    connection
        .execute(
            r#"
                INSERT INTO orchestrator_runs (
                    run_ulid, session_ulid, state, cancel_requested,
                    created_at_unix_ms, started_at_unix_ms, updated_at_unix_ms,
                    prompt_tokens, completion_tokens, total_tokens
                ) VALUES (?1, ?2, 'done', 0, 1, 1, 1, 0, 0, 0)
            "#,
            params![run_id, session_id],
        )
        .expect("legacy run should insert");
    drop(connection);

    let store = JournalStore::open(journal_config(db_path))
        .expect("current journal should migrate the legacy db");
    assert_eq!(store.load_metadata_trace(run_id).expect("legacy run should remain readable"), None);
    let guard = store.connection.lock().expect("connection lock should not be poisoned");
    let migration_count: i64 = guard
        .query_row("SELECT COUNT(*) FROM schema_migrations WHERE version = 44", [], |row| {
            row.get(0)
        })
        .expect("metadata trace migration should be recorded");
    assert_eq!(migration_count, 1);
}
