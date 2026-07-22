//! Focused crash-window and duplicate-invariant tests for runtime finalization.

use std::path::Path;

use palyra_common::runtime_contracts::{
    RuntimeGenerationLane, RuntimeGenerationTransitionKind, RuntimeTerminalOutcome,
};
use rusqlite::params;

use crate::journal::{
    JournalConfig, JournalError, JournalStore, OrchestratorRunStartRequest,
    OrchestratorSessionUpsertRequest, RuntimeGenerationActivateRequest,
};

use super::{
    runtime_finalization_now, FinalOutputArtifactDescriptor, FinalizationEvidenceRef,
    RuntimeDeliveryIntentDescriptor, RuntimeDeliveryLinkObservation, RuntimeDeliveryState,
    RuntimeFinalizationCommitOutcome,
};

struct Fixture {
    store: JournalStore,
    run_lease_id: String,
    run_generation: palyra_common::runtime_contracts::RuntimeGeneration,
    delivery_lease_id: String,
    delivery_generation: palyra_common::runtime_contracts::RuntimeGeneration,
}

fn setup(path: &Path) -> Fixture {
    let store = JournalStore::open(JournalConfig {
        db_path: path.to_owned(),
        hash_chain_enabled: false,
        max_payload_bytes: 256 * 1024,
        max_events: 10_000,
    })
    .expect("journal should open");
    store
        .upsert_orchestrator_session(&OrchestratorSessionUpsertRequest {
            session_id: "session-final".to_owned(),
            session_key: "session-final".to_owned(),
            session_label: None,
            principal: "user:test".to_owned(),
            device_id: "device-final".to_owned(),
            channel: Some("cli".to_owned()),
        })
        .expect("session should persist");
    store
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
    let run_lease = store
        .active_runtime_generation_for_run("run-final", RuntimeGenerationLane::Run)
        .expect("run generation should load")
        .expect("run generation should be active");
    let delivery_lease = store
        .activate_runtime_generation(&RuntimeGenerationActivateRequest {
            session_id: "session-final".to_owned(),
            run_id: Some("run-final".to_owned()),
            lane: RuntimeGenerationLane::Delivery,
            owner: "runtime-finalization-test".to_owned(),
            ttl_ms: 60_000,
            transition_kind: RuntimeGenerationTransitionKind::Activated,
            reason_code: "runtime.delivery.test_activated".to_owned(),
        })
        .expect("delivery generation should activate");
    Fixture {
        store,
        run_lease_id: run_lease.lease_id.into_inner(),
        run_generation: run_lease.generation,
        delivery_lease_id: delivery_lease.lease_id.into_inner(),
        delivery_generation: delivery_lease.generation,
    }
}

fn evidence(kind: &str, reference_id: &str, byte: char) -> FinalizationEvidenceRef {
    FinalizationEvidenceRef {
        kind: kind.to_owned(),
        reference_id: reference_id.to_owned(),
        sha256: byte.to_string().repeat(64),
    }
}

fn artifact(
    fixture: &Fixture,
    outcome: RuntimeTerminalOutcome,
    visible: bool,
) -> FinalOutputArtifactDescriptor {
    FinalOutputArtifactDescriptor {
        artifact_id: "final-projection".to_owned(),
        session_id: "session-final".to_owned(),
        run_id: "run-final".to_owned(),
        run_generation: fixture.run_generation,
        run_lease_id: fixture.run_lease_id.clone(),
        terminal_outcome: outcome,
        content_sha256: "a".repeat(64),
        projection_sha256: "a".repeat(64),
        user_visible: visible,
        verification_evidence: vec![evidence("verification_report", "verification-1", 'b')],
        missing_artifacts: vec![evidence("missing_artifact", "artifact-missing-1", 'c')],
        active_process_state: vec![evidence("process_lease", "process-lease-1", 'd')],
        reason_code: outcome.reason_code().to_owned(),
        committed_at_unix_ms: runtime_finalization_now().expect("clock should be available"),
    }
}

fn intent(fixture: &Fixture, intent_id: &str) -> RuntimeDeliveryIntentDescriptor {
    RuntimeDeliveryIntentDescriptor {
        delivery_intent_id: intent_id.to_owned(),
        artifact_id: "final-projection".to_owned(),
        session_id: "session-final".to_owned(),
        run_id: "run-final".to_owned(),
        run_generation: fixture.run_generation,
        run_lease_id: fixture.run_lease_id.clone(),
        delivery_generation: fixture.delivery_generation,
        delivery_lease_id: fixture.delivery_lease_id.clone(),
        destination_binding_sha256: "e".repeat(64),
        connector_id: "echo:default".to_owned(),
        outbox_envelope_id: "final:run-final:1".to_owned(),
        content_sha256: "a".repeat(64),
        outbound_request_sha256: "6".repeat(64),
        dedupe_key: "echo:default:final:run-final:1".to_owned(),
        committed_at_unix_ms: runtime_finalization_now().expect("clock should be available"),
    }
}

fn observation(state: RuntimeDeliveryState, reason_code: &str) -> RuntimeDeliveryLinkObservation {
    RuntimeDeliveryLinkObservation {
        delivery_intent_id: "delivery-final".to_owned(),
        state,
        connector_id: "echo:default".to_owned(),
        outbox_envelope_id: "final:run-final:1".to_owned(),
        evidence_sha256: match state {
            RuntimeDeliveryState::Queued => "1".repeat(64),
            RuntimeDeliveryState::OutcomeUnknown => "2".repeat(64),
            RuntimeDeliveryState::Delivered => "3".repeat(64),
            RuntimeDeliveryState::IntentRecorded => "4".repeat(64),
        },
        reason_code: reason_code.to_owned(),
        native_message_id_sha256: (state == RuntimeDeliveryState::Delivered)
            .then(|| "f".repeat(64)),
        observed_at_unix_ms: runtime_finalization_now().expect("clock should be available"),
    }
}

#[test]
fn first_commit_uses_journal_time_for_artifact_and_intent_evidence() {
    let directory = tempfile::tempdir().expect("temporary directory should create");
    let fixture = setup(&directory.path().join("journal.sqlite3"));
    let mut descriptor = artifact(&fixture, RuntimeTerminalOutcome::Completed, true);
    descriptor.committed_at_unix_ms = 0;
    fixture
        .store
        .commit_runtime_final_output(&descriptor)
        .expect("artifact should commit with journal-owned time");
    let stored_artifact = fixture
        .store
        .runtime_final_output("run-final", fixture.run_generation)
        .expect("artifact should load")
        .expect("artifact should exist");
    assert!(
        stored_artifact.committed_at_unix_ms > 0,
        "caller time must not become durable artifact evidence"
    );

    let mut delivery_intent = intent(&fixture, "delivery-final");
    delivery_intent.committed_at_unix_ms = 0;
    fixture
        .store
        .commit_runtime_delivery_intent(&delivery_intent)
        .expect("intent should commit with journal-owned time");
    let connection = fixture.store.connection.lock().expect("journal lock should be available");
    let (intent_json, committed_at_unix_ms): (String, i64) = connection
        .query_row(
            r#"
                SELECT intent_json, committed_at_unix_ms
                FROM runtime_delivery_intents_v2
                WHERE delivery_intent_ulid = ?1
            "#,
            params!["delivery-final"],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("stored intent evidence should load");
    let stored_intent: RuntimeDeliveryIntentDescriptor =
        serde_json::from_str(&intent_json).expect("stored intent should decode");
    assert!(committed_at_unix_ms > 0);
    assert_eq!(stored_intent.committed_at_unix_ms, committed_at_unix_ms);
}

#[test]
fn backdated_artifact_commit_cannot_revive_an_expired_run_lease() {
    let directory = tempfile::tempdir().expect("temporary directory should create");
    let fixture = setup(&directory.path().join("journal.sqlite3"));
    let expired_at =
        runtime_finalization_now().expect("clock should be available").saturating_sub(1);
    {
        let connection = fixture.store.connection.lock().expect("journal lock should be available");
        connection
            .execute(
                r#"
                    UPDATE runtime_generation_leases
                    SET expires_at_unix_ms = ?1
                    WHERE session_ulid = ?2 AND lane = ?3
                "#,
                params![expired_at, "session-final", RuntimeGenerationLane::Run.as_str()],
            )
            .expect("run lease should expire");
    }
    let mut descriptor = artifact(&fixture, RuntimeTerminalOutcome::Completed, true);
    descriptor.committed_at_unix_ms = 0;
    assert!(matches!(
        fixture.store.commit_runtime_final_output(&descriptor),
        Err(JournalError::RuntimeFinalizationAuthorityStale { .. })
    ));
}

#[test]
fn superseded_delivery_authority_rejects_a_backdated_intent() {
    let directory = tempfile::tempdir().expect("temporary directory should create");
    let fixture = setup(&directory.path().join("journal.sqlite3"));
    fixture
        .store
        .commit_runtime_final_output(&artifact(&fixture, RuntimeTerminalOutcome::Completed, true))
        .expect("artifact should commit");
    fixture
        .store
        .activate_runtime_generation(&RuntimeGenerationActivateRequest {
            session_id: "session-final".to_owned(),
            run_id: Some("run-final".to_owned()),
            lane: RuntimeGenerationLane::Delivery,
            owner: "runtime-finalization-replacement".to_owned(),
            ttl_ms: 60_000,
            transition_kind: RuntimeGenerationTransitionKind::SteerSuperseded,
            reason_code: "runtime.delivery.test_superseded".to_owned(),
        })
        .expect("delivery authority should be superseded");
    let mut stale_intent = intent(&fixture, "delivery-final");
    stale_intent.committed_at_unix_ms = 0;
    assert!(matches!(
        fixture.store.commit_runtime_delivery_intent(&stale_intent),
        Err(JournalError::RuntimeFinalizationAuthorityStale { .. })
    ));
}

#[test]
fn final_output_load_rejects_schema_digest_and_denormalized_tampering() {
    let tamper_cases = [
        ("UPDATE runtime_final_output_artifacts SET schema_version = 2", "schema"),
        (
            "UPDATE runtime_final_output_artifacts SET descriptor_sha256 = 'tampered'",
            "descriptor digest",
        ),
        (
            "UPDATE runtime_final_output_artifacts SET content_sha256 = \
             'ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff'",
            "denormalized content digest",
        ),
    ];
    for (statement, label) in tamper_cases {
        let directory = tempfile::tempdir().expect("temporary directory should create");
        let fixture = setup(&directory.path().join("journal.sqlite3"));
        fixture
            .store
            .commit_runtime_final_output(&artifact(
                &fixture,
                RuntimeTerminalOutcome::Completed,
                true,
            ))
            .expect("artifact should commit");
        {
            let connection =
                fixture.store.connection.lock().expect("journal lock should be available");
            connection
                .execute_batch(
                    "DROP TRIGGER trg_runtime_final_artifacts_prevent_update;\
                     PRAGMA ignore_check_constraints = ON;",
                )
                .expect("test should allow durable evidence tampering");
            connection.execute(statement, []).expect("artifact row should be tampered");
        }
        assert!(
            matches!(
                fixture.store.runtime_final_output("run-final", fixture.run_generation),
                Err(JournalError::InvalidRuntimeFinalOutput { .. })
            ),
            "{label} tampering must fail closed"
        );
    }
}

#[test]
fn delivery_intent_load_rejects_schema_digest_and_denormalized_tampering() {
    let tamper_cases = [
        ("UPDATE runtime_delivery_intents_v2 SET schema_version = 2", "schema"),
        ("UPDATE runtime_delivery_intents_v2 SET intent_sha256 = 'tampered'", "intent digest"),
        (
            "UPDATE runtime_delivery_intents_v2 SET delivery_lease_ulid = 'tampered-lease'",
            "denormalized lease identity",
        ),
        (
            "UPDATE runtime_delivery_intents_v2 SET outbound_request_sha256 = \
             'ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff'",
            "denormalized outbound request digest",
        ),
    ];
    for (statement, label) in tamper_cases {
        let directory = tempfile::tempdir().expect("temporary directory should create");
        let fixture = setup(&directory.path().join("journal.sqlite3"));
        fixture
            .store
            .commit_runtime_final_output(&artifact(
                &fixture,
                RuntimeTerminalOutcome::Completed,
                true,
            ))
            .expect("artifact should commit");
        fixture
            .store
            .commit_runtime_delivery_intent(&intent(&fixture, "delivery-final"))
            .expect("intent should commit");
        {
            let connection =
                fixture.store.connection.lock().expect("journal lock should be available");
            connection
                .execute_batch(
                    "DROP TRIGGER trg_runtime_delivery_intents_v2_prevent_update;\
                     PRAGMA ignore_check_constraints = ON;",
                )
                .expect("test should allow durable evidence tampering");
            connection.execute(statement, []).expect("intent row should be tampered");
        }
        assert!(
            matches!(
                fixture.store.runtime_delivery_snapshot("delivery-final"),
                Err(JournalError::InvalidRuntimeDeliveryIntent { .. })
            ),
            "{label} tampering must fail closed"
        );
    }
}

#[test]
fn delivery_link_load_rejects_digest_json_and_denormalized_tampering() {
    let tamper_cases = [
        ("UPDATE runtime_delivery_links_v2 SET schema_version = 2", "schema"),
        ("UPDATE runtime_delivery_links_v2 SET link_sha256 = 'tampered'", "link digest"),
        ("UPDATE runtime_delivery_links_v2 SET link_json = '{}'", "canonical link JSON"),
        (
            "UPDATE runtime_delivery_links_v2 SET connector_id = 'tampered:connector'",
            "denormalized connector",
        ),
        (
            "UPDATE runtime_delivery_links_v2 SET delivery_intent_ulid = 'tampered-intent'",
            "denormalized intent identity",
        ),
        (
            "UPDATE runtime_delivery_links_v2 SET outbox_envelope_id = 'tampered-envelope'",
            "denormalized envelope",
        ),
        (
            "UPDATE runtime_delivery_links_v2 SET evidence_sha256 = \
             'ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff'",
            "denormalized evidence",
        ),
        (
            "UPDATE runtime_delivery_links_v2 SET reason_code = 'runtime.delivery.tampered'",
            "denormalized reason",
        ),
        (
            "UPDATE runtime_delivery_links_v2 SET observed_at_unix_ms = 999",
            "denormalized observation time",
        ),
        ("UPDATE runtime_delivery_links_v2 SET link_index = 999", "denormalized link index"),
        (
            "UPDATE runtime_delivery_links_v2 SET state = 'delivered', \
             native_message_id_sha256 = NULL",
            "state and native id invariant",
        ),
    ];
    for (statement, label) in tamper_cases {
        let directory = tempfile::tempdir().expect("temporary directory should create");
        let fixture = setup(&directory.path().join("journal.sqlite3"));
        fixture
            .store
            .commit_runtime_final_output(&artifact(
                &fixture,
                RuntimeTerminalOutcome::Completed,
                true,
            ))
            .expect("artifact should commit");
        fixture
            .store
            .commit_runtime_delivery_intent(&intent(&fixture, "delivery-final"))
            .expect("intent should commit");
        fixture
            .store
            .record_runtime_delivery_link(&observation(
                RuntimeDeliveryState::Queued,
                "runtime.delivery.outbox_queued",
            ))
            .expect("queued link should commit");
        {
            let connection =
                fixture.store.connection.lock().expect("journal lock should be available");
            connection
                .execute_batch(
                    "DROP TRIGGER trg_runtime_delivery_links_v2_prevent_update;\
                     PRAGMA ignore_check_constraints = ON;\
                     PRAGMA foreign_keys = OFF;",
                )
                .expect("test should allow durable link tampering");
            connection.execute(statement, []).expect("delivery link should be tampered");
        }
        assert!(
            matches!(
                fixture.store.runtime_delivery_snapshot("delivery-final"),
                Err(JournalError::InvalidRuntimeDeliveryIntent { .. })
            ),
            "{label} tampering must fail closed"
        );
    }
}

#[test]
fn duplicate_final_and_transport_replay_reuse_one_artifact() {
    let directory = tempfile::tempdir().expect("temporary directory should create");
    let fixture = setup(&directory.path().join("journal.sqlite3"));
    let descriptor = artifact(&fixture, RuntimeTerminalOutcome::Completed, true);

    assert_eq!(
        fixture.store.commit_runtime_final_output(&descriptor).expect("first final should commit"),
        RuntimeFinalizationCommitOutcome::Inserted
    );
    let mut replayed = descriptor.clone();
    replayed.committed_at_unix_ms += 120_000;
    assert_eq!(
        fixture
            .store
            .commit_runtime_final_output(&replayed)
            .expect("transport replay should be idempotent after lease expiry"),
        RuntimeFinalizationCommitOutcome::Existing
    );

    let mut conflicting = descriptor;
    conflicting.artifact_id = "second-final-projection".to_owned();
    assert!(matches!(
        fixture.store.commit_runtime_final_output(&conflicting),
        Err(JournalError::RuntimeFinalOutputConflict { .. })
    ));
}

#[test]
fn recovery_fills_artifact_to_intent_and_intent_to_queue_crash_windows() {
    let directory = tempfile::tempdir().expect("temporary directory should create");
    let fixture = setup(&directory.path().join("journal.sqlite3"));
    fixture
        .store
        .commit_runtime_final_output(&artifact(&fixture, RuntimeTerminalOutcome::Completed, true))
        .expect("artifact should commit");
    assert_eq!(
        fixture.store.runtime_delivery_state("delivery-final").expect("state lookup should work"),
        None,
        "crash after artifact leaves no invented intent"
    );

    fixture
        .store
        .commit_runtime_delivery_intent(&intent(&fixture, "delivery-final"))
        .expect("recovery should commit intent");
    assert_eq!(
        fixture.store.runtime_delivery_state("delivery-final").expect("intent state should load"),
        Some(RuntimeDeliveryState::IntentRecorded),
        "crash before enqueue remains recoverable without claiming send"
    );
    assert_eq!(
        fixture
            .store
            .record_runtime_delivery_link(&observation(
                RuntimeDeliveryState::Queued,
                "runtime.delivery.outbox_queued",
            ))
            .expect("recovery should link existing outbox"),
        RuntimeDeliveryState::Queued
    );
}

#[test]
fn connector_unknown_outcome_never_downgrades_to_queued() {
    let directory = tempfile::tempdir().expect("temporary directory should create");
    let fixture = setup(&directory.path().join("journal.sqlite3"));
    fixture
        .store
        .commit_runtime_final_output(&artifact(&fixture, RuntimeTerminalOutcome::Completed, true))
        .expect("artifact should commit");
    fixture
        .store
        .commit_runtime_delivery_intent(&intent(&fixture, "delivery-final"))
        .expect("intent should commit");
    fixture
        .store
        .record_runtime_delivery_link(&observation(
            RuntimeDeliveryState::OutcomeUnknown,
            "runtime.delivery.outcome_unknown",
        ))
        .expect("unknown should persist");
    let snapshot_before = fixture
        .store
        .runtime_delivery_snapshot("delivery-final")
        .expect("unknown snapshot should load")
        .expect("unknown snapshot should exist");
    assert_eq!(
        fixture
            .store
            .record_runtime_delivery_link(&observation(
                RuntimeDeliveryState::Queued,
                "runtime.delivery.outbox_queued",
            ))
            .expect("blind retry must be suppressed"),
        RuntimeDeliveryState::OutcomeUnknown
    );
    assert_eq!(
        fixture
            .store
            .runtime_delivery_snapshot("delivery-final")
            .expect("replayed unknown snapshot should load")
            .expect("replayed unknown snapshot should exist"),
        snapshot_before,
        "unknown replay must retain the original evidence"
    );
    let mut distinct_unknown = observation(
        RuntimeDeliveryState::OutcomeUnknown,
        "runtime.delivery.transport_disconnected",
    );
    distinct_unknown.evidence_sha256 = "9".repeat(64);
    assert_eq!(
        fixture
            .store
            .record_runtime_delivery_link(&distinct_unknown)
            .expect("repeated unknown must be suppressed"),
        RuntimeDeliveryState::OutcomeUnknown
    );
    assert_eq!(
        fixture
            .store
            .runtime_delivery_snapshot("delivery-final")
            .expect("suppressed unknown snapshot should load")
            .expect("suppressed unknown snapshot should exist"),
        snapshot_before,
        "a second unknown observation must not replace original evidence"
    );
}

#[test]
fn cancelled_hidden_run_has_no_delivery_intent() {
    let directory = tempfile::tempdir().expect("temporary directory should create");
    let fixture = setup(&directory.path().join("journal.sqlite3"));
    fixture
        .store
        .commit_runtime_final_output(&artifact(&fixture, RuntimeTerminalOutcome::Cancelled, false))
        .expect("cancelled artifact should commit");
    assert!(matches!(
        fixture.store.commit_runtime_delivery_intent(&intent(&fixture, "delivery-final")),
        Err(JournalError::RuntimeDeliveryNotVisible { .. })
    ));
}

#[test]
fn cancellation_after_ack_cannot_rewrite_delivery_history() {
    let directory = tempfile::tempdir().expect("temporary directory should create");
    let fixture = setup(&directory.path().join("journal.sqlite3"));
    fixture
        .store
        .commit_runtime_final_output(&artifact(&fixture, RuntimeTerminalOutcome::Completed, true))
        .expect("artifact should commit");
    fixture
        .store
        .commit_runtime_delivery_intent(&intent(&fixture, "delivery-final"))
        .expect("intent should commit");
    fixture
        .store
        .record_runtime_delivery_link(&observation(
            RuntimeDeliveryState::Delivered,
            "runtime.delivery.acknowledged",
        ))
        .expect("ack should persist");
    let snapshot_before = fixture
        .store
        .runtime_delivery_snapshot("delivery-final")
        .expect("ack snapshot should load")
        .expect("ack snapshot should exist");

    assert!(matches!(
        fixture.store.commit_runtime_final_output(&artifact(
            &fixture,
            RuntimeTerminalOutcome::Cancelled,
            false,
        )),
        Err(JournalError::RuntimeFinalOutputConflict { .. })
    ));
    assert_eq!(
        fixture
            .store
            .record_runtime_delivery_link(&observation(
                RuntimeDeliveryState::OutcomeUnknown,
                "runtime.delivery.transport_disconnected",
            ))
            .expect("post-ack observation should be ignored"),
        RuntimeDeliveryState::Delivered
    );
    assert_eq!(
        fixture
            .store
            .runtime_delivery_snapshot("delivery-final")
            .expect("replayed ack snapshot should load")
            .expect("replayed ack snapshot should exist"),
        snapshot_before,
        "post-ack replay must retain the original acknowledgement evidence"
    );
    let connection = fixture.store.connection.lock().expect("journal lock should be available");
    let link_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM runtime_delivery_links_v2 WHERE delivery_intent_ulid = ?1",
            params!["delivery-final"],
            |row| row.get(0),
        )
        .expect("link count should load");
    assert_eq!(link_count, 1, "post-ack replay must not append another link");
}

#[test]
fn duplicate_delivery_identity_and_link_create_one_durable_send_lane() {
    let directory = tempfile::tempdir().expect("temporary directory should create");
    let fixture = setup(&directory.path().join("journal.sqlite3"));
    fixture
        .store
        .commit_runtime_final_output(&artifact(&fixture, RuntimeTerminalOutcome::Completed, true))
        .expect("artifact should commit");
    let descriptor = intent(&fixture, "delivery-final");
    fixture.store.commit_runtime_delivery_intent(&descriptor).expect("intent should commit");
    let mut replayed = descriptor.clone();
    replayed.committed_at_unix_ms += 120_000;
    assert_eq!(
        fixture
            .store
            .commit_runtime_delivery_intent(&replayed)
            .expect("exact replay should be idempotent after lease expiry"),
        RuntimeFinalizationCommitOutcome::Existing
    );
    let mut changed_request = descriptor.clone();
    changed_request.outbound_request_sha256 = "7".repeat(64);
    assert!(matches!(
        fixture.store.commit_runtime_delivery_intent(&changed_request),
        Err(JournalError::RuntimeDeliveryIntentConflict { .. })
    ));
    let queued = observation(RuntimeDeliveryState::Queued, "runtime.delivery.outbox_queued");
    fixture.store.record_runtime_delivery_link(&queued).expect("first link should persist");
    fixture
        .store
        .record_runtime_delivery_link(&queued)
        .expect("duplicate link should be idempotent");

    let connection = fixture.store.connection.lock().expect("journal lock should be available");
    let intent_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM runtime_delivery_intents_v2 WHERE run_ulid = ?1",
            params!["run-final"],
            |row| row.get(0),
        )
        .expect("intent count should load");
    let link_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM runtime_delivery_links_v2 WHERE delivery_intent_ulid = ?1",
            params!["delivery-final"],
            |row| row.get(0),
        )
        .expect("link count should load");
    assert_eq!(intent_count, 1);
    assert_eq!(link_count, 1);
}
