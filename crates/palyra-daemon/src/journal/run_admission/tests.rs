//! Focused storage tests for the atomic RuntimeKernelV2 admission boundary.
//!
//! Every case uses the real migration and SQLite transaction path.

use std::{num::NonZeroU64, sync::Arc};

use palyra_common::runtime_contracts::{RuntimeGenerationLane, RuntimeGenerationTransitionKind};
use rusqlite::params;
use tempfile::TempDir;

use super::*;
use crate::journal::{JournalConfig, OrchestratorSessionUpsertRequest};

struct PersistKernelHead {
    fail: bool,
}

impl JournalRunAdmissionEvidenceHook for PersistKernelHead {
    fn persist_admit_now_evidence(
        &mut self,
        transaction: &rusqlite::Transaction<'_>,
        context: &JournalRunAdmissionHookContext<'_>,
        input: &JournalRunAdmissionEvidenceHookInput,
    ) -> Result<JournalRunAdmissionPersistedEvidence, JournalError> {
        if self.fail {
            return Err(JournalError::RunAdmissionEvidenceHook { reason: "injected".to_owned() });
        }
        assert_eq!(context.session.session_id, context.run_lease.session_id.as_str());
        assert_eq!(context.run_id, context.run_lease.run_id.as_ref().unwrap().as_str());
        assert!(!context.admission_id.is_empty());
        assert!(!context.initial_attempt_id.is_empty());
        let snapshot = "{}";
        let kernel_head_sha256 = sha256_hex(snapshot.as_bytes());
        transaction.execute(
            r#"
                INSERT INTO runtime_kernel_heads (
                    run_ulid, session_ulid, runtime_version, run_generation,
                    revision, snapshot_json, snapshot_sha256,
                    initialized_at_unix_ms, updated_at_unix_ms
                ) VALUES (?1, ?2, 'runtime_kernel_v2', ?3, 0, ?4, ?5, 1, 1)
            "#,
            params![
                context.run_id,
                context.session.session_id,
                generation_i64(context.run_lease.generation)?,
                snapshot,
                kernel_head_sha256,
            ],
        )?;
        Ok(JournalRunAdmissionPersistedEvidence {
            authority_decision_json: input.authority_input_json.clone(),
            authority_decision_sha256: input.authority_input_sha256.clone(),
            admission_snapshot_json: input.kernel_input_json.clone(),
            admission_snapshot_sha256: input.kernel_input_sha256.clone(),
            kernel_head_sha256,
        })
    }
}

fn store(path: &std::path::Path) -> JournalStore {
    JournalStore::open(JournalConfig {
        db_path: path.to_owned(),
        hash_chain_enabled: true,
        max_payload_bytes: 256 * 1024,
        max_events: 10_000,
    })
    .unwrap()
}

fn request(session_id: &str, suffix: &str, fresh: bool) -> JournalRunAdmissionRequest {
    let canonical = "{}".to_owned();
    let digest = sha256_hex(canonical.as_bytes());
    let mut request = JournalRunAdmissionRequest {
        admission_id: format!("admission-{suffix}"),
        idempotency_scope: format!("session:{session_id}"),
        idempotency_key: format!("key-{suffix}"),
        request_sha256: String::new(),
        trace_id: format!("trace-{suffix}"),
        run_id: format!("run-{suffix}"),
        initial_attempt_id: format!("attempt-{suffix}"),
        session: JournalRunAdmissionSessionSelector {
            session_id: Some(session_id.to_owned()),
            session_key: Some(session_id.to_owned()),
            session_label: None,
            require_existing: false,
            reset_session: false,
        },
        caller_principal: "principal".to_owned(),
        caller_device_id: "device".to_owned(),
        caller_channel: Some("console".to_owned()),
        origin_kind: RunAdmissionOriginKind::Console,
        origin_run_id: None,
        delegated_admission_json: None,
        queue_input: (!fresh).then(|| JournalRunAdmissionQueueInput {
            queued_input_id: format!("queued-{suffix}"),
            text: "follow up".to_owned(),
            requested_mode: "followup".to_owned(),
            policy_channel: "console".to_owned(),
            policy_agent: "default".to_owned(),
            safe_boundary_flags_json: canonical.clone(),
        }),
        fresh_run_intent: fresh,
        policy: JournalRunAdmissionPolicy {
            access_policy_json: canonical.clone(),
            queue_policy_json: canonical.clone(),
            access_policy_sha256: digest.clone(),
            queue_policy_sha256: digest.clone(),
            policy_sha256: sha256_hex(format!("{digest}:{digest}").as_bytes()),
            max_pending_queue_depth: 8,
            active_run_disposition: RunAdmissionDisposition::DurableQueue,
            forced_rejection_reason: None,
        },
        evidence_hook_input: JournalRunAdmissionEvidenceHookInput {
            authority_input_json: canonical.clone(),
            authority_input_sha256: digest.clone(),
            kernel_input_json: canonical,
            kernel_input_sha256: digest,
        },
        session_authority_intent: JournalSessionAuthorityIntent {
            configured_profile: JournalRuntimeProfile::V2,
            selected_runtime: JournalRuntimeAuthority::V2,
            reason: JournalRuntimeAuthorityReason::V2ProfileSelected,
            shadow_evaluation_enabled: false,
        },
    };
    request.request_sha256 = request_digest(&request);
    request
}

fn request_digest(request: &JournalRunAdmissionRequest) -> String {
    run_admission_request_sha256(request).unwrap()
}

fn count(store: &JournalStore, table: &str) -> i64 {
    let connection = store.connection.lock().unwrap();
    connection.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| row.get(0)).unwrap()
}

fn upsert_session(store: &JournalStore, session_id: &str) {
    store
        .upsert_orchestrator_session(&OrchestratorSessionUpsertRequest {
            session_id: session_id.to_owned(),
            session_key: session_id.to_owned(),
            session_label: None,
            principal: "principal".to_owned(),
            device_id: "device".to_owned(),
            channel: Some("console".to_owned()),
        })
        .unwrap();
}

fn legacy_pin_request(
    session_id: &str,
    expected_revision: u64,
) -> JournalInitialSessionAuthorityPinRequest {
    JournalInitialSessionAuthorityPinRequest {
        session_id: session_id.to_owned(),
        expected_revision,
        intent: JournalSessionAuthorityIntent {
            configured_profile: JournalRuntimeProfile::Legacy,
            selected_runtime: JournalRuntimeAuthority::Legacy,
            reason: JournalRuntimeAuthorityReason::LegacyProfileSelected,
            shadow_evaluation_enabled: false,
        },
        migration_reason_code: "runtime.session_authority.test".to_owned(),
    }
}

fn migration_proof(
    session_id: &str,
    expected_revision: u64,
    target: JournalSessionAuthorityIntent,
) -> HostVerifiedSessionAuthorityMigration {
    HostVerifiedSessionAuthorityMigration::test_only(
        session_id.to_owned(),
        NonZeroU64::new(expected_revision).unwrap(),
        target,
    )
}

#[test]
fn admit_now_is_atomic_and_exact_replay_does_not_allocate_again() {
    let temp = TempDir::new().unwrap();
    let store = store(&temp.path().join("journal.sqlite3"));
    let request = request("session-a", "a", true);
    let first =
        store.commit_run_admission(&request, &mut PersistKernelHead { fail: false }).unwrap();
    assert_eq!(first.disposition, RunAdmissionDisposition::AdmitNow);
    assert!(!first.replayed);
    assert_eq!(first.allocated_run_id.as_deref(), Some("run-a"));
    assert_eq!(first.initial_attempt_id.as_deref(), Some("attempt-a"));
    assert!(first.run_lease.is_some());
    assert!(first.authority_decision_sha256.is_some());
    assert!(first.admission_snapshot_sha256.is_some());
    assert!(first.kernel_head_sha256.is_some());
    assert_eq!(first.session_authority_pin.as_ref().map(|pin| pin.revision), Some(1));

    let replay =
        store.commit_run_admission(&request, &mut PersistKernelHead { fail: true }).unwrap();
    assert!(replay.replayed);
    assert_eq!(count(&store, "orchestrator_runs"), 1);
    assert_eq!(count(&store, "runtime_run_initial_attempt_reservations"), 1);
    assert_eq!(count(&store, "runtime_generation_events"), 1);
    assert_eq!(count(&store, "runtime_session_write_lease_bindings"), 1);
    assert_eq!(count(&store, "runtime_session_authority_pin_history"), 1);
    assert_eq!(count(&store, "runtime_run_admission_pin_bindings"), 1);
}

#[test]
fn conflict_and_identity_mismatch_leave_zero_new_allocations() {
    let temp = TempDir::new().unwrap();
    let store = store(&temp.path().join("journal.sqlite3"));
    let original = request("session-b", "b", true);
    store.commit_run_admission(&original, &mut PersistKernelHead { fail: false }).unwrap();
    let mut conflict = original.clone();
    conflict.run_id = "run-conflict".to_owned();
    conflict.request_sha256 = request_digest(&conflict);
    assert!(matches!(
        store.commit_run_admission(&conflict, &mut PersistKernelHead { fail: false }),
        Err(JournalError::RunAdmissionIdempotencyConflict { .. })
    ));

    let mut mismatch = request("session-b", "mismatch", false);
    mismatch.caller_device_id = "other-device".to_owned();
    mismatch.request_sha256 = request_digest(&mismatch);
    assert!(matches!(
        store.commit_run_admission(&mismatch, &mut PersistKernelHead { fail: false }),
        Err(JournalError::SessionIdentityMismatch { .. })
    ));
    assert_eq!(count(&store, "runtime_run_admissions"), 1);
    assert_eq!(count(&store, "orchestrator_queued_inputs"), 0);
}

#[test]
fn fresh_reject_pins_session_and_exact_retry_reuses_authority() {
    let temp = TempDir::new().unwrap();
    let store = store(&temp.path().join("journal.sqlite3"));
    let mut rejected = request("session-reject", "reject", true);
    rejected.policy.forced_rejection_reason = Some("policy.denied".to_owned());
    rejected.request_sha256 = request_digest(&rejected);
    let first =
        store.commit_run_admission(&rejected, &mut PersistKernelHead { fail: true }).unwrap();
    assert_eq!(first.disposition, RunAdmissionDisposition::Reject);
    assert!(!first.replayed);
    assert_eq!(first.session_authority_pin.as_ref().map(|pin| pin.revision), Some(1));
    let replay =
        store.commit_run_admission(&rejected, &mut PersistKernelHead { fail: true }).unwrap();
    assert!(replay.replayed);
    assert_eq!(
        replay.session_authority_pin.as_ref().map(|pin| pin.pin_sha256.as_str()),
        first.session_authority_pin.as_ref().map(|pin| pin.pin_sha256.as_str())
    );

    let admitted = store
        .commit_run_admission(
            &request("session-reject", "retry", true),
            &mut PersistKernelHead { fail: false },
        )
        .unwrap();
    assert_eq!(admitted.disposition, RunAdmissionDisposition::AdmitNow);
    assert_eq!(admitted.session_authority_pin.as_ref().map(|pin| pin.revision), Some(1));
    assert_eq!(count(&store, "runtime_session_authority_pin_history"), 1);
    assert_eq!(count(&store, "runtime_run_admission_pin_bindings"), 2);
}

#[test]
fn queue_dispositions_bind_active_generation_without_allocating() {
    let temp = TempDir::new().unwrap();
    let store = store(&temp.path().join("journal.sqlite3"));
    store
        .commit_run_admission(
            &request("session-c", "active", true),
            &mut PersistKernelHead { fail: false },
        )
        .unwrap();
    for (index, disposition) in [
        RunAdmissionDisposition::DurableQueue,
        RunAdmissionDisposition::Merge,
        RunAdmissionDisposition::SteerCandidate,
    ]
    .into_iter()
    .enumerate()
    {
        let mut queued = request("session-c", &format!("q{index}"), false);
        queued.policy.active_run_disposition = disposition;
        queued.request_sha256 = request_digest(&queued);
        let outcome =
            store.commit_run_admission(&queued, &mut PersistKernelHead { fail: true }).unwrap();
        assert_eq!(outcome.disposition, disposition);
        assert_eq!(outcome.target_active_run_id.as_deref(), Some("run-active"));
        assert!(outcome.allocated_run_id.is_none());
        assert!(outcome.initial_attempt_id.is_none());
        assert!(outcome.run_lease.is_none());
    }
    assert_eq!(count(&store, "orchestrator_runs"), 1);
    assert_eq!(count(&store, "runtime_generation_events"), 1);
    assert_eq!(count(&store, "runtime_run_admission_queue_bindings"), 3);
    assert_eq!(count(&store, "runtime_run_admission_pin_bindings"), 4);
}

#[test]
fn active_run_queue_dispositions_reject_mismatched_session_pin() {
    let temp = TempDir::new().unwrap();
    let store = store(&temp.path().join("journal.sqlite3"));
    store
        .commit_run_admission(
            &request("session-queue-pin", "active", true),
            &mut PersistKernelHead { fail: false },
        )
        .unwrap();
    for (index, disposition) in [
        RunAdmissionDisposition::DurableQueue,
        RunAdmissionDisposition::Merge,
        RunAdmissionDisposition::SteerCandidate,
    ]
    .into_iter()
    .enumerate()
    {
        let mut mismatched = request("session-queue-pin", &format!("mismatch-{index}"), false);
        mismatched.policy.active_run_disposition = disposition;
        mismatched.session_authority_intent = JournalSessionAuthorityIntent {
            configured_profile: JournalRuntimeProfile::V2Canary,
            selected_runtime: JournalRuntimeAuthority::V2,
            reason: JournalRuntimeAuthorityReason::V2CanarySessionSelected,
            shadow_evaluation_enabled: false,
        };
        mismatched.request_sha256 = request_digest(&mismatched);
        assert!(matches!(
            store.commit_run_admission(&mismatched, &mut PersistKernelHead { fail: false }),
            Err(JournalError::InvalidRunAdmission { .. })
        ));
    }
    assert_eq!(count(&store, "runtime_run_admissions"), 1);
    assert_eq!(count(&store, "orchestrator_queued_inputs"), 0);
}

#[test]
fn capacity_hook_failure_and_append_only_guards_roll_back() {
    let temp = TempDir::new().unwrap();
    let store = store(&temp.path().join("journal.sqlite3"));
    let failed = request("session-d", "failed", true);
    assert!(matches!(
        store.commit_run_admission(&failed, &mut PersistKernelHead { fail: true }),
        Err(JournalError::RunAdmissionEvidenceHook { .. })
    ));
    assert_eq!(count(&store, "orchestrator_runs"), 0);
    assert_eq!(count(&store, "runtime_run_admissions"), 0);
    assert_eq!(count(&store, "runtime_session_authority_pin_history"), 0);
    assert_eq!(count(&store, "runtime_run_admission_pin_bindings"), 0);

    store
        .commit_run_admission(
            &request("session-d", "active", true),
            &mut PersistKernelHead { fail: false },
        )
        .unwrap();
    let mut overflow = request("session-d", "overflow", false);
    overflow.policy.max_pending_queue_depth = 0;
    overflow.request_sha256 = request_digest(&overflow);
    assert!(matches!(
        store.commit_run_admission(&overflow, &mut PersistKernelHead { fail: false }),
        Err(JournalError::RunAdmissionQueueCapacityExceeded { .. })
    ));

    let connection = store.connection.lock().unwrap();
    assert!(connection
        .execute("UPDATE runtime_run_admissions SET reason_code = 'changed'", [],)
        .is_err());
    assert!(connection.execute("DELETE FROM runtime_run_admissions", []).is_err());
}

#[test]
fn concurrent_stores_admit_exactly_one_run_and_migration_reopens_once() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("journal.sqlite3");
    let first = Arc::new(store(&path));
    drop(first);
    let left = Arc::new(store(&path));
    let right = Arc::new(store(&path));
    let left_thread = {
        let store = Arc::clone(&left);
        std::thread::spawn(move || {
            store.commit_run_admission(
                &request("session-e", "left", true),
                &mut PersistKernelHead { fail: false },
            )
        })
    };
    let right_thread = {
        let store = Arc::clone(&right);
        std::thread::spawn(move || {
            store.commit_run_admission(
                &request("session-e", "right", true),
                &mut PersistKernelHead { fail: false },
            )
        })
    };
    let outcomes = [left_thread.join().unwrap().unwrap(), right_thread.join().unwrap().unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| outcome.disposition == RunAdmissionDisposition::AdmitNow)
            .count(),
        1
    );
    assert_eq!(count(&left, "orchestrator_runs"), 1);
    assert_eq!(count(&left, "runtime_run_initial_attempt_reservations"), 1);
    let connection = left.connection.lock().unwrap();
    let migrations: i64 = connection
        .query_row("SELECT COUNT(*) FROM schema_migrations WHERE version = 75", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(migrations, 1);
}

#[test]
fn initial_pin_is_restart_stable_and_migration_is_append_only_cas() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("journal.sqlite3");
    let initial_store = store(&path);
    upsert_session(&initial_store, "session-pin");
    let request = legacy_pin_request("session-pin", 0);
    let created = initial_store.pin_initial_session_runtime_authority(&request).unwrap();
    let JournalSessionAuthorityPinOutcome::Created(created) = created else {
        panic!("first pin must be created");
    };
    assert_eq!(created.revision, 1);
    assert_eq!(
        initial_store.pin_initial_session_runtime_authority(&request).unwrap(),
        JournalSessionAuthorityPinOutcome::Existing(created.clone())
    );
    drop(initial_store);

    let reopened = store(&path);
    assert_eq!(reopened.load_session_runtime_authority("session-pin").unwrap(), Some(created));
    let v2_intent = JournalSessionAuthorityIntent {
        configured_profile: JournalRuntimeProfile::V2,
        selected_runtime: JournalRuntimeAuthority::V2,
        reason: JournalRuntimeAuthorityReason::V2ProfileSelected,
        shadow_evaluation_enabled: false,
    };
    let proof = migration_proof("session-pin", 1, v2_intent.clone());
    let migrated = reopened.migrate_session_runtime_authority(&proof).unwrap();
    assert_eq!(migrated.revision, 2);
    assert_eq!(
        migrated.safe_boundary_evidence,
        Some(serde_json::json!({
            "expected_revision": 1,
            "proof_kind": "host_verified_no_active_run",
            "reason_code": "runtime.session_authority.configured_profile_change",
            "schema_version": 1
        }))
    );
    assert_eq!(count(&reopened, "runtime_session_authority_pin_history"), 2);
    assert!(reopened
        .migrate_session_runtime_authority(&migration_proof("session-pin", 1, v2_intent))
        .is_err());

    let connection = reopened.connection.lock().unwrap();
    connection
        .execute_batch(
            "DROP TRIGGER trg_runtime_session_authority_pin_prevent_update;
             UPDATE runtime_session_authority_pin_history
             SET pin_sha256 = 'ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff'
             WHERE session_ulid = 'session-pin' AND revision = 2;",
        )
        .unwrap();
    drop(connection);
    assert!(reopened.load_session_runtime_authority("session-pin").is_err());
}

#[test]
fn migration_api_has_no_caller_supplied_json_evidence_field() {
    assert!(!include_str!("types.rs").contains("safe_boundary_evidence_json"));
}

#[test]
fn conflicting_initial_pin_race_has_one_durable_winner() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("journal.sqlite3");
    let initial = store(&path);
    upsert_session(&initial, "session-race");
    drop(initial);
    let left = Arc::new(store(&path));
    let right = Arc::new(store(&path));
    let left_thread = {
        let store = Arc::clone(&left);
        std::thread::spawn(move || {
            store.pin_initial_session_runtime_authority(&legacy_pin_request("session-race", 0))
        })
    };
    let right_thread = {
        let store = Arc::clone(&right);
        std::thread::spawn(move || {
            store.pin_initial_session_runtime_authority(&JournalInitialSessionAuthorityPinRequest {
                session_id: "session-race".to_owned(),
                expected_revision: 0,
                intent: JournalSessionAuthorityIntent {
                    configured_profile: JournalRuntimeProfile::V2,
                    selected_runtime: JournalRuntimeAuthority::V2,
                    reason: JournalRuntimeAuthorityReason::V2ProfileSelected,
                    shadow_evaluation_enabled: false,
                },
                migration_reason_code: "runtime.session_authority.test".to_owned(),
            })
        })
    };
    let outcomes = [left_thread.join().unwrap(), right_thread.join().unwrap()];
    assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(count(&left, "runtime_session_authority_pin_history"), 1);
}

#[test]
fn concurrent_migration_cas_appends_exactly_one_revision() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("journal.sqlite3");
    let initial = store(&path);
    upsert_session(&initial, "session-migration-race");
    initial
        .pin_initial_session_runtime_authority(&legacy_pin_request("session-migration-race", 0))
        .unwrap();
    drop(initial);
    let left = Arc::new(store(&path));
    let right = Arc::new(store(&path));
    let left_thread = {
        let store = Arc::clone(&left);
        std::thread::spawn(move || {
            store.migrate_session_runtime_authority(&migration_proof(
                "session-migration-race",
                1,
                JournalSessionAuthorityIntent {
                    configured_profile: JournalRuntimeProfile::V2,
                    selected_runtime: JournalRuntimeAuthority::V2,
                    reason: JournalRuntimeAuthorityReason::V2ProfileSelected,
                    shadow_evaluation_enabled: false,
                },
            ))
        })
    };
    let right_thread = {
        let store = Arc::clone(&right);
        std::thread::spawn(move || {
            store.migrate_session_runtime_authority(&migration_proof(
                "session-migration-race",
                1,
                JournalSessionAuthorityIntent {
                    configured_profile: JournalRuntimeProfile::V2Canary,
                    selected_runtime: JournalRuntimeAuthority::V2,
                    reason: JournalRuntimeAuthorityReason::V2CanarySessionSelected,
                    shadow_evaluation_enabled: false,
                },
            ))
        })
    };
    let outcomes = [left_thread.join().unwrap(), right_thread.join().unwrap()];
    assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(count(&left, "runtime_session_authority_pin_history"), 2);
    assert_eq!(
        left.load_session_runtime_authority("session-migration-race").unwrap().unwrap().revision,
        2
    );
}

#[test]
fn migration_requires_a_safe_boundary_and_records_prior_generation() {
    let temp = TempDir::new().unwrap();
    let store = store(&temp.path().join("journal.sqlite3"));
    store
        .commit_run_admission(
            &request("session-migrate", "migrate", true),
            &mut PersistKernelHead { fail: false },
        )
        .unwrap();
    let migration =
        migration_proof("session-migrate", 1, legacy_pin_request("session-migrate", 0).intent);
    assert!(store.migrate_session_runtime_authority(&migration).is_err());
    store
        .update_orchestrator_run_state("run-migrate", RunLifecycleState::InProgress, None)
        .unwrap();
    store.update_orchestrator_run_state("run-migrate", RunLifecycleState::Done, None).unwrap();
    store
        .invalidate_runtime_generation(&shared_runtime::RuntimeGenerationInvalidateRequest {
            session_id: "session-migrate".to_owned(),
            run_id: Some("run-migrate".to_owned()),
            lane: RuntimeGenerationLane::Run,
            transition_kind: RuntimeGenerationTransitionKind::Released,
            reason_code: "runtime.session_authority.test_safe_boundary".to_owned(),
        })
        .unwrap();
    let migrated = store.migrate_session_runtime_authority(&migration).unwrap();
    assert_eq!(migrated.revision, 2);
    assert_eq!(migrated.created_after_run_generation, 1);
}

#[test]
fn schema_checks_reject_malformed_dispositions_and_all_origins_are_closed() {
    let _origins = [
        RunAdmissionOriginKind::Console,
        RunAdmissionOriginKind::Channel,
        RunAdmissionOriginKind::Cron,
        RunAdmissionOriginKind::Internal,
        RunAdmissionOriginKind::Delegation,
    ];
    let temp = TempDir::new().unwrap();
    let store = store(&temp.path().join("journal.sqlite3"));
    store
        .commit_run_admission(
            &request("session-f", "active", true),
            &mut PersistKernelHead { fail: false },
        )
        .unwrap();
    let connection = store.connection.lock().unwrap();
    let malformed = connection.execute(
        r#"
            INSERT INTO runtime_run_admissions (
                admission_ulid, idempotency_scope, idempotency_key, request_sha256,
                trace_ulid, disposition, reason_code, origin_kind, session_ulid,
                caller_binding_sha256, access_policy_sha256, queue_policy_sha256,
                policy_sha256, created_at_unix_ms
            ) VALUES ('bad', 'bad', 'bad', 'bad', 'bad', 'admit_now', 'bad',
                'console', 'session-f', 'bad', 'bad', 'bad', 'bad', 1)
        "#,
        [],
    );
    assert!(malformed.is_err());
}
