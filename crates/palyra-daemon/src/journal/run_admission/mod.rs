//! Atomic journal admission for every RuntimeKernelV2 Run entrypoint.
//!
//! Session ownership, capacity, Run authority, queue evidence, and immutable
//! admission evidence are decided under one SQLite `IMMEDIATE` transaction.

#[cfg(test)]
mod tests;
mod types;

use palyra_common::runtime_contracts::{
    GenerationLeaseV1, RuntimeActorKind, RuntimeActorRef, RuntimeGeneration, RuntimeGenerationLane,
    RuntimeGenerationTransitionKind,
};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde::Serialize;
use serde_json::{Map, Value};
use ulid::Ulid;

use super::{
    acquire_session_write_lease_tx, active_session_last_run, append_run_lifecycle_event_tx,
    current_unix_ms, hydrate_orchestrator_session_snapshot, internal_session_write_lease_request,
    load_orchestrator_session_by_id, load_orchestrator_session_by_key,
    load_orchestrator_session_by_label, metadata_trace, normalize_optional_session_field,
    release_session_write_lease_record_tx, sha256_hex, shared_runtime, JournalError, JournalStore,
    OrchestratorRunStartRequest, OrchestratorSessionRecord, RunLifecycleEventAppendRequest,
    ORCHESTRATOR_TITLE_GENERATION_STATE_IDLE, ORCHESTRATOR_TITLE_GENERATION_STATE_MANUAL_LOCKED,
};
use crate::application::runtime_kernel_v2::runtime_selection::HostVerifiedSessionAuthorityMigration;
use crate::orchestrator::RunLifecycleState;

pub(crate) use types::{
    JournalInitialSessionAuthorityPinRequest, JournalRunAdmissionEvidenceHook,
    JournalRunAdmissionEvidenceHookInput, JournalRunAdmissionHookContext,
    JournalRunAdmissionOutcome, JournalRunAdmissionPersistedEvidence, JournalRunAdmissionPolicy,
    JournalRunAdmissionQueueInput, JournalRunAdmissionRequest, JournalRunAdmissionSessionSelector,
    JournalRuntimeAuthority, JournalRuntimeAuthorityReason, JournalRuntimeProfile,
    JournalSessionAuthorityIntent, JournalSessionAuthorityPin, JournalSessionAuthorityPinOutcome,
    RunAdmissionDisposition, RunAdmissionOriginKind,
};

const RUN_LEASE_TTL_MS: i64 = 24 * 60 * 60 * 1_000;
const MAX_ADMISSION_TEXT_BYTES: usize = 256 * 1024;
const MAX_ADMISSION_ID_BYTES: usize = 256;
const SESSION_AUTHORITY_PIN_SCHEMA_VERSION: u32 = 1;

/// Migration 73: immutable admission, attempt, queue-generation, and writer-binding evidence.
pub(super) const MIGRATION_73_SQL: &str = r#"
    CREATE TABLE IF NOT EXISTS runtime_run_admissions ( admission_ulid TEXT PRIMARY KEY,
        idempotency_scope TEXT NOT NULL, idempotency_key TEXT NOT NULL,
        request_sha256 TEXT NOT NULL, trace_ulid TEXT NOT NULL,
        disposition TEXT NOT NULL CHECK (
            disposition IN ('reject', 'durable_queue', 'merge', 'steer_candidate', 'admit_now')
        ),
        reason_code TEXT NOT NULL, origin_kind TEXT NOT NULL CHECK (
            origin_kind IN ('console', 'channel', 'cron', 'internal', 'delegation')
        ),
        origin_run_ulid TEXT, delegated_admission_json TEXT,
        session_ulid TEXT NOT NULL,
        allocated_run_ulid TEXT, target_active_run_ulid TEXT,
        queued_input_ulid TEXT, initial_attempt_ulid TEXT,
        allocated_run_generation INTEGER, run_lease_ulid TEXT,
        caller_binding_sha256 TEXT NOT NULL,
        access_policy_sha256 TEXT NOT NULL, queue_policy_sha256 TEXT NOT NULL,
        policy_sha256 TEXT NOT NULL,
        authority_decision_json TEXT, authority_decision_sha256 TEXT,
        admission_snapshot_json TEXT, admission_snapshot_sha256 TEXT,
        kernel_head_sha256 TEXT,
        created_at_unix_ms INTEGER NOT NULL,
        schema_version INTEGER NOT NULL DEFAULT 1 CHECK (schema_version = 1),
        UNIQUE(idempotency_scope, idempotency_key),
        UNIQUE(allocated_run_ulid),
        UNIQUE(initial_attempt_ulid),
        FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid), FOREIGN KEY(allocated_run_ulid) REFERENCES orchestrator_runs(run_ulid),
        FOREIGN KEY(target_active_run_ulid) REFERENCES orchestrator_runs(run_ulid), FOREIGN KEY(queued_input_ulid) REFERENCES orchestrator_queued_inputs(queued_input_ulid),
        CHECK (
            (disposition = 'admit_now'
                AND allocated_run_ulid IS NOT NULL
                AND target_active_run_ulid IS NULL
                AND queued_input_ulid IS NULL
                AND initial_attempt_ulid IS NOT NULL
                AND allocated_run_generation IS NOT NULL
                AND allocated_run_generation > 0
                AND run_lease_ulid IS NOT NULL
                AND authority_decision_json IS NOT NULL
                AND authority_decision_sha256 IS NOT NULL
                AND admission_snapshot_json IS NOT NULL
                AND admission_snapshot_sha256 IS NOT NULL
                AND kernel_head_sha256 IS NOT NULL)
            OR
            (disposition IN ('durable_queue', 'merge', 'steer_candidate')
                AND allocated_run_ulid IS NULL
                AND target_active_run_ulid IS NOT NULL
                AND queued_input_ulid IS NOT NULL
                AND initial_attempt_ulid IS NULL
                AND allocated_run_generation IS NULL
                AND run_lease_ulid IS NULL
                AND authority_decision_json IS NULL
                AND authority_decision_sha256 IS NULL
                AND admission_snapshot_json IS NULL
                AND admission_snapshot_sha256 IS NULL
                AND kernel_head_sha256 IS NULL)
            OR
            (disposition = 'reject'
                AND allocated_run_ulid IS NULL
                AND target_active_run_ulid IS NULL
                AND queued_input_ulid IS NULL
                AND initial_attempt_ulid IS NULL
                AND allocated_run_generation IS NULL
                AND run_lease_ulid IS NULL
                AND authority_decision_json IS NULL
                AND authority_decision_sha256 IS NULL
                AND admission_snapshot_json IS NULL
                AND admission_snapshot_sha256 IS NULL
                AND kernel_head_sha256 IS NULL)
        )
    ); CREATE INDEX IF NOT EXISTS idx_runtime_run_admissions_session_created
        ON runtime_run_admissions(session_ulid, created_at_unix_ms, admission_ulid);
    CREATE TABLE IF NOT EXISTS runtime_run_initial_attempt_reservations (
        attempt_ulid TEXT PRIMARY KEY, admission_ulid TEXT NOT NULL UNIQUE,
        session_ulid TEXT NOT NULL,
        run_ulid TEXT NOT NULL UNIQUE,
        run_generation INTEGER NOT NULL CHECK (run_generation > 0),
        run_lease_ulid TEXT NOT NULL,
        state TEXT NOT NULL DEFAULT 'reserved' CHECK (state = 'reserved'),
        reserved_at_unix_ms INTEGER NOT NULL,
        schema_version INTEGER NOT NULL DEFAULT 1 CHECK (schema_version = 1),
        FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid), FOREIGN KEY(run_ulid) REFERENCES orchestrator_runs(run_ulid)
    ); CREATE TABLE IF NOT EXISTS runtime_run_admission_queue_bindings (
        admission_ulid TEXT PRIMARY KEY,
        queued_input_ulid TEXT NOT NULL UNIQUE,
        session_ulid TEXT NOT NULL,
        target_run_ulid TEXT NOT NULL,
        target_run_generation INTEGER NOT NULL CHECK (target_run_generation > 0),
        target_run_lease_ulid TEXT NOT NULL,
        bound_at_unix_ms INTEGER NOT NULL,
        schema_version INTEGER NOT NULL DEFAULT 1 CHECK (schema_version = 1),
        FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid), FOREIGN KEY(target_run_ulid) REFERENCES orchestrator_runs(run_ulid),
        FOREIGN KEY(queued_input_ulid) REFERENCES orchestrator_queued_inputs(queued_input_ulid)
    ); CREATE TABLE IF NOT EXISTS runtime_session_write_lease_bindings (
        admission_ulid TEXT PRIMARY KEY,
        session_ulid TEXT NOT NULL,
        writer_lease_ulid TEXT NOT NULL,
        run_ulid TEXT NOT NULL,
        run_generation INTEGER NOT NULL CHECK (run_generation > 0),
        run_lease_ulid TEXT NOT NULL,
        bound_at_unix_ms INTEGER NOT NULL,
        schema_version INTEGER NOT NULL DEFAULT 1 CHECK (schema_version = 1),
        FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid), FOREIGN KEY(run_ulid) REFERENCES orchestrator_runs(run_ulid)
    );
    CREATE TRIGGER IF NOT EXISTS trg_runtime_run_admissions_prevent_update BEFORE UPDATE ON runtime_run_admissions
    BEGIN SELECT RAISE(ABORT, 'runtime_run_admissions is append-only'); END;
    CREATE TRIGGER IF NOT EXISTS trg_runtime_run_admissions_prevent_delete BEFORE DELETE ON runtime_run_admissions
    BEGIN SELECT RAISE(ABORT, 'runtime_run_admissions is append-only'); END;
    CREATE TRIGGER IF NOT EXISTS trg_runtime_run_initial_attempts_prevent_update BEFORE UPDATE ON runtime_run_initial_attempt_reservations
    BEGIN SELECT RAISE(ABORT, 'runtime_run_initial_attempt_reservations is append-only'); END;
    CREATE TRIGGER IF NOT EXISTS trg_runtime_run_initial_attempts_prevent_delete BEFORE DELETE ON runtime_run_initial_attempt_reservations
    BEGIN SELECT RAISE(ABORT, 'runtime_run_initial_attempt_reservations is append-only'); END;
    CREATE TRIGGER IF NOT EXISTS trg_runtime_run_admission_queue_bindings_prevent_update BEFORE UPDATE ON runtime_run_admission_queue_bindings
    BEGIN SELECT RAISE(ABORT, 'runtime_run_admission_queue_bindings is append-only'); END;
    CREATE TRIGGER IF NOT EXISTS trg_runtime_run_admission_queue_bindings_prevent_delete BEFORE DELETE ON runtime_run_admission_queue_bindings
    BEGIN SELECT RAISE(ABORT, 'runtime_run_admission_queue_bindings is append-only'); END;
    CREATE TRIGGER IF NOT EXISTS trg_runtime_session_write_lease_bindings_prevent_update BEFORE UPDATE ON runtime_session_write_lease_bindings
    BEGIN SELECT RAISE(ABORT, 'runtime_session_write_lease_bindings is append-only'); END;
    CREATE TRIGGER IF NOT EXISTS trg_runtime_session_write_lease_bindings_prevent_delete BEFORE DELETE ON runtime_session_write_lease_bindings
    BEGIN SELECT RAISE(ABORT, 'runtime_session_write_lease_bindings is append-only'); END;
"#;

/// Migration 75: append-only identity-free session authority pins and admission bindings.
pub(super) const MIGRATION_75_SQL: &str = r#"
    CREATE TABLE IF NOT EXISTS runtime_session_authority_pin_history (
        session_ulid TEXT NOT NULL,
        revision INTEGER NOT NULL CHECK (revision > 0),
        configured_profile TEXT NOT NULL CHECK (
            configured_profile IN ('legacy', 'v2_shadow', 'v2_canary', 'v2')
        ),
        selected_runtime TEXT NOT NULL CHECK (selected_runtime IN ('legacy', 'v2')),
        reason_code TEXT NOT NULL CHECK (
            reason_code IN (
                'legacy_profile_selected',
                'v2_shadow_legacy_authority',
                'v2_canary_session_excluded',
                'v2_canary_session_selected',
                'v2_profile_selected'
            )
        ),
        shadow_evaluation_enabled INTEGER NOT NULL CHECK (
            shadow_evaluation_enabled IN (0, 1)
        ),
        created_after_run_generation INTEGER NOT NULL CHECK (
            created_after_run_generation >= 0
        ),
        created_at_unix_ms INTEGER NOT NULL,
        migration_reason_code TEXT NOT NULL,
        safe_boundary_evidence_json TEXT,
        pin_json TEXT NOT NULL,
        pin_sha256 TEXT NOT NULL,
        schema_version INTEGER NOT NULL DEFAULT 1 CHECK (schema_version = 1),
        PRIMARY KEY(session_ulid, revision),
        FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid),
        CHECK (
            (configured_profile = 'legacy'
                AND selected_runtime = 'legacy'
                AND reason_code = 'legacy_profile_selected'
                AND shadow_evaluation_enabled = 0)
            OR
            (configured_profile = 'v2_shadow'
                AND selected_runtime = 'legacy'
                AND reason_code = 'v2_shadow_legacy_authority'
                AND shadow_evaluation_enabled = 1)
            OR
            (configured_profile = 'v2_canary'
                AND selected_runtime = 'legacy'
                AND reason_code = 'v2_canary_session_excluded'
                AND shadow_evaluation_enabled = 0)
            OR
            (configured_profile = 'v2_canary'
                AND selected_runtime = 'v2'
                AND reason_code = 'v2_canary_session_selected'
                AND shadow_evaluation_enabled = 0)
            OR
            (configured_profile = 'v2'
                AND selected_runtime = 'v2'
                AND reason_code = 'v2_profile_selected'
                AND shadow_evaluation_enabled = 0)
        )
    );
    CREATE INDEX IF NOT EXISTS idx_runtime_session_authority_pin_latest
        ON runtime_session_authority_pin_history(session_ulid, revision DESC);
    CREATE TABLE IF NOT EXISTS runtime_run_admission_pin_bindings (
        admission_ulid TEXT PRIMARY KEY,
        session_ulid TEXT NOT NULL,
        pin_revision INTEGER NOT NULL CHECK (pin_revision > 0),
        bound_at_unix_ms INTEGER NOT NULL,
        schema_version INTEGER NOT NULL DEFAULT 1 CHECK (schema_version = 1),
        FOREIGN KEY(admission_ulid) REFERENCES runtime_run_admissions(admission_ulid),
        FOREIGN KEY(session_ulid, pin_revision)
            REFERENCES runtime_session_authority_pin_history(session_ulid, revision)
    );
    CREATE TRIGGER IF NOT EXISTS trg_runtime_session_authority_pin_prevent_update
    BEFORE UPDATE ON runtime_session_authority_pin_history BEGIN
        SELECT RAISE(ABORT, 'runtime_session_authority_pin_history is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_runtime_session_authority_pin_prevent_delete
    BEFORE DELETE ON runtime_session_authority_pin_history BEGIN
        SELECT RAISE(ABORT, 'runtime_session_authority_pin_history is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_runtime_run_admission_pin_bindings_prevent_update
    BEFORE UPDATE ON runtime_run_admission_pin_bindings BEGIN
        SELECT RAISE(ABORT, 'runtime_run_admission_pin_bindings is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_runtime_run_admission_pin_bindings_prevent_delete
    BEFORE DELETE ON runtime_run_admission_pin_bindings BEGIN
        SELECT RAISE(ABORT, 'runtime_run_admission_pin_bindings is append-only');
    END;
"#;

#[derive(Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionAuthorityPinPayload {
    schema_version: u32,
    revision: u64,
    configured_profile: JournalRuntimeProfile,
    selected_runtime: JournalRuntimeAuthority,
    reason: JournalRuntimeAuthorityReason,
    shadow_evaluation_enabled: bool,
    created_after_run_generation: u64,
    created_at_unix_ms: i64,
    migration_reason_code: String,
    safe_boundary_evidence: Option<Value>,
}

#[derive(Serialize)]
struct SessionAuthoritySafeBoundaryEvidence<'a> {
    schema_version: u32,
    proof_kind: &'static str,
    reason_code: &'a str,
    expected_revision: u64,
}

#[derive(Serialize)]
struct CanonicalRequest<'a> {
    admission_id: &'a str,
    idempotency_scope: &'a str,
    idempotency_key: &'a str,
    trace_id: &'a str,
    run_id: &'a str,
    initial_attempt_id: &'a str,
    session: &'a JournalRunAdmissionSessionSelector,
    caller_principal: &'a str,
    caller_device_id: &'a str,
    caller_channel: &'a Option<String>,
    origin_kind: RunAdmissionOriginKind,
    origin_run_id: &'a Option<String>,
    delegated_admission_json: &'a Option<String>,
    queue_input: &'a Option<JournalRunAdmissionQueueInput>,
    fresh_run_intent: bool,
    policy: &'a JournalRunAdmissionPolicy,
    evidence_hook_input: &'a JournalRunAdmissionEvidenceHookInput,
    session_authority_intent: &'a JournalSessionAuthorityIntent,
}

#[derive(Debug)]
struct StoredAdmission {
    request_sha256: String,
    session_id: String,
    disposition: RunAdmissionDisposition,
    reason_code: String,
    allocated_run_id: Option<String>,
    #[cfg(test)]
    target_active_run_id: Option<String>,
    #[cfg(test)]
    queued_input_id: Option<String>,
    initial_attempt_id: Option<String>,
    generation: Option<RuntimeGeneration>,
    run_lease_id: Option<String>,
    authority_sha256: Option<String>,
    snapshot_sha256: Option<String>,
    kernel_head_sha256: Option<String>,
    session_authority_pin_revision: Option<u64>,
}

/// Computes the canonical digest accepted by the durable admission boundary.
pub(crate) fn run_admission_request_sha256(
    request: &JournalRunAdmissionRequest,
) -> Result<String, JournalError> {
    let canonical = serde_json::to_string(&CanonicalRequest {
        admission_id: request.admission_id.as_str(),
        idempotency_scope: request.idempotency_scope.as_str(),
        idempotency_key: request.idempotency_key.as_str(),
        trace_id: request.trace_id.as_str(),
        run_id: request.run_id.as_str(),
        initial_attempt_id: request.initial_attempt_id.as_str(),
        session: &request.session,
        caller_principal: request.caller_principal.as_str(),
        caller_device_id: request.caller_device_id.as_str(),
        caller_channel: &request.caller_channel,
        origin_kind: request.origin_kind,
        origin_run_id: &request.origin_run_id,
        delegated_admission_json: &request.delegated_admission_json,
        queue_input: &request.queue_input,
        fresh_run_intent: request.fresh_run_intent,
        policy: &request.policy,
        evidence_hook_input: &request.evidence_hook_input,
        session_authority_intent: &request.session_authority_intent,
    })?;
    Ok(sha256_hex(canonical.as_bytes()))
}

impl JournalStore {
    /// Creates the first durable session route or returns the identical winner.
    ///
    /// The session must already exist and have no active Run. Concurrent
    /// different intents fail compare-and-swap instead of replacing authority.
    ///
    /// # Errors
    /// Fails when the request is invalid, the session is missing or active, a
    /// different pin already won the CAS, or durable storage fails.
    pub(crate) fn pin_initial_session_runtime_authority(
        &self,
        request: &JournalInitialSessionAuthorityPinRequest,
    ) -> Result<JournalSessionAuthorityPinOutcome, JournalError> {
        validate_initial_session_authority_pin_request(request)?;
        if request.expected_revision != 0 {
            return invalid("initial session authority pin requires expected revision zero");
        }
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        require_session_without_active_run(&transaction, request.session_id.as_str(), now)?;
        if let Some(existing) =
            load_latest_session_authority_pin_tx(&transaction, request.session_id.as_str())?
        {
            if pin_matches_intent(&existing, &request.intent) {
                transaction.commit()?;
                return Ok(JournalSessionAuthorityPinOutcome::Existing(existing));
            }
            return invalid("session authority pin compare-and-swap conflict");
        }
        let created_after_run_generation =
            latest_session_run_generation_tx(&transaction, request.session_id.as_str())?;
        require_zero_generation_only_for_fresh_session(
            &transaction,
            request.session_id.as_str(),
            created_after_run_generation,
        )?;
        let pin = insert_session_authority_pin_tx(
            &transaction,
            request,
            1,
            created_after_run_generation,
            now,
            None,
        )?;
        transaction.commit()?;
        Ok(JournalSessionAuthorityPinOutcome::Created(pin))
    }

    /// Appends a safe-boundary session authority migration with exact CAS.
    ///
    /// # Errors
    /// Fails when the session has an active Run, the sealed proof revision is
    /// stale, the proposed route is invalid, or durable storage fails.
    pub(crate) fn migrate_session_runtime_authority(
        &self,
        proof: &HostVerifiedSessionAuthorityMigration,
    ) -> Result<JournalSessionAuthorityPin, JournalError> {
        let request = JournalInitialSessionAuthorityPinRequest {
            session_id: proof.session_id().to_owned(),
            expected_revision: proof.expected_revision(),
            intent: proof.target().clone(),
            migration_reason_code: proof.reason_code().to_owned(),
        };
        validate_initial_session_authority_pin_request(&request)?;
        let safe_boundary_evidence_json = canonical_json(&SessionAuthoritySafeBoundaryEvidence {
            schema_version: 1,
            proof_kind: "host_verified_no_active_run",
            reason_code: proof.reason_code(),
            expected_revision: proof.expected_revision(),
        })?;
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        require_session_without_active_run(&transaction, request.session_id.as_str(), now)?;
        let current =
            load_latest_session_authority_pin_tx(&transaction, request.session_id.as_str())?
                .ok_or_else(|| JournalError::InvalidRunAdmission {
                    reason: "session authority migration requires an existing pin".to_owned(),
                })?;
        if current.revision != request.expected_revision {
            return invalid("session authority pin compare-and-swap conflict");
        }
        if pin_matches_intent(&current, &request.intent) {
            return invalid("session authority migration must change the selected route");
        }
        let revision =
            current.revision.checked_add(1).ok_or_else(|| JournalError::InvalidRunAdmission {
                reason: "session authority pin revision is exhausted".to_owned(),
            })?;
        let created_after_run_generation =
            latest_session_run_generation_tx(&transaction, request.session_id.as_str())?;
        let pin = insert_session_authority_pin_tx(
            &transaction,
            &request,
            revision,
            created_after_run_generation,
            now,
            Some(safe_boundary_evidence_json.as_str()),
        )?;
        transaction.commit()?;
        Ok(pin)
    }

    /// Loads and validates the latest identity-free session authority pin.
    ///
    /// # Errors
    /// Fails when the session identifier or persisted pin is invalid, or when
    /// durable storage cannot be read.
    pub(crate) fn load_session_runtime_authority(
        &self,
        session_id: &str,
    ) -> Result<Option<JournalSessionAuthorityPin>, JournalError> {
        validate_identifier("session_id", session_id)?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        load_latest_session_authority_pin_tx(&guard, session_id)
    }

    /// Commits one admission decision, all allocations, and V2 evidence atomically.
    ///
    /// # Errors
    /// Returns explicit validation, ownership, idempotency, capacity, hook, or
    /// storage errors. Any error rolls back every mutation in this boundary.
    pub(crate) fn commit_run_admission<H: JournalRunAdmissionEvidenceHook>(
        &self,
        request: &JournalRunAdmissionRequest,
        hook: &mut H,
    ) -> Result<JournalRunAdmissionOutcome, JournalError> {
        validate_request(request, self.config.max_payload_bytes)?;
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;

        if let Some(stored) = load_admission_tx(
            &transaction,
            request.idempotency_scope.as_str(),
            request.idempotency_key.as_str(),
        )? {
            if stored.request_sha256 != request.request_sha256 {
                return Err(JournalError::RunAdmissionIdempotencyConflict {
                    idempotency_scope: request.idempotency_scope.clone(),
                    idempotency_key: request.idempotency_key.clone(),
                });
            }
            let outcome = hydrate_outcome_tx(&transaction, stored, now, true)?;
            transaction.commit()?;
            return Ok(outcome);
        }

        let resolved = resolve_orchestrator_session_tx(&transaction, request, now)?;
        let session = resolved.session;
        let lease_request = internal_session_write_lease_request(
            session.session_id.as_str(),
            "commit_run_admission",
            false,
        );
        let writer_lease = acquire_session_write_lease_tx(&transaction, &lease_request, now)?;
        let active_run_id = active_session_last_run(&transaction, session.session_id.as_str())?;
        let active_run_lease = active_run_id
            .as_deref()
            .map(|run_id| {
                shared_runtime::active_runtime_generation_tx(
                    &transaction,
                    session.session_id.as_str(),
                    run_id,
                    RuntimeGenerationLane::Run,
                    now,
                )
            })
            .transpose()?
            .flatten();
        if active_run_id.is_some() && active_run_lease.is_none() {
            return Err(JournalError::InvalidRunAdmission {
                reason: "active session run lacks exact live Run generation".to_owned(),
            });
        }
        let pending_depth = pending_queue_depth_tx(&transaction, session.session_id.as_str())?;
        let (disposition, reason_code) =
            decide_disposition(request, active_run_id.as_deref(), pending_depth)?;
        let session_authority_pin = ensure_admission_session_authority_pin_tx(
            &transaction,
            session.session_id.as_str(),
            &request.session_authority_intent,
            active_run_id.is_none(),
            now,
        )?;

        let outcome = match disposition {
            RunAdmissionDisposition::Reject => {
                insert_admission_tx(
                    &transaction,
                    request,
                    session.session_id.as_str(),
                    disposition,
                    reason_code.as_str(),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    now,
                )?;
                bind_admission_session_authority_pin_tx(
                    &transaction,
                    request.admission_id.as_str(),
                    session.session_id.as_str(),
                    session_authority_pin.revision,
                    now,
                )?;
                outcome_from_parts(
                    session.clone(),
                    disposition,
                    reason_code,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(session_authority_pin),
                    false,
                )
            }
            RunAdmissionDisposition::DurableQueue
            | RunAdmissionDisposition::Merge
            | RunAdmissionDisposition::SteerCandidate => {
                let active_run_id =
                    active_run_id.ok_or_else(|| JournalError::InvalidRunAdmission {
                        reason: "queue disposition requires an active run".to_owned(),
                    })?;
                let active_run_lease =
                    active_run_lease.ok_or_else(|| JournalError::InvalidRunAdmission {
                        reason: "queue disposition requires an active Run generation".to_owned(),
                    })?;
                let queue_input = request.queue_input.as_ref().ok_or_else(|| {
                    JournalError::InvalidRunAdmission {
                        reason: "queue disposition requires queue input".to_owned(),
                    }
                })?;
                insert_queued_input_tx(
                    &transaction,
                    QueuedInputInsert {
                        request,
                        input: queue_input,
                        session_id: session.session_id.as_str(),
                        active_run_id: active_run_id.as_str(),
                        disposition,
                        reason: reason_code.as_str(),
                        now,
                    },
                )?;
                transaction.execute(
                    r#"
                        INSERT INTO runtime_run_admission_queue_bindings (
                            admission_ulid, queued_input_ulid, session_ulid,
                            target_run_ulid, target_run_generation,
                            target_run_lease_ulid, bound_at_unix_ms
                        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                    "#,
                    params![
                        request.admission_id,
                        queue_input.queued_input_id,
                        session.session_id,
                        active_run_id,
                        generation_i64(active_run_lease.generation)?,
                        active_run_lease.lease_id.as_str(),
                        now,
                    ],
                )?;
                insert_admission_tx(
                    &transaction,
                    request,
                    session.session_id.as_str(),
                    disposition,
                    reason_code.as_str(),
                    None,
                    Some(active_run_id.as_str()),
                    Some(queue_input.queued_input_id.as_str()),
                    None,
                    None,
                    None,
                    None,
                    now,
                )?;
                bind_admission_session_authority_pin_tx(
                    &transaction,
                    request.admission_id.as_str(),
                    session.session_id.as_str(),
                    session_authority_pin.revision,
                    now,
                )?;
                outcome_from_parts(
                    session.clone(),
                    disposition,
                    reason_code,
                    Some(active_run_id),
                    Some(queue_input.queued_input_id.clone()),
                    None,
                    None,
                    None,
                    None,
                    Some(session_authority_pin),
                    false,
                )
            }
            RunAdmissionDisposition::AdmitNow => {
                let start = OrchestratorRunStartRequest {
                    run_id: request.run_id.clone(),
                    session_id: session.session_id.clone(),
                    origin_kind: request.origin_kind.as_str().to_owned(),
                    origin_run_id: request.origin_run_id.clone(),
                    triggered_by_principal: Some(request.caller_principal.clone()),
                    parameter_delta_json: None,
                    delegated_admission: None,
                };
                insert_run_tx(&transaction, request, &start, now)?;
                metadata_trace::create_root_metadata_trace_tx(&transaction, &start, now)?;
                let run_lease = shared_runtime::activate_or_refresh_run_generation_tx(
                    &transaction,
                    session.session_id.as_str(),
                    request.run_id.as_str(),
                    "runtime_run_admission",
                    RUN_LEASE_TTL_MS,
                    RuntimeGenerationTransitionKind::Activated,
                    "runtime.generation.run_admitted_v2",
                    now,
                )?;
                reserve_initial_attempt_tx(&transaction, request, &session, &run_lease, now)?;
                bind_writer_lease_tx(
                    &transaction,
                    request.admission_id.as_str(),
                    &writer_lease,
                    request.run_id.as_str(),
                    &run_lease,
                    now,
                )?;
                let evidence = hook.persist_admit_now_evidence(
                    &transaction,
                    &JournalRunAdmissionHookContext {
                        admission_id: request.admission_id.as_str(),
                        trace_id: request.trace_id.as_str(),
                        session: &session,
                        run_id: request.run_id.as_str(),
                        initial_attempt_id: request.initial_attempt_id.as_str(),
                        run_lease: &run_lease,
                        max_payload_bytes: self.config.max_payload_bytes,
                    },
                    &request.evidence_hook_input,
                )?;
                validate_persisted_evidence(&evidence)?;
                insert_admission_tx(
                    &transaction,
                    request,
                    session.session_id.as_str(),
                    disposition,
                    reason_code.as_str(),
                    Some(request.run_id.as_str()),
                    None,
                    None,
                    Some(request.initial_attempt_id.as_str()),
                    Some(&run_lease),
                    Some(&evidence),
                    None,
                    now,
                )?;
                bind_admission_session_authority_pin_tx(
                    &transaction,
                    request.admission_id.as_str(),
                    session.session_id.as_str(),
                    session_authority_pin.revision,
                    now,
                )?;
                outcome_from_parts(
                    session.clone(),
                    disposition,
                    reason_code,
                    None,
                    None,
                    Some(request.run_id.clone()),
                    Some(run_lease),
                    Some(request.initial_attempt_id.clone()),
                    Some(evidence),
                    Some(session_authority_pin),
                    false,
                )
            }
        };

        if !release_session_write_lease_record_tx(&transaction, &writer_lease, now)? {
            return Err(JournalError::InvalidRunAdmission {
                reason: "session writer lease disappeared before admission commit".to_owned(),
            });
        }
        transaction.commit()?;
        Ok(outcome)
    }
}

fn validate_initial_session_authority_pin_request(
    request: &JournalInitialSessionAuthorityPinRequest,
) -> Result<(), JournalError> {
    validate_identifier("session_id", request.session_id.as_str())?;
    validate_identifier("migration_reason_code", request.migration_reason_code.as_str())?;
    validate_session_authority_intent(&request.intent)?;
    Ok(())
}

fn validate_session_authority_intent(
    intent: &JournalSessionAuthorityIntent,
) -> Result<(), JournalError> {
    let valid = matches!(
        (
            intent.configured_profile,
            intent.selected_runtime,
            intent.reason,
            intent.shadow_evaluation_enabled,
        ),
        (
            JournalRuntimeProfile::Legacy,
            JournalRuntimeAuthority::Legacy,
            JournalRuntimeAuthorityReason::LegacyProfileSelected,
            false,
        ) | (
            JournalRuntimeProfile::V2Shadow,
            JournalRuntimeAuthority::Legacy,
            JournalRuntimeAuthorityReason::V2ShadowLegacyAuthority,
            true,
        ) | (
            JournalRuntimeProfile::V2Canary,
            JournalRuntimeAuthority::Legacy,
            JournalRuntimeAuthorityReason::V2CanarySessionExcluded,
            false,
        ) | (
            JournalRuntimeProfile::V2Canary,
            JournalRuntimeAuthority::V2,
            JournalRuntimeAuthorityReason::V2CanarySessionSelected,
            false,
        ) | (
            JournalRuntimeProfile::V2,
            JournalRuntimeAuthority::V2,
            JournalRuntimeAuthorityReason::V2ProfileSelected,
            false,
        )
    );
    if valid {
        Ok(())
    } else {
        invalid("session authority intent is inconsistent")
    }
}

fn require_session_without_active_run(
    connection: &Connection,
    session_id: &str,
    now: i64,
) -> Result<(), JournalError> {
    if load_orchestrator_session_by_id(connection, session_id)?.is_none() {
        return Err(JournalError::SessionNotFound { selector: session_id.to_owned() });
    }
    let live_run_lease: bool = connection.query_row(
        r#"
            SELECT EXISTS(
                SELECT 1
                FROM runtime_generation_leases
                WHERE session_ulid = ?1 AND lane = 'run' AND expires_at_unix_ms > ?2
            )
        "#,
        params![session_id, now],
        |row| row.get(0),
    )?;
    if active_session_last_run(connection, session_id)?.is_some() || live_run_lease {
        return invalid("session authority pin change requires no active run");
    }
    Ok(())
}

fn latest_session_run_generation_tx(
    connection: &Connection,
    session_id: &str,
) -> Result<u64, JournalError> {
    let generation = connection.query_row(
        r#"
            SELECT COALESCE(MAX(COALESCE(to_generation, from_generation, 0)), 0)
            FROM runtime_generation_events
            WHERE session_ulid = ?1 AND lane = 'run'
        "#,
        params![session_id],
        |row| row.get::<_, i64>(0),
    )?;
    u64::try_from(generation).map_err(|_| JournalError::InvalidRunAdmission {
        reason: "persisted session Run generation is negative".to_owned(),
    })
}

fn require_zero_generation_only_for_fresh_session(
    connection: &Connection,
    session_id: &str,
    generation: u64,
) -> Result<(), JournalError> {
    if generation != 0 {
        return Ok(());
    }
    let prior_runs: i64 = connection.query_row(
        "SELECT COUNT(*) FROM orchestrator_runs WHERE session_ulid = ?1",
        params![session_id],
        |row| row.get(0),
    )?;
    if prior_runs == 0 {
        Ok(())
    } else {
        invalid("zero-generation session authority pin requires a fresh session")
    }
}

fn pin_matches_intent(
    pin: &JournalSessionAuthorityPin,
    intent: &JournalSessionAuthorityIntent,
) -> bool {
    pin.configured_profile == intent.configured_profile
        && pin.selected_runtime == intent.selected_runtime
        && pin.reason == intent.reason
        && pin.shadow_evaluation_enabled == intent.shadow_evaluation_enabled
}

fn ensure_admission_session_authority_pin_tx(
    connection: &Connection,
    session_id: &str,
    intent: &JournalSessionAuthorityIntent,
    allow_initial_pin: bool,
    now: i64,
) -> Result<JournalSessionAuthorityPin, JournalError> {
    validate_session_authority_intent(intent)?;
    if intent.selected_runtime != JournalRuntimeAuthority::V2 {
        return invalid("V2 admission requires a V2 session authority pin");
    }
    if let Some(existing) = load_latest_session_authority_pin_tx(connection, session_id)? {
        if pin_matches_intent(&existing, intent) {
            return Ok(existing);
        }
        return invalid("V2 admission conflicts with the durable session authority pin");
    }
    if !allow_initial_pin {
        return invalid("active Run admission requires an existing exact session authority pin");
    }
    let created_after_run_generation = latest_session_run_generation_tx(connection, session_id)?;
    require_zero_generation_only_for_fresh_session(
        connection,
        session_id,
        created_after_run_generation,
    )?;
    insert_session_authority_pin_tx(
        connection,
        &JournalInitialSessionAuthorityPinRequest {
            session_id: session_id.to_owned(),
            expected_revision: 0,
            intent: intent.clone(),
            migration_reason_code: "runtime.session_authority.initial_v2_admission".to_owned(),
        },
        1,
        created_after_run_generation,
        now,
        None,
    )
}

fn bind_admission_session_authority_pin_tx(
    connection: &Connection,
    admission_id: &str,
    session_id: &str,
    pin_revision: u64,
    now: i64,
) -> Result<(), JournalError> {
    connection.execute(
        r#"
            INSERT INTO runtime_run_admission_pin_bindings (
                admission_ulid, session_ulid, pin_revision, bound_at_unix_ms
            ) VALUES (?1, ?2, ?3, ?4)
        "#,
        params![
            admission_id,
            session_id,
            i64::try_from(pin_revision).map_err(|_| JournalError::InvalidRunAdmission {
                reason: "session authority pin revision exceeds SQLite range".to_owned(),
            })?,
            now,
        ],
    )?;
    Ok(())
}

fn insert_session_authority_pin_tx(
    connection: &Connection,
    request: &JournalInitialSessionAuthorityPinRequest,
    revision: u64,
    created_after_run_generation: u64,
    now: i64,
    safe_boundary_evidence_json: Option<&str>,
) -> Result<JournalSessionAuthorityPin, JournalError> {
    let safe_boundary_evidence =
        safe_boundary_evidence_json.map(serde_json::from_str).transpose()?;
    let payload = SessionAuthorityPinPayload {
        schema_version: SESSION_AUTHORITY_PIN_SCHEMA_VERSION,
        revision,
        configured_profile: request.intent.configured_profile,
        selected_runtime: request.intent.selected_runtime,
        reason: request.intent.reason,
        shadow_evaluation_enabled: request.intent.shadow_evaluation_enabled,
        created_after_run_generation,
        created_at_unix_ms: now,
        migration_reason_code: request.migration_reason_code.clone(),
        safe_boundary_evidence,
    };
    let pin_json = canonical_json(&payload)?;
    let pin_sha256 = sha256_hex(pin_json.as_bytes());
    connection.execute(
        r#"
            INSERT INTO runtime_session_authority_pin_history (
                session_ulid, revision, configured_profile, selected_runtime,
                reason_code, shadow_evaluation_enabled,
                created_after_run_generation, created_at_unix_ms,
                migration_reason_code, safe_boundary_evidence_json,
                pin_json, pin_sha256
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
        "#,
        params![
            request.session_id,
            i64::try_from(revision).map_err(|_| JournalError::InvalidRunAdmission {
                reason: "session authority pin revision exceeds SQLite range".to_owned(),
            })?,
            request.intent.configured_profile.as_str(),
            request.intent.selected_runtime.as_str(),
            request.intent.reason.as_str(),
            if request.intent.shadow_evaluation_enabled { 1_i64 } else { 0_i64 },
            i64::try_from(created_after_run_generation).map_err(|_| {
                JournalError::InvalidRunAdmission {
                    reason: "session authority generation exceeds SQLite range".to_owned(),
                }
            })?,
            now,
            request.migration_reason_code,
            safe_boundary_evidence_json,
            pin_json,
            pin_sha256,
        ],
    )?;
    load_session_authority_pin_revision_tx(connection, request.session_id.as_str(), revision)?
        .ok_or_else(|| JournalError::InvalidRunAdmission {
            reason: "inserted session authority pin could not be reloaded".to_owned(),
        })
}

fn canonical_json(value: &impl Serialize) -> Result<String, JournalError> {
    let value = serde_json::to_value(value)?;
    Ok(serde_json::to_string(&canonicalize_json(value))?)
}

fn load_latest_session_authority_pin_tx(
    connection: &Connection,
    session_id: &str,
) -> Result<Option<JournalSessionAuthorityPin>, JournalError> {
    let revision = connection
        .query_row(
            r#"
                SELECT revision
                FROM runtime_session_authority_pin_history
                WHERE session_ulid = ?1
                ORDER BY revision DESC
                LIMIT 1
            "#,
            params![session_id],
            |row| row.get::<_, i64>(0),
        )
        .optional()?;
    revision
        .map(|revision| {
            let revision =
                u64::try_from(revision).map_err(|_| JournalError::InvalidRunAdmission {
                    reason: "session authority pin revision is invalid".to_owned(),
                })?;
            load_session_authority_pin_revision_tx(connection, session_id, revision)?.ok_or_else(
                || JournalError::InvalidRunAdmission {
                    reason: "latest session authority pin disappeared".to_owned(),
                },
            )
        })
        .transpose()
}

fn load_session_authority_pin_revision_tx(
    connection: &Connection,
    session_id: &str,
    revision: u64,
) -> Result<Option<JournalSessionAuthorityPin>, JournalError> {
    let revision_i64 = i64::try_from(revision).map_err(|_| JournalError::InvalidRunAdmission {
        reason: "session authority pin revision exceeds SQLite range".to_owned(),
    })?;
    connection
        .query_row(
            r#"
                SELECT configured_profile, selected_runtime, reason_code,
                    shadow_evaluation_enabled, created_after_run_generation,
                    created_at_unix_ms, migration_reason_code,
                    safe_boundary_evidence_json, pin_json, pin_sha256, schema_version
                FROM runtime_session_authority_pin_history
                WHERE session_ulid = ?1 AND revision = ?2
            "#,
            params![session_id, revision_i64],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, String>(9)?,
                    row.get::<_, i64>(10)?,
                ))
            },
        )
        .optional()?
        .map(
            |(
                configured_profile,
                selected_runtime,
                reason_code,
                shadow,
                created_generation,
                created_at,
                migration_reason,
                safe_evidence_json,
                pin_json,
                pin_sha256,
                schema_version,
            )| {
                hydrate_session_authority_pin(
                    revision,
                    configured_profile.as_str(),
                    selected_runtime.as_str(),
                    reason_code.as_str(),
                    shadow,
                    created_generation,
                    created_at,
                    migration_reason.as_str(),
                    safe_evidence_json.as_deref(),
                    pin_json.as_str(),
                    pin_sha256.as_str(),
                    schema_version,
                )
            },
        )
        .transpose()
}

#[allow(clippy::too_many_arguments)]
fn hydrate_session_authority_pin(
    revision: u64,
    configured_profile: &str,
    selected_runtime: &str,
    reason_code: &str,
    shadow: i64,
    created_generation: i64,
    created_at: i64,
    migration_reason: &str,
    safe_evidence_json: Option<&str>,
    pin_json: &str,
    pin_sha256: &str,
    schema_version: i64,
) -> Result<JournalSessionAuthorityPin, JournalError> {
    if schema_version != i64::from(SESSION_AUTHORITY_PIN_SCHEMA_VERSION)
        || shadow != 0 && shadow != 1
        || created_generation < 0
    {
        return invalid("session authority pin columns are invalid");
    }
    let canonical = validate_canonical_json("session_authority_pin", pin_json)?;
    if sha256_hex(canonical.as_bytes()) != pin_sha256 {
        return invalid("session authority pin digest mismatch");
    }
    let payload: SessionAuthorityPinPayload = serde_json::from_str(canonical.as_str())?;
    let profile = parse_runtime_profile(configured_profile)?;
    let authority = parse_runtime_authority(selected_runtime)?;
    let reason = parse_runtime_authority_reason(reason_code)?;
    let safe_boundary_evidence = safe_evidence_json
        .map(|raw| {
            let canonical = validate_canonical_json("safe_boundary_evidence", raw)?;
            serde_json::from_str::<Value>(canonical.as_str()).map_err(JournalError::from)
        })
        .transpose()?;
    let created_after_run_generation =
        u64::try_from(created_generation).map_err(|_| JournalError::InvalidRunAdmission {
            reason: "session authority generation is invalid".to_owned(),
        })?;
    if payload.schema_version != SESSION_AUTHORITY_PIN_SCHEMA_VERSION
        || payload.revision != revision
        || payload.configured_profile != profile
        || payload.selected_runtime != authority
        || payload.reason != reason
        || payload.shadow_evaluation_enabled != (shadow == 1)
        || payload.created_after_run_generation != created_after_run_generation
        || payload.created_at_unix_ms != created_at
        || payload.migration_reason_code != migration_reason
        || payload.safe_boundary_evidence != safe_boundary_evidence
    {
        return invalid("session authority pin JSON does not match indexed columns");
    }
    let pin = JournalSessionAuthorityPin {
        schema_version: payload.schema_version,
        revision: payload.revision,
        configured_profile: payload.configured_profile,
        selected_runtime: payload.selected_runtime,
        reason: payload.reason,
        shadow_evaluation_enabled: payload.shadow_evaluation_enabled,
        created_after_run_generation: payload.created_after_run_generation,
        created_at_unix_ms: payload.created_at_unix_ms,
        migration_reason_code: payload.migration_reason_code,
        safe_boundary_evidence: payload.safe_boundary_evidence,
        pin_sha256: pin_sha256.to_owned(),
    };
    validate_session_authority_intent(&JournalSessionAuthorityIntent {
        configured_profile: pin.configured_profile,
        selected_runtime: pin.selected_runtime,
        reason: pin.reason,
        shadow_evaluation_enabled: pin.shadow_evaluation_enabled,
    })?;
    Ok(pin)
}

fn parse_runtime_profile(raw: &str) -> Result<JournalRuntimeProfile, JournalError> {
    match raw {
        "legacy" => Ok(JournalRuntimeProfile::Legacy),
        "v2_shadow" => Ok(JournalRuntimeProfile::V2Shadow),
        "v2_canary" => Ok(JournalRuntimeProfile::V2Canary),
        "v2" => Ok(JournalRuntimeProfile::V2),
        _ => invalid("session authority pin profile is invalid"),
    }
}

fn parse_runtime_authority(raw: &str) -> Result<JournalRuntimeAuthority, JournalError> {
    match raw {
        "legacy" => Ok(JournalRuntimeAuthority::Legacy),
        "v2" => Ok(JournalRuntimeAuthority::V2),
        _ => invalid("session authority pin selected runtime is invalid"),
    }
}

fn parse_runtime_authority_reason(
    raw: &str,
) -> Result<JournalRuntimeAuthorityReason, JournalError> {
    match raw {
        "legacy_profile_selected" => Ok(JournalRuntimeAuthorityReason::LegacyProfileSelected),
        "v2_shadow_legacy_authority" => Ok(JournalRuntimeAuthorityReason::V2ShadowLegacyAuthority),
        "v2_canary_session_excluded" => Ok(JournalRuntimeAuthorityReason::V2CanarySessionExcluded),
        "v2_canary_session_selected" => Ok(JournalRuntimeAuthorityReason::V2CanarySessionSelected),
        "v2_profile_selected" => Ok(JournalRuntimeAuthorityReason::V2ProfileSelected),
        _ => invalid("session authority pin reason is invalid"),
    }
}

fn validate_request(
    request: &JournalRunAdmissionRequest,
    max_payload_bytes: usize,
) -> Result<(), JournalError> {
    validate_session_authority_intent(&request.session_authority_intent)?;
    if request.session_authority_intent.selected_runtime != JournalRuntimeAuthority::V2 {
        return invalid("V2 run admission requires a V2 session authority intent");
    }
    for (field, value) in [
        ("admission_id", request.admission_id.as_str()),
        ("idempotency_scope", request.idempotency_scope.as_str()),
        ("idempotency_key", request.idempotency_key.as_str()),
        ("trace_id", request.trace_id.as_str()),
        ("run_id", request.run_id.as_str()),
        ("initial_attempt_id", request.initial_attempt_id.as_str()),
        ("caller_principal", request.caller_principal.as_str()),
        ("caller_device_id", request.caller_device_id.as_str()),
    ] {
        validate_identifier(field, value)?;
    }
    if request.queue_input.is_some() == request.fresh_run_intent {
        return invalid("exactly one of queue_input or fresh_run_intent is required");
    }
    if !request.fresh_run_intent
        && matches!(request.policy.active_run_disposition, RunAdmissionDisposition::AdmitNow)
    {
        return invalid("active_run_disposition cannot be admit_now");
    }
    validate_canonical_digest(
        "access_policy",
        request.policy.access_policy_json.as_str(),
        request.policy.access_policy_sha256.as_str(),
        max_payload_bytes,
    )?;
    validate_canonical_digest(
        "queue_policy",
        request.policy.queue_policy_json.as_str(),
        request.policy.queue_policy_sha256.as_str(),
        max_payload_bytes,
    )?;
    let aggregate =
        format!("{}:{}", request.policy.access_policy_sha256, request.policy.queue_policy_sha256);
    if sha256_hex(aggregate.as_bytes()) != request.policy.policy_sha256 {
        return invalid("aggregate policy_sha256 mismatch");
    }
    validate_canonical_digest(
        "authority_hook_input",
        request.evidence_hook_input.authority_input_json.as_str(),
        request.evidence_hook_input.authority_input_sha256.as_str(),
        max_payload_bytes,
    )?;
    validate_canonical_digest(
        "kernel_hook_input",
        request.evidence_hook_input.kernel_input_json.as_str(),
        request.evidence_hook_input.kernel_input_sha256.as_str(),
        max_payload_bytes,
    )?;
    if let Some(queue) = request.queue_input.as_ref() {
        validate_identifier("queued_input_id", queue.queued_input_id.as_str())?;
        if queue.text.trim().is_empty() || queue.text.len() > MAX_ADMISSION_TEXT_BYTES {
            return invalid("queue text is empty or exceeds the admission bound");
        }
        validate_canonical_json("safe_boundary_flags", queue.safe_boundary_flags_json.as_str())?;
    }
    let request_sha256 = run_admission_request_sha256(request)?;
    if serde_json::to_vec(request)?.len() > max_payload_bytes
        || request_sha256 != request.request_sha256
    {
        return invalid("request_sha256 does not match the bounded canonical request");
    }
    Ok(())
}

fn validate_identifier(field: &str, value: &str) -> Result<(), JournalError> {
    if value.trim().is_empty() || value.len() > MAX_ADMISSION_ID_BYTES {
        return invalid(format!("{field} is empty or exceeds {MAX_ADMISSION_ID_BYTES} bytes"));
    }
    Ok(())
}

fn validate_canonical_json(field: &str, raw: &str) -> Result<String, JournalError> {
    let parsed: Value = serde_json::from_str(raw).map_err(|_| {
        JournalError::InvalidRunAdmission { reason: format!("{field} is not JSON") }
    })?;
    let canonical = serde_json::to_string(&canonicalize_json(parsed))?;
    if canonical != raw {
        return invalid(format!("{field} is not canonical JSON"));
    }
    Ok(canonical)
}

fn validate_canonical_digest(
    field: &str,
    raw: &str,
    expected: &str,
    max_payload_bytes: usize,
) -> Result<(), JournalError> {
    if raw.len() > max_payload_bytes {
        return Err(JournalError::PayloadTooLarge {
            payload_kind: "run_admission_evidence",
            actual_bytes: raw.len(),
            max_bytes: max_payload_bytes,
        });
    }
    let canonical = validate_canonical_json(field, raw)?;
    if sha256_hex(canonical.as_bytes()) != expected {
        return invalid(format!("{field} digest mismatch"));
    }
    Ok(())
}

fn canonicalize_json(value: Value) -> Value {
    match value {
        Value::Object(values) => {
            let mut keys = values.into_iter().collect::<Vec<_>>();
            keys.sort_by(|left, right| left.0.cmp(&right.0));
            Value::Object(
                keys.into_iter()
                    .map(|(key, value)| (key, canonicalize_json(value)))
                    .collect::<Map<_, _>>(),
            )
        }
        Value::Array(values) => Value::Array(values.into_iter().map(canonicalize_json).collect()),
        other => other,
    }
}

fn invalid<T>(reason: impl Into<String>) -> Result<T, JournalError> {
    Err(JournalError::InvalidRunAdmission { reason: reason.into() })
}

struct ResolvedSession {
    session: OrchestratorSessionRecord,
}

fn resolve_orchestrator_session_tx(
    connection: &Connection,
    request: &JournalRunAdmissionRequest,
    now: i64,
) -> Result<ResolvedSession, JournalError> {
    let selector = &request.session;
    let requested_id = selector.session_id.clone().and_then(normalize_optional_session_field);
    let requested_key = selector.session_key.clone().and_then(normalize_optional_session_field);
    let requested_label = selector.session_label.clone().and_then(normalize_optional_session_field);
    let by_id = requested_id
        .as_deref()
        .map(|value| load_orchestrator_session_by_id(connection, value))
        .transpose()?
        .flatten();
    let by_key = requested_key
        .as_deref()
        .map(|value| load_orchestrator_session_by_key(connection, value))
        .transpose()?
        .flatten();
    let existing = match (by_id, by_key) {
        (Some(left), Some(right)) if left.session_id != right.session_id => {
            return Err(JournalError::InvalidSessionSelector {
                reason: "session_id and session_key resolve to different sessions".to_owned(),
            });
        }
        (Some(session), Some(_)) | (Some(session), None) | (None, Some(session)) => Some(session),
        (None, None) if requested_id.is_none() && requested_key.is_none() => requested_label
            .as_deref()
            .map(|value| load_orchestrator_session_by_label(connection, value))
            .transpose()?
            .flatten(),
        (None, None) => None,
    };
    if let Some(mut session) = existing {
        if requested_id.as_deref().is_some_and(|value| value != session.session_id)
            || requested_key.as_deref().is_some_and(|value| value != session.session_key)
        {
            return Err(JournalError::InvalidSessionSelector {
                reason: "provided selectors do not identify the same session".to_owned(),
            });
        }
        if session.principal != request.caller_principal
            || session.device_id != request.caller_device_id
            || session.channel != request.caller_channel
        {
            return Err(JournalError::SessionIdentityMismatch { session_id: session.session_id });
        }
        connection.execute(
            r#"
                UPDATE orchestrator_sessions SET
                    updated_at_unix_ms = ?2,
                    session_label = COALESCE(?3, session_label),
                    last_run_ulid = CASE WHEN ?4 = 1 THEN NULL ELSE last_run_ulid END
                WHERE session_ulid = ?1
            "#,
            params![session.session_id, now, requested_label, i64::from(selector.reset_session),],
        )?;
        if selector.reset_session {
            connection.execute(
                "DELETE FROM session_project_context_state WHERE session_ulid = ?1",
                params![session.session_id],
            )?;
            session.last_run_id = None;
        }
        session.updated_at_unix_ms = now;
        if requested_label.is_some() {
            session.session_label = requested_label;
        }
        return Ok(ResolvedSession {
            session: hydrate_orchestrator_session_snapshot(connection, session, None)?,
        });
    }
    if selector.require_existing {
        return Err(JournalError::SessionNotFound {
            selector: requested_id
                .or(requested_key)
                .or(requested_label)
                .unwrap_or_else(|| "<unspecified>".to_owned()),
        });
    }
    let session_id = requested_id.unwrap_or_else(|| Ulid::new().to_string());
    let session_key = requested_key.unwrap_or_else(|| session_id.clone());
    connection.execute(
        r#"
            INSERT INTO orchestrator_sessions (
                session_ulid, session_key, session_label, principal, device_id,
                channel, created_at_unix_ms, updated_at_unix_ms, last_run_ulid,
                title_generation_state, manual_title_locked,
                manual_title_updated_at_unix_ms, branch_state
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7, NULL, ?8, ?9, ?10, 'root')
        "#,
        params![
            session_id,
            session_key,
            requested_label,
            request.caller_principal,
            request.caller_device_id,
            request.caller_channel,
            now,
            if requested_label.is_some() {
                ORCHESTRATOR_TITLE_GENERATION_STATE_MANUAL_LOCKED
            } else {
                ORCHESTRATOR_TITLE_GENERATION_STATE_IDLE
            },
            i64::from(requested_label.is_some()),
            requested_label.as_ref().map(|_| now),
        ],
    )?;
    let session = load_orchestrator_session_by_id(connection, session_id.as_str())?
        .ok_or_else(|| JournalError::SessionNotFound { selector: session_id })?;
    Ok(ResolvedSession {
        session: hydrate_orchestrator_session_snapshot(connection, session, None)?,
    })
}

fn pending_queue_depth_tx(connection: &Connection, session_id: &str) -> Result<u64, JournalError> {
    let count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM orchestrator_queued_inputs WHERE session_ulid = ?1 AND state = 'pending'",
        params![session_id],
        |row| row.get(0),
    )?;
    Ok(count.max(0) as u64)
}

fn decide_disposition(
    request: &JournalRunAdmissionRequest,
    active_run_id: Option<&str>,
    pending_depth: u64,
) -> Result<(RunAdmissionDisposition, String), JournalError> {
    if let Some(reason) = request.policy.forced_rejection_reason.as_ref() {
        return Ok((RunAdmissionDisposition::Reject, reason.clone()));
    }
    match (active_run_id, request.queue_input.as_ref(), request.fresh_run_intent) {
        (None, None, true) => {
            Ok((RunAdmissionDisposition::AdmitNow, "run_admission.admit_now".to_owned()))
        }
        (Some(_), Some(_), false) => {
            if pending_depth >= request.policy.max_pending_queue_depth {
                return Err(JournalError::RunAdmissionQueueCapacityExceeded {
                    session_id: request
                        .session
                        .session_id
                        .clone()
                        .unwrap_or_else(|| "<resolved>".to_owned()),
                    pending_depth,
                    maximum_depth: request.policy.max_pending_queue_depth,
                });
            }
            let disposition = request.policy.active_run_disposition;
            if !matches!(
                disposition,
                RunAdmissionDisposition::DurableQueue
                    | RunAdmissionDisposition::Merge
                    | RunAdmissionDisposition::SteerCandidate
            ) {
                return invalid("active_run_disposition must be queue, merge, or steer_candidate");
            }
            Ok((disposition, format!("run_admission.{}", disposition.as_str())))
        }
        (Some(_), None, true) => {
            Ok((RunAdmissionDisposition::Reject, "run_admission.active_run_conflict".to_owned()))
        }
        (None, Some(_), false) => Ok((
            RunAdmissionDisposition::Reject,
            "run_admission.no_active_run_for_queue".to_owned(),
        )),
        _ => invalid("admission intent is inconsistent"),
    }
}

fn insert_run_tx(
    connection: &Connection,
    request: &JournalRunAdmissionRequest,
    start: &OrchestratorRunStartRequest,
    now: i64,
) -> Result<(), JournalError> {
    connection
        .execute(
            r#"
            INSERT INTO orchestrator_runs (
                run_ulid, session_ulid, state, cancel_requested, cancel_reason,
                created_at_unix_ms, started_at_unix_ms, completed_at_unix_ms,
                updated_at_unix_ms, prompt_tokens, completion_tokens, total_tokens,
                last_error, origin_kind, origin_run_ulid, parent_run_ulid,
                triggered_by_principal, parameter_delta_json
            ) VALUES (?1, ?2, ?3, 0, NULL, ?4, ?4, NULL, ?4, 0, 0, 0, NULL, ?5, ?6, ?6, ?7, NULL)
        "#,
            params![
                start.run_id,
                start.session_id,
                RunLifecycleState::Accepted.as_str(),
                now,
                start.origin_kind,
                start.origin_run_id,
                start.triggered_by_principal,
            ],
        )
        .map_err(|error| {
            if error.sqlite_error_code() == Some(rusqlite::ErrorCode::ConstraintViolation) {
                JournalError::DuplicateRunId { run_id: request.run_id.clone() }
            } else {
                error.into()
            }
        })?;
    connection.execute(
        "UPDATE orchestrator_sessions SET last_run_ulid = ?2, updated_at_unix_ms = ?3 WHERE session_ulid = ?1",
        params![start.session_id, start.run_id, now],
    )?;
    append_run_lifecycle_event_tx(
        connection,
        &RunLifecycleEventAppendRequest {
            event_id: Ulid::new().to_string(),
            run_id: start.run_id.clone(),
            session_id: start.session_id.clone(),
            from_state: None,
            to_state: palyra_common::runtime_contracts::RunLifecyclePhase::Queued,
            actor: RuntimeActorRef {
                kind: RuntimeActorKind::Principal,
                id: request.caller_principal.clone(),
            },
            correlation_id: request.trace_id.clone(),
            parent_run_id: request.origin_run_id.clone(),
            idempotency_key: Some(format!("run:admission:{}", request.admission_id)),
            reason: "run.admission.accepted".to_owned(),
            payload_json: "{}".to_owned(),
        },
        now,
    )
}

fn reserve_initial_attempt_tx(
    connection: &Connection,
    request: &JournalRunAdmissionRequest,
    session: &OrchestratorSessionRecord,
    run_lease: &GenerationLeaseV1,
    now: i64,
) -> Result<(), JournalError> {
    connection.execute(
        r#"
            INSERT INTO runtime_run_initial_attempt_reservations (
                attempt_ulid, admission_ulid, session_ulid, run_ulid,
                run_generation, run_lease_ulid, state, reserved_at_unix_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'reserved', ?7)
        "#,
        params![
            request.initial_attempt_id,
            request.admission_id,
            session.session_id,
            request.run_id,
            generation_i64(run_lease.generation)?,
            run_lease.lease_id.as_str(),
            now,
        ],
    )?;
    Ok(())
}

fn bind_writer_lease_tx(
    connection: &Connection,
    admission_id: &str,
    writer_lease: &super::SessionWriteLeaseRecord,
    run_id: &str,
    run_lease: &GenerationLeaseV1,
    now: i64,
) -> Result<(), JournalError> {
    connection.execute(
        r#"
            INSERT INTO runtime_session_write_lease_bindings (
                admission_ulid, session_ulid, writer_lease_ulid, run_ulid,
                run_generation, run_lease_ulid, bound_at_unix_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
        "#,
        params![
            admission_id,
            writer_lease.session_id,
            writer_lease.lease_id,
            run_id,
            generation_i64(run_lease.generation)?,
            run_lease.lease_id.as_str(),
            now,
        ],
    )?;
    Ok(())
}

struct QueuedInputInsert<'a> {
    request: &'a JournalRunAdmissionRequest,
    input: &'a JournalRunAdmissionQueueInput,
    session_id: &'a str,
    active_run_id: &'a str,
    disposition: RunAdmissionDisposition,
    reason: &'a str,
    now: i64,
}

fn insert_queued_input_tx(
    connection: &Transaction<'_>,
    insert: QueuedInputInsert<'_>,
) -> Result<(), JournalError> {
    let QueuedInputInsert { request, input, session_id, active_run_id, disposition, reason, now } =
        insert;
    let state = disposition.queue_state();
    let terminal = (state != "pending").then_some(now);
    connection.execute(
        r#"
            INSERT INTO orchestrator_queued_inputs (
                queued_input_ulid, run_ulid, session_ulid, state, text,
                origin_run_ulid, created_at_unix_ms, updated_at_unix_ms,
                queue_mode, priority_lane, safe_boundary_flags_json,
                decision_reason, accepted_at_unix_ms, coalesced_at_unix_ms,
                terminal_at_unix_ms, policy_snapshot_json, explain_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7, ?8, 'normal', ?9, ?10, ?7, ?11, ?11, ?12, ?13)
        "#,
        params![
            input.queued_input_id,
            active_run_id,
            session_id,
            state,
            input.text,
            request.origin_run_id,
            now,
            input.requested_mode,
            input.safe_boundary_flags_json,
            reason,
            terminal,
            request.policy.queue_policy_json,
            serde_json::to_string(&serde_json::json!({
                "admission_id": request.admission_id,
                "policy_channel": input.policy_channel,
                "policy_agent": input.policy_agent,
                "disposition": disposition.as_str(),
            }))?,
        ],
    )?;
    if state == "pending" {
        super::objective_guards::objective_guard_reset_for_session_tx(
            connection,
            session_id,
            "objective.guard.user_correction_reset",
            now,
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn insert_admission_tx(
    connection: &Connection,
    request: &JournalRunAdmissionRequest,
    session_id: &str,
    disposition: RunAdmissionDisposition,
    reason: &str,
    allocated_run_id: Option<&str>,
    target_active_run_id: Option<&str>,
    queued_input_id: Option<&str>,
    initial_attempt_id: Option<&str>,
    run_lease: Option<&GenerationLeaseV1>,
    evidence: Option<&JournalRunAdmissionPersistedEvidence>,
    _reserved: Option<&str>,
    now: i64,
) -> Result<(), JournalError> {
    let caller_binding_sha256 = sha256_hex(
        format!(
            "{}\u{0}{}\u{0}{}",
            request.caller_principal,
            request.caller_device_id,
            request.caller_channel.as_deref().unwrap_or("")
        )
        .as_bytes(),
    );
    connection.execute(
        r#"
            INSERT INTO runtime_run_admissions (
                admission_ulid, idempotency_scope, idempotency_key, request_sha256,
                trace_ulid, disposition, reason_code, origin_kind, origin_run_ulid,
                delegated_admission_json, session_ulid, allocated_run_ulid,
                target_active_run_ulid, queued_input_ulid, initial_attempt_ulid,
                allocated_run_generation, run_lease_ulid, caller_binding_sha256,
                access_policy_sha256, queue_policy_sha256, policy_sha256,
                authority_decision_json, authority_decision_sha256,
                admission_snapshot_json, admission_snapshot_sha256,
                kernel_head_sha256, created_at_unix_ms
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13,
                ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25,
                ?26, ?27
            )
        "#,
        params![
            request.admission_id,
            request.idempotency_scope,
            request.idempotency_key,
            request.request_sha256,
            request.trace_id,
            disposition.as_str(),
            reason,
            request.origin_kind.as_str(),
            request.origin_run_id,
            request.delegated_admission_json,
            session_id,
            allocated_run_id,
            target_active_run_id,
            queued_input_id,
            initial_attempt_id,
            run_lease.map(|lease| generation_i64(lease.generation)).transpose()?,
            run_lease.map(|lease| lease.lease_id.as_str()),
            caller_binding_sha256,
            request.policy.access_policy_sha256,
            request.policy.queue_policy_sha256,
            request.policy.policy_sha256,
            evidence.map(|value| value.authority_decision_json.as_str()),
            evidence.map(|value| value.authority_decision_sha256.as_str()),
            evidence.map(|value| value.admission_snapshot_json.as_str()),
            evidence.map(|value| value.admission_snapshot_sha256.as_str()),
            evidence.map(|value| value.kernel_head_sha256.as_str()),
            now,
        ],
    )?;
    Ok(())
}

fn validate_persisted_evidence(
    evidence: &JournalRunAdmissionPersistedEvidence,
) -> Result<(), JournalError> {
    for (field, json, digest) in [
        (
            "authority_decision",
            evidence.authority_decision_json.as_str(),
            evidence.authority_decision_sha256.as_str(),
        ),
        (
            "admission_snapshot",
            evidence.admission_snapshot_json.as_str(),
            evidence.admission_snapshot_sha256.as_str(),
        ),
    ] {
        let canonical = validate_canonical_json(field, json).map_err(|_| {
            JournalError::InvalidRunAdmissionEvidence {
                reason: format!("{field} is not canonical JSON"),
            }
        })?;
        if sha256_hex(canonical.as_bytes()) != digest {
            return Err(JournalError::InvalidRunAdmissionEvidence {
                reason: format!("{field} digest mismatch"),
            });
        }
    }
    if evidence.kernel_head_sha256.len() != 64
        || !evidence.kernel_head_sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(JournalError::InvalidRunAdmissionEvidence {
            reason: "kernel head digest must be SHA-256 hex".to_owned(),
        });
    }
    Ok(())
}

fn load_admission_tx(
    connection: &Connection,
    scope: &str,
    key: &str,
) -> Result<Option<StoredAdmission>, JournalError> {
    connection
        .query_row(
            r#"
                SELECT request_sha256, session_ulid, disposition, reason_code,
                    allocated_run_ulid, target_active_run_ulid, queued_input_ulid,
                    initial_attempt_ulid, allocated_run_generation, run_lease_ulid,
                    authority_decision_sha256, admission_snapshot_sha256,
                    kernel_head_sha256,
                    (
                        SELECT pin_revision
                        FROM runtime_run_admission_pin_bindings bindings
                        WHERE bindings.admission_ulid = admissions.admission_ulid
                    )
                FROM runtime_run_admissions admissions
                WHERE idempotency_scope = ?1 AND idempotency_key = ?2
            "#,
            params![scope, key],
            |row| {
                let raw: String = row.get(2)?;
                Ok(StoredAdmission {
                    request_sha256: row.get(0)?,
                    session_id: row.get(1)?,
                    disposition: parse_disposition(raw.as_str()).map_err(|reason| {
                        rusqlite::Error::FromSqlConversionFailure(
                            2,
                            rusqlite::types::Type::Text,
                            Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, reason)),
                        )
                    })?,
                    reason_code: row.get(3)?,
                    allocated_run_id: row.get(4)?,
                    #[cfg(test)]
                    target_active_run_id: row.get(5)?,
                    #[cfg(test)]
                    queued_input_id: row.get(6)?,
                    initial_attempt_id: row.get(7)?,
                    generation: row
                        .get::<_, Option<i64>>(8)?
                        .map(|value| RuntimeGeneration::new(value.max(0) as u64))
                        .transpose()
                        .map_err(|error| {
                            rusqlite::Error::FromSqlConversionFailure(
                                8,
                                rusqlite::types::Type::Integer,
                                Box::new(error),
                            )
                        })?,
                    run_lease_id: row.get(9)?,
                    authority_sha256: row.get(10)?,
                    snapshot_sha256: row.get(11)?,
                    kernel_head_sha256: row.get(12)?,
                    session_authority_pin_revision: row
                        .get::<_, Option<i64>>(13)?
                        .map(u64::try_from)
                        .transpose()
                        .map_err(|error| {
                            rusqlite::Error::FromSqlConversionFailure(
                                13,
                                rusqlite::types::Type::Integer,
                                Box::new(error),
                            )
                        })?,
                })
            },
        )
        .optional()
        .map_err(JournalError::from)
}

fn hydrate_outcome_tx(
    connection: &Connection,
    stored: StoredAdmission,
    now: i64,
    _replayed: bool,
) -> Result<JournalRunAdmissionOutcome, JournalError> {
    let session = load_orchestrator_session_by_id(connection, stored.session_id.as_str())?
        .ok_or_else(|| JournalError::SessionNotFound { selector: stored.session_id.clone() })?;
    let run_lease = match (
        stored.allocated_run_id.as_deref(),
        stored.generation,
        stored.run_lease_id.as_deref(),
    ) {
        (Some(run_id), Some(generation), Some(lease_id)) => {
            let active = shared_runtime::active_runtime_generation_tx(
                connection,
                stored.session_id.as_str(),
                run_id,
                RuntimeGenerationLane::Run,
                now,
            )?;
            active.filter(|lease| {
                lease.generation == generation && lease.lease_id.as_str() == lease_id
            })
        }
        _ => None,
    };
    let session_authority_pin = stored
        .session_authority_pin_revision
        .map(|revision| {
            load_session_authority_pin_revision_tx(
                connection,
                stored.session_id.as_str(),
                revision,
            )?
            .ok_or_else(|| JournalError::InvalidRunAdmission {
                reason: "admission references a missing session authority pin".to_owned(),
            })
        })
        .transpose()?;
    Ok(JournalRunAdmissionOutcome {
        session: hydrate_orchestrator_session_snapshot(connection, session, None)?,
        disposition: stored.disposition,
        reason_code: stored.reason_code,
        #[cfg(test)]
        target_active_run_id: stored.target_active_run_id,
        #[cfg(test)]
        queued_input_id: stored.queued_input_id,
        allocated_run_id: stored.allocated_run_id,
        run_lease,
        initial_attempt_id: stored.initial_attempt_id,
        authority_decision_sha256: stored.authority_sha256,
        admission_snapshot_sha256: stored.snapshot_sha256,
        kernel_head_sha256: stored.kernel_head_sha256,
        session_authority_pin,
        #[cfg(test)]
        replayed: _replayed,
    })
}

#[allow(clippy::too_many_arguments)]
fn outcome_from_parts(
    session: OrchestratorSessionRecord,
    disposition: RunAdmissionDisposition,
    reason_code: String,
    _target_active_run_id: Option<String>,
    _queued_input_id: Option<String>,
    allocated_run_id: Option<String>,
    run_lease: Option<GenerationLeaseV1>,
    initial_attempt_id: Option<String>,
    evidence: Option<JournalRunAdmissionPersistedEvidence>,
    session_authority_pin: Option<JournalSessionAuthorityPin>,
    _replayed: bool,
) -> JournalRunAdmissionOutcome {
    JournalRunAdmissionOutcome {
        session,
        disposition,
        reason_code,
        #[cfg(test)]
        target_active_run_id: _target_active_run_id,
        #[cfg(test)]
        queued_input_id: _queued_input_id,
        allocated_run_id,
        run_lease,
        initial_attempt_id,
        authority_decision_sha256: evidence
            .as_ref()
            .map(|value| value.authority_decision_sha256.clone()),
        admission_snapshot_sha256: evidence
            .as_ref()
            .map(|value| value.admission_snapshot_sha256.clone()),
        kernel_head_sha256: evidence.map(|value| value.kernel_head_sha256),
        session_authority_pin,
        #[cfg(test)]
        replayed: _replayed,
    }
}

fn parse_disposition(value: &str) -> Result<RunAdmissionDisposition, String> {
    match value {
        "reject" => Ok(RunAdmissionDisposition::Reject),
        "durable_queue" => Ok(RunAdmissionDisposition::DurableQueue),
        "merge" => Ok(RunAdmissionDisposition::Merge),
        "steer_candidate" => Ok(RunAdmissionDisposition::SteerCandidate),
        "admit_now" => Ok(RunAdmissionDisposition::AdmitNow),
        other => Err(format!("unknown admission disposition {other}")),
    }
}

fn generation_i64(generation: RuntimeGeneration) -> Result<i64, JournalError> {
    i64::try_from(generation.get()).map_err(|_| JournalError::InvalidRunAdmission {
        reason: "Run generation exceeds SQLite integer range".to_owned(),
    })
}
