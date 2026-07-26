//! Durable generation-fenced remediation for stale read-only runs.

use palyra_common::runtime_contracts::{AuxiliaryTaskKind, AuxiliaryTaskState, RuntimeEventName};
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use ulid::Ulid;

use super::{
    current_unix_ms, load_resume_tape_observations, next_orchestrator_tape_seq,
    summarize_resume_tape_observations, JournalError, JournalStore,
};

pub(super) const MIGRATION_83_SQL: &str = r#"
    CREATE TABLE IF NOT EXISTS stuck_run_incidents_v2 (
        incident_ulid TEXT PRIMARY KEY,
        run_ulid TEXT NOT NULL,
        session_ulid TEXT NOT NULL,
        generation INTEGER NOT NULL CHECK (generation > 0),
        heartbeat_updated_at_unix_ms INTEGER NOT NULL,
        requeue_idempotency_key TEXT NOT NULL UNIQUE,
        incident_json TEXT NOT NULL,
        incident_sha256 TEXT NOT NULL,
        created_at_unix_ms INTEGER NOT NULL,
        schema_version INTEGER NOT NULL CHECK (schema_version = 2),
        FOREIGN KEY(run_ulid) REFERENCES orchestrator_runs(run_ulid),
        FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid)
    );
    CREATE INDEX IF NOT EXISTS idx_stuck_run_incidents_run
        ON stuck_run_incidents_v2(run_ulid, created_at_unix_ms DESC);

    CREATE TABLE IF NOT EXISTS stuck_run_remediation_claims (
        incident_ulid TEXT PRIMARY KEY,
        worker_id_sha256 TEXT NOT NULL,
        claim_epoch INTEGER NOT NULL CHECK (claim_epoch > 0),
        state TEXT NOT NULL CHECK (state IN ('claimed', 'completed', 'failed')),
        claim_expires_at_unix_ms INTEGER NOT NULL,
        continuation_task_ulid TEXT,
        continuation_run_ulid TEXT,
        updated_at_unix_ms INTEGER NOT NULL,
        FOREIGN KEY(incident_ulid) REFERENCES stuck_run_incidents_v2(incident_ulid)
    );

    CREATE TABLE IF NOT EXISTS stuck_run_remediation_events (
        event_ulid TEXT PRIMARY KEY,
        incident_ulid TEXT NOT NULL,
        event_type TEXT NOT NULL,
        reason_code TEXT NOT NULL,
        decision_json TEXT NOT NULL,
        decision_sha256 TEXT NOT NULL,
        worker_id_sha256 TEXT,
        claim_epoch INTEGER,
        created_at_unix_ms INTEGER NOT NULL,
        schema_version INTEGER NOT NULL CHECK (schema_version = 1),
        FOREIGN KEY(incident_ulid) REFERENCES stuck_run_incidents_v2(incident_ulid)
    );
    CREATE INDEX IF NOT EXISTS idx_stuck_run_remediation_events_incident
        ON stuck_run_remediation_events(incident_ulid, created_at_unix_ms ASC);

    CREATE TRIGGER IF NOT EXISTS trg_stuck_run_incidents_prevent_update
    BEFORE UPDATE ON stuck_run_incidents_v2 BEGIN
        SELECT RAISE(ABORT, 'stuck_run_incidents_v2 is immutable');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_stuck_run_incidents_prevent_delete
    BEFORE DELETE ON stuck_run_incidents_v2 BEGIN
        SELECT RAISE(ABORT, 'stuck_run_incidents_v2 is immutable');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_stuck_run_remediation_events_prevent_update
    BEFORE UPDATE ON stuck_run_remediation_events BEGIN
        SELECT RAISE(ABORT, 'stuck_run_remediation_events is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_stuck_run_remediation_events_prevent_delete
    BEFORE DELETE ON stuck_run_remediation_events BEGIN
        SELECT RAISE(ABORT, 'stuck_run_remediation_events is append-only');
    END;
"#;

const CONTINUATION_BUDGET_TOKENS: i64 = 4_096;
const CONTINUATION_INSTRUCTION: &str = concat!(
    "Continue after generation-safe recovery of an interrupted read-only wait. ",
    "Reconstruct from durable tape and do not replay unresolved mutations."
);

/// Operator policy for stale-run remediation.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StuckRunRemediationPolicy {
    ObserveOnly,
    GenerationSafeAutoRecovery,
}

/// Stable reasoned outcome of the pre-mutation remediation decision.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StuckRunRemediationDecisionKind {
    AutoRecover,
    ObserveOnly,
    FreshHeartbeat,
    StaleGeneration,
    ExpiredGenerationLease,
    ActiveMutationBlocked,
    ApprovalBlocked,
    UnsafeWaitState,
    LaneOwnerMismatch,
    RateLimited,
    CircuitOpen,
}

impl StuckRunRemediationDecisionKind {
    #[must_use]
    pub(crate) const fn reason_code(self) -> &'static str {
        match self {
            Self::AutoRecover => "runtime.healing.stuck_run.auto_recover",
            Self::ObserveOnly => "runtime.healing.stuck_run.observe_only",
            Self::FreshHeartbeat => "runtime.healing.stuck_run.fresh_heartbeat",
            Self::StaleGeneration => "runtime.healing.stuck_run.stale_generation",
            Self::ExpiredGenerationLease => "runtime.healing.stuck_run.expired_generation_lease",
            Self::ActiveMutationBlocked => "runtime.healing.stuck_run.active_mutation",
            Self::ApprovalBlocked => "runtime.healing.stuck_run.pending_approval",
            Self::UnsafeWaitState => "runtime.healing.stuck_run.unsafe_wait_state",
            Self::LaneOwnerMismatch => "runtime.healing.stuck_run.lane_owner_mismatch",
            Self::RateLimited => "runtime.healing.stuck_run.rate_limited",
            Self::CircuitOpen => "runtime.healing.stuck_run.circuit_open",
        }
    }
}

/// Immutable evidence captured when a stale run is first classified.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StuckRunIncidentV2 {
    pub incident_id: String,
    pub run_id: String,
    pub session_id: String,
    pub generation: u64,
    pub generation_lease_id: String,
    pub generation_lease_expires_at_unix_ms: i64,
    pub lane_owner: String,
    pub heartbeat_generation: Option<u64>,
    pub heartbeat_updated_at_unix_ms: i64,
    pub provider_wait_in_flight: bool,
    pub read_only_tool_wait: bool,
    pub mutating_tool_in_flight: bool,
    pub pending_approval: bool,
    pub requeue_idempotency_key: String,
    pub continuation_task_id: String,
    pub continuation_run_id: String,
    pub created_at_unix_ms: i64,
    pub schema_version: u32,
}

/// Replay-visible decision made from an immutable incident.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemediationDecision {
    pub incident_id: String,
    pub policy: StuckRunRemediationPolicy,
    pub decision: StuckRunRemediationDecisionKind,
    pub reason_code: String,
    pub decided_at_unix_ms: i64,
    pub schema_version: u32,
}

impl RemediationDecision {
    #[must_use]
    pub(crate) fn new(
        incident_id: impl Into<String>,
        policy: StuckRunRemediationPolicy,
        decision: StuckRunRemediationDecisionKind,
        decided_at_unix_ms: i64,
    ) -> Self {
        Self {
            incident_id: incident_id.into(),
            policy,
            decision,
            reason_code: decision.reason_code().to_owned(),
            decided_at_unix_ms,
            schema_version: 1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StuckRunRemediationClaimOutcome {
    Claimed { claim_epoch: u64 },
    Busy,
    AlreadyCompleted,
    StaleAuthority,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StuckRunRemediationCompletionOutcome {
    ContinuationQueued { task_id: String, run_id: String },
    AlreadyQueued { task_id: String, run_id: String },
    StaleClaim,
}

#[derive(Debug)]
struct RunAuthority {
    session_id: String,
    generation: u64,
    lease_id: String,
    lease_expires_at_unix_ms: i64,
    owner: String,
}

impl JournalStore {
    /// Captures immutable safety evidence for a run while binding it to the active run lane.
    pub fn inspect_stuck_run_incident(
        &self,
        run_id: &str,
        heartbeat_generation: Option<u64>,
        heartbeat_updated_at_unix_ms: i64,
    ) -> Result<Option<StuckRunIncidentV2>, JournalError> {
        let now = current_unix_ms()?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let Some(authority) = load_run_authority(&guard, run_id)? else {
            return Ok(None);
        };
        let signals =
            summarize_resume_tape_observations(&load_resume_tape_observations(&guard, run_id)?);
        let pending_approval = signals.pending_approval
            || guard.query_row(
                "SELECT EXISTS(SELECT 1 FROM approvals WHERE run_ulid = ?1 AND decision IS NULL)",
                params![run_id],
                |row| row.get::<_, bool>(0),
            )?;
        let provider_wait_in_flight = provider_wait_in_flight(&guard, run_id)?;
        let incident_id = stable_id(
            "stuck-run",
            format!("{run_id}:{}:{heartbeat_updated_at_unix_ms}", authority.generation).as_str(),
        );
        if let Some(existing) = load_incident(&guard, incident_id.as_str())? {
            return Ok(Some(existing));
        }
        let incident = StuckRunIncidentV2 {
            incident_id,
            run_id: run_id.to_owned(),
            session_id: authority.session_id,
            generation: authority.generation,
            generation_lease_id: authority.lease_id,
            generation_lease_expires_at_unix_ms: authority.lease_expires_at_unix_ms,
            lane_owner: authority.owner,
            heartbeat_generation,
            heartbeat_updated_at_unix_ms,
            provider_wait_in_flight,
            read_only_tool_wait: signals.read_only_tool_wait,
            mutating_tool_in_flight: signals.mutating_tool_in_flight,
            pending_approval,
            requeue_idempotency_key: stable_id(
                "stuck-run-requeue",
                format!("{run_id}:{}", authority.generation).as_str(),
            ),
            continuation_task_id: Ulid::new().to_string(),
            continuation_run_id: Ulid::new().to_string(),
            created_at_unix_ms: now,
            schema_version: 2,
        };
        let incident_json = serde_json::to_string(&incident)?;
        guard.execute(
            r#"
                INSERT INTO stuck_run_incidents_v2 (
                    incident_ulid, run_ulid, session_ulid, generation,
                    heartbeat_updated_at_unix_ms, requeue_idempotency_key,
                    incident_json, incident_sha256, created_at_unix_ms, schema_version
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 2)
            "#,
            params![
                incident.incident_id,
                incident.run_id,
                incident.session_id,
                sqlite_generation(incident.generation)?,
                incident.heartbeat_updated_at_unix_ms,
                incident.requeue_idempotency_key,
                incident_json,
                sha256_hex(incident_json.as_bytes()),
                incident.created_at_unix_ms,
            ],
        )?;
        Ok(Some(incident))
    }

    /// Appends a redacted, stable decision for diagnostics and replay.
    pub fn record_stuck_run_remediation_decision(
        &self,
        decision: &RemediationDecision,
    ) -> Result<(), JournalError> {
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        append_decision_event(&transaction, decision, "decision", None, None)?;
        transaction.commit()?;
        Ok(())
    }

    /// Claims remediation only while the exact run generation, lease, lane owner, and safety
    /// posture still match the immutable incident.
    pub fn claim_stuck_run_remediation(
        &self,
        incident: &StuckRunIncidentV2,
        worker_id: &str,
        claim_ttl_ms: i64,
    ) -> Result<StuckRunRemediationClaimOutcome, JournalError> {
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(authority) = load_run_authority(&transaction, incident.run_id.as_str())? else {
            transaction.commit()?;
            return Ok(StuckRunRemediationClaimOutcome::StaleAuthority);
        };
        let signals = summarize_resume_tape_observations(&load_resume_tape_observations(
            &transaction,
            incident.run_id.as_str(),
        )?);
        let pending_approval = signals.pending_approval
            || transaction.query_row(
                "SELECT EXISTS(SELECT 1 FROM approvals WHERE run_ulid = ?1 AND decision IS NULL)",
                params![incident.run_id],
                |row| row.get::<_, bool>(0),
            )?;
        let provider_wait_in_flight =
            provider_wait_in_flight(&transaction, incident.run_id.as_str())?;
        if authority.session_id != incident.session_id
            || authority.generation != incident.generation
            || authority.lease_id != incident.generation_lease_id
            || authority.lease_expires_at_unix_ms != incident.generation_lease_expires_at_unix_ms
            || authority.lease_expires_at_unix_ms <= now
            || authority.owner != incident.lane_owner
            || signals.mutating_tool_in_flight
            || !(signals.read_only_tool_wait || provider_wait_in_flight)
            || pending_approval
        {
            transaction.commit()?;
            return Ok(StuckRunRemediationClaimOutcome::StaleAuthority);
        }
        let existing = transaction
            .query_row(
                r#"
                    SELECT worker_id_sha256, claim_epoch, state, claim_expires_at_unix_ms,
                           continuation_task_ulid, continuation_run_ulid
                    FROM stuck_run_remediation_claims
                    WHERE incident_ulid = ?1
                "#,
                params![incident.incident_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                    ))
                },
            )
            .optional()?;
        if let Some((_worker, _epoch, state, expires_at, task_id, run_id)) = existing.as_ref() {
            if state == "completed" {
                transaction.commit()?;
                debug_assert!(task_id.is_some() && run_id.is_some());
                return Ok(StuckRunRemediationClaimOutcome::AlreadyCompleted);
            }
            if *expires_at > now {
                transaction.commit()?;
                return Ok(StuckRunRemediationClaimOutcome::Busy);
            }
        }
        let next_epoch = existing.as_ref().map_or(1_i64, |row| row.1.saturating_add(1));
        let worker_id_sha256 = sha256_hex(worker_id.as_bytes());
        transaction.execute(
            r#"
                INSERT INTO stuck_run_remediation_claims (
                    incident_ulid, worker_id_sha256, claim_epoch, state,
                    claim_expires_at_unix_ms, continuation_task_ulid,
                    continuation_run_ulid, updated_at_unix_ms
                ) VALUES (?1, ?2, ?3, 'claimed', ?4, NULL, NULL, ?5)
                ON CONFLICT(incident_ulid) DO UPDATE SET
                    worker_id_sha256 = excluded.worker_id_sha256,
                    claim_epoch = excluded.claim_epoch,
                    state = 'claimed',
                    claim_expires_at_unix_ms = excluded.claim_expires_at_unix_ms,
                    updated_at_unix_ms = excluded.updated_at_unix_ms
            "#,
            params![
                incident.incident_id,
                worker_id_sha256,
                next_epoch,
                now.saturating_add(claim_ttl_ms.max(1)),
                now,
            ],
        )?;
        let decision = RemediationDecision::new(
            incident.incident_id.clone(),
            StuckRunRemediationPolicy::GenerationSafeAutoRecovery,
            StuckRunRemediationDecisionKind::AutoRecover,
            now,
        );
        append_decision_event(
            &transaction,
            &decision,
            "claimed",
            Some(worker_id_sha256.as_str()),
            Some(next_epoch),
        )?;
        transaction.commit()?;
        Ok(StuckRunRemediationClaimOutcome::Claimed {
            claim_epoch: u64::try_from(next_epoch).map_err(|_| {
                JournalError::InvalidArgument("remediation claim epoch is invalid".to_owned())
            })?,
        })
    }

    /// Atomically queues one continuation for a successfully fenced remediation claim.
    pub fn complete_stuck_run_remediation(
        &self,
        incident: &StuckRunIncidentV2,
        worker_id: &str,
        claim_epoch: u64,
    ) -> Result<StuckRunRemediationCompletionOutcome, JournalError> {
        let now = current_unix_ms()?;
        let worker_id_sha256 = sha256_hex(worker_id.as_bytes());
        let claim_epoch = sqlite_generation(claim_epoch)?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let claim = transaction
            .query_row(
                r#"
                    SELECT worker_id_sha256, claim_epoch, state,
                           continuation_task_ulid, continuation_run_ulid
                    FROM stuck_run_remediation_claims
                    WHERE incident_ulid = ?1
                "#,
                params![incident.incident_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                    ))
                },
            )
            .optional()?;
        let Some((owner, epoch, state, task_id, run_id)) = claim else {
            transaction.commit()?;
            return Ok(StuckRunRemediationCompletionOutcome::StaleClaim);
        };
        if state == "completed" {
            transaction.commit()?;
            return Ok(StuckRunRemediationCompletionOutcome::AlreadyQueued {
                task_id: task_id.unwrap_or_else(|| incident.continuation_task_id.clone()),
                run_id: run_id.unwrap_or_else(|| incident.continuation_run_id.clone()),
            });
        }
        if owner != worker_id_sha256 || epoch != claim_epoch || state != "claimed" {
            transaction.commit()?;
            return Ok(StuckRunRemediationCompletionOutcome::StaleClaim);
        }
        transaction.execute(
            r#"
                INSERT INTO orchestrator_background_tasks (
                    task_ulid, task_kind, session_ulid, child_session_ulid,
                    parent_run_ulid, target_run_ulid, planned_child_run_ulid,
                    queued_input_ulid, owner_principal, device_id, channel,
                    state, priority, revision, execution_generation,
                    attempt_count, max_attempts, budget_tokens, delegation_json,
                    cancellation_context_json, not_before_unix_ms,
                    expires_at_unix_ms, notification_target_json, input_text,
                    payload_json, last_error, result_json, created_at_unix_ms,
                    updated_at_unix_ms, started_at_unix_ms, completed_at_unix_ms
                )
                SELECT
                    ?1, ?2, incident.session_ulid, NULL, incident.run_ulid, NULL,
                    ?3, NULL, session.principal, session.device_id, session.channel,
                    ?4, 100, 0, 0, 0, 1, ?5, NULL, NULL, NULL, NULL, NULL,
                    ?6, ?7, NULL, NULL, ?8, ?8, NULL, NULL
                FROM stuck_run_incidents_v2 incident
                JOIN orchestrator_sessions session
                  ON session.session_ulid = incident.session_ulid
                WHERE incident.incident_ulid = ?9
            "#,
            params![
                incident.continuation_task_id,
                AuxiliaryTaskKind::BackgroundPrompt.as_str(),
                incident.continuation_run_id,
                AuxiliaryTaskState::Queued.as_str(),
                CONTINUATION_BUDGET_TOKENS,
                CONTINUATION_INSTRUCTION,
                json!({
                    "schema_version": 1,
                    "entry_point": "stuck_run_remediation",
                    "recovered_from_run_id": incident.run_id,
                    "original_generation": incident.generation,
                    "requeue_idempotency_key": incident.requeue_idempotency_key,
                    "replay_mutations": false,
                })
                .to_string(),
                now,
                incident.incident_id,
            ],
        )?;
        transaction.execute(
            r#"
                UPDATE stuck_run_remediation_claims
                SET state = 'completed',
                    continuation_task_ulid = ?2,
                    continuation_run_ulid = ?3,
                    updated_at_unix_ms = ?4
                WHERE incident_ulid = ?1
                  AND worker_id_sha256 = ?5
                  AND claim_epoch = ?6
                  AND state = 'claimed'
            "#,
            params![
                incident.incident_id,
                incident.continuation_task_id,
                incident.continuation_run_id,
                now,
                worker_id_sha256,
                claim_epoch,
            ],
        )?;
        let decision = RemediationDecision::new(
            incident.incident_id.clone(),
            StuckRunRemediationPolicy::GenerationSafeAutoRecovery,
            StuckRunRemediationDecisionKind::AutoRecover,
            now,
        );
        append_decision_event(
            &transaction,
            &decision,
            "continuation_queued",
            Some(worker_id_sha256.as_str()),
            Some(claim_epoch),
        )?;
        transaction.commit()?;
        Ok(StuckRunRemediationCompletionOutcome::ContinuationQueued {
            task_id: incident.continuation_task_id.clone(),
            run_id: incident.continuation_run_id.clone(),
        })
    }
}

fn load_run_authority(
    connection: &rusqlite::Connection,
    run_id: &str,
) -> Result<Option<RunAuthority>, JournalError> {
    connection
        .query_row(
            r#"
                SELECT run.session_ulid, lease.generation, lease.lease_ulid,
                       lease.expires_at_unix_ms, lease.owner
                FROM orchestrator_runs run
                JOIN runtime_generation_leases lease
                  ON lease.session_ulid = run.session_ulid
                 AND lease.run_ulid = run.run_ulid
                 AND lease.lane = 'run'
                WHERE run.run_ulid = ?1
                  AND run.state NOT IN ('done', 'failed', 'cancelled')
            "#,
            params![run_id],
            |row| {
                let generation = row.get::<_, i64>(1)?;
                Ok(RunAuthority {
                    session_id: row.get(0)?,
                    generation: u64::try_from(generation)
                        .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(1, generation))?,
                    lease_id: row.get(2)?,
                    lease_expires_at_unix_ms: row.get(3)?,
                    owner: row.get(4)?,
                })
            },
        )
        .optional()
        .map_err(Into::into)
}

fn load_incident(
    connection: &rusqlite::Connection,
    incident_id: &str,
) -> Result<Option<StuckRunIncidentV2>, JournalError> {
    connection
        .query_row(
            "SELECT incident_json FROM stuck_run_incidents_v2 WHERE incident_ulid = ?1",
            params![incident_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|payload| serde_json::from_str(payload.as_str()).map_err(JournalError::from))
        .transpose()
}

fn provider_wait_in_flight(
    connection: &rusqlite::Connection,
    run_id: &str,
) -> Result<bool, JournalError> {
    let latest = connection
        .query_row(
            r#"
                SELECT event_name
                FROM runtime_events_v2
                WHERE run_ulid = ?1 AND lane = 'provider'
                ORDER BY generation DESC, sequence DESC
                LIMIT 1
            "#,
            params![run_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    Ok(latest.as_deref() == Some(RuntimeEventName::ProviderAttemptStarted.as_str()))
}

fn append_decision_event(
    connection: &rusqlite::Connection,
    decision: &RemediationDecision,
    event_type: &str,
    worker_id_sha256: Option<&str>,
    claim_epoch: Option<i64>,
) -> Result<(), JournalError> {
    let decision_json = serde_json::to_string(decision)?;
    connection.execute(
        r#"
            INSERT INTO stuck_run_remediation_events (
                event_ulid, incident_ulid, event_type, reason_code,
                decision_json, decision_sha256, worker_id_sha256,
                claim_epoch, created_at_unix_ms, schema_version
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 1)
        "#,
        params![
            Ulid::new().to_string(),
            decision.incident_id,
            event_type,
            decision.reason_code,
            decision_json,
            sha256_hex(decision_json.as_bytes()),
            worker_id_sha256,
            claim_epoch,
            decision.decided_at_unix_ms,
        ],
    )?;
    let run_id = connection.query_row(
        "SELECT run_ulid FROM stuck_run_incidents_v2 WHERE incident_ulid = ?1",
        params![decision.incident_id],
        |row| row.get::<_, String>(0),
    )?;
    let tape_sequence = next_orchestrator_tape_seq(connection, run_id.as_str())?;
    connection.execute(
        r#"
            INSERT INTO orchestrator_tape (
                run_ulid, seq, event_type, payload_json, created_at_unix_ms
            ) VALUES (?1, ?2, 'stuck_run_remediation_decision', ?3, ?4)
        "#,
        params![
            run_id,
            tape_sequence,
            json!({
                "schema_version": 1,
                "incident_id": decision.incident_id,
                "policy": decision.policy,
                "decision": decision.decision,
                "reason_code": decision.reason_code,
                "event_type": event_type,
                "claim_epoch": claim_epoch,
            })
            .to_string(),
            decision.decided_at_unix_ms,
        ],
    )?;
    Ok(())
}

fn sqlite_generation(generation: u64) -> Result<i64, JournalError> {
    i64::try_from(generation).map_err(|_| {
        JournalError::InvalidArgument("runtime generation exceeds SQLite range".to_owned())
    })
}

fn stable_id(namespace: &str, material: &str) -> String {
    format!("{namespace}:{}", sha256_hex(material.as_bytes()))
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::journal::JournalConfig;

    fn temp_db_path() -> PathBuf {
        std::env::temp_dir().join(format!("palyra-stuck-run-{}.sqlite3", Ulid::new()))
    }

    fn open_store(path: PathBuf) -> JournalStore {
        JournalStore::open(JournalConfig {
            db_path: path,
            hash_chain_enabled: false,
            max_payload_bytes: 256 * 1_024,
            max_events: 10_000,
        })
        .expect("test journal should open")
    }

    fn insert_run_fixture(store: &JournalStore, mutating: bool) {
        let guard = store.connection.lock().expect("journal connection should lock");
        guard
            .execute_batch(
                r#"
                    INSERT INTO orchestrator_sessions (
                        session_ulid, principal, device_id, channel,
                        created_at_unix_ms, updated_at_unix_ms
                    ) VALUES ('session', 'user:test', 'device', 'test', 1, 1);
                    INSERT INTO orchestrator_runs (
                        run_ulid, session_ulid, state, created_at_unix_ms,
                        started_at_unix_ms, updated_at_unix_ms
                    ) VALUES ('run', 'session', 'running', 1, 1, 1);
                    INSERT INTO runtime_generation_leases (
                        session_ulid, lane, lease_ulid, run_ulid, generation,
                        owner, acquired_at_unix_ms, expires_at_unix_ms
                    ) VALUES (
                        'session', 'run', 'lease', 'run', 7,
                        'orchestrator_run', 1, 9223372036854775807
                    );
                "#,
            )
            .expect("run fixture should insert");
        let tool_name = if mutating { "palyra.fs.apply_patch" } else { "palyra.gateway.status" };
        guard
            .execute(
                r#"
                    INSERT INTO orchestrator_tape (
                        run_ulid, seq, event_type, payload_json, created_at_unix_ms
                    ) VALUES ('run', 0, 'tool_proposal', ?1, 10)
                "#,
                params![json!({
                    "tool_proposal": {
                        "proposal_id": "proposal",
                        "tool_name": tool_name,
                    }
                })
                .to_string()],
            )
            .expect("tool proposal should insert");
        guard
            .execute(
                r#"
                    INSERT INTO orchestrator_tape (
                        run_ulid, seq, event_type, payload_json, created_at_unix_ms
                    ) VALUES ('run', 1, 'tool_decision', ?1, 11)
                "#,
                params![json!({
                    "tool_decision": {
                        "proposal_id": "proposal",
                        "kind": "allow",
                    }
                })
                .to_string()],
            )
            .expect("tool decision should insert");
    }

    fn inspect_read_only_incident(store: &JournalStore) -> StuckRunIncidentV2 {
        store
            .inspect_stuck_run_incident("run", Some(7), 1_000)
            .expect("stuck run inspection should succeed")
            .expect("active run authority should exist")
    }

    #[test]
    fn two_remediation_workers_cannot_claim_the_same_generation() {
        let store = open_store(temp_db_path());
        insert_run_fixture(&store, false);
        let incident = inspect_read_only_incident(&store);

        let first = store
            .claim_stuck_run_remediation(&incident, "worker-a", 30_000)
            .expect("first claim should succeed");
        let second = store
            .claim_stuck_run_remediation(&incident, "worker-b", 30_000)
            .expect("second claim should return a bounded outcome");

        assert_eq!(first, StuckRunRemediationClaimOutcome::Claimed { claim_epoch: 1 });
        assert_eq!(second, StuckRunRemediationClaimOutcome::Busy);
    }

    #[test]
    fn restart_during_cleanup_reclaims_and_queues_continuation_once() {
        let path = temp_db_path();
        let store = open_store(path.clone());
        insert_run_fixture(&store, false);
        let incident = inspect_read_only_incident(&store);
        assert_eq!(
            store
                .claim_stuck_run_remediation(&incident, "worker-before-restart", 1)
                .expect("initial claim should succeed"),
            StuckRunRemediationClaimOutcome::Claimed { claim_epoch: 1 }
        );
        store
            .connection
            .lock()
            .expect("journal connection should lock")
            .execute("UPDATE stuck_run_remediation_claims SET claim_expires_at_unix_ms = 0", [])
            .expect("crash fixture should expire the abandoned claim");
        drop(store);

        let reopened = open_store(path);
        let reclaimed = reopened
            .claim_stuck_run_remediation(&incident, "worker-after-restart", 30_000)
            .expect("expired claim should be reclaimable");
        assert_eq!(reclaimed, StuckRunRemediationClaimOutcome::Claimed { claim_epoch: 2 });
        let first = reopened
            .complete_stuck_run_remediation(&incident, "worker-after-restart", 2)
            .expect("reclaimed remediation should complete");
        let replay = reopened
            .complete_stuck_run_remediation(&incident, "worker-after-restart", 2)
            .expect("completion replay should be idempotent");
        assert!(matches!(first, StuckRunRemediationCompletionOutcome::ContinuationQueued { .. }));
        assert!(matches!(replay, StuckRunRemediationCompletionOutcome::AlreadyQueued { .. }));
        let guard = reopened.connection.lock().expect("journal connection should lock");
        let queued_count: i64 = guard
            .query_row(
                "SELECT COUNT(*) FROM orchestrator_background_tasks WHERE task_ulid = ?1",
                params![incident.continuation_task_id],
                |row| row.get(0),
            )
            .expect("continuation count should load");
        assert_eq!(queued_count, 1);
    }

    #[test]
    fn mutating_tool_state_cannot_be_claimed_for_replay() {
        let store = open_store(temp_db_path());
        insert_run_fixture(&store, true);
        let incident = store
            .inspect_stuck_run_incident("run", Some(7), 1_000)
            .expect("stuck run inspection should succeed")
            .expect("active run authority should exist");

        assert!(incident.mutating_tool_in_flight);
        assert_eq!(
            store
                .claim_stuck_run_remediation(&incident, "worker", 30_000)
                .expect("blocked claim should return a bounded outcome"),
            StuckRunRemediationClaimOutcome::StaleAuthority
        );
    }

    #[test]
    fn expired_generation_lease_cannot_be_claimed() {
        let store = open_store(temp_db_path());
        insert_run_fixture(&store, false);
        let incident = inspect_read_only_incident(&store);
        store
            .connection
            .lock()
            .expect("journal connection should lock")
            .execute("UPDATE runtime_generation_leases SET expires_at_unix_ms = 0", [])
            .expect("test generation lease should expire");

        assert_eq!(
            store
                .claim_stuck_run_remediation(&incident, "worker", 30_000)
                .expect("expired claim should return a bounded outcome"),
            StuckRunRemediationClaimOutcome::StaleAuthority
        );
    }
}
