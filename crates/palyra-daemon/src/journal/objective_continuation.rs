//! Durable objective/runtime bindings, judge attempts, and continuation
//! transitions. The journal owns replay identity and compare-and-set state;
//! the file-backed objective registry remains the operator-facing read model.

use super::*;

const OBJECTIVE_CONTINUATION_SCHEMA_VERSION: i64 = 1;
const OBJECTIVE_CONTINUATION_SCAN_LIMIT: i64 = 256;

pub(super) const MIGRATION_87_SQL: &str = r#"
    CREATE TABLE IF NOT EXISTS objective_runtime_bindings_v1 (
        objective_ulid TEXT PRIMARY KEY,
        routine_ulid TEXT,
        session_ulid TEXT NOT NULL,
        root_run_ulid TEXT NOT NULL,
        current_run_generation INTEGER NOT NULL CHECK (current_run_generation >= 1),
        current_attempt_ulid TEXT NOT NULL,
        workgraph_ulid TEXT,
        contract_sha256 TEXT NOT NULL,
        revision INTEGER NOT NULL DEFAULT 1 CHECK (revision >= 1),
        schema_version INTEGER NOT NULL DEFAULT 1 CHECK (schema_version = 1),
        created_at_unix_ms INTEGER NOT NULL,
        updated_at_unix_ms INTEGER NOT NULL,
        FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid),
        FOREIGN KEY(root_run_ulid) REFERENCES orchestrator_runs(run_ulid)
    );
    CREATE INDEX IF NOT EXISTS idx_objective_runtime_binding_session
        ON objective_runtime_bindings_v1(session_ulid, updated_at_unix_ms);

    CREATE TABLE IF NOT EXISTS objective_continuation_attempts_v1 (
        attempt_ulid TEXT PRIMARY KEY,
        objective_ulid TEXT NOT NULL,
        routine_ulid TEXT,
        session_ulid TEXT NOT NULL,
        root_run_ulid TEXT NOT NULL,
        source_run_ulid TEXT NOT NULL,
        source_run_generation INTEGER NOT NULL CHECK (source_run_generation >= 1),
        judge_task_ulid TEXT NOT NULL UNIQUE,
        continuation_task_ulid TEXT UNIQUE,
        owner_principal TEXT NOT NULL,
        device_id TEXT NOT NULL,
        channel TEXT,
        judge_payload_json TEXT NOT NULL,
        judge_payload_sha256 TEXT NOT NULL,
        contract_sha256 TEXT NOT NULL,
        budget_tokens INTEGER NOT NULL CHECK (budget_tokens >= 0),
        state TEXT NOT NULL CHECK (
            state IN (
                'judge_enqueue_pending',
                'judge_enqueued',
                'decision_pending',
                'continuation_enqueue_pending',
                'continuation_enqueued',
                'settled'
            )
        ),
        decision TEXT NOT NULL CHECK (
            decision IN ('pending', 'done', 'continue', 'wait', 'blocked', 'needs_user')
        ),
        reason_code TEXT NOT NULL,
        summary_text TEXT NOT NULL,
        evidence_refs_json TEXT NOT NULL,
        next_action TEXT,
        retry_count INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
        next_eligible_at_unix_ms INTEGER,
        schema_version INTEGER NOT NULL DEFAULT 1 CHECK (schema_version = 1),
        created_at_unix_ms INTEGER NOT NULL,
        updated_at_unix_ms INTEGER NOT NULL,
        UNIQUE(objective_ulid, source_run_ulid, source_run_generation),
        FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid),
        FOREIGN KEY(root_run_ulid) REFERENCES orchestrator_runs(run_ulid),
        FOREIGN KEY(source_run_ulid) REFERENCES orchestrator_runs(run_ulid)
    );
    CREATE INDEX IF NOT EXISTS idx_objective_continuation_pending
        ON objective_continuation_attempts_v1(state, updated_at_unix_ms);
    CREATE INDEX IF NOT EXISTS idx_objective_continuation_objective
        ON objective_continuation_attempts_v1(objective_ulid, created_at_unix_ms);

    CREATE TABLE IF NOT EXISTS objective_attempt_transitions_v1 (
        transition_ulid TEXT PRIMARY KEY,
        attempt_ulid TEXT NOT NULL,
        from_state TEXT,
        to_state TEXT NOT NULL,
        decision TEXT NOT NULL CHECK (
            decision IN ('pending', 'done', 'continue', 'wait', 'blocked', 'needs_user')
        ),
        reason_code TEXT NOT NULL,
        evidence_refs_json TEXT NOT NULL,
        schema_version INTEGER NOT NULL DEFAULT 1 CHECK (schema_version = 1),
        created_at_unix_ms INTEGER NOT NULL,
        FOREIGN KEY(attempt_ulid)
            REFERENCES objective_continuation_attempts_v1(attempt_ulid)
    );
    CREATE INDEX IF NOT EXISTS idx_objective_attempt_transition_attempt
        ON objective_attempt_transitions_v1(attempt_ulid, created_at_unix_ms);
"#;

/// Typed host decision derived from one strict objective-judge result.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ObjectiveContinuationDecision {
    Pending,
    Done,
    Continue,
    Wait,
    Blocked,
    NeedsUser,
}

impl ObjectiveContinuationDecision {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Done => "done",
            Self::Continue => "continue",
            Self::Wait => "wait",
            Self::Blocked => "blocked",
            Self::NeedsUser => "needs_user",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "done" => Some(Self::Done),
            "continue" => Some(Self::Continue),
            "wait" => Some(Self::Wait),
            "blocked" => Some(Self::Blocked),
            "needs_user" => Some(Self::NeedsUser),
            _ => None,
        }
    }
}

/// Exact runtime binding for one long-lived objective.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ObjectiveRuntimeBindingRecord {
    pub(crate) objective_id: String,
    pub(crate) routine_id: Option<String>,
    pub(crate) session_id: String,
    pub(crate) root_run_id: String,
    pub(crate) current_run_generation: u64,
    pub(crate) current_attempt_id: String,
    pub(crate) workgraph_id: Option<String>,
    pub(crate) contract_sha256: String,
    pub(crate) revision: u64,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) updated_at_unix_ms: i64,
}

/// Replayable judge/continuation state for one objective attempt.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ObjectiveContinuationAttemptRecord {
    pub(crate) attempt_id: String,
    pub(crate) objective_id: String,
    pub(crate) routine_id: Option<String>,
    pub(crate) session_id: String,
    pub(crate) root_run_id: String,
    pub(crate) source_run_id: String,
    pub(crate) source_run_generation: u64,
    pub(crate) judge_task_id: String,
    pub(crate) continuation_task_id: Option<String>,
    pub(crate) owner_principal: String,
    pub(crate) device_id: String,
    pub(crate) channel: Option<String>,
    pub(crate) judge_payload_json: String,
    pub(crate) judge_payload_sha256: String,
    pub(crate) contract_sha256: String,
    pub(crate) budget_tokens: u64,
    pub(crate) state: String,
    pub(crate) decision: ObjectiveContinuationDecision,
    pub(crate) reason_code: String,
    pub(crate) summary_text: String,
    pub(crate) evidence_refs_json: String,
    pub(crate) next_action: Option<String>,
    pub(crate) retry_count: u64,
    pub(crate) next_eligible_at_unix_ms: Option<i64>,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) updated_at_unix_ms: i64,
}

/// One append-only objective attempt transition.
#[cfg(test)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ObjectiveAttemptTransition {
    pub(crate) transition_id: String,
    pub(crate) attempt_id: String,
    pub(crate) from_state: Option<String>,
    pub(crate) to_state: String,
    pub(crate) decision: ObjectiveContinuationDecision,
    pub(crate) reason_code: String,
    pub(crate) evidence_refs_json: String,
    pub(crate) created_at_unix_ms: i64,
}

/// Host-snapshotted inputs used to reserve a judge attempt before enqueue.
#[derive(Debug, Clone)]
pub(crate) struct ObjectiveAttemptReserveRequest {
    pub(crate) attempt_id: String,
    pub(crate) objective_id: String,
    pub(crate) routine_id: Option<String>,
    pub(crate) session_id: String,
    pub(crate) root_run_id: String,
    pub(crate) source_run_id: String,
    pub(crate) source_run_generation: u64,
    pub(crate) judge_task_id: String,
    pub(crate) owner_principal: String,
    pub(crate) device_id: String,
    pub(crate) channel: Option<String>,
    pub(crate) judge_payload_json: String,
    pub(crate) contract_sha256: String,
    pub(crate) budget_tokens: u64,
    pub(crate) workgraph_id: Option<String>,
}

/// Strict judge outcome awaiting objective-read-model application.
#[derive(Debug, Clone)]
pub(crate) struct ObjectiveJudgeDecisionRequest {
    pub(crate) judge_task_id: String,
    pub(crate) decision: ObjectiveContinuationDecision,
    pub(crate) reason_code: String,
    pub(crate) summary_text: String,
    pub(crate) evidence_refs_json: String,
    pub(crate) next_action: Option<String>,
    pub(crate) retry_count: u64,
    pub(crate) next_eligible_at_unix_ms: Option<i64>,
    pub(crate) guard: super::ObjectiveGuardEvaluationRequest,
}

/// Outcome of reserving a continuation under the session-input priority fence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ObjectiveContinuationTaskReserveOutcome {
    Reserved(ObjectiveContinuationAttemptRecord),
    UserPreempted(ObjectiveContinuationAttemptRecord),
}

impl JournalStore {
    /// Reserves one deduplicated objective judge attempt and updates the
    /// objective's exact runtime binding in the same transaction.
    ///
    /// # Errors
    /// Returns a journal error when identities, payloads, or storage fail.
    pub(crate) fn reserve_objective_attempt(
        &self,
        request: &ObjectiveAttemptReserveRequest,
    ) -> Result<ObjectiveContinuationAttemptRecord, JournalError> {
        validate_attempt_reservation(request)?;
        let now = current_unix_ms()?;
        let payload_sha256 = hex::encode(Sha256::digest(request.judge_payload_json.as_bytes()));
        let budget_tokens = u64_to_sqlite(request.budget_tokens, "budget_tokens")?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing) = load_objective_attempt_for_source_tx(
            &transaction,
            &request.objective_id,
            &request.source_run_id,
            request.source_run_generation,
        )? {
            if existing.judge_payload_sha256 != payload_sha256
                || existing.contract_sha256 != request.contract_sha256
                || existing.session_id != request.session_id
                || existing.root_run_id != request.root_run_id
                || existing.source_run_generation != request.source_run_generation
            {
                return Err(JournalError::InvalidArgument(
                    "objective attempt replay conflicts with committed binding evidence".to_owned(),
                ));
            }
            transaction.commit()?;
            return Ok(existing);
        }
        transaction.execute(
            r#"
                INSERT INTO objective_continuation_attempts_v1 (
                    attempt_ulid, objective_ulid, routine_ulid, session_ulid,
                    root_run_ulid, source_run_ulid, judge_task_ulid,
                    source_run_generation,
                    continuation_task_ulid, owner_principal, device_id, channel,
                    judge_payload_json, judge_payload_sha256, contract_sha256,
                    budget_tokens, state, decision, reason_code, summary_text,
                    evidence_refs_json, next_action, retry_count,
                    next_eligible_at_unix_ms, schema_version,
                    created_at_unix_ms, updated_at_unix_ms
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, NULL, ?9, ?10, ?11,
                    ?12, ?13, ?14, ?15, 'judge_enqueue_pending', 'pending',
                    'objective.continuation.judge_reserved', '', '[]', NULL, 0,
                    NULL, 1, ?16, ?16
                )
            "#,
            params![
                request.attempt_id,
                request.objective_id,
                request.routine_id,
                request.session_id,
                request.root_run_id,
                request.source_run_id,
                request.judge_task_id,
                u64_to_sqlite(request.source_run_generation, "source_run_generation")?,
                request.owner_principal,
                request.device_id,
                request.channel,
                request.judge_payload_json,
                payload_sha256,
                request.contract_sha256,
                budget_tokens,
                now,
            ],
        )?;
        transaction.execute(
            r#"
                INSERT INTO objective_runtime_bindings_v1 (
                    objective_ulid, routine_ulid, session_ulid, root_run_ulid,
                    current_run_generation, current_attempt_ulid, workgraph_ulid, contract_sha256,
                    revision, schema_version, created_at_unix_ms, updated_at_unix_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 1, 1, ?9, ?9)
                ON CONFLICT(objective_ulid) DO UPDATE SET
                    routine_ulid = excluded.routine_ulid,
                    session_ulid = excluded.session_ulid,
                    current_run_generation = excluded.current_run_generation,
                    current_attempt_ulid = excluded.current_attempt_ulid,
                    workgraph_ulid = excluded.workgraph_ulid,
                    contract_sha256 = excluded.contract_sha256,
                    revision = objective_runtime_bindings_v1.revision + 1,
                    updated_at_unix_ms = excluded.updated_at_unix_ms
            "#,
            params![
                request.objective_id,
                request.routine_id,
                request.session_id,
                request.root_run_id,
                u64_to_sqlite(request.source_run_generation, "source_run_generation")?,
                request.attempt_id,
                request.workgraph_id,
                request.contract_sha256,
                now,
            ],
        )?;
        append_objective_transition_tx(
            &transaction,
            &request.attempt_id,
            None,
            "judge_enqueue_pending",
            ObjectiveContinuationDecision::Pending,
            "objective.continuation.judge_reserved",
            "[]",
            now,
        )?;
        let record =
            load_objective_attempt_tx(&transaction, &request.attempt_id)?.ok_or_else(|| {
                JournalError::InvalidArgument(
                    "reserved objective attempt could not be reloaded".to_owned(),
                )
            })?;
        transaction.commit()?;
        Ok(record)
    }

    /// Loads an attempt by its durable judge task identity.
    pub(crate) fn objective_attempt_for_judge_task(
        &self,
        judge_task_id: &str,
    ) -> Result<Option<ObjectiveContinuationAttemptRecord>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        load_objective_attempt_by_column_tx(&guard, "judge_task_ulid", judge_task_id)
    }

    /// Loads an attempt by its durable continuation task identity.
    pub(crate) fn objective_attempt_for_continuation_task(
        &self,
        continuation_task_id: &str,
    ) -> Result<Option<ObjectiveContinuationAttemptRecord>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        load_objective_attempt_by_column_tx(&guard, "continuation_task_ulid", continuation_task_id)
    }

    /// Loads an attempt by its durable identity.
    pub(crate) fn objective_attempt_by_id(
        &self,
        attempt_id: &str,
    ) -> Result<Option<ObjectiveContinuationAttemptRecord>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        load_objective_attempt_tx(&guard, attempt_id)
    }

    /// Loads the current runtime binding for one objective.
    pub(crate) fn objective_runtime_binding(
        &self,
        objective_id: &str,
    ) -> Result<Option<ObjectiveRuntimeBindingRecord>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        guard
            .query_row(
                r#"
                    SELECT
                        objective_ulid, routine_ulid, session_ulid, root_run_ulid,
                        current_run_generation, current_attempt_ulid, workgraph_ulid, contract_sha256,
                        revision, created_at_unix_ms, updated_at_unix_ms
                    FROM objective_runtime_bindings_v1
                    WHERE objective_ulid = ?1
                "#,
                params![objective_id],
                |row| {
                    Ok(ObjectiveRuntimeBindingRecord {
                        objective_id: row.get(0)?,
                        routine_id: row.get(1)?,
                        session_id: row.get(2)?,
                        root_run_id: row.get(3)?,
                        current_run_generation: integer_to_u64(
                            row,
                            4,
                            "current_run_generation",
                        )?,
                        current_attempt_id: row.get(5)?,
                        workgraph_id: row.get(6)?,
                        contract_sha256: row.get(7)?,
                        revision: integer_to_u64(row, 8, "revision")?,
                        created_at_unix_ms: row.get(9)?,
                        updated_at_unix_ms: row.get(10)?,
                    })
                },
            )
            .optional()
            .map_err(JournalError::from)
    }

    /// Lists bounded attempts whose enqueue or read-model application must be replayed.
    pub(crate) fn pending_objective_attempts(
        &self,
    ) -> Result<Vec<ObjectiveContinuationAttemptRecord>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let query = format!(
            "SELECT {OBJECTIVE_ATTEMPT_COLUMNS} \
             FROM objective_continuation_attempts_v1 \
             WHERE state IN (
                'judge_enqueue_pending',
                'judge_enqueued',
                'decision_pending',
                'continuation_enqueue_pending',
                'continuation_enqueued'
             ) \
             ORDER BY updated_at_unix_ms ASC, attempt_ulid ASC \
             LIMIT {OBJECTIVE_CONTINUATION_SCAN_LIMIT}"
        );
        let mut statement = guard.prepare(query.as_str())?;
        let rows = statement.query_map([], map_objective_attempt_row)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(JournalError::from)
    }

    /// Advances a reserved judge attempt after the matching task is durable.
    pub(crate) fn mark_objective_judge_enqueued(
        &self,
        judge_task_id: &str,
    ) -> Result<ObjectiveContinuationAttemptRecord, JournalError> {
        self.transition_objective_attempt(
            "judge_task_ulid",
            judge_task_id,
            &["judge_enqueue_pending"],
            "judge_enqueued",
            ObjectiveContinuationDecision::Pending,
            "objective.continuation.judge_enqueued",
            "[]",
        )
    }

    /// Commits one strict judge decision exactly once.
    pub(crate) fn settle_objective_judge_decision(
        &self,
        request: &ObjectiveJudgeDecisionRequest,
    ) -> Result<ObjectiveContinuationAttemptRecord, JournalError> {
        if request.decision == ObjectiveContinuationDecision::Pending {
            return Err(JournalError::InvalidArgument(
                "objective judge cannot settle to pending".to_owned(),
            ));
        }
        validate_bounded_text(&request.reason_code, "reason_code", 128)?;
        validate_bounded_text(&request.summary_text, "summary_text", 2_048)?;
        if let Some(next_action) = request.next_action.as_deref() {
            validate_bounded_text(next_action, "next_action", 2_048)?;
        }
        let _: Value =
            serde_json::from_str(request.evidence_refs_json.as_str()).map_err(|error| {
                JournalError::InvalidArgument(format!(
                    "objective evidence_refs_json is invalid: {error}"
                ))
            })?;
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = load_objective_attempt_by_column_tx(
            &transaction,
            "judge_task_ulid",
            request.judge_task_id.as_str(),
        )?
        .ok_or_else(|| {
            JournalError::InvalidArgument(
                "objective judge task is not bound to an attempt".to_owned(),
            )
        })?;
        if current.state == "decision_pending"
            || current.state == "settled"
            || current.state == "continuation_enqueue_pending"
            || current.state == "continuation_enqueued"
        {
            if current.decision != request.decision
                || current.reason_code != request.reason_code
                || current.evidence_refs_json != request.evidence_refs_json
            {
                return Err(JournalError::InvalidArgument(
                    "objective judge replay conflicts with committed decision".to_owned(),
                ));
            }
            super::objective_guards::evaluate_objective_guard_tx(
                &transaction,
                &request.guard,
                now,
            )?;
            transaction.commit()?;
            return Ok(current);
        }
        if !matches!(current.state.as_str(), "judge_enqueued" | "judge_enqueue_pending") {
            return Err(JournalError::InvalidArgument(format!(
                "objective judge cannot settle from state '{}'",
                current.state
            )));
        }
        transaction.execute(
            r#"
                UPDATE objective_continuation_attempts_v1
                SET state = 'decision_pending',
                    decision = ?2,
                    reason_code = ?3,
                    summary_text = ?4,
                    evidence_refs_json = ?5,
                    next_action = ?6,
                    retry_count = ?7,
                    next_eligible_at_unix_ms = ?8,
                    updated_at_unix_ms = ?9
                WHERE attempt_ulid = ?1
            "#,
            params![
                current.attempt_id,
                request.decision.as_str(),
                request.reason_code,
                request.summary_text,
                request.evidence_refs_json,
                request.next_action,
                u64_to_sqlite(request.retry_count, "retry_count")?,
                request.next_eligible_at_unix_ms,
                now,
            ],
        )?;
        append_objective_transition_tx(
            &transaction,
            current.attempt_id.as_str(),
            Some(current.state.as_str()),
            "decision_pending",
            request.decision,
            request.reason_code.as_str(),
            request.evidence_refs_json.as_str(),
            now,
        )?;
        super::objective_guards::evaluate_objective_guard_tx(&transaction, &request.guard, now)?;
        let record = load_objective_attempt_tx(&transaction, current.attempt_id.as_str())?
            .ok_or_else(|| {
                JournalError::InvalidArgument(
                    "settled objective attempt could not be reloaded".to_owned(),
                )
            })?;
        transaction.commit()?;
        Ok(record)
    }

    /// Reserves the exact background task that will perform a continue turn.
    pub(crate) fn reserve_objective_continuation_task(
        &self,
        attempt_id: &str,
        continuation_task_id: &str,
        reason_code: &str,
    ) -> Result<ObjectiveContinuationTaskReserveOutcome, JournalError> {
        validate_bounded_text(continuation_task_id, "continuation_task_id", 128)?;
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = load_objective_attempt_tx(&transaction, attempt_id)?.ok_or_else(|| {
            JournalError::InvalidArgument("objective attempt does not exist".to_owned())
        })?;
        if !matches!(
            current.decision,
            ObjectiveContinuationDecision::Continue | ObjectiveContinuationDecision::Wait
        ) {
            return Err(JournalError::InvalidArgument(
                "only continue or wait can reserve a continuation task".to_owned(),
            ));
        }
        if let Some(existing) = current.continuation_task_id.as_deref() {
            if existing != continuation_task_id {
                return Err(JournalError::InvalidArgument(
                    "objective continuation task identity already committed".to_owned(),
                ));
            }
            transaction.commit()?;
            return Ok(ObjectiveContinuationTaskReserveOutcome::Reserved(current));
        }
        if current.state != "decision_pending" {
            return Err(JournalError::InvalidArgument(format!(
                "objective continuation cannot reserve from state '{}'",
                current.state
            )));
        }
        let user_input_pending = transaction.query_row(
            r#"
                SELECT EXISTS(
                    SELECT 1
                    FROM orchestrator_queued_inputs
                    WHERE session_ulid = ?1
                      AND state IN ('pending', 'claimed', 'deferred')
                )
            "#,
            params![current.session_id],
            |row| row.get::<_, i64>(0),
        )? != 0;
        if user_input_pending {
            transaction.commit()?;
            return Ok(ObjectiveContinuationTaskReserveOutcome::UserPreempted(current));
        }
        transaction.execute(
            r#"
                UPDATE objective_continuation_attempts_v1
                SET continuation_task_ulid = ?2,
                    state = 'continuation_enqueue_pending',
                    reason_code = ?3,
                    updated_at_unix_ms = ?4
                WHERE attempt_ulid = ?1
            "#,
            params![attempt_id, continuation_task_id, reason_code, now],
        )?;
        append_objective_transition_tx(
            &transaction,
            attempt_id,
            Some(current.state.as_str()),
            "continuation_enqueue_pending",
            current.decision,
            reason_code,
            current.evidence_refs_json.as_str(),
            now,
        )?;
        let record = load_objective_attempt_tx(&transaction, attempt_id)?.ok_or_else(|| {
            JournalError::InvalidArgument(
                "continuation-reserved attempt could not be reloaded".to_owned(),
            )
        })?;
        transaction.commit()?;
        Ok(ObjectiveContinuationTaskReserveOutcome::Reserved(record))
    }

    /// Marks a continuation task durable, or settles a non-continue decision.
    pub(crate) fn mark_objective_attempt_applied(
        &self,
        attempt_id: &str,
        target_state: &str,
        reason_code: &str,
    ) -> Result<ObjectiveContinuationAttemptRecord, JournalError> {
        let current = {
            let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
            load_objective_attempt_tx(&guard, attempt_id)?.ok_or_else(|| {
                JournalError::InvalidArgument("objective attempt does not exist".to_owned())
            })?
        };
        let allowed = match target_state {
            "continuation_enqueued" => {
                current.state == "continuation_enqueue_pending"
                    && matches!(
                        current.decision,
                        ObjectiveContinuationDecision::Continue
                            | ObjectiveContinuationDecision::Wait
                    )
            }
            "settled" => {
                matches!(current.state.as_str(), "decision_pending" | "continuation_enqueued")
            }
            _ => false,
        };
        if current.state == target_state {
            return Ok(current);
        }
        if !allowed {
            return Err(JournalError::InvalidArgument(format!(
                "objective attempt cannot transition from '{}' to '{target_state}'",
                current.state
            )));
        }
        self.transition_objective_attempt(
            "attempt_ulid",
            attempt_id,
            &[current.state.as_str()],
            target_state,
            current.decision,
            reason_code,
            current.evidence_refs_json.as_str(),
        )
    }

    /// Returns append-only transitions for one attempt.
    #[cfg(test)]
    pub(crate) fn objective_attempt_transitions(
        &self,
        attempt_id: &str,
    ) -> Result<Vec<ObjectiveAttemptTransition>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let mut statement = guard.prepare(
            r#"
                SELECT
                    transition_ulid, attempt_ulid, from_state, to_state,
                    decision, reason_code, evidence_refs_json, created_at_unix_ms
                FROM objective_attempt_transitions_v1
                WHERE attempt_ulid = ?1
                ORDER BY created_at_unix_ms ASC, transition_ulid ASC
            "#,
        )?;
        let rows = statement.query_map(params![attempt_id], map_objective_transition_row)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(JournalError::from)
    }

    // Keeping the expected-state fence and evidence together makes every CAS
    // call site auditable; splitting them across mutable builder steps would
    // make accidental omission easier.
    #[allow(clippy::too_many_arguments)]
    fn transition_objective_attempt(
        &self,
        identity_column: &str,
        identity: &str,
        expected_states: &[&str],
        target_state: &str,
        decision: ObjectiveContinuationDecision,
        reason_code: &str,
        evidence_refs_json: &str,
    ) -> Result<ObjectiveContinuationAttemptRecord, JournalError> {
        if !matches!(identity_column, "attempt_ulid" | "judge_task_ulid" | "continuation_task_ulid")
        {
            return Err(JournalError::InvalidArgument(
                "unsupported objective attempt identity column".to_owned(),
            ));
        }
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = load_objective_attempt_by_column_tx(&transaction, identity_column, identity)?
            .ok_or_else(|| {
                JournalError::InvalidArgument("objective attempt does not exist".to_owned())
            })?;
        if current.state == target_state {
            transaction.commit()?;
            return Ok(current);
        }
        if !expected_states.iter().any(|state| *state == current.state) {
            return Err(JournalError::InvalidArgument(format!(
                "objective attempt cannot transition from '{}' to '{target_state}'",
                current.state
            )));
        }
        transaction.execute(
            "UPDATE objective_continuation_attempts_v1 \
             SET state = ?2, decision = ?3, reason_code = ?4, updated_at_unix_ms = ?5 \
             WHERE attempt_ulid = ?1",
            params![current.attempt_id, target_state, decision.as_str(), reason_code, now],
        )?;
        append_objective_transition_tx(
            &transaction,
            current.attempt_id.as_str(),
            Some(current.state.as_str()),
            target_state,
            decision,
            reason_code,
            evidence_refs_json,
            now,
        )?;
        let record = load_objective_attempt_tx(&transaction, current.attempt_id.as_str())?
            .ok_or_else(|| {
                JournalError::InvalidArgument(
                    "transitioned objective attempt could not be reloaded".to_owned(),
                )
            })?;
        transaction.commit()?;
        Ok(record)
    }
}

fn validate_attempt_reservation(
    request: &ObjectiveAttemptReserveRequest,
) -> Result<(), JournalError> {
    for (field, value, max_len) in [
        ("attempt_id", request.attempt_id.as_str(), 128),
        ("objective_id", request.objective_id.as_str(), 128),
        ("session_id", request.session_id.as_str(), 128),
        ("root_run_id", request.root_run_id.as_str(), 128),
        ("source_run_id", request.source_run_id.as_str(), 128),
        ("judge_task_id", request.judge_task_id.as_str(), 128),
        ("owner_principal", request.owner_principal.as_str(), 256),
        ("device_id", request.device_id.as_str(), 256),
        ("contract_sha256", request.contract_sha256.as_str(), 64),
    ] {
        validate_bounded_text(value, field, max_len)?;
    }
    if request.contract_sha256.len() != 64
        || !request.contract_sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(JournalError::InvalidArgument(
            "objective contract_sha256 must be a hexadecimal SHA-256 digest".to_owned(),
        ));
    }
    if request.judge_payload_json.len() > 64 * 1_024 {
        return Err(JournalError::PayloadTooLarge {
            payload_kind: "objective judge payload",
            actual_bytes: request.judge_payload_json.len(),
            max_bytes: 64 * 1_024,
        });
    }
    let _: Value = serde_json::from_str(request.judge_payload_json.as_str()).map_err(|error| {
        JournalError::InvalidArgument(format!("objective judge payload is invalid: {error}"))
    })?;
    Ok(())
}

fn validate_bounded_text(
    value: &str,
    field: &'static str,
    max_len: usize,
) -> Result<(), JournalError> {
    if value.trim().is_empty() || value.len() > max_len {
        return Err(JournalError::InvalidArgument(format!("{field} must be 1..={max_len} bytes")));
    }
    Ok(())
}

// Transition evidence is intentionally explicit at each transactional call
// site so state, decision, reason, and evidence cannot drift independently.
#[allow(clippy::too_many_arguments)]
fn append_objective_transition_tx(
    connection: &Connection,
    attempt_id: &str,
    from_state: Option<&str>,
    to_state: &str,
    decision: ObjectiveContinuationDecision,
    reason_code: &str,
    evidence_refs_json: &str,
    now: i64,
) -> Result<(), JournalError> {
    connection.execute(
        r#"
            INSERT INTO objective_attempt_transitions_v1 (
                transition_ulid, attempt_ulid, from_state, to_state,
                decision, reason_code, evidence_refs_json,
                schema_version, created_at_unix_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
        "#,
        params![
            Ulid::new().to_string(),
            attempt_id,
            from_state,
            to_state,
            decision.as_str(),
            reason_code,
            evidence_refs_json,
            OBJECTIVE_CONTINUATION_SCHEMA_VERSION,
            now,
        ],
    )?;
    Ok(())
}

fn load_objective_attempt_for_source_tx(
    connection: &Connection,
    objective_id: &str,
    source_run_id: &str,
    source_run_generation: u64,
) -> Result<Option<ObjectiveContinuationAttemptRecord>, JournalError> {
    let source_run_generation = u64_to_sqlite(source_run_generation, "source_run_generation")?;
    let query = format!(
        "SELECT {OBJECTIVE_ATTEMPT_COLUMNS} \
         FROM objective_continuation_attempts_v1 \
         WHERE objective_ulid = ?1
           AND source_run_ulid = ?2
           AND source_run_generation = ?3"
    );
    connection
        .query_row(
            query.as_str(),
            params![objective_id, source_run_id, source_run_generation],
            map_objective_attempt_row,
        )
        .optional()
        .map_err(JournalError::from)
}

fn load_objective_attempt_tx(
    connection: &Connection,
    attempt_id: &str,
) -> Result<Option<ObjectiveContinuationAttemptRecord>, JournalError> {
    load_objective_attempt_by_column_tx(connection, "attempt_ulid", attempt_id)
}

fn load_objective_attempt_by_column_tx(
    connection: &Connection,
    column: &str,
    identity: &str,
) -> Result<Option<ObjectiveContinuationAttemptRecord>, JournalError> {
    if !matches!(column, "attempt_ulid" | "judge_task_ulid" | "continuation_task_ulid") {
        return Err(JournalError::InvalidArgument(
            "unsupported objective attempt lookup column".to_owned(),
        ));
    }
    let query = format!(
        "SELECT {OBJECTIVE_ATTEMPT_COLUMNS} \
         FROM objective_continuation_attempts_v1 WHERE {column} = ?1"
    );
    connection
        .query_row(query.as_str(), params![identity], map_objective_attempt_row)
        .optional()
        .map_err(JournalError::from)
}

const OBJECTIVE_ATTEMPT_COLUMNS: &str = r#"
    attempt_ulid,
    objective_ulid,
    routine_ulid,
    session_ulid,
    root_run_ulid,
    source_run_ulid,
    source_run_generation,
    judge_task_ulid,
    continuation_task_ulid,
    owner_principal,
    device_id,
    channel,
    judge_payload_json,
    judge_payload_sha256,
    contract_sha256,
    budget_tokens,
    state,
    decision,
    reason_code,
    summary_text,
    evidence_refs_json,
    next_action,
    retry_count,
    next_eligible_at_unix_ms,
    created_at_unix_ms,
    updated_at_unix_ms
"#;

fn map_objective_attempt_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<ObjectiveContinuationAttemptRecord> {
    let source_run_generation = row.get::<_, i64>(6)?;
    let budget_tokens = row.get::<_, i64>(15)?;
    let decision = row.get::<_, String>(17)?;
    let retry_count = row.get::<_, i64>(22)?;
    Ok(ObjectiveContinuationAttemptRecord {
        attempt_id: row.get(0)?,
        objective_id: row.get(1)?,
        routine_id: row.get(2)?,
        session_id: row.get(3)?,
        root_run_id: row.get(4)?,
        source_run_id: row.get(5)?,
        source_run_generation: u64::try_from(source_run_generation)
            .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(6, source_run_generation))?,
        judge_task_id: row.get(7)?,
        continuation_task_id: row.get(8)?,
        owner_principal: row.get(9)?,
        device_id: row.get(10)?,
        channel: row.get(11)?,
        judge_payload_json: row.get(12)?,
        judge_payload_sha256: row.get(13)?,
        contract_sha256: row.get(14)?,
        budget_tokens: u64::try_from(budget_tokens)
            .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(15, budget_tokens))?,
        state: row.get(16)?,
        decision: ObjectiveContinuationDecision::parse(decision.as_str()).ok_or_else(|| {
            rusqlite::Error::FromSqlConversionFailure(
                17,
                rusqlite::types::Type::Text,
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("invalid objective continuation decision '{decision}'"),
                )),
            )
        })?,
        reason_code: row.get(18)?,
        summary_text: row.get(19)?,
        evidence_refs_json: row.get(20)?,
        next_action: row.get(21)?,
        retry_count: u64::try_from(retry_count)
            .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(22, retry_count))?,
        next_eligible_at_unix_ms: row.get(23)?,
        created_at_unix_ms: row.get(24)?,
        updated_at_unix_ms: row.get(25)?,
    })
}

#[cfg(test)]
fn map_objective_transition_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<ObjectiveAttemptTransition> {
    let decision = row.get::<_, String>(4)?;
    Ok(ObjectiveAttemptTransition {
        transition_id: row.get(0)?,
        attempt_id: row.get(1)?,
        from_state: row.get(2)?,
        to_state: row.get(3)?,
        decision: ObjectiveContinuationDecision::parse(decision.as_str()).ok_or_else(|| {
            rusqlite::Error::FromSqlConversionFailure(
                4,
                rusqlite::types::Type::Text,
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("invalid objective continuation decision '{decision}'"),
                )),
            )
        })?,
        reason_code: row.get(5)?,
        evidence_refs_json: row.get(6)?,
        created_at_unix_ms: row.get(7)?,
    })
}

#[cfg(test)]
mod tests;
