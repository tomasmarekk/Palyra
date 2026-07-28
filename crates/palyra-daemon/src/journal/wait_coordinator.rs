//! Durable typed wait barriers and coalesced wake intents.
//!
//! Event producers write source identity and wake evidence transactionally.
//! The async coordinator only delivers already-authorized durable intents.

use super::*;

mod migration;

const WAKE_SCAN_LIMIT: i64 = 256;
pub(super) const MIGRATION_88_SQL: &str = migration::MIGRATION_88_SQL;

/// Supported durable event sources that can satisfy a wait barrier.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WaitBarrierKind {
    ProcessSession,
    TerminalPid,
    TimeDeadline,
    Approval,
    Webhook,
    FlowStep,
    DelegationChild,
    BackgroundTask,
    ExternalArtifact,
    UserInput,
}

impl WaitBarrierKind {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::ProcessSession => "process_session",
            Self::TerminalPid => "terminal_pid",
            Self::TimeDeadline => "time_deadline",
            Self::Approval => "approval",
            Self::Webhook => "webhook",
            Self::FlowStep => "flow_step",
            Self::DelegationChild => "delegation_child",
            Self::BackgroundTask => "background_task",
            Self::ExternalArtifact => "external_artifact",
            Self::UserInput => "user_input",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "process_session" => Some(Self::ProcessSession),
            "terminal_pid" => Some(Self::TerminalPid),
            "time_deadline" => Some(Self::TimeDeadline),
            "approval" => Some(Self::Approval),
            "webhook" => Some(Self::Webhook),
            "flow_step" => Some(Self::FlowStep),
            "delegation_child" => Some(Self::DelegationChild),
            "background_task" => Some(Self::BackgroundTask),
            "external_artifact" => Some(Self::ExternalArtifact),
            "user_input" => Some(Self::UserInput),
            _ => None,
        }
    }
}

/// Host decision for a coalesced wake intent.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WakeDecision {
    Run,
    Defer,
    Coalesce,
    Cancel,
    DeliveryOnly,
}

impl WakeDecision {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Run => "run",
            Self::Defer => "defer",
            Self::Coalesce => "coalesce",
            Self::Cancel => "cancel",
            Self::DeliveryOnly => "delivery_only",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "run" => Some(Self::Run),
            "defer" => Some(Self::Defer),
            "coalesce" => Some(Self::Coalesce),
            "cancel" => Some(Self::Cancel),
            "delivery_only" => Some(Self::DeliveryOnly),
            _ => None,
        }
    }
}

/// Persistent `WaitBarrierV1` contract.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct WaitBarrierV1 {
    pub(crate) barrier_id: String,
    pub(crate) owner_kind: String,
    pub(crate) owner_id: String,
    pub(crate) session_id: String,
    pub(crate) root_run_id: Option<String>,
    pub(crate) barrier_kind: WaitBarrierKind,
    pub(crate) source_kind: String,
    pub(crate) source_id: String,
    pub(crate) state: String,
    pub(crate) wake_decision: WakeDecision,
    pub(crate) continuation_prompt: Option<String>,
    pub(crate) budget_tokens: u64,
    pub(crate) attempt_generation: u64,
    pub(crate) wake_at_unix_ms: Option<i64>,
    pub(crate) expires_at_unix_ms: Option<i64>,
    pub(crate) liveness_probe_json: String,
    pub(crate) active_hours_json: Option<String>,
    pub(crate) stale_policy: String,
    pub(crate) reason_code: String,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) updated_at_unix_ms: i64,
}

/// Persistent `WakeIntentV1` contract.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct WakeIntentV1 {
    pub(crate) intent_id: String,
    pub(crate) barrier_id: String,
    pub(crate) session_id: String,
    pub(crate) source_kind: String,
    pub(crate) source_id: String,
    pub(crate) source_generation: u64,
    pub(crate) wake_reason: String,
    pub(crate) decision: WakeDecision,
    pub(crate) state: String,
    pub(crate) attempt_generation: u64,
    pub(crate) source_event_count: u64,
    pub(crate) continuation_task_id: Option<String>,
    pub(crate) delivery_outcome: String,
    pub(crate) evidence_json: String,
    pub(crate) next_eligible_at_unix_ms: Option<i64>,
    pub(crate) first_event_at_unix_ms: i64,
    pub(crate) last_event_at_unix_ms: i64,
    pub(crate) delivered_at_unix_ms: Option<i64>,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) updated_at_unix_ms: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct WaitBarrierCreateRequest {
    pub(crate) barrier_id: String,
    pub(crate) owner_kind: String,
    pub(crate) owner_id: String,
    pub(crate) session_id: String,
    pub(crate) root_run_id: Option<String>,
    pub(crate) barrier_kind: WaitBarrierKind,
    pub(crate) source_kind: String,
    pub(crate) source_id: String,
    pub(crate) wake_decision: WakeDecision,
    pub(crate) continuation_prompt: Option<String>,
    pub(crate) budget_tokens: u64,
    pub(crate) attempt_generation: u64,
    pub(crate) wake_at_unix_ms: Option<i64>,
    pub(crate) expires_at_unix_ms: Option<i64>,
    pub(crate) liveness_probe_json: String,
    pub(crate) active_hours_json: Option<String>,
    pub(crate) stale_policy: String,
    pub(crate) reason_code: String,
}

#[derive(Debug, Clone)]
pub(crate) struct WakeEventRequest {
    pub(crate) source_event_id: String,
    pub(crate) source_kind: String,
    pub(crate) source_id: String,
    pub(crate) source_generation: u64,
    pub(crate) reason_code: String,
    pub(crate) evidence_json: String,
    pub(crate) occurred_at_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WakeTaskReserveOutcome {
    Reserved(WakeIntentV1),
    UserPreempted(WakeIntentV1),
}

impl JournalStore {
    /// Creates one replay-stable typed barrier.
    pub(crate) fn register_wait_barrier(
        &self,
        request: &WaitBarrierCreateRequest,
    ) -> Result<WaitBarrierV1, JournalError> {
        validate_barrier_request(request)?;
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let barrier = register_wait_barrier_tx(&transaction, request, now)?;
        transaction.commit()?;
        Ok(barrier)
    }

    /// Coalesces one durable source event into each matching active barrier.
    pub(crate) fn emit_wake_event(
        &self,
        request: &WakeEventRequest,
    ) -> Result<Vec<WakeIntentV1>, JournalError> {
        validate_wake_event(request)?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let intents = emit_wake_event_tx(&transaction, request)?;
        transaction.commit()?;
        Ok(intents)
    }

    /// Converts due deadlines and expired barriers into explicit wake intents.
    pub(crate) fn materialize_due_wait_barriers(
        &self,
        now_unix_ms: i64,
    ) -> Result<Vec<WakeIntentV1>, JournalError> {
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let barriers = {
            let mut statement = transaction.prepare(
                r#"
                    SELECT
                        barrier_ulid, owner_kind, owner_ulid, session_ulid, root_run_ulid,
                        barrier_kind, source_kind, source_id, state, wake_decision,
                        continuation_prompt, budget_tokens, attempt_generation,
                        wake_at_unix_ms, expires_at_unix_ms, liveness_probe_json,
                        active_hours_json, stale_policy, reason_code,
                        created_at_unix_ms, updated_at_unix_ms
                    FROM wait_barriers_v1
                    WHERE state = 'active'
                      AND (
                        (wake_at_unix_ms IS NOT NULL AND wake_at_unix_ms <= ?1)
                        OR (expires_at_unix_ms IS NOT NULL AND expires_at_unix_ms <= ?1)
                      )
                    ORDER BY COALESCE(wake_at_unix_ms, expires_at_unix_ms), barrier_ulid
                    LIMIT 256
                "#,
            )?;
            let rows = statement
                .query_map(params![now_unix_ms], map_wait_barrier_row)?
                .collect::<Result<Vec<_>, _>>()?;
            rows
        };
        let mut intents = Vec::with_capacity(barriers.len());
        for barrier in barriers {
            let expired = barrier.expires_at_unix_ms.is_some_and(|value| value <= now_unix_ms)
                && barrier.wake_at_unix_ms.is_none_or(|value| value > now_unix_ms);
            let decision = if expired {
                match barrier.stale_policy.as_str() {
                    "wake" => WakeDecision::Run,
                    "defer" => WakeDecision::Defer,
                    _ => WakeDecision::Cancel,
                }
            } else {
                barrier.wake_decision
            };
            let reason_code =
                if expired { "wait.barrier.expired" } else { "wait.barrier.deadline_reached" };
            let event = WakeEventRequest {
                source_event_id: format!("deadline:{}:{now_unix_ms}", barrier.barrier_id),
                source_kind: barrier.source_kind.clone(),
                source_id: barrier.source_id.clone(),
                source_generation: barrier.attempt_generation,
                reason_code: reason_code.to_owned(),
                evidence_json: json!({
                    "schema_version": 1,
                    "barrier_id": barrier.barrier_id,
                    "deadline_unix_ms": barrier.wake_at_unix_ms,
                    "expired": expired,
                })
                .to_string(),
                occurred_at_unix_ms: now_unix_ms,
            };
            intents.push(coalesce_wake_intent_tx(&transaction, &barrier, &event, decision)?);
        }
        transaction.commit()?;
        Ok(intents)
    }

    /// Lists bounded intents ready for coordinator delivery.
    pub(crate) fn ready_wake_intents(
        &self,
        now_unix_ms: i64,
    ) -> Result<Vec<WakeIntentV1>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let query = format!(
            "SELECT {WAKE_INTENT_COLUMNS} FROM wake_intents_v1 \
             WHERE state IN ('pending', 'deferred', 'task_reserved') \
               AND (next_eligible_at_unix_ms IS NULL OR next_eligible_at_unix_ms <= ?1) \
             ORDER BY first_event_at_unix_ms, intent_ulid LIMIT {WAKE_SCAN_LIMIT}"
        );
        let mut statement = guard.prepare(query.as_str())?;
        let rows = statement.query_map(params![now_unix_ms], map_wake_intent_row)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(JournalError::from)
    }

    /// Returns the nearest coordinator wake time without polling.
    pub(crate) fn next_wait_coordinator_deadline(&self) -> Result<Option<i64>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        guard
            .query_row(
                r#"
                    SELECT MIN(deadline)
                    FROM (
                        SELECT wake_at_unix_ms AS deadline FROM wait_barriers_v1
                        WHERE state = 'active' AND wake_at_unix_ms IS NOT NULL
                        UNION ALL
                        SELECT expires_at_unix_ms AS deadline FROM wait_barriers_v1
                        WHERE state = 'active' AND expires_at_unix_ms IS NOT NULL
                        UNION ALL
                        SELECT next_eligible_at_unix_ms AS deadline FROM wake_intents_v1
                        WHERE state = 'deferred' AND next_eligible_at_unix_ms IS NOT NULL
                    )
                "#,
                [],
                |row| row.get(0),
            )
            .map_err(JournalError::from)
    }

    /// Reports whether canonical admission still owns an active run in a session.
    pub(crate) fn session_has_active_run(&self, session_id: &str) -> Result<bool, JournalError> {
        validate_wait_text(session_id, "session_id", 128)?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let active = guard.query_row(
            r#"
                SELECT EXISTS(
                    SELECT 1 FROM orchestrator_runs
                    WHERE session_ulid = ?1 AND state IN ('accepted', 'running', 'in_progress')
                )
            "#,
            params![session_id],
            |row| row.get::<_, i64>(0),
        )?;
        Ok(active != 0)
    }

    /// Reports whether accepted user or operator input must preempt autonomous work.
    pub(crate) fn session_has_active_queued_input(
        &self,
        session_id: &str,
    ) -> Result<bool, JournalError> {
        validate_wait_text(session_id, "session_id", 128)?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let active = guard.query_row(
            r#"
                SELECT EXISTS(
                    SELECT 1 FROM orchestrator_queued_inputs
                    WHERE session_ulid = ?1 AND state IN ('pending', 'claimed', 'deferred')
                )
            "#,
            params![session_id],
            |row| row.get::<_, i64>(0),
        )?;
        Ok(active != 0)
    }

    /// Reserves one continuation task under an atomic user-input fence.
    pub(crate) fn reserve_wake_task(
        &self,
        intent_id: &str,
        continuation_task_id: &str,
    ) -> Result<WakeTaskReserveOutcome, JournalError> {
        validate_wait_text(intent_id, "intent_id", 128)?;
        validate_wait_text(continuation_task_id, "continuation_task_id", 128)?;
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let intent = load_wake_intent_tx(&transaction, intent_id)?.ok_or_else(|| {
            JournalError::InvalidArgument("wake intent does not exist".to_owned())
        })?;
        if let Some(existing) = intent.continuation_task_id.as_deref() {
            if existing != continuation_task_id {
                return Err(JournalError::InvalidArgument(
                    "wake continuation task identity already committed".to_owned(),
                ));
            }
            transaction.commit()?;
            return Ok(WakeTaskReserveOutcome::Reserved(intent));
        }
        if intent.state == "cancelled" && intent.delivery_outcome == "user_preempted" {
            transaction.commit()?;
            return Ok(WakeTaskReserveOutcome::UserPreempted(intent));
        }
        if !matches!(intent.state.as_str(), "pending" | "deferred") {
            return Err(JournalError::InvalidArgument(format!(
                "wake task cannot reserve from state '{}'",
                intent.state
            )));
        }
        let user_input_pending = transaction.query_row(
            r#"
                SELECT EXISTS(
                    SELECT 1 FROM orchestrator_queued_inputs
                    WHERE session_ulid = ?1
                      AND state IN ('pending', 'claimed', 'deferred')
                )
            "#,
            params![intent.session_id],
            |row| row.get::<_, i64>(0),
        )? != 0;
        if user_input_pending {
            transaction.execute(
                r#"
                    UPDATE wake_intents_v1
                    SET decision = 'cancel', state = 'cancelled',
                        delivery_outcome = 'user_preempted',
                        delivered_at_unix_ms = ?2, updated_at_unix_ms = ?2
                    WHERE intent_ulid = ?1
                "#,
                params![intent_id, now],
            )?;
            let cancelled = load_wake_intent_tx(&transaction, intent_id)?.ok_or_else(|| {
                JournalError::InvalidArgument(
                    "cancelled wake intent could not be loaded".to_owned(),
                )
            })?;
            transaction.commit()?;
            return Ok(WakeTaskReserveOutcome::UserPreempted(cancelled));
        }
        transaction.execute(
            r#"
                UPDATE wake_intents_v1
                SET state = 'task_reserved', continuation_task_ulid = ?2,
                    delivery_outcome = 'task_reserved', updated_at_unix_ms = ?3
                WHERE intent_ulid = ?1
            "#,
            params![intent_id, continuation_task_id, now],
        )?;
        let reserved = load_wake_intent_tx(&transaction, intent_id)?.ok_or_else(|| {
            JournalError::InvalidArgument("reserved wake intent could not be loaded".to_owned())
        })?;
        transaction.commit()?;
        Ok(WakeTaskReserveOutcome::Reserved(reserved))
    }

    /// Defers an intent at an explicit host-owned boundary.
    pub(crate) fn defer_wake_intent(
        &self,
        intent_id: &str,
        reason_code: &str,
        next_eligible_at_unix_ms: i64,
    ) -> Result<WakeIntentV1, JournalError> {
        self.set_wake_intent_state(
            intent_id,
            "deferred",
            WakeDecision::Defer,
            reason_code,
            Some(next_eligible_at_unix_ms),
            None,
        )
    }

    /// Settles delivery-only, cancelled, expired, or completed wake work.
    pub(crate) fn settle_wake_intent(
        &self,
        intent_id: &str,
        state: &str,
        decision: WakeDecision,
        delivery_outcome: &str,
    ) -> Result<WakeIntentV1, JournalError> {
        if !matches!(state, "delivered" | "cancelled" | "expired") {
            return Err(JournalError::InvalidArgument(
                "wake settlement state must be terminal".to_owned(),
            ));
        }
        self.set_wake_intent_state(
            intent_id,
            state,
            decision,
            delivery_outcome,
            None,
            Some(current_unix_ms()?),
        )
    }

    /// Loads a wake intent by its continuation task.
    pub(crate) fn wake_intent_for_task(
        &self,
        task_id: &str,
    ) -> Result<Option<WakeIntentV1>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let query = format!(
            "SELECT {WAKE_INTENT_COLUMNS} FROM wake_intents_v1 \
             WHERE continuation_task_ulid = ?1"
        );
        guard
            .query_row(query.as_str(), params![task_id], map_wake_intent_row)
            .optional()
            .map_err(JournalError::from)
    }

    /// Loads the barrier owning one intent.
    pub(crate) fn wait_barrier_for_intent(
        &self,
        intent_id: &str,
    ) -> Result<Option<WaitBarrierV1>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        guard
            .query_row(
                r#"
                    SELECT
                        barrier_ulid, owner_kind, owner_ulid, session_ulid, root_run_ulid,
                        barrier_kind, source_kind, source_id, state, wake_decision,
                        continuation_prompt, budget_tokens, attempt_generation,
                        wake_at_unix_ms, expires_at_unix_ms, liveness_probe_json,
                        active_hours_json, stale_policy, reason_code,
                        created_at_unix_ms, updated_at_unix_ms
                    FROM wait_barriers_v1
                    WHERE barrier_ulid = (
                        SELECT barrier_ulid FROM wake_intents_v1 WHERE intent_ulid = ?1
                    )
                "#,
                params![intent_id],
                map_wait_barrier_row,
            )
            .optional()
            .map_err(JournalError::from)
    }

    #[allow(clippy::too_many_arguments)]
    fn set_wake_intent_state(
        &self,
        intent_id: &str,
        state: &str,
        decision: WakeDecision,
        delivery_outcome: &str,
        next_eligible_at_unix_ms: Option<i64>,
        delivered_at_unix_ms: Option<i64>,
    ) -> Result<WakeIntentV1, JournalError> {
        validate_wait_text(delivery_outcome, "delivery_outcome", 128)?;
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = load_wake_intent_tx(&transaction, intent_id)?.ok_or_else(|| {
            JournalError::InvalidArgument("wake intent does not exist".to_owned())
        })?;
        if matches!(current.state.as_str(), "delivered" | "cancelled" | "expired") {
            transaction.commit()?;
            return Ok(current);
        }
        transaction.execute(
            r#"
                UPDATE wake_intents_v1
                SET state = ?2, decision = ?3, delivery_outcome = ?4,
                    next_eligible_at_unix_ms = ?5, delivered_at_unix_ms = ?6,
                    updated_at_unix_ms = ?7
                WHERE intent_ulid = ?1
            "#,
            params![
                intent_id,
                state,
                decision.as_str(),
                delivery_outcome,
                next_eligible_at_unix_ms,
                delivered_at_unix_ms,
                now,
            ],
        )?;
        if matches!(state, "delivered" | "cancelled" | "expired") {
            let barrier_state = match state {
                "delivered" => "satisfied",
                "expired" => "expired",
                _ => "cancelled",
            };
            transaction.execute(
                "UPDATE wait_barriers_v1 SET state = ?2, updated_at_unix_ms = ?3 \
                 WHERE barrier_ulid = ?1 AND state = 'active'",
                params![current.barrier_id, barrier_state, now],
            )?;
        }
        let updated = load_wake_intent_tx(&transaction, intent_id)?.ok_or_else(|| {
            JournalError::InvalidArgument("updated wake intent could not be loaded".to_owned())
        })?;
        transaction.commit()?;
        Ok(updated)
    }
}

fn validate_barrier_request(request: &WaitBarrierCreateRequest) -> Result<(), JournalError> {
    for (field, value, max) in [
        ("barrier_id", request.barrier_id.as_str(), 128),
        ("owner_kind", request.owner_kind.as_str(), 64),
        ("owner_id", request.owner_id.as_str(), 128),
        ("session_id", request.session_id.as_str(), 128),
        ("source_kind", request.source_kind.as_str(), 64),
        ("source_id", request.source_id.as_str(), 256),
        ("stale_policy", request.stale_policy.as_str(), 16),
        ("reason_code", request.reason_code.as_str(), 128),
    ] {
        validate_wait_text(value, field, max)?;
    }
    if !matches!(request.stale_policy.as_str(), "cancel" | "wake" | "defer") {
        return Err(JournalError::InvalidArgument(
            "wait stale_policy must be cancel, wake, or defer".to_owned(),
        ));
    }
    if request.attempt_generation == 0 {
        return Err(JournalError::InvalidArgument(
            "wait attempt_generation must be positive".to_owned(),
        ));
    }
    if let (Some(wake_at), Some(expires_at)) = (request.wake_at_unix_ms, request.expires_at_unix_ms)
    {
        if expires_at < wake_at {
            return Err(JournalError::InvalidArgument(
                "wait barrier expiry cannot precede wake deadline".to_owned(),
            ));
        }
    }
    for (field, value) in [
        ("liveness_probe_json", Some(request.liveness_probe_json.as_str())),
        ("active_hours_json", request.active_hours_json.as_deref()),
    ] {
        if let Some(value) = value {
            if value.len() > 16 * 1_024 {
                return Err(JournalError::PayloadTooLarge {
                    payload_kind: "wait barrier json",
                    actual_bytes: value.len(),
                    max_bytes: 16 * 1_024,
                });
            }
            serde_json::from_str::<Value>(value).map_err(|error| {
                JournalError::InvalidArgument(format!("{field} is invalid: {error}"))
            })?;
        }
    }
    Ok(())
}

fn validate_wake_event(request: &WakeEventRequest) -> Result<(), JournalError> {
    validate_wait_text(&request.source_event_id, "source_event_id", 128)?;
    validate_wait_text(&request.source_kind, "source_kind", 64)?;
    validate_wait_text(&request.source_id, "source_id", 256)?;
    validate_wait_text(&request.reason_code, "reason_code", 128)?;
    if request.source_generation == 0 {
        return Err(JournalError::InvalidArgument(
            "wake source_generation must be positive".to_owned(),
        ));
    }
    if request.evidence_json.len() > 16 * 1_024 {
        return Err(JournalError::PayloadTooLarge {
            payload_kind: "wake evidence",
            actual_bytes: request.evidence_json.len(),
            max_bytes: 16 * 1_024,
        });
    }
    serde_json::from_str::<Value>(&request.evidence_json).map_err(|error| {
        JournalError::InvalidArgument(format!("wake evidence is invalid: {error}"))
    })?;
    Ok(())
}

fn validate_wait_text(
    value: &str,
    field: &'static str,
    max_len: usize,
) -> Result<(), JournalError> {
    if value.trim().is_empty() || value.len() > max_len {
        return Err(JournalError::InvalidArgument(format!("{field} must be 1..={max_len} bytes")));
    }
    Ok(())
}

fn coalesce_wake_intent_tx(
    connection: &Connection,
    barrier: &WaitBarrierV1,
    event: &WakeEventRequest,
    decision: WakeDecision,
) -> Result<WakeIntentV1, JournalError> {
    let intent_id = Ulid::new().to_string();
    let generation = u64_to_sqlite(barrier.attempt_generation, "attempt_generation")?;
    connection.execute(
        r#"
            INSERT INTO wake_intents_v1 (
                intent_ulid, barrier_ulid, session_ulid, source_kind, source_id,
                source_generation, wake_reason, decision, state,
                attempt_generation, source_event_count,
                continuation_task_ulid, delivery_outcome, evidence_json,
                next_eligible_at_unix_ms, first_event_at_unix_ms, last_event_at_unix_ms,
                delivered_at_unix_ms, created_at_unix_ms, updated_at_unix_ms
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'pending', ?9, 1,
                NULL, 'pending', ?10, NULL, ?11, ?11, NULL, ?11, ?11
            )
            ON CONFLICT(barrier_ulid, attempt_generation) DO UPDATE SET
                source_event_count = wake_intents_v1.source_event_count + 1,
                decision = CASE
                    WHEN wake_intents_v1.state IN ('pending', 'deferred') THEN 'coalesce'
                    ELSE wake_intents_v1.decision
                END,
                wake_reason = excluded.wake_reason,
                evidence_json = excluded.evidence_json,
                last_event_at_unix_ms = excluded.last_event_at_unix_ms,
                updated_at_unix_ms = excluded.updated_at_unix_ms
        "#,
        params![
            intent_id,
            barrier.barrier_id,
            barrier.session_id,
            event.source_kind,
            event.source_id,
            u64_to_sqlite(event.source_generation, "source_generation")?,
            event.reason_code,
            decision.as_str(),
            generation,
            event.evidence_json,
            event.occurred_at_unix_ms,
        ],
    )?;
    load_wake_intent_for_barrier_tx(connection, &barrier.barrier_id, barrier.attempt_generation)?
        .ok_or_else(|| {
            JournalError::InvalidArgument("coalesced wake intent could not be loaded".to_owned())
        })
}

#[allow(clippy::too_many_arguments)]
fn append_barrier_event_tx(
    connection: &Connection,
    barrier: &WaitBarrierV1,
    intent_id: Option<&str>,
    event_type: &str,
    reason_code: &str,
    evidence_json: &str,
    now: i64,
) -> Result<(), JournalError> {
    connection.execute(
        r#"
            INSERT INTO wait_barrier_events_v1 (
                event_ulid, barrier_ulid, intent_ulid, event_type, reason_code,
                source_kind, source_id, attempt_generation, evidence_json,
                created_at_unix_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
        "#,
        params![
            Ulid::new().to_string(),
            barrier.barrier_id,
            intent_id,
            event_type,
            reason_code,
            barrier.source_kind,
            barrier.source_id,
            u64_to_sqlite(barrier.attempt_generation, "attempt_generation")?,
            evidence_json,
            now,
        ],
    )?;
    Ok(())
}

fn load_active_barriers_for_source_tx(
    connection: &Connection,
    source_kind: &str,
    source_id: &str,
) -> Result<Vec<WaitBarrierV1>, JournalError> {
    let mut statement = connection.prepare(
        r#"
            SELECT
                barrier_ulid, owner_kind, owner_ulid, session_ulid, root_run_ulid,
                barrier_kind, source_kind, source_id, state, wake_decision,
                continuation_prompt, budget_tokens, attempt_generation,
                wake_at_unix_ms, expires_at_unix_ms, liveness_probe_json,
                active_hours_json, stale_policy, reason_code,
                created_at_unix_ms, updated_at_unix_ms
            FROM wait_barriers_v1
            WHERE state = 'active' AND source_kind = ?1 AND source_id = ?2
            ORDER BY barrier_ulid
            LIMIT 256
        "#,
    )?;
    let barriers = statement
        .query_map(params![source_kind, source_id], map_wait_barrier_row)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(JournalError::from)?;
    Ok(barriers)
}

pub(super) fn register_wait_barrier_tx(
    connection: &Connection,
    request: &WaitBarrierCreateRequest,
    now: i64,
) -> Result<WaitBarrierV1, JournalError> {
    validate_barrier_request(request)?;
    let inserted = connection.execute(
        r#"
            INSERT INTO wait_barriers_v1 (
                barrier_ulid, owner_kind, owner_ulid, session_ulid, root_run_ulid,
                barrier_kind, source_kind, source_id, state, wake_decision,
                continuation_prompt, budget_tokens, attempt_generation,
                wake_at_unix_ms, expires_at_unix_ms, liveness_probe_json,
                active_hours_json, stale_policy, reason_code,
                created_at_unix_ms, updated_at_unix_ms
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'active', ?9,
                ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?19
            )
            ON CONFLICT(
                owner_kind, owner_ulid, barrier_kind, source_kind, source_id, attempt_generation
            ) DO NOTHING
        "#,
        params![
            request.barrier_id,
            request.owner_kind,
            request.owner_id,
            request.session_id,
            request.root_run_id,
            request.barrier_kind.as_str(),
            request.source_kind,
            request.source_id,
            request.wake_decision.as_str(),
            request.continuation_prompt,
            u64_to_sqlite(request.budget_tokens, "budget_tokens")?,
            u64_to_sqlite(request.attempt_generation, "attempt_generation")?,
            request.wake_at_unix_ms,
            request.expires_at_unix_ms,
            request.liveness_probe_json,
            request.active_hours_json,
            request.stale_policy,
            request.reason_code,
            now,
        ],
    )?;
    let barrier = load_wait_barrier_by_owner_tx(
        connection,
        &request.owner_kind,
        &request.owner_id,
        request.barrier_kind,
        &request.source_kind,
        &request.source_id,
        request.attempt_generation,
    )?
    .ok_or_else(|| {
        JournalError::InvalidArgument("registered wait barrier could not be loaded".to_owned())
    })?;
    if barrier.session_id != request.session_id
        || barrier.root_run_id != request.root_run_id
        || barrier.wake_decision != request.wake_decision
    {
        return Err(JournalError::InvalidArgument(
            "wait barrier replay conflicts with committed authority".to_owned(),
        ));
    }
    if inserted == 1 {
        append_barrier_event_tx(
            connection,
            &barrier,
            None,
            "barrier_registered",
            &request.reason_code,
            &request.liveness_probe_json,
            now,
        )?;
        if let Some(event) = load_latest_wake_source_event_tx(
            connection,
            request.source_kind.as_str(),
            request.source_id.as_str(),
        )? {
            let intent =
                coalesce_wake_intent_tx(connection, &barrier, &event, barrier.wake_decision)?;
            append_barrier_event_tx(
                connection,
                &barrier,
                Some(intent.intent_id.as_str()),
                "wake_event_recovered",
                event.reason_code.as_str(),
                event.evidence_json.as_str(),
                now,
            )?;
        }
    }
    Ok(barrier)
}

pub(super) fn record_wake_source_tx(
    connection: &Connection,
    request: &WakeEventRequest,
) -> Result<bool, JournalError> {
    validate_wake_event(request)?;
    let created_at_unix_ms = current_unix_ms()?;
    let inserted = connection.execute(
        r#"
            INSERT INTO wake_source_events_v1 (
                source_event_ulid, source_kind, source_id, source_generation,
                reason_code, evidence_json, occurred_at_unix_ms, created_at_unix_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            ON CONFLICT(source_event_ulid) DO NOTHING
        "#,
        params![
            request.source_event_id,
            request.source_kind,
            request.source_id,
            u64_to_sqlite(request.source_generation, "source_generation")?,
            request.reason_code,
            request.evidence_json,
            request.occurred_at_unix_ms,
            created_at_unix_ms,
        ],
    )?;
    Ok(inserted == 1)
}

pub(super) fn emit_wake_event_tx(
    connection: &Connection,
    request: &WakeEventRequest,
) -> Result<Vec<WakeIntentV1>, JournalError> {
    let inserted = record_wake_source_tx(connection, request)?;
    if !inserted {
        return load_wake_intents_for_source_tx(
            connection,
            &request.source_kind,
            &request.source_id,
        );
    }
    let barriers =
        load_active_barriers_for_source_tx(connection, &request.source_kind, &request.source_id)?;
    let mut intents = Vec::with_capacity(barriers.len());
    for barrier in barriers {
        let intent = coalesce_wake_intent_tx(connection, &barrier, request, barrier.wake_decision)?;
        append_barrier_event_tx(
            connection,
            &barrier,
            Some(&intent.intent_id),
            "wake_event_coalesced",
            &request.reason_code,
            &request.evidence_json,
            request.occurred_at_unix_ms,
        )?;
        intents.push(intent);
    }
    Ok(intents)
}

fn load_latest_wake_source_event_tx(
    connection: &Connection,
    source_kind: &str,
    source_id: &str,
) -> Result<Option<WakeEventRequest>, JournalError> {
    connection
        .query_row(
            r#"
                SELECT source_event_ulid, source_kind, source_id, source_generation,
                       reason_code, evidence_json, occurred_at_unix_ms
                FROM wake_source_events_v1
                WHERE source_kind = ?1 AND source_id = ?2
                ORDER BY occurred_at_unix_ms DESC, source_event_ulid DESC
                LIMIT 1
            "#,
            params![source_kind, source_id],
            |row| {
                Ok(WakeEventRequest {
                    source_event_id: row.get(0)?,
                    source_kind: row.get(1)?,
                    source_id: row.get(2)?,
                    source_generation: integer_to_u64(row, 3, "source_generation")?,
                    reason_code: row.get(4)?,
                    evidence_json: row.get(5)?,
                    occurred_at_unix_ms: row.get(6)?,
                })
            },
        )
        .optional()
        .map_err(JournalError::from)
}

fn load_wake_intents_for_source_tx(
    connection: &Connection,
    source_kind: &str,
    source_id: &str,
) -> Result<Vec<WakeIntentV1>, JournalError> {
    let query = format!(
        "SELECT {WAKE_INTENT_COLUMNS} FROM wake_intents_v1 \
         WHERE source_kind = ?1 AND source_id = ?2 ORDER BY intent_ulid"
    );
    let mut statement = connection.prepare(query.as_str())?;
    let intents = statement
        .query_map(params![source_kind, source_id], map_wake_intent_row)?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(intents)
}

#[allow(clippy::too_many_arguments)]
fn load_wait_barrier_by_owner_tx(
    connection: &Connection,
    owner_kind: &str,
    owner_id: &str,
    barrier_kind: WaitBarrierKind,
    source_kind: &str,
    source_id: &str,
    attempt_generation: u64,
) -> Result<Option<WaitBarrierV1>, JournalError> {
    connection
        .query_row(
            r#"
                SELECT
                    barrier_ulid, owner_kind, owner_ulid, session_ulid, root_run_ulid,
                    barrier_kind, source_kind, source_id, state, wake_decision,
                    continuation_prompt, budget_tokens, attempt_generation,
                    wake_at_unix_ms, expires_at_unix_ms, liveness_probe_json,
                    active_hours_json, stale_policy, reason_code,
                    created_at_unix_ms, updated_at_unix_ms
                FROM wait_barriers_v1
                WHERE owner_kind = ?1 AND owner_ulid = ?2 AND barrier_kind = ?3
                  AND source_kind = ?4 AND source_id = ?5 AND attempt_generation = ?6
            "#,
            params![
                owner_kind,
                owner_id,
                barrier_kind.as_str(),
                source_kind,
                source_id,
                u64_to_sqlite(attempt_generation, "attempt_generation")?,
            ],
            map_wait_barrier_row,
        )
        .optional()
        .map_err(JournalError::from)
}

fn load_wake_intent_for_barrier_tx(
    connection: &Connection,
    barrier_id: &str,
    attempt_generation: u64,
) -> Result<Option<WakeIntentV1>, JournalError> {
    let query = format!(
        "SELECT {WAKE_INTENT_COLUMNS} FROM wake_intents_v1 \
         WHERE barrier_ulid = ?1 AND attempt_generation = ?2"
    );
    connection
        .query_row(
            query.as_str(),
            params![barrier_id, u64_to_sqlite(attempt_generation, "attempt_generation")?],
            map_wake_intent_row,
        )
        .optional()
        .map_err(JournalError::from)
}

fn load_wake_intent_tx(
    connection: &Connection,
    intent_id: &str,
) -> Result<Option<WakeIntentV1>, JournalError> {
    let query = format!("SELECT {WAKE_INTENT_COLUMNS} FROM wake_intents_v1 WHERE intent_ulid = ?1");
    connection
        .query_row(query.as_str(), params![intent_id], map_wake_intent_row)
        .optional()
        .map_err(JournalError::from)
}

const WAKE_INTENT_COLUMNS: &str = r#"
    intent_ulid, barrier_ulid, session_ulid, source_kind, source_id,
    source_generation, wake_reason, decision, state,
    attempt_generation, source_event_count,
    continuation_task_ulid, delivery_outcome, evidence_json,
    next_eligible_at_unix_ms, first_event_at_unix_ms, last_event_at_unix_ms,
    delivered_at_unix_ms, created_at_unix_ms, updated_at_unix_ms
"#;

fn map_wait_barrier_row(row: &Row<'_>) -> rusqlite::Result<WaitBarrierV1> {
    let barrier_kind = row.get::<_, String>(5)?;
    let wake_decision = row.get::<_, String>(9)?;
    Ok(WaitBarrierV1 {
        barrier_id: row.get(0)?,
        owner_kind: row.get(1)?,
        owner_id: row.get(2)?,
        session_id: row.get(3)?,
        root_run_id: row.get(4)?,
        barrier_kind: WaitBarrierKind::parse(&barrier_kind).ok_or_else(|| {
            invalid_wait_column(5, format!("invalid barrier kind '{barrier_kind}'"))
        })?,
        source_kind: row.get(6)?,
        source_id: row.get(7)?,
        state: row.get(8)?,
        wake_decision: WakeDecision::parse(&wake_decision).ok_or_else(|| {
            invalid_wait_column(9, format!("invalid wake decision '{wake_decision}'"))
        })?,
        continuation_prompt: row.get(10)?,
        budget_tokens: integer_to_u64(row, 11, "budget_tokens")?,
        attempt_generation: integer_to_u64(row, 12, "attempt_generation")?,
        wake_at_unix_ms: row.get(13)?,
        expires_at_unix_ms: row.get(14)?,
        liveness_probe_json: row.get(15)?,
        active_hours_json: row.get(16)?,
        stale_policy: row.get(17)?,
        reason_code: row.get(18)?,
        created_at_unix_ms: row.get(19)?,
        updated_at_unix_ms: row.get(20)?,
    })
}

fn map_wake_intent_row(row: &Row<'_>) -> rusqlite::Result<WakeIntentV1> {
    let decision = row.get::<_, String>(7)?;
    Ok(WakeIntentV1 {
        intent_id: row.get(0)?,
        barrier_id: row.get(1)?,
        session_id: row.get(2)?,
        source_kind: row.get(3)?,
        source_id: row.get(4)?,
        source_generation: integer_to_u64(row, 5, "source_generation")?,
        wake_reason: row.get(6)?,
        decision: WakeDecision::parse(&decision)
            .ok_or_else(|| invalid_wait_column(7, format!("invalid wake decision '{decision}'")))?,
        state: row.get(8)?,
        attempt_generation: integer_to_u64(row, 9, "attempt_generation")?,
        source_event_count: integer_to_u64(row, 10, "source_event_count")?,
        continuation_task_id: row.get(11)?,
        delivery_outcome: row.get(12)?,
        evidence_json: row.get(13)?,
        next_eligible_at_unix_ms: row.get(14)?,
        first_event_at_unix_ms: row.get(15)?,
        last_event_at_unix_ms: row.get(16)?,
        delivered_at_unix_ms: row.get(17)?,
        created_at_unix_ms: row.get(18)?,
        updated_at_unix_ms: row.get(19)?,
    })
}

fn invalid_wait_column(column: usize, message: String) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        column,
        rusqlite::types::Type::Text,
        Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, message)),
    )
}

#[cfg(test)]
mod tests;
