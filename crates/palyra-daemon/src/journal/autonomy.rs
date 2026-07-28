//! Durable suspension and wake admission for parent runs waiting on delegated
//! children. SQLite is authoritative; process-local notifications are only
//! hints emitted after these transactions commit.

use super::*;

const PARENT_SUSPENSION_SCHEMA_VERSION: i64 = 1;
const PARENT_SUSPENSION_MAX_CHILDREN: usize = 32;
const PARENT_SUSPENSION_MAX_TIMEOUT_MS: i64 = 24 * 60 * 60 * 1_000;
const PARENT_WAKE_BUDGET_TOKENS: i64 = 16_384;
const PARENT_SUSPENDED_REASON_CODE: &str = "run.suspended.waiting_child";
const PARENT_WAKE_REASON_CODE: &str = "run.wake.child_terminal";
const PARENT_SUSPENDED_STATE: &str = "suspended_waiting_child";
const PARENT_SUSPENSION_WAITING_STATE: &str = "waiting";
const PARENT_SUSPENSION_WAKE_PENDING_STATE: &str = "wake_pending";
const CHILD_SUBSCRIPTION_WAITING_STATE: &str = "waiting";
const CHILD_SUBSCRIPTION_MATCHED_STATE: &str = "matched";
const CHILD_SUBSCRIPTION_CANCELLED_STATE: &str = "cancelled";
const CHILD_SUBSCRIPTION_EXPIRED_STATE: &str = "expired";

pub(super) const MIGRATION_84_SQL: &str = r#"
    CREATE TABLE IF NOT EXISTS parent_suspensions_v1 (
        suspension_ulid TEXT PRIMARY KEY,
        parent_run_ulid TEXT NOT NULL,
        parent_session_ulid TEXT NOT NULL,
        owner_principal TEXT NOT NULL,
        device_id TEXT NOT NULL,
        channel TEXT,
        parent_generation INTEGER NOT NULL CHECK (parent_generation > 0),
        checkpoint_ref TEXT NOT NULL,
        checkpoint_sha256 TEXT NOT NULL,
        wait_policy TEXT NOT NULL CHECK (wait_policy IN ('all', 'any')),
        state TEXT NOT NULL,
        reason_code TEXT NOT NULL,
        deadline_unix_ms INTEGER NOT NULL,
        wake_intent_ulid TEXT,
        continuation_task_ulid TEXT,
        continuation_run_ulid TEXT,
        schema_version INTEGER NOT NULL DEFAULT 1 CHECK (schema_version = 1),
        created_at_unix_ms INTEGER NOT NULL,
        updated_at_unix_ms INTEGER NOT NULL,
        UNIQUE(parent_run_ulid, parent_generation),
        FOREIGN KEY(parent_run_ulid) REFERENCES orchestrator_runs(run_ulid),
        FOREIGN KEY(parent_session_ulid) REFERENCES orchestrator_sessions(session_ulid)
    );
    CREATE INDEX IF NOT EXISTS idx_parent_suspensions_pending
        ON parent_suspensions_v1(state, deadline_unix_ms, created_at_unix_ms);

    CREATE TABLE IF NOT EXISTS child_wake_subscriptions_v1 (
        subscription_ulid TEXT PRIMARY KEY,
        suspension_ulid TEXT NOT NULL,
        task_ulid TEXT NOT NULL,
        child_run_ulid TEXT,
        expected_task_generation INTEGER NOT NULL CHECK (expected_task_generation >= 0),
        state TEXT NOT NULL,
        terminal_state TEXT,
        terminal_result_sha256 TEXT,
        matched_at_unix_ms INTEGER,
        schema_version INTEGER NOT NULL DEFAULT 1 CHECK (schema_version = 1),
        created_at_unix_ms INTEGER NOT NULL,
        updated_at_unix_ms INTEGER NOT NULL,
        UNIQUE(suspension_ulid, task_ulid),
        FOREIGN KEY(suspension_ulid) REFERENCES parent_suspensions_v1(suspension_ulid),
        FOREIGN KEY(task_ulid) REFERENCES orchestrator_background_tasks(task_ulid)
    );
    CREATE INDEX IF NOT EXISTS idx_child_wake_subscriptions_task
        ON child_wake_subscriptions_v1(task_ulid, state, expected_task_generation);

    CREATE TABLE IF NOT EXISTS parent_wake_intents_v1 (
        wake_intent_ulid TEXT PRIMARY KEY,
        suspension_ulid TEXT NOT NULL UNIQUE,
        source_task_ulid TEXT NOT NULL,
        source_task_generation INTEGER NOT NULL CHECK (source_task_generation >= 0),
        continuation_task_ulid TEXT NOT NULL UNIQUE,
        continuation_run_ulid TEXT NOT NULL UNIQUE,
        reason_code TEXT NOT NULL,
        state TEXT NOT NULL,
        schema_version INTEGER NOT NULL DEFAULT 1 CHECK (schema_version = 1),
        created_at_unix_ms INTEGER NOT NULL,
        updated_at_unix_ms INTEGER NOT NULL,
        FOREIGN KEY(suspension_ulid) REFERENCES parent_suspensions_v1(suspension_ulid),
        FOREIGN KEY(source_task_ulid) REFERENCES orchestrator_background_tasks(task_ulid)
    );
"#;

/// Satisfaction policy for a parent waiting on multiple delegated children.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ParentWaitPolicy {
    All,
    Any,
}

impl ParentWaitPolicy {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Any => "any",
        }
    }

    fn parse(value: &str) -> Result<Self, JournalError> {
        match value {
            "all" => Ok(Self::All),
            "any" => Ok(Self::Any),
            _ => Err(JournalError::InvalidArgument(
                "parent suspension wait policy is invalid".to_owned(),
            )),
        }
    }
}

/// One exact child-task generation that may satisfy a parent suspension.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChildWakeSubscriptionCreateRequest {
    pub task_id: String,
    pub child_run_id: Option<String>,
    pub expected_task_generation: u64,
}

/// Atomic request to checkpoint and suspend one active parent generation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ParentSuspensionCreateRequest {
    pub parent_run_id: String,
    pub parent_session_id: String,
    pub owner_principal: String,
    pub device_id: String,
    pub channel: Option<String>,
    pub wait_policy: ParentWaitPolicy,
    pub timeout_ms: i64,
    pub children: Vec<ChildWakeSubscriptionCreateRequest>,
}

/// Durable parent suspension projection returned to the yield adapter.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ParentSuspensionRecord {
    pub suspension_id: String,
    pub parent_run_id: String,
    pub parent_session_id: String,
    pub parent_generation: u64,
    pub checkpoint_ref: String,
    pub checkpoint_sha256: String,
    pub wait_policy: ParentWaitPolicy,
    pub state: String,
    pub reason_code: String,
    pub deadline_unix_ms: i64,
    pub wake_intent_id: Option<String>,
    pub continuation_task_id: Option<String>,
    pub continuation_run_id: Option<String>,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

/// Result of applying one terminal child-task generation to subscriptions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum ParentSuspensionWakeOutcome {
    NoSubscription,
    Waiting {
        suspension_id: String,
        remaining_children: u64,
    },
    ContinuationQueued {
        suspension_id: String,
        wake_intent_id: String,
        continuation_task_id: String,
        continuation_run_id: String,
    },
    AlreadyQueued {
        suspension_id: String,
        wake_intent_id: String,
        continuation_task_id: String,
        continuation_run_id: String,
    },
}

/// Bounded reconciliation summary used by startup and deadline workers.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ParentSuspensionReconcileReport {
    pub matched_child_count: u64,
    pub continuation_queued_count: u64,
    pub timed_out_count: u64,
}

impl JournalStore {
    /// Atomically persists a safe checkpoint reference and child subscriptions,
    /// marks the parent suspended, then releases its active run generation.
    ///
    /// Repeating the exact parent generation returns its existing suspension.
    ///
    /// # Errors
    /// Returns a typed journal error when scope, generation, task ownership, or
    /// persistence validation fails.
    pub fn suspend_parent_for_children(
        &self,
        request: &ParentSuspensionCreateRequest,
    ) -> Result<ParentSuspensionRecord, JournalError> {
        validate_parent_suspension_request(request)?;
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;

        let session =
            load_orchestrator_session_by_id(&transaction, request.parent_session_id.as_str())?
                .ok_or_else(|| JournalError::SessionNotFound {
                    selector: request.parent_session_id.clone(),
                })?;
        if session.principal != request.owner_principal
            || session.device_id != request.device_id
            || session.channel != request.channel
        {
            return Err(JournalError::InvalidArgument(
                "parent suspension identity is outside the session scope".to_owned(),
            ));
        }
        let (run_session_id, run_state) = transaction
            .query_row(
                "SELECT session_ulid, state FROM orchestrator_runs WHERE run_ulid = ?1",
                params![request.parent_run_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?
            .ok_or_else(|| JournalError::RunNotFound { run_id: request.parent_run_id.clone() })?;
        if run_session_id != request.parent_session_id {
            return Err(JournalError::InvalidArgument(
                "parent suspension run does not belong to the supplied session".to_owned(),
            ));
        }
        if !matches!(run_state.as_str(), "accepted" | "in_progress") {
            return Err(JournalError::InvalidArgument(format!(
                "parent suspension requires an active run, found {run_state}"
            )));
        }
        let generation = shared_runtime::active_runtime_generation_tx(
            &transaction,
            request.parent_session_id.as_str(),
            request.parent_run_id.as_str(),
            RuntimeGenerationLane::Run,
            now,
        )?
        .ok_or_else(|| {
            JournalError::InvalidArgument(
                "parent suspension requires an active run generation".to_owned(),
            )
        })?;
        if let Some(existing) = load_parent_suspension_by_generation_tx(
            &transaction,
            request.parent_run_id.as_str(),
            generation.generation.get(),
        )? {
            transaction.commit()?;
            return Ok(existing);
        }

        for child in &request.children {
            validate_child_subscription_authority_tx(&transaction, request, child)?;
        }
        let tape_seq = transaction.query_row(
            "SELECT COALESCE(MAX(seq), -1) FROM orchestrator_tape WHERE run_ulid = ?1",
            params![request.parent_run_id],
            |row| row.get::<_, i64>(0),
        )?;
        let checkpoint = json!({
            "schema_version": PARENT_SUSPENSION_SCHEMA_VERSION,
            "parent_run_id": request.parent_run_id,
            "parent_session_id": request.parent_session_id,
            "parent_generation": generation.generation.get(),
            "tape_seq": tape_seq,
            "wait_policy": request.wait_policy.as_str(),
            "children": request.children.iter().map(|child| json!({
                "task_id": child.task_id,
                "child_run_id": child.child_run_id,
                "expected_task_generation": child.expected_task_generation,
            })).collect::<Vec<_>>(),
        });
        let checkpoint_json = serde_json::to_vec(&checkpoint)?;
        let checkpoint_sha256 = hex::encode(Sha256::digest(checkpoint_json.as_slice()));
        let checkpoint_ref =
            format!("orchestrator_tape:{}:{}", request.parent_run_id, tape_seq.max(0));
        let suspension_id = Ulid::new().to_string();
        let deadline_unix_ms = now.saturating_add(request.timeout_ms);
        transaction.execute(
            r#"
                INSERT INTO parent_suspensions_v1 (
                    suspension_ulid, parent_run_ulid, parent_session_ulid,
                    owner_principal, device_id, channel, parent_generation,
                    checkpoint_ref, checkpoint_sha256, wait_policy, state,
                    reason_code, deadline_unix_ms, wake_intent_ulid,
                    continuation_task_ulid, continuation_run_ulid,
                    schema_version, created_at_unix_ms, updated_at_unix_ms
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12,
                    ?13, NULL, NULL, NULL, 1, ?14, ?14
                )
            "#,
            params![
                suspension_id,
                request.parent_run_id,
                request.parent_session_id,
                request.owner_principal,
                request.device_id,
                request.channel,
                u64_to_sqlite(generation.generation.get(), "parent_generation")?,
                checkpoint_ref,
                checkpoint_sha256,
                request.wait_policy.as_str(),
                PARENT_SUSPENSION_WAITING_STATE,
                PARENT_SUSPENDED_REASON_CODE,
                deadline_unix_ms,
                now,
            ],
        )?;
        for child in &request.children {
            transaction.execute(
                r#"
                    INSERT INTO child_wake_subscriptions_v1 (
                        subscription_ulid, suspension_ulid, task_ulid,
                        child_run_ulid, expected_task_generation, state,
                        terminal_state, terminal_result_sha256, matched_at_unix_ms,
                        schema_version, created_at_unix_ms, updated_at_unix_ms
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, NULL, NULL, 1, ?7, ?7)
                "#,
                params![
                    Ulid::new().to_string(),
                    suspension_id,
                    child.task_id,
                    child.child_run_id,
                    u64_to_sqlite(child.expected_task_generation, "expected_task_generation")?,
                    CHILD_SUBSCRIPTION_WAITING_STATE,
                    now,
                ],
            )?;
        }
        wait_coordinator::register_wait_barrier_tx(
            &transaction,
            &wait_coordinator::WaitBarrierCreateRequest {
                barrier_id: Ulid::new().to_string(),
                owner_kind: "parent_suspension".to_owned(),
                owner_id: suspension_id.clone(),
                session_id: request.parent_session_id.clone(),
                root_run_id: Some(request.parent_run_id.clone()),
                barrier_kind: wait_coordinator::WaitBarrierKind::DelegationChild,
                source_kind: wait_coordinator::WaitBarrierKind::DelegationChild.as_str().to_owned(),
                source_id: suspension_id.clone(),
                // The specialized M049 transaction still owns the parent task
                // insertion; the generic barrier records and coalesces the
                // same wake without allocating a second continuation.
                wake_decision: wait_coordinator::WakeDecision::DeliveryOnly,
                continuation_prompt: Some(
                    "Continue the suspended parent objective using the durable child completion \
                     evidence."
                        .to_owned(),
                ),
                budget_tokens: PARENT_WAKE_BUDGET_TOKENS.unsigned_abs(),
                attempt_generation: generation.generation.get(),
                wake_at_unix_ms: None,
                expires_at_unix_ms: Some(deadline_unix_ms),
                liveness_probe_json: json!({
                    "schema_version": PARENT_SUSPENSION_SCHEMA_VERSION,
                    "suspension_id": suspension_id,
                    "wait_policy": request.wait_policy.as_str(),
                    "child_count": request.children.len(),
                })
                .to_string(),
                active_hours_json: None,
                stale_policy: "cancel".to_owned(),
                reason_code: PARENT_SUSPENDED_REASON_CODE.to_owned(),
            },
            now,
        )?;
        let updated = transaction.execute(
            r#"
                UPDATE orchestrator_runs
                SET state = ?2, completed_at_unix_ms = NULL, updated_at_unix_ms = ?3
                WHERE run_ulid = ?1 AND session_ulid = ?4 AND state IN ('accepted', 'in_progress')
            "#,
            params![request.parent_run_id, PARENT_SUSPENDED_STATE, now, request.parent_session_id,],
        )?;
        if updated != 1 {
            return Err(JournalError::InvalidArgument(
                "parent run changed while suspension was committed".to_owned(),
            ));
        }
        append_run_lifecycle_event_tx(
            &transaction,
            &RunLifecycleEventAppendRequest {
                event_id: Ulid::new().to_string(),
                run_id: request.parent_run_id.clone(),
                session_id: request.parent_session_id.clone(),
                from_state: Some(RunLifecyclePhase::Running),
                to_state: RunLifecyclePhase::Paused,
                actor: RuntimeActorRef {
                    kind: RuntimeActorKind::System,
                    id: "system:parent-suspension".to_owned(),
                },
                correlation_id: suspension_id.clone(),
                parent_run_id: None,
                idempotency_key: Some(format!(
                    "parent-suspend:{}:{}",
                    request.parent_run_id,
                    generation.generation.get()
                )),
                reason: PARENT_SUSPENDED_REASON_CODE.to_owned(),
                payload_json: json!({
                    "schema_version": PARENT_SUSPENSION_SCHEMA_VERSION,
                    "suspension_id": suspension_id,
                    "checkpoint_ref": checkpoint_ref,
                    "checkpoint_sha256": checkpoint_sha256,
                    "wait_policy": request.wait_policy.as_str(),
                    "child_count": request.children.len(),
                    "deadline_unix_ms": deadline_unix_ms,
                })
                .to_string(),
            },
            now,
        )?;
        invalidate_runtime_generation_tx(
            &transaction,
            &RuntimeGenerationInvalidateRequest {
                session_id: request.parent_session_id.clone(),
                run_id: Some(request.parent_run_id.clone()),
                lane: RuntimeGenerationLane::Run,
                transition_kind: RuntimeGenerationTransitionKind::Released,
                reason_code: PARENT_SUSPENDED_REASON_CODE.to_owned(),
            },
            now,
        )?;
        let record = load_parent_suspension_by_generation_tx(
            &transaction,
            request.parent_run_id.as_str(),
            generation.generation.get(),
        )?
        .ok_or_else(|| {
            JournalError::InvalidArgument(
                "parent suspension disappeared during its transaction".to_owned(),
            )
        })?;
        transaction.commit()?;
        Ok(record)
    }

    /// Matches one terminal child task against durable subscriptions and
    /// atomically queues at most one continuation for each satisfied parent.
    ///
    /// # Errors
    /// Returns a journal error if completion evidence cannot be read or the
    /// wake admission transaction fails.
    pub fn settle_parent_suspensions_for_child(
        &self,
        task_id: &str,
    ) -> Result<Vec<ParentSuspensionWakeOutcome>, JournalError> {
        if task_id.trim().is_empty() || task_id.len() > 256 {
            return Err(JournalError::InvalidArgument(
                "child wake task id must be 1..=256 bytes".to_owned(),
            ));
        }
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let task = load_background_task_tx(&transaction, task_id)?
            .ok_or_else(|| JournalError::BackgroundTaskNotFound { task_id: task_id.to_owned() })?;
        if !AuxiliaryTaskState::from_str(task.state.as_str()).is_some_and(|state| {
            matches!(
                state,
                AuxiliaryTaskState::Succeeded
                    | AuxiliaryTaskState::Failed
                    | AuxiliaryTaskState::Cancelled
                    | AuxiliaryTaskState::Expired
            )
        }) {
            return Err(JournalError::InvalidArgument(
                "child wake settlement requires a terminal background task".to_owned(),
            ));
        }
        let result_sha256 =
            task.result_json.as_deref().map(|value| hex::encode(Sha256::digest(value.as_bytes())));
        let subscription_rows =
            load_waiting_subscription_ids_tx(&transaction, task_id, task.execution_generation)?;
        if subscription_rows.is_empty() {
            transaction.commit()?;
            return Ok(vec![ParentSuspensionWakeOutcome::NoSubscription]);
        }
        let mut outcomes = Vec::with_capacity(subscription_rows.len());
        for (suspension_id, subscription_id) in subscription_rows {
            transaction.execute(
                r#"
                    UPDATE child_wake_subscriptions_v1
                    SET state = ?2, terminal_state = ?3, terminal_result_sha256 = ?4,
                        matched_at_unix_ms = ?5, updated_at_unix_ms = ?5
                    WHERE subscription_ulid = ?1
                      AND state = ?6
                      AND expected_task_generation = ?7
                "#,
                params![
                    subscription_id,
                    CHILD_SUBSCRIPTION_MATCHED_STATE,
                    task.state,
                    result_sha256,
                    now,
                    CHILD_SUBSCRIPTION_WAITING_STATE,
                    u64_to_sqlite(task.execution_generation, "execution_generation")?,
                ],
            )?;
            outcomes.push(satisfy_parent_suspension_tx(
                &transaction,
                suspension_id.as_str(),
                &task,
                now,
            )?);
        }
        transaction.commit()?;
        Ok(outcomes)
    }

    /// Reconciles terminal children and expired suspension deadlines from
    /// durable state. Safe to repeat after a crash or timer duplication.
    ///
    /// # Errors
    /// Returns a journal error when reconciliation cannot complete atomically.
    pub fn reconcile_parent_suspensions(
        &self,
    ) -> Result<ParentSuspensionReconcileReport, JournalError> {
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let terminal_tasks = load_terminal_subscription_tasks_tx(&transaction)?;
        let mut report = ParentSuspensionReconcileReport::default();
        for task in terminal_tasks {
            let rows = load_waiting_subscription_ids_tx(
                &transaction,
                task.task_id.as_str(),
                task.execution_generation,
            )?;
            let result_sha256 = task
                .result_json
                .as_deref()
                .map(|value| hex::encode(Sha256::digest(value.as_bytes())));
            for (suspension_id, subscription_id) in rows {
                let updated = transaction.execute(
                    r#"
                        UPDATE child_wake_subscriptions_v1
                        SET state = ?2, terminal_state = ?3,
                            terminal_result_sha256 = ?4, matched_at_unix_ms = ?5,
                            updated_at_unix_ms = ?5
                        WHERE subscription_ulid = ?1 AND state = ?6
                    "#,
                    params![
                        subscription_id,
                        CHILD_SUBSCRIPTION_MATCHED_STATE,
                        task.state,
                        result_sha256,
                        now,
                        CHILD_SUBSCRIPTION_WAITING_STATE,
                    ],
                )?;
                if updated == 0 {
                    continue;
                }
                report.matched_child_count = report.matched_child_count.saturating_add(1);
                if matches!(
                    satisfy_parent_suspension_tx(&transaction, suspension_id.as_str(), &task, now,)?,
                    ParentSuspensionWakeOutcome::ContinuationQueued { .. }
                ) {
                    report.continuation_queued_count =
                        report.continuation_queued_count.saturating_add(1);
                }
            }
        }

        let expired_ids = {
            let mut statement = transaction.prepare(
                r#"
                    SELECT suspension_ulid, parent_run_ulid, parent_session_ulid
                    FROM parent_suspensions_v1
                    WHERE state = ?1 AND deadline_unix_ms <= ?2
                    ORDER BY deadline_unix_ms ASC, suspension_ulid ASC
                    LIMIT 256
                "#,
            )?;
            let rows =
                statement.query_map(params![PARENT_SUSPENSION_WAITING_STATE, now], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })?;
            rows.collect::<Result<Vec<_>, _>>()?
        };
        for (suspension_id, parent_run_id, parent_session_id) in expired_ids {
            transaction.execute(
                r#"
                    UPDATE parent_suspensions_v1
                    SET state = 'timed_out', reason_code = 'run.suspension.timed_out',
                        updated_at_unix_ms = ?2
                    WHERE suspension_ulid = ?1 AND state = ?3
                "#,
                params![suspension_id, now, PARENT_SUSPENSION_WAITING_STATE],
            )?;
            transaction.execute(
                r#"
                    UPDATE child_wake_subscriptions_v1
                    SET state = ?2, updated_at_unix_ms = ?3
                    WHERE suspension_ulid = ?1 AND state = ?4
                "#,
                params![
                    suspension_id,
                    CHILD_SUBSCRIPTION_EXPIRED_STATE,
                    now,
                    CHILD_SUBSCRIPTION_WAITING_STATE,
                ],
            )?;
            let updated_run = transaction.execute(
                r#"
                    UPDATE orchestrator_runs
                    SET state = 'failed', completed_at_unix_ms = ?2,
                        updated_at_unix_ms = ?2,
                        last_error = 'run.suspension.timed_out'
                    WHERE run_ulid = ?1 AND state = ?3
                "#,
                params![parent_run_id, now, PARENT_SUSPENDED_STATE],
            )?;
            if updated_run == 1 {
                append_run_lifecycle_event_tx(
                    &transaction,
                    &RunLifecycleEventAppendRequest {
                        event_id: Ulid::new().to_string(),
                        run_id: parent_run_id,
                        session_id: parent_session_id,
                        from_state: Some(RunLifecyclePhase::Paused),
                        to_state: RunLifecyclePhase::Expired,
                        actor: RuntimeActorRef {
                            kind: RuntimeActorKind::System,
                            id: "system:parent-suspension-deadline".to_owned(),
                        },
                        correlation_id: suspension_id.clone(),
                        parent_run_id: None,
                        idempotency_key: Some(format!("parent-timeout:{suspension_id}")),
                        reason: "run.suspension.timed_out".to_owned(),
                        payload_json: json!({
                            "schema_version": PARENT_SUSPENSION_SCHEMA_VERSION,
                            "suspension_id": suspension_id,
                            "outcome": "timed_out",
                        })
                        .to_string(),
                    },
                    now,
                )?;
            }
            report.timed_out_count = report.timed_out_count.saturating_add(1);
        }
        transaction.commit()?;
        Ok(report)
    }
}

pub(super) fn settle_parent_suspension_for_parent_terminal_tx(
    connection: &Connection,
    parent_run_id: &str,
    terminal_state: RunLifecycleState,
    now: i64,
) -> Result<(), JournalError> {
    let (state, reason_code, subscription_state) = match terminal_state {
        RunLifecycleState::Cancelled => {
            ("cancelled", "run.suspension.parent_cancelled", CHILD_SUBSCRIPTION_CANCELLED_STATE)
        }
        RunLifecycleState::Failed => {
            ("orphaned", "run.suspension.parent_failed", CHILD_SUBSCRIPTION_CANCELLED_STATE)
        }
        RunLifecycleState::Done => {
            ("closed", "run.suspension.parent_closed", CHILD_SUBSCRIPTION_CANCELLED_STATE)
        }
        RunLifecycleState::Pending
        | RunLifecycleState::Accepted
        | RunLifecycleState::InProgress => return Ok(()),
    };
    let suspension_ids = {
        let mut statement = connection.prepare(
            "SELECT suspension_ulid FROM parent_suspensions_v1 WHERE parent_run_ulid = ?1 AND state = ?2",
        )?;
        let rows = statement
            .query_map(params![parent_run_id, PARENT_SUSPENSION_WAITING_STATE], |row| {
                row.get::<_, String>(0)
            })?;
        rows.collect::<Result<Vec<_>, _>>()?
    };
    for suspension_id in suspension_ids {
        connection.execute(
            r#"
                UPDATE parent_suspensions_v1
                SET state = ?2, reason_code = ?3, updated_at_unix_ms = ?4
                WHERE suspension_ulid = ?1 AND state = ?5
            "#,
            params![suspension_id, state, reason_code, now, PARENT_SUSPENSION_WAITING_STATE,],
        )?;
        connection.execute(
            r#"
                UPDATE child_wake_subscriptions_v1
                SET state = ?2, updated_at_unix_ms = ?3
                WHERE suspension_ulid = ?1 AND state = ?4
            "#,
            params![suspension_id, subscription_state, now, CHILD_SUBSCRIPTION_WAITING_STATE,],
        )?;
    }
    Ok(())
}

fn validate_parent_suspension_request(
    request: &ParentSuspensionCreateRequest,
) -> Result<(), JournalError> {
    for (field, value, max_len) in [
        ("parent_run_id", request.parent_run_id.as_str(), 256),
        ("parent_session_id", request.parent_session_id.as_str(), 128),
        ("owner_principal", request.owner_principal.as_str(), 256),
        ("device_id", request.device_id.as_str(), 256),
    ] {
        if value.trim().is_empty() || value.len() > max_len {
            return Err(JournalError::InvalidArgument(format!(
                "parent suspension {field} must be 1..={max_len} bytes"
            )));
        }
    }
    if request.children.is_empty() || request.children.len() > PARENT_SUSPENSION_MAX_CHILDREN {
        return Err(JournalError::InvalidArgument(format!(
            "parent suspension requires 1..={PARENT_SUSPENSION_MAX_CHILDREN} children"
        )));
    }
    if !(1..=PARENT_SUSPENSION_MAX_TIMEOUT_MS).contains(&request.timeout_ms) {
        return Err(JournalError::InvalidArgument(format!(
            "parent suspension timeout must be 1..={PARENT_SUSPENSION_MAX_TIMEOUT_MS} ms"
        )));
    }
    let mut task_ids = BTreeSet::new();
    for child in &request.children {
        if child.task_id.trim().is_empty()
            || child.task_id.len() > 256
            || !task_ids.insert(child.task_id.as_str())
        {
            return Err(JournalError::InvalidArgument(
                "parent suspension child task ids must be unique and 1..=256 bytes".to_owned(),
            ));
        }
        if child
            .child_run_id
            .as_deref()
            .is_some_and(|run_id| run_id.trim().is_empty() || run_id.len() > 256)
        {
            return Err(JournalError::InvalidArgument(
                "parent suspension child run id must be 1..=256 bytes".to_owned(),
            ));
        }
    }
    Ok(())
}

fn validate_child_subscription_authority_tx(
    connection: &Connection,
    request: &ParentSuspensionCreateRequest,
    child: &ChildWakeSubscriptionCreateRequest,
) -> Result<(), JournalError> {
    let task = load_background_task_tx(connection, child.task_id.as_str())?
        .ok_or_else(|| JournalError::BackgroundTaskNotFound { task_id: child.task_id.clone() })?;
    if task.session_id != request.parent_session_id
        || task.parent_run_id.as_deref() != Some(request.parent_run_id.as_str())
        || task.owner_principal != request.owner_principal
        || task.device_id != request.device_id
        || task.channel != request.channel
    {
        return Err(JournalError::InvalidArgument(
            "child wake subscription is outside the parent authority".to_owned(),
        ));
    }
    if task.execution_generation != child.expected_task_generation {
        return Err(JournalError::InvalidArgument(
            "child wake subscription task generation is stale".to_owned(),
        ));
    }
    let durable_child_run_id =
        task.target_run_id.as_deref().or(task.planned_child_run_id.as_deref());
    if child.child_run_id.as_deref() != durable_child_run_id {
        return Err(JournalError::InvalidArgument(
            "child wake subscription run identity does not match durable task evidence".to_owned(),
        ));
    }
    if AuxiliaryTaskState::from_str(task.state.as_str()).is_some_and(|state| {
        matches!(
            state,
            AuxiliaryTaskState::Succeeded
                | AuxiliaryTaskState::Failed
                | AuxiliaryTaskState::Cancelled
                | AuxiliaryTaskState::Expired
        )
    }) {
        return Err(JournalError::InvalidArgument(
            "terminal child tasks must be projected before parent suspension".to_owned(),
        ));
    }
    Ok(())
}

fn load_parent_suspension_by_generation_tx(
    connection: &Connection,
    parent_run_id: &str,
    parent_generation: u64,
) -> Result<Option<ParentSuspensionRecord>, JournalError> {
    connection
        .query_row(
            r#"
                SELECT suspension_ulid, parent_run_ulid, parent_session_ulid,
                       parent_generation, checkpoint_ref, checkpoint_sha256,
                       wait_policy, state, reason_code, deadline_unix_ms,
                       wake_intent_ulid, continuation_task_ulid,
                       continuation_run_ulid, created_at_unix_ms, updated_at_unix_ms
                FROM parent_suspensions_v1
                WHERE parent_run_ulid = ?1 AND parent_generation = ?2
            "#,
            params![parent_run_id, u64_to_sqlite(parent_generation, "parent_generation")?],
            map_parent_suspension_row,
        )
        .optional()
        .map_err(JournalError::from)
}

fn map_parent_suspension_row(row: &Row<'_>) -> rusqlite::Result<ParentSuspensionRecord> {
    let parent_generation = row.get::<_, i64>(3)?;
    let wait_policy =
        ParentWaitPolicy::parse(row.get::<_, String>(6)?.as_str()).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                6,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?;
    Ok(ParentSuspensionRecord {
        suspension_id: row.get(0)?,
        parent_run_id: row.get(1)?,
        parent_session_id: row.get(2)?,
        parent_generation: parent_generation.max(0) as u64,
        checkpoint_ref: row.get(4)?,
        checkpoint_sha256: row.get(5)?,
        wait_policy,
        state: row.get(7)?,
        reason_code: row.get(8)?,
        deadline_unix_ms: row.get(9)?,
        wake_intent_id: row.get(10)?,
        continuation_task_id: row.get(11)?,
        continuation_run_id: row.get(12)?,
        created_at_unix_ms: row.get(13)?,
        updated_at_unix_ms: row.get(14)?,
    })
}

fn load_waiting_subscription_ids_tx(
    connection: &Connection,
    task_id: &str,
    task_generation: u64,
) -> Result<Vec<(String, String)>, JournalError> {
    let mut statement = connection.prepare(
        r#"
            SELECT suspension_ulid, subscription_ulid
            FROM child_wake_subscriptions_v1
            WHERE task_ulid = ?1
              AND expected_task_generation = ?2
              AND state = ?3
            ORDER BY created_at_unix_ms ASC, subscription_ulid ASC
        "#,
    )?;
    let rows = statement.query_map(
        params![
            task_id,
            u64_to_sqlite(task_generation, "execution_generation")?,
            CHILD_SUBSCRIPTION_WAITING_STATE,
        ],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    rows.collect::<Result<Vec<_>, _>>().map_err(JournalError::from)
}

fn load_terminal_subscription_tasks_tx(
    connection: &Connection,
) -> Result<Vec<OrchestratorBackgroundTaskRecord>, JournalError> {
    let task_ids = {
        let mut statement = connection.prepare(
            r#"
                SELECT DISTINCT task.task_ulid
                FROM child_wake_subscriptions_v1 AS subscription
                JOIN orchestrator_background_tasks AS task
                  ON task.task_ulid = subscription.task_ulid
                 AND task.execution_generation = subscription.expected_task_generation
                WHERE subscription.state = ?1
                  AND task.state IN (?2, ?3, ?4, ?5)
                ORDER BY task.task_ulid ASC
                LIMIT 256
            "#,
        )?;
        let rows = statement.query_map(
            params![
                CHILD_SUBSCRIPTION_WAITING_STATE,
                AuxiliaryTaskState::Succeeded.as_str(),
                AuxiliaryTaskState::Failed.as_str(),
                AuxiliaryTaskState::Cancelled.as_str(),
                AuxiliaryTaskState::Expired.as_str(),
            ],
            |row| row.get::<_, String>(0),
        )?;
        rows.collect::<Result<Vec<_>, _>>()?
    };
    task_ids
        .into_iter()
        .map(|task_id| {
            load_background_task_tx(connection, task_id.as_str())?
                .ok_or_else(|| JournalError::BackgroundTaskNotFound { task_id })
        })
        .collect()
}

fn satisfy_parent_suspension_tx(
    connection: &Connection,
    suspension_id: &str,
    source_task: &OrchestratorBackgroundTaskRecord,
    now: i64,
) -> Result<ParentSuspensionWakeOutcome, JournalError> {
    let suspension = connection
        .query_row(
            r#"
                SELECT suspension_ulid, parent_run_ulid, parent_session_ulid,
                       parent_generation, checkpoint_ref, checkpoint_sha256,
                       wait_policy, state, reason_code, deadline_unix_ms,
                       wake_intent_ulid, continuation_task_ulid,
                       continuation_run_ulid, created_at_unix_ms, updated_at_unix_ms
                FROM parent_suspensions_v1
                WHERE suspension_ulid = ?1
            "#,
            params![suspension_id],
            map_parent_suspension_row,
        )
        .optional()?
        .ok_or_else(|| JournalError::InvalidArgument("parent suspension is missing".to_owned()))?;
    if let (Some(wake_intent_id), Some(continuation_task_id), Some(continuation_run_id)) = (
        suspension.wake_intent_id.clone(),
        suspension.continuation_task_id.clone(),
        suspension.continuation_run_id.clone(),
    ) {
        return Ok(ParentSuspensionWakeOutcome::AlreadyQueued {
            suspension_id: suspension.suspension_id,
            wake_intent_id,
            continuation_task_id,
            continuation_run_id,
        });
    }
    let remaining_children = connection.query_row(
        "SELECT COUNT(*) FROM child_wake_subscriptions_v1 WHERE suspension_ulid = ?1 AND state = ?2",
        params![suspension_id, CHILD_SUBSCRIPTION_WAITING_STATE],
        |row| row.get::<_, i64>(0),
    )?;
    let satisfied = suspension.wait_policy == ParentWaitPolicy::Any || remaining_children == 0;
    if !satisfied {
        return Ok(ParentSuspensionWakeOutcome::Waiting {
            suspension_id: suspension.suspension_id,
            remaining_children: remaining_children.max(0) as u64,
        });
    }

    wait_coordinator::emit_wake_event_tx(
        connection,
        &wait_coordinator::WakeEventRequest {
            source_event_id: format!(
                "wake:parent_suspension:{}:{}:{}",
                suspension_id, source_task.task_id, source_task.execution_generation
            ),
            source_kind: wait_coordinator::WaitBarrierKind::DelegationChild.as_str().to_owned(),
            source_id: suspension_id.to_owned(),
            source_generation: suspension.parent_generation,
            reason_code: PARENT_WAKE_REASON_CODE.to_owned(),
            evidence_json: json!({
                "schema_version": PARENT_SUSPENSION_SCHEMA_VERSION,
                "suspension_id": suspension_id,
                "source_task_id": source_task.task_id,
                "source_task_generation": source_task.execution_generation,
                "remaining_children": remaining_children,
            })
            .to_string(),
            occurred_at_unix_ms: now,
        },
    )?;

    let wake_intent_id = Ulid::new().to_string();
    let continuation_task_id = Ulid::new().to_string();
    let continuation_run_id = Ulid::new().to_string();
    connection.execute(
        r#"
            INSERT INTO parent_wake_intents_v1 (
                wake_intent_ulid, suspension_ulid, source_task_ulid,
                source_task_generation, continuation_task_ulid,
                continuation_run_ulid, reason_code, state, schema_version,
                created_at_unix_ms, updated_at_unix_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'queued', 1, ?8, ?8)
            ON CONFLICT(suspension_ulid) DO NOTHING
        "#,
        params![
            wake_intent_id,
            suspension_id,
            source_task.task_id,
            u64_to_sqlite(source_task.execution_generation, "execution_generation")?,
            continuation_task_id,
            continuation_run_id,
            PARENT_WAKE_REASON_CODE,
            now,
        ],
    )?;
    let inserted = connection.execute(
        r#"
            INSERT INTO orchestrator_background_tasks (
                task_ulid, task_kind, session_ulid, child_session_ulid,
                parent_run_ulid, target_run_ulid, planned_child_run_ulid,
                queued_input_ulid, owner_principal, device_id, channel, state,
                priority, revision, execution_generation, attempt_count,
                max_attempts, budget_tokens, delegation_json,
                cancellation_context_json, not_before_unix_ms,
                expires_at_unix_ms, notification_target_json, input_text,
                payload_json, last_error, result_json, created_at_unix_ms,
                updated_at_unix_ms, started_at_unix_ms, completed_at_unix_ms
            )
            SELECT
                ?1, ?2, parent_session_ulid, NULL, parent_run_ulid, NULL, ?3,
                NULL, owner_principal, device_id, channel, ?4, 100, 0, 0, 0,
                3, ?5, NULL, NULL, NULL, NULL, NULL, ?6, ?7, NULL, NULL,
                ?8, ?8, NULL, NULL
            FROM parent_suspensions_v1
            WHERE suspension_ulid = ?9 AND state = ?10
        "#,
        params![
            continuation_task_id,
            AuxiliaryTaskKind::BackgroundPrompt.as_str(),
            continuation_run_id,
            AuxiliaryTaskState::Queued.as_str(),
            PARENT_WAKE_BUDGET_TOKENS,
            "Continue the suspended parent objective using the durable child completion evidence.",
            json!({
                "schema_version": PARENT_SUSPENSION_SCHEMA_VERSION,
                "kind": "parent_suspension_continuation",
                "suspension_id": suspension_id,
                "checkpoint_ref": suspension.checkpoint_ref,
                "checkpoint_sha256": suspension.checkpoint_sha256,
                "source_task_id": source_task.task_id,
                "source_task_generation": source_task.execution_generation,
            })
            .to_string(),
            now,
            suspension_id,
            PARENT_SUSPENSION_WAITING_STATE,
        ],
    )?;
    if inserted != 1 {
        return load_existing_wake_outcome_tx(connection, suspension_id);
    }
    connection.execute(
        r#"
            UPDATE parent_suspensions_v1
            SET state = ?2, reason_code = ?3, wake_intent_ulid = ?4,
                continuation_task_ulid = ?5, continuation_run_ulid = ?6,
                updated_at_unix_ms = ?7
            WHERE suspension_ulid = ?1 AND state = ?8
        "#,
        params![
            suspension_id,
            PARENT_SUSPENSION_WAKE_PENDING_STATE,
            PARENT_WAKE_REASON_CODE,
            wake_intent_id,
            continuation_task_id,
            continuation_run_id,
            now,
            PARENT_SUSPENSION_WAITING_STATE,
        ],
    )?;
    connection.execute(
        r#"
            UPDATE orchestrator_runs
            SET state = 'done', completed_at_unix_ms = ?2, updated_at_unix_ms = ?2
            WHERE run_ulid = ?1 AND state = ?3
        "#,
        params![suspension.parent_run_id, now, PARENT_SUSPENDED_STATE],
    )?;
    append_run_lifecycle_event_tx(
        connection,
        &RunLifecycleEventAppendRequest {
            event_id: Ulid::new().to_string(),
            run_id: suspension.parent_run_id.clone(),
            session_id: suspension.parent_session_id.clone(),
            from_state: Some(RunLifecyclePhase::Paused),
            to_state: RunLifecyclePhase::Completed,
            actor: RuntimeActorRef {
                kind: RuntimeActorKind::System,
                id: "system:parent-wake".to_owned(),
            },
            correlation_id: wake_intent_id.clone(),
            parent_run_id: None,
            idempotency_key: Some(format!("parent-wake:{suspension_id}")),
            reason: PARENT_WAKE_REASON_CODE.to_owned(),
            payload_json: json!({
                "schema_version": PARENT_SUSPENSION_SCHEMA_VERSION,
                "suspension_id": suspension_id,
                "wake_intent_id": wake_intent_id,
                "continuation_task_id": continuation_task_id,
                "continuation_run_id": continuation_run_id,
                "source_task_id": source_task.task_id,
                "source_task_generation": source_task.execution_generation,
            })
            .to_string(),
        },
        now,
    )?;
    Ok(ParentSuspensionWakeOutcome::ContinuationQueued {
        suspension_id: suspension.suspension_id,
        wake_intent_id,
        continuation_task_id,
        continuation_run_id,
    })
}

fn load_existing_wake_outcome_tx(
    connection: &Connection,
    suspension_id: &str,
) -> Result<ParentSuspensionWakeOutcome, JournalError> {
    connection
        .query_row(
            r#"
                SELECT wake_intent_ulid, continuation_task_ulid, continuation_run_ulid
                FROM parent_suspensions_v1
                WHERE suspension_ulid = ?1
                  AND wake_intent_ulid IS NOT NULL
                  AND continuation_task_ulid IS NOT NULL
                  AND continuation_run_ulid IS NOT NULL
            "#,
            params![suspension_id],
            |row| {
                Ok(ParentSuspensionWakeOutcome::AlreadyQueued {
                    suspension_id: suspension_id.to_owned(),
                    wake_intent_id: row.get(0)?,
                    continuation_task_id: row.get(1)?,
                    continuation_run_id: row.get(2)?,
                })
            },
        )
        .optional()?
        .ok_or_else(|| {
            JournalError::InvalidArgument(
                "parent wake admission conflicted without durable replay evidence".to_owned(),
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    struct SuspensionFixture {
        _root: tempfile::TempDir,
        store: JournalStore,
        db_path: PathBuf,
        session_id: String,
        parent_run_id: String,
        task: OrchestratorBackgroundTaskRecord,
    }

    fn suspension_fixture() -> SuspensionFixture {
        let root = tempfile::tempdir().expect("temporary journal root should create");
        let db_path = root.path().join("journal.db");
        let store = open_store(db_path.clone());
        let session_id = Ulid::new().to_string();
        let parent_run_id = Ulid::new().to_string();
        store
            .upsert_orchestrator_session(&OrchestratorSessionUpsertRequest {
                session_id: session_id.clone(),
                session_key: session_id.clone(),
                session_label: None,
                principal: "user:phase-six".to_owned(),
                device_id: "device-phase-six".to_owned(),
                channel: Some("cli".to_owned()),
            })
            .expect("parent session should create");
        store
            .start_orchestrator_run(&OrchestratorRunStartRequest {
                run_id: parent_run_id.clone(),
                session_id: session_id.clone(),
                origin_kind: "user".to_owned(),
                origin_run_id: None,
                triggered_by_principal: Some("user:phase-six".to_owned()),
                parameter_delta_json: None,
                delegated_admission: None,
            })
            .expect("parent run should start");
        store
            .update_orchestrator_run_state(
                parent_run_id.as_str(),
                RunLifecycleState::InProgress,
                None,
            )
            .expect("parent run should enter progress");
        let task = store
            .create_orchestrator_background_task(&OrchestratorBackgroundTaskCreateRequest {
                task_id: Ulid::new().to_string(),
                task_kind: AuxiliaryTaskKind::BackgroundPrompt.as_str().to_owned(),
                session_id: session_id.clone(),
                child_session_id: None,
                parent_run_id: Some(parent_run_id.clone()),
                target_run_id: None,
                planned_child_run_id: None,
                queued_input_id: None,
                owner_principal: "user:phase-six".to_owned(),
                device_id: "device-phase-six".to_owned(),
                channel: Some("cli".to_owned()),
                state: AuxiliaryTaskState::Queued.as_str().to_owned(),
                priority: 0,
                max_attempts: 3,
                budget_tokens: 1_024,
                delegation: None,
                cancellation_context: None,
                not_before_unix_ms: None,
                expires_at_unix_ms: None,
                notification_target_json: None,
                input_text: Some("child work".to_owned()),
                payload_json: None,
            })
            .expect("child task should create");
        let task = store
            .claim_orchestrator_background_task(&OrchestratorBackgroundTaskClaimRequest {
                task_id: task.task_id,
                expected_revision: task.revision,
                started_at_unix_ms: current_unix_ms().expect("clock should be available"),
            })
            .expect("child task should be claimed");
        SuspensionFixture { _root: root, store, db_path, session_id, parent_run_id, task }
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

    fn suspension_request(fixture: &SuspensionFixture) -> ParentSuspensionCreateRequest {
        ParentSuspensionCreateRequest {
            parent_run_id: fixture.parent_run_id.clone(),
            parent_session_id: fixture.session_id.clone(),
            owner_principal: "user:phase-six".to_owned(),
            device_id: "device-phase-six".to_owned(),
            channel: Some("cli".to_owned()),
            wait_policy: ParentWaitPolicy::All,
            timeout_ms: 30_000,
            children: vec![ChildWakeSubscriptionCreateRequest {
                task_id: fixture.task.task_id.clone(),
                child_run_id: None,
                expected_task_generation: fixture.task.execution_generation,
            }],
        }
    }

    fn complete_child(store: &JournalStore, task: &OrchestratorBackgroundTaskRecord) {
        store
            .update_orchestrator_background_task_from_worker(
                &OrchestratorBackgroundTaskWorkerUpdateRequest {
                    task_id: task.task_id.clone(),
                    execution_generation: task.execution_generation,
                    state: Some(AuxiliaryTaskState::Succeeded.as_str().to_owned()),
                    target_run_id: None,
                    last_error: Some(None),
                    result_json: Some(Some(
                        json!({
                            "schema_version": 1,
                            "status": "succeeded",
                            "evidence_refs": ["artifact:child-result"],
                        })
                        .to_string(),
                    )),
                    started_at_unix_ms: None,
                    completed_at_unix_ms: Some(Some(
                        current_unix_ms().expect("clock should be available"),
                    )),
                },
            )
            .expect("child task should complete");
    }

    #[test]
    fn suspension_releases_lane_and_queues_exactly_one_wake() {
        let fixture = suspension_fixture();
        let request = suspension_request(&fixture);
        let suspension =
            fixture.store.suspend_parent_for_children(&request).expect("parent should suspend");
        assert_eq!(suspension.state, PARENT_SUSPENSION_WAITING_STATE);
        assert_eq!(suspension.parent_generation, 1);

        let next_user_run_id = Ulid::new().to_string();
        fixture
            .store
            .start_orchestrator_run(&OrchestratorRunStartRequest {
                run_id: next_user_run_id.clone(),
                session_id: fixture.session_id.clone(),
                origin_kind: "user".to_owned(),
                origin_run_id: None,
                triggered_by_principal: Some("user:phase-six".to_owned()),
                parameter_delta_json: None,
                delegated_admission: None,
            })
            .expect("suspended parent must not hold the session lane");
        fixture
            .store
            .update_orchestrator_run_state(
                next_user_run_id.as_str(),
                RunLifecycleState::Cancelled,
                Some("test user turn settled"),
            )
            .expect("replacement user run should settle");

        complete_child(&fixture.store, &fixture.task);
        let outcomes = fixture
            .store
            .settle_parent_suspensions_for_child(fixture.task.task_id.as_str())
            .expect("terminal child should wake parent");
        assert!(matches!(
            outcomes.as_slice(),
            [ParentSuspensionWakeOutcome::ContinuationQueued { .. }]
        ));
        let replay = fixture
            .store
            .settle_parent_suspensions_for_child(fixture.task.task_id.as_str())
            .expect("duplicate completion should be harmless");
        assert_eq!(replay, vec![ParentSuspensionWakeOutcome::NoSubscription]);
        let queued_count = fixture
            .store
            .connection
            .lock()
            .expect("journal lock should be available")
            .query_row(
                "SELECT COUNT(*) FROM parent_wake_intents_v1 WHERE suspension_ulid = ?1",
                params![suspension.suspension_id],
                |row| row.get::<_, i64>(0),
            )
            .expect("wake count should load");
        assert_eq!(queued_count, 1);
    }

    #[test]
    fn suspension_survives_restart_before_child_completion() {
        let fixture = suspension_fixture();
        let request = suspension_request(&fixture);
        let suspension =
            fixture.store.suspend_parent_for_children(&request).expect("parent should suspend");
        let task = fixture.task.clone();
        let db_path = fixture.db_path.clone();
        drop(fixture.store);
        let reopened = open_store(db_path);

        complete_child(&reopened, &task);
        let outcomes = reopened
            .settle_parent_suspensions_for_child(task.task_id.as_str())
            .expect("reopened journal should wake parent");
        assert!(matches!(
            outcomes.as_slice(),
            [ParentSuspensionWakeOutcome::ContinuationQueued {
                suspension_id,
                ..
            }] if suspension_id == &suspension.suspension_id
        ));
    }

    #[test]
    fn stale_child_generation_cannot_satisfy_subscription() {
        let fixture = suspension_fixture();
        let mut request = suspension_request(&fixture);
        request.children[0].expected_task_generation =
            request.children[0].expected_task_generation.saturating_add(1);
        let error = fixture
            .store
            .suspend_parent_for_children(&request)
            .expect_err("stale generation must be rejected");
        assert!(error.to_string().contains("generation is stale"));
    }

    #[test]
    fn reconciliation_records_explicit_timeout_outcome() {
        let fixture = suspension_fixture();
        let suspension = fixture
            .store
            .suspend_parent_for_children(&suspension_request(&fixture))
            .expect("parent should suspend");
        fixture
            .store
            .connection
            .lock()
            .expect("journal lock should be available")
            .execute(
                "UPDATE parent_suspensions_v1 SET deadline_unix_ms = 0 WHERE suspension_ulid = ?1",
                params![suspension.suspension_id],
            )
            .expect("test deadline should update");

        let report = fixture
            .store
            .reconcile_parent_suspensions()
            .expect("deadline reconciliation should succeed");
        assert_eq!(report.timed_out_count, 1);
        let state = fixture
            .store
            .connection
            .lock()
            .expect("journal lock should be available")
            .query_row(
                "SELECT state FROM parent_suspensions_v1 WHERE suspension_ulid = ?1",
                params![suspension.suspension_id],
                |row| row.get::<_, String>(0),
            )
            .expect("suspension state should load");
        assert_eq!(state, "timed_out");
    }

    #[test]
    fn cancelling_suspended_parent_closes_child_subscriptions() {
        let fixture = suspension_fixture();
        let suspension = fixture
            .store
            .suspend_parent_for_children(&suspension_request(&fixture))
            .expect("parent should suspend");
        fixture
            .store
            .update_orchestrator_run_state(
                fixture.parent_run_id.as_str(),
                RunLifecycleState::Cancelled,
                Some("user cancelled suspended parent"),
            )
            .expect("suspended parent should cancel");
        let states = fixture
            .store
            .connection
            .lock()
            .expect("journal lock should be available")
            .query_row(
                r#"
                    SELECT suspension.state, subscription.state
                    FROM parent_suspensions_v1 AS suspension
                    JOIN child_wake_subscriptions_v1 AS subscription
                      ON subscription.suspension_ulid = suspension.suspension_ulid
                    WHERE suspension.suspension_ulid = ?1
                "#,
                params![suspension.suspension_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .expect("suspension states should load");
        assert_eq!(states, ("cancelled".to_owned(), "cancelled".to_owned()));
    }
}
