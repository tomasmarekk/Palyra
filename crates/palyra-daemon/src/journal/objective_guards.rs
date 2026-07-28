//! Cross-run objective budgets, progress guardrails, and durable plan links.
//! Guard evaluation and plan initialization are transaction-owned so replay
//! cannot double-charge consumption or create duplicate model-visible plans.

use super::*;

mod migration;
mod plan;
mod storage;
#[cfg(test)]
mod tests;

use super::objective_continuation::ObjectiveContinuationDecision;
use plan::*;
use storage::*;

pub(super) const MIGRATION_89_SQL: &str = migration::SQL;

const SCHEMA_VERSION: i64 = 1;
const AUTO_PLAN_REASON: &str = "agent.plan.v2_complex_auto_initialized";
const MAX_GUARD_EVIDENCE_BYTES: usize = 256 * 1024;

/// Hard consumption limits and bounded-loop thresholds for one objective.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ObjectiveGuardPolicy {
    pub(crate) max_runs: Option<u64>,
    pub(crate) max_turns: Option<u64>,
    pub(crate) max_provider_calls: Option<u64>,
    pub(crate) max_tokens: Option<u64>,
    pub(crate) max_cost_micros: Option<u64>,
    pub(crate) max_wall_time_ms: Option<u64>,
    pub(crate) max_consecutive_no_progress: u64,
    pub(crate) max_consecutive_identical_plan: u64,
    pub(crate) max_consecutive_tool_error: u64,
    pub(crate) max_consecutive_parse_failures: u64,
    pub(crate) max_verdict_oscillations: u64,
}

impl Default for ObjectiveGuardPolicy {
    fn default() -> Self {
        Self {
            max_runs: None,
            max_turns: None,
            max_provider_calls: None,
            max_tokens: None,
            max_cost_micros: None,
            max_wall_time_ms: None,
            max_consecutive_no_progress: 3,
            max_consecutive_identical_plan: 3,
            max_consecutive_tool_error: 3,
            max_consecutive_parse_failures: 3,
            max_verdict_oscillations: 4,
        }
    }
}

/// Verification state attached to an objective judge observation.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ObjectiveVerificationStatus {
    Unknown,
    NotRequired,
    Verified,
    MissingEvidence,
    MissingArtifacts,
    Failed,
}

impl ObjectiveVerificationStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::NotRequired => "not_required",
            Self::Verified => "verified",
            Self::MissingEvidence => "missing_evidence",
            Self::MissingArtifacts => "missing_artifacts",
            Self::Failed => "failed",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "unknown" => Some(Self::Unknown),
            "not_required" => Some(Self::NotRequired),
            "verified" => Some(Self::Verified),
            "missing_evidence" => Some(Self::MissingEvidence),
            "missing_artifacts" => Some(Self::MissingArtifacts),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

/// One host-projected run observation used to advance the objective ledger.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ObjectiveProgressObservation {
    pub(crate) attempt_id: String,
    pub(crate) objective_id: String,
    pub(crate) session_id: String,
    pub(crate) root_run_id: String,
    pub(crate) source_run_id: String,
    pub(crate) source_run_generation: u64,
    pub(crate) decision: ObjectiveContinuationDecision,
    pub(crate) runs_delta: u64,
    pub(crate) turns_delta: u64,
    pub(crate) provider_calls_delta: u64,
    pub(crate) tokens_delta: u64,
    pub(crate) cost_micros_delta: u64,
    pub(crate) wall_time_ms_delta: u64,
    pub(crate) progress_detected: bool,
    pub(crate) progress_sha256: Option<String>,
    pub(crate) plan_sha256: Option<String>,
    pub(crate) tool_error_sha256: Option<String>,
    pub(crate) parse_failure: bool,
    pub(crate) verification_status: ObjectiveVerificationStatus,
    pub(crate) verification_reason_code: Option<String>,
    pub(crate) verification_evidence_json: String,
    pub(crate) missing_artifacts_json: String,
}

/// Complete transaction input for one replay-keyed guard evaluation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ObjectiveGuardEvaluationRequest {
    pub(crate) policy: ObjectiveGuardPolicy,
    pub(crate) observation: ObjectiveProgressObservation,
}

/// Persistent cross-run consumption and progress state for one objective.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ObjectiveBudgetLedger {
    pub(crate) objective_id: String,
    pub(crate) session_id: String,
    pub(crate) root_run_id: String,
    pub(crate) max_runs: Option<u64>,
    pub(crate) max_turns: Option<u64>,
    pub(crate) max_provider_calls: Option<u64>,
    pub(crate) max_tokens: Option<u64>,
    pub(crate) max_cost_micros: Option<u64>,
    pub(crate) max_wall_time_ms: Option<u64>,
    pub(crate) runs_consumed: u64,
    pub(crate) turns_consumed: u64,
    pub(crate) provider_calls_consumed: u64,
    pub(crate) tokens_consumed: u64,
    pub(crate) cost_micros_consumed: u64,
    pub(crate) wall_time_ms_consumed: u64,
    pub(crate) parse_failures_total: u64,
    pub(crate) consecutive_parse_failures: u64,
    pub(crate) consecutive_no_progress: u64,
    pub(crate) consecutive_identical_plan: u64,
    pub(crate) consecutive_tool_error: u64,
    pub(crate) verdict_oscillations: u64,
    pub(crate) progress_epoch: u64,
    pub(crate) progress_reset_count: u64,
    pub(crate) last_progress_sha256: Option<String>,
    pub(crate) last_plan_sha256: Option<String>,
    pub(crate) last_tool_error_sha256: Option<String>,
    pub(crate) previous_verdict: Option<String>,
    pub(crate) last_verdict: Option<String>,
    pub(crate) paused_reason_code: Option<String>,
    pub(crate) revision: u64,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) updated_at_unix_ms: i64,
}

/// Immutable evidence for one attempt's guard decision.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ObjectiveProgressFingerprint {
    pub(crate) attempt_id: String,
    pub(crate) objective_id: String,
    pub(crate) session_id: String,
    pub(crate) root_run_id: String,
    pub(crate) source_run_id: String,
    pub(crate) source_run_generation: u64,
    pub(crate) request_sha256: String,
    pub(crate) decision: ObjectiveContinuationDecision,
    pub(crate) progress_sha256: Option<String>,
    pub(crate) plan_sha256: Option<String>,
    pub(crate) tool_error_sha256: Option<String>,
    pub(crate) progress_detected: bool,
    pub(crate) parse_failure: bool,
    pub(crate) verification_status: ObjectiveVerificationStatus,
    pub(crate) verification_reason_code: Option<String>,
    pub(crate) verification_evidence_json: String,
    pub(crate) missing_artifacts_json: String,
    pub(crate) disposition: ObjectiveGuardDisposition,
    pub(crate) reason_code: String,
    pub(crate) cumulative_runs: u64,
    pub(crate) cumulative_turns: u64,
    pub(crate) cumulative_provider_calls: u64,
    pub(crate) cumulative_tokens: u64,
    pub(crate) cumulative_cost_micros: u64,
    pub(crate) cumulative_wall_time_ms: u64,
    pub(crate) consecutive_parse_failures: u64,
    pub(crate) consecutive_no_progress: u64,
    pub(crate) consecutive_identical_plan: u64,
    pub(crate) consecutive_tool_error: u64,
    pub(crate) verdict_oscillations: u64,
    pub(crate) progress_epoch: u64,
    pub(crate) created_at_unix_ms: i64,
}

/// Whether the objective may apply its judge decision or must pause safely.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ObjectiveGuardDisposition {
    Proceed,
    Pause,
}

impl ObjectiveGuardDisposition {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Proceed => "proceed",
            Self::Pause => "pause",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "proceed" => Some(Self::Proceed),
            "pause" => Some(Self::Pause),
            _ => None,
        }
    }
}

/// Result of an objective guard evaluation, including its updated ledger.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ObjectiveGuardEvaluation {
    pub(crate) disposition: ObjectiveGuardDisposition,
    pub(crate) reason_code: String,
    pub(crate) replayed: bool,
    pub(crate) ledger: ObjectiveBudgetLedger,
    pub(crate) fingerprint: ObjectiveProgressFingerprint,
}

/// Aggregated operational counters for objective guard behavior.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub(crate) struct ObjectiveGuardDiagnostics {
    pub(crate) objectives_tracked: u64,
    pub(crate) observations_total: u64,
    pub(crate) pauses_total: u64,
    pub(crate) completed_objectives: u64,
    pub(crate) turns_to_completion_total: u64,
    pub(crate) runs_consumed: u64,
    pub(crate) turns_consumed: u64,
    pub(crate) provider_calls_consumed: u64,
    pub(crate) tokens_consumed: u64,
    pub(crate) cost_micros_consumed: u64,
    pub(crate) wall_time_ms_consumed: u64,
    pub(crate) parse_failures_total: u64,
    pub(crate) progress_resets_total: u64,
    pub(crate) auto_plans_total: u64,
    pub(crate) pause_reason_counts: BTreeMap<String, u64>,
}

/// Durable association between a model-visible plan item and objective focus.
///
/// `objective_id` is either a real objective id or the reserved
/// `run:<root_run_id>` scope used by non-objective complex V2 tasks.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct PlanObjectiveLink {
    pub(crate) objective_id: String,
    pub(crate) plan_item_id: String,
    pub(crate) session_id: String,
    pub(crate) root_run_id: String,
    pub(crate) focus: String,
    pub(crate) is_root: bool,
    pub(crate) active: bool,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) updated_at_unix_ms: i64,
}

/// Inputs for atomically initializing an authoritative-V2 complex-task plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V2ComplexPlanEnsureRequest {
    pub(crate) plan_item_id: String,
    pub(crate) objective_id: String,
    pub(crate) session_id: String,
    pub(crate) root_run_id: String,
    pub(crate) source_run_id: String,
    pub(crate) owner_principal: String,
    pub(crate) device_id: String,
    pub(crate) channel: Option<String>,
    pub(crate) actor_principal: String,
    pub(crate) title: String,
    pub(crate) focus: String,
}

/// Idempotent result of authoritative-V2 complex-task plan initialization.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct V2ComplexPlanEnsureOutcome {
    pub(crate) created: bool,
    pub(crate) plan_item: AgentPlanItemRecord,
    pub(crate) link: PlanObjectiveLink,
}

#[derive(Debug, Clone)]
struct AttemptIdentity {
    objective_id: String,
    session_id: String,
    root_run_id: String,
    source_run_id: String,
    source_run_generation: u64,
    decision: ObjectiveContinuationDecision,
}

impl JournalStore {
    /// Evaluates and persists one replay-keyed objective guard observation.
    ///
    /// # Errors
    /// Returns a journal error when identities, JSON evidence, limits, or
    /// storage state are invalid.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the direct boundary is retained for journal callers outside atomic judge settlement"
        )
    )]
    pub(crate) fn evaluate_objective_guard(
        &self,
        request: &ObjectiveGuardEvaluationRequest,
    ) -> Result<ObjectiveGuardEvaluation, JournalError> {
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let evaluation = evaluate_objective_guard_tx(&transaction, request, now)?;
        transaction.commit()?;
        Ok(evaluation)
    }

    /// Loads the committed guard evaluation for one attempt.
    ///
    /// # Errors
    /// Returns a journal error when the attempt id is empty or storage fails.
    pub(crate) fn objective_guard_evaluation_for_attempt(
        &self,
        attempt_id: &str,
    ) -> Result<Option<ObjectiveGuardEvaluation>, JournalError> {
        ensure_nonempty_field(attempt_id, "attempt_id")?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        load_evaluation_tx(&guard, attempt_id, true)
    }

    /// Loads the current cross-run budget ledger for one objective.
    ///
    /// # Errors
    /// Returns a journal error when the objective id is empty or storage fails.
    #[cfg(test)]
    pub(crate) fn objective_budget_ledger(
        &self,
        objective_id: &str,
    ) -> Result<Option<ObjectiveBudgetLedger>, JournalError> {
        ensure_nonempty_field(objective_id, "objective_id")?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        load_ledger_tx(&guard, objective_id)
    }

    /// Lists immutable progress fingerprints for one objective.
    ///
    /// # Errors
    /// Returns a journal error when the objective id is empty, a persisted
    /// record is corrupt, or storage fails.
    #[cfg(test)]
    pub(crate) fn objective_progress_fingerprints(
        &self,
        objective_id: &str,
        limit: usize,
    ) -> Result<Vec<ObjectiveProgressFingerprint>, JournalError> {
        ensure_nonempty_field(objective_id, "objective_id")?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let mut statement = guard.prepare(
            r#"
                SELECT
                    attempt_ulid, objective_ulid, session_ulid, root_run_ulid,
                    source_run_ulid, source_run_generation, request_sha256, verdict,
                    progress_sha256, plan_sha256, tool_error_sha256, progress_detected,
                    parse_failure, verification_status, verification_reason_code,
                    verification_evidence_json, missing_artifacts_json, disposition,
                    reason_code, cumulative_runs, cumulative_turns,
                    cumulative_provider_calls, cumulative_tokens, cumulative_cost_micros,
                    cumulative_wall_time_ms, consecutive_parse_failures,
                    consecutive_no_progress, consecutive_identical_plan,
                    consecutive_tool_error, verdict_oscillations, progress_epoch,
                    created_at_unix_ms
                FROM objective_progress_fingerprints_v1
                WHERE objective_ulid = ?1
                ORDER BY created_at_unix_ms ASC, attempt_ulid ASC
                LIMIT ?2
            "#,
        )?;
        let limit = i64::try_from(limit.clamp(1, 1_000)).unwrap_or(1_000);
        let rows = statement.query_map(params![objective_id, limit], map_fingerprint_row)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(JournalError::from)
    }

    /// Returns aggregate guard counters and stable pause-reason counts.
    ///
    /// # Errors
    /// Returns a journal error when persisted counters are corrupt or storage
    /// fails.
    pub(crate) fn objective_guard_diagnostics(
        &self,
    ) -> Result<ObjectiveGuardDiagnostics, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let mut diagnostics = guard.query_row(
            r#"
                SELECT
                    COUNT(*),
                    COALESCE(SUM(runs_consumed), 0),
                    COALESCE(SUM(turns_consumed), 0),
                    COALESCE(SUM(provider_calls_consumed), 0),
                    COALESCE(SUM(tokens_consumed), 0),
                    COALESCE(SUM(cost_micros_consumed), 0),
                    COALESCE(SUM(wall_time_ms_consumed), 0),
                    COALESCE(SUM(parse_failures_total), 0),
                    COALESCE(SUM(progress_reset_count), 0)
                FROM objective_budget_ledgers_v1
            "#,
            [],
            |row| {
                Ok(ObjectiveGuardDiagnostics {
                    objectives_tracked: integer_to_u64(row, 0, "objectives_tracked")?,
                    runs_consumed: integer_to_u64(row, 1, "runs_consumed")?,
                    turns_consumed: integer_to_u64(row, 2, "turns_consumed")?,
                    provider_calls_consumed: integer_to_u64(row, 3, "provider_calls_consumed")?,
                    tokens_consumed: integer_to_u64(row, 4, "tokens_consumed")?,
                    cost_micros_consumed: integer_to_u64(row, 5, "cost_micros_consumed")?,
                    wall_time_ms_consumed: integer_to_u64(row, 6, "wall_time_ms_consumed")?,
                    parse_failures_total: integer_to_u64(row, 7, "parse_failures_total")?,
                    progress_resets_total: integer_to_u64(row, 8, "progress_resets_total")?,
                    ..ObjectiveGuardDiagnostics::default()
                })
            },
        )?;
        let (observations, pauses, completed, completion_turns) = guard.query_row(
            r#"
                SELECT
                    COUNT(*),
                    COALESCE(SUM(CASE WHEN disposition = 'pause' THEN 1 ELSE 0 END), 0),
                    COALESCE(SUM(
                        CASE WHEN disposition = 'proceed' AND verdict = 'done' THEN 1 ELSE 0 END
                    ), 0),
                    COALESCE(SUM(
                        CASE
                            WHEN disposition = 'proceed' AND verdict = 'done'
                            THEN cumulative_turns
                            ELSE 0
                        END
                    ), 0)
                FROM objective_progress_fingerprints_v1
            "#,
            [],
            |row| {
                Ok((
                    integer_to_u64(row, 0, "observations_total")?,
                    integer_to_u64(row, 1, "pauses_total")?,
                    integer_to_u64(row, 2, "completed_objectives")?,
                    integer_to_u64(row, 3, "turns_to_completion_total")?,
                ))
            },
        )?;
        diagnostics.observations_total = observations;
        diagnostics.pauses_total = pauses;
        diagnostics.completed_objectives = completed;
        diagnostics.turns_to_completion_total = completion_turns;
        diagnostics.auto_plans_total = guard.query_row(
            "SELECT COUNT(*) FROM plan_objective_links_v1 WHERE is_root = 1",
            [],
            |row| integer_to_u64(row, 0, "auto_plans_total"),
        )?;
        let mut statement = guard.prepare(
            r#"
                SELECT reason_code, COUNT(*)
                FROM objective_progress_fingerprints_v1
                WHERE disposition = 'pause'
                GROUP BY reason_code
                ORDER BY reason_code ASC
            "#,
        )?;
        let rows = statement.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, integer_to_u64(row, 1, "pause_reason_count")?))
        })?;
        diagnostics.pause_reason_counts = rows.collect::<Result<BTreeMap<_, _>, _>>()?;
        Ok(diagnostics)
    }

    /// Atomically creates one session-durable root plan and objective link.
    ///
    /// # Errors
    /// Returns a journal error when scope, text, or storage validation fails.
    pub(crate) fn ensure_v2_complex_plan(
        &self,
        request: &V2ComplexPlanEnsureRequest,
    ) -> Result<V2ComplexPlanEnsureOutcome, JournalError> {
        validate_plan_ensure_request(request)?;
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let outcome =
            ensure_v2_complex_plan_tx(&transaction, self.config.max_payload_bytes, request, now)?;
        transaction.commit()?;
        Ok(outcome)
    }

    /// Lists durable plan links for an objective.
    ///
    /// # Errors
    /// Returns a journal error when the scope id is empty or storage fails.
    #[cfg(test)]
    pub(crate) fn plan_objective_links(
        &self,
        objective_id: &str,
    ) -> Result<Vec<PlanObjectiveLink>, JournalError> {
        ensure_nonempty_field(objective_id, "objective_id")?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let mut statement = guard.prepare(
            r#"
                SELECT
                    objective_ulid, plan_item_ulid, session_ulid, root_run_ulid,
                    focus, is_root, active, created_at_unix_ms, updated_at_unix_ms
                FROM plan_objective_links_v1
                WHERE objective_ulid = ?1
                ORDER BY is_root DESC, created_at_unix_ms ASC, plan_item_ulid ASC
            "#,
        )?;
        let rows = statement.query_map(params![objective_id], map_plan_link_row)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(JournalError::from)
    }

    /// Lists active plan items linked to one objective scope.
    ///
    /// # Errors
    /// Returns a journal error when the objective id is empty or storage fails.
    pub(crate) fn active_plan_item_ids_for_objective(
        &self,
        objective_id: &str,
    ) -> Result<Vec<String>, JournalError> {
        ensure_nonempty_field(objective_id, "objective_id")?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let mut statement = guard.prepare(
            r#"
                SELECT links.plan_item_ulid
                FROM plan_objective_links_v1 AS links
                JOIN agent_plan_items AS plans
                  ON plans.plan_item_ulid = links.plan_item_ulid
                WHERE links.objective_ulid = ?1
                  AND links.active = 1
                  AND plans.status NOT IN ('completed', 'cancelled')
                ORDER BY links.is_root DESC, links.created_at_unix_ms ASC,
                         links.plan_item_ulid ASC
            "#,
        )?;
        let rows = statement.query_map(params![objective_id], |row| row.get::<_, String>(0))?;
        rows.collect::<Result<Vec<_>, _>>().map_err(JournalError::from)
    }

    /// Reports whether a session has a durable active auto-initialized V2 plan.
    ///
    /// # Errors
    /// Returns a journal error when the session id is empty or storage fails.
    pub(crate) fn has_active_v2_complex_plan_for_session(
        &self,
        session_id: &str,
    ) -> Result<bool, JournalError> {
        ensure_nonempty_field(session_id, "session_id")?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        guard
            .query_row(
                r#"
                    SELECT EXISTS(
                        SELECT 1
                        FROM plan_objective_links_v1 AS links
                        JOIN agent_plan_items AS plans
                          ON plans.plan_item_ulid = links.plan_item_ulid
                        WHERE links.session_ulid = ?1
                          AND links.is_root = 1
                          AND links.active = 1
                          AND plans.status NOT IN ('completed', 'cancelled')
                          AND plans.reason_code = ?2
                    )
                "#,
                params![session_id, AUTO_PLAN_REASON],
                |row| row.get::<_, i64>(0),
            )
            .map(|exists| exists != 0)
            .map_err(JournalError::from)
    }
}

/// Applies one observation inside the caller's objective-settlement transaction.
pub(super) fn evaluate_objective_guard_tx(
    transaction: &Transaction<'_>,
    request: &ObjectiveGuardEvaluationRequest,
    now: i64,
) -> Result<ObjectiveGuardEvaluation, JournalError> {
    validate_evaluation_request(request)?;
    let request_json = serde_json::to_vec(request).map_err(|error| {
        JournalError::InvalidArgument(format!("objective guard request cannot serialize: {error}"))
    })?;
    let request_sha256 = sha256_hex(request_json.as_slice());
    if let Some(existing) =
        load_evaluation_tx(transaction, request.observation.attempt_id.as_str(), false)?
    {
        if existing.fingerprint.request_sha256 != request_sha256 {
            return Err(JournalError::InvalidArgument(format!(
                "objective guard attempt {} replay payload changed",
                request.observation.attempt_id
            )));
        }
        return Ok(ObjectiveGuardEvaluation { replayed: true, ..existing });
    }

    let identity = load_attempt_identity(transaction, request.observation.attempt_id.as_str())?
        .ok_or_else(|| {
            JournalError::InvalidArgument(format!(
                "objective guard attempt {} does not exist",
                request.observation.attempt_id
            ))
        })?;
    validate_attempt_identity(&identity, &request.observation)?;
    let mut ledger = load_ledger_tx(transaction, request.observation.objective_id.as_str())?
        .unwrap_or_else(|| new_ledger(request, now));
    advance_ledger(&mut ledger, request, now)?;
    let (disposition, reason_code) = evaluate_disposition(&ledger, request);
    ledger.paused_reason_code =
        (disposition == ObjectiveGuardDisposition::Pause).then(|| reason_code.to_owned());
    persist_ledger_tx(transaction, &ledger)?;
    let fingerprint = fingerprint_from_observation(
        request,
        &ledger,
        request_sha256,
        disposition,
        reason_code,
        now,
    );
    insert_fingerprint_tx(transaction, &fingerprint)?;
    Ok(ObjectiveGuardEvaluation {
        disposition,
        reason_code: reason_code.to_owned(),
        replayed: false,
        ledger,
        fingerprint,
    })
}

/// Resets progress comparisons after a user correction without refunding usage.
pub(super) fn objective_guard_reset_for_session_tx(
    transaction: &Transaction<'_>,
    session_id: &str,
    reason: &str,
    now: i64,
) -> Result<u64, JournalError> {
    ensure_nonempty_field(session_id, "session_id")?;
    validate_reason_code(reason)?;
    let mut statement = transaction.prepare(
        r#"
            SELECT objective_ulid, progress_epoch
            FROM objective_budget_ledgers_v1
            WHERE session_ulid = ?1
            ORDER BY objective_ulid ASC
        "#,
    )?;
    let rows = statement.query_map(params![session_id], |row| {
        Ok((row.get::<_, String>(0)?, integer_to_u64(row, 1, "progress_epoch")?))
    })?;
    let ledgers = rows.collect::<Result<Vec<_>, _>>()?;
    drop(statement);
    for (objective_id, progress_epoch) in &ledgers {
        let next_epoch = progress_epoch.checked_add(1).ok_or_else(|| {
            JournalError::InvalidArgument("objective progress epoch overflow".to_owned())
        })?;
        transaction.execute(
            r#"
                UPDATE objective_budget_ledgers_v1
                SET
                    consecutive_parse_failures = 0,
                    consecutive_no_progress = 0,
                    consecutive_identical_plan = 0,
                    consecutive_tool_error = 0,
                    verdict_oscillations = 0,
                    progress_epoch = ?2,
                    progress_reset_count = progress_reset_count + 1,
                    last_progress_sha256 = NULL,
                    last_plan_sha256 = NULL,
                    last_tool_error_sha256 = NULL,
                    previous_verdict = NULL,
                    last_verdict = NULL,
                    paused_reason_code = NULL,
                    revision = revision + 1,
                    updated_at_unix_ms = ?3
                WHERE objective_ulid = ?1
            "#,
            params![objective_id, u64_to_sqlite(next_epoch, "progress_epoch")?, now],
        )?;
        transaction.execute(
            r#"
                INSERT INTO objective_progress_resets_v1 (
                    reset_ulid, objective_ulid, session_ulid, progress_epoch,
                    reason_code, created_at_unix_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            "#,
            params![
                Ulid::new().to_string(),
                objective_id,
                session_id,
                u64_to_sqlite(next_epoch, "progress_epoch")?,
                reason,
                now,
            ],
        )?;
    }
    u64::try_from(ledgers.len()).map_err(|_| {
        JournalError::InvalidArgument("objective reset count exceeds u64 range".to_owned())
    })
}

fn validate_evaluation_request(
    request: &ObjectiveGuardEvaluationRequest,
) -> Result<(), JournalError> {
    let observation = &request.observation;
    for (value, field) in [
        (observation.attempt_id.as_str(), "attempt_id"),
        (observation.objective_id.as_str(), "objective_id"),
        (observation.session_id.as_str(), "session_id"),
        (observation.root_run_id.as_str(), "root_run_id"),
        (observation.source_run_id.as_str(), "source_run_id"),
    ] {
        ensure_nonempty_field(value, field)?;
    }
    if observation.source_run_generation == 0 {
        return Err(JournalError::InvalidArgument(
            "source_run_generation must be at least 1".to_owned(),
        ));
    }
    for (value, field) in [
        (observation.progress_sha256.as_deref(), "progress_sha256"),
        (observation.plan_sha256.as_deref(), "plan_sha256"),
        (observation.tool_error_sha256.as_deref(), "tool_error_sha256"),
    ] {
        if let Some(value) = value {
            validate_sha256(value, field)?;
        }
    }
    if let Some(reason) = observation.verification_reason_code.as_deref() {
        validate_reason_code(reason)?;
    }
    ensure_json_field(
        observation.verification_evidence_json.as_str(),
        "verification_evidence_json",
    )?;
    ensure_json_field(observation.missing_artifacts_json.as_str(), "missing_artifacts_json")?;
    for (value, field) in [
        (observation.verification_evidence_json.as_str(), "verification_evidence_json"),
        (observation.missing_artifacts_json.as_str(), "missing_artifacts_json"),
    ] {
        if value.len() > MAX_GUARD_EVIDENCE_BYTES {
            return Err(JournalError::InvalidArgument(format!(
                "{field} exceeds {MAX_GUARD_EVIDENCE_BYTES} bytes"
            )));
        }
    }
    for (value, field) in [
        (request.policy.max_consecutive_no_progress, "max_consecutive_no_progress"),
        (request.policy.max_consecutive_identical_plan, "max_consecutive_identical_plan"),
        (request.policy.max_consecutive_tool_error, "max_consecutive_tool_error"),
        (request.policy.max_consecutive_parse_failures, "max_consecutive_parse_failures"),
        (request.policy.max_verdict_oscillations, "max_verdict_oscillations"),
    ] {
        if value == 0 {
            return Err(JournalError::InvalidArgument(format!("{field} must be at least 1")));
        }
    }
    Ok(())
}

fn validate_sha256(value: &str, field: &'static str) -> Result<(), JournalError> {
    if value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(JournalError::InvalidArgument(format!(
            "{field} must be a 64-character hex SHA-256 digest"
        )))
    }
}

fn validate_reason_code(reason: &str) -> Result<(), JournalError> {
    if valid_terminal_reason_code(reason) {
        Ok(())
    } else {
        Err(JournalError::InvalidArgument("invalid objective guard reason code".to_owned()))
    }
}

fn validate_attempt_identity(
    identity: &AttemptIdentity,
    observation: &ObjectiveProgressObservation,
) -> Result<(), JournalError> {
    if identity.objective_id != observation.objective_id
        || identity.session_id != observation.session_id
        || identity.root_run_id != observation.root_run_id
        || identity.source_run_id != observation.source_run_id
        || identity.source_run_generation != observation.source_run_generation
        || identity.decision != observation.decision
    {
        return Err(JournalError::InvalidArgument(format!(
            "objective guard attempt {} scope or decision changed",
            observation.attempt_id
        )));
    }
    Ok(())
}

fn load_attempt_identity(
    connection: &Connection,
    attempt_id: &str,
) -> Result<Option<AttemptIdentity>, JournalError> {
    connection
        .query_row(
            r#"
                SELECT
                    objective_ulid, session_ulid, root_run_ulid, source_run_ulid,
                    source_run_generation, decision
                FROM objective_continuation_attempts_v1
                WHERE attempt_ulid = ?1
            "#,
            params![attempt_id],
            |row| {
                let decision = row.get::<_, String>(5)?;
                Ok(AttemptIdentity {
                    objective_id: row.get(0)?,
                    session_id: row.get(1)?,
                    root_run_id: row.get(2)?,
                    source_run_id: row.get(3)?,
                    source_run_generation: integer_to_u64(row, 4, "source_run_generation")?,
                    decision: parse_objective_decision(decision.as_str())
                        .ok_or_else(|| invalid_column(5, format!("unknown verdict {decision}")))?,
                })
            },
        )
        .optional()
        .map_err(JournalError::from)
}

fn new_ledger(request: &ObjectiveGuardEvaluationRequest, now: i64) -> ObjectiveBudgetLedger {
    let observation = &request.observation;
    ObjectiveBudgetLedger {
        objective_id: observation.objective_id.clone(),
        session_id: observation.session_id.clone(),
        root_run_id: observation.root_run_id.clone(),
        max_runs: request.policy.max_runs,
        max_turns: request.policy.max_turns,
        max_provider_calls: request.policy.max_provider_calls,
        max_tokens: request.policy.max_tokens,
        max_cost_micros: request.policy.max_cost_micros,
        max_wall_time_ms: request.policy.max_wall_time_ms,
        runs_consumed: 0,
        turns_consumed: 0,
        provider_calls_consumed: 0,
        tokens_consumed: 0,
        cost_micros_consumed: 0,
        wall_time_ms_consumed: 0,
        parse_failures_total: 0,
        consecutive_parse_failures: 0,
        consecutive_no_progress: 0,
        consecutive_identical_plan: 0,
        consecutive_tool_error: 0,
        verdict_oscillations: 0,
        progress_epoch: 0,
        progress_reset_count: 0,
        last_progress_sha256: None,
        last_plan_sha256: None,
        last_tool_error_sha256: None,
        previous_verdict: None,
        last_verdict: None,
        paused_reason_code: None,
        revision: 0,
        created_at_unix_ms: now,
        updated_at_unix_ms: now,
    }
}

fn advance_ledger(
    ledger: &mut ObjectiveBudgetLedger,
    request: &ObjectiveGuardEvaluationRequest,
    now: i64,
) -> Result<(), JournalError> {
    let observation = &request.observation;
    if ledger.session_id != observation.session_id || ledger.root_run_id != observation.root_run_id
    {
        return Err(JournalError::InvalidArgument(format!(
            "objective guard ledger {} scope changed",
            ledger.objective_id
        )));
    }
    ledger.max_runs = request.policy.max_runs;
    ledger.max_turns = request.policy.max_turns;
    ledger.max_provider_calls = request.policy.max_provider_calls;
    ledger.max_tokens = request.policy.max_tokens;
    ledger.max_cost_micros = request.policy.max_cost_micros;
    ledger.max_wall_time_ms = request.policy.max_wall_time_ms;
    ledger.runs_consumed = add_counter(ledger.runs_consumed, observation.runs_delta, "runs")?;
    ledger.turns_consumed = add_counter(ledger.turns_consumed, observation.turns_delta, "turns")?;
    ledger.provider_calls_consumed = add_counter(
        ledger.provider_calls_consumed,
        observation.provider_calls_delta,
        "provider calls",
    )?;
    ledger.tokens_consumed =
        add_counter(ledger.tokens_consumed, observation.tokens_delta, "tokens")?;
    ledger.cost_micros_consumed =
        add_counter(ledger.cost_micros_consumed, observation.cost_micros_delta, "cost micros")?;
    ledger.wall_time_ms_consumed =
        add_counter(ledger.wall_time_ms_consumed, observation.wall_time_ms_delta, "wall time")?;
    if observation.parse_failure {
        ledger.parse_failures_total =
            add_counter(ledger.parse_failures_total, 1, "parse failures")?;
        ledger.consecutive_parse_failures =
            add_counter(ledger.consecutive_parse_failures, 1, "consecutive parse failures")?;
    } else {
        ledger.consecutive_parse_failures = 0;
    }
    let progress_changed = observation.progress_sha256.is_some()
        && observation.progress_sha256 != ledger.last_progress_sha256;
    if progress_changed {
        ledger.consecutive_no_progress = 0;
    } else {
        ledger.consecutive_no_progress =
            add_counter(ledger.consecutive_no_progress, 1, "consecutive no progress")?;
    }
    ledger.consecutive_identical_plan = repeated_counter(
        ledger.last_plan_sha256.as_deref(),
        observation.plan_sha256.as_deref(),
        ledger.consecutive_identical_plan,
        "consecutive identical plan",
    )?;
    ledger.consecutive_tool_error = repeated_counter(
        ledger.last_tool_error_sha256.as_deref(),
        observation.tool_error_sha256.as_deref(),
        ledger.consecutive_tool_error,
        "consecutive tool error",
    )?;
    let verdict = observation.decision.as_str();
    if ledger.previous_verdict.as_deref().is_some_and(|previous| previous == verdict)
        && ledger.last_verdict.as_deref().is_some_and(|last| last != verdict)
    {
        ledger.verdict_oscillations =
            add_counter(ledger.verdict_oscillations, 1, "verdict oscillations")?;
    }
    ledger.previous_verdict = ledger.last_verdict.take();
    ledger.last_verdict = Some(verdict.to_owned());
    ledger.last_progress_sha256 = observation.progress_sha256.clone();
    ledger.last_plan_sha256 = observation.plan_sha256.clone();
    ledger.last_tool_error_sha256 = observation.tool_error_sha256.clone();
    ledger.revision = add_counter(ledger.revision, 1, "ledger revision")?;
    ledger.updated_at_unix_ms = now;
    Ok(())
}

fn repeated_counter(
    previous: Option<&str>,
    current: Option<&str>,
    counter: u64,
    field: &'static str,
) -> Result<u64, JournalError> {
    match current {
        Some(current) if previous == Some(current) => add_counter(counter, 1, field),
        Some(_) | None => Ok(0),
    }
}

fn add_counter(current: u64, delta: u64, field: &'static str) -> Result<u64, JournalError> {
    current
        .checked_add(delta)
        .ok_or_else(|| JournalError::InvalidArgument(format!("objective guard {field} overflow")))
}

fn evaluate_disposition(
    ledger: &ObjectiveBudgetLedger,
    request: &ObjectiveGuardEvaluationRequest,
) -> (ObjectiveGuardDisposition, &'static str) {
    let is_done = request.observation.decision == ObjectiveContinuationDecision::Done;
    for (consumed, maximum, reason) in [
        (ledger.runs_consumed, ledger.max_runs, "objective.guard.budget.runs_exhausted"),
        (ledger.turns_consumed, ledger.max_turns, "objective.guard.budget.turns_exhausted"),
        (
            ledger.provider_calls_consumed,
            ledger.max_provider_calls,
            "objective.guard.budget.provider_calls_exhausted",
        ),
        (ledger.tokens_consumed, ledger.max_tokens, "objective.guard.budget.tokens_exhausted"),
        (
            ledger.cost_micros_consumed,
            ledger.max_cost_micros,
            "objective.guard.budget.cost_exhausted",
        ),
        (
            ledger.wall_time_ms_consumed,
            ledger.max_wall_time_ms,
            "objective.guard.budget.wall_time_exhausted",
        ),
    ] {
        if maximum.is_some_and(|maximum| consumed > maximum || (!is_done && consumed == maximum)) {
            return (ObjectiveGuardDisposition::Pause, reason);
        }
    }
    for (current, maximum, reason) in [
        (
            ledger.consecutive_parse_failures,
            request.policy.max_consecutive_parse_failures,
            "objective.guard.parse_failures",
        ),
        (
            ledger.consecutive_no_progress,
            request.policy.max_consecutive_no_progress,
            "objective.guard.no_progress",
        ),
        (
            ledger.consecutive_identical_plan,
            request.policy.max_consecutive_identical_plan,
            "objective.guard.identical_plan",
        ),
        (
            ledger.consecutive_tool_error,
            request.policy.max_consecutive_tool_error,
            "objective.guard.repeated_tool_error",
        ),
        (
            ledger.verdict_oscillations,
            request.policy.max_verdict_oscillations,
            "objective.guard.verdict_oscillation",
        ),
    ] {
        if current >= maximum {
            return (ObjectiveGuardDisposition::Pause, reason);
        }
    }
    if is_done {
        if !json_has_evidence(request.observation.verification_evidence_json.as_str()) {
            return (
                ObjectiveGuardDisposition::Pause,
                "objective.guard.verification_missing_evidence",
            );
        }
        return match request.observation.verification_status {
            ObjectiveVerificationStatus::Verified | ObjectiveVerificationStatus::NotRequired => {
                (ObjectiveGuardDisposition::Proceed, "objective.guard.allowed")
            }
            ObjectiveVerificationStatus::MissingArtifacts => {
                (ObjectiveGuardDisposition::Pause, "objective.guard.verification_missing_artifacts")
            }
            ObjectiveVerificationStatus::Unknown
            | ObjectiveVerificationStatus::MissingEvidence
            | ObjectiveVerificationStatus::Failed => {
                (ObjectiveGuardDisposition::Pause, "objective.guard.verification_failed")
            }
        };
    }
    (ObjectiveGuardDisposition::Proceed, "objective.guard.allowed")
}

fn json_has_evidence(raw: &str) -> bool {
    serde_json::from_str::<Value>(raw).is_ok_and(|value| match value {
        Value::Null => false,
        Value::Array(values) => !values.is_empty(),
        Value::Object(values) => !values.is_empty(),
        Value::String(value) => !value.trim().is_empty(),
        Value::Bool(_) | Value::Number(_) => true,
    })
}

fn parse_objective_decision(value: &str) -> Option<ObjectiveContinuationDecision> {
    match value {
        "pending" => Some(ObjectiveContinuationDecision::Pending),
        "done" => Some(ObjectiveContinuationDecision::Done),
        "continue" => Some(ObjectiveContinuationDecision::Continue),
        "wait" => Some(ObjectiveContinuationDecision::Wait),
        "blocked" => Some(ObjectiveContinuationDecision::Blocked),
        "needs_user" => Some(ObjectiveContinuationDecision::NeedsUser),
        _ => None,
    }
}

fn invalid_column(column: usize, message: String) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        column,
        rusqlite::types::Type::Text,
        Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, message)),
    )
}
