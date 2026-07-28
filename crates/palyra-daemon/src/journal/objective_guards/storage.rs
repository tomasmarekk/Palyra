//! SQLite mapping and persistence for objective guard ledgers and immutable
//! fingerprints. Conversion helpers reject corrupt negative counters instead
//! of projecting them into unsigned runtime values.

use super::*;

const LEDGER_COLUMNS: &str = "
    objective_ulid, session_ulid, root_run_ulid,
    max_runs, max_turns, max_provider_calls, max_tokens, max_cost_micros, max_wall_time_ms,
    runs_consumed, turns_consumed, provider_calls_consumed, tokens_consumed,
    cost_micros_consumed, wall_time_ms_consumed, parse_failures_total,
    consecutive_parse_failures, consecutive_no_progress, consecutive_identical_plan,
    consecutive_tool_error, verdict_oscillations, progress_epoch, progress_reset_count,
    last_progress_sha256, last_plan_sha256, last_tool_error_sha256,
    previous_verdict, last_verdict, paused_reason_code, revision,
    created_at_unix_ms, updated_at_unix_ms
";

const FINGERPRINT_COLUMNS: &str = "
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
";

pub(super) fn load_ledger_tx(
    connection: &Connection,
    objective_id: &str,
) -> Result<Option<ObjectiveBudgetLedger>, JournalError> {
    let query = format!(
        "SELECT {LEDGER_COLUMNS} FROM objective_budget_ledgers_v1 WHERE objective_ulid = ?1"
    );
    connection
        .query_row(query.as_str(), params![objective_id], map_ledger_row)
        .optional()
        .map_err(JournalError::from)
}

pub(super) fn persist_ledger_tx(
    transaction: &Transaction<'_>,
    ledger: &ObjectiveBudgetLedger,
) -> Result<(), JournalError> {
    transaction.execute(
        r#"
            INSERT INTO objective_budget_ledgers_v1 (
                objective_ulid, session_ulid, root_run_ulid,
                max_runs, max_turns, max_provider_calls, max_tokens,
                max_cost_micros, max_wall_time_ms, runs_consumed, turns_consumed,
                provider_calls_consumed, tokens_consumed, cost_micros_consumed,
                wall_time_ms_consumed, parse_failures_total,
                consecutive_parse_failures, consecutive_no_progress,
                consecutive_identical_plan, consecutive_tool_error,
                verdict_oscillations, progress_epoch, progress_reset_count,
                last_progress_sha256, last_plan_sha256, last_tool_error_sha256,
                previous_verdict, last_verdict, paused_reason_code, revision,
                schema_version, created_at_unix_ms, updated_at_unix_ms
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13,
                ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25,
                ?26, ?27, ?28, ?29, ?30, ?31, ?32, ?33
            )
            ON CONFLICT(objective_ulid) DO UPDATE SET
                max_runs = excluded.max_runs,
                max_turns = excluded.max_turns,
                max_provider_calls = excluded.max_provider_calls,
                max_tokens = excluded.max_tokens,
                max_cost_micros = excluded.max_cost_micros,
                max_wall_time_ms = excluded.max_wall_time_ms,
                runs_consumed = excluded.runs_consumed,
                turns_consumed = excluded.turns_consumed,
                provider_calls_consumed = excluded.provider_calls_consumed,
                tokens_consumed = excluded.tokens_consumed,
                cost_micros_consumed = excluded.cost_micros_consumed,
                wall_time_ms_consumed = excluded.wall_time_ms_consumed,
                parse_failures_total = excluded.parse_failures_total,
                consecutive_parse_failures = excluded.consecutive_parse_failures,
                consecutive_no_progress = excluded.consecutive_no_progress,
                consecutive_identical_plan = excluded.consecutive_identical_plan,
                consecutive_tool_error = excluded.consecutive_tool_error,
                verdict_oscillations = excluded.verdict_oscillations,
                progress_epoch = excluded.progress_epoch,
                progress_reset_count = excluded.progress_reset_count,
                last_progress_sha256 = excluded.last_progress_sha256,
                last_plan_sha256 = excluded.last_plan_sha256,
                last_tool_error_sha256 = excluded.last_tool_error_sha256,
                previous_verdict = excluded.previous_verdict,
                last_verdict = excluded.last_verdict,
                paused_reason_code = excluded.paused_reason_code,
                revision = excluded.revision,
                updated_at_unix_ms = excluded.updated_at_unix_ms
        "#,
        params![
            ledger.objective_id,
            ledger.session_id,
            ledger.root_run_id,
            optional_u64_to_sqlite(ledger.max_runs, "max_runs")?,
            optional_u64_to_sqlite(ledger.max_turns, "max_turns")?,
            optional_u64_to_sqlite(ledger.max_provider_calls, "max_provider_calls")?,
            optional_u64_to_sqlite(ledger.max_tokens, "max_tokens")?,
            optional_u64_to_sqlite(ledger.max_cost_micros, "max_cost_micros")?,
            optional_u64_to_sqlite(ledger.max_wall_time_ms, "max_wall_time_ms")?,
            u64_to_sqlite(ledger.runs_consumed, "runs_consumed")?,
            u64_to_sqlite(ledger.turns_consumed, "turns_consumed")?,
            u64_to_sqlite(ledger.provider_calls_consumed, "provider_calls_consumed")?,
            u64_to_sqlite(ledger.tokens_consumed, "tokens_consumed")?,
            u64_to_sqlite(ledger.cost_micros_consumed, "cost_micros_consumed")?,
            u64_to_sqlite(ledger.wall_time_ms_consumed, "wall_time_ms_consumed")?,
            u64_to_sqlite(ledger.parse_failures_total, "parse_failures_total")?,
            u64_to_sqlite(ledger.consecutive_parse_failures, "consecutive_parse_failures",)?,
            u64_to_sqlite(ledger.consecutive_no_progress, "consecutive_no_progress")?,
            u64_to_sqlite(ledger.consecutive_identical_plan, "consecutive_identical_plan",)?,
            u64_to_sqlite(ledger.consecutive_tool_error, "consecutive_tool_error")?,
            u64_to_sqlite(ledger.verdict_oscillations, "verdict_oscillations")?,
            u64_to_sqlite(ledger.progress_epoch, "progress_epoch")?,
            u64_to_sqlite(ledger.progress_reset_count, "progress_reset_count")?,
            ledger.last_progress_sha256,
            ledger.last_plan_sha256,
            ledger.last_tool_error_sha256,
            ledger.previous_verdict,
            ledger.last_verdict,
            ledger.paused_reason_code,
            u64_to_sqlite(ledger.revision, "revision")?,
            SCHEMA_VERSION,
            ledger.created_at_unix_ms,
            ledger.updated_at_unix_ms,
        ],
    )?;
    Ok(())
}

pub(super) fn insert_fingerprint_tx(
    transaction: &Transaction<'_>,
    fingerprint: &ObjectiveProgressFingerprint,
) -> Result<(), JournalError> {
    transaction.execute(
        r#"
            INSERT INTO objective_progress_fingerprints_v1 (
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
                schema_version, created_at_unix_ms
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13,
                ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25,
                ?26, ?27, ?28, ?29, ?30, ?31, ?32, ?33
            )
        "#,
        params![
            fingerprint.attempt_id,
            fingerprint.objective_id,
            fingerprint.session_id,
            fingerprint.root_run_id,
            fingerprint.source_run_id,
            u64_to_sqlite(fingerprint.source_run_generation, "source_run_generation")?,
            fingerprint.request_sha256,
            fingerprint.decision.as_str(),
            fingerprint.progress_sha256,
            fingerprint.plan_sha256,
            fingerprint.tool_error_sha256,
            bool_to_sqlite(fingerprint.progress_detected),
            bool_to_sqlite(fingerprint.parse_failure),
            fingerprint.verification_status.as_str(),
            fingerprint.verification_reason_code,
            fingerprint.verification_evidence_json,
            fingerprint.missing_artifacts_json,
            fingerprint.disposition.as_str(),
            fingerprint.reason_code,
            u64_to_sqlite(fingerprint.cumulative_runs, "cumulative_runs")?,
            u64_to_sqlite(fingerprint.cumulative_turns, "cumulative_turns")?,
            u64_to_sqlite(fingerprint.cumulative_provider_calls, "cumulative_provider_calls",)?,
            u64_to_sqlite(fingerprint.cumulative_tokens, "cumulative_tokens")?,
            u64_to_sqlite(fingerprint.cumulative_cost_micros, "cumulative_cost_micros",)?,
            u64_to_sqlite(fingerprint.cumulative_wall_time_ms, "cumulative_wall_time_ms",)?,
            u64_to_sqlite(fingerprint.consecutive_parse_failures, "consecutive_parse_failures",)?,
            u64_to_sqlite(fingerprint.consecutive_no_progress, "consecutive_no_progress",)?,
            u64_to_sqlite(fingerprint.consecutive_identical_plan, "consecutive_identical_plan",)?,
            u64_to_sqlite(fingerprint.consecutive_tool_error, "consecutive_tool_error",)?,
            u64_to_sqlite(fingerprint.verdict_oscillations, "verdict_oscillations")?,
            u64_to_sqlite(fingerprint.progress_epoch, "progress_epoch")?,
            SCHEMA_VERSION,
            fingerprint.created_at_unix_ms,
        ],
    )?;
    Ok(())
}

pub(super) fn load_evaluation_tx(
    connection: &Connection,
    attempt_id: &str,
    replayed: bool,
) -> Result<Option<ObjectiveGuardEvaluation>, JournalError> {
    let query = format!(
        "SELECT {FINGERPRINT_COLUMNS} \
         FROM objective_progress_fingerprints_v1 WHERE attempt_ulid = ?1"
    );
    let fingerprint = connection
        .query_row(query.as_str(), params![attempt_id], map_fingerprint_row)
        .optional()?;
    let Some(fingerprint) = fingerprint else {
        return Ok(None);
    };
    let ledger =
        load_ledger_tx(connection, fingerprint.objective_id.as_str())?.ok_or_else(|| {
            JournalError::InvalidArgument(format!(
                "objective guard ledger {} is missing",
                fingerprint.objective_id
            ))
        })?;
    Ok(Some(ObjectiveGuardEvaluation {
        disposition: fingerprint.disposition,
        reason_code: fingerprint.reason_code.clone(),
        replayed,
        ledger,
        fingerprint,
    }))
}

pub(super) fn fingerprint_from_observation(
    request: &ObjectiveGuardEvaluationRequest,
    ledger: &ObjectiveBudgetLedger,
    request_sha256: String,
    disposition: ObjectiveGuardDisposition,
    reason_code: &str,
    now: i64,
) -> ObjectiveProgressFingerprint {
    let observation = &request.observation;
    ObjectiveProgressFingerprint {
        attempt_id: observation.attempt_id.clone(),
        objective_id: observation.objective_id.clone(),
        session_id: observation.session_id.clone(),
        root_run_id: observation.root_run_id.clone(),
        source_run_id: observation.source_run_id.clone(),
        source_run_generation: observation.source_run_generation,
        request_sha256,
        decision: observation.decision,
        progress_sha256: observation.progress_sha256.clone(),
        plan_sha256: observation.plan_sha256.clone(),
        tool_error_sha256: observation.tool_error_sha256.clone(),
        progress_detected: observation.progress_detected,
        parse_failure: observation.parse_failure,
        verification_status: observation.verification_status,
        verification_reason_code: observation.verification_reason_code.clone(),
        verification_evidence_json: observation.verification_evidence_json.clone(),
        missing_artifacts_json: observation.missing_artifacts_json.clone(),
        disposition,
        reason_code: reason_code.to_owned(),
        cumulative_runs: ledger.runs_consumed,
        cumulative_turns: ledger.turns_consumed,
        cumulative_provider_calls: ledger.provider_calls_consumed,
        cumulative_tokens: ledger.tokens_consumed,
        cumulative_cost_micros: ledger.cost_micros_consumed,
        cumulative_wall_time_ms: ledger.wall_time_ms_consumed,
        consecutive_parse_failures: ledger.consecutive_parse_failures,
        consecutive_no_progress: ledger.consecutive_no_progress,
        consecutive_identical_plan: ledger.consecutive_identical_plan,
        consecutive_tool_error: ledger.consecutive_tool_error,
        verdict_oscillations: ledger.verdict_oscillations,
        progress_epoch: ledger.progress_epoch,
        created_at_unix_ms: now,
    }
}

pub(super) fn map_fingerprint_row(
    row: &rusqlite::Row<'_>,
) -> Result<ObjectiveProgressFingerprint, rusqlite::Error> {
    let decision = row.get::<_, String>(7)?;
    let verification = row.get::<_, String>(13)?;
    let disposition = row.get::<_, String>(17)?;
    Ok(ObjectiveProgressFingerprint {
        attempt_id: row.get(0)?,
        objective_id: row.get(1)?,
        session_id: row.get(2)?,
        root_run_id: row.get(3)?,
        source_run_id: row.get(4)?,
        source_run_generation: integer_to_u64(row, 5, "source_run_generation")?,
        request_sha256: row.get(6)?,
        decision: parse_objective_decision(decision.as_str())
            .ok_or_else(|| invalid_column(7, format!("unknown verdict {decision}")))?,
        progress_sha256: row.get(8)?,
        plan_sha256: row.get(9)?,
        tool_error_sha256: row.get(10)?,
        progress_detected: row.get::<_, i64>(11)? != 0,
        parse_failure: row.get::<_, i64>(12)? != 0,
        verification_status: ObjectiveVerificationStatus::parse(verification.as_str()).ok_or_else(
            || invalid_column(13, format!("unknown verification status {verification}")),
        )?,
        verification_reason_code: row.get(14)?,
        verification_evidence_json: row.get(15)?,
        missing_artifacts_json: row.get(16)?,
        disposition: ObjectiveGuardDisposition::parse(disposition.as_str())
            .ok_or_else(|| invalid_column(17, format!("unknown disposition {disposition}")))?,
        reason_code: row.get(18)?,
        cumulative_runs: integer_to_u64(row, 19, "cumulative_runs")?,
        cumulative_turns: integer_to_u64(row, 20, "cumulative_turns")?,
        cumulative_provider_calls: integer_to_u64(row, 21, "cumulative_provider_calls")?,
        cumulative_tokens: integer_to_u64(row, 22, "cumulative_tokens")?,
        cumulative_cost_micros: integer_to_u64(row, 23, "cumulative_cost_micros")?,
        cumulative_wall_time_ms: integer_to_u64(row, 24, "cumulative_wall_time_ms")?,
        consecutive_parse_failures: integer_to_u64(row, 25, "consecutive_parse_failures")?,
        consecutive_no_progress: integer_to_u64(row, 26, "consecutive_no_progress")?,
        consecutive_identical_plan: integer_to_u64(row, 27, "consecutive_identical_plan")?,
        consecutive_tool_error: integer_to_u64(row, 28, "consecutive_tool_error")?,
        verdict_oscillations: integer_to_u64(row, 29, "verdict_oscillations")?,
        progress_epoch: integer_to_u64(row, 30, "progress_epoch")?,
        created_at_unix_ms: row.get(31)?,
    })
}

fn map_ledger_row(row: &rusqlite::Row<'_>) -> Result<ObjectiveBudgetLedger, rusqlite::Error> {
    Ok(ObjectiveBudgetLedger {
        objective_id: row.get(0)?,
        session_id: row.get(1)?,
        root_run_id: row.get(2)?,
        max_runs: optional_integer_to_u64(row, 3, "max_runs")?,
        max_turns: optional_integer_to_u64(row, 4, "max_turns")?,
        max_provider_calls: optional_integer_to_u64(row, 5, "max_provider_calls")?,
        max_tokens: optional_integer_to_u64(row, 6, "max_tokens")?,
        max_cost_micros: optional_integer_to_u64(row, 7, "max_cost_micros")?,
        max_wall_time_ms: optional_integer_to_u64(row, 8, "max_wall_time_ms")?,
        runs_consumed: integer_to_u64(row, 9, "runs_consumed")?,
        turns_consumed: integer_to_u64(row, 10, "turns_consumed")?,
        provider_calls_consumed: integer_to_u64(row, 11, "provider_calls_consumed")?,
        tokens_consumed: integer_to_u64(row, 12, "tokens_consumed")?,
        cost_micros_consumed: integer_to_u64(row, 13, "cost_micros_consumed")?,
        wall_time_ms_consumed: integer_to_u64(row, 14, "wall_time_ms_consumed")?,
        parse_failures_total: integer_to_u64(row, 15, "parse_failures_total")?,
        consecutive_parse_failures: integer_to_u64(row, 16, "consecutive_parse_failures")?,
        consecutive_no_progress: integer_to_u64(row, 17, "consecutive_no_progress")?,
        consecutive_identical_plan: integer_to_u64(row, 18, "consecutive_identical_plan")?,
        consecutive_tool_error: integer_to_u64(row, 19, "consecutive_tool_error")?,
        verdict_oscillations: integer_to_u64(row, 20, "verdict_oscillations")?,
        progress_epoch: integer_to_u64(row, 21, "progress_epoch")?,
        progress_reset_count: integer_to_u64(row, 22, "progress_reset_count")?,
        last_progress_sha256: row.get(23)?,
        last_plan_sha256: row.get(24)?,
        last_tool_error_sha256: row.get(25)?,
        previous_verdict: row.get(26)?,
        last_verdict: row.get(27)?,
        paused_reason_code: row.get(28)?,
        revision: integer_to_u64(row, 29, "revision")?,
        created_at_unix_ms: row.get(30)?,
        updated_at_unix_ms: row.get(31)?,
    })
}

fn optional_integer_to_u64(
    row: &rusqlite::Row<'_>,
    column: usize,
    field: &'static str,
) -> Result<Option<u64>, rusqlite::Error> {
    row.get::<_, Option<i64>>(column)?
        .map(|value| {
            u64::try_from(value).map_err(|_| invalid_column(column, format!("{field} is negative")))
        })
        .transpose()
}

fn optional_u64_to_sqlite(
    value: Option<u64>,
    field: &'static str,
) -> Result<Option<i64>, JournalError> {
    value.map(|value| u64_to_sqlite(value, field)).transpose()
}
