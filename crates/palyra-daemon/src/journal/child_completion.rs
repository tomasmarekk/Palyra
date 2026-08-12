//! Durable child-completion envelopes, announcement outbox delivery, and
//! restart-safe orphan classification. Parent prompts receive only bounded
//! structured handoff data; raw child transcripts remain isolated.

use super::*;

const CHILD_COMPLETION_SCHEMA_VERSION: i64 = 1;
const CHILD_COMPLETION_SCAN_LIMIT: i64 = 256;
const CHILD_COMPLETION_SUMMARY_BYTES: usize = 2_048;
const CHILD_COMPLETION_RESULT_BYTES: usize = 16 * 1_024;
const CHILD_COMPLETION_REF_LIMIT: usize = 32;
const CHILD_COMPLETION_REF_FIELD_BYTES: usize = 512;
const CHILD_COMPLETION_MAX_NESTING_DEPTH: usize = 32;
const CHILD_COMPLETION_MAX_DESCENDANT_SESSIONS: usize = 256;

pub(super) const MIGRATION_86_SQL: &str = r#"
    CREATE TABLE IF NOT EXISTS child_completion_envelopes_v1 (
        envelope_ulid TEXT PRIMARY KEY,
        dedupe_key TEXT NOT NULL UNIQUE,
        task_ulid TEXT NOT NULL,
        child_session_ulid TEXT NOT NULL,
        child_run_ulid TEXT NOT NULL,
        child_generation INTEGER NOT NULL CHECK (child_generation >= 0),
        parent_session_ulid TEXT NOT NULL,
        parent_run_ulid TEXT NOT NULL,
        parent_generation INTEGER NOT NULL CHECK (parent_generation >= 0),
        terminal_state TEXT NOT NULL,
        summary_text TEXT NOT NULL,
        structured_result_json TEXT NOT NULL,
        artifact_refs_json TEXT NOT NULL,
        evidence_refs_json TEXT NOT NULL,
        verification_status TEXT NOT NULL,
        merge_preview_sha256 TEXT,
        merge_actual_sha256 TEXT,
        merge_safety_verdict TEXT NOT NULL,
        schema_version INTEGER NOT NULL DEFAULT 1 CHECK (schema_version = 1),
        created_at_unix_ms INTEGER NOT NULL,
        FOREIGN KEY(task_ulid) REFERENCES orchestrator_background_tasks(task_ulid),
        FOREIGN KEY(child_session_ulid) REFERENCES orchestrator_sessions(session_ulid),
        FOREIGN KEY(parent_session_ulid) REFERENCES orchestrator_sessions(session_ulid),
        FOREIGN KEY(parent_run_ulid) REFERENCES orchestrator_runs(run_ulid)
    );
    CREATE INDEX IF NOT EXISTS idx_child_completion_parent
        ON child_completion_envelopes_v1(
            parent_run_ulid,
            parent_generation,
            created_at_unix_ms
        );

    CREATE TABLE IF NOT EXISTS child_announce_intents_v1 (
        announce_intent_ulid TEXT PRIMARY KEY,
        envelope_ulid TEXT NOT NULL UNIQUE,
        dedupe_key TEXT NOT NULL UNIQUE,
        state TEXT NOT NULL,
        reason_code TEXT NOT NULL,
        attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
        delivered_tape_seq INTEGER,
        created_at_unix_ms INTEGER NOT NULL,
        updated_at_unix_ms INTEGER NOT NULL,
        FOREIGN KEY(envelope_ulid)
            REFERENCES child_completion_envelopes_v1(envelope_ulid)
    );
    CREATE INDEX IF NOT EXISTS idx_child_announce_pending
        ON child_announce_intents_v1(state, created_at_unix_ms);

    CREATE TABLE IF NOT EXISTS orphan_child_recovery_v1 (
        recovery_ulid TEXT PRIMARY KEY,
        task_ulid TEXT NOT NULL,
        child_generation INTEGER NOT NULL CHECK (child_generation >= 0),
        outcome TEXT NOT NULL,
        reason_code TEXT NOT NULL,
        child_run_state TEXT,
        budget_tokens INTEGER NOT NULL CHECK (budget_tokens >= 0),
        attempt_count INTEGER NOT NULL CHECK (attempt_count >= 0),
        max_attempts INTEGER NOT NULL CHECK (max_attempts >= 0),
        created_at_unix_ms INTEGER NOT NULL,
        updated_at_unix_ms INTEGER NOT NULL,
        UNIQUE(task_ulid, child_generation),
        FOREIGN KEY(task_ulid) REFERENCES orchestrator_background_tasks(task_ulid)
    );
"#;

/// Bounded, artifact-oriented child result committed before parent delivery.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChildCompletionEnvelope {
    pub envelope_id: String,
    pub task_id: String,
    pub child_session_id: String,
    pub child_run_id: String,
    pub child_generation: u64,
    pub parent_session_id: String,
    pub parent_run_id: String,
    pub parent_generation: u64,
    pub terminal_state: String,
    pub summary_text: String,
    pub structured_result_json: String,
    pub artifact_refs_json: String,
    pub evidence_refs_json: String,
    pub verification_status: String,
    pub merge_preview_sha256: Option<String>,
    pub merge_actual_sha256: Option<String>,
    pub merge_safety_verdict: String,
    pub created_at_unix_ms: i64,
}

/// Durable outbox record that deduplicates parent completion delivery.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChildAnnounceIntent {
    pub announce_intent_id: String,
    pub envelope_id: String,
    pub state: String,
    pub reason_code: String,
    pub attempt_count: u64,
    pub delivered_tape_seq: Option<i64>,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

/// Restart-time classification of one child task without increasing its budget.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OrphanChildRecoveryOutcome {
    pub task_id: String,
    pub child_generation: u64,
    pub outcome: String,
    pub reason_code: String,
    pub child_run_state: Option<String>,
    pub budget_tokens: u64,
    pub attempt_count: u64,
    pub max_attempts: u64,
}

/// Bounded reconciliation counters for startup diagnostics and metadata trace.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChildCompletionReconcileReport {
    pub classified_orphans: u64,
    pub delivered_announcements: u64,
    pub deferred_for_nested_children: u64,
    pub stale_announcements: u64,
    pub cancelled_announcements: u64,
    pub manual_review_announcements: u64,
}

#[derive(Debug)]
struct CompletionProjection {
    summary_text: String,
    structured_result_json: String,
    artifact_refs_json: String,
    evidence_refs_json: String,
    verification_status: String,
    merge_preview_sha256: Option<String>,
    merge_actual_sha256: Option<String>,
    merge_safety_verdict: String,
}

#[derive(Debug)]
struct PendingAnnouncement {
    intent: ChildAnnounceIntent,
    envelope: ChildCompletionEnvelope,
}

impl JournalStore {
    /// Loads the durable completion envelope for one child task.
    ///
    /// # Errors
    /// Returns a journal error when the query or numeric conversion fails.
    #[cfg(test)]
    pub fn child_completion_for_task(
        &self,
        task_id: &str,
    ) -> Result<Option<ChildCompletionEnvelope>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        load_child_completion_for_task_tx(&guard, task_id)
    }

    /// Loads the outbox intent paired with one child task completion.
    ///
    /// # Errors
    /// Returns a journal error when the query or numeric conversion fails.
    #[cfg(test)]
    pub fn child_announce_intent_for_task(
        &self,
        task_id: &str,
    ) -> Result<Option<ChildAnnounceIntent>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let query = format!(
            "SELECT {CHILD_ANNOUNCE_COLUMNS} \
             FROM child_announce_intents_v1 AS intents \
             INNER JOIN child_completion_envelopes_v1 AS envelopes \
                ON envelopes.envelope_ulid = intents.envelope_ulid \
             WHERE envelopes.task_ulid = ?1"
        );
        guard
            .query_row(query.as_str(), params![task_id], map_child_announce_row)
            .optional()
            .map_err(JournalError::from)
    }

    /// Loads the latest restart classification for one child task.
    ///
    /// # Errors
    /// Returns a journal error when the query or numeric conversion fails.
    #[cfg(test)]
    pub fn orphan_child_recovery_for_task(
        &self,
        task_id: &str,
    ) -> Result<Option<OrphanChildRecoveryOutcome>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        guard
            .query_row(
                r#"
                    SELECT
                        task_ulid, child_generation, outcome, reason_code,
                        child_run_state, budget_tokens, attempt_count, max_attempts
                    FROM orphan_child_recovery_v1
                    WHERE task_ulid = ?1
                    ORDER BY updated_at_unix_ms DESC
                    LIMIT 1
                "#,
                params![task_id],
                map_orphan_child_recovery_row,
            )
            .optional()
            .map_err(JournalError::from)
    }

    /// Classifies child orphans and delivers pending completion announcements
    /// exactly once to a current or durably suspended parent generation.
    ///
    /// # Errors
    /// Returns a journal error when durable classification, fencing, redaction,
    /// or tape delivery fails.
    pub fn reconcile_child_completions(
        &self,
    ) -> Result<ChildCompletionReconcileReport, JournalError> {
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        materialize_missing_terminal_completions_tx(&transaction)?;
        let classified_orphans = classify_orphan_children_tx(&transaction, now)?;
        let pending = load_pending_announcements_tx(&transaction)?;
        let mut report =
            ChildCompletionReconcileReport { classified_orphans, ..Default::default() };

        for announcement in pending {
            let nested_children = active_nested_child_count_tx(
                &transaction,
                announcement.envelope.child_session_id.as_str(),
            )?;
            if nested_children > 0 {
                update_announce_state_tx(
                    &transaction,
                    announcement.intent.announce_intent_id.as_str(),
                    "waiting_nested",
                    "child.announce.waiting_nested_descendants",
                    None,
                    now,
                )?;
                report.deferred_for_nested_children =
                    report.deferred_for_nested_children.saturating_add(1);
                continue;
            }
            if announcement.envelope.merge_safety_verdict != "matched" {
                let reason_code = match announcement.envelope.merge_safety_verdict.as_str() {
                    "approval_required" => "child.announce.approval_required",
                    _ => "child.announce.merge_safety_conflict",
                };
                update_announce_state_tx(
                    &transaction,
                    announcement.intent.announce_intent_id.as_str(),
                    "manual_review",
                    reason_code,
                    None,
                    now,
                )?;
                report.manual_review_announcements =
                    report.manual_review_announcements.saturating_add(1);
                continue;
            }
            let parent_state = transaction
                .query_row(
                    "SELECT state FROM orchestrator_runs WHERE run_ulid = ?1",
                    params![announcement.envelope.parent_run_id],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            let Some(parent_state) = parent_state else {
                update_announce_state_tx(
                    &transaction,
                    announcement.intent.announce_intent_id.as_str(),
                    "manual_review",
                    "child.announce.parent_missing",
                    None,
                    now,
                )?;
                report.manual_review_announcements =
                    report.manual_review_announcements.saturating_add(1);
                continue;
            };
            if matches!(parent_state.as_str(), "cancelled" | "done") {
                update_announce_state_tx(
                    &transaction,
                    announcement.intent.announce_intent_id.as_str(),
                    "cancelled",
                    "child.announce.parent_terminal",
                    None,
                    now,
                )?;
                report.cancelled_announcements = report.cancelled_announcements.saturating_add(1);
                continue;
            }
            if parent_state == "failed" {
                update_announce_state_tx(
                    &transaction,
                    announcement.intent.announce_intent_id.as_str(),
                    "manual_review",
                    "child.announce.parent_failed",
                    None,
                    now,
                )?;
                report.manual_review_announcements =
                    report.manual_review_announcements.saturating_add(1);
                continue;
            }
            let generation_current = parent_generation_accepts_announcement_tx(
                &transaction,
                &announcement.envelope,
                parent_state.as_str(),
                now,
            )?;
            if !generation_current {
                if matches!(
                    parent_state.as_str(),
                    "accepted" | "in_progress" | "running" | "suspended_waiting_child"
                ) {
                    update_announce_state_tx(
                        &transaction,
                        announcement.intent.announce_intent_id.as_str(),
                        "stale",
                        "child.announce.parent_generation_stale",
                        None,
                        now,
                    )?;
                    report.stale_announcements = report.stale_announcements.saturating_add(1);
                }
                continue;
            }

            let tape_seq = next_orchestrator_tape_seq(
                &transaction,
                announcement.envelope.parent_run_id.as_str(),
            )?;
            let payload = json!({
                "schema_version": CHILD_COMPLETION_SCHEMA_VERSION,
                "envelope_id": announcement.envelope.envelope_id,
                "task_id": announcement.envelope.task_id,
                "child_session_id": announcement.envelope.child_session_id,
                "child_run_id": announcement.envelope.child_run_id,
                "child_generation": announcement.envelope.child_generation,
                "terminal_state": announcement.envelope.terminal_state,
                "summary": announcement.envelope.summary_text,
                "structured_result": serde_json::from_str::<Value>(
                    announcement.envelope.structured_result_json.as_str()
                ).unwrap_or(Value::Null),
                "artifact_refs": serde_json::from_str::<Value>(
                    announcement.envelope.artifact_refs_json.as_str()
                ).unwrap_or_else(|_| json!([])),
                "evidence_refs": serde_json::from_str::<Value>(
                    announcement.envelope.evidence_refs_json.as_str()
                ).unwrap_or_else(|_| json!([])),
                "verification_status": announcement.envelope.verification_status,
                "merge_safety_verdict": announcement.envelope.merge_safety_verdict,
                "reason_code": "child.announce.delivered",
            });
            append_orchestrator_tape_event_tx(
                &transaction,
                self.config.max_payload_bytes,
                &OrchestratorTapeAppendRequest {
                    run_id: announcement.envelope.parent_run_id.clone(),
                    seq: tape_seq,
                    event_type: "child_completion.announced".to_owned(),
                    payload_json: payload.to_string(),
                },
                now,
            )?;
            update_announce_state_tx(
                &transaction,
                announcement.intent.announce_intent_id.as_str(),
                "delivered",
                "child.announce.delivered",
                Some(tape_seq),
                now,
            )?;
            report.delivered_announcements = report.delivered_announcements.saturating_add(1);
        }
        transaction.commit()?;
        Ok(report)
    }
}

pub(super) fn materialize_child_completion_tx(
    connection: &Connection,
    task: &OrchestratorBackgroundTaskRecord,
    parent_generation: Option<RuntimeGeneration>,
) -> Result<(), JournalError> {
    let Some(terminal_state) = AuxiliaryTaskState::from_str(task.state.as_str()) else {
        return Ok(());
    };
    if !terminal_state.is_terminal() {
        return Ok(());
    }
    let (Some(child_session_id), Some(parent_run_id), Some(child_run_id)) = (
        task.child_session_id.as_deref(),
        task.parent_run_id.as_deref(),
        task.target_run_id.as_deref().or(task.planned_child_run_id.as_deref()),
    ) else {
        return Ok(());
    };
    let parent_generation = match parent_generation
        .or_else(|| task.cancellation_context.as_ref().map(|context| context.generation))
    {
        Some(generation) => generation.get(),
        None => shared_runtime::active_runtime_generation_tx(
            connection,
            task.session_id.as_str(),
            parent_run_id,
            RuntimeGenerationLane::Run,
            task.updated_at_unix_ms,
        )?
        .map_or(0, |record| record.generation.get()),
    };
    let projection = child_completion_projection(task)?;
    let dedupe_key = format!(
        "{}:{}:{}:{}",
        child_run_id, task.execution_generation, parent_run_id, parent_generation
    );
    connection.execute(
        r#"
            INSERT OR IGNORE INTO child_completion_envelopes_v1 (
                envelope_ulid, dedupe_key, task_ulid, child_session_ulid,
                child_run_ulid, child_generation, parent_session_ulid,
                parent_run_ulid, parent_generation, terminal_state,
                summary_text, structured_result_json, artifact_refs_json,
                evidence_refs_json, verification_status, merge_preview_sha256,
                merge_actual_sha256, merge_safety_verdict, schema_version,
                created_at_unix_ms
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
                ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, 1, ?19
            )
        "#,
        params![
            Ulid::generate().to_string(),
            dedupe_key,
            task.task_id,
            child_session_id,
            child_run_id,
            u64_to_sqlite(task.execution_generation, "child_generation")?,
            task.session_id,
            parent_run_id,
            u64_to_sqlite(parent_generation, "parent_generation")?,
            task.state,
            projection.summary_text,
            projection.structured_result_json,
            projection.artifact_refs_json,
            projection.evidence_refs_json,
            projection.verification_status,
            projection.merge_preview_sha256,
            projection.merge_actual_sha256,
            projection.merge_safety_verdict,
            task.updated_at_unix_ms,
        ],
    )?;
    let envelope_id = connection.query_row(
        "SELECT envelope_ulid FROM child_completion_envelopes_v1 WHERE dedupe_key = ?1",
        params![dedupe_key],
        |row| row.get::<_, String>(0),
    )?;
    connection.execute(
        r#"
            INSERT OR IGNORE INTO child_announce_intents_v1 (
                announce_intent_ulid, envelope_ulid, dedupe_key, state,
                reason_code, attempt_count, delivered_tape_seq,
                created_at_unix_ms, updated_at_unix_ms
            ) VALUES (?1, ?2, ?3, 'pending', 'child.announce.pending', 0, NULL, ?4, ?4)
        "#,
        params![Ulid::generate().to_string(), envelope_id, dedupe_key, task.updated_at_unix_ms],
    )?;
    Ok(())
}

fn materialize_missing_terminal_completions_tx(
    connection: &Connection,
) -> Result<(), JournalError> {
    let task_ids = {
        let mut statement = connection.prepare(
            r#"
                SELECT tasks.task_ulid
                FROM orchestrator_background_tasks AS tasks
                WHERE tasks.child_session_ulid IS NOT NULL
                  AND tasks.parent_run_ulid IS NOT NULL
                  AND tasks.state IN ('succeeded', 'failed', 'cancelled', 'expired')
                  AND NOT EXISTS (
                      SELECT 1
                      FROM child_completion_envelopes_v1 AS envelopes
                      WHERE envelopes.task_ulid = tasks.task_ulid
                  )
                ORDER BY tasks.updated_at_unix_ms ASC, tasks.task_ulid ASC
                LIMIT ?1
            "#,
        )?;
        let rows = statement
            .query_map(params![CHILD_COMPLETION_SCAN_LIMIT], |row| row.get::<_, String>(0))?;
        rows.collect::<Result<Vec<_>, _>>()?
    };
    for task_id in task_ids {
        if let Some(task) = load_background_task_tx(connection, task_id.as_str())? {
            materialize_child_completion_tx(connection, &task, None)?;
        }
    }
    Ok(())
}

fn child_completion_projection(
    task: &OrchestratorBackgroundTaskRecord,
) -> Result<CompletionProjection, JournalError> {
    let raw_result = task.result_json.as_deref().unwrap_or("{}");
    let result = serde_json::from_str::<Value>(raw_result).unwrap_or(Value::Null);
    let run = result.get("run").unwrap_or(&Value::Null);
    let merge = run.get("merge_result").unwrap_or(&Value::Null);
    let summary = merge
        .get("summary_text")
        .and_then(Value::as_str)
        .or_else(|| result.get("summary").and_then(Value::as_str))
        .or(task.last_error.as_deref())
        .unwrap_or(task.state.as_str());
    let artifact_refs = merge
        .get("artifact_references")
        .or_else(|| result.get("artifact_refs"))
        .map(bounded_reference_array)
        .unwrap_or_else(|| json!([]));
    let evidence_refs =
        result.get("evidence_refs").map(bounded_reference_array).unwrap_or_else(|| json!([]));
    let verification_status = result
        .get("verification_status")
        .and_then(Value::as_str)
        .unwrap_or_else(|| if task.state == "succeeded" { "passed" } else { "incomplete" });
    let normalized_merge = normalized_merge_projection(merge);
    let actual_merge_sha256 = (!normalized_merge.is_null())
        .then(|| hex::encode(Sha256::digest(normalized_merge.to_string().as_bytes())));
    let preview_merge_sha256 = result
        .get("merge_preview_sha256")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| actual_merge_sha256.clone());
    let merge_safety_verdict = match (&preview_merge_sha256, &actual_merge_sha256) {
        (Some(preview), Some(actual)) if preview != actual => "conflict",
        _ if merge.get("approval_required").and_then(Value::as_bool).unwrap_or(false) => {
            "approval_required"
        }
        _ if merge.get("failure_category").is_some_and(|value| !value.is_null()) => "conflict",
        _ => "matched",
    }
    .to_owned();
    let structured = json!({
        "schema_version": CHILD_COMPLETION_SCHEMA_VERSION,
        "status": task.state,
        "task_id": task.task_id,
        "child_run_id": task.target_run_id.as_deref().or(task.planned_child_run_id.as_deref()),
        "summary": truncate_utf8(summary, CHILD_COMPLETION_SUMMARY_BYTES),
        "artifact_refs": artifact_refs,
        "evidence_refs": evidence_refs,
        "verification_status": verification_status,
        "merge": normalized_merge,
    });
    let structured_result_json =
        truncate_json_value(structured, CHILD_COMPLETION_RESULT_BYTES).to_string();
    Ok(CompletionProjection {
        summary_text: truncate_utf8(summary, CHILD_COMPLETION_SUMMARY_BYTES),
        structured_result_json,
        artifact_refs_json: artifact_refs.to_string(),
        evidence_refs_json: evidence_refs.to_string(),
        verification_status: truncate_utf8(verification_status, 128),
        merge_preview_sha256: preview_merge_sha256,
        merge_actual_sha256: actual_merge_sha256,
        merge_safety_verdict,
    })
}

fn bounded_reference_array(value: &Value) -> Value {
    let Some(items) = value.as_array() else {
        return json!([]);
    };
    Value::Array(
        items
            .iter()
            .take(CHILD_COMPLETION_REF_LIMIT)
            .filter_map(|item| match item {
                Value::String(reference) => {
                    Some(Value::String(truncate_utf8(reference, CHILD_COMPLETION_REF_FIELD_BYTES)))
                }
                Value::Object(object) => {
                    let mut bounded = serde_json::Map::new();
                    for key in
                        ["artifact_id", "reference", "path", "media_type", "sha256", "evidence_id"]
                    {
                        if let Some(value) = object.get(key).and_then(Value::as_str) {
                            bounded.insert(
                                key.to_owned(),
                                Value::String(truncate_utf8(
                                    value,
                                    CHILD_COMPLETION_REF_FIELD_BYTES,
                                )),
                            );
                        }
                    }
                    (!bounded.is_empty()).then_some(Value::Object(bounded))
                }
                _ => None,
            })
            .collect(),
    )
}

fn normalized_merge_projection(merge: &Value) -> Value {
    let Some(object) = merge.as_object() else {
        return Value::Null;
    };
    let mut normalized = serde_json::Map::new();
    for key in [
        "status",
        "strategy",
        "summary_text",
        "warnings",
        "failure_category",
        "approval_required",
        "approval_summary",
        "usage_summary",
        "artifact_references",
        "provenance",
    ] {
        if let Some(value) = object.get(key) {
            normalized.insert(key.to_owned(), value.clone());
        }
    }
    truncate_json_value(Value::Object(normalized), CHILD_COMPLETION_RESULT_BYTES / 2)
}

fn truncate_json_value(value: Value, max_bytes: usize) -> Value {
    let serialized = value.to_string();
    if serialized.len() <= max_bytes {
        value
    } else {
        json!({
            "truncated": true,
            "sha256": hex::encode(Sha256::digest(serialized.as_bytes())),
        })
    }
}

fn truncate_utf8(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_owned();
    }
    const TRUNCATION_MARKER: &str = "…";
    if max_bytes < TRUNCATION_MARKER.len() {
        return String::new();
    }
    let mut boundary = max_bytes - TRUNCATION_MARKER.len();
    while boundary > 0 && !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    let mut output = String::with_capacity(max_bytes);
    output.push_str(&value[..boundary]);
    output.push_str(TRUNCATION_MARKER);
    output
}

fn classify_orphan_children_tx(connection: &Connection, now: i64) -> Result<u64, JournalError> {
    let tasks = {
        let mut statement = connection.prepare(
            r#"
                SELECT
                    task_ulid, state, execution_generation, target_run_ulid,
                    planned_child_run_ulid, attempt_count, max_attempts, budget_tokens
                FROM orchestrator_background_tasks
                WHERE child_session_ulid IS NOT NULL
                  AND parent_run_ulid IS NOT NULL
                ORDER BY updated_at_unix_ms ASC, task_ulid ASC
                LIMIT ?1
            "#,
        )?;
        let rows = statement.query_map(params![CHILD_COMPLETION_SCAN_LIMIT], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, i64>(7)?,
            ))
        })?;
        rows.collect::<Result<Vec<_>, _>>()?
    };
    let mut classified = 0_u64;
    for (
        task_id,
        task_state,
        child_generation,
        target_run_id,
        planned_child_run_id,
        attempt_count,
        max_attempts,
        budget_tokens,
    ) in tasks
    {
        let child_run_id = target_run_id.as_ref().or(planned_child_run_id.as_ref());
        let child_run_state = child_run_id
            .map(|run_id| {
                connection
                    .query_row(
                        "SELECT state FROM orchestrator_runs WHERE run_ulid = ?1",
                        params![run_id],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()
            })
            .transpose()?
            .flatten();
        let terminal = AuxiliaryTaskState::from_str(task_state.as_str())
            .is_some_and(AuxiliaryTaskState::is_terminal);
        let (outcome, reason_code) = if terminal {
            ("terminal_notice", "child.recovery.terminal_notice")
        } else {
            match child_run_state.as_deref() {
                Some("accepted" | "in_progress" | "running") => {
                    ("resume", "child.recovery.resume_attached_run")
                }
                Some("pending") => ("wait", "child.recovery.wait_pending_run"),
                Some("done" | "failed" | "cancelled" | "expired") => {
                    ("terminal_notice", "child.recovery.terminal_run")
                }
                _ if attempt_count < max_attempts => ("retry", "child.recovery.retry_missing_run"),
                _ => ("manual_review", "child.recovery.retry_budget_exhausted"),
            }
        };
        let recovery = OrphanChildRecoveryOutcome {
            task_id,
            child_generation: journal_nonnegative_u64(
                child_generation,
                "orphan_child_recovery.child_generation",
            )?,
            outcome: outcome.to_owned(),
            reason_code: reason_code.to_owned(),
            child_run_state,
            budget_tokens: journal_nonnegative_u64(
                budget_tokens,
                "orphan_child_recovery.budget_tokens",
            )?,
            attempt_count: journal_nonnegative_u64(
                attempt_count,
                "orphan_child_recovery.attempt_count",
            )?,
            max_attempts: journal_nonnegative_u64(
                max_attempts,
                "orphan_child_recovery.max_attempts",
            )?,
        };
        connection.execute(
            r#"
                INSERT INTO orphan_child_recovery_v1 (
                    recovery_ulid, task_ulid, child_generation, outcome,
                    reason_code, child_run_state, budget_tokens, attempt_count,
                    max_attempts, created_at_unix_ms, updated_at_unix_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?10)
                ON CONFLICT(task_ulid, child_generation) DO UPDATE SET
                    outcome = excluded.outcome,
                    reason_code = excluded.reason_code,
                    child_run_state = excluded.child_run_state,
                    updated_at_unix_ms = excluded.updated_at_unix_ms
            "#,
            params![
                Ulid::generate().to_string(),
                recovery.task_id,
                u64_to_sqlite(recovery.child_generation, "child_generation")?,
                recovery.outcome,
                recovery.reason_code,
                recovery.child_run_state,
                u64_to_sqlite(recovery.budget_tokens, "budget_tokens")?,
                u64_to_sqlite(recovery.attempt_count, "attempt_count")?,
                u64_to_sqlite(recovery.max_attempts, "max_attempts")?,
                now,
            ],
        )?;
        classified = classified.saturating_add(1);
    }
    Ok(classified)
}

fn load_pending_announcements_tx(
    connection: &Connection,
) -> Result<Vec<PendingAnnouncement>, JournalError> {
    let query = format!(
        "SELECT {CHILD_ANNOUNCE_COLUMNS}, {CHILD_COMPLETION_COLUMNS} \
         FROM child_announce_intents_v1 AS intents \
         INNER JOIN child_completion_envelopes_v1 AS envelopes \
            ON envelopes.envelope_ulid = intents.envelope_ulid \
         WHERE intents.state IN ('pending', 'waiting_nested') \
         ORDER BY intents.created_at_unix_ms ASC, intents.announce_intent_ulid ASC \
         LIMIT {CHILD_COMPLETION_SCAN_LIMIT}"
    );
    let mut statement = connection.prepare(query.as_str())?;
    let rows = statement.query_map([], |row| {
        Ok(PendingAnnouncement {
            intent: map_child_announce_row(row)?,
            envelope: map_child_completion_row_at(row, 8)?,
        })
    })?;
    rows.collect::<Result<Vec<_>, _>>().map_err(JournalError::from)
}

fn active_nested_child_count_tx(
    connection: &Connection,
    child_session_id: &str,
) -> Result<i64, JournalError> {
    let mut pending = vec![(child_session_id.to_owned(), 0_usize)];
    let mut visited = BTreeSet::new();
    let mut active_count = 0_i64;
    while let Some((session_id, depth)) = pending.pop() {
        if !visited.insert(session_id.clone()) {
            continue;
        }
        if visited.len() > CHILD_COMPLETION_MAX_DESCENDANT_SESSIONS {
            return Ok(active_count.saturating_add(1));
        }
        active_count = active_count.saturating_add(connection.query_row(
            r#"
                SELECT COUNT(*)
                FROM orchestrator_background_tasks
                WHERE session_ulid = ?1
                  AND state IN ('queued', 'running', 'paused', 'cancel_requested')
            "#,
            params![session_id],
            |row| row.get::<_, i64>(0),
        )?);
        let descendants = {
            let mut statement = connection.prepare(
                r#"
                    SELECT session_ulid
                    FROM orchestrator_sessions
                    WHERE parent_session_ulid = ?1
                    ORDER BY session_ulid ASC
                "#,
            )?;
            let rows = statement.query_map(params![session_id], |row| row.get::<_, String>(0))?;
            rows.collect::<Result<Vec<_>, _>>()?
        };
        if !descendants.is_empty() && depth >= CHILD_COMPLETION_MAX_NESTING_DEPTH {
            return Ok(active_count.saturating_add(1));
        }
        pending.extend(
            descendants.into_iter().rev().map(|descendant| (descendant, depth.saturating_add(1))),
        );
    }
    Ok(active_count)
}

fn parent_generation_accepts_announcement_tx(
    connection: &Connection,
    envelope: &ChildCompletionEnvelope,
    parent_state: &str,
    now: i64,
) -> Result<bool, JournalError> {
    if matches!(parent_state, "suspended_waiting_child") {
        return connection
            .query_row(
                r#"
                    SELECT EXISTS(
                        SELECT 1
                        FROM parent_suspensions_v1
                        WHERE parent_run_ulid = ?1
                          AND parent_generation = ?2
                          AND state IN ('waiting', 'wake_pending')
                    )
                "#,
                params![
                    envelope.parent_run_id,
                    u64_to_sqlite(envelope.parent_generation, "parent_generation")?,
                ],
                |row| row.get::<_, bool>(0),
            )
            .map_err(JournalError::from);
    }
    let active = shared_runtime::active_runtime_generation_tx(
        connection,
        envelope.parent_session_id.as_str(),
        envelope.parent_run_id.as_str(),
        RuntimeGenerationLane::Run,
        now,
    )?;
    Ok(active.is_some_and(|record| record.generation.get() == envelope.parent_generation))
}

fn update_announce_state_tx(
    connection: &Connection,
    intent_id: &str,
    state: &str,
    reason_code: &str,
    tape_seq: Option<i64>,
    now: i64,
) -> Result<(), JournalError> {
    connection.execute(
        r#"
            UPDATE child_announce_intents_v1
            SET state = ?2,
                reason_code = ?3,
                attempt_count = attempt_count + 1,
                delivered_tape_seq = COALESCE(?4, delivered_tape_seq),
                updated_at_unix_ms = ?5
            WHERE announce_intent_ulid = ?1
              AND state IN ('pending', 'waiting_nested')
        "#,
        params![intent_id, state, reason_code, tape_seq, now],
    )?;
    Ok(())
}

#[cfg(test)]
fn load_child_completion_for_task_tx(
    connection: &Connection,
    task_id: &str,
) -> Result<Option<ChildCompletionEnvelope>, JournalError> {
    let query = format!(
        "SELECT {CHILD_COMPLETION_COLUMNS} \
         FROM child_completion_envelopes_v1 AS envelopes \
         WHERE task_ulid = ?1 \
         ORDER BY created_at_unix_ms DESC LIMIT 1"
    );
    connection
        .query_row(query.as_str(), params![task_id], map_child_completion_row)
        .optional()
        .map_err(JournalError::from)
}

const CHILD_COMPLETION_COLUMNS: &str = r#"
    envelopes.envelope_ulid,
    envelopes.task_ulid,
    envelopes.child_session_ulid,
    envelopes.child_run_ulid,
    envelopes.child_generation,
    envelopes.parent_session_ulid,
    envelopes.parent_run_ulid,
    envelopes.parent_generation,
    envelopes.terminal_state,
    envelopes.summary_text,
    envelopes.structured_result_json,
    envelopes.artifact_refs_json,
    envelopes.evidence_refs_json,
    envelopes.verification_status,
    envelopes.merge_preview_sha256,
    envelopes.merge_actual_sha256,
    envelopes.merge_safety_verdict,
    envelopes.created_at_unix_ms
"#;

#[cfg(test)]
fn map_child_completion_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ChildCompletionEnvelope> {
    map_child_completion_row_at(row, 0)
}

fn map_child_completion_row_at(
    row: &rusqlite::Row<'_>,
    offset: usize,
) -> rusqlite::Result<ChildCompletionEnvelope> {
    let child_generation = row.get::<_, i64>(offset + 4)?;
    let parent_generation = row.get::<_, i64>(offset + 7)?;
    Ok(ChildCompletionEnvelope {
        envelope_id: row.get(offset)?,
        task_id: row.get(offset + 1)?,
        child_session_id: row.get(offset + 2)?,
        child_run_id: row.get(offset + 3)?,
        child_generation: u64::try_from(child_generation)
            .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(offset + 4, child_generation))?,
        parent_session_id: row.get(offset + 5)?,
        parent_run_id: row.get(offset + 6)?,
        parent_generation: u64::try_from(parent_generation)
            .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(offset + 7, parent_generation))?,
        terminal_state: row.get(offset + 8)?,
        summary_text: row.get(offset + 9)?,
        structured_result_json: row.get(offset + 10)?,
        artifact_refs_json: row.get(offset + 11)?,
        evidence_refs_json: row.get(offset + 12)?,
        verification_status: row.get(offset + 13)?,
        merge_preview_sha256: row.get(offset + 14)?,
        merge_actual_sha256: row.get(offset + 15)?,
        merge_safety_verdict: row.get(offset + 16)?,
        created_at_unix_ms: row.get(offset + 17)?,
    })
}

const CHILD_ANNOUNCE_COLUMNS: &str = r#"
    intents.announce_intent_ulid,
    intents.envelope_ulid,
    intents.state,
    intents.reason_code,
    intents.attempt_count,
    intents.delivered_tape_seq,
    intents.created_at_unix_ms,
    intents.updated_at_unix_ms
"#;

fn map_child_announce_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ChildAnnounceIntent> {
    let attempt_count = row.get::<_, i64>(4)?;
    Ok(ChildAnnounceIntent {
        announce_intent_id: row.get(0)?,
        envelope_id: row.get(1)?,
        state: row.get(2)?,
        reason_code: row.get(3)?,
        attempt_count: u64::try_from(attempt_count)
            .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(4, attempt_count))?,
        delivered_tape_seq: row.get(5)?,
        created_at_unix_ms: row.get(6)?,
        updated_at_unix_ms: row.get(7)?,
    })
}

#[cfg(test)]
fn map_orphan_child_recovery_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<OrphanChildRecoveryOutcome> {
    let child_generation = row.get::<_, i64>(1)?;
    let budget_tokens = row.get::<_, i64>(5)?;
    let attempt_count = row.get::<_, i64>(6)?;
    let max_attempts = row.get::<_, i64>(7)?;
    Ok(OrphanChildRecoveryOutcome {
        task_id: row.get(0)?,
        child_generation: u64::try_from(child_generation)
            .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(1, child_generation))?,
        outcome: row.get(2)?,
        reason_code: row.get(3)?,
        child_run_state: row.get(4)?,
        budget_tokens: u64::try_from(budget_tokens)
            .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(5, budget_tokens))?,
        attempt_count: u64::try_from(attempt_count)
            .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(6, attempt_count))?,
        max_attempts: u64::try_from(max_attempts)
            .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(7, max_attempts))?,
    })
}

fn journal_nonnegative_u64(value: i64, field: &str) -> Result<u64, JournalError> {
    u64::try_from(value).map_err(|_| {
        JournalError::InvalidArgument(format!("{field} must be a non-negative 64-bit integer"))
    })
}

#[cfg(test)]
mod tests;
