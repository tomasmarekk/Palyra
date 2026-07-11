use super::*;

pub(super) fn load_failure_run_projection_from_snapshot(
    sandbox: &QaDaemonSandbox,
    database_path: &Path,
    _has_wal: bool,
    run_id: &str,
) -> Result<Option<QaFailureRunProjection>> {
    let database_uri = sqlite_read_only_uri(database_path, true)?;
    let connection = Connection::open_with_flags(
        database_uri,
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_NOFOLLOW
            | OpenFlags::SQLITE_OPEN_URI,
    )
    .context("qa.runner.failure_diagnostics_journal_unavailable")?;
    connection
        .execute_batch("PRAGMA query_only = ON; PRAGMA trusted_schema = OFF;")
        .context("qa.runner.failure_diagnostics_journal_unavailable")?;
    connection
        .busy_timeout(Duration::from_millis(250))
        .context("qa.runner.failure_diagnostics_journal_unavailable")?;
    let payload_limit = i64::try_from(MAX_FAILURE_PAYLOAD_BYTES)
        .context("qa.runner.failure_diagnostics_limit_invalid")?;
    let text_limit = i64::try_from(MAX_FAILURE_SQL_TEXT_BYTES)
        .context("qa.runner.failure_diagnostics_limit_invalid")?;
    let run = connection
        .query_row(
            r#"SELECT CASE WHEN typeof(state) = 'text'
                                   AND length(CAST(state AS BLOB)) <= ?2
                              THEN state ELSE NULL END,
                      cancel_requested,
                      CASE WHEN last_error IS NULL
                                 OR (typeof(last_error) = 'text'
                                     AND length(CAST(last_error AS BLOB)) <= ?2)
                           THEN last_error ELSE NULL END,
                      CASE WHEN last_error IS NULL
                                 OR (typeof(last_error) = 'text'
                                     AND length(CAST(last_error AS BLOB)) <= ?2)
                           THEN 1 ELSE 0 END
               FROM orchestrator_runs WHERE run_ulid = ?1"#,
            params![run_id, text_limit],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, i64>(1)? != 0,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, i64>(3)? != 0,
                ))
            },
        )
        .optional()
        .context("qa.runner.failure_diagnostics_journal_unavailable")?;
    let Some((state, cancel_requested, last_error, last_error_complete)) = run else {
        return Ok(None);
    };
    let state = state.context("qa.runner.failure_diagnostics_journal_text_invalid")?;
    let (tape_events, tape_events_complete) =
        load_failure_tape_events(sandbox, &connection, run_id, payload_limit)?;
    let (journal_events, journal_events_complete) =
        load_failure_journal_events(sandbox, &connection, run_id, payload_limit)?;
    let (last_error, last_error_projection_complete) = match last_error.as_deref() {
        Some(error) => {
            let (projected, complete) = sandbox.project_diagnostic_text(error);
            (Some(projected), complete)
        }
        None => (None, true),
    };
    Ok(Some(QaFailureRunProjection {
        state: sandbox.sanitize_diagnostic_text(state.as_str()),
        cancel_requested,
        last_error,
        last_error_complete: last_error_complete && last_error_projection_complete,
        tape_events_complete,
        journal_events_complete,
        tape_events,
        journal_events,
    }))
}

fn load_failure_tape_events(
    sandbox: &QaDaemonSandbox,
    connection: &Connection,
    run_id: &str,
    payload_limit: i64,
) -> Result<(Vec<QaFailureTapeEvent>, bool)> {
    let row_count = connection
        .query_row(
            "SELECT COUNT(*) FROM orchestrator_tape WHERE run_ulid = ?1",
            params![run_id],
            |row| row.get::<_, i64>(0),
        )
        .context("qa.runner.failure_diagnostics_journal_unavailable")?;
    let event_limit = i64::try_from(MAX_FAILURE_TAPE_EVENTS)
        .context("qa.runner.failure_diagnostics_limit_invalid")?;
    let text_limit = i64::try_from(MAX_FAILURE_SQL_TEXT_BYTES)
        .context("qa.runner.failure_diagnostics_limit_invalid")?;
    let mut statement = connection
        .prepare(
            r#"SELECT seq,
                      CASE WHEN typeof(event_type) = 'text'
                                 AND length(CAST(event_type AS BLOB)) <= ?2
                           THEN event_type ELSE NULL END,
                      CASE WHEN typeof(payload_json) = 'text'
                                 AND length(CAST(payload_json AS BLOB)) <= ?3
                           THEN payload_json ELSE NULL END
               FROM orchestrator_tape
               WHERE run_ulid = ?1
               ORDER BY seq DESC LIMIT ?4"#,
        )
        .context("qa.runner.failure_diagnostics_journal_unavailable")?;
    let rows = statement
        .query_map(params![run_id, text_limit, payload_limit, event_limit], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })
        .context("qa.runner.failure_diagnostics_journal_unavailable")?;
    let mut events = Vec::new();
    let mut events_complete = row_count <= event_limit;
    for row in rows {
        let (seq, event_type, payload) =
            row.context("qa.runner.failure_diagnostics_journal_unavailable")?;
        let Some(event_type) = event_type else {
            events_complete = false;
            continue;
        };
        let (fields, payload_complete) = project_failure_payload(sandbox, payload.as_deref());
        events.push(QaFailureTapeEvent {
            seq,
            event_type: sandbox.sanitize_diagnostic_text(event_type.as_str()),
            payload_complete,
            fields,
        });
    }
    events.reverse();
    Ok((events, events_complete))
}

fn load_failure_journal_events(
    sandbox: &QaDaemonSandbox,
    connection: &Connection,
    run_id: &str,
    payload_limit: i64,
) -> Result<(Vec<QaFailureJournalEvent>, bool)> {
    let row_count = connection
        .query_row(
            "SELECT COUNT(*) FROM journal_events WHERE run_ulid = ?1",
            params![run_id],
            |row| row.get::<_, i64>(0),
        )
        .context("qa.runner.failure_diagnostics_journal_unavailable")?;
    let event_limit = i64::try_from(MAX_FAILURE_JOURNAL_EVENTS)
        .context("qa.runner.failure_diagnostics_limit_invalid")?;
    let mut statement = connection
        .prepare(
            r#"SELECT seq, kind, actor, redacted,
                      CASE WHEN typeof(payload_json) = 'text'
                                 AND length(CAST(payload_json AS BLOB)) <= ?2
                           THEN payload_json ELSE NULL END
               FROM journal_events
               WHERE run_ulid = ?1
               ORDER BY seq DESC LIMIT ?3"#,
        )
        .context("qa.runner.failure_diagnostics_journal_unavailable")?;
    let rows = statement
        .query_map(params![run_id, payload_limit, event_limit], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i32>(1)?,
                row.get::<_, i32>(2)?,
                row.get::<_, i64>(3)? != 0,
                row.get::<_, Option<String>>(4)?,
            ))
        })
        .context("qa.runner.failure_diagnostics_journal_unavailable")?;
    let mut events = Vec::new();
    for row in rows {
        let (seq, kind, actor, redacted, payload) =
            row.context("qa.runner.failure_diagnostics_journal_unavailable")?;
        let (fields, payload_complete) = project_failure_payload(sandbox, payload.as_deref());
        events.push(QaFailureJournalEvent { seq, kind, actor, redacted, payload_complete, fields });
    }
    events.reverse();
    Ok((events, row_count <= event_limit))
}

pub(super) fn project_failure_payload(
    sandbox: &QaDaemonSandbox,
    payload: Option<&str>,
) -> (Map<String, Value>, bool) {
    let Some(payload) = payload else {
        return (Map::new(), false);
    };
    let Ok(value) = serde_json::from_str::<Value>(payload) else {
        return (Map::new(), false);
    };
    if !value.is_object() {
        return (Map::new(), false);
    }
    let mut fields = Map::new();
    let mut complete = true;
    collect_failure_payload_fields(sandbox, &value, 0, &mut fields, &mut complete);
    (fields, complete)
}

pub(super) fn collect_failure_payload_fields(
    sandbox: &QaDaemonSandbox,
    value: &Value,
    depth: usize,
    fields: &mut Map<String, Value>,
    complete: &mut bool,
) {
    if depth > MAX_FAILURE_PAYLOAD_DEPTH {
        *complete = false;
        return;
    }
    let Value::Object(object) = value else {
        *complete = false;
        return;
    };
    for (key, value) in object {
        if fields.len() >= MAX_FAILURE_PAYLOAD_FIELDS {
            *complete = false;
            break;
        }
        if is_sensitive_key(key) || is_failure_payload_container_denied(key) {
            *complete = false;
            continue;
        }
        if let Some(canonical_key) = failure_payload_field_name(key) {
            if let Some((projected, projected_complete)) =
                project_failure_payload_scalar(sandbox, value)
            {
                *complete &= projected_complete;
                if fields.insert(canonical_key.to_owned(), projected).is_some() {
                    *complete = false;
                }
                continue;
            }
            *complete = false;
        } else {
            *complete = false;
        }
        collect_failure_payload_fields(sandbox, value, depth.saturating_add(1), fields, complete);
    }
}

fn failure_payload_field_name(key: &str) -> Option<&'static str> {
    match key {
        "action" => Some("action"),
        "activation_id" => Some("activation_id"),
        "allowed" => Some("allowed"),
        "approval_id" => Some("approval_id"),
        "approval_required" => Some("approval_required"),
        "approved" => Some("approved"),
        "backend" => Some("backend"),
        "decision" => Some("decision"),
        "decision_source" => Some("decision_source"),
        "error" => Some("error"),
        "event_name" => Some("event_name"),
        "execution_backend" => Some("execution_backend"),
        "executor" => Some("executor"),
        "occurrence" => Some("occurrence"),
        "outcome" => Some("outcome"),
        "point_id" => Some("point_id"),
        "policy_enforced" => Some("policy_enforced"),
        "proposal_id" => Some("proposal_id"),
        "reason" => Some("reason"),
        "reason_code" => Some("reason_code"),
        "recovery_class" => Some("recovery_class"),
        "sandbox_enforcement" => Some("sandbox_enforcement"),
        "state" => Some("state"),
        "status" => Some("status"),
        "success" => Some("success"),
        "timed_out" => Some("timed_out"),
        "tool_name" => Some("tool_name"),
        _ => None,
    }
}

fn is_failure_payload_container_denied(key: &str) -> bool {
    matches!(
        key,
        "arguments"
            | "content"
            | "env"
            | "environment"
            | "headers"
            | "input"
            | "input_json"
            | "message"
            | "output"
            | "output_json"
            | "payload"
            | "prompt"
            | "reply_text"
            | "request"
            | "response"
            | "text"
    )
}

fn project_failure_payload_scalar(
    sandbox: &QaDaemonSandbox,
    value: &Value,
) -> Option<(Value, bool)> {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) => Some((value.clone(), true)),
        Value::String(text) => {
            let (projected, complete) = sandbox.project_diagnostic_text(text);
            Some((Value::String(projected), complete))
        }
        Value::Array(values) => {
            let mut complete = values.len() <= MAX_FAILURE_PAYLOAD_ARRAY_ITEMS;
            let mut projected =
                Vec::with_capacity(values.len().min(MAX_FAILURE_PAYLOAD_ARRAY_ITEMS));
            for value in values.iter().take(MAX_FAILURE_PAYLOAD_ARRAY_ITEMS) {
                let (value, value_complete) = project_failure_payload_scalar(sandbox, value)?;
                complete &= value_complete;
                projected.push(value);
            }
            Some((Value::Array(projected), complete))
        }
        Value::Object(_) => None,
    }
}
