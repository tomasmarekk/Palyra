//! Transactional initialization and lookup for authoritative-V2 objective
//! plans. The plan item is session-durable while its creation audit is also
//! projected onto the source run's tape.

use super::*;

pub(super) fn validate_plan_ensure_request(
    request: &V2ComplexPlanEnsureRequest,
) -> Result<(), JournalError> {
    for (value, field) in [
        (request.plan_item_id.as_str(), "plan_item_id"),
        (request.objective_id.as_str(), "objective_id"),
        (request.session_id.as_str(), "session_id"),
        (request.root_run_id.as_str(), "root_run_id"),
        (request.source_run_id.as_str(), "source_run_id"),
        (request.owner_principal.as_str(), "owner_principal"),
        (request.device_id.as_str(), "device_id"),
        (request.actor_principal.as_str(), "actor_principal"),
        (request.title.as_str(), "title"),
        (request.focus.as_str(), "focus"),
    ] {
        ensure_nonempty_field(value, field)?;
    }
    Ok(())
}

pub(super) fn ensure_v2_complex_plan_tx(
    transaction: &Transaction<'_>,
    max_payload_bytes: usize,
    request: &V2ComplexPlanEnsureRequest,
    now: i64,
) -> Result<V2ComplexPlanEnsureOutcome, JournalError> {
    validate_plan_scope(transaction, request)?;
    deactivate_terminal_root_links(transaction, request.objective_id.as_str(), now)?;
    if let Some((link, plan_item)) =
        load_active_root_plan(transaction, request.objective_id.as_str())?
    {
        return Ok(V2ComplexPlanEnsureOutcome { created: false, plan_item, link });
    }

    let title = sanitize_plan_text_field(request.title.as_str(), "title", "agent_plan.title")?;
    let focus = sanitize_plan_text_field(request.focus.as_str(), "focus", "agent_plan.focus")?;
    let redaction_level = if title != request.title.trim() || focus != request.focus.trim() {
        "redacted"
    } else {
        "none"
    };
    let details_json = json!({
        "schema_version": 1,
        "objective_id": request.objective_id,
        "focus": focus,
        "authoritative_runtime": "v2",
        "auto_initialized": true,
    })
    .to_string();
    let payload_json = sanitize_json_payload_field(
        details_json.as_str(),
        "agent_plan.payload_json",
        max_payload_bytes,
    )?;
    transaction
        .execute(
            r#"
            INSERT INTO agent_plan_items (
                plan_item_ulid, session_ulid, run_ulid, parent_run_ulid,
                owner_principal, device_id, channel, title, details_json,
                status, priority, blocked_reason, evidence_refs_json,
                redaction_level, reason_code, created_at_unix_ms,
                updated_at_unix_ms, completed_at_unix_ms, cancelled_at_unix_ms
            ) VALUES (
                ?1, ?2, NULL, NULL, ?3, ?4, ?5, ?6, ?7,
                'in_progress', 100, NULL, '[]', ?8, ?9, ?10, ?10, NULL, NULL
            )
        "#,
            params![
                request.plan_item_id,
                request.session_id,
                request.owner_principal,
                request.device_id,
                request.channel,
                title,
                payload_json,
                redaction_level,
                AUTO_PLAN_REASON,
                now,
            ],
        )
        .map_err(|error| match error {
            rusqlite::Error::SqliteFailure(sqlite, message)
                if sqlite.code == ErrorCode::ConstraintViolation =>
            {
                JournalError::InvalidArgument(format!(
                    "automatic plan item {} conflicts with durable state: {}",
                    request.plan_item_id,
                    message.unwrap_or_else(|| "constraint violation".to_owned())
                ))
            }
            error => JournalError::from(error),
        })?;
    insert_agent_plan_event(
        transaction,
        AgentPlanEventInsert {
            event_id: Ulid::new().to_string(),
            plan_item_id: request.plan_item_id.as_str(),
            session_id: request.session_id.as_str(),
            run_id: Some(request.source_run_id.as_str()),
            event_type: "agent.plan.created",
            actor_principal: request.actor_principal.as_str(),
            from_status: None,
            to_status: Some("in_progress"),
            reason_code: AUTO_PLAN_REASON,
            summary: "authoritative V2 complex-task plan initialized",
            payload_json: payload_json.as_str(),
            evidence_refs_json: "[]",
            redaction_level,
            created_at_unix_ms: now,
        },
    )?;
    append_agent_plan_tape_event_if_scoped(
        transaction,
        max_payload_bytes,
        Some(request.source_run_id.as_str()),
        "agent.plan.created",
        AgentPlanTapeEvent {
            plan_item_id: request.plan_item_id.as_str(),
            session_id: request.session_id.as_str(),
            event_type: "agent.plan.created",
            from_status: None,
            to_status: Some("in_progress"),
            reason_code: AUTO_PLAN_REASON,
            redaction_level,
            payload_json: payload_json.as_str(),
            evidence_refs_json: "[]",
        },
        now,
    )?;
    transaction.execute(
        r#"
            INSERT INTO plan_objective_links_v1 (
                objective_ulid, plan_item_ulid, session_ulid, root_run_ulid,
                focus, is_root, active, schema_version,
                created_at_unix_ms, updated_at_unix_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, 1, 1, ?6, ?7, ?7)
        "#,
        params![
            request.objective_id,
            request.plan_item_id,
            request.session_id,
            request.root_run_id,
            focus,
            SCHEMA_VERSION,
            now,
        ],
    )?;
    insert_link_event(
        transaction,
        request.objective_id.as_str(),
        request.plan_item_id.as_str(),
        "agent.plan.objective_linked",
        json!({
            "schema_version": 1,
            "root": true,
            "focus": focus,
            "source_run_id": request.source_run_id,
        })
        .to_string()
        .as_str(),
        now,
    )?;
    let plan_item = query_agent_plan_item_by_id(transaction, request.plan_item_id.as_str())?
        .ok_or_else(|| JournalError::AgentPlanItemNotFound {
            plan_item_id: request.plan_item_id.clone(),
        })?;
    let link =
        load_plan_link(transaction, request.objective_id.as_str(), request.plan_item_id.as_str())?
            .ok_or_else(|| {
                JournalError::InvalidArgument("automatic plan link is missing".to_owned())
            })?;
    Ok(V2ComplexPlanEnsureOutcome { created: true, plan_item, link })
}

pub(super) fn map_plan_link_row(
    row: &rusqlite::Row<'_>,
) -> Result<PlanObjectiveLink, rusqlite::Error> {
    Ok(PlanObjectiveLink {
        objective_id: row.get(0)?,
        plan_item_id: row.get(1)?,
        session_id: row.get(2)?,
        root_run_id: row.get(3)?,
        focus: row.get(4)?,
        is_root: row.get::<_, i64>(5)? != 0,
        active: row.get::<_, i64>(6)? != 0,
        created_at_unix_ms: row.get(7)?,
        updated_at_unix_ms: row.get(8)?,
    })
}

fn validate_plan_scope(
    connection: &Connection,
    request: &V2ComplexPlanEnsureRequest,
) -> Result<(), JournalError> {
    let session_scope = connection
        .query_row(
            r#"
                SELECT principal, device_id, channel
                FROM orchestrator_sessions
                WHERE session_ulid = ?1
            "#,
            params![request.session_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            },
        )
        .optional()?
        .ok_or_else(|| {
            JournalError::InvalidArgument(format!(
                "automatic plan session {} does not exist",
                request.session_id
            ))
        })?;
    if session_scope
        != (request.owner_principal.clone(), request.device_id.clone(), request.channel.clone())
    {
        return Err(JournalError::InvalidArgument(
            "automatic plan owner, device, or channel scope changed".to_owned(),
        ));
    }
    let run_scope = format!("run:{}", request.root_run_id);
    if request.objective_id == run_scope {
        let root_session = connection
            .query_row(
                "SELECT session_ulid FROM orchestrator_runs WHERE run_ulid = ?1",
                params![request.root_run_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .ok_or_else(|| JournalError::RunNotFound { run_id: request.root_run_id.clone() })?;
        if root_session != request.session_id {
            return Err(JournalError::InvalidArgument(
                "automatic run-scoped plan is outside the requested session".to_owned(),
            ));
        }
    } else {
        let binding = connection
            .query_row(
                r#"
                    SELECT session_ulid, root_run_ulid
                    FROM objective_runtime_bindings_v1
                    WHERE objective_ulid = ?1
                "#,
                params![request.objective_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?
            .ok_or_else(|| {
                JournalError::InvalidArgument(format!(
                    "objective binding {} does not exist",
                    request.objective_id
                ))
            })?;
        if binding != (request.session_id.clone(), request.root_run_id.clone()) {
            return Err(JournalError::InvalidArgument(format!(
                "objective plan {} binding scope changed",
                request.objective_id
            )));
        }
    }
    let source_session = connection
        .query_row(
            "SELECT session_ulid FROM orchestrator_runs WHERE run_ulid = ?1",
            params![request.source_run_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .ok_or_else(|| JournalError::RunNotFound { run_id: request.source_run_id.clone() })?;
    if source_session != request.session_id {
        return Err(JournalError::InvalidArgument(
            "automatic plan source run is outside the objective session".to_owned(),
        ));
    }
    Ok(())
}

fn load_active_root_plan(
    connection: &Connection,
    objective_id: &str,
) -> Result<Option<(PlanObjectiveLink, AgentPlanItemRecord)>, JournalError> {
    let link = connection
        .query_row(
            r#"
                SELECT
                    links.objective_ulid, links.plan_item_ulid, links.session_ulid,
                    links.root_run_ulid, links.focus, links.is_root, links.active,
                    links.created_at_unix_ms, links.updated_at_unix_ms
                FROM plan_objective_links_v1 AS links
                JOIN agent_plan_items AS plans
                  ON plans.plan_item_ulid = links.plan_item_ulid
                WHERE links.objective_ulid = ?1
                  AND links.is_root = 1
                  AND links.active = 1
                  AND plans.status NOT IN ('completed', 'cancelled')
                LIMIT 1
            "#,
            params![objective_id],
            map_plan_link_row,
        )
        .optional()?;
    let Some(link) = link else {
        return Ok(None);
    };
    let plan_item = query_agent_plan_item_by_id(connection, link.plan_item_id.as_str())?
        .ok_or_else(|| JournalError::AgentPlanItemNotFound {
            plan_item_id: link.plan_item_id.clone(),
        })?;
    Ok(Some((link, plan_item)))
}

fn load_plan_link(
    connection: &Connection,
    objective_id: &str,
    plan_item_id: &str,
) -> Result<Option<PlanObjectiveLink>, JournalError> {
    connection
        .query_row(
            r#"
                SELECT
                    objective_ulid, plan_item_ulid, session_ulid, root_run_ulid,
                    focus, is_root, active, created_at_unix_ms, updated_at_unix_ms
                FROM plan_objective_links_v1
                WHERE objective_ulid = ?1 AND plan_item_ulid = ?2
            "#,
            params![objective_id, plan_item_id],
            map_plan_link_row,
        )
        .optional()
        .map_err(JournalError::from)
}

fn deactivate_terminal_root_links(
    transaction: &Transaction<'_>,
    objective_id: &str,
    now: i64,
) -> Result<(), JournalError> {
    let mut statement = transaction.prepare(
        r#"
            SELECT links.plan_item_ulid
            FROM plan_objective_links_v1 AS links
            JOIN agent_plan_items AS plans
              ON plans.plan_item_ulid = links.plan_item_ulid
            WHERE links.objective_ulid = ?1
              AND links.is_root = 1
              AND links.active = 1
              AND plans.status IN ('completed', 'cancelled')
        "#,
    )?;
    let rows = statement.query_map(params![objective_id], |row| row.get::<_, String>(0))?;
    let terminal_plan_ids = rows.collect::<Result<Vec<_>, _>>()?;
    drop(statement);
    for plan_item_id in terminal_plan_ids {
        transaction.execute(
            r#"
                UPDATE plan_objective_links_v1
                SET active = 0, updated_at_unix_ms = ?3
                WHERE objective_ulid = ?1 AND plan_item_ulid = ?2 AND active = 1
            "#,
            params![objective_id, plan_item_id, now],
        )?;
        insert_link_event(
            transaction,
            objective_id,
            plan_item_id.as_str(),
            "agent.plan.objective_link_deactivated",
            r#"{"schema_version":1,"reason":"plan_terminal"}"#,
            now,
        )?;
    }
    Ok(())
}

fn insert_link_event(
    transaction: &Transaction<'_>,
    objective_id: &str,
    plan_item_id: &str,
    event_type: &str,
    payload_json: &str,
    now: i64,
) -> Result<(), JournalError> {
    ensure_json_field(payload_json, "plan_objective_link_event.payload_json")?;
    transaction.execute(
        r#"
            INSERT INTO plan_objective_link_events_v1 (
                event_ulid, objective_ulid, plan_item_ulid, event_type,
                reason_code, payload_json, created_at_unix_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
        "#,
        params![
            Ulid::new().to_string(),
            objective_id,
            plan_item_id,
            event_type,
            AUTO_PLAN_REASON,
            payload_json,
            now,
        ],
    )?;
    Ok(())
}
