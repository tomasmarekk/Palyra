use crate::*;

pub(crate) fn run_sessions(command: SessionsCommand) -> Result<()> {
    let root_context = app::current_root_context()
        .ok_or_else(|| anyhow!("CLI root context is unavailable for sessions command"))?;
    let connection = root_context.resolve_grpc_connection(
        app::ConnectionOverrides::default(),
        app::ConnectionDefaults::USER,
    )?;
    let runtime = build_runtime()?;
    runtime.block_on(run_sessions_async(command, connection))
}

pub(crate) async fn run_sessions_async(
    command: SessionsCommand,
    connection: AgentConnection,
) -> Result<()> {
    let json = match &command {
        SessionsCommand::List { json, .. }
        | SessionsCommand::Show { json, .. }
        | SessionsCommand::Resolve { json, .. }
        | SessionsCommand::Rename { json, .. }
        | SessionsCommand::Reset { json, .. }
        | SessionsCommand::Cleanup { json, .. }
        | SessionsCommand::Abort { json, .. } => output::preferred_json(*json),
    };
    let runtime = client::operator::OperatorRuntime::new(connection);

    match command {
        SessionsCommand::List { after, limit, include_archived, json: _, ndjson } => {
            let ndjson = output::preferred_ndjson(json, ndjson);
            let response = runtime.list_sessions(after, include_archived, limit).await?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "sessions": response.sessions.iter().map(session_to_json).collect::<Vec<_>>(),
                        "next_after_session_key": redacted_text_json_or_null(
                            response.next_after_session_key.as_str()
                        ),
                        "include_archived": include_archived,
                    }))?
                );
            } else if ndjson {
                for session in &response.sessions {
                    println!(
                        "{}",
                        serde_json::to_string(&json!({
                            "type": "session",
                            "session": session_to_json(session),
                        }))?
                    );
                }
            } else {
                println!(
                    "sessions.list count={} next_after={} include_archived={}",
                    response.sessions.len(),
                    redacted_text_or_none(!response.next_after_session_key.trim().is_empty()),
                    include_archived
                );
                for session in &response.sessions {
                    println!(
                        "session id={} key={} label={} updated_at_unix_ms={} last_run_id={} archived_at_unix_ms={}",
                        session_id_text(session),
                        redacted_text_or_none(!session.session_key.trim().is_empty()),
                        redacted_text_or_none(!session.session_label.trim().is_empty()),
                        session.updated_at_unix_ms,
                        redacted_presence_for_output(session.last_run_id.is_some()),
                        optional_unix_ms_text(session.archived_at_unix_ms)
                    );
                }
            }
        }
        SessionsCommand::Show { session_id, session_key, json: _ } => {
            let response = runtime
                .resolve_session(build_resolve_session_request(
                    session_id,
                    session_key,
                    None,
                    true,
                    false,
                )?)
                .await?;
            let session =
                response.session.context("ResolveSession returned empty session payload")?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "session": session_to_json(&session),
                        "created": response.created,
                        "reset_applied": response.reset_applied,
                    }))?
                );
            } else {
                println!(
                    "sessions.show id={} key={} label={} created_at_unix_ms={} updated_at_unix_ms={} last_run_id={} archived_at_unix_ms={}",
                    session_id_text(&session),
                    redacted_text_or_none(!session.session_key.trim().is_empty()),
                    redacted_text_or_none(!session.session_label.trim().is_empty()),
                    session.created_at_unix_ms,
                    session.updated_at_unix_ms,
                    redacted_presence_for_output(session.last_run_id.is_some()),
                    optional_unix_ms_text(session.archived_at_unix_ms)
                );
            }
        }
        SessionsCommand::Resolve {
            session_id,
            session_key,
            session_label,
            require_existing,
            reset,
            json: _,
        } => {
            let response = runtime
                .resolve_session(build_resolve_session_request(
                    session_id,
                    session_key,
                    session_label,
                    require_existing,
                    reset,
                )?)
                .await?;
            let session =
                response.session.context("ResolveSession returned empty session payload")?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "session": session_to_json(&session),
                        "created": response.created,
                        "reset_applied": response.reset_applied,
                    }))?
                );
            } else {
                println!(
                    "sessions.resolve id={} key={} label={} created={} reset_applied={} archived_at_unix_ms={}",
                    session_id_text(&session),
                    redacted_text_or_none(!session.session_key.trim().is_empty()),
                    redacted_text_or_none(!session.session_label.trim().is_empty()),
                    response.created,
                    response.reset_applied,
                    optional_unix_ms_text(session.archived_at_unix_ms)
                );
            }
        }
        SessionsCommand::Rename { session_id, session_label, json: _ } => {
            let response = runtime
                .resolve_session(gateway_v1::ResolveSessionRequest {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    session_id: Some(common_v1::CanonicalId {
                        ulid: resolve_or_generate_canonical_id(Some(session_id))?,
                    }),
                    session_key: String::new(),
                    session_label,
                    require_existing: true,
                    reset_session: false,
                })
                .await?;
            let session =
                response.session.context("ResolveSession returned empty session payload")?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "session": session_to_json(&session),
                        "created": response.created,
                        "reset_applied": response.reset_applied,
                    }))?
                );
            } else {
                println!(
                    "sessions.rename id={} label={} updated_at_unix_ms={} archived_at_unix_ms={}",
                    session_id_text(&session),
                    redacted_text_or_none(!session.session_label.trim().is_empty()),
                    session.updated_at_unix_ms,
                    optional_unix_ms_text(session.archived_at_unix_ms)
                );
            }
        }
        SessionsCommand::Reset { session_id, json: _ } => {
            let response = runtime
                .resolve_session(gateway_v1::ResolveSessionRequest {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    session_id: Some(common_v1::CanonicalId {
                        ulid: resolve_or_generate_canonical_id(Some(session_id))?,
                    }),
                    session_key: String::new(),
                    session_label: String::new(),
                    require_existing: true,
                    reset_session: true,
                })
                .await?;
            let session =
                response.session.context("ResolveSession returned empty session payload")?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "session": session_to_json(&session),
                        "created": response.created,
                        "reset_applied": response.reset_applied,
                    }))?
                );
            } else {
                println!(
                    "sessions.reset id={} reset_applied={} updated_at_unix_ms={} archived_at_unix_ms={}",
                    session_id_text(&session),
                    response.reset_applied,
                    session.updated_at_unix_ms,
                    optional_unix_ms_text(session.archived_at_unix_ms)
                );
            }
        }
        SessionsCommand::Cleanup { session_id, session_key, yes, dry_run, json: _ } => {
            let request = build_cleanup_session_request(session_id, session_key)?;
            if dry_run {
                let response = runtime
                    .resolve_session(gateway_v1::ResolveSessionRequest {
                        v: CANONICAL_PROTOCOL_MAJOR,
                        session_id: request.session_id.clone(),
                        session_key: request.session_key.clone(),
                        session_label: String::new(),
                        require_existing: true,
                        reset_session: false,
                    })
                    .await?;
                let session =
                    response.session.context("ResolveSession returned empty session payload")?;
                if json {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&json!({
                            "dry_run": true,
                            "session": session_to_json(&session),
                            "would_archive": session.archived_at_unix_ms == 0,
                        }))?
                    );
                } else {
                    println!(
                        "sessions.cleanup.dry_run id={} key={} archived_at_unix_ms={} would_archive={}",
                        session_id_text(&session),
                        redacted_text_or_none(!session.session_key.trim().is_empty()),
                        optional_unix_ms_text(session.archived_at_unix_ms),
                        session.archived_at_unix_ms == 0
                    );
                }
            } else {
                if !yes {
                    anyhow::bail!(
                        "sessions cleanup is destructive; rerun with --yes or preview with --dry-run"
                    );
                }
                let response = runtime.cleanup_session(request).await?;
                let session =
                    response.session.context("CleanupSession returned empty session payload")?;
                if json {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&json!({
                            "session": session_to_json(&session),
                            "cleaned": response.cleaned,
                            "newly_archived": response.newly_archived,
                            "previous_session_key": redacted_text_json_or_null(
                                &response.previous_session_key
                            ),
                            "run_count": response.run_count,
                        }))?
                    );
                } else {
                    println!(
                        "sessions.cleanup id={} cleaned={} newly_archived={} previous_key={} archived_at_unix_ms={} run_count={}",
                        session_id_text(&session),
                        response.cleaned,
                        response.newly_archived,
                        redacted_text_or_none(!response.previous_session_key.trim().is_empty()),
                        optional_unix_ms_text(session.archived_at_unix_ms),
                        response.run_count
                    );
                }
            }
        }
        SessionsCommand::Abort { run_id, reason, json: _ } => {
            let response =
                runtime.abort_run(resolve_or_generate_canonical_id(Some(run_id))?, reason).await?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "run_id": redacted_identifier_json_value(
                            response.run_id.as_ref().map(|_| "present")
                        ),
                        "cancel_requested": response.cancel_requested,
                        "reason": redacted_text_json_or_null(response.reason.as_str()),
                    }))?
                );
            } else {
                println!(
                    "sessions.abort run_id={} cancel_requested={} reason={}",
                    redacted_presence_for_output(response.run_id.is_some()),
                    response.cancel_requested,
                    redacted_text_or_none(!response.reason.trim().is_empty())
                );
            }
        }
    }

    std::io::stdout().flush().context("stdout flush failed")
}

fn build_resolve_session_request(
    session_id: Option<String>,
    session_key: Option<String>,
    session_label: Option<String>,
    require_existing: bool,
    reset_session: bool,
) -> Result<gateway_v1::ResolveSessionRequest> {
    if session_id.is_none() && session_key.is_none() {
        anyhow::bail!("session_id or session_key is required");
    }
    Ok(gateway_v1::ResolveSessionRequest {
        v: CANONICAL_PROTOCOL_MAJOR,
        session_id: session_id
            .map(|value| resolve_or_generate_canonical_id(Some(value)))
            .transpose()?
            .map(|ulid| common_v1::CanonicalId { ulid }),
        session_key: session_key.unwrap_or_default(),
        session_label: session_label.unwrap_or_default(),
        require_existing,
        reset_session,
    })
}

fn session_id_text(session: &gateway_v1::SessionSummary) -> String {
    if session.session_id.is_some() {
        REDACTED.to_owned()
    } else {
        "none".to_owned()
    }
}

fn session_to_json(session: &gateway_v1::SessionSummary) -> Value {
    json!({
        "session_id": if session.session_id.is_some() { Value::String(REDACTED.to_owned()) } else { Value::Null },
        "session_key": redacted_presence_json_value(!session.session_key.trim().is_empty()),
        "session_label": redacted_presence_json_value(!session.session_label.trim().is_empty()),
        "created_at_unix_ms": session.created_at_unix_ms,
        "updated_at_unix_ms": session.updated_at_unix_ms,
        "last_run_id": if session.last_run_id.is_some() { Value::String(REDACTED.to_owned()) } else { Value::Null },
        "archived_at_unix_ms": empty_unix_ms(session.archived_at_unix_ms),
    })
}

fn redacted_text_or_none(present: bool) -> String {
    redacted_presence_for_output(present)
}

fn redacted_presence_for_output(present: bool) -> String {
    if present {
        REDACTED.to_owned()
    } else {
        "none".to_owned()
    }
}

fn redacted_presence_json_value(present: bool) -> Value {
    if present {
        Value::String(REDACTED.to_owned())
    } else {
        Value::Null
    }
}

fn redacted_text_json_or_null(value: &str) -> Value {
    if value.trim().is_empty() {
        Value::Null
    } else {
        Value::String(REDACTED.to_owned())
    }
}

fn optional_unix_ms_text(value: i64) -> String {
    empty_unix_ms(value).map(|value| value.to_string()).unwrap_or_else(|| "none".to_owned())
}

fn empty_unix_ms(value: i64) -> Option<i64> {
    if value > 0 {
        Some(value)
    } else {
        None
    }
}

fn build_cleanup_session_request(
    session_id: Option<String>,
    session_key: Option<String>,
) -> Result<gateway_v1::CleanupSessionRequest> {
    if session_id.is_none() && session_key.is_none() {
        anyhow::bail!("session_id or session_key is required");
    }
    Ok(gateway_v1::CleanupSessionRequest {
        v: CANONICAL_PROTOCOL_MAJOR,
        session_id: session_id
            .map(|value| resolve_or_generate_canonical_id(Some(value)))
            .transpose()?
            .map(|ulid| common_v1::CanonicalId { ulid }),
        session_key: session_key.unwrap_or_default(),
    })
}

#[cfg(test)]
mod tests {
    use super::{build_cleanup_session_request, build_resolve_session_request};

    #[test]
    fn resolve_session_request_requires_identifier() {
        let error = build_resolve_session_request(None, None, None, false, false)
            .expect_err("resolve session should require session_id or session_key");
        assert!(
            error.to_string().contains("session_id or session_key is required"),
            "error should explain missing identity: {error}"
        );
    }

    #[test]
    fn resolve_session_request_accepts_session_key_only() {
        let request = build_resolve_session_request(
            None,
            Some("ops:triage".to_owned()),
            Some("Ops Triage".to_owned()),
            true,
            false,
        )
        .expect("resolve request should build");
        assert!(request.session_id.is_none(), "session_id should stay empty");
        assert_eq!(request.session_key, "ops:triage");
        assert_eq!(request.session_label, "Ops Triage");
        assert!(request.require_existing);
        assert!(!request.reset_session);
    }

    #[test]
    fn cleanup_session_request_requires_identifier() {
        let error = build_cleanup_session_request(None, None)
            .expect_err("cleanup session should require session_id or session_key");
        assert!(
            error.to_string().contains("session_id or session_key is required"),
            "error should explain missing identity: {error}"
        );
    }
}
