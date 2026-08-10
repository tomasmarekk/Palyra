//! Session lifecycle commands: list/resolve/rename/reset/cleanup, queue control,
//! retry/branch, transcript search/export, compaction, checkpoints, and background tasks.
//!
//! Mixes the gateway gRPC operator runtime (session resolution, runs) with the admin
//! console HTTP API (queue, compaction, checkpoints, background tasks). Text output
//! preserves command identifiers needed for resume/debug while redacting free-form labels.

use crate::*;
use palyra_common::runtime_contracts::{AuxiliaryTaskKind, AuxiliaryTaskState};
use std::io::Read as _;

/// Runs a `palyra sessions` subcommand on a fresh Tokio runtime.
///
/// # Errors
/// Returns an error when the CLI root context is unavailable, the gRPC connection
/// cannot be resolved, the runtime cannot be built, or the subcommand fails.
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

/// Dispatches a `palyra sessions` subcommand over an already-resolved connection.
///
/// # Errors
/// Returns an error when a daemon request fails, a response payload is missing
/// required fields, or subcommand input validation rejects the arguments.
pub(crate) async fn run_sessions_async(
    command: SessionsCommand,
    connection: AgentConnection,
) -> Result<()> {
    let json = match &command {
        SessionsCommand::List { json, .. }
        | SessionsCommand::History { json, .. }
        | SessionsCommand::Show { json, .. }
        | SessionsCommand::Status { json, .. }
        | SessionsCommand::Resolve { json, .. }
        | SessionsCommand::Rename { json, .. }
        | SessionsCommand::Reset { json, .. }
        | SessionsCommand::Cleanup { json, .. }
        | SessionsCommand::Abort { json, .. }
        | SessionsCommand::QueuePolicy { json, .. }
        | SessionsCommand::QueuePause { json, .. }
        | SessionsCommand::QueueResume { json, .. }
        | SessionsCommand::QueueDrain { json, .. }
        | SessionsCommand::QueueCollectSummary { json, .. }
        | SessionsCommand::QueueCancel { json, .. }
        | SessionsCommand::Retry { json, .. }
        | SessionsCommand::Branch { json, .. }
        | SessionsCommand::TranscriptSearch { json, .. }
        | SessionsCommand::Export { json, .. }
        | SessionsCommand::Subagents { json, .. }
        | SessionsCommand::CompactPreview { json, .. }
        | SessionsCommand::CompactApply { json, .. }
        | SessionsCommand::CompactionShow { json, .. }
        | SessionsCommand::CheckpointCreate { json, .. }
        | SessionsCommand::CheckpointShow { json, .. }
        | SessionsCommand::CheckpointRestore { json, .. }
        | SessionsCommand::BackgroundEnqueue { json, .. }
        | SessionsCommand::BackgroundList { json, .. }
        | SessionsCommand::BackgroundShow { json, .. }
        | SessionsCommand::BackgroundPause { json, .. }
        | SessionsCommand::BackgroundResume { json, .. }
        | SessionsCommand::BackgroundRetry { json, .. }
        | SessionsCommand::BackgroundCancel { json, .. } => output::preferred_json(*json),
    };
    let runtime = client::operator::OperatorRuntime::new(connection.clone());

    match command {
        SessionsCommand::List { after, limit, include_archived, json: _, ndjson } => {
            let ndjson = output::preferred_ndjson(json, ndjson);
            let response = runtime.list_sessions(after, include_archived, limit, None).await?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "sessions": response.sessions.iter().map(session_to_json).collect::<Vec<_>>(),
                        "next_after_session_key": empty_to_json_or_null(
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
                    empty_to_none(response.next_after_session_key.as_str()),
                    include_archived
                );
                for session in &response.sessions {
                    println!(
                        "session title={} source={} preview={} key={} label={} updated_at_unix_ms={} last_run_state={} last_run_id={} archived_at_unix_ms={}",
                        session_title_for_output(session),
                        empty_to_none(session.title_source.as_str()),
                        empty_to_none(session.preview.as_str()),
                        empty_to_none(session.session_key.as_str()),
                        redacted_text_or_none(!session.session_label.trim().is_empty()),
                        session.updated_at_unix_ms,
                        empty_to_none(session.last_run_state.as_str()),
                        optional_canonical_id_text(&session.last_run_id),
                        optional_unix_ms_text(session.archived_at_unix_ms)
                    );
                }
            }
        }
        SessionsCommand::History {
            query,
            limit,
            include_archived,
            resume_first,
            json: _,
            ndjson,
        } => {
            let ndjson = output::preferred_ndjson(json, ndjson);
            let context = client::control_plane::connect_admin_console(app::ConnectionOverrides {
                grpc_url: Some(connection.grpc_url.clone()),
                daemon_url: None,
                token: connection.token.clone(),
                principal: Some(connection.principal.clone()),
                device_id: Some(connection.device_id.clone()),
                channel: Some(connection.channel.clone()),
            })
            .await?;
            let limit = limit.unwrap_or(20).clamp(1, 100);
            let response = context
                .client
                .list_session_catalog(vec![
                    ("limit", Some(limit.to_string())),
                    ("sort", Some("updated_desc".to_owned())),
                    ("q", normalize_optional_text(query.clone())),
                    ("include_archived", include_archived.then(|| "true".to_owned())),
                ])
                .await?;
            if resume_first {
                let Some(first) = response.sessions.first() else {
                    anyhow::bail!("no session matched the requested history query");
                };
                let resumed = runtime
                    .resolve_session(SessionResolveInput {
                        session_id: Some(resolve_required_canonical_id(first.session_id.clone())?),
                        session_key: String::new(),
                        session_label: String::new(),
                        require_existing: true,
                        reset_session: false,
                    })
                    .await?;
                let session =
                    resumed.session.context("ResolveSession returned empty session payload")?;
                if json {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&json!({
                            "history_query": query,
                            "matched_session": first,
                            "session": session_to_json(&session),
                        }))?
                    );
                } else {
                    println!(
                        "sessions.history.resume title={} preview={} archived={} session_id={} session_key={}",
                        first.title,
                        first.preview.as_deref().unwrap_or("none"),
                        first.archived,
                        REDACTED,
                        empty_to_none(first.session_key.as_str())
                    );
                }
            } else if json {
                println!("{}", serde_json::to_string_pretty(&response)?);
            } else if ndjson {
                for session in &response.sessions {
                    println!(
                        "{}",
                        serde_json::to_string(&json!({
                            "type": "session_history",
                            "session": session,
                        }))?
                    );
                }
            } else {
                println!(
                    "sessions.history count={} include_archived={} query={}",
                    response.sessions.len(),
                    include_archived,
                    normalize_optional_text(query).unwrap_or_else(|| "none".to_owned())
                );
                for session in &response.sessions {
                    println!(
                        "session title={} source={} archived={} pending_approvals={} preview={}",
                        session.title,
                        session.title_source,
                        session.archived,
                        session.pending_approvals,
                        session.preview.as_deref().unwrap_or("none")
                    );
                }
            }
        }
        SessionsCommand::Show { session_id, session_key, json: _ } => {
            let session = load_session_summary_for_show(&runtime, session_id, session_key).await?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "session": session_to_json(&session),
                        "created": false,
                        "reset_applied": false,
                    }))?
                );
            } else {
                println!(
                    "sessions.show title={} source={} preview={} key={} label={} created_at_unix_ms={} updated_at_unix_ms={} last_run_state={} last_run_id={} archived_at_unix_ms={}",
                    session_title_for_output(&session),
                    empty_to_none(session.title_source.as_str()),
                    empty_to_none(session.preview.as_str()),
                    empty_to_none(session.session_key.as_str()),
                    redacted_text_or_none(!session.session_label.trim().is_empty()),
                    session.created_at_unix_ms,
                    session.updated_at_unix_ms,
                    empty_to_none(session.last_run_state.as_str()),
                    optional_canonical_id_text(&session.last_run_id),
                    optional_unix_ms_text(session.archived_at_unix_ms)
                );
            }
        }
        SessionsCommand::Status { session_id, session_key, json: _ } => {
            let session = load_session_summary_for_show(&runtime, session_id, session_key).await?;
            let session_id = session
                .session_id
                .as_ref()
                .map(|id| id.ulid.trim())
                .filter(|ulid| !ulid.is_empty())
                .map(ToOwned::to_owned)
                .context("session status resolved a session without a session_id")?;
            let context = connect_sessions_admin_console(&connection).await?;
            let payload = context
                .client
                .get_json_value(format!(
                    "console/v1/sessions/{}/snapshot",
                    percent_encode_component(session_id.as_str())
                ))
                .await?;
            print_session_status_payload(&payload, json)?;
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
                    "sessions.resolve title={} source={} preview={} key={} label={} created={} reset_applied={} archived_at_unix_ms={}",
                    session_title_for_output(&session),
                    empty_to_none(session.title_source.as_str()),
                    empty_to_none(session.preview.as_str()),
                    empty_to_none(session.session_key.as_str()),
                    redacted_text_or_none(!session.session_label.trim().is_empty()),
                    response.created,
                    response.reset_applied,
                    optional_unix_ms_text(session.archived_at_unix_ms)
                );
            }
        }
        SessionsCommand::Rename { session_id, session_key, session_label, json: _ } => {
            let response = runtime
                .resolve_session(build_resolve_session_request(
                    session_id,
                    session_key,
                    Some(session_label),
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
                    "sessions.rename label={} updated_at_unix_ms={} archived_at_unix_ms={}",
                    redacted_text_or_none(!session.session_label.trim().is_empty()),
                    session.updated_at_unix_ms,
                    optional_unix_ms_text(session.archived_at_unix_ms)
                );
            }
        }
        SessionsCommand::Reset { session_id, json: _ } => {
            let response = runtime
                .resolve_session(SessionResolveInput {
                    session_id: Some(resolve_required_canonical_id(session_id)?),
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
                    "sessions.reset reset_applied={} updated_at_unix_ms={} archived_at_unix_ms={}",
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
                    .resolve_session(SessionResolveInput {
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
                        "sessions.cleanup.dry_run key={} archived_at_unix_ms={} would_archive={}",
                        empty_to_none(session.session_key.as_str()),
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
                        "sessions.cleanup cleaned={} newly_archived={} previous_key={} archived_at_unix_ms={} run_count={}",
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
                        "cleanup_warning": redacted_cleanup_warning_json(
                            response.cleanup_warning.as_str()
                        ),
                    }))?
                );
            } else {
                if response.cleanup_warning.trim().is_empty() {
                    println!(
                        "sessions.abort run_id={} cancel_requested={} reason={}",
                        redacted_presence_for_output(response.run_id.is_some()),
                        response.cancel_requested,
                        redacted_text_or_none(!response.reason.trim().is_empty())
                    );
                } else {
                    println!(
                        "sessions.abort run_id={} cancel_requested={} reason={} cleanup_warning={}",
                        redacted_presence_for_output(response.run_id.is_some()),
                        response.cancel_requested,
                        redacted_text_or_none(!response.reason.trim().is_empty()),
                        redacted_cleanup_warning_text(response.cleanup_warning.as_str())
                    );
                }
            }
        }
        SessionsCommand::QueuePolicy { session_id, session_key, json: _ } => {
            let session_id =
                resolve_session_selector_to_id(&runtime, session_id, session_key).await?;
            let context = connect_sessions_admin_console(&connection).await?;
            let payload = context
                .client
                .get_json_value(format!(
                    "console/v1/chat/sessions/{}/queue/policy",
                    percent_encode_component(session_id.as_str())
                ))
                .await?;
            print_session_queue_payload("policy", &payload, json)?;
        }
        SessionsCommand::QueuePause { session_id, reason, json: _ } => {
            handle_session_queue_action(&connection, session_id, "pause", reason, None, json)
                .await?;
        }
        SessionsCommand::QueueResume { session_id, json: _ } => {
            handle_session_queue_action(&connection, session_id, "resume", None, None, json)
                .await?;
        }
        SessionsCommand::QueueDrain { session_id, reason, json: _ } => {
            handle_session_queue_action(&connection, session_id, "drain", reason, None, json)
                .await?;
        }
        SessionsCommand::QueueCollectSummary { session_id, reason, json: _ } => {
            handle_session_queue_action(
                &connection,
                session_id,
                "collect-summary",
                reason,
                None,
                json,
            )
            .await?;
        }
        SessionsCommand::QueueCancel { session_id, queued_input_id, reason, json: _ } => {
            handle_session_queue_action(
                &connection,
                session_id,
                "cancel",
                reason,
                Some(queued_input_id),
                json,
            )
            .await?;
        }
        SessionsCommand::Retry { session_id, allow_sensitive_tools, approval_mode, json: _ } => {
            let context = connect_sessions_admin_console(&connection).await?;
            let payload = context
                .client
                .post_json_value(
                    format!(
                        "console/v1/chat/sessions/{}/retry",
                        percent_encode_component(session_id.as_str())
                    ),
                    &json!({}),
                )
                .await?;
            let request = build_session_retry_agent_run_input(
                session_id,
                &payload,
                allow_sensitive_tools,
                approval_mode,
            )?;
            let mut client = client::runtime::GatewayRuntimeClient::connect(connection).await?;
            let outcome = stream_agent_events_async(&mut client, request, |event| {
                if json {
                    emit_acp_event_ndjson(event)
                } else {
                    emit_agent_event_text(event)
                }
            })
            .await?;
            outcome.ensure_success()?;
        }
        SessionsCommand::Branch { session_id, session_label, json: _ } => {
            let context = connect_sessions_admin_console(&connection).await?;
            let payload = context
                .client
                .post_json_value(
                    format!(
                        "console/v1/chat/sessions/{}/branch",
                        percent_encode_component(session_id.as_str())
                    ),
                    &json!({
                        "session_label": session_label,
                    }),
                )
                .await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&payload)?);
            } else {
                let session = payload
                    .pointer("/session")
                    .context("branch response is missing session payload")?;
                println!(
                    "sessions.branch session_id={} title={} branch_state={} parent_session_id={} source_run_id={}",
                    redacted_optional_identifier_for_output(
                        session.pointer("/session_id").and_then(Value::as_str)
                    ),
                    json_optional_string_in(session, "/title").unwrap_or_else(|| "none".to_owned()),
                    json_optional_string_in(session, "/branch_state")
                        .unwrap_or_else(|| "none".to_owned()),
                    redacted_optional_identifier_for_output(
                        session.pointer("/parent_session_id").and_then(Value::as_str)
                    ),
                    redacted_optional_identifier_for_output(
                        payload.pointer("/source_run_id").and_then(Value::as_str)
                    )
                );
            }
        }
        SessionsCommand::TranscriptSearch { session_id, query, json: _ } => {
            let context = connect_sessions_admin_console(&connection).await?;
            let payload = context
                .client
                .get_json_value(format!(
                    "console/v1/chat/sessions/{}/transcript/search?q={}",
                    percent_encode_component(session_id.as_str()),
                    percent_encode_component(query.as_str())
                ))
                .await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&payload)?);
            } else {
                let matches = payload
                    .pointer("/matches")
                    .and_then(Value::as_array)
                    .context("transcript search response is missing matches array")?;
                println!("sessions.transcript.search count={} query={}", matches.len(), query);
                for entry in matches {
                    println!(
                        "match seq={} event_type={} origin_kind={} run_id={} snippet={}",
                        entry.pointer("/seq").and_then(Value::as_i64).unwrap_or_default(),
                        json_optional_string_in(entry, "/event_type")
                            .unwrap_or_else(|| "unknown".to_owned()),
                        json_optional_string_in(entry, "/origin_kind")
                            .unwrap_or_else(|| "unknown".to_owned()),
                        redacted_optional_identifier_for_output(
                            entry.pointer("/run_id").and_then(Value::as_str)
                        ),
                        json_optional_string_in(entry, "/snippet")
                            .unwrap_or_else(|| "none".to_owned())
                    );
                }
            }
        }
        SessionsCommand::Export { session_id, format, json: _ } => {
            let context = connect_sessions_admin_console(&connection).await?;
            let normalized_format = match format.trim().to_ascii_lowercase().as_str() {
                "json" => "json",
                "markdown" | "md" => "markdown",
                other => {
                    anyhow::bail!("unsupported export format '{other}'; expected json or markdown")
                }
            };
            let payload = context
                .client
                .get_json_value(format!(
                    "console/v1/chat/sessions/{}/export?format={}",
                    percent_encode_component(session_id.as_str()),
                    normalized_format
                ))
                .await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&payload)?);
            } else if normalized_format == "markdown" {
                println!(
                    "{}",
                    json_required_string(&payload, "/content")
                        .context("markdown export content is missing")?
                );
            } else {
                let content =
                    payload.pointer("/content").context("json export content is missing")?;
                println!("{}", serde_json::to_string_pretty(content)?);
            }
        }
        SessionsCommand::Subagents { session_id, json: _ } => {
            let context = connect_sessions_admin_console(&connection).await?;
            let payload = context
                .client
                .get_json_value(format!(
                    "console/v1/sessions/{}/snapshot",
                    percent_encode_component(session_id.as_str())
                ))
                .await?;
            print_session_subagents_payload(&payload, json)?;
        }
        SessionsCommand::CompactPreview {
            session_id,
            session_key,
            trigger_reason,
            trigger_policy,
            operator_instruction,
            json: _,
        } => {
            let session_id =
                resolve_session_selector_to_id(&runtime, session_id, session_key).await?;
            let context = connect_sessions_admin_console(&connection).await?;
            let payload = context
                .client
                .post_json_value(
                    format!(
                        "console/v1/chat/sessions/{}/compactions/preview",
                        percent_encode_component(session_id.as_str())
                    ),
                    &json!({
                        "trigger_reason": trigger_reason,
                        "trigger_policy": trigger_policy,
                        "operator_instruction": operator_instruction,
                    }),
                )
                .await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&payload)?);
            } else {
                let preview =
                    payload.pointer("/preview").context("compaction preview is missing")?;
                let review_candidate_count = preview
                    .pointer("/summary/planner/review_candidate_count")
                    .and_then(Value::as_u64)
                    .unwrap_or_default();
                let write_count = preview
                    .pointer("/summary/writes")
                    .and_then(Value::as_array)
                    .map(|writes| writes.len())
                    .unwrap_or_default();
                let blocked_reason = json_optional_string_in(preview, "/summary/blocked_reason")
                    .or_else(|| json_optional_string_in(preview, "/blocked_reason"))
                    .unwrap_or_else(|| "none".to_owned());
                let checkpoint_name =
                    json_optional_string_in(preview, "/summary/checkpoint_preview/name")
                        .unwrap_or_else(|| "none".to_owned());
                println!(
                    "sessions.compact.preview eligible={} source_events={} protected={} condensed={} token_delta={} writes={} review_candidates={} blocked_reason={} checkpoint={} preview={}",
                    preview.pointer("/eligible").and_then(Value::as_bool).unwrap_or(false),
                    preview.pointer("/source_event_count").and_then(Value::as_u64).unwrap_or_default(),
                    preview.pointer("/protected_event_count").and_then(Value::as_u64).unwrap_or_default(),
                    preview.pointer("/condensed_event_count").and_then(Value::as_u64).unwrap_or_default(),
                    preview.pointer("/token_delta").and_then(Value::as_u64).unwrap_or_default(),
                    write_count,
                    review_candidate_count,
                    blocked_reason,
                    checkpoint_name,
                    json_optional_string_in(preview, "/summary_preview").unwrap_or_else(|| "none".to_owned())
                );
            }
        }
        SessionsCommand::CompactApply {
            session_id,
            trigger_reason,
            trigger_policy,
            operator_instruction,
            accept_candidate_ids,
            reject_candidate_ids,
            json: _,
        } => {
            let context = connect_sessions_admin_console(&connection).await?;
            let payload = context
                .client
                .post_json_value(
                    format!(
                        "console/v1/chat/sessions/{}/compactions",
                        percent_encode_component(session_id.as_str())
                    ),
                    &json!({
                        "trigger_reason": trigger_reason,
                        "trigger_policy": trigger_policy,
                        "operator_instruction": operator_instruction,
                        "accept_candidate_ids": accept_candidate_ids,
                        "reject_candidate_ids": reject_candidate_ids,
                    }),
                )
                .await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&payload)?);
            } else {
                let artifact =
                    payload.pointer("/artifact").context("compaction artifact is missing")?;
                let checkpoint =
                    payload.pointer("/checkpoint").context("checkpoint payload is missing")?;
                let summary =
                    parse_json_string(artifact.pointer("/summary_json").and_then(Value::as_str));
                let lifecycle_state = summary
                    .as_ref()
                    .and_then(|value| json_optional_string_in(value, "/lifecycle_state"))
                    .unwrap_or_else(|| "stored".to_owned());
                let write_count = summary
                    .as_ref()
                    .and_then(|value| value.pointer("/writes").and_then(Value::as_array))
                    .map(|writes| writes.len())
                    .unwrap_or_default();
                let review_candidate_count = summary
                    .as_ref()
                    .and_then(|value| {
                        value.pointer("/planner/review_candidate_count").and_then(Value::as_u64)
                    })
                    .unwrap_or_default();
                println!(
                    "sessions.compact.apply artifact_id={} checkpoint_id={} mode={} strategy={} lifecycle={} writes={} review_candidates={} input_tokens={} output_tokens={}",
                    redacted_optional_identifier_for_output(artifact.pointer("/artifact_id").and_then(Value::as_str)),
                    redacted_optional_identifier_for_output(
                        checkpoint.pointer("/checkpoint_id").and_then(Value::as_str)
                    ),
                    json_optional_string_in(artifact, "/mode").unwrap_or_else(|| "unknown".to_owned()),
                    json_optional_string_in(artifact, "/strategy").unwrap_or_else(|| "unknown".to_owned()),
                    lifecycle_state,
                    write_count,
                    review_candidate_count,
                    artifact.pointer("/estimated_input_tokens").and_then(Value::as_u64).unwrap_or_default(),
                    artifact.pointer("/estimated_output_tokens").and_then(Value::as_u64).unwrap_or_default(),
                );
            }
        }
        SessionsCommand::CompactionShow { artifact_id, json: _ } => {
            let context = connect_sessions_admin_console(&connection).await?;
            let payload = context
                .client
                .get_json_value(format!(
                    "console/v1/chat/compactions/{}",
                    percent_encode_component(artifact_id.as_str())
                ))
                .await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&payload)?);
            } else {
                let artifact =
                    payload.pointer("/artifact").context("compaction artifact is missing")?;
                let related_checkpoint_count = payload
                    .pointer("/related_checkpoints")
                    .and_then(Value::as_array)
                    .map(|items| items.len())
                    .unwrap_or_default();
                let summary =
                    parse_json_string(artifact.pointer("/summary_json").and_then(Value::as_str));
                let lifecycle_state = summary
                    .as_ref()
                    .and_then(|value| json_optional_string_in(value, "/lifecycle_state"))
                    .unwrap_or_else(|| "stored".to_owned());
                let write_count = summary
                    .as_ref()
                    .and_then(|value| value.pointer("/writes").and_then(Value::as_array))
                    .map(|writes| writes.len())
                    .unwrap_or_default();
                let review_candidate_count = summary
                    .as_ref()
                    .and_then(|value| {
                        value.pointer("/planner/review_candidate_count").and_then(Value::as_u64)
                    })
                    .unwrap_or_default();
                println!(
                    "sessions.compaction.show artifact_id={} mode={} trigger_reason={} lifecycle={} writes={} review_candidates={} related_checkpoints={} preview={}",
                    redacted_optional_identifier_for_output(
                        artifact.pointer("/artifact_id").and_then(Value::as_str)
                    ),
                    json_optional_string_in(artifact, "/mode")
                        .unwrap_or_else(|| "unknown".to_owned()),
                    json_optional_string_in(artifact, "/trigger_reason")
                        .unwrap_or_else(|| "unknown".to_owned()),
                    lifecycle_state,
                    write_count,
                    review_candidate_count,
                    related_checkpoint_count,
                    json_optional_string_in(artifact, "/summary_preview")
                        .unwrap_or_else(|| "none".to_owned())
                );
            }
        }
        SessionsCommand::CheckpointCreate { session_id, name, note, tags, json: _ } => {
            let context = connect_sessions_admin_console(&connection).await?;
            let payload = context
                .client
                .post_json_value(
                    format!(
                        "console/v1/chat/sessions/{}/checkpoints",
                        percent_encode_component(session_id.as_str())
                    ),
                    &json!({
                        "name": name,
                        "note": note,
                        "tags": tags,
                    }),
                )
                .await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&payload)?);
            } else {
                let checkpoint =
                    payload.pointer("/checkpoint").context("checkpoint payload is missing")?;
                println!(
                    "sessions.checkpoint.create checkpoint_id={} name={} restore_count={} branch_state={}",
                    redacted_optional_identifier_for_output(checkpoint.pointer("/checkpoint_id").and_then(Value::as_str)),
                    json_optional_string_in(checkpoint, "/name").unwrap_or_else(|| "unknown".to_owned()),
                    checkpoint.pointer("/restore_count").and_then(Value::as_u64).unwrap_or_default(),
                    json_optional_string_in(checkpoint, "/branch_state").unwrap_or_else(|| "unknown".to_owned())
                );
            }
        }
        SessionsCommand::CheckpointShow { checkpoint_id, json: _ } => {
            let context = connect_sessions_admin_console(&connection).await?;
            let payload = context
                .client
                .get_json_value(format!(
                    "console/v1/chat/checkpoints/{}",
                    percent_encode_component(checkpoint_id.as_str())
                ))
                .await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&payload)?);
            } else {
                let checkpoint =
                    payload.pointer("/checkpoint").context("checkpoint payload is missing")?;
                println!(
                    "sessions.checkpoint.show checkpoint_id={} name={} restore_count={} last_restored_at_unix_ms={}",
                    redacted_optional_identifier_for_output(checkpoint.pointer("/checkpoint_id").and_then(Value::as_str)),
                    json_optional_string_in(checkpoint, "/name").unwrap_or_else(|| "unknown".to_owned()),
                    checkpoint.pointer("/restore_count").and_then(Value::as_u64).unwrap_or_default(),
                    checkpoint.pointer("/last_restored_at_unix_ms").and_then(Value::as_i64).map(|value| value.to_string()).unwrap_or_else(|| "none".to_owned())
                );
            }
        }
        SessionsCommand::CheckpointRestore { checkpoint_id, session_label, json: _ } => {
            let context = connect_sessions_admin_console(&connection).await?;
            let payload = context
                .client
                .post_json_value(
                    format!(
                        "console/v1/chat/checkpoints/{}/restore",
                        percent_encode_component(checkpoint_id.as_str())
                    ),
                    &json!({
                        "session_label": session_label,
                    }),
                )
                .await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&payload)?);
            } else {
                let session =
                    payload.pointer("/session").context("restored session payload is missing")?;
                println!(
                    "sessions.checkpoint.restore session_id={} branch_state={} parent_session_id={}",
                    redacted_optional_identifier_for_output(session.pointer("/session_id").and_then(Value::as_str)),
                    json_optional_string_in(session, "/branch_state").unwrap_or_else(|| "unknown".to_owned()),
                    redacted_optional_identifier_for_output(session.pointer("/parent_session_id").and_then(Value::as_str))
                );
            }
        }
        SessionsCommand::BackgroundEnqueue {
            session_id,
            text,
            text_stdin,
            priority,
            max_attempts,
            budget_tokens,
            not_before_unix_ms,
            expires_at_unix_ms,
            json,
        } => {
            let text = resolve_background_task_text(text, text_stdin)?;
            let context = connect_sessions_admin_console(&connection).await?;
            let payload = context
                .client
                .post_json_value(
                    format!(
                        "console/v1/chat/sessions/{}/background-tasks",
                        percent_encode_component(session_id.as_str())
                    ),
                    &json!({
                        "text": text,
                        "priority": priority,
                        "max_attempts": max_attempts,
                        "budget_tokens": budget_tokens,
                        "not_before_unix_ms": not_before_unix_ms,
                        "expires_at_unix_ms": expires_at_unix_ms,
                    }),
                )
                .await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&payload)?);
            } else {
                let task =
                    payload.pointer("/task").context("background task payload is missing")?;
                println!(
                    "sessions.background.enqueue task_id={} kind={} state={} priority={} max_attempts={} topology={} delegation={} limits={} live_steering={} diagnostic={}",
                    redacted_optional_identifier_for_output(
                        task.pointer("/task_id").and_then(Value::as_str)
                    ),
                    render_auxiliary_task_kind(task.pointer("/task_kind").and_then(Value::as_str)),
                    render_auxiliary_task_state(task.pointer("/state").and_then(Value::as_str)),
                    task.pointer("/priority").and_then(Value::as_i64).unwrap_or_default(),
                    task.pointer("/max_attempts").and_then(Value::as_u64).unwrap_or_default(),
                    render_background_task_topology(task),
                    render_background_task_delegation(task),
                    render_background_task_limits(task),
                    render_background_enqueue_live_steering(payload.pointer("/live_steering")),
                    render_background_task_diagnostic(task, None)
                );
            }
        }
        SessionsCommand::BackgroundList { session_id, include_completed, limit, json: _ } => {
            let context = connect_sessions_admin_console(&connection).await?;
            let mut path =
                format!("console/v1/chat/background-tasks?include_completed={include_completed}");
            if let Some(session_id) = session_id {
                path.push_str("&session_id=");
                path.push_str(percent_encode_component(session_id.as_str()).as_str());
            }
            if let Some(limit) = limit {
                path.push_str("&limit=");
                path.push_str(limit.to_string().as_str());
            }
            let payload = context.client.get_json_value(path).await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&payload)?);
            } else {
                let tasks = payload
                    .pointer("/tasks")
                    .and_then(Value::as_array)
                    .context("background tasks array is missing")?;
                println!(
                    "sessions.background.list count={} include_completed={}",
                    tasks.len(),
                    include_completed
                );
                for task in tasks {
                    println!(
                        "task task_id={} kind={} state={} priority={} topology={} delegation={} limits={} diagnostic={} created_at_unix_ms={}",
                        redacted_optional_identifier_for_output(
                            task.pointer("/task_id").and_then(Value::as_str)
                        ),
                        render_auxiliary_task_kind(task.pointer("/task_kind").and_then(Value::as_str)),
                        render_auxiliary_task_state(task.pointer("/state").and_then(Value::as_str)),
                        task.pointer("/priority").and_then(Value::as_i64).unwrap_or_default(),
                        render_background_task_topology(task),
                        render_background_task_delegation(task),
                        render_background_task_limits(task),
                        render_background_task_diagnostic(task, None),
                        task.pointer("/created_at_unix_ms")
                            .and_then(Value::as_i64)
                            .unwrap_or_default()
                    );
                }
            }
        }
        SessionsCommand::BackgroundShow { task_id, json: _ } => {
            let context = connect_sessions_admin_console(&connection).await?;
            let payload = context
                .client
                .get_json_value(format!(
                    "console/v1/chat/background-tasks/{}",
                    percent_encode_component(task_id.as_str())
                ))
                .await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&payload)?);
            } else {
                let task =
                    payload.pointer("/task").context("background task payload is missing")?;
                let run = payload.pointer("/run");
                println!(
                    "sessions.background.show task_id={} kind={} state={} attempt_count={} max_attempts={} topology={} delegation={} limits={} diagnostic={}",
                    redacted_optional_identifier_for_output(task.pointer("/task_id").and_then(Value::as_str)),
                    render_auxiliary_task_kind(task.pointer("/task_kind").and_then(Value::as_str)),
                    render_auxiliary_task_state(task.pointer("/state").and_then(Value::as_str)),
                    task.pointer("/attempt_count").and_then(Value::as_u64).unwrap_or_default(),
                    task.pointer("/max_attempts").and_then(Value::as_u64).unwrap_or_default(),
                    render_background_task_topology(task),
                    render_background_task_delegation(task),
                    render_background_task_limits(task),
                    render_background_task_diagnostic(task, run)
                );
            }
        }
        SessionsCommand::BackgroundPause { task_id, json: _ } => {
            handle_background_task_action(&connection, task_id, "pause", json).await?;
        }
        SessionsCommand::BackgroundResume { task_id, json: _ } => {
            handle_background_task_action(&connection, task_id, "resume", json).await?;
        }
        SessionsCommand::BackgroundRetry { task_id, json: _ } => {
            handle_background_task_action(&connection, task_id, "retry", json).await?;
        }
        SessionsCommand::BackgroundCancel { task_id, json: _ } => {
            handle_background_task_action(&connection, task_id, "cancel", json).await?;
        }
    }

    std::io::stdout().flush().context("stdout flush failed")
}

/// Reads non-empty background-task text from exactly one of `--text` or `--text-stdin`.
fn resolve_background_task_text(text: Option<String>, text_stdin: bool) -> Result<String> {
    if text_stdin {
        if text.is_some() {
            anyhow::bail!("cannot use --text together with --text-stdin");
        }
        let mut input = Vec::new();
        std::io::stdin()
            .lock()
            .read_to_end(&mut input)
            .context("failed to read background task text from stdin")?;
        let text = normalize_prompt_stdin_bytes(input.as_slice())
            .context("failed to decode background task text from stdin")?;
        if text.trim().is_empty() {
            anyhow::bail!(
                "background task text from stdin is empty; pipe text into stdin or use --text"
            );
        }
        return Ok(text);
    }

    let text = text.context("missing background task text: use --text or --text-stdin")?;
    let text = normalize_single_line_cli_text_arg(text, "--text", "--text-stdin")?;
    if text.trim().is_empty() {
        anyhow::bail!("background task text cannot be empty");
    }
    Ok(text)
}

/// Turns the console retry payload (original prompt, origin metadata, parameter
/// delta) into an agent run input replayed against the same session.
fn build_session_retry_agent_run_input(
    session_id: String,
    payload: &Value,
    allow_sensitive_tools: bool,
    approval_mode: AgentApprovalModeArg,
) -> Result<AgentRunInput> {
    crate::commands::agent::ensure_agent_run_approval_flags(
        allow_sensitive_tools,
        approval_mode,
        false,
    )?;
    let prompt = json_required_string(payload, "/text")?;
    let origin_kind = json_optional_string(payload, "/origin_kind");
    let origin_run_id = json_optional_string(payload, "/origin_run_id");
    let parameter_delta_json = payload
        .pointer("/parameter_delta")
        .filter(|value| !value.is_null())
        .map(serde_json::to_string)
        .transpose()?;
    build_agent_run_input(AgentRunInputArgs {
        session_id: Some(resolve_required_canonical_id(session_id)?),
        session_key: None,
        session_label: None,
        require_existing: true,
        reset_session: false,
        run_id: None,
        prompt,
        allow_sensitive_tools,
        interrupt_active_run: false,
        approval_mode: approval_mode.into(),
        origin_kind,
        origin_run_id,
        parameter_delta_json,
    })
}

fn build_resolve_session_request(
    session_id: Option<String>,
    session_key: Option<String>,
    session_label: Option<String>,
    require_existing: bool,
    reset_session: bool,
) -> Result<SessionResolveInput> {
    if session_id.is_none() && session_key.is_none() {
        anyhow::bail!("session_id or session_key is required");
    }
    Ok(SessionResolveInput {
        session_id: resolve_optional_canonical_id(session_id)?,
        session_key: session_key.unwrap_or_default(),
        session_label: session_label.unwrap_or_default(),
        require_existing,
        reset_session,
    })
}

/// Finds a session summary by id and/or key by paging through `list_sessions`;
/// the gateway exposes no direct summary lookup. When both selectors are given,
/// they must resolve to the same session.
async fn load_session_summary_for_show(
    runtime: &client::operator::OperatorRuntime,
    session_id: Option<String>,
    session_key: Option<String>,
) -> Result<gateway_v1::SessionSummary> {
    let requested_session_id = resolve_optional_canonical_id(session_id)?.map(|id| id.ulid);
    let requested_session_key = normalize_optional_text(session_key);
    if requested_session_id.is_none() && requested_session_key.is_none() {
        anyhow::bail!("session_id or session_key is required");
    }

    let mut after_session_key = None;
    loop {
        let response =
            runtime.list_sessions(after_session_key.clone(), true, Some(100), None).await?;
        for session in response.sessions {
            let id_matches = requested_session_id
                .as_deref()
                .is_some_and(|expected| session_id_matches(&session, expected));
            let key_matches = requested_session_key
                .as_deref()
                .is_some_and(|expected| session.session_key == expected);
            let selector_matches = requested_session_id.as_ref().is_none_or(|_| id_matches)
                && requested_session_key.as_ref().is_none_or(|_| key_matches);
            if selector_matches {
                return Ok(session);
            }
            // Reaching this point with a partial match means both selectors were
            // provided but point at different sessions.
            if id_matches || key_matches {
                anyhow::bail!(
                    "invalid session selector: session_id and session_key resolve to different sessions"
                );
            }
        }

        let next_after_session_key = normalize_optional_text(Some(response.next_after_session_key));
        // A repeated cursor would loop forever; treat it as the end of the listing.
        if next_after_session_key.is_none() || next_after_session_key == after_session_key {
            break;
        }
        after_session_key = next_after_session_key;
    }

    anyhow::bail!(
        "session not found: {}",
        requested_session_id
            .as_deref()
            .or(requested_session_key.as_deref())
            .unwrap_or("<unspecified>")
    )
}

fn session_id_matches(session: &gateway_v1::SessionSummary, expected: &str) -> bool {
    session.session_id.as_ref().is_some_and(|id| id.ulid == expected)
}

fn session_to_json(session: &gateway_v1::SessionSummary) -> Value {
    json!({
        "session_id": optional_canonical_id_json_value(&session.session_id),
        "session_key": empty_to_json_or_null(session.session_key.as_str()),
        "session_label": empty_to_json_or_null(session.session_label.as_str()),
        "title": empty_to_json_or_null(session.title.as_str()),
        "title_source": empty_to_json_or_null(session.title_source.as_str()),
        "title_generator_version": empty_to_json_or_null(session.title_generator_version.as_str()),
        "preview": empty_to_json_or_null(session.preview.as_str()),
        "preview_state": empty_to_json_or_null(session.preview_state.as_str()),
        "last_intent": empty_to_json_or_null(session.last_intent.as_str()),
        "last_summary": empty_to_json_or_null(session.last_summary.as_str()),
        "match_snippet": empty_to_json_or_null(session.match_snippet.as_str()),
        "branch_state": empty_to_json_or_null(session.branch_state.as_str()),
        "parent_session_id": optional_canonical_id_json_value(&session.parent_session_id),
        "last_run_state": empty_to_json_or_null(session.last_run_state.as_str()),
        "created_at_unix_ms": session.created_at_unix_ms,
        "updated_at_unix_ms": session.updated_at_unix_ms,
        "last_run_id": optional_canonical_id_json_value(&session.last_run_id),
        "archived_at_unix_ms": empty_unix_ms(session.archived_at_unix_ms),
    })
}

fn optional_canonical_id_json_value(value: &Option<common_v1::CanonicalId>) -> Value {
    value
        .as_ref()
        .map(|id| id.ulid.trim())
        .filter(|ulid| !ulid.is_empty())
        .map(|ulid| Value::String(ulid.to_owned()))
        .unwrap_or(Value::Null)
}

fn optional_canonical_id_text(value: &Option<common_v1::CanonicalId>) -> String {
    value
        .as_ref()
        .map(|id| id.ulid.trim())
        .filter(|ulid| !ulid.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| "none".to_owned())
}

// Free-form labels and reasons may carry user-identifying content, so text
// renderers print only `<redacted>`/`none` presence markers for those fields.
fn redacted_text_or_none(present: bool) -> String {
    redacted_presence_for_output(present)
}

fn normalize_optional_text(value: Option<String>) -> Option<String> {
    value.map(|entry| entry.trim().to_owned()).filter(|entry| !entry.is_empty())
}

fn redacted_presence_for_output(present: bool) -> String {
    if present {
        REDACTED.to_owned()
    } else {
        "none".to_owned()
    }
}

fn redacted_text_json_or_null(value: &str) -> Value {
    if value.trim().is_empty() {
        Value::Null
    } else {
        Value::String(REDACTED.to_owned())
    }
}

fn redacted_cleanup_warning_json(value: &str) -> Value {
    redacted_text_json_or_null(value)
}

fn redacted_cleanup_warning_text(value: &str) -> String {
    redacted_text_or_none(!value.trim().is_empty())
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

fn empty_to_none(value: &str) -> String {
    if value.trim().is_empty() {
        "none".to_owned()
    } else {
        value.to_owned()
    }
}

fn empty_to_json_or_null(value: &str) -> Value {
    if value.trim().is_empty() {
        Value::Null
    } else {
        Value::String(value.to_owned())
    }
}

fn session_title_for_output(session: &gateway_v1::SessionSummary) -> String {
    empty_to_none(session.title.as_str())
}

fn build_cleanup_session_request(
    session_id: Option<String>,
    session_key: Option<String>,
) -> Result<SessionCleanupInput> {
    if session_id.is_none() && session_key.is_none() {
        anyhow::bail!("session_id or session_key is required");
    }
    Ok(SessionCleanupInput {
        session_id: resolve_optional_canonical_id(session_id)?,
        session_key: session_key.unwrap_or_default(),
    })
}

async fn resolve_session_selector_to_id(
    runtime: &client::operator::OperatorRuntime,
    session_id: Option<String>,
    session_key: Option<String>,
) -> Result<String> {
    if session_key.is_none() {
        let session_id = session_id.context("session_id or session_key is required")?;
        return Ok(resolve_required_canonical_id(session_id)?.ulid);
    }

    let response = runtime
        .resolve_session(build_resolve_session_request(session_id, session_key, None, true, false)?)
        .await?;
    let session = response.session.context("ResolveSession returned empty session payload")?;
    session
        .session_id
        .as_ref()
        .map(|id| id.ulid.trim())
        .filter(|ulid| !ulid.is_empty())
        .map(ToOwned::to_owned)
        .context("ResolveSession returned a session without a session_id")
}

async fn connect_sessions_admin_console(
    connection: &AgentConnection,
) -> Result<client::control_plane::AdminConsoleContext> {
    client::control_plane::connect_admin_console(app::ConnectionOverrides {
        grpc_url: Some(connection.grpc_url.clone()),
        daemon_url: None,
        token: connection.token.clone(),
        principal: Some(connection.principal.clone()),
        device_id: Some(connection.device_id.clone()),
        channel: Some(connection.channel.clone()),
    })
    .await
}

async fn handle_session_queue_action(
    connection: &AgentConnection,
    session_id: String,
    action: &str,
    reason: Option<String>,
    queued_input_id: Option<String>,
    json_output: bool,
) -> Result<()> {
    let context = connect_sessions_admin_console(connection).await?;
    let path = if action == "cancel" {
        let queued_input_id =
            queued_input_id.context("queued_input_id is required for queue cancel")?;
        format!(
            "console/v1/chat/sessions/{}/queue/items/{}/cancel",
            percent_encode_component(session_id.as_str()),
            percent_encode_component(queued_input_id.as_str())
        )
    } else {
        format!(
            "console/v1/chat/sessions/{}/queue/{}",
            percent_encode_component(session_id.as_str()),
            action
        )
    };
    let payload = context.client.post_json_value(path, &json!({ "reason": reason })).await?;
    print_session_queue_payload(action, &payload, json_output)
}

// Cap for queued-input lines in text output; `--json` returns the full queue.
const QUEUED_INPUT_TEXT_LINE_LIMIT: usize = 12;

fn print_session_queue_payload(action: &str, payload: &Value, json_output: bool) -> Result<()> {
    if json_output {
        println!("{}", serde_json::to_string_pretty(payload)?);
        return Ok(());
    }
    let queue = payload.pointer("/queue").unwrap_or(payload);
    let metrics = queue.pointer("/metrics").unwrap_or(&Value::Null);
    let control = queue.pointer("/control").unwrap_or(&Value::Null);
    let queued_inputs =
        queue.pointer("/queued_inputs").and_then(Value::as_array).cloned().unwrap_or_default();
    println!(
        "sessions.queue.{} paused={} pending_depth={} terminal_count={} total_count={} active_run_id={}",
        action,
        control.pointer("/paused").and_then(Value::as_bool).unwrap_or(false),
        metrics.pointer("/pending_depth").and_then(Value::as_u64).unwrap_or_default(),
        metrics.pointer("/terminal_count").and_then(Value::as_u64).unwrap_or_default(),
        metrics.pointer("/total_count").and_then(Value::as_u64).unwrap_or_default(),
        redacted_optional_identifier_for_output(queue.pointer("/active_run_id").and_then(Value::as_str))
    );
    for queued in queued_inputs.iter().rev().take(QUEUED_INPUT_TEXT_LINE_LIMIT) {
        println!(
            "queued_input id={} state={} mode={} lane={} run_id={} reason={}",
            redacted_optional_identifier_for_output(
                queued.pointer("/queued_input_id").and_then(Value::as_str)
            ),
            json_optional_string_in(queued, "/state").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_in(queued, "/queue_mode").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_in(queued, "/priority_lane")
                .unwrap_or_else(|| "normal".to_owned()),
            redacted_optional_identifier_for_output(
                queued.pointer("/run_id").and_then(Value::as_str)
            ),
            json_optional_string_in(queued, "/decision_reason")
                .unwrap_or_else(|| "none".to_owned())
        );
    }
    Ok(())
}

fn print_session_status_payload(payload: &Value, json_output: bool) -> Result<()> {
    if json_output {
        println!("{}", serde_json::to_string_pretty(payload)?);
        return Ok(());
    }
    let snapshot = payload.pointer("/snapshot").unwrap_or(payload);
    let lifecycle = snapshot.pointer("/lifecycle").unwrap_or(&Value::Null);
    let queue = snapshot.pointer("/queue").unwrap_or(&Value::Null);
    let approvals = snapshot.pointer("/approvals").unwrap_or(&Value::Null);
    let usage = snapshot.pointer("/usage/tokens").unwrap_or(&Value::Null);
    let safe_operations = snapshot.pointer("/safe_operations").unwrap_or(&Value::Null);
    println!(
        "sessions.status state={} queue={} pending_depth={} active_run_id={} approvals_pending={} can_start_run={} can_cancel={} can_compact={} token_total={}",
        json_optional_string_in(lifecycle, "/state").unwrap_or_else(|| "unknown".to_owned()),
        json_optional_string_in(queue, "/busy_state").unwrap_or_else(|| "unknown".to_owned()),
        queue.pointer("/pending_depth").and_then(Value::as_u64).unwrap_or_default(),
        redacted_optional_identifier_for_output(queue.pointer("/active_run_id").and_then(Value::as_str)),
        approvals.pointer("/pending").and_then(Value::as_bool).unwrap_or(false),
        safe_operations.pointer("/can_start_run").and_then(Value::as_bool).unwrap_or(false),
        safe_operations.pointer("/can_cancel").and_then(Value::as_bool).unwrap_or(false),
        safe_operations.pointer("/can_compact").and_then(Value::as_bool).unwrap_or(false),
        usage.pointer("/total").and_then(Value::as_u64).map(|value| value.to_string()).unwrap_or_else(|| "none".to_owned())
    );
    Ok(())
}

fn print_session_subagents_payload(payload: &Value, json_output: bool) -> Result<()> {
    let subagents = payload
        .pointer("/snapshot/subagents")
        .or_else(|| payload.pointer("/subagents"))
        .context("session snapshot is missing subagents")?;
    if json_output {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "subagents": subagents,
                "contract": payload.pointer("/contract").cloned().unwrap_or(Value::Null),
            }))?
        );
        return Ok(());
    }
    let records =
        subagents.pointer("/records").and_then(Value::as_array).cloned().unwrap_or_default();
    println!(
        "sessions.subagents count={} stale_links={} child_sessions={}",
        subagents
            .pointer("/subagent_count")
            .and_then(Value::as_u64)
            .unwrap_or(records.len() as u64),
        subagents.pointer("/stale_link_count").and_then(Value::as_u64).unwrap_or_default(),
        subagents.pointer("/child_count").and_then(Value::as_u64).unwrap_or_default()
    );
    for record in records {
        println!(
            "subagent task_id={} child_run_id={} status={} transcript={} link={} role={} budget_tokens={}",
            redacted_optional_identifier_for_output(record.pointer("/task_id").and_then(Value::as_str)),
            redacted_optional_identifier_for_output(record.pointer("/child_run_id").and_then(Value::as_str)),
            json_optional_string_in(&record, "/status").unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_in(&record, "/transcript_ref/status")
                .unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_in(&record, "/stale_link_repair/status")
                .unwrap_or_else(|| "unknown".to_owned()),
            json_optional_string_in(&record, "/role").unwrap_or_else(|| "unknown".to_owned()),
            record.pointer("/budget/budget_tokens").and_then(Value::as_u64).unwrap_or_default()
        );
    }
    Ok(())
}

async fn handle_background_task_action(
    connection: &AgentConnection,
    task_id: String,
    action: &str,
    json_output: bool,
) -> Result<()> {
    let context = connect_sessions_admin_console(connection).await?;
    let payload = context
        .client
        .post_json_value(
            format!(
                "console/v1/chat/background-tasks/{}/{}",
                percent_encode_component(task_id.as_str()),
                action
            ),
            &json!({}),
        )
        .await?;
    if json_output {
        println!("{}", serde_json::to_string_pretty(&payload)?);
    } else {
        let task = payload.pointer("/task").context("background task payload is missing")?;
        println!(
            "sessions.background.{} task_id={} state={}",
            action,
            redacted_optional_identifier_for_output(
                task.pointer("/task_id").and_then(Value::as_str)
            ),
            render_auxiliary_task_state(task.pointer("/state").and_then(Value::as_str))
        );
    }
    Ok(())
}

// The renderers below map daemon compatibility aliases (e.g. legacy state names)
// to canonical runtime-contract values; unknown values pass through unchanged.
fn render_auxiliary_task_kind(value: Option<&str>) -> String {
    match value {
        Some(raw) => AuxiliaryTaskKind::from_str(raw)
            .map(|kind| kind.as_str().to_owned())
            .unwrap_or_else(|| raw.to_owned()),
        None => "unknown".to_owned(),
    }
}

fn render_auxiliary_task_state(value: Option<&str>) -> String {
    match value {
        Some(raw) => AuxiliaryTaskState::from_str(raw)
            .map(|state| state.as_str().to_owned())
            .unwrap_or_else(|| raw.to_owned()),
        None => "unknown".to_owned(),
    }
}

fn render_background_task_topology(task: &Value) -> String {
    format!(
        "parent={} child={} session={}",
        redacted_optional_identifier_for_output(
            task.pointer("/parent_run_id").and_then(Value::as_str)
        ),
        redacted_optional_identifier_for_output(
            task.pointer("/target_run_id").and_then(Value::as_str)
        ),
        redacted_optional_identifier_for_output(
            task.pointer("/session_id").and_then(Value::as_str)
        )
    )
}

fn render_background_task_delegation(task: &Value) -> String {
    let Some(delegation) = task.pointer("/delegation") else {
        return "none".to_owned();
    };
    format!(
        "profile={} mode={} group={}",
        delegation.pointer("/profile_id").and_then(Value::as_str).unwrap_or("unknown"),
        delegation.pointer("/execution_mode").and_then(Value::as_str).unwrap_or("unknown"),
        delegation.pointer("/group_id").and_then(Value::as_str).unwrap_or("unknown")
    )
}

fn render_background_task_limits(task: &Value) -> String {
    let budget_tokens = task.pointer("/budget_tokens").and_then(Value::as_u64).unwrap_or_default();
    let Some(limits) = task.pointer("/delegation/runtime_limits") else {
        return format!("budget_tokens={budget_tokens}");
    };
    let budget_override = limits
        .pointer("/child_budget_override")
        .and_then(Value::as_u64)
        .map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_owned());
    format!(
        "budget_tokens={} max_concurrent_children={} max_children_per_parent={} max_parallel_groups={} child_timeout_ms={} child_budget_override={}",
        budget_tokens,
        limits.pointer("/max_concurrent_children").and_then(Value::as_u64).unwrap_or_default(),
        limits.pointer("/max_children_per_parent").and_then(Value::as_u64).unwrap_or_default(),
        limits.pointer("/max_parallel_groups").and_then(Value::as_u64).unwrap_or_default(),
        limits.pointer("/child_timeout_ms").and_then(Value::as_u64).unwrap_or_default(),
        budget_override
    )
}

fn render_background_enqueue_live_steering(value: Option<&Value>) -> String {
    let Some(value) = value else {
        return "unknown".to_owned();
    };
    let supported = value.pointer("/supported").and_then(Value::as_bool).unwrap_or(false);
    let parent_active =
        value.pointer("/parent_run_active").and_then(Value::as_bool).unwrap_or(false);
    let parent_state =
        value.pointer("/parent_run_state").and_then(Value::as_str).unwrap_or("unknown");
    if parent_active && !supported {
        return format!("unsupported_active_parent_state={parent_state}");
    }
    format!("supported={supported} parent_state={parent_state}")
}

fn render_background_task_diagnostic(task: &Value, run: Option<&Value>) -> String {
    if let Some(result) = parse_json_string(task.pointer("/result_json").and_then(Value::as_str)) {
        if result.pointer("/status").and_then(Value::as_str) == Some("waiting") {
            return format!(
                "waiting:{}",
                result.pointer("/reason").and_then(Value::as_str).unwrap_or("unknown")
            );
        }
    }
    if let Some(failure_category) = run
        .and_then(|value| value.pointer("/merge_result/failure_category"))
        .and_then(Value::as_str)
    {
        let total_tokens = run
            .and_then(|value| value.pointer("/merge_result/usage_summary/total_tokens"))
            .and_then(Value::as_u64)
            .unwrap_or_default();
        return format!("merge_failure={failure_category} merge_tokens={total_tokens}");
    }
    task.pointer("/last_error")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(|value| format!("error={value}"))
        .unwrap_or_else(|| "none".to_owned())
}

fn json_required_string(payload: &Value, pointer: &str) -> Result<String> {
    payload
        .pointer(pointer)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .with_context(|| format!("response is missing required string at {pointer}"))
}

fn json_optional_string(payload: &Value, pointer: &str) -> Option<String> {
    payload.pointer(pointer).and_then(Value::as_str).map(ToOwned::to_owned)
}

fn json_optional_string_in(payload: &Value, pointer: &str) -> Option<String> {
    json_optional_string(payload, pointer)
}

fn parse_json_string(value: Option<&str>) -> Option<Value> {
    value.and_then(|raw| serde_json::from_str::<Value>(raw).ok())
}

// Percent-encodes everything outside the RFC 3986 unreserved set so caller-supplied
// ids and queries embed safely into console URL paths.
fn percent_encode_component(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(char::from(*byte));
            }
            other => {
                encoded.push('%');
                encoded.push_str(format!("{other:02X}").as_str());
            }
        }
    }
    encoded
}

#[cfg(test)]
mod render_tests {
    use super::{render_auxiliary_task_kind, render_auxiliary_task_state};

    #[test]
    fn auxiliary_task_renderers_normalize_compat_aliases() {
        assert_eq!(render_auxiliary_task_kind(Some("reflection")), "post_run_reflection");
        assert_eq!(render_auxiliary_task_state(Some("pending")), "queued");
        assert_eq!(render_auxiliary_task_state(Some("canceled")), "cancelled");
    }

    #[test]
    fn auxiliary_task_renderers_preserve_unknown_values() {
        assert_eq!(render_auxiliary_task_kind(Some("custom_task")), "custom_task");
        assert_eq!(render_auxiliary_task_state(Some("mystery_state")), "mystery_state");
        assert_eq!(render_auxiliary_task_state(None), "unknown");
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_cleanup_session_request, build_resolve_session_request,
        build_session_retry_agent_run_input, optional_canonical_id_text,
        redacted_cleanup_warning_json, redacted_cleanup_warning_text, session_to_json,
    };
    use crate::args::AgentApprovalModeArg;
    use crate::proto::palyra::{common::v1 as common_v1, gateway::v1 as gateway_v1};
    use crate::AgentApprovalMode;

    #[test]
    fn resolve_session_request_requires_identifier() {
        let error = build_resolve_session_request(None, None, None, false, false)
            .err()
            .expect("resolve session should require session_id or session_key");
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
    fn retry_agent_run_input_restores_payload_context_with_prompt_default() {
        let payload = serde_json::json!({
            "text": "retry the failed workspace task",
            "origin_kind": "retry",
            "origin_run_id": "01ARZ3NDEKTSV4RRFFQ69G5FAW",
            "parameter_delta": {
                "cli_context": {
                    "launch_cwd": "C:/work/project",
                    "workspace_roots": ["C:/work/project"]
                }
            }
        });

        let request = build_session_retry_agent_run_input(
            "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            &payload,
            false,
            AgentApprovalModeArg::Prompt,
        )
        .expect("retry run input should build");

        assert_eq!(request.prompt, "retry the failed workspace task");
        assert_eq!(request.approval_mode, AgentApprovalMode::Prompt);
        assert!(!request.allow_sensitive_tools);
        assert_eq!(request.origin_kind.as_deref(), Some("retry"));
        assert_eq!(request.origin_run_id.as_deref(), Some("01ARZ3NDEKTSV4RRFFQ69G5FAW"));
        let parameter_delta: serde_json::Value = serde_json::from_str(
            request.parameter_delta_json.as_deref().expect("retry should carry a launch context"),
        )
        .expect("parameter delta should be valid JSON");
        assert_eq!(
            parameter_delta.pointer("/cli_context/launch_cwd").and_then(serde_json::Value::as_str),
            Some("C:/work/project")
        );
    }

    #[test]
    fn session_json_preserves_command_identifiers_and_label() {
        let session = gateway_v1::SessionSummary {
            session_id: Some(common_v1::CanonicalId {
                ulid: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            }),
            session_key: "onboarding-smoke".to_owned(),
            session_label: "Sensitive user label".to_owned(),
            parent_session_id: Some(common_v1::CanonicalId {
                ulid: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned(),
            }),
            last_run_id: Some(common_v1::CanonicalId {
                ulid: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
            }),
            ..Default::default()
        };
        let payload = session_to_json(&session);

        assert_eq!(
            payload.get("session_id").and_then(serde_json::Value::as_str),
            Some("01ARZ3NDEKTSV4RRFFQ69G5FAV")
        );
        assert_eq!(
            payload.get("session_key").and_then(serde_json::Value::as_str),
            Some("onboarding-smoke")
        );
        assert_eq!(
            payload.get("parent_session_id").and_then(serde_json::Value::as_str),
            Some("01ARZ3NDEKTSV4RRFFQ69G5FAW")
        );
        assert_eq!(
            payload.get("last_run_id").and_then(serde_json::Value::as_str),
            Some("01ARZ3NDEKTSV4RRFFQ69G5FAX")
        );
        assert_eq!(
            payload.get("session_label").and_then(serde_json::Value::as_str),
            Some("Sensitive user label")
        );
    }

    #[test]
    fn text_identifier_renderer_preserves_resume_ids() {
        assert_eq!(
            optional_canonical_id_text(&Some(common_v1::CanonicalId {
                ulid: " 01ARZ3NDEKTSV4RRFFQ69G5FAX ".to_owned(),
            })),
            "01ARZ3NDEKTSV4RRFFQ69G5FAX"
        );
        assert_eq!(optional_canonical_id_text(&None), "none");
        assert_eq!(
            optional_canonical_id_text(&Some(common_v1::CanonicalId { ulid: " ".to_owned() })),
            "none"
        );
    }

    #[test]
    fn cleanup_warning_renderers_reveal_only_presence() {
        let warning =
            "pid=4821 ports=4567 start_context=cwd=C:/secret command=tool --token raw-secret";

        assert_eq!(redacted_cleanup_warning_json(warning), serde_json::json!("<redacted>"));
        assert_eq!(redacted_cleanup_warning_text(warning), "<redacted>");
        assert_eq!(redacted_cleanup_warning_json("  "), serde_json::Value::Null);
        assert_eq!(redacted_cleanup_warning_text("  "), "none");
    }

    #[test]
    fn cleanup_session_request_requires_identifier() {
        let error = build_cleanup_session_request(None, None)
            .err()
            .expect("cleanup session should require session_id or session_key");
        assert!(
            error.to_string().contains("session_id or session_key is required"),
            "error should explain missing identity: {error}"
        );
    }
}
