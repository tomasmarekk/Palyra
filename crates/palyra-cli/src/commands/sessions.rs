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
        | SessionsCommand::History { json, .. }
        | SessionsCommand::Show { json, .. }
        | SessionsCommand::Resolve { json, .. }
        | SessionsCommand::Rename { json, .. }
        | SessionsCommand::Reset { json, .. }
        | SessionsCommand::Cleanup { json, .. }
        | SessionsCommand::Abort { json, .. }
        | SessionsCommand::Retry { json, .. }
        | SessionsCommand::Branch { json, .. }
        | SessionsCommand::TranscriptSearch { json, .. }
        | SessionsCommand::Export { json, .. }
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
                        "session title={} source={} preview={} key={} label={} updated_at_unix_ms={} last_run_state={} last_run_id={} archived_at_unix_ms={}",
                        session_title_for_output(session),
                        empty_to_none(session.title_source.as_str()),
                        empty_to_none(session.preview.as_str()),
                        redacted_text_or_none(!session.session_key.trim().is_empty()),
                        redacted_text_or_none(!session.session_label.trim().is_empty()),
                        session.updated_at_unix_ms,
                        empty_to_none(session.last_run_state.as_str()),
                        redacted_presence_for_output(session.last_run_id.is_some()),
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
                        redacted_text_or_none(!first.session_key.trim().is_empty())
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
                    "sessions.show title={} source={} preview={} key={} label={} created_at_unix_ms={} updated_at_unix_ms={} last_run_state={} last_run_id={} archived_at_unix_ms={}",
                    session_title_for_output(&session),
                    empty_to_none(session.title_source.as_str()),
                    empty_to_none(session.preview.as_str()),
                    redacted_text_or_none(!session.session_key.trim().is_empty()),
                    redacted_text_or_none(!session.session_label.trim().is_empty()),
                    session.created_at_unix_ms,
                    session.updated_at_unix_ms,
                    empty_to_none(session.last_run_state.as_str()),
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
                    "sessions.resolve title={} source={} preview={} key={} label={} created={} reset_applied={} archived_at_unix_ms={}",
                    session_title_for_output(&session),
                    empty_to_none(session.title_source.as_str()),
                    empty_to_none(session.preview.as_str()),
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
                .resolve_session(SessionResolveInput {
                    session_id: Some(resolve_required_canonical_id(session_id)?),
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
        SessionsCommand::Retry { session_id, json: _ } => {
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
            let prompt = json_required_string(&payload, "/text")?;
            let origin_kind = json_optional_string(&payload, "/origin_kind");
            let origin_run_id = json_optional_string(&payload, "/origin_run_id");
            let parameter_delta_json = payload
                .pointer("/parameter_delta")
                .filter(|value| !value.is_null())
                .map(serde_json::to_string)
                .transpose()?;
            let request = build_agent_run_input(AgentRunInputArgs {
                session_id: Some(resolve_required_canonical_id(session_id.clone())?),
                session_key: None,
                session_label: None,
                require_existing: true,
                reset_session: false,
                run_id: None,
                prompt,
                allow_sensitive_tools: false,
                origin_kind,
                origin_run_id,
                parameter_delta_json,
            })?;
            let mut client = client::runtime::GatewayRuntimeClient::connect(connection).await?;
            let _resolved = stream_agent_events_async(&mut client, request, |event| {
                if json {
                    emit_acp_event_ndjson(event)
                } else {
                    emit_agent_event_text(event)
                }
            })
            .await?;
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
        SessionsCommand::CompactPreview { session_id, trigger_reason, trigger_policy, json: _ } => {
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
            priority,
            max_attempts,
            budget_tokens,
            not_before_unix_ms,
            expires_at_unix_ms,
            json: _,
        } => {
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
                    "sessions.background.enqueue task_id={} state={} priority={} max_attempts={}",
                    redacted_optional_identifier_for_output(
                        task.pointer("/task_id").and_then(Value::as_str)
                    ),
                    json_optional_string_in(task, "/state").unwrap_or_else(|| "unknown".to_owned()),
                    task.pointer("/priority").and_then(Value::as_i64).unwrap_or_default(),
                    task.pointer("/max_attempts").and_then(Value::as_u64).unwrap_or_default()
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
                        "task task_id={} state={} priority={} run_id={} created_at_unix_ms={}",
                        redacted_optional_identifier_for_output(
                            task.pointer("/task_id").and_then(Value::as_str)
                        ),
                        json_optional_string_in(task, "/state")
                            .unwrap_or_else(|| "unknown".to_owned()),
                        task.pointer("/priority").and_then(Value::as_i64).unwrap_or_default(),
                        redacted_optional_identifier_for_output(
                            task.pointer("/target_run_id").and_then(Value::as_str)
                        ),
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
                println!(
                    "sessions.background.show task_id={} state={} attempt_count={} max_attempts={} run_id={}",
                    redacted_optional_identifier_for_output(task.pointer("/task_id").and_then(Value::as_str)),
                    json_optional_string_in(task, "/state").unwrap_or_else(|| "unknown".to_owned()),
                    task.pointer("/attempt_count").and_then(Value::as_u64).unwrap_or_default(),
                    task.pointer("/max_attempts").and_then(Value::as_u64).unwrap_or_default(),
                    redacted_optional_identifier_for_output(task.pointer("/target_run_id").and_then(Value::as_str))
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

fn session_to_json(session: &gateway_v1::SessionSummary) -> Value {
    json!({
        "session_id": if session.session_id.is_some() { Value::String(REDACTED.to_owned()) } else { Value::Null },
        "session_key": redacted_presence_json_value(!session.session_key.trim().is_empty()),
        "session_label": redacted_presence_json_value(!session.session_label.trim().is_empty()),
        "title": empty_to_json_or_null(session.title.as_str()),
        "title_source": empty_to_json_or_null(session.title_source.as_str()),
        "title_generator_version": empty_to_json_or_null(session.title_generator_version.as_str()),
        "preview": empty_to_json_or_null(session.preview.as_str()),
        "preview_state": empty_to_json_or_null(session.preview_state.as_str()),
        "last_intent": empty_to_json_or_null(session.last_intent.as_str()),
        "last_summary": empty_to_json_or_null(session.last_summary.as_str()),
        "match_snippet": empty_to_json_or_null(session.match_snippet.as_str()),
        "branch_state": empty_to_json_or_null(session.branch_state.as_str()),
        "parent_session_id": if session.parent_session_id.is_some() {
            Value::String(REDACTED.to_owned())
        } else {
            Value::Null
        },
        "last_run_state": empty_to_json_or_null(session.last_run_state.as_str()),
        "created_at_unix_ms": session.created_at_unix_ms,
        "updated_at_unix_ms": session.updated_at_unix_ms,
        "last_run_id": if session.last_run_id.is_some() { Value::String(REDACTED.to_owned()) } else { Value::Null },
        "archived_at_unix_ms": empty_unix_ms(session.archived_at_unix_ms),
    })
}

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
            json_optional_string_in(task, "/state").unwrap_or_else(|| "unknown".to_owned())
        );
    }
    Ok(())
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
mod tests {
    use super::{build_cleanup_session_request, build_resolve_session_request};

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
