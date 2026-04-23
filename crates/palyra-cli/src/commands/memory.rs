use crate::args::MemoryWorkspaceCommand;
use crate::commands::memory_external_index::{
    emit_memory_index_drift, emit_memory_index_reconcile, memory_external_index_payload,
    print_external_drift_line, print_external_index_line,
};
use crate::*;

pub(crate) fn run_memory(command: MemoryCommand) -> Result<()> {
    let root_context = app::current_root_context()
        .ok_or_else(|| anyhow!("CLI root context is unavailable for memory command"))?;
    let runtime = build_runtime()?;
    match command {
        MemoryCommand::Status { .. }
        | MemoryCommand::Index { .. }
        | MemoryCommand::IndexDrift { .. }
        | MemoryCommand::IndexReconcile { .. }
        | MemoryCommand::Workspace { .. }
        | MemoryCommand::Recall { .. }
        | MemoryCommand::SearchAll { .. }
        | MemoryCommand::SessionSearch { .. }
        | MemoryCommand::RecallArtifacts { .. }
        | MemoryCommand::Learning { .. } => runtime.block_on(run_memory_admin_async(command)),
        other => {
            let connection = root_context.resolve_grpc_connection(
                app::ConnectionOverrides::default(),
                app::ConnectionDefaults::USER,
            )?;
            runtime.block_on(run_memory_async(other, connection))
        }
    }
}

pub(crate) async fn run_memory_async(
    command: MemoryCommand,
    connection: AgentConnection,
) -> Result<()> {
    let mut client =
        memory_v1::memory_service_client::MemoryServiceClient::connect(connection.grpc_url.clone())
            .await
            .with_context(|| {
                format!("failed to connect gateway gRPC endpoint {}", connection.grpc_url)
            })?;

    match command {
        MemoryCommand::Search {
            query,
            scope,
            session,
            channel,
            top_k,
            min_score,
            tag,
            source,
            include_score_breakdown,
            show_metadata,
            json,
        } => {
            if query.trim().is_empty() {
                return Err(anyhow!("memory search query cannot be empty"));
            }
            let min_score =
                parse_float_arg(min_score, "memory search --min-score", 0.0, 1.0, Some(0.0))?;
            let (channel_scope, session_scope) =
                resolve_memory_scope(scope, channel, session, &connection)?;
            let mut request = Request::new(memory_v1::SearchMemoryRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                query,
                channel: channel_scope.unwrap_or_default(),
                session_id: session_scope.map(|ulid| common_v1::CanonicalId { ulid }),
                top_k: top_k.unwrap_or(5),
                min_score,
                tags: tag,
                sources: source.into_iter().map(memory_source_to_proto).collect(),
                include_score_breakdown,
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response = client
                .search_memory(request)
                .await
                .context("failed to call memory SearchMemory")?
                .into_inner();
            if output::preferred_json(json) {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "hits": response.hits.iter().map(memory_search_hit_to_json).collect::<Vec<_>>(),
                    }))
                    .context("failed to serialize JSON output")?
                );
            } else if output::preferred_ndjson(json, false) {
                output::print_json_line(
                    &json!({
                        "hits": response
                            .hits
                            .iter()
                            .map(memory_search_hit_to_json)
                            .collect::<Vec<_>>(),
                    }),
                    "failed to encode memory search output as NDJSON",
                )?;
            } else {
                println!("memory.search hits={}", response.hits.len());
                for hit in response.hits {
                    let item = hit.item.as_ref();
                    let id = item
                        .and_then(|value| value.memory_id.as_ref())
                        .map(|value| value.ulid.as_str())
                        .unwrap_or("unknown");
                    let source_label =
                        item.map(|value| memory_source_to_text(value.source)).unwrap_or("unknown");
                    let created_at = item.map(|value| value.created_at_unix_ms).unwrap_or_default();
                    println!(
                        "memory.hit id={} source={} score={:.4} created_at_ms={} snippet={}",
                        id, source_label, hit.score, created_at, hit.snippet
                    );
                    if show_metadata {
                        let channel = item.map(|value| value.channel.as_str()).unwrap_or_default();
                        let session_scope = memory_session_scope_label(
                            item.and_then(|value| value.session_id.as_ref()).is_some(),
                        );
                        let tags = item
                            .map(|value| {
                                if value.tags.is_empty() {
                                    "none".to_owned()
                                } else {
                                    value.tags.join(",")
                                }
                            })
                            .unwrap_or_else(|| "none".to_owned());
                        let confidence = item.map(|value| value.confidence).unwrap_or_default();
                        let ttl_unix_ms = item.map(|value| value.ttl_unix_ms).unwrap_or_default();
                        let updated_at_unix_ms =
                            item.map(|value| value.updated_at_unix_ms).unwrap_or_default();
                        let content_hash =
                            item.map(|value| value.content_hash.as_str()).unwrap_or_default();
                        println!(
                            "memory.hit.meta id={} channel={} session_scope={} tags={} confidence={:.3} ttl_unix_ms={} updated_at_unix_ms={} content_hash={}",
                            id,
                            channel,
                            session_scope,
                            tags,
                            confidence,
                            ttl_unix_ms,
                            updated_at_unix_ms,
                            content_hash
                        );
                    }
                    if include_score_breakdown {
                        if let Some(breakdown) = hit.breakdown.as_ref() {
                            println!(
                                "memory.hit.breakdown id={} lexical_score={:.4} vector_score={:.4} recency_score={:.4} final_score={:.4}",
                                id,
                                breakdown.lexical_score,
                                breakdown.vector_score,
                                breakdown.recency_score,
                                breakdown.final_score
                            );
                        }
                    }
                }
            }
        }
        MemoryCommand::Purge { session, channel, principal, json } => {
            if !principal && session.is_none() && channel.is_none() {
                return Err(anyhow!(
                    "memory purge requires one of: --principal, --session, or --channel"
                ));
            }
            let session_id = if let Some(session) = session {
                validate_canonical_id(session.as_str())
                    .context("memory purge --session must be a canonical ULID")?;
                Some(common_v1::CanonicalId { ulid: session })
            } else {
                None
            };
            let mut request = Request::new(memory_v1::PurgeMemoryRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                channel: channel.unwrap_or_default(),
                session_id,
                purge_all_principal: principal,
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response = client
                .purge_memory(request)
                .await
                .context("failed to call memory PurgeMemory")?
                .into_inner();
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(
                        &json!({ "deleted_count": response.deleted_count })
                    )
                    .context("failed to serialize JSON output")?
                );
            } else {
                println!("memory.purge deleted_count={}", response.deleted_count);
            }
        }
        MemoryCommand::Ingest {
            content,
            source,
            session,
            channel,
            tag,
            confidence,
            ttl_unix_ms,
            json,
        } => {
            if content.trim().is_empty() {
                return Err(anyhow!("memory ingest content cannot be empty"));
            }
            let confidence =
                parse_float_arg(confidence, "memory ingest --confidence", 0.0, 1.0, Some(1.0))?;
            let session_id = if let Some(session) = session {
                validate_canonical_id(session.as_str())
                    .context("memory ingest --session must be a canonical ULID")?;
                Some(common_v1::CanonicalId { ulid: session })
            } else {
                None
            };
            let mut request = Request::new(memory_v1::IngestMemoryRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                source: memory_source_to_proto(source),
                content_text: content,
                channel: channel.unwrap_or(connection.channel.clone()),
                session_id,
                tags: tag,
                confidence,
                ttl_unix_ms: ttl_unix_ms.unwrap_or_default(),
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response = client
                .ingest_memory(request)
                .await
                .context("failed to call memory IngestMemory")?
                .into_inner();
            let item = response.item.context("memory IngestMemory returned empty item payload")?;
            if json {
                println!("{}", serde_json::to_string_pretty(&memory_item_to_json(&item))?);
            } else {
                println!(
                    "memory.ingest id={} source={} created_at_ms={}",
                    item.memory_id.map(|value| value.ulid).unwrap_or_default(),
                    memory_source_to_text(item.source),
                    item.created_at_unix_ms
                );
            }
        }
        MemoryCommand::Status { .. }
        | MemoryCommand::Index { .. }
        | MemoryCommand::IndexDrift { .. }
        | MemoryCommand::IndexReconcile { .. }
        | MemoryCommand::Workspace { .. }
        | MemoryCommand::Recall { .. }
        | MemoryCommand::SearchAll { .. }
        | MemoryCommand::SessionSearch { .. }
        | MemoryCommand::RecallArtifacts { .. }
        | MemoryCommand::Learning { .. } => {
            unreachable!("memory admin commands are handled by run_memory_admin_async")
        }
    }

    std::io::stdout().flush().context("stdout flush failed")
}

async fn run_memory_admin_async(command: MemoryCommand) -> Result<()> {
    let context =
        client::control_plane::connect_admin_console(app::ConnectionOverrides::default()).await?;
    match command {
        MemoryCommand::Status { json } => {
            let payload = context.client.get_json_value("console/v1/memory/status").await?;
            emit_memory_status(&payload, output::preferred_json(json))
        }
        MemoryCommand::Index { batch_size, until_complete, run_maintenance, json } => {
            let mut request = json!({
                "until_complete": until_complete,
                "run_maintenance": run_maintenance,
            });
            if let Some(batch_size) = batch_size {
                request["batch_size"] = json!(batch_size);
            }
            let payload =
                context.client.post_json_value("console/v1/memory/index", &request).await?;
            emit_memory_index(&payload, output::preferred_json(json))
        }
        MemoryCommand::IndexDrift { json } => {
            let payload = context.client.get_json_value("console/v1/memory/index/drift").await?;
            emit_memory_index_drift(&payload, output::preferred_json(json))
        }
        MemoryCommand::IndexReconcile { batch_size, json } => {
            let mut request = json!({});
            if let Some(batch_size) = batch_size {
                request["batch_size"] = json!(batch_size);
            }
            let payload = context
                .client
                .post_json_value("console/v1/memory/index/reconcile", &request)
                .await?;
            emit_memory_index_reconcile(&payload, output::preferred_json(json))
        }
        MemoryCommand::Workspace { command } => match command {
            MemoryWorkspaceCommand::List {
                prefix,
                channel,
                agent_id,
                include_deleted,
                limit,
                json,
            } => {
                let path = build_console_query_path(
                    "console/v1/memory/workspace/documents",
                    vec![
                        ("prefix", prefix),
                        ("channel", channel),
                        ("agent_id", agent_id),
                        ("include_deleted", include_deleted.then(|| "true".to_owned())),
                        ("limit", limit.map(|value| value.to_string())),
                    ],
                );
                let payload = context.client.get_json_value(path.as_str()).await?;
                emit_admin_payload(
                    "workspace.list",
                    &payload,
                    output::preferred_json(json),
                    &["/documents"],
                )
            }
            MemoryWorkspaceCommand::Get { path, channel, agent_id, include_deleted, json } => {
                let path = build_console_query_path(
                    "console/v1/memory/workspace/document",
                    vec![
                        ("path", Some(path)),
                        ("channel", channel),
                        ("agent_id", agent_id),
                        ("include_deleted", include_deleted.then(|| "true".to_owned())),
                    ],
                );
                let payload = context.client.get_json_value(path.as_str()).await?;
                emit_admin_payload(
                    "workspace.get",
                    &payload,
                    output::preferred_json(json),
                    &["/document"],
                )
            }
            MemoryWorkspaceCommand::Write {
                path,
                content,
                title,
                channel,
                agent_id,
                session,
                manual_override,
                json,
            } => {
                let request = json!({
                    "path": path,
                    "content_text": content,
                    "title": title,
                    "channel": channel,
                    "agent_id": agent_id,
                    "session_id": session,
                    "manual_override": manual_override,
                });
                let payload = context
                    .client
                    .post_json_value("console/v1/memory/workspace/document", &request)
                    .await?;
                emit_admin_payload(
                    "workspace.write",
                    &payload,
                    output::preferred_json(json),
                    &["/document"],
                )
            }
            MemoryWorkspaceCommand::Move { path, next_path, channel, agent_id, session, json } => {
                let request = json!({
                    "path": path,
                    "next_path": next_path,
                    "channel": channel,
                    "agent_id": agent_id,
                    "session_id": session,
                });
                let payload = context
                    .client
                    .post_json_value("console/v1/memory/workspace/document/move", &request)
                    .await?;
                emit_admin_payload(
                    "workspace.move",
                    &payload,
                    output::preferred_json(json),
                    &["/document"],
                )
            }
            MemoryWorkspaceCommand::Delete { path, channel, agent_id, session, json } => {
                let request = json!({
                    "path": path,
                    "channel": channel,
                    "agent_id": agent_id,
                    "session_id": session,
                });
                let payload = context
                    .client
                    .post_json_value("console/v1/memory/workspace/document/delete", &request)
                    .await?;
                emit_admin_payload(
                    "workspace.delete",
                    &payload,
                    output::preferred_json(json),
                    &["/document"],
                )
            }
            MemoryWorkspaceCommand::Pin { path, pinned, channel, agent_id, json } => {
                let request = json!({
                    "path": path,
                    "pinned": pinned,
                    "channel": channel,
                    "agent_id": agent_id,
                });
                let payload = context
                    .client
                    .post_json_value("console/v1/memory/workspace/document/pin", &request)
                    .await?;
                emit_admin_payload(
                    "workspace.pin",
                    &payload,
                    output::preferred_json(json),
                    &["/document"],
                )
            }
            MemoryWorkspaceCommand::Versions { path, channel, agent_id, limit, json } => {
                let path = build_console_query_path(
                    "console/v1/memory/workspace/document/versions",
                    vec![
                        ("path", Some(path)),
                        ("channel", channel),
                        ("agent_id", agent_id),
                        ("limit", limit.map(|value| value.to_string())),
                    ],
                );
                let payload = context.client.get_json_value(path.as_str()).await?;
                emit_admin_payload(
                    "workspace.versions",
                    &payload,
                    output::preferred_json(json),
                    &["/versions"],
                )
            }
            MemoryWorkspaceCommand::Bootstrap {
                channel,
                agent_id,
                session,
                force_repair,
                json,
            } => {
                let request = json!({
                    "channel": channel,
                    "agent_id": agent_id,
                    "session_id": session,
                    "force_repair": force_repair,
                });
                let payload = context
                    .client
                    .post_json_value("console/v1/memory/workspace/bootstrap", &request)
                    .await?;
                emit_admin_payload(
                    "workspace.bootstrap",
                    &payload,
                    output::preferred_json(json),
                    &["/bootstrap"],
                )
            }
            MemoryWorkspaceCommand::Search {
                query,
                channel,
                agent_id,
                prefix,
                top_k,
                min_score,
                include_historical,
                include_quarantined,
                json,
            } => {
                let min_score = parse_float_arg(
                    min_score,
                    "memory workspace search --min-score",
                    0.0,
                    1.0,
                    Some(0.0),
                )?;
                let path = build_console_query_path(
                    "console/v1/memory/workspace/search",
                    vec![
                        ("query", Some(query)),
                        ("channel", channel),
                        ("agent_id", agent_id),
                        ("prefix", prefix),
                        ("top_k", top_k.map(|value| value.to_string())),
                        ("min_score", Some(min_score.to_string())),
                        ("include_historical", include_historical.then(|| "true".to_owned())),
                        ("include_quarantined", include_quarantined.then(|| "true".to_owned())),
                    ],
                );
                let payload = context.client.get_json_value(path.as_str()).await?;
                emit_admin_payload(
                    "workspace.search",
                    &payload,
                    output::preferred_json(json),
                    &["/hits"],
                )
            }
        },
        MemoryCommand::Recall {
            query,
            session,
            channel,
            agent_id,
            memory_top_k,
            workspace_top_k,
            min_score,
            workspace_prefix,
            include_workspace_historical,
            include_workspace_quarantined,
            json,
        } => {
            let min_score =
                parse_float_arg(min_score, "memory recall --min-score", 0.0, 1.0, Some(0.0))?;
            let request = json!({
                "query": query,
                "session_id": session,
                "channel": channel,
                "agent_id": agent_id,
                "memory_top_k": memory_top_k,
                "workspace_top_k": workspace_top_k,
                "min_score": min_score,
                "workspace_prefix": workspace_prefix,
                "include_workspace_historical": include_workspace_historical,
                "include_workspace_quarantined": include_workspace_quarantined,
            });
            let payload = context
                .client
                .post_json_value("console/v1/memory/recall/preview", &request)
                .await?;
            emit_admin_payload(
                "memory.recall",
                &payload,
                output::preferred_json(json),
                &[
                    "/plan",
                    "/top_candidates",
                    "/structured_output",
                    "/memory_hits",
                    "/workspace_hits",
                    "/transcript_hits",
                    "/checkpoint_hits",
                    "/compaction_hits",
                    "/artifact",
                    "/parameter_delta",
                    "/prompt_preview",
                ],
            )
        }
        MemoryCommand::SearchAll {
            query,
            session,
            channel,
            agent_id,
            top_k,
            min_score,
            workspace_prefix,
            json,
        } => {
            let min_score =
                parse_float_arg(min_score, "memory search-all --min-score", 0.0, 1.0, Some(0.0))?;
            let path = build_console_query_path(
                "console/v1/memory/search-all",
                vec![
                    ("q", Some(query)),
                    ("session_id", session),
                    ("channel", channel),
                    ("agent_id", agent_id),
                    ("top_k", top_k.map(|value| value.to_string())),
                    ("min_score", Some(min_score.to_string())),
                    ("workspace_prefix", workspace_prefix),
                ],
            );
            let payload = context.client.get_json_value(path.as_str()).await?;
            emit_admin_payload(
                "memory.search_all",
                &payload,
                output::preferred_json(json),
                &["/groups"],
            )
        }
        MemoryCommand::SessionSearch {
            query,
            channel,
            top_k,
            min_score,
            window_before,
            window_after,
            max_windows_per_session,
            include_archived,
            json,
        } => {
            let min_score = parse_float_arg(
                min_score,
                "memory session-search --min-score",
                0.0,
                1.0,
                Some(0.0),
            )?;
            let path = build_console_query_path(
                "console/v1/memory/session-search",
                vec![
                    ("q", Some(query)),
                    ("channel", channel),
                    ("top_k", top_k.map(|value| value.to_string())),
                    ("min_score", Some(min_score.to_string())),
                    ("window_before", window_before.map(|value| value.to_string())),
                    ("window_after", window_after.map(|value| value.to_string())),
                    (
                        "max_windows_per_session",
                        max_windows_per_session.map(|value| value.to_string()),
                    ),
                    ("include_archived", include_archived.then(|| "true".to_owned())),
                ],
            );
            let payload = context.client.get_json_value(path.as_str()).await?;
            emit_admin_payload(
                "memory.session_search",
                &payload,
                output::preferred_json(json),
                &["/groups", "/diagnostics", "/artifact"],
            )
        }
        MemoryCommand::RecallArtifacts { kind, session, channel, limit, json } => {
            let path = build_console_query_path(
                "console/v1/memory/recall-artifacts",
                vec![
                    ("kind", kind),
                    ("session_id", session),
                    ("channel", channel),
                    ("limit", limit.map(|value| value.to_string())),
                ],
            );
            let payload = context.client.get_json_value(path.as_str()).await?;
            emit_admin_payload(
                "memory.recall_artifacts",
                &payload,
                output::preferred_json(json),
                &["/artifacts"],
            )
        }
        MemoryCommand::Learning { command } => match command {
            MemoryLearningCommand::List {
                candidate_kind,
                status,
                risk_level,
                scope_kind,
                scope_id,
                session,
                min_confidence,
                max_confidence,
                limit,
                json,
            } => {
                let path = build_console_query_path(
                    "console/v1/memory/learning/candidates",
                    vec![
                        ("candidate_kind", candidate_kind),
                        ("status", status),
                        ("risk_level", risk_level),
                        ("scope_kind", scope_kind),
                        ("scope_id", scope_id),
                        ("session_id", session),
                        ("min_confidence", min_confidence),
                        ("max_confidence", max_confidence),
                        ("limit", limit.map(|value| value.to_string())),
                    ],
                );
                let payload = context.client.get_json_value(path.as_str()).await?;
                emit_admin_payload(
                    "memory.learning.list",
                    &payload,
                    output::preferred_json(json),
                    &["/candidates"],
                )
            }
            MemoryLearningCommand::History { candidate_id, json } => {
                let payload = context
                    .client
                    .get_json_value(
                        format!(
                            "console/v1/memory/learning/candidates/{}/history",
                            percent_encode_component(candidate_id.as_str())
                        )
                        .as_str(),
                    )
                    .await?;
                emit_admin_payload(
                    "memory.learning.history",
                    &payload,
                    output::preferred_json(json),
                    &["/history"],
                )
            }
            MemoryLearningCommand::Review {
                candidate_id,
                status,
                summary,
                payload,
                apply_preference,
                json,
            } => {
                let request = json!({
                    "status": status,
                    "action_summary": summary,
                    "action_payload_json": payload,
                    "apply_preference": apply_preference,
                });
                let payload = context
                    .client
                    .post_json_value(
                        format!(
                            "console/v1/memory/learning/candidates/{}/review",
                            percent_encode_component(candidate_id.as_str())
                        )
                        .as_str(),
                        &request,
                    )
                    .await?;
                emit_admin_payload(
                    "memory.learning.review",
                    &payload,
                    output::preferred_json(json),
                    &["/candidate", "/preference"],
                )
            }
            MemoryLearningCommand::Apply { candidate_id, summary, json } => {
                let request = json!({
                    "action_summary": summary,
                });
                let payload = context
                    .client
                    .post_json_value(
                        format!(
                            "console/v1/memory/learning/candidates/{}/apply",
                            percent_encode_component(candidate_id.as_str())
                        )
                        .as_str(),
                        &request,
                    )
                    .await?;
                emit_admin_payload(
                    "memory.learning.apply",
                    &payload,
                    output::preferred_json(json),
                    &["/candidate", "/apply"],
                )
            }
            MemoryLearningCommand::Preferences {
                status,
                scope_kind,
                scope_id,
                key,
                limit,
                json,
            } => {
                let path = build_console_query_path(
                    "console/v1/memory/preferences",
                    vec![
                        ("status", status),
                        ("scope_kind", scope_kind),
                        ("scope_id", scope_id),
                        ("key", key),
                        ("limit", limit.map(|value| value.to_string())),
                    ],
                );
                let payload = context.client.get_json_value(path.as_str()).await?;
                emit_admin_payload(
                    "memory.learning.preferences",
                    &payload,
                    output::preferred_json(json),
                    &["/preferences"],
                )
            }
            MemoryLearningCommand::PromoteProcedure {
                candidate_id,
                skill_id,
                version,
                publisher,
                name,
                accept_candidate,
                json,
            } => {
                let request = json!({
                    "skill_id": skill_id,
                    "version": version,
                    "publisher": publisher,
                    "name": name,
                    "accept_candidate": accept_candidate,
                });
                let payload = context
                    .client
                    .post_json_value(
                        format!(
                            "console/v1/skills/candidates/{}/promote",
                            percent_encode_component(candidate_id.as_str())
                        )
                        .as_str(),
                        &request,
                    )
                    .await?;
                emit_admin_payload(
                    "memory.learning.promote_procedure",
                    &payload,
                    output::preferred_json(json),
                    &["/skill"],
                )
            }
        },
        _ => unreachable!("memory user-scoped commands are handled by run_memory_async"),
    }
}

fn emit_admin_payload(
    label: &str,
    payload: &Value,
    json_output: bool,
    pointers: &[&str],
) -> Result<()> {
    if json_output {
        return output::print_json_pretty(payload, "failed to encode memory admin payload as JSON");
    }
    println!("{label}");
    for pointer in pointers {
        if let Some(value) = payload.pointer(pointer) {
            println!("{pointer}={}", serde_json::to_string(value)?);
        }
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn build_console_query_path(base: &str, params: Vec<(&str, Option<String>)>) -> String {
    let parts = params
        .into_iter()
        .filter_map(|(key, value)| value.map(|value| (key, value)))
        .map(|(key, value)| {
            format!(
                "{}={}",
                percent_encode_component(key),
                percent_encode_component(value.as_str())
            )
        })
        .collect::<Vec<_>>();
    if parts.is_empty() {
        return base.to_owned();
    }
    format!("{base}?{}", parts.join("&"))
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

fn emit_memory_status(payload: &Value, json_output: bool) -> Result<()> {
    if json_output {
        return output::print_json_pretty(payload, "failed to encode memory status as JSON");
    }

    let entries = payload.pointer("/usage/entries").and_then(Value::as_u64).unwrap_or(0);
    let approx_bytes = payload.pointer("/usage/approx_bytes").and_then(Value::as_u64).unwrap_or(0);
    let mode = payload.pointer("/embeddings/mode").and_then(Value::as_str).unwrap_or("unknown");
    let target_model =
        payload.pointer("/embeddings/target_model_id").and_then(Value::as_str).unwrap_or("unknown");
    let target_dims =
        payload.pointer("/embeddings/target_dims").and_then(Value::as_u64).unwrap_or(0);
    let target_version =
        payload.pointer("/embeddings/target_version").and_then(Value::as_i64).unwrap_or(0);
    let indexed_count =
        payload.pointer("/embeddings/indexed_count").and_then(Value::as_u64).unwrap_or(0);
    let pending_count =
        payload.pointer("/embeddings/pending_count").and_then(Value::as_u64).unwrap_or(0);
    let max_entries = payload
        .pointer("/retention/max_entries")
        .and_then(Value::as_u64)
        .map_or("none".to_owned(), |v| v.to_string());
    let max_bytes = payload
        .pointer("/retention/max_bytes")
        .and_then(Value::as_u64)
        .map_or("none".to_owned(), |v| v.to_string());
    let ttl_days = payload
        .pointer("/retention/ttl_days")
        .and_then(Value::as_u64)
        .map_or("none".to_owned(), |v| v.to_string());
    let vacuum_schedule =
        payload.pointer("/retention/vacuum_schedule").and_then(Value::as_str).unwrap_or("none");
    let interval_ms =
        payload.pointer("/maintenance/interval_ms").and_then(Value::as_i64).unwrap_or_default();
    let last_run_at_ms = payload
        .pointer("/maintenance/last_run/ran_at_unix_ms")
        .and_then(Value::as_i64)
        .map_or("none".to_owned(), |v| v.to_string());
    let last_deleted_total = payload
        .pointer("/maintenance/last_run/deleted_total_count")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let next_run_at_ms = payload
        .pointer("/maintenance/next_run_at_unix_ms")
        .and_then(Value::as_i64)
        .map_or("none".to_owned(), |v| v.to_string());
    let next_vacuum_due_at_ms = payload
        .pointer("/maintenance/next_vacuum_due_at_unix_ms")
        .and_then(Value::as_i64)
        .map_or("none".to_owned(), |v| v.to_string());

    println!(
        "memory.status entries={} approx_bytes={} embeddings_mode={} target_model={} target_dims={} target_version={} indexed={} pending={}",
        entries, approx_bytes, mode, target_model, target_dims, target_version, indexed_count, pending_count
    );
    println!(
        "memory.retention max_entries={} max_bytes={} ttl_days={} vacuum_schedule={}",
        max_entries, max_bytes, ttl_days, vacuum_schedule
    );
    println!(
        "memory.maintenance interval_ms={} last_run_at_unix_ms={} last_deleted_total={} next_run_at_unix_ms={} next_vacuum_due_at_unix_ms={}",
        interval_ms, last_run_at_ms, last_deleted_total, next_run_at_ms, next_vacuum_due_at_ms
    );
    if let Some(external_index) = memory_external_index_payload(payload) {
        print_external_index_line("memory.external_index", external_index);
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn emit_memory_index(payload: &Value, json_output: bool) -> Result<()> {
    if json_output {
        return output::print_json_pretty(payload, "failed to encode memory index payload as JSON");
    }

    let batches_executed =
        payload.pointer("/index/batches_executed").and_then(Value::as_u64).unwrap_or(0);
    let scanned_count =
        payload.pointer("/index/scanned_count").and_then(Value::as_u64).unwrap_or(0);
    let updated_count =
        payload.pointer("/index/updated_count").and_then(Value::as_u64).unwrap_or(0);
    let pending_count =
        payload.pointer("/index/pending_count").and_then(Value::as_u64).unwrap_or(0);
    let complete = payload.pointer("/index/complete").and_then(Value::as_bool).unwrap_or(false);
    let target_model =
        payload.pointer("/index/target_model_id").and_then(Value::as_str).unwrap_or("unknown");
    let target_dims = payload.pointer("/index/target_dims").and_then(Value::as_u64).unwrap_or(0);
    let mode = payload.pointer("/embeddings/mode").and_then(Value::as_str).unwrap_or("unknown");
    println!(
        "memory.index batches={} scanned={} updated={} pending={} complete={} embeddings_mode={} target_model={} target_dims={}",
        batches_executed,
        scanned_count,
        updated_count,
        pending_count,
        complete,
        mode,
        target_model,
        target_dims
    );
    if let Some(maintenance) = payload.get("maintenance").filter(|value| !value.is_null()) {
        let deleted_total =
            maintenance.get("deleted_total_count").and_then(Value::as_u64).unwrap_or(0);
        let vacuum_performed =
            maintenance.get("vacuum_performed").and_then(Value::as_bool).unwrap_or(false);
        let ran_at_unix_ms = maintenance
            .get("ran_at_unix_ms")
            .and_then(Value::as_i64)
            .map_or("none".to_owned(), |v| v.to_string());
        println!(
            "memory.index.maintenance ran_at_unix_ms={} deleted_total={} vacuum_performed={}",
            ran_at_unix_ms, deleted_total, vacuum_performed
        );
    }
    if let Some(external_indexer) = payload.get("external_indexer").filter(|value| !value.is_null())
    {
        let indexed_memory_items =
            external_indexer.get("indexed_memory_items").and_then(Value::as_u64).unwrap_or(0);
        let indexed_workspace_chunks =
            external_indexer.get("indexed_workspace_chunks").and_then(Value::as_u64).unwrap_or(0);
        let pending_memory_items =
            external_indexer.get("pending_memory_items").and_then(Value::as_u64).unwrap_or(0);
        let pending_workspace_chunks =
            external_indexer.get("pending_workspace_chunks").and_then(Value::as_u64).unwrap_or(0);
        let checkpoint_committed =
            external_indexer.get("checkpoint_committed").and_then(Value::as_bool).unwrap_or(false);
        let complete = external_indexer.get("complete").and_then(Value::as_bool).unwrap_or(false);
        println!(
            "memory.external_indexer indexed_memory_items={} indexed_workspace_chunks={} pending_memory_items={} pending_workspace_chunks={} checkpoint_committed={} complete={}",
            indexed_memory_items,
            indexed_workspace_chunks,
            pending_memory_items,
            pending_workspace_chunks,
            checkpoint_committed,
            complete
        );
    }
    if let Some(external_index) = memory_external_index_payload(payload) {
        print_external_index_line("memory.external_index", external_index);
    }
    if let Some(drift) = payload.get("drift").filter(|value| !value.is_null()) {
        print_external_drift_line("memory.external_index.drift", drift);
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn memory_session_scope_label(has_session_scope: bool) -> &'static str {
    if has_session_scope {
        "present"
    } else {
        "none"
    }
}

#[cfg(test)]
mod tests {
    use super::memory_session_scope_label;
    #[test]
    fn memory_session_scope_label_redacts_identifier_value() {
        assert_eq!(memory_session_scope_label(false), "none");
        assert_eq!(memory_session_scope_label(true), "present");
    }
}
