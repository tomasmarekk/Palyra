use crate::args::MemoryWorkspaceCommand;
use crate::commands::memory_external_index::{
    emit_memory_index_drift, emit_memory_index_reconcile, memory_external_index_payload,
    print_external_drift_line, print_external_index_line,
};
use crate::*;

const MEMORY_SEARCH_HITS_PRESENT_CLAIM_BOUNDARY: &str =
    "durable memory hits were returned; cite them as stored memory evidence";
const MEMORY_SEARCH_HITS_ABSENT_CLAIM_BOUNDARY: &str =
    "no durable memory hits were returned by this memory search; this does not search prior session transcripts; use memory search-all or memory session-search for transcript recall";

fn cli_inferred_workspace_memory_prefix() -> Option<String> {
    let current_dir = std::env::current_dir().ok()?;
    cli_project_memory_prefix_from_workspace_root(current_dir.as_path())
}

fn cli_project_memory_prefix_from_workspace_root(root: &std::path::Path) -> Option<String> {
    let canonical = std::fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf());
    let name = cli_last_normal_path_segment(canonical.as_path())?;
    let slug = cli_project_memory_slug(name.as_str());
    let fingerprint = cli_project_memory_root_fingerprint(canonical.as_path());
    let digest = sha256_hex(fingerprint.as_bytes());
    let hash = digest.get(..10)?;
    Some(format!("projects/project-{slug}-{hash}"))
}

fn cli_last_normal_path_segment(path: &std::path::Path) -> Option<String> {
    path.components().rev().find_map(|component| match component {
        std::path::Component::Normal(value) => {
            value.to_str().map(str::trim).filter(|value| !value.is_empty()).map(str::to_owned)
        }
        _ => None,
    })
}

fn cli_project_memory_slug(name: &str) -> String {
    const MAX_SLUG_CHARS: usize = 80;

    let mut slug = String::new();
    let mut previous_separator = false;
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch.to_ascii_lowercase());
            previous_separator = false;
        } else if !previous_separator && !slug.is_empty() {
            slug.push('-');
            previous_separator = true;
        }
        if slug.chars().count() >= MAX_SLUG_CHARS {
            break;
        }
    }
    let slug = slug.trim_matches('-');
    if slug.is_empty() {
        "workspace".to_owned()
    } else {
        slug.to_owned()
    }
}

fn cli_project_memory_root_fingerprint(root: &std::path::Path) -> String {
    let normalized = root.to_string_lossy().replace('\\', "/").trim_end_matches('/').to_owned();
    #[cfg(windows)]
    {
        normalized.to_ascii_lowercase()
    }
    #[cfg(not(windows))]
    {
        normalized
    }
}

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

#[async_trait::async_trait]
trait MemoryReplaceRpc {
    async fn get_memory_item(
        &mut self,
        request: Request<memory_v1::GetMemoryItemRequest>,
    ) -> Result<tonic::Response<memory_v1::GetMemoryItemResponse>, tonic::Status>;

    async fn ingest_memory(
        &mut self,
        request: Request<memory_v1::IngestMemoryRequest>,
    ) -> Result<tonic::Response<memory_v1::IngestMemoryResponse>, tonic::Status>;

    async fn delete_memory_item(
        &mut self,
        request: Request<memory_v1::DeleteMemoryItemRequest>,
    ) -> Result<tonic::Response<memory_v1::DeleteMemoryItemResponse>, tonic::Status>;
}

struct GrpcMemoryReplaceRpc<'a> {
    client:
        &'a mut memory_v1::memory_service_client::MemoryServiceClient<tonic::transport::Channel>,
}

#[async_trait::async_trait]
impl MemoryReplaceRpc for GrpcMemoryReplaceRpc<'_> {
    async fn get_memory_item(
        &mut self,
        request: Request<memory_v1::GetMemoryItemRequest>,
    ) -> Result<tonic::Response<memory_v1::GetMemoryItemResponse>, tonic::Status> {
        self.client.get_memory_item(request).await
    }

    async fn ingest_memory(
        &mut self,
        request: Request<memory_v1::IngestMemoryRequest>,
    ) -> Result<tonic::Response<memory_v1::IngestMemoryResponse>, tonic::Status> {
        self.client.ingest_memory(request).await
    }

    async fn delete_memory_item(
        &mut self,
        request: Request<memory_v1::DeleteMemoryItemRequest>,
    ) -> Result<tonic::Response<memory_v1::DeleteMemoryItemResponse>, tonic::Status> {
        self.client.delete_memory_item(request).await
    }
}

#[derive(Debug)]
struct MemoryReplaceOptions {
    memory_id: String,
    content: String,
    source: Option<MemorySourceArg>,
    tags: Vec<String>,
    confidence: Option<String>,
    ttl_unix_ms: Option<i64>,
}

#[derive(Debug)]
struct MemoryReplaceOutcome {
    replaced_memory_ulid: String,
    replacement: memory_v1::MemoryItem,
    deleted_original: bool,
}

async fn replace_memory_item(
    client: &mut dyn MemoryReplaceRpc,
    connection: &AgentConnection,
    options: MemoryReplaceOptions,
) -> Result<MemoryReplaceOutcome> {
    if options.content.trim().is_empty() {
        return Err(anyhow!("memory replace content cannot be empty"));
    }
    let replacement_confidence = options
        .confidence
        .map(|raw| parse_float_arg(Some(raw), "memory replace --confidence", 0.0, 1.0, None))
        .transpose()?;
    let memory_id = resolve_required_canonical_id(options.memory_id)
        .context("memory replace memory_id must be a canonical ULID")?;
    let replaced_memory_ulid = memory_id.ulid.clone();

    let mut get_request = Request::new(memory_v1::GetMemoryItemRequest {
        v: CANONICAL_PROTOCOL_MAJOR,
        memory_id: Some(memory_id.clone()),
    });
    inject_run_stream_metadata(get_request.metadata_mut(), connection)?;
    let existing = client
        .get_memory_item(get_request)
        .await
        .context("failed to call memory GetMemoryItem before replace")?
        .into_inner()
        .item
        .context("memory GetMemoryItem returned empty item payload before replace")?;

    let replacement_tags =
        if options.tags.is_empty() { existing.tags.clone() } else { options.tags };
    let replacement_source = options.source.map(memory_source_to_proto).unwrap_or(existing.source);
    let replacement_channel = existing.channel.clone();
    let replacement_session_id = existing.session_id.clone();
    let replacement_confidence = replacement_confidence.unwrap_or(existing.confidence);
    let replacement_ttl_unix_ms = options.ttl_unix_ms.unwrap_or(existing.ttl_unix_ms);

    let mut ingest_request = Request::new(memory_v1::IngestMemoryRequest {
        v: CANONICAL_PROTOCOL_MAJOR,
        source: replacement_source,
        content_text: options.content,
        channel: replacement_channel,
        session_id: replacement_session_id,
        tags: replacement_tags,
        confidence: replacement_confidence,
        ttl_unix_ms: replacement_ttl_unix_ms,
    });
    inject_run_stream_metadata(ingest_request.metadata_mut(), connection)?;
    let replacement = client
        .ingest_memory(ingest_request)
        .await
        .context("failed to call memory IngestMemory for replacement")?
        .into_inner()
        .item
        .context("memory IngestMemory returned empty replacement item payload")?;
    let replacement_id =
        replacement.memory_id.as_ref().map(|value| value.ulid.clone()).unwrap_or_default();

    let mut delete_request = Request::new(memory_v1::DeleteMemoryItemRequest {
        v: CANONICAL_PROTOCOL_MAJOR,
        memory_id: Some(memory_id),
    });
    inject_run_stream_metadata(delete_request.metadata_mut(), connection)?;
    let delete_response = client
        .delete_memory_item(delete_request)
        .await
        .with_context(|| {
            format!(
                "failed to call memory DeleteMemoryItem after replacement ingest; replacement_id={replacement_id}"
            )
        })?
        .into_inner();
    if !delete_response.deleted {
        return Err(anyhow!(
            "memory replace ingested replacement_id={} but did not delete original_id={}",
            replacement_id,
            replaced_memory_ulid
        ));
    }

    Ok(MemoryReplaceOutcome {
        replaced_memory_ulid,
        replacement,
        deleted_original: delete_response.deleted,
    })
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
                resolve_memory_scope(scope, channel, session, &connection).await?;
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
            let search_payload = memory_search_output_payload(response.hits.as_slice());
            if output::preferred_json(json) {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&search_payload)
                        .context("failed to serialize JSON output")?
                );
            } else if output::preferred_ndjson(json, false) {
                output::print_json_line(
                    &search_payload,
                    "failed to encode memory search output as NDJSON",
                )?;
            } else {
                let hit_count = response.hits.len();
                println!(
                    "memory.search durable_memory_hits={} claim_boundary={}",
                    hit_count,
                    quoted_text_field(memory_search_claim_boundary(hit_count))
                );
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
        MemoryCommand::Get { memory_id, json } => {
            let memory_id = resolve_required_canonical_id(memory_id)
                .context("memory get memory_id must be a canonical ULID")?;
            let mut request = Request::new(memory_v1::GetMemoryItemRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                memory_id: Some(memory_id.clone()),
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response = client
                .get_memory_item(request)
                .await
                .context("failed to call memory GetMemoryItem")?
                .into_inner();
            let item = response.item.context("memory GetMemoryItem returned empty item payload")?;
            if output::preferred_json(json) {
                output::print_json_pretty(
                    &memory_item_to_json(&item),
                    "failed to encode memory get output as JSON",
                )?;
            } else if output::preferred_ndjson(json, false) {
                output::print_json_line(
                    &memory_item_to_json(&item),
                    "failed to encode memory get output as NDJSON",
                )?;
            } else {
                println!(
                    "memory.get id={} source={} created_at_ms={} content={}",
                    item.memory_id.map(|value| value.ulid).unwrap_or_default(),
                    memory_source_to_text(item.source),
                    item.created_at_unix_ms,
                    item.content_text
                );
            }
        }
        MemoryCommand::Delete { memory_id, json } => {
            let memory_id = resolve_required_canonical_id(memory_id)
                .context("memory delete memory_id must be a canonical ULID")?;
            let memory_ulid = memory_id.ulid.clone();
            let mut request = Request::new(memory_v1::DeleteMemoryItemRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                memory_id: Some(memory_id),
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response = client
                .delete_memory_item(request)
                .await
                .context("failed to call memory DeleteMemoryItem")?
                .into_inner();
            let payload = json!({
                "memory_id": memory_ulid,
                "deleted": response.deleted,
            });
            if output::preferred_json(json) {
                output::print_json_pretty(
                    &payload,
                    "failed to encode memory delete output as JSON",
                )?;
            } else if output::preferred_ndjson(json, false) {
                output::print_json_line(
                    &payload,
                    "failed to encode memory delete output as NDJSON",
                )?;
            } else {
                println!(
                    "memory.delete id={} deleted={}",
                    payload.get("memory_id").and_then(Value::as_str).unwrap_or_default(),
                    response.deleted
                );
            }
        }
        MemoryCommand::Replace {
            memory_id,
            content,
            source,
            tag,
            confidence,
            ttl_unix_ms,
            json,
        } => {
            let mut replace_client = GrpcMemoryReplaceRpc { client: &mut client };
            let outcome = replace_memory_item(
                &mut replace_client,
                &connection,
                MemoryReplaceOptions {
                    memory_id,
                    content,
                    source,
                    tags: tag,
                    confidence,
                    ttl_unix_ms,
                },
            )
            .await?;
            let replaced_memory_ulid = outcome.replaced_memory_ulid;
            let replacement = outcome.replacement;
            let replacement_id =
                replacement.memory_id.as_ref().map(|value| value.ulid.clone()).unwrap_or_default();

            let payload = json!({
                "replaced_memory_id": replaced_memory_ulid,
                "replacement": memory_item_to_json(&replacement),
                "deleted_original": outcome.deleted_original,
            });
            if output::preferred_json(json) {
                output::print_json_pretty(
                    &payload,
                    "failed to encode memory replace output as JSON",
                )?;
            } else if output::preferred_ndjson(json, false) {
                output::print_json_line(
                    &payload,
                    "failed to encode memory replace output as NDJSON",
                )?;
            } else {
                println!(
                    "memory.replace replaced_id={} replacement_id={} deleted_original={}",
                    replaced_memory_ulid, replacement_id, outcome.deleted_original
                );
            }
        }
        MemoryCommand::Purge { session, channel, principal, json } => {
            if !principal && session.is_none() && channel.is_none() {
                return Err(anyhow!(
                    "memory purge requires one of: --principal, --session, or --channel"
                ));
            }
            let session_id =
                resolve_optional_memory_session_id(session, &connection, "memory purge --session")
                    .await?;
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
            let session_id =
                resolve_optional_memory_session_id(session, &connection, "memory ingest --session")
                    .await?;
            let mut request = Request::new(memory_v1::IngestMemoryRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                source: memory_source_to_proto(source),
                content_text: content,
                channel: channel.unwrap_or_default(),
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
                let mut payload = memory_item_to_json(&item);
                attach_manual_ingest_visibility(&mut payload);
                println!("{}", serde_json::to_string_pretty(&payload)?);
            } else {
                println!(
                    "memory.ingest id={} source={} created_at_ms={} agent_visibility=searchable_recall_default_auto_inject recall_hint=\"palyra memory recall <query> --json\"",
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
                let prefix = prefix.or_else(cli_inferred_workspace_memory_prefix);
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
            let workspace_prefix = workspace_prefix.or_else(cli_inferred_workspace_memory_prefix);
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
            query_option,
            session,
            channel,
            agent_id,
            top_k,
            min_score,
            workspace_prefix,
            json,
        } => {
            let query = resolve_optional_query_arg(query, query_option, "memory search-all")?;
            let min_score =
                parse_float_arg(min_score, "memory search-all --min-score", 0.0, 1.0, Some(0.0))?;
            let workspace_prefix = workspace_prefix.or_else(cli_inferred_workspace_memory_prefix);
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

fn resolve_optional_query_arg(
    positional: Option<String>,
    option: Option<String>,
    command_label: &str,
) -> Result<String> {
    match (positional, option) {
        (Some(positional), None) | (None, Some(positional)) => Ok(positional),
        (None, None) => Err(anyhow!("{command_label} query cannot be empty")),
        (Some(_), Some(_)) => {
            Err(anyhow!("{command_label} accepts either a positional query or --query, not both"))
        }
    }
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
    let auto_inject_enabled =
        payload.pointer("/auto_inject/enabled").and_then(Value::as_bool).unwrap_or(false);
    let auto_inject_max_items =
        payload.pointer("/auto_inject/max_items").and_then(Value::as_u64).unwrap_or(0);
    let auto_inject_min_score =
        payload.pointer("/auto_inject/min_score").and_then(Value::as_f64).unwrap_or(0.0);
    let auto_inject_sources = payload
        .pointer("/auto_inject/sources")
        .and_then(Value::as_array)
        .map(|sources| sources.iter().filter_map(Value::as_str).collect::<Vec<_>>().join(","))
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unknown".to_owned());

    println!(
        "memory.status entries={} approx_bytes={} embeddings_mode={} target_model={} target_dims={} target_version={} indexed={} pending={}",
        entries, approx_bytes, mode, target_model, target_dims, target_version, indexed_count, pending_count
    );
    println!(
        "memory.retention max_entries={} max_bytes={} ttl_days={} vacuum_schedule={}",
        max_entries, max_bytes, ttl_days, vacuum_schedule
    );
    println!(
        "memory.auto_inject enabled={} max_items={} min_score={:.3} sources={}",
        auto_inject_enabled, auto_inject_max_items, auto_inject_min_score, auto_inject_sources
    );
    println!(
        "memory.maintenance interval_ms={} last_run_at_unix_ms={} last_deleted_total={} next_run_at_unix_ms={} next_vacuum_due_at_unix_ms={}",
        interval_ms, last_run_at_ms, last_deleted_total, next_run_at_ms, next_vacuum_due_at_ms
    );
    if let Some(line) = memory_embeddings_degraded_line(payload) {
        println!("{line}");
    }
    if let Some(external_index) = memory_external_index_payload(payload) {
        print_external_index_line("memory.external_index", external_index);
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn memory_embeddings_degraded_line(payload: &Value) -> Option<String> {
    let embeddings = payload.get("embeddings")?;
    let mode = embeddings.get("mode").and_then(Value::as_str).unwrap_or("unknown");
    let production_default_active = embeddings
        .get("production_default_active")
        .and_then(Value::as_bool)
        .unwrap_or(mode == "model_provider");
    let reason_code =
        embeddings.get("degraded_reason_code").and_then(Value::as_str).unwrap_or("none");
    let degraded = !production_default_active || mode == "hash_fallback" || reason_code != "none";
    if !degraded {
        return None;
    }

    let warning = embeddings
        .get("warning")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| memory_embeddings_default_warning(mode, reason_code));
    let remediation = memory_embeddings_remediation(reason_code);
    Some(format!(
        "memory.embeddings.degraded mode={} quality=degraded_hash_fallback production_default_active={} reason_code={} warning={} remediation={}",
        mode,
        production_default_active,
        reason_code,
        quoted_text_field(warning),
        quoted_text_field(remediation)
    ))
}

fn memory_embeddings_default_warning(mode: &str, reason_code: &str) -> &'static str {
    match reason_code {
        "offline_mode_enabled" | "explicit_hash_fallback" => {
            "memory recall is using the explicit offline hash fallback; semantic similarity quality is degraded"
        }
        "embeddings_model_not_configured" => {
            "memory recall is using hash fallback because no embeddings-capable provider or model is configured"
        }
        "embeddings_dimensions_unknown" => {
            "memory recall is using hash fallback because the configured embeddings dimensions are unknown"
        }
        "embeddings_credentials_missing" | "embeddings_provider_missing_credentials" => {
            "memory recall is using hash fallback because the embeddings provider credential reference is missing"
        }
        _ if mode == "hash_fallback" => {
            "memory recall is using hash fallback; semantic similarity quality is degraded"
        }
        _ => "memory embeddings are degraded; semantic recall quality may be reduced",
    }
}

fn memory_embeddings_remediation(reason_code: &str) -> &'static str {
    match reason_code {
        "offline_mode_enabled" | "explicit_hash_fallback" => {
            "disable PALYRA_OFFLINE to use a production embeddings provider, restart the gateway, then run `palyra memory index --until-complete`"
        }
        "embeddings_dimensions_unknown" => {
            "set model_provider.openai_embeddings_dims for the selected embeddings model, restart the gateway, then run `palyra memory index --until-complete`"
        }
        "embeddings_credentials_missing" | "embeddings_provider_missing_credentials" => {
            "store the embeddings provider credential reference, restart the gateway, then run `palyra memory index --until-complete`"
        }
        _ => {
            "configure an embeddings-capable OpenAI-compatible provider or registry embeddings model, select it with `palyra models set-embeddings <model>`, restart the gateway, then run `palyra memory index --until-complete`; until then memory recall uses hash fallback"
        }
    }
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

fn memory_search_output_payload(hits: &[memory_v1::MemorySearchHit]) -> Value {
    json!({
        "memory_store_kind": "durable_memory",
        "hit_count": hits.len(),
        "claim_boundary": memory_search_claim_boundary(hits.len()),
        "hits": hits.iter().map(memory_search_hit_to_json).collect::<Vec<_>>(),
    })
}

fn memory_search_claim_boundary(hit_count: usize) -> &'static str {
    if hit_count == 0 {
        MEMORY_SEARCH_HITS_ABSENT_CLAIM_BOUNDARY
    } else {
        MEMORY_SEARCH_HITS_PRESENT_CLAIM_BOUNDARY
    }
}

fn attach_manual_ingest_visibility(payload: &mut Value) {
    let Some(object) = payload.as_object_mut() else {
        return;
    };
    object.insert(
        "agent_visibility".to_owned(),
        json!({
            "manual_ingest_auto_attached_by_command": false,
            "auto_inject_default_enabled": true,
            "normal_agent_run_context": "manual memory ingest stores searchable memory and default agent runs may attach relevant durable memories automatically; use explicit recall for deterministic preview",
            "recall_for_prompt_preview": "palyra memory recall <query> --json",
        }),
    );
}

#[cfg(test)]
mod tests {
    use super::{
        attach_manual_ingest_visibility, memory_embeddings_degraded_line,
        memory_search_claim_boundary, memory_search_output_payload, memory_session_scope_label,
        replace_memory_item, resolve_optional_query_arg, MemoryReplaceOptions, MemoryReplaceRpc,
    };
    use crate::{common_v1, memory_v1, AgentConnection, CANONICAL_PROTOCOL_MAJOR};
    use serde_json::{json, Value};
    use std::sync::{Arc, Mutex};
    use tonic::{Request, Response, Status};

    const ORIGINAL_MEMORY_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
    const REPLACEMENT_MEMORY_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAW";

    #[derive(Debug, Default)]
    struct MockMemoryState {
        calls: Mutex<Vec<String>>,
        original_present: Mutex<bool>,
        replacement_present: Mutex<bool>,
        reject_ingest: bool,
    }

    impl MockMemoryState {
        fn new(reject_ingest: bool) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                original_present: Mutex::new(true),
                replacement_present: Mutex::new(false),
                reject_ingest,
            }
        }

        fn record_call(&self, call: impl Into<String>) {
            self.calls.lock().expect("mock calls lock should not be poisoned").push(call.into());
        }

        fn calls(&self) -> Vec<String> {
            self.calls.lock().expect("mock calls lock should not be poisoned").clone()
        }

        fn original_present(&self) -> bool {
            *self
                .original_present
                .lock()
                .expect("mock original_present lock should not be poisoned")
        }

        fn set_original_present(&self, present: bool) {
            *self
                .original_present
                .lock()
                .expect("mock original_present lock should not be poisoned") = present;
        }

        fn set_replacement_present(&self, present: bool) {
            *self
                .replacement_present
                .lock()
                .expect("mock replacement_present lock should not be poisoned") = present;
        }
    }

    #[derive(Debug, Clone)]
    struct MockMemoryRpc {
        state: Arc<MockMemoryState>,
    }

    #[async_trait::async_trait]
    impl MemoryReplaceRpc for MockMemoryRpc {
        async fn ingest_memory(
            &mut self,
            request: Request<memory_v1::IngestMemoryRequest>,
        ) -> Result<Response<memory_v1::IngestMemoryResponse>, Status> {
            let payload = request.into_inner();
            self.state.record_call(format!("ingest:{}", payload.content_text));
            if self.state.reject_ingest {
                return Err(Status::invalid_argument("memory content exceeds byte limit (42 > 8)"));
            }
            self.state.set_replacement_present(true);
            Ok(Response::new(memory_v1::IngestMemoryResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                item: Some(mock_memory_item(REPLACEMENT_MEMORY_ID, payload.content_text.as_str())),
            }))
        }

        async fn get_memory_item(
            &mut self,
            request: Request<memory_v1::GetMemoryItemRequest>,
        ) -> Result<Response<memory_v1::GetMemoryItemResponse>, Status> {
            let memory_id =
                request.into_inner().memory_id.map(|value| value.ulid).unwrap_or_default();
            self.state.record_call(format!("get:{memory_id}"));
            if memory_id == ORIGINAL_MEMORY_ID && self.state.original_present() {
                Ok(Response::new(memory_v1::GetMemoryItemResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    item: Some(mock_memory_item(ORIGINAL_MEMORY_ID, "original memory")),
                }))
            } else {
                Err(Status::not_found(format!("memory item not found: {memory_id}")))
            }
        }

        async fn delete_memory_item(
            &mut self,
            request: Request<memory_v1::DeleteMemoryItemRequest>,
        ) -> Result<Response<memory_v1::DeleteMemoryItemResponse>, Status> {
            let memory_id =
                request.into_inner().memory_id.map(|value| value.ulid).unwrap_or_default();
            self.state.record_call(format!("delete:{memory_id}"));
            let deleted = if memory_id == ORIGINAL_MEMORY_ID && self.state.original_present() {
                self.state.set_original_present(false);
                true
            } else if memory_id == REPLACEMENT_MEMORY_ID {
                self.state.set_replacement_present(false);
                true
            } else {
                false
            };
            Ok(Response::new(memory_v1::DeleteMemoryItemResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                deleted,
            }))
        }
    }

    fn test_connection() -> AgentConnection {
        AgentConnection {
            grpc_url: "memory://mock".to_owned(),
            token: None,
            principal: "user:test".to_owned(),
            device_id: "device:test".to_owned(),
            channel: "cli".to_owned(),
            trace_id: "trace:test".to_owned(),
        }
    }

    fn mock_memory_item(memory_id: &str, content_text: &str) -> memory_v1::MemoryItem {
        memory_v1::MemoryItem {
            v: CANONICAL_PROTOCOL_MAJOR,
            memory_id: Some(common_v1::CanonicalId { ulid: memory_id.to_owned() }),
            principal: "user:test".to_owned(),
            channel: "cli".to_owned(),
            session_id: Some(common_v1::CanonicalId {
                ulid: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
            }),
            source: memory_v1::MemorySource::Manual as i32,
            content_text: content_text.to_owned(),
            content_hash: "sha256:test".to_owned(),
            tags: vec!["existing".to_owned()],
            confidence: 0.75,
            ttl_unix_ms: 9_999,
            created_at_unix_ms: 1_000,
            updated_at_unix_ms: 1_000,
        }
    }

    fn memory_replace_options(content: &str) -> MemoryReplaceOptions {
        MemoryReplaceOptions {
            memory_id: ORIGINAL_MEMORY_ID.to_owned(),
            content: content.to_owned(),
            source: None,
            tags: Vec::new(),
            confidence: None,
            ttl_unix_ms: None,
        }
    }

    #[test]
    fn memory_session_scope_label_redacts_identifier_value() {
        assert_eq!(memory_session_scope_label(false), "none");
        assert_eq!(memory_session_scope_label(true), "present");
    }

    #[test]
    fn memory_search_payload_identifies_empty_durable_memory_scope() {
        let payload = memory_search_output_payload(&[]);

        assert_eq!(
            payload.get("memory_store_kind").and_then(Value::as_str),
            Some("durable_memory")
        );
        assert_eq!(payload.get("hit_count").and_then(Value::as_u64), Some(0));
        assert!(
            payload.get("claim_boundary").and_then(Value::as_str).is_some_and(
                |boundary| boundary.contains("does not search prior session transcripts")
            ),
            "{payload}"
        );
    }

    #[test]
    fn memory_search_claim_boundary_distinguishes_hits_from_absence() {
        assert!(memory_search_claim_boundary(0).contains("no durable memory hits"));
        assert!(memory_search_claim_boundary(1).contains("stored memory evidence"));
    }

    #[test]
    fn resolve_optional_query_arg_accepts_positional_or_flagged_query() {
        assert_eq!(
            resolve_optional_query_arg(Some("positional".to_owned()), None, "memory search-all")
                .expect("positional query should resolve"),
            "positional"
        );
        assert_eq!(
            resolve_optional_query_arg(None, Some("flagged".to_owned()), "memory search-all")
                .expect("flagged query should resolve"),
            "flagged"
        );
    }

    #[test]
    fn resolve_optional_query_arg_rejects_missing_or_ambiguous_query() {
        let missing = resolve_optional_query_arg(None, None, "memory search-all")
            .expect_err("missing query should fail");
        assert!(missing.to_string().contains("query cannot be empty"));

        let ambiguous = resolve_optional_query_arg(
            Some("positional".to_owned()),
            Some("flagged".to_owned()),
            "memory search-all",
        )
        .expect_err("ambiguous query should fail");
        assert!(ambiguous.to_string().contains("either a positional query or --query"));
    }

    #[test]
    fn cli_project_memory_prefix_uses_workspace_root_identity() {
        let prefix = super::cli_project_memory_prefix_from_workspace_root(std::path::Path::new(
            "/tmp/Client Portal",
        ))
        .expect("workspace root should produce a project prefix");

        assert!(prefix.starts_with("projects/project-client-portal-"), "{prefix}");
        assert_eq!(prefix.len(), "projects/project-client-portal-".len() + 10);
    }

    #[test]
    fn manual_ingest_visibility_makes_agent_boundary_explicit() {
        let mut payload = json!({ "memory_id": "01ARZ3NDEKTSV4RRFFQ69G5FAW" });
        attach_manual_ingest_visibility(&mut payload);

        assert_eq!(
            payload.pointer("/agent_visibility/manual_ingest_auto_attached_by_command"),
            Some(&json!(false))
        );
        assert_eq!(
            payload.pointer("/agent_visibility/auto_inject_default_enabled"),
            Some(&json!(true))
        );
        assert_eq!(
            payload.pointer("/agent_visibility/recall_for_prompt_preview"),
            Some(&json!("palyra memory recall <query> --json"))
        );
    }

    #[test]
    fn memory_status_explains_hash_fallback_quality_and_recovery() {
        let payload = json!({
            "embeddings": {
                "mode": "hash_fallback",
                "production_default_active": false,
                "degraded_reason_code": "embeddings_model_not_configured",
                "warning": "retrieval embeddings defaulted to hash fallback because no embeddings-capable provider or model is configured"
            }
        });

        let line = memory_embeddings_degraded_line(&payload)
            .expect("hash fallback status should produce a diagnostic line");

        assert!(line.contains("quality=degraded_hash_fallback"), "{line}");
        assert!(line.contains("reason_code=embeddings_model_not_configured"), "{line}");
        assert!(line.contains("palyra models set-embeddings <model>"), "{line}");
        assert!(line.contains("palyra memory index --until-complete"), "{line}");
    }

    #[test]
    fn memory_status_omits_degraded_line_for_production_embeddings() {
        let payload = json!({
            "embeddings": {
                "mode": "model_provider",
                "production_default_active": true
            }
        });

        assert_eq!(memory_embeddings_degraded_line(&payload), None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn memory_replace_failed_ingest_preserves_original_item() -> anyhow::Result<()> {
        let state = Arc::new(MockMemoryState::new(true));
        let mut client = MockMemoryRpc { state: Arc::clone(&state) };

        let result = replace_memory_item(
            &mut client,
            &test_connection(),
            memory_replace_options("oversized replacement"),
        )
        .await;

        let error = result.expect_err("replacement ingest failure should be reported");
        assert!(
            error.to_string().contains("failed to call memory IngestMemory for replacement"),
            "{error:?}"
        );
        assert_eq!(
            state.calls(),
            [format!("get:{ORIGINAL_MEMORY_ID}"), "ingest:oversized replacement".to_owned(),],
            "replace must not delete the original until replacement ingest succeeds"
        );
        assert!(
            state.original_present(),
            "failed replacement must leave the original item present"
        );
        Ok(())
    }
}
