use crate::*;

pub(crate) fn run_memory(command: MemoryCommand) -> Result<()> {
    let root_context = app::current_root_context()
        .ok_or_else(|| anyhow!("CLI root context is unavailable for memory command"))?;
    let connection = root_context
        .resolve_grpc_connection(app::ConnectionOverrides::default(), app::ConnectionDefaults::USER)?;
    let runtime = build_runtime()?;
    runtime.block_on(run_memory_async(command, connection))
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
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "hits": response.hits.iter().map(memory_search_hit_to_json).collect::<Vec<_>>(),
                    }))
                    .context("failed to serialize JSON output")?
                );
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
    }

    std::io::stdout().flush().context("stdout flush failed")
}
