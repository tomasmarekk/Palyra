use crate::gateway::current_unix_ms;
use crate::gateway::ListOrchestratorSessionsRequest;
use crate::journal::MemoryRetentionPolicy;
use crate::*;
use crate::{
    application::provider_input::render_memory_augmented_prompt,
    domain::workspace::{curated_workspace_roots, curated_workspace_templates},
};

pub(crate) async fn console_memory_status_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let maintenance_status =
        state.runtime.memory_maintenance_status().await.map_err(runtime_status_response)?;
    let embeddings_status =
        state.runtime.memory_embeddings_status().await.map_err(runtime_status_response)?;
    let memory_config = state.runtime.memory_config_snapshot();
    let workspace_preview = state
        .runtime
        .list_workspace_documents(journal::WorkspaceDocumentListFilter {
            principal: _session.context.principal.clone(),
            channel: _session.context.channel.clone(),
            agent_id: None,
            prefix: None,
            include_deleted: false,
            limit: 8,
        })
        .await
        .map_err(runtime_status_response)?;
    let derived = state.channels.derived_stats().map_err(channel_platform_error_response)?;
    let maintenance_interval_ms =
        i64::try_from(MEMORY_MAINTENANCE_INTERVAL.as_millis()).unwrap_or(i64::MAX);
    Ok(Json(json!({
        "usage": maintenance_status.usage,
        "embeddings": embeddings_status,
        "retention": {
            "max_entries": memory_config.retention_max_entries,
            "max_bytes": memory_config.retention_max_bytes,
            "ttl_days": memory_config.retention_ttl_days,
            "vacuum_schedule": memory_config.retention_vacuum_schedule,
        },
        "maintenance": {
            "interval_ms": maintenance_interval_ms,
            "last_run": maintenance_status.last_run,
            "last_vacuum_at_unix_ms": maintenance_status.last_vacuum_at_unix_ms,
            "next_vacuum_due_at_unix_ms": maintenance_status.next_vacuum_due_at_unix_ms,
            "next_run_at_unix_ms": maintenance_status.next_maintenance_run_at_unix_ms,
        },
        "workspace": {
            "roots": curated_workspace_roots(),
            "curated_paths": curated_workspace_templates()
                .into_iter()
                .map(|template| template.path)
                .collect::<Vec<_>>(),
            "recent_documents": workspace_preview,
        },
        "derived": derived,
    })))
}

pub(crate) async fn console_memory_derived_artifacts_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleMemoryDerivedArtifactsQuery>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let workspace_document_id = query.workspace_document_id.and_then(trim_to_option);
    let memory_item_id = query.memory_item_id.and_then(trim_to_option);
    if workspace_document_id.is_none() && memory_item_id.is_none() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "workspace_document_id or memory_item_id must be provided",
        )));
    }
    let derived_artifacts = state
        .channels
        .list_linked_derived_artifacts(
            workspace_document_id.as_deref(),
            memory_item_id.as_deref(),
            query.limit.unwrap_or(24).clamp(1, 128),
        )
        .map_err(channel_platform_error_response)?
        .into_iter()
        .filter(|record| record.principal.as_deref() == Some(session.context.principal.as_str()))
        .filter(|record| record.channel.as_deref() == session.context.channel.as_deref())
        .collect::<Vec<_>>();
    Ok(Json(json!({
        "workspace_document_id": workspace_document_id,
        "memory_item_id": memory_item_id,
        "derived_artifacts": derived_artifacts,
        "contract": contract_descriptor(),
    })))
}

pub(crate) async fn console_memory_index_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleMemoryIndexRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let batch_size = payload.batch_size.unwrap_or(64).clamp(1, 256);
    let until_complete = payload.until_complete.unwrap_or(false);
    let run_maintenance = payload.run_maintenance.unwrap_or(false);

    let maintenance =
        if run_maintenance { Some(run_memory_maintenance_now(&state).await?) } else { None };

    let mut outcome = state
        .runtime
        .run_memory_embeddings_backfill(batch_size)
        .await
        .map_err(runtime_status_response)?;
    let mut batches_executed = 1_u64;
    let mut scanned_count = outcome.scanned_count;
    let mut updated_count = outcome.updated_count;
    while until_complete && !outcome.is_complete() {
        outcome = state
            .runtime
            .run_memory_embeddings_backfill(batch_size)
            .await
            .map_err(runtime_status_response)?;
        scanned_count = scanned_count.saturating_add(outcome.scanned_count);
        updated_count = updated_count.saturating_add(outcome.updated_count);
        batches_executed = batches_executed.saturating_add(1);
    }
    let embeddings_status =
        state.runtime.memory_embeddings_status().await.map_err(runtime_status_response)?;
    let maintenance_payload = maintenance.as_ref().map(|outcome| {
        json!({
            "ran_at_unix_ms": outcome.ran_at_unix_ms,
            "deleted_expired_count": outcome.deleted_expired_count,
            "deleted_capacity_count": outcome.deleted_capacity_count,
            "deleted_total_count": outcome.deleted_total_count,
            "entries_before": outcome.entries_before,
            "entries_after": outcome.entries_after,
            "approx_bytes_before": outcome.approx_bytes_before,
            "approx_bytes_after": outcome.approx_bytes_after,
            "vacuum_performed": outcome.vacuum_performed,
            "last_vacuum_at_unix_ms": outcome.last_vacuum_at_unix_ms,
            "next_vacuum_due_at_unix_ms": outcome.next_vacuum_due_at_unix_ms,
            "next_maintenance_run_at_unix_ms": outcome.next_maintenance_run_at_unix_ms,
        })
    });
    let index_payload = json!({
        "ran_at_unix_ms": outcome.ran_at_unix_ms,
        "batch_size": outcome.batch_size,
        "batches_executed": batches_executed,
        "scanned_count": scanned_count,
        "updated_count": updated_count,
        "pending_count": outcome.pending_count,
        "complete": outcome.is_complete(),
        "target_model_id": outcome.target_model_id,
        "target_dims": outcome.target_dims,
        "target_version": outcome.target_version,
        "until_complete": until_complete,
    });
    let event_details = json!({
        "batch_size": batch_size,
        "until_complete": until_complete,
        "run_maintenance": run_maintenance,
        "index": index_payload.clone(),
        "maintenance": maintenance_payload.clone(),
    });
    if let Err(error) = state
        .runtime
        .record_console_event(&session.context, "memory.index.run", event_details)
        .await
    {
        warn!(error = %error, "failed to record memory index console event");
    }

    Ok(Json(json!({
        "maintenance": maintenance_payload,
        "index": index_payload,
        "embeddings": embeddings_status,
    })))
}

pub(crate) async fn console_memory_search_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleMemorySearchQuery>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let search_query = query.query.trim();
    if search_query.is_empty() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "query cannot be empty",
        )));
    }
    let min_score = query.min_score.unwrap_or(0.0);
    if !min_score.is_finite() || !(0.0..=1.0).contains(&min_score) {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "min_score must be in range 0.0..=1.0",
        )));
    }
    let session_scope = query.session_id.clone().and_then(trim_to_option);
    if let Some(session_scope) = session_scope.as_deref() {
        validate_canonical_id(session_scope).map_err(|_| {
            runtime_status_response(tonic::Status::invalid_argument(
                "session_id must be a canonical ULID",
            ))
        })?;
    }

    let sources = parse_memory_sources_csv(query.sources_csv.as_deref())?;
    let hits = state
        .runtime
        .search_memory(journal::MemorySearchRequest {
            principal: session.context.principal,
            channel: query.channel.or(session.context.channel),
            session_id: session_scope,
            query: search_query.to_owned(),
            top_k: query.top_k.unwrap_or(8).clamp(1, 50),
            min_score,
            tags: parse_csv_values(query.tags_csv.as_deref()),
            sources,
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({ "hits": hits })))
}

pub(crate) async fn console_memory_purge_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleMemoryPurgeRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let session_scope = payload.session_id.clone().and_then(trim_to_option);
    if let Some(session_scope) = session_scope.as_deref() {
        validate_canonical_id(session_scope).map_err(|_| {
            runtime_status_response(tonic::Status::invalid_argument(
                "session_id must be a canonical ULID",
            ))
        })?;
    }
    let purge_all_principal = payload.purge_all_principal.unwrap_or(false);
    if !purge_all_principal
        && payload.channel.as_deref().is_none_or(|value| value.trim().is_empty())
        && session_scope.is_none()
    {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "purge request requires purge_all_principal=true or channel/session scope",
        )));
    }

    let deleted_count = state
        .runtime
        .purge_memory(MemoryPurgeRequest {
            principal: session.context.principal,
            channel: payload.channel,
            session_id: session_scope,
            purge_all_principal,
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({ "deleted_count": deleted_count })))
}

pub(crate) async fn console_workspace_documents_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleWorkspaceDocumentsQuery>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let records = state
        .runtime
        .list_workspace_documents(journal::WorkspaceDocumentListFilter {
            principal: session.context.principal.clone(),
            channel: query.channel.or(session.context.channel),
            agent_id: query.agent_id.and_then(trim_to_option),
            prefix: query.prefix.and_then(trim_to_option).or(query.path.and_then(trim_to_option)),
            include_deleted: query.include_deleted.unwrap_or(false),
            limit: query.limit.unwrap_or(32).clamp(1, 128),
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "documents": records,
        "roots": curated_workspace_roots(),
        "contract": contract_descriptor(),
    })))
}

pub(crate) async fn console_workspace_document_get_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleWorkspaceDocumentQuery>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let path = trim_to_option(query.path).ok_or_else(|| {
        runtime_status_response(tonic::Status::invalid_argument("path cannot be empty"))
    })?;
    let record = state
        .runtime
        .workspace_document_by_path(
            session.context.principal.clone(),
            query.channel.or(session.context.channel),
            query.agent_id.and_then(trim_to_option),
            path.clone(),
            query.include_deleted.unwrap_or(false),
        )
        .await
        .map_err(runtime_status_response)?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found(format!(
                "workspace document not found: {path}"
            )))
        })?;
    Ok(Json(json!({
        "document": record,
        "contract": contract_descriptor(),
    })))
}

pub(crate) async fn console_workspace_document_write_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleWorkspaceDocumentWriteRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let path = trim_to_option(payload.path).ok_or_else(|| {
        runtime_status_response(tonic::Status::invalid_argument("path cannot be empty"))
    })?;
    let content_text = trim_to_option(payload.content_text).ok_or_else(|| {
        runtime_status_response(tonic::Status::invalid_argument("content_text cannot be empty"))
    })?;
    let session_id = payload.session_id.and_then(trim_to_option);
    if let Some(session_id) = session_id.as_deref() {
        validate_canonical_id(session_id).map_err(|_| {
            runtime_status_response(tonic::Status::invalid_argument(
                "session_id must be a canonical ULID",
            ))
        })?;
    }
    let document = state
        .runtime
        .upsert_workspace_document(journal::WorkspaceDocumentWriteRequest {
            document_id: payload.document_id.and_then(trim_to_option),
            principal: session.context.principal.clone(),
            channel: payload.channel.or(session.context.channel),
            agent_id: payload.agent_id.and_then(trim_to_option),
            session_id,
            path,
            title: payload.title.and_then(trim_to_option),
            content_text,
            template_id: payload.template_id.and_then(trim_to_option),
            template_version: payload.template_version,
            template_content_hash: payload.template_content_hash.and_then(trim_to_option),
            source_memory_id: payload.source_memory_id.and_then(trim_to_option),
            manual_override: payload.manual_override.unwrap_or(false),
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "document": document,
        "contract": contract_descriptor(),
    })))
}

pub(crate) async fn console_workspace_document_move_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleWorkspaceDocumentMoveRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let path = trim_to_option(payload.path).ok_or_else(|| {
        runtime_status_response(tonic::Status::invalid_argument("path cannot be empty"))
    })?;
    let next_path = trim_to_option(payload.next_path).ok_or_else(|| {
        runtime_status_response(tonic::Status::invalid_argument("next_path cannot be empty"))
    })?;
    let session_id = payload.session_id.and_then(trim_to_option);
    if let Some(session_id) = session_id.as_deref() {
        validate_canonical_id(session_id).map_err(|_| {
            runtime_status_response(tonic::Status::invalid_argument(
                "session_id must be a canonical ULID",
            ))
        })?;
    }
    let document = state
        .runtime
        .move_workspace_document(journal::WorkspaceDocumentMoveRequest {
            principal: session.context.principal.clone(),
            channel: payload.channel.or(session.context.channel),
            agent_id: payload.agent_id.and_then(trim_to_option),
            session_id,
            path,
            next_path,
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "document": document,
        "contract": contract_descriptor(),
    })))
}

pub(crate) async fn console_workspace_document_delete_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleWorkspaceDocumentDeleteRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let path = trim_to_option(payload.path).ok_or_else(|| {
        runtime_status_response(tonic::Status::invalid_argument("path cannot be empty"))
    })?;
    let session_id = payload.session_id.and_then(trim_to_option);
    if let Some(session_id) = session_id.as_deref() {
        validate_canonical_id(session_id).map_err(|_| {
            runtime_status_response(tonic::Status::invalid_argument(
                "session_id must be a canonical ULID",
            ))
        })?;
    }
    let document = state
        .runtime
        .soft_delete_workspace_document(journal::WorkspaceDocumentDeleteRequest {
            principal: session.context.principal.clone(),
            channel: payload.channel.or(session.context.channel),
            agent_id: payload.agent_id.and_then(trim_to_option),
            session_id,
            path,
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "document": document,
        "contract": contract_descriptor(),
    })))
}

pub(crate) async fn console_workspace_document_versions_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleWorkspaceDocumentVersionsQuery>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let path = trim_to_option(query.path).ok_or_else(|| {
        runtime_status_response(tonic::Status::invalid_argument("path cannot be empty"))
    })?;
    let document = state
        .runtime
        .workspace_document_by_path(
            session.context.principal.clone(),
            query.channel.or(session.context.channel.clone()),
            query.agent_id.and_then(trim_to_option),
            path.clone(),
            true,
        )
        .await
        .map_err(runtime_status_response)?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found(format!(
                "workspace document not found: {path}"
            )))
        })?;
    let versions = state
        .runtime
        .list_workspace_document_versions(
            document.document_id.clone(),
            query.limit.unwrap_or(20).clamp(1, 100),
        )
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "document": document,
        "versions": versions,
        "contract": contract_descriptor(),
    })))
}

pub(crate) async fn console_workspace_document_pin_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleWorkspaceDocumentPinRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let path = trim_to_option(payload.path).ok_or_else(|| {
        runtime_status_response(tonic::Status::invalid_argument("path cannot be empty"))
    })?;
    let document = state
        .runtime
        .set_workspace_document_pinned(
            session.context.principal.clone(),
            payload.channel.or(session.context.channel),
            payload.agent_id.and_then(trim_to_option),
            path.clone(),
            payload.pinned,
        )
        .await
        .map_err(runtime_status_response)?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found(format!(
                "workspace document not found: {path}"
            )))
        })?;
    Ok(Json(json!({
        "document": document,
        "contract": contract_descriptor(),
    })))
}

pub(crate) async fn console_workspace_bootstrap_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleWorkspaceBootstrapRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let session_id = payload.session_id.and_then(trim_to_option);
    if let Some(session_id) = session_id.as_deref() {
        validate_canonical_id(session_id).map_err(|_| {
            runtime_status_response(tonic::Status::invalid_argument(
                "session_id must be a canonical ULID",
            ))
        })?;
    }
    let outcome = state
        .runtime
        .bootstrap_workspace(journal::WorkspaceBootstrapRequest {
            principal: session.context.principal.clone(),
            channel: payload.channel.or(session.context.channel),
            agent_id: payload.agent_id.and_then(trim_to_option),
            session_id,
            force_repair: payload.force_repair.unwrap_or(false),
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "bootstrap": outcome,
        "roots": curated_workspace_roots(),
        "contract": contract_descriptor(),
    })))
}

pub(crate) async fn console_workspace_search_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleWorkspaceSearchQuery>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let search_query = query.query.trim();
    if search_query.is_empty() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "query cannot be empty",
        )));
    }
    let min_score = query.min_score.unwrap_or(0.0);
    if !min_score.is_finite() || !(0.0..=1.0).contains(&min_score) {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "min_score must be in range 0.0..=1.0",
        )));
    }
    let hits = state
        .runtime
        .search_workspace_documents(journal::WorkspaceSearchRequest {
            principal: session.context.principal.clone(),
            channel: query.channel.or(session.context.channel),
            agent_id: query.agent_id.and_then(trim_to_option),
            query: search_query.to_owned(),
            prefix: query.prefix.and_then(trim_to_option),
            top_k: query.top_k.unwrap_or(8).clamp(1, 32),
            min_score,
            include_historical: query.include_historical.unwrap_or(false),
            include_quarantined: query.include_quarantined.unwrap_or(false),
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "hits": hits,
        "contract": contract_descriptor(),
    })))
}

pub(crate) async fn console_recall_preview_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleRecallPreviewRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let query_text = payload.query.trim();
    if query_text.is_empty() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "query cannot be empty",
        )));
    }
    let min_score = payload.min_score.unwrap_or(0.0);
    if !min_score.is_finite() || !(0.0..=1.0).contains(&min_score) {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "min_score must be in range 0.0..=1.0",
        )));
    }
    let session_scope = payload.session_id.and_then(trim_to_option);
    if let Some(session_scope) = session_scope.as_deref() {
        validate_canonical_id(session_scope).map_err(|_| {
            runtime_status_response(tonic::Status::invalid_argument(
                "session_id must be a canonical ULID",
            ))
        })?;
    }
    let recall_channel = payload.channel.clone().or(session.context.channel.clone());
    let recall_agent_id = payload.agent_id.clone().and_then(trim_to_option);
    let workspace_prefix = payload.workspace_prefix.clone().and_then(trim_to_option);
    let memory_hits = if payload.memory_top_k.unwrap_or(4) == 0 {
        Vec::new()
    } else {
        state
            .runtime
            .search_memory(journal::MemorySearchRequest {
                principal: session.context.principal.clone(),
                channel: recall_channel.clone(),
                session_id: session_scope.clone(),
                query: query_text.to_owned(),
                top_k: payload.memory_top_k.unwrap_or(4).clamp(1, 16),
                min_score,
                tags: Vec::new(),
                sources: Vec::new(),
            })
            .await
            .map_err(runtime_status_response)?
    };
    let workspace_hits = if payload.workspace_top_k.unwrap_or(4) == 0 {
        Vec::new()
    } else {
        state
            .runtime
            .search_workspace_documents(journal::WorkspaceSearchRequest {
                principal: session.context.principal.clone(),
                channel: recall_channel.clone(),
                agent_id: recall_agent_id.clone(),
                query: query_text.to_owned(),
                prefix: workspace_prefix.clone(),
                top_k: payload.workspace_top_k.unwrap_or(4).clamp(1, 16),
                min_score,
                include_historical: payload.include_workspace_historical.unwrap_or(false),
                include_quarantined: payload.include_workspace_quarantined.unwrap_or(false),
            })
            .await
            .map_err(runtime_status_response)?
    };
    let parameter_delta = json!({
        "explicit_recall": {
            "query": query_text,
            "channel": recall_channel,
            "session_id": session_scope,
            "agent_id": recall_agent_id,
            "min_score": min_score,
            "workspace_prefix": workspace_prefix,
            "include_workspace_historical": payload.include_workspace_historical.unwrap_or(false),
            "include_workspace_quarantined": payload.include_workspace_quarantined.unwrap_or(false),
            "memory_item_ids": memory_hits
                .iter()
                .map(|hit| hit.item.memory_id.clone())
                .collect::<Vec<_>>(),
            "workspace_document_ids": workspace_hits
                .iter()
                .map(|hit| hit.document.document_id.clone())
                .collect::<Vec<_>>(),
        }
    });
    Ok(Json(json!({
        "query": query_text,
        "memory_hits": memory_hits,
        "workspace_hits": workspace_hits,
        "parameter_delta": parameter_delta,
        "prompt_preview": render_recall_preview_prompt(
            query_text,
            memory_hits.as_slice(),
            workspace_hits.as_slice(),
        ),
        "contract": contract_descriptor(),
    })))
}

pub(crate) async fn console_search_all_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleSearchAllQuery>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let search_query = query.q.trim();
    if search_query.is_empty() {
        return Err(runtime_status_response(tonic::Status::invalid_argument("q cannot be empty")));
    }
    let min_score = query.min_score.unwrap_or(0.0);
    if !min_score.is_finite() || !(0.0..=1.0).contains(&min_score) {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "min_score must be in range 0.0..=1.0",
        )));
    }
    let session_scope = query.session_id.and_then(trim_to_option);
    if let Some(session_scope) = session_scope.as_deref() {
        validate_canonical_id(session_scope).map_err(|_| {
            runtime_status_response(tonic::Status::invalid_argument(
                "session_id must be a canonical ULID",
            ))
        })?;
    }
    let top_k = query.top_k.unwrap_or(8).clamp(1, 24);
    let channel = query.channel.or(session.context.channel.clone());
    let memory_hits = state
        .runtime
        .search_memory(journal::MemorySearchRequest {
            principal: session.context.principal.clone(),
            channel: channel.clone(),
            session_id: session_scope.clone(),
            query: search_query.to_owned(),
            top_k,
            min_score,
            tags: Vec::new(),
            sources: Vec::new(),
        })
        .await
        .map_err(runtime_status_response)?;
    let workspace_hits = state
        .runtime
        .search_workspace_documents(journal::WorkspaceSearchRequest {
            principal: session.context.principal.clone(),
            channel: channel.clone(),
            agent_id: query.agent_id.and_then(trim_to_option),
            query: search_query.to_owned(),
            prefix: query.workspace_prefix.and_then(trim_to_option),
            top_k,
            min_score,
            include_historical: false,
            include_quarantined: false,
        })
        .await
        .map_err(runtime_status_response)?;
    let sessions = state
        .runtime
        .list_orchestrator_sessions(ListOrchestratorSessionsRequest {
            after_session_key: None,
            principal: session.context.principal.clone(),
            device_id: session.context.device_id.clone(),
            channel,
            include_archived: false,
            requested_limit: Some(top_k),
            search_query: Some(search_query.to_owned()),
        })
        .await
        .map_err(runtime_status_response)?;
    let session_count = sessions.0.len();
    let session_hits = sessions
        .0
        .into_iter()
        .map(|record| {
            json!({
                "source_type": "session",
                "session_id": record.session_id,
                "title": record.title,
                "preview": record.preview,
                "updated_at_unix_ms": record.updated_at_unix_ms,
                "match_snippet": record.match_snippet,
                "last_run_state": record.last_run_state,
            })
        })
        .collect::<Vec<_>>();
    Ok(Json(json!({
        "query": search_query,
        "groups": {
            "sessions": session_hits,
            "workspace": workspace_hits,
            "memory": memory_hits,
        },
        "counts": {
            "sessions": session_count,
            "workspace": workspace_hits.len(),
            "memory": memory_hits.len(),
        },
        "contract": contract_descriptor(),
    })))
}

#[allow(clippy::result_large_err)]
async fn run_memory_maintenance_now(
    state: &AppState,
) -> Result<crate::journal::MemoryMaintenanceOutcome, Response> {
    let now_unix_ms = current_unix_ms();
    let maintenance_status =
        state.runtime.memory_maintenance_status().await.map_err(runtime_status_response)?;
    let memory_config = state.runtime.memory_config_snapshot();
    state
        .runtime
        .run_memory_maintenance(
            now_unix_ms,
            MemoryRetentionPolicy {
                max_entries: memory_config.retention_max_entries,
                max_bytes: memory_config.retention_max_bytes,
                ttl_days: memory_config.retention_ttl_days,
            },
            maintenance_status.next_vacuum_due_at_unix_ms,
            Some(now_unix_ms.saturating_add(
                i64::try_from(MEMORY_MAINTENANCE_INTERVAL.as_millis()).unwrap_or(i64::MAX),
            )),
        )
        .await
        .map_err(runtime_status_response)
}

fn render_recall_preview_prompt(
    query: &str,
    memory_hits: &[journal::MemorySearchHit],
    workspace_hits: &[journal::WorkspaceSearchHit],
) -> String {
    let mut sections = Vec::new();
    if !memory_hits.is_empty() {
        sections.push(render_memory_augmented_prompt(memory_hits, query));
    }
    if !workspace_hits.is_empty() {
        let mut block = String::from("<workspace_recall>\n");
        for (index, hit) in workspace_hits.iter().enumerate() {
            let snippet = preview_text(hit.snippet.as_str(), 220);
            block.push_str(
                format!(
                    "{}. document_id={} path={} version={} reason={} risk_state={} snippet={}\n",
                    index + 1,
                    hit.document.document_id,
                    hit.document.path,
                    hit.version,
                    hit.reason,
                    hit.document.risk_state,
                    snippet
                )
                .as_str(),
            );
        }
        block.push_str("</workspace_recall>\n");
        block.push_str(query);
        sections.push(block);
    }
    if sections.is_empty() {
        return query.to_owned();
    }
    sections.join("\n\n")
}

fn preview_text(raw: &str, max_chars: usize) -> String {
    let normalized = raw.replace(['\r', '\n'], " ");
    let trimmed = normalized.split_whitespace().collect::<Vec<_>>().join(" ");
    if trimmed.chars().count() <= max_chars {
        return trimmed;
    }
    let mut truncated = trimmed.chars().take(max_chars).collect::<String>();
    truncated.push_str("...");
    truncated
}
