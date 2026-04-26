use crate::gateway::current_unix_ms;
use crate::gateway::ListOrchestratorSessionsRequest;
use crate::journal::{
    MemoryRetentionPolicy, RecallArtifactCreateRequest, RecallArtifactListFilter,
    SessionSearchOutcome, SessionSearchRequest, RECALL_ARTIFACT_KIND_PREVIEW,
    RECALL_ARTIFACT_KIND_SESSION_SEARCH,
};
use crate::*;
use crate::{
    application::learning::{apply_patch_learning_candidate, apply_preference_candidate},
    application::recall::{
        preview_recall, recall_preview_console_payload, RecallPreviewEnvelope, RecallRequest,
    },
    domain::workspace::{curated_workspace_roots, curated_workspace_templates},
};
use palyra_common::runtime_preview::{
    RuntimeDecisionActorKind, RuntimeDecisionEventType, RuntimeDecisionPayload,
    RuntimeDecisionTiming, RuntimeEntityRef, RuntimePreviewCapability, RuntimeResourceBudget,
};
use ulid::Ulid;

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
    let retrieval_config = state.runtime.retrieval_config_snapshot();
    let retrieval_backend =
        state.runtime.retrieval_backend_snapshot().map_err(runtime_status_response)?;
    let learning_config = state.runtime.learning_config_snapshot();
    let counters = state.runtime.counters.snapshot();
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
    let latest_recall_artifacts = state
        .runtime
        .list_recall_artifacts(RecallArtifactListFilter {
            principal: _session.context.principal.clone(),
            device_id: _session.context.device_id.clone(),
            channel: _session.context.channel.clone(),
            session_id: None,
            artifact_kind: None,
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
        "retrieval": {
            "backend": retrieval_backend,
            "external_index": retrieval_backend.external_index.clone(),
            "scoring": retrieval_config.scoring,
        },
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
        "learning": {
            "enabled": learning_config.enabled,
            "sampling_percent": learning_config.sampling_percent,
            "cooldown_ms": learning_config.cooldown_ms,
            "budget_tokens": learning_config.budget_tokens,
            "max_candidates_per_run": learning_config.max_candidates_per_run,
            "durable_fact_review_min_confidence_bps": learning_config.durable_fact_review_min_confidence_bps,
            "durable_fact_auto_write_threshold_bps": learning_config.durable_fact_auto_write_threshold_bps,
            "preference_review_min_confidence_bps": learning_config.preference_review_min_confidence_bps,
            "procedure_min_occurrences": learning_config.procedure_min_occurrences,
            "procedure_review_min_confidence_bps": learning_config.procedure_review_min_confidence_bps,
            "thresholds": {
                "durable_fact": {
                    "review_min_confidence_bps": learning_config.durable_fact_review_min_confidence_bps,
                    "auto_apply_confidence_bps": learning_config.durable_fact_auto_write_threshold_bps,
                },
                "preference": {
                    "review_min_confidence_bps": learning_config.preference_review_min_confidence_bps,
                },
                "procedure": {
                    "review_min_confidence_bps": learning_config.procedure_review_min_confidence_bps,
                    "min_occurrences": learning_config.procedure_min_occurrences,
                }
            },
            "counters": {
                "reflections_scheduled": counters.learning_reflections_scheduled,
                "reflections_completed": counters.learning_reflections_completed,
                "candidates_created": counters.learning_candidates_created,
                "candidates_auto_applied": counters.learning_candidates_auto_applied,
            },
        },
        "workspace": {
            "roots": curated_workspace_roots(),
            "curated_paths": curated_workspace_templates()
                .into_iter()
                .map(|template| template.path)
                .collect::<Vec<_>>(),
            "recent_documents": workspace_preview,
        },
        "recall_artifacts": {
            "latest": latest_recall_artifacts,
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
    let mut external_indexer = state
        .runtime
        .run_external_retrieval_indexer(batch_size)
        .await
        .map_err(runtime_status_response)?;
    while until_complete && !external_indexer.complete {
        external_indexer = state
            .runtime
            .run_external_retrieval_indexer(batch_size)
            .await
            .map_err(runtime_status_response)?;
    }
    let drift =
        state.runtime.external_retrieval_drift_report().await.map_err(runtime_status_response)?;
    let retrieval_backend =
        state.runtime.retrieval_backend_snapshot().map_err(runtime_status_response)?;
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
        "external_indexer": external_indexer.clone(),
        "drift": drift.clone(),
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
        "external_indexer": external_indexer,
        "drift": drift,
        "external_index": retrieval_backend.external_index.clone(),
        "retrieval": {
            "backend": retrieval_backend,
        },
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
    let outcome = state
        .runtime
        .search_memory_with_diagnostics(journal::MemorySearchRequest {
            principal: session.context.principal,
            channel: query.channel.or(session.context.channel),
            session_id: session_scope.clone(),
            query: search_query.to_owned(),
            top_k: query.top_k.unwrap_or(8).clamp(1, 50),
            min_score,
            tags: parse_csv_values(query.tags_csv.as_deref()),
            sources,
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "hits": outcome.hits,
        "diagnostics": outcome.diagnostics,
    })))
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

pub(crate) async fn console_learning_candidates_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleLearningCandidatesQuery>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let candidates = state
        .runtime
        .list_learning_candidates(journal::LearningCandidateListFilter {
            candidate_id: query.candidate_id.and_then(trim_to_option),
            owner_principal: Some(session.context.principal.clone()),
            device_id: None,
            channel: session.context.channel.clone(),
            session_id: query.session_id.and_then(trim_to_option),
            scope_kind: query.scope_kind.and_then(trim_to_option),
            scope_id: query.scope_id.and_then(trim_to_option),
            candidate_kind: query.candidate_kind.and_then(trim_to_option),
            status: query.status.and_then(trim_to_option),
            risk_level: query.risk_level.and_then(trim_to_option),
            source_task_id: query.source_task_id.and_then(trim_to_option),
            min_confidence: query.min_confidence,
            max_confidence: query.max_confidence,
            limit: query.limit.unwrap_or(64).clamp(1, 256),
        })
        .await
        .map_err(runtime_status_response)?;
    let lifecycle = learning_candidates_lifecycle_summary(candidates.as_slice());
    Ok(Json(json!({
        "candidates": candidates,
        "lifecycle": lifecycle,
        "contract": contract_descriptor(),
    })))
}

pub(crate) async fn console_learning_candidate_history_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(candidate_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let candidate =
        load_console_learning_candidate(&state, &session.context, candidate_id.as_str()).await?;
    let history = state
        .runtime
        .learning_candidate_history(candidate.candidate_id.clone())
        .await
        .map_err(runtime_status_response)?;
    let lifecycle = learning_candidate_lifecycle(&candidate, history.as_slice());
    Ok(Json(json!({
        "candidate": candidate,
        "history": history,
        "lifecycle": lifecycle,
        "contract": contract_descriptor(),
    })))
}

pub(crate) async fn console_learning_candidate_review_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(candidate_id): Path<String>,
    Json(payload): Json<ConsoleLearningCandidateReviewRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let candidate =
        load_console_learning_candidate(&state, &session.context, candidate_id.as_str()).await?;
    let status = normalize_learning_candidate_review_status(payload.status.as_str())
        .map_err(runtime_status_response)?;
    let reviewed = state
        .runtime
        .review_learning_candidate(journal::LearningCandidateReviewRequest {
            candidate_id: candidate.candidate_id.clone(),
            status,
            reviewed_by_principal: session.context.principal.clone(),
            action_summary: payload.action_summary.and_then(trim_to_option),
            action_payload_json: payload.action_payload_json.and_then(trim_to_option),
        })
        .await
        .map_err(runtime_status_response)?;
    let applied_preference = if payload.apply_preference.unwrap_or(false)
        || (reviewed.candidate_kind == "preference"
            && matches!(reviewed.status.as_str(), "accepted" | "approved" | "deployed"))
    {
        apply_preference_candidate(&state.runtime, &reviewed, session.context.principal.as_str())
            .await
            .map_err(runtime_status_response)?
    } else {
        None
    };
    let lifecycle = learning_candidate_lifecycle(&reviewed, &[]);
    Ok(Json(json!({
        "candidate": reviewed,
        "applied_preference": applied_preference,
        "lifecycle": lifecycle,
        "contract": contract_descriptor(),
    })))
}

pub(crate) async fn console_learning_candidate_apply_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(candidate_id): Path<String>,
    Json(payload): Json<ConsoleLearningCandidateApplyRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let candidate =
        load_console_learning_candidate(&state, &session.context, candidate_id.as_str()).await?;
    let applied = apply_patch_learning_candidate(
        &state.runtime,
        &candidate,
        session.context.principal.as_str(),
        payload.action_summary.as_deref(),
    )
    .await
    .map_err(runtime_status_response)?
    .ok_or_else(|| {
        runtime_status_response(tonic::Status::failed_precondition(
            "only patch-based learning candidates can be applied",
        ))
    })?;
    let response_candidate = applied.get("candidate").cloned().unwrap_or_else(|| json!(candidate));
    let lifecycle = learning_candidate_lifecycle_from_value(&response_candidate);
    Ok(Json(json!({
        "candidate": response_candidate,
        "apply": applied,
        "lifecycle": lifecycle,
        "contract": contract_descriptor(),
    })))
}

pub(crate) async fn console_learning_preferences_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleLearningPreferencesQuery>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let preferences = state
        .runtime
        .list_learning_preferences(journal::LearningPreferenceListFilter {
            owner_principal: Some(session.context.principal.clone()),
            device_id: None,
            channel: session.context.channel.clone(),
            scope_kind: query.scope_kind.and_then(trim_to_option),
            scope_id: query.scope_id.and_then(trim_to_option),
            status: query.status.and_then(trim_to_option),
            key: query.key.and_then(trim_to_option),
            limit: query.limit.unwrap_or(64).clamp(1, 256),
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "preferences": preferences,
        "contract": contract_descriptor(),
    })))
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
    let outcome = state
        .runtime
        .search_workspace_documents_with_diagnostics(journal::WorkspaceSearchRequest {
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
        "hits": outcome.hits,
        "diagnostics": outcome.diagnostics,
        "contract": contract_descriptor(),
    })))
}

pub(crate) async fn console_recall_preview_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleRecallPreviewRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    if let Some(message) = crate::runtime_preview_controls::capability_blocker_message(
        &state.runtime.config,
        RuntimePreviewCapability::RetrievalDualPath,
    ) {
        return Err(runtime_status_response(tonic::Status::failed_precondition(message)));
    }
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
    let max_candidates = payload.max_candidates.unwrap_or(8);
    if !(1..=12).contains(&max_candidates) {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "max_candidates must be in range 1..=12",
        )));
    }
    let prompt_budget_tokens = payload.prompt_budget_tokens.unwrap_or(
        usize::try_from(state.runtime.config.retrieval_dual_path.prompt_budget_tokens)
            .unwrap_or(usize::MAX),
    );
    if !(512..=4_096).contains(&prompt_budget_tokens) {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "prompt_budget_tokens must be in range 512..=4096",
        )));
    }
    let recall_channel = payload.channel.clone().or(session.context.channel.clone());
    let started_at_unix_ms = current_unix_ms();
    let preview = preview_recall(
        &state.runtime,
        &session.context,
        RecallRequest {
            query: query_text.to_owned(),
            channel: recall_channel.clone(),
            session_id: session_scope.clone(),
            agent_id: payload.agent_id.and_then(trim_to_option),
            memory_top_k: payload.memory_top_k.unwrap_or(4).clamp(0, 16),
            workspace_top_k: payload.workspace_top_k.unwrap_or(4).clamp(0, 16),
            min_score,
            workspace_prefix: payload.workspace_prefix.and_then(trim_to_option),
            include_workspace_historical: payload.include_workspace_historical.unwrap_or(false),
            include_workspace_quarantined: payload.include_workspace_quarantined.unwrap_or(false),
            max_candidates,
            prompt_budget_tokens,
        },
    )
    .await
    .map_err(runtime_status_response)?;
    let artifact = state
        .runtime
        .create_recall_artifact(build_recall_preview_artifact_request(
            &session.context,
            recall_channel,
            session_scope.clone(),
            &preview,
        ))
        .await
        .map_err(runtime_status_response)?;
    let elapsed_ms = current_unix_ms().saturating_sub(started_at_unix_ms).max(0) as u64;
    state
        .runtime
        .record_runtime_decision_event(
            &session.context,
            session_scope.as_deref(),
            None,
            RuntimeDecisionPayload::new(
                RuntimeDecisionEventType::RecallSessionSearch,
                state.runtime.runtime_decision_actor_from_context(
                    &session.context,
                    RuntimeDecisionActorKind::Operator,
                ),
                "recall_preview_requested",
                "retrieval_dual_path.preview.recall",
                RuntimeDecisionTiming::observed_with_duration(started_at_unix_ms, elapsed_ms),
            )
            .with_input(RuntimeEntityRef::new(
                "session",
                "session",
                session_scope.clone().unwrap_or_else(|| session.context.principal.clone()),
            ))
            .with_output(RuntimeEntityRef::new("preview", "recall_preview", "console"))
            .with_resource_budget(RuntimeResourceBudget {
                queue_depth: None,
                token_budget: Some(prompt_budget_tokens as u64),
                pruning_token_delta: None,
                retrieval_branch_latency_ms: Some(elapsed_ms),
                retry_count: None,
                suppression_count: None,
            })
            .with_details(json!({
                "query": query_text,
                "memory_hits": preview.memory_hits.len(),
                "workspace_hits": preview.workspace_hits.len(),
                "transcript_hits": preview.transcript_hits.len(),
                "checkpoint_hits": preview.checkpoint_hits.len(),
                "compaction_hits": preview.compaction_hits.len(),
                "top_candidates": preview.top_candidates.len(),
                "diagnostics": preview.diagnostics.clone(),
            })),
        )
        .await
        .map_err(runtime_status_response)?;
    if let Err(error) = state
        .runtime
        .record_console_event(
            &session.context,
            "memory.recall.preview",
            recall_preview_console_event_payload(&preview, artifact.artifact_id.as_str()),
        )
        .await
    {
        warn!(error = %error, "failed to record recall preview console event");
    }
    Ok(Json(json!({
        "query": preview.query,
        "memory_hits": preview.memory_hits,
        "workspace_hits": preview.workspace_hits,
        "transcript_hits": preview.transcript_hits,
        "checkpoint_hits": preview.checkpoint_hits,
        "compaction_hits": preview.compaction_hits,
        "top_candidates": preview.top_candidates,
        "structured_output": preview.structured_output,
        "plan": preview.plan,
        "diagnostics": preview.diagnostics,
        "parameter_delta": preview.parameter_delta,
        "prompt_preview": preview.prompt_preview,
        "artifact": artifact,
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

pub(crate) async fn console_session_search_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleSessionSearchQuery>,
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
    let channel = query.channel.or(session.context.channel.clone());
    let outcome = state
        .runtime
        .search_orchestrator_session_windows(SessionSearchRequest {
            principal: session.context.principal.clone(),
            device_id: session.context.device_id.clone(),
            channel: channel.clone(),
            query: search_query.to_owned(),
            top_k: query.top_k.unwrap_or(8).clamp(1, 24),
            min_score,
            window_before: query.window_before.unwrap_or(2).min(8),
            window_after: query.window_after.unwrap_or(2).min(8),
            max_windows_per_session: query.max_windows_per_session.unwrap_or(3).clamp(1, 8),
            include_archived: query.include_archived.unwrap_or(false),
        })
        .await
        .map_err(runtime_status_response)?;
    let artifact = state
        .runtime
        .create_recall_artifact(build_session_search_artifact_request(
            &session.context,
            channel,
            &outcome,
        ))
        .await
        .map_err(runtime_status_response)?;
    if let Err(error) = state
        .runtime
        .record_console_event(
            &session.context,
            "memory.session_search",
            json!({
                "capability": "palyra.recall.session_search",
                "query": search_query,
                "group_count": outcome.groups.len(),
                "window_count": outcome
                    .groups
                    .iter()
                    .map(|group| group.windows.len())
                    .sum::<usize>(),
                "diagnostics": outcome.diagnostics.clone(),
                "artifact_id": artifact.artifact_id,
            }),
        )
        .await
    {
        warn!(error = %error, "failed to record session search console event");
    }
    Ok(Json(json!({
        "capability": "palyra.recall.session_search",
        "query": outcome.query,
        "groups": outcome.groups,
        "diagnostics": outcome.diagnostics,
        "artifact": artifact,
        "contract": contract_descriptor(),
    })))
}

pub(crate) async fn console_recall_artifacts_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleRecallArtifactsQuery>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let session_scope = query.session_id.and_then(trim_to_option);
    if let Some(session_scope) = session_scope.as_deref() {
        validate_canonical_id(session_scope).map_err(|_| {
            runtime_status_response(tonic::Status::invalid_argument(
                "session_id must be a canonical ULID",
            ))
        })?;
    }
    let artifacts = state
        .runtime
        .list_recall_artifacts(RecallArtifactListFilter {
            principal: session.context.principal.clone(),
            device_id: session.context.device_id.clone(),
            channel: query.channel.or(session.context.channel.clone()),
            session_id: session_scope,
            artifact_kind: query.kind.and_then(trim_to_option),
            limit: query.limit.unwrap_or(24).clamp(1, 100),
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "artifacts": artifacts,
        "contract": contract_descriptor(),
    })))
}

fn build_recall_preview_artifact_request(
    context: &RequestContext,
    channel: Option<String>,
    session_id: Option<String>,
    preview: &RecallPreviewEnvelope,
) -> RecallArtifactCreateRequest {
    RecallArtifactCreateRequest {
        artifact_id: Ulid::new().to_string(),
        artifact_kind: RECALL_ARTIFACT_KIND_PREVIEW.to_owned(),
        principal: context.principal.clone(),
        device_id: context.device_id.clone(),
        channel,
        session_id,
        query: preview.query.clone(),
        summary: recall_preview_summary(preview),
        payload: json!({
            "query": preview.query,
            "plan": preview.plan,
            "memory_hits": preview.memory_hits,
            "workspace_hits": preview.workspace_hits,
            "transcript_hits": preview.transcript_hits,
            "checkpoint_hits": preview.checkpoint_hits,
            "compaction_hits": preview.compaction_hits,
            "top_candidates": preview.top_candidates,
            "structured_output": preview.structured_output,
            "diagnostics": preview.diagnostics,
            "parameter_delta": preview.parameter_delta,
            "prompt_preview": preview.prompt_preview,
            "durable_memory_write": false,
        }),
        diagnostics: json!({
            "branches": preview.diagnostics,
            "top_candidate_count": preview.top_candidates.len(),
            "memory_hit_count": preview.memory_hits.len(),
            "workspace_hit_count": preview.workspace_hits.len(),
            "transcript_hit_count": preview.transcript_hits.len(),
        }),
        provenance: recall_preview_provenance(preview),
        created_by_principal: context.principal.clone(),
    }
}

fn build_session_search_artifact_request(
    context: &RequestContext,
    channel: Option<String>,
    outcome: &SessionSearchOutcome,
) -> RecallArtifactCreateRequest {
    RecallArtifactCreateRequest {
        artifact_id: Ulid::new().to_string(),
        artifact_kind: RECALL_ARTIFACT_KIND_SESSION_SEARCH.to_owned(),
        principal: context.principal.clone(),
        device_id: context.device_id.clone(),
        channel,
        session_id: None,
        query: outcome.query.clone(),
        summary: session_search_summary(outcome),
        payload: json!({
            "capability": "palyra.recall.session_search",
            "query": outcome.query,
            "groups": outcome.groups,
            "diagnostics": outcome.diagnostics,
            "durable_memory_write": false,
        }),
        diagnostics: json!({
            "source_kind": outcome.diagnostics.source_kind,
            "candidate_count": outcome.diagnostics.candidate_count,
            "fused_hit_count": outcome.diagnostics.fused_hit_count,
            "total_latency_ms": outcome.diagnostics.total_latency_ms,
            "latency_budget_exceeded": outcome.diagnostics.latency_budget_exceeded,
            "degraded_reason": outcome.diagnostics.degraded_reason,
        }),
        provenance: session_search_provenance(outcome),
        created_by_principal: context.principal.clone(),
    }
}

fn recall_preview_summary(preview: &RecallPreviewEnvelope) -> String {
    format!(
        "{} candidate(s), {} memory hit(s), {} workspace hit(s), {} transcript hit(s)",
        preview.top_candidates.len(),
        preview.memory_hits.len(),
        preview.workspace_hits.len(),
        preview.transcript_hits.len()
    )
}

fn session_search_summary(outcome: &SessionSearchOutcome) -> String {
    let window_count = outcome.groups.iter().map(|group| group.windows.len()).sum::<usize>();
    format!("{} session group(s), {window_count} bounded window(s)", outcome.groups.len())
}

fn recall_preview_provenance(preview: &RecallPreviewEnvelope) -> Value {
    json!({
        "source": "recall_preview",
        "memory": preview.memory_hits.iter().map(|hit| {
            json!({
                "source_type": "memory",
                "memory_id": hit.item.memory_id,
                "channel": hit.item.channel,
                "session_id": hit.item.session_id,
                "source": hit.item.source,
            })
        }).collect::<Vec<_>>(),
        "workspace": preview.workspace_hits.iter().map(|hit| {
            json!({
                "source_type": "workspace_document",
                "document_id": hit.document.document_id,
                "path": hit.document.path,
                "version": hit.version,
                "chunk_index": hit.chunk_index,
            })
        }).collect::<Vec<_>>(),
        "transcript": preview.transcript_hits.iter().map(|hit| {
            json!({
                "source_type": "orchestrator_tape",
                "run_id": hit.run_id,
                "seq": hit.seq,
                "event_type": hit.event_type,
            })
        }).collect::<Vec<_>>(),
        "checkpoints": preview.checkpoint_hits.iter().map(|hit| {
            json!({
                "source_type": "checkpoint",
                "checkpoint_id": hit.checkpoint_id,
                "workspace_paths": hit.workspace_paths,
            })
        }).collect::<Vec<_>>(),
        "compactions": preview.compaction_hits.iter().map(|hit| {
            json!({
                "source_type": "compaction_artifact",
                "artifact_id": hit.artifact_id,
                "mode": hit.mode,
                "strategy": hit.strategy,
            })
        }).collect::<Vec<_>>(),
    })
}

fn session_search_provenance(outcome: &SessionSearchOutcome) -> Value {
    json!({
        "source": "session_search",
        "windows": outcome
            .groups
            .iter()
            .flat_map(|group| group.windows.iter())
            .map(|window| json!(window.provenance))
            .collect::<Vec<_>>(),
    })
}

fn recall_preview_console_event_payload(
    preview: &RecallPreviewEnvelope,
    artifact_id: &str,
) -> Value {
    let mut payload = recall_preview_console_payload(preview);
    if let Some(object) = payload.as_object_mut() {
        object.insert("artifact_id".to_owned(), json!(artifact_id));
    }
    payload
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

fn normalize_learning_candidate_review_status(status: &str) -> Result<String, tonic::Status> {
    let normalized = status.trim().to_ascii_lowercase().replace('_', "-");
    let accepted = match normalized.as_str() {
        "proposed" | "queued" => normalized,
        "needs-review" | "review" | "pending-review" => "needs-review".to_owned(),
        "approve" | "approved" => "approved".to_owned(),
        "accept" | "accepted" => "accepted".to_owned(),
        "reject" | "rejected" => "rejected".to_owned(),
        "deny" | "denied" => "denied".to_owned(),
        "suppress" | "suppressed" => "suppressed".to_owned(),
        "deploy" | "deployed" => "deployed".to_owned(),
        "applied" | "auto-applied" => normalized,
        "rollback" | "rolled-back" => "rolled-back".to_owned(),
        "conflicted" => normalized,
        _ => {
            return Err(tonic::Status::invalid_argument(
                "status must be proposed, needs-review, approved, rejected, deployed, rolled-back, or a supported legacy review state",
            ));
        }
    };
    Ok(accepted)
}

fn learning_candidates_lifecycle_summary(candidates: &[journal::LearningCandidateRecord]) -> Value {
    let mut counts = serde_json::Map::new();
    for status in ["proposed", "needs-review", "approved", "rejected", "deployed", "rolled-back"] {
        counts.insert(status.to_owned(), json!(0_u64));
    }
    let mut injection_conflicts = 0_u64;
    for candidate in candidates {
        let status = learning_candidate_lifecycle_status(candidate);
        let count = counts.get(status).and_then(Value::as_u64).unwrap_or(0).saturating_add(1);
        counts.insert(status.to_owned(), json!(count));
        if learning_candidate_has_injection_conflict(candidate) {
            injection_conflicts = injection_conflicts.saturating_add(1);
        }
    }
    json!({
        "candidate_count": candidates.len(),
        "status_counts": counts,
        "injection_conflicts": injection_conflicts,
        "allowed_statuses": [
            "proposed",
            "needs-review",
            "approved",
            "rejected",
            "deployed",
            "rolled-back"
        ],
        "review_actions": ["approve", "reject", "edit", "merge", "deploy", "rollback"],
        "deployment_policy": {
            "auto_deploy_enabled": false,
            "policy_gate": "operator_review_required",
            "raw_prompts_included": false,
            "raw_secrets_included": false,
        },
    })
}

fn learning_candidate_lifecycle(
    candidate: &journal::LearningCandidateRecord,
    history: &[journal::LearningCandidateHistoryRecord],
) -> Value {
    let status = learning_candidate_lifecycle_status(candidate);
    let rollback_seen = status == "rolled-back"
        || history.iter().any(|entry| {
            entry.status.eq_ignore_ascii_case("rolled-back")
                || entry
                    .action_summary
                    .as_deref()
                    .is_some_and(|summary| summary.to_ascii_lowercase().contains("rollback"))
        });
    json!({
        "candidate_id": candidate.candidate_id,
        "type": candidate.candidate_kind,
        "status": status,
        "stored_status": candidate.status,
        "scope": {
            "kind": candidate.scope_kind,
            "id": candidate.scope_id,
            "owner_principal": candidate.owner_principal,
            "device_id": candidate.device_id,
            "channel": candidate.channel,
            "session_id": candidate.session_id,
        },
        "evidence": {
            "confidence": candidate.confidence,
            "risk_level": candidate.risk_level,
            "provenance_present": !candidate.provenance_json.trim().is_empty(),
            "source_task_id": candidate.source_task_id,
            "created_at_unix_ms": candidate.created_at_unix_ms,
            "updated_at_unix_ms": candidate.updated_at_unix_ms,
        },
        "proposed_change": {
            "title": candidate.title,
            "summary": candidate.summary,
            "target_path": candidate.target_path,
            "content_json_bytes": candidate.content_json.len(),
        },
        "deployment_posture": learning_candidate_deployment_posture(candidate, status),
        "rollback": {
            "available": matches!(status, "approved" | "deployed" | "rolled-back"),
            "seen": rollback_seen,
            "restore_contract": "rollback records status=rolled-back with action_payload_json evidence for restored memory, routine, config, or patch state",
            "previous_state_restored": candidate
                .last_action_payload_json
                .as_deref()
                .and_then(parse_json_object)
                .and_then(|payload| payload.get("previous_state_restored").and_then(Value::as_bool))
                .unwrap_or(false),
        },
    })
}

fn learning_candidate_lifecycle_from_value(candidate: &Value) -> Value {
    let status = candidate.get("status").and_then(Value::as_str).unwrap_or("proposed");
    let auto_applied = candidate.get("auto_applied").and_then(Value::as_bool).unwrap_or(false);
    let lifecycle_status = learning_candidate_status_label(status, auto_applied, None);
    json!({
        "candidate_id": candidate.get("candidate_id").and_then(Value::as_str),
        "type": candidate.get("candidate_kind").and_then(Value::as_str),
        "status": lifecycle_status,
        "stored_status": status,
        "deployment_posture": {
            "auto_deploy_enabled": false,
            "policy_gate": "operator_review_required",
            "impact_scope": candidate.get("scope_kind").and_then(Value::as_str),
            "deployed": lifecycle_status == "deployed",
        },
        "rollback": {
            "available": matches!(lifecycle_status, "approved" | "deployed" | "rolled-back"),
            "restore_contract": "rollback records status=rolled-back with action_payload_json evidence for restored memory, routine, config, or patch state",
        },
    })
}

fn learning_candidate_lifecycle_status(
    candidate: &journal::LearningCandidateRecord,
) -> &'static str {
    let applied_by_action = candidate
        .last_action_payload_json
        .as_deref()
        .and_then(parse_json_object)
        .and_then(|payload| payload.get("action").and_then(Value::as_str).map(str::to_owned))
        .is_some_and(|action| action == "apply_preference" || action == "apply_patch_candidate");
    learning_candidate_status_label(
        candidate.status.as_str(),
        candidate.auto_applied,
        Some(applied_by_action),
    )
}

fn learning_candidate_status_label(
    status: &str,
    auto_applied: bool,
    applied_by_action: Option<bool>,
) -> &'static str {
    if auto_applied || applied_by_action.unwrap_or(false) {
        return "deployed";
    }
    match status.trim().to_ascii_lowercase().replace('_', "-").as_str() {
        "" | "queued" | "proposed" => "proposed",
        "needs-review" | "review" | "pending-review" => "needs-review",
        "approved" | "accepted" => "approved",
        "rejected" | "denied" | "suppressed" | "conflicted" => "rejected",
        "applied" | "auto-applied" | "deployed" => "deployed",
        "rolled-back" | "rollback" => "rolled-back",
        _ => "proposed",
    }
}

fn learning_candidate_deployment_posture(
    candidate: &journal::LearningCandidateRecord,
    lifecycle_status: &str,
) -> Value {
    let impact_scope = match candidate.scope_kind.as_str() {
        "global" => "global",
        "workspace" | "workspace_document" => "workspace",
        "user" | "profile" => "user",
        "session" => "session",
        _ => "candidate_scope",
    };
    json!({
        "auto_deploy_enabled": false,
        "policy_gate": "operator_review_required",
        "impact_scope": impact_scope,
        "candidate_kind": candidate.candidate_kind,
        "deployed": lifecycle_status == "deployed",
        "requires_review_before_deploy": !matches!(lifecycle_status, "deployed" | "rolled-back"),
    })
}

fn learning_candidate_has_injection_conflict(candidate: &journal::LearningCandidateRecord) -> bool {
    [
        candidate.risk_level.as_str(),
        candidate.title.as_str(),
        candidate.summary.as_str(),
        candidate.content_json.as_str(),
        candidate.provenance_json.as_str(),
    ]
    .iter()
    .any(|value| {
        let normalized = value.to_ascii_lowercase();
        normalized.contains("prompt_injection")
            || normalized.contains("prompt-injection")
            || normalized.contains("injection conflict")
    })
}

fn parse_json_object(payload: &str) -> Option<serde_json::Map<String, Value>> {
    serde_json::from_str::<Value>(payload).ok()?.as_object().cloned()
}

async fn load_console_learning_candidate(
    state: &AppState,
    context: &RequestContext,
    candidate_id: &str,
) -> Result<journal::LearningCandidateRecord, Response> {
    let candidate = state
        .runtime
        .list_learning_candidates(journal::LearningCandidateListFilter {
            candidate_id: Some(candidate_id.to_owned()),
            owner_principal: Some(context.principal.clone()),
            device_id: None,
            channel: context.channel.clone(),
            session_id: None,
            scope_kind: None,
            scope_id: None,
            candidate_kind: None,
            status: None,
            risk_level: None,
            source_task_id: None,
            min_confidence: None,
            max_confidence: None,
            limit: 1,
        })
        .await
        .map_err(runtime_status_response)?
        .into_iter()
        .next()
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found("learning candidate not found"))
        })?;
    Ok(candidate)
}
