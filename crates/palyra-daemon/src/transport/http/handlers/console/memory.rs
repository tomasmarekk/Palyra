//! Console memory administration handlers for the `/console/v1/memory/*`
//! routes: status/index/search/purge, learning candidates and preferences,
//! workspace documents, recall previews, and session search.
//!
//! Every handler is scoped to the authenticated console session's principal
//! (and usually its channel) -- this surface is per-user memory
//! administration, not a cross-tenant admin API. Heavy operations are
//! bounded: index runs are single-flight with a per-request batch budget,
//! and purge demands an explicit scope. Drift/reconcile endpoints for the
//! external retrieval index live in the sibling
//! `memory_external_index` module; recall/search handlers persist their
//! outcomes as recall artifacts in the journal.

use std::{
    collections::BTreeSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use serde::{Deserialize, Serialize};

use crate::gateway::ListOrchestratorSessionsRequest;
use crate::gateway::{current_unix_ms, MEMORY_AUTO_INJECT_MIN_SCORE};
use crate::journal::{
    session_search_source_refs_projection, MemoryRetentionPolicy, RecallArtifactCreateRequest,
    RecallArtifactListFilter, RecallArtifactRecord, SessionSearchOutcome, SessionSearchRequest,
    RECALL_ARTIFACT_KIND_LEARNING_CURATOR_REPORT, RECALL_ARTIFACT_KIND_PREVIEW,
    RECALL_ARTIFACT_KIND_SESSION_SEARCH,
};
use crate::*;
use crate::{
    application::learning::{
        apply_patch_learning_candidate, apply_preference_candidate,
        preference_procedure_conflict_report, project_skill_invocation_hygiene_for_candidate,
        shadow_learning_candidate_lifecycle, LearningCurator, LearningCuratorInput,
        LearningCuratorReport, PreferenceProcedureConflictReport,
        LEARNING_CURATOR_EVENT_REPORT_CREATED,
    },
    application::memory_provider::{
        explain_provider_hit, memory_provider_prefetch_snapshot, memory_provider_status_snapshot,
        memory_provider_system_prompt_snapshot, run_memory_provider_reindex,
        MemoryProviderHookContext,
    },
    application::provider_input::curated_memory_sources_for_prompt_context,
    application::recall::{
        preview_recall, recall_preview_console_payload, RecallPreviewEnvelope, RecallRequest,
    },
    domain::workspace::{curated_workspace_roots, curated_workspace_templates},
};
use palyra_common::runtime_preview::{
    RuntimeDecisionActorKind, RuntimeDecisionEventType, RuntimeDecisionPayload,
    RuntimeDecisionTiming, RuntimeEntityRef, RuntimePreviewCapability, RuntimeResourceBudget,
};
use sha2::{Digest, Sha256};
use ulid::Ulid;

/// Batches executed per index request when the caller does not pick a limit.
const MEMORY_INDEX_DEFAULT_MAX_BATCHES_PER_REQUEST: u64 = 8;
/// Upper bound on batches per index request even when the caller asks for
/// more; keeps a single HTTP request from running unbounded reindex work.
const MEMORY_INDEX_HARD_MAX_BATCHES_PER_REQUEST: u64 = 32;
/// Wire-visible `cancel_reason` reported when the batch budget stops a run.
const MEMORY_INDEX_BATCH_LIMIT_REASON: &str = "batch_limit_reached";
/// Error message returned while another index run holds the single-flight
/// guard.
const MEMORY_INDEX_CONCURRENT_RUN_MESSAGE: &str = "memory index run already in progress";

/// `GET /console/v1/memory/status` — aggregates the memory subsystem view:
/// usage, embeddings, retrieval backend plus diagnostics, providers,
/// retention/auto-inject/maintenance/learning config, a workspace preview,
/// and a shallow recall-artifact inventory.
///
/// # Errors
/// Returns an error response when console authorization fails or any of the
/// underlying runtime/channel snapshots fail.
pub(crate) async fn console_memory_status_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let provider_context = MemoryProviderHookContext::from_request_context(&session.context);
    let provider_statuses =
        memory_provider_status_snapshot(state.runtime.clone(), &provider_context)
            .await
            .map_err(runtime_status_response)?;
    let provider_system_prompt =
        memory_provider_system_prompt_snapshot(state.runtime.clone(), &provider_context)
            .await
            .map_err(runtime_status_response)?;
    let maintenance_status =
        state.runtime.memory_maintenance_status().await.map_err(runtime_status_response)?;
    let embeddings_status =
        state.runtime.memory_embeddings_status().await.map_err(runtime_status_response)?;
    let memory_config = state.runtime.memory_config_snapshot();
    let auto_inject_sources = curated_memory_sources_for_prompt_context()
        .into_iter()
        .map(|source| source.as_str())
        .collect::<Vec<_>>();
    let retrieval_config = state.runtime.retrieval_config_snapshot();
    let retrieval_backend =
        state.runtime.retrieval_backend_snapshot().map_err(runtime_status_response)?;
    let retrieval_diagnostics = super::memory_external_index::build_memory_retrieval_diagnostics(
        &retrieval_backend,
        None,
        None,
    );
    let learning_config = state.runtime.learning_config_snapshot();
    let counters = state.runtime.counters.snapshot();
    let workspace_preview = state
        .runtime
        .list_workspace_documents(journal::WorkspaceDocumentListFilter {
            principal: session.context.principal.clone(),
            channel: session.context.channel.clone(),
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
            principal: session.context.principal.clone(),
            device_id: session.context.device_id.clone(),
            channel: session.context.channel.clone(),
            session_id: None,
            artifact_kind: None,
            limit: 8,
        })
        .await
        .map_err(runtime_status_response)?;
    let derived = state.channels.derived_stats().map_err(channel_platform_error_response)?;
    let maintenance_interval_ms =
        i64::try_from(MEMORY_MAINTENANCE_INTERVAL.as_millis()).unwrap_or(i64::MAX);
    let scoring_diagnostics = memory_retrieval_scoring_diagnostics(&retrieval_config.scoring);
    let doctor = memory_doctor_report(&embeddings_status, scoring_diagnostics);
    Ok(Json(json!({
        "usage": maintenance_status.usage,
        "embeddings": embeddings_status,
        "doctor": doctor,
        "retrieval": {
            "backend": retrieval_backend,
            "external_index": retrieval_backend.external_index.clone(),
            "diagnostics": retrieval_diagnostics,
            "scoring": retrieval_config.scoring,
        },
        "providers": provider_statuses,
        "provider_context": {
            "system_prompt": provider_system_prompt,
        },
        "retention": {
            "max_entries": memory_config.retention_max_entries,
            "max_bytes": memory_config.retention_max_bytes,
            "ttl_days": memory_config.retention_ttl_days,
            "vacuum_schedule": memory_config.retention_vacuum_schedule,
        },
        "auto_inject": {
            "enabled": memory_config.auto_inject_enabled,
            "max_items": memory_config.auto_inject_max_items,
            "min_score": MEMORY_AUTO_INJECT_MIN_SCORE,
            "sources": auto_inject_sources,
            "manual_ingest_visibility": "manual/import memories are eligible for automatic prompt context when enabled",
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
            "latest": latest_recall_artifacts
                .iter()
                .map(recall_artifact_inventory_json)
                .collect::<Vec<_>>(),
            "detail_source": "/console/v1/memory/recall-artifacts",
        },
        "derived": derived,
    })))
}

fn memory_doctor_report(
    embeddings_status: &journal::MemoryEmbeddingsStatus,
    scoring_diagnostics: Value,
) -> Value {
    let coverage_ratio = if embeddings_status.total_count == 0 {
        1.0
    } else {
        embeddings_status.indexed_count as f64 / embeddings_status.total_count as f64
    };
    let vector_degraded = !embeddings_status.production_default_active
        || embeddings_status.mode == journal::MemoryEmbeddingsMode::HashFallback;
    let stale_or_failed_jobs =
        embeddings_status.queue.stale_count.saturating_add(embeddings_status.queue.failed_count);
    let mut findings = Vec::new();
    if vector_degraded {
        findings.push(json!({
            "severity": "warning",
            "code": "vector_degraded",
            "message": embeddings_status.warning.clone().unwrap_or_else(|| {
                "memory vector retrieval is running in degraded mode".to_owned()
            }),
        }));
    }
    if embeddings_status.pending_count > 0 {
        findings.push(json!({
            "severity": "warning",
            "code": "stale_embeddings",
            "message": format!("{} memory items need embedding refresh", embeddings_status.pending_count),
        }));
    }
    if stale_or_failed_jobs > 0 {
        findings.push(json!({
            "severity": "error",
            "code": "embedding_jobs_attention_required",
            "message": format!("{} embedding jobs are stale or failed", stale_or_failed_jobs),
        }));
    }
    if scoring_diagnostics.get("extreme_weight_warning").and_then(Value::as_bool).unwrap_or(false) {
        findings.push(json!({
            "severity": "warning",
            "code": "retrieval_scoring_extreme_weights",
            "message": "retrieval scoring has an extreme component weight; verify lexical, vector, recency, diversity, and trust balance",
        }));
    }
    let status = if findings
        .iter()
        .any(|finding| finding.get("severity").and_then(Value::as_str) == Some("error"))
    {
        "error"
    } else if findings.is_empty() {
        "healthy"
    } else {
        "warning"
    };
    json!({
        "status": status,
        "checked_at_unix_ms": crate::node_runtime::current_unix_ms().unwrap_or_default(),
        "indexes": {
            "fts": {
                "status": "available",
                "schema_version": 1,
                "fallback_ready": true,
            },
            "vector": {
                "status": if vector_degraded { "degraded" } else { "available" },
                "embedding_model_id": embeddings_status.target_model_id,
                "embedding_model_version": embeddings_status.target_version,
                "coverage_ratio": coverage_ratio,
                "degraded_mode": vector_degraded,
                "lag_count": embeddings_status.pending_count,
            },
            "embedding_queue": embeddings_status.queue,
        },
        "reindex": {
            "single_flight_lock": true,
            "duplicate_reindex_policy": "reject_concurrent_run",
            "progress_events": "memory.index.run console events",
            "batch_limit": embeddings_status.batch_limit,
        },
        "scoring": scoring_diagnostics,
        "support_bundle": {
            "raw_memory_content_included": false,
            "redaction": "doctor reports counts, hashes, refs, and safe diagnostics only",
        },
        "findings": findings,
        "remediation": embeddings_status.remediation,
    })
}

fn memory_retrieval_scoring_diagnostics<T: Serialize>(scoring: &T) -> Value {
    let scoring_value = serde_json::to_value(scoring).unwrap_or_else(|_| json!({}));
    let mut extreme_fields = Vec::new();
    collect_extreme_scoring_weights("", &scoring_value, &mut extreme_fields);
    json!({
        "defaults_validated": true,
        "components": ["lexical", "vector", "recency", "diversity", "trust"],
        "extreme_weight_warning": !extreme_fields.is_empty(),
        "extreme_fields": extreme_fields,
    })
}

fn collect_extreme_scoring_weights(prefix: &str, value: &Value, extreme_fields: &mut Vec<String>) {
    match value {
        Value::Object(object) => {
            for (key, child) in object {
                let next_prefix =
                    if prefix.is_empty() { key.clone() } else { format!("{prefix}.{key}") };
                collect_extreme_scoring_weights(next_prefix.as_str(), child, extreme_fields);
            }
        }
        Value::Number(number) if prefix.ends_with("_bps") => {
            if let Some(value) = number.as_u64() {
                let is_primary_weight = prefix.ends_with("lexical_bps")
                    || prefix.ends_with("vector_bps")
                    || prefix.ends_with("recency_bps")
                    || prefix.ends_with("source_quality_bps");
                if is_primary_weight && (value >= 9_000 || value <= 50) {
                    extreme_fields.push(prefix.to_owned());
                }
            }
        }
        _ => {}
    }
}

/// Shallow inventory row for a recall artifact: metadata plus availability
/// flags only. The full payload/diagnostics/provenance bodies are
/// deliberately omitted so the status endpoint stays small; clients fetch
/// detail from `/console/v1/memory/recall-artifacts`.
fn recall_artifact_inventory_json(artifact: &RecallArtifactRecord) -> Value {
    json!({
        "artifact_id": artifact.artifact_id,
        "artifact_kind": artifact.artifact_kind,
        "channel": artifact.channel,
        "session_id": artifact.session_id,
        "query": truncate_recall_inventory_text(artifact.query.as_str(), 160),
        "summary": truncate_recall_inventory_text(artifact.summary.as_str(), 320),
        "created_by_principal": artifact.created_by_principal,
        "created_at_unix_ms": artifact.created_at_unix_ms,
        "payload_available": !artifact.payload.is_null(),
        "diagnostics_available": !artifact.diagnostics.is_null(),
        "provenance_available": !artifact.provenance.is_null(),
    })
}

/// Truncates to `max_chars` characters (not bytes) with a `...` marker, so
/// multi-byte UTF-8 content can never be split mid-character.
fn truncate_recall_inventory_text(value: &str, max_chars: usize) -> String {
    let trimmed = value.trim();
    let mut output = String::with_capacity(trimmed.len().min(max_chars));
    let mut count = 0usize;
    for ch in trimmed.chars() {
        if count >= max_chars {
            output.push_str("...");
            return output;
        }
        output.push(ch);
        count = count.saturating_add(1);
    }
    output
}

/// `GET /console/v1/memory/derived-artifacts` — lists derived artifacts
/// linked to a workspace document and/or memory item, post-filtered to the
/// caller's principal and channel.
///
/// # Errors
/// Returns an error response when console authorization fails, neither
/// `workspace_document_id` nor `memory_item_id` is provided, or the channel
/// platform query fails.
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

/// `POST /console/v1/memory/index` — runs provider reindex and external
/// retrieval indexer batches (optionally preceded by memory maintenance) and
/// reports progress, drift, and embeddings status.
///
/// Index runs are single-flight per daemon and bounded by a per-request
/// batch budget; a budget-stopped run is reported as cancelled with
/// `cancel_reason = "batch_limit_reached"` so the operator can rerun to
/// continue.
///
/// # Errors
/// Returns a resource-exhausted response while another index run is in
/// flight, and an error response when console authorization or any
/// maintenance/reindex/snapshot step fails.
pub(crate) async fn console_memory_index_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleMemoryIndexRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let _index_guard = try_acquire_console_memory_index_guard(&state.console_memory_index_active)
        .map_err(runtime_status_response)?;
    let batch_size = payload.batch_size.unwrap_or(64).clamp(1, 256);
    let until_complete = payload.until_complete.unwrap_or(false);
    let run_maintenance = payload.run_maintenance.unwrap_or(false);
    let batch_budget = memory_index_batch_budget(payload.cancel_after_batches);

    let maintenance =
        if run_maintenance { Some(run_memory_maintenance_now(&state).await?) } else { None };

    let mut provider_reindex = run_memory_provider_reindex(state.runtime.clone(), batch_size)
        .await
        .map_err(runtime_status_response)?;
    let mut batches_executed = 1_u64;
    let mut provider_batch_limit_reached = false;
    let mut scanned_count = provider_reindex.progress.scanned_count;
    let mut updated_count = provider_reindex.progress.updated_count;
    while until_complete && !provider_reindex.progress.complete {
        if batches_executed >= batch_budget.max_batches_per_request {
            provider_batch_limit_reached = true;
            mark_provider_reindex_batch_limited(
                &mut provider_reindex,
                batch_budget.max_batches_per_request,
            );
            break;
        }
        provider_reindex = run_memory_provider_reindex(state.runtime.clone(), batch_size)
            .await
            .map_err(runtime_status_response)?;
        scanned_count = scanned_count.saturating_add(provider_reindex.progress.scanned_count);
        updated_count = updated_count.saturating_add(provider_reindex.progress.updated_count);
        batches_executed = batches_executed.saturating_add(1);
    }
    let mut external_indexer = state
        .runtime
        .run_external_retrieval_indexer(batch_size)
        .await
        .map_err(runtime_status_response)?;
    let mut external_indexer_batches_executed = 1_u64;
    let mut external_indexer_batch_limit_reached = false;
    // The external indexer stops looping once the provider phase was
    // cancelled (budget hit): the request already burned its batch budget,
    // so the rerun that continues the provider phase continues this one too.
    while until_complete && !provider_reindex.cancelled && !external_indexer.complete {
        if external_indexer_batches_executed >= batch_budget.max_batches_per_request {
            external_indexer_batch_limit_reached = true;
            break;
        }
        external_indexer = state
            .runtime
            .run_external_retrieval_indexer(batch_size)
            .await
            .map_err(runtime_status_response)?;
        external_indexer_batches_executed = external_indexer_batches_executed.saturating_add(1);
    }
    let external_indexer_payload = external_indexer_payload(
        &external_indexer,
        external_indexer_batches_executed,
        batch_budget.max_batches_per_request,
        external_indexer_batch_limit_reached,
    );
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
        "ran_at_unix_ms": provider_reindex.ran_at_unix_ms,
        "batch_size": provider_reindex.progress.batch_size,
        "batches_executed": batches_executed,
        "scanned_count": scanned_count,
        "updated_count": updated_count,
        "pending_count": provider_reindex.progress.pending_count,
        "claimed_count": provider_reindex.progress.claimed_count,
        "failed_count": provider_reindex.progress.failed_count,
        "stale_count": provider_reindex.progress.stale_count,
        "complete": provider_reindex.progress.complete,
        "target_model_id": provider_reindex.progress.target_model_id,
        "target_dims": provider_reindex.progress.target_dims,
        "target_version": provider_reindex.progress.target_version,
        "queue": embeddings_status.queue.clone(),
        "until_complete": until_complete,
        "cancel_after_batches": batch_budget.requested_cancel_after_batches,
        "max_batches_per_request": batch_budget.max_batches_per_request,
        "batch_limit_reached": provider_batch_limit_reached,
        "cancelled": provider_reindex.cancelled,
        "cancel_reason": provider_reindex.cancel_reason.clone(),
    });
    let event_details = json!({
        "batch_size": batch_size,
        "until_complete": until_complete,
        "run_maintenance": run_maintenance,
        "index": index_payload.clone(),
        "external_indexer": external_indexer_payload.clone(),
        "provider_reindex": provider_reindex.clone(),
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
        "external_indexer": external_indexer_payload,
        "provider_reindex": provider_reindex,
        "drift": drift,
        "external_index": retrieval_backend.external_index.clone(),
        "retrieval": {
            "backend": retrieval_backend,
        },
        "embeddings": embeddings_status,
    })))
}

/// Effective per-request batch budget for an index run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MemoryIndexBatchBudget {
    /// What the caller asked for (`None` when absent or zero), echoed back in
    /// the response for transparency.
    requested_cancel_after_batches: Option<u64>,
    /// Enforced limit after defaulting and clamping to the hard maximum.
    max_batches_per_request: u64,
}

/// Resolves the caller's `cancel_after_batches` into an enforced budget;
/// zero is treated the same as absent.
fn memory_index_batch_budget(cancel_after_batches: Option<u64>) -> MemoryIndexBatchBudget {
    let requested_cancel_after_batches = cancel_after_batches.filter(|value| *value > 0);
    let max_batches_per_request = requested_cancel_after_batches
        .unwrap_or(MEMORY_INDEX_DEFAULT_MAX_BATCHES_PER_REQUEST)
        .clamp(1, MEMORY_INDEX_HARD_MAX_BATCHES_PER_REQUEST);
    MemoryIndexBatchBudget { requested_cancel_after_batches, max_batches_per_request }
}

/// Rewrites a budget-stopped provider reindex outcome into the cancelled
/// shape the wire contract uses, so clients see one consistent
/// "stopped early, rerun to continue" signal for both explicit cancels and
/// budget exhaustion.
fn mark_provider_reindex_batch_limited(
    provider_reindex: &mut crate::application::memory_provider::MemoryProviderReindexOutcome,
    max_batches_per_request: u64,
) {
    provider_reindex.state = "cancelled".to_owned();
    provider_reindex.cancelled = true;
    provider_reindex.cancel_reason = Some(MEMORY_INDEX_BATCH_LIMIT_REASON.to_owned());
    provider_reindex.artifact_log.push(format!(
        "memory provider reindex stopped after {max_batches_per_request} batches; rerun to continue"
    ));
}

/// Serializes the external indexer outcome and annotates it with the batch
/// accounting fields (`batches_executed`, budget, cancellation) the index
/// endpoint promises.
fn external_indexer_payload(
    external_indexer: &crate::retrieval::ExternalRetrievalIndexerOutcome,
    batches_executed: u64,
    max_batches_per_request: u64,
    batch_limit_reached: bool,
) -> Value {
    let mut payload = serde_json::to_value(external_indexer).unwrap_or_else(|_| json!({}));
    if let Some(object) = payload.as_object_mut() {
        object.insert("batches_executed".to_owned(), json!(batches_executed));
        object.insert("max_batches_per_request".to_owned(), json!(max_batches_per_request));
        object.insert("batch_limit_reached".to_owned(), json!(batch_limit_reached));
        object.insert("cancelled".to_owned(), json!(batch_limit_reached));
        if batch_limit_reached {
            object.insert("cancel_reason".to_owned(), json!(MEMORY_INDEX_BATCH_LIMIT_REASON));
        }
    }
    payload
}

/// RAII guard marking a console memory index run as active; releases the
/// single-flight flag on drop (including early returns and panics).
struct ConsoleMemoryIndexRunGuard {
    active: Arc<AtomicBool>,
}

impl Drop for ConsoleMemoryIndexRunGuard {
    fn drop(&mut self) {
        self.active.store(false, Ordering::Release);
    }
}

/// Attempts to claim the single-flight index slot. Reindex batches hammer
/// the journal and embedding providers, so concurrent console-triggered runs
/// are rejected rather than queued.
///
/// # Errors
/// Returns `resource_exhausted` while another run holds the slot.
fn try_acquire_console_memory_index_guard(
    active: &Arc<AtomicBool>,
) -> Result<ConsoleMemoryIndexRunGuard, tonic::Status> {
    active
        .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
        .map_err(|_| tonic::Status::resource_exhausted(MEMORY_INDEX_CONCURRENT_RUN_MESSAGE))?;
    Ok(ConsoleMemoryIndexRunGuard { active: Arc::clone(active) })
}

/// `GET /console/v1/memory/search` — searches the caller's memory items with
/// scoring diagnostics, optionally filtered by channel, session, tags, and
/// sources.
///
/// # Errors
/// Returns an error response when console authorization fails, the query is
/// empty, `min_score`/`session_id`/`sources_csv` are invalid, or the search
/// itself fails.
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
    let session_scope = query.session_id.and_then(trim_to_option);
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

/// `GET /console/v1/memory/providers/explain` — runs a provider prefetch for
/// the query and explains the scoring of each hit above `min_score`, for
/// debugging what auto-inject would surface.
///
/// # Errors
/// Returns an error response when console authorization fails, the query is
/// empty, `min_score` is invalid, or the provider prefetch fails.
pub(crate) async fn console_memory_provider_explain_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleMemoryProviderExplainQuery>,
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
    let mut provider_context = MemoryProviderHookContext::from_request_context(&session.context);
    provider_context.channel = query.channel.or(session.context.channel.clone());
    provider_context.session_id = query.session_id.and_then(trim_to_option);
    provider_context.agent_id = query.agent_id.and_then(trim_to_option);
    provider_context.workspace_id = query.workspace_prefix.and_then(trim_to_option);
    provider_context.objective = Some(search_query.to_owned());
    provider_context.provenance = json!({
        "source": "console_memory_provider_explain",
        "principal": session.context.principal.clone(),
        "device_id": session.context.device_id.clone(),
        "channel": provider_context.channel.clone(),
    });
    let outcomes = memory_provider_prefetch_snapshot(state.runtime.clone(), &provider_context)
        .await
        .map_err(runtime_status_response)?;
    let top_k = query.top_k.unwrap_or(8).clamp(1, 32);
    let explanations = outcomes
        .iter()
        .flat_map(|outcome| outcome.hits.iter())
        .filter(|hit| hit.score.final_score >= min_score)
        .take(top_k)
        .map(explain_provider_hit)
        .collect::<Vec<_>>();
    Ok(Json(json!({
        "query": search_query,
        "providers": outcomes,
        "explanations": explanations,
        "contract": contract_descriptor(),
    })))
}

/// `POST /console/v1/memory/purge` — deletes the caller's memory items within
/// an explicit scope and returns the deleted count.
///
/// A purge with no channel/session scope must say `purge_all_principal=true`
/// out loud; this keeps a payload of accidental nulls from silently wiping
/// the principal's entire memory.
///
/// # Errors
/// Returns an error response when console authorization fails, the scope is
/// missing or invalid, or the purge itself fails.
pub(crate) async fn console_memory_purge_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleMemoryPurgeRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let session_scope = payload.session_id.and_then(trim_to_option);
    if let Some(session_scope) = session_scope.as_deref() {
        validate_canonical_id(session_scope).map_err(|_| {
            runtime_status_response(tonic::Status::invalid_argument(
                "session_id must be a canonical ULID",
            ))
        })?;
    }
    let channel = payload.channel.and_then(trim_to_option).or(session.context.channel.clone());
    let purge_all_principal = payload.purge_all_principal.unwrap_or(false);
    if !purge_all_principal && channel.is_none() && session_scope.is_none() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "purge request requires purge_all_principal=true or channel/session scope",
        )));
    }

    let deleted_count = state
        .runtime
        .purge_memory(MemoryPurgeRequest {
            principal: session.context.principal,
            channel,
            session_id: session_scope,
            purge_all_principal,
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({ "deleted_count": deleted_count })))
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct ConsoleLearningCuratorReportRequest {
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    stale_after_ms: Option<i64>,
}

/// `POST /console/v1/memory/learning/curator/report` — builds an observe-only
/// learning curator report, stores it as a recall artifact, and records a
/// console audit event. The report suggests dedupe, merge, conflict, and
/// archive actions but never mutates candidates, preferences, or procedures.
///
/// # Errors
/// Returns an error response when authorization, journal reads, artifact
/// creation, or console-event recording fails.
pub(crate) async fn console_learning_curator_report_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleLearningCuratorReportRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let limit = payload.limit.unwrap_or(256).clamp(1, 512);
    let stale_after_ms = payload.stale_after_ms.unwrap_or(14 * 24 * 60 * 60 * 1_000).max(0);
    let candidates = state
        .runtime
        .list_learning_candidates(journal::LearningCandidateListFilter {
            candidate_id: None,
            owner_principal: Some(session.context.principal.clone()),
            device_id: None,
            channel: session.context.channel.clone(),
            session_id: None,
            scope_kind: None,
            scope_id: None,
            candidate_kind: None,
            status: None,
            risk_level: None,
            source_task_id: None,
            min_confidence: None,
            max_confidence: None,
            limit,
        })
        .await
        .map_err(runtime_status_response)?;
    let preferences = state
        .runtime
        .list_learning_preferences(journal::LearningPreferenceListFilter {
            owner_principal: Some(session.context.principal.clone()),
            device_id: None,
            channel: session.context.channel.clone(),
            scope_kind: None,
            scope_id: None,
            status: Some("active".to_owned()),
            key: None,
            limit,
        })
        .await
        .map_err(runtime_status_response)?;
    let report = LearningCurator.curate(LearningCuratorInput {
        report_id: Ulid::new().to_string(),
        generated_at_unix_ms: current_unix_ms(),
        stale_after_ms,
        candidates: candidates.as_slice(),
        preferences: preferences.as_slice(),
    });
    let conflict_report = preference_procedure_conflict_report(&report);
    let artifact = state
        .runtime
        .create_recall_artifact(build_learning_curator_artifact_request(
            &session.context,
            session.context.channel.clone(),
            &report,
            &conflict_report,
        ))
        .await
        .map_err(runtime_status_response)?;
    state
        .runtime
        .record_console_event(
            &session.context,
            LEARNING_CURATOR_EVENT_REPORT_CREATED,
            json!({
                "artifact_id": artifact.artifact_id,
                "report_id": report.run.report_id,
                "finding_count": report.finding_count,
                "conflict_count": conflict_report.conflict_count,
                "candidate_count": report.run.candidate_count,
                "preference_count": report.run.preference_count,
                "decision": report.decision,
                "reason_code": report.reason_code,
                "mutation_policy": report.run.mutation_policy,
            }),
        )
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "report": report,
        "conflict_report": conflict_report,
        "artifact": artifact,
        "contract": contract_descriptor(),
    })))
}

/// `GET /console/v1/memory/learning/candidates` — lists the caller's learning
/// candidates with optional filters and a lifecycle summary.
///
/// # Errors
/// Returns an error response when console authorization fails or the journal
/// query fails.
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

/// `GET /console/v1/memory/learning/candidates/{candidate_id}/history` —
/// returns a candidate (caller-scoped) with its review history and lifecycle
/// view.
///
/// # Errors
/// Returns a not-found response when the candidate does not exist for the
/// caller, and an error response when authorization or the journal query
/// fails.
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
    let evals = state
        .runtime
        .list_learning_candidate_evals(candidate.candidate_id.clone(), 64)
        .await
        .map_err(runtime_status_response)?;
    let rollouts = state
        .runtime
        .list_learning_candidate_rollouts(candidate.candidate_id.clone(), 64)
        .await
        .map_err(runtime_status_response)?;
    let lifecycle = learning_candidate_lifecycle(
        &candidate,
        history.as_slice(),
        evals.as_slice(),
        rollouts.as_slice(),
    );
    Ok(Json(json!({
        "candidate": candidate,
        "history": history,
        "evals": evals,
        "rollouts": rollouts,
        "lifecycle": lifecycle,
        "contract": contract_descriptor(),
    })))
}

/// `POST /console/v1/memory/learning/candidates/{candidate_id}/review` —
/// records a review decision for a candidate. Accepting a preference
/// candidate (or passing `apply_preference=true`) also applies it as a
/// learning preference.
///
/// # Errors
/// Returns a not-found response when the candidate does not exist for the
/// caller, an invalid-argument response for an unknown status, and an error
/// response when the review or preference application fails.
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
    let action_summary = payload.action_summary.and_then(trim_to_option);
    let action_payload_json = payload.action_payload_json.and_then(trim_to_option);
    let apply_preference = payload.apply_preference.unwrap_or(false);
    let reviewed = state
        .runtime
        .review_learning_candidate(journal::LearningCandidateReviewRequest {
            candidate_id: candidate.candidate_id.clone(),
            status,
            reviewed_by_principal: session.context.principal.clone(),
            action_summary: action_summary.clone(),
            action_payload_json: action_payload_json.clone(),
        })
        .await
        .map_err(runtime_status_response)?;
    if reviewed.status == "rolled-back" {
        let rollback_payload = action_payload_json
            .as_deref()
            .and_then(parse_json_object)
            .map(Value::Object)
            .unwrap_or_else(|| json!({}));
        state
            .runtime
            .record_learning_candidate_rollout(journal::LearningCandidateRolloutCreateRequest {
                rollout_id: None,
                candidate_id: reviewed.candidate_id.clone(),
                rollout_kind: reviewed.candidate_kind.clone(),
                state: "rollback".to_owned(),
                target_ref: reviewed
                    .target_path
                    .clone()
                    .unwrap_or_else(|| reviewed.candidate_id.clone()),
                previous_version_json: rollback_payload
                    .get("previous_version")
                    .or_else(|| rollback_payload.get("previous_state"))
                    .cloned()
                    .unwrap_or_else(|| json!({}))
                    .to_string(),
                activated_version_json: rollback_payload
                    .get("activated_version")
                    .or_else(|| rollback_payload.get("rolled_back_version"))
                    .cloned()
                    .unwrap_or_else(|| json!({}))
                    .to_string(),
                telemetry_json: json!({
                    "rollback_reason": action_summary,
                    "candidate_status": reviewed.status,
                })
                .to_string(),
                reason: action_summary
                    .clone()
                    .unwrap_or_else(|| "learning candidate rolled back".to_owned()),
                actor_principal: session.context.principal.clone(),
                policy_decision: "operator_rollback".to_owned(),
                evidence_refs_json: json!([{
                    "kind": "learning_candidate",
                    "ref": reviewed.candidate_id,
                }])
                .to_string(),
                rolled_back_at_unix_ms: reviewed.reviewed_at_unix_ms,
            })
            .await
            .map_err(runtime_status_response)?;
    }
    let applied_preference = if apply_preference
        || (reviewed.candidate_kind == "preference"
            && matches!(reviewed.status.as_str(), "accepted" | "approved" | "deployed"))
    {
        apply_preference_candidate(&state.runtime, &reviewed, session.context.principal.as_str())
            .await
            .map_err(runtime_status_response)?
    } else {
        None
    };
    let lifecycle = learning_candidate_lifecycle(&reviewed, &[], &[], &[]);
    Ok(Json(json!({
        "candidate": reviewed,
        "applied_preference": applied_preference,
        "lifecycle": lifecycle,
        "contract": contract_descriptor(),
    })))
}

/// `POST /console/v1/memory/learning/candidates/{candidate_id}/eval` —
/// appends an evaluation gate result for a learning candidate.
///
/// # Errors
/// Returns a not-found response when the candidate does not exist for the
/// caller, an invalid-argument response for malformed evidence JSON, and an
/// error response when authorization or journal writes fail.
pub(crate) async fn console_learning_candidate_eval_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(candidate_id): Path<String>,
    Json(payload): Json<ConsoleLearningCandidateEvalRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let candidate =
        load_console_learning_candidate(&state, &session.context, candidate_id.as_str()).await?;
    let evidence_refs_json =
        payload.evidence_refs_json.and_then(trim_to_option).unwrap_or_else(|| {
            json!([{
                "kind": "learning_candidate",
                "ref": candidate.candidate_id,
            }])
            .to_string()
        });
    serde_json::from_str::<Value>(evidence_refs_json.as_str()).map_err(|error| {
        runtime_status_response(tonic::Status::invalid_argument(format!(
            "evidence_refs_json must be valid JSON: {error}"
        )))
    })?;
    let eval = state
        .runtime
        .record_learning_candidate_eval(journal::LearningCandidateEvalCreateRequest {
            eval_id: None,
            candidate_id: candidate.candidate_id.clone(),
            eval_suite: payload.eval_suite,
            result: payload.result,
            threshold: payload.threshold,
            score: payload.score,
            decision: normalize_learning_eval_decision(payload.decision.as_str())
                .map_err(runtime_status_response)?,
            actor_principal: session.context.principal.clone(),
            policy_decision: payload
                .policy_decision
                .and_then(trim_to_option)
                .unwrap_or_else(|| "operator_recorded_eval_gate".to_owned()),
            evidence_refs_json,
        })
        .await
        .map_err(runtime_status_response)?;
    let evals = state
        .runtime
        .list_learning_candidate_evals(candidate.candidate_id.clone(), 64)
        .await
        .map_err(runtime_status_response)?;
    let rollouts = state
        .runtime
        .list_learning_candidate_rollouts(candidate.candidate_id.clone(), 64)
        .await
        .map_err(runtime_status_response)?;
    let history = state
        .runtime
        .learning_candidate_history(candidate.candidate_id.clone())
        .await
        .map_err(runtime_status_response)?;
    let lifecycle = learning_candidate_lifecycle(
        &candidate,
        history.as_slice(),
        evals.as_slice(),
        rollouts.as_slice(),
    );
    Ok(Json(json!({
        "candidate": candidate,
        "eval": eval,
        "evals": evals,
        "rollouts": rollouts,
        "lifecycle": lifecycle,
        "contract": contract_descriptor(),
    })))
}

/// `POST /console/v1/memory/learning/candidates/{candidate_id}/apply` —
/// applies a patch-based learning candidate and returns the resulting
/// candidate state plus apply outcome.
///
/// # Errors
/// Returns a not-found response when the candidate does not exist for the
/// caller, a failed-precondition response for non-patch candidates, and an
/// error response when authorization or the apply itself fails.
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

/// `GET /console/v1/memory/preferences` — lists the caller's learning
/// preferences with optional scope/status/key filters.
///
/// # Errors
/// Returns an error response when console authorization fails or the journal
/// query fails.
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

/// `GET /console/v1/memory/workspace/documents` — lists the caller's
/// workspace documents, optionally filtered by agent and path prefix.
///
/// # Errors
/// Returns an error response when console authorization fails or the journal
/// query fails.
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

/// `GET /console/v1/memory/workspace/document` — fetches one workspace
/// document by path within the caller's scope.
///
/// # Errors
/// Returns an invalid-argument response for an empty path, a not-found
/// response when no document matches, and an error response when
/// authorization or the journal query fails.
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

/// `POST /console/v1/memory/workspace/document` — creates or updates a
/// workspace document at the given path within the caller's scope.
///
/// # Errors
/// Returns an invalid-argument response for an empty path/content or a
/// non-ULID session id, and an error response when authorization or the
/// upsert fails.
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

/// `POST /console/v1/memory/workspace/document/move` — moves a workspace
/// document from `path` to `next_path` within the caller's scope.
///
/// # Errors
/// Returns an invalid-argument response for empty paths or a non-ULID
/// session id, and an error response when authorization or the move fails.
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

/// `POST /console/v1/memory/workspace/document/delete` — soft-deletes a
/// workspace document (history stays recoverable via versions).
///
/// # Errors
/// Returns an invalid-argument response for an empty path or a non-ULID
/// session id, and an error response when authorization or the delete fails.
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

/// `GET /console/v1/memory/workspace/document/versions` — returns a document
/// (including soft-deleted ones, so deleted history stays inspectable) and
/// its version history.
///
/// # Errors
/// Returns an invalid-argument response for an empty path, a not-found
/// response when no document matches, and an error response when
/// authorization or the journal query fails.
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

/// `POST /console/v1/memory/workspace/document/pin` — pins or unpins a
/// workspace document.
///
/// # Errors
/// Returns an invalid-argument response for an empty path, a not-found
/// response when no document matches, and an error response when
/// authorization or the update fails.
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

/// `POST /console/v1/memory/workspace/bootstrap` — seeds (or force-repairs)
/// the caller's curated workspace roots and templates.
///
/// # Errors
/// Returns an invalid-argument response for a non-ULID session id and an
/// error response when authorization or the bootstrap fails.
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

/// `GET /console/v1/memory/workspace/search` — searches the caller's
/// workspace documents with scoring diagnostics.
///
/// # Errors
/// Returns an error response when console authorization fails, the query is
/// empty, `min_score` is invalid, or the search fails.
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

/// `POST /console/v1/memory/recall/preview` — runs the dual-path recall
/// pipeline for a query (without writing durable memory), persists the
/// outcome as a recall artifact, and records a runtime decision event plus a
/// `memory.recall.preview` console event.
///
/// # Errors
/// Returns a failed-precondition response when the retrieval dual-path
/// preview capability is blocked, an invalid-argument response for bad
/// query/score/budget parameters, and an error response when the preview,
/// artifact write, or decision event fails.
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
    let recall_channel = payload.channel.or(session.context.channel.clone());
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

/// `GET /console/v1/memory/search-all` — federated search across memory,
/// workspace documents, and orchestrator sessions in one call; transcript
/// windows are included only when a `session_id` scope is given (a global
/// transcript scan would be too expensive for an interactive endpoint).
///
/// # Errors
/// Returns an error response when console authorization fails, the query is
/// empty, `min_score`/`session_id` are invalid, or any of the underlying
/// searches fail.
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
    let channel = query.channel.and_then(trim_to_option).or(session.context.channel.clone());
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
            channel: channel.clone(),
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
    let session_transcript_outcome = if session_scope.is_some() {
        Some(
            state
                .runtime
                .search_orchestrator_session_windows(SessionSearchRequest {
                    principal: session.context.principal.clone(),
                    device_id: session.context.device_id.clone(),
                    channel: channel.clone(),
                    session_id: session_scope.clone(),
                    exclude_session_id: None,
                    query: search_query.to_owned(),
                    top_k,
                    min_score,
                    window_before: 2,
                    window_after: 2,
                    max_windows_per_session: 3,
                    include_archived: false,
                })
                .await
                .map_err(runtime_status_response)?,
        )
    } else {
        None
    };
    let (session_transcript_hits, session_transcript_diagnostics) =
        if let Some(outcome) = session_transcript_outcome {
            (outcome.groups, Some(outcome.diagnostics))
        } else {
            (Vec::new(), None)
        };
    let session_transcript_count = session_transcript_hits.len();
    Ok(Json(json!({
        "query": search_query,
        "groups": {
            "sessions": session_hits,
            "session_transcripts": session_transcript_hits,
            "workspace": workspace_hits,
            "memory": memory_hits,
        },
        "counts": {
            "sessions": session_count,
            "session_transcripts": session_transcript_count,
            "workspace": workspace_hits.len(),
            "memory": memory_hits.len(),
        },
        "diagnostics": {
            "session_transcripts": session_transcript_diagnostics,
        },
        "contract": contract_descriptor(),
    })))
}

/// `GET /console/v1/memory/session-search` — searches transcript windows
/// across the caller's orchestrator sessions, persists the outcome as a
/// session-search recall artifact, and records a `memory.session_search`
/// console event.
///
/// # Errors
/// Returns an error response when console authorization fails, the query is
/// empty, `min_score` is invalid, or the search or artifact write fails.
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
            session_id: None,
            exclude_session_id: None,
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
                "source_refs_projection": session_search_source_refs_projection(&outcome),
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
        "synthesis": session_search_synthesis(&outcome),
        "source_refs": session_search_source_refs_projection(&outcome).source_refs,
        "source_refs_projection": session_search_source_refs_projection(&outcome),
        "diagnostics": outcome.diagnostics,
        "artifact": artifact,
        "contract": contract_descriptor(),
    })))
}

/// `GET /console/v1/memory/recall-artifacts` — lists the caller's stored
/// recall artifacts (full records, unlike the status inventory).
///
/// # Errors
/// Returns an invalid-argument response for a non-ULID session id and an
/// error response when authorization or the journal query fails.
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

/// Builds the journal artifact record for a recall preview, embedding the
/// full preview payload plus diagnostics and per-source provenance.
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
            "provider_usage": preview.structured_output.provider_usage,
            "synthesis_hash": preview.structured_output.synthesis_hash,
        }),
        provenance: recall_preview_provenance(preview),
        created_by_principal: context.principal.clone(),
    }
}

/// Builds the journal artifact record for a session search, embedding the
/// result groups, synthesis, diagnostics, and per-window provenance.
fn build_session_search_artifact_request(
    context: &RequestContext,
    channel: Option<String>,
    outcome: &SessionSearchOutcome,
) -> RecallArtifactCreateRequest {
    let source_refs_projection = session_search_source_refs_projection(outcome);
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
            "synthesis": session_search_synthesis(outcome),
            "source_refs": &source_refs_projection.source_refs,
            "source_refs_projection": &source_refs_projection,
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
            "synthesis_hash": session_search_synthesis_hash(outcome),
            "source_ref_count": source_refs_projection.source_ref_count,
            "source_refs_decision": source_refs_projection.decision,
        }),
        provenance: session_search_provenance(outcome, &source_refs_projection),
        created_by_principal: context.principal.clone(),
    }
}

fn build_learning_curator_artifact_request(
    context: &RequestContext,
    channel: Option<String>,
    report: &LearningCuratorReport,
    conflict_report: &PreferenceProcedureConflictReport,
) -> RecallArtifactCreateRequest {
    RecallArtifactCreateRequest {
        artifact_id: report.run.report_id.clone(),
        artifact_kind: RECALL_ARTIFACT_KIND_LEARNING_CURATOR_REPORT.to_owned(),
        principal: context.principal.clone(),
        device_id: context.device_id.clone(),
        channel,
        session_id: None,
        query: "learning curator report".to_owned(),
        summary: format!("{} learning curator finding(s)", report.finding_count),
        payload: json!({
            "report": report,
            "conflict_report": conflict_report,
            "durable_memory_write": false,
        }),
        diagnostics: json!({
            "event_type": report.event_type,
            "decision": report.decision,
            "reason_code": report.reason_code,
            "finding_count": report.finding_count,
            "conflict_count": conflict_report.conflict_count,
            "preference_conflict_count": conflict_report.preference_conflict_count,
            "procedure_conflict_count": conflict_report.procedure_conflict_count,
            "candidate_count": report.run.candidate_count,
            "preference_count": report.run.preference_count,
            "redaction_level": report.redaction_level,
            "mutation_policy": report.run.mutation_policy,
        }),
        provenance: json!({
            "source": "learning_curator",
            "event_type": report.event_type,
            "report_id": report.run.report_id,
            "evidence_refs": report
                .findings
                .iter()
                .flat_map(|finding| finding.evidence_refs.iter().cloned())
                .chain(
                    conflict_report
                        .conflicts
                        .iter()
                        .flat_map(|conflict| conflict.evidence_refs.iter().cloned()),
                )
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>(),
            "redaction_level": report.redaction_level,
        }),
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
        "synthesis_hash": preview.structured_output.synthesis_hash,
        "provider_usage": preview.structured_output.provider_usage,
        "source_refs": preview.structured_output.source_refs,
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

fn session_search_provenance(
    outcome: &SessionSearchOutcome,
    source_refs_projection: &crate::journal::SessionSearchUxSourceRefsProjection,
) -> Value {
    json!({
        "source": "session_search",
        "synthesis_hash": session_search_synthesis_hash(outcome),
        "provider_usage": session_search_provider_usage(outcome),
        "source_refs_projection": source_refs_projection,
        "source_refs": &source_refs_projection.source_refs,
        "windows": outcome
            .groups
            .iter()
            .flat_map(|group| group.windows.iter())
            .map(|window| json!(window.provenance))
            .collect::<Vec<_>>(),
    })
}

/// Builds the evidence-backed synthesis block for a session search: summary,
/// confidence, contradictions, and up to 12 citation-bearing evidence rows
/// (capped so synthesis stays prompt-budget friendly).
fn session_search_synthesis(outcome: &SessionSearchOutcome) -> Value {
    let source_refs_projection = session_search_source_refs_projection(outcome);
    let evidence = outcome
        .groups
        .iter()
        .flat_map(|group| group.windows.iter())
        .take(12)
        .enumerate()
        .map(|(index, window)| {
            json!({
                "evidence_id": format!("session-evidence-{}", index + 1),
                "source_kind": "transcript",
                "source_ref": &window.source_ref,
                "citation": {
                    "source_type": window.provenance.source_type,
                    "session_id": window.provenance.session_id,
                    "run_id": window.provenance.run_id,
                    "tape_seq": window.provenance.tape_seq,
                    "event_type": window.provenance.event_type,
                    "created_at_unix_ms": window.provenance.created_at_unix_ms,
                },
                "snippet": window.snippet,
                "score": window.score,
            })
        })
        .collect::<Vec<_>>();
    let summary = if evidence.is_empty() {
        format!(
            "No evidence-backed session recall summary is available for query '{}'.",
            outcome.query
        )
    } else {
        format!(
            "Evidence-backed session recall for '{}': {} group(s), {} bounded window(s).",
            outcome.query,
            outcome.groups.len(),
            evidence.len()
        )
    };
    json!({
        "summary": summary,
        "confidence": session_search_confidence(outcome),
        "unresolved": if evidence.is_empty() {
            vec![format!("No session windows matched '{}'.", outcome.query)]
        } else {
            Vec::<String>::new()
        },
        "contradictions": session_search_contradictions(outcome),
        "evidence": evidence,
        "source_refs": &source_refs_projection.source_refs,
        "source_refs_projection": &source_refs_projection,
        "provider_usage": session_search_provider_usage(outcome),
        "synthesis_hash": session_search_synthesis_hash(outcome),
    })
}

fn session_search_provider_usage(outcome: &SessionSearchOutcome) -> Value {
    let evidence_count = outcome.groups.iter().map(|group| group.windows.len()).sum::<usize>();
    json!([{
        "provider_id": "session_history",
        "source_kind": "transcript",
        "evidence_count": evidence_count,
    }])
}

/// Confidence is the mean score of the first three windows in result order
/// (results arrive ranked, so this approximates top-3 strength); `None` when
/// nothing matched.
fn session_search_confidence(outcome: &SessionSearchOutcome) -> Option<f64> {
    let scores = outcome
        .groups
        .iter()
        .flat_map(|group| group.windows.iter().map(|window| window.score))
        .take(3)
        .collect::<Vec<_>>();
    if scores.is_empty() {
        return None;
    }
    let total = scores.iter().sum::<f64>();
    Some(total / scores.len() as f64)
}

/// Flags potential contradictions across the matched snippets.
///
/// Deliberately a coarse lexical heuristic (both halves of an antonym pair
/// appearing anywhere in the joined evidence): it can only over-warn, and the
/// operator reviews the cited windows either way. No model call is involved.
fn session_search_contradictions(outcome: &SessionSearchOutcome) -> Vec<String> {
    const CONTRADICTION_PAIRS: &[(&str, &str)] = &[
        ("enable", "disable"),
        ("allow", "deny"),
        ("must", "must not"),
        ("use", "avoid"),
        ("keep", "remove"),
        ("public", "private"),
    ];
    let joined = outcome
        .groups
        .iter()
        .flat_map(|group| group.windows.iter())
        .map(|window| window.snippet.to_ascii_lowercase())
        .collect::<Vec<_>>()
        .join("\n");
    CONTRADICTION_PAIRS
        .iter()
        .filter(|(left, right)| joined.contains(left) && joined.contains(right))
        .map(|(left, right)| format!("Evidence contains both '{left}' and '{right}'."))
        .collect()
}

/// Stable SHA-256 over the query plus every (session, window, snippet)
/// triple, used as a provenance fingerprint to tie artifacts to the exact
/// evidence set they summarized.
fn session_search_synthesis_hash(outcome: &SessionSearchOutcome) -> String {
    let mut hasher = Sha256::new();
    hasher.update(outcome.query.as_bytes());
    for group in &outcome.groups {
        hasher.update(b"\nsession:");
        hasher.update(group.session.session_id.as_bytes());
        for window in &group.windows {
            hasher.update(b"\nwindow:");
            hasher.update(window.window_id.as_bytes());
            hasher.update(b":");
            hasher.update(window.snippet.as_bytes());
        }
    }
    hex::encode(hasher.finalize())
}

/// Console-event payload for a recall preview, annotated with the persisted
/// artifact id so the event links back to the stored artifact.
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

/// Runs one memory maintenance pass immediately with the currently
/// configured retention policy, scheduling the next run one interval out.
///
/// # Errors
/// Returns the mapped runtime status response when the status snapshot or
/// the maintenance run fails.
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

/// Normalizes a review status into its stored form, accepting verb forms
/// ("approve"), legacy synonyms, and `_`/`-` spelling variants so older
/// clients keep working.
///
/// # Errors
/// Returns `invalid_argument` for statuses outside the supported set.
fn normalize_learning_candidate_review_status(status: &str) -> Result<String, tonic::Status> {
    let normalized = status.trim().to_ascii_lowercase().replace('_', "-");
    let accepted = match normalized.as_str() {
        "proposed" | "queued" => normalized,
        "needs-review" | "review" | "pending-review" => "needs-review".to_owned(),
        "eval" | "evaluating" => "eval".to_owned(),
        "eval-passed" | "eval-passed-review" => "eval_passed".to_owned(),
        "eval-failed" | "eval-failed-review" => "eval_failed".to_owned(),
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

fn normalize_learning_eval_decision(decision: &str) -> Result<String, tonic::Status> {
    let normalized = decision.trim().to_ascii_lowercase().replace('_', "-");
    match normalized.as_str() {
        "pass" | "passed" | "approve" | "approved" => Ok("pass".to_owned()),
        "fail" | "failed" | "reject" | "rejected" => Ok("fail".to_owned()),
        "hold" | "review" | "needs-review" => Ok("hold".to_owned()),
        _ => Err(tonic::Status::invalid_argument("eval decision must be pass, fail, or hold")),
    }
}

/// Aggregates a candidate list into per-status counts, injection-conflict
/// totals, and the static review/deployment policy the console renders.
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

/// Builds the full lifecycle view for one candidate: canonical status, scope,
/// evidence, proposed change, deployment posture, and rollback availability
/// (derived from both the current status and the review history).
fn learning_candidate_lifecycle(
    candidate: &journal::LearningCandidateRecord,
    history: &[journal::LearningCandidateHistoryRecord],
    evals: &[journal::LearningCandidateEvalRecord],
    rollouts: &[journal::LearningCandidateRolloutRecord],
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
        "state_machine": learning_candidate_state_machine(candidate, history, evals, rollouts),
        "skill_invocation_hygiene": project_skill_invocation_hygiene_for_candidate(candidate),
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

fn learning_candidate_state_machine(
    candidate: &journal::LearningCandidateRecord,
    history: &[journal::LearningCandidateHistoryRecord],
    evals: &[journal::LearningCandidateEvalRecord],
    rollouts: &[journal::LearningCandidateRolloutRecord],
) -> Value {
    let requires_review = learning_candidate_requires_review(candidate);
    let requires_eval = learning_candidate_requires_eval(candidate);
    let eval_passed =
        evals.iter().any(|eval| eval.decision == "pass" && eval.score >= eval.threshold);
    let reviewed = matches!(
        candidate.status.as_str(),
        "approved" | "accepted" | "eval_passed" | "applied" | "deployed"
    );
    let current_state = learning_candidate_state_for_record(candidate, evals, rollouts);
    let mut transitions = Vec::new();
    transitions.push(json!({
        "state": "observation",
        "actor": "learning_pipeline",
        "policy_decision": "candidate_observed",
        "evidence_refs": learning_candidate_default_evidence_refs(candidate),
        "timestamp_unix_ms": candidate.created_at_unix_ms,
    }));
    transitions.push(json!({
        "state": "candidate",
        "actor": "learning_pipeline",
        "policy_decision": if requires_review { "review_required" } else { "review_optional" },
        "evidence_refs": learning_candidate_default_evidence_refs(candidate),
        "timestamp_unix_ms": candidate.created_at_unix_ms,
    }));
    if requires_review {
        transitions.push(json!({
            "state": "review",
            "actor": candidate
                .reviewed_by_principal
                .as_deref()
                .unwrap_or("operator_required"),
            "policy_decision": "operator_review_required",
            "evidence_refs": learning_candidate_default_evidence_refs(candidate),
            "timestamp_unix_ms": candidate.reviewed_at_unix_ms.unwrap_or(candidate.updated_at_unix_ms),
        }));
    }
    for entry in history.iter().rev() {
        transitions.push(json!({
            "state": learning_state_from_candidate_status(entry.status.as_str()),
            "actor": entry.reviewed_by_principal,
            "policy_decision": learning_policy_from_action_payload(entry.action_payload_json.as_deref())
                .unwrap_or_else(|| "operator_review".to_owned()),
            "evidence_refs": learning_evidence_refs_from_payload(entry.action_payload_json.as_deref())
                .unwrap_or_else(|| learning_candidate_default_evidence_refs(candidate)),
            "timestamp_unix_ms": entry.created_at_unix_ms,
            "stored_status": entry.status,
        }));
    }
    for eval in evals.iter().rev() {
        transitions.push(json!({
            "state": "eval",
            "actor": eval.actor_principal,
            "policy_decision": eval.policy_decision,
            "evidence_refs": learning_parse_json_or_default(
                eval.evidence_refs_json.as_str(),
                learning_candidate_default_evidence_refs(candidate),
            ),
            "timestamp_unix_ms": eval.created_at_unix_ms,
            "eval": {
                "eval_id": eval.eval_id,
                "suite": eval.eval_suite,
                "result": eval.result,
                "threshold": eval.threshold,
                "score": eval.score,
                "decision": eval.decision,
            },
        }));
    }
    for rollout in rollouts.iter().rev() {
        transitions.push(json!({
            "state": rollout.state,
            "actor": rollout.actor_principal,
            "policy_decision": rollout.policy_decision,
            "evidence_refs": learning_parse_json_or_default(
                rollout.evidence_refs_json.as_str(),
                learning_candidate_default_evidence_refs(candidate),
            ),
            "timestamp_unix_ms": rollout.created_at_unix_ms,
            "rollout": {
                "rollout_id": rollout.rollout_id,
                "kind": rollout.rollout_kind,
                "target_ref": rollout.target_ref,
                "telemetry": learning_parse_json_or_default(rollout.telemetry_json.as_str(), json!({})),
                "rolled_back_at_unix_ms": rollout.rolled_back_at_unix_ms,
            },
        }));
    }
    json!({
        "states": [
            "observation",
            "candidate",
            "review",
            "eval",
            "package",
            "activation",
            "monitoring",
            "rollback",
            "retirement"
        ],
        "current_state": current_state,
        "candidate_kind": learning_candidate_state_machine_kind(candidate.candidate_kind.as_str()),
        "requires_review": requires_review,
        "requires_eval": requires_eval,
        "eval_passed": eval_passed,
        "gates_satisfied": (!requires_review || reviewed) && (!requires_eval || eval_passed),
        "activation_blocked_reason": learning_activation_blocked_reason(
            requires_review,
            reviewed,
            requires_eval,
            eval_passed,
        ),
        "transitions": transitions,
    })
}

fn learning_candidate_requires_review(candidate: &journal::LearningCandidateRecord) -> bool {
    learning_candidate_requires_eval(candidate)
        || !matches!(candidate.candidate_kind.as_str(), "durable_fact")
        || candidate.confidence < 0.95
}

fn learning_candidate_requires_eval(candidate: &journal::LearningCandidateRecord) -> bool {
    matches!(
        candidate.candidate_kind.as_str(),
        "patch_skill" | "patch_procedure" | "write_support_file"
    ) || matches!(
        candidate.risk_level.trim().to_ascii_lowercase().as_str(),
        "high" | "review" | "sensitive" | "poisoned" | "blocked_sensitive" | "blocked_poisoned"
    )
}

fn learning_candidate_state_for_record(
    candidate: &journal::LearningCandidateRecord,
    evals: &[journal::LearningCandidateEvalRecord],
    rollouts: &[journal::LearningCandidateRolloutRecord],
) -> &'static str {
    let shadow = shadow_learning_candidate_lifecycle(candidate, current_unix_ms());
    if shadow.expired {
        return "retirement";
    }
    if shadow.shadow_write {
        return "candidate";
    }
    if rollouts.iter().any(|rollout| rollout.state == "rollback") {
        return "rollback";
    }
    if rollouts.iter().any(|rollout| rollout.state == "activation") {
        return "monitoring";
    }
    if !evals.is_empty() {
        return "eval";
    }
    learning_state_from_candidate_status(candidate.status.as_str())
}

fn learning_state_from_candidate_status(status: &str) -> &'static str {
    match status.trim().to_ascii_lowercase().replace('_', "-").as_str() {
        "" | "queued" | "proposed" => "candidate",
        "needs-review" | "review" | "pending-review" => "review",
        "eval" | "eval-passed" | "eval-failed" => "eval",
        "approved" | "accepted" => "package",
        "applied" | "auto-applied" | "deployed" => "activation",
        "rollback" | "rolled-back" => "rollback",
        "rejected" | "denied" | "suppressed" | "conflicted" => "retirement",
        _ => "candidate",
    }
}

fn learning_candidate_state_machine_kind(candidate_kind: &str) -> &'static str {
    match candidate_kind {
        "durable_fact" => "durable_fact",
        "preference" => "preference",
        "patch_skill" => "patch_skill",
        "patch_procedure" => "patch_procedure",
        "write_support_file" => "support_file",
        "commitment_observation" => "commitment_observation",
        _ => "durable_fact",
    }
}

fn learning_activation_blocked_reason(
    requires_review: bool,
    reviewed: bool,
    requires_eval: bool,
    eval_passed: bool,
) -> Option<&'static str> {
    if requires_review && !reviewed {
        return Some("review_required");
    }
    if requires_eval && !eval_passed {
        return Some("passing_eval_required");
    }
    None
}

fn learning_candidate_default_evidence_refs(candidate: &journal::LearningCandidateRecord) -> Value {
    let mut refs = vec![json!({
        "kind": "learning_candidate",
        "ref": candidate.candidate_id,
    })];
    if let Some(source_task_id) = candidate.source_task_id.as_deref() {
        refs.push(json!({
            "kind": "background_task",
            "ref": source_task_id,
        }));
    }
    refs.push(json!({
        "kind": "provenance_hash",
        "sha256": crate::sha256_hex(candidate.provenance_json.as_bytes()),
    }));
    json!(refs)
}

fn learning_policy_from_action_payload(payload_json: Option<&str>) -> Option<String> {
    let payload = payload_json.and_then(parse_json_object)?;
    payload.get("policy_decision").and_then(Value::as_str).map(ToOwned::to_owned)
}

fn learning_evidence_refs_from_payload(payload_json: Option<&str>) -> Option<Value> {
    let payload = payload_json.and_then(parse_json_object)?;
    payload.get("evidence_refs").cloned()
}

fn learning_parse_json_or_default(raw: &str, fallback: Value) -> Value {
    serde_json::from_str::<Value>(raw).unwrap_or(fallback)
}

/// Lifecycle view variant for the apply endpoint, where the candidate is
/// already JSON (returned by the apply pipeline) rather than a journal
/// record.
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

/// Canonical lifecycle status for a stored candidate. A recorded
/// apply-action payload counts as deployment even when the stored status
/// lags behind it.
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

/// Collapses stored/legacy status spellings onto the six canonical lifecycle
/// labels the console contract exposes; anything unrecognized degrades to
/// "proposed" rather than failing the whole listing.
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
        "eval" | "eval-passed" | "eval-failed" => "needs-review",
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

/// Heuristic substring scan for prompt-injection markers across a
/// candidate's risk level, text, content, and provenance. Safety analysis
/// upstream tags these fields; this only surfaces the tag in the lifecycle
/// summary so reviewers are pointed at suspect candidates.
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

/// Parses a string as a JSON object, returning `None` for invalid JSON or
/// non-object values.
fn parse_json_object(payload: &str) -> Option<serde_json::Map<String, Value>> {
    serde_json::from_str::<Value>(payload).ok()?.as_object().cloned()
}

/// Loads one learning candidate scoped to the caller's principal and
/// channel. Scoping is part of the access check: another principal's
/// candidate id yields the same not-found as a nonexistent one, so ids do
/// not leak across users.
///
/// # Errors
/// Returns a not-found response when no matching candidate exists and the
/// mapped runtime status response when the journal query fails.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recall_artifact_inventory_omits_deep_payloads() {
        let artifact = RecallArtifactRecord {
            artifact_id: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned(),
            artifact_kind: RECALL_ARTIFACT_KIND_SESSION_SEARCH.to_owned(),
            principal: "operator".to_owned(),
            device_id: "device".to_owned(),
            channel: Some("cli".to_owned()),
            session_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned()),
            query: "PALYRA_E2E_BETA".to_owned(),
            summary: "session search matched the feature flag".to_owned(),
            payload: json!({
                "groups": [{
                    "windows": [{
                        "transcript": "large transcript body should require explicit artifact detail"
                    }]
                }]
            }),
            diagnostics: json!({
                "scoring": {
                    "breakdown": "large diagnostic body should not be in memory status"
                }
            }),
            provenance: json!({
                "prompt_preview": "large prompt preview should not be in memory status"
            }),
            created_by_principal: "operator".to_owned(),
            created_at_unix_ms: 1_700_000_000_000,
        };

        let inventory = recall_artifact_inventory_json(&artifact);
        let encoded = inventory.to_string();

        assert_eq!(inventory["artifact_id"], "01ARZ3NDEKTSV4RRFFQ69G5FAW");
        assert_eq!(inventory["artifact_kind"], RECALL_ARTIFACT_KIND_SESSION_SEARCH);
        assert_eq!(inventory["query"], "PALYRA_E2E_BETA");
        assert_eq!(inventory["payload_available"], true);
        assert_eq!(inventory["diagnostics_available"], true);
        assert_eq!(inventory["provenance_available"], true);
        assert!(!encoded.contains("large transcript body"), "{encoded}");
        assert!(!encoded.contains("large diagnostic body"), "{encoded}");
        assert!(!encoded.contains("large prompt preview"), "{encoded}");
        assert!(inventory.get("payload").is_none());
        assert!(inventory.get("diagnostics").is_none());
        assert!(inventory.get("provenance").is_none());
    }

    #[test]
    fn memory_index_batch_budget_defaults_and_clamps() {
        assert_eq!(
            memory_index_batch_budget(None),
            MemoryIndexBatchBudget {
                requested_cancel_after_batches: None,
                max_batches_per_request: MEMORY_INDEX_DEFAULT_MAX_BATCHES_PER_REQUEST,
            }
        );
        assert_eq!(
            memory_index_batch_budget(Some(2)),
            MemoryIndexBatchBudget {
                requested_cancel_after_batches: Some(2),
                max_batches_per_request: 2,
            }
        );
        assert_eq!(
            memory_index_batch_budget(Some(u64::MAX)),
            MemoryIndexBatchBudget {
                requested_cancel_after_batches: Some(u64::MAX),
                max_batches_per_request: MEMORY_INDEX_HARD_MAX_BATCHES_PER_REQUEST,
            }
        );
        assert_eq!(
            memory_index_batch_budget(Some(0)),
            MemoryIndexBatchBudget {
                requested_cancel_after_batches: None,
                max_batches_per_request: MEMORY_INDEX_DEFAULT_MAX_BATCHES_PER_REQUEST,
            }
        );
    }

    #[test]
    fn console_memory_index_guard_is_single_flight() {
        let active = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let guard = match try_acquire_console_memory_index_guard(&active) {
            Ok(guard) => guard,
            Err(_) => panic!("first memory index guard acquisition should succeed"),
        };

        assert!(
            try_acquire_console_memory_index_guard(&active).is_err(),
            "second memory index guard acquisition must fail while first guard is held"
        );

        drop(guard);
        assert!(
            try_acquire_console_memory_index_guard(&active).is_ok(),
            "memory index guard should be released on drop"
        );
    }

    #[test]
    fn external_indexer_payload_reports_batch_limit() {
        let outcome = crate::retrieval::ExternalRetrievalIndexerOutcome {
            ran_at_unix_ms: 1,
            batch_size: 64,
            attempt_count: 1,
            indexed_memory_items: 2,
            indexed_workspace_chunks: 3,
            pending_memory_items: 4,
            pending_workspace_chunks: 5,
            journal_watermark_unix_ms: 6,
            checkpoint_committed: true,
            complete: false,
            retry_policy: "none".to_owned(),
        };

        let payload = external_indexer_payload(&outcome, 8, 8, true);

        assert_eq!(payload["indexed_memory_items"], 2);
        assert_eq!(payload["batches_executed"], 8);
        assert_eq!(payload["max_batches_per_request"], 8);
        assert_eq!(payload["batch_limit_reached"], true);
        assert_eq!(payload["cancelled"], true);
        assert_eq!(payload["cancel_reason"], MEMORY_INDEX_BATCH_LIMIT_REASON);
    }
}
