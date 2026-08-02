//! Console handlers for external retrieval-index drift inspection and
//! reconciliation: `GET /console/v1/memory/index/drift` and
//! `POST /console/v1/memory/index/reconcile`.
//!
//! The journal is the memory source of truth; the external retrieval index
//! is derived from it and can drift. These endpoints expose that drift and
//! drive reconciliation, and [`build_memory_retrieval_diagnostics`] is shared
//! with the sibling `memory` module's status endpoint so both surfaces
//! describe retrieval health identically.

use crate::*;
use serde_json::{json, Value};

/// `GET /console/v1/memory/index/drift` — previews drift between the journal
/// and the external retrieval index without mutating anything.
///
/// # Errors
/// Returns an error response when console authorization fails or the drift
/// preview or backend snapshot fails.
pub(crate) async fn console_memory_index_drift_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let drift =
        state.runtime.preview_external_retrieval_drift().await.map_err(runtime_status_response)?;
    let retrieval_backend =
        state.runtime.retrieval_backend_snapshot().map_err(runtime_status_response)?;
    let diagnostics = build_memory_index_diagnostics(&retrieval_backend, &drift);
    let health = drift.health.clone();
    Ok(Json(json!({
        "drift": drift,
        "diagnostics": diagnostics,
        "health": health,
        "external_index": retrieval_backend.external_index.clone(),
        "retrieval": {
            "backend": retrieval_backend,
        },
        "contract": contract_descriptor(),
    })))
}

/// `POST /console/v1/memory/index/reconcile` — reconciles the external
/// retrieval index against the journal in batches (clamped to 1..=10000)
/// and records a `memory.index.reconcile` console event.
///
/// # Errors
/// Returns an error response when console authorization fails or the
/// reconciliation or backend snapshot fails. A failed audit-event write is
/// only logged: the reconciliation already happened, so the outcome is still
/// returned to the operator.
pub(crate) async fn console_memory_index_reconcile_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleMemoryIndexRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let batch_size = payload.batch_size.unwrap_or(256).clamp(1, 10_000);
    let reconciliation = state
        .runtime
        .reconcile_external_retrieval_index(batch_size)
        .await
        .map_err(runtime_status_response)?;
    let retrieval_backend =
        state.runtime.retrieval_backend_snapshot().map_err(runtime_status_response)?;
    let diagnostics =
        build_memory_index_diagnostics(&retrieval_backend, &reconciliation.drift_after);
    let health = reconciliation.drift_after.health.clone();
    let event_details = json!({
        "batch_size": batch_size,
        "reconciliation": reconciliation.clone(),
        "reason_codes": health.reason_codes.clone(),
        "redaction_level": health.redaction_level.clone(),
    });
    if let Err(error) = state
        .runtime
        .record_console_event(&session.context, "memory.index.reconcile", event_details)
        .await
    {
        tracing::warn!(error = %error, "failed to record memory index reconciliation console event");
    }
    Ok(Json(json!({
        "reconciliation": reconciliation,
        "diagnostics": diagnostics,
        "health": health,
        "external_index": retrieval_backend.external_index.clone(),
        "retrieval": {
            "backend": retrieval_backend,
        },
        "contract": contract_descriptor(),
    })))
}

fn build_memory_index_diagnostics(
    retrieval_backend: &crate::retrieval::RetrievalBackendSnapshot,
    drift: &crate::retrieval::ExternalRetrievalDriftReport,
) -> Value {
    let mut diagnostics = build_memory_retrieval_diagnostics(
        retrieval_backend,
        Some(drift.drift_count),
        Some(drift.reconciliation_required),
    );
    if let Some(object) = diagnostics.as_object_mut() {
        object.insert("health_state".to_owned(), json!(drift.health.state));
        object.insert("reason_codes".to_owned(), json!(drift.health.reason_codes));
        object.insert("vector_available".to_owned(), json!(drift.health.vector_available));
        object.insert("vector_reason_code".to_owned(), json!(drift.health.vector_reason_code));
        object.insert("rebuild_checkpoint".to_owned(), json!(drift.health.rebuild_checkpoint));
        object.insert("redaction_level".to_owned(), json!(drift.health.redaction_level));
    }
    diagnostics
}

/// Builds the operator-facing retrieval diagnostics block (mode, fallback
/// availability, drift, recommended command, operator note).
///
/// `drift_count`/`reconciliation_required` override the snapshot's own
/// values when the caller just measured fresher numbers (drift preview or a
/// completed reconciliation); pass `None` to fall back to the snapshot. The
/// mode labels and operator notes are part of the console wire contract and
/// are pinned by unit tests below.
pub(crate) fn build_memory_retrieval_diagnostics(
    retrieval_backend: &crate::retrieval::RetrievalBackendSnapshot,
    drift_count: Option<u64>,
    reconciliation_required: Option<bool>,
) -> Value {
    let fallback_available = retrieval_backend.capabilities.journal_fallback;
    let external_index = retrieval_backend.external_index.as_ref();
    let drift_count =
        drift_count.or_else(|| external_index.map(|snapshot| snapshot.drift_count)).unwrap_or(0);
    let reconciliation_required = reconciliation_required.unwrap_or(drift_count > 0);
    // Mode precedence: stale index (with or without fallback) outranks a
    // healthy external index, which outranks fallback-only and plain journal
    // modes -- staleness must never be masked by a "ready" label.
    let external_state = external_index
        .map(|snapshot| match snapshot.state {
            crate::retrieval::RetrievalBackendState::Ready => "ready",
            crate::retrieval::RetrievalBackendState::Degraded => "degraded",
        })
        .unwrap_or("not_configured");
    let retrieval_mode = if reconciliation_required && fallback_available {
        "degraded_journal_fallback"
    } else if reconciliation_required {
        "stale_external_index"
    } else if external_state == "ready" {
        "external_index_ready"
    } else if fallback_available {
        "journal_fallback"
    } else {
        "journal_source_of_truth"
    };
    let operator_note = if reconciliation_required && fallback_available {
        "Search and recall can still return results from the journal fallback while the external derived index is stale; run `palyra memory index-reconcile --json` to clear drift."
    } else if reconciliation_required {
        "The external derived index is stale and should be reconciled with `palyra memory index-reconcile --json` before trusting index freshness."
    } else if fallback_available && external_state == "degraded" {
        "The external derived index is degraded; search remains queryable through journal fallback, but external-index freshness is not current."
    } else {
        "Retrieval index diagnostics do not currently require reconciliation."
    };
    json!({
        "retrieval_mode": retrieval_mode,
        "external_index_state": external_state,
        "fallback_available": fallback_available,
        "search_surface": if fallback_available { "queryable_with_journal_fallback" } else { "primary_index_only" },
        "drift_count": drift_count,
        "reconciliation_required": reconciliation_required,
        "recommended_command": if reconciliation_required { Some("palyra memory index-reconcile --json") } else { None },
        "operator_note": operator_note,
    })
}

#[cfg(test)]
mod tests {
    use super::build_memory_retrieval_diagnostics;
    use crate::retrieval::{
        retrieval_externalization_policy, ExternalRetrievalIndexSnapshot,
        ExternalRetrievalScaleSloSnapshot, RetrievalBackendCapabilities, RetrievalBackendKind,
        RetrievalBackendSnapshot, RetrievalBackendState,
    };

    fn retrieval_backend_with_fallback() -> RetrievalBackendSnapshot {
        RetrievalBackendSnapshot {
            kind: RetrievalBackendKind::ExternalDerivedPreview,
            state: RetrievalBackendState::Ready,
            reason: "external index with journal fallback".to_owned(),
            capabilities: RetrievalBackendCapabilities {
                lexical_search: true,
                vector_search: true,
                lexical_candidate_generation: true,
                vector_candidate_generation: true,
                hybrid_fusion: true,
                source_aware_fusion: true,
                branch_diagnostics: true,
                transcript_fusion: true,
                checkpoint_fusion: true,
                compaction_fusion: true,
                lazy_reindex: true,
                batch_backfill: true,
                external_derived_index: true,
                journal_source_of_truth: true,
                journal_fallback: true,
                drift_detection: true,
                async_indexer: true,
                scale_slos: true,
            },
            externalization: retrieval_externalization_policy(),
            external_index: Some(ExternalRetrievalIndexSnapshot {
                provider: "local".to_owned(),
                state: RetrievalBackendState::Ready,
                reason: "ready".to_owned(),
                indexed_memory_items: 0,
                indexed_workspace_chunks: 0,
                last_indexed_at_unix_ms: None,
                journal_watermark_unix_ms: None,
                freshness_lag_ms: None,
                drift_count: 21,
                pending_reconciliation_count: 21,
                scale_slos: ExternalRetrievalScaleSloSnapshot::default(),
                last_error: None,
            }),
        }
    }

    #[test]
    fn drift_diagnostics_explain_journal_fallback_mode() {
        let diagnostics = build_memory_retrieval_diagnostics(
            &retrieval_backend_with_fallback(),
            Some(21),
            Some(true),
        );

        assert_eq!(
            diagnostics.get("retrieval_mode").and_then(serde_json::Value::as_str),
            Some("degraded_journal_fallback")
        );
        assert_eq!(
            diagnostics.get("fallback_available").and_then(serde_json::Value::as_bool),
            Some(true)
        );
        assert!(
            diagnostics
                .get("operator_note")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|note| note.contains("journal fallback")
                    && note.contains("memory index-reconcile")),
            "drift diagnostics should explain why search can work while reconciliation is required: {diagnostics}"
        );
    }
}
