//! Authorized semantic-memory proposal, review, lifecycle, and retrieval endpoints.

use axum::{
    extract::{Path, Query, State},
    http::HeaderMap,
    response::Response,
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use ulid::Ulid;

use crate::{
    app::state::AppState,
    application::semantic_memory::{
        SemanticMemoryCandidateDraftV1, SemanticMemoryConsolidationPolicy,
        SemanticMemoryEvidenceRefV1, SemanticMemoryQualityEvalCaseV1,
        SemanticMemoryRetrievalFeedbackV1,
    },
    gateway::current_unix_ms,
    journal::{
        semantic_memory::{
            semantic_memory_approval_request, SemanticMemoryReviewAuthority,
            SemanticMemoryTargetScope,
        },
        JournalError, MemorySearchRequest, OrchestratorSessionResolveRequest,
    },
    runtime_status_response,
    transport::http::handlers::console::diagnostics::authorize_console_session,
};

const SEMANTIC_MEMORY_MAX_AGE_MS: i64 = 30 * 24 * 60 * 60 * 1_000;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SemanticMemoryProposalRequest {
    memory_id: String,
    candidate_id: String,
    summary_text: String,
    evidence_refs: Vec<SemanticMemoryEvidenceRefV1>,
    retention_expires_at_unix_ms: Option<i64>,
    eval_cases: Vec<SemanticMemoryQualityEvalCaseV1>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SemanticMemoryActivationRequest {
    candidate_id: String,
    approval_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SemanticMemoryRollbackProposalRequest {
    memory_id: String,
    target_record_sha256: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SemanticMemoryRollbackRequest {
    memory_id: String,
    target_record_sha256: String,
    approval_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SemanticMemoryFeedbackRequest {
    useful: bool,
    corrected: bool,
    correction_evidence_ref: Option<SemanticMemoryEvidenceRefV1>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SemanticMemorySearchQuery {
    query: String,
    top_k: Option<usize>,
    min_score: Option<f64>,
}

/// Creates an inert evaluated proposal and the exact standard host approval.
pub(crate) async fn console_semantic_memory_proposal_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<SemanticMemoryProposalRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    require_rollout(&state)?;
    let now = current_unix_ms();
    let authority = new_review_authority(&state, &session.context).map_err(journal_response)?;
    let target_scope = target_scope(&session.context);
    let proposed = state
        .runtime
        .journal_store
        .propose_semantic_memory(
            payload.memory_id.as_str(),
            SemanticMemoryCandidateDraftV1 {
                candidate_id: payload.candidate_id,
                summary_text: payload.summary_text,
                evidence_refs: payload.evidence_refs,
                retention_expires_at_unix_ms: payload.retention_expires_at_unix_ms,
                created_at_unix_ms: now,
            },
            payload.eval_cases.as_slice(),
            &SemanticMemoryConsolidationPolicy {
                enabled: true,
                ..SemanticMemoryConsolidationPolicy::default()
            },
            &target_scope,
            &authority,
        )
        .map_err(journal_response)?;
    let approval = state
        .runtime
        .create_approval_record(semantic_memory_approval_request(
            Ulid::new().to_string(),
            &proposed,
            &authority,
        ))
        .await
        .map_err(internal_status)?;
    record_lifecycle_event(
        &state,
        &session.context,
        "semantic_memory.proposed",
        &proposed,
        Some(approval.approval_id.as_str()),
    )
    .await;
    Ok(Json(json!({
        "schema_version": 1,
        "state": "proposed",
        "reason_code": proposed.reason_code,
        "approval_id": approval.approval_id,
        "approval_subject_id": proposed.approval_subject_id,
        "candidate_sha256": proposed.candidate_sha256,
        "eval_sha256": proposed.candidate.quality_eval.evidence_sha256,
        "approval_generation": proposed.context.approval_generation,
        "candidate": proposed.candidate,
    })))
}

/// Returns the exact sanitized candidate projection an operator is reviewing.
pub(crate) async fn console_semantic_memory_proposal_detail_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(candidate_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    require_rollout(&state)?;
    let proposed = state
        .runtime
        .journal_store
        .semantic_memory_proposal(candidate_id.as_str(), &target_scope(&session.context))
        .map_err(journal_response)?
        .ok_or_else(|| failed_precondition("semantic_memory.proposal_missing_or_stale"))?;
    Ok(Json(json!({
        "schema_version": 1,
        "state": "proposed",
        "reason_code": proposed.reason_code,
        "approval_subject_id": proposed.approval_subject_id,
        "candidate_sha256": proposed.candidate_sha256,
        "eval_sha256": proposed.candidate.quality_eval.evidence_sha256,
        "approval_generation": proposed.context.approval_generation,
        "candidate": proposed.candidate,
    })))
}

/// Atomically consumes review and publishes a normal searchable MemoryItem.
pub(crate) async fn console_semantic_memory_activation_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<SemanticMemoryActivationRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    require_rollout(&state)?;
    let approval = state
        .runtime
        .approval_record(payload.approval_id.clone())
        .await
        .map_err(internal_status)?
        .ok_or_else(|| failed_precondition("semantic_memory.host_approval_missing_or_stale"))?;
    if approval.principal != session.context.principal
        || approval.device_id != session.context.device_id
        || approval.channel != session.context.channel
    {
        return Err(failed_precondition("semantic_memory.host_approval_principal_mismatch"));
    }
    let authority = SemanticMemoryReviewAuthority {
        session_id: approval.session_id,
        run_id: approval.run_id,
        principal: approval.principal,
        device_id: approval.device_id,
        channel: approval.channel,
        host_policy_sha256: semantic_memory_host_policy_sha256(&state),
    };
    let target_scope = target_scope(&session.context);
    let active = state
        .runtime
        .journal_store
        .activate_semantic_memory(
            payload.candidate_id.as_str(),
            payload.approval_id.as_str(),
            &target_scope,
            &authority,
            current_unix_ms(),
        )
        .map_err(journal_response)?;
    state.runtime.clear_memory_search_cache();
    let _ = state
        .runtime
        .record_console_event(
            &session.context,
            "semantic_memory.lifecycle.transition",
            lifecycle_metadata(
                active.record.memory_id.as_str(),
                active.record.record_sha256.as_str(),
                active.record.quality_eval.evidence_sha256.as_str(),
                active.record.approval_generation,
                active.record.reason_code.as_str(),
            ),
        )
        .await;
    Ok(Json(json!({
        "schema_version": 1,
        "state": "active",
        "reason_code": active.record.reason_code,
        "record_sha256": active.record.record_sha256,
        "memory_item_id": active.projected_memory.memory_id,
        "version": active.record.version,
        "approval_generation": active.record.approval_generation,
        "citations": active.record.citations,
    })))
}

/// Searches the ordinary memory index and enriches active hits with citations.
pub(crate) async fn console_semantic_memory_search_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<SemanticMemorySearchQuery>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    require_rollout(&state)?;
    let hits = state
        .runtime
        .search_active_semantic_memory(MemorySearchRequest {
            principal: session.context.principal,
            channel: session.context.channel,
            session_id: None,
            query: query.query,
            top_k: query.top_k.unwrap_or(8).clamp(1, 32),
            min_score: query.min_score.unwrap_or(0.0).clamp(0.0, 1.0),
            tags: Vec::new(),
            sources: Vec::new(),
        })
        .await
        .map_err(internal_status)?;
    Ok(Json(json!({
        "schema_version": 1,
        "reason_code": "semantic_memory.search_completed",
        "hits": hits,
    })))
}

/// Records bounded usefulness/correction feedback with server-owned time.
pub(crate) async fn console_semantic_memory_feedback_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(memory_id): Path<String>,
    Json(payload): Json<SemanticMemoryFeedbackRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    require_rollout(&state)?;
    let target_scope = target_scope(&session.context);
    let record = state
        .runtime
        .journal_store
        .apply_semantic_memory_feedback(
            memory_id.as_str(),
            &target_scope,
            SemanticMemoryRetrievalFeedbackV1 {
                useful: payload.useful,
                corrected: payload.corrected,
                retrieved_at_unix_ms: current_unix_ms(),
                correction_evidence_ref: payload.correction_evidence_ref,
            },
        )
        .map_err(journal_response)?;
    state.runtime.clear_memory_search_cache();
    record_transition(&state, &session.context, &record).await;
    Ok(Json(json!({
        "schema_version": 1,
        "state": record.lifecycle,
        "reason_code": record.reason_code,
        "record_sha256": record.record_sha256,
        "version": record.version,
        "retrieval_metrics": record.retrieval_metrics,
    })))
}

/// Applies the host-owned freshness policy without accepting a caller age.
pub(crate) async fn console_semantic_memory_stale_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(memory_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    require_rollout(&state)?;
    let now = current_unix_ms();
    let changed = state
        .runtime
        .journal_store
        .mark_semantic_memory_stale_durable(
            memory_id.as_str(),
            &target_scope(&session.context),
            now,
            SEMANTIC_MEMORY_MAX_AGE_MS,
        )
        .map_err(journal_response)?;
    if changed {
        state.runtime.clear_memory_search_cache();
        let _ = state
            .runtime
            .record_console_event(
                &session.context,
                "semantic_memory.lifecycle.transition",
                json!({
                    "schema_version": 1,
                    "memory_id_sha256": digest_text(memory_id.as_str()),
                    "reason_code": "semantic_memory.stale",
                }),
            )
            .await;
    }
    Ok(Json(json!({
        "schema_version": 1,
        "state": if changed { "degraded" } else { "active" },
        "reason_code": if changed {
            "semantic_memory.stale"
        } else {
            "semantic_memory.fresh"
        },
    })))
}

/// Archives current recall while preserving immutable evidence and lineage.
pub(crate) async fn console_semantic_memory_archive_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(memory_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    require_rollout(&state)?;
    let target_scope = target_scope(&session.context);
    let record = state
        .runtime
        .journal_store
        .archive_semantic_memory_durable(memory_id.as_str(), &target_scope, current_unix_ms())
        .map_err(journal_response)?;
    state.runtime.clear_memory_search_cache();
    record_transition(&state, &session.context, &record).await;
    Ok(Json(json!({
        "schema_version": 1,
        "state": record.lifecycle,
        "reason_code": record.reason_code,
        "record_sha256": record.record_sha256,
        "version": record.version,
    })))
}

/// Creates the exact standard approval required to rollback a current lineage.
pub(crate) async fn console_semantic_memory_rollback_proposal_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<SemanticMemoryRollbackProposalRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    require_rollout(&state)?;
    let authority = new_review_authority(&state, &session.context).map_err(journal_response)?;
    let target_scope = target_scope(&session.context);
    let proposed = state
        .runtime
        .journal_store
        .semantic_memory_rollback_review(
            payload.memory_id.as_str(),
            payload.target_record_sha256.as_str(),
            &target_scope,
            &authority,
        )
        .map_err(journal_response)?;
    let approval = state
        .runtime
        .create_approval_record(semantic_memory_approval_request(
            Ulid::new().to_string(),
            &proposed,
            &authority,
        ))
        .await
        .map_err(internal_status)?;
    Ok(Json(json!({
        "schema_version": 1,
        "state": "rollback_proposed",
        "reason_code": proposed.reason_code,
        "approval_id": approval.approval_id,
        "approval_subject_id": proposed.approval_subject_id,
        "target_record_sha256": payload.target_record_sha256,
        "approval_generation": proposed.context.approval_generation,
        "candidate": proposed.candidate,
    })))
}

/// Consumes exact rollback review and republishes the prior evidence version.
pub(crate) async fn console_semantic_memory_rollback_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<SemanticMemoryRollbackRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    require_rollout(&state)?;
    let approval = state
        .runtime
        .approval_record(payload.approval_id.clone())
        .await
        .map_err(internal_status)?
        .ok_or_else(|| failed_precondition("semantic_memory.host_approval_missing_or_stale"))?;
    if approval.principal != session.context.principal
        || approval.device_id != session.context.device_id
        || approval.channel != session.context.channel
    {
        return Err(failed_precondition("semantic_memory.host_approval_principal_mismatch"));
    }
    let authority = SemanticMemoryReviewAuthority {
        session_id: approval.session_id,
        run_id: approval.run_id,
        principal: approval.principal,
        device_id: approval.device_id,
        channel: approval.channel,
        host_policy_sha256: semantic_memory_host_policy_sha256(&state),
    };
    let target_scope = target_scope(&session.context);
    let active = state
        .runtime
        .journal_store
        .rollback_semantic_memory_durable(
            payload.memory_id.as_str(),
            payload.target_record_sha256.as_str(),
            payload.approval_id.as_str(),
            &target_scope,
            &authority,
            current_unix_ms(),
        )
        .map_err(journal_response)?;
    state.runtime.clear_memory_search_cache();
    record_transition(&state, &session.context, &active.record).await;
    Ok(Json(json!({
        "schema_version": 1,
        "state": "active",
        "reason_code": active.record.reason_code,
        "record_sha256": active.record.record_sha256,
        "memory_item_id": active.projected_memory.memory_id,
        "version": active.record.version,
        "approval_generation": active.record.approval_generation,
        "citations": active.record.citations,
    })))
}

/// Returns hash-only semantic-memory diagnostics.
pub(crate) async fn console_semantic_memory_diagnostics_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let diagnostics = state
        .runtime
        .journal_store
        .semantic_memory_diagnostics(&target_scope(&session.context))
        .map_err(journal_response)?;
    Ok(Json(json!({
        "schema_version": 1,
        "rollout_enabled": state
            .runtime
            .config
            .feature_rollouts
            .semantic_memory_consolidation
            .enabled,
        "diagnostics": diagnostics,
    })))
}

#[allow(clippy::result_large_err)]
fn require_rollout(state: &AppState) -> Result<(), Response> {
    if state.runtime.config.feature_rollouts.semantic_memory_consolidation.enabled {
        Ok(())
    } else {
        Err(failed_precondition("semantic_memory.rollout_disabled"))
    }
}

fn new_review_authority(
    state: &AppState,
    context: &crate::gateway::RequestContext,
) -> Result<SemanticMemoryReviewAuthority, JournalError> {
    let session_key = semantic_memory_review_session_key(context);
    let session = state
        .runtime
        .journal_store
        .resolve_orchestrator_session(&OrchestratorSessionResolveRequest {
            session_id: None,
            session_key: Some(session_key),
            session_label: None,
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
            require_existing: false,
            reset_session: false,
        })?
        .session;
    Ok(SemanticMemoryReviewAuthority {
        session_id: session.session_id,
        run_id: Ulid::new().to_string(),
        principal: context.principal.clone(),
        device_id: context.device_id.clone(),
        channel: context.channel.clone(),
        host_policy_sha256: semantic_memory_host_policy_sha256(state),
    })
}

fn semantic_memory_review_session_key(context: &crate::gateway::RequestContext) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"palyra.semantic-memory.review-session.v1\0");
    for value in [
        context.principal.as_str(),
        context.device_id.as_str(),
        context.channel.as_deref().unwrap_or(""),
    ] {
        hasher.update((value.len() as u64).to_le_bytes());
        hasher.update(value.as_bytes());
    }
    format!("semantic-memory-review-{}", hex::encode(hasher.finalize()))
}

fn target_scope(context: &crate::gateway::RequestContext) -> SemanticMemoryTargetScope {
    SemanticMemoryTargetScope {
        principal: context.principal.clone(),
        channel: context.channel.clone(),
        session_id: None,
    }
}

fn semantic_memory_host_policy_sha256(state: &AppState) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"palyra.semantic-memory.host-policy.v1\0");
    hasher.update(
        u8::from(state.runtime.config.feature_rollouts.semantic_memory_consolidation.enabled)
            .to_le_bytes(),
    );
    hasher.update(2_u64.to_le_bytes());
    hasher.update(8_500_u16.to_le_bytes());
    hasher.update((30_i64 * 24 * 60 * 60 * 1_000).to_le_bytes());
    hex::encode(hasher.finalize())
}

async fn record_lifecycle_event(
    state: &AppState,
    context: &crate::gateway::RequestContext,
    reason_code: &str,
    proposed: &crate::journal::semantic_memory::SemanticMemoryProposedRecord,
    approval_id: Option<&str>,
) {
    let _ = state
        .runtime
        .record_console_event(
            context,
            "semantic_memory.lifecycle.transition",
            json!({
                "schema_version": 1,
                "memory_id_sha256": digest_text(proposed.memory_id.as_str()),
                "candidate_sha256": proposed.candidate_sha256,
                "eval_sha256": proposed.candidate.quality_eval.evidence_sha256,
                "approval_id_sha256": approval_id.map(digest_text),
                "approval_generation": proposed.context.approval_generation,
                "reason_code": reason_code,
            }),
        )
        .await;
}

async fn record_transition(
    state: &AppState,
    context: &crate::gateway::RequestContext,
    record: &crate::application::semantic_memory::ConsolidatedMemoryRecord,
) {
    let _ = state
        .runtime
        .record_console_event(
            context,
            "semantic_memory.lifecycle.transition",
            lifecycle_metadata(
                record.memory_id.as_str(),
                record.record_sha256.as_str(),
                record.quality_eval.evidence_sha256.as_str(),
                record.approval_generation,
                record.reason_code.as_str(),
            ),
        )
        .await;
}

fn lifecycle_metadata(
    memory_id: &str,
    record_sha256: &str,
    eval_sha256: &str,
    approval_generation: u64,
    reason_code: &str,
) -> Value {
    json!({
        "schema_version": 1,
        "memory_id_sha256": digest_text(memory_id),
        "record_sha256": record_sha256,
        "eval_sha256": eval_sha256,
        "approval_generation": approval_generation,
        "reason_code": reason_code,
    })
}

fn journal_response(error: JournalError) -> Response {
    match error {
        JournalError::InvalidArgument(reason_code) => failed_precondition(reason_code.as_str()),
        _ => runtime_status_response(tonic::Status::internal("semantic_memory.internal")),
    }
}

fn internal_status(_error: tonic::Status) -> Response {
    runtime_status_response(tonic::Status::internal("semantic_memory.internal"))
}

fn failed_precondition(reason_code: &str) -> Response {
    runtime_status_response(tonic::Status::failed_precondition(reason_code.to_owned()))
}

fn digest_text(value: &str) -> String {
    hex::encode(Sha256::digest(value.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::semantic_memory_review_session_key;
    use crate::gateway::RequestContext;

    #[test]
    fn review_session_key_is_stable_scoped_and_hash_only() {
        let context = RequestContext {
            principal: "review-principal".to_owned(),
            device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            channel: Some("console".to_owned()),
        };
        let key = semantic_memory_review_session_key(&context);

        assert_eq!(key, semantic_memory_review_session_key(&context));
        assert_ne!(
            key,
            semantic_memory_review_session_key(&RequestContext {
                channel: Some("other-channel".to_owned()),
                ..context.clone()
            })
        );
        assert!(key.starts_with("semantic-memory-review-"));
        assert!(key.len() <= 128);
        assert!(!key.contains(context.principal.as_str()));
        assert!(!key.contains(context.device_id.as_str()));
    }
}
