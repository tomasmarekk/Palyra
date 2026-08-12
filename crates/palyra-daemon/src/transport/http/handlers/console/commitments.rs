//! Console HTTP handlers for commitment ledger review and scheduling.
//!
//! Commitments are extracted as proposed rows, manually reviewed, then bridged
//! to scheduling/delivery through explicit audited lifecycle events.

use axum::{
    extract::{Path, Query, State},
    http::HeaderMap,
    response::Response,
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tonic::Status;
use ulid::Ulid;

use super::diagnostics::{authorize_console_session, build_page_info};
use crate::{
    app::state::{AppState, ConsoleSession},
    commitments::{
        build_commitment_create_plan, CommitmentExtractionInput,
        HYBRID_INFERRED_COMMITMENTS_EVENT_COMPLETED, HYBRID_INFERRED_COMMITMENTS_EVENT_FAILED,
        HYBRID_INFERRED_COMMITMENTS_EVENT_STARTED, HYBRID_INFERRED_COMMITMENTS_REDACTION_LEVEL,
        HYBRID_INFERRED_COMMITMENTS_ROLLOUT_OBSERVE_ONLY,
        HYBRID_INFERRED_COMMITMENTS_SCHEMA_VERSION,
    },
    gateway::current_unix_ms,
    journal::{
        CommitmentDeliveryAttemptCreateRequest, CommitmentListFilter, CommitmentRecord,
        CommitmentUpdateRequest,
    },
    runtime_status_response,
};

const DEFAULT_COMMITMENT_LIMIT: usize = 100;
const MAX_COMMITMENT_LIMIT: usize = 500;
const DEFAULT_COMMITMENT_EVENT_LIMIT: usize = 512;

#[derive(Debug, Deserialize)]
pub(crate) struct ConsoleCommitmentsListQuery {
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    include_terminal: Option<bool>,
    #[serde(default)]
    due_before_unix_ms: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConsoleCommitmentActionRequest {
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    due_at_unix_ms: Option<i64>,
    #[serde(default)]
    user_wording: Option<String>,
    #[serde(default)]
    normalized_action: Option<String>,
    #[serde(default)]
    due_condition: Option<Value>,
    #[serde(default)]
    recurrence: Option<Value>,
    #[serde(default)]
    channel_binding: Option<Value>,
    #[serde(default)]
    privacy_label: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConsoleCommitmentExtractRequest {
    source_text: String,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    run_id: Option<String>,
    #[serde(default)]
    extraction_model: Option<String>,
    #[serde(default)]
    include_inferred: bool,
}

/// Lists commitments for the caller.
///
/// # Errors
/// Returns unauthorized for invalid sessions, or a mapped runtime error.
pub(crate) async fn console_commitments_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleCommitmentsListQuery>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let limit = query.limit.unwrap_or(DEFAULT_COMMITMENT_LIMIT).clamp(1, MAX_COMMITMENT_LIMIT);
    let channel = session.context.channel.clone();
    let mut commitments = state
        .runtime
        .list_commitments(CommitmentListFilter {
            owner_principal: Some(session.context.principal.clone()),
            device_id: Some(session.context.device_id.clone()),
            channel: channel.clone(),
            status: query.status,
            due_before_unix_ms: query.due_before_unix_ms,
            include_terminal: query.include_terminal.unwrap_or(false),
            limit: MAX_COMMITMENT_LIMIT,
        })
        .await
        .map_err(runtime_status_response)?;
    commitments.retain(|commitment| commitment.channel == channel);
    commitments.truncate(limit);
    let next_cursor = commitments.last().map(|commitment| commitment.commitment_id.clone());
    Ok(Json(json!({
        "contract": commitment_contract_descriptor(),
        "page": build_page_info(limit, commitments.len(), next_cursor),
        "summary": summarize_commitments(commitments.as_slice()),
        "commitments": commitments,
    })))
}

/// Returns one commitment with events and delivery attempts.
///
/// # Errors
/// Returns not-found, permission denied, or a mapped runtime error.
pub(crate) async fn console_commitment_get_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(commitment_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let commitment = load_authorized_commitment(&state, &session, commitment_id.as_str()).await?;
    let events = state
        .runtime
        .list_commitment_events(commitment_id.clone(), DEFAULT_COMMITMENT_EVENT_LIMIT)
        .await
        .map_err(runtime_status_response)?;
    let attempts = state
        .runtime
        .list_commitment_delivery_attempts(commitment_id, DEFAULT_COMMITMENT_EVENT_LIMIT)
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "contract": commitment_contract_descriptor(),
        "commitment": commitment,
        "events": events,
        "delivery_attempts": attempts,
    })))
}

/// Returns source evidence for a commitment.
///
/// # Errors
/// Returns not-found, permission denied, or a mapped runtime error.
pub(crate) async fn console_commitment_sources_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(commitment_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let commitment = load_authorized_commitment(&state, &session, commitment_id.as_str()).await?;
    let sources = state
        .runtime
        .list_commitment_sources(commitment_id)
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "contract": commitment_contract_descriptor(),
        "commitment": commitment,
        "sources": sources,
    })))
}

/// Explains why a commitment is in its current lifecycle state.
///
/// # Errors
/// Returns not-found, permission denied, or a mapped runtime error.
pub(crate) async fn console_commitment_explain_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(commitment_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let commitment = load_authorized_commitment(&state, &session, commitment_id.as_str()).await?;
    Ok(Json(json!({
        "contract": commitment_contract_descriptor(),
        "commitment_id": commitment.commitment_id,
        "status": commitment.status,
        "review_reason": commitment.review_reason,
        "approval_requirement": commitment.approval_requirement,
        "privacy_label": commitment.privacy_label,
        "scheduler_binding": json_value(commitment.scheduler_binding_json.as_str()),
        "due_condition": json_value(commitment.due_condition_json.as_str()),
    })))
}

/// Extracts proposed commitments from provided post-run text.
///
/// # Errors
/// Returns unauthorized, invalid arguments, or a mapped runtime error.
pub(crate) async fn console_commitments_extract_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<ConsoleCommitmentExtractRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let input = CommitmentExtractionInput {
        owner_principal: session.context.principal.clone(),
        device_id: session.context.device_id.clone(),
        channel: session.context.channel.clone(),
        session_id: body.session_id,
        run_id: body.run_id,
        source_text: body.source_text,
        extraction_model: body.extraction_model,
        include_inferred: body.include_inferred,
        auxiliary_selection: None,
    };
    let plan = build_commitment_create_plan(&input, session.context.principal.as_str());
    let mut commitments = Vec::new();
    for request in plan.requests {
        commitments
            .push(state.runtime.create_commitment(request).await.map_err(runtime_status_response)?);
    }
    Ok(Json(json!({
        "contract": commitment_contract_descriptor(),
        "hybrid_inference": plan.inference,
        "extracted_count": commitments.len(),
        "commitments": commitments,
    })))
}

/// Approves a proposed commitment.
///
/// # Errors
/// Returns not-found, permission denied, invalid transition, or a mapped runtime error.
pub(crate) async fn console_commitment_approve_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(commitment_id): Path<String>,
    Json(body): Json<ConsoleCommitmentActionRequest>,
) -> Result<Json<Value>, Response> {
    update_commitment_status(state, headers, commitment_id, "approved", body).await
}

/// Dismisses a commitment.
///
/// # Errors
/// Returns not-found, permission denied, invalid transition, or a mapped runtime error.
pub(crate) async fn console_commitment_dismiss_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(commitment_id): Path<String>,
    Json(body): Json<ConsoleCommitmentActionRequest>,
) -> Result<Json<Value>, Response> {
    update_commitment_status(state, headers, commitment_id, "dismissed", body).await
}

/// Snoozes a commitment without delivering it.
///
/// # Errors
/// Returns not-found, permission denied, invalid transition, or a mapped runtime error.
pub(crate) async fn console_commitment_snooze_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(commitment_id): Path<String>,
    Json(body): Json<ConsoleCommitmentActionRequest>,
) -> Result<Json<Value>, Response> {
    update_commitment_status(state, headers, commitment_id, "snoozed", body).await
}

/// Edits commitment review fields without changing status unless provided.
///
/// # Errors
/// Returns not-found, permission denied, invalid JSON, or a mapped runtime error.
pub(crate) async fn console_commitment_edit_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(commitment_id): Path<String>,
    Json(body): Json<ConsoleCommitmentActionRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let _ = load_authorized_commitment(&state, &session, commitment_id.as_str()).await?;
    let commitment = state
        .runtime
        .update_commitment(CommitmentUpdateRequest {
            commitment_id,
            expected_status: None,
            status: None,
            user_wording: body.user_wording,
            normalized_action: body.normalized_action,
            due_condition_json: body.due_condition.map(|value| value.to_string()),
            recurrence_json: body.recurrence.map(|value| value.to_string()),
            channel_binding_json: body.channel_binding.map(|value| value.to_string()),
            approval_requirement: None,
            privacy_label: body.privacy_label,
            review_reason: body.reason.clone(),
            scheduler_binding_json: None,
            due_at_unix_ms: body.due_at_unix_ms.map(Some),
            scheduled_at_unix_ms: None,
            completed_at_unix_ms: None,
            actor_principal: session.context.principal,
            event_type: "commitment.edited".to_owned(),
            summary: action_reason(body.reason, "commitment edited"),
            payload_json: json!({ "operation": "edit" }).to_string(),
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({ "contract": commitment_contract_descriptor(), "commitment": commitment })))
}

/// Bridges an approved commitment into the scheduling/delivery queue.
///
/// # Errors
/// Returns not-found, permission denied, invalid transition, or a mapped runtime error.
pub(crate) async fn console_commitment_schedule_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(commitment_id): Path<String>,
    Json(body): Json<ConsoleCommitmentActionRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let commitment = load_authorized_commitment(&state, &session, commitment_id.as_str()).await?;
    if !matches!(commitment.status.as_str(), "approved" | "snoozed" | "scheduled") {
        return Err(runtime_status_response(Status::failed_precondition(
            "only approved, snoozed, or scheduled commitments can be scheduled",
        )));
    }
    let now = current_unix_ms();
    let scheduler_binding = json!({
        "type": "routine_bridge",
        "state": "queued_for_delivery",
        "queued_at_unix_ms": now,
        "due_at_unix_ms": body.due_at_unix_ms.or(commitment.due_at_unix_ms),
    });
    let updated = state
        .runtime
        .update_commitment(CommitmentUpdateRequest {
            commitment_id: commitment.commitment_id.clone(),
            expected_status: None,
            status: Some("scheduled".to_owned()),
            user_wording: None,
            normalized_action: None,
            due_condition_json: None,
            recurrence_json: None,
            channel_binding_json: None,
            approval_requirement: None,
            privacy_label: None,
            review_reason: Some(action_reason(body.reason, "commitment scheduled")),
            scheduler_binding_json: Some(scheduler_binding.to_string()),
            due_at_unix_ms: Some(body.due_at_unix_ms.or(commitment.due_at_unix_ms)),
            scheduled_at_unix_ms: Some(Some(now)),
            completed_at_unix_ms: None,
            actor_principal: session.context.principal,
            event_type: "commitment.scheduled".to_owned(),
            summary: "commitment scheduled".to_owned(),
            payload_json: scheduler_binding.to_string(),
        })
        .await
        .map_err(runtime_status_response)?;
    let attempt = state
        .runtime
        .create_commitment_delivery_attempt(CommitmentDeliveryAttemptCreateRequest {
            attempt_id: Ulid::generate().to_string(),
            commitment_id: updated.commitment_id.clone(),
            delivery_intent_id: Some(format!("routine-bridge:{}", updated.commitment_id)),
            channel_binding_json: updated.channel_binding_json.clone(),
            status: "queued".to_owned(),
            reason: "scheduled for routine delivery bridge".to_owned(),
            result_json: json!({ "scheduler_binding": scheduler_binding }).to_string(),
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "contract": commitment_contract_descriptor(),
        "commitment": updated,
        "delivery_attempt": attempt,
    })))
}

async fn update_commitment_status(
    state: AppState,
    headers: HeaderMap,
    commitment_id: String,
    status: &'static str,
    body: ConsoleCommitmentActionRequest,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let _ = load_authorized_commitment(&state, &session, commitment_id.as_str()).await?;
    let terminal_at =
        matches!(status, "dismissed" | "delivered" | "failed").then_some(Some(current_unix_ms()));
    let commitment = state
        .runtime
        .update_commitment(CommitmentUpdateRequest {
            commitment_id,
            expected_status: None,
            status: Some(status.to_owned()),
            user_wording: None,
            normalized_action: None,
            due_condition_json: None,
            recurrence_json: None,
            channel_binding_json: None,
            approval_requirement: None,
            privacy_label: None,
            review_reason: body.reason.clone(),
            scheduler_binding_json: None,
            due_at_unix_ms: body.due_at_unix_ms.map(Some),
            scheduled_at_unix_ms: None,
            completed_at_unix_ms: terminal_at,
            actor_principal: session.context.principal,
            event_type: format!("commitment.{status}"),
            summary: action_reason(body.reason, format!("commitment {status}").as_str()),
            payload_json: json!({ "status": status }).to_string(),
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({ "contract": commitment_contract_descriptor(), "commitment": commitment })))
}

async fn load_authorized_commitment(
    state: &AppState,
    session: &ConsoleSession,
    commitment_id: &str,
) -> Result<CommitmentRecord, Response> {
    let Some(commitment) = state
        .runtime
        .get_commitment(commitment_id.to_owned())
        .await
        .map_err(runtime_status_response)?
    else {
        return Err(runtime_status_response(Status::not_found("commitment not found")));
    };
    if commitment.owner_principal != session.context.principal
        || commitment.device_id != session.context.device_id
        || commitment.channel != session.context.channel
    {
        return Err(runtime_status_response(Status::permission_denied(
            "commitment belongs to a different console scope",
        )));
    }
    Ok(commitment)
}

fn summarize_commitments(commitments: &[CommitmentRecord]) -> Value {
    let proposed = commitments.iter().filter(|item| item.status == "proposed").count();
    let approved = commitments.iter().filter(|item| item.status == "approved").count();
    let scheduled = commitments.iter().filter(|item| item.status == "scheduled").count();
    let terminal = commitments
        .iter()
        .filter(|item| matches!(item.status.as_str(), "delivered" | "dismissed" | "failed"))
        .count();
    json!({
        "total": commitments.len(),
        "proposed": proposed,
        "approved": approved,
        "scheduled": scheduled,
        "terminal": terminal,
    })
}

fn action_reason(reason: Option<String>, default_reason: &str) -> String {
    reason
        .and_then(|value| {
            let trimmed = value.trim();
            (!trimmed.is_empty()).then(|| trimmed.to_owned())
        })
        .unwrap_or_else(|| default_reason.to_owned())
}

fn json_value(raw: &str) -> Value {
    serde_json::from_str(raw).unwrap_or_else(|_| json!({ "raw": raw }))
}

fn commitment_contract_descriptor() -> Value {
    json!({
        "schema": "palyra.console.commitments.v1",
        "statuses": ["proposed", "approved", "scheduled", "snoozed", "delivered", "dismissed", "failed"],
        "review_required": true,
        "hybrid_inferred_candidates": {
            "schema_version": HYBRID_INFERRED_COMMITMENTS_SCHEMA_VERSION,
            "request_field": "include_inferred",
            "default_enabled": false,
            "rollout_mode": HYBRID_INFERRED_COMMITMENTS_ROLLOUT_OBSERVE_ONLY,
            "candidate_status": "proposed",
            "approval_requirement": "manual_review",
            "event_types": {
                "started": HYBRID_INFERRED_COMMITMENTS_EVENT_STARTED,
                "completed": HYBRID_INFERRED_COMMITMENTS_EVENT_COMPLETED,
                "failed": HYBRID_INFERRED_COMMITMENTS_EVENT_FAILED,
            },
            "redaction_level": HYBRID_INFERRED_COMMITMENTS_REDACTION_LEVEL,
        },
    })
}
