//! Console HTTP handlers for the unified task runtime and WorkBoard.
//!
//! `/console/v1/tasks*` is a read model over source runtimes; durable WorkBoard
//! items live in the journal and are exposed under `/console/v1/workboard*`.

use axum::{
    extract::{Path, Query, State},
    http::HeaderMap,
    response::Response,
    Json,
};
use palyra_common::runtime_contracts::{AuxiliaryTaskState, FlowState};
use serde::Deserialize;
use serde_json::{json, Value};
use tonic::Status;
use ulid::Ulid;

use super::diagnostics::{authorize_console_session, build_page_info};
use crate::{
    app::state::{AppState, ConsoleSession},
    gateway::current_unix_ms,
    journal::{
        CommitmentUpdateRequest, FlowTransitionRequest, OrchestratorBackgroundTaskUpdateRequest,
        WorkItemCreateRequest, WorkItemListFilter, WorkItemUpdateRequest,
    },
    runtime_status_response,
    task_runtime::{TaskAccessPolicy, TaskRuntime, TaskRuntimeFilter},
};

const DEFAULT_TASK_LIMIT: usize = 100;
const MAX_TASK_LIMIT: usize = 500;
const DEFAULT_WORK_ITEM_LEASE_MS: i64 = 300_000;

#[derive(Debug, Deserialize)]
pub(crate) struct ConsoleTasksQuery {
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    include_terminal: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConsoleTaskActionRequest {
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConsoleWorkItemCreateRequest {
    title: String,
    #[serde(default)]
    summary: Option<String>,
    #[serde(default)]
    priority: Option<i64>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    run_id: Option<String>,
    #[serde(default)]
    objective_id: Option<String>,
    #[serde(default)]
    routine_id: Option<String>,
    #[serde(default)]
    assigned_worker: Option<String>,
    #[serde(default)]
    dependencies: Option<Value>,
    #[serde(default)]
    artifact_refs: Option<Value>,
    #[serde(default)]
    blocker: Option<Value>,
    #[serde(default)]
    metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConsoleWorkItemClaimRequest {
    #[serde(default)]
    worker: Option<String>,
    #[serde(default)]
    lease_ms: Option<i64>,
}

/// Lists normalized tasks for the caller.
///
/// # Errors
/// Returns unauthorized for invalid sessions, or a mapped runtime error.
pub(crate) async fn console_tasks_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleTasksQuery>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let limit = query.limit.unwrap_or(DEFAULT_TASK_LIMIT).clamp(1, MAX_TASK_LIMIT);
    let snapshot = TaskRuntime::snapshot(
        &state.runtime,
        TaskRuntimeFilter {
            access: task_access(&session),
            state: query.state,
            include_terminal: query.include_terminal.unwrap_or(false),
            limit,
        },
    )
    .await
    .map_err(runtime_status_response)?;
    let next_cursor = snapshot.tasks.last().map(|task| task.task_id.clone());
    Ok(Json(json!({
        "contract": task_contract_descriptor(),
        "page": build_page_info(limit, snapshot.tasks.len(), next_cursor),
        "summary": snapshot.summary,
        "projection": snapshot.projection,
        "tasks": snapshot.tasks,
    })))
}

/// Returns one normalized task.
///
/// # Errors
/// Returns not-found, permission denied, or a mapped runtime error.
pub(crate) async fn console_task_get_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(task_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let task = load_authorized_task(&state, &session, task_id.as_str()).await?;
    Ok(Json(json!({ "contract": task_contract_descriptor(), "task": task })))
}

/// Returns the normalized task timeline.
///
/// # Errors
/// Returns not-found, permission denied, or a mapped runtime error.
pub(crate) async fn console_task_timeline_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(task_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let access = task_access(&session);
    let task = TaskRuntime::get(&state.runtime, &access, task_id.as_str())
        .await
        .map_err(runtime_status_response)?
        .ok_or_else(|| runtime_status_response(Status::not_found("task not found")))?;
    let events = TaskRuntime::timeline(&state.runtime, &access, task_id.as_str())
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "contract": task_contract_descriptor(),
        "task": task,
        "events": events,
    })))
}

/// Requests cancellation for a normalized task source.
///
/// # Errors
/// Returns not-found, permission denied, unsupported source, or a mapped runtime error.
pub(crate) async fn console_task_cancel_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(task_id): Path<String>,
    Json(body): Json<ConsoleTaskActionRequest>,
) -> Result<Json<Value>, Response> {
    run_task_action(state, headers, task_id, "cancel", body).await
}

/// Pauses a normalized task source when supported.
///
/// # Errors
/// Returns not-found, permission denied, unsupported source, or a mapped runtime error.
pub(crate) async fn console_task_pause_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(task_id): Path<String>,
    Json(body): Json<ConsoleTaskActionRequest>,
) -> Result<Json<Value>, Response> {
    run_task_action(state, headers, task_id, "pause", body).await
}

/// Retries or resumes a normalized task source when supported.
///
/// # Errors
/// Returns not-found, permission denied, unsupported source, or a mapped runtime error.
pub(crate) async fn console_task_retry_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(task_id): Path<String>,
    Json(body): Json<ConsoleTaskActionRequest>,
) -> Result<Json<Value>, Response> {
    run_task_action(state, headers, task_id, "retry", body).await
}

/// Lists the caller's WorkBoard items.
///
/// # Errors
/// Returns unauthorized for invalid sessions, or a mapped runtime error.
pub(crate) async fn console_workboard_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleTasksQuery>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let limit = query.limit.unwrap_or(DEFAULT_TASK_LIMIT).clamp(1, MAX_TASK_LIMIT);
    let items = state
        .runtime
        .list_work_items(WorkItemListFilter {
            owner_principal: Some(session.context.principal.clone()),
            device_id: Some(session.context.device_id.clone()),
            channel: session.context.channel.clone(),
            state: query.state,
            include_terminal: query.include_terminal.unwrap_or(false),
            limit,
        })
        .await
        .map_err(runtime_status_response)?;
    let next_cursor = items.last().map(|item| item.work_item_id.clone());
    Ok(Json(json!({
        "contract": workboard_contract_descriptor(),
        "page": build_page_info(limit, items.len(), next_cursor),
        "items": items,
    })))
}

/// Creates a WorkBoard item owned by the caller.
///
/// # Errors
/// Returns unauthorized for invalid sessions, invalid arguments, or a mapped
/// runtime error.
pub(crate) async fn console_workboard_create_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<ConsoleWorkItemCreateRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let item = state
        .runtime
        .create_work_item(WorkItemCreateRequest {
            work_item_id: Ulid::new().to_string(),
            owner_principal: session.context.principal.clone(),
            device_id: session.context.device_id.clone(),
            channel: session.context.channel.clone(),
            session_id: body.session_id,
            run_id: body.run_id,
            objective_id: body.objective_id,
            routine_id: body.routine_id,
            title: body.title,
            summary: body.summary.unwrap_or_default(),
            state: "queued".to_owned(),
            priority: body.priority.unwrap_or(0),
            assigned_worker: body.assigned_worker,
            dependencies_json: body.dependencies.unwrap_or_else(|| json!([])).to_string(),
            artifact_refs_json: body.artifact_refs.unwrap_or_else(|| json!([])).to_string(),
            blocker_json: body.blocker.unwrap_or_else(|| json!({})).to_string(),
            metadata_json: body.metadata.unwrap_or_else(|| json!({})).to_string(),
            actor_principal: session.context.principal,
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({ "contract": workboard_contract_descriptor(), "item": item })))
}

/// Claims a WorkBoard item lease for a worker.
///
/// # Errors
/// Returns not-found, permission denied, or a mapped runtime error.
pub(crate) async fn console_workboard_claim_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(item_id): Path<String>,
    Json(body): Json<ConsoleWorkItemClaimRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    authorize_work_item(&state, &session, item_id.as_str()).await?;
    if body.lease_ms.is_some_and(|lease_ms| lease_ms <= 0) {
        return Err(runtime_status_response(Status::invalid_argument(
            "lease_ms must be greater than zero",
        )));
    }
    let now = current_unix_ms();
    let worker = body.worker.unwrap_or_else(|| session.context.principal.clone());
    let item = state
        .runtime
        .update_work_item(WorkItemUpdateRequest {
            work_item_id: item_id,
            state: Some("running".to_owned()),
            assigned_worker: Some(Some(worker.clone())),
            claim_owner: Some(Some(worker)),
            claim_expires_at_unix_ms: Some(Some(
                now.saturating_add(body.lease_ms.unwrap_or(DEFAULT_WORK_ITEM_LEASE_MS)),
            )),
            heartbeat_at_unix_ms: Some(Some(now)),
            actor_principal: session.context.principal,
            event_type: "work_item.claimed".to_owned(),
            summary: "work item claimed".to_owned(),
            payload_json: json!({ "lease_ms": body.lease_ms }).to_string(),
            ..WorkItemUpdateRequest::default()
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({ "contract": workboard_contract_descriptor(), "item": item })))
}

/// Records a WorkBoard heartbeat.
///
/// # Errors
/// Returns not-found, permission denied, or a mapped runtime error.
pub(crate) async fn console_workboard_heartbeat_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(item_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    authorize_work_item(&state, &session, item_id.as_str()).await?;
    let item = state
        .runtime
        .update_work_item(WorkItemUpdateRequest {
            work_item_id: item_id,
            heartbeat_at_unix_ms: Some(Some(current_unix_ms())),
            actor_principal: session.context.principal,
            event_type: "work_item.heartbeat".to_owned(),
            summary: "work item heartbeat".to_owned(),
            payload_json: json!({}).to_string(),
            ..WorkItemUpdateRequest::default()
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({ "contract": workboard_contract_descriptor(), "item": item })))
}

/// Completes a WorkBoard item.
///
/// # Errors
/// Returns not-found, permission denied, or a mapped runtime error.
pub(crate) async fn console_workboard_complete_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(item_id): Path<String>,
    Json(body): Json<ConsoleTaskActionRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    authorize_work_item(&state, &session, item_id.as_str()).await?;
    let now = current_unix_ms();
    let item = state
        .runtime
        .update_work_item(WorkItemUpdateRequest {
            work_item_id: item_id,
            state: Some("succeeded".to_owned()),
            claim_owner: Some(None),
            claim_expires_at_unix_ms: Some(None),
            completed_at_unix_ms: Some(Some(now)),
            actor_principal: session.context.principal,
            event_type: "work_item.completed".to_owned(),
            summary: action_reason(body.reason, "work item completed"),
            payload_json: json!({ "completed_at_unix_ms": now }).to_string(),
            ..WorkItemUpdateRequest::default()
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({ "contract": workboard_contract_descriptor(), "item": item })))
}

async fn run_task_action(
    state: AppState,
    headers: HeaderMap,
    task_id: String,
    action: &'static str,
    body: ConsoleTaskActionRequest,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let task = load_authorized_task(&state, &session, task_id.as_str()).await?;
    let reason = action_reason(body.reason, format!("task {action} requested").as_str());
    match task.source_kind.as_str() {
        "flow" => {
            let next_state = match action {
                "cancel" => FlowState::CancelRequested,
                "pause" => FlowState::Paused,
                "retry" => FlowState::Ready,
                _ => unreachable!("task action is fixed by handler"),
            };
            let Some(bundle) = state
                .runtime
                .get_flow_bundle(task.source_id.clone(), 1)
                .await
                .map_err(runtime_status_response)?
            else {
                return Err(runtime_status_response(Status::not_found("flow not found")));
            };
            state
                .runtime
                .transition_flow(FlowTransitionRequest {
                    flow_id: task.source_id.clone(),
                    expected_revision: Some(bundle.flow.revision),
                    state: next_state.as_str().to_owned(),
                    current_step_id: None,
                    lock_owner: if action == "retry" { Some(None) } else { None },
                    lock_expires_at_unix_ms: if action == "retry" { Some(None) } else { None },
                    completed_at_unix_ms: if action == "retry" { Some(None) } else { None },
                    actor_principal: session.context.principal.clone(),
                    event_type: format!("task.{action}"),
                    summary: reason.clone(),
                    payload_json: json!({ "task_id": task.task_id, "reason": reason }).to_string(),
                })
                .await
                .map_err(runtime_status_response)?;
        }
        "background_task" => {
            let next_state = match action {
                "cancel" => AuxiliaryTaskState::CancelRequested,
                "pause" => AuxiliaryTaskState::Paused,
                "retry" => AuxiliaryTaskState::Queued,
                _ => unreachable!("task action is fixed by handler"),
            };
            state
                .runtime
                .update_orchestrator_background_task(OrchestratorBackgroundTaskUpdateRequest {
                    task_id: task.source_id.clone(),
                    state: Some(next_state.as_str().to_owned()),
                    target_run_id: None,
                    increment_attempt_count: action == "retry",
                    last_error: Some(if action == "retry" { None } else { Some(reason.clone()) }),
                    result_json: None,
                    started_at_unix_ms: if action == "retry" { Some(None) } else { None },
                    completed_at_unix_ms: if action == "retry" { Some(None) } else { None },
                })
                .await
                .map_err(runtime_status_response)?;
        }
        "work_item" => {
            let next_state = match action {
                "cancel" => "cancel_requested",
                "pause" => "paused",
                "retry" => "queued",
                _ => unreachable!("task action is fixed by handler"),
            };
            state
                .runtime
                .update_work_item(WorkItemUpdateRequest {
                    work_item_id: task.source_id.clone(),
                    state: Some(next_state.to_owned()),
                    claim_owner: Some(None),
                    claim_expires_at_unix_ms: Some(None),
                    completed_at_unix_ms: if action == "retry" { Some(None) } else { None },
                    actor_principal: session.context.principal.clone(),
                    event_type: format!("task.{action}"),
                    summary: reason.clone(),
                    payload_json: json!({ "task_id": task.task_id, "reason": reason }).to_string(),
                    ..WorkItemUpdateRequest::default()
                })
                .await
                .map_err(runtime_status_response)?;
        }
        "commitment" => {
            let next_status = match action {
                "cancel" => "dismissed",
                "pause" => "snoozed",
                "retry" => "approved",
                _ => unreachable!("task action is fixed by handler"),
            };
            state
                .runtime
                .update_commitment(CommitmentUpdateRequest {
                    commitment_id: task.source_id.clone(),
                    expected_status: None,
                    status: Some(next_status.to_owned()),
                    user_wording: None,
                    normalized_action: None,
                    due_condition_json: None,
                    recurrence_json: None,
                    channel_binding_json: None,
                    approval_requirement: None,
                    privacy_label: None,
                    review_reason: Some(reason.clone()),
                    scheduler_binding_json: None,
                    due_at_unix_ms: None,
                    scheduled_at_unix_ms: None,
                    completed_at_unix_ms: if next_status == "dismissed" {
                        Some(Some(current_unix_ms()))
                    } else {
                        None
                    },
                    actor_principal: session.context.principal.clone(),
                    event_type: format!("task.{action}"),
                    summary: reason.clone(),
                    payload_json: json!({ "task_id": task.task_id, "reason": reason }).to_string(),
                })
                .await
                .map_err(runtime_status_response)?;
        }
        "tool_job" => {
            return Err(runtime_status_response(Status::failed_precondition(
                "tool jobs must be controlled through /console/v1/jobs",
            )));
        }
        _ => return Err(runtime_status_response(Status::invalid_argument("unknown task kind"))),
    }

    let access = task_access(&session);
    let updated = TaskRuntime::get(&state.runtime, &access, task_id.as_str())
        .await
        .map_err(runtime_status_response)?
        .ok_or_else(|| runtime_status_response(Status::not_found("task not found")))?;
    Ok(Json(json!({ "contract": task_contract_descriptor(), "task": updated })))
}

async fn load_authorized_task(
    state: &AppState,
    session: &ConsoleSession,
    task_id: &str,
) -> Result<crate::task_runtime::TaskRun, Response> {
    TaskRuntime::get(&state.runtime, &task_access(session), task_id)
        .await
        .map_err(runtime_status_response)?
        .ok_or_else(|| runtime_status_response(Status::not_found("task not found")))
}

async fn authorize_work_item(
    state: &AppState,
    session: &ConsoleSession,
    item_id: &str,
) -> Result<(), Response> {
    let Some(item) =
        state.runtime.get_work_item(item_id.to_owned()).await.map_err(runtime_status_response)?
    else {
        return Err(runtime_status_response(Status::not_found("work item not found")));
    };
    if item.owner_principal != session.context.principal
        || item.device_id != session.context.device_id
        || item.channel != session.context.channel
    {
        return Err(runtime_status_response(Status::permission_denied(
            "work item belongs to a different console scope",
        )));
    }
    Ok(())
}

fn task_access(session: &ConsoleSession) -> TaskAccessPolicy {
    TaskAccessPolicy {
        owner_principal: session.context.principal.clone(),
        device_id: Some(session.context.device_id.clone()),
        channel: session.context.channel.clone(),
    }
}

fn action_reason(reason: Option<String>, default_reason: &str) -> String {
    reason
        .and_then(|value| {
            let trimmed = value.trim();
            (!trimmed.is_empty()).then(|| trimmed.to_owned())
        })
        .unwrap_or_else(|| default_reason.to_owned())
}

fn task_contract_descriptor() -> Value {
    json!({
        "schema": "palyra.console.tasks.v1",
        "sources": ["background_task", "flow", "tool_job", "work_item", "commitment", "agent_plan_item"],
        "id_format": "<source-prefix>:<source-id>",
        "projection": {
            "schema_version": crate::task_runtime::TASK_PROJECTION_SCHEMA_VERSION,
            "rollout_mode": crate::task_runtime::TASK_PROJECTION_ROLLOUT_OBSERVE_ONLY,
            "audit_ledger": "journal_read_model",
            "event_types": {
                "started": crate::task_runtime::TASK_PROJECTION_EVENT_STARTED,
                "completed": crate::task_runtime::TASK_PROJECTION_EVENT_COMPLETED,
                "failed": crate::task_runtime::TASK_PROJECTION_EVENT_FAILED,
            },
            "redaction_level": crate::task_runtime::TASK_PROJECTION_REDACTION_METADATA_ONLY,
        },
    })
}

fn workboard_contract_descriptor() -> Value {
    json!({
        "schema": "palyra.console.workboard.v1",
        "states": ["queued", "running", "paused", "blocked", "waiting", "succeeded", "failed", "cancel_requested", "cancelled"],
    })
}
