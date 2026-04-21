use axum::{
    extract::{Path, Query, State},
    http::HeaderMap,
    response::Response,
    Json,
};
use palyra_common::runtime_contracts::{FlowState, FlowStepState};
use serde::Deserialize;
use serde_json::{json, Value};

use super::diagnostics::{authorize_console_session, build_page_info};

use crate::{
    app::state::AppState,
    flows::{self, FlowLineage, FlowMode},
    journal::{
        FlowListFilter, FlowRecord, FlowStepCreateRequest, FlowStepRecord, FlowStepUpdateRequest,
        FlowTransitionRequest,
    },
    runtime_status_response,
};

const DEFAULT_FLOW_PAGE_LIMIT: usize = 100;
const MAX_FLOW_PAGE_LIMIT: usize = 500;
const DEFAULT_FLOW_EVENT_LIMIT: usize = 512;

#[derive(Debug, Deserialize)]
pub(crate) struct ConsoleFlowsListQuery {
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    include_terminal: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConsoleFlowCreateRequest {
    title: String,
    #[serde(default)]
    summary: Option<String>,
    #[serde(default)]
    mode: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    origin_run_id: Option<String>,
    #[serde(default)]
    objective_id: Option<String>,
    #[serde(default)]
    routine_id: Option<String>,
    #[serde(default)]
    webhook_id: Option<String>,
    #[serde(default)]
    retry_policy: Option<Value>,
    #[serde(default)]
    timeout_ms: Option<i64>,
    #[serde(default)]
    metadata: Option<Value>,
    steps: Vec<ConsoleFlowStepCreateRequest>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConsoleFlowStepCreateRequest {
    adapter: String,
    #[serde(default)]
    step_kind: Option<String>,
    title: String,
    #[serde(default)]
    input: Option<Value>,
    #[serde(default)]
    lineage: Option<Value>,
    #[serde(default)]
    depends_on_step_ids: Option<Vec<String>>,
    #[serde(default)]
    max_attempts: Option<u64>,
    #[serde(default)]
    backoff_ms: Option<u64>,
    #[serde(default)]
    timeout_ms: Option<i64>,
    #[serde(default)]
    not_before_unix_ms: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConsoleFlowActionRequest {
    #[serde(default)]
    reason: Option<String>,
}

struct ConsoleFlowStepAction<'a> {
    flow_id: &'a str,
    step_id: &'a str,
    next_state: FlowStepState,
    event_type: &'a str,
    reason: String,
    terminal: bool,
}

pub(crate) async fn console_flows_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleFlowsListQuery>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let limit = query.limit.unwrap_or(DEFAULT_FLOW_PAGE_LIMIT).clamp(1, MAX_FLOW_PAGE_LIMIT);
    let flows = state
        .runtime
        .list_flows(FlowListFilter {
            owner_principal: Some(session.context.principal.clone()),
            device_id: Some(session.context.device_id.clone()),
            channel: session.context.channel.clone(),
            state: query.state,
            include_terminal: query.include_terminal.unwrap_or(true),
            limit,
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "flows": flows.iter().map(flow_view).collect::<Vec<_>>(),
        "summary": summarize_flows(flows.as_slice()),
        "adapters": flows::flow_adapter_contracts(),
        "rollout": flow_rollout_payload(&state),
        "page": build_page_info(limit, flows.len(), None),
    })))
}

pub(crate) async fn console_flow_create_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleFlowCreateRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let ConsoleFlowCreateRequest {
        title,
        summary,
        mode,
        session_id,
        origin_run_id,
        objective_id,
        routine_id,
        webhook_id,
        retry_policy,
        timeout_ms,
        metadata,
        steps,
    } = payload;
    if steps.is_empty() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "flow must include at least one step",
        )));
    }
    let mode = match mode.as_deref() {
        Some(value) => FlowMode::parse(value).ok_or_else(|| {
            runtime_status_response(tonic::Status::invalid_argument(
                "flow mode must be one of managed|mirrored",
            ))
        })?,
        None => FlowMode::Managed,
    };
    let steps = build_console_flow_steps(steps).map_err(runtime_status_response)?;
    let mut request = flows::build_flow_create_request(flows::FlowCreateDescriptor {
        owner_principal: session.context.principal.clone(),
        device_id: session.context.device_id.clone(),
        channel: session.context.channel.clone(),
        title,
        summary: summary.unwrap_or_else(|| "Durable operator flow".to_owned()),
        mode,
        session_id,
        origin_run_id,
        steps,
    });
    request.objective_id = objective_id;
    request.routine_id = routine_id;
    request.webhook_id = webhook_id;
    if let Some(retry_policy) = retry_policy {
        request.retry_policy_json = retry_policy.to_string();
    }
    if let Some(metadata) = metadata {
        request.metadata_json = metadata.to_string();
    }
    request.timeout_ms = timeout_ms;
    let flow = state.runtime.create_flow(request).await.map_err(runtime_status_response)?;
    let bundle = state
        .runtime
        .get_flow_bundle(flow.flow_id.clone(), DEFAULT_FLOW_EVENT_LIMIT)
        .await
        .map_err(runtime_status_response)?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found("created flow not found"))
        })?;
    Ok(Json(flow_bundle_view(&bundle)))
}

pub(crate) async fn console_flow_get_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(flow_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let bundle =
        load_owned_flow_bundle(&state, &session.context.principal, flow_id.as_str()).await?;
    Ok(Json(flow_bundle_view(&bundle)))
}

pub(crate) async fn console_flow_pause_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(flow_id): Path<String>,
    Json(payload): Json<ConsoleFlowActionRequest>,
) -> Result<Json<Value>, Response> {
    transition_console_flow(
        &state,
        &headers,
        flow_id.as_str(),
        FlowState::Paused,
        "flow.paused",
        payload.reason.unwrap_or_else(|| "operator paused flow".to_owned()),
        false,
    )
    .await
}

pub(crate) async fn console_flow_resume_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(flow_id): Path<String>,
    Json(payload): Json<ConsoleFlowActionRequest>,
) -> Result<Json<Value>, Response> {
    transition_console_flow(
        &state,
        &headers,
        flow_id.as_str(),
        FlowState::Pending,
        "flow.resumed",
        payload.reason.unwrap_or_else(|| "operator resumed flow".to_owned()),
        true,
    )
    .await
}

pub(crate) async fn console_flow_cancel_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(flow_id): Path<String>,
    Json(payload): Json<ConsoleFlowActionRequest>,
) -> Result<Json<Value>, Response> {
    transition_console_flow(
        &state,
        &headers,
        flow_id.as_str(),
        FlowState::CancelRequested,
        "flow.cancel_requested",
        payload.reason.unwrap_or_else(|| "operator requested flow cancellation".to_owned()),
        false,
    )
    .await
}

pub(crate) async fn console_flow_step_retry_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((flow_id, step_id)): Path<(String, String)>,
    Json(payload): Json<ConsoleFlowActionRequest>,
) -> Result<Json<Value>, Response> {
    update_console_flow_step(
        &state,
        &headers,
        ConsoleFlowStepAction {
            flow_id: flow_id.as_str(),
            step_id: step_id.as_str(),
            next_state: FlowStepState::Retrying,
            event_type: "flow.step.retry_requested",
            reason: payload.reason.unwrap_or_else(|| "operator requested retry".to_owned()),
            terminal: false,
        },
    )
    .await
}

pub(crate) async fn console_flow_step_skip_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((flow_id, step_id)): Path<(String, String)>,
    Json(payload): Json<ConsoleFlowActionRequest>,
) -> Result<Json<Value>, Response> {
    update_console_flow_step(
        &state,
        &headers,
        ConsoleFlowStepAction {
            flow_id: flow_id.as_str(),
            step_id: step_id.as_str(),
            next_state: FlowStepState::Skipped,
            event_type: "flow.step.skipped",
            reason: payload.reason.unwrap_or_else(|| "operator skipped step".to_owned()),
            terminal: true,
        },
    )
    .await
}

pub(crate) async fn console_flow_step_compensate_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((flow_id, step_id)): Path<(String, String)>,
    Json(payload): Json<ConsoleFlowActionRequest>,
) -> Result<Json<Value>, Response> {
    update_console_flow_step(
        &state,
        &headers,
        ConsoleFlowStepAction {
            flow_id: flow_id.as_str(),
            step_id: step_id.as_str(),
            next_state: FlowStepState::Compensating,
            event_type: "flow.step.compensation_requested",
            reason: payload.reason.unwrap_or_else(|| "operator requested compensation".to_owned()),
            terminal: false,
        },
    )
    .await
}

fn build_console_flow_steps(
    steps: Vec<ConsoleFlowStepCreateRequest>,
) -> Result<Vec<FlowStepCreateRequest>, tonic::Status> {
    let adapter_names = flows::flow_adapter_contracts()
        .into_iter()
        .map(|contract| contract.adapter)
        .collect::<Vec<_>>();
    steps
        .into_iter()
        .enumerate()
        .map(|(index, step)| {
            if !adapter_names.contains(&step.adapter.as_str()) {
                return Err(tonic::Status::invalid_argument(format!(
                    "unsupported flow step adapter '{}'",
                    step.adapter
                )));
            }
            let lineage = match step.lineage {
                Some(lineage) => {
                    serde_json::from_value::<FlowLineage>(lineage).map_err(|error| {
                        tonic::Status::invalid_argument(format!("invalid step lineage: {error}"))
                    })?
                }
                None => FlowLineage::default(),
            };
            let mut request = flows::build_flow_step(
                i64::try_from(index).unwrap_or(i64::MAX),
                step.adapter.as_str(),
                step.step_kind.as_deref().unwrap_or(step.adapter.as_str()),
                step.title,
                step.input.unwrap_or_else(|| json!({})),
                lineage,
            );
            request.depends_on_step_ids_json =
                json!(step.depends_on_step_ids.unwrap_or_default()).to_string();
            request.max_attempts = step.max_attempts.unwrap_or(request.max_attempts).max(1);
            request.backoff_ms = step.backoff_ms.unwrap_or(request.backoff_ms);
            request.timeout_ms = step.timeout_ms;
            request.not_before_unix_ms = step.not_before_unix_ms;
            Ok(request)
        })
        .collect()
}

async fn transition_console_flow(
    state: &AppState,
    headers: &HeaderMap,
    flow_id: &str,
    next_state: FlowState,
    event_type: &str,
    reason: String,
    clear_completed_at: bool,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(state, headers, true)?;
    let bundle = load_owned_flow_bundle(state, &session.context.principal, flow_id).await?;
    let completed_at_unix_ms = if clear_completed_at {
        Some(None)
    } else if next_state.is_terminal() {
        Some(Some(crate::gateway::current_unix_ms()))
    } else {
        None
    };
    state
        .runtime
        .transition_flow(FlowTransitionRequest {
            flow_id: flow_id.to_owned(),
            expected_revision: Some(bundle.flow.revision),
            state: next_state.as_str().to_owned(),
            current_step_id: None,
            lock_owner: None,
            lock_expires_at_unix_ms: None,
            completed_at_unix_ms,
            actor_principal: session.context.principal,
            event_type: event_type.to_owned(),
            summary: reason.clone(),
            payload_json: json!({ "reason": reason, "source": "console" }).to_string(),
        })
        .await
        .map_err(runtime_status_response)?;
    let updated = state
        .runtime
        .get_flow_bundle(flow_id.to_owned(), DEFAULT_FLOW_EVENT_LIMIT)
        .await
        .map_err(runtime_status_response)?
        .ok_or_else(|| runtime_status_response(tonic::Status::not_found("flow not found")))?;
    Ok(Json(flow_bundle_view(&updated)))
}

async fn update_console_flow_step(
    state: &AppState,
    headers: &HeaderMap,
    action: ConsoleFlowStepAction<'_>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(state, headers, true)?;
    let bundle = load_owned_flow_bundle(state, &session.context.principal, action.flow_id).await?;
    let step =
        bundle.steps.iter().find(|step| step.step_id == action.step_id).ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found("flow step not found"))
        })?;
    let not_before_unix_ms = if action.next_state == FlowStepState::Retrying {
        Some(Some(crate::gateway::current_unix_ms().saturating_add(step.backoff_ms as i64)))
    } else {
        Some(None)
    };
    state
        .runtime
        .update_flow_step(FlowStepUpdateRequest {
            flow_id: action.flow_id.to_owned(),
            step_id: action.step_id.to_owned(),
            state: Some(action.next_state.as_str().to_owned()),
            increment_attempt_count: false,
            output_json: if action.next_state == FlowStepState::Retrying {
                Some(None)
            } else {
                None
            },
            lineage_json: None,
            not_before_unix_ms,
            waiting_reason: Some(None),
            last_error: Some(None),
            started_at_unix_ms: if action.next_state == FlowStepState::Retrying {
                Some(None)
            } else {
                None
            },
            completed_at_unix_ms: if action.terminal {
                Some(Some(crate::gateway::current_unix_ms()))
            } else if action.next_state == FlowStepState::Retrying {
                Some(None)
            } else {
                None
            },
            actor_principal: session.context.principal.clone(),
            event_type: action.event_type.to_owned(),
            summary: action.reason.clone(),
            payload_json: json!({ "reason": action.reason, "source": "console" }).to_string(),
        })
        .await
        .map_err(runtime_status_response)?;
    if action.next_state == FlowStepState::Retrying {
        let latest = state
            .runtime
            .get_flow_bundle(action.flow_id.to_owned(), DEFAULT_FLOW_EVENT_LIMIT)
            .await
            .map_err(runtime_status_response)?
            .ok_or_else(|| runtime_status_response(tonic::Status::not_found("flow not found")))?;
        let _ = state
            .runtime
            .transition_flow(FlowTransitionRequest {
                flow_id: action.flow_id.to_owned(),
                expected_revision: Some(latest.flow.revision),
                state: FlowState::Pending.as_str().to_owned(),
                current_step_id: Some(Some(action.step_id.to_owned())),
                lock_owner: None,
                lock_expires_at_unix_ms: None,
                completed_at_unix_ms: Some(None),
                actor_principal: session.context.principal,
                event_type: "flow.retry_reactivated".to_owned(),
                summary: "operator retry reactivated flow".to_owned(),
                payload_json: json!({ "step_id": action.step_id }).to_string(),
            })
            .await;
    }
    let updated = state
        .runtime
        .get_flow_bundle(action.flow_id.to_owned(), DEFAULT_FLOW_EVENT_LIMIT)
        .await
        .map_err(runtime_status_response)?
        .ok_or_else(|| runtime_status_response(tonic::Status::not_found("flow not found")))?;
    Ok(Json(flow_bundle_view(&updated)))
}

async fn load_owned_flow_bundle(
    state: &AppState,
    principal: &str,
    flow_id: &str,
) -> Result<crate::journal::FlowBundleRecord, Response> {
    let bundle = state
        .runtime
        .get_flow_bundle(flow_id.to_owned(), DEFAULT_FLOW_EVENT_LIMIT)
        .await
        .map_err(runtime_status_response)?
        .ok_or_else(|| runtime_status_response(tonic::Status::not_found("flow not found")))?;
    if bundle.flow.owner_principal != principal {
        return Err(runtime_status_response(tonic::Status::permission_denied(
            "flow belongs to a different principal",
        )));
    }
    Ok(bundle)
}

fn flow_bundle_view(bundle: &crate::journal::FlowBundleRecord) -> Value {
    json!({
        "flow": flow_view(&bundle.flow),
        "steps": bundle.steps.iter().map(step_view).collect::<Vec<_>>(),
        "events": bundle.events.iter().map(event_view).collect::<Vec<_>>(),
        "revisions": bundle.revisions.iter().map(revision_view).collect::<Vec<_>>(),
        "blockers": current_blockers(bundle.steps.as_slice()),
        "retry_history": retry_history(bundle.events.as_slice()),
        "lineage": bundle.steps.iter().map(step_lineage_view).collect::<Vec<_>>(),
    })
}

fn flow_view(flow: &FlowRecord) -> Value {
    json!({
        "flow_id": flow.flow_id,
        "mode": flow.mode,
        "state": flow.state,
        "owner_principal": flow.owner_principal,
        "device_id": flow.device_id,
        "channel": flow.channel,
        "session_id": flow.session_id,
        "origin_run_id": flow.origin_run_id,
        "objective_id": flow.objective_id,
        "routine_id": flow.routine_id,
        "webhook_id": flow.webhook_id,
        "title": flow.title,
        "summary": flow.summary,
        "current_step_id": flow.current_step_id,
        "revision": flow.revision,
        "lock_owner": flow.lock_owner,
        "lock_expires_at_unix_ms": flow.lock_expires_at_unix_ms,
        "retry_policy": json_value(flow.retry_policy_json.as_str()),
        "timeout_ms": flow.timeout_ms,
        "metadata": json_value(flow.metadata_json.as_str()),
        "created_at_unix_ms": flow.created_at_unix_ms,
        "updated_at_unix_ms": flow.updated_at_unix_ms,
        "completed_at_unix_ms": flow.completed_at_unix_ms,
    })
}

fn step_view(step: &FlowStepRecord) -> Value {
    json!({
        "step_id": step.step_id,
        "flow_id": step.flow_id,
        "step_index": step.step_index,
        "step_kind": step.step_kind,
        "adapter": step.adapter,
        "state": step.state,
        "title": step.title,
        "input": json_value(step.input_json.as_str()),
        "output": step.output_json.as_deref().map(json_value),
        "lineage": json_value(step.lineage_json.as_str()),
        "depends_on_step_ids": json_value(step.depends_on_step_ids_json.as_str()),
        "attempt_count": step.attempt_count,
        "max_attempts": step.max_attempts,
        "backoff_ms": step.backoff_ms,
        "timeout_ms": step.timeout_ms,
        "not_before_unix_ms": step.not_before_unix_ms,
        "waiting_reason": step.waiting_reason,
        "last_error": step.last_error,
        "created_at_unix_ms": step.created_at_unix_ms,
        "updated_at_unix_ms": step.updated_at_unix_ms,
        "started_at_unix_ms": step.started_at_unix_ms,
        "completed_at_unix_ms": step.completed_at_unix_ms,
    })
}

fn event_view(event: &crate::journal::FlowEventRecord) -> Value {
    json!({
        "event_id": event.event_id,
        "flow_id": event.flow_id,
        "step_id": event.step_id,
        "event_type": event.event_type,
        "actor_principal": event.actor_principal,
        "from_state": event.from_state,
        "to_state": event.to_state,
        "summary": event.summary,
        "payload": json_value(event.payload_json.as_str()),
        "created_at_unix_ms": event.created_at_unix_ms,
    })
}

fn revision_view(revision: &crate::journal::FlowRevisionRecord) -> Value {
    json!({
        "revision_id": revision.revision_id,
        "flow_id": revision.flow_id,
        "revision": revision.revision,
        "parent_revision": revision.parent_revision,
        "change_kind": revision.change_kind,
        "actor_principal": revision.actor_principal,
        "payload": json_value(revision.payload_json.as_str()),
        "created_at_unix_ms": revision.created_at_unix_ms,
    })
}

fn step_lineage_view(step: &FlowStepRecord) -> Value {
    json!({
        "step_id": step.step_id,
        "adapter": step.adapter,
        "lineage": json_value(step.lineage_json.as_str()),
    })
}

fn current_blockers(steps: &[FlowStepRecord]) -> Vec<Value> {
    steps
        .iter()
        .filter(|step| {
            matches!(
                FlowStepState::from_str(step.state.as_str()),
                Some(
                    FlowStepState::Blocked
                        | FlowStepState::WaitingForApproval
                        | FlowStepState::TimedOut
                )
            )
        })
        .map(|step| {
            json!({
                "step_id": step.step_id,
                "state": step.state,
                "reason": step.waiting_reason.as_ref().or(step.last_error.as_ref()),
            })
        })
        .collect()
}

fn retry_history(events: &[crate::journal::FlowEventRecord]) -> Vec<Value> {
    events.iter().filter(|event| event.event_type.contains("retry")).map(event_view).collect()
}

fn summarize_flows(flows: &[FlowRecord]) -> Value {
    let active = flows
        .iter()
        .filter(|flow| {
            !FlowState::from_str(flow.state.as_str()).is_some_and(FlowState::is_terminal)
        })
        .count();
    let waiting =
        flows.iter().filter(|flow| flow.state == FlowState::WaitingForApproval.as_str()).count();
    let blocked = flows.iter().filter(|flow| flow.state == FlowState::Blocked.as_str()).count();
    json!({
        "total": flows.len(),
        "active": active,
        "waiting_for_approval": waiting,
        "blocked": blocked,
    })
}

fn flow_rollout_payload(state: &AppState) -> Value {
    json!({
        "mode": state.runtime.config.flow_orchestration.mode.as_str(),
        "rollout_enabled": state.runtime.config.feature_rollouts.flow_orchestration.enabled,
        "rollout_source": state.runtime.config.feature_rollouts.flow_orchestration.source,
        "max_retry_count": state.runtime.config.flow_orchestration.max_retry_count,
        "cancellation_gate_enabled": state.runtime.config.flow_orchestration.cancellation_gate_enabled,
    })
}

fn json_value(raw: &str) -> Value {
    serde_json::from_str(raw).unwrap_or_else(|_| json!({ "raw": raw }))
}
