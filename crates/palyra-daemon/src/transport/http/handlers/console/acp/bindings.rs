use palyra_common::runtime_contracts::AcpCursor;
use serde::Deserialize;
use serde_json::{json, Value};

use super::{acp_runtime_response, parse_binding_sensitivity};
use crate::*;
use crate::{
    acp::{ConversationBindingFilter, ConversationBindingUpsert},
    transport::grpc::auth::RequestContext,
};

#[derive(Debug, Deserialize)]
pub(crate) struct BindingListQuery {
    owner_principal: Option<String>,
    connector_kind: Option<String>,
    external_identity: Option<String>,
    palyra_session_id: Option<String>,
    include_detached: Option<bool>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConversationBindingUpsertRequest {
    connector_kind: String,
    external_identity: String,
    external_conversation_id: String,
    palyra_session_id: String,
    device_id: Option<String>,
    channel: Option<String>,
    scopes: Option<Vec<String>>,
    sensitivity: Option<String>,
    cursor_sequence: Option<u64>,
    last_event_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BindingRepairApplyRequest {
    apply: bool,
}

pub(crate) async fn console_bindings_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<BindingListQuery>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let owner_principal = query.owner_principal.or_else(|| Some(session.context.principal.clone()));
    if let Some(owner_principal) = owner_principal.as_deref() {
        ensure_rest_binding_owner(&session.context, owner_principal)
            .map_err(runtime_status_response)?;
    }
    let conversation_bindings = state
        .acp_runtime
        .list_conversation_bindings(ConversationBindingFilter {
            owner_principal,
            connector_kind: query.connector_kind,
            external_identity: query.external_identity,
            palyra_session_id: query.palyra_session_id,
            include_detached: query.include_detached.unwrap_or(false),
            limit: query.limit,
        })
        .map_err(acp_runtime_response)?;
    let session_bindings = state
        .acp_runtime
        .list_session_bindings(Some(session.context.principal.as_str()))
        .map_err(acp_runtime_response)?;
    Ok(Json(json!({
        "conversation_bindings": conversation_bindings,
        "session_bindings": session_bindings,
    })))
}

pub(crate) async fn console_binding_upsert_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConversationBindingUpsertRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let binding = state
        .acp_runtime
        .upsert_conversation_binding(conversation_upsert_from_http_payload(
            &session.context,
            payload,
        ))
        .map_err(acp_runtime_response)?;
    let _ = state
        .runtime
        .record_console_event(
            &session.context,
            "acp.binding.upsert",
            json!({
                "binding_id": binding.binding_id,
                "connector_kind": binding.connector_kind,
                "palyra_session_id": binding.palyra_session_id,
                "conflict_state": binding.conflict_state.as_str(),
            }),
        )
        .await;
    Ok(Json(json!({ "binding": binding })))
}

pub(crate) async fn console_binding_get_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(binding_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    if let Ok(binding) = state.acp_runtime.get_conversation_binding(binding_id.as_str()) {
        ensure_rest_binding_owner(&session.context, binding.owner_principal.as_str())
            .map_err(runtime_status_response)?;
        return Ok(Json(json!({ "binding": binding })));
    }
    let snapshot =
        state.acp_runtime.explain_binding(binding_id.as_str()).map_err(acp_runtime_response)?;
    ensure_rest_binding_owner(&session.context, snapshot.owner_principal.as_str())
        .map_err(runtime_status_response)?;
    Ok(Json(json!({ "binding": snapshot })))
}

pub(crate) async fn console_binding_detach_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(binding_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let before =
        state.acp_runtime.explain_binding(binding_id.as_str()).map_err(acp_runtime_response)?;
    ensure_rest_binding_owner(&session.context, before.owner_principal.as_str())
        .map_err(runtime_status_response)?;
    let detached = state
        .acp_runtime
        .detach_conversation_binding(binding_id.as_str())
        .map_err(acp_runtime_response)?;
    let _ = state
        .runtime
        .record_console_event(
            &session.context,
            "acp.binding.detach",
            json!({ "binding_id": binding_id, "palyra_session_id": detached.palyra_session_id }),
        )
        .await;
    Ok(Json(json!({ "binding": detached })))
}

pub(crate) async fn console_bindings_repair_plan_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    ensure_admin_binding_operation(&session.context).map_err(runtime_status_response)?;
    let plan =
        state.acp_runtime.plan_conversation_binding_repair().map_err(acp_runtime_response)?;
    Ok(Json(json!({ "plan": plan })))
}

pub(crate) async fn console_bindings_repair_apply_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<BindingRepairApplyRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    ensure_admin_binding_operation(&session.context).map_err(runtime_status_response)?;
    if !payload.apply {
        let plan =
            state.acp_runtime.plan_conversation_binding_repair().map_err(acp_runtime_response)?;
        return Ok(Json(json!({ "plan": plan, "applied": false })));
    }
    let plan =
        state.acp_runtime.apply_conversation_binding_repair().map_err(acp_runtime_response)?;
    let _ = state
        .runtime
        .record_console_event(
            &session.context,
            "acp.binding.repair.apply",
            json!({ "action_count": plan.actions.len() }),
        )
        .await;
    Ok(Json(json!({ "plan": plan, "applied": true })))
}

pub(crate) async fn console_binding_explain_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(binding_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let snapshot =
        state.acp_runtime.explain_binding(binding_id.as_str()).map_err(acp_runtime_response)?;
    ensure_rest_binding_owner(&session.context, snapshot.owner_principal.as_str())
        .map_err(runtime_status_response)?;
    Ok(Json(json!({ "explain": snapshot })))
}

fn is_admin_principal(request_context: &RequestContext) -> bool {
    request_context.principal.starts_with("admin:")
}

fn ensure_rest_binding_owner(
    request_context: &RequestContext,
    owner_principal: &str,
) -> Result<(), tonic::Status> {
    if request_context.principal == owner_principal || is_admin_principal(request_context) {
        return Ok(());
    }
    Err(tonic::Status::permission_denied("binding belongs to a different principal"))
}

fn ensure_admin_binding_operation(request_context: &RequestContext) -> Result<(), tonic::Status> {
    if is_admin_principal(request_context) {
        return Ok(());
    }
    Err(tonic::Status::permission_denied("binding repair operations require an admin principal"))
}

fn conversation_upsert_from_http_payload(
    request_context: &RequestContext,
    payload: ConversationBindingUpsertRequest,
) -> ConversationBindingUpsert {
    ConversationBindingUpsert {
        connector_kind: payload.connector_kind,
        external_identity: payload.external_identity,
        external_conversation_id: payload.external_conversation_id,
        palyra_session_id: payload.palyra_session_id,
        owner_principal: request_context.principal.clone(),
        device_id: payload.device_id.unwrap_or_else(|| request_context.device_id.clone()),
        channel: payload.channel.or_else(|| request_context.channel.clone()),
        scopes: payload.scopes.unwrap_or_else(|| vec!["sessions:read".to_owned()]),
        sensitivity: parse_binding_sensitivity(payload.sensitivity.as_deref()),
        delivery_cursor: AcpCursor { sequence: payload.cursor_sequence.unwrap_or(0) },
        last_event_id: payload.last_event_id,
    }
}
