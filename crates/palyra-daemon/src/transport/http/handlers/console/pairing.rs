use crate::*;

#[derive(Debug, Default, Deserialize)]
pub(crate) struct ConsoleNodePairingListQuery {
    limit: Option<usize>,
    #[serde(default, alias = "status")]
    state: Option<String>,
    client_kind: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct ConsoleNodePairingDecisionRequest {
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    decision_scope: Option<String>,
    #[serde(default)]
    decision_scope_ttl_ms: Option<i64>,
}

pub(crate) async fn console_pairing_summary_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<control_plane::PairingSummaryEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    Ok(Json(control_plane::PairingSummaryEnvelope {
        contract: contract_descriptor(),
        channels: state
            .runtime
            .channel_router_pairing_snapshot(None)
            .iter()
            .map(control_plane_pairing_snapshot_from_runtime)
            .collect(),
    }))
}

pub(crate) async fn console_node_pairing_requests_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleNodePairingListQuery>,
) -> Result<Json<control_plane::NodePairingListEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let limit = query.limit.unwrap_or(100).clamp(1, 500);
    let mut requests = state.node_runtime.pairing_requests().map_err(runtime_status_response)?;
    if let Some(state_filter) =
        query.state.as_deref().map(str::trim).filter(|value| !value.is_empty())
    {
        requests.retain(|record| record.state.as_str() == state_filter);
    }
    if let Some(client_kind) =
        query.client_kind.as_deref().map(str::trim).filter(|value| !value.is_empty())
    {
        requests.retain(|record| record.client_kind.as_str() == client_kind);
    }
    requests.truncate(limit);
    let codes = state
        .node_runtime
        .pairing_codes()
        .map_err(runtime_status_response)?
        .iter()
        .map(control_plane_node_pairing_code_view)
        .collect::<Vec<_>>();
    Ok(Json(control_plane::NodePairingListEnvelope {
        contract: contract_descriptor(),
        codes,
        requests: requests.iter().map(control_plane_node_pairing_request_view).collect(),
        page: build_page_info(limit, requests.len(), None),
    }))
}

pub(crate) async fn console_node_pairing_code_mint_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<control_plane::NodePairingCodeMintRequest>,
) -> Result<Json<control_plane::NodePairingCodeEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let method = match payload.method {
        control_plane::NodePairingMethod::Pin => crate::node_runtime::PairingCodeMethod::Pin,
        control_plane::NodePairingMethod::Qr => crate::node_runtime::PairingCodeMethod::Qr,
    };
    let record = state
        .node_runtime
        .mint_pairing_code(
            method,
            payload.issued_by.as_deref().unwrap_or(session.context.principal.as_str()),
            payload.ttl_ms,
        )
        .map_err(runtime_status_response)?;
    Ok(Json(control_plane::NodePairingCodeEnvelope {
        contract: contract_descriptor(),
        code: control_plane_node_pairing_code_view(&record),
    }))
}

pub(crate) async fn console_node_pairing_approve_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(request_id): Path<String>,
    Json(payload): Json<ConsoleNodePairingDecisionRequest>,
) -> Result<Json<control_plane::NodePairingRequestEnvelope>, Response> {
    resolve_node_pairing_decision(state, headers, request_id, payload, true).await
}

pub(crate) async fn console_node_pairing_reject_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(request_id): Path<String>,
    Json(payload): Json<ConsoleNodePairingDecisionRequest>,
) -> Result<Json<control_plane::NodePairingRequestEnvelope>, Response> {
    resolve_node_pairing_decision(state, headers, request_id, payload, false).await
}

async fn resolve_node_pairing_decision(
    state: AppState,
    headers: HeaderMap,
    request_id: String,
    payload: ConsoleNodePairingDecisionRequest,
    approved: bool,
) -> Result<Json<control_plane::NodePairingRequestEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    validate_canonical_id(request_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "request_id must be a canonical ULID",
        ))
    })?;
    let request = state
        .node_runtime
        .pairing_request(request_id.as_str())
        .map_err(runtime_status_response)?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found("pairing request was not found"))
        })?;
    let decision_scope = parse_pairing_decision_scope(payload.decision_scope.as_deref())
        .map_err(runtime_status_response)?;
    let reason = payload
        .reason
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| {
            if approved {
                "approved_by_operator".to_owned()
            } else {
                "rejected_by_operator".to_owned()
            }
        });
    let resolved = state
        .runtime
        .resolve_approval_record(journal::ApprovalResolveRequest {
            approval_id: request.approval_id.clone(),
            decision: if approved { ApprovalDecision::Allow } else { ApprovalDecision::Deny },
            decision_scope,
            decision_reason: reason.clone(),
            decision_scope_ttl_ms: payload.decision_scope_ttl_ms,
        })
        .await
        .map_err(runtime_status_response)?;
    let updated = state
        .node_runtime
        .apply_pairing_approval(
            request.approval_id.as_str(),
            approved,
            reason.as_str(),
            resolved.decision_scope_ttl_ms,
        )
        .map_err(runtime_status_response)?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found(
                "pairing request disappeared during decision",
            ))
        })?;
    crate::application::approvals::record_approval_resolved_journal_event(
        &state.runtime,
        &session.context,
        resolved.session_id.as_str(),
        resolved.run_id.as_str(),
        None,
        resolved.approval_id.as_str(),
        resolved.decision.unwrap_or(ApprovalDecision::Error),
        resolved.decision_scope.unwrap_or(ApprovalDecisionScope::Once),
        resolved.decision_scope_ttl_ms,
        resolved.decision_reason.as_deref().unwrap_or(reason.as_str()),
    )
    .await
    .map_err(runtime_status_response)?;
    Ok(Json(control_plane::NodePairingRequestEnvelope {
        contract: contract_descriptor(),
        request: control_plane_node_pairing_request_view(&updated),
    }))
}

fn parse_pairing_decision_scope(
    value: Option<&str>,
) -> Result<ApprovalDecisionScope, tonic::Status> {
    let Some(raw) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(ApprovalDecisionScope::Once);
    };
    match raw.to_ascii_lowercase().as_str() {
        "once" => Ok(ApprovalDecisionScope::Once),
        "session" => Ok(ApprovalDecisionScope::Session),
        "timeboxed" => Ok(ApprovalDecisionScope::Timeboxed),
        _ => Err(tonic::Status::invalid_argument(
            "decision_scope must be one of once|session|timeboxed",
        )),
    }
}

pub(crate) async fn console_pairing_code_mint_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<control_plane::PairingCodeMintRequest>,
) -> Result<Json<control_plane::PairingSummaryEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    state
        .runtime
        .channel_router_mint_pairing_code(
            payload.channel.as_str(),
            payload.issued_by.as_deref().unwrap_or(session.context.principal.as_str()),
            payload.ttl_ms,
        )
        .map_err(runtime_status_response)?;
    Ok(Json(control_plane::PairingSummaryEnvelope {
        contract: contract_descriptor(),
        channels: state
            .runtime
            .channel_router_pairing_snapshot(Some(payload.channel.as_str()))
            .iter()
            .map(control_plane_pairing_snapshot_from_runtime)
            .collect(),
    }))
}

pub(crate) fn control_plane_node_pairing_code_view(
    record: &crate::node_runtime::DevicePairingCodeRecord,
) -> control_plane::NodePairingCodeView {
    control_plane::NodePairingCodeView {
        code: record.code.clone(),
        method: control_plane_node_pairing_method(record.method),
        issued_by: record.issued_by.clone(),
        created_at_unix_ms: record.created_at_unix_ms,
        expires_at_unix_ms: record.expires_at_unix_ms,
    }
}

pub(crate) fn control_plane_node_pairing_request_view(
    record: &crate::node_runtime::DevicePairingRequestRecord,
) -> control_plane::NodePairingRequestView {
    control_plane::NodePairingRequestView {
        request_id: record.request_id.clone(),
        session_id: record.session_id.clone(),
        device_id: record.device_id.clone(),
        client_kind: record.client_kind.as_str().to_owned(),
        method: control_plane_node_pairing_method(record.method),
        code_issued_by: record.code_issued_by.clone(),
        requested_at_unix_ms: record.requested_at_unix_ms,
        expires_at_unix_ms: record.expires_at_unix_ms,
        approval_id: record.approval_id.clone(),
        state: control_plane_node_pairing_request_state(record.state),
        decision_reason: record.decision_reason.clone(),
        decision_scope_ttl_ms: record.decision_scope_ttl_ms,
        identity_fingerprint: record.verified_pairing.identity_fingerprint.clone(),
        transcript_hash_hex: record.verified_pairing.transcript_hash_hex.clone(),
        cert_expires_at_unix_ms: record
            .material
            .as_ref()
            .map(|value| value.cert_expires_at_unix_ms),
    }
}

fn control_plane_node_pairing_method(
    method: crate::node_runtime::PairingCodeMethod,
) -> control_plane::NodePairingMethod {
    match method {
        crate::node_runtime::PairingCodeMethod::Pin => control_plane::NodePairingMethod::Pin,
        crate::node_runtime::PairingCodeMethod::Qr => control_plane::NodePairingMethod::Qr,
    }
}

fn control_plane_node_pairing_request_state(
    state: crate::node_runtime::DevicePairingRequestState,
) -> control_plane::NodePairingRequestState {
    match state {
        crate::node_runtime::DevicePairingRequestState::PendingApproval => {
            control_plane::NodePairingRequestState::PendingApproval
        }
        crate::node_runtime::DevicePairingRequestState::Approved => {
            control_plane::NodePairingRequestState::Approved
        }
        crate::node_runtime::DevicePairingRequestState::Rejected => {
            control_plane::NodePairingRequestState::Rejected
        }
        crate::node_runtime::DevicePairingRequestState::Completed => {
            control_plane::NodePairingRequestState::Completed
        }
        crate::node_runtime::DevicePairingRequestState::Expired => {
            control_plane::NodePairingRequestState::Expired
        }
    }
}
