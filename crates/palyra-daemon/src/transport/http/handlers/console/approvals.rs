use crate::*;

pub(crate) async fn console_approvals_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleApprovalsQuery>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let limit = query.limit.unwrap_or(100).clamp(1, 500);
    let decision = parse_console_approval_decision(query.decision.as_deref())?;
    let subject_type = parse_console_approval_subject_type(query.subject_type.as_deref())?;
    let (approvals, next_after_approval_id) = state
        .runtime
        .list_approval_records(
            query.after_approval_id,
            query.limit,
            query.since_unix_ms,
            query.until_unix_ms,
            query.subject_id,
            query.principal,
            decision,
            subject_type,
        )
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "approvals": approvals,
        "next_after_approval_id": next_after_approval_id,
        "page": build_page_info(limit, approvals.len(), next_after_approval_id.clone()),
    })))
}

pub(crate) async fn console_approval_get_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(approval_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    validate_canonical_id(approval_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "approval_id must be a canonical ULID",
        ))
    })?;
    let record = state
        .runtime
        .approval_record(approval_id.clone())
        .await
        .map_err(runtime_status_response)?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found(format!(
                "approval record not found: {approval_id}"
            )))
        })?;
    Ok(Json(json!({ "approval": record })))
}

pub(crate) async fn console_approval_decision_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(approval_id): Path<String>,
    Json(payload): Json<ConsoleApprovalDecisionRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    validate_canonical_id(approval_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "approval_id must be a canonical ULID",
        ))
    })?;
    let decision_scope = parse_console_decision_scope(payload.decision_scope.as_deref())?;
    let reason = payload
        .reason
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| {
            if payload.approved {
                "approved_by_console".to_owned()
            } else {
                "denied_by_console".to_owned()
            }
        });
    let resolved = state
        .runtime
        .resolve_approval_record(journal::ApprovalResolveRequest {
            approval_id,
            decision: if payload.approved {
                ApprovalDecision::Allow
            } else {
                ApprovalDecision::Deny
            },
            decision_scope,
            decision_reason: reason,
            decision_scope_ttl_ms: payload.decision_scope_ttl_ms,
        })
        .await
        .map_err(runtime_status_response)?;
    let pairing_outcome = if matches!(resolved.subject_type, ApprovalSubjectType::ChannelSend)
        && resolved.subject_id.starts_with("dm_pairing:")
    {
        let outcome = state.runtime.channel_router_apply_pairing_approval(
            resolved.approval_id.as_str(),
            matches!(resolved.decision, Some(ApprovalDecision::Allow)),
            resolved.decision_scope_ttl_ms,
        );
        Some(match outcome {
            channel_router::PairingApprovalOutcome::Approved(_) => "approved",
            channel_router::PairingApprovalOutcome::Denied => "denied",
            channel_router::PairingApprovalOutcome::MissingPending => "missing_pending",
            channel_router::PairingApprovalOutcome::PairingDisabled => "pairing_disabled",
        })
    } else {
        None
    };
    let forwarded_to_console_chat = sync_console_chat_approval_to_stream(&state, &resolved).await;
    if !forwarded_to_console_chat {
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
            resolved.decision_reason.as_deref().unwrap_or("approval resolved"),
        )
        .await
        .map_err(runtime_status_response)?;
    }
    Ok(Json(json!({
        "approval": resolved,
        "dm_pairing": pairing_outcome,
    })))
}

#[allow(clippy::result_large_err)]
fn parse_console_approval_decision(
    value: Option<&str>,
) -> Result<Option<ApprovalDecision>, Response> {
    let Some(raw) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    match raw.to_ascii_lowercase().as_str() {
        "allow" => Ok(Some(ApprovalDecision::Allow)),
        "deny" => Ok(Some(ApprovalDecision::Deny)),
        "timeout" => Ok(Some(ApprovalDecision::Timeout)),
        "error" => Ok(Some(ApprovalDecision::Error)),
        _ => Err(runtime_status_response(tonic::Status::invalid_argument(
            "decision must be one of allow|deny|timeout|error",
        ))),
    }
}

#[allow(clippy::result_large_err)]
fn parse_console_approval_subject_type(
    value: Option<&str>,
) -> Result<Option<ApprovalSubjectType>, Response> {
    let Some(raw) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    match raw.to_ascii_lowercase().as_str() {
        "tool" => Ok(Some(ApprovalSubjectType::Tool)),
        "channel_send" => Ok(Some(ApprovalSubjectType::ChannelSend)),
        "secret_access" => Ok(Some(ApprovalSubjectType::SecretAccess)),
        "browser_action" => Ok(Some(ApprovalSubjectType::BrowserAction)),
        "node_capability" => Ok(Some(ApprovalSubjectType::NodeCapability)),
        _ => Err(runtime_status_response(tonic::Status::invalid_argument(
            "subject_type must be one of tool|channel_send|secret_access|browser_action|node_capability",
        ))),
    }
}

#[allow(clippy::result_large_err)]
fn parse_console_decision_scope(value: Option<&str>) -> Result<ApprovalDecisionScope, Response> {
    let Some(raw) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(ApprovalDecisionScope::Once);
    };
    match raw.to_ascii_lowercase().as_str() {
        "once" => Ok(ApprovalDecisionScope::Once),
        "session" => Ok(ApprovalDecisionScope::Session),
        "timeboxed" => Ok(ApprovalDecisionScope::Timeboxed),
        _ => Err(runtime_status_response(tonic::Status::invalid_argument(
            "decision_scope must be one of once|session|timeboxed",
        ))),
    }
}
