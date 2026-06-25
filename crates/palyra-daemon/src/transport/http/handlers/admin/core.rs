//! Core admin HTTP handlers for runtime diagnostics and run control.
//!
//! These handlers are intentionally thin adapters around daemon runtime
//! services so auth, counters, and response shaping stay consistent with the
//! rest of the transport layer.

use crate::*;

/// Returns the aggregate admin status document for the daemon.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction,
/// runtime snapshot collection, serialization, or diagnostics assembly fails.
pub(crate) async fn admin_status_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let snapshot = state
        .runtime
        .status_snapshot_async(context.clone(), state.auth.clone())
        .await
        .map_err(runtime_status_response)?;
    let auth_snapshot = state
        .auth_runtime
        .admin_status_snapshot(Arc::clone(&state.runtime))
        .await
        .map_err(runtime_status_response)?;
    let mut payload = serde_json::to_value(&snapshot).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to serialize admin status snapshot: {error}"
        )))
    })?;
    let auth_payload = serde_json::to_value(auth_snapshot).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to serialize auth status snapshot: {error}"
        )))
    })?;
    let media_payload = state.channels.media_snapshot().map_err(channel_platform_error_response)?;
    let observability_payload = build_observability_payload(
        &state,
        &context,
        &snapshot.model_provider,
        &auth_payload,
        &media_payload,
    )
    .await?;
    let tool_jobs = state
        .runtime
        .list_tool_jobs(crate::journal::ToolJobsListFilter {
            owner_principal: Some(context.principal.clone()),
            session_id: None,
            run_id: None,
            include_terminal: true,
            limit: 256,
        })
        .await
        .map_err(runtime_status_response)?;
    let generated_at_unix_ms = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock: {error}"
        )))
    })?;
    let memory_payload = json!({
        "usage": {
            "entries": snapshot.counters.memory_items_ingested,
            "bytes": 0,
        },
        "providers": [],
    });
    let skills_payload = collect_console_skills_diagnostics(&state).await;
    let plugins_payload = collect_console_plugins_diagnostics();
    let networked_workers_payload = collect_console_networked_worker_diagnostics(&state);
    let null_payload = Value::Null;
    let runtime_preview_payload =
        observability_payload.pointer("/runtime_preview").unwrap_or(&null_payload);
    let support_bundle_payload =
        observability_payload.pointer("/support_bundle").unwrap_or(&null_payload);
    let runtime_health = crate::runtime_diagnostics::build_runtime_health_snapshot(
        generated_at_unix_ms,
        &snapshot,
        &auth_payload,
        &memory_payload,
        &skills_payload,
        &plugins_payload,
        &networked_workers_payload,
        support_bundle_payload,
        runtime_preview_payload,
        &tool_jobs,
    );
    let runtime_metrics = crate::runtime_diagnostics::build_agent_runtime_metrics_snapshot(
        &snapshot,
        runtime_preview_payload,
        &memory_payload,
        &tool_jobs,
    );
    if let Value::Object(ref mut map) = payload {
        map.insert("auth".to_owned(), auth_payload);
        map.insert("media".to_owned(), media_payload);
        map.insert("observability".to_owned(), observability_payload);
        map.insert(
            "runtime_health".to_owned(),
            serde_json::to_value(runtime_health).map_err(|error| {
                runtime_status_response(tonic::Status::internal(format!(
                    "failed to serialize runtime health snapshot: {error}"
                )))
            })?,
        );
        map.insert("agent_runtime_metrics".to_owned(), runtime_metrics);
    }
    Ok(Json(payload))
}

/// Renders daemon runtime metrics in Prometheus text format.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction, or
/// runtime metric snapshot collection fails.
pub(crate) async fn admin_metrics_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let snapshot = state
        .runtime
        .status_snapshot_async(context.clone(), state.auth.clone())
        .await
        .map_err(runtime_status_response)?;
    let tool_jobs = state
        .runtime
        .list_tool_jobs(crate::journal::ToolJobsListFilter {
            owner_principal: Some(context.principal),
            session_id: None,
            run_id: None,
            include_terminal: true,
            limit: 256,
        })
        .await
        .map_err(runtime_status_response)?;
    let body = crate::runtime_diagnostics::render_prometheus_metrics(&snapshot, &tool_jobs);
    Ok(([(CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")], body).into_response())
}

/// Returns the most recent gateway journal records.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction, or
/// journal snapshot collection fails.
pub(crate) async fn admin_journal_recent_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<JournalRecentQuery>,
) -> Result<Json<gateway::JournalRecentSnapshot>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let limit = query.limit.unwrap_or(20);
    let snapshot =
        state.runtime.recent_journal_snapshot(limit).await.map_err(runtime_status_response)?;
    Ok(Json(snapshot))
}

/// Explains the policy decision for an operator-supplied principal/action pair.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction, or
/// policy evaluation fails.
pub(crate) async fn admin_policy_explain_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<PolicyExplainQuery>,
) -> Result<Json<PolicyExplainResponse>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();

    let request = PolicyRequest {
        principal: query.principal,
        action: query.action,
        resource: query.resource,
    };
    let requested_tool = requested_tool_for_admin_policy_explain(&request);
    let request_context = palyra_policy::PolicyRequestContext {
        device_id: query.device_id,
        channel: query.channel,
        session_id: query.session_id,
        run_id: query.run_id,
        tool_name: requested_tool.clone(),
        skill_id: None,
        capabilities: requested_tool
            .as_deref()
            .map(crate::tool_protocol::tool_policy_capability_names)
            .unwrap_or_default(),
    };
    let evaluation_config = PolicyEvaluationConfig {
        allowlisted_tools: state.runtime.config.tool_call.allowed_tools.clone(),
        sensitive_tool_names: palyra_common::tool_catalog::sensitive_allowlisted_tool_names(
            state.runtime.config.tool_call.allowed_tools.as_slice(),
        ),
        sensitive_capability_names: palyra_common::tool_catalog::SENSITIVE_CAPABILITY_POLICY_NAMES
            .iter()
            .map(|value| (*value).to_owned())
            .collect(),
        ..PolicyEvaluationConfig::default()
    };
    let evaluation =
        palyra_policy::evaluate_with_context(&request, &request_context, &evaluation_config)
            .map_err(|error| {
                runtime_status_response(tonic::Status::internal(format!(
                    "failed to evaluate policy with Cedar engine: {error}"
                )))
            })?;
    let diagnostics = palyra_policy::policy_explain_diagnostics_value(&request, &evaluation);
    let (decision, approval_required, reason) = match &evaluation.decision {
        PolicyDecision::Allow => ("allow".to_owned(), false, evaluation.explanation.reason.clone()),
        PolicyDecision::DenyByDefault { reason } => {
            ("deny_by_default".to_owned(), true, reason.clone())
        }
    };
    let runtime_approval_tool = requested_tool;
    let runtime_approval_required = runtime_approval_tool
        .as_deref()
        .map(crate::tool_protocol::tool_requires_approval)
        .unwrap_or(false);

    Ok(Json(PolicyExplainResponse {
        principal: request.principal,
        action: request.action,
        resource: request.resource,
        decision,
        approval_required,
        runtime_approval_required,
        runtime_approval_tool,
        reason,
        matched_policies: evaluation.explanation.matched_policy_ids,
        diagnostics,
    }))
}

fn requested_tool_for_admin_policy_explain(request: &PolicyRequest) -> Option<String> {
    if !request.action.eq_ignore_ascii_case("tool.execute") {
        return None;
    }
    let trimmed = request.resource.trim();
    if trimmed.is_empty() {
        return None;
    }
    let tool_name = trimmed.strip_prefix("tool:").unwrap_or(trimmed).trim();
    if tool_name.is_empty() {
        None
    } else {
        Some(tool_name.to_ascii_lowercase())
    }
}

/// Returns an orchestrator run status snapshot for admin diagnostics.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction,
/// run-id validation, run-id resolution, or status snapshot lookup fails.
pub(crate) async fn admin_run_status_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    validate_canonical_id(run_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument("run_id must be a canonical ULID"))
    })?;
    state.runtime.record_admin_status_request();
    let diagnostics_run_id = resolve_admin_diagnostics_run_id(&state, run_id.as_str()).await?;
    let snapshot = state
        .runtime
        .orchestrator_run_status_snapshot(diagnostics_run_id.clone())
        .await
        .map_err(runtime_status_response)?;
    let Some(snapshot) = snapshot else {
        return Err(runtime_status_response(tonic::Status::not_found(format!(
            "orchestrator run not found after resolving {run_id} to {diagnostics_run_id}"
        ))));
    };
    Ok(Json(
        run_status_payload(&snapshot)
            .map_err(|error| runtime_status_response(tonic::Status::internal(error)))?,
    ))
}

fn run_status_payload(snapshot: &OrchestratorRunStatusSnapshot) -> Result<Value, String> {
    let mut payload = serde_json::to_value(snapshot)
        .map_err(|error| format!("failed to serialize run status snapshot: {error}"))?;
    enrich_run_status_lifecycle(&mut payload, snapshot);
    Ok(payload)
}

fn enrich_run_status_lifecycle(payload: &mut Value, snapshot: &OrchestratorRunStatusSnapshot) {
    if snapshot.state != "failed" {
        return;
    }
    let Some(message) = snapshot.last_error.as_deref() else {
        return;
    };
    if !is_needs_continuation_message(message) {
        return;
    }
    payload["wire_state"] = Value::String(snapshot.state.clone());
    payload["lifecycle_state"] = Value::String("needs_continuation".to_owned());
    payload["partial"] = Value::Bool(true);
    payload["continuation_required"] = Value::Bool(true);
    payload["continuation_available"] = Value::Bool(true);
    if let Some(reason_code) = continuation_reason_code(message) {
        payload["reason_code"] = Value::String(reason_code.to_owned());
    }
}

fn is_needs_continuation_message(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("needs_continuation=true") || lower.contains("needs continuation")
}

fn continuation_reason_code(message: &str) -> Option<&str> {
    let marker = "reason_code=";
    let start = message.find(marker)? + marker.len();
    let rest = &message[start..];
    let end = rest
        .find(|ch: char| ch.is_whitespace() || matches!(ch, ';' | ',' | ')'))
        .unwrap_or(rest.len());
    let reason = rest[..end].trim();
    (!reason.is_empty()).then_some(reason)
}

/// Returns a paginated orchestrator run tape for admin diagnostics.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction,
/// run-id validation, run-id resolution, or tape snapshot lookup fails.
pub(crate) async fn admin_run_tape_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
    Query(query): Query<RunTapeQuery>,
) -> Result<Json<gateway::RunTapeSnapshot>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    validate_canonical_id(run_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument("run_id must be a canonical ULID"))
    })?;
    state.runtime.record_admin_status_request();
    let diagnostics_run_id = resolve_admin_diagnostics_run_id(&state, run_id.as_str()).await?;
    let snapshot = state
        .runtime
        .orchestrator_tape_snapshot(diagnostics_run_id, query.after_seq, query.limit)
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(snapshot))
}

async fn resolve_admin_diagnostics_run_id(
    state: &AppState,
    requested_run_id: &str,
) -> Result<String, Response> {
    state
        .runtime
        .resolve_orchestrator_diagnostics_run_id(requested_run_id.to_owned())
        .await
        .map_err(runtime_status_response)?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found(format!(
                "orchestrator run not found: {requested_run_id}; gateway diagnostics accept \
                 orchestrator_run_id and linked routine/cron run_id values. If this id came from \
                 objective or routine output, use orchestrator_run_id when available or retry after \
                 the run links one."
            )))
    })
}

/// Requests cancellation of an orchestrator run from the admin API.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction,
/// run-id validation, cancellation request recording, or cleanup signaling
/// fails.
pub(crate) async fn admin_run_cancel_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
    payload: Option<Json<RunCancelRequest>>,
) -> Result<Json<gateway::RunCancelSnapshot>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    validate_canonical_id(run_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument("run_id must be a canonical ULID"))
    })?;
    state.runtime.record_admin_status_request();
    let reason = payload
        .and_then(|body| body.0.reason)
        .and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_owned())
            }
        })
        .unwrap_or_else(|| "admin_cancel_requested".to_owned());
    let response = state
        .runtime
        .request_orchestrator_cancel(OrchestratorCancelRequest { run_id, reason })
        .await
        .map_err(runtime_status_response)?;
    gateway::cleanup_run_resources(
        &state.runtime,
        response.run_id.as_str(),
        response.reason.as_str(),
    )
    .await;
    Ok(Json(response))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_status_payload_marks_resumable_failed_runs_as_needs_continuation() {
        let snapshot = OrchestratorRunStatusSnapshot {
            run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAS".to_owned(),
            state: "failed".to_owned(),
            cancel_requested: false,
            cancel_reason: None,
            principal: "user:ops".to_owned(),
            device_id: "device:test".to_owned(),
            channel: None,
            prompt_tokens: 10,
            completion_tokens: 2,
            total_tokens: 12,
            created_at_unix_ms: 1,
            started_at_unix_ms: 2,
            completed_at_unix_ms: Some(3),
            updated_at_unix_ms: 3,
            last_error: Some(
                "agent loop wall-clock budget exhausted; needs_continuation=true reason_code=wall_clock"
                    .to_owned(),
            ),
            origin_kind: "agent_run".to_owned(),
            origin_run_id: None,
            parent_run_id: None,
            triggered_by_principal: None,
            parameter_delta_json: None,
            delegation: None,
            merge_result: None,
            tape_events: 42,
        };

        let payload = run_status_payload(&snapshot).expect("snapshot should serialize");

        assert_eq!(payload["state"], "failed");
        assert_eq!(payload["wire_state"], "failed");
        assert_eq!(payload["lifecycle_state"], "needs_continuation");
        assert_eq!(payload["continuation_required"], true);
        assert_eq!(payload["continuation_available"], true);
        assert_eq!(payload["partial"], true);
        assert_eq!(payload["reason_code"], "wall_clock");
    }
}
