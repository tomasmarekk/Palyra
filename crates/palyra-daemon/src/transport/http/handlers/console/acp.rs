use std::collections::BTreeSet;

use palyra_common::runtime_contracts::{
    AcpCapability, AcpClientContext, AcpCommand, AcpCommandEnvelope, AcpCommandResultEnvelope,
    AcpCursor, AcpReplayCap, AcpScope, AcpSessionMode, ConversationBindingSensitivity,
    RealtimeCapability, RealtimeCommand, RealtimeCommandEnvelope, RealtimeCursor,
    RealtimeHandshakeRequest, RealtimeRole, RealtimeScope, StableErrorEnvelope,
    ACP_DEFAULT_REPLAY_MAX_EVENTS,
};
use serde::Deserialize;
use serde_json::{json, Map, Value};

use crate::{
    acp::{
        AcpPendingPromptUpsert, AcpRuntimeError, AcpSessionBindingUpsert,
        ConversationBindingFilter, ConversationBindingUpsert,
    },
    application::session_compaction::{
        apply_session_compaction, preview_session_compaction, SessionCompactionApplyRequest,
    },
    command_router::{dispatch_realtime_command, CommandRouterContext},
    gateway::ListOrchestratorSessionsRequest,
    journal::{
        ApprovalCreateRequest, ApprovalDecisionScope, ApprovalPolicySnapshot, ApprovalPromptOption,
        ApprovalPromptRecord, ApprovalRiskLevel, ApprovalSubjectType,
        OrchestratorSessionLineageUpdateRequest, OrchestratorSessionRecord,
        OrchestratorSessionResolveRequest,
    },
    realtime::negotiate_realtime_handshake,
    transport::grpc::auth::RequestContext,
};

use crate::*;

mod bindings;
mod status;

pub(crate) use bindings::{
    console_binding_detach_handler, console_binding_explain_handler, console_binding_get_handler,
    console_binding_upsert_handler, console_bindings_list_handler,
    console_bindings_repair_apply_handler, console_bindings_repair_plan_handler,
};
pub(crate) use status::console_acp_status_handler;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AcpHttpCommandRequest {
    pub(crate) client: AcpClientContext,
    pub(crate) command: AcpCommandEnvelope,
}

pub(crate) async fn console_acp_command_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<AcpHttpCommandRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let result =
        dispatch_acp_command(&state, &session.context, request.client, request.command).await;
    Ok(Json(json!(result)))
}

async fn dispatch_acp_command(
    state: &AppState,
    request_context: &RequestContext,
    client: AcpClientContext,
    envelope: AcpCommandEnvelope,
) -> AcpCommandResultEnvelope {
    let request_id = envelope.request_id.clone();
    let command = envelope.command;
    let idempotency_key = envelope.idempotency_key.clone();
    let result = async {
        ensure_client_matches_session(&client, request_context)?;
        let now = unix_ms_now().map_err(|error| AcpRuntimeError::InvalidField {
            field: "system_time",
            message: error.to_string(),
        })?;
        state.acp_runtime.check_rate_limit(client.client_id.as_str(), command, now)?;
        let value = execute_acp_command(state, request_context, &client, envelope.clone()).await?;
        Ok::<Value, AcpDispatchError>(value)
    }
    .await;
    match result {
        Ok(value) => {
            let _ = state
                .runtime
                .record_console_event(
                    request_context,
                    "acp.command",
                    json!({
                        "client_id": client.client_id,
                        "command": command.as_str(),
                        "request_id": request_id,
                        "ok": true,
                    }),
                )
                .await;
            crate::acp::AcpRuntime::success_envelope(request_id, command, value, idempotency_key)
        }
        Err(error) => {
            let stable = error.to_stable_error();
            let _ = state
                .runtime
                .record_console_event(
                    request_context,
                    "acp.command",
                    json!({
                        "client_id": client.client_id,
                        "command": command.as_str(),
                        "request_id": request_id,
                        "ok": false,
                        "error_code": stable.code,
                    }),
                )
                .await;
            AcpCommandResultEnvelope {
                request_id,
                command,
                ok: false,
                result: None,
                error: Some(stable),
                idempotency_key,
                replayed: false,
            }
        }
    }
}

async fn execute_acp_command(
    state: &AppState,
    request_context: &RequestContext,
    client: &AcpClientContext,
    envelope: AcpCommandEnvelope,
) -> Result<Value, AcpDispatchError> {
    match envelope.command {
        AcpCommand::SessionList => session_list(state, request_context, client, &envelope).await,
        AcpCommand::SessionLoad => session_load(state, request_context, client, &envelope).await,
        AcpCommand::SessionNew => session_new(state, request_context, client, &envelope).await,
        AcpCommand::SessionReplay => {
            session_replay(state, request_context, client, &envelope).await
        }
        AcpCommand::SessionResume | AcpCommand::Reconnect => {
            session_reconnect(state, request_context, client, &envelope).await
        }
        AcpCommand::SessionFork => session_fork(state, request_context, client, &envelope).await,
        AcpCommand::SessionCompactPreview => {
            session_compact_preview(state, request_context, client, &envelope).await
        }
        AcpCommand::SessionCompactApply => {
            session_compact_apply(state, request_context, client, &envelope).await
        }
        AcpCommand::SessionExplain => {
            session_explain(state, request_context, client, &envelope).await
        }
        AcpCommand::SessionModeSet | AcpCommand::SessionConfigSet => {
            session_config_set(state, request_context, client, &envelope).await
        }
        AcpCommand::RunCreate
        | AcpCommand::RunAbort
        | AcpCommand::ApprovalList
        | AcpCommand::ApprovalDecide => {
            dispatch_via_command_router(state, request_context, client, envelope).await
        }
        AcpCommand::ApprovalRequest => {
            approval_request(state, request_context, client, &envelope).await
        }
        AcpCommand::BindingList => binding_list(state, client, &envelope).await,
        AcpCommand::BindingUpsert => {
            binding_upsert(state, request_context, client, &envelope).await
        }
        AcpCommand::BindingGet => binding_get(state, client, &envelope).await,
        AcpCommand::BindingDetach => {
            binding_detach(state, request_context, client, &envelope).await
        }
        AcpCommand::BindingRepairPlan => binding_repair_plan(state, client).await,
        AcpCommand::BindingRepairApply => {
            binding_repair_apply(state, request_context, client).await
        }
        AcpCommand::BindingExplain => binding_explain(state, client, &envelope).await,
    }
}

async fn session_list(
    state: &AppState,
    request_context: &RequestContext,
    client: &AcpClientContext,
    envelope: &AcpCommandEnvelope,
) -> Result<Value, AcpDispatchError> {
    ensure_grant(client, AcpScope::SessionsRead, AcpCapability::SessionList)?;
    let limit = optional_usize(&envelope.params, "limit").unwrap_or(50).clamp(1, 100);
    let include_archived =
        envelope.params.get("include_archived").and_then(Value::as_bool).unwrap_or(false);
    let (sessions, next_after_session_key) = state
        .runtime
        .list_orchestrator_sessions(ListOrchestratorSessionsRequest {
            after_session_key: optional_string(&envelope.params, "after_session_key"),
            principal: request_context.principal.clone(),
            device_id: request_context.device_id.clone(),
            channel: request_context.channel.clone(),
            include_archived,
            requested_limit: Some(limit),
            search_query: optional_string(&envelope.params, "search_query"),
        })
        .await
        .map_err(AcpDispatchError::from_status)?;
    Ok(json!({
        "sessions": sessions,
        "next_after_session_key": next_after_session_key,
        "cap": { "transcript_included": false },
    }))
}

async fn session_load(
    state: &AppState,
    request_context: &RequestContext,
    client: &AcpClientContext,
    envelope: &AcpCommandEnvelope,
) -> Result<Value, AcpDispatchError> {
    ensure_grant(client, AcpScope::SessionsRead, AcpCapability::SessionLoad)?;
    let session_id = required_string(&envelope.params, "session_id")?;
    validate_canonical(&session_id, "session_id")?;
    let session = load_owned_session(state, request_context, &session_id).await?;
    let binding = optional_string(&envelope.params, "acp_session_id")
        .map(|acp_session_id| {
            state.acp_runtime.upsert_session_binding(AcpSessionBindingUpsert {
                context: client.clone(),
                acp_session_id,
                palyra_session_id: session.session_id.clone(),
                session_key: session.session_key.clone(),
                session_label: Some(session.title.clone()),
                mode: AcpSessionMode::Normal,
                config: envelope.params.get("config").cloned().unwrap_or_else(|| json!({})),
                cursor: AcpCursor::default(),
            })
        })
        .transpose()
        .map_err(AcpDispatchError::Acp)?;
    Ok(json!({ "session": session, "binding": binding }))
}

async fn session_new(
    state: &AppState,
    request_context: &RequestContext,
    client: &AcpClientContext,
    envelope: &AcpCommandEnvelope,
) -> Result<Value, AcpDispatchError> {
    ensure_grant(client, AcpScope::SessionsWrite, AcpCapability::SessionNew)?;
    let acp_session_id = required_string(&envelope.params, "acp_session_id")?;
    let session_key = optional_string(&envelope.params, "session_key")
        .unwrap_or_else(|| format!("acp:{}:{}", client.client_id, Ulid::new()));
    let session_label = optional_string(&envelope.params, "session_label");
    let outcome = state
        .runtime
        .resolve_orchestrator_session(OrchestratorSessionResolveRequest {
            session_id: None,
            session_key: Some(session_key),
            session_label,
            principal: request_context.principal.clone(),
            device_id: request_context.device_id.clone(),
            channel: request_context.channel.clone(),
            require_existing: false,
            reset_session: false,
        })
        .await
        .map_err(AcpDispatchError::from_status)?;
    let binding = state
        .acp_runtime
        .upsert_session_binding(AcpSessionBindingUpsert {
            context: client.clone(),
            acp_session_id,
            palyra_session_id: outcome.session.session_id.clone(),
            session_key: outcome.session.session_key.clone(),
            session_label: Some(outcome.session.title.clone()),
            mode: AcpSessionMode::Normal,
            config: envelope.params.get("config").cloned().unwrap_or_else(|| json!({})),
            cursor: AcpCursor::default(),
        })
        .map_err(AcpDispatchError::Acp)?;
    Ok(json!({ "session": outcome.session, "created": outcome.created, "binding": binding }))
}

async fn session_replay(
    state: &AppState,
    request_context: &RequestContext,
    client: &AcpClientContext,
    envelope: &AcpCommandEnvelope,
) -> Result<Value, AcpDispatchError> {
    ensure_grant(client, AcpScope::SessionsRead, AcpCapability::SessionReplay)?;
    let session_id = required_string(&envelope.params, "session_id")?;
    let session = load_owned_session(state, request_context, &session_id).await?;
    let cap = parse_replay_cap(&envelope.params, client)?;
    let mut records = state
        .runtime
        .list_orchestrator_session_transcript(session.session_id.clone())
        .await
        .map_err(AcpDispatchError::from_status)?;
    let total_records = records.len();
    records.sort_by(|left, right| left.seq.cmp(&right.seq));
    if records.len() > cap.max_events {
        let start = records.len().saturating_sub(cap.max_events);
        records = records.split_off(start);
    }
    let mut events = Vec::new();
    let mut replay_bytes = 0usize;
    for record in records {
        if replay_bytes >= cap.max_payload_bytes {
            break;
        }
        let event = redacted_replay_event(record, cap.include_sensitive)?;
        replay_bytes = replay_bytes.saturating_add(event.to_string().len());
        events.push(event);
    }
    Ok(json!({
        "session": session,
        "events": events,
        "cap": {
            "max_events": cap.max_events,
            "max_payload_bytes": cap.max_payload_bytes,
            "include_sensitive": cap.include_sensitive,
            "total_records": total_records,
        },
    }))
}

async fn session_reconnect(
    state: &AppState,
    _request_context: &RequestContext,
    client: &AcpClientContext,
    envelope: &AcpCommandEnvelope,
) -> Result<Value, AcpDispatchError> {
    ensure_grant(client, AcpScope::SessionsRead, AcpCapability::PendingPrompts)?;
    let acp_session_id = required_string(&envelope.params, "acp_session_id")?;
    let cursor = AcpCursor {
        sequence: envelope.params.get("cursor_sequence").and_then(Value::as_u64).unwrap_or(0),
    };
    let outcome = state
        .acp_runtime
        .reconnect(client, acp_session_id.as_str(), cursor)
        .map_err(AcpDispatchError::Acp)?;
    Ok(json!({
        "binding": outcome.binding,
        "pending_prompts": outcome.pending_prompts,
        "expired_prompt_ids": outcome.expired_prompt_ids,
    }))
}

async fn session_fork(
    state: &AppState,
    request_context: &RequestContext,
    client: &AcpClientContext,
    envelope: &AcpCommandEnvelope,
) -> Result<Value, AcpDispatchError> {
    ensure_grant(client, AcpScope::SessionsWrite, AcpCapability::SessionFork)?;
    let parent_session_id = required_string(&envelope.params, "parent_session_id")?;
    let parent = load_owned_session(state, request_context, &parent_session_id).await?;
    let fork_key = optional_string(&envelope.params, "session_key")
        .unwrap_or_else(|| format!("fork:{}:{}", parent.session_key, Ulid::new()));
    let label = optional_string(&envelope.params, "session_label")
        .or_else(|| Some(format!("Fork of {}", parent.title)));
    let outcome = state
        .runtime
        .resolve_orchestrator_session(OrchestratorSessionResolveRequest {
            session_id: None,
            session_key: Some(fork_key),
            session_label: label,
            principal: request_context.principal.clone(),
            device_id: request_context.device_id.clone(),
            channel: request_context.channel.clone(),
            require_existing: false,
            reset_session: false,
        })
        .await
        .map_err(AcpDispatchError::from_status)?;
    state
        .runtime
        .update_orchestrator_session_lineage(OrchestratorSessionLineageUpdateRequest {
            session_id: outcome.session.session_id.clone(),
            branch_state: "forked".to_owned(),
            parent_session_id: Some(parent.session_id.clone()),
            branch_origin_run_id: optional_string(&envelope.params, "branch_origin_run_id"),
            suggested_auto_title: Some(outcome.session.title.clone()),
        })
        .await
        .map_err(AcpDispatchError::from_status)?;
    let acp_session_id = optional_string(&envelope.params, "acp_session_id")
        .unwrap_or_else(|| format!("fork-{}", outcome.session.session_id));
    let binding = state
        .acp_runtime
        .upsert_session_binding(AcpSessionBindingUpsert {
            context: client.clone(),
            acp_session_id,
            palyra_session_id: outcome.session.session_id.clone(),
            session_key: outcome.session.session_key.clone(),
            session_label: Some(outcome.session.title.clone()),
            mode: AcpSessionMode::Normal,
            config: envelope.params.get("config").cloned().unwrap_or_else(|| json!({})),
            cursor: AcpCursor::default(),
        })
        .map_err(AcpDispatchError::Acp)?;
    Ok(json!({ "parent_session": parent, "session": outcome.session, "binding": binding }))
}

async fn session_compact_preview(
    state: &AppState,
    request_context: &RequestContext,
    client: &AcpClientContext,
    envelope: &AcpCommandEnvelope,
) -> Result<Value, AcpDispatchError> {
    ensure_grant(client, AcpScope::SessionsRead, AcpCapability::SessionCompact)?;
    let session_id = required_string(&envelope.params, "session_id")?;
    let session = load_owned_session(state, request_context, &session_id).await?;
    let trigger_reason = optional_string(&envelope.params, "trigger_reason")
        .unwrap_or_else(|| "acp_preview".to_owned());
    let trigger_policy = optional_string(&envelope.params, "trigger_policy");
    let plan = preview_session_compaction(
        &state.runtime,
        &session,
        Some(trigger_reason.as_str()),
        trigger_policy.as_deref(),
    )
    .await
    .map_err(AcpDispatchError::from_status)?;
    Ok(json!({ "plan": plan.to_response_json() }))
}

async fn session_compact_apply(
    state: &AppState,
    request_context: &RequestContext,
    client: &AcpClientContext,
    envelope: &AcpCommandEnvelope,
) -> Result<Value, AcpDispatchError> {
    ensure_grant(client, AcpScope::SessionsWrite, AcpCapability::SessionCompact)?;
    let session_id = required_string(&envelope.params, "session_id")?;
    let session = load_owned_session(state, request_context, &session_id).await?;
    let accept_candidate_ids = string_array(&envelope.params, "accept_candidate_ids")?;
    let reject_candidate_ids = string_array(&envelope.params, "reject_candidate_ids")?;
    let run_id = optional_string(&envelope.params, "run_id");
    let mode =
        optional_string(&envelope.params, "mode").unwrap_or_else(|| "operator_review".to_owned());
    let trigger_reason = optional_string(&envelope.params, "trigger_reason");
    let trigger_policy = optional_string(&envelope.params, "trigger_policy");
    let execution = apply_session_compaction(SessionCompactionApplyRequest {
        runtime_state: &state.runtime,
        session: &session,
        actor_principal: request_context.principal.as_str(),
        run_id: run_id.as_deref(),
        mode: mode.as_str(),
        trigger_reason: trigger_reason.as_deref(),
        trigger_policy: trigger_policy.as_deref(),
        accept_candidate_ids: &accept_candidate_ids,
        reject_candidate_ids: &reject_candidate_ids,
    })
    .await
    .map_err(AcpDispatchError::from_status)?;
    Ok(json!({
        "plan": execution.plan.to_response_json(),
        "artifact": execution.artifact,
        "checkpoint": execution.checkpoint,
        "writes": execution.writes,
    }))
}

async fn session_explain(
    state: &AppState,
    request_context: &RequestContext,
    client: &AcpClientContext,
    envelope: &AcpCommandEnvelope,
) -> Result<Value, AcpDispatchError> {
    ensure_grant(client, AcpScope::SessionsRead, AcpCapability::SessionExplain)?;
    let session_id = required_string(&envelope.params, "session_id")?;
    let session = load_owned_session(state, request_context, &session_id).await?;
    let runs = state
        .runtime
        .list_orchestrator_session_runs(session.session_id.clone())
        .await
        .map_err(AcpDispatchError::from_status)?;
    let checkpoints = state
        .runtime
        .list_orchestrator_checkpoints(session.session_id.clone())
        .await
        .map_err(AcpDispatchError::from_status)?;
    let compactions = state
        .runtime
        .list_orchestrator_compaction_artifacts(session.session_id.clone())
        .await
        .map_err(AcpDispatchError::from_status)?;
    let bindings = state
        .acp_runtime
        .list_session_bindings(Some(request_context.principal.as_str()))
        .map_err(AcpDispatchError::Acp)?
        .into_iter()
        .filter(|binding| binding.palyra_session_id == session.session_id)
        .collect::<Vec<_>>();
    Ok(json!({
        "session": session,
        "active_task": runs.first().map(|run| json!({
            "run_id": run.run_id,
            "state": run.state,
            "cancel_requested": run.cancel_requested,
        })),
        "run_count": runs.len(),
        "recent_runs": runs.into_iter().take(10).collect::<Vec<_>>(),
        "checkpoint_count": checkpoints.len(),
        "compaction_count": compactions.len(),
        "bindings": bindings,
        "policy": {
            "owner_principal": request_context.principal.clone(),
            "device_id": request_context.device_id.clone(),
            "channel": request_context.channel.clone(),
            "model_provider": "redacted",
        },
    }))
}

async fn session_config_set(
    state: &AppState,
    _request_context: &RequestContext,
    client: &AcpClientContext,
    envelope: &AcpCommandEnvelope,
) -> Result<Value, AcpDispatchError> {
    ensure_grant(client, AcpScope::SessionsWrite, AcpCapability::SessionConfig)?;
    let binding = if let Some(binding_id) = optional_string(&envelope.params, "binding_id") {
        state.acp_runtime.get_session_binding(binding_id.as_str())
    } else {
        let acp_session_id = required_string(&envelope.params, "acp_session_id")?;
        state
            .acp_runtime
            .session_binding_for_acp(client.client_id.as_str(), acp_session_id.as_str())
    }
    .map_err(AcpDispatchError::Acp)?;
    let mode = match optional_string(&envelope.params, "mode") {
        Some(raw) => AcpSessionMode::parse(raw.as_str()).ok_or_else(|| {
            AcpDispatchError::Acp(AcpRuntimeError::InvalidField {
                field: "mode",
                message: format!("unsupported ACP session mode '{raw}'"),
            })
        })?,
        None => binding.mode,
    };
    let config = envelope.params.get("config").cloned().unwrap_or(binding.config.clone());
    let updated = state
        .acp_runtime
        .upsert_session_binding(AcpSessionBindingUpsert {
            context: client.clone(),
            acp_session_id: binding.acp_session_id,
            palyra_session_id: binding.palyra_session_id,
            session_key: binding.session_key,
            session_label: binding.session_label,
            mode,
            config,
            cursor: binding.cursor,
        })
        .map_err(AcpDispatchError::Acp)?;
    Ok(json!({ "binding": updated }))
}

async fn dispatch_via_command_router(
    state: &AppState,
    request_context: &RequestContext,
    client: &AcpClientContext,
    envelope: AcpCommandEnvelope,
) -> Result<Value, AcpDispatchError> {
    let realtime_command = match envelope.command {
        AcpCommand::RunCreate => {
            ensure_grant(client, AcpScope::RunsWrite, AcpCapability::RunControl)?;
            RealtimeCommand::RunCreate
        }
        AcpCommand::RunAbort => {
            ensure_grant(client, AcpScope::RunsWrite, AcpCapability::RunControl)?;
            RealtimeCommand::RunAbort
        }
        AcpCommand::ApprovalList => {
            ensure_grant(client, AcpScope::ApprovalsRead, AcpCapability::ApprovalBridge)?;
            RealtimeCommand::ApprovalList
        }
        AcpCommand::ApprovalDecide => {
            ensure_grant(client, AcpScope::ApprovalsWrite, AcpCapability::ApprovalBridge)?;
            RealtimeCommand::ApprovalDecide
        }
        _ => unreachable!("only command-router backed ACP commands are mapped here"),
    };
    let (_, realtime_context) = negotiate_realtime_handshake(
        RealtimeHandshakeRequest {
            protocol_version: 1,
            client_id: format!("acp:{}", client.client_id),
            role: RealtimeRole::Operator,
            requested_scopes: vec![
                RealtimeScope::RunsRead,
                RealtimeScope::RunsWrite,
                RealtimeScope::ApprovalsRead,
                RealtimeScope::ApprovalsWrite,
            ],
            requested_capabilities: vec![
                RealtimeCapability::RunControl,
                RealtimeCapability::ApprovalControl,
            ],
            requested_commands: vec![realtime_command],
            event_cursor: Some(RealtimeCursor::default()),
            subscriptions: Vec::new(),
            heartbeat_interval_ms: None,
        },
        request_context.principal.clone(),
        unix_ms_now().unwrap_or(0),
    )
    .map_err(|error| AcpDispatchError::Stable(error.error))?;
    let result = dispatch_realtime_command(
        state,
        &CommandRouterContext {
            request_context: request_context.clone(),
            realtime: realtime_context,
        },
        RealtimeCommandEnvelope {
            request_id: envelope.request_id,
            command: realtime_command,
            params: envelope.params,
            idempotency_key: envelope.idempotency_key,
            expected_version: envelope.expected_version,
        },
    )
    .await;
    if result.ok {
        Ok(result.result.unwrap_or_else(|| json!({})))
    } else {
        Err(AcpDispatchError::Stable(result.error.unwrap_or_else(|| {
            StableErrorEnvelope::new(
                "acp/command_router_error",
                "command router returned an empty error",
                "retry the command or inspect daemon logs",
            )
        })))
    }
}

async fn approval_request(
    state: &AppState,
    request_context: &RequestContext,
    client: &AcpClientContext,
    envelope: &AcpCommandEnvelope,
) -> Result<Value, AcpDispatchError> {
    ensure_grant(client, AcpScope::ApprovalsWrite, AcpCapability::ApprovalBridge)?;
    let session_id = required_string(&envelope.params, "session_id")?;
    let run_id =
        optional_string(&envelope.params, "run_id").unwrap_or_else(|| Ulid::new().to_string());
    validate_canonical(&session_id, "session_id")?;
    validate_canonical(&run_id, "run_id")?;
    let subject_id = optional_string(&envelope.params, "subject_id")
        .unwrap_or_else(|| "acp_permission".to_owned());
    let summary = optional_string(&envelope.params, "summary")
        .unwrap_or_else(|| "ACP permission request".to_owned());
    let risk_level = parse_risk_level(optional_string(&envelope.params, "risk_level").as_deref());
    let approval_id =
        optional_string(&envelope.params, "approval_id").unwrap_or_else(|| Ulid::new().to_string());
    validate_canonical(&approval_id, "approval_id")?;
    let record = state
        .runtime
        .create_approval_record(ApprovalCreateRequest {
            approval_id: approval_id.clone(),
            session_id: session_id.clone(),
            run_id: run_id.clone(),
            principal: request_context.principal.clone(),
            device_id: request_context.device_id.clone(),
            channel: request_context.channel.clone(),
            subject_type: ApprovalSubjectType::Tool,
            subject_id: subject_id.clone(),
            request_summary: summary.clone(),
            policy_snapshot: ApprovalPolicySnapshot {
                policy_id: "acp_permission_bridge".to_owned(),
                policy_hash: "runtime".to_owned(),
                evaluation_summary: "ACP permission request bridged into Palyra approval audit".to_owned(),
            },
            prompt: ApprovalPromptRecord {
                title: "ACP permission request".to_owned(),
                risk_level,
                subject_id,
                summary: summary.clone(),
                options: vec![
                    ApprovalPromptOption {
                        option_id: "allow_once".to_owned(),
                        label: "Allow once".to_owned(),
                        description: "Allow this ACP action once".to_owned(),
                        default_selected: false,
                        decision_scope: ApprovalDecisionScope::Once,
                        timebox_ttl_ms: None,
                    },
                    ApprovalPromptOption {
                        option_id: "deny".to_owned(),
                        label: "Deny".to_owned(),
                        description: "Deny this ACP action".to_owned(),
                        default_selected: true,
                        decision_scope: ApprovalDecisionScope::Once,
                        timebox_ttl_ms: None,
                    },
                ],
                timeout_seconds: optional_u64(&envelope.params, "timeout_seconds")
                    .unwrap_or(300)
                    .clamp(5, 3_600) as u32,
                details_json: json!({
                    "source": "acp",
                    "client_id": client.client_id,
                    "evidence_refs": envelope.params.get("evidence_refs").cloned().unwrap_or_else(|| json!([])),
                })
                .to_string(),
                policy_explanation: "Converted from ACP permission request; no secrets persisted".to_owned(),
            },
        })
        .await
        .map_err(AcpDispatchError::from_status)?;
    if let Some(acp_session_id) = optional_string(&envelope.params, "acp_session_id") {
        let _ = state.acp_runtime.remember_pending_prompt(AcpPendingPromptUpsert {
            prompt_id: format!("approval-{approval_id}"),
            acp_client_id: client.client_id.clone(),
            acp_session_id,
            palyra_session_id: session_id,
            approval_id: Some(approval_id),
            run_id: Some(run_id),
            prompt_kind: "approval".to_owned(),
            redacted_summary: summary,
            ttl_ms: optional_i64(&envelope.params, "ttl_ms").unwrap_or(300_000),
        });
    }
    Ok(json!({ "approval": record }))
}

async fn binding_list(
    state: &AppState,
    client: &AcpClientContext,
    envelope: &AcpCommandEnvelope,
) -> Result<Value, AcpDispatchError> {
    ensure_grant(client, AcpScope::BindingsRead, AcpCapability::ConversationBindings)?;
    let conversation_bindings = state
        .acp_runtime
        .list_conversation_bindings(ConversationBindingFilter {
            owner_principal: Some(client.owner_principal.clone()),
            connector_kind: optional_string(&envelope.params, "connector_kind"),
            external_identity: optional_string(&envelope.params, "external_identity"),
            palyra_session_id: optional_string(&envelope.params, "palyra_session_id"),
            include_detached: envelope
                .params
                .get("include_detached")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            limit: optional_usize(&envelope.params, "limit"),
        })
        .map_err(AcpDispatchError::Acp)?;
    let session_bindings = state
        .acp_runtime
        .list_session_bindings(Some(client.owner_principal.as_str()))
        .map_err(AcpDispatchError::Acp)?;
    Ok(
        json!({ "conversation_bindings": conversation_bindings, "session_bindings": session_bindings }),
    )
}

async fn binding_upsert(
    state: &AppState,
    request_context: &RequestContext,
    client: &AcpClientContext,
    envelope: &AcpCommandEnvelope,
) -> Result<Value, AcpDispatchError> {
    ensure_grant(client, AcpScope::BindingsWrite, AcpCapability::ConversationBindings)?;
    let binding = state
        .acp_runtime
        .upsert_conversation_binding(conversation_upsert_from_command(
            request_context,
            &envelope.params,
        )?)
        .map_err(AcpDispatchError::Acp)?;
    let _ = state
        .runtime
        .record_console_event(
            request_context,
            "acp.binding.upsert",
            json!({
                "binding_id": binding.binding_id,
                "connector_kind": binding.connector_kind,
                "palyra_session_id": binding.palyra_session_id,
                "conflict_state": binding.conflict_state.as_str(),
            }),
        )
        .await;
    Ok(json!({ "binding": binding }))
}

async fn binding_get(
    state: &AppState,
    client: &AcpClientContext,
    envelope: &AcpCommandEnvelope,
) -> Result<Value, AcpDispatchError> {
    ensure_grant(client, AcpScope::BindingsRead, AcpCapability::ConversationBindings)?;
    let binding_id = required_string(&envelope.params, "binding_id")?;
    let snapshot =
        state.acp_runtime.explain_binding(binding_id.as_str()).map_err(AcpDispatchError::Acp)?;
    ensure_binding_owner(client, snapshot.owner_principal.as_str())?;
    Ok(json!({ "binding": snapshot }))
}

async fn binding_detach(
    state: &AppState,
    request_context: &RequestContext,
    client: &AcpClientContext,
    envelope: &AcpCommandEnvelope,
) -> Result<Value, AcpDispatchError> {
    ensure_grant(client, AcpScope::BindingsWrite, AcpCapability::ConversationBindings)?;
    let binding_id = required_string(&envelope.params, "binding_id")?;
    let snapshot =
        state.acp_runtime.explain_binding(binding_id.as_str()).map_err(AcpDispatchError::Acp)?;
    ensure_binding_owner(client, snapshot.owner_principal.as_str())?;
    let detached = state
        .acp_runtime
        .detach_conversation_binding(binding_id.as_str())
        .map_err(AcpDispatchError::Acp)?;
    let _ = state
        .runtime
        .record_console_event(
            request_context,
            "acp.binding.detach",
            json!({ "binding_id": binding_id, "palyra_session_id": detached.palyra_session_id }),
        )
        .await;
    Ok(json!({ "binding": detached }))
}

async fn binding_repair_plan(
    state: &AppState,
    client: &AcpClientContext,
) -> Result<Value, AcpDispatchError> {
    ensure_grant(client, AcpScope::BindingsRead, AcpCapability::BindingRepair)?;
    let plan =
        state.acp_runtime.plan_conversation_binding_repair().map_err(AcpDispatchError::Acp)?;
    Ok(json!({ "plan": plan }))
}

async fn binding_repair_apply(
    state: &AppState,
    request_context: &RequestContext,
    client: &AcpClientContext,
) -> Result<Value, AcpDispatchError> {
    ensure_grant(client, AcpScope::BindingsWrite, AcpCapability::BindingRepair)?;
    let plan =
        state.acp_runtime.apply_conversation_binding_repair().map_err(AcpDispatchError::Acp)?;
    let _ = state
        .runtime
        .record_console_event(
            request_context,
            "acp.binding.repair.apply",
            json!({ "action_count": plan.actions.len() }),
        )
        .await;
    Ok(json!({ "plan": plan, "applied": true }))
}

async fn binding_explain(
    state: &AppState,
    client: &AcpClientContext,
    envelope: &AcpCommandEnvelope,
) -> Result<Value, AcpDispatchError> {
    ensure_grant(client, AcpScope::BindingsRead, AcpCapability::ConversationBindings)?;
    let binding_id = required_string(&envelope.params, "binding_id")?;
    let snapshot =
        state.acp_runtime.explain_binding(binding_id.as_str()).map_err(AcpDispatchError::Acp)?;
    ensure_binding_owner(client, snapshot.owner_principal.as_str())?;
    Ok(json!({ "explain": snapshot }))
}

fn ensure_client_matches_session(
    client: &AcpClientContext,
    request_context: &RequestContext,
) -> Result<(), AcpDispatchError> {
    if client.owner_principal != request_context.principal {
        return Err(AcpDispatchError::Acp(AcpRuntimeError::Permission {
            message: "ACP owner_principal does not match authenticated console principal"
                .to_owned(),
        }));
    }
    if client.device_id != request_context.device_id {
        return Err(AcpDispatchError::Acp(AcpRuntimeError::Permission {
            message: "ACP device_id does not match authenticated console device".to_owned(),
        }));
    }
    if client.channel != request_context.channel {
        return Err(AcpDispatchError::Acp(AcpRuntimeError::Permission {
            message: "ACP channel does not match authenticated console channel".to_owned(),
        }));
    }
    Ok(())
}

fn ensure_grant(
    client: &AcpClientContext,
    scope: AcpScope,
    capability: AcpCapability,
) -> Result<(), AcpDispatchError> {
    let scopes = client.scopes.iter().copied().collect::<BTreeSet<_>>();
    let capabilities = client.capabilities.iter().copied().collect::<BTreeSet<_>>();
    if !scopes.contains(&scope) {
        return Err(AcpDispatchError::Acp(AcpRuntimeError::Permission {
            message: format!("missing ACP scope {}", scope.as_str()),
        }));
    }
    if !capabilities.contains(&capability) {
        return Err(AcpDispatchError::Acp(AcpRuntimeError::Permission {
            message: format!("missing ACP capability {}", capability.as_str()),
        }));
    }
    Ok(())
}

fn ensure_binding_owner(
    client: &AcpClientContext,
    owner_principal: &str,
) -> Result<(), AcpDispatchError> {
    if client.owner_principal == owner_principal || client.owner_principal.starts_with("admin:") {
        return Ok(());
    }
    Err(AcpDispatchError::Acp(AcpRuntimeError::Permission {
        message: "binding belongs to a different principal".to_owned(),
    }))
}

async fn load_owned_session(
    state: &AppState,
    request_context: &RequestContext,
    session_id: &str,
) -> Result<OrchestratorSessionRecord, AcpDispatchError> {
    validate_canonical(session_id, "session_id")?;
    let session = state
        .runtime
        .orchestrator_session_by_id(session_id.to_owned())
        .await
        .map_err(AcpDispatchError::from_status)?
        .ok_or_else(|| {
            AcpDispatchError::Acp(AcpRuntimeError::NotFound {
                kind: "session",
                id: session_id.to_owned(),
            })
        })?;
    if session.principal != request_context.principal
        || session.device_id != request_context.device_id
        || session.channel != request_context.channel
    {
        return Err(AcpDispatchError::Acp(AcpRuntimeError::Permission {
            message: "session belongs to a different owner scope".to_owned(),
        }));
    }
    Ok(session)
}

fn parse_replay_cap(
    params: &Value,
    client: &AcpClientContext,
) -> Result<AcpReplayCap, AcpDispatchError> {
    let include_sensitive =
        params.get("include_sensitive").and_then(Value::as_bool).unwrap_or(false);
    if include_sensitive {
        ensure_grant(client, AcpScope::EventsSensitive, AcpCapability::SensitiveReplay)?;
    }
    Ok(AcpReplayCap {
        max_events: optional_usize(params, "max_events")
            .unwrap_or(ACP_DEFAULT_REPLAY_MAX_EVENTS)
            .clamp(1, 500),
        max_payload_bytes: optional_usize(params, "max_payload_bytes")
            .unwrap_or(64 * 1024)
            .clamp(1_024, 512 * 1024),
        include_sensitive,
    })
}

fn redacted_replay_event(
    record: crate::journal::OrchestratorSessionTranscriptRecord,
    include_sensitive: bool,
) -> Result<Value, AcpDispatchError> {
    let acp_event_type = crate::acp::translate_palyra_event_type(record.event_type.as_str())
        .map_err(AcpDispatchError::Acp)?;
    let payload = serde_json::from_str::<Value>(record.payload_json.as_str()).unwrap_or_else(
        |_| json!({ "preview": record.payload_json.chars().take(256).collect::<String>() }),
    );
    let redacted_payload = if include_sensitive { payload } else { redact_replay_payload(payload) };
    Ok(json!({
        "ref": {
            "session_id": record.session_id,
            "run_id": record.run_id,
            "seq": record.seq,
        },
        "event_type": acp_event_type,
        "palyra_event_type": record.event_type,
        "created_at_unix_ms": record.created_at_unix_ms,
        "origin_kind": record.origin_kind,
        "origin_run_id": record.origin_run_id,
        "payload": redacted_payload,
    }))
}

fn redact_replay_payload(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut output = Map::new();
            for (key, child) in map {
                if redaction_key_is_sensitive(key.as_str()) {
                    output.insert(key, json!({ "redacted": true, "ref": "sensitive-field" }));
                } else if key.contains("raw") || key.contains("payload") {
                    output
                        .insert(key, json!({ "redacted": true, "preview": safe_preview(&child) }));
                } else {
                    output.insert(key, redact_replay_payload(child));
                }
            }
            Value::Object(output)
        }
        Value::Array(items) => Value::Array(items.into_iter().map(redact_replay_payload).collect()),
        Value::String(text) if text.len() > 512 => {
            json!({ "preview": text.chars().take(256).collect::<String>(), "truncated": true })
        }
        other => other,
    }
}

fn safe_preview(value: &Value) -> String {
    let raw = value.to_string();
    raw.chars().take(256).collect()
}

fn required_string(params: &Value, field: &'static str) -> Result<String, AcpDispatchError> {
    optional_string(params, field).ok_or_else(|| {
        AcpDispatchError::Acp(AcpRuntimeError::InvalidField {
            field,
            message: "field is required".to_owned(),
        })
    })
}

fn optional_string(params: &Value, field: &'static str) -> Option<String> {
    params
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn optional_usize(params: &Value, field: &'static str) -> Option<usize> {
    params.get(field).and_then(Value::as_u64).and_then(|value| usize::try_from(value).ok())
}

fn optional_u64(params: &Value, field: &'static str) -> Option<u64> {
    params.get(field).and_then(Value::as_u64)
}

fn optional_i64(params: &Value, field: &'static str) -> Option<i64> {
    params.get(field).and_then(Value::as_i64)
}

fn string_array(params: &Value, field: &'static str) -> Result<Vec<String>, AcpDispatchError> {
    match params.get(field) {
        None => Ok(Vec::new()),
        Some(Value::Array(items)) => items
            .iter()
            .map(|item| {
                item.as_str().map(|value| value.trim().to_owned()).ok_or_else(|| {
                    AcpDispatchError::Acp(AcpRuntimeError::InvalidField {
                        field,
                        message: "expected string array".to_owned(),
                    })
                })
            })
            .collect(),
        Some(_) => Err(AcpDispatchError::Acp(AcpRuntimeError::InvalidField {
            field,
            message: "expected string array".to_owned(),
        })),
    }
}

fn validate_canonical(value: &str, field: &'static str) -> Result<(), AcpDispatchError> {
    validate_canonical_id(value).map_err(|_| {
        AcpDispatchError::Acp(AcpRuntimeError::InvalidField {
            field,
            message: "expected canonical Palyra ULID".to_owned(),
        })
    })
}

fn parse_risk_level(raw: Option<&str>) -> ApprovalRiskLevel {
    match raw.unwrap_or("medium") {
        "low" => ApprovalRiskLevel::Low,
        "high" => ApprovalRiskLevel::High,
        "critical" => ApprovalRiskLevel::Critical,
        _ => ApprovalRiskLevel::Medium,
    }
}

fn conversation_upsert_from_command(
    request_context: &RequestContext,
    params: &Value,
) -> Result<ConversationBindingUpsert, AcpDispatchError> {
    Ok(ConversationBindingUpsert {
        connector_kind: required_string(params, "connector_kind")?,
        external_identity: required_string(params, "external_identity")?,
        external_conversation_id: required_string(params, "external_conversation_id")?,
        palyra_session_id: required_string(params, "palyra_session_id")?,
        owner_principal: request_context.principal.clone(),
        device_id: optional_string(params, "device_id")
            .unwrap_or_else(|| request_context.device_id.clone()),
        channel: optional_string(params, "channel").or_else(|| request_context.channel.clone()),
        scopes: string_array(params, "scopes").map(|scopes| {
            if scopes.is_empty() {
                vec!["sessions:read".to_owned()]
            } else {
                scopes
            }
        })?,
        sensitivity: parse_binding_sensitivity(optional_string(params, "sensitivity").as_deref()),
        delivery_cursor: AcpCursor {
            sequence: params.get("cursor_sequence").and_then(Value::as_u64).unwrap_or(0),
        },
        last_event_id: optional_string(params, "last_event_id"),
    })
}

fn parse_binding_sensitivity(raw: Option<&str>) -> ConversationBindingSensitivity {
    match raw.unwrap_or("internal") {
        "public" => ConversationBindingSensitivity::Public,
        "sensitive" => ConversationBindingSensitivity::Sensitive,
        _ => ConversationBindingSensitivity::Internal,
    }
}

pub(super) fn acp_runtime_response(error: AcpRuntimeError) -> Response {
    let message = error.to_string();
    let status = match error {
        AcpRuntimeError::NotFound { .. } => tonic::Status::not_found(message.clone()),
        AcpRuntimeError::Permission { .. } => tonic::Status::permission_denied(message.clone()),
        AcpRuntimeError::InvalidField { .. }
        | AcpRuntimeError::UnsupportedProtocolVersion { .. }
        | AcpRuntimeError::Compatibility { .. } => tonic::Status::invalid_argument(message.clone()),
        AcpRuntimeError::RateLimited { .. } => tonic::Status::resource_exhausted(message.clone()),
        AcpRuntimeError::Conflict { .. } => tonic::Status::failed_precondition(message.clone()),
        AcpRuntimeError::Io { .. }
        | AcpRuntimeError::Json { .. }
        | AcpRuntimeError::VersionedJson { .. }
        | AcpRuntimeError::PermissionHarden { .. }
        | AcpRuntimeError::StateInvariant { .. } => tonic::Status::internal(message),
    };
    runtime_status_response(status)
}

#[derive(Debug)]
enum AcpDispatchError {
    Acp(AcpRuntimeError),
    Stable(StableErrorEnvelope),
}

impl From<AcpRuntimeError> for AcpDispatchError {
    fn from(error: AcpRuntimeError) -> Self {
        Self::Acp(error)
    }
}

impl AcpDispatchError {
    fn from_status(status: tonic::Status) -> Self {
        Self::Stable(StableErrorEnvelope::new(
            "acp/runtime_error",
            status.message().to_owned(),
            "inspect the target session/run and retry",
        ))
    }

    fn to_stable_error(&self) -> StableErrorEnvelope {
        match self {
            Self::Acp(error) => error.to_stable_error(),
            Self::Stable(error) => error.clone(),
        }
    }
}
