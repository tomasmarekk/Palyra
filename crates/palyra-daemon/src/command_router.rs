use std::sync::Arc;

use palyra_common::{
    daemon_config_schema::{is_secret_config_path, SECRET_CONFIG_PATHS},
    runtime_contracts::{
        IdempotencyRecordSnapshot, IdempotencyReplayDecision, RealtimeCommand,
        RealtimeCommandEnvelope, RealtimeCommandResultEnvelope, RealtimeEventEnvelope,
        RealtimeEventSensitivity, RealtimeEventTopic, RealtimeNodePresence,
        RuntimeConfigSchemaField, StableErrorEnvelope, ToolResultSensitivity,
        REALTIME_DEFAULT_HEARTBEAT_INTERVAL_MS,
    },
    validate_canonical_id,
};
use serde_json::{json, Value};
use tonic::Status;
use ulid::Ulid;

use crate::{
    app::state::AppState,
    gateway::{self, RequestContext},
    journal::{
        self, ApprovalDecision, ApprovalDecisionScope, ApprovalResolveRequest,
        IdempotencyBeginRequest, IdempotencyCompleteRequest, IdempotencyFailRequest,
        OrchestratorCancelRequest, OrchestratorRunStartRequest,
    },
    realtime::{authorize_realtime_command, descriptor_for_command, RealtimeConnectionContext},
};

#[derive(Debug, Clone)]
pub(crate) struct CommandRouterContext {
    pub(crate) request_context: RequestContext,
    pub(crate) realtime: RealtimeConnectionContext,
}

#[derive(Debug, Clone)]
struct CommandIdempotencyOutcome {
    replayed: bool,
    result: Option<Value>,
}

pub(crate) async fn dispatch_realtime_command(
    state: &AppState,
    context: &CommandRouterContext,
    envelope: RealtimeCommandEnvelope,
) -> RealtimeCommandResultEnvelope {
    let command = envelope.command;
    let authorization =
        authorize_realtime_command(&context.realtime, command, envelope.idempotency_key.as_deref());
    if let Err(error) = authorization {
        return command_error(envelope, error, false);
    }
    let now = crate::unix_ms_now().unwrap_or(0);
    let rate_limit = {
        let mut limiter =
            state.realtime_rate_limit.lock().unwrap_or_else(|error| error.into_inner());
        limiter.check(&context.realtime, command, now)
    };
    if let Err(error) = rate_limit {
        return command_error(envelope, error, false);
    }

    let idempotency = match begin_command_idempotency(state, &envelope).await {
        Ok(outcome) => outcome,
        Err(error) => return command_error(envelope, error, false),
    };
    if idempotency.replayed {
        let fallback = json!({
            "replayed": true,
            "command": command.as_str(),
            "idempotency_key": envelope.idempotency_key.clone(),
        });
        let result = idempotency.result.unwrap_or(fallback);
        return command_ok(envelope, result, true);
    }

    let result = execute_command(state, context, &envelope).await;
    match result {
        Ok(value) => {
            if let Err(error) = complete_command_idempotency(state, &envelope, &value).await {
                return command_error(envelope, error, false);
            }
            publish_command_event(state, context, command, &value);
            let _ = state
                .runtime
                .record_console_event(
                    &context.request_context,
                    "realtime.command.executed",
                    json!({
                        "command": command.as_str(),
                        "request_id": envelope.request_id,
                        "idempotency_key_present": envelope.idempotency_key.is_some(),
                    }),
                )
                .await;
            command_ok(envelope, value, false)
        }
        Err(error) => {
            let _ = fail_command_idempotency(state, &envelope, &error).await;
            command_error(envelope, error, false)
        }
    }
}

async fn execute_command(
    state: &AppState,
    context: &CommandRouterContext,
    envelope: &RealtimeCommandEnvelope,
) -> Result<Value, StableErrorEnvelope> {
    match envelope.command {
        RealtimeCommand::RunCreate => run_create(state, context, envelope).await,
        RealtimeCommand::RunWait => run_wait(state, context, envelope).await,
        RealtimeCommand::RunEvents => run_events(state, context, envelope).await,
        RealtimeCommand::RunAbort => run_abort(state, context, envelope).await,
        RealtimeCommand::RunGet => run_get(state, context, envelope).await,
        RealtimeCommand::ApprovalList => approval_list(state, context, envelope).await,
        RealtimeCommand::ApprovalGet => approval_get(state, context, envelope).await,
        RealtimeCommand::ApprovalDecide => approval_decide(state, context, envelope).await,
        RealtimeCommand::NodePresence => node_presence(state).await,
        RealtimeCommand::NodeCapabilityGrant => {
            node_capability_grant(state, context, envelope).await
        }
        RealtimeCommand::NodeCapabilityRevoke => {
            node_capability_revoke(state, context, envelope).await
        }
        RealtimeCommand::ConfigSchemaLookup => config_schema_lookup(envelope),
        RealtimeCommand::ConfigReloadPlan => config_reload_plan(state, context, envelope).await,
        RealtimeCommand::ConfigReloadApply => config_reload_apply(state, context, envelope).await,
    }
}

async fn run_create(
    state: &AppState,
    context: &CommandRouterContext,
    envelope: &RealtimeCommandEnvelope,
) -> Result<Value, StableErrorEnvelope> {
    let session_id = required_str(&envelope.params, "session_id")?;
    validate_canonical_id(session_id).map_err(|_| {
        stable_error(
            "command/invalid_session_id",
            "session_id must be a canonical ULID",
            "use a valid session_id",
        )
    })?;
    let run_id = optional_str(&envelope.params, "run_id")
        .map(str::to_owned)
        .unwrap_or_else(|| Ulid::new().to_string());
    validate_canonical_id(run_id.as_str()).map_err(|_| {
        stable_error(
            "command/invalid_run_id",
            "run_id must be a canonical ULID",
            "use a valid run_id or omit it",
        )
    })?;
    state
        .runtime
        .start_orchestrator_run(OrchestratorRunStartRequest {
            run_id: run_id.clone(),
            session_id: session_id.to_owned(),
            origin_kind: "realtime".to_owned(),
            origin_run_id: optional_str(&envelope.params, "origin_run_id").map(str::to_owned),
            triggered_by_principal: Some(context.request_context.principal.clone()),
            parameter_delta_json: envelope.params.get("parameter_delta").map(Value::to_string),
        })
        .await
        .map_err(stable_error_from_status)?;
    let snapshot = load_owned_run(state, &context.request_context, run_id.as_str()).await?;
    Ok(json!({ "run": snapshot }))
}

async fn run_wait(
    state: &AppState,
    context: &CommandRouterContext,
    envelope: &RealtimeCommandEnvelope,
) -> Result<Value, StableErrorEnvelope> {
    let run_id = required_run_id(&envelope.params)?;
    let timeout_ms =
        optional_u64(&envelope.params, "timeout_ms").unwrap_or(30_000).clamp(25, 120_000);
    let return_on_waiting =
        envelope.params.get("return_on_waiting").and_then(Value::as_bool).unwrap_or(false);
    let outcome = state
        .runtime
        .wait_for_orchestrator_run(gateway::OrchestratorRunWaitRequest {
            run_id: run_id.to_owned(),
            timeout: std::time::Duration::from_millis(timeout_ms),
            poll_interval: std::time::Duration::from_millis(250),
            return_on_waiting,
        })
        .await
        .map_err(stable_error_from_status)?;
    ensure_run_owned(&outcome.snapshot, &context.request_context)?;
    Ok(json!({
        "run": outcome.snapshot,
        "canonical_state": outcome.canonical_state.as_str(),
    }))
}

async fn run_events(
    state: &AppState,
    context: &CommandRouterContext,
    envelope: &RealtimeCommandEnvelope,
) -> Result<Value, StableErrorEnvelope> {
    let run_id = required_run_id(&envelope.params)?;
    let run = load_owned_run(state, &context.request_context, run_id).await?;
    let after_seq = envelope.params.get("after_seq").and_then(Value::as_i64);
    let limit = optional_usize(&envelope.params, "limit");
    let tape = state
        .runtime
        .orchestrator_tape_snapshot(run_id.to_owned(), after_seq, limit)
        .await
        .map_err(stable_error_from_status)?;
    Ok(json!({ "run": run, "tape": tape }))
}

async fn run_abort(
    state: &AppState,
    context: &CommandRouterContext,
    envelope: &RealtimeCommandEnvelope,
) -> Result<Value, StableErrorEnvelope> {
    let run_id = required_run_id(&envelope.params)?;
    let run = load_owned_run(state, &context.request_context, run_id).await?;
    let reason = optional_str(&envelope.params, "reason")
        .map(str::to_owned)
        .unwrap_or_else(|| "realtime_run_abort".to_owned());
    let cancel = state
        .runtime
        .request_orchestrator_cancel(OrchestratorCancelRequest {
            run_id: run.run_id.clone(),
            reason,
        })
        .await
        .map_err(stable_error_from_status)?;
    Ok(json!({ "cancel": cancel }))
}

async fn run_get(
    state: &AppState,
    context: &CommandRouterContext,
    envelope: &RealtimeCommandEnvelope,
) -> Result<Value, StableErrorEnvelope> {
    let run_id = required_run_id(&envelope.params)?;
    let run = load_owned_run(state, &context.request_context, run_id).await?;
    Ok(json!({ "run": run }))
}

async fn approval_list(
    state: &AppState,
    context: &CommandRouterContext,
    envelope: &RealtimeCommandEnvelope,
) -> Result<Value, StableErrorEnvelope> {
    let principal_filter = if context.request_context.principal.starts_with("admin:") {
        optional_str(&envelope.params, "principal").map(str::to_owned)
    } else {
        Some(context.request_context.principal.clone())
    };
    let (approvals, next_after_approval_id) = state
        .runtime
        .list_approval_records(
            optional_str(&envelope.params, "after_approval_id").map(str::to_owned),
            optional_usize(&envelope.params, "limit"),
            envelope.params.get("since_unix_ms").and_then(Value::as_i64),
            envelope.params.get("until_unix_ms").and_then(Value::as_i64),
            optional_str(&envelope.params, "subject_id").map(str::to_owned),
            principal_filter,
            None,
            None,
        )
        .await
        .map_err(stable_error_from_status)?;
    Ok(json!({ "approvals": approvals, "next_after_approval_id": next_after_approval_id }))
}

async fn approval_get(
    state: &AppState,
    context: &CommandRouterContext,
    envelope: &RealtimeCommandEnvelope,
) -> Result<Value, StableErrorEnvelope> {
    let approval_id = required_str(&envelope.params, "approval_id")?;
    let record = load_owned_approval(state, &context.request_context, approval_id).await?;
    Ok(json!({ "approval": record }))
}

async fn approval_decide(
    state: &AppState,
    context: &CommandRouterContext,
    envelope: &RealtimeCommandEnvelope,
) -> Result<Value, StableErrorEnvelope> {
    let approval_id = required_str(&envelope.params, "approval_id")?;
    let existing = load_owned_approval(state, &context.request_context, approval_id).await?;
    if existing.decision.is_some() {
        return Err(stable_error(
            "approval/already_resolved",
            "approval has already reached a terminal decision",
            "refresh approval state before retrying",
        ));
    }
    if let Some(expected_version) = envelope.expected_version {
        ensure_expected_approval_version(existing.updated_at_unix_ms, expected_version)?;
    }
    let approved = envelope.params.get("approved").and_then(Value::as_bool).unwrap_or(false);
    if approved {
        let run = load_owned_run(state, &context.request_context, existing.run_id.as_str()).await?;
        ensure_approval_not_racing_abort(approved, run.cancel_requested)?;
    }
    let decision_scope = parse_decision_scope(optional_str(&envelope.params, "decision_scope"))?;
    let reason = optional_str(&envelope.params, "reason").map(str::to_owned).unwrap_or_else(|| {
        if approved { "approved_by_realtime" } else { "denied_by_realtime" }.to_owned()
    });
    let resolved = state
        .runtime
        .resolve_approval_record(ApprovalResolveRequest {
            approval_id: approval_id.to_owned(),
            decision: if approved { ApprovalDecision::Allow } else { ApprovalDecision::Deny },
            decision_scope,
            decision_reason: reason,
            decision_scope_ttl_ms: envelope
                .params
                .get("decision_scope_ttl_ms")
                .and_then(Value::as_i64),
        })
        .await
        .map_err(stable_error_from_status)?;
    Ok(json!({ "approval": resolved }))
}

async fn node_presence(state: &AppState) -> Result<Value, StableErrorEnvelope> {
    let now = crate::unix_ms_now().unwrap_or(0);
    let ttl_ms = REALTIME_DEFAULT_HEARTBEAT_INTERVAL_MS.saturating_mul(4);
    let nodes = state
        .node_runtime
        .nodes()
        .map_err(stable_error_from_status)?
        .into_iter()
        .map(|node| {
            let age = now.saturating_sub(node.last_seen_at_unix_ms);
            RealtimeNodePresence {
                device_id: node.device_id,
                state: if age > i64::try_from(ttl_ms).unwrap_or(i64::MAX) {
                    "stale".to_owned()
                } else {
                    "online".to_owned()
                },
                ttl_ms,
                last_seen_at_unix_ms: node.last_seen_at_unix_ms,
                heartbeat_interval_ms: REALTIME_DEFAULT_HEARTBEAT_INTERVAL_MS,
                capabilities: node
                    .capabilities
                    .into_iter()
                    .filter(|capability| capability.available)
                    .map(|capability| capability.name)
                    .collect(),
                attestation: vec!["node_rpc.mtls".to_owned()],
            }
        })
        .collect::<Vec<_>>();
    Ok(json!({ "nodes": nodes, "ttl_ms": ttl_ms }))
}

async fn node_capability_grant(
    state: &AppState,
    context: &CommandRouterContext,
    envelope: &RealtimeCommandEnvelope,
) -> Result<Value, StableErrorEnvelope> {
    let device_id = required_str(&envelope.params, "device_id")?;
    let capability = required_str(&envelope.params, "capability")?;
    let node =
        state.node_runtime.node(device_id).map_err(stable_error_from_status)?.ok_or_else(|| {
            stable_error("node/not_found", "node was not found", "refresh node presence")
        })?;
    ensure_node_not_stale(&node)?;
    let node = state
        .node_runtime
        .set_node_capability_availability(device_id, capability, true)
        .map_err(stable_error_from_status)?;
    let grant_id = Ulid::new().to_string();
    state
        .runtime
        .record_console_event(
            &context.request_context,
            "realtime.node.capability_granted",
            json!({
                "grant_id": grant_id,
                "device_id": device_id,
                "capability": capability,
                "idempotency_key_present": envelope.idempotency_key.is_some(),
            }),
        )
        .await
        .map_err(stable_error_from_status)?;
    Ok(
        json!({ "grant_id": grant_id, "device_id": device_id, "capability": capability, "granted": true, "node": node }),
    )
}

async fn node_capability_revoke(
    state: &AppState,
    context: &CommandRouterContext,
    envelope: &RealtimeCommandEnvelope,
) -> Result<Value, StableErrorEnvelope> {
    let device_id = required_str(&envelope.params, "device_id")?;
    let capability = required_str(&envelope.params, "capability")?;
    let node = state
        .node_runtime
        .set_node_capability_availability(device_id, capability, false)
        .map_err(stable_error_from_status)?;
    state
        .runtime
        .record_console_event(
            &context.request_context,
            "realtime.node.capability_revoked",
            json!({
                "device_id": device_id,
                "capability": capability,
                "idempotency_key_present": envelope.idempotency_key.is_some(),
            }),
        )
        .await
        .map_err(stable_error_from_status)?;
    Ok(json!({ "device_id": device_id, "capability": capability, "revoked": true, "node": node }))
}

fn config_schema_lookup(envelope: &RealtimeCommandEnvelope) -> Result<Value, StableErrorEnvelope> {
    let path_filter = optional_str(&envelope.params, "path");
    let fields = runtime_config_schema_fields()
        .into_iter()
        .filter(|field| {
            path_filter.is_none_or(|path| field.path == path || field.path.starts_with(path))
        })
        .collect::<Vec<_>>();
    if fields.is_empty() {
        return Err(stable_error(
            "config/schema_not_found",
            "no runtime config schema field matched the requested path",
            "request a known config path or omit path to list supported fields",
        ));
    }
    Ok(json!({ "fields": fields }))
}

async fn config_reload_plan(
    state: &AppState,
    context: &CommandRouterContext,
    envelope: &RealtimeCommandEnvelope,
) -> Result<Value, StableErrorEnvelope> {
    let request = palyra_control_plane::ConfigReloadPlanRequest {
        path: optional_str(&envelope.params, "path").map(str::to_owned),
    };
    let plan = crate::transport::http::handlers::console::config::plan_config_reload_for_context(
        state,
        &context.request_context,
        request,
    )
    .await
    .map_err(stable_error_from_response)?;
    Ok(json!({ "plan": plan }))
}

async fn config_reload_apply(
    state: &AppState,
    context: &CommandRouterContext,
    envelope: &RealtimeCommandEnvelope,
) -> Result<Value, StableErrorEnvelope> {
    let request = palyra_control_plane::ConfigReloadApplyRequest {
        path: optional_str(&envelope.params, "path").map(str::to_owned),
        plan_id: optional_str(&envelope.params, "plan_id").map(str::to_owned),
        idempotency_key: optional_str(&envelope.params, "idempotency_key").map(str::to_owned),
        dry_run: envelope.params.get("dry_run").and_then(Value::as_bool).unwrap_or(false),
        force: envelope.params.get("force").and_then(Value::as_bool).unwrap_or(false),
    };
    let apply = crate::transport::http::handlers::console::config::apply_config_reload_for_context(
        state,
        &context.request_context,
        request,
    )
    .await
    .map_err(stable_error_from_response)?;
    Ok(json!({ "reload": apply }))
}

async fn load_owned_run(
    state: &AppState,
    context: &RequestContext,
    run_id: &str,
) -> Result<journal::OrchestratorRunStatusSnapshot, StableErrorEnvelope> {
    validate_canonical_id(run_id).map_err(|_| {
        stable_error(
            "command/invalid_run_id",
            "run_id must be a canonical ULID",
            "use a valid run_id",
        )
    })?;
    let run = state
        .runtime
        .orchestrator_run_status_snapshot(run_id.to_owned())
        .await
        .map_err(stable_error_from_status)?
        .ok_or_else(|| {
            stable_error("run/not_found", "orchestrator run was not found", "refresh run state")
        })?;
    ensure_run_owned(&run, context)?;
    Ok(run)
}

fn ensure_run_owned(
    run: &journal::OrchestratorRunStatusSnapshot,
    context: &RequestContext,
) -> Result<(), StableErrorEnvelope> {
    if run.principal != context.principal || run.device_id != context.device_id {
        return Err(stable_error(
            "run/permission_denied",
            "run does not belong to the authenticated realtime context",
            "use a run owned by the authenticated principal and device",
        ));
    }
    match (&run.channel, &context.channel) {
        (Some(left), Some(right)) if left == right => Ok(()),
        (None, None) => Ok(()),
        _ => Err(stable_error(
            "run/permission_denied",
            "run channel does not match the authenticated realtime context",
            "use a run from the same channel context",
        )),
    }
}

async fn load_owned_approval(
    state: &AppState,
    context: &RequestContext,
    approval_id: &str,
) -> Result<journal::ApprovalRecord, StableErrorEnvelope> {
    validate_canonical_id(approval_id).map_err(|_| {
        stable_error(
            "approval/invalid_id",
            "approval_id must be a canonical ULID",
            "use a valid approval_id",
        )
    })?;
    let approval = state
        .runtime
        .approval_record(approval_id.to_owned())
        .await
        .map_err(stable_error_from_status)?
        .ok_or_else(|| {
            stable_error("approval/not_found", "approval was not found", "refresh approval state")
        })?;
    if !context.principal.starts_with("admin:") && approval.principal != context.principal {
        return Err(stable_error(
            "approval/permission_denied",
            "approval does not belong to the authenticated realtime context",
            "use an approval visible to the authenticated principal",
        ));
    }
    Ok(approval)
}

fn ensure_node_not_stale(
    node: &crate::node_runtime::RegisteredNodeRecord,
) -> Result<(), StableErrorEnvelope> {
    let now = crate::unix_ms_now().unwrap_or(0);
    let ttl_ms =
        i64::try_from(REALTIME_DEFAULT_HEARTBEAT_INTERVAL_MS.saturating_mul(4)).unwrap_or(i64::MAX);
    if now.saturating_sub(node.last_seen_at_unix_ms) > ttl_ms {
        return Err(stable_error(
            "node/stale",
            "stale node cannot receive new capability work",
            "wait for the node heartbeat before granting or invoking capabilities",
        ));
    }
    Ok(())
}

fn runtime_config_schema_fields() -> Vec<RuntimeConfigSchemaField> {
    let mut fields = vec![
        schema_field(
            "memory.max_item_bytes",
            "integer",
            "16384",
            "1..=1048576",
            false,
            true,
            "hot_safe",
        ),
        schema_field(
            "memory.auto_inject.enabled",
            "boolean",
            "false",
            "boolean",
            false,
            true,
            "hot_safe",
        ),
        schema_field(
            "model_provider.openai_api_key",
            "string",
            "",
            "secret or vault ref preferred",
            true,
            false,
            "restart_required",
        ),
        schema_field(
            "model_provider.openai_api_key_secret_ref",
            "string",
            "",
            "vault secret ref",
            true,
            false,
            "restart_required",
        ),
        schema_field(
            "tool_call.allowed_tools",
            "array",
            "[]",
            "known tool names",
            false,
            false,
            "manual_review",
        ),
        schema_field(
            "tool_call.browser_service.auth_token",
            "string",
            "",
            "secret token",
            true,
            false,
            "restart_required",
        ),
        schema_field(
            "gateway.bind_profile",
            "string",
            "loopback",
            "loopback|public_tls",
            false,
            false,
            "restart_required",
        ),
        schema_field(
            "gateway.tls.enabled",
            "boolean",
            "false",
            "boolean",
            false,
            false,
            "restart_required",
        ),
        schema_field(
            "admin.require_auth",
            "boolean",
            "true",
            "boolean",
            false,
            false,
            "restart_required",
        ),
        schema_field(
            "channel_router.enabled",
            "boolean",
            "true",
            "boolean",
            false,
            false,
            "manual_review",
        ),
    ];
    for path in SECRET_CONFIG_PATHS {
        if fields.iter().all(|field| field.path != *path) {
            fields.push(schema_field(
                path,
                "string",
                "",
                "secret value or vault ref",
                true,
                false,
                "restart_required",
            ));
        }
    }
    fields
}

fn schema_field(
    path: &str,
    value_type: &str,
    default_value: &str,
    validator: &str,
    secret: bool,
    reloadable: bool,
    reload_impact: &str,
) -> RuntimeConfigSchemaField {
    RuntimeConfigSchemaField {
        path: path.to_owned(),
        value_type: value_type.to_owned(),
        default_value: if secret || is_secret_config_path(path) {
            "<redacted>".to_owned()
        } else {
            default_value.to_owned()
        },
        validator: validator.to_owned(),
        sensitivity: if secret || is_secret_config_path(path) {
            ToolResultSensitivity::Secret
        } else {
            ToolResultSensitivity::Public
        },
        reloadable,
        reload_impact: reload_impact.to_owned(),
    }
}

fn publish_command_event(
    state: &AppState,
    context: &CommandRouterContext,
    command: RealtimeCommand,
    result: &Value,
) {
    let topic = match command {
        RealtimeCommand::RunCreate
        | RealtimeCommand::RunWait
        | RealtimeCommand::RunEvents
        | RealtimeCommand::RunAbort
        | RealtimeCommand::RunGet => RealtimeEventTopic::Run,
        RealtimeCommand::ApprovalList
        | RealtimeCommand::ApprovalGet
        | RealtimeCommand::ApprovalDecide => RealtimeEventTopic::Approval,
        RealtimeCommand::NodePresence
        | RealtimeCommand::NodeCapabilityGrant
        | RealtimeCommand::NodeCapabilityRevoke => RealtimeEventTopic::Node,
        RealtimeCommand::ConfigSchemaLookup
        | RealtimeCommand::ConfigReloadPlan
        | RealtimeCommand::ConfigReloadApply => RealtimeEventTopic::Config,
    };
    let owner_session_id = result
        .pointer("/run/session_id")
        .or_else(|| result.pointer("/approval/session_id"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let payload = json!({
        "command": command.as_str(),
        "role": context.realtime.role.as_str(),
        "run_id": result.pointer("/run/run_id").and_then(Value::as_str),
        "approval_id": result.pointer("/approval/approval_id").and_then(Value::as_str),
        "device_id": result.pointer("/device_id").and_then(Value::as_str),
        "outcome": result.pointer("/reload/outcome").and_then(Value::as_str),
    });
    let mut router = state.realtime_events.lock().unwrap_or_else(|error| error.into_inner());
    let _ = router.publish(RealtimeEventEnvelope {
        schema_version: 1,
        sequence: 0,
        event_id: Ulid::new().to_string(),
        topic,
        sensitivity: RealtimeEventSensitivity::Internal,
        owner_principal: Some(context.request_context.principal.clone()),
        owner_session_id,
        occurred_at_unix_ms: crate::unix_ms_now().unwrap_or(0),
        payload,
    });
}

async fn begin_command_idempotency(
    state: &AppState,
    envelope: &RealtimeCommandEnvelope,
) -> Result<CommandIdempotencyOutcome, StableErrorEnvelope> {
    let descriptor = descriptor_for_command(envelope.command).ok_or_else(|| {
        stable_error(
            "realtime/unknown_command",
            "realtime command is not registered",
            "refresh the method registry and retry with a supported command",
        )
    })?;
    if !descriptor.side_effecting {
        return Ok(CommandIdempotencyOutcome { replayed: false, result: None });
    }
    let Some(raw_key) = envelope.idempotency_key.as_deref() else {
        return Ok(CommandIdempotencyOutcome { replayed: false, result: None });
    };
    let payload = serde_json::to_vec(&json!({
        "command": envelope.command.as_str(),
        "params": envelope.params,
        "expected_version": envelope.expected_version,
    }))
    .map_err(|error| {
        stable_error(
            "command/payload_encode_failed",
            error.to_string(),
            "send JSON-safe command params",
        )
    })?;
    let key = format!("realtime:{}:{raw_key}", envelope.command.as_str());
    let runtime = Arc::clone(&state.runtime);
    let command = envelope.command;
    let payload_sha256 = crate::sha256_hex(payload.as_slice());
    let begin = tokio::task::spawn_blocking(move || {
        runtime.journal_store.begin_idempotency_operation(&IdempotencyBeginRequest {
            key,
            scope: "realtime_command".to_owned(),
            operation_kind: command.as_str().to_owned(),
            payload_sha256,
            expires_at_unix_ms: Some(
                crate::unix_ms_now().unwrap_or(0).saturating_add(24 * 60 * 60 * 1_000),
            ),
        })
    })
    .await
    .map_err(|_| {
        stable_error(
            "command/idempotency_worker_failed",
            "idempotency worker panicked",
            "retry the command",
        )
    })?
    .map_err(|error| {
        stable_error(
            "command/idempotency_failed",
            error.to_string(),
            "retry after idempotency store recovers",
        )
    })?;

    match begin.decision {
        IdempotencyReplayDecision::CompletedReplayResult => Ok(CommandIdempotencyOutcome {
            replayed: true,
            result: replay_result_from_record(begin.record.as_ref())?,
        }),
        IdempotencyReplayDecision::ConflictingPayload => Err(stable_error(
            "command/idempotency_conflict",
            "idempotency key was reused with a different command payload",
            "retry with a new idempotency_key for the changed payload",
        )),
        IdempotencyReplayDecision::SamePayloadRetry => Err(stable_error(
            "command/idempotency_in_progress",
            "idempotency key already has a same-payload operation that has not completed",
            "wait for the original command result or retry after the idempotency record expires",
        )),
        IdempotencyReplayDecision::Reserved | IdempotencyReplayDecision::ExpiredRetry => {
            Ok(CommandIdempotencyOutcome { replayed: false, result: None })
        }
    }
}

fn replay_result_from_record(
    record: Option<&IdempotencyRecordSnapshot>,
) -> Result<Option<Value>, StableErrorEnvelope> {
    record
        .and_then(|record| record.result_json.as_deref())
        .map(serde_json::from_str)
        .transpose()
        .map_err(|error| {
            stable_error(
                "command/idempotency_replay_decode_failed",
                format!("stored idempotency result is not valid JSON: {error}"),
                "retry with a new idempotency_key or repair the idempotency store",
            )
        })
}

async fn complete_command_idempotency(
    state: &AppState,
    envelope: &RealtimeCommandEnvelope,
    result: &Value,
) -> Result<(), StableErrorEnvelope> {
    let Some(raw_key) = envelope.idempotency_key.as_deref() else {
        return Ok(());
    };
    let descriptor = descriptor_for_command(envelope.command).ok_or_else(|| {
        stable_error(
            "realtime/unknown_command",
            "realtime command is not registered",
            "refresh method registry",
        )
    })?;
    if !descriptor.side_effecting {
        return Ok(());
    }
    let key = format!("realtime:{}:{raw_key}", envelope.command.as_str());
    let result_json = result.to_string();
    let runtime = Arc::clone(&state.runtime);
    tokio::task::spawn_blocking(move || {
        runtime
            .journal_store
            .complete_idempotency_operation(&IdempotencyCompleteRequest { key, result_json })
    })
    .await
    .map_err(|_| {
        stable_error(
            "command/idempotency_worker_failed",
            "idempotency worker panicked",
            "retry the command",
        )
    })?
    .map(|_| ())
    .map_err(|error| {
        stable_error(
            "command/idempotency_failed",
            error.to_string(),
            "retry after idempotency store recovers",
        )
    })
}

async fn fail_command_idempotency(
    state: &AppState,
    envelope: &RealtimeCommandEnvelope,
    error: &StableErrorEnvelope,
) -> Result<(), StableErrorEnvelope> {
    let Some(raw_key) = envelope.idempotency_key.as_deref() else {
        return Ok(());
    };
    let descriptor = descriptor_for_command(envelope.command).ok_or_else(|| {
        stable_error(
            "realtime/unknown_command",
            "realtime command is not registered",
            "refresh method registry",
        )
    })?;
    if !descriptor.side_effecting {
        return Ok(());
    }
    let key = format!("realtime:{}:{raw_key}", envelope.command.as_str());
    let runtime = Arc::clone(&state.runtime);
    let error = error.clone();
    tokio::task::spawn_blocking(move || {
        runtime.journal_store.fail_idempotency_operation(&IdempotencyFailRequest { key, error })
    })
    .await
    .map_err(|_| {
        stable_error(
            "command/idempotency_worker_failed",
            "idempotency worker panicked",
            "retry the command",
        )
    })?
    .map(|_| ())
    .map_err(|store_error| {
        stable_error(
            "command/idempotency_failed",
            store_error.to_string(),
            "retry after idempotency store recovers",
        )
    })
}

fn required_run_id(params: &Value) -> Result<&str, StableErrorEnvelope> {
    let run_id = required_str(params, "run_id")?;
    validate_canonical_id(run_id).map_err(|_| {
        stable_error(
            "command/invalid_run_id",
            "run_id must be a canonical ULID",
            "use a valid run_id",
        )
    })?;
    Ok(run_id)
}

fn required_str<'a>(params: &'a Value, key: &str) -> Result<&'a str, StableErrorEnvelope> {
    params
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            stable_error(
                "command/missing_parameter",
                format!("missing required string parameter `{key}`"),
                "send all required command parameters",
            )
        })
}

fn optional_str<'a>(params: &'a Value, key: &str) -> Option<&'a str> {
    params.get(key).and_then(Value::as_str).map(str::trim).filter(|value| !value.is_empty())
}

fn optional_u64(params: &Value, key: &str) -> Option<u64> {
    params.get(key).and_then(Value::as_u64)
}

fn optional_usize(params: &Value, key: &str) -> Option<usize> {
    optional_u64(params, key).and_then(|value| usize::try_from(value).ok())
}

fn parse_decision_scope(value: Option<&str>) -> Result<ApprovalDecisionScope, StableErrorEnvelope> {
    let Some(value) = value else {
        return Ok(ApprovalDecisionScope::Once);
    };
    ApprovalDecisionScope::from_str(value).ok_or_else(|| {
        stable_error(
            "approval/invalid_decision_scope",
            "decision_scope must be one of once|session|timeboxed",
            "send a supported approval decision scope",
        )
    })
}

fn ensure_expected_approval_version(
    actual_updated_at_unix_ms: i64,
    expected_version: u64,
) -> Result<(), StableErrorEnvelope> {
    if u64::try_from(actual_updated_at_unix_ms).ok() != Some(expected_version) {
        return Err(stable_error(
            "approval/version_conflict",
            "approval version changed before realtime decision",
            "refresh the approval record and retry with the new expected_version",
        ));
    }
    Ok(())
}

fn ensure_approval_not_racing_abort(
    approved: bool,
    run_cancel_requested: bool,
) -> Result<(), StableErrorEnvelope> {
    if approved && run_cancel_requested {
        return Err(stable_error(
            "approval/abort_race",
            "approval allow raced with an already requested run abort",
            "refresh run and approval state before deciding again",
        ));
    }
    Ok(())
}

fn command_ok(
    envelope: RealtimeCommandEnvelope,
    result: Value,
    replayed: bool,
) -> RealtimeCommandResultEnvelope {
    RealtimeCommandResultEnvelope {
        request_id: envelope.request_id,
        command: envelope.command,
        ok: true,
        result: Some(result),
        error: None,
        idempotency_key: envelope.idempotency_key,
        replayed,
    }
}

fn command_error(
    envelope: RealtimeCommandEnvelope,
    error: StableErrorEnvelope,
    replayed: bool,
) -> RealtimeCommandResultEnvelope {
    RealtimeCommandResultEnvelope {
        request_id: envelope.request_id,
        command: envelope.command,
        ok: false,
        result: None,
        error: Some(error),
        idempotency_key: envelope.idempotency_key,
        replayed,
    }
}

fn stable_error_from_status(status: Status) -> StableErrorEnvelope {
    stable_error(
        format!("status/{:?}", status.code()).to_ascii_lowercase(),
        status.message().to_owned(),
        "inspect the command parameters and retry after refreshing runtime state",
    )
}

fn stable_error_from_response(response: axum::response::Response) -> StableErrorEnvelope {
    stable_error(
        format!("http/{}", response.status().as_u16()),
        response.status().canonical_reason().unwrap_or("request failed").to_owned(),
        "inspect the command parameters and retry after refreshing runtime state",
    )
}

fn stable_error(
    code: impl Into<String>,
    message: impl Into<String>,
    recovery_hint: impl Into<String>,
) -> StableErrorEnvelope {
    StableErrorEnvelope::new(code, message, recovery_hint)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_lookup_redacts_secret_defaults() {
        let fields = runtime_config_schema_fields();
        let secret = fields
            .iter()
            .find(|field| field.path == "model_provider.openai_api_key")
            .expect("schema should include provider key");
        assert_eq!(secret.default_value, "<redacted>");
        assert_eq!(secret.sensitivity, ToolResultSensitivity::Secret);
    }

    #[test]
    fn required_run_id_rejects_non_canonical_values() {
        let error = required_run_id(&json!({ "run_id": "not-a-ulid" }))
            .expect_err("invalid run_id should fail");
        assert_eq!(error.code, "command/invalid_run_id");
    }

    #[test]
    fn schema_lookup_rejects_unknown_path() {
        let error = config_schema_lookup(&RealtimeCommandEnvelope {
            request_id: "req-1".to_owned(),
            command: RealtimeCommand::ConfigSchemaLookup,
            params: json!({ "path": "missing.path" }),
            idempotency_key: None,
            expected_version: None,
        })
        .expect_err("unknown schema path should fail");
        assert_eq!(error.code, "config/schema_not_found");
    }

    #[test]
    fn completed_idempotency_record_replays_stored_result() {
        let replayed = replay_result_from_record(Some(&IdempotencyRecordSnapshot {
            key: "realtime:run.abort:k1".to_owned(),
            scope: "realtime_command".to_owned(),
            operation_kind: "run.abort".to_owned(),
            payload_sha256: "abc".to_owned(),
            state: palyra_common::runtime_contracts::IdempotencyOperationState::Completed,
            result_json: Some(r#"{"cancel":{"accepted":true}}"#.to_owned()),
            error: None,
            first_seen_at_unix_ms: 1,
            updated_at_unix_ms: 2,
            expires_at_unix_ms: None,
        }))
        .expect("stored result should decode")
        .expect("completed record should carry a result");
        assert_eq!(replayed["cancel"]["accepted"], true);
    }

    #[test]
    fn approval_preflight_blocks_stale_version_and_abort_race() {
        let version_error =
            ensure_expected_approval_version(10, 9).expect_err("version mismatch should fail");
        assert_eq!(version_error.code, "approval/version_conflict");

        let race_error =
            ensure_approval_not_racing_abort(true, true).expect_err("abort race should fail");
        assert_eq!(race_error.code, "approval/abort_race");
        assert!(ensure_approval_not_racing_abort(false, true).is_ok());
    }
}
