//! Model-visible read, message, and control operations for related sessions.
//! All mutations require the exact background-task ownership token and pass
//! through durable queue, cancellation, and command-ledger authorities.

use std::{collections::BTreeMap, sync::Arc};

use palyra_common::{
    redaction::{redact_auth_error, redact_url_segments_in_text},
    runtime_contracts::{
        AuxiliaryTaskState, LiveModelSwitchRequest, QueueDecision, QueueMode,
        SessionInterruptRequest, SessionMessageRequest, SessionSteerRequest,
    },
};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tonic::Status;

use crate::{
    application::session_queue::SessionQueueSafeBoundary,
    gateway::{
        GatewayRuntimeState, ListOrchestratorSessionsRequest, SessionQueueAdmissionRequest,
        ToolRuntimeExecutionContext, SESSIONS_HISTORY_TOOL_NAME, SESSIONS_INTERRUPT_TOOL_NAME,
        SESSIONS_LIST_TOOL_NAME, SESSIONS_SEND_TOOL_NAME, SESSIONS_STATUS_TOOL_NAME,
        SESSIONS_STEER_TOOL_NAME, SESSIONS_SWITCH_MODEL_TOOL_NAME,
    },
    journal::{
        OrchestratorBackgroundTaskListFilter, OrchestratorBackgroundTaskRecord,
        OrchestratorBackgroundTaskUpdateRequest, OrchestratorCancelRequest,
        OrchestratorQueuedInputRecord, OrchestratorSessionQuickControlsUpdateRequest,
        OrchestratorSessionRecord, SessionModelCommandKind, SessionModelCommandRecord,
        SessionModelCommandReserveOutcome, SessionModelCommandReserveRequest,
        SessionModelCommandSettlementRequest,
    },
    tool_protocol::{build_tool_execution_outcome, ToolExecutionOutcome},
};

mod projection;

use projection::{
    command_outcome_json, redact_payload_json, related_session_map, run_status_json,
    session_summary_json,
};

const SESSION_TOOL_EXECUTOR: &str = "session_operations_runtime";
const SESSION_TOOL_SANDBOX: &str = "session_lineage_scope";
const MAX_SESSION_LIST_RESULTS: usize = 64;
const MAX_SESSION_HISTORY_RESULTS: usize = 32;
const MAX_SESSION_MESSAGE_BYTES: usize = 4_096;
const MAX_SESSION_INSTRUCTION_BYTES: usize = 8_192;

#[derive(Debug, Default, Deserialize)]
struct SessionsListInput {
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct SessionReadInput {
    target_session_id: String,
    #[serde(default)]
    limit: Option<usize>,
}

struct AuthorizedTarget {
    session: OrchestratorSessionRecord,
    task: Option<OrchestratorBackgroundTaskRecord>,
}

enum CommandAdmission {
    Proceed(u64),
    Settled(Value),
}

/// Executes one model-visible session operation and folds all errors into the
/// normal redacted tool outcome contract.
pub(crate) async fn execute_session_operation_tool(
    runtime: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    tool_name: &str,
    proposal_id: &str,
    input_json: &[u8],
) -> ToolExecutionOutcome {
    let result =
        execute_session_operation_tool_inner(runtime, &context, tool_name, input_json).await;
    match result {
        Ok(output) => {
            build_outcome(proposal_id, tool_name, input_json, true, output, String::new())
        }
        Err(error) => build_outcome(
            proposal_id,
            tool_name,
            input_json,
            false,
            session_error_json(&error),
            error.message().to_owned(),
        ),
    }
}

async fn execute_session_operation_tool_inner(
    runtime: &Arc<GatewayRuntimeState>,
    context: &ToolRuntimeExecutionContext<'_>,
    tool_name: &str,
    input_json: &[u8],
) -> Result<Value, Status> {
    match tool_name {
        SESSIONS_LIST_TOOL_NAME => {
            let input = parse_input::<SessionsListInput>(tool_name, input_json)?;
            sessions_list(runtime, context, input).await
        }
        SESSIONS_STATUS_TOOL_NAME => {
            let input = parse_input::<SessionReadInput>(tool_name, input_json)?;
            sessions_status(runtime, context, input).await
        }
        SESSIONS_HISTORY_TOOL_NAME => {
            let input = parse_input::<SessionReadInput>(tool_name, input_json)?;
            sessions_history(runtime, context, input).await
        }
        SESSIONS_SEND_TOOL_NAME => {
            let input = parse_input::<SessionMessageRequest>(tool_name, input_json)?;
            sessions_send(runtime, context, input).await
        }
        SESSIONS_STEER_TOOL_NAME => {
            let input = parse_input::<SessionSteerRequest>(tool_name, input_json)?;
            sessions_steer(runtime, context, input).await
        }
        SESSIONS_INTERRUPT_TOOL_NAME => {
            let input = parse_input::<SessionInterruptRequest>(tool_name, input_json)?;
            sessions_interrupt(runtime, context, input).await
        }
        SESSIONS_SWITCH_MODEL_TOOL_NAME => {
            let input = parse_input::<LiveModelSwitchRequest>(tool_name, input_json)?;
            sessions_switch_model(runtime, context, input).await
        }
        _ => Err(Status::invalid_argument("unsupported session operation tool name")),
    }
}

async fn sessions_list(
    runtime: &Arc<GatewayRuntimeState>,
    context: &ToolRuntimeExecutionContext<'_>,
    input: SessionsListInput,
) -> Result<Value, Status> {
    let limit = input.limit.unwrap_or(32).clamp(1, MAX_SESSION_LIST_RESULTS);
    let (scoped, _) = runtime
        .list_orchestrator_sessions(ListOrchestratorSessionsRequest {
            after_session_key: None,
            principal: context.principal.to_owned(),
            device_id: context.device_id.to_owned(),
            channel: context.channel.map(ToOwned::to_owned),
            include_archived: false,
            requested_limit: Some(MAX_SESSION_LIST_RESULTS),
            search_query: None,
        })
        .await?;
    let related = related_session_map(context.session_id, scoped);
    let tasks = scoped_lineage_tasks(runtime, context, &related).await?;
    let tasks_by_session = tasks
        .iter()
        .filter_map(|task| {
            task.child_session_id.as_ref().map(|session_id| (session_id.clone(), task))
        })
        .collect::<BTreeMap<_, _>>();
    let run_ids =
        related.values().filter_map(|session| session.last_run_id.clone()).collect::<Vec<_>>();
    let runs = runtime.list_orchestrator_run_status_snapshots(run_ids).await?;
    let runs_by_id = runs.iter().map(|run| (run.run_id.as_str(), run)).collect::<BTreeMap<_, _>>();
    let generations = runtime
        .list_scoped_session_runtime_generations(
            context.principal.to_owned(),
            context.device_id.to_owned(),
            context.channel.map(ToOwned::to_owned),
        )
        .await?;
    let generations_by_run = generations
        .iter()
        .map(|record| (record.run_id.as_str(), record.generation))
        .collect::<BTreeMap<_, _>>();

    let summaries = related
        .values()
        .take(limit)
        .map(|session| {
            let task = tasks_by_session.get(session.session_id.as_str()).copied();
            let run =
                session.last_run_id.as_deref().and_then(|run_id| runs_by_id.get(run_id).copied());
            let generation = session
                .last_run_id
                .as_deref()
                .and_then(|run_id| generations_by_run.get(run_id).copied());
            session_summary_json(context.session_id, session, run, task, generation)
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "schema_version": 2,
        "sessions": summaries,
        "count": summaries.len(),
        "bounded": true,
    }))
}

async fn sessions_status(
    runtime: &Arc<GatewayRuntimeState>,
    context: &ToolRuntimeExecutionContext<'_>,
    input: SessionReadInput,
) -> Result<Value, Status> {
    let target = authorize_read_target(runtime, context, input.target_session_id.as_str()).await?;
    let run = if let Some(run_id) = target.session.last_run_id.as_ref() {
        runtime.orchestrator_run_status_snapshot(run_id.clone()).await?
    } else {
        None
    };
    let generation = if let Some(run) = run.as_ref() {
        runtime
            .runtime_generation_for_run(run.run_id.clone())
            .await?
            .map(|(_, generation)| generation.get())
    } else {
        None
    };
    Ok(json!({
        "schema_version": 2,
        "session": session_summary_json(
            context.session_id,
            &target.session,
            run.as_ref(),
            target.task.as_ref(),
            generation,
        ),
        "generation": generation,
        "run": run.as_ref().map(run_status_json),
    }))
}

async fn sessions_history(
    runtime: &Arc<GatewayRuntimeState>,
    context: &ToolRuntimeExecutionContext<'_>,
    input: SessionReadInput,
) -> Result<Value, Status> {
    let target = authorize_read_target(runtime, context, input.target_session_id.as_str()).await?;
    let limit = input.limit.unwrap_or(16).clamp(1, MAX_SESSION_HISTORY_RESULTS);
    let transcript = runtime
        .list_bounded_orchestrator_session_transcript(target.session.session_id.clone(), limit)
        .await?;
    let history = transcript
        .into_iter()
        .map(|record| {
            json!({
                "source_ref": {
                    "session_id": record.session_id,
                    "run_id": record.run_id,
                    "tape_seq": record.seq,
                },
                "event_type": record.event_type,
                "payload": redact_payload_json(record.payload_json.as_str()),
                "created_at_unix_ms": record.created_at_unix_ms,
                "origin_kind": record.origin_kind,
                "origin_run_id": record.origin_run_id,
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "schema_version": 1,
        "target_session_id": target.session.session_id,
        "history": history,
        "count": history.len(),
        "bounded": true,
        "redacted": true,
    }))
}

async fn sessions_send(
    runtime: &Arc<GatewayRuntimeState>,
    context: &ToolRuntimeExecutionContext<'_>,
    input: SessionMessageRequest,
) -> Result<Value, Status> {
    let message =
        normalize_bounded_text(input.message.as_str(), "message", MAX_SESSION_MESSAGE_BYTES)?;
    let target = authorize_write_target(
        runtime,
        context,
        input.target_session_id.as_str(),
        input.ownership_token.as_str(),
        false,
    )
    .await?;
    let (task, target_run_id) = target_command_binding(&target)?;
    let reserve = reserve_command(
        runtime,
        context,
        input.request_id,
        SessionModelCommandKind::Send,
        &target,
        task,
        target_run_id.as_str(),
        message.as_bytes(),
        None,
        input.expected_generation,
    )
    .await?;
    if let Some(outcome) = replay_session_command(runtime, &reserve, true).await? {
        return Ok(outcome);
    }
    let generation = match admit_active_command(
        runtime,
        &reserve,
        target_run_id.as_str(),
        input.expected_generation,
    )
    .await?
    {
        CommandAdmission::Proceed(generation) => generation,
        CommandAdmission::Settled(outcome) => return Ok(outcome),
    };
    let command = admit_and_settle_queue_command(
        runtime,
        &reserve.command,
        SessionQueueAdmissionRequest {
            queued_input_id: Some(reserve.command.command_id.clone()),
            session_id: target.session.session_id.clone(),
            run_id: target_run_id,
            origin_run_id: Some(context.run_id.to_owned()),
            text: format!(
                "[message from authorized parent session {}] {}",
                context.session_id, message
            ),
            requested_mode: Some(QueueMode::Followup),
            policy_channel: context.channel.map(ToOwned::to_owned),
            policy_agent_id: None,
            safe_boundary: SessionQueueSafeBoundary::active(true, false),
            actor_principal: context.principal.to_owned(),
            actor_device_id: context.device_id.to_owned(),
            actor_channel: context.channel.map(ToOwned::to_owned),
            source: "model_visible.sessions_send".to_owned(),
        },
    )
    .await?;
    Ok(command_outcome_json(&command, Some(generation), reserve.superseded_command_id))
}

async fn sessions_steer(
    runtime: &Arc<GatewayRuntimeState>,
    context: &ToolRuntimeExecutionContext<'_>,
    input: SessionSteerRequest,
) -> Result<Value, Status> {
    let instruction = normalize_bounded_text(
        input.instruction.as_str(),
        "instruction",
        MAX_SESSION_INSTRUCTION_BYTES,
    )?;
    let target = authorize_write_target(
        runtime,
        context,
        input.target_session_id.as_str(),
        input.ownership_token.as_str(),
        true,
    )
    .await?;
    let (task, target_run_id) = target_command_binding(&target)?;
    let reserve = reserve_command(
        runtime,
        context,
        input.request_id,
        SessionModelCommandKind::Steer,
        &target,
        task,
        target_run_id.as_str(),
        instruction.as_bytes(),
        None,
        input.expected_generation,
    )
    .await?;
    if let Some(outcome) = replay_session_command(runtime, &reserve, true).await? {
        return Ok(outcome);
    }
    let generation = match admit_active_command(
        runtime,
        &reserve,
        target_run_id.as_str(),
        input.expected_generation,
    )
    .await?
    {
        CommandAdmission::Proceed(generation) => generation,
        CommandAdmission::Settled(outcome) => return Ok(outcome),
    };
    let command = admit_and_settle_queue_command(
        runtime,
        &reserve.command,
        SessionQueueAdmissionRequest {
            queued_input_id: Some(reserve.command.command_id.clone()),
            session_id: target.session.session_id.clone(),
            run_id: target_run_id,
            origin_run_id: Some(context.run_id.to_owned()),
            text: instruction,
            requested_mode: Some(QueueMode::Steer),
            policy_channel: context.channel.map(ToOwned::to_owned),
            policy_agent_id: None,
            safe_boundary: SessionQueueSafeBoundary::active(true, false),
            actor_principal: context.principal.to_owned(),
            actor_device_id: context.device_id.to_owned(),
            actor_channel: context.channel.map(ToOwned::to_owned),
            source: "model_visible.sessions_steer".to_owned(),
        },
    )
    .await?;
    Ok(command_outcome_json(&command, Some(generation), reserve.superseded_command_id))
}

async fn sessions_interrupt(
    runtime: &Arc<GatewayRuntimeState>,
    context: &ToolRuntimeExecutionContext<'_>,
    input: SessionInterruptRequest,
) -> Result<Value, Status> {
    let reason = normalize_optional_text(input.reason.as_deref(), MAX_SESSION_MESSAGE_BYTES)
        .unwrap_or_else(|| "sessions.interrupt.parent_requested".to_owned());
    let target = authorize_write_target(
        runtime,
        context,
        input.target_session_id.as_str(),
        input.ownership_token.as_str(),
        true,
    )
    .await?;
    let (task, target_run_id) = target_command_binding(&target)?;
    let reserve = reserve_command(
        runtime,
        context,
        input.request_id,
        SessionModelCommandKind::Interrupt,
        &target,
        task,
        target_run_id.as_str(),
        reason.as_bytes(),
        None,
        input.expected_generation,
    )
    .await?;
    if let Some(outcome) = replay_session_command(runtime, &reserve, false).await? {
        return Ok(outcome);
    }
    let generation = match admit_active_command(
        runtime,
        &reserve,
        target_run_id.as_str(),
        input.expected_generation,
    )
    .await?
    {
        CommandAdmission::Proceed(generation) => generation,
        CommandAdmission::Settled(outcome) => return Ok(outcome),
    };
    let cancel = runtime
        .request_orchestrator_cancel(OrchestratorCancelRequest { run_id: target_run_id, reason })
        .await?;
    runtime
        .update_orchestrator_background_task(OrchestratorBackgroundTaskUpdateRequest {
            task_id: task.task_id.clone(),
            expected_revision: task.revision,
            state: Some(AuxiliaryTaskState::CancelRequested.as_str().to_owned()),
            last_error: Some(Some("session.interrupt.parent_requested".to_owned())),
            ..Default::default()
        })
        .await?;
    let command = runtime
        .settle_session_model_command(SessionModelCommandSettlementRequest {
            command_id: reserve.command.command_id,
            state: "interrupted".to_owned(),
            reason_code: "session.interrupt.accepted".to_owned(),
            queued_input_id: None,
        })
        .await?;
    Ok(json!({
        "schema_version": 1,
        "outcome": command_outcome_json(
            &command,
            Some(generation),
            reserve.superseded_command_id
        ),
        "cancel": cancel,
        "restart": false,
    }))
}

async fn sessions_switch_model(
    runtime: &Arc<GatewayRuntimeState>,
    context: &ToolRuntimeExecutionContext<'_>,
    input: LiveModelSwitchRequest,
) -> Result<Value, Status> {
    let model_profile = normalize_bounded_text(input.model_profile.as_str(), "model_profile", 128)?;
    let instruction =
        normalize_optional_text(input.instruction.as_deref(), MAX_SESSION_INSTRUCTION_BYTES)
            .unwrap_or_else(|| {
                "Continue the current objective using the newly selected model route.".to_owned()
            });
    let target = authorize_write_target(
        runtime,
        context,
        input.target_session_id.as_str(),
        input.ownership_token.as_str(),
        true,
    )
    .await?;
    let (task, target_run_id) = target_command_binding(&target)?;
    let reserve = reserve_command(
        runtime,
        context,
        input.request_id,
        SessionModelCommandKind::SwitchModel,
        &target,
        task,
        target_run_id.as_str(),
        instruction.as_bytes(),
        Some(model_profile.clone()),
        input.expected_generation,
    )
    .await?;
    if let Some(outcome) = replay_session_command(runtime, &reserve, true).await? {
        return Ok(outcome);
    }
    let generation = match admit_active_command(
        runtime,
        &reserve,
        target_run_id.as_str(),
        input.expected_generation,
    )
    .await?
    {
        CommandAdmission::Proceed(generation) => generation,
        CommandAdmission::Settled(outcome) => return Ok(outcome),
    };
    if !model_profile_is_available(runtime, model_profile.as_str()) {
        let command = runtime
            .settle_session_model_command(SessionModelCommandSettlementRequest {
                command_id: reserve.command.command_id.clone(),
                state: "rejected".to_owned(),
                reason_code: "session.command.model_unavailable".to_owned(),
                queued_input_id: None,
            })
            .await?;
        return Ok(command_outcome_json(&command, Some(generation), reserve.superseded_command_id));
    }
    let updated = runtime
        .update_orchestrator_session_quick_controls(OrchestratorSessionQuickControlsUpdateRequest {
            session_id: target.session.session_id.clone(),
            principal: context.principal.to_owned(),
            device_id: context.device_id.to_owned(),
            channel: context.channel.map(ToOwned::to_owned),
            model_profile_override: Some(Some(model_profile)),
            thinking_override: None,
            trace_override: None,
            verbose_override: None,
        })
        .await?;
    let command = admit_and_settle_queue_command(
        runtime,
        &reserve.command,
        SessionQueueAdmissionRequest {
            queued_input_id: Some(reserve.command.command_id.clone()),
            session_id: target.session.session_id,
            run_id: target_run_id,
            origin_run_id: Some(context.run_id.to_owned()),
            text: instruction,
            requested_mode: Some(QueueMode::Steer),
            policy_channel: context.channel.map(ToOwned::to_owned),
            policy_agent_id: None,
            safe_boundary: SessionQueueSafeBoundary::active(true, false),
            actor_principal: context.principal.to_owned(),
            actor_device_id: context.device_id.to_owned(),
            actor_channel: context.channel.map(ToOwned::to_owned),
            source: "model_visible.sessions_switch_model".to_owned(),
        },
    )
    .await?;
    Ok(json!({
        "schema_version": 1,
        "outcome": command_outcome_json(
            &command,
            Some(generation),
            reserve.superseded_command_id
        ),
        "model_profile": updated.model_profile_override,
        "approval_subject": "preserved",
        "confirmed_tool_effects": "preserved",
        "auth_selection": "reselect_on_next_provider_attempt",
    }))
}

async fn authorize_read_target(
    runtime: &Arc<GatewayRuntimeState>,
    context: &ToolRuntimeExecutionContext<'_>,
    target_session_id: &str,
) -> Result<AuthorizedTarget, Status> {
    let (target, related) = load_scoped_related_target(runtime, context, target_session_id).await?;
    if target.session_id == context.session_id {
        return Ok(AuthorizedTarget { session: target, task: None });
    }
    let tasks = scoped_lineage_tasks(runtime, context, &related).await?;
    let task = tasks
        .into_iter()
        .find(|task| task.child_session_id.as_deref() == Some(target.session_id.as_str()))
        .ok_or_else(|| Status::permission_denied("target session is not an authorized child"))?;
    Ok(AuthorizedTarget { session: target, task: Some(task) })
}

async fn authorize_write_target(
    runtime: &Arc<GatewayRuntimeState>,
    context: &ToolRuntimeExecutionContext<'_>,
    target_session_id: &str,
    ownership_token: &str,
    allow_descendant: bool,
) -> Result<AuthorizedTarget, Status> {
    let (target, related) = load_scoped_related_target(runtime, context, target_session_id).await?;
    if target.session_id == context.session_id {
        return Err(Status::permission_denied(
            "cross-session command cannot target the calling session",
        ));
    }
    let task = runtime
        .get_orchestrator_background_task(ownership_token.trim().to_owned())
        .await?
        .ok_or_else(|| Status::permission_denied("session ownership token is invalid"))?;
    let capability_parent_is_related = related.contains_key(task.session_id.as_str());
    if (!allow_descendant && task.session_id != context.session_id)
        || !capability_parent_is_related
        || task.child_session_id.as_deref() != Some(target.session_id.as_str())
        || task.owner_principal != context.principal
        || task.device_id != context.device_id
        || task.channel.as_deref() != context.channel
    {
        return Err(Status::permission_denied(
            "session ownership token does not authorize the target",
        ));
    }
    Ok(AuthorizedTarget { session: target, task: Some(task) })
}

async fn load_scoped_related_target(
    runtime: &Arc<GatewayRuntimeState>,
    context: &ToolRuntimeExecutionContext<'_>,
    target_session_id: &str,
) -> Result<(OrchestratorSessionRecord, BTreeMap<String, OrchestratorSessionRecord>), Status> {
    let target_session_id = normalize_bounded_text(target_session_id, "target_session_id", 128)?;
    let target = runtime
        .orchestrator_session_by_id_snapshot(target_session_id)
        .await?
        .ok_or_else(|| Status::not_found("target session was not found"))?;
    if target.principal != context.principal
        || target.device_id != context.device_id
        || target.channel.as_deref() != context.channel
    {
        return Err(Status::permission_denied("target session is outside the caller scope"));
    }
    if target.archived_at_unix_ms.is_some() {
        return Err(Status::failed_precondition("target session is archived"));
    }
    let (scoped, _) = runtime
        .list_orchestrator_sessions(ListOrchestratorSessionsRequest {
            after_session_key: None,
            principal: context.principal.to_owned(),
            device_id: context.device_id.to_owned(),
            channel: context.channel.map(ToOwned::to_owned),
            include_archived: false,
            requested_limit: Some(256),
            search_query: None,
        })
        .await?;
    let related = related_session_map(context.session_id, scoped);
    if !related.contains_key(target.session_id.as_str()) {
        return Err(Status::permission_denied("target session is outside the caller lineage"));
    }
    Ok((target, related))
}

async fn scoped_lineage_tasks(
    runtime: &Arc<GatewayRuntimeState>,
    context: &ToolRuntimeExecutionContext<'_>,
    related: &BTreeMap<String, OrchestratorSessionRecord>,
) -> Result<Vec<OrchestratorBackgroundTaskRecord>, Status> {
    let tasks = runtime
        .list_orchestrator_background_tasks(OrchestratorBackgroundTaskListFilter {
            owner_principal: Some(context.principal.to_owned()),
            device_id: Some(context.device_id.to_owned()),
            channel: context.channel.map(ToOwned::to_owned),
            session_id: None,
            include_completed: true,
            limit: 256,
        })
        .await?;
    Ok(tasks
        .into_iter()
        .filter(|task| {
            related.contains_key(task.session_id.as_str())
                && task
                    .child_session_id
                    .as_deref()
                    .is_some_and(|session_id| related.contains_key(session_id))
        })
        .collect())
}

fn target_command_binding(
    target: &AuthorizedTarget,
) -> Result<(&OrchestratorBackgroundTaskRecord, String), Status> {
    let task = target
        .task
        .as_ref()
        .ok_or_else(|| Status::permission_denied("session command requires an ownership token"))?;
    let target_run_id = task
        .target_run_id
        .as_ref()
        .ok_or_else(|| Status::failed_precondition("target child run is not attached"))?
        .clone();
    Ok((task, target_run_id))
}

async fn admit_active_command(
    runtime: &Arc<GatewayRuntimeState>,
    reserve: &SessionModelCommandReserveOutcome,
    target_run_id: &str,
    expected_generation: Option<u64>,
) -> Result<CommandAdmission, Status> {
    let active_generation = runtime.runtime_generation_for_run(target_run_id.to_owned()).await?;
    let Some((_, generation)) = active_generation else {
        let command = runtime
            .settle_session_model_command(SessionModelCommandSettlementRequest {
                command_id: reserve.command.command_id.clone(),
                state: "target_busy".to_owned(),
                reason_code: "session.command.target_not_active".to_owned(),
                queued_input_id: None,
            })
            .await?;
        return Ok(CommandAdmission::Settled(command_outcome_json(
            &command,
            None,
            reserve.superseded_command_id.clone(),
        )));
    };
    if expected_generation.is_some_and(|expected| expected != generation.get()) {
        let command = runtime
            .settle_session_model_command(SessionModelCommandSettlementRequest {
                command_id: reserve.command.command_id.clone(),
                state: "rejected".to_owned(),
                reason_code: "session.command.stale_generation".to_owned(),
                queued_input_id: None,
            })
            .await?;
        return Ok(CommandAdmission::Settled(command_outcome_json(
            &command,
            Some(generation.get()),
            reserve.superseded_command_id.clone(),
        )));
    }
    Ok(CommandAdmission::Proceed(generation.get()))
}

#[allow(clippy::too_many_arguments)]
async fn reserve_command(
    runtime: &Arc<GatewayRuntimeState>,
    context: &ToolRuntimeExecutionContext<'_>,
    request_key: String,
    command_kind: SessionModelCommandKind,
    target: &AuthorizedTarget,
    task: &OrchestratorBackgroundTaskRecord,
    target_run_id: &str,
    payload: &[u8],
    requested_model_profile: Option<String>,
    expected_generation: Option<u64>,
) -> Result<crate::journal::SessionModelCommandReserveOutcome, Status> {
    let payload_sha256 = session_command_payload_sha256(
        command_kind,
        target.session.session_id.as_str(),
        target_run_id,
        expected_generation,
        payload,
        requested_model_profile.as_deref(),
    );
    runtime
        .reserve_session_model_command(SessionModelCommandReserveRequest {
            request_key: normalize_bounded_text(request_key.as_str(), "request_id", 128)?,
            command_kind,
            owner_session_id: context.session_id.to_owned(),
            owner_run_id: context.run_id.to_owned(),
            target_session_id: target.session.session_id.clone(),
            target_run_id: target_run_id.to_owned(),
            ownership_task_id: task.task_id.clone(),
            owner_principal: context.principal.to_owned(),
            device_id: context.device_id.to_owned(),
            channel: context.channel.map(ToOwned::to_owned),
            payload_sha256,
            requested_model_profile,
        })
        .await
}

async fn replay_session_command(
    runtime: &Arc<GatewayRuntimeState>,
    reserve: &SessionModelCommandReserveOutcome,
    queue_backed: bool,
) -> Result<Option<Value>, Status> {
    if !reserve.duplicate {
        return Ok(None);
    }
    let generation = runtime
        .runtime_generation_for_run(reserve.command.target_run_id.clone())
        .await?
        .map(|(_, generation)| generation.get());
    if reserve.command.state != "reserved" {
        return Ok(Some(command_outcome_json(
            &reserve.command,
            generation,
            reserve.superseded_command_id.clone(),
        )));
    }
    if queue_backed {
        if let Some(queued_input) =
            runtime.orchestrator_queued_input_by_id(reserve.command.command_id.clone()).await?
        {
            let command =
                reconcile_queued_command(runtime, &reserve.command, &queued_input).await?;
            return Ok(Some(command_outcome_json(
                &command,
                generation,
                reserve.superseded_command_id.clone(),
            )));
        }
    } else if let Some(run) =
        runtime.orchestrator_run_status_snapshot(reserve.command.target_run_id.clone()).await?
    {
        if run.cancel_requested || run.completed_at_unix_ms.is_some() {
            if let Some(task) = runtime
                .get_orchestrator_background_task(reserve.command.ownership_task_id.clone())
                .await?
            {
                if !matches!(
                    task.state.as_str(),
                    "succeeded" | "failed" | "cancelled" | "expired" | "cancel_requested"
                ) {
                    runtime
                        .update_orchestrator_background_task(
                            OrchestratorBackgroundTaskUpdateRequest {
                                task_id: task.task_id,
                                expected_revision: task.revision,
                                state: Some(
                                    AuxiliaryTaskState::CancelRequested.as_str().to_owned(),
                                ),
                                last_error: Some(Some(
                                    "session.interrupt.parent_requested".to_owned(),
                                )),
                                ..Default::default()
                            },
                        )
                        .await?;
                }
            }
            let command = runtime
                .settle_session_model_command(SessionModelCommandSettlementRequest {
                    command_id: reserve.command.command_id.clone(),
                    state: "interrupted".to_owned(),
                    reason_code: "session.interrupt.reconciled".to_owned(),
                    queued_input_id: None,
                })
                .await?;
            return Ok(Some(command_outcome_json(
                &command,
                generation,
                reserve.superseded_command_id.clone(),
            )));
        }
    }
    Ok(None)
}

async fn reconcile_queued_command(
    runtime: &Arc<GatewayRuntimeState>,
    command: &SessionModelCommandRecord,
    queued_input: &OrchestratorQueuedInputRecord,
) -> Result<SessionModelCommandRecord, Status> {
    let (state, reason_code) = reconciled_queue_settlement(queued_input.state.as_str());
    runtime
        .settle_session_model_command(SessionModelCommandSettlementRequest {
            command_id: command.command_id.clone(),
            state: state.to_owned(),
            reason_code: reason_code.to_owned(),
            queued_input_id: Some(queued_input.queued_input_id.clone()),
        })
        .await
}

fn reconciled_queue_settlement(queue_state: &str) -> (&'static str, &'static str) {
    match queue_state {
        "deferred" => ("target_busy", "session.command.reconciled_deferred"),
        "injected" | "forwarded" | "steered" | "merged" => {
            ("delivered", "session.command.reconciled_delivered")
        }
        "delivery_failed" | "rejected" | "overflowed" | "cancelled" | "superseded" => {
            ("rejected", "session.command.reconciled_rejected")
        }
        _ => ("queued", "session.command.reconciled_queued"),
    }
}

fn model_profile_is_available(runtime: &GatewayRuntimeState, model_profile: &str) -> bool {
    let snapshot = runtime.model_provider_status_snapshot();
    let registry_match = snapshot
        .registry
        .models
        .iter()
        .find(|model| model.model_id == model_profile && model.role == "chat" && model.enabled)
        .is_some_and(|model| {
            snapshot
                .registry
                .providers
                .iter()
                .any(|provider| provider.provider_id == model.provider_id && provider.enabled)
        });
    registry_match
        || [
            snapshot.registry.default_chat_model_id.as_deref(),
            snapshot.model_id.as_deref(),
            snapshot.openai_model.as_deref(),
            snapshot.anthropic_model.as_deref(),
        ]
        .into_iter()
        .flatten()
        .any(|configured| configured == model_profile)
}

async fn settle_queue_command(
    runtime: &Arc<GatewayRuntimeState>,
    command: &SessionModelCommandRecord,
    queue: &crate::gateway::SessionQueueAdmissionOutcome,
) -> Result<SessionModelCommandRecord, Status> {
    let (state, reason_code) = if !queue.decision.accepted {
        ("rejected", "session.command.queue_rejected")
    } else {
        match queue.decision.decision {
            QueueDecision::Defer => ("target_busy", "session.command.target_busy"),
            QueueDecision::Overflow => ("rejected", "session.command.queue_overflow"),
            _ => ("queued", "session.command.queued"),
        }
    };
    runtime
        .settle_session_model_command(SessionModelCommandSettlementRequest {
            command_id: command.command_id.clone(),
            state: state.to_owned(),
            reason_code: reason_code.to_owned(),
            queued_input_id: Some(queue.queued_input.queued_input_id.clone()),
        })
        .await
}

async fn admit_and_settle_queue_command(
    runtime: &Arc<GatewayRuntimeState>,
    command: &SessionModelCommandRecord,
    request: SessionQueueAdmissionRequest,
) -> Result<SessionModelCommandRecord, Status> {
    match runtime.admit_session_queued_input(request).await {
        Ok(queue) => settle_queue_command(runtime, command, &queue).await,
        Err(error) => {
            let Some(queued_input) =
                runtime.orchestrator_queued_input_by_id(command.command_id.clone()).await?
            else {
                return Err(error);
            };
            reconcile_queued_command(runtime, command, &queued_input).await
        }
    }
}

fn parse_input<T: for<'de> Deserialize<'de>>(
    tool_name: &str,
    input_json: &[u8],
) -> Result<T, Status> {
    serde_json::from_slice(input_json)
        .map_err(|error| Status::invalid_argument(format!("{tool_name} input is invalid: {error}")))
}

fn normalize_bounded_text(value: &str, field: &str, max_bytes: usize) -> Result<String, Status> {
    let value = value.trim();
    if value.is_empty() {
        return Err(Status::invalid_argument(format!("{field} cannot be empty")));
    }
    if value.len() > max_bytes {
        return Err(Status::invalid_argument(format!(
            "{field} exceeds the {max_bytes}-byte limit"
        )));
    }
    Ok(safe_text(value))
}

fn normalize_optional_text(value: Option<&str>, max_bytes: usize) -> Option<String> {
    value.and_then(|value| {
        let value = value.trim();
        (!value.is_empty()).then(|| truncate_text(safe_text(value), max_bytes))
    })
}

fn safe_text(value: &str) -> String {
    redact_url_segments_in_text(&redact_auth_error(value))
}

fn session_error_json(error: &Status) -> Value {
    let message = safe_text(error.message());
    let (outcome, reason_code) = if message.contains("generation changed") {
        ("superseded", "session.command.stale_generation")
    } else if message.contains("not active") {
        ("target_busy", "session.command.target_not_active")
    } else {
        ("rejected", "session.command.rejected")
    };
    json!({
        "schema_version": 1,
        "outcome": outcome,
        "reason_code": reason_code,
        "error": message,
    })
}

fn truncate_text(mut value: String, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value;
    }
    let mut boundary = max_bytes;
    while boundary > 0 && !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    value.truncate(boundary);
    value.push('…');
    value
}

fn session_command_payload_sha256(
    command_kind: SessionModelCommandKind,
    target_session_id: &str,
    target_run_id: &str,
    expected_generation: Option<u64>,
    payload: &[u8],
    requested_model_profile: Option<&str>,
) -> String {
    let mut digest = Sha256::new();
    for field in [
        command_kind.as_str().as_bytes(),
        target_session_id.as_bytes(),
        target_run_id.as_bytes(),
        expected_generation.unwrap_or_default().to_string().as_bytes(),
        requested_model_profile.unwrap_or_default().as_bytes(),
    ] {
        digest.update(field.len().to_le_bytes());
        digest.update(field);
    }
    digest.update(payload.len().to_le_bytes());
    digest.update(payload);
    hex::encode(digest.finalize())
}

fn build_outcome(
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    success: bool,
    output: Value,
    error: String,
) -> ToolExecutionOutcome {
    build_tool_execution_outcome(
        proposal_id,
        tool_name,
        input_json,
        success,
        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
        error,
        false,
        SESSION_TOOL_EXECUTOR.to_owned(),
        SESSION_TOOL_SANDBOX.to_owned(),
    )
}

#[cfg(test)]
mod tests {
    use super::reconciled_queue_settlement;

    #[test]
    fn queued_command_replay_maps_terminal_queue_states() {
        assert_eq!(
            reconciled_queue_settlement("forwarded"),
            ("delivered", "session.command.reconciled_delivered")
        );
        assert_eq!(
            reconciled_queue_settlement("deferred"),
            ("target_busy", "session.command.reconciled_deferred")
        );
        assert_eq!(
            reconciled_queue_settlement("superseded"),
            ("rejected", "session.command.reconciled_rejected")
        );
    }
}
