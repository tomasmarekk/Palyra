//! Tool-runtime executor for the scoped delegation tools.
//!
//! Implements `palyra.delegation.query` (list/status/merge_preview) and
//! `palyra.delegation.control` (delegate/interrupt) on top of orchestrator
//! background tasks. Dispatched from the gateway tool runtime
//! (`gateway::execute_tool_with_runtime_dispatch`); every free-text field is
//! redacted before it becomes model-visible output.

use std::{
    collections::BTreeSet,
    sync::Arc,
    time::{Duration, Instant},
};

use palyra_common::{
    redaction::{redact_auth_error, redact_url_segments_in_text},
    runtime_contracts::{AuxiliaryTaskKind, AuxiliaryTaskState},
};
use serde::Deserialize;
use serde_json::{json, Value};
use tonic::Status;
use ulid::Ulid;

use crate::{
    agents::AgentResolveRequest,
    delegation::{
        resolve_delegation_request, DelegationExecutionMode, DelegationManifestInput,
        DelegationMemoryScopeKind, DelegationParentContext, DelegationRequestInput,
    },
    gateway::{
        current_unix_ms, GatewayRuntimeState, ToolRuntimeExecutionContext,
        DELEGATION_CONTROL_TOOL_NAME, DELEGATION_QUERY_TOOL_NAME, SESSIONS_SPAWN_TOOL_NAME,
        SESSIONS_YIELD_TOOL_NAME,
    },
    journal::{
        OrchestratorBackgroundTaskCreateRequest, OrchestratorBackgroundTaskListFilter,
        OrchestratorBackgroundTaskRecord, OrchestratorBackgroundTaskUpdateRequest,
        OrchestratorCancelRequest,
    },
    tool_protocol::{build_tool_execution_outcome, ToolExecutionOutcome},
};

const DELEGATION_TOOL_EXECUTOR: &str = "delegation_runtime";
const DELEGATION_TOOL_SANDBOX: &str = "delegation_scope";
const MAX_DELEGATION_TOOL_TASKS: usize = 256;
const MAX_SESSIONS_YIELD_TIMEOUT_MS: u64 = 30_000;
const SESSIONS_YIELD_POLL_INTERVAL_MS: u64 = 100;

/// Combined input shape for both delegation tools; each operation reads only
/// the fields it needs and ignores the rest.
#[derive(Debug, Deserialize)]
struct DelegationToolInput {
    operation: String,
    #[serde(default)]
    objective: Option<String>,
    #[serde(default)]
    profile_id: Option<String>,
    #[serde(default)]
    template_id: Option<String>,
    #[serde(default)]
    parent_run_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    task_id: Option<String>,
    #[serde(default)]
    run_id: Option<String>,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    priority: Option<i64>,
    #[serde(default)]
    budget_tokens: Option<u64>,
    #[serde(default)]
    max_attempts: Option<u64>,
    #[serde(default)]
    execution_mode: Option<DelegationExecutionMode>,
    #[serde(default)]
    group_id: Option<String>,
    #[serde(default)]
    model_profile: Option<String>,
    #[serde(default)]
    memory_scope: Option<DelegationMemoryScopeKind>,
    #[serde(default)]
    tool_allowlist: Vec<String>,
    #[serde(default)]
    skill_allowlist: Vec<String>,
    #[serde(default)]
    approval_required: Option<bool>,
    #[serde(default)]
    max_concurrent_children: Option<u64>,
    #[serde(default)]
    max_children_per_parent: Option<u64>,
    #[serde(default)]
    max_total_children: Option<u64>,
    #[serde(default)]
    max_parallel_groups: Option<u64>,
    #[serde(default)]
    max_depth: Option<u64>,
    #[serde(default)]
    max_budget_share_bps: Option<u64>,
    #[serde(default)]
    child_timeout_ms: Option<u64>,
    #[serde(default)]
    include_completed: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct SessionsSpawnInput {
    task: String,
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    context_mode: Option<DelegationMemoryScopeKind>,
    #[serde(default)]
    priority: Option<i64>,
    #[serde(default)]
    return_mode: Option<SessionsSpawnReturnMode>,
    #[serde(default)]
    allowed_tools: Option<Vec<String>>,
    #[serde(default)]
    budget: Option<SessionsSpawnBudgetInput>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum SessionsSpawnReturnMode {
    IdsOnly,
    #[default]
    StatusRef,
    Ack,
}

impl SessionsSpawnReturnMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::IdsOnly => "ids_only",
            Self::StatusRef => "status_ref",
            Self::Ack => "ack",
        }
    }
}

#[derive(Debug, Default, Deserialize)]
struct SessionsSpawnBudgetInput {
    #[serde(default)]
    tokens: Option<u64>,
    #[serde(default)]
    max_attempts: Option<u64>,
    #[serde(default)]
    max_concurrent_children: Option<u64>,
    #[serde(default)]
    max_children_per_parent: Option<u64>,
    #[serde(default)]
    max_total_children: Option<u64>,
    #[serde(default)]
    max_parallel_groups: Option<u64>,
    #[serde(default)]
    max_depth: Option<u64>,
    #[serde(default)]
    max_budget_share_bps: Option<u64>,
    #[serde(default)]
    child_timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct SessionsYieldInput {
    #[serde(default)]
    child_run_ids: Vec<String>,
    #[serde(default)]
    task_ids: Vec<String>,
    #[serde(default)]
    timeout_ms: Option<u64>,
    #[serde(default)]
    return_mode: Option<SessionsYieldReturnMode>,
    #[serde(default)]
    partial_ok: Option<bool>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum SessionsYieldReturnMode {
    IdsOnly,
    #[default]
    Summary,
    Full,
}

impl SessionsYieldReturnMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::IdsOnly => "ids_only",
            Self::Summary => "summary",
            Self::Full => "full",
        }
    }
}

#[derive(Debug)]
struct DelegationSpawnRequest {
    objective: String,
    profile_id: Option<String>,
    template_id: Option<String>,
    group_id: Option<String>,
    execution_mode: Option<DelegationExecutionMode>,
    display_name: Option<String>,
    model_profile: Option<String>,
    memory_scope: Option<DelegationMemoryScopeKind>,
    tool_allowlist: Vec<String>,
    explicit_empty_tool_allowlist: bool,
    skill_allowlist: Vec<String>,
    approval_required: Option<bool>,
    budget_tokens: Option<u64>,
    max_attempts: Option<u64>,
    max_concurrent_children: Option<u64>,
    max_children_per_parent: Option<u64>,
    max_total_children: Option<u64>,
    max_parallel_groups: Option<u64>,
    max_depth: Option<u64>,
    max_budget_share_bps: Option<u64>,
    child_timeout_ms: Option<u64>,
    priority: i64,
    preallocated_child_run_id: Option<String>,
    payload_json: Option<String>,
}

/// Executes one `palyra.delegation.query` or `palyra.delegation.control` call.
///
/// Never fails at the call boundary: invalid input and runtime errors are
/// folded into an unsuccessful [`ToolExecutionOutcome`] whose output carries
/// a redacted `error` field.
pub(crate) async fn execute_delegation_tool(
    runtime: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    tool_name: &str,
    proposal_id: &str,
    input_json: &[u8],
) -> ToolExecutionOutcome {
    let result = execute_delegation_tool_inner(runtime, context, tool_name, input_json).await;
    match result {
        Ok(output) => {
            build_outcome(proposal_id, tool_name, input_json, true, output, String::new())
        }
        Err(error) => build_outcome(
            proposal_id,
            tool_name,
            input_json,
            false,
            json!({ "error": safe_text(error.message()) }),
            error.message().to_owned(),
        ),
    }
}

async fn execute_delegation_tool_inner(
    runtime: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    tool_name: &str,
    input_json: &[u8],
) -> Result<Value, Status> {
    if tool_name == SESSIONS_SPAWN_TOOL_NAME {
        let input = serde_json::from_slice::<SessionsSpawnInput>(input_json).map_err(|error| {
            Status::invalid_argument(format!("sessions_spawn input is invalid JSON: {error}"))
        })?;
        return create_sessions_spawn(runtime, context, &input).await;
    }
    if tool_name == SESSIONS_YIELD_TOOL_NAME {
        let input = serde_json::from_slice::<SessionsYieldInput>(input_json).map_err(|error| {
            Status::invalid_argument(format!("sessions_yield input is invalid JSON: {error}"))
        })?;
        return create_sessions_yield(runtime, context, &input).await;
    }

    let input = serde_json::from_slice::<DelegationToolInput>(input_json).map_err(|error| {
        Status::invalid_argument(format!("delegation tool input is invalid JSON: {error}"))
    })?;
    let operation = input.operation.trim().to_ascii_lowercase();
    match tool_name {
        DELEGATION_QUERY_TOOL_NAME => match operation.as_str() {
            "list" => list_delegations(runtime, context, &input).await,
            "status" => delegation_status(runtime, context, &input).await,
            "merge_preview" => delegation_merge_preview(runtime, context, &input).await,
            _ => Err(Status::invalid_argument(
                "palyra.delegation.query operation must be one of list|status|merge_preview",
            )),
        },
        DELEGATION_CONTROL_TOOL_NAME => match operation.as_str() {
            "delegate" => create_delegation(runtime, context, &input).await,
            "interrupt" => interrupt_delegation(runtime, context, &input).await,
            _ => Err(Status::invalid_argument(
                "palyra.delegation.control operation must be one of delegate|interrupt",
            )),
        },
        _ => Err(Status::invalid_argument("unsupported delegation tool name")),
    }
}

async fn create_delegation(
    runtime: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    input: &DelegationToolInput,
) -> Result<Value, Status> {
    let objective = normalize_required(input.objective.as_deref(), "objective")?;
    let task = create_delegation_background_task(
        runtime,
        context,
        DelegationSpawnRequest {
            objective,
            profile_id: normalize_optional(input.profile_id.as_deref()),
            template_id: normalize_optional(input.template_id.as_deref()),
            group_id: normalize_optional(input.group_id.as_deref()),
            execution_mode: input.execution_mode,
            display_name: None,
            model_profile: normalize_optional(input.model_profile.as_deref()),
            memory_scope: input.memory_scope,
            tool_allowlist: input.tool_allowlist.clone(),
            explicit_empty_tool_allowlist: false,
            skill_allowlist: input.skill_allowlist.clone(),
            approval_required: input.approval_required,
            budget_tokens: input.budget_tokens,
            max_attempts: input.max_attempts,
            max_concurrent_children: input.max_concurrent_children,
            max_children_per_parent: input.max_children_per_parent,
            max_total_children: input.max_total_children,
            max_parallel_groups: input.max_parallel_groups,
            max_depth: input.max_depth,
            max_budget_share_bps: input.max_budget_share_bps,
            child_timeout_ms: input.child_timeout_ms,
            priority: input.priority.unwrap_or(0).clamp(-10, 10),
            preallocated_child_run_id: None,
            payload_json: None,
        },
    )
    .await?;

    Ok(json!({
        "schema_version": 1,
        "operation": "delegate",
        "created": true,
        "task": task_safe_json(&task),
        "progress_ref": {
            "task_id": task.task_id,
            "parent_run_id": task.parent_run_id,
            "child_run_id": task.target_run_id,
            "state": task.state,
        },
    }))
}

async fn create_sessions_spawn(
    runtime: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    input: &SessionsSpawnInput,
) -> Result<Value, Status> {
    let child_run_id = Ulid::new().to_string();
    let return_mode = input.return_mode.unwrap_or_default();
    let request = sessions_spawn_delegation_spawn_request(input, child_run_id.clone())?;
    let task = create_delegation_background_task(runtime, context, request).await?;
    Ok(sessions_spawn_response(&task, return_mode, child_run_id.as_str()))
}

fn sessions_spawn_delegation_spawn_request(
    input: &SessionsSpawnInput,
    child_run_id: String,
) -> Result<DelegationSpawnRequest, Status> {
    let objective = normalize_required(Some(input.task.as_str()), "task")?;
    let allowed_tools =
        input.allowed_tools.as_ref().map(|values| normalize_tool_list(values)).unwrap_or_default();
    let explicit_empty_tool_allowlist =
        input.allowed_tools.as_ref().is_some_and(|_| allowed_tools.is_empty());
    let budget = input.budget.as_ref();

    Ok(DelegationSpawnRequest {
        objective,
        profile_id: None,
        template_id: None,
        group_id: None,
        execution_mode: None,
        display_name: normalize_optional(input.label.as_deref()),
        model_profile: None,
        memory_scope: input.context_mode,
        tool_allowlist: allowed_tools,
        explicit_empty_tool_allowlist,
        skill_allowlist: Vec::new(),
        approval_required: None,
        budget_tokens: budget.and_then(|value| value.tokens),
        max_attempts: budget.and_then(|value| value.max_attempts),
        max_concurrent_children: budget.and_then(|value| value.max_concurrent_children),
        max_children_per_parent: budget.and_then(|value| value.max_children_per_parent),
        max_total_children: budget.and_then(|value| value.max_total_children),
        max_parallel_groups: budget.and_then(|value| value.max_parallel_groups),
        max_depth: budget.and_then(|value| value.max_depth),
        max_budget_share_bps: budget.and_then(|value| value.max_budget_share_bps),
        child_timeout_ms: budget.and_then(|value| value.child_timeout_ms),
        priority: input.priority.unwrap_or(0).clamp(-10, 10),
        preallocated_child_run_id: Some(child_run_id),
        payload_json: Some(json!({"source_tool":"sessions_spawn"}).to_string()),
    })
}

async fn create_delegation_background_task(
    runtime: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    request: DelegationSpawnRequest,
) -> Result<OrchestratorBackgroundTaskRecord, Status> {
    let parent_run_id = context.run_id.to_owned();
    // The delegation resolver enforces per-child budget-share limits against
    // the parent budget, but tool input does not carry the real parent
    // budget. Synthesize one: double the requested child budget (keeps the
    // child's share at 50%), falling back to the objective's estimated token
    // count when no budget was requested.
    let parent_budget_tokens =
        request.budget_tokens.map(|budget| budget.saturating_mul(2).max(1)).or_else(|| {
            Some(crate::orchestrator::estimate_token_count(request.objective.as_str()).max(1))
        });
    let resolved_agent = runtime
        .resolve_agent_for_context(AgentResolveRequest {
            principal: context.principal.to_owned(),
            channel: context.channel.map(ToOwned::to_owned),
            session_id: Some(context.session_id.to_owned()),
            preferred_agent_id: None,
            persist_session_binding: false,
        })
        .await?;
    let delegation_request = delegation_request_for_spawn(&request);
    let mut delegation = resolve_delegation_request(
        &delegation_request,
        &DelegationParentContext {
            parent_run_id: Some(parent_run_id.clone()),
            agent_id: Some(resolved_agent.agent.agent_id.clone()),
            parent_model_profile: normalize_optional(Some(
                resolved_agent.agent.default_model_profile.as_str(),
            )),
            parent_tool_allowlist: parent_tool_allowlist_for_spawn_resolution(
                &request,
                resolved_agent.agent.default_tool_allowlist.as_slice(),
            ),
            parent_skill_allowlist: resolved_agent.agent.default_skill_allowlist.clone(),
            parent_budget_tokens,
        },
    )?;
    if request.explicit_empty_tool_allowlist {
        delegation.tool_allowlist.clear();
    }

    runtime
        .create_orchestrator_background_task(OrchestratorBackgroundTaskCreateRequest {
            task_id: Ulid::new().to_string(),
            task_kind: AuxiliaryTaskKind::DelegationPrompt.as_str().to_owned(),
            session_id: context.session_id.to_owned(),
            parent_run_id: Some(parent_run_id),
            target_run_id: request.preallocated_child_run_id,
            queued_input_id: None,
            owner_principal: context.principal.to_owned(),
            device_id: context.device_id.to_owned(),
            channel: context.channel.map(ToOwned::to_owned),
            state: AuxiliaryTaskState::Queued.as_str().to_owned(),
            priority: request.priority,
            max_attempts: delegation.max_attempts,
            budget_tokens: delegation.budget_tokens,
            delegation: Some(delegation),
            not_before_unix_ms: None,
            expires_at_unix_ms: None,
            notification_target_json: None,
            input_text: Some(request.objective),
            payload_json: request.payload_json,
        })
        .await
}

fn delegation_request_for_spawn(request: &DelegationSpawnRequest) -> DelegationRequestInput {
    DelegationRequestInput {
        profile_id: request.profile_id.clone(),
        template_id: request.template_id.clone(),
        group_id: request.group_id.clone(),
        execution_mode: request.execution_mode,
        manifest: Some(DelegationManifestInput {
            display_name: request.display_name.clone(),
            model_profile: request.model_profile.clone(),
            tool_allowlist: request.tool_allowlist.clone(),
            skill_allowlist: request.skill_allowlist.clone(),
            memory_scope: request.memory_scope,
            budget_tokens: request.budget_tokens,
            max_attempts: request.max_attempts,
            approval_required: request.approval_required,
            max_concurrent_children: request.max_concurrent_children,
            max_children_per_parent: request.max_children_per_parent,
            max_total_children: request.max_total_children,
            max_parallel_groups: request.max_parallel_groups,
            max_depth: request.max_depth,
            max_budget_share_bps: request.max_budget_share_bps,
            child_timeout_ms: request.child_timeout_ms,
            ..Default::default()
        }),
    }
}

fn parent_tool_allowlist_for_spawn_resolution(
    request: &DelegationSpawnRequest,
    parent_tool_allowlist: &[String],
) -> Vec<String> {
    if request.explicit_empty_tool_allowlist {
        return Vec::new();
    }
    parent_tool_allowlist.to_vec()
}

fn sessions_spawn_response(
    task: &OrchestratorBackgroundTaskRecord,
    return_mode: SessionsSpawnReturnMode,
    child_run_id: &str,
) -> Value {
    json!({
        "schema_version": 1,
        "operation": "sessions_spawn",
        "spawned": true,
        "task_id": task.task_id,
        "parent_run_id": task.parent_run_id,
        "child_run_id": child_run_id,
        "child_session_id": task.session_id,
        "state": task.state,
        "return_mode": return_mode.as_str(),
        "transcript_ref": {
            "kind": "orchestrator_run_tape",
            "status": "pending",
            "run_id": child_run_id,
            "session_id": task.session_id,
        },
        "progress_ref": {
            "task_id": task.task_id,
            "parent_run_id": task.parent_run_id,
            "child_run_id": child_run_id,
            "state": task.state,
        },
    })
}

async fn create_sessions_yield(
    runtime: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    input: &SessionsYieldInput,
) -> Result<Value, Status> {
    let timeout_ms = input.timeout_ms.unwrap_or(0).min(MAX_SESSIONS_YIELD_TIMEOUT_MS);
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let output = sessions_yield_snapshot(runtime, context, input, timeout_ms).await?;
        let complete = output.get("complete").and_then(Value::as_bool).unwrap_or(false);
        if complete {
            return Ok(output);
        }
        if timeout_ms == 0 || Instant::now() >= deadline {
            if !input.partial_ok.unwrap_or(true) {
                return Err(Status::deadline_exceeded(
                    "sessions_yield timed out before all selected child runs completed",
                ));
            }
            return Ok(output);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        tokio::time::sleep(remaining.min(Duration::from_millis(SESSIONS_YIELD_POLL_INTERVAL_MS)))
            .await;
    }
}

async fn sessions_yield_snapshot(
    runtime: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    input: &SessionsYieldInput,
    timeout_ms: u64,
) -> Result<Value, Status> {
    let child_run_ids = normalize_id_list(input.child_run_ids.as_slice());
    let task_ids = normalize_id_list(input.task_ids.as_slice());
    let requested_child_run_ids = child_run_ids.iter().cloned().collect::<BTreeSet<_>>();
    let requested_task_ids = task_ids.iter().cloned().collect::<BTreeSet<_>>();
    let requested_specific_children =
        !requested_child_run_ids.is_empty() || !requested_task_ids.is_empty();
    let filter = DelegationToolInput {
        operation: "list".to_owned(),
        objective: None,
        profile_id: None,
        template_id: None,
        parent_run_id: None,
        session_id: Some(context.session_id.to_owned()),
        task_id: None,
        run_id: None,
        reason: None,
        priority: None,
        budget_tokens: None,
        max_attempts: None,
        execution_mode: None,
        group_id: None,
        model_profile: None,
        memory_scope: None,
        tool_allowlist: Vec::new(),
        skill_allowlist: Vec::new(),
        approval_required: None,
        max_concurrent_children: None,
        max_children_per_parent: None,
        max_total_children: None,
        max_parallel_groups: None,
        max_depth: None,
        max_budget_share_bps: None,
        child_timeout_ms: None,
        include_completed: Some(true),
    };
    let tasks = scoped_delegation_tasks(runtime, context, &filter, true).await?;
    let return_mode = input.return_mode.unwrap_or_default();
    let mut completions = Vec::new();
    let mut pending = Vec::new();
    let mut matched_child_run_ids = BTreeSet::new();
    let mut matched_task_ids = BTreeSet::new();

    for task in tasks {
        if !sessions_yield_selects_task(
            &task,
            context.run_id,
            requested_specific_children,
            &requested_child_run_ids,
            &requested_task_ids,
        ) {
            continue;
        }
        matched_task_ids.insert(task.task_id.clone());
        if let Some(child_run_id) = task.target_run_id.as_ref() {
            matched_child_run_ids.insert(child_run_id.clone());
        }
        let run = if let Some(run_id) = task.target_run_id.as_ref() {
            runtime.orchestrator_run_status_snapshot(run_id.clone()).await?
        } else {
            None
        };
        let terminal = sessions_yield_task_terminal(&task, run.as_ref());
        let projected = sessions_yield_task_projection(&task, run.as_ref(), return_mode, terminal);
        if terminal {
            completions.push(projected);
        } else {
            pending.push(projected);
        }
    }

    for child_run_id in requested_child_run_ids.difference(&matched_child_run_ids) {
        pending.push(sessions_yield_missing_child_json(Some(child_run_id.as_str()), None));
    }
    for task_id in requested_task_ids.difference(&matched_task_ids) {
        pending.push(sessions_yield_missing_child_json(None, Some(task_id.as_str())));
    }

    let complete = pending.is_empty();
    Ok(json!({
        "schema_version": 1,
        "operation": "sessions_yield",
        "complete": complete,
        "partial": !complete && !completions.is_empty(),
        "timeout_ms": timeout_ms,
        "return_mode": return_mode.as_str(),
        "partial_ok": input.partial_ok.unwrap_or(true),
        "completions": completions,
        "pending": pending,
        "idempotency_keys": completions
            .iter()
            .filter_map(|completion| completion.get("idempotency_key").and_then(Value::as_str))
            .collect::<Vec<_>>(),
    }))
}

fn sessions_yield_selects_task(
    task: &OrchestratorBackgroundTaskRecord,
    parent_run_id: &str,
    requested_specific_children: bool,
    requested_child_run_ids: &BTreeSet<String>,
    requested_task_ids: &BTreeSet<String>,
) -> bool {
    if requested_specific_children {
        return requested_task_ids.contains(task.task_id.as_str())
            || task
                .target_run_id
                .as_ref()
                .is_some_and(|child_run_id| requested_child_run_ids.contains(child_run_id));
    }
    task.parent_run_id.as_deref() == Some(parent_run_id)
}

fn sessions_yield_task_terminal(
    task: &OrchestratorBackgroundTaskRecord,
    run: Option<&crate::journal::OrchestratorRunStatusSnapshot>,
) -> bool {
    AuxiliaryTaskState::from_str(task.state.as_str()).is_some_and(AuxiliaryTaskState::is_terminal)
        || run.is_some_and(|snapshot| sessions_yield_terminal_run_state(snapshot.state.as_str()))
}

fn sessions_yield_terminal_run_state(state: &str) -> bool {
    matches!(state, "done" | "failed" | "cancelled" | "canceled" | "timed_out" | "rejected")
}

fn sessions_yield_task_projection(
    task: &OrchestratorBackgroundTaskRecord,
    run: Option<&crate::journal::OrchestratorRunStatusSnapshot>,
    return_mode: SessionsYieldReturnMode,
    terminal: bool,
) -> Value {
    let child_run_id =
        task.target_run_id.as_deref().or_else(|| run.map(|snapshot| snapshot.run_id.as_str()));
    let child_state = run.map(|snapshot| snapshot.state.as_str()).unwrap_or(task.state.as_str());
    let idempotency_key = sessions_yield_idempotency_key(task, child_run_id, child_state, run);
    let mut output = json!({
        "task_id": task.task_id,
        "child_run_id": child_run_id,
        "child_session_id": task.session_id,
        "state": child_state,
        "terminal": terminal,
        "idempotency_key": idempotency_key,
        "transcript_ref": child_run_id.map(|run_id| json!({
            "kind": "orchestrator_run_tape",
            "status": if terminal { "complete" } else { "pending" },
            "run_id": run_id,
            "session_id": task.session_id,
        })),
    });
    if return_mode == SessionsYieldReturnMode::IdsOnly {
        return output;
    }

    let merge_preview = task_merge_preview(task, run);
    if let Some(object) = output.as_object_mut() {
        object
            .insert("summary".to_owned(), json!(sessions_yield_summary(task, run, &merge_preview)));
        object.insert(
            "artifact_refs".to_owned(),
            merge_preview.get("changed_artifacts").cloned().unwrap_or_else(|| json!([])),
        );
        object.insert(
            "evidence_refs".to_owned(),
            merge_preview.get("evidence_refs").cloned().unwrap_or_else(|| json!([])),
        );
        object.insert(
            "verification_state".to_owned(),
            json!(sessions_yield_verification_state(child_state, terminal, &merge_preview)),
        );
        if return_mode == SessionsYieldReturnMode::Full {
            object.insert("merge_preview".to_owned(), merge_preview);
            object.insert("task".to_owned(), task_safe_json(task));
            object.insert("child_run".to_owned(), run.map(run_safe_json).unwrap_or(Value::Null));
        }
    }
    output
}

fn sessions_yield_summary(
    task: &OrchestratorBackgroundTaskRecord,
    run: Option<&crate::journal::OrchestratorRunStatusSnapshot>,
    merge_preview: &Value,
) -> String {
    if let Some(summary) = merge_preview.get("summary").and_then(Value::as_str) {
        return summary.to_owned();
    }
    run.and_then(|snapshot| snapshot.last_error.as_deref())
        .or(task.last_error.as_deref())
        .map(safe_text)
        .unwrap_or_else(|| "child run has not produced a merge summary yet".to_owned())
}

fn sessions_yield_verification_state(
    child_state: &str,
    terminal: bool,
    merge_preview: &Value,
) -> &'static str {
    if !terminal {
        return "pending";
    }
    if matches!(child_state, "failed" | "cancelled" | "canceled" | "timed_out" | "rejected") {
        return "failed";
    }
    if merge_preview.get("approval_required").and_then(Value::as_bool).unwrap_or(false) {
        return "review_required";
    }
    if merge_preview.get("ready").and_then(Value::as_bool).unwrap_or(false) {
        return "verified";
    }
    "completion_recorded"
}

fn sessions_yield_idempotency_key(
    task: &OrchestratorBackgroundTaskRecord,
    child_run_id: Option<&str>,
    child_state: &str,
    run: Option<&crate::journal::OrchestratorRunStatusSnapshot>,
) -> String {
    let completed_at = task
        .completed_at_unix_ms
        .or_else(|| run.and_then(|snapshot| snapshot.completed_at_unix_ms))
        .unwrap_or(task.updated_at_unix_ms);
    format!(
        "subagent_completion:{}:{}:{}:{}",
        task.task_id,
        child_run_id.unwrap_or("pending"),
        child_state,
        completed_at
    )
}

fn sessions_yield_missing_child_json(child_run_id: Option<&str>, task_id: Option<&str>) -> Value {
    json!({
        "task_id": task_id,
        "child_run_id": child_run_id,
        "state": "not_found",
        "terminal": false,
        "transcript_ref": Value::Null,
    })
}

async fn list_delegations(
    runtime: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    input: &DelegationToolInput,
) -> Result<Value, Status> {
    let tasks =
        scoped_delegation_tasks(runtime, context, input, input.include_completed.unwrap_or(false))
            .await?;
    let parent_run_id = normalize_optional(input.parent_run_id.as_deref());
    let tasks = tasks
        .into_iter()
        .filter(|task| {
            parent_run_id
                .as_deref()
                .is_none_or(|run_id| task.parent_run_id.as_deref() == Some(run_id))
        })
        .map(|task| task_safe_json(&task))
        .collect::<Vec<_>>();
    Ok(json!({
        "schema_version": 1,
        "operation": "list",
        "tasks": tasks,
    }))
}

async fn delegation_status(
    runtime: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    input: &DelegationToolInput,
) -> Result<Value, Status> {
    let task = find_scoped_delegation_task(runtime, context, input).await?;
    let run = if let Some(run_id) = task.target_run_id.as_deref().or(input.run_id.as_deref()) {
        runtime.orchestrator_run_status_snapshot(run_id.to_owned()).await?
    } else {
        None
    };
    Ok(json!({
        "schema_version": 1,
        "operation": "status",
        "task": task_safe_json(&task),
        "child_run": run.as_ref().map(run_safe_json),
    }))
}

async fn delegation_merge_preview(
    runtime: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    input: &DelegationToolInput,
) -> Result<Value, Status> {
    let task = find_scoped_delegation_task(runtime, context, input).await?;
    let run = if let Some(run_id) = task.target_run_id.as_deref().or(input.run_id.as_deref()) {
        runtime.orchestrator_run_status_snapshot(run_id.to_owned()).await?
    } else {
        None
    };
    let preview = task_merge_preview(&task, run.as_ref());
    Ok(json!({
        "schema_version": 1,
        "operation": "merge_preview",
        "task": task_safe_json(&task),
        "child_run": run.as_ref().map(run_safe_json),
        "merge_preview": preview,
    }))
}

async fn interrupt_delegation(
    runtime: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    input: &DelegationToolInput,
) -> Result<Value, Status> {
    let task = find_scoped_delegation_task(runtime, context, input).await?;
    let reason = normalize_optional(input.reason.as_deref())
        .unwrap_or_else(|| "delegated run interrupted by scoped command".to_owned());
    let child_run_id =
        task.target_run_id.clone().or_else(|| normalize_optional(input.run_id.as_deref()));
    let cancel = if let Some(run_id) = child_run_id.clone() {
        Some(
            runtime
                .request_orchestrator_cancel(OrchestratorCancelRequest {
                    run_id,
                    reason: reason.clone(),
                })
                .await?,
        )
    } else {
        None
    };
    // The orchestrator cancel only reaches tasks with a live child run; tasks
    // that never started or are parked in a non-running state are finalized
    // directly so the background queue cannot pick them up later.
    if child_run_id.is_none()
        || matches!(
            AuxiliaryTaskState::from_str(task.state.as_str()),
            Some(
                AuxiliaryTaskState::Queued
                    | AuxiliaryTaskState::Paused
                    | AuxiliaryTaskState::Failed
            )
        )
    {
        runtime
            .update_orchestrator_background_task(OrchestratorBackgroundTaskUpdateRequest {
                task_id: task.task_id.clone(),
                state: Some(AuxiliaryTaskState::Cancelled.as_str().to_owned()),
                last_error: Some(Some(reason.clone())),
                completed_at_unix_ms: Some(Some(current_unix_ms())),
                ..Default::default()
            })
            .await?;
    }
    let refreshed = runtime.get_orchestrator_background_task(task.task_id.clone()).await?;
    Ok(json!({
        "schema_version": 1,
        "operation": "interrupt",
        // Without a child run the task was finalized directly above, so the
        // interrupt is reported as effective.
        "cancel_requested": cancel.as_ref().is_none_or(|value| value.cancel_requested),
        "reason": safe_text(reason.as_str()),
        "task": refreshed.as_ref().map(task_safe_json).unwrap_or_else(|| task_safe_json(&task)),
        "child_run": cancel.map(|value| json!({
            "run_id": value.run_id,
            "cancel_requested": value.cancel_requested,
            "reason": safe_text(value.reason.as_str()),
        })),
    }))
}

async fn scoped_delegation_tasks(
    runtime: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    input: &DelegationToolInput,
    include_completed: bool,
) -> Result<Vec<OrchestratorBackgroundTaskRecord>, Status> {
    let tasks = runtime
        .list_orchestrator_background_tasks(OrchestratorBackgroundTaskListFilter {
            owner_principal: Some(context.principal.to_owned()),
            device_id: Some(context.device_id.to_owned()),
            channel: context.channel.map(ToOwned::to_owned),
            session_id: normalize_optional(input.session_id.as_deref())
                .or_else(|| Some(context.session_id.to_owned())),
            include_completed,
            limit: MAX_DELEGATION_TOOL_TASKS,
        })
        .await?;
    // The background-task store holds every auxiliary task kind; only
    // delegation-backed tasks are visible through these tools.
    Ok(tasks.into_iter().filter(|task| task.delegation.is_some()).collect())
}

async fn find_scoped_delegation_task(
    runtime: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    input: &DelegationToolInput,
) -> Result<OrchestratorBackgroundTaskRecord, Status> {
    let tasks = scoped_delegation_tasks(runtime, context, input, true).await?;
    let task_id = normalize_optional(input.task_id.as_deref());
    let run_id = normalize_optional(input.run_id.as_deref());
    tasks
        .into_iter()
        .find(|task| {
            task_id.as_deref().is_some_and(|value| task.task_id == value)
                || run_id
                    .as_deref()
                    .is_some_and(|value| task.target_run_id.as_deref() == Some(value))
        })
        .ok_or_else(|| Status::not_found("delegated task not found in scoped runtime"))
}

/// Projects a background task into the model-visible shape: allowlisted
/// fields only, with free-text fields passed through redaction.
fn task_safe_json(task: &OrchestratorBackgroundTaskRecord) -> Value {
    let delegation = task.delegation.as_ref().map(|snapshot| {
        json!({
            "profile_id": snapshot.profile_id,
            "display_name": safe_text(snapshot.display_name.as_str()),
            "template_id": snapshot.template_id,
            "role": snapshot.role,
            "execution_mode": snapshot.execution_mode,
            "group_id": snapshot.group_id,
            "model_profile": snapshot.model_profile,
            "tool_allowlist": snapshot.tool_allowlist,
            "skill_allowlist": snapshot.skill_allowlist,
            "memory_scope": snapshot.memory_scope,
            "budget_tokens": snapshot.budget_tokens,
            "max_attempts": snapshot.max_attempts,
            "merge_contract": snapshot.merge_contract,
            "runtime_limits": snapshot.runtime_limits,
            "agent_id": snapshot.agent_id,
        })
    });
    json!({
        "task_id": task.task_id,
        "task_kind": task.task_kind,
        "session_id": task.session_id,
        "parent_run_id": task.parent_run_id,
        "child_run_id": task.target_run_id,
        "state": task.state,
        "priority": task.priority,
        "attempt_count": task.attempt_count,
        "budget_tokens": task.budget_tokens,
        "delegation": delegation,
        "objective": task.input_text.as_deref().map(safe_text),
        "last_error": task.last_error.as_deref().map(safe_text),
        "created_at_unix_ms": task.created_at_unix_ms,
        "updated_at_unix_ms": task.updated_at_unix_ms,
        "started_at_unix_ms": task.started_at_unix_ms,
        "completed_at_unix_ms": task.completed_at_unix_ms,
    })
}

/// Projects a child run snapshot into the model-visible shape with redacted
/// free-text fields.
fn run_safe_json(run: &crate::journal::OrchestratorRunStatusSnapshot) -> Value {
    json!({
        "run_id": run.run_id,
        "session_id": run.session_id,
        "state": run.state,
        "cancel_requested": run.cancel_requested,
        "cancel_reason": run.cancel_reason.as_deref().map(safe_text),
        "parent_run_id": run.parent_run_id,
        "total_tokens": run.total_tokens,
        "last_error": run.last_error.as_deref().map(safe_text),
        "updated_at_unix_ms": run.updated_at_unix_ms,
        "completed_at_unix_ms": run.completed_at_unix_ms,
    })
}

fn task_merge_preview(
    task: &OrchestratorBackgroundTaskRecord,
    run: Option<&crate::journal::OrchestratorRunStatusSnapshot>,
) -> Value {
    let result_json = task.result_json.as_deref().and_then(parse_json_object);
    // Prefer the live run snapshot; fall back to the archived task result for
    // runs whose orchestrator state has already been pruned.
    let merge_result = run
        .and_then(|snapshot| snapshot.merge_result.as_ref())
        .and_then(|merge| serde_json::to_value(merge).ok())
        .or_else(|| result_json.as_ref().and_then(|value| value.get("merge_result").cloned()));
    let Some(merge_result) = merge_result else {
        return json!({
            "ready": false,
            "reason": "merge preview is not available until the child run reaches a merge checkpoint",
        });
    };
    json!({
        "ready": true,
        "summary": merge_result
            .get("summary_text")
            .and_then(Value::as_str)
            .map(safe_text)
            .unwrap_or_else(|| "no summary".to_owned()),
        "evidence_refs": merge_result.get("provenance").cloned().unwrap_or_else(|| json!([])),
        "changed_artifacts": merge_result
            .get("artifact_references")
            .cloned()
            .unwrap_or_else(|| json!([])),
        "sensitivity": if merge_result
            .get("approval_required")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            "review_required"
        } else {
            "internal"
        },
        "approval_required": merge_result
            .get("approval_required")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        "warnings": merge_result.get("warnings").cloned().unwrap_or_else(|| json!([])),
    })
}

fn parse_json_object(value: &str) -> Option<Value> {
    serde_json::from_str::<Value>(value).ok().filter(Value::is_object)
}

fn normalize_required(value: Option<&str>, field: &str) -> Result<String, Status> {
    normalize_optional(value)
        .ok_or_else(|| Status::invalid_argument(format!("{field} is required")))
}

fn normalize_optional(value: Option<&str>) -> Option<String> {
    value.map(str::trim).filter(|value| !value.is_empty()).map(ToOwned::to_owned)
}

fn normalize_tool_list(values: &[String]) -> Vec<String> {
    let mut normalized = values
        .iter()
        .filter_map(|value| {
            let value = normalize_optional(Some(value.as_str()))?;
            Some(value.to_ascii_lowercase())
        })
        .collect::<Vec<_>>();
    normalized.sort();
    normalized.dedup();
    normalized
}

fn normalize_id_list(values: &[String]) -> Vec<String> {
    let mut normalized = values
        .iter()
        .filter_map(|value| normalize_optional(Some(value.as_str())))
        .collect::<Vec<_>>();
    normalized.sort();
    normalized.dedup();
    normalized
}

/// Applies the model-visible redaction chain to free-form text.
fn safe_text(value: &str) -> String {
    redact_url_segments_in_text(&redact_auth_error(value))
}

fn build_outcome(
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    success: bool,
    output: Value,
    error: String,
) -> ToolExecutionOutcome {
    let output_json = serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec());
    build_tool_execution_outcome(
        proposal_id,
        tool_name,
        input_json,
        success,
        output_json,
        error,
        false,
        DELEGATION_TOOL_EXECUTOR.to_owned(),
        DELEGATION_TOOL_SANDBOX.to_owned(),
    )
}

#[cfg(test)]
mod tests {
    use super::{
        delegation_request_for_spawn, parent_tool_allowlist_for_spawn_resolution,
        sessions_spawn_delegation_spawn_request, sessions_spawn_response,
        sessions_yield_idempotency_key, sessions_yield_missing_child_json,
        sessions_yield_selects_task, sessions_yield_task_projection,
        sessions_yield_terminal_run_state, task_merge_preview, task_safe_json,
        SessionsSpawnBudgetInput, SessionsSpawnInput, SessionsSpawnReturnMode,
        SessionsYieldReturnMode,
    };
    use crate::{
        delegation::{
            resolve_delegation_request, DelegationExecutionMode, DelegationMemoryScopeKind,
            DelegationMergeContract, DelegationMergeStrategy, DelegationParentContext,
            DelegationRole, DelegationRuntimeLimits, DelegationSnapshot,
        },
        journal::OrchestratorBackgroundTaskRecord,
    };
    use palyra_common::runtime_contracts::AuxiliaryTaskState;
    use std::collections::BTreeSet;

    #[test]
    fn task_safe_json_redacts_objective_and_projects_scope() {
        let task = sample_task();
        let value = task_safe_json(&task);

        assert_eq!(value["task_id"], "task-1");
        assert_eq!(value["delegation"]["profile_id"], "research");
        assert!(!value["objective"]
            .as_str()
            .expect("objective should be present")
            .contains("secret"));
    }

    #[test]
    fn merge_preview_reports_review_required_summary() {
        let mut task = sample_task();
        task.result_json = Some(
            serde_json::json!({
                "merge_result": {
                    "summary_text": "Patch changed https://example.com/callback?access_token=secret",
                    "approval_required": true,
                    "provenance": [{"kind":"artifact","label":"diff"}],
                    "artifact_references": [{"artifact_id":"a1"}],
                    "warnings": ["requires review"]
                }
            })
            .to_string(),
        );

        let preview = task_merge_preview(&task, None);

        assert_eq!(preview["ready"], true);
        assert_eq!(preview["sensitivity"], "review_required");
        assert!(!preview["summary"]
            .as_str()
            .expect("summary should be present")
            .contains("secret"));
    }

    #[test]
    fn sessions_spawn_request_preserves_explicit_empty_allowed_tools() {
        let input = SessionsSpawnInput {
            task: "Inspect the pending handoff".to_owned(),
            label: None,
            context_mode: Some(DelegationMemoryScopeKind::ParentSession),
            priority: Some(4),
            return_mode: None,
            allowed_tools: Some(Vec::new()),
            budget: None,
        };

        let request = sessions_spawn_delegation_spawn_request(&input, "child-run".to_owned())
            .expect("sessions_spawn request should normalize");
        assert!(request.explicit_empty_tool_allowlist);
        assert!(request.tool_allowlist.is_empty());
        assert_eq!(request.priority, 4);
        assert_eq!(request.preallocated_child_run_id.as_deref(), Some("child-run"));

        let mut snapshot = resolve_delegation_request(
            &delegation_request_for_spawn(&request),
            &test_parent_context(Some(2_000)),
        )
        .expect("base delegation should resolve");
        assert!(
            !snapshot.tool_allowlist.is_empty(),
            "the profile default would grant tools unless the explicit empty list is applied"
        );
        if request.explicit_empty_tool_allowlist {
            snapshot.tool_allowlist.clear();
        }
        assert!(snapshot.tool_allowlist.is_empty());

        let parent_resolution_allowlist = parent_tool_allowlist_for_spawn_resolution(
            &request,
            &["sessions_spawn".to_owned(), "palyra.echo".to_owned()],
        );
        assert!(
            parent_resolution_allowlist.is_empty(),
            "explicit child no-tools requests must not fail on profile default tools"
        );
    }

    #[test]
    fn sessions_spawn_budget_share_is_denied_by_delegation_resolver() {
        let input = SessionsSpawnInput {
            task: "Summarize the large report".to_owned(),
            label: None,
            context_mode: None,
            priority: None,
            return_mode: None,
            allowed_tools: None,
            budget: Some(SessionsSpawnBudgetInput {
                tokens: Some(1_300),
                max_budget_share_bps: Some(5_000),
                ..Default::default()
            }),
        };

        let request = sessions_spawn_delegation_spawn_request(&input, "child-run".to_owned())
            .expect("sessions_spawn request should normalize");
        let error = resolve_delegation_request(
            &delegation_request_for_spawn(&request),
            &test_parent_context(Some(2_000)),
        )
        .expect_err("child budget above parent share should be denied");

        assert!(error.message().contains("configured parent budget share"));
    }

    #[test]
    fn sessions_spawn_response_does_not_echo_task_text() {
        let mut task = sample_task();
        task.input_text =
            Some("Read https://example.com/callback?access_token=secret and summarize".to_owned());

        let response =
            sessions_spawn_response(&task, SessionsSpawnReturnMode::StatusRef, "child-run");
        let response_text = response.to_string();

        assert_eq!(response["operation"], "sessions_spawn");
        assert_eq!(response["child_run_id"], "child-run");
        assert_eq!(response["child_session_id"], "session-1");
        assert!(!response_text.contains("access_token"));
        assert!(!response_text.contains("secret"));
        assert!(!response_text.contains("Read https://example.com"));
    }

    #[test]
    fn sessions_spawn_return_modes_use_stable_wire_labels() {
        assert_eq!(SessionsSpawnReturnMode::IdsOnly.as_str(), "ids_only");
        assert_eq!(SessionsSpawnReturnMode::StatusRef.as_str(), "status_ref");
        assert_eq!(SessionsSpawnReturnMode::Ack.as_str(), "ack");
    }

    #[test]
    fn sessions_yield_projection_returns_completion_contract() {
        let mut task = sample_task();
        task.state = AuxiliaryTaskState::Succeeded.as_str().to_owned();
        task.completed_at_unix_ms = Some(20);
        task.result_json = Some(
            serde_json::json!({
                "merge_result": {
                    "summary_text": "Investigated https://example.com/callback?access_token=secret",
                    "approval_required": false,
                    "provenance": [{"kind":"message","label":"child summary","child_run_id":"child-run","requires_approval":false}],
                    "artifact_references": [{"artifact_id":"artifact-1","artifact_kind":"report","label":"summary"}],
                    "warnings": []
                }
            })
            .to_string(),
        );

        let value =
            sessions_yield_task_projection(&task, None, SessionsYieldReturnMode::Summary, true);
        let text = value.to_string();

        assert_eq!(value["task_id"], "task-1");
        assert_eq!(value["child_run_id"], "child-run");
        assert_eq!(value["child_session_id"], "session-1");
        assert_eq!(value["terminal"], true);
        assert_eq!(value["verification_state"], "verified");
        assert_eq!(value["transcript_ref"]["run_id"], "child-run");
        assert_eq!(value["artifact_refs"][0]["artifact_id"], "artifact-1");
        assert!(value["idempotency_key"]
            .as_str()
            .expect("idempotency key should be present")
            .starts_with("subagent_completion:task-1:child-run"));
        assert!(!text.contains("secret"));
        assert!(!text.contains("access_token=secret"));
    }

    #[test]
    fn sessions_yield_selects_requested_or_parent_children() {
        let task = sample_task();
        let requested_runs = ["child-run".to_owned()].into_iter().collect();
        let requested_tasks = BTreeSet::new();

        assert!(sessions_yield_selects_task(
            &task,
            "other-parent",
            true,
            &requested_runs,
            &requested_tasks
        ));
        assert!(sessions_yield_selects_task(
            &task,
            "parent-run",
            false,
            &BTreeSet::new(),
            &BTreeSet::new()
        ));
        assert!(!sessions_yield_selects_task(
            &task,
            "other-parent",
            false,
            &BTreeSet::new(),
            &BTreeSet::new()
        ));
    }

    #[test]
    fn sessions_yield_missing_and_timeout_helpers_are_stable() {
        assert!(sessions_yield_terminal_run_state("done"));
        assert!(sessions_yield_terminal_run_state("failed"));
        assert!(!sessions_yield_terminal_run_state("running"));
        assert_eq!(SessionsYieldReturnMode::IdsOnly.as_str(), "ids_only");
        assert_eq!(SessionsYieldReturnMode::Summary.as_str(), "summary");
        assert_eq!(SessionsYieldReturnMode::Full.as_str(), "full");

        let missing = sessions_yield_missing_child_json(Some("child-missing"), None);
        assert_eq!(missing["child_run_id"], "child-missing");
        assert_eq!(missing["state"], "not_found");
        assert_eq!(missing["terminal"], false);

        let task = sample_task();
        assert_eq!(
            sessions_yield_idempotency_key(&task, Some("child-run"), "queued", None),
            "subagent_completion:task-1:child-run:queued:1"
        );
    }

    fn test_parent_context(parent_budget_tokens: Option<u64>) -> DelegationParentContext {
        DelegationParentContext {
            parent_run_id: Some("parent-run".to_owned()),
            agent_id: Some("main".to_owned()),
            parent_model_profile: Some("deterministic".to_owned()),
            parent_tool_allowlist: Vec::new(),
            parent_skill_allowlist: Vec::new(),
            parent_budget_tokens,
        }
    }

    fn sample_task() -> OrchestratorBackgroundTaskRecord {
        OrchestratorBackgroundTaskRecord {
            task_id: "task-1".to_owned(),
            task_kind: "delegation_prompt".to_owned(),
            session_id: "session-1".to_owned(),
            parent_run_id: Some("parent-run".to_owned()),
            target_run_id: Some("child-run".to_owned()),
            queued_input_id: None,
            owner_principal: "principal".to_owned(),
            device_id: "device".to_owned(),
            channel: Some("web".to_owned()),
            state: AuxiliaryTaskState::Queued.as_str().to_owned(),
            priority: 0,
            attempt_count: 0,
            max_attempts: 3,
            budget_tokens: 1_000,
            delegation: Some(DelegationSnapshot {
                profile_id: "research".to_owned(),
                display_name: "Research".to_owned(),
                description: None,
                template_id: None,
                role: DelegationRole::Research,
                execution_mode: DelegationExecutionMode::Parallel,
                group_id: "default".to_owned(),
                model_profile: "deterministic".to_owned(),
                tool_allowlist: vec!["palyra.http.fetch".to_owned()],
                skill_allowlist: Vec::new(),
                memory_scope: DelegationMemoryScopeKind::ParentSession,
                budget_tokens: 1_000,
                max_attempts: 3,
                merge_contract: DelegationMergeContract {
                    strategy: DelegationMergeStrategy::Summarize,
                    approval_required: false,
                },
                runtime_limits: DelegationRuntimeLimits::default(),
                agent_id: Some("main".to_owned()),
            }),
            not_before_unix_ms: None,
            expires_at_unix_ms: None,
            notification_target_json: None,
            input_text: Some("Read https://example.com/callback?access_token=secret".to_owned()),
            payload_json: None,
            last_error: None,
            result_json: None,
            created_at_unix_ms: 1,
            updated_at_unix_ms: 1,
            started_at_unix_ms: None,
            completed_at_unix_ms: None,
        }
    }
}
