//! Tool-runtime executor for the scoped delegation tools.
//!
//! Implements `palyra.delegation.query` (list/status/merge_preview) and
//! `palyra.delegation.control` (delegate/interrupt) on top of orchestrator
//! background tasks. Dispatched from the gateway tool runtime
//! (`gateway::execute_tool_with_runtime_dispatch`); every free-text field is
//! redacted before it becomes model-visible output.

use std::sync::Arc;

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
        DELEGATION_CONTROL_TOOL_NAME, DELEGATION_QUERY_TOOL_NAME,
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
    let parent_run_id = context.run_id.to_owned();
    // The delegation resolver enforces per-child budget-share limits against
    // the parent budget, but tool input does not carry the real parent
    // budget. Synthesize one: double the requested child budget (keeps the
    // child's share at 50%), falling back to the objective's estimated token
    // count when no budget was requested.
    let parent_budget_tokens = input
        .budget_tokens
        .map(|budget| budget.saturating_mul(2).max(1))
        .or_else(|| Some(crate::orchestrator::estimate_token_count(objective.as_str()).max(1)));
    let resolved_agent = runtime
        .resolve_agent_for_context(AgentResolveRequest {
            principal: context.principal.to_owned(),
            channel: context.channel.map(ToOwned::to_owned),
            session_id: Some(context.session_id.to_owned()),
            preferred_agent_id: None,
            persist_session_binding: false,
        })
        .await?;
    let request = DelegationRequestInput {
        profile_id: normalize_optional(input.profile_id.as_deref()),
        template_id: normalize_optional(input.template_id.as_deref()),
        group_id: normalize_optional(input.group_id.as_deref()),
        execution_mode: input.execution_mode,
        manifest: Some(DelegationManifestInput {
            model_profile: normalize_optional(input.model_profile.as_deref()),
            tool_allowlist: input.tool_allowlist.clone(),
            skill_allowlist: input.skill_allowlist.clone(),
            memory_scope: input.memory_scope,
            budget_tokens: input.budget_tokens,
            max_attempts: input.max_attempts,
            approval_required: input.approval_required,
            max_concurrent_children: input.max_concurrent_children,
            max_children_per_parent: input.max_children_per_parent,
            max_total_children: input.max_total_children,
            max_parallel_groups: input.max_parallel_groups,
            max_depth: input.max_depth,
            max_budget_share_bps: input.max_budget_share_bps,
            child_timeout_ms: input.child_timeout_ms,
            ..Default::default()
        }),
    };
    let delegation = resolve_delegation_request(
        &request,
        &DelegationParentContext {
            parent_run_id: Some(parent_run_id.clone()),
            agent_id: Some(resolved_agent.agent.agent_id.clone()),
            parent_model_profile: normalize_optional(Some(
                resolved_agent.agent.default_model_profile.as_str(),
            )),
            parent_tool_allowlist: resolved_agent.agent.default_tool_allowlist.clone(),
            parent_skill_allowlist: resolved_agent.agent.default_skill_allowlist.clone(),
            parent_budget_tokens,
        },
    )?;
    let task = runtime
        .create_orchestrator_background_task(OrchestratorBackgroundTaskCreateRequest {
            task_id: Ulid::new().to_string(),
            task_kind: AuxiliaryTaskKind::DelegationPrompt.as_str().to_owned(),
            session_id: context.session_id.to_owned(),
            parent_run_id: Some(parent_run_id),
            target_run_id: None,
            queued_input_id: None,
            owner_principal: context.principal.to_owned(),
            device_id: context.device_id.to_owned(),
            channel: context.channel.map(ToOwned::to_owned),
            state: AuxiliaryTaskState::Queued.as_str().to_owned(),
            priority: input.priority.unwrap_or(0).clamp(-10, 10),
            max_attempts: delegation.max_attempts,
            budget_tokens: delegation.budget_tokens,
            delegation: Some(delegation),
            not_before_unix_ms: None,
            expires_at_unix_ms: None,
            notification_target_json: None,
            input_text: Some(objective),
            payload_json: None,
        })
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
    use super::{task_merge_preview, task_safe_json};
    use crate::{
        delegation::{
            DelegationExecutionMode, DelegationMemoryScopeKind, DelegationMergeContract,
            DelegationMergeStrategy, DelegationRole, DelegationRuntimeLimits, DelegationSnapshot,
        },
        journal::OrchestratorBackgroundTaskRecord,
    };
    use palyra_common::runtime_contracts::AuxiliaryTaskState;

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
