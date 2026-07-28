//! Runtime implementation for model-visible agent plan management.

use std::sync::Arc;

use serde::Serialize;
use serde_json::{json, Map, Value};

use crate::{
    application::plan_state::{
        AgentPlanCreateCommand, AgentPlanItem, AgentPlanQuery, AgentPlanStatus, AgentPlanStore,
        AgentPlanUpdateCommand, AGENT_PLAN_SCHEMA_VERSION,
    },
    gateway::{GatewayRuntimeState, ToolRuntimeExecutionContext, PLAN_MANAGE_TOOL_NAME},
    journal::AgentPlanToolInvocationRequest,
    tool_protocol::{build_tool_execution_outcome, ToolExecutionOutcome},
};

const PLAN_MANAGE_EXECUTOR: &str = "agent_plan_runtime";
const PLAN_MANAGE_SANDBOX: &str = "none";
const PLAN_MANAGE_SCHEMA_VERSION: u64 = 1;
const MAX_PLAN_MANAGE_TOOL_INPUT_BYTES: usize = 64 * 1024;
const MAX_PLAN_MANAGE_ITEMS: usize = 20;
const MAX_PLAN_MANAGE_ACTIVE_ITEMS: usize = 50;
const MAX_PLAN_TITLE_CHARS: usize = 160;
const MAX_PLAN_DETAILS_BYTES: usize = 8 * 1024;
const MAX_PLAN_BLOCKED_REASON_CHARS: usize = 512;
const MAX_PLAN_REASON_CODE_CHARS: usize = 96;
const MAX_PLAN_EVIDENCE_REFS: usize = 16;
const MAX_PLAN_EVIDENCE_REF_CHARS: usize = 256;

/// Executes one `palyra.plan.manage` call against journal-backed plan state.
///
/// Parse, quota, rollout, ownership, and storage failures are returned as
/// attested unsuccessful tool outcomes instead of panicking across the tool
/// boundary.
pub(crate) async fn execute_plan_manage_tool(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    proposal_id: &str,
    input_json: &[u8],
) -> ToolExecutionOutcome {
    if input_json.len() > MAX_PLAN_MANAGE_TOOL_INPUT_BYTES {
        return plan_manage_outcome(
            proposal_id,
            input_json,
            false,
            error_output(
                "plan_manage_input_too_large",
                format!(
                    "{PLAN_MANAGE_TOOL_NAME} input exceeds {MAX_PLAN_MANAGE_TOOL_INPUT_BYTES} bytes"
                ),
            ),
            format!(
                "{PLAN_MANAGE_TOOL_NAME} input exceeds {MAX_PLAN_MANAGE_TOOL_INPUT_BYTES} bytes"
            ),
        );
    }

    let request = match parse_plan_manage_request(input_json) {
        Ok(request) => request,
        Err(error) => {
            return plan_manage_outcome(
                proposal_id,
                input_json,
                false,
                error_output("plan_manage_invalid_input", error.clone()),
                error,
            );
        }
    };
    let owned_context = PlanManageContext {
        principal: context.principal.to_owned(),
        device_id: context.device_id.to_owned(),
        channel: context.channel.map(str::to_owned),
        session_id: context.session_id.to_owned(),
        run_id: context.run_id.to_owned(),
    };
    let state = Arc::clone(runtime_state);
    let result = tokio::task::spawn_blocking(move || {
        execute_plan_manage_blocking(&state, owned_context, request)
    })
    .await;

    match result {
        Ok(Ok(output)) => {
            let success = output.ok;
            let error = if success {
                String::new()
            } else {
                output
                    .rejected_items
                    .first()
                    .map(|rejection| rejection.message.clone())
                    .unwrap_or_else(|| "plan manage operation was rejected".to_owned())
            };
            match serde_json::to_vec(&output) {
                Ok(output_json) => {
                    plan_manage_outcome(proposal_id, input_json, success, output_json, error)
                }
                Err(error) => plan_manage_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    format!("failed to encode plan manage output: {error}"),
                ),
            }
        }
        Ok(Err(error)) => plan_manage_outcome(
            proposal_id,
            input_json,
            false,
            error_output("plan_manage_runtime_error", error.clone()),
            error,
        ),
        Err(_) => plan_manage_outcome(
            proposal_id,
            input_json,
            false,
            error_output(
                "plan_manage_runtime_worker_panicked",
                "plan manage worker panicked".to_owned(),
            ),
            "plan manage worker panicked".to_owned(),
        ),
    }
}

fn execute_plan_manage_blocking(
    runtime_state: &GatewayRuntimeState,
    context: PlanManageContext,
    mut request: PlanManageRequest,
) -> Result<PlanManageOutput, String> {
    request.actor_principal = context.principal.clone();
    let rollout_enabled = runtime_state.config.feature_rollouts.agent_plan_state.enabled;
    let effective_enabled = rollout_enabled
        || runtime_state
            .journal_store
            .has_active_v2_complex_plan_for_session(context.session_id.as_str())
            .map_err(|error| format!("failed to inspect effective plan state: {error}"))?;
    let store = AgentPlanStore::new(&runtime_state.journal_store);
    let mut output = PlanManageOutput::new(request.operation, effective_enabled);

    if request.operation.is_mutating() && !effective_enabled {
        output.rejected_items.push(PlanManageRejection {
            item_id: None,
            reason_code: "agent_plan_state_rollout_disabled".to_owned(),
            message: "agent plan state rollout is disabled for mutating plan operations".to_owned(),
        });
    } else {
        apply_plan_manage_operation(&store, &context, &request, &mut output);
    }

    output.active_items = list_plan_items(
        &store,
        &context,
        request.operation == PlanManageOperation::Read && request.include_terminal,
        request.limit,
    )?;
    output.ok = output.rejected_items.is_empty();
    output.audit_event_recorded =
        append_plan_manage_invocation(runtime_state, &context, &request, &output);
    if !output.audit_event_recorded {
        output.ok = false;
        output.rejected_items.push(PlanManageRejection {
            item_id: None,
            reason_code: "agent_plan_tool_invocation_audit_failed".to_owned(),
            message: "failed to append agent.plan.tool_invoked audit event".to_owned(),
        });
    }
    Ok(output)
}

fn apply_plan_manage_operation(
    store: &AgentPlanStore<'_>,
    context: &PlanManageContext,
    request: &PlanManageRequest,
    output: &mut PlanManageOutput,
) {
    match request.operation {
        PlanManageOperation::Read => {}
        PlanManageOperation::Upsert => {
            for item in &request.items {
                upsert_plan_item(store, context, request, item, output);
            }
        }
        PlanManageOperation::Reorder => {
            for item in &request.items {
                update_existing_plan_item(store, context, request, item, output, ReorderPatch);
            }
        }
        PlanManageOperation::Block => {
            for item in &request.items {
                update_existing_plan_item(store, context, request, item, output, BlockPatch);
            }
        }
        PlanManageOperation::Complete => {
            for item in &request.items {
                update_existing_plan_item(store, context, request, item, output, CompletePatch);
            }
        }
        PlanManageOperation::Cancel => {
            for item in &request.items {
                update_existing_plan_item(store, context, request, item, output, CancelPatch);
            }
        }
        PlanManageOperation::ClearActive => {
            let active = match list_plan_items(store, context, false, request.limit) {
                Ok(active) => active,
                Err(error) => {
                    output.rejected_items.push(PlanManageRejection {
                        item_id: None,
                        reason_code: "agent_plan_list_failed".to_owned(),
                        message: error,
                    });
                    return;
                }
            };
            for item in active {
                let patch = PlanItemInput {
                    item_id: Some(item.plan_item_id),
                    blocked_reason: Some("cleared by palyra.plan.manage".to_owned()),
                    ..PlanItemInput::default()
                };
                update_existing_plan_item(
                    store,
                    context,
                    request,
                    &patch,
                    output,
                    ClearActivePatch,
                );
            }
        }
    }
}

fn upsert_plan_item(
    store: &AgentPlanStore<'_>,
    context: &PlanManageContext,
    request: &PlanManageRequest,
    item: &PlanItemInput,
    output: &mut PlanManageOutput,
) {
    let Some(item_id) = item.item_id.as_deref() else {
        create_plan_item(store, context, request, item, output);
        return;
    };

    match store.get_item(item_id) {
        Ok(Some(existing)) => {
            if let Err(message) = ensure_item_access(&existing, context) {
                output.rejected_items.push(PlanManageRejection {
                    item_id: Some(item_id.to_owned()),
                    reason_code: "agent_plan_item_scope_mismatch".to_owned(),
                    message,
                });
                return;
            }
            if existing.status.is_terminal() {
                output.rejected_items.push(PlanManageRejection {
                    item_id: Some(item_id.to_owned()),
                    reason_code: "agent_plan_item_terminal".to_owned(),
                    message: "terminal agent plan items cannot be updated".to_owned(),
                });
                return;
            }
            let command =
                update_command_from_input(item_id, request, item, "agent plan item updated");
            match store.update_item(command) {
                Ok(updated) => output.changed_items.push(updated),
                Err(error) => output.rejected_items.push(PlanManageRejection {
                    item_id: Some(item_id.to_owned()),
                    reason_code: "agent_plan_update_failed".to_owned(),
                    message: error.to_string(),
                }),
            }
        }
        Ok(None) => create_plan_item(store, context, request, item, output),
        Err(error) => output.rejected_items.push(PlanManageRejection {
            item_id: Some(item_id.to_owned()),
            reason_code: "agent_plan_lookup_failed".to_owned(),
            message: error.to_string(),
        }),
    }
}

fn create_plan_item(
    store: &AgentPlanStore<'_>,
    context: &PlanManageContext,
    request: &PlanManageRequest,
    item: &PlanItemInput,
    output: &mut PlanManageOutput,
) {
    let Some(title) = item.title.clone() else {
        output.rejected_items.push(PlanManageRejection {
            item_id: item.item_id.clone(),
            reason_code: "agent_plan_title_required".to_owned(),
            message: "upsert create requires title".to_owned(),
        });
        return;
    };
    let status = item.status.unwrap_or(AgentPlanStatus::InProgress);
    if status.is_terminal() {
        output.rejected_items.push(PlanManageRejection {
            item_id: item.item_id.clone(),
            reason_code: "agent_plan_terminal_create_rejected".to_owned(),
            message: "upsert create cannot create terminal plan items".to_owned(),
        });
        return;
    }
    let command = AgentPlanCreateCommand {
        plan_item_id: item.item_id.clone(),
        session_id: context.session_id.clone(),
        run_id: Some(context.run_id.clone()),
        parent_run_id: None,
        owner_principal: context.principal.clone(),
        device_id: context.device_id.clone(),
        channel: context.channel.clone(),
        title,
        details: item.details.clone().unwrap_or_else(|| json!({})),
        status,
        priority: item.priority.unwrap_or(0),
        blocked_reason: item.blocked_reason.clone(),
        evidence_refs: item.evidence_refs.clone().unwrap_or_else(|| json!([])),
        reason_code: request.reason_code.clone(),
        actor_principal: context.principal.clone(),
        payload: tool_event_payload(request.operation, item.item_id.as_deref()),
    };
    match store.create_item(command) {
        Ok(created) => output.changed_items.push(created),
        Err(error) => output.rejected_items.push(PlanManageRejection {
            item_id: item.item_id.clone(),
            reason_code: "agent_plan_create_failed".to_owned(),
            message: error.to_string(),
        }),
    }
}

trait PlanPatch {
    fn update_command(
        &self,
        item_id: &str,
        request: &PlanManageRequest,
        input: &PlanItemInput,
    ) -> Result<AgentPlanUpdateCommand, PlanManageRejection>;
}

#[derive(Clone, Copy)]
struct ReorderPatch;
#[derive(Clone, Copy)]
struct BlockPatch;
#[derive(Clone, Copy)]
struct CompletePatch;
#[derive(Clone, Copy)]
struct CancelPatch;
#[derive(Clone, Copy)]
struct ClearActivePatch;

impl PlanPatch for ReorderPatch {
    fn update_command(
        &self,
        item_id: &str,
        request: &PlanManageRequest,
        input: &PlanItemInput,
    ) -> Result<AgentPlanUpdateCommand, PlanManageRejection> {
        if input.priority.is_none() {
            return Err(rejection(
                Some(item_id),
                "agent_plan_priority_required",
                "reorder requires priority",
            ));
        }
        let mut command = base_update_command(item_id, request, "agent plan item reordered");
        command.priority = input.priority;
        Ok(command)
    }
}

impl PlanPatch for BlockPatch {
    fn update_command(
        &self,
        item_id: &str,
        request: &PlanManageRequest,
        input: &PlanItemInput,
    ) -> Result<AgentPlanUpdateCommand, PlanManageRejection> {
        let Some(blocked_reason) = input.blocked_reason.clone() else {
            return Err(rejection(
                Some(item_id),
                "agent_plan_blocked_reason_required",
                "block requires blocked_reason",
            ));
        };
        let mut command = base_update_command(item_id, request, "agent plan item blocked");
        command.status = Some(AgentPlanStatus::Blocked);
        command.blocked_reason = Some(Some(blocked_reason));
        Ok(command)
    }
}

impl PlanPatch for CompletePatch {
    fn update_command(
        &self,
        item_id: &str,
        request: &PlanManageRequest,
        _input: &PlanItemInput,
    ) -> Result<AgentPlanUpdateCommand, PlanManageRejection> {
        let mut command = base_update_command(item_id, request, "agent plan item completed");
        command.status = Some(AgentPlanStatus::Completed);
        Ok(command)
    }
}

impl PlanPatch for CancelPatch {
    fn update_command(
        &self,
        item_id: &str,
        request: &PlanManageRequest,
        input: &PlanItemInput,
    ) -> Result<AgentPlanUpdateCommand, PlanManageRejection> {
        let mut command = base_update_command(item_id, request, "agent plan item cancelled");
        command.status = Some(AgentPlanStatus::Cancelled);
        command.blocked_reason = input.blocked_reason.clone().map(Some);
        Ok(command)
    }
}

impl PlanPatch for ClearActivePatch {
    fn update_command(
        &self,
        item_id: &str,
        request: &PlanManageRequest,
        input: &PlanItemInput,
    ) -> Result<AgentPlanUpdateCommand, PlanManageRejection> {
        let mut command = base_update_command(item_id, request, "agent plan item cleared");
        command.status = Some(AgentPlanStatus::Cancelled);
        command.blocked_reason = input.blocked_reason.clone().map(Some);
        Ok(command)
    }
}

fn update_existing_plan_item<P: PlanPatch>(
    store: &AgentPlanStore<'_>,
    context: &PlanManageContext,
    request: &PlanManageRequest,
    input: &PlanItemInput,
    output: &mut PlanManageOutput,
    patch: P,
) {
    let Some(item_id) = input.item_id.as_deref() else {
        output.rejected_items.push(rejection(
            None,
            "agent_plan_item_id_required",
            "operation requires item_id",
        ));
        return;
    };
    let existing = match store.get_item(item_id) {
        Ok(Some(existing)) => existing,
        Ok(None) => {
            output.rejected_items.push(rejection(
                Some(item_id),
                "agent_plan_item_not_found",
                "agent plan item not found",
            ));
            return;
        }
        Err(error) => {
            output.rejected_items.push(PlanManageRejection {
                item_id: Some(item_id.to_owned()),
                reason_code: "agent_plan_lookup_failed".to_owned(),
                message: error.to_string(),
            });
            return;
        }
    };
    if let Err(message) = ensure_item_access(&existing, context) {
        output.rejected_items.push(PlanManageRejection {
            item_id: Some(item_id.to_owned()),
            reason_code: "agent_plan_item_scope_mismatch".to_owned(),
            message,
        });
        return;
    }
    if existing.status.is_terminal() {
        output.rejected_items.push(PlanManageRejection {
            item_id: Some(item_id.to_owned()),
            reason_code: "agent_plan_item_terminal".to_owned(),
            message: "terminal agent plan items cannot be updated".to_owned(),
        });
        return;
    }
    let command = match patch.update_command(item_id, request, input) {
        Ok(command) => command,
        Err(rejection) => {
            output.rejected_items.push(rejection);
            return;
        }
    };
    match store.update_item(command) {
        Ok(updated) => output.changed_items.push(updated),
        Err(error) => output.rejected_items.push(PlanManageRejection {
            item_id: Some(item_id.to_owned()),
            reason_code: "agent_plan_update_failed".to_owned(),
            message: error.to_string(),
        }),
    }
}

fn update_command_from_input(
    item_id: &str,
    request: &PlanManageRequest,
    input: &PlanItemInput,
    summary: &str,
) -> AgentPlanUpdateCommand {
    let mut command = base_update_command(item_id, request, summary);
    command.status = input.status;
    command.title = input.title.clone();
    command.details = input.details.clone();
    command.priority = input.priority;
    command.blocked_reason = if input.clear_blocked_reason {
        Some(None)
    } else {
        input.blocked_reason.clone().map(Some)
    };
    command.evidence_refs = input.evidence_refs.clone();
    command
}

fn base_update_command(
    item_id: &str,
    request: &PlanManageRequest,
    summary: &str,
) -> AgentPlanUpdateCommand {
    AgentPlanUpdateCommand {
        plan_item_id: item_id.to_owned(),
        reason_code: request.reason_code.clone(),
        actor_principal: request.actor_principal.clone(),
        summary: summary.to_owned(),
        payload: tool_event_payload(request.operation, Some(item_id)),
        ..AgentPlanUpdateCommand::default()
    }
}

fn list_plan_items(
    store: &AgentPlanStore<'_>,
    context: &PlanManageContext,
    include_terminal: bool,
    limit: usize,
) -> Result<Vec<AgentPlanItem>, String> {
    store
        .list_items(&AgentPlanQuery {
            owner_principal: Some(context.principal.clone()),
            device_id: Some(context.device_id.clone()),
            channel: context.channel.clone(),
            session_id: Some(context.session_id.clone()),
            run_id: None,
            status: None,
            include_terminal,
            limit,
        })
        .map_err(|error| error.to_string())
}

fn ensure_item_access(item: &AgentPlanItem, context: &PlanManageContext) -> Result<(), String> {
    if item.owner_principal != context.principal
        || item.device_id != context.device_id
        || item.channel != context.channel
        || item.session_id != context.session_id
    {
        return Err(
            "agent plan item is outside the current principal/device/channel/session scope"
                .to_owned(),
        );
    }
    Ok(())
}

fn append_plan_manage_invocation(
    runtime_state: &GatewayRuntimeState,
    context: &PlanManageContext,
    request: &PlanManageRequest,
    output: &PlanManageOutput,
) -> bool {
    let payload_json = match serde_json::to_string(&json!({
        "operation": request.operation,
        "changed_item_ids": output
            .changed_items
            .iter()
            .map(|item| item.plan_item_id.as_str())
            .collect::<Vec<_>>(),
        "rejected_items": &output.rejected_items,
    })) {
        Ok(payload_json) => payload_json,
        Err(_) => return false,
    };
    runtime_state
        .journal_store
        .append_agent_plan_tool_invocation(&AgentPlanToolInvocationRequest {
            run_id: context.run_id.clone(),
            session_id: context.session_id.clone(),
            operation: request.operation.as_str().to_owned(),
            actor_principal: context.principal.clone(),
            success: output.ok,
            changed_count: output.changed_items.len(),
            rejected_count: output.rejected_items.len(),
            reason_code: request.reason_code.clone(),
            payload_json,
        })
        .is_ok()
}

fn tool_event_payload(operation: PlanManageOperation, item_id: Option<&str>) -> Value {
    json!({
        "schema_version": PLAN_MANAGE_SCHEMA_VERSION,
        "tool_name": PLAN_MANAGE_TOOL_NAME,
        "operation": operation,
        "item_id": item_id,
    })
}

fn parse_plan_manage_request(input_json: &[u8]) -> Result<PlanManageRequest, String> {
    let value = serde_json::from_slice::<Value>(input_json)
        .map_err(|error| format!("plan manage input must be valid JSON: {error}"))?;
    let object =
        value.as_object().ok_or_else(|| "plan manage input must be a JSON object".to_owned())?;
    let operation = PlanManageOperation::parse(required_string(object, "operation")?)?;
    let include_terminal = optional_bool(object, "include_terminal")?.unwrap_or(false);
    let limit = optional_usize(object, "limit")?
        .unwrap_or(MAX_PLAN_MANAGE_ACTIVE_ITEMS)
        .clamp(1, MAX_PLAN_MANAGE_ACTIVE_ITEMS);
    let reason_code = optional_limited_string(object, "reason_code", MAX_PLAN_REASON_CODE_CHARS)?
        .unwrap_or_else(|| format!("plan_manage_{}", operation.as_str()));
    let items = parse_plan_items(operation, object)?;
    Ok(PlanManageRequest {
        operation,
        include_terminal,
        limit,
        reason_code,
        actor_principal: String::new(),
        items,
    })
}

fn parse_plan_items(
    operation: PlanManageOperation,
    object: &Map<String, Value>,
) -> Result<Vec<PlanItemInput>, String> {
    if matches!(operation, PlanManageOperation::Read | PlanManageOperation::ClearActive) {
        return Ok(Vec::new());
    }
    let items = match object.get("items") {
        Some(Value::Array(items)) => {
            if items.is_empty() {
                return Err("items must not be empty for mutating plan operations".to_owned());
            }
            if items.len() > MAX_PLAN_MANAGE_ITEMS {
                return Err(format!(
                    "items exceeds the maximum of {MAX_PLAN_MANAGE_ITEMS} plan items"
                ));
            }
            items
                .iter()
                .map(|value| {
                    value
                        .as_object()
                        .ok_or_else(|| "each items entry must be a JSON object".to_owned())
                        .and_then(parse_plan_item)
                })
                .collect::<Result<Vec<_>, _>>()?
        }
        Some(_) => return Err("items must be an array".to_owned()),
        None => vec![parse_plan_item(object)?],
    };
    Ok(items)
}

fn parse_plan_item(object: &Map<String, Value>) -> Result<PlanItemInput, String> {
    let item_id = optional_limited_string(object, "item_id", 128)?;
    let title = optional_limited_string(object, "title", MAX_PLAN_TITLE_CHARS)?;
    let status = optional_status(object)?;
    let priority = optional_i64(object, "priority")?;
    if let Some(priority) = priority {
        if !(-100..=100).contains(&priority) {
            return Err("priority must be in -100..=100".to_owned());
        }
    }
    let details = optional_bounded_json(object, "details", MAX_PLAN_DETAILS_BYTES)?;
    let (blocked_reason, clear_blocked_reason) =
        optional_nullable_string(object, "blocked_reason", MAX_PLAN_BLOCKED_REASON_CHARS)?;
    let evidence_refs = optional_evidence_refs(object)?;
    Ok(PlanItemInput {
        item_id,
        title,
        details,
        status,
        priority,
        blocked_reason,
        clear_blocked_reason,
        evidence_refs,
    })
}

fn required_string<'a>(object: &'a Map<String, Value>, key: &str) -> Result<&'a str, String> {
    object
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("{key} is required and must be a non-empty string"))
}

fn optional_limited_string(
    object: &Map<String, Value>,
    key: &str,
    max_chars: usize,
) -> Result<Option<String>, String> {
    match object.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            if trimmed.chars().count() > max_chars {
                return Err(format!("{key} exceeds {max_chars} characters"));
            }
            Ok(Some(trimmed.to_owned()))
        }
        Some(_) => Err(format!("{key} must be a string")),
    }
}

fn optional_nullable_string(
    object: &Map<String, Value>,
    key: &str,
    max_chars: usize,
) -> Result<(Option<String>, bool), String> {
    match object.get(key) {
        Some(Value::Null) => Ok((None, true)),
        _ => optional_limited_string(object, key, max_chars).map(|value| (value, false)),
    }
}

fn optional_bool(object: &Map<String, Value>, key: &str) -> Result<Option<bool>, String> {
    match object.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Bool(value)) => Ok(Some(*value)),
        Some(_) => Err(format!("{key} must be a boolean")),
    }
}

fn optional_usize(object: &Map<String, Value>, key: &str) -> Result<Option<usize>, String> {
    match object.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(value)) => value
            .as_u64()
            .and_then(|value| usize::try_from(value).ok())
            .filter(|value| *value > 0)
            .map(Some)
            .ok_or_else(|| format!("{key} must be a positive integer")),
        Some(_) => Err(format!("{key} must be a positive integer")),
    }
}

fn optional_i64(object: &Map<String, Value>, key: &str) -> Result<Option<i64>, String> {
    match object.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(value)) => {
            value.as_i64().map(Some).ok_or_else(|| format!("{key} must be an integer"))
        }
        Some(_) => Err(format!("{key} must be an integer")),
    }
}

fn optional_status(object: &Map<String, Value>) -> Result<Option<AgentPlanStatus>, String> {
    match object.get("status") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => {
            value.trim().parse::<AgentPlanStatus>().map(Some).map_err(|error| error.to_string())
        }
        Some(_) => Err("status must be a string".to_owned()),
    }
}

fn optional_bounded_json(
    object: &Map<String, Value>,
    key: &str,
    max_bytes: usize,
) -> Result<Option<Value>, String> {
    let Some(value) = object.get(key) else {
        return Ok(None);
    };
    let bytes = serde_json::to_vec(value)
        .map_err(|error| format!("{key} failed to encode as JSON: {error}"))?;
    if bytes.len() > max_bytes {
        return Err(format!("{key} exceeds {max_bytes} bytes"));
    }
    Ok(Some(value.clone()))
}

fn optional_evidence_refs(object: &Map<String, Value>) -> Result<Option<Value>, String> {
    let Some(value) = object.get("evidence_refs") else {
        return Ok(None);
    };
    let refs =
        value.as_array().ok_or_else(|| "evidence_refs must be an array of strings".to_owned())?;
    if refs.len() > MAX_PLAN_EVIDENCE_REFS {
        return Err(format!("evidence_refs exceeds {MAX_PLAN_EVIDENCE_REFS} items"));
    }
    for reference in refs {
        let Some(reference) = reference.as_str() else {
            return Err("evidence_refs entries must be strings".to_owned());
        };
        if reference.trim().is_empty() {
            return Err("evidence_refs entries must not be empty".to_owned());
        }
        if reference.chars().count() > MAX_PLAN_EVIDENCE_REF_CHARS {
            return Err(format!(
                "evidence_refs entries must not exceed {MAX_PLAN_EVIDENCE_REF_CHARS} characters"
            ));
        }
    }
    Ok(Some(value.clone()))
}

fn rejection(item_id: Option<&str>, reason_code: &str, message: &str) -> PlanManageRejection {
    PlanManageRejection {
        item_id: item_id.map(str::to_owned),
        reason_code: reason_code.to_owned(),
        message: message.to_owned(),
    }
}

fn error_output(reason_code: &str, message: String) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "schema_version": PLAN_MANAGE_SCHEMA_VERSION,
        "ok": false,
        "reason_code": reason_code,
        "message": message,
    }))
    .unwrap_or_else(|_| b"{}".to_vec())
}

fn plan_manage_outcome(
    proposal_id: &str,
    input_json: &[u8],
    success: bool,
    output_json: Vec<u8>,
    error: String,
) -> ToolExecutionOutcome {
    build_tool_execution_outcome(
        proposal_id,
        PLAN_MANAGE_TOOL_NAME,
        input_json,
        success,
        output_json,
        error,
        false,
        PLAN_MANAGE_EXECUTOR.to_owned(),
        PLAN_MANAGE_SANDBOX.to_owned(),
    )
}

#[derive(Debug, Clone)]
struct PlanManageContext {
    principal: String,
    device_id: String,
    channel: Option<String>,
    session_id: String,
    run_id: String,
}

#[derive(Debug, Clone)]
struct PlanManageRequest {
    operation: PlanManageOperation,
    include_terminal: bool,
    limit: usize,
    reason_code: String,
    actor_principal: String,
    items: Vec<PlanItemInput>,
}

#[derive(Debug, Clone, Default)]
struct PlanItemInput {
    item_id: Option<String>,
    title: Option<String>,
    details: Option<Value>,
    status: Option<AgentPlanStatus>,
    priority: Option<i64>,
    blocked_reason: Option<String>,
    clear_blocked_reason: bool,
    evidence_refs: Option<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum PlanManageOperation {
    Read,
    Upsert,
    Reorder,
    Block,
    Complete,
    Cancel,
    ClearActive,
}

impl PlanManageOperation {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "read" => Ok(Self::Read),
            "upsert" => Ok(Self::Upsert),
            "reorder" => Ok(Self::Reorder),
            "block" => Ok(Self::Block),
            "complete" => Ok(Self::Complete),
            "cancel" => Ok(Self::Cancel),
            "clear_active" => Ok(Self::ClearActive),
            other => Err(format!("unknown plan manage operation: {other}")),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Upsert => "upsert",
            Self::Reorder => "reorder",
            Self::Block => "block",
            Self::Complete => "complete",
            Self::Cancel => "cancel",
            Self::ClearActive => "clear_active",
        }
    }

    const fn is_mutating(self) -> bool {
        !matches!(self, Self::Read)
    }
}

#[derive(Debug, Serialize)]
struct PlanManageOutput {
    schema_version: u64,
    plan_schema_version: u64,
    ok: bool,
    operation: PlanManageOperation,
    rollout_enabled: bool,
    audit_event_recorded: bool,
    active_items: Vec<AgentPlanItem>,
    changed_items: Vec<AgentPlanItem>,
    rejected_items: Vec<PlanManageRejection>,
}

impl PlanManageOutput {
    fn new(operation: PlanManageOperation, rollout_enabled: bool) -> Self {
        Self {
            schema_version: PLAN_MANAGE_SCHEMA_VERSION,
            plan_schema_version: AGENT_PLAN_SCHEMA_VERSION,
            ok: true,
            operation,
            rollout_enabled,
            audit_event_recorded: false,
            active_items: Vec::new(),
            changed_items: Vec::new(),
            rejected_items: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct PlanManageRejection {
    #[serde(skip_serializing_if = "Option::is_none")]
    item_id: Option<String>,
    reason_code: String,
    message: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_manage_read_input_defaults_to_bounded_active_window() {
        let request =
            parse_plan_manage_request(br#"{"operation":"read"}"#).expect("read input should parse");

        assert_eq!(request.operation, PlanManageOperation::Read);
        assert_eq!(request.limit, MAX_PLAN_MANAGE_ACTIVE_ITEMS);
        assert!(request.items.is_empty());
    }

    #[test]
    fn plan_manage_rejects_too_many_items() {
        let items = (0..=MAX_PLAN_MANAGE_ITEMS)
            .map(|index| json!({"title": format!("item {index}")}))
            .collect::<Vec<_>>();
        let input = serde_json::to_vec(&json!({"operation":"upsert","items":items}))
            .expect("input should encode");

        let error = parse_plan_manage_request(input.as_slice())
            .expect_err("oversized batch should be rejected");
        assert!(error.contains("items exceeds"));
    }

    #[test]
    fn plan_manage_block_requires_blocked_reason() {
        let request = parse_plan_manage_request(
            br#"{"operation":"block","item_id":"01HZ0000000000000000000000"}"#,
        )
        .expect("input shape should parse");
        let mut output = PlanManageOutput::new(request.operation, true);
        let patch = BlockPatch;
        let result = patch.update_command(
            "01HZ0000000000000000000000",
            &request,
            request.items.first().expect("item should parse"),
        );

        output.rejected_items.push(result.expect_err("rejection"));
        assert_eq!(output.rejected_items[0].reason_code, "agent_plan_blocked_reason_required");
    }
}
