use std::{
    collections::BTreeSet,
    sync::Arc,
    time::{Duration, Instant},
};

use palyra_common::{
    redaction::{is_sensitive_key, redact_auth_error, redact_url_segments_in_text, REDACTED},
    runtime_contracts::{
        ArtifactRetentionPolicy, ToolResultArtifactRef, ToolResultSensitivity, ToolTurnBudget,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use ulid::Ulid;

use crate::{
    gateway::{
        current_unix_ms, GatewayRuntimeState, ToolRuntimeExecutionContext,
        TOOL_PROGRAM_RUN_TOOL_NAME,
    },
    journal::ToolResultArtifactCreateRequest,
    tool_protocol::{
        build_tool_execution_outcome, decide_tool_call, execute_tool_call, tool_metadata,
        ToolAttestation, ToolExecutionOutcome, ToolRequestContext,
    },
};

use super::process_registry::{
    BackgroundTaskRecord, BackgroundTaskRegistry, CleanupPolicy, ProcessRegistry,
    RuntimeProcessRecord, RuntimeProcessState,
};

const TOOL_PROGRAM_SCHEMA_VERSION: u32 = 1;
const MAX_PROGRAM_ID_BYTES: usize = 128;
const MAX_STEP_ID_BYTES: usize = 128;
const MAX_TOOL_PROGRAM_STEPS: usize = 32;

#[derive(Debug, Clone, Deserialize)]
struct ToolProgramRunRequest {
    schema_version: u32,
    program_id: String,
    #[serde(default)]
    budgets: ToolProgramBudgets,
    #[serde(default)]
    safety_policy: ToolProgramSafetyPolicy,
    steps: Vec<ToolProgramStep>,
}

#[derive(Debug, Clone, Deserialize)]
struct ToolProgramStep {
    step_id: String,
    tool: String,
    #[serde(default)]
    input: Value,
    #[serde(default)]
    scopes: Vec<String>,
    #[serde(default)]
    budget: ToolProgramStepBudget,
    #[serde(default)]
    allowed_artifact_refs: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ToolProgramBudgets {
    #[serde(default = "default_max_steps")]
    max_steps: usize,
    #[serde(default = "default_max_runtime_ms")]
    max_runtime_ms: u64,
    #[serde(default = "default_max_child_runs")]
    max_child_runs: usize,
    #[serde(default)]
    max_nested_approvals: usize,
    #[serde(default = "default_max_step_output_bytes")]
    max_step_output_bytes: usize,
    #[serde(default = "default_max_total_output_bytes")]
    max_total_output_bytes: usize,
}

impl Default for ToolProgramBudgets {
    fn default() -> Self {
        Self {
            max_steps: default_max_steps(),
            max_runtime_ms: default_max_runtime_ms(),
            max_child_runs: default_max_child_runs(),
            max_nested_approvals: 0,
            max_step_output_bytes: default_max_step_output_bytes(),
            max_total_output_bytes: default_max_total_output_bytes(),
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
struct ToolProgramStepBudget {
    #[serde(default)]
    max_output_bytes: Option<usize>,
    #[serde(default)]
    timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
struct ToolProgramSafetyPolicy {
    #[serde(default = "default_true")]
    deny_sensitive_tools_without_approval: bool,
    #[serde(default = "default_true")]
    stop_on_error: bool,
    #[serde(default)]
    allow_nested_programs: bool,
}

impl Default for ToolProgramSafetyPolicy {
    fn default() -> Self {
        Self {
            deny_sensitive_tools_without_approval: true,
            stop_on_error: true,
            allow_nested_programs: false,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct ToolProgramRunResponse {
    schema_version: u32,
    program_id: String,
    status: ToolProgramStatus,
    steps: Vec<ToolProgramStepResult>,
    child_attestations: Vec<ChildToolAttestation>,
    budget: ToolProgramBudgetReport,
    process_diagnostics: Vec<Value>,
    background_task_diagnostics: Vec<Value>,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ToolProgramStatus {
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize)]
struct ToolProgramStepResult {
    step_id: String,
    tool: String,
    status: ToolProgramStepStatus,
    success: bool,
    decision_reason: String,
    approval_required: bool,
    output: Value,
    error: String,
    artifact: Option<ToolResultArtifactRef>,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ToolProgramStepStatus {
    Completed,
    Denied,
    Failed,
    Cancelled,
    Spilled,
}

#[derive(Debug, Clone, Serialize)]
struct ChildToolAttestation {
    parent_program_id: String,
    parent_proposal_id: String,
    step_id: String,
    tool_name: String,
    attestation_id: String,
    execution_sha256: String,
    executor: String,
    sandbox_enforcement: String,
    timed_out: bool,
}

#[derive(Debug, Clone, Default, Serialize)]
struct ToolProgramBudgetReport {
    steps_used: usize,
    child_runs_used: usize,
    nested_approval_requests: usize,
    output_bytes_observed: usize,
    spilled_artifacts: u64,
    rejected_payloads: u64,
    saved_model_visible_bytes: u64,
}

pub(crate) async fn execute_tool_program_run_tool(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    proposal_id: &str,
    input_json: &[u8],
) -> ToolExecutionOutcome {
    let request = match parse_and_validate_request(input_json) {
        Ok(request) => request,
        Err(error) => {
            return tool_program_outcome(
                proposal_id,
                input_json,
                false,
                json!({ "schema_version": TOOL_PROGRAM_SCHEMA_VERSION, "error": error }),
                error,
                false,
            );
        }
    };

    match execute_validated_program(runtime_state, context, proposal_id, input_json, request).await
    {
        Ok((response, error)) => {
            let success = response.status == ToolProgramStatus::Completed;
            tool_program_outcome(proposal_id, input_json, success, json!(response), error, false)
        }
        Err(error) => tool_program_outcome(
            proposal_id,
            input_json,
            false,
            json!({ "schema_version": TOOL_PROGRAM_SCHEMA_VERSION, "error": error }),
            error,
            false,
        ),
    }
}

async fn execute_validated_program(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    proposal_id: &str,
    _input_json: &[u8],
    request: ToolProgramRunRequest,
) -> Result<(ToolProgramRunResponse, String), String> {
    let started_at = Instant::now();
    let mut budget = ToolProgramBudgetReport::default();
    let mut results = Vec::new();
    let mut child_attestations = Vec::new();
    let mut process_registry = ProcessRegistry::default();
    let mut background_registry = BackgroundTaskRegistry::default();
    let cleanup_policy = CleanupPolicy::tool_program_default();
    background_registry.register(BackgroundTaskRecord {
        task_id: format!("tool-program:{}", request.program_id),
        owner: context.run_id.to_owned(),
        purpose: "palyra.tool_program.run".to_owned(),
        started_at_unix_ms: current_unix_ms(),
        cancellation_handle: format!("cancel:{}", proposal_id),
        cleanup_policy: cleanup_policy.clone(),
        state: RuntimeProcessState::Running,
    })?;

    let mut final_status = ToolProgramStatus::Completed;
    let mut final_error = String::new();
    for step in &request.steps {
        if runtime_state
            .is_orchestrator_cancel_requested(context.run_id.to_owned())
            .await
            .map_err(|status| format!("cancellation check failed: {}", status.message()))?
        {
            final_status = ToolProgramStatus::Cancelled;
            final_error = "tool program cancelled before next step".to_owned();
            results.push(cancelled_step_result(step, final_error.as_str()));
            break;
        }
        if started_at.elapsed() > Duration::from_millis(request.budgets.max_runtime_ms) {
            final_status = ToolProgramStatus::Failed;
            final_error = format!(
                "tool program exceeded runtime budget max_runtime_ms={}",
                request.budgets.max_runtime_ms
            );
            results.push(failed_step_result(step, final_error.as_str()));
            break;
        }
        if budget.child_runs_used >= request.budgets.max_child_runs {
            final_status = ToolProgramStatus::Failed;
            final_error = "tool program child run budget exhausted".to_owned();
            results.push(failed_step_result(step, final_error.as_str()));
            break;
        }

        let process_id = format!("{}:{}", request.program_id, step.step_id);
        process_registry.register(RuntimeProcessRecord {
            process_id: process_id.clone(),
            owner: context.run_id.to_owned(),
            purpose: format!("tool-program-step:{}", step.tool),
            started_at_unix_ms: current_unix_ms(),
            cancellation_handle: format!("cancel:{proposal_id}:{}", step.step_id),
            cleanup_policy: cleanup_policy.clone(),
            state: RuntimeProcessState::Running,
        })?;

        let (step_result, child_attestation) =
            execute_program_step(runtime_state, context, proposal_id, &request, step, &mut budget)
                .await?;
        if let Some(attestation) = child_attestation {
            child_attestations.push(attestation);
        }
        if step_result.status == ToolProgramStepStatus::Cancelled {
            process_registry.cancel(process_id.as_str(), elapsed_millis(started_at))?;
        } else {
            process_registry.complete(process_id.as_str())?;
        }

        if !step_result.success {
            final_status = if step_result.status == ToolProgramStepStatus::Cancelled {
                ToolProgramStatus::Cancelled
            } else {
                ToolProgramStatus::Failed
            };
            final_error = step_result.error.clone();
            results.push(step_result);
            if request.safety_policy.stop_on_error {
                break;
            }
            continue;
        }
        results.push(step_result);
    }

    let _shutdown = process_registry.shutdown(elapsed_millis(started_at));
    let _ = background_registry.complete(format!("tool-program:{}", request.program_id).as_str());
    Ok((
        ToolProgramRunResponse {
            schema_version: TOOL_PROGRAM_SCHEMA_VERSION,
            program_id: request.program_id,
            status: final_status,
            steps: results,
            child_attestations,
            budget,
            process_diagnostics: process_registry
                .diagnostics(current_unix_ms())
                .into_iter()
                .map(|diagnostic| json!({ "id": diagnostic.id, "purpose": diagnostic.purpose }))
                .collect(),
            background_task_diagnostics: background_registry
                .diagnostics(current_unix_ms())
                .into_iter()
                .map(|diagnostic| json!({ "id": diagnostic.id, "purpose": diagnostic.purpose }))
                .collect(),
        },
        final_error,
    ))
}

async fn execute_program_step(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    proposal_id: &str,
    request: &ToolProgramRunRequest,
    step: &ToolProgramStep,
    budget: &mut ToolProgramBudgetReport,
) -> Result<(ToolProgramStepResult, Option<ChildToolAttestation>), String> {
    budget.steps_used += 1;
    if step.tool == TOOL_PROGRAM_RUN_TOOL_NAME && !request.safety_policy.allow_nested_programs {
        return Ok((
            denied_step_result(
                step,
                "nested tool programs require allow_nested_programs=true and explicit outer approval",
                true,
            ),
            None,
        ));
    }

    if step.budget.timeout_ms.is_some_and(|timeout_ms| timeout_ms > request.budgets.max_runtime_ms)
    {
        return Ok((
            denied_step_result(
                step,
                "step timeout_ms cannot exceed parent tool program max_runtime_ms",
                false,
            ),
            None,
        ));
    }

    if step.allowed_artifact_refs.iter().any(|artifact_ref| artifact_ref.len() > 512) {
        return Ok((
            denied_step_result(step, "allowed artifact refs must be bounded", false),
            None,
        ));
    }

    if step.scopes.iter().any(|scope| scope.len() > 128) {
        return Ok((denied_step_result(step, "step scopes must be bounded", false), None));
    }

    let input_bytes = serde_json::to_vec(&step.input)
        .map_err(|error| format!("failed to serialize tool program step input: {error}"))?;
    let mut remaining_budget = 1;
    let decision = decide_tool_call(
        &runtime_state.config.tool_call,
        &mut remaining_budget,
        &ToolRequestContext {
            principal: context.principal.to_owned(),
            device_id: Some(context.device_id.to_owned()),
            channel: context.channel.map(ToOwned::to_owned),
            session_id: Some(context.session_id.to_owned()),
            run_id: Some(context.run_id.to_owned()),
            skill_id: None,
        },
        step.tool.as_str(),
        false,
    );
    if decision.approval_required {
        budget.nested_approval_requests += 1;
    }
    if request.safety_policy.deny_sensitive_tools_without_approval
        && budget.nested_approval_requests > request.budgets.max_nested_approvals
    {
        budget.rejected_payloads += 1;
        return Ok((
            denied_step_result(
                step,
                "nested approval budget exhausted; tool program cannot self-approve sensitive tools",
                true,
            ),
            None,
        ));
    }
    if !decision.allowed {
        budget.rejected_payloads += 1;
        return Ok((
            denied_step_result(step, decision.reason.as_str(), decision.approval_required),
            None,
        ));
    }

    let child_proposal_id = format!("{proposal_id}:{}", step.step_id);
    let outcome = execute_tool_call(
        &runtime_state.config.tool_call,
        child_proposal_id.as_str(),
        step.tool.as_str(),
        input_bytes.as_slice(),
    )
    .await;
    let child_attestation =
        Some(child_attestation_from_outcome(request, proposal_id, step, &outcome.attestation));
    budget.child_runs_used += 1;
    budget.output_bytes_observed =
        budget.output_bytes_observed.saturating_add(outcome.output_json.len());

    let max_output_bytes = step
        .budget
        .max_output_bytes
        .unwrap_or(request.budgets.max_step_output_bytes)
        .min(request.budgets.max_total_output_bytes);
    if outcome.output_json.len() > max_output_bytes {
        let artifact = create_step_artifact(
            runtime_state,
            context,
            proposal_id,
            step,
            outcome.output_json.as_slice(),
        )
        .await?;
        budget.spilled_artifacts += 1;
        budget.saved_model_visible_bytes = budget
            .saved_model_visible_bytes
            .saturating_add(outcome.output_json.len().saturating_sub(max_output_bytes) as u64);
        return Ok((
            ToolProgramStepResult {
                step_id: step.step_id.clone(),
                tool: step.tool.clone(),
                status: ToolProgramStepStatus::Spilled,
                success: outcome.success,
                decision_reason: decision.reason,
                approval_required: decision.approval_required,
                output: json!({
                    "summary": summarize_output(outcome.output_json.as_slice(), max_output_bytes),
                    "artifact_id": artifact.artifact_id,
                    "digest_sha256": artifact.digest_sha256,
                }),
                error: outcome.error,
                artifact: Some(artifact),
            },
            child_attestation,
        ));
    }

    Ok((
        ToolProgramStepResult {
            step_id: step.step_id.clone(),
            tool: step.tool.clone(),
            status: if outcome.success {
                ToolProgramStepStatus::Completed
            } else {
                ToolProgramStepStatus::Failed
            },
            success: outcome.success,
            decision_reason: decision.reason,
            approval_required: decision.approval_required,
            output: parse_or_preview_output(outcome.output_json.as_slice()),
            error: outcome.error,
            artifact: None,
        },
        child_attestation,
    ))
}

async fn create_step_artifact(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    proposal_id: &str,
    step: &ToolProgramStep,
    output_json: &[u8],
) -> Result<ToolResultArtifactRef, String> {
    let turn_budget = ToolTurnBudget::default();
    let preview = summarize_output(output_json, turn_budget.max_artifact_preview_bytes);
    runtime_state
        .create_tool_result_artifact(ToolResultArtifactCreateRequest {
            artifact_id: Ulid::new().to_string(),
            session_id: context.session_id.to_owned(),
            run_id: context.run_id.to_owned(),
            proposal_id: proposal_id.to_owned(),
            tool_name: step.tool.clone(),
            mime_type: "application/json".to_owned(),
            sensitivity: step_result_sensitivity(step.tool.as_str()),
            retention: ArtifactRetentionPolicy::keep(),
            redacted_preview: preview,
            content: output_json.to_vec(),
        })
        .await
        .map_err(|status| {
            format!("failed to create tool program step artifact: {}", status.message())
        })
}

fn child_attestation_from_outcome(
    request: &ToolProgramRunRequest,
    proposal_id: &str,
    step: &ToolProgramStep,
    attestation: &ToolAttestation,
) -> ChildToolAttestation {
    ChildToolAttestation {
        parent_program_id: request.program_id.clone(),
        parent_proposal_id: proposal_id.to_owned(),
        step_id: step.step_id.clone(),
        tool_name: step.tool.clone(),
        attestation_id: attestation.attestation_id.clone(),
        execution_sha256: attestation.execution_sha256.clone(),
        executor: attestation.executor.clone(),
        sandbox_enforcement: attestation.sandbox_enforcement.clone(),
        timed_out: attestation.timed_out,
    }
}

fn parse_and_validate_request(input_json: &[u8]) -> Result<ToolProgramRunRequest, String> {
    let request = serde_json::from_slice::<ToolProgramRunRequest>(input_json)
        .map_err(|error| format!("palyra.tool_program.run input must be valid JSON: {error}"))?;
    validate_request(&request)?;
    Ok(request)
}

fn validate_request(request: &ToolProgramRunRequest) -> Result<(), String> {
    if request.schema_version != TOOL_PROGRAM_SCHEMA_VERSION {
        return Err(format!(
            "palyra.tool_program.run schema_version={} is unsupported",
            request.schema_version
        ));
    }
    if request.program_id.trim().is_empty() || request.program_id.len() > MAX_PROGRAM_ID_BYTES {
        return Err("palyra.tool_program.run requires bounded non-empty program_id".to_owned());
    }
    if request.steps.is_empty() {
        return Err("palyra.tool_program.run requires at least one step".to_owned());
    }
    if request.steps.len() > request.budgets.max_steps
        || request.steps.len() > MAX_TOOL_PROGRAM_STEPS
    {
        return Err("palyra.tool_program.run step count exceeds budget".to_owned());
    }
    if request.budgets.max_runtime_ms == 0
        || request.budgets.max_child_runs == 0
        || request.budgets.max_step_output_bytes == 0
        || request.budgets.max_total_output_bytes == 0
    {
        return Err("palyra.tool_program.run budgets must be positive".to_owned());
    }
    let mut step_ids = BTreeSet::new();
    for step in &request.steps {
        if step.step_id.trim().is_empty() || step.step_id.len() > MAX_STEP_ID_BYTES {
            return Err("palyra.tool_program.run step_id must be bounded and non-empty".to_owned());
        }
        if !step_ids.insert(step.step_id.to_ascii_lowercase()) {
            return Err(format!("duplicate tool program step_id '{}'", step.step_id));
        }
        if step.tool.trim().is_empty() {
            return Err("palyra.tool_program.run step tool must not be empty".to_owned());
        }
        if step.budget.timeout_ms == Some(0) {
            return Err("palyra.tool_program.run step timeout_ms must be positive".to_owned());
        }
        if step.allowed_artifact_refs.iter().any(|artifact_ref| artifact_ref.trim().is_empty()) {
            return Err(
                "palyra.tool_program.run allowed artifact refs must not be empty".to_owned()
            );
        }
    }
    Ok(())
}

fn tool_program_outcome(
    proposal_id: &str,
    input_json: &[u8],
    success: bool,
    output: Value,
    error: String,
    timed_out: bool,
) -> ToolExecutionOutcome {
    let output_json = serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec());
    build_tool_execution_outcome(
        proposal_id,
        TOOL_PROGRAM_RUN_TOOL_NAME,
        input_json,
        success,
        output_json,
        error,
        timed_out,
        "tool_program_runtime".to_owned(),
        "nested_tool_policy".to_owned(),
    )
}

fn cancelled_step_result(step: &ToolProgramStep, error: &str) -> ToolProgramStepResult {
    ToolProgramStepResult {
        step_id: step.step_id.clone(),
        tool: step.tool.clone(),
        status: ToolProgramStepStatus::Cancelled,
        success: false,
        decision_reason: "run cancellation requested".to_owned(),
        approval_required: false,
        output: json!({}),
        error: error.to_owned(),
        artifact: None,
    }
}

fn failed_step_result(step: &ToolProgramStep, error: &str) -> ToolProgramStepResult {
    ToolProgramStepResult {
        step_id: step.step_id.clone(),
        tool: step.tool.clone(),
        status: ToolProgramStepStatus::Failed,
        success: false,
        decision_reason: "tool program safety policy stopped execution".to_owned(),
        approval_required: false,
        output: json!({}),
        error: error.to_owned(),
        artifact: None,
    }
}

fn denied_step_result(
    step: &ToolProgramStep,
    reason: &str,
    approval_required: bool,
) -> ToolProgramStepResult {
    ToolProgramStepResult {
        step_id: step.step_id.clone(),
        tool: step.tool.clone(),
        status: ToolProgramStepStatus::Denied,
        success: false,
        decision_reason: reason.to_owned(),
        approval_required,
        output: json!({}),
        error: reason.to_owned(),
        artifact: None,
    }
}

fn parse_or_preview_output(output_json: &[u8]) -> Value {
    serde_json::from_slice::<Value>(output_json)
        .map(redact_sensitive_value)
        .unwrap_or_else(|_| json!({ "preview": summarize_output(output_json, 1024) }))
}

fn summarize_output(output_json: &[u8], max_bytes: usize) -> String {
    let raw = String::from_utf8_lossy(output_json);
    let redacted = redact_url_segments_in_text(redact_auth_error(raw.as_ref()).as_str());
    truncate_utf8(redacted.as_str(), max_bytes)
}

fn redact_sensitive_value(mut value: Value) -> Value {
    match &mut value {
        Value::Object(map) => {
            for (key, child) in map.iter_mut() {
                if is_sensitive_key(key) {
                    *child = Value::String(REDACTED.to_owned());
                } else {
                    *child = redact_sensitive_value(std::mem::take(child));
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                *item = redact_sensitive_value(std::mem::take(item));
            }
        }
        Value::String(raw) => {
            *raw = redact_url_segments_in_text(redact_auth_error(raw.as_str()).as_str());
        }
        _ => {}
    }
    value
}

fn step_result_sensitivity(tool_name: &str) -> ToolResultSensitivity {
    if tool_name == crate::gateway::PROCESS_RUNNER_TOOL_NAME {
        ToolResultSensitivity::StdoutStderr
    } else if tool_metadata(tool_name).is_some_and(|metadata| metadata.default_sensitive) {
        ToolResultSensitivity::ApprovalRiskData
    } else {
        ToolResultSensitivity::Public
    }
}

fn truncate_utf8(raw: &str, max_bytes: usize) -> String {
    if raw.len() <= max_bytes {
        return raw.to_owned();
    }
    let mut end = max_bytes.min(raw.len());
    while end > 0 && !raw.is_char_boundary(end) {
        end -= 1;
    }
    let mut output = raw[..end].to_owned();
    output.push_str("...");
    output
}

fn elapsed_millis(started_at: Instant) -> u64 {
    started_at.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

fn default_max_steps() -> usize {
    8
}

fn default_max_runtime_ms() -> u64 {
    30_000
}

fn default_max_child_runs() -> usize {
    8
}

fn default_max_step_output_bytes() -> usize {
    ToolTurnBudget::default().max_model_inline_bytes
}

fn default_max_total_output_bytes() -> usize {
    256 * 1024
}

fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::{parse_and_validate_request, ToolProgramStatus};

    #[test]
    fn validates_declarative_program_shape() {
        let request = parse_and_validate_request(
            br#"{
                "schema_version": 1,
                "program_id": "program-a",
                "steps": [
                    {"step_id": "echo", "tool": "palyra.echo", "input": {"text": "ok"}}
                ]
            }"#,
        )
        .expect("program should validate");

        assert_eq!(request.program_id, "program-a");
        assert_eq!(request.steps.len(), 1);
    }

    #[test]
    fn rejects_duplicate_step_ids_and_unknown_schema_versions() {
        let schema_error = parse_and_validate_request(
            br#"{"schema_version":2,"program_id":"bad","steps":[{"step_id":"a","tool":"palyra.echo"}]}"#,
        )
        .expect_err("unknown schema must fail closed");
        assert!(schema_error.contains("unsupported"));

        let duplicate_error = parse_and_validate_request(
            br#"{
                "schema_version": 1,
                "program_id": "bad",
                "steps": [
                    {"step_id": "a", "tool": "palyra.echo"},
                    {"step_id": "A", "tool": "palyra.echo"}
                ]
            }"#,
        )
        .expect_err("duplicate step IDs must fail closed");
        assert!(duplicate_error.contains("duplicate"));
    }

    #[test]
    fn tool_program_status_serializes_as_stable_snake_case() {
        let serialized =
            serde_json::to_string(&ToolProgramStatus::Cancelled).expect("status should serialize");
        assert_eq!(serialized, "\"cancelled\"");
    }
}
