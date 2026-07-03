//! Declarative tool-program runtime for `palyra.tool_program.run`.
//!
//! Validates a program (bounded steps, explicit tool grants, DAG
//! dependencies), then executes it level by level: every step becomes a
//! grant-checked tool RPC call (`tool_rpc.rs`) under shared budgets for
//! steps, runtime, child runs, nested approvals, and output bytes. Oversized
//! step outputs are spilled to tool-result artifacts so only a redacted
//! summary stays model-visible. Progress is journaled as a durable tool job
//! with per-step tail entries.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{Duration, Instant},
};

use futures::future::join_all;
use palyra_common::{
    redaction::{is_sensitive_key, redact_auth_error, redact_url_segments_in_text, REDACTED},
    runtime_contracts::{
        ArtifactRetentionPolicy, ToolResultArtifactRef, ToolResultSensitivity, ToolTurnBudget,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tracing::warn;
use ulid::Ulid;

use crate::{
    execution_backends::ExecutionBackendPreference,
    gateway::{
        current_unix_ms, GatewayRuntimeState, SharedToolBudget, ToolRuntimeExecutionContext,
        TOOL_PROGRAM_RUN_TOOL_NAME,
    },
    journal::{
        ToolJobCreateRequest, ToolJobRetentionPolicy,
        ToolJobRetryPolicy as DurableToolJobRetryPolicy, ToolJobState, ToolJobTailAppendRequest,
        ToolJobTailStream, ToolJobTransitionRequest, ToolResultArtifactCreateRequest,
    },
    tool_protocol::{build_tool_execution_outcome, tool_metadata, ToolExecutionOutcome},
    transport::grpc::proto::palyra::common::v1 as common_v1,
};

use super::artifacts::bounded_tool_result_artifact_content;
use super::process_registry::{
    BackgroundTaskRecord, BackgroundTaskRegistry, CleanupPolicy, ProcessRegistry,
    RuntimeProcessDiagnostic, RuntimeProcessRecord, RuntimeProcessState,
    EXECUTION_ENVIRONMENT_HEALTH_COMPLETED_EVENT, EXECUTION_ENVIRONMENT_HEALTH_FAILED_EVENT,
    EXECUTION_ENVIRONMENT_HEALTH_SCHEMA_VERSION, EXECUTION_ENVIRONMENT_HEALTH_STARTED_EVENT,
};
use super::process_registry::{
    ExecutionEnvironmentHealthJournalProjection, ExecutionEnvironmentHealthLongCommandHeartbeat,
    ExecutionEnvironmentResourceKind,
};
use super::tool_rpc::{
    build_python_tool_rpc_bridge_context_with_transports, execute_granted_tool_rpc_call,
    python_tool_rpc_sdk_source_for_tools, python_tool_rpc_sdk_wrappers, PythonToolRpcBridgeContext,
    PythonToolRpcTransportDescriptor, ToolRpcAttestation, ToolRpcRequest, ToolRpcResponse,
    ToolRpcResultProjection, ToolRpcScope, ToolRpcStatus, TOOL_RPC_SCHEMA_VERSION,
};

const TOOL_PROGRAM_SCHEMA_VERSION: u32 = 1;
const MAX_PROGRAM_ID_BYTES: usize = 128;
const MAX_STEP_ID_BYTES: usize = 128;
const MAX_TOOL_PROGRAM_STEPS: usize = 32;
const MAX_PYTHON_RPC_SCRIPT_BYTES: usize = 64 * 1024;

/// Top-level `palyra.tool_program.run` input: grants, budgets, safety
/// switches, and either a declarative step list or a Python RPC script plan.
#[derive(Debug, Clone, Deserialize)]
struct ToolProgramRunRequest {
    schema_version: u32,
    program_id: String,
    #[serde(default)]
    program_kind: ToolProgramKind,
    #[serde(default)]
    budgets: ToolProgramBudgets,
    #[serde(default)]
    safety_policy: ToolProgramSafetyPolicy,
    #[serde(default)]
    granted_tools: Vec<String>,
    #[serde(default)]
    script: Option<ToolProgramScriptSpec>,
    #[serde(default)]
    steps: Vec<ToolProgramStep>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ToolProgramKind {
    #[default]
    Declarative,
    PythonRpcScript,
}

/// Auditable script body plus its typed RPC calls. The daemon executes the
/// call plan through the normal tool RPC bridge instead of interpreting Python
/// in-process.
#[derive(Debug, Clone, Default, Deserialize)]
struct ToolProgramScriptSpec {
    #[serde(default)]
    source: String,
    #[serde(default)]
    calls: Vec<ToolProgramScriptCall>,
}

#[derive(Debug, Clone, Deserialize)]
struct ToolProgramScriptCall {
    call_id: String,
    tool_name: String,
    #[serde(default)]
    arguments: Value,
    #[serde(default)]
    scope: ToolRpcScope,
    #[serde(default)]
    timeout_ms: Option<u64>,
    #[serde(default)]
    max_output_bytes: Option<usize>,
    #[serde(default)]
    result_projection: ToolRpcResultProjection,
    #[serde(default)]
    depends_on: Vec<String>,
    #[serde(default)]
    parallelism: ToolParallelism,
    #[serde(default)]
    path_scope: Vec<String>,
    #[serde(default)]
    retry_policy: ToolProgramStepRetryPolicy,
}

/// One declarative step: a granted tool call plus its dependency, budget,
/// and parallelism constraints.
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
    #[serde(default)]
    result_projection: ToolRpcResultProjection,
    #[serde(default)]
    depends_on: Vec<String>,
    #[serde(default)]
    parallelism: ToolParallelism,
    #[serde(default)]
    path_scope: Vec<String>,
    #[serde(default)]
    retry_policy: ToolProgramStepRetryPolicy,
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

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ToolParallelism {
    #[default]
    Never,
    ReadOnlySafe,
    PathScoped,
    IdempotentNetwork,
}

impl ToolParallelism {
    fn allows_parallel(self) -> bool {
        !matches!(self, Self::Never)
    }
}

#[derive(Debug, Clone, Deserialize)]
struct ToolProgramStepRetryPolicy {
    #[serde(default = "default_retry_max_attempts")]
    max_attempts: u32,
    #[serde(default)]
    idempotency_key: Option<String>,
}

impl Default for ToolProgramStepRetryPolicy {
    fn default() -> Self {
        Self { max_attempts: default_retry_max_attempts(), idempotency_key: None }
    }
}

/// Fail-closed safety switches; defaults deny nested programs and stop on
/// the first failed step.
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
    program_kind: ToolProgramKind,
    status: ToolProgramStatus,
    steps: Vec<ToolProgramStepResult>,
    child_call_transcript: Vec<ToolProgramChildCallTranscript>,
    child_attestations: Vec<ChildToolAttestation>,
    budget: ToolProgramBudgetReport,
    budget_debit: ToolProgramBudgetReport,
    attestation_summary: ToolProgramAttestationSummary,
    python_bridge: PythonToolRpcBridgeContext,
    python_sdk: PythonToolRpcSdkManifest,
    python_sdk_bytes: usize,
    process_diagnostics: Vec<RuntimeProcessDiagnostic>,
    background_task_diagnostics: Vec<RuntimeProcessDiagnostic>,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ToolProgramStatus {
    Completed,
    Failed,
    Cancelled,
}

impl ToolProgramStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
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

#[derive(Debug, Clone, Serialize)]
struct ToolProgramChildCallTranscript {
    call_id: String,
    tool_name: String,
    status: ToolProgramStepStatus,
    success: bool,
    decision_reason: String,
    approval_required: bool,
    artifact_id: Option<String>,
    output_preview: String,
    attestation_id: Option<String>,
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

#[derive(Debug, Clone, Serialize)]
struct ToolProgramAttestationSummary {
    child_call_count: usize,
    attested_child_call_count: usize,
    executors: Vec<String>,
    execution_sha256: String,
}

#[derive(Debug, Clone, Serialize)]
struct PythonToolRpcSdkManifest {
    module_name: String,
    bytes: usize,
    source_sha256: String,
    source: String,
    wrappers: BTreeMap<String, String>,
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

#[derive(Debug)]
struct ToolProgramExecution {
    response: ToolProgramRunResponse,
    final_error: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ToolProgramJobCompletion {
    next_state: ToolJobState,
    reason: String,
    last_error: Option<String>,
}

/// Executes one `palyra.tool_program.run` proposal.
///
/// Never fails at the tool boundary: parse/validation errors and runtime
/// failures are folded into an unsuccessful [`ToolExecutionOutcome`] whose
/// output JSON carries the error string.
pub(crate) async fn execute_tool_program_run_tool(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    proposal_id: &str,
    input_json: &[u8],
    remaining_tool_budget: SharedToolBudget,
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

    match execute_validated_program(
        runtime_state,
        context,
        proposal_id,
        input_json,
        request,
        remaining_tool_budget,
    )
    .await
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
    input_json: &[u8],
    request: ToolProgramRunRequest,
    remaining_tool_budget: SharedToolBudget,
) -> Result<(ToolProgramRunResponse, String), String> {
    let started_at = Instant::now();
    let mut budget = ToolProgramBudgetReport::default();
    let mut results = Vec::new();
    let mut child_attestations = Vec::new();
    let grants = granted_tool_set(&request)?;
    let program_kind = request.program_kind;
    let python_bridge = build_python_tool_rpc_bridge_context_with_transports(
        proposal_id,
        request.program_id.as_str(),
        &grants,
        tool_rpc_transports_for_context(context, proposal_id),
    );
    let python_sdk_source = python_tool_rpc_sdk_source_for_tools(&grants);
    let python_sdk = PythonToolRpcSdkManifest {
        module_name: "palyra_tools.py".to_owned(),
        bytes: python_sdk_source.len(),
        source_sha256: sha256_hex(python_sdk_source.as_bytes()),
        source: python_sdk_source,
        wrappers: python_tool_rpc_sdk_wrappers(&grants),
    };
    let python_sdk_bytes = python_sdk.bytes;
    let job_id = Ulid::new().to_string();
    runtime_state
        .create_tool_job(ToolJobCreateRequest {
            job_id: job_id.clone(),
            owner_principal: context.principal.to_owned(),
            device_id: context.device_id.to_owned(),
            channel: context.channel.map(ToOwned::to_owned),
            session_id: context.session_id.to_owned(),
            run_id: context.run_id.to_owned(),
            tool_call_id: proposal_id.to_owned(),
            tool_name: TOOL_PROGRAM_RUN_TOOL_NAME.to_owned(),
            backend: context.execution_backend.as_str().to_owned(),
            backend_reason_code: Some(context.backend_reason_code.to_owned()),
            command_sha256: sha256_hex(input_json),
            program_sha256: Some(sha256_hex(input_json)),
            state: ToolJobState::Running,
            retry_policy: DurableToolJobRetryPolicy::default(),
            cancellation_handle: Some(format!("cancel:{proposal_id}")),
            retention: ToolJobRetentionPolicy::default(),
            artifact_refs_json: None,
            lease_expires_at_unix_ms: None,
        })
        .await
        .map_err(|status| format!("failed to create tool program job: {}", status.message()))?;

    let execution = async {
        let mut process_registry = ProcessRegistry::default();
        let mut background_registry = BackgroundTaskRegistry::default();
        let cleanup_policy = CleanupPolicy::tool_program_default();
        let background_task_id = format!("tool-program:{}", request.program_id);
        let background_started_at_unix_ms = current_unix_ms();
        background_registry.register(BackgroundTaskRecord {
            task_id: background_task_id.clone(),
            owner: context.run_id.to_owned(),
            purpose: "palyra.tool_program.run".to_owned(),
            started_at_unix_ms: background_started_at_unix_ms,
            last_heartbeat_at_unix_ms: Some(background_started_at_unix_ms),
            cancellation_handle: format!("cancel:{proposal_id}"),
            cleanup_policy: cleanup_policy.clone(),
            state: RuntimeProcessState::Running,
        })?;
        let started_health = background_registry
            .heartbeat(background_task_id.as_str(), background_started_at_unix_ms)?;
        record_execution_environment_health_journal_projection(
            runtime_state,
            context,
            execution_environment_health_journal_projection(
                EXECUTION_ENVIRONMENT_HEALTH_STARTED_EVENT,
                context,
                request.program_id.as_str(),
                ExecutionEnvironmentResourceKind::ToolProgram,
                started_health,
                background_started_at_unix_ms,
                execution_environment_health_evidence_refs(
                    proposal_id,
                    job_id.as_str(),
                    request.program_id.as_str(),
                    "running",
                ),
            ),
        )
        .await;

        let execution_plan = build_execution_plan(&request)?;
        let mut final_status = ToolProgramStatus::Completed;
        let mut final_error = String::new();
        'program: for level in execution_plan {
            background_registry.heartbeat(background_task_id.as_str(), current_unix_ms())?;
            if runtime_state
                .is_orchestrator_cancel_requested(context.run_id.to_owned())
                .await
                .map_err(|status| format!("cancellation check failed: {}", status.message()))?
            {
                final_status = ToolProgramStatus::Cancelled;
                final_error = "tool program cancelled before next step".to_owned();
                for step_index in level {
                    results.push(cancelled_step_result(
                        &request.steps[step_index],
                        final_error.as_str(),
                    ));
                }
                break;
            }
            if started_at.elapsed() > Duration::from_millis(request.budgets.max_runtime_ms) {
                final_status = ToolProgramStatus::Failed;
                final_error = format!(
                    "tool program exceeded runtime budget max_runtime_ms={}",
                    request.budgets.max_runtime_ms
                );
                for step_index in level {
                    results
                        .push(failed_step_result(&request.steps[step_index], final_error.as_str()));
                }
                break;
            }
            if budget.child_runs_used.saturating_add(level.len()) > request.budgets.max_child_runs {
                final_status = ToolProgramStatus::Failed;
                final_error = "tool program child run budget exhausted".to_owned();
                for step_index in level {
                    results
                        .push(failed_step_result(&request.steps[step_index], final_error.as_str()));
                }
                break;
            }

            let parallel = level.len() > 1 && level_allows_parallel(&request.steps, &level);
            for step_index in &level {
                let step = &request.steps[*step_index];
                let process_id = format!("{}:{}", request.program_id, step.step_id);
                let process_started_at_unix_ms = current_unix_ms();
                process_registry.register(RuntimeProcessRecord {
                    process_id,
                    owner: context.run_id.to_owned(),
                    purpose: format!("tool-program-step:{}", step.tool),
                    started_at_unix_ms: process_started_at_unix_ms,
                    last_heartbeat_at_unix_ms: Some(process_started_at_unix_ms),
                    cancellation_handle: format!("cancel:{proposal_id}:{}", step.step_id),
                    cleanup_policy: cleanup_policy.clone(),
                    state: RuntimeProcessState::Running,
                })?;
            }

            // Budget deltas are applied only after the whole level resolves, so
            // every step in a level (parallel or sequential) sees the budget as
            // of level start; siblings cannot observe each other's spending.
            let step_executions = if parallel {
                let budget_snapshot = budget.clone();
                let futures = level.iter().map(|step_index| {
                    let step = &request.steps[*step_index];
                    execute_program_step(
                        runtime_state,
                        context,
                        proposal_id,
                        &request,
                        step,
                        &budget_snapshot,
                        &grants,
                        Some(remaining_tool_budget.clone()),
                    )
                });
                join_all(futures).await
            } else {
                let mut executions = Vec::with_capacity(level.len());
                for step_index in &level {
                    let step = &request.steps[*step_index];
                    executions.push(
                        execute_program_step(
                            runtime_state,
                            context,
                            proposal_id,
                            &request,
                            step,
                            &budget,
                            &grants,
                            Some(remaining_tool_budget.clone()),
                        )
                        .await,
                    );
                }
                executions
            };

            for (step_index, execution) in level.into_iter().zip(step_executions) {
                let step = &request.steps[step_index];
                let process_id = format!("{}:{}", request.program_id, step.step_id);
                let (step_result, child_attestation, budget_delta) = execution?;
                apply_budget_delta(&mut budget, &budget_delta);
                if let Some(attestation) = child_attestation {
                    child_attestations.push(attestation);
                }
                let _ = runtime_state
                    .append_tool_job_tail(ToolJobTailAppendRequest {
                        job_id: job_id.clone(),
                        stream: if step_result.success {
                            ToolJobTailStream::Stdout
                        } else {
                            ToolJobTailStream::Stderr
                        },
                        chunk: format!(
                            "step={} status={:?} success={} error={}",
                            step.step_id,
                            step_result.status,
                            step_result.success,
                            step_result.error
                        ),
                    })
                    .await;
                process_registry.heartbeat(process_id.as_str(), current_unix_ms())?;
                background_registry.heartbeat(background_task_id.as_str(), current_unix_ms())?;
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
                        break 'program;
                    }
                    continue;
                }
                results.push(step_result);
            }
        }

        let completed_at_unix_ms = current_unix_ms();
        let final_health =
            background_registry.heartbeat(background_task_id.as_str(), completed_at_unix_ms)?;
        let final_event_type = if final_status == ToolProgramStatus::Completed {
            EXECUTION_ENVIRONMENT_HEALTH_COMPLETED_EVENT
        } else {
            EXECUTION_ENVIRONMENT_HEALTH_FAILED_EVENT
        };
        record_execution_environment_health_journal_projection(
            runtime_state,
            context,
            execution_environment_health_journal_projection(
                final_event_type,
                context,
                request.program_id.as_str(),
                ExecutionEnvironmentResourceKind::ToolProgram,
                final_health,
                completed_at_unix_ms,
                execution_environment_health_evidence_refs(
                    proposal_id,
                    job_id.as_str(),
                    request.program_id.as_str(),
                    final_status.as_str(),
                ),
            ),
        )
        .await;
        let process_diagnostics = process_registry.diagnostics(current_unix_ms());
        let _shutdown = process_registry.shutdown(elapsed_millis(started_at));
        let _ = background_registry.complete(background_task_id.as_str());
        let child_call_transcript =
            child_call_transcript_from_results(&results, &child_attestations);
        let attestation_summary =
            tool_program_attestation_summary(results.len(), &child_attestations);
        let budget_debit = budget.clone();
        Ok(ToolProgramExecution {
            response: ToolProgramRunResponse {
                schema_version: TOOL_PROGRAM_SCHEMA_VERSION,
                program_id: request.program_id,
                program_kind,
                status: final_status,
                steps: results,
                child_call_transcript,
                child_attestations,
                budget,
                budget_debit,
                attestation_summary,
                python_bridge,
                python_sdk,
                python_sdk_bytes,
                process_diagnostics,
                background_task_diagnostics: background_registry.diagnostics(current_unix_ms()),
            },
            final_error,
        })
    }
    .await;
    transition_tool_program_job(runtime_state, job_id.as_str(), &execution).await;
    execution.map(|execution| (execution.response, execution.final_error))
}

fn execution_environment_health_journal_projection(
    event_type: &str,
    context: ToolRuntimeExecutionContext<'_>,
    resource_id: &str,
    resource_kind: ExecutionEnvironmentResourceKind,
    health: ExecutionEnvironmentHealthLongCommandHeartbeat,
    created_at_unix_ms: i64,
    evidence_refs: Vec<String>,
) -> ExecutionEnvironmentHealthJournalProjection {
    ExecutionEnvironmentHealthJournalProjection {
        schema_version: EXECUTION_ENVIRONMENT_HEALTH_SCHEMA_VERSION,
        event_type: event_type.to_owned(),
        session_id: context.session_id.to_owned(),
        run_id: context.run_id.to_owned(),
        resource_id: resource_id.to_owned(),
        resource_kind,
        decision: health.decision,
        reason_codes: health.reason_codes,
        created_at_unix_ms,
        evidence_refs,
        redaction_level: health.redaction_level,
    }
}

async fn record_execution_environment_health_journal_projection(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    projection: ExecutionEnvironmentHealthJournalProjection,
) {
    let payload_json = match serde_json::to_string(&projection) {
        Ok(payload) => payload,
        Err(error) => {
            warn!(
                event_type = %projection.event_type,
                error = %error,
                "failed to serialize execution environment health journal projection"
            );
            return;
        }
    };
    let event_type = projection.event_type.clone();
    if let Err(error) = runtime_state
        .record_journal_event(crate::journal::JournalAppendRequest {
            event_id: Ulid::new().to_string(),
            session_id: projection.session_id.clone(),
            run_id: projection.run_id.clone(),
            kind: common_v1::journal_event::EventKind::ToolExecuted as i32,
            actor: common_v1::journal_event::EventActor::System as i32,
            timestamp_unix_ms: projection.created_at_unix_ms,
            payload_json: payload_json.into_bytes(),
            principal: context.principal.to_owned(),
            device_id: context.device_id.to_owned(),
            channel: context.channel.map(str::to_owned),
        })
        .await
    {
        warn!(
            event_type = %event_type,
            status_code = ?error.code(),
            status_message = %error.message(),
            "execution environment health journal write failed"
        );
    }
}

fn tool_rpc_transports_for_context(
    context: ToolRuntimeExecutionContext<'_>,
    proposal_id: &str,
) -> Vec<PythonToolRpcTransportDescriptor> {
    let proposal_digest = sha256_hex(proposal_id.as_bytes());
    let bridge_id = &proposal_digest[..16];
    match context.execution_backend {
        ExecutionBackendPreference::Docker => vec![PythonToolRpcTransportDescriptor::file_jsonl(
            format!("/workspace/.palyra/tool-rpc/{bridge_id}/requests"),
            format!("/workspace/.palyra/tool-rpc/{bridge_id}/responses"),
            30_000,
        )],
        ExecutionBackendPreference::NetworkedWorker | ExecutionBackendPreference::SshTunnel => {
            vec![PythonToolRpcTransportDescriptor::artifact_jsonl(
                format!("tool-rpc:{bridge_id}"),
                30_000,
            )]
        }
        _ => vec![PythonToolRpcTransportDescriptor::stdio_jsonl(30_000)],
    }
}

fn execution_environment_health_evidence_refs(
    proposal_id: &str,
    job_id: &str,
    program_id: &str,
    status: &str,
) -> Vec<String> {
    vec![
        format!("tool_call:{proposal_id}"),
        format!("tool_job:{job_id}"),
        format!("tool_program:{program_id}"),
        format!("status:{status}"),
    ]
}

async fn transition_tool_program_job(
    runtime_state: &Arc<GatewayRuntimeState>,
    job_id: &str,
    execution: &Result<ToolProgramExecution, String>,
) {
    let completion = tool_program_job_completion(execution);
    let _ = runtime_state
        .transition_tool_job(ToolJobTransitionRequest {
            job_id: job_id.to_owned(),
            expected_state: None,
            next_state: completion.next_state,
            reason: completion.reason,
            last_error: completion.last_error,
            heartbeat_at_unix_ms: Some(current_unix_ms()),
            lease_expires_at_unix_ms: None,
        })
        .await;
}

fn tool_program_job_completion(
    execution: &Result<ToolProgramExecution, String>,
) -> ToolProgramJobCompletion {
    match execution {
        Ok(execution) => tool_program_job_completion_for_status(
            execution.response.status,
            &execution.final_error,
        ),
        Err(error) => ToolProgramJobCompletion {
            next_state: ToolJobState::Failed,
            reason: error.clone(),
            last_error: Some(error.clone()),
        },
    }
}

fn tool_program_job_completion_for_status(
    status: ToolProgramStatus,
    final_error: &str,
) -> ToolProgramJobCompletion {
    let next_state = match status {
        ToolProgramStatus::Completed => ToolJobState::Completed,
        ToolProgramStatus::Failed => ToolJobState::Failed,
        ToolProgramStatus::Cancelled => ToolJobState::Cancelled,
    };
    ToolProgramJobCompletion {
        next_state,
        reason: if final_error.is_empty() {
            "tool_program_finished".to_owned()
        } else {
            final_error.to_owned()
        },
        last_error: (!final_error.is_empty()).then(|| final_error.to_owned()),
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute_program_step(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    proposal_id: &str,
    request: &ToolProgramRunRequest,
    step: &ToolProgramStep,
    budget_snapshot: &ToolProgramBudgetReport,
    grants: &BTreeSet<String>,
    remaining_tool_budget: Option<SharedToolBudget>,
) -> Result<(ToolProgramStepResult, Option<ChildToolAttestation>, ToolProgramBudgetReport), String>
{
    let mut budget_delta =
        ToolProgramBudgetReport { steps_used: 1, ..ToolProgramBudgetReport::default() };
    if step.tool == TOOL_PROGRAM_RUN_TOOL_NAME && !request.safety_policy.allow_nested_programs {
        return Ok((
            denied_step_result(
                step,
                "nested tool programs require allow_nested_programs=true and explicit outer approval",
                true,
            ),
            None,
            budget_delta,
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
            budget_delta,
        ));
    }

    if step.allowed_artifact_refs.iter().any(|artifact_ref| artifact_ref.len() > 512) {
        return Ok((
            denied_step_result(step, "allowed artifact refs must be bounded", false),
            None,
            budget_delta,
        ));
    }

    if step.scopes.iter().any(|scope| scope.len() > 128) {
        return Ok((
            denied_step_result(step, "step scopes must be bounded", false),
            None,
            budget_delta,
        ));
    }

    let rpc_request = ToolRpcRequest {
        schema_version: TOOL_RPC_SCHEMA_VERSION,
        call_id: step.step_id.clone(),
        tool_name: step.tool.clone(),
        arguments: step.input.clone(),
        scope: ToolRpcScope {
            scopes: step.scopes.clone(),
            allowed_artifact_refs: step.allowed_artifact_refs.clone(),
        },
        timeout_ms: step.budget.timeout_ms,
        result_projection: step.result_projection.clone(),
    };
    let (rpc_response, child_runs_consumed) = execute_tool_rpc_with_retries(
        runtime_state,
        context,
        proposal_id,
        grants,
        remaining_tool_budget,
        rpc_request,
        &step.retry_policy,
    )
    .await;
    budget_delta.child_runs_used += usize::try_from(child_runs_consumed).unwrap_or(usize::MAX);
    if rpc_response.approval_required {
        budget_delta.nested_approval_requests += 1;
    }
    // The approval budget is checked against the pre-level snapshot plus this
    // step's own delta; with the default max_nested_approvals=0 any
    // approval-shaped child call is rejected outright.
    if request.safety_policy.deny_sensitive_tools_without_approval
        && budget_snapshot
            .nested_approval_requests
            .saturating_add(budget_delta.nested_approval_requests)
            > request.budgets.max_nested_approvals
    {
        budget_delta.rejected_payloads += 1;
        return Ok((
            denied_step_result(
                step,
                "nested approval budget exhausted; tool program cannot self-approve sensitive tools",
                true,
            ),
            None,
            budget_delta,
        ));
    }
    if rpc_response.status == ToolRpcStatus::Denied {
        budget_delta.rejected_payloads += 1;
        return Ok((
            denied_step_result(step, rpc_response.error.as_str(), rpc_response.approval_required),
            None,
            budget_delta,
        ));
    }

    let child_attestation =
        child_attestation_from_rpc_response(request, proposal_id, step, &rpc_response);
    let output_json = serde_json::to_vec(&rpc_response.output)
        .map_err(|error| format!("failed to serialize tool rpc response output: {error}"))?;
    budget_delta.output_bytes_observed =
        budget_delta.output_bytes_observed.saturating_add(output_json.len());

    let max_output_bytes = step
        .budget
        .max_output_bytes
        .unwrap_or(request.budgets.max_step_output_bytes)
        .min(request.budgets.max_total_output_bytes);
    // Oversized output is spilled to an artifact: the step keeps its success
    // flag, but only a redacted summary plus the artifact reference stays
    // model-visible.
    if output_json.len() > max_output_bytes {
        let artifact =
            create_step_artifact(runtime_state, context, proposal_id, step, output_json.as_slice())
                .await?;
        budget_delta.spilled_artifacts += 1;
        budget_delta.saved_model_visible_bytes = budget_delta
            .saved_model_visible_bytes
            .saturating_add(output_json.len().saturating_sub(max_output_bytes) as u64);
        return Ok((
            ToolProgramStepResult {
                step_id: step.step_id.clone(),
                tool: step.tool.clone(),
                status: ToolProgramStepStatus::Spilled,
                success: rpc_response.success,
                decision_reason: rpc_response.decision_reason,
                approval_required: rpc_response.approval_required,
                output: json!({
                    "summary": summarize_output(output_json.as_slice(), max_output_bytes),
                    "artifact_id": artifact.artifact_id,
                    "digest_sha256": artifact.digest_sha256,
                }),
                error: rpc_response.error,
                artifact: Some(artifact),
            },
            child_attestation,
            budget_delta,
        ));
    }

    Ok((
        ToolProgramStepResult {
            step_id: step.step_id.clone(),
            tool: step.tool.clone(),
            status: match rpc_response.status {
                ToolRpcStatus::Completed => ToolProgramStepStatus::Completed,
                ToolRpcStatus::Denied => ToolProgramStepStatus::Denied,
                // A timed-out child call was dropped mid-flight by its
                // deadline, so it surfaces as a cancelled step rather than a
                // tool failure.
                ToolRpcStatus::TimedOut => ToolProgramStepStatus::Cancelled,
                ToolRpcStatus::Failed => ToolProgramStepStatus::Failed,
            },
            success: rpc_response.success,
            decision_reason: rpc_response.decision_reason,
            approval_required: rpc_response.approval_required,
            output: parse_or_preview_output(output_json.as_slice()),
            error: rpc_response.error,
            artifact: None,
        },
        child_attestation,
        budget_delta,
    ))
}

async fn execute_tool_rpc_with_retries(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    proposal_id: &str,
    grants: &BTreeSet<String>,
    remaining_tool_budget: Option<SharedToolBudget>,
    request: ToolRpcRequest,
    retry_policy: &ToolProgramStepRetryPolicy,
) -> (ToolRpcResponse, u32) {
    let max_attempts = retry_policy.max_attempts.max(1);
    let mut attempt = 1;
    let mut child_runs_consumed = 0_u32;
    loop {
        let (response, consumed) = execute_granted_tool_rpc_call(
            runtime_state,
            context,
            proposal_id,
            grants,
            remaining_tool_budget.clone(),
            request.clone(),
        )
        .await;
        child_runs_consumed = child_runs_consumed.saturating_add(consumed);
        if !should_retry_tool_rpc_response(&response, retry_policy, attempt) {
            return (response, child_runs_consumed);
        }
        if attempt >= max_attempts {
            return (response, child_runs_consumed);
        }
        attempt += 1;
    }
}

fn should_retry_tool_rpc_response(
    response: &ToolRpcResponse,
    retry_policy: &ToolProgramStepRetryPolicy,
    attempt: u32,
) -> bool {
    attempt < retry_policy.max_attempts
        && retry_policy.idempotency_key.as_deref().is_some_and(|value| !value.is_empty())
        && !response.approval_required
        && matches!(response.status, ToolRpcStatus::Failed | ToolRpcStatus::TimedOut)
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
    let content = bounded_tool_result_artifact_content(
        output_json,
        runtime_state.tool_result_artifact_max_payload_bytes(),
    )
    .map_err(|error| format!("failed to prepare tool program step artifact: {error}"))?
    .content;
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
            content,
        })
        .await
        .map_err(|status| {
            format!("failed to create tool program step artifact: {}", status.message())
        })
}

fn child_attestation_from_rpc_response(
    request: &ToolProgramRunRequest,
    proposal_id: &str,
    step: &ToolProgramStep,
    response: &ToolRpcResponse,
) -> Option<ChildToolAttestation> {
    let attestation: &ToolRpcAttestation = response.attestation.as_ref()?;
    Some(ChildToolAttestation {
        parent_program_id: request.program_id.clone(),
        parent_proposal_id: proposal_id.to_owned(),
        step_id: step.step_id.clone(),
        tool_name: step.tool.clone(),
        attestation_id: attestation.attestation_id.clone(),
        execution_sha256: attestation.execution_sha256.clone(),
        executor: attestation.executor.clone(),
        sandbox_enforcement: attestation.sandbox_enforcement.clone(),
        timed_out: attestation.timed_out,
    })
}

fn child_call_transcript_from_results(
    results: &[ToolProgramStepResult],
    child_attestations: &[ChildToolAttestation],
) -> Vec<ToolProgramChildCallTranscript> {
    let attestation_by_step = child_attestations
        .iter()
        .map(|attestation| (attestation.step_id.as_str(), attestation.attestation_id.as_str()))
        .collect::<BTreeMap<_, _>>();
    results
        .iter()
        .map(|result| ToolProgramChildCallTranscript {
            call_id: result.step_id.clone(),
            tool_name: result.tool.clone(),
            status: result.status,
            success: result.success,
            decision_reason: result.decision_reason.clone(),
            approval_required: result.approval_required,
            artifact_id: result.artifact.as_ref().map(|artifact| artifact.artifact_id.clone()),
            output_preview: transcript_output_preview(&result.output),
            attestation_id: attestation_by_step
                .get(result.step_id.as_str())
                .map(|attestation_id| (*attestation_id).to_owned()),
        })
        .collect()
}

fn transcript_output_preview(output: &Value) -> String {
    let output_json = serde_json::to_vec(output).unwrap_or_else(|_| b"{}".to_vec());
    summarize_output(output_json.as_slice(), 1024)
}

fn tool_program_attestation_summary(
    child_call_count: usize,
    child_attestations: &[ChildToolAttestation],
) -> ToolProgramAttestationSummary {
    let executors = child_attestations
        .iter()
        .map(|attestation| attestation.executor.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut hasher = Sha256::new();
    for attestation in child_attestations {
        hasher.update(attestation.step_id.as_bytes());
        hasher.update([0]);
        hasher.update(attestation.execution_sha256.as_bytes());
        hasher.update([0]);
    }
    ToolProgramAttestationSummary {
        child_call_count,
        attested_child_call_count: child_attestations.len(),
        executors,
        execution_sha256: hex::encode(hasher.finalize()),
    }
}

fn granted_tool_set(request: &ToolProgramRunRequest) -> Result<BTreeSet<String>, String> {
    let grants = request
        .granted_tools
        .iter()
        .map(|tool| tool.trim())
        .filter(|tool| !tool.is_empty())
        .map(ToOwned::to_owned)
        .collect::<BTreeSet<_>>();
    if grants.is_empty() {
        return Err("palyra.tool_program.run requires explicit granted_tools".to_owned());
    }
    Ok(grants)
}

/// Builds the level-ordered execution plan: Kahn's topological sort where
/// each level holds the steps whose dependencies are fully satisfied by
/// earlier levels.
fn build_execution_plan(request: &ToolProgramRunRequest) -> Result<Vec<Vec<usize>>, String> {
    let mut id_to_index = BTreeMap::new();
    for (index, step) in request.steps.iter().enumerate() {
        // Dependency ids resolve case-insensitively, mirroring the
        // duplicate-id check in validate_request.
        id_to_index.insert(step.step_id.to_ascii_lowercase(), index);
    }

    let mut incoming = vec![0_usize; request.steps.len()];
    let mut outgoing = vec![Vec::<usize>::new(); request.steps.len()];
    for (index, step) in request.steps.iter().enumerate() {
        for dependency in &step.depends_on {
            let dependency_key = dependency.to_ascii_lowercase();
            let Some(dependency_index) = id_to_index.get(dependency_key.as_str()).copied() else {
                return Err(format!(
                    "tool program step '{}' depends on unknown step '{}'",
                    step.step_id, dependency
                ));
            };
            incoming[index] += 1;
            outgoing[dependency_index].push(index);
        }
    }

    // BTreeSet keeps the within-level ordering deterministic, which stable
    // plan fixtures and tests rely on.
    let mut ready = incoming
        .iter()
        .enumerate()
        .filter_map(|(index, count)| (*count == 0).then_some(index))
        .collect::<BTreeSet<_>>();
    let mut visited = 0_usize;
    let mut levels = Vec::new();
    while !ready.is_empty() {
        let level = ready.iter().copied().collect::<Vec<_>>();
        ready.clear();
        for index in &level {
            visited += 1;
            for child in &outgoing[*index] {
                incoming[*child] = incoming[*child].saturating_sub(1);
                if incoming[*child] == 0 {
                    ready.insert(*child);
                }
            }
        }
        levels.push(level);
    }

    // Any node left unvisited by the sort can only sit on a dependency cycle.
    if visited != request.steps.len() {
        return Err("tool program dependency graph contains a cycle".to_owned());
    }
    Ok(levels)
}

fn level_allows_parallel(steps: &[ToolProgramStep], level: &[usize]) -> bool {
    if level.iter().any(|index| !steps[*index].parallelism.allows_parallel()) {
        return false;
    }
    !level_has_path_conflicts(steps, level)
}

fn level_has_path_conflicts(steps: &[ToolProgramStep], level: &[usize]) -> bool {
    for (position, left_index) in level.iter().enumerate() {
        for right_index in level.iter().skip(position + 1) {
            if step_paths_conflict(&steps[*left_index], &steps[*right_index]) {
                return true;
            }
        }
    }
    false
}

fn step_paths_conflict(left: &ToolProgramStep, right: &ToolProgramStep) -> bool {
    // An empty scope means the step declared no paths (non-path_scoped
    // parallelism modes); such steps never path-conflict.
    if left.path_scope.is_empty() || right.path_scope.is_empty() {
        return false;
    }
    left.path_scope.iter().any(|left_path| {
        right
            .path_scope
            .iter()
            .any(|right_path| paths_conflict(left_path.as_str(), right_path.as_str()))
    })
}

// Two scopes conflict when they are equal or one is a directory prefix of
// the other ("src" vs "src/app"); sibling paths stay parallel-safe.
fn paths_conflict(left: &str, right: &str) -> bool {
    let left = left.trim_matches('/');
    let right = right.trim_matches('/');
    left == right
        || left.strip_prefix(right).is_some_and(|suffix| suffix.starts_with('/'))
        || right.strip_prefix(left).is_some_and(|suffix| suffix.starts_with('/'))
}

fn apply_budget_delta(budget: &mut ToolProgramBudgetReport, delta: &ToolProgramBudgetReport) {
    budget.steps_used = budget.steps_used.saturating_add(delta.steps_used);
    budget.child_runs_used = budget.child_runs_used.saturating_add(delta.child_runs_used);
    budget.nested_approval_requests =
        budget.nested_approval_requests.saturating_add(delta.nested_approval_requests);
    budget.output_bytes_observed =
        budget.output_bytes_observed.saturating_add(delta.output_bytes_observed);
    budget.spilled_artifacts = budget.spilled_artifacts.saturating_add(delta.spilled_artifacts);
    budget.rejected_payloads = budget.rejected_payloads.saturating_add(delta.rejected_payloads);
    budget.saved_model_visible_bytes =
        budget.saved_model_visible_bytes.saturating_add(delta.saved_model_visible_bytes);
}

fn parse_and_validate_request(input_json: &[u8]) -> Result<ToolProgramRunRequest, String> {
    let mut request = serde_json::from_slice::<ToolProgramRunRequest>(input_json)
        .map_err(|error| format!("palyra.tool_program.run input must be valid JSON: {error}"))?;
    materialize_python_rpc_script_steps(&mut request)?;
    validate_request(&request)?;
    Ok(request)
}

fn materialize_python_rpc_script_steps(request: &mut ToolProgramRunRequest) -> Result<(), String> {
    match request.program_kind {
        ToolProgramKind::Declarative => {
            if request.script.is_some() {
                return Err(
                    "palyra.tool_program.run script requires program_kind=python_rpc_script"
                        .to_owned(),
                );
            }
            Ok(())
        }
        ToolProgramKind::PythonRpcScript => {
            if !request.steps.is_empty() {
                return Err(
                    "palyra.tool_program.run python_rpc_script uses script.calls instead of steps"
                        .to_owned(),
                );
            }
            let script = request.script.as_ref().ok_or_else(|| {
                "palyra.tool_program.run python_rpc_script requires script".to_owned()
            })?;
            if script.source.trim().is_empty() {
                return Err(
                    "palyra.tool_program.run python_rpc_script requires non-empty script.source"
                        .to_owned(),
                );
            }
            if script.source.len() > MAX_PYTHON_RPC_SCRIPT_BYTES {
                return Err("palyra.tool_program.run python_rpc_script source exceeds 65536 bytes"
                    .to_owned());
            }
            if script.calls.is_empty() {
                return Err(
                    "palyra.tool_program.run python_rpc_script requires at least one script call"
                        .to_owned(),
                );
            }
            request.steps = script.calls.iter().map(tool_program_step_from_script_call).collect();
            Ok(())
        }
    }
}

fn tool_program_step_from_script_call(call: &ToolProgramScriptCall) -> ToolProgramStep {
    ToolProgramStep {
        step_id: call.call_id.clone(),
        tool: call.tool_name.clone(),
        input: call.arguments.clone(),
        scopes: call.scope.scopes.clone(),
        budget: ToolProgramStepBudget {
            max_output_bytes: call.max_output_bytes,
            timeout_ms: call.timeout_ms,
        },
        allowed_artifact_refs: call.scope.allowed_artifact_refs.clone(),
        result_projection: call.result_projection.clone(),
        depends_on: call.depends_on.clone(),
        parallelism: call.parallelism,
        path_scope: call.path_scope.clone(),
        retry_policy: call.retry_policy.clone(),
    }
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
    let granted_tools = granted_tool_set(request)?;
    let mut step_ids = BTreeSet::new();
    let mut max_child_attempts = 0_usize;
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
        if !granted_tools.contains(&step.tool) {
            return Err(format!(
                "tool program step '{}' uses tool '{}' outside granted_tools",
                step.step_id, step.tool
            ));
        }
        if step.budget.timeout_ms == Some(0) {
            return Err("palyra.tool_program.run step timeout_ms must be positive".to_owned());
        }
        if step.allowed_artifact_refs.iter().any(|artifact_ref| artifact_ref.trim().is_empty()) {
            return Err(
                "palyra.tool_program.run allowed artifact refs must not be empty".to_owned()
            );
        }
        if step.depends_on.iter().any(|dependency| dependency.trim().is_empty()) {
            return Err("palyra.tool_program.run dependencies must not be empty".to_owned());
        }
        if step
            .depends_on
            .iter()
            .any(|dependency| dependency.eq_ignore_ascii_case(step.step_id.as_str()))
        {
            return Err(format!("tool program step '{}' cannot depend on itself", step.step_id));
        }
        if step.parallelism == ToolParallelism::PathScoped && step.path_scope.is_empty() {
            return Err("path_scoped parallelism requires non-empty path_scope".to_owned());
        }
        if step.retry_policy.max_attempts == 0 {
            return Err("tool program retry max_attempts must be positive".to_owned());
        }
        if step.retry_policy.max_attempts > 1
            && step.retry_policy.idempotency_key.as_deref().is_none_or(str::is_empty)
        {
            return Err("tool program retries require an idempotency_key".to_owned());
        }
        max_child_attempts = max_child_attempts
            .saturating_add(usize::try_from(step.retry_policy.max_attempts).unwrap_or(usize::MAX));
    }
    if max_child_attempts > request.budgets.max_child_runs {
        return Err("tool program retry attempts cannot exceed max_child_runs budget".to_owned());
    }
    build_execution_plan(request)?;
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
    // Clamped to u64::MAX first, so the narrowing cast cannot truncate.
    started_at.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

fn sha256_hex(payload: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(payload);
    hex::encode(hasher.finalize())
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

fn default_retry_max_attempts() -> u32 {
    1
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::super::tool_rpc::{ToolRpcResponse, ToolRpcStatus, TOOL_RPC_SCHEMA_VERSION};
    use super::{
        build_execution_plan, parse_and_validate_request, should_retry_tool_rpc_response,
        tool_program_job_completion, tool_program_job_completion_for_status, ToolProgramKind,
        ToolProgramStatus, ToolProgramStepRetryPolicy,
    };
    use crate::journal::ToolJobState;

    #[test]
    fn validates_declarative_program_shape() {
        let request = parse_and_validate_request(
            br#"{
                "schema_version": 1,
                "program_id": "program-a",
                "granted_tools": ["palyra.echo"],
                "steps": [
                    {"step_id": "echo", "tool": "palyra.echo", "input": {"text": "ok"}}
                ]
            }"#,
        )
        .expect("program should validate");

        assert_eq!(request.program_id, "program-a");
        assert_eq!(request.program_kind, ToolProgramKind::Declarative);
        assert_eq!(request.steps.len(), 1);
    }

    #[test]
    fn validates_python_rpc_script_call_plan() {
        let request = parse_and_validate_request(
            br#"{
                "schema_version": 1,
                "program_id": "program-script",
                "program_kind": "python_rpc_script",
                "granted_tools": ["palyra.fs.read_file", "palyra.fs.search"],
                "script": {
                    "source": "from palyra_tools import call_palyra_fs_read_file, call_palyra_fs_search\n",
                    "calls": [
                        {
                            "call_id": "read",
                            "tool_name": "palyra.fs.read_file",
                            "arguments": {"path": "README.md", "max_bytes": 1024},
                            "result_projection": "summary_only"
                        },
                        {
                            "call_id": "search",
                            "tool_name": "palyra.fs.search",
                            "arguments": {"query": "Palyra", "path": "."},
                            "depends_on": ["read"]
                        }
                    ]
                }
            }"#,
        )
        .expect("script program should validate");

        assert_eq!(request.program_kind, ToolProgramKind::PythonRpcScript);
        assert_eq!(request.steps.len(), 2);
        assert_eq!(request.steps[0].step_id, "read");
        assert_eq!(request.steps[0].tool, "palyra.fs.read_file");
        assert_eq!(request.steps[1].depends_on, vec!["read"]);
        let plan = build_execution_plan(&request).expect("script plan should build");
        assert_eq!(plan, vec![vec![0], vec![1]]);
    }

    #[test]
    fn rejects_ambiguous_python_rpc_script_shape() {
        let error = parse_and_validate_request(
            br#"{
                "schema_version": 1,
                "program_id": "program-script",
                "program_kind": "python_rpc_script",
                "granted_tools": ["palyra.echo"],
                "script": {
                    "source": "from palyra_tools import call_palyra_echo\n",
                    "calls": [
                        {"call_id": "echo", "tool_name": "palyra.echo"}
                    ]
                },
                "steps": [
                    {"step_id": "echo", "tool": "palyra.echo"}
                ]
            }"#,
        )
        .expect_err("script mode must not accept a second step source");

        assert!(error.contains("script.calls instead of steps"));
    }

    #[test]
    fn rejects_duplicate_step_ids_and_unknown_schema_versions() {
        let schema_error = parse_and_validate_request(
            br#"{"schema_version":2,"program_id":"bad","granted_tools":["palyra.echo"],"steps":[{"step_id":"a","tool":"palyra.echo"}]}"#,
        )
        .expect_err("unknown schema must fail closed");
        assert!(schema_error.contains("unsupported"));

        let duplicate_error = parse_and_validate_request(
            br#"{
                "schema_version": 1,
                "program_id": "bad",
                "granted_tools": ["palyra.echo"],
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
    fn validates_dag_dependencies_and_parallel_path_policy() {
        let request = parse_and_validate_request(
            br#"{
                "schema_version": 1,
                "program_id": "program-dag",
                "granted_tools": ["palyra.echo"],
                "steps": [
                    {
                        "step_id": "read-a",
                        "tool": "palyra.echo",
                        "parallelism": "path_scoped",
                        "path_scope": ["src/a"]
                    },
                    {
                        "step_id": "read-b",
                        "tool": "palyra.echo",
                        "parallelism": "path_scoped",
                        "path_scope": ["src/b"]
                    },
                    {
                        "step_id": "join",
                        "tool": "palyra.echo",
                        "depends_on": ["read-a", "read-b"]
                    }
                ]
            }"#,
        )
        .expect("dag program should validate");
        let plan = build_execution_plan(&request).expect("plan should build");
        assert_eq!(plan, vec![vec![0, 1], vec![2]]);

        let cycle_error = parse_and_validate_request(
            br#"{
                "schema_version": 1,
                "program_id": "program-cycle",
                "granted_tools": ["palyra.echo"],
                "steps": [
                    {"step_id": "a", "tool": "palyra.echo", "depends_on": ["b"]},
                    {"step_id": "b", "tool": "palyra.echo", "depends_on": ["a"]}
                ]
            }"#,
        )
        .expect_err("cycle must fail closed");
        assert!(cycle_error.contains("cycle"));
    }

    #[test]
    fn validates_retry_policy_budget_and_idempotency() {
        let missing_idempotency = parse_and_validate_request(
            br#"{
                "schema_version": 1,
                "program_id": "program-retry",
                "granted_tools": ["palyra.echo"],
                "steps": [
                    {
                        "step_id": "retry",
                        "tool": "palyra.echo",
                        "retry_policy": {"max_attempts": 2}
                    }
                ]
            }"#,
        )
        .expect_err("multi-attempt retry requires idempotency key");
        assert!(missing_idempotency.contains("idempotency_key"));

        let over_budget = parse_and_validate_request(
            br#"{
                "schema_version": 1,
                "program_id": "program-retry-budget",
                "budgets": {"max_child_runs": 2},
                "granted_tools": ["palyra.echo"],
                "steps": [
                    {
                        "step_id": "retry",
                        "tool": "palyra.echo",
                        "retry_policy": {"max_attempts": 3, "idempotency_key": "retry-1"}
                    }
                ]
            }"#,
        )
        .expect_err("declared attempts must fit child run budget");
        assert!(over_budget.contains("max_child_runs"));

        parse_and_validate_request(
            br#"{
                "schema_version": 1,
                "program_id": "program-retry-ok",
                "budgets": {"max_child_runs": 2},
                "granted_tools": ["palyra.echo"],
                "steps": [
                    {
                        "step_id": "retry",
                        "tool": "palyra.echo",
                        "retry_policy": {"max_attempts": 2, "idempotency_key": "retry-1"}
                    }
                ]
            }"#,
        )
        .expect("idempotent retry within child run budget should validate");
    }

    #[test]
    fn retry_predicate_only_retries_retryable_child_rpc_results() {
        let retry_policy =
            ToolProgramStepRetryPolicy { max_attempts: 2, idempotency_key: Some("idem".into()) };
        assert!(should_retry_tool_rpc_response(
            &rpc_response(ToolRpcStatus::Failed, false),
            &retry_policy,
            1
        ));
        assert!(should_retry_tool_rpc_response(
            &rpc_response(ToolRpcStatus::TimedOut, false),
            &retry_policy,
            1
        ));
        assert!(!should_retry_tool_rpc_response(
            &rpc_response(ToolRpcStatus::Denied, false),
            &retry_policy,
            1
        ));
        assert!(!should_retry_tool_rpc_response(
            &rpc_response(ToolRpcStatus::Failed, true),
            &retry_policy,
            1
        ));
        assert!(!should_retry_tool_rpc_response(
            &rpc_response(ToolRpcStatus::Failed, false),
            &retry_policy,
            2
        ));
    }

    #[test]
    fn tool_program_job_completion_marks_runtime_errors_failed() {
        let completed = tool_program_job_completion_for_status(ToolProgramStatus::Completed, "");
        assert_eq!(completed.next_state, ToolJobState::Completed);
        assert_eq!(completed.reason, "tool_program_finished");
        assert_eq!(completed.last_error, None);

        let failed = tool_program_job_completion(&Err("registry failed".to_owned()));
        assert_eq!(failed.next_state, ToolJobState::Failed);
        assert_eq!(failed.reason, "registry failed");
        assert_eq!(failed.last_error.as_deref(), Some("registry failed"));
    }

    #[test]
    fn tool_program_status_serializes_as_stable_snake_case() {
        let serialized =
            serde_json::to_string(&ToolProgramStatus::Cancelled).expect("status should serialize");
        assert_eq!(serialized, "\"cancelled\"");
    }

    fn rpc_response(status: ToolRpcStatus, approval_required: bool) -> ToolRpcResponse {
        ToolRpcResponse {
            schema_version: TOOL_RPC_SCHEMA_VERSION,
            call_id: "call-1".to_owned(),
            tool_name: "palyra.echo".to_owned(),
            status,
            success: status == ToolRpcStatus::Completed,
            decision_reason: "test".to_owned(),
            approval_required,
            output: json!({}),
            error: if status == ToolRpcStatus::Completed {
                String::new()
            } else {
                "failed".to_owned()
            },
            redacted_preview: String::new(),
            attestation: None,
        }
    }
}
