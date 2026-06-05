use std::{
    collections::BTreeSet,
    sync::{atomic::AtomicBool, Arc},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use palyra_common::tool_catalog::{
    sensitive_allowlisted_tool_names, SENSITIVE_CAPABILITY_POLICY_NAMES,
};
pub use palyra_common::tool_catalog::{
    tool_metadata, tool_policy_capability_names, tool_requires_approval, ToolCapability,
};
use palyra_policy::{
    evaluate_with_context, PolicyDecision, PolicyEvaluationConfig, PolicyRequest,
    PolicyRequestContext,
};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tracing::warn;
use ulid::Ulid;

use crate::sandbox_runner::{
    background_process_status_by_pid, process_runner_executor_name,
    run_constrained_process_with_cancellation, stop_background_process_by_pid,
    EgressEnforcementMode, SandboxProcessRunErrorKind, SandboxProcessRunnerPolicy,
};
use crate::wasm_plugin_runner::{run_wasm_plugin, WasmPluginRunErrorKind, WasmPluginRunnerPolicy};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCallConfig {
    pub allowed_tools: Vec<String>,
    pub max_calls_per_run: u32,
    pub execution_timeout_ms: u64,
    pub process_runner: SandboxProcessRunnerPolicy,
    pub wasm_runtime: WasmPluginRunnerPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolDecision {
    pub allowed: bool,
    pub reason: String,
    pub approval_required: bool,
    pub policy_enforced: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolRequestContext {
    pub principal: String,
    pub device_id: Option<String>,
    pub channel: Option<String>,
    pub session_id: Option<String>,
    pub run_id: Option<String>,
    pub skill_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolAttestation {
    pub attestation_id: String,
    pub execution_sha256: String,
    pub executed_at_unix_ms: i64,
    pub timed_out: bool,
    pub executor: String,
    pub sandbox_enforcement: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolExecutionOutcome {
    pub success: bool,
    pub output_json: Vec<u8>,
    pub error: String,
    pub attestation: ToolAttestation,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ToolCallPolicySnapshot {
    pub allowed_tools: Vec<String>,
    pub max_calls_per_run: u32,
    pub execution_timeout_ms: u64,
    pub process_runner: ProcessRunnerPolicySnapshot,
    pub wasm_runtime: WasmRuntimePolicySnapshot,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProcessRunnerPolicySnapshot {
    pub enabled: bool,
    pub tier: String,
    pub workspace_root: String,
    pub allowed_executables: Vec<String>,
    pub allow_interpreters: bool,
    pub egress_enforcement_mode: String,
    pub allowed_egress_hosts: Vec<String>,
    pub allowed_dns_suffixes: Vec<String>,
    pub cpu_time_limit_ms: u64,
    pub memory_limit_bytes: u64,
    pub max_output_bytes: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct WasmRuntimePolicySnapshot {
    pub enabled: bool,
    pub allow_inline_modules: bool,
    pub max_module_size_bytes: u64,
    pub fuel_budget: u64,
    pub max_memory_bytes: u64,
    pub max_table_elements: u64,
    pub max_instances: u64,
    pub allowed_http_hosts: Vec<String>,
    pub allowed_secrets: Vec<String>,
    pub allowed_storage_prefixes: Vec<String>,
    pub allowed_channels: Vec<String>,
}

const BUDGET_DENY_REASON: &str = "tool execution budget exhausted for run";
const UNSUPPORTED_TOOL_DENY_REASON: &str =
    "tool is allowlisted but unsupported by runtime executor";
const TOOL_MAX_SLEEP_MS: u64 = 30_000;
const TOOL_INPUT_TOO_LARGE_ERROR_CODE: &str = "quota/tool_input_too_large";
const MAX_ECHO_TOOL_INPUT_BYTES: usize = 16 * 1024;
const MAX_SLEEP_TOOL_INPUT_BYTES: usize = 8 * 1024;
const MAX_MEMORY_STATUS_TOOL_INPUT_BYTES: usize = 16 * 1024;
const MAX_MEMORY_SEARCH_TOOL_INPUT_BYTES: usize = 64 * 1024;
const MAX_MEMORY_RECALL_TOOL_INPUT_BYTES: usize = 64 * 1024;
const MAX_MEMORY_SESSION_SEARCH_TOOL_INPUT_BYTES: usize = 64 * 1024;
const MAX_MEMORY_RETAIN_TOOL_INPUT_BYTES: usize = 64 * 1024;
const MAX_MEMORY_DELETE_TOOL_INPUT_BYTES: usize = 16 * 1024;
const MAX_MEMORY_REPLACE_TOOL_INPUT_BYTES: usize = 64 * 1024;
const MAX_MEMORY_REFLECT_TOOL_INPUT_BYTES: usize = 64 * 1024;
const MAX_ROUTINES_QUERY_TOOL_INPUT_BYTES: usize = 64 * 1024;
const MAX_ROUTINES_CONTROL_TOOL_INPUT_BYTES: usize = 128 * 1024;
const MAX_DELEGATION_QUERY_TOOL_INPUT_BYTES: usize = 64 * 1024;
const MAX_DELEGATION_CONTROL_TOOL_INPUT_BYTES: usize = 64 * 1024;
const MAX_HTTP_FETCH_TOOL_INPUT_BYTES: usize = 64 * 1024;
const MAX_PROCESS_RUNNER_TOOL_INPUT_BYTES: usize = 128 * 1024;
const MAX_TOOL_PROGRAM_RUN_TOOL_INPUT_BYTES: usize = 256 * 1024;
const MAX_WORKSPACE_READ_FILE_TOOL_INPUT_BYTES: usize = 16 * 1024;
const MAX_WORKSPACE_LIST_DIR_TOOL_INPUT_BYTES: usize = 16 * 1024;
const MAX_WORKSPACE_SEARCH_TOOL_INPUT_BYTES: usize = 16 * 1024;
const MAX_WORKSPACE_PATCH_TOOL_INPUT_BYTES: usize = 256 * 1024;
const MAX_OS_FILE_TOOL_INPUT_BYTES: usize = 384 * 1024;
const MAX_BROWSER_TOOL_INPUT_BYTES: usize = 128 * 1024;
const MAX_ARTIFACT_READ_TOOL_INPUT_BYTES: usize = 16 * 1024;
const MAX_WASM_PLUGIN_TOOL_INPUT_BYTES: usize = 448 * 1024;

pub fn tool_policy_snapshot(config: &ToolCallConfig) -> ToolCallPolicySnapshot {
    ToolCallPolicySnapshot {
        allowed_tools: config.allowed_tools.clone(),
        max_calls_per_run: config.max_calls_per_run,
        execution_timeout_ms: config.execution_timeout_ms,
        process_runner: ProcessRunnerPolicySnapshot {
            enabled: config.process_runner.enabled,
            tier: config.process_runner.tier.as_str().to_owned(),
            workspace_root: config.process_runner.workspace_root.to_string_lossy().into_owned(),
            allowed_executables: config.process_runner.allowed_executables.clone(),
            allow_interpreters: config.process_runner.allow_interpreters,
            egress_enforcement_mode: config
                .process_runner
                .egress_enforcement_mode
                .as_str()
                .to_owned(),
            allowed_egress_hosts: config.process_runner.allowed_egress_hosts.clone(),
            allowed_dns_suffixes: config.process_runner.allowed_dns_suffixes.clone(),
            cpu_time_limit_ms: config.process_runner.cpu_time_limit_ms,
            memory_limit_bytes: config.process_runner.memory_limit_bytes,
            max_output_bytes: config.process_runner.max_output_bytes,
        },
        wasm_runtime: WasmRuntimePolicySnapshot {
            enabled: config.wasm_runtime.enabled,
            allow_inline_modules: config.wasm_runtime.allow_inline_modules,
            max_module_size_bytes: config.wasm_runtime.max_module_size_bytes,
            fuel_budget: config.wasm_runtime.fuel_budget,
            max_memory_bytes: config.wasm_runtime.max_memory_bytes,
            max_table_elements: config.wasm_runtime.max_table_elements,
            max_instances: config.wasm_runtime.max_instances,
            allowed_http_hosts: config.wasm_runtime.allowed_http_hosts.clone(),
            allowed_secrets: config.wasm_runtime.allowed_secrets.clone(),
            allowed_storage_prefixes: config.wasm_runtime.allowed_storage_prefixes.clone(),
            allowed_channels: config.wasm_runtime.allowed_channels.clone(),
        },
    }
}

pub fn decide_tool_call(
    config: &ToolCallConfig,
    remaining_budget: &mut u32,
    request_context: &ToolRequestContext,
    tool_name: &str,
    allow_sensitive_tools: bool,
) -> ToolDecision {
    let approval_required = tool_requires_approval(tool_name);
    if *remaining_budget == 0 {
        return ToolDecision {
            allowed: false,
            reason: BUDGET_DENY_REASON.to_owned(),
            approval_required: false,
            policy_enforced: true,
        };
    }

    let policy_request = PolicyRequest {
        principal: request_context.principal.clone(),
        action: "tool.execute".to_owned(),
        resource: format!("tool:{tool_name}"),
    };
    let policy_request_context = PolicyRequestContext {
        device_id: request_context.device_id.clone(),
        channel: request_context.channel.clone(),
        session_id: request_context.session_id.clone(),
        run_id: request_context.run_id.clone(),
        tool_name: Some(tool_name.to_ascii_lowercase()),
        skill_id: request_context.skill_id.clone(),
        capabilities: tool_policy_capability_names(tool_name),
    };
    let policy_allowlisted_tools =
        allowlisted_tools_with_compat_aliases(config.allowed_tools.as_slice());
    let policy_config = PolicyEvaluationConfig {
        allowlisted_tools: policy_allowlisted_tools.clone(),
        allow_sensitive_tools,
        sensitive_tool_names: sensitive_allowlisted_tool_names(policy_allowlisted_tools.as_slice()),
        sensitive_capability_names: SENSITIVE_CAPABILITY_POLICY_NAMES
            .iter()
            .map(|value| (*value).to_string())
            .collect(),
        ..PolicyEvaluationConfig::default()
    };
    let policy_evaluation =
        match evaluate_with_context(&policy_request, &policy_request_context, &policy_config) {
            Ok(evaluation) => evaluation,
            Err(error) => {
                return ToolDecision {
                    allowed: false,
                    reason: format!("policy evaluation failed safely: {error}"),
                    approval_required,
                    policy_enforced: true,
                };
            }
        };
    if let PolicyDecision::DenyByDefault { reason } = policy_evaluation.decision {
        let approval_required =
            approval_required && reason.contains("explicit user approval required");
        return ToolDecision {
            allowed: false,
            reason: format_policy_reason(
                reason.as_str(),
                policy_evaluation.explanation.matched_policy_ids.as_slice(),
                policy_evaluation.explanation.diagnostics_errors.as_slice(),
            ),
            approval_required,
            policy_enforced: true,
        };
    }

    if !is_runtime_supported_tool(tool_name) {
        return ToolDecision {
            allowed: false,
            reason: UNSUPPORTED_TOOL_DENY_REASON.to_owned(),
            approval_required: false,
            policy_enforced: true,
        };
    }

    *remaining_budget = remaining_budget.saturating_sub(1);
    ToolDecision {
        allowed: true,
        reason: format_policy_reason(
            "tool is allowlisted by Cedar runtime policy",
            policy_evaluation.explanation.matched_policy_ids.as_slice(),
            policy_evaluation.explanation.diagnostics_errors.as_slice(),
        ),
        approval_required,
        policy_enforced: true,
    }
}

fn allowlisted_tools_with_compat_aliases(allowed_tools: &[String]) -> Vec<String> {
    let mut names = BTreeSet::new();
    for tool_name in allowed_tools {
        let normalized = tool_name.trim().to_ascii_lowercase();
        if normalized.is_empty() {
            continue;
        }
        names.insert(normalized.clone());
        match normalized.as_str() {
            "palyra.memory.session_search" | "palyra.session_search" => {
                names.insert("palyra.memory.session_search".to_owned());
                names.insert("palyra.session_search".to_owned());
            }
            "palyra.memory.retain" | "palyra.retain" => {
                names.insert("palyra.memory.retain".to_owned());
                names.insert("palyra.retain".to_owned());
            }
            "palyra.process.run" => {
                names.insert("palyra.process.stop".to_owned());
                names.insert("palyra.process.status".to_owned());
                names.insert("palyra.process.list".to_owned());
            }
            _ => {}
        }
    }
    names.into_iter().collect()
}

fn format_policy_reason(
    base_reason: &str,
    matched_policy_ids: &[String],
    diagnostics_errors: &[String],
) -> String {
    if !diagnostics_errors.is_empty() {
        return format!("{base_reason}; diagnostics_errors={}", diagnostics_errors.join("|"));
    }
    if !matched_policy_ids.is_empty() {
        return format!("{base_reason}; matched_policies={}", matched_policy_ids.join(","));
    }
    base_reason.to_owned()
}

pub fn denied_execution_outcome(
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    reason: &str,
) -> ToolExecutionOutcome {
    build_execution_outcome(
        proposal_id,
        tool_name,
        input_json,
        ToolExecutionRawResult {
            success: false,
            output_json: b"{}".to_vec(),
            error: reason.to_owned(),
            timed_out: false,
            executor: "policy".to_owned(),
            sandbox_enforcement: "none".to_owned(),
        },
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_tool_execution_outcome(
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    success: bool,
    output_json: Vec<u8>,
    error: String,
    timed_out: bool,
    executor: String,
    sandbox_enforcement: String,
) -> ToolExecutionOutcome {
    build_execution_outcome(
        proposal_id,
        tool_name,
        input_json,
        ToolExecutionRawResult {
            success,
            output_json,
            error,
            timed_out,
            executor,
            sandbox_enforcement,
        },
    )
}

pub async fn execute_tool_call(
    config: &ToolCallConfig,
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
) -> ToolExecutionOutcome {
    execute_tool_call_with_cancellation(config, proposal_id, tool_name, input_json, None).await
}

pub async fn execute_tool_call_with_cancellation(
    config: &ToolCallConfig,
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    cancellation_requested: Option<Arc<AtomicBool>>,
) -> ToolExecutionOutcome {
    if let Some(raw) = reject_oversized_tool_input(config, tool_name, input_json) {
        return build_execution_outcome(proposal_id, tool_name, input_json, raw);
    }

    let raw = if tool_name == "palyra.plugin.run" {
        run_allowlisted_tool_with_cancellation(
            config,
            tool_name,
            input_json,
            cancellation_requested,
        )
        .await
    } else {
        let timeout = Duration::from_millis(config.execution_timeout_ms);
        match tokio::time::timeout(
            timeout,
            run_allowlisted_tool_with_cancellation(
                config,
                tool_name,
                input_json,
                cancellation_requested,
            ),
        )
        .await
        {
            Ok(raw) => raw,
            Err(_) => ToolExecutionRawResult {
                success: false,
                output_json: b"{}".to_vec(),
                error: format!("tool execution timed out after {}ms", config.execution_timeout_ms),
                timed_out: true,
                executor: tool_executor_name(config, tool_name),
                sandbox_enforcement: sandbox_enforcement_for_tool(config, tool_name),
            },
        }
    };

    build_execution_outcome(proposal_id, tool_name, input_json, raw)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ToolExecutionRawResult {
    success: bool,
    output_json: Vec<u8>,
    error: String,
    timed_out: bool,
    executor: String,
    sandbox_enforcement: String,
}

fn build_execution_outcome(
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    raw: ToolExecutionRawResult,
) -> ToolExecutionOutcome {
    let executed_at_unix_ms = current_unix_ms();
    let output_json = normalize_failure_output_json(tool_name, &raw);
    let execution_sha256 = compute_execution_hash(
        proposal_id,
        tool_name,
        input_json,
        raw.success,
        output_json.as_slice(),
        raw.error.as_str(),
        raw.timed_out,
        raw.executor.as_str(),
        raw.sandbox_enforcement.as_str(),
        executed_at_unix_ms,
    );
    ToolExecutionOutcome {
        success: raw.success,
        output_json,
        error: raw.error,
        attestation: ToolAttestation {
            attestation_id: Ulid::new().to_string(),
            execution_sha256,
            executed_at_unix_ms,
            timed_out: raw.timed_out,
            executor: raw.executor,
            sandbox_enforcement: raw.sandbox_enforcement,
        },
    }
}

fn normalize_failure_output_json(tool_name: &str, raw: &ToolExecutionRawResult) -> Vec<u8> {
    if raw.success || !tool_output_json_is_empty_object(raw.output_json.as_slice()) {
        return raw.output_json.clone();
    }
    failed_tool_output_json(
        tool_name,
        raw.error.as_str(),
        raw.timed_out,
        raw.executor.as_str(),
        raw.sandbox_enforcement.as_str(),
    )
}

pub(crate) fn tool_output_json_is_empty_object(output_json: &[u8]) -> bool {
    std::str::from_utf8(output_json).map(|raw| raw.trim() == "{}").unwrap_or(false)
}

pub(crate) fn failed_tool_output_json(
    tool_name: &str,
    error: &str,
    timed_out: bool,
    executor: &str,
    sandbox_enforcement: &str,
) -> Vec<u8> {
    let mut payload = json!({
        "success": false,
        "tool": tool_name,
        "error": error,
        "recovery_hint": tool_failure_recovery_hint(tool_name, error, timed_out),
        "timed_out": timed_out,
        "executor": executor,
        "sandbox_enforcement": sandbox_enforcement,
    });
    if tool_name == "palyra.fs.apply_patch" {
        if let Some(object) = payload.as_object_mut() {
            object.insert(
                "grammar_hint".to_owned(),
                json!("Retry with one complete Palyra patch document starting with '*** Begin Patch' and ending with exactly one '*** End Patch'."),
            );
        }
    }
    serde_json::to_vec(&payload)
        .unwrap_or_else(|_| br#"{"success":false,"error":"tool failed"}"#.to_vec())
}

fn tool_failure_recovery_hint(tool_name: &str, error: &str, timed_out: bool) -> String {
    if timed_out {
        return "Retry with a smaller operation, narrower scope, or a larger configured tool timeout."
            .to_owned();
    }
    if error.contains(TOOL_INPUT_TOO_LARGE_ERROR_CODE) {
        return "Reduce the tool input size and retry with smaller chunks.".to_owned();
    }
    if error.contains("requires gateway") {
        return "Retry through the normal gateway runtime path for this tool; the generic executor lacks the required runtime context.".to_owned();
    }
    if error.contains("disabled by runtime policy") {
        return "Enable the relevant runtime policy or choose a tool that is available under the current policy.".to_owned();
    }
    match tool_name {
        "palyra.fs.apply_patch" => {
            "Inspect the patch error, read the current file when context is stale, and retry with a smaller complete patch.".to_owned()
        }
        "palyra.process.run" | "palyra.process.stop" | "palyra.process.status" | "palyra.process.list" => {
            "Inspect command, args, cwd, allowlist, and resource limits; retry with a portable workspace-scoped process request.".to_owned()
        }
        "palyra.plugin.run" => {
            "Inspect the plugin error and retry with a smaller module or allowed capability set.".to_owned()
        }
        _ => "Inspect the error field, adjust the request or policy, and retry.".to_owned(),
    }
}

async fn run_allowlisted_tool_with_cancellation(
    config: &ToolCallConfig,
    tool_name: &str,
    input_json: &[u8],
    cancellation_requested: Option<Arc<AtomicBool>>,
) -> ToolExecutionRawResult {
    match tool_name {
        "palyra.echo" => match execute_echo_tool(input_json) {
            Ok(output_json) => ToolExecutionRawResult {
                success: true,
                output_json,
                error: String::new(),
                timed_out: false,
                executor: "builtin".to_owned(),
                sandbox_enforcement: "none".to_owned(),
            },
            Err(error) => ToolExecutionRawResult {
                success: false,
                output_json: b"{}".to_vec(),
                error,
                timed_out: false,
                executor: "builtin".to_owned(),
                sandbox_enforcement: "none".to_owned(),
            },
        },
        "palyra.sleep" => match execute_sleep_tool(input_json).await {
            Ok(output_json) => ToolExecutionRawResult {
                success: true,
                output_json,
                error: String::new(),
                timed_out: false,
                executor: "builtin".to_owned(),
                sandbox_enforcement: "none".to_owned(),
            },
            Err(error) => ToolExecutionRawResult {
                success: false,
                output_json: b"{}".to_vec(),
                error,
                timed_out: false,
                executor: "builtin".to_owned(),
                sandbox_enforcement: "none".to_owned(),
            },
        },
        "palyra.memory.status" => ToolExecutionRawResult {
            success: false,
            output_json: b"{}".to_vec(),
            error: "palyra.memory.status requires gateway memory runtime context".to_owned(),
            timed_out: false,
            executor: "gateway_runtime".to_owned(),
            sandbox_enforcement: "none".to_owned(),
        },
        "palyra.memory.search" => ToolExecutionRawResult {
            success: false,
            output_json: b"{}".to_vec(),
            error: "palyra.memory.search requires gateway memory runtime context".to_owned(),
            timed_out: false,
            executor: "gateway_runtime".to_owned(),
            sandbox_enforcement: "none".to_owned(),
        },
        "palyra.memory.recall" => ToolExecutionRawResult {
            success: false,
            output_json: b"{}".to_vec(),
            error: "palyra.memory.recall requires gateway memory runtime context".to_owned(),
            timed_out: false,
            executor: "gateway_runtime".to_owned(),
            sandbox_enforcement: "none".to_owned(),
        },
        "palyra.memory.session_search" | "palyra.session_search" => ToolExecutionRawResult {
            success: false,
            output_json: b"{}".to_vec(),
            error: format!("{tool_name} requires gateway memory runtime context"),
            timed_out: false,
            executor: "gateway_runtime".to_owned(),
            sandbox_enforcement: "none".to_owned(),
        },
        "palyra.memory.retain" | "palyra.retain" => ToolExecutionRawResult {
            success: false,
            output_json: b"{}".to_vec(),
            error: format!("{tool_name} requires gateway memory runtime context"),
            timed_out: false,
            executor: "gateway_runtime".to_owned(),
            sandbox_enforcement: "none".to_owned(),
        },
        "palyra.memory.delete" => ToolExecutionRawResult {
            success: false,
            output_json: b"{}".to_vec(),
            error: "palyra.memory.delete requires gateway memory runtime context".to_owned(),
            timed_out: false,
            executor: "gateway_runtime".to_owned(),
            sandbox_enforcement: "none".to_owned(),
        },
        "palyra.memory.replace" => ToolExecutionRawResult {
            success: false,
            output_json: b"{}".to_vec(),
            error: "palyra.memory.replace requires gateway memory runtime context".to_owned(),
            timed_out: false,
            executor: "gateway_runtime".to_owned(),
            sandbox_enforcement: "none".to_owned(),
        },
        "palyra.memory.reflect" => ToolExecutionRawResult {
            success: false,
            output_json: b"{}".to_vec(),
            error: "palyra.memory.reflect requires gateway memory runtime context".to_owned(),
            timed_out: false,
            executor: "gateway_runtime".to_owned(),
            sandbox_enforcement: "none".to_owned(),
        },
        "palyra.routines.query" | "palyra.routines.control" => ToolExecutionRawResult {
            success: false,
            output_json: b"{}".to_vec(),
            error: format!("{tool_name} requires gateway routines runtime context"),
            timed_out: false,
            executor: "routines_runtime".to_owned(),
            sandbox_enforcement: "none".to_owned(),
        },
        "palyra.delegation.query" | "palyra.delegation.control" => ToolExecutionRawResult {
            success: false,
            output_json: b"{}".to_vec(),
            error: format!("{tool_name} requires gateway delegation runtime context"),
            timed_out: false,
            executor: "delegation_runtime".to_owned(),
            sandbox_enforcement: "delegation_scope".to_owned(),
        },
        "palyra.artifact.read" => ToolExecutionRawResult {
            success: false,
            output_json: b"{}".to_vec(),
            error: "palyra.artifact.read requires gateway artifact runtime context".to_owned(),
            timed_out: false,
            executor: "gateway_artifacts".to_owned(),
            sandbox_enforcement: "artifact_scope".to_owned(),
        },
        "palyra.http.fetch" => ToolExecutionRawResult {
            success: false,
            output_json: b"{}".to_vec(),
            error: "palyra.http.fetch requires gateway HTTP fetch runtime context".to_owned(),
            timed_out: false,
            executor: "gateway_http_fetch".to_owned(),
            sandbox_enforcement: "ssrf_guard".to_owned(),
        },
        "palyra.process.run" => {
            execute_process_runner_tool(config, input_json, cancellation_requested).await
        }
        "palyra.process.stop" | "palyra.process.status" => {
            execute_process_lifecycle_tool(config, tool_name, input_json).await
        }
        "palyra.process.list" => ToolExecutionRawResult {
            success: false,
            output_json: b"{}".to_vec(),
            error: "palyra.process.list requires gateway process runtime context".to_owned(),
            timed_out: false,
            executor: process_runner_executor_name(&config.process_runner),
            sandbox_enforcement: sandbox_enforcement_for_tool(config, tool_name),
        },
        "palyra.tool_program.run" => ToolExecutionRawResult {
            success: false,
            output_json: b"{}".to_vec(),
            error: "palyra.tool_program.run requires gateway tool program runtime context"
                .to_owned(),
            timed_out: false,
            executor: "tool_program_runtime".to_owned(),
            sandbox_enforcement: "nested_tool_policy".to_owned(),
        },
        "palyra.fs.apply_patch" => ToolExecutionRawResult {
            success: false,
            output_json: b"{}".to_vec(),
            error: "palyra.fs.apply_patch requires gateway workspace context".to_owned(),
            timed_out: false,
            executor: "workspace_patch".to_owned(),
            sandbox_enforcement: "workspace_roots".to_owned(),
        },
        "palyra.fs.read_file" => ToolExecutionRawResult {
            success: false,
            output_json: b"{}".to_vec(),
            error: "palyra.fs.read_file requires gateway workspace context".to_owned(),
            timed_out: false,
            executor: "workspace_file".to_owned(),
            sandbox_enforcement: "workspace_roots".to_owned(),
        },
        "palyra.fs.list_dir" => ToolExecutionRawResult {
            success: false,
            output_json: b"{}".to_vec(),
            error: "palyra.fs.list_dir requires gateway workspace context".to_owned(),
            timed_out: false,
            executor: "workspace_file".to_owned(),
            sandbox_enforcement: "workspace_roots".to_owned(),
        },
        "palyra.fs.search" => ToolExecutionRawResult {
            success: false,
            output_json: b"{}".to_vec(),
            error: "palyra.fs.search requires gateway workspace context".to_owned(),
            timed_out: false,
            executor: "workspace_file".to_owned(),
            sandbox_enforcement: "workspace_roots".to_owned(),
        },
        "palyra.fs.os_file" => ToolExecutionRawResult {
            success: false,
            output_json: b"{}".to_vec(),
            error: "palyra.fs.os_file requires gateway OS file runtime context".to_owned(),
            timed_out: false,
            executor: "os_file".to_owned(),
            sandbox_enforcement: "approved_os_paths".to_owned(),
        },
        "palyra.browser.session.create"
        | "palyra.browser.session.close"
        | "palyra.browser.navigate"
        | "palyra.browser.reload"
        | "palyra.browser.click"
        | "palyra.browser.type"
        | "palyra.browser.fill"
        | "palyra.browser.upload"
        | "palyra.browser.press"
        | "palyra.browser.select"
        | "palyra.browser.viewport"
        | "palyra.browser.highlight"
        | "palyra.browser.scroll"
        | "palyra.browser.wait_for"
        | "palyra.browser.title"
        | "palyra.browser.screenshot"
        | "palyra.browser.pdf"
        | "palyra.browser.observe"
        | "palyra.browser.storage"
        | "palyra.browser.network_log"
        | "palyra.browser.console_log"
        | "palyra.browser.reset_state"
        | "palyra.browser.tabs.list"
        | "palyra.browser.tabs.open"
        | "palyra.browser.tabs.switch"
        | "palyra.browser.tabs.close"
        | "palyra.browser.permissions.get"
        | "palyra.browser.permissions.set"
        | "palyra.browser.downloads.list"
        | "palyra.browser.downloads.get" => ToolExecutionRawResult {
            success: false,
            output_json: b"{}".to_vec(),
            error: "palyra.browser.* requires gateway browser broker runtime context".to_owned(),
            timed_out: false,
            executor: "browser_broker".to_owned(),
            sandbox_enforcement: "browser_service".to_owned(),
        },
        "palyra.plugin.run" => execute_wasm_plugin_tool(config, input_json).await,
        _ => ToolExecutionRawResult {
            success: false,
            output_json: b"{}".to_vec(),
            error: "allowlisted tool is not implemented by runtime executor".to_owned(),
            timed_out: false,
            executor: "builtin".to_owned(),
            sandbox_enforcement: "none".to_owned(),
        },
    }
}

fn is_runtime_supported_tool(tool_name: &str) -> bool {
    matches!(
        tool_name,
        "palyra.echo"
            | "palyra.sleep"
            | "palyra.memory.status"
            | "palyra.memory.search"
            | "palyra.memory.recall"
            | "palyra.memory.session_search"
            | "palyra.session_search"
            | "palyra.memory.retain"
            | "palyra.retain"
            | "palyra.memory.delete"
            | "palyra.memory.replace"
            | "palyra.memory.reflect"
            | "palyra.routines.query"
            | "palyra.routines.control"
            | "palyra.delegation.query"
            | "palyra.delegation.control"
            | "palyra.artifact.read"
            | "palyra.http.fetch"
            | "palyra.process.run"
            | "palyra.process.stop"
            | "palyra.process.status"
            | "palyra.process.list"
            | "palyra.tool_program.run"
            | "palyra.fs.read_file"
            | "palyra.fs.list_dir"
            | "palyra.fs.search"
            | "palyra.fs.apply_patch"
            | "palyra.fs.os_file"
            | "palyra.browser.session.create"
            | "palyra.browser.session.close"
            | "palyra.browser.navigate"
            | "palyra.browser.reload"
            | "palyra.browser.click"
            | "palyra.browser.type"
            | "palyra.browser.fill"
            | "palyra.browser.upload"
            | "palyra.browser.press"
            | "palyra.browser.select"
            | "palyra.browser.viewport"
            | "palyra.browser.highlight"
            | "palyra.browser.scroll"
            | "palyra.browser.wait_for"
            | "palyra.browser.title"
            | "palyra.browser.screenshot"
            | "palyra.browser.pdf"
            | "palyra.browser.observe"
            | "palyra.browser.storage"
            | "palyra.browser.network_log"
            | "palyra.browser.console_log"
            | "palyra.browser.reset_state"
            | "palyra.browser.tabs.list"
            | "palyra.browser.tabs.open"
            | "palyra.browser.tabs.switch"
            | "palyra.browser.tabs.close"
            | "palyra.browser.permissions.get"
            | "palyra.browser.permissions.set"
            | "palyra.browser.downloads.list"
            | "palyra.browser.downloads.get"
            | "palyra.plugin.run"
    )
}

fn tool_executor_name(config: &ToolCallConfig, tool_name: &str) -> String {
    if matches!(
        tool_name,
        "palyra.process.run"
            | "palyra.process.stop"
            | "palyra.process.status"
            | "palyra.process.list"
    ) {
        process_runner_executor_name(&config.process_runner)
    } else if tool_name == "palyra.tool_program.run" {
        "tool_program_runtime".to_owned()
    } else if matches!(tool_name, "palyra.fs.read_file" | "palyra.fs.list_dir" | "palyra.fs.search")
    {
        "workspace_file".to_owned()
    } else if tool_name == "palyra.fs.apply_patch" {
        "workspace_patch".to_owned()
    } else if tool_name == "palyra.fs.os_file" {
        "os_file".to_owned()
    } else if tool_name == "palyra.http.fetch" {
        "gateway_http_fetch".to_owned()
    } else if tool_name.starts_with("palyra.browser.") {
        "browser_broker".to_owned()
    } else if matches!(
        tool_name,
        "palyra.memory.status"
            | "palyra.memory.search"
            | "palyra.memory.recall"
            | "palyra.memory.session_search"
            | "palyra.session_search"
            | "palyra.memory.retain"
            | "palyra.retain"
            | "palyra.memory.delete"
            | "palyra.memory.replace"
            | "palyra.memory.reflect"
    ) {
        "gateway_runtime".to_owned()
    } else if matches!(tool_name, "palyra.routines.query" | "palyra.routines.control") {
        "routines_runtime".to_owned()
    } else if matches!(tool_name, "palyra.delegation.query" | "palyra.delegation.control") {
        "delegation_runtime".to_owned()
    } else if tool_name == "palyra.artifact.read" {
        "gateway_artifacts".to_owned()
    } else if tool_name == "palyra.plugin.run" {
        "sandbox_tier_a".to_owned()
    } else {
        "builtin".to_owned()
    }
}

fn tool_input_limit_bytes(tool_name: &str) -> usize {
    match tool_name {
        "palyra.echo" => MAX_ECHO_TOOL_INPUT_BYTES,
        "palyra.sleep" => MAX_SLEEP_TOOL_INPUT_BYTES,
        "palyra.memory.status" => MAX_MEMORY_STATUS_TOOL_INPUT_BYTES,
        "palyra.memory.search" => MAX_MEMORY_SEARCH_TOOL_INPUT_BYTES,
        "palyra.memory.recall" => MAX_MEMORY_RECALL_TOOL_INPUT_BYTES,
        "palyra.memory.session_search" | "palyra.session_search" => {
            MAX_MEMORY_SESSION_SEARCH_TOOL_INPUT_BYTES
        }
        "palyra.memory.retain" | "palyra.retain" => MAX_MEMORY_RETAIN_TOOL_INPUT_BYTES,
        "palyra.memory.delete" => MAX_MEMORY_DELETE_TOOL_INPUT_BYTES,
        "palyra.memory.replace" => MAX_MEMORY_REPLACE_TOOL_INPUT_BYTES,
        "palyra.memory.reflect" => MAX_MEMORY_REFLECT_TOOL_INPUT_BYTES,
        "palyra.routines.query" => MAX_ROUTINES_QUERY_TOOL_INPUT_BYTES,
        "palyra.routines.control" => MAX_ROUTINES_CONTROL_TOOL_INPUT_BYTES,
        "palyra.delegation.query" => MAX_DELEGATION_QUERY_TOOL_INPUT_BYTES,
        "palyra.delegation.control" => MAX_DELEGATION_CONTROL_TOOL_INPUT_BYTES,
        "palyra.artifact.read" => MAX_ARTIFACT_READ_TOOL_INPUT_BYTES,
        "palyra.http.fetch" => MAX_HTTP_FETCH_TOOL_INPUT_BYTES,
        "palyra.process.run"
        | "palyra.process.stop"
        | "palyra.process.status"
        | "palyra.process.list" => MAX_PROCESS_RUNNER_TOOL_INPUT_BYTES,
        "palyra.tool_program.run" => MAX_TOOL_PROGRAM_RUN_TOOL_INPUT_BYTES,
        "palyra.fs.read_file" => MAX_WORKSPACE_READ_FILE_TOOL_INPUT_BYTES,
        "palyra.fs.list_dir" => MAX_WORKSPACE_LIST_DIR_TOOL_INPUT_BYTES,
        "palyra.fs.search" => MAX_WORKSPACE_SEARCH_TOOL_INPUT_BYTES,
        "palyra.fs.apply_patch" => MAX_WORKSPACE_PATCH_TOOL_INPUT_BYTES,
        "palyra.fs.os_file" => MAX_OS_FILE_TOOL_INPUT_BYTES,
        "palyra.browser.session.create"
        | "palyra.browser.session.close"
        | "palyra.browser.navigate"
        | "palyra.browser.reload"
        | "palyra.browser.click"
        | "palyra.browser.type"
        | "palyra.browser.fill"
        | "palyra.browser.upload"
        | "palyra.browser.press"
        | "palyra.browser.select"
        | "palyra.browser.viewport"
        | "palyra.browser.highlight"
        | "palyra.browser.scroll"
        | "palyra.browser.wait_for"
        | "palyra.browser.title"
        | "palyra.browser.screenshot"
        | "palyra.browser.pdf"
        | "palyra.browser.observe"
        | "palyra.browser.storage"
        | "palyra.browser.network_log"
        | "palyra.browser.console_log"
        | "palyra.browser.reset_state"
        | "palyra.browser.tabs.list"
        | "palyra.browser.tabs.open"
        | "palyra.browser.tabs.switch"
        | "palyra.browser.tabs.close"
        | "palyra.browser.permissions.get"
        | "palyra.browser.permissions.set"
        | "palyra.browser.downloads.list"
        | "palyra.browser.downloads.get" => MAX_BROWSER_TOOL_INPUT_BYTES,
        "palyra.plugin.run" => MAX_WASM_PLUGIN_TOOL_INPUT_BYTES,
        _ => MAX_MEMORY_SEARCH_TOOL_INPUT_BYTES,
    }
}

fn sandbox_enforcement_for_tool(config: &ToolCallConfig, tool_name: &str) -> String {
    if matches!(
        tool_name,
        "palyra.process.run"
            | "palyra.process.stop"
            | "palyra.process.status"
            | "palyra.process.list"
    ) {
        if crate::sandbox_runner::process_runner_allows_host_access(&config.process_runner) {
            "host_access".to_owned()
        } else {
            config.process_runner.egress_enforcement_mode.as_str().to_owned()
        }
    } else if tool_name == "palyra.tool_program.run" {
        "nested_tool_policy".to_owned()
    } else if matches!(
        tool_name,
        "palyra.fs.read_file" | "palyra.fs.list_dir" | "palyra.fs.search" | "palyra.fs.apply_patch"
    ) {
        "workspace_roots".to_owned()
    } else if tool_name == "palyra.fs.os_file" {
        "approved_os_paths".to_owned()
    } else if tool_name == "palyra.http.fetch" {
        "ssrf_guard".to_owned()
    } else if tool_name.starts_with("palyra.browser.") {
        "browser_service".to_owned()
    } else if tool_name == "palyra.artifact.read" {
        "artifact_scope".to_owned()
    } else if matches!(tool_name, "palyra.delegation.query" | "palyra.delegation.control") {
        "delegation_scope".to_owned()
    } else {
        "none".to_owned()
    }
}

fn reject_oversized_tool_input(
    config: &ToolCallConfig,
    tool_name: &str,
    input_json: &[u8],
) -> Option<ToolExecutionRawResult> {
    let max_input_bytes = tool_input_limit_bytes(tool_name);
    if input_json.len() <= max_input_bytes {
        return None;
    }
    Some(ToolExecutionRawResult {
        success: false,
        output_json: b"{}".to_vec(),
        error: format!(
            "{TOOL_INPUT_TOO_LARGE_ERROR_CODE}: tool={tool_name} input_bytes={} limit_bytes={max_input_bytes}",
            input_json.len()
        ),
        timed_out: false,
        executor: tool_executor_name(config, tool_name),
        sandbox_enforcement: sandbox_enforcement_for_tool(config, tool_name),
    })
}

async fn execute_process_runner_tool(
    config: &ToolCallConfig,
    input_json: &[u8],
    cancellation_requested: Option<Arc<AtomicBool>>,
) -> ToolExecutionRawResult {
    let policy = config.process_runner.clone();
    let executor = process_runner_executor_name(&policy);
    if matches!(policy.egress_enforcement_mode, EgressEnforcementMode::Preflight) {
        warn!(
            allowed_egress_hosts = ?policy.allowed_egress_hosts,
            allowed_dns_suffixes = ?policy.allowed_dns_suffixes,
            "sandbox process runner uses preflight egress validation only; OS-level network egress is not enforced"
        );
    }
    let sandbox_enforcement = if crate::sandbox_runner::process_runner_allows_host_access(&policy) {
        "host_access".to_owned()
    } else {
        policy.egress_enforcement_mode.as_str().to_owned()
    };
    let input = input_json.to_vec();
    let timeout = Duration::from_millis(config.execution_timeout_ms);
    match tokio::task::spawn_blocking(move || {
        run_constrained_process_with_cancellation(
            &policy,
            input.as_slice(),
            timeout,
            cancellation_requested,
        )
    })
    .await
    {
        Ok(Ok(success)) => ToolExecutionRawResult {
            success: true,
            output_json: success.output_json,
            error: String::new(),
            timed_out: false,
            executor: executor.clone(),
            sandbox_enforcement: sandbox_enforcement.clone(),
        },
        Ok(Err(error)) => {
            if matches!(
                error.kind,
                SandboxProcessRunErrorKind::QuotaExceeded | SandboxProcessRunErrorKind::TimedOut
            ) {
                warn!(error = %error.message, "sandbox process runner terminated execution due to quota");
            }
            ToolExecutionRawResult {
                success: false,
                output_json: b"{}".to_vec(),
                error: error.message,
                timed_out: matches!(error.kind, SandboxProcessRunErrorKind::TimedOut),
                executor: executor.clone(),
                sandbox_enforcement: sandbox_enforcement.clone(),
            }
        }
        Err(join_error) => ToolExecutionRawResult {
            success: false,
            output_json: b"{}".to_vec(),
            error: format!("sandbox process runner worker failed: {join_error}"),
            timed_out: false,
            executor,
            sandbox_enforcement,
        },
    }
}

async fn execute_process_lifecycle_tool(
    config: &ToolCallConfig,
    tool_name: &str,
    input_json: &[u8],
) -> ToolExecutionRawResult {
    let policy = config.process_runner.clone();
    let executor = process_runner_executor_name(&policy);
    let sandbox_enforcement = sandbox_enforcement_for_tool(config, tool_name);
    if !policy.enabled {
        return ToolExecutionRawResult {
            success: false,
            output_json: b"{}".to_vec(),
            error: "sandbox process runner is disabled by runtime policy".to_owned(),
            timed_out: false,
            executor,
            sandbox_enforcement,
        };
    }
    let pid = match process_lifecycle_pid_from_input(input_json, tool_name) {
        Ok(pid) => pid,
        Err(error) => {
            return ToolExecutionRawResult {
                success: false,
                output_json: b"{}".to_vec(),
                error,
                timed_out: false,
                executor,
                sandbox_enforcement,
            };
        }
    };
    let lifecycle_tool = tool_name.to_owned();
    match tokio::task::spawn_blocking(move || match lifecycle_tool.as_str() {
        "palyra.process.stop" => stop_background_process_by_pid(pid),
        "palyra.process.status" => background_process_status_by_pid(pid),
        _ => unreachable!("validated process lifecycle tool"),
    })
    .await
    {
        Ok(Ok(success)) => ToolExecutionRawResult {
            success: true,
            output_json: success.output_json,
            error: String::new(),
            timed_out: false,
            executor,
            sandbox_enforcement,
        },
        Ok(Err(error)) => ToolExecutionRawResult {
            success: false,
            output_json: b"{}".to_vec(),
            error: error.message,
            timed_out: false,
            executor,
            sandbox_enforcement,
        },
        Err(join_error) => ToolExecutionRawResult {
            success: false,
            output_json: b"{}".to_vec(),
            error: format!("sandbox process lifecycle worker failed: {join_error}"),
            timed_out: false,
            executor,
            sandbox_enforcement,
        },
    }
}

fn process_lifecycle_pid_from_input(input_json: &[u8], tool_name: &str) -> Result<u32, String> {
    let payload = serde_json::from_slice::<Value>(input_json)
        .map_err(|error| format!("{tool_name} input must be valid JSON: {error}"))?;
    let Some(pid) = payload
        .get("pid")
        .and_then(|value| value.as_u64().or_else(|| value.as_str()?.trim().parse::<u64>().ok()))
    else {
        return Err(format!("{tool_name} requires numeric field 'pid'"));
    };
    let pid = u32::try_from(pid).map_err(|_| format!("{tool_name} field 'pid' is too large"))?;
    if pid == 0 {
        return Err(format!("{tool_name} field 'pid' must be positive"));
    }
    Ok(pid)
}

async fn execute_wasm_plugin_tool(
    config: &ToolCallConfig,
    input_json: &[u8],
) -> ToolExecutionRawResult {
    let policy = config.wasm_runtime.clone();
    let input = input_json.to_vec();
    let timeout = Duration::from_millis(config.execution_timeout_ms);
    match tokio::task::spawn_blocking(move || run_wasm_plugin(&policy, input.as_slice(), timeout))
        .await
    {
        Ok(Ok(success)) => ToolExecutionRawResult {
            success: true,
            output_json: success.output_json,
            error: String::new(),
            timed_out: false,
            executor: "sandbox_tier_a".to_owned(),
            sandbox_enforcement: "none".to_owned(),
        },
        Ok(Err(error)) => {
            if matches!(
                error.kind,
                WasmPluginRunErrorKind::QuotaExceeded | WasmPluginRunErrorKind::TimedOut
            ) {
                warn!(
                    error = %error.message,
                    "sandbox wasm runtime terminated execution due to quota or timeout"
                );
            }
            ToolExecutionRawResult {
                success: false,
                output_json: b"{}".to_vec(),
                error: error.message,
                timed_out: matches!(error.kind, WasmPluginRunErrorKind::TimedOut),
                executor: "sandbox_tier_a".to_owned(),
                sandbox_enforcement: "none".to_owned(),
            }
        }
        Err(join_error) => ToolExecutionRawResult {
            success: false,
            output_json: b"{}".to_vec(),
            error: format!("sandbox wasm plugin worker failed: {join_error}"),
            timed_out: false,
            executor: "sandbox_tier_a".to_owned(),
            sandbox_enforcement: "none".to_owned(),
        },
    }
}

fn execute_echo_tool(input_json: &[u8]) -> Result<Vec<u8>, String> {
    let payload = parse_input_json(input_json)?;
    let text = payload
        .get("text")
        .and_then(Value::as_str)
        .ok_or_else(|| "palyra.echo requires string field 'text'".to_owned())?;
    if text.len() > 4_096 {
        return Err("palyra.echo text exceeds 4096 bytes".to_owned());
    }
    serde_json::to_vec(&json!({ "echo": text }))
        .map_err(|error| format!("failed to serialize palyra.echo output: {error}"))
}

async fn execute_sleep_tool(input_json: &[u8]) -> Result<Vec<u8>, String> {
    let payload = parse_input_json(input_json)?;
    let duration_ms = payload
        .get("duration_ms")
        .and_then(Value::as_u64)
        .ok_or_else(|| "palyra.sleep requires numeric field 'duration_ms'".to_owned())?;
    if duration_ms > TOOL_MAX_SLEEP_MS {
        return Err(format!("palyra.sleep duration_ms must be <= {TOOL_MAX_SLEEP_MS}"));
    }
    tokio::time::sleep(Duration::from_millis(duration_ms)).await;
    serde_json::to_vec(&json!({ "slept_ms": duration_ms }))
        .map_err(|error| format!("failed to serialize palyra.sleep output: {error}"))
}

fn parse_input_json(input_json: &[u8]) -> Result<Value, String> {
    let parsed = serde_json::from_slice::<Value>(input_json)
        .map_err(|error| format!("tool input must be valid JSON object: {error}"))?;
    if parsed.is_object() {
        Ok(parsed)
    } else {
        Err("tool input must be a JSON object".to_owned())
    }
}

#[allow(clippy::too_many_arguments)]
fn compute_execution_hash(
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    success: bool,
    output_json: &[u8],
    error: &str,
    timed_out: bool,
    executor: &str,
    sandbox_enforcement: &str,
    executed_at_unix_ms: i64,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"palyra.tool.attestation.v1");
    hash_len_prefixed_str(&mut hasher, proposal_id);
    hash_len_prefixed_str(&mut hasher, tool_name);
    hash_len_prefixed_bytes(&mut hasher, input_json);
    hasher.update([u8::from(success)]);
    hash_len_prefixed_bytes(&mut hasher, output_json);
    hash_len_prefixed_str(&mut hasher, error);
    hasher.update([u8::from(timed_out)]);
    hash_len_prefixed_str(&mut hasher, executor);
    hash_len_prefixed_str(&mut hasher, sandbox_enforcement);
    hasher.update(executed_at_unix_ms.to_be_bytes());
    hex::encode(hasher.finalize())
}

fn hash_len_prefixed_str(hasher: &mut Sha256, value: &str) {
    hash_len_prefixed_bytes(hasher, value.as_bytes());
}

fn hash_len_prefixed_bytes(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn current_unix_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::{
        decide_tool_call, denied_execution_outcome, execute_tool_call, tool_metadata,
        tool_policy_snapshot, tool_requires_approval, ToolCallConfig, ToolCapability,
        ToolRequestContext,
    };
    use crate::sandbox_runner::{
        EgressEnforcementMode, SandboxProcessRunnerPolicy, SandboxProcessRunnerTier,
    };
    use crate::wasm_plugin_runner::WasmPluginRunnerPolicy;

    fn portable_test_process_runner_memory_limit_bytes() -> u64 {
        #[cfg(target_os = "macos")]
        {
            // Keep macOS test fixtures above the inherited harness footprint so fail-closed
            // RLIMIT_AS setup does not short-circuit the behavior being exercised.
            return 512 * 1024 * 1024;
        }
        #[cfg(not(target_os = "macos"))]
        {
            256 * 1024 * 1024
        }
    }

    fn default_process_runner_policy() -> SandboxProcessRunnerPolicy {
        SandboxProcessRunnerPolicy {
            enabled: false,
            tier: SandboxProcessRunnerTier::B,
            workspace_root: std::env::current_dir().unwrap_or_else(|_| ".".into()),
            allowed_executables: Vec::new(),
            allow_interpreters: false,
            egress_enforcement_mode: EgressEnforcementMode::Strict,
            allowed_egress_hosts: Vec::new(),
            allowed_dns_suffixes: Vec::new(),
            cpu_time_limit_ms: 2_000,
            memory_limit_bytes: portable_test_process_runner_memory_limit_bytes(),
            max_output_bytes: 64 * 1024,
        }
    }

    fn default_wasm_runtime_policy() -> WasmPluginRunnerPolicy {
        WasmPluginRunnerPolicy {
            enabled: false,
            allow_inline_modules: false,
            max_module_size_bytes: 256 * 1024,
            fuel_budget: 10_000_000,
            max_memory_bytes: 64 * 1024 * 1024,
            max_table_elements: 100_000,
            max_instances: 256,
            allowed_http_hosts: Vec::new(),
            allowed_secrets: Vec::new(),
            allowed_storage_prefixes: Vec::new(),
            allowed_channels: Vec::new(),
        }
    }

    fn allowlisted_config() -> ToolCallConfig {
        ToolCallConfig {
            allowed_tools: vec!["palyra.echo".to_owned(), "palyra.sleep".to_owned()],
            max_calls_per_run: 2,
            execution_timeout_ms: 250,
            process_runner: default_process_runner_policy(),
            wasm_runtime: default_wasm_runtime_policy(),
        }
    }

    fn tool_request_context(principal: &str) -> ToolRequestContext {
        ToolRequestContext {
            principal: principal.to_owned(),
            device_id: Some("device:test".to_owned()),
            channel: Some("cli".to_owned()),
            session_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned()),
            run_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned()),
            skill_id: None,
        }
    }

    #[test]
    fn decide_tool_call_enforces_deny_by_default_policy() {
        let config = ToolCallConfig {
            allowed_tools: Vec::new(),
            max_calls_per_run: 2,
            execution_timeout_ms: 250,
            process_runner: default_process_runner_policy(),
            wasm_runtime: default_wasm_runtime_policy(),
        };
        let mut budget = 2;
        let request_context = tool_request_context("user:ops");
        let decision =
            decide_tool_call(&config, &mut budget, &request_context, "palyra.echo", false);
        assert!(!decision.allowed);
        assert!(!decision.approval_required, "not-allowlisted denials cannot be fixed by approval");
        assert_eq!(budget, 2, "denied decisions must not consume budget");
        assert!(decision.reason.contains("denied by default"));

        let unknown_browser_tool = decide_tool_call(
            &config,
            &mut budget,
            &request_context,
            "palyra.browser.evaluate",
            false,
        );
        assert!(!unknown_browser_tool.allowed);
        assert!(
            !unknown_browser_tool.approval_required,
            "unknown browser tools should fail without creating an approval prompt"
        );
        assert!(unknown_browser_tool.reason.contains("not allowlisted"));
    }

    #[test]
    fn decide_tool_call_consumes_budget_for_allowed_tools() {
        let config = allowlisted_config();
        let mut budget = config.max_calls_per_run;
        let request_context = tool_request_context("user:ops");
        let first = decide_tool_call(&config, &mut budget, &request_context, "palyra.echo", false);
        assert!(first.allowed);
        assert!(!first.approval_required, "safe tools should not require approval by default");
        assert_eq!(budget, 1);
        let second =
            decide_tool_call(&config, &mut budget, &request_context, "palyra.sleep", false);
        assert!(second.allowed);
        assert!(!second.approval_required, "safe tools should not require approval by default");
        assert_eq!(budget, 0);
        let third = decide_tool_call(&config, &mut budget, &request_context, "palyra.echo", false);
        assert!(!third.allowed, "third call should be denied by budget");
        assert!(!third.approval_required, "budget exhaustion should not create an approval prompt");
    }

    #[test]
    fn decide_tool_call_allows_memory_search_when_allowlisted() {
        let config = ToolCallConfig {
            allowed_tools: vec!["palyra.memory.search".to_owned()],
            max_calls_per_run: 1,
            execution_timeout_ms: 250,
            process_runner: default_process_runner_policy(),
            wasm_runtime: default_wasm_runtime_policy(),
        };
        let mut budget = 1;
        let request_context = tool_request_context("user:ops");
        let decision =
            decide_tool_call(&config, &mut budget, &request_context, "palyra.memory.search", false);
        assert!(decision.allowed, "allowlisted memory search tool should be executable");
        assert!(
            !decision.approval_required,
            "memory search should not require interactive approval"
        );
        assert_eq!(budget, 0, "allowed tool should consume budget");
    }

    #[test]
    fn decide_tool_call_requires_approval_for_memory_recall_when_allowlisted() {
        let config = ToolCallConfig {
            allowed_tools: vec!["palyra.memory.recall".to_owned()],
            max_calls_per_run: 1,
            execution_timeout_ms: 250,
            process_runner: default_process_runner_policy(),
            wasm_runtime: default_wasm_runtime_policy(),
        };
        let mut budget = 1;
        let request_context = tool_request_context("user:ops");
        let decision =
            decide_tool_call(&config, &mut budget, &request_context, "palyra.memory.recall", false);
        assert!(!decision.allowed, "memory recall should require explicit approval");
        assert!(decision.approval_required, "memory recall approval metadata should be visible");
        assert_eq!(budget, 1, "denied tool should not consume budget");

        let decision =
            decide_tool_call(&config, &mut budget, &request_context, "palyra.memory.recall", true);
        assert!(decision.allowed, "approved memory recall should be executable");
        assert!(
            decision.approval_required,
            "approved execution should preserve sensitive-tool metadata"
        );
        assert_eq!(budget, 0, "allowed tool should consume budget");
    }

    #[test]
    fn decide_tool_call_session_search_requires_approval_when_allowlisted() {
        for (tool_name, allowlisted_tool) in [
            ("palyra.memory.session_search", "palyra.memory.session_search"),
            ("palyra.session_search", "palyra.session_search"),
            ("palyra.session_search", "palyra.memory.session_search"),
        ] {
            let config = ToolCallConfig {
                allowed_tools: vec![allowlisted_tool.to_owned()],
                max_calls_per_run: 1,
                execution_timeout_ms: 250,
                process_runner: default_process_runner_policy(),
                wasm_runtime: default_wasm_runtime_policy(),
            };
            let request_context = tool_request_context("user:ops");
            let mut budget = 1;

            let denied = decide_tool_call(&config, &mut budget, &request_context, tool_name, false);

            assert!(!denied.allowed, "allowlisted {tool_name} should still need approval");
            assert!(denied.approval_required, "{tool_name} should require interactive approval");
            assert_eq!(budget, 1, "denied approval must not consume budget");

            let approved =
                decide_tool_call(&config, &mut budget, &request_context, tool_name, true);

            assert!(approved.allowed, "approved {tool_name} should be executable");
            assert!(approved.approval_required, "sensitive metadata should remain visible");
            assert_eq!(budget, 0, "approved tool should consume budget");
        }
    }

    #[test]
    fn decide_tool_call_allows_memory_lifecycle_tools_when_allowlisted() {
        for (tool_name, allowlisted_tool) in [
            ("palyra.memory.retain", "palyra.memory.retain"),
            ("palyra.retain", "palyra.memory.retain"),
            ("palyra.memory.retain", "palyra.retain"),
            ("palyra.memory.reflect", "palyra.memory.reflect"),
        ] {
            let config = ToolCallConfig {
                allowed_tools: vec![allowlisted_tool.to_owned()],
                max_calls_per_run: 1,
                execution_timeout_ms: 250,
                process_runner: default_process_runner_policy(),
                wasm_runtime: default_wasm_runtime_policy(),
            };
            let mut budget = 1;
            let request_context = tool_request_context("user:ops");
            let decision =
                decide_tool_call(&config, &mut budget, &request_context, tool_name, false);
            assert!(decision.allowed, "allowlisted {tool_name} should be executable");
            assert!(
                !decision.approval_required,
                "{tool_name} should return structured lifecycle status instead of approval gating"
            );
            assert_eq!(budget, 0, "allowed tool should consume budget");
        }
    }

    #[test]
    fn process_run_allowlist_exposes_lifecycle_controls() {
        for tool_name in ["palyra.process.stop", "palyra.process.status", "palyra.process.list"] {
            let config = ToolCallConfig {
                allowed_tools: vec!["palyra.process.run".to_owned()],
                max_calls_per_run: 1,
                execution_timeout_ms: 250,
                process_runner: default_process_runner_policy(),
                wasm_runtime: default_wasm_runtime_policy(),
            };
            let request_context = tool_request_context("user:ops");
            let mut budget = 1;

            let denied = decide_tool_call(&config, &mut budget, &request_context, tool_name, false);
            assert!(!denied.allowed, "{tool_name} should still require approval");
            assert!(denied.approval_required, "{tool_name} should preserve process approval");

            let approved =
                decide_tool_call(&config, &mut budget, &request_context, tool_name, true);
            assert!(approved.allowed, "approved {tool_name} should be executable");
            assert_eq!(budget, 0, "approved lifecycle tool should consume budget");
        }
    }

    #[test]
    fn decide_tool_call_allows_workspace_read_tools_when_allowlisted() {
        for tool_name in ["palyra.fs.read_file", "palyra.fs.list_dir", "palyra.fs.search"] {
            let config = ToolCallConfig {
                allowed_tools: vec![tool_name.to_owned()],
                max_calls_per_run: 1,
                execution_timeout_ms: 250,
                process_runner: default_process_runner_policy(),
                wasm_runtime: default_wasm_runtime_policy(),
            };
            let request_context = tool_request_context("user:ops");
            let mut budget = 1;

            let decision =
                decide_tool_call(&config, &mut budget, &request_context, tool_name, false);

            assert!(decision.allowed, "allowlisted read-only {tool_name} should execute");
            assert!(
                !decision.approval_required,
                "{tool_name} should not need interactive approval"
            );
            assert_eq!(budget, 0, "allowed read-only tool should consume budget");
        }
    }

    #[test]
    fn decide_tool_call_denies_allowlisted_unsupported_runtime_tool() {
        let config = ToolCallConfig {
            allowed_tools: vec!["custom.noop".to_owned()],
            max_calls_per_run: 2,
            execution_timeout_ms: 250,
            process_runner: default_process_runner_policy(),
            wasm_runtime: default_wasm_runtime_policy(),
        };
        let mut budget = config.max_calls_per_run;
        let request_context = tool_request_context("user:ops");
        let decision =
            decide_tool_call(&config, &mut budget, &request_context, "custom.noop", true);
        assert!(!decision.allowed, "unsupported runtime tool must be denied");
        assert!(
            !decision.approval_required,
            "approval cannot make an unsupported runtime tool executable"
        );
        assert_eq!(budget, 2, "denied decisions must not consume budget");
        assert!(decision.reason.contains("unsupported by runtime executor"));
    }

    #[test]
    fn decide_tool_call_marks_sensitive_tool_as_approval_required() {
        let config = ToolCallConfig {
            allowed_tools: vec!["palyra.process.run".to_owned()],
            max_calls_per_run: 2,
            execution_timeout_ms: 250,
            process_runner: default_process_runner_policy(),
            wasm_runtime: default_wasm_runtime_policy(),
        };
        let mut budget = config.max_calls_per_run;
        let request_context = tool_request_context("user:ops");

        let decision =
            decide_tool_call(&config, &mut budget, &request_context, "palyra.process.run", false);

        assert!(
            !decision.allowed,
            "sensitive tool call should stay denied until explicit approval is present"
        );
        assert!(
            decision.approval_required,
            "process execution should always require explicit approval"
        );
        assert_eq!(budget, 2, "denied decision must not consume budget");
        assert!(
            decision.reason.contains("sensitive action blocked by default"),
            "policy deny reason should explain explicit approval requirement"
        );
    }

    #[test]
    fn decide_tool_call_allows_sensitive_tool_with_explicit_approval() {
        let config = ToolCallConfig {
            allowed_tools: vec!["palyra.process.run".to_owned()],
            max_calls_per_run: 2,
            execution_timeout_ms: 250,
            process_runner: default_process_runner_policy(),
            wasm_runtime: default_wasm_runtime_policy(),
        };
        let mut budget = config.max_calls_per_run;
        let request_context = tool_request_context("user:ops");

        let decision =
            decide_tool_call(&config, &mut budget, &request_context, "palyra.process.run", true);

        assert!(decision.allowed, "allowlisted process runner tool should pass policy gate");
        assert!(
            decision.approval_required,
            "process execution should always require explicit approval"
        );
        assert_eq!(budget, 1, "allowed decision should consume budget");
    }

    #[test]
    fn decide_tool_call_allows_browser_reload_with_explicit_approval() {
        let config = ToolCallConfig {
            allowed_tools: vec!["palyra.browser.reload".to_owned()],
            max_calls_per_run: 2,
            execution_timeout_ms: 250,
            process_runner: default_process_runner_policy(),
            wasm_runtime: default_wasm_runtime_policy(),
        };
        let mut budget = config.max_calls_per_run;
        let request_context = tool_request_context("user:ops");

        let without_approval = decide_tool_call(
            &config,
            &mut budget,
            &request_context,
            "palyra.browser.reload",
            false,
        );

        assert!(!without_approval.allowed, "browser reload should require explicit approval");
        assert!(without_approval.approval_required);
        assert_eq!(budget, 2, "denied decision must not consume budget");

        let with_approval =
            decide_tool_call(&config, &mut budget, &request_context, "palyra.browser.reload", true);

        assert!(with_approval.allowed, "approved browser reload should pass policy gate");
        assert!(with_approval.approval_required);
        assert_eq!(budget, 1, "allowed decision should consume budget");
    }

    #[test]
    fn decide_tool_call_workspace_patch_requires_explicit_approval() {
        let config = ToolCallConfig {
            allowed_tools: vec!["palyra.fs.apply_patch".to_owned()],
            max_calls_per_run: 2,
            execution_timeout_ms: 250,
            process_runner: default_process_runner_policy(),
            wasm_runtime: default_wasm_runtime_policy(),
        };
        let mut budget = config.max_calls_per_run;
        let request_context = tool_request_context("user:ops");

        let decision = decide_tool_call(
            &config,
            &mut budget,
            &request_context,
            "palyra.fs.apply_patch",
            false,
        );

        assert!(!decision.allowed, "patch tool should be denied without explicit approval");
        assert!(decision.approval_required, "patch tool should require explicit approval");
        assert_eq!(budget, 2, "denied decision must not consume budget");
    }

    #[test]
    fn decide_tool_call_workspace_patch_allows_with_explicit_approval() {
        let config = ToolCallConfig {
            allowed_tools: vec!["palyra.fs.apply_patch".to_owned()],
            max_calls_per_run: 2,
            execution_timeout_ms: 250,
            process_runner: default_process_runner_policy(),
            wasm_runtime: default_wasm_runtime_policy(),
        };
        let mut budget = config.max_calls_per_run;
        let request_context = tool_request_context("user:ops");

        let decision =
            decide_tool_call(&config, &mut budget, &request_context, "palyra.fs.apply_patch", true);

        assert!(decision.allowed, "patch tool should be allowed with explicit approval");
        assert!(decision.approval_required, "sensitive tool metadata should remain visible");
        assert_eq!(budget, 1, "allowed decision should consume budget");
    }

    #[test]
    fn tool_requires_approval_flags_sensitive_capabilities() {
        assert!(!tool_requires_approval("palyra.echo"));
        assert!(!tool_requires_approval("palyra.sleep"));
        assert!(!tool_requires_approval("palyra.memory.search"));
        assert!(tool_requires_approval("palyra.memory.recall"));
        assert!(tool_requires_approval("palyra.memory.session_search"));
        assert!(tool_requires_approval("palyra.session_search"));
        assert!(!tool_requires_approval("palyra.memory.retain"));
        assert!(!tool_requires_approval("palyra.retain"));
        assert!(tool_requires_approval("palyra.memory.delete"));
        assert!(tool_requires_approval("palyra.memory.replace"));
        assert!(!tool_requires_approval("palyra.memory.reflect"));
        assert!(!tool_requires_approval("palyra.routines.query"));
        assert!(!tool_requires_approval("palyra.artifact.read"));
        assert!(!tool_requires_approval("palyra.fs.read_file"));
        assert!(!tool_requires_approval("palyra.fs.list_dir"));
        assert!(!tool_requires_approval("palyra.fs.search"));
        assert!(tool_requires_approval("palyra.routines.control"));
        assert!(tool_requires_approval("palyra.http.fetch"));
        assert!(tool_requires_approval("palyra.process.run"));
        assert!(tool_requires_approval("palyra.process.stop"));
        assert!(tool_requires_approval("palyra.process.status"));
        assert!(tool_requires_approval("palyra.process.list"));
        assert!(tool_requires_approval("palyra.fs.apply_patch"));
        assert!(tool_requires_approval("palyra.fs.os_file"));
        assert!(tool_requires_approval("palyra.tool_program.run"));
        assert!(tool_requires_approval("palyra.browser.session.create"));
        assert!(tool_requires_approval("palyra.browser.navigate"));
        assert!(tool_requires_approval("palyra.browser.reload"));
        assert!(tool_requires_approval("palyra.browser.click"));
        assert!(tool_requires_approval("palyra.browser.type"));
        assert!(tool_requires_approval("palyra.browser.fill"));
        assert!(tool_requires_approval("palyra.browser.upload"));
        assert!(tool_requires_approval("palyra.browser.press"));
        assert!(tool_requires_approval("palyra.browser.select"));
        assert!(tool_requires_approval("palyra.browser.viewport"));
        assert!(tool_requires_approval("palyra.browser.highlight"));
        assert!(tool_requires_approval("palyra.browser.scroll"));
        assert!(tool_requires_approval("palyra.browser.wait_for"));
        assert!(tool_requires_approval("palyra.browser.title"));
        assert!(tool_requires_approval("palyra.browser.screenshot"));
        assert!(tool_requires_approval("palyra.browser.pdf"));
        assert!(tool_requires_approval("palyra.browser.observe"));
        assert!(tool_requires_approval("palyra.browser.storage"));
        assert!(tool_requires_approval("palyra.browser.network_log"));
        assert!(tool_requires_approval("palyra.browser.console_log"));
        assert!(tool_requires_approval("palyra.browser.reset_state"));
        assert!(tool_requires_approval("palyra.browser.tabs.list"));
        assert!(tool_requires_approval("palyra.browser.tabs.open"));
        assert!(tool_requires_approval("palyra.browser.tabs.switch"));
        assert!(tool_requires_approval("palyra.browser.tabs.close"));
        assert!(tool_requires_approval("palyra.browser.permissions.get"));
        assert!(tool_requires_approval("palyra.browser.permissions.set"));
        assert!(tool_requires_approval("palyra.browser.downloads.list"));
        assert!(tool_requires_approval("palyra.browser.downloads.get"));
        assert!(tool_requires_approval("palyra.plugin.run"));
        assert!(
            tool_requires_approval("custom.unknown"),
            "unknown tools should default to approval-required"
        );
    }

    #[test]
    fn artifact_read_tool_exposes_artifact_capability_without_default_approval() {
        let metadata = tool_metadata("palyra.artifact.read").expect("artifact read metadata");
        assert_eq!(metadata.capabilities, &[ToolCapability::ArtifactsRead]);
        assert!(!metadata.default_sensitive);
        assert!(!tool_requires_approval("palyra.artifact.read"));
    }

    #[test]
    fn http_fetch_tool_exposes_network_and_secret_read_capabilities() {
        let metadata = tool_metadata("palyra.http.fetch").expect("http fetch metadata");
        assert_eq!(metadata.capabilities, &[ToolCapability::Network, ToolCapability::SecretsRead]);
        assert!(metadata.default_sensitive);
        assert!(tool_requires_approval("palyra.http.fetch"));
    }

    #[test]
    fn browser_fill_tool_exposes_browser_policy_metadata_and_runtime_support() {
        let metadata = tool_metadata("palyra.browser.fill").expect("browser fill metadata");
        assert_eq!(metadata.capabilities, &[ToolCapability::Network]);
        assert!(metadata.default_sensitive);
        assert!(tool_requires_approval("palyra.browser.fill"));

        let config = ToolCallConfig {
            allowed_tools: vec!["palyra.browser.fill".to_owned()],
            max_calls_per_run: 1,
            execution_timeout_ms: 250,
            process_runner: default_process_runner_policy(),
            wasm_runtime: default_wasm_runtime_policy(),
        };
        let request_context = tool_request_context("user:ops");
        let mut budget = 1;

        let denied =
            decide_tool_call(&config, &mut budget, &request_context, "palyra.browser.fill", false);

        assert!(!denied.allowed, "browser fill should require explicit approval");
        assert!(denied.approval_required);
        assert_eq!(budget, 1, "denied approval must not consume budget");

        let approved =
            decide_tool_call(&config, &mut budget, &request_context, "palyra.browser.fill", true);

        assert!(approved.allowed, "approved browser fill should pass the runtime support gate");
        assert!(approved.approval_required, "sensitive browser metadata should remain visible");
        assert_eq!(budget, 0, "approved tool should consume budget");
    }

    #[test]
    fn browser_file_transfer_tools_expose_browser_policy_metadata() {
        for tool_name in [
            "palyra.browser.upload",
            "palyra.browser.downloads.list",
            "palyra.browser.downloads.get",
        ] {
            let metadata = tool_metadata(tool_name).expect("browser file transfer metadata");
            assert_eq!(metadata.capabilities, &[ToolCapability::Network]);
            assert!(metadata.default_sensitive, "{tool_name} should remain approval-gated");
            assert!(tool_requires_approval(tool_name));

            let config = ToolCallConfig {
                allowed_tools: vec![tool_name.to_owned()],
                max_calls_per_run: 1,
                execution_timeout_ms: 250,
                process_runner: default_process_runner_policy(),
                wasm_runtime: default_wasm_runtime_policy(),
            };
            let request_context = tool_request_context("user:ops");
            let mut budget = 1;

            let denied = decide_tool_call(&config, &mut budget, &request_context, tool_name, false);

            assert!(!denied.allowed, "{tool_name} should require explicit approval");
            assert!(denied.approval_required);
            assert_eq!(budget, 1, "denied approval must not consume budget");

            let approved =
                decide_tool_call(&config, &mut budget, &request_context, tool_name, true);

            assert!(approved.allowed, "approved {tool_name} should pass the runtime support gate");
            assert!(approved.approval_required, "sensitive browser metadata should remain visible");
            assert_eq!(budget, 0, "approved tool should consume budget");
        }
    }

    #[test]
    fn workspace_read_tools_expose_read_only_filesystem_capability_without_approval() {
        let metadata = tool_metadata("palyra.fs.read_file").expect("workspace read metadata");
        assert_eq!(metadata.capabilities, &[ToolCapability::FilesystemRead]);
        assert!(!metadata.default_sensitive);
        assert!(!tool_requires_approval("palyra.fs.read_file"));

        let metadata = tool_metadata("palyra.fs.list_dir").expect("workspace list metadata");
        assert_eq!(metadata.capabilities, &[ToolCapability::FilesystemRead]);
        assert!(!metadata.default_sensitive);
        assert!(!tool_requires_approval("palyra.fs.list_dir"));

        let metadata = tool_metadata("palyra.fs.search").expect("workspace search metadata");
        assert_eq!(metadata.capabilities, &[ToolCapability::FilesystemRead]);
        assert!(!metadata.default_sensitive);
        assert!(!tool_requires_approval("palyra.fs.search"));
    }

    #[test]
    fn os_file_tool_exposes_approval_gated_filesystem_read_write() {
        let metadata = tool_metadata("palyra.fs.os_file").expect("OS file metadata");
        assert_eq!(
            metadata.capabilities,
            &[ToolCapability::FilesystemRead, ToolCapability::FilesystemWrite]
        );
        assert!(metadata.default_sensitive);
        assert!(tool_requires_approval("palyra.fs.os_file"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_tool_call_runs_echo_tool() {
        let config = allowlisted_config();
        let outcome = execute_tool_call(
            &config,
            "01ARZ3NDEKTSV4RRFFQ69G5FA1",
            "palyra.echo",
            br#"{"text":"hello"}"#,
        )
        .await;
        assert!(outcome.success, "echo tool should succeed");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&outcome.output_json)
                .expect("echo output should be valid JSON"),
            serde_json::json!({ "echo": "hello" })
        );
        assert!(!outcome.attestation.execution_sha256.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_tool_call_tool_program_requires_gateway_runtime_context() {
        let config = ToolCallConfig {
            allowed_tools: vec!["palyra.tool_program.run".to_owned()],
            max_calls_per_run: 1,
            execution_timeout_ms: 250,
            process_runner: default_process_runner_policy(),
            wasm_runtime: default_wasm_runtime_policy(),
        };
        let outcome = execute_tool_call(
            &config,
            "01ARZ3NDEKTSV4RRFFQ69G5FAC",
            "palyra.tool_program.run",
            br#"{"schema_version":1,"program_id":"p","steps":[]}"#,
        )
        .await;

        assert!(!outcome.success, "generic tool executor should not run tool programs");
        assert!(
            outcome.error.contains("requires gateway tool program runtime context"),
            "delegated executor error should be explicit: {}",
            outcome.error
        );
        assert_eq!(outcome.attestation.executor, "tool_program_runtime");
        assert_eq!(outcome.attestation.sandbox_enforcement, "nested_tool_policy");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_tool_call_workspace_read_file_requires_gateway_runtime_context() {
        let config = ToolCallConfig {
            allowed_tools: vec![
                "palyra.fs.read_file".to_owned(),
                "palyra.fs.list_dir".to_owned(),
                "palyra.fs.search".to_owned(),
                "palyra.fs.os_file".to_owned(),
            ],
            max_calls_per_run: 1,
            execution_timeout_ms: 250,
            process_runner: default_process_runner_policy(),
            wasm_runtime: default_wasm_runtime_policy(),
        };
        let outcome = execute_tool_call(
            &config,
            "01ARZ3NDEKTSV4RRFFQ69G5FAC",
            "palyra.fs.read_file",
            br#"{"path":"agent-e2e-tool-test.js"}"#,
        )
        .await;

        assert!(!outcome.success, "generic tool executor should not run workspace file reads");
        assert!(
            outcome.error.contains("requires gateway workspace context"),
            "delegated executor error should be explicit: {}",
            outcome.error
        );
        assert_eq!(outcome.attestation.executor, "workspace_file");
        assert_eq!(outcome.attestation.sandbox_enforcement, "workspace_roots");

        let outcome = execute_tool_call(
            &config,
            "01ARZ3NDEKTSV4RRFFQ69G5FAD",
            "palyra.fs.list_dir",
            br#"{"path":"."}"#,
        )
        .await;

        assert!(!outcome.success, "generic tool executor should not run workspace listings");
        assert!(
            outcome.error.contains("requires gateway workspace context"),
            "delegated executor error should be explicit: {}",
            outcome.error
        );
        assert_eq!(outcome.attestation.executor, "workspace_file");
        assert_eq!(outcome.attestation.sandbox_enforcement, "workspace_roots");

        let outcome = execute_tool_call(
            &config,
            "01ARZ3NDEKTSV4RRFFQ69G5FAE",
            "palyra.fs.search",
            br#"{"query":"customerId"}"#,
        )
        .await;

        assert!(!outcome.success, "generic tool executor should not run workspace searches");
        assert!(
            outcome.error.contains("requires gateway workspace context"),
            "delegated executor error should be explicit: {}",
            outcome.error
        );
        assert_eq!(outcome.attestation.executor, "workspace_file");
        assert_eq!(outcome.attestation.sandbox_enforcement, "workspace_roots");

        let outcome = execute_tool_call(
            &config,
            "01ARZ3NDEKTSV4RRFFQ69G5FAF",
            "palyra.fs.os_file",
            br#"{"operation":"stat","path":"/tmp/palyra-os-file.txt"}"#,
        )
        .await;

        assert!(!outcome.success, "generic tool executor should not run OS file operations");
        assert!(
            outcome.error.contains("requires gateway OS file runtime context"),
            "delegated executor error should be explicit: {}",
            outcome.error
        );
        assert_eq!(outcome.attestation.executor, "os_file");
        assert_eq!(outcome.attestation.sandbox_enforcement, "approved_os_paths");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_tool_call_memory_search_requires_gateway_runtime_context() {
        let config = ToolCallConfig {
            allowed_tools: vec!["palyra.memory.search".to_owned()],
            max_calls_per_run: 1,
            execution_timeout_ms: 250,
            process_runner: default_process_runner_policy(),
            wasm_runtime: default_wasm_runtime_policy(),
        };
        let outcome = execute_tool_call(
            &config,
            "01ARZ3NDEKTSV4RRFFQ69G5FA9",
            "palyra.memory.search",
            br#"{"query":"incident summary"}"#,
        )
        .await;

        assert!(!outcome.success, "generic tool executor should not run gateway memory search");
        assert!(
            outcome.error.contains("requires gateway memory runtime context"),
            "delegated executor error should be explicit: {}",
            outcome.error
        );
        assert_eq!(outcome.attestation.executor, "gateway_runtime");
        assert!(!outcome.attestation.timed_out, "delegation error must not be timeout");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_tool_call_memory_recall_requires_gateway_runtime_context() {
        for tool_name in
            ["palyra.memory.recall", "palyra.memory.session_search", "palyra.session_search"]
        {
            let config = ToolCallConfig {
                allowed_tools: vec![tool_name.to_owned()],
                max_calls_per_run: 1,
                execution_timeout_ms: 250,
                process_runner: default_process_runner_policy(),
                wasm_runtime: default_wasm_runtime_policy(),
            };
            let outcome = execute_tool_call(
                &config,
                "01ARZ3NDEKTSV4RRFFQ69G5FAA",
                tool_name,
                br#"{"query":"incident summary"}"#,
            )
            .await;

            assert!(!outcome.success, "generic tool executor should not run {tool_name}");
            assert!(
                outcome.error.contains("requires gateway memory runtime context"),
                "delegated executor error should be explicit: {}",
                outcome.error
            );
            assert_eq!(outcome.attestation.executor, "gateway_runtime");
            assert!(!outcome.attestation.timed_out, "delegation error must not be timeout");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_tool_call_memory_lifecycle_tools_require_gateway_runtime_context() {
        for (tool_name, input_json) in [
            ("palyra.memory.retain", br#"{"content_text":"remember this"}"#.as_slice()),
            ("palyra.retain", br#"{"content_text":"remember this"}"#.as_slice()),
            ("palyra.memory.delete", br#"{"memory_id":"01ARZ3NDEKTSV4RRFFQ69G5FAC"}"#.as_slice()),
            (
                "palyra.memory.replace",
                br#"{"memory_id":"01ARZ3NDEKTSV4RRFFQ69G5FAC","content_text":"corrected preference"}"#
                    .as_slice(),
            ),
            ("palyra.memory.reflect", br#"{"content_text":"prefer concise output"}"#.as_slice()),
        ] {
            let config = ToolCallConfig {
                allowed_tools: vec![tool_name.to_owned()],
                max_calls_per_run: 1,
                execution_timeout_ms: 250,
                process_runner: default_process_runner_policy(),
                wasm_runtime: default_wasm_runtime_policy(),
            };
            let outcome =
                execute_tool_call(&config, "01ARZ3NDEKTSV4RRFFQ69G5FAB", tool_name, input_json)
                    .await;

            assert!(!outcome.success, "generic tool executor should not run {tool_name}");
            assert!(
                outcome.error.contains("requires gateway memory runtime context"),
                "delegated executor error should be explicit: {}",
                outcome.error
            );
            assert_eq!(outcome.attestation.executor, "gateway_runtime");
            assert!(!outcome.attestation.timed_out, "delegation error must not be timeout");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_tool_call_routines_query_requires_gateway_runtime_context() {
        let config = ToolCallConfig {
            allowed_tools: vec!["palyra.routines.query".to_owned()],
            max_calls_per_run: 1,
            execution_timeout_ms: 250,
            process_runner: default_process_runner_policy(),
            wasm_runtime: default_wasm_runtime_policy(),
        };
        let outcome = execute_tool_call(
            &config,
            "01ARZ3NDEKTSV4RRFFQ69G5FAA",
            "palyra.routines.query",
            br#"{"operation":"list"}"#,
        )
        .await;

        assert!(!outcome.success, "generic tool executor should not run gateway routines query");
        assert!(
            outcome.error.contains("requires gateway routines runtime context"),
            "delegated executor error should be explicit: {}",
            outcome.error
        );
        assert_eq!(outcome.attestation.executor, "routines_runtime");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_tool_call_routines_control_requires_gateway_runtime_context() {
        let config = ToolCallConfig {
            allowed_tools: vec!["palyra.routines.control".to_owned()],
            max_calls_per_run: 1,
            execution_timeout_ms: 250,
            process_runner: default_process_runner_policy(),
            wasm_runtime: default_wasm_runtime_policy(),
        };
        let outcome = execute_tool_call(
            &config,
            "01ARZ3NDEKTSV4RRFFQ69G5FAB",
            "palyra.routines.control",
            br#"{"operation":"pause","routine_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV"}"#,
        )
        .await;

        assert!(!outcome.success, "generic tool executor should not run gateway routines control");
        assert!(
            outcome.error.contains("requires gateway routines runtime context"),
            "delegated executor error should be explicit: {}",
            outcome.error
        );
        assert_eq!(outcome.attestation.executor, "routines_runtime");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_tool_call_http_fetch_requires_gateway_runtime_context() {
        let config = ToolCallConfig {
            allowed_tools: vec!["palyra.http.fetch".to_owned()],
            max_calls_per_run: 1,
            execution_timeout_ms: 250,
            process_runner: default_process_runner_policy(),
            wasm_runtime: default_wasm_runtime_policy(),
        };
        let outcome = execute_tool_call(
            &config,
            "01ARZ3NDEKTSV4RRFFQ69G5FAA",
            "palyra.http.fetch",
            br#"{"url":"https://example.com"}"#,
        )
        .await;
        assert!(!outcome.success, "generic tool executor should not run gateway HTTP fetch");
        assert!(
            outcome.error.contains("requires gateway HTTP fetch runtime context"),
            "delegated executor error should be explicit: {}",
            outcome.error
        );
        assert_eq!(outcome.attestation.executor, "gateway_http_fetch");
        assert!(!outcome.attestation.timed_out, "delegation error must not be timeout");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_tool_call_browser_tools_require_gateway_runtime_context() {
        let config = ToolCallConfig {
            allowed_tools: vec!["palyra.browser.navigate".to_owned()],
            max_calls_per_run: 1,
            execution_timeout_ms: 250,
            process_runner: default_process_runner_policy(),
            wasm_runtime: default_wasm_runtime_policy(),
        };
        let outcome = execute_tool_call(
            &config,
            "01ARZ3NDEKTSV4RRFFQ69G5FAB",
            "palyra.browser.navigate",
            br#"{"session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAA","url":"https://example.com"}"#,
        )
        .await;
        assert!(!outcome.success, "generic tool executor should not run browser broker flow");
        assert!(
            outcome.error.contains("requires gateway browser broker runtime context"),
            "delegated executor error should be explicit: {}",
            outcome.error
        );
        assert_eq!(outcome.attestation.executor, "browser_broker");
        assert!(!outcome.attestation.timed_out, "delegation error must not be timeout");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_tool_call_rejects_oversized_echo_input_with_quota_error() {
        let config = allowlisted_config();
        let input = serde_json::to_vec(&serde_json::json!({
            "text": "ok",
            "padding": "a".repeat(super::MAX_ECHO_TOOL_INPUT_BYTES),
        }))
        .expect("oversized payload should serialize");
        let outcome = execute_tool_call(
            &config,
            "01ARZ3NDEKTSV4RRFFQ69G5FA2",
            "palyra.echo",
            input.as_slice(),
        )
        .await;
        assert!(!outcome.success, "oversized tool input must fail");
        assert!(
            outcome.error.contains("quota/tool_input_too_large"),
            "quota failure reason should include stable code: {}",
            outcome.error
        );
        assert_eq!(outcome.attestation.executor, "builtin");
        assert!(!outcome.attestation.timed_out, "quota rejection must not be timeout");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_tool_call_rejects_oversized_process_runner_input_with_attestation() {
        let config = ToolCallConfig {
            allowed_tools: vec!["palyra.process.run".to_owned()],
            max_calls_per_run: 1,
            execution_timeout_ms: 250,
            process_runner: default_process_runner_policy(),
            wasm_runtime: default_wasm_runtime_policy(),
        };
        let input = serde_json::to_vec(&serde_json::json!({
            "command": "uname",
            "args": [],
            "padding": "a".repeat(super::MAX_PROCESS_RUNNER_TOOL_INPUT_BYTES),
        }))
        .expect("oversized payload should serialize");
        let outcome = execute_tool_call(
            &config,
            "01ARZ3NDEKTSV4RRFFQ69G5FA3",
            "palyra.process.run",
            input.as_slice(),
        )
        .await;
        assert!(!outcome.success, "oversized tool input must fail");
        assert!(
            outcome.error.contains("quota/tool_input_too_large"),
            "quota failure reason should include stable code: {}",
            outcome.error
        );
        assert_eq!(outcome.attestation.executor, "sandbox_tier_b");
        assert_eq!(outcome.attestation.sandbox_enforcement, "strict");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_tool_call_failed_process_runner_returns_diagnostic_payload() {
        let config = ToolCallConfig {
            allowed_tools: vec!["palyra.process.run".to_owned()],
            max_calls_per_run: 1,
            execution_timeout_ms: 250,
            process_runner: default_process_runner_policy(),
            wasm_runtime: default_wasm_runtime_policy(),
        };

        let outcome = execute_tool_call(
            &config,
            "01ARZ3NDEKTSV4RRFFQ69G5FA3",
            "palyra.process.run",
            br#"{"command":"node","args":["--version"]}"#,
        )
        .await;

        assert!(!outcome.success, "disabled process runner should fail");
        let output: serde_json::Value =
            serde_json::from_slice(outcome.output_json.as_slice()).expect("output should parse");
        assert_eq!(output.get("success").and_then(serde_json::Value::as_bool), Some(false));
        assert_eq!(
            output.get("tool").and_then(serde_json::Value::as_str),
            Some("palyra.process.run")
        );
        assert!(
            output
                .get("error")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .contains("disabled by runtime policy"),
            "error should be surfaced in output JSON: {output}"
        );
        assert!(
            output
                .get("recovery_hint")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .contains("Enable"),
            "recovery hint should guide the operator: {output}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_tool_call_uses_tier_c_executor_label_for_process_runner() {
        let config = ToolCallConfig {
            allowed_tools: vec!["palyra.process.run".to_owned()],
            max_calls_per_run: 1,
            execution_timeout_ms: 250,
            process_runner: SandboxProcessRunnerPolicy {
                enabled: true,
                tier: SandboxProcessRunnerTier::C,
                workspace_root: std::env::current_dir().unwrap_or_else(|_| ".".into()),
                allowed_executables: vec!["uname".to_owned()],
                allow_interpreters: false,
                egress_enforcement_mode: EgressEnforcementMode::Strict,
                allowed_egress_hosts: Vec::new(),
                allowed_dns_suffixes: Vec::new(),
                cpu_time_limit_ms: 2_000,
                memory_limit_bytes: 128 * 1024 * 1024,
                max_output_bytes: 64 * 1024,
            },
            wasm_runtime: default_wasm_runtime_policy(),
        };
        let input = serde_json::to_vec(&serde_json::json!({
            "command": "uname",
            "args": [],
            "padding": "a".repeat(super::MAX_PROCESS_RUNNER_TOOL_INPUT_BYTES),
        }))
        .expect("oversized payload should serialize");
        let outcome = execute_tool_call(
            &config,
            "01ARZ3NDEKTSV4RRFFQ69G5FA3",
            "palyra.process.run",
            input.as_slice(),
        )
        .await;
        assert!(!outcome.success, "oversized tool input must fail");
        assert!(
            outcome.attestation.executor.starts_with("sandbox_tier_c_"),
            "tier-c process runner should expose tier-c executor label"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_tool_call_enforces_timeout() {
        let config = ToolCallConfig {
            allowed_tools: vec!["palyra.sleep".to_owned()],
            max_calls_per_run: 1,
            execution_timeout_ms: 5,
            process_runner: default_process_runner_policy(),
            wasm_runtime: default_wasm_runtime_policy(),
        };
        let outcome = execute_tool_call(
            &config,
            "01ARZ3NDEKTSV4RRFFQ69G5FA2",
            "palyra.sleep",
            br#"{"duration_ms":50}"#,
        )
        .await;
        assert!(!outcome.success, "sleep tool should time out under a tiny timeout budget");
        assert!(outcome.attestation.timed_out, "attestation must record timeout");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_sleep_rejects_above_bounded_limit_without_waiting() {
        let config = ToolCallConfig {
            allowed_tools: vec!["palyra.sleep".to_owned()],
            max_calls_per_run: 1,
            execution_timeout_ms: 1_000,
            process_runner: default_process_runner_policy(),
            wasm_runtime: default_wasm_runtime_policy(),
        };
        let outcome = execute_tool_call(
            &config,
            "01ARZ3NDEKTSV4RRFFQ69G5FA2",
            "palyra.sleep",
            br#"{"duration_ms":30001}"#,
        )
        .await;
        assert!(!outcome.success, "sleep above bounded limit must fail fast");
        assert!(!outcome.attestation.timed_out, "limit rejection should not wait for timeout");
        assert!(
            outcome.error.contains("duration_ms must be <= 30000"),
            "failure should include the bounded sleep limit: {}",
            outcome.error
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    #[cfg(all(unix, not(target_os = "macos")))]
    async fn execute_tool_call_runs_sandbox_process_runner() {
        if std::process::Command::new("uname").output().is_err() {
            return;
        }
        let config = ToolCallConfig {
            allowed_tools: vec!["palyra.process.run".to_owned()],
            max_calls_per_run: 1,
            execution_timeout_ms: 2_000,
            process_runner: SandboxProcessRunnerPolicy {
                enabled: true,
                tier: SandboxProcessRunnerTier::B,
                workspace_root: std::env::current_dir().expect("current_dir should resolve"),
                allowed_executables: vec!["uname".to_owned()],
                allow_interpreters: false,
                egress_enforcement_mode: EgressEnforcementMode::Preflight,
                allowed_egress_hosts: Vec::new(),
                allowed_dns_suffixes: Vec::new(),
                cpu_time_limit_ms: 2_000,
                memory_limit_bytes: portable_test_process_runner_memory_limit_bytes(),
                max_output_bytes: 64 * 1024,
            },
            wasm_runtime: default_wasm_runtime_policy(),
        };

        let outcome = execute_tool_call(
            &config,
            "01ARZ3NDEKTSV4RRFFQ69G5FA2",
            "palyra.process.run",
            br#"{"command":"uname","args":[]}"#,
        )
        .await;

        assert!(outcome.success, "sandbox process runner should execute allowlisted command");
        assert_eq!(outcome.attestation.executor, "sandbox_tier_b");
        assert_eq!(
            outcome.attestation.sandbox_enforcement,
            config.process_runner.egress_enforcement_mode.as_str()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    #[cfg(target_os = "macos")]
    async fn execute_tool_call_fails_closed_for_macos_process_runner() {
        if std::process::Command::new("uname").output().is_err() {
            return;
        }
        let config = ToolCallConfig {
            allowed_tools: vec!["palyra.process.run".to_owned()],
            max_calls_per_run: 1,
            execution_timeout_ms: 2_000,
            process_runner: SandboxProcessRunnerPolicy {
                enabled: true,
                tier: SandboxProcessRunnerTier::B,
                workspace_root: std::env::current_dir().expect("current_dir should resolve"),
                allowed_executables: vec!["uname".to_owned()],
                allow_interpreters: false,
                egress_enforcement_mode: EgressEnforcementMode::Preflight,
                allowed_egress_hosts: Vec::new(),
                allowed_dns_suffixes: Vec::new(),
                cpu_time_limit_ms: 2_000,
                memory_limit_bytes: portable_test_process_runner_memory_limit_bytes(),
                max_output_bytes: 64 * 1024,
            },
            wasm_runtime: default_wasm_runtime_policy(),
        };

        let outcome = execute_tool_call(
            &config,
            "01ARZ3NDEKTSV4RRFFQ69G5FA2",
            "palyra.process.run",
            br#"{"command":"uname","args":[]}"#,
        )
        .await;

        assert!(
            !outcome.success,
            "macOS process runner must fail closed without reliable resource quotas"
        );
        assert!(
            outcome.error.contains("unavailable on macOS"),
            "macOS failure should explain missing quota enforcement: {}",
            outcome.error
        );
        assert_eq!(outcome.attestation.executor, "sandbox_tier_b");
        assert_eq!(
            outcome.attestation.sandbox_enforcement,
            config.process_runner.egress_enforcement_mode.as_str()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    #[cfg(unix)]
    async fn execute_tool_call_denies_sandbox_path_traversal() {
        let config = ToolCallConfig {
            allowed_tools: vec!["palyra.process.run".to_owned()],
            max_calls_per_run: 1,
            execution_timeout_ms: 2_000,
            process_runner: SandboxProcessRunnerPolicy {
                enabled: true,
                tier: SandboxProcessRunnerTier::B,
                workspace_root: std::env::current_dir().expect("current_dir should resolve"),
                allowed_executables: vec!["uname".to_owned()],
                allow_interpreters: false,
                egress_enforcement_mode: EgressEnforcementMode::Strict,
                allowed_egress_hosts: Vec::new(),
                allowed_dns_suffixes: Vec::new(),
                cpu_time_limit_ms: 2_000,
                memory_limit_bytes: 128 * 1024 * 1024,
                max_output_bytes: 64 * 1024,
            },
            wasm_runtime: default_wasm_runtime_policy(),
        };

        let outcome = execute_tool_call(
            &config,
            "01ARZ3NDEKTSV4RRFFQ69G5FA2",
            "palyra.process.run",
            br#"{"command":"uname","args":["../outside.txt"]}"#,
        )
        .await;

        assert!(!outcome.success, "sandbox runner must block traversal path");
        assert_eq!(outcome.attestation.executor, "sandbox_tier_b");
        assert_eq!(outcome.attestation.sandbox_enforcement, "strict");
        assert!(outcome.error.contains("path traversal"));
    }

    #[tokio::test(flavor = "multi_thread")]
    #[cfg(unix)]
    async fn execute_tool_call_denies_secret_exfiltration_path_and_emits_attestation() {
        if std::process::Command::new("cat").output().is_err() {
            return;
        }

        let config = ToolCallConfig {
            allowed_tools: vec!["palyra.process.run".to_owned()],
            max_calls_per_run: 1,
            execution_timeout_ms: 2_000,
            process_runner: SandboxProcessRunnerPolicy {
                enabled: true,
                tier: SandboxProcessRunnerTier::B,
                workspace_root: std::env::current_dir().expect("current_dir should resolve"),
                allowed_executables: vec!["cat".to_owned()],
                allow_interpreters: false,
                egress_enforcement_mode: EgressEnforcementMode::Strict,
                allowed_egress_hosts: Vec::new(),
                allowed_dns_suffixes: Vec::new(),
                cpu_time_limit_ms: 2_000,
                memory_limit_bytes: 128 * 1024 * 1024,
                max_output_bytes: 64 * 1024,
            },
            wasm_runtime: default_wasm_runtime_policy(),
        };

        let outcome = execute_tool_call(
            &config,
            "01ARZ3NDEKTSV4RRFFQ69G5FA2",
            "palyra.process.run",
            br#"{"command":"cat","args":["/etc/shadow"]}"#,
        )
        .await;

        assert!(!outcome.success, "secret path exfil attempt must be denied");
        assert_eq!(outcome.attestation.executor, "sandbox_tier_b");
        assert_eq!(outcome.attestation.sandbox_enforcement, "strict");
        assert!(
            outcome.error.contains("escapes workspace scope"),
            "secret-path denial should explain workspace scope boundary"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_tool_call_runs_sandbox_wasm_plugin() {
        let config = ToolCallConfig {
            allowed_tools: vec!["palyra.plugin.run".to_owned()],
            max_calls_per_run: 1,
            execution_timeout_ms: 2_000,
            process_runner: default_process_runner_policy(),
            wasm_runtime: WasmPluginRunnerPolicy {
                enabled: true,
                allow_inline_modules: true,
                max_module_size_bytes: 256 * 1024,
                fuel_budget: 10_000_000,
                max_memory_bytes: 64 * 1024 * 1024,
                max_table_elements: 100_000,
                max_instances: 256,
                allowed_http_hosts: vec!["api.example.com".to_owned()],
                allowed_secrets: Vec::new(),
                allowed_storage_prefixes: Vec::new(),
                allowed_channels: Vec::new(),
            },
        };

        let outcome = execute_tool_call(
            &config,
            "01ARZ3NDEKTSV4RRFFQ69G5FA2",
            "palyra.plugin.run",
            br#"{
                "skill_id":"acme.echo_http",
                "skill_version":"1.2.3",
                "module_wat":"(module (import \"palyra:plugins/host-capabilities@0.1.0\" \"http-count\" (func $http_count (result i32))) (func (export \"run\") (result i32) call $http_count))",
                "capabilities":{"http_hosts":["api.example.com"]}
            }"#,
        )
        .await;

        assert!(outcome.success, "wasm plugin runner should execute allowlisted module");
        assert_eq!(outcome.attestation.executor, "sandbox_tier_a");
        let output: serde_json::Value =
            serde_json::from_slice(&outcome.output_json).expect("output should parse");
        assert_eq!(output.get("exit_code").and_then(serde_json::Value::as_i64), Some(1));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_tool_call_marks_wasm_timeout_in_attestation() {
        let config = ToolCallConfig {
            allowed_tools: vec!["palyra.plugin.run".to_owned()],
            max_calls_per_run: 1,
            execution_timeout_ms: 10,
            process_runner: default_process_runner_policy(),
            wasm_runtime: WasmPluginRunnerPolicy {
                enabled: true,
                allow_inline_modules: true,
                max_module_size_bytes: 256 * 1024,
                fuel_budget: 1_000_000_000,
                max_memory_bytes: 64 * 1024 * 1024,
                max_table_elements: 100_000,
                max_instances: 256,
                allowed_http_hosts: Vec::new(),
                allowed_secrets: Vec::new(),
                allowed_storage_prefixes: Vec::new(),
                allowed_channels: Vec::new(),
            },
        };

        let outcome = execute_tool_call(
            &config,
            "01ARZ3NDEKTSV4RRFFQ69G5FA2",
            "palyra.plugin.run",
            br#"{
                "module_wat":"(module (func (export \"run\") (result i32) (loop (br 0)) i32.const 0))"
            }"#,
        )
        .await;

        assert!(!outcome.success, "infinite loop plugin must time out");
        assert_eq!(outcome.attestation.executor, "sandbox_tier_a");
        assert!(
            outcome.attestation.timed_out,
            "attestation must record wasm runtime wall-clock timeout"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_tool_call_denies_wasm_plugin_non_allowlisted_capability() {
        let config = ToolCallConfig {
            allowed_tools: vec!["palyra.plugin.run".to_owned()],
            max_calls_per_run: 1,
            execution_timeout_ms: 2_000,
            process_runner: default_process_runner_policy(),
            wasm_runtime: WasmPluginRunnerPolicy {
                enabled: true,
                allow_inline_modules: true,
                max_module_size_bytes: 256 * 1024,
                fuel_budget: 10_000_000,
                max_memory_bytes: 64 * 1024 * 1024,
                max_table_elements: 100_000,
                max_instances: 256,
                allowed_http_hosts: vec!["api.example.com".to_owned()],
                allowed_secrets: Vec::new(),
                allowed_storage_prefixes: Vec::new(),
                allowed_channels: Vec::new(),
            },
        };

        let outcome = execute_tool_call(
            &config,
            "01ARZ3NDEKTSV4RRFFQ69G5FA2",
            "palyra.plugin.run",
            br#"{
                "module_wat":"(module (func (export \"run\") (result i32) i32.const 1))",
                "capabilities":{"http_hosts":["blocked.example"]}
            }"#,
        )
        .await;

        assert!(!outcome.success, "wasm plugin runner must deny non-allowlisted capabilities");
        assert_eq!(outcome.attestation.executor, "sandbox_tier_a");
        assert!(outcome.error.contains("capability denied"));
    }

    #[test]
    fn denied_execution_outcome_generates_attestation() {
        let outcome = denied_execution_outcome(
            "01ARZ3NDEKTSV4RRFFQ69G5FA3",
            "shell.exec",
            br#"{"command":"whoami"}"#,
            "denied",
        );
        assert!(!outcome.success);
        assert!(outcome.error.contains("denied"));
        assert_eq!(outcome.attestation.executor, "policy");
        assert_eq!(outcome.attestation.sandbox_enforcement, "none");
    }

    #[test]
    fn tool_policy_snapshot_reflects_runtime_configuration() {
        let config = allowlisted_config();
        let snapshot = tool_policy_snapshot(&config);
        assert_eq!(snapshot.max_calls_per_run, 2);
        assert_eq!(snapshot.execution_timeout_ms, 250);
        assert_eq!(snapshot.allowed_tools.len(), 2);
        assert!(!snapshot.wasm_runtime.enabled);
    }

    #[test]
    fn compute_execution_hash_is_unambiguous_for_delimiter_like_payloads() {
        let hash_one = super::compute_execution_hash(
            "01ARZ3NDEKTSV4RRFFQ69G5FA4",
            "palyra.echo",
            br#"{"text":"hello|world"}"#,
            false,
            b"A",
            "B|C",
            false,
            "builtin",
            "none",
            1_735_689_600_000,
        );
        let hash_two = super::compute_execution_hash(
            "01ARZ3NDEKTSV4RRFFQ69G5FA4",
            "palyra.echo",
            br#"{"text":"hello|world"}"#,
            false,
            b"A|B",
            "C",
            false,
            "builtin",
            "none",
            1_735_689_600_000,
        );
        assert_ne!(hash_one, hash_two, "distinct field tuples must hash differently");
    }
}
