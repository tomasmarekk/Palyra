use std::time::{Duration, SystemTime, UNIX_EPOCH};

use palyra_policy::{evaluate_with_config, PolicyDecision, PolicyEvaluationConfig, PolicyRequest};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tracing::warn;
use ulid::Ulid;

use crate::sandbox_runner::{
    run_constrained_process, SandboxProcessRunErrorKind, SandboxProcessRunnerPolicy,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolCapability {
    ProcessExec,
    Network,
    SecretsRead,
    FilesystemWrite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ToolMetadata {
    pub capabilities: &'static [ToolCapability],
    pub default_sensitive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolAttestation {
    pub attestation_id: String,
    pub execution_sha256: String,
    pub executed_at_unix_ms: i64,
    pub timed_out: bool,
    pub executor: String,
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
    pub workspace_root: String,
    pub allowed_executables: Vec<String>,
    pub allowed_egress_hosts: Vec<String>,
    pub allowed_dns_suffixes: Vec<String>,
    pub cpu_time_limit_ms: u64,
    pub memory_limit_bytes: u64,
    pub max_output_bytes: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct WasmRuntimePolicySnapshot {
    pub enabled: bool,
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
const TOOL_MAX_SLEEP_MS: u64 = 5_000;
const EMPTY_TOOL_CAPABILITIES: &[ToolCapability] = &[];
const PROCESS_RUNNER_CAPABILITIES: &[ToolCapability] = &[ToolCapability::ProcessExec];
const WASM_PLUGIN_CAPABILITIES: &[ToolCapability] =
    &[ToolCapability::Network, ToolCapability::SecretsRead, ToolCapability::FilesystemWrite];

pub fn tool_policy_snapshot(config: &ToolCallConfig) -> ToolCallPolicySnapshot {
    ToolCallPolicySnapshot {
        allowed_tools: config.allowed_tools.clone(),
        max_calls_per_run: config.max_calls_per_run,
        execution_timeout_ms: config.execution_timeout_ms,
        process_runner: ProcessRunnerPolicySnapshot {
            enabled: config.process_runner.enabled,
            workspace_root: config.process_runner.workspace_root.to_string_lossy().into_owned(),
            allowed_executables: config.process_runner.allowed_executables.clone(),
            allowed_egress_hosts: config.process_runner.allowed_egress_hosts.clone(),
            allowed_dns_suffixes: config.process_runner.allowed_dns_suffixes.clone(),
            cpu_time_limit_ms: config.process_runner.cpu_time_limit_ms,
            memory_limit_bytes: config.process_runner.memory_limit_bytes,
            max_output_bytes: config.process_runner.max_output_bytes,
        },
        wasm_runtime: WasmRuntimePolicySnapshot {
            enabled: config.wasm_runtime.enabled,
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
    principal: &str,
    tool_name: &str,
    allow_sensitive_tools: bool,
) -> ToolDecision {
    let approval_required = tool_requires_approval(tool_name);
    if *remaining_budget == 0 {
        return ToolDecision {
            allowed: false,
            reason: BUDGET_DENY_REASON.to_owned(),
            approval_required,
            policy_enforced: true,
        };
    }

    let policy_request = PolicyRequest {
        principal: principal.to_owned(),
        action: "tool.execute".to_owned(),
        resource: format!("tool:{tool_name}"),
    };
    let policy_config = PolicyEvaluationConfig {
        allowlisted_tools: config.allowed_tools.clone(),
        allow_sensitive_tools,
        sensitive_tool_names: sensitive_allowlisted_tool_names(config.allowed_tools.as_slice()),
        allowlisted_skills: Vec::new(),
    };
    let policy_evaluation = match evaluate_with_config(&policy_request, &policy_config) {
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
            approval_required,
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

#[must_use]
pub fn tool_metadata(tool_name: &str) -> Option<ToolMetadata> {
    match tool_name {
        "palyra.echo" => {
            Some(ToolMetadata { capabilities: EMPTY_TOOL_CAPABILITIES, default_sensitive: false })
        }
        "palyra.sleep" => {
            Some(ToolMetadata { capabilities: EMPTY_TOOL_CAPABILITIES, default_sensitive: false })
        }
        "palyra.memory.search" => {
            Some(ToolMetadata { capabilities: EMPTY_TOOL_CAPABILITIES, default_sensitive: false })
        }
        "palyra.process.run" => Some(ToolMetadata {
            capabilities: PROCESS_RUNNER_CAPABILITIES,
            default_sensitive: true,
        }),
        "palyra.plugin.run" => {
            Some(ToolMetadata { capabilities: WASM_PLUGIN_CAPABILITIES, default_sensitive: true })
        }
        _ => None,
    }
}

#[must_use]
pub fn tool_requires_approval(tool_name: &str) -> bool {
    let Some(metadata) = tool_metadata(tool_name) else {
        return true;
    };
    metadata.default_sensitive
        || metadata.capabilities.iter().any(|capability| {
            matches!(
                capability,
                ToolCapability::ProcessExec
                    | ToolCapability::Network
                    | ToolCapability::SecretsRead
                    | ToolCapability::FilesystemWrite
            )
        })
}

fn sensitive_allowlisted_tool_names(allowlisted_tools: &[String]) -> Vec<String> {
    allowlisted_tools
        .iter()
        .filter(|tool_name| tool_requires_approval(tool_name.as_str()))
        .map(|tool_name| tool_name.to_ascii_lowercase())
        .collect()
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
        },
    )
}

pub async fn execute_tool_call(
    config: &ToolCallConfig,
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
) -> ToolExecutionOutcome {
    let raw = if tool_name == "palyra.plugin.run" {
        run_allowlisted_tool(config, tool_name, input_json).await
    } else {
        let timeout = Duration::from_millis(config.execution_timeout_ms);
        match tokio::time::timeout(timeout, run_allowlisted_tool(config, tool_name, input_json))
            .await
        {
            Ok(raw) => raw,
            Err(_) => ToolExecutionRawResult {
                success: false,
                output_json: b"{}".to_vec(),
                error: format!("tool execution timed out after {}ms", config.execution_timeout_ms),
                timed_out: true,
                executor: tool_executor_name(tool_name).to_owned(),
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
}

fn build_execution_outcome(
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    raw: ToolExecutionRawResult,
) -> ToolExecutionOutcome {
    let executed_at_unix_ms = current_unix_ms();
    let execution_sha256 = compute_execution_hash(
        proposal_id,
        tool_name,
        input_json,
        raw.success,
        raw.output_json.as_slice(),
        raw.error.as_str(),
        raw.timed_out,
        raw.executor.as_str(),
        executed_at_unix_ms,
    );
    ToolExecutionOutcome {
        success: raw.success,
        output_json: raw.output_json,
        error: raw.error,
        attestation: ToolAttestation {
            attestation_id: Ulid::new().to_string(),
            execution_sha256,
            executed_at_unix_ms,
            timed_out: raw.timed_out,
            executor: raw.executor,
        },
    }
}

async fn run_allowlisted_tool(
    config: &ToolCallConfig,
    tool_name: &str,
    input_json: &[u8],
) -> ToolExecutionRawResult {
    match tool_name {
        "palyra.echo" => match execute_echo_tool(input_json) {
            Ok(output_json) => ToolExecutionRawResult {
                success: true,
                output_json,
                error: String::new(),
                timed_out: false,
                executor: "builtin".to_owned(),
            },
            Err(error) => ToolExecutionRawResult {
                success: false,
                output_json: b"{}".to_vec(),
                error,
                timed_out: false,
                executor: "builtin".to_owned(),
            },
        },
        "palyra.sleep" => match execute_sleep_tool(input_json).await {
            Ok(output_json) => ToolExecutionRawResult {
                success: true,
                output_json,
                error: String::new(),
                timed_out: false,
                executor: "builtin".to_owned(),
            },
            Err(error) => ToolExecutionRawResult {
                success: false,
                output_json: b"{}".to_vec(),
                error,
                timed_out: false,
                executor: "builtin".to_owned(),
            },
        },
        "palyra.process.run" => execute_process_runner_tool(config, input_json).await,
        "palyra.plugin.run" => execute_wasm_plugin_tool(config, input_json).await,
        _ => ToolExecutionRawResult {
            success: false,
            output_json: b"{}".to_vec(),
            error: "allowlisted tool is not implemented by runtime executor".to_owned(),
            timed_out: false,
            executor: "builtin".to_owned(),
        },
    }
}

fn is_runtime_supported_tool(tool_name: &str) -> bool {
    matches!(
        tool_name,
        "palyra.echo"
            | "palyra.sleep"
            | "palyra.memory.search"
            | "palyra.process.run"
            | "palyra.plugin.run"
    )
}

fn tool_executor_name(tool_name: &str) -> &'static str {
    if tool_name == "palyra.process.run" {
        "sandbox_tier_b"
    } else if tool_name == "palyra.memory.search" {
        "gateway_runtime"
    } else if tool_name == "palyra.plugin.run" {
        "sandbox_tier_a"
    } else {
        "builtin"
    }
}

async fn execute_process_runner_tool(
    config: &ToolCallConfig,
    input_json: &[u8],
) -> ToolExecutionRawResult {
    let policy = config.process_runner.clone();
    let input = input_json.to_vec();
    let timeout = Duration::from_millis(config.execution_timeout_ms);
    match tokio::task::spawn_blocking(move || {
        run_constrained_process(&policy, input.as_slice(), timeout)
    })
    .await
    {
        Ok(Ok(success)) => ToolExecutionRawResult {
            success: true,
            output_json: success.output_json,
            error: String::new(),
            timed_out: false,
            executor: "sandbox_tier_b".to_owned(),
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
                executor: "sandbox_tier_b".to_owned(),
            }
        }
        Err(join_error) => ToolExecutionRawResult {
            success: false,
            output_json: b"{}".to_vec(),
            error: format!("sandbox process runner worker failed: {join_error}"),
            timed_out: false,
            executor: "sandbox_tier_b".to_owned(),
        },
    }
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
            }
        }
        Err(join_error) => ToolExecutionRawResult {
            success: false,
            output_json: b"{}".to_vec(),
            error: format!("sandbox wasm plugin worker failed: {join_error}"),
            timed_out: false,
            executor: "sandbox_tier_a".to_owned(),
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
    hasher.update(executed_at_unix_ms.to_be_bytes());
    format!("{:x}", hasher.finalize())
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
        decide_tool_call, denied_execution_outcome, execute_tool_call, tool_policy_snapshot,
        tool_requires_approval, ToolCallConfig,
    };
    use crate::sandbox_runner::SandboxProcessRunnerPolicy;
    use crate::wasm_plugin_runner::WasmPluginRunnerPolicy;

    fn default_process_runner_policy() -> SandboxProcessRunnerPolicy {
        SandboxProcessRunnerPolicy {
            enabled: false,
            workspace_root: std::env::current_dir().unwrap_or_else(|_| ".".into()),
            allowed_executables: Vec::new(),
            allowed_egress_hosts: Vec::new(),
            allowed_dns_suffixes: Vec::new(),
            cpu_time_limit_ms: 2_000,
            memory_limit_bytes: 256 * 1024 * 1024,
            max_output_bytes: 64 * 1024,
        }
    }

    fn default_wasm_runtime_policy() -> WasmPluginRunnerPolicy {
        WasmPluginRunnerPolicy {
            enabled: false,
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
        let decision = decide_tool_call(&config, &mut budget, "user:ops", "palyra.echo", false);
        assert!(!decision.allowed);
        assert_eq!(budget, 2, "denied decisions must not consume budget");
        assert!(decision.reason.contains("denied by default"));
    }

    #[test]
    fn decide_tool_call_consumes_budget_for_allowed_tools() {
        let config = allowlisted_config();
        let mut budget = config.max_calls_per_run;
        let first = decide_tool_call(&config, &mut budget, "user:ops", "palyra.echo", false);
        assert!(first.allowed);
        assert!(!first.approval_required, "safe tools should not require approval by default");
        assert_eq!(budget, 1);
        let second = decide_tool_call(&config, &mut budget, "user:ops", "palyra.sleep", false);
        assert!(second.allowed);
        assert!(!second.approval_required, "safe tools should not require approval by default");
        assert_eq!(budget, 0);
        let third = decide_tool_call(&config, &mut budget, "user:ops", "palyra.echo", false);
        assert!(!third.allowed, "third call should be denied by budget");
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
        let decision =
            decide_tool_call(&config, &mut budget, "user:ops", "palyra.memory.search", false);
        assert!(decision.allowed, "allowlisted memory search tool should be executable");
        assert!(
            !decision.approval_required,
            "memory search should not require interactive approval"
        );
        assert_eq!(budget, 0, "allowed tool should consume budget");
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
        let decision = decide_tool_call(&config, &mut budget, "user:ops", "custom.noop", true);
        assert!(!decision.allowed, "unsupported runtime tool must be denied");
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

        let decision =
            decide_tool_call(&config, &mut budget, "user:ops", "palyra.process.run", false);

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

        let decision =
            decide_tool_call(&config, &mut budget, "user:ops", "palyra.process.run", true);

        assert!(decision.allowed, "allowlisted process runner tool should pass policy gate");
        assert!(
            decision.approval_required,
            "process execution should always require explicit approval"
        );
        assert_eq!(budget, 1, "allowed decision should consume budget");
    }

    #[test]
    fn tool_requires_approval_flags_sensitive_capabilities() {
        assert!(!tool_requires_approval("palyra.echo"));
        assert!(!tool_requires_approval("palyra.sleep"));
        assert!(!tool_requires_approval("palyra.memory.search"));
        assert!(tool_requires_approval("palyra.process.run"));
        assert!(tool_requires_approval("palyra.plugin.run"));
        assert!(
            tool_requires_approval("custom.unknown"),
            "unknown tools should default to approval-required"
        );
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
    #[cfg(unix)]
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
                workspace_root: std::env::current_dir().expect("current_dir should resolve"),
                allowed_executables: vec!["uname".to_owned()],
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
            br#"{"command":"uname","args":[]}"#,
        )
        .await;

        assert!(outcome.success, "sandbox process runner should execute allowlisted command");
        assert_eq!(outcome.attestation.executor, "sandbox_tier_b");
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
                workspace_root: std::env::current_dir().expect("current_dir should resolve"),
                allowed_executables: vec!["uname".to_owned()],
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
        assert!(outcome.error.contains("path traversal"));
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
            1_735_689_600_000,
        );
        assert_ne!(hash_one, hash_two, "distinct field tuples must hash differently");
    }
}
