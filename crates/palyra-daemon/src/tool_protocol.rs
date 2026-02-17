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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCallConfig {
    pub allowed_tools: Vec<String>,
    pub max_calls_per_run: u32,
    pub execution_timeout_ms: u64,
    pub process_runner: SandboxProcessRunnerPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolDecision {
    pub allowed: bool,
    pub reason: String,
    pub approval_required: bool,
    pub policy_enforced: bool,
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

const BUDGET_DENY_REASON: &str = "tool execution budget exhausted for run";
const UNSUPPORTED_TOOL_DENY_REASON: &str =
    "tool is allowlisted but unsupported by runtime executor";
const TOOL_MAX_SLEEP_MS: u64 = 5_000;

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
    }
}

pub fn decide_tool_call(
    config: &ToolCallConfig,
    remaining_budget: &mut u32,
    principal: &str,
    tool_name: &str,
) -> ToolDecision {
    if *remaining_budget == 0 {
        return ToolDecision {
            allowed: false,
            reason: BUDGET_DENY_REASON.to_owned(),
            approval_required: true,
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
        allow_sensitive_tools: false,
    };
    let policy_evaluation = match evaluate_with_config(&policy_request, &policy_config) {
        Ok(evaluation) => evaluation,
        Err(error) => {
            return ToolDecision {
                allowed: false,
                reason: format!("policy evaluation failed safely: {error}"),
                approval_required: true,
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
            approval_required: true,
            policy_enforced: true,
        };
    }

    if !is_runtime_supported_tool(tool_name) {
        return ToolDecision {
            allowed: false,
            reason: UNSUPPORTED_TOOL_DENY_REASON.to_owned(),
            approval_required: true,
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
        approval_required: true,
        policy_enforced: true,
    }
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
    let timeout = Duration::from_millis(config.execution_timeout_ms);
    let raw =
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
    matches!(tool_name, "palyra.echo" | "palyra.sleep" | "palyra.process.run")
}

fn tool_executor_name(tool_name: &str) -> &'static str {
    if tool_name == "palyra.process.run" {
        "sandbox_tier_b"
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
        ToolCallConfig,
    };
    use crate::sandbox_runner::SandboxProcessRunnerPolicy;

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

    fn allowlisted_config() -> ToolCallConfig {
        ToolCallConfig {
            allowed_tools: vec!["palyra.echo".to_owned(), "palyra.sleep".to_owned()],
            max_calls_per_run: 2,
            execution_timeout_ms: 250,
            process_runner: default_process_runner_policy(),
        }
    }

    #[test]
    fn decide_tool_call_enforces_deny_by_default_policy() {
        let config = ToolCallConfig {
            allowed_tools: Vec::new(),
            max_calls_per_run: 2,
            execution_timeout_ms: 250,
            process_runner: default_process_runner_policy(),
        };
        let mut budget = 2;
        let decision = decide_tool_call(&config, &mut budget, "user:ops", "palyra.echo");
        assert!(!decision.allowed);
        assert_eq!(budget, 2, "denied decisions must not consume budget");
        assert!(decision.reason.contains("denied by default"));
    }

    #[test]
    fn decide_tool_call_consumes_budget_for_allowed_tools() {
        let config = allowlisted_config();
        let mut budget = config.max_calls_per_run;
        let first = decide_tool_call(&config, &mut budget, "user:ops", "palyra.echo");
        assert!(first.allowed);
        assert_eq!(budget, 1);
        let second = decide_tool_call(&config, &mut budget, "user:ops", "palyra.sleep");
        assert!(second.allowed);
        assert_eq!(budget, 0);
        let third = decide_tool_call(&config, &mut budget, "user:ops", "palyra.echo");
        assert!(!third.allowed, "third call should be denied by budget");
    }

    #[test]
    fn decide_tool_call_denies_allowlisted_unsupported_runtime_tool() {
        let config = ToolCallConfig {
            allowed_tools: vec!["custom.noop".to_owned()],
            max_calls_per_run: 2,
            execution_timeout_ms: 250,
            process_runner: default_process_runner_policy(),
        };
        let mut budget = config.max_calls_per_run;
        let decision = decide_tool_call(&config, &mut budget, "user:ops", "custom.noop");
        assert!(!decision.allowed, "unsupported runtime tool must be denied");
        assert_eq!(budget, 2, "denied decisions must not consume budget");
        assert!(decision.reason.contains("unsupported by runtime executor"));
    }

    #[test]
    fn decide_tool_call_denies_sensitive_allowlisted_tool() {
        let config = ToolCallConfig {
            allowed_tools: vec!["shell.exec".to_owned()],
            max_calls_per_run: 2,
            execution_timeout_ms: 250,
            process_runner: default_process_runner_policy(),
        };
        let mut budget = config.max_calls_per_run;

        let decision = decide_tool_call(&config, &mut budget, "user:ops", "shell.exec");

        assert!(!decision.allowed, "sensitive allowlisted tool must require explicit approval");
        assert_eq!(budget, 2, "denied decisions must not consume budget");
        assert!(decision.reason.contains("sensitive action blocked by default"));
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
        if std::process::Command::new("rustc").arg("--version").output().is_err() {
            return;
        }
        let config = ToolCallConfig {
            allowed_tools: vec!["palyra.process.run".to_owned()],
            max_calls_per_run: 1,
            execution_timeout_ms: 2_000,
            process_runner: SandboxProcessRunnerPolicy {
                enabled: true,
                workspace_root: std::env::current_dir().expect("current_dir should resolve"),
                allowed_executables: vec!["rustc".to_owned()],
                allowed_egress_hosts: Vec::new(),
                allowed_dns_suffixes: Vec::new(),
                cpu_time_limit_ms: 2_000,
                memory_limit_bytes: 128 * 1024 * 1024,
                max_output_bytes: 64 * 1024,
            },
        };

        let outcome = execute_tool_call(
            &config,
            "01ARZ3NDEKTSV4RRFFQ69G5FA2",
            "palyra.process.run",
            br#"{"command":"rustc","args":["--version"]}"#,
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
                allowed_executables: vec!["rustc".to_owned()],
                allowed_egress_hosts: Vec::new(),
                allowed_dns_suffixes: Vec::new(),
                cpu_time_limit_ms: 2_000,
                memory_limit_bytes: 128 * 1024 * 1024,
                max_output_bytes: 64 * 1024,
            },
        };

        let outcome = execute_tool_call(
            &config,
            "01ARZ3NDEKTSV4RRFFQ69G5FA2",
            "palyra.process.run",
            br#"{"command":"rustc","args":["../outside.txt"]}"#,
        )
        .await;

        assert!(!outcome.success, "sandbox runner must block traversal path");
        assert_eq!(outcome.attestation.executor, "sandbox_tier_b");
        assert!(outcome.error.contains("path traversal"));
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
