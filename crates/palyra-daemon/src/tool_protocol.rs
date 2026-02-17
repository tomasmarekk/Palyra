use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use ulid::Ulid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCallConfig {
    pub allowed_tools: Vec<String>,
    pub max_calls_per_run: u32,
    pub execution_timeout_ms: u64,
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
}

const POLICY_DENY_REASON: &str = "tool execution denied by default: tool is not allowlisted";
const BUDGET_DENY_REASON: &str = "tool execution budget exhausted for run";
const TOOL_MAX_SLEEP_MS: u64 = 5_000;

pub fn tool_policy_snapshot(config: &ToolCallConfig) -> ToolCallPolicySnapshot {
    ToolCallPolicySnapshot {
        allowed_tools: config.allowed_tools.clone(),
        max_calls_per_run: config.max_calls_per_run,
        execution_timeout_ms: config.execution_timeout_ms,
    }
}

pub fn decide_tool_call(
    config: &ToolCallConfig,
    remaining_budget: &mut u32,
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

    if !config.allowed_tools.iter().any(|allowed| allowed == tool_name) {
        return ToolDecision {
            allowed: false,
            reason: POLICY_DENY_REASON.to_owned(),
            approval_required: true,
            policy_enforced: true,
        };
    }

    *remaining_budget = remaining_budget.saturating_sub(1);
    ToolDecision {
        allowed: true,
        reason: "tool is allowlisted by runtime policy".to_owned(),
        approval_required: true,
        policy_enforced: true,
    }
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
    let raw = match tokio::time::timeout(timeout, run_allowlisted_tool(tool_name, input_json)).await
    {
        Ok(result) => match result {
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
        Err(_) => ToolExecutionRawResult {
            success: false,
            output_json: b"{}".to_vec(),
            error: format!("tool execution timed out after {}ms", config.execution_timeout_ms),
            timed_out: true,
            executor: "builtin".to_owned(),
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

async fn run_allowlisted_tool(tool_name: &str, input_json: &[u8]) -> Result<Vec<u8>, String> {
    match tool_name {
        "palyra.echo" => execute_echo_tool(input_json),
        "palyra.sleep" => execute_sleep_tool(input_json).await,
        _ => Err("allowlisted tool is not implemented by runtime executor".to_owned()),
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
    hasher.update(proposal_id.as_bytes());
    hasher.update(b"|");
    hasher.update(tool_name.as_bytes());
    hasher.update(b"|");
    hasher.update(input_json);
    hasher.update(b"|");
    hasher.update(if success { b"1" } else { b"0" });
    hasher.update(b"|");
    hasher.update(output_json);
    hasher.update(b"|");
    hasher.update(error.as_bytes());
    hasher.update(b"|");
    hasher.update(if timed_out { b"1" } else { b"0" });
    hasher.update(b"|");
    hasher.update(executor.as_bytes());
    hasher.update(b"|");
    hasher.update(executed_at_unix_ms.to_string().as_bytes());
    format!("{:x}", hasher.finalize())
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

    fn allowlisted_config() -> ToolCallConfig {
        ToolCallConfig {
            allowed_tools: vec!["palyra.echo".to_owned(), "palyra.sleep".to_owned()],
            max_calls_per_run: 2,
            execution_timeout_ms: 250,
        }
    }

    #[test]
    fn decide_tool_call_enforces_deny_by_default_policy() {
        let config = ToolCallConfig {
            allowed_tools: Vec::new(),
            max_calls_per_run: 2,
            execution_timeout_ms: 250,
        };
        let mut budget = 2;
        let decision = decide_tool_call(&config, &mut budget, "palyra.echo");
        assert!(!decision.allowed);
        assert_eq!(budget, 2, "denied decisions must not consume budget");
        assert!(decision.reason.contains("denied by default"));
    }

    #[test]
    fn decide_tool_call_consumes_budget_for_allowed_tools() {
        let config = allowlisted_config();
        let mut budget = config.max_calls_per_run;
        let first = decide_tool_call(&config, &mut budget, "palyra.echo");
        assert!(first.allowed);
        assert_eq!(budget, 1);
        let second = decide_tool_call(&config, &mut budget, "palyra.sleep");
        assert!(second.allowed);
        assert_eq!(budget, 0);
        let third = decide_tool_call(&config, &mut budget, "palyra.echo");
        assert!(!third.allowed, "third call should be denied by budget");
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
}
