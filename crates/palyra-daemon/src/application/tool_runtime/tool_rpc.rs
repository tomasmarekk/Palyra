use std::{collections::BTreeMap, collections::BTreeSet, sync::Arc, time::Duration};

use palyra_common::redaction::{redact_auth_error, redact_url_segments_in_text};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{
    gateway::{
        execute_tool_with_runtime_dispatch, GatewayRuntimeState, ToolRuntimeExecutionContext,
    },
    tool_protocol::{decide_tool_call, ToolAttestation, ToolRequestContext},
};

pub(crate) const TOOL_RPC_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct ToolRpcRequest {
    pub schema_version: u32,
    pub call_id: String,
    pub tool_name: String,
    #[serde(default)]
    pub arguments: Value,
    #[serde(default)]
    pub scope: ToolRpcScope,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub result_projection: ToolRpcResultProjection,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub(crate) struct ToolRpcScope {
    #[serde(default)]
    pub scopes: Vec<String>,
    #[serde(default)]
    pub allowed_artifact_refs: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ToolRpcResultProjection {
    #[default]
    ModelVisible,
    SummaryOnly,
    ArtifactOnly,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ToolRpcStatus {
    Completed,
    Denied,
    Failed,
    TimedOut,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ToolRpcResponse {
    pub schema_version: u32,
    pub call_id: String,
    pub tool_name: String,
    pub status: ToolRpcStatus,
    pub success: bool,
    pub decision_reason: String,
    pub approval_required: bool,
    pub output: Value,
    pub error: String,
    pub redacted_preview: String,
    pub attestation: Option<ToolRpcAttestation>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ToolRpcAttestation {
    pub attestation_id: String,
    pub execution_sha256: String,
    pub executed_at_unix_ms: i64,
    pub timed_out: bool,
    pub executor: String,
    pub sandbox_enforcement: String,
}

impl From<&ToolAttestation> for ToolRpcAttestation {
    fn from(value: &ToolAttestation) -> Self {
        Self {
            attestation_id: value.attestation_id.clone(),
            execution_sha256: value.execution_sha256.clone(),
            executed_at_unix_ms: value.executed_at_unix_ms,
            timed_out: value.timed_out,
            executor: value.executor.clone(),
            sandbox_enforcement: value.sandbox_enforcement.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct PythonToolRpcBridgeContext {
    pub schema_version: u32,
    pub job_id: String,
    pub program_id: String,
    pub ipc: String,
    pub allowed_tools: Vec<String>,
    pub environment: BTreeMap<String, String>,
}

pub(crate) async fn execute_granted_tool_rpc_call(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    parent_proposal_id: &str,
    grants: &BTreeSet<String>,
    request: ToolRpcRequest,
) -> ToolRpcResponse {
    if let Err(error) = validate_tool_rpc_request(&request) {
        return denied_response(request, error, false);
    }
    if !grants.contains(&request.tool_name) {
        return denied_response(
            request,
            "tool rpc call is not in the program grant set".to_owned(),
            false,
        );
    }

    let input_bytes = match serde_json::to_vec(&request.arguments) {
        Ok(bytes) => bytes,
        Err(error) => {
            return failed_response(
                request,
                format!("failed to serialize tool rpc arguments: {error}"),
                None,
            );
        }
    };

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
        request.tool_name.as_str(),
        false,
    );
    if !decision.allowed {
        return ToolRpcResponse {
            schema_version: TOOL_RPC_SCHEMA_VERSION,
            call_id: request.call_id,
            tool_name: request.tool_name,
            status: ToolRpcStatus::Denied,
            success: false,
            decision_reason: decision.reason.clone(),
            approval_required: decision.approval_required,
            output: json!({}),
            error: decision.reason,
            redacted_preview: String::new(),
            attestation: None,
        };
    }

    let child_proposal_id = format!("{parent_proposal_id}:{}", request.call_id);
    let timeout = request.timeout_ms.map(Duration::from_millis);
    let execution = Box::pin(execute_tool_with_runtime_dispatch(
        runtime_state,
        context,
        child_proposal_id.as_str(),
        request.tool_name.as_str(),
        input_bytes.as_slice(),
    ));
    let outcome = match timeout {
        Some(timeout) => match tokio::time::timeout(timeout, execution).await {
            Ok(outcome) => outcome,
            Err(_) => {
                return ToolRpcResponse {
                    schema_version: TOOL_RPC_SCHEMA_VERSION,
                    call_id: request.call_id,
                    tool_name: request.tool_name,
                    status: ToolRpcStatus::TimedOut,
                    success: false,
                    decision_reason: decision.reason,
                    approval_required: decision.approval_required,
                    output: json!({}),
                    error: "tool rpc call timed out".to_owned(),
                    redacted_preview: String::new(),
                    attestation: None,
                };
            }
        },
        None => execution.await,
    };

    let redacted_preview = summarize_rpc_output(outcome.output_json.as_slice(), 1024);
    ToolRpcResponse {
        schema_version: TOOL_RPC_SCHEMA_VERSION,
        call_id: request.call_id,
        tool_name: request.tool_name,
        status: if outcome.success { ToolRpcStatus::Completed } else { ToolRpcStatus::Failed },
        success: outcome.success,
        decision_reason: decision.reason,
        approval_required: decision.approval_required,
        output: project_rpc_output(
            outcome.output_json.as_slice(),
            request.result_projection,
            redacted_preview.as_str(),
        ),
        error: outcome.error,
        redacted_preview,
        attestation: Some(ToolRpcAttestation::from(&outcome.attestation)),
    }
}

pub(crate) fn python_tool_rpc_sdk_source() -> &'static str {
    r#"import json
import sys


class ToolRpcError(RuntimeError):
    def __init__(self, message, response=None):
        super().__init__(message)
        self.response = response or {}


class ToolRpcClient:
    def __init__(self, stdin=None, stdout=None):
        self._stdin = stdin or sys.stdin
        self._stdout = stdout or sys.stdout

    def call(self, tool_name, arguments=None, timeout_ms=None):
        request = {
            "schema_version": 1,
            "tool_name": tool_name,
            "arguments": arguments or {},
        }
        if timeout_ms is not None:
            request["timeout_ms"] = int(timeout_ms)
        self._stdout.write(json.dumps(request, separators=(",", ":")) + "\n")
        self._stdout.flush()
        line = self._stdin.readline()
        if not line:
            raise ToolRpcError("tool rpc bridge closed")
        response = json.loads(line)
        if not response.get("success", False):
            raise ToolRpcError(response.get("error", "tool rpc call failed"), response)
        return response.get("output")
"#
}

pub(crate) fn build_python_tool_rpc_bridge_context(
    job_id: &str,
    program_id: &str,
    grants: &BTreeSet<String>,
) -> PythonToolRpcBridgeContext {
    let environment = BTreeMap::from([
        ("PALYRA_TOOL_RPC_SCHEMA_VERSION".to_owned(), TOOL_RPC_SCHEMA_VERSION.to_string()),
        ("PALYRA_TOOL_RPC_IPC".to_owned(), "stdio-jsonl".to_owned()),
        ("PALYRA_TOOL_RPC_JOB_ID".to_owned(), job_id.to_owned()),
        ("PALYRA_TOOL_RPC_PROGRAM_ID".to_owned(), program_id.to_owned()),
    ]);
    PythonToolRpcBridgeContext {
        schema_version: TOOL_RPC_SCHEMA_VERSION,
        job_id: job_id.to_owned(),
        program_id: program_id.to_owned(),
        ipc: "stdio-jsonl".to_owned(),
        allowed_tools: grants.iter().cloned().collect(),
        environment,
    }
}

fn validate_tool_rpc_request(request: &ToolRpcRequest) -> Result<(), String> {
    if request.schema_version != TOOL_RPC_SCHEMA_VERSION {
        return Err(format!("tool rpc schema_version={} is unsupported", request.schema_version));
    }
    if request.call_id.trim().is_empty() || request.call_id.len() > 128 {
        return Err("tool rpc call_id must be bounded and non-empty".to_owned());
    }
    if request.tool_name.trim().is_empty() || request.tool_name.len() > 256 {
        return Err("tool rpc tool_name must be bounded and non-empty".to_owned());
    }
    if request.timeout_ms == Some(0) {
        return Err("tool rpc timeout_ms must be positive".to_owned());
    }
    if request.scope.scopes.iter().any(|scope| scope.len() > 128)
        || request.scope.allowed_artifact_refs.iter().any(|artifact_ref| artifact_ref.len() > 512)
    {
        return Err("tool rpc scope values must be bounded".to_owned());
    }
    Ok(())
}

fn denied_response(
    request: ToolRpcRequest,
    error: String,
    approval_required: bool,
) -> ToolRpcResponse {
    ToolRpcResponse {
        schema_version: TOOL_RPC_SCHEMA_VERSION,
        call_id: request.call_id,
        tool_name: request.tool_name,
        status: ToolRpcStatus::Denied,
        success: false,
        decision_reason: error.clone(),
        approval_required,
        output: json!({}),
        error,
        redacted_preview: String::new(),
        attestation: None,
    }
}

fn failed_response(
    request: ToolRpcRequest,
    error: String,
    attestation: Option<ToolRpcAttestation>,
) -> ToolRpcResponse {
    ToolRpcResponse {
        schema_version: TOOL_RPC_SCHEMA_VERSION,
        call_id: request.call_id,
        tool_name: request.tool_name,
        status: ToolRpcStatus::Failed,
        success: false,
        decision_reason: "tool rpc bridge failure".to_owned(),
        approval_required: false,
        output: json!({}),
        error,
        redacted_preview: String::new(),
        attestation,
    }
}

fn project_rpc_output(
    output_json: &[u8],
    projection: ToolRpcResultProjection,
    redacted_preview: &str,
) -> Value {
    match projection {
        ToolRpcResultProjection::ModelVisible => serde_json::from_slice(output_json)
            .unwrap_or_else(|_| json!({ "preview": redacted_preview })),
        ToolRpcResultProjection::SummaryOnly => json!({ "summary": redacted_preview }),
        ToolRpcResultProjection::ArtifactOnly => json!({ "artifact_required": true }),
    }
}

fn summarize_rpc_output(output_json: &[u8], max_bytes: usize) -> String {
    let raw = String::from_utf8_lossy(output_json);
    let redacted = redact_url_segments_in_text(redact_auth_error(raw.as_ref()).as_str());
    if redacted.len() <= max_bytes {
        return redacted;
    }
    let mut end = max_bytes.min(redacted.len());
    while end > 0 && !redacted.is_char_boundary(end) {
        end -= 1;
    }
    let mut output = redacted[..end].to_owned();
    output.push_str("...");
    output
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{
        build_python_tool_rpc_bridge_context, python_tool_rpc_sdk_source, TOOL_RPC_SCHEMA_VERSION,
    };

    #[test]
    fn python_bridge_context_exports_only_scoped_handles() {
        let grants = BTreeSet::from(["palyra.echo".to_owned(), "palyra.http.fetch".to_owned()]);
        let context = build_python_tool_rpc_bridge_context("job-1", "program-1", &grants);
        assert_eq!(context.schema_version, TOOL_RPC_SCHEMA_VERSION);
        assert_eq!(context.environment["PALYRA_TOOL_RPC_IPC"], "stdio-jsonl");
        let serialized = serde_json::to_string(&context).expect("context should serialize");
        assert!(!serialized.to_ascii_lowercase().contains("secret"));
        assert!(!serialized.to_ascii_lowercase().contains("token"));
        assert!(serialized.contains("palyra.echo"));
    }

    #[test]
    fn python_sdk_uses_jsonl_without_env_secrets() {
        let source = python_tool_rpc_sdk_source();
        assert!(source.contains("ToolRpcClient"));
        assert!(source.contains("json.dumps"));
        assert!(!source.contains("API_KEY"));
        assert!(!source.contains("TOKEN"));
    }
}
