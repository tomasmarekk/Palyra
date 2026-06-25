//! Nested tool-call RPC bridge for tool programs.
//!
//! Executes a single grant-checked child tool call on behalf of a running
//! tool program (`tool_program.rs`), re-running the full proposal security
//! evaluation per call so a program cannot escalate past its parent proposal.
//! Also ships the stdio-JSONL Python SDK source and the bridge context handed
//! to sandboxed program code.

use std::{collections::BTreeMap, collections::BTreeSet, sync::Arc, time::Duration};

use palyra_common::redaction::{redact_auth_error, redact_url_segments_in_text};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{
    application::{
        execution_gate::ToolProposalApprovalState,
        tool_security::{
            evaluate_tool_proposal_security, resolve_tool_proposal_decision_for_context,
            ResolvedToolProposalDecision, ToolProposalSecurityEvaluation,
        },
    },
    gateway::{
        execute_tool_with_runtime_dispatch, GatewayRuntimeState, SharedToolBudget,
        ToolRuntimeExecutionContext, APPROVAL_CHANNEL_UNAVAILABLE_REASON, HTTP_FETCH_TOOL_NAME,
    },
    tool_protocol::{self, ToolAttestation},
    transport::grpc::auth::RequestContext,
};

/// Wire schema version for tool RPC requests and responses; any mismatch
/// fails closed in [`execute_granted_tool_rpc_call`].
pub(crate) const TOOL_RPC_SCHEMA_VERSION: u32 = 1;

/// One child tool call requested by a tool program step or bridge client.
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

/// Caller-declared scopes and artifact references attached to a call; sizes
/// are bounded by [`execute_granted_tool_rpc_call`] validation.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub(crate) struct ToolRpcScope {
    #[serde(default)]
    pub scopes: Vec<String>,
    #[serde(default)]
    pub allowed_artifact_refs: Vec<String>,
}

/// How much of the child tool output becomes model-visible: the full parsed
/// output, a redacted summary, or only an artifact requirement marker.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ToolRpcResultProjection {
    #[default]
    ModelVisible,
    SummaryOnly,
    ArtifactOnly,
}

/// Terminal status of a tool RPC call.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ToolRpcStatus {
    Completed,
    Denied,
    Failed,
    TimedOut,
}

/// Result envelope returned to the program for one tool RPC call; denials and
/// failures are encoded in `status`/`error` rather than as transport errors.
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

/// Serializable projection of a child tool execution attestation.
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

/// Connection metadata handed to sandboxed Python program code: the IPC
/// shape, the scoped tool grant list, and non-secret environment variables.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct PythonToolRpcBridgeContext {
    pub schema_version: u32,
    pub job_id: String,
    pub program_id: String,
    pub ipc: String,
    pub allowed_tools: Vec<String>,
    pub environment: BTreeMap<String, String>,
}

/// Executes one grant-checked child tool call on behalf of a tool program.
///
/// Validates the request envelope, requires `tool_name` to be in the program
/// grant set, then re-runs the full proposal security evaluation under a
/// derived child proposal id before dispatching. Never returns a transport
/// error: every denial, failure, and timeout is folded into the returned
/// [`ToolRpcResponse`].
pub(crate) async fn execute_granted_tool_rpc_call(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    parent_proposal_id: &str,
    grants: &BTreeSet<String>,
    remaining_tool_budget: Option<SharedToolBudget>,
    request: ToolRpcRequest,
) -> (ToolRpcResponse, u32) {
    if let Err(error) = validate_tool_rpc_request(&request) {
        return (denied_response(request, error, false), 0);
    }
    if !grants.contains(&request.tool_name) {
        return (
            denied_response(
                request,
                "tool rpc call is not in the program grant set".to_owned(),
                false,
            ),
            0,
        );
    }

    let input_bytes = match serde_json::to_vec(&request.arguments) {
        Ok(bytes) => bytes,
        Err(error) => {
            return (
                failed_response(
                    request,
                    format!("failed to serialize tool rpc arguments: {error}"),
                    None,
                ),
                0,
            );
        }
    };

    // Deriving the child proposal id from the parent keeps journal entries
    // and attestations for nested calls correlated to the outer proposal.
    let child_proposal_id = format!("{parent_proposal_id}:{}", request.call_id);
    let request_context = RequestContext {
        principal: context.principal.to_owned(),
        device_id: context.device_id.to_owned(),
        channel: context.channel.map(ToOwned::to_owned),
    };
    let ToolProposalSecurityEvaluation {
        skill_context,
        skill_gate_decision,
        approval_subject_id: _,
        proposal_approval_required,
        effective_posture,
        backend_selection,
    } = evaluate_tool_proposal_security(
        runtime_state,
        &request_context,
        context.session_id,
        context.run_id,
        child_proposal_id.as_str(),
        request.tool_name.as_str(),
        input_bytes.as_slice(),
    )
    .await;
    let (ResolvedToolProposalDecision { decision, gate_report: _ }, mut budget_debit) =
        with_tool_rpc_budget(remaining_tool_budget.as_ref(), 1, |remaining_budget| {
            resolve_tool_proposal_decision_for_context(
                runtime_state,
                &request_context,
                context.channel,
                context.session_id,
                context.run_id,
                request.tool_name.as_str(),
                skill_context.as_ref(),
                remaining_budget,
                skill_gate_decision,
                proposal_approval_required,
                &effective_posture,
                &backend_selection,
                ToolProposalApprovalState::default(),
            )
        });
    let child_tool_requires_approval =
        tool_protocol::tool_requires_approval(request.tool_name.as_str());
    let mut execution_decision = decision;
    let mut inherits_parent_approval = false;
    let mut inherited_approval_policy_checked = false;
    if child_tool_may_inherit_parent_approval(request.tool_name.as_str(), &execution_decision) {
        inherited_approval_policy_checked = true;
        let mut inherited_approval_budget = 1;
        let inherited_approval_decision = tool_protocol::decide_tool_call(
            &runtime_state.config.tool_call,
            &mut inherited_approval_budget,
            &tool_protocol::ToolRequestContext {
                principal: request_context.principal.clone(),
                device_id: Some(request_context.device_id.clone()),
                channel: context.channel.map(ToOwned::to_owned),
                session_id: Some(context.session_id.to_owned()),
                run_id: Some(context.run_id.to_owned()),
                skill_id: skill_context
                    .as_ref()
                    .map(crate::gateway::ToolSkillContext::skill_id)
                    .map(ToOwned::to_owned),
            },
            request.tool_name.as_str(),
            true,
        );
        if inherited_approval_decision.allowed {
            inherits_parent_approval = true;
            execution_decision.allowed = true;
            execution_decision.approval_required = false;
            execution_decision.reason = format!(
                "parent tool program approval inherited for child tool={}; original_reason={}; post_approval_reason={}",
                request.tool_name, execution_decision.reason, inherited_approval_decision.reason
            );
        } else {
            execution_decision = inherited_approval_decision;
        }
    }
    // A nested call has no operator to prompt, so anything approval-shaped
    // (proposal gate, tool metadata, or resolved decision) fails closed here
    // instead of suspending the program. `palyra.http.fetch` is the one
    // approval-required child allowed to inherit the already-approved parent
    // program, but only after a post-approval policy check proves the child
    // is runtime-allowlisted; it still runs through normal fetch egress and
    // content policy.
    let child_tool_requires_standalone_approval = child_tool_requires_approval
        && !inherited_approval_policy_checked
        && (request.tool_name != HTTP_FETCH_TOOL_NAME
            || execution_decision.allowed
            || execution_decision.approval_required);
    if (((proposal_approval_required || child_tool_requires_standalone_approval)
        && !inherited_approval_policy_checked)
        || execution_decision.approval_required)
        && !inherits_parent_approval
    {
        budget_debit.refund();
        let denial_reason = nested_approval_denial_reason(
            request.tool_name.as_str(),
            execution_decision.reason.as_str(),
        );
        return (denied_response(request, denial_reason, true), 0);
    }
    if !execution_decision.allowed {
        budget_debit.refund();
        return (denied_response(request, execution_decision.reason, false), 0);
    }

    let timeout = request.timeout_ms.map(Duration::from_millis);
    let child_context = ToolRuntimeExecutionContext {
        execution_backend: backend_selection.resolution.resolved,
        backend_reason_code: backend_selection.resolution.reason_code.as_str(),
        ..context
    };
    // Box::pin breaks the otherwise infinitely sized recursive future:
    // dispatch can re-enter tool programs, which re-enter this function.
    let execution = Box::pin(execute_tool_with_runtime_dispatch(
        runtime_state,
        child_context,
        child_proposal_id.as_str(),
        request.tool_name.as_str(),
        input_bytes.as_slice(),
        remaining_tool_budget.clone(),
    ));
    let outcome = match timeout {
        Some(timeout) => match tokio::time::timeout(timeout, execution).await {
            Ok(outcome) => outcome,
            Err(_) => {
                return (
                    ToolRpcResponse {
                        schema_version: TOOL_RPC_SCHEMA_VERSION,
                        call_id: request.call_id,
                        tool_name: request.tool_name,
                        status: ToolRpcStatus::TimedOut,
                        success: false,
                        decision_reason: execution_decision.reason,
                        approval_required: execution_decision.approval_required,
                        output: json!({}),
                        error: "tool rpc call timed out".to_owned(),
                        redacted_preview: String::new(),
                        attestation: None,
                    },
                    1,
                );
            }
        },
        None => execution.await,
    };

    let redacted_preview = summarize_rpc_output(outcome.output_json.as_slice(), 1024);
    (
        ToolRpcResponse {
            schema_version: TOOL_RPC_SCHEMA_VERSION,
            call_id: request.call_id,
            tool_name: request.tool_name,
            status: if outcome.success { ToolRpcStatus::Completed } else { ToolRpcStatus::Failed },
            success: outcome.success,
            decision_reason: execution_decision.reason,
            approval_required: execution_decision.approval_required,
            output: project_rpc_output(
                outcome.output_json.as_slice(),
                request.result_projection,
                redacted_preview.as_str(),
            ),
            error: outcome.error,
            redacted_preview,
            attestation: Some(ToolRpcAttestation::from(&outcome.attestation)),
        },
        1,
    )
}

/// Runs `resolve` against the shared legacy budget counter, or against a
/// local fallback counter when the caller did not thread one.
///
/// The counter is retained for audit compatibility; step-count limits are not
/// terminal for agentic execution.
fn with_tool_rpc_budget<T>(
    remaining_tool_budget: Option<&SharedToolBudget>,
    fallback_budget: u32,
    resolve: impl FnOnce(&mut u32) -> T,
) -> (T, ToolRpcBudgetDebit) {
    if let Some(remaining_tool_budget) = remaining_tool_budget {
        // A poisoned lock only means another worker panicked mid-update; the
        // budget counter itself stays valid, so recover rather than panic.
        let mut guard =
            remaining_tool_budget.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let before = *guard;
        let result = resolve(&mut guard);
        let consumed = before.saturating_sub(*guard);
        return (
            result,
            ToolRpcBudgetDebit { consumed, shared_budget: Some(Arc::clone(remaining_tool_budget)) },
        );
    }

    let mut local_budget = fallback_budget;
    let before = local_budget;
    let result = resolve(&mut local_budget);
    let consumed = before.saturating_sub(local_budget);
    (result, ToolRpcBudgetDebit { consumed, shared_budget: None })
}

#[derive(Debug)]
struct ToolRpcBudgetDebit {
    consumed: u32,
    shared_budget: Option<SharedToolBudget>,
}

impl ToolRpcBudgetDebit {
    fn refund(&mut self) {
        if self.consumed == 0 {
            return;
        }
        if let Some(shared_budget) = &self.shared_budget {
            let mut guard = shared_budget.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            *guard = guard.saturating_add(self.consumed);
        }
        self.consumed = 0;
    }
}

/// Returns the embedded Python SDK source for the stdio-JSONL tool RPC
/// bridge, shipped verbatim into sandboxed program workspaces.
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

/// Builds the non-secret bridge context (IPC shape, scoped grant list, and
/// bridge environment variables) surfaced to sandboxed Python program code.
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

fn nested_approval_denial_reason(tool_name: &str, original_reason: &str) -> String {
    format!(
        "tool program cannot self-approve approval-required child tool; tool={tool_name}; original_reason={original_reason}"
    )
}

fn child_tool_may_inherit_parent_approval(
    tool_name: &str,
    decision: &tool_protocol::ToolDecision,
) -> bool {
    tool_name == HTTP_FETCH_TOOL_NAME
        && decision.approval_required
        && (decision.allowed || child_decision_is_approval_gate_only(decision.reason.as_str()))
}

fn child_decision_is_approval_gate_only(reason: &str) -> bool {
    reason.contains(APPROVAL_CHANNEL_UNAVAILABLE_REASON)
        || reason.contains("approval required (pending approval_id=")
        || reason.contains("explicit user approval required")
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
        build_python_tool_rpc_bridge_context, child_tool_may_inherit_parent_approval,
        python_tool_rpc_sdk_source, TOOL_RPC_SCHEMA_VERSION,
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

    #[test]
    fn only_http_fetch_child_can_request_parent_approval_inheritance() {
        let approval_gate_decision = crate::tool_protocol::ToolDecision {
            allowed: false,
            reason: crate::gateway::APPROVAL_CHANNEL_UNAVAILABLE_REASON.to_owned(),
            approval_required: true,
            policy_enforced: true,
        };

        assert!(child_tool_may_inherit_parent_approval(
            crate::gateway::HTTP_FETCH_TOOL_NAME,
            &approval_gate_decision
        ));
        assert!(!child_tool_may_inherit_parent_approval(
            crate::gateway::PROCESS_RUNNER_TOOL_NAME,
            &approval_gate_decision
        ));
    }
}
