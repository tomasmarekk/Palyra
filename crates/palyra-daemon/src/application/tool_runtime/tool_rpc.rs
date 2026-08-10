//! Nested tool-call RPC bridge for tool programs.
//!
//! Executes a single grant-checked child tool call on behalf of a running
//! tool program (`tool_program.rs`), re-running the full proposal security
//! evaluation per call so a program cannot escalate past its parent proposal.
//! Also ships the stdio-JSONL Python SDK source and the bridge context handed
//! to sandboxed program code.

use std::{
    collections::BTreeMap,
    collections::BTreeSet,
    fs::{self, OpenOptions},
    future::Future,
    io::{Read, Write},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;

use palyra_common::runtime_contracts::CancellationContextV1;
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
        execute_tool_with_runtime_dispatch_with_cancellation_and_progress, GatewayRuntimeState,
        SharedToolBudget, ToolRuntimeDispatchControls, ToolRuntimeExecutionContext,
        APPROVAL_CHANNEL_UNAVAILABLE_REASON, HTTP_FETCH_TOOL_NAME,
    },
    tool_protocol::{self, ToolAttestation},
    transport::grpc::auth::RequestContext,
};

/// Wire schema version for tool RPC requests and responses; any mismatch
/// fails closed in [`execute_granted_tool_rpc_call`].
pub(crate) const TOOL_RPC_SCHEMA_VERSION: u32 = 1;
const MAX_TOOL_RPC_FILE_REQUEST_BYTES: u64 = 1024 * 1024;
#[cfg(windows)]
const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;

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

impl ToolRpcStatus {
    #[allow(dead_code)]
    const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Denied => "denied",
            Self::Failed => "failed",
            Self::TimedOut => "timed_out",
        }
    }
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
    pub transports: Vec<PythonToolRpcTransportDescriptor>,
    pub allowed_tools: Vec<String>,
    pub environment: BTreeMap<String, String>,
}

/// One supported IPC transport for script-mode tool RPC.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct PythonToolRpcTransportDescriptor {
    pub kind: ToolRpcTransportKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_dir: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_dir: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact_channel: Option<String>,
    pub orphan_timeout_ms: u64,
}

impl PythonToolRpcTransportDescriptor {
    pub(crate) fn stdio_jsonl(orphan_timeout_ms: u64) -> Self {
        Self {
            kind: ToolRpcTransportKind::Stdio,
            request_dir: None,
            response_dir: None,
            artifact_channel: None,
            orphan_timeout_ms,
        }
    }

    pub(crate) fn file_jsonl(
        request_dir: impl Into<String>,
        response_dir: impl Into<String>,
        orphan_timeout_ms: u64,
    ) -> Self {
        Self {
            kind: ToolRpcTransportKind::File,
            request_dir: Some(request_dir.into()),
            response_dir: Some(response_dir.into()),
            artifact_channel: None,
            orphan_timeout_ms,
        }
    }

    pub(crate) fn artifact_jsonl(
        artifact_channel: impl Into<String>,
        orphan_timeout_ms: u64,
    ) -> Self {
        Self {
            kind: ToolRpcTransportKind::Artifact,
            request_dir: None,
            response_dir: None,
            artifact_channel: Some(artifact_channel.into()),
            orphan_timeout_ms,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ToolRpcTransportKind {
    #[serde(rename = "stdio_jsonl")]
    Stdio,
    #[serde(rename = "file_jsonl")]
    File,
    #[serde(rename = "artifact_jsonl")]
    Artifact,
}

impl ToolRpcTransportKind {
    const fn env_value(self) -> &'static str {
        match self {
            Self::Stdio => "stdio-jsonl",
            Self::File => "file-jsonl",
            Self::Artifact => "artifact-jsonl",
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct ToolRpcFileTransportConfig {
    pub request_dir: PathBuf,
    pub response_dir: PathBuf,
    pub orphan_timeout: Duration,
}

#[derive(Debug, Clone, Serialize)]
#[allow(dead_code)]
pub(crate) struct ToolRpcFileTransportSweep {
    pub transport: ToolRpcTransportKind,
    pub processed: usize,
    pub denied: usize,
    pub failed: usize,
    pub orphaned: usize,
    pub responses: Vec<ToolRpcFileTransportAudit>,
}

#[derive(Debug, Clone, Serialize)]
#[allow(dead_code)]
pub(crate) struct ToolRpcFileTransportAudit {
    pub correlation_id: String,
    pub call_id: Option<String>,
    pub tool_name: Option<String>,
    pub status: String,
    pub success: bool,
    pub response_path: String,
    pub reason: String,
}

#[derive(Debug, Serialize)]
#[allow(dead_code)]
struct ToolRpcFileResponseEnvelope {
    schema_version: u32,
    correlation_id: String,
    status: String,
    success: bool,
    error: String,
    response: Option<ToolRpcResponse>,
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
    child_task_parent_context: Option<&CancellationContextV1>,
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
    let timeout_cancellation = timeout.map(|_| Arc::new(AtomicBool::new(false)));
    let child_context = ToolRuntimeExecutionContext {
        execution_backend: backend_selection.resolution.resolved,
        backend_reason_code: backend_selection.resolution.reason_code.as_str(),
        ..context
    };
    // Box::pin breaks the otherwise infinitely sized recursive future:
    // dispatch can re-enter tool programs, which re-enter this function.
    let execution = Box::pin(execute_tool_with_runtime_dispatch_with_cancellation_and_progress(
        runtime_state,
        child_context,
        child_proposal_id.as_str(),
        request.tool_name.as_str(),
        input_bytes.as_slice(),
        ToolRuntimeDispatchControls {
            remaining_tool_budget: remaining_tool_budget.clone(),
            cancellation_requested: timeout_cancellation.clone(),
            process_progress_sink: None,
            cancellation_context: None,
            child_task_parent_context: child_task_parent_context.cloned(),
            expected_dynamic_provenance: None,
        },
    ));
    let outcome = match settle_tool_rpc_execution(
        execution,
        timeout,
        timeout_cancellation.as_deref(),
    )
    .await
    {
        ToolRpcExecutionSettlement::Completed(outcome) => outcome,
        ToolRpcExecutionSettlement::TimedOut(outcome) => {
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
                    error: "tool rpc call timed out after child execution settled".to_owned(),
                    redacted_preview: String::new(),
                    attestation: Some(ToolRpcAttestation::from(&outcome.attestation)),
                },
                1,
            );
        }
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

#[derive(Debug, Clone, PartialEq, Eq)]
enum ToolRpcExecutionSettlement<T> {
    Completed(T),
    TimedOut(T),
}

/// Requests cooperative cancellation at the deadline and keeps ownership of
/// the execution future until every child action has settled.
async fn settle_tool_rpc_execution<F>(
    mut execution: Pin<Box<F>>,
    timeout: Option<Duration>,
    cancellation_requested: Option<&AtomicBool>,
) -> ToolRpcExecutionSettlement<F::Output>
where
    F: Future,
{
    let Some(timeout) = timeout else {
        return ToolRpcExecutionSettlement::Completed(execution.await);
    };
    match tokio::time::timeout(timeout, execution.as_mut()).await {
        Ok(outcome) => ToolRpcExecutionSettlement::Completed(outcome),
        Err(_) => {
            if let Some(cancellation_requested) = cancellation_requested {
                cancellation_requested.store(true, Ordering::Release);
            }
            ToolRpcExecutionSettlement::TimedOut(execution.await)
        }
    }
}

/// Processes one batch of file-backed RPC requests for remote script runtimes.
///
/// Each `*.request.json` file is correlation-bound to a `*.response.json`
/// envelope. Normal requests still execute through
/// [`execute_granted_tool_rpc_call`], so remote file transport cannot expand
/// the parent grant set or bypass nested approval policy.
#[allow(dead_code)]
pub(crate) async fn process_tool_rpc_file_transport_once(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    parent_proposal_id: &str,
    grants: &BTreeSet<String>,
    remaining_tool_budget: Option<SharedToolBudget>,
    config: &ToolRpcFileTransportConfig,
    child_task_parent_context: Option<&CancellationContextV1>,
) -> Result<ToolRpcFileTransportSweep, String> {
    let request_dir = canonicalize_rpc_dir(config.request_dir.as_path(), "request_dir")?;
    fs::create_dir_all(config.response_dir.as_path())
        .map_err(|error| format!("failed to create tool rpc response_dir: {error}"))?;
    let response_dir = canonicalize_rpc_dir(config.response_dir.as_path(), "response_dir")?;
    let mut sweep = ToolRpcFileTransportSweep {
        transport: ToolRpcTransportKind::File,
        processed: 0,
        denied: 0,
        failed: 0,
        orphaned: 0,
        responses: Vec::new(),
    };

    for entry in fs::read_dir(request_dir.as_path())
        .map_err(|error| format!("failed to read tool rpc request_dir: {error}"))?
    {
        let entry =
            entry.map_err(|error| format!("failed to read tool rpc request entry: {error}"))?;
        let request_path = entry.path();
        let Some(correlation_id) = tool_rpc_file_correlation_id(request_path.as_path()) else {
            continue;
        };
        let response_path = response_dir.join(format!("{correlation_id}.response.json"));
        let (input_json, request_metadata) = read_file_rpc_request(request_path.as_path())
            .map_err(|error| {
                format!("failed to read tool rpc request file {}: {error}", request_path.display())
            })?;
        if rpc_request_is_orphaned(&request_metadata, config.orphan_timeout) {
            write_orphaned_file_rpc_response(
                request_path.as_path(),
                response_path.as_path(),
                correlation_id,
                &mut sweep,
            )?;
            continue;
        }

        let request = match serde_json::from_slice::<ToolRpcRequest>(input_json.as_slice()) {
            Ok(request) => request,
            Err(error) => {
                write_failed_file_rpc_response(
                    request_path.as_path(),
                    response_path.as_path(),
                    correlation_id,
                    format!("tool rpc request file is not valid JSON RPC: {error}"),
                    &mut sweep,
                )?;
                continue;
            }
        };
        let call_id = request.call_id.clone();
        let tool_name = request.tool_name.clone();
        let (response, _consumed) = execute_granted_tool_rpc_call(
            runtime_state,
            context,
            parent_proposal_id,
            grants,
            remaining_tool_budget.clone(),
            request,
            child_task_parent_context,
        )
        .await;
        let status = response.status;
        let success = response.success;
        let reason = if response.error.is_empty() {
            response.decision_reason.clone()
        } else {
            response.error.clone()
        };
        write_file_rpc_response(
            response_path.as_path(),
            &ToolRpcFileResponseEnvelope {
                schema_version: TOOL_RPC_SCHEMA_VERSION,
                correlation_id: correlation_id.clone(),
                status: status.as_str().to_owned(),
                success,
                error: if success { String::new() } else { reason.clone() },
                response: Some(response),
            },
        )?;
        mark_rpc_request_processed(request_path.as_path(), correlation_id.as_str(), "processed")?;
        sweep.processed += 1;
        if status == ToolRpcStatus::Denied {
            sweep.denied += 1;
        } else if !success {
            sweep.failed += 1;
        }
        sweep.responses.push(ToolRpcFileTransportAudit {
            correlation_id,
            call_id: Some(call_id),
            tool_name: Some(tool_name),
            status: status.as_str().to_owned(),
            success,
            response_path: response_path.display().to_string(),
            reason,
        });
    }
    Ok(sweep)
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
    r#"import itertools
import json
import os
from pathlib import Path
import sys
import time


_CALL_COUNTER = itertools.count(1)


def _next_call_id(tool_name):
    safe_name = "".join(ch if ch.isalnum() else "_" for ch in tool_name).strip("_")
    if not safe_name:
        safe_name = "tool"
    return "{}_{}".format(safe_name, next(_CALL_COUNTER))


class ToolRpcError(RuntimeError):
    def __init__(self, message, response=None):
        super().__init__(message)
        self.response = response or {}


class ToolRpcClient:
    def __init__(self, stdin=None, stdout=None, request_dir=None, response_dir=None):
        self._stdin = stdin or sys.stdin
        self._stdout = stdout or sys.stdout
        self._transport = os.environ.get("PALYRA_TOOL_RPC_TRANSPORT", "stdio-jsonl")
        self._request_dir = Path(request_dir or os.environ.get("PALYRA_TOOL_RPC_REQUEST_DIR", ""))
        self._response_dir = Path(response_dir or os.environ.get("PALYRA_TOOL_RPC_RESPONSE_DIR", ""))

    def call(
        self,
        tool_name,
        arguments=None,
        timeout_ms=None,
        call_id=None,
        scope=None,
        result_projection=None,
    ):
        request = {
            "schema_version": 1,
            "call_id": call_id or _next_call_id(tool_name),
            "tool_name": tool_name,
            "arguments": arguments or {},
        }
        if timeout_ms is not None:
            request["timeout_ms"] = int(timeout_ms)
        if scope is not None:
            request["scope"] = scope
        if result_projection is not None:
            request["result_projection"] = result_projection
        if self._transport == "file-jsonl":
            return self._call_file_jsonl(request, timeout_ms)
        return self._call_stdio_jsonl(request)

    def _call_stdio_jsonl(self, request):
        self._stdout.write(json.dumps(request, separators=(",", ":")) + "\n")
        self._stdout.flush()
        line = self._stdin.readline()
        if not line:
            raise ToolRpcError("tool rpc bridge closed")
        response = json.loads(line)
        if not response.get("success", False):
            raise ToolRpcError(response.get("error", "tool rpc call failed"), response)
        return response.get("output")

    def _call_file_jsonl(self, request, timeout_ms):
        if not self._request_dir or not self._response_dir:
            raise ToolRpcError("tool rpc file transport directories are not configured")
        call_id = request["call_id"]
        request_path = self._request_dir / (call_id + ".request.json")
        response_path = self._response_dir / (call_id + ".response.json")
        request_path.write_text(json.dumps(request, separators=(",", ":")), encoding="utf-8")
        deadline = time.monotonic() + ((timeout_ms or 30000) / 1000.0)
        while time.monotonic() <= deadline:
            if response_path.exists():
                envelope = json.loads(response_path.read_text(encoding="utf-8"))
                response = envelope.get("response") or {}
                if not envelope.get("success", False):
                    raise ToolRpcError(envelope.get("error", "tool rpc call failed"), response)
                return response.get("output")
            time.sleep(0.025)
        raise ToolRpcError("tool rpc file transport timed out")
"#
}

/// Builds the generated `palyra_tools.py` module for a concrete grant set.
pub(crate) fn python_tool_rpc_sdk_source_for_tools(grants: &BTreeSet<String>) -> String {
    let mut source = python_tool_rpc_sdk_source().to_owned();
    source.push_str("\n\nDEFAULT_CLIENT = ToolRpcClient()\n\n");
    for (tool_name, wrapper_name) in python_tool_rpc_sdk_wrappers(grants) {
        source.push_str(&format!(
            r#"
def {wrapper_name}(arguments=None, timeout_ms=None, call_id=None, scope=None, result_projection=None):
    return DEFAULT_CLIENT.call(
        "{tool_name}",
        arguments,
        timeout_ms=timeout_ms,
        call_id=call_id,
        scope=scope,
        result_projection=result_projection,
    )
"#,
        ));
    }
    source
}

/// Returns the tool-name to Python-wrapper map used by `palyra_tools.py`.
pub(crate) fn python_tool_rpc_sdk_wrappers(grants: &BTreeSet<String>) -> BTreeMap<String, String> {
    let mut wrappers = BTreeMap::new();
    let mut used_names = BTreeSet::new();
    for tool_name in grants {
        let base_name = python_tool_rpc_wrapper_name(tool_name);
        let mut wrapper_name = base_name.clone();
        let mut suffix = 2_u32;
        while !used_names.insert(wrapper_name.clone()) {
            wrapper_name = format!("{base_name}_{suffix}");
            suffix = suffix.saturating_add(1);
        }
        wrappers.insert(tool_name.clone(), wrapper_name);
    }
    wrappers
}

fn python_tool_rpc_wrapper_name(tool_name: &str) -> String {
    let mut normalized = String::from("call_");
    let mut previous_separator = false;
    for character in tool_name.chars() {
        if character.is_ascii_alphanumeric() {
            normalized.push(character.to_ascii_lowercase());
            previous_separator = false;
        } else if !previous_separator {
            normalized.push('_');
            previous_separator = true;
        }
    }
    while normalized.ends_with('_') {
        normalized.pop();
    }
    if normalized == "call" {
        return "call_tool".to_owned();
    }
    normalized
}

/// Builds the non-secret bridge context (IPC shape, scoped grant list, and
/// bridge environment variables) surfaced to sandboxed Python program code.
pub(crate) fn build_python_tool_rpc_bridge_context_with_transports(
    job_id: &str,
    program_id: &str,
    grants: &BTreeSet<String>,
    transports: Vec<PythonToolRpcTransportDescriptor>,
) -> PythonToolRpcBridgeContext {
    let mut environment = BTreeMap::from([
        ("PALYRA_TOOL_RPC_SCHEMA_VERSION".to_owned(), TOOL_RPC_SCHEMA_VERSION.to_string()),
        ("PALYRA_TOOL_RPC_IPC".to_owned(), "stdio-jsonl".to_owned()),
        ("PALYRA_TOOL_RPC_JOB_ID".to_owned(), job_id.to_owned()),
        ("PALYRA_TOOL_RPC_PROGRAM_ID".to_owned(), program_id.to_owned()),
    ]);
    if let Some(primary_transport) = transports.first() {
        environment.insert(
            "PALYRA_TOOL_RPC_TRANSPORT".to_owned(),
            primary_transport.kind.env_value().to_owned(),
        );
        if let Some(request_dir) = &primary_transport.request_dir {
            environment.insert("PALYRA_TOOL_RPC_REQUEST_DIR".to_owned(), request_dir.clone());
        }
        if let Some(response_dir) = &primary_transport.response_dir {
            environment.insert("PALYRA_TOOL_RPC_RESPONSE_DIR".to_owned(), response_dir.clone());
        }
        if let Some(artifact_channel) = &primary_transport.artifact_channel {
            environment
                .insert("PALYRA_TOOL_RPC_ARTIFACT_CHANNEL".to_owned(), artifact_channel.clone());
        }
    }
    PythonToolRpcBridgeContext {
        schema_version: TOOL_RPC_SCHEMA_VERSION,
        job_id: job_id.to_owned(),
        program_id: program_id.to_owned(),
        ipc: "stdio-jsonl".to_owned(),
        transports,
        allowed_tools: grants.iter().cloned().collect(),
        environment,
    }
}

fn nested_approval_denial_reason(tool_name: &str, original_reason: &str) -> String {
    format!(
        "tool program cannot self-approve approval-required child tool; tool={tool_name}; original_reason={original_reason}"
    )
}

fn canonicalize_rpc_dir(path: &Path, field_name: &str) -> Result<PathBuf, String> {
    let canonical = fs::canonicalize(path)
        .map_err(|error| format!("tool rpc {field_name} must exist and canonicalize: {error}"))?;
    if !canonical.is_dir() {
        return Err(format!("tool rpc {field_name} must be a directory"));
    }
    Ok(canonical)
}

fn tool_rpc_file_correlation_id(path: &Path) -> Option<String> {
    let file_name = path.file_name()?.to_str()?;
    let correlation_id = file_name.strip_suffix(".request.json")?;
    is_safe_rpc_file_id(correlation_id).then(|| correlation_id.to_owned())
}

fn is_safe_rpc_file_id(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn rpc_request_is_orphaned(metadata: &fs::Metadata, orphan_timeout: Duration) -> bool {
    let Ok(modified) = metadata.modified() else {
        return false;
    };
    modified.elapsed().is_ok_and(|age| age >= orphan_timeout)
}

fn read_file_rpc_request(path: &Path) -> Result<(Vec<u8>, fs::Metadata), String> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    #[cfg(windows)]
    options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    let file = options
        .open(path)
        .map_err(|error| format!("request must be an unlinked regular file: {error}"))?;
    let metadata =
        file.metadata().map_err(|error| format!("failed to inspect opened request: {error}"))?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err("request must be a regular file and must not be a symlink".to_owned());
    }
    if metadata.len() > MAX_TOOL_RPC_FILE_REQUEST_BYTES {
        return Err(format!(
            "request exceeds the {MAX_TOOL_RPC_FILE_REQUEST_BYTES}-byte file transport limit"
        ));
    }
    let mut input_json = Vec::with_capacity(
        usize::try_from(metadata.len()).unwrap_or(MAX_TOOL_RPC_FILE_REQUEST_BYTES as usize),
    );
    file.take(MAX_TOOL_RPC_FILE_REQUEST_BYTES.saturating_add(1))
        .read_to_end(&mut input_json)
        .map_err(|error| format!("failed to read bounded request: {error}"))?;
    if u64::try_from(input_json.len()).unwrap_or(u64::MAX) > MAX_TOOL_RPC_FILE_REQUEST_BYTES {
        return Err(format!(
            "request exceeds the {MAX_TOOL_RPC_FILE_REQUEST_BYTES}-byte file transport limit"
        ));
    }
    Ok((input_json, metadata))
}

fn write_orphaned_file_rpc_response(
    request_path: &Path,
    response_path: &Path,
    correlation_id: String,
    sweep: &mut ToolRpcFileTransportSweep,
) -> Result<(), String> {
    let reason = "tool rpc request orphaned before execution".to_owned();
    write_file_rpc_response(
        response_path,
        &ToolRpcFileResponseEnvelope {
            schema_version: TOOL_RPC_SCHEMA_VERSION,
            correlation_id: correlation_id.clone(),
            status: ToolRpcStatus::TimedOut.as_str().to_owned(),
            success: false,
            error: reason.clone(),
            response: None,
        },
    )?;
    mark_rpc_request_processed(request_path, correlation_id.as_str(), "orphaned")?;
    sweep.orphaned += 1;
    sweep.responses.push(ToolRpcFileTransportAudit {
        correlation_id,
        call_id: None,
        tool_name: None,
        status: ToolRpcStatus::TimedOut.as_str().to_owned(),
        success: false,
        response_path: response_path.display().to_string(),
        reason,
    });
    Ok(())
}

fn write_failed_file_rpc_response(
    request_path: &Path,
    response_path: &Path,
    correlation_id: String,
    reason: String,
    sweep: &mut ToolRpcFileTransportSweep,
) -> Result<(), String> {
    write_file_rpc_response(
        response_path,
        &ToolRpcFileResponseEnvelope {
            schema_version: TOOL_RPC_SCHEMA_VERSION,
            correlation_id: correlation_id.clone(),
            status: ToolRpcStatus::Failed.as_str().to_owned(),
            success: false,
            error: reason.clone(),
            response: None,
        },
    )?;
    mark_rpc_request_processed(request_path, correlation_id.as_str(), "processed")?;
    sweep.failed += 1;
    sweep.responses.push(ToolRpcFileTransportAudit {
        correlation_id,
        call_id: None,
        tool_name: None,
        status: ToolRpcStatus::Failed.as_str().to_owned(),
        success: false,
        response_path: response_path.display().to_string(),
        reason,
    });
    Ok(())
}

fn write_file_rpc_response(
    response_path: &Path,
    envelope: &ToolRpcFileResponseEnvelope,
) -> Result<(), String> {
    let response_json = serde_json::to_vec_pretty(envelope)
        .map_err(|error| format!("failed to serialize tool rpc file response: {error}"))?;
    let mut response =
        OpenOptions::new().write(true).create_new(true).open(response_path).map_err(|error| {
            format!("tool rpc response path must be a new regular file: {error}")
        })?;
    response
        .write_all(response_json.as_slice())
        .map_err(|error| format!("failed to write tool rpc file response: {error}"))
}

fn mark_rpc_request_processed(
    request_path: &Path,
    correlation_id: &str,
    suffix: &str,
) -> Result<(), String> {
    let processed_path = request_path.with_file_name(format!("{correlation_id}.{suffix}.json"));
    fs::rename(request_path, processed_path)
        .map_err(|error| format!("failed to mark tool rpc request {suffix}: {error}"))
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
    let redacted = crate::journal::redact_payload_json(output_json)
        .unwrap_or_else(|_| r#"{"redacted":true,"reason":"summary_redaction_failed"}"#.to_owned());
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
    use std::{
        collections::BTreeSet,
        fs,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::Duration,
    };

    use super::{
        build_python_tool_rpc_bridge_context_with_transports,
        child_tool_may_inherit_parent_approval, python_tool_rpc_sdk_source,
        python_tool_rpc_sdk_source_for_tools, python_tool_rpc_sdk_wrappers, read_file_rpc_request,
        settle_tool_rpc_execution, summarize_rpc_output, write_file_rpc_response,
        PythonToolRpcTransportDescriptor, ToolRpcExecutionSettlement, ToolRpcFileResponseEnvelope,
        ToolRpcTransportKind, MAX_TOOL_RPC_FILE_REQUEST_BYTES, TOOL_RPC_SCHEMA_VERSION,
    };

    #[test]
    fn file_rpc_request_read_is_bounded() {
        let temp = tempfile::tempdir().expect("temporary directory should be created");
        let request_path = temp.path().join("large.request.json");
        fs::write(
            request_path.as_path(),
            vec![
                b'x';
                usize::try_from(MAX_TOOL_RPC_FILE_REQUEST_BYTES)
                    .expect("request limit should fit usize")
                    + 1
            ],
        )
        .expect("oversized request fixture should be written");

        let error = read_file_rpc_request(request_path.as_path())
            .expect_err("oversized request must fail before parsing");

        assert!(error.contains("file transport limit"));
    }

    #[test]
    fn file_rpc_response_never_overwrites_existing_entries() {
        let temp = tempfile::tempdir().expect("temporary directory should be created");
        let response_path = temp.path().join("call.response.json");
        fs::write(response_path.as_path(), b"sentinel")
            .expect("existing response fixture should be written");
        let envelope = ToolRpcFileResponseEnvelope {
            schema_version: TOOL_RPC_SCHEMA_VERSION,
            correlation_id: "call".to_owned(),
            status: "completed".to_owned(),
            success: true,
            error: String::new(),
            response: None,
        };

        write_file_rpc_response(response_path.as_path(), &envelope)
            .expect_err("existing response entries must not be followed or overwritten");

        assert_eq!(
            fs::read(response_path).expect("existing response should remain readable"),
            b"sentinel"
        );
    }

    #[cfg(unix)]
    #[test]
    fn file_rpc_request_read_rejects_symlinks() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().expect("temporary directory should be created");
        let target = temp.path().join("target.json");
        let request_path = temp.path().join("call.request.json");
        fs::write(target.as_path(), b"{}").expect("target fixture should be written");
        symlink(target.as_path(), request_path.as_path())
            .expect("request symlink should be created");

        read_file_rpc_request(request_path.as_path())
            .expect_err("request symlinks must be rejected without following them");
    }

    #[tokio::test]
    async fn timeout_waits_for_child_execution_to_settle() {
        let cancellation_requested = Arc::new(AtomicBool::new(false));
        let child_observed_cancellation = Arc::new(AtomicBool::new(false));
        let child_cancellation_requested = Arc::clone(&cancellation_requested);
        let child_observed = Arc::clone(&child_observed_cancellation);
        let execution = Box::pin(async move {
            loop {
                if child_cancellation_requested.load(Ordering::Acquire) {
                    child_observed.store(true, Ordering::Release);
                    return "settled";
                }
                tokio::task::yield_now().await;
            }
        });

        let settlement = settle_tool_rpc_execution(
            execution,
            Some(Duration::from_millis(1)),
            Some(cancellation_requested.as_ref()),
        )
        .await;

        assert_eq!(settlement, ToolRpcExecutionSettlement::TimedOut("settled"));
        assert!(cancellation_requested.load(Ordering::Acquire));
        assert!(child_observed_cancellation.load(Ordering::Acquire));
    }

    #[test]
    fn python_bridge_context_exports_only_scoped_handles() {
        let grants = BTreeSet::from(["palyra.echo".to_owned(), "palyra.http.fetch".to_owned()]);
        let context = build_python_tool_rpc_bridge_context_with_transports(
            "job-1",
            "program-1",
            &grants,
            vec![PythonToolRpcTransportDescriptor::stdio_jsonl(30_000)],
        );
        assert_eq!(context.schema_version, TOOL_RPC_SCHEMA_VERSION);
        assert_eq!(context.environment["PALYRA_TOOL_RPC_IPC"], "stdio-jsonl");
        assert_eq!(context.transports[0].kind, ToolRpcTransportKind::Stdio);
        let serialized = serde_json::to_string(&context).expect("context should serialize");
        assert!(!serialized.to_ascii_lowercase().contains("secret"));
        assert!(!serialized.to_ascii_lowercase().contains("token"));
        assert!(serialized.contains("palyra.echo"));
    }

    #[test]
    fn python_bridge_context_exports_file_transport_bootstrap() {
        let grants = BTreeSet::from(["palyra.echo".to_owned()]);
        let context = build_python_tool_rpc_bridge_context_with_transports(
            "job-1",
            "program-1",
            &grants,
            vec![PythonToolRpcTransportDescriptor::file_jsonl(
                "/workspace/.palyra/rpc/requests",
                "/workspace/.palyra/rpc/responses",
                30_000,
            )],
        );

        assert_eq!(context.transports[0].kind, ToolRpcTransportKind::File);
        assert_eq!(context.environment["PALYRA_TOOL_RPC_TRANSPORT"], "file-jsonl");
        assert_eq!(
            context.environment["PALYRA_TOOL_RPC_REQUEST_DIR"],
            "/workspace/.palyra/rpc/requests"
        );
        assert_eq!(
            context.environment["PALYRA_TOOL_RPC_RESPONSE_DIR"],
            "/workspace/.palyra/rpc/responses"
        );
    }

    #[test]
    fn python_sdk_uses_jsonl_without_env_secrets() {
        let source = python_tool_rpc_sdk_source();
        assert!(source.contains("ToolRpcClient"));
        assert!(source.contains("json.dumps"));
        assert!(source.contains("\"call_id\""));
        assert!(source.contains("_call_file_jsonl"));
        assert!(!source.contains("API_KEY"));
        assert!(!source.contains("TOKEN"));
    }

    #[test]
    fn python_sdk_generates_strong_wrappers_for_granted_tools() {
        let grants = BTreeSet::from([
            "palyra.echo".to_owned(),
            "palyra.fs.read_file".to_owned(),
            "palyra.fs.search".to_owned(),
        ]);
        let wrappers = python_tool_rpc_sdk_wrappers(&grants);

        assert_eq!(wrappers["palyra.echo"], "call_palyra_echo");
        assert_eq!(wrappers["palyra.fs.read_file"], "call_palyra_fs_read_file");
        assert_eq!(wrappers["palyra.fs.search"], "call_palyra_fs_search");

        let source = python_tool_rpc_sdk_source_for_tools(&grants);
        assert!(source.contains("def call_palyra_echo("));
        assert!(source.contains("def call_palyra_fs_read_file("));
        assert!(source.contains("\"palyra.fs.search\""));
        assert!(!source.contains("palyra.process.run"));
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

    #[test]
    fn summary_projection_structurally_redacts_nested_sensitive_fields() {
        let summary = summarize_rpc_output(
            br#"{"status":"ok","nested":{"api_key":"summary-secret","label":"safe"}}"#,
            1_024,
        );

        assert!(!summary.contains("summary-secret"));
        assert!(summary.contains("<redacted>"));
        assert!(summary.contains("\"label\":\"safe\""));
    }
}
