//! Explicit-approval flow for sensitive tool executions.
//!
//! Builds approval prompts (deny is always the default option), applies
//! operator decisions to tool decisions, reuses cached session-scoped
//! approvals, and records approval request/resolution journal events.
//! Workspace patches and process-runner commands get extra risk context so
//! operators see what they are approving.

use std::{sync::Arc, time::Duration};

use palyra_common::process_runner_input::parse_process_runner_tool_input;
use serde::Serialize;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use tonic::Status;
use tracing::info;
use ulid::Ulid;

use crate::{
    application::tool_governance::build_tool_call_signature,
    gateway::{
        current_unix_ms, truncate_with_ellipsis, GatewayRuntimeState, ToolApprovalOutcome,
        ToolSkillContext, APPROVAL_CHANNEL_UNAVAILABLE_REASON, APPROVAL_DENIED_REASON,
        APPROVAL_POLICY_ID, APPROVAL_PROMPT_TIMEOUT_SECONDS, APPROVAL_REQUEST_SUMMARY_MAX_BYTES,
        BROWSER_UPLOAD_TOOL_NAME, PROCESS_INPUT_TOOL_NAME, PROCESS_RUNNER_TOOL_NAME,
        PROCESS_SEND_KEYS_TOOL_NAME,
    },
    journal::{
        ApprovalDecision, ApprovalDecisionScope, ApprovalPolicySnapshot, ApprovalPromptOption,
        ApprovalPromptRecord, ApprovalRiskLevel, ApprovalSubjectType, JournalAppendRequest,
    },
    sandbox_runner::background_process_lifetime_approval_metadata,
    tool_protocol::{tool_policy_snapshot, ToolCallConfig, ToolDecision},
    transport::grpc::{auth::RequestContext, proto::palyra::common::v1 as common_v1},
};

const WORKSPACE_PATCH_TOOL_NAME: &str = "palyra.fs.apply_patch";
const OS_FILE_TOOL_NAME: &str = "palyra.fs.os_file";
const PERMISSION_REQUEST_SCHEMA_VERSION: u32 = 1;

/// Fully built approval request ready to be journaled and surfaced to the
/// approval channel; `approval_id` is freshly generated per request.
#[derive(Debug, Clone)]
pub(crate) struct PendingToolApproval {
    pub(crate) approval_id: String,
    pub(crate) request_summary: String,
    pub(crate) policy_snapshot: ApprovalPolicySnapshot,
    pub(crate) prompt: ApprovalPromptRecord,
}

/// Execution-backend resolution metadata embedded into approval prompts so
/// operators can see where an approved tool call will actually run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ApprovalExecutionContext {
    pub(crate) requested_backend: String,
    pub(crate) resolved_backend: String,
    pub(crate) reason_code: String,
    pub(crate) approval_required: bool,
    pub(crate) reason: String,
    pub(crate) agent_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct PermissionRequestEnvelope {
    schema_version: u32,
    source: &'static str,
    tool_name: String,
    subject_id: String,
    normalized_args_sha256: String,
    mutability_class: String,
    risk_posture: String,
    requested_scope: &'static str,
    ttl_seconds: u64,
    idempotency_key: String,
    requester: PermissionRequestRequester,
    #[serde(skip_serializing_if = "Option::is_none")]
    execution_backend: Option<PermissionRequestExecutionBackend>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct PermissionRequestRequester {
    kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    skill_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    skill_version: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct PermissionRequestExecutionBackend {
    requested: String,
    resolved: String,
    reason_code: String,
    approval_required: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    agent_id: Option<String>,
}

/// Folds an operator approval outcome into a tool decision.
///
/// Only decisions that are allowed *and* require approval are affected.
/// Fail-closed: a missing approval outcome (approval channel unavailable)
/// denies the call, as does an explicit deny. Every branch appends the
/// original reason so the audit trail keeps the policy rationale.
pub(crate) fn apply_tool_approval_outcome(
    mut decision: ToolDecision,
    tool_name: &str,
    approval: Option<&ToolApprovalOutcome>,
) -> ToolDecision {
    if !(decision.allowed && decision.approval_required) {
        return decision;
    }

    let Some(approval) = approval else {
        decision.allowed = false;
        decision.reason = format!(
            "{APPROVAL_CHANNEL_UNAVAILABLE_REASON}; tool={tool_name}; original_reason={}",
            decision.reason
        );
        return decision;
    };

    if approval.approved {
        decision.reason = format!(
            "explicit approval granted for tool={tool_name}; approval_reason={}; original_reason={}",
            approval.reason, decision.reason
        );
        return decision;
    }

    decision.allowed = false;
    decision.reason = format!(
        "{APPROVAL_DENIED_REASON}; tool={tool_name}; approval_reason={}; original_reason={}",
        approval.reason, decision.reason
    );
    decision
}

/// Looks up a previously granted session-scoped approval for the proposal's
/// subject, logging the reuse when one is found.
///
/// Returns `None` when the proposal does not require approval or no cached
/// decision exists, in which case the caller must prompt.
#[allow(clippy::too_many_arguments)]
pub(crate) fn resolve_cached_tool_approval_for_proposal(
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    session_id: &str,
    approval_subject_id: &str,
    proposal_approval_required: bool,
    run_id: &str,
    proposal_id: &str,
    execution_surface: &str,
) -> Option<ToolApprovalOutcome> {
    if !proposal_approval_required {
        return None;
    }
    let cached_outcome = runtime_state.resolve_cached_tool_approval(
        request_context,
        session_id,
        approval_subject_id,
    );
    if let Some(cached_outcome) = cached_outcome.as_ref() {
        info!(
            run_id = %run_id,
            proposal_id = %proposal_id,
            approval_id = %cached_outcome.approval_id,
            subject_id = %approval_subject_id,
            decision = %cached_outcome.decision.as_str(),
            decision_scope = %cached_outcome.decision_scope.as_str(),
            execution_surface = execution_surface,
            "reusing cached tool approval decision"
        );
    }
    cached_outcome
}

/// Assembles the approval prompt, request summary, and policy snapshot for
/// a tool call that requires explicit approval.
///
/// Workspace patch calls additionally embed checkpoint/rollback context and
/// the touched paths so the operator reviews the blast radius up front.
pub(crate) fn build_pending_tool_approval(
    tool_name: &str,
    skill_context: Option<&ToolSkillContext>,
    input_json: &[u8],
    config: &ToolCallConfig,
    execution_context: Option<&ApprovalExecutionContext>,
) -> PendingToolApproval {
    let subject_id = build_tool_approval_subject_id(tool_name, skill_context, input_json);
    let policy_snapshot = build_tool_policy_snapshot(config, tool_name);
    let mut details = approval_input_details(tool_name, input_json, config);
    let risk_level = approval_risk_for_tool(tool_name, input_json, config);
    let permission_request = build_permission_request_envelope(
        tool_name,
        skill_context,
        input_json,
        subject_id.as_str(),
        risk_level,
        execution_context,
    );
    details["permission_request"] =
        serde_json::to_value(&permission_request).unwrap_or_else(|_| json!({}));
    let request_summary =
        build_tool_request_summary(tool_name, skill_context, &details, execution_context);
    if let Some(execution_context) = execution_context {
        details["execution_backend"] = json!({
            "requested": execution_context.requested_backend,
            "resolved": execution_context.resolved_backend,
            "reason_code": execution_context.reason_code,
            "approval_required": execution_context.approval_required,
            "reason": execution_context.reason,
            "agent_id": execution_context.agent_id,
        });
    }
    if tool_name == WORKSPACE_PATCH_TOOL_NAME {
        details["workspace_safety"] = workspace_patch_approval_context(input_json);
    }
    let prompt = ApprovalPromptRecord {
        title: format!("Approve {}", tool_name),
        risk_level,
        subject_id: subject_id.clone(),
        summary: execution_context.map_or_else(
            || format!("Tool `{tool_name}` requested explicit approval"),
            |execution_context| {
                format!(
                    "Tool `{tool_name}` requested explicit approval on backend `{}`",
                    execution_context.resolved_backend
                )
            },
        ),
        options: default_approval_prompt_options(),
        timeout_seconds: APPROVAL_PROMPT_TIMEOUT_SECONDS,
        details_json: json!({
            "tool_name": tool_name,
            "subject_id": subject_id,
            "skill_id": skill_context.map(ToolSkillContext::skill_id),
            "skill_version": skill_context.and_then(ToolSkillContext::version),
            "input_json": details,
        })
        .to_string(),
        policy_explanation: execution_context.map_or_else(
            || "Sensitive tool actions are deny-by-default until explicitly approved".to_owned(),
            |execution_context| {
                format!(
                    "Sensitive tool actions are deny-by-default until explicitly approved; backend_requested={}; backend_resolved={}; backend_reason_code={}",
                    execution_context.requested_backend,
                    execution_context.resolved_backend,
                    execution_context.reason_code
                )
            },
        ),
    };
    PendingToolApproval {
        approval_id: Ulid::generate().to_string(),
        request_summary,
        policy_snapshot,
        prompt,
    }
}

fn build_permission_request_envelope(
    tool_name: &str,
    skill_context: Option<&ToolSkillContext>,
    input_json: &[u8],
    subject_id: &str,
    risk_level: ApprovalRiskLevel,
    execution_context: Option<&ApprovalExecutionContext>,
) -> PermissionRequestEnvelope {
    let signature = build_tool_call_signature(tool_name, input_json);
    let normalized_args_sha256 = signature.normalized_args_hash.clone();
    let skill_id = skill_context.map(ToolSkillContext::skill_id).map(str::to_owned);
    let skill_version = skill_context.and_then(ToolSkillContext::version).map(str::to_owned);
    let idempotency_payload = json!({
        "tool_name": tool_name,
        "subject_id": subject_id,
        "normalized_args_sha256": normalized_args_sha256,
        "risk_posture": risk_level.as_str(),
        "skill_id": skill_id.as_deref(),
        "skill_version": skill_version.as_deref(),
    });
    let idempotency_key = serde_json::to_vec(&idempotency_payload)
        .map(|bytes| sha256_hex(bytes.as_slice()))
        .unwrap_or_else(|_| sha256_hex(input_json));

    PermissionRequestEnvelope {
        schema_version: PERMISSION_REQUEST_SCHEMA_VERSION,
        source: "tool_proposal",
        tool_name: tool_name.to_owned(),
        subject_id: subject_id.to_owned(),
        normalized_args_sha256,
        mutability_class: signature.mutability_class,
        risk_posture: risk_level.as_str().to_owned(),
        requested_scope: "single_tool_call",
        ttl_seconds: u64::from(APPROVAL_PROMPT_TIMEOUT_SECONDS),
        idempotency_key,
        requester: PermissionRequestRequester {
            kind: "host_approval_relay",
            skill_id,
            skill_version,
        },
        execution_backend: execution_context.map(|context| PermissionRequestExecutionBackend {
            requested: context.requested_backend.clone(),
            resolved: context.resolved_backend.clone(),
            reason_code: context.reason_code.clone(),
            approval_required: context.approval_required,
            agent_id: context.agent_id.clone(),
        }),
    }
}

fn approval_input_details(tool_name: &str, input_json: &[u8], config: &ToolCallConfig) -> Value {
    let mut details = serde_json::from_slice::<Value>(input_json)
        .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(input_json).to_string() }));
    if tool_name == PROCESS_RUNNER_TOOL_NAME {
        add_process_runner_background_lifetime_context(&mut details, input_json, config);
    }
    if tool_name == PROCESS_INPUT_TOOL_NAME {
        redact_process_input_approval_details(&mut details, input_json);
    }
    if tool_name == PROCESS_SEND_KEYS_TOOL_NAME {
        redact_process_send_keys_approval_details(&mut details, input_json);
    }
    details
}

fn redact_process_input_approval_details(details: &mut Value, input_json: &[u8]) {
    let input_len = details.get("input").and_then(Value::as_str).map(str::len).unwrap_or_default();
    if let Value::Object(details) = details {
        details.insert("input".to_owned(), Value::String("<redacted>".to_owned()));
        details.insert("input_bytes".to_owned(), json!(input_len));
        details.insert("input_sha256".to_owned(), json!(sha256_hex(input_json)));
        details.insert("redaction_level".to_owned(), json!("input_redacted"));
    }
}

fn redact_process_send_keys_approval_details(details: &mut Value, input_json: &[u8]) {
    let key_count = details.get("keys").and_then(Value::as_array).map(Vec::len).unwrap_or_default();
    if let Value::Object(details) = details {
        details.insert("keys".to_owned(), Value::String("<redacted>".to_owned()));
        details.insert("key_count".to_owned(), json!(key_count));
        details.insert("keys_sha256".to_owned(), json!(sha256_hex(input_json)));
        details.insert("redaction_level".to_owned(), json!("input_redacted"));
    }
}

fn add_process_runner_background_lifetime_context(
    details: &mut Value,
    input_json: &[u8],
    config: &ToolCallConfig,
) {
    let Ok(input) = parse_process_runner_tool_input(input_json) else {
        return;
    };
    if !input.background {
        return;
    }
    let lifetime = background_process_lifetime_approval_metadata(
        input.timeout_ms,
        Duration::from_millis(config.execution_timeout_ms),
    );
    let Value::Object(details) = details else {
        return;
    };
    details.insert(
        "background_lifetime".to_owned(),
        json!({
            "requested_lifetime_ms": lifetime.requested_lifetime_ms,
            "effective_lifetime_ms": lifetime.effective_lifetime_ms,
            "max_lifetime_ms": lifetime.max_lifetime_ms,
            "min_background_lifetime_ms": lifetime.min_background_lifetime_ms,
            "adjusted": lifetime.adjusted,
            "adjustment_reason": lifetime.adjustment_reason,
            "approval_applies_to": "effective_lifetime_ms",
        }),
    );
}

/// Builds the approval cache subject for a tool call.
///
/// The skill id is part of the subject so a session-scoped approval granted
/// to one skill never silently covers the same tool used by another skill.
/// OS-file calls additionally include a fingerprint of the requested
/// operation, paths, and content-affecting inputs so a session approval for one
/// local file operation cannot cover a different OS path or mutation.
/// Browser-upload calls include the browser session, target selector, and
/// local file path so approval for one transfer cannot authorize another.
/// Detached process-runner handoffs include the requested lifetime boundary so
/// an ordinary run-owned process approval cannot silently cover post-run
/// process persistence.
pub(crate) fn build_tool_approval_subject_id(
    tool_name: &str,
    skill_context: Option<&ToolSkillContext>,
    input_json: &[u8],
) -> String {
    let mut subject_id = format!("tool:{tool_name}");
    if tool_name == OS_FILE_TOOL_NAME {
        subject_id.push_str("|os_file:");
        subject_id.push_str(os_file_approval_fingerprint(input_json).as_str());
    }
    if tool_name == BROWSER_UPLOAD_TOOL_NAME {
        subject_id.push_str("|browser_upload:");
        subject_id.push_str(browser_upload_approval_fingerprint(input_json).as_str());
    }
    if tool_name == PROCESS_RUNNER_TOOL_NAME {
        if let Some(lifetime) = process_runner_lifetime_approval_subject(input_json) {
            subject_id.push_str("|process_lifetime:");
            subject_id.push_str(lifetime);
        }
    }
    if tool_name == PROCESS_INPUT_TOOL_NAME {
        subject_id.push_str("|pid:");
        subject_id.push_str(process_input_approval_pid(input_json).as_deref().unwrap_or("unknown"));
    }
    if tool_name == PROCESS_SEND_KEYS_TOOL_NAME {
        subject_id.push_str("|pid:");
        subject_id.push_str(process_input_approval_pid(input_json).as_deref().unwrap_or("unknown"));
    }
    if let Some(skill_context) = skill_context {
        subject_id.push_str("|skill:");
        subject_id.push_str(skill_context.skill_id());
    }
    subject_id
}

fn process_runner_lifetime_approval_subject(input_json: &[u8]) -> Option<&'static str> {
    let input = parse_process_runner_tool_input(input_json).ok()?;
    let lifetime_mode = input.effective_lifetime_mode();
    lifetime_mode.is_detached_handoff().then_some(lifetime_mode.as_str())
}

fn process_input_approval_pid(input_json: &[u8]) -> Option<String> {
    let payload = serde_json::from_slice::<Value>(input_json).ok()?;
    let pid = payload
        .get("pid")
        .and_then(|value| value.as_u64().or_else(|| value.as_str()?.trim().parse::<u64>().ok()))?;
    (pid > 0).then(|| pid.to_string())
}

fn browser_upload_approval_fingerprint(input_json: &[u8]) -> String {
    let payload = match serde_json::from_slice::<Value>(input_json) {
        Ok(Value::Object(payload)) => json!({
            "session_id": normalized_string_field(&payload, "session_id"),
            "selector": normalized_string_field(&payload, "selector"),
            "file_path": normalized_string_field(&payload, "file_path"),
        }),
        _ => json!({ "raw_sha256": sha256_hex(input_json) }),
    };
    let payload_json = serde_json::to_vec(&payload).unwrap_or_else(|_| input_json.to_vec());
    sha256_hex(payload_json.as_slice())
}

fn os_file_approval_fingerprint(input_json: &[u8]) -> String {
    let payload = match serde_json::from_slice::<Value>(input_json) {
        Ok(Value::Object(payload)) => os_file_approval_fingerprint_payload(&payload),
        _ => json!({ "raw_sha256": sha256_hex(input_json) }),
    };
    let operation = payload
        .get("operation")
        .and_then(Value::as_str)
        .map(safe_subject_component)
        .unwrap_or_else(|| "unknown".to_owned());
    let payload_json = serde_json::to_vec(&payload).unwrap_or_else(|_| input_json.to_vec());
    format!("{operation}:{}", sha256_hex(payload_json.as_slice()))
}

fn os_file_approval_fingerprint_payload(payload: &Map<String, Value>) -> Value {
    json!({
        "operation": normalized_string_field(payload, "operation"),
        "path": normalized_string_field(payload, "path"),
        "target_path": normalized_string_field(payload, "target_path"),
        "content": os_file_content_fingerprint(payload),
        "create_parent_dirs": copied_json_field(payload, "create_parent_dirs"),
        "overwrite": copied_json_field(payload, "overwrite"),
        "full_replace": copied_json_field(payload, "full_replace"),
        "dry_run": copied_json_field(payload, "dry_run"),
        "offset_bytes": copied_json_field(payload, "offset_bytes"),
        "max_bytes": copied_json_field(payload, "max_bytes"),
        "query": normalized_string_field(payload, "query"),
        "case_sensitive": copied_json_field(payload, "case_sensitive"),
        "max_entries": copied_json_field(payload, "max_entries"),
        "max_matches": copied_json_field(payload, "max_matches"),
    })
}

fn os_file_content_fingerprint(payload: &Map<String, Value>) -> Value {
    let content_text = payload.get("content_text").and_then(Value::as_str);
    let bytes_base64 = payload.get("bytes_base64").and_then(Value::as_str);
    match (content_text, bytes_base64) {
        (Some(""), Some(bytes_base64)) if !bytes_base64.is_empty() => {
            json!({ "kind": "bytes_base64", "sha256": sha256_hex(bytes_base64.as_bytes()) })
        }
        (Some(content_text), _) => {
            json!({ "kind": "content_text", "sha256": sha256_hex(content_text.as_bytes()) })
        }
        (None, Some(bytes_base64)) => {
            json!({ "kind": "bytes_base64", "sha256": sha256_hex(bytes_base64.as_bytes()) })
        }
        (None, None) => Value::Null,
    }
}

fn normalized_string_field(payload: &Map<String, Value>, field: &str) -> Value {
    payload
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| json!(value))
        .unwrap_or(Value::Null)
}

fn copied_json_field(payload: &Map<String, Value>, field: &str) -> Value {
    payload.get(field).cloned().unwrap_or(Value::Null)
}

fn safe_subject_component(raw: &str) -> String {
    let component = raw
        .chars()
        .take(64)
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '_' | '-') {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();
    if component.is_empty() {
        "unknown".to_owned()
    } else {
        component
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// Classifies the approval subject; browser tools are audited as browser
/// actions, everything else as a generic tool.
pub(crate) fn approval_subject_type_for_tool(tool_name: &str) -> ApprovalSubjectType {
    if tool_name.starts_with("palyra.browser.") {
        ApprovalSubjectType::BrowserAction
    } else {
        ApprovalSubjectType::Tool
    }
}

// Deny is the default-selected option so a timed-out or fat-fingered prompt
// fails closed; tests pin this invariant.
fn default_approval_prompt_options() -> Vec<ApprovalPromptOption> {
    vec![
        ApprovalPromptOption {
            option_id: "allow_once".to_owned(),
            label: "Allow once".to_owned(),
            description: "Approve this single action".to_owned(),
            default_selected: false,
            decision_scope: ApprovalDecisionScope::Once,
            timebox_ttl_ms: None,
        },
        ApprovalPromptOption {
            option_id: "allow_session".to_owned(),
            label: "Allow for session".to_owned(),
            description: "Remember approval for this session".to_owned(),
            default_selected: false,
            decision_scope: ApprovalDecisionScope::Session,
            timebox_ttl_ms: None,
        },
        ApprovalPromptOption {
            option_id: "deny_once".to_owned(),
            label: "Deny".to_owned(),
            description: "Reject this action".to_owned(),
            default_selected: true,
            decision_scope: ApprovalDecisionScope::Once,
            timebox_ttl_ms: None,
        },
    ]
}

fn build_tool_request_summary(
    tool_name: &str,
    skill_context: Option<&ToolSkillContext>,
    input_details: &Value,
    execution_context: Option<&ApprovalExecutionContext>,
) -> String {
    let summary = truncate_with_ellipsis(
        json!({
            "tool_name": tool_name,
            "skill_id": skill_context.map(ToolSkillContext::skill_id),
            "skill_version": skill_context.and_then(ToolSkillContext::version),
            "input_json": input_details,
        })
        .to_string(),
        APPROVAL_REQUEST_SUMMARY_MAX_BYTES,
    );
    execution_context.map_or(summary.clone(), |execution_context| {
        truncate_with_ellipsis(
            format!(
                "{summary}; backend_requested={}; backend_resolved={}; backend_reason_code={}",
                execution_context.requested_backend,
                execution_context.resolved_backend,
                execution_context.reason_code
            ),
            APPROVAL_REQUEST_SUMMARY_MAX_BYTES,
        )
    })
}

fn build_tool_policy_snapshot(config: &ToolCallConfig, tool_name: &str) -> ApprovalPolicySnapshot {
    let snapshot = tool_policy_snapshot(config);
    let policy_snapshot_json = serde_json::to_vec(&snapshot).unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(policy_snapshot_json.as_slice());
    let policy_hash = hex::encode(hasher.finalize());
    ApprovalPolicySnapshot {
        policy_id: APPROVAL_POLICY_ID.to_owned(),
        policy_hash,
        evaluation_summary: format!(
            "action=tool.execute resource=tool:{tool_name} approval_required=true deny_by_default=true"
        ),
    }
}

fn workspace_patch_approval_context(input_json: &[u8]) -> Value {
    let parsed = serde_json::from_slice::<Value>(input_json).unwrap_or(Value::Null);
    let patch = parsed.get("patch").and_then(Value::as_str).unwrap_or_default();
    json!({
        "checkpoint_flow": "preflight -> post_change",
        "preflight_checkpoint_required": true,
        "post_change_checkpoint_required": true,
        "compare_available_after_execution": true,
        "restore_target": "preflight_checkpoint",
        "review_posture": "review_required",
        "policy_hooks": workspace_patch_policy_hooks(patch),
        "paths": workspace_patch_header_paths(patch),
        "degrade_behavior": {
            "high_risk": "fail_closed_without_preflight",
            "low_or_medium_risk": "explicit_tool_result_degradation"
        },
    })
}

fn workspace_patch_policy_hooks(patch: &str) -> Vec<&'static str> {
    let paths = workspace_patch_header_paths(patch);
    let mut hooks = vec!["workspace_source_code"];
    if paths.iter().any(|path| {
        let lower = path.to_ascii_lowercase();
        lower.ends_with(".toml")
            || lower.ends_with(".yaml")
            || lower.ends_with(".yml")
            || lower.ends_with(".json")
    }) {
        hooks.push("config");
    }
    if paths.iter().any(|path| {
        let lower = path.to_ascii_lowercase();
        lower.contains("/generated/") || lower.starts_with("schemas/generated/")
    }) {
        hooks.push("generated_artifacts");
    }
    if paths.iter().any(|path| {
        let lower = path.to_ascii_lowercase();
        lower.ends_with(".md") || lower.starts_with("docs/")
    }) {
        hooks.push("docs");
    }
    if paths.len() > 8 {
        hooks.push("bulk_patch");
    }
    hooks
}

// Extracts touched paths from both the structured patch format and plain
// unified diffs. Capped at 16 paths: the approval prompt needs the blast
// radius, not an exhaustive listing of an attacker-sized patch.
fn workspace_patch_header_paths(patch: &str) -> Vec<String> {
    const PATH_PREFIXES: &[&str] = &[
        "*** Add File: ",
        "*** Replace File: ",
        "*** Replace Line: ",
        "*** Update File: ",
        "*** Delete File: ",
        "*** Move to: ",
    ];
    let mut paths = Vec::new();
    let lines = patch.lines().collect::<Vec<_>>();
    let mut index = 0usize;
    while index < lines.len() {
        let line = lines[index];
        if let Some(path) = PATH_PREFIXES.iter().find_map(|prefix| line.strip_prefix(prefix)) {
            push_workspace_patch_path(&mut paths, path);
        } else if let Some(old_path) = line.strip_prefix("--- ") {
            if let Some(new_path) =
                lines.get(index.saturating_add(1)).and_then(|line| line.strip_prefix("+++ "))
            {
                push_unified_diff_header_path(&mut paths, old_path, new_path);
                index = index.saturating_add(1);
            }
        }
        if paths.len() >= 16 {
            break;
        }
        index = index.saturating_add(1);
    }
    paths
}

// Prefers the post-change (+++) path; the pre-change (---) path only wins
// for deletions, where the new side is /dev/null.
fn push_unified_diff_header_path(paths: &mut Vec<String>, old_path: &str, new_path: &str) {
    let old_path = parse_unified_diff_header_path(old_path);
    let new_path = parse_unified_diff_header_path(new_path);
    match (old_path, new_path) {
        (_, Some(path)) => push_workspace_patch_path(paths, path.as_str()),
        (Some(path), None) => push_workspace_patch_path(paths, path.as_str()),
        (None, None) => {}
    }
}

fn parse_unified_diff_header_path(path: &str) -> Option<String> {
    let normalized = path.split('\t').next().unwrap_or(path).trim();
    if normalized == "/dev/null" {
        return None;
    }
    let normalized = normalized
        .strip_prefix("a/")
        .or_else(|| normalized.strip_prefix("b/"))
        .unwrap_or(normalized)
        .trim();
    if normalized.is_empty() {
        None
    } else {
        Some(normalized.to_owned())
    }
}

fn push_workspace_patch_path(paths: &mut Vec<String>, path: &str) {
    let normalized = path.trim();
    if normalized.is_empty() || paths.iter().any(|existing| existing == normalized) {
        return;
    }
    paths.push(truncate_with_ellipsis(normalized.to_owned(), 256));
}

/// Classifies prompt risk for an approval-gated tool call.
///
/// Everything defaults to High. The only downgrade (to Medium) is a
/// process-runner call under the strongest sandbox tier (C) whose command
/// is on the read-only allowlist; weaker tiers or unparseable input stay
/// High.
pub(crate) fn approval_risk_for_tool(
    tool_name: &str,
    input_json: &[u8],
    config: &ToolCallConfig,
) -> ApprovalRiskLevel {
    if tool_name != PROCESS_RUNNER_TOOL_NAME {
        return ApprovalRiskLevel::High;
    }
    if !matches!(config.process_runner.tier, crate::sandbox_runner::SandboxProcessRunnerTier::C) {
        return ApprovalRiskLevel::High;
    }
    if process_runner_command_is_read_only(input_json) {
        ApprovalRiskLevel::Medium
    } else {
        ApprovalRiskLevel::High
    }
}

// Matches the bare command name only; arguments are deliberately ignored
// because the Tier C sandbox, not this list, is the actual write barrier.
fn process_runner_command_is_read_only(input_json: &[u8]) -> bool {
    const READ_ONLY_COMMANDS: &[&str] = &[
        "cat", "find", "grep", "head", "id", "ls", "pwd", "rg", "stat", "tail", "uname", "wc",
        "whoami",
    ];

    let parsed = match serde_json::from_slice::<Value>(input_json) {
        Ok(value) => value,
        Err(_) => return false,
    };
    let Some(payload) = parsed.as_object() else {
        return false;
    };
    let Some(command) = payload.get("command").and_then(Value::as_str).map(str::trim) else {
        return false;
    };

    READ_ONLY_COMMANDS.iter().any(|candidate| candidate.eq_ignore_ascii_case(command))
}

/// Records an `approval.requested` journal event for a pending approval.
///
/// # Errors
/// Returns the journal append failure from the gateway runtime.
#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn record_approval_requested_journal_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    session_id: &str,
    run_id: &str,
    proposal_id: &str,
    approval_id: &str,
    tool_name: &str,
    subject_id: &str,
    request_summary: &str,
    policy_snapshot: &ApprovalPolicySnapshot,
    prompt: &ApprovalPromptRecord,
) -> Result<(), Status> {
    runtime_state
        .record_journal_event(JournalAppendRequest {
            event_id: Ulid::generate().to_string(),
            session_id: session_id.to_owned(),
            run_id: run_id.to_owned(),
            kind: common_v1::journal_event::EventKind::ToolProposed as i32,
            actor: common_v1::journal_event::EventActor::System as i32,
            timestamp_unix_ms: current_unix_ms(),
            payload_json: approval_requested_journal_payload(
                proposal_id,
                approval_id,
                tool_name,
                subject_id,
                request_summary,
                policy_snapshot,
                prompt,
            ),
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
        })
        .await
        .map(|_| ())
}

fn approval_requested_journal_payload(
    proposal_id: &str,
    approval_id: &str,
    tool_name: &str,
    subject_id: &str,
    request_summary: &str,
    policy_snapshot: &ApprovalPolicySnapshot,
    prompt: &ApprovalPromptRecord,
) -> Vec<u8> {
    let prompt_details_json = serde_json::from_str::<Value>(prompt.details_json.as_str())
        .unwrap_or_else(|_| json!({ "raw": prompt.details_json }));
    let subject_type = approval_subject_type_for_tool(tool_name);
    json!({
        "event": "approval.requested",
        "proposal_id": proposal_id,
        "approval_id": approval_id,
        "subject_type": subject_type.as_str(),
        "subject_id": subject_id,
        "tool_name": tool_name,
        "request_summary": request_summary,
        "policy_snapshot": policy_snapshot,
        "prompt": {
            "title": prompt.title,
            "risk_level": prompt.risk_level.as_str(),
            "subject_id": prompt.subject_id,
            "summary": prompt.summary,
            "timeout_seconds": prompt.timeout_seconds,
            "policy_explanation": prompt.policy_explanation,
            "options": prompt.options.iter().map(|option| json!({
                "option_id": option.option_id,
                "label": option.label,
                "description": option.description,
                "default_selected": option.default_selected,
                "decision_scope": option.decision_scope.as_str(),
                "timebox_ttl_ms": option.timebox_ttl_ms,
            })).collect::<Vec<_>>(),
            "details_json": prompt_details_json,
        },
    })
    .to_string()
    .into_bytes()
}

/// Records an `approval.resolved` journal event for an operator decision.
///
/// `proposal_id` is `None` for operator-initiated resolutions that are not
/// tied to a specific tool proposal.
///
/// # Errors
/// Returns the journal append failure from the gateway runtime.
#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn record_approval_resolved_journal_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    session_id: &str,
    run_id: &str,
    proposal_id: Option<&str>,
    approval_id: &str,
    decision: ApprovalDecision,
    decision_scope: ApprovalDecisionScope,
    decision_scope_ttl_ms: Option<i64>,
    reason: &str,
) -> Result<(), Status> {
    runtime_state
        .record_journal_event(JournalAppendRequest {
            event_id: Ulid::generate().to_string(),
            session_id: session_id.to_owned(),
            run_id: run_id.to_owned(),
            kind: common_v1::journal_event::EventKind::ToolExecuted as i32,
            actor: common_v1::journal_event::EventActor::System as i32,
            timestamp_unix_ms: current_unix_ms(),
            payload_json: approval_resolved_journal_payload(
                proposal_id,
                approval_id,
                decision,
                decision_scope,
                decision_scope_ttl_ms,
                reason,
            ),
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
        })
        .await
        .map(|_| ())
}

fn approval_resolved_journal_payload(
    proposal_id: Option<&str>,
    approval_id: &str,
    decision: ApprovalDecision,
    decision_scope: ApprovalDecisionScope,
    decision_scope_ttl_ms: Option<i64>,
    reason: &str,
) -> Vec<u8> {
    json!({
        "event": "approval.resolved",
        "proposal_id": proposal_id,
        "approval_id": approval_id,
        "decision": decision.as_str(),
        "decision_scope": decision_scope.as_str(),
        "decision_scope_ttl_ms": decision_scope_ttl_ms,
        "reason": reason,
    })
    .to_string()
    .into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn test_tool_call_config(allowed_tool: &str) -> ToolCallConfig {
        ToolCallConfig {
            allowed_tools: vec![allowed_tool.to_owned()],
            max_calls_per_run: 1,
            execution_timeout_ms: 250,
            process_runner: crate::sandbox_runner::SandboxProcessRunnerPolicy {
                enabled: true,
                tier: crate::sandbox_runner::SandboxProcessRunnerTier::B,
                workspace_root: std::env::current_dir().expect("current_dir should resolve"),
                path_access_mode: crate::sandbox_runner::PathAccessMode::WorkspaceOnly,
                allowed_executables: vec!["cargo".to_owned()],
                allow_interpreters: false,
                egress_enforcement_mode: crate::sandbox_runner::EgressEnforcementMode::Preflight,
                allowed_egress_hosts: Vec::new(),
                allowed_dns_suffixes: Vec::new(),
                cpu_time_limit_ms: 1_000,
                memory_limit_bytes: 1_048_576,
                max_output_bytes: 1_048_576,
            },
            wasm_runtime: crate::wasm_plugin_runner::WasmPluginRunnerPolicy {
                enabled: false,
                allow_inline_modules: false,
                max_module_size_bytes: 256 * 1024,
                fuel_budget: 1_000_000,
                max_memory_bytes: 64 * 1024 * 1024,
                max_table_elements: 1_024,
                max_instances: 8,
                allowed_http_hosts: Vec::new(),
                allowed_secrets: Vec::new(),
                allowed_storage_prefixes: Vec::new(),
                allowed_channels: Vec::new(),
            },
        }
    }

    #[test]
    fn approval_resolved_payload_includes_proposal_id_when_available() {
        let payload = approval_resolved_journal_payload(
            Some("01ARZ3NDEKTSV4RRFFQ69G5FA1"),
            "01ARZ3NDEKTSV4RRFFQ69G5FA2",
            ApprovalDecision::Allow,
            ApprovalDecisionScope::Session,
            Some(60_000),
            "operator-approved",
        );
        let json: Value = serde_json::from_slice(payload.as_slice())
            .expect("approval resolved payload should remain valid JSON");
        assert_eq!(
            json.get("proposal_id").and_then(Value::as_str),
            Some("01ARZ3NDEKTSV4RRFFQ69G5FA1")
        );
    }

    #[test]
    fn approval_resolved_payload_allows_missing_proposal_id() {
        let payload = approval_resolved_journal_payload(
            None,
            "01ARZ3NDEKTSV4RRFFQ69G5FA2",
            ApprovalDecision::Deny,
            ApprovalDecisionScope::Once,
            None,
            "operator-denied",
        );
        let json: Value = serde_json::from_slice(payload.as_slice())
            .expect("approval resolved payload should remain valid JSON");
        assert!(
            json.get("proposal_id").is_some_and(Value::is_null),
            "operator-driven approval audit should tolerate missing proposal ids"
        );
    }

    #[test]
    fn browser_tools_use_browser_action_subject_type() {
        assert_eq!(
            approval_subject_type_for_tool("palyra.browser.navigate"),
            ApprovalSubjectType::BrowserAction
        );
        assert_eq!(approval_subject_type_for_tool("palyra.process.run"), ApprovalSubjectType::Tool);
    }

    #[test]
    fn os_file_approval_subject_scopes_cache_to_operation_and_path() {
        let stat_subject = build_tool_approval_subject_id(
            OS_FILE_TOOL_NAME,
            None,
            br#"{"operation":"stat","path":"/tmp/palyra/harmless.txt"}"#,
        );
        let read_same_path_subject = build_tool_approval_subject_id(
            OS_FILE_TOOL_NAME,
            None,
            br#"{"operation":"read","path":"/tmp/palyra/harmless.txt"}"#,
        );
        let read_other_path_subject = build_tool_approval_subject_id(
            OS_FILE_TOOL_NAME,
            None,
            br#"{"operation":"read","path":"/tmp/palyra/secrets.txt"}"#,
        );

        assert!(stat_subject.starts_with("tool:palyra.fs.os_file|os_file:stat:"));
        assert!(read_same_path_subject.starts_with("tool:palyra.fs.os_file|os_file:read:"));
        assert_ne!(
            stat_subject, read_same_path_subject,
            "approving a harmless stat must not cache approval for a read of the same path"
        );
        assert_ne!(
            read_same_path_subject, read_other_path_subject,
            "approving one OS file path must not cache approval for another path"
        );
        assert!(
            !read_same_path_subject.contains("harmless.txt"),
            "subject ids should not expose raw local path components"
        );
    }

    #[test]
    fn os_file_approval_subject_scopes_write_cache_to_content_hash() {
        let first_write = build_tool_approval_subject_id(
            OS_FILE_TOOL_NAME,
            None,
            br#"{"operation":"write","path":"/tmp/palyra/config.txt","content_text":"first"}"#,
        );
        let second_write = build_tool_approval_subject_id(
            OS_FILE_TOOL_NAME,
            None,
            br#"{"operation":"write","path":"/tmp/palyra/config.txt","content_text":"second"}"#,
        );

        assert!(first_write.starts_with("tool:palyra.fs.os_file|os_file:write:"));
        assert_ne!(
            first_write, second_write,
            "session-scoped write approval must be bound to the approved content hash"
        );
    }

    #[test]
    fn os_file_approval_subject_hashes_effective_base64_content() {
        let first_write = build_tool_approval_subject_id(
            OS_FILE_TOOL_NAME,
            None,
            br#"{"operation":"write","path":"/tmp/palyra/config.bin","content_text":"","bytes_base64":"Zmlyc3Q="}"#,
        );
        let second_write = build_tool_approval_subject_id(
            OS_FILE_TOOL_NAME,
            None,
            br#"{"operation":"write","path":"/tmp/palyra/config.bin","content_text":"","bytes_base64":"c2Vjb25k"}"#,
        );

        assert_ne!(
            first_write, second_write,
            "session approval must be bound to the base64 content selected by the write runtime"
        );
    }

    #[test]
    fn browser_upload_approval_subject_scopes_cache_to_exact_transfer() {
        let first = build_tool_approval_subject_id(
            BROWSER_UPLOAD_TOOL_NAME,
            None,
            br##"{"session_id":"session-1","selector":"#upload","file_path":"fixtures/report.csv"}"##,
        );
        let other_path = build_tool_approval_subject_id(
            BROWSER_UPLOAD_TOOL_NAME,
            None,
            br##"{"session_id":"session-1","selector":"#upload","file_path":"home/.ssh/id_rsa"}"##,
        );
        let other_selector = build_tool_approval_subject_id(
            BROWSER_UPLOAD_TOOL_NAME,
            None,
            br##"{"session_id":"session-1","selector":"#avatar","file_path":"fixtures/report.csv"}"##,
        );
        let other_session = build_tool_approval_subject_id(
            BROWSER_UPLOAD_TOOL_NAME,
            None,
            br##"{"session_id":"session-2","selector":"#upload","file_path":"fixtures/report.csv"}"##,
        );

        assert!(first.starts_with("tool:palyra.browser.upload|browser_upload:"));
        assert_ne!(first, other_path);
        assert_ne!(first, other_selector);
        assert_ne!(first, other_session);
        assert!(
            !first.contains("report.csv"),
            "approval subject must not expose raw local path components"
        );
    }

    #[test]
    fn generic_tool_approval_subjects_remain_tool_and_skill_scoped() {
        let skill_context =
            ToolSkillContext::new("acme.audit".to_owned(), Some("1.0.0".to_owned()));
        assert_eq!(
            build_tool_approval_subject_id(
                PROCESS_RUNNER_TOOL_NAME,
                Some(&skill_context),
                br#"{"command":"cargo","args":["test"]}"#,
            ),
            "tool:palyra.process.run|skill:acme.audit"
        );
    }

    #[test]
    fn process_runner_detached_lifetimes_use_distinct_approval_subjects() {
        let run_owned = build_tool_approval_subject_id(
            PROCESS_RUNNER_TOOL_NAME,
            None,
            br#"{"command":"python","args":["server.py"],"background":true}"#,
        );
        let detached = build_tool_approval_subject_id(
            PROCESS_RUNNER_TOOL_NAME,
            None,
            br#"{"command":"python","args":["server.py"],"background":true,"lifetime_mode":"detached"}"#,
        );
        let compatibility_alias = build_tool_approval_subject_id(
            PROCESS_RUNNER_TOOL_NAME,
            None,
            br#"{"command":"python","args":["server.py"],"background":true,"keep_running_after_run":true}"#,
        );
        let until_verifier = build_tool_approval_subject_id(
            PROCESS_RUNNER_TOOL_NAME,
            None,
            br#"{"command":"python","args":["server.py"],"background":true,"lifetime_mode":"until_verifier"}"#,
        );

        assert_eq!(run_owned, "tool:palyra.process.run");
        assert_eq!(detached, "tool:palyra.process.run|process_lifetime:detached");
        assert_eq!(compatibility_alias, detached);
        assert_eq!(until_verifier, "tool:palyra.process.run|process_lifetime:until_verifier");
    }

    #[test]
    fn process_runner_background_approval_exposes_effective_lifetime() {
        let mut config = test_tool_call_config("palyra.process.run");
        config.execution_timeout_ms = 180_000;
        let pending = build_pending_tool_approval(
            "palyra.process.run",
            None,
            br#"{"command":"cargo","args":["test"],"background":true,"timeout_ms":1}"#,
            &config,
            None,
        );

        assert!(pending.request_summary.contains("background_lifetime"));
        assert!(pending.request_summary.contains("effective_lifetime_ms"));
        assert!(pending.request_summary.contains("120000"));

        let details_json: Value = serde_json::from_str(pending.prompt.details_json.as_str())
            .expect("approval prompt details should remain valid JSON");
        let lifetime = details_json
            .pointer("/input_json/background_lifetime")
            .expect("background lifetime context should be embedded");
        assert_eq!(lifetime.get("requested_lifetime_ms").and_then(Value::as_u64), Some(1));
        assert_eq!(lifetime.get("effective_lifetime_ms").and_then(Value::as_u64), Some(120_000));
        assert_eq!(lifetime.get("max_lifetime_ms").and_then(Value::as_u64), Some(180_000));
        assert_eq!(
            lifetime.get("min_background_lifetime_ms").and_then(Value::as_u64),
            Some(120_000)
        );
        assert_eq!(lifetime.get("adjusted").and_then(Value::as_bool), Some(true));
        assert_eq!(
            lifetime.get("adjustment_reason").and_then(Value::as_str),
            Some("raised_to_minimum_background_lifetime")
        );
        assert_eq!(
            lifetime.get("approval_applies_to").and_then(Value::as_str),
            Some("effective_lifetime_ms")
        );
    }

    #[test]
    fn process_input_approval_is_pid_scoped_and_redacted() {
        let input = br#"{"pid":1234,"input":"super-secret-command","append_newline":true}"#;
        let subject = build_tool_approval_subject_id(PROCESS_INPUT_TOOL_NAME, None, input);
        let config = test_tool_call_config(PROCESS_INPUT_TOOL_NAME);
        let pending =
            build_pending_tool_approval(PROCESS_INPUT_TOOL_NAME, None, input, &config, None);

        assert_eq!(subject, "tool:palyra.process.input|pid:1234");
        assert!(!pending.request_summary.contains("super-secret-command"));
        assert!(pending.request_summary.contains("<redacted>"));

        let details_json: Value = serde_json::from_str(pending.prompt.details_json.as_str())
            .expect("approval prompt details should remain valid JSON");
        let input_json = details_json
            .get("input_json")
            .expect("approval prompt should include redacted input JSON");
        assert_eq!(input_json.get("pid").and_then(Value::as_u64), Some(1234));
        assert_eq!(input_json.get("input").and_then(Value::as_str), Some("<redacted>"));
        assert_eq!(
            input_json.get("redaction_level").and_then(Value::as_str),
            Some("input_redacted")
        );
        assert_eq!(
            input_json.get("input_bytes").and_then(Value::as_u64),
            Some("super-secret-command".len() as u64)
        );
        assert_eq!(input_json.get("input_sha256").and_then(Value::as_str).map(str::len), Some(64));
    }

    #[test]
    fn process_send_keys_approval_is_pid_scoped_and_redacted() {
        let input = br#"{"pid":1234,"keys":[{"key":"text","text":"secret menu value"},{"key":"enter"}],"allow_stdin_fallback":true}"#;
        let subject = build_tool_approval_subject_id(PROCESS_SEND_KEYS_TOOL_NAME, None, input);
        let config = test_tool_call_config(PROCESS_SEND_KEYS_TOOL_NAME);
        let pending =
            build_pending_tool_approval(PROCESS_SEND_KEYS_TOOL_NAME, None, input, &config, None);

        assert_eq!(subject, "tool:palyra.process.send_keys|pid:1234");
        assert!(!pending.request_summary.contains("secret menu value"));
        assert!(pending.request_summary.contains("<redacted>"));

        let details_json: Value = serde_json::from_str(pending.prompt.details_json.as_str())
            .expect("approval prompt details should remain valid JSON");
        let input_json = details_json
            .get("input_json")
            .expect("approval prompt should include redacted input JSON");
        assert_eq!(input_json.get("pid").and_then(Value::as_u64), Some(1234));
        assert_eq!(input_json.get("keys").and_then(Value::as_str), Some("<redacted>"));
        assert_eq!(input_json.get("key_count").and_then(Value::as_u64), Some(2));
        assert_eq!(
            input_json.get("redaction_level").and_then(Value::as_str),
            Some("input_redacted")
        );
        assert_eq!(input_json.get("keys_sha256").and_then(Value::as_str).map(str::len), Some(64));
    }

    #[test]
    fn default_tool_approval_prompt_marks_deny_as_terminal_default() {
        let options = default_approval_prompt_options();

        assert_eq!(
            options
                .iter()
                .find(|option| option.option_id == "allow_once")
                .map(|option| option.default_selected),
            Some(false)
        );
        assert_eq!(
            options
                .iter()
                .find(|option| option.option_id == "deny_once")
                .map(|option| option.default_selected),
            Some(true)
        );
    }

    #[test]
    fn pending_tool_approval_embeds_permission_request_envelope() {
        let config = test_tool_call_config(PROCESS_RUNNER_TOOL_NAME);
        let pending = build_pending_tool_approval(
            PROCESS_RUNNER_TOOL_NAME,
            None,
            br#"{"command":"cargo","args":["test"]}"#,
            &config,
            None,
        );
        let details_json: Value = serde_json::from_str(pending.prompt.details_json.as_str())
            .expect("approval prompt details should remain valid JSON");
        let permission_request = details_json
            .pointer("/input_json/permission_request")
            .expect("permission request should be embedded");

        assert_eq!(permission_request.get("schema_version").and_then(Value::as_u64), Some(1));
        assert_eq!(permission_request.get("source").and_then(Value::as_str), Some("tool_proposal"));
        assert_eq!(
            permission_request.get("tool_name").and_then(Value::as_str),
            Some(PROCESS_RUNNER_TOOL_NAME)
        );
        assert_eq!(
            permission_request.get("subject_id").and_then(Value::as_str),
            Some("tool:palyra.process.run")
        );
        assert_eq!(
            permission_request.get("requested_scope").and_then(Value::as_str),
            Some("single_tool_call")
        );
        assert_eq!(
            permission_request.get("ttl_seconds").and_then(Value::as_u64),
            Some(u64::from(APPROVAL_PROMPT_TIMEOUT_SECONDS))
        );
        assert_eq!(
            permission_request.get("normalized_args_sha256").and_then(Value::as_str).map(str::len),
            Some(64)
        );
        assert_eq!(
            permission_request.get("idempotency_key").and_then(Value::as_str).map(str::len),
            Some(64)
        );
        assert_eq!(
            permission_request.pointer("/requester/kind").and_then(Value::as_str),
            Some("host_approval_relay")
        );
    }

    #[test]
    fn permission_request_digest_is_stable_for_reordered_json_object_keys() {
        let config = test_tool_call_config(PROCESS_RUNNER_TOOL_NAME);
        let first = build_pending_tool_approval(
            PROCESS_RUNNER_TOOL_NAME,
            None,
            br#"{"command":"cargo","args":["test"]}"#,
            &config,
            None,
        );
        let second = build_pending_tool_approval(
            PROCESS_RUNNER_TOOL_NAME,
            None,
            br#"{"args":["test"],"command":"cargo"}"#,
            &config,
            None,
        );
        let first_details: Value = serde_json::from_str(first.prompt.details_json.as_str())
            .expect("first details should parse");
        let second_details: Value = serde_json::from_str(second.prompt.details_json.as_str())
            .expect("second details should parse");

        assert_eq!(
            first_details.pointer("/input_json/permission_request/normalized_args_sha256"),
            second_details.pointer("/input_json/permission_request/normalized_args_sha256")
        );
        assert_eq!(
            first_details.pointer("/input_json/permission_request/idempotency_key"),
            second_details.pointer("/input_json/permission_request/idempotency_key")
        );
    }

    #[test]
    fn approval_requested_payload_uses_browser_subject_type_for_browser_tools() {
        let prompt = ApprovalPromptRecord {
            title: "Approve palyra.browser.navigate".to_owned(),
            risk_level: ApprovalRiskLevel::High,
            subject_id: "tool:palyra.browser.navigate".to_owned(),
            summary: "Tool requested explicit approval".to_owned(),
            options: Vec::new(),
            timeout_seconds: 30,
            details_json: "{}".to_owned(),
            policy_explanation: "Sensitive tool actions require approval".to_owned(),
        };
        let payload = approval_requested_journal_payload(
            "01ARZ3NDEKTSV4RRFFQ69G5FA1",
            "01ARZ3NDEKTSV4RRFFQ69G5FA2",
            "palyra.browser.navigate",
            "tool:palyra.browser.navigate",
            "{}",
            &ApprovalPolicySnapshot {
                policy_id: "policy".to_owned(),
                policy_hash: "0".repeat(64),
                evaluation_summary: "summary".to_owned(),
            },
            &prompt,
        );
        let json: Value = serde_json::from_slice(payload.as_slice())
            .expect("approval requested payload should remain valid JSON");
        assert_eq!(json.get("subject_type").and_then(Value::as_str), Some("browser_action"));
    }

    #[test]
    fn build_pending_tool_approval_embeds_backend_execution_context() {
        let execution_context = ApprovalExecutionContext {
            requested_backend: "networked_worker".to_owned(),
            resolved_backend: "networked_worker".to_owned(),
            reason_code: "backend.available.networked_worker".to_owned(),
            approval_required: true,
            reason: "attested worker fleet is available".to_owned(),
            agent_id: Some("agent.networked".to_owned()),
        };
        let config = test_tool_call_config("palyra.process.run");
        let pending = build_pending_tool_approval(
            "palyra.process.run",
            None,
            br#"{"command":"cargo","args":["test"]}"#,
            &config,
            Some(&execution_context),
        );
        assert!(
            pending.request_summary.contains("backend_resolved=networked_worker"),
            "request summary should preserve backend explain metadata"
        );
        assert!(
            pending.prompt.summary.contains("networked_worker"),
            "approval prompt summary should call out the resolved backend"
        );
        let details_json: Value = serde_json::from_str(pending.prompt.details_json.as_str())
            .expect("approval prompt details should remain valid JSON");
        assert_eq!(
            details_json
                .get("input_json")
                .and_then(|value| value.get("execution_backend"))
                .and_then(|value| value.get("reason_code"))
                .and_then(Value::as_str),
            Some("backend.available.networked_worker")
        );
    }

    #[test]
    fn browser_reload_approval_exposes_expected_url_destination() {
        let config = test_tool_call_config("palyra.browser.reload");
        let input = br#"{"session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","expected_url":"http://127.0.0.1:8080/admin/export?nonce=approval-visible","allow_private_targets":true}"#;

        let pending =
            build_pending_tool_approval("palyra.browser.reload", None, input, &config, None);

        assert!(pending.request_summary.contains("expected_url"));
        assert!(pending.request_summary.contains("127.0.0.1:8080/admin/export"));
        let details_json: Value = serde_json::from_str(pending.prompt.details_json.as_str())
            .expect("approval prompt details should remain valid JSON");
        assert_eq!(
            details_json.pointer("/input_json/expected_url").and_then(Value::as_str),
            Some("http://127.0.0.1:8080/admin/export?nonce=approval-visible")
        );
    }

    #[test]
    fn workspace_patch_approval_embeds_rollback_path_context() {
        let config = test_tool_call_config(WORKSPACE_PATCH_TOOL_NAME);
        let pending = build_pending_tool_approval(
            WORKSPACE_PATCH_TOOL_NAME,
            None,
            br#"{"patch":"*** Begin Patch\n*** Update File: crates/palyra-daemon/src/lib.rs\n@@\n-old\n+new\n*** End Patch\n"}"#,
            &config,
            None,
        );
        let details_json: Value = serde_json::from_str(pending.prompt.details_json.as_str())
            .expect("approval prompt details should remain valid JSON");
        let safety = details_json
            .pointer("/input_json/workspace_safety")
            .expect("workspace safety context should be embedded");
        assert_eq!(
            safety.get("checkpoint_flow").and_then(Value::as_str),
            Some("preflight -> post_change")
        );
        assert_eq!(
            safety.get("restore_target").and_then(Value::as_str),
            Some("preflight_checkpoint")
        );
        assert_eq!(
            safety.pointer("/degrade_behavior/high_risk").and_then(Value::as_str),
            Some("fail_closed_without_preflight")
        );
        assert_eq!(
            safety
                .get("paths")
                .and_then(Value::as_array)
                .and_then(|paths| paths.first())
                .and_then(Value::as_str),
            Some("crates/palyra-daemon/src/lib.rs")
        );
    }

    #[test]
    fn workspace_patch_approval_extracts_unified_diff_paths() {
        let safety = workspace_patch_approval_context(
            br#"{"patch":"--- /dev/null\n+++ b/docs/guide.md\n@@ -0,0 +1 @@\n+guide\n--- a/config/default.toml\n+++ /dev/null\n@@ -1 +0,0 @@\n-value\n"}"#,
        );
        let paths = safety
            .get("paths")
            .and_then(Value::as_array)
            .expect("workspace safety context should include paths");
        assert_eq!(
            paths.iter().filter_map(Value::as_str).collect::<Vec<_>>(),
            vec!["docs/guide.md", "config/default.toml"]
        );
        let hooks = safety
            .get("policy_hooks")
            .and_then(Value::as_array)
            .expect("workspace safety context should include hooks");
        assert!(
            hooks.iter().any(|hook| hook.as_str() == Some("docs")),
            "docs files should trigger docs policy hook"
        );
        assert!(
            hooks.iter().any(|hook| hook.as_str() == Some("config")),
            "toml files should trigger config policy hook"
        );
    }

    #[test]
    fn workspace_patch_approval_extracts_replace_operation_paths() {
        let safety = workspace_patch_approval_context(
            br#"{"patch":"*** Begin Patch\n*** Replace File: config/runtime.toml\n+api_url = \"https://example.test\"\n*** Replace Line: docs/guide.md\n-old\n+new\n*** End Patch\n"}"#,
        );
        let paths = safety
            .get("paths")
            .and_then(Value::as_array)
            .expect("workspace safety context should include paths");

        assert_eq!(
            paths.iter().filter_map(Value::as_str).collect::<Vec<_>>(),
            vec!["config/runtime.toml", "docs/guide.md"]
        );
        let hooks = safety
            .get("policy_hooks")
            .and_then(Value::as_array)
            .expect("workspace safety context should include hooks");
        assert!(hooks.iter().any(|hook| hook.as_str() == Some("config")));
        assert!(hooks.iter().any(|hook| hook.as_str() == Some("docs")));
    }
}
