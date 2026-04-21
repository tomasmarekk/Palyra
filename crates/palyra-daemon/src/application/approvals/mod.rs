use std::sync::Arc;

use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tonic::Status;
use tracing::info;
use ulid::Ulid;

use crate::{
    gateway::{
        current_unix_ms, truncate_with_ellipsis, GatewayRuntimeState, ToolApprovalOutcome,
        ToolSkillContext, APPROVAL_CHANNEL_UNAVAILABLE_REASON, APPROVAL_DENIED_REASON,
        APPROVAL_POLICY_ID, APPROVAL_PROMPT_TIMEOUT_SECONDS, APPROVAL_REQUEST_SUMMARY_MAX_BYTES,
        PROCESS_RUNNER_TOOL_NAME,
    },
    journal::{
        ApprovalDecision, ApprovalDecisionScope, ApprovalPolicySnapshot, ApprovalPromptOption,
        ApprovalPromptRecord, ApprovalRiskLevel, ApprovalSubjectType, JournalAppendRequest,
    },
    tool_protocol::{tool_policy_snapshot, ToolCallConfig, ToolDecision},
    transport::grpc::{auth::RequestContext, proto::palyra::common::v1 as common_v1},
};

const WORKSPACE_PATCH_TOOL_NAME: &str = "palyra.fs.apply_patch";

#[derive(Debug, Clone)]
pub(crate) struct PendingToolApproval {
    pub(crate) approval_id: String,
    pub(crate) request_summary: String,
    pub(crate) policy_snapshot: ApprovalPolicySnapshot,
    pub(crate) prompt: ApprovalPromptRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ApprovalExecutionContext {
    pub(crate) requested_backend: String,
    pub(crate) resolved_backend: String,
    pub(crate) reason_code: String,
    pub(crate) approval_required: bool,
    pub(crate) reason: String,
    pub(crate) agent_id: Option<String>,
}

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

pub(crate) fn build_pending_tool_approval(
    tool_name: &str,
    skill_context: Option<&ToolSkillContext>,
    input_json: &[u8],
    config: &ToolCallConfig,
    execution_context: Option<&ApprovalExecutionContext>,
) -> PendingToolApproval {
    let subject_id = build_tool_approval_subject_id(tool_name, skill_context);
    let request_summary =
        build_tool_request_summary(tool_name, skill_context, input_json, execution_context);
    let policy_snapshot = build_tool_policy_snapshot(config, tool_name);
    let mut details = serde_json::from_slice::<Value>(input_json)
        .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(input_json).to_string() }));
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
        risk_level: approval_risk_for_tool(tool_name, input_json, config),
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
        approval_id: Ulid::new().to_string(),
        request_summary,
        policy_snapshot,
        prompt,
    }
}

pub(crate) fn build_tool_approval_subject_id(
    tool_name: &str,
    skill_context: Option<&ToolSkillContext>,
) -> String {
    if let Some(skill_context) = skill_context {
        format!("tool:{tool_name}|skill:{}", skill_context.skill_id())
    } else {
        format!("tool:{tool_name}")
    }
}

pub(crate) fn approval_subject_type_for_tool(tool_name: &str) -> ApprovalSubjectType {
    if tool_name.starts_with("palyra.browser.") {
        ApprovalSubjectType::BrowserAction
    } else {
        ApprovalSubjectType::Tool
    }
}

fn default_approval_prompt_options() -> Vec<ApprovalPromptOption> {
    vec![
        ApprovalPromptOption {
            option_id: "allow_once".to_owned(),
            label: "Allow once".to_owned(),
            description: "Approve this single action".to_owned(),
            default_selected: true,
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
            default_selected: false,
            decision_scope: ApprovalDecisionScope::Once,
            timebox_ttl_ms: None,
        },
    ]
}

fn build_tool_request_summary(
    tool_name: &str,
    skill_context: Option<&ToolSkillContext>,
    input_json: &[u8],
    execution_context: Option<&ApprovalExecutionContext>,
) -> String {
    let normalized_input = serde_json::from_slice::<Value>(input_json)
        .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(input_json).to_string() }));
    let summary = truncate_with_ellipsis(
        json!({
            "tool_name": tool_name,
            "skill_id": skill_context.map(ToolSkillContext::skill_id),
            "skill_version": skill_context.and_then(ToolSkillContext::version),
            "input_json": normalized_input,
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

fn workspace_patch_header_paths(patch: &str) -> Vec<String> {
    const PATH_PREFIXES: &[&str] =
        &["*** Add File: ", "*** Update File: ", "*** Delete File: ", "*** Move to: "];
    let mut paths = Vec::new();
    for line in patch.lines() {
        let Some(path) = PATH_PREFIXES.iter().find_map(|prefix| line.strip_prefix(prefix)) else {
            continue;
        };
        let normalized = path.trim();
        if normalized.is_empty() || paths.iter().any(|existing| existing == normalized) {
            continue;
        }
        paths.push(truncate_with_ellipsis(normalized.to_owned(), 256));
        if paths.len() >= 16 {
            break;
        }
    }
    paths
}

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
            event_id: Ulid::new().to_string(),
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
            event_id: Ulid::new().to_string(),
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
}
