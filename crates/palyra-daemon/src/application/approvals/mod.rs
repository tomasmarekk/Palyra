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

#[derive(Debug, Clone)]
pub(crate) struct PendingToolApproval {
    pub(crate) approval_id: String,
    pub(crate) request_summary: String,
    pub(crate) policy_snapshot: ApprovalPolicySnapshot,
    pub(crate) prompt: ApprovalPromptRecord,
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
) -> PendingToolApproval {
    let subject_id = build_tool_approval_subject_id(tool_name, skill_context);
    let request_summary = build_tool_request_summary(tool_name, skill_context, input_json);
    let policy_snapshot = build_tool_policy_snapshot(config, tool_name);
    let details = serde_json::from_slice::<Value>(input_json)
        .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(input_json).to_string() }));
    let prompt = ApprovalPromptRecord {
        title: format!("Approve {}", tool_name),
        risk_level: approval_risk_for_tool(tool_name, input_json, config),
        subject_id: subject_id.clone(),
        summary: format!("Tool `{tool_name}` requested explicit approval"),
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
        policy_explanation: "Sensitive tool actions are deny-by-default until explicitly approved"
            .to_owned(),
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
) -> String {
    let normalized_input = serde_json::from_slice::<Value>(input_json)
        .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(input_json).to_string() }));
    truncate_with_ellipsis(
        json!({
            "tool_name": tool_name,
            "skill_id": skill_context.map(ToolSkillContext::skill_id),
            "skill_version": skill_context.and_then(ToolSkillContext::version),
            "input_json": normalized_input,
        })
        .to_string(),
        APPROVAL_REQUEST_SUMMARY_MAX_BYTES,
    )
}

fn build_tool_policy_snapshot(config: &ToolCallConfig, tool_name: &str) -> ApprovalPolicySnapshot {
    let snapshot = tool_policy_snapshot(config);
    let policy_snapshot_json = serde_json::to_vec(&snapshot).unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(policy_snapshot_json.as_slice());
    let policy_hash = format!("{:x}", hasher.finalize());
    ApprovalPolicySnapshot {
        policy_id: APPROVAL_POLICY_ID.to_owned(),
        policy_hash,
        evaluation_summary: format!(
            "action=tool.execute resource=tool:{tool_name} approval_required=true deny_by_default=true"
        ),
    }
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
}
