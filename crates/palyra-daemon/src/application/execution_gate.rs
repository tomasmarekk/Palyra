use palyra_common::feature_rollouts::{
    EXECUTION_GATE_PIPELINE_V2_ROLLOUT_CONFIG_PATH, EXECUTION_GATE_PIPELINE_V2_ROLLOUT_ENV,
};
use serde::Serialize;
use serde_json::{json, Value};

use crate::{
    gateway::{
        ToolApprovalOutcome, ToolSkillContext, APPROVAL_CHANNEL_UNAVAILABLE_REASON,
        APPROVAL_DENIED_REASON, SKILL_EXECUTION_DENY_REASON_PREFIX,
    },
    tool_posture::{EffectiveToolPosture, ToolPostureState},
    tool_protocol::{decide_tool_call, ToolCallConfig, ToolDecision, ToolRequestContext},
};

use super::tool_security::{
    annotate_tool_decision_with_backend_context, evaluate_backend_capability_gate,
    ToolProposalBackendSelection,
};

const EXECUTION_GATE_PIPELINE_VERSION: &str = "v2";
const BUDGET_DENY_REASON: &str = "tool execution budget exhausted for run";
const UNSUPPORTED_TOOL_DENY_REASON: &str =
    "tool is allowlisted but unsupported by runtime executor";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExecutionGateVerdict {
    Allow,
    Block,
    RequireApproval,
    Skipped,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct ExecutionGateStep {
    pub(crate) gate_id: String,
    pub(crate) verdict: ExecutionGateVerdict,
    pub(crate) reason_code: String,
    pub(crate) summary: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) remediation: Option<String>,
    pub(crate) metadata: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct ExecutionGateDecisionSnapshot {
    pub(crate) allowed: bool,
    pub(crate) approval_required: bool,
    pub(crate) policy_enforced: bool,
    pub(crate) reason: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct ExecutionGateReport {
    pub(crate) pipeline_version: String,
    pub(crate) rollout_env: String,
    pub(crate) rollout_config_path: String,
    pub(crate) final_verdict: ExecutionGateVerdict,
    pub(crate) final_reason_code: String,
    pub(crate) final_decision: ExecutionGateDecisionSnapshot,
    pub(crate) steps: Vec<ExecutionGateStep>,
}

#[derive(Debug, Clone)]
pub(crate) struct ExecutionGatePipelineOutcome {
    pub(crate) decision: ToolDecision,
    pub(crate) remaining_budget: u32,
    pub(crate) report: ExecutionGateReport,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ToolProposalApprovalState<'a> {
    pub(crate) outcome: Option<&'a ToolApprovalOutcome>,
    pub(crate) pending_approval_id: Option<&'a str>,
}

#[derive(Debug)]
pub(crate) struct ExecutionGatePipelineInput<'a> {
    pub(crate) tool_call_config: &'a ToolCallConfig,
    pub(crate) request_context: &'a ToolRequestContext,
    pub(crate) tool_name: &'a str,
    pub(crate) skill_context: Option<&'a ToolSkillContext>,
    pub(crate) skill_gate_decision: Option<ToolDecision>,
    pub(crate) proposal_approval_required: bool,
    pub(crate) effective_posture: &'a EffectiveToolPosture,
    pub(crate) backend_selection: &'a ToolProposalBackendSelection,
    pub(crate) approval_state: ToolProposalApprovalState<'a>,
    pub(crate) remaining_budget: u32,
}

pub(crate) fn evaluate_execution_gate_pipeline(
    input: ExecutionGatePipelineInput<'_>,
) -> ExecutionGatePipelineOutcome {
    let ExecutionGatePipelineInput {
        tool_call_config,
        request_context,
        tool_name,
        skill_context,
        skill_gate_decision,
        proposal_approval_required,
        effective_posture,
        backend_selection,
        approval_state,
        remaining_budget,
    } = input;

    let mut steps = Vec::new();
    steps.push(request_context_step(request_context, skill_context));

    let skill_step = skill_gate_step(skill_context, skill_gate_decision.as_ref());
    let skill_gate_blocked = matches!(skill_step.verdict, ExecutionGateVerdict::Block);
    steps.push(skill_step);

    let posture_step = posture_gate_step(effective_posture);
    let posture_blocked = effective_posture.effective_state == ToolPostureState::Disabled;
    steps.push(posture_step);

    let backend_capability_decision = if skill_gate_blocked || posture_blocked {
        None
    } else {
        evaluate_backend_capability_gate(tool_name, backend_selection)
    };
    steps.push(backend_gate_step(
        tool_name,
        backend_selection,
        backend_capability_decision.as_ref(),
    ));
    steps.push(secret_resolution_step(tool_name));
    steps.push(safety_boundary_step(tool_name));

    let preemptive_decision = if let Some(decision) = skill_gate_decision {
        Some(annotate_tool_decision_with_backend_context(decision, backend_selection))
    } else if posture_blocked {
        let posture_reason = effective_posture.lock_reason.clone().unwrap_or_else(|| {
            format!("tool posture disabled in {}", effective_posture.source_scope_label)
        });
        Some(annotate_tool_decision_with_backend_context(
            ToolDecision {
                allowed: false,
                reason: posture_reason,
                approval_required: false,
                policy_enforced: true,
            },
            backend_selection,
        ))
    } else {
        backend_capability_decision.map(|decision| {
            annotate_tool_decision_with_backend_context(decision, backend_selection)
        })
    };

    let mut policy_reason_code = "policy.skipped_upstream_denial".to_owned();
    let mut policy_summary =
        "Policy gate was skipped because an earlier execution gate already blocked the proposal."
            .to_owned();
    let mut policy_remediation = None;
    let mut policy_metadata = json!({
        "evaluated": false,
    });

    let mut decision = preemptive_decision.clone();
    let mut resulting_budget = remaining_budget;

    if preemptive_decision.is_none() {
        let posture_always_allow =
            effective_posture.effective_state == ToolPostureState::AlwaysAllow;
        let mut pre_policy_budget = remaining_budget;
        let pre_policy_decision = decide_tool_call(
            tool_call_config,
            &mut pre_policy_budget,
            request_context,
            tool_name,
            posture_always_allow,
        );
        let pre_policy_reason_code = infer_policy_reason_code(
            pre_policy_decision.reason.as_str(),
            pre_policy_decision.allowed,
        );
        let approval_can_override_policy =
            pre_policy_reason_code == "policy.sensitive.requires_approval";
        let effective_approval_required =
            proposal_approval_required || approval_can_override_policy;

        let mut approved_policy_budget = remaining_budget;
        let mut approved_policy_decision = if approval_can_override_policy {
            decide_tool_call(
                tool_call_config,
                &mut approved_policy_budget,
                request_context,
                tool_name,
                true,
            )
        } else {
            pre_policy_decision.clone()
        };
        if approved_policy_decision.allowed && posture_always_allow {
            approved_policy_decision.approval_required = false;
            approved_policy_decision.reason = format!(
                "{}; posture={} ({})",
                approved_policy_decision.reason,
                effective_posture.effective_state.as_str(),
                effective_posture.source_scope_label
            );
        }

        policy_reason_code = if pre_policy_decision.allowed {
            "policy.allowed".to_owned()
        } else {
            pre_policy_reason_code.clone()
        };
        policy_summary = pre_policy_decision.reason.clone();
        policy_remediation =
            policy_reason_code_remediation(policy_reason_code.as_str(), effective_posture);
        policy_metadata = json!({
            "evaluated": true,
            "pre_approval_allowed": pre_policy_decision.allowed,
            "pre_approval_required": pre_policy_decision.approval_required,
            "pre_approval_reason": pre_policy_decision.reason,
            "approval_can_override_policy": approval_can_override_policy,
            "posture_state": effective_posture.effective_state.as_str(),
            "proposal_approval_required": proposal_approval_required,
        });

        let final_decision = if !pre_policy_decision.allowed && !approval_can_override_policy {
            annotate_tool_decision_with_backend_context(
                pre_policy_decision.clone(),
                backend_selection,
            )
        } else if effective_approval_required {
            if let Some(pending_approval_id) = approval_state.pending_approval_id {
                annotate_tool_decision_with_backend_context(
                    approval_pending_decision(
                        tool_name,
                        pending_approval_id,
                        pre_policy_decision.reason.as_str(),
                    ),
                    backend_selection,
                )
            } else if let Some(approval_outcome) = approval_state.outcome {
                if approval_outcome.approved {
                    let mut approved = approved_policy_decision;
                    approved.allowed = true;
                    approved.approval_required = true;
                    approved.reason = format!(
                        "explicit approval granted for tool={tool_name}; approval_reason={}; original_reason={}",
                        approval_outcome.reason,
                        approved.reason
                    );
                    annotate_tool_decision_with_backend_context(approved, backend_selection)
                } else {
                    annotate_tool_decision_with_backend_context(
                        ToolDecision {
                            allowed: false,
                            reason: format!(
                                "{APPROVAL_DENIED_REASON}; tool={tool_name}; approval_reason={}; original_reason={}",
                                approval_outcome.reason,
                                pre_policy_decision.reason
                            ),
                            approval_required: true,
                            policy_enforced: true,
                        },
                        backend_selection,
                    )
                }
            } else {
                annotate_tool_decision_with_backend_context(
                    ToolDecision {
                        allowed: false,
                        reason: format!(
                            "{APPROVAL_CHANNEL_UNAVAILABLE_REASON}; tool={tool_name}; original_reason={}",
                            pre_policy_decision.reason
                        ),
                        approval_required: true,
                        policy_enforced: true,
                    },
                    backend_selection,
                )
            }
        } else {
            let mut allowed = pre_policy_decision;
            if allowed.allowed && posture_always_allow {
                allowed.approval_required = false;
                allowed.reason = format!(
                    "{}; posture={} ({})",
                    allowed.reason,
                    effective_posture.effective_state.as_str(),
                    effective_posture.source_scope_label
                );
            }
            annotate_tool_decision_with_backend_context(allowed, backend_selection)
        };

        resulting_budget = if final_decision.allowed {
            if effective_approval_required
                && approval_state.outcome.is_some_and(|value| value.approved)
            {
                approved_policy_budget
            } else {
                pre_policy_budget
            }
        } else {
            remaining_budget
        };
        decision = Some(final_decision);
    }

    steps.push(ExecutionGateStep {
        gate_id: "policy".to_owned(),
        verdict: if decision.as_ref().is_some_and(|value| value.allowed) {
            ExecutionGateVerdict::Allow
        } else if policy_reason_code == "policy.skipped_upstream_denial" {
            ExecutionGateVerdict::Skipped
        } else if policy_reason_code == "policy.sensitive.requires_approval" {
            ExecutionGateVerdict::RequireApproval
        } else {
            ExecutionGateVerdict::Block
        },
        reason_code: policy_reason_code,
        summary: policy_summary,
        remediation: policy_remediation,
        metadata: policy_metadata,
    });

    let final_decision = decision.expect("pipeline must always yield a decision");
    let final_reason_code = infer_final_reason_code(
        final_decision.reason.as_str(),
        final_decision.allowed,
        approval_state.pending_approval_id.is_some(),
    );
    steps.push(approval_gate_step(
        &final_decision,
        proposal_approval_required,
        approval_state,
        final_reason_code.as_str(),
    ));
    steps.push(execution_launch_step(
        &final_decision,
        final_reason_code.as_str(),
        resulting_budget,
    ));

    let report = ExecutionGateReport {
        pipeline_version: EXECUTION_GATE_PIPELINE_VERSION.to_owned(),
        rollout_env: EXECUTION_GATE_PIPELINE_V2_ROLLOUT_ENV.to_owned(),
        rollout_config_path: EXECUTION_GATE_PIPELINE_V2_ROLLOUT_CONFIG_PATH.to_owned(),
        final_verdict: final_verdict(&final_decision, final_reason_code.as_str()),
        final_reason_code: final_reason_code.clone(),
        final_decision: decision_snapshot(&final_decision),
        steps,
    };

    ExecutionGatePipelineOutcome {
        decision: final_decision,
        remaining_budget: resulting_budget,
        report,
    }
}

pub(crate) fn append_audit_finalization_step(
    report: &mut ExecutionGateReport,
    legacy_decision: &ToolDecision,
) {
    let matches_legacy = report.final_decision.allowed == legacy_decision.allowed
        && report.final_decision.approval_required == legacy_decision.approval_required
        && report.final_decision.policy_enforced == legacy_decision.policy_enforced
        && report.final_decision.reason == legacy_decision.reason;
    report.steps.push(ExecutionGateStep {
        gate_id: "audit.finalization".to_owned(),
        verdict: if matches_legacy {
            ExecutionGateVerdict::Allow
        } else {
            ExecutionGateVerdict::Block
        },
        reason_code: if matches_legacy {
            "audit.finalized".to_owned()
        } else {
            "audit.legacy_mismatch".to_owned()
        },
        summary: if matches_legacy {
            "Execution gate report matched the legacy decision path and will be attached to journal/support bundle output."
                .to_owned()
        } else {
            "Execution gate report differed from the legacy decision path; keeping the comparison in audit payloads for rollback triage."
                .to_owned()
        },
        remediation: if matches_legacy {
            None
        } else {
            Some(
                "Disable feature_rollouts.execution_gate_pipeline_v2 if rollout diagnostics show an unexpected mismatch."
                    .to_owned(),
            )
        },
        metadata: json!({
            "matches_legacy": matches_legacy,
            "legacy_decision": decision_snapshot(legacy_decision),
        }),
    });
}

fn request_context_step(
    request_context: &ToolRequestContext,
    skill_context: Option<&ToolSkillContext>,
) -> ExecutionGateStep {
    ExecutionGateStep {
        gate_id: "request_context".to_owned(),
        verdict: ExecutionGateVerdict::Allow,
        reason_code: "context.ready".to_owned(),
        summary: "Resolved request context for execution gate evaluation.".to_owned(),
        remediation: None,
        metadata: json!({
            "principal": request_context.principal,
            "device_id": request_context.device_id,
            "channel": request_context.channel,
            "session_id": request_context.session_id,
            "run_id": request_context.run_id,
            "skill_id": skill_context.map(ToolSkillContext::skill_id),
            "skill_version": skill_context.and_then(ToolSkillContext::version),
        }),
    }
}

fn skill_gate_step(
    skill_context: Option<&ToolSkillContext>,
    skill_gate_decision: Option<&ToolDecision>,
) -> ExecutionGateStep {
    if let Some(skill_gate_decision) = skill_gate_decision {
        let reason_code = if skill_gate_decision.reason.contains("invalid skill context") {
            "skill.execution.invalid_context"
        } else if skill_gate_decision.reason.contains("status=missing") {
            "skill.execution.missing"
        } else if skill_gate_decision.reason.contains("evaluation_error=") {
            "skill.execution.evaluation_error"
        } else {
            "skill.execution.denied"
        };
        return ExecutionGateStep {
            gate_id: "skill_gate".to_owned(),
            verdict: ExecutionGateVerdict::Block,
            reason_code: reason_code.to_owned(),
            summary: skill_gate_decision.reason.clone(),
            remediation: Some(
                "Inspect skill status/quarantine state or fix the skill_id/version payload before retrying."
                    .to_owned(),
            ),
            metadata: json!({
                "skill_id": skill_context.map(ToolSkillContext::skill_id),
                "skill_version": skill_context.and_then(ToolSkillContext::version),
                "reason_prefix": SKILL_EXECUTION_DENY_REASON_PREFIX,
            }),
        };
    }
    ExecutionGateStep {
        gate_id: "skill_gate".to_owned(),
        verdict: if skill_context.is_some() {
            ExecutionGateVerdict::Allow
        } else {
            ExecutionGateVerdict::Skipped
        },
        reason_code: if skill_context.is_some() {
            "skill.execution.allowed".to_owned()
        } else {
            "skill.execution.not_applicable".to_owned()
        },
        summary: if let Some(skill_context) = skill_context {
            format!("Skill execution context resolved for {}", skill_context.skill_id())
        } else {
            "Tool proposal does not target a managed skill execution path.".to_owned()
        },
        remediation: None,
        metadata: json!({
            "skill_id": skill_context.map(ToolSkillContext::skill_id),
            "skill_version": skill_context.and_then(ToolSkillContext::version),
        }),
    }
}

fn posture_gate_step(effective_posture: &EffectiveToolPosture) -> ExecutionGateStep {
    let verdict = match effective_posture.effective_state {
        ToolPostureState::AlwaysAllow => ExecutionGateVerdict::Allow,
        ToolPostureState::AskEachTime => ExecutionGateVerdict::RequireApproval,
        ToolPostureState::Disabled => ExecutionGateVerdict::Block,
    };
    ExecutionGateStep {
        gate_id: "tool_posture".to_owned(),
        verdict,
        reason_code: format!("tool.posture.{}", effective_posture.effective_state.as_str()),
        summary: effective_posture.lock_reason.clone().unwrap_or_else(|| {
            format!(
                "Effective tool posture is '{}' from {}.",
                effective_posture.effective_state.as_str(),
                effective_posture.source_scope_label
            )
        }),
        remediation: match effective_posture.effective_state {
            ToolPostureState::Disabled => Some(
                "Re-enable the tool for the active scope or move the request onto a scope with a less restrictive posture."
                    .to_owned(),
            ),
            ToolPostureState::AskEachTime => Some(
                "Keep an approval surface available or switch the tool posture if interactive approval is not expected."
                    .to_owned(),
            ),
            ToolPostureState::AlwaysAllow => None,
        },
        metadata: json!({
            "effective_state": effective_posture.effective_state.as_str(),
            "default_state": effective_posture.default_state.as_str(),
            "approval_mode": effective_posture.approval_mode,
            "source_scope_kind": effective_posture.source_scope_kind.as_str(),
            "source_scope_id": effective_posture.source_scope_id,
            "source_scope_label": effective_posture.source_scope_label,
            "editable": effective_posture.editable,
        }),
    }
}

fn backend_gate_step(
    tool_name: &str,
    backend_selection: &ToolProposalBackendSelection,
    capability_gate_decision: Option<&ToolDecision>,
) -> ExecutionGateStep {
    if let Some(capability_gate_decision) = capability_gate_decision {
        return ExecutionGateStep {
            gate_id: "backend_selection".to_owned(),
            verdict: ExecutionGateVerdict::Block,
            reason_code: "backend.policy.capability_denied".to_owned(),
            summary: capability_gate_decision.reason.clone(),
            remediation: Some(
                "Switch the agent backend_preference to local_sandbox or automatic for tools that need filesystem, process, or secret access."
                    .to_owned(),
            ),
            metadata: json!({
                "tool_name": tool_name,
                "requested_backend": backend_selection.requested_preference.as_str(),
                "resolved_backend": backend_selection.resolution.resolved.as_str(),
                "backend_reason_code": backend_selection.resolution.reason_code,
                "backend_reason": backend_selection.resolution.reason,
                "agent_id": backend_selection.agent_id,
            }),
        };
    }

    ExecutionGateStep {
        gate_id: "backend_selection".to_owned(),
        verdict: if backend_selection.resolution.approval_required {
            ExecutionGateVerdict::RequireApproval
        } else {
            ExecutionGateVerdict::Allow
        },
        reason_code: backend_selection.resolution.reason_code.clone(),
        summary: backend_selection.resolution.reason.clone(),
        remediation: if backend_selection.resolution.approval_required {
            Some(
                "Provide explicit approval for the resolved backend or switch the agent back to local_sandbox."
                    .to_owned(),
            )
        } else {
            None
        },
        metadata: json!({
            "requested_backend": backend_selection.requested_preference.as_str(),
            "resolved_backend": backend_selection.resolution.resolved.as_str(),
            "fallback_used": backend_selection.resolution.fallback_used,
            "approval_required": backend_selection.resolution.approval_required,
            "reason": backend_selection.resolution.reason,
            "agent_id": backend_selection.agent_id,
        }),
    }
}

fn secret_resolution_step(tool_name: &str) -> ExecutionGateStep {
    let applicable = tool_name == "palyra.http.fetch";
    ExecutionGateStep {
        gate_id: "secret_resolution".to_owned(),
        verdict: ExecutionGateVerdict::Skipped,
        reason_code: if applicable {
            "secret_resolution.deferred_to_runtime".to_owned()
        } else {
            "secret_resolution.not_applicable".to_owned()
        },
        summary: if applicable {
            "Credential binding resolution is deferred to the HTTP runtime adapter so Vault-backed headers stay scoped to the concrete request."
                .to_owned()
        } else {
            "This tool proposal does not resolve credential bindings before execution.".to_owned()
        },
        remediation: None,
        metadata: json!({
            "tool_name": tool_name,
            "runtime_surface": if applicable { Some("http_fetch") } else { None::<&str> },
        }),
    }
}

fn safety_boundary_step(tool_name: &str) -> ExecutionGateStep {
    let applicable = tool_name == "palyra.http.fetch" || tool_name.starts_with("palyra.browser.");
    ExecutionGateStep {
        gate_id: "safety_boundary".to_owned(),
        verdict: ExecutionGateVerdict::Skipped,
        reason_code: if applicable {
            "safety_boundary.deferred_to_runtime".to_owned()
        } else {
            "safety_boundary.not_applicable".to_owned()
        },
        summary: if applicable {
            "Unified safety scanning runs inside the browser/HTTP runtime boundary so exported data stays redacted before it reaches the prompt."
                .to_owned()
        } else {
            "This tool proposal does not cross the external-content safety boundary during pre-execution gating."
                .to_owned()
        },
        remediation: None,
        metadata: json!({
            "tool_name": tool_name,
            "runtime_surface": if tool_name == "palyra.http.fetch" {
                Some("http_fetch")
            } else if tool_name.starts_with("palyra.browser.") {
                Some("browser")
            } else {
                None::<&str>
            },
        }),
    }
}

fn approval_gate_step(
    decision: &ToolDecision,
    proposal_approval_required: bool,
    approval_state: ToolProposalApprovalState<'_>,
    final_reason_code: &str,
) -> ExecutionGateStep {
    let approval_engaged = proposal_approval_required || decision.approval_required;
    let (verdict, reason_code, summary, remediation) = if approval_state
        .pending_approval_id
        .is_some()
    {
        (
            ExecutionGateVerdict::RequireApproval,
            "approval.pending".to_owned(),
            decision.reason.clone(),
            Some(
                "Wait for the pending approval response or cancel the proposal if the backend/tool should not proceed."
                    .to_owned(),
            ),
        )
    } else if let Some(outcome) = approval_state.outcome {
        if outcome.approved {
            (
                ExecutionGateVerdict::Allow,
                "approval.granted".to_owned(),
                decision.reason.clone(),
                None,
            )
        } else {
            (
                ExecutionGateVerdict::Block,
                "approval.denied".to_owned(),
                decision.reason.clone(),
                Some(
                    "Review the approval rationale and retry only after reducing risk or switching to a posture/backend that no longer requires operator approval."
                        .to_owned(),
                ),
            )
        }
    } else if approval_engaged && final_reason_code == "approval.channel_unavailable" {
        (
            ExecutionGateVerdict::Block,
            "approval.channel_unavailable".to_owned(),
            decision.reason.clone(),
            Some(
                "Restore the approval channel or retry through an execution surface that can wait for operator confirmation."
                    .to_owned(),
            ),
        )
    } else if approval_engaged && decision.allowed {
        (
            ExecutionGateVerdict::Allow,
            "approval.satisfied".to_owned(),
            "Approval requirements were satisfied before execution launch.".to_owned(),
            None,
        )
    } else if approval_engaged {
        (
            ExecutionGateVerdict::RequireApproval,
            "approval.required".to_owned(),
            decision.reason.clone(),
            Some("Provide explicit operator approval before retrying the proposal.".to_owned()),
        )
    } else {
        (
            ExecutionGateVerdict::Skipped,
            "approval.not_required".to_owned(),
            "No approval gate was required for this proposal.".to_owned(),
            None,
        )
    };

    ExecutionGateStep {
        gate_id: "approval".to_owned(),
        verdict,
        reason_code,
        summary,
        remediation,
        metadata: json!({
            "proposal_approval_required": proposal_approval_required,
            "decision_approval_required": decision.approval_required,
            "pending_approval_id": approval_state.pending_approval_id,
            "approval_outcome": approval_state.outcome.map(|value| json!({
                "approval_id": value.approval_id,
                "approved": value.approved,
                "reason": value.reason,
                "decision": value.decision.as_str(),
                "decision_scope": value.decision_scope.as_str(),
                "decision_scope_ttl_ms": value.decision_scope_ttl_ms,
            })),
        }),
    }
}

fn execution_launch_step(
    decision: &ToolDecision,
    final_reason_code: &str,
    resulting_budget: u32,
) -> ExecutionGateStep {
    let (verdict, reason_code, summary, remediation) = if decision.allowed {
        (
            ExecutionGateVerdict::Allow,
            "execution.launch.ready".to_owned(),
            "Execution launch is permitted and the proposal can proceed into the runtime dispatcher."
                .to_owned(),
            None,
        )
    } else if final_reason_code == "approval.pending" {
        (
            ExecutionGateVerdict::RequireApproval,
            "execution.launch.awaiting_approval".to_owned(),
            "Execution launch is deferred until the pending approval resolves.".to_owned(),
            Some("Approve or reject the pending proposal to unblock launch.".to_owned()),
        )
    } else {
        (
            ExecutionGateVerdict::Block,
            "execution.launch.blocked".to_owned(),
            "Execution launch stayed fail-closed because an upstream gate denied the proposal."
                .to_owned(),
            Some(
                "Inspect the earlier gate report entries and remediate the first blocking reason."
                    .to_owned(),
            ),
        )
    };

    ExecutionGateStep {
        gate_id: "execution_launch".to_owned(),
        verdict,
        reason_code,
        summary,
        remediation,
        metadata: json!({
            "remaining_budget": resulting_budget,
            "decision_allowed": decision.allowed,
            "decision_approval_required": decision.approval_required,
        }),
    }
}

fn decision_snapshot(decision: &ToolDecision) -> ExecutionGateDecisionSnapshot {
    ExecutionGateDecisionSnapshot {
        allowed: decision.allowed,
        approval_required: decision.approval_required,
        policy_enforced: decision.policy_enforced,
        reason: decision.reason.clone(),
    }
}

fn final_verdict(decision: &ToolDecision, final_reason_code: &str) -> ExecutionGateVerdict {
    if decision.allowed {
        ExecutionGateVerdict::Allow
    } else if final_reason_code == "approval.pending" {
        ExecutionGateVerdict::RequireApproval
    } else {
        ExecutionGateVerdict::Block
    }
}

fn infer_policy_reason_code(reason: &str, allowed: bool) -> String {
    if allowed {
        return "policy.allowed".to_owned();
    }
    if reason == BUDGET_DENY_REASON {
        return "policy.budget_exhausted".to_owned();
    }
    if reason == UNSUPPORTED_TOOL_DENY_REASON {
        return "policy.runtime.unsupported_tool".to_owned();
    }
    if reason.contains("policy evaluation failed safely") {
        return "policy.evaluation_failed".to_owned();
    }
    if reason.contains("sensitive action blocked by default") {
        return "policy.sensitive.requires_approval".to_owned();
    }
    if reason.contains("denied by default") {
        return "policy.denied_by_default".to_owned();
    }
    "policy.denied".to_owned()
}

fn infer_final_reason_code(reason: &str, allowed: bool, has_pending_approval: bool) -> String {
    if allowed {
        if reason.contains("explicit approval granted") {
            return "approval.granted".to_owned();
        }
        return "policy.allowed".to_owned();
    }
    if has_pending_approval || reason.contains("approval required (pending approval_id=") {
        return "approval.pending".to_owned();
    }
    if reason.contains(APPROVAL_DENIED_REASON) {
        return "approval.denied".to_owned();
    }
    if reason.contains(APPROVAL_CHANNEL_UNAVAILABLE_REASON) {
        return "approval.channel_unavailable".to_owned();
    }
    if reason.contains(SKILL_EXECUTION_DENY_REASON_PREFIX) {
        return "skill.execution.denied".to_owned();
    }
    if reason.contains("backend policy blocked") {
        return "backend.policy.capability_denied".to_owned();
    }
    if reason.contains("tool posture disabled") {
        return "tool.posture.disabled".to_owned();
    }
    infer_policy_reason_code(reason, false)
}

fn policy_reason_code_remediation(
    reason_code: &str,
    effective_posture: &EffectiveToolPosture,
) -> Option<String> {
    match reason_code {
        "policy.denied_by_default" => Some(
            "Allowlist the tool for the active principal/scope before retrying the proposal."
                .to_owned(),
        ),
        "policy.budget_exhausted" => Some(
            "Start a fresh run or reduce the number of tool calls in the current run before retrying."
                .to_owned(),
        ),
        "policy.runtime.unsupported_tool" => Some(
            "Switch to a runtime-supported tool identifier or implement the executor before enabling this path."
                .to_owned(),
        ),
        "policy.sensitive.requires_approval" => {
            if effective_posture.effective_state == ToolPostureState::AlwaysAllow {
                None
            } else {
                Some("Provide explicit approval or relax the tool posture if this sensitive action is expected.".to_owned())
            }
        }
        "policy.evaluation_failed" => Some(
            "Inspect policy diagnostics and keep the request denied until Cedar evaluation succeeds again."
                .to_owned(),
        ),
        _ => None,
    }
}

fn approval_pending_decision(
    tool_name: &str,
    approval_id: &str,
    original_reason: &str,
) -> ToolDecision {
    ToolDecision {
        allowed: false,
        reason: format!(
            "approval required (pending approval_id={approval_id}); tool={tool_name}; original_reason={original_reason}"
        ),
        approval_required: true,
        policy_enforced: true,
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use serde_json::json;

    use crate::{
        execution_backends::{ExecutionBackendPreference, ExecutionBackendResolution},
        gateway::ToolApprovalOutcome,
        journal::{ApprovalDecision, ApprovalDecisionScope},
        sandbox_runner::{
            EgressEnforcementMode, SandboxProcessRunnerPolicy, SandboxProcessRunnerTier,
        },
        tool_posture::{EffectiveToolPosture, ToolPostureScopeKind, ToolPostureState},
        tool_protocol::{ToolCallConfig, ToolRequestContext},
        wasm_plugin_runner::WasmPluginRunnerPolicy,
    };

    use super::{
        append_audit_finalization_step, evaluate_execution_gate_pipeline,
        ExecutionGatePipelineInput, ExecutionGateVerdict, ToolProposalApprovalState,
    };
    use crate::application::tool_security::ToolProposalBackendSelection;

    fn request_context() -> ToolRequestContext {
        ToolRequestContext {
            principal: "workspace:test".to_owned(),
            device_id: Some("device:test".to_owned()),
            channel: Some("cli".to_owned()),
            session_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned()),
            run_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned()),
            skill_id: None,
        }
    }

    fn tool_call_config(allowed_tools: &[&str]) -> ToolCallConfig {
        ToolCallConfig {
            allowed_tools: allowed_tools.iter().map(|value| (*value).to_owned()).collect(),
            max_calls_per_run: 2,
            execution_timeout_ms: 250,
            process_runner: SandboxProcessRunnerPolicy {
                enabled: true,
                tier: SandboxProcessRunnerTier::C,
                workspace_root: PathBuf::from("."),
                allowed_executables: vec!["cargo".to_owned()],
                allow_interpreters: false,
                egress_enforcement_mode: EgressEnforcementMode::Preflight,
                allowed_egress_hosts: Vec::new(),
                allowed_dns_suffixes: Vec::new(),
                cpu_time_limit_ms: 1_000,
                memory_limit_bytes: 1_048_576,
                max_output_bytes: 1_048_576,
            },
            wasm_runtime: WasmPluginRunnerPolicy {
                enabled: true,
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
            },
        }
    }

    fn effective_posture(state: ToolPostureState) -> EffectiveToolPosture {
        EffectiveToolPosture {
            effective_state: state,
            default_state: ToolPostureState::AskEachTime,
            approval_mode: state.as_str().to_owned(),
            source_scope_kind: ToolPostureScopeKind::Session,
            source_scope_id: "session:test".to_owned(),
            source_scope_label: "Current session".to_owned(),
            chain: Vec::new(),
            lock_reason: (state == ToolPostureState::Disabled)
                .then(|| "tool posture disabled in Current session".to_owned()),
            editable: state != ToolPostureState::Disabled,
        }
    }

    fn local_backend_selection() -> ToolProposalBackendSelection {
        ToolProposalBackendSelection {
            agent_id: None,
            requested_preference: ExecutionBackendPreference::Automatic,
            resolution: ExecutionBackendResolution {
                requested: ExecutionBackendPreference::Automatic,
                resolved: ExecutionBackendPreference::LocalSandbox,
                fallback_used: false,
                reason_code: "backend.default.local_sandbox".to_owned(),
                approval_required: false,
                reason: "Automatic keeps execution on the daemon host.".to_owned(),
            },
        }
    }

    fn networked_backend_selection() -> ToolProposalBackendSelection {
        ToolProposalBackendSelection {
            agent_id: Some("agent-network".to_owned()),
            requested_preference: ExecutionBackendPreference::NetworkedWorker,
            resolution: ExecutionBackendResolution {
                requested: ExecutionBackendPreference::NetworkedWorker,
                resolved: ExecutionBackendPreference::NetworkedWorker,
                fallback_used: false,
                reason_code: "backend.available.networked_worker".to_owned(),
                approval_required: true,
                reason: "attested worker is available".to_owned(),
            },
        }
    }

    fn approved_outcome(reason: &str) -> ToolApprovalOutcome {
        ToolApprovalOutcome {
            approval_id: "approval-1".to_owned(),
            approved: true,
            reason: reason.to_owned(),
            decision: ApprovalDecision::Allow,
            decision_scope: ApprovalDecisionScope::Session,
            decision_scope_ttl_ms: None,
        }
    }

    #[test]
    fn pipeline_keeps_sensitive_tool_pending_without_consuming_budget() {
        let mut outcome = evaluate_execution_gate_pipeline(ExecutionGatePipelineInput {
            tool_call_config: &tool_call_config(&["palyra.process.run"]),
            request_context: &request_context(),
            tool_name: "palyra.process.run",
            skill_context: None,
            skill_gate_decision: None,
            proposal_approval_required: false,
            effective_posture: &effective_posture(ToolPostureState::AskEachTime),
            backend_selection: &local_backend_selection(),
            approval_state: ToolProposalApprovalState {
                outcome: None,
                pending_approval_id: Some("approval-1"),
            },
            remaining_budget: 2,
        });
        append_audit_finalization_step(&mut outcome.report, &outcome.decision);
        assert!(!outcome.decision.allowed);
        assert!(outcome.decision.approval_required);
        assert_eq!(outcome.remaining_budget, 2);
        assert_eq!(outcome.report.final_reason_code, "approval.pending");
        assert_eq!(outcome.report.final_verdict, ExecutionGateVerdict::RequireApproval);
        assert!(outcome.decision.reason.contains("pending approval_id=approval-1"));
    }

    #[test]
    fn pipeline_requires_networked_backend_approval_for_supported_tool() {
        let outcome = evaluate_execution_gate_pipeline(ExecutionGatePipelineInput {
            tool_call_config: &tool_call_config(&["palyra.echo"]),
            request_context: &request_context(),
            tool_name: "palyra.echo",
            skill_context: None,
            skill_gate_decision: None,
            proposal_approval_required: true,
            effective_posture: &effective_posture(ToolPostureState::AskEachTime),
            backend_selection: &networked_backend_selection(),
            approval_state: ToolProposalApprovalState {
                outcome: None,
                pending_approval_id: Some("approval-backend"),
            },
            remaining_budget: 2,
        });
        assert!(!outcome.decision.allowed);
        assert!(outcome.decision.approval_required);
        assert_eq!(outcome.remaining_budget, 2);
        assert_eq!(outcome.report.final_reason_code, "approval.pending");
        assert!(outcome.report.steps.iter().any(|step| step.gate_id == "backend_selection"
            && step.reason_code == "backend.available.networked_worker"));
    }

    #[test]
    fn pipeline_allows_sensitive_tool_after_explicit_approval_and_consumes_budget_once() {
        let outcome = evaluate_execution_gate_pipeline(ExecutionGatePipelineInput {
            tool_call_config: &tool_call_config(&["palyra.process.run"]),
            request_context: &request_context(),
            tool_name: "palyra.process.run",
            skill_context: None,
            skill_gate_decision: None,
            proposal_approval_required: false,
            effective_posture: &effective_posture(ToolPostureState::AskEachTime),
            backend_selection: &local_backend_selection(),
            approval_state: ToolProposalApprovalState {
                outcome: Some(&approved_outcome("operator approved process execution")),
                pending_approval_id: None,
            },
            remaining_budget: 2,
        });
        assert!(outcome.decision.allowed);
        assert!(outcome.decision.approval_required);
        assert_eq!(outcome.remaining_budget, 1);
        assert_eq!(outcome.report.final_reason_code, "approval.granted");
        assert!(outcome
            .decision
            .reason
            .contains("explicit approval granted for tool=palyra.process.run"));
    }

    #[test]
    fn pipeline_report_json_snapshot_includes_reason_codes_and_legacy_comparison() {
        let mut outcome = evaluate_execution_gate_pipeline(ExecutionGatePipelineInput {
            tool_call_config: &tool_call_config(&["palyra.memory.search"]),
            request_context: &request_context(),
            tool_name: "palyra.memory.search",
            skill_context: None,
            skill_gate_decision: None,
            proposal_approval_required: false,
            effective_posture: &effective_posture(ToolPostureState::AskEachTime),
            backend_selection: &local_backend_selection(),
            approval_state: ToolProposalApprovalState::default(),
            remaining_budget: 2,
        });
        let legacy_decision = outcome.decision.clone();
        append_audit_finalization_step(&mut outcome.report, &legacy_decision);
        let value = serde_json::to_value(&outcome.report).expect("report should serialize");
        assert_eq!(value["pipeline_version"], json!("v2"));
        assert_eq!(value["final_reason_code"], json!("policy.allowed"));
        assert_eq!(value["final_decision"]["allowed"], json!(true));
        assert!(value["steps"]
            .as_array()
            .expect("steps should be an array")
            .iter()
            .any(|step| step["gate_id"] == json!("audit.finalization")
                && step["reason_code"] == json!("audit.finalized")));
    }
}
