use std::sync::Arc;

use palyra_policy::{
    evaluate_with_context, PolicyDecision, PolicyEvaluationConfig, PolicyRequest,
    PolicyRequestContext,
};
use serde_json::{json, Value};
use tonic::Status;
use tracing::warn;
use ulid::Ulid;

use crate::{
    agents::AgentBindingQuery,
    application::approvals::{apply_tool_approval_outcome, build_tool_approval_subject_id},
    gateway::{
        current_unix_ms, GatewayRuntimeState, ToolApprovalOutcome, ToolSkillContext,
        SKILL_EXECUTION_DENY_REASON_PREFIX,
    },
    journal::{JournalAppendRequest, SkillExecutionStatus},
    tool_posture::{
        derive_scope_chain, evaluate_effective_tool_posture, ToolPostureScopeKind,
        ToolPostureScopeRef, ToolPostureState,
    },
    tool_protocol::{decide_tool_call, ToolDecision, ToolRequestContext},
    transport::grpc::{auth::RequestContext, proto::palyra::common::v1 as common_v1},
};

#[derive(Debug, Clone)]
pub(crate) struct ToolProposalSecurityEvaluation {
    pub(crate) skill_context: Option<ToolSkillContext>,
    pub(crate) skill_gate_decision: Option<ToolDecision>,
    pub(crate) approval_subject_id: String,
    pub(crate) proposal_approval_required: bool,
    pub(crate) effective_posture: crate::tool_posture::EffectiveToolPosture,
}

#[allow(clippy::result_large_err)]
fn parse_tool_skill_context(
    tool_name: &str,
    input_json: &[u8],
) -> Result<Option<ToolSkillContext>, Status> {
    if tool_name != "palyra.plugin.run" {
        return Ok(None);
    }
    let payload = serde_json::from_slice::<Value>(input_json)
        .map_err(|error| Status::invalid_argument(format!("invalid tool input JSON: {error}")))?;
    let object = payload
        .as_object()
        .ok_or_else(|| Status::invalid_argument("tool input must be a JSON object"))?;
    let has_inline_module_payload =
        object.contains_key("module_wat") || object.contains_key("module_base64");
    let skill_id = match object.get("skill_id") {
        Some(skill_id_value) => Some(
            skill_id_value
                .as_str()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    Status::invalid_argument(
                        "palyra.plugin.run skill_id must be a non-empty string",
                    )
                })?
                .to_ascii_lowercase(),
        ),
        None => None,
    };
    let version = object
        .get("skill_version")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    if skill_id.is_none() {
        if version.is_some() {
            return Err(Status::invalid_argument(
                "palyra.plugin.run skill_version requires non-empty skill_id",
            ));
        }
        return Ok(None);
    }
    if has_inline_module_payload {
        return Err(Status::invalid_argument(
            "palyra.plugin.run skill_id cannot be combined with inline module payloads",
        ));
    }
    Ok(Some(ToolSkillContext::new(skill_id.expect("checked"), version)))
}

#[allow(clippy::result_large_err)]
async fn evaluate_skill_execution_gate(
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    context: &ToolSkillContext,
) -> Result<Option<ToolDecision>, Status> {
    let status_record = if let Some(version) = context.version() {
        runtime_state.skill_status(context.skill_id().to_owned(), version.to_owned()).await?
    } else {
        runtime_state.latest_skill_status(context.skill_id().to_owned()).await?
    };
    let Some(status_record) = status_record else {
        let version = context.version().unwrap_or("latest");
        return Ok(Some(ToolDecision {
            allowed: false,
            reason: format!(
                "{SKILL_EXECUTION_DENY_REASON_PREFIX}: skill={} version={} status=missing",
                context.skill_id(),
                version
            ),
            approval_required: false,
            policy_enforced: true,
        }));
    };
    if !matches!(status_record.status, SkillExecutionStatus::Active) {
        return Ok(Some(ToolDecision {
            allowed: false,
            reason: format!(
                "{SKILL_EXECUTION_DENY_REASON_PREFIX}: skill={} version={} status={} reason={}",
                status_record.skill_id,
                status_record.version,
                status_record.status.as_str(),
                status_record.reason.unwrap_or_else(|| "none".to_owned())
            ),
            approval_required: false,
            policy_enforced: true,
        }));
    }

    let evaluation = evaluate_with_context(
        &PolicyRequest {
            principal: request_context.principal.clone(),
            action: "skill.execute".to_owned(),
            resource: format!("skill:{}", context.skill_id()),
        },
        &PolicyRequestContext {
            device_id: Some(request_context.device_id.clone()),
            channel: request_context.channel.clone(),
            skill_id: Some(context.skill_id().to_owned()),
            ..PolicyRequestContext::default()
        },
        &PolicyEvaluationConfig {
            allowlisted_skills: vec![status_record.skill_id.clone()],
            ..PolicyEvaluationConfig::default()
        },
    )
    .map_err(|error| Status::internal(format!("failed to evaluate skill policy: {error}")))?;

    match evaluation.decision {
        PolicyDecision::Allow => Ok(None),
        PolicyDecision::DenyByDefault { reason } => Ok(Some(ToolDecision {
            allowed: false,
            reason: format!(
                "{SKILL_EXECUTION_DENY_REASON_PREFIX}: skill={} reason={reason}",
                context.skill_id()
            ),
            approval_required: false,
            policy_enforced: true,
        })),
    }
}

#[allow(clippy::result_large_err)]
pub(crate) async fn evaluate_tool_proposal_security(
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    session_id: &str,
    run_id: &str,
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
) -> ToolProposalSecurityEvaluation {
    let mut skill_gate_decision: Option<ToolDecision> = None;
    let skill_context = match parse_tool_skill_context(tool_name, input_json) {
        Ok(context) => context,
        Err(error) => {
            warn!(
                run_id = %run_id,
                proposal_id = %proposal_id,
                tool_name = %tool_name,
                error = %error.message(),
                "skill context parsing failed; proposal will be denied safely"
            );
            skill_gate_decision = Some(ToolDecision {
                allowed: false,
                reason: format!(
                    "{SKILL_EXECUTION_DENY_REASON_PREFIX}: invalid skill context: {}",
                    error.message()
                ),
                approval_required: false,
                policy_enforced: true,
            });
            None
        }
    };
    if skill_gate_decision.is_none() {
        if let Some(skill_context) = skill_context.as_ref() {
            skill_gate_decision =
                match evaluate_skill_execution_gate(runtime_state, request_context, skill_context)
                    .await
                {
                    Ok(value) => value,
                    Err(error) => Some(ToolDecision {
                        allowed: false,
                        reason: format!(
                            "{SKILL_EXECUTION_DENY_REASON_PREFIX}: skill={} evaluation_error={}",
                            skill_context.skill_id(),
                            error.message()
                        ),
                        approval_required: false,
                        policy_enforced: true,
                    }),
                };
        }
    }
    let overrides = runtime_state.list_tool_posture_overrides().unwrap_or_default();
    let agent_scope = runtime_state
        .list_agent_bindings(AgentBindingQuery {
            agent_id: None,
            principal: Some(request_context.principal.clone()),
            channel: request_context.channel.clone(),
            session_id: Some(session_id.to_owned()),
            limit: Some(1),
        })
        .await
        .ok()
        .and_then(|bindings| bindings.into_iter().next())
        .map(|binding| ToolPostureScopeRef {
            kind: ToolPostureScopeKind::Agent,
            scope_id: binding.agent_id,
            label: "Agent default".to_owned(),
        });
    let workspace_scope =
        request_context.principal.strip_prefix("workspace:").map(|workspace_id| {
            ToolPostureScopeRef {
                kind: ToolPostureScopeKind::Workspace,
                scope_id: workspace_id.to_owned(),
                label: format!("Workspace {workspace_id}"),
            }
        });
    let effective_posture = evaluate_effective_tool_posture(
        &runtime_state.config,
        overrides.as_slice(),
        &derive_scope_chain(
            ToolPostureScopeRef {
                kind: ToolPostureScopeKind::Session,
                scope_id: session_id.to_owned(),
                label: "Current session".to_owned(),
            },
            workspace_scope,
            agent_scope,
        ),
        tool_name,
    );
    if skill_gate_decision.is_none()
        && effective_posture.effective_state == ToolPostureState::Disabled
    {
        let posture_reason = effective_posture.lock_reason.clone().unwrap_or_else(|| {
            format!("tool posture disabled in {}", effective_posture.source_scope_label)
        });
        skill_gate_decision = Some(ToolDecision {
            allowed: false,
            reason: posture_reason,
            approval_required: false,
            policy_enforced: true,
        });
    }
    let proposal_approval_required = skill_gate_decision
        .as_ref()
        .map(|decision| {
            decision.allowed && effective_posture.effective_state == ToolPostureState::AskEachTime
        })
        .unwrap_or(effective_posture.effective_state == ToolPostureState::AskEachTime);
    let approval_subject_id = build_tool_approval_subject_id(tool_name, skill_context.as_ref());
    ToolProposalSecurityEvaluation {
        skill_context,
        skill_gate_decision,
        approval_subject_id,
        proposal_approval_required,
        effective_posture,
    }
}

fn resolve_tool_proposal_decision(
    remaining_tool_budget: &mut u32,
    policy_request_context: &ToolRequestContext,
    tool_name: &str,
    skill_gate_decision: Option<ToolDecision>,
    effective_posture: &crate::tool_posture::EffectiveToolPosture,
    approval_outcome: Option<&ToolApprovalOutcome>,
    runtime_state: &Arc<GatewayRuntimeState>,
) -> ToolDecision {
    if let Some(skill_gate_decision) = skill_gate_decision {
        return skill_gate_decision;
    }
    let mut decision = decide_tool_call(
        &runtime_state.config.tool_call,
        remaining_tool_budget,
        policy_request_context,
        tool_name,
        approval_outcome.map(|response| response.approved).unwrap_or(false)
            || effective_posture.effective_state == ToolPostureState::AlwaysAllow,
    );
    if decision.allowed && effective_posture.effective_state == ToolPostureState::AlwaysAllow {
        decision.approval_required = false;
        decision.reason = format!(
            "{}; posture={} ({})",
            decision.reason,
            effective_posture.effective_state.as_str(),
            effective_posture.source_scope_label
        );
    }
    apply_tool_approval_outcome(decision, tool_name, approval_outcome)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn resolve_tool_proposal_decision_for_context(
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    channel: Option<&str>,
    session_id: &str,
    run_id: &str,
    tool_name: &str,
    skill_context: Option<&ToolSkillContext>,
    remaining_tool_budget: &mut u32,
    skill_gate_decision: Option<ToolDecision>,
    effective_posture: &crate::tool_posture::EffectiveToolPosture,
    approval_outcome: Option<&ToolApprovalOutcome>,
) -> ToolDecision {
    let policy_request_context = build_tool_policy_request_context(
        request_context,
        channel,
        session_id,
        run_id,
        skill_context,
    );
    let decision = resolve_tool_proposal_decision(
        remaining_tool_budget,
        &policy_request_context,
        tool_name,
        skill_gate_decision,
        effective_posture,
        approval_outcome,
        runtime_state,
    );
    runtime_state.record_tool_decision(tool_name, decision.allowed);
    decision
}

fn build_tool_policy_request_context(
    request_context: &RequestContext,
    channel: Option<&str>,
    session_id: &str,
    run_id: &str,
    skill_context: Option<&ToolSkillContext>,
) -> ToolRequestContext {
    ToolRequestContext {
        principal: request_context.principal.clone(),
        device_id: Some(request_context.device_id.clone()),
        channel: channel.map(ToOwned::to_owned),
        session_id: Some(session_id.to_owned()),
        run_id: Some(run_id.to_owned()),
        skill_id: skill_context.map(|context| context.skill_id().to_owned()),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn record_tool_proposal_decision_audit_trail(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    session_id: &str,
    run_id: &str,
    proposal_id: &str,
    tool_name: &str,
    skill_context: Option<&ToolSkillContext>,
    decision: &ToolDecision,
) -> Result<(), Status> {
    record_policy_decision_journal_event(
        runtime_state,
        context,
        session_id,
        run_id,
        proposal_id,
        tool_name,
        decision.allowed,
        decision.reason.as_str(),
        decision.approval_required,
        decision.policy_enforced,
    )
    .await?;
    record_skill_gate_denial_if_needed(
        runtime_state,
        context,
        session_id,
        run_id,
        proposal_id,
        tool_name,
        skill_context,
        decision,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn record_skill_gate_denial_if_needed(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    session_id: &str,
    run_id: &str,
    proposal_id: &str,
    tool_name: &str,
    skill_context: Option<&ToolSkillContext>,
    decision: &ToolDecision,
) -> Result<(), Status> {
    if decision.allowed || !decision.reason.contains(SKILL_EXECUTION_DENY_REASON_PREFIX) {
        return Ok(());
    }
    let Some(skill_context) = skill_context else {
        return Ok(());
    };
    runtime_state.record_skill_execution_denied();
    record_skill_execution_denied_journal_event(
        runtime_state,
        context,
        session_id,
        run_id,
        proposal_id,
        tool_name,
        skill_context,
        decision.reason.as_str(),
    )
    .await
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn record_policy_decision_journal_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    session_id: &str,
    run_id: &str,
    proposal_id: &str,
    tool_name: &str,
    allowed: bool,
    reason: &str,
    approval_required: bool,
    policy_enforced: bool,
) -> Result<(), Status> {
    runtime_state
        .record_journal_event(JournalAppendRequest {
            event_id: Ulid::new().to_string(),
            session_id: session_id.to_owned(),
            run_id: run_id.to_owned(),
            kind: common_v1::journal_event::EventKind::ToolProposed as i32,
            actor: common_v1::journal_event::EventActor::System as i32,
            timestamp_unix_ms: current_unix_ms(),
            payload_json: tool_decision_journal_payload(
                proposal_id,
                tool_name,
                allowed,
                reason,
                approval_required,
                policy_enforced,
            ),
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
        })
        .await
        .map(|_| ())
}

fn tool_decision_journal_payload(
    proposal_id: &str,
    tool_name: &str,
    allowed: bool,
    reason: &str,
    approval_required: bool,
    policy_enforced: bool,
) -> Vec<u8> {
    json!({
        "event": "policy_decision",
        "proposal_id": proposal_id,
        "tool_name": tool_name,
        "kind": if allowed { "allow" } else { "deny" },
        "reason": reason,
        "approval_required": approval_required,
        "policy_enforced": policy_enforced,
    })
    .to_string()
    .into_bytes()
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn record_skill_execution_denied_journal_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    session_id: &str,
    run_id: &str,
    proposal_id: &str,
    tool_name: &str,
    skill_context: &ToolSkillContext,
    reason: &str,
) -> Result<(), Status> {
    runtime_state
        .record_journal_event(JournalAppendRequest {
            event_id: Ulid::new().to_string(),
            session_id: session_id.to_owned(),
            run_id: run_id.to_owned(),
            kind: common_v1::journal_event::EventKind::ToolProposed as i32,
            actor: common_v1::journal_event::EventActor::System as i32,
            timestamp_unix_ms: current_unix_ms(),
            payload_json: json!({
                "event": "skill.execution_denied",
                "proposal_id": proposal_id,
                "tool_name": tool_name,
                "skill_id": skill_context.skill_id(),
                "skill_version": skill_context.version(),
                "reason": reason,
            })
            .to_string()
            .into_bytes(),
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
        })
        .await
        .map(|_| ())
}
