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
    application::approvals::{
        apply_tool_approval_outcome, build_tool_approval_subject_id, ApprovalExecutionContext,
    },
    application::execution_gate::{
        append_audit_finalization_step, evaluate_execution_gate_pipeline,
        ExecutionGatePipelineInput, ExecutionGateReport, ToolProposalApprovalState,
    },
    execution_backends::{
        build_execution_backend_inventory_with_worker_state, resolve_execution_backend,
        ExecutionBackendPreference, ExecutionBackendResolution,
    },
    gateway::{
        current_unix_ms, GatewayRuntimeState, ToolApprovalOutcome, ToolSkillContext,
        SKILL_EXECUTION_DENY_REASON_PREFIX,
    },
    journal::{JournalAppendRequest, SkillExecutionStatus},
    tool_posture::{
        derive_scope_chain, evaluate_effective_tool_posture, ToolPostureScopeKind,
        ToolPostureScopeRef, ToolPostureState,
    },
    tool_protocol::{
        decide_tool_call, tool_metadata, ToolCapability, ToolDecision, ToolRequestContext,
    },
    transport::grpc::{auth::RequestContext, proto::palyra::common::v1 as common_v1},
};

#[derive(Debug, Clone)]
pub(crate) struct ToolProposalSecurityEvaluation {
    pub(crate) skill_context: Option<ToolSkillContext>,
    pub(crate) skill_gate_decision: Option<ToolDecision>,
    pub(crate) approval_subject_id: String,
    pub(crate) proposal_approval_required: bool,
    pub(crate) effective_posture: crate::tool_posture::EffectiveToolPosture,
    pub(crate) backend_selection: ToolProposalBackendSelection,
}

#[derive(Debug, Clone)]
pub(crate) struct ResolvedToolProposalDecision {
    pub(crate) decision: ToolDecision,
    pub(crate) gate_report: Option<ExecutionGateReport>,
}

#[derive(Debug, Clone)]
pub(crate) struct ToolProposalBackendSelection {
    pub(crate) agent_id: Option<String>,
    pub(crate) requested_preference: ExecutionBackendPreference,
    pub(crate) resolution: ExecutionBackendResolution,
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

async fn derive_tool_proposal_backend_selection(
    runtime_state: &Arc<GatewayRuntimeState>,
    agent_id: Option<&str>,
) -> ToolProposalBackendSelection {
    let requested_preference = if let Some(agent_id) = agent_id {
        runtime_state
            .get_agent(agent_id.to_owned())
            .await
            .map(|(agent, _)| agent.execution_backend_preference)
            .unwrap_or_default()
    } else {
        ExecutionBackendPreference::Automatic
    };
    let inventory = build_execution_backend_inventory_with_worker_state(
        &runtime_state.config.tool_call.process_runner,
        &[],
        current_unix_ms(),
        &runtime_state.config.feature_rollouts,
        runtime_state.worker_fleet_snapshot(),
        &runtime_state.worker_fleet_policy(),
    );
    let resolution = resolve_execution_backend(requested_preference, &inventory);
    ToolProposalBackendSelection {
        agent_id: agent_id.map(ToOwned::to_owned),
        requested_preference,
        resolution,
    }
}

pub(crate) fn approval_execution_context_for_backend_selection(
    backend_selection: &ToolProposalBackendSelection,
) -> Option<ApprovalExecutionContext> {
    let resolution = &backend_selection.resolution;
    let default_local_resolution = backend_selection.requested_preference
        == ExecutionBackendPreference::Automatic
        && resolution.resolved == ExecutionBackendPreference::LocalSandbox
        && !resolution.fallback_used
        && !resolution.approval_required;
    if default_local_resolution {
        return None;
    }
    Some(ApprovalExecutionContext {
        requested_backend: backend_selection.requested_preference.as_str().to_owned(),
        resolved_backend: resolution.resolved.as_str().to_owned(),
        reason_code: resolution.reason_code.clone(),
        approval_required: resolution.approval_required,
        reason: resolution.reason.clone(),
        agent_id: backend_selection.agent_id.clone(),
    })
}

fn backend_capability_label(capability: ToolCapability) -> &'static str {
    match capability {
        ToolCapability::ProcessExec => "process_exec",
        ToolCapability::Network => "network",
        ToolCapability::SecretsRead => "secrets_read",
        ToolCapability::FilesystemWrite => "filesystem_write",
    }
}

pub(crate) fn evaluate_backend_capability_gate(
    tool_name: &str,
    backend_selection: &ToolProposalBackendSelection,
) -> Option<ToolDecision> {
    if backend_selection.resolution.resolved != ExecutionBackendPreference::NetworkedWorker {
        return None;
    }
    let restricted_capabilities = tool_metadata(tool_name)
        .map(|metadata| {
            metadata
                .capabilities
                .iter()
                .copied()
                .filter(|capability| {
                    matches!(
                        capability,
                        ToolCapability::ProcessExec
                            | ToolCapability::SecretsRead
                            | ToolCapability::FilesystemWrite
                    )
                })
                .map(backend_capability_label)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if restricted_capabilities.is_empty() {
        return None;
    }
    Some(ToolDecision {
        allowed: false,
        reason: format!(
            "backend policy blocked tool={tool_name}; reason_code=backend.policy.capability_denied; resolved_backend={}; blocked_capabilities={}; remediation=switch agent backend_preference to local_sandbox or automatic; backend_reason={}",
            backend_selection.resolution.resolved.as_str(),
            restricted_capabilities.join(","),
            backend_selection.resolution.reason
        ),
        approval_required: false,
        policy_enforced: true,
    })
}

pub(crate) fn annotate_tool_decision_with_backend_context(
    mut decision: ToolDecision,
    backend_selection: &ToolProposalBackendSelection,
) -> ToolDecision {
    let resolution = &backend_selection.resolution;
    let default_local_resolution = backend_selection.requested_preference
        == ExecutionBackendPreference::Automatic
        && resolution.resolved == ExecutionBackendPreference::LocalSandbox
        && !resolution.fallback_used
        && !resolution.approval_required;
    if default_local_resolution {
        return decision;
    }
    let agent_fragment = backend_selection
        .agent_id
        .as_deref()
        .map(|agent_id| format!("; backend_agent_id={agent_id}"))
        .unwrap_or_default();
    decision.reason = format!(
        "{}; backend_requested={}; backend_resolved={}; backend_reason_code={}; backend_approval_required={}; backend_reason={}{}",
        decision.reason,
        backend_selection.requested_preference.as_str(),
        resolution.resolved.as_str(),
        resolution.reason_code,
        resolution.approval_required,
        resolution.reason,
        agent_fragment
    );
    decision
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
    let agent_binding = runtime_state
        .list_agent_bindings(AgentBindingQuery {
            agent_id: None,
            principal: Some(request_context.principal.clone()),
            channel: request_context.channel.clone(),
            session_id: Some(session_id.to_owned()),
            limit: Some(1),
        })
        .await
        .ok()
        .and_then(|bindings| bindings.into_iter().next());
    let agent_scope = agent_binding.as_ref().map(|binding| ToolPostureScopeRef {
        kind: ToolPostureScopeKind::Agent,
        scope_id: binding.agent_id.clone(),
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
    let backend_selection = derive_tool_proposal_backend_selection(
        runtime_state,
        agent_binding.as_ref().map(|binding| binding.agent_id.as_str()),
    )
    .await;
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
    if skill_gate_decision.is_none() {
        skill_gate_decision = evaluate_backend_capability_gate(tool_name, &backend_selection);
    }
    let proposal_approval_required = skill_gate_decision
        .as_ref()
        .map(|decision| {
            decision.allowed && effective_posture.effective_state == ToolPostureState::AskEachTime
        })
        .unwrap_or(effective_posture.effective_state == ToolPostureState::AskEachTime)
        || backend_selection.resolution.approval_required;
    let approval_subject_id = build_tool_approval_subject_id(tool_name, skill_context.as_ref());
    ToolProposalSecurityEvaluation {
        skill_context,
        skill_gate_decision,
        approval_subject_id,
        proposal_approval_required,
        effective_posture,
        backend_selection,
    }
}
#[allow(clippy::too_many_arguments)]
fn resolve_tool_proposal_decision(
    remaining_tool_budget: &mut u32,
    policy_request_context: &ToolRequestContext,
    tool_name: &str,
    skill_gate_decision: Option<ToolDecision>,
    effective_posture: &crate::tool_posture::EffectiveToolPosture,
    backend_selection: &ToolProposalBackendSelection,
    approval_outcome: Option<&ToolApprovalOutcome>,
    runtime_state: &Arc<GatewayRuntimeState>,
) -> ToolDecision {
    if let Some(skill_gate_decision) = skill_gate_decision {
        return annotate_tool_decision_with_backend_context(skill_gate_decision, backend_selection);
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
    annotate_tool_decision_with_backend_context(
        apply_tool_approval_outcome(decision, tool_name, approval_outcome),
        backend_selection,
    )
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
    proposal_approval_required: bool,
    effective_posture: &crate::tool_posture::EffectiveToolPosture,
    backend_selection: &ToolProposalBackendSelection,
    approval_state: ToolProposalApprovalState<'_>,
) -> ResolvedToolProposalDecision {
    let policy_request_context = build_tool_policy_request_context(
        request_context,
        channel,
        session_id,
        run_id,
        skill_context,
    );
    let mut legacy_budget = *remaining_tool_budget;
    let legacy_decision = resolve_tool_proposal_decision(
        &mut legacy_budget,
        &policy_request_context,
        tool_name,
        skill_gate_decision.clone(),
        effective_posture,
        backend_selection,
        approval_state.outcome,
        runtime_state,
    );
    let rollout_enabled = runtime_state.config.feature_rollouts.execution_gate_pipeline_v2.enabled;
    let (decision, gate_report, resulting_budget) = if rollout_enabled {
        let mut pipeline_outcome = evaluate_execution_gate_pipeline(ExecutionGatePipelineInput {
            tool_call_config: &runtime_state.config.tool_call,
            request_context: &policy_request_context,
            tool_name,
            skill_context,
            skill_gate_decision,
            proposal_approval_required,
            effective_posture,
            backend_selection,
            approval_state,
            remaining_budget: *remaining_tool_budget,
        });
        append_audit_finalization_step(&mut pipeline_outcome.report, &legacy_decision);
        (
            pipeline_outcome.decision,
            Some(pipeline_outcome.report),
            pipeline_outcome.remaining_budget,
        )
    } else {
        (legacy_decision, None, legacy_budget)
    };
    *remaining_tool_budget = resulting_budget;
    runtime_state.record_tool_decision(tool_name, decision.allowed);
    ResolvedToolProposalDecision { decision, gate_report }
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
    gate_report: Option<&ExecutionGateReport>,
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
        gate_report,
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
    gate_report: Option<&ExecutionGateReport>,
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
                gate_report,
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
    gate_report: Option<&ExecutionGateReport>,
) -> Vec<u8> {
    let mut payload = json!({
        "event": "policy_decision",
        "proposal_id": proposal_id,
        "tool_name": tool_name,
        "kind": if allowed { "allow" } else { "deny" },
        "reason": reason,
        "approval_required": approval_required,
        "policy_enforced": policy_enforced,
    });
    if let Some(gate_report) = gate_report {
        payload["execution_gate"] = json!(gate_report);
    }
    payload.to_string().into_bytes()
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

#[cfg(test)]
mod tests {
    use crate::{
        execution_backends::{ExecutionBackendPreference, ExecutionBackendResolution},
        tool_protocol::ToolDecision,
    };

    use super::{
        annotate_tool_decision_with_backend_context, evaluate_backend_capability_gate,
        ToolProposalBackendSelection,
    };

    fn networked_worker_selection() -> ToolProposalBackendSelection {
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

    #[test]
    fn networked_worker_backend_denies_filesystem_write_tools() {
        let selection = networked_worker_selection();
        let decision = evaluate_backend_capability_gate("palyra.fs.apply_patch", &selection)
            .expect("filesystem write should be blocked on networked workers");
        assert!(!decision.allowed);
        assert!(decision.reason.contains("backend.policy.capability_denied"));
        assert!(decision.reason.contains("filesystem_write"));
    }

    #[test]
    fn networked_worker_annotation_surfaces_backend_reason_codes() {
        let selection = networked_worker_selection();
        let decision = annotate_tool_decision_with_backend_context(
            ToolDecision {
                allowed: true,
                reason: "tool is allowlisted by Cedar runtime policy".to_owned(),
                approval_required: true,
                policy_enforced: true,
            },
            &selection,
        );
        assert!(decision.reason.contains("backend_requested=networked_worker"));
        assert!(decision.reason.contains("backend_reason_code=backend.available.networked_worker"));
    }

    #[test]
    fn default_local_backend_annotation_keeps_reason_compact() {
        let selection = ToolProposalBackendSelection {
            agent_id: None,
            requested_preference: ExecutionBackendPreference::Automatic,
            resolution: ExecutionBackendResolution {
                requested: ExecutionBackendPreference::Automatic,
                resolved: ExecutionBackendPreference::LocalSandbox,
                fallback_used: false,
                reason_code: "backend.default.local_sandbox".to_owned(),
                approval_required: false,
                reason: "automatic stays local".to_owned(),
            },
        };
        let decision = annotate_tool_decision_with_backend_context(
            ToolDecision {
                allowed: true,
                reason: "tool is allowlisted by Cedar runtime policy".to_owned(),
                approval_required: false,
                policy_enforced: true,
            },
            &selection,
        );
        assert_eq!(decision.reason, "tool is allowlisted by Cedar runtime policy");
    }
}
