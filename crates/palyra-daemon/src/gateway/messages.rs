use super::*;
use crate::agents::SessionAgentBinding;

pub(crate) fn session_summary_message(
    session: &OrchestratorSessionRecord,
) -> gateway_v1::SessionSummary {
    gateway_v1::SessionSummary {
        session_id: Some(common_v1::CanonicalId { ulid: session.session_id.clone() }),
        session_key: session.session_key.clone(),
        session_label: session.session_label.clone().unwrap_or_default(),
        created_at_unix_ms: session.created_at_unix_ms,
        updated_at_unix_ms: session.updated_at_unix_ms,
        last_run_id: session
            .last_run_id
            .as_ref()
            .map(|run_id| common_v1::CanonicalId { ulid: run_id.clone() }),
        archived_at_unix_ms: session.archived_at_unix_ms.unwrap_or_default(),
        title: session.title.clone(),
        title_source: session.title_source.clone(),
        title_generator_version: session.title_generator_version.clone().unwrap_or_default(),
        preview: session.preview.clone().unwrap_or_default(),
        preview_state: if session.preview.is_some() {
            "computed".to_owned()
        } else {
            "missing".to_owned()
        },
        last_intent: session.last_intent.clone().unwrap_or_default(),
        last_summary: session.last_summary.clone().unwrap_or_default(),
        match_snippet: session.match_snippet.clone().unwrap_or_default(),
        branch_state: session.branch_state.clone(),
        parent_session_id: session
            .parent_session_id
            .as_ref()
            .map(|session_id| common_v1::CanonicalId { ulid: session_id.clone() }),
        last_run_state: session.last_run_state.clone().unwrap_or_default(),
    }
}

pub(crate) fn agent_message(agent: &AgentRecord) -> gateway_v1::Agent {
    gateway_v1::Agent {
        agent_id: agent.agent_id.clone(),
        display_name: agent.display_name.clone(),
        agent_dir: agent.agent_dir.clone(),
        workspace_roots: agent.workspace_roots.clone(),
        default_model_profile: agent.default_model_profile.clone(),
        default_tool_allowlist: agent.default_tool_allowlist.clone(),
        default_skill_allowlist: agent.default_skill_allowlist.clone(),
        created_at_unix_ms: agent.created_at_unix_ms,
        updated_at_unix_ms: agent.updated_at_unix_ms,
    }
}

pub(crate) fn agent_binding_message(binding: &SessionAgentBinding) -> gateway_v1::AgentBinding {
    gateway_v1::AgentBinding {
        principal: binding.principal.clone(),
        channel: binding.channel.clone().unwrap_or_default(),
        session_id: Some(common_v1::CanonicalId { ulid: binding.session_id.clone() }),
        agent_id: binding.agent_id.clone(),
        updated_at_unix_ms: binding.updated_at_unix_ms,
    }
}

pub(crate) fn agent_resolution_source_to_proto(source: AgentResolutionSource) -> i32 {
    match source {
        AgentResolutionSource::SessionBinding => {
            gateway_v1::AgentResolutionSource::SessionBinding as i32
        }
        AgentResolutionSource::Default => gateway_v1::AgentResolutionSource::Default as i32,
        AgentResolutionSource::Fallback => gateway_v1::AgentResolutionSource::Fallback as i32,
    }
}

pub(crate) fn agent_resolution_source_label(source: AgentResolutionSource) -> &'static str {
    match source {
        AgentResolutionSource::SessionBinding => "session_binding",
        AgentResolutionSource::Default => "default",
        AgentResolutionSource::Fallback => "fallback",
    }
}

pub(crate) fn approval_option_messages(
    options: &[ApprovalPromptOption],
) -> Vec<common_v1::ApprovalOption> {
    options
        .iter()
        .map(|option| common_v1::ApprovalOption {
            option_id: option.option_id.clone(),
            label: option.label.clone(),
            description: option.description.clone(),
            default_selected: option.default_selected,
            decision_scope: approval_scope_to_proto(option.decision_scope),
            timebox_ttl_ms: option.timebox_ttl_ms.unwrap_or_default(),
        })
        .collect()
}

pub(crate) fn approval_prompt_message(prompt: &ApprovalPromptRecord) -> common_v1::ApprovalPrompt {
    common_v1::ApprovalPrompt {
        title: prompt.title.clone(),
        risk_level: approval_risk_to_proto(prompt.risk_level),
        subject_id: prompt.subject_id.clone(),
        summary: prompt.summary.clone(),
        options: approval_option_messages(prompt.options.as_slice()),
        timeout_seconds: prompt.timeout_seconds,
        details_json: prompt.details_json.as_bytes().to_vec(),
        policy_explanation: prompt.policy_explanation.clone(),
    }
}

pub(crate) fn approval_policy_snapshot_message(
    value: &ApprovalPolicySnapshot,
) -> gateway_v1::ApprovalPolicySnapshot {
    gateway_v1::ApprovalPolicySnapshot {
        policy_id: value.policy_id.clone(),
        policy_hash: value.policy_hash.clone(),
        evaluation_summary: value.evaluation_summary.clone(),
    }
}

pub(crate) fn approval_record_message(record: &ApprovalRecord) -> gateway_v1::ApprovalRecord {
    gateway_v1::ApprovalRecord {
        v: CANONICAL_PROTOCOL_MAJOR,
        approval_id: Some(common_v1::CanonicalId { ulid: record.approval_id.clone() }),
        session_id: Some(common_v1::CanonicalId { ulid: record.session_id.clone() }),
        run_id: Some(common_v1::CanonicalId { ulid: record.run_id.clone() }),
        principal: record.principal.clone(),
        device_id: record.device_id.clone(),
        channel: record.channel.clone().unwrap_or_default(),
        requested_at_unix_ms: record.requested_at_unix_ms,
        resolved_at_unix_ms: record.resolved_at_unix_ms.unwrap_or_default(),
        subject_type: approval_subject_type_to_proto(record.subject_type),
        subject_id: record.subject_id.clone(),
        request_summary: record.request_summary.clone(),
        policy_snapshot: Some(approval_policy_snapshot_message(&record.policy_snapshot)),
        prompt: Some(approval_prompt_message(&record.prompt)),
        decision: record
            .decision
            .map(approval_decision_to_proto)
            .unwrap_or(gateway_v1::ApprovalDecision::Unspecified as i32),
        decision_scope: record
            .decision_scope
            .map(approval_scope_to_proto)
            .unwrap_or(common_v1::ApprovalDecisionScope::Unspecified as i32),
        decision_reason: record.decision_reason.clone().unwrap_or_default(),
        decision_scope_ttl_ms: record.decision_scope_ttl_ms.unwrap_or_default(),
    }
}
