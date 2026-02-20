use std::{str::FromStr, sync::OnceLock};

use cedar_policy::{Authorizer, Context, Decision, Entities, EntityUid, PolicySet, Request};
use serde_json::json;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyRequest {
    pub principal: String,
    pub action: String,
    pub resource: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PolicyEvaluationConfig {
    pub allowlisted_tools: Vec<String>,
    pub allowlisted_skills: Vec<String>,
    pub allow_sensitive_tools: bool,
    pub sensitive_tool_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyDecision {
    Allow,
    DenyByDefault { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyExplanation {
    pub evaluated_with_cedar: bool,
    pub reason: String,
    pub matched_policy_ids: Vec<String>,
    pub diagnostics_errors: Vec<String>,
    pub is_sensitive_action: bool,
    pub is_allowlisted_tool: bool,
    pub is_allowlisted_skill: bool,
    pub requested_tool: Option<String>,
    pub requested_skill: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyEvaluation {
    pub decision: PolicyDecision,
    pub explanation: PolicyExplanation,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum PolicyEngineError {
    #[error("failed to initialize Cedar policy engine: {message}")]
    EngineInitialization { message: String },
    #[error("failed to build Cedar request context: {message}")]
    InvalidContext { message: String },
    #[error("failed to construct Cedar authorization request: {message}")]
    InvalidRequest { message: String },
}

const DEFAULT_POLICY_SRC: &str = r#"
@id("deny_sensitive_without_approval")
forbid(principal, action, resource)
when {
    context.is_sensitive_action &&
    !context.allow_sensitive_tools
};

@id("allow_read_only_actions")
permit(principal, action, resource)
when {
    context.action == "tool.read" ||
    context.action == "tool.read.status" ||
    context.action == "tool.status" ||
    context.action == "tool.list" ||
    context.action == "tool.health" ||
    context.action == "tool.get" ||
    context.action == "daemon.status" ||
    context.action == "protocol.version"
};

@id("allow_allowlisted_tool_execute")
permit(principal, action, resource)
when {
    context.action == "tool.execute" &&
    context.is_allowlisted_tool
};

@id("allow_allowlisted_skill_execute")
permit(principal, action, resource)
when {
    context.action == "skill.execute" &&
    context.is_allowlisted_skill
};

@id("allow_cron_management_actions")
permit(principal, action, resource)
when {
    context.action == "cron.create" ||
    context.action == "cron.update" ||
    context.action == "cron.delete" ||
    context.action == "cron.get" ||
    context.action == "cron.list" ||
    context.action == "cron.logs" ||
    context.action == "cron.run"
};

@id("allow_memory_actions")
permit(principal, action, resource)
when {
    context.action == "memory.ingest" ||
    context.action == "memory.search" ||
    context.action == "memory.get" ||
    context.action == "memory.list" ||
    context.action == "memory.delete" ||
    context.action == "memory.purge"
};

@id("allow_vault_actions")
permit(principal, action, resource)
when {
    context.action == "vault.put" ||
    context.action == "vault.get" ||
    context.action == "vault.delete" ||
    context.action == "vault.list"
};
"#;

const POLICY_DENY_REASON: &str = "tool execution denied by default: tool is not allowlisted";
const SKILL_POLICY_DENY_REASON: &str =
    "skill execution denied by default: skill is not active/allowlisted";
const SENSITIVE_DENY_REASON: &str =
    "sensitive action blocked by default; explicit user approval required";
const BASELINE_DENY_REASON: &str = "deny-by-default baseline policy";

#[must_use]
pub fn evaluate(request: &PolicyRequest) -> PolicyDecision {
    match evaluate_with_config(request, &PolicyEvaluationConfig::default()) {
        Ok(evaluation) => evaluation.decision,
        Err(error) => PolicyDecision::DenyByDefault {
            reason: format!("policy evaluation failed safely: {error}"),
        },
    }
}

pub fn evaluate_with_config(
    request: &PolicyRequest,
    config: &PolicyEvaluationConfig,
) -> Result<PolicyEvaluation, PolicyEngineError> {
    let normalized_action = request.action.to_ascii_lowercase();
    let requested_tool = requested_tool_name(normalized_action.as_str(), request.resource.as_str());
    let requested_skill =
        requested_skill_name(normalized_action.as_str(), request.resource.as_str());
    let is_allowlisted_tool =
        is_allowlisted_tool(requested_tool.as_deref(), config.allowlisted_tools.as_slice());
    let is_allowlisted_skill =
        is_allowlisted_skill(requested_skill.as_deref(), config.allowlisted_skills.as_slice());
    let is_sensitive_action = is_sensitive_action(
        normalized_action.as_str(),
        requested_tool.as_deref(),
        request,
        config.sensitive_tool_names.as_slice(),
    );

    let context = Context::from_json_value(
        json!({
            "action": normalized_action,
            "resource": request.resource,
            "is_sensitive_action": is_sensitive_action,
            "is_allowlisted_tool": is_allowlisted_tool,
            "is_allowlisted_skill": is_allowlisted_skill,
            "allow_sensitive_tools": config.allow_sensitive_tools,
        }),
        None,
    )
    .map_err(|error| PolicyEngineError::InvalidContext { message: error.to_string() })?;

    let cedar_request =
        Request::new(principal_uid()?, action_uid()?, resource_uid()?, context, None)
            .map_err(|error| PolicyEngineError::InvalidRequest { message: error.to_string() })?;

    let response =
        Authorizer::new().is_authorized(&cedar_request, default_policy_set()?, &Entities::empty());

    let mut matched_policy_ids =
        response.diagnostics().reason().map(ToString::to_string).collect::<Vec<_>>();
    matched_policy_ids.sort();
    let diagnostics_errors =
        response.diagnostics().errors().map(ToString::to_string).collect::<Vec<_>>();

    let reason = decision_reason(
        response.decision(),
        normalized_action.as_str(),
        is_sensitive_action,
        is_allowlisted_tool,
        is_allowlisted_skill,
        config.allow_sensitive_tools,
        diagnostics_errors.as_slice(),
    );
    let decision = if response.decision() == Decision::Allow {
        PolicyDecision::Allow
    } else {
        PolicyDecision::DenyByDefault { reason: reason.clone() }
    };

    Ok(PolicyEvaluation {
        decision,
        explanation: PolicyExplanation {
            evaluated_with_cedar: true,
            reason,
            matched_policy_ids,
            diagnostics_errors,
            is_sensitive_action,
            is_allowlisted_tool,
            is_allowlisted_skill,
            requested_tool,
            requested_skill,
        },
    })
}

fn default_policy_set() -> Result<&'static PolicySet, PolicyEngineError> {
    static POLICY_SET: OnceLock<Result<PolicySet, PolicyEngineError>> = OnceLock::new();
    match POLICY_SET.get_or_init(|| {
        PolicySet::from_str(DEFAULT_POLICY_SRC)
            .map_err(|error| PolicyEngineError::EngineInitialization { message: error.to_string() })
    }) {
        Ok(policy_set) => Ok(policy_set),
        Err(error) => Err(error.clone()),
    }
}

fn principal_uid() -> Result<EntityUid, PolicyEngineError> {
    static UID: OnceLock<Result<EntityUid, PolicyEngineError>> = OnceLock::new();
    match UID.get_or_init(|| parse_entity_uid(r#"Principal::"request_principal""#)) {
        Ok(uid) => Ok(uid.clone()),
        Err(error) => Err(error.clone()),
    }
}

fn action_uid() -> Result<EntityUid, PolicyEngineError> {
    static UID: OnceLock<Result<EntityUid, PolicyEngineError>> = OnceLock::new();
    match UID.get_or_init(|| parse_entity_uid(r#"Action::"request_action""#)) {
        Ok(uid) => Ok(uid.clone()),
        Err(error) => Err(error.clone()),
    }
}

fn resource_uid() -> Result<EntityUid, PolicyEngineError> {
    static UID: OnceLock<Result<EntityUid, PolicyEngineError>> = OnceLock::new();
    match UID.get_or_init(|| parse_entity_uid(r#"Resource::"request_resource""#)) {
        Ok(uid) => Ok(uid.clone()),
        Err(error) => Err(error.clone()),
    }
}

fn parse_entity_uid(raw: &str) -> Result<EntityUid, PolicyEngineError> {
    raw.parse::<EntityUid>().map_err(|error| PolicyEngineError::EngineInitialization {
        message: format!("failed to parse Cedar entity UID '{raw}': {error}"),
    })
}

fn decision_reason(
    decision: Decision,
    normalized_action: &str,
    is_sensitive_action: bool,
    is_allowlisted_tool: bool,
    is_allowlisted_skill: bool,
    allow_sensitive_tools: bool,
    diagnostics_errors: &[String],
) -> String {
    if decision == Decision::Allow {
        if normalized_action == "tool.execute" {
            return "tool execution allowed by Cedar policy (allowlisted tool)".to_owned();
        }
        if normalized_action == "skill.execute" {
            return "skill execution allowed by Cedar policy (active/allowlisted skill)".to_owned();
        }
        if normalized_action.starts_with("cron.") {
            return "cron action allowed by Cedar policy".to_owned();
        }
        if normalized_action.starts_with("memory.") {
            return "memory action allowed by Cedar policy".to_owned();
        }
        if normalized_action.starts_with("vault.") {
            return "vault action allowed by Cedar policy".to_owned();
        }
        return "read-only action allowed by Cedar baseline policy".to_owned();
    }

    if let Some(first_error) = diagnostics_errors.first() {
        return format!("policy evaluation diagnostics triggered deny-by-default: {first_error}");
    }

    if is_sensitive_action && !allow_sensitive_tools {
        return SENSITIVE_DENY_REASON.to_owned();
    }

    if normalized_action == "tool.execute" && !is_allowlisted_tool {
        return POLICY_DENY_REASON.to_owned();
    }
    if normalized_action == "skill.execute" && !is_allowlisted_skill {
        return SKILL_POLICY_DENY_REASON.to_owned();
    }

    BASELINE_DENY_REASON.to_owned()
}

fn requested_tool_name(normalized_action: &str, resource: &str) -> Option<String> {
    if normalized_action != "tool.execute" {
        return None;
    }
    let trimmed = resource.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Some(tool_name) = trimmed.strip_prefix("tool:") {
        let tool_name = tool_name.trim();
        if !tool_name.is_empty() {
            return Some(tool_name.to_ascii_lowercase());
        }
    }
    Some(trimmed.to_ascii_lowercase())
}

fn requested_skill_name(normalized_action: &str, resource: &str) -> Option<String> {
    if normalized_action != "skill.execute" {
        return None;
    }
    let trimmed = resource.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Some(skill_name) = trimmed.strip_prefix("skill:") {
        let skill_name = skill_name.trim();
        if !skill_name.is_empty() {
            return Some(skill_name.to_ascii_lowercase());
        }
    }
    Some(trimmed.to_ascii_lowercase())
}

fn is_allowlisted_tool(requested_tool: Option<&str>, allowlisted_tools: &[String]) -> bool {
    let Some(requested_tool) = requested_tool else {
        return false;
    };
    allowlisted_tools.iter().any(|allowlisted| allowlisted.eq_ignore_ascii_case(requested_tool))
}

fn is_allowlisted_skill(requested_skill: Option<&str>, allowlisted_skills: &[String]) -> bool {
    let Some(requested_skill) = requested_skill else {
        return false;
    };
    allowlisted_skills.iter().any(|allowlisted| allowlisted.eq_ignore_ascii_case(requested_skill))
}

fn is_sensitive_action(
    normalized_action: &str,
    requested_tool: Option<&str>,
    request: &PolicyRequest,
    sensitive_tool_names: &[String],
) -> bool {
    if normalized_action.starts_with("vault.") {
        return false;
    }
    if normalized_action == "tool.execute" {
        return is_sensitive_tool_name(requested_tool, sensitive_tool_names);
    }
    is_sensitive_action_heuristic(request)
}

fn is_sensitive_tool_name(requested_tool: Option<&str>, sensitive_tool_names: &[String]) -> bool {
    let Some(requested_tool) = requested_tool else {
        return false;
    };
    sensitive_tool_names
        .iter()
        .any(|sensitive_tool| sensitive_tool.eq_ignore_ascii_case(requested_tool))
}

fn is_sensitive_action_heuristic(request: &PolicyRequest) -> bool {
    let action = request.action.to_ascii_lowercase();
    let resource = request.resource.to_ascii_lowercase();
    ["shell", "delete", "payment"]
        .iter()
        .any(|keyword| action.contains(keyword) || resource.contains(keyword))
        || resource.contains("secrets")
        || resource.contains("credential")
}

#[cfg(test)]
mod tests {
    use super::{
        evaluate, evaluate_with_config, PolicyDecision, PolicyEvaluationConfig, PolicyRequest,
        POLICY_DENY_REASON, SENSITIVE_DENY_REASON, SKILL_POLICY_DENY_REASON,
    };

    #[test]
    fn default_policy_denies_all_requests() {
        let request = PolicyRequest {
            principal: "user:bootstrap".to_owned(),
            action: "tool.execute".to_owned(),
            resource: "tool:filesystem".to_owned(),
        };

        let decision = evaluate(&request);

        assert!(matches!(decision, PolicyDecision::DenyByDefault { .. }));
    }

    #[test]
    fn sensitive_actions_require_explicit_approval() {
        let request = PolicyRequest {
            principal: "user:bootstrap".to_owned(),
            action: "tool.execute".to_owned(),
            resource: "tool:palyra.process.run".to_owned(),
        };
        let config = PolicyEvaluationConfig {
            sensitive_tool_names: vec!["palyra.process.run".to_owned()],
            ..PolicyEvaluationConfig::default()
        };

        let evaluation = evaluate_with_config(&request, &config).expect("evaluation");

        assert_eq!(
            evaluation.decision,
            PolicyDecision::DenyByDefault { reason: SENSITIVE_DENY_REASON.to_owned() }
        );
        assert!(evaluation.explanation.is_sensitive_action);
        assert!(!evaluation.explanation.matched_policy_ids.is_empty());
    }

    #[test]
    fn allowlisted_sensitive_tool_resource_requires_explicit_approval() {
        let request = PolicyRequest {
            principal: "user:bootstrap".to_owned(),
            action: "tool.execute".to_owned(),
            resource: "tool:palyra.process.run".to_owned(),
        };
        let config = PolicyEvaluationConfig {
            allowlisted_tools: vec!["palyra.process.run".to_owned()],
            allowlisted_skills: Vec::new(),
            allow_sensitive_tools: false,
            sensitive_tool_names: vec!["palyra.process.run".to_owned()],
        };

        let evaluation = evaluate_with_config(&request, &config).expect("evaluation");

        assert_eq!(
            evaluation.decision,
            PolicyDecision::DenyByDefault { reason: SENSITIVE_DENY_REASON.to_owned() }
        );
        assert!(evaluation.explanation.is_sensitive_action);
        assert!(evaluation.explanation.is_allowlisted_tool);
    }

    #[test]
    fn read_only_actions_are_allowed() {
        let request = PolicyRequest {
            principal: "user:bootstrap".to_owned(),
            action: "tool.read.status".to_owned(),
            resource: "tool:daemon".to_owned(),
        };

        let evaluation =
            evaluate_with_config(&request, &PolicyEvaluationConfig::default()).expect("evaluation");

        assert_eq!(evaluation.decision, PolicyDecision::Allow);
        assert!(!evaluation.explanation.matched_policy_ids.is_empty());
    }

    #[test]
    fn cron_actions_are_explicitly_allowed() {
        let request = PolicyRequest {
            principal: "user:ops".to_owned(),
            action: "cron.create".to_owned(),
            resource: "cron:job".to_owned(),
        };

        let evaluation =
            evaluate_with_config(&request, &PolicyEvaluationConfig::default()).expect("evaluation");

        assert_eq!(evaluation.decision, PolicyDecision::Allow);
        assert!(!evaluation.explanation.matched_policy_ids.is_empty());
    }

    #[test]
    fn memory_actions_are_explicitly_allowed() {
        let request = PolicyRequest {
            principal: "user:ops".to_owned(),
            action: "memory.search".to_owned(),
            resource: "memory:session".to_owned(),
        };

        let evaluation =
            evaluate_with_config(&request, &PolicyEvaluationConfig::default()).expect("evaluation");

        assert_eq!(evaluation.decision, PolicyDecision::Allow);
        assert!(
            evaluation.explanation.reason.contains("memory action allowed"),
            "memory allow reason should reflect dedicated memory policy"
        );
    }

    #[test]
    fn vault_actions_are_explicitly_allowed() {
        let request = PolicyRequest {
            principal: "user:ops".to_owned(),
            action: "vault.get".to_owned(),
            resource: "secrets:global:openai_api_key".to_owned(),
        };

        let evaluation =
            evaluate_with_config(&request, &PolicyEvaluationConfig::default()).expect("evaluation");

        assert_eq!(evaluation.decision, PolicyDecision::Allow);
        assert!(
            evaluation.explanation.reason.contains("vault action allowed"),
            "vault allow reason should reflect dedicated vault policy"
        );
    }

    #[test]
    fn tool_execute_is_allowed_only_when_allowlisted() {
        let request = PolicyRequest {
            principal: "user:bootstrap".to_owned(),
            action: "tool.execute".to_owned(),
            resource: "tool:palyra.echo".to_owned(),
        };
        let denied =
            evaluate_with_config(&request, &PolicyEvaluationConfig::default()).expect("evaluation");
        assert_eq!(
            denied.decision,
            PolicyDecision::DenyByDefault { reason: POLICY_DENY_REASON.to_owned() }
        );

        let allowed_config = PolicyEvaluationConfig {
            allowlisted_tools: vec!["palyra.echo".to_owned()],
            allow_sensitive_tools: false,
            sensitive_tool_names: Vec::new(),
            allowlisted_skills: Vec::new(),
        };
        let allowed = evaluate_with_config(&request, &allowed_config).expect("evaluation");
        assert_eq!(allowed.decision, PolicyDecision::Allow);
        assert!(allowed.explanation.is_allowlisted_tool);
    }

    #[test]
    fn skill_execute_is_allowed_only_when_allowlisted() {
        let request = PolicyRequest {
            principal: "user:bootstrap".to_owned(),
            action: "skill.execute".to_owned(),
            resource: "skill:acme.echo_http".to_owned(),
        };
        let denied =
            evaluate_with_config(&request, &PolicyEvaluationConfig::default()).expect("evaluation");
        assert_eq!(
            denied.decision,
            PolicyDecision::DenyByDefault { reason: SKILL_POLICY_DENY_REASON.to_owned() }
        );

        let allowed_config = PolicyEvaluationConfig {
            allowlisted_skills: vec!["acme.echo_http".to_owned()],
            allow_sensitive_tools: false,
            allowlisted_tools: Vec::new(),
            sensitive_tool_names: Vec::new(),
        };
        let allowed = evaluate_with_config(&request, &allowed_config).expect("evaluation");
        assert_eq!(allowed.decision, PolicyDecision::Allow);
        assert!(allowed.explanation.is_allowlisted_skill);
    }

    #[test]
    fn substring_collision_does_not_grant_read_only_access() {
        let request = PolicyRequest {
            principal: "user:bootstrap".to_owned(),
            action: "tool.target.reset".to_owned(),
            resource: "tool:filesystem".to_owned(),
        };

        let decision = evaluate(&request);

        assert!(matches!(decision, PolicyDecision::DenyByDefault { .. }));
    }

    #[test]
    fn mixed_scope_mutating_action_is_denied() {
        let request = PolicyRequest {
            principal: "user:bootstrap".to_owned(),
            action: "tool.read.write".to_owned(),
            resource: "tool:filesystem".to_owned(),
        };

        let decision = evaluate(&request);

        assert!(matches!(decision, PolicyDecision::DenyByDefault { .. }));
    }
}
