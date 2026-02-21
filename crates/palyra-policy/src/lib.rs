use std::{str::FromStr, sync::OnceLock};

use cedar_policy::{
    Authorizer, Context, Decision, Entities, Entity, EntityId, EntityTypeName, EntityUid,
    PolicySet, Request, Schema,
};
use serde_json::json;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyRequest {
    pub principal: String,
    pub action: String,
    pub resource: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PolicyRequestContext {
    pub device_id: Option<String>,
    pub channel: Option<String>,
    pub session_id: Option<String>,
    pub run_id: Option<String>,
    pub tool_name: Option<String>,
    pub skill_id: Option<String>,
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PolicyEvaluationConfig {
    pub allowlisted_tools: Vec<String>,
    pub allowlisted_skills: Vec<String>,
    pub allow_sensitive_tools: bool,
    pub sensitive_tool_names: Vec<String>,
    pub sensitive_actions: Vec<String>,
    pub sensitive_capability_names: Vec<String>,
    pub tool_execute_principal_allowlist: Vec<String>,
    pub tool_execute_channel_allowlist: Vec<String>,
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
    pub is_tool_execute_principal_allowed: bool,
    pub is_tool_execute_channel_allowed: bool,
    pub requested_tool: Option<String>,
    pub requested_skill: Option<String>,
    pub request_capabilities: Vec<String>,
    pub constructed_entities: Vec<String>,
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
    context.is_allowlisted_tool &&
    context.is_tool_execute_principal_allowed &&
    context.is_tool_execute_channel_allowed
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
const TOOL_EXECUTE_PRINCIPAL_DENY_REASON: &str =
    "tool execution denied by default: principal is not allowlisted for tool.execute";
const TOOL_EXECUTE_CHANNEL_DENY_REASON: &str =
    "tool execution denied by default: channel is not allowlisted for tool.execute";
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
    evaluate_with_context(request, &PolicyRequestContext::default(), config)
}

pub fn evaluate_with_context(
    request: &PolicyRequest,
    request_context: &PolicyRequestContext,
    config: &PolicyEvaluationConfig,
) -> Result<PolicyEvaluation, PolicyEngineError> {
    let normalized_action = request.action.to_ascii_lowercase();
    let normalized_request_context = normalize_request_context(request_context);
    let requested_tool = normalized_request_context
        .tool_name
        .clone()
        .or_else(|| requested_tool_name(normalized_action.as_str(), request.resource.as_str()));
    let requested_skill = normalized_request_context
        .skill_id
        .clone()
        .or_else(|| requested_skill_name(normalized_action.as_str(), request.resource.as_str()));
    let is_allowlisted_tool =
        is_allowlisted_tool(requested_tool.as_deref(), config.allowlisted_tools.as_slice());
    let is_allowlisted_skill =
        is_allowlisted_skill(requested_skill.as_deref(), config.allowlisted_skills.as_slice());
    let is_tool_execute_principal_allowed = is_tool_execute_principal_allowed(
        normalized_action.as_str(),
        request.principal.as_str(),
        config.tool_execute_principal_allowlist.as_slice(),
    );
    let is_tool_execute_channel_allowed = is_tool_execute_channel_allowed(
        normalized_action.as_str(),
        normalized_request_context.channel.as_deref(),
        config.tool_execute_channel_allowlist.as_slice(),
    );
    let is_sensitive_action = is_sensitive_action(
        normalized_action.as_str(),
        requested_tool.as_deref(),
        config.sensitive_tool_names.as_slice(),
        config.sensitive_actions.as_slice(),
        normalized_request_context.capabilities.as_slice(),
        config.sensitive_capability_names.as_slice(),
    );
    let principal_uid = principal_uid(request.principal.as_str())?;
    let action_uid = action_uid(normalized_action.as_str())?;
    let resource_uid = resource_uid(request.resource.as_str())?;
    let entities = build_request_entities(
        principal_uid.clone(),
        resource_uid.clone(),
        requested_tool.as_deref(),
        requested_skill.as_deref(),
        normalized_request_context.channel.as_deref(),
    );
    let entities = entities?;
    let mut constructed_entities =
        entities.iter().map(|entity| entity.uid().to_string()).collect::<Vec<_>>();
    constructed_entities.sort();

    let context = Context::from_json_value(
        json!({
            "action": normalized_action,
            "resource": request.resource,
            "principal": request.principal,
            "device_id": normalized_request_context.device_id.clone().unwrap_or_default(),
            "channel": normalized_request_context.channel.clone().unwrap_or_default(),
            "session_id": normalized_request_context.session_id.clone().unwrap_or_default(),
            "run_id": normalized_request_context.run_id.clone().unwrap_or_default(),
            "tool_name": requested_tool.clone().unwrap_or_default(),
            "skill_id": requested_skill.clone().unwrap_or_default(),
            "capabilities": normalized_request_context.capabilities.clone(),
            "is_sensitive_action": is_sensitive_action,
            "is_allowlisted_tool": is_allowlisted_tool,
            "is_allowlisted_skill": is_allowlisted_skill,
            "is_tool_execute_principal_allowed": is_tool_execute_principal_allowed,
            "is_tool_execute_channel_allowed": is_tool_execute_channel_allowed,
            "allow_sensitive_tools": config.allow_sensitive_tools,
        }),
        None,
    )
    .map_err(|error| PolicyEngineError::InvalidContext { message: error.to_string() })?;

    let cedar_request = Request::new(principal_uid, action_uid, resource_uid, context, None)
        .map_err(|error| PolicyEngineError::InvalidRequest { message: error.to_string() })?;

    let response =
        Authorizer::new().is_authorized(&cedar_request, default_policy_set()?, &entities);

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
        is_tool_execute_principal_allowed,
        is_tool_execute_channel_allowed,
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
            is_tool_execute_principal_allowed,
            is_tool_execute_channel_allowed,
            requested_tool: requested_tool.clone(),
            requested_skill: requested_skill.clone(),
            request_capabilities: normalized_request_context.capabilities,
            constructed_entities,
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

fn default_schema() -> Result<&'static Schema, PolicyEngineError> {
    static SCHEMA: OnceLock<Result<Schema, PolicyEngineError>> = OnceLock::new();
    match SCHEMA.get_or_init(|| {
        Schema::from_json_value(json!({
            "": {
                "entityTypes": {
                    "Principal": {},
                    "Resource": {},
                    "Tool": {},
                    "Skill": {},
                    "Channel": {},
                },
                "actions": {}
            }
        }))
        .map_err(|error| PolicyEngineError::EngineInitialization {
            message: format!("failed to parse Cedar schema: {error}"),
        })
    }) {
        Ok(schema) => Ok(schema),
        Err(error) => Err(error.clone()),
    }
}

fn principal_uid(principal: &str) -> Result<EntityUid, PolicyEngineError> {
    Ok(entity_uid(principal_entity_type_name()?, principal))
}

fn action_uid(action: &str) -> Result<EntityUid, PolicyEngineError> {
    Ok(entity_uid(action_entity_type_name()?, action))
}

fn resource_uid(resource: &str) -> Result<EntityUid, PolicyEngineError> {
    Ok(entity_uid(resource_entity_type_name()?, resource))
}

fn build_request_entities(
    principal_uid: EntityUid,
    resource_uid: EntityUid,
    requested_tool: Option<&str>,
    requested_skill: Option<&str>,
    channel: Option<&str>,
) -> Result<Entities, PolicyEngineError> {
    let mut entities = vec![Entity::with_uid(principal_uid)];

    if let Some(tool_name) = requested_tool {
        let tool_uid = entity_uid(tool_entity_type_name()?, tool_name);
        entities.push(Entity::with_uid(tool_uid));
    }
    if let Some(skill_id) = requested_skill {
        let skill_uid = entity_uid(skill_entity_type_name()?, skill_id);
        entities.push(Entity::with_uid(skill_uid));
    }
    if let Some(channel_name) = channel {
        let channel_uid = entity_uid(channel_entity_type_name()?, channel_name);
        entities.push(Entity::with_uid(channel_uid));
    }
    entities.push(Entity::with_uid(resource_uid));

    Entities::from_entities(entities, Some(default_schema()?)).map_err(|error| {
        PolicyEngineError::EngineInitialization {
            message: format!("failed to construct Cedar entities: {error}"),
        }
    })
}

#[allow(clippy::too_many_arguments)]
fn decision_reason(
    decision: Decision,
    normalized_action: &str,
    is_sensitive_action: bool,
    is_allowlisted_tool: bool,
    is_allowlisted_skill: bool,
    is_tool_execute_principal_allowed: bool,
    is_tool_execute_channel_allowed: bool,
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

    if normalized_action == "tool.execute" && !is_tool_execute_principal_allowed {
        return TOOL_EXECUTE_PRINCIPAL_DENY_REASON.to_owned();
    }
    if normalized_action == "tool.execute" && !is_tool_execute_channel_allowed {
        return TOOL_EXECUTE_CHANNEL_DENY_REASON.to_owned();
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

#[derive(Debug, Clone, Default)]
struct NormalizedPolicyRequestContext {
    device_id: Option<String>,
    channel: Option<String>,
    session_id: Option<String>,
    run_id: Option<String>,
    tool_name: Option<String>,
    skill_id: Option<String>,
    capabilities: Vec<String>,
}

fn normalize_request_context(
    request_context: &PolicyRequestContext,
) -> NormalizedPolicyRequestContext {
    let mut capabilities = request_context
        .capabilities
        .iter()
        .filter_map(|raw| {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_ascii_lowercase())
            }
        })
        .collect::<Vec<_>>();
    capabilities.sort();
    capabilities.dedup();
    NormalizedPolicyRequestContext {
        device_id: normalize_context_identifier(request_context.device_id.as_deref(), false),
        channel: normalize_context_identifier(request_context.channel.as_deref(), false),
        session_id: normalize_context_identifier(request_context.session_id.as_deref(), false),
        run_id: normalize_context_identifier(request_context.run_id.as_deref(), false),
        tool_name: normalize_context_identifier(request_context.tool_name.as_deref(), true),
        skill_id: normalize_context_identifier(request_context.skill_id.as_deref(), true),
        capabilities,
    }
}

fn normalize_context_identifier(value: Option<&str>, lowercase: bool) -> Option<String> {
    let value = value?.trim();
    if value.is_empty() {
        return None;
    }
    if lowercase {
        return Some(value.to_ascii_lowercase());
    }
    Some(value.to_owned())
}

fn parse_entity_type_name(raw: &str) -> Result<EntityTypeName, PolicyEngineError> {
    EntityTypeName::from_str(raw).map_err(|error| PolicyEngineError::EngineInitialization {
        message: format!("failed to parse Cedar entity type '{raw}': {error}"),
    })
}

fn principal_entity_type_name() -> Result<EntityTypeName, PolicyEngineError> {
    static ENTITY_TYPE: OnceLock<Result<EntityTypeName, PolicyEngineError>> = OnceLock::new();
    match ENTITY_TYPE.get_or_init(|| parse_entity_type_name("Principal")) {
        Ok(entity_type) => Ok(entity_type.clone()),
        Err(error) => Err(error.clone()),
    }
}

fn action_entity_type_name() -> Result<EntityTypeName, PolicyEngineError> {
    static ENTITY_TYPE: OnceLock<Result<EntityTypeName, PolicyEngineError>> = OnceLock::new();
    match ENTITY_TYPE.get_or_init(|| parse_entity_type_name("Action")) {
        Ok(entity_type) => Ok(entity_type.clone()),
        Err(error) => Err(error.clone()),
    }
}

fn resource_entity_type_name() -> Result<EntityTypeName, PolicyEngineError> {
    static ENTITY_TYPE: OnceLock<Result<EntityTypeName, PolicyEngineError>> = OnceLock::new();
    match ENTITY_TYPE.get_or_init(|| parse_entity_type_name("Resource")) {
        Ok(entity_type) => Ok(entity_type.clone()),
        Err(error) => Err(error.clone()),
    }
}

fn tool_entity_type_name() -> Result<EntityTypeName, PolicyEngineError> {
    static ENTITY_TYPE: OnceLock<Result<EntityTypeName, PolicyEngineError>> = OnceLock::new();
    match ENTITY_TYPE.get_or_init(|| parse_entity_type_name("Tool")) {
        Ok(entity_type) => Ok(entity_type.clone()),
        Err(error) => Err(error.clone()),
    }
}

fn skill_entity_type_name() -> Result<EntityTypeName, PolicyEngineError> {
    static ENTITY_TYPE: OnceLock<Result<EntityTypeName, PolicyEngineError>> = OnceLock::new();
    match ENTITY_TYPE.get_or_init(|| parse_entity_type_name("Skill")) {
        Ok(entity_type) => Ok(entity_type.clone()),
        Err(error) => Err(error.clone()),
    }
}

fn channel_entity_type_name() -> Result<EntityTypeName, PolicyEngineError> {
    static ENTITY_TYPE: OnceLock<Result<EntityTypeName, PolicyEngineError>> = OnceLock::new();
    match ENTITY_TYPE.get_or_init(|| parse_entity_type_name("Channel")) {
        Ok(entity_type) => Ok(entity_type.clone()),
        Err(error) => Err(error.clone()),
    }
}

fn entity_uid(entity_type: EntityTypeName, value: &str) -> EntityUid {
    EntityUid::from_type_name_and_id(entity_type, EntityId::new(value))
}

fn is_tool_execute_principal_allowed(
    normalized_action: &str,
    principal: &str,
    allowlisted_principals: &[String],
) -> bool {
    if normalized_action != "tool.execute" || allowlisted_principals.is_empty() {
        return true;
    }
    allowlisted_principals.iter().any(|allowlisted| allowlisted.eq_ignore_ascii_case(principal))
}

fn is_tool_execute_channel_allowed(
    normalized_action: &str,
    channel: Option<&str>,
    allowlisted_channels: &[String],
) -> bool {
    if normalized_action != "tool.execute" || allowlisted_channels.is_empty() {
        return true;
    }
    let Some(channel) = channel else {
        return false;
    };
    allowlisted_channels.iter().any(|allowlisted| allowlisted.eq_ignore_ascii_case(channel))
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
    sensitive_tool_names: &[String],
    sensitive_actions: &[String],
    requested_capabilities: &[String],
    sensitive_capability_names: &[String],
) -> bool {
    if is_sensitive_action_name(normalized_action, sensitive_actions) {
        return true;
    }
    if has_sensitive_capability(requested_capabilities, sensitive_capability_names) {
        return true;
    }
    if normalized_action == "tool.execute" {
        return is_sensitive_tool_name(requested_tool, sensitive_tool_names);
    }
    false
}

fn is_sensitive_tool_name(requested_tool: Option<&str>, sensitive_tool_names: &[String]) -> bool {
    let Some(requested_tool) = requested_tool else {
        return false;
    };
    sensitive_tool_names
        .iter()
        .any(|sensitive_tool| sensitive_tool.eq_ignore_ascii_case(requested_tool))
}

fn is_sensitive_action_name(normalized_action: &str, sensitive_actions: &[String]) -> bool {
    sensitive_actions
        .iter()
        .any(|sensitive_action| sensitive_action.eq_ignore_ascii_case(normalized_action))
}

fn has_sensitive_capability(
    requested_capabilities: &[String],
    sensitive_capability_names: &[String],
) -> bool {
    if requested_capabilities.is_empty() || sensitive_capability_names.is_empty() {
        return false;
    }
    requested_capabilities.iter().any(|capability| {
        sensitive_capability_names
            .iter()
            .any(|sensitive| sensitive.eq_ignore_ascii_case(capability))
    })
}

#[cfg(test)]
mod tests {
    use super::{
        evaluate, evaluate_with_config, evaluate_with_context, PolicyDecision,
        PolicyEvaluationConfig, PolicyRequest, PolicyRequestContext, BASELINE_DENY_REASON,
        POLICY_DENY_REASON, SENSITIVE_DENY_REASON, SKILL_POLICY_DENY_REASON,
        TOOL_EXECUTE_CHANNEL_DENY_REASON, TOOL_EXECUTE_PRINCIPAL_DENY_REASON,
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
            allow_sensitive_tools: false,
            sensitive_tool_names: vec!["palyra.process.run".to_owned()],
            ..PolicyEvaluationConfig::default()
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
    fn cron_delete_action_is_not_implicitly_sensitive() {
        let request = PolicyRequest {
            principal: "user:ops".to_owned(),
            action: "cron.delete".to_owned(),
            resource: "cron:job".to_owned(),
        };

        let evaluation =
            evaluate_with_config(&request, &PolicyEvaluationConfig::default()).expect("evaluation");

        assert_eq!(evaluation.decision, PolicyDecision::Allow);
        assert!(
            !evaluation.explanation.is_sensitive_action,
            "cron delete should not be marked sensitive without explicit configuration"
        );
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
    fn explicit_sensitive_actions_require_explicit_approval() {
        let request = PolicyRequest {
            principal: "user:ops".to_owned(),
            action: "vault.delete".to_owned(),
            resource: "secrets:global:openai_api_key".to_owned(),
        };
        let config = PolicyEvaluationConfig {
            sensitive_actions: vec!["vault.delete".to_owned()],
            ..PolicyEvaluationConfig::default()
        };

        let evaluation = evaluate_with_config(&request, &config).expect("evaluation");

        assert_eq!(
            evaluation.decision,
            PolicyDecision::DenyByDefault { reason: SENSITIVE_DENY_REASON.to_owned() }
        );
        assert!(evaluation.explanation.is_sensitive_action);
    }

    #[test]
    fn unknown_delete_actions_are_not_keyword_sensitive_by_default() {
        let request = PolicyRequest {
            principal: "user:ops".to_owned(),
            action: "custom.delete".to_owned(),
            resource: "custom:resource".to_owned(),
        };

        let evaluation =
            evaluate_with_config(&request, &PolicyEvaluationConfig::default()).expect("evaluation");

        assert_eq!(
            evaluation.decision,
            PolicyDecision::DenyByDefault { reason: BASELINE_DENY_REASON.to_owned() }
        );
        assert!(
            !evaluation.explanation.is_sensitive_action,
            "delete keyword should not auto-classify unknown actions as sensitive"
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
            ..PolicyEvaluationConfig::default()
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
            sensitive_tool_names: Vec::new(),
            ..PolicyEvaluationConfig::default()
        };
        let allowed = evaluate_with_config(&request, &allowed_config).expect("evaluation");
        assert_eq!(allowed.decision, PolicyDecision::Allow);
        assert!(allowed.explanation.is_allowlisted_skill);
    }

    #[test]
    fn tool_execute_principal_allowlist_denies_mismatched_principal() {
        let request = PolicyRequest {
            principal: "user:finance".to_owned(),
            action: "tool.execute".to_owned(),
            resource: "tool:palyra.echo".to_owned(),
        };
        let config = PolicyEvaluationConfig {
            allowlisted_tools: vec!["palyra.echo".to_owned()],
            tool_execute_principal_allowlist: vec!["user:ops".to_owned()],
            ..PolicyEvaluationConfig::default()
        };

        let evaluation = evaluate_with_context(&request, &PolicyRequestContext::default(), &config)
            .expect("evaluation");
        assert_eq!(
            evaluation.decision,
            PolicyDecision::DenyByDefault { reason: TOOL_EXECUTE_PRINCIPAL_DENY_REASON.to_owned() }
        );
        assert!(!evaluation.explanation.is_tool_execute_principal_allowed);
    }

    #[test]
    fn tool_execute_channel_allowlist_requires_matching_context_channel() {
        let request = PolicyRequest {
            principal: "user:ops".to_owned(),
            action: "tool.execute".to_owned(),
            resource: "tool:palyra.echo".to_owned(),
        };
        let config = PolicyEvaluationConfig {
            allowlisted_tools: vec!["palyra.echo".to_owned()],
            tool_execute_channel_allowlist: vec!["cli".to_owned()],
            ..PolicyEvaluationConfig::default()
        };
        let denied = evaluate_with_context(&request, &PolicyRequestContext::default(), &config)
            .expect("evaluation");
        assert_eq!(
            denied.decision,
            PolicyDecision::DenyByDefault { reason: TOOL_EXECUTE_CHANNEL_DENY_REASON.to_owned() }
        );
        let allowed = evaluate_with_context(
            &request,
            &PolicyRequestContext {
                channel: Some("cli".to_owned()),
                ..PolicyRequestContext::default()
            },
            &config,
        )
        .expect("evaluation");
        assert_eq!(allowed.decision, PolicyDecision::Allow);
        assert!(allowed.explanation.is_tool_execute_channel_allowed);
    }

    #[test]
    fn sensitive_capabilities_require_explicit_approval() {
        let request = PolicyRequest {
            principal: "user:ops".to_owned(),
            action: "tool.execute".to_owned(),
            resource: "tool:palyra.echo".to_owned(),
        };
        let context = PolicyRequestContext {
            capabilities: vec!["network".to_owned()],
            ..PolicyRequestContext::default()
        };
        let config = PolicyEvaluationConfig {
            allowlisted_tools: vec!["palyra.echo".to_owned()],
            sensitive_capability_names: vec!["network".to_owned()],
            allow_sensitive_tools: false,
            ..PolicyEvaluationConfig::default()
        };
        let denied = evaluate_with_context(&request, &context, &config).expect("evaluation");
        assert_eq!(
            denied.decision,
            PolicyDecision::DenyByDefault { reason: SENSITIVE_DENY_REASON.to_owned() }
        );

        let allowed = evaluate_with_context(
            &request,
            &context,
            &PolicyEvaluationConfig { allow_sensitive_tools: true, ..config },
        )
        .expect("evaluation");
        assert_eq!(allowed.decision, PolicyDecision::Allow);
    }

    #[test]
    fn context_entities_include_principal_tool_skill_and_channel() {
        let request = PolicyRequest {
            principal: "user:ops".to_owned(),
            action: "tool.execute".to_owned(),
            resource: "tool:palyra.echo".to_owned(),
        };
        let evaluation = evaluate_with_context(
            &request,
            &PolicyRequestContext {
                channel: Some("cli".to_owned()),
                tool_name: Some("palyra.echo".to_owned()),
                skill_id: Some("acme.echo_http".to_owned()),
                ..PolicyRequestContext::default()
            },
            &PolicyEvaluationConfig {
                allowlisted_tools: vec!["palyra.echo".to_owned()],
                ..PolicyEvaluationConfig::default()
            },
        )
        .expect("evaluation");

        assert_eq!(evaluation.decision, PolicyDecision::Allow);
        assert!(
            evaluation
                .explanation
                .constructed_entities
                .iter()
                .any(|uid| uid.starts_with("Principal::")),
            "principal entity should be present in constructed Cedar entities"
        );
        assert!(
            evaluation.explanation.constructed_entities.iter().any(|uid| uid.starts_with("Tool::")),
            "tool entity should be present in constructed Cedar entities"
        );
        assert!(
            evaluation
                .explanation
                .constructed_entities
                .iter()
                .any(|uid| uid.starts_with("Skill::")),
            "skill entity should be present in constructed Cedar entities"
        );
        assert!(
            evaluation
                .explanation
                .constructed_entities
                .iter()
                .any(|uid| uid.starts_with("Channel::")),
            "channel entity should be present in constructed Cedar entities"
        );
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
