//! Deny-by-default policy evaluation core built on Cedar.
//!
//! Every authorization request is denied unless an explicit Cedar permit in the embedded
//! baseline policy matches, and sensitive actions additionally require an explicit approval
//! flag. Consumed by the daemon (gateway, tool security, service authorization), the CLI, and
//! the skills runtime; all callers must treat any engine error as a deny (fail closed).

use std::{str::FromStr, sync::OnceLock};

use cedar_policy::{
    Authorizer, Context, Decision, Entities, Entity, EntityId, EntityTypeName, EntityUid,
    PolicySet, Request, Schema,
};
use serde_json::json;
use thiserror::Error;

/// An authorization request: which principal wants to perform which action on which resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyRequest {
    /// Requesting identity, e.g. `user:ops`.
    pub principal: String,
    /// Dotted action name, e.g. `tool.execute`; matched case-insensitively.
    pub action: String,
    /// Target resource identifier, e.g. `tool:filesystem`.
    pub resource: String,
}

/// Optional caller-supplied context attached to a [`PolicyRequest`].
///
/// All identifiers are trimmed before evaluation; empty values are treated as absent.
/// `tool_name` and `skill_id`, when present, take precedence over names derived from the
/// request resource.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PolicyRequestContext {
    /// Device the request originated from.
    pub device_id: Option<String>,
    /// Originating channel; matched against the `tool.execute` channel allowlist.
    pub channel: Option<String>,
    /// Session correlation identifier.
    pub session_id: Option<String>,
    /// Run correlation identifier.
    pub run_id: Option<String>,
    /// Explicit tool name; overrides the tool name parsed from the resource.
    pub tool_name: Option<String>,
    /// Explicit skill identifier; overrides the skill name parsed from the resource.
    pub skill_id: Option<String>,
    /// Capabilities requested for this call; matched against sensitive capability names.
    pub capabilities: Vec<String>,
}

/// Allowlists and approval flags applied during policy evaluation.
///
/// The default configuration allowlists nothing and marks `cron.delete` and `memory.purge`
/// as sensitive, so `tool.execute` and `skill.execute` are denied until explicitly granted.
/// All name matching is ASCII case-insensitive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyEvaluationConfig {
    /// Tools permitted for `tool.execute`.
    pub allowlisted_tools: Vec<String>,
    /// Skills permitted for `skill.execute`.
    pub allowlisted_skills: Vec<String>,
    /// Explicit approval flag that unlocks sensitive actions, tools, and capabilities.
    pub allow_sensitive_tools: bool,
    /// Tool names whose execution counts as a sensitive action.
    pub sensitive_tool_names: Vec<String>,
    /// Action names that always count as sensitive.
    pub sensitive_actions: Vec<String>,
    /// Requested capabilities that mark the request as sensitive.
    pub sensitive_capability_names: Vec<String>,
    /// Principals permitted to run `tool.execute`; empty disables this gate.
    pub tool_execute_principal_allowlist: Vec<String>,
    /// Channels permitted to run `tool.execute`; empty disables this gate.
    pub tool_execute_channel_allowlist: Vec<String>,
}

const DEFAULT_SENSITIVE_ACTIONS: &[&str] = &["cron.delete", "memory.purge"];

impl Default for PolicyEvaluationConfig {
    fn default() -> Self {
        Self {
            allowlisted_tools: Vec::new(),
            allowlisted_skills: Vec::new(),
            allow_sensitive_tools: false,
            sensitive_tool_names: Vec::new(),
            sensitive_actions: DEFAULT_SENSITIVE_ACTIONS
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
            sensitive_capability_names: Vec::new(),
            tool_execute_principal_allowlist: Vec::new(),
            tool_execute_channel_allowlist: Vec::new(),
        }
    }
}

/// Final outcome of a policy evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyDecision {
    /// An explicit Cedar permit matched and no forbid overrode it.
    Allow,
    /// No permit matched, or a forbid fired; `reason` is a redaction-safe summary.
    DenyByDefault { reason: String },
}

/// Detailed account of how a [`PolicyDecision`] was reached.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyExplanation {
    /// Always `true` for decisions produced by this engine; distinguishes Cedar-backed
    /// evaluations from decisions synthesized elsewhere.
    pub evaluated_with_cedar: bool,
    /// Human-readable summary mirroring the deny reason or describing the matched permit.
    pub reason: String,
    /// Identifiers of the Cedar policies that determined the decision, sorted.
    pub matched_policy_ids: Vec<String>,
    /// Cedar evaluation diagnostics; any entry forces a deny.
    pub diagnostics_errors: Vec<String>,
    /// Whether the request matched a sensitive action, tool, or capability.
    pub is_sensitive_action: bool,
    /// Whether the requested tool was on the tool allowlist.
    pub is_allowlisted_tool: bool,
    /// Whether the requested skill was on the skill allowlist.
    pub is_allowlisted_skill: bool,
    /// Whether the principal passed the `tool.execute` principal gate.
    pub is_tool_execute_principal_allowed: bool,
    /// Whether the channel passed the `tool.execute` channel gate.
    pub is_tool_execute_channel_allowed: bool,
    /// Normalized tool name resolved from context or resource, if any.
    pub requested_tool: Option<String>,
    /// Normalized skill identifier resolved from context or resource, if any.
    pub requested_skill: Option<String>,
    /// Normalized (trimmed, lowercased, deduplicated) capabilities from the request context.
    pub request_capabilities: Vec<String>,
    /// UIDs of the Cedar entities constructed for the evaluation, sorted.
    pub constructed_entities: Vec<String>,
}

/// A [`PolicyDecision`] together with its [`PolicyExplanation`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyEvaluation {
    /// The allow/deny outcome.
    pub decision: PolicyDecision,
    /// How the outcome was reached.
    pub explanation: PolicyExplanation,
}

/// Failure to set up or run a Cedar evaluation; callers must treat it as a deny.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum PolicyEngineError {
    /// The embedded policy set, schema, or an entity could not be constructed.
    #[error("failed to initialize Cedar policy engine: {message}")]
    EngineInitialization { message: String },
    /// The request context could not be converted into a Cedar context.
    #[error("failed to build Cedar request context: {message}")]
    InvalidContext { message: String },
    /// The principal/action/resource tuple was rejected by Cedar.
    #[error("failed to construct Cedar authorization request: {message}")]
    InvalidRequest { message: String },
}

// Baseline Cedar policy: Cedar denies unless a permit matches, so any action absent from this
// list is denied by default, and the single forbid overrides every permit for sensitive
// actions without explicit approval.
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

@id("allow_message_router_actions")
permit(principal, action, resource)
when {
    context.action == "message.reply" ||
    context.action == "message.broadcast" ||
    context.action == "channel.send" ||
    context.action == "channel.message.read" ||
    context.action == "channel.message.search" ||
    context.action == "channel.message.edit" ||
    context.action == "channel.message.delete" ||
    context.action == "channel.message.react_add" ||
    context.action == "channel.message.react_remove" ||
    context.action == "channel.command.status" ||
    context.action == "channel.command.stop" ||
    context.action == "channel.command.reset" ||
    context.action == "channel.command.compact" ||
    context.action == "channel.command.approve" ||
    context.action == "channel.command.queue" ||
    context.action == "channel.command.routine.status" ||
    context.action == "channel.command.routine.pause" ||
    context.action == "channel.command.routine.resume" ||
    context.action == "channel.command.routine.run_now" ||
    context.action == "channel.command.routine.cancel" ||
    context.action == "channel.command.routine.history" ||
    context.action == "channel.command.whoami"
};

@id("allow_attachment_metadata_actions")
permit(principal, action, resource)
when {
    context.action == "attachment.metadata.accept"
};
"#;

// INTENTIONAL: these reason strings are mapped to stable reason codes in
// `policy_reason_code` and may be pinned by golden fixtures in consuming crates —
// keep them byte-identical when editing.
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

/// Evaluates `request` against the default configuration, failing closed.
///
/// Any [`PolicyEngineError`] is converted into [`PolicyDecision::DenyByDefault`] so an engine
/// failure can never be observed as an allow.
#[must_use]
pub fn evaluate(request: &PolicyRequest) -> PolicyDecision {
    match evaluate_with_config(request, &PolicyEvaluationConfig::default()) {
        Ok(evaluation) => evaluation.decision,
        Err(error) => PolicyDecision::DenyByDefault {
            reason: format!("policy evaluation failed safely: {error}"),
        },
    }
}

/// Evaluates `request` against `config` with an empty request context.
///
/// # Errors
/// Returns [`PolicyEngineError`] when the Cedar policy set, schema, entities, context, or
/// request cannot be constructed. Callers must treat any error as a deny.
pub fn evaluate_with_config(
    request: &PolicyRequest,
    config: &PolicyEvaluationConfig,
) -> Result<PolicyEvaluation, PolicyEngineError> {
    evaluate_with_context(request, &PolicyRequestContext::default(), config)
}

/// Evaluates `request` with full caller context against `config`.
///
/// The action is lowercased and the context identifiers are normalized before the allowlist
/// and sensitivity gates are computed; the gate results are then passed into Cedar as context
/// attributes for the baseline policy to act on.
///
/// # Errors
/// Returns [`PolicyEngineError`] when the Cedar policy set, schema, entities, context, or
/// request cannot be constructed. Callers must treat any error as a deny.
pub fn evaluate_with_context(
    request: &PolicyRequest,
    request_context: &PolicyRequestContext,
    config: &PolicyEvaluationConfig,
) -> Result<PolicyEvaluation, PolicyEngineError> {
    let normalized_action = request.action.to_ascii_lowercase();
    let normalized_request_context = normalize_request_context(request_context);
    // AIDEV-NOTE: a context-supplied tool_name/skill_id overrides the name parsed from the
    // resource, and the allowlist and sensitivity gates trust it. Callers are trusted daemon
    // code and must keep the context consistent with the resource they execute.
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
    )?;
    let mut constructed_entities =
        entities.iter().map(|entity| entity.uid().to_string()).collect::<Vec<_>>();
    constructed_entities.sort();
    let context = Context::from_json_value(
        json!({
            "action": normalized_action,
            "resource": request.resource,
            "principal": request.principal,
            "device_id": normalized_request_context.device_id.as_deref().unwrap_or_default(),
            "channel": normalized_request_context.channel.as_deref().unwrap_or_default(),
            "session_id": normalized_request_context.session_id.as_deref().unwrap_or_default(),
            "run_id": normalized_request_context.run_id.as_deref().unwrap_or_default(),
            "tool_name": requested_tool.as_deref().unwrap_or_default(),
            "skill_id": requested_skill.as_deref().unwrap_or_default(),
            "capabilities": &normalized_request_context.capabilities,
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
    let request_capabilities = normalized_request_context.capabilities;

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
            requested_tool,
            requested_skill,
            request_capabilities,
            constructed_entities,
        },
    })
}

/// Builds a redaction-safe JSON diagnostics value for a completed evaluation.
///
/// Raw principal and resource identifiers are reduced to coarse classes and the internal
/// policy source is never included, so the value is safe to log or surface to operators.
#[must_use]
pub fn policy_explain_diagnostics_value(
    request: &PolicyRequest,
    evaluation: &PolicyEvaluation,
) -> serde_json::Value {
    let decision = match evaluation.decision {
        PolicyDecision::Allow => "allow",
        PolicyDecision::DenyByDefault { .. } => "deny_by_default",
    };
    let reason_code = policy_reason_code(evaluation.explanation.reason.as_str());
    json!({
        "schema_version": 1,
        "decision": decision,
        "action": request.action,
        "principal_class": identifier_class(request.principal.as_str()),
        "resource_class": identifier_class(request.resource.as_str()),
        "action_class": action_class(request.action.as_str()),
        "reason_code": reason_code,
        "missing_grant_hints": missing_grant_hints(evaluation),
        "redaction": {
            "raw_principal_included": false,
            "raw_resource_included": false,
            "internal_policy_source_included": false,
        },
        "matched": {
            "reason_count": evaluation.explanation.matched_policy_ids.len(),
            "diagnostic_error_count": evaluation.explanation.diagnostics_errors.len(),
        }
    })
}

// This and the cached statics below store a `Result` (instead of using `LazyLock`) so that a
// broken embedded policy or schema surfaces as a deny-producing error on every call rather
// than a panic.
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

#[expect(
    clippy::too_many_arguments,
    reason = "flags are individually computed booleans; grouping them adds no clarity"
)]
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
    // INTENTIONAL: allow reasons must keep the "allowed by Cedar" phrasing —
    // `policy_reason_code` matches on it to emit the stable "policy.allow" code.
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
        if normalized_action.starts_with("message.")
            || normalized_action == "channel.send"
            || normalized_action.starts_with("channel.message.")
            || normalized_action.starts_with("channel.command.")
        {
            return "message router action allowed by Cedar policy".to_owned();
        }
        if normalized_action == "attachment.metadata.accept" {
            return "attachment metadata action allowed by Cedar policy".to_owned();
        }
        return "read-only action allowed by Cedar baseline policy".to_owned();
    }

    if let Some(first_error) = diagnostics_errors.first() {
        return format!("policy evaluation diagnostics triggered deny-by-default: {first_error}");
    }

    // Deny reasons follow gate precedence (diagnostics, principal, channel, sensitivity,
    // allowlists, baseline); only the first failing gate is reported even when several
    // failed, and consumers rely on that ordering for stable reason codes.
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

fn policy_reason_code(reason: &str) -> &'static str {
    if reason.contains("allowed by Cedar policy") || reason.contains("allowed by Cedar baseline") {
        return "policy.allow";
    }
    if reason == SENSITIVE_DENY_REASON {
        return "policy.explicit_approval_required";
    }
    if reason == TOOL_EXECUTE_PRINCIPAL_DENY_REASON {
        return "tool.execute.principal_not_allowlisted";
    }
    if reason == TOOL_EXECUTE_CHANNEL_DENY_REASON {
        return "tool.execute.channel_not_allowlisted";
    }
    if reason == POLICY_DENY_REASON {
        return "tool.execute.tool_not_allowlisted";
    }
    if reason == SKILL_POLICY_DENY_REASON {
        return "skill.execute.skill_not_allowlisted";
    }
    if reason == BASELINE_DENY_REASON {
        return "policy.baseline_deny";
    }
    if reason.contains("policy evaluation diagnostics") || reason.contains("failed safely") {
        return "policy.deny.evaluation_error";
    }
    "policy.deny.unspecified"
}

fn identifier_class(identifier: &str) -> String {
    let trimmed = identifier.trim();
    if trimmed.is_empty() {
        return "unknown".to_owned();
    }
    let has_scope = trimmed.contains(':');
    let class = trimmed
        .split_once(':')
        .map(|(class, _)| class)
        .unwrap_or(trimmed)
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || *ch == '_' || *ch == '-')
        .collect::<String>()
        .to_ascii_lowercase();
    if class.is_empty() {
        return "unknown".to_owned();
    }
    if has_scope {
        format!("{class}:*")
    } else {
        class
    }
}

fn action_class(action: &str) -> String {
    let trimmed = action.trim();
    if trimmed.is_empty() {
        return "unknown".to_owned();
    }
    trimmed
        .split_once('.')
        .map(|(class, _)| class)
        .unwrap_or(trimmed)
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || *ch == '_' || *ch == '-')
        .collect::<String>()
        .to_ascii_lowercase()
}

fn missing_grant_hints(evaluation: &PolicyEvaluation) -> Vec<&'static str> {
    let mut hints = Vec::new();
    let explanation = &evaluation.explanation;
    if matches!(evaluation.decision, PolicyDecision::Allow) {
        return hints;
    }
    if explanation.is_sensitive_action {
        hints.push("request_explicit_approval");
    }
    if explanation.requested_tool.is_some() && !explanation.is_allowlisted_tool {
        hints.push("grant_tool_allowlist");
    }
    if explanation.requested_skill.is_some() && !explanation.is_allowlisted_skill {
        hints.push("enable_or_allowlist_skill");
    }
    if !explanation.is_tool_execute_principal_allowed {
        hints.push("add_principal_to_tool_execute_allowlist");
    }
    if !explanation.is_tool_execute_channel_allowed {
        hints.push("add_channel_to_tool_execute_allowlist");
    }
    if hints.is_empty() {
        hints.push("review_action_resource_scope");
    }
    hints
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
    // Tool and skill names are case-insensitive identifiers, so they are lowercased; the
    // remaining identifiers keep their casing because they are opaque correlation values.
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
    // An empty allowlist disables this gate rather than denying every principal;
    // tool.execute is still gated by the tool allowlist itself.
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
    // An empty allowlist disables this gate; once configured, a request without a channel in
    // its context fails the gate (fail closed).
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

        let evaluation = evaluate_with_config(&request, &config)
            .expect("well-formed request evaluates without engine error");

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

        let evaluation = evaluate_with_config(&request, &config)
            .expect("well-formed request evaluates without engine error");

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

        let evaluation = evaluate_with_config(&request, &PolicyEvaluationConfig::default())
            .expect("well-formed request evaluates without engine error");

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

        let evaluation = evaluate_with_config(&request, &PolicyEvaluationConfig::default())
            .expect("well-formed request evaluates without engine error");

        assert_eq!(evaluation.decision, PolicyDecision::Allow);
        assert!(!evaluation.explanation.matched_policy_ids.is_empty());
    }

    #[test]
    fn cron_delete_action_requires_explicit_approval_by_default() {
        let request = PolicyRequest {
            principal: "user:ops".to_owned(),
            action: "cron.delete".to_owned(),
            resource: "cron:job".to_owned(),
        };

        let evaluation = evaluate_with_config(&request, &PolicyEvaluationConfig::default())
            .expect("well-formed request evaluates without engine error");

        assert_eq!(
            evaluation.decision,
            PolicyDecision::DenyByDefault { reason: SENSITIVE_DENY_REASON.to_owned() }
        );
        assert!(
            evaluation.explanation.is_sensitive_action,
            "cron delete should be marked sensitive by the default policy configuration"
        );
    }

    #[test]
    fn memory_actions_are_explicitly_allowed() {
        let request = PolicyRequest {
            principal: "user:ops".to_owned(),
            action: "memory.search".to_owned(),
            resource: "memory:session".to_owned(),
        };

        let evaluation = evaluate_with_config(&request, &PolicyEvaluationConfig::default())
            .expect("well-formed request evaluates without engine error");

        assert_eq!(evaluation.decision, PolicyDecision::Allow);
        assert!(
            evaluation.explanation.reason.contains("memory action allowed"),
            "memory allow reason should reflect dedicated memory policy"
        );
    }

    #[test]
    fn memory_delete_is_allowed_by_default() {
        let request = PolicyRequest {
            principal: "user:ops".to_owned(),
            action: "memory.delete".to_owned(),
            resource: "memory:item".to_owned(),
        };

        let evaluation = evaluate_with_config(&request, &PolicyEvaluationConfig::default())
            .expect("well-formed request evaluates without engine error");

        assert_eq!(evaluation.decision, PolicyDecision::Allow);
        assert!(
            !evaluation.explanation.is_sensitive_action,
            "single-item memory delete should remain a scoped cleanup action by default"
        );
    }

    #[test]
    fn memory_purge_requires_explicit_approval_by_default() {
        let request = PolicyRequest {
            principal: "user:ops".to_owned(),
            action: "memory.purge".to_owned(),
            resource: "memory:session".to_owned(),
        };

        let evaluation = evaluate_with_config(&request, &PolicyEvaluationConfig::default())
            .expect("well-formed request evaluates without engine error");

        assert_eq!(
            evaluation.decision,
            PolicyDecision::DenyByDefault { reason: SENSITIVE_DENY_REASON.to_owned() }
        );
        assert!(
            evaluation.explanation.is_sensitive_action,
            "memory purge should be marked sensitive by the default policy configuration"
        );
    }

    #[test]
    fn vault_actions_are_explicitly_allowed() {
        let request = PolicyRequest {
            principal: "user:ops".to_owned(),
            action: "vault.get".to_owned(),
            resource: "secrets:global:openai_api_key".to_owned(),
        };

        let evaluation = evaluate_with_config(&request, &PolicyEvaluationConfig::default())
            .expect("well-formed request evaluates without engine error");

        assert_eq!(evaluation.decision, PolicyDecision::Allow);
        assert!(
            evaluation.explanation.reason.contains("vault action allowed"),
            "vault allow reason should reflect dedicated vault policy"
        );
    }

    #[test]
    fn message_router_actions_are_explicitly_allowed() {
        let request = PolicyRequest {
            principal: "user:ops".to_owned(),
            action: "message.reply".to_owned(),
            resource: "channel:slack".to_owned(),
        };

        let evaluation = evaluate_with_config(&request, &PolicyEvaluationConfig::default())
            .expect("well-formed request evaluates without engine error");

        assert_eq!(evaluation.decision, PolicyDecision::Allow);
        assert!(
            evaluation.explanation.reason.contains("message router action allowed"),
            "message action allow reason should reflect dedicated message policy"
        );
    }

    #[test]
    fn channel_message_read_action_is_explicitly_allowed() {
        let request = PolicyRequest {
            principal: "user:ops".to_owned(),
            action: "channel.message.read".to_owned(),
            resource: "channel:discord:default:message:123:456".to_owned(),
        };

        let evaluation = evaluate_with_config(&request, &PolicyEvaluationConfig::default())
            .expect("well-formed request evaluates without engine error");

        assert_eq!(evaluation.decision, PolicyDecision::Allow);
        assert!(
            evaluation.explanation.reason.contains("message router action allowed"),
            "channel message read should use the dedicated message policy allow reason"
        );
    }

    #[test]
    fn channel_message_delete_can_be_marked_sensitive() {
        let request = PolicyRequest {
            principal: "user:ops".to_owned(),
            action: "channel.message.delete".to_owned(),
            resource: "channel:discord:default:message:123:456".to_owned(),
        };
        let config = PolicyEvaluationConfig {
            sensitive_actions: vec!["channel.message.delete".to_owned()],
            ..PolicyEvaluationConfig::default()
        };

        let evaluation = evaluate_with_config(&request, &config)
            .expect("well-formed request evaluates without engine error");

        assert_eq!(
            evaluation.decision,
            PolicyDecision::DenyByDefault { reason: SENSITIVE_DENY_REASON.to_owned() }
        );
        assert!(evaluation.explanation.is_sensitive_action);
    }

    #[test]
    fn channel_send_action_is_explicitly_allowed() {
        let request = PolicyRequest {
            principal: "user:ops".to_owned(),
            action: "channel.send".to_owned(),
            resource: "channel:slack".to_owned(),
        };

        let evaluation = evaluate_with_config(&request, &PolicyEvaluationConfig::default())
            .expect("well-formed request evaluates without engine error");

        assert_eq!(evaluation.decision, PolicyDecision::Allow);
    }

    #[test]
    fn channel_command_action_is_explicitly_allowed() {
        let request = PolicyRequest {
            principal: "user:ops".to_owned(),
            action: "channel.command.status".to_owned(),
            resource: "channel:discord:default".to_owned(),
        };

        let evaluation = evaluate_with_config(&request, &PolicyEvaluationConfig::default())
            .expect("well-formed request evaluates without engine error");

        assert_eq!(evaluation.decision, PolicyDecision::Allow);
        assert!(
            evaluation.explanation.reason.contains("message router action allowed"),
            "channel commands should share the message router policy surface"
        );
    }

    #[test]
    fn routine_channel_command_actions_are_explicitly_allowed() {
        for action in [
            "channel.command.routine.status",
            "channel.command.routine.pause",
            "channel.command.routine.resume",
            "channel.command.routine.run_now",
            "channel.command.routine.cancel",
            "channel.command.routine.history",
        ] {
            let request = PolicyRequest {
                principal: "user:ops".to_owned(),
                action: action.to_owned(),
                resource: "channel:discord:default".to_owned(),
            };

            let evaluation = evaluate_with_config(&request, &PolicyEvaluationConfig::default())
                .expect("well-formed request evaluates without engine error");

            assert_eq!(evaluation.decision, PolicyDecision::Allow, "{action} should be allowed");
            assert!(
                evaluation.explanation.reason.contains("message router action allowed"),
                "routine channel commands should share the message router policy surface"
            );
        }
    }

    #[test]
    fn attachment_metadata_accept_is_explicitly_allowed() {
        let request = PolicyRequest {
            principal: "user:ops".to_owned(),
            action: "attachment.metadata.accept".to_owned(),
            resource: "channel:discord:default".to_owned(),
        };

        let evaluation = evaluate_with_config(&request, &PolicyEvaluationConfig::default())
            .expect("well-formed request evaluates without engine error");

        assert_eq!(evaluation.decision, PolicyDecision::Allow);
        assert!(
            evaluation.explanation.reason.contains("attachment metadata action allowed"),
            "attachment metadata accept should use the dedicated Cedar allow reason"
        );
    }

    #[test]
    fn attachment_download_is_denied_by_default() {
        let request = PolicyRequest {
            principal: "user:ops".to_owned(),
            action: "attachment.download".to_owned(),
            resource: "channel:discord:default".to_owned(),
        };

        let evaluation = evaluate_with_config(&request, &PolicyEvaluationConfig::default())
            .expect("well-formed request evaluates without engine error");

        assert_eq!(
            evaluation.decision,
            PolicyDecision::DenyByDefault { reason: BASELINE_DENY_REASON.to_owned() }
        );
    }

    #[test]
    fn attachment_vision_is_denied_by_default() {
        let request = PolicyRequest {
            principal: "user:ops".to_owned(),
            action: "attachment.vision".to_owned(),
            resource: "channel:discord:default".to_owned(),
        };

        let evaluation = evaluate_with_config(&request, &PolicyEvaluationConfig::default())
            .expect("well-formed request evaluates without engine error");

        assert_eq!(
            evaluation.decision,
            PolicyDecision::DenyByDefault { reason: BASELINE_DENY_REASON.to_owned() }
        );
    }

    #[test]
    fn attachment_upload_is_denied_by_default() {
        let request = PolicyRequest {
            principal: "user:ops".to_owned(),
            action: "attachment.upload".to_owned(),
            resource: "channel:discord:default".to_owned(),
        };

        let evaluation = evaluate_with_config(&request, &PolicyEvaluationConfig::default())
            .expect("well-formed request evaluates without engine error");

        assert_eq!(
            evaluation.decision,
            PolicyDecision::DenyByDefault { reason: BASELINE_DENY_REASON.to_owned() }
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

        let evaluation = evaluate_with_config(&request, &config)
            .expect("well-formed request evaluates without engine error");

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

        let evaluation = evaluate_with_config(&request, &PolicyEvaluationConfig::default())
            .expect("well-formed request evaluates without engine error");

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
        let denied = evaluate_with_config(&request, &PolicyEvaluationConfig::default())
            .expect("well-formed request evaluates without engine error");
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
        let allowed = evaluate_with_config(&request, &allowed_config)
            .expect("well-formed request evaluates without engine error");
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
        let denied = evaluate_with_config(&request, &PolicyEvaluationConfig::default())
            .expect("well-formed request evaluates without engine error");
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
        let allowed = evaluate_with_config(&request, &allowed_config)
            .expect("well-formed request evaluates without engine error");
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
            .expect("well-formed request evaluates without engine error");
        assert_eq!(
            evaluation.decision,
            PolicyDecision::DenyByDefault { reason: TOOL_EXECUTE_PRINCIPAL_DENY_REASON.to_owned() }
        );
        assert!(!evaluation.explanation.is_tool_execute_principal_allowed);
    }

    #[test]
    fn explain_diagnostics_reports_safe_reason_code_and_hints() {
        let request = PolicyRequest {
            principal: "user:finance@example.com".to_owned(),
            action: "tool.execute".to_owned(),
            resource: "tool:palyra.echo".to_owned(),
        };
        let config = PolicyEvaluationConfig {
            allowlisted_tools: vec!["palyra.echo".to_owned()],
            tool_execute_principal_allowlist: vec!["user:ops".to_owned()],
            ..PolicyEvaluationConfig::default()
        };

        let evaluation = evaluate_with_config(&request, &config)
            .expect("well-formed request evaluates without engine error");
        let diagnostics = super::policy_explain_diagnostics_value(&request, &evaluation);

        assert_eq!(diagnostics["schema_version"], 1);
        assert_eq!(diagnostics["decision"], "deny_by_default");
        assert_eq!(diagnostics["principal_class"], "user:*");
        assert_eq!(diagnostics["reason_code"], "tool.execute.principal_not_allowlisted");
        assert_eq!(
            diagnostics["missing_grant_hints"][0],
            "add_principal_to_tool_execute_allowlist"
        );
        assert_eq!(diagnostics["redaction"]["internal_policy_source_included"], false);
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
            .expect("well-formed request evaluates without engine error");
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
        .expect("well-formed request evaluates without engine error");
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
        let denied = evaluate_with_context(&request, &context, &config)
            .expect("well-formed request evaluates without engine error");
        assert_eq!(
            denied.decision,
            PolicyDecision::DenyByDefault { reason: SENSITIVE_DENY_REASON.to_owned() }
        );

        let allowed = evaluate_with_context(
            &request,
            &context,
            &PolicyEvaluationConfig { allow_sensitive_tools: true, ..config },
        )
        .expect("well-formed request evaluates without engine error");
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
        .expect("well-formed request evaluates without engine error");

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
