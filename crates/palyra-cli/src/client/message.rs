//! Connector message operations (send, read, search, edit, delete, reactions)
//! over the daemon channels admin API.
//!
//! Every mutating call is preflighted against the connector's advertised
//! capability metadata so unsupported actions fail before any POST is made.

use anyhow::{bail, Result};
use serde_json::{json, Value};

use crate::commands::channels::{post_connector_action, resolve_connector_status};

// User-facing message action names. `extract_action_details` maps each one to
// its `capabilities.message` field in the daemon's connector status payload
// (the reaction actions map to the `react_add`/`react_remove` fields).
pub(crate) const MESSAGE_ACTION_SEND: &str = "send";
pub(crate) const MESSAGE_ACTION_THREAD: &str = "thread";
pub(crate) const MESSAGE_ACTION_REPLY: &str = "reply";
pub(crate) const MESSAGE_ACTION_READ: &str = "read";
pub(crate) const MESSAGE_ACTION_SEARCH: &str = "search";
pub(crate) const MESSAGE_ACTION_EDIT: &str = "edit";
pub(crate) const MESSAGE_ACTION_DELETE: &str = "delete";
pub(crate) const MESSAGE_ACTION_REACT_ADD: &str = "react:add";
pub(crate) const MESSAGE_ACTION_REACT_REMOVE: &str = "react:remove";

/// Inputs for an outbound message send, including optional thread/reply targets
/// and connection identity.
#[derive(Debug, Clone)]
pub(crate) struct MessageDispatchOptions {
    pub connector_id: String,
    pub target: String,
    pub text: String,
    pub confirm: bool,
    pub auto_reaction: Option<String>,
    pub thread_id: Option<String>,
    pub reply_to_message_id: Option<String>,
    pub url: Option<String>,
    pub token: Option<String>,
    pub principal: String,
    pub device_id: String,
    pub channel: Option<String>,
}

/// Per-action capability metadata: support flag plus policy, approval, risk,
/// audit, and permission requirements advertised by the connector.
#[derive(Debug, Clone)]
pub(crate) struct MessageCapabilityDetail {
    pub action: String,
    pub supported: bool,
    pub reason: Option<String>,
    pub policy_action: Option<String>,
    pub approval_mode: Option<String>,
    pub risk_level: Option<String>,
    pub audit_event_type: Option<String>,
    pub required_permissions: Vec<String>,
}

/// A connector's message capability summary, partitioned into supported and
/// unsupported actions.
#[derive(Debug, Clone)]
pub(crate) struct MessageCapabilities {
    pub provider_kind: String,
    pub supported_actions: Vec<String>,
    pub unsupported_actions: Vec<String>,
    pub action_details: Vec<MessageCapabilityDetail>,
}

/// Inputs for reading conversation history, with mutually optional message-id
/// anchors (`before`/`after`/`around`) and a result limit.
#[derive(Debug, Clone)]
pub(crate) struct MessageReadOptions {
    pub connector_id: String,
    pub conversation_id: String,
    pub thread_id: Option<String>,
    pub message_id: Option<String>,
    pub before_message_id: Option<String>,
    pub after_message_id: Option<String>,
    pub around_message_id: Option<String>,
    pub limit: usize,
    pub url: Option<String>,
    pub token: Option<String>,
    pub principal: String,
    pub device_id: String,
    pub channel: Option<String>,
}

/// Inputs for searching messages within a conversation by text, author, or
/// attachment presence.
#[derive(Debug, Clone)]
pub(crate) struct MessageSearchOptions {
    pub connector_id: String,
    pub conversation_id: String,
    pub thread_id: Option<String>,
    pub query: Option<String>,
    pub author_id: Option<String>,
    pub has_attachments: Option<bool>,
    pub before_message_id: Option<String>,
    pub limit: usize,
    pub url: Option<String>,
    pub token: Option<String>,
    pub principal: String,
    pub device_id: String,
    pub channel: Option<String>,
}

/// Inputs for editing an existing message body.
#[derive(Debug, Clone)]
pub(crate) struct MessageEditOptions {
    pub connector_id: String,
    pub conversation_id: String,
    pub thread_id: Option<String>,
    pub message_id: String,
    pub body: String,
    pub approval_id: Option<String>,
    pub url: Option<String>,
    pub token: Option<String>,
    pub principal: String,
    pub device_id: String,
    pub channel: Option<String>,
}

/// Inputs for deleting a message, with an optional audit reason.
#[derive(Debug, Clone)]
pub(crate) struct MessageDeleteOptions {
    pub connector_id: String,
    pub conversation_id: String,
    pub thread_id: Option<String>,
    pub message_id: String,
    pub reason: Option<String>,
    pub approval_id: Option<String>,
    pub url: Option<String>,
    pub token: Option<String>,
    pub principal: String,
    pub device_id: String,
    pub channel: Option<String>,
}

/// Inputs for adding or removing an emoji reaction on a message.
#[derive(Debug, Clone)]
pub(crate) struct MessageReactionOptions {
    pub connector_id: String,
    pub conversation_id: String,
    pub thread_id: Option<String>,
    pub message_id: String,
    pub emoji: String,
    pub approval_id: Option<String>,
    pub url: Option<String>,
    pub token: Option<String>,
    pub principal: String,
    pub device_id: String,
    pub channel: Option<String>,
}

/// Extracts the provider kind from a connector status payload, accepting both
/// the wrapped (`{"connector": {...}}`) and bare connector object shapes.
pub(crate) fn connector_kind(payload: &Value) -> Option<&str> {
    payload.get("connector").unwrap_or(payload).get("kind").and_then(Value::as_str)
}

/// Loads a connector's message capabilities from its daemon status payload.
///
/// # Errors
/// Returns an error when the connector id is not routable or the status
/// endpoint call fails.
pub(crate) fn load_capabilities(
    connector_id: &str,
    url: Option<String>,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
) -> Result<MessageCapabilities> {
    let status = load_connector_status(connector_id, url, token, principal, device_id, channel)?;
    let provider_kind = connector_kind(&status).unwrap_or("unknown").to_owned();
    Ok(capabilities_from_status(&status, provider_kind.as_str()))
}

fn load_connector_status(
    connector_id: &str,
    url: Option<String>,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
) -> Result<Value> {
    validate_message_connector_id(connector_id)?;
    resolve_connector_status(
        connector_id,
        url,
        token,
        principal,
        device_id,
        channel,
        "failed to call channels status endpoint for message capabilities",
    )
}

/// Validates that a connector id is routable for message commands.
///
/// # Errors
/// Returns an error when the id is empty or is a provider shorthand without an
/// instance segment (no `:`); the message suggests the `provider:instance` form.
pub(crate) fn validate_message_connector_id(connector_id: &str) -> Result<()> {
    let connector_id = connector_id.trim();
    if connector_id.is_empty() {
        bail!("message commands require a connector id such as `echo:default`");
    }
    if connector_id.contains(':') {
        return Ok(());
    }
    bail!(
        "message commands require a routable connector id with provider and instance, such as `{connector_id}:default`; `{connector_id}` is a provider shorthand for channel filters, not a message connector id. Run `palyra channels list --json` and use the `connector_id` value."
    );
}

/// Sends an outbound message after capability and readiness preflights.
///
/// # Errors
/// Returns an error when preflight fails (unsupported action, connector
/// disabled or not ready/running) or the send endpoint call fails.
pub(crate) fn send_message(options: MessageDispatchOptions) -> Result<Value> {
    let status = ensure_message_actions_supported(
        options.connector_id.as_str(),
        options.url.clone(),
        options.token.clone(),
        options.principal.clone(),
        options.device_id.clone(),
        options.channel.clone(),
        dispatch_action_names(&options),
    )?;
    ensure_connector_can_send(options.connector_id.as_str(), &status)?;
    post_connector_action(
        options.connector_id.as_str(),
        "/test-send",
        Some(json!({
            "target": options.target,
            "text": options.text,
            "confirm": options.confirm,
            "auto_reaction": options.auto_reaction,
            "thread_id": options.thread_id,
            "reply_to_message_id": options.reply_to_message_id,
        })),
        options.url,
        options.token,
        options.principal,
        options.device_id,
        options.channel,
        "failed to call message send endpoint",
    )
}

/// Reads conversation history after a capability preflight.
///
/// # Errors
/// Returns an error when the action is unsupported or the read endpoint fails.
pub(crate) fn read_messages(options: MessageReadOptions) -> Result<Value> {
    ensure_message_actions_supported(
        options.connector_id.as_str(),
        options.url.clone(),
        options.token.clone(),
        options.principal.clone(),
        options.device_id.clone(),
        options.channel.clone(),
        vec![MESSAGE_ACTION_READ],
    )?;
    post_connector_action(
        options.connector_id.as_str(),
        "/messages/read",
        Some(json!({
            "conversation_id": options.conversation_id,
            "thread_id": options.thread_id,
            "message_id": options.message_id,
            "before_message_id": options.before_message_id,
            "after_message_id": options.after_message_id,
            "around_message_id": options.around_message_id,
            "limit": options.limit,
        })),
        options.url,
        options.token,
        options.principal,
        options.device_id,
        options.channel,
        "failed to call message read endpoint",
    )
}

/// Searches messages after a capability preflight.
///
/// # Errors
/// Returns an error when the action is unsupported or the search endpoint fails.
pub(crate) fn search_messages(options: MessageSearchOptions) -> Result<Value> {
    ensure_message_actions_supported(
        options.connector_id.as_str(),
        options.url.clone(),
        options.token.clone(),
        options.principal.clone(),
        options.device_id.clone(),
        options.channel.clone(),
        vec![MESSAGE_ACTION_SEARCH],
    )?;
    post_connector_action(
        options.connector_id.as_str(),
        "/messages/search",
        Some(json!({
            "conversation_id": options.conversation_id,
            "thread_id": options.thread_id,
            "query": options.query,
            "author_id": options.author_id,
            "has_attachments": options.has_attachments,
            "before_message_id": options.before_message_id,
            "limit": options.limit,
        })),
        options.url,
        options.token,
        options.principal,
        options.device_id,
        options.channel,
        "failed to call message search endpoint",
    )
}

/// Edits a message after a capability preflight.
///
/// # Errors
/// Returns an error when the action is unsupported or the edit endpoint fails.
pub(crate) fn edit_message(options: MessageEditOptions) -> Result<Value> {
    ensure_message_actions_supported(
        options.connector_id.as_str(),
        options.url.clone(),
        options.token.clone(),
        options.principal.clone(),
        options.device_id.clone(),
        options.channel.clone(),
        vec![MESSAGE_ACTION_EDIT],
    )?;
    post_connector_action(
        options.connector_id.as_str(),
        "/messages/edit",
        Some(json!({
            "locator": {
                "conversation_id": options.conversation_id,
                "thread_id": options.thread_id,
                "message_id": options.message_id,
            },
            "body": options.body,
            "approval_id": options.approval_id,
        })),
        options.url,
        options.token,
        options.principal,
        options.device_id,
        options.channel,
        "failed to call message edit endpoint",
    )
}

/// Deletes a message after a capability preflight.
///
/// # Errors
/// Returns an error when the action is unsupported or the delete endpoint fails.
pub(crate) fn delete_message(options: MessageDeleteOptions) -> Result<Value> {
    ensure_message_actions_supported(
        options.connector_id.as_str(),
        options.url.clone(),
        options.token.clone(),
        options.principal.clone(),
        options.device_id.clone(),
        options.channel.clone(),
        vec![MESSAGE_ACTION_DELETE],
    )?;
    post_connector_action(
        options.connector_id.as_str(),
        "/messages/delete",
        Some(json!({
            "locator": {
                "conversation_id": options.conversation_id,
                "thread_id": options.thread_id,
                "message_id": options.message_id,
            },
            "reason": options.reason,
            "approval_id": options.approval_id,
        })),
        options.url,
        options.token,
        options.principal,
        options.device_id,
        options.channel,
        "failed to call message delete endpoint",
    )
}

/// Adds an emoji reaction after a capability preflight.
///
/// # Errors
/// Returns an error when the action is unsupported or the endpoint call fails.
pub(crate) fn add_reaction(options: MessageReactionOptions) -> Result<Value> {
    mutate_reaction(options, "/messages/react-add", MESSAGE_ACTION_REACT_ADD)
}

/// Removes an emoji reaction after a capability preflight.
///
/// # Errors
/// Returns an error when the action is unsupported or the endpoint call fails.
pub(crate) fn remove_reaction(options: MessageReactionOptions) -> Result<Value> {
    mutate_reaction(options, "/messages/react-remove", MESSAGE_ACTION_REACT_REMOVE)
}

/// Encodes capabilities into the stable JSON shape emitted by
/// `palyra message capabilities --json`.
pub(crate) fn encode_capabilities_json(
    connector_id: &str,
    capabilities: &MessageCapabilities,
) -> Value {
    json!({
        "connector_id": connector_id,
        "provider_kind": capabilities.provider_kind,
        "supported_actions": capabilities.supported_actions,
        "unsupported_actions": capabilities.unsupported_actions,
        "action_details": capabilities
            .action_details
            .iter()
            .map(|detail| {
                json!({
                    "action": detail.action,
                    "supported": detail.supported,
                    "reason": detail.reason,
                    "policy_action": detail.policy_action,
                    "approval_mode": detail.approval_mode,
                    "risk_level": detail.risk_level,
                    "audit_event_type": detail.audit_event_type,
                    "required_permissions": detail.required_permissions,
                })
            })
            .collect::<Vec<_>>(),
        "notes": [
            "message capabilities are sourced from connector status capabilities exposed by the daemon",
            "send and thread currently reuse the authenticated admin channels transport",
            "read, search, edit, delete, and reaction operations now expose explicit policy, approval, and permission metadata when the connector supports them"
        ],
    })
}

/// Wraps a connector response in the stable JSON envelope used by message
/// command `--json` output.
pub(crate) fn encode_dispatch_json(action: &str, connector_id: &str, response: Value) -> Value {
    json!({
        "action": action,
        "connector_id": connector_id,
        "response": response,
    })
}

fn dispatch_action_names(options: &MessageDispatchOptions) -> Vec<&'static str> {
    let mut actions = vec![MESSAGE_ACTION_SEND];
    if options.thread_id.is_some() {
        actions.push(MESSAGE_ACTION_THREAD);
    }
    if options.reply_to_message_id.is_some() {
        actions.push(MESSAGE_ACTION_REPLY);
    }
    actions
}

// Returns the full status payload on success so callers (send preflight) can
// reuse it without a second status round-trip.
fn ensure_message_actions_supported(
    connector_id: &str,
    url: Option<String>,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
    actions: Vec<&'static str>,
) -> Result<Value> {
    let status = load_connector_status(connector_id, url, token, principal, device_id, channel)?;
    let provider_kind = connector_kind(&status).unwrap_or("unknown").to_owned();
    let capabilities = capabilities_from_status(&status, provider_kind.as_str());
    let unsupported = actions
        .into_iter()
        .filter_map(|action| {
            let detail = message_action_detail(&capabilities, action)?;
            (!detail.supported).then(|| {
                (action, detail.reason.clone().unwrap_or_else(|| "unsupported".to_owned()))
            })
        })
        .collect::<Vec<_>>();
    if unsupported.is_empty() {
        return Ok(status);
    }

    let reasons = unsupported
        .into_iter()
        .map(|(action, reason)| format!("{action} ({reason})"))
        .collect::<Vec<_>>()
        .join(", ");
    bail!("unsupported message capability for connector '{connector_id}': {reasons}")
}

fn ensure_connector_can_send(connector_id: &str, status: &Value) -> Result<()> {
    let connector = status.get("connector").unwrap_or(status);
    let enabled = connector.get("enabled").and_then(Value::as_bool).unwrap_or(false);
    let readiness = connector.get("readiness").and_then(Value::as_str).unwrap_or("unknown");
    let liveness = connector.get("liveness").and_then(Value::as_str).unwrap_or("unknown");
    if enabled && readiness == "ready" && liveness == "running" {
        return Ok(());
    }

    let mut reasons = Vec::new();
    if !enabled {
        reasons.push("disabled".to_owned());
    }
    if readiness != "ready" {
        reasons.push(format!("readiness={readiness}"));
    }
    if liveness != "running" {
        reasons.push(format!("liveness={liveness}"));
    }
    bail!(
        "precondition failed: connector '{connector_id}' cannot send outbound messages because {}; run `palyra channels status {connector_id}` and enable or configure the connector before retrying",
        reasons.join(", ")
    )
}

fn capabilities_from_status(status: &Value, provider_kind: &str) -> MessageCapabilities {
    let action_details =
        extract_action_details(status).unwrap_or_else(|| missing_capability_details(provider_kind));
    let supported_actions = action_details
        .iter()
        .filter(|detail| detail.supported)
        .map(|detail| detail.action.clone())
        .collect();
    let unsupported_actions = action_details
        .iter()
        .filter(|detail| !detail.supported)
        .map(|detail| detail.action.clone())
        .collect();
    MessageCapabilities {
        provider_kind: provider_kind.to_owned(),
        supported_actions,
        unsupported_actions,
        action_details,
    }
}

fn extract_action_details(status: &Value) -> Option<Vec<MessageCapabilityDetail>> {
    let message = status.get("connector").unwrap_or(status).get("capabilities")?.get("message")?;
    Some(vec![
        build_action_detail(message, "send", MESSAGE_ACTION_SEND),
        build_action_detail(message, "thread", MESSAGE_ACTION_THREAD),
        build_action_detail(message, "reply", MESSAGE_ACTION_REPLY),
        build_action_detail(message, "read", MESSAGE_ACTION_READ),
        build_action_detail(message, "search", MESSAGE_ACTION_SEARCH),
        build_action_detail(message, "edit", MESSAGE_ACTION_EDIT),
        build_action_detail(message, "delete", MESSAGE_ACTION_DELETE),
        build_action_detail(message, "react_add", MESSAGE_ACTION_REACT_ADD),
        build_action_detail(message, "react_remove", MESSAGE_ACTION_REACT_REMOVE),
    ])
}

// Fail closed: when the status payload carries no capability metadata, every
// action is reported as unsupported instead of being optimistically allowed.
fn missing_capability_details(provider_kind: &str) -> Vec<MessageCapabilityDetail> {
    let reason =
        format!("message capability metadata is unavailable for provider '{}'", provider_kind);
    [
        MESSAGE_ACTION_SEND,
        MESSAGE_ACTION_THREAD,
        MESSAGE_ACTION_REPLY,
        MESSAGE_ACTION_READ,
        MESSAGE_ACTION_SEARCH,
        MESSAGE_ACTION_EDIT,
        MESSAGE_ACTION_DELETE,
        MESSAGE_ACTION_REACT_ADD,
        MESSAGE_ACTION_REACT_REMOVE,
    ]
    .into_iter()
    .map(|action| MessageCapabilityDetail {
        action: action.to_owned(),
        supported: false,
        reason: Some(reason.clone()),
        policy_action: None,
        approval_mode: None,
        risk_level: None,
        audit_event_type: None,
        required_permissions: Vec::new(),
    })
    .collect()
}

fn build_action_detail(
    message_payload: &Value,
    field: &str,
    action: &str,
) -> MessageCapabilityDetail {
    let payload = message_payload.get(field).unwrap_or(&Value::Null);
    MessageCapabilityDetail {
        action: action.to_owned(),
        supported: payload.get("supported").and_then(Value::as_bool).unwrap_or(false),
        reason: payload.get("reason").and_then(Value::as_str).map(ToOwned::to_owned),
        policy_action: payload.get("policy_action").and_then(Value::as_str).map(ToOwned::to_owned),
        approval_mode: payload.get("approval_mode").and_then(Value::as_str).map(ToOwned::to_owned),
        risk_level: payload.get("risk_level").and_then(Value::as_str).map(ToOwned::to_owned),
        audit_event_type: payload
            .get("audit_event_type")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        required_permissions: payload
            .get("required_permissions")
            .and_then(Value::as_array)
            .map(|values| {
                values.iter().filter_map(Value::as_str).map(ToOwned::to_owned).collect::<Vec<_>>()
            })
            .unwrap_or_default(),
    }
}

fn message_action_detail<'a>(
    capabilities: &'a MessageCapabilities,
    action: &str,
) -> Option<&'a MessageCapabilityDetail> {
    capabilities.action_details.iter().find(|detail| detail.action == action)
}

fn mutate_reaction(
    options: MessageReactionOptions,
    endpoint: &'static str,
    action: &'static str,
) -> Result<Value> {
    ensure_message_actions_supported(
        options.connector_id.as_str(),
        options.url.clone(),
        options.token.clone(),
        options.principal.clone(),
        options.device_id.clone(),
        options.channel.clone(),
        vec![action],
    )?;
    post_connector_action(
        options.connector_id.as_str(),
        endpoint,
        Some(json!({
            "locator": {
                "conversation_id": options.conversation_id,
                "thread_id": options.thread_id,
                "message_id": options.message_id,
            },
            "emoji": options.emoji,
            "approval_id": options.approval_id,
        })),
        options.url,
        options.token,
        options.principal,
        options.device_id,
        options.channel,
        "failed to call message reaction endpoint",
    )
}

#[cfg(test)]
mod tests {
    use super::{
        capabilities_from_status, ensure_connector_can_send, validate_message_connector_id,
        MESSAGE_ACTION_DELETE, MESSAGE_ACTION_EDIT, MESSAGE_ACTION_REACT_ADD,
        MESSAGE_ACTION_REACT_REMOVE, MESSAGE_ACTION_READ, MESSAGE_ACTION_REPLY,
        MESSAGE_ACTION_SEARCH, MESSAGE_ACTION_SEND, MESSAGE_ACTION_THREAD,
    };
    use serde_json::{json, Value};

    #[test]
    fn capabilities_from_status_uses_connector_capability_payload() {
        let status = json!({
            "connector": {
                "kind": "discord",
                "capabilities": {
                    "message": {
                        "send": { "supported": true },
                        "thread": { "supported": true },
                        "reply": { "supported": true },
                        "read": { "supported": true, "policy_action": "channel.message.read", "approval_mode": "none", "risk_level": "low", "audit_event_type": "discord.message.read", "required_permissions": ["ViewChannel", "ReadMessageHistory"] },
                        "search": { "supported": true, "policy_action": "channel.message.search", "approval_mode": "none", "risk_level": "low", "audit_event_type": "discord.message.search", "required_permissions": ["ViewChannel", "ReadMessageHistory"] },
                        "edit": { "supported": true, "policy_action": "channel.message.edit", "approval_mode": "conditional", "risk_level": "conditional", "audit_event_type": "discord.message.edit", "required_permissions": ["ViewChannel", "SendMessages"] },
                        "delete": { "supported": true, "policy_action": "channel.message.delete", "approval_mode": "required", "risk_level": "high", "audit_event_type": "discord.message.delete", "required_permissions": ["ViewChannel", "ManageMessages"] },
                        "react_add": { "supported": true, "policy_action": "channel.message.react_add", "approval_mode": "conditional", "risk_level": "conditional", "audit_event_type": "discord.message.react_add", "required_permissions": ["ViewChannel", "AddReactions"] },
                        "react_remove": { "supported": true, "policy_action": "channel.message.react_remove", "approval_mode": "conditional", "risk_level": "conditional", "audit_event_type": "discord.message.react_remove", "required_permissions": ["ViewChannel", "AddReactions"] }
                    }
                }
            }
        });

        let capabilities = capabilities_from_status(&status, "discord");

        assert_eq!(
            capabilities.supported_actions,
            vec![
                MESSAGE_ACTION_SEND.to_owned(),
                MESSAGE_ACTION_THREAD.to_owned(),
                MESSAGE_ACTION_REPLY.to_owned(),
                MESSAGE_ACTION_READ.to_owned(),
                MESSAGE_ACTION_SEARCH.to_owned(),
                MESSAGE_ACTION_EDIT.to_owned(),
                MESSAGE_ACTION_DELETE.to_owned(),
                MESSAGE_ACTION_REACT_ADD.to_owned(),
                MESSAGE_ACTION_REACT_REMOVE.to_owned(),
            ]
        );
        assert!(capabilities.unsupported_actions.is_empty());
        let edit = capabilities
            .action_details
            .iter()
            .find(|detail| detail.action == MESSAGE_ACTION_EDIT)
            .expect("edit action detail should exist");
        assert_eq!(edit.policy_action.as_deref(), Some("channel.message.edit"));
        assert_eq!(edit.approval_mode.as_deref(), Some("conditional"));
        assert_eq!(edit.risk_level.as_deref(), Some("conditional"));
        assert_eq!(edit.audit_event_type.as_deref(), Some("discord.message.edit"));
        assert_eq!(edit.required_permissions, vec!["ViewChannel", "SendMessages"]);
    }

    #[test]
    fn capabilities_from_status_marks_missing_metadata_as_unsupported() {
        let capabilities = capabilities_from_status(&Value::Null, "discord");

        assert!(capabilities.supported_actions.is_empty());
        assert_eq!(capabilities.unsupported_actions.len(), 9);
        assert!(capabilities.action_details.iter().all(|detail| !detail.supported));
        assert!(capabilities.action_details.iter().all(|detail| {
            detail.reason.as_deref()
                == Some("message capability metadata is unavailable for provider 'discord'")
        }));
    }

    #[test]
    fn capabilities_from_status_marks_missing_metadata_as_unsupported_for_non_discord() {
        let capabilities = capabilities_from_status(&Value::Null, "slack");

        assert!(capabilities.supported_actions.is_empty());
        assert_eq!(capabilities.unsupported_actions.len(), 9);
    }

    #[test]
    fn send_preflight_rejects_disabled_or_unready_connectors() {
        let status = json!({
            "connector": {
                "enabled": false,
                "readiness": "missing_credential",
                "liveness": "stopped"
            }
        });

        let error = ensure_connector_can_send("discord:default", &status)
            .expect_err("message send should fail before POSTing to disabled connectors");

        let message = error.to_string();
        assert!(message.contains("precondition failed"));
        assert!(message.contains("disabled"));
        assert!(message.contains("readiness=missing_credential"));
        assert!(message.contains("liveness=stopped"));
        assert!(message.contains("palyra channels status discord:default"));
    }

    #[test]
    fn send_preflight_accepts_ready_running_connectors() {
        let status = json!({
            "connector": {
                "enabled": true,
                "readiness": "ready",
                "liveness": "running"
            }
        });

        ensure_connector_can_send("discord:default", &status)
            .expect("ready running connectors should pass send preflight");
    }

    #[test]
    fn message_connector_id_validation_suggests_default_instance_for_provider_shorthand() {
        let error = validate_message_connector_id("echo")
            .expect_err("message commands should reject provider shorthand");

        assert!(
            error.to_string().contains("echo:default")
                && error.to_string().contains("palyra channels list --json")
                && error.to_string().contains("provider shorthand"),
            "provider shorthand should produce an actionable connector id hint: {error}"
        );
    }

    #[test]
    fn message_connector_id_validation_accepts_provider_instance_ids() {
        validate_message_connector_id("echo:default")
            .expect("provider instance connector ids should be accepted");
    }
}
