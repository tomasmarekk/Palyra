use anyhow::{bail, Result};
use serde_json::{json, Value};

use crate::commands::channels::{post_connector_action, resolve_connector_status};

pub(crate) const MESSAGE_ACTION_SEND: &str = "send";
pub(crate) const MESSAGE_ACTION_THREAD: &str = "thread";
pub(crate) const MESSAGE_ACTION_REPLY: &str = "reply";
pub(crate) const MESSAGE_ACTION_READ: &str = "read";
pub(crate) const MESSAGE_ACTION_SEARCH: &str = "search";
pub(crate) const MESSAGE_ACTION_EDIT: &str = "edit";
pub(crate) const MESSAGE_ACTION_DELETE: &str = "delete";
pub(crate) const MESSAGE_ACTION_REACT_ADD: &str = "react:add";
pub(crate) const MESSAGE_ACTION_REACT_REMOVE: &str = "react:remove";

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

#[derive(Debug, Clone)]
pub(crate) struct MessageCapabilities {
    pub provider_kind: String,
    pub supported_actions: Vec<String>,
    pub unsupported_actions: Vec<String>,
    pub action_details: Vec<MessageCapabilityDetail>,
}

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

pub(crate) fn connector_kind(payload: &Value) -> Option<&str> {
    payload.get("connector").unwrap_or(payload).get("kind").and_then(Value::as_str)
}

pub(crate) fn load_capabilities(
    connector_id: &str,
    url: Option<String>,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
) -> Result<MessageCapabilities> {
    let status = resolve_connector_status(
        connector_id,
        url,
        token,
        principal,
        device_id,
        channel,
        "failed to call channels status endpoint for message capabilities",
    )?;
    let provider_kind = connector_kind(&status).unwrap_or("unknown").to_owned();
    Ok(capabilities_from_status(&status, provider_kind.as_str()))
}

pub(crate) fn send_message(options: MessageDispatchOptions) -> Result<Value> {
    ensure_message_actions_supported(
        options.connector_id.as_str(),
        options.url.clone(),
        options.token.clone(),
        options.principal.clone(),
        options.device_id.clone(),
        options.channel.clone(),
        dispatch_action_names(&options),
    )?;
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

pub(crate) fn add_reaction(options: MessageReactionOptions) -> Result<Value> {
    mutate_reaction(options, "/messages/react-add", MESSAGE_ACTION_REACT_ADD)
}

pub(crate) fn remove_reaction(options: MessageReactionOptions) -> Result<Value> {
    mutate_reaction(options, "/messages/react-remove", MESSAGE_ACTION_REACT_REMOVE)
}

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

fn ensure_message_actions_supported(
    connector_id: &str,
    url: Option<String>,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
    actions: Vec<&'static str>,
) -> Result<()> {
    let capabilities = load_capabilities(connector_id, url, token, principal, device_id, channel)?;
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
        return Ok(());
    }

    let reasons = unsupported
        .into_iter()
        .map(|(action, reason)| format!("{action} ({reason})"))
        .collect::<Vec<_>>()
        .join(", ");
    bail!("unsupported message capability for connector '{connector_id}': {reasons}")
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
        capabilities_from_status, MESSAGE_ACTION_DELETE, MESSAGE_ACTION_EDIT,
        MESSAGE_ACTION_REACT_ADD, MESSAGE_ACTION_REACT_REMOVE, MESSAGE_ACTION_READ,
        MESSAGE_ACTION_REPLY, MESSAGE_ACTION_SEARCH, MESSAGE_ACTION_SEND, MESSAGE_ACTION_THREAD,
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
}
