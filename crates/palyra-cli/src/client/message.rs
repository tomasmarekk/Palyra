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

pub(crate) const SUPPORTED_MESSAGE_ACTIONS: &[&str] =
    &[MESSAGE_ACTION_SEND, MESSAGE_ACTION_THREAD, MESSAGE_ACTION_REPLY];
pub(crate) const UNSUPPORTED_MESSAGE_ACTIONS: &[&str] = &[
    MESSAGE_ACTION_READ,
    MESSAGE_ACTION_SEARCH,
    MESSAGE_ACTION_EDIT,
    MESSAGE_ACTION_DELETE,
    MESSAGE_ACTION_REACT_ADD,
    MESSAGE_ACTION_REACT_REMOVE,
];

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
}

#[derive(Debug, Clone)]
pub(crate) struct MessageCapabilities {
    pub provider_kind: String,
    pub supported_actions: Vec<String>,
    pub unsupported_actions: Vec<String>,
    pub action_details: Vec<MessageCapabilityDetail>,
}

#[derive(Debug, Clone)]
pub(crate) struct MessageActionSupportQuery {
    pub action: String,
    pub connector_id: String,
    pub detail: Option<String>,
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

pub(crate) fn unsupported_message_action(query: MessageActionSupportQuery) -> Result<Value> {
    let capabilities = load_capabilities(
        query.connector_id.as_str(),
        query.url,
        query.token,
        query.principal,
        query.device_id,
        query.channel,
    )?;
    let detail = message_action_detail(&capabilities, query.action.as_str());
    Ok(json!({
        "connector_id": query.connector_id,
        "provider_kind": capabilities.provider_kind,
        "action": query.action,
        "detail": query.detail,
        "supported": detail.map(|entry| entry.supported).unwrap_or(false),
        "reason": detail.and_then(|entry| entry.reason.clone()).unwrap_or_else(|| {
            format!(
                "message action '{}' is unavailable for provider '{}'",
                query.action,
                capabilities.provider_kind
            )
        }),
        "supported_actions": capabilities.supported_actions,
        "unsupported_actions": capabilities.unsupported_actions,
    }))
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
                })
            })
            .collect::<Vec<_>>(),
        "notes": [
            "message capabilities are sourced from connector status capabilities exposed by the daemon",
            "send and thread currently reuse the authenticated admin channels transport",
            "read/search/edit/delete/react remain explicitly surfaced as unsupported until dedicated connector APIs exist"
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
        extract_action_details(status).unwrap_or_else(|| fallback_action_details(provider_kind));
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

fn fallback_action_details(provider_kind: &str) -> Vec<MessageCapabilityDetail> {
    let discord = provider_kind.eq_ignore_ascii_case("discord");
    [
        (MESSAGE_ACTION_SEND, discord, None),
        (MESSAGE_ACTION_THREAD, discord, None),
        (
            MESSAGE_ACTION_REPLY,
            discord,
            (!discord).then_some("reply support is unavailable for this provider".to_owned()),
        ),
        (
            MESSAGE_ACTION_READ,
            false,
            Some("message read requires a dedicated connector read surface".to_owned()),
        ),
        (
            MESSAGE_ACTION_SEARCH,
            false,
            Some("message search requires a dedicated connector search surface".to_owned()),
        ),
        (
            MESSAGE_ACTION_EDIT,
            false,
            Some("message edit is not implemented in the current admin surface".to_owned()),
        ),
        (
            MESSAGE_ACTION_DELETE,
            false,
            Some("message delete is not implemented in the current admin surface".to_owned()),
        ),
        (
            MESSAGE_ACTION_REACT_ADD,
            false,
            Some("reaction add is not implemented in the current admin surface".to_owned()),
        ),
        (
            MESSAGE_ACTION_REACT_REMOVE,
            false,
            Some("reaction remove is not implemented in the current admin surface".to_owned()),
        ),
    ]
    .into_iter()
    .map(|(action, supported, reason)| MessageCapabilityDetail {
        action: action.to_owned(),
        supported,
        reason,
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
    }
}

fn message_action_detail<'a>(
    capabilities: &'a MessageCapabilities,
    action: &str,
) -> Option<&'a MessageCapabilityDetail> {
    capabilities.action_details.iter().find(|detail| detail.action == action)
}

#[cfg(test)]
mod tests {
    use super::{
        capabilities_from_status, MESSAGE_ACTION_DELETE, MESSAGE_ACTION_EDIT,
        MESSAGE_ACTION_REACT_ADD, MESSAGE_ACTION_REACT_REMOVE, MESSAGE_ACTION_READ,
        MESSAGE_ACTION_REPLY, MESSAGE_ACTION_SEARCH, MESSAGE_ACTION_SEND, MESSAGE_ACTION_THREAD,
    };
    use serde_json::json;

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
                        "read": { "supported": false, "reason": "read unavailable" },
                        "search": { "supported": false, "reason": "search unavailable" },
                        "edit": { "supported": false, "reason": "edit unavailable" },
                        "delete": { "supported": false, "reason": "delete unavailable" },
                        "react_add": { "supported": false, "reason": "react add unavailable" },
                        "react_remove": { "supported": false, "reason": "react remove unavailable" }
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
            ]
        );
        assert_eq!(
            capabilities.unsupported_actions,
            vec![
                MESSAGE_ACTION_READ.to_owned(),
                MESSAGE_ACTION_SEARCH.to_owned(),
                MESSAGE_ACTION_EDIT.to_owned(),
                MESSAGE_ACTION_DELETE.to_owned(),
                MESSAGE_ACTION_REACT_ADD.to_owned(),
                MESSAGE_ACTION_REACT_REMOVE.to_owned(),
            ]
        );
    }

    #[test]
    fn capabilities_from_status_falls_back_for_discord_when_capabilities_are_missing() {
        let capabilities =
            capabilities_from_status(&json!({"connector": {"kind": "discord"}}), "discord");

        assert!(capabilities.supported_actions.contains(&MESSAGE_ACTION_SEND.to_owned()));
        assert!(capabilities.supported_actions.contains(&MESSAGE_ACTION_THREAD.to_owned()));
        assert!(capabilities.supported_actions.contains(&MESSAGE_ACTION_REPLY.to_owned()));
    }
}
