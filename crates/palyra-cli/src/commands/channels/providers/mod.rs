//! Central provider dispatch for CLI channel workflows.
//!
//! The CLI keeps generic argument normalization here and delegates
//! provider-specific lifecycle behavior to submodules.

pub(super) mod discord;

use anyhow::Result;
use serde_json::{json, Value};

use crate::args::{ChannelProviderArg, ChannelResolveEntityArg};

#[allow(clippy::too_many_arguments)]
pub(super) fn run_channel_lifecycle_upsert(
    action: &'static str,
    provider: ChannelProviderArg,
    account_id: String,
    interactive: bool,
    credential: Option<String>,
    credential_stdin: bool,
    credential_prompt: bool,
    mode: String,
    inbound_scope: String,
    allow_from: Vec<String>,
    deny_from: Vec<String>,
    require_mention: Option<bool>,
    mention_patterns: Vec<String>,
    concurrency_limit: Option<u64>,
    direct_message_policy: Option<String>,
    broadcast_strategy: Option<String>,
    confirm_open_guild_channels: bool,
    verify_channel_id: Option<String>,
    url: Option<String>,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
    json_output: bool,
) -> Result<()> {
    match provider {
        ChannelProviderArg::Discord => discord::run_channel_lifecycle_upsert(
            action,
            account_id,
            interactive,
            credential,
            credential_stdin,
            credential_prompt,
            mode,
            inbound_scope,
            allow_from,
            deny_from,
            require_mention,
            mention_patterns,
            concurrency_limit,
            direct_message_policy,
            broadcast_strategy,
            confirm_open_guild_channels,
            verify_channel_id,
            url,
            token,
            principal,
            device_id,
            channel,
            json_output,
        ),
        other => super::unsupported_provider_action(
            "channels",
            action,
            other,
            None,
            json_output,
            "provider lifecycle is implemented for Discord only in the current provider set",
        ),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn run_channel_lifecycle_disable(
    action: &'static str,
    provider: ChannelProviderArg,
    account_id: String,
    keep_credential: bool,
    url: Option<String>,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
    json_output: bool,
) -> Result<()> {
    match provider {
        ChannelProviderArg::Discord => discord::run_channel_lifecycle_disable(
            action,
            account_id,
            keep_credential,
            url,
            token,
            principal,
            device_id,
            channel,
            json_output,
        ),
        other => super::unsupported_provider_action(
            "channels",
            action,
            other,
            None,
            json_output,
            "provider logout/remove is implemented for Discord only in the current provider set",
        ),
    }
}

pub(super) fn build_channel_resolution_payload(
    provider: ChannelProviderArg,
    account_id: String,
    entity: ChannelResolveEntityArg,
    value: String,
) -> Result<Value> {
    let normalized_input = crate::normalize_required_text_arg(value, "value")?;
    match provider {
        ChannelProviderArg::Discord => {
            discord::build_channel_resolution_payload(account_id, entity, normalized_input)
        }
        other => Ok(json!({
            "provider": label(other),
            "account_id": account_id,
            "entity": super::resolve_entity_label(entity),
            "input": normalized_input,
            "supported": false,
            "reason": format!(
                "entity resolution is implemented for Discord only (requested provider={})",
                label(other)
            ),
        })),
    }
}

pub(super) fn connector_id_for_provider(
    provider: ChannelProviderArg,
    account_id: &str,
) -> Result<String> {
    match provider {
        ChannelProviderArg::Discord => discord::connector_id(account_id),
        ChannelProviderArg::Echo => {
            Ok(format!("echo:{}", super::normalize_generic_account_id(account_id, "account_id")?))
        }
        ChannelProviderArg::Slack => {
            Ok(format!("slack:{}", super::normalize_generic_account_id(account_id, "account_id")?))
        }
        ChannelProviderArg::Telegram => Ok(format!(
            "telegram:{}",
            super::normalize_generic_account_id(account_id, "account_id")?
        )),
        ChannelProviderArg::Webhook => Ok(format!(
            "webhook:{}",
            super::normalize_generic_account_id(account_id, "account_id")?
        )),
    }
}

pub(super) fn infer_provider_from_connector_id(connector_id: &str) -> Option<ChannelProviderArg> {
    let prefix = connector_id.trim().split(':').next()?.to_ascii_lowercase();
    match prefix.as_str() {
        "discord" => Some(ChannelProviderArg::Discord),
        "slack" => Some(ChannelProviderArg::Slack),
        "telegram" => Some(ChannelProviderArg::Telegram),
        "webhook" => Some(ChannelProviderArg::Webhook),
        "echo" => Some(ChannelProviderArg::Echo),
        _ => None,
    }
}

pub(super) fn supported_lifecycle_actions(provider: ChannelProviderArg) -> Vec<&'static str> {
    match provider {
        ChannelProviderArg::Discord => vec![
            "add",
            "login",
            "logout",
            "remove",
            "status",
            "health_refresh",
            "logs",
            "capabilities",
            "resolve",
            "pairings",
            "pairing_code",
            "qr",
        ],
        ChannelProviderArg::Echo => vec!["status", "logs", "capabilities"],
        ChannelProviderArg::Slack | ChannelProviderArg::Telegram | ChannelProviderArg::Webhook => {
            vec!["capabilities"]
        }
    }
}

pub(super) fn supported_resolve_entities(provider: ChannelProviderArg) -> Vec<&'static str> {
    match provider {
        ChannelProviderArg::Discord => vec!["channel", "conversation", "thread", "user"],
        ChannelProviderArg::Echo
        | ChannelProviderArg::Slack
        | ChannelProviderArg::Telegram
        | ChannelProviderArg::Webhook => Vec::new(),
    }
}

pub(super) fn pairing_supported(provider: ChannelProviderArg) -> bool {
    matches!(provider, ChannelProviderArg::Discord)
}

pub(super) fn capability_notes(provider: ChannelProviderArg) -> Vec<&'static str> {
    match provider {
        ChannelProviderArg::Discord => vec![
            "discord lifecycle reuses the authenticated onboarding flow",
            "direct outbound send/thread map to the existing discord test-send transport",
            "pairing QR output emits qr_text suitable for external QR encoding",
        ],
        ChannelProviderArg::Echo => vec![
            "echo remains an internal/runtime diagnostic connector",
            "provider lifecycle commands are intentionally unavailable for echo",
        ],
        ChannelProviderArg::Slack | ChannelProviderArg::Telegram | ChannelProviderArg::Webhook => {
            vec!["provider contract reserved for a future connector implementation"]
        }
    }
}

pub(super) const fn label(provider: ChannelProviderArg) -> &'static str {
    match provider {
        ChannelProviderArg::Discord => "discord",
        ChannelProviderArg::Slack => "slack",
        ChannelProviderArg::Telegram => "telegram",
        ChannelProviderArg::Webhook => "webhook",
        ChannelProviderArg::Echo => "echo",
    }
}
