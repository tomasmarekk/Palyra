//! Discord implementation of the generic channel lifecycle surface.
//!
//! Upserts reuse the daemon's onboarding probe/apply endpoints so the
//! credential is validated before any configuration is persisted; account
//! ids are normalized by `palyra-connectors` rules before any request.

use anyhow::{bail, Result};
use palyra_connectors::providers::discord::{
    canonical_discord_channel_identity, canonical_discord_sender_identity,
    normalize_discord_account_id, normalize_discord_target,
};
use serde_json::{json, Value};

use crate::{
    args::{ChannelProviderArg, ChannelResolveEntityArg, ChannelsDiscordCommand},
    client::channels as channels_client,
    commands::channels::connectors,
    normalize_required_text_arg,
};

/// Derives the canonical Discord connector id for a raw account id.
///
/// # Errors
/// Fails when Discord account-id normalization rejects the value.
pub(super) fn connector_id(account_id: &str) -> Result<String> {
    connectors::discord::connector_id(account_id)
}

/// Adds or updates a Discord channel connector.
///
/// Interactive mode hands off to the guided setup flow; otherwise the
/// credential is probed against the daemon before the apply request
/// persists configuration.
///
/// # Errors
/// Fails when credential intake, argument normalization, or either
/// onboarding endpoint call fails.
#[allow(clippy::too_many_arguments)]
pub(super) fn run_channel_lifecycle_upsert(
    action: &'static str,
    account_id: String,
    interactive: bool,
    credential: Option<String>,
    credential_stdin: bool,
    credential_prompt: bool,
    allow_insecure_credential_arg: bool,
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
    if interactive {
        if credential.is_some() || credential_stdin || credential_prompt {
            bail!(
                "channels {action} --interactive cannot be combined with explicit credential input"
            );
        }
        return connectors::discord::run(ChannelsDiscordCommand::Setup {
            account_id,
            url,
            token,
            principal,
            device_id,
            channel,
            verify_channel_id,
            json: json_output,
        });
    }

    let normalized_account_id = normalize_discord_account_id(account_id.as_str())
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    let connector_id = connectors::discord::connector_id(normalized_account_id.as_str())?;
    let credential = super::super::load_channel_credential(
        credential,
        credential_stdin,
        credential_prompt,
        allow_insecure_credential_arg,
        "Discord bot token: ",
    )?;
    let mode = normalize_required_text_arg(mode, "mode")?;
    let inbound_scope = normalize_required_text_arg(inbound_scope, "inbound_scope")?;
    let mention_patterns = super::super::normalize_string_list(mention_patterns);
    let allow_from = super::super::normalize_string_list(allow_from);
    let deny_from = super::super::normalize_string_list(deny_from);
    let request_context =
        channels_client::resolve_request_context(url, token, principal, device_id, channel)?;
    let client = channels_client::build_client()?;

    let probe_endpoint = format!(
        "{}/admin/v1/channels/discord/onboarding/probe",
        request_context.base_url.trim_end_matches('/'),
    );
    let _probe_response = channels_client::send_request(
        client.post(probe_endpoint).json(&connectors::discord::probe_payload(
            normalized_account_id.as_str(),
            credential.as_str(),
            mode.as_str(),
            verify_channel_id.as_deref(),
        )),
        request_context.clone(),
        "failed to call discord onboarding probe endpoint",
    )?;

    let apply_endpoint = format!(
        "{}/admin/v1/channels/discord/onboarding/apply",
        request_context.base_url.trim_end_matches('/'),
    );
    let broadcast_strategy = broadcast_strategy.unwrap_or_else(|| "deny".to_owned());
    let response = channels_client::send_request(
        client.post(apply_endpoint).json(&connectors::discord::apply_payload(
            normalized_account_id.as_str(),
            credential.as_str(),
            mode.as_str(),
            inbound_scope.as_str(),
            &mention_patterns,
            &allow_from,
            &deny_from,
            require_mention.unwrap_or(true),
            direct_message_policy.as_deref(),
            concurrency_limit.unwrap_or(2),
            broadcast_strategy.as_str(),
            confirm_open_guild_channels,
            verify_channel_id.as_deref(),
        )),
        request_context,
        "failed to call discord onboarding apply endpoint",
    )?;
    connectors::discord::emit_apply_response(connector_id.as_str(), response, json_output)
}

/// Logs out or removes a Discord channel connector, optionally keeping the
/// vaulted credential.
///
/// # Errors
/// Fails when account-id normalization or the account action endpoint call
/// fails.
#[allow(clippy::too_many_arguments)]
pub(super) fn run_channel_lifecycle_disable(
    action: &'static str,
    account_id: String,
    keep_credential: bool,
    url: Option<String>,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
    json_output: bool,
) -> Result<()> {
    let normalized_account_id = normalize_discord_account_id(account_id.as_str())
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    let connector_id = connectors::discord::connector_id(normalized_account_id.as_str())?;
    let response = super::super::post_discord_account_action(
        normalized_account_id.as_str(),
        action,
        json!({ "keep_credential": keep_credential }),
        url,
        token,
        principal,
        device_id,
        channel,
        if action == "logout" {
            "failed to call discord account logout endpoint"
        } else {
            "failed to call discord account remove endpoint"
        },
    )?;
    let credential_deleted =
        response.get("credential_deleted").and_then(Value::as_bool).unwrap_or(false);
    super::super::emit_channel_lifecycle_disable(
        action,
        ChannelProviderArg::Discord,
        normalized_account_id.as_str(),
        connector_id.as_str(),
        keep_credential,
        credential_deleted,
        response,
        json_output,
    )
}

/// Builds the resolution payload for Discord entities: users canonicalize as
/// sender identities, channel-like entities normalize then canonicalize as
/// channel identities.
///
/// # Errors
/// Fails when account-id or target normalization rejects the input.
pub(super) fn build_channel_resolution_payload(
    account_id: String,
    entity: ChannelResolveEntityArg,
    normalized_input: String,
) -> Result<Value> {
    let normalized_account_id = normalize_discord_account_id(account_id.as_str())
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    let connector_id = connectors::discord::connector_id(normalized_account_id.as_str())?;
    let (normalized, canonical) = match entity {
        ChannelResolveEntityArg::User => {
            let canonical = canonical_discord_sender_identity(normalized_input.as_str());
            (normalized_input.clone(), canonical)
        }
        ChannelResolveEntityArg::Channel
        | ChannelResolveEntityArg::Conversation
        | ChannelResolveEntityArg::Thread => {
            let normalized = normalize_discord_target(normalized_input.as_str())
                .map_err(|error| anyhow::anyhow!(error.to_string()))?;
            let canonical = canonical_discord_channel_identity(normalized.as_str());
            (normalized, canonical)
        }
    };
    Ok(json!({
        "provider": super::label(ChannelProviderArg::Discord),
        "account_id": normalized_account_id,
        "connector_id": connector_id,
        "entity": super::super::resolve_entity_label(entity),
        "input": normalized_input,
        "normalized": normalized,
        "canonical": canonical,
    }))
}
