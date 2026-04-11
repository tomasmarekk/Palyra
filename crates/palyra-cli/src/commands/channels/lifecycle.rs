use anyhow::{Context, Result};
use serde_json::{json, Value};

use crate::args::ChannelProviderArg;

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
    super::providers::run_channel_lifecycle_upsert(
        action,
        provider,
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
    )
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
    super::providers::run_channel_lifecycle_disable(
        action,
        provider,
        account_id,
        keep_credential,
        url,
        token,
        principal,
        device_id,
        channel,
        json_output,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn emit_channel_lifecycle_disable(
    action: &str,
    provider: ChannelProviderArg,
    account_id: &str,
    connector_id: &str,
    keep_credential: bool,
    credential_deleted: bool,
    response: Value,
    json_output: bool,
) -> Result<()> {
    let payload = json!({
        "action": action,
        "provider": super::provider_label(provider),
        "account_id": account_id,
        "connector_id": connector_id,
        "keep_credential": keep_credential,
        "credential_deleted": credential_deleted,
        "response": response,
    });
    if json_output {
        println!(
            "{}",
            serde_json::to_string_pretty(&payload)
                .context("failed to encode channel lifecycle payload as JSON")?
        );
    } else {
        println!(
            "channels.{} provider={} disabled=true keep_credential={} credential_deleted={}",
            action,
            super::provider_label(provider),
            keep_credential,
            credential_deleted
        );
        let status_payload = payload
            .pointer("/response/status")
            .or_else(|| payload.pointer("/response/status_before_remove"))
            .or_else(|| payload.pointer("/response"))
            .unwrap_or(&Value::Null);
        for line in crate::output::channels::render_status_lines(status_payload) {
            println!("{line}");
        }
    }
    Ok(())
}
