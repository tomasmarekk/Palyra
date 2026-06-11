//! Payload builders for Discord onboarding probe/apply and verify requests.
//!
//! The credential travels only inside the request body to the daemon, which
//! validates it and stores it in the vault; it is never echoed in output.

use crate::*;
use palyra_connectors::providers::discord::{discord_connector_id, normalize_discord_account_id};

/// Derives the canonical Discord connector id for a raw account id.
///
/// # Errors
/// Fails when Discord account-id normalization rejects the value.
pub(crate) fn connector_id(account_id: &str) -> Result<String> {
    let normalized = normalize_discord_account_id(account_id)
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    Ok(discord_connector_id(normalized.as_str()))
}

/// Builds the onboarding probe payload that validates the credential without
/// persisting configuration.
pub(crate) fn probe_payload(
    account_id: &str,
    token: &str,
    setup_mode: &str,
    verify_channel_id: Option<&str>,
) -> Value {
    json!({
        "account_id": account_id,
        "token": token,
        "mode": setup_mode,
        "verify_channel_id": verify_channel_id,
    })
}

/// Builds the onboarding apply payload that persists the connector
/// configuration and vaults the credential.
#[allow(clippy::too_many_arguments)]
pub(crate) fn apply_payload(
    account_id: &str,
    token: &str,
    setup_mode: &str,
    inbound_scope: &str,
    mention_patterns: &[String],
    allow_from: &[String],
    deny_from: &[String],
    require_mention: bool,
    direct_message_policy: Option<&str>,
    concurrency_limit: u64,
    broadcast_strategy: &str,
    confirm_open_guild_channels: bool,
    verify_channel_id: Option<&str>,
) -> Value {
    json!({
        "account_id": account_id,
        "token": token,
        "mode": setup_mode,
        "inbound_scope": inbound_scope,
        "mention_patterns": mention_patterns,
        "allow_from": allow_from,
        "deny_from": deny_from,
        "require_mention": require_mention,
        "direct_message_policy": direct_message_policy,
        "concurrency_limit": concurrency_limit,
        "broadcast_strategy": broadcast_strategy,
        "confirm_open_guild_channels": confirm_open_guild_channels,
        "verify_channel_id": verify_channel_id,
    })
}

/// Builds the test-send payload; `confirm` is always true because the CLI
/// requires the explicit `--confirm` flag before reaching this point.
pub(crate) fn verify_payload(
    target: &str,
    text: &str,
    auto_reaction: Option<&str>,
    thread_id: Option<&str>,
) -> Value {
    json!({
        "target": target,
        "text": text,
        "confirm": true,
        "auto_reaction": auto_reaction,
        "thread_id": thread_id,
    })
}
