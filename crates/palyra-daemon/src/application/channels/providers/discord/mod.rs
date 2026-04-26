//! Discord-owned daemon channel application helpers.
//!
//! This module contains the provider-specific onboarding, governance, and
//! payload assembly that backs the generic channel application layer.

mod lifecycle;
mod onboarding;

use palyra_connectors::providers::discord::{
    discord_permission_labels_for_operation, discord_policy_action_for_operation,
    DiscordMessageOperation,
};
use serde_json::{json, Value};

use crate::{app::state::AppState, *};

pub(crate) use lifecycle::{perform_discord_account_logout, perform_discord_account_remove};
pub(crate) use onboarding::{
    apply_discord_onboarding, build_discord_channel_permission_warnings,
    build_discord_inbound_monitor_warnings, build_discord_onboarding_preflight,
    discord_inbound_monitor_is_alive, load_discord_inbound_monitor_summary,
    normalize_optional_discord_channel_id, probe_discord_bot_identity,
};
#[cfg(test)]
pub(crate) use onboarding::{
    build_discord_onboarding_plan, build_discord_onboarding_security_defaults,
    finalize_discord_onboarding_plan, normalize_discord_token, summarize_discord_inbound_monitor,
};

pub(crate) fn build_discord_channel_operations_payload(
    connector_id: &str,
    connector: &palyra_connectors::ConnectorStatusSnapshot,
    runtime: Option<&Value>,
    recent_dead_letters: &[palyra_connectors::DeadLetterRecord],
) -> Value {
    let last_permission_failure = super::find_matching_message(
        [
            connector.last_error.as_deref(),
            runtime.and_then(|payload| payload.get("last_error")).and_then(Value::as_str),
            recent_dead_letters.first().map(|entry| entry.reason.as_str()),
        ],
        &[
            "missing permissions",
            "permission",
            "forbidden",
            "view channels",
            "send messages",
            "read message history",
            "embed links",
            "attach files",
            "send messages in threads",
        ],
    );
    json!({
        "required_permissions": discord_required_permission_labels(),
        "last_permission_failure": last_permission_failure,
        "exact_gap_check_available": true,
        "health_refresh_hint": format!(
            "Run channel health refresh for '{}' with verify_channel_id to confirm channel-specific Discord permission gaps.",
            connector_id
        ),
    })
}

#[allow(clippy::result_large_err)]
pub(crate) async fn build_discord_channel_health_refresh_payload(
    state: &AppState,
    connector_id: &str,
    verify_channel_id: Option<String>,
) -> Result<Value, Response> {
    let token = resolve_discord_connector_token(state, connector_id);
    let verify_channel_id = normalize_optional_discord_channel_id(verify_channel_id.as_deref())?;
    let inbound_monitor = load_discord_inbound_monitor_summary(state, connector_id);
    let inbound_alive = discord_inbound_monitor_is_alive(&inbound_monitor);
    let mut warnings = build_discord_inbound_monitor_warnings(&inbound_monitor);
    match token {
        Ok(token) => match probe_discord_bot_identity(token.as_str(), verify_channel_id.as_deref())
            .await
        {
            Ok((bot, application, channel_permission_check)) => {
                let permission_warnings =
                    build_discord_channel_permission_warnings(channel_permission_check.as_ref());
                warnings.extend(permission_warnings.clone());
                Ok(json!({
                    "supported": true,
                    "refreshed": true,
                    "bot": bot,
                    "application": application,
                    "required_permissions": discord_required_permission_labels(),
                    "channel_permission_check": channel_permission_check,
                    "permission_warnings": permission_warnings,
                    "inbound_monitor": inbound_monitor,
                    "inbound_alive": inbound_alive,
                    "warnings": warnings,
                }))
            }
            Err(error) => Ok(json!({
                "supported": true,
                "refreshed": false,
                "message": sanitize_http_error_message(error.message()),
                "required_permissions": discord_required_permission_labels(),
                "inbound_monitor": inbound_monitor,
                "inbound_alive": inbound_alive,
                "warnings": warnings,
            })),
        },
        Err(message) => Ok(json!({
            "supported": true,
            "refreshed": false,
            "message": message,
            "required_permissions": discord_required_permission_labels(),
            "inbound_monitor": inbound_monitor,
            "inbound_alive": inbound_alive,
            "warnings": warnings,
        })),
    }
}

fn discord_account_id_from_connector_id(connector_id: &str) -> Option<&str> {
    connector_id.trim().strip_prefix("discord:").map(str::trim).filter(|value| !value.is_empty())
}

pub(crate) fn resolve_discord_connector_token(
    state: &AppState,
    connector_id: &str,
) -> Result<String, String> {
    let instance = state.channels.connector_instance(connector_id).map_err(|error| {
        format!(
            "failed to load connector instance '{}' for Discord token lookup: {error}",
            connector_id.trim()
        )
    })?;
    let vault_ref_raw = if let Some(vault_ref) =
        instance.token_vault_ref.as_deref().map(str::trim).filter(|value| !value.is_empty())
    {
        vault_ref.to_owned()
    } else {
        let Some(account_id) = discord_account_id_from_connector_id(connector_id) else {
            return Err(format!("connector '{}' is not a Discord connector", connector_id.trim()));
        };
        channels::discord_token_vault_ref(account_id)
    };
    let vault_ref = VaultRef::parse(vault_ref_raw.as_str()).map_err(|error| {
        format!("failed to parse Discord token vault ref '{}': {error}", vault_ref_raw)
    })?;
    let value =
        state.vault.get_secret(&vault_ref.scope, vault_ref.key.as_str()).map_err(|error| {
            format!("failed to load Discord token from vault ref '{}': {error}", vault_ref_raw)
        })?;
    let decoded = String::from_utf8(value).map_err(|error| {
        format!("Discord token from vault ref '{}' was not valid UTF-8: {error}", vault_ref_raw)
    })?;
    let token = decoded.trim().to_owned();
    if token.is_empty() {
        return Err(format!(
            "Discord token vault ref '{}' resolved to an empty secret",
            vault_ref_raw
        ));
    }
    Ok(token)
}

pub(crate) fn channel_message_policy_action(
    operation: channels::DiscordMessageMutationKind,
) -> &'static str {
    let discord_operation = match operation {
        channels::DiscordMessageMutationKind::Edit => DiscordMessageOperation::Edit,
        channels::DiscordMessageMutationKind::Delete => DiscordMessageOperation::Delete,
        channels::DiscordMessageMutationKind::ReactAdd => DiscordMessageOperation::ReactAdd,
        channels::DiscordMessageMutationKind::ReactRemove => DiscordMessageOperation::ReactRemove,
    };
    discord_policy_action_for_operation(discord_operation)
}

pub(crate) fn channel_message_required_permissions(
    operation: channels::DiscordMessageMutationKind,
) -> Vec<String> {
    let discord_operation = match operation {
        channels::DiscordMessageMutationKind::Edit => DiscordMessageOperation::Edit,
        channels::DiscordMessageMutationKind::Delete => DiscordMessageOperation::Delete,
        channels::DiscordMessageMutationKind::ReactAdd => DiscordMessageOperation::ReactAdd,
        channels::DiscordMessageMutationKind::ReactRemove => DiscordMessageOperation::ReactRemove,
    };
    discord_permission_labels_for_operation(discord_operation)
        .iter()
        .map(|value| (*value).to_owned())
        .collect()
}
