use std::{fs, path::PathBuf};

use serde_json::{json, Value};

use crate::{
    app::state::AppState,
    transport::http::contracts::channels::discord::DiscordAccountLifecycleRequest, *,
};

use super::onboarding::{
    resolve_discord_onboarding_config_path, validate_discord_onboarding_document_for_lifecycle,
};

#[allow(clippy::result_large_err)]
pub(crate) fn perform_discord_account_logout(
    state: &AppState,
    account_id: String,
    payload: &DiscordAccountLifecycleRequest,
) -> Result<Value, Response> {
    let normalized_account_id = channels::normalize_discord_account_id(account_id.as_str())
        .map_err(channel_platform_error_response)?;
    let connector_id = channels::discord_connector_id(normalized_account_id.as_str());
    let status = state
        .channels
        .set_enabled(connector_id.as_str(), false)
        .map_err(channel_platform_error_response)?;
    let credential_deleted = delete_discord_credential_if_requested(
        state,
        normalized_account_id.as_str(),
        payload.keep_credential,
    )?;
    Ok(json!({
        "action": "logout",
        "provider": "discord",
        "account_id": normalized_account_id,
        "connector_id": connector_id,
        "keep_credential": payload.keep_credential.unwrap_or(false),
        "credential_deleted": credential_deleted,
        "status": status,
    }))
}

#[allow(clippy::result_large_err)]
pub(crate) fn perform_discord_account_remove(
    state: &AppState,
    account_id: String,
    payload: &DiscordAccountLifecycleRequest,
) -> Result<Value, Response> {
    let normalized_account_id = channels::normalize_discord_account_id(account_id.as_str())
        .map_err(channel_platform_error_response)?;
    let connector_id = channels::discord_connector_id(normalized_account_id.as_str());
    let status_before_remove = state
        .channels
        .set_enabled(connector_id.as_str(), false)
        .map_err(channel_platform_error_response)?;
    let credential_deleted = delete_discord_credential_if_requested(
        state,
        normalized_account_id.as_str(),
        payload.keep_credential,
    )?;
    let config_path = remove_discord_onboarding_config(normalized_account_id.as_str())?;
    state
        .channels
        .remove_connector(connector_id.as_str())
        .map_err(channel_platform_error_response)?;
    Ok(json!({
        "action": "remove",
        "provider": "discord",
        "account_id": normalized_account_id,
        "connector_id": connector_id,
        "keep_credential": payload.keep_credential.unwrap_or(false),
        "credential_deleted": credential_deleted,
        "config_updated": config_path.is_some(),
        "config_path": config_path.map(|path| path.display().to_string()),
        "removed": true,
        "status_before_remove": status_before_remove,
    }))
}

#[allow(clippy::result_large_err)]
fn delete_discord_credential_if_requested(
    state: &AppState,
    normalized_account_id: &str,
    keep_credential: Option<bool>,
) -> Result<bool, Response> {
    if keep_credential.unwrap_or(false) {
        return Ok(false);
    }
    let vault_ref = channels::discord_token_vault_ref(normalized_account_id);
    let parsed_ref = VaultRef::parse(vault_ref.as_str()).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to parse discord token vault ref: {error}"
        )))
    })?;
    state.vault.delete_secret(&parsed_ref.scope, parsed_ref.key.as_str()).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to delete discord token from vault: {error}"
        )))
    })
}

#[allow(clippy::result_large_err)]
fn remove_discord_onboarding_config(account_id: &str) -> Result<Option<PathBuf>, Response> {
    let normalized_account_id = channels::normalize_discord_account_id(account_id)
        .map_err(channel_platform_error_response)?;
    let connector_id = channels::discord_connector_id(normalized_account_id.as_str());
    let config_path = resolve_discord_onboarding_config_path()?;
    if !config_path.exists() {
        return Ok(None);
    }

    let content = fs::read_to_string(config_path.as_path()).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read config for discord onboarding removal: {error}"
        )))
    })?;
    let (mut document, _) = parse_document_with_migration(content.as_str()).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to parse config document for discord onboarding removal: {error}"
        )))
    })?;

    let remaining_rules = document
        .get("channel_router")
        .and_then(|value| value.get("routing"))
        .and_then(|value| value.get("channels"))
        .and_then(toml::Value::as_array)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|rule| {
            rule.get("channel")
                .and_then(toml::Value::as_str)
                .is_none_or(|channel| !channel.eq_ignore_ascii_case(connector_id.as_str()))
        })
        .collect::<Vec<_>>();

    set_value_at_path(
        &mut document,
        "channel_router.routing.channels",
        toml::Value::Array(remaining_rules),
    )
    .map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to update channel router rules during discord onboarding removal: {error}"
        )))
    })?;
    let channel_router_enabled = document
        .get("channel_router")
        .and_then(|value| value.get("routing"))
        .and_then(|value| value.get("channels"))
        .and_then(toml::Value::as_array)
        .is_some_and(|entries| !entries.is_empty());
    set_value_at_path(
        &mut document,
        "channel_router.enabled",
        toml::Value::Boolean(channel_router_enabled),
    )
    .map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to update channel_router.enabled during discord onboarding removal: {error}"
        )))
    })?;
    validate_discord_onboarding_document_for_lifecycle(&document)?;
    write_document_with_backups(
        config_path.as_path(),
        &document,
        DISCORD_ONBOARDING_CONFIG_BACKUPS,
    )
    .map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to persist config during discord onboarding removal: {error}"
        )))
    })?;
    Ok(Some(config_path))
}
