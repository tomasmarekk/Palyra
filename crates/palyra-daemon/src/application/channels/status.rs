use serde_json::{json, Value};

use crate::{
    app::state::AppState,
    application::channels::providers::discord::{
        build_discord_channel_permission_warnings, build_discord_inbound_monitor_warnings,
        discord_inbound_monitor_is_alive, load_discord_inbound_monitor_summary,
        normalize_optional_discord_channel_id, probe_discord_bot_identity,
    },
    *,
};

#[allow(clippy::result_large_err)]
pub(crate) fn build_channel_status_payload(
    state: &AppState,
    connector_id: &str,
) -> Result<Value, Response> {
    let connector = state.channels.status(connector_id).map_err(channel_platform_error_response)?;
    let runtime =
        state.channels.runtime_snapshot(connector_id).map_err(channel_platform_error_response)?;
    let queue =
        state.channels.queue_snapshot(connector_id).map_err(channel_platform_error_response)?;
    let recent_dead_letters = state
        .channels
        .dead_letters(connector_id, Some(5))
        .map_err(channel_platform_error_response)?;
    Ok(json!({
        "connector": connector,
        "runtime": runtime,
        "operations": build_channel_operations_snapshot(
            connector_id,
            &connector,
            runtime.as_ref(),
            &queue,
            recent_dead_letters.as_slice(),
        ),
    }))
}

#[allow(clippy::result_large_err)]
pub(crate) async fn build_channel_health_refresh_payload(
    state: &AppState,
    connector_id: &str,
    verify_channel_id: Option<String>,
) -> Result<Value, Response> {
    let mut payload = build_channel_status_payload(state, connector_id)?;
    if !connector_id.trim().starts_with("discord:") {
        payload["health_refresh"] = json!({
            "supported": false,
            "message": "health refresh is currently implemented for Discord connectors only",
        });
        return Ok(payload);
    }

    let token = match resolve_discord_connector_token(state, connector_id) {
        Ok(token) => token,
        Err(message) => {
            payload["health_refresh"] = json!({
                "supported": true,
                "refreshed": false,
                "message": message,
                "required_permissions": discord_required_permission_labels(),
            });
            return Ok(payload);
        }
    };

    let verify_channel_id = normalize_optional_discord_channel_id(verify_channel_id.as_deref())?;
    let inbound_monitor = load_discord_inbound_monitor_summary(state, connector_id);
    let inbound_alive = discord_inbound_monitor_is_alive(&inbound_monitor);
    let mut warnings = build_discord_inbound_monitor_warnings(&inbound_monitor);
    match probe_discord_bot_identity(token.as_str(), verify_channel_id.as_deref()).await {
        Ok((bot, application, channel_permission_check)) => {
            let permission_warnings =
                build_discord_channel_permission_warnings(channel_permission_check.as_ref());
            warnings.extend(permission_warnings.clone());
            payload["health_refresh"] = json!({
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
            });
        }
        Err(error) => {
            let message = sanitize_http_error_message(error.message());
            payload["health_refresh"] = json!({
                "supported": true,
                "refreshed": false,
                "message": message,
                "required_permissions": discord_required_permission_labels(),
                "inbound_monitor": inbound_monitor,
                "inbound_alive": inbound_alive,
                "warnings": warnings,
            });
        }
    }
    Ok(payload)
}

fn build_channel_operations_snapshot(
    connector_id: &str,
    connector: &palyra_connectors::ConnectorStatusSnapshot,
    runtime: Option<&Value>,
    queue: &palyra_connectors::ConnectorQueueSnapshot,
    recent_dead_letters: &[palyra_connectors::DeadLetterRecord],
) -> Value {
    let last_runtime_error = runtime
        .and_then(|payload| payload.get("last_error"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    let runtime_global_retry_after_ms = runtime
        .and_then(|payload| payload.get("global_retry_after_ms"))
        .and_then(Value::as_i64)
        .filter(|value| *value > 0);
    let active_route_limits = runtime
        .and_then(|payload| payload.get("route_rate_limits"))
        .and_then(Value::as_array)
        .map(|entries| {
            entries
                .iter()
                .filter(|entry| {
                    entry
                        .get("retry_after_ms")
                        .and_then(Value::as_i64)
                        .is_some_and(|value| value > 0)
                })
                .count()
        })
        .unwrap_or(0);
    let last_permission_failure = find_matching_message(
        [
            connector.last_error.as_deref(),
            last_runtime_error.as_deref(),
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
    let last_auth_failure = find_matching_message(
        [
            connector.last_error.as_deref(),
            last_runtime_error.as_deref(),
            recent_dead_letters.first().map(|entry| entry.reason.as_str()),
        ],
        &["auth", "token", "unauthorized", "credential missing", "missing credential"],
    );
    let mut saturation_reasons = Vec::new();
    let saturation_state = if !connector.enabled {
        saturation_reasons.push("connector_disabled".to_owned());
        "paused"
    } else if queue.paused {
        saturation_reasons.push("queue_paused".to_owned());
        if let Some(reason) = queue.pause_reason.as_deref() {
            saturation_reasons.push(format!("pause_reason={reason}"));
        }
        "paused"
    } else if queue.dead_letters > 0 {
        saturation_reasons.push(format!("dead_letters={}", queue.dead_letters));
        "dead_lettered"
    } else if runtime_global_retry_after_ms.is_some() || active_route_limits > 0 {
        if let Some(wait_ms) = runtime_global_retry_after_ms {
            saturation_reasons.push(format!("global_retry_after_ms={wait_ms}"));
        }
        if active_route_limits > 0 {
            saturation_reasons.push(format!("active_route_limits={active_route_limits}"));
        }
        "rate_limited"
    } else if queue.claimed_outbox > 0 || queue.due_outbox > 0 {
        if queue.claimed_outbox > 0 {
            saturation_reasons.push(format!("claimed_outbox={}", queue.claimed_outbox));
        }
        if queue.due_outbox > 0 {
            saturation_reasons.push(format!("due_outbox={}", queue.due_outbox));
        }
        "backpressure"
    } else if queue.pending_outbox > 0 {
        saturation_reasons.push(format!("pending_outbox={}", queue.pending_outbox));
        "retrying"
    } else {
        "healthy"
    };
    if let Some(error) = &connector.last_error {
        saturation_reasons.push(format!("last_error={error}"));
    } else if let Some(error) = &last_runtime_error {
        saturation_reasons.push(format!("runtime_error={error}"));
    }
    let discord = if connector.kind == palyra_connectors::ConnectorKind::Discord {
        json!({
            "required_permissions": discord_required_permission_labels(),
            "last_permission_failure": last_permission_failure,
            "exact_gap_check_available": true,
            "health_refresh_hint": format!(
                "Run channel health refresh for '{}' with verify_channel_id to confirm channel-specific Discord permission gaps.",
                connector_id
            ),
        })
    } else {
        Value::Null
    };
    json!({
        "queue": {
            "pending_outbox": queue.pending_outbox,
            "due_outbox": queue.due_outbox,
            "claimed_outbox": queue.claimed_outbox,
            "dead_letters": queue.dead_letters,
            "paused": queue.paused,
            "pause_reason": queue.pause_reason,
            "pause_updated_at_unix_ms": queue.pause_updated_at_unix_ms,
            "next_attempt_unix_ms": queue.next_attempt_unix_ms,
            "oldest_pending_created_at_unix_ms": queue.oldest_pending_created_at_unix_ms,
            "latest_dead_letter_unix_ms": queue.latest_dead_letter_unix_ms,
        },
        "saturation": {
            "state": saturation_state,
            "reasons": saturation_reasons,
        },
        "last_auth_failure": last_auth_failure,
        "rate_limits": {
            "global_retry_after_ms": runtime_global_retry_after_ms,
            "active_route_limits": active_route_limits,
            "routes": runtime.and_then(|payload| payload.get("route_rate_limits")).cloned(),
        },
        "discord": discord,
    })
}

pub(crate) fn find_matching_message<'a, I>(messages: I, needles: &[&str]) -> Option<String>
where
    I: IntoIterator<Item = Option<&'a str>>,
{
    messages.into_iter().flatten().find_map(|message| {
        let normalized = message.trim().to_ascii_lowercase();
        if normalized.is_empty() {
            return None;
        }
        if needles.iter().any(|needle| normalized.contains(needle)) {
            Some(sanitize_http_error_message(message.trim()))
        } else {
            None
        }
    })
}

fn discord_account_id_from_connector_id(connector_id: &str) -> Option<&str> {
    connector_id.trim().strip_prefix("discord:").map(str::trim).filter(|value| !value.is_empty())
}

fn resolve_discord_connector_token(state: &AppState, connector_id: &str) -> Result<String, String> {
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
