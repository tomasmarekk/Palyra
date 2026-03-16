pub(crate) mod connectors;

use crate::transport::http::handlers::console::channels::build_channel_router_preview_input;
use crate::transport::http::handlers::console::channels::connectors::discord::{
    build_discord_channel_permission_warnings, build_discord_inbound_monitor_warnings,
    discord_inbound_monitor_is_alive, load_discord_inbound_monitor_summary,
    normalize_optional_discord_channel_id, probe_discord_bot_identity,
};
use crate::*;

pub(crate) async fn admin_channels_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let connectors = state.channels.list().map_err(channel_platform_error_response)?;
    Ok(Json(json!({ "connectors": connectors })))
}

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

fn find_matching_message<'a, I>(messages: I, needles: &[&str]) -> Option<String>
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

pub(crate) async fn admin_channel_status_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(connector_id): Path<String>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let connector_id = normalize_non_empty_field(connector_id, "connector_id")?;
    Ok(Json(build_channel_status_payload(&state, connector_id.as_str())?))
}

pub(crate) async fn admin_channel_set_enabled_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(connector_id): Path<String>,
    Json(payload): Json<ChannelEnabledRequest>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let connector_id = normalize_non_empty_field(connector_id, "connector_id")?;
    let connector = state
        .channels
        .set_enabled(connector_id.as_str(), payload.enabled)
        .map_err(channel_platform_error_response)?;
    Ok(Json(json!({ "connector": connector })))
}

pub(crate) async fn admin_channel_logs_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(connector_id): Path<String>,
    Query(query): Query<ChannelLogsQuery>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let connector_id = normalize_non_empty_field(connector_id, "connector_id")?;
    let events = state
        .channels
        .logs(connector_id.as_str(), query.limit)
        .map_err(channel_platform_error_response)?;
    let dead_letters = state
        .channels
        .dead_letters(connector_id.as_str(), query.limit)
        .map_err(channel_platform_error_response)?;
    Ok(Json(json!({
        "events": events,
        "dead_letters": dead_letters,
    })))
}

pub(crate) async fn admin_channel_health_refresh_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(connector_id): Path<String>,
    Json(payload): Json<ChannelHealthRefreshRequest>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let connector_id = normalize_non_empty_field(connector_id, "connector_id")?;
    let payload = build_channel_health_refresh_payload(
        &state,
        connector_id.as_str(),
        payload.verify_channel_id,
    )
    .await?;
    Ok(Json(payload))
}

pub(crate) async fn admin_channel_queue_pause_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(connector_id): Path<String>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let connector_id = normalize_non_empty_field(connector_id, "connector_id")?;
    let queue = state
        .channels
        .set_queue_paused(
            connector_id.as_str(),
            true,
            Some("operator requested queue pause via admin API"),
        )
        .map_err(channel_platform_error_response)?;
    let mut payload = build_channel_status_payload(&state, connector_id.as_str())?;
    payload["action"] = json!({
        "type": "queue_pause",
        "message": format!("queue paused for connector '{}'", connector_id),
        "queue": queue,
    });
    Ok(Json(payload))
}

pub(crate) async fn admin_channel_queue_resume_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(connector_id): Path<String>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let connector_id = normalize_non_empty_field(connector_id, "connector_id")?;
    let queue = state
        .channels
        .set_queue_paused(connector_id.as_str(), false, None)
        .map_err(channel_platform_error_response)?;
    let mut payload = build_channel_status_payload(&state, connector_id.as_str())?;
    payload["action"] = json!({
        "type": "queue_resume",
        "message": format!("queue resumed for connector '{}'", connector_id),
        "queue": queue,
    });
    Ok(Json(payload))
}

pub(crate) async fn admin_channel_queue_drain_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(connector_id): Path<String>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let connector_id = normalize_non_empty_field(connector_id, "connector_id")?;
    let drain = state
        .channels
        .drain_due_for_connector(connector_id.as_str())
        .await
        .map_err(channel_platform_error_response)?;
    let mut payload = build_channel_status_payload(&state, connector_id.as_str())?;
    payload["action"] = json!({
        "type": "queue_drain",
        "message": format!("queue drain completed for connector '{}'", connector_id),
        "drain": drain,
    });
    Ok(Json(payload))
}

pub(crate) async fn admin_channel_dead_letter_replay_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(path): Path<DeadLetterActionPath>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let connector_id = normalize_non_empty_field(path.connector_id, "connector_id")?;
    let replayed = state
        .channels
        .replay_dead_letter(connector_id.as_str(), path.dead_letter_id)
        .map_err(channel_platform_error_response)?;
    let mut payload = build_channel_status_payload(&state, connector_id.as_str())?;
    payload["action"] = json!({
        "type": "dead_letter_replay",
        "message": format!(
            "dead-letter {} replayed for connector '{}'",
            path.dead_letter_id, connector_id
        ),
        "dead_letter": replayed,
    });
    Ok(Json(payload))
}

pub(crate) async fn admin_channel_dead_letter_discard_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(path): Path<DeadLetterActionPath>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let connector_id = normalize_non_empty_field(path.connector_id, "connector_id")?;
    let discarded = state
        .channels
        .discard_dead_letter(connector_id.as_str(), path.dead_letter_id)
        .map_err(channel_platform_error_response)?;
    let mut payload = build_channel_status_payload(&state, connector_id.as_str())?;
    payload["action"] = json!({
        "type": "dead_letter_discard",
        "message": format!(
            "dead-letter {} discarded for connector '{}'",
            path.dead_letter_id, connector_id
        ),
        "dead_letter": discarded,
    });
    Ok(Json(payload))
}

pub(crate) async fn admin_channel_test_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(connector_id): Path<String>,
    Json(payload): Json<ChannelTestRequest>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let connector_id = normalize_non_empty_field(connector_id, "connector_id")?;
    let ingest = state
        .channels
        .submit_test_message(
            connector_id.as_str(),
            channels::ChannelTestMessageRequest {
                text: payload.text,
                conversation_id: payload
                    .conversation_id
                    .unwrap_or_else(|| "test:conversation".to_owned()),
                sender_id: payload.sender_id.unwrap_or_else(|| "test-user".to_owned()),
                sender_display: payload.sender_display,
                simulate_crash_once: payload.simulate_crash_once.unwrap_or(false),
                is_direct_message: payload.is_direct_message.unwrap_or(true),
                requested_broadcast: payload.requested_broadcast.unwrap_or(false),
            },
        )
        .await
        .map_err(channel_platform_error_response)?;
    let status =
        state.channels.status(connector_id.as_str()).map_err(channel_platform_error_response)?;
    let runtime = state
        .channels
        .runtime_snapshot(connector_id.as_str())
        .map_err(channel_platform_error_response)?;
    Ok(Json(json!({
        "ingest": ingest,
        "status": status,
        "runtime": runtime,
    })))
}

pub(crate) async fn admin_channel_test_send_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(connector_id): Path<String>,
    Json(payload): Json<ChannelTestSendRequest>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let connector_id = normalize_non_empty_field(connector_id, "connector_id")?;
    let dispatch = state
        .channels
        .submit_discord_test_send(
            connector_id.as_str(),
            channels::ChannelDiscordTestSendRequest {
                target: payload.target,
                text: payload.text.unwrap_or_else(|| "palyra discord test message".to_owned()),
                confirm: payload.confirm.unwrap_or(false),
                auto_reaction: payload.auto_reaction,
                thread_id: payload.thread_id,
            },
        )
        .await
        .map_err(channel_platform_error_response)?;
    let status =
        state.channels.status(connector_id.as_str()).map_err(channel_platform_error_response)?;
    let runtime = state
        .channels
        .runtime_snapshot(connector_id.as_str())
        .map_err(channel_platform_error_response)?;
    Ok(Json(json!({
        "dispatch": dispatch,
        "status": status,
        "runtime": runtime,
    })))
}

pub(crate) async fn admin_channel_router_rules_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let config = state.runtime.channel_router_config_snapshot();
    let config_hash = state.runtime.channel_router_config_hash();
    Ok(Json(json!({
        "config": config,
        "config_hash": config_hash,
    })))
}

pub(crate) async fn admin_channel_router_warnings_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    Ok(Json(json!({
        "warnings": state.runtime.channel_router_validation_warnings(),
        "config_hash": state.runtime.channel_router_config_hash(),
    })))
}

pub(crate) async fn admin_channel_router_preview_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ChannelRouterPreviewRequest>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let preview_input = build_channel_router_preview_input(payload)?;
    let preview = state.runtime.channel_router_preview(&preview_input);
    Ok(Json(json!({ "preview": preview })))
}

pub(crate) async fn admin_channel_router_pairings_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ChannelRouterPairingsQuery>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let channel = query.channel.as_deref().map(str::trim).filter(|value| !value.is_empty());
    Ok(Json(json!({
        "pairings": state.runtime.channel_router_pairing_snapshot(channel),
        "config_hash": state.runtime.channel_router_config_hash(),
    })))
}

pub(crate) async fn admin_channel_router_pairing_code_mint_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ChannelRouterPairingCodeMintRequest>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let channel = normalize_non_empty_field(payload.channel, "channel")?;
    let issued_by =
        payload.issued_by.unwrap_or_else(|| format!("{}@{}", context.principal, context.device_id));
    let code = state
        .runtime
        .channel_router_mint_pairing_code(channel.as_str(), issued_by.as_str(), payload.ttl_ms)
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "code": code,
        "config_hash": state.runtime.channel_router_config_hash(),
    })))
}

#[cfg(test)]
mod tests {
    use super::find_matching_message;

    #[test]
    fn find_matching_message_redacts_secret_like_values() {
        let message = find_matching_message(
            [Some("unauthorized: bearer topsecret token=abc123")],
            &["unauthorized", "token"],
        )
        .expect("matching auth failure should be returned");

        assert!(message.contains("<redacted>"), "matching message should be sanitized: {message}");
        assert!(
            !message.contains("topsecret") && !message.contains("token=abc123"),
            "matching message should not leak sensitive values: {message}"
        );
    }
}
