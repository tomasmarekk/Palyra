pub(crate) mod connectors;

use crate::transport::http::handlers::admin::channels::{
    build_channel_health_refresh_payload, build_channel_status_payload,
};
use crate::*;

pub(crate) async fn console_channels_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let connectors = state.channels.list().map_err(channel_platform_error_response)?;
    Ok(Json(json!({
        "connectors": connectors,
        "page": build_page_info(connectors.len().max(1), connectors.len(), None),
    })))
}

pub(crate) async fn console_channel_status_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(connector_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let connector_id = normalize_non_empty_field(connector_id, "connector_id")?;
    Ok(Json(build_channel_status_payload(&state, connector_id.as_str())?))
}

pub(crate) async fn console_channel_set_enabled_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(connector_id): Path<String>,
    Json(payload): Json<ChannelEnabledRequest>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, true)?;
    let connector_id = normalize_non_empty_field(connector_id, "connector_id")?;
    let connector = state
        .channels
        .set_enabled(connector_id.as_str(), payload.enabled)
        .map_err(channel_platform_error_response)?;
    Ok(Json(json!({ "connector": connector })))
}

pub(crate) async fn console_channel_logs_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(connector_id): Path<String>,
    Query(query): Query<ChannelLogsQuery>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let limit = query.limit.unwrap_or(100);
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
        "page": build_page_info(
            limit.max(1),
            events.len().max(dead_letters.len()),
            None
        ),
    })))
}

pub(crate) async fn console_channel_health_refresh_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(connector_id): Path<String>,
    Json(payload): Json<ChannelHealthRefreshRequest>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, true)?;
    let connector_id = normalize_non_empty_field(connector_id, "connector_id")?;
    let payload = build_channel_health_refresh_payload(
        &state,
        connector_id.as_str(),
        payload.verify_channel_id,
    )
    .await?;
    Ok(Json(payload))
}

pub(crate) async fn console_channel_queue_pause_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(connector_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, true)?;
    let connector_id = normalize_non_empty_field(connector_id, "connector_id")?;
    let queue = state
        .channels
        .set_queue_paused(
            connector_id.as_str(),
            true,
            Some("operator requested queue pause via console"),
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

pub(crate) async fn console_channel_queue_resume_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(connector_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, true)?;
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

pub(crate) async fn console_channel_queue_drain_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(connector_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, true)?;
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

pub(crate) async fn console_channel_dead_letter_replay_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(path): Path<DeadLetterActionPath>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, true)?;
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

pub(crate) async fn console_channel_dead_letter_discard_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(path): Path<DeadLetterActionPath>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, true)?;
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

pub(crate) async fn console_channel_test_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(connector_id): Path<String>,
    Json(payload): Json<ChannelTestRequest>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, true)?;
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

pub(crate) async fn console_channel_test_send_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(connector_id): Path<String>,
    Json(payload): Json<ChannelTestSendRequest>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, true)?;
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
                reply_to_message_id: payload.reply_to_message_id,
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

pub(crate) async fn console_channel_router_rules_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let config = state.runtime.channel_router_config_snapshot();
    let config_hash = state.runtime.channel_router_config_hash();
    Ok(Json(json!({
        "config": config,
        "config_hash": config_hash,
    })))
}

pub(crate) async fn console_channel_router_warnings_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    Ok(Json(json!({
        "warnings": state.runtime.channel_router_validation_warnings(),
        "config_hash": state.runtime.channel_router_config_hash(),
    })))
}

pub(crate) async fn console_channel_router_preview_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ChannelRouterPreviewRequest>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, true)?;
    let preview_input = build_channel_router_preview_input(payload)?;
    let preview = state.runtime.channel_router_preview(&preview_input);
    Ok(Json(json!({ "preview": preview })))
}

pub(crate) async fn console_channel_router_pairings_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ChannelRouterPairingsQuery>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let channel = query.channel.as_deref().map(str::trim).filter(|value| !value.is_empty());
    Ok(Json(json!({
        "pairings": state.runtime.channel_router_pairing_snapshot(channel),
        "config_hash": state.runtime.channel_router_config_hash(),
    })))
}

pub(crate) async fn console_channel_router_pairing_code_mint_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ChannelRouterPairingCodeMintRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let channel = normalize_non_empty_field(payload.channel, "channel")?;
    let issued_by = payload
        .issued_by
        .unwrap_or_else(|| format!("{}@{}", session.context.principal, session.context.device_id));
    let code = state
        .runtime
        .channel_router_mint_pairing_code(channel.as_str(), issued_by.as_str(), payload.ttl_ms)
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "code": code,
        "config_hash": state.runtime.channel_router_config_hash(),
    })))
}

#[allow(clippy::result_large_err)]
pub(crate) fn build_channel_router_preview_input(
    payload: ChannelRouterPreviewRequest,
) -> Result<channel_router::InboundMessage, Response> {
    let channel = normalize_non_empty_field(payload.channel, "channel")?;
    if payload.text.trim().is_empty() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "text cannot be empty for routing preview",
        )));
    }
    Ok(channel_router::InboundMessage {
        envelope_id: Ulid::new().to_string(),
        channel,
        conversation_id: payload.conversation_id.and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_owned())
            }
        }),
        sender_handle: payload.sender_identity.and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_owned())
            }
        }),
        sender_display: payload.sender_display.and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_owned())
            }
        }),
        sender_verified: payload.sender_verified.unwrap_or(false),
        text: payload.text,
        max_payload_bytes: payload.max_payload_bytes.unwrap_or(64 * 1024),
        is_direct_message: payload.is_direct_message.unwrap_or(false),
        requested_broadcast: payload.requested_broadcast.unwrap_or(false),
        adapter_message_id: payload.adapter_message_id.and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_owned())
            }
        }),
        adapter_thread_id: payload.adapter_thread_id.and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_owned())
            }
        }),
        retry_attempt: 0,
    })
}
