//! Admin channel HTTP handlers.
//!
//! These endpoints expose connector status, queue operations, message
//! inspection, mutation approval flows, and router diagnostics for operators.

pub(crate) mod connectors;
mod message_mutations;

use crate::application::channels::{
    build_channel_health_refresh_payload, build_channel_status_payload, build_channel_test_payload,
    build_channel_test_send_payload,
};
use crate::transport::http::handlers::console::channels::build_channel_router_preview_input;
use crate::*;
pub(crate) use message_mutations::{
    channel_message_delete_response, channel_message_edit_response,
    channel_message_reaction_response, channel_message_read_response,
    channel_message_search_response,
};

/// Lists configured channel connectors for the admin console.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction, or
/// connector enumeration fails.
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

/// Returns status for one configured channel connector.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction,
/// connector-id normalization, or status payload construction fails.
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

/// Enables or disables a channel connector.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction,
/// connector-id normalization, or connector state mutation fails.
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

/// Returns recent channel events and dead letters for a connector.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction,
/// connector-id normalization, or log collection fails.
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
    admin_channel_logs_response(&state, connector_id, query.limit)
}

/// Reads channel messages through the admin API.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction,
/// connector-id normalization, provider access, or audit-event recording fails.
pub(crate) async fn admin_channel_message_read_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(connector_id): Path<String>,
    Json(payload): Json<ChannelMessageReadBody>,
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
    channel_message_read_response(&state, &context, connector_id, payload.request).await
}

/// Searches channel messages through the admin API.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction,
/// connector-id normalization, provider search, or audit-event recording fails.
pub(crate) async fn admin_channel_message_search_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(connector_id): Path<String>,
    Json(payload): Json<ChannelMessageSearchBody>,
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
    channel_message_search_response(&state, &context, connector_id, payload.request).await
}

/// Edits a channel message after policy and approval checks.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction,
/// connector-id normalization, preview lookup, policy evaluation, approval
/// validation, provider mutation, or audit-event recording fails.
pub(crate) async fn admin_channel_message_edit_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(connector_id): Path<String>,
    Json(payload): Json<ChannelMessageEditBody>,
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
    channel_message_edit_response(
        &state,
        &context,
        connector_id,
        payload.request,
        payload.approval_id,
    )
    .await
}

/// Deletes a channel message after policy and approval checks.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction,
/// connector-id normalization, preview lookup, policy evaluation, approval
/// validation, provider mutation, or audit-event recording fails.
pub(crate) async fn admin_channel_message_delete_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(connector_id): Path<String>,
    Json(payload): Json<ChannelMessageDeleteBody>,
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
    channel_message_delete_response(
        &state,
        &context,
        connector_id,
        payload.request,
        payload.approval_id,
    )
    .await
}

/// Adds a reaction to a channel message after policy and approval checks.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction,
/// connector-id normalization, preview lookup, policy evaluation, approval
/// validation, provider mutation, or audit-event recording fails.
pub(crate) async fn admin_channel_message_react_add_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(connector_id): Path<String>,
    Json(payload): Json<ChannelMessageReactionBody>,
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
    channel_message_reaction_response(
        &state,
        &context,
        connector_id,
        payload.request,
        payload.approval_id,
        channels::DiscordMessageMutationKind::ReactAdd,
    )
    .await
}

/// Removes a reaction from a channel message after policy and approval checks.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction,
/// connector-id normalization, preview lookup, policy evaluation, approval
/// validation, provider mutation, or audit-event recording fails.
pub(crate) async fn admin_channel_message_react_remove_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(connector_id): Path<String>,
    Json(payload): Json<ChannelMessageReactionBody>,
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
    channel_message_reaction_response(
        &state,
        &context,
        connector_id,
        payload.request,
        payload.approval_id,
        channels::DiscordMessageMutationKind::ReactRemove,
    )
    .await
}

/// Returns recent channel events and dead letters using a JSON request body.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction,
/// connector-id normalization, or log collection fails.
pub(crate) async fn admin_channel_logs_query_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ChannelLogsRequest>,
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
    admin_channel_logs_response(&state, payload.connector_id, payload.limit)
}

/// Refreshes provider health for a channel connector.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction,
/// connector-id normalization, or provider health refresh fails.
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

#[expect(
    clippy::result_large_err,
    reason = "axum handler helpers return Response directly to preserve transport contracts"
)]
fn admin_channel_logs_response(
    state: &AppState,
    connector_id: String,
    limit: Option<usize>,
) -> Result<Json<Value>, Response> {
    let connector_id = normalize_non_empty_field(connector_id, "connector_id")?;
    let events = state
        .channels
        .logs(connector_id.as_str(), limit)
        .map_err(channel_platform_error_response)?;
    let dead_letters = state
        .channels
        .dead_letters(connector_id.as_str(), limit)
        .map_err(channel_platform_error_response)?;
    Ok(Json(json!({
        "events": events,
        "dead_letters": dead_letters,
    })))
}

/// Pauses outbound queue processing for one connector.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction,
/// connector-id normalization, queue mutation, or status payload construction
/// fails.
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

/// Resumes outbound queue processing for one connector.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction,
/// connector-id normalization, queue mutation, or status payload construction
/// fails.
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

/// Drains due outbound work for one connector.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction,
/// connector-id normalization, queue drain execution, or status payload
/// construction fails.
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

/// Replays one dead-lettered channel event.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction,
/// connector-id normalization, dead-letter replay, or status payload
/// construction fails.
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

/// Discards one dead-lettered channel event.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction,
/// connector-id normalization, dead-letter discard, or status payload
/// construction fails.
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

/// Runs a provider connectivity test for one connector.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction,
/// connector-id normalization, or provider test execution fails.
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
    let response = build_channel_test_payload(&state, connector_id.as_str(), payload).await?;
    Ok(Json(response))
}

/// Sends a provider test message for one connector.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction,
/// connector-id normalization, or provider test-send execution fails.
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
    let response = build_channel_test_send_payload(&state, connector_id.as_str(), payload).await?;
    Ok(Json(response))
}

/// Returns the active channel-router rules and config hash.
///
/// # Errors
/// Returns an error response when admin authorization or context extraction
/// fails.
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

/// Returns channel-router validation warnings and config hash.
///
/// # Errors
/// Returns an error response when admin authorization or context extraction
/// fails.
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

/// Previews channel-router resolution for an operator-supplied message.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction, or
/// preview input validation fails.
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

/// Returns channel-router pairing records, optionally scoped to a channel.
///
/// # Errors
/// Returns an error response when admin authorization or context extraction
/// fails.
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

/// Mints a channel-router pairing code for a target channel.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction,
/// channel normalization, or pairing-code creation fails.
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
    use crate::application::channels::providers::find_matching_message;

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
