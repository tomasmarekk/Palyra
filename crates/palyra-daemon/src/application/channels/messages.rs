use serde_json::{json, Value};

use crate::{app::state::AppState, channels, *};

pub(crate) async fn build_channel_test_payload(
    state: &AppState,
    connector_id: &str,
    payload: ChannelTestRequest,
) -> Result<Value, Response> {
    let ingest = state
        .channels
        .submit_test_message(
            connector_id,
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
    let status = state.channels.status(connector_id).map_err(channel_platform_error_response)?;
    let runtime =
        state.channels.runtime_snapshot(connector_id).map_err(channel_platform_error_response)?;
    Ok(json!({
        "ingest": ingest,
        "status": status,
        "runtime": runtime,
    }))
}

pub(crate) async fn build_channel_test_send_payload(
    state: &AppState,
    connector_id: &str,
    payload: ChannelTestSendRequest,
) -> Result<Value, Response> {
    let dispatch = state
        .channels
        .submit_discord_test_send(
            connector_id,
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
    let status = state.channels.status(connector_id).map_err(channel_platform_error_response)?;
    let runtime =
        state.channels.runtime_snapshot(connector_id).map_err(channel_platform_error_response)?;
    Ok(json!({
        "dispatch": dispatch,
        "status": status,
        "runtime": runtime,
    }))
}
