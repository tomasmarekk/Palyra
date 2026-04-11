use anyhow::{Context, Result};
use serde_json::{json, Value};

use crate::{
    args::ChannelProviderArg, client::channels as channels_client,
    output::channels as channels_output,
};

#[allow(clippy::too_many_arguments)]
pub(super) fn run_list(
    url: Option<String>,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
    json_output: bool,
) -> Result<()> {
    let request_context =
        channels_client::resolve_request_context(url, token, principal, device_id, channel)?;
    let endpoint = format!("{}/admin/v1/channels", request_context.base_url.trim_end_matches('/'));
    let client = channels_client::build_client()?;
    let response = channels_client::send_request(
        client.get(endpoint),
        request_context,
        "failed to call channels list endpoint",
    )?;
    channels_output::emit_list(response, json_output)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn run_status(
    connector_id: Option<String>,
    provider: Option<ChannelProviderArg>,
    account_id: Option<String>,
    url: Option<String>,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
    json_output: bool,
) -> Result<()> {
    let connector_id =
        super::common::resolve_connector_selector(connector_id, provider, account_id)?;
    let response = super::resolve_connector_status(
        connector_id.as_str(),
        url,
        token,
        principal,
        device_id,
        channel,
        "failed to call channels status endpoint",
    )?;
    channels_output::emit_status(response, json_output)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn run_health_refresh(
    connector_id: Option<String>,
    provider: Option<ChannelProviderArg>,
    account_id: Option<String>,
    verify_channel_id: Option<String>,
    url: Option<String>,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
    json_output: bool,
) -> Result<()> {
    let connector_id =
        super::common::resolve_connector_selector(connector_id, provider, account_id)?;
    let response = super::post_connector_action(
        connector_id.as_str(),
        "/operations/health-refresh",
        Some(json!({ "verify_channel_id": verify_channel_id })),
        url,
        token,
        principal,
        device_id,
        channel,
        "failed to call channels health-refresh endpoint",
    )?;
    channels_output::emit_status(response, json_output)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn run_enable_toggle(
    connector_id: String,
    enabled: bool,
    url: Option<String>,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
    json_output: bool,
) -> Result<()> {
    let response = super::post_connector_action(
        connector_id.as_str(),
        "/enabled",
        Some(json!({ "enabled": enabled })),
        url,
        token,
        principal,
        device_id,
        channel,
        if enabled {
            "failed to call channels enable endpoint"
        } else {
            "failed to call channels disable endpoint"
        },
    )?;
    channels_output::emit_status(response, json_output)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn run_queue_action(
    connector_id: String,
    action_suffix: &'static str,
    error_context: &'static str,
    url: Option<String>,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
    json_output: bool,
) -> Result<()> {
    let response = super::post_connector_action(
        connector_id.as_str(),
        action_suffix,
        None,
        url,
        token,
        principal,
        device_id,
        channel,
        error_context,
    )?;
    channels_output::emit_status(response, json_output)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn run_dead_letter_action(
    connector_id: String,
    dead_letter_id: i64,
    action: &'static str,
    url: Option<String>,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
    json_output: bool,
) -> Result<()> {
    let action_suffix = format!("/operations/dead-letters/{dead_letter_id}/{action}");
    let response = super::post_connector_action(
        connector_id.as_str(),
        action_suffix.as_str(),
        None,
        url,
        token,
        principal,
        device_id,
        channel,
        if action == "replay" {
            "failed to call channels dead-letter replay endpoint"
        } else {
            "failed to call channels dead-letter discard endpoint"
        },
    )?;
    channels_output::emit_status(response, json_output)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn run_logs(
    connector_id: Option<String>,
    provider: Option<ChannelProviderArg>,
    account_id: Option<String>,
    url: Option<String>,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
    limit: Option<usize>,
    json_output: bool,
) -> Result<()> {
    let connector_id =
        super::common::resolve_connector_selector(connector_id, provider, account_id)?;
    let request_context =
        channels_client::resolve_request_context(url, token, principal, device_id, channel)?;
    let endpoint =
        format!("{}/admin/v1/channels/logs/query", request_context.base_url.trim_end_matches('/'),);
    let client = channels_client::build_client()?;
    let request = client.post(endpoint).json(&json!({
        "connector_id": connector_id,
        "limit": limit,
    }));
    let response = channels_client::send_request(
        request,
        request_context,
        "failed to call channels logs endpoint",
    )?;
    emit_logs(response, json_output)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn run_test(
    connector_id: String,
    text: String,
    url: Option<String>,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
    conversation_id: Option<String>,
    sender_id: Option<String>,
    sender_display: Option<String>,
    simulate_crash_once: bool,
    is_direct_message: bool,
    requested_broadcast: bool,
    json_output: bool,
) -> Result<()> {
    let response = super::post_connector_action(
        connector_id.as_str(),
        "/test",
        Some(json!({
            "text": text,
            "conversation_id": conversation_id,
            "sender_id": sender_id,
            "sender_display": sender_display,
            "simulate_crash_once": simulate_crash_once,
            "is_direct_message": is_direct_message,
            "requested_broadcast": requested_broadcast,
        })),
        url,
        token,
        principal,
        device_id,
        channel,
        "failed to call channels test endpoint",
    )?;
    emit_test(connector_id.as_str(), response, json_output)
}

fn emit_logs(response: Value, json_output: bool) -> Result<()> {
    if json_output {
        println!(
            "{}",
            serde_json::to_string_pretty(&response)
                .context("failed to encode channels logs payload as JSON")?
        );
    } else {
        let events =
            response.get("events").and_then(Value::as_array).map(|items| items.len()).unwrap_or(0);
        let dead_letters = response
            .get("dead_letters")
            .and_then(Value::as_array)
            .map(|items| items.len())
            .unwrap_or(0);
        println!("channels.logs events={} dead_letters={}", events, dead_letters);
    }
    Ok(())
}

fn emit_test(connector_id: &str, response: Value, json_output: bool) -> Result<()> {
    if json_output {
        println!(
            "{}",
            serde_json::to_string_pretty(&response)
                .context("failed to encode channels test payload as JSON")?
        );
    } else {
        let accepted = response
            .get("ingest")
            .and_then(Value::as_object)
            .and_then(|ingest| ingest.get("accepted"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let immediate_delivery = response
            .get("ingest")
            .and_then(Value::as_object)
            .and_then(|ingest| ingest.get("immediate_delivery"))
            .and_then(Value::as_u64)
            .unwrap_or(0);
        println!(
            "channels.test connector_id={} accepted={} immediate_delivery={}",
            connector_id, accepted, immediate_delivery
        );
    }
    Ok(())
}
