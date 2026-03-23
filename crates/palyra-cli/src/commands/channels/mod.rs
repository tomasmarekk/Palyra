mod router;

pub(crate) mod connectors {
    pub(crate) mod discord;
}

use anyhow::{Context, Result};
use serde_json::{json, Value};
use std::io::Write;

use crate::{
    args::ChannelsCommand, client::channels as channels_client, output, output::channels as channels_output,
};

pub(crate) fn run(command: ChannelsCommand) -> Result<()> {
    match command {
        ChannelsCommand::Discord { command } => connectors::discord::run(command)?,
        ChannelsCommand::Router { command } => router::run(command)?,
        ChannelsCommand::List { url, token, principal, device_id, channel, json } => {
            let request_context =
                channels_client::resolve_request_context(url, token, principal, device_id, channel)?;
            let endpoint =
                format!("{}/admin/v1/channels", request_context.base_url.trim_end_matches('/'));
            let client = channels_client::build_client()?;
            let response = channels_client::send_request(
                client.get(endpoint),
                request_context,
                "failed to call channels list endpoint",
            )?;
            channels_output::emit_list(response, output::preferred_json(json))?;
        }
        ChannelsCommand::Status {
            connector_id,
            url,
            token,
            principal,
            device_id,
            channel,
            json,
        } => {
            let response = get_connector_status(
                connector_id.as_str(),
                url,
                token,
                principal,
                device_id,
                channel,
                "failed to call channels status endpoint",
            )?;
            channels_output::emit_status(response, output::preferred_json(json))?;
        }
        ChannelsCommand::HealthRefresh {
            connector_id,
            verify_channel_id,
            url,
            token,
            principal,
            device_id,
            channel,
            json,
        } => {
            let response = post_connector_action(
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
            channels_output::emit_status(response, output::preferred_json(json))?;
        }
        ChannelsCommand::Enable {
            connector_id,
            url,
            token,
            principal,
            device_id,
            channel,
            json,
        } => {
            let response = post_connector_action(
                connector_id.as_str(),
                "/enabled",
                Some(json!({ "enabled": true })),
                url,
                token,
                principal,
                device_id,
                channel,
                "failed to call channels enable endpoint",
            )?;
            channels_output::emit_status(response, output::preferred_json(json))?;
        }
        ChannelsCommand::QueuePause {
            connector_id,
            url,
            token,
            principal,
            device_id,
            channel,
            json,
        } => {
            let response = post_connector_action(
                connector_id.as_str(),
                "/operations/queue/pause",
                None,
                url,
                token,
                principal,
                device_id,
                channel,
                "failed to call channels queue pause endpoint",
            )?;
            channels_output::emit_status(response, output::preferred_json(json))?;
        }
        ChannelsCommand::QueueResume {
            connector_id,
            url,
            token,
            principal,
            device_id,
            channel,
            json,
        } => {
            let response = post_connector_action(
                connector_id.as_str(),
                "/operations/queue/resume",
                None,
                url,
                token,
                principal,
                device_id,
                channel,
                "failed to call channels queue resume endpoint",
            )?;
            channels_output::emit_status(response, output::preferred_json(json))?;
        }
        ChannelsCommand::QueueDrain {
            connector_id,
            url,
            token,
            principal,
            device_id,
            channel,
            json,
        } => {
            let response = post_connector_action(
                connector_id.as_str(),
                "/operations/queue/drain",
                None,
                url,
                token,
                principal,
                device_id,
                channel,
                "failed to call channels queue drain endpoint",
            )?;
            channels_output::emit_status(response, output::preferred_json(json))?;
        }
        ChannelsCommand::DeadLetterReplay {
            connector_id,
            dead_letter_id,
            url,
            token,
            principal,
            device_id,
            channel,
            json,
        } => {
            let action = format!("/operations/dead-letters/{dead_letter_id}/replay");
            let response = post_connector_action(
                connector_id.as_str(),
                action.as_str(),
                None,
                url,
                token,
                principal,
                device_id,
                channel,
                "failed to call channels dead-letter replay endpoint",
            )?;
            channels_output::emit_status(response, output::preferred_json(json))?;
        }
        ChannelsCommand::DeadLetterDiscard {
            connector_id,
            dead_letter_id,
            url,
            token,
            principal,
            device_id,
            channel,
            json,
        } => {
            let action = format!("/operations/dead-letters/{dead_letter_id}/discard");
            let response = post_connector_action(
                connector_id.as_str(),
                action.as_str(),
                None,
                url,
                token,
                principal,
                device_id,
                channel,
                "failed to call channels dead-letter discard endpoint",
            )?;
            channels_output::emit_status(response, output::preferred_json(json))?;
        }
        ChannelsCommand::Disable {
            connector_id,
            url,
            token,
            principal,
            device_id,
            channel,
            json,
        } => {
            let response = post_connector_action(
                connector_id.as_str(),
                "/enabled",
                Some(json!({ "enabled": false })),
                url,
                token,
                principal,
                device_id,
                channel,
                "failed to call channels disable endpoint",
            )?;
            channels_output::emit_status(response, output::preferred_json(json))?;
        }
        ChannelsCommand::Logs {
            connector_id,
            url,
            token,
            principal,
            device_id,
            channel,
            limit,
            json,
        } => {
            let request_context =
                channels_client::resolve_request_context(url, token, principal, device_id, channel)?;
            let endpoint = format!(
                "{}/admin/v1/channels/{}/logs",
                request_context.base_url.trim_end_matches('/'),
                connector_id
            );
            let client = channels_client::build_client()?;
            let mut request = client.get(endpoint);
            if let Some(limit) = limit {
                request = request.query(&[("limit", limit)]);
            }
            let response = channels_client::send_request(
                request,
                request_context,
                "failed to call channels logs endpoint",
            )?;
            emit_logs(connector_id.as_str(), response, output::preferred_json(json))?;
        }
        ChannelsCommand::Test {
            connector_id,
            text,
            url,
            token,
            principal,
            device_id,
            channel,
            conversation_id,
            sender_id,
            sender_display,
            simulate_crash_once,
            is_direct_message,
            requested_broadcast,
            json,
        } => {
            let response = post_connector_action(
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
            emit_test(connector_id.as_str(), response, output::preferred_json(json))?;
        }
    }
    std::io::stdout().flush().context("stdout flush failed")
}

pub(super) fn get_connector_status(
    connector_id: &str,
    url: Option<String>,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
    error_context: &'static str,
) -> Result<Value> {
    let request_context =
        channels_client::resolve_request_context(url, token, principal, device_id, channel)?;
    let endpoint = format!(
        "{}/admin/v1/channels/{}",
        request_context.base_url.trim_end_matches('/'),
        connector_id
    );
    let client = channels_client::build_client()?;
    channels_client::send_request(client.get(endpoint), request_context, error_context)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn post_connector_action(
    connector_id: &str,
    action_suffix: &str,
    payload: Option<Value>,
    url: Option<String>,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
    error_context: &'static str,
) -> Result<Value> {
    let request_context =
        channels_client::resolve_request_context(url, token, principal, device_id, channel)?;
    let endpoint = format!(
        "{}/admin/v1/channels/{}{}",
        request_context.base_url.trim_end_matches('/'),
        connector_id,
        action_suffix
    );
    let client = channels_client::build_client()?;
    let request = if let Some(payload) = payload {
        client.post(endpoint).json(&payload)
    } else {
        client.post(endpoint)
    };
    channels_client::send_request(request, request_context, error_context)
}

fn emit_logs(connector_id: &str, response: Value, json_output: bool) -> Result<()> {
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
        println!(
            "channels.logs connector_id={} events={} dead_letters={}",
            connector_id, events, dead_letters
        );
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
