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
    match super::common::resolve_optional_connector_selector(connector_id, provider, account_id)? {
        Some(connector_id) => {
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
        None => run_list(url, token, principal, device_id, channel, json_output),
    }
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
    let connector_id = match super::common::resolve_optional_connector_selector(
        connector_id,
        provider,
        account_id,
    )? {
        Some(connector_id) => connector_id,
        None => {
            return run_all_logs(url, token, principal, device_id, channel, limit, json_output);
        }
    };
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
fn run_all_logs(
    url: Option<String>,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
    limit: Option<usize>,
    json_output: bool,
) -> Result<()> {
    let request_context =
        channels_client::resolve_request_context(url, token, principal, device_id, channel)?;
    let client = channels_client::build_client()?;
    let list_endpoint =
        format!("{}/admin/v1/channels", request_context.base_url.trim_end_matches('/'),);
    let list_response = channels_client::send_request(
        client.get(list_endpoint),
        request_context.clone(),
        "failed to call channels list endpoint",
    )?;
    let connectors =
        list_response.get("connectors").and_then(Value::as_array).cloned().unwrap_or_default();
    let mut events = Vec::<Value>::new();
    let mut dead_letters = Vec::<Value>::new();
    let mut connector_logs = Vec::<Value>::new();
    for connector in connectors {
        let Some(connector_id) =
            connector.get("connector_id").and_then(Value::as_str).filter(|value| !value.is_empty())
        else {
            continue;
        };
        let logs_endpoint = format!(
            "{}/admin/v1/channels/logs/query",
            request_context.base_url.trim_end_matches('/'),
        );
        let request = client.post(logs_endpoint).json(&json!({
            "connector_id": connector_id,
            "limit": limit,
        }));
        let response = channels_client::send_request(
            request,
            request_context.clone(),
            "failed to call channels logs endpoint",
        )?;
        let connector_events =
            response.get("events").and_then(Value::as_array).cloned().unwrap_or_default();
        let connector_dead_letters =
            response.get("dead_letters").and_then(Value::as_array).cloned().unwrap_or_default();
        events.extend(connector_events.iter().cloned());
        dead_letters.extend(connector_dead_letters.iter().cloned());
        connector_logs.push(json!({
            "connector_id": connector_id,
            "events": connector_events,
            "dead_letters": connector_dead_letters,
        }));
    }
    emit_logs(
        json!({
            "scope": "all",
            "connector_count": connector_logs.len(),
            "events": events,
            "dead_letters": dead_letters,
            "connectors": connector_logs,
        }),
        json_output,
    )
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
        println!("{}", render_logs_summary_line(&response));
    }
    Ok(())
}

fn render_logs_summary_line(response: &Value) -> String {
    let connector_count = response.get("connector_count").and_then(Value::as_u64);
    let events =
        response.get("events").and_then(Value::as_array).map(|items| items.len()).unwrap_or(0);
    let dead_letters = response
        .get("dead_letters")
        .and_then(Value::as_array)
        .map(|items| items.len())
        .unwrap_or(0);
    if let Some(connector_count) = connector_count {
        format!(
            "channels.logs scope=all connectors={} events={} dead_letters={}",
            connector_count, events, dead_letters
        )
    } else {
        format!("channels.logs events={} dead_letters={}", events, dead_letters)
    }
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::render_logs_summary_line;

    #[test]
    fn render_logs_summary_line_includes_global_scope_when_fanned_out() {
        let line = render_logs_summary_line(&json!({
            "connector_count": 2,
            "events": [{ "connector_id": "discord:default" }],
            "dead_letters": [],
        }));

        assert_eq!(line, "channels.logs scope=all connectors=2 events=1 dead_letters=0");
    }

    #[test]
    fn render_logs_summary_line_preserves_single_connector_shape() {
        let line = render_logs_summary_line(&json!({
            "events": [],
            "dead_letters": [{ "connector_id": "discord:default" }],
        }));

        assert_eq!(line, "channels.logs events=0 dead_letters=1");
    }
}
