//! Connector observability and operations commands: list, status, health
//! refresh, enable/disable, queue control, dead letters, logs, and test
//! ingestion.
//!
//! All handlers call daemon admin endpoints and emit through the pinned
//! `output::channels` renderers.

use anyhow::{Context, Result};
use serde_json::{json, Value};

use crate::{
    args::ChannelProviderArg, client::channels as channels_client,
    output::channels as channels_output,
};

/// Lists all connectors known to the daemon.
///
/// # Errors
/// Fails when the request context cannot be resolved, the endpoint call
/// fails, or output encoding fails.
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

/// Shows status for one connector, or falls back to the list view when no
/// selector was provided.
///
/// # Errors
/// Fails when selector resolution, the endpoint call, or output encoding
/// fails.
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

/// Triggers a daemon-side health refresh for one connector.
///
/// # Errors
/// Fails when selector resolution, the endpoint call, or output encoding
/// fails.
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

/// Enables or disables a connector through the shared `/enabled` action.
///
/// # Errors
/// Fails when the endpoint call or output encoding fails.
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

/// Runs a payload-less queue operation (pause/resume/drain) on a connector.
///
/// # Errors
/// Fails when the endpoint call or output encoding fails.
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

/// Replays or discards one dead-lettered outbound message.
///
/// # Errors
/// Fails when the endpoint call or output encoding fails.
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

/// Queries recent connector events and dead letters, fanning out across all
/// connectors when no selector was provided.
///
/// # Errors
/// Fails when selector resolution, any endpoint call, or output encoding
/// fails.
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
    let Some(connector_id) =
        super::common::resolve_optional_connector_selector(connector_id, provider, account_id)?
    else {
        return run_all_logs(url, token, principal, device_id, channel, limit, json_output);
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

/// Lists durable ingress events for a connector.
///
/// # Errors
/// Fails when the endpoint call or output encoding fails.
#[allow(clippy::too_many_arguments)]
pub(super) fn run_ingress_list(
    connector_id: String,
    status: Option<String>,
    limit: Option<usize>,
    url: Option<String>,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
    json_output: bool,
) -> Result<()> {
    let request_context =
        channels_client::resolve_request_context(url, token, principal, device_id, channel)?;
    let endpoint = format!(
        "{}/admin/v1/channels/{}/ingress",
        request_context.base_url.trim_end_matches('/'),
        connector_id,
    );
    let client = channels_client::build_client()?;
    let response = channels_client::send_request(
        client.get(endpoint).query(&query_pairs(status, limit)),
        request_context,
        "failed to call channels ingress list endpoint",
    )?;
    emit_ingress(response, json_output)
}

/// Shows one durable ingress event.
///
/// # Errors
/// Fails when the endpoint call or output encoding fails.
#[allow(clippy::too_many_arguments)]
pub(super) fn run_ingress_show(
    connector_id: String,
    ingress_event_id: i64,
    url: Option<String>,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
    json_output: bool,
) -> Result<()> {
    let request_context =
        channels_client::resolve_request_context(url, token, principal, device_id, channel)?;
    let endpoint = format!(
        "{}/admin/v1/channels/{}/ingress/{}",
        request_context.base_url.trim_end_matches('/'),
        connector_id,
        ingress_event_id,
    );
    let client = channels_client::build_client()?;
    let response = channels_client::send_request(
        client.get(endpoint),
        request_context,
        "failed to call channels ingress show endpoint",
    )?;
    emit_ingress(response, json_output)
}

/// Lists delivery intents for a connector.
///
/// # Errors
/// Fails when the endpoint call or output encoding fails.
#[allow(clippy::too_many_arguments)]
pub(super) fn run_delivery_list(
    connector_id: String,
    status: Option<String>,
    limit: Option<usize>,
    url: Option<String>,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
    json_output: bool,
) -> Result<()> {
    let request_context =
        channels_client::resolve_request_context(url, token, principal, device_id, channel)?;
    let endpoint = format!(
        "{}/admin/v1/channels/{}/delivery",
        request_context.base_url.trim_end_matches('/'),
        connector_id,
    );
    let client = channels_client::build_client()?;
    let response = channels_client::send_request(
        client.get(endpoint).query(&query_pairs(status, limit)),
        request_context,
        "failed to call channels delivery list endpoint",
    )?;
    emit_delivery(response, json_output)
}

/// Shows one delivery intent.
///
/// # Errors
/// Fails when the endpoint call or output encoding fails.
#[allow(clippy::too_many_arguments)]
pub(super) fn run_delivery_show(
    intent_id: String,
    url: Option<String>,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
    json_output: bool,
) -> Result<()> {
    let request_context =
        channels_client::resolve_request_context(url, token, principal, device_id, channel)?;
    let endpoint = format!(
        "{}/admin/v1/channels/delivery/{}",
        request_context.base_url.trim_end_matches('/'),
        intent_id,
    );
    let client = channels_client::build_client()?;
    let response = channels_client::send_request(
        client.get(endpoint),
        request_context,
        "failed to call channels delivery show endpoint",
    )?;
    emit_delivery(response, json_output)
}

/// Retries one delivery intent from a safe retry state.
///
/// # Errors
/// Fails when the endpoint call or output encoding fails.
#[allow(clippy::too_many_arguments)]
pub(super) fn run_delivery_retry(
    intent_id: String,
    url: Option<String>,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
    json_output: bool,
) -> Result<()> {
    let request_context =
        channels_client::resolve_request_context(url, token, principal, device_id, channel)?;
    let endpoint = format!(
        "{}/admin/v1/channels/delivery/{}/retry",
        request_context.base_url.trim_end_matches('/'),
        intent_id,
    );
    let client = channels_client::build_client()?;
    let response = channels_client::send_request(
        client.post(endpoint),
        request_context,
        "failed to call channels delivery retry endpoint",
    )?;
    emit_delivery(response, json_output)
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
    let mut events = Vec::<Value>::new();
    let mut dead_letters = Vec::<Value>::new();
    let mut connector_logs = Vec::<Value>::new();
    for connector in list_response.get("connectors").and_then(Value::as_array).into_iter().flatten()
    {
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

/// Injects a synthetic inbound message into a connector for end-to-end
/// pipeline testing.
///
/// # Errors
/// Fails when the endpoint call or output encoding fails.
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

fn emit_ingress(response: Value, json_output: bool) -> Result<()> {
    if json_output {
        println!(
            "{}",
            serde_json::to_string_pretty(&response)
                .context("failed to encode channels ingress payload as JSON")?
        );
    } else if let Some(events) = response.get("ingress_events").and_then(Value::as_array) {
        println!("channels.ingress count={}", events.len());
        for event in events {
            let ingress_event_id =
                event.get("ingress_event_id").and_then(Value::as_i64).unwrap_or(0);
            let connector_id =
                event.get("connector_id").and_then(Value::as_str).unwrap_or("unknown");
            let status = event.get("status").and_then(Value::as_str).unwrap_or("unknown");
            let attempts = event.get("attempts").and_then(Value::as_u64).unwrap_or(0);
            let envelope_id = event.get("envelope_id").and_then(Value::as_str).unwrap_or("unknown");
            println!(
                "channels.ingress.event id={} connector_id={} status={} attempts={} envelope_id={}",
                ingress_event_id, connector_id, status, attempts, envelope_id
            );
        }
    } else {
        let event = response.get("ingress_event").unwrap_or(&response);
        let ingress_event_id = event.get("ingress_event_id").and_then(Value::as_i64).unwrap_or(0);
        let connector_id = event.get("connector_id").and_then(Value::as_str).unwrap_or("unknown");
        let status = event.get("status").and_then(Value::as_str).unwrap_or("unknown");
        let payload_hash = event.get("payload_hash").and_then(Value::as_str).unwrap_or("unknown");
        println!(
            "channels.ingress.show id={} connector_id={} status={} payload_hash={}",
            ingress_event_id, connector_id, status, payload_hash
        );
    }
    Ok(())
}

fn emit_delivery(response: Value, json_output: bool) -> Result<()> {
    if json_output {
        println!(
            "{}",
            serde_json::to_string_pretty(&response)
                .context("failed to encode channels delivery payload as JSON")?
        );
    } else if let Some(intents) = response.get("delivery_intents").and_then(Value::as_array) {
        println!("channels.delivery count={}", intents.len());
        for intent in intents {
            println!("{}", render_delivery_intent_line("channels.delivery.intent", intent));
        }
    } else if let Some(retry) = response.get("retry") {
        let requeued = retry.get("requeued").and_then(Value::as_bool).unwrap_or(false);
        let intent = retry.get("intent").unwrap_or(retry);
        println!(
            "{} requeued={}",
            render_delivery_intent_line("channels.delivery.retry", intent),
            requeued
        );
    } else {
        let intent = response.get("delivery_intent").unwrap_or(&response);
        println!("{}", render_delivery_intent_line("channels.delivery.show", intent));
    }
    Ok(())
}

fn render_delivery_intent_line(prefix: &str, intent: &Value) -> String {
    let intent_id = intent.get("intent_id").and_then(Value::as_str).unwrap_or("unknown");
    let connector_id = intent.get("connector_id").and_then(Value::as_str).unwrap_or("unknown");
    let status = intent.get("status").and_then(Value::as_str).unwrap_or("unknown");
    let send_attempts = intent.get("send_attempts").and_then(Value::as_u64).unwrap_or(0);
    let outbox_envelope_id =
        intent.get("outbox_envelope_id").and_then(Value::as_str).unwrap_or("unknown");
    format!(
        "{} id={} connector_id={} status={} send_attempts={} outbox_envelope_id={}",
        prefix, intent_id, connector_id, status, send_attempts, outbox_envelope_id
    )
}

fn query_pairs(status: Option<String>, limit: Option<usize>) -> Vec<(String, String)> {
    let mut pairs = Vec::new();
    if let Some(status) = status {
        pairs.push(("status".to_owned(), status));
    }
    if let Some(limit) = limit {
        pairs.push(("limit".to_owned(), limit.to_string()));
    }
    pairs
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
