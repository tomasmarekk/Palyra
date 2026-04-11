use anyhow::{Context, Result};
use serde_json::{json, Value};
use std::fs;

use crate::{
    args::ChannelProviderArg, client::channels as channels_client,
    output::channels as channels_output,
};

#[allow(clippy::too_many_arguments)]
pub(super) fn run_pairings(
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
    let route_channel =
        super::common::resolve_connector_selector(connector_id, provider, account_id)?;
    let response =
        fetch_router_pairings(Some(route_channel), url, token, principal, device_id, channel)?;
    channels_output::emit_router_pairings(response, json_output)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn run_pairing_code(
    connector_id: Option<String>,
    provider: Option<ChannelProviderArg>,
    account_id: Option<String>,
    issued_by: Option<String>,
    ttl_ms: Option<u64>,
    url: Option<String>,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
    json_output: bool,
) -> Result<()> {
    let route_channel =
        super::common::resolve_connector_selector(connector_id, provider, account_id)?;
    let response = mint_router_pairing_code(
        route_channel,
        issued_by,
        ttl_ms,
        url,
        token,
        principal,
        device_id,
        channel,
    )?;
    channels_output::emit_router_pairing_code(response, json_output)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn run_qr(
    connector_id: Option<String>,
    provider: Option<ChannelProviderArg>,
    account_id: Option<String>,
    issued_by: Option<String>,
    ttl_ms: Option<u64>,
    artifact: Option<String>,
    url: Option<String>,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
    json_output: bool,
) -> Result<()> {
    let route_channel =
        super::common::resolve_connector_selector(connector_id, provider, account_id)?;
    let response = mint_router_pairing_code(
        route_channel.clone(),
        issued_by,
        ttl_ms,
        url,
        token,
        principal,
        device_id,
        channel,
    )?;
    emit_channel_qr(route_channel, response, artifact, json_output)
}

fn fetch_router_pairings(
    route_channel: Option<String>,
    url: Option<String>,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
) -> Result<Value> {
    let request_context =
        channels_client::resolve_request_context(url, token, principal, device_id, channel)?;
    let endpoint = format!(
        "{}/admin/v1/channels/router/pairings",
        request_context.base_url.trim_end_matches('/')
    );
    let client = channels_client::build_client()?;
    let mut request = client.get(endpoint);
    if let Some(route_channel) = route_channel {
        request = request.query(&[("channel", route_channel)]);
    }
    channels_client::send_request(
        request,
        request_context,
        "failed to call channel router pairings endpoint",
    )
}

#[allow(clippy::too_many_arguments)]
fn mint_router_pairing_code(
    route_channel: String,
    issued_by: Option<String>,
    ttl_ms: Option<u64>,
    url: Option<String>,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
) -> Result<Value> {
    let request_context =
        channels_client::resolve_request_context(url, token, principal, device_id, channel)?;
    let endpoint = format!(
        "{}/admin/v1/channels/router/pairing-codes",
        request_context.base_url.trim_end_matches('/')
    );
    let client = channels_client::build_client()?;
    let payload = json!({
        "channel": route_channel,
        "issued_by": issued_by.and_then(crate::normalize_optional_text_arg),
        "ttl_ms": ttl_ms,
    });
    channels_client::send_request(
        client.post(endpoint).json(&payload),
        request_context,
        "failed to call channel router pairing-code mint endpoint",
    )
}

fn emit_channel_qr(
    route_channel: String,
    response: Value,
    artifact: Option<String>,
    json_output: bool,
) -> Result<()> {
    let code = response
        .pointer("/code/code")
        .and_then(Value::as_str)
        .context("pairing code payload did not include code")?;
    let issued_by =
        response.pointer("/code/issued_by").and_then(Value::as_str).unwrap_or("unknown");
    let expires_at =
        response.pointer("/code/expires_at_unix_ms").and_then(Value::as_i64).unwrap_or(0);
    let qr_text = format!("pair {code}");
    if let Some(path) = artifact.as_deref() {
        fs::write(path, format!("{qr_text}\n"))
            .with_context(|| format!("failed to write pairing QR text artifact to {path}"))?;
    }
    let payload = json!({
        "route_channel": route_channel,
        "code": code,
        "issued_by": issued_by,
        "expires_at_unix_ms": expires_at,
        "qr_text": qr_text,
        "artifact": artifact,
        "config_hash": response.get("config_hash").cloned().unwrap_or(Value::Null),
    });
    if json_output {
        println!(
            "{}",
            serde_json::to_string_pretty(&payload)
                .context("failed to encode channel QR payload as JSON")?
        );
    } else {
        println!(
            "channels.qr channel={} code={} issued_by={} expires_at_unix_ms={} qr_text=\"{}\" artifact={}",
            payload.get("route_channel").and_then(Value::as_str).unwrap_or("unknown"),
            code,
            issued_by,
            expires_at,
            payload.get("qr_text").and_then(Value::as_str).unwrap_or(""),
            payload.get("artifact").and_then(Value::as_str).unwrap_or("-"),
        );
    }
    Ok(())
}
