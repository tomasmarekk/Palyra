use anyhow::{Context, Result};
use serde_json::{json, Value};

use crate::{
    args::{ChannelProviderArg, ChannelResolveEntityArg},
    client::message,
};

#[allow(clippy::too_many_arguments)]
pub(super) fn run_capabilities(
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
    let payload = build_channel_capabilities_payload(
        connector_id,
        provider,
        account_id,
        url,
        token,
        principal,
        device_id,
        channel,
    )?;
    emit_channel_capabilities(payload, json_output)
}

pub(super) fn run_resolution(
    provider: ChannelProviderArg,
    account_id: String,
    entity: ChannelResolveEntityArg,
    value: String,
    json_output: bool,
) -> Result<()> {
    let payload =
        super::providers::build_channel_resolution_payload(provider, account_id, entity, value)?;
    emit_channel_resolution(payload, json_output)
}

#[allow(clippy::too_many_arguments)]
fn build_channel_capabilities_payload(
    connector_id: Option<String>,
    provider: Option<ChannelProviderArg>,
    account_id: Option<String>,
    url: Option<String>,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
) -> Result<Value> {
    let resolved_connector_id =
        super::common::resolve_connector_selector(connector_id, provider, account_id)?;
    let provider = provider
        .or_else(|| {
            super::providers::infer_provider_from_connector_id(resolved_connector_id.as_str())
        })
        .unwrap_or(ChannelProviderArg::Echo);
    let provider_name = super::providers::label(provider);
    let message_capabilities = message::load_capabilities(
        resolved_connector_id.as_str(),
        url,
        token,
        principal,
        device_id,
        channel,
    )?;
    let lifecycle_actions = super::providers::supported_lifecycle_actions(provider);
    let resolve_entities = super::providers::supported_resolve_entities(provider);
    Ok(json!({
        "connector_id": resolved_connector_id,
        "provider": provider_name,
        "supported": matches!(provider, ChannelProviderArg::Discord | ChannelProviderArg::Echo),
        "lifecycle_actions": lifecycle_actions,
        "message": {
            "provider_kind": message_capabilities.provider_kind,
            "supported_actions": message_capabilities.supported_actions,
            "unsupported_actions": message_capabilities.unsupported_actions,
            "action_details": message_capabilities
                .action_details
                .iter()
                .map(|detail| {
                    json!({
                        "action": detail.action,
                        "supported": detail.supported,
                        "reason": detail.reason,
                        "policy_action": detail.policy_action,
                        "approval_mode": detail.approval_mode,
                        "risk_level": detail.risk_level,
                        "audit_event_type": detail.audit_event_type,
                        "required_permissions": detail.required_permissions,
                    })
                })
                .collect::<Vec<_>>(),
        },
        "resolve_entities": resolve_entities,
        "pairing": {
            "supported": super::providers::pairing_supported(provider),
            "route_channel": super::providers::pairing_supported(provider)
                .then_some(resolved_connector_id.clone()),
            "qr_text_format": super::providers::pairing_supported(provider)
                .then_some("pair <code>"),
        },
        "notes": super::providers::capability_notes(provider),
    }))
}

fn emit_channel_capabilities(payload: Value, json_output: bool) -> Result<()> {
    if json_output {
        println!(
            "{}",
            serde_json::to_string_pretty(&payload)
                .context("failed to encode channel capabilities payload as JSON")?
        );
        return Ok(());
    }

    let lifecycle = payload
        .get("lifecycle_actions")
        .and_then(Value::as_array)
        .map(|values| values.iter().filter_map(Value::as_str).collect::<Vec<_>>().join(","))
        .unwrap_or_else(|| "none".to_owned());
    let supported_message = payload
        .pointer("/message/supported_actions")
        .and_then(Value::as_array)
        .map(|values| values.iter().filter_map(Value::as_str).collect::<Vec<_>>().join(","))
        .unwrap_or_else(|| "none".to_owned());
    let unsupported_message = payload
        .pointer("/message/unsupported_actions")
        .and_then(Value::as_array)
        .map(|values| values.iter().filter_map(Value::as_str).collect::<Vec<_>>().join(","))
        .unwrap_or_else(|| "none".to_owned());
    let resolve_entities = payload
        .get("resolve_entities")
        .and_then(Value::as_array)
        .map(|values| values.iter().filter_map(Value::as_str).collect::<Vec<_>>().join(","))
        .unwrap_or_else(|| "none".to_owned());
    println!(
        "channels.capabilities connector_id={} provider={} supported={} lifecycle={} message_supported={} message_unsupported={} resolve={} pairings_supported={}",
        payload.get("connector_id").and_then(Value::as_str).unwrap_or("unknown"),
        payload.get("provider").and_then(Value::as_str).unwrap_or("unknown"),
        payload.get("supported").and_then(Value::as_bool).unwrap_or(false),
        lifecycle,
        supported_message,
        unsupported_message,
        resolve_entities,
        payload.pointer("/pairing/supported").and_then(Value::as_bool).unwrap_or(false),
    );
    Ok(())
}

fn emit_channel_resolution(payload: Value, json_output: bool) -> Result<()> {
    if json_output {
        println!(
            "{}",
            serde_json::to_string_pretty(&payload)
                .context("failed to encode channel resolution payload as JSON")?
        );
        return Ok(());
    }
    println!(
        "channels.resolve provider={} account_id={} entity={} input={} normalized={} canonical={} supported={}",
        payload.get("provider").and_then(Value::as_str).unwrap_or("unknown"),
        payload.get("account_id").and_then(Value::as_str).unwrap_or("unknown"),
        payload.get("entity").and_then(Value::as_str).unwrap_or("unknown"),
        payload.get("input").and_then(Value::as_str).unwrap_or(""),
        payload.get("normalized").and_then(Value::as_str).unwrap_or("-"),
        payload.get("canonical").and_then(Value::as_str).unwrap_or("-"),
        payload.get("supported").and_then(Value::as_bool).unwrap_or(true),
    );
    Ok(())
}
