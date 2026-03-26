mod router;

pub(crate) mod connectors {
    pub(crate) mod discord;
}

use anyhow::{bail, Context, Result};
use palyra_connector_discord::{
    canonical_discord_channel_identity, canonical_discord_sender_identity,
    normalize_discord_account_id, normalize_discord_target,
};
use serde_json::{json, Value};
use std::fs;
use std::io::{IsTerminal, Read, Write};

use crate::{
    args::{ChannelProviderArg, ChannelResolveEntityArg, ChannelsCommand},
    client::{channels as channels_client, message},
    normalize_optional_text_arg, normalize_required_text_arg, output,
    output::channels as channels_output,
    prompt_secret_value,
};

#[derive(Debug, Clone)]
struct ChannelAdminConnection {
    url: Option<String>,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
}

impl ChannelAdminConnection {
    fn new(
        url: Option<String>,
        token: Option<String>,
        principal: String,
        device_id: String,
        channel: Option<String>,
    ) -> Self {
        Self { url, token, principal, device_id, channel }
    }
}

#[derive(Debug, Clone)]
struct ChannelCapabilityQuery {
    connector_id: Option<String>,
    provider: Option<ChannelProviderArg>,
    account_id: Option<String>,
    connection: ChannelAdminConnection,
}

#[derive(Debug, Clone)]
struct RouterPairingCodeRequest {
    route_channel: String,
    issued_by: Option<String>,
    ttl_ms: Option<u64>,
    connection: ChannelAdminConnection,
}

pub(crate) fn run(command: ChannelsCommand) -> Result<()> {
    match command {
        ChannelsCommand::Add {
            provider,
            account_id,
            interactive,
            credential,
            credential_stdin,
            credential_prompt,
            mode,
            inbound_scope,
            allow_from,
            deny_from,
            require_mention,
            mention_patterns,
            concurrency_limit,
            direct_message_policy,
            broadcast_strategy,
            confirm_open_guild_channels,
            verify_channel_id,
            url,
            token,
            principal,
            device_id,
            channel,
            json,
        } => run_channel_lifecycle_upsert(
            "add",
            provider,
            account_id,
            interactive,
            credential,
            credential_stdin,
            credential_prompt,
            mode,
            inbound_scope,
            allow_from,
            deny_from,
            require_mention,
            mention_patterns,
            concurrency_limit,
            direct_message_policy,
            broadcast_strategy,
            confirm_open_guild_channels,
            verify_channel_id,
            url,
            token,
            principal,
            device_id,
            channel,
            output::preferred_json(json),
        )?,
        ChannelsCommand::Login {
            provider,
            account_id,
            credential,
            credential_stdin,
            credential_prompt,
            mode,
            inbound_scope,
            allow_from,
            deny_from,
            require_mention,
            mention_patterns,
            concurrency_limit,
            direct_message_policy,
            broadcast_strategy,
            confirm_open_guild_channels,
            verify_channel_id,
            url,
            token,
            principal,
            device_id,
            channel,
            json,
        } => run_channel_lifecycle_upsert(
            "login",
            provider,
            account_id,
            false,
            credential,
            credential_stdin,
            credential_prompt,
            mode,
            inbound_scope,
            allow_from,
            deny_from,
            require_mention,
            mention_patterns,
            concurrency_limit,
            direct_message_policy,
            broadcast_strategy,
            confirm_open_guild_channels,
            verify_channel_id,
            url,
            token,
            principal,
            device_id,
            channel,
            output::preferred_json(json),
        )?,
        ChannelsCommand::Logout {
            provider,
            account_id,
            keep_credential,
            url,
            token,
            principal,
            device_id,
            channel,
            json,
        } => run_channel_lifecycle_disable(
            "logout",
            provider,
            account_id,
            keep_credential,
            url,
            token,
            principal,
            device_id,
            channel,
            output::preferred_json(json),
        )?,
        ChannelsCommand::Remove {
            provider,
            account_id,
            keep_credential,
            url,
            token,
            principal,
            device_id,
            channel,
            json,
        } => run_channel_lifecycle_disable(
            "remove",
            provider,
            account_id,
            keep_credential,
            url,
            token,
            principal,
            device_id,
            channel,
            output::preferred_json(json),
        )?,
        ChannelsCommand::Capabilities {
            connector_id,
            provider,
            account_id,
            url,
            token,
            principal,
            device_id,
            channel,
            json,
        } => {
            let capabilities = build_channel_capabilities_payload(ChannelCapabilityQuery {
                connector_id,
                provider,
                account_id,
                connection: ChannelAdminConnection::new(url, token, principal, device_id, channel),
            })?;
            emit_channel_capabilities(capabilities, output::preferred_json(json))?;
        }
        ChannelsCommand::Resolve { provider, account_id, entity, value, json } => {
            let payload = build_channel_resolution_payload(provider, account_id, entity, value)?;
            emit_channel_resolution(payload, output::preferred_json(json))?;
        }
        ChannelsCommand::Pairings {
            connector_id,
            provider,
            account_id,
            url,
            token,
            principal,
            device_id,
            channel,
            json,
        } => {
            let route_channel = resolve_connector_selector(connector_id, provider, account_id)?;
            let response = fetch_router_pairings(
                Some(route_channel),
                url,
                token,
                principal,
                device_id,
                channel,
            )?;
            channels_output::emit_router_pairings(response, output::preferred_json(json))?;
        }
        ChannelsCommand::PairingCode {
            connector_id,
            provider,
            account_id,
            issued_by,
            ttl_ms,
            url,
            token,
            principal,
            device_id,
            channel,
            json,
        } => {
            let route_channel = resolve_connector_selector(connector_id, provider, account_id)?;
            let response = mint_router_pairing_code(RouterPairingCodeRequest {
                route_channel,
                issued_by,
                ttl_ms,
                connection: ChannelAdminConnection::new(url, token, principal, device_id, channel),
            })?;
            channels_output::emit_router_pairing_code(response, output::preferred_json(json))?;
        }
        ChannelsCommand::Qr {
            connector_id,
            provider,
            account_id,
            issued_by,
            ttl_ms,
            artifact,
            url,
            token,
            principal,
            device_id,
            channel,
            json,
        } => {
            let route_channel = resolve_connector_selector(connector_id, provider, account_id)?;
            let response = mint_router_pairing_code(RouterPairingCodeRequest {
                route_channel: route_channel.clone(),
                issued_by,
                ttl_ms,
                connection: ChannelAdminConnection::new(url, token, principal, device_id, channel),
            })?;
            emit_channel_qr(route_channel, response, artifact, output::preferred_json(json))?;
        }
        ChannelsCommand::Discord { command } => connectors::discord::run(command)?,
        ChannelsCommand::Router { command } => router::run(command)?,
        ChannelsCommand::List { url, token, principal, device_id, channel, json } => {
            let request_context = channels_client::resolve_request_context(
                url, token, principal, device_id, channel,
            )?;
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
            provider,
            account_id,
            url,
            token,
            principal,
            device_id,
            channel,
            json,
        } => {
            let connector_id = resolve_connector_selector(connector_id, provider, account_id)?;
            let response = resolve_connector_status(
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
            provider,
            account_id,
            verify_channel_id,
            url,
            token,
            principal,
            device_id,
            channel,
            json,
        } => {
            let connector_id = resolve_connector_selector(connector_id, provider, account_id)?;
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
            provider,
            account_id,
            url,
            token,
            principal,
            device_id,
            channel,
            limit,
            json,
        } => {
            let connector_id = resolve_connector_selector(connector_id, provider, account_id)?;
            let request_context = channels_client::resolve_request_context(
                url, token, principal, device_id, channel,
            )?;
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

#[allow(clippy::too_many_arguments)]
fn run_channel_lifecycle_upsert(
    action: &'static str,
    provider: ChannelProviderArg,
    account_id: String,
    interactive: bool,
    credential: Option<String>,
    credential_stdin: bool,
    credential_prompt: bool,
    mode: String,
    inbound_scope: String,
    allow_from: Vec<String>,
    deny_from: Vec<String>,
    require_mention: Option<bool>,
    mention_patterns: Vec<String>,
    concurrency_limit: Option<u64>,
    direct_message_policy: Option<String>,
    broadcast_strategy: Option<String>,
    confirm_open_guild_channels: bool,
    verify_channel_id: Option<String>,
    url: Option<String>,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
    json_output: bool,
) -> Result<()> {
    match provider {
        ChannelProviderArg::Discord => {
            if interactive {
                if credential.is_some() || credential_stdin || credential_prompt {
                    bail!(
                        "channels {action} --interactive cannot be combined with explicit credential input"
                    );
                }
                return connectors::discord::run(crate::args::ChannelsDiscordCommand::Setup {
                    account_id,
                    url,
                    token,
                    principal,
                    device_id,
                    channel,
                    verify_channel_id,
                    json: json_output,
                });
            }

            let normalized_account_id = normalize_discord_account_id(account_id.as_str())
                .map_err(|error| anyhow::anyhow!(error.to_string()))?;
            let connector_id = connectors::discord::connector_id(normalized_account_id.as_str())?;
            let credential = load_channel_credential(
                credential,
                credential_stdin,
                credential_prompt,
                "Discord bot token: ",
            )?;
            let mode = normalize_required_text_arg(mode, "mode")?;
            let inbound_scope = normalize_required_text_arg(inbound_scope, "inbound_scope")?;
            let mention_patterns = normalize_string_list(mention_patterns);
            let allow_from = normalize_string_list(allow_from);
            let deny_from = normalize_string_list(deny_from);
            let request_context = channels_client::resolve_request_context(
                url, token, principal, device_id, channel,
            )?;
            let client = channels_client::build_client()?;

            let probe_endpoint = format!(
                "{}/admin/v1/channels/discord/onboarding/probe",
                request_context.base_url.trim_end_matches('/'),
            );
            let _probe_response = channels_client::send_request(
                client.post(probe_endpoint).json(&connectors::discord::probe_payload(
                    normalized_account_id.as_str(),
                    credential.as_str(),
                    mode.as_str(),
                    verify_channel_id.as_deref(),
                )),
                request_context.clone(),
                "failed to call discord onboarding probe endpoint",
            )?;

            let apply_endpoint = format!(
                "{}/admin/v1/channels/discord/onboarding/apply",
                request_context.base_url.trim_end_matches('/'),
            );
            let broadcast_strategy = broadcast_strategy.unwrap_or_else(|| "deny".to_owned());
            let response = channels_client::send_request(
                client.post(apply_endpoint).json(&connectors::discord::apply_payload(
                    normalized_account_id.as_str(),
                    credential.as_str(),
                    mode.as_str(),
                    inbound_scope.as_str(),
                    &mention_patterns,
                    &allow_from,
                    &deny_from,
                    require_mention.unwrap_or(true),
                    direct_message_policy.as_deref(),
                    concurrency_limit.unwrap_or(2),
                    broadcast_strategy.as_str(),
                    confirm_open_guild_channels,
                    verify_channel_id.as_deref(),
                )),
                request_context,
                "failed to call discord onboarding apply endpoint",
            )?;
            connectors::discord::emit_apply_response(connector_id.as_str(), response, json_output)
        }
        other => unsupported_provider_action(
            "channels",
            action,
            other,
            None,
            json_output,
            "provider lifecycle is implemented for Discord only in this milestone batch",
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn run_channel_lifecycle_disable(
    action: &'static str,
    provider: ChannelProviderArg,
    account_id: String,
    keep_credential: bool,
    url: Option<String>,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
    json_output: bool,
) -> Result<()> {
    match provider {
        ChannelProviderArg::Discord => {
            let normalized_account_id = normalize_discord_account_id(account_id.as_str())
                .map_err(|error| anyhow::anyhow!(error.to_string()))?;
            let connector_id = connectors::discord::connector_id(normalized_account_id.as_str())?;
            let response = post_discord_account_action(
                normalized_account_id.as_str(),
                action,
                json!({ "keep_credential": keep_credential }),
                url,
                token,
                principal,
                device_id,
                channel,
                if action == "logout" {
                    "failed to call discord account logout endpoint"
                } else {
                    "failed to call discord account remove endpoint"
                },
            )?;
            let credential_deleted =
                response.get("credential_deleted").and_then(Value::as_bool).unwrap_or(false);
            emit_channel_lifecycle_disable(
                action,
                provider,
                normalized_account_id.as_str(),
                connector_id.as_str(),
                keep_credential,
                credential_deleted,
                response,
                json_output,
            )
        }
        other => unsupported_provider_action(
            "channels",
            action,
            other,
            None,
            json_output,
            "provider logout/remove is implemented for Discord only in this milestone batch",
        ),
    }
}

fn build_channel_capabilities_payload(query: ChannelCapabilityQuery) -> Result<Value> {
    let ChannelCapabilityQuery { connector_id, provider, account_id, connection } = query;
    let resolved_connector_id = match (connector_id, provider, account_id) {
        (Some(connector_id), _, _) => connector_id,
        (None, Some(provider), account_id) => {
            let account_id = account_id.unwrap_or_else(|| "default".to_owned());
            connector_id_for_provider(provider, account_id.as_str())?
        }
        (None, None, None) | (None, None, Some(_)) => {
            bail!("channels capabilities requires a connector_id or --provider [--account-id]")
        }
    };

    let provider = provider
        .or_else(|| infer_provider_from_connector_id(resolved_connector_id.as_str()))
        .unwrap_or(ChannelProviderArg::Echo);
    let provider_name = provider_label(provider);
    let message_capabilities = message::load_capabilities(
        resolved_connector_id.as_str(),
        connection.url,
        connection.token,
        connection.principal,
        connection.device_id,
        connection.channel,
    )
    .unwrap_or_else(|_| message::MessageCapabilities {
        provider_kind: provider_name.to_owned(),
        supported_actions: provider_supported_message_actions(provider)
            .into_iter()
            .map(str::to_owned)
            .collect(),
        unsupported_actions: message::UNSUPPORTED_MESSAGE_ACTIONS
            .iter()
            .map(|action| (*action).to_owned())
            .collect(),
        action_details: Vec::new(),
    });

    let lifecycle_actions = provider_supported_lifecycle_actions(provider);
    let resolve_entities = provider_supported_resolve_entities(provider);
    Ok(json!({
        "connector_id": resolved_connector_id,
        "provider": provider_name,
        "supported": matches!(provider, ChannelProviderArg::Discord | ChannelProviderArg::Echo),
        "lifecycle_actions": lifecycle_actions,
        "message": {
            "provider_kind": message_capabilities.provider_kind,
            "supported_actions": message_capabilities.supported_actions,
            "unsupported_actions": message_capabilities.unsupported_actions,
        },
        "resolve_entities": resolve_entities,
        "pairing": {
            "supported": provider_pairing_supported(provider),
            "route_channel": provider_pairing_supported(provider)
                .then_some(resolved_connector_id.clone()),
            "qr_text_format": provider_pairing_supported(provider).then_some("pair <code>"),
        },
        "notes": provider_capability_notes(provider),
    }))
}

fn build_channel_resolution_payload(
    provider: ChannelProviderArg,
    account_id: String,
    entity: ChannelResolveEntityArg,
    value: String,
) -> Result<Value> {
    let normalized_input = normalize_required_text_arg(value, "value")?;
    match provider {
        ChannelProviderArg::Discord => {
            let normalized_account_id = normalize_discord_account_id(account_id.as_str())
                .map_err(|error| anyhow::anyhow!(error.to_string()))?;
            let connector_id = connectors::discord::connector_id(normalized_account_id.as_str())?;
            let (normalized, canonical) = match entity {
                ChannelResolveEntityArg::User => {
                    let canonical = canonical_discord_sender_identity(normalized_input.as_str());
                    (normalized_input.clone(), canonical)
                }
                ChannelResolveEntityArg::Channel
                | ChannelResolveEntityArg::Conversation
                | ChannelResolveEntityArg::Thread => {
                    let normalized = normalize_discord_target(normalized_input.as_str())
                        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
                    let canonical = canonical_discord_channel_identity(normalized.as_str());
                    (normalized, canonical)
                }
            };
            Ok(json!({
                "provider": provider_label(provider),
                "account_id": normalized_account_id,
                "connector_id": connector_id,
                "entity": resolve_entity_label(entity),
                "input": normalized_input,
                "normalized": normalized,
                "canonical": canonical,
            }))
        }
        other => Ok(json!({
            "provider": provider_label(other),
            "account_id": account_id,
            "entity": resolve_entity_label(entity),
            "input": normalized_input,
            "supported": false,
            "reason": format!(
                "entity resolution is implemented for Discord only (requested provider={})",
                provider_label(other)
            ),
        })),
    }
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

fn mint_router_pairing_code(request: RouterPairingCodeRequest) -> Result<Value> {
    let request_context = channels_client::resolve_request_context(
        request.connection.url,
        request.connection.token,
        request.connection.principal,
        request.connection.device_id,
        request.connection.channel,
    )?;
    let endpoint = format!(
        "{}/admin/v1/channels/router/pairing-codes",
        request_context.base_url.trim_end_matches('/')
    );
    let client = channels_client::build_client()?;
    let payload = json!({
        "channel": request.route_channel,
        "issued_by": request.issued_by.and_then(normalize_optional_text_arg),
        "ttl_ms": request.ttl_ms,
    });
    channels_client::send_request(
        client.post(endpoint).json(&payload),
        request_context,
        "failed to call channel router pairing-code mint endpoint",
    )
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

#[allow(clippy::too_many_arguments)]
fn emit_channel_lifecycle_disable(
    action: &str,
    provider: ChannelProviderArg,
    account_id: &str,
    connector_id: &str,
    keep_credential: bool,
    credential_deleted: bool,
    response: Value,
    json_output: bool,
) -> Result<()> {
    let payload = json!({
        "action": action,
        "provider": provider_label(provider),
        "account_id": account_id,
        "connector_id": connector_id,
        "keep_credential": keep_credential,
        "credential_deleted": credential_deleted,
        "response": response,
    });
    if json_output {
        println!(
            "{}",
            serde_json::to_string_pretty(&payload)
                .context("failed to encode channel lifecycle payload as JSON")?
        );
    } else {
        println!(
            "channels.{} provider={} account_id={} connector_id={} disabled=true keep_credential={} credential_deleted={}",
            action,
            provider_label(provider),
            account_id,
            connector_id,
            keep_credential,
            credential_deleted
        );
        let status_payload = payload
            .pointer("/response/status")
            .or_else(|| payload.pointer("/response/status_before_remove"))
            .or_else(|| payload.pointer("/response"))
            .unwrap_or(&Value::Null);
        for line in channels_output::render_status_lines(status_payload) {
            println!("{line}");
        }
    }
    Ok(())
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

fn load_channel_credential(
    explicit: Option<String>,
    from_stdin: bool,
    from_prompt: bool,
    prompt: &str,
) -> Result<String> {
    let source_count =
        usize::from(explicit.is_some()) + usize::from(from_stdin) + usize::from(from_prompt);
    if source_count != 1 {
        bail!("select exactly one credential source: --credential, --credential-stdin, or --credential-prompt");
    }
    let credential = if let Some(value) = explicit {
        value
    } else if from_stdin {
        let mut value = String::new();
        std::io::stdin()
            .read_to_string(&mut value)
            .context("failed to read credential from stdin")?;
        value
    } else {
        if !std::io::stdin().is_terminal() || !std::io::stdout().is_terminal() {
            bail!("credential prompt requires an interactive terminal");
        }
        prompt_secret_value(prompt)?
    };
    let normalized = credential.trim().to_owned();
    if normalized.is_empty() {
        bail!("credential input is empty");
    }
    Ok(normalized)
}

fn resolve_connector_selector(
    connector_id: Option<String>,
    provider: Option<ChannelProviderArg>,
    account_id: Option<String>,
) -> Result<String> {
    match (connector_id, provider) {
        (Some(connector_id), None) | (Some(connector_id), Some(_)) => {
            normalize_required_text_arg(connector_id, "connector_id")
        }
        (None, Some(provider)) => {
            let account_id = account_id.unwrap_or_else(|| "default".to_owned());
            connector_id_for_provider(provider, account_id.as_str())
        }
        (None, None) => {
            bail!("connector selector requires connector_id or --provider [--account-id]")
        }
    }
}

fn connector_id_for_provider(provider: ChannelProviderArg, account_id: &str) -> Result<String> {
    match provider {
        ChannelProviderArg::Discord => connectors::discord::connector_id(account_id),
        ChannelProviderArg::Echo => {
            Ok(format!("echo:{}", normalize_generic_account_id(account_id, "account_id")?))
        }
        ChannelProviderArg::Slack => {
            Ok(format!("slack:{}", normalize_generic_account_id(account_id, "account_id")?))
        }
        ChannelProviderArg::Telegram => {
            Ok(format!("telegram:{}", normalize_generic_account_id(account_id, "account_id")?))
        }
        ChannelProviderArg::Webhook => {
            Ok(format!("webhook:{}", normalize_generic_account_id(account_id, "account_id")?))
        }
    }
}

fn normalize_generic_account_id(raw: &str, label: &str) -> Result<String> {
    let value = raw.trim();
    if value.is_empty() {
        bail!("{label} cannot be empty");
    }
    if !value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | ':' | '@'))
    {
        bail!("{label} contains unsupported characters");
    }
    Ok(value.to_ascii_lowercase())
}

fn unsupported_provider_action(
    surface: &str,
    action: &str,
    provider: ChannelProviderArg,
    connector_id: Option<&str>,
    json_output: bool,
    reason: &str,
) -> Result<()> {
    let payload = json!({
        "surface": surface,
        "action": action,
        "provider": provider_label(provider),
        "connector_id": connector_id,
        "supported": false,
        "reason": reason,
        "supported_providers": ["discord"],
    });
    if json_output {
        println!(
            "{}",
            serde_json::to_string_pretty(&payload)
                .context("failed to encode unsupported provider payload as JSON")?
        );
        Ok(())
    } else {
        bail!(
            "unsupported {} action '{}' for provider '{}': {}",
            surface,
            action,
            provider_label(provider),
            reason
        )
    }
}

fn normalize_string_list(values: Vec<String>) -> Vec<String> {
    let mut normalized = Vec::new();
    for value in values {
        if let Some(value) = normalize_optional_text_arg(value) {
            let lowered = value.to_ascii_lowercase();
            if !normalized.iter().any(|existing| existing == &lowered) {
                normalized.push(lowered);
            }
        }
    }
    normalized
}

fn infer_provider_from_connector_id(connector_id: &str) -> Option<ChannelProviderArg> {
    let prefix = connector_id.trim().split(':').next()?.to_ascii_lowercase();
    match prefix.as_str() {
        "discord" => Some(ChannelProviderArg::Discord),
        "slack" => Some(ChannelProviderArg::Slack),
        "telegram" => Some(ChannelProviderArg::Telegram),
        "webhook" => Some(ChannelProviderArg::Webhook),
        "echo" => Some(ChannelProviderArg::Echo),
        _ => None,
    }
}

fn provider_supported_lifecycle_actions(provider: ChannelProviderArg) -> Vec<&'static str> {
    match provider {
        ChannelProviderArg::Discord => vec![
            "add",
            "login",
            "logout",
            "remove",
            "status",
            "health_refresh",
            "logs",
            "capabilities",
            "resolve",
            "pairings",
            "pairing_code",
            "qr",
        ],
        ChannelProviderArg::Echo => vec!["status", "logs", "capabilities"],
        ChannelProviderArg::Slack | ChannelProviderArg::Telegram | ChannelProviderArg::Webhook => {
            vec!["capabilities"]
        }
    }
}

fn provider_supported_message_actions(provider: ChannelProviderArg) -> Vec<&'static str> {
    match provider {
        ChannelProviderArg::Discord => message::SUPPORTED_MESSAGE_ACTIONS.to_vec(),
        ChannelProviderArg::Echo
        | ChannelProviderArg::Slack
        | ChannelProviderArg::Telegram
        | ChannelProviderArg::Webhook => Vec::new(),
    }
}

fn provider_supported_resolve_entities(provider: ChannelProviderArg) -> Vec<&'static str> {
    match provider {
        ChannelProviderArg::Discord => vec!["channel", "conversation", "thread", "user"],
        ChannelProviderArg::Echo
        | ChannelProviderArg::Slack
        | ChannelProviderArg::Telegram
        | ChannelProviderArg::Webhook => Vec::new(),
    }
}

fn provider_pairing_supported(provider: ChannelProviderArg) -> bool {
    matches!(provider, ChannelProviderArg::Discord)
}

fn provider_capability_notes(provider: ChannelProviderArg) -> Vec<&'static str> {
    match provider {
        ChannelProviderArg::Discord => vec![
            "discord lifecycle reuses the authenticated onboarding flow",
            "direct outbound send/thread map to the existing discord test-send transport",
            "pairing QR output emits qr_text suitable for external QR encoding",
        ],
        ChannelProviderArg::Echo => vec![
            "echo remains an internal/runtime diagnostic connector",
            "provider lifecycle commands are intentionally unavailable for echo",
        ],
        ChannelProviderArg::Slack | ChannelProviderArg::Telegram | ChannelProviderArg::Webhook => {
            vec!["provider contract reserved for a future connector implementation"]
        }
    }
}

fn provider_label(provider: ChannelProviderArg) -> &'static str {
    match provider {
        ChannelProviderArg::Discord => "discord",
        ChannelProviderArg::Slack => "slack",
        ChannelProviderArg::Telegram => "telegram",
        ChannelProviderArg::Webhook => "webhook",
        ChannelProviderArg::Echo => "echo",
    }
}

fn resolve_entity_label(entity: ChannelResolveEntityArg) -> &'static str {
    match entity {
        ChannelResolveEntityArg::Channel => "channel",
        ChannelResolveEntityArg::Conversation => "conversation",
        ChannelResolveEntityArg::Thread => "thread",
        ChannelResolveEntityArg::User => "user",
    }
}

pub(crate) fn resolve_connector_status(
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
pub(crate) fn post_connector_action(
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

#[allow(clippy::too_many_arguments)]
fn post_discord_account_action(
    account_id: &str,
    action: &str,
    payload: Value,
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
        "{}/admin/v1/channels/discord/accounts/{}/{}",
        request_context.base_url.trim_end_matches('/'),
        account_id,
        action
    );
    let client = channels_client::build_client()?;
    channels_client::send_request(
        client.post(endpoint).json(&payload),
        request_context,
        error_context,
    )
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
