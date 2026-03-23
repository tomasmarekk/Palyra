use anyhow::{bail, Context, Result};
use serde_json::Value;
use std::io::IsTerminal;

use crate::{client::channels as channels_client, output, prompt_yes_no, prompt_yes_no_default};

use super::{emit, prompt, request};

#[allow(clippy::too_many_arguments)]
pub(crate) fn run(
    account_id: String,
    url: Option<String>,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
    verify_channel_id: Option<String>,
    json_output: bool,
) -> Result<()> {
    if !std::io::stdin().is_terminal()
        || !std::io::stderr().is_terminal()
        || !std::io::stdout().is_terminal()
    {
        bail!("discord setup requires an interactive terminal (stdin/stdout/stderr TTY)");
    }

    let request_context =
        channels_client::resolve_request_context(url, token, principal, device_id, channel)?;
    let setup_mode = prompt::setup_mode()?;
    let setup_token = prompt::setup_token()?;
    let connector_id = request::connector_id(account_id.as_str())?;
    let client = channels_client::build_client()?;
    let probe_endpoint =
        format!(
            "{}/admin/v1/channels/discord/onboarding/probe",
            request_context.base_url.trim_end_matches('/'),
        );
    let probe_response = channels_client::send_request(
        client.post(probe_endpoint).json(&request::probe_payload(
            account_id.as_str(),
            setup_token.as_str(),
            setup_mode.as_str(),
            verify_channel_id.as_deref(),
        )),
        request_context.clone(),
        "failed to call discord onboarding probe endpoint",
    )?;
    eprintln!("discord setup preflight: token validation succeeded");
    emit::onboarding_warnings(&probe_response);
    emit::inbound_monitor_summary(&probe_response);
    emit::channel_permission_check(&probe_response);
    emit::onboarding_defaults(&probe_response);

    let inbound_scope = prompt::inbound_scope()?;
    let allow_from = prompt::sender_filters("Allow-from sender IDs (comma separated, optional): ")?;
    let deny_from = prompt::sender_filters("Deny-from sender IDs (comma separated, optional): ")?;
    let require_mention_default = inbound_scope != "open_guild_channels";
    let require_mention = prompt_yes_no_default(
        format!(
            "Require mention in guild channels? [{}]: ",
            if require_mention_default { "Y/n" } else { "y/N" }
        )
        .as_str(),
        require_mention_default,
    )?;
    let broadcast_strategy = prompt::broadcast_strategy()?;
    let concurrency_limit = prompt::concurrency_limit()?;
    let confirm_open = if inbound_scope == "open_guild_channels" {
        prompt_yes_no("Open guild channels are high-risk. Confirm open scope? [y/N]: ")?
    } else {
        false
    };

    let apply_endpoint =
        format!(
            "{}/admin/v1/channels/discord/onboarding/apply",
            request_context.base_url.trim_end_matches('/'),
        );
    let response = channels_client::send_request(
        client.post(apply_endpoint).json(&request::apply_payload(
            account_id.as_str(),
            setup_token.as_str(),
            setup_mode.as_str(),
            inbound_scope.as_str(),
            &allow_from,
            &deny_from,
            require_mention,
            concurrency_limit,
            broadcast_strategy.as_str(),
            confirm_open,
            verify_channel_id.as_deref(),
        )),
        request_context,
        "failed to call discord onboarding apply endpoint",
    )?;
    emit_apply_response(connector_id.as_str(), response, output::preferred_json(json_output))
}

fn emit_apply_response(connector_id: &str, response: Value, json_output: bool) -> Result<()> {
    if json_output {
        println!(
            "{}",
            serde_json::to_string_pretty(&response)
                .context("failed to encode discord onboarding apply payload as JSON")?
        );
    } else {
        emit::emit_setup_success(connector_id, &response);
        emit::onboarding_warnings(&response);
        emit::inbound_monitor_summary(&response);
        emit::channel_permission_check(&response);
        emit::onboarding_defaults(&response);
    }
    Ok(())
}
