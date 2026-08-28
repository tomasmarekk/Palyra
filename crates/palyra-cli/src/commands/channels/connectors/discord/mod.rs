//! Discord connector CLI surface: guided setup, status, health refresh, and
//! confirmed verify sends.
//!
//! Split by concern: `prompt` gathers interactive input, `request` builds
//! payloads, `emit` renders responses, `setup`/`verify` drive the flows.

mod emit;
mod prompt;
mod request;
mod setup;
mod verify;

pub(crate) use request::{apply_payload, connector_id, probe_payload};
pub(crate) use setup::emit_apply_response;

use anyhow::Result;

use crate::{
    args::ChannelsDiscordCommand,
    commands::channels::{post_connector_action, resolve_connector_status},
    output::{self, channels as channels_output},
};

/// Dispatches a `palyra channels discord` subcommand to its handler.
///
/// # Errors
/// Propagates handler failures (argument validation, daemon transport, or
/// output encoding).
pub(crate) fn run(command: ChannelsDiscordCommand) -> Result<()> {
    match command {
        ChannelsDiscordCommand::Setup {
            account_id,
            url,
            token,
            principal,
            device_id,
            channel,
            verify_channel_id,
            json,
        } => setup::run(
            account_id,
            url,
            token,
            principal,
            device_id,
            channel,
            verify_channel_id,
            json,
        )?,
        ChannelsDiscordCommand::Status {
            account_id,
            url,
            token,
            principal,
            device_id,
            channel,
            json,
        } => {
            let connector_id = request::connector_id(account_id.as_str())?;
            let response = resolve_connector_status(
                connector_id.as_str(),
                url,
                token,
                principal,
                device_id,
                channel,
                "failed to call discord channels status endpoint",
            )?;
            emit_status(response, json)?;
        }
        ChannelsDiscordCommand::HealthRefresh {
            account_id,
            verify_channel_id,
            url,
            token,
            principal,
            device_id,
            channel,
            json,
        } => {
            let connector_id = request::connector_id(account_id.as_str())?;
            let response = post_connector_action(
                connector_id.as_str(),
                "/operations/health-refresh",
                Some(serde_json::json!({ "verify_channel_id": verify_channel_id })),
                url,
                token,
                principal,
                device_id,
                channel,
                "failed to call discord channels health-refresh endpoint",
            )?;
            emit_status(response, json)?;
        }
        ChannelsDiscordCommand::Verify {
            account_id,
            to,
            text,
            confirm,
            auto_reaction,
            thread_id,
            url,
            token,
            principal,
            device_id,
            channel,
            json,
        } => verify::run(
            account_id,
            to,
            text,
            confirm,
            auto_reaction,
            thread_id,
            url,
            token,
            principal,
            device_id,
            channel,
            json,
        )?,
    }
    Ok(())
}

fn emit_status(response: serde_json::Value, explicit_json: bool) -> Result<()> {
    if output::preferred_json(explicit_json) {
        channels_output::emit_status(response, true)
    } else if output::preferred_ndjson(explicit_json, false) {
        output::print_json_line(&response, "failed to encode Discord channel status as NDJSON")
    } else {
        channels_output::emit_status(response, false)
    }
}
