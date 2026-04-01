use anyhow::{anyhow, Result};

use crate::cli::{AcpBridgeArgs, AcpShimArgs, AcpSubcommand};
use crate::*;

pub(crate) fn run_acp(command: AcpCommand) -> Result<()> {
    match command.subcommand {
        Some(AcpSubcommand::Shim { command }) => run_acp_shim(command),
        None => run_acp_bridge(command.bridge),
    }
}

pub(crate) fn run_legacy_agent_acp(command: AcpBridgeArgs) -> Result<()> {
    run_acp_bridge(command)
}

pub(crate) fn run_legacy_agent_acp_shim(command: AcpShimArgs) -> Result<()> {
    run_acp_shim(command)
}

fn run_acp_bridge(command: AcpBridgeArgs) -> Result<()> {
    let root_context = app::current_root_context()
        .ok_or_else(|| anyhow!("CLI root context is unavailable for ACP command"))?;
    let connection = root_context.resolve_grpc_connection(
        app::ConnectionOverrides {
            grpc_url: command.connection.grpc_url,
            token: command.connection.token,
            principal: command.connection.principal,
            device_id: command.connection.device_id,
            channel: command.connection.channel,
            daemon_url: None,
        },
        app::ConnectionDefaults::USER,
    )?;
    acp_bridge::run_agent_acp_bridge(
        connection,
        command.allow_sensitive_tools,
        acp_bridge::AcpSessionDefaults {
            session_key: command.session_defaults.session_key,
            session_label: command.session_defaults.session_label,
            require_existing: command.session_defaults.require_existing,
            reset_session: command.session_defaults.reset_session,
        },
    )
}

fn run_acp_shim(command: AcpShimArgs) -> Result<()> {
    let root_context = app::current_root_context()
        .ok_or_else(|| anyhow!("CLI root context is unavailable for ACP command"))?;
    let connection = root_context.resolve_grpc_connection(
        app::ConnectionOverrides {
            grpc_url: command.connection.grpc_url,
            token: command.connection.token,
            principal: command.connection.principal,
            device_id: command.connection.device_id,
            channel: command.connection.channel,
            daemon_url: None,
        },
        app::ConnectionDefaults::USER,
    )?;
    if command.ndjson_stdin {
        return run_acp_shim_from_stdin(connection, command.allow_sensitive_tools);
    }

    let input_prompt = resolve_prompt_input(command.prompt, command.prompt_stdin)?;
    let request = build_agent_run_input(AgentRunInputArgs {
        session_id: resolve_optional_canonical_id(command.session_id)?,
        session_key: command.session_defaults.session_key,
        session_label: command.session_defaults.session_label,
        require_existing: command.session_defaults.require_existing,
        reset_session: command.session_defaults.reset_session,
        run_id: command.run_id,
        prompt: input_prompt,
        allow_sensitive_tools: command.allow_sensitive_tools,
        origin_kind: None,
        origin_run_id: None,
        parameter_delta_json: None,
    })?;
    run_agent_stream_as_acp(connection, request)
}
