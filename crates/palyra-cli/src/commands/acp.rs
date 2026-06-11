//! Agent Client Protocol (ACP) entry points: the long-lived editor bridge
//! and the one-shot shim that replays a single agent run as ACP output.
//!
//! The bridge derives its control-plane principal from the resolved gRPC
//! connection so editor sessions act under the operator's admin identity.

use anyhow::{anyhow, Result};

use crate::cli::{AcpBridgeArgs, AcpShimArgs, AcpSubcommand};
use crate::*;

/// Dispatches `palyra acp`, defaulting to the bridge when no subcommand is
/// given.
///
/// # Errors
/// Propagates connection resolution and bridge/shim runtime failures.
pub(crate) fn run_acp(command: AcpCommand) -> Result<()> {
    match command.subcommand {
        Some(AcpSubcommand::Shim { command }) => run_acp_shim(command),
        None => run_acp_bridge(command.bridge),
    }
}

/// Runs the ACP bridge for the deprecated `agent acp` spelling.
///
/// # Errors
/// Propagates connection resolution and bridge runtime failures.
pub(crate) fn run_legacy_agent_acp(command: AcpBridgeArgs) -> Result<()> {
    run_acp_bridge(command)
}

/// Runs the ACP shim for the deprecated `agent acp-shim` spelling.
///
/// # Errors
/// Propagates connection resolution and shim runtime failures.
pub(crate) fn run_legacy_agent_acp_shim(command: AcpShimArgs) -> Result<()> {
    run_acp_shim(command)
}

fn run_acp_bridge(command: AcpBridgeArgs) -> Result<()> {
    let root_context = app::current_root_context()
        .ok_or_else(|| anyhow!("CLI root context is unavailable for ACP command"))?;
    let grpc_overrides = app::ConnectionOverrides {
        grpc_url: command.connection.grpc_url,
        token: command.connection.token,
        principal: command.connection.principal,
        device_id: command.connection.device_id,
        channel: command.connection.channel,
        daemon_url: None,
    };
    let connection = root_context
        .resolve_grpc_connection(grpc_overrides.clone(), app::ConnectionDefaults::USER)?;
    let admin_principal =
        root_context.resolve_admin_console_principal(Some(connection.principal.as_str()));
    let control_plane_overrides = app::ConnectionOverrides {
        daemon_url: None,
        grpc_url: None,
        token: connection.token.clone(),
        principal: Some(admin_principal),
        device_id: Some(connection.device_id.clone()),
        channel: Some(connection.channel.clone()),
    };
    acp_bridge::run_agent_acp_bridge(
        connection,
        control_plane_overrides,
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
        interrupt_active_run: false,
        approval_mode: AgentApprovalMode::Prompt,
        origin_kind: None,
        origin_run_id: None,
        parameter_delta_json: None,
    })?;
    run_agent_stream_as_acp(connection, request)
}
