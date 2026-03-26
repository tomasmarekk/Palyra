use crate::{app, args::TuiCommand, resolve_optional_canonical_id, tui, Result};

pub(crate) fn run_tui(command: TuiCommand) -> Result<()> {
    let root_context = app::current_root_context()
        .ok_or_else(|| anyhow::anyhow!("CLI root context is unavailable for tui command"))?;
    let connection = root_context.resolve_grpc_connection(
        app::ConnectionOverrides {
            grpc_url: command.grpc_url,
            token: command.token,
            principal: command.principal,
            device_id: command.device_id,
            channel: command.channel,
            daemon_url: None,
        },
        app::ConnectionDefaults::USER,
    )?;
    tui::run(tui::LaunchOptions {
        connection,
        session_id: resolve_optional_canonical_id(command.session_id)?,
        session_key: command.session_key,
        session_label: command.session_label,
        require_existing: command.require_existing,
        allow_sensitive_tools: command.allow_sensitive_tools,
        include_archived_sessions: command.include_archived_sessions,
    })
}
