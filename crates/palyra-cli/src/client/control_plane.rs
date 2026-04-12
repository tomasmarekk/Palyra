use anyhow::{Context, Result};
use palyra_control_plane::{ConsoleLoginRequest, ControlPlaneClient, ControlPlaneClientConfig};

use crate::app;

pub(crate) struct AdminConsoleContext {
    pub(crate) client: ControlPlaneClient,
}

pub(crate) async fn connect_admin_console(
    overrides: app::ConnectionOverrides,
) -> Result<AdminConsoleContext> {
    let root_context = app::current_root_context().ok_or_else(|| {
        anyhow::anyhow!("CLI root context is unavailable for control-plane command")
    })?;
    let connection =
        root_context.resolve_http_connection(overrides, app::ConnectionDefaults::ADMIN)?;
    let mut client =
        ControlPlaneClient::new(ControlPlaneClientConfig::new(connection.base_url.clone()))
            .context("failed to initialize control-plane HTTP client")?;
    client
        .login(&ConsoleLoginRequest {
            admin_token: connection.token.clone(),
            principal: connection.principal.clone(),
            device_id: connection.device_id.clone(),
            channel: Some(connection.channel.clone()),
        })
        .await
        .context("failed to establish authenticated console session")?;
    Ok(AdminConsoleContext { client })
}
