//! Authenticated control-plane (admin console) session setup.
//!
//! Resolves the admin HTTP connection from the CLI root context plus per-command
//! overrides, then performs the console login so callers get a ready client.

use std::time::Duration;

use anyhow::{Context, Result};
use palyra_control_plane::{ConsoleLoginRequest, ControlPlaneClient, ControlPlaneClientConfig};

use crate::app;

/// A control-plane client that already holds an authenticated console session.
pub(crate) struct AdminConsoleContext {
    pub(crate) client: ControlPlaneClient,
}

/// Connects to the admin console using the client's default request timeout.
///
/// # Errors
/// Returns an error when the CLI root context is unavailable, the connection
/// cannot be resolved, client initialization fails, or the console login fails.
pub(crate) async fn connect_admin_console(
    overrides: app::ConnectionOverrides,
) -> Result<AdminConsoleContext> {
    connect_admin_console_with_request_timeout(overrides, None).await
}

/// Connects to the admin console, optionally overriding the request timeout.
///
/// # Errors
/// Returns an error when the CLI root context is unavailable, the connection
/// cannot be resolved, client initialization fails, or the console login fails.
pub(crate) async fn connect_admin_console_with_request_timeout(
    overrides: app::ConnectionOverrides,
    request_timeout: Option<Duration>,
) -> Result<AdminConsoleContext> {
    let root_context = app::current_root_context().ok_or_else(|| {
        anyhow::anyhow!("CLI root context is unavailable for control-plane command")
    })?;
    let connection =
        root_context.resolve_http_connection(overrides, app::ConnectionDefaults::ADMIN)?;
    let mut config = ControlPlaneClientConfig::new(connection.base_url.clone());
    if let Some(request_timeout) = request_timeout {
        config.request_timeout = request_timeout;
    }
    let mut client = ControlPlaneClient::new(config)
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
