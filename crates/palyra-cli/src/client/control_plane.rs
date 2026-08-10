//! Authenticated control-plane (admin console) session setup.
//!
//! Resolves the admin HTTP connection from the CLI root context plus per-command
//! overrides, then performs the console login so callers get a ready client.

use std::time::Duration;

use anyhow::{Context, Result};
use palyra_control_plane::{
    ConsoleLoginRequest, ConsoleSession, ControlPlaneClient, ControlPlaneClientConfig,
    ControlPlaneClientError,
};

use crate::app;

const ADMIN_LOGIN_RATE_LIMIT_RETRY_DELAYS: [Duration; 3] =
    [Duration::from_millis(100), Duration::from_millis(250), Duration::from_millis(500)];

/// A control-plane client that already holds an authenticated console session.
pub(crate) struct AdminConsoleContext {
    pub(crate) client: ControlPlaneClient,
    pub(crate) principal: String,
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
    let login_request = ConsoleLoginRequest {
        admin_token: connection.token.clone(),
        principal: connection.principal.clone(),
        device_id: connection.device_id.clone(),
        channel: Some(connection.channel.clone()),
    };
    let session = login_admin_console_with_rate_limit_retry(&mut client, &login_request)
        .await
        .context("failed to establish authenticated console session")?;
    Ok(AdminConsoleContext { client, principal: session.principal })
}

async fn login_admin_console_with_rate_limit_retry(
    client: &mut ControlPlaneClient,
    request: &ConsoleLoginRequest,
) -> Result<ConsoleSession, ControlPlaneClientError> {
    for delay in ADMIN_LOGIN_RATE_LIMIT_RETRY_DELAYS {
        match client.login(request).await {
            Ok(session) => return Ok(session),
            Err(error) if control_plane_login_error_is_rate_limited(&error) => {
                tokio::time::sleep(delay).await;
            }
            Err(error) => return Err(error),
        }
    }
    client.login(request).await
}

fn control_plane_login_error_is_rate_limited(error: &ControlPlaneClientError) -> bool {
    matches!(error, ControlPlaneClientError::Http { status: 429, .. })
}

#[cfg(test)]
mod tests {
    use palyra_control_plane::{ControlPlaneClientError, ErrorCategory, ErrorEnvelope};

    use super::control_plane_login_error_is_rate_limited;

    #[test]
    fn control_plane_login_retry_is_limited_to_http_429() {
        let rate_limited = ControlPlaneClientError::Http {
            status: 429,
            message: "admin API rate limit exceeded for 127.0.0.1".to_owned(),
            envelope: Some(ErrorEnvelope {
                error: "admin API rate limit exceeded for 127.0.0.1".to_owned(),
                code: "rate_limited".to_owned(),
                category: ErrorCategory::Availability,
                retryable: true,
                redacted: false,
                validation_errors: Vec::new(),
            }),
        };
        let unauthorized = ControlPlaneClientError::Http {
            status: 401,
            message: "unauthorized".to_owned(),
            envelope: None,
        };

        assert!(control_plane_login_error_is_rate_limited(&rate_limited));
        assert!(!control_plane_login_error_is_rate_limited(&unauthorized));
    }
}
