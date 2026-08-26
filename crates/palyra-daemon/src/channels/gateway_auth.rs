//! Gateway authentication for connector-originated RouteMessage calls.

use crate::transport::grpc::auth::GatewayAuthConfig;

use palyra_connectors::ConnectorSupervisorError;

/// Resolves the principal and optional bearer header for a connector's
/// gateway call.
///
/// Connector traffic must authenticate with the dedicated connector token,
/// and its channel must be present in the server-owned allowlist. The admin
/// token (and its bound principal) is deliberately never reused here.
///
/// # Errors
/// Returns a router error when gateway auth is enabled but no connector token
/// is configured, or the connector channel is outside the token allowlist.
#[allow(clippy::result_large_err)]
pub(super) fn resolve_connector_gateway_auth(
    auth: &GatewayAuthConfig,
    connector_principal: &str,
) -> Result<(String, Option<String>), ConnectorSupervisorError> {
    if !auth.require_auth {
        return Ok((connector_principal.to_owned(), None));
    }
    let connector_token = auth.connector_token.as_deref().ok_or_else(|| {
        ConnectorSupervisorError::Router(
            "connector_token is required for RouteMessage when gateway auth is enabled".to_owned(),
        )
    })?;
    let channel = connector_principal.strip_prefix("channel:").ok_or_else(|| {
        ConnectorSupervisorError::Router(
            "connector principal must use the channel:<id> form".to_owned(),
        )
    })?;
    if !auth.connector_allowed_channels.iter().any(|allowed| channel.eq_ignore_ascii_case(allowed))
    {
        return Err(ConnectorSupervisorError::Router(format!(
            "connector channel '{channel}' is not allowed by admin.connector_allowed_channels"
        )));
    }
    Ok((connector_principal.to_owned(), Some(format!("Bearer {connector_token}"))))
}
