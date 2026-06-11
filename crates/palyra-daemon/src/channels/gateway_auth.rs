//! Gateway authentication for connector-originated RouteMessage calls.

use crate::transport::grpc::auth::GatewayAuthConfig;

use palyra_connectors::ConnectorSupervisorError;

/// Resolves the principal and optional bearer header for a connector's
/// gateway call.
///
/// Connector traffic must authenticate with the dedicated connector token;
/// the admin token (and its bound principal) is deliberately never reused
/// here, so the channel principal is preserved end to end.
///
/// # Errors
/// Returns a router error when gateway auth is enabled but no connector
/// token is configured.
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
    Ok((connector_principal.to_owned(), Some(format!("Bearer {connector_token}"))))
}
