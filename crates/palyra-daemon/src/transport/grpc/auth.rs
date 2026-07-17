//! Authentication and request-context extraction for daemon gRPC services.
//!
//! Admin tokens authorize general gateway methods; the connector token is
//! intentionally narrower and can only route messages for its bound channel.

use axum::http::{header::AUTHORIZATION, HeaderMap};
use palyra_common::validate_canonical_id;
use serde::{Deserialize, Serialize};
use tonic::metadata::MetadataMap;

/// Metadata/header key carrying the authenticated runtime principal.
pub const HEADER_PRINCIPAL: &str = "x-palyra-principal";
/// Metadata/header key carrying the authenticated runtime device id.
pub const HEADER_DEVICE_ID: &str = "x-palyra-device-id";
/// Metadata/header key carrying the optional originating channel.
pub const HEADER_CHANNEL: &str = "x-palyra-channel";

/// gRPC and gateway HTTP authentication settings loaded from daemon config.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayAuthConfig {
    /// Whether bearer-token authentication is enforced.
    pub require_auth: bool,
    /// Admin bearer token for privileged gateway calls.
    pub admin_token: Option<String>,
    /// Connector bearer token for `RouteMessage` calls from channel adapters.
    pub connector_token: Option<String>,
    /// Optional principal that the admin token is allowed to impersonate.
    pub bound_principal: Option<String>,
}

/// Runtime caller context extracted from authenticated request metadata.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RequestContext {
    /// Principal used for authorization and journal attribution.
    pub principal: String,
    /// Canonical device id used for runtime attribution.
    pub device_id: String,
    /// Optional channel scope attached by channel adapters and console flows.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
}

/// Authentication or metadata validation failure.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AuthError {
    /// Auth is required but no admin token is configured.
    #[error("admin auth token is required but no token is configured")]
    MissingConfiguredToken,
    /// The bearer authorization header is missing or malformed.
    #[error("authorization header is missing or malformed")]
    InvalidAuthorizationHeader,
    /// The supplied bearer token did not match an accepted token.
    #[error("authorization token is invalid")]
    InvalidToken,
    /// Required request-context metadata was not present.
    #[error("request context field '{0}' is required")]
    MissingContext(&'static str),
    /// Required request-context metadata was present but blank.
    #[error("request context field '{0}' cannot be empty")]
    EmptyContext(&'static str),
    /// The device id metadata was not a canonical ULID.
    #[error("request context device_id must be a canonical ULID")]
    InvalidDeviceId,
}

/// Authorizes HTTP headers against the gateway admin token.
///
/// # Errors
/// Returns an auth error when auth is required and the bearer token is missing,
/// invalid, not configured, or bound to a different principal.
pub fn authorize_headers(headers: &HeaderMap, auth: &GatewayAuthConfig) -> Result<(), AuthError> {
    if !auth.require_auth {
        return Ok(());
    }
    let token = auth.admin_token.as_ref().ok_or(AuthError::MissingConfiguredToken)?;
    let candidate =
        extract_bearer_token(headers.get(AUTHORIZATION).and_then(|value| value.to_str().ok()))
            .ok_or(AuthError::InvalidAuthorizationHeader)?;
    if constant_time_eq(token.as_bytes(), candidate.as_bytes()) {
        let principal = require_context_value(
            &|name| headers.get(name).and_then(|value| value.to_str().ok()).map(ToOwned::to_owned),
            HEADER_PRINCIPAL,
        )?;
        enforce_token_principal_binding(principal.as_str(), auth)
    } else {
        Err(AuthError::InvalidToken)
    }
}

/// Returns the exact principal bound by host configuration to an authenticated admin token.
///
/// A valid bearer token without `bound_principal` authenticates the credential but not the
/// caller-controlled principal header, so it cannot authorize actor-sensitive recovery.
#[must_use]
pub(crate) fn bound_admin_actor_principal<'a>(
    auth: &'a GatewayAuthConfig,
    context: &RequestContext,
) -> Option<&'a str> {
    if !auth.require_auth {
        return None;
    }
    auth.bound_principal.as_deref().filter(|principal| *principal == context.principal)
}

/// Extracts a validated request context from HTTP headers.
///
/// # Errors
/// Returns an auth error when required context fields are missing, blank, or
/// the device id is not canonical.
pub fn request_context_from_headers(headers: &HeaderMap) -> Result<RequestContext, AuthError> {
    request_context_from_header_resolver(|name| {
        headers.get(name).and_then(|value| value.to_str().ok()).map(ToOwned::to_owned)
    })
}

/// Authorizes gRPC metadata and returns the validated request context.
///
/// # Errors
/// Returns an auth error when bearer auth fails, when the connector token is
/// used outside `RouteMessage`, or when required context metadata is invalid.
pub(crate) fn authorize_metadata(
    metadata: &MetadataMap,
    auth: &GatewayAuthConfig,
    method: &'static str,
) -> Result<RequestContext, AuthError> {
    let token_kind = if auth.require_auth {
        let candidate = extract_bearer_token(
            metadata.get(AUTHORIZATION.as_str()).and_then(|value| value.to_str().ok()),
        )
        .ok_or(AuthError::InvalidAuthorizationHeader)?;
        resolve_rpc_token_kind(candidate, auth, method)?
    } else {
        RpcTokenKind::Admin
    };

    let context = request_context_from_header_resolver(|name| {
        metadata.get(name).and_then(|value| value.to_str().ok()).map(ToOwned::to_owned)
    })?;
    match token_kind {
        RpcTokenKind::Admin => enforce_token_principal_binding(context.principal.as_str(), auth)?,
        RpcTokenKind::Connector => enforce_connector_token_context_binding(&context)?,
    }
    Ok(context)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RpcTokenKind {
    Admin,
    Connector,
}

fn resolve_rpc_token_kind(
    candidate: &str,
    auth: &GatewayAuthConfig,
    method: &'static str,
) -> Result<RpcTokenKind, AuthError> {
    let admin_token = auth.admin_token.as_ref().ok_or(AuthError::MissingConfiguredToken)?;
    if constant_time_eq(admin_token.as_bytes(), candidate.as_bytes()) {
        return Ok(RpcTokenKind::Admin);
    }
    if method.eq_ignore_ascii_case("RouteMessage") {
        if let Some(connector_token) = auth.connector_token.as_deref() {
            if constant_time_eq(connector_token.as_bytes(), candidate.as_bytes()) {
                return Ok(RpcTokenKind::Connector);
            }
        }
    }
    Err(AuthError::InvalidToken)
}

fn enforce_connector_token_context_binding(context: &RequestContext) -> Result<(), AuthError> {
    let Some(channel) = context.channel.as_deref().map(str::trim).filter(|value| !value.is_empty())
    else {
        return Err(AuthError::InvalidToken);
    };
    let expected_principal = format!("channel:{channel}");
    if context.principal.eq_ignore_ascii_case(expected_principal.as_str()) {
        Ok(())
    } else {
        Err(AuthError::InvalidToken)
    }
}

fn enforce_token_principal_binding(
    principal: &str,
    auth: &GatewayAuthConfig,
) -> Result<(), AuthError> {
    if !auth.require_auth {
        return Ok(());
    }
    let Some(expected_principal) = auth.bound_principal.as_ref() else {
        return Ok(());
    };
    if principal == expected_principal {
        Ok(())
    } else {
        Err(AuthError::InvalidToken)
    }
}

fn request_context_from_header_resolver<F>(resolver: F) -> Result<RequestContext, AuthError>
where
    F: Fn(&'static str) -> Option<String>,
{
    let principal = require_context_value(&resolver, HEADER_PRINCIPAL)?;
    let device_id = require_context_value(&resolver, HEADER_DEVICE_ID)?;
    validate_canonical_id(device_id.as_str()).map_err(|_| AuthError::InvalidDeviceId)?;
    let channel = optional_context_value(&resolver, HEADER_CHANNEL)?;

    Ok(RequestContext { principal, device_id, channel })
}

fn require_context_value<F>(resolver: &F, key: &'static str) -> Result<String, AuthError>
where
    F: Fn(&'static str) -> Option<String>,
{
    let value = resolver(key).ok_or(AuthError::MissingContext(key))?;
    let value = value.trim();
    if value.is_empty() {
        return Err(AuthError::EmptyContext(key));
    }
    Ok(value.to_owned())
}

fn optional_context_value<F>(resolver: &F, key: &'static str) -> Result<Option<String>, AuthError>
where
    F: Fn(&'static str) -> Option<String>,
{
    let Some(value) = resolver(key) else {
        return Ok(None);
    };
    let value = value.trim();
    if value.is_empty() {
        return Err(AuthError::EmptyContext(key));
    }
    Ok(Some(value.to_owned()))
}

fn extract_bearer_token(raw: Option<&str>) -> Option<&str> {
    let value = raw?.trim();
    let separator_index = value.find(char::is_whitespace)?;
    let (scheme, remainder) = value.split_at(separator_index);
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = remainder.trim();
    if token.is_empty() {
        return None;
    }
    Some(token)
}

/// Compares byte slices without content-dependent early return.
pub(crate) fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let max_len = left.len().max(right.len());
    let mut diff = left.len() ^ right.len();
    for index in 0..max_len {
        let lhs = left.get(index).copied().unwrap_or_default();
        let rhs = right.get(index).copied().unwrap_or_default();
        diff |= usize::from(lhs ^ rhs);
    }
    diff == 0
}
