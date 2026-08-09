//! Input normalization and validation for profiles, identifiers, and token endpoints.
//!
//! Token endpoints are validated in two phases: a syntactic/literal check at write time
//! (`normalize_token_endpoint`) and a DNS-resolving network-policy check at refresh time
//! (`validate_runtime_token_endpoint`), so a hostname cannot be re-pointed at a private
//! address after registration (DNS rebinding).

use std::{
    collections::BTreeMap,
    net::{IpAddr, ToSocketAddrs},
};

use palyra_common::netguard;
use palyra_vault::VaultRef;
use reqwest::Url;

use crate::{
    constants::{MAX_PROFILE_COUNT, REGISTRY_VERSION},
    error::AuthProfileError,
    models::{
        AuthCredential, AuthProfileListFilter, AuthProfileRecord, AuthProfileScope,
        AuthProfileSetRequest, AuthProvider, AuthProviderKind, AuthScopeFilter, OAuthRefreshError,
        OAuthRefreshState,
    },
    storage::{unix_ms_now, RegistryDocument},
};

/// Normalizes a loaded registry document in place: version check, per-profile
/// normalization, de-duplication by profile id (later entries win), and size limit.
pub(crate) fn normalize_document(document: &mut RegistryDocument) -> Result<(), AuthProfileError> {
    // Version 0 cannot be produced by this crate's serializer; treat it as "unset"
    // from a hand-edited file and upgrade to the current version.
    if document.version == 0 {
        document.version = REGISTRY_VERSION;
    }
    if document.version != REGISTRY_VERSION {
        return Err(AuthProfileError::UnsupportedVersion(document.version));
    }

    let fallback_now = unix_ms_now()?;
    let mut deduped = BTreeMap::<String, AuthProfileRecord>::new();
    for profile in document.profiles.drain(..) {
        let normalized = normalize_profile_record(profile, fallback_now)?;
        deduped.insert(normalized.profile_id.clone(), normalized);
    }
    if deduped.len() > MAX_PROFILE_COUNT {
        return Err(AuthProfileError::RegistryLimitExceeded);
    }
    document.profiles = deduped.into_values().collect();
    document.version = REGISTRY_VERSION;
    Ok(())
}

/// Normalizes every field of an incoming set request before it touches the registry.
pub(crate) fn normalize_set_request(
    request: AuthProfileSetRequest,
) -> Result<AuthProfileSetRequest, AuthProfileError> {
    Ok(AuthProfileSetRequest {
        profile_id: normalize_profile_id(request.profile_id.as_str())?,
        provider: normalize_provider(request.provider)?,
        profile_name: normalize_profile_name(request.profile_name.as_str())?,
        scope: normalize_scope(request.scope)?,
        credential: normalize_credential(request.credential)?,
    })
}

/// Normalizes a stored profile record, repairing absent or inverted timestamps.
pub(crate) fn normalize_profile_record(
    mut record: AuthProfileRecord,
    fallback_now: i64,
) -> Result<AuthProfileRecord, AuthProfileError> {
    record.profile_id = normalize_profile_id(record.profile_id.as_str())?;
    record.provider = normalize_provider(record.provider)?;
    record.profile_name = normalize_profile_name(record.profile_name.as_str())?;
    record.scope = normalize_scope(record.scope)?;
    record.credential = normalize_credential(record.credential)?;

    if record.created_at_unix_ms <= 0 {
        record.created_at_unix_ms = fallback_now;
    }
    if record.updated_at_unix_ms < record.created_at_unix_ms {
        record.updated_at_unix_ms = record.created_at_unix_ms;
    }
    Ok(record)
}

fn normalize_provider(provider: AuthProvider) -> Result<AuthProvider, AuthProfileError> {
    let custom_name = provider.custom_name.as_deref().map(str::trim);
    let provider = match provider.kind {
        AuthProviderKind::Custom => {
            let value = custom_name.filter(|value| !value.is_empty()).ok_or_else(|| {
                AuthProfileError::InvalidField {
                    field: "provider.custom_name",
                    message: "custom providers require non-empty custom_name".to_owned(),
                }
            })?;
            AuthProvider {
                kind: AuthProviderKind::Custom,
                custom_name: Some(normalize_identifier(value, "provider.custom_name", 64)?),
            }
        }
        kind => AuthProvider { kind, custom_name: None },
    };
    Ok(provider)
}

pub(crate) fn normalize_profile_id(raw: &str) -> Result<String, AuthProfileError> {
    normalize_identifier(raw, "profile_id", 128)
}

pub(crate) fn normalize_agent_id(raw: &str) -> Result<String, AuthProfileError> {
    normalize_identifier(raw, "scope.agent_id", 128)
}

/// Trims, lowercases, and restricts an identifier to `[a-z0-9._-]` within `max_len`.
fn normalize_identifier(
    raw: &str,
    field: &'static str,
    max_len: usize,
) -> Result<String, AuthProfileError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(AuthProfileError::InvalidField {
            field,
            message: "value cannot be empty".to_owned(),
        });
    }
    if trimmed.len() > max_len {
        return Err(AuthProfileError::InvalidField {
            field,
            message: format!("value exceeds max length ({max_len})"),
        });
    }
    let normalized = trimmed.to_ascii_lowercase();
    if !normalized.chars().all(|ch| {
        ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-' || ch == '_' || ch == '.'
    }) {
        return Err(AuthProfileError::InvalidField {
            field,
            message: "value contains unsupported characters".to_owned(),
        });
    }
    Ok(normalized)
}

fn normalize_profile_name(raw: &str) -> Result<String, AuthProfileError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(AuthProfileError::InvalidField {
            field: "profile_name",
            message: "value cannot be empty".to_owned(),
        });
    }
    if trimmed.len() > 256 {
        return Err(AuthProfileError::InvalidField {
            field: "profile_name",
            message: "value exceeds max length (256)".to_owned(),
        });
    }
    Ok(trimmed.to_owned())
}

fn normalize_scope(scope: AuthProfileScope) -> Result<AuthProfileScope, AuthProfileError> {
    match scope {
        AuthProfileScope::Global => Ok(AuthProfileScope::Global),
        AuthProfileScope::Agent { agent_id } => {
            Ok(AuthProfileScope::Agent { agent_id: normalize_agent_id(agent_id.as_str())? })
        }
    }
}

fn normalize_credential(credential: AuthCredential) -> Result<AuthCredential, AuthProfileError> {
    match credential {
        AuthCredential::ApiKey { api_key_vault_ref } => Ok(AuthCredential::ApiKey {
            api_key_vault_ref: normalize_vault_ref(
                api_key_vault_ref.as_str(),
                "api_key_vault_ref",
            )?,
        }),
        AuthCredential::Oauth {
            access_token_vault_ref,
            refresh_token_vault_ref,
            token_endpoint,
            client_id,
            client_secret_vault_ref,
            scopes,
            expires_at_unix_ms,
            refresh_state,
        } => Ok(AuthCredential::Oauth {
            access_token_vault_ref: normalize_vault_ref(
                access_token_vault_ref.as_str(),
                "oauth.access_token_vault_ref",
            )?,
            refresh_token_vault_ref: normalize_vault_ref(
                refresh_token_vault_ref.as_str(),
                "oauth.refresh_token_vault_ref",
            )?,
            token_endpoint: normalize_token_endpoint(token_endpoint.as_str())?,
            client_id: normalize_optional_text(client_id, 256),
            client_secret_vault_ref: normalize_optional_vault_ref(
                client_secret_vault_ref,
                "oauth.client_secret_vault_ref",
            )?,
            scopes: normalize_scopes(scopes),
            expires_at_unix_ms: expires_at_unix_ms.filter(|value| *value > 0),
            refresh_state: normalize_refresh_state(refresh_state),
        }),
    }
}

fn normalize_refresh_state(mut value: OAuthRefreshState) -> OAuthRefreshState {
    value.last_error = value.last_error.and_then(|raw| {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_owned())
        }
    });
    value.last_attempt_unix_ms = value.last_attempt_unix_ms.filter(|timestamp| *timestamp > 0);
    value.last_success_unix_ms = value.last_success_unix_ms.filter(|timestamp| *timestamp > 0);
    value.next_allowed_refresh_unix_ms =
        value.next_allowed_refresh_unix_ms.filter(|timestamp| *timestamp > 0);
    value
}

/// Trims free-form text, mapping blank input to `None` and truncating to `max_len` bytes.
pub(crate) fn normalize_optional_text(value: Option<String>, max_len: usize) -> Option<String> {
    value.and_then(|raw| {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            None
        } else if trimmed.len() > max_len {
            // Walk back to a UTF-8 char boundary so the byte-length cut cannot panic
            // or split a multi-byte character.
            let mut end = max_len.min(trimmed.len());
            while end > 0 && !trimmed.is_char_boundary(end) {
                end = end.saturating_sub(1);
            }
            Some(trimmed[..end].to_owned())
        } else {
            Some(trimmed.to_owned())
        }
    })
}

fn normalize_optional_vault_ref(
    value: Option<String>,
    field: &'static str,
) -> Result<Option<String>, AuthProfileError> {
    if let Some(raw) = value {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Ok(None);
        }
        return Ok(Some(normalize_vault_ref(trimmed, field)?));
    }
    Ok(None)
}

/// Validates a vault reference and re-renders it in canonical `scope/key` form.
fn normalize_vault_ref(raw: &str, field: &'static str) -> Result<String, AuthProfileError> {
    let trimmed = raw.trim();
    let parsed = VaultRef::parse(trimmed).map_err(|error| AuthProfileError::InvalidField {
        field,
        message: format!("invalid vault reference: {error}"),
    })?;
    Ok(format!("{}/{}", parsed.scope, parsed.key))
}

/// Write-time token endpoint validation; see the module docs for the two-phase scheme.
pub(crate) fn normalize_token_endpoint(raw: &str) -> Result<String, AuthProfileError> {
    parse_oauth_token_endpoint_url(raw)
        .map_err(|message| AuthProfileError::InvalidField {
            field: "oauth.token_endpoint",
            message,
        })
        .map(|parsed| parsed.to_string())
}

fn is_loopback_endpoint(parsed: &Url) -> bool {
    let Some(host) = parsed.host_str() else {
        return false;
    };
    // Url keeps IPv6 literals bracketed (`[::1]`); strip brackets before IpAddr parsing.
    let normalized_host = host.trim_start_matches('[').trim_end_matches(']');
    if normalized_host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    normalized_host.parse::<IpAddr>().is_ok_and(|address| address.is_loopback())
}

/// Syntactic and literal-host policy for OAuth token endpoints.
///
/// Userinfo, query, and fragment components are rejected because refresh credentials
/// travel in the POST body only; https endpoints must not target localhost/private
/// literals (SSRF), while plain http is permitted solely for loopback development flows.
fn parse_oauth_token_endpoint_url(raw: &str) -> Result<Url, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("value cannot be empty".to_owned());
    }
    let parsed = Url::parse(trimmed).map_err(|error| format!("invalid URL: {error}"))?;
    if parsed.host_str().is_none() {
        return Err("URL must include a host".to_owned());
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err("URL must not include username/password components".to_owned());
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err("URL must not include query or fragment components".to_owned());
    }
    match parsed.scheme() {
        "https" => {
            let host = parsed.host_str().ok_or_else(|| "URL must include a host".to_owned())?;
            if token_endpoint_targets_private_host_literal(host)? {
                return Err("https URL must not target localhost/private network hosts".to_owned());
            }
        }
        "http" => {
            if !is_loopback_endpoint(&parsed) {
                return Err("http URL is allowed only for loopback hosts".to_owned());
            }
        }
        _ => {
            return Err("URL scheme must be https, or http for loopback hosts".to_owned());
        }
    }
    Ok(parsed)
}

fn token_endpoint_targets_private_host_literal(host: &str) -> Result<bool, String> {
    if netguard::is_localhost_hostname(host) {
        return Ok(true);
    }
    Ok(netguard::parse_host_ip_literal(host)?.is_some_and(netguard::is_private_or_local_ip))
}

fn resolve_token_endpoint_addresses(host: &str, port: u16) -> std::io::Result<Vec<IpAddr>> {
    (host, port).to_socket_addrs().map(|resolved| resolved.map(|address| address.ip()).collect())
}

/// Refresh-time token endpoint validation; see the module docs for the two-phase scheme.
pub(crate) fn validate_runtime_token_endpoint(url: &str) -> Result<Url, OAuthRefreshError> {
    validate_runtime_token_endpoint_with_resolver(url, resolve_token_endpoint_addresses)
}

/// Like [`validate_runtime_token_endpoint`] with an injectable resolver for tests.
///
/// # Errors
/// Returns [`OAuthRefreshError::Transport`] when the URL fails syntactic policy, the host
/// cannot be resolved, or any resolved address violates the private-network guard.
pub(crate) fn validate_runtime_token_endpoint_with_resolver<F>(
    url: &str,
    resolver: F,
) -> Result<Url, OAuthRefreshError>
where
    F: Fn(&str, u16) -> std::io::Result<Vec<IpAddr>>,
{
    let parsed = parse_oauth_token_endpoint_url(url).map_err(OAuthRefreshError::Transport)?;
    // http already passed the loopback-only check above; DNS policy applies to https,
    // where a public hostname could be re-pointed at a private address (DNS rebinding).
    if parsed.scheme() == "http" {
        return Ok(parsed);
    }

    let host = parsed
        .host_str()
        .ok_or_else(|| OAuthRefreshError::Transport("URL must include a host".to_owned()))?;
    let port = parsed.port_or_known_default().ok_or_else(|| {
        OAuthRefreshError::Transport("token endpoint URL must use a known default port".to_owned())
    })?;
    let resolved = resolver(host, port).map_err(|error| {
        OAuthRefreshError::Transport(format!(
            "oauth token endpoint host could not be resolved to enforce network policy: {error}"
        ))
    })?;
    netguard::validate_resolved_ip_addrs(resolved.as_slice(), false).map_err(|error| {
        OAuthRefreshError::Transport(format!(
            "oauth token endpoint host violates network policy: {error}"
        ))
    })?;
    Ok(parsed)
}

/// Returns a strictly increasing `updated_at` for a profile mutation.
///
/// The +1 floor guarantees the timestamp advances even when the wall clock has not,
/// which the stale-refresh guard in `persist_refresh_failure` relies on to detect
/// concurrent profile changes by comparing observed vs. current `updated_at`.
pub(crate) fn next_profile_updated_at(previous_updated_at_unix_ms: i64, now_unix_ms: i64) -> i64 {
    previous_updated_at_unix_ms.saturating_add(1).max(now_unix_ms)
}

/// Trims, drops empties, de-duplicates, and sorts OAuth scopes.
fn normalize_scopes(input: Vec<String>) -> Vec<String> {
    let mut deduped = BTreeMap::<String, ()>::new();
    for raw in input {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            deduped.insert(trimmed.to_owned(), ());
        }
    }
    deduped.into_keys().collect()
}

pub(crate) fn profile_matches_filter(
    profile: &AuthProfileRecord,
    filter: &AuthProfileListFilter,
) -> bool {
    if let Some(provider) = filter.provider.as_ref() {
        if profile.provider.canonical_key() != provider.canonical_key() {
            return false;
        }
    }
    if let Some(scope) = filter.scope.as_ref() {
        if !profile_scope_matches_filter(&profile.scope, scope) {
            return false;
        }
    }
    true
}

pub(crate) fn profile_scope_matches_filter(
    profile_scope: &AuthProfileScope,
    filter: &AuthScopeFilter,
) -> bool {
    match (filter, profile_scope) {
        (AuthScopeFilter::Global, AuthProfileScope::Global) => true,
        (
            AuthScopeFilter::Agent { agent_id: left },
            AuthProfileScope::Agent { agent_id: right },
        ) => left.eq_ignore_ascii_case(right),
        _ => false,
    }
}

/// Returns the provider+name key under which agent-scoped profiles shadow global ones
/// in `merged_profiles_for_agent`.
pub(crate) fn profile_merge_key(profile: &AuthProfileRecord) -> String {
    format!("{}:{}", profile.provider.canonical_key(), profile.profile_name.to_ascii_lowercase())
}
