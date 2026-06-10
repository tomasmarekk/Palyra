//! OAuth token refresh: HTTP adapter, backoff policy, health evaluation, vault I/O.
//!
//! Refresh secrets are loaded from the vault just-in-time and exist in memory only for
//! the duration of one refresh call; failure reasons persisted to disk pass through
//! [`sanitize_refresh_error`] so provider response text never reaches stored state.

use std::time::Duration;

use palyra_vault::{Vault, VaultError, VaultRef};
use reqwest::blocking::Client;
use serde_json::Value;

use crate::{
    constants::DEFAULT_REFRESH_TIMEOUT_SECS,
    error::AuthProfileError,
    models::{
        AuthCredential, AuthCredentialType, AuthExpiryDistribution, AuthProfileHealthRecord,
        AuthProfileHealthState, AuthProfileRecord, AuthProvider, AuthProviderKind,
        OAuthRefreshError, OAuthRefreshRequest, OAuthRefreshResponse,
    },
    validation::validate_runtime_token_endpoint,
};

/// Exchanges a refresh token for a new access token at a provider's token endpoint.
///
/// Implementations receive raw secret material in the request and must not log it.
pub trait OAuthRefreshAdapter: Send + Sync {
    /// Performs one `refresh_token` grant against the request's token endpoint.
    ///
    /// # Errors
    /// Returns [`OAuthRefreshError`] on transport failure, a non-success HTTP status,
    /// or a response body missing a usable `access_token`.
    fn refresh_access_token(
        &self,
        request: &OAuthRefreshRequest,
    ) -> Result<OAuthRefreshResponse, OAuthRefreshError>;
}

/// Production [`OAuthRefreshAdapter`] performing blocking HTTPS form-POST refreshes.
#[derive(Debug)]
pub struct HttpOAuthRefreshAdapter {
    timeout: Duration,
}

impl HttpOAuthRefreshAdapter {
    /// Creates an adapter with the given request timeout.
    ///
    /// # Errors
    /// Returns [`AuthProfileError::InvalidField`] when `timeout` is zero, which would
    /// disable the timeout entirely.
    pub fn with_timeout(timeout: Duration) -> Result<Self, AuthProfileError> {
        if timeout.is_zero() {
            return Err(AuthProfileError::InvalidField {
                field: "oauth.timeout",
                message: "timeout must be greater than zero".to_owned(),
            });
        }
        Ok(Self { timeout })
    }
}

impl Default for HttpOAuthRefreshAdapter {
    fn default() -> Self {
        Self::with_timeout(Duration::from_secs(DEFAULT_REFRESH_TIMEOUT_SECS))
            .expect("DEFAULT_REFRESH_TIMEOUT_SECS is non-zero, so construction cannot fail")
    }
}

impl OAuthRefreshAdapter for HttpOAuthRefreshAdapter {
    fn refresh_access_token(
        &self,
        request: &OAuthRefreshRequest,
    ) -> Result<OAuthRefreshResponse, OAuthRefreshError> {
        // Re-validate at request time even though the registry validated at write time:
        // DNS may have been re-pointed at a private address since registration.
        let token_endpoint = validate_runtime_token_endpoint(request.token_endpoint.as_str())?;
        // Redirects are refused so credentials in the POST body cannot be replayed to an
        // attacker-chosen Location; the proxy bypass keeps token traffic off egress
        // proxies that this crate cannot vet.
        let client = Client::builder()
            .timeout(self.timeout)
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .build()
            .map_err(|error| OAuthRefreshError::Transport(error.to_string()))?;
        let mut form_fields = vec![
            ("grant_type".to_owned(), "refresh_token".to_owned()),
            ("refresh_token".to_owned(), request.refresh_token.clone()),
        ];
        if let Some(client_id) = request.client_id.as_deref() {
            form_fields.push(("client_id".to_owned(), client_id.to_owned()));
        }
        if let Some(client_secret) = request.client_secret.as_deref() {
            form_fields.push(("client_secret".to_owned(), client_secret.to_owned()));
        }
        if !request.scopes.is_empty() {
            form_fields.push(("scope".to_owned(), request.scopes.join(" ")));
        }

        let response = client
            .post(token_endpoint)
            .form(&form_fields)
            .send()
            .map_err(|error| OAuthRefreshError::Transport(error.to_string()))?;
        let status = response.status();
        let payload =
            response.text().map_err(|error| OAuthRefreshError::Transport(error.to_string()))?;
        if !status.is_success() {
            return Err(OAuthRefreshError::HttpStatus { status: status.as_u16() });
        }

        let parsed: Value = serde_json::from_str(payload.as_str()).map_err(|error| {
            OAuthRefreshError::InvalidResponse(format!("response body is not JSON: {error}"))
        })?;
        let access_token = parsed
            .get("access_token")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                OAuthRefreshError::InvalidResponse(
                    "response is missing non-empty 'access_token'".to_owned(),
                )
            })?
            .to_owned();
        let refresh_token = parsed
            .get("refresh_token")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
        // Some providers ship the non-standard `expired_in` spelling; accept it as an
        // alias rather than treating their tokens as never-expiring.
        let expires_in_seconds = parse_oauth_expires_in(&parsed, "expires_in")
            .or_else(|| parse_oauth_expires_in(&parsed, "expired_in"));

        Ok(OAuthRefreshResponse { access_token, refresh_token, expires_in_seconds })
    }
}

/// Reads an expiry field that providers send as either a JSON number or a numeric string.
fn parse_oauth_expires_in(parsed: &Value, field: &str) -> Option<u64> {
    match parsed.get(field) {
        Some(Value::Number(value)) => value.as_u64(),
        Some(Value::String(value)) => value.parse::<u64>().ok(),
        Some(_) | None => None,
    }
}

/// How a refresh attempt for one profile concluded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OAuthRefreshOutcomeKind {
    /// Profile uses api-key credentials; there is nothing to refresh.
    SkippedNotOauth,
    /// Token is not within the refresh window yet.
    SkippedNotDue,
    /// A failure cooldown is still active for this profile.
    SkippedCooldown,
    Succeeded,
    Failed,
}

impl OAuthRefreshOutcomeKind {
    /// Returns whether a refresh request was actually sent (succeeded or failed).
    #[must_use]
    pub const fn attempted(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed)
    }

    /// Returns whether the refresh attempt succeeded.
    #[must_use]
    pub const fn success(self) -> bool {
        matches!(self, Self::Succeeded)
    }
}

/// Result of one refresh attempt, with a sanitized human-readable reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OAuthRefreshOutcome {
    pub profile_id: String,
    pub provider: String,
    pub kind: OAuthRefreshOutcomeKind,
    /// Sanitized explanation; never contains provider response text or secrets.
    pub reason: String,
    pub next_allowed_refresh_unix_ms: Option<i64>,
    pub expires_at_unix_ms: Option<i64>,
}

/// Exponential-backoff bounds applied to refresh failures for one provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderBackoffPolicy {
    pub base_backoff_ms: u64,
    pub max_backoff_ms: u64,
}

/// Returns the per-provider backoff bounds for refresh failures.
pub fn provider_backoff_policy(provider: &AuthProvider) -> ProviderBackoffPolicy {
    match provider.kind {
        AuthProviderKind::Openai => {
            ProviderBackoffPolicy { base_backoff_ms: 15_000, max_backoff_ms: 5 * 60 * 1_000 }
        }
        AuthProviderKind::Anthropic => {
            ProviderBackoffPolicy { base_backoff_ms: 20_000, max_backoff_ms: 10 * 60 * 1_000 }
        }
        AuthProviderKind::Telegram
        | AuthProviderKind::Slack
        | AuthProviderKind::Discord
        | AuthProviderKind::Webhook
        | AuthProviderKind::Custom => {
            ProviderBackoffPolicy { base_backoff_ms: 30_000, max_backoff_ms: 30 * 60 * 1_000 }
        }
    }
}

/// Computes the cooldown after `failure_count` consecutive refresh failures: base
/// backoff doubled per failure, capped at the provider's maximum.
pub fn compute_backoff_ms(provider: &AuthProvider, failure_count: u32) -> u64 {
    let policy = provider_backoff_policy(provider);
    if failure_count == 0 {
        return policy.base_backoff_ms;
    }
    // Cap the exponent so the shift itself cannot overflow; saturating_mul plus the
    // max_backoff_ms clamp bound the result regardless.
    let shift = failure_count.saturating_sub(1).min(20);
    let factor = 1_u64 << shift;
    policy.base_backoff_ms.saturating_mul(factor).min(policy.max_backoff_ms)
}

/// Copy of the OAuth fields needed for one refresh, taken under the registry lock so the
/// network call can run without holding it.
///
/// `observed_updated_at_unix_ms` records the profile's `updated_at` at snapshot time;
/// `persist_refresh_failure` compares it against the live value so a stale failure from a
/// lost race cannot overwrite a concurrent successful refresh.
#[derive(Debug, Clone)]
pub(crate) struct OAuthRefreshSnapshot {
    pub(crate) profile_id: String,
    pub(crate) provider: AuthProvider,
    pub(crate) access_token_vault_ref: String,
    pub(crate) refresh_token_vault_ref: String,
    pub(crate) token_endpoint: String,
    pub(crate) client_id: Option<String>,
    pub(crate) client_secret_vault_ref: Option<String>,
    pub(crate) scopes: Vec<String>,
    pub(crate) expires_at_unix_ms: Option<i64>,
    pub(crate) failure_count: u32,
    pub(crate) observed_updated_at_unix_ms: i64,
}

/// Either a snapshot ready for the network call, or an early skip outcome.
pub(crate) enum PreparedOAuthRefresh {
    Snapshot(OAuthRefreshSnapshot),
    Outcome(OAuthRefreshOutcome),
}

/// Decides whether a profile may be refreshed now and snapshots it if so.
pub(crate) fn prepare_oauth_refresh_snapshot(
    profile: &AuthProfileRecord,
    now_unix_ms: i64,
) -> PreparedOAuthRefresh {
    let AuthCredential::Oauth {
        access_token_vault_ref,
        refresh_token_vault_ref,
        token_endpoint,
        client_id,
        client_secret_vault_ref,
        scopes,
        expires_at_unix_ms,
        refresh_state,
    } = &profile.credential
    else {
        return PreparedOAuthRefresh::Outcome(OAuthRefreshOutcome {
            profile_id: profile.profile_id.clone(),
            provider: profile.provider.label(),
            kind: OAuthRefreshOutcomeKind::SkippedNotOauth,
            reason: "refresh skipped because profile uses api_key credentials".to_owned(),
            next_allowed_refresh_unix_ms: None,
            expires_at_unix_ms: None,
        });
    };
    if let Some(next_allowed) = refresh_state.next_allowed_refresh_unix_ms {
        if now_unix_ms < next_allowed {
            return PreparedOAuthRefresh::Outcome(OAuthRefreshOutcome {
                profile_id: profile.profile_id.clone(),
                provider: profile.provider.label(),
                kind: OAuthRefreshOutcomeKind::SkippedCooldown,
                reason: "refresh skipped due to cooldown after previous failures".to_owned(),
                next_allowed_refresh_unix_ms: Some(next_allowed),
                expires_at_unix_ms: *expires_at_unix_ms,
            });
        }
    }

    PreparedOAuthRefresh::Snapshot(OAuthRefreshSnapshot {
        profile_id: profile.profile_id.clone(),
        provider: profile.provider.clone(),
        access_token_vault_ref: access_token_vault_ref.clone(),
        refresh_token_vault_ref: refresh_token_vault_ref.clone(),
        token_endpoint: token_endpoint.clone(),
        client_id: client_id.clone(),
        client_secret_vault_ref: client_secret_vault_ref.clone(),
        scopes: scopes.clone(),
        expires_at_unix_ms: *expires_at_unix_ms,
        failure_count: refresh_state.failure_count,
        observed_updated_at_unix_ms: profile.updated_at_unix_ms,
    })
}

/// Maps a refresh error to a category string safe to persist and display.
///
/// `Transport` and `InvalidResponse` payloads can quote provider response bodies, which
/// may embed token material — only the category (plus the bare HTTP status) survives.
pub(crate) fn sanitize_refresh_error(error: &OAuthRefreshError) -> String {
    match error {
        OAuthRefreshError::Transport(_) => "oauth refresh transport failure".to_owned(),
        OAuthRefreshError::HttpStatus { status } => {
            format!("oauth refresh endpoint returned status {status}")
        }
        OAuthRefreshError::InvalidResponse(_) => "oauth refresh response was invalid".to_owned(),
    }
}

/// Stores a UTF-8 secret value under a `scope/key` vault reference.
pub(crate) fn persist_secret_utf8(
    vault: &Vault,
    vault_ref: &str,
    value: &str,
) -> Result<(), VaultError> {
    let parsed = VaultRef::parse(vault_ref)?;
    vault.put_secret(&parsed.scope, parsed.key.as_str(), value.as_bytes())?;
    Ok(())
}

/// Loads a secret by vault reference, requiring valid UTF-8 and non-blank content.
///
/// Blank values are reported as `NotFound` so callers treat an empty placeholder
/// exactly like an absent credential (e.g. trigger a refresh instead of using it).
pub(crate) fn load_secret_utf8(vault: &Vault, vault_ref: &str) -> Result<String, VaultError> {
    let parsed = VaultRef::parse(vault_ref)?;
    let raw = vault.get_secret(&parsed.scope, parsed.key.as_str())?;
    let decoded = String::from_utf8(raw)
        .map_err(|error| VaultError::Crypto(format!("secret must be valid UTF-8: {error}")))?;
    if decoded.trim().is_empty() {
        return Err(VaultError::NotFound);
    }
    Ok(decoded)
}

/// Returns whether an OAuth profile is due for refresh: always when either token secret
/// is missing from the vault, otherwise only when a known expiry falls inside the
/// refresh window (an unknown expiry is never proactively refreshed).
pub(crate) fn should_attempt_oauth_refresh(
    profile: &AuthProfileRecord,
    vault: &Vault,
    now_unix_ms: i64,
    refresh_window_ms: i64,
) -> bool {
    let AuthCredential::Oauth {
        access_token_vault_ref,
        refresh_token_vault_ref,
        expires_at_unix_ms,
        ..
    } = &profile.credential
    else {
        return false;
    };

    let access_exists = vault_secret_exists(vault, access_token_vault_ref);
    let refresh_exists = vault_secret_exists(vault, refresh_token_vault_ref);

    if !access_exists || !refresh_exists {
        return true;
    }

    let Some(expires_at_unix_ms) = expires_at_unix_ms else {
        return false;
    };
    *expires_at_unix_ms <= now_unix_ms.saturating_add(refresh_window_ms)
}

/// Returns the OAuth access-token expiry, or `None` for api-key profiles.
pub(crate) fn oauth_expires_at(profile: &AuthProfileRecord) -> Option<i64> {
    let AuthCredential::Oauth { expires_at_unix_ms, .. } = &profile.credential else {
        return None;
    };
    *expires_at_unix_ms
}

/// Classifies a profile's credential health from vault presence and token expiry.
pub(crate) fn evaluate_profile_health(
    profile: &AuthProfileRecord,
    vault: &Vault,
    now_unix_ms: i64,
    expiring_window_ms: i64,
) -> AuthProfileHealthRecord {
    let scope = profile.scope.scope_key();
    let provider = profile.provider.label();

    match &profile.credential {
        AuthCredential::ApiKey { api_key_vault_ref } => {
            if vault_secret_exists(vault, api_key_vault_ref) {
                AuthProfileHealthRecord {
                    profile_id: profile.profile_id.clone(),
                    provider,
                    profile_name: profile.profile_name.clone(),
                    scope,
                    credential_type: AuthCredentialType::ApiKey,
                    state: AuthProfileHealthState::Static,
                    reason: "api key credential is present (static token)".to_owned(),
                    expires_at_unix_ms: None,
                }
            } else {
                AuthProfileHealthRecord {
                    profile_id: profile.profile_id.clone(),
                    provider,
                    profile_name: profile.profile_name.clone(),
                    scope,
                    credential_type: AuthCredentialType::ApiKey,
                    state: AuthProfileHealthState::Missing,
                    reason: "api key vault reference is missing or unreadable".to_owned(),
                    expires_at_unix_ms: None,
                }
            }
        }
        AuthCredential::Oauth {
            access_token_vault_ref,
            refresh_token_vault_ref,
            expires_at_unix_ms,
            ..
        } => {
            let access_exists = vault_secret_exists(vault, access_token_vault_ref);
            let refresh_exists = vault_secret_exists(vault, refresh_token_vault_ref);
            if !refresh_exists {
                return AuthProfileHealthRecord {
                    profile_id: profile.profile_id.clone(),
                    provider,
                    profile_name: profile.profile_name.clone(),
                    scope,
                    credential_type: AuthCredentialType::Oauth,
                    state: AuthProfileHealthState::Missing,
                    reason: "oauth refresh token vault reference is missing or unreadable"
                        .to_owned(),
                    expires_at_unix_ms: *expires_at_unix_ms,
                };
            }
            if !access_exists {
                return AuthProfileHealthRecord {
                    profile_id: profile.profile_id.clone(),
                    provider,
                    profile_name: profile.profile_name.clone(),
                    scope,
                    credential_type: AuthCredentialType::Oauth,
                    state: AuthProfileHealthState::Missing,
                    reason: "oauth access token vault reference is missing; refresh required"
                        .to_owned(),
                    expires_at_unix_ms: *expires_at_unix_ms,
                };
            }

            let Some(expires_at_unix_ms) = expires_at_unix_ms else {
                return AuthProfileHealthRecord {
                    profile_id: profile.profile_id.clone(),
                    provider,
                    profile_name: profile.profile_name.clone(),
                    scope,
                    credential_type: AuthCredentialType::Oauth,
                    state: AuthProfileHealthState::Ok,
                    reason: "oauth access token is present; expiry is not provided".to_owned(),
                    expires_at_unix_ms: None,
                };
            };

            let remaining_ms = expires_at_unix_ms.saturating_sub(now_unix_ms);
            if remaining_ms <= 0 {
                return AuthProfileHealthRecord {
                    profile_id: profile.profile_id.clone(),
                    provider,
                    profile_name: profile.profile_name.clone(),
                    scope,
                    credential_type: AuthCredentialType::Oauth,
                    state: AuthProfileHealthState::Expired,
                    reason: "oauth access token has expired".to_owned(),
                    expires_at_unix_ms: Some(*expires_at_unix_ms),
                };
            }
            if remaining_ms <= expiring_window_ms {
                return AuthProfileHealthRecord {
                    profile_id: profile.profile_id.clone(),
                    provider,
                    profile_name: profile.profile_name.clone(),
                    scope,
                    credential_type: AuthCredentialType::Oauth,
                    state: AuthProfileHealthState::Expiring,
                    reason: "oauth access token is nearing expiration".to_owned(),
                    expires_at_unix_ms: Some(*expires_at_unix_ms),
                };
            }
            AuthProfileHealthRecord {
                profile_id: profile.profile_id.clone(),
                provider,
                profile_name: profile.profile_name.clone(),
                scope,
                credential_type: AuthCredentialType::Oauth,
                state: AuthProfileHealthState::Ok,
                reason: "oauth access token is healthy".to_owned(),
                expires_at_unix_ms: Some(*expires_at_unix_ms),
            }
        }
    }
}

/// Returns whether a vault reference resolves to a non-empty secret; any vault error
/// counts as "missing" because health checks must not fail on unreadable backends.
fn vault_secret_exists(vault: &Vault, vault_ref: &str) -> bool {
    let parsed = match VaultRef::parse(vault_ref) {
        Ok(value) => value,
        Err(_) => return false,
    };
    match vault.get_secret(&parsed.scope, parsed.key.as_str()) {
        Ok(value) => !value.is_empty(),
        Err(_) => false,
    }
}

/// Adds one profile's health result to the expiry histogram.
pub(crate) fn update_expiry_distribution(
    distribution: &mut AuthExpiryDistribution,
    state: AuthProfileHealthState,
    expires_at_unix_ms: Option<i64>,
    now_unix_ms: i64,
) {
    match state {
        AuthProfileHealthState::Missing => {
            distribution.missing = distribution.missing.saturating_add(1);
        }
        AuthProfileHealthState::Static => {
            distribution.static_count = distribution.static_count.saturating_add(1);
        }
        AuthProfileHealthState::Expired => {
            distribution.expired = distribution.expired.saturating_add(1);
        }
        AuthProfileHealthState::Expiring | AuthProfileHealthState::Ok => {
            let Some(expires_at_unix_ms) = expires_at_unix_ms else {
                distribution.unknown = distribution.unknown.saturating_add(1);
                return;
            };
            let remaining_ms = expires_at_unix_ms.saturating_sub(now_unix_ms);
            if remaining_ms <= 0 {
                distribution.expired = distribution.expired.saturating_add(1);
            } else if remaining_ms <= 5 * 60 * 1_000 {
                distribution.under_5m = distribution.under_5m.saturating_add(1);
            } else if remaining_ms <= 15 * 60 * 1_000 {
                distribution.between_5m_15m = distribution.between_5m_15m.saturating_add(1);
            } else if remaining_ms <= 60 * 60 * 1_000 {
                distribution.between_15m_60m = distribution.between_15m_60m.saturating_add(1);
            } else if remaining_ms <= 24 * 60 * 60 * 1_000 {
                distribution.between_1h_24h = distribution.between_1h_24h.saturating_add(1);
            } else {
                distribution.over_24h = distribution.over_24h.saturating_add(1);
            }
        }
    }
}
