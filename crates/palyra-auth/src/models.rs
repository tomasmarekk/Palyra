//! Data model for auth profiles, credentials, health, runtime state, and selection.
//!
//! Credential structs carry vault references (`scope/key` strings), never secret values;
//! the only types holding raw token material are the in-memory [`OAuthRefreshRequest`] /
//! [`OAuthRefreshResponse`] pair exchanged with refresh adapters. Serde shapes here define
//! the on-disk TOML registry format and console/CLI payloads — field names are contract.

use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

/// Provider family an auth profile authenticates against.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum AuthProviderKind {
    Openai,
    Anthropic,
    Telegram,
    Slack,
    Discord,
    Webhook,
    /// Provider outside the built-in set; requires [`AuthProvider::custom_name`].
    Custom,
}

/// A provider identity: a built-in kind, or `Custom` plus a normalized custom name.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct AuthProvider {
    pub kind: AuthProviderKind,
    /// Set only when `kind` is [`AuthProviderKind::Custom`]; cleared otherwise.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom_name: Option<String>,
}

impl AuthProvider {
    /// Builds a provider for a built-in kind with no custom name.
    #[must_use]
    pub const fn known(kind: AuthProviderKind) -> Self {
        Self { kind, custom_name: None }
    }

    /// Returns the lowercase display label (the custom name for custom providers).
    #[must_use]
    pub fn label(&self) -> String {
        match self.kind {
            AuthProviderKind::Openai => "openai".to_owned(),
            AuthProviderKind::Anthropic => "anthropic".to_owned(),
            AuthProviderKind::Telegram => "telegram".to_owned(),
            AuthProviderKind::Slack => "slack".to_owned(),
            AuthProviderKind::Discord => "discord".to_owned(),
            AuthProviderKind::Webhook => "webhook".to_owned(),
            AuthProviderKind::Custom => {
                self.custom_name.clone().unwrap_or_else(|| "custom".to_owned()).to_ascii_lowercase()
            }
        }
    }

    /// Returns a stable identity key used for provider comparison and ordering records.
    ///
    /// Custom providers are prefixed with `custom:` so a custom provider named like a
    /// built-in (e.g. "openai") can never collide with the built-in's key.
    #[must_use]
    pub fn canonical_key(&self) -> String {
        if self.kind == AuthProviderKind::Custom {
            return format!("custom:{}", self.label());
        }
        self.label()
    }
}

/// Visibility scope of a profile: shared globally or owned by a single agent.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AuthProfileScope {
    Global,
    Agent { agent_id: String },
}

impl AuthProfileScope {
    /// Returns the stable string form (`global` or `agent:<id>`) used in persisted records.
    #[must_use]
    pub fn scope_key(&self) -> String {
        match self {
            Self::Global => "global".to_owned(),
            Self::Agent { agent_id } => format!("agent:{agent_id}"),
        }
    }
}

/// Kind of credential a profile carries.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum AuthCredentialType {
    ApiKey,
    Oauth,
}

/// Persisted bookkeeping for OAuth refresh attempts on a profile.
///
/// `last_error` only ever stores the sanitized failure category (see
/// `sanitize_refresh_error`), never raw provider responses or token material.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct OAuthRefreshState {
    pub failure_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_attempt_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_success_unix_ms: Option<i64>,
    /// Cooldown deadline after failures; refresh attempts before it are skipped.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_allowed_refresh_unix_ms: Option<i64>,
}

/// Credential descriptor stored on a profile.
///
/// All `*_vault_ref` fields are `scope/key` vault references; the secret values
/// themselves live exclusively in the vault and are loaded just-in-time.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type")]
#[allow(clippy::large_enum_variant)]
pub enum AuthCredential {
    #[serde(rename = "api_key")]
    ApiKey { api_key_vault_ref: String },
    #[serde(rename = "oauth")]
    Oauth {
        access_token_vault_ref: String,
        refresh_token_vault_ref: String,
        token_endpoint: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        client_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        client_secret_vault_ref: Option<String>,
        #[serde(default)]
        scopes: Vec<String>,
        /// Access-token expiry; `None` means the provider did not report one.
        #[serde(skip_serializing_if = "Option::is_none")]
        expires_at_unix_ms: Option<i64>,
        #[serde(default)]
        refresh_state: OAuthRefreshState,
    },
}

impl AuthCredential {
    /// Returns the credential kind without exposing any credential fields.
    #[must_use]
    pub const fn credential_type(&self) -> AuthCredentialType {
        match self {
            Self::ApiKey { .. } => AuthCredentialType::ApiKey,
            Self::Oauth { .. } => AuthCredentialType::Oauth,
        }
    }
}

/// A persisted auth profile: identity, provider, scope, and credential descriptor.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthProfileRecord {
    pub profile_id: String,
    pub provider: AuthProvider,
    pub profile_name: String,
    pub scope: AuthProfileScope,
    pub credential: AuthCredential,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

/// Request payload for creating or replacing a profile; timestamps are assigned by
/// the registry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthProfileSetRequest {
    pub profile_id: String,
    pub provider: AuthProvider,
    pub profile_name: String,
    pub scope: AuthProfileScope,
    pub credential: AuthCredential,
}

/// Scope predicate for listing profiles.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthScopeFilter {
    Global,
    Agent { agent_id: String },
}

/// Filter and cursor pagination options for listing profiles.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthProfileListFilter {
    /// Resume after this profile id; must exist in the filtered result set.
    pub after_profile_id: Option<String>,
    pub limit: Option<usize>,
    pub provider: Option<AuthProvider>,
    pub scope: Option<AuthScopeFilter>,
}

/// One page of profiles plus the cursor for the next page, if any.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthProfilesPage {
    pub profiles: Vec<AuthProfileRecord>,
    /// Pass back as `after_profile_id` to fetch the next page; `None` on the last page.
    pub next_after_profile_id: Option<String>,
}

/// Health classification of a profile's credential.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuthProfileHealthState {
    Ok,
    Expiring,
    Expired,
    /// A referenced vault secret is absent or unreadable.
    Missing,
    /// Static api-key credential; expiry tracking does not apply.
    Static,
}

/// Per-profile health evaluation result; safe to serialize (no secret values).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthProfileHealthRecord {
    pub profile_id: String,
    pub provider: String,
    pub profile_name: String,
    pub scope: String,
    pub credential_type: AuthCredentialType,
    pub state: AuthProfileHealthState,
    pub reason: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at_unix_ms: Option<i64>,
}

/// Counts of profiles per health state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct AuthHealthSummary {
    pub total: u64,
    pub ok: u64,
    pub expiring: u64,
    pub expired: u64,
    pub missing: u64,
    pub static_count: u64,
}

/// Histogram of profiles bucketed by remaining token lifetime.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct AuthExpiryDistribution {
    pub expired: u64,
    pub under_5m: u64,
    pub between_5m_15m: u64,
    pub between_15m_60m: u64,
    pub between_1h_24h: u64,
    pub over_24h: u64,
    /// Tokens whose provider did not report an expiry.
    pub unknown: u64,
    pub static_count: u64,
    pub missing: u64,
}

/// Aggregate health report: summary counts, expiry histogram, and per-profile records.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthHealthReport {
    pub summary: AuthHealthSummary,
    pub expiry_distribution: AuthExpiryDistribution,
    pub profiles: Vec<AuthProfileHealthRecord>,
}

/// Category of an observed profile failure; drives cooldown and eligibility policy.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuthProfileFailureKind {
    AuthInvalid,
    RefreshDue,
    RefreshFailed,
    Quota,
    RateLimit,
    Transient,
    ConfigMissing,
}

impl AuthProfileFailureKind {
    /// Returns the stable snake_case identifier used in doctor-hint codes.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AuthInvalid => "auth_invalid",
            Self::RefreshDue => "refresh_due",
            Self::RefreshFailed => "refresh_failed",
            Self::Quota => "quota",
            Self::RateLimit => "rate_limit",
            Self::Transient => "transient",
            Self::ConfigMissing => "config_missing",
        }
    }
}

/// Severity of an operator-facing doctor hint.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuthProfileDoctorSeverity {
    Info,
    Warning,
    Error,
}

/// Actionable operator hint attached to a runtime record (e.g. "rotate the credential").
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthProfileDoctorHint {
    /// Stable machine-readable code (e.g. `token_expired`, `credential_missing`).
    pub code: String,
    pub severity: AuthProfileDoctorSeverity,
    pub message: String,
}

/// Expiry classification of a profile's token as tracked in runtime state.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuthTokenExpiryState {
    /// Static api-key credential; expiry tracking does not apply.
    Static,
    /// A referenced vault secret is absent or unreadable.
    Missing,
    Valid,
    Expiring,
    Expired,
    /// No health evaluation has been recorded yet.
    Unknown,
}

/// Whether a profile may currently be selected for use, and if not, why.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuthProfileEligibility {
    Eligible,
    /// Temporarily excluded until its cooldown deadline passes.
    CoolingDown,
    Expired,
    Revoked,
    MissingCredential,
    Unsupported,
    PolicyDenied,
}

/// Persisted runtime bookkeeping for one profile: usage, failures, cooldown, expiry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthProfileRuntimeRecord {
    pub profile_id: String,
    pub provider: String,
    pub scope: String,
    pub credential_type: AuthCredentialType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_used_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_success_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_failure_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_failure_kind: Option<AuthProfileFailureKind>,
    pub failure_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cooldown_until_unix_ms: Option<i64>,
    pub token_expiry_state: AuthTokenExpiryState,
    pub eligibility: AuthProfileEligibility,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doctor_hint: Option<AuthProfileDoctorHint>,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

/// Persisted operator-defined profile ordering for one scope/provider combination.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthProfileOrderRecord {
    pub scope: String,
    /// Canonical provider key; `None` means the order applies across providers.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    pub profile_ids: Vec<String>,
    pub updated_at_unix_ms: i64,
}

/// Inputs constraining a profile selection.
///
/// Empty collections mean "no restriction": an empty `explicit_profile_order` falls back
/// to the persisted order record, and empty `allowed_credential_types` permits all kinds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthProfileSelectionRequest {
    pub provider: Option<AuthProvider>,
    pub agent_id: Option<String>,
    pub explicit_profile_order: Vec<String>,
    pub allowed_credential_types: Vec<AuthCredentialType>,
    pub policy_denied_profile_ids: Vec<String>,
}

/// One evaluated candidate in a selection result, with the reason it was or was not chosen.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthProfileSelectionCandidate {
    pub profile_id: String,
    pub provider: String,
    pub scope: String,
    pub credential_type: AuthCredentialType,
    pub token_expiry_state: AuthTokenExpiryState,
    pub eligibility: AuthProfileEligibility,
    pub failure_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cooldown_until_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_used_unix_ms: Option<i64>,
    pub selected: bool,
    /// Stable code such as `eligible`, `cooldown_active`, or `policy_denied`.
    pub reason_code: String,
}

/// Outcome of a profile selection: the winner (if any) plus the full ranked explanation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthProfileSelectionResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selected_profile_id: Option<String>,
    /// `selected`, `no_candidates`, or `no_eligible_candidates`.
    pub reason_code: String,
    pub candidates: Vec<AuthProfileSelectionCandidate>,
    pub generated_at_unix_ms: i64,
}

/// In-memory request handed to an [`OAuthRefreshAdapter`](crate::OAuthRefreshAdapter).
///
/// `Debug` redacts raw token fields so adapter diagnostics cannot leak secrets.
#[derive(Clone, PartialEq, Eq)]
pub struct OAuthRefreshRequest {
    pub provider: AuthProvider,
    pub token_endpoint: String,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    pub refresh_token: String,
    pub scopes: Vec<String>,
}

impl fmt::Debug for OAuthRefreshRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OAuthRefreshRequest")
            .field("provider", &self.provider)
            .field("token_endpoint", &self.token_endpoint)
            .field("client_id", &self.client_id)
            .field("client_secret", &self.client_secret.as_ref().map(|_| "<redacted>"))
            .field("refresh_token", &"<redacted>")
            .field("scopes", &self.scopes)
            .finish()
    }
}

/// In-memory token response returned by an adapter; same `Debug` caveat as
/// [`OAuthRefreshRequest`].
#[derive(Clone, PartialEq, Eq)]
pub struct OAuthRefreshResponse {
    pub access_token: String,
    /// Rotated refresh token, when the provider issued a new one.
    pub refresh_token: Option<String>,
    pub expires_in_seconds: Option<u64>,
}

impl fmt::Debug for OAuthRefreshResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OAuthRefreshResponse")
            .field("access_token", &"<redacted>")
            .field("refresh_token", &self.refresh_token.as_ref().map(|_| "<redacted>"))
            .field("expires_in_seconds", &self.expires_in_seconds)
            .finish()
    }
}

/// Failure modes of an OAuth token refresh attempt.
///
/// `Transport` and `InvalidResponse` payloads may quote response-derived text; the
/// registry persists only a sanitized category string, never these payloads.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum OAuthRefreshError {
    #[error("oauth refresh transport failure: {0}")]
    Transport(String),
    #[error("oauth refresh endpoint returned non-success status {status}")]
    HttpStatus { status: u16 },
    #[error("oauth refresh response is invalid: {0}")]
    InvalidResponse(String),
}
