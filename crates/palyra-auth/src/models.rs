use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum AuthProviderKind {
    Openai,
    Anthropic,
    Telegram,
    Slack,
    Discord,
    Webhook,
    Custom,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct AuthProvider {
    pub kind: AuthProviderKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom_name: Option<String>,
}

impl AuthProvider {
    #[must_use]
    pub const fn known(kind: AuthProviderKind) -> Self {
        Self { kind, custom_name: None }
    }

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

    #[must_use]
    pub fn canonical_key(&self) -> String {
        if self.kind == AuthProviderKind::Custom {
            return format!("custom:{}", self.label());
        }
        self.label()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AuthProfileScope {
    Global,
    Agent { agent_id: String },
}

impl AuthProfileScope {
    #[must_use]
    pub fn scope_key(&self) -> String {
        match self {
            Self::Global => "global".to_owned(),
            Self::Agent { agent_id } => format!("agent:{agent_id}"),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum AuthCredentialType {
    ApiKey,
    Oauth,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct OAuthRefreshState {
    pub failure_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_attempt_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_success_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_allowed_refresh_unix_ms: Option<i64>,
}

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
        #[serde(skip_serializing_if = "Option::is_none")]
        expires_at_unix_ms: Option<i64>,
        #[serde(default)]
        refresh_state: OAuthRefreshState,
    },
}

impl AuthCredential {
    #[must_use]
    pub const fn credential_type(&self) -> AuthCredentialType {
        match self {
            Self::ApiKey { .. } => AuthCredentialType::ApiKey,
            Self::Oauth { .. } => AuthCredentialType::Oauth,
        }
    }
}

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthProfileSetRequest {
    pub profile_id: String,
    pub provider: AuthProvider,
    pub profile_name: String,
    pub scope: AuthProfileScope,
    pub credential: AuthCredential,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthScopeFilter {
    Global,
    Agent { agent_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthProfileListFilter {
    pub after_profile_id: Option<String>,
    pub limit: Option<usize>,
    pub provider: Option<AuthProvider>,
    pub scope: Option<AuthScopeFilter>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthProfilesPage {
    pub profiles: Vec<AuthProfileRecord>,
    pub next_after_profile_id: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuthProfileHealthState {
    Ok,
    Expiring,
    Expired,
    Missing,
    Static,
}

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct AuthHealthSummary {
    pub total: u64,
    pub ok: u64,
    pub expiring: u64,
    pub expired: u64,
    pub missing: u64,
    pub static_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct AuthExpiryDistribution {
    pub expired: u64,
    pub under_5m: u64,
    pub between_5m_15m: u64,
    pub between_15m_60m: u64,
    pub between_1h_24h: u64,
    pub over_24h: u64,
    pub unknown: u64,
    pub static_count: u64,
    pub missing: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthHealthReport {
    pub summary: AuthHealthSummary,
    pub expiry_distribution: AuthExpiryDistribution,
    pub profiles: Vec<AuthProfileHealthRecord>,
}

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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuthProfileDoctorSeverity {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthProfileDoctorHint {
    pub code: String,
    pub severity: AuthProfileDoctorSeverity,
    pub message: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuthTokenExpiryState {
    Static,
    Missing,
    Valid,
    Expiring,
    Expired,
    Unknown,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuthProfileEligibility {
    Eligible,
    CoolingDown,
    Expired,
    Revoked,
    MissingCredential,
    Unsupported,
    PolicyDenied,
}

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthProfileOrderRecord {
    pub scope: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    pub profile_ids: Vec<String>,
    pub updated_at_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthProfileSelectionRequest {
    pub provider: Option<AuthProvider>,
    pub agent_id: Option<String>,
    pub explicit_profile_order: Vec<String>,
    pub allowed_credential_types: Vec<AuthCredentialType>,
    pub policy_denied_profile_ids: Vec<String>,
}

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
    pub reason_code: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthProfileSelectionResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selected_profile_id: Option<String>,
    pub reason_code: String,
    pub candidates: Vec<AuthProfileSelectionCandidate>,
    pub generated_at_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OAuthRefreshRequest {
    pub provider: AuthProvider,
    pub token_endpoint: String,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    pub refresh_token: String,
    pub scopes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OAuthRefreshResponse {
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub expires_in_seconds: Option<u64>,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum OAuthRefreshError {
    #[error("oauth refresh transport failure: {0}")]
    Transport(String),
    #[error("oauth refresh endpoint returned non-success status {status}")]
    HttpStatus { status: u16 },
    #[error("oauth refresh response is invalid: {0}")]
    InvalidResponse(String),
}
