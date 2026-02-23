use std::{
    collections::BTreeMap,
    env, fs,
    net::IpAddr,
    path::{Component, Path, PathBuf},
    sync::Mutex,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use palyra_vault::{Vault, VaultError, VaultRef};
use reqwest::{blocking::Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

const REGISTRY_VERSION: u32 = 1;
const REGISTRY_FILE: &str = "auth_profiles.toml";
const ENV_STATE_ROOT: &str = "PALYRA_STATE_ROOT";
const ENV_REGISTRY_PATH: &str = "PALYRA_AUTH_PROFILES_PATH";
const MAX_PROFILE_COUNT: usize = 2_048;
const MAX_PROFILE_PAGE_LIMIT: usize = 500;
const DEFAULT_EXPIRING_WINDOW_MS: i64 = 15 * 60 * 1_000;
const DEFAULT_REFRESH_WINDOW_MS: i64 = 5 * 60 * 1_000;
const DEFAULT_REFRESH_TIMEOUT_SECS: u64 = 10;

#[derive(Debug, Error)]
pub enum AuthProfileError {
    #[error("auth profile registry lock poisoned")]
    LockPoisoned,
    #[error("invalid path in {field}: {message}")]
    InvalidPath { field: &'static str, message: String },
    #[error("failed to read auth profile registry {path}: {source}")]
    ReadRegistry {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse auth profile registry {path}: {source}")]
    ParseRegistry {
        path: PathBuf,
        #[source]
        source: Box<toml::de::Error>,
    },
    #[error("failed to write auth profile registry {path}: {source}")]
    WriteRegistry {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to serialize auth profile registry: {0}")]
    SerializeRegistry(#[from] toml::ser::Error),
    #[error("unsupported auth profile registry version {0}")]
    UnsupportedVersion(u32),
    #[error("invalid field '{field}': {message}")]
    InvalidField { field: &'static str, message: String },
    #[error("auth profile not found: {0}")]
    ProfileNotFound(String),
    #[error("auth profile registry exceeds maximum entries")]
    RegistryLimitExceeded,
    #[error("system time before unix epoch: {0}")]
    InvalidSystemTime(#[from] std::time::SystemTimeError),
}

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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
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

pub trait OAuthRefreshAdapter: Send + Sync {
    fn refresh_access_token(
        &self,
        request: &OAuthRefreshRequest,
    ) -> Result<OAuthRefreshResponse, OAuthRefreshError>;
}

#[derive(Debug)]
pub struct HttpOAuthRefreshAdapter {
    timeout: Duration,
}

impl HttpOAuthRefreshAdapter {
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
            .expect("default oauth refresh adapter should initialize")
    }
}

impl OAuthRefreshAdapter for HttpOAuthRefreshAdapter {
    fn refresh_access_token(
        &self,
        request: &OAuthRefreshRequest,
    ) -> Result<OAuthRefreshResponse, OAuthRefreshError> {
        let client = Client::builder()
            .timeout(self.timeout)
            .redirect(reqwest::redirect::Policy::none())
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
            .post(request.token_endpoint.as_str())
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
        let expires_in_seconds = match parsed.get("expires_in") {
            Some(Value::Number(value)) => value.as_u64(),
            Some(Value::String(value)) => value.parse::<u64>().ok(),
            Some(_) | None => None,
        };

        Ok(OAuthRefreshResponse { access_token, refresh_token, expires_in_seconds })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OAuthRefreshOutcomeKind {
    SkippedNotOauth,
    SkippedNotDue,
    SkippedCooldown,
    Succeeded,
    Failed,
}

impl OAuthRefreshOutcomeKind {
    #[must_use]
    pub const fn attempted(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed)
    }

    #[must_use]
    pub const fn success(self) -> bool {
        matches!(self, Self::Succeeded)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OAuthRefreshOutcome {
    pub profile_id: String,
    pub provider: String,
    pub kind: OAuthRefreshOutcomeKind,
    pub reason: String,
    pub next_allowed_refresh_unix_ms: Option<i64>,
    pub expires_at_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderBackoffPolicy {
    pub base_backoff_ms: u64,
    pub max_backoff_ms: u64,
}

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

pub fn compute_backoff_ms(provider: &AuthProvider, failure_count: u32) -> u64 {
    let policy = provider_backoff_policy(provider);
    if failure_count == 0 {
        return policy.base_backoff_ms;
    }
    let shift = failure_count.saturating_sub(1).min(20);
    let factor = 1_u64 << shift;
    policy.base_backoff_ms.saturating_mul(factor).min(policy.max_backoff_ms)
}

#[derive(Debug)]
pub struct AuthProfileRegistry {
    registry_path: PathBuf,
    state: Mutex<RegistryDocument>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RegistryDocument {
    version: u32,
    #[serde(default)]
    profiles: Vec<AuthProfileRecord>,
}

impl Default for RegistryDocument {
    fn default() -> Self {
        Self { version: REGISTRY_VERSION, profiles: Vec::new() }
    }
}
impl AuthProfileRegistry {
    pub fn open(identity_store_root: &Path) -> Result<Self, AuthProfileError> {
        let state_root = resolve_state_root(identity_store_root)?;
        let registry_path = resolve_registry_path(state_root.as_path())?;
        if let Some(parent) = registry_path.parent() {
            fs::create_dir_all(parent).map_err(|source| AuthProfileError::WriteRegistry {
                path: parent.to_path_buf(),
                source,
            })?;
        }

        let mut document = if registry_path.exists() {
            let raw = fs::read_to_string(&registry_path).map_err(|source| {
                AuthProfileError::ReadRegistry { path: registry_path.clone(), source }
            })?;
            toml::from_str::<RegistryDocument>(&raw).map_err(|source| {
                AuthProfileError::ParseRegistry {
                    path: registry_path.clone(),
                    source: Box::new(source),
                }
            })?
        } else {
            RegistryDocument::default()
        };
        normalize_document(&mut document)?;
        persist_registry(registry_path.as_path(), &document)?;

        Ok(Self { registry_path, state: Mutex::new(document) })
    }

    pub fn list_profiles(
        &self,
        filter: AuthProfileListFilter,
    ) -> Result<AuthProfilesPage, AuthProfileError> {
        let guard = self.state.lock().map_err(|_| AuthProfileError::LockPoisoned)?;
        let limit = filter.limit.unwrap_or(100).clamp(1, MAX_PROFILE_PAGE_LIMIT);
        let mut filtered = guard
            .profiles
            .iter()
            .filter(|profile| profile_matches_filter(profile, &filter))
            .cloned()
            .collect::<Vec<_>>();
        let start = if let Some(after) = filter.after_profile_id.as_deref() {
            filtered
                .iter()
                .position(|profile| profile.profile_id == after)
                .map_or(0, |index| index.saturating_add(1))
        } else {
            0
        };
        let mut page = filtered.drain(start..).take(limit.saturating_add(1)).collect::<Vec<_>>();
        let has_more = page.len() > limit;
        if has_more {
            page.truncate(limit);
        }
        Ok(AuthProfilesPage {
            next_after_profile_id: if has_more {
                page.last().map(|profile| profile.profile_id.clone())
            } else {
                None
            },
            profiles: page,
        })
    }

    pub fn get_profile(
        &self,
        profile_id: &str,
    ) -> Result<Option<AuthProfileRecord>, AuthProfileError> {
        let profile_id = normalize_profile_id(profile_id)?;
        let guard = self.state.lock().map_err(|_| AuthProfileError::LockPoisoned)?;
        Ok(guard.profiles.iter().find(|profile| profile.profile_id == profile_id).cloned())
    }

    pub fn set_profile(
        &self,
        request: AuthProfileSetRequest,
    ) -> Result<AuthProfileRecord, AuthProfileError> {
        let normalized = normalize_set_request(request)?;
        let now = unix_ms_now()?;

        let mut guard = self.state.lock().map_err(|_| AuthProfileError::LockPoisoned)?;
        let mut record = AuthProfileRecord {
            profile_id: normalized.profile_id,
            provider: normalized.provider,
            profile_name: normalized.profile_name,
            scope: normalized.scope,
            credential: normalized.credential,
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
        };
        if let Some(existing) =
            guard.profiles.iter_mut().find(|profile| profile.profile_id == record.profile_id)
        {
            record.created_at_unix_ms = existing.created_at_unix_ms;
            *existing = record.clone();
        } else {
            if guard.profiles.len() >= MAX_PROFILE_COUNT {
                return Err(AuthProfileError::RegistryLimitExceeded);
            }
            guard.profiles.push(record.clone());
        }
        guard.profiles.sort_by(|left, right| left.profile_id.cmp(&right.profile_id));
        persist_registry(self.registry_path.as_path(), &guard)?;
        Ok(record)
    }

    pub fn delete_profile(&self, profile_id: &str) -> Result<bool, AuthProfileError> {
        let profile_id = normalize_profile_id(profile_id)?;
        let mut guard = self.state.lock().map_err(|_| AuthProfileError::LockPoisoned)?;
        let before = guard.profiles.len();
        guard.profiles.retain(|profile| profile.profile_id != profile_id);
        let deleted = guard.profiles.len() != before;
        if deleted {
            persist_registry(self.registry_path.as_path(), &guard)?;
        }
        Ok(deleted)
    }

    pub fn merged_profiles_for_agent(
        &self,
        agent_id: Option<&str>,
    ) -> Result<Vec<AuthProfileRecord>, AuthProfileError> {
        let guard = self.state.lock().map_err(|_| AuthProfileError::LockPoisoned)?;
        if let Some(agent_id_raw) = agent_id {
            let normalized_agent_id = normalize_agent_id(agent_id_raw)?;
            let mut merged = BTreeMap::<String, AuthProfileRecord>::new();
            for profile in &guard.profiles {
                if matches!(profile.scope, AuthProfileScope::Global) {
                    merged.insert(profile_merge_key(profile), profile.clone());
                }
            }
            for profile in &guard.profiles {
                if matches!(
                    profile.scope,
                    AuthProfileScope::Agent { ref agent_id } if agent_id == &normalized_agent_id
                ) {
                    merged.insert(profile_merge_key(profile), profile.clone());
                }
            }
            return Ok(merged.into_values().collect());
        }
        Ok(guard.profiles.clone())
    }

    pub fn health_report(
        &self,
        vault: &Vault,
        agent_id: Option<&str>,
    ) -> Result<AuthHealthReport, AuthProfileError> {
        self.health_report_with_clock(vault, agent_id, unix_ms_now()?, DEFAULT_EXPIRING_WINDOW_MS)
    }

    pub fn health_report_with_clock(
        &self,
        vault: &Vault,
        agent_id: Option<&str>,
        now_unix_ms: i64,
        expiring_window_ms: i64,
    ) -> Result<AuthHealthReport, AuthProfileError> {
        let profiles = self.merged_profiles_for_agent(agent_id)?;
        let mut report = AuthHealthReport {
            summary: AuthHealthSummary::default(),
            expiry_distribution: AuthExpiryDistribution::default(),
            profiles: Vec::with_capacity(profiles.len()),
        };

        for profile in profiles {
            let health = evaluate_profile_health(&profile, vault, now_unix_ms, expiring_window_ms);
            report.summary.total = report.summary.total.saturating_add(1);
            match health.state {
                AuthProfileHealthState::Ok => {
                    report.summary.ok = report.summary.ok.saturating_add(1)
                }
                AuthProfileHealthState::Expiring => {
                    report.summary.expiring = report.summary.expiring.saturating_add(1)
                }
                AuthProfileHealthState::Expired => {
                    report.summary.expired = report.summary.expired.saturating_add(1)
                }
                AuthProfileHealthState::Missing => {
                    report.summary.missing = report.summary.missing.saturating_add(1)
                }
                AuthProfileHealthState::Static => {
                    report.summary.static_count = report.summary.static_count.saturating_add(1)
                }
            }
            update_expiry_distribution(
                &mut report.expiry_distribution,
                health.state,
                health.expires_at_unix_ms,
                now_unix_ms,
            );
            report.profiles.push(health);
        }

        Ok(report)
    }

    pub fn refresh_due_oauth_profiles(
        &self,
        vault: &Vault,
        adapter: &dyn OAuthRefreshAdapter,
        agent_id: Option<&str>,
    ) -> Result<Vec<OAuthRefreshOutcome>, AuthProfileError> {
        self.refresh_due_oauth_profiles_with_clock(
            vault,
            adapter,
            agent_id,
            unix_ms_now()?,
            DEFAULT_REFRESH_WINDOW_MS,
        )
    }

    pub fn refresh_due_oauth_profiles_with_clock(
        &self,
        vault: &Vault,
        adapter: &dyn OAuthRefreshAdapter,
        agent_id: Option<&str>,
        now_unix_ms: i64,
        refresh_window_ms: i64,
    ) -> Result<Vec<OAuthRefreshOutcome>, AuthProfileError> {
        let profiles = self.merged_profiles_for_agent(agent_id)?;
        let mut outcomes = Vec::new();
        for profile in profiles {
            let profile_id = profile.profile_id.clone();
            if let AuthCredential::Oauth { .. } = profile.credential {
                if !should_attempt_oauth_refresh(&profile, vault, now_unix_ms, refresh_window_ms) {
                    outcomes.push(OAuthRefreshOutcome {
                        profile_id: profile_id.clone(),
                        provider: profile.provider.label(),
                        kind: OAuthRefreshOutcomeKind::SkippedNotDue,
                        reason: "refresh skipped because token is not yet due".to_owned(),
                        next_allowed_refresh_unix_ms: None,
                        expires_at_unix_ms: oauth_expires_at(&profile),
                    });
                    continue;
                }
                outcomes.push(self.refresh_oauth_profile_with_clock(
                    profile_id.as_str(),
                    vault,
                    adapter,
                    now_unix_ms,
                )?);
            }
        }
        Ok(outcomes)
    }

    pub fn refresh_oauth_profile(
        &self,
        profile_id: &str,
        vault: &Vault,
        adapter: &dyn OAuthRefreshAdapter,
    ) -> Result<OAuthRefreshOutcome, AuthProfileError> {
        self.refresh_oauth_profile_with_clock(profile_id, vault, adapter, unix_ms_now()?)
    }

    pub fn refresh_oauth_profile_with_clock(
        &self,
        profile_id: &str,
        vault: &Vault,
        adapter: &dyn OAuthRefreshAdapter,
        now_unix_ms: i64,
    ) -> Result<OAuthRefreshOutcome, AuthProfileError> {
        let profile_id = normalize_profile_id(profile_id)?;

        let snapshot = {
            let guard = self.state.lock().map_err(|_| AuthProfileError::LockPoisoned)?;
            let profile = guard
                .profiles
                .iter()
                .find(|profile| profile.profile_id == profile_id)
                .ok_or_else(|| AuthProfileError::ProfileNotFound(profile_id.clone()))?;
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
                return Ok(OAuthRefreshOutcome {
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
                    return Ok(OAuthRefreshOutcome {
                        profile_id: profile.profile_id.clone(),
                        provider: profile.provider.label(),
                        kind: OAuthRefreshOutcomeKind::SkippedCooldown,
                        reason: "refresh skipped due to cooldown after previous failures"
                            .to_owned(),
                        next_allowed_refresh_unix_ms: Some(next_allowed),
                        expires_at_unix_ms: *expires_at_unix_ms,
                    });
                }
            }

            OAuthRefreshSnapshot {
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
            }
        };

        let refresh_token = match load_secret_utf8(vault, snapshot.refresh_token_vault_ref.as_str())
        {
            Ok(token) => token,
            Err(_) => {
                return self.persist_refresh_failure(
                    snapshot,
                    now_unix_ms,
                    "refresh token reference is missing or unreadable".to_owned(),
                );
            }
        };

        let client_secret =
            if let Some(client_secret_ref) = snapshot.client_secret_vault_ref.as_deref() {
                match load_secret_utf8(vault, client_secret_ref) {
                    Ok(secret) => Some(secret),
                    Err(_) => {
                        return self.persist_refresh_failure(
                            snapshot,
                            now_unix_ms,
                            "client secret reference is missing or unreadable".to_owned(),
                        );
                    }
                }
            } else {
                None
            };

        let response = adapter.refresh_access_token(&OAuthRefreshRequest {
            provider: snapshot.provider.clone(),
            token_endpoint: snapshot.token_endpoint.clone(),
            client_id: snapshot.client_id.clone(),
            client_secret,
            refresh_token,
            scopes: snapshot.scopes.clone(),
        });
        match response {
            Ok(payload) => {
                if let Some(refresh_token) = payload.refresh_token.as_deref() {
                    if persist_secret_utf8(
                        vault,
                        snapshot.refresh_token_vault_ref.as_str(),
                        refresh_token,
                    )
                    .is_err()
                    {
                        return self.persist_refresh_failure(
                            snapshot,
                            now_unix_ms,
                            "failed to persist rotated refresh token into vault".to_owned(),
                        );
                    }
                }
                if persist_secret_utf8(
                    vault,
                    snapshot.access_token_vault_ref.as_str(),
                    payload.access_token.as_str(),
                )
                .is_err()
                {
                    return self.persist_refresh_failure(
                        snapshot,
                        now_unix_ms,
                        "failed to persist refreshed access token into vault".to_owned(),
                    );
                }
                let computed_expires_at = payload
                    .expires_in_seconds
                    .map(|seconds| {
                        now_unix_ms.saturating_add((seconds as i64).saturating_mul(1_000))
                    })
                    .or(snapshot.expires_at_unix_ms);
                let mut guard = self.state.lock().map_err(|_| AuthProfileError::LockPoisoned)?;
                let (profile_id, provider, expires_at_unix_ms) = {
                    let profile = guard
                        .profiles
                        .iter_mut()
                        .find(|profile| profile.profile_id == snapshot.profile_id)
                        .ok_or_else(|| {
                            AuthProfileError::ProfileNotFound(snapshot.profile_id.clone())
                        })?;
                    let provider = profile.provider.label();
                    let profile_id = profile.profile_id.clone();
                    let AuthCredential::Oauth { expires_at_unix_ms, refresh_state, .. } =
                        &mut profile.credential
                    else {
                        return Ok(OAuthRefreshOutcome {
                            profile_id,
                            provider,
                            kind: OAuthRefreshOutcomeKind::SkippedNotOauth,
                            reason: "profile credential type changed before refresh completed"
                                .to_owned(),
                            next_allowed_refresh_unix_ms: None,
                            expires_at_unix_ms: None,
                        });
                    };
                    *expires_at_unix_ms = computed_expires_at;
                    refresh_state.failure_count = 0;
                    refresh_state.last_error = None;
                    refresh_state.last_attempt_unix_ms = Some(now_unix_ms);
                    refresh_state.last_success_unix_ms = Some(now_unix_ms);
                    refresh_state.next_allowed_refresh_unix_ms = None;
                    profile.updated_at_unix_ms =
                        next_profile_updated_at(profile.updated_at_unix_ms, now_unix_ms);
                    (profile_id, provider, *expires_at_unix_ms)
                };
                persist_registry(self.registry_path.as_path(), &guard)?;
                Ok(OAuthRefreshOutcome {
                    profile_id,
                    provider,
                    kind: OAuthRefreshOutcomeKind::Succeeded,
                    reason: "oauth access token refreshed".to_owned(),
                    next_allowed_refresh_unix_ms: None,
                    expires_at_unix_ms,
                })
            }
            Err(error) => {
                self.persist_refresh_failure(snapshot, now_unix_ms, sanitize_refresh_error(&error))
            }
        }
    }

    fn persist_refresh_failure(
        &self,
        snapshot: OAuthRefreshSnapshot,
        now_unix_ms: i64,
        reason: String,
    ) -> Result<OAuthRefreshOutcome, AuthProfileError> {
        let mut guard = self.state.lock().map_err(|_| AuthProfileError::LockPoisoned)?;
        let profile = guard
            .profiles
            .iter_mut()
            .find(|profile| profile.profile_id == snapshot.profile_id)
            .ok_or_else(|| AuthProfileError::ProfileNotFound(snapshot.profile_id.clone()))?;
        let provider = profile.provider.label();
        let profile_id = profile.profile_id.clone();
        let (next_allowed, expires_at_unix_ms) = {
            let AuthCredential::Oauth { refresh_state, expires_at_unix_ms, .. } =
                &mut profile.credential
            else {
                return Ok(OAuthRefreshOutcome {
                    profile_id,
                    provider,
                    kind: OAuthRefreshOutcomeKind::SkippedNotOauth,
                    reason: "profile credential type changed before refresh failure persisted"
                        .to_owned(),
                    next_allowed_refresh_unix_ms: None,
                    expires_at_unix_ms: None,
                });
            };
            if profile.updated_at_unix_ms != snapshot.observed_updated_at_unix_ms {
                return Ok(OAuthRefreshOutcome {
                    profile_id,
                    provider,
                    kind: OAuthRefreshOutcomeKind::SkippedCooldown,
                    reason: "stale refresh failure ignored because profile state changed"
                        .to_owned(),
                    next_allowed_refresh_unix_ms: refresh_state.next_allowed_refresh_unix_ms,
                    expires_at_unix_ms: *expires_at_unix_ms,
                });
            }
            let failure_count = snapshot.failure_count.saturating_add(1);
            let backoff_ms = compute_backoff_ms(&snapshot.provider, failure_count);
            let next_allowed = now_unix_ms.saturating_add(backoff_ms as i64);

            refresh_state.failure_count = failure_count;
            refresh_state.last_error = Some(reason.clone());
            refresh_state.last_attempt_unix_ms = Some(now_unix_ms);
            refresh_state.next_allowed_refresh_unix_ms = Some(next_allowed);
            (next_allowed, *expires_at_unix_ms)
        };
        profile.updated_at_unix_ms =
            next_profile_updated_at(profile.updated_at_unix_ms, now_unix_ms);

        persist_registry(self.registry_path.as_path(), &guard)?;

        Ok(OAuthRefreshOutcome {
            profile_id,
            provider,
            kind: OAuthRefreshOutcomeKind::Failed,
            reason,
            next_allowed_refresh_unix_ms: Some(next_allowed),
            expires_at_unix_ms,
        })
    }
}

#[derive(Debug, Clone)]
struct OAuthRefreshSnapshot {
    profile_id: String,
    provider: AuthProvider,
    access_token_vault_ref: String,
    refresh_token_vault_ref: String,
    token_endpoint: String,
    client_id: Option<String>,
    client_secret_vault_ref: Option<String>,
    scopes: Vec<String>,
    expires_at_unix_ms: Option<i64>,
    failure_count: u32,
    observed_updated_at_unix_ms: i64,
}

fn sanitize_refresh_error(error: &OAuthRefreshError) -> String {
    match error {
        OAuthRefreshError::Transport(_) => "oauth refresh transport failure".to_owned(),
        OAuthRefreshError::HttpStatus { status } => {
            format!("oauth refresh endpoint returned status {status}")
        }
        OAuthRefreshError::InvalidResponse(_) => "oauth refresh response was invalid".to_owned(),
    }
}

fn persist_secret_utf8(vault: &Vault, vault_ref: &str, value: &str) -> Result<(), VaultError> {
    let parsed = VaultRef::parse(vault_ref)?;
    vault.put_secret(&parsed.scope, parsed.key.as_str(), value.as_bytes())?;
    Ok(())
}

fn load_secret_utf8(vault: &Vault, vault_ref: &str) -> Result<String, VaultError> {
    let parsed = VaultRef::parse(vault_ref)?;
    let raw = vault.get_secret(&parsed.scope, parsed.key.as_str())?;
    let decoded = String::from_utf8(raw)
        .map_err(|error| VaultError::Crypto(format!("secret must be valid UTF-8: {error}")))?;
    if decoded.trim().is_empty() {
        return Err(VaultError::NotFound);
    }
    Ok(decoded)
}

fn should_attempt_oauth_refresh(
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

fn oauth_expires_at(profile: &AuthProfileRecord) -> Option<i64> {
    let AuthCredential::Oauth { expires_at_unix_ms, .. } = &profile.credential else {
        return None;
    };
    *expires_at_unix_ms
}

fn evaluate_profile_health(
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

fn update_expiry_distribution(
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

fn resolve_state_root(identity_store_root: &Path) -> Result<PathBuf, AuthProfileError> {
    if let Ok(raw) = env::var(ENV_STATE_ROOT) {
        let state_root = normalize_configured_path(raw.as_str(), ENV_STATE_ROOT)?;
        return Ok(if state_root.is_absolute() {
            state_root
        } else {
            identity_store_root.join(state_root)
        });
    }
    Ok(identity_store_root
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| identity_store_root.to_path_buf()))
}

fn resolve_registry_path(state_root: &Path) -> Result<PathBuf, AuthProfileError> {
    if let Ok(raw) = env::var(ENV_REGISTRY_PATH) {
        let configured = normalize_configured_path(raw.as_str(), ENV_REGISTRY_PATH)?;
        return Ok(if configured.is_absolute() { configured } else { state_root.join(configured) });
    }
    Ok(state_root.join(REGISTRY_FILE))
}

fn normalize_configured_path(raw: &str, field: &'static str) -> Result<PathBuf, AuthProfileError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(AuthProfileError::InvalidPath {
            field,
            message: "path cannot be empty".to_owned(),
        });
    }
    let path = PathBuf::from(trimmed);
    for component in path.components() {
        if matches!(component, Component::ParentDir) {
            return Err(AuthProfileError::InvalidPath {
                field,
                message: "path cannot contain '..' segments".to_owned(),
            });
        }
    }
    Ok(path)
}

fn normalize_document(document: &mut RegistryDocument) -> Result<(), AuthProfileError> {
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

fn normalize_set_request(
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

fn normalize_profile_record(
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

fn normalize_profile_id(raw: &str) -> Result<String, AuthProfileError> {
    normalize_identifier(raw, "profile_id", 128)
}

fn normalize_agent_id(raw: &str) -> Result<String, AuthProfileError> {
    normalize_identifier(raw, "scope.agent_id", 128)
}

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

fn normalize_optional_text(value: Option<String>, max_len: usize) -> Option<String> {
    value.and_then(|raw| {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            None
        } else if trimmed.len() > max_len {
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

fn normalize_vault_ref(raw: &str, field: &'static str) -> Result<String, AuthProfileError> {
    let trimmed = raw.trim();
    let parsed = VaultRef::parse(trimmed).map_err(|error| AuthProfileError::InvalidField {
        field,
        message: format!("invalid vault reference: {error}"),
    })?;
    Ok(format!("{}/{}", parsed.scope, parsed.key))
}

fn normalize_token_endpoint(raw: &str) -> Result<String, AuthProfileError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(AuthProfileError::InvalidField {
            field: "oauth.token_endpoint",
            message: "value cannot be empty".to_owned(),
        });
    }
    let parsed = Url::parse(trimmed).map_err(|error| AuthProfileError::InvalidField {
        field: "oauth.token_endpoint",
        message: format!("invalid URL: {error}"),
    })?;
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(AuthProfileError::InvalidField {
            field: "oauth.token_endpoint",
            message: "URL must not include username/password components".to_owned(),
        });
    }
    match parsed.scheme() {
        "https" => {}
        "http" => {
            if !is_loopback_endpoint(&parsed) {
                return Err(AuthProfileError::InvalidField {
                    field: "oauth.token_endpoint",
                    message: "http URL is allowed only for loopback hosts".to_owned(),
                });
            }
        }
        _ => {
            return Err(AuthProfileError::InvalidField {
                field: "oauth.token_endpoint",
                message: "URL scheme must be https, or http for loopback hosts".to_owned(),
            });
        }
    }
    Ok(parsed.to_string())
}

fn is_loopback_endpoint(parsed: &Url) -> bool {
    let Some(host) = parsed.host_str() else {
        return false;
    };
    let normalized_host = host.trim_start_matches('[').trim_end_matches(']');
    if normalized_host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    normalized_host.parse::<IpAddr>().is_ok_and(|address| address.is_loopback())
}

fn next_profile_updated_at(previous_updated_at_unix_ms: i64, now_unix_ms: i64) -> i64 {
    previous_updated_at_unix_ms.saturating_add(1).max(now_unix_ms)
}

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

fn profile_matches_filter(profile: &AuthProfileRecord, filter: &AuthProfileListFilter) -> bool {
    if let Some(provider) = filter.provider.as_ref() {
        if profile.provider.canonical_key() != provider.canonical_key() {
            return false;
        }
    }
    if let Some(scope) = filter.scope.as_ref() {
        match (scope, &profile.scope) {
            (AuthScopeFilter::Global, AuthProfileScope::Global) => {}
            (
                AuthScopeFilter::Agent { agent_id: left },
                AuthProfileScope::Agent { agent_id: right },
            ) if left.eq_ignore_ascii_case(right) => {}
            _ => return false,
        }
    }
    true
}

fn profile_merge_key(profile: &AuthProfileRecord) -> String {
    format!("{}:{}", profile.provider.canonical_key(), profile.profile_name.to_ascii_lowercase())
}

fn persist_registry(path: &Path, document: &RegistryDocument) -> Result<(), AuthProfileError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| AuthProfileError::WriteRegistry {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    let serialized = toml::to_string_pretty(document)?;
    fs::write(path, serialized)
        .map_err(|source| AuthProfileError::WriteRegistry { path: path.to_path_buf(), source })?;
    Ok(())
}

fn unix_ms_now() -> Result<i64, AuthProfileError> {
    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH)?;
    Ok(elapsed.as_millis() as i64)
}

#[cfg(test)]
mod tests {
    use super::{
        compute_backoff_ms, load_secret_utf8, normalize_optional_text, normalize_token_endpoint,
        persist_secret_utf8, AuthCredential, AuthProfileError, AuthProfileRegistry,
        AuthProfileScope, AuthProfileSetRequest, AuthProvider, AuthProviderKind,
        HttpOAuthRefreshAdapter, OAuthRefreshAdapter, OAuthRefreshError, OAuthRefreshOutcomeKind,
        OAuthRefreshRequest, OAuthRefreshResponse, OAuthRefreshState,
    };
    use palyra_vault::Vault;
    use palyra_vault::{
        BackendPreference as VaultBackendPreference, VaultConfig as VaultConfigOptions,
    };
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::path::Path;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Barrier, Mutex,
    };
    use std::thread;
    use std::time::Duration;

    struct StubRefreshAdapter {
        response: Result<OAuthRefreshResponse, OAuthRefreshError>,
        call_count: Arc<Mutex<u64>>,
    }

    impl OAuthRefreshAdapter for StubRefreshAdapter {
        fn refresh_access_token(
            &self,
            _request: &OAuthRefreshRequest,
        ) -> Result<OAuthRefreshResponse, OAuthRefreshError> {
            let mut guard = self.call_count.lock().expect("test mutex should be available");
            *guard = guard.saturating_add(1);
            self.response.clone()
        }
    }

    struct RacingRefreshAdapter {
        barrier: Arc<Barrier>,
        call_count: Arc<AtomicUsize>,
    }

    impl OAuthRefreshAdapter for RacingRefreshAdapter {
        fn refresh_access_token(
            &self,
            _request: &OAuthRefreshRequest,
        ) -> Result<OAuthRefreshResponse, OAuthRefreshError> {
            let call_index = self.call_count.fetch_add(1, Ordering::SeqCst);
            self.barrier.wait();
            if call_index == 0 {
                return Ok(OAuthRefreshResponse {
                    access_token: "race-access-token".to_owned(),
                    refresh_token: None,
                    expires_in_seconds: Some(60),
                });
            }
            thread::sleep(Duration::from_millis(750));
            Err(OAuthRefreshError::Transport("simulated transport fault".to_owned()))
        }
    }

    fn open_test_vault(root: &Path, identity_root: &Path) -> Vault {
        Vault::open_with_config(VaultConfigOptions {
            root: Some(root.to_path_buf()),
            identity_store_root: Some(identity_root.to_path_buf()),
            backend_preference: VaultBackendPreference::EncryptedFile,
            ..VaultConfigOptions::default()
        })
        .expect("test vault should initialize")
    }

    fn sample_oauth_profile_request(
        token_endpoint: String,
        expires_at_unix_ms: Option<i64>,
        refresh_state: OAuthRefreshState,
    ) -> AuthProfileSetRequest {
        AuthProfileSetRequest {
            profile_id: "openai-default".to_owned(),
            provider: AuthProvider::known(AuthProviderKind::Openai),
            profile_name: "default".to_owned(),
            scope: AuthProfileScope::Global,
            credential: AuthCredential::Oauth {
                access_token_vault_ref: "global/auth_openai_access".to_owned(),
                refresh_token_vault_ref: "global/auth_openai_refresh".to_owned(),
                token_endpoint,
                client_id: Some("test-client".to_owned()),
                client_secret_vault_ref: Some("global/auth_openai_client_secret".to_owned()),
                scopes: vec!["chat:read".to_owned()],
                expires_at_unix_ms,
                refresh_state,
            },
        }
    }

    fn spawn_oauth_server(response_body: String) -> (String, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("test server should bind");
        let address = listener.local_addr().expect("test server should resolve local addr");
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("test server should accept request");
            let mut request_buffer = [0_u8; 2048];
            let _ = stream.read(&mut request_buffer);
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                response_body.len()
            );
            stream
                .write_all(headers.as_bytes())
                .expect("test server should write response headers");
            stream
                .write_all(response_body.as_bytes())
                .expect("test server should write response body");
            stream.flush().expect("test server should flush response");
        });
        (format!("http://{address}/oauth/token"), handle)
    }

    fn spawn_redirect_server(location: String) -> (String, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("test server should bind");
        let address = listener.local_addr().expect("test server should resolve local addr");
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("test server should accept request");
            let mut request_buffer = [0_u8; 2048];
            let _ = stream.read(&mut request_buffer);
            let response = format!(
                "HTTP/1.1 307 Temporary Redirect\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            );
            stream
                .write_all(response.as_bytes())
                .expect("test server should write redirect response");
            stream.flush().expect("test server should flush redirect response");
        });
        (format!("http://{address}/oauth/token"), handle)
    }

    #[test]
    fn compute_backoff_grows_exponentially_and_caps_per_provider() {
        let provider = AuthProvider::known(AuthProviderKind::Openai);
        assert_eq!(compute_backoff_ms(&provider, 1), 15_000);
        assert_eq!(compute_backoff_ms(&provider, 2), 30_000);
        assert_eq!(compute_backoff_ms(&provider, 3), 60_000);
        assert_eq!(compute_backoff_ms(&provider, 20), 300_000);
    }

    #[test]
    fn normalize_optional_text_truncates_on_utf8_boundary() {
        let normalized = normalize_optional_text(Some("A🙂B".to_owned()), 3)
            .expect("non-empty input should remain present");
        assert_eq!(normalized, "A");
    }

    #[test]
    fn normalize_token_endpoint_rejects_non_loopback_http_hosts() {
        let error = normalize_token_endpoint("http://example.test/oauth/token")
            .expect_err("non-loopback http endpoint must be rejected");
        assert!(matches!(
            error,
            AuthProfileError::InvalidField { field, message }
                if field == "oauth.token_endpoint"
                    && message.contains("loopback")
        ));
    }

    #[test]
    fn normalize_token_endpoint_allows_loopback_http_hosts() {
        let ipv4 = normalize_token_endpoint("http://127.0.0.1:8080/oauth/token")
            .expect("loopback ipv4 endpoint should be accepted");
        let host = normalize_token_endpoint("http://localhost:8080/oauth/token")
            .expect("localhost endpoint should be accepted");
        let ipv6 = normalize_token_endpoint("http://[::1]:8080/oauth/token")
            .expect("loopback ipv6 endpoint should be accepted");
        assert_eq!(ipv4, "http://127.0.0.1:8080/oauth/token");
        assert_eq!(host, "http://localhost:8080/oauth/token");
        assert_eq!(ipv6, "http://[::1]:8080/oauth/token");
    }

    #[test]
    fn normalize_token_endpoint_rejects_userinfo_components() {
        let username_error = normalize_token_endpoint("https://user@example.test/oauth/token")
            .expect_err("username in URL should be rejected");
        let password_error =
            normalize_token_endpoint("https://user:secret@example.test/oauth/token")
                .expect_err("username/password in URL should be rejected");
        assert!(matches!(
            username_error,
            AuthProfileError::InvalidField { field, message }
                if field == "oauth.token_endpoint"
                    && message.contains("username/password")
        ));
        assert!(matches!(
            password_error,
            AuthProfileError::InvalidField { field, message }
                if field == "oauth.token_endpoint"
                    && message.contains("username/password")
        ));
    }

    #[test]
    fn oauth_refresh_adapter_does_not_follow_redirects() {
        let (token_endpoint, redirect_thread) =
            spawn_redirect_server("http://127.0.0.1:0/oauth/token".to_owned());
        let adapter = HttpOAuthRefreshAdapter::with_timeout(Duration::from_secs(2))
            .expect("HTTP adapter should initialize");
        let request = OAuthRefreshRequest {
            provider: AuthProvider::known(AuthProviderKind::Openai),
            token_endpoint,
            client_id: Some("test-client".to_owned()),
            client_secret: Some("test-secret".to_owned()),
            refresh_token: "refresh-token".to_owned(),
            scopes: vec!["chat:read".to_owned()],
        };
        let result = adapter.refresh_access_token(&request);
        assert!(matches!(
            result,
            Err(OAuthRefreshError::HttpStatus { status }) if status == 307
        ));
        redirect_thread.join().expect("redirect test server thread should exit cleanly");
    }

    #[test]
    fn refresh_skips_when_cooldown_is_active() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let identity_root = tempdir.path().join("identity");
        let vault_root = tempdir.path().join("vault");
        let registry =
            AuthProfileRegistry::open(identity_root.as_path()).expect("registry should initialize");
        let vault = open_test_vault(vault_root.as_path(), identity_root.as_path());
        persist_secret_utf8(&vault, "global/auth_openai_access", "access-old")
            .expect("access secret should persist");
        persist_secret_utf8(&vault, "global/auth_openai_refresh", "refresh-static")
            .expect("refresh secret should persist");
        persist_secret_utf8(&vault, "global/auth_openai_client_secret", "client-secret")
            .expect("client secret should persist");

        let now = 1_730_000_000_000_i64;
        registry
            .set_profile(sample_oauth_profile_request(
                "https://example.test/token".to_owned(),
                Some(now.saturating_add(30_000)),
                OAuthRefreshState {
                    failure_count: 2,
                    last_error: Some("oauth refresh transport failure".to_owned()),
                    last_attempt_unix_ms: Some(now.saturating_sub(1_000)),
                    last_success_unix_ms: None,
                    next_allowed_refresh_unix_ms: Some(now.saturating_add(120_000)),
                },
            ))
            .expect("profile should persist");

        let calls = Arc::new(Mutex::new(0_u64));
        let adapter = StubRefreshAdapter {
            response: Ok(OAuthRefreshResponse {
                access_token: "new-access".to_owned(),
                refresh_token: None,
                expires_in_seconds: Some(60),
            }),
            call_count: Arc::clone(&calls),
        };
        let outcome = registry
            .refresh_oauth_profile_with_clock("openai-default", &vault, &adapter, now)
            .expect("cooldown check should succeed");
        assert_eq!(outcome.kind, OAuthRefreshOutcomeKind::SkippedCooldown);
        assert_eq!(
            *calls.lock().expect("call counter should be available"),
            0,
            "adapter must not be called when cooldown is active"
        );
    }

    #[test]
    fn oauth_refresh_integration_updates_vault_secret() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let identity_root = tempdir.path().join("identity");
        let vault_root = tempdir.path().join("vault");
        let registry =
            AuthProfileRegistry::open(identity_root.as_path()).expect("registry should initialize");
        let vault = open_test_vault(vault_root.as_path(), identity_root.as_path());
        persist_secret_utf8(&vault, "global/auth_openai_access", "access-old")
            .expect("access secret should persist");
        persist_secret_utf8(&vault, "global/auth_openai_refresh", "refresh-static")
            .expect("refresh secret should persist");
        persist_secret_utf8(&vault, "global/auth_openai_client_secret", "client-secret")
            .expect("client secret should persist");

        let now = 1_730_000_000_000_i64;
        let (token_endpoint, server_thread) = spawn_oauth_server(
            r#"{"access_token":"access-new","refresh_token":"refresh-new","expires_in":120}"#
                .to_owned(),
        );

        registry
            .set_profile(sample_oauth_profile_request(
                token_endpoint,
                Some(now.saturating_sub(1_000)),
                OAuthRefreshState::default(),
            ))
            .expect("profile should persist");
        let adapter = HttpOAuthRefreshAdapter::with_timeout(Duration::from_secs(2))
            .expect("HTTP adapter should initialize");
        let outcome = registry
            .refresh_oauth_profile_with_clock("openai-default", &vault, &adapter, now)
            .expect("refresh should succeed");
        assert_eq!(outcome.kind, OAuthRefreshOutcomeKind::Succeeded);

        let access = load_secret_utf8(&vault, "global/auth_openai_access")
            .expect("access secret should be readable");
        let refresh = load_secret_utf8(&vault, "global/auth_openai_refresh")
            .expect("refresh secret should be readable");
        assert_eq!(access, "access-new");
        assert_eq!(refresh, "refresh-new");
        server_thread.join().expect("test server thread should exit cleanly");
    }

    #[test]
    fn refresh_fails_when_client_secret_reference_is_missing() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let identity_root = tempdir.path().join("identity");
        let vault_root = tempdir.path().join("vault");
        let registry =
            AuthProfileRegistry::open(identity_root.as_path()).expect("registry should initialize");
        let vault = open_test_vault(vault_root.as_path(), identity_root.as_path());
        persist_secret_utf8(&vault, "global/auth_openai_access", "access-old")
            .expect("access secret should persist");
        persist_secret_utf8(&vault, "global/auth_openai_refresh", "refresh-static")
            .expect("refresh secret should persist");

        let now = 1_730_000_000_000_i64;
        registry
            .set_profile(sample_oauth_profile_request(
                "https://example.test/token".to_owned(),
                Some(now.saturating_sub(1_000)),
                OAuthRefreshState::default(),
            ))
            .expect("profile should persist");

        let call_count = Arc::new(Mutex::new(0_u64));
        let adapter = StubRefreshAdapter {
            response: Ok(OAuthRefreshResponse {
                access_token: "unused-access".to_owned(),
                refresh_token: None,
                expires_in_seconds: Some(120),
            }),
            call_count: Arc::clone(&call_count),
        };

        let outcome = registry
            .refresh_oauth_profile_with_clock("openai-default", &vault, &adapter, now)
            .expect("missing client secret should produce persisted failure");
        assert_eq!(outcome.kind, OAuthRefreshOutcomeKind::Failed);
        assert_eq!(
            outcome.reason, "client secret reference is missing or unreadable",
            "failure reason should explain missing secret reference"
        );
        assert_eq!(
            *call_count.lock().expect("call counter should be available"),
            0,
            "adapter must not be called when configured client secret cannot be loaded"
        );
    }

    #[test]
    fn concurrent_refresh_stale_failure_does_not_override_success_state() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let identity_root = tempdir.path().join("identity");
        let vault_root = tempdir.path().join("vault");
        let registry = Arc::new(
            AuthProfileRegistry::open(identity_root.as_path()).expect("registry should initialize"),
        );
        let vault = Arc::new(open_test_vault(vault_root.as_path(), identity_root.as_path()));
        persist_secret_utf8(vault.as_ref(), "global/auth_openai_access", "access-old")
            .expect("access secret should persist");
        persist_secret_utf8(vault.as_ref(), "global/auth_openai_refresh", "refresh-old")
            .expect("refresh secret should persist");
        persist_secret_utf8(vault.as_ref(), "global/auth_openai_client_secret", "client-secret")
            .expect("client secret should persist");

        let now = 1_730_000_000_000_i64;
        registry
            .set_profile(sample_oauth_profile_request(
                "https://example.test/token".to_owned(),
                Some(now.saturating_sub(1_000)),
                OAuthRefreshState::default(),
            ))
            .expect("profile should persist");

        let adapter = Arc::new(RacingRefreshAdapter {
            barrier: Arc::new(Barrier::new(2)),
            call_count: Arc::new(AtomicUsize::new(0)),
        });
        let registry_left = Arc::clone(&registry);
        let vault_left = Arc::clone(&vault);
        let adapter_left = Arc::clone(&adapter);
        let worker_left = thread::spawn(move || {
            registry_left.refresh_oauth_profile_with_clock(
                "openai-default",
                vault_left.as_ref(),
                adapter_left.as_ref(),
                now,
            )
        });
        let registry_right = Arc::clone(&registry);
        let vault_right = Arc::clone(&vault);
        let adapter_right = Arc::clone(&adapter);
        let worker_right = thread::spawn(move || {
            registry_right.refresh_oauth_profile_with_clock(
                "openai-default",
                vault_right.as_ref(),
                adapter_right.as_ref(),
                now,
            )
        });

        let left_outcome = worker_left
            .join()
            .expect("left worker thread should join")
            .expect("left refresh call should complete");
        let right_outcome = worker_right
            .join()
            .expect("right worker thread should join")
            .expect("right refresh call should complete");
        let kinds = [left_outcome.kind, right_outcome.kind];
        assert!(
            kinds.contains(&OAuthRefreshOutcomeKind::Succeeded),
            "one concurrent refresh should succeed"
        );
        assert!(
            kinds.contains(&OAuthRefreshOutcomeKind::SkippedCooldown),
            "stale failure result should be ignored instead of overwriting success"
        );

        let profile = registry
            .get_profile("openai-default")
            .expect("profile lookup should succeed")
            .expect("profile should exist");
        let AuthCredential::Oauth { refresh_state, .. } = profile.credential else {
            panic!("profile should keep oauth credential type");
        };
        assert_eq!(
            refresh_state.failure_count, 0,
            "stale concurrent failure must not increment failure count"
        );
        assert!(
            refresh_state.last_error.is_none(),
            "stale concurrent failure must not write last_error"
        );
        let access = load_secret_utf8(vault.as_ref(), "global/auth_openai_access")
            .expect("access token should be readable");
        assert_eq!(access, "race-access-token");
    }

    #[test]
    fn refresh_failure_reason_is_sanitized_and_does_not_leak_secret_material() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let identity_root = tempdir.path().join("identity");
        let vault_root = tempdir.path().join("vault");
        let registry =
            AuthProfileRegistry::open(identity_root.as_path()).expect("registry should initialize");
        let vault = open_test_vault(vault_root.as_path(), identity_root.as_path());
        persist_secret_utf8(&vault, "global/auth_openai_access", "access-old")
            .expect("access secret should persist");
        persist_secret_utf8(&vault, "global/auth_openai_refresh", "refresh-top-secret")
            .expect("refresh secret should persist");
        persist_secret_utf8(&vault, "global/auth_openai_client_secret", "client-secret")
            .expect("client secret should persist");

        let now = 1_730_000_000_000_i64;
        registry
            .set_profile(sample_oauth_profile_request(
                "https://example.test/token".to_owned(),
                Some(now.saturating_sub(1_000)),
                OAuthRefreshState::default(),
            ))
            .expect("profile should persist");

        let adapter = StubRefreshAdapter {
            response: Err(OAuthRefreshError::InvalidResponse(
                "response contains refresh_token=refresh-top-secret".to_owned(),
            )),
            call_count: Arc::new(Mutex::new(0)),
        };
        let outcome = registry
            .refresh_oauth_profile_with_clock("openai-default", &vault, &adapter, now)
            .expect("refresh failure should be persisted");
        assert_eq!(outcome.kind, OAuthRefreshOutcomeKind::Failed);
        assert!(
            !outcome.reason.contains("refresh-top-secret"),
            "sanitized refresh reason must not leak secret values"
        );

        let profile = registry
            .get_profile("openai-default")
            .expect("profile lookup should succeed")
            .expect("profile should exist");
        let AuthCredential::Oauth { refresh_state, .. } = profile.credential else {
            panic!("profile should keep oauth credential type");
        };
        let stored_error = refresh_state.last_error.unwrap_or_default();
        assert!(
            !stored_error.contains("refresh-top-secret"),
            "persisted refresh error should not leak refresh token values"
        );
    }

    #[test]
    fn health_report_state_survives_registry_reopen() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let identity_root = tempdir.path().join("identity");
        let vault_root = tempdir.path().join("vault");
        let now = 1_730_000_000_000_i64;

        let vault = open_test_vault(vault_root.as_path(), identity_root.as_path());
        persist_secret_utf8(&vault, "global/auth_openai_access", "access-old")
            .expect("access secret should persist");
        persist_secret_utf8(&vault, "global/auth_openai_refresh", "refresh-old")
            .expect("refresh secret should persist");
        persist_secret_utf8(&vault, "global/auth_openai_client_secret", "client-secret")
            .expect("client secret should persist");

        let registry =
            AuthProfileRegistry::open(identity_root.as_path()).expect("registry should initialize");
        registry
            .set_profile(sample_oauth_profile_request(
                "https://example.test/token".to_owned(),
                Some(now.saturating_sub(1_000)),
                OAuthRefreshState::default(),
            ))
            .expect("profile should persist");
        drop(registry);

        let reopened = AuthProfileRegistry::open(identity_root.as_path())
            .expect("registry should reopen from persisted file");
        let report = reopened
            .health_report_with_clock(&vault, None, now, 15 * 60 * 1_000)
            .expect("health report should compute");
        assert_eq!(report.summary.total, 1);
        assert_eq!(report.summary.expired, 1);
    }

    #[test]
    fn invalid_profile_id_is_rejected() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let identity_root = tempdir.path().join("identity");
        let registry =
            AuthProfileRegistry::open(identity_root.as_path()).expect("registry should initialize");
        let request = AuthProfileSetRequest {
            profile_id: "bad profile".to_owned(),
            provider: AuthProvider::known(AuthProviderKind::Openai),
            profile_name: "default".to_owned(),
            scope: AuthProfileScope::Global,
            credential: AuthCredential::ApiKey {
                api_key_vault_ref: "global/openai_api_key".to_owned(),
            },
        };
        let error = registry.set_profile(request).expect_err("invalid id should fail");
        assert!(
            matches!(error, AuthProfileError::InvalidField { field, .. } if field == "profile_id"),
            "invalid profile_id should return field validation error"
        );
    }

    #[test]
    fn refresh_due_profiles_marks_transport_failure_without_retry_spam() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let identity_root = tempdir.path().join("identity");
        let vault_root = tempdir.path().join("vault");
        let registry =
            AuthProfileRegistry::open(identity_root.as_path()).expect("registry should initialize");
        let vault = open_test_vault(vault_root.as_path(), identity_root.as_path());
        persist_secret_utf8(&vault, "global/auth_openai_access", "access-old")
            .expect("access secret should persist");
        persist_secret_utf8(&vault, "global/auth_openai_refresh", "refresh-old")
            .expect("refresh secret should persist");
        persist_secret_utf8(&vault, "global/auth_openai_client_secret", "client-secret")
            .expect("client secret should persist");
        let now = 1_730_000_000_000_i64;

        registry
            .set_profile(sample_oauth_profile_request(
                "https://example.test/token".to_owned(),
                Some(now.saturating_sub(1_000)),
                OAuthRefreshState::default(),
            ))
            .expect("profile should persist");
        let calls = Arc::new(Mutex::new(0_u64));
        let adapter = StubRefreshAdapter {
            response: Err(OAuthRefreshError::Transport("connection reset".to_owned())),
            call_count: Arc::clone(&calls),
        };
        let first = registry
            .refresh_due_oauth_profiles_with_clock(&vault, &adapter, None, now, 5 * 60 * 1_000)
            .expect("refresh sweep should complete");
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].kind, OAuthRefreshOutcomeKind::Failed);
        let second = registry
            .refresh_due_oauth_profiles_with_clock(
                &vault,
                &adapter,
                None,
                now.saturating_add(1_000),
                5 * 60 * 1_000,
            )
            .expect("refresh sweep should complete");
        assert_eq!(second[0].kind, OAuthRefreshOutcomeKind::SkippedCooldown);
        assert_eq!(
            *calls.lock().expect("call counter should be available"),
            1,
            "cooldown should suppress immediate repeated refresh attempts"
        );
    }
}
