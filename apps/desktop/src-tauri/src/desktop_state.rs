use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Context, Result};
use palyra_vault::{BackendPreference, Vault, VaultConfig, VaultError, VaultScope};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::{
    normalize_optional_text, DESKTOP_SECRET_KEY_ADMIN_TOKEN, DESKTOP_SECRET_KEY_BROWSER_AUTH_TOKEN,
    DESKTOP_SECRET_MAX_BYTES, DESKTOP_STATE_SCHEMA_VERSION,
};

const DESKTOP_ONBOARDING_EVENT_LIMIT: usize = 40;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DesktopOnboardingStep {
    Welcome,
    Environment,
    StateRoot,
    GatewayInit,
    OperatorAuthBootstrap,
    OpenAiConnect,
    DiscordConnect,
    DashboardHandoff,
    Completion,
}

impl DesktopOnboardingStep {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Welcome => "welcome",
            Self::Environment => "environment",
            Self::StateRoot => "state_root",
            Self::GatewayInit => "gateway_init",
            Self::OperatorAuthBootstrap => "operator_auth_bootstrap",
            Self::OpenAiConnect => "openai_connect",
            Self::DiscordConnect => "discord_connect",
            Self::DashboardHandoff => "dashboard_handoff",
            Self::Completion => "completion",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DesktopOnboardingFailureState {
    pub(crate) step: DesktopOnboardingStep,
    pub(crate) message: String,
    pub(crate) recorded_at_unix_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DesktopOnboardingEvent {
    pub(crate) kind: String,
    pub(crate) detail: Option<String>,
    pub(crate) recorded_at_unix_ms: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct DesktopOpenAiOnboardingState {
    pub(crate) preferred_method: Option<String>,
    pub(crate) last_profile_id: Option<String>,
    pub(crate) last_connected_at_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct DesktopDiscordOnboardingState {
    pub(crate) account_id: String,
    pub(crate) mode: String,
    pub(crate) inbound_scope: String,
    pub(crate) allow_from: Vec<String>,
    pub(crate) deny_from: Vec<String>,
    pub(crate) require_mention: bool,
    pub(crate) concurrency_limit: u64,
    pub(crate) broadcast_strategy: String,
    pub(crate) confirm_open_guild_channels: bool,
    pub(crate) verify_channel_id: Option<String>,
    pub(crate) last_connector_id: Option<String>,
    pub(crate) last_verified_target: Option<String>,
    pub(crate) last_verified_at_unix_ms: Option<i64>,
}

impl Default for DesktopDiscordOnboardingState {
    fn default() -> Self {
        Self {
            account_id: "default".to_owned(),
            mode: "local".to_owned(),
            inbound_scope: "dm_only".to_owned(),
            allow_from: Vec::new(),
            deny_from: Vec::new(),
            require_mention: true,
            concurrency_limit: 2,
            broadcast_strategy: "deny".to_owned(),
            confirm_open_guild_channels: false,
            verify_channel_id: None,
            last_connector_id: None,
            last_verified_target: None,
            last_verified_at_unix_ms: None,
        }
    }
}

impl DesktopDiscordOnboardingState {
    pub(crate) fn connector_id(&self) -> String {
        let account_id = normalize_optional_text(self.account_id.as_str()).unwrap_or("default");
        format!("discord:{account_id}")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct DesktopOnboardingState {
    pub(crate) flow_id: String,
    pub(crate) welcome_acknowledged_at_unix_ms: Option<i64>,
    pub(crate) state_root_confirmed_at_unix_ms: Option<i64>,
    pub(crate) dashboard_handoff_at_unix_ms: Option<i64>,
    pub(crate) completed_at_unix_ms: Option<i64>,
    pub(crate) last_failure: Option<DesktopOnboardingFailureState>,
    pub(crate) recent_events: Vec<DesktopOnboardingEvent>,
    pub(crate) failure_step_counts: BTreeMap<String, u64>,
    pub(crate) support_bundle_export_attempts: u64,
    pub(crate) support_bundle_export_successes: u64,
    pub(crate) support_bundle_export_failures: u64,
    pub(crate) openai: DesktopOpenAiOnboardingState,
    pub(crate) discord: DesktopDiscordOnboardingState,
}

impl DesktopOnboardingState {
    pub(crate) fn ensure_flow_id(&mut self) {
        if self.flow_id.trim().is_empty() {
            self.flow_id = Ulid::new().to_string();
        }
    }

    pub(crate) fn push_event(
        &mut self,
        kind: impl Into<String>,
        detail: Option<String>,
        recorded_at_unix_ms: i64,
    ) {
        self.recent_events.push(DesktopOnboardingEvent {
            kind: kind.into(),
            detail: detail.and_then(|value| normalize_optional_text(value.as_str()).map(str::to_owned)),
            recorded_at_unix_ms,
        });
        if self.recent_events.len() > DESKTOP_ONBOARDING_EVENT_LIMIT {
            let overflow = self.recent_events.len().saturating_sub(DESKTOP_ONBOARDING_EVENT_LIMIT);
            self.recent_events.drain(0..overflow);
        }
    }

    pub(crate) fn record_failure_step(&mut self, step: DesktopOnboardingStep) {
        let key = step.as_str().to_owned();
        let next = self.failure_step_counts.get(key.as_str()).copied().unwrap_or_default();
        self.failure_step_counts.insert(key, next.saturating_add(1));
    }

    pub(crate) fn record_support_bundle_export_result(&mut self, success: bool) {
        self.support_bundle_export_attempts = self.support_bundle_export_attempts.saturating_add(1);
        if success {
            self.support_bundle_export_successes =
                self.support_bundle_export_successes.saturating_add(1);
        } else {
            self.support_bundle_export_failures =
                self.support_bundle_export_failures.saturating_add(1);
        }
    }
}

impl Default for DesktopOnboardingState {
    fn default() -> Self {
        Self {
            flow_id: Ulid::new().to_string(),
            welcome_acknowledged_at_unix_ms: None,
            state_root_confirmed_at_unix_ms: None,
            dashboard_handoff_at_unix_ms: None,
            completed_at_unix_ms: None,
            last_failure: None,
            recent_events: Vec::new(),
            failure_step_counts: BTreeMap::new(),
            support_bundle_export_attempts: 0,
            support_bundle_export_successes: 0,
            support_bundle_export_failures: 0,
            openai: DesktopOpenAiOnboardingState::default(),
            discord: DesktopDiscordOnboardingState::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct DesktopStateFile {
    pub(crate) schema_version: u32,
    pub(crate) browser_service_enabled: bool,
    pub(crate) runtime_state_root: Option<String>,
    pub(crate) onboarding: DesktopOnboardingState,
}

impl DesktopStateFile {
    pub(crate) fn new_default() -> Self {
        Self {
            schema_version: DESKTOP_STATE_SCHEMA_VERSION,
            browser_service_enabled: true,
            runtime_state_root: None,
            onboarding: DesktopOnboardingState::default(),
        }
    }

    pub(crate) fn resolve_runtime_root(&self, default_root: &Path) -> Result<PathBuf> {
        match normalize_optional_text(self.runtime_state_root.as_deref().unwrap_or_default()) {
            Some(raw) => {
                let candidate = PathBuf::from(raw);
                if !candidate.is_absolute() {
                    return Err(anyhow!("desktop runtime state root must be an absolute path"));
                }
                Ok(candidate)
            }
            None => Ok(default_root.to_path_buf()),
        }
    }

    pub(crate) fn normalized_runtime_state_root(&self) -> Option<String> {
        normalize_optional_text(self.runtime_state_root.as_deref().unwrap_or_default())
            .map(str::to_owned)
    }
}

impl Default for DesktopStateFile {
    fn default() -> Self {
        Self::new_default()
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
struct PersistedDesktopStateEnvelope {
    #[serde(default = "default_legacy_schema_version")]
    schema_version: u32,
    #[serde(default)]
    admin_token: String,
    #[serde(default)]
    browser_auth_token: String,
    #[serde(default = "default_browser_service_enabled")]
    browser_service_enabled: bool,
    #[serde(default)]
    runtime_state_root: Option<String>,
    #[serde(default)]
    onboarding: DesktopOnboardingState,
}

impl Default for PersistedDesktopStateEnvelope {
    fn default() -> Self {
        Self {
            schema_version: default_legacy_schema_version(),
            admin_token: String::new(),
            browser_auth_token: String::new(),
            browser_service_enabled: default_browser_service_enabled(),
            runtime_state_root: None,
            onboarding: DesktopOnboardingState::default(),
        }
    }
}

impl PersistedDesktopStateEnvelope {
    fn into_state(self) -> DesktopStateFile {
        let _ = self.schema_version;
        DesktopStateFile {
            schema_version: DESKTOP_STATE_SCHEMA_VERSION,
            browser_service_enabled: self.browser_service_enabled,
            runtime_state_root: normalize_optional_text(
                self.runtime_state_root.as_deref().unwrap_or_default(),
            )
            .map(str::to_owned),
            onboarding: self.onboarding,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct LoadedDesktopState {
    pub(crate) persisted: DesktopStateFile,
    pub(crate) admin_token: String,
    pub(crate) browser_auth_token: String,
}

pub(crate) struct DesktopSecretStore {
    vault: Vault,
}

impl DesktopSecretStore {
    pub(crate) fn open(state_dir: &Path) -> Result<Self> {
        let backend_preference =
            if cfg!(test) { BackendPreference::EncryptedFile } else { BackendPreference::Auto };
        let vault = Vault::open_with_config(VaultConfig {
            root: Some(state_dir.join("vault")),
            identity_store_root: Some(state_dir.join("identity")),
            backend_preference,
            max_secret_bytes: DESKTOP_SECRET_MAX_BYTES,
        })
        .map_err(|error| anyhow!("failed to initialize desktop secret store: {error}"))?;
        Ok(Self { vault })
    }

    pub(crate) fn load_or_create_secret(
        &self,
        key: &str,
        legacy_value: Option<&str>,
    ) -> Result<String> {
        let scope = VaultScope::Global;
        if let Some(value) = self.read_secret_utf8(&scope, key)? {
            return Ok(value);
        }

        let value = normalize_optional_text(legacy_value.unwrap_or_default())
            .map(ToOwned::to_owned)
            .unwrap_or_else(generate_secret_token);
        self.vault
            .put_secret(&scope, key, value.as_bytes())
            .map_err(|error| anyhow!("failed to persist desktop secret '{key}': {error}"))?;
        Ok(value)
    }

    fn read_secret_utf8(&self, scope: &VaultScope, key: &str) -> Result<Option<String>> {
        match self.vault.get_secret(scope, key) {
            Ok(raw) => {
                let decoded = String::from_utf8(raw)
                    .with_context(|| format!("desktop secret '{key}' contains non UTF-8 bytes"))?;
                if decoded.trim().is_empty() {
                    return Ok(None);
                }
                Ok(Some(decoded))
            }
            Err(VaultError::NotFound) => Ok(None),
            Err(error) => Err(anyhow!("failed to read desktop secret '{key}': {error}")),
        }
    }
}

const fn default_legacy_schema_version() -> u32 {
    1
}

const fn default_browser_service_enabled() -> bool {
    true
}

pub(crate) fn resolve_desktop_state_root() -> Result<PathBuf> {
    palyra_common::default_state_root().map_err(|error| {
        anyhow!("failed to resolve default state root for desktop control center: {}", error)
    })
}

pub(crate) fn load_or_initialize_state_file(
    path: &Path,
    secret_store: &DesktopSecretStore,
) -> Result<LoadedDesktopState> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!("failed to create desktop state directory {}", parent.display())
        })?;
    }

    if path.exists() {
        let raw = fs::read_to_string(path)
            .with_context(|| format!("failed to read desktop state file {}", path.display()))?;
        let persisted_envelope: PersistedDesktopStateEnvelope = serde_json::from_str(raw.as_str())
            .with_context(|| format!("failed to parse desktop state file {}", path.display()))?;
        let admin_token = secret_store.load_or_create_secret(
            DESKTOP_SECRET_KEY_ADMIN_TOKEN,
            Some(persisted_envelope.admin_token.as_str()),
        )?;
        let browser_auth_token = secret_store.load_or_create_secret(
            DESKTOP_SECRET_KEY_BROWSER_AUTH_TOKEN,
            Some(persisted_envelope.browser_auth_token.as_str()),
        )?;
        let mut persisted = persisted_envelope.into_state();
        persisted.onboarding.ensure_flow_id();
        persist_desktop_state_file(path, &persisted, "normalized")?;
        return Ok(LoadedDesktopState { persisted, admin_token, browser_auth_token });
    }

    let persisted = DesktopStateFile::new_default();
    let admin_token = secret_store.load_or_create_secret(DESKTOP_SECRET_KEY_ADMIN_TOKEN, None)?;
    let browser_auth_token =
        secret_store.load_or_create_secret(DESKTOP_SECRET_KEY_BROWSER_AUTH_TOKEN, None)?;
    persist_desktop_state_file(path, &persisted, "default")?;
    Ok(LoadedDesktopState { persisted, admin_token, browser_auth_token })
}

fn persist_desktop_state_file(path: &Path, state: &DesktopStateFile, label: &str) -> Result<()> {
    let encoded = serde_json::to_string_pretty(state)
        .with_context(|| format!("failed to encode {label} desktop state file"))?;
    fs::write(path, encoded)
        .with_context(|| format!("failed to persist {label} desktop state file {}", path.display()))
}

fn generate_secret_token() -> String {
    format!("{}{}", Ulid::new(), Ulid::new())
}
