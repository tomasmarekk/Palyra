use std::{
    collections::BTreeMap,
    fs,
    path::{Component, Path, PathBuf},
};

use anyhow::{anyhow, Context, Result};
use palyra_vault::{BackendPreference, Vault, VaultConfig, VaultError, VaultScope};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

pub(crate) use crate::features::onboarding::connectors::discord::DesktopDiscordOnboardingState;

use super::{
    normalize_optional_text, DESKTOP_SECRET_KEY_ADMIN_TOKEN, DESKTOP_SECRET_KEY_BROWSER_AUTH_TOKEN,
    DESKTOP_SECRET_MAX_BYTES, DESKTOP_STATE_SCHEMA_VERSION,
};

const DESKTOP_ONBOARDING_EVENT_LIMIT: usize = 40;
const DESKTOP_COMPANION_NOTIFICATION_LIMIT: usize = 40;
const DESKTOP_COMPANION_OFFLINE_DRAFT_LIMIT: usize = 20;
const DESKTOP_RECENT_PROFILE_LIMIT: usize = 6;
pub(crate) const IMPLICIT_DESKTOP_PROFILE_NAME: &str = "desktop-local";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DesktopCompanionSection {
    Home,
    Chat,
    Approvals,
    Access,
    Onboarding,
}

impl Default for DesktopCompanionSection {
    fn default() -> Self {
        Self::Home
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DesktopCompanionNotificationKind {
    Approval,
    Connection,
    Run,
    Draft,
    Trust,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DesktopCompanionNotification {
    pub(crate) notification_id: String,
    pub(crate) kind: DesktopCompanionNotificationKind,
    pub(crate) title: String,
    pub(crate) detail: String,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) read: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DesktopCompanionOfflineDraft {
    pub(crate) draft_id: String,
    pub(crate) session_id: Option<String>,
    pub(crate) text: String,
    pub(crate) reason: String,
    pub(crate) created_at_unix_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct DesktopCompanionRolloutState {
    pub(crate) companion_shell_enabled: bool,
    pub(crate) desktop_notifications_enabled: bool,
    pub(crate) offline_drafts_enabled: bool,
    pub(crate) release_channel: String,
}

impl Default for DesktopCompanionRolloutState {
    fn default() -> Self {
        Self {
            companion_shell_enabled: true,
            desktop_notifications_enabled: true,
            offline_drafts_enabled: true,
            release_channel: "preview".to_owned(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct DesktopCompanionState {
    pub(crate) active_section: DesktopCompanionSection,
    pub(crate) active_session_id: Option<String>,
    pub(crate) active_device_id: Option<String>,
    pub(crate) last_run_id: Option<String>,
    pub(crate) last_connection_state: String,
    pub(crate) last_connected_at_unix_ms: Option<i64>,
    pub(crate) last_pending_approval_count: usize,
    pub(crate) notifications: Vec<DesktopCompanionNotification>,
    pub(crate) offline_drafts: Vec<DesktopCompanionOfflineDraft>,
    pub(crate) rollout: DesktopCompanionRolloutState,
}

impl Default for DesktopCompanionState {
    fn default() -> Self {
        Self {
            active_section: DesktopCompanionSection::Home,
            active_session_id: None,
            active_device_id: None,
            last_run_id: None,
            last_connection_state: "unknown".to_owned(),
            last_connected_at_unix_ms: None,
            last_pending_approval_count: 0,
            notifications: Vec::new(),
            offline_drafts: Vec::new(),
            rollout: DesktopCompanionRolloutState::default(),
        }
    }
}

impl DesktopCompanionState {
    pub(crate) fn set_active_section(&mut self, next: DesktopCompanionSection) {
        self.active_section = next;
    }

    pub(crate) fn set_active_session_id(&mut self, next: Option<&str>) {
        self.active_session_id = next.and_then(normalize_optional_text).map(str::to_owned);
    }

    pub(crate) fn set_active_device_id(&mut self, next: Option<&str>) {
        self.active_device_id = next.and_then(normalize_optional_text).map(str::to_owned);
    }

    pub(crate) fn set_last_run_id(&mut self, next: Option<&str>) {
        self.last_run_id = next.and_then(normalize_optional_text).map(str::to_owned);
    }

    pub(crate) fn push_notification(
        &mut self,
        kind: DesktopCompanionNotificationKind,
        title: impl Into<String>,
        detail: impl Into<String>,
        created_at_unix_ms: i64,
    ) {
        self.notifications.push(DesktopCompanionNotification {
            notification_id: Ulid::new().to_string(),
            kind,
            title: title.into(),
            detail: detail.into(),
            created_at_unix_ms,
            read: false,
        });
        if self.notifications.len() > DESKTOP_COMPANION_NOTIFICATION_LIMIT {
            let overflow =
                self.notifications.len().saturating_sub(DESKTOP_COMPANION_NOTIFICATION_LIMIT);
            self.notifications.drain(0..overflow);
        }
    }

    pub(crate) fn mark_notifications_read(&mut self, ids: Option<&[String]>) {
        for notification in &mut self.notifications {
            let should_mark = match ids {
                None => true,
                Some(values) => {
                    values.iter().any(|candidate| candidate == &notification.notification_id)
                }
            };
            if should_mark {
                notification.read = true;
            }
        }
    }

    pub(crate) fn queue_offline_draft(
        &mut self,
        session_id: Option<&str>,
        text: &str,
        reason: &str,
        created_at_unix_ms: i64,
    ) -> String {
        let draft_id = Ulid::new().to_string();
        self.offline_drafts.push(DesktopCompanionOfflineDraft {
            draft_id: draft_id.clone(),
            session_id: session_id.and_then(normalize_optional_text).map(str::to_owned),
            text: text.to_owned(),
            reason: reason.to_owned(),
            created_at_unix_ms,
        });
        if self.offline_drafts.len() > DESKTOP_COMPANION_OFFLINE_DRAFT_LIMIT {
            let overflow =
                self.offline_drafts.len().saturating_sub(DESKTOP_COMPANION_OFFLINE_DRAFT_LIMIT);
            self.offline_drafts.drain(0..overflow);
        }
        draft_id
    }

    pub(crate) fn remove_offline_draft(&mut self, draft_id: &str) {
        self.offline_drafts.retain(|draft| draft.draft_id != draft_id);
    }
}

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
            detail: detail
                .and_then(|value| normalize_optional_text(value.as_str()).map(str::to_owned)),
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
pub(crate) struct DesktopProfileState {
    pub(crate) runtime_state_root: Option<String>,
    pub(crate) onboarding: DesktopOnboardingState,
    pub(crate) companion: DesktopCompanionState,
}

impl Default for DesktopProfileState {
    fn default() -> Self {
        Self {
            runtime_state_root: None,
            onboarding: DesktopOnboardingState::default(),
            companion: DesktopCompanionState::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct DesktopStateFile {
    pub(crate) schema_version: u32,
    pub(crate) browser_service_enabled: bool,
    pub(crate) active_profile_name: Option<String>,
    pub(crate) recent_profile_names: Vec<String>,
    pub(crate) profile_states: BTreeMap<String, DesktopProfileState>,
}

impl DesktopStateFile {
    pub(crate) fn new_default() -> Self {
        let mut profile_states = BTreeMap::new();
        profile_states
            .insert(IMPLICIT_DESKTOP_PROFILE_NAME.to_owned(), DesktopProfileState::default());
        Self {
            schema_version: DESKTOP_STATE_SCHEMA_VERSION,
            browser_service_enabled: true,
            active_profile_name: Some(IMPLICIT_DESKTOP_PROFILE_NAME.to_owned()),
            recent_profile_names: vec![IMPLICIT_DESKTOP_PROFILE_NAME.to_owned()],
            profile_states,
        }
    }

    pub(crate) fn resolve_runtime_root(&self, default_root: &Path) -> Result<PathBuf> {
        validate_runtime_state_root_override(
            normalize_optional_text(
                self.active_profile_state().runtime_state_root.as_deref().unwrap_or_default(),
            ),
            default_root,
        )
    }

    pub(crate) fn normalized_runtime_state_root(&self) -> Option<String> {
        normalize_optional_text(
            self.active_profile_state().runtime_state_root.as_deref().unwrap_or_default(),
        )
        .map(str::to_owned)
    }

    pub(crate) fn active_profile_name(&self) -> &str {
        self.active_profile_name
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(IMPLICIT_DESKTOP_PROFILE_NAME)
    }

    pub(crate) fn recent_profile_names(&self) -> &[String] {
        self.recent_profile_names.as_slice()
    }

    pub(crate) fn active_profile_state(&self) -> &DesktopProfileState {
        self.profile_states
            .get(self.active_profile_name())
            .or_else(|| self.profile_states.get(IMPLICIT_DESKTOP_PROFILE_NAME))
            .or_else(|| self.profile_states.values().next())
            .expect("desktop state must always contain at least one profile state")
    }

    pub(crate) fn active_profile_state_mut(&mut self) -> &mut DesktopProfileState {
        let profile_name = self.active_profile_name().to_owned();
        self.ensure_profile_state(profile_name.as_str())
    }

    pub(crate) fn active_onboarding(&self) -> &DesktopOnboardingState {
        &self.active_profile_state().onboarding
    }

    pub(crate) fn active_onboarding_mut(&mut self) -> &mut DesktopOnboardingState {
        &mut self.active_profile_state_mut().onboarding
    }

    pub(crate) fn active_companion(&self) -> &DesktopCompanionState {
        &self.active_profile_state().companion
    }

    pub(crate) fn active_companion_mut(&mut self) -> &mut DesktopCompanionState {
        &mut self.active_profile_state_mut().companion
    }

    pub(crate) fn ensure_profile_state(&mut self, profile_name: &str) -> &mut DesktopProfileState {
        self.profile_states
            .entry(profile_name.to_owned())
            .or_insert_with(DesktopProfileState::default)
    }

    pub(crate) fn activate_profile(&mut self, profile_name: &str) {
        let normalized = normalize_profile_name(profile_name);
        self.active_profile_name = Some(normalized.clone());
        let _ = self.ensure_profile_state(normalized.as_str());
        self.promote_recent_profile(normalized.as_str());
    }

    pub(crate) fn active_profile_completion_unix_ms(&self) -> Option<i64> {
        self.active_onboarding().completed_at_unix_ms
    }

    pub(crate) fn ensure_profile_integrity(&mut self) {
        if self.profile_states.is_empty() {
            self.profile_states
                .insert(IMPLICIT_DESKTOP_PROFILE_NAME.to_owned(), DesktopProfileState::default());
        }

        let active_name = normalize_profile_name(self.active_profile_name());
        if !self.profile_states.contains_key(active_name.as_str()) {
            self.profile_states.insert(active_name.clone(), DesktopProfileState::default());
        }
        self.active_profile_name = Some(active_name.clone());
        self.promote_recent_profile(active_name.as_str());

        let valid_names = self.profile_states.keys().cloned().collect::<Vec<_>>();
        self.recent_profile_names
            .retain(|name| valid_names.iter().any(|candidate| candidate == name));
        if self.recent_profile_names.len() > DESKTOP_RECENT_PROFILE_LIMIT {
            self.recent_profile_names.truncate(DESKTOP_RECENT_PROFILE_LIMIT);
        }

        for state in self.profile_states.values_mut() {
            state.onboarding.ensure_flow_id();
        }
    }

    fn promote_recent_profile(&mut self, profile_name: &str) {
        self.recent_profile_names.retain(|candidate| candidate != profile_name);
        self.recent_profile_names.insert(0, profile_name.to_owned());
        if self.recent_profile_names.len() > DESKTOP_RECENT_PROFILE_LIMIT {
            self.recent_profile_names.truncate(DESKTOP_RECENT_PROFILE_LIMIT);
        }
    }
}

impl Default for DesktopStateFile {
    fn default() -> Self {
        Self::new_default()
    }
}

pub(crate) fn validate_runtime_state_root_override(
    candidate: Option<&str>,
    default_root: &Path,
) -> Result<PathBuf> {
    let Some(raw) = candidate.and_then(normalize_optional_text) else {
        return Ok(default_root.to_path_buf());
    };
    let parsed = PathBuf::from(raw);
    if !parsed.is_absolute() {
        return Err(anyhow!("desktop runtime state root must be an absolute path"));
    }
    if parsed.components().any(|component| matches!(component, Component::ParentDir)) {
        return Err(anyhow!("desktop runtime state root must not contain parent traversal"));
    }

    let allowed_root = default_root.parent().ok_or_else(|| {
        anyhow!("desktop runtime state root validation requires a desktop state directory parent")
    })?;
    let canonical_allowed_root = fs::canonicalize(allowed_root).with_context(|| {
        format!("failed to resolve desktop state directory {}", allowed_root.display())
    })?;
    let existing_ancestor =
        parsed.ancestors().find(|ancestor| ancestor.exists()).ok_or_else(|| {
            anyhow!("desktop runtime state root must include an existing filesystem root")
        })?;
    let canonical_existing_ancestor = fs::canonicalize(existing_ancestor).with_context(|| {
        format!(
            "failed to resolve desktop runtime state root ancestor {}",
            existing_ancestor.display()
        )
    })?;
    if !canonical_existing_ancestor.starts_with(canonical_allowed_root.as_path()) {
        return Err(anyhow!(
            "desktop runtime state root must stay within the desktop state directory {}",
            canonical_allowed_root.display()
        ));
    }
    Ok(parsed)
}

#[derive(Clone, Deserialize)]
#[serde(default)]
struct PersistedDesktopStateEnvelope {
    #[serde(default = "default_legacy_schema_version")]
    schema_version: u32,
    #[serde(flatten)]
    legacy_secrets: LegacyDesktopSecrets,
    #[serde(default = "default_browser_service_enabled")]
    browser_service_enabled: bool,
    #[serde(default)]
    active_profile_name: Option<String>,
    #[serde(default)]
    recent_profile_names: Vec<String>,
    #[serde(default)]
    profile_states: BTreeMap<String, DesktopProfileState>,
    #[serde(default)]
    runtime_state_root: Option<String>,
    #[serde(default)]
    onboarding: DesktopOnboardingState,
    #[serde(default)]
    companion: DesktopCompanionState,
}

impl std::fmt::Debug for PersistedDesktopStateEnvelope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PersistedDesktopStateEnvelope")
            .field("schema_version", &self.schema_version)
            .field("legacy_secrets", &"<redacted>")
            .field("browser_service_enabled", &self.browser_service_enabled)
            .field("active_profile_name", &self.active_profile_name)
            .field("recent_profile_names", &self.recent_profile_names)
            .field("profile_states", &self.profile_states)
            .field("runtime_state_root", &self.runtime_state_root)
            .field("onboarding", &self.onboarding)
            .field("companion", &self.companion)
            .finish()
    }
}

impl Default for PersistedDesktopStateEnvelope {
    fn default() -> Self {
        Self {
            schema_version: default_legacy_schema_version(),
            legacy_secrets: LegacyDesktopSecrets::default(),
            browser_service_enabled: default_browser_service_enabled(),
            active_profile_name: Some(IMPLICIT_DESKTOP_PROFILE_NAME.to_owned()),
            recent_profile_names: vec![IMPLICIT_DESKTOP_PROFILE_NAME.to_owned()],
            profile_states: BTreeMap::new(),
            runtime_state_root: None,
            onboarding: DesktopOnboardingState::default(),
            companion: DesktopCompanionState::default(),
        }
    }
}

#[derive(Clone, Default, Deserialize)]
#[serde(default)]
struct LegacyDesktopSecrets {
    admin_token: String,
    browser_auth_token: String,
}

impl std::fmt::Debug for LegacyDesktopSecrets {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LegacyDesktopSecrets")
            .field("admin_token", &"<redacted>")
            .field("browser_auth_token", &"<redacted>")
            .finish()
    }
}

impl PersistedDesktopStateEnvelope {
    fn into_parts(self) -> (DesktopStateFile, LegacyDesktopSecrets) {
        let _ = self.schema_version;
        let legacy_secrets = self.legacy_secrets;
        let mut profile_states = self.profile_states;
        if profile_states.is_empty() {
            profile_states.insert(
                IMPLICIT_DESKTOP_PROFILE_NAME.to_owned(),
                DesktopProfileState {
                    runtime_state_root: normalize_optional_text(
                        self.runtime_state_root.as_deref().unwrap_or_default(),
                    )
                    .map(str::to_owned),
                    onboarding: self.onboarding,
                    companion: self.companion,
                },
            );
        }

        let mut state = DesktopStateFile {
            schema_version: DESKTOP_STATE_SCHEMA_VERSION,
            browser_service_enabled: self.browser_service_enabled,
            active_profile_name: self.active_profile_name,
            recent_profile_names: self.recent_profile_names,
            profile_states,
        };
        state.ensure_profile_integrity();
        (state, legacy_secrets)
    }
}

#[derive(Clone)]
pub(crate) struct LoadedDesktopState {
    pub(crate) persisted: DesktopStateFile,
    pub(crate) admin_token: String,
    pub(crate) browser_auth_token: String,
}

impl std::fmt::Debug for LoadedDesktopState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoadedDesktopState")
            .field("persisted", &self.persisted)
            .field("admin_token", &"<redacted>")
            .field("browser_auth_token", &"<redacted>")
            .finish()
    }
}

struct DesktopRuntimeSecrets {
    admin_token: String,
    browser_auth_token: String,
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
    if let Ok(raw) = std::env::var("PALYRA_STATE_ROOT") {
        if let Some(value) = normalize_optional_text(raw.as_str()) {
            let candidate = PathBuf::from(value);
            if !candidate.is_absolute() {
                return Err(anyhow!(
                    "desktop state root from PALYRA_STATE_ROOT must be an absolute path"
                ));
            }
            return Ok(candidate);
        }
    }

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
        let (mut persisted, legacy_secrets) = persisted_envelope.into_parts();
        let runtime_secrets = load_desktop_runtime_secrets(secret_store, legacy_secrets)?;
        persisted.ensure_profile_integrity();
        persist_desktop_state_file(path, &persisted, "normalized")?;
        return Ok(LoadedDesktopState {
            persisted,
            admin_token: runtime_secrets.admin_token,
            browser_auth_token: runtime_secrets.browser_auth_token,
        });
    }

    let persisted = DesktopStateFile::new_default();
    let runtime_secrets = load_desktop_runtime_secrets(secret_store, LegacyDesktopSecrets::default())?;
    persist_desktop_state_file(path, &persisted, "default")?;
    Ok(LoadedDesktopState {
        persisted,
        admin_token: runtime_secrets.admin_token,
        browser_auth_token: runtime_secrets.browser_auth_token,
    })
}

fn persist_desktop_state_file(path: &Path, state: &DesktopStateFile, label: &str) -> Result<()> {
    let encoded = serde_json::to_string_pretty(state)
        .with_context(|| format!("failed to encode {label} desktop state file"))?;
    fs::write(path, encoded)
        .with_context(|| format!("failed to persist {label} desktop state file {}", path.display()))
}

fn load_desktop_runtime_secrets(
    secret_store: &DesktopSecretStore,
    legacy_secrets: LegacyDesktopSecrets,
) -> Result<DesktopRuntimeSecrets> {
    let admin_token = secret_store.load_or_create_secret(
        DESKTOP_SECRET_KEY_ADMIN_TOKEN,
        Some(legacy_secrets.admin_token.as_str()),
    )?;
    let browser_auth_token = secret_store.load_or_create_secret(
        DESKTOP_SECRET_KEY_BROWSER_AUTH_TOKEN,
        Some(legacy_secrets.browser_auth_token.as_str()),
    )?;
    Ok(DesktopRuntimeSecrets { admin_token, browser_auth_token })
}

fn generate_secret_token() -> String {
    format!("{}{}", Ulid::new(), Ulid::new())
}

fn normalize_profile_name(raw: &str) -> String {
    normalize_optional_text(raw).unwrap_or(IMPLICIT_DESKTOP_PROFILE_NAME).to_owned()
}

#[cfg(test)]
mod tests {
    use super::{
        DesktopCompanionNotificationKind, DesktopCompanionState, LegacyDesktopSecrets,
        PersistedDesktopStateEnvelope, DESKTOP_COMPANION_NOTIFICATION_LIMIT,
        DESKTOP_COMPANION_OFFLINE_DRAFT_LIMIT,
    };

    #[test]
    fn companion_notifications_can_be_marked_read_selectively() {
        let mut state = DesktopCompanionState::default();
        state.push_notification(DesktopCompanionNotificationKind::Run, "Run finished", "detail", 1);
        state.push_notification(
            DesktopCompanionNotificationKind::Approval,
            "Approval waiting",
            "detail",
            2,
        );

        let first_id = state.notifications[0].notification_id.clone();
        state.mark_notifications_read(Some(&[first_id]));

        assert!(state.notifications[0].read);
        assert!(!state.notifications[1].read);
    }

    #[test]
    fn companion_offline_draft_queue_stays_bounded() {
        let mut state = DesktopCompanionState::default();
        for index in 0..(DESKTOP_COMPANION_OFFLINE_DRAFT_LIMIT + 3) {
            let text = format!("draft-{index}");
            state.queue_offline_draft(Some("session-1"), text.as_str(), "offline", index as i64);
        }

        assert_eq!(state.offline_drafts.len(), DESKTOP_COMPANION_OFFLINE_DRAFT_LIMIT);
        assert!(state.offline_drafts.iter().all(|draft| draft.text != "draft-0"
            && draft.text != "draft-1"
            && draft.text != "draft-2"));
    }

    #[test]
    fn companion_notification_list_stays_bounded() {
        let mut state = DesktopCompanionState::default();
        for index in 0..(DESKTOP_COMPANION_NOTIFICATION_LIMIT + 2) {
            state.push_notification(
                DesktopCompanionNotificationKind::Connection,
                format!("notification-{index}"),
                "detail",
                index as i64,
            );
        }

        assert_eq!(state.notifications.len(), DESKTOP_COMPANION_NOTIFICATION_LIMIT);
        assert!(state
            .notifications
            .iter()
            .all(|entry| entry.title != "notification-0" && entry.title != "notification-1"));
    }

    #[test]
    fn persisted_state_envelope_debug_redacts_legacy_secrets() {
        let mut envelope = PersistedDesktopStateEnvelope::default();
        envelope.legacy_secrets = LegacyDesktopSecrets {
            admin_token: "admin-secret".to_owned(),
            browser_auth_token: "browser-secret".to_owned(),
        };

        let rendered = format!("{envelope:?}");

        assert!(!rendered.contains("admin-secret"));
        assert!(!rendered.contains("browser-secret"));
        assert!(rendered.contains("<redacted>"));
    }
}
