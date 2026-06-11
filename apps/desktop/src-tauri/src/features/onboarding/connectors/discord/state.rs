//! Persisted Discord onboarding defaults for the desktop app.

use serde::{Deserialize, Serialize};

use crate::normalize_optional_text;

/// Per-profile Discord onboarding defaults and verification state.
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
    /// Returns the connector id derived from the configured account id.
    pub(crate) fn connector_id(&self) -> String {
        let account_id = normalize_optional_text(self.account_id.as_str()).unwrap_or("default");
        format!("discord:{account_id}")
    }
}
