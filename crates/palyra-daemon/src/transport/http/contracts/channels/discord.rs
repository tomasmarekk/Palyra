//! Discord-specific HTTP DTOs for channel onboarding and lifecycle routes.
//!
//! These types are intentionally transport-local because the web console and
//! admin APIs need Discord onboarding details that are not part of the shared
//! control-plane protobuf surface.

use palyra_connectors::providers::discord::DiscordPrivilegedIntentsSummary;
use serde::{Deserialize, Serialize};

/// Request body for Discord account logout or removal.
#[derive(Debug, Default, Deserialize)]
pub(crate) struct DiscordAccountLifecycleRequest {
    #[serde(default)]
    pub(crate) keep_credential: Option<bool>,
}

/// Request body variant for lifecycle routes that carry the account id in JSON.
#[derive(Debug, Deserialize)]
pub(crate) struct DiscordAccountLifecycleActionRequest {
    pub(crate) account_id: String,
    #[serde(default)]
    pub(crate) keep_credential: Option<bool>,
}

/// Request body for Discord onboarding preflight and apply operations.
#[derive(Debug, Deserialize)]
pub(crate) struct DiscordOnboardingRequest {
    #[serde(default)]
    pub(crate) account_id: Option<String>,
    pub(crate) token: String,
    #[serde(default)]
    pub(crate) mode: Option<String>,
    #[serde(default)]
    pub(crate) inbound_scope: Option<String>,
    #[serde(default)]
    pub(crate) allow_from: Option<Vec<String>>,
    #[serde(default)]
    pub(crate) deny_from: Option<Vec<String>>,
    #[serde(default)]
    pub(crate) require_mention: Option<bool>,
    #[serde(default)]
    pub(crate) mention_patterns: Option<Vec<String>>,
    #[serde(default)]
    pub(crate) concurrency_limit: Option<u64>,
    #[serde(default)]
    pub(crate) direct_message_policy: Option<String>,
    #[serde(default)]
    pub(crate) broadcast_strategy: Option<String>,
    #[serde(default)]
    pub(crate) confirm_open_guild_channels: Option<bool>,
    #[serde(default)]
    pub(crate) verify_channel_id: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
/// Deployment target selected for Discord onboarding.
pub(crate) enum DiscordOnboardingMode {
    /// Run the Discord connector on the local daemon host.
    Local,
    /// Run the Discord connector through a remote VPS deployment.
    RemoteVps,
}

impl DiscordOnboardingMode {
    /// Parses a user-facing onboarding mode alias.
    pub(crate) fn parse(raw: Option<&str>) -> Option<Self> {
        let normalized = raw?.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "local" => Some(Self::Local),
            "remote_vps" | "remote-vps" | "remote" | "vps" => Some(Self::RemoteVps),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
/// Inbound message scope selected for Discord onboarding.
pub(crate) enum DiscordOnboardingScope {
    /// Accept only direct messages.
    DmOnly,
    /// Accept guild-channel messages only from configured allowlists.
    AllowlistedGuildChannels,
    /// Accept open guild-channel messages after explicit confirmation.
    OpenGuildChannels,
}

impl DiscordOnboardingScope {
    /// Parses a user-facing onboarding scope alias.
    pub(crate) fn parse(raw: Option<&str>) -> Option<Self> {
        let normalized = raw?.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "dm_only" | "dm-only" | "dm" => Some(Self::DmOnly),
            "allowlisted_guild_channels" | "allowlisted-guild-channels" | "allowlisted" => {
                Some(Self::AllowlistedGuildChannels)
            }
            "open_guild_channels" | "open-guild-channels" | "open" => Some(Self::OpenGuildChannels),
            _ => None,
        }
    }
}

/// Discord application metadata returned by onboarding preflight.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct DiscordApplicationSummary {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) flags: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) intents: Option<DiscordPrivilegedIntentsSummary>,
}

/// Discord bot identity returned by onboarding preflight.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct DiscordBotIdentitySummary {
    pub(crate) id: String,
    pub(crate) username: String,
}

/// Preview of the connector routing policy that onboarding would install.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct DiscordRoutingPreview {
    pub(crate) connector_id: String,
    pub(crate) mode: DiscordOnboardingMode,
    pub(crate) inbound_scope: DiscordOnboardingScope,
    pub(crate) require_mention: bool,
    pub(crate) mention_patterns: Vec<String>,
    pub(crate) allow_from: Vec<String>,
    pub(crate) deny_from: Vec<String>,
    pub(crate) allow_direct_messages: bool,
    pub(crate) direct_message_policy: String,
    pub(crate) broadcast_strategy: String,
    pub(crate) concurrency_limit: u64,
}

/// Runtime inbound-monitor state returned by Discord onboarding preflight.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct DiscordInboundMonitorSummary {
    pub(crate) connector_registered: bool,
    pub(crate) gateway_connected: bool,
    pub(crate) recent_inbound: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) last_inbound_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) last_connect_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) last_disconnect_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) last_event_type: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
/// Result class for checking Discord channel permissions.
pub(crate) enum DiscordChannelPermissionCheckStatus {
    /// Permissions were fetched and meet the required set.
    Ok,
    /// Discord rejected the permission lookup.
    Forbidden,
    /// The requested channel was not found.
    NotFound,
    /// Permission lookup could not be completed.
    Unavailable,
    /// Discord returned permissions that could not be parsed.
    ParseError,
}

/// Channel-level permission check returned by onboarding preflight.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct DiscordChannelPermissionCheck {
    pub(crate) channel_id: String,
    pub(crate) status: DiscordChannelPermissionCheckStatus,
    pub(crate) can_view_channel: bool,
    pub(crate) can_send_messages: bool,
    pub(crate) can_read_message_history: bool,
    pub(crate) can_embed_links: bool,
    pub(crate) can_attach_files: bool,
    pub(crate) can_send_messages_in_threads: bool,
}

/// Full Discord onboarding preflight response for console and admin routes.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct DiscordOnboardingPreflightResponse {
    pub(crate) connector_id: String,
    pub(crate) account_id: String,
    pub(crate) mode: DiscordOnboardingMode,
    pub(crate) inbound_scope: DiscordOnboardingScope,
    pub(crate) bot: DiscordBotIdentitySummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) application: Option<DiscordApplicationSummary>,
    pub(crate) invite_url_template: String,
    pub(crate) required_permissions: Vec<String>,
    pub(crate) egress_allowlist: Vec<String>,
    pub(crate) security_defaults: Vec<String>,
    pub(crate) routing_preview: DiscordRoutingPreview,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) channel_permission_check: Option<DiscordChannelPermissionCheck>,
    pub(crate) inbound_monitor: DiscordInboundMonitorSummary,
    pub(crate) inbound_alive: bool,
    pub(crate) warnings: Vec<String>,
    pub(crate) policy_warnings: Vec<String>,
}
