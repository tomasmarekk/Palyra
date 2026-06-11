//! Discord permission, intent, and policy mappings for message operations.
//!
//! Single source of truth tying each [`DiscordMessageOperation`] to its required Discord
//! permission bits, policy action, approval mode, risk level, and audit event type, so the
//! adapter, daemon policy layer, and console all agree on operation gating.

use serde::Serialize;

use crate::core::{
    ConnectorApprovalMode, ConnectorCapabilitySupport, ConnectorOperationPreflight,
    ConnectorRiskLevel,
};

// Application flag bits reported by Discord's application metadata. Each privileged intent
// has a fully `enabled` bit and a `_LIMITED` bit granted to unverified apps below the guild
// threshold; `resolve_discord_intents_from_flags` collapses the pair into one status.

/// Application flag: presence intent fully enabled.
pub const DISCORD_APP_FLAG_GATEWAY_PRESENCE: u64 = 1 << 12;
/// Application flag: presence intent enabled in limited (unverified) mode.
pub const DISCORD_APP_FLAG_GATEWAY_PRESENCE_LIMITED: u64 = 1 << 13;
/// Application flag: guild members intent fully enabled.
pub const DISCORD_APP_FLAG_GATEWAY_GUILD_MEMBERS: u64 = 1 << 14;
/// Application flag: guild members intent enabled in limited (unverified) mode.
pub const DISCORD_APP_FLAG_GATEWAY_GUILD_MEMBERS_LIMITED: u64 = 1 << 15;
/// Application flag: message content intent fully enabled.
pub const DISCORD_APP_FLAG_GATEWAY_MESSAGE_CONTENT: u64 = 1 << 18;
/// Application flag: message content intent enabled in limited (unverified) mode.
pub const DISCORD_APP_FLAG_GATEWAY_MESSAGE_CONTENT_LIMITED: u64 = 1 << 19;

// Channel permission bits as defined by the Discord permissions bitfield.

/// Permission bit for `View Channels`.
pub const DISCORD_PERMISSION_VIEW_CHANNEL: u64 = 1 << 10;
/// Permission bit for `Add Reactions`.
pub const DISCORD_PERMISSION_ADD_REACTIONS: u64 = 1 << 6;
/// Permission bit for `Send Messages`.
pub const DISCORD_PERMISSION_SEND_MESSAGES: u64 = 1 << 11;
/// Permission bit for `Manage Messages`.
pub const DISCORD_PERMISSION_MANAGE_MESSAGES: u64 = 1 << 13;
/// Permission bit for `Embed Links`.
pub const DISCORD_PERMISSION_EMBED_LINKS: u64 = 1 << 14;
/// Permission bit for `Attach Files`.
pub const DISCORD_PERMISSION_ATTACH_FILES: u64 = 1 << 15;
/// Permission bit for `Read Message History`.
pub const DISCORD_PERMISSION_READ_MESSAGE_HISTORY: u64 = 1 << 16;
/// Permission bit for `Send Messages in Threads`.
pub const DISCORD_PERMISSION_SEND_MESSAGES_IN_THREADS: u64 = 1 << 38;

/// Message-level operation the Discord adapter can perform on behalf of the runtime.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DiscordMessageOperation {
    Send,
    Thread,
    Reply,
    Read,
    Search,
    Edit,
    Delete,
    ReactAdd,
    ReactRemove,
}

/// Effective status of one privileged gateway intent for the configured application.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DiscordPrivilegedIntentStatus {
    Enabled,
    Limited,
    Disabled,
}

/// Per-intent status summary derived from the application's flag bitfield.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DiscordPrivilegedIntentsSummary {
    pub message_content: DiscordPrivilegedIntentStatus,
    pub guild_members: DiscordPrivilegedIntentStatus,
    pub presence: DiscordPrivilegedIntentStatus,
}

/// Resolves the privileged-intent summary from a Discord application flag bitfield.
///
/// The fully enabled bit wins over the limited bit when both are set.
#[must_use]
pub fn resolve_discord_intents_from_flags(flags: u64) -> DiscordPrivilegedIntentsSummary {
    let resolve = |enabled_bit: u64, limited_bit: u64| {
        if (flags & enabled_bit) != 0 {
            DiscordPrivilegedIntentStatus::Enabled
        } else if (flags & limited_bit) != 0 {
            DiscordPrivilegedIntentStatus::Limited
        } else {
            DiscordPrivilegedIntentStatus::Disabled
        }
    };
    DiscordPrivilegedIntentsSummary {
        presence: resolve(
            DISCORD_APP_FLAG_GATEWAY_PRESENCE,
            DISCORD_APP_FLAG_GATEWAY_PRESENCE_LIMITED,
        ),
        guild_members: resolve(
            DISCORD_APP_FLAG_GATEWAY_GUILD_MEMBERS,
            DISCORD_APP_FLAG_GATEWAY_GUILD_MEMBERS_LIMITED,
        ),
        message_content: resolve(
            DISCORD_APP_FLAG_GATEWAY_MESSAGE_CONTENT,
            DISCORD_APP_FLAG_GATEWAY_MESSAGE_CONTENT_LIMITED,
        ),
    }
}

/// Returns the label/bit pairs every Discord connector installation must be granted.
#[must_use]
pub fn discord_required_permissions() -> [(&'static str, u64); 6] {
    [
        ("View Channels", DISCORD_PERMISSION_VIEW_CHANNEL),
        ("Send Messages", DISCORD_PERMISSION_SEND_MESSAGES),
        ("Read Message History", DISCORD_PERMISSION_READ_MESSAGE_HISTORY),
        ("Embed Links", DISCORD_PERMISSION_EMBED_LINKS),
        ("Attach Files", DISCORD_PERMISSION_ATTACH_FILES),
        ("Send Messages in Threads", DISCORD_PERMISSION_SEND_MESSAGES_IN_THREADS),
    ]
}

/// Returns the human-readable labels of the baseline required permissions.
#[must_use]
pub fn discord_required_permission_labels() -> Vec<String> {
    discord_required_permissions().iter().map(|(name, _)| (*name).to_owned()).collect()
}

/// Returns the combined permission mask to embed in bot invite URLs.
#[must_use]
pub fn discord_min_invite_permissions() -> u64 {
    discord_required_permissions().iter().fold(0_u64, |mask, (_, bit)| mask | *bit)
}

/// Returns the label/bit pairs the bot needs in the target channel for `operation`.
#[must_use]
pub fn discord_permissions_for_operation(
    operation: DiscordMessageOperation,
) -> &'static [(&'static str, u64)] {
    match operation {
        DiscordMessageOperation::Send
        | DiscordMessageOperation::Thread
        | DiscordMessageOperation::Reply => &[
            ("View Channels", DISCORD_PERMISSION_VIEW_CHANNEL),
            ("Send Messages", DISCORD_PERMISSION_SEND_MESSAGES),
            ("Read Message History", DISCORD_PERMISSION_READ_MESSAGE_HISTORY),
        ],
        DiscordMessageOperation::Read | DiscordMessageOperation::Search => &[
            ("View Channels", DISCORD_PERMISSION_VIEW_CHANNEL),
            ("Read Message History", DISCORD_PERMISSION_READ_MESSAGE_HISTORY),
        ],
        DiscordMessageOperation::Edit => &[
            ("View Channels", DISCORD_PERMISSION_VIEW_CHANNEL),
            ("Read Message History", DISCORD_PERMISSION_READ_MESSAGE_HISTORY),
            ("Send Messages", DISCORD_PERMISSION_SEND_MESSAGES),
        ],
        DiscordMessageOperation::Delete => &[
            ("View Channels", DISCORD_PERMISSION_VIEW_CHANNEL),
            ("Read Message History", DISCORD_PERMISSION_READ_MESSAGE_HISTORY),
            ("Manage Messages", DISCORD_PERMISSION_MANAGE_MESSAGES),
        ],
        DiscordMessageOperation::ReactAdd => &[
            ("View Channels", DISCORD_PERMISSION_VIEW_CHANNEL),
            ("Read Message History", DISCORD_PERMISSION_READ_MESSAGE_HISTORY),
            ("Add Reactions", DISCORD_PERMISSION_ADD_REACTIONS),
        ],
        // Removing the bot's own reaction needs no Add Reactions grant on Discord's side.
        DiscordMessageOperation::ReactRemove => &[
            ("View Channels", DISCORD_PERMISSION_VIEW_CHANNEL),
            ("Read Message History", DISCORD_PERMISSION_READ_MESSAGE_HISTORY),
        ],
    }
}

/// Returns the human-readable permission labels required for `operation`.
#[must_use]
pub fn discord_permission_labels_for_operation(operation: DiscordMessageOperation) -> Vec<String> {
    discord_permissions_for_operation(operation)
        .iter()
        .map(|(name, _)| (*name).to_owned())
        .collect()
}

/// Returns the policy-engine action string evaluated before `operation` runs.
#[must_use]
pub const fn discord_policy_action_for_operation(
    operation: DiscordMessageOperation,
) -> &'static str {
    match operation {
        DiscordMessageOperation::Send => "channel.send",
        DiscordMessageOperation::Thread => "message.thread",
        DiscordMessageOperation::Reply => "message.reply",
        DiscordMessageOperation::Read => "channel.message.read",
        DiscordMessageOperation::Search => "channel.message.search",
        DiscordMessageOperation::Edit => "channel.message.edit",
        DiscordMessageOperation::Delete => "channel.message.delete",
        DiscordMessageOperation::ReactAdd => "channel.message.react_add",
        DiscordMessageOperation::ReactRemove => "channel.message.react_remove",
    }
}

/// Returns the audit-journal event type recorded for `operation`.
#[must_use]
pub const fn discord_audit_event_type_for_operation(
    operation: DiscordMessageOperation,
) -> &'static str {
    match operation {
        DiscordMessageOperation::Send => "message.send",
        DiscordMessageOperation::Thread => "message.thread",
        DiscordMessageOperation::Reply => "message.reply",
        DiscordMessageOperation::Read => "message.read",
        DiscordMessageOperation::Search => "message.search",
        DiscordMessageOperation::Edit => "message.edit",
        DiscordMessageOperation::Delete => "message.delete",
        DiscordMessageOperation::ReactAdd => "message.react_add",
        DiscordMessageOperation::ReactRemove => "message.react_remove",
    }
}

/// Returns the default approval mode for `operation` (mutations are conditional).
#[must_use]
pub const fn discord_approval_mode_for_operation(
    operation: DiscordMessageOperation,
) -> ConnectorApprovalMode {
    match operation {
        DiscordMessageOperation::Send
        | DiscordMessageOperation::Thread
        | DiscordMessageOperation::Reply
        | DiscordMessageOperation::Read
        | DiscordMessageOperation::Search => ConnectorApprovalMode::None,
        DiscordMessageOperation::Edit
        | DiscordMessageOperation::Delete
        | DiscordMessageOperation::ReactAdd
        | DiscordMessageOperation::ReactRemove => ConnectorApprovalMode::Conditional,
    }
}

/// Returns the default risk classification for `operation`.
#[must_use]
pub const fn discord_risk_level_for_operation(
    operation: DiscordMessageOperation,
) -> ConnectorRiskLevel {
    match operation {
        DiscordMessageOperation::Read | DiscordMessageOperation::Search => ConnectorRiskLevel::Low,
        DiscordMessageOperation::Send
        | DiscordMessageOperation::Thread
        | DiscordMessageOperation::Reply => ConnectorRiskLevel::Medium,
        DiscordMessageOperation::Edit
        | DiscordMessageOperation::Delete
        | DiscordMessageOperation::ReactAdd
        | DiscordMessageOperation::ReactRemove => ConnectorRiskLevel::Conditional,
    }
}

/// Builds the capability-support record for `operation`, annotated with the operation's
/// policy action, approval mode, risk level, audit event type, and required permissions.
#[must_use]
pub fn discord_capability_support(
    operation: DiscordMessageOperation,
    supported: bool,
    reason: Option<&str>,
) -> ConnectorCapabilitySupport {
    let base = if supported {
        ConnectorCapabilitySupport::supported()
    } else {
        ConnectorCapabilitySupport::unsupported(reason.unwrap_or("unsupported"))
    };
    base.with_policy_action(discord_policy_action_for_operation(operation))
        .with_approval_mode(discord_approval_mode_for_operation(operation))
        .with_risk_level(discord_risk_level_for_operation(operation))
        .with_audit_event_type(discord_audit_event_type_for_operation(operation))
        .with_required_permissions(discord_permission_labels_for_operation(operation))
}

/// Builds a preflight verdict for `operation`, falling back to the operation's default risk
/// level and approval mode when no override is supplied.
#[must_use]
pub fn discord_operation_preflight(
    operation: DiscordMessageOperation,
    allowed: bool,
    reason: Option<String>,
    risk_level: Option<ConnectorRiskLevel>,
    approval_mode: Option<ConnectorApprovalMode>,
) -> ConnectorOperationPreflight {
    ConnectorOperationPreflight {
        allowed,
        policy_action: discord_policy_action_for_operation(operation).to_owned(),
        approval_mode: approval_mode
            .unwrap_or_else(|| discord_approval_mode_for_operation(operation)),
        risk_level: risk_level.unwrap_or_else(|| discord_risk_level_for_operation(operation)),
        audit_event_type: discord_audit_event_type_for_operation(operation).to_owned(),
        required_permissions: discord_permission_labels_for_operation(operation),
        reason,
    }
}
