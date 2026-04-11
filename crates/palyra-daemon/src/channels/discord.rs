use palyra_connectors::{
    providers::discord as shared, ConnectorInstanceRecord, ConnectorInstanceSpec,
    ConnectorMessageRecord,
};

use crate::journal::ApprovalRiskLevel;

use super::ChannelPlatformError;

pub use shared::{
    canonical_discord_channel_identity, canonical_discord_sender_identity, discord_connector_id,
    discord_default_egress_allowlist, discord_principal, discord_token_vault_ref,
    is_discord_connector,
};

#[allow(clippy::result_large_err)]
pub fn normalize_discord_account_id(raw: &str) -> Result<String, ChannelPlatformError> {
    shared::normalize_discord_account_id(raw)
        .map_err(|error| ChannelPlatformError::InvalidInput(error.to_string()))
}

pub(super) fn discord_connector_spec(account_id: &str, enabled: bool) -> ConnectorInstanceSpec {
    shared::discord_connector_spec(account_id, enabled)
        .expect("daemon should only construct Discord connector specs from validated account ids")
}

#[allow(clippy::result_large_err)]
pub(super) fn normalize_discord_target(raw: &str) -> Result<String, ChannelPlatformError> {
    shared::normalize_discord_target(raw)
        .map_err(|error| ChannelPlatformError::InvalidInput(error.to_string()))
}

const LOW_RISK_EDIT_WINDOW_MS: i64 = 15 * 60 * 1_000;
const LOW_RISK_REACTION_WINDOW_MS: i64 = 6 * 60 * 60 * 1_000;
const HIGH_RISK_MUTATION_WINDOW_MS: i64 = 24 * 60 * 60 * 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DiscordMessageMutationKind {
    Edit,
    Delete,
    ReactAdd,
    ReactRemove,
}

impl DiscordMessageMutationKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Edit => "edit",
            Self::Delete => "delete",
            Self::ReactAdd => "react_add",
            Self::ReactRemove => "react_remove",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DiscordMessageMutationGovernance {
    pub risk_level: ApprovalRiskLevel,
    pub approval_required: bool,
    pub reason: String,
}

pub(crate) fn classify_discord_message_mutation_governance(
    instance: &ConnectorInstanceRecord,
    message: &ConnectorMessageRecord,
    operation: DiscordMessageMutationKind,
    now_unix_ms: i64,
) -> DiscordMessageMutationGovernance {
    let age_ms = now_unix_ms.saturating_sub(message.created_at_unix_ms).max(0);
    let default_connector_scope = instance.connector_id.eq_ignore_ascii_case("discord:default")
        || instance
            .auth_profile_ref
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .is_some_and(|value| value.eq_ignore_ascii_case("discord.default"));
    let public_channel = !message.is_direct_message;

    let (risk_level, approval_required) = match operation {
        DiscordMessageMutationKind::Edit => {
            if message.is_connector_authored
                && !default_connector_scope
                && !public_channel
                && age_ms <= LOW_RISK_EDIT_WINDOW_MS
            {
                (ApprovalRiskLevel::Low, false)
            } else if message.is_connector_authored && age_ms <= HIGH_RISK_MUTATION_WINDOW_MS {
                (ApprovalRiskLevel::High, public_channel || default_connector_scope)
            } else {
                (ApprovalRiskLevel::Critical, true)
            }
        }
        DiscordMessageMutationKind::Delete => {
            if message.is_connector_authored
                && !default_connector_scope
                && !public_channel
                && age_ms <= LOW_RISK_EDIT_WINDOW_MS
            {
                (ApprovalRiskLevel::Medium, true)
            } else if message.is_connector_authored && age_ms <= HIGH_RISK_MUTATION_WINDOW_MS {
                (ApprovalRiskLevel::High, true)
            } else {
                (ApprovalRiskLevel::Critical, true)
            }
        }
        DiscordMessageMutationKind::ReactAdd | DiscordMessageMutationKind::ReactRemove => {
            if !default_connector_scope && !public_channel && age_ms <= LOW_RISK_REACTION_WINDOW_MS
            {
                (ApprovalRiskLevel::Low, false)
            } else if age_ms <= HIGH_RISK_MUTATION_WINDOW_MS {
                (ApprovalRiskLevel::Medium, public_channel || default_connector_scope)
            } else {
                (ApprovalRiskLevel::High, true)
            }
        }
    };

    let channel_scope = if public_channel { "guild_or_shared_channel" } else { "direct_message" };
    let connector_scope = if default_connector_scope {
        "default_connector_profile"
    } else {
        "scoped_connector_profile"
    };
    let age_bucket = if age_ms <= LOW_RISK_EDIT_WINDOW_MS {
        "fresh"
    } else if age_ms <= HIGH_RISK_MUTATION_WINDOW_MS {
        "recent"
    } else {
        "stale"
    };
    DiscordMessageMutationGovernance {
        risk_level,
        approval_required,
        reason: format!(
            "discord mutation governance: operation={} channel_scope={} connector_scope={} age_bucket={} connector_authored={}",
            operation.as_str(),
            channel_scope,
            connector_scope,
            age_bucket,
            message.is_connector_authored
        ),
    }
}

#[cfg(test)]
mod tests {
    use palyra_connectors::{
        ConnectorKind, ConnectorLiveness, ConnectorMessageLocator, ConnectorReadiness,
    };

    use super::*;

    fn sample_instance(
        connector_id: &str,
        auth_profile_ref: Option<&str>,
    ) -> ConnectorInstanceRecord {
        ConnectorInstanceRecord {
            connector_id: connector_id.to_owned(),
            kind: ConnectorKind::Discord,
            principal: format!("channel:{connector_id}"),
            auth_profile_ref: auth_profile_ref.map(ToOwned::to_owned),
            token_vault_ref: Some("global/discord_bot_token".to_owned()),
            egress_allowlist: vec!["discord.com".to_owned()],
            enabled: true,
            readiness: ConnectorReadiness::Ready,
            liveness: ConnectorLiveness::Running,
            restart_count: 0,
            last_error: None,
            last_inbound_unix_ms: None,
            last_outbound_unix_ms: None,
            created_at_unix_ms: 1,
            updated_at_unix_ms: 1,
        }
    }

    fn sample_message(
        is_direct_message: bool,
        is_connector_authored: bool,
        created_at_unix_ms: i64,
    ) -> ConnectorMessageRecord {
        ConnectorMessageRecord {
            locator: ConnectorMessageLocator {
                target: palyra_connectors::ConnectorConversationTarget {
                    conversation_id: "123".to_owned(),
                    thread_id: None,
                },
                message_id: "456".to_owned(),
            },
            sender_id: "discord:user:1".to_owned(),
            sender_display: Some("ops".to_owned()),
            body: "hello".to_owned(),
            created_at_unix_ms,
            edited_at_unix_ms: None,
            is_direct_message,
            is_connector_authored,
            link: None,
            attachments: Vec::new(),
            reactions: Vec::new(),
        }
    }

    #[test]
    fn scoped_dm_edit_can_stay_low_risk() {
        let instance = sample_instance("discord:ops", Some("discord.ops"));
        let message = sample_message(true, true, 1_000);
        let posture = classify_discord_message_mutation_governance(
            &instance,
            &message,
            DiscordMessageMutationKind::Edit,
            1_000 + 60_000,
        );
        assert_eq!(posture.risk_level, ApprovalRiskLevel::Low);
        assert!(!posture.approval_required);
    }

    #[test]
    fn default_connector_public_delete_is_critical() {
        let instance = sample_instance("discord:default", Some("discord.default"));
        let message = sample_message(false, true, 1_000);
        let posture = classify_discord_message_mutation_governance(
            &instance,
            &message,
            DiscordMessageMutationKind::Delete,
            1_000 + HIGH_RISK_MUTATION_WINDOW_MS + 1,
        );
        assert_eq!(posture.risk_level, ApprovalRiskLevel::Critical);
        assert!(posture.approval_required);
    }

    #[test]
    fn fresh_dm_reaction_can_skip_approval() {
        let instance = sample_instance("discord:ops", Some("discord.ops"));
        let message = sample_message(true, false, 1_000);
        let posture = classify_discord_message_mutation_governance(
            &instance,
            &message,
            DiscordMessageMutationKind::ReactAdd,
            1_000 + 5 * 60 * 1_000,
        );
        assert_eq!(posture.risk_level, ApprovalRiskLevel::Low);
        assert!(!posture.approval_required);
    }
}
