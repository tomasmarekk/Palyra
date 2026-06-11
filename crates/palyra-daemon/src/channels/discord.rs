//! Discord-specific channel-platform behavior.
//!
//! Wraps the shared Discord provider helpers with daemon error types, adds
//! the confirmed operator test-send flow, and classifies message-mutation
//! risk (edit/delete/reaction) into approval requirements based on channel
//! visibility, connector scope, and message age.

use palyra_connectors::{
    providers::discord as shared, ConnectorInstanceRecord, ConnectorInstanceSpec, ConnectorKind,
    ConnectorLiveness, ConnectorMessageRecord, ConnectorReadiness, ConnectorStatusSnapshot,
    OutboundMessageRequest,
};
use ulid::Ulid;

use crate::journal::ApprovalRiskLevel;

use super::{ChannelPlatform, ChannelPlatformError};

pub use shared::{
    canonical_discord_channel_identity, canonical_discord_sender_identity, discord_connector_id,
    discord_default_egress_allowlist, discord_principal, discord_token_vault_ref,
    is_discord_connector,
};

/// Normalizes a Discord account id to the supported charset.
///
/// # Errors
/// Returns [`ChannelPlatformError::InvalidInput`] for empty ids or
/// unsupported characters.
#[allow(clippy::result_large_err)]
pub fn normalize_discord_account_id(raw: &str) -> Result<String, ChannelPlatformError> {
    shared::normalize_discord_account_id(raw)
        .map_err(|error| ChannelPlatformError::InvalidInput(error.to_string()))
}

/// Builds the connector spec for a Discord account.
///
/// # Panics
/// Panics when `account_id` was not produced by
/// [`normalize_discord_account_id`]; callers validate first.
pub(super) fn discord_connector_spec(account_id: &str, enabled: bool) -> ConnectorInstanceSpec {
    shared::discord_connector_spec(account_id, enabled)
        .expect("daemon should only construct Discord connector specs from validated account ids")
}

/// Normalizes a Discord send target (`channel:<id>` or raw id) to the
/// plain target id.
///
/// # Errors
/// Returns [`ChannelPlatformError::InvalidInput`] for empty or unsupported
/// targets.
#[allow(clippy::result_large_err)]
pub(super) fn normalize_discord_target(raw: &str) -> Result<String, ChannelPlatformError> {
    shared::normalize_discord_target(raw)
        .map_err(|error| ChannelPlatformError::InvalidInput(error.to_string()))
}

/// Operator request for a real outbound Discord test message; `confirm`
/// must be set explicitly because the send leaves the local runtime.
#[derive(Debug, Clone)]
pub struct ChannelDiscordTestSendRequest {
    pub target: String,
    pub text: String,
    pub confirm: bool,
    pub auto_reaction: Option<String>,
    pub thread_id: Option<String>,
    pub reply_to_message_id: Option<String>,
}

/// Delivery report of a Discord test send: enqueue/drain counters plus the
/// provider-native message id when delivery could be confirmed.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ChannelDiscordTestSendOutcome {
    pub envelope_id: String,
    pub connector_id: String,
    pub target: String,
    pub enqueued: bool,
    pub delivered: usize,
    pub retried: usize,
    pub dead_lettered: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub native_message_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to_message_id: Option<String>,
}

impl ChannelPlatform {
    /// Enqueues and immediately drains one confirmed outbound Discord test
    /// message, reporting delivery counters and the native message id.
    ///
    /// # Errors
    /// Returns [`ChannelPlatformError::InvalidInput`] for missing
    /// confirmation, empty fields, or non-Discord connectors;
    /// [`ChannelPlatformError::Precondition`] when the connector is not
    /// enabled, ready, and running; and supervisor errors from enqueue or
    /// drain.
    pub async fn submit_discord_test_send(
        &self,
        connector_id: &str,
        request: ChannelDiscordTestSendRequest,
    ) -> Result<ChannelDiscordTestSendOutcome, ChannelPlatformError> {
        let connector_id = connector_id.trim();
        if connector_id.is_empty() {
            return Err(ChannelPlatformError::InvalidInput(
                "connector_id cannot be empty".to_owned(),
            ));
        }
        if !request.confirm {
            return Err(ChannelPlatformError::InvalidInput(
                "discord test send requires explicit confirmation".to_owned(),
            ));
        }
        let status = self.status(connector_id)?;
        if status.kind != ConnectorKind::Discord {
            return Err(ChannelPlatformError::InvalidInput(format!(
                "discord test send is only supported for discord connectors (received kind={})",
                status.kind.as_str()
            )));
        }
        ensure_discord_test_send_dispatchable(&status)?;

        let text = request.text.trim();
        if text.is_empty() {
            return Err(ChannelPlatformError::InvalidInput(
                "test-send text cannot be empty".to_owned(),
            ));
        }
        let target = normalize_discord_target(request.target.as_str())?;
        let thread_id = request
            .thread_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
        let auto_reaction = request
            .auto_reaction
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
        let reply_to_message_id = request
            .reply_to_message_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);

        let outbound = OutboundMessageRequest {
            envelope_id: Ulid::new().to_string(),
            connector_id: connector_id.to_owned(),
            conversation_id: target.clone(),
            reply_thread_id: thread_id,
            in_reply_to_message_id: reply_to_message_id,
            text: text.to_owned(),
            broadcast: false,
            auto_ack_text: None,
            auto_reaction,
            attachments: Vec::new(),
            structured_json: None,
            a2ui_update: None,
            timeout_ms: 30_000,
            max_payload_bytes: self.supervisor_config().max_outbound_body_bytes,
        };
        let enqueue = self.supervisor.enqueue_outbound(&outbound)?;
        let drain = self
            .supervisor
            .drain_due_outbox_for_connector(
                connector_id,
                self.supervisor_config().immediate_drain_batch_size,
            )
            .await?;
        let native_message_id = (drain.delivered > 0)
            .then(|| self.find_native_message_id(connector_id, outbound.envelope_id.as_str()))
            .transpose()?
            .flatten();
        Ok(ChannelDiscordTestSendOutcome {
            envelope_id: outbound.envelope_id,
            connector_id: connector_id.to_owned(),
            target,
            enqueued: enqueue.created,
            delivered: drain.delivered,
            retried: drain.retried,
            dead_lettered: drain.dead_lettered,
            native_message_id,
            thread_id: outbound.reply_thread_id,
            in_reply_to_message_id: outbound.in_reply_to_message_id,
        })
    }
}

/// Rejects test sends up front (before enqueueing) unless the connector is
/// enabled, ready, and running, listing every failed precondition.
fn ensure_discord_test_send_dispatchable(
    status: &ConnectorStatusSnapshot,
) -> Result<(), ChannelPlatformError> {
    if status.enabled
        && status.readiness == ConnectorReadiness::Ready
        && status.liveness == ConnectorLiveness::Running
    {
        return Ok(());
    }

    let mut reasons = Vec::new();
    if !status.enabled {
        reasons.push("disabled".to_owned());
    }
    if status.readiness != ConnectorReadiness::Ready {
        reasons.push(format!("readiness={}", status.readiness.as_str()));
    }
    if status.liveness != ConnectorLiveness::Running {
        reasons.push(format!("liveness={}", status.liveness.as_str()));
    }
    Err(ChannelPlatformError::Precondition(format!(
        "connector '{}' cannot send outbound messages because {}; enable and configure the connector before retrying",
        status.connector_id,
        reasons.join(", ")
    )))
}

const LOW_RISK_EDIT_WINDOW_MS: i64 = 15 * 60 * 1_000;
const LOW_RISK_REACTION_WINDOW_MS: i64 = 6 * 60 * 60 * 1_000;
const HIGH_RISK_MUTATION_WINDOW_MS: i64 = 24 * 60 * 60 * 1_000;

/// Kind of Discord message mutation being risk-classified.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DiscordMessageMutationKind {
    Edit,
    Delete,
    ReactAdd,
    ReactRemove,
}

impl DiscordMessageMutationKind {
    /// Returns the snake_case operation name used in governance reasons.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Edit => "edit",
            Self::Delete => "delete",
            Self::ReactAdd => "react_add",
            Self::ReactRemove => "react_remove",
        }
    }
}

/// Risk posture of one mutation: level, whether an approval gate is
/// required, and an auditable reason string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DiscordMessageMutationGovernance {
    pub risk_level: ApprovalRiskLevel,
    pub approval_required: bool,
    pub reason: String,
}

/// Classifies a Discord message mutation into risk level and approval
/// requirement.
///
/// Risk escalates with exposure: mutations on the shared default
/// connector profile or in public (guild) channels, mutations of messages
/// the connector did not author, and older messages all push toward
/// approval. Only fresh, connector-authored activity in DMs on a scoped
/// connector profile can skip approval.
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
        providers::provider_capabilities, ConnectorAvailability, ConnectorKind, ConnectorLiveness,
        ConnectorMessageLocator, ConnectorQueueDepth, ConnectorReadiness, ConnectorStatusSnapshot,
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

    fn sample_status(
        enabled: bool,
        readiness: ConnectorReadiness,
        liveness: ConnectorLiveness,
    ) -> ConnectorStatusSnapshot {
        ConnectorStatusSnapshot {
            connector_id: "discord:default".to_owned(),
            kind: ConnectorKind::Discord,
            availability: ConnectorAvailability::Supported,
            capabilities: provider_capabilities(ConnectorKind::Discord),
            principal: "channel:discord:default".to_owned(),
            enabled,
            readiness,
            liveness,
            restart_count: 0,
            queue_depth: ConnectorQueueDepth { pending_outbox: 0, dead_letters: 0 },
            last_error: None,
            last_inbound_unix_ms: None,
            last_outbound_unix_ms: None,
            updated_at_unix_ms: 1,
        }
    }

    #[test]
    fn discord_test_send_requires_dispatchable_connector_state() {
        ensure_discord_test_send_dispatchable(&sample_status(
            true,
            ConnectorReadiness::Ready,
            ConnectorLiveness::Running,
        ))
        .expect("ready running connector should accept test sends");

        let error = ensure_discord_test_send_dispatchable(&sample_status(
            false,
            ConnectorReadiness::MissingCredential,
            ConnectorLiveness::Stopped,
        ))
        .expect_err("disabled connector should reject test sends before enqueue");

        let message = error.to_string();
        assert!(message.contains("cannot send outbound messages"));
        assert!(message.contains("disabled"));
        assert!(message.contains("readiness=missing_credential"));
        assert!(message.contains("liveness=stopped"));
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
