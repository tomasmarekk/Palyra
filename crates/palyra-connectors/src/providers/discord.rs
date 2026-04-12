use std::sync::Arc;

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;

use crate::core::{
    ConnectorAdapter, ConnectorAdapterError, ConnectorApprovalMode, ConnectorAvailability,
    ConnectorCapabilitySet, ConnectorCapabilitySupport, ConnectorInstanceRecord,
    ConnectorInstanceSpec, ConnectorKind, ConnectorMessageDeleteRequest,
    ConnectorMessageEditRequest, ConnectorMessageMutationResult, ConnectorMessageReactionRequest,
    ConnectorMessageReadRequest, ConnectorMessageReadResult, ConnectorMessageSearchRequest,
    ConnectorMessageSearchResult, ConnectorOperationPreflight, ConnectorRiskLevel, DeliveryOutcome,
    InboundMessageEvent, OutboundMessageRequest,
};
use palyra_connector_discord::ConnectorAdapter as LegacyConnectorAdapter;

pub use palyra_connector_discord::{
    canonical_discord_channel_identity, canonical_discord_sender_identity,
    discord_audit_event_type_for_operation, discord_auth_profile_ref, discord_connector_id,
    discord_default_egress_allowlist, discord_min_invite_permissions,
    discord_permission_labels_for_operation, discord_permissions_for_operation,
    discord_policy_action_for_operation, discord_principal, discord_required_permission_labels,
    discord_required_permissions, discord_token_vault_ref, is_discord_connector,
    normalize_discord_account_id, normalize_discord_target, resolve_discord_intents_from_flags,
    DiscordAdapterConfig, DiscordCredential, DiscordCredentialResolver, DiscordMessageOperation,
    DiscordPrivilegedIntentStatus, DiscordPrivilegedIntentsSummary, DiscordSemanticsError,
    EnvDiscordCredentialResolver, DISCORD_APP_FLAG_GATEWAY_GUILD_MEMBERS,
    DISCORD_APP_FLAG_GATEWAY_GUILD_MEMBERS_LIMITED, DISCORD_APP_FLAG_GATEWAY_MESSAGE_CONTENT,
    DISCORD_APP_FLAG_GATEWAY_MESSAGE_CONTENT_LIMITED, DISCORD_APP_FLAG_GATEWAY_PRESENCE,
    DISCORD_APP_FLAG_GATEWAY_PRESENCE_LIMITED, DISCORD_PERMISSION_ADD_REACTIONS,
    DISCORD_PERMISSION_ATTACH_FILES, DISCORD_PERMISSION_EMBED_LINKS,
    DISCORD_PERMISSION_MANAGE_MESSAGES, DISCORD_PERMISSION_READ_MESSAGE_HISTORY,
    DISCORD_PERMISSION_SEND_MESSAGES, DISCORD_PERMISSION_SEND_MESSAGES_IN_THREADS,
    DISCORD_PERMISSION_VIEW_CHANNEL,
};

#[derive(Debug)]
pub struct DiscordConnectorAdapter {
    inner: palyra_connector_discord::DiscordConnectorAdapter,
}

impl Default for DiscordConnectorAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl DiscordConnectorAdapter {
    #[must_use]
    pub fn new() -> Self {
        Self { inner: palyra_connector_discord::DiscordConnectorAdapter::new() }
    }

    #[must_use]
    pub fn with_credential_resolver(
        credential_resolver: Arc<dyn DiscordCredentialResolver>,
    ) -> Self {
        Self {
            inner: palyra_connector_discord::DiscordConnectorAdapter::with_credential_resolver(
                credential_resolver,
            ),
        }
    }
}

#[async_trait]
impl ConnectorAdapter for DiscordConnectorAdapter {
    fn kind(&self) -> ConnectorKind {
        ConnectorKind::Discord
    }

    fn availability(&self) -> ConnectorAvailability {
        ConnectorAvailability::Supported
    }

    fn capabilities(&self) -> ConnectorCapabilitySet {
        convert_runtime_value(LegacyConnectorAdapter::capabilities(&self.inner))
            .expect("discord capability schema should stay aligned between connector crates")
    }

    fn split_outbound(
        &self,
        instance: &ConnectorInstanceRecord,
        request: &OutboundMessageRequest,
    ) -> Result<Vec<OutboundMessageRequest>, ConnectorAdapterError> {
        let legacy_instance = to_legacy_instance(instance);
        let legacy_request = convert_runtime_value(request.clone())?;
        let legacy_parts =
            LegacyConnectorAdapter::split_outbound(&self.inner, &legacy_instance, &legacy_request)
                .map_err(map_legacy_adapter_error)?;
        convert_runtime_value(legacy_parts)
    }

    fn runtime_snapshot(&self, instance: &ConnectorInstanceRecord) -> Option<Value> {
        let legacy_instance = to_legacy_instance(instance);
        LegacyConnectorAdapter::runtime_snapshot(&self.inner, &legacy_instance)
    }

    async fn poll_inbound(
        &self,
        instance: &ConnectorInstanceRecord,
        limit: usize,
    ) -> Result<Vec<InboundMessageEvent>, ConnectorAdapterError> {
        let legacy_instance = to_legacy_instance(instance);
        let events = LegacyConnectorAdapter::poll_inbound(&self.inner, &legacy_instance, limit)
            .await
            .map_err(map_legacy_adapter_error)?;
        convert_runtime_value(events)
    }

    async fn send_outbound(
        &self,
        instance: &ConnectorInstanceRecord,
        request: &OutboundMessageRequest,
    ) -> Result<DeliveryOutcome, ConnectorAdapterError> {
        let legacy_instance = to_legacy_instance(instance);
        let legacy_request = convert_runtime_value(request.clone())?;
        let outcome =
            LegacyConnectorAdapter::send_outbound(&self.inner, &legacy_instance, &legacy_request)
                .await
                .map_err(map_legacy_adapter_error)?;
        convert_runtime_value(outcome)
    }

    async fn read_messages(
        &self,
        instance: &ConnectorInstanceRecord,
        request: &ConnectorMessageReadRequest,
    ) -> Result<ConnectorMessageReadResult, ConnectorAdapterError> {
        let legacy_instance = to_legacy_instance(instance);
        let legacy_request = convert_runtime_value(request.clone())?;
        let result =
            LegacyConnectorAdapter::read_messages(&self.inner, &legacy_instance, &legacy_request)
                .await
                .map_err(map_legacy_adapter_error)?;
        convert_runtime_value(result)
    }

    async fn search_messages(
        &self,
        instance: &ConnectorInstanceRecord,
        request: &ConnectorMessageSearchRequest,
    ) -> Result<ConnectorMessageSearchResult, ConnectorAdapterError> {
        let legacy_instance = to_legacy_instance(instance);
        let legacy_request = convert_runtime_value(request.clone())?;
        let result =
            LegacyConnectorAdapter::search_messages(&self.inner, &legacy_instance, &legacy_request)
                .await
                .map_err(map_legacy_adapter_error)?;
        convert_runtime_value(result)
    }

    async fn edit_message(
        &self,
        instance: &ConnectorInstanceRecord,
        request: &ConnectorMessageEditRequest,
    ) -> Result<ConnectorMessageMutationResult, ConnectorAdapterError> {
        let legacy_instance = to_legacy_instance(instance);
        let legacy_request = convert_runtime_value(request.clone())?;
        let result =
            LegacyConnectorAdapter::edit_message(&self.inner, &legacy_instance, &legacy_request)
                .await
                .map_err(map_legacy_adapter_error)?;
        convert_runtime_value(result)
    }

    async fn delete_message(
        &self,
        instance: &ConnectorInstanceRecord,
        request: &ConnectorMessageDeleteRequest,
    ) -> Result<ConnectorMessageMutationResult, ConnectorAdapterError> {
        let legacy_instance = to_legacy_instance(instance);
        let legacy_request = convert_runtime_value(request.clone())?;
        let result =
            LegacyConnectorAdapter::delete_message(&self.inner, &legacy_instance, &legacy_request)
                .await
                .map_err(map_legacy_adapter_error)?;
        convert_runtime_value(result)
    }

    async fn add_reaction(
        &self,
        instance: &ConnectorInstanceRecord,
        request: &ConnectorMessageReactionRequest,
    ) -> Result<ConnectorMessageMutationResult, ConnectorAdapterError> {
        let legacy_instance = to_legacy_instance(instance);
        let legacy_request = convert_runtime_value(request.clone())?;
        let result =
            LegacyConnectorAdapter::add_reaction(&self.inner, &legacy_instance, &legacy_request)
                .await
                .map_err(map_legacy_adapter_error)?;
        convert_runtime_value(result)
    }

    async fn remove_reaction(
        &self,
        instance: &ConnectorInstanceRecord,
        request: &ConnectorMessageReactionRequest,
    ) -> Result<ConnectorMessageMutationResult, ConnectorAdapterError> {
        let legacy_instance = to_legacy_instance(instance);
        let legacy_request = convert_runtime_value(request.clone())?;
        let result =
            LegacyConnectorAdapter::remove_reaction(&self.inner, &legacy_instance, &legacy_request)
                .await
                .map_err(map_legacy_adapter_error)?;
        convert_runtime_value(result)
    }
}

pub fn discord_connector_spec(
    account_id: &str,
    enabled: bool,
) -> Result<ConnectorInstanceSpec, DiscordSemanticsError> {
    let normalized = normalize_discord_account_id(account_id)?;
    Ok(ConnectorInstanceSpec {
        connector_id: discord_connector_id(normalized.as_str()),
        kind: ConnectorKind::Discord,
        principal: discord_principal(normalized.as_str()),
        auth_profile_ref: Some(discord_auth_profile_ref(normalized.as_str())),
        token_vault_ref: Some(discord_token_vault_ref(normalized.as_str())),
        egress_allowlist: discord_default_egress_allowlist(),
        enabled,
    })
}

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

fn to_legacy_instance(
    instance: &ConnectorInstanceRecord,
) -> palyra_connector_discord::ConnectorInstanceRecord {
    palyra_connector_discord::ConnectorInstanceRecord {
        connector_id: instance.connector_id.clone(),
        kind: palyra_connector_discord::ConnectorKind::Discord,
        principal: instance.principal.clone(),
        auth_profile_ref: instance.auth_profile_ref.clone(),
        token_vault_ref: instance.token_vault_ref.clone(),
        egress_allowlist: instance.egress_allowlist.clone(),
        enabled: instance.enabled,
        readiness: match instance.readiness {
            crate::ConnectorReadiness::Ready => palyra_connector_discord::ConnectorReadiness::Ready,
            crate::ConnectorReadiness::MissingCredential => {
                palyra_connector_discord::ConnectorReadiness::MissingCredential
            }
            crate::ConnectorReadiness::AuthFailed => {
                palyra_connector_discord::ConnectorReadiness::AuthFailed
            }
            crate::ConnectorReadiness::Misconfigured => {
                palyra_connector_discord::ConnectorReadiness::Misconfigured
            }
        },
        liveness: match instance.liveness {
            crate::ConnectorLiveness::Stopped => {
                palyra_connector_discord::ConnectorLiveness::Stopped
            }
            crate::ConnectorLiveness::Running => {
                palyra_connector_discord::ConnectorLiveness::Running
            }
            crate::ConnectorLiveness::Restarting => {
                palyra_connector_discord::ConnectorLiveness::Restarting
            }
            crate::ConnectorLiveness::Crashed => {
                palyra_connector_discord::ConnectorLiveness::Crashed
            }
        },
        restart_count: instance.restart_count,
        last_error: instance.last_error.clone(),
        last_inbound_unix_ms: instance.last_inbound_unix_ms,
        last_outbound_unix_ms: instance.last_outbound_unix_ms,
        created_at_unix_ms: instance.created_at_unix_ms,
        updated_at_unix_ms: instance.updated_at_unix_ms,
    }
}

fn convert_runtime_value<Src, Dst>(value: Src) -> Result<Dst, ConnectorAdapterError>
where
    Src: Serialize,
    Dst: DeserializeOwned,
{
    let serialized = serde_json::to_value(value).map_err(|error| {
        ConnectorAdapterError::Backend(format!(
            "discord adapter bridge serialization failed: {error}"
        ))
    })?;
    serde_json::from_value(serialized).map_err(|error| {
        ConnectorAdapterError::Backend(format!("discord adapter bridge conversion failed: {error}"))
    })
}

fn map_legacy_adapter_error(
    error: palyra_connector_discord::ConnectorAdapterError,
) -> ConnectorAdapterError {
    ConnectorAdapterError::Backend(error.to_string())
}
