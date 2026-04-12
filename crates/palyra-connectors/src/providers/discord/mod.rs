mod adapter;
mod defaults;
mod error;
mod ids;
mod normalize;
pub(crate) mod permissions;

pub use crate::core::net::{ConnectorNetGuard, ConnectorNetGuardError};
pub use crate::core::{
    net, protocol, storage, supervisor, AttachmentKind, AttachmentRef, ConnectorAdapter,
    ConnectorAdapterError, ConnectorApprovalMode, ConnectorAvailability,
    ConnectorConversationTarget, ConnectorEventRecord, ConnectorInstanceRecord,
    ConnectorInstanceSpec, ConnectorKind, ConnectorLiveness, ConnectorMessageDeleteRequest,
    ConnectorMessageEditRequest, ConnectorMessageLocator, ConnectorMessageMutationDiff,
    ConnectorMessageMutationResult, ConnectorMessageMutationStatus, ConnectorMessageReactionRecord,
    ConnectorMessageReactionRequest, ConnectorMessageReadRequest, ConnectorMessageReadResult,
    ConnectorMessageRecord, ConnectorMessageSearchRequest, ConnectorMessageSearchResult,
    ConnectorOperationPreflight, ConnectorQueueDepth, ConnectorQueueSnapshot, ConnectorReadiness,
    ConnectorRiskLevel, ConnectorRouter, ConnectorRouterError, ConnectorStatusSnapshot,
    ConnectorStore, ConnectorStoreError, ConnectorSupervisor, ConnectorSupervisorConfig,
    ConnectorSupervisorError, DeadLetterRecord, DeliveryOutcome, DrainOutcome,
    InboundIngestOutcome, InboundMessageEvent, OutboundA2uiUpdate, OutboundAttachment,
    OutboundMessageRequest, OutboxEnqueueOutcome, OutboxEntryRecord, RetryClass,
    RouteInboundResult, RoutedOutboundMessage,
};
pub use adapter::{
    DiscordAdapterConfig, DiscordConnectorAdapter, DiscordCredential, DiscordCredentialResolver,
    EnvDiscordCredentialResolver,
};
pub use defaults::{discord_connector_spec, discord_default_egress_allowlist};
pub use error::DiscordSemanticsError;
pub use ids::{
    discord_auth_profile_ref, discord_connector_id, discord_principal, discord_token_vault_ref,
    normalize_discord_account_id,
};
pub use normalize::{
    canonical_discord_channel_identity, canonical_discord_sender_identity, is_discord_connector,
    normalize_discord_target,
};
pub use permissions::{
    discord_approval_mode_for_operation, discord_audit_event_type_for_operation,
    discord_capability_support, discord_min_invite_permissions, discord_operation_preflight,
    discord_permission_labels_for_operation, discord_permissions_for_operation,
    discord_policy_action_for_operation, discord_required_permission_labels,
    discord_required_permissions, discord_risk_level_for_operation,
    resolve_discord_intents_from_flags, DiscordMessageOperation, DiscordPrivilegedIntentStatus,
    DiscordPrivilegedIntentsSummary, DISCORD_APP_FLAG_GATEWAY_GUILD_MEMBERS,
    DISCORD_APP_FLAG_GATEWAY_GUILD_MEMBERS_LIMITED, DISCORD_APP_FLAG_GATEWAY_MESSAGE_CONTENT,
    DISCORD_APP_FLAG_GATEWAY_MESSAGE_CONTENT_LIMITED, DISCORD_APP_FLAG_GATEWAY_PRESENCE,
    DISCORD_APP_FLAG_GATEWAY_PRESENCE_LIMITED, DISCORD_PERMISSION_ADD_REACTIONS,
    DISCORD_PERMISSION_ATTACH_FILES, DISCORD_PERMISSION_EMBED_LINKS,
    DISCORD_PERMISSION_MANAGE_MESSAGES, DISCORD_PERMISSION_READ_MESSAGE_HISTORY,
    DISCORD_PERMISSION_SEND_MESSAGES, DISCORD_PERMISSION_SEND_MESSAGES_IN_THREADS,
    DISCORD_PERMISSION_VIEW_CHANNEL,
};
