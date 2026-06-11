//! Provider-neutral connector runtime and storage primitives owned by
//! `palyra-connectors`.
//!
//! Providers integrate exclusively through the [`supervisor::ConnectorAdapter`]
//! and [`supervisor::ConnectorRouter`] traits; the only provider knowledge the
//! core consumes is the capability/availability registry in `crate::providers`.

pub mod net;
pub mod protocol;
pub mod storage;
pub mod supervisor;

pub use protocol::{
    AttachmentKind, AttachmentRef, ChannelCommandArgumentKind, ChannelCommandSyncState,
    ChannelCommandSyncStatus, ChannelNativeCommandArgument, ChannelNativeCommandInvocationPayload,
    ChannelNativeCommandSpec, ConnectorApprovalMode, ConnectorAvailability, ConnectorCapabilitySet,
    ConnectorCapabilitySupport, ConnectorConversationTarget, ConnectorInstanceSpec, ConnectorKind,
    ConnectorLiveness, ConnectorMessageCapabilitySet, ConnectorMessageDeleteRequest,
    ConnectorMessageEditRequest, ConnectorMessageLocator, ConnectorMessageMutationDiff,
    ConnectorMessageMutationResult, ConnectorMessageMutationStatus, ConnectorMessageReactionRecord,
    ConnectorMessageReactionRequest, ConnectorMessageReadRequest, ConnectorMessageReadResult,
    ConnectorMessageRecord, ConnectorMessageSearchRequest, ConnectorMessageSearchResult,
    ConnectorOperationPreflight, ConnectorQueueDepth, ConnectorReadiness, ConnectorRiskLevel,
    ConnectorStatusSnapshot, DeliveryOutcome, DeliveryReceipt, DeliveryReceiptState,
    InboundMessageEvent, OutboundA2uiUpdate, OutboundAttachment, OutboundMessageRequest,
    RetryClass, RouteInboundResult, RoutedOutboundMessage,
};
pub use storage::{
    ConnectorEventRecord, ConnectorInstanceRecord, ConnectorQueueSnapshot, ConnectorStore,
    ConnectorStoreError, DeadLetterRecord, OutboxEnqueueOutcome, OutboxEntryRecord,
};
pub use supervisor::{
    ConnectorAdapter, ConnectorAdapterError, ConnectorAdapterSdkDescriptor,
    ConnectorAdapterSdkOperation, ConnectorRouter, ConnectorRouterError, ConnectorSupervisor,
    ConnectorSupervisorConfig, ConnectorSupervisorError, DrainOutcome, InboundIngestOutcome,
};
