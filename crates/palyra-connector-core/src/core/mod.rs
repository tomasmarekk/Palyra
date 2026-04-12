//! Temporary shim that forwards the legacy crate to the generic core sources
//! owned by `palyra-connectors`.

#[path = "../../../palyra-connectors/src/core/net.rs"]
pub mod net;
#[path = "../../../palyra-connectors/src/core/protocol.rs"]
pub mod protocol;
#[path = "../../../palyra-connectors/src/core/storage.rs"]
pub mod storage;
#[path = "../../../palyra-connectors/src/core/supervisor.rs"]
pub mod supervisor;

pub use protocol::{
    AttachmentKind, AttachmentRef, ConnectorApprovalMode, ConnectorAvailability,
    ConnectorCapabilitySet, ConnectorCapabilitySupport, ConnectorConversationTarget,
    ConnectorInstanceSpec, ConnectorKind, ConnectorLiveness, ConnectorMessageCapabilitySet,
    ConnectorMessageDeleteRequest, ConnectorMessageEditRequest, ConnectorMessageLocator,
    ConnectorMessageMutationDiff, ConnectorMessageMutationResult, ConnectorMessageMutationStatus,
    ConnectorMessageReactionRecord, ConnectorMessageReactionRequest, ConnectorMessageReadRequest,
    ConnectorMessageReadResult, ConnectorMessageRecord, ConnectorMessageSearchRequest,
    ConnectorMessageSearchResult, ConnectorOperationPreflight, ConnectorQueueDepth,
    ConnectorReadiness, ConnectorRiskLevel, ConnectorStatusSnapshot, DeliveryOutcome,
    InboundMessageEvent, OutboundA2uiUpdate, OutboundAttachment, OutboundMessageRequest,
    RetryClass, RouteInboundResult, RoutedOutboundMessage,
};
pub use storage::{
    ConnectorEventRecord, ConnectorInstanceRecord, ConnectorQueueSnapshot, ConnectorStore,
    ConnectorStoreError, DeadLetterRecord, OutboxEnqueueOutcome, OutboxEntryRecord,
};
pub use supervisor::{
    ConnectorAdapter, ConnectorAdapterError, ConnectorRouter, ConnectorRouterError,
    ConnectorSupervisor, ConnectorSupervisorConfig, ConnectorSupervisorError, DrainOutcome,
    InboundIngestOutcome,
};
