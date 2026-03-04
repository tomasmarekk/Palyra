pub mod connectors;
pub mod net;
pub mod protocol;
pub mod storage;
pub mod supervisor;

pub use protocol::{
    AttachmentKind, AttachmentRef, ConnectorInstanceSpec, ConnectorKind, ConnectorLiveness,
    ConnectorQueueDepth, ConnectorReadiness, ConnectorStatusSnapshot, DeliveryOutcome,
    InboundMessageEvent, OutboundA2uiUpdate, OutboundAttachment, OutboundMessageRequest,
    RetryClass, RouteInboundResult, RoutedOutboundMessage,
};
pub use storage::{
    ConnectorEventRecord, ConnectorInstanceRecord, ConnectorStore, ConnectorStoreError,
    DeadLetterRecord, OutboxEnqueueOutcome, OutboxEntryRecord,
};
pub use supervisor::{
    ConnectorAdapter, ConnectorAdapterError, ConnectorRouter, ConnectorRouterError,
    ConnectorSupervisor, ConnectorSupervisorConfig, ConnectorSupervisorError, DrainOutcome,
    InboundIngestOutcome,
};
