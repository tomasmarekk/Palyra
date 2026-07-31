//! Persistent MCP runtime contracts and single-owner session coordination.
//!
//! Protocol transport adapters live here, while process creation, network I/O,
//! vault access, approvals, and resource policy stay behind injected host ports.

mod actor;
mod catalog;
mod connectors;
mod oauth;
mod policy;
mod production;
mod registry;
mod security;
mod supervisor;
mod transport;

pub use actor::{
    McpActorError, McpActorExit, McpActorHandle, McpActorNotification, McpActorParts,
    McpActorSnapshot, McpCallbackBinding, McpDrainOutcome, McpHostCallbackError,
    McpHostCallbackPort, McpSessionActor, McpSessionActorConfig,
};
pub use catalog::{McpCatalogAuthority, McpCatalogAuthorityError, McpCatalogEpochPin};
pub use connectors::{
    McpByteReader, McpByteWriter, McpConnectorCatalogState, McpConnectorEvidenceHandle,
    McpConnectorEvidenceSnapshot, McpConnectorLimits, McpConnectorPortError, McpHttpConnector,
    McpHttpConnectorConfig, McpHttpSessionCloseRequest, McpHttpSessionEventRequest,
    McpHttpSessionExchangeRequest, McpHttpSessionOpenRequest, McpHttpSessionPort,
    McpHttpSessionResponse, McpLaunchedProcessSession, McpProcessCloseEvidence, McpProcessControl,
    McpProcessLaunchRequest, McpProcessLauncher, McpReconnectEvidence, McpSseConnector,
    McpSseConnectorConfig, McpStdioConnector, McpStdioConnectorConfig,
};
pub use oauth::{
    McpOAuthCredentialError, McpOAuthCredentialLease, McpOAuthCredentialPort,
    McpOAuthRefreshCoordinator, McpOAuthRefreshError, McpOAuthRefreshRequest,
};
pub use policy::{
    McpAuthorizedElicitationRequest, McpAuthorizedSamplingRequest, McpElicitationExecutionPort,
    McpHostCallbackPolicy, McpHostExecutionError, McpHostPolicyBuildError,
    McpHostPolicyCallbackService, McpPolicyAuditAppendOutcome, McpPolicyAuditEventV1,
    McpPolicyAuditKind, McpPolicyAuditOutcome, McpPolicyAuditStore, McpPolicyAuditStoreError,
    McpSamplingExecutionPort, McpSamplingUsage,
};
pub(crate) use production::{McpProductionRuntime, McpProductionRuntimeError};
pub use registry::{
    McpActorDrainRecord, McpActorFactoryError, McpActorLaunchPlan, McpActorRegistry,
    McpActorRegistryDrainReport, McpActorRegistryError, McpActorRuntimeFactory,
};
pub use security::{
    McpSecurityEvidenceStore, McpSecurityEvidenceStoreError, McpTrustedToolActivationState,
    McpTrustedToolApproval, McpTrustedToolRecordV1, McpTrustedToolRegistry,
    McpTrustedToolRegistryError,
};
pub use supervisor::{
    admit_external_tool_descriptor, McpAdmittedToolDescriptor, McpConformanceCheck,
    McpConformanceCheckKind, McpConformanceCheckStatus, McpConformanceReportV1,
    McpDescriptorAdmissionError, McpDescriptorAdmissionPolicy, McpDescriptorAttestation,
    McpDescriptorTrustVerifier, McpExternalToolDescriptor, McpReconnectPolicy, McpRuntimeEventV2,
    McpRuntimeLifecycleState, McpRuntimeRecordStore, McpRuntimeStoreError, McpRuntimeSupervisor,
    McpRuntimeSupervisorError, McpServerRecordV2, McpToolEffectClassification,
    McpVerifiedDescriptorIdentity, TrustedExternalToolRegistrationRequest,
    MCP_SERVER_RECORD_SCHEMA_VERSION,
};
pub use transport::{
    McpCallbackResponsePayload, McpConnectRequest, McpConnectedSession, McpElicitationRequest,
    McpInitializeRequest, McpInitializeResult, McpProtocolCapabilities, McpRemoteError,
    McpResponsePayload, McpSamplingRequest, McpServerCallbackRequest, McpServerCallbackResponse,
    McpServerCallbackType, McpServerNotification, McpSessionConnector, McpSessionReader,
    McpSessionRequest, McpSessionTransportKind, McpSessionWriter, McpTransportError,
    McpTransportEvent,
};
