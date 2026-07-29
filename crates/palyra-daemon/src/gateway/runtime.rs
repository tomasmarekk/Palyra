//! Gateway runtime state: [`GatewayRuntimeState`] is the daemon-wide hub that
//! every transport (gRPC, HTTP console, QUIC) shares, together with the
//! config/snapshot types it exposes.
//!
//! Relationship to its neighbours: `gateway.rs` (the parent module) owns tool
//! dispatch, approval prompting, and run cleanup; `application::run_stream`
//! drives the streaming agent loop. Both lean on this file for persistence and
//! policy: orchestrator run lifecycle records (queued -> running -> waiting ->
//! terminal, parsed via `RunLifecyclePhase`), tape pagination, cancel flags and
//! run-completion wakeups (`orchestrator_run_notify`), approval decision
//! caching, provider lease admission, and the journal-backed stores for
//! sessions, memory, workspace, cron, flows, skills, and workers.
//!
//! Conventions used throughout this file:
//! - Journal/SQLite access is synchronous. Each `*_blocking` method performs
//!   the query and maps `JournalError` into a gRPC `Status`; its `pub async`
//!   twin moves that call onto `tokio::task::spawn_blocking` so transports
//!   never block the runtime. The async wrappers fail with `Status::internal`
//!   when the worker panics.
//! - `std::sync` locks guard in-memory state only and are never held across an
//!   `.await`. Poisoned locks are recovered with `into_inner()` (warn and
//!   continue) because the guarded data remains structurally valid.
//! - Counters are relaxed atomics; they feed status snapshots, never control
//!   flow.

mod managed_coding;

use super::*;
use std::collections::{BTreeMap, BTreeSet};

use palyra_auth::{
    AuthCredential, AuthCredentialType, AuthProfileEligibility, AuthProfileRecord,
    AuthProfileScope, AuthProfileSelectionRequest, AuthTokenExpiryState, CredentialAttemptBinding,
    CredentialSelectionReport, OAuthRefreshOutcomeKind,
};
use palyra_common::runtime_contracts::{
    RuntimeCausalLink, RuntimeCausalLinkKind, RuntimeIdentityKind, RuntimeIdentityRef,
};
use palyra_vault::{SensitiveBytes, VaultRef};

use crate::agents::{
    AgentBindingOutcome, AgentBindingQuery, AgentBindingRequest, AgentDeleteOutcome, AgentListPage,
    AgentRecord, AgentResolveOutcome, AgentResolveRequest, AgentSetDefaultOutcome,
    AgentUnbindOutcome, AgentUnbindRequest, SessionAgentBinding,
};
use crate::application::daemon_lifecycle::{
    DaemonDrainRequest, DaemonLifecycleController, DaemonLifecycleError, DaemonLifecyclePhase,
    DaemonLifecycleSnapshot, LifecycleSubsystem,
};
use crate::application::file_view_registry::{
    FileViewRegistry, WorkspaceFileViewRecord, WorkspacePatchFileViewReport,
};
use crate::application::restart_coordinator::{
    decide_restart, RestartBlockerSnapshot, RestartDecision, RestartRequest,
};
use crate::application::run_stream::flow_control::{
    RunInterruptLatencyObservation, RunInterruptPhase, RUN_INTERRUPT_LATENCY_CLAMPED_REASON_CODE,
    RUN_INTERRUPT_LATENCY_MAX_MS, RUN_INTERRUPT_LATENCY_REASON_CODE,
};
use crate::application::tool_governance::{
    ToolCallSignature, ToolGuardrailController, ToolGuardrailDecision,
};
use crate::application::tool_runtime::networked_worker::NetworkedWorkerRemoteDispatcher;
use crate::application::{
    auth::map_auth_profile_error,
    code_intel_runtime::{
        CodeIntelLanguage, CodeIntelProviderObservation, CodeIntelProviderRuntimeAuthority,
        CodeIntelProviderSnapshotAuthority, CodeIntelRuntime, CodeIntelRuntimeAuditEvent,
        CodeIntelRuntimeObservationRequest, CodeIntelRuntimeSnapshot,
        CodeIntelRuntimeSnapshotRequest,
    },
    progress_draft::project_progress_draft_tape_event,
    session_queue::{
        decide_queue_steering, decide_session_queue_mode, pending_queue_depth, queue_outcome,
        QueueSteeringAction, QueueSteeringDecision, QueueSteeringRequest, SessionQueueDecision,
        SessionQueuePolicy, SessionQueueSafeBoundary, QUEUE_STEERING_EVENT_COMPLETED,
        QUEUE_STEERING_EVENT_FAILED, QUEUE_STEERING_EVENT_STARTED,
    },
    turn_control::{
        decide_turn_control_request, ControlActivePhase, TurnControlAction,
        TurnControlApplyOutcome, TurnControlDecision, TurnControlOperation, TurnControlRequest,
    },
};
use crate::feature_usage::{
    FeatureUsageCapability, FeatureUsagePath, FeatureUsageRegistry, FeatureUsageSnapshot,
};
use crate::journal::state_health::{
    JournalHashChainVerificationReport, JournalHashVerificationScope, JournalHealthReport,
    JournalStateRepairReport, JournalStateRepairRequest, JournalWalCheckpointMode,
    JournalWalCheckpointReport, SidecarIndexDescriptor,
};
use crate::journal::{
    ChildCompletionReconcileReport, CommitmentCandidateV2Diagnostics, CommitmentCreateRequest,
    CommitmentDeliveryAttemptCreateRequest, CommitmentDeliveryAttemptRecord, CommitmentEventRecord,
    CommitmentListFilter, CommitmentRecord, CommitmentSourceRecord, CommitmentUpdateRequest,
    FlowBundleRecord, FlowCreateRequest, FlowDependenciesQuarantineRequest,
    FlowDependenciesRepairRequest, FlowDependencyStartupAuditReport, FlowListFilter, FlowRecord,
    FlowStepRecord, FlowStepUpdateRequest, FlowTransitionRequest, IdempotencyBeginRequest,
    IdempotencyCompleteRequest, IdempotencyFailRequest, LearningCandidateCreateRequest,
    LearningCandidateEvalCreateRequest, LearningCandidateEvalRecord,
    LearningCandidateHistoryRecord, LearningCandidateListFilter, LearningCandidateRecord,
    LearningCandidateReviewRequest, LearningCandidateRolloutCreateRequest,
    LearningCandidateRolloutRecord, LearningPreferenceListFilter, LearningPreferenceRecord,
    LearningPreferenceUpsertRequest, MemoryEmbeddingsStatus, MemoryItemLifecycleUpdateRequest,
    MemoryItemRecord, NetworkedWorkerDeliveryReservationOutcome,
    NetworkedWorkerDeliveryReservationRequest, NetworkedWorkerDispatchAbortBeforeReleaseOutcome,
    NetworkedWorkerDispatchCancelOutcome, NetworkedWorkerDispatchClaim,
    NetworkedWorkerDispatchClaimCreateRequest, NetworkedWorkerExpiryOutboxRecord,
    NetworkedWorkerPayloadAcknowledgementOutcome, NetworkedWorkerPayloadAcknowledgementRequest,
    NetworkedWorkerPayloadReleaseOutcome, NetworkedWorkerPayloadReleaseRequest,
    OrchestratorBackgroundTaskClaimRequest, OrchestratorBackgroundTaskCreateRequest,
    OrchestratorBackgroundTaskListFilter, OrchestratorBackgroundTaskRecord,
    OrchestratorBackgroundTaskUpdateRequest, OrchestratorBackgroundTaskWorkerUpdateRequest,
    OrchestratorCheckpointCreateRequest, OrchestratorCheckpointRecord,
    OrchestratorCheckpointRestoreMarkRequest, OrchestratorCompactionArtifactCreateRequest,
    OrchestratorCompactionArtifactRecord, OrchestratorParentGenerationGuard,
    OrchestratorQueuedInputCreateRequest, OrchestratorQueuedInputRecord,
    OrchestratorQueuedInputUpdateRequest, OrchestratorRunMetadataUpdateRequest,
    OrchestratorRunTerminalSettlement, OrchestratorRunTerminalSettlementRequest,
    OrchestratorSessionCleanupOutcome, OrchestratorSessionCleanupRequest,
    OrchestratorSessionLineageUpdateRequest, OrchestratorSessionPinCreateRequest,
    OrchestratorSessionPinRecord, OrchestratorSessionQueueControlRecord,
    OrchestratorSessionQueueControlUpdateRequest, OrchestratorSessionRecord,
    OrchestratorSessionTitleUpdateRequest, OrchestratorSessionTranscriptRecord,
    OrchestratorStartupBackgroundTaskRecoveryReport, OrchestratorStartupRunRecoveryReport,
    OrchestratorUsageQuery, OrchestratorUsageRunRecord, OrchestratorUsageSessionRecord,
    OrchestratorUsageSummary, ParentSuspensionCreateRequest, ParentSuspensionReconcileReport,
    ParentSuspensionRecord, ParentSuspensionWakeOutcome, PersistedProcessLeaseRecord,
    ProgressDraftEventRecord, ProgressDraftListFilter, ProgressDraftRecord,
    ProgressDraftTapeEventRequest, ProviderAttemptCompletionOutcome,
    ProviderAttemptCompletionRequest,
    ProviderAttemptRuntimeAuthority as JournalProviderAttemptRuntimeAuthority,
    ProviderAttemptStartRequest, ProviderConfigurationAttemptCompletionOutcome,
    ProviderConfigurationAttemptCompletionRequest,
    ProviderConfigurationAttemptRuntimeAuthority as JournalProviderConfigurationAttemptRuntimeAuthority,
    ProviderConfigurationAttemptStartRequest, ProviderCredentialAttemptMetadata,
    RecallArtifactCreateRequest, RecallArtifactListFilter, RecallArtifactRecord,
    RemediationDecision, RetrievalBranchDiagnostics, RunScopedRuntimeHealthObservationOutcome,
    RuntimeEventAppendOutcome, RuntimeEventAppendRequest, RuntimeHealthComponentActivation,
    RuntimeHealthObservationRequest, RuntimeHealthProbeBeginRequest,
    RuntimeHealthProbeReconciliationMode, RuntimeHealthProbeReconciliationOutcome,
    RuntimeHealthProbeSettlementOutcome, RuntimeHealthProbeSettlementRequest,
    RuntimeHealthQuarantineClearOutcome, RuntimeHealthQuarantineClearRequest,
    RuntimeStaleEventDiagnosticRequest, ScopedSessionRuntimeGeneration, SessionModelCommandRecord,
    SessionModelCommandReserveOutcome, SessionModelCommandReserveRequest,
    SessionModelCommandSettlementRequest, SessionProjectContextStateCopyRequest,
    SessionProjectContextStateRecord, SessionProjectContextStateUpsertRequest,
    SessionSearchOutcome, SessionSearchRequest, SessionWriteLeaseRecord, SharedRuntimeDiagnostics,
    SideEffectFenceCleanupOutcomeRequest, SideEffectFenceOperatorResolutionRequest,
    StuckRunIncidentV2, StuckRunRemediationClaimOutcome, StuckRunRemediationCompletionOutcome,
    ToolEffectObservationCommitRequest, ToolJobAttachRequest, ToolJobCreateRequest, ToolJobRecord,
    ToolJobRetryRequest, ToolJobTailAppendRequest, ToolJobTailPage, ToolJobTailReadRequest,
    ToolJobTransitionRequest, ToolJobsListFilter, ToolResultArtifactCreateRequest,
    ToolResultArtifactReadRequest, TurnControlAuditEventAppendRequest,
    TurnControlAuditEventListFilter, TurnControlAuditEventRecord, WorkItemCreateRequest,
    WorkItemEventRecord, WorkItemListFilter, WorkItemRecord, WorkItemUpdateRequest,
    WorkspaceBootstrapOutcome, WorkspaceBootstrapRequest, WorkspaceCheckpointCreateRequest,
    WorkspaceCheckpointFilePayload, WorkspaceCheckpointFileRecord, WorkspaceCheckpointListFilter,
    WorkspaceCheckpointPairLinkRequest, WorkspaceCheckpointRecord,
    WorkspaceCheckpointRestoreMarkRequest, WorkspaceDocumentDeleteRequest,
    WorkspaceDocumentListFilter, WorkspaceDocumentMoveRequest, WorkspaceDocumentRecord,
    WorkspaceDocumentVersionRecord, WorkspaceDocumentWriteRequest, WorkspaceRestoreActivityFilter,
    WorkspaceRestoreActivitySummary, WorkspaceRestoreReportCreateRequest,
    WorkspaceRestoreReportListFilter, WorkspaceRestoreReportRecord, WorkspaceScoreBreakdown,
    WorkspaceSearchHit, WorkspaceSearchRequest, NETWORKED_WORKER_DISPATCH_CLAIM_MAX_ENTRIES,
    NETWORKED_WORKER_EXPIRY_MAX_ENTRIES, NETWORKED_WORKER_FLEET_MAX_ENTRIES,
};
use crate::model_provider::{
    is_provider_attempt_superseded_error, provider_attempt_admission_provider_error,
    ProviderAttemptAdmission, ProviderAttemptAdmissionError, ProviderAttemptBinding,
    ProviderAttemptCompletionDisposition, ProviderAttemptCompletionFuture,
    ProviderAttemptHealthAuthority, ProviderAttemptPermit, ProviderAttemptPermitFuture,
    ProviderAttemptPreparationFuture, ProviderAttemptRuntimeAuthority, ProviderAttemptStartFuture,
    ProviderCredentialLease, ProviderCredentialLeaseFuture, ProviderFailureClass,
    ProviderHealthProbeTarget, ProviderModelRole, ProviderProbeAdmission,
};
use crate::node_runtime::CapabilityDispatchAuthorizer;
use crate::provider_leases::{
    LeasePriority, ProviderCredentialFeedbackKind, ProviderCredentialFeedbackRequest,
    ProviderLeaseAcquireError, ProviderLeaseAcquireRequest, ProviderLeaseExecutionContext,
    ProviderLeaseManager, ProviderLeaseManagerSnapshot, ProviderLeasePreviewRequest,
    ProviderLeasePreviewSnapshot,
};
use crate::qa_fault_injection::QaFaultRuntime;
use crate::retrieval::{
    lexical_overlap_score, recency_score as retrieval_recency_score, score_memory_candidates,
    score_with_profile, score_workspace_candidates, workspace_source_quality,
    ExternalRetrievalRuntime, RetrievalBackend, RetrievalBackendSnapshot, RetrievalRuntimeConfig,
    RetrievalSourceProfileKind,
};
use crate::self_healing::{
    IncidentDomain, OrphanReconciliationReport, RemediationAttemptStatus,
    RuntimeIncidentHistoryEntry, RuntimeIncidentObservation, RuntimeIncidentRecord,
    RuntimeIncidentSummary, RuntimeRemediationAttemptRecord, SelfHealingFeature,
    SelfHealingSettingsSnapshot, SelfHealingState, WorkHeartbeatKind, WorkHeartbeatRecord,
    WorkHeartbeatUpdate,
};
use crate::tool_posture::{
    ToolPostureAuditEventRecord, ToolPostureOverrideClearRequest, ToolPostureOverrideRecord,
    ToolPostureOverrideUpsertRequest, ToolPostureRecommendationActionRecord,
    ToolPostureRecommendationActionRequest, ToolPostureRegistry, ToolPostureScopeResetRequest,
};
use crate::usage_governance::SmartRoutingRuntimeConfig;
use futures::FutureExt;
use palyra_auth::{AuthHealthReport, AuthProfileFailureKind};
use palyra_common::qa_fault_injection::{QaFaultAction, QaFaultDirective};
use palyra_common::replay_bundle::ReplayBundle;
use palyra_common::runtime_contracts::{
    ArtifactReadResponse, CircuitBreakerPolicy, CleanupOutcome, CleanupReportV1,
    CleanupStepDisposition, CleanupStepKind, CleanupStepRecord, HealthProbeDisposition,
    HealthProbeLeaseV1, HealthProbeResult, HealthProbeSettlementV1, IdempotencyReplayDecision,
    ProcessLeaseV1, ProcessProvenanceDisposition, QuarantineClearRequest, QueueDecision, QueueMode,
    QueuedInputState, RunLifecyclePhase, RuntimeApprovalSubjectId, RuntimeAttemptId,
    RuntimeAuthorityClass, RuntimeComponentHealthV1, RuntimeEventEnvelopeV2, RuntimeEventId,
    RuntimeEventName, RuntimeEventPayloadRef, RuntimeGeneration, RuntimeGenerationLane,
    RuntimeHandleDescriptorV1, RuntimeHandleState, RuntimeIdentitySetV1, RuntimeInstanceId,
    RuntimeLeaseId, RuntimeOperationId, RuntimeOrdinaryAdmissionDecision,
    RuntimeProbeAdmissionDecision, RuntimeRunId, RuntimeSessionId, RuntimeSubsystem,
    RuntimeToolExecutionId, RuntimeToolProposalId, SideEffectFenceState, SideEffectFenceV1,
    SideEffectRetryDecision, StableErrorEnvelope, StaleEventDisposition, ToolResultArtifactRef,
    HEALTH_PROBE_LEASE_SCHEMA_VERSION, HEALTH_PROBE_RESULT_SCHEMA_VERSION,
    HEALTH_PROBE_SETTLEMENT_SCHEMA_VERSION,
};
use palyra_common::runtime_preview::{
    RuntimeDecisionActor, RuntimeDecisionActorKind, RuntimeDecisionEventType,
    RuntimeDecisionPayload, RuntimeDecisionTiming, RuntimeEntityRef, RuntimeResourceBudget,
};
use palyra_workerd::{
    networked_worker_expiry_event_id, WorkerAttestation, WorkerCleanupReport, WorkerFleetManager,
    WorkerFleetPolicy, WorkerFleetSnapshot, WorkerLease, WorkerLeaseIdentity, WorkerLeaseRequest,
    WorkerLifecycleEvent, WORKER_REMOTE_TOOL_CAPABILITIES,
};
use ring::hmac;
use serde_json::{json, Value};
use std::{panic::AssertUnwindSafe, path::PathBuf};
use tokio::sync::{oneshot, Mutex as AsyncMutex, Notify};

pub(crate) const PROCESS_LEASE_RECONCILIATION_BATCH_SIZE: usize = 256;
const PROCESS_LEASE_RECONCILIATION_CHECKPOINT_KEY: &str = "process_leases.v1";
pub(crate) const PENDING_PROCESS_CLEANUP_MAX_ENTRIES: usize = 256;
pub(crate) const PENDING_NETWORKED_WORKER_EXPIRY_MAX_ENTRIES: usize =
    NETWORKED_WORKER_EXPIRY_MAX_ENTRIES;

/// Hash-only receipt metadata committed atomically with a verified remote worker completion.
#[derive(Debug, Clone)]
pub(crate) struct NetworkedWorkerArtifactReceipt {
    pub(crate) request_id: String,
    pub(crate) proposal_id: String,
    pub(crate) tool_name: String,
    pub(crate) principal: String,
    pub(crate) device_id: String,
    pub(crate) channel: Option<String>,
    pub(crate) session_id: String,
    pub(crate) run_id: String,
    pub(crate) input_json_sha256: String,
    pub(crate) output_json_sha256: String,
    pub(crate) output_manifest_sha256: String,
    pub(crate) validated_result_sha256: String,
    pub(crate) grant_id: String,
    pub(crate) required_capabilities: Vec<String>,
    pub(crate) workspace_scope: palyra_workerd::WorkerWorkspaceScope,
    pub(crate) log_stream_id: String,
    pub(crate) scratch_directory_id: String,
    pub(crate) observed_at_unix_ms: i64,
}

/// Exact node-delivery identity required to settle a durable worker dispatch claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NetworkedWorkerDispatchSettlementIdentity {
    pub(crate) remote_request_id: String,
    pub(crate) delivery_attempt_id: Option<String>,
    pub(crate) session_id: String,
    pub(crate) run_generation: RuntimeGeneration,
}

#[derive(Debug, Clone)]
struct PreparedNetworkedWorkerLifecycleEvidence {
    commit: crate::journal::NetworkedWorkerLifecycleCommit,
    payload: RuntimeDecisionPayload,
}

#[derive(Debug)]
enum NetworkedWorkerResultCompletionOutcome {
    Completed(WorkerLifecycleEvent),
    StaleSuppressed,
}

#[derive(Debug)]
enum PreparedNetworkedWorkerLifecycleCommitOutcome {
    Committed { acknowledgement_error: Option<JournalError> },
    StaleSuppressed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TerminalSettlementAuthority {
    CancellationAware,
    Exact,
}

/// Result of assigning a worker lease under exact run-generation authority.
#[derive(Debug)]
pub(crate) enum NetworkedWorkerLeaseAssignmentOutcome {
    Assigned { lease: Box<WorkerLease> },
    TransportRejected { reason: String },
    StaleSuppressed,
}

fn process_reconciliation_evidence_sha256(
    record: &PersistedProcessLeaseRecord,
    disposition: ProcessProvenanceDisposition,
    expired: bool,
) -> String {
    let mut hasher = sha2::Sha256::new();
    hasher.update(b"palyra.process_lease_reconciliation.v1\0");
    hasher.update(record.lease.lease_id.as_str().as_bytes());
    hasher.update(b"\0");
    hasher.update(record.descriptor.instance_id.as_str().as_bytes());
    hasher.update(b"\0");
    hasher.update(record.lease.provenance.start_token.as_bytes());
    hasher.update(b"\0");
    hasher.update(record.lease.provenance.executable_sha256.as_bytes());
    hasher.update(b"\0");
    hasher.update(disposition.as_str().as_bytes());
    hasher.update(b"\0");
    hasher.update(if expired { "expired" } else { "active" }.as_bytes());
    hex::encode(hasher.finalize())
}

fn process_reconciliation_report_id(
    record: &PersistedProcessLeaseRecord,
    disposition: ProcessProvenanceDisposition,
    expired: bool,
) -> String {
    let digest = process_reconciliation_evidence_sha256(record, disposition, expired);
    format!("process-reconcile:{}", &digest[..32])
}

fn provider_health_component_id(
    provider_id: &str,
) -> Result<RuntimeInstanceId, palyra_common::runtime_contracts::RuntimeIdentityError> {
    let normalized = provider_id.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return RuntimeInstanceId::parse(normalized.as_str());
    }
    let direct = format!("provider:{normalized}");
    let value = if direct.len() <= palyra_common::runtime_contracts::MAX_RUNTIME_ID_BYTES {
        direct
    } else {
        let mut hasher = sha2::Sha256::new();
        hasher.update(normalized.as_bytes());
        format!("provider:sha256:{}", hex::encode(hasher.finalize()))
    };
    RuntimeInstanceId::parse(value.as_str())
}

const MANAGED_RUNTIME_HEALTH_SCHEMA_VERSION: u32 = 1;
const MANAGED_RUNTIME_HEALTH_PROBE_LEASE_MS: i64 = 15_000;
const PLUGIN_HEALTH_STRIKE_THRESHOLD: u32 = 3;
const PLUGIN_HEALTH_COOLDOWN_MS: u64 = 60_000;
const MCP_HEALTH_STRIKE_THRESHOLD: u32 = 3;
const MCP_HEALTH_COOLDOWN_MS: u64 = 30_000;
const LSP_HEALTH_STRIKE_THRESHOLD: u32 = 3;
const LSP_HEALTH_COOLDOWN_MS: u64 = 30_000;
const SSH_HEALTH_STRIKE_THRESHOLD: u32 = 3;
const SSH_HEALTH_COOLDOWN_MS: u64 = 60_000;
const WORKER_HEALTH_STRIKE_THRESHOLD: u32 = 3;
const WORKER_HEALTH_COOLDOWN_MS: u64 = 60_000;

/// Long-lived daemon component families governed by shared durable health.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ManagedRuntimeHealthFamily {
    Plugin,
    Mcp,
    Lsp,
    Ssh,
    Worker,
}

impl ManagedRuntimeHealthFamily {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Plugin => "plugin",
            Self::Mcp => "mcp",
            Self::Lsp => "lsp",
            Self::Ssh => "ssh",
            Self::Worker => "worker",
        }
    }

    const fn authority_class(self) -> RuntimeAuthorityClass {
        match self {
            Self::Lsp => RuntimeAuthorityClass::ObserveOnly,
            Self::Plugin | Self::Mcp | Self::Ssh | Self::Worker => {
                RuntimeAuthorityClass::PrivilegedMutation
            }
        }
    }

    const fn policy(self) -> CircuitBreakerPolicy {
        let (strike_threshold, cooldown_ms) = match self {
            Self::Plugin => (PLUGIN_HEALTH_STRIKE_THRESHOLD, PLUGIN_HEALTH_COOLDOWN_MS),
            Self::Mcp => (MCP_HEALTH_STRIKE_THRESHOLD, MCP_HEALTH_COOLDOWN_MS),
            Self::Lsp => (LSP_HEALTH_STRIKE_THRESHOLD, LSP_HEALTH_COOLDOWN_MS),
            Self::Ssh => (SSH_HEALTH_STRIKE_THRESHOLD, SSH_HEALTH_COOLDOWN_MS),
            Self::Worker => (WORKER_HEALTH_STRIKE_THRESHOLD, WORKER_HEALTH_COOLDOWN_MS),
        };
        CircuitBreakerPolicy {
            strike_threshold,
            cooldown_ms,
            max_probe_concurrency: 1,
            security_quarantine_auto_clear: false,
        }
    }

    const fn activation_reason_code(self) -> &'static str {
        match self {
            Self::Plugin => "runtime.health.plugin_activated",
            Self::Mcp => "runtime.health.mcp_activated",
            Self::Lsp => "runtime.health.lsp_activated",
            Self::Ssh => "runtime.health.ssh_activated",
            Self::Worker => "runtime.health.worker_activated",
        }
    }

    const fn generation_lane(self) -> RuntimeGenerationLane {
        match self {
            Self::Plugin => RuntimeGenerationLane::Plugin,
            Self::Mcp => RuntimeGenerationLane::Mcp,
            Self::Lsp | Self::Ssh => RuntimeGenerationLane::Process,
            Self::Worker => RuntimeGenerationLane::Worker,
        }
    }

    const fn subsystem(self) -> RuntimeSubsystem {
        match self {
            Self::Plugin => RuntimeSubsystem::Plugin,
            Self::Mcp => RuntimeSubsystem::Mcp,
            Self::Lsp | Self::Ssh => RuntimeSubsystem::Tool,
            Self::Worker => RuntimeSubsystem::Worker,
        }
    }

    fn from_component_id(component_id: &str) -> Option<Self> {
        [Self::Plugin, Self::Mcp, Self::Lsp, Self::Ssh, Self::Worker].into_iter().find(|family| {
            component_id.strip_prefix(family.as_str()).is_some_and(|suffix| suffix.starts_with(':'))
        })
    }
}

/// Exact component generation captured before a managed runtime effect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManagedRuntimeHealthAuthority {
    pub(crate) family: ManagedRuntimeHealthFamily,
    pub(crate) component_id: RuntimeInstanceId,
    pub(crate) generation: RuntimeGeneration,
}

/// Read-model projection plus final provider authority classifications.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CodeIntelRuntimeProjectionOutcome {
    pub(crate) snapshot: CodeIntelRuntimeSnapshot,
    pub(crate) audit_events: Vec<CodeIntelRuntimeAuditEvent>,
    pub(crate) provider_snapshot_authority:
        BTreeMap<CodeIntelLanguage, CodeIntelProviderSnapshotAuthority>,
}

/// Redacted bounded inventory exposed to operator diagnostics.
#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct ManagedRuntimeHealthSnapshot {
    pub(crate) schema_version: u32,
    pub(crate) generated_at_unix_ms: i64,
    pub(crate) inventory_sha256: String,
    pub(crate) components: Vec<RuntimeComponentHealthV1>,
    pub(crate) components_by_family: BTreeMap<String, u64>,
    pub(crate) components_by_state: BTreeMap<String, u64>,
    pub(crate) stale_suppressions_total: u64,
}

/// Reload-fenced provider registry, epoch, and exact durable health evidence.
///
/// The gateway holds the provider runtime read lock while capturing every
/// field, so a provider reload cannot combine candidates from one
/// configuration with health authorities from another.
#[derive(Debug, Clone)]
pub(crate) struct GatewayProviderSelectionSnapshot {
    pub(crate) observed_at_unix_ms: i64,
    pub(crate) configuration_epoch: RuntimeGeneration,
    pub(crate) status: ProviderStatusSnapshot,
    pub(crate) health_authority_by_provider: BTreeMap<String, ProviderAttemptHealthAuthority>,
    pub(crate) health_records: Vec<RuntimeComponentHealthV1>,
    pub(crate) embedded_harness_descriptors:
        Vec<crate::application::agent_harness::AgentHarnessDescriptor>,
    pub(crate) context_engine_registry:
        crate::application::context_engine::ContextEngineRegistrySnapshot,
    pub(crate) build_version: String,
}

pub(crate) fn managed_runtime_health_component_id(
    family: ManagedRuntimeHealthFamily,
    raw_id: &str,
) -> Result<RuntimeInstanceId, palyra_common::runtime_contracts::RuntimeIdentityError> {
    let normalized = raw_id.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return RuntimeInstanceId::parse(normalized.as_str());
    }
    let direct = format!("{}:{normalized}", family.as_str());
    let value = if direct.len() <= palyra_common::runtime_contracts::MAX_RUNTIME_ID_BYTES {
        direct
    } else {
        let mut hasher = sha2::Sha256::new();
        hasher.update(normalized.as_bytes());
        format!("{}:sha256:{}", family.as_str(), hex::encode(hasher.finalize()))
    };
    RuntimeInstanceId::parse(value.as_str())
}

fn managed_runtime_health_activation(
    family: ManagedRuntimeHealthFamily,
    raw_id: &str,
) -> Result<RuntimeHealthComponentActivation, JournalError> {
    let component_id = managed_runtime_health_component_id(family, raw_id)
        .map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
    Ok(RuntimeHealthComponentActivation {
        component_id,
        authority_class: family.authority_class(),
        fallback_component_id: None,
        fallback_authority_class: None,
        policy: family.policy(),
        reason_code: family.activation_reason_code().to_owned(),
    })
}

fn managed_runtime_health_inventory(
    config: &GatewayRuntimeConfigSnapshot,
    durable_worker_ids: impl IntoIterator<Item = String>,
) -> Result<Vec<RuntimeHealthComponentActivation>, JournalError> {
    let mut activations = Vec::new();
    for plugin_id in &config.plugin_binding_ids {
        activations.push(managed_runtime_health_activation(
            ManagedRuntimeHealthFamily::Plugin,
            plugin_id,
        )?);
    }
    if config.mcp_servers.mode != palyra_common::runtime_preview::RuntimePreviewMode::Disabled {
        for server in config.mcp_servers.servers.iter().filter(|server| server.enabled) {
            activations.push(managed_runtime_health_activation(
                ManagedRuntimeHealthFamily::Mcp,
                server.id.as_str(),
            )?);
        }
    }
    if config.code_intel.enabled {
        for language in crate::application::code_intel_runtime::CodeIntelLanguage::ALL {
            activations.push(managed_runtime_health_activation(
                ManagedRuntimeHealthFamily::Lsp,
                language.as_str(),
            )?);
        }
    }
    if config.execution_backend_profiles.mode
        != palyra_common::runtime_preview::RuntimePreviewMode::Disabled
    {
        for profile in config
            .execution_backend_profiles
            .profiles
            .iter()
            .filter(|profile| profile.enabled && profile.kind == "ssh_worker")
        {
            activations.push(managed_runtime_health_activation(
                ManagedRuntimeHealthFamily::Ssh,
                profile.id.as_str(),
            )?);
        }
    }
    for worker_id in durable_worker_ids {
        activations.push(managed_runtime_health_activation(
            ManagedRuntimeHealthFamily::Worker,
            worker_id.as_str(),
        )?);
    }
    activations.sort_by(|left, right| left.component_id.as_str().cmp(right.component_id.as_str()));
    activations.dedup_by(|left, right| left.component_id == right.component_id);
    Ok(activations)
}

#[derive(Debug)]
struct ProviderHealthInventory {
    activations: Vec<RuntimeHealthComponentActivation>,
    component_ids_by_provider: BTreeMap<String, RuntimeInstanceId>,
}

fn provider_health_inventory(
    snapshot: &ProviderStatusSnapshot,
) -> Result<ProviderHealthInventory, JournalError> {
    let callable_provider_ids = snapshot
        .registry
        .models
        .iter()
        .filter(|model| {
            model.enabled && matches!(model.role.as_str(), "chat" | "audio_transcription")
        })
        .map(|model| model.provider_id.as_str())
        .collect::<HashSet<_>>();
    let mut activations = Vec::new();
    let mut component_ids_by_provider = BTreeMap::new();

    for provider in snapshot.registry.providers.iter().filter(|provider| {
        provider.enabled && callable_provider_ids.contains(provider.provider_id.as_str())
    }) {
        let component_id = provider_health_component_id(provider.provider_id.as_str())
            .map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
        if component_ids_by_provider
            .insert(provider.provider_id.clone(), component_id.clone())
            .is_some()
        {
            return Err(JournalError::InvalidArgument(
                "provider health inventory contains duplicate provider identities".to_owned(),
            ));
        }
        activations.push(RuntimeHealthComponentActivation {
            component_id,
            authority_class: RuntimeAuthorityClass::PrivilegedMutation,
            fallback_component_id: None,
            fallback_authority_class: None,
            policy: CircuitBreakerPolicy {
                strike_threshold: provider.circuit_breaker.failure_threshold.max(1),
                cooldown_ms: provider.circuit_breaker.cooldown_ms.max(1),
                max_probe_concurrency: 1,
                security_quarantine_auto_clear: false,
            },
            reason_code: "runtime.health.provider_activated".to_owned(),
        });
    }

    if activations.is_empty() {
        let component_id = provider_health_component_id(snapshot.provider_id.as_str())
            .map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
        component_ids_by_provider.insert(snapshot.provider_id.clone(), component_id.clone());
        activations.push(RuntimeHealthComponentActivation {
            component_id,
            authority_class: RuntimeAuthorityClass::PrivilegedMutation,
            fallback_component_id: None,
            fallback_authority_class: None,
            policy: CircuitBreakerPolicy {
                strike_threshold: snapshot.circuit_breaker.failure_threshold.max(1),
                cooldown_ms: snapshot.circuit_breaker.cooldown_ms.max(1),
                max_probe_concurrency: 1,
                security_quarantine_auto_clear: false,
            },
            reason_code: "runtime.health.provider_activated".to_owned(),
        });
    }

    activations.sort_by(|left, right| left.component_id.cmp(&right.component_id));
    Ok(ProviderHealthInventory { activations, component_ids_by_provider })
}

fn activated_provider_health_authorities(
    inventory: &ProviderHealthInventory,
    activation: &crate::journal::RuntimeHealthActivationOutcome,
) -> Result<BTreeMap<String, ProviderAttemptHealthAuthority>, JournalError> {
    inventory
        .component_ids_by_provider
        .iter()
        .map(|(provider_id, component_id)| {
            activation
                .generations
                .get(component_id.as_str())
                .copied()
                .map(|generation| {
                    (
                        provider_id.clone(),
                        ProviderAttemptHealthAuthority {
                            component_id: component_id.clone(),
                            generation,
                        },
                    )
                })
                .ok_or_else(|| {
                    JournalError::InvalidArgument(format!(
                        "provider health activation omitted provider '{provider_id}'"
                    ))
                })
        })
        .collect()
}

#[derive(Debug, Clone, Copy)]
struct SharedRuntimeTapeEvent {
    name: RuntimeEventName,
    reason_code: &'static str,
}

fn shared_runtime_event_for_tape(
    request: &OrchestratorTapeAppendRequest,
) -> Option<SharedRuntimeTapeEvent> {
    let (name, reason_code) = match request.event_type.as_str() {
        "status" => {
            let payload = serde_json::from_str::<Value>(request.payload_json.as_str()).ok()?;
            match payload.get("kind").and_then(Value::as_str)? {
                "accepted" => (RuntimeEventName::RunQueued, "runtime.event.run_queued"),
                "in_progress"
                    if payload
                        .get("lifecycle_state")
                        .and_then(Value::as_str)
                        .is_some_and(|state| state == "in_progress") =>
                {
                    (RuntimeEventName::RunStarted, "runtime.event.run_started")
                }
                "done" => (RuntimeEventName::RunCompleted, "runtime.event.run_completed"),
                "cancelled" => (RuntimeEventName::RunCancelled, "runtime.event.run_cancelled"),
                "failed" => (RuntimeEventName::RunFailed, "runtime.event.run_failed"),
                _ => return None,
            }
        }
        "model_token" => (RuntimeEventName::ModelDelta, "runtime.event.model_delta"),
        "tool_proposal" => (RuntimeEventName::ToolProposed, "runtime.event.tool_proposed"),
        "tool_decision" | "tool_denied" => {
            (RuntimeEventName::ToolDecisionRecorded, "runtime.event.tool_decision_recorded")
        }
        "tool_approval_request" => {
            (RuntimeEventName::ApprovalRequired, "runtime.event.approval_required")
        }
        "tool_approval_response" => {
            (RuntimeEventName::ApprovalResolved, "runtime.event.approval_resolved")
        }
        "tool_result" => {
            (RuntimeEventName::ToolResultObserved, "runtime.event.tool_result_observed")
        }
        "tool_attestation" => {
            (RuntimeEventName::ToolAttestationObserved, "runtime.event.tool_attestation_observed")
        }
        crate::application::run_stream::flow_control::PROCESS_PROGRESS_BACKPRESSURE_TAPE_EVENT => (
            RuntimeEventName::BackpressureApplied,
            crate::application::run_stream::flow_control::PROCESS_PROGRESS_BACKPRESSURE_REASON_CODE,
        ),
        crate::runtime_diagnostics::RUN_RUNTIME_PATH_SUMMARY_EVENT => return None,
        _ => return None,
    };
    Some(SharedRuntimeTapeEvent { name, reason_code })
}

fn runtime_tape_payload_field<'a>(payload: &'a Value, field: &str) -> Option<&'a str> {
    payload.get(field).and_then(Value::as_str).filter(|value| !value.is_empty())
}

fn apply_shared_runtime_tape_identities(
    identities: &mut RuntimeIdentitySetV1,
    request: &OrchestratorTapeAppendRequest,
) -> Result<(), JournalError> {
    let payload =
        serde_json::from_str::<Value>(request.payload_json.as_str()).map_err(|error| {
            JournalError::InvalidArgument(format!(
                "shared runtime tape payload is invalid JSON: {error}"
            ))
        })?;
    if let Some(proposal_id) = runtime_tape_payload_field(&payload, "proposal_id") {
        identities.tool_proposal_id = Some(
            RuntimeToolProposalId::parse(proposal_id)
                .map_err(|error| JournalError::InvalidArgument(error.to_string()))?,
        );
    }
    if let Some(approval_id) = runtime_tape_payload_field(&payload, "approval_id") {
        identities.approval_subject_id = Some(
            RuntimeApprovalSubjectId::parse(approval_id)
                .map_err(|error| JournalError::InvalidArgument(error.to_string()))?,
        );
    }
    if matches!(request.event_type.as_str(), "tool_result" | "tool_attestation") {
        let Some(proposal_id) = identities.tool_proposal_id.as_ref() else {
            return Ok(());
        };
        let stable_execution_id = format!("tool:{}", proposal_id.as_str());
        identities.tool_execution_id = Some(
            RuntimeToolExecutionId::parse(stable_execution_id.as_str())
                .map_err(|error| JournalError::InvalidArgument(error.to_string()))?,
        );
        identities.operation_id = Some(
            RuntimeOperationId::parse(stable_execution_id.as_str())
                .map_err(|error| JournalError::InvalidArgument(error.to_string()))?,
        );
    }
    Ok(())
}

mod external_retrieval;
mod metadata_trace;

fn sign_canvas_hmac_sha256(secret: &[u8], domain: &str, parts: &[&[u8]]) -> String {
    let key = hmac::Key::new(hmac::HMAC_SHA256, secret);
    let mut context = hmac::Context::with_key(&key);
    context.update(domain.as_bytes());
    for part in parts {
        context.update(&(part.len() as u64).to_be_bytes());
        context.update(part);
    }
    URL_SAFE_NO_PAD.encode(context.sign().as_ref())
}

fn session_queue_boundary_for_control(
    active_phase: ControlActivePhase,
) -> SessionQueueSafeBoundary {
    match active_phase {
        ControlActivePhase::ProviderStream => SessionQueueSafeBoundary {
            active_run_stream: true,
            pending_approval: false,
            sensitive_tool_execution: false,
            delivery_in_progress: false,
            before_model_round: true,
            after_model_round: false,
            after_tool_result: false,
            after_approval_wait: false,
            after_child_merge: false,
        },
        ControlActivePhase::ToolExecution => SessionQueueSafeBoundary {
            active_run_stream: true,
            pending_approval: false,
            sensitive_tool_execution: true,
            delivery_in_progress: false,
            before_model_round: false,
            after_model_round: false,
            after_tool_result: false,
            after_approval_wait: false,
            after_child_merge: false,
        },
        ControlActivePhase::ApprovalPending => SessionQueueSafeBoundary {
            active_run_stream: true,
            pending_approval: true,
            sensitive_tool_execution: false,
            delivery_in_progress: false,
            before_model_round: false,
            after_model_round: false,
            after_tool_result: false,
            after_approval_wait: true,
            after_child_merge: false,
        },
        ControlActivePhase::Queue
        | ControlActivePhase::BackgroundTask
        | ControlActivePhase::Idle => SessionQueueSafeBoundary {
            active_run_stream: false,
            pending_approval: false,
            sensitive_tool_execution: false,
            delivery_in_progress: false,
            before_model_round: false,
            after_model_round: false,
            after_tool_result: false,
            after_approval_wait: false,
            after_child_merge: false,
        },
    }
}

/// Immutable copy of the daemon configuration the gateway runtime was started with.
///
/// Captured once at startup and shared read-only. Settings that can be retuned
/// at runtime (memory, retrieval, learning, routines) live separately in
/// [`GatewayRuntimeState`] behind locks.
#[derive(Debug, Clone)]
pub struct GatewayRuntimeConfigSnapshot {
    pub grpc_bind_addr: String,
    pub grpc_port: u16,
    pub quic_bind_addr: String,
    pub quic_port: u16,
    pub quic_enabled: bool,
    pub orchestrator_runloop_v1_enabled: bool,
    pub model_provider_request_timeout_ms: u64,
    pub qa_execution_key_digest: Option<String>,
    pub qa_provider_binding_sha256: Option<String>,
    pub node_rpc_mtls_required: bool,
    pub admin_auth_required: bool,
    pub vault_get_approval_required_refs: Vec<String>,
    pub max_tape_entries_per_response: usize,
    pub max_tape_bytes_per_response: usize,
    pub feature_rollouts: crate::config::FeatureRolloutsConfig,
    pub session_queue_policy: crate::config::SessionQueuePolicyConfig,
    pub pruning_policy_matrix: crate::config::PruningPolicyMatrixConfig,
    pub retrieval_dual_path: crate::config::RetrievalDualPathConfig,
    pub auxiliary_executor: crate::config::AuxiliaryExecutorConfig,
    pub flow_orchestration: crate::config::FlowOrchestrationConfig,
    pub delivery_arbitration: crate::config::DeliveryArbitrationConfig,
    pub replay_capture: crate::config::ReplayCaptureConfig,
    pub networked_workers: crate::config::NetworkedWorkersConfig,
    pub mcp_servers: crate::config::McpServersConfig,
    pub(crate) plugin_binding_ids: Vec<String>,
    pub execution_backend_profiles: crate::config::ExecutionBackendProfilesConfig,
    pub agent_harness_registry: crate::config::AgentHarnessRegistryConfig,
    pub channel_router: ChannelRouterConfig,
    pub media: MediaRuntimeConfig,
    pub code_intel: crate::config::CodeIntelConfig,
    pub tool_call: ToolCallConfig,
    pub tool_catalog_policy: crate::application::tool_registry::ToolCatalogPolicySnapshot,
    pub http_fetch: HttpFetchRuntimeConfig,
    pub browser_service: BrowserServiceRuntimeConfig,
    pub canvas_host: CanvasHostRuntimeConfig,
    pub smart_routing: SmartRoutingRuntimeConfig,
}

/// Cursor-paginated filter for listing sessions scoped to a principal and
/// device (plus optional channel and case-insensitive search).
#[derive(Debug, Clone)]
pub struct ListOrchestratorSessionsRequest {
    pub after_session_key: Option<String>,
    pub principal: String,
    pub device_id: String,
    pub channel: Option<String>,
    pub include_archived: bool,
    pub requested_limit: Option<usize>,
    pub search_query: Option<String>,
}

/// Cursor-paginated filter for listing sessions across all devices of one
/// principal.
#[derive(Debug, Clone)]
pub struct ListPrincipalOrchestratorSessionsRequest {
    pub after_session_key: Option<String>,
    pub principal: String,
    pub include_archived: bool,
    pub requested_limit: Option<usize>,
    pub search_query: Option<String>,
}

/// Parameters for [`GatewayRuntimeState::wait_for_orchestrator_run`].
///
/// `return_on_waiting` makes the wait resolve as soon as the run parks in a
/// waiting phase (for example pending approval) instead of only on terminal
/// phases.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrchestratorRunWaitRequest {
    pub run_id: String,
    pub timeout: Duration,
    pub poll_interval: Duration,
    pub return_on_waiting: bool,
}

/// Final run snapshot observed by a wait, plus its parsed canonical phase.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrchestratorRunWaitOutcome {
    pub snapshot: OrchestratorRunStatusSnapshot,
    pub canonical_state: RunLifecyclePhase,
}

/// Request for the shared session queue admission path.
#[derive(Debug, Clone)]
pub(crate) struct SessionQueueAdmissionRequest {
    pub(crate) queued_input_id: Option<String>,
    pub(crate) session_id: String,
    pub(crate) run_id: String,
    pub(crate) origin_run_id: Option<String>,
    pub(crate) text: String,
    pub(crate) requested_mode: Option<QueueMode>,
    pub(crate) policy_channel: Option<String>,
    pub(crate) policy_agent_id: Option<String>,
    pub(crate) safe_boundary: SessionQueueSafeBoundary,
    pub(crate) actor_principal: String,
    pub(crate) actor_device_id: String,
    pub(crate) actor_channel: Option<String>,
    pub(crate) source: String,
}

/// Outcome from admitting an input into a session queue.
#[derive(Debug, Clone)]
pub(crate) struct SessionQueueAdmissionOutcome {
    pub(crate) queued_input: OrchestratorQueuedInputRecord,
    pub(crate) decision: SessionQueueDecision,
    pub(crate) observed_queue_depth: u64,
}

/// Live-tunable memory subsystem limits, auto-injection policy, and retention
/// policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MemoryRuntimeConfig {
    pub max_item_bytes: usize,
    pub max_item_tokens: usize,
    pub auto_inject_enabled: bool,
    pub auto_inject_max_items: usize,
    pub default_ttl_ms: Option<i64>,
    pub retention_max_entries: Option<usize>,
    pub retention_max_bytes: Option<u64>,
    pub retention_ttl_days: Option<u32>,
    pub retention_vacuum_schedule: String,
}

/// Live-tunable learning/reflection pipeline settings; confidence thresholds
/// are expressed in basis points (10000 = 1.0).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LearningRuntimeConfig {
    pub enabled: bool,
    pub sampling_percent: u8,
    pub cooldown_ms: i64,
    pub budget_tokens: u64,
    pub max_candidates_per_run: usize,
    pub durable_fact_review_min_confidence_bps: u16,
    pub durable_fact_auto_write_threshold_bps: u16,
    pub preference_review_min_confidence_bps: u16,
    pub procedure_min_occurrences: usize,
    pub procedure_review_min_confidence_bps: u16,
}

/// Egress policy for the `http_fetch` tool: timeouts, size caps,
/// redirect/header/content-type allowlists, credential vault-ref allowlist,
/// and response caching.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HttpFetchRuntimeConfig {
    pub allow_private_targets: bool,
    pub connect_timeout_ms: u64,
    pub request_timeout_ms: u64,
    pub max_response_bytes: usize,
    pub allow_redirects: bool,
    pub max_redirects: usize,
    pub allowed_content_types: Vec<String>,
    pub allowed_request_headers: Vec<String>,
    pub allowed_credential_vault_refs: Vec<String>,
    pub cache_enabled: bool,
    pub cache_ttl_ms: u64,
    pub max_cache_entries: usize,
}

/// Connection settings and response size caps for the external browser
/// service (`palyra-browserd`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BrowserServiceRuntimeConfig {
    pub enabled: bool,
    pub endpoint: String,
    pub auth_token: Option<String>,
    pub connect_timeout_ms: u64,
    pub request_timeout_ms: u64,
    pub max_screenshot_bytes: usize,
    pub max_title_bytes: usize,
}

/// Canvas host limits: enablement, public base URL, token TTL, and
/// state/bundle/update-rate caps.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CanvasHostRuntimeConfig {
    pub enabled: bool,
    pub public_base_url: String,
    pub token_ttl_ms: u64,
    pub max_state_bytes: usize,
    pub max_bundle_bytes: usize,
    pub max_assets_per_bundle: usize,
    pub max_updates_per_minute: usize,
}

impl Default for MemoryRuntimeConfig {
    fn default() -> Self {
        Self {
            max_item_bytes: MAX_MEMORY_ITEM_BYTES,
            max_item_tokens: MAX_MEMORY_ITEM_TOKENS,
            auto_inject_enabled: true,
            auto_inject_max_items: 3,
            default_ttl_ms: Some(30 * 24 * 60 * 60 * 1_000),
            retention_max_entries: None,
            retention_max_bytes: None,
            retention_ttl_days: None,
            retention_vacuum_schedule: "0 0 * * 0".to_owned(),
        }
    }
}

impl Default for LearningRuntimeConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            sampling_percent: 100,
            cooldown_ms: 5 * 60 * 1_000,
            budget_tokens: 1_200,
            max_candidates_per_run: 24,
            durable_fact_review_min_confidence_bps: 7_500,
            durable_fact_auto_write_threshold_bps: 9_000,
            preference_review_min_confidence_bps: 8_000,
            procedure_min_occurrences: 2,
            procedure_review_min_confidence_bps: 8_500,
        }
    }
}

/// Resolved tool approval decision as consumed by the run stream, including
/// the scope and TTL that drive decision caching.
#[derive(Debug, Clone)]
pub(crate) struct ToolApprovalOutcome {
    pub(crate) approval_id: String,
    pub(crate) approved: bool,
    pub(crate) reason: String,
    pub(crate) decision: ApprovalDecision,
    pub(crate) decision_scope: ApprovalDecisionScope,
    pub(crate) decision_scope_ttl_ms: Option<i64>,
}

#[derive(Debug, Clone)]
struct CachedToolApprovalDecision {
    approval_id: String,
    approved: bool,
    reason: String,
    decision: ApprovalDecision,
    decision_scope: ApprovalDecisionScope,
    expires_at_unix_ms: Option<i64>,
}

/// Session-scoped cache of resolved approval decisions.
///
/// `generations` counts cache invalidations per session prefix so that a
/// decision resolved before an invalidation cannot be written back afterwards
/// (see [`GatewayRuntimeState::remember_tool_approval_if_generation`]).
#[derive(Debug, Default)]
struct ToolApprovalCacheState {
    decisions: HashMap<String, CachedToolApprovalDecision>,
    generations: HashMap<String, u64>,
}

fn tool_approval_cache_generation(cache: &ToolApprovalCacheState, key_prefix: &str) -> u64 {
    cache.generations.get(key_prefix).copied().unwrap_or(0)
}

fn bump_tool_approval_cache_generation(cache: &mut ToolApprovalCacheState, key_prefix: &str) {
    let generation = cache.generations.entry(key_prefix.to_owned()).or_insert(0);
    *generation = generation.saturating_add(1);
}

/// Cached `http_fetch` tool response with an absolute expiry.
#[derive(Debug, Clone)]
pub(crate) struct CachedHttpFetchEntry {
    pub(crate) expires_at_unix_ms: i64,
    pub(crate) output_json: Vec<u8>,
}

/// Cached memory search hits; `expires_at_unix_ms` is the earliest TTL among
/// the hits so the cache never serves an already-expired item.
#[derive(Debug, Clone)]
pub(crate) struct CachedMemorySearchEntry {
    pub(crate) hits: Vec<MemorySearchHit>,
    pub(crate) expires_at_unix_ms: Option<i64>,
}

/// Memory search hits plus retrieval branch diagnostics for observability.
#[derive(Debug, Clone)]
pub(crate) struct MemorySearchOutcome {
    pub(crate) hits: Vec<MemorySearchHit>,
    pub(crate) diagnostics: RetrievalBranchDiagnostics,
}

/// Workspace document search hits plus retrieval branch diagnostics.
#[derive(Debug, Clone)]
pub(crate) struct WorkspaceSearchOutcome {
    pub(crate) hits: Vec<WorkspaceSearchHit>,
    pub(crate) diagnostics: RetrievalBranchDiagnostics,
}

/// Skill attribution attached to a tool call (skill id plus optional version).
#[derive(Debug, Clone)]
pub(crate) struct ToolSkillContext {
    pub(crate) skill_id: String,
    pub(crate) version: Option<String>,
}

impl ToolSkillContext {
    /// Creates a skill attribution from id and optional version.
    pub(crate) fn new(skill_id: String, version: Option<String>) -> Self {
        Self { skill_id, version }
    }

    /// The skill identifier.
    pub(crate) fn skill_id(&self) -> &str {
        self.skill_id.as_str()
    }

    /// The skill version, when one was specified.
    pub(crate) fn version(&self) -> Option<&str> {
        self.version.as_deref()
    }
}

/// Result of executing one approved tool proposal inside the run stream;
/// `Terminal` preserves the durable terminal state that won while the tool was
/// in flight.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum RunStreamToolExecutionOutcome {
    Completed {
        proposal_id: String,
        tool_name: String,
        input_json: Vec<u8>,
        outcome: crate::tool_protocol::ToolExecutionOutcome,
    },
    Suspended,
    Terminal(RunLifecycleState),
}

/// Single canvas bundle asset: content type plus raw body bytes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CanvasAssetRecord {
    pub(crate) content_type: String,
    pub(crate) body: Vec<u8>,
}

/// Validated canvas bundle: assets keyed by normalized path, a content hash,
/// and the gateway-issued signature binding it to canvas/principal/session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CanvasBundleRecord {
    pub(crate) bundle_id: String,
    pub(crate) entrypoint_path: String,
    pub(crate) assets: HashMap<String, CanvasAssetRecord>,
    pub(crate) sha256: String,
    pub(crate) signature: String,
}

/// In-memory canvas state: current state JSON/version, bundle, parent-origin
/// allowlist, expiry, and the rolling per-minute update timestamps that back
/// the update rate limit.
#[derive(Debug, Clone)]
pub(crate) struct CanvasRecord {
    pub(crate) canvas_id: String,
    pub(crate) session_id: String,
    pub(crate) principal: String,
    pub(crate) state_version: u64,
    pub(crate) state_schema_version: u64,
    pub(crate) state_json: Vec<u8>,
    pub(crate) bundle: CanvasBundleRecord,
    pub(crate) allowed_parent_origins: Vec<String>,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) updated_at_unix_ms: i64,
    pub(crate) expires_at_unix_ms: i64,
    pub(crate) closed: bool,
    pub(crate) close_reason: Option<String>,
    pub(crate) update_timestamps_unix_ms: VecDeque<i64>,
}

/// Rendered HTML shell served at `/canvas/v1/frame/{canvas_id}` together with
/// the CSP header to attach to the response.
#[derive(Debug, Clone, Serialize)]
pub struct CanvasFrameDocument {
    pub canvas_id: String,
    pub html: String,
    pub csp: String,
    pub expires_at_unix_ms: i64,
}

/// Canvas asset payload plus the CSP header to serve with it.
#[derive(Debug, Clone)]
pub struct CanvasAssetResponse {
    pub content_type: String,
    pub body: Vec<u8>,
    pub csp: String,
}

/// Canvas state document returned by the HTTP polling endpoint.
#[derive(Debug, Clone, Serialize)]
pub struct CanvasStateResponse {
    pub canvas_id: String,
    pub state_version: u64,
    pub state_schema_version: u64,
    pub state: Value,
    pub closed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub close_reason: Option<String>,
    pub expires_at_unix_ms: i64,
}

/// Client-facing handle to an active canvas: frame/runtime URLs plus a signed
/// auth token bound to the canvas, principal, and session.
#[derive(Debug, Clone, Serialize)]
pub struct CanvasRuntimeDescriptor {
    pub canvas_id: String,
    pub frame_url: String,
    pub runtime_url: String,
    pub auth_token: String,
    pub expires_at_unix_ms: i64,
}

/// Maximum patch records fetched when assembling a canvas patch history
/// response (further trimmed by the byte budget below).
pub(crate) const CANVAS_PATCH_HISTORY_RESPONSE_ROW_LIMIT: usize = 100;
const CANVAS_PATCH_HISTORY_RESPONSE_BYTE_LIMIT: usize = 4 * 1024 * 1024;
const CANVAS_PATCH_HISTORY_RESPONSE_RECORD_OVERHEAD: usize = 256;

/// Signed canvas token claims: canvas/principal/session scope, validity
/// window, and a nonce making every issued token unique.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CanvasTokenPayload {
    pub(crate) canvas_id: String,
    pub(crate) principal: String,
    pub(crate) session_id: String,
    issued_at_unix_ms: i64,
    pub(crate) expires_at_unix_ms: i64,
    nonce: String,
}

/// Fixed-window request counter for one principal's vault operations.
#[derive(Debug, Clone, Copy)]
pub(crate) struct VaultRateLimitEntry {
    window_started_at: Instant,
    requests_in_window: u32,
}

/// Journal storage settings surfaced in status snapshots.
#[derive(Debug, Clone)]
pub struct GatewayJournalConfigSnapshot {
    pub db_path: PathBuf,
    pub hash_chain_enabled: bool,
}

/// Externally constructed collaborators injected into
/// [`GatewayRuntimeState::new_with_provider`].
#[rustfmt::skip]
pub struct GatewayRuntimeDependencies { pub model_provider: Arc<dyn ModelProvider>, pub vault: Arc<Vault>, pub auth_profile_registry: Option<Arc<AuthProfileRegistry>>, pub auth_runtime: Option<Arc<AuthRuntimeState>>, pub agent_registry: AgentRegistry, pub tool_posture_registry: ToolPostureRegistry, pub retrieval_backend: Arc<dyn RetrievalBackend>, pub external_retrieval_index: Arc<ExternalRetrievalRuntime>, pub conversation_bindings: ConversationBindingStore, pub fault_injection: QaFaultRuntime, pub(crate) runtime_kernel_dispatcher: Arc<crate::application::runtime_kernel_v2::dispatcher::RuntimeKernelDispatcher>, pub(crate) managed_coding_services: Option<Arc<crate::application::managed_coding_services::ManagedCodingRuntimeServices>> }

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ProviderHealthAuthorityKey {
    component_id: RuntimeInstanceId,
    generation: RuntimeGeneration,
}

impl From<&ProviderAttemptHealthAuthority> for ProviderHealthAuthorityKey {
    fn from(authority: &ProviderAttemptHealthAuthority) -> Self {
        Self { component_id: authority.component_id.clone(), generation: authority.generation }
    }
}

#[derive(Clone)]
struct ModelProviderRuntime {
    provider: Arc<dyn ModelProvider>,
    configuration_epoch: RuntimeGeneration,
    health_authority_by_provider: BTreeMap<String, ProviderAttemptHealthAuthority>,
}

/// Maps internal lease preview reason codes to operator-facing wording.
/// Internal codes must not leak into user-visible errors (pinned by tests).
fn provider_lease_pressure_reason(preview: &ProviderLeasePreviewSnapshot) -> &'static str {
    match preview.reason.as_deref() {
        Some("shared_capacity_exhausted") => "shared provider capacity is exhausted",
        Some("foreground_waiters_present") => "foreground work is already queued",
        Some("foreground_capacity_reserved") => "foreground provider capacity is reserved",
        Some(reason) if reason.starts_with("credential_feedback:") => {
            "provider credential is cooling down after a provider error"
        }
        _ => "provider capacity is busy",
    }
}

fn provider_lease_deferred_status(
    lease_context: &ProviderLeaseExecutionContext,
    preview: ProviderLeasePreviewSnapshot,
) -> Status {
    let reason = provider_lease_pressure_reason(&preview);
    Status::resource_exhausted(format!(
        "model provider capacity is busy for {} on provider '{}' ({reason}); retry shortly or reduce concurrent agent runs",
        lease_context.task_label, lease_context.provider_id,
    ))
}

fn provider_lease_timeout_status(
    waited_ms: u64,
    lease_context: &ProviderLeaseExecutionContext,
    preview: ProviderLeasePreviewSnapshot,
) -> Status {
    let reason = provider_lease_pressure_reason(&preview);
    Status::resource_exhausted(format!(
        "model provider capacity is busy for {} on provider '{}' ({reason}); queued for {waited_ms} ms before timing out; retry shortly or reduce concurrent agent runs",
        lease_context.task_label, lease_context.provider_id,
    ))
}

#[derive(Debug)]
enum ProviderAttemptFeedback {
    Success(ProviderAttemptBinding),
    Failure(ProviderAttemptBinding, ProviderError),
}

#[derive(Debug, Clone)]
enum GatewayProviderAttemptRuntimeAuthority {
    Run(JournalProviderAttemptRuntimeAuthority),
    Configuration(JournalProviderConfigurationAttemptRuntimeAuthority),
}

/// Cancellation-safe owner of one durably started provider effect.
///
/// Provider futures are cancellation points. If an outer deadline drops the
/// future after start evidence was committed, this guard closes the exact
/// attempt as outcome-unknown before releasing its authority. Successful,
/// failed, and stale settlements disarm the guard explicitly.
struct GatewayProviderAttemptRuntimeAuthorityGuard {
    runtime_state: Arc<GatewayRuntimeState>,
    authority: GatewayProviderAttemptRuntimeAuthority,
    provider_id: String,
    model_id: String,
    settled: Arc<AtomicBool>,
}

#[cfg(test)]
impl Clone for GatewayProviderAttemptRuntimeAuthorityGuard {
    fn clone(&self) -> Self {
        Self {
            runtime_state: Arc::clone(&self.runtime_state),
            authority: self.authority.clone(),
            provider_id: self.provider_id.clone(),
            model_id: self.model_id.clone(),
            settled: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl Drop for GatewayProviderAttemptRuntimeAuthorityGuard {
    fn drop(&mut self) {
        if self.settled.swap(true, Ordering::AcqRel) {
            return;
        }
        let result = settle_gateway_provider_attempt(
            &self.runtime_state,
            self.authority.clone(),
            self.provider_id.clone(),
            self.model_id.clone(),
            "outcome_unknown",
            Some("provider_future_cancelled_before_settlement".to_owned()),
        );
        if let Err(error) = result {
            self.runtime_state.counters.journal_persist_failures.fetch_add(1, Ordering::Relaxed);
            warn!(
                provider_id = self.provider_id.as_str(),
                model_id = self.model_id.as_str(),
                error = %error,
                "failed to settle cancelled provider attempt as outcome-unknown"
            );
        }
    }
}

fn settle_gateway_provider_attempt(
    runtime_state: &GatewayRuntimeState,
    authority: GatewayProviderAttemptRuntimeAuthority,
    provider_id: String,
    model_id: String,
    outcome: &str,
    error_class: Option<String>,
) -> Result<ProviderAttemptCompletionDisposition, JournalError> {
    match authority {
        GatewayProviderAttemptRuntimeAuthority::Run(authority) => runtime_state
            .journal_store
            .complete_provider_attempt(&ProviderAttemptCompletionRequest {
                authority,
                provider_id,
                model_id,
                outcome: outcome.to_owned(),
                error_class,
            })
            .map(|outcome| match outcome {
                ProviderAttemptCompletionOutcome::Appended { .. } => {
                    ProviderAttemptCompletionDisposition::Appended
                }
                ProviderAttemptCompletionOutcome::AlreadyAppended { .. } => {
                    ProviderAttemptCompletionDisposition::AlreadyAppended
                }
                ProviderAttemptCompletionOutcome::StaleSuppressed => {
                    ProviderAttemptCompletionDisposition::StaleSuppressed
                }
            }),
        GatewayProviderAttemptRuntimeAuthority::Configuration(authority) => runtime_state
            .journal_store
            .complete_provider_configuration_attempt(
                &ProviderConfigurationAttemptCompletionRequest {
                    authority,
                    provider_id,
                    model_id,
                    outcome: outcome.to_owned(),
                    error_class,
                },
            )
            .map(|outcome| match outcome {
                ProviderConfigurationAttemptCompletionOutcome::Appended => {
                    ProviderAttemptCompletionDisposition::Appended
                }
                ProviderConfigurationAttemptCompletionOutcome::AlreadyAppended => {
                    ProviderAttemptCompletionDisposition::AlreadyAppended
                }
                ProviderConfigurationAttemptCompletionOutcome::StaleSuppressed => {
                    ProviderAttemptCompletionDisposition::StaleSuppressed
                }
            }),
    }
}

fn provider_probe_error_is_ambiguous(error: &ProviderError) -> bool {
    matches!(
        error.classification().class,
        ProviderFailureClass::TransientUpstream
            | ProviderFailureClass::ProviderUnavailable
            | ProviderFailureClass::NetworkUnavailable
            | ProviderFailureClass::ProviderTimeout
    )
}

fn provider_failure_affects_candidate_health(error: &ProviderError) -> bool {
    !matches!(
        error.classification().class,
        ProviderFailureClass::ContextOverflow | ProviderFailureClass::ContextWindowExceeded
    )
}

fn credential_attempt_admission_error(
    safe_message: &str,
    reason_code: &str,
    operator_action_required: bool,
) -> ProviderAttemptAdmissionError {
    ProviderAttemptAdmissionError::HealthBlocked {
        safe_message: safe_message.to_owned(),
        reason_code: reason_code.to_owned(),
        retry_after_ms: None,
        operator_action_required,
    }
}

/// Host service that composes auth selection, OAuth refresh, and vault access.
#[derive(Clone)]
struct CredentialAvailabilityService {
    auth_runtime: Arc<AuthRuntimeState>,
    vault: Arc<Vault>,
}

impl CredentialAvailabilityService {
    fn new(auth_runtime: Arc<AuthRuntimeState>, vault: Arc<Vault>) -> Self {
        Self { auth_runtime, vault }
    }

    async fn select_attempt(
        &self,
        provider_id: &str,
        configured_credential_id: &str,
        excluded_profile_ids: Vec<String>,
    ) -> Result<
        Option<(String, CredentialAttemptBinding, CredentialSelectionReport)>,
        ProviderAttemptAdmissionError,
    > {
        let Some(configured_profile_id) =
            auth_profile_id_from_credential_id(configured_credential_id).map(str::to_owned)
        else {
            return Ok(None);
        };
        let registry = Arc::clone(&self.auth_runtime.registry);
        let configured_profile_id_for_lookup = configured_profile_id.clone();
        let configured_profile = tokio::task::spawn_blocking(move || {
            registry.get_profile(configured_profile_id_for_lookup.as_str())
        })
        .await
        .map_err(|_| {
            credential_attempt_admission_error(
                "credential profile lookup worker failed",
                "credential_selection_worker_failed",
                false,
            )
        })?
        .map_err(|_| {
            credential_attempt_admission_error(
                "credential profile registry is unavailable",
                "credential_selection_registry_unavailable",
                false,
            )
        })?
        .ok_or_else(|| {
            credential_attempt_admission_error(
                "configured credential profile does not exist",
                "credential_selection_profile_missing",
                true,
            )
        })?;

        let agent_id = match &configured_profile.scope {
            AuthProfileScope::Global => None,
            AuthProfileScope::Agent { agent_id } => Some(agent_id.clone()),
        };
        let request = AuthProfileSelectionRequest {
            provider: Some(configured_profile.provider.clone()),
            agent_id,
            explicit_profile_order: Vec::new(),
            allowed_credential_types: Vec::new(),
            policy_denied_profile_ids: excluded_profile_ids,
        };
        let mut selection = self.select_profiles(request.clone()).await?;
        if selection.selected_profile_id.is_none() {
            let refresh_candidates = selection
                .candidates
                .iter()
                .filter(|candidate| {
                    candidate.credential_type == AuthCredentialType::Oauth
                        && candidate.token_expiry_state == AuthTokenExpiryState::Expired
                        && candidate.eligibility == AuthProfileEligibility::Expired
                })
                .map(|candidate| candidate.profile_id.clone())
                .collect::<Vec<_>>();
            for profile_id in refresh_candidates {
                let outcome = self
                    .auth_runtime
                    .refresh_oauth_profile(profile_id, Arc::clone(&self.vault))
                    .await
                    .map_err(|_| {
                        credential_attempt_admission_error(
                            "OAuth credential refresh failed",
                            "credential_selection_refresh_failed",
                            false,
                        )
                    })?;
                if matches!(outcome.kind, OAuthRefreshOutcomeKind::Failed) {
                    continue;
                }
            }
            selection = self.select_profiles(request).await?;
        }

        let selected_profile_id = selection.selected_profile_id.clone().ok_or_else(|| {
            credential_attempt_admission_error(
                "no eligible credential profile is available",
                "credential_selection_exhausted",
                false,
            )
        })?;
        let selected = selection
            .candidates
            .iter()
            .find(|candidate| candidate.profile_id == selected_profile_id)
            .ok_or_else(|| {
                credential_attempt_admission_error(
                    "credential selection result is inconsistent",
                    "credential_selection_inconsistent",
                    true,
                )
            })?;
        let profile_id_sha256 = crate::sha256_hex(selected_profile_id.as_bytes());
        let attempt = CredentialAttemptBinding {
            profile_id: selected_profile_id.clone(),
            profile_id_sha256: profile_id_sha256.clone(),
            auth_class: selected.credential_type,
            selection_reason: selected.reason_code.clone(),
        };
        let report = CredentialSelectionReport {
            schema_version: 1,
            selected: Some(attempt.clone()),
            reason_code: selection.reason_code,
            considered_profile_hashes: selection
                .candidates
                .iter()
                .map(|candidate| crate::sha256_hex(candidate.profile_id.as_bytes()))
                .collect(),
            generated_at_unix_ms: selection.generated_at_unix_ms,
        };
        Ok(Some((format!("auth-profile:{provider_id}:{selected_profile_id}"), attempt, report)))
    }

    async fn select_profiles(
        &self,
        request: AuthProfileSelectionRequest,
    ) -> Result<palyra_auth::AuthProfileSelectionResult, ProviderAttemptAdmissionError> {
        let registry = Arc::clone(&self.auth_runtime.registry);
        let vault = Arc::clone(&self.vault);
        tokio::task::spawn_blocking(move || registry.select_auth_profile(vault.as_ref(), request))
            .await
            .map_err(|_| {
                credential_attempt_admission_error(
                    "credential selection worker failed",
                    "credential_selection_worker_failed",
                    false,
                )
            })?
            .map_err(|_| {
                credential_attempt_admission_error(
                    "credential selection failed",
                    "credential_selection_failed",
                    false,
                )
            })
    }

    async fn materialize(
        &self,
        attempt: CredentialAttemptBinding,
    ) -> Result<ProviderCredentialLease, ProviderAttemptAdmissionError> {
        let registry = Arc::clone(&self.auth_runtime.registry);
        let vault = Arc::clone(&self.vault);
        tokio::task::spawn_blocking(move || {
            let profile = registry
                .get_profile(attempt.profile_id.as_str())
                .map_err(|_| "registry_unavailable")?
                .ok_or("profile_missing")?;
            if profile.credential.credential_type() != attempt.auth_class {
                return Err("credential_class_changed");
            }
            let vault_ref = match profile.credential {
                AuthCredential::ApiKey { api_key_vault_ref } => api_key_vault_ref,
                AuthCredential::Oauth { access_token_vault_ref, .. } => access_token_vault_ref,
            };
            let parsed = VaultRef::parse(vault_ref.as_str()).map_err(|_| "vault_ref_invalid")?;
            let secret = vault
                .get_secret(&parsed.scope, parsed.key.as_str())
                .map_err(|_| "secret_unavailable")?;
            let secret_text =
                std::str::from_utf8(secret.as_slice()).map_err(|_| "secret_invalid_utf8")?;
            if secret_text.trim().is_empty() {
                return Err("secret_empty");
            }
            Ok(ProviderCredentialLease::new(attempt.auth_class, SensitiveBytes::new(secret)))
        })
        .await
        .map_err(|_| {
            credential_attempt_admission_error(
                "credential materialization worker failed",
                "credential_materialization_worker_failed",
                false,
            )
        })?
        .map_err(|reason_code| {
            credential_attempt_admission_error(
                "selected credential could not be materialized",
                reason_code,
                false,
            )
        })
    }
}

#[derive(Clone)]
struct GatewayProviderAttemptAdmission {
    runtime_state: Arc<GatewayRuntimeState>,
    lease_context: ProviderLeaseExecutionContext,
    expected_configuration_epoch: RuntimeGeneration,
    health_authority_by_provider: Arc<BTreeMap<String, ProviderAttemptHealthAuthority>>,
    feedback: Arc<Mutex<Vec<ProviderAttemptFeedback>>>,
    attempted_profile_ids: Arc<Mutex<BTreeMap<String, BTreeSet<String>>>>,
    #[cfg(test)]
    fail_health_observation_once: Option<Arc<AtomicBool>>,
}

#[derive(Clone)]
struct GatewayProviderProbeAdmission {
    runtime_state: Arc<GatewayRuntimeState>,
    expected_configuration_epoch: RuntimeGeneration,
    health_authority_by_provider: Arc<BTreeMap<String, ProviderAttemptHealthAuthority>>,
    probe_lease: HealthProbeLeaseV1,
}

fn bind_provider_attempt(
    health_authority_by_provider: &BTreeMap<String, ProviderAttemptHealthAuthority>,
    provider_id: &str,
    credential_id: &str,
    model_id: &str,
) -> Result<ProviderAttemptBinding, ProviderAttemptAdmissionError> {
    let authority = health_authority_by_provider.get(provider_id).cloned().ok_or_else(|| {
        ProviderAttemptAdmissionError::HealthBlocked {
            safe_message: "provider candidate health authority is missing".to_owned(),
            reason_code: "provider_attempt_admission_health_authority_missing".to_owned(),
            retry_after_ms: None,
            operator_action_required: true,
        }
    })?;
    let attempt_id = RuntimeAttemptId::parse(Ulid::new().to_string().as_str()).map_err(|_| {
        ProviderAttemptAdmissionError::RuntimeAuthority {
            safe_message: "provider attempt identity is invalid".to_owned(),
            reason_code: "provider_attempt_identity_invalid".to_owned(),
            retryable: false,
        }
    })?;
    Ok(ProviderAttemptBinding {
        attempt_id,
        provider_id: provider_id.to_owned(),
        credential_id: credential_id.to_owned(),
        model_id: model_id.to_owned(),
        health_authority: authority,
        credential_attempt: None,
        credential_selection: None,
    })
}

impl GatewayProviderAttemptAdmission {
    fn candidate_lease_context(
        &self,
        binding: &ProviderAttemptBinding,
    ) -> ProviderLeaseExecutionContext {
        ProviderLeaseExecutionContext {
            provider_id: binding.provider_id.clone(),
            credential_id: binding.credential_id.clone(),
            priority: self.lease_context.priority,
            task_label: self.lease_context.task_label.clone(),
            max_wait_ms: self.lease_context.max_wait_ms,
            session_id: self.lease_context.session_id.clone(),
            run_id: self.lease_context.run_id.clone(),
            runtime_authority: self.lease_context.runtime_authority.clone(),
            diagnostic_scope_id: self.lease_context.diagnostic_scope_id.clone(),
        }
    }

    fn record_shared_health_observation(
        &self,
        binding: &ProviderAttemptBinding,
        succeeded: bool,
        reason_code: &str,
    ) {
        #[cfg(test)]
        let injected_failure = self
            .fail_health_observation_once
            .as_ref()
            .is_some_and(|flag| flag.swap(false, Ordering::SeqCst));
        #[cfg(not(test))]
        let injected_failure = false;
        let result = if injected_failure {
            Err(JournalError::InvalidArgument(
                "injected provider health observation failure".to_owned(),
            ))
        } else {
            self.runtime_state.journal_store.record_runtime_health_observation(
                &RuntimeHealthObservationRequest {
                    component_id: binding.health_authority.component_id.clone(),
                    expected_generation: binding.health_authority.generation,
                    succeeded,
                    reason_code: reason_code.to_owned(),
                    observed_at_unix_ms: current_unix_ms(),
                },
            )
        };
        if let Err(error) = result {
            self.runtime_state.latch_provider_health_authority(&binding.health_authority);
            self.runtime_state.counters.journal_persist_failures.fetch_add(1, Ordering::Relaxed);
            warn!(
                provider_id = binding.provider_id.as_str(),
                generation = binding.health_authority.generation.get(),
                error = %error,
                "failed to record exact provider health observation; latched provider authority"
            );
        }
    }

    fn apply_feedback(&self, feedback: ProviderAttemptFeedback) {
        match feedback {
            ProviderAttemptFeedback::Success(binding) => {
                self.record_shared_health_observation(
                    &binding,
                    true,
                    "runtime.health.provider_call_succeeded",
                );
                let attribution = provider_credential_attribution_from_parts(
                    binding.provider_id.as_str(),
                    binding.credential_id.as_str(),
                );
                self.runtime_state.record_provider_credential_feedback(
                    ProviderCredentialFeedbackRequest {
                        provider_id: binding.provider_id,
                        credential_id: binding.credential_id,
                        kind: ProviderCredentialFeedbackKind::Success,
                        retry_after_ms: None,
                        reason: "provider candidate succeeded".to_owned(),
                        observed_at_unix_ms: current_unix_ms(),
                    },
                );
                self.runtime_state.record_auth_profile_success_for_attribution(&attribution);
            }
            ProviderAttemptFeedback::Failure(binding, error) => {
                // Context pressure belongs to this request, not the provider or
                // credential. Penalizing shared health would block the compacted retry.
                if !provider_failure_affects_candidate_health(&error) {
                    return;
                }
                self.record_shared_health_observation(
                    &binding,
                    false,
                    "runtime.health.provider_call_failed",
                );
                let candidate = self.candidate_lease_context(&binding);
                self.runtime_state.record_auth_profile_failure_for_lease(&candidate, &error);
                self.runtime_state.record_provider_lease_feedback_for_error(&candidate, &error);
            }
        }
    }

    fn apply_buffered_feedback(&self) -> bool {
        let _reload_guard = self
            .runtime_state
            .model_provider_reload_lock
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let feedback = {
            let mut guard = self.feedback.lock().unwrap_or_else(|error| error.into_inner());
            std::mem::take(&mut *guard)
        };
        if self
            .runtime_state
            .model_provider
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .configuration_epoch
            != self.expected_configuration_epoch
        {
            return false;
        }
        for entry in feedback {
            self.apply_feedback(entry);
        }
        true
    }

    fn check_candidate_health(
        &self,
        binding: &ProviderAttemptBinding,
    ) -> Result<(), ProviderAttemptAdmissionError> {
        if self.runtime_state.provider_health_authority_is_latched(&binding.health_authority) {
            return Err(ProviderAttemptAdmissionError::HealthBlocked {
                safe_message: "provider candidate health evidence is unavailable".to_owned(),
                reason_code: "provider_attempt_admission_health_observation_unavailable".to_owned(),
                retry_after_ms: None,
                operator_action_required: true,
            });
        }
        let Some(health) = self
            .runtime_state
            .journal_store
            .runtime_component_health(binding.health_authority.component_id.as_str())
            .map_err(|_| ProviderAttemptAdmissionError::HealthBlocked {
                safe_message: "provider candidate health state is unavailable".to_owned(),
                reason_code: "provider_attempt_admission_health_unavailable".to_owned(),
                retry_after_ms: None,
                operator_action_required: false,
            })?
        else {
            return Err(ProviderAttemptAdmissionError::HealthBlocked {
                safe_message: "provider candidate health state is missing".to_owned(),
                reason_code: "provider_attempt_admission_health_missing".to_owned(),
                retry_after_ms: None,
                operator_action_required: true,
            });
        };
        if health.component_id != binding.health_authority.component_id
            || health.generation != binding.health_authority.generation
        {
            return Err(ProviderAttemptAdmissionError::HealthBlocked {
                safe_message: "provider candidate health authority is stale".to_owned(),
                reason_code: "provider_attempt_admission_health_generation_mismatch".to_owned(),
                retry_after_ms: None,
                operator_action_required: false,
            });
        }
        let now = current_unix_ms();
        let retry_after_ms = health
            .expires_at_unix_ms
            .and_then(|expiry| u64::try_from(expiry.saturating_sub(now).max(0)).ok());
        match health.ordinary_admission_decision(now) {
            RuntimeOrdinaryAdmissionDecision::Allowed => Ok(()),
            RuntimeOrdinaryAdmissionDecision::CooldownBlocked => {
                Err(ProviderAttemptAdmissionError::HealthBlocked {
                    safe_message: "provider candidate is cooling down".to_owned(),
                    reason_code: "provider_attempt_admission_health_cooldown".to_owned(),
                    retry_after_ms,
                    operator_action_required: false,
                })
            }
            RuntimeOrdinaryAdmissionDecision::ProbeRequired => {
                Err(ProviderAttemptAdmissionError::HealthBlocked {
                    safe_message: "provider candidate requires a bounded health probe".to_owned(),
                    reason_code: "provider_attempt_admission_probe_required".to_owned(),
                    retry_after_ms: None,
                    operator_action_required: false,
                })
            }
            RuntimeOrdinaryAdmissionDecision::ProbeInProgress => {
                Err(ProviderAttemptAdmissionError::HealthBlocked {
                    safe_message: "provider candidate probe is in progress".to_owned(),
                    reason_code: "provider_attempt_admission_probe_in_progress".to_owned(),
                    retry_after_ms: None,
                    operator_action_required: false,
                })
            }
            RuntimeOrdinaryAdmissionDecision::Quarantined => {
                Err(ProviderAttemptAdmissionError::HealthBlocked {
                    safe_message: "provider candidate is quarantined".to_owned(),
                    reason_code: "provider_attempt_admission_health_quarantined".to_owned(),
                    retry_after_ms: None,
                    operator_action_required: true,
                })
            }
            RuntimeOrdinaryAdmissionDecision::Disabled => {
                Err(ProviderAttemptAdmissionError::HealthBlocked {
                    safe_message: "provider candidate is disabled".to_owned(),
                    reason_code: "provider_attempt_admission_health_disabled".to_owned(),
                    retry_after_ms: None,
                    operator_action_required: true,
                })
            }
        }
    }
}

impl ProviderAttemptAdmission for GatewayProviderAttemptAdmission {
    fn prepare_attempt<'a>(
        &'a self,
        provider_id: &'a str,
        credential_id: &'a str,
        model_id: &'a str,
    ) -> ProviderAttemptPreparationFuture<'a> {
        Box::pin(async move {
            let Some(service) = self.runtime_state.credential_availability.as_ref() else {
                return self.bind_attempt(provider_id, credential_id, model_id);
            };
            let excluded_profile_ids = self
                .attempted_profile_ids
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .get(provider_id)
                .map(|profiles| profiles.iter().cloned().collect())
                .unwrap_or_default();
            let Some((selected_credential_id, attempt, report)) =
                service.select_attempt(provider_id, credential_id, excluded_profile_ids).await?
            else {
                return self.bind_attempt(provider_id, credential_id, model_id);
            };
            self.attempted_profile_ids
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .entry(provider_id.to_owned())
                .or_default()
                .insert(attempt.profile_id.clone());
            let mut binding = bind_provider_attempt(
                self.health_authority_by_provider.as_ref(),
                provider_id,
                selected_credential_id.as_str(),
                model_id,
            )?;
            binding.credential_attempt = Some(attempt);
            binding.credential_selection = Some(report);
            Ok(binding)
        })
    }

    fn bind_attempt(
        &self,
        provider_id: &str,
        credential_id: &str,
        model_id: &str,
    ) -> Result<ProviderAttemptBinding, ProviderAttemptAdmissionError> {
        bind_provider_attempt(
            self.health_authority_by_provider.as_ref(),
            provider_id,
            credential_id,
            model_id,
        )
    }

    fn check_eligibility(
        &self,
        binding: &ProviderAttemptBinding,
    ) -> Result<(), ProviderAttemptAdmissionError> {
        self.check_candidate_health(binding)
    }

    fn acquire<'a>(
        &'a self,
        binding: &'a ProviderAttemptBinding,
    ) -> ProviderAttemptPermitFuture<'a> {
        Box::pin(async move {
            self.check_candidate_health(binding)?;
            let candidate = self.candidate_lease_context(binding);
            let guard = self
                .runtime_state
                .provider_leases
                .acquire(ProviderLeaseAcquireRequest {
                    provider_id: candidate.provider_id.as_str(),
                    credential_id: candidate.credential_id.as_str(),
                    priority: candidate.priority,
                    task_label: candidate.task_label.as_str(),
                    max_wait_ms: candidate.max_wait_ms,
                    session_id: candidate.session_id.as_deref(),
                    run_id: candidate.run_id.as_deref(),
                })
                .await
                .map_err(|error| match error {
                    ProviderLeaseAcquireError::Deferred(preview) => {
                        let retry_after_ms = preview.retry_after_ms;
                        ProviderAttemptAdmissionError::Deferred {
                            safe_message: provider_lease_deferred_status(&candidate, preview)
                                .message()
                                .to_owned(),
                            retry_after_ms,
                        }
                    }
                    ProviderLeaseAcquireError::TimedOut { waited_ms, preview } => {
                        let retry_after_ms = preview.retry_after_ms;
                        ProviderAttemptAdmissionError::TimedOut {
                            safe_message: provider_lease_timeout_status(
                                waited_ms, &candidate, preview,
                            )
                            .message()
                            .to_owned(),
                            waited_ms,
                            retry_after_ms,
                        }
                    }
                })?;
            if let Err(error) = self.check_candidate_health(binding) {
                drop(guard);
                return Err(error);
            }
            Ok(Box::new(guard) as Box<dyn ProviderAttemptPermit>)
        })
    }

    fn materialize_credential<'a>(
        &'a self,
        binding: &'a ProviderAttemptBinding,
    ) -> ProviderCredentialLeaseFuture<'a> {
        Box::pin(async move {
            let Some(attempt) = binding.credential_attempt.clone() else {
                return Ok(None);
            };
            let service = self.runtime_state.credential_availability.as_ref().ok_or_else(|| {
                credential_attempt_admission_error(
                    "credential availability service is unavailable",
                    "credential_materialization_service_unavailable",
                    true,
                )
            })?;
            service.materialize(attempt).await.map(Some)
        })
    }

    fn record_started<'a>(
        &'a self,
        binding: &'a ProviderAttemptBinding,
    ) -> ProviderAttemptStartFuture<'a> {
        Box::pin(async move {
            let session_id = self.lease_context.session_id.clone();
            let run_id = self.lease_context.run_id.clone();
            if session_id.is_none() && run_id.is_some() {
                return Err(ProviderAttemptAdmissionError::RuntimeAuthority {
                    safe_message: "provider run authority is missing its owning session".to_owned(),
                    reason_code: "provider_attempt_runtime_session_missing".to_owned(),
                    retryable: false,
                });
            }
            let runtime_state = Arc::clone(&self.runtime_state);
            let expected_configuration_epoch = self.expected_configuration_epoch;
            let runtime_authority = self.lease_context.runtime_authority.clone();
            let attempt_id = binding.attempt_id.clone();
            let provider_id = binding.provider_id.clone();
            let model_id = binding.model_id.clone();
            let credential = binding.credential_attempt.as_ref().map(|attempt| {
                ProviderCredentialAttemptMetadata {
                    profile_id_sha256: attempt.profile_id_sha256.clone(),
                    auth_class: match attempt.auth_class {
                        AuthCredentialType::ApiKey => "api_key",
                        AuthCredentialType::Oauth => "oauth",
                    }
                    .to_owned(),
                    selection_reason: attempt.selection_reason.clone(),
                }
            });
            let authority = tokio::task::spawn_blocking(move || {
                let _reload_guard = runtime_state
                    .model_provider_reload_lock
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                if runtime_state
                    .model_provider
                    .read()
                    .unwrap_or_else(|error| error.into_inner())
                    .configuration_epoch
                    != expected_configuration_epoch
                {
                    return Err(ProviderAttemptAdmissionError::RuntimeAuthority {
                        safe_message: "provider attempt was superseded before effect start"
                            .to_owned(),
                        reason_code: "provider_attempt_runtime_superseded".to_owned(),
                        retryable: true,
                    });
                }
                let authority = match (session_id, run_id) {
                    (Some(session_id), Some(run_id)) => runtime_state
                        .journal_store
                        .start_provider_attempt(&ProviderAttemptStartRequest {
                            session_id,
                            run_id,
                            attempt_id,
                            expected_configuration_epoch,
                            runtime_authority,
                            provider_id,
                            model_id,
                            credential,
                        })
                        .map(GatewayProviderAttemptRuntimeAuthority::Run),
                    (_, None) => runtime_state
                        .journal_store
                        .start_provider_configuration_attempt(
                            &ProviderConfigurationAttemptStartRequest {
                                attempt_id,
                                expected_configuration_epoch,
                                provider_id,
                                model_id,
                            },
                        )
                        .map(GatewayProviderAttemptRuntimeAuthority::Configuration),
                    (None, Some(_)) => unreachable!(
                        "run identity without an owning session was rejected before persistence"
                    ),
                };
                authority.map_err(|error| {
                    let stale = matches!(error, JournalError::InvalidArgument(_));
                    ProviderAttemptAdmissionError::RuntimeAuthority {
                        safe_message: if stale {
                            "provider attempt was superseded before effect start".to_owned()
                        } else {
                            "provider runtime start evidence could not be persisted".to_owned()
                        },
                        reason_code: if stale {
                            "provider_attempt_runtime_superseded".to_owned()
                        } else {
                            "provider_attempt_runtime_start_persist_failed".to_owned()
                        },
                        retryable: true,
                    }
                })
            })
            .await
            .map_err(|_| ProviderAttemptAdmissionError::RuntimeAuthority {
                safe_message: "provider runtime start persistence worker panicked".to_owned(),
                reason_code: "provider_attempt_runtime_start_worker_panicked".to_owned(),
                retryable: true,
            })??;
            Ok(Box::new(GatewayProviderAttemptRuntimeAuthorityGuard {
                runtime_state: Arc::clone(&self.runtime_state),
                authority,
                provider_id: binding.provider_id.clone(),
                model_id: binding.model_id.clone(),
                settled: Arc::new(AtomicBool::new(false)),
            }) as Box<dyn ProviderAttemptRuntimeAuthority>)
        })
    }

    fn record_success<'a>(
        &'a self,
        binding: &'a ProviderAttemptBinding,
        authority: Box<dyn ProviderAttemptRuntimeAuthority>,
    ) -> ProviderAttemptCompletionFuture<'a> {
        Box::pin(async move {
            let authority_guard = authority
                .as_ref()
                .as_any()
                .downcast_ref::<GatewayProviderAttemptRuntimeAuthorityGuard>()
                .ok_or_else(|| ProviderAttemptAdmissionError::RuntimeAuthority {
                    safe_message: "provider attempt completion authority is invalid".to_owned(),
                    reason_code: "provider_attempt_runtime_authority_invalid".to_owned(),
                    retryable: false,
                })?;
            let exact_authority = authority_guard.authority.clone();
            let settled = Arc::clone(&authority_guard.settled);
            let runtime_state = Arc::clone(&self.runtime_state);
            let provider_id = binding.provider_id.clone();
            let model_id = binding.model_id.clone();
            let outcome = tokio::task::spawn_blocking(move || {
                let outcome = match exact_authority {
                    GatewayProviderAttemptRuntimeAuthority::Run(authority) => runtime_state
                        .journal_store
                        .complete_provider_attempt(&ProviderAttemptCompletionRequest {
                            authority,
                            provider_id,
                            model_id,
                            outcome: "success".to_owned(),
                            error_class: None,
                        })
                        .map(|outcome| match outcome {
                            ProviderAttemptCompletionOutcome::Appended { .. } => {
                                ProviderAttemptCompletionDisposition::Appended
                            }
                            ProviderAttemptCompletionOutcome::AlreadyAppended { .. } => {
                                ProviderAttemptCompletionDisposition::AlreadyAppended
                            }
                            ProviderAttemptCompletionOutcome::StaleSuppressed => {
                                ProviderAttemptCompletionDisposition::StaleSuppressed
                            }
                        }),
                    GatewayProviderAttemptRuntimeAuthority::Configuration(authority) => {
                        runtime_state
                            .journal_store
                            .complete_provider_configuration_attempt(
                                &ProviderConfigurationAttemptCompletionRequest {
                                    authority,
                                    provider_id,
                                    model_id,
                                    outcome: "success".to_owned(),
                                    error_class: None,
                                },
                            )
                            .map(|outcome| match outcome {
                                ProviderConfigurationAttemptCompletionOutcome::Appended => {
                                    ProviderAttemptCompletionDisposition::Appended
                                }
                                ProviderConfigurationAttemptCompletionOutcome::AlreadyAppended => {
                                    ProviderAttemptCompletionDisposition::AlreadyAppended
                                }
                                ProviderConfigurationAttemptCompletionOutcome::StaleSuppressed => {
                                    ProviderAttemptCompletionDisposition::StaleSuppressed
                                }
                            })
                    }
                };
                outcome.map_err(|_| ProviderAttemptAdmissionError::RuntimeAuthority {
                    safe_message: "provider runtime completion evidence could not be persisted"
                        .to_owned(),
                    reason_code: "provider_attempt_runtime_completion_persist_failed".to_owned(),
                    retryable: true,
                })
            })
            .await
            .map_err(|_| ProviderAttemptAdmissionError::RuntimeAuthority {
                safe_message: "provider runtime completion persistence worker panicked".to_owned(),
                reason_code: "provider_attempt_runtime_completion_worker_panicked".to_owned(),
                retryable: true,
            })??;
            settled.store(true, Ordering::Release);
            if outcome == ProviderAttemptCompletionDisposition::Appended {
                self.feedback
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .push(ProviderAttemptFeedback::Success(binding.clone()));
            }
            Ok(outcome)
        })
    }

    fn record_failure<'a>(
        &'a self,
        binding: &'a ProviderAttemptBinding,
        authority: Box<dyn ProviderAttemptRuntimeAuthority>,
        error: &'a ProviderError,
    ) -> ProviderAttemptCompletionFuture<'a> {
        Box::pin(async move {
            let authority_guard = authority
                .as_ref()
                .as_any()
                .downcast_ref::<GatewayProviderAttemptRuntimeAuthorityGuard>()
                .ok_or_else(|| ProviderAttemptAdmissionError::RuntimeAuthority {
                    safe_message: "provider attempt completion authority is invalid".to_owned(),
                    reason_code: "provider_attempt_runtime_authority_invalid".to_owned(),
                    retryable: false,
                })?;
            let exact_authority = authority_guard.authority.clone();
            let settled = Arc::clone(&authority_guard.settled);
            let runtime_state = Arc::clone(&self.runtime_state);
            let provider_id = binding.provider_id.clone();
            let model_id = binding.model_id.clone();
            let error_class = error.classification().class.as_str().to_owned();
            let outcome = tokio::task::spawn_blocking(move || {
                let outcome = match exact_authority {
                    GatewayProviderAttemptRuntimeAuthority::Run(authority) => runtime_state
                        .journal_store
                        .complete_provider_attempt(&ProviderAttemptCompletionRequest {
                            authority,
                            provider_id,
                            model_id,
                            outcome: "failure".to_owned(),
                            error_class: Some(error_class),
                        })
                        .map(|outcome| match outcome {
                            ProviderAttemptCompletionOutcome::Appended { .. } => {
                                ProviderAttemptCompletionDisposition::Appended
                            }
                            ProviderAttemptCompletionOutcome::AlreadyAppended { .. } => {
                                ProviderAttemptCompletionDisposition::AlreadyAppended
                            }
                            ProviderAttemptCompletionOutcome::StaleSuppressed => {
                                ProviderAttemptCompletionDisposition::StaleSuppressed
                            }
                        }),
                    GatewayProviderAttemptRuntimeAuthority::Configuration(authority) => {
                        runtime_state
                            .journal_store
                            .complete_provider_configuration_attempt(
                                &ProviderConfigurationAttemptCompletionRequest {
                                    authority,
                                    provider_id,
                                    model_id,
                                    outcome: "failure".to_owned(),
                                    error_class: Some(error_class),
                                },
                            )
                            .map(|outcome| match outcome {
                                ProviderConfigurationAttemptCompletionOutcome::Appended => {
                                    ProviderAttemptCompletionDisposition::Appended
                                }
                                ProviderConfigurationAttemptCompletionOutcome::AlreadyAppended => {
                                    ProviderAttemptCompletionDisposition::AlreadyAppended
                                }
                                ProviderConfigurationAttemptCompletionOutcome::StaleSuppressed => {
                                    ProviderAttemptCompletionDisposition::StaleSuppressed
                                }
                            })
                    }
                };
                outcome.map_err(|_| ProviderAttemptAdmissionError::RuntimeAuthority {
                    safe_message: "provider runtime completion evidence could not be persisted"
                        .to_owned(),
                    reason_code: "provider_attempt_runtime_completion_persist_failed".to_owned(),
                    retryable: true,
                })
            })
            .await
            .map_err(|_| ProviderAttemptAdmissionError::RuntimeAuthority {
                safe_message: "provider runtime completion persistence worker panicked".to_owned(),
                reason_code: "provider_attempt_runtime_completion_worker_panicked".to_owned(),
                retryable: true,
            })??;
            settled.store(true, Ordering::Release);
            if outcome == ProviderAttemptCompletionDisposition::Appended {
                self.feedback
                    .lock()
                    .unwrap_or_else(|lock_error| lock_error.into_inner())
                    .push(ProviderAttemptFeedback::Failure(binding.clone(), error.clone()));
            }
            Ok(outcome)
        })
    }
}

impl ProviderProbeAdmission for GatewayProviderProbeAdmission {
    fn bind_probe(
        &self,
        provider_id: &str,
        credential_id: &str,
        model_id: &str,
    ) -> Result<ProviderAttemptBinding, ProviderAttemptAdmissionError> {
        bind_provider_attempt(
            self.health_authority_by_provider.as_ref(),
            provider_id,
            credential_id,
            model_id,
        )
    }

    fn check_probe_eligibility(
        &self,
        binding: &ProviderAttemptBinding,
    ) -> Result<(), ProviderAttemptAdmissionError> {
        if self
            .runtime_state
            .model_provider
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .configuration_epoch
            != self.expected_configuration_epoch
        {
            return Err(ProviderAttemptAdmissionError::HealthBlocked {
                safe_message: "provider probe configuration authority is stale".to_owned(),
                reason_code: "provider_probe_admission_configuration_mismatch".to_owned(),
                retry_after_ms: None,
                operator_action_required: false,
            });
        }
        let Some(health) = self
            .runtime_state
            .journal_store
            .runtime_component_health(binding.health_authority.component_id.as_str())
            .map_err(|_| ProviderAttemptAdmissionError::HealthBlocked {
                safe_message: "provider probe health state is unavailable".to_owned(),
                reason_code: "provider_probe_admission_health_unavailable".to_owned(),
                retry_after_ms: None,
                operator_action_required: false,
            })?
        else {
            return Err(ProviderAttemptAdmissionError::HealthBlocked {
                safe_message: "provider probe health state is missing".to_owned(),
                reason_code: "provider_probe_admission_health_missing".to_owned(),
                retry_after_ms: None,
                operator_action_required: true,
            });
        };
        if health.component_id != binding.health_authority.component_id
            || health.generation != binding.health_authority.generation
        {
            return Err(ProviderAttemptAdmissionError::HealthBlocked {
                safe_message: "provider probe health authority is stale".to_owned(),
                reason_code: "provider_probe_admission_generation_mismatch".to_owned(),
                retry_after_ms: None,
                operator_action_required: false,
            });
        }
        match health.probe_admission_decision(current_unix_ms(), Some(&self.probe_lease)) {
            RuntimeProbeAdmissionDecision::AuthorizedNonMutating => Ok(()),
            RuntimeProbeAdmissionDecision::LeaseRequired => {
                Err(ProviderAttemptAdmissionError::HealthBlocked {
                    safe_message: "provider probe lease is required".to_owned(),
                    reason_code: "provider_probe_admission_lease_required".to_owned(),
                    retry_after_ms: None,
                    operator_action_required: false,
                })
            }
            RuntimeProbeAdmissionDecision::LeaseInactive => {
                Err(ProviderAttemptAdmissionError::HealthBlocked {
                    safe_message: "provider probe lease is inactive".to_owned(),
                    reason_code: "provider_probe_admission_lease_inactive".to_owned(),
                    retry_after_ms: None,
                    operator_action_required: false,
                })
            }
            RuntimeProbeAdmissionDecision::LeaseMismatch => {
                Err(ProviderAttemptAdmissionError::HealthBlocked {
                    safe_message: "provider probe lease does not match durable authority"
                        .to_owned(),
                    reason_code: "provider_probe_admission_lease_mismatch".to_owned(),
                    retry_after_ms: None,
                    operator_action_required: true,
                })
            }
            RuntimeProbeAdmissionDecision::HealthNotProbing => {
                Err(ProviderAttemptAdmissionError::HealthBlocked {
                    safe_message: "provider health is not in probing state".to_owned(),
                    reason_code: "provider_probe_admission_health_not_probing".to_owned(),
                    retry_after_ms: None,
                    operator_action_required: false,
                })
            }
            RuntimeProbeAdmissionDecision::Quarantined => {
                Err(ProviderAttemptAdmissionError::HealthBlocked {
                    safe_message: "provider remains quarantined".to_owned(),
                    reason_code: "provider_probe_admission_quarantined".to_owned(),
                    retry_after_ms: None,
                    operator_action_required: true,
                })
            }
            RuntimeProbeAdmissionDecision::Disabled => {
                Err(ProviderAttemptAdmissionError::HealthBlocked {
                    safe_message: "provider is disabled".to_owned(),
                    reason_code: "provider_probe_admission_disabled".to_owned(),
                    retry_after_ms: None,
                    operator_action_required: true,
                })
            }
        }
    }
}

/// Routines/objectives wiring installed late, once the scheduler is running
/// (see [`GatewayRuntimeState::configure_routines_runtime`]).
#[derive(Clone)]
pub(crate) struct RoutinesRuntimeConfig {
    pub registry: Arc<crate::routines::RoutineRegistry>,
    pub objectives: Arc<crate::objectives::ObjectiveRegistry>,
    pub auth: GatewayAuthConfig,
    pub grpc_url: String,
    pub scheduler_wake: Arc<Notify>,
    pub timezone_mode: crate::cron::CronTimezoneMode,
}

/// Live resources tied to a run that terminal-run cleanup must release.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RunCleanupResources {
    pub(crate) browser_session_ids: Vec<String>,
    pub(crate) background_processes: Vec<RunOwnedBackgroundProcess>,
}

impl RunCleanupResources {
    /// True when no resources remain registered for the run.
    pub(crate) fn is_empty(&self) -> bool {
        self.browser_session_ids.is_empty() && self.background_processes.is_empty()
    }
}

/// In-memory cleanup authority backed by the same descriptor and lease persisted in the journal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RunOwnedBackgroundProcess {
    pub(crate) descriptor: RuntimeHandleDescriptorV1,
    pub(crate) lease: ProcessLeaseV1,
}

impl RunOwnedBackgroundProcess {
    /// Returns the PID anchoring exact process-tree ownership and cleanup authority.
    pub(crate) fn ownership_root_pid(&self) -> u32 {
        self.lease.pid
    }
}

/// Exact in-memory process authority awaiting verified cleanup or durable finalization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingProcessCleanup {
    pub(crate) run_id: String,
    pub(crate) process: RunOwnedBackgroundProcess,
    pub(crate) report: Option<CleanupReportV1>,
    pub(crate) final_state: RuntimeHandleState,
}

/// Aggregate result of one bounded pending-process cleanup retry pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PendingProcessCleanupReport {
    pub(crate) inspected_count: usize,
    pub(crate) completed_count: usize,
    pub(crate) pending_count: usize,
}

impl PendingProcessCleanup {
    fn key(&self) -> String {
        pending_process_cleanup_key(&self.process)
    }
}

/// Worker lease-revocation evidence retained durably until its exact journal row exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingNetworkedWorkerExpiry {
    event_id: String,
    event: WorkerLifecycleEvent,
}

impl PendingNetworkedWorkerExpiry {
    pub(crate) fn from_record(record: NetworkedWorkerExpiryOutboxRecord) -> Result<Self, Status> {
        let expected = networked_worker_expiry_event_id(&record.event).map_err(|error| {
            Status::failed_precondition(format!(
                "pending networked worker expiry evidence is invalid: {error}"
            ))
        })?;
        if record.event_id != expected {
            return Err(Status::failed_precondition(
                "pending networked worker expiry evidence id does not match exact lease evidence",
            ));
        }
        Ok(Self { event_id: record.event_id, event: record.event })
    }

    fn new(event: WorkerLifecycleEvent) -> Result<Self, Status> {
        let event_id = networked_worker_expiry_event_id(&event).map_err(|error| {
            Status::failed_precondition(format!(
                "pending networked worker expiry evidence is invalid: {error}"
            ))
        })?;
        Ok(Self { event_id, event })
    }

    fn key(&self) -> &str {
        self.event_id.as_str()
    }

    fn outbox_record(&self) -> NetworkedWorkerExpiryOutboxRecord {
        NetworkedWorkerExpiryOutboxRecord {
            event_id: self.event_id.clone(),
            event: self.event.clone(),
        }
    }
}

fn pending_process_cleanup_key(process: &RunOwnedBackgroundProcess) -> String {
    format!(
        "{}:{}:{}",
        process.lease.lease_id.as_str(),
        process.descriptor.instance_id.as_str(),
        process.lease.generation.get()
    )
}

fn pending_process_cleanup_report(process: &RunOwnedBackgroundProcess) -> CleanupReportV1 {
    let completed_at_unix_ms = current_unix_ms().max(process.descriptor.created_at_unix_ms);
    let mut hasher = sha2::Sha256::new();
    hasher.update(b"palyra.pending_process_cleanup.v1\0");
    hasher.update(process.lease.lease_id.as_str().as_bytes());
    hasher.update(b"\0");
    hasher.update(process.descriptor.instance_id.as_str().as_bytes());
    hasher.update(b"\0");
    hasher.update(process.lease.provenance.ownership_identity_sha256.as_bytes());
    let digest = hex::encode(hasher.finalize());
    CleanupReportV1 {
        schema_version: palyra_common::runtime_contracts::RUNTIME_HANDLE_SCHEMA_VERSION,
        report_id: format!("process-cleanup-retry:{}", &digest[..32]),
        instance_id: process.descriptor.instance_id.clone(),
        lease_id: Some(process.lease.lease_id.clone()),
        outcome: CleanupOutcome::Completed,
        steps: vec![
            CleanupStepRecord {
                ordinal: 0,
                step: CleanupStepKind::KillTree,
                disposition: CleanupStepDisposition::Completed,
                reason_code: "runtime.cleanup.retry_kill_tree_completed".to_owned(),
                evidence_sha256: None,
                completed_at_unix_ms,
            },
            CleanupStepRecord {
                ordinal: 1,
                step: CleanupStepKind::VerifyAbsence,
                disposition: CleanupStepDisposition::Completed,
                reason_code: "runtime.cleanup.retry_absence_verified".to_owned(),
                evidence_sha256: None,
                completed_at_unix_ms,
            },
        ],
        reason_code: "runtime.cleanup.retry_completed".to_owned(),
        completed_at_unix_ms,
    }
}

#[cfg(test)]
struct ProcessLeaseReconciliationActivity<'a> {
    runtime: &'a GatewayRuntimeState,
}

#[cfg(test)]
impl<'a> ProcessLeaseReconciliationActivity<'a> {
    fn begin(runtime: &'a GatewayRuntimeState) -> Self {
        let active = runtime
            .process_lease_reconciliation_active
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1);
        runtime.process_lease_reconciliation_max_active.fetch_max(active, Ordering::SeqCst);
        Self { runtime }
    }
}

#[cfg(test)]
impl Drop for ProcessLeaseReconciliationActivity<'_> {
    fn drop(&mut self) {
        self.runtime.process_lease_reconciliation_active.fetch_sub(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
struct NetworkedWorkerExpiryActivity<'a> {
    runtime: &'a GatewayRuntimeState,
}

#[cfg(test)]
impl<'a> NetworkedWorkerExpiryActivity<'a> {
    fn begin(runtime: &'a GatewayRuntimeState) -> Self {
        let active =
            runtime.networked_worker_expiry_active.fetch_add(1, Ordering::SeqCst).saturating_add(1);
        runtime.networked_worker_expiry_max_active.fetch_max(active, Ordering::SeqCst);
        Self { runtime }
    }
}

#[cfg(test)]
impl Drop for NetworkedWorkerExpiryActivity<'_> {
    fn drop(&mut self) {
        self.runtime.networked_worker_expiry_active.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Aggregate result of reconciling durable process leases against restart-visible evidence.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ProcessLeaseReconciliationReport {
    pub(crate) inspected_count: usize,
    pub(crate) closed_count: usize,
    pub(crate) orphaned_count: usize,
    pub(crate) quarantined_count: usize,
    pub(crate) expired_count: usize,
    pub(crate) pending_cleanup_inspected_count: usize,
    pub(crate) pending_cleanup_completed_count: usize,
    pub(crate) pending_cleanup_count: usize,
}

/// Detached background resources created by a run and intentionally left out
/// of terminal run cleanup. They are reported in the terminal cleanup summary
/// so post-run verifiers and users get explicit stop/status commands.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct RunDetachedResources {
    pub(crate) background_processes: Vec<DetachedBackgroundProcessResource>,
}

impl RunDetachedResources {
    /// True when the run did not create any detached handoff resources.
    pub(crate) fn is_empty(&self) -> bool {
        self.background_processes.is_empty()
    }
}

/// One detached background-process handoff record.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DetachedBackgroundProcessResource {
    pub(crate) pid: u32,
    pub(crate) lifetime_mode: String,
    pub(crate) ports: Vec<u16>,
    pub(crate) lifetime_ms: Option<u64>,
    pub(crate) max_lifetime_ms: Option<u64>,
    pub(crate) start_command: Value,
    pub(crate) cleanup: Value,
}

/// Bounded dedupe ledger of browser sessions that already closed, so terminal
/// run cleanup does not try to close them again. Insertion order is kept so
/// the oldest entries can be evicted once capacity is reached.
#[derive(Debug, Default)]
struct ClosedBrowserSessionLedger {
    ids: HashSet<String>,
    order: VecDeque<String>,
}

impl ClosedBrowserSessionLedger {
    fn insert(&mut self, session_id: String) {
        if !self.ids.insert(session_id.clone()) {
            return;
        }
        self.order.push_back(session_id);
        while self.ids.len() > CLOSED_BROWSER_SESSION_LEDGER_CAPACITY {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            self.ids.remove(oldest.as_str());
        }
    }

    fn remove(&mut self, session_id: &str) {
        self.ids.remove(session_id);
        self.order.retain(|existing| existing != session_id);
    }

    fn contains(&self, session_id: &str) -> bool {
        self.ids.contains(session_id)
    }
}

#[derive(Debug, Default)]
struct RunParameterDeltaCache {
    entries: HashMap<String, String>,
    order: VecDeque<String>,
}

impl RunParameterDeltaCache {
    fn insert(&mut self, run_id: &str, parameter_delta_json: &str) {
        let run_id = run_id.trim();
        let parameter_delta_json = parameter_delta_json.trim();
        if run_id.is_empty() || parameter_delta_json.is_empty() {
            return;
        }
        if let Some(existing) = self.entries.get_mut(run_id) {
            *existing = parameter_delta_json.to_owned();
            return;
        }

        self.entries.insert(run_id.to_owned(), parameter_delta_json.to_owned());
        self.order.push_back(run_id.to_owned());
        while self.entries.len() > RUN_PARAMETER_DELTA_CACHE_CAPACITY {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            self.entries.remove(oldest.as_str());
        }
    }

    fn get(&self, run_id: &str) -> Option<String> {
        let run_id = run_id.trim();
        if run_id.is_empty() {
            return None;
        }
        self.entries.get(run_id).cloned()
    }
}

/// Daemon-wide gateway state shared (via `Arc`) by every transport surface.
///
/// Owns the journal store and all in-memory runtime state: counters, caches
/// (memory search, http fetch, tool approvals, run launch context), run cleanup bookkeeping,
/// canvas records, provider leases, the channel router, and the worker fleet.
/// All mutation goes through interior mutability; methods take `&self` (or
/// `&Arc<Self>` for the `spawn_blocking` wrappers).
pub struct GatewayRuntimeState {
    pub(crate) started_at: Instant,
    pub(crate) build: BuildSnapshot,
    pub(crate) config: GatewayRuntimeConfigSnapshot,
    pub(crate) journal_config: GatewayJournalConfigSnapshot,
    pub(crate) counters: RuntimeCounters,
    feature_usage: FeatureUsageRegistry,
    runtime_kernel_dispatcher:
        Arc<crate::application::runtime_kernel_v2::dispatcher::RuntimeKernelDispatcher>,
    runtime_shadow_diagnostics:
        crate::runtime_diagnostics::shadow_differential::ShadowDifferentialDiagnostics,
    managed_coding_services:
        Option<Arc<crate::application::managed_coding_services::ManagedCodingRuntimeServices>>,
    pub(crate) journal_store: JournalStore,
    pub(crate) daemon_lifecycle: DaemonLifecycleController,
    daemon_lifecycle_transition_lock: Mutex<()>,
    pub(crate) fault_injection: QaFaultRuntime,
    revoked_certificate_count: usize,
    model_provider: RwLock<ModelProviderRuntime>,
    model_provider_reload_lock: Mutex<()>,
    provider_health_authority_latches: Mutex<BTreeSet<ProviderHealthAuthorityKey>>,
    managed_runtime_health_authorities: RwLock<BTreeMap<String, ManagedRuntimeHealthAuthority>>,
    managed_runtime_health_stale_suppressions: AtomicU64,
    auth_profile_registry: Option<Arc<AuthProfileRegistry>>,
    credential_availability: Option<CredentialAvailabilityService>,
    pub(crate) vault: Arc<Vault>,
    pub(crate) memory_config: RwLock<MemoryRuntimeConfig>,
    pub(crate) retrieval_config: RwLock<RetrievalRuntimeConfig>,
    pub(crate) learning_config: RwLock<LearningRuntimeConfig>,
    pub(crate) memory_search_cache: Mutex<HashMap<String, CachedMemorySearchEntry>>,
    pub(crate) http_fetch_cache: Mutex<HashMap<String, CachedHttpFetchEntry>>,
    code_intel_runtime: Mutex<CodeIntelRuntime>,
    recent_context_assembly_traces: Mutex<Vec<Value>>,
    tool_approval_cache: Mutex<ToolApprovalCacheState>,
    file_view_registry: Mutex<FileViewRegistry>,
    tool_guardrails: Mutex<HashMap<String, ToolGuardrailController>>,
    run_parameter_delta_cache: Mutex<RunParameterDeltaCache>,
    run_cleanup_resources: Mutex<HashMap<String, RunCleanupResources>>,
    run_detached_resources: Mutex<HashMap<String, RunDetachedResources>>,
    pending_process_cleanups: Mutex<HashMap<String, PendingProcessCleanup>>,
    process_lease_reconciliation_lock: AsyncMutex<()>,
    #[cfg(test)]
    background_process_registration_committed: Mutex<Option<Arc<Notify>>>,
    #[cfg(test)]
    background_process_registration_release: Mutex<Option<Arc<Notify>>>,
    #[cfg(test)]
    process_lease_reconciliation_active: AtomicU64,
    #[cfg(test)]
    process_lease_reconciliation_max_active: AtomicU64,
    closed_browser_sessions: Mutex<ClosedBrowserSessionLedger>,
    worker_fleet: RwLock<WorkerFleetManager>,
    worker_fleet_generation: AtomicU64,
    pending_networked_worker_expiry: Mutex<HashMap<String, PendingNetworkedWorkerExpiry>>,
    networked_worker_expiry_lock: AsyncMutex<()>,
    #[cfg(test)]
    networked_worker_expiry_active: AtomicU64,
    #[cfg(test)]
    networked_worker_expiry_max_active: AtomicU64,
    networked_worker_remote_dispatcher: RwLock<Option<Arc<dyn NetworkedWorkerRemoteDispatcher>>>,
    pub(crate) provider_leases: ProviderLeaseManager,
    pub(crate) retrieval_backend: Arc<dyn RetrievalBackend>,
    pub(crate) external_retrieval_index: Arc<ExternalRetrievalRuntime>,
    pub(crate) tool_posture_registry: ToolPostureRegistry,
    pub(crate) routines_runtime: RwLock<Option<RoutinesRuntimeConfig>>,
    pub(crate) vault_rate_limit: Mutex<HashMap<String, VaultRateLimitEntry>>,
    pub(crate) orchestrator_run_notify: Arc<Notify>,
    #[cfg(test)]
    fail_background_task_child_attachment_once: AtomicBool,
    canvas_records: Mutex<HashMap<String, CanvasRecord>>,
    canvas_signing_secret: [u8; 32],
    agent_registry: AgentRegistry,
    pub(crate) channel_router: ChannelRouter,
    pub(crate) inbound_coalescer: InboundCoalescer,
    pub(crate) channel_bot_loop_guard: ChannelBotLoopGuard,
    pub(crate) channel_turn_history: ChannelHistoryStore,
    pub(crate) conversation_bindings: ConversationBindingStore,
    pub(crate) observability: Arc<crate::observability::ObservabilityState>,
    pub(crate) self_healing: Arc<SelfHealingState>,
}

#[derive(Debug, Default)]
struct RunInterruptLatencyAccumulator {
    observations: AtomicU64,
    total_latency_ms: AtomicU64,
    max_latency_ms: AtomicU64,
    clamped_observations: AtomicU64,
}

impl RunInterruptLatencyAccumulator {
    fn record(&self, observation: RunInterruptLatencyObservation) {
        saturating_atomic_add(&self.observations, 1);
        saturating_atomic_add(&self.total_latency_ms, observation.latency_ms);
        self.max_latency_ms.fetch_max(observation.latency_ms, Ordering::Relaxed);
        if observation.clamped {
            saturating_atomic_add(&self.clamped_observations, 1);
        }
    }

    fn snapshot(&self, phase: RunInterruptPhase) -> RunInterruptLatencyPhaseSnapshot {
        RunInterruptLatencyPhaseSnapshot {
            phase: phase.as_str().to_owned(),
            observations: self.observations.load(Ordering::Relaxed),
            total_latency_ms: self.total_latency_ms.load(Ordering::Relaxed),
            max_latency_ms: self.max_latency_ms.load(Ordering::Relaxed),
            clamped_observations: self.clamped_observations.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Default)]
struct RunInterruptLatencyCounters {
    phases: [RunInterruptLatencyAccumulator; RunInterruptPhase::ALL.len()],
}

impl RunInterruptLatencyCounters {
    fn record(&self, observation: RunInterruptLatencyObservation) {
        self.phases[observation.phase.index()].record(observation);
    }

    fn snapshot(&self) -> RunInterruptLatencySnapshot {
        RunInterruptLatencySnapshot {
            schema_version: 1,
            reason_code: RUN_INTERRUPT_LATENCY_REASON_CODE.to_owned(),
            clamped_reason_code: RUN_INTERRUPT_LATENCY_CLAMPED_REASON_CODE.to_owned(),
            max_observation_ms: RUN_INTERRUPT_LATENCY_MAX_MS,
            phases: RunInterruptPhase::ALL
                .into_iter()
                .map(|phase| self.phases[phase.index()].snapshot(phase))
                .collect(),
        }
    }
}

fn saturating_atomic_add(value: &AtomicU64, increment: u64) {
    let _ = value.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(increment))
    });
}

/// Relaxed atomic counters backing [`CountersSnapshot`]. All values count
/// since process start except `journal_events`, which is seeded from the
/// persisted journal total.
#[derive(Debug)]
pub(crate) struct RuntimeCounters {
    pub(crate) run_stream_requests: AtomicU64,
    pub(crate) append_event_requests: AtomicU64,
    admin_status_requests: AtomicU64,
    denied_requests: AtomicU64,
    journal_events: AtomicU64,
    journal_persist_failures: AtomicU64,
    journal_redacted_events: AtomicU64,
    orchestrator_runs_started: AtomicU64,
    orchestrator_runs_completed: AtomicU64,
    orchestrator_runs_failed: AtomicU64,
    orchestrator_runs_cancelled: AtomicU64,
    orchestrator_cancel_requests: AtomicU64,
    orchestrator_tape_events: AtomicU64,
    metadata_trace_events: AtomicU64,
    metadata_trace_failures: AtomicU64,
    model_provider_requests: AtomicU64,
    model_provider_failures: AtomicU64,
    model_provider_retry_attempts: AtomicU64,
    model_provider_circuit_open_rejections: AtomicU64,
    tool_proposals: AtomicU64,
    pub(crate) tool_decisions_allowed: AtomicU64,
    pub(crate) tool_decisions_denied: AtomicU64,
    tool_execution_attempts: AtomicU64,
    run_stream_progress_coalesced: AtomicU64,
    run_stream_tool_deadline_exceeded: AtomicU64,
    run_stream_approval_cancelled: AtomicU64,
    run_stream_terminal_delivery_timeouts: AtomicU64,
    run_interrupt_latency: RunInterruptLatencyCounters,
    pub(crate) tool_execution_failures: AtomicU64,
    pub(crate) tool_execution_timeouts: AtomicU64,
    tool_attestations_emitted: AtomicU64,
    pub(crate) sandbox_launches: AtomicU64,
    pub(crate) sandbox_policy_denies: AtomicU64,
    pub(crate) sandbox_escape_attempts_blocked_workspace: AtomicU64,
    pub(crate) sandbox_escape_attempts_blocked_egress: AtomicU64,
    pub(crate) sandbox_escape_attempts_blocked_executable: AtomicU64,
    pub(crate) sandbox_backend_selected_tier_b: AtomicU64,
    pub(crate) sandbox_backend_selected_tier_c_linux_bubblewrap: AtomicU64,
    pub(crate) sandbox_backend_selected_tier_c_macos_sandbox_exec: AtomicU64,
    pub(crate) sandbox_backend_selected_tier_c_windows_job_object: AtomicU64,
    pub(crate) patches_applied: AtomicU64,
    pub(crate) patches_rejected: AtomicU64,
    pub(crate) patch_files_touched: AtomicU64,
    pub(crate) patch_rollbacks: AtomicU64,
    cron_jobs_created: AtomicU64,
    cron_jobs_updated: AtomicU64,
    cron_jobs_deleted: AtomicU64,
    cron_triggers_fired: AtomicU64,
    cron_runs_started: AtomicU64,
    cron_runs_completed: AtomicU64,
    cron_runs_failed: AtomicU64,
    cron_runs_skipped: AtomicU64,
    memory_items_ingested: AtomicU64,
    memory_items_rejected: AtomicU64,
    memory_search_requests: AtomicU64,
    memory_search_cache_hits: AtomicU64,
    memory_auto_inject_events: AtomicU64,
    learning_reflections_scheduled: AtomicU64,
    learning_reflections_completed: AtomicU64,
    learning_candidates_created: AtomicU64,
    learning_candidates_auto_applied: AtomicU64,
    vault_put_requests: AtomicU64,
    vault_get_requests: AtomicU64,
    vault_delete_requests: AtomicU64,
    vault_list_requests: AtomicU64,
    vault_rate_limited_requests: AtomicU64,
    pub(crate) vault_access_audit_events: AtomicU64,
    skill_status_updates: AtomicU64,
    pub(crate) skill_execution_denied: AtomicU64,
    approvals_tool_requested: AtomicU64,
    approvals_tool_resolved_allow: AtomicU64,
    approvals_tool_resolved_deny: AtomicU64,
    approvals_tool_resolved_timeout: AtomicU64,
    approvals_tool_resolved_error: AtomicU64,
    agent_mutations: AtomicU64,
    agent_resolution_hits: AtomicU64,
    agent_resolution_misses: AtomicU64,
    pub(crate) agent_validation_failures: AtomicU64,
    pub(crate) channel_messages_inbound: AtomicU64,
    channel_messages_routed: AtomicU64,
    channel_messages_replied: AtomicU64,
    pub(crate) channel_messages_rejected: AtomicU64,
    pub(crate) channel_messages_queued: AtomicU64,
    pub(crate) channel_messages_quarantined: AtomicU64,
    pub(crate) channel_router_queue_depth: AtomicU64,
    channel_reply_failures: AtomicU64,
    canvas_created: AtomicU64,
    canvas_updated: AtomicU64,
    canvas_closed: AtomicU64,
    canvas_denied: AtomicU64,
}

/// Build metadata reported in status responses.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct BuildSnapshot {
    pub(crate) version: String,
    pub(crate) git_hash: String,
    pub(crate) build_profile: String,
}

/// Top-level admin status document returned by the gateway status endpoints.
#[derive(Debug, Clone, Serialize)]
pub struct GatewayStatusSnapshot {
    pub service: &'static str,
    pub status: &'static str,
    pub version: String,
    pub git_hash: String,
    pub build_profile: String,
    pub uptime_seconds: u64,
    pub transport: TransportSnapshot,
    pub security: SecuritySnapshot,
    pub storage: StorageSnapshot,
    pub model_provider: ProviderStatusSnapshot,
    pub tool_call_policy: ToolCallPolicySnapshot,
    pub counters: CountersSnapshot,
    pub agents: AgentRuntimeSnapshot,
    pub runtime_kernel: Value,
    pub request_context: RequestContext,
}

/// Listener addresses and transport toggles in the status document.
#[derive(Debug, Clone, Serialize)]
pub struct TransportSnapshot {
    pub grpc_bind_addr: String,
    pub grpc_port: u16,
    pub quic_bind_addr: String,
    pub quic_port: u16,
    pub quic_enabled: bool,
}

/// Security posture flags in the status document.
#[derive(Debug, Clone, Serialize)]
pub struct SecuritySnapshot {
    pub deny_by_default: bool,
    pub admin_auth_required: bool,
    pub admin_token_configured: bool,
    pub orchestrator_runloop_v1_enabled: bool,
    pub node_rpc_mtls_required: bool,
    pub revoked_certificate_count: usize,
    pub smart_routing_enabled: bool,
    pub smart_routing_default_mode: String,
}

/// Journal storage details in the status document.
#[derive(Debug, Clone, Serialize)]
pub struct StorageSnapshot {
    pub journal_db_path: String,
    pub journal_hash_chain_enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_event_hash: Option<String>,
}

/// Point-in-time copy of [`RuntimeCounters`] for status responses.
#[derive(Debug, Clone, Serialize)]
pub struct CountersSnapshot {
    pub run_stream_requests: u64,
    pub append_event_requests: u64,
    pub admin_status_requests: u64,
    pub denied_requests: u64,
    pub journal_events: u64,
    pub journal_persist_failures: u64,
    pub journal_redacted_events: u64,
    pub orchestrator_runs_started: u64,
    pub orchestrator_runs_completed: u64,
    pub orchestrator_runs_failed: u64,
    pub orchestrator_runs_cancelled: u64,
    pub orchestrator_cancel_requests: u64,
    pub orchestrator_tape_events: u64,
    pub metadata_trace_events: u64,
    pub metadata_trace_failures: u64,
    pub model_provider_requests: u64,
    pub model_provider_failures: u64,
    pub model_provider_retry_attempts: u64,
    pub model_provider_circuit_open_rejections: u64,
    pub tool_proposals: u64,
    pub tool_decisions_allowed: u64,
    pub tool_decisions_denied: u64,
    pub tool_execution_attempts: u64,
    pub run_stream_progress_coalesced: u64,
    pub run_stream_tool_deadline_exceeded: u64,
    pub run_stream_approval_cancelled: u64,
    pub run_stream_terminal_delivery_timeouts: u64,
    pub run_interrupt_latency: RunInterruptLatencySnapshot,
    pub tool_execution_failures: u64,
    pub tool_execution_timeouts: u64,
    pub tool_attestations_emitted: u64,
    pub sandbox_launches: u64,
    pub sandbox_policy_denies: u64,
    pub sandbox_escape_attempts_blocked_workspace: u64,
    pub sandbox_escape_attempts_blocked_egress: u64,
    pub sandbox_escape_attempts_blocked_executable: u64,
    pub sandbox_backend_selected_tier_b: u64,
    pub sandbox_backend_selected_tier_c_linux_bubblewrap: u64,
    pub sandbox_backend_selected_tier_c_macos_sandbox_exec: u64,
    pub sandbox_backend_selected_tier_c_windows_job_object: u64,
    pub patches_applied: u64,
    pub patches_rejected: u64,
    pub patch_files_touched: u64,
    pub patch_rollbacks: u64,
    pub cron_jobs_created: u64,
    pub cron_jobs_updated: u64,
    pub cron_jobs_deleted: u64,
    pub cron_triggers_fired: u64,
    pub cron_runs_started: u64,
    pub cron_runs_completed: u64,
    pub cron_runs_failed: u64,
    pub cron_runs_skipped: u64,
    pub memory_items_ingested: u64,
    pub memory_items_rejected: u64,
    pub memory_search_requests: u64,
    pub memory_search_cache_hits: u64,
    pub memory_auto_inject_events: u64,
    pub learning_reflections_scheduled: u64,
    pub learning_reflections_completed: u64,
    pub learning_candidates_created: u64,
    pub learning_candidates_auto_applied: u64,
    pub vault_put_requests: u64,
    pub vault_get_requests: u64,
    pub vault_delete_requests: u64,
    pub vault_list_requests: u64,
    pub vault_rate_limited_requests: u64,
    pub vault_access_audit_events: u64,
    pub skill_status_updates: u64,
    pub skill_execution_denied: u64,
    pub approvals_tool_requested: u64,
    pub approvals_tool_resolved_allow: u64,
    pub approvals_tool_resolved_deny: u64,
    pub approvals_tool_resolved_timeout: u64,
    pub approvals_tool_resolved_error: u64,
    pub agent_mutations: u64,
    pub agent_resolution_hits: u64,
    pub agent_resolution_misses: u64,
    pub agent_validation_failures: u64,
    pub channel_messages_inbound: u64,
    pub channel_messages_routed: u64,
    pub channel_messages_replied: u64,
    pub channel_messages_rejected: u64,
    pub channel_messages_queued: u64,
    pub channel_messages_quarantined: u64,
    pub channel_router_queue_depth: u64,
    pub channel_reply_failures: u64,
    pub canvas_created: u64,
    pub canvas_updated: u64,
    pub canvas_closed: u64,
    pub canvas_denied: u64,
}

/// Bounded request-to-observation latency aggregates for one runtime phase.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RunInterruptLatencyPhaseSnapshot {
    pub phase: String,
    pub observations: u64,
    pub total_latency_ms: u64,
    pub max_latency_ms: u64,
    pub clamped_observations: u64,
}

/// Low-cardinality interrupt-latency diagnostics for all run-stream phases.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RunInterruptLatencySnapshot {
    pub schema_version: u32,
    pub reason_code: String,
    pub clamped_reason_code: String,
    pub max_observation_ms: u64,
    pub phases: Vec<RunInterruptLatencyPhaseSnapshot>,
}

/// Agent registry summary in the status document; session ids are redacted.
#[derive(Debug, Clone, Serialize)]
pub struct AgentRuntimeSnapshot {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_agent_id: Option<String>,
    pub agent_count: usize,
    pub active_session_bindings: Vec<AgentSessionBindingSnapshot>,
}

/// One active session-to-agent binding (redacted session id).
#[derive(Debug, Clone, Serialize)]
pub struct AgentSessionBindingSnapshot {
    pub session_id_redacted: String,
    pub agent_id: String,
}

/// OAuth refresh attempt counts for a single provider.
#[derive(Debug, Clone, Serialize, Default)]
pub struct AuthProviderRefreshMetricsSnapshot {
    pub provider: String,
    pub attempts: u64,
    pub successes: u64,
    pub failures: u64,
}

/// Aggregate OAuth refresh metrics, broken down per provider.
#[derive(Debug, Clone, Serialize, Default)]
pub struct AuthRefreshMetricsSnapshot {
    pub attempts: u64,
    pub successes: u64,
    pub failures: u64,
    pub by_provider: Vec<AuthProviderRefreshMetricsSnapshot>,
}

/// Admin-facing auth health: profile summary, expiry buckets, and refresh
/// metrics.
#[derive(Debug, Clone, Serialize, Default)]
pub struct AuthAdminStatusSnapshot {
    pub summary: AuthHealthSummary,
    pub expiry_distribution: AuthExpiryDistribution,
    pub refresh_metrics: AuthRefreshMetricsSnapshot,
}

/// Most recent journal events plus chain metadata for admin inspection.
#[derive(Debug, Clone, Serialize)]
pub struct JournalRecentSnapshot {
    pub total_events: u64,
    pub hash_chain_enabled: bool,
    pub events: Vec<JournalEventRecord>,
}

/// Page of orchestrator tape events with entry/byte budget bookkeeping;
/// `next_after_seq` is set when more events remain.
#[derive(Debug, Clone, Serialize)]
pub struct RunTapeSnapshot {
    pub run_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requested_after_seq: Option<i64>,
    pub limit: usize,
    pub max_response_bytes: usize,
    pub returned_bytes: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_after_seq: Option<i64>,
    pub events: Vec<OrchestratorTapeRecord>,
}

/// Run state right after a cancel request was recorded.
#[derive(Debug, Clone, Serialize)]
pub struct RunCancelSnapshot {
    pub run_id: String,
    pub state: String,
    pub cancel_requested: bool,
    pub reason: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cleanup_warning: Option<String>,
}

#[derive(Debug, Clone, Copy, Default)]
struct AuthProviderRefreshCounters {
    attempts: u64,
    successes: u64,
    failures: u64,
}

/// Mutable refresh-metrics accumulator behind [`AuthRuntimeState`].
#[derive(Debug, Default)]
struct AuthRefreshMetricsState {
    attempts: AtomicU64,
    successes: AtomicU64,
    failures: AtomicU64,
    by_provider: Mutex<HashMap<String, AuthProviderRefreshCounters>>,
}

impl AuthRefreshMetricsState {
    fn record_outcome(&self, outcome: &OAuthRefreshOutcome) {
        if !outcome.kind.attempted() {
            return;
        }
        self.attempts.fetch_add(1, Ordering::Relaxed);
        if outcome.kind.success() {
            self.successes.fetch_add(1, Ordering::Relaxed);
        } else {
            self.failures.fetch_add(1, Ordering::Relaxed);
        }
        if let Ok(mut guard) = self.by_provider.lock() {
            let provider_key = outcome.provider.to_ascii_lowercase();
            let entry = guard.entry(provider_key).or_default();
            entry.attempts = entry.attempts.saturating_add(1);
            if outcome.kind.success() {
                entry.successes = entry.successes.saturating_add(1);
            } else {
                entry.failures = entry.failures.saturating_add(1);
            }
        }
    }

    fn snapshot(&self) -> AuthRefreshMetricsSnapshot {
        let by_provider = if let Ok(guard) = self.by_provider.lock() {
            let mut rows = guard
                .iter()
                .map(|(provider, counters)| AuthProviderRefreshMetricsSnapshot {
                    provider: provider.clone(),
                    attempts: counters.attempts,
                    successes: counters.successes,
                    failures: counters.failures,
                })
                .collect::<Vec<_>>();
            rows.sort_by(|left, right| left.provider.cmp(&right.provider));
            rows
        } else {
            Vec::new()
        };
        AuthRefreshMetricsSnapshot {
            attempts: self.attempts.load(Ordering::Relaxed),
            successes: self.successes.load(Ordering::Relaxed),
            failures: self.failures.load(Ordering::Relaxed),
            by_provider,
        }
    }
}

/// OAuth profile refresh runtime: serializes refreshes per profile, records
/// refresh metrics, and produces auth health snapshots for admin surfaces.
#[derive(Clone)]
pub struct AuthRuntimeState {
    registry: Arc<AuthProfileRegistry>,
    refresh_adapter: Arc<dyn OAuthRefreshAdapter>,
    refresh_metrics: Arc<AuthRefreshMetricsState>,
    refresh_locks: Arc<Mutex<HashMap<String, Arc<AsyncMutex<()>>>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OAuthRefreshMarker {
    profile_id: String,
    provider: String,
    updated_at_unix_ms: i64,
    last_success_unix_ms: Option<i64>,
    expires_at_unix_ms: Option<i64>,
}

fn oauth_refresh_marker(profile: AuthProfileRecord) -> Option<OAuthRefreshMarker> {
    let AuthCredential::Oauth { expires_at_unix_ms, refresh_state, .. } = profile.credential else {
        return None;
    };
    Some(OAuthRefreshMarker {
        profile_id: profile.profile_id,
        provider: profile.provider.label(),
        updated_at_unix_ms: profile.updated_at_unix_ms,
        last_success_unix_ms: refresh_state.last_success_unix_ms,
        expires_at_unix_ms,
    })
}

impl AuthRuntimeState {
    /// Creates the auth runtime over a profile registry and refresh adapter.
    #[must_use]
    pub fn new(
        registry: Arc<AuthProfileRegistry>,
        refresh_adapter: Arc<dyn OAuthRefreshAdapter>,
    ) -> Self {
        Self {
            registry,
            refresh_adapter,
            refresh_metrics: Arc::new(AuthRefreshMetricsState::default()),
            refresh_locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Returns the underlying auth profile registry.
    pub fn registry(&self) -> &AuthProfileRegistry {
        self.registry.as_ref()
    }

    /// Returns the current refresh metrics, sorted per provider.
    pub fn refresh_metrics_snapshot(&self) -> AuthRefreshMetricsSnapshot {
        self.refresh_metrics.snapshot()
    }

    /// Records a refresh outcome into the metrics (no-op when nothing was
    /// attempted).
    pub fn record_refresh_outcome(&self, outcome: &OAuthRefreshOutcome) {
        self.refresh_metrics.record_outcome(outcome);
    }

    // One async mutex per profile id: refreshes for the same profile must not
    // race each other (token rotation in the vault), while different profiles
    // may refresh concurrently.
    fn refresh_lock(&self, profile_id: &str) -> Arc<AsyncMutex<()>> {
        let mut guard = self.refresh_locks.lock().unwrap_or_else(|error| error.into_inner());
        guard.entry(profile_id.to_owned()).or_insert_with(|| Arc::new(AsyncMutex::new(()))).clone()
    }

    /// Refreshes one OAuth profile on a blocking worker, serialized per
    /// profile id, and records the outcome in the refresh metrics.
    ///
    /// # Errors
    /// Returns the mapped auth profile error, or `Status::internal` if the
    /// worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn refresh_oauth_profile(
        self: &Arc<Self>,
        profile_id: String,
        vault: Arc<Vault>,
    ) -> Result<OAuthRefreshOutcome, Status> {
        let observed_marker = self
            .registry
            .get_profile(profile_id.as_str())
            .map_err(map_auth_profile_error)?
            .and_then(oauth_refresh_marker);
        let refresh_lock = self.refresh_lock(profile_id.as_str());
        let _refresh_guard = refresh_lock.lock().await;
        let current_marker = self
            .registry
            .get_profile(profile_id.as_str())
            .map_err(map_auth_profile_error)?
            .and_then(oauth_refresh_marker);
        if let (Some(observed), Some(current)) = (observed_marker.as_ref(), current_marker.as_ref())
        {
            if current.updated_at_unix_ms != observed.updated_at_unix_ms
                && current.last_success_unix_ms.is_some()
                && current.last_success_unix_ms != observed.last_success_unix_ms
            {
                return Ok(OAuthRefreshOutcome {
                    profile_id: current.profile_id.clone(),
                    provider: current.provider.clone(),
                    kind: OAuthRefreshOutcomeKind::SkippedNotDue,
                    reason: "refresh satisfied by an in-flight request".to_owned(),
                    next_allowed_refresh_unix_ms: None,
                    expires_at_unix_ms: current.expires_at_unix_ms,
                });
            }
        }
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let outcome = state
                .registry
                .refresh_oauth_profile(
                    profile_id.as_str(),
                    vault.as_ref(),
                    state.refresh_adapter.as_ref(),
                )
                .map_err(map_auth_profile_error)?;
            state.record_refresh_outcome(&outcome);
            Ok(outcome)
        })
        .await
        .map_err(|_| Status::internal("auth refresh worker panicked"))?
    }

    /// Builds the admin auth status (health report plus refresh metrics) on a
    /// blocking worker.
    ///
    /// # Errors
    /// Returns the mapped auth profile error, or `Status::internal` if the
    /// worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn admin_status_snapshot(
        self: &Arc<Self>,
        runtime_state: Arc<GatewayRuntimeState>,
    ) -> Result<AuthAdminStatusSnapshot, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let report = state
                .registry
                .health_report(runtime_state.vault.as_ref(), None)
                .map_err(map_auth_profile_error)?;
            Ok(AuthAdminStatusSnapshot {
                summary: report.summary,
                expiry_distribution: report.expiry_distribution,
                refresh_metrics: state.refresh_metrics.snapshot(),
            })
        })
        .await
        .map_err(|_| Status::internal("auth status worker panicked"))?
    }

    /// Refreshes all due OAuth profiles (optionally filtered by agent) and
    /// returns the resulting health report, refresh outcomes, and metrics.
    ///
    /// # Errors
    /// Returns the mapped auth profile error, or `Status::internal` if the
    /// worker panicked.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn refresh_health_report(
        self: &Arc<Self>,
        runtime_state: Arc<GatewayRuntimeState>,
        agent_id: String,
    ) -> Result<(AuthHealthReport, Vec<OAuthRefreshOutcome>, AuthRefreshMetricsSnapshot), Status>
    {
        let state = Arc::clone(self);
        let agent_id_filter = non_empty(agent_id);
        tokio::task::spawn_blocking(move || {
            let outcomes = state
                .registry()
                .refresh_due_oauth_profiles(
                    runtime_state.vault.as_ref(),
                    state.refresh_adapter.as_ref(),
                    agent_id_filter.as_deref(),
                )
                .map_err(map_auth_profile_error)?;
            for outcome in &outcomes {
                state.record_refresh_outcome(outcome);
            }
            let report = state
                .registry()
                .health_report(runtime_state.vault.as_ref(), agent_id_filter.as_deref())
                .map_err(map_auth_profile_error)?;
            Ok::<_, Status>((report, outcomes, state.refresh_metrics_snapshot()))
        })
        .await
        .map_err(|_| Status::internal("auth health worker panicked"))?
    }
}

impl RuntimeCounters {
    /// Copies every counter into a serializable snapshot (relaxed loads; the
    /// values are not guaranteed to be mutually consistent).
    pub(crate) fn snapshot(&self) -> CountersSnapshot {
        CountersSnapshot {
            run_stream_requests: self.run_stream_requests.load(Ordering::Relaxed),
            append_event_requests: self.append_event_requests.load(Ordering::Relaxed),
            admin_status_requests: self.admin_status_requests.load(Ordering::Relaxed),
            denied_requests: self.denied_requests.load(Ordering::Relaxed),
            journal_events: self.journal_events.load(Ordering::Relaxed),
            journal_persist_failures: self.journal_persist_failures.load(Ordering::Relaxed),
            journal_redacted_events: self.journal_redacted_events.load(Ordering::Relaxed),
            orchestrator_runs_started: self.orchestrator_runs_started.load(Ordering::Relaxed),
            orchestrator_runs_completed: self.orchestrator_runs_completed.load(Ordering::Relaxed),
            orchestrator_runs_failed: self.orchestrator_runs_failed.load(Ordering::Relaxed),
            orchestrator_runs_cancelled: self.orchestrator_runs_cancelled.load(Ordering::Relaxed),
            orchestrator_cancel_requests: self.orchestrator_cancel_requests.load(Ordering::Relaxed),
            orchestrator_tape_events: self.orchestrator_tape_events.load(Ordering::Relaxed),
            metadata_trace_events: self.metadata_trace_events.load(Ordering::Relaxed),
            metadata_trace_failures: self.metadata_trace_failures.load(Ordering::Relaxed),
            model_provider_requests: self.model_provider_requests.load(Ordering::Relaxed),
            model_provider_failures: self.model_provider_failures.load(Ordering::Relaxed),
            model_provider_retry_attempts: self
                .model_provider_retry_attempts
                .load(Ordering::Relaxed),
            model_provider_circuit_open_rejections: self
                .model_provider_circuit_open_rejections
                .load(Ordering::Relaxed),
            tool_proposals: self.tool_proposals.load(Ordering::Relaxed),
            tool_decisions_allowed: self.tool_decisions_allowed.load(Ordering::Relaxed),
            tool_decisions_denied: self.tool_decisions_denied.load(Ordering::Relaxed),
            tool_execution_attempts: self.tool_execution_attempts.load(Ordering::Relaxed),
            run_stream_progress_coalesced: self
                .run_stream_progress_coalesced
                .load(Ordering::Relaxed),
            run_stream_tool_deadline_exceeded: self
                .run_stream_tool_deadline_exceeded
                .load(Ordering::Relaxed),
            run_stream_approval_cancelled: self
                .run_stream_approval_cancelled
                .load(Ordering::Relaxed),
            run_stream_terminal_delivery_timeouts: self
                .run_stream_terminal_delivery_timeouts
                .load(Ordering::Relaxed),
            run_interrupt_latency: self.run_interrupt_latency.snapshot(),
            tool_execution_failures: self.tool_execution_failures.load(Ordering::Relaxed),
            tool_execution_timeouts: self.tool_execution_timeouts.load(Ordering::Relaxed),
            tool_attestations_emitted: self.tool_attestations_emitted.load(Ordering::Relaxed),
            sandbox_launches: self.sandbox_launches.load(Ordering::Relaxed),
            sandbox_policy_denies: self.sandbox_policy_denies.load(Ordering::Relaxed),
            sandbox_escape_attempts_blocked_workspace: self
                .sandbox_escape_attempts_blocked_workspace
                .load(Ordering::Relaxed),
            sandbox_escape_attempts_blocked_egress: self
                .sandbox_escape_attempts_blocked_egress
                .load(Ordering::Relaxed),
            sandbox_escape_attempts_blocked_executable: self
                .sandbox_escape_attempts_blocked_executable
                .load(Ordering::Relaxed),
            sandbox_backend_selected_tier_b: self
                .sandbox_backend_selected_tier_b
                .load(Ordering::Relaxed),
            sandbox_backend_selected_tier_c_linux_bubblewrap: self
                .sandbox_backend_selected_tier_c_linux_bubblewrap
                .load(Ordering::Relaxed),
            sandbox_backend_selected_tier_c_macos_sandbox_exec: self
                .sandbox_backend_selected_tier_c_macos_sandbox_exec
                .load(Ordering::Relaxed),
            sandbox_backend_selected_tier_c_windows_job_object: self
                .sandbox_backend_selected_tier_c_windows_job_object
                .load(Ordering::Relaxed),
            patches_applied: self.patches_applied.load(Ordering::Relaxed),
            patches_rejected: self.patches_rejected.load(Ordering::Relaxed),
            patch_files_touched: self.patch_files_touched.load(Ordering::Relaxed),
            patch_rollbacks: self.patch_rollbacks.load(Ordering::Relaxed),
            cron_jobs_created: self.cron_jobs_created.load(Ordering::Relaxed),
            cron_jobs_updated: self.cron_jobs_updated.load(Ordering::Relaxed),
            cron_jobs_deleted: self.cron_jobs_deleted.load(Ordering::Relaxed),
            cron_triggers_fired: self.cron_triggers_fired.load(Ordering::Relaxed),
            cron_runs_started: self.cron_runs_started.load(Ordering::Relaxed),
            cron_runs_completed: self.cron_runs_completed.load(Ordering::Relaxed),
            cron_runs_failed: self.cron_runs_failed.load(Ordering::Relaxed),
            cron_runs_skipped: self.cron_runs_skipped.load(Ordering::Relaxed),
            memory_items_ingested: self.memory_items_ingested.load(Ordering::Relaxed),
            memory_items_rejected: self.memory_items_rejected.load(Ordering::Relaxed),
            memory_search_requests: self.memory_search_requests.load(Ordering::Relaxed),
            memory_search_cache_hits: self.memory_search_cache_hits.load(Ordering::Relaxed),
            memory_auto_inject_events: self.memory_auto_inject_events.load(Ordering::Relaxed),
            learning_reflections_scheduled: self
                .learning_reflections_scheduled
                .load(Ordering::Relaxed),
            learning_reflections_completed: self
                .learning_reflections_completed
                .load(Ordering::Relaxed),
            learning_candidates_created: self.learning_candidates_created.load(Ordering::Relaxed),
            learning_candidates_auto_applied: self
                .learning_candidates_auto_applied
                .load(Ordering::Relaxed),
            vault_put_requests: self.vault_put_requests.load(Ordering::Relaxed),
            vault_get_requests: self.vault_get_requests.load(Ordering::Relaxed),
            vault_delete_requests: self.vault_delete_requests.load(Ordering::Relaxed),
            vault_list_requests: self.vault_list_requests.load(Ordering::Relaxed),
            vault_rate_limited_requests: self.vault_rate_limited_requests.load(Ordering::Relaxed),
            vault_access_audit_events: self.vault_access_audit_events.load(Ordering::Relaxed),
            skill_status_updates: self.skill_status_updates.load(Ordering::Relaxed),
            skill_execution_denied: self.skill_execution_denied.load(Ordering::Relaxed),
            approvals_tool_requested: self.approvals_tool_requested.load(Ordering::Relaxed),
            approvals_tool_resolved_allow: self
                .approvals_tool_resolved_allow
                .load(Ordering::Relaxed),
            approvals_tool_resolved_deny: self.approvals_tool_resolved_deny.load(Ordering::Relaxed),
            approvals_tool_resolved_timeout: self
                .approvals_tool_resolved_timeout
                .load(Ordering::Relaxed),
            approvals_tool_resolved_error: self
                .approvals_tool_resolved_error
                .load(Ordering::Relaxed),
            agent_mutations: self.agent_mutations.load(Ordering::Relaxed),
            agent_resolution_hits: self.agent_resolution_hits.load(Ordering::Relaxed),
            agent_resolution_misses: self.agent_resolution_misses.load(Ordering::Relaxed),
            agent_validation_failures: self.agent_validation_failures.load(Ordering::Relaxed),
            channel_messages_inbound: self.channel_messages_inbound.load(Ordering::Relaxed),
            channel_messages_routed: self.channel_messages_routed.load(Ordering::Relaxed),
            channel_messages_replied: self.channel_messages_replied.load(Ordering::Relaxed),
            channel_messages_rejected: self.channel_messages_rejected.load(Ordering::Relaxed),
            channel_messages_queued: self.channel_messages_queued.load(Ordering::Relaxed),
            channel_messages_quarantined: self.channel_messages_quarantined.load(Ordering::Relaxed),
            channel_router_queue_depth: self.channel_router_queue_depth.load(Ordering::Relaxed),
            channel_reply_failures: self.channel_reply_failures.load(Ordering::Relaxed),
            canvas_created: self.canvas_created.load(Ordering::Relaxed),
            canvas_updated: self.canvas_updated.load(Ordering::Relaxed),
            canvas_closed: self.canvas_closed.load(Ordering::Relaxed),
            canvas_denied: self.canvas_denied.load(Ordering::Relaxed),
        }
    }
}

impl CountersSnapshot {
    /// Returns runs that have started but have not reached any terminal state.
    #[must_use]
    pub(crate) fn active_orchestrator_runs(&self) -> u64 {
        self.orchestrator_runs_started
            .saturating_sub(self.orchestrator_runs_completed)
            .saturating_sub(self.orchestrator_runs_failed)
            .saturating_sub(self.orchestrator_runs_cancelled)
    }
}

fn complete_retrieval_diagnostics(
    mut diagnostics: RetrievalBranchDiagnostics,
    fusion_latency_ms: u64,
    fused_hit_count: u64,
    total_latency_ms: u64,
) -> RetrievalBranchDiagnostics {
    let latency_budget_ms = u64::try_from(MEMORY_SEARCH_LATENCY_BUDGET_MS).unwrap_or(u64::MAX);
    diagnostics.fusion_latency_ms = fusion_latency_ms;
    diagnostics.fused_hit_count = fused_hit_count;
    diagnostics.total_latency_ms = total_latency_ms;
    diagnostics.latency_budget_ms = latency_budget_ms;
    diagnostics.latency_budget_exceeded = total_latency_ms > latency_budget_ms;
    diagnostics
}

fn elapsed_millis(started_at: Instant) -> u64 {
    u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// Extracts the profile id from an `auth-profile:<provider>:<profile>`
/// credential id; `None` for other credential id shapes.
fn auth_profile_id_from_credential_id(credential_id: &str) -> Option<&str> {
    let mut parts = credential_id.splitn(3, ':');
    let prefix = parts.next()?;
    let provider_id = parts.next()?;
    let profile_id = parts.next()?;
    if prefix == "auth-profile" && !provider_id.is_empty() && !profile_id.is_empty() {
        Some(profile_id)
    } else {
        None
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProviderCredentialAttribution {
    provider_id: String,
    credential_id: String,
    auth_profile_id: Option<String>,
}

fn provider_credential_attribution_from_parts(
    provider_id: &str,
    credential_id: &str,
) -> ProviderCredentialAttribution {
    ProviderCredentialAttribution {
        provider_id: provider_id.to_owned(),
        credential_id: credential_id.to_owned(),
        auth_profile_id: auth_profile_id_from_credential_id(credential_id).map(str::to_owned),
    }
}

/// Resolves which credential actually served `provider_id`. The provider that
/// answered may differ from the leased one when registry failover routed the
/// request elsewhere, so the live snapshot is consulted before falling back to
/// the lease context.
#[cfg(test)]
fn provider_credential_attribution_for_provider(
    snapshot: &ProviderStatusSnapshot,
    lease_context: &ProviderLeaseExecutionContext,
    provider_id: &str,
) -> Option<ProviderCredentialAttribution> {
    if snapshot.provider_id == provider_id {
        return Some(provider_credential_attribution_from_parts(
            snapshot.provider_id.as_str(),
            snapshot.credential_id.as_str(),
        ));
    }
    if let Some(provider) =
        snapshot.registry.providers.iter().find(|provider| provider.provider_id == provider_id)
    {
        return Some(provider_credential_attribution_from_parts(
            provider.provider_id.as_str(),
            provider.credential_id.as_str(),
        ));
    }
    if let Some(credential) = snapshot
        .registry
        .credentials
        .iter()
        .find(|credential| credential.provider_id == provider_id)
    {
        return Some(provider_credential_attribution_from_parts(
            credential.provider_id.as_str(),
            credential.credential_id.as_str(),
        ));
    }
    (lease_context.provider_id == provider_id).then(|| {
        provider_credential_attribution_from_parts(
            lease_context.provider_id.as_str(),
            lease_context.credential_id.as_str(),
        )
    })
}

fn auth_profile_failure_kind_for_provider_error(
    error: &ProviderError,
) -> Option<AuthProfileFailureKind> {
    if matches!(error, ProviderError::MissingApiKey | ProviderError::MissingAnthropicApiKey) {
        return Some(AuthProfileFailureKind::ConfigMissing);
    }
    let failure = error.failure_snapshot();
    match failure.class.as_str() {
        "auth_expired" => Some(AuthProfileFailureKind::RefreshDue),
        "auth_invalid" => Some(AuthProfileFailureKind::AuthInvalid),
        "permission_denied" => Some(AuthProfileFailureKind::Permission),
        "quota" | "quota_exceeded" => Some(AuthProfileFailureKind::Quota),
        "rate_limit" | "rate_limited" => Some(AuthProfileFailureKind::RateLimit),
        "suspected_compromise" => Some(AuthProfileFailureKind::SuspectedCompromise),
        "network_unavailable"
        | "provider_timeout"
        | "transient_upstream"
        | "malformed_response" => Some(AuthProfileFailureKind::Transient),
        _ => None,
    }
}

/// Canonical fingerprint of a run start request, used to detect conflicting
/// payloads behind the same idempotency key.
fn orchestrator_run_start_payload_sha256(
    request: &OrchestratorRunStartRequest,
) -> Result<String, Status> {
    let payload = json!({
        "run_id": request.run_id,
        "session_id": request.session_id,
        "origin_kind": request.origin_kind,
        "origin_run_id": request.origin_run_id,
        "triggered_by_principal": request.triggered_by_principal,
        "parameter_delta_json": request.parameter_delta_json,
        "delegated_admission": request.delegated_admission.as_ref().map(|admission| json!({
            "task_id": admission.task_id,
            "task_kind": admission.task_kind,
            "parent_session_id": admission.parent_session_id,
            "child_session_id": admission.child_session_id,
            "parent_run_id": admission.parent_run_id,
            "cancellation_context": admission.cancellation_context,
        })),
    });
    let encoded = serde_json::to_vec(&payload).map_err(|error| {
        Status::internal(format!("failed to encode run start payload: {error}"))
    })?;
    let mut hasher = Sha256::new();
    hasher.update(encoded.as_slice());
    Ok(hex::encode(hasher.finalize()))
}

fn stable_error_from_journal(code: &str, error: &JournalError) -> StableErrorEnvelope {
    StableErrorEnvelope::new(
        code,
        error.to_string(),
        "inspect the journal error and retry only after the underlying state is corrected",
    )
}

fn journal_state_error_status(error: JournalError) -> Status {
    match &error {
        JournalError::InvalidArgument(message) => Status::invalid_argument(message.clone()),
        JournalError::WriteBlockedByHashChainMismatch { .. } => {
            Status::failed_precondition(error.to_string())
        }
        JournalError::EmptyPath | JournalError::ParentTraversalPath { .. } => {
            Status::invalid_argument(error.to_string())
        }
        _ => Status::internal(format!("journal state operation failed: {error}")),
    }
}

fn daemon_lifecycle_status(error: DaemonLifecycleError) -> Status {
    match error {
        DaemonLifecycleError::InvalidTransition { .. }
        | DaemonLifecycleError::StaleEpoch { .. } => Status::failed_precondition(error.to_string()),
        DaemonLifecycleError::LockPoisoned => Status::internal(error.to_string()),
    }
}

/// Parses the stored run state into the canonical lifecycle phase, failing
/// closed on states this build does not know.
fn canonical_phase_from_snapshot(
    snapshot: &OrchestratorRunStatusSnapshot,
) -> Result<RunLifecyclePhase, Status> {
    RunLifecyclePhase::parse(snapshot.state.as_str()).ok_or_else(|| {
        Status::failed_precondition(format!(
            "orchestrator run {} has unknown lifecycle state {}",
            snapshot.run_id, snapshot.state
        ))
    })
}

impl CapabilityDispatchAuthorizer for GatewayRuntimeState {
    fn reserve_networked_worker_delivery(
        &self,
        request: &NetworkedWorkerDeliveryReservationRequest,
    ) -> Result<NetworkedWorkerDeliveryReservationOutcome, Status> {
        self.journal_store.reserve_networked_worker_delivery(request).map_err(|error| {
            map_orchestrator_store_error("reserve networked worker delivery", error)
        })
    }

    fn release_networked_worker_payload(
        &self,
        request: &NetworkedWorkerPayloadReleaseRequest,
    ) -> Result<NetworkedWorkerPayloadReleaseOutcome, Status> {
        self.journal_store.release_networked_worker_payload(request).map_err(|error| {
            map_orchestrator_store_error("release networked worker payload", error)
        })
    }

    fn acknowledge_networked_worker_payload(
        &self,
        request: &NetworkedWorkerPayloadAcknowledgementRequest,
    ) -> Result<NetworkedWorkerPayloadAcknowledgementOutcome, Status> {
        self.journal_store.acknowledge_networked_worker_payload(request).map_err(|error| {
            map_orchestrator_store_error("acknowledge networked worker payload", error)
        })
    }

    fn abort_networked_worker_dispatch_before_payload_release(
        &self,
        remote_request_id: &str,
        node_request_id: &str,
        request_sha256: &str,
        dispatch_fleet_generation: u64,
        observed_at_unix_ms: i64,
    ) -> Result<NetworkedWorkerDispatchAbortBeforeReleaseOutcome, Status> {
        self.journal_store
            .abort_networked_worker_dispatch_before_payload_release(
                remote_request_id,
                node_request_id,
                request_sha256,
                dispatch_fleet_generation,
                observed_at_unix_ms,
            )
            .map_err(|error| {
                map_orchestrator_store_error(
                    "abort networked worker dispatch before payload release",
                    error,
                )
            })
    }

    fn cancel_networked_worker_dispatch(
        &self,
        remote_request_id: &str,
        node_request_id: &str,
        reason_code: &str,
        observed_at_unix_ms: i64,
    ) -> Result<NetworkedWorkerDispatchCancelOutcome, Status> {
        self.journal_store
            .cancel_networked_worker_dispatch_claim(
                remote_request_id,
                node_request_id,
                reason_code,
                observed_at_unix_ms,
            )
            .map_err(|error| {
                map_orchestrator_store_error("cancel networked worker dispatch", error)
            })
    }

    #[cfg(test)]
    fn authorize_networked_worker_result(
        &self,
        remote_request_id: &str,
        node_request_id: &str,
        delivery_attempt_id: &str,
        run_generation: RuntimeGeneration,
        reporting_worker_id: &str,
        observed_at_unix_ms: i64,
    ) -> Result<crate::node_runtime::NetworkedWorkerResultAuthorizationOutcome, Status> {
        if self.record_networked_worker_stale_result_if_needed(
            remote_request_id,
            node_request_id,
            delivery_attempt_id,
            reporting_worker_id,
            run_generation,
        ) {
            return Ok(crate::node_runtime::NetworkedWorkerResultAuthorizationOutcome::Rejected);
        }
        self.journal_store
            .authorize_networked_worker_result_attempt(
                remote_request_id,
                node_request_id,
                delivery_attempt_id,
                run_generation,
                reporting_worker_id,
                observed_at_unix_ms,
            )
            .map(|outcome| match outcome {
                crate::journal::NetworkedWorkerResultAuthorizationOutcome::Authorized => {
                    crate::node_runtime::NetworkedWorkerResultAuthorizationOutcome::Authorized
                }
                crate::journal::NetworkedWorkerResultAuthorizationOutcome::Rejected => {
                    crate::node_runtime::NetworkedWorkerResultAuthorizationOutcome::Rejected
                }
            })
            .map_err(|error| {
                map_orchestrator_store_error("authorize networked worker result", error)
            })
    }

    fn commit_networked_worker_result(
        &self,
        request: &crate::node_runtime::NetworkedWorkerResultCommitRequest,
    ) -> Result<crate::node_runtime::NetworkedWorkerResultCommitOutcome, Status> {
        use crate::journal::NetworkedWorkerDispatchClaimState;
        use crate::node_runtime::{
            NetworkedWorkerResultCommitDisposition, NetworkedWorkerResultCommitOutcome,
        };

        let remote_request = &request.context.request;
        if request.reporting_worker_id != remote_request.lease.worker_id
            || request.result.worker_id != request.reporting_worker_id
            || request.callback_run_generation != remote_request.lease.run_generation
            || request.result.run_generation != request.callback_run_generation
        {
            return Ok(NetworkedWorkerResultCommitOutcome::Rejected);
        }
        let validated_result_sha256 = request
            .result
            .validated_receipt_sha256(remote_request, request.observed_at_unix_ms)
            .map_err(|error| {
                Status::failed_precondition(format!(
                    "networked worker result contract validation failed: {error}"
                ))
            })?;
        let claim = self
            .journal_store
            .networked_worker_dispatch_claim(remote_request.request_id.as_str())
            .map_err(|error| {
                map_orchestrator_store_error("inspect networked worker callback claim", error)
            })?;
        let Some(claim) = claim else {
            return Ok(NetworkedWorkerResultCommitOutcome::Rejected);
        };
        let exact_claim_binding = claim.node_request_id == request.node_request_id
            && claim.worker_id == request.reporting_worker_id
            && claim.lease_id == remote_request.lease.lease_id
            && claim.session_id.as_deref() == Some(remote_request.lease.session_id.as_str())
            && claim.run_id == remote_request.lease.run_id
            && claim.run_generation == Some(request.callback_run_generation)
            && claim.delivery_attempt_id.as_deref() == Some(request.delivery_attempt_id.as_str());
        if !exact_claim_binding {
            return Ok(NetworkedWorkerResultCommitOutcome::Rejected);
        }

        let committed_outcome = |disposition, canonical_observed_at_unix_ms| {
            NetworkedWorkerResultCommitOutcome::Committed {
                disposition,
                canonical_observed_at_unix_ms,
                validated_result_sha256: validated_result_sha256.clone(),
            }
        };
        match claim.state {
            NetworkedWorkerDispatchClaimState::Settled => {
                if claim.validated_result_sha256.as_deref()
                    != Some(validated_result_sha256.as_str())
                {
                    return Ok(NetworkedWorkerResultCommitOutcome::Rejected);
                }
                let observed_at_unix_ms = claim.result_observed_at_unix_ms.ok_or_else(|| {
                    Status::failed_precondition(
                        "settled networked worker callback is missing observation evidence",
                    )
                })?;
                Ok(committed_outcome(
                    NetworkedWorkerResultCommitDisposition::ExactReplay,
                    observed_at_unix_ms,
                ))
            }
            NetworkedWorkerDispatchClaimState::Reconciling => {
                let settlement_identity = NetworkedWorkerDispatchSettlementIdentity {
                    remote_request_id: remote_request.request_id.clone(),
                    delivery_attempt_id: Some(request.delivery_attempt_id.clone()),
                    session_id: remote_request.lease.session_id.clone(),
                    run_generation: request.callback_run_generation,
                };
                if let Err(error) = self.settle_reconciling_networked_worker_dispatch(
                    &settlement_identity,
                    request.reporting_worker_id.as_str(),
                    &WorkerLeaseIdentity {
                        lease_id: remote_request.lease.lease_id.clone(),
                        run_id: remote_request.lease.run_id.clone(),
                    },
                    validated_result_sha256.as_str(),
                    request.observed_at_unix_ms,
                ) {
                    let stale = self
                        .runtime_generation_for_tool_blocking(remote_request.lease.run_id.as_str())?
                        .is_none_or(|(session_id, generation)| {
                            session_id != remote_request.lease.session_id
                                || generation != request.callback_run_generation
                        });
                    return if stale {
                        Ok(NetworkedWorkerResultCommitOutcome::StaleSuppressed)
                    } else {
                        Err(error)
                    };
                }
                let settled = self
                    .journal_store
                    .networked_worker_dispatch_claim(remote_request.request_id.as_str())
                    .map_err(|error| {
                        map_orchestrator_store_error(
                            "reload reconciled networked worker callback",
                            error,
                        )
                    })?
                    .ok_or_else(|| {
                        Status::failed_precondition(
                            "reconciled networked worker callback disappeared",
                        )
                    })?;
                let observed_at_unix_ms = settled.result_observed_at_unix_ms.ok_or_else(|| {
                    Status::failed_precondition(
                        "reconciled networked worker callback is missing observation evidence",
                    )
                })?;
                Ok(committed_outcome(
                    NetworkedWorkerResultCommitDisposition::LateReconciliation,
                    observed_at_unix_ms,
                ))
            }
            NetworkedWorkerDispatchClaimState::InFlight => {
                let receipt = NetworkedWorkerArtifactReceipt {
                    request_id: remote_request.request_id.clone(),
                    proposal_id: remote_request.proposal_id.clone(),
                    tool_name: remote_request.tool_name.clone(),
                    principal: request.context.host.principal.clone(),
                    device_id: request.context.host.device_id.clone(),
                    channel: request.context.host.channel.clone(),
                    session_id: remote_request.lease.session_id.clone(),
                    run_id: remote_request.lease.run_id.clone(),
                    input_json_sha256: remote_request.input_json_sha256.clone(),
                    output_json_sha256: request.result.output_json_sha256.clone(),
                    output_manifest_sha256: request.result.output_manifest_sha256.clone(),
                    validated_result_sha256: validated_result_sha256.clone(),
                    grant_id: remote_request.lease.grant_id.clone(),
                    required_capabilities: remote_request.lease.required_capabilities.clone(),
                    workspace_scope: remote_request.lease.workspace_scope.clone(),
                    log_stream_id: remote_request.lease.artifact_transport.log_stream_id.clone(),
                    scratch_directory_id: remote_request
                        .lease
                        .artifact_transport
                        .scratch_directory_id
                        .clone(),
                    observed_at_unix_ms: request.observed_at_unix_ms,
                };
                match self.complete_networked_worker_result_blocking(
                    request.reporting_worker_id.as_str(),
                    WorkerLeaseIdentity {
                        lease_id: remote_request.lease.lease_id.clone(),
                        run_id: remote_request.lease.run_id.clone(),
                    },
                    request.result.cleanup_report.clone(),
                    receipt,
                    Some(NetworkedWorkerDispatchSettlementIdentity {
                        remote_request_id: remote_request.request_id.clone(),
                        delivery_attempt_id: Some(request.delivery_attempt_id.clone()),
                        session_id: remote_request.lease.session_id.clone(),
                        run_generation: request.callback_run_generation,
                    }),
                )? {
                    NetworkedWorkerResultCompletionOutcome::StaleSuppressed => {
                        Ok(NetworkedWorkerResultCommitOutcome::StaleSuppressed)
                    }
                    NetworkedWorkerResultCompletionOutcome::Completed(_) => {
                        let settled = self
                            .journal_store
                            .networked_worker_dispatch_claim(remote_request.request_id.as_str())
                            .map_err(|error| {
                                map_orchestrator_store_error(
                                    "reload committed networked worker callback",
                                    error,
                                )
                            })?
                            .ok_or_else(|| {
                                Status::failed_precondition(
                                    "committed networked worker callback disappeared",
                                )
                            })?;
                        let observed_at_unix_ms =
                            settled.result_observed_at_unix_ms.ok_or_else(|| {
                                Status::failed_precondition(
                                    "committed networked worker callback is missing observation evidence",
                                )
                            })?;
                        Ok(committed_outcome(
                            NetworkedWorkerResultCommitDisposition::ActiveCompletion,
                            observed_at_unix_ms,
                        ))
                    }
                }
            }
            NetworkedWorkerDispatchClaimState::Queued
            | NetworkedWorkerDispatchClaimState::Cancelled
            | NetworkedWorkerDispatchClaimState::FailedClosed => {
                Ok(NetworkedWorkerResultCommitOutcome::Rejected)
            }
        }
    }
}

impl GatewayRuntimeState {
    fn record_networked_worker_stale_result_if_needed(
        &self,
        remote_request_id: &str,
        node_request_id: &str,
        delivery_attempt_id: &str,
        reporting_worker_id: &str,
        observed_generation: RuntimeGeneration,
    ) -> bool {
        let claim = match self.journal_store.networked_worker_dispatch_claim(remote_request_id) {
            Ok(Some(claim)) => claim,
            Ok(None) => return false,
            Err(error) => {
                warn!(
                    remote_request_id,
                    error = %error,
                    "failed to inspect networked worker generation authority"
                );
                return false;
            }
        };
        if claim.node_request_id != node_request_id
            || claim.worker_id != reporting_worker_id
            || claim.delivery_attempt_id.as_deref() != Some(delivery_attempt_id)
        {
            return false;
        }
        let Some(session_id) = claim.session_id.as_deref() else {
            return false;
        };
        let active = match self.runtime_generation_for_tool_blocking(claim.run_id.as_str()) {
            Ok(active) => active,
            Err(error) => {
                warn!(
                    remote_request_id,
                    error = %error.message(),
                    "failed to inspect active run generation for worker result"
                );
                return false;
            }
        };
        let expected_generation = active
            .as_ref()
            .filter(|(active_session_id, _)| active_session_id == session_id)
            .map(|(_, generation)| *generation)
            .or(claim.run_generation);
        let stale = claim.run_generation != Some(observed_generation)
            || active.as_ref().is_none_or(|(active_session_id, active_generation)| {
                active_session_id != session_id || *active_generation != observed_generation
            });
        if !stale {
            return false;
        }

        self.managed_runtime_health_stale_suppressions.fetch_add(1, Ordering::Relaxed);
        if let Err(error) = self.journal_store.record_runtime_stale_event_diagnostic(
            &RuntimeStaleEventDiagnosticRequest {
                session_id: session_id.to_owned(),
                run_id: Some(claim.run_id),
                lane: RuntimeGenerationLane::Run,
                expected_generation,
                observed_generation,
                subsystem: RuntimeSubsystem::Worker,
                disposition: StaleEventDisposition::PersistedDiagnostic,
                reason_code: "runtime.worker.stale_result_suppressed".to_owned(),
            },
        ) {
            warn!(
                remote_request_id,
                error = %error,
                "failed to persist stale networked worker result diagnostic"
            );
        }
        true
    }

    fn record_networked_worker_stale_settlement_if_needed(
        &self,
        settlement: &crate::journal::NetworkedWorkerDispatchSettlement,
    ) {
        let Ok(Some(claim)) = self
            .journal_store
            .networked_worker_dispatch_claim(settlement.remote_request_id.as_str())
        else {
            return;
        };
        let Some(delivery_attempt_id) = settlement.delivery_attempt_id.as_deref() else {
            return;
        };
        self.record_networked_worker_stale_result_if_needed(
            settlement.remote_request_id.as_str(),
            claim.node_request_id.as_str(),
            delivery_attempt_id,
            settlement.worker_id.as_str(),
            settlement.run_generation,
        );
    }

    // Cache entries expire when the earliest hit TTL lapses, so a cached
    // result can never include an item the journal already considers expired.
    fn cached_memory_search_expires_at(hits: &[MemorySearchHit]) -> Option<i64> {
        hits.iter().filter_map(|hit| hit.item.ttl_unix_ms).min()
    }

    // Construction. `new` is the test-only convenience constructor with
    // deterministic defaults; production wiring goes through
    // `new_with_provider`.

    #[cfg(test)]
    pub fn new(
        config: GatewayRuntimeConfigSnapshot,
        journal_config: GatewayJournalConfigSnapshot,
        journal_store: JournalStore,
        revoked_certificate_count: usize,
        agent_registry: AgentRegistry,
    ) -> Result<Arc<Self>, JournalError> {
        Self::new_with_fault_injection(
            config,
            journal_config,
            journal_store,
            revoked_certificate_count,
            agent_registry,
            QaFaultRuntime::default(),
        )
    }

    /// Test-only constructor that wires an explicit QA fault runtime through the real gateway.
    #[cfg(test)]
    pub(crate) fn new_with_fault_injection(
        config: GatewayRuntimeConfigSnapshot,
        journal_config: GatewayJournalConfigSnapshot,
        journal_store: JournalStore,
        revoked_certificate_count: usize,
        agent_registry: AgentRegistry,
        fault_injection: QaFaultRuntime,
    ) -> Result<Arc<Self>, JournalError> {
        let default_provider = crate::model_provider::build_model_provider(
            &crate::model_provider::ModelProviderConfig::default(),
        )
        .expect("default deterministic model provider should initialize");
        let default_vault = build_test_vault();
        let tool_posture_root =
            std::env::temp_dir().join(format!("palyra-tool-posture-{}", Ulid::new()));
        let tool_posture_registry = ToolPostureRegistry::open(tool_posture_root.as_path())
            .expect("test tool posture registry should initialize");
        #[rustfmt::skip]
        let dependencies = GatewayRuntimeDependencies { model_provider: default_provider, vault: default_vault, auth_profile_registry: None, auth_runtime: None, agent_registry, tool_posture_registry, retrieval_backend: Arc::new(crate::retrieval::JournalRetrievalBackend), external_retrieval_index: Arc::new(crate::retrieval::ExternalRetrievalRuntime::default()), conversation_bindings: ConversationBindingStore::open_temp(), fault_injection, runtime_kernel_dispatcher: Arc::new(crate::application::runtime_kernel_v2::dispatcher::RuntimeKernelDispatcher::legacy_default().expect("legacy runtime dispatcher should initialize")), managed_coding_services: None };
        Self::new_with_provider(
            config,
            journal_config,
            journal_store,
            revoked_certificate_count,
            dependencies,
        )
    }

    /// Builds the runtime state, recovering canvas records from journal
    /// snapshots and seeding the journal event counter from the stored total.
    ///
    /// Canvas recovery is verified by replaying the patch chain and comparing
    /// it against the latest snapshot; construction fails closed on any
    /// divergence rather than serving a canvas whose history cannot be
    /// reproduced.
    ///
    /// # Errors
    /// Returns [`JournalError`] when journal reads fail or canvas replay
    /// verification does not match the persisted snapshots.
    pub fn new_with_provider(
        config: GatewayRuntimeConfigSnapshot,
        journal_config: GatewayJournalConfigSnapshot,
        journal_store: JournalStore,
        revoked_certificate_count: usize,
        dependencies: GatewayRuntimeDependencies,
    ) -> Result<Arc<Self>, JournalError> {
        #[rustfmt::skip]
        let GatewayRuntimeDependencies { model_provider, vault, auth_profile_registry, auth_runtime, agent_registry, tool_posture_registry, retrieval_backend, external_retrieval_index, conversation_bindings, fault_injection, runtime_kernel_dispatcher, managed_coding_services } = dependencies;
        let credential_availability = auth_runtime.map(|auth_runtime| {
            CredentialAvailabilityService::new(auth_runtime, Arc::clone(&vault))
        });
        let build = build_metadata();
        let existing_events = journal_store.total_events()? as u64;
        let canvas_snapshots =
            journal_store.list_canvas_state_snapshots(MAX_CANVAS_RECOVERY_SNAPSHOTS)?;
        for snapshot in &canvas_snapshots {
            let replayed = journal_store.replay_canvas_state(snapshot.canvas_id.as_str())?.ok_or(
                JournalError::InvalidCanvasReplay {
                    canvas_id: snapshot.canvas_id.clone(),
                    reason: "snapshot exists but replay produced no state".to_owned(),
                },
            )?;
            let replay_state: Value =
                serde_json::from_str(replayed.state_json.as_str()).map_err(|error| {
                    JournalError::InvalidCanvasReplay {
                        canvas_id: snapshot.canvas_id.clone(),
                        reason: format!("replay state JSON is invalid: {error}"),
                    }
                })?;
            let snapshot_state: Value = serde_json::from_str(snapshot.state_json.as_str())
                .map_err(|error| JournalError::InvalidCanvasReplay {
                    canvas_id: snapshot.canvas_id.clone(),
                    reason: format!("snapshot state JSON is invalid: {error}"),
                })?;
            if replayed.state_version != snapshot.state_version
                || replayed.state_schema_version != snapshot.state_schema_version
                || replay_state != snapshot_state
            {
                return Err(JournalError::InvalidCanvasReplay {
                    canvas_id: snapshot.canvas_id.clone(),
                    reason: "replay outcome does not match latest snapshot".to_owned(),
                });
            }
        }
        let recovered_canvas_records =
            load_canvas_records_from_snapshots(canvas_snapshots.as_slice())?;
        let channel_router = ChannelRouter::new(config.channel_router.clone());
        let inbound_coalescer =
            InboundCoalescer::new(config.channel_router.inbound_coalescing.clone());
        let durable_worker_snapshot = journal_store
            .load_networked_worker_fleet_snapshot(NETWORKED_WORKER_FLEET_MAX_ENTRIES)?;
        let worker_fleet_generation = durable_worker_snapshot.generation;
        let durable_worker_ids =
            durable_worker_snapshot.records.keys().cloned().collect::<Vec<_>>();
        #[cfg(feature = "qa-fault-injection")]
        let worker_fleet =
            WorkerFleetManager::from_durable_records(durable_worker_snapshot.records)
                .map_err(|error| JournalError::InvalidArgument(error.to_string()))?
                .with_qa_fault_probe(fault_injection.probe_handle());
        #[cfg(not(feature = "qa-fault-injection"))]
        let worker_fleet =
            WorkerFleetManager::from_durable_records(durable_worker_snapshot.records)
                .map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
        let provider_snapshot = model_provider.status_snapshot();
        let provider_health_inventory = provider_health_inventory(&provider_snapshot)?;
        let provider_activation = journal_store.activate_provider_runtime(
            provider_health_inventory.activations.as_slice(),
            current_unix_ms(),
        )?;
        let provider_health_authority_by_provider = activated_provider_health_authorities(
            &provider_health_inventory,
            &provider_activation.health,
        )?;
        let provider_configuration_epoch = provider_activation.configuration_epoch;
        let managed_health_inventory =
            managed_runtime_health_inventory(&config, durable_worker_ids)?;
        let managed_health_activation = journal_store.activate_runtime_health_components(
            managed_health_inventory.as_slice(),
            current_unix_ms(),
        )?;
        let managed_runtime_health_authorities = managed_health_inventory
            .iter()
            .map(|activation| {
                let component_id = activation.component_id.as_str();
                let generation =
                    managed_health_activation.generations.get(component_id).copied().ok_or_else(
                        || {
                            JournalError::InvalidArgument(format!(
                                "managed runtime health activation omitted component {component_id}"
                            ))
                        },
                    )?;
                let family = ManagedRuntimeHealthFamily::from_component_id(component_id)
                    .ok_or_else(|| {
                        JournalError::InvalidArgument(format!(
                            "managed runtime health component has unknown family: {component_id}"
                        ))
                    })?;
                Ok((
                    component_id.to_owned(),
                    ManagedRuntimeHealthAuthority {
                        family,
                        component_id: activation.component_id.clone(),
                        generation,
                    },
                ))
            })
            .collect::<Result<BTreeMap<_, _>, JournalError>>()?;
        let pending_networked_worker_expiry = journal_store
            .list_networked_worker_expiry_outbox(PENDING_NETWORKED_WORKER_EXPIRY_MAX_ENTRIES)?
            .into_iter()
            .map(PendingNetworkedWorkerExpiry::from_record)
            .map(|entry| {
                entry
                    .map(|entry| (entry.key().to_owned(), entry))
                    .map_err(|status| JournalError::InvalidArgument(status.message().to_owned()))
            })
            .collect::<Result<HashMap<_, _>, _>>()?;
        journal_store.reconcile_networked_worker_dispatch_claims_after_restart(
            NETWORKED_WORKER_DISPATCH_CLAIM_MAX_ENTRIES,
            current_unix_ms(),
        )?;
        let daemon_lifecycle_startup = journal_store.begin_daemon_lifecycle_startup()?;
        let state = Arc::new(Self {
            started_at: Instant::now(),
            build: BuildSnapshot {
                version: build.version.to_owned(),
                git_hash: build.git_hash.to_owned(),
                build_profile: build.build_profile.to_owned(),
            },
            config,
            journal_config,
            counters: RuntimeCounters {
                run_stream_requests: AtomicU64::new(0),
                append_event_requests: AtomicU64::new(0),
                admin_status_requests: AtomicU64::new(0),
                denied_requests: AtomicU64::new(0),
                journal_events: AtomicU64::new(existing_events),
                journal_persist_failures: AtomicU64::new(0),
                journal_redacted_events: AtomicU64::new(0),
                orchestrator_runs_started: AtomicU64::new(0),
                orchestrator_runs_completed: AtomicU64::new(0),
                orchestrator_runs_failed: AtomicU64::new(0),
                orchestrator_runs_cancelled: AtomicU64::new(0),
                orchestrator_cancel_requests: AtomicU64::new(0),
                orchestrator_tape_events: AtomicU64::new(0),
                metadata_trace_events: AtomicU64::new(0),
                metadata_trace_failures: AtomicU64::new(0),
                model_provider_requests: AtomicU64::new(0),
                model_provider_failures: AtomicU64::new(0),
                model_provider_retry_attempts: AtomicU64::new(0),
                model_provider_circuit_open_rejections: AtomicU64::new(0),
                tool_proposals: AtomicU64::new(0),
                tool_decisions_allowed: AtomicU64::new(0),
                tool_decisions_denied: AtomicU64::new(0),
                tool_execution_attempts: AtomicU64::new(0),
                run_stream_progress_coalesced: AtomicU64::new(0),
                run_stream_tool_deadline_exceeded: AtomicU64::new(0),
                run_stream_approval_cancelled: AtomicU64::new(0),
                run_stream_terminal_delivery_timeouts: AtomicU64::new(0),
                run_interrupt_latency: RunInterruptLatencyCounters::default(),
                tool_execution_failures: AtomicU64::new(0),
                tool_execution_timeouts: AtomicU64::new(0),
                tool_attestations_emitted: AtomicU64::new(0),
                sandbox_launches: AtomicU64::new(0),
                sandbox_policy_denies: AtomicU64::new(0),
                sandbox_escape_attempts_blocked_workspace: AtomicU64::new(0),
                sandbox_escape_attempts_blocked_egress: AtomicU64::new(0),
                sandbox_escape_attempts_blocked_executable: AtomicU64::new(0),
                sandbox_backend_selected_tier_b: AtomicU64::new(0),
                sandbox_backend_selected_tier_c_linux_bubblewrap: AtomicU64::new(0),
                sandbox_backend_selected_tier_c_macos_sandbox_exec: AtomicU64::new(0),
                sandbox_backend_selected_tier_c_windows_job_object: AtomicU64::new(0),
                patches_applied: AtomicU64::new(0),
                patches_rejected: AtomicU64::new(0),
                patch_files_touched: AtomicU64::new(0),
                patch_rollbacks: AtomicU64::new(0),
                cron_jobs_created: AtomicU64::new(0),
                cron_jobs_updated: AtomicU64::new(0),
                cron_jobs_deleted: AtomicU64::new(0),
                cron_triggers_fired: AtomicU64::new(0),
                cron_runs_started: AtomicU64::new(0),
                cron_runs_completed: AtomicU64::new(0),
                cron_runs_failed: AtomicU64::new(0),
                cron_runs_skipped: AtomicU64::new(0),
                memory_items_ingested: AtomicU64::new(0),
                memory_items_rejected: AtomicU64::new(0),
                memory_search_requests: AtomicU64::new(0),
                memory_search_cache_hits: AtomicU64::new(0),
                memory_auto_inject_events: AtomicU64::new(0),
                learning_reflections_scheduled: AtomicU64::new(0),
                learning_reflections_completed: AtomicU64::new(0),
                learning_candidates_created: AtomicU64::new(0),
                learning_candidates_auto_applied: AtomicU64::new(0),
                vault_put_requests: AtomicU64::new(0),
                vault_get_requests: AtomicU64::new(0),
                vault_delete_requests: AtomicU64::new(0),
                vault_list_requests: AtomicU64::new(0),
                vault_rate_limited_requests: AtomicU64::new(0),
                vault_access_audit_events: AtomicU64::new(0),
                skill_status_updates: AtomicU64::new(0),
                skill_execution_denied: AtomicU64::new(0),
                approvals_tool_requested: AtomicU64::new(0),
                approvals_tool_resolved_allow: AtomicU64::new(0),
                approvals_tool_resolved_deny: AtomicU64::new(0),
                approvals_tool_resolved_timeout: AtomicU64::new(0),
                approvals_tool_resolved_error: AtomicU64::new(0),
                agent_mutations: AtomicU64::new(0),
                agent_resolution_hits: AtomicU64::new(0),
                agent_resolution_misses: AtomicU64::new(0),
                agent_validation_failures: AtomicU64::new(0),
                channel_messages_inbound: AtomicU64::new(0),
                channel_messages_routed: AtomicU64::new(0),
                channel_messages_replied: AtomicU64::new(0),
                channel_messages_rejected: AtomicU64::new(0),
                channel_messages_queued: AtomicU64::new(0),
                channel_messages_quarantined: AtomicU64::new(0),
                channel_router_queue_depth: AtomicU64::new(0),
                channel_reply_failures: AtomicU64::new(0),
                canvas_created: AtomicU64::new(0),
                canvas_updated: AtomicU64::new(0),
                canvas_closed: AtomicU64::new(0),
                canvas_denied: AtomicU64::new(0),
            },
            feature_usage: FeatureUsageRegistry::new(),
            runtime_kernel_dispatcher,
            runtime_shadow_diagnostics:
                crate::runtime_diagnostics::shadow_differential::ShadowDifferentialDiagnostics::default(),
            managed_coding_services,
            journal_store,
            daemon_lifecycle: DaemonLifecycleController::new(daemon_lifecycle_startup),
            daemon_lifecycle_transition_lock: Mutex::new(()),
            fault_injection,
            revoked_certificate_count,
            model_provider: RwLock::new(ModelProviderRuntime {
                provider: model_provider,
                configuration_epoch: provider_configuration_epoch,
                health_authority_by_provider: provider_health_authority_by_provider,
            }),
            model_provider_reload_lock: Mutex::new(()),
            provider_health_authority_latches: Mutex::new(BTreeSet::new()),
            managed_runtime_health_authorities: RwLock::new(managed_runtime_health_authorities),
            managed_runtime_health_stale_suppressions: AtomicU64::new(0),
            auth_profile_registry,
            credential_availability,
            vault,
            memory_config: RwLock::new(MemoryRuntimeConfig::default()),
            retrieval_config: RwLock::new(RetrievalRuntimeConfig::default()),
            learning_config: RwLock::new(LearningRuntimeConfig::default()),
            memory_search_cache: Mutex::new(HashMap::new()),
            http_fetch_cache: Mutex::new(HashMap::new()),
            code_intel_runtime: Mutex::new(CodeIntelRuntime::new()),
            recent_context_assembly_traces: Mutex::new(Vec::new()),
            tool_approval_cache: Mutex::new(ToolApprovalCacheState::default()),
            file_view_registry: Mutex::new(FileViewRegistry::default()),
            tool_guardrails: Mutex::new(HashMap::new()),
            run_parameter_delta_cache: Mutex::new(RunParameterDeltaCache::default()),
            run_cleanup_resources: Mutex::new(HashMap::new()),
            run_detached_resources: Mutex::new(HashMap::new()),
            pending_process_cleanups: Mutex::new(HashMap::new()),
            process_lease_reconciliation_lock: AsyncMutex::new(()),
            #[cfg(test)]
            background_process_registration_committed: Mutex::new(None),
            #[cfg(test)]
            background_process_registration_release: Mutex::new(None),
            #[cfg(test)]
            process_lease_reconciliation_active: AtomicU64::new(0),
            #[cfg(test)]
            process_lease_reconciliation_max_active: AtomicU64::new(0),
            closed_browser_sessions: Mutex::new(ClosedBrowserSessionLedger::default()),
            worker_fleet: RwLock::new(worker_fleet),
            worker_fleet_generation: AtomicU64::new(worker_fleet_generation),
            pending_networked_worker_expiry: Mutex::new(pending_networked_worker_expiry),
            networked_worker_expiry_lock: AsyncMutex::new(()),
            #[cfg(test)]
            networked_worker_expiry_active: AtomicU64::new(0),
            #[cfg(test)]
            networked_worker_expiry_max_active: AtomicU64::new(0),
            networked_worker_remote_dispatcher: RwLock::new(None),
            provider_leases: ProviderLeaseManager::default(),
            retrieval_backend,
            external_retrieval_index,
            tool_posture_registry,
            routines_runtime: RwLock::new(None),
            vault_rate_limit: Mutex::new(HashMap::new()),
            orchestrator_run_notify: Arc::new(Notify::new()),
            #[cfg(test)]
            fail_background_task_child_attachment_once: AtomicBool::new(false),
            canvas_records: Mutex::new(recovered_canvas_records),
            canvas_signing_secret: generate_canvas_signing_secret(),
            agent_registry,
            channel_router,
            inbound_coalescer,
            channel_bot_loop_guard: ChannelBotLoopGuard::default(),
            channel_turn_history: ChannelHistoryStore::default(),
            conversation_bindings,
            observability: Arc::new(crate::observability::ObservabilityState::default()),
            self_healing: Arc::new(SelfHealingState::new()),
        });
        state.install_managed_coding_wake_bridge()?;
        Ok(state)
    }

    // Counter recorders and run cleanup bookkeeping. The `record_*` methods
    // are fire-and-forget; identifier arguments are trimmed and empty values
    // ignored so malformed callers cannot poison the cleanup maps.

    /// Counts a denied request.
    pub fn record_denied(&self) {
        self.counters.denied_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// Counts an admin status request.
    pub fn record_admin_status_request(&self) {
        self.counters.admin_status_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a redacted direct or fallback observation for one rollout capability.
    pub(crate) fn record_feature_usage(
        &self,
        run_id: &str,
        capability: FeatureUsageCapability,
        path: FeatureUsagePath,
    ) {
        self.feature_usage.record(run_id, capability, path);
    }

    /// Returns aggregate rollout usage for the bounded process-local run window.
    pub(crate) fn feature_usage_snapshot(&self) -> FeatureUsageSnapshot {
        self.feature_usage.snapshot()
    }

    /// Records one identity-free V2 shadow sampling or differential outcome.
    pub(crate) fn record_runtime_shadow_observation(
        &self,
        result: &crate::application::runtime_kernel_v2::shadow::ShadowObservationResult,
    ) {
        self.runtime_shadow_diagnostics.record_observation(result);
    }

    /// Records an attempted shadow side-effect service acquisition.
    #[cfg(test)]
    pub(crate) fn record_runtime_shadow_authority_denial(
        &self,
        denial: crate::application::runtime_kernel_v2::shadow::ShadowAuthorityDenied,
    ) {
        self.runtime_shadow_diagnostics.record_authority_denial(denial);
    }

    /// Records one bounded production shadow observation failure.
    pub(crate) fn record_runtime_shadow_failure(
        &self,
        failure: crate::runtime_diagnostics::shadow_differential::RuntimeShadowFailureKind,
    ) {
        self.runtime_shadow_diagnostics.record_failure(failure);
    }

    /// Returns the fixed-cardinality process-local shadow diagnostics snapshot.
    pub(crate) fn runtime_shadow_diagnostics_snapshot(
        &self,
    ) -> crate::runtime_diagnostics::shadow_differential::ShadowDifferentialDiagnosticsSnapshotV1
    {
        self.runtime_shadow_diagnostics.snapshot()
    }

    /// Freezes retained rollout evidence after the durable run transition succeeds.
    pub(crate) fn mark_feature_usage_run_terminal(&self, run_id: &str) {
        self.feature_usage.mark_terminal(run_id);
    }

    /// Registers a browser session for cleanup when the run terminates,
    /// clearing any stale closed-marker for the same session first.
    pub(crate) fn record_run_browser_session(&self, run_id: &str, session_id: &str) {
        let run_id = run_id.trim();
        let session_id = session_id.trim();
        if run_id.is_empty() || session_id.is_empty() {
            return;
        }

        self.forget_closed_browser_session(session_id);
        match self.run_cleanup_resources.lock() {
            Ok(mut resources_by_run) => {
                let resources = resources_by_run.entry(run_id.to_owned()).or_default();
                if !resources.browser_session_ids.iter().any(|existing| existing == session_id) {
                    resources.browser_session_ids.push(session_id.to_owned());
                }
            }
            Err(error) => {
                warn!(
                    run_id,
                    error = %error,
                    "failed to record browser session for run cleanup"
                );
            }
        }
    }

    /// Marks a browser session as already closed so terminal-run cleanup
    /// skips it.
    pub(crate) fn record_closed_browser_session(&self, session_id: &str) {
        let session_id = session_id.trim();
        if session_id.is_empty() {
            return;
        }

        match self.closed_browser_sessions.lock() {
            Ok(mut ledger) => ledger.insert(session_id.to_owned()),
            Err(error) => {
                warn!(
                    session_id,
                    error = %error,
                    "failed to record closed browser session"
                );
            }
        }
    }

    /// Clears the closed-marker for a browser session (it is live again).
    pub(crate) fn forget_closed_browser_session(&self, session_id: &str) {
        let session_id = session_id.trim();
        if session_id.is_empty() {
            return;
        }

        match self.closed_browser_sessions.lock() {
            Ok(mut ledger) => ledger.remove(session_id),
            Err(error) => {
                warn!(
                    session_id,
                    error = %error,
                    "failed to clear closed browser session marker"
                );
            }
        }
    }

    /// Whether a browser session is marked as already closed.
    pub(crate) fn is_browser_session_closed(&self, session_id: &str) -> bool {
        let session_id = session_id.trim();
        if session_id.is_empty() {
            return false;
        }

        match self.closed_browser_sessions.lock() {
            Ok(ledger) => ledger.contains(session_id),
            Err(error) => {
                warn!(
                    session_id,
                    error = %error,
                    "failed to inspect closed browser session marker"
                );
                false
            }
        }
    }

    fn remember_run_parameter_delta_json(&self, run_id: &str, parameter_delta_json: Option<&str>) {
        let Some(parameter_delta_json) = parameter_delta_json else {
            return;
        };
        match self.run_parameter_delta_cache.lock() {
            Ok(mut cache) => cache.insert(run_id, parameter_delta_json),
            Err(error) => {
                warn!(
                    run_id,
                    error = %error,
                    "failed to lock run launch context cache"
                );
                error.into_inner().insert(run_id, parameter_delta_json);
            }
        }
    }

    /// Returns the in-process start-time parameter delta for a run, when this
    /// daemon instance started or replayed it.
    pub(crate) fn cached_run_parameter_delta_json(&self, run_id: &str) -> Option<String> {
        match self.run_parameter_delta_cache.lock() {
            Ok(cache) => cache.get(run_id),
            Err(error) => {
                warn!(
                    run_id,
                    error = %error,
                    "failed to read run launch context cache"
                );
                error.into_inner().get(run_id)
            }
        }
    }

    /// Unregisters a browser session from the run's cleanup set, dropping the
    /// run entry when nothing remains.
    pub(crate) fn forget_run_browser_session(&self, run_id: &str, session_id: &str) {
        let run_id = run_id.trim();
        let session_id = session_id.trim();
        if run_id.is_empty() || session_id.is_empty() {
            return;
        }

        match self.run_cleanup_resources.lock() {
            Ok(mut resources_by_run) => {
                if let Some(resources) = resources_by_run.get_mut(run_id) {
                    resources.browser_session_ids.retain(|existing| existing != session_id);
                    if resources.is_empty() {
                        resources_by_run.remove(run_id);
                    }
                }
            }
            Err(error) => {
                warn!(
                    run_id,
                    error = %error,
                    "failed to forget browser session for run cleanup"
                );
            }
        }
    }

    /// Persists and registers an exact process lease for cleanup when the run terminates.
    ///
    /// # Errors
    /// Returns a fail-closed status when the descriptor or lease cannot be persisted.
    #[allow(clippy::result_large_err)]
    pub(crate) fn record_run_background_process(
        &self,
        run_id: &str,
        process: RunOwnedBackgroundProcess,
    ) -> Result<(), Status> {
        self.record_run_background_process_inner(run_id, process, false)
    }

    /// Persists a process lease only while its exact run generation remains active.
    #[allow(clippy::result_large_err)]
    pub(crate) fn record_run_background_process_for_active_generation(
        &self,
        run_id: &str,
        process: RunOwnedBackgroundProcess,
    ) -> Result<(), Status> {
        self.record_run_background_process_inner(run_id, process, true)
    }

    #[allow(clippy::result_large_err)]
    fn record_run_background_process_inner(
        &self,
        run_id: &str,
        process: RunOwnedBackgroundProcess,
        require_active_generation: bool,
    ) -> Result<(), Status> {
        let run_id = run_id.trim();
        if run_id.is_empty() || process.ownership_root_pid() == 0 {
            return Err(Status::invalid_argument(
                "run-owned background process requires a run id and positive pid",
            ));
        }
        let pid = process.ownership_root_pid();
        let cleanup_authority_retained =
            match crate::sandbox_runner::retain_background_process_cleanup_authority(
                pid,
                &process.lease.provenance,
            ) {
                Ok(()) => true,
                #[cfg(test)]
                Err(error)
                    if error.kind() == std::io::ErrorKind::NotFound
                        && process.descriptor.owner.ends_with("-test") =>
                {
                    false
                }
                Err(error) => {
                    return Err(Status::failed_precondition(format!(
                        "retain runtime process cleanup authority: {error}"
                    )));
                }
            };
        let mut resources_by_run = match self.run_cleanup_resources.lock() {
            Ok(resources_by_run) => resources_by_run,
            Err(error) => {
                warn!(
                    run_id,
                    pid,
                    error = %error,
                    "failed to acquire background process cleanup registry"
                );
                if cleanup_authority_retained {
                    match crate::sandbox_runner::terminate_retained_background_process(
                        pid,
                        &process.lease.provenance,
                    ) {
                        Ok(_) => {
                            crate::sandbox_runner::release_background_process_cleanup_authority(
                                pid,
                                &process.lease.provenance,
                            );
                        }
                        Err(cleanup_error) => {
                            self.enqueue_pending_process_cleanup(PendingProcessCleanup {
                                run_id: run_id.to_owned(),
                                process,
                                report: None,
                                final_state: RuntimeHandleState::Closed,
                            })
                            .map_err(|transfer_error| {
                                Status::resource_exhausted(format!(
                                    "run cleanup registry lock is poisoned; exact synchronous cleanup failed: {cleanup_error}; retry supervisor transfer failed while exact authority remains retained: {}",
                                    transfer_error.message()
                                ))
                            })?;
                        }
                    }
                }
                return Err(Status::internal(
                    "run cleanup registry lock is poisoned; process ownership was not persisted",
                ));
            }
        };
        // Terminal cleanup acquires this registry only after its journal transaction
        // commits. Holding it across active-generation authorization and publication
        // makes successful registration visible to that one cleanup snapshot.
        let persistence = if require_active_generation {
            self.journal_store.register_process_handle_and_lease_for_active_generation(
                &process.descriptor,
                &process.lease,
            )
        } else {
            self.journal_store
                .register_process_handle_and_lease(&process.descriptor, &process.lease)
        };
        if let Err(error) = persistence {
            drop(resources_by_run);
            let persistence_error =
                map_orchestrator_store_error("persist runtime process ownership", error);
            let recovery = if cleanup_authority_retained {
                match crate::sandbox_runner::terminate_retained_background_process(
                    pid,
                    &process.lease.provenance,
                ) {
                    Ok(status) => {
                        crate::sandbox_runner::release_background_process_cleanup_authority(
                            pid,
                            &process.lease.provenance,
                        );
                        format!(
                            "exact synchronous cleanup verified direct_pid_alive={} process_tree_alive={}",
                            status.direct_pid_alive(),
                            status.process_tree_alive()
                        )
                    }
                    Err(cleanup_error) => {
                        let transfer =
                            self.enqueue_pending_process_cleanup(PendingProcessCleanup {
                                run_id: run_id.to_owned(),
                                process,
                                report: None,
                                final_state: RuntimeHandleState::Closed,
                            });
                        match transfer {
                            Ok(()) => format!(
                                "exact synchronous cleanup failed and exact authority transferred to the bounded retry supervisor: {cleanup_error}"
                            ),
                            Err(transfer_error) => {
                                return Err(Status::resource_exhausted(format!(
                                    "{}; exact synchronous cleanup failed: {cleanup_error}; retry supervisor transfer failed while exact authority remains retained: {}",
                                    persistence_error.message(),
                                    transfer_error.message()
                                )));
                            }
                        }
                    }
                }
            } else {
                "synthetic test process has no live cleanup authority; synchronous cleanup was not attempted"
                    .to_owned()
            };
            return Err(Status::internal(format!("{}; {recovery}", persistence_error.message())));
        }

        #[cfg(test)]
        if require_active_generation {
            let committed = self
                .background_process_registration_committed
                .lock()
                .expect("process registration test hook lock poisoned")
                .clone();
            let release = self
                .background_process_registration_release
                .lock()
                .expect("process registration test hook lock poisoned")
                .clone();
            if let Some(committed) = committed {
                committed.notify_one();
            }
            if let Some(release) = release {
                futures::executor::block_on(release.notified());
            }
        }

        let resources = resources_by_run.entry(run_id.to_owned()).or_default();
        if let Some(existing) = resources
            .background_processes
            .iter_mut()
            .find(|existing| existing.ownership_root_pid() == pid)
        {
            *existing = process;
        } else {
            resources.background_processes.push(process);
        }
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    fn exact_run_background_process_authority(
        &self,
        record: &PersistedProcessLeaseRecord,
    ) -> Result<Option<(String, RunOwnedBackgroundProcess)>, Status> {
        let resources_by_run = self.run_cleanup_resources.lock().map_err(|error| {
            Status::internal(format!(
                "run cleanup registry lock poisoned during process reconciliation: {error}"
            ))
        })?;
        Ok(resources_by_run.iter().find_map(|(run_id, resources)| {
            resources
                .background_processes
                .iter()
                .find(|process| {
                    process.descriptor == record.descriptor && process.lease == record.lease
                })
                .cloned()
                .map(|process| (run_id.clone(), process))
        }))
    }

    /// Returns the exact in-memory process authority currently registered for a run and PID.
    pub(crate) fn run_background_process(
        &self,
        run_id: &str,
        pid: u32,
    ) -> Option<RunOwnedBackgroundProcess> {
        let run_id = run_id.trim();
        if run_id.is_empty() || pid == 0 {
            return None;
        }

        match self.run_cleanup_resources.lock() {
            Ok(resources_by_run) => resources_by_run
                .get(run_id)
                .and_then(|resources| {
                    resources
                        .background_processes
                        .iter()
                        .find(|process| process.ownership_root_pid() == pid)
                })
                .cloned(),
            Err(error) => {
                warn!(
                    run_id,
                    pid,
                    error = %error,
                    "failed to inspect background process for run cleanup"
                );
                None
            }
        }
    }

    /// Unregisters a background process lease from the run's cleanup set,
    /// dropping the run entry when nothing remains.
    pub(crate) fn forget_run_background_process(&self, run_id: &str, pid: u32) {
        let run_id = run_id.trim();
        if run_id.is_empty() || pid == 0 {
            return;
        }

        match self.run_cleanup_resources.lock() {
            Ok(mut resources_by_run) => {
                if let Some(resources) = resources_by_run.get_mut(run_id) {
                    resources
                        .background_processes
                        .retain(|existing| existing.ownership_root_pid() != pid);
                    if resources.is_empty() {
                        resources_by_run.remove(run_id);
                    }
                }
            }
            Err(error) => {
                warn!(
                    run_id,
                    pid,
                    error = %error,
                    "failed to forget background process for run cleanup"
                );
            }
        }
    }

    /// Transfers exact process authority to the bounded in-memory cleanup retry owner.
    ///
    /// Duplicate transfers are idempotent only when the complete lease, descriptor, and cleanup
    /// intent match. A reused lease key with different provenance fails closed.
    ///
    /// # Errors
    /// Returns `invalid_argument` for malformed cleanup intent, `failed_precondition` for a
    /// conflicting exact-identity reuse, or `resource_exhausted` when the bounded owner is full.
    #[allow(clippy::result_large_err)]
    pub(crate) fn enqueue_pending_process_cleanup(
        &self,
        cleanup: PendingProcessCleanup,
    ) -> Result<(), Status> {
        let run_id = cleanup.run_id.trim();
        if run_id.is_empty() || cleanup.process.ownership_root_pid() == 0 {
            return Err(Status::invalid_argument(
                "pending process cleanup requires a run id and positive pid",
            ));
        }
        cleanup
            .process
            .descriptor
            .validate()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        cleanup
            .process
            .lease
            .validate()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        if cleanup.process.descriptor.instance_id != cleanup.process.lease.instance_id
            || cleanup.process.descriptor.generation != cleanup.process.lease.generation
            || !matches!(
                cleanup.final_state,
                RuntimeHandleState::Closed
                    | RuntimeHandleState::Orphaned
                    | RuntimeHandleState::Quarantined
            )
        {
            return Err(Status::invalid_argument(
                "pending process cleanup requires matching exact ownership and a terminal state",
            ));
        }
        if let Some(report) = cleanup.report.as_ref() {
            report.validate().map_err(|error| Status::invalid_argument(error.to_string()))?;
            if report.instance_id != cleanup.process.descriptor.instance_id
                || report.lease_id.as_ref() != Some(&cleanup.process.lease.lease_id)
            {
                return Err(Status::invalid_argument(
                    "pending process cleanup report does not match exact process ownership",
                ));
            }
        }

        let key = cleanup.key();
        let mut pending = self.pending_process_cleanups.lock().map_err(|error| {
            Status::internal(format!("pending process cleanup registry lock poisoned: {error}"))
        })?;
        if let Some(existing) = pending.get(key.as_str()) {
            return if existing == &cleanup {
                Ok(())
            } else {
                Err(Status::failed_precondition(
                    "pending process cleanup key conflicts with different exact ownership",
                ))
            };
        }
        if pending.len() >= PENDING_PROCESS_CLEANUP_MAX_ENTRIES {
            return Err(Status::resource_exhausted(format!(
                "pending process cleanup capacity {} exhausted",
                PENDING_PROCESS_CLEANUP_MAX_ENTRIES
            )));
        }
        pending.insert(key, cleanup);
        Ok(())
    }

    /// Retries one bounded snapshot of exact in-memory process cleanup authority.
    ///
    /// Entries remain owned by the supervisor after any termination or persistence failure.
    /// Cleanup authority is released only after durable finalization commits.
    ///
    /// # Errors
    /// Returns an error only when the supervisor registry cannot be inspected safely. Per-entry
    /// failures remain pending and are retried by a later serialized reconciliation pass.
    #[allow(clippy::result_large_err)]
    pub(crate) fn reconcile_pending_process_cleanups(
        &self,
    ) -> Result<PendingProcessCleanupReport, Status> {
        let entries = {
            let pending = self.pending_process_cleanups.lock().map_err(|error| {
                Status::internal(format!("pending process cleanup registry lock poisoned: {error}"))
            })?;
            pending.values().take(PENDING_PROCESS_CLEANUP_MAX_ENTRIES).cloned().collect::<Vec<_>>()
        };
        let mut summary = PendingProcessCleanupReport::default();
        for cleanup in entries {
            summary.inspected_count = summary.inspected_count.saturating_add(1);
            let key = cleanup.key();
            let report = match cleanup.report.clone() {
                Some(report) => report,
                None => match crate::sandbox_runner::terminate_retained_background_process(
                    cleanup.process.ownership_root_pid(),
                    &cleanup.process.lease.provenance,
                ) {
                    Ok(status) if !status.alive() => {
                        pending_process_cleanup_report(&cleanup.process)
                    }
                    Ok(_) => {
                        warn!(
                            run_id = cleanup.run_id,
                            pid = cleanup.process.ownership_root_pid(),
                            "pending process cleanup did not establish exact process absence"
                        );
                        continue;
                    }
                    Err(error) => {
                        warn!(
                            run_id = cleanup.run_id,
                            pid = cleanup.process.ownership_root_pid(),
                            error = %error,
                            "pending process cleanup retry failed"
                        );
                        continue;
                    }
                },
            };
            let finalization = if cleanup.report.is_none() {
                match self.journal_store.register_process_handle_and_lease(
                    &cleanup.process.descriptor,
                    &cleanup.process.lease,
                ) {
                    Ok(()) => self.finalize_process_cleanup(
                        &cleanup.process,
                        &report,
                        cleanup.final_state,
                    ),
                    Err(error) => Err(map_orchestrator_store_error(
                        "persist pending process cleanup ownership",
                        error,
                    )),
                }
            } else {
                self.finalize_process_cleanup(&cleanup.process, &report, cleanup.final_state)
            };
            if let Err(error) = finalization {
                warn!(
                    run_id = cleanup.run_id,
                    pid = cleanup.process.ownership_root_pid(),
                    error = %error,
                    "pending process cleanup durable finalization failed"
                );
                continue;
            }
            if let Err(error) =
                self.journal_store.finalize_startup_recovery_metadata_trace(cleanup.run_id.as_str())
            {
                warn!(
                    run_id_sha256 = crate::metadata_trace::hash_metadata_trace_run_id(
                        cleanup.run_id.as_str()
                    )
                    .unwrap_or_else(|| "invalid".to_owned()),
                    error = %error,
                    "failed to finalize startup-recovery trace after pending process cleanup"
                );
            }

            let removed = {
                let mut pending = self.pending_process_cleanups.lock().map_err(|error| {
                    Status::internal(format!(
                        "pending process cleanup registry lock poisoned: {error}"
                    ))
                })?;
                pending.remove(key.as_str()).is_some()
            };
            if removed {
                self.forget_run_background_process(
                    cleanup.run_id.as_str(),
                    cleanup.process.ownership_root_pid(),
                );
                crate::sandbox_runner::release_background_process_cleanup_authority(
                    cleanup.process.ownership_root_pid(),
                    &cleanup.process.lease.provenance,
                );
                summary.completed_count = summary.completed_count.saturating_add(1);
            }
        }
        summary.pending_count = self
            .pending_process_cleanups
            .lock()
            .map_err(|error| {
                Status::internal(format!("pending process cleanup registry lock poisoned: {error}"))
            })?
            .len();
        Ok(summary)
    }

    #[cfg(test)]
    pub(crate) fn pending_process_cleanup_count(&self) -> usize {
        self.pending_process_cleanups
            .lock()
            .map(|pending| pending.len())
            .unwrap_or(PENDING_PROCESS_CLEANUP_MAX_ENTRIES)
    }

    /// Reconciles one bounded page of durable process leases without PID-only adoption.
    ///
    /// Exact leases held by this runtime remain active while their registered ownership domain is
    /// live. When the in-process monitor has already proven an exact Windows Job Object empty,
    /// reconciliation closes the handle, retires the lease, and releases the retained anchor.
    /// Unix process-group absence remains orphaned because a numeric PGID cannot prove that an
    /// untrusted descendant did not escape the group. Restart-only records likewise remain
    /// orphaned on apparent absence because signalling authority is not reconstructed; live
    /// identity mismatches are quarantined. The keyset cursor advances across retained unresolved
    /// leases so later rows cannot starve. This path never signals a process.
    ///
    /// # Errors
    /// Returns a mapped journal error when durable records cannot be loaded or finalized.
    #[allow(clippy::result_large_err)]
    pub(crate) fn reconcile_persisted_process_leases(
        &self,
    ) -> Result<ProcessLeaseReconciliationReport, Status> {
        let after_lease_id = self
            .journal_store
            .process_reconciliation_checkpoint(PROCESS_LEASE_RECONCILIATION_CHECKPOINT_KEY)
            .map_err(|error| {
                map_orchestrator_store_error("load process reconciliation checkpoint", error)
            })?;
        let mut records = self
            .journal_store
            .list_persisted_process_leases_after(
                after_lease_id.as_deref(),
                PROCESS_LEASE_RECONCILIATION_BATCH_SIZE,
            )
            .map_err(|error| {
                map_orchestrator_store_error("load persisted process leases", error)
            })?;
        if records.is_empty() && after_lease_id.is_some() {
            records = self
                .journal_store
                .list_persisted_process_leases_after(None, PROCESS_LEASE_RECONCILIATION_BATCH_SIZE)
                .map_err(|error| {
                    map_orchestrator_store_error("load persisted process leases", error)
                })?;
        }
        let page_is_full = records.len() == PROCESS_LEASE_RECONCILIATION_BATCH_SIZE;
        let next_cursor = records.last().map(|record| record.lease.lease_id.as_str().to_owned());
        let now = current_unix_ms();
        let mut summary = ProcessLeaseReconciliationReport::default();
        for record in records {
            summary.inspected_count = summary.inspected_count.saturating_add(1);
            let exact_authority = self.exact_run_background_process_authority(&record)?;
            let disposition = if exact_authority.is_some() {
                match crate::sandbox_runner::background_process_registration_is_active(
                    record.lease.pid,
                    &record.lease.provenance,
                )
                .map_err(|error| {
                    Status::internal(format!(
                        "inspect exact process registration during reconciliation: {error}"
                    ))
                })? {
                    Some(true) | None => continue,
                    // Only the sandbox monitor or an exact cleanup path clears this flag, and both
                    // do so after proving ownership-domain absence. Re-probing a synthetic test
                    // PID (or a rapidly reused numeric ID) would weaken that stronger evidence.
                    Some(false) => ProcessProvenanceDisposition::Missing,
                }
            } else {
                crate::sandbox_runner::verify_persisted_process_provenance(
                    record.lease.pid,
                    &record.lease.provenance,
                )
            };
            if exact_authority.is_some() && disposition != ProcessProvenanceDisposition::Missing {
                continue;
            }
            let expired = now >= record.lease.expires_at_unix_ms;
            if expired {
                summary.expired_count = summary.expired_count.saturating_add(1);
            }
            let (handle_state, outcome, step_disposition, reason_code) = match disposition {
                ProcessProvenanceDisposition::Missing
                    if exact_authority.as_ref().is_some_and(|(_, process)| {
                        process.lease.provenance.ownership_kind
                            == ProcessOwnershipKind::WindowsJobObject
                    }) =>
                {
                    summary.closed_count = summary.closed_count.saturating_add(1);
                    (
                        RuntimeHandleState::Closed,
                        CleanupOutcome::Completed,
                        CleanupStepDisposition::Completed,
                        "runtime.cleanup.current_runtime_absence_verified",
                    )
                }
                ProcessProvenanceDisposition::Missing => {
                    summary.orphaned_count = summary.orphaned_count.saturating_add(1);
                    (
                        RuntimeHandleState::Orphaned,
                        CleanupOutcome::Unknown,
                        CleanupStepDisposition::Unknown,
                        if exact_authority.is_some() {
                            "runtime.cleanup.current_runtime_absence_unverifiable"
                        } else {
                            "runtime.cleanup.restart_absence_unverifiable"
                        },
                    )
                }
                ProcessProvenanceDisposition::Mismatch => {
                    summary.quarantined_count = summary.quarantined_count.saturating_add(1);
                    (
                        RuntimeHandleState::Quarantined,
                        CleanupOutcome::Unknown,
                        CleanupStepDisposition::Unknown,
                        "runtime.cleanup.restart_provenance_mismatch",
                    )
                }
                ProcessProvenanceDisposition::Match | ProcessProvenanceDisposition::Unsupported => {
                    summary.orphaned_count = summary.orphaned_count.saturating_add(1);
                    (
                        RuntimeHandleState::Orphaned,
                        CleanupOutcome::Unknown,
                        CleanupStepDisposition::Unknown,
                        if expired {
                            "runtime.cleanup.expired_lease_unverifiable"
                        } else {
                            "runtime.cleanup.restart_ownership_unavailable"
                        },
                    )
                }
            };
            let report_id = process_reconciliation_report_id(&record, disposition, expired);
            let replayed_report = self
                .journal_store
                .cleanup_report_for_exact_replay(report_id.as_str())
                .map_err(|error| {
                    map_orchestrator_store_error("load exact process reconciliation report", error)
                })?;
            // Exact replay reuses only the originally committed observation time. Every other
            // field is reconstructed from current evidence so semantic drift still fails closed.
            let completed_at_unix_ms = replayed_report.as_ref().map_or_else(
                || now.max(record.descriptor.created_at_unix_ms),
                |report| report.completed_at_unix_ms,
            );
            let evidence_sha256 = (step_disposition == CleanupStepDisposition::Completed)
                .then(|| process_reconciliation_evidence_sha256(&record, disposition, expired));
            let report = CleanupReportV1 {
                schema_version: palyra_common::runtime_contracts::RUNTIME_HANDLE_SCHEMA_VERSION,
                report_id,
                instance_id: record.descriptor.instance_id.clone(),
                lease_id: Some(record.lease.lease_id.clone()),
                outcome,
                steps: vec![CleanupStepRecord {
                    ordinal: 0,
                    step: CleanupStepKind::VerifyAbsence,
                    disposition: step_disposition,
                    reason_code: reason_code.to_owned(),
                    evidence_sha256,
                    completed_at_unix_ms,
                }],
                reason_code: reason_code.to_owned(),
                completed_at_unix_ms,
            };
            if replayed_report.as_ref().is_some_and(|exact| exact != &report) {
                return Err(Status::failed_precondition(
                    "exact process reconciliation report conflicts with current durable evidence",
                ));
            }
            let mut descriptor = record.descriptor;
            descriptor.state = handle_state;
            descriptor.updated_at_unix_ms = completed_at_unix_ms;
            let cleanup_run_id =
                descriptor.run_id.as_ref().map(|run_id| run_id.as_str().to_owned());
            self.journal_store.finalize_process_cleanup(&descriptor, &report).map_err(|error| {
                map_orchestrator_store_error("reconcile persisted process lease", error)
            })?;
            if let Some(run_id) = cleanup_run_id.as_deref() {
                if let Err(error) =
                    self.journal_store.finalize_startup_recovery_metadata_trace(run_id)
                {
                    warn!(
                        run_id_sha256 = crate::metadata_trace::hash_metadata_trace_run_id(run_id)
                            .unwrap_or_else(|| "invalid".to_owned()),
                        error = %error,
                        "failed to finalize startup-recovery trace after process reconciliation"
                    );
                }
            }
            if handle_state == RuntimeHandleState::Closed {
                if let Some((run_id, process)) = exact_authority {
                    self.forget_run_background_process(
                        run_id.as_str(),
                        process.ownership_root_pid(),
                    );
                    crate::sandbox_runner::release_background_process_cleanup_authority(
                        process.ownership_root_pid(),
                        &process.lease.provenance,
                    );
                }
            }
        }
        self.journal_store
            .update_process_reconciliation_checkpoint(
                PROCESS_LEASE_RECONCILIATION_CHECKPOINT_KEY,
                if page_is_full { next_cursor.as_deref() } else { None },
                now,
            )
            .map_err(|error| {
                map_orchestrator_store_error("persist process reconciliation checkpoint", error)
            })?;
        Ok(summary)
    }

    /// Runs one serialized process-lease reconciliation pass on a blocking worker.
    ///
    /// # Errors
    /// Returns the blocking reconciliation error, or `internal` when the worker panics.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn reconcile_persisted_process_leases_async(
        self: &Arc<Self>,
    ) -> Result<ProcessLeaseReconciliationReport, Status> {
        let _guard = self.process_lease_reconciliation_lock.lock().await;
        #[cfg(test)]
        let _activity = ProcessLeaseReconciliationActivity::begin(self.as_ref());
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let pending = state.reconcile_pending_process_cleanups()?;
            let mut durable = state.reconcile_persisted_process_leases()?;
            durable.pending_cleanup_inspected_count = pending.inspected_count;
            durable.pending_cleanup_completed_count = pending.completed_count;
            durable.pending_cleanup_count = pending.pending_count;
            Ok(durable)
        })
        .await
        .map_err(|_| Status::internal("process lease reconciliation worker panicked"))?
    }

    #[cfg(test)]
    pub(crate) fn set_background_process_registration_publication_barrier(
        &self,
        committed: Arc<Notify>,
        release: Arc<Notify>,
    ) {
        *self
            .background_process_registration_committed
            .lock()
            .expect("process registration test hook lock poisoned") = Some(committed);
        *self
            .background_process_registration_release
            .lock()
            .expect("process registration test hook lock poisoned") = Some(release);
    }

    #[cfg(test)]
    pub(crate) fn clear_background_process_registration_publication_barrier(&self) {
        *self
            .background_process_registration_committed
            .lock()
            .expect("process registration test hook lock poisoned") = None;
        *self
            .background_process_registration_release
            .lock()
            .expect("process registration test hook lock poisoned") = None;
    }

    #[cfg(test)]
    pub(crate) fn process_lease_reconciliation_max_active(&self) -> u64 {
        self.process_lease_reconciliation_max_active.load(Ordering::SeqCst)
    }

    /// Persists structured cleanup evidence, terminal handle state, and lease retirement.
    ///
    /// # Errors
    /// Returns the mapped journal error when validation or persistence fails.
    #[allow(clippy::result_large_err)]
    pub(crate) fn finalize_process_cleanup(
        &self,
        process: &RunOwnedBackgroundProcess,
        report: &CleanupReportV1,
        state: palyra_common::runtime_contracts::RuntimeHandleState,
    ) -> Result<(), Status> {
        let normalize_replay = |existing: CleanupReportV1| -> Result<CleanupReportV1, Status> {
            // Retry callers reconstruct semantic cleanup evidence but cannot
            // recreate its original observation timestamps. Restore only
            // those immutable times, then require the full typed report to
            // match before delegating to the journal's exact replay check.
            let mut replay = report.clone();
            replay.completed_at_unix_ms = existing.completed_at_unix_ms;
            if replay.steps.len() != existing.steps.len() {
                return Err(Status::failed_precondition(
                    "process cleanup replay conflicts with ordered durable evidence",
                ));
            }
            for (step, existing_step) in replay.steps.iter_mut().zip(existing.steps.iter()) {
                step.completed_at_unix_ms = existing_step.completed_at_unix_ms;
            }
            if replay != existing {
                return Err(Status::failed_precondition(
                    "process cleanup replay conflicts with durable evidence",
                ));
            }
            Ok(existing)
        };
        let existing =
            self.journal_store.cleanup_report_for_exact_replay(report.report_id.as_str()).map_err(
                |error| map_orchestrator_store_error("load exact process cleanup report", error),
            )?;
        let may_race_first_writer = existing.is_none();
        let effective_report = match existing {
            Some(existing) => normalize_replay(existing)?,
            None => report.clone(),
        };
        let mut descriptor = process.descriptor.clone();
        descriptor.state = state;
        descriptor.updated_at_unix_ms = effective_report.completed_at_unix_ms;
        let first_result =
            self.journal_store.finalize_process_cleanup(&descriptor, &effective_report);
        let Err(first_error) = first_result else {
            self.orchestrator_run_notify.notify_waiters();
            return Ok(());
        };
        if !may_race_first_writer {
            return Err(map_orchestrator_store_error("finalize process cleanup", first_error));
        }

        // One concurrent first writer may have committed the same report after
        // the pre-read. Canonicalize against that durable winner and retry once.
        let Some(existing) =
            self.journal_store.cleanup_report_for_exact_replay(report.report_id.as_str()).map_err(
                |error| map_orchestrator_store_error("reload raced process cleanup report", error),
            )?
        else {
            return Err(map_orchestrator_store_error("finalize process cleanup", first_error));
        };
        let effective_report = normalize_replay(existing)?;
        descriptor.updated_at_unix_ms = effective_report.completed_at_unix_ms;
        self.journal_store
            .finalize_process_cleanup(&descriptor, &effective_report)
            .map_err(|error| map_orchestrator_store_error("replay raced process cleanup", error))?;
        self.orchestrator_run_notify.notify_waiters();
        Ok(())
    }

    /// Closes the terminal metadata trace after run-owned cleanup has emitted its evidence.
    #[allow(clippy::result_large_err)]
    pub(crate) fn finalize_orchestrator_run_metadata_trace(
        &self,
        run_id: &str,
    ) -> Result<(), Status> {
        self.journal_store.finalize_orchestrator_run_metadata_trace(run_id).map_err(|error| {
            map_orchestrator_store_error("finalize orchestrator run metadata trace", error)
        })
    }

    /// Lists the exact background process leases currently registered for a run.
    pub(crate) fn list_run_background_processes(
        &self,
        run_id: &str,
    ) -> Vec<RunOwnedBackgroundProcess> {
        let run_id = run_id.trim();
        if run_id.is_empty() {
            return Vec::new();
        }

        match self.run_cleanup_resources.lock() {
            Ok(resources_by_run) => resources_by_run
                .get(run_id)
                .map(|resources| resources.background_processes.clone())
                .unwrap_or_default(),
            Err(error) => {
                warn!(
                    run_id,
                    error = %error,
                    "failed to list background processes for run cleanup"
                );
                Vec::new()
            }
        }
    }

    /// Lists run-owned process leases without blocking an async executor worker on publication.
    ///
    /// # Errors
    /// Returns a join error if the blocking snapshot worker panics or is cancelled at shutdown.
    pub(crate) async fn list_run_background_processes_for_cleanup(
        self: &Arc<Self>,
        run_id: &str,
    ) -> Result<Vec<RunOwnedBackgroundProcess>, tokio::task::JoinError> {
        let state = Arc::clone(self);
        let run_id = run_id.to_owned();
        tokio::task::spawn_blocking(move || state.list_run_background_processes(run_id.as_str()))
            .await
    }

    /// Registers a detached background process for terminal handoff reporting
    /// without adding it to terminal cleanup.
    pub(crate) fn record_run_detached_background_process(
        &self,
        run_id: &str,
        resource: DetachedBackgroundProcessResource,
    ) {
        let run_id = run_id.trim();
        if run_id.is_empty() || resource.pid == 0 {
            return;
        }

        match self.run_detached_resources.lock() {
            Ok(mut resources_by_run) => {
                let resources = resources_by_run.entry(run_id.to_owned()).or_default();
                if let Some(existing) = resources
                    .background_processes
                    .iter_mut()
                    .find(|existing| existing.pid == resource.pid)
                {
                    *existing = resource;
                } else {
                    resources.background_processes.push(resource);
                }
            }
            Err(error) => {
                warn!(
                    run_id,
                    pid = resource.pid,
                    error = %error,
                    "failed to record detached background process for run handoff"
                );
            }
        }
    }

    /// Removes and returns detached resources for a terminal-run handoff
    /// summary. The resources are not stopped here.
    pub(crate) fn take_run_detached_resources(&self, run_id: &str) -> RunDetachedResources {
        let run_id = run_id.trim();
        if run_id.is_empty() {
            return RunDetachedResources::default();
        }

        match self.run_detached_resources.lock() {
            Ok(mut resources_by_run) => resources_by_run.remove(run_id).unwrap_or_default(),
            Err(error) => {
                warn!(
                    run_id,
                    error = %error,
                    "failed to take detached background resources"
                );
                RunDetachedResources::default()
            }
        }
    }

    /// Removes and returns everything still registered for the run.
    ///
    /// This test-only broad drain verifies registry state. Terminal cleanup uses
    /// [`Self::take_run_browser_sessions`] and a process snapshot instead so process-group or Job
    /// Object authority remains available until durable cleanup finalization commits.
    #[cfg(test)]
    pub(crate) fn take_run_cleanup_resources(&self, run_id: &str) -> RunCleanupResources {
        let run_id = run_id.trim();
        if run_id.is_empty() {
            return RunCleanupResources::default();
        }

        match self.run_cleanup_resources.lock() {
            Ok(mut resources_by_run) => resources_by_run.remove(run_id).unwrap_or_default(),
            Err(error) => {
                warn!(
                    run_id,
                    error = %error,
                    "failed to take run cleanup resources"
                );
                RunCleanupResources::default()
            }
        }
    }

    /// Drains browser-session cleanup ownership while retaining exact process authority.
    pub(crate) fn take_run_browser_sessions(&self, run_id: &str) -> Vec<String> {
        let run_id = run_id.trim();
        if run_id.is_empty() {
            return Vec::new();
        }

        match self.run_cleanup_resources.lock() {
            Ok(mut resources_by_run) => {
                let Some(resources) = resources_by_run.get_mut(run_id) else {
                    return Vec::new();
                };
                let browser_session_ids = std::mem::take(&mut resources.browser_session_ids);
                if resources.is_empty() {
                    resources_by_run.remove(run_id);
                }
                browser_session_ids
            }
            Err(error) => {
                warn!(
                    run_id,
                    error = %error,
                    "failed to take run browser sessions for cleanup"
                );
                Vec::new()
            }
        }
    }

    // Self-healing: thin delegates to `SelfHealingState`; the names mirror
    // the methods they forward to.

    #[must_use]
    pub(crate) fn self_healing_settings_snapshot(&self) -> SelfHealingSettingsSnapshot {
        self.self_healing.settings_snapshot()
    }

    #[must_use]
    pub(crate) fn self_healing_incident_summary(&self) -> RuntimeIncidentSummary {
        self.self_healing.incident_summary()
    }

    #[must_use]
    pub(crate) fn self_healing_active_incidents(&self, limit: usize) -> Vec<RuntimeIncidentRecord> {
        self.self_healing.active_incidents(limit)
    }

    #[must_use]
    pub(crate) fn self_healing_recent_history(
        &self,
        limit: usize,
    ) -> Vec<RuntimeIncidentHistoryEntry> {
        self.self_healing.recent_incident_history(limit)
    }

    #[must_use]
    pub(crate) fn self_healing_recent_remediation_attempts(
        &self,
        limit: usize,
    ) -> Vec<RuntimeRemediationAttemptRecord> {
        self.self_healing.recent_remediation_attempts(limit)
    }

    #[must_use]
    pub(crate) fn self_healing_heartbeats(&self) -> Vec<WorkHeartbeatRecord> {
        self.self_healing.list_heartbeats()
    }

    #[must_use]
    pub(crate) fn self_healing_heartbeat(
        &self,
        kind: WorkHeartbeatKind,
        object_id: &str,
    ) -> Option<WorkHeartbeatRecord> {
        self.self_healing.heartbeat(kind, object_id)
    }

    #[must_use]
    pub(crate) fn latest_orphan_reconciliation(&self) -> Option<OrphanReconciliationReport> {
        self.self_healing.latest_orphan_reconciliation()
    }

    pub(crate) fn record_self_healing_heartbeat(&self, update: WorkHeartbeatUpdate) {
        self.self_healing.record_heartbeat(update);
    }

    pub(crate) fn clear_self_healing_heartbeat(&self, kind: WorkHeartbeatKind, object_id: &str) {
        self.self_healing.clear_heartbeat(kind, object_id);
    }

    pub(crate) fn clear_self_healing_heartbeat_if_generation(
        &self,
        kind: WorkHeartbeatKind,
        object_id: &str,
        execution_generation: u64,
    ) {
        self.self_healing.clear_heartbeat_if_generation(
            kind,
            object_id,
            Some(execution_generation),
        );
    }

    #[must_use]
    pub(crate) fn observe_self_healing_incident(
        &self,
        observation: RuntimeIncidentObservation,
    ) -> RuntimeIncidentRecord {
        self.self_healing.observe_incident(observation)
    }

    pub(crate) fn resolve_self_healing_incident(
        &self,
        domain: IncidentDomain,
        dedupe_key: &str,
        summary: &str,
    ) {
        self.self_healing.resolve_incident(domain, dedupe_key, summary);
    }

    #[must_use]
    pub(crate) fn record_self_healing_remediation_attempt(
        &self,
        incident_id: &str,
        remediation_id: &str,
        feature: SelfHealingFeature,
        status: RemediationAttemptStatus,
        detail: impl Into<String>,
    ) -> RuntimeRemediationAttemptRecord {
        self.self_healing.record_remediation_attempt(
            incident_id,
            remediation_id,
            feature,
            status,
            detail,
        )
    }

    pub(crate) fn inspect_stuck_run_incident(
        &self,
        heartbeat: &WorkHeartbeatRecord,
    ) -> Result<Option<StuckRunIncidentV2>, JournalError> {
        self.journal_store.inspect_stuck_run_incident(
            heartbeat.object_id.as_str(),
            heartbeat.execution_generation,
            heartbeat.updated_at_unix_ms,
        )
    }

    pub(crate) fn record_stuck_run_remediation_decision(
        &self,
        decision: &RemediationDecision,
    ) -> Result<(), JournalError> {
        self.journal_store.record_stuck_run_remediation_decision(decision)
    }

    pub(crate) fn claim_stuck_run_remediation(
        &self,
        incident: &StuckRunIncidentV2,
        worker_id: &str,
        claim_ttl_ms: i64,
    ) -> Result<StuckRunRemediationClaimOutcome, JournalError> {
        self.journal_store.claim_stuck_run_remediation(incident, worker_id, claim_ttl_ms)
    }

    pub(crate) fn complete_stuck_run_remediation(
        &self,
        incident: &StuckRunIncidentV2,
        worker_id: &str,
        claim_epoch: u64,
    ) -> Result<StuckRunRemediationCompletionOutcome, JournalError> {
        self.journal_store.complete_stuck_run_remediation(incident, worker_id, claim_epoch)
    }

    pub(crate) fn record_channel_message_routed(&self) {
        self.counters.channel_messages_routed.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_channel_message_replied(&self) {
        self.counters.channel_messages_replied.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_channel_reply_failure(&self) {
        self.counters.channel_reply_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn refresh_channel_router_queue_depth(&self) {
        self.counters
            .channel_router_queue_depth
            .store(self.channel_router.queue_depth() as u64, Ordering::Relaxed);
    }

    pub(crate) fn record_tool_proposal(&self) {
        self.counters.tool_proposals.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_tool_execution_attempt(&self) {
        self.counters.tool_execution_attempts.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_run_stream_progress_coalesced(&self, count: u64) {
        self.counters.run_stream_progress_coalesced.fetch_add(count, Ordering::Relaxed);
    }

    pub(crate) fn record_run_stream_tool_deadline_exceeded(&self) {
        self.counters.run_stream_tool_deadline_exceeded.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_run_stream_approval_cancelled(&self) {
        self.counters.run_stream_approval_cancelled.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_run_stream_terminal_delivery_timeout(&self) {
        self.counters.run_stream_terminal_delivery_timeouts.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_run_interrupt_latency(&self, observation: RunInterruptLatencyObservation) {
        self.counters.run_interrupt_latency.record(observation);
    }

    pub(crate) fn record_tool_attestation_emitted(&self) {
        self.counters.tool_attestations_emitted.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_memory_auto_inject_event(&self) {
        self.counters.memory_auto_inject_events.fetch_add(1, Ordering::Relaxed);
    }

    /// Prepends a context assembly trace, keeping only the most recent few
    /// for the diagnostics endpoint.
    pub(crate) fn record_context_assembly_trace(&self, trace: Value) {
        const MAX_RECENT_CONTEXT_ASSEMBLY_TRACES: usize = 16;

        let mut traces = match self.recent_context_assembly_traces.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!("context assembly trace buffer lock poisoned while recording trace");
                poisoned.into_inner()
            }
        };
        traces.insert(0, trace);
        traces.truncate(MAX_RECENT_CONTEXT_ASSEMBLY_TRACES);
    }

    /// Copies the retained context assembly traces, newest first.
    #[must_use]
    pub(crate) fn context_assembly_traces_snapshot(&self) -> Vec<Value> {
        match self.recent_context_assembly_traces.lock() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => {
                warn!("context assembly trace buffer lock poisoned while reading trace snapshot");
                poisoned.into_inner().clone()
            }
        }
    }

    pub(crate) fn record_learning_reflection_scheduled(&self) {
        self.counters.learning_reflections_scheduled.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_learning_reflection_completed(&self) {
        self.counters.learning_reflections_completed.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_learning_candidate_created(&self) {
        self.counters.learning_candidates_created.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_learning_candidate_auto_applied(&self) {
        self.counters.learning_candidates_auto_applied.fetch_add(1, Ordering::Relaxed);
    }

    /// Counts an allow/deny tool decision. Denials also count as denied
    /// requests, and process-runner denials additionally count as sandbox
    /// policy denies.
    pub(crate) fn record_tool_decision(&self, tool_name: &str, decision_allowed: bool) {
        if decision_allowed {
            self.counters.tool_decisions_allowed.fetch_add(1, Ordering::Relaxed);
            return;
        }

        self.counters.tool_decisions_denied.fetch_add(1, Ordering::Relaxed);
        self.record_denied();
        if tool_name == PROCESS_RUNNER_TOOL_NAME {
            self.counters.sandbox_policy_denies.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn record_skill_execution_denied(&self) {
        self.counters.skill_execution_denied.fetch_add(1, Ordering::Relaxed);
    }

    // Canvas host. In-memory records live behind `canvas_records`; every
    // state transition is journaled while that lock is held so the persisted
    // patch sequence can never diverge from the in-memory version counter.
    // Public HTTP access (frame, runtime assets, state polling) is authorized
    // exclusively through signed canvas tokens.

    #[allow(clippy::result_large_err)]
    fn ensure_canvas_host_enabled(&self) -> Result<(), Status> {
        if self.config.canvas_host.enabled {
            Ok(())
        } else {
            Err(Status::failed_precondition("canvas host is disabled (canvas_host.enabled=false)"))
        }
    }

    /// Creates a canvas: validates state/bundle limits, signs the bundle,
    /// journals the initial transition, and issues the first runtime
    /// descriptor with a signed auth token.
    ///
    /// # Errors
    /// `failed_precondition` when the canvas host is disabled,
    /// `invalid_argument`/`resource_exhausted` for malformed or oversized
    /// payloads, and `already_exists` for duplicate canvas ids.
    #[allow(clippy::result_large_err)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create_canvas(
        &self,
        context: &RequestContext,
        requested_canvas_id: Option<String>,
        session_id: String,
        initial_state_json: &[u8],
        initial_state_version: u64,
        requested_state_schema_version: Option<u64>,
        bundle: gateway_v1::CanvasBundle,
        allowed_parent_origins: Vec<String>,
        requested_token_ttl_seconds: Option<u32>,
    ) -> Result<(CanvasRecord, CanvasRuntimeDescriptor), Status> {
        self.ensure_canvas_host_enabled()?;
        if initial_state_json.len() > self.config.canvas_host.max_state_bytes {
            return Err(Status::resource_exhausted(format!(
                "canvas state payload exceeds limit ({} > {})",
                initial_state_json.len(),
                self.config.canvas_host.max_state_bytes
            )));
        }
        let validated_initial_state =
            serde_json::from_slice::<Value>(initial_state_json).map_err(|error| {
                Status::invalid_argument(format!("initial_state_json must be valid JSON: {error}"))
            })?;
        let state_schema_version = resolve_canvas_state_schema_version(
            requested_state_schema_version,
            &validated_initial_state,
            None,
        )?;
        let canonical_initial_state_json =
            serde_json::to_vec(&validated_initial_state).map_err(|error| {
                Status::internal(format!("failed to encode initial state JSON: {error}"))
            })?;
        let initial_patch = build_replace_root_patch(&validated_initial_state);
        let initial_patch_json = patch_document_to_bytes(&initial_patch).map_err(|error| {
            Status::internal(format!("failed to encode initial canvas patch payload: {error}"))
        })?;
        let now_unix_ms = unix_ms_now_for_status()?;
        let canvas_id = match requested_canvas_id {
            Some(value) => normalize_canvas_identifier(value.as_str(), "canvas_id")?,
            None => Ulid::new().to_string(),
        };
        let state_version = if initial_state_version == 0 { 1 } else { initial_state_version };
        ensure_canvas_version_fits_sqlite("state_version", state_version)?;
        let allowed_parent_origins =
            parse_canvas_allowed_parent_origins(allowed_parent_origins.as_slice())?;
        let mut bundle = self.parse_canvas_bundle(bundle)?;
        let token_ttl_ms =
            self.resolve_canvas_token_ttl_ms(requested_token_ttl_seconds.unwrap_or_default())?;
        let expires_at_unix_ms = now_unix_ms.saturating_add(token_ttl_ms as i64);
        bundle.signature = self.sign_canvas_bundle(
            canvas_id.as_str(),
            bundle.sha256.as_str(),
            context.principal.as_str(),
            session_id.as_str(),
        );

        // The registry lock stays held through the journal write below so the
        // duplicate-id check and the persisted transition are atomic with the
        // in-memory insert.
        let mut records = self
            .canvas_records
            .lock()
            .map_err(|_| Status::internal("canvas registry lock poisoned"))?;
        if records.contains_key(canvas_id.as_str()) {
            return Err(Status::already_exists(format!("canvas already exists: {canvas_id}")));
        }

        let record = CanvasRecord {
            canvas_id: canvas_id.clone(),
            session_id: session_id.clone(),
            principal: context.principal.clone(),
            state_version,
            state_schema_version,
            state_json: canonical_initial_state_json.clone(),
            bundle,
            allowed_parent_origins,
            created_at_unix_ms: now_unix_ms,
            updated_at_unix_ms: now_unix_ms,
            expires_at_unix_ms,
            closed: false,
            close_reason: None,
            update_timestamps_unix_ms: VecDeque::new(),
        };
        let transition = CanvasStateTransitionRequest {
            canvas_id: record.canvas_id.clone(),
            session_id: record.session_id.clone(),
            principal: record.principal.clone(),
            state_version: record.state_version,
            base_state_version: 0,
            state_schema_version: record.state_schema_version,
            state_json: String::from_utf8(record.state_json.clone()).map_err(|error| {
                Status::internal(format!("failed to encode initial state JSON as UTF-8: {error}"))
            })?,
            patch_json: String::from_utf8(initial_patch_json).map_err(|error| {
                Status::internal(format!("failed to encode initial patch JSON as UTF-8: {error}"))
            })?,
            bundle_json: serde_json::to_string(&record.bundle).map_err(|error| {
                Status::internal(format!("failed to encode canvas bundle for persistence: {error}"))
            })?,
            allowed_parent_origins_json: serde_json::to_string(&record.allowed_parent_origins)
                .map_err(|error| {
                    Status::internal(format!(
                        "failed to encode canvas origin allowlist for persistence: {error}"
                    ))
                })?,
            created_at_unix_ms: record.created_at_unix_ms,
            updated_at_unix_ms: record.updated_at_unix_ms,
            expires_at_unix_ms: record.expires_at_unix_ms,
            closed: record.closed,
            close_reason: record.close_reason.clone(),
            actor_principal: context.principal.clone(),
            actor_device_id: context.device_id.clone(),
        };
        self.journal_store
            .record_canvas_state_transition(&transition)
            .map_err(|error| map_canvas_store_error("record_canvas_state_transition", error))?;
        records.insert(canvas_id.clone(), record.clone());
        self.counters.canvas_created.fetch_add(1, Ordering::Relaxed);

        let auth_token = self.issue_canvas_token(
            canvas_id.as_str(),
            context.principal.as_str(),
            session_id.as_str(),
            now_unix_ms,
            expires_at_unix_ms,
        )?;
        let descriptor = CanvasRuntimeDescriptor {
            canvas_id: canvas_id.clone(),
            frame_url: format!(
                "{}/canvas/v1/frame/{}",
                self.config.canvas_host.public_base_url, canvas_id
            ),
            runtime_url: format!(
                "{}/canvas/v1/runtime.js",
                self.config.canvas_host.public_base_url
            ),
            auth_token,
            expires_at_unix_ms,
        };
        Ok((record, descriptor))
    }

    /// Applies a full-state replacement or JSON patch to a canvas, enforcing
    /// version/schema preconditions and the per-minute update rate limit, and
    /// journals the resulting transition.
    ///
    /// # Errors
    /// `invalid_argument` for malformed payloads, `failed_precondition` for
    /// version/schema mismatches or closed canvases, `permission_denied` for
    /// principal mismatch or expiry, and `resource_exhausted` for size or
    /// rate limits.
    #[allow(clippy::result_large_err)]
    pub(crate) fn update_canvas_state(
        &self,
        context: &RequestContext,
        canvas_id: &str,
        state_json: Option<&[u8]>,
        patch_json: Option<&[u8]>,
        expected_state_version: Option<u64>,
        expected_state_schema_version: Option<u64>,
    ) -> Result<CanvasRecord, Status> {
        self.ensure_canvas_host_enabled()?;
        let has_state_payload = state_json.is_some_and(|payload| !payload.is_empty());
        let has_patch_payload = patch_json.is_some_and(|payload| !payload.is_empty());
        if !has_state_payload && !has_patch_payload {
            return Err(Status::invalid_argument(
                "canvas update requires non-empty state_json or patch_json payload",
            ));
        }
        if has_state_payload && has_patch_payload {
            return Err(Status::invalid_argument(
                "canvas update accepts either state_json or patch_json, not both",
            ));
        }
        if let Some(payload) = state_json {
            if payload.len() > self.config.canvas_host.max_state_bytes {
                return Err(Status::resource_exhausted(format!(
                    "canvas state payload exceeds limit ({} > {})",
                    payload.len(),
                    self.config.canvas_host.max_state_bytes
                )));
            }
        }
        if let Some(payload) = patch_json {
            if payload.len() > self.config.canvas_host.max_state_bytes {
                return Err(Status::resource_exhausted(format!(
                    "canvas patch payload exceeds limit ({} > {})",
                    payload.len(),
                    self.config.canvas_host.max_state_bytes
                )));
            }
        }
        let normalized_canvas_id = normalize_canvas_identifier(canvas_id, "canvas_id")?;
        let now_unix_ms = unix_ms_now_for_status()?;
        let mut records = self
            .canvas_records
            .lock()
            .map_err(|_| Status::internal("canvas registry lock poisoned"))?;
        let Some(record) = records.get_mut(normalized_canvas_id.as_str()) else {
            return Err(Status::not_found(format!("canvas not found: {normalized_canvas_id}")));
        };
        if record.principal != context.principal {
            self.counters.canvas_denied.fetch_add(1, Ordering::Relaxed);
            return Err(Status::permission_denied("canvas access denied: principal mismatch"));
        }
        if record.closed {
            return Err(Status::failed_precondition("canvas is closed and cannot be updated"));
        }
        if record.expires_at_unix_ms <= now_unix_ms {
            self.counters.canvas_denied.fetch_add(1, Ordering::Relaxed);
            return Err(Status::permission_denied("canvas token/session expired"));
        }
        if let Some(expected_state_version) = expected_state_version {
            if expected_state_version > 0 && record.state_version != expected_state_version {
                return Err(Status::failed_precondition(format!(
                    "canvas version mismatch (expected {}, current {})",
                    expected_state_version, record.state_version
                )));
            }
        }
        if let Some(expected_state_schema_version) = expected_state_schema_version {
            if expected_state_schema_version > 0
                && record.state_schema_version != expected_state_schema_version
            {
                return Err(Status::failed_precondition(format!(
                    "canvas schema mismatch (expected {}, current {})",
                    expected_state_schema_version, record.state_schema_version
                )));
            }
        }

        let current_state: Value =
            serde_json::from_slice(record.state_json.as_slice()).map_err(|error| {
                Status::internal(format!("persisted canvas state JSON is invalid: {error}"))
            })?;
        let (next_state, patch_document) = if let Some(payload) = state_json {
            let next_state = serde_json::from_slice::<Value>(payload).map_err(|error| {
                Status::invalid_argument(format!("state_json must be valid JSON: {error}"))
            })?;
            (next_state.clone(), build_replace_root_patch(&next_state))
        } else {
            let payload = patch_json.ok_or_else(|| {
                Status::invalid_argument("canvas patch update requires non-empty patch_json")
            })?;
            let patch_document = parse_patch_document(payload).map_err(|error| {
                Status::invalid_argument(format!("patch_json is invalid: {error}"))
            })?;
            let next_state =
                apply_patch_document(&current_state, &patch_document).map_err(|error| {
                    Status::failed_precondition(format!("patch application failed: {error}"))
                })?;
            (next_state, patch_document)
        };
        let next_state_schema_version = resolve_canvas_state_schema_version(
            None,
            &next_state,
            Some(record.state_schema_version),
        )?;
        let canonical_next_state_json = serde_json::to_vec(&next_state).map_err(|error| {
            Status::internal(format!("failed to encode next state JSON: {error}"))
        })?;
        if canonical_next_state_json.len() > self.config.canvas_host.max_state_bytes {
            return Err(Status::resource_exhausted(format!(
                "canvas state payload exceeds limit after patch apply ({} > {})",
                canonical_next_state_json.len(),
                self.config.canvas_host.max_state_bytes
            )));
        }
        let canonical_patch_json = patch_document_to_bytes(&patch_document)
            .map_err(|error| Status::internal(format!("failed to encode patch JSON: {error}")))?;
        if record.state_version >= MAX_CANVAS_SQLITE_VERSION {
            return Err(Status::failed_precondition(format!(
                "canvas state_version cannot advance beyond maximum supported value {MAX_CANVAS_SQLITE_VERSION}"
            )));
        }
        let next_state_version = record.state_version + 1;

        // Sliding-window rate limit: drop timestamps older than one minute,
        // then reject the update if the window is already at capacity.
        while record
            .update_timestamps_unix_ms
            .front()
            .is_some_and(|value| now_unix_ms.saturating_sub(*value) > 60_000)
        {
            let _ = record.update_timestamps_unix_ms.pop_front();
        }
        if record.update_timestamps_unix_ms.len() >= self.config.canvas_host.max_updates_per_minute
        {
            return Err(Status::resource_exhausted(format!(
                "canvas update rate limit exceeded (>{} updates/minute)",
                self.config.canvas_host.max_updates_per_minute
            )));
        }
        let transition = CanvasStateTransitionRequest {
            canvas_id: record.canvas_id.clone(),
            session_id: record.session_id.clone(),
            principal: record.principal.clone(),
            state_version: next_state_version,
            base_state_version: record.state_version,
            state_schema_version: next_state_schema_version,
            state_json: String::from_utf8(canonical_next_state_json.clone()).map_err(|error| {
                Status::internal(format!("failed to encode state JSON as UTF-8: {error}"))
            })?,
            patch_json: String::from_utf8(canonical_patch_json).map_err(|error| {
                Status::internal(format!("failed to encode patch JSON as UTF-8: {error}"))
            })?,
            bundle_json: serde_json::to_string(&record.bundle).map_err(|error| {
                Status::internal(format!("failed to encode canvas bundle for persistence: {error}"))
            })?,
            allowed_parent_origins_json: serde_json::to_string(&record.allowed_parent_origins)
                .map_err(|error| {
                    Status::internal(format!(
                        "failed to encode canvas origin allowlist for persistence: {error}"
                    ))
                })?,
            created_at_unix_ms: record.created_at_unix_ms,
            updated_at_unix_ms: now_unix_ms,
            expires_at_unix_ms: record.expires_at_unix_ms,
            closed: record.closed,
            close_reason: record.close_reason.clone(),
            actor_principal: context.principal.clone(),
            actor_device_id: context.device_id.clone(),
        };
        self.journal_store
            .record_canvas_state_transition(&transition)
            .map_err(|error| map_canvas_store_error("record_canvas_state_transition", error))?;

        record.update_timestamps_unix_ms.push_back(now_unix_ms);
        record.state_version = next_state_version;
        record.state_schema_version = next_state_schema_version;
        record.state_json = canonical_next_state_json;
        record.updated_at_unix_ms = now_unix_ms;
        self.counters.canvas_updated.fetch_add(1, Ordering::Relaxed);
        Ok(record.clone())
    }

    /// Closes a canvas (idempotent), journaling one final transition with the
    /// close reason.
    ///
    /// # Errors
    /// `not_found` for unknown ids, `permission_denied` for principal
    /// mismatch, and journal mapping errors from persisting the transition.
    #[allow(clippy::result_large_err)]
    pub(crate) fn close_canvas(
        &self,
        context: &RequestContext,
        canvas_id: &str,
        reason: Option<String>,
    ) -> Result<CanvasRecord, Status> {
        self.ensure_canvas_host_enabled()?;
        let normalized_canvas_id = normalize_canvas_identifier(canvas_id, "canvas_id")?;
        let now_unix_ms = unix_ms_now_for_status()?;
        let mut records = self
            .canvas_records
            .lock()
            .map_err(|_| Status::internal("canvas registry lock poisoned"))?;
        let Some(record) = records.get_mut(normalized_canvas_id.as_str()) else {
            return Err(Status::not_found(format!("canvas not found: {normalized_canvas_id}")));
        };
        if record.principal != context.principal {
            self.counters.canvas_denied.fetch_add(1, Ordering::Relaxed);
            return Err(Status::permission_denied("canvas access denied: principal mismatch"));
        }
        if !record.closed {
            let resolved_reason =
                reason.and_then(non_empty).or_else(|| Some("closed_by_operator".to_owned()));
            let current_state: Value = serde_json::from_slice(record.state_json.as_slice())
                .map_err(|error| {
                    Status::internal(format!("persisted canvas state JSON is invalid: {error}"))
                })?;
            let close_patch = build_replace_root_patch(&current_state);
            let close_patch_json = patch_document_to_bytes(&close_patch).map_err(|error| {
                Status::internal(format!("failed to encode close patch: {error}"))
            })?;
            if record.state_version >= MAX_CANVAS_SQLITE_VERSION {
                return Err(Status::failed_precondition(format!(
                    "canvas state_version cannot advance beyond maximum supported value {MAX_CANVAS_SQLITE_VERSION}"
                )));
            }
            let next_state_version = record.state_version + 1;
            let transition = CanvasStateTransitionRequest {
                canvas_id: record.canvas_id.clone(),
                session_id: record.session_id.clone(),
                principal: record.principal.clone(),
                state_version: next_state_version,
                base_state_version: record.state_version,
                state_schema_version: record.state_schema_version,
                state_json: String::from_utf8(record.state_json.clone()).map_err(|error| {
                    Status::internal(format!("failed to encode close state as UTF-8: {error}"))
                })?,
                patch_json: String::from_utf8(close_patch_json).map_err(|error| {
                    Status::internal(format!("failed to encode close patch as UTF-8: {error}"))
                })?,
                bundle_json: serde_json::to_string(&record.bundle).map_err(|error| {
                    Status::internal(format!(
                        "failed to encode canvas bundle for persistence: {error}"
                    ))
                })?,
                allowed_parent_origins_json: serde_json::to_string(&record.allowed_parent_origins)
                    .map_err(|error| {
                        Status::internal(format!(
                            "failed to encode canvas origin allowlist for persistence: {error}"
                        ))
                    })?,
                created_at_unix_ms: record.created_at_unix_ms,
                updated_at_unix_ms: now_unix_ms,
                expires_at_unix_ms: record.expires_at_unix_ms,
                closed: true,
                close_reason: resolved_reason.clone(),
                actor_principal: context.principal.clone(),
                actor_device_id: context.device_id.clone(),
            };
            self.journal_store
                .record_canvas_state_transition(&transition)
                .map_err(|error| map_canvas_store_error("record_canvas_state_transition", error))?;
            record.state_version = next_state_version;
            record.close_reason = resolved_reason;
            record.closed = true;
            record.updated_at_unix_ms = now_unix_ms;
            self.counters.canvas_closed.fetch_add(1, Ordering::Relaxed);
        }
        Ok(record.clone())
    }

    /// Loads a canvas record, enforcing principal ownership.
    ///
    /// # Errors
    /// `not_found` for unknown ids, `permission_denied` for principal
    /// mismatch.
    #[allow(clippy::result_large_err)]
    pub(crate) fn get_canvas(
        &self,
        context: &RequestContext,
        canvas_id: &str,
    ) -> Result<CanvasRecord, Status> {
        self.ensure_canvas_host_enabled()?;
        let normalized_canvas_id = normalize_canvas_identifier(canvas_id, "canvas_id")?;
        let records = self
            .canvas_records
            .lock()
            .map_err(|_| Status::internal("canvas registry lock poisoned"))?;
        let Some(record) = records.get(normalized_canvas_id.as_str()) else {
            return Err(Status::not_found(format!("canvas not found: {normalized_canvas_id}")));
        };
        if record.principal != context.principal {
            self.counters.canvas_denied.fetch_add(1, Ordering::Relaxed);
            return Err(Status::permission_denied("canvas access denied: principal mismatch"));
        }
        Ok(record.clone())
    }

    /// Lists journal-backed state patches after a version (limit clamped to
    /// the streaming batch maximum).
    ///
    /// # Errors
    /// Ownership errors from [`Self::get_canvas`] plus mapped journal errors.
    #[allow(clippy::result_large_err)]
    pub(crate) fn list_canvas_state_patches(
        &self,
        context: &RequestContext,
        canvas_id: &str,
        after_state_version: u64,
        limit: usize,
    ) -> Result<Vec<CanvasStatePatchRecord>, Status> {
        let record = self.get_canvas(context, canvas_id)?;
        let limited = limit.clamp(1, MAX_CANVAS_STREAM_PATCH_BATCH);
        self.journal_store
            .list_canvas_state_patches(record.canvas_id.as_str(), after_state_version, limited)
            .map_err(|error| map_canvas_store_error("list_canvas_state_patches", error))
    }

    /// Lists the principal's canvases for one session, newest update first.
    ///
    /// # Errors
    /// `invalid_argument` for non-canonical session ids; `failed_precondition`
    /// when the canvas host is disabled.
    #[allow(clippy::result_large_err)]
    pub(crate) fn list_session_canvases(
        &self,
        context: &RequestContext,
        session_id: &str,
    ) -> Result<Vec<CanvasRecord>, Status> {
        self.ensure_canvas_host_enabled()?;
        validate_canonical_id(session_id).map_err(|_| {
            Status::invalid_argument("session_id must be a canonical ULID identifier")
        })?;
        let records = self
            .canvas_records
            .lock()
            .map_err(|_| Status::internal("canvas registry lock poisoned"))?;
        let mut scoped = records
            .values()
            .filter(|record| {
                record.principal == context.principal && record.session_id.as_str() == session_id
            })
            .cloned()
            .collect::<Vec<_>>();
        scoped.sort_by(|left, right| {
            right
                .updated_at_unix_ms
                .cmp(&left.updated_at_unix_ms)
                .then_with(|| left.canvas_id.cmp(&right.canvas_id))
        });
        Ok(scoped)
    }

    /// Issues a fresh runtime descriptor (and token) for an existing canvas;
    /// the token expiry never outlives the canvas session itself.
    ///
    /// # Errors
    /// Ownership errors from [`Self::get_canvas`]; `failed_precondition` when
    /// the canvas session already expired.
    #[allow(clippy::result_large_err)]
    pub(crate) fn issue_canvas_runtime_descriptor(
        &self,
        context: &RequestContext,
        canvas_id: &str,
        requested_token_ttl_seconds: Option<u32>,
    ) -> Result<CanvasRuntimeDescriptor, Status> {
        self.ensure_canvas_host_enabled()?;
        let record = self.get_canvas(context, canvas_id)?;
        let now_unix_ms = unix_ms_now_for_status()?;
        if record.expires_at_unix_ms <= now_unix_ms {
            self.counters.canvas_denied.fetch_add(1, Ordering::Relaxed);
            return Err(Status::failed_precondition("canvas session expired"));
        }
        let token_ttl_ms =
            self.resolve_canvas_token_ttl_ms(requested_token_ttl_seconds.unwrap_or_default())?;
        let expires_at_unix_ms =
            now_unix_ms.saturating_add(token_ttl_ms as i64).min(record.expires_at_unix_ms);
        let auth_token = self.issue_canvas_token(
            record.canvas_id.as_str(),
            context.principal.as_str(),
            record.session_id.as_str(),
            now_unix_ms,
            expires_at_unix_ms,
        )?;
        Ok(CanvasRuntimeDescriptor {
            canvas_id: record.canvas_id.clone(),
            frame_url: format!(
                "{}/canvas/v1/frame/{}",
                self.config.canvas_host.public_base_url, record.canvas_id
            ),
            runtime_url: format!(
                "{}/canvas/v1/runtime.js",
                self.config.canvas_host.public_base_url
            ),
            auth_token,
            expires_at_unix_ms,
        })
    }

    /// Restores a canvas to an earlier persisted state version by replaying
    /// that version's resulting state as a new update (history only ever
    /// moves forward; no versions are rewritten).
    ///
    /// # Errors
    /// `invalid_argument` for version 0, `not_found` for unknown versions,
    /// `failed_precondition` for closed canvases or closed target revisions.
    #[allow(clippy::result_large_err)]
    pub(crate) fn restore_canvas_state(
        &self,
        context: &RequestContext,
        canvas_id: &str,
        target_state_version: u64,
    ) -> Result<CanvasRecord, Status> {
        if target_state_version == 0 {
            return Err(Status::invalid_argument("target_state_version must be greater than 0"));
        }
        let record = self.get_canvas(context, canvas_id)?;
        if record.closed {
            return Err(Status::failed_precondition("canvas is closed and cannot be restored"));
        }
        if record.state_version == target_state_version {
            return Ok(record);
        }
        let target_patch = self
            .journal_store
            .get_canvas_state_patch(record.canvas_id.as_str(), target_state_version)
            .map_err(|error| map_canvas_store_error("get_canvas_state_patch", error))?
            .ok_or_else(|| {
                Status::not_found(format!(
                    "canvas state version not found: {}@{}",
                    record.canvas_id, target_state_version
                ))
            })?;
        if target_patch.closed {
            return Err(Status::failed_precondition("closed canvas revisions cannot be restored"));
        }
        self.update_canvas_state(
            context,
            record.canvas_id.as_str(),
            Some(target_patch.resulting_state_json.as_bytes()),
            None,
            Some(record.state_version),
            Some(record.state_schema_version),
        )
    }

    /// Renders the HTML shell for the canvas iframe; access is authorized by
    /// the signed token in the URL, not by request principal.
    ///
    /// # Errors
    /// Token/ownership errors from `Self::authorize_canvas_http_request`.
    #[allow(clippy::result_large_err)]
    pub fn canvas_frame_document(
        &self,
        canvas_id: &str,
        token: &str,
    ) -> Result<CanvasFrameDocument, Status> {
        let record = self.authorize_canvas_http_request(canvas_id, token)?;
        let csp = build_canvas_csp_header(record.allowed_parent_origins.as_slice());
        let encoded_canvas_id = url_encode_component(record.canvas_id.as_str());
        let encoded_entrypoint = url_encode_path_component(record.bundle.entrypoint_path.as_str());
        let encoded_token = url_encode_component(token);
        let mut origins_meta = String::new();
        for origin in record.allowed_parent_origins.iter() {
            origins_meta.push_str("<meta name=\"palyra-canvas-origin\" content=\"");
            origins_meta.push_str(escape_html_attribute(origin).as_str());
            origins_meta.push_str("\" />\n");
        }
        let html = format!(
            concat!(
                "<!doctype html>\n",
                "<html lang=\"en\">\n",
                "<head>\n",
                "<meta charset=\"utf-8\" />\n",
                "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />\n",
                "<title>Palyra Canvas</title>\n",
                "{origins_meta}",
                "<link rel=\"stylesheet\" href=\"/canvas/v1/runtime.css?canvas_id={canvas_id}&token={token}\" />\n",
                "</head>\n",
                "<body>\n",
                "<main id=\"palyra-canvas-root\" data-canvas-id=\"{canvas_id}\"></main>\n",
                "<pre id=\"palyra-canvas-state\" hidden></pre>\n",
                "<script src=\"/canvas/v1/runtime.js?canvas_id={canvas_id}&token={token}\" defer></script>\n",
                "<script src=\"/canvas/v1/bundle/{canvas_id}/{entrypoint}?token={token}\" defer></script>\n",
                "</body>\n",
                "</html>\n"
            ),
            origins_meta = origins_meta,
            canvas_id = encoded_canvas_id,
            entrypoint = encoded_entrypoint,
            token = encoded_token
        );
        Ok(CanvasFrameDocument {
            canvas_id: record.canvas_id,
            html,
            csp,
            expires_at_unix_ms: record.expires_at_unix_ms,
        })
    }

    /// Serves the generated canvas runtime script (state polling plus
    /// postMessage bridge restricted to the allowed parent origins).
    ///
    /// # Errors
    /// Token/ownership errors from `Self::authorize_canvas_http_request`.
    #[allow(clippy::result_large_err)]
    pub fn canvas_runtime_script(
        &self,
        canvas_id: &str,
        token: &str,
    ) -> Result<CanvasAssetResponse, Status> {
        let record = self.authorize_canvas_http_request(canvas_id, token)?;
        let script = format!(
            concat!(
                "(function () {{\n",
                "  'use strict';\n",
                "  const root = document.getElementById('palyra-canvas-root');\n",
                "  const statePreview = document.getElementById('palyra-canvas-state');\n",
                "  const params = new URLSearchParams(window.location.search);\n",
                "  const canvasId = params.get('canvas_id') || {canvas_id_json};\n",
                "  const token = params.get('token') || '';\n",
                "  const allowedOrigins = new Set(Array.from(document.querySelectorAll('meta[name=\"palyra-canvas-origin\"]')).map((node) => node.content));\n",
                "  let stateVersion = 0;\n",
                "  function renderState(state) {{\n",
                "    if (statePreview) {{\n",
                "      statePreview.hidden = false;\n",
                "      statePreview.textContent = JSON.stringify(state, null, 2);\n",
                "    }}\n",
                "    window.dispatchEvent(new CustomEvent('palyra:canvas-state', {{ detail: state }}));\n",
                "  }}\n",
                "  async function pollState() {{\n",
                "    if (!canvasId || !token) return;\n",
                "    const url = new URL('/canvas/v1/state/' + encodeURIComponent(canvasId), window.location.origin);\n",
                "    url.searchParams.set('token', token);\n",
                "    url.searchParams.set('after_version', String(stateVersion));\n",
                "    const response = await fetch(url.toString(), {{ method: 'GET', cache: 'no-store', credentials: 'omit' }});\n",
                "    if (response.status === 204) return;\n",
                "    if (!response.ok) return;\n",
                "    const payload = await response.json();\n",
                "    if (typeof payload.state_version !== 'number') return;\n",
                "    if (payload.state_version <= stateVersion) return;\n",
                "    stateVersion = payload.state_version;\n",
                "    renderState(payload.state);\n",
                "  }}\n",
                "  window.addEventListener('message', (event) => {{\n",
                "    if (!allowedOrigins.has(event.origin)) return;\n",
                "    const message = event.data;\n",
                "    if (!message || typeof message !== 'object') return;\n",
                "    if (message.type !== 'palyra.canvas.state') return;\n",
                "    if (message.token !== token) return;\n",
                "    if (typeof message.version !== 'number' || message.version <= stateVersion) return;\n",
                "    stateVersion = message.version;\n",
                "    renderState(message.state);\n",
                "  }});\n",
                "  if (window.parent && window.parent !== window) {{\n",
                "    for (const origin of allowedOrigins) {{\n",
                "      window.parent.postMessage({{ type: 'palyra.canvas.ready', canvas_id: canvasId }}, origin);\n",
                "    }}\n",
                "  }}\n",
                "  setInterval(() => {{ void pollState(); }}, 750);\n",
                "  void pollState();\n",
                "  if (root) {{\n",
                "    root.setAttribute('data-canvas-ready', 'true');\n",
                "  }}\n",
                "}})();\n"
            ),
            canvas_id_json = serde_json::to_string(&record.canvas_id).map_err(|error| {
                Status::internal(format!("failed to encode canvas runtime identifier: {error}"))
            })?
        );
        Ok(CanvasAssetResponse {
            content_type: "application/javascript; charset=utf-8".to_owned(),
            body: script.into_bytes(),
            csp: build_canvas_csp_header(record.allowed_parent_origins.as_slice()),
        })
    }

    /// Serves the static canvas stylesheet.
    ///
    /// # Errors
    /// Token/ownership errors from `Self::authorize_canvas_http_request`.
    #[allow(clippy::result_large_err)]
    pub fn canvas_runtime_stylesheet(
        &self,
        canvas_id: &str,
        token: &str,
    ) -> Result<CanvasAssetResponse, Status> {
        let record = self.authorize_canvas_http_request(canvas_id, token)?;
        let stylesheet = concat!(
            ":root { color-scheme: light; font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }\n",
            "html, body { margin: 0; padding: 0; background: #f5f7fb; color: #111827; }\n",
            "#palyra-canvas-root { min-height: 2rem; }\n",
            "#palyra-canvas-state { margin: 0; padding: 1rem; white-space: pre-wrap; word-break: break-word; }\n"
        );
        Ok(CanvasAssetResponse {
            content_type: "text/css; charset=utf-8".to_owned(),
            body: stylesheet.as_bytes().to_vec(),
            csp: build_canvas_csp_header(record.allowed_parent_origins.as_slice()),
        })
    }

    /// Serves one asset from the canvas bundle by normalized path.
    ///
    /// # Errors
    /// Token/ownership errors from `Self::authorize_canvas_http_request`;
    /// `not_found` for unknown asset paths.
    #[allow(clippy::result_large_err)]
    pub fn canvas_bundle_asset(
        &self,
        canvas_id: &str,
        asset_path: &str,
        token: &str,
    ) -> Result<CanvasAssetResponse, Status> {
        let record = self.authorize_canvas_http_request(canvas_id, token)?;
        let normalized_asset_path = normalize_canvas_asset_path(asset_path, "asset_path")?;
        let Some(asset) = record.bundle.assets.get(normalized_asset_path.as_str()) else {
            return Err(Status::not_found(format!(
                "canvas asset not found: {}",
                normalized_asset_path
            )));
        };
        Ok(CanvasAssetResponse {
            content_type: asset.content_type.clone(),
            body: asset.body.clone(),
            csp: build_canvas_csp_header(record.allowed_parent_origins.as_slice()),
        })
    }

    /// Returns the current canvas state, or `None` when `after_version` is
    /// already current (the HTTP layer turns that into 204 No Content).
    ///
    /// # Errors
    /// Token/ownership errors from `Self::authorize_canvas_http_request`.
    #[allow(clippy::result_large_err)]
    pub fn canvas_state(
        &self,
        canvas_id: &str,
        token: &str,
        after_version: Option<u64>,
    ) -> Result<Option<CanvasStateResponse>, Status> {
        let record = self.authorize_canvas_http_request(canvas_id, token)?;
        if after_version.is_some_and(|value| value >= record.state_version) {
            return Ok(None);
        }
        let state = serde_json::from_slice::<Value>(&record.state_json).map_err(|error| {
            Status::internal(format!("persisted canvas state JSON is invalid: {error}"))
        })?;
        Ok(Some(CanvasStateResponse {
            canvas_id: record.canvas_id,
            state_version: record.state_version,
            state_schema_version: record.state_schema_version,
            state,
            closed: record.closed,
            close_reason: record.close_reason,
            expires_at_unix_ms: record.expires_at_unix_ms,
        }))
    }

    /// Loads recent patch history for a canvas, trimmed to the response row
    /// and byte budgets (newest records win).
    ///
    /// # Errors
    /// Mapped journal errors from the patch history read.
    #[allow(clippy::result_large_err)]
    pub(crate) fn load_canvas_patch_history(
        &self,
        canvas_id: &str,
    ) -> Result<Vec<CanvasStatePatchRecord>, Status> {
        let history = self
            .journal_store
            .list_recent_canvas_state_patches(canvas_id, CANVAS_PATCH_HISTORY_RESPONSE_ROW_LIMIT)
            .map_err(|error| map_canvas_store_error("list_recent_canvas_state_patches", error))?;
        Ok(Self::limit_canvas_patch_history_response(
            history,
            CANVAS_PATCH_HISTORY_RESPONSE_BYTE_LIMIT,
        ))
    }

    // Walks history newest-first so the byte budget keeps the most recent
    // contiguous records; oversized records at the newest edge are skipped
    // until something fits (pinned by a unit test below).
    fn limit_canvas_patch_history_response(
        history: Vec<CanvasStatePatchRecord>,
        max_response_bytes: usize,
    ) -> Vec<CanvasStatePatchRecord> {
        let mut selected = Vec::new();
        let mut selected_bytes = 0_usize;
        for record in history.into_iter().rev() {
            let record_bytes = Self::canvas_patch_history_response_bytes(&record);
            if record_bytes > max_response_bytes {
                if selected.is_empty() {
                    continue;
                }
                break;
            }
            if selected_bytes.saturating_add(record_bytes) > max_response_bytes {
                break;
            }
            selected_bytes = selected_bytes.saturating_add(record_bytes);
            selected.push(record);
        }
        selected.reverse();
        selected
    }

    fn canvas_patch_history_response_bytes(record: &CanvasStatePatchRecord) -> usize {
        CANVAS_PATCH_HISTORY_RESPONSE_RECORD_OVERHEAD
            .saturating_add(record.canvas_id.len())
            .saturating_add(record.patch_json.len())
            .saturating_add(record.resulting_state_json.len())
            .saturating_add(record.close_reason.as_deref().map_or(0, str::len))
            .saturating_add(record.actor_principal.len())
            .saturating_add(record.actor_device_id.len())
    }

    /// Verifies a canvas token and checks that its scope (canvas, principal,
    /// session) matches the live record and that neither token nor canvas has
    /// expired. Every rejection increments the `canvas_denied` counter.
    #[allow(clippy::result_large_err)]
    fn authorize_canvas_http_request(
        &self,
        canvas_id: &str,
        token: &str,
    ) -> Result<CanvasRecord, Status> {
        self.ensure_canvas_host_enabled()?;
        let normalized_canvas_id = normalize_canvas_identifier(canvas_id, "canvas_id")?;
        let token_payload = self.verify_canvas_token(token)?;
        if token_payload.canvas_id != normalized_canvas_id {
            self.counters.canvas_denied.fetch_add(1, Ordering::Relaxed);
            return Err(Status::permission_denied("canvas token does not match canvas id"));
        }
        let now_unix_ms = unix_ms_now_for_status()?;
        if token_payload.expires_at_unix_ms <= now_unix_ms {
            self.counters.canvas_denied.fetch_add(1, Ordering::Relaxed);
            return Err(Status::permission_denied("canvas token expired"));
        }

        let records = self
            .canvas_records
            .lock()
            .map_err(|_| Status::internal("canvas registry lock poisoned"))?;
        let Some(record) = records.get(normalized_canvas_id.as_str()) else {
            return Err(Status::not_found(format!("canvas not found: {normalized_canvas_id}")));
        };
        if record.expires_at_unix_ms <= now_unix_ms {
            self.counters.canvas_denied.fetch_add(1, Ordering::Relaxed);
            return Err(Status::permission_denied("canvas session expired"));
        }
        if record.principal != token_payload.principal
            || record.session_id != token_payload.session_id
        {
            self.counters.canvas_denied.fetch_add(1, Ordering::Relaxed);
            return Err(Status::permission_denied("canvas token scope mismatch"));
        }
        Ok(record.clone())
    }

    #[allow(clippy::result_large_err)]
    fn parse_canvas_bundle(
        &self,
        bundle: gateway_v1::CanvasBundle,
    ) -> Result<CanvasBundleRecord, Status> {
        let bundle_id = normalize_canvas_bundle_identifier(bundle.bundle_id.as_str())?;
        let entrypoint_path =
            normalize_canvas_asset_path(bundle.entrypoint_path.as_str(), "bundle.entrypoint_path")?;
        if bundle.assets.is_empty() {
            return Err(Status::invalid_argument("bundle.assets must include at least one asset"));
        }
        if bundle.assets.len() > self.config.canvas_host.max_assets_per_bundle {
            return Err(Status::resource_exhausted(format!(
                "bundle.assets exceeds limit ({} > {})",
                bundle.assets.len(),
                self.config.canvas_host.max_assets_per_bundle
            )));
        }
        let mut assets = HashMap::new();
        let mut total_bytes = 0usize;
        for (index, asset) in bundle.assets.iter().enumerate() {
            let source = format!("bundle.assets[{index}]");
            let normalized_path =
                normalize_canvas_asset_path(asset.path.as_str(), source.as_str())?;
            if assets.contains_key(normalized_path.as_str()) {
                return Err(Status::invalid_argument(format!(
                    "{source}.path duplicates asset path '{normalized_path}'"
                )));
            }
            let content_type =
                normalize_canvas_asset_content_type(asset.content_type.as_str(), source.as_str())?;
            total_bytes = total_bytes.saturating_add(asset.body.len());
            if total_bytes > self.config.canvas_host.max_bundle_bytes {
                return Err(Status::resource_exhausted(format!(
                    "bundle byte size exceeds limit ({} > {})",
                    total_bytes, self.config.canvas_host.max_bundle_bytes
                )));
            }
            assets.insert(
                normalized_path,
                CanvasAssetRecord { content_type, body: asset.body.clone() },
            );
        }
        let Some(entrypoint_asset) = assets.get(entrypoint_path.as_str()) else {
            return Err(Status::invalid_argument(
                "bundle.entrypoint_path must reference an existing asset",
            ));
        };
        if !is_canvas_javascript_content_type(entrypoint_asset.content_type.as_str()) {
            return Err(Status::failed_precondition(
                "bundle.entrypoint_path asset must use javascript content type",
            ));
        }
        let sha256 = compute_canvas_bundle_sha256(&assets);
        Ok(CanvasBundleRecord {
            bundle_id,
            entrypoint_path,
            assets,
            sha256,
            signature: String::new(),
        })
    }

    #[allow(clippy::result_large_err)]
    fn resolve_canvas_token_ttl_ms(&self, requested_ttl_seconds: u32) -> Result<u64, Status> {
        let requested_ttl_ms = if requested_ttl_seconds == 0 {
            self.config.canvas_host.token_ttl_ms
        } else {
            u64::from(requested_ttl_seconds).saturating_mul(1_000)
        };
        let bounded = requested_ttl_ms.clamp(MIN_CANVAS_TOKEN_TTL_MS, MAX_CANVAS_TOKEN_TTL_MS);
        if bounded == 0 {
            return Err(Status::invalid_argument("canvas auth token ttl must be positive"));
        }
        Ok(bounded)
    }

    fn sign_canvas_bundle(
        &self,
        canvas_id: &str,
        bundle_sha256: &str,
        principal: &str,
        session_id: &str,
    ) -> String {
        sign_canvas_hmac_sha256(
            &self.canvas_signing_secret,
            "canvas_bundle.v1",
            &[
                canvas_id.as_bytes(),
                bundle_sha256.as_bytes(),
                principal.as_bytes(),
                session_id.as_bytes(),
            ],
        )
    }

    /// Issues a `payload_b64.signature` token scoped to canvas, principal,
    /// and session. The signing secret is generated per process, so tokens do
    /// not survive a daemon restart.
    #[allow(clippy::result_large_err)]
    fn issue_canvas_token(
        &self,
        canvas_id: &str,
        principal: &str,
        session_id: &str,
        issued_at_unix_ms: i64,
        expires_at_unix_ms: i64,
    ) -> Result<String, Status> {
        let payload = CanvasTokenPayload {
            canvas_id: canvas_id.to_owned(),
            principal: principal.to_owned(),
            session_id: session_id.to_owned(),
            issued_at_unix_ms,
            expires_at_unix_ms,
            nonce: Ulid::new().to_string(),
        };
        let payload_json = serde_json::to_vec(&payload).map_err(|error| {
            Status::internal(format!("failed to serialize canvas token payload: {error}"))
        })?;
        let payload_b64 = URL_SAFE_NO_PAD.encode(payload_json);
        let signature = sign_canvas_hmac_sha256(
            &self.canvas_signing_secret,
            "canvas_token.v1",
            &[payload_b64.as_bytes()],
        );
        Ok(format!("{payload_b64}.{signature}"))
    }

    #[allow(clippy::result_large_err)]
    fn verify_canvas_token(&self, token: &str) -> Result<CanvasTokenPayload, Status> {
        if token.trim().is_empty() {
            return Err(Status::invalid_argument("canvas token is required"));
        }
        let Some((payload_b64, signature_b64)) = token.split_once('.') else {
            return Err(Status::invalid_argument("canvas token format is invalid"));
        };
        let expected_signature = sign_canvas_hmac_sha256(
            &self.canvas_signing_secret,
            "canvas_token.v1",
            &[payload_b64.as_bytes()],
        );
        if !constant_time_eq(expected_signature.as_bytes(), signature_b64.as_bytes()) {
            return Err(Status::permission_denied("canvas token signature is invalid"));
        }
        let payload_json = URL_SAFE_NO_PAD.decode(payload_b64).map_err(|error| {
            Status::invalid_argument(format!("canvas token payload encoding is invalid: {error}"))
        })?;
        let payload =
            serde_json::from_slice::<CanvasTokenPayload>(&payload_json).map_err(|error| {
                Status::invalid_argument(format!("canvas token payload is invalid JSON: {error}"))
            })?;
        Ok(payload)
    }

    // Journal event recording and vault access (rate limiting, counters, and
    // spawn_blocking wrappers around the synchronous vault backends).

    /// Appends a journal event, updating event/redaction counters and warning
    /// when the write exceeds the latency budget.
    ///
    /// # Errors
    /// `already_exists` for duplicate event ids, `resource_exhausted` when the
    /// journal capacity is reached, `internal` for other persistence failures
    /// (the persist-failure counter is bumped for the latter two).
    #[allow(clippy::result_large_err)]
    pub(crate) fn record_journal_event_blocking(
        &self,
        request: &JournalAppendRequest,
    ) -> Result<crate::journal::JournalAppendOutcome, Status> {
        let outcome = match self.journal_store.append(request) {
            Ok(outcome) => outcome,
            Err(JournalError::DuplicateEventId { event_id }) => {
                return Err(Status::already_exists(format!(
                    "journal event already exists: {event_id}"
                )));
            }
            Err(JournalError::JournalCapacityExceeded { current_events, max_events }) => {
                self.counters.journal_persist_failures.fetch_add(1, Ordering::Relaxed);
                return Err(Status::resource_exhausted(format!(
                    "journal capacity reached ({current_events} >= {max_events})"
                )));
            }
            Err(error) => {
                self.counters.journal_persist_failures.fetch_add(1, Ordering::Relaxed);
                return Err(Status::internal(format!(
                    "failed to persist journal event '{}': {error}",
                    request.event_id
                )));
            }
        };
        self.counters.journal_events.fetch_add(1, Ordering::Relaxed);
        if outcome.redacted {
            self.counters.journal_redacted_events.fetch_add(1, Ordering::Relaxed);
        }
        if outcome.write_duration.as_millis() > JOURNAL_WRITE_LATENCY_BUDGET_MS {
            warn!(
                event_id = %request.event_id,
                write_duration_ms = outcome.write_duration.as_millis(),
                budget_ms = JOURNAL_WRITE_LATENCY_BUDGET_MS,
                "journal write exceeded latency budget"
            );
        }
        Ok(outcome)
    }

    /// Async wrapper for [`Self::record_journal_event_blocking`] on a
    /// blocking worker.
    ///
    /// # Errors
    /// Same as the blocking variant, plus `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn record_journal_event(
        self: &Arc<Self>,
        request: JournalAppendRequest,
    ) -> Result<crate::journal::JournalAppendOutcome, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.record_journal_event_blocking(&request))
            .await
            .map_err(|_| Status::internal("journal write worker panicked"))?
    }

    /// Consumes one slot from the principal's fixed-window vault rate limit;
    /// returns `false` when the budget is exhausted.
    ///
    /// Fails closed: a poisoned lock denies the request. The bucket map is
    /// bounded; when full, stale windows are pruned and, if still full, the
    /// principal with the oldest window is evicted so an attacker cannot grow
    /// the map without bound by rotating principals.
    pub(crate) fn consume_vault_rate_limit(&self, principal: &str) -> bool {
        let now = Instant::now();
        let mut buckets = match self.vault_rate_limit.lock() {
            Ok(guard) => guard,
            Err(_) => return false,
        };
        if !buckets.contains_key(principal)
            && buckets.len() >= VAULT_RATE_LIMIT_MAX_PRINCIPAL_BUCKETS
        {
            buckets.retain(|_, entry| {
                now.duration_since(entry.window_started_at).as_millis() as u64
                    <= VAULT_RATE_LIMIT_WINDOW_MS
            });
            if buckets.len() >= VAULT_RATE_LIMIT_MAX_PRINCIPAL_BUCKETS {
                let evicted = buckets
                    .iter()
                    .min_by(|(left_principal, left_entry), (right_principal, right_entry)| {
                        left_entry
                            .window_started_at
                            .cmp(&right_entry.window_started_at)
                            .then_with(|| left_principal.cmp(right_principal))
                    })
                    .map(|(oldest_principal, _)| oldest_principal.clone());
                let Some(oldest_principal) = evicted else {
                    return false;
                };
                buckets.remove(oldest_principal.as_str());
            }
        }
        let entry = buckets
            .entry(principal.to_owned())
            .or_insert(VaultRateLimitEntry { window_started_at: now, requests_in_window: 0 });
        if now.duration_since(entry.window_started_at).as_millis() as u64
            > VAULT_RATE_LIMIT_WINDOW_MS
        {
            entry.window_started_at = now;
            entry.requests_in_window = 0;
        }
        if entry.requests_in_window >= VAULT_RATE_LIMIT_MAX_REQUESTS_PER_WINDOW {
            return false;
        }
        entry.requests_in_window = entry.requests_in_window.saturating_add(1);
        true
    }

    pub(crate) fn record_vault_rate_limited_request(&self) {
        self.counters.vault_rate_limited_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_vault_put_request(&self) {
        self.counters.vault_put_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_vault_get_request(&self) {
        self.counters.vault_get_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_vault_delete_request(&self) {
        self.counters.vault_delete_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_vault_list_request(&self) {
        self.counters.vault_list_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// Stores a secret in the vault scope on a blocking worker.
    ///
    /// # Errors
    /// Returns the mapped vault error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn vault_put_secret(
        self: &Arc<Self>,
        scope: VaultScope,
        key: String,
        value: Vec<u8>,
    ) -> Result<VaultSecretMetadata, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.vault.put_secret(&scope, key.as_str(), value.as_slice())
        })
        .await
        .map_err(|_| Status::internal("vault write worker panicked"))?
        .map_err(|error| map_vault_error("put secret", error))
    }

    /// Reads a secret from the vault scope on a blocking worker.
    ///
    /// # Errors
    /// Returns the mapped vault error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn vault_get_secret(
        self: &Arc<Self>,
        scope: VaultScope,
        key: String,
    ) -> Result<Vec<u8>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.vault.get_secret(&scope, key.as_str()))
            .await
            .map_err(|_| Status::internal("vault read worker panicked"))?
            .map_err(|error| map_vault_error("get secret", error))
    }

    /// Deletes a secret from the vault scope on a blocking worker; returns
    /// whether it existed.
    ///
    /// # Errors
    /// Returns the mapped vault error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn vault_delete_secret(
        self: &Arc<Self>,
        scope: VaultScope,
        key: String,
    ) -> Result<bool, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.vault.delete_secret(&scope, key.as_str()))
            .await
            .map_err(|_| Status::internal("vault delete worker panicked"))?
            .map_err(|error| map_vault_error("delete secret", error))
    }

    /// Lists secret metadata in the vault scope on a blocking worker (values
    /// are never returned by this call).
    ///
    /// # Errors
    /// Returns the mapped vault error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn vault_list_secrets(
        self: &Arc<Self>,
        scope: VaultScope,
    ) -> Result<Vec<VaultSecretMetadata>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.vault.list_secrets(&scope))
            .await
            .map_err(|_| Status::internal("vault list worker panicked"))?
            .map_err(|error| map_vault_error("list secrets", error))
    }

    // Status snapshots and model provider execution.

    /// Returns the daemon-wide runtime dispatcher.
    #[must_use]
    pub(crate) fn runtime_kernel_dispatcher(
        &self,
    ) -> &crate::application::runtime_kernel_v2::dispatcher::RuntimeKernelDispatcher {
        self.runtime_kernel_dispatcher.as_ref()
    }

    /// Assembles the full gateway status document. Performs synchronous
    /// journal reads; call [`Self::status_snapshot_async`] from async code.
    pub fn status_snapshot(
        &self,
        context: RequestContext,
        auth_config: &GatewayAuthConfig,
    ) -> GatewayStatusSnapshot {
        let latest_event_hash = self.journal_store.latest_hash().ok().flatten();
        let agents_runtime = self
            .agent_registry
            .status_snapshot()
            .map(|snapshot| AgentRuntimeSnapshot {
                default_agent_id: snapshot.default_agent_id,
                agent_count: snapshot.agent_count,
                active_session_bindings: snapshot
                    .session_bindings
                    .into_iter()
                    .take(MAX_AGENT_STATUS_BINDINGS)
                    .map(|binding| AgentSessionBindingSnapshot {
                        session_id_redacted: redact_session_id(binding.session_id.as_str()),
                        agent_id: binding.agent_id,
                    })
                    .collect(),
            })
            .unwrap_or_else(|_| AgentRuntimeSnapshot {
                default_agent_id: None,
                agent_count: 0,
                active_session_bindings: Vec::new(),
            });
        self.counters
            .channel_router_queue_depth
            .store(self.channel_router.queue_depth() as u64, Ordering::Relaxed);
        GatewayStatusSnapshot {
            service: "palyrad",
            status: "ok",
            version: self.build.version.clone(),
            git_hash: self.build.git_hash.clone(),
            build_profile: self.build.build_profile.clone(),
            uptime_seconds: self.started_at.elapsed().as_secs(),
            transport: TransportSnapshot {
                grpc_bind_addr: self.config.grpc_bind_addr.clone(),
                grpc_port: self.config.grpc_port,
                quic_bind_addr: self.config.quic_bind_addr.clone(),
                quic_port: self.config.quic_port,
                quic_enabled: self.config.quic_enabled,
            },
            security: SecuritySnapshot {
                deny_by_default: true,
                admin_auth_required: self.config.admin_auth_required,
                admin_token_configured: auth_config.admin_token.is_some(),
                orchestrator_runloop_v1_enabled: self.config.orchestrator_runloop_v1_enabled,
                node_rpc_mtls_required: self.config.node_rpc_mtls_required,
                revoked_certificate_count: self.revoked_certificate_count,
                smart_routing_enabled: self.config.smart_routing.enabled,
                smart_routing_default_mode: self.config.smart_routing.default_mode.clone(),
            },
            storage: StorageSnapshot {
                journal_db_path: self.journal_config.db_path.to_string_lossy().into_owned(),
                journal_hash_chain_enabled: self.journal_config.hash_chain_enabled,
                latest_event_hash,
            },
            model_provider: self.current_model_provider().status_snapshot(),
            tool_call_policy: tool_policy_snapshot(&self.config.tool_call),
            counters: self.counters.snapshot(),
            agents: agents_runtime,
            runtime_kernel: serde_json::to_value(self.runtime_kernel_dispatcher.diagnostics())
                .unwrap_or_else(|_| {
                    json!({
                        "status": "unavailable",
                        "reason_code": "runtime.kernel.diagnostics_serialization_failed",
                    })
                }),
            request_context: context,
        }
    }

    /// Builds [`Self::status_snapshot`] on a blocking worker.
    ///
    /// # Errors
    /// `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn status_snapshot_async(
        self: &Arc<Self>,
        context: RequestContext,
        auth_config: GatewayAuthConfig,
    ) -> Result<GatewayStatusSnapshot, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.status_snapshot(context, &auth_config))
            .await
            .map_err(|_| Status::internal("status snapshot worker panicked"))
    }

    /// Loads the most recent journal events plus totals (limit clamped to
    /// `MAX_JOURNAL_RECENT_EVENTS`).
    ///
    /// # Errors
    /// `internal` when journal reads fail.
    #[allow(clippy::result_large_err)]
    pub(crate) fn recent_journal_snapshot_blocking(
        &self,
        limit: usize,
    ) -> Result<JournalRecentSnapshot, Status> {
        let limit = limit.clamp(1, MAX_JOURNAL_RECENT_EVENTS);
        let events = self.journal_store.recent(limit).map_err(|error| {
            Status::internal(format!("failed to load recent journal events: {error}"))
        })?;
        let total_events =
            self.journal_store.total_events().map_err(|error| {
                Status::internal(format!("failed to count journal events: {error}"))
            })? as u64;
        Ok(JournalRecentSnapshot {
            total_events,
            hash_chain_enabled: self.journal_config.hash_chain_enabled,
            events,
        })
    }

    /// Async wrapper for `Self::recent_journal_snapshot_blocking`.
    ///
    /// # Errors
    /// Same as the blocking variant, plus `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn recent_journal_snapshot(
        self: &Arc<Self>,
        limit: usize,
    ) -> Result<JournalRecentSnapshot, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.recent_journal_snapshot_blocking(limit))
            .await
            .map_err(|_| Status::internal("journal read worker panicked"))?
    }

    /// Loads recent journal events for one run plus its scoped event count.
    ///
    /// # Errors
    /// `internal` when journal reads fail or the blocking worker panics.
    #[allow(clippy::result_large_err)]
    pub(crate) fn journal_snapshot_for_run_blocking(
        &self,
        run_id: &str,
        limit: usize,
    ) -> Result<JournalRecentSnapshot, Status> {
        let limit = limit.clamp(1, MAX_JOURNAL_RECENT_EVENTS);
        let events = self.journal_store.recent_for_run(run_id, limit).map_err(|error| {
            Status::internal(format!("failed to load run journal events: {error}"))
        })?;
        let total_events = self.journal_store.total_events_for_run(run_id).map_err(|error| {
            Status::internal(format!("failed to count run journal events: {error}"))
        })? as u64;
        Ok(JournalRecentSnapshot {
            total_events,
            hash_chain_enabled: self.journal_config.hash_chain_enabled,
            events,
        })
    }

    /// Async wrapper for `Self::journal_snapshot_for_run_blocking`.
    ///
    /// # Errors
    /// Same as the blocking variant, plus `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn journal_snapshot_for_run(
        self: &Arc<Self>,
        run_id: String,
        limit: usize,
    ) -> Result<JournalRecentSnapshot, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.journal_snapshot_for_run_blocking(run_id.as_str(), limit)
        })
        .await
        .map_err(|_| Status::internal("run journal read worker panicked"))?
    }

    /// Builds the journal state doctor report on the blocking journal worker.
    ///
    /// # Errors
    /// `internal` when state probes fail, `failed_precondition` when writes are
    /// already blocked by a known hash-chain mismatch.
    #[allow(clippy::result_large_err)]
    pub(crate) fn journal_state_health_report_blocking(
        &self,
        fast_window: Option<usize>,
    ) -> Result<JournalHealthReport, Status> {
        self.journal_store.state_health_report(fast_window).map_err(journal_state_error_status)
    }

    /// Async wrapper for [`Self::journal_state_health_report_blocking`].
    ///
    /// # Errors
    /// Same as the blocking variant, plus `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn journal_state_health_report(
        self: &Arc<Self>,
        fast_window: Option<usize>,
    ) -> Result<JournalHealthReport, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.journal_state_health_report_blocking(fast_window))
            .await
            .map_err(|_| Status::internal("journal state doctor worker panicked"))?
    }

    /// Applies or previews journal state repair on the blocking journal worker.
    ///
    /// # Errors
    /// `invalid_argument` when the requested repair is outside the supported
    /// FTS-only compatibility contract, or `internal` when the repair fails.
    #[allow(clippy::result_large_err)]
    pub(crate) fn repair_journal_state_blocking(
        &self,
        request: &JournalStateRepairRequest,
    ) -> Result<JournalStateRepairReport, Status> {
        self.journal_store.repair_state(request).map_err(journal_state_error_status)
    }

    /// Async wrapper for [`Self::repair_journal_state_blocking`].
    ///
    /// # Errors
    /// Same as the blocking variant, plus `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn repair_journal_state(
        self: &Arc<Self>,
        request: JournalStateRepairRequest,
    ) -> Result<JournalStateRepairReport, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.repair_journal_state_blocking(&request))
            .await
            .map_err(|_| Status::internal("journal state repair worker panicked"))?
    }

    /// Runs a WAL checkpoint on the blocking journal worker.
    ///
    /// # Errors
    /// `internal` when SQLite rejects the checkpoint.
    #[allow(clippy::result_large_err)]
    pub(crate) fn checkpoint_journal_wal_blocking(
        &self,
        mode: JournalWalCheckpointMode,
    ) -> Result<JournalWalCheckpointReport, Status> {
        self.journal_store.checkpoint_wal(mode).map_err(journal_state_error_status)
    }

    /// Async wrapper for [`Self::checkpoint_journal_wal_blocking`].
    ///
    /// # Errors
    /// Same as the blocking variant, plus `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn checkpoint_journal_wal(
        self: &Arc<Self>,
        mode: JournalWalCheckpointMode,
    ) -> Result<JournalWalCheckpointReport, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.checkpoint_journal_wal_blocking(mode))
            .await
            .map_err(|_| Status::internal("journal WAL checkpoint worker panicked"))?
    }

    /// Returns the process-wide startup, drain, and shutdown state.
    pub(crate) fn daemon_lifecycle_snapshot(&self) -> Result<DaemonLifecycleSnapshot, Status> {
        self.daemon_lifecycle.snapshot().map_err(daemon_lifecycle_status)
    }

    /// Opens ingress after every startup recovery actuator has completed.
    ///
    /// # Errors
    /// Returns `internal` when durable transition evidence cannot be committed.
    pub(crate) fn complete_daemon_startup_recovery(
        &self,
    ) -> Result<DaemonLifecycleSnapshot, Status> {
        let _transition_guard = self
            .daemon_lifecycle_transition_lock
            .lock()
            .map_err(|_| Status::internal("daemon lifecycle transition lock poisoned"))?;
        let next =
            self.daemon_lifecycle.propose_startup_ready().map_err(daemon_lifecycle_status)?;
        self.journal_store
            .append_daemon_lifecycle_snapshot(&next)
            .map_err(journal_state_error_status)?;
        self.daemon_lifecycle.apply(next.clone()).map_err(daemon_lifecycle_status)?;
        Ok(next)
    }

    /// Starts the sole coordinated drain sequence.
    ///
    /// Concurrent signal and admin requests converge on the first committed
    /// drain epoch. The elected caller waits for active runs, drains subsystem
    /// producers, checkpoints SQLite, and finally releases all transports.
    ///
    /// # Errors
    /// Returns a transport status when lifecycle evidence or the final
    /// checkpoint cannot be committed.
    pub(crate) async fn begin_daemon_drain(
        self: &Arc<Self>,
        request: DaemonDrainRequest,
    ) -> Result<DaemonLifecycleSnapshot, Status> {
        let (snapshot, elected) = self.request_daemon_drain(request)?;
        if elected {
            self.run_daemon_drain(snapshot.epoch).await?;
        }
        self.daemon_lifecycle_snapshot()
    }

    /// Persists a drain request and runs its coordinator in the background.
    ///
    /// This variant lets the admin transport acknowledge the committed
    /// boundary before that same transport is asked to stop.
    ///
    /// # Errors
    /// Returns a transport status when the initial drain transition cannot be
    /// committed.
    pub(crate) fn spawn_daemon_drain(
        self: &Arc<Self>,
        request: DaemonDrainRequest,
    ) -> Result<DaemonLifecycleSnapshot, Status> {
        let (snapshot, elected) = self.request_daemon_drain(request)?;
        if elected {
            let runtime = Arc::clone(self);
            let epoch = snapshot.epoch;
            tokio::spawn(async move {
                if let Err(error) = runtime.run_daemon_drain(epoch).await {
                    tracing::error!(
                        code = %error.code(),
                        message = %error.message(),
                        lifecycle_epoch = epoch,
                        "background daemon drain coordinator failed"
                    );
                }
            });
        }
        Ok(snapshot)
    }

    /// Cancels a drain before checkpointing begins.
    ///
    /// # Errors
    /// Returns `failed_precondition` after the point of no return, or
    /// `internal` when durable transition evidence cannot be committed.
    pub(crate) fn cancel_daemon_drain(
        &self,
        epoch: u64,
        requested_by: String,
    ) -> Result<DaemonLifecycleSnapshot, Status> {
        let _transition_guard = self
            .daemon_lifecycle_transition_lock
            .lock()
            .map_err(|_| Status::internal("daemon lifecycle transition lock poisoned"))?;
        let next = self
            .daemon_lifecycle
            .propose_cancel(epoch, requested_by)
            .map_err(daemon_lifecycle_status)?;
        self.journal_store
            .append_daemon_lifecycle_snapshot(&next)
            .map_err(journal_state_error_status)?;
        self.daemon_lifecycle.apply(next.clone()).map_err(daemon_lifecycle_status)?;
        Ok(next)
    }

    /// Resolves when the lifecycle controller releases process transports.
    pub(crate) async fn wait_for_daemon_shutdown(&self) {
        self.daemon_lifecycle.wait_for_shutdown().await;
    }

    fn request_daemon_drain(
        &self,
        request: DaemonDrainRequest,
    ) -> Result<(DaemonLifecycleSnapshot, bool), Status> {
        let _transition_guard = self
            .daemon_lifecycle_transition_lock
            .lock()
            .map_err(|_| Status::internal("daemon lifecycle transition lock poisoned"))?;
        let current = self.daemon_lifecycle.snapshot().map_err(daemon_lifecycle_status)?;
        let next = self
            .daemon_lifecycle
            .propose_drain(request, current_unix_ms())
            .map_err(daemon_lifecycle_status)?;
        if next.revision == current.revision {
            return Ok((current, false));
        }
        self.journal_store
            .append_daemon_lifecycle_snapshot(&next)
            .map_err(journal_state_error_status)?;
        self.daemon_lifecycle.apply(next.clone()).map_err(daemon_lifecycle_status)?;
        Ok((next, true))
    }

    async fn run_daemon_drain(self: &Arc<Self>, epoch: u64) -> Result<(), Status> {
        loop {
            let snapshot = self.daemon_lifecycle_snapshot()?;
            if snapshot.epoch != epoch || snapshot.phase == DaemonLifecyclePhase::Running {
                return Ok(());
            }
            let active_runs = self.counters.snapshot().active_orchestrator_runs();
            if active_runs == 0
                || snapshot.deadline_unix_ms.is_some_and(|deadline| current_unix_ms() >= deadline)
            {
                break;
            }
            let notified = self.orchestrator_run_notify.notified();
            let _ = tokio::time::timeout(Duration::from_millis(250), notified).await;
        }

        let draining =
            self.advance_daemon_lifecycle(epoch, DaemonLifecyclePhase::DrainingSubsystems)?;
        self.daemon_lifecycle
            .abort_subsystem(LifecycleSubsystem::Channels)
            .map_err(daemon_lifecycle_status)?;
        let mut lifecycle = self.daemon_lifecycle.subscribe();
        loop {
            if self.daemon_lifecycle.subsystems_settled().map_err(daemon_lifecycle_status)? {
                break;
            }
            let deadline_reached =
                draining.deadline_unix_ms.is_some_and(|deadline| current_unix_ms() >= deadline);
            if deadline_reached {
                self.daemon_lifecycle
                    .abort_undrained_subsystems()
                    .map_err(daemon_lifecycle_status)?;
                break;
            }
            let _ = tokio::time::timeout(Duration::from_millis(100), lifecycle.changed()).await;
        }
        self.advance_daemon_lifecycle(epoch, DaemonLifecyclePhase::Checkpointing)?;
        self.checkpoint_journal_wal(JournalWalCheckpointMode::Full).await?;
        self.advance_daemon_lifecycle(epoch, DaemonLifecyclePhase::ShutdownRequested)?;
        Ok(())
    }

    fn advance_daemon_lifecycle(
        &self,
        epoch: u64,
        phase: DaemonLifecyclePhase,
    ) -> Result<DaemonLifecycleSnapshot, Status> {
        let _transition_guard = self
            .daemon_lifecycle_transition_lock
            .lock()
            .map_err(|_| Status::internal("daemon lifecycle transition lock poisoned"))?;
        let next =
            self.daemon_lifecycle.propose_advance(epoch, phase).map_err(daemon_lifecycle_status)?;
        self.journal_store
            .append_daemon_lifecycle_snapshot(&next)
            .map_err(journal_state_error_status)?;
        self.daemon_lifecycle.apply(next.clone()).map_err(daemon_lifecycle_status)?;
        Ok(next)
    }

    /// Classifies and persists one validated configuration restart request.
    ///
    /// Automatic restart is denied while any durable mutation fence is
    /// started or outcome-unknown. Equivalent candidate hashes coalesce before
    /// they can create a second lifecycle drain.
    ///
    /// # Errors
    /// Returns a transport status when diagnostics or durable decision
    /// evidence cannot be loaded.
    pub(crate) async fn coordinate_config_restart(
        self: &Arc<Self>,
        request: RestartRequest,
        blocked_active_steps: u32,
        manual_review_steps: u32,
    ) -> Result<RestartDecision, Status> {
        let existing_request_id = self
            .journal_store
            .restart_request_for_coalescing_key(request.coalescing_key.as_str())
            .map_err(journal_state_error_status)?;
        let diagnostics = self.shared_runtime_diagnostics().await?;
        let outcome_unknown_mutations = diagnostics
            .side_effect_fences_by_state
            .get("effect_started")
            .copied()
            .unwrap_or(0)
            .saturating_add(
                diagnostics.side_effect_fences_by_state.get("effect_unknown").copied().unwrap_or(0),
            );
        let lifecycle = self.daemon_lifecycle_snapshot()?;
        let decision = decide_restart(
            request,
            RestartBlockerSnapshot {
                active_runs: self.counters.snapshot().active_orchestrator_runs(),
                outcome_unknown_mutations,
                blocked_active_steps,
                manual_review_steps,
                lifecycle_phase: lifecycle.phase.as_str().to_owned(),
            },
            existing_request_id,
            current_unix_ms(),
        );
        self.journal_store
            .record_restart_decision(&decision)
            .map_err(journal_state_error_status)?;
        if decision.kind.starts_drain() {
            self.spawn_daemon_drain(DaemonDrainRequest {
                trigger: crate::application::daemon_lifecycle::DaemonDrainTrigger::ConfigRestart,
                reason_code: decision.reason_code.clone(),
                requested_by: "system:config_watcher".to_owned(),
                deadline_unix_ms: current_unix_ms().saturating_add(30_000),
                admission_policy:
                    crate::application::daemon_lifecycle::DrainAdmissionPolicy::RejectNew,
            })?;
        }
        Ok(decision)
    }

    /// Returns bounded redacted restart decisions for diagnostics.
    pub(crate) fn recent_config_restart_decisions(&self) -> Result<Vec<RestartDecision>, Status> {
        self.journal_store.recent_restart_decisions(16).map_err(journal_state_error_status)
    }

    pub(crate) fn recent_startup_recovery_actions(
        &self,
    ) -> Result<Vec<crate::journal::StartupRecoveryAction>, Status> {
        self.journal_store.recent_startup_recovery_actions(16).map_err(journal_state_error_status)
    }

    /// Verifies the journal hash chain on the blocking journal worker.
    ///
    /// # Errors
    /// `internal` when SQLite verification fails.
    #[allow(clippy::result_large_err)]
    pub(crate) fn verify_journal_hash_chain_blocking(
        &self,
        scope: JournalHashVerificationScope,
    ) -> Result<JournalHashChainVerificationReport, Status> {
        self.journal_store.verify_hash_chain(scope).map_err(journal_state_error_status)
    }

    /// Async wrapper for [`Self::verify_journal_hash_chain_blocking`].
    ///
    /// # Errors
    /// Same as the blocking variant, plus `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn verify_journal_hash_chain(
        self: &Arc<Self>,
        scope: JournalHashVerificationScope,
    ) -> Result<JournalHashChainVerificationReport, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.verify_journal_hash_chain_blocking(scope))
            .await
            .map_err(|_| Status::internal("journal hash-chain verifier worker panicked"))?
    }

    /// Creates rebuildable journal sidecar directories on the blocking worker.
    ///
    /// # Errors
    /// `internal` when directory creation or permission hardening fails.
    #[allow(clippy::result_large_err)]
    pub(crate) fn prepare_journal_sidecar_storage_blocking(
        &self,
    ) -> Result<Vec<SidecarIndexDescriptor>, Status> {
        self.journal_store.prepare_sidecar_storage().map_err(journal_state_error_status)
    }

    /// Async wrapper for [`Self::prepare_journal_sidecar_storage_blocking`].
    ///
    /// # Errors
    /// Same as the blocking variant, plus `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn prepare_journal_sidecar_storage(
        self: &Arc<Self>,
    ) -> Result<Vec<SidecarIndexDescriptor>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.prepare_journal_sidecar_storage_blocking())
            .await
            .map_err(|_| Status::internal("journal sidecar prepare worker panicked"))?
    }

    /// Whether the v1 orchestrator run loop is enabled by configuration.
    #[must_use]
    pub const fn is_orchestrator_runloop_enabled(&self) -> bool {
        self.config.orchestrator_runloop_v1_enabled
    }

    fn current_model_provider(&self) -> Arc<dyn ModelProvider> {
        self.current_model_provider_runtime().provider
    }

    fn current_model_provider_runtime(&self) -> ModelProviderRuntime {
        self.model_provider.read().unwrap_or_else(|error| error.into_inner()).clone()
    }

    fn latch_provider_health_authority(&self, authority: &ProviderAttemptHealthAuthority) {
        self.provider_health_authority_latches
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(ProviderHealthAuthorityKey::from(authority));
    }

    fn provider_health_authority_is_latched(
        &self,
        authority: &ProviderAttemptHealthAuthority,
    ) -> bool {
        self.provider_health_authority_latches
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .contains(&ProviderHealthAuthorityKey::from(authority))
    }

    fn retain_active_provider_health_authority_latches(
        &self,
        health_authority_by_provider: &BTreeMap<String, ProviderAttemptHealthAuthority>,
    ) {
        let active = health_authority_by_provider
            .values()
            .map(ProviderHealthAuthorityKey::from)
            .collect::<BTreeSet<_>>();
        self.provider_health_authority_latches
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .retain(|key| active.contains(key));
    }

    fn managed_runtime_health_authority(
        &self,
        family: ManagedRuntimeHealthFamily,
        raw_id: &str,
    ) -> Result<ManagedRuntimeHealthAuthority, Status> {
        let component_id =
            managed_runtime_health_component_id(family, raw_id).map_err(|error| {
                Status::invalid_argument(format!(
                    "managed runtime health identity is invalid: {error}"
                ))
            })?;
        self.managed_runtime_health_authorities
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(component_id.as_str())
            .cloned()
            .ok_or_else(|| {
                Status::failed_precondition(format!(
                    "runtime.health.component_not_active: {}",
                    component_id.as_str()
                ))
            })
    }

    /// Returns exact shared-health authority when ordinary admission is allowed.
    ///
    /// # Errors
    /// Fails closed when the component is not active, its durable generation
    /// differs, or its current shared health posture blocks ordinary work.
    #[allow(clippy::result_large_err)]
    pub(crate) fn admit_managed_runtime_health(
        &self,
        family: ManagedRuntimeHealthFamily,
        raw_id: &str,
    ) -> Result<ManagedRuntimeHealthAuthority, Status> {
        let authority = self.managed_runtime_health_authority(family, raw_id)?;
        let health = self
            .journal_store
            .runtime_component_health(authority.component_id.as_str())
            .map_err(|error| {
                Status::internal(format!(
                    "runtime.health.read_failed: component={} error={error}",
                    authority.component_id.as_str()
                ))
            })?
            .ok_or_else(|| {
                Status::failed_precondition(format!(
                    "runtime.health.component_missing: {}",
                    authority.component_id.as_str()
                ))
            })?;
        if health.generation != authority.generation {
            return Err(Status::failed_precondition(format!(
                "runtime.health.generation_changed: component={} expected={} actual={}",
                authority.component_id.as_str(),
                authority.generation.get(),
                health.generation.get()
            )));
        }
        let decision = health.ordinary_admission_decision(current_unix_ms());
        if decision != RuntimeOrdinaryAdmissionDecision::Allowed {
            return Err(Status::failed_precondition(format!(
                "runtime.health.admission_blocked: component={} state={} decision={} reason_code={}",
                authority.component_id.as_str(),
                health.state.as_str(),
                decision.as_str(),
                health.reason_code
            )));
        }
        Ok(authority)
    }

    fn record_managed_runtime_stale_observation(
        &self,
        authority: &ManagedRuntimeHealthAuthority,
        expected_generation: Option<RuntimeGeneration>,
    ) {
        self.managed_runtime_health_stale_suppressions.fetch_add(1, Ordering::Relaxed);
        if let Err(error) = self.journal_store.record_runtime_stale_event_diagnostic(
            &RuntimeStaleEventDiagnosticRequest {
                session_id: authority.component_id.as_str().to_owned(),
                run_id: None,
                lane: authority.family.generation_lane(),
                expected_generation,
                observed_generation: authority.generation,
                subsystem: authority.family.subsystem(),
                disposition: StaleEventDisposition::PersistedDiagnostic,
                reason_code: "runtime.health.stale_observation_suppressed".to_owned(),
            },
        ) {
            warn!(
                component_id = authority.component_id.as_str(),
                error = %error,
                "failed to persist stale managed runtime health observation"
            );
        }
    }

    /// Records a success or failure against authority captured before an effect.
    ///
    /// A late observation is retained as metadata-only stale evidence and
    /// cannot mutate a replacement generation. Returns `true` only when the
    /// observation was durably applied to the captured generation.
    pub(crate) fn record_managed_runtime_health_observation(
        &self,
        authority: &ManagedRuntimeHealthAuthority,
        succeeded: bool,
        reason_code: &str,
    ) -> bool {
        let active = self
            .managed_runtime_health_authorities
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(authority.component_id.as_str())
            .cloned();
        if active.as_ref().is_none_or(|active| active.generation != authority.generation) {
            self.record_managed_runtime_stale_observation(
                authority,
                active.as_ref().map(|active| active.generation),
            );
            return false;
        }
        match self.journal_store.record_runtime_health_observation(
            &RuntimeHealthObservationRequest {
                component_id: authority.component_id.clone(),
                expected_generation: authority.generation,
                succeeded,
                reason_code: reason_code.to_owned(),
                observed_at_unix_ms: current_unix_ms(),
            },
        ) {
            Ok(_) => true,
            Err(JournalError::InvalidArgument(message))
                if message.contains("stale component generation") =>
            {
                let expected = self
                    .journal_store
                    .runtime_component_health(authority.component_id.as_str())
                    .ok()
                    .flatten()
                    .map(|health| health.generation);
                self.record_managed_runtime_stale_observation(authority, expected);
                false
            }
            Err(error) => {
                warn!(
                    component_id = authority.component_id.as_str(),
                    reason_code,
                    error = %error,
                    "failed to persist managed runtime health observation"
                );
                false
            }
        }
    }

    /// Records worker health only while both component and run authority remain exact.
    ///
    /// Returns `true` only when the observation was durably applied.
    pub(crate) fn record_managed_runtime_health_observation_for_run(
        &self,
        authority: &ManagedRuntimeHealthAuthority,
        session_id: &RuntimeSessionId,
        run_id: &RuntimeRunId,
        run_generation: RuntimeGeneration,
        succeeded: bool,
        reason_code: &str,
    ) -> bool {
        if authority.family != ManagedRuntimeHealthFamily::Worker {
            warn!(
                component_id = authority.component_id.as_str(),
                reason_code, "run-scoped worker health observation received non-worker authority"
            );
            return false;
        }
        let active = self
            .managed_runtime_health_authorities
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(authority.component_id.as_str())
            .cloned();
        if active.as_ref().is_none_or(|active| active.generation != authority.generation) {
            self.record_managed_runtime_stale_observation(
                authority,
                active.as_ref().map(|active| active.generation),
            );
            return false;
        }
        match self.journal_store.record_runtime_health_observation_for_run(
            &RuntimeHealthObservationRequest {
                component_id: authority.component_id.clone(),
                expected_generation: authority.generation,
                succeeded,
                reason_code: reason_code.to_owned(),
                observed_at_unix_ms: current_unix_ms(),
            },
            session_id,
            run_id,
            run_generation,
        ) {
            Ok(RunScopedRuntimeHealthObservationOutcome::Applied(_)) => true,
            Ok(RunScopedRuntimeHealthObservationOutcome::Stale { expected_generation }) => {
                self.managed_runtime_health_stale_suppressions.fetch_add(1, Ordering::Relaxed);
                if let Err(error) = self.journal_store.record_runtime_stale_event_diagnostic(
                    &RuntimeStaleEventDiagnosticRequest {
                        session_id: session_id.as_str().to_owned(),
                        run_id: Some(run_id.as_str().to_owned()),
                        lane: RuntimeGenerationLane::Run,
                        expected_generation,
                        observed_generation: run_generation,
                        subsystem: RuntimeSubsystem::Worker,
                        disposition: StaleEventDisposition::PersistedDiagnostic,
                        reason_code: "runtime.worker.stale_health_observation_suppressed"
                            .to_owned(),
                    },
                ) {
                    warn!(
                        component_id = authority.component_id.as_str(),
                        session_id = session_id.as_str(),
                        run_id = run_id.as_str(),
                        error = %error,
                        "failed to persist stale networked worker health observation"
                    );
                }
                false
            }
            Err(JournalError::InvalidArgument(message))
                if message.contains("stale component generation") =>
            {
                let expected = self
                    .journal_store
                    .runtime_component_health(authority.component_id.as_str())
                    .ok()
                    .flatten()
                    .map(|health| health.generation);
                self.record_managed_runtime_stale_observation(authority, expected);
                false
            }
            Err(error) => {
                warn!(
                    component_id = authority.component_id.as_str(),
                    session_id = session_id.as_str(),
                    run_id = run_id.as_str(),
                    reason_code,
                    error = %error,
                    "failed to persist run-scoped managed runtime health observation"
                );
                false
            }
        }
    }

    fn classify_code_intel_runtime_authority(
        &self,
        authority: &CodeIntelProviderRuntimeAuthority,
        language: CodeIntelLanguage,
    ) -> CodeIntelProviderSnapshotAuthority {
        let Ok(component_id) =
            managed_runtime_health_component_id(ManagedRuntimeHealthFamily::Lsp, language.as_str())
        else {
            return CodeIntelProviderSnapshotAuthority::Stale;
        };
        let captured = ManagedRuntimeHealthAuthority {
            family: ManagedRuntimeHealthFamily::Lsp,
            component_id,
            generation: authority.generation,
        };
        let active = self
            .managed_runtime_health_authorities
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(captured.component_id.as_str())
            .cloned();
        let durable =
            match self.journal_store.runtime_component_health(captured.component_id.as_str()) {
                Ok(durable) => durable,
                Err(error) => {
                    warn!(
                        component_id = captured.component_id.as_str(),
                        error = %error,
                        "failed to verify code-intelligence runtime authority"
                    );
                    self.record_managed_runtime_stale_observation(
                        &captured,
                        active.as_ref().map(|active| active.generation),
                    );
                    return CodeIntelProviderSnapshotAuthority::Stale;
                }
            };
        let expected_generation = durable
            .as_ref()
            .map(|health| health.generation)
            .or_else(|| active.as_ref().map(|active| active.generation));
        let is_current = authority.component_id == captured.component_id
            && active.as_ref().is_some_and(|active| active.generation == authority.generation)
            && durable.as_ref().is_some_and(|health| health.generation == authority.generation);
        if !is_current {
            self.record_managed_runtime_stale_observation(&captured, expected_generation);
        }
        if is_current {
            CodeIntelProviderSnapshotAuthority::Authoritative
        } else {
            CodeIntelProviderSnapshotAuthority::Stale
        }
    }

    fn replace_managed_runtime_health_family(
        &self,
        family: ManagedRuntimeHealthFamily,
        raw_ids: impl IntoIterator<Item = String>,
    ) -> Result<(), JournalError> {
        // LSP generation replacement and read-model projection share this lock
        // so neither can linearize between the other's authority check and write.
        let _code_intel_projection_guard = if family == ManagedRuntimeHealthFamily::Lsp {
            Some(self.code_intel_runtime.lock().unwrap_or_else(|error| error.into_inner()))
        } else {
            None
        };
        let activations = raw_ids
            .into_iter()
            .map(|raw_id| managed_runtime_health_activation(family, raw_id.as_str()))
            .collect::<Result<Vec<_>, _>>()?;
        let outcome = self
            .journal_store
            .activate_runtime_health_components(activations.as_slice(), current_unix_ms())?;
        let replacements = activations
            .iter()
            .map(|activation| {
                let component_id = activation.component_id.as_str();
                let generation =
                    outcome.generations.get(component_id).copied().ok_or_else(|| {
                        JournalError::InvalidArgument(format!(
                            "managed runtime health activation omitted component {component_id}"
                        ))
                    })?;
                Ok((
                    component_id.to_owned(),
                    ManagedRuntimeHealthAuthority {
                        family,
                        component_id: activation.component_id.clone(),
                        generation,
                    },
                ))
            })
            .collect::<Result<BTreeMap<_, _>, JournalError>>()?;
        let mut authorities = self
            .managed_runtime_health_authorities
            .write()
            .unwrap_or_else(|error| error.into_inner());
        authorities.retain(|_, authority| authority.family != family);
        authorities.extend(replacements);
        Ok(())
    }

    /// Re-activates the exact enabled plugin binding inventory after a live edit.
    pub(crate) fn try_configure_plugin_runtime_health(
        &self,
        plugin_ids: impl IntoIterator<Item = String>,
    ) -> Result<(), JournalError> {
        self.replace_managed_runtime_health_family(ManagedRuntimeHealthFamily::Plugin, plugin_ids)
    }

    /// Re-activates the exact enabled MCP server inventory before supervisor reload.
    pub(crate) fn try_configure_mcp_runtime_health(
        &self,
        config: &crate::config::McpServersConfig,
    ) -> Result<(), JournalError> {
        let ids = if config.mode == palyra_common::runtime_preview::RuntimePreviewMode::Disabled {
            Vec::new()
        } else {
            config
                .servers
                .iter()
                .filter(|server| server.enabled)
                .map(|server| server.id.clone())
                .collect()
        };
        self.replace_managed_runtime_health_family(ManagedRuntimeHealthFamily::Mcp, ids)
    }

    fn activate_networked_worker_runtime_health(
        &self,
        worker_id: &str,
    ) -> Result<(), JournalError> {
        let activation =
            managed_runtime_health_activation(ManagedRuntimeHealthFamily::Worker, worker_id)?;
        let outcome = self.journal_store.activate_runtime_health_components(
            std::slice::from_ref(&activation),
            current_unix_ms(),
        )?;
        let generation =
            outcome.generations.get(activation.component_id.as_str()).copied().ok_or_else(
                || {
                    JournalError::InvalidArgument(
                        "networked worker health activation omitted component".to_owned(),
                    )
                },
            )?;
        self.managed_runtime_health_authorities
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .insert(
                activation.component_id.as_str().to_owned(),
                ManagedRuntimeHealthAuthority {
                    family: ManagedRuntimeHealthFamily::Worker,
                    component_id: activation.component_id,
                    generation,
                },
            );
        Ok(())
    }

    fn managed_runtime_health_snapshot_blocking(
        &self,
    ) -> Result<ManagedRuntimeHealthSnapshot, Status> {
        let authorities = self
            .managed_runtime_health_authorities
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut components = Vec::with_capacity(authorities.len());
        for authority in authorities {
            let health = self
                .journal_store
                .runtime_component_health(authority.component_id.as_str())
                .map_err(|error| {
                    Status::internal(format!(
                        "runtime.health.inventory_read_failed: component={} error={error}",
                        authority.component_id.as_str()
                    ))
                })?
                .ok_or_else(|| {
                    Status::failed_precondition(format!(
                        "runtime.health.inventory_component_missing: {}",
                        authority.component_id.as_str()
                    ))
                })?;
            if health.generation != authority.generation {
                return Err(Status::failed_precondition(format!(
                    "runtime.health.inventory_generation_changed: component={}",
                    authority.component_id.as_str()
                )));
            }
            components.push(health);
        }
        components
            .sort_by(|left, right| left.component_id.as_str().cmp(right.component_id.as_str()));
        let mut components_by_family = BTreeMap::new();
        let mut components_by_state = BTreeMap::new();
        for component in &components {
            if let Some(family) =
                ManagedRuntimeHealthFamily::from_component_id(component.component_id.as_str())
            {
                let count = components_by_family.entry(family.as_str().to_owned()).or_insert(0_u64);
                *count = count.saturating_add(1);
            }
            let count =
                components_by_state.entry(component.state.as_str().to_owned()).or_insert(0_u64);
            *count = count.saturating_add(1);
        }
        let inventory_bytes = serde_json::to_vec(&components).map_err(|error| {
            Status::internal(format!("runtime health inventory serialization failed: {error}"))
        })?;
        Ok(ManagedRuntimeHealthSnapshot {
            schema_version: MANAGED_RUNTIME_HEALTH_SCHEMA_VERSION,
            generated_at_unix_ms: current_unix_ms(),
            inventory_sha256: hex::encode(sha2::Sha256::digest(inventory_bytes)),
            components,
            components_by_family,
            components_by_state,
            stale_suppressions_total: self
                .managed_runtime_health_stale_suppressions
                .load(Ordering::Relaxed),
        })
    }

    /// Returns the bounded exact-generation managed health inventory.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn managed_runtime_health_snapshot(
        self: &Arc<Self>,
    ) -> Result<ManagedRuntimeHealthSnapshot, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.managed_runtime_health_snapshot_blocking())
            .await
            .map_err(|_| Status::internal("managed runtime health snapshot worker panicked"))?
    }

    /// Synchronous snapshot used by already-synchronous diagnostics collectors.
    #[cfg(test)]
    pub(crate) fn managed_runtime_health_snapshot_sync(
        &self,
    ) -> Result<ManagedRuntimeHealthSnapshot, Status> {
        self.managed_runtime_health_snapshot_blocking()
    }

    /// Begins one generic host-owned, non-mutating managed-component probe.
    #[allow(clippy::result_large_err)]
    pub(crate) fn begin_managed_runtime_health_probe(
        &self,
        component_id: &str,
        reason_code: String,
        authorization_evidence_sha256: Option<String>,
        authorized_actor_id_sha256: Option<String>,
    ) -> Result<crate::journal::RuntimeHealthProbeBeginOutcome, Status> {
        let authority = self
            .managed_runtime_health_authorities
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(component_id)
            .cloned()
            .ok_or_else(|| {
                Status::failed_precondition(
                    "runtime health component has no registered managed probe executor",
                )
            })?;
        let health = self
            .journal_store
            .runtime_component_health(component_id)
            .map_err(|error| Status::internal(format!("runtime health read failed: {error}")))?
            .ok_or_else(|| Status::failed_precondition("runtime health state is missing"))?;
        if health.generation != authority.generation {
            return Err(Status::failed_precondition(
                "runtime health authority changed before probe begin",
            ));
        }
        let now = current_unix_ms();
        let lease = HealthProbeLeaseV1 {
            schema_version: HEALTH_PROBE_LEASE_SCHEMA_VERSION,
            lease_id: RuntimeLeaseId::parse(format!("health-probe:{}", Ulid::new()).as_str())
                .map_err(|error| {
                    Status::internal(format!("managed health probe lease invalid: {error}"))
                })?,
            component_id: authority.component_id,
            expected_generation: authority.generation,
            authority_class: health.authority_class,
            issued_at_unix_ms: now,
            expires_at_unix_ms: now.saturating_add(MANAGED_RUNTIME_HEALTH_PROBE_LEASE_MS),
            non_mutating: true,
        };
        self.journal_store
            .begin_runtime_health_probe(&RuntimeHealthProbeBeginRequest {
                lease,
                reason_code,
                authorization_evidence_sha256,
                authorized_actor_id_sha256,
            })
            .map_err(|error| {
                Status::failed_precondition(format!("managed health probe begin failed: {error}"))
            })
    }

    /// Settles one exact generic managed-component probe.
    #[allow(clippy::result_large_err)]
    pub(crate) fn settle_managed_runtime_health_probe(
        &self,
        lease: &HealthProbeLeaseV1,
        disposition: HealthProbeDisposition,
        reason_code: &str,
        probe_evidence_sha256: String,
    ) -> Result<RuntimeHealthProbeSettlementOutcome, Status> {
        let completed_at_unix_ms = current_unix_ms();
        let settlement = HealthProbeSettlementV1 {
            schema_version: HEALTH_PROBE_SETTLEMENT_SCHEMA_VERSION,
            lease_id: lease.lease_id.clone(),
            expected_generation: lease.expected_generation,
            result: HealthProbeResult {
                schema_version: HEALTH_PROBE_RESULT_SCHEMA_VERSION,
                component_id: lease.component_id.clone(),
                disposition,
                reason_code: reason_code.to_owned(),
                mutation_attempted: false,
                completed_at_unix_ms,
            },
        };
        self.journal_store
            .settle_runtime_health_probe(&RuntimeHealthProbeSettlementRequest {
                settlement,
                probe_evidence_sha256: Some(probe_evidence_sha256),
            })
            .map_err(|error| {
                Status::failed_precondition(format!(
                    "managed health probe settlement failed: {error}"
                ))
            })
    }

    /// Clears one exact durable quarantine and appends its hash-only operator audit.
    ///
    /// The caller must already have authenticated the request and bound the
    /// supplied actor digest to that credential. Only the currently active
    /// provider or managed-component generation is eligible.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn clear_runtime_component_quarantine(
        self: &Arc<Self>,
        clear: QuarantineClearRequest,
        context: RequestContext,
    ) -> Result<RuntimeHealthQuarantineClearOutcome, Status> {
        let expected_generation = clear.expected_generation;
        let component_id = clear.component_id.as_str().to_owned();
        let provider_authority_is_active =
            self.current_model_provider_runtime().health_authority_by_provider.values().any(
                |authority| {
                    authority.component_id == clear.component_id
                        && authority.generation == expected_generation
                },
            );
        let managed_authority_is_active = self
            .managed_runtime_health_authorities
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(component_id.as_str())
            .is_some_and(|authority| authority.generation == expected_generation);
        if !provider_authority_is_active && !managed_authority_is_active {
            return Err(Status::failed_precondition(
                "runtime quarantine clear requires the exact active component generation",
            ));
        }

        let now_unix_ms = current_unix_ms();
        let authorization_evidence_sha256 = clear.authorization_evidence_sha256.clone();
        let authorized_actor_id_sha256 = clear.actor_id.clone();
        let reason_code = clear.reason_code.clone();
        let probe_lease_id_sha256 = clear
            .probe_lease
            .as_ref()
            .map(|lease| hex::encode(sha2::Sha256::digest(lease.lease_id.as_str().as_bytes())));
        let probe_evidence_sha256 = clear.probe_evidence_sha256.clone();
        let session_id = Ulid::new().to_string();
        let audit_event = JournalAppendRequest {
            event_id: Ulid::new().to_string(),
            session_id: session_id.clone(),
            run_id: session_id,
            kind: common_v1::journal_event::EventKind::ToolExecuted as i32,
            actor: common_v1::journal_event::EventActor::User as i32,
            timestamp_unix_ms: now_unix_ms,
            payload_json: json!({
                "event": "runtime.health.quarantine_cleared",
                "component_id": component_id,
                "expected_generation": expected_generation.get(),
                "reason_code": reason_code,
                "authorized_actor_id_sha256": authorized_actor_id_sha256,
                "authorization_evidence_sha256": authorization_evidence_sha256,
                "probe_lease_id_sha256": probe_lease_id_sha256,
                "probe_evidence_sha256": probe_evidence_sha256,
            })
            .to_string()
            .into_bytes(),
            principal: context.principal,
            device_id: context.device_id,
            channel: context.channel,
        };
        let state = Arc::clone(self);
        let outcome = tokio::task::spawn_blocking(move || {
            state.journal_store.clear_runtime_component_quarantine(
                &RuntimeHealthQuarantineClearRequest {
                    clear,
                    audit_event,
                    cleared_at_unix_ms: now_unix_ms,
                },
            )
        })
        .await
        .map_err(|_| Status::internal("runtime quarantine clear worker panicked"))?
        .map_err(|error| match error {
            JournalError::JournalCapacityExceeded { current_events, max_events } => {
                Status::resource_exhausted(format!(
                    "journal capacity reached ({current_events} >= {max_events})"
                ))
            }
            JournalError::InvalidArgument(message) => Status::failed_precondition(message),
            other => Status::internal(format!("runtime quarantine clear failed: {other}")),
        })?;
        self.counters.journal_events.fetch_add(1, Ordering::Relaxed);
        if outcome.audit_payload_redacted {
            self.counters.journal_redacted_events.fetch_add(1, Ordering::Relaxed);
        }
        Ok(outcome)
    }

    fn apply_fixture_provider_fault(
        &self,
        point_id: &'static str,
        actor: &str,
    ) -> Result<(), Status> {
        match self.fault_injection.checkpoint(point_id, actor).map_err(|error| {
            Status::internal(format!("qa_fault.provider_checkpoint_failed: {error}"))
        })? {
            QaFaultDirective::Continue => Ok(()),
            QaFaultDirective::Activate(directive) => match directive.activation.action.clone() {
                QaFaultAction::Timeout => {
                    self.fault_injection.record_immediate_recovery(&directive).map_err(
                        |error| {
                            Status::internal(format!("qa_fault.provider_recovery_failed: {error}"))
                        },
                    )?;
                    Err(Status::deadline_exceeded(format!(
                        "qa_fault.provider_timeout: activation={}",
                        directive.activation.id
                    )))
                }
                QaFaultAction::Disconnect => {
                    self.fault_injection.record_immediate_recovery(&directive).map_err(
                        |error| {
                            Status::internal(format!("qa_fault.provider_recovery_failed: {error}"))
                        },
                    )?;
                    Err(Status::unavailable(format!(
                        "qa_fault.provider_disconnect: activation={}",
                        directive.activation.id
                    )))
                }
                QaFaultAction::MalformedEvent => {
                    self.fault_injection.record_immediate_recovery(&directive).map_err(
                        |error| {
                            Status::internal(format!("qa_fault.provider_recovery_failed: {error}"))
                        },
                    )?;
                    Err(Status::data_loss(format!(
                        "qa_fault.provider_malformed_event: activation={}",
                        directive.activation.id
                    )))
                }
                QaFaultAction::TerminateProcess => {
                    self.fault_injection.record_immediate_recovery(&directive).map_err(
                        |error| {
                            Status::internal(format!("qa_fault.provider_recovery_failed: {error}"))
                        },
                    )?;
                    #[cfg(feature = "qa-fault-injection")]
                    self.fault_injection.terminate_process();
                    #[cfg(not(feature = "qa-fault-injection"))]
                    Err(Status::internal(
                        "qa_fault.feature_disabled: terminate directive reached a feature-off build",
                    ))
                }
                action => Err(Status::internal(format!(
                    "qa_fault.provider_action_unsupported: {}",
                    action.kind().as_str()
                ))),
            },
        }
    }

    /// Replaces the model provider used by new requests and returns the durable generation.
    ///
    /// Activation commits before the provider swap. Failure leaves the previous provider
    /// visible; tests treat activation failure as an invariant violation.
    #[must_use]
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "production reload surfaces fallible activation")
    )]
    pub fn configure_model_provider(&self, model_provider: Arc<dyn ModelProvider>) -> u64 {
        match self.try_configure_model_provider(model_provider) {
            Ok(generation) => generation,
            Err(error) => panic!("test provider health activation should succeed: {error}"),
        }
    }

    /// Fallible provider replacement used by reload paths that can surface activation failure.
    ///
    /// # Errors
    /// Returns [`JournalError`] when durable provider health activation fails.
    pub fn try_configure_model_provider(
        &self,
        model_provider: Arc<dyn ModelProvider>,
    ) -> Result<u64, JournalError> {
        let _reload_guard =
            self.model_provider_reload_lock.lock().unwrap_or_else(|error| error.into_inner());
        let snapshot = model_provider.status_snapshot();
        let inventory = provider_health_inventory(&snapshot)?;
        let activation = self
            .journal_store
            .activate_provider_runtime(inventory.activations.as_slice(), current_unix_ms())?;
        let health_authority_by_provider =
            activated_provider_health_authorities(&inventory, &activation.health)?;
        let configuration_epoch = activation.configuration_epoch;
        let mut guard = self.model_provider.write().unwrap_or_else(|error| error.into_inner());
        let active_health_authority_by_provider = health_authority_by_provider.clone();
        *guard = ModelProviderRuntime {
            provider: model_provider,
            configuration_epoch,
            health_authority_by_provider,
        };
        drop(guard);
        self.retain_active_provider_health_authority_latches(&active_health_authority_by_provider);
        Ok(configuration_epoch.get())
    }

    /// Monotonic generation of the live model-provider runtime.
    #[must_use]
    #[cfg(test)]
    pub fn model_provider_generation(&self) -> u64 {
        self.model_provider
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .configuration_epoch
            .get()
    }

    /// Current model provider/registry status.
    #[must_use]
    pub fn model_provider_status_snapshot(&self) -> ProviderStatusSnapshot {
        self.current_model_provider().status_snapshot()
    }

    /// Current provider lease manager state (active leases and waiters).
    #[must_use]
    pub fn provider_lease_snapshot(&self) -> ProviderLeaseManagerSnapshot {
        self.provider_leases.snapshot()
    }

    /// Captures one reload-fenced provider-selection snapshot.
    ///
    /// # Errors
    /// Fails closed when an active provider has no durable health record or
    /// when its durable generation no longer matches the runtime authority.
    #[allow(clippy::result_large_err)]
    pub(crate) fn provider_selection_snapshot(
        &self,
    ) -> Result<GatewayProviderSelectionSnapshot, Status> {
        let runtime = self.model_provider.read().unwrap_or_else(|error| error.into_inner());
        let status = runtime.provider.status_snapshot();
        let embedded_harness_descriptors =
            crate::application::agent_harness::AgentHarnessRegistry::with_embedded_default()
                .map_err(|error| {
                    Status::failed_precondition(format!(
                        "runtime.selection.embedded_harness_unavailable: {error}"
                    ))
                })?
                .list();
        let context_engine_registry =
            crate::application::context_engine::ContextEngineRegistry::production_default()
                .snapshot();
        let mut health_records = Vec::with_capacity(runtime.health_authority_by_provider.len());
        for (provider_id, authority) in &runtime.health_authority_by_provider {
            let health = self
                .journal_store
                .runtime_component_health(authority.component_id.as_str())
                .map_err(|error| {
                    Status::internal(format!(
                        "runtime.selection.provider_health_read_failed: provider={provider_id} error={error}"
                    ))
                })?
                .ok_or_else(|| {
                    Status::failed_precondition(format!(
                        "runtime.selection.provider_health_missing: provider={provider_id}"
                    ))
                })?;
            if health.generation != authority.generation {
                return Err(Status::failed_precondition(format!(
                    "runtime.selection.provider_health_generation_changed: provider={provider_id}"
                )));
            }
            health_records.push(health);
        }
        health_records
            .sort_by(|left, right| left.component_id.as_str().cmp(right.component_id.as_str()));
        Ok(GatewayProviderSelectionSnapshot {
            observed_at_unix_ms: current_unix_ms(),
            configuration_epoch: runtime.configuration_epoch,
            status,
            health_authority_by_provider: runtime.health_authority_by_provider.clone(),
            health_records,
            embedded_harness_descriptors,
            context_engine_registry,
            build_version: self.build.version.clone(),
        })
    }

    /// Feeds a credential health signal (success, rate limit, quota, auth,
    /// transient) into the lease manager's cooldown logic.
    pub fn record_provider_credential_feedback(&self, request: ProviderCredentialFeedbackRequest) {
        self.provider_leases.record_credential_feedback(request);
    }

    /// Snapshot of the retrieval backend including memory embeddings status.
    ///
    /// # Errors
    /// Returns the mapped memory store error when the embeddings status read
    /// fails.
    #[allow(clippy::result_large_err)]
    pub fn retrieval_backend_snapshot(&self) -> Result<RetrievalBackendSnapshot, Status> {
        let embeddings_status = self
            .journal_store
            .memory_embeddings_status()
            .map_err(|error| map_memory_store_error("load retrieval backend snapshot", error))?;
        Ok(self.retrieval_backend.snapshot(&self.retrieval_config_snapshot(), &embeddings_status))
    }

    /// Previews lease admission for a provider/credential without acquiring
    /// or queueing anything. `task_label` is accepted for signature parity
    /// with acquisition but does not influence the preview.
    #[must_use]
    pub fn preview_provider_lease(
        &self,
        provider_id: &str,
        credential_id: &str,
        priority: crate::provider_leases::LeasePriority,
        task_label: &str,
        max_wait_ms: u64,
    ) -> ProviderLeasePreviewSnapshot {
        let _ = task_label;
        self.provider_leases.preview(ProviderLeasePreviewRequest {
            provider_id,
            credential_id,
            priority,
            max_wait_ms,
        })
    }

    /// Test-only completion path that skips lease admission while still
    /// updating provider counters.
    ///
    /// # Errors
    /// Returns the mapped provider error.
    #[cfg(test)]
    #[allow(clippy::result_large_err)]
    pub async fn execute_model_provider(
        self: &Arc<Self>,
        request: ProviderRequest,
    ) -> Result<crate::model_provider::ProviderResponse, Status> {
        self.counters.model_provider_requests.fetch_add(1, Ordering::Relaxed);
        let provider_runtime = self.current_model_provider_runtime();
        let model_provider = Arc::clone(&provider_runtime.provider);
        let provider_generation = provider_runtime.configuration_epoch;
        self.apply_fixture_provider_fault("provider.fixture.before_effect", "test-provider")?;
        let result = model_provider.complete(request).await;
        self.apply_fixture_provider_fault(
            "provider.fixture.after_effect_before_ack",
            "test-provider",
        )?;
        if self.model_provider.read().unwrap_or_else(|error| error.into_inner()).configuration_epoch
            != provider_generation
        {
            return Err(provider_reconfigured_status());
        }
        match result {
            Ok(response) => {
                if response.retry_count > 0 {
                    self.counters
                        .model_provider_retry_attempts
                        .fetch_add(response.retry_count as u64, Ordering::Relaxed);
                }
                Ok(response)
            }
            Err(error) => {
                self.counters.model_provider_failures.fetch_add(1, Ordering::Relaxed);
                if error.retry_count() > 0 {
                    self.counters
                        .model_provider_retry_attempts
                        .fetch_add(error.retry_count() as u64, Ordering::Relaxed);
                }
                if error.is_circuit_open() {
                    self.counters
                        .model_provider_circuit_open_rejections
                        .fetch_add(1, Ordering::Relaxed);
                }
                Err(map_provider_error(error))
            }
        }
    }

    /// Returns QA-only binding evidence for the currently installed provider.
    ///
    /// The caller must retain or publish the result only after the matching
    /// provider effect starts. A configuration swap still invalidates the
    /// generation-pinned V2 provider authority before its result can settle.
    pub(crate) fn qa_model_provider_lane_attestation(
        &self,
        request: &ProviderRequest,
    ) -> Option<palyra_common::qa_runtime_path::ProviderLaneAttestationEvent> {
        self.current_model_provider_runtime().provider.qa_lane_attestation_for_request(request)
    }

    /// Completes a model request under provider lease admission.
    ///
    /// Waits for (or is refused) a lease first, then runs the request and
    /// feeds credential-health and auth-profile signals back from the
    /// outcome. The lease guard is held for the whole provider call so
    /// concurrency limits hold.
    ///
    /// # Errors
    /// `resource_exhausted` when capacity is busy or the lease wait times
    /// out, otherwise the mapped provider error.
    #[allow(clippy::result_large_err)]
    pub async fn execute_model_provider_with_lease(
        self: &Arc<Self>,
        request: ProviderRequest,
        lease_context: ProviderLeaseExecutionContext,
    ) -> Result<crate::model_provider::ProviderResponse, Status> {
        let fault_actor = lease_context
            .run_id
            .as_deref()
            .or(lease_context.session_id.as_deref())
            .unwrap_or("provider");
        self.apply_fixture_provider_fault("provider.fixture.before_intent", fault_actor)?;
        let provider_runtime = self.current_model_provider_runtime();
        let model_provider = Arc::clone(&provider_runtime.provider);
        let provider_generation = provider_runtime.configuration_epoch;
        let uses_candidate_admission = model_provider.uses_candidate_attempt_admission();
        let candidate_admission = GatewayProviderAttemptAdmission {
            runtime_state: Arc::clone(self),
            lease_context: lease_context.clone(),
            expected_configuration_epoch: provider_generation,
            health_authority_by_provider: Arc::new(
                provider_runtime.health_authority_by_provider.clone(),
            ),
            feedback: Arc::new(Mutex::new(Vec::new())),
            attempted_profile_ids: Arc::new(Mutex::new(std::collections::BTreeMap::new())),
            #[cfg(test)]
            fail_health_observation_once: None,
        };
        let result = if uses_candidate_admission {
            self.apply_fixture_provider_fault("provider.fixture.after_intent", fault_actor)?;
            self.counters.model_provider_requests.fetch_add(1, Ordering::Relaxed);
            self.apply_fixture_provider_fault("provider.fixture.before_effect", fault_actor)?;
            model_provider
                .complete_with_attempt_admission(request, Arc::new(candidate_admission.clone()))
                .await
        } else {
            let provider_status = model_provider.status_snapshot();
            let model_id = provider_status.model_id.as_deref().ok_or_else(|| {
                Status::failed_precondition(
                    "model provider does not identify its direct completion model",
                )
            })?;
            let mut effect_gate_applied = false;
            loop {
                let binding = match candidate_admission
                    .prepare_attempt(
                        provider_status.provider_id.as_str(),
                        provider_status.credential_id.as_str(),
                        model_id,
                    )
                    .await
                {
                    Ok(binding) => binding,
                    Err(error) => {
                        break Err(provider_attempt_admission_provider_error(error));
                    }
                };
                if let Err(error) = candidate_admission.check_eligibility(&binding) {
                    break Err(provider_attempt_admission_provider_error(error));
                }
                let _permit = match candidate_admission.acquire(&binding).await {
                    Ok(permit) => permit,
                    Err(error) => {
                        break Err(provider_attempt_admission_provider_error(error));
                    }
                };
                let credential = match candidate_admission.materialize_credential(&binding).await {
                    Ok(credential) => credential,
                    Err(_) if binding.credential_attempt.is_some() => continue,
                    Err(error) => {
                        break Err(provider_attempt_admission_provider_error(error));
                    }
                };
                let runtime_authority = match candidate_admission.record_started(&binding).await {
                    Ok(authority) => authority,
                    Err(error) => {
                        break Err(provider_attempt_admission_provider_error(error));
                    }
                };
                if !effect_gate_applied {
                    self.apply_fixture_provider_fault(
                        "provider.fixture.after_intent",
                        fault_actor,
                    )?;
                    self.counters.model_provider_requests.fetch_add(1, Ordering::Relaxed);
                    self.apply_fixture_provider_fault(
                        "provider.fixture.before_effect",
                        fault_actor,
                    )?;
                    effect_gate_applied = true;
                }
                let attempt_result = model_provider
                    .complete_with_credential(request.clone(), credential.as_ref())
                    .await;
                drop(credential);
                let completion = match &attempt_result {
                    Ok(_) => candidate_admission.record_success(&binding, runtime_authority).await,
                    Err(error) => {
                        candidate_admission.record_failure(&binding, runtime_authority, error).await
                    }
                };
                let completion = match completion {
                    Ok(completion) => completion,
                    Err(error) => {
                        break Err(provider_attempt_admission_provider_error(error));
                    }
                };
                if completion == ProviderAttemptCompletionDisposition::StaleSuppressed {
                    break Err(crate::model_provider::provider_attempt_superseded_error());
                }
                if let Err(error) = &attempt_result {
                    if binding.credential_attempt.is_some()
                        && crate::model_provider::provider_error_allows_credential_rotation(
                            error, false,
                        )
                    {
                        continue;
                    }
                }
                break attempt_result;
            }
        };
        self.apply_fixture_provider_fault("provider.fixture.after_effect_before_ack", fault_actor)?;
        if result.as_ref().err().is_some_and(is_provider_attempt_superseded_error) {
            // Run-scoped completion suppression persists its diagnostic in the same
            // transaction that rejects stale authority. Configuration-scoped
            // attempts have no run generation transaction, so their task
            // correlation still needs the bounded best-effort diagnostic here.
            if lease_context.run_id.is_none() {
                self.record_stale_provider_result_best_effort(
                    &lease_context,
                    provider_generation,
                    self.model_provider
                        .read()
                        .unwrap_or_else(|error| error.into_inner())
                        .configuration_epoch,
                );
            }
            return Err(provider_reconfigured_status());
        }
        if !candidate_admission.apply_buffered_feedback() {
            let current_provider_generation = self
                .model_provider
                .read()
                .unwrap_or_else(|error| error.into_inner())
                .configuration_epoch;
            self.record_stale_provider_result_best_effort(
                &lease_context,
                provider_generation,
                current_provider_generation,
            );
            return Err(provider_reconfigured_status());
        }
        match result {
            Ok(response) => {
                if response.retry_count > 0 {
                    self.counters
                        .model_provider_retry_attempts
                        .fetch_add(response.retry_count as u64, Ordering::Relaxed);
                }
                Ok(response)
            }
            Err(error) => {
                self.counters.model_provider_failures.fetch_add(1, Ordering::Relaxed);
                if error.retry_count() > 0 {
                    self.counters
                        .model_provider_retry_attempts
                        .fetch_add(error.retry_count() as u64, Ordering::Relaxed);
                }
                if error.is_circuit_open() {
                    self.counters
                        .model_provider_circuit_open_rejections
                        .fetch_add(1, Ordering::Relaxed);
                }
                Err(map_provider_error(error))
            }
        }
    }

    fn record_stale_provider_result_best_effort(
        &self,
        lease_context: &ProviderLeaseExecutionContext,
        observed_generation: RuntimeGeneration,
        expected_generation: RuntimeGeneration,
    ) {
        let (Some(session_id), Some(diagnostic_run_id)) = (
            lease_context.session_id.as_ref(),
            lease_context.run_id.as_ref().or(lease_context.diagnostic_scope_id.as_ref()),
        ) else {
            return;
        };
        if let Err(error) = self.journal_store.record_runtime_stale_event_diagnostic(
            &RuntimeStaleEventDiagnosticRequest {
                session_id: session_id.clone(),
                // This historical column is the bounded correlation slot for
                // diagnostics and has no run foreign key; targetless auxiliary
                // work uses its task id here without claiming run ownership.
                run_id: Some(diagnostic_run_id.clone()),
                lane: RuntimeGenerationLane::Provider,
                expected_generation: Some(expected_generation),
                observed_generation,
                subsystem: RuntimeSubsystem::Provider,
                disposition: StaleEventDisposition::PersistedDiagnostic,
                reason_code: "runtime.generation.provider_reconfigured".to_owned(),
            },
        ) {
            warn!(error = %error, "failed to persist stale provider result diagnostic");
        }
    }

    fn record_auth_profile_success_for_attribution(
        &self,
        attribution: &ProviderCredentialAttribution,
    ) {
        let Some(profile_id) = attribution.auth_profile_id.as_deref() else {
            return;
        };
        let Some(registry) = self.auth_profile_registry.as_ref() else {
            return;
        };
        if let Err(error) = registry.record_profile_success(profile_id) {
            warn!(
                profile_id,
                provider_id = attribution.provider_id.as_str(),
                error = %error,
                "failed to record auth profile provider success"
            );
        }
    }

    fn record_auth_profile_failure_for_lease(
        &self,
        lease_context: &ProviderLeaseExecutionContext,
        error: &ProviderError,
    ) {
        let Some(profile_id) =
            auth_profile_id_from_credential_id(lease_context.credential_id.as_str())
        else {
            return;
        };
        let Some(kind) = auth_profile_failure_kind_for_provider_error(error) else {
            return;
        };
        let Some(registry) = self.auth_profile_registry.as_ref() else {
            return;
        };
        if let Err(record_error) = registry.record_profile_failure(profile_id, kind) {
            warn!(
                profile_id,
                provider_id = lease_context.provider_id.as_str(),
                failure_kind = kind.as_str(),
                error = %record_error,
                "failed to record auth profile provider failure"
            );
        }
    }

    fn record_provider_lease_feedback_for_error(
        &self,
        lease_context: &ProviderLeaseExecutionContext,
        error: &ProviderError,
    ) {
        let failure = error.failure_snapshot();
        let kind = match failure.recovery.category.as_str() {
            "rate_limit" => ProviderCredentialFeedbackKind::RateLimited,
            "quota" => ProviderCredentialFeedbackKind::QuotaExhausted,
            "auth" => ProviderCredentialFeedbackKind::AuthFailed,
            "transient" => ProviderCredentialFeedbackKind::TransientFailure,
            _ => return,
        };
        self.record_provider_credential_feedback(ProviderCredentialFeedbackRequest {
            provider_id: lease_context.provider_id.clone(),
            credential_id: lease_context.credential_id.clone(),
            kind,
            retry_after_ms: failure.recovery.retry_after_ms,
            reason: format!(
                "class={} category={} action={}",
                failure.class, failure.recovery.category, failure.recovery.action
            ),
            observed_at_unix_ms: current_unix_ms(),
        });
    }

    /// Transcribes audio under the exact provider health generation and capacity gate.
    ///
    /// Registry-backed providers bind the configured audio candidate internally. A
    /// direct provider is bound from its status snapshot before the effect begins.
    /// Buffered observations are committed only after stale-result suppression.
    ///
    /// # Errors
    /// Returns a provider admission failure, a stale-provider abort, or the mapped
    /// terminal provider error.
    #[allow(clippy::result_large_err)]
    pub async fn execute_audio_transcription(
        self: &Arc<Self>,
        request: AudioTranscriptionRequest,
    ) -> Result<AudioTranscriptionResponse, Status> {
        let provider_runtime = self.current_model_provider_runtime();
        let model_provider = Arc::clone(&provider_runtime.provider);
        let provider_status = model_provider.status_snapshot();
        let lease_context = ProviderLeaseExecutionContext {
            provider_id: provider_status.provider_id.clone(),
            credential_id: provider_status.credential_id.clone(),
            priority: LeasePriority::Foreground,
            task_label: "audio_transcription".to_owned(),
            max_wait_ms: 30_000,
            session_id: None,
            run_id: None,
            runtime_authority: None,
            diagnostic_scope_id: None,
        };
        let candidate_admission = GatewayProviderAttemptAdmission {
            runtime_state: Arc::clone(self),
            lease_context,
            expected_configuration_epoch: provider_runtime.configuration_epoch,
            health_authority_by_provider: Arc::new(
                provider_runtime.health_authority_by_provider.clone(),
            ),
            feedback: Arc::new(Mutex::new(Vec::new())),
            attempted_profile_ids: Arc::new(Mutex::new(std::collections::BTreeMap::new())),
            #[cfg(test)]
            fail_health_observation_once: None,
        };

        self.counters.model_provider_requests.fetch_add(1, Ordering::Relaxed);
        let result = if model_provider.uses_candidate_attempt_admission() {
            model_provider
                .transcribe_audio_with_attempt_admission(
                    request,
                    Arc::new(candidate_admission.clone()),
                )
                .await
        } else {
            let model_id = provider_status.model_id.as_deref().ok_or_else(|| {
                Status::failed_precondition(
                    "model provider does not identify its audio transcription model",
                )
            })?;
            loop {
                let binding = match candidate_admission
                    .prepare_attempt(
                        provider_status.provider_id.as_str(),
                        provider_status.credential_id.as_str(),
                        model_id,
                    )
                    .await
                {
                    Ok(binding) => binding,
                    Err(error) => {
                        break Err(provider_attempt_admission_provider_error(error));
                    }
                };
                if let Err(error) = candidate_admission.check_eligibility(&binding) {
                    break Err(provider_attempt_admission_provider_error(error));
                }
                let _permit = match candidate_admission.acquire(&binding).await {
                    Ok(permit) => permit,
                    Err(error) => {
                        break Err(provider_attempt_admission_provider_error(error));
                    }
                };
                let credential = match candidate_admission.materialize_credential(&binding).await {
                    Ok(credential) => credential,
                    Err(_) if binding.credential_attempt.is_some() => continue,
                    Err(error) => {
                        break Err(provider_attempt_admission_provider_error(error));
                    }
                };
                let runtime_authority = match candidate_admission.record_started(&binding).await {
                    Ok(authority) => authority,
                    Err(error) => {
                        break Err(provider_attempt_admission_provider_error(error));
                    }
                };
                let attempt_result = model_provider
                    .transcribe_audio_with_credential(request.clone(), credential.as_ref())
                    .await;
                drop(credential);
                let completion = match &attempt_result {
                    Ok(_) => candidate_admission.record_success(&binding, runtime_authority).await,
                    Err(error) => {
                        candidate_admission.record_failure(&binding, runtime_authority, error).await
                    }
                };
                let completion = match completion {
                    Ok(completion) => completion,
                    Err(error) => {
                        break Err(provider_attempt_admission_provider_error(error));
                    }
                };
                if completion == ProviderAttemptCompletionDisposition::StaleSuppressed {
                    break Err(crate::model_provider::provider_attempt_superseded_error());
                }
                if let Err(error) = &attempt_result {
                    if binding.credential_attempt.is_some()
                        && crate::model_provider::provider_error_allows_credential_rotation(
                            error, false,
                        )
                    {
                        continue;
                    }
                }
                break attempt_result;
            }
        };
        if !candidate_admission.apply_buffered_feedback() {
            return Err(provider_reconfigured_status());
        }
        match result {
            Ok(response) => {
                if response.retry_count > 0 {
                    self.counters
                        .model_provider_retry_attempts
                        .fetch_add(response.retry_count as u64, Ordering::Relaxed);
                }
                Ok(response)
            }
            Err(error) => {
                self.counters.model_provider_failures.fetch_add(1, Ordering::Relaxed);
                if error.retry_count() > 0 {
                    self.counters
                        .model_provider_retry_attempts
                        .fetch_add(error.retry_count() as u64, Ordering::Relaxed);
                }
                if error.is_circuit_open() {
                    self.counters
                        .model_provider_circuit_open_rejections
                        .fetch_add(1, Ordering::Relaxed);
                }
                Err(map_provider_error(error))
            }
        }
    }

    /// Runs a host-owned, fixed provider probe and atomically settles its durable lease.
    ///
    /// The provider determines no routing, failover, cache, prompt, or tool input from
    /// an external caller. Once durable begin commits, a host-owned task completes the
    /// probe and settlement even when the requesting transport disconnects. Returned
    /// provider text and raw provider errors are discarded.
    ///
    /// # Errors
    /// Returns an error when the component is unknown, the durable probe lifecycle
    /// cannot begin or settle, or no supported non-mutating probe target exists.
    #[allow(clippy::result_large_err)]
    pub async fn execute_provider_health_probe(
        self: &Arc<Self>,
        component_id: &str,
        reason_code: String,
        authorization_evidence_sha256: Option<String>,
        authorized_actor_id_sha256: Option<String>,
    ) -> Result<RuntimeHealthProbeSettlementOutcome, Status> {
        let provider_runtime = self.current_model_provider_runtime();
        let (provider_id, authority) = provider_runtime
            .health_authority_by_provider
            .iter()
            .find(|(_, authority)| authority.component_id.as_str() == component_id)
            .map(|(provider_id, authority)| (provider_id.clone(), authority.clone()))
            .ok_or_else(|| Status::not_found("provider health authority was not found"))?;
        let model_provider = Arc::clone(&provider_runtime.provider);
        let provider_generation = provider_runtime.configuration_epoch;
        if !model_provider.uses_candidate_attempt_admission() {
            return Err(Status::failed_precondition(
                "model provider does not support an exact host-owned probe",
            ));
        }
        let provider_status = model_provider.status_snapshot();
        let provider = provider_status
            .registry
            .providers
            .iter()
            .find(|provider| provider.enabled && provider.provider_id == provider_id)
            .ok_or_else(|| Status::not_found("enabled provider status was not found"))?;
        let model = provider_status
            .registry
            .models
            .iter()
            .find(|model| model.enabled && model.provider_id == provider_id && model.role == "chat")
            .or_else(|| {
                provider_status.registry.models.iter().find(|model| {
                    model.enabled
                        && model.provider_id == provider_id
                        && model.role == "audio_transcription"
                })
            })
            .ok_or_else(|| {
                Status::failed_precondition(
                    "provider has no supported host-owned health probe target",
                )
            })?;
        let role = match model.role.as_str() {
            "chat" => ProviderModelRole::Chat,
            "audio_transcription" => ProviderModelRole::AudioTranscription,
            _ => {
                return Err(Status::failed_precondition(
                    "provider health probe target role is unsupported",
                ));
            }
        };
        let target = ProviderHealthProbeTarget {
            provider_id: provider_id.clone(),
            credential_id: provider.credential_id.clone(),
            model_id: model.model_id.clone(),
            role,
        };
        let health = self
            .journal_store
            .runtime_component_health(authority.component_id.as_str())
            .map_err(|error| Status::internal(format!("provider health read failed: {error}")))?
            .ok_or_else(|| Status::failed_precondition("provider health state is missing"))?;
        if health.generation != authority.generation {
            return Err(Status::failed_precondition(
                "provider health authority changed before probe begin",
            ));
        }
        let now = current_unix_ms();
        let lease = HealthProbeLeaseV1 {
            schema_version: HEALTH_PROBE_LEASE_SCHEMA_VERSION,
            lease_id: RuntimeLeaseId::parse(format!("provider-probe:{}", Ulid::new()).as_str())
                .map_err(|error| {
                    Status::internal(format!("provider probe lease invalid: {error}"))
                })?,
            component_id: authority.component_id.clone(),
            expected_generation: authority.generation,
            authority_class: health.authority_class,
            issued_at_unix_ms: now,
            expires_at_unix_ms: now.saturating_add(15_000),
            non_mutating: true,
        };
        let begun = self
            .journal_store
            .begin_runtime_health_probe(&RuntimeHealthProbeBeginRequest {
                lease,
                reason_code,
                authorization_evidence_sha256,
                authorized_actor_id_sha256,
            })
            .map_err(|error| {
                Status::failed_precondition(format!("provider probe begin failed: {error}"))
            })?;
        let lease_id = begun.lease.lease_id.clone();
        let admission = GatewayProviderProbeAdmission {
            runtime_state: Arc::clone(self),
            expected_configuration_epoch: provider_runtime.configuration_epoch,
            health_authority_by_provider: Arc::new(
                provider_runtime.health_authority_by_provider.clone(),
            ),
            probe_lease: begun.lease.clone(),
        };
        let state = Arc::clone(self);
        let panic_lease = begun.lease.clone();
        let (outcome_tx, outcome_rx) = oneshot::channel();
        tokio::spawn(async move {
            let panic_state = Arc::clone(&state);
            let outcome = AssertUnwindSafe(async move {
                let probe_result = tokio::time::timeout(
                    Duration::from_secs(10),
                    model_provider.probe_with_attempt_admission(target, Arc::new(admission)),
                )
                .await;
                let provider_reconfigured = state
                    .model_provider
                    .read()
                    .unwrap_or_else(|error| error.into_inner())
                    .configuration_epoch
                    != provider_generation;
                let (disposition, reason_code) = if provider_reconfigured {
                    (
                        HealthProbeDisposition::Inconclusive,
                        "runtime.health.provider_probe_runtime_reconfigured",
                    )
                } else {
                    match probe_result {
                        Ok(Ok(())) => {
                            (HealthProbeDisposition::Passed, "runtime.health.provider_probe_passed")
                        }
                        Ok(Err(error)) if provider_probe_error_is_ambiguous(&error) => (
                            HealthProbeDisposition::Inconclusive,
                            "runtime.health.provider_probe_ambiguous",
                        ),
                        Ok(Err(_)) => {
                            (HealthProbeDisposition::Failed, "runtime.health.provider_probe_failed")
                        }
                        Err(_) => (
                            HealthProbeDisposition::Inconclusive,
                            "runtime.health.provider_probe_timed_out",
                        ),
                    }
                };
                let completed_at_unix_ms = current_unix_ms().max(begun.lease.issued_at_unix_ms);
                state
                    .journal_store
                    .settle_runtime_health_probe(&RuntimeHealthProbeSettlementRequest {
                        settlement: HealthProbeSettlementV1 {
                            schema_version: HEALTH_PROBE_SETTLEMENT_SCHEMA_VERSION,
                            lease_id: begun.lease.lease_id,
                            expected_generation: begun.lease.expected_generation,
                            result: HealthProbeResult {
                                schema_version: HEALTH_PROBE_RESULT_SCHEMA_VERSION,
                                component_id: begun.lease.component_id,
                                disposition,
                                reason_code: reason_code.to_owned(),
                                mutation_attempted: false,
                                completed_at_unix_ms,
                            },
                        },
                        probe_evidence_sha256: None,
                    })
                    .map_err(|error| {
                        Status::failed_precondition(format!(
                            "provider probe settle failed: {error}"
                        ))
                    })
            })
            .catch_unwind()
            .await;
            let outcome = match outcome {
                Ok(outcome) => outcome,
                Err(_) => {
                    let completed_at_unix_ms = current_unix_ms().max(panic_lease.issued_at_unix_ms);
                    panic_state
                        .journal_store
                        .settle_runtime_health_probe(&RuntimeHealthProbeSettlementRequest {
                            settlement: HealthProbeSettlementV1 {
                                schema_version: HEALTH_PROBE_SETTLEMENT_SCHEMA_VERSION,
                                lease_id: panic_lease.lease_id,
                                expected_generation: panic_lease.expected_generation,
                                result: HealthProbeResult {
                                    schema_version: HEALTH_PROBE_RESULT_SCHEMA_VERSION,
                                    component_id: panic_lease.component_id,
                                    disposition: HealthProbeDisposition::Inconclusive,
                                    reason_code: "runtime.health.provider_probe_panicked"
                                        .to_owned(),
                                    mutation_attempted: false,
                                    completed_at_unix_ms,
                                },
                            },
                            probe_evidence_sha256: None,
                        })
                        .map_err(|error| {
                            Status::failed_precondition(format!(
                                "panicked provider probe settle failed: {error}"
                            ))
                        })
                }
            };
            if outcome_tx.send(outcome).is_err() {
                warn!(lease_id = %lease_id, "provider health probe settled after caller disconnected");
            }
        });
        outcome_rx.await.map_err(|_| {
            Status::internal("provider health probe task ended without reporting settlement")
        })?
    }

    // Orchestrator sessions, usage accounting, and agent registry access.
    // From here to the end of the impl, most methods follow the
    // blocking/async delegate pattern described in the module header; the
    // async wrapper is the documented public surface.

    #[allow(clippy::result_large_err)]
    fn resolve_orchestrator_session_blocking(
        &self,
        request: &OrchestratorSessionResolveRequest,
    ) -> Result<OrchestratorSessionResolveOutcome, Status> {
        self.journal_store
            .resolve_orchestrator_session(request)
            .map_err(|error| map_orchestrator_store_error("resolve orchestrator session", error))
    }

    /// Resolves (or creates) the orchestrator session for a conversation context.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn resolve_orchestrator_session(
        self: &Arc<Self>,
        request: OrchestratorSessionResolveRequest,
    ) -> Result<OrchestratorSessionResolveOutcome, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.resolve_orchestrator_session_blocking(&request))
            .await
            .map_err(|_| Status::internal("orchestrator session resolve worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn update_orchestrator_session_title_blocking(
        &self,
        request: &OrchestratorSessionTitleUpdateRequest,
    ) -> Result<OrchestratorSessionRecord, Status> {
        self.journal_store.update_orchestrator_session_title(request).map_err(|error| {
            map_orchestrator_store_error("update orchestrator session title", error)
        })
    }

    /// Updates a session title.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn update_orchestrator_session_title(
        self: &Arc<Self>,
        request: OrchestratorSessionTitleUpdateRequest,
    ) -> Result<OrchestratorSessionRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.update_orchestrator_session_title_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("orchestrator session title worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn orchestrator_session_by_id_blocking(
        &self,
        session_id: &str,
    ) -> Result<Option<OrchestratorSessionRecord>, Status> {
        self.journal_store
            .orchestrator_session_by_id(session_id)
            .map_err(|error| map_orchestrator_store_error("load orchestrator session by id", error))
    }

    #[allow(clippy::result_large_err)]
    fn orchestrator_session_by_id_snapshot_blocking(
        &self,
        session_id: &str,
    ) -> Result<Option<OrchestratorSessionRecord>, Status> {
        self.journal_store.orchestrator_session_by_id_snapshot(session_id).map_err(|error| {
            map_orchestrator_store_error("load orchestrator session snapshot by id", error)
        })
    }

    /// Loads a session by id.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn orchestrator_session_by_id(
        self: &Arc<Self>,
        session_id: String,
    ) -> Result<Option<OrchestratorSessionRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.orchestrator_session_by_id_blocking(&session_id))
            .await
            .map_err(|_| Status::internal("orchestrator session lookup worker panicked"))?
    }

    /// Loads a read-only snapshot of a session by id.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn orchestrator_session_by_id_snapshot(
        self: &Arc<Self>,
        session_id: String,
    ) -> Result<Option<OrchestratorSessionRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.orchestrator_session_by_id_snapshot_blocking(&session_id)
        })
        .await
        .map_err(|_| Status::internal("orchestrator session snapshot worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn session_project_context_state_blocking(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionProjectContextStateRecord>, Status> {
        self.journal_store.session_project_context_state(session_id).map_err(|error| {
            map_orchestrator_store_error("load session project context state", error)
        })
    }

    /// Loads the per-session project context state, if any.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn session_project_context_state(
        self: &Arc<Self>,
        session_id: String,
    ) -> Result<Option<SessionProjectContextStateRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.session_project_context_state_blocking(session_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("session project context state worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn upsert_session_project_context_state_blocking(
        &self,
        request: &SessionProjectContextStateUpsertRequest,
    ) -> Result<SessionProjectContextStateRecord, Status> {
        self.journal_store.upsert_session_project_context_state(request).map_err(|error| {
            map_orchestrator_store_error("upsert session project context state", error)
        })
    }

    /// Creates or updates the per-session project context state.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn upsert_session_project_context_state(
        self: &Arc<Self>,
        request: SessionProjectContextStateUpsertRequest,
    ) -> Result<SessionProjectContextStateRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.upsert_session_project_context_state_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("session project context upsert worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn copy_session_project_context_state_blocking(
        &self,
        request: &SessionProjectContextStateCopyRequest,
    ) -> Result<Option<SessionProjectContextStateRecord>, Status> {
        self.journal_store.copy_session_project_context_state(request).map_err(|error| {
            map_orchestrator_store_error("copy session project context state", error)
        })
    }

    /// Copies project context state between sessions; `None` when the source has no state.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn copy_session_project_context_state(
        self: &Arc<Self>,
        request: SessionProjectContextStateCopyRequest,
    ) -> Result<Option<SessionProjectContextStateRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.copy_session_project_context_state_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("session project context copy worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn update_orchestrator_session_quick_controls_blocking(
        &self,
        request: &crate::journal::OrchestratorSessionQuickControlsUpdateRequest,
    ) -> Result<OrchestratorSessionRecord, Status> {
        self.journal_store.update_orchestrator_session_quick_controls(request).map_err(|error| {
            map_orchestrator_store_error("update orchestrator session quick controls", error)
        })
    }

    /// Updates a session's quick-control settings.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn update_orchestrator_session_quick_controls(
        self: &Arc<Self>,
        request: crate::journal::OrchestratorSessionQuickControlsUpdateRequest,
    ) -> Result<OrchestratorSessionRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.update_orchestrator_session_quick_controls_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("orchestrator session quick controls worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_orchestrator_sessions_blocking(
        &self,
        request: &ListOrchestratorSessionsRequest,
    ) -> Result<(Vec<OrchestratorSessionRecord>, Option<String>), Status> {
        let limit = request.requested_limit.unwrap_or(100).clamp(1, MAX_SESSIONS_PAGE_LIMIT);
        let normalized_search = request
            .search_query
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| value.to_ascii_lowercase());
        // Search cannot be pushed into the store query, so matching pages are
        // scanned in store order until limit+1 hits are collected; the extra
        // hit only signals has_more and is truncated below.
        let mut sessions = if let Some(search) = normalized_search.as_deref() {
            let mut matched = Vec::new();
            let mut cursor = request.after_session_key.clone();
            loop {
                let page = self
                    .journal_store
                    .list_orchestrator_sessions(
                        cursor.as_deref(),
                        request.principal.as_str(),
                        request.device_id.as_str(),
                        request.channel.as_deref(),
                        request.include_archived,
                        MAX_SESSIONS_PAGE_LIMIT,
                    )
                    .map_err(|error| {
                        map_orchestrator_store_error("list orchestrator sessions", error)
                    })?;
                if page.is_empty() {
                    break;
                }
                cursor = page.last().map(|session| session.session_key.clone());
                for mut session in page {
                    let matched_field = [
                        Some(session.title.as_str()),
                        session.preview.as_deref(),
                        session.last_intent.as_deref(),
                        session.last_summary.as_deref(),
                        session.last_run_state.as_deref(),
                    ]
                    .into_iter()
                    .flatten()
                    .find(|value| value.to_ascii_lowercase().contains(search))
                    .map(ToOwned::to_owned);
                    if let Some(snippet) = matched_field {
                        session.match_snippet = Some(snippet);
                        matched.push(session);
                        if matched.len() > limit {
                            break;
                        }
                    }
                }
                if matched.len() > limit || cursor.is_none() {
                    break;
                }
            }
            matched
        } else {
            self.journal_store
                .list_orchestrator_sessions(
                    request.after_session_key.as_deref(),
                    request.principal.as_str(),
                    request.device_id.as_str(),
                    request.channel.as_deref(),
                    request.include_archived,
                    limit.saturating_add(1),
                )
                .map_err(|error| {
                    map_orchestrator_store_error("list orchestrator sessions", error)
                })?
        };
        let has_more = sessions.len() > limit;
        if has_more {
            sessions.truncate(limit);
        }
        let next_after_session_key = if has_more {
            sessions.last().map(|session| session.session_key.clone())
        } else {
            None
        };
        Ok((sessions, next_after_session_key))
    }

    /// Lists sessions for a principal/device with cursor pagination and optional case-insensitive
    /// search over title/preview/intent/summary/run-state.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_orchestrator_sessions(
        self: &Arc<Self>,
        request: ListOrchestratorSessionsRequest,
    ) -> Result<(Vec<OrchestratorSessionRecord>, Option<String>), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.list_orchestrator_sessions_blocking(&request))
            .await
            .map_err(|_| Status::internal("orchestrator session list worker panicked"))?
    }

    // Same pagination/search shape as list_orchestrator_sessions_blocking,
    // but scoped to the principal across devices.
    fn list_orchestrator_sessions_for_principal_blocking(
        &self,
        request: &ListPrincipalOrchestratorSessionsRequest,
    ) -> Result<(Vec<OrchestratorSessionRecord>, Option<String>), Status> {
        let limit = request.requested_limit.unwrap_or(100).clamp(1, MAX_SESSIONS_PAGE_LIMIT);
        let normalized_search = request
            .search_query
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| value.to_ascii_lowercase());
        let mut sessions = if let Some(search) = normalized_search.as_deref() {
            let mut matched = Vec::new();
            let mut cursor = request.after_session_key.clone();
            loop {
                let page = self
                    .journal_store
                    .list_orchestrator_sessions_for_principal(
                        cursor.as_deref(),
                        request.principal.as_str(),
                        request.include_archived,
                        MAX_SESSIONS_PAGE_LIMIT,
                    )
                    .map_err(|error| {
                        map_orchestrator_store_error(
                            "list orchestrator sessions for principal",
                            error,
                        )
                    })?;
                if page.is_empty() {
                    break;
                }
                cursor = page.last().map(|session| session.session_key.clone());
                for mut session in page {
                    let matched_field = [
                        Some(session.title.as_str()),
                        session.preview.as_deref(),
                        session.last_intent.as_deref(),
                        session.last_summary.as_deref(),
                        session.last_run_state.as_deref(),
                    ]
                    .into_iter()
                    .flatten()
                    .find(|value| value.to_ascii_lowercase().contains(search))
                    .map(ToOwned::to_owned);
                    if let Some(snippet) = matched_field {
                        session.match_snippet = Some(snippet);
                        matched.push(session);
                        if matched.len() > limit {
                            break;
                        }
                    }
                }
                if matched.len() > limit || cursor.is_none() {
                    break;
                }
            }
            matched
        } else {
            self.journal_store
                .list_orchestrator_sessions_for_principal(
                    request.after_session_key.as_deref(),
                    request.principal.as_str(),
                    request.include_archived,
                    limit.saturating_add(1),
                )
                .map_err(|error| {
                    map_orchestrator_store_error("list orchestrator sessions for principal", error)
                })?
        };
        let has_more = sessions.len() > limit;
        if has_more {
            sessions.truncate(limit);
        }
        let next_after_session_key = if has_more {
            sessions.last().map(|session| session.session_key.clone())
        } else {
            None
        };
        Ok((sessions, next_after_session_key))
    }

    /// Like [`Self::list_orchestrator_sessions`] but scoped to the principal across all devices.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_orchestrator_sessions_for_principal(
        self: &Arc<Self>,
        request: ListPrincipalOrchestratorSessionsRequest,
    ) -> Result<(Vec<OrchestratorSessionRecord>, Option<String>), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.list_orchestrator_sessions_for_principal_blocking(&request)
        })
        .await
        .map_err(|_| {
            Status::internal("principal-scoped orchestrator session list worker panicked")
        })?
    }

    #[allow(clippy::result_large_err)]
    fn summarize_orchestrator_usage_blocking(
        &self,
        query: &OrchestratorUsageQuery,
    ) -> Result<OrchestratorUsageSummary, Status> {
        self.journal_store
            .summarize_orchestrator_usage(query)
            .map_err(|error| map_orchestrator_store_error("summarize orchestrator usage", error))
    }

    /// Aggregates orchestrator usage for the query window.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn summarize_orchestrator_usage(
        self: &Arc<Self>,
        query: OrchestratorUsageQuery,
    ) -> Result<OrchestratorUsageSummary, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.summarize_orchestrator_usage_blocking(&query))
            .await
            .map_err(|_| Status::internal("orchestrator usage summary worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_orchestrator_usage_sessions_blocking(
        &self,
        query: &OrchestratorUsageQuery,
    ) -> Result<Vec<OrchestratorUsageSessionRecord>, Status> {
        self.journal_store.list_orchestrator_usage_sessions(query).map_err(|error| {
            map_orchestrator_store_error("list orchestrator usage sessions", error)
        })
    }

    /// Lists per-session usage rows for the query window.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_orchestrator_usage_sessions(
        self: &Arc<Self>,
        query: OrchestratorUsageQuery,
    ) -> Result<Vec<OrchestratorUsageSessionRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.list_orchestrator_usage_sessions_blocking(&query))
            .await
            .map_err(|_| Status::internal("orchestrator usage session list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn get_orchestrator_usage_session_blocking(
        &self,
        query: &OrchestratorUsageQuery,
        session_id: &str,
        run_limit: usize,
    ) -> Result<Option<(OrchestratorUsageSessionRecord, Vec<OrchestratorUsageRunRecord>)>, Status>
    {
        self.journal_store
            .get_orchestrator_usage_session(query, session_id, run_limit)
            .map_err(|error| map_orchestrator_store_error("get orchestrator usage session", error))
    }

    /// Loads one session's usage plus up to `run_limit` of its run records.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn get_orchestrator_usage_session(
        self: &Arc<Self>,
        query: OrchestratorUsageQuery,
        session_id: String,
        run_limit: usize,
    ) -> Result<Option<(OrchestratorUsageSessionRecord, Vec<OrchestratorUsageRunRecord>)>, Status>
    {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.get_orchestrator_usage_session_blocking(&query, session_id.as_str(), run_limit)
        })
        .await
        .map_err(|_| Status::internal("orchestrator usage session detail worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn cleanup_orchestrator_session_blocking(
        &self,
        request: &OrchestratorSessionCleanupRequest,
    ) -> Result<OrchestratorSessionCleanupOutcome, Status> {
        self.journal_store
            .cleanup_orchestrator_session(request)
            .map_err(|error| map_orchestrator_store_error("cleanup orchestrator session", error))
    }

    /// Archives/cleans a session per the cleanup request.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn cleanup_orchestrator_session(
        self: &Arc<Self>,
        request: OrchestratorSessionCleanupRequest,
    ) -> Result<OrchestratorSessionCleanupOutcome, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.cleanup_orchestrator_session_blocking(&request))
            .await
            .map_err(|_| Status::internal("orchestrator session cleanup worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_agents_blocking(
        &self,
        after_agent_id: Option<String>,
        requested_limit: Option<usize>,
    ) -> Result<AgentListPage, Status> {
        self.agent_registry
            .list_agents(after_agent_id.as_deref(), requested_limit.or(Some(MAX_AGENTS_PAGE_LIMIT)))
            .map_err(|error| map_agent_registry_error("list agents", error))
    }

    /// Lists agents with cursor pagination.
    ///
    /// # Errors
    /// Returns the mapped agent registry error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_agents(
        self: &Arc<Self>,
        after_agent_id: Option<String>,
        requested_limit: Option<usize>,
    ) -> Result<AgentListPage, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.list_agents_blocking(after_agent_id, requested_limit)
        })
        .await
        .map_err(|_| Status::internal("agent list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn get_agent_blocking(&self, agent_id: &str) -> Result<(AgentRecord, bool), Status> {
        self.agent_registry
            .get_agent(agent_id)
            .map_err(|error| map_agent_registry_error("get agent", error))
    }

    /// Loads an agent record; the bool flags whether it is the default agent.
    ///
    /// # Errors
    /// Returns the mapped agent registry error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn get_agent(
        self: &Arc<Self>,
        agent_id: String,
    ) -> Result<(AgentRecord, bool), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.get_agent_blocking(agent_id.as_str()))
            .await
            .map_err(|_| Status::internal("agent get worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn create_agent_blocking(
        &self,
        request: &AgentCreateRequest,
    ) -> Result<AgentCreateOutcome, Status> {
        self.agent_registry
            .create_agent(request.clone())
            .map_err(|error| map_agent_registry_error("create agent", error))
    }

    /// Creates an agent, defaulting its model profile from the provider registry when unset;
    /// updates mutation/validation counters.
    ///
    /// # Errors
    /// Returns the mapped agent registry error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn create_agent(
        self: &Arc<Self>,
        request: AgentCreateRequest,
    ) -> Result<AgentCreateOutcome, Status> {
        let request = self.create_agent_request_with_runtime_defaults(request);
        let state = Arc::clone(self);
        let result = tokio::task::spawn_blocking(move || state.create_agent_blocking(&request))
            .await
            .map_err(|_| Status::internal("agent create worker panicked"))?;
        if let Err(status) = &result {
            if status.code() == tonic::Code::InvalidArgument {
                self.counters.agent_validation_failures.fetch_add(1, Ordering::Relaxed);
            }
        } else {
            self.counters.agent_mutations.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    fn create_agent_request_with_runtime_defaults(
        &self,
        mut request: AgentCreateRequest,
    ) -> AgentCreateRequest {
        if request
            .default_model_profile
            .as_deref()
            .and_then(normalize_optional_agent_model_profile)
            .is_none()
        {
            request.default_model_profile = self.default_agent_model_profile();
        }
        request
    }

    fn default_agent_model_profile(&self) -> Option<String> {
        let snapshot = self.current_model_provider().status_snapshot();
        select_default_agent_model_profile(
            snapshot.registry.default_chat_model_id.as_deref(),
            snapshot.model_id.as_deref(),
            snapshot.openai_model.as_deref(),
            snapshot.anthropic_model.as_deref(),
        )
    }

    #[allow(clippy::result_large_err)]
    fn delete_agent_blocking(&self, agent_id: &str) -> Result<AgentDeleteOutcome, Status> {
        self.agent_registry
            .delete_agent(agent_id)
            .map_err(|error| map_agent_registry_error("delete agent", error))
    }

    /// Deletes an agent; updates mutation/validation counters.
    ///
    /// # Errors
    /// Returns the mapped agent registry error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn delete_agent(
        self: &Arc<Self>,
        agent_id: String,
    ) -> Result<AgentDeleteOutcome, Status> {
        let state = Arc::clone(self);
        let result =
            tokio::task::spawn_blocking(move || state.delete_agent_blocking(agent_id.as_str()))
                .await
                .map_err(|_| Status::internal("agent delete worker panicked"))?;
        if let Err(status) = &result {
            if status.code() == tonic::Code::InvalidArgument {
                self.counters.agent_validation_failures.fetch_add(1, Ordering::Relaxed);
            }
        } else {
            self.counters.agent_mutations.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    #[allow(clippy::result_large_err)]
    fn set_default_agent_blocking(&self, agent_id: &str) -> Result<AgentSetDefaultOutcome, Status> {
        self.agent_registry
            .set_default_agent(agent_id)
            .map_err(|error| map_agent_registry_error("set default agent", error))
    }

    /// Marks an agent as the default; updates mutation/validation counters.
    ///
    /// # Errors
    /// Returns the mapped agent registry error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn set_default_agent(
        self: &Arc<Self>,
        agent_id: String,
    ) -> Result<AgentSetDefaultOutcome, Status> {
        let state = Arc::clone(self);
        let result = tokio::task::spawn_blocking(move || {
            state.set_default_agent_blocking(agent_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("agent default worker panicked"))?;
        if let Err(status) = &result {
            if status.code() == tonic::Code::InvalidArgument {
                self.counters.agent_validation_failures.fetch_add(1, Ordering::Relaxed);
            }
        } else {
            self.counters.agent_mutations.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    #[allow(clippy::result_large_err)]
    fn list_agent_bindings_blocking(
        &self,
        query: &AgentBindingQuery,
    ) -> Result<Vec<SessionAgentBinding>, Status> {
        self.agent_registry
            .list_bindings(query.clone())
            .map_err(|error| map_agent_registry_error("list agent bindings", error))
    }

    /// Lists session-agent bindings matching the query.
    ///
    /// # Errors
    /// Returns the mapped agent registry error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_agent_bindings(
        self: &Arc<Self>,
        query: AgentBindingQuery,
    ) -> Result<Vec<SessionAgentBinding>, Status> {
        let state = Arc::clone(self);
        let result =
            tokio::task::spawn_blocking(move || state.list_agent_bindings_blocking(&query))
                .await
                .map_err(|_| Status::internal("agent binding list worker panicked"))?;
        if let Err(status) = &result {
            if status.code() == tonic::Code::InvalidArgument {
                self.counters.agent_validation_failures.fetch_add(1, Ordering::Relaxed);
            }
        }
        result
    }

    #[allow(clippy::result_large_err)]
    fn bind_agent_for_context_blocking(
        &self,
        request: &AgentBindingRequest,
    ) -> Result<AgentBindingOutcome, Status> {
        self.agent_registry
            .bind_agent_for_context(request.clone())
            .map_err(|error| map_agent_registry_error("bind agent for context", error))
    }

    /// Binds an agent to a conversation context; updates mutation/validation counters.
    ///
    /// # Errors
    /// Returns the mapped agent registry error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn bind_agent_for_context(
        self: &Arc<Self>,
        request: AgentBindingRequest,
    ) -> Result<AgentBindingOutcome, Status> {
        let state = Arc::clone(self);
        let result =
            tokio::task::spawn_blocking(move || state.bind_agent_for_context_blocking(&request))
                .await
                .map_err(|_| Status::internal("agent bind worker panicked"))?;
        match &result {
            Ok(_) => {
                self.counters.agent_mutations.fetch_add(1, Ordering::Relaxed);
            }
            Err(status) => {
                if status.code() == tonic::Code::InvalidArgument {
                    self.counters.agent_validation_failures.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        result
    }

    #[allow(clippy::result_large_err)]
    fn unbind_agent_for_context_blocking(
        &self,
        request: &AgentUnbindRequest,
    ) -> Result<AgentUnbindOutcome, Status> {
        self.agent_registry
            .unbind_agent_for_context(request.clone())
            .map_err(|error| map_agent_registry_error("unbind agent for context", error))
    }

    /// Removes an agent binding; counts a mutation only when something was actually removed.
    ///
    /// # Errors
    /// Returns the mapped agent registry error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn unbind_agent_for_context(
        self: &Arc<Self>,
        request: AgentUnbindRequest,
    ) -> Result<AgentUnbindOutcome, Status> {
        let state = Arc::clone(self);
        let result =
            tokio::task::spawn_blocking(move || state.unbind_agent_for_context_blocking(&request))
                .await
                .map_err(|_| Status::internal("agent unbind worker panicked"))?;
        match &result {
            Ok(outcome) => {
                if outcome.removed {
                    self.counters.agent_mutations.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(status) => {
                if status.code() == tonic::Code::InvalidArgument {
                    self.counters.agent_validation_failures.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        result
    }

    #[allow(clippy::result_large_err)]
    fn resolve_agent_for_context_blocking(
        &self,
        request: &AgentResolveRequest,
    ) -> Result<AgentResolveOutcome, Status> {
        self.agent_registry
            .resolve_agent_for_context(request.clone())
            .map_err(|error| map_agent_registry_error("resolve agent for context", error))
    }

    /// Resolves the agent for a context, recording binding hit/miss counters and any binding
    /// created on the way.
    ///
    /// # Errors
    /// Returns the mapped agent registry error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn resolve_agent_for_context(
        self: &Arc<Self>,
        request: AgentResolveRequest,
    ) -> Result<AgentResolveOutcome, Status> {
        let state = Arc::clone(self);
        let result =
            tokio::task::spawn_blocking(move || state.resolve_agent_for_context_blocking(&request))
                .await
                .map_err(|_| Status::internal("agent resolve worker panicked"))?;
        match &result {
            Ok(outcome) => {
                if matches!(outcome.source, AgentResolutionSource::SessionBinding) {
                    self.counters.agent_resolution_hits.fetch_add(1, Ordering::Relaxed);
                } else {
                    self.counters.agent_resolution_misses.fetch_add(1, Ordering::Relaxed);
                }
                if outcome.binding_created {
                    self.counters.agent_mutations.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(status) => {
                if status.code() == tonic::Code::InvalidArgument {
                    self.counters.agent_validation_failures.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        result
    }

    // Run lifecycle: idempotent start, metadata/state updates, tape, cancel
    // flags, startup recovery, and run-completion waiting.

    // Returns Ok(true) only when this call actually inserted the run, so the
    // async wrapper counts each run exactly once across idempotent retries.
    #[allow(clippy::result_large_err)]
    fn start_orchestrator_run_blocking(
        &self,
        request: &OrchestratorRunStartRequest,
    ) -> Result<bool, Status> {
        let idempotency_key = format!("run:start:{}", request.run_id);
        let payload_sha256 = orchestrator_run_start_payload_sha256(request)?;
        let begin = self
            .journal_store
            .begin_idempotency_operation(&IdempotencyBeginRequest {
                key: idempotency_key.clone(),
                scope: "orchestrator_run".to_owned(),
                operation_kind: "start_orchestrator_run".to_owned(),
                payload_sha256,
                expires_at_unix_ms: Some(
                    current_unix_ms().saturating_add(24_i64 * 60 * 60 * 1_000),
                ),
            })
            .map_err(|error| map_orchestrator_store_error("begin idempotent run start", error))?;

        match begin.decision {
            IdempotencyReplayDecision::CompletedReplayResult => return Ok(false),
            IdempotencyReplayDecision::ConflictingPayload => {
                return Err(Status::already_exists(format!(
                    "conflicting idempotent start request for run {}",
                    request.run_id
                )));
            }
            IdempotencyReplayDecision::SamePayloadRetry
            | IdempotencyReplayDecision::Reserved
            | IdempotencyReplayDecision::ExpiredRetry => {}
        }

        match self.journal_store.start_orchestrator_run(request) {
            Ok(()) => {
                self.complete_run_start_idempotency(idempotency_key.as_str(), request)?;
                Ok(true)
            }
            // A duplicate run id on a retry/reserved decision means an
            // earlier attempt inserted the run but crashed before completing
            // the idempotency record: finish that record and report replay.
            Err(JournalError::DuplicateRunId { .. })
                if matches!(
                    begin.decision,
                    IdempotencyReplayDecision::SamePayloadRetry
                        | IdempotencyReplayDecision::Reserved
                        | IdempotencyReplayDecision::ExpiredRetry
                ) =>
            {
                self.complete_run_start_idempotency(idempotency_key.as_str(), request)?;
                Ok(false)
            }
            Err(error) => {
                let _ = self.journal_store.fail_idempotency_operation(&IdempotencyFailRequest {
                    key: idempotency_key,
                    error: stable_error_from_journal("run_start_failed", &error),
                });
                Err(map_orchestrator_store_error("start orchestrator run", error))
            }
        }
    }

    /// Starts a run idempotently: replays of the same start payload are
    /// accepted without effect, conflicting payloads under the same run id
    /// are rejected. Wakes run waiters either way.
    ///
    /// # Errors
    /// `already_exists` for conflicting idempotent payloads, otherwise the
    /// mapped journal error or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn start_orchestrator_run(
        self: &Arc<Self>,
        request: OrchestratorRunStartRequest,
    ) -> Result<(), Status> {
        let run_id = request.run_id.clone();
        let parameter_delta_json = request.parameter_delta_json.clone();
        let persisted_request = request.clone();
        let state = Arc::clone(self);
        let inserted = tokio::task::spawn_blocking(move || {
            state.start_orchestrator_run_blocking(&persisted_request)
        })
        .await
        .map_err(|_| Status::internal("orchestrator run worker panicked"))??;
        self.remember_run_parameter_delta_json(run_id.as_str(), parameter_delta_json.as_deref());
        if inserted {
            self.counters.orchestrator_runs_started.fetch_add(1, Ordering::Relaxed);
            let state = Arc::clone(self);
            let admission =
                tokio::task::spawn_blocking(move || state.admit_managed_coding_run(&request))
                    .await
                    .map_err(|_| Status::internal("managed coding admission worker panicked"))?;
            if admission.is_err() {
                let message = "managed coding workspace admission failed".to_owned();
                self.update_orchestrator_run_state(
                    run_id.clone(),
                    RunLifecycleState::Failed,
                    Some(message.clone()),
                )
                .await?;
                return Err(Status::failed_precondition(message));
            }
        }
        self.orchestrator_run_notify.notify_waiters();
        Ok(())
    }

    fn complete_run_start_idempotency(
        &self,
        idempotency_key: &str,
        request: &OrchestratorRunStartRequest,
    ) -> Result<(), Status> {
        let result_json = json!({
            "run_id": request.run_id,
            "session_id": request.session_id,
            "state": RunLifecyclePhase::Queued.as_str(),
        })
        .to_string();
        self.journal_store
            .complete_idempotency_operation(&IdempotencyCompleteRequest {
                key: idempotency_key.to_owned(),
                result_json,
            })
            .map(|_| ())
            .map_err(|error| map_orchestrator_store_error("complete idempotent run start", error))
    }

    #[allow(clippy::result_large_err)]
    fn update_orchestrator_run_metadata_blocking(
        &self,
        request: &OrchestratorRunMetadataUpdateRequest,
    ) -> Result<(), Status> {
        self.journal_store.update_orchestrator_run_metadata(request).map_err(|error| {
            map_orchestrator_store_error("update orchestrator run metadata", error)
        })
    }

    /// Updates run metadata (intent, summary, parameters).
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn update_orchestrator_run_metadata(
        self: &Arc<Self>,
        request: OrchestratorRunMetadataUpdateRequest,
    ) -> Result<(), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.update_orchestrator_run_metadata_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("orchestrator run metadata worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn update_orchestrator_run_metadata_if_parent_generation_blocking(
        &self,
        request: &OrchestratorRunMetadataUpdateRequest,
        parent_guard: &OrchestratorParentGenerationGuard,
    ) -> Result<bool, Status> {
        self.journal_store
            .update_orchestrator_run_metadata_if_parent_generation(request, parent_guard)
            .map_err(|error| {
                map_orchestrator_store_error(
                    "update orchestrator run metadata under parent generation",
                    error,
                )
            })
    }

    /// Updates child metadata under exact durable parent-generation authority.
    ///
    /// `false` means the parent was superseded before the mutation acquired its
    /// journal transaction; no metadata was changed.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn update_orchestrator_run_metadata_if_parent_generation(
        self: &Arc<Self>,
        request: OrchestratorRunMetadataUpdateRequest,
        parent_guard: OrchestratorParentGenerationGuard,
    ) -> Result<bool, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.update_orchestrator_run_metadata_if_parent_generation_blocking(
                &request,
                &parent_guard,
            )
        })
        .await
        .map_err(|_| Status::internal("guarded orchestrator run metadata worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn settle_orchestrator_run_terminal_blocking(
        &self,
        request: &OrchestratorRunTerminalSettlementRequest,
        authority: TerminalSettlementAuthority,
    ) -> Result<OrchestratorRunTerminalSettlement, Status> {
        match authority {
            TerminalSettlementAuthority::Exact => self
                .journal_store
                .settle_orchestrator_run_terminal_exact(request)
                .map_err(|error| {
                    map_orchestrator_store_error("settle exact orchestrator run terminal", error)
                }),
            TerminalSettlementAuthority::CancellationAware => {
                self.journal_store.settle_orchestrator_run_terminal(request).map_err(|error| {
                    map_orchestrator_store_error("settle orchestrator run terminal", error)
                })
            }
        }
    }

    #[allow(clippy::result_large_err)]
    async fn settle_orchestrator_run_terminal_with_authority(
        self: &Arc<Self>,
        request: OrchestratorRunTerminalSettlementRequest,
        authority: TerminalSettlementAuthority,
    ) -> Result<OrchestratorRunTerminalSettlement, Status> {
        let run_id = request.run_id.clone();
        let state_ref = Arc::clone(self);
        let settlement = tokio::task::spawn_blocking(move || {
            state_ref.settle_orchestrator_run_terminal_blocking(&request, authority)
        })
        .await
        .map_err(|_| Status::internal("orchestrator run settlement worker panicked"))??;
        if settlement.changed {
            let persisted_tape_events = usize::from(settlement.summary_tape_sequence.is_some()) + 1;
            self.counters.orchestrator_tape_events.fetch_add(
                u64::try_from(persisted_tape_events).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            match settlement.effective_state {
                RunLifecycleState::Done => {
                    self.counters.orchestrator_runs_completed.fetch_add(1, Ordering::Relaxed);
                }
                RunLifecycleState::Failed => {
                    self.counters.orchestrator_runs_failed.fetch_add(1, Ordering::Relaxed);
                }
                RunLifecycleState::Cancelled => {
                    self.counters.orchestrator_runs_cancelled.fetch_add(1, Ordering::Relaxed);
                }
                RunLifecycleState::Pending
                | RunLifecycleState::Accepted
                | RunLifecycleState::InProgress => {}
            }
            self.mark_feature_usage_run_terminal(run_id.as_str());
            self.orchestrator_run_notify.notify_waiters();
        }
        Ok(settlement)
    }

    /// Atomically settles a run terminal state and applies terminal accounting
    /// only when this call owns the durable transition.
    ///
    /// # Errors
    /// Returns the mapped journal error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn settle_orchestrator_run_terminal(
        self: &Arc<Self>,
        request: OrchestratorRunTerminalSettlementRequest,
    ) -> Result<OrchestratorRunTerminalSettlement, Status> {
        self.settle_orchestrator_run_terminal_with_authority(
            request,
            TerminalSettlementAuthority::CancellationAware,
        )
        .await
    }

    /// Converges the outer run lifecycle to an authoritative kernel outcome.
    ///
    /// Pending cancellation remains recorded but cannot replace the supplied
    /// outcome. An already-terminal run must have the same outcome.
    ///
    /// # Errors
    /// Returns the mapped journal error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn settle_orchestrator_run_terminal_exact(
        self: &Arc<Self>,
        request: OrchestratorRunTerminalSettlementRequest,
    ) -> Result<OrchestratorRunTerminalSettlement, Status> {
        self.settle_orchestrator_run_terminal_with_authority(
            request,
            TerminalSettlementAuthority::Exact,
        )
        .await
    }

    #[allow(clippy::result_large_err)]
    fn update_orchestrator_run_state_blocking(
        &self,
        run_id: &str,
        state: RunLifecycleState,
        error_message: Option<&str>,
    ) -> Result<(), Status> {
        self.journal_store
            .update_orchestrator_run_state(run_id, state, error_message)
            .map_err(|error| map_orchestrator_store_error("update orchestrator run state", error))
    }

    /// Transitions a run's lifecycle state, bumping completed/cancelled
    /// counters on terminal states, and wakes run waiters.
    ///
    /// # Errors
    /// Returns the mapped journal error, or `internal` if the worker
    /// panicked.
    #[allow(clippy::result_large_err)]
    pub async fn update_orchestrator_run_state(
        self: &Arc<Self>,
        run_id: String,
        state: RunLifecycleState,
        error_message: Option<String>,
    ) -> Result<(), Status> {
        let state_ref = Arc::clone(self);
        let error_message_ref = error_message.clone();
        let persisted_run_id = run_id.clone();
        tokio::task::spawn_blocking(move || {
            state_ref.update_orchestrator_run_state_blocking(
                persisted_run_id.as_str(),
                state,
                error_message_ref.as_deref(),
            )
        })
        .await
        .map_err(|_| Status::internal("orchestrator run state worker panicked"))??;
        match state {
            RunLifecycleState::Done => {
                self.counters.orchestrator_runs_completed.fetch_add(1, Ordering::Relaxed);
            }
            RunLifecycleState::Failed => {
                self.counters.orchestrator_runs_failed.fetch_add(1, Ordering::Relaxed);
            }
            RunLifecycleState::Cancelled => {
                self.counters.orchestrator_runs_cancelled.fetch_add(1, Ordering::Relaxed);
            }
            RunLifecycleState::Pending
            | RunLifecycleState::Accepted
            | RunLifecycleState::InProgress => {}
        }
        if matches!(
            state,
            RunLifecycleState::Done | RunLifecycleState::Failed | RunLifecycleState::Cancelled
        ) {
            self.mark_feature_usage_run_terminal(run_id.as_str());
        }
        self.orchestrator_run_notify.notify_waiters();
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    fn terminalize_orphaned_orchestrator_runs_on_startup_blocking(
        &self,
        reason: &str,
    ) -> Result<OrchestratorStartupRunRecoveryReport, Status> {
        self.journal_store.terminalize_orphaned_orchestrator_runs_on_startup(reason).map_err(
            |error| map_orchestrator_store_error("terminalize orphaned orchestrator runs", error),
        )
    }

    /// Marks runs left non-terminal by a previous daemon process as failed
    /// with `reason`; wakes run waiters when anything changed.
    ///
    /// # Errors
    /// Returns the mapped journal error, or `internal` if the worker
    /// panicked.
    #[allow(clippy::result_large_err)]
    pub async fn terminalize_orphaned_orchestrator_runs_on_startup(
        self: &Arc<Self>,
        reason: impl Into<String>,
    ) -> Result<OrchestratorStartupRunRecoveryReport, Status> {
        let state = Arc::clone(self);
        let reason = reason.into();
        let report = tokio::task::spawn_blocking(move || {
            state.terminalize_orphaned_orchestrator_runs_on_startup_blocking(reason.as_str())
        })
        .await
        .map_err(|_| Status::internal("orchestrator startup recovery worker panicked"))??;
        if report.terminalized_count > 0 {
            self.orchestrator_run_notify.notify_waiters();
        }
        Ok(report)
    }

    #[allow(clippy::result_large_err)]
    fn reconcile_orphaned_background_tasks_on_startup_blocking(
        &self,
        reason: &str,
    ) -> Result<OrchestratorStartupBackgroundTaskRecoveryReport, Status> {
        self.journal_store.reconcile_orphaned_background_tasks_on_startup(reason).map_err(|error| {
            map_orchestrator_store_error("reconcile orphaned background tasks", error)
        })
    }

    /// Fails closed in-process background tasks whose detached workers could
    /// not survive daemon restart.
    ///
    /// # Errors
    /// Returns the mapped journal error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn reconcile_orphaned_background_tasks_on_startup(
        self: &Arc<Self>,
        reason: impl Into<String>,
    ) -> Result<OrchestratorStartupBackgroundTaskRecoveryReport, Status> {
        let state = Arc::clone(self);
        let reason = reason.into();
        tokio::task::spawn_blocking(move || {
            state.reconcile_orphaned_background_tasks_on_startup_blocking(reason.as_str())
        })
        .await
        .map_err(|_| Status::internal("background task startup recovery worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn resolve_background_task_child_blocking(
        &self,
        task_id: &str,
        expected_state: &str,
        requested_run_id: Option<&str>,
    ) -> Result<crate::journal::BackgroundTaskChildResolution, Status> {
        self.journal_store
            .resolve_background_task_child(task_id, expected_state, requested_run_id)
            .map_err(|error| map_orchestrator_store_error("resolve background task child", error))
    }

    #[allow(clippy::result_large_err)]
    fn attach_background_task_child_blocking(
        &self,
        task_id: &str,
        run_id: &str,
        execution_generation: u64,
    ) -> Result<crate::journal::BackgroundTaskChildResolution, Status> {
        self.journal_store
            .attach_background_task_child(task_id, run_id, execution_generation)
            .map_err(|error| map_orchestrator_store_error("attach background task child", error))
    }

    /// Resolves and atomically attaches or reconciles one delegated child run.
    ///
    /// # Errors
    /// Returns a mapped journal error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn resolve_background_task_child(
        self: &Arc<Self>,
        task_id: String,
        expected_state: String,
        requested_run_id: Option<String>,
    ) -> Result<crate::journal::BackgroundTaskChildResolution, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.resolve_background_task_child_blocking(
                task_id.as_str(),
                expected_state.as_str(),
                requested_run_id.as_deref(),
            )
        })
        .await
        .map_err(|_| Status::internal("background task child resolution worker panicked"))?
    }

    /// Attaches an exact child run while its task is running or cancellation is pending.
    ///
    /// # Errors
    /// Returns a mapped journal error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    #[cfg(test)]
    pub(crate) fn fail_next_background_task_child_attachment_for_test(&self) {
        self.fail_background_task_child_attachment_once.store(true, Ordering::SeqCst);
    }

    pub async fn attach_background_task_child(
        self: &Arc<Self>,
        task_id: String,
        run_id: String,
        execution_generation: u64,
    ) -> Result<crate::journal::BackgroundTaskChildResolution, Status> {
        #[cfg(test)]
        if self.fail_background_task_child_attachment_once.swap(false, Ordering::SeqCst) {
            return Err(Status::internal("QA fault injection: background child attachment failed"));
        }
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.attach_background_task_child_blocking(
                task_id.as_str(),
                run_id.as_str(),
                execution_generation,
            )
        })
        .await
        .map_err(|_| Status::internal("background task child attachment worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn reconcile_background_task_before_retry_blocking(
        &self,
        task_id: &str,
        expected_state: &str,
    ) -> Result<Option<OrchestratorBackgroundTaskRecord>, Status> {
        self.journal_store.reconcile_background_task_before_retry(task_id, expected_state).map_err(
            |error| map_orchestrator_store_error("reconcile background task before retry", error),
        )
    }

    /// Reconciles a terminal child-backed task before an operator retry may clear evidence.
    ///
    /// # Errors
    /// Returns the mapped journal error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn reconcile_background_task_before_retry(
        self: &Arc<Self>,
        task_id: String,
        expected_state: String,
    ) -> Result<Option<OrchestratorBackgroundTaskRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.reconcile_background_task_before_retry_blocking(
                task_id.as_str(),
                expected_state.as_str(),
            )
        })
        .await
        .map_err(|_| Status::internal("background task retry reconciliation worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn add_orchestrator_usage_blocking(
        &self,
        delta: &OrchestratorUsageDelta,
    ) -> Result<(), Status> {
        self.journal_store
            .add_orchestrator_usage(delta)
            .map_err(|error| map_orchestrator_store_error("update orchestrator usage", error))
    }

    /// Accumulates a usage delta onto run and session totals.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn add_orchestrator_usage(
        self: &Arc<Self>,
        delta: OrchestratorUsageDelta,
    ) -> Result<(), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.add_orchestrator_usage_blocking(&delta))
            .await
            .map_err(|_| Status::internal("orchestrator usage worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_orchestrator_usage_runs_blocking(
        &self,
        query: &OrchestratorUsageQuery,
        limit: usize,
    ) -> Result<Vec<crate::journal::OrchestratorUsageInsightsRunRecord>, Status> {
        self.journal_store
            .list_orchestrator_usage_runs(query, limit)
            .map_err(|error| map_orchestrator_store_error("list orchestrator usage runs", error))
    }

    /// Lists usage-insight run records for the query window.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_orchestrator_usage_runs(
        self: &Arc<Self>,
        query: OrchestratorUsageQuery,
        limit: usize,
    ) -> Result<Vec<crate::journal::OrchestratorUsageInsightsRunRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.list_orchestrator_usage_runs_blocking(&query, limit)
        })
        .await
        .map_err(|_| Status::internal("orchestrator usage runs worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_usage_pricing_records_blocking(
        &self,
    ) -> Result<Vec<crate::journal::UsagePricingRecord>, Status> {
        self.journal_store
            .list_usage_pricing_records()
            .map_err(|error| map_orchestrator_store_error("list usage pricing records", error))
    }

    /// Lists model pricing records.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_usage_pricing_records(
        self: &Arc<Self>,
    ) -> Result<Vec<crate::journal::UsagePricingRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.list_usage_pricing_records_blocking())
            .await
            .map_err(|_| Status::internal("usage pricing list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn upsert_usage_pricing_record_blocking(
        &self,
        request: &crate::journal::UsagePricingUpsertRequest,
    ) -> Result<crate::journal::UsagePricingRecord, Status> {
        self.journal_store
            .upsert_usage_pricing_record(request)
            .map_err(|error| map_orchestrator_store_error("upsert usage pricing record", error))
    }

    /// Creates or updates a model pricing record.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn upsert_usage_pricing_record(
        self: &Arc<Self>,
        request: crate::journal::UsagePricingUpsertRequest,
    ) -> Result<crate::journal::UsagePricingRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.upsert_usage_pricing_record_blocking(&request))
            .await
            .map_err(|_| Status::internal("usage pricing upsert worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn upsert_usage_budget_policy_blocking(
        &self,
        request: &crate::journal::UsageBudgetPolicyUpsertRequest,
    ) -> Result<crate::journal::UsageBudgetPolicyRecord, Status> {
        self.journal_store
            .upsert_usage_budget_policy(request)
            .map_err(|error| map_orchestrator_store_error("upsert usage budget policy", error))
    }

    /// Creates or updates a usage budget policy.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn upsert_usage_budget_policy(
        self: &Arc<Self>,
        request: crate::journal::UsageBudgetPolicyUpsertRequest,
    ) -> Result<crate::journal::UsageBudgetPolicyRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.upsert_usage_budget_policy_blocking(&request))
            .await
            .map_err(|_| Status::internal("usage budget upsert worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_usage_budget_policies_blocking(
        &self,
        filter: &crate::journal::UsageBudgetPoliciesFilter,
    ) -> Result<Vec<crate::journal::UsageBudgetPolicyRecord>, Status> {
        self.journal_store
            .list_usage_budget_policies(filter)
            .map_err(|error| map_orchestrator_store_error("list usage budget policies", error))
    }

    /// Lists usage budget policies matching the filter.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_usage_budget_policies(
        self: &Arc<Self>,
        filter: crate::journal::UsageBudgetPoliciesFilter,
    ) -> Result<Vec<crate::journal::UsageBudgetPolicyRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.list_usage_budget_policies_blocking(&filter))
            .await
            .map_err(|_| Status::internal("usage budget list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn create_usage_routing_decision_blocking(
        &self,
        request: &crate::journal::UsageRoutingDecisionCreateRequest,
    ) -> Result<crate::journal::UsageRoutingDecisionRecord, Status> {
        self.journal_store
            .create_usage_routing_decision(request)
            .map_err(|error| map_orchestrator_store_error("create usage routing decision", error))
    }

    /// Records a smart-routing decision for auditability.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn create_usage_routing_decision(
        self: &Arc<Self>,
        request: crate::journal::UsageRoutingDecisionCreateRequest,
    ) -> Result<crate::journal::UsageRoutingDecisionRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.create_usage_routing_decision_blocking(&request))
            .await
            .map_err(|_| Status::internal("usage routing decision worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_usage_routing_decisions_blocking(
        &self,
        filter: &crate::journal::UsageRoutingDecisionsFilter,
    ) -> Result<Vec<crate::journal::UsageRoutingDecisionRecord>, Status> {
        self.journal_store
            .list_usage_routing_decisions(filter)
            .map_err(|error| map_orchestrator_store_error("list usage routing decisions", error))
    }

    /// Lists recorded smart-routing decisions matching the filter.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_usage_routing_decisions(
        self: &Arc<Self>,
        filter: crate::journal::UsageRoutingDecisionsFilter,
    ) -> Result<Vec<crate::journal::UsageRoutingDecisionRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.list_usage_routing_decisions_blocking(&filter))
            .await
            .map_err(|_| Status::internal("usage routing decision list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn upsert_usage_alert_blocking(
        &self,
        request: &crate::journal::UsageAlertUpsertRequest,
    ) -> Result<crate::journal::UsageAlertRecord, Status> {
        self.journal_store
            .upsert_usage_alert(request)
            .map_err(|error| map_orchestrator_store_error("upsert usage alert", error))
    }

    /// Creates or updates a usage alert.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn upsert_usage_alert(
        self: &Arc<Self>,
        request: crate::journal::UsageAlertUpsertRequest,
    ) -> Result<crate::journal::UsageAlertRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.upsert_usage_alert_blocking(&request))
            .await
            .map_err(|_| Status::internal("usage alert upsert worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_usage_alerts_blocking(
        &self,
        filter: &crate::journal::UsageAlertsFilter,
    ) -> Result<Vec<crate::journal::UsageAlertRecord>, Status> {
        self.journal_store
            .list_usage_alerts(filter)
            .map_err(|error| map_orchestrator_store_error("list usage alerts", error))
    }

    /// Lists usage alerts matching the filter.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_usage_alerts(
        self: &Arc<Self>,
        filter: crate::journal::UsageAlertsFilter,
    ) -> Result<Vec<crate::journal::UsageAlertRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.list_usage_alerts_blocking(&filter))
            .await
            .map_err(|_| Status::internal("usage alerts list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn shared_runtime_diagnostics_blocking(&self) -> Result<SharedRuntimeDiagnostics, Status> {
        self.journal_store
            .shared_runtime_diagnostics()
            .map_err(|error| map_orchestrator_store_error("load shared runtime diagnostics", error))
    }

    /// Reconciles expired supported health probes without blocking the async runtime.
    ///
    /// # Errors
    /// Returns the mapped journal error, or `internal` if the blocking worker panics.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn reconcile_runtime_health_probes_async(
        self: &Arc<Self>,
    ) -> Result<RuntimeHealthProbeReconciliationOutcome, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .reconcile_runtime_health_probes(
                    RuntimeHealthProbeReconciliationMode::Periodic,
                    current_unix_ms(),
                )
                .map_err(|error| {
                    map_orchestrator_store_error("reconcile runtime health probes", error)
                })
        })
        .await
        .map_err(|_| Status::internal("runtime health reconciliation worker panicked"))?
    }

    /// Loads bounded aggregate diagnostics for generation, side-effect, health, and cleanup state.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn shared_runtime_diagnostics(
        self: &Arc<Self>,
    ) -> Result<SharedRuntimeDiagnostics, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.shared_runtime_diagnostics_blocking())
            .await
            .map_err(|_| Status::internal("shared runtime diagnostics worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn latest_approval_by_subject_blocking(
        &self,
        subject_id: &str,
    ) -> Result<Option<crate::journal::ApprovalRecord>, Status> {
        self.journal_store
            .latest_approval_by_subject(subject_id)
            .map_err(|error| map_orchestrator_store_error("load latest approval by subject", error))
    }

    /// Loads the most recent approval recorded for a subject id.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn latest_approval_by_subject(
        self: &Arc<Self>,
        subject_id: String,
    ) -> Result<Option<crate::journal::ApprovalRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.latest_approval_by_subject_blocking(subject_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("usage approval lookup worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn prepare_tool_side_effect_fence_blocking(
        &self,
        session_id: &str,
        run_id: &str,
        fence: &SideEffectFenceV1,
    ) -> Result<SideEffectRetryDecision, Status> {
        self.journal_store
            .prepare_side_effect_fence(session_id, run_id, fence)
            .map_err(|error| map_orchestrator_store_error("prepare tool side-effect fence", error))
    }

    /// Loads or durably records a mutating tool intent before dispatch.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn prepare_tool_side_effect_fence(
        self: &Arc<Self>,
        session_id: String,
        run_id: String,
        fence: SideEffectFenceV1,
    ) -> Result<SideEffectRetryDecision, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.prepare_tool_side_effect_fence_blocking(
                session_id.as_str(),
                run_id.as_str(),
                &fence,
            )
        })
        .await
        .map_err(|_| Status::internal("tool side-effect fence worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn transition_tool_side_effect_fence_blocking(
        &self,
        operation_id: &RuntimeOperationId,
        next: SideEffectFenceState,
        generation: palyra_common::runtime_contracts::RuntimeGeneration,
        reason_code: &str,
        evidence_sha256: Option<String>,
    ) -> Result<SideEffectFenceV1, Status> {
        self.journal_store
            .transition_side_effect_fence(
                operation_id.as_str(),
                next,
                generation,
                reason_code,
                evidence_sha256,
            )
            .map_err(|error| {
                map_orchestrator_store_error("transition tool side-effect fence", error)
            })
    }

    /// Advances a durable mutating-tool fence after intent admission.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn transition_tool_side_effect_fence(
        self: &Arc<Self>,
        operation_id: RuntimeOperationId,
        next: SideEffectFenceState,
        generation: palyra_common::runtime_contracts::RuntimeGeneration,
        reason_code: String,
        evidence_sha256: Option<String>,
    ) -> Result<SideEffectFenceV1, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.transition_tool_side_effect_fence_blocking(
                &operation_id,
                next,
                generation,
                reason_code.as_str(),
                evidence_sha256,
            )
        })
        .await
        .map_err(|_| Status::internal("tool side-effect fence transition worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn record_tool_side_effect_cleanup_outcome_blocking(
        &self,
        request: &SideEffectFenceCleanupOutcomeRequest,
    ) -> Result<SideEffectFenceV1, Status> {
        self.journal_store.record_side_effect_cleanup_outcome(request).map_err(|error| {
            map_orchestrator_store_error("record tool side-effect cleanup outcome", error)
        })
    }

    /// Records a late cleanup-owner observation for an already uncertain tool effect.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn record_tool_side_effect_cleanup_outcome(
        self: &Arc<Self>,
        request: SideEffectFenceCleanupOutcomeRequest,
    ) -> Result<SideEffectFenceV1, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.record_tool_side_effect_cleanup_outcome_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("tool side-effect cleanup outcome worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn resolve_tool_side_effect_fence_as_operator_blocking(
        &self,
        request: &SideEffectFenceOperatorResolutionRequest,
    ) -> Result<SideEffectFenceV1, Status> {
        self.journal_store.resolve_side_effect_fence_as_operator(request).map_err(|error| {
            map_orchestrator_store_error("resolve tool side-effect fence as operator", error)
        })
    }

    /// Closes one uncertain side-effect fence from authenticated operator evidence.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn resolve_tool_side_effect_fence_as_operator(
        self: &Arc<Self>,
        request: SideEffectFenceOperatorResolutionRequest,
    ) -> Result<SideEffectFenceV1, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.resolve_tool_side_effect_fence_as_operator_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("operator side-effect resolution worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    pub(crate) fn runtime_generation_for_run_blocking(
        &self,
        run_id: &str,
    ) -> Result<Option<(String, palyra_common::runtime_contracts::RuntimeGeneration)>, Status> {
        self.journal_store
            .active_runtime_generation_for_run(run_id, RuntimeGenerationLane::Run)
            .map(|lease| lease.map(|lease| (lease.session_id.into_inner(), lease.generation)))
            .map_err(|error| map_orchestrator_store_error("load run runtime generation", error))
    }

    /// Loads the active host generation for one orchestrator run.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn runtime_generation_for_run(
        self: &Arc<Self>,
        run_id: String,
    ) -> Result<Option<(String, palyra_common::runtime_contracts::RuntimeGeneration)>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.runtime_generation_for_run_blocking(run_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("run runtime generation worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn supersede_run_generation_for_steer_blocking(
        &self,
        session_id: &str,
        run_id: &str,
    ) -> Result<RuntimeGeneration, Status> {
        self.journal_store
            .supersede_run_runtime_generation(
                session_id,
                run_id,
                "runtime.generation.active_run_steered",
            )
            .map(|lease| lease.generation)
            .map_err(|error| match error {
                crate::journal::JournalError::InvalidArgument(message) => {
                    Status::failed_precondition(message)
                }
                other => map_orchestrator_store_error("supersede steered run generation", other),
            })
    }

    /// Atomically advances the durable run generation for accepted steering.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn supersede_run_generation_for_steer(
        self: &Arc<Self>,
        session_id: String,
        run_id: String,
    ) -> Result<RuntimeGeneration, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.supersede_run_generation_for_steer_blocking(session_id.as_str(), run_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("run generation supersession worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn persist_runtime_stale_event_diagnostic_blocking(
        &self,
        request: &RuntimeStaleEventDiagnosticRequest,
    ) -> Result<(), Status> {
        self.journal_store.record_runtime_stale_event_diagnostic(request).map_err(|error| {
            map_orchestrator_store_error("persist runtime stale-event diagnostic", error)
        })
    }

    /// Persists metadata-only stale evidence without blocking the async runtime.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn persist_runtime_stale_event_diagnostic(
        self: &Arc<Self>,
        request: RuntimeStaleEventDiagnosticRequest,
    ) -> Result<(), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.persist_runtime_stale_event_diagnostic_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("runtime stale-event diagnostic worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    pub(crate) fn persisted_runtime_generation_for_run_blocking(
        &self,
        run_id: &str,
    ) -> Result<Option<(String, RuntimeGeneration)>, Status> {
        self.journal_store
            .latest_persisted_runtime_generation_for_run(run_id, RuntimeGenerationLane::Run)
            .map(|generation| {
                generation.map(|(session_id, generation)| (session_id.into_inner(), generation))
            })
            .map_err(|error| {
                map_orchestrator_store_error("load persisted run runtime generation", error)
            })
    }

    /// Loads the latest persisted run generation for replay correlation.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn persisted_runtime_generation_for_run(
        self: &Arc<Self>,
        run_id: String,
    ) -> Result<Option<(String, RuntimeGeneration)>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.persisted_runtime_generation_for_run_blocking(run_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("persisted run runtime generation worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn persisted_runtime_event_for_tape_sequence_blocking(
        &self,
        run_id: &str,
        tape_sequence: i64,
    ) -> Result<Option<RuntimeEventEnvelopeV2>, Status> {
        self.journal_store.persisted_runtime_event_for_tape_sequence(run_id, tape_sequence).map_err(
            |error| map_orchestrator_store_error("load persisted tape runtime event", error),
        )
    }

    /// Loads the exact canonical V2 projection paired with one replay tape row.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn persisted_runtime_event_for_tape_sequence(
        self: &Arc<Self>,
        run_id: String,
        tape_sequence: i64,
    ) -> Result<Option<RuntimeEventEnvelopeV2>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.persisted_runtime_event_for_tape_sequence_blocking(run_id.as_str(), tape_sequence)
        })
        .await
        .map_err(|_| Status::internal("persisted tape runtime event worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    pub(crate) fn runtime_generation_for_tool_blocking(
        &self,
        run_id: &str,
    ) -> Result<Option<(String, palyra_common::runtime_contracts::RuntimeGeneration)>, Status> {
        self.runtime_generation_for_run_blocking(run_id)
    }

    /// Loads the active run generation used to authorize a tool side effect.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn runtime_generation_for_tool(
        self: &Arc<Self>,
        run_id: String,
    ) -> Result<Option<(String, palyra_common::runtime_contracts::RuntimeGeneration)>, Status> {
        self.runtime_generation_for_run(run_id).await
    }

    /// Parses stable fence identities derived from a normalized tool proposal.
    #[allow(clippy::result_large_err)]
    pub(crate) fn tool_side_effect_identities(
        proposal_id: &str,
    ) -> Result<(RuntimeOperationId, RuntimeToolExecutionId), Status> {
        let operation_id = RuntimeOperationId::parse(format!("tool:{proposal_id}").as_str())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let tool_execution_id =
            RuntimeToolExecutionId::parse(format!("tool:{proposal_id}").as_str())
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
        Ok((operation_id, tool_execution_id))
    }

    #[allow(clippy::result_large_err)]
    fn append_orchestrator_tape_event_blocking(
        &self,
        request: &OrchestratorTapeAppendRequest,
    ) -> Result<(), Status> {
        let active_generation = self
            .journal_store
            .active_runtime_generation_for_run(request.run_id.as_str(), RuntimeGenerationLane::Run)
            .map_err(|error| map_orchestrator_store_error("authorize tape generation", error))?
            .ok_or_else(|| {
                Status::aborted("orchestrator tape event rejected for inactive run generation")
            })?;
        let runtime_event = self
            .shared_runtime_tape_projection(request, &active_generation)
            .map_err(|error| map_orchestrator_store_error("build shared runtime event", error))?;
        let projected = self
            .journal_store
            .append_orchestrator_tape_event_with_runtime_projection(request, runtime_event.as_ref())
            .map_err(|error| {
                map_orchestrator_store_error("append orchestrator tape boundary", error)
            })?;
        if matches!(projected, Some(RuntimeEventAppendOutcome::StaleSuppressed)) {
            return Err(Status::aborted("runtime event rejected for stale or inactive generation"));
        }
        let record = crate::journal::OrchestratorTapeRecord {
            seq: request.seq,
            event_type: request.event_type.clone(),
            payload_json: request.payload_json.clone(),
        };
        match self
            .journal_store
            .append_projected_metadata_trace_event(request.run_id.as_str(), &record)
        {
            Ok(true) => {
                self.counters.metadata_trace_events.fetch_add(1, Ordering::Relaxed);
            }
            Ok(false) => {}
            Err(_) => {
                self.counters.metadata_trace_failures.fetch_add(1, Ordering::Relaxed);
                let run_id_sha256 = palyra_common::metadata_trace::metadata_trace_id_sha256(
                    palyra_common::metadata_trace::MetadataTraceIdDomainV1::Run,
                    request.run_id.as_str(),
                )
                .unwrap_or_else(|_| "invalid".to_owned());
                warn!(
                    run_id_sha256 = %run_id_sha256,
                    reason_code = "metadata_trace.projection_append_failed",
                    "metadata trace projection failed after durable tape append"
                );
            }
        }
        Ok(())
    }

    fn shared_runtime_tape_projection(
        &self,
        request: &OrchestratorTapeAppendRequest,
        active_generation: &palyra_common::runtime_contracts::GenerationLeaseV1,
    ) -> Result<Option<RuntimeEventAppendRequest>, JournalError> {
        let Some(event) = shared_runtime_event_for_tape(request) else {
            return Ok(None);
        };
        let descriptor = event.name.descriptor();
        if descriptor.generation_lane != RuntimeGenerationLane::Run {
            return Err(JournalError::InvalidArgument(format!(
                "legacy tape projection {} requires unsupported generation lane {}",
                event.name.as_str(),
                descriptor.generation_lane.as_str()
            )));
        }
        let (mut identities, legacy_identity_adapter) = RuntimeIdentitySetV1::from_legacy_run(
            active_generation.session_id.as_str(),
            request.run_id.as_str(),
            active_generation.generation,
        )
        .map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
        apply_shared_runtime_tape_identities(&mut identities, request)?;
        if event.name == RuntimeEventName::RunStarted {
            let run =
                self.journal_store.orchestrator_run_status_snapshot(request.run_id.as_str())?;
            if let Some(run) = run {
                if matches!(run.origin_kind.as_str(), "retry" | "cli_auto_resume") {
                    if let Some(origin_run_id) = run.origin_run_id.as_deref() {
                        identities.causal_links.push(RuntimeCausalLink {
                            relation: RuntimeCausalLinkKind::RecoveredFrom,
                            source: RuntimeIdentityRef::new(
                                RuntimeIdentityKind::Run,
                                request.run_id.as_str(),
                            )
                            .map_err(|error| JournalError::InvalidArgument(error.to_string()))?,
                            target: RuntimeIdentityRef::new(
                                RuntimeIdentityKind::Run,
                                origin_run_id,
                            )
                            .map_err(|error| JournalError::InvalidArgument(error.to_string()))?,
                        });
                    }
                }
            }
        }
        let source_sequence = u64::try_from(request.seq).map_err(|_| {
            JournalError::InvalidArgument(
                "runtime event source sequence must be a non-negative sqlite integer".to_owned(),
            )
        })?;
        let event_id = RuntimeEventId::parse(
            format!("run_stream:{}:{source_sequence}", request.run_id).as_str(),
        )
        .map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
        let payload = if event.name == RuntimeEventName::BackpressureApplied {
            let metadata =
                serde_json::from_str::<Value>(request.payload_json.as_str()).map_err(|error| {
                    JournalError::InvalidArgument(format!(
                        "runtime backpressure tape payload is invalid JSON: {error}"
                    ))
                })?;
            RuntimeEventPayloadRef::Inline { metadata }
        } else {
            RuntimeEventPayloadRef::Omitted {
                reason_code: "runtime.event.legacy_tape_payload_omitted".to_owned(),
                digest_sha256: None,
                size_bytes: u64::try_from(request.payload_json.len()).unwrap_or(u64::MAX),
            }
        };
        let mut envelope = RuntimeEventEnvelopeV2 {
            schema_version: 2,
            event_id,
            identities,
            sequence: 0,
            causal_parent_event_id: None,
            subsystem: descriptor.subsystem,
            phase: descriptor.phase,
            event_name: event.name,
            reason_code: event.reason_code.to_owned(),
            actor_kind: descriptor.actor_kind,
            retryability: descriptor.retryability,
            redaction_class: descriptor.redaction_class,
            terminal: descriptor.terminal,
            payload,
            occurred_at_unix_ms: current_unix_ms(),
            extensions: std::collections::BTreeMap::new(),
        };
        if let Some(legacy_identity_adapter) =
            legacy_identity_adapter.reconcile_with_identities(&envelope.identities)
        {
            envelope
                .record_legacy_identity_adapter(legacy_identity_adapter)
                .map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
        }
        if !envelope.identities.causal_links.is_empty() {
            envelope.extensions.insert(
                "runtime_identity_diagnostics_v1".to_owned(),
                serde_json::to_value(envelope.identities.redacted_diagnostics())?,
            );
        }
        Ok(Some(RuntimeEventAppendRequest { lane: descriptor.generation_lane, envelope }))
    }

    #[allow(clippy::result_large_err)]
    fn commit_tool_effect_observation_blocking(
        &self,
        request: &ToolEffectObservationCommitRequest,
    ) -> Result<SideEffectFenceV1, Status> {
        let active_generation = self
            .journal_store
            .active_runtime_generation_for_run(
                request.tape_events.first().map_or("", |event| event.run_id.as_str()),
                RuntimeGenerationLane::Run,
            )
            .map_err(|error| {
                map_orchestrator_store_error("authorize tool result generation", error)
            })?
            .ok_or_else(|| Status::aborted("tool result rejected for inactive run generation"))?;
        if active_generation.generation != request.generation {
            return Err(Status::aborted("tool result rejected for stale run generation"));
        }
        let runtime_events = request
            .tape_events
            .iter()
            .map(|event| self.shared_runtime_tape_projection(event, &active_generation))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| {
                map_orchestrator_store_error("build tool result runtime events", error)
            })?;
        let outcome = self
            .journal_store
            .commit_tool_effect_observation_with_runtime_projection_outcome(
                request,
                runtime_events.as_slice(),
            )
            .map_err(|error| {
                map_orchestrator_store_error("commit tool result and side effect", error)
            })?;
        if outcome.metadata_trace_events_appended > 0 {
            self.counters
                .metadata_trace_events
                .fetch_add(outcome.metadata_trace_events_appended, Ordering::Relaxed);
        }
        Ok(outcome.fence)
    }

    /// Atomically persists canonical tool-result evidence and its observed effect fence.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn commit_tool_effect_observation(
        self: &Arc<Self>,
        request: ToolEffectObservationCommitRequest,
    ) -> Result<SideEffectFenceV1, Status> {
        let tape_event_count = request.tape_events.len();
        let state = Arc::clone(self);
        let fence = tokio::task::spawn_blocking(move || {
            state.commit_tool_effect_observation_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("tool effect observation worker panicked"))??;
        self.counters
            .orchestrator_tape_events
            .fetch_add(u64::try_from(tape_event_count).unwrap_or(u64::MAX), Ordering::Relaxed);
        Ok(fence)
    }

    #[allow(clippy::result_large_err)]
    fn upsert_progress_draft_from_tape_event_blocking(
        &self,
        request: &ProgressDraftTapeEventRequest,
    ) -> Result<ProgressDraftRecord, Status> {
        self.journal_store
            .upsert_progress_draft_from_tape_event(request)
            .map_err(|error| map_orchestrator_store_error("upsert progress draft", error))
    }

    #[allow(clippy::result_large_err)]
    fn list_progress_drafts_blocking(
        &self,
        filter: &ProgressDraftListFilter,
    ) -> Result<Vec<ProgressDraftRecord>, Status> {
        self.journal_store
            .list_progress_drafts(filter)
            .map_err(|error| map_orchestrator_store_error("list progress drafts", error))
    }

    /// Lists progress drafts matching the filter.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_progress_drafts(
        self: &Arc<Self>,
        filter: ProgressDraftListFilter,
    ) -> Result<Vec<ProgressDraftRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.list_progress_drafts_blocking(&filter))
            .await
            .map_err(|_| Status::internal("progress draft list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_progress_draft_events_blocking(
        &self,
        draft_id: &str,
        limit: usize,
    ) -> Result<Vec<ProgressDraftEventRecord>, Status> {
        self.journal_store
            .list_progress_draft_events(draft_id, limit)
            .map_err(|error| map_orchestrator_store_error("list progress draft events", error))
    }

    /// Lists append-only progress draft audit events.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_progress_draft_events(
        self: &Arc<Self>,
        draft_id: String,
        limit: usize,
    ) -> Result<Vec<ProgressDraftEventRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.list_progress_draft_events_blocking(draft_id.as_str(), limit)
        })
        .await
        .map_err(|_| Status::internal("progress draft event list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn append_turn_control_event_blocking(
        &self,
        request: &TurnControlAuditEventAppendRequest,
    ) -> Result<TurnControlAuditEventRecord, Status> {
        self.journal_store
            .append_turn_control_event(request)
            .map_err(|error| map_orchestrator_store_error("append turn control audit event", error))
    }

    #[allow(clippy::result_large_err)]
    fn list_turn_control_events_blocking(
        &self,
        filter: &TurnControlAuditEventListFilter,
    ) -> Result<Vec<TurnControlAuditEventRecord>, Status> {
        self.journal_store
            .list_turn_control_events(filter)
            .map_err(|error| map_orchestrator_store_error("list turn control audit events", error))
    }

    /// Lists recent turn-control audit events.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_turn_control_events(
        self: &Arc<Self>,
        filter: TurnControlAuditEventListFilter,
    ) -> Result<Vec<TurnControlAuditEventRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.list_turn_control_events_blocking(&filter))
            .await
            .map_err(|_| Status::internal("turn control audit list worker panicked"))?
    }

    /// Applies a basic turn-control operation through the existing runtime surfaces.
    ///
    /// # Errors
    /// Returns the mapped journal/runtime error. Audit `started` is fail-closed
    /// for accepted mutating operations so a control action never happens
    /// without a durable decision record.
    #[allow(clippy::result_large_err)]
    pub async fn apply_turn_control(
        self: &Arc<Self>,
        request: TurnControlRequest,
    ) -> Result<TurnControlApplyOutcome, Status> {
        let decision = decide_turn_control_request(&request);
        if !decision.accepted {
            self.record_turn_control_decision_event(
                &request,
                &decision,
                decision.journal_projection.terminal_event_type.as_str(),
                "failed",
            )
            .await?;
            return Ok(TurnControlApplyOutcome { decision, effect: json!({"accepted": false}) });
        }

        self.record_turn_control_decision_event(
            &request,
            &decision,
            decision.journal_projection.started_event_type.as_str(),
            "started",
        )
        .await?;
        let effect = if request.dry_run {
            json!({"dry_run": true})
        } else {
            self.apply_turn_control_effect(&request, &decision).await?
        };
        self.record_turn_control_decision_event(
            &request,
            &decision,
            decision.journal_projection.terminal_event_type.as_str(),
            "completed",
        )
        .await?;
        Ok(TurnControlApplyOutcome { decision, effect })
    }

    #[allow(clippy::result_large_err)]
    async fn apply_turn_control_effect(
        self: &Arc<Self>,
        request: &TurnControlRequest,
        decision: &TurnControlDecision,
    ) -> Result<Value, Status> {
        match decision.action {
            TurnControlAction::Observe => self.turn_control_status_effect(request).await,
            TurnControlAction::RequestRunCancel => {
                let run_id = decision.target_id.as_deref().ok_or_else(|| {
                    Status::invalid_argument("turn control cancel decision missing run id")
                })?;
                let cancel = self
                    .request_orchestrator_cancel(OrchestratorCancelRequest {
                        run_id: run_id.to_owned(),
                        reason: request
                            .reason
                            .clone()
                            .unwrap_or_else(|| decision.reason_code.clone()),
                    })
                    .await?;
                serde_json::to_value(cancel).map_err(|error| {
                    Status::internal(format!(
                        "failed to serialize turn control cancel effect: {error}"
                    ))
                })
            }
            TurnControlAction::EnqueueRedirect => {
                self.turn_control_redirect_effect(request, decision).await
            }
            TurnControlAction::SetQueuePaused => {
                let session_id = decision.target_id.as_deref().ok_or_else(|| {
                    Status::invalid_argument("turn control queue decision missing session id")
                })?;
                let paused = request.operation == TurnControlOperation::PauseQueue;
                let control = self
                    .upsert_orchestrator_session_queue_control(
                        OrchestratorSessionQueueControlUpdateRequest {
                            session_id: session_id.to_owned(),
                            paused,
                            pause_reason: paused.then(|| {
                                request
                                    .reason
                                    .clone()
                                    .unwrap_or_else(|| decision.reason_code.clone())
                            }),
                        },
                    )
                    .await?;
                serde_json::to_value(control).map_err(|error| {
                    Status::internal(format!(
                        "failed to serialize turn control queue effect: {error}"
                    ))
                })
            }
            TurnControlAction::SetQueuedInputPriority => {
                let queued_input_id = decision.target_id.as_deref().ok_or_else(|| {
                    Status::invalid_argument(
                        "turn control priority decision missing queued input id",
                    )
                })?;
                let priority_lane = request
                    .priority_lane
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| {
                        Status::invalid_argument("turn control priority missing priority lane")
                    })?;
                if let Some(session_id) =
                    request.session_id.as_deref().map(str::trim).filter(|value| !value.is_empty())
                {
                    let steering_decision = self
                        .steer_orchestrator_queued_input(
                            session_id.to_owned(),
                            queued_input_id.to_owned(),
                            QueueSteeringRequest {
                                actor_principal: decision.actor_principal.clone(),
                                requested_priority_lane: priority_lane.to_owned(),
                                reason: request.reason.clone(),
                            },
                        )
                        .await?;
                    return serde_json::to_value(steering_decision).map_err(|error| {
                        Status::internal(format!(
                            "failed to serialize queue steering effect: {error}"
                        ))
                    });
                }
                self.prioritize_orchestrator_queued_input(
                    queued_input_id.to_owned(),
                    None,
                    priority_lane.to_owned(),
                    decision.reason_code.clone(),
                    decision.journal_projection.payload_json.clone(),
                )
                .await?;
                Ok(json!({
                    "queued_input_id": queued_input_id,
                    "priority_lane": priority_lane,
                    "prioritized": true,
                }))
            }
            TurnControlAction::Yield => Ok(json!({
                "yielded": true,
                "active_phase": decision.active_phase.as_str(),
                "target_kind": decision.target_kind.as_str(),
                "target_id": decision.target_id.as_deref(),
            })),
            TurnControlAction::Reject => Ok(json!({"accepted": false})),
        }
    }

    #[allow(clippy::result_large_err)]
    async fn turn_control_redirect_effect(
        self: &Arc<Self>,
        request: &TurnControlRequest,
        decision: &TurnControlDecision,
    ) -> Result<Value, Status> {
        let run_id = decision.target_id.as_deref().ok_or_else(|| {
            Status::invalid_argument("turn control redirect decision missing run id")
        })?;
        let instruction = request
            .instruction
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| Status::invalid_argument("turn control redirect missing instruction"))?;
        let snapshot = self
            .orchestrator_run_status_snapshot(run_id.to_owned())
            .await?
            .ok_or_else(|| Status::not_found(format!("orchestrator run not found: {run_id}")))?;
        let session_id = if let Some(request_session_id) =
            request.session_id.as_deref().map(str::trim).filter(|value| !value.is_empty())
        {
            if request_session_id != snapshot.session_id {
                return Err(Status::invalid_argument(format!(
                    "turn control redirect session_id {} does not match run {} session {}",
                    request_session_id, run_id, snapshot.session_id
                )));
            }
            request_session_id.to_owned()
        } else {
            snapshot.session_id.clone()
        };
        let queued_inputs = self.list_orchestrator_queued_inputs(session_id.clone()).await?;
        let policy = SessionQueuePolicy::from_config(
            &self.config.session_queue_policy,
            &session_id,
            None,
            None,
        );
        let current_depth =
            pending_queue_depth(queued_inputs.as_slice(), Some(policy.coalescing_group.as_str()));
        let safe_boundary = session_queue_boundary_for_control(decision.active_phase);
        let queue_decision = decide_session_queue_mode(
            policy,
            Some(QueueMode::Interrupt),
            safe_boundary,
            current_depth,
        );
        let (_, active_generation) =
            self.runtime_generation_for_run(run_id.to_owned()).await?.ok_or_else(|| {
                Status::failed_precondition(
                    "turn control redirect requires an active runtime generation",
                )
            })?;
        let expected_active_generation = i64::try_from(active_generation.get()).map_err(|_| {
            Status::failed_precondition("runtime generation exceeds the journal integer range")
        })?;
        let queued_input_id = Ulid::new().to_string();
        let queued_state = if !queue_decision.accepted {
            QueuedInputState::Overflowed
        } else if queue_decision.decision == QueueDecision::Defer {
            QueuedInputState::Deferred
        } else {
            QueuedInputState::Pending
        };
        let queue_outcome = queue_outcome(
            queued_input_id.clone(),
            queued_state,
            queue_decision.delivery_boundary,
            Some(active_generation.get()),
            Some(active_generation.get()),
            queue_decision.accepted,
            queue_decision.reason.clone(),
        );
        let queued = self
            .create_orchestrator_queued_input(OrchestratorQueuedInputCreateRequest {
                queued_input_id: queued_input_id.clone(),
                run_id: run_id.to_owned(),
                session_id: session_id.clone(),
                state: queued_state.as_str().to_owned(),
                text: instruction.to_owned(),
                origin_run_id: Some(run_id.to_owned()),
                queue_mode: queue_decision.mode.as_str().to_owned(),
                delivery_boundary: queue_decision.delivery_boundary.as_str().to_owned(),
                expected_active_generation: Some(expected_active_generation),
                priority_lane: queue_decision.policy.priority_lane.clone(),
                coalescing_group: Some(queue_decision.policy.coalescing_group.clone()),
                overflow_summary_ref: None,
                safe_boundary_flags_json: serde_json::to_string(&queue_decision.safe_boundary)
                    .unwrap_or_else(|_| "{}".to_owned()),
                decision_reason: queue_decision.reason.clone(),
                attachments_json: "[]".to_owned(),
                queue_outcome_json: serde_json::to_string(&queue_outcome)
                    .map_err(|error| Status::internal(error.to_string()))?,
                accepted_at_unix_ms: queue_decision
                    .accepted
                    .then_some(crate::gateway::current_unix_ms()),
                policy_snapshot_json: queue_decision.policy.snapshot_json().to_string(),
                explain_json: queue_decision.explain_json().to_string(),
            })
            .await?;
        let queue_event_type = match queue_decision.decision {
            QueueDecision::Interrupt => RuntimeDecisionEventType::QueueInterrupt,
            QueueDecision::Steer | QueueDecision::SteerBacklog => {
                RuntimeDecisionEventType::QueueSteer
            }
            QueueDecision::Merge => RuntimeDecisionEventType::QueueMerge,
            QueueDecision::Overflow => RuntimeDecisionEventType::QueueOverflow,
            QueueDecision::Enqueue | QueueDecision::Defer => RuntimeDecisionEventType::QueueEnqueue,
        };
        let runtime_decision = RuntimeDecisionPayload::new(
            queue_event_type,
            RuntimeDecisionActor::new(
                RuntimeDecisionActorKind::Operator,
                decision.actor_principal.clone(),
                "control-plane",
                None,
            ),
            decision.reason_code.clone(),
            queue_decision.policy.policy_id.clone(),
            RuntimeDecisionTiming::observed(crate::gateway::current_unix_ms()),
        )
        .with_input(
            RuntimeEntityRef::new("queued_input", "queued_input", queued.queued_input_id.clone())
                .with_state(queued.state.as_str()),
        )
        .with_output(RuntimeEntityRef::new("run", "run", run_id.to_owned()).with_state("active"))
        .with_resource_budget(RuntimeResourceBudget {
            queue_depth: Some(
                current_depth.saturating_add(usize::from(queue_decision.accepted)) as u64
            ),
            token_budget: None,
            pruning_token_delta: None,
            retrieval_branch_latency_ms: None,
            retry_count: None,
            suppression_count: None,
        })
        .with_related_entity(RuntimeEntityRef::new("session", "session", session_id.clone()))
        .with_details(json!({
            "control_command": decision.command.as_str(),
            "active_phase": decision.active_phase.as_str(),
            "safety_boundary": &decision.safety_boundary,
            "queue_decision": queue_decision.decision.as_str(),
            "queue_mode": queue_decision.mode.as_str(),
            "redirect_preserves_existing_state": true,
        }));
        self.record_system_runtime_decision_event(
            decision.actor_principal.as_str(),
            "control-plane",
            None,
            Some(session_id.as_str()),
            Some(run_id),
            runtime_decision.clone(),
        )
        .await?;
        let mut tape_seq = snapshot.tape_events as i64;
        crate::application::run_stream::tape::append_runtime_decision_tape_event(
            self,
            run_id,
            &mut tape_seq,
            &runtime_decision,
        )
        .await?;
        Ok(json!({
            "redirect_queued": queue_decision.accepted,
            "queued_input": queued,
            "decision": queue_decision.explain_json(),
            "preserved_state": {
                "artifacts": "append_only",
                "checkpoints": "unchanged",
                "partial_evidence": "unchanged",
            },
        }))
    }

    /// Admits an input into the journal-backed session queue for an active run.
    ///
    /// # Errors
    /// Returns `not_found` when the target run is absent, `invalid_argument`
    /// when the run does not belong to the supplied session, or the mapped
    /// journal/runtime error when queue persistence or audit recording fails.
    #[allow(clippy::result_large_err)]
    pub async fn admit_session_queued_input(
        self: &Arc<Self>,
        request: SessionQueueAdmissionRequest,
    ) -> Result<SessionQueueAdmissionOutcome, Status> {
        let snapshot =
            self.orchestrator_run_status_snapshot(request.run_id.clone()).await?.ok_or_else(
                || Status::not_found(format!("orchestrator run not found: {}", request.run_id)),
            )?;
        if snapshot.session_id != request.session_id {
            return Err(Status::invalid_argument(format!(
                "queued input session_id {} does not match run {} session {}",
                request.session_id, request.run_id, snapshot.session_id
            )));
        }

        let queued_inputs =
            self.list_orchestrator_queued_inputs(request.session_id.clone()).await?;
        let policy = SessionQueuePolicy::from_config(
            &self.config.session_queue_policy,
            request.session_id.as_str(),
            request.policy_channel.as_deref(),
            request.policy_agent_id.as_deref(),
        );
        let current_depth =
            pending_queue_depth(queued_inputs.as_slice(), Some(policy.coalescing_group.as_str()));
        let decision = decide_session_queue_mode(
            policy,
            request.requested_mode,
            request.safe_boundary.clone(),
            current_depth,
        );
        let (_, active_generation) =
            self.runtime_generation_for_run(request.run_id.clone()).await?.ok_or_else(|| {
                Status::failed_precondition(
                    "queued input admission requires an active runtime generation",
                )
            })?;
        let expected_active_generation = i64::try_from(active_generation.get()).map_err(|_| {
            Status::failed_precondition("runtime generation exceeds the journal integer range")
        })?;
        let timestamp_unix_ms = current_unix_ms();
        let queued_input_id = request.queued_input_id.unwrap_or_else(|| Ulid::new().to_string());
        let queued_state = if !decision.accepted {
            QueuedInputState::Overflowed
        } else if decision.decision == QueueDecision::Defer {
            QueuedInputState::Deferred
        } else {
            QueuedInputState::Pending
        };
        let queue_outcome = queue_outcome(
            queued_input_id.clone(),
            queued_state,
            decision.delivery_boundary,
            Some(active_generation.get()),
            Some(active_generation.get()),
            decision.accepted,
            decision.reason.clone(),
        );
        let queued_input = self
            .create_orchestrator_queued_input(OrchestratorQueuedInputCreateRequest {
                queued_input_id: queued_input_id.clone(),
                run_id: request.run_id.clone(),
                session_id: request.session_id.clone(),
                state: queued_state.as_str().to_owned(),
                text: request.text,
                origin_run_id: request.origin_run_id.or_else(|| Some(request.run_id.clone())),
                queue_mode: decision.mode.as_str().to_owned(),
                delivery_boundary: decision.delivery_boundary.as_str().to_owned(),
                expected_active_generation: Some(expected_active_generation),
                priority_lane: decision.policy.priority_lane.clone(),
                coalescing_group: Some(decision.policy.coalescing_group.clone()),
                overflow_summary_ref: None,
                safe_boundary_flags_json: serde_json::to_string(&decision.safe_boundary)
                    .unwrap_or_else(|_| "{}".to_owned()),
                decision_reason: decision.reason.clone(),
                attachments_json: "[]".to_owned(),
                queue_outcome_json: serde_json::to_string(&queue_outcome)
                    .map_err(|error| Status::internal(error.to_string()))?,
                accepted_at_unix_ms: decision.accepted.then_some(timestamp_unix_ms),
                policy_snapshot_json: decision.policy.snapshot_json().to_string(),
                explain_json: decision.explain_json().to_string(),
            })
            .await?;

        let observed_queue_depth =
            if decision.accepted { current_depth.saturating_add(1) } else { current_depth } as u64;
        let queue_event_type = match decision.decision {
            QueueDecision::Interrupt => RuntimeDecisionEventType::QueueInterrupt,
            QueueDecision::Steer | QueueDecision::SteerBacklog => {
                RuntimeDecisionEventType::QueueSteer
            }
            QueueDecision::Merge => RuntimeDecisionEventType::QueueMerge,
            QueueDecision::Overflow => RuntimeDecisionEventType::QueueOverflow,
            QueueDecision::Enqueue | QueueDecision::Defer => RuntimeDecisionEventType::QueueEnqueue,
        };
        let runtime_decision = RuntimeDecisionPayload::new(
            queue_event_type,
            RuntimeDecisionActor::new(
                RuntimeDecisionActorKind::Operator,
                request.actor_principal.clone(),
                request.actor_device_id.clone(),
                request.actor_channel.clone(),
            ),
            decision.reason.clone(),
            decision.policy.policy_id.clone(),
            RuntimeDecisionTiming::observed(timestamp_unix_ms),
        )
        .with_input(
            RuntimeEntityRef::new(
                "queued_input",
                "queued_input",
                queued_input.queued_input_id.clone(),
            )
            .with_state(queued_input.state.as_str()),
        )
        .with_output(
            RuntimeEntityRef::new("run", "run", request.run_id.clone())
                .with_state(snapshot.state.as_str()),
        )
        .with_resource_budget(RuntimeResourceBudget {
            queue_depth: Some(observed_queue_depth),
            token_budget: None,
            pruning_token_delta: None,
            retrieval_branch_latency_ms: None,
            retry_count: None,
            suppression_count: None,
        })
        .with_related_entity(RuntimeEntityRef::new(
            "session",
            "session",
            request.session_id.clone(),
        ))
        .with_details(json!({
            "source": request.source,
            "decision": decision.decision.as_str(),
            "queue_mode": decision.mode.as_str(),
            "safe_boundary": decision.safe_boundary,
            "policy": decision.policy.snapshot_json(),
        }));
        self.record_system_runtime_decision_event(
            request.actor_principal.as_str(),
            request.actor_device_id.as_str(),
            request.actor_channel.as_deref(),
            Some(request.session_id.as_str()),
            Some(request.run_id.as_str()),
            runtime_decision.clone(),
        )
        .await?;
        self.observability.observe_runtime_queue_depth(observed_queue_depth);
        let mut tape_seq = snapshot.tape_events as i64;
        crate::application::run_stream::tape::append_runtime_decision_tape_event(
            self,
            request.run_id.as_str(),
            &mut tape_seq,
            &runtime_decision,
        )
        .await?;

        Ok(SessionQueueAdmissionOutcome { queued_input, decision, observed_queue_depth })
    }

    #[allow(clippy::result_large_err)]
    async fn turn_control_status_effect(
        self: &Arc<Self>,
        request: &TurnControlRequest,
    ) -> Result<Value, Status> {
        if let Some(run_id) =
            request.run_id.as_deref().map(str::trim).filter(|value| !value.is_empty())
        {
            let snapshot = self.orchestrator_run_status_snapshot(run_id.to_owned()).await?;
            return serde_json::to_value(snapshot).map_err(|error| {
                Status::internal(format!("failed to serialize turn control run status: {error}"))
            });
        }
        if let Some(session_id) =
            request.session_id.as_deref().map(str::trim).filter(|value| !value.is_empty())
        {
            let queue_control =
                self.get_orchestrator_session_queue_control(session_id.to_owned()).await?;
            return serde_json::to_value(queue_control).map_err(|error| {
                Status::internal(format!("failed to serialize turn control queue status: {error}"))
            });
        }
        Ok(json!({"status": "ok"}))
    }

    #[allow(clippy::result_large_err)]
    async fn record_turn_control_decision_event(
        self: &Arc<Self>,
        request: &TurnControlRequest,
        decision: &TurnControlDecision,
        event_type: &str,
        outcome: &str,
    ) -> Result<TurnControlAuditEventRecord, Status> {
        let append_request = TurnControlAuditEventAppendRequest {
            event_id: ulid::Ulid::new().to_string(),
            event_type: event_type.to_owned(),
            operation: decision.operation.as_str().to_owned(),
            actor_principal: decision.actor_principal.clone(),
            target_kind: decision.target_kind.clone(),
            target_id: decision.target_id.clone(),
            session_id: request.session_id.clone(),
            run_id: request.run_id.clone(),
            outcome: outcome.to_owned(),
            reason_code: decision.reason_code.clone(),
            payload_json: decision.journal_projection.payload_json.clone(),
            evidence_refs_json: decision.journal_projection.evidence_refs_json.clone(),
            redaction_level: decision.journal_projection.redaction_level.clone(),
        };
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.append_turn_control_event_blocking(&append_request)
        })
        .await
        .map_err(|_| Status::internal("turn control audit append worker panicked"))?
    }

    /// Applies a queued-input steering request through the journal-backed audit path.
    ///
    /// # Errors
    /// Returns `not_found` if the queued input does not belong to the session,
    /// or the mapped journal/runtime error when audit or priority updates fail.
    #[allow(clippy::result_large_err)]
    pub async fn steer_orchestrator_queued_input(
        self: &Arc<Self>,
        session_id: String,
        queued_input_id: String,
        request: QueueSteeringRequest,
    ) -> Result<QueueSteeringDecision, Status> {
        let queued_inputs = self.list_orchestrator_queued_inputs(session_id.clone()).await?;
        let queued = queued_inputs
            .into_iter()
            .find(|queued| queued.queued_input_id.as_str() == queued_input_id.as_str())
            .ok_or_else(|| {
                Status::not_found(format!(
                    "queued input not found in session {session_id}: {queued_input_id}"
                ))
            })?;
        let decision = decide_queue_steering(&queued, &request);
        self.record_queue_steering_event(
            &request,
            &decision,
            QUEUE_STEERING_EVENT_STARTED,
            "started",
        )
        .await?;
        if !decision.accepted {
            self.record_queue_steering_event(
                &request,
                &decision,
                QUEUE_STEERING_EVENT_FAILED,
                "failed",
            )
            .await?;
            return Ok(decision);
        }
        match decision.action {
            QueueSteeringAction::SetPriorityLane => {
                let priority_lane = decision.to_priority_lane.as_ref().ok_or_else(|| {
                    Status::internal("accepted queue steering decision missing priority lane")
                })?;
                self.prioritize_orchestrator_queued_input(
                    decision.queued_input_id.clone(),
                    Some(queued.lifecycle_revision),
                    priority_lane.clone(),
                    decision.reason_code.clone(),
                    decision.payload_json.clone(),
                )
                .await?;
            }
            QueueSteeringAction::Noop => {}
            QueueSteeringAction::Reject => {
                return Err(Status::internal(
                    "accepted queue steering decision cannot select reject action",
                ));
            }
        }
        self.record_queue_steering_event(
            &request,
            &decision,
            QUEUE_STEERING_EVENT_COMPLETED,
            "completed",
        )
        .await?;
        Ok(decision)
    }

    #[allow(clippy::result_large_err)]
    async fn record_queue_steering_event(
        self: &Arc<Self>,
        request: &QueueSteeringRequest,
        decision: &QueueSteeringDecision,
        event_type: &str,
        outcome: &str,
    ) -> Result<TurnControlAuditEventRecord, Status> {
        let actor_principal = request.actor_principal.trim();
        let actor_principal = if actor_principal.is_empty() {
            "unknown".to_owned()
        } else {
            actor_principal.to_owned()
        };
        let append_request = TurnControlAuditEventAppendRequest {
            event_id: ulid::Ulid::new().to_string(),
            event_type: event_type.to_owned(),
            operation: "queue_steering".to_owned(),
            actor_principal,
            target_kind: "queued_input".to_owned(),
            target_id: Some(decision.queued_input_id.clone()),
            session_id: Some(decision.session_id.clone()),
            run_id: Some(decision.run_id.clone()),
            outcome: outcome.to_owned(),
            reason_code: decision.reason_code.clone(),
            payload_json: decision.payload_json.clone(),
            evidence_refs_json: decision.evidence_refs_json.clone(),
            redaction_level: decision.redaction_level.clone(),
        };
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.append_turn_control_event_blocking(&append_request)
        })
        .await
        .map_err(|_| Status::internal("queue steering audit append worker panicked"))?
    }

    /// Appends one tape event for a run and bumps the tape counter.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn append_orchestrator_tape_event(
        self: &Arc<Self>,
        request: OrchestratorTapeAppendRequest,
    ) -> Result<(), Status> {
        let progress_request = self
            .config
            .feature_rollouts
            .progress_drafts
            .enabled
            .then(|| project_progress_draft_tape_event(&request))
            .flatten();
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.append_orchestrator_tape_event_blocking(&request)?;
            if let Some(progress_request) = progress_request.as_ref() {
                if let Err(error) =
                    state.upsert_progress_draft_from_tape_event_blocking(progress_request)
                {
                    warn!(
                        run_id = %progress_request.run_id,
                        source_tape_seq = progress_request.source_tape_seq,
                        error = %error,
                        "progress draft update failed after tape append"
                    );
                }
            }
            Ok::<(), Status>(())
        })
        .await
        .map_err(|_| Status::internal("orchestrator tape worker panicked"))??;
        self.counters.orchestrator_tape_events.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    fn request_orchestrator_cancel_blocking(
        &self,
        request: &OrchestratorCancelRequest,
    ) -> Result<crate::journal::OrchestratorCancelSnapshot, Status> {
        self.journal_store
            .request_orchestrator_cancel(request)
            .map_err(|error| map_orchestrator_store_error("request orchestrator cancel", error))
    }

    /// Persists cancellation intent and bumps the cancel counter.
    /// In-flight work observes the flag and owns terminal settlement.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn request_orchestrator_cancel(
        self: &Arc<Self>,
        request: OrchestratorCancelRequest,
    ) -> Result<RunCancelSnapshot, Status> {
        let state = Arc::clone(self);
        let snapshot = tokio::task::spawn_blocking(move || {
            state.request_orchestrator_cancel_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("orchestrator cancel worker panicked"))??;
        self.counters.orchestrator_cancel_requests.fetch_add(1, Ordering::Relaxed);
        Ok(RunCancelSnapshot {
            run_id: snapshot.run_id,
            state: snapshot.state,
            cancel_requested: snapshot.cancel_requested,
            reason: snapshot.reason,
            cleanup_warning: None,
        })
    }

    #[allow(clippy::result_large_err)]
    fn is_orchestrator_cancel_requested_blocking(&self, run_id: &str) -> Result<bool, Status> {
        self.journal_store
            .is_orchestrator_cancel_requested(run_id)
            .map_err(|error| map_orchestrator_store_error("load orchestrator cancel flag", error))
    }

    /// Reads the cancel-requested flag for a run.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn is_orchestrator_cancel_requested(
        self: &Arc<Self>,
        run_id: String,
    ) -> Result<bool, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.is_orchestrator_cancel_requested_blocking(run_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("orchestrator cancel read worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn orchestrator_run_status_snapshot_blocking(
        &self,
        run_id: &str,
    ) -> Result<Option<OrchestratorRunStatusSnapshot>, Status> {
        self.journal_store
            .orchestrator_run_status_snapshot(run_id)
            .map_err(|error| map_orchestrator_store_error("load orchestrator run snapshot", error))
    }

    /// Loads the current run status snapshot, if the run exists.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn orchestrator_run_status_snapshot(
        self: &Arc<Self>,
        run_id: String,
    ) -> Result<Option<OrchestratorRunStatusSnapshot>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.orchestrator_run_status_snapshot_blocking(run_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("orchestrator snapshot worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_orchestrator_run_status_snapshots_blocking(
        &self,
        run_ids: &[String],
    ) -> Result<Vec<OrchestratorRunStatusSnapshot>, Status> {
        self.journal_store
            .list_orchestrator_run_status_snapshots(run_ids)
            .map_err(|error| map_orchestrator_store_error("list orchestrator run snapshots", error))
    }

    /// Loads current run status snapshots for the supplied run ids.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_orchestrator_run_status_snapshots(
        self: &Arc<Self>,
        run_ids: Vec<String>,
    ) -> Result<Vec<OrchestratorRunStatusSnapshot>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.list_orchestrator_run_status_snapshots_blocking(run_ids.as_slice())
        })
        .await
        .map_err(|_| Status::internal("orchestrator snapshot list worker panicked"))?
    }

    // Diagnostics accept either an orchestrator run id or a cron run id;
    // cron runs are followed to the orchestrator run they spawned.
    #[allow(clippy::result_large_err)]
    fn resolve_orchestrator_diagnostics_run_id_blocking(
        &self,
        run_id: &str,
    ) -> Result<Option<String>, Status> {
        let run_exists = self
            .journal_store
            .orchestrator_run_status_snapshot(run_id)
            .map_err(|error| map_orchestrator_store_error("load orchestrator run snapshot", error))?
            .is_some();
        if run_exists {
            return Ok(Some(run_id.to_owned()));
        }

        let cron_run = self
            .journal_store
            .cron_run(run_id)
            .map_err(|error| map_cron_store_error("load cron run", error))?;
        Ok(cron_run.and_then(|run| run.orchestrator_run_id))
    }

    /// Maps an operator-supplied id to an orchestrator run id, following cron run linkage when
    /// needed.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn resolve_orchestrator_diagnostics_run_id(
        self: &Arc<Self>,
        run_id: String,
    ) -> Result<Option<String>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.resolve_orchestrator_diagnostics_run_id_blocking(run_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("orchestrator diagnostics run-id resolver worker panicked"))?
    }

    /// Waits until the run reaches a terminal phase (or also a waiting phase
    /// when `return_on_waiting` is set), up to `request.timeout`.
    ///
    /// # Errors
    /// `not_found` for unknown runs, `failed_precondition` for unknown
    /// lifecycle states, `deadline_exceeded` on timeout, plus mapped journal
    /// errors from the snapshot reads.
    #[allow(clippy::result_large_err)]
    pub async fn wait_for_orchestrator_run(
        self: &Arc<Self>,
        request: OrchestratorRunWaitRequest,
    ) -> Result<OrchestratorRunWaitOutcome, Status> {
        let poll_interval = request.poll_interval.max(Duration::from_millis(25));
        let wait = async {
            loop {
                let snapshot = self
                    .orchestrator_run_status_snapshot(request.run_id.clone())
                    .await?
                    .ok_or_else(|| {
                        Status::not_found(format!("orchestrator run not found: {}", request.run_id))
                    })?;
                let canonical_state = canonical_phase_from_snapshot(&snapshot)?;
                if canonical_state.is_terminal()
                    || (request.return_on_waiting && canonical_state.is_waiting())
                {
                    return Ok(OrchestratorRunWaitOutcome { snapshot, canonical_state });
                }
                // Hybrid notify/poll: `notified()` is registered only after
                // the snapshot read, so a state change in that gap would be
                // missed; bounding the wait by poll_interval guarantees the
                // loop re-reads the snapshot regardless.
                let notified = self.orchestrator_run_notify.notified();
                let _ = tokio::time::timeout(poll_interval, notified).await;
            }
        };
        tokio::time::timeout(request.timeout, wait).await.map_err(|_| {
            Status::deadline_exceeded(format!(
                "timed out waiting for orchestrator run {}",
                request.run_id
            ))
        })?
    }

    // Tool result artifacts and tool jobs (background tool execution).

    #[allow(clippy::result_large_err)]
    fn create_tool_result_artifact_blocking(
        &self,
        request: &ToolResultArtifactCreateRequest,
    ) -> Result<ToolResultArtifactRef, Status> {
        self.journal_store
            .create_tool_result_artifact(request)
            .map_err(|error| map_orchestrator_store_error("create tool result artifact", error))
    }

    /// Persists a tool result artifact and returns its reference.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn create_tool_result_artifact(
        self: &Arc<Self>,
        request: ToolResultArtifactCreateRequest,
    ) -> Result<ToolResultArtifactRef, Status> {
        let state = Arc::clone(self);
        let artifact = tokio::task::spawn_blocking(move || {
            state.create_tool_result_artifact_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("tool result artifact create worker panicked"))??;
        self.orchestrator_run_notify.notify_waiters();
        Ok(artifact)
    }

    /// Maximum artifact payload size accepted by the journal store.
    pub(crate) fn tool_result_artifact_max_payload_bytes(&self) -> usize {
        self.journal_store.max_payload_bytes()
    }

    #[allow(clippy::result_large_err)]
    fn read_tool_result_artifact_blocking(
        &self,
        request: &ToolResultArtifactReadRequest,
    ) -> Result<ArtifactReadResponse, Status> {
        self.journal_store
            .read_tool_result_artifact(request)
            .map_err(|error| map_orchestrator_store_error("read tool result artifact", error))
    }

    /// Reads a tool result artifact range by reference.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn read_tool_result_artifact(
        self: &Arc<Self>,
        request: ToolResultArtifactReadRequest,
    ) -> Result<ArtifactReadResponse, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.read_tool_result_artifact_blocking(&request))
            .await
            .map_err(|_| Status::internal("tool result artifact read worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn create_tool_job_blocking(
        &self,
        request: &ToolJobCreateRequest,
    ) -> Result<ToolJobRecord, Status> {
        self.journal_store
            .create_tool_job(request)
            .map_err(|error| map_orchestrator_store_error("create tool job", error))
    }

    /// Creates a background tool job record.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn create_tool_job(
        self: &Arc<Self>,
        request: ToolJobCreateRequest,
    ) -> Result<ToolJobRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.create_tool_job_blocking(&request))
            .await
            .map_err(|_| Status::internal("tool job create worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn get_tool_job_blocking(&self, job_id: &str) -> Result<Option<ToolJobRecord>, Status> {
        self.journal_store
            .get_tool_job(job_id)
            .map_err(|error| map_orchestrator_store_error("get tool job", error))
    }

    /// Loads a tool job by id.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn get_tool_job(
        self: &Arc<Self>,
        job_id: String,
    ) -> Result<Option<ToolJobRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.get_tool_job_blocking(job_id.as_str()))
            .await
            .map_err(|_| Status::internal("tool job get worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_tool_jobs_blocking(
        &self,
        filter: &ToolJobsListFilter,
    ) -> Result<Vec<ToolJobRecord>, Status> {
        self.journal_store
            .list_tool_jobs(filter)
            .map_err(|error| map_orchestrator_store_error("list tool jobs", error))
    }

    /// Lists tool jobs matching the filter.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_tool_jobs(
        self: &Arc<Self>,
        filter: ToolJobsListFilter,
    ) -> Result<Vec<ToolJobRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.list_tool_jobs_blocking(&filter))
            .await
            .map_err(|_| Status::internal("tool job list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn transition_tool_job_blocking(
        &self,
        request: &ToolJobTransitionRequest,
    ) -> Result<ToolJobRecord, Status> {
        self.journal_store
            .transition_tool_job(request)
            .map_err(|error| map_orchestrator_store_error("transition tool job", error))
    }

    /// Applies a state transition to a tool job.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn transition_tool_job(
        self: &Arc<Self>,
        request: ToolJobTransitionRequest,
    ) -> Result<ToolJobRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.transition_tool_job_blocking(&request))
            .await
            .map_err(|_| Status::internal("tool job transition worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn append_tool_job_tail_blocking(
        &self,
        request: &ToolJobTailAppendRequest,
    ) -> Result<crate::journal::ToolJobTailEntry, Status> {
        self.journal_store
            .append_tool_job_tail(request)
            .map_err(|error| map_orchestrator_store_error("append tool job tail", error))
    }

    /// Appends an output tail entry to a tool job.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn append_tool_job_tail(
        self: &Arc<Self>,
        request: ToolJobTailAppendRequest,
    ) -> Result<crate::journal::ToolJobTailEntry, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.append_tool_job_tail_blocking(&request))
            .await
            .map_err(|_| Status::internal("tool job tail append worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn tail_tool_job_blocking(
        &self,
        request: &ToolJobTailReadRequest,
    ) -> Result<ToolJobTailPage, Status> {
        self.journal_store
            .tail_tool_job(request)
            .map_err(|error| map_orchestrator_store_error("tail tool job", error))
    }

    /// Reads a page of a tool job's output tail.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn tail_tool_job(
        self: &Arc<Self>,
        request: ToolJobTailReadRequest,
    ) -> Result<ToolJobTailPage, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.tail_tool_job_blocking(&request))
            .await
            .map_err(|_| Status::internal("tool job tail worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn attach_tool_job_blocking(
        &self,
        request: &ToolJobAttachRequest,
    ) -> Result<ToolJobRecord, Status> {
        self.journal_store
            .attach_tool_job(request)
            .map_err(|error| map_orchestrator_store_error("attach tool job", error))
    }

    /// Attaches a consumer to a tool job.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn attach_tool_job(
        self: &Arc<Self>,
        request: ToolJobAttachRequest,
    ) -> Result<ToolJobRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.attach_tool_job_blocking(&request))
            .await
            .map_err(|_| Status::internal("tool job attach worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn release_tool_job_attachment_blocking(&self, job_id: &str) -> Result<ToolJobRecord, Status> {
        self.journal_store
            .release_tool_job_attachment(job_id)
            .map_err(|error| map_orchestrator_store_error("release tool job attachment", error))
    }

    /// Releases a tool job's consumer attachment.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn release_tool_job_attachment(
        self: &Arc<Self>,
        job_id: String,
    ) -> Result<ToolJobRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.release_tool_job_attachment_blocking(job_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("tool job attachment release worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn retry_tool_job_blocking(
        &self,
        request: &ToolJobRetryRequest,
    ) -> Result<ToolJobRecord, Status> {
        self.journal_store
            .retry_tool_job(request)
            .map_err(|error| map_orchestrator_store_error("retry tool job", error))
    }

    /// Requeues a failed tool job for retry.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn retry_tool_job(
        self: &Arc<Self>,
        request: ToolJobRetryRequest,
    ) -> Result<ToolJobRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.retry_tool_job_blocking(&request))
            .await
            .map_err(|_| Status::internal("tool job retry worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn sweep_expired_tool_jobs_blocking(
        &self,
        now_unix_ms: i64,
        limit: usize,
    ) -> Result<Vec<ToolJobRecord>, Status> {
        self.journal_store
            .sweep_expired_tool_jobs(now_unix_ms, limit)
            .map_err(|error| map_orchestrator_store_error("sweep expired tool jobs", error))
    }

    /// Expires tool jobs past their deadline, bounded by `limit`.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn sweep_expired_tool_jobs(
        self: &Arc<Self>,
        now_unix_ms: i64,
        limit: usize,
    ) -> Result<Vec<ToolJobRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.sweep_expired_tool_jobs_blocking(now_unix_ms, limit)
        })
        .await
        .map_err(|_| Status::internal("tool job sweep worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn recover_stale_tool_jobs_blocking(
        &self,
        now_unix_ms: i64,
        stale_after_ms: i64,
        limit: usize,
    ) -> Result<Vec<ToolJobRecord>, Status> {
        self.journal_store
            .recover_stale_tool_jobs(now_unix_ms, stale_after_ms, limit)
            .map_err(|error| map_orchestrator_store_error("recover stale tool jobs", error))
    }

    /// Recovers jobs whose owner stopped heartbeating for `stale_after_ms`.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn recover_stale_tool_jobs(
        self: &Arc<Self>,
        now_unix_ms: i64,
        stale_after_ms: i64,
        limit: usize,
    ) -> Result<Vec<ToolJobRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.recover_stale_tool_jobs_blocking(now_unix_ms, stale_after_ms, limit)
        })
        .await
        .map_err(|_| Status::internal("tool job recovery worker panicked"))?
    }

    // Session history and context: runs, lineage, transcripts, window
    // search, recall artifacts, queued inputs, queue controls, pins,
    // compaction artifacts, checkpoints, workspace checkpoints/restore
    // reports, flows, background tasks, and learning records.

    #[allow(clippy::result_large_err)]
    fn list_session_write_leases_blocking(&self) -> Result<Vec<SessionWriteLeaseRecord>, Status> {
        self.journal_store
            .list_session_write_leases()
            .map_err(|error| map_orchestrator_store_error("list session write leases", error))
    }

    /// Lists active session write leases for operator diagnostics.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_session_write_leases(
        self: &Arc<Self>,
    ) -> Result<Vec<SessionWriteLeaseRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.list_session_write_leases_blocking())
            .await
            .map_err(|_| Status::internal("session write lease list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_orchestrator_session_runs_blocking(
        &self,
        session_id: &str,
    ) -> Result<Vec<OrchestratorRunStatusSnapshot>, Status> {
        self.journal_store
            .list_orchestrator_session_runs(session_id)
            .map_err(|error| map_orchestrator_store_error("list orchestrator session runs", error))
    }

    /// Lists run snapshots belonging to a session.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_orchestrator_session_runs(
        self: &Arc<Self>,
        session_id: String,
    ) -> Result<Vec<OrchestratorRunStatusSnapshot>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.list_orchestrator_session_runs_blocking(session_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("orchestrator session runs worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn update_orchestrator_session_lineage_blocking(
        &self,
        request: &OrchestratorSessionLineageUpdateRequest,
    ) -> Result<(), Status> {
        self.journal_store.update_orchestrator_session_lineage(request).map_err(|error| {
            map_orchestrator_store_error("update orchestrator session lineage", error)
        })
    }

    /// Updates a session's fork/branch lineage pointers.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn update_orchestrator_session_lineage(
        self: &Arc<Self>,
        request: OrchestratorSessionLineageUpdateRequest,
    ) -> Result<(), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.update_orchestrator_session_lineage_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("orchestrator session lineage worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_orchestrator_session_transcript_blocking(
        &self,
        session_id: &str,
    ) -> Result<Vec<OrchestratorSessionTranscriptRecord>, Status> {
        self.journal_store.list_orchestrator_session_transcript(session_id).map_err(|error| {
            map_orchestrator_store_error("load orchestrator session transcript", error)
        })
    }

    /// Loads the transcript records for a session.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_orchestrator_session_transcript(
        self: &Arc<Self>,
        session_id: String,
    ) -> Result<Vec<OrchestratorSessionTranscriptRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.list_orchestrator_session_transcript_blocking(session_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("orchestrator transcript worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_bounded_orchestrator_session_transcript_blocking(
        &self,
        session_id: &str,
        limit: usize,
    ) -> Result<Vec<OrchestratorSessionTranscriptRecord>, Status> {
        self.journal_store.list_bounded_orchestrator_session_transcript(session_id, limit).map_err(
            |error| {
                map_orchestrator_store_error("load bounded orchestrator session transcript", error)
            },
        )
    }

    /// Loads only the newest bounded transcript window for a session.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_bounded_orchestrator_session_transcript(
        self: &Arc<Self>,
        session_id: String,
        limit: usize,
    ) -> Result<Vec<OrchestratorSessionTranscriptRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.list_bounded_orchestrator_session_transcript_blocking(session_id.as_str(), limit)
        })
        .await
        .map_err(|_| Status::internal("bounded orchestrator transcript worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_scoped_session_runtime_generations_blocking(
        &self,
        principal: &str,
        device_id: &str,
        channel: Option<&str>,
    ) -> Result<Vec<ScopedSessionRuntimeGeneration>, Status> {
        self.journal_store
            .list_scoped_session_runtime_generations(principal, device_id, channel)
            .map_err(|error| {
                map_orchestrator_store_error("list scoped session runtime generations", error)
            })
    }

    /// Lists active run generations for one exact session owner scope.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_scoped_session_runtime_generations(
        self: &Arc<Self>,
        principal: String,
        device_id: String,
        channel: Option<String>,
    ) -> Result<Vec<ScopedSessionRuntimeGeneration>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.list_scoped_session_runtime_generations_blocking(
                principal.as_str(),
                device_id.as_str(),
                channel.as_deref(),
            )
        })
        .await
        .map_err(|_| Status::internal("session generation list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn reserve_session_model_command_blocking(
        &self,
        request: &SessionModelCommandReserveRequest,
    ) -> Result<SessionModelCommandReserveOutcome, Status> {
        self.journal_store.reserve_session_model_command(request).map_err(|error| {
            map_orchestrator_store_error("reserve model-visible session command", error)
        })
    }

    /// Reserves a model-visible session command before its queue/control effect.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn reserve_session_model_command(
        self: &Arc<Self>,
        request: SessionModelCommandReserveRequest,
    ) -> Result<SessionModelCommandReserveOutcome, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.reserve_session_model_command_blocking(&request))
            .await
            .map_err(|_| Status::internal("session command reservation worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn settle_session_model_command_blocking(
        &self,
        request: &SessionModelCommandSettlementRequest,
    ) -> Result<SessionModelCommandRecord, Status> {
        self.journal_store.settle_session_model_command(request).map_err(|error| {
            map_orchestrator_store_error("settle model-visible session command", error)
        })
    }

    /// Attaches the observable queue/control result to a reserved command.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn settle_session_model_command(
        self: &Arc<Self>,
        request: SessionModelCommandSettlementRequest,
    ) -> Result<SessionModelCommandRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.settle_session_model_command_blocking(&request))
            .await
            .map_err(|_| Status::internal("session command settlement worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn latest_orchestrator_session_transcript_event_blocking(
        &self,
        session_id: &str,
        event_type: &str,
    ) -> Result<Option<OrchestratorSessionTranscriptRecord>, Status> {
        self.journal_store
            .latest_orchestrator_session_transcript_event(session_id, event_type)
            .map_err(|error| {
                map_orchestrator_store_error("load latest orchestrator transcript event", error)
            })
    }

    /// Loads the newest transcript event of one type for a session.
    ///
    /// # Errors
    /// Returns the mapped journal error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn latest_orchestrator_session_transcript_event(
        self: &Arc<Self>,
        session_id: String,
        event_type: String,
    ) -> Result<Option<OrchestratorSessionTranscriptRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.latest_orchestrator_session_transcript_event_blocking(
                session_id.as_str(),
                event_type.as_str(),
            )
        })
        .await
        .map_err(|_| Status::internal("latest orchestrator transcript worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn search_orchestrator_session_windows_blocking(
        &self,
        request: &SessionSearchRequest,
    ) -> Result<SessionSearchOutcome, Status> {
        self.journal_store.search_orchestrator_session_windows(request).map_err(|error| {
            map_orchestrator_store_error("search orchestrator session windows", error)
        })
    }

    /// Searches transcript windows of a session.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn search_orchestrator_session_windows(
        self: &Arc<Self>,
        request: SessionSearchRequest,
    ) -> Result<SessionSearchOutcome, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.search_orchestrator_session_windows_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("orchestrator session search worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn create_recall_artifact_blocking(
        &self,
        request: &RecallArtifactCreateRequest,
    ) -> Result<RecallArtifactRecord, Status> {
        self.journal_store
            .create_recall_artifact(request)
            .map_err(|error| map_memory_store_error("create recall artifact", error))
    }

    /// Persists a recall artifact.
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn create_recall_artifact(
        self: &Arc<Self>,
        request: RecallArtifactCreateRequest,
    ) -> Result<RecallArtifactRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.create_recall_artifact_blocking(&request))
            .await
            .map_err(|_| Status::internal("recall artifact create worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_recall_artifacts_blocking(
        &self,
        filter: &RecallArtifactListFilter,
    ) -> Result<Vec<RecallArtifactRecord>, Status> {
        self.journal_store
            .list_recall_artifacts(filter)
            .map_err(|error| map_memory_store_error("list recall artifacts", error))
    }

    /// Lists recall artifacts matching the filter.
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_recall_artifacts(
        self: &Arc<Self>,
        filter: RecallArtifactListFilter,
    ) -> Result<Vec<RecallArtifactRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.list_recall_artifacts_blocking(&filter))
            .await
            .map_err(|_| Status::internal("recall artifact list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn create_orchestrator_queued_input_blocking(
        &self,
        request: &OrchestratorQueuedInputCreateRequest,
    ) -> Result<OrchestratorQueuedInputRecord, Status> {
        self.journal_store.create_orchestrator_queued_input(request).map_err(|error| {
            map_orchestrator_store_error("create queued orchestrator input", error)
        })
    }

    /// Queues an input for a busy session.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn create_orchestrator_queued_input(
        self: &Arc<Self>,
        request: OrchestratorQueuedInputCreateRequest,
    ) -> Result<OrchestratorQueuedInputRecord, Status> {
        let state = Arc::clone(self);
        let record = tokio::task::spawn_blocking(move || {
            state.create_orchestrator_queued_input_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("orchestrator queued input worker panicked"))??;
        self.orchestrator_run_notify.notify_waiters();
        Ok(record)
    }

    #[allow(clippy::result_large_err)]
    fn orchestrator_queued_input_by_id_blocking(
        &self,
        queued_input_id: &str,
    ) -> Result<Option<OrchestratorQueuedInputRecord>, Status> {
        self.journal_store
            .orchestrator_queued_input_by_id(queued_input_id)
            .map_err(|error| map_orchestrator_store_error("load queued orchestrator input", error))
    }

    /// Loads one durable queued input for command replay reconciliation.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn orchestrator_queued_input_by_id(
        self: &Arc<Self>,
        queued_input_id: String,
    ) -> Result<Option<OrchestratorQueuedInputRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.orchestrator_queued_input_by_id_blocking(queued_input_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("queued input lookup worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn update_orchestrator_queued_input_state_blocking(
        &self,
        request: &OrchestratorQueuedInputUpdateRequest,
    ) -> Result<OrchestratorQueuedInputRecord, Status> {
        self.journal_store.update_orchestrator_queued_input_state(request).map_err(|error| {
            map_orchestrator_store_error("update queued orchestrator input", error)
        })
    }

    /// Updates the state of a queued input.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn update_orchestrator_queued_input_state(
        self: &Arc<Self>,
        request: OrchestratorQueuedInputUpdateRequest,
    ) -> Result<OrchestratorQueuedInputRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.update_orchestrator_queued_input_state_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("orchestrator queued input state worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn prioritize_orchestrator_queued_input_blocking(
        &self,
        queued_input_id: &str,
        expected_revision: Option<i64>,
        priority_lane: &str,
        decision_reason: &str,
        explain_json: &str,
    ) -> Result<(), Status> {
        let expected_revision = match expected_revision {
            Some(revision) => revision,
            None => {
                self.journal_store
                    .orchestrator_queued_input_by_id(queued_input_id)
                    .map_err(|error| {
                        map_orchestrator_store_error("load queued orchestrator input", error)
                    })?
                    .ok_or_else(|| {
                        Status::not_found(format!("queued input not found: {queued_input_id}"))
                    })?
                    .lifecycle_revision
            }
        };
        self.journal_store
            .prioritize_orchestrator_queued_input(
                queued_input_id,
                expected_revision,
                priority_lane,
                decision_reason,
                explain_json,
            )
            .map_err(|error| {
                map_orchestrator_store_error("prioritize queued orchestrator input", error)
            })
    }

    /// Moves a queued input to a priority lane, recording the decision reason and explanation for
    /// audit.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn prioritize_orchestrator_queued_input(
        self: &Arc<Self>,
        queued_input_id: String,
        expected_revision: Option<i64>,
        priority_lane: String,
        decision_reason: String,
        explain_json: String,
    ) -> Result<(), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.prioritize_orchestrator_queued_input_blocking(
                queued_input_id.as_str(),
                expected_revision,
                priority_lane.as_str(),
                decision_reason.as_str(),
                explain_json.as_str(),
            )
        })
        .await
        .map_err(|_| Status::internal("orchestrator queued input priority worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn get_orchestrator_session_queue_control_blocking(
        &self,
        session_id: &str,
    ) -> Result<Option<OrchestratorSessionQueueControlRecord>, Status> {
        self.journal_store.get_orchestrator_session_queue_control(session_id).map_err(|error| {
            map_orchestrator_store_error("load orchestrator session queue control", error)
        })
    }

    /// Loads a session's queue control record, if any.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn get_orchestrator_session_queue_control(
        self: &Arc<Self>,
        session_id: String,
    ) -> Result<Option<OrchestratorSessionQueueControlRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.get_orchestrator_session_queue_control_blocking(session_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("orchestrator queue control worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn upsert_orchestrator_session_queue_control_blocking(
        &self,
        request: &OrchestratorSessionQueueControlUpdateRequest,
    ) -> Result<OrchestratorSessionQueueControlRecord, Status> {
        self.journal_store.upsert_orchestrator_session_queue_control(request).map_err(|error| {
            map_orchestrator_store_error("upsert orchestrator session queue control", error)
        })
    }

    /// Creates or updates a session's queue control record.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn upsert_orchestrator_session_queue_control(
        self: &Arc<Self>,
        request: OrchestratorSessionQueueControlUpdateRequest,
    ) -> Result<OrchestratorSessionQueueControlRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.upsert_orchestrator_session_queue_control_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("orchestrator queue control upsert worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_orchestrator_queued_inputs_blocking(
        &self,
        session_id: &str,
    ) -> Result<Vec<OrchestratorQueuedInputRecord>, Status> {
        self.journal_store
            .list_orchestrator_queued_inputs(session_id)
            .map_err(|error| map_orchestrator_store_error("load queued orchestrator inputs", error))
    }

    /// Lists the queued inputs of a session.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_orchestrator_queued_inputs(
        self: &Arc<Self>,
        session_id: String,
    ) -> Result<Vec<OrchestratorQueuedInputRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.list_orchestrator_queued_inputs_blocking(session_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("orchestrator queued input list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn create_orchestrator_session_pin_blocking(
        &self,
        request: &OrchestratorSessionPinCreateRequest,
    ) -> Result<OrchestratorSessionPinRecord, Status> {
        self.journal_store
            .create_orchestrator_session_pin(request)
            .map_err(|error| map_orchestrator_store_error("create orchestrator session pin", error))
    }

    /// Pins a transcript item in a session.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn create_orchestrator_session_pin(
        self: &Arc<Self>,
        request: OrchestratorSessionPinCreateRequest,
    ) -> Result<OrchestratorSessionPinRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.create_orchestrator_session_pin_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("orchestrator session pin worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_orchestrator_session_pins_blocking(
        &self,
        session_id: &str,
    ) -> Result<Vec<OrchestratorSessionPinRecord>, Status> {
        self.journal_store
            .list_orchestrator_session_pins(session_id)
            .map_err(|error| map_orchestrator_store_error("load orchestrator session pins", error))
    }

    /// Lists the pins of a session.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_orchestrator_session_pins(
        self: &Arc<Self>,
        session_id: String,
    ) -> Result<Vec<OrchestratorSessionPinRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.list_orchestrator_session_pins_blocking(session_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("orchestrator session pin list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn delete_orchestrator_session_pin_blocking(
        &self,
        session_id: &str,
        pin_id: &str,
    ) -> Result<bool, Status> {
        self.journal_store
            .delete_orchestrator_session_pin(session_id, pin_id)
            .map_err(|error| map_orchestrator_store_error("delete orchestrator session pin", error))
    }

    /// Deletes a session pin; returns whether it existed in the supplied session.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn delete_orchestrator_session_pin(
        self: &Arc<Self>,
        session_id: String,
        pin_id: String,
    ) -> Result<bool, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.delete_orchestrator_session_pin_blocking(session_id.as_str(), pin_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("orchestrator session pin delete worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn create_orchestrator_compaction_artifact_blocking(
        &self,
        request: &OrchestratorCompactionArtifactCreateRequest,
    ) -> Result<OrchestratorCompactionArtifactRecord, Status> {
        self.journal_store.create_orchestrator_compaction_artifact(request).map_err(|error| {
            map_orchestrator_store_error("create orchestrator compaction artifact", error)
        })
    }

    /// Persists a context-compaction artifact for a session.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn create_orchestrator_compaction_artifact(
        self: &Arc<Self>,
        request: OrchestratorCompactionArtifactCreateRequest,
    ) -> Result<OrchestratorCompactionArtifactRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.create_orchestrator_compaction_artifact_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("orchestrator compaction artifact worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_orchestrator_compaction_artifacts_blocking(
        &self,
        session_id: &str,
    ) -> Result<Vec<OrchestratorCompactionArtifactRecord>, Status> {
        self.journal_store.list_orchestrator_compaction_artifacts(session_id).map_err(|error| {
            map_orchestrator_store_error("list orchestrator compaction artifacts", error)
        })
    }

    /// Lists the compaction artifacts of a session.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_orchestrator_compaction_artifacts(
        self: &Arc<Self>,
        session_id: String,
    ) -> Result<Vec<OrchestratorCompactionArtifactRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.list_orchestrator_compaction_artifacts_blocking(session_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("orchestrator compaction artifact list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn get_orchestrator_compaction_artifact_blocking(
        &self,
        artifact_id: &str,
    ) -> Result<Option<OrchestratorCompactionArtifactRecord>, Status> {
        self.journal_store.get_orchestrator_compaction_artifact(artifact_id).map_err(|error| {
            map_orchestrator_store_error("load orchestrator compaction artifact", error)
        })
    }

    /// Loads a compaction artifact by id.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn get_orchestrator_compaction_artifact(
        self: &Arc<Self>,
        artifact_id: String,
    ) -> Result<Option<OrchestratorCompactionArtifactRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.get_orchestrator_compaction_artifact_blocking(artifact_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("orchestrator compaction artifact detail worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn create_orchestrator_checkpoint_blocking(
        &self,
        request: &OrchestratorCheckpointCreateRequest,
    ) -> Result<OrchestratorCheckpointRecord, Status> {
        self.journal_store
            .create_orchestrator_checkpoint(request)
            .map_err(|error| map_orchestrator_store_error("create orchestrator checkpoint", error))
    }

    /// Persists a conversation checkpoint.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn create_orchestrator_checkpoint(
        self: &Arc<Self>,
        request: OrchestratorCheckpointCreateRequest,
    ) -> Result<OrchestratorCheckpointRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.create_orchestrator_checkpoint_blocking(&request))
            .await
            .map_err(|_| Status::internal("orchestrator checkpoint worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_orchestrator_checkpoints_blocking(
        &self,
        session_id: &str,
    ) -> Result<Vec<OrchestratorCheckpointRecord>, Status> {
        self.journal_store
            .list_orchestrator_checkpoints(session_id)
            .map_err(|error| map_orchestrator_store_error("list orchestrator checkpoints", error))
    }

    /// Lists the checkpoints of a session.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_orchestrator_checkpoints(
        self: &Arc<Self>,
        session_id: String,
    ) -> Result<Vec<OrchestratorCheckpointRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.list_orchestrator_checkpoints_blocking(session_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("orchestrator checkpoint list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn get_orchestrator_checkpoint_blocking(
        &self,
        checkpoint_id: &str,
    ) -> Result<Option<OrchestratorCheckpointRecord>, Status> {
        self.journal_store
            .get_orchestrator_checkpoint(checkpoint_id)
            .map_err(|error| map_orchestrator_store_error("load orchestrator checkpoint", error))
    }

    /// Loads a checkpoint by id.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn get_orchestrator_checkpoint(
        self: &Arc<Self>,
        checkpoint_id: String,
    ) -> Result<Option<OrchestratorCheckpointRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.get_orchestrator_checkpoint_blocking(checkpoint_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("orchestrator checkpoint detail worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn mark_orchestrator_checkpoint_restored_blocking(
        &self,
        request: &OrchestratorCheckpointRestoreMarkRequest,
    ) -> Result<(), Status> {
        self.journal_store.mark_orchestrator_checkpoint_restored(request).map_err(|error| {
            map_orchestrator_store_error("mark orchestrator checkpoint restored", error)
        })
    }

    /// Marks a checkpoint as restored.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn mark_orchestrator_checkpoint_restored(
        self: &Arc<Self>,
        request: OrchestratorCheckpointRestoreMarkRequest,
    ) -> Result<(), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.mark_orchestrator_checkpoint_restored_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("orchestrator checkpoint restore worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn create_workspace_checkpoint_blocking(
        &self,
        request: &WorkspaceCheckpointCreateRequest,
    ) -> Result<WorkspaceCheckpointRecord, Status> {
        self.journal_store
            .create_workspace_checkpoint(request)
            .map_err(|error| map_orchestrator_store_error("create workspace checkpoint", error))
    }

    /// Persists a workspace (file-state) checkpoint.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn create_workspace_checkpoint(
        self: &Arc<Self>,
        request: WorkspaceCheckpointCreateRequest,
    ) -> Result<WorkspaceCheckpointRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.create_workspace_checkpoint_blocking(&request))
            .await
            .map_err(|_| Status::internal("workspace checkpoint worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn link_workspace_checkpoint_pair_blocking(
        &self,
        request: &WorkspaceCheckpointPairLinkRequest,
    ) -> Result<(), Status> {
        self.journal_store
            .link_workspace_checkpoint_pair(request)
            .map_err(|error| map_orchestrator_store_error("link workspace checkpoint pair", error))
    }

    /// Links the pre/post workspace checkpoints of one operation.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn link_workspace_checkpoint_pair(
        self: &Arc<Self>,
        request: WorkspaceCheckpointPairLinkRequest,
    ) -> Result<(), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.link_workspace_checkpoint_pair_blocking(&request))
            .await
            .map_err(|_| Status::internal("workspace checkpoint pair worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_workspace_checkpoints_blocking(
        &self,
        filter: &WorkspaceCheckpointListFilter,
    ) -> Result<Vec<WorkspaceCheckpointRecord>, Status> {
        self.journal_store
            .list_workspace_checkpoints(filter)
            .map_err(|error| map_orchestrator_store_error("list workspace checkpoints", error))
    }

    /// Lists workspace checkpoints matching the filter.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_workspace_checkpoints(
        self: &Arc<Self>,
        filter: WorkspaceCheckpointListFilter,
    ) -> Result<Vec<WorkspaceCheckpointRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.list_workspace_checkpoints_blocking(&filter))
            .await
            .map_err(|_| Status::internal("workspace checkpoint list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn get_workspace_checkpoint_blocking(
        &self,
        checkpoint_id: &str,
    ) -> Result<Option<WorkspaceCheckpointRecord>, Status> {
        self.journal_store
            .get_workspace_checkpoint(checkpoint_id)
            .map_err(|error| map_orchestrator_store_error("load workspace checkpoint", error))
    }

    /// Loads a workspace checkpoint by id.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn get_workspace_checkpoint(
        self: &Arc<Self>,
        checkpoint_id: String,
    ) -> Result<Option<WorkspaceCheckpointRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.get_workspace_checkpoint_blocking(checkpoint_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("workspace checkpoint detail worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_workspace_checkpoint_files_blocking(
        &self,
        checkpoint_id: &str,
    ) -> Result<Vec<WorkspaceCheckpointFileRecord>, Status> {
        self.journal_store
            .list_workspace_checkpoint_files(checkpoint_id)
            .map_err(|error| map_orchestrator_store_error("list workspace checkpoint files", error))
    }

    /// Lists the file records captured by a workspace checkpoint.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_workspace_checkpoint_files(
        self: &Arc<Self>,
        checkpoint_id: String,
    ) -> Result<Vec<WorkspaceCheckpointFileRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.list_workspace_checkpoint_files_blocking(checkpoint_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("workspace checkpoint file list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn get_workspace_checkpoint_file_payload_blocking(
        &self,
        artifact_id: &str,
    ) -> Result<Option<WorkspaceCheckpointFilePayload>, Status> {
        self.journal_store.get_workspace_checkpoint_file_payload(artifact_id).map_err(|error| {
            map_orchestrator_store_error("get workspace checkpoint file payload", error)
        })
    }

    /// Loads one captured file payload by artifact id.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn get_workspace_checkpoint_file_payload(
        self: &Arc<Self>,
        artifact_id: String,
    ) -> Result<Option<WorkspaceCheckpointFilePayload>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.get_workspace_checkpoint_file_payload_blocking(artifact_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("workspace checkpoint file payload worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn get_workspace_restore_report_blocking(
        &self,
        report_id: &str,
    ) -> Result<Option<WorkspaceRestoreReportRecord>, Status> {
        self.journal_store
            .get_workspace_restore_report(report_id)
            .map_err(|error| map_orchestrator_store_error("load workspace restore report", error))
    }

    /// Loads a workspace restore report by id.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn get_workspace_restore_report(
        self: &Arc<Self>,
        report_id: String,
    ) -> Result<Option<WorkspaceRestoreReportRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.get_workspace_restore_report_blocking(report_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("workspace restore report detail worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_workspace_restore_reports_blocking(
        &self,
        filter: &WorkspaceRestoreReportListFilter,
    ) -> Result<Vec<WorkspaceRestoreReportRecord>, Status> {
        self.journal_store
            .list_workspace_restore_reports(filter)
            .map_err(|error| map_orchestrator_store_error("list workspace restore reports", error))
    }

    /// Lists workspace restore reports matching the filter.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_workspace_restore_reports(
        self: &Arc<Self>,
        filter: WorkspaceRestoreReportListFilter,
    ) -> Result<Vec<WorkspaceRestoreReportRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.list_workspace_restore_reports_blocking(&filter))
            .await
            .map_err(|_| Status::internal("workspace restore report list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn summarize_workspace_restore_activity_blocking(
        &self,
        filter: &WorkspaceRestoreActivityFilter,
    ) -> Result<WorkspaceRestoreActivitySummary, Status> {
        self.journal_store.summarize_workspace_restore_activity(filter).map_err(|error| {
            map_orchestrator_store_error("summarize workspace restore activity", error)
        })
    }

    /// Aggregates workspace restore activity for the filter window.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn summarize_workspace_restore_activity(
        self: &Arc<Self>,
        filter: WorkspaceRestoreActivityFilter,
    ) -> Result<WorkspaceRestoreActivitySummary, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.summarize_workspace_restore_activity_blocking(&filter)
        })
        .await
        .map_err(|_| Status::internal("workspace restore activity worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn create_workspace_restore_report_blocking(
        &self,
        request: &WorkspaceRestoreReportCreateRequest,
    ) -> Result<WorkspaceRestoreReportRecord, Status> {
        self.journal_store
            .create_workspace_restore_report(request)
            .map_err(|error| map_orchestrator_store_error("create workspace restore report", error))
    }

    /// Persists a workspace restore report.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn create_workspace_restore_report(
        self: &Arc<Self>,
        request: WorkspaceRestoreReportCreateRequest,
    ) -> Result<WorkspaceRestoreReportRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.create_workspace_restore_report_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("workspace restore report worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn mark_workspace_checkpoint_restored_blocking(
        &self,
        request: &WorkspaceCheckpointRestoreMarkRequest,
    ) -> Result<(), Status> {
        self.journal_store.mark_workspace_checkpoint_restored(request).map_err(|error| {
            map_orchestrator_store_error("mark workspace checkpoint restored", error)
        })
    }

    /// Marks a workspace checkpoint as restored.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn mark_workspace_checkpoint_restored(
        self: &Arc<Self>,
        request: WorkspaceCheckpointRestoreMarkRequest,
    ) -> Result<(), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.mark_workspace_checkpoint_restored_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("workspace checkpoint restore worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn create_flow_blocking(&self, request: &FlowCreateRequest) -> Result<FlowRecord, Status> {
        self.journal_store
            .create_flow(request)
            .map_err(|error| map_orchestrator_store_error("create flow", error))
    }

    /// Creates a flow record.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn create_flow(
        self: &Arc<Self>,
        request: FlowCreateRequest,
    ) -> Result<FlowRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.create_flow_blocking(&request))
            .await
            .map_err(|_| Status::internal("flow create worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_flows_blocking(&self, filter: &FlowListFilter) -> Result<Vec<FlowRecord>, Status> {
        self.journal_store
            .list_flows(filter)
            .map_err(|error| map_orchestrator_store_error("list flows", error))
    }

    /// Lists flows matching the filter.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_flows(
        self: &Arc<Self>,
        filter: FlowListFilter,
    ) -> Result<Vec<FlowRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.list_flows_blocking(&filter))
            .await
            .map_err(|_| Status::internal("flow list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_flows_for_reconciliation_blocking(
        &self,
        limit: usize,
    ) -> Result<Vec<FlowRecord>, Status> {
        self.journal_store
            .list_flows_for_reconciliation(limit)
            .map_err(|error| map_orchestrator_store_error("list flows for reconciliation", error))
    }

    /// Lists the next fair batch of runnable flow-coordinator candidates.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_flows_for_reconciliation(
        self: &Arc<Self>,
        limit: usize,
    ) -> Result<Vec<FlowRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.list_flows_for_reconciliation_blocking(limit))
            .await
            .map_err(|_| Status::internal("flow reconciliation list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn get_flow_bundle_blocking(
        &self,
        flow_id: &str,
        event_limit: usize,
    ) -> Result<Option<FlowBundleRecord>, Status> {
        self.journal_store
            .get_flow_bundle(flow_id, event_limit)
            .map_err(|error| map_orchestrator_store_error("load flow", error))
    }

    /// Loads a flow with up to `event_limit` of its events.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn get_flow_bundle(
        self: &Arc<Self>,
        flow_id: String,
        event_limit: usize,
    ) -> Result<Option<FlowBundleRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.get_flow_bundle_blocking(flow_id.as_str(), event_limit)
        })
        .await
        .map_err(|_| Status::internal("flow detail worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn transition_flow_blocking(
        &self,
        request: &FlowTransitionRequest,
    ) -> Result<FlowRecord, Status> {
        self.journal_store
            .transition_flow(request)
            .map_err(|error| map_orchestrator_store_error("transition flow", error))
    }

    /// Applies a state transition to a flow.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn transition_flow(
        self: &Arc<Self>,
        request: FlowTransitionRequest,
    ) -> Result<FlowRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.transition_flow_blocking(&request))
            .await
            .map_err(|_| Status::internal("flow transition worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn update_flow_step_blocking(
        &self,
        request: &FlowStepUpdateRequest,
    ) -> Result<FlowStepRecord, Status> {
        self.journal_store
            .update_flow_step(request)
            .map_err(|error| map_orchestrator_store_error("update flow step", error))
    }

    /// Updates one step of a flow.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn update_flow_step(
        self: &Arc<Self>,
        request: FlowStepUpdateRequest,
    ) -> Result<FlowStepRecord, Status> {
        let state = Arc::clone(self);
        let step = tokio::task::spawn_blocking(move || state.update_flow_step_blocking(&request))
            .await
            .map_err(|_| Status::internal("flow step update worker panicked"))??;
        self.orchestrator_run_notify.notify_waiters();
        Ok(step)
    }

    #[allow(clippy::result_large_err)]
    fn audit_flow_dependencies_on_startup_blocking(
        &self,
    ) -> Result<FlowDependencyStartupAuditReport, Status> {
        self.journal_store.audit_flow_dependencies_on_startup().map_err(|error| {
            map_orchestrator_store_error("audit flow dependencies on startup", error)
        })
    }

    /// Validates every durable flow graph and records missing invalid-graph lifecycle evidence.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn audit_flow_dependencies_on_startup(
        self: &Arc<Self>,
    ) -> Result<FlowDependencyStartupAuditReport, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.audit_flow_dependencies_on_startup_blocking())
            .await
            .map_err(|_| Status::internal("flow dependency startup audit worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn quarantine_invalid_flow_dependencies_blocking(
        &self,
        request: &FlowDependenciesQuarantineRequest,
    ) -> Result<Option<FlowRecord>, Status> {
        self.journal_store
            .quarantine_invalid_flow_dependencies(request)
            .map_err(|error| map_orchestrator_store_error("quarantine flow dependencies", error))
    }

    /// Revalidates and quarantines an invalid graph with optimistic revision checking.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn quarantine_invalid_flow_dependencies(
        self: &Arc<Self>,
        request: FlowDependenciesQuarantineRequest,
    ) -> Result<Option<FlowRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.quarantine_invalid_flow_dependencies_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("flow dependency quarantine worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn repair_flow_dependencies_blocking(
        &self,
        request: &FlowDependenciesRepairRequest,
    ) -> Result<FlowRecord, Status> {
        self.journal_store
            .repair_flow_dependencies(request)
            .map_err(|error| map_orchestrator_store_error("repair flow dependencies", error))
    }

    /// Replaces dependency lists as one optimistic, graph-wide mutation.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn repair_flow_dependencies(
        self: &Arc<Self>,
        request: FlowDependenciesRepairRequest,
    ) -> Result<FlowRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.repair_flow_dependencies_blocking(&request))
            .await
            .map_err(|_| Status::internal("flow dependency repair worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn create_orchestrator_background_task_blocking(
        &self,
        request: &OrchestratorBackgroundTaskCreateRequest,
    ) -> Result<OrchestratorBackgroundTaskRecord, Status> {
        self.journal_store.create_orchestrator_background_task(request).map_err(|error| {
            map_orchestrator_store_error("create orchestrator background task", error)
        })
    }

    /// Creates a background task record.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn create_orchestrator_background_task(
        self: &Arc<Self>,
        request: OrchestratorBackgroundTaskCreateRequest,
    ) -> Result<OrchestratorBackgroundTaskRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.create_orchestrator_background_task_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("orchestrator background task worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn update_orchestrator_background_task_blocking(
        &self,
        request: &OrchestratorBackgroundTaskUpdateRequest,
    ) -> Result<OrchestratorBackgroundTaskRecord, Status> {
        self.journal_store.update_orchestrator_background_task(request).map_err(|error| {
            map_orchestrator_store_error("update orchestrator background task", error)
        })
    }

    /// Updates a background task record under host-owned revision authority.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn update_orchestrator_background_task(
        self: &Arc<Self>,
        request: OrchestratorBackgroundTaskUpdateRequest,
    ) -> Result<OrchestratorBackgroundTaskRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.update_orchestrator_background_task_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("orchestrator background task update worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn claim_orchestrator_background_task_blocking(
        &self,
        request: &OrchestratorBackgroundTaskClaimRequest,
    ) -> Result<OrchestratorBackgroundTaskRecord, Status> {
        self.journal_store.claim_orchestrator_background_task(request).map_err(|error| {
            map_orchestrator_store_error("claim orchestrator background task", error)
        })
    }

    /// Atomically claims queued background work and returns its worker authority.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn claim_orchestrator_background_task(
        self: &Arc<Self>,
        request: OrchestratorBackgroundTaskClaimRequest,
    ) -> Result<OrchestratorBackgroundTaskRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.claim_orchestrator_background_task_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("orchestrator background task claim worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn update_orchestrator_background_task_from_worker_blocking(
        &self,
        request: &OrchestratorBackgroundTaskWorkerUpdateRequest,
    ) -> Result<OrchestratorBackgroundTaskRecord, Status> {
        self.journal_store.update_orchestrator_background_task_from_worker(request).map_err(
            |error| {
                map_orchestrator_store_error(
                    "update orchestrator background task from worker",
                    error,
                )
            },
        )
    }

    #[allow(clippy::result_large_err)]
    fn update_orchestrator_background_task_from_worker_if_parent_generation_blocking(
        &self,
        request: &OrchestratorBackgroundTaskWorkerUpdateRequest,
        parent_guard: &OrchestratorParentGenerationGuard,
    ) -> Result<Option<OrchestratorBackgroundTaskRecord>, Status> {
        self.journal_store
            .update_orchestrator_background_task_from_worker_if_parent_generation(
                request,
                parent_guard,
            )
            .map_err(|error| {
                map_orchestrator_store_error(
                    "update orchestrator background task under parent generation",
                    error,
                )
            })
    }

    /// Applies a detached-worker callback under exact execution-generation authority.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn update_orchestrator_background_task_from_worker(
        self: &Arc<Self>,
        request: OrchestratorBackgroundTaskWorkerUpdateRequest,
    ) -> Result<OrchestratorBackgroundTaskRecord, Status> {
        let state = Arc::clone(self);
        let record = tokio::task::spawn_blocking(move || {
            state.update_orchestrator_background_task_from_worker_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("orchestrator background task worker callback panicked"))??;
        self.orchestrator_run_notify.notify_waiters();
        Ok(record)
    }

    /// Applies a child worker callback under both execution-generation and
    /// exact parent-generation authority.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn update_orchestrator_background_task_from_worker_if_parent_generation(
        self: &Arc<Self>,
        request: OrchestratorBackgroundTaskWorkerUpdateRequest,
        parent_guard: OrchestratorParentGenerationGuard,
    ) -> Result<Option<OrchestratorBackgroundTaskRecord>, Status> {
        let state = Arc::clone(self);
        let record = tokio::task::spawn_blocking(move || {
            state.update_orchestrator_background_task_from_worker_if_parent_generation_blocking(
                &request,
                &parent_guard,
            )
        })
        .await
        .map_err(|_| Status::internal("guarded background task worker callback panicked"))??;
        self.orchestrator_run_notify.notify_waiters();
        Ok(record)
    }

    /// Persists a parent checkpoint and child subscriptions before releasing
    /// the active run generation.
    ///
    /// # Errors
    /// Returns the mapped journal error or `internal` if the blocking worker
    /// panics.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn suspend_parent_for_children(
        self: &Arc<Self>,
        request: ParentSuspensionCreateRequest,
    ) -> Result<ParentSuspensionRecord, Status> {
        let state = Arc::clone(self);
        let record = tokio::task::spawn_blocking(move || {
            state.journal_store.suspend_parent_for_children(&request)
        })
        .await
        .map_err(|_| Status::internal("parent suspension worker panicked"))?
        .map_err(|error| map_orchestrator_store_error("suspend parent for children", error))?;
        self.orchestrator_run_notify.notify_waiters();
        Ok(record)
    }

    /// Applies one terminal child task to all matching durable parent
    /// subscriptions and wakes dispatchers after a continuation commit.
    ///
    /// # Errors
    /// Returns the mapped journal error or `internal` if the blocking worker
    /// panics.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn settle_parent_suspensions_for_child(
        self: &Arc<Self>,
        task_id: String,
    ) -> Result<Vec<ParentSuspensionWakeOutcome>, Status> {
        let state = Arc::clone(self);
        let outcomes = tokio::task::spawn_blocking(move || {
            state.journal_store.settle_parent_suspensions_for_child(task_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("parent wake worker panicked"))?
        .map_err(|error| map_orchestrator_store_error("wake suspended parent", error))?;
        if outcomes.iter().any(|outcome| {
            matches!(outcome, ParentSuspensionWakeOutcome::ContinuationQueued { .. })
        }) {
            self.orchestrator_run_notify.notify_waiters();
        }
        Ok(outcomes)
    }

    /// Reconciles terminal child evidence and expired parent deadlines after a
    /// restart or deadline notification.
    ///
    /// # Errors
    /// Returns the mapped journal error or `internal` if the blocking worker
    /// panics.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn reconcile_parent_suspensions(
        self: &Arc<Self>,
    ) -> Result<ParentSuspensionReconcileReport, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.journal_store.reconcile_parent_suspensions())
            .await
            .map_err(|_| Status::internal("parent suspension reconciliation worker panicked"))?
            .map_err(|error| map_orchestrator_store_error("reconcile parent suspensions", error))
    }

    /// Reconciles durable child-completion envelopes, orphan classifications,
    /// and exactly-once parent announcement delivery.
    ///
    /// # Errors
    /// Returns the mapped journal error or `internal` if the blocking worker
    /// panics.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn reconcile_child_completions(
        self: &Arc<Self>,
    ) -> Result<ChildCompletionReconcileReport, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.journal_store.reconcile_child_completions())
            .await
            .map_err(|_| Status::internal("child completion reconciliation worker panicked"))?
            .map_err(|error| map_orchestrator_store_error("reconcile child completions", error))
    }

    #[allow(clippy::result_large_err)]
    fn list_orchestrator_background_tasks_blocking(
        &self,
        filter: &OrchestratorBackgroundTaskListFilter,
    ) -> Result<Vec<OrchestratorBackgroundTaskRecord>, Status> {
        self.journal_store.list_orchestrator_background_tasks(filter).map_err(|error| {
            map_orchestrator_store_error("list orchestrator background tasks", error)
        })
    }

    /// Lists background tasks matching the filter.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_orchestrator_background_tasks(
        self: &Arc<Self>,
        filter: OrchestratorBackgroundTaskListFilter,
    ) -> Result<Vec<OrchestratorBackgroundTaskRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.list_orchestrator_background_tasks_blocking(&filter)
        })
        .await
        .map_err(|_| Status::internal("orchestrator background task list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn get_orchestrator_background_task_blocking(
        &self,
        task_id: &str,
    ) -> Result<Option<OrchestratorBackgroundTaskRecord>, Status> {
        self.journal_store.get_orchestrator_background_task(task_id).map_err(|error| {
            map_orchestrator_store_error("load orchestrator background task", error)
        })
    }

    /// Loads a background task by id.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn get_orchestrator_background_task(
        self: &Arc<Self>,
        task_id: String,
    ) -> Result<Option<OrchestratorBackgroundTaskRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.get_orchestrator_background_task_blocking(task_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("orchestrator background task detail worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn create_work_item_blocking(
        &self,
        request: &WorkItemCreateRequest,
    ) -> Result<WorkItemRecord, Status> {
        self.journal_store
            .create_work_item(request)
            .map_err(|error| map_orchestrator_store_error("create work item", error))
    }

    /// Creates a WorkBoard item.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn create_work_item(
        self: &Arc<Self>,
        request: WorkItemCreateRequest,
    ) -> Result<WorkItemRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.create_work_item_blocking(&request))
            .await
            .map_err(|_| Status::internal("work item create worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn update_work_item_blocking(
        &self,
        request: &WorkItemUpdateRequest,
    ) -> Result<WorkItemRecord, Status> {
        self.journal_store
            .update_work_item(request)
            .map_err(|error| map_orchestrator_store_error("update work item", error))
    }

    /// Updates a WorkBoard item.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn update_work_item(
        self: &Arc<Self>,
        request: WorkItemUpdateRequest,
    ) -> Result<WorkItemRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.update_work_item_blocking(&request))
            .await
            .map_err(|_| Status::internal("work item update worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_work_items_blocking(
        &self,
        filter: &WorkItemListFilter,
    ) -> Result<Vec<WorkItemRecord>, Status> {
        self.journal_store
            .list_work_items(filter)
            .map_err(|error| map_orchestrator_store_error("list work items", error))
    }

    /// Lists WorkBoard items matching the filter.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_work_items(
        self: &Arc<Self>,
        filter: WorkItemListFilter,
    ) -> Result<Vec<WorkItemRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.list_work_items_blocking(&filter))
            .await
            .map_err(|_| Status::internal("work item list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn get_work_item_blocking(&self, work_item_id: &str) -> Result<Option<WorkItemRecord>, Status> {
        self.journal_store
            .get_work_item(work_item_id)
            .map_err(|error| map_orchestrator_store_error("load work item", error))
    }

    /// Loads a WorkBoard item by id.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn get_work_item(
        self: &Arc<Self>,
        work_item_id: String,
    ) -> Result<Option<WorkItemRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.get_work_item_blocking(work_item_id.as_str()))
            .await
            .map_err(|_| Status::internal("work item detail worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_work_item_events_blocking(
        &self,
        work_item_id: &str,
        limit: usize,
    ) -> Result<Vec<WorkItemEventRecord>, Status> {
        self.journal_store
            .list_work_item_events(work_item_id, limit)
            .map_err(|error| map_orchestrator_store_error("list work item events", error))
    }

    /// Lists a WorkBoard item's audit events.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_work_item_events(
        self: &Arc<Self>,
        work_item_id: String,
        limit: usize,
    ) -> Result<Vec<WorkItemEventRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.list_work_item_events_blocking(work_item_id.as_str(), limit)
        })
        .await
        .map_err(|_| Status::internal("work item event list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn create_commitment_blocking(
        &self,
        request: &CommitmentCreateRequest,
    ) -> Result<CommitmentRecord, Status> {
        self.journal_store
            .create_commitment(request)
            .map_err(|error| map_orchestrator_store_error("create commitment", error))
    }

    /// Creates a commitment record.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn create_commitment(
        self: &Arc<Self>,
        request: CommitmentCreateRequest,
    ) -> Result<CommitmentRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.create_commitment_blocking(&request))
            .await
            .map_err(|_| Status::internal("commitment create worker panicked"))?
    }

    /// Returns owner-scoped, redacted V2 commitment candidate counts.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn commitment_candidate_v2_diagnostics(
        self: &Arc<Self>,
        owner_principal: String,
    ) -> Result<CommitmentCandidateV2Diagnostics, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .commitment_candidate_v2_diagnostics(owner_principal.as_str())
                .map_err(|error| {
                    map_orchestrator_store_error("read commitment candidate diagnostics", error)
                })
        })
        .await
        .map_err(|_| Status::internal("commitment candidate diagnostics worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn update_commitment_blocking(
        &self,
        request: &CommitmentUpdateRequest,
    ) -> Result<CommitmentRecord, Status> {
        self.journal_store
            .update_commitment(request)
            .map_err(|error| map_orchestrator_store_error("update commitment", error))
    }

    /// Updates a commitment record.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn update_commitment(
        self: &Arc<Self>,
        request: CommitmentUpdateRequest,
    ) -> Result<CommitmentRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.update_commitment_blocking(&request))
            .await
            .map_err(|_| Status::internal("commitment update worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_commitments_blocking(
        &self,
        filter: &CommitmentListFilter,
    ) -> Result<Vec<CommitmentRecord>, Status> {
        self.journal_store
            .list_commitments(filter)
            .map_err(|error| map_orchestrator_store_error("list commitments", error))
    }

    /// Lists commitments matching the filter.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_commitments(
        self: &Arc<Self>,
        filter: CommitmentListFilter,
    ) -> Result<Vec<CommitmentRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.list_commitments_blocking(&filter))
            .await
            .map_err(|_| Status::internal("commitment list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn get_commitment_blocking(
        &self,
        commitment_id: &str,
    ) -> Result<Option<CommitmentRecord>, Status> {
        self.journal_store
            .get_commitment(commitment_id)
            .map_err(|error| map_orchestrator_store_error("load commitment", error))
    }

    /// Loads a commitment by id.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn get_commitment(
        self: &Arc<Self>,
        commitment_id: String,
    ) -> Result<Option<CommitmentRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.get_commitment_blocking(commitment_id.as_str()))
            .await
            .map_err(|_| Status::internal("commitment detail worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_commitment_sources_blocking(
        &self,
        commitment_id: &str,
    ) -> Result<Vec<CommitmentSourceRecord>, Status> {
        self.journal_store
            .list_commitment_sources(commitment_id)
            .map_err(|error| map_orchestrator_store_error("list commitment sources", error))
    }

    /// Lists source evidence for a commitment.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_commitment_sources(
        self: &Arc<Self>,
        commitment_id: String,
    ) -> Result<Vec<CommitmentSourceRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.list_commitment_sources_blocking(commitment_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("commitment source list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_commitment_events_blocking(
        &self,
        commitment_id: &str,
        limit: usize,
    ) -> Result<Vec<CommitmentEventRecord>, Status> {
        self.journal_store
            .list_commitment_events(commitment_id, limit)
            .map_err(|error| map_orchestrator_store_error("list commitment events", error))
    }

    /// Lists commitment audit events.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_commitment_events(
        self: &Arc<Self>,
        commitment_id: String,
        limit: usize,
    ) -> Result<Vec<CommitmentEventRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.list_commitment_events_blocking(commitment_id.as_str(), limit)
        })
        .await
        .map_err(|_| Status::internal("commitment event list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn create_commitment_delivery_attempt_blocking(
        &self,
        request: &CommitmentDeliveryAttemptCreateRequest,
    ) -> Result<CommitmentDeliveryAttemptRecord, Status> {
        self.journal_store.create_commitment_delivery_attempt(request).map_err(|error| {
            map_orchestrator_store_error("create commitment delivery attempt", error)
        })
    }

    /// Records a commitment delivery attempt.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn create_commitment_delivery_attempt(
        self: &Arc<Self>,
        request: CommitmentDeliveryAttemptCreateRequest,
    ) -> Result<CommitmentDeliveryAttemptRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.create_commitment_delivery_attempt_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("commitment delivery attempt worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_commitment_delivery_attempts_blocking(
        &self,
        commitment_id: &str,
        limit: usize,
    ) -> Result<Vec<CommitmentDeliveryAttemptRecord>, Status> {
        self.journal_store.list_commitment_delivery_attempts(commitment_id, limit).map_err(
            |error| map_orchestrator_store_error("list commitment delivery attempts", error),
        )
    }

    /// Lists commitment delivery attempts.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_commitment_delivery_attempts(
        self: &Arc<Self>,
        commitment_id: String,
        limit: usize,
    ) -> Result<Vec<CommitmentDeliveryAttemptRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.list_commitment_delivery_attempts_blocking(commitment_id.as_str(), limit)
        })
        .await
        .map_err(|_| Status::internal("commitment delivery attempt list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn upsert_learning_candidate_blocking(
        &self,
        request: &LearningCandidateCreateRequest,
    ) -> Result<LearningCandidateRecord, Status> {
        self.journal_store
            .upsert_learning_candidate(request)
            .map_err(|error| map_orchestrator_store_error("upsert learning candidate", error))
    }

    /// Creates or updates a learning candidate.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn upsert_learning_candidate(
        self: &Arc<Self>,
        request: LearningCandidateCreateRequest,
    ) -> Result<LearningCandidateRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.upsert_learning_candidate_blocking(&request))
            .await
            .map_err(|_| Status::internal("learning candidate upsert worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn review_learning_candidate_blocking(
        &self,
        request: &LearningCandidateReviewRequest,
    ) -> Result<LearningCandidateRecord, Status> {
        self.journal_store
            .review_learning_candidate(request)
            .map_err(|error| map_orchestrator_store_error("review learning candidate", error))
    }

    /// Applies a review decision to a learning candidate.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn review_learning_candidate(
        self: &Arc<Self>,
        request: LearningCandidateReviewRequest,
    ) -> Result<LearningCandidateRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.review_learning_candidate_blocking(&request))
            .await
            .map_err(|_| Status::internal("learning candidate review worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_learning_candidates_blocking(
        &self,
        filter: &LearningCandidateListFilter,
    ) -> Result<Vec<LearningCandidateRecord>, Status> {
        self.journal_store
            .list_learning_candidates(filter)
            .map_err(|error| map_orchestrator_store_error("list learning candidates", error))
    }

    /// Lists learning candidates matching the filter.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_learning_candidates(
        self: &Arc<Self>,
        filter: LearningCandidateListFilter,
    ) -> Result<Vec<LearningCandidateRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.list_learning_candidates_blocking(&filter))
            .await
            .map_err(|_| Status::internal("learning candidate list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn learning_candidate_history_blocking(
        &self,
        candidate_id: &str,
    ) -> Result<Vec<LearningCandidateHistoryRecord>, Status> {
        self.journal_store
            .learning_candidate_history(candidate_id)
            .map_err(|error| map_orchestrator_store_error("list learning candidate history", error))
    }

    /// Loads the review history of a learning candidate.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn learning_candidate_history(
        self: &Arc<Self>,
        candidate_id: String,
    ) -> Result<Vec<LearningCandidateHistoryRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.learning_candidate_history_blocking(candidate_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("learning candidate history worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn record_learning_candidate_eval_blocking(
        &self,
        request: &LearningCandidateEvalCreateRequest,
    ) -> Result<LearningCandidateEvalRecord, Status> {
        self.journal_store
            .record_learning_candidate_eval(request)
            .map_err(|error| map_orchestrator_store_error("record learning candidate eval", error))
    }

    /// Appends an evaluation gate result for a learning candidate.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn record_learning_candidate_eval(
        self: &Arc<Self>,
        request: LearningCandidateEvalCreateRequest,
    ) -> Result<LearningCandidateEvalRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.record_learning_candidate_eval_blocking(&request))
            .await
            .map_err(|_| Status::internal("learning candidate eval worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_learning_candidate_evals_blocking(
        &self,
        candidate_id: &str,
        limit: usize,
    ) -> Result<Vec<LearningCandidateEvalRecord>, Status> {
        self.journal_store
            .list_learning_candidate_evals(candidate_id, limit)
            .map_err(|error| map_orchestrator_store_error("list learning candidate evals", error))
    }

    /// Lists evaluation gate results for a learning candidate.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_learning_candidate_evals(
        self: &Arc<Self>,
        candidate_id: String,
        limit: usize,
    ) -> Result<Vec<LearningCandidateEvalRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.list_learning_candidate_evals_blocking(candidate_id.as_str(), limit)
        })
        .await
        .map_err(|_| Status::internal("learning candidate eval list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn record_learning_candidate_rollout_blocking(
        &self,
        request: &LearningCandidateRolloutCreateRequest,
    ) -> Result<LearningCandidateRolloutRecord, Status> {
        self.journal_store.record_learning_candidate_rollout(request).map_err(|error| {
            map_orchestrator_store_error("record learning candidate rollout", error)
        })
    }

    /// Appends a rollout, monitoring, or rollback event for a learning candidate.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn record_learning_candidate_rollout(
        self: &Arc<Self>,
        request: LearningCandidateRolloutCreateRequest,
    ) -> Result<LearningCandidateRolloutRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.record_learning_candidate_rollout_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("learning candidate rollout worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_learning_candidate_rollouts_blocking(
        &self,
        candidate_id: &str,
        limit: usize,
    ) -> Result<Vec<LearningCandidateRolloutRecord>, Status> {
        self.journal_store.list_learning_candidate_rollouts(candidate_id, limit).map_err(|error| {
            map_orchestrator_store_error("list learning candidate rollouts", error)
        })
    }

    /// Lists rollout, monitoring, and rollback events for a learning candidate.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_learning_candidate_rollouts(
        self: &Arc<Self>,
        candidate_id: String,
        limit: usize,
    ) -> Result<Vec<LearningCandidateRolloutRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.list_learning_candidate_rollouts_blocking(candidate_id.as_str(), limit)
        })
        .await
        .map_err(|_| Status::internal("learning candidate rollout list worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn upsert_learning_preference_blocking(
        &self,
        request: &LearningPreferenceUpsertRequest,
    ) -> Result<LearningPreferenceRecord, Status> {
        self.journal_store
            .upsert_learning_preference(request)
            .map_err(|error| map_orchestrator_store_error("upsert learning preference", error))
    }

    /// Creates or updates a learned preference.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn upsert_learning_preference(
        self: &Arc<Self>,
        request: LearningPreferenceUpsertRequest,
    ) -> Result<LearningPreferenceRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.upsert_learning_preference_blocking(&request))
            .await
            .map_err(|_| Status::internal("learning preference upsert worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_learning_preferences_blocking(
        &self,
        filter: &LearningPreferenceListFilter,
    ) -> Result<Vec<LearningPreferenceRecord>, Status> {
        self.journal_store
            .list_learning_preferences(filter)
            .map_err(|error| map_orchestrator_store_error("list learning preferences", error))
    }

    /// Lists learned preferences matching the filter.
    ///
    /// # Errors
    /// Returns the mapped journal store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_learning_preferences(
        self: &Arc<Self>,
        filter: LearningPreferenceListFilter,
    ) -> Result<Vec<LearningPreferenceRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.list_learning_preferences_blocking(&filter))
            .await
            .map_err(|_| Status::internal("learning preference list worker panicked"))?
    }

    /// Assembles a redacted, budgeted page of tape events for a run.
    ///
    /// Payloads are redacted before byte accounting, the page is capped by
    /// both the entry and byte budgets, and `next_after_seq` is set when more
    /// events remain.
    ///
    /// # Errors
    /// `not_found` for unknown runs, `resource_exhausted` when a single event
    /// alone exceeds the byte budget (otherwise pagination could never make
    /// progress), plus mapped journal errors.
    #[allow(clippy::result_large_err)]
    pub(crate) fn orchestrator_tape_snapshot_blocking(
        &self,
        run_id: &str,
        after_seq: Option<i64>,
        requested_limit: Option<usize>,
    ) -> Result<RunTapeSnapshot, Status> {
        let run_exists = self
            .journal_store
            .orchestrator_run_status_snapshot(run_id)
            .map_err(|error| map_orchestrator_store_error("load orchestrator run snapshot", error))?
            .is_some();
        if !run_exists {
            return Err(Status::not_found(format!("orchestrator run not found: {run_id}")));
        }
        let limit = requested_limit
            .unwrap_or(self.config.max_tape_entries_per_response)
            .clamp(MIN_TAPE_PAGE_LIMIT, self.config.max_tape_entries_per_response);
        let fetched_events = self
            .journal_store
            .orchestrator_tape_page(run_id, after_seq, limit.saturating_add(1))
            .map_err(|error| map_orchestrator_store_error("load orchestrator tape", error))?;
        let mut events = Vec::with_capacity(limit);
        let mut returned_bytes = 0_usize;
        let mut has_more = false;

        for record in fetched_events {
            if events.len() >= limit {
                has_more = true;
                break;
            }
            let sanitized_payload =
                crate::journal::redact_payload_json(record.payload_json.as_bytes()).map_err(
                    |error| map_orchestrator_store_error("redact orchestrator tape payload", error),
                )?;
            let payload_bytes = sanitized_payload.len();
            if events.is_empty() && payload_bytes > self.config.max_tape_bytes_per_response {
                return Err(Status::resource_exhausted(format!(
                    "single orchestrator tape event exceeds response byte limit ({payload_bytes} > {})",
                    self.config.max_tape_bytes_per_response
                )));
            }
            if returned_bytes.saturating_add(payload_bytes)
                > self.config.max_tape_bytes_per_response
            {
                has_more = true;
                break;
            }
            returned_bytes = returned_bytes.saturating_add(payload_bytes);
            events.push(OrchestratorTapeRecord {
                seq: record.seq,
                event_type: record.event_type,
                payload_json: sanitized_payload,
            });
        }

        let next_after_seq = if has_more { events.last().map(|event| event.seq) } else { None };
        Ok(RunTapeSnapshot {
            run_id: run_id.to_owned(),
            requested_after_seq: after_seq,
            limit,
            max_response_bytes: self.config.max_tape_bytes_per_response,
            returned_bytes,
            next_after_seq,
            events,
        })
    }

    /// Async wrapper for `Self::orchestrator_tape_snapshot_blocking`.
    ///
    /// # Errors
    /// Same as the blocking variant, plus `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn orchestrator_tape_snapshot(
        self: &Arc<Self>,
        run_id: String,
        after_seq: Option<i64>,
        limit: Option<usize>,
    ) -> Result<RunTapeSnapshot, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.orchestrator_tape_snapshot_blocking(run_id.as_str(), after_seq, limit)
        })
        .await
        .map_err(|_| Status::internal("orchestrator tape snapshot worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    #[allow(dead_code)]
    fn incident_replay_bundle_blocking(
        &self,
        run_id: &str,
        requested_limit: Option<usize>,
    ) -> Result<ReplayBundle, Status> {
        let max_events = requested_limit
            .unwrap_or(self.config.replay_capture.max_events_per_run)
            .clamp(MIN_TAPE_PAGE_LIMIT, self.config.replay_capture.max_events_per_run);
        crate::replay_capture::capture_incident_replay_bundle(
            crate::replay_capture::IncidentReplayCaptureRequest {
                journal_store: &self.journal_store,
                replay_capture: &self.config.replay_capture,
                feature_rollouts: &self.config.feature_rollouts,
                run_id,
                generated_at_unix_ms: current_unix_ms(),
                max_events,
            },
        )
        .map_err(|error| Status::internal(format!("failed to capture replay bundle: {error:#}")))
    }

    /// Captures a deterministic replay bundle for an incident run, bounded by
    /// the replay-capture event budget.
    ///
    /// # Errors
    /// `internal` when capture fails or the worker panicked.
    #[allow(clippy::result_large_err)]
    #[allow(dead_code)]
    pub async fn incident_replay_bundle(
        self: &Arc<Self>,
        run_id: String,
        limit: Option<usize>,
    ) -> Result<ReplayBundle, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.incident_replay_bundle_blocking(run_id.as_str(), limit)
        })
        .await
        .map_err(|_| Status::internal("incident replay capture worker panicked"))?
    }

    // Cron jobs and cron runs.

    #[allow(clippy::result_large_err)]
    fn create_cron_job_blocking(
        &self,
        request: &CronJobCreateRequest,
    ) -> Result<CronJobRecord, Status> {
        self.journal_store
            .create_cron_job(request)
            .map_err(|error| map_cron_store_error("create cron job", error))
    }

    /// Creates a cron job and counts the creation.
    ///
    /// # Errors
    /// Returns the mapped cron store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn create_cron_job(
        self: &Arc<Self>,
        request: CronJobCreateRequest,
    ) -> Result<CronJobRecord, Status> {
        if request.enabled {
            crate::cron::ensure_archived_objective_allows_cron_job_enable(
                self.as_ref(),
                request.job_id.as_str(),
            )?;
        }
        let state = Arc::clone(self);
        let result = tokio::task::spawn_blocking(move || state.create_cron_job_blocking(&request))
            .await
            .map_err(|_| Status::internal("cron create worker panicked"))??;
        self.counters.cron_jobs_created.fetch_add(1, Ordering::Relaxed);
        Ok(result)
    }

    #[allow(clippy::result_large_err)]
    fn update_cron_job_blocking(
        &self,
        job_id: &str,
        patch: &CronJobUpdatePatch,
    ) -> Result<CronJobRecord, Status> {
        self.journal_store
            .update_cron_job(job_id, patch)
            .map_err(|error| map_cron_store_error("update cron job", error))
    }

    /// Applies a patch to a cron job and counts the update.
    ///
    /// # Errors
    /// Returns the mapped cron store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn update_cron_job(
        self: &Arc<Self>,
        job_id: String,
        patch: CronJobUpdatePatch,
    ) -> Result<CronJobRecord, Status> {
        if patch.enabled == Some(true) {
            crate::cron::ensure_archived_objective_allows_cron_job_enable(
                self.as_ref(),
                job_id.as_str(),
            )?;
        }
        let state = Arc::clone(self);
        let result = tokio::task::spawn_blocking(move || {
            state.update_cron_job_blocking(job_id.as_str(), &patch)
        })
        .await
        .map_err(|_| Status::internal("cron update worker panicked"))??;
        self.counters.cron_jobs_updated.fetch_add(1, Ordering::Relaxed);
        Ok(result)
    }

    #[allow(clippy::result_large_err)]
    fn delete_cron_job_blocking(&self, job_id: &str) -> Result<bool, Status> {
        self.journal_store
            .delete_cron_job(job_id)
            .map_err(|error| map_cron_store_error("delete cron job", error))
    }

    /// Deletes a cron job; returns whether it existed and counts the deletion.
    ///
    /// # Errors
    /// Returns the mapped cron store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn delete_cron_job(self: &Arc<Self>, job_id: String) -> Result<bool, Status> {
        let state = Arc::clone(self);
        let deleted =
            tokio::task::spawn_blocking(move || state.delete_cron_job_blocking(job_id.as_str()))
                .await
                .map_err(|_| Status::internal("cron delete worker panicked"))??;
        if deleted {
            self.counters.cron_jobs_deleted.fetch_add(1, Ordering::Relaxed);
        }
        Ok(deleted)
    }

    #[allow(clippy::result_large_err)]
    fn cron_job_blocking(&self, job_id: &str) -> Result<Option<CronJobRecord>, Status> {
        self.journal_store
            .cron_job(job_id)
            .map_err(|error| map_cron_store_error("load cron job", error))
    }

    /// Loads a cron job by id.
    ///
    /// # Errors
    /// Returns the mapped cron store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn cron_job(
        self: &Arc<Self>,
        job_id: String,
    ) -> Result<Option<CronJobRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.cron_job_blocking(job_id.as_str()))
            .await
            .map_err(|_| Status::internal("cron read worker panicked"))?
    }

    /// Atomically admits or rejects one scheduler-owned autonomous wake.
    ///
    /// # Errors
    /// Returns the mapped journal error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn admit_autonomous_wake(
        self: &Arc<Self>,
        request: AutonomousWakeAdmissionRequest,
    ) -> Result<AutonomousWakeAdmissionRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .admit_autonomous_wake(&request)
                .map_err(|error| map_cron_store_error("admit autonomous wake", error))
        })
        .await
        .map_err(|_| Status::internal("autonomous wake admission worker panicked"))?
    }

    /// Returns owner-scoped autonomous wake admission counts and last reason.
    ///
    /// # Errors
    /// Returns the mapped journal error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn autonomous_wake_diagnostics(
        self: &Arc<Self>,
        owner_principal: String,
    ) -> Result<crate::journal::AutonomousWakeDiagnostics, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .autonomous_wake_diagnostics(owner_principal.as_str())
                .map_err(|error| map_cron_store_error("read autonomous wake diagnostics", error))
        })
        .await
        .map_err(|_| Status::internal("autonomous wake diagnostics worker panicked"))?
    }

    /// Lists cron jobs with cursor pagination and optional enabled/owner/channel filters.
    ///
    /// # Errors
    /// Returns the mapped cron store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_cron_jobs(
        self: &Arc<Self>,
        after_job_id: Option<String>,
        requested_limit: Option<usize>,
        enabled: Option<bool>,
        owner_principal: Option<String>,
        channel: Option<String>,
    ) -> Result<(Vec<CronJobRecord>, Option<String>), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let limit = requested_limit.unwrap_or(100).clamp(1, MAX_CRON_PAGE_LIMIT);
            state
                .journal_store
                .list_cron_jobs(CronJobsListFilter {
                    after_job_id: after_job_id.as_deref(),
                    limit: limit.saturating_add(1),
                    enabled,
                    owner_principal: owner_principal.as_deref(),
                    channel: channel.as_deref(),
                })
                .map_err(|error| map_cron_store_error("list cron jobs", error))
        })
        .await
        .map_err(|_| Status::internal("cron list worker panicked"))?
        .map(|mut jobs| {
            let limit = requested_limit.unwrap_or(100).clamp(1, MAX_CRON_PAGE_LIMIT);
            let has_more = jobs.len() > limit;
            if has_more {
                jobs.truncate(limit);
            }
            let next_after =
                if has_more { jobs.last().map(|job| job.job_id.clone()) } else { None };
            (jobs, next_after)
        })
    }

    /// Lists enabled cron jobs due at or before `now_unix_ms`.
    ///
    /// # Errors
    /// Returns the mapped cron store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_due_cron_jobs(
        self: &Arc<Self>,
        now_unix_ms: i64,
        limit: usize,
    ) -> Result<Vec<CronJobRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .list_due_cron_jobs(now_unix_ms, limit)
                .map_err(|error| map_cron_store_error("list due cron jobs", error))
        })
        .await
        .map_err(|_| Status::internal("cron due-list worker panicked"))?
    }

    /// Earliest `next_run_at` across cron jobs, if any (scheduler wake time).
    ///
    /// # Errors
    /// Returns the mapped cron store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn first_due_cron_job_time(self: &Arc<Self>) -> Result<Option<i64>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .first_due_cron_job_time()
                .map_err(|error| map_cron_store_error("load first due cron job time", error))
        })
        .await
        .map_err(|_| Status::internal("cron next due worker panicked"))?
    }

    /// Updates a cron job's next/last run timestamps.
    ///
    /// # Errors
    /// Returns the mapped cron store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn set_cron_job_next_run(
        self: &Arc<Self>,
        job_id: String,
        next_run_at_unix_ms: Option<i64>,
        last_run_at_unix_ms: Option<i64>,
    ) -> Result<(), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .set_cron_job_next_run(job_id.as_str(), next_run_at_unix_ms, last_run_at_unix_ms)
                .map_err(|error| map_cron_store_error("update cron job next run", error))
        })
        .await
        .map_err(|_| Status::internal("cron next-run worker panicked"))?
    }

    /// Sets whether a cron job has a queued run pending.
    ///
    /// # Errors
    /// Returns the mapped cron store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn set_cron_job_queue_state(
        self: &Arc<Self>,
        job_id: String,
        queued_run: bool,
    ) -> Result<(), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .set_cron_job_queue_state(job_id.as_str(), queued_run)
                .map_err(|error| map_cron_store_error("update cron job queue state", error))
        })
        .await
        .map_err(|_| Status::internal("cron queue worker panicked"))?
    }

    /// Records the start of a cron run and counts it.
    ///
    /// # Errors
    /// Returns the mapped cron store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn start_cron_run(
        self: &Arc<Self>,
        request: CronRunStartRequest,
    ) -> Result<(), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .start_cron_run(&request)
                .map_err(|error| map_cron_store_error("start cron run", error))
        })
        .await
        .map_err(|_| Status::internal("cron run start worker panicked"))??;
        self.counters.cron_runs_started.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Finalizes a cron run and bumps the counter matching its terminal status.
    ///
    /// # Errors
    /// Returns the mapped cron store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn finalize_cron_run(
        self: &Arc<Self>,
        request: CronRunFinalizeRequest,
    ) -> Result<(), Status> {
        let terminal_status = request.status;
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .finalize_cron_run(&request)
                .map_err(|error| map_cron_store_error("finalize cron run", error))
        })
        .await
        .map_err(|_| Status::internal("cron run finalize worker panicked"))??;
        match terminal_status {
            CronRunStatus::Succeeded => {
                self.counters.cron_runs_completed.fetch_add(1, Ordering::Relaxed);
            }
            CronRunStatus::Failed | CronRunStatus::Denied => {
                self.counters.cron_runs_failed.fetch_add(1, Ordering::Relaxed);
            }
            CronRunStatus::Skipped => {
                self.counters.cron_runs_skipped.fetch_add(1, Ordering::Relaxed);
            }
            CronRunStatus::Accepted | CronRunStatus::Running => {}
        }
        Ok(())
    }

    /// Loads a cron run by id.
    ///
    /// # Errors
    /// Returns the mapped cron store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn cron_run(
        self: &Arc<Self>,
        run_id: String,
    ) -> Result<Option<CronRunRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .cron_run(run_id.as_str())
                .map_err(|error| map_cron_store_error("load cron run", error))
        })
        .await
        .map_err(|_| Status::internal("cron run read worker panicked"))?
    }

    /// Loads the active (non-terminal) cron run of a job, if any.
    ///
    /// # Errors
    /// Returns the mapped cron store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn active_cron_run_for_job(
        self: &Arc<Self>,
        job_id: String,
    ) -> Result<Option<CronRunRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .active_cron_run_for_job(job_id.as_str())
                .map_err(|error| map_cron_store_error("load active cron run", error))
        })
        .await
        .map_err(|_| Status::internal("active cron run worker panicked"))?
    }

    /// Lists cron runs (optionally for one job) with cursor pagination.
    ///
    /// # Errors
    /// Returns the mapped cron store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_cron_runs(
        self: &Arc<Self>,
        job_id: Option<String>,
        after_run_id: Option<String>,
        requested_limit: Option<usize>,
    ) -> Result<(Vec<CronRunRecord>, Option<String>), Status> {
        self.list_cron_runs_filtered(job_id, None, after_run_id, requested_limit).await
    }

    /// Lists cron runs across all jobs owned by a principal, with cursor pagination.
    ///
    /// # Errors
    /// Returns the mapped cron store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_cron_runs_for_owner(
        self: &Arc<Self>,
        owner_principal: String,
        after_run_id: Option<String>,
        requested_limit: Option<usize>,
    ) -> Result<(Vec<CronRunRecord>, Option<String>), Status> {
        self.list_cron_runs_filtered(None, Some(owner_principal), after_run_id, requested_limit)
            .await
    }

    #[allow(clippy::result_large_err)]
    async fn list_cron_runs_filtered(
        self: &Arc<Self>,
        job_id: Option<String>,
        owner_principal: Option<String>,
        after_run_id: Option<String>,
        requested_limit: Option<usize>,
    ) -> Result<(Vec<CronRunRecord>, Option<String>), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let limit = requested_limit.unwrap_or(100).clamp(1, MAX_CRON_PAGE_LIMIT);
            state
                .journal_store
                .list_cron_runs(CronRunsListFilter {
                    job_id: job_id.as_deref(),
                    owner_principal: owner_principal.as_deref(),
                    after_run_id: after_run_id.as_deref(),
                    limit: limit.saturating_add(1),
                })
                .map_err(|error| map_cron_store_error("list cron runs", error))
        })
        .await
        .map_err(|_| Status::internal("cron runs list worker panicked"))?
        .map(|mut runs| {
            let limit = requested_limit.unwrap_or(100).clamp(1, MAX_CRON_PAGE_LIMIT);
            let has_more = runs.len() > limit;
            if has_more {
                runs.truncate(limit);
            }
            let next_after =
                if has_more { runs.last().map(|run| run.run_id.clone()) } else { None };
            (runs, next_after)
        })
    }

    // Approvals: persistence plus the session-scoped decision cache that lets
    // identical tool proposals reuse a prior Allow/Deny within its scope.

    /// Persists an approval request; tool-subject requests bump the request
    /// counter.
    ///
    /// # Errors
    /// Returns the mapped approval store error, or `internal` if the worker
    /// panicked.
    #[allow(clippy::result_large_err)]
    pub async fn create_approval_record(
        self: &Arc<Self>,
        request: ApprovalCreateRequest,
    ) -> Result<ApprovalRecord, Status> {
        let subject_type = request.subject_type;
        let state = Arc::clone(self);
        let result = tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .create_approval(&request)
                .map_err(|error| map_approval_store_error("create approval", error))
        })
        .await
        .map_err(|_| Status::internal("approval create worker panicked"))??;
        if subject_type == ApprovalSubjectType::Tool {
            self.counters.approvals_tool_requested.fetch_add(1, Ordering::Relaxed);
        }
        Ok(result)
    }

    /// Resolves an approval. For tool subjects this also bumps the matching
    /// decision counter and seeds the session approval cache so later
    /// identical proposals can reuse the decision within its scope.
    ///
    /// # Errors
    /// Returns the mapped approval store error, or `internal` if the worker
    /// panicked.
    #[allow(clippy::result_large_err)]
    pub async fn resolve_approval_record(
        self: &Arc<Self>,
        request: ApprovalResolveRequest,
    ) -> Result<ApprovalRecord, Status> {
        let decision = request.decision;
        let state = Arc::clone(self);
        let result = tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .resolve_approval(&request)
                .map_err(|error| map_approval_store_error("resolve approval", error))
        })
        .await
        .map_err(|_| Status::internal("approval resolve worker panicked"))??;
        if result.subject_type == ApprovalSubjectType::Tool {
            match decision {
                ApprovalDecision::Allow => {
                    self.counters.approvals_tool_resolved_allow.fetch_add(1, Ordering::Relaxed);
                }
                ApprovalDecision::Deny => {
                    self.counters.approvals_tool_resolved_deny.fetch_add(1, Ordering::Relaxed);
                }
                ApprovalDecision::Timeout => {
                    self.counters.approvals_tool_resolved_timeout.fetch_add(1, Ordering::Relaxed);
                }
                ApprovalDecision::Error => {
                    self.counters.approvals_tool_resolved_error.fetch_add(1, Ordering::Relaxed);
                }
            }
            let cache_context = RequestContext {
                principal: result.principal.clone(),
                device_id: result.device_id.clone(),
                channel: result.channel.clone(),
            };
            let cached_outcome = tool_approval_outcome_from_record(&result, decision);
            self.remember_tool_approval(
                &cache_context,
                result.session_id.as_str(),
                result.subject_id.as_str(),
                &cached_outcome,
            );
        }
        self.orchestrator_run_notify.notify_waiters();
        Ok(result)
    }

    /// Loads one approval by id.
    ///
    /// # Errors
    /// Returns the mapped approval store error, or `internal` if the worker
    /// panicked.
    #[allow(clippy::result_large_err)]
    pub async fn approval_record(
        self: &Arc<Self>,
        approval_id: String,
    ) -> Result<Option<ApprovalRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .approval(approval_id.as_str())
                .map_err(|error| map_approval_store_error("load approval", error))
        })
        .await
        .map_err(|_| Status::internal("approval read worker panicked"))?
    }

    /// Atomically consumes an allowed once-scoped approval.
    ///
    /// Returns `false` when the approval was already consumed or is not
    /// currently an allowed once-scoped approval.
    ///
    /// # Errors
    /// Returns the mapped approval store error, or `internal` if the worker
    /// panicked.
    #[allow(clippy::result_large_err)]
    pub async fn consume_approval_once(
        self: &Arc<Self>,
        approval_id: String,
        consume_reason: String,
    ) -> Result<bool, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .consume_approval_once(approval_id.as_str(), consume_reason.as_str())
                .map_err(|error| map_approval_store_error("consume approval", error))
        })
        .await
        .map_err(|_| Status::internal("approval consume worker panicked"))?
    }

    /// Lists approvals with cursor pagination and optional time, subject,
    /// principal, decision, and subject-type filters.
    ///
    /// # Errors
    /// Returns the mapped approval store error, or `internal` if the worker
    /// panicked.
    #[allow(clippy::result_large_err)]
    #[allow(clippy::too_many_arguments)]
    pub async fn list_approval_records(
        self: &Arc<Self>,
        after_approval_id: Option<String>,
        requested_limit: Option<usize>,
        since_unix_ms: Option<i64>,
        until_unix_ms: Option<i64>,
        subject_id: Option<String>,
        principal: Option<String>,
        decision: Option<ApprovalDecision>,
        subject_type: Option<ApprovalSubjectType>,
    ) -> Result<(Vec<ApprovalRecord>, Option<String>), Status> {
        let effective_limit = requested_limit
            .filter(|value| *value > 0)
            .unwrap_or(100)
            .clamp(1, MAX_APPROVAL_PAGE_LIMIT);
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .list_approvals(ApprovalsListFilter {
                    after_approval_id: after_approval_id.as_deref(),
                    limit: effective_limit.saturating_add(1),
                    since_unix_ms,
                    until_unix_ms,
                    subject_id: subject_id.as_deref(),
                    principal: principal.as_deref(),
                    decision,
                    subject_type,
                })
                .map_err(|error| map_approval_store_error("list approvals", error))
        })
        .await
        .map_err(|_| Status::internal("approvals list worker panicked"))?
        .map(|mut approvals| {
            let has_more = approvals.len() > effective_limit;
            if has_more {
                approvals.truncate(effective_limit);
            }
            let next_after = if has_more {
                approvals.last().map(|approval| approval.approval_id.clone())
            } else {
                None
            };
            (approvals, next_after)
        })
    }

    // Tool posture registry delegates (synchronous; the registry has its own
    // storage). All of them map registry failures to `Status::internal`.

    /// Lists all tool posture overrides.
    ///
    /// # Errors
    /// `internal` when the posture registry read fails.
    pub fn list_tool_posture_overrides(&self) -> Result<Vec<ToolPostureOverrideRecord>, Status> {
        self.tool_posture_registry.list_overrides().map_err(|error| {
            Status::internal(format!("failed to list tool posture overrides: {error}"))
        })
    }

    /// Lists recorded posture recommendation actions.
    ///
    /// # Errors
    /// `internal` when the posture registry read fails.
    pub fn list_tool_posture_recommendation_actions(
        &self,
    ) -> Result<Vec<ToolPostureRecommendationActionRecord>, Status> {
        self.tool_posture_registry.list_recommendation_actions().map_err(|error| {
            Status::internal(format!("failed to list tool posture recommendation actions: {error}"))
        })
    }

    /// Lists posture audit events.
    ///
    /// # Errors
    /// `internal` when the posture registry read fails.
    pub fn list_tool_posture_audit_events(
        &self,
    ) -> Result<Vec<ToolPostureAuditEventRecord>, Status> {
        self.tool_posture_registry.list_audit_events().map_err(|error| {
            Status::internal(format!("failed to list tool posture audit events: {error}"))
        })
    }

    /// Creates or updates a posture override.
    ///
    /// # Errors
    /// `internal` when persisting the override fails.
    pub fn upsert_tool_posture_override(
        &self,
        request: ToolPostureOverrideUpsertRequest,
    ) -> Result<ToolPostureOverrideRecord, Status> {
        self.tool_posture_registry.upsert_override(request).map_err(|error| {
            Status::internal(format!("failed to persist tool posture override: {error}"))
        })
    }

    /// Clears a posture override; returns whether one existed.
    ///
    /// # Errors
    /// `internal` when clearing the override fails.
    pub fn clear_tool_posture_override(
        &self,
        request: ToolPostureOverrideClearRequest,
    ) -> Result<bool, Status> {
        self.tool_posture_registry.clear_override(request).map_err(|error| {
            Status::internal(format!("failed to clear tool posture override: {error}"))
        })
    }

    /// Resets all overrides in a scope, returning the removed records.
    ///
    /// # Errors
    /// `internal` when the scope reset fails.
    pub fn reset_tool_posture_scope(
        &self,
        request: ToolPostureScopeResetRequest,
    ) -> Result<Vec<ToolPostureOverrideRecord>, Status> {
        self.tool_posture_registry.reset_scope(request).map_err(|error| {
            Status::internal(format!("failed to reset tool posture scope: {error}"))
        })
    }

    /// Records the action taken on a posture recommendation.
    ///
    /// # Errors
    /// `internal` when persisting the action fails.
    pub fn record_tool_posture_recommendation_action(
        &self,
        request: ToolPostureRecommendationActionRequest,
    ) -> Result<ToolPostureRecommendationActionRecord, Status> {
        self.tool_posture_registry.record_recommendation_action(request).map_err(|error| {
            Status::internal(format!(
                "failed to persist tool posture recommendation action: {error}"
            ))
        })
    }

    // Skill status records and journaled runtime/console events.

    /// Persists a skill status record and counts the update.
    ///
    /// # Errors
    /// Returns the mapped skill store error, or `internal` if the worker
    /// panicked.
    #[allow(clippy::result_large_err)]
    pub async fn upsert_skill_status(
        self: &Arc<Self>,
        request: SkillStatusUpsertRequest,
    ) -> Result<SkillStatusRecord, Status> {
        let state = Arc::clone(self);
        let record = tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .upsert_skill_status(&request)
                .map_err(|error| map_skill_store_error("upsert skill status", error))
        })
        .await
        .map_err(|_| Status::internal("skill status update worker panicked"))??;
        self.counters.skill_status_updates.fetch_add(1, Ordering::Relaxed);
        Ok(record)
    }

    /// Loads the status record for one skill version.
    ///
    /// # Errors
    /// Returns the mapped skill store error, or `internal` if the worker
    /// panicked.
    #[allow(clippy::result_large_err)]
    pub async fn skill_status(
        self: &Arc<Self>,
        skill_id: String,
        version: String,
    ) -> Result<Option<SkillStatusRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .skill_status(skill_id.as_str(), version.as_str())
                .map_err(|error| map_skill_store_error("load skill status", error))
        })
        .await
        .map_err(|_| Status::internal("skill status read worker panicked"))?
    }

    /// Loads the most recent status record for a skill across versions.
    ///
    /// # Errors
    /// Returns the mapped skill store error, or `internal` if the worker
    /// panicked.
    #[allow(clippy::result_large_err)]
    pub async fn latest_skill_status(
        self: &Arc<Self>,
        skill_id: String,
    ) -> Result<Option<SkillStatusRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .latest_skill_status(skill_id.as_str())
                .map_err(|error| map_skill_store_error("load latest skill status", error))
        })
        .await
        .map_err(|_| Status::internal("latest skill status read worker panicked"))?
    }

    /// Journals a skill lifecycle event under fresh synthetic session/run ids
    /// (these events are not tied to a conversation).
    ///
    /// # Errors
    /// Same as `Self::record_journal_event`.
    #[allow(clippy::result_large_err)]
    pub async fn record_skill_status_event(
        self: &Arc<Self>,
        context: &RequestContext,
        event: &str,
        record: &SkillStatusRecord,
    ) -> Result<(), Status> {
        self.record_journal_event(JournalAppendRequest {
            event_id: Ulid::new().to_string(),
            session_id: Ulid::new().to_string(),
            run_id: Ulid::new().to_string(),
            kind: common_v1::journal_event::EventKind::ToolExecuted as i32,
            actor: common_v1::journal_event::EventActor::System as i32,
            timestamp_unix_ms: current_unix_ms(),
            payload_json: json!({
                "event": event,
                "skill_id": record.skill_id,
                "version": record.version,
                "status": record.status.as_str(),
                "reason": record.reason,
                "detected_at_ms": record.detected_at_ms,
                "operator_principal": record.operator_principal,
            })
            .to_string()
            .into_bytes(),
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
        })
        .await
        .map(|_| ())
    }

    /// Journals an operator console event under fresh synthetic session/run
    /// ids.
    ///
    /// # Errors
    /// Same as `Self::record_journal_event`.
    #[allow(clippy::result_large_err)]
    pub async fn record_console_event(
        self: &Arc<Self>,
        context: &RequestContext,
        event: &str,
        details: Value,
    ) -> Result<(), Status> {
        self.record_journal_event(JournalAppendRequest {
            event_id: Ulid::new().to_string(),
            session_id: Ulid::new().to_string(),
            run_id: Ulid::new().to_string(),
            kind: common_v1::journal_event::EventKind::ToolExecuted as i32,
            actor: common_v1::journal_event::EventActor::System as i32,
            timestamp_unix_ms: current_unix_ms(),
            payload_json: json!({
                "event": event,
                "details": details,
            })
            .to_string()
            .into_bytes(),
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
        })
        .await
        .map(|_| ())
    }

    #[allow(clippy::result_large_err)]
    async fn append_runtime_decision_event(
        self: &Arc<Self>,
        principal: String,
        device_id: String,
        channel: Option<String>,
        session_id: Option<String>,
        run_id: Option<String>,
        payload: RuntimeDecisionPayload,
    ) -> Result<(), Status> {
        self.append_runtime_decision_event_with_id(
            Ulid::new().to_string(),
            principal,
            device_id,
            channel,
            session_id,
            run_id,
            payload,
        )
        .await
    }

    #[allow(clippy::result_large_err, clippy::too_many_arguments)]
    async fn append_runtime_decision_event_with_id(
        self: &Arc<Self>,
        event_id: String,
        principal: String,
        device_id: String,
        channel: Option<String>,
        session_id: Option<String>,
        run_id: Option<String>,
        payload: RuntimeDecisionPayload,
    ) -> Result<(), Status> {
        let session_id = session_id.unwrap_or_else(|| Ulid::new().to_string());
        let run_id = run_id.unwrap_or_else(|| session_id.clone());
        let request = JournalAppendRequest {
            event_id,
            session_id,
            run_id,
            kind: common_v1::journal_event::EventKind::ToolExecuted as i32,
            actor: common_v1::journal_event::EventActor::System as i32,
            timestamp_unix_ms: current_unix_ms(),
            payload_json: json!({
                "event": payload.event_type.journal_event(),
                "payload": payload,
            })
            .to_string()
            .into_bytes(),
            principal,
            device_id,
            channel,
        };
        match self.record_journal_event(request.clone()).await {
            Ok(_) => {
                self.observability.record_runtime_decision_event(&payload);
                Ok(())
            }
            Err(error) if error.code() == tonic::Code::AlreadyExists => {
                if self.runtime_decision_event_matches(&request)? {
                    Ok(())
                } else {
                    Err(Status::failed_precondition(format!(
                        "journal event id conflicts with different runtime decision evidence: {}",
                        request.event_id
                    )))
                }
            }
            Err(error) => Err(error),
        }
    }

    #[allow(clippy::result_large_err)]
    fn runtime_decision_event_matches(
        &self,
        request: &JournalAppendRequest,
    ) -> Result<bool, Status> {
        let expected_payload = crate::journal::redact_payload_json(request.payload_json.as_slice())
            .map_err(|error| {
                map_orchestrator_store_error("redact runtime decision event for replay", error)
            })?;
        self.journal_store
            .event_by_id(request.event_id.as_str())
            .map(|event| {
                event.is_some_and(|event| {
                    event.session_id == request.session_id
                        && event.run_id == request.run_id
                        && event.kind == request.kind
                        && event.actor == request.actor
                        && event.payload_json == expected_payload
                        && event.principal == request.principal
                        && event.device_id == request.device_id
                        && event.channel == request.channel
                })
            })
            .map_err(|error| {
                map_orchestrator_store_error("inspect runtime decision event replay", error)
            })
    }

    /// Journals a runtime decision event for an authenticated request context
    /// and feeds it into observability.
    ///
    /// # Errors
    /// Same as `Self::record_journal_event`.
    #[allow(clippy::result_large_err)]
    pub async fn record_runtime_decision_event(
        self: &Arc<Self>,
        context: &RequestContext,
        session_id: Option<&str>,
        run_id: Option<&str>,
        payload: RuntimeDecisionPayload,
    ) -> Result<(), Status> {
        self.append_runtime_decision_event(
            context.principal.clone(),
            context.device_id.clone(),
            context.channel.clone(),
            session_id.map(ToOwned::to_owned),
            run_id.map(ToOwned::to_owned),
            payload,
        )
        .await
    }

    /// Journals a runtime decision event attributed to a system principal
    /// (background work with no request context).
    ///
    /// # Errors
    /// Same as `Self::record_journal_event`.
    #[allow(clippy::result_large_err)]
    pub async fn record_system_runtime_decision_event(
        self: &Arc<Self>,
        principal: &str,
        device_id: &str,
        channel: Option<&str>,
        session_id: Option<&str>,
        run_id: Option<&str>,
        payload: RuntimeDecisionPayload,
    ) -> Result<(), Status> {
        self.append_runtime_decision_event(
            principal.to_owned(),
            device_id.to_owned(),
            channel.map(ToOwned::to_owned),
            session_id.map(ToOwned::to_owned),
            run_id.map(ToOwned::to_owned),
            payload,
        )
        .await
    }

    /// Builds the decision actor for an authenticated request context.
    #[must_use]
    pub fn runtime_decision_actor_from_context(
        &self,
        context: &RequestContext,
        kind: RuntimeDecisionActorKind,
    ) -> RuntimeDecisionActor {
        RuntimeDecisionActor::new(
            kind,
            context.principal.clone(),
            context.device_id.clone(),
            context.channel.clone(),
        )
    }

    // Live runtime configuration (memory/retrieval/learning/routines) and
    // channel router delegates.

    /// Replaces the memory config and invalidates the memory search cache
    /// (cached scores depend on the old limits).
    pub fn configure_memory(&self, config: MemoryRuntimeConfig) {
        match self.memory_config.write() {
            Ok(mut guard) => {
                *guard = config;
            }
            Err(poisoned) => {
                warn!("memory config lock poisoned while applying runtime config");
                let mut guard = poisoned.into_inner();
                *guard = config;
            }
        }
        self.clear_memory_search_cache();
    }

    /// Replaces the retrieval config and invalidates the memory search cache.
    pub fn configure_retrieval(&self, config: RetrievalRuntimeConfig) {
        match self.retrieval_config.write() {
            Ok(mut guard) => {
                *guard = config;
            }
            Err(poisoned) => {
                warn!("retrieval config lock poisoned while applying runtime config");
                let mut guard = poisoned.into_inner();
                *guard = config;
            }
        }
        self.clear_memory_search_cache();
    }

    /// Installs the routines/objectives wiring once the scheduler is up.
    pub fn configure_routines_runtime(&self, config: RoutinesRuntimeConfig) {
        match self.routines_runtime.write() {
            Ok(mut guard) => {
                *guard = Some(config);
            }
            Err(poisoned) => {
                warn!("routines runtime lock poisoned while applying runtime config");
                let mut guard = poisoned.into_inner();
                *guard = Some(config);
            }
        }
    }

    /// Returns the routines wiring.
    ///
    /// # Errors
    /// `failed_precondition` until [`Self::configure_routines_runtime`] has
    /// run.
    #[allow(clippy::result_large_err)]
    pub fn routines_runtime_config(&self) -> Result<RoutinesRuntimeConfig, Status> {
        match self.routines_runtime.read() {
            Ok(config) => config
                .clone()
                .ok_or_else(|| Status::failed_precondition("routines runtime is not configured")),
            Err(poisoned) => poisoned
                .into_inner()
                .clone()
                .ok_or_else(|| Status::failed_precondition("routines runtime is not configured")),
        }
    }

    /// Current memory config (cloned).
    #[must_use]
    pub fn memory_config_snapshot(&self) -> MemoryRuntimeConfig {
        match self.memory_config.read() {
            Ok(config) => config.clone(),
            Err(poisoned) => {
                warn!("memory config lock poisoned while reading runtime config");
                poisoned.into_inner().clone()
            }
        }
    }

    /// Current code-intelligence runtime lifecycle snapshot.
    #[must_use]
    pub(crate) fn code_intel_runtime_snapshot(&self) -> CodeIntelRuntimeSnapshot {
        let request =
            CodeIntelRuntimeSnapshotRequest {
                enabled: self.config.code_intel.enabled,
                workspace_root: self.config.code_intel.workspace_root.as_deref().and_then(|path| {
                    path.to_str().map(str::trim).filter(|value| !value.is_empty())
                }),
                timeout_ms: self.config.code_intel.timeout_ms,
                idle_reap_ms: self.config.code_intel.idle_reap_ms,
                now_unix_ms: current_unix_ms(),
            };
        match self.code_intel_runtime.lock() {
            Ok(mut runtime) => runtime.snapshot(request),
            Err(poisoned) => {
                warn!("code-intelligence runtime lock poisoned while reading snapshot");
                poisoned.into_inner().snapshot(request)
            }
        }
    }

    /// Applies provider observations to the code-intelligence runtime read model.
    pub(crate) fn observe_code_intel_runtime(
        &self,
        workspace_root: Option<&str>,
        observations: &[CodeIntelProviderObservation],
        evidence_refs: &[String],
    ) -> CodeIntelRuntimeProjectionOutcome {
        self.observe_code_intel_runtime_with_dependencies(
            workspace_root,
            observations,
            &[],
            evidence_refs,
        )
    }

    /// Applies current provider observations after revalidating all snapshot dependencies.
    pub(crate) fn observe_code_intel_runtime_with_dependencies(
        &self,
        workspace_root: Option<&str>,
        observations: &[CodeIntelProviderObservation],
        dependent_observations: &[CodeIntelProviderObservation],
        evidence_refs: &[String],
    ) -> CodeIntelRuntimeProjectionOutcome {
        let mut runtime = match self.code_intel_runtime.lock() {
            Ok(runtime) => runtime,
            Err(poisoned) => {
                warn!("code-intelligence runtime lock poisoned while applying observations");
                poisoned.into_inner()
            }
        };
        let mut exact_authority =
            BTreeMap::<(String, u64), CodeIntelProviderSnapshotAuthority>::new();
        let mut provider_snapshot_authority = BTreeMap::new();
        let mut classify = |observation: &CodeIntelProviderObservation| {
            let classification =
                if observation.snapshot_authority == CodeIntelProviderSnapshotAuthority::Stale {
                    CodeIntelProviderSnapshotAuthority::Stale
                } else {
                    observation.runtime_authority.as_ref().map_or(
                        CodeIntelProviderSnapshotAuthority::Authoritative,
                        |authority| {
                            let key = (
                                authority.component_id.as_str().to_owned(),
                                authority.generation.get(),
                            );
                            *exact_authority.entry(key).or_insert_with(|| {
                                self.classify_code_intel_runtime_authority(
                                    authority,
                                    observation.language,
                                )
                            })
                        },
                    )
                };
            provider_snapshot_authority
                .entry(observation.language)
                .and_modify(|current| {
                    if classification == CodeIntelProviderSnapshotAuthority::Stale {
                        *current = CodeIntelProviderSnapshotAuthority::Stale;
                    }
                })
                .or_insert(classification);
            classification
        };
        for observation in dependent_observations {
            classify(observation);
        }
        let current_observations = observations
            .iter()
            .filter_map(|observation| match classify(observation) {
                CodeIntelProviderSnapshotAuthority::Authoritative => Some(observation.clone()),
                CodeIntelProviderSnapshotAuthority::Stale => None,
            })
            .collect::<Vec<_>>();
        let has_managed_observations =
            observations.iter().any(|observation| observation.runtime_authority.is_some());
        let has_current_managed_observations =
            current_observations.iter().any(|observation| observation.runtime_authority.is_some());
        let request = CodeIntelRuntimeObservationRequest {
            enabled: self.config.code_intel.enabled,
            workspace_root,
            observations: current_observations.as_slice(),
            timeout_ms: self.config.code_intel.timeout_ms,
            idle_reap_ms: self.config.code_intel.idle_reap_ms,
            now_unix_ms: current_unix_ms(),
            evidence_refs,
        };
        let outcome = if has_managed_observations && !has_current_managed_observations {
            crate::application::code_intel_runtime::CodeIntelRuntimeObservationOutcome {
                snapshot: runtime.snapshot_without_reap(CodeIntelRuntimeSnapshotRequest {
                    enabled: request.enabled,
                    workspace_root: request.workspace_root,
                    timeout_ms: request.timeout_ms,
                    idle_reap_ms: request.idle_reap_ms,
                    now_unix_ms: request.now_unix_ms,
                }),
                audit_events: Vec::new(),
            }
        } else {
            runtime.observe(request)
        };
        CodeIntelRuntimeProjectionOutcome {
            snapshot: outcome.snapshot,
            audit_events: outcome.audit_events,
            provider_snapshot_authority,
        }
    }

    /// Current retrieval config (cloned).
    #[must_use]
    pub fn retrieval_config_snapshot(&self) -> RetrievalRuntimeConfig {
        match self.retrieval_config.read() {
            Ok(config) => config.clone(),
            Err(poisoned) => {
                warn!("retrieval config lock poisoned while reading runtime config");
                poisoned.into_inner().clone()
            }
        }
    }

    /// Replaces the learning config (no caches depend on it).
    pub fn configure_learning(&self, config: LearningRuntimeConfig) {
        match self.learning_config.write() {
            Ok(mut guard) => {
                *guard = config;
            }
            Err(poisoned) => {
                warn!("learning config lock poisoned while applying runtime config");
                let mut guard = poisoned.into_inner();
                *guard = config;
            }
        }
    }

    /// Current learning config (cloned).
    #[must_use]
    pub fn learning_config_snapshot(&self) -> LearningRuntimeConfig {
        match self.learning_config.read() {
            Ok(config) => config.clone(),
            Err(poisoned) => {
                warn!("learning config lock poisoned while reading runtime config");
                poisoned.into_inner().clone()
            }
        }
    }

    /// Channel router config as loaded at startup (cloned).
    #[must_use]
    pub fn channel_router_config_snapshot(&self) -> ChannelRouterConfig {
        self.config.channel_router.clone()
    }

    /// Stable hash of the active channel router config (drift detection).
    #[must_use]
    pub fn channel_router_config_hash(&self) -> String {
        self.channel_router.config_hash()
    }

    /// Full startup config snapshot (cloned).
    #[must_use]
    pub fn runtime_config_snapshot(&self) -> GatewayRuntimeConfigSnapshot {
        self.config.clone()
    }

    /// Validation warnings produced when the router config was loaded.
    #[must_use]
    pub fn channel_router_validation_warnings(&self) -> Vec<String> {
        self.channel_router.validation_warnings()
    }

    /// Dry-runs routing for an inbound message without delivering it.
    #[must_use]
    pub fn channel_router_preview(&self, message: &ChannelInboundMessage) -> ChannelRoutePreview {
        self.channel_router.preview_route(message)
    }

    /// Pairing state per channel (optionally filtered to one channel).
    #[must_use]
    pub fn channel_router_pairing_snapshot(
        &self,
        channel: Option<&str>,
    ) -> Vec<ChannelPairingSnapshot> {
        self.channel_router.pairing_snapshot(channel)
    }

    /// Mints a pairing code for a channel.
    ///
    /// # Errors
    /// `failed_precondition` with the router's refusal reason.
    pub fn channel_router_mint_pairing_code(
        &self,
        channel: &str,
        issued_by: &str,
        ttl_ms: Option<u64>,
    ) -> Result<PairingCodeRecord, Status> {
        self.channel_router
            .mint_pairing_code(channel, issued_by, ttl_ms)
            .map_err(|reason| Status::failed_precondition(reason.as_str()))
    }

    /// Attempts to consume a pairing code for a sender identity.
    #[must_use]
    pub fn channel_router_consume_pairing_code(
        &self,
        channel: &str,
        sender_identity: Option<&str>,
        code: &str,
        pending_ttl_ms: Option<u64>,
    ) -> PairingConsumeOutcome {
        self.channel_router.consume_pairing_code(channel, sender_identity, code, pending_ttl_ms)
    }

    /// Associates an approval id with a pending pairing; `false` when no
    /// matching pending entry exists.
    #[must_use]
    pub fn channel_router_attach_pairing_pending_approval(
        &self,
        channel: &str,
        sender_identity: &str,
        approval_id: &str,
    ) -> bool {
        self.channel_router
            .attach_pairing_pending_approval(channel, sender_identity, approval_id)
            .is_some()
    }

    /// Applies an operator approval/denial to a pending pairing.
    #[must_use]
    pub fn channel_router_apply_pairing_approval(
        &self,
        approval_id: &str,
        approved: bool,
        decision_scope_ttl_ms: Option<i64>,
    ) -> PairingApprovalOutcome {
        self.channel_router.apply_pairing_approval(approval_id, approved, decision_scope_ttl_ms)
    }

    // Networked worker fleet. The fleet manager lives behind an RwLock; each
    // mutation journals its lifecycle event afterwards, and cleanup paths
    // fail closed when a worker's cleanup report shows leftover scoped data.

    /// Installs the remote dispatcher used by leased networked-worker tools.
    pub(crate) fn configure_networked_worker_remote_dispatcher(
        &self,
        dispatcher: Arc<dyn NetworkedWorkerRemoteDispatcher>,
    ) {
        match self.networked_worker_remote_dispatcher.write() {
            Ok(mut configured) => {
                *configured = Some(dispatcher);
            }
            Err(poisoned) => {
                warn!("networked worker remote dispatcher lock poisoned while configuring");
                *poisoned.into_inner() = Some(dispatcher);
            }
        }
    }

    /// Configured remote dispatcher for leased networked-worker tools.
    #[must_use]
    pub(crate) fn networked_worker_remote_dispatcher(
        &self,
    ) -> Option<Arc<dyn NetworkedWorkerRemoteDispatcher>> {
        match self.networked_worker_remote_dispatcher.read() {
            Ok(configured) => configured.clone(),
            Err(poisoned) => {
                warn!("networked worker remote dispatcher lock poisoned while reading");
                poisoned.into_inner().clone()
            }
        }
    }

    /// Builds the fleet admission policy from networked-worker config. The
    /// trusted capability list is currently a fixed built-in allowlist.
    #[must_use]
    pub fn worker_fleet_policy(&self) -> WorkerFleetPolicy {
        WorkerFleetPolicy {
            max_ttl_ms: self.config.networked_workers.lease_ttl_ms,
            heartbeat_timeout_ms: 30_000,
            trusted_capabilities: ["tool:palyra.echo", "tool:palyra.sleep"]
                .into_iter()
                .chain(WORKER_REMOTE_TOOL_CAPABILITIES.iter().copied())
                .map(str::to_owned)
                .collect(),
            required_capability_authority_sha256: None,
            required_sdk_protocol_version: Some(1),
            required_wit_abi_version: Some("palyra-worker-abi/v1".to_owned()),
            attestation: palyra_workerd::WorkerAttestationExpectation {
                require_egress_proxy: self.config.networked_workers.require_attestation,
                image_digest_sha256: self
                    .config
                    .networked_workers
                    .expected_image_digest_sha256
                    .clone(),
                build_digest_sha256: self
                    .config
                    .networked_workers
                    .expected_build_digest_sha256
                    .clone(),
                artifact_digest_sha256: self
                    .config
                    .networked_workers
                    .expected_artifact_digest_sha256
                    .clone(),
            },
        }
    }

    /// Current worker fleet state.
    #[must_use]
    pub fn worker_fleet_snapshot(&self) -> WorkerFleetSnapshot {
        match self.worker_fleet.read() {
            Ok(manager) => manager.snapshot(),
            Err(poisoned) => {
                warn!("worker fleet lock poisoned while reading snapshot");
                poisoned.into_inner().snapshot()
            }
        }
    }

    /// Recent worker lifecycle events retained by the fleet manager.
    #[must_use]
    pub fn worker_fleet_recent_events(&self) -> Vec<WorkerLifecycleEvent> {
        match self.worker_fleet.read() {
            Ok(manager) => manager.recent_events(),
            Err(poisoned) => {
                warn!("worker fleet lock poisoned while reading recent events");
                poisoned.into_inner().recent_events()
            }
        }
    }

    /// Stored attestation for a registered networked worker.
    #[must_use]
    pub fn networked_worker_attestation(&self, worker_id: &str) -> Option<WorkerAttestation> {
        match self.worker_fleet.read() {
            Ok(manager) => manager.worker_attestation(worker_id),
            Err(poisoned) => {
                warn!("worker fleet lock poisoned while reading worker attestation");
                poisoned.into_inner().worker_attestation(worker_id)
            }
        }
    }

    /// Evaluates one worker using only durable lifecycle, heartbeat, and
    /// attestation metadata. No lease is issued and no remote request is sent.
    pub(crate) fn probe_networked_worker_health(
        &self,
        component_id: &str,
    ) -> (HealthProbeDisposition, &'static str, Value) {
        let now_unix_ms = current_unix_ms();
        let policy = self.worker_fleet_policy();
        let records = match self.worker_fleet.read() {
            Ok(manager) => manager.durable_records(),
            Err(poisoned) => {
                warn!("worker fleet lock poisoned while evaluating health probe");
                poisoned.into_inner().durable_records()
            }
        };
        let record = records.iter().find_map(|(worker_id, record)| {
            managed_runtime_health_component_id(ManagedRuntimeHealthFamily::Worker, worker_id)
                .ok()
                .filter(|candidate| candidate.as_str() == component_id)
                .map(|_| record)
        });
        let Some(record) = record else {
            return (
                HealthProbeDisposition::Failed,
                "runtime.health.worker_missing",
                json!({"registered": false}),
            );
        };
        let attestation_valid =
            record.attestation.validate(&policy.attestation, now_unix_ms).is_ok();
        let heartbeat_fresh = now_unix_ms.saturating_sub(record.last_heartbeat_unix_ms)
            <= i64::try_from(policy.heartbeat_timeout_ms).unwrap_or(i64::MAX);
        let lifecycle_blocked = matches!(
            record.state,
            palyra_common::runtime_contracts::WorkerLifecycleState::Failed
                | palyra_common::runtime_contracts::WorkerLifecycleState::Offline
                | palyra_common::runtime_contracts::WorkerLifecycleState::Orphaned
        );
        let lifecycle_transitional = matches!(
            record.state,
            palyra_common::runtime_contracts::WorkerLifecycleState::Draining
        );
        let evidence = json!({
            "registered": true,
            "state": record.state.as_str(),
            "attestation_valid": attestation_valid,
            "heartbeat_fresh": heartbeat_fresh,
            "lease_active": record.lease.is_some(),
        });
        if !attestation_valid {
            return (
                HealthProbeDisposition::Failed,
                "runtime.health.worker_attestation_invalid",
                evidence,
            );
        }
        if !heartbeat_fresh || lifecycle_blocked {
            return (HealthProbeDisposition::Failed, "runtime.health.worker_unavailable", evidence);
        }
        if lifecycle_transitional {
            return (
                HealthProbeDisposition::Inconclusive,
                "runtime.health.worker_draining",
                evidence,
            );
        }
        (HealthProbeDisposition::Passed, "runtime.health.worker_probe_passed", evidence)
    }

    #[allow(clippy::result_large_err)]
    fn prepare_networked_worker_lifecycle_evidence(
        &self,
        transition_id: &str,
        event: &WorkerLifecycleEvent,
        details: Value,
    ) -> Result<PreparedNetworkedWorkerLifecycleEvidence, Status> {
        let payload = self.networked_worker_lifecycle_payload(event, details.clone());
        let request =
            self.networked_worker_lifecycle_journal_request(transition_id, event, details)?;
        Ok(PreparedNetworkedWorkerLifecycleEvidence {
            commit: crate::journal::NetworkedWorkerLifecycleCommit {
                request,
                event: event.clone(),
            },
            payload,
        })
    }

    #[allow(clippy::result_large_err)]
    fn reload_networked_worker_fleet_after_conflict(
        &self,
        manager: &mut WorkerFleetManager,
    ) -> Result<(), Status> {
        let snapshot = self
            .journal_store
            .load_networked_worker_fleet_snapshot(NETWORKED_WORKER_FLEET_MAX_ENTRIES)
            .map_err(|error| {
                map_orchestrator_store_error("reload networked worker fleet", error)
            })?;
        manager.restore_durable_records(snapshot.records).map_err(|error| {
            Status::failed_precondition(format!(
                "networked worker durable state restore failed: {error}"
            ))
        })?;
        self.worker_fleet_generation.store(snapshot.generation, Ordering::Relaxed);
        Ok(())
    }

    /// Creates exact durable node-dispatch authority for the active worker lease.
    ///
    /// # Errors
    /// Returns a gRPC status when claim metadata is invalid, capacity is exhausted, the exact
    /// durable lease changed, or storage cannot commit.
    #[allow(clippy::result_large_err)]
    pub(crate) fn create_networked_worker_dispatch_claim(
        &self,
        request: &NetworkedWorkerDispatchClaimCreateRequest,
    ) -> Result<NetworkedWorkerDispatchClaim, Status> {
        self.journal_store
            .create_networked_worker_dispatch_claim(
                request,
                NETWORKED_WORKER_DISPATCH_CLAIM_MAX_ENTRIES,
                current_unix_ms(),
            )
            .map_err(|error| {
                map_orchestrator_store_error("create networked worker dispatch claim", error)
            })
    }

    #[allow(clippy::result_large_err)]
    fn commit_prepared_networked_worker_lifecycle_with_run_authority(
        &self,
        manager: &mut WorkerFleetManager,
        candidate: &WorkerFleetManager,
        evidence: &[PreparedNetworkedWorkerLifecycleEvidence],
        revocations: &[crate::journal::NetworkedWorkerLeaseRevocation],
        settlement: Option<&crate::journal::NetworkedWorkerDispatchSettlement>,
        run_authority: Option<&crate::journal::NetworkedWorkerRunGenerationAuthority>,
    ) -> Result<PreparedNetworkedWorkerLifecycleCommitOutcome, Status> {
        let commits = evidence.iter().map(|item| item.commit.clone()).collect::<Vec<_>>();
        let expected_generation = self.worker_fleet_generation.load(Ordering::Relaxed);
        let outcome = match self.journal_store.commit_networked_worker_lifecycle_with_revocations(
            commits.as_slice(),
            &candidate.durable_records(),
            expected_generation,
            NETWORKED_WORKER_FLEET_MAX_ENTRIES,
            current_unix_ms(),
            revocations,
            settlement,
            run_authority,
        ) {
            Ok(outcome) => outcome,
            Err(error @ JournalError::NetworkedWorkerFleetGenerationConflict { .. }) => {
                self.counters.journal_persist_failures.fetch_add(1, Ordering::Relaxed);
                self.reload_networked_worker_fleet_after_conflict(manager)?;
                return Err(map_orchestrator_store_error(
                    "commit networked worker lifecycle",
                    error,
                ));
            }
            Err(error @ JournalError::NetworkedWorkerDispatchSettlementRejected { .. }) => {
                if let Some(settlement) = settlement {
                    self.record_networked_worker_stale_settlement_if_needed(settlement);
                }
                self.counters.journal_persist_failures.fetch_add(1, Ordering::Relaxed);
                return Err(map_orchestrator_store_error(
                    "commit networked worker lifecycle",
                    error,
                ));
            }
            Err(error) => {
                self.counters.journal_persist_failures.fetch_add(1, Ordering::Relaxed);
                return Err(map_orchestrator_store_error(
                    "commit networked worker lifecycle",
                    error,
                ));
            }
        };
        let crate::journal::NetworkedWorkerLifecycleCommitOutcome::Committed {
            fleet_generation,
            journal_outcomes,
            acknowledgement_error,
        } = outcome
        else {
            return Ok(PreparedNetworkedWorkerLifecycleCommitOutcome::StaleSuppressed);
        };
        self.worker_fleet_generation.store(fleet_generation, Ordering::Relaxed);
        for journal_outcome in journal_outcomes {
            self.counters.journal_events.fetch_add(1, Ordering::Relaxed);
            if journal_outcome.redacted {
                self.counters.journal_redacted_events.fetch_add(1, Ordering::Relaxed);
            }
            if journal_outcome.write_duration.as_millis() > JOURNAL_WRITE_LATENCY_BUDGET_MS {
                warn!(
                    write_duration_ms = journal_outcome.write_duration.as_millis(),
                    budget_ms = JOURNAL_WRITE_LATENCY_BUDGET_MS,
                    "networked worker lifecycle journal write exceeded latency budget"
                );
            }
        }
        *manager = candidate.clone();
        for item in evidence {
            self.observability.record_runtime_decision_event(&item.payload);
        }
        Ok(PreparedNetworkedWorkerLifecycleCommitOutcome::Committed { acknowledgement_error })
    }

    #[allow(clippy::result_large_err)]
    fn commit_prepared_networked_worker_lifecycle_with_authority_changes(
        &self,
        manager: &mut WorkerFleetManager,
        candidate: &WorkerFleetManager,
        evidence: &[PreparedNetworkedWorkerLifecycleEvidence],
        revocations: &[crate::journal::NetworkedWorkerLeaseRevocation],
        settlement: Option<&crate::journal::NetworkedWorkerDispatchSettlement>,
    ) -> Result<Option<JournalError>, Status> {
        match self.commit_prepared_networked_worker_lifecycle_with_run_authority(
            manager,
            candidate,
            evidence,
            revocations,
            settlement,
            None,
        )? {
            PreparedNetworkedWorkerLifecycleCommitOutcome::Committed { acknowledgement_error } => {
                Ok(acknowledgement_error)
            }
            PreparedNetworkedWorkerLifecycleCommitOutcome::StaleSuppressed => {
                Err(Status::internal(
                    "unfenced networked worker lifecycle commit was unexpectedly suppressed",
                ))
            }
        }
    }

    #[allow(clippy::result_large_err)]
    fn commit_networked_worker_lifecycle(
        &self,
        manager: &mut WorkerFleetManager,
        candidate: &WorkerFleetManager,
        events: &[(WorkerLifecycleEvent, Value)],
    ) -> Result<Option<JournalError>, Status> {
        self.commit_networked_worker_lifecycle_with_revocations(manager, candidate, events, &[])
    }

    #[allow(clippy::result_large_err)]
    fn commit_networked_worker_lifecycle_with_revocations(
        &self,
        manager: &mut WorkerFleetManager,
        candidate: &WorkerFleetManager,
        events: &[(WorkerLifecycleEvent, Value)],
        revocations: &[crate::journal::NetworkedWorkerLeaseRevocation],
    ) -> Result<Option<JournalError>, Status> {
        let transition_id = Ulid::new().to_string();
        let evidence = events
            .iter()
            .map(|(event, details)| {
                self.prepare_networked_worker_lifecycle_evidence(
                    transition_id.as_str(),
                    event,
                    details.clone(),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.commit_prepared_networked_worker_lifecycle_with_authority_changes(
            manager,
            candidate,
            evidence.as_slice(),
            revocations,
            None,
        )
    }

    /// Admits a worker after attestation checks and journals the event.
    ///
    /// # Errors
    /// `failed_precondition` on policy rejection, plus journaling errors.
    #[allow(clippy::result_large_err)]
    #[allow(dead_code)]
    pub async fn register_networked_worker(
        self: &Arc<Self>,
        attestation: WorkerAttestation,
    ) -> Result<WorkerLifecycleEvent, Status> {
        let policy = self.worker_fleet_policy();
        let now_unix_ms = current_unix_ms();
        let mut manager = match self.worker_fleet.write() {
            Ok(manager) => manager,
            Err(poisoned) => {
                warn!("worker fleet lock poisoned while registering worker");
                poisoned.into_inner()
            }
        };
        let mut candidate = manager.clone();
        let event =
            candidate.register_worker(attestation, &policy, now_unix_ms).map_err(|error| {
                Status::failed_precondition(format!(
                    "networked worker registration failed: {error}"
                ))
            })?;
        let acknowledgement_error = self.commit_networked_worker_lifecycle(
            &mut manager,
            &candidate,
            &[(event.clone(), Value::Null)],
        )?;
        if let Some(error) = acknowledgement_error {
            return Err(map_orchestrator_store_error(
                "acknowledge networked worker registration",
                error,
            ));
        }
        drop(manager);
        self.activate_networked_worker_runtime_health(event.worker_id.as_str()).map_err(
            |error| map_orchestrator_store_error("activate networked worker runtime health", error),
        )?;
        Ok(event)
    }

    /// Assigns a lease to a specific worker and journals the event.
    ///
    /// # Errors
    /// `failed_precondition` when assignment is refused, plus journaling
    /// errors.
    #[allow(clippy::result_large_err)]
    #[allow(dead_code)]
    pub async fn assign_networked_worker_lease(
        self: &Arc<Self>,
        worker_id: &str,
        request: WorkerLeaseRequest,
    ) -> Result<(WorkerLease, WorkerLifecycleEvent), Status> {
        let policy = self.worker_fleet_policy();
        let now_unix_ms = current_unix_ms();
        let assign_work = |manager: &mut WorkerFleetManager| {
            manager.assign_work(worker_id, request.clone(), &policy, now_unix_ms).map_err(|error| {
                Status::failed_precondition(format!(
                    "networked worker lease assignment failed: {error}"
                ))
            })
        };
        let mut manager = match self.worker_fleet.write() {
            Ok(manager) => manager,
            Err(poisoned) => {
                warn!("worker fleet lock poisoned while assigning lease");
                poisoned.into_inner()
            }
        };
        let mut candidate = manager.clone();
        let (lease, event) = assign_work(&mut candidate)?;
        let acknowledgement_error = self.commit_networked_worker_lifecycle(
            &mut manager,
            &candidate,
            &[(event.clone(), Value::Null)],
        )?;
        if let Some(error) = acknowledgement_error {
            return Err(map_orchestrator_store_error(
                "acknowledge networked worker lease assignment",
                error,
            ));
        }
        Ok((lease, event))
    }

    /// Assigns a lease to the next eligible worker and journals the event.
    ///
    /// # Errors
    /// `failed_precondition` when no worker can take the lease, plus
    /// journaling errors.
    #[allow(clippy::result_large_err)]
    #[allow(dead_code)]
    pub async fn assign_next_networked_worker_lease(
        self: &Arc<Self>,
        request: WorkerLeaseRequest,
    ) -> Result<(WorkerLease, WorkerLifecycleEvent), Status> {
        let policy = self.worker_fleet_policy();
        let now_unix_ms = current_unix_ms();
        let assign_work = |manager: &mut WorkerFleetManager| {
            manager.assign_next_work(request.clone(), &policy, now_unix_ms).map_err(|error| {
                Status::failed_precondition(format!(
                    "networked worker lease assignment failed: {error}"
                ))
            })
        };
        let mut manager = match self.worker_fleet.write() {
            Ok(manager) => manager,
            Err(poisoned) => {
                warn!("worker fleet lock poisoned while assigning next lease");
                poisoned.into_inner()
            }
        };
        let mut candidate = manager.clone();
        let (lease, event) = assign_work(&mut candidate)?;
        let acknowledgement_error = self.commit_networked_worker_lifecycle(
            &mut manager,
            &candidate,
            &[(event.clone(), Value::Null)],
        )?;
        if let Some(error) = acknowledgement_error {
            return Err(map_orchestrator_store_error(
                "acknowledge next networked worker lease assignment",
                error,
            ));
        }
        Ok((lease, event))
    }

    /// Assigns the next eligible worker only while the captured run generation remains current.
    ///
    /// A superseded generation commits one metadata-only diagnostic and leaves both the durable
    /// and in-memory fleet unchanged.
    ///
    /// # Errors
    /// Returns `invalid_argument` for mismatched run identity, `failed_precondition` when no worker
    /// can take the lease, or a mapped journal error when persistence fails.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn assign_next_networked_worker_lease_for_run(
        self: &Arc<Self>,
        request: WorkerLeaseRequest,
        session_id: &str,
        run_id: &str,
        generation: RuntimeGeneration,
    ) -> Result<NetworkedWorkerLeaseAssignmentOutcome, Status> {
        if session_id.trim().is_empty() || run_id.trim().is_empty() || request.run_id != run_id {
            return Err(Status::invalid_argument(
                "networked worker lease assignment run authority is invalid",
            ));
        }
        let Some(dispatcher) = self.networked_worker_remote_dispatcher() else {
            return Ok(NetworkedWorkerLeaseAssignmentOutcome::TransportRejected {
                reason: "remote worker transport is not configured".to_owned(),
            });
        };
        // Snapshot fleet ids before consulting Node state. Node callbacks acquire Node locks before
        // the fleet lock, so preflight must not invert that order. The filtered assignment below
        // revalidates the live fleet, and dispatch repeats transport readiness before any claim.
        let worker_ids = match self.worker_fleet.read() {
            Ok(manager) => manager.durable_records().into_keys().collect::<Vec<_>>(),
            Err(poisoned) => {
                warn!("worker fleet lock poisoned while preflighting run-owned lease");
                poisoned.into_inner().durable_records().into_keys().collect::<Vec<_>>()
            }
        };
        let mut compatible_worker_ids = BTreeSet::new();
        let mut first_preflight_error = None;
        for worker_id in worker_ids {
            match dispatcher
                .preflight_worker(worker_id.as_str(), request.required_capabilities.as_slice())
            {
                Ok(()) => {
                    compatible_worker_ids.insert(worker_id);
                }
                Err(error) => {
                    first_preflight_error.get_or_insert(error);
                }
            }
        }
        if compatible_worker_ids.is_empty() {
            return Ok(NetworkedWorkerLeaseAssignmentOutcome::TransportRejected {
                reason: first_preflight_error.map_or_else(
                    || "networked worker fleet has no transport-compatible worker".to_owned(),
                    |error| error.to_string(),
                ),
            });
        }
        let policy = self.worker_fleet_policy();
        let now_unix_ms = current_unix_ms();
        let mut manager = match self.worker_fleet.write() {
            Ok(manager) => manager,
            Err(poisoned) => {
                warn!("worker fleet lock poisoned while assigning run-owned lease");
                poisoned.into_inner()
            }
        };
        let mut candidate = manager.clone();
        let (lease, event) = candidate
            .assign_next_work_from_candidates(&compatible_worker_ids, request, &policy, now_unix_ms)
            .map_err(|error| {
                Status::failed_precondition(format!(
                    "networked worker lease assignment failed: {error}"
                ))
            })?;
        let transition_id = Ulid::new().to_string();
        let evidence = [self.prepare_networked_worker_lifecycle_evidence(
            transition_id.as_str(),
            &event,
            Value::Null,
        )?];
        let authority = crate::journal::NetworkedWorkerRunGenerationAuthority {
            session_id: session_id.to_owned(),
            run_id: run_id.to_owned(),
            generation,
        };
        match self.commit_prepared_networked_worker_lifecycle_with_run_authority(
            &mut manager,
            &candidate,
            &evidence,
            &[],
            None,
            Some(&authority),
        )? {
            PreparedNetworkedWorkerLifecycleCommitOutcome::Committed { acknowledgement_error } => {
                if let Some(error) = acknowledgement_error {
                    return Err(map_orchestrator_store_error(
                        "acknowledge run-owned networked worker lease assignment",
                        error,
                    ));
                }
                Ok(NetworkedWorkerLeaseAssignmentOutcome::Assigned { lease: Box::new(lease) })
            }
            PreparedNetworkedWorkerLifecycleCommitOutcome::StaleSuppressed => {
                Ok(NetworkedWorkerLeaseAssignmentOutcome::StaleSuppressed)
            }
        }
    }

    /// Finalizes a worker's lease and atomically revokes every exact dispatch claim it authorized.
    /// Incomplete cleanup is journaled as a non-recoverable orphan and surfaced as an error so
    /// scoped data leaks need operator action.
    ///
    /// # Errors
    /// `failed_precondition` when finalization is refused or cleanup left
    /// scoped data behind, plus journaling errors.
    #[allow(clippy::result_large_err)]
    #[allow(dead_code)]
    pub async fn complete_networked_worker_lease(
        self: &Arc<Self>,
        worker_id: &str,
        lease_identity: WorkerLeaseIdentity,
        cleanup_report: WorkerCleanupReport,
    ) -> Result<WorkerLifecycleEvent, Status> {
        let now_unix_ms = current_unix_ms();
        let mut manager = match self.worker_fleet.write() {
            Ok(manager) => manager,
            Err(poisoned) => {
                warn!("worker fleet lock poisoned while completing worker lease");
                poisoned.into_inner()
            }
        };
        let mut candidate = manager.clone();
        let outcome = candidate
            .finalize_work(worker_id, &lease_identity, cleanup_report, now_unix_ms)
            .map_err(|error| {
                Status::failed_precondition(format!("networked worker cleanup failed: {error}"))
            })?;
        let cleanup_report = redacted_networked_worker_cleanup_report(&outcome.cleanup_report);
        let details = json!({
            "cleanup_report": cleanup_report,
            "cleanup_succeeded": outcome.cleanup_succeeded,
            "orphan_classification": if outcome.cleanup_succeeded {
                "resolved"
            } else {
                "non_recoverable_requires_operator_cleanup"
            },
        });
        let revocation = crate::journal::NetworkedWorkerLeaseRevocation {
            worker_id: worker_id.to_owned(),
            lease_id: lease_identity.lease_id.clone(),
            run_id: lease_identity.run_id.clone(),
            reason_code: "worker.dispatch.revoked_by_lease_finalization".to_owned(),
        };
        let acknowledgement_error = self.commit_networked_worker_lifecycle_with_revocations(
            &mut manager,
            &candidate,
            &[(outcome.event.clone(), details)],
            std::slice::from_ref(&revocation),
        )?;
        if let Some(error) = acknowledgement_error {
            return Err(map_orchestrator_store_error(
                "acknowledge networked worker cleanup",
                error,
            ));
        }
        if outcome.cleanup_succeeded {
            Ok(outcome.event)
        } else {
            Err(Status::failed_precondition(
                "networked worker cleanup did not remove all scoped data",
            ))
        }
    }

    /// Settles a verified late result for a revoked in-flight claim without restoring fleet use.
    ///
    /// The caller must supply the exact delivery attempt and a trusted host observation timestamp
    /// from a durable late-result inbox or equivalent host-owned receipt boundary; worker-reported
    /// clocks are never settlement proof.
    ///
    /// # Errors
    /// Returns `failed_precondition` when the claim is absent, no longer reconciling, observed at
    /// or after lease expiry, or does not match the exact worker/run/lease identity.
    #[allow(clippy::result_large_err)]
    pub(crate) fn settle_reconciling_networked_worker_dispatch(
        &self,
        dispatch_identity: &NetworkedWorkerDispatchSettlementIdentity,
        worker_id: &str,
        lease_identity: &WorkerLeaseIdentity,
        validated_result_sha256: &str,
        observed_at_unix_ms: i64,
    ) -> Result<(), Status> {
        let settlement = crate::journal::NetworkedWorkerDispatchSettlement {
            remote_request_id: dispatch_identity.remote_request_id.clone(),
            worker_id: worker_id.to_owned(),
            lease_id: lease_identity.lease_id.clone(),
            session_id: dispatch_identity.session_id.clone(),
            run_id: lease_identity.run_id.clone(),
            run_generation: dispatch_identity.run_generation,
            delivery_attempt_id: dispatch_identity.delivery_attempt_id.clone(),
            validated_result_sha256: validated_result_sha256.to_owned(),
            observed_at_unix_ms,
        };
        match self.journal_store.settle_networked_worker_dispatch_claim(&settlement) {
            Ok(()) => Ok(()),
            Err(error @ JournalError::NetworkedWorkerDispatchSettlementRejected { .. }) => {
                self.record_networked_worker_stale_settlement_if_needed(&settlement);
                Err(map_orchestrator_store_error(
                    "settle reconciling networked worker dispatch",
                    error,
                ))
            }
            Err(error) => Err(map_orchestrator_store_error(
                "settle reconciling networked worker dispatch",
                error,
            )),
        }
    }

    /// Finalizes a verified remote worker result and its artifact receipt in one transaction.
    ///
    /// A node-backed result must include its exact dispatch and delivery-attempt identity so the
    /// in-flight claim settles atomically with fleet completion and evidence. Test-only or non-node
    /// dispatchers may omit it.
    /// A replay of the exact stable receipt is accepted without mutating a newer lease. Conflicting
    /// receipt evidence fails closed.
    ///
    /// # Errors
    /// Returns `failed_precondition` for invalid cleanup, stale lease identity, or conflicting
    /// receipt evidence, plus mapped journal errors.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn complete_networked_worker_result(
        self: &Arc<Self>,
        worker_id: &str,
        lease_identity: WorkerLeaseIdentity,
        cleanup_report: WorkerCleanupReport,
        receipt: NetworkedWorkerArtifactReceipt,
        dispatch_settlement: Option<NetworkedWorkerDispatchSettlementIdentity>,
    ) -> Result<WorkerLifecycleEvent, Status> {
        match self.complete_networked_worker_result_blocking(
            worker_id,
            lease_identity,
            cleanup_report,
            receipt,
            dispatch_settlement,
        )? {
            NetworkedWorkerResultCompletionOutcome::Completed(event) => Ok(event),
            NetworkedWorkerResultCompletionOutcome::StaleSuppressed => {
                Err(Status::failed_precondition(
                    "networked worker result belongs to a superseded run generation",
                ))
            }
        }
    }

    #[allow(clippy::result_large_err)]
    fn complete_networked_worker_result_blocking(
        &self,
        worker_id: &str,
        lease_identity: WorkerLeaseIdentity,
        cleanup_report: WorkerCleanupReport,
        receipt: NetworkedWorkerArtifactReceipt,
        dispatch_settlement: Option<NetworkedWorkerDispatchSettlementIdentity>,
    ) -> Result<NetworkedWorkerResultCompletionOutcome, Status> {
        validate_networked_worker_artifact_receipt(worker_id, &lease_identity, &receipt)?;
        let mut expected_receipt_input = receipt.clone();
        let mut settled_dispatch_replay = false;
        if let Some(identity) = dispatch_settlement.as_ref() {
            if identity.remote_request_id != receipt.request_id {
                return Err(Status::failed_precondition(
                    "networked worker result receipt conflicts with dispatch identity",
                ));
            }
            let dispatch_receipt = self
                .journal_store
                .networked_worker_dispatch_claim(identity.remote_request_id.as_str())
                .map_err(|error| {
                    map_orchestrator_store_error("inspect networked worker dispatch receipt", error)
                })?;
            if let Some(claim) = dispatch_receipt.as_ref() {
                if claim.state == crate::journal::NetworkedWorkerDispatchClaimState::Settled {
                    let exact_binding = claim.worker_id == worker_id
                        && claim.lease_id == lease_identity.lease_id
                        && claim.session_id.as_deref() == Some(identity.session_id.as_str())
                        && claim.run_id == lease_identity.run_id
                        && claim.run_generation == Some(identity.run_generation)
                        && claim.delivery_attempt_id == identity.delivery_attempt_id
                        && claim.validated_result_sha256.as_deref()
                            == Some(receipt.validated_result_sha256.as_str());
                    if !exact_binding {
                        return Err(Status::failed_precondition(
                            "networked worker result receipt conflicts with settled dispatch authority",
                        ));
                    }
                    if !cleanup_report.is_verified() {
                        return Err(Status::failed_precondition(
                            "networked worker result replay conflicts with verified cleanup evidence",
                        ));
                    }
                    expected_receipt_input.observed_at_unix_ms =
                        claim.result_observed_at_unix_ms.ok_or_else(|| {
                            Status::failed_precondition(
                            "settled networked worker dispatch is missing result receipt evidence",
                        )
                        })?;
                    settled_dispatch_replay = true;
                }
            }
        }
        let expected_receipt = self.networked_worker_artifact_journal_request(
            worker_id,
            &lease_identity,
            &expected_receipt_input,
        )?;
        let transition_id = networked_worker_result_transition_id(
            worker_id,
            &lease_identity,
            &expected_receipt_input,
        );
        let expected_completion = self.networked_worker_lifecycle_journal_request(
            transition_id.as_str(),
            &networked_worker_completed_event_from_receipt(
                worker_id,
                &lease_identity,
                &expected_receipt_input,
            ),
            networked_worker_completion_details(),
        )?;
        let existing_receipt = self
            .journal_store
            .event_by_id(expected_receipt.event_id.as_str())
            .map_err(|error| {
            map_orchestrator_store_error("inspect networked worker result replay", error)
        })?;
        let existing_completion = self
            .journal_store
            .event_by_id(expected_completion.event_id.as_str())
            .map_err(|error| {
                map_orchestrator_store_error("inspect networked worker completion replay", error)
            })?;
        if settled_dispatch_replay || existing_receipt.is_some() || existing_completion.is_some() {
            let exact_receipt = match existing_receipt.as_ref() {
                Some(existing) => {
                    networked_worker_journal_event_matches(existing, &expected_receipt)?
                }
                None => false,
            };
            let exact_completion = match existing_completion.as_ref() {
                Some(existing) => {
                    networked_worker_journal_event_matches(existing, &expected_completion)?
                }
                None => false,
            };
            let durable = self
                .journal_store
                .list_networked_worker_fleet_records(NETWORKED_WORKER_FLEET_MAX_ENTRIES)
                .map_err(|error| {
                    map_orchestrator_store_error("inspect networked worker result fleet", error)
                })?;
            let exact_completed_state = durable.get(worker_id).is_some_and(|record| {
                record.state == palyra_common::runtime_contracts::WorkerLifecycleState::Completed
                    && record.lease.is_none()
            });
            if exact_receipt && exact_completion && exact_completed_state {
                return Ok(NetworkedWorkerResultCompletionOutcome::Completed(
                    networked_worker_completed_event_from_receipt(
                        worker_id,
                        &lease_identity,
                        &expected_receipt_input,
                    ),
                ));
            }
            return Err(Status::failed_precondition(format!(
                "networked worker result evidence conflicts with durable state: {}",
                expected_receipt.event_id
            )));
        }

        let mut manager = match self.worker_fleet.write() {
            Ok(manager) => manager,
            Err(poisoned) => {
                warn!("worker fleet lock poisoned while completing remote worker result");
                poisoned.into_inner()
            }
        };
        let mut candidate = manager.clone();
        let outcome = candidate
            .finalize_work(worker_id, &lease_identity, cleanup_report, receipt.observed_at_unix_ms)
            .map_err(|error| {
                Status::failed_precondition(format!("networked worker cleanup failed: {error}"))
            })?;
        if !outcome.cleanup_succeeded {
            return Err(Status::failed_precondition(
                "networked worker cleanup did not remove all scoped data",
            ));
        }
        if outcome.event.state != palyra_common::runtime_contracts::WorkerLifecycleState::Completed
        {
            return Err(Status::failed_precondition(
                "networked worker result cannot attest a fail-closed worker state",
            ));
        }

        let cleanup_details = networked_worker_completion_details();
        let mut evidence = vec![self.prepare_networked_worker_lifecycle_evidence(
            transition_id.as_str(),
            &outcome.event,
            cleanup_details,
        )?];
        let artifact_payload =
            networked_worker_artifact_payload(worker_id, &lease_identity, &receipt);
        evidence.push(PreparedNetworkedWorkerLifecycleEvidence {
            commit: crate::journal::NetworkedWorkerLifecycleCommit {
                request: expected_receipt,
                event: outcome.event.clone(),
            },
            payload: artifact_payload,
        });
        let settlement =
            dispatch_settlement.map(|identity| crate::journal::NetworkedWorkerDispatchSettlement {
                remote_request_id: identity.remote_request_id,
                worker_id: worker_id.to_owned(),
                lease_id: lease_identity.lease_id.clone(),
                session_id: identity.session_id,
                run_id: lease_identity.run_id.clone(),
                run_generation: identity.run_generation,
                delivery_attempt_id: identity.delivery_attempt_id,
                validated_result_sha256: receipt.validated_result_sha256.clone(),
                observed_at_unix_ms: receipt.observed_at_unix_ms,
            });
        let run_authority = settlement.as_ref().map(|settlement| {
            crate::journal::NetworkedWorkerRunGenerationAuthority {
                session_id: settlement.session_id.clone(),
                run_id: settlement.run_id.clone(),
                generation: settlement.run_generation,
            }
        });
        let acknowledgement_error = match self
            .commit_prepared_networked_worker_lifecycle_with_run_authority(
                &mut manager,
                &candidate,
                evidence.as_slice(),
                &[],
                settlement.as_ref(),
                run_authority.as_ref(),
            )? {
            PreparedNetworkedWorkerLifecycleCommitOutcome::Committed { acknowledgement_error } => {
                acknowledgement_error
            }
            PreparedNetworkedWorkerLifecycleCommitOutcome::StaleSuppressed => {
                self.managed_runtime_health_stale_suppressions.fetch_add(1, Ordering::Relaxed);
                return Ok(NetworkedWorkerResultCompletionOutcome::StaleSuppressed);
            }
        };
        if let Some(error) = acknowledgement_error {
            return Err(map_orchestrator_store_error("acknowledge networked worker result", error));
        }
        Ok(NetworkedWorkerResultCompletionOutcome::Completed(outcome.event))
    }

    /// Expires workers past their lease TTL, journaling each event with
    /// recovery guidance for operators.
    ///
    /// Manual and periodic callers share one async owner so pending evidence
    /// is durable before another expiry pass changes fleet state.
    ///
    /// # Errors
    /// Returns a fail-closed reconciliation error or a lifecycle journaling
    /// error. Revoked leases remain represented in the bounded pending ledger
    /// until exact durable evidence exists.
    #[allow(clippy::result_large_err)]
    #[allow(dead_code)]
    pub async fn reap_expired_networked_workers(
        self: &Arc<Self>,
    ) -> Result<Vec<WorkerLifecycleEvent>, Status> {
        self.reap_expired_networked_workers_at(current_unix_ms()).await
    }

    #[allow(clippy::result_large_err)]
    pub(crate) async fn reap_expired_networked_workers_at(
        self: &Arc<Self>,
        now_unix_ms: i64,
    ) -> Result<Vec<WorkerLifecycleEvent>, Status> {
        let _guard = self.networked_worker_expiry_lock.lock().await;
        #[cfg(test)]
        let _activity = NetworkedWorkerExpiryActivity::begin(self.as_ref());
        self.reap_expired_networked_workers_serialized(now_unix_ms).await
    }

    #[allow(clippy::result_large_err)]
    async fn reap_expired_networked_workers_serialized(
        self: &Arc<Self>,
        now_unix_ms: i64,
    ) -> Result<Vec<WorkerLifecycleEvent>, Status> {
        if let Err(error) = self.persist_pending_networked_worker_expiry().await {
            warn!(error = %error, "pending worker expiry evidence remains undrained; continuing bounded lease revocation");
        }
        let available_capacity = PENDING_NETWORKED_WORKER_EXPIRY_MAX_ENTRIES
            .saturating_sub(self.pending_networked_worker_expiry_count()?);
        let events = {
            let mut manager = match self.worker_fleet.write() {
                Ok(manager) => manager,
                Err(poisoned) => {
                    warn!("worker fleet lock poisoned while expiring workers");
                    poisoned.into_inner()
                }
            };
            let plan = manager
                .plan_expired_workers_bounded(now_unix_ms, available_capacity)
                .map_err(|error| {
                    Status::failed_precondition(format!(
                        "networked worker expiry planning failed: {error}"
                    ))
                })?;
            if plan.events().is_empty() {
                return Ok(Vec::new());
            }
            let entries = plan
                .events()
                .iter()
                .cloned()
                .map(PendingNetworkedWorkerExpiry::new)
                .collect::<Result<Vec<_>, _>>()?;
            let records =
                entries.iter().map(PendingNetworkedWorkerExpiry::outbox_record).collect::<Vec<_>>();
            let mut candidate = manager.clone();
            let events = candidate.apply_expired_worker_plan(plan).map_err(|error| {
                Status::failed_precondition(format!(
                    "networked worker expiry reconciliation failed: {error}"
                ))
            })?;
            let expected_generation = self.worker_fleet_generation.load(Ordering::Relaxed);
            let generation = match self.journal_store.commit_networked_worker_expiry_plan(
                records.as_slice(),
                &candidate.durable_records(),
                expected_generation,
                PENDING_NETWORKED_WORKER_EXPIRY_MAX_ENTRIES,
                NETWORKED_WORKER_FLEET_MAX_ENTRIES,
                current_unix_ms(),
            ) {
                Ok(generation) => generation,
                Err(error @ JournalError::NetworkedWorkerFleetGenerationConflict { .. }) => {
                    self.reload_networked_worker_fleet_after_conflict(&mut manager)?;
                    return Err(map_orchestrator_store_error(
                        "persist networked worker expiry state",
                        error,
                    ));
                }
                Err(error) => {
                    return Err(map_orchestrator_store_error(
                        "persist networked worker expiry state",
                        error,
                    ));
                }
            };
            self.worker_fleet_generation.store(generation, Ordering::Relaxed);
            if let Err(error) = self.retain_pending_networked_worker_expiry(entries) {
                manager.restore_durable_records(candidate.durable_records()).map_err(
                    |restore_error| {
                        Status::failed_precondition(format!(
                            "networked worker expiry state restore failed: {restore_error}"
                        ))
                    },
                )?;
                for event in &events {
                    manager.retain_recent_event(event.clone());
                }
                return Err(error);
            }
            *manager = candidate;
            events
        };
        self.persist_pending_networked_worker_expiry().await?;
        Ok(events)
    }

    #[allow(clippy::result_large_err)]
    fn pending_networked_worker_expiry_count(&self) -> Result<usize, Status> {
        self.pending_networked_worker_expiry.lock().map(|pending| pending.len()).map_err(|error| {
            Status::internal(format!(
                "pending networked worker expiry registry lock poisoned: {error}"
            ))
        })
    }

    #[allow(clippy::result_large_err)]
    fn retain_pending_networked_worker_expiry(
        &self,
        entries: Vec<PendingNetworkedWorkerExpiry>,
    ) -> Result<(), Status> {
        let mut pending = self.pending_networked_worker_expiry.lock().map_err(|error| {
            Status::internal(format!(
                "pending networked worker expiry registry lock poisoned: {error}"
            ))
        })?;
        for entry in entries {
            let key = entry.key().to_owned();
            if let Some(existing) = pending.get(key.as_str()) {
                if existing != &entry {
                    return Err(Status::failed_precondition(
                        "pending networked worker expiry key conflicts with different evidence",
                    ));
                }
            } else {
                pending.insert(key, entry);
            }
        }
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    async fn persist_pending_networked_worker_expiry(self: &Arc<Self>) -> Result<(), Status> {
        let mut entries = self
            .pending_networked_worker_expiry
            .lock()
            .map_err(|error| {
                Status::internal(format!(
                    "pending networked worker expiry registry lock poisoned: {error}"
                ))
            })?
            .values()
            .cloned()
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| {
            left.event
                .timestamp_unix_ms
                .cmp(&right.event.timestamp_unix_ms)
                .then_with(|| left.event.worker_id.cmp(&right.event.worker_id))
                .then_with(|| left.event.run_id.cmp(&right.event.run_id))
                .then_with(|| left.event.lease_id.cmp(&right.event.lease_id))
        });
        let mut first_error = None;
        for entry in entries {
            let result = async {
                self.record_networked_worker_expiry_event(&entry.event).await?;
                let key = entry.key().to_owned();
                self.journal_store
                    .remove_networked_worker_expiry_outbox(key.as_str(), &entry.event)
                    .map_err(|error| {
                        map_orchestrator_store_error(
                            "retire networked worker expiry evidence",
                            error,
                        )
                    })?;
                let mut pending = self.pending_networked_worker_expiry.lock().map_err(|error| {
                    Status::internal(format!(
                        "pending networked worker expiry registry lock poisoned: {error}"
                    ))
                })?;
                if pending.get(key.as_str()) == Some(&entry) {
                    pending.remove(key.as_str());
                }
                Ok::<(), Status>(())
            }
            .await;
            if let Err(error) = result {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    #[cfg(test)]
    pub(crate) fn pending_networked_worker_expiry_evidence_count(&self) -> usize {
        self.pending_networked_worker_expiry
            .lock()
            .map(|pending| pending.len())
            .unwrap_or(PENDING_NETWORKED_WORKER_EXPIRY_MAX_ENTRIES)
    }

    #[allow(clippy::result_large_err)]
    async fn record_networked_worker_expiry_event(
        self: &Arc<Self>,
        event: &WorkerLifecycleEvent,
    ) -> Result<(), Status> {
        self.record_networked_worker_lifecycle_event_with_details(
            event,
            json!({
                "orphan_classification": "recoverable_requires_force_cleanup",
                "recommended_actions": [
                    "force_cleanup",
                    "reverify",
                    "quarantine"
                ],
            }),
        )
        .await
    }

    #[cfg(test)]
    pub(crate) fn networked_worker_expiry_max_active(&self) -> u64 {
        self.networked_worker_expiry_max_active.load(Ordering::SeqCst)
    }

    /// Quarantines every worker (operator drain) and journals the events.
    ///
    /// # Errors
    /// Journaling errors from recording the lifecycle events.
    #[allow(clippy::result_large_err)]
    #[allow(dead_code)]
    pub async fn drain_networked_workers(
        self: &Arc<Self>,
    ) -> Result<Vec<WorkerLifecycleEvent>, Status> {
        let now_unix_ms = current_unix_ms();
        let mut manager = match self.worker_fleet.write() {
            Ok(manager) => manager,
            Err(poisoned) => {
                warn!("worker fleet lock poisoned while draining workers");
                poisoned.into_inner()
            }
        };
        let mut candidate = manager.clone();
        let events = candidate.quarantine_all_workers("worker.drained_by_operator", now_unix_ms);
        if events.is_empty() {
            return Ok(events);
        }
        let evidence = events
            .iter()
            .cloned()
            .map(|event| {
                (
                    event,
                    json!({
                        "operator_action": "drain",
                        "cleanup_required": true,
                    }),
                )
            })
            .collect::<Vec<_>>();
        let revocations = exact_networked_worker_lease_revocations(
            &manager,
            events.as_slice(),
            "worker.dispatch.revoked_by_operator_drain",
        );
        let acknowledgement_error = self.commit_networked_worker_lifecycle_with_revocations(
            &mut manager,
            &candidate,
            evidence.as_slice(),
            revocations.as_slice(),
        )?;
        if let Some(error) = acknowledgement_error {
            return Err(map_orchestrator_store_error("acknowledge networked worker drain", error));
        }
        Ok(events)
    }

    /// Quarantines one worker by operator action and journals the event.
    ///
    /// # Errors
    /// `failed_precondition` when the worker cannot be quarantined, plus
    /// journaling errors.
    #[allow(clippy::result_large_err)]
    #[allow(dead_code)]
    pub async fn quarantine_networked_worker(
        self: &Arc<Self>,
        worker_id: &str,
    ) -> Result<WorkerLifecycleEvent, Status> {
        let now_unix_ms = current_unix_ms();
        let mut manager = match self.worker_fleet.write() {
            Ok(manager) => manager,
            Err(poisoned) => {
                warn!("worker fleet lock poisoned while quarantining worker");
                poisoned.into_inner()
            }
        };
        let mut candidate = manager.clone();
        let event = candidate
            .quarantine_worker(worker_id, "worker.quarantined_by_operator", now_unix_ms)
            .map_err(|error| {
                Status::failed_precondition(format!("networked worker quarantine failed: {error}"))
            })?;
        let revocations = exact_networked_worker_lease_revocations(
            &manager,
            std::slice::from_ref(&event),
            "worker.dispatch.revoked_by_operator_quarantine",
        );
        let acknowledgement_error = self.commit_networked_worker_lifecycle_with_revocations(
            &mut manager,
            &candidate,
            &[(event.clone(), json!({ "operator_action": "quarantine" }))],
            revocations.as_slice(),
        )?;
        if let Some(error) = acknowledgement_error {
            return Err(map_orchestrator_store_error(
                "acknowledge networked worker quarantine",
                error,
            ));
        }
        Ok(event)
    }

    /// Re-runs attestation checks for a quarantined worker and journals the
    /// outcome.
    ///
    /// # Errors
    /// `failed_precondition` when re-verification fails, plus journaling
    /// errors.
    #[allow(clippy::result_large_err)]
    #[allow(dead_code)]
    pub async fn reverify_networked_worker(
        self: &Arc<Self>,
        worker_id: &str,
    ) -> Result<WorkerLifecycleEvent, Status> {
        let policy = self.worker_fleet_policy();
        let now_unix_ms = current_unix_ms();
        let mut manager = match self.worker_fleet.write() {
            Ok(manager) => manager,
            Err(poisoned) => {
                warn!("worker fleet lock poisoned while re-verifying worker");
                poisoned.into_inner()
            }
        };
        let mut candidate = manager.clone();
        let event =
            candidate.reverify_worker(worker_id, &policy, now_unix_ms).map_err(|error| {
                Status::failed_precondition(format!(
                    "networked worker re-verification failed: {error}"
                ))
            })?;
        let acknowledgement_error = self.commit_networked_worker_lifecycle(
            &mut manager,
            &candidate,
            &[(event.clone(), json!({ "operator_action": "reverify" }))],
        )?;
        if let Some(error) = acknowledgement_error {
            return Err(map_orchestrator_store_error(
                "acknowledge networked worker re-verification",
                error,
            ));
        }
        Ok(event)
    }

    /// Operator-forced cleanup of an orphaned worker; fails closed when the
    /// cleanup report shows leftover scoped data.
    ///
    /// # Errors
    /// `failed_precondition` when cleanup is refused or incomplete, plus
    /// journaling errors.
    #[allow(clippy::result_large_err)]
    #[allow(dead_code)]
    pub async fn force_cleanup_networked_worker(
        self: &Arc<Self>,
        worker_id: &str,
        cleanup_report: WorkerCleanupReport,
    ) -> Result<WorkerLifecycleEvent, Status> {
        let now_unix_ms = current_unix_ms();
        let force_cleanup = |manager: &mut WorkerFleetManager| {
            manager.force_cleanup_worker(worker_id, cleanup_report.clone(), now_unix_ms).map_err(
                |error| {
                    Status::failed_precondition(format!(
                        "networked worker force cleanup failed: {error}"
                    ))
                },
            )
        };
        let mut manager = match self.worker_fleet.write() {
            Ok(manager) => manager,
            Err(poisoned) => {
                warn!("worker fleet lock poisoned while force-cleaning worker");
                poisoned.into_inner()
            }
        };
        let mut candidate = manager.clone();
        let outcome = force_cleanup(&mut candidate)?;
        let cleanup_report = redacted_networked_worker_cleanup_report(&outcome.cleanup_report);
        let details = json!({
            "operator_action": "force_cleanup",
            "cleanup_report": cleanup_report,
            "cleanup_succeeded": outcome.cleanup_succeeded,
            "orphan_classification": if outcome.cleanup_succeeded {
                "resolved"
            } else {
                "non_recoverable_requires_operator_cleanup"
            },
        });
        let acknowledgement_error = self.commit_networked_worker_lifecycle(
            &mut manager,
            &candidate,
            &[(outcome.event.clone(), details)],
        )?;
        if let Some(error) = acknowledgement_error {
            return Err(map_orchestrator_store_error(
                "acknowledge networked worker force cleanup",
                error,
            ));
        }
        if outcome.cleanup_succeeded {
            Ok(outcome.event)
        } else {
            Err(Status::failed_precondition(
                "networked worker force cleanup did not remove all scoped data",
            ))
        }
    }

    fn networked_worker_lifecycle_payload(
        &self,
        event: &WorkerLifecycleEvent,
        extra_details: Value,
    ) -> RuntimeDecisionPayload {
        let mut details = json!({
            "run_id": event.run_id,
            "lease_id": event.lease_id,
            "reason_code": event.reason_code,
            "state": event.state.as_str(),
        });
        if let (Some(details), Value::Object(extra)) = (details.as_object_mut(), extra_details) {
            for (key, value) in extra {
                details.insert(key, value);
            }
        }
        RuntimeDecisionPayload::new(
            RuntimeDecisionEventType::WorkerLeaseLifecycle,
            RuntimeDecisionActor::new(
                RuntimeDecisionActorKind::Worker,
                "system:networked-worker",
                "networked-worker",
                Some("system".to_owned()),
            ),
            event.reason_code.clone(),
            "networked_workers.lease.preview",
            RuntimeDecisionTiming::observed(event.timestamp_unix_ms),
        )
        .with_input(RuntimeEntityRef::new("worker", "worker", event.worker_id.clone()))
        .with_output(
            RuntimeEntityRef::new("worker_lifecycle", "worker", event.worker_id.clone())
                .with_state(event.state.as_str()),
        )
        .with_resource_budget(RuntimeResourceBudget::default())
        .with_details(details)
    }

    #[allow(clippy::result_large_err)]
    fn networked_worker_lifecycle_journal_request(
        &self,
        transition_id: &str,
        event: &WorkerLifecycleEvent,
        extra_details: Value,
    ) -> Result<JournalAppendRequest, Status> {
        let payload = self.networked_worker_lifecycle_payload(event, extra_details);
        let scope_id = event.run_id.clone().unwrap_or_else(|| transition_id.to_owned());
        Ok(JournalAppendRequest {
            event_id: palyra_workerd::networked_worker_lifecycle_event_id(transition_id, event)
                .map_err(|error| {
                    Status::failed_precondition(format!(
                        "networked worker lifecycle event identity is invalid: {error}"
                    ))
                })?,
            session_id: scope_id.clone(),
            run_id: scope_id,
            kind: common_v1::journal_event::EventKind::ToolExecuted as i32,
            actor: common_v1::journal_event::EventActor::System as i32,
            timestamp_unix_ms: event.timestamp_unix_ms,
            payload_json: json!({
                "event": payload.event_type.journal_event(),
                "payload": payload,
            })
            .to_string()
            .into_bytes(),
            principal: "system:networked-worker".to_owned(),
            device_id: "networked-worker".to_owned(),
            channel: Some("system".to_owned()),
        })
    }

    #[allow(clippy::result_large_err)]
    fn networked_worker_artifact_journal_request(
        &self,
        worker_id: &str,
        lease_identity: &WorkerLeaseIdentity,
        receipt: &NetworkedWorkerArtifactReceipt,
    ) -> Result<JournalAppendRequest, Status> {
        validate_networked_worker_artifact_receipt(worker_id, lease_identity, receipt)?;
        let payload = networked_worker_artifact_payload(worker_id, lease_identity, receipt);
        Ok(JournalAppendRequest {
            event_id: networked_worker_artifact_event_id(worker_id, lease_identity, receipt),
            session_id: receipt.session_id.clone(),
            run_id: receipt.run_id.clone(),
            kind: common_v1::journal_event::EventKind::ToolExecuted as i32,
            actor: common_v1::journal_event::EventActor::System as i32,
            timestamp_unix_ms: receipt.observed_at_unix_ms,
            payload_json: json!({
                "event": payload.event_type.journal_event(),
                "payload": payload,
            })
            .to_string()
            .into_bytes(),
            principal: receipt.principal.clone(),
            device_id: receipt.device_id.clone(),
            channel: receipt.channel.clone(),
        })
    }

    #[allow(clippy::result_large_err)]
    async fn record_networked_worker_lifecycle_event_with_details(
        self: &Arc<Self>,
        event: &WorkerLifecycleEvent,
        extra_details: Value,
    ) -> Result<(), Status> {
        let payload = self.networked_worker_lifecycle_payload(event, extra_details);
        self.append_runtime_decision_event_with_id(
            networked_worker_expiry_event_id(event).map_err(|error| {
                Status::failed_precondition(format!(
                    "networked worker expiry event identity is invalid: {error}"
                ))
            })?,
            "system:networked-worker".to_owned(),
            "networked-worker".to_owned(),
            Some("system".to_owned()),
            event.run_id.clone(),
            event.run_id.clone(),
            payload,
        )
        .await
    }

    // In-memory caches: memory search results and tool approval decisions.

    /// Records metadata from a successful workspace read for stale-edit guards.
    pub(crate) fn record_workspace_file_view(&self, record: WorkspaceFileViewRecord) {
        match self.file_view_registry.lock() {
            Ok(mut registry) => registry.record_read(record),
            Err(poisoned) => {
                warn!("file view registry lock poisoned while recording read");
                let mut registry = poisoned.into_inner();
                registry.record_read(record);
            }
        }
    }

    /// Evaluates a pending patch against the current per-run file view registry.
    pub(crate) fn evaluate_workspace_patch_file_view_guard(
        &self,
        run_id: &str,
        patch_text: &str,
    ) -> WorkspacePatchFileViewReport {
        match self.file_view_registry.lock() {
            Ok(registry) => registry.evaluate_patch(run_id, patch_text),
            Err(poisoned) => {
                warn!("file view registry lock poisoned while evaluating patch");
                let registry = poisoned.into_inner();
                registry.evaluate_patch(run_id, patch_text)
            }
        }
    }

    /// Returns a host guardrail decision for a repeated failing tool call.
    pub(crate) fn before_tool_guardrail_decision(
        &self,
        run_id: &str,
        signature: &ToolCallSignature,
    ) -> Option<ToolGuardrailDecision> {
        match self.tool_guardrails.lock() {
            Ok(registry) => {
                registry.get(run_id).and_then(|controller| controller.before_tool(signature))
            }
            Err(poisoned) => {
                warn!("tool guardrail registry lock poisoned while evaluating proposal");
                let registry = poisoned.into_inner();
                registry.get(run_id).and_then(|controller| controller.before_tool(signature))
            }
        }
    }

    /// Records one tool result for per-run repeated-failure guardrails.
    pub(crate) fn record_tool_guardrail_result(
        &self,
        run_id: &str,
        signature: &ToolCallSignature,
        success: bool,
        failure_reason: Option<&str>,
    ) {
        match self.tool_guardrails.lock() {
            Ok(mut registry) => {
                let controller = registry.entry(run_id.to_owned()).or_default();
                controller.observe_tool_result(signature, success, failure_reason);
            }
            Err(poisoned) => {
                warn!("tool guardrail registry lock poisoned while recording result");
                let mut registry = poisoned.into_inner();
                let controller = registry.entry(run_id.to_owned()).or_default();
                controller.observe_tool_result(signature, success, failure_reason);
            }
        }
    }

    /// Drops every cached memory search result. Called on any write that can
    /// change search outcomes (config changes, item mutations, maintenance).
    pub fn clear_memory_search_cache(&self) {
        match self.memory_search_cache.lock() {
            Ok(mut cache) => {
                cache.clear();
            }
            Err(poisoned) => {
                warn!("memory search cache lock poisoned while clearing cache");
                let mut cache = poisoned.into_inner();
                cache.clear();
            }
        }
    }

    /// Removes cached approval decisions for one session scope and bumps the
    /// scope's generation so in-flight `remember_*_if_generation` writes that
    /// raced this invalidation are discarded.
    pub(crate) fn clear_tool_approval_cache_for_session(
        &self,
        context: &RequestContext,
        session_id: &str,
    ) {
        let key_prefix = tool_approval_cache_key_prefix(context, session_id);
        match self.tool_approval_cache.lock() {
            Ok(mut cache) => {
                cache.decisions.retain(|key, _| !key.starts_with(key_prefix.as_str()));
                bump_tool_approval_cache_generation(&mut cache, key_prefix.as_str());
            }
            Err(poisoned) => {
                warn!("tool approval cache lock poisoned while clearing session cache");
                let mut cache = poisoned.into_inner();
                cache.decisions.retain(|key, _| !key.starts_with(key_prefix.as_str()));
                bump_tool_approval_cache_generation(&mut cache, key_prefix.as_str());
            }
        }
    }

    /// Current cache generation for the session scope. Read this before an
    /// approval wait and pass it to
    /// [`Self::remember_tool_approval_if_generation`] afterwards.
    pub(crate) fn tool_approval_cache_generation_for_session(
        &self,
        context: &RequestContext,
        session_id: &str,
    ) -> u64 {
        let key_prefix = tool_approval_cache_key_prefix(context, session_id);
        match self.tool_approval_cache.lock() {
            Ok(cache) => tool_approval_cache_generation(&cache, key_prefix.as_str()),
            Err(poisoned) => {
                warn!("tool approval cache lock poisoned while reading session cache generation");
                let cache = poisoned.into_inner();
                tool_approval_cache_generation(&cache, key_prefix.as_str())
            }
        }
    }

    /// Returns a previously remembered decision for the subject, pruning
    /// expired entries first; the returned TTL is the remaining lifetime.
    pub(crate) fn resolve_cached_tool_approval(
        &self,
        context: &RequestContext,
        session_id: &str,
        subject_id: &str,
    ) -> Option<ToolApprovalOutcome> {
        let now_unix_ms = current_unix_ms();
        let cache_key = tool_approval_cache_key(context, session_id, subject_id);
        let resolve_from_cache =
            |cache: &mut ToolApprovalCacheState| -> Option<ToolApprovalOutcome> {
                cache.decisions.retain(|_, entry| match entry.expires_at_unix_ms {
                    Some(expires_at_unix_ms) => expires_at_unix_ms > now_unix_ms,
                    None => true,
                });
                let cached = cache.decisions.get(cache_key.as_str())?.clone();
                let remaining_ttl_ms = cached
                    .expires_at_unix_ms
                    .map(|expires_at_unix_ms| expires_at_unix_ms.saturating_sub(now_unix_ms))
                    .filter(|remaining| *remaining > 0);
                Some(ToolApprovalOutcome {
                    approval_id: cached.approval_id,
                    approved: cached.approved,
                    reason: format!(
                        "cached_approval(scope={}): {}",
                        cached.decision_scope.as_str(),
                        cached.reason
                    ),
                    decision: cached.decision,
                    decision_scope: cached.decision_scope,
                    decision_scope_ttl_ms: remaining_ttl_ms,
                })
            };
        match self.tool_approval_cache.lock() {
            Ok(mut cache) => resolve_from_cache(&mut cache),
            Err(poisoned) => {
                warn!("tool approval cache lock poisoned while resolving cached decision");
                let mut cache = poisoned.into_inner();
                resolve_from_cache(&mut cache)
            }
        }
    }

    /// Caches a decision unconditionally (no generation check); used when the
    /// decision is fresh and no invalidation race is possible.
    pub(crate) fn remember_tool_approval(
        &self,
        context: &RequestContext,
        session_id: &str,
        subject_id: &str,
        outcome: &ToolApprovalOutcome,
    ) {
        let _remembered = self
            .remember_tool_approval_if_generation(context, session_id, subject_id, outcome, None);
    }

    /// Caches an Allow/Deny decision for `Session`/`Timeboxed` scopes
    /// (`Once` decisions and timeboxed entries without a positive TTL are
    /// never cached). When `expected_generation` is set, the write is dropped
    /// if the session cache was invalidated while the approval was pending,
    /// so a stale decision cannot resurrect after a cache clear. Returns
    /// whether the decision was stored.
    pub(crate) fn remember_tool_approval_if_generation(
        &self,
        context: &RequestContext,
        session_id: &str,
        subject_id: &str,
        outcome: &ToolApprovalOutcome,
        expected_generation: Option<u64>,
    ) -> bool {
        if !matches!(outcome.decision, ApprovalDecision::Allow | ApprovalDecision::Deny) {
            return false;
        }
        let now_unix_ms = current_unix_ms();
        let expires_at_unix_ms = match outcome.decision_scope {
            ApprovalDecisionScope::Once => return false,
            ApprovalDecisionScope::Session => outcome
                .decision_scope_ttl_ms
                .filter(|ttl_ms| *ttl_ms > 0)
                .map(|ttl_ms| now_unix_ms.saturating_add(ttl_ms)),
            ApprovalDecisionScope::Timeboxed => {
                let Some(ttl_ms) = outcome.decision_scope_ttl_ms.filter(|ttl_ms| *ttl_ms > 0)
                else {
                    warn!(
                        approval_id = %outcome.approval_id,
                        "ignoring timeboxed approval memory entry without positive ttl"
                    );
                    return false;
                };
                Some(now_unix_ms.saturating_add(ttl_ms))
            }
        };
        let cache_key = tool_approval_cache_key(context, session_id, subject_id);
        let generation_key = tool_approval_cache_key_prefix(context, session_id);
        let cache_entry = CachedToolApprovalDecision {
            approval_id: outcome.approval_id.clone(),
            approved: outcome.approved,
            reason: outcome.reason.clone(),
            decision: outcome.decision,
            decision_scope: outcome.decision_scope,
            expires_at_unix_ms,
        };
        let remember_in_cache = |cache: &mut ToolApprovalCacheState| -> bool {
            if let Some(expected_generation) = expected_generation {
                let current_generation =
                    tool_approval_cache_generation(cache, generation_key.as_str());
                if current_generation != expected_generation {
                    return false;
                }
            }
            cache.decisions.retain(|_, entry| match entry.expires_at_unix_ms {
                Some(entry_expires_at_unix_ms) => entry_expires_at_unix_ms > now_unix_ms,
                None => true,
            });
            // Capacity eviction removes an arbitrary entry (HashMap iteration
            // order), not LRU: this is a size bound, not a hit-rate strategy.
            if cache.decisions.len() >= APPROVAL_DECISION_CACHE_CAPACITY {
                if let Some(first_key) = cache.decisions.keys().next().cloned() {
                    cache.decisions.remove(first_key.as_str());
                }
            }
            cache.decisions.insert(cache_key.clone(), cache_entry.clone());
            true
        };
        match self.tool_approval_cache.lock() {
            Ok(mut cache) => remember_in_cache(&mut cache),
            Err(poisoned) => {
                warn!("tool approval cache lock poisoned while recording decision");
                let mut cache = poisoned.into_inner();
                remember_in_cache(&mut cache)
            }
        }
    }

    // Memory items, memory search (cached and diagnostic variants), and
    // workspace documents.

    /// Validates content limits, applies the default TTL when none is set,
    /// persists the item, and invalidates the memory search cache.
    ///
    /// # Errors
    /// `invalid_argument` when content exceeds the configured byte/token
    /// limits (the rejection counter is bumped), otherwise the mapped memory
    /// store error or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn ingest_memory_item(
        self: &Arc<Self>,
        mut request: MemoryItemCreateRequest,
    ) -> Result<MemoryItemRecord, Status> {
        let config = self.memory_config_snapshot();
        if let Err(status) =
            validate_memory_item_content_limits(request.content_text.as_str(), &config)
        {
            self.counters.memory_items_rejected.fetch_add(1, Ordering::Relaxed);
            return Err(status);
        }
        if request.ttl_unix_ms.is_none() {
            if let Some(default_ttl_ms) = config.default_ttl_ms {
                let now = current_unix_ms_status()?;
                request.ttl_unix_ms = Some(now.saturating_add(default_ttl_ms));
            }
        }

        let state = Arc::clone(self);
        let created = tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .create_memory_item(&request)
                .map_err(|error| map_memory_store_error("ingest memory item", error))
        })
        .await
        .map_err(|_| Status::internal("memory ingest worker panicked"))??;
        self.counters.memory_items_ingested.fetch_add(1, Ordering::Relaxed);
        self.clear_memory_search_cache();
        Ok(created)
    }

    /// Loads a memory item by id.
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn memory_item(
        self: &Arc<Self>,
        memory_id: String,
    ) -> Result<Option<MemoryItemRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .memory_item(memory_id.as_str())
                .map_err(|error| map_memory_store_error("load memory item", error))
        })
        .await
        .map_err(|_| Status::internal("memory read worker panicked"))?
    }

    /// Applies a lifecycle update, revalidating content limits when the text
    /// changes, and invalidates the search cache when something was updated.
    ///
    /// # Errors
    /// `invalid_argument` for content over limits, otherwise the mapped
    /// memory store error or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn update_memory_item_lifecycle(
        self: &Arc<Self>,
        request: MemoryItemLifecycleUpdateRequest,
    ) -> Result<Option<MemoryItemRecord>, Status> {
        if let Some(content_text) = request.content_text.as_deref() {
            let config = self.memory_config_snapshot();
            if let Err(status) = validate_memory_item_content_limits(content_text, &config) {
                self.counters.memory_items_rejected.fetch_add(1, Ordering::Relaxed);
                return Err(status);
            }
        }
        let state = Arc::clone(self);
        let updated = tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .update_memory_item_lifecycle(&request)
                .map_err(|error| map_memory_store_error("update memory item lifecycle", error))
        })
        .await
        .map_err(|_| Status::internal("memory lifecycle update worker panicked"))??;
        if updated.is_some() {
            self.clear_memory_search_cache();
        }
        Ok(updated)
    }

    /// Deletes a memory item scoped to principal/channel; invalidates the search cache when
    /// something was removed.
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn delete_memory_item(
        self: &Arc<Self>,
        memory_id: String,
        principal: String,
        channel: Option<String>,
    ) -> Result<bool, Status> {
        let state = Arc::clone(self);
        let deleted = tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .delete_memory_item(memory_id.as_str(), principal.as_str(), channel.as_deref())
                .map_err(|error| map_memory_store_error("delete memory item", error))
        })
        .await
        .map_err(|_| Status::internal("memory delete worker panicked"))??;
        if deleted {
            self.clear_memory_search_cache();
        }
        Ok(deleted)
    }

    /// Lists memory items with cursor pagination and tag/source filters.
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err, clippy::too_many_arguments)]
    pub async fn list_memory_items(
        self: &Arc<Self>,
        after_memory_id: Option<String>,
        requested_limit: Option<usize>,
        principal: String,
        channel: Option<String>,
        session_id: Option<String>,
        tags: Vec<String>,
        sources: Vec<MemorySource>,
    ) -> Result<(Vec<MemoryItemRecord>, Option<String>), Status> {
        let effective_limit = requested_limit.unwrap_or(100).clamp(1, MAX_MEMORY_PAGE_LIMIT);
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .list_memory_items(&MemoryItemsListFilter {
                    after_memory_id,
                    principal,
                    channel,
                    session_id,
                    limit: effective_limit.saturating_add(1),
                    tags,
                    sources,
                })
                .map_err(|error| map_memory_store_error("list memory items", error))
        })
        .await
        .map_err(|_| Status::internal("memory list worker panicked"))?
        .map(|mut items| {
            let has_more = items.len() > effective_limit;
            if has_more {
                items.truncate(effective_limit);
            }
            let next_after =
                if has_more { items.last().map(|item| item.memory_id.clone()) } else { None };
            (items, next_after)
        })
    }

    /// Bulk-deletes memory matching the request and returns the count; invalidates the search
    /// cache when anything was removed.
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn purge_memory(
        self: &Arc<Self>,
        request: MemoryPurgeRequest,
    ) -> Result<u64, Status> {
        let state = Arc::clone(self);
        let deleted = tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .purge_memory(&request)
                .map_err(|error| map_memory_store_error("purge memory items", error))
        })
        .await
        .map_err(|_| Status::internal("memory purge worker panicked"))??;
        if deleted > 0 {
            self.clear_memory_search_cache();
        }
        Ok(deleted)
    }

    /// Loads the memory maintenance bookkeeping (last/next runs).
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn memory_maintenance_status(
        self: &Arc<Self>,
    ) -> Result<MemoryMaintenanceStatus, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .memory_maintenance_status()
                .map_err(|error| map_memory_store_error("load memory maintenance status", error))
        })
        .await
        .map_err(|_| Status::internal("memory maintenance status worker panicked"))?
    }

    /// Loads embedding coverage statistics for memory items.
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn memory_embeddings_status(
        self: &Arc<Self>,
    ) -> Result<MemoryEmbeddingsStatus, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .memory_embeddings_status()
                .map_err(|error| map_memory_store_error("load memory embeddings status", error))
        })
        .await
        .map_err(|_| Status::internal("memory embeddings status worker panicked"))?
    }

    /// Runs retention/vacuum maintenance; invalidates the search cache when anything was deleted.
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn run_memory_maintenance(
        self: &Arc<Self>,
        now_unix_ms: i64,
        retention: MemoryRetentionPolicy,
        next_vacuum_due_at_unix_ms: Option<i64>,
        next_maintenance_run_at_unix_ms: Option<i64>,
    ) -> Result<crate::journal::MemoryMaintenanceOutcome, Status> {
        let state = Arc::clone(self);
        let outcome = tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .run_memory_maintenance(&MemoryMaintenanceRequest {
                    now_unix_ms,
                    retention,
                    next_vacuum_due_at_unix_ms,
                    next_maintenance_run_at_unix_ms,
                })
                .map_err(|error| map_memory_store_error("run memory maintenance", error))
        })
        .await
        .map_err(|_| Status::internal("memory maintenance worker panicked"))??;
        if outcome.deleted_total_count > 0 {
            self.clear_memory_search_cache();
        }
        Ok(outcome)
    }

    /// Backfills missing memory embeddings in batches; invalidates the search cache when rows
    /// changed.
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn run_memory_embeddings_backfill(
        self: &Arc<Self>,
        batch_size: usize,
    ) -> Result<MemoryEmbeddingsBackfillOutcome, Status> {
        let state = Arc::clone(self);
        let outcome = tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .run_memory_embeddings_backfill(batch_size)
                .map_err(|error| map_memory_store_error("run memory embeddings backfill", error))
        })
        .await
        .map_err(|_| Status::internal("memory embeddings backfill worker panicked"))??;
        if outcome.updated_count > 0 {
            self.clear_memory_search_cache();
        }
        Ok(outcome)
    }

    /// Searches memory and returns hits plus full retrieval diagnostics.
    /// Never served from (nor written to) the search cache; logs when the
    /// latency budget is exceeded.
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker
    /// panicked.
    #[allow(clippy::result_large_err)]
    pub async fn search_memory_with_diagnostics(
        self: &Arc<Self>,
        request: MemorySearchRequest,
    ) -> Result<MemorySearchOutcome, Status> {
        self.counters.memory_search_requests.fetch_add(1, Ordering::Relaxed);
        let total_started = Instant::now();
        let state = Arc::clone(self);
        let outcome = tokio::task::spawn_blocking(move || {
            let retrieval_config = state.retrieval_config_snapshot();
            let candidate_outcome = state
                .retrieval_backend
                .search_memory_candidate_outcome(&state.journal_store, &request, &retrieval_config)
                .map_err(|error| map_memory_store_error("search memory items", error))?;
            let fusion_started = Instant::now();
            let hits = score_memory_candidates(
                candidate_outcome.candidates,
                request.min_score,
                &retrieval_config,
            );
            let diagnostics = complete_retrieval_diagnostics(
                candidate_outcome.diagnostics,
                elapsed_millis(fusion_started),
                hits.len() as u64,
                elapsed_millis(total_started),
            );
            Ok::<_, Status>(MemorySearchOutcome { hits, diagnostics })
        })
        .await
        .map_err(|_| Status::internal("memory search worker panicked"))??;
        if outcome.diagnostics.latency_budget_exceeded {
            warn!(
                elapsed_ms = outcome.diagnostics.total_latency_ms,
                budget_ms = outcome.diagnostics.latency_budget_ms,
                "memory search exceeded latency budget"
            );
        }
        Ok(outcome)
    }

    /// Cached memory search: a hit is served until the earliest TTL among its
    /// items lapses; misses run the backend search and repopulate the cache.
    /// Concurrent misses for the same key may compute twice (last write
    /// wins), which is acceptable for a read-through cache.
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker
    /// panicked.
    #[allow(clippy::result_large_err)]
    pub async fn search_memory(
        self: &Arc<Self>,
        request: MemorySearchRequest,
    ) -> Result<Vec<MemorySearchHit>, Status> {
        self.counters.memory_search_requests.fetch_add(1, Ordering::Relaxed);
        let cache_key = memory_search_cache_key(&request);
        let now_unix_ms = current_unix_ms();
        let cached_hits = match self.memory_search_cache.lock() {
            Ok(mut cache) => match cache.get(cache_key.as_str()) {
                Some(entry)
                    if entry
                        .expires_at_unix_ms
                        .is_some_and(|expires_at| expires_at <= now_unix_ms) =>
                {
                    cache.remove(cache_key.as_str());
                    None
                }
                Some(entry) => Some(entry.hits.clone()),
                None => None,
            },
            Err(poisoned) => {
                warn!("memory search cache lock poisoned while reading cache");
                let mut cache = poisoned.into_inner();
                match cache.get(cache_key.as_str()) {
                    Some(entry)
                        if entry
                            .expires_at_unix_ms
                            .is_some_and(|expires_at| expires_at <= now_unix_ms) =>
                    {
                        cache.remove(cache_key.as_str());
                        None
                    }
                    Some(entry) => Some(entry.hits.clone()),
                    None => None,
                }
            }
        };
        if let Some(cached) = cached_hits {
            self.counters.memory_search_cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(cached);
        }

        let started_at = Instant::now();
        let state = Arc::clone(self);
        let results = tokio::task::spawn_blocking(move || {
            let retrieval_config = state.retrieval_config_snapshot();
            let candidates = state
                .retrieval_backend
                .search_memory_candidates(&state.journal_store, &request, &retrieval_config)
                .map_err(|error| map_memory_store_error("search memory items", error))?;
            Ok::<_, Status>(score_memory_candidates(
                candidates,
                request.min_score,
                &retrieval_config,
            ))
        })
        .await
        .map_err(|_| Status::internal("memory search worker panicked"))??;
        if started_at.elapsed().as_millis() > MEMORY_SEARCH_LATENCY_BUDGET_MS {
            warn!(
                elapsed_ms = started_at.elapsed().as_millis(),
                budget_ms = MEMORY_SEARCH_LATENCY_BUDGET_MS,
                "memory search exceeded latency budget"
            );
        }

        // Capacity eviction removes an arbitrary entry (HashMap iteration
        // order), not LRU: this is a size bound, not a hit-rate strategy.
        match self.memory_search_cache.lock() {
            Ok(mut cache) => {
                if cache.len() >= MEMORY_SEARCH_CACHE_CAPACITY {
                    if let Some(first_key) = cache.keys().next().cloned() {
                        cache.remove(first_key.as_str());
                    }
                }
                cache.insert(
                    cache_key,
                    CachedMemorySearchEntry {
                        hits: results.clone(),
                        expires_at_unix_ms: Self::cached_memory_search_expires_at(&results),
                    },
                );
            }
            Err(poisoned) => {
                warn!("memory search cache lock poisoned while writing cache");
                let mut cache = poisoned.into_inner();
                if cache.len() >= MEMORY_SEARCH_CACHE_CAPACITY {
                    if let Some(first_key) = cache.keys().next().cloned() {
                        cache.remove(first_key.as_str());
                    }
                }
                cache.insert(
                    cache_key,
                    CachedMemorySearchEntry {
                        hits: results.clone(),
                        expires_at_unix_ms: Self::cached_memory_search_expires_at(&results),
                    },
                );
            }
        }
        Ok(results)
    }

    /// Loads a workspace document by path within the principal/channel/agent scope.
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn workspace_document_by_path(
        self: &Arc<Self>,
        principal: String,
        channel: Option<String>,
        agent_id: Option<String>,
        path: String,
        include_deleted: bool,
    ) -> Result<Option<WorkspaceDocumentRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .workspace_document_by_path(
                    principal.as_str(),
                    channel.as_deref(),
                    agent_id.as_deref(),
                    path.as_str(),
                    include_deleted,
                )
                .map_err(|error| map_memory_store_error("load workspace document", error))
        })
        .await
        .map_err(|_| Status::internal("workspace document worker panicked"))?
    }

    /// Loads a workspace document by id within the principal/channel/agent scope.
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn workspace_document_by_id(
        self: &Arc<Self>,
        principal: String,
        channel: Option<String>,
        agent_id: Option<String>,
        document_id: String,
        include_deleted: bool,
    ) -> Result<Option<WorkspaceDocumentRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .workspace_document_by_id(
                    principal.as_str(),
                    channel.as_deref(),
                    agent_id.as_deref(),
                    document_id.as_str(),
                    include_deleted,
                )
                .map_err(|error| map_memory_store_error("load workspace document", error))
        })
        .await
        .map_err(|_| Status::internal("workspace document worker panicked"))?
    }

    /// Lists workspace documents matching the filter.
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_workspace_documents(
        self: &Arc<Self>,
        filter: WorkspaceDocumentListFilter,
    ) -> Result<Vec<WorkspaceDocumentRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .list_workspace_documents(&filter)
                .map_err(|error| map_memory_store_error("list workspace documents", error))
        })
        .await
        .map_err(|_| Status::internal("workspace document list worker panicked"))?
    }

    /// Creates or updates a workspace document (new version on change).
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn upsert_workspace_document(
        self: &Arc<Self>,
        request: WorkspaceDocumentWriteRequest,
    ) -> Result<WorkspaceDocumentRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .upsert_workspace_document(&request)
                .map_err(|error| map_memory_store_error("upsert workspace document", error))
        })
        .await
        .map_err(|_| Status::internal("workspace document write worker panicked"))?
    }

    /// Moves/renames a workspace document.
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn move_workspace_document(
        self: &Arc<Self>,
        request: WorkspaceDocumentMoveRequest,
    ) -> Result<WorkspaceDocumentRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .move_workspace_document(&request)
                .map_err(|error| map_memory_store_error("move workspace document", error))
        })
        .await
        .map_err(|_| Status::internal("workspace document move worker panicked"))?
    }

    /// Soft-deletes a workspace document (history is retained).
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn soft_delete_workspace_document(
        self: &Arc<Self>,
        request: WorkspaceDocumentDeleteRequest,
    ) -> Result<WorkspaceDocumentRecord, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .soft_delete_workspace_document(&request)
                .map_err(|error| map_memory_store_error("delete workspace document", error))
        })
        .await
        .map_err(|_| Status::internal("workspace document delete worker panicked"))?
    }

    /// Lists the stored versions of a workspace document.
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn list_workspace_document_versions(
        self: &Arc<Self>,
        document_id: String,
        limit: usize,
    ) -> Result<Vec<WorkspaceDocumentVersionRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .list_workspace_document_versions(document_id.as_str(), limit)
                .map_err(|error| map_memory_store_error("list workspace document versions", error))
        })
        .await
        .map_err(|_| Status::internal("workspace document versions worker panicked"))?
    }

    /// Sets or clears the pinned flag on a workspace document by path.
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn set_workspace_document_pinned(
        self: &Arc<Self>,
        principal: String,
        channel: Option<String>,
        agent_id: Option<String>,
        path: String,
        pinned: bool,
    ) -> Result<Option<WorkspaceDocumentRecord>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .set_workspace_document_pinned(
                    principal.as_str(),
                    channel.as_deref(),
                    agent_id.as_deref(),
                    path.as_str(),
                    pinned,
                )
                .map_err(|error| map_memory_store_error("pin workspace document", error))
        })
        .await
        .map_err(|_| Status::internal("workspace document pin worker panicked"))?
    }

    /// Records that a workspace document was recalled into context.
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn record_workspace_document_recall(
        self: &Arc<Self>,
        document_id: String,
        recalled_at_unix_ms: i64,
    ) -> Result<(), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .record_workspace_document_recall(document_id.as_str(), recalled_at_unix_ms)
                .map_err(|error| map_memory_store_error("record workspace recall", error))
        })
        .await
        .map_err(|_| Status::internal("workspace recall worker panicked"))?
    }

    /// Seeds the default workspace documents for a principal.
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker panicked.
    #[allow(clippy::result_large_err)]
    pub async fn bootstrap_workspace(
        self: &Arc<Self>,
        request: WorkspaceBootstrapRequest,
    ) -> Result<WorkspaceBootstrapOutcome, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state
                .journal_store
                .bootstrap_workspace(&request)
                .map_err(|error| map_memory_store_error("bootstrap workspace", error))
        })
        .await
        .map_err(|_| Status::internal("workspace bootstrap worker panicked"))?
    }

    /// Searches workspace documents. When the retrieval index returns no
    /// hits, falls back to lexical scoring over journal documents so content
    /// that has not been indexed yet stays findable.
    ///
    /// # Errors
    /// Returns the mapped memory store error, or `internal` if the worker
    /// panicked.
    #[allow(clippy::result_large_err)]
    pub async fn search_workspace_documents_with_diagnostics(
        self: &Arc<Self>,
        request: WorkspaceSearchRequest,
    ) -> Result<WorkspaceSearchOutcome, Status> {
        let total_started = Instant::now();
        let state = Arc::clone(self);
        let outcome = tokio::task::spawn_blocking(move || {
            let retrieval_config = state.retrieval_config_snapshot();
            let candidate_outcome = state
                .retrieval_backend
                .search_workspace_candidate_outcome(
                    &state.journal_store,
                    &request,
                    &retrieval_config,
                )
                .map_err(|error| map_memory_store_error("search workspace documents", error))?;
            let fusion_started = Instant::now();
            let mut hits = score_workspace_candidates(
                candidate_outcome.candidates,
                request.min_score,
                &retrieval_config,
            );
            if hits.is_empty() {
                let documents = state
                    .journal_store
                    .list_workspace_documents(&WorkspaceDocumentListFilter {
                        principal: request.principal.clone(),
                        channel: request.channel.clone(),
                        agent_id: request.agent_id.clone(),
                        prefix: request.prefix.clone(),
                        include_deleted: false,
                        limit: request.top_k.clamp(1, MAX_MEMORY_SEARCH_TOP_K),
                    })
                    .map_err(|error| {
                        map_memory_store_error("fallback search workspace documents", error)
                    })?;
                hits = fallback_workspace_document_search_hits(
                    documents,
                    &request,
                    &retrieval_config,
                    current_unix_ms(),
                );
            }
            let diagnostics = complete_retrieval_diagnostics(
                candidate_outcome.diagnostics,
                elapsed_millis(fusion_started),
                hits.len() as u64,
                elapsed_millis(total_started),
            );
            Ok::<_, Status>(WorkspaceSearchOutcome { hits, diagnostics })
        })
        .await
        .map_err(|_| Status::internal("workspace search worker panicked"))??;
        if outcome.diagnostics.latency_budget_exceeded {
            warn!(
                elapsed_ms = outcome.diagnostics.total_latency_ms,
                budget_ms = outcome.diagnostics.latency_budget_ms,
                "workspace search exceeded latency budget"
            );
        }
        Ok(outcome)
    }

    /// Hits-only variant of
    /// [`Self::search_workspace_documents_with_diagnostics`].
    ///
    /// # Errors
    /// Same as the diagnostics variant.
    #[allow(clippy::result_large_err)]
    pub async fn search_workspace_documents(
        self: &Arc<Self>,
        request: WorkspaceSearchRequest,
    ) -> Result<Vec<WorkspaceSearchHit>, Status> {
        Ok(self.search_workspace_documents_with_diagnostics(request).await?.hits)
    }

    /// Counts a cron trigger firing.
    pub fn record_cron_trigger_fired(&self) {
        self.counters.cron_triggers_fired.fetch_add(1, Ordering::Relaxed);
    }
}

fn exact_networked_worker_lease_revocations(
    manager: &WorkerFleetManager,
    events: &[WorkerLifecycleEvent],
    reason_code: &str,
) -> Vec<crate::journal::NetworkedWorkerLeaseRevocation> {
    let durable_records = manager.durable_records();
    events
        .iter()
        .filter_map(|event| {
            let lease = durable_records.get(event.worker_id.as_str())?.lease.as_ref()?;
            Some(crate::journal::NetworkedWorkerLeaseRevocation {
                worker_id: event.worker_id.clone(),
                lease_id: lease.lease_id.clone(),
                run_id: lease.run_id.clone(),
                reason_code: reason_code.to_owned(),
            })
        })
        .collect()
}

fn redacted_networked_worker_cleanup_report(report: &WorkerCleanupReport) -> Value {
    json!({
        "removed_workspace_scope": report.removed_workspace_scope,
        "removed_artifacts": report.removed_artifacts,
        "removed_logs": report.removed_logs,
        "failure_reason": report.failure_reason.as_ref().map(|_| "worker.cleanup.incomplete"),
    })
}

fn networked_worker_completion_details() -> Value {
    json!({
        "cleanup_report": redacted_networked_worker_cleanup_report(&WorkerCleanupReport {
            removed_workspace_scope: true,
            removed_artifacts: true,
            removed_logs: true,
            failure_reason: None,
        }),
        "cleanup_succeeded": true,
        "orphan_classification": "resolved",
    })
}

fn networked_worker_result_transition_id(
    worker_id: &str,
    lease_identity: &WorkerLeaseIdentity,
    receipt: &NetworkedWorkerArtifactReceipt,
) -> String {
    format!(
        "worker-result:{}",
        networked_worker_artifact_digest(worker_id, lease_identity, receipt)
    )
}

fn networked_worker_artifact_event_id(
    worker_id: &str,
    lease_identity: &WorkerLeaseIdentity,
    receipt: &NetworkedWorkerArtifactReceipt,
) -> String {
    format!(
        "worker-artifact:{}",
        networked_worker_artifact_digest(worker_id, lease_identity, receipt)
    )
}

fn networked_worker_artifact_digest(
    worker_id: &str,
    lease_identity: &WorkerLeaseIdentity,
    receipt: &NetworkedWorkerArtifactReceipt,
) -> String {
    let mut hasher = sha2::Sha256::new();
    hasher.update(b"palyra.networked_worker.artifact_receipt.v2\0");
    update_worker_receipt_field(&mut hasher, worker_id.as_bytes());
    update_worker_receipt_field(&mut hasher, lease_identity.lease_id.as_bytes());
    update_worker_receipt_field(&mut hasher, lease_identity.run_id.as_bytes());
    update_worker_receipt_field(&mut hasher, receipt.request_id.as_bytes());
    update_worker_receipt_field(&mut hasher, receipt.proposal_id.as_bytes());
    update_worker_receipt_field(&mut hasher, receipt.tool_name.as_bytes());
    update_worker_receipt_field(&mut hasher, receipt.session_id.as_bytes());
    update_worker_receipt_field(&mut hasher, receipt.run_id.as_bytes());
    update_worker_receipt_field(&mut hasher, receipt.input_json_sha256.as_bytes());
    update_worker_receipt_field(&mut hasher, receipt.output_json_sha256.as_bytes());
    update_worker_receipt_field(&mut hasher, receipt.output_manifest_sha256.as_bytes());
    update_worker_receipt_field(&mut hasher, receipt.validated_result_sha256.as_bytes());
    update_worker_receipt_field(&mut hasher, receipt.grant_id.as_bytes());
    hasher.update(
        u64::try_from(receipt.required_capabilities.len()).unwrap_or(u64::MAX).to_be_bytes(),
    );
    for capability in &receipt.required_capabilities {
        update_worker_receipt_field(&mut hasher, capability.as_bytes());
    }
    update_worker_receipt_field(&mut hasher, receipt.workspace_scope.workspace_root.as_bytes());
    hasher.update([u8::from(receipt.workspace_scope.read_only)]);
    hasher.update(
        u64::try_from(receipt.workspace_scope.allowed_paths.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    for path in &receipt.workspace_scope.allowed_paths {
        update_worker_receipt_field(&mut hasher, path.as_bytes());
    }
    update_worker_receipt_field(&mut hasher, receipt.log_stream_id.as_bytes());
    update_worker_receipt_field(&mut hasher, receipt.scratch_directory_id.as_bytes());
    hasher.update(receipt.observed_at_unix_ms.to_be_bytes());
    hex::encode(hasher.finalize())
}

fn update_worker_receipt_field(hasher: &mut sha2::Sha256, value: &[u8]) {
    hasher.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(value);
}

fn networked_worker_artifact_payload(
    worker_id: &str,
    lease_identity: &WorkerLeaseIdentity,
    receipt: &NetworkedWorkerArtifactReceipt,
) -> RuntimeDecisionPayload {
    RuntimeDecisionPayload::new(
        RuntimeDecisionEventType::WorkerLeaseLifecycle,
        RuntimeDecisionActor::new(
            RuntimeDecisionActorKind::Worker,
            receipt.principal.clone(),
            receipt.device_id.clone(),
            receipt.channel.clone(),
        ),
        "worker.artifact_transport.attested",
        "networked_workers.artifact_transport.daemon",
        RuntimeDecisionTiming::observed(receipt.observed_at_unix_ms),
    )
    .with_input(
        RuntimeEntityRef::new("worker_lease", "worker", lease_identity.lease_id.clone())
            .with_state("completed"),
    )
    .with_output(
        RuntimeEntityRef::new(
            "artifact_manifest",
            "artifact",
            receipt.output_manifest_sha256.clone(),
        )
        .with_state("attested"),
    )
    .with_resource_budget(RuntimeResourceBudget::default())
    .with_details(json!({
        "request_id": receipt.request_id,
        "proposal_id": receipt.proposal_id,
        "tool_name": receipt.tool_name,
        "worker_id": worker_id,
        "lease_id": lease_identity.lease_id,
        "input_json_sha256": receipt.input_json_sha256.clone(),
        "output_json_sha256": receipt.output_json_sha256.clone(),
        "output_manifest_sha256": receipt.output_manifest_sha256.clone(),
        "validated_result_sha256": receipt.validated_result_sha256.clone(),
        "observed_at_unix_ms": receipt.observed_at_unix_ms,
        "grant_id": receipt.grant_id,
        "required_capabilities": receipt.required_capabilities,
        "workspace_scope": {
            "read_only": receipt.workspace_scope.read_only,
            "allowed_paths": receipt.workspace_scope.allowed_paths,
        },
        "artifact_transport": {
            "input_manifest_sha256": receipt.input_json_sha256,
            "output_manifest_sha256": receipt.output_manifest_sha256,
            "log_stream_id": receipt.log_stream_id,
            "scratch_directory_id": receipt.scratch_directory_id,
        },
        "workspace_writeback": {
            "mode": "patch_bundle",
            "authoritative_workspace_mutation": false,
            "approval_required": true,
            "conflict_policy": "reject_changed_local_workspace",
            "cleanup_attestation_required": true,
        },
    }))
}

#[allow(clippy::result_large_err)]
fn validate_networked_worker_artifact_receipt(
    worker_id: &str,
    lease_identity: &WorkerLeaseIdentity,
    receipt: &NetworkedWorkerArtifactReceipt,
) -> Result<(), Status> {
    let bounded_identity = |value: &str| !value.trim().is_empty() && value.len() <= 256;
    let valid_sha256 =
        |value: &str| value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit());
    if !bounded_identity(worker_id)
        || !bounded_identity(lease_identity.lease_id.as_str())
        || !bounded_identity(lease_identity.run_id.as_str())
        || !bounded_identity(receipt.request_id.as_str())
        || !bounded_identity(receipt.proposal_id.as_str())
        || !bounded_identity(receipt.tool_name.as_str())
        || !bounded_identity(receipt.principal.as_str())
        || !bounded_identity(receipt.device_id.as_str())
        || !bounded_identity(receipt.session_id.as_str())
        || !bounded_identity(receipt.grant_id.as_str())
        || !bounded_identity(receipt.log_stream_id.as_str())
        || !bounded_identity(receipt.scratch_directory_id.as_str())
        || !bounded_identity(receipt.workspace_scope.workspace_root.as_str())
        || receipt.required_capabilities.is_empty()
        || receipt.required_capabilities.len() > 64
        || receipt
            .required_capabilities
            .iter()
            .any(|capability| !bounded_identity(capability.as_str()))
        || receipt.workspace_scope.allowed_paths.len() > 256
        || receipt.workspace_scope.allowed_paths.iter().any(|path| !bounded_identity(path.as_str()))
        || receipt.run_id != lease_identity.run_id
        || !valid_sha256(receipt.input_json_sha256.as_str())
        || !valid_sha256(receipt.output_json_sha256.as_str())
        || !valid_sha256(receipt.output_manifest_sha256.as_str())
        || !valid_sha256(receipt.validated_result_sha256.as_str())
        || receipt.validated_result_sha256.bytes().any(|byte| byte.is_ascii_uppercase())
        || receipt.observed_at_unix_ms < 0
    {
        return Err(Status::failed_precondition("networked worker artifact receipt is invalid"));
    }
    Ok(())
}

#[allow(clippy::result_large_err)]
fn networked_worker_journal_event_matches(
    existing: &crate::journal::JournalEventRecord,
    expected: &JournalAppendRequest,
) -> Result<bool, Status> {
    let expected_payload = crate::journal::redact_payload_json(expected.payload_json.as_slice())
        .map_err(|error| {
            map_orchestrator_store_error("redact networked worker result replay", error)
        })?;
    Ok(existing.session_id == expected.session_id
        && existing.run_id == expected.run_id
        && existing.kind == expected.kind
        && existing.actor == expected.actor
        && existing.timestamp_unix_ms == expected.timestamp_unix_ms
        && existing.payload_json == expected_payload
        && existing.principal == expected.principal
        && existing.device_id == expected.device_id
        && existing.channel == expected.channel)
}

fn networked_worker_completed_event_from_receipt(
    worker_id: &str,
    lease_identity: &WorkerLeaseIdentity,
    receipt: &NetworkedWorkerArtifactReceipt,
) -> WorkerLifecycleEvent {
    networked_worker_completed_event(worker_id, lease_identity, receipt.observed_at_unix_ms)
}

fn networked_worker_completed_event(
    worker_id: &str,
    lease_identity: &WorkerLeaseIdentity,
    observed_at_unix_ms: i64,
) -> WorkerLifecycleEvent {
    WorkerLifecycleEvent {
        worker_id: worker_id.to_owned(),
        state: palyra_common::runtime_contracts::WorkerLifecycleState::Completed,
        run_id: Some(lease_identity.run_id.clone()),
        lease_id: Some(lease_identity.lease_id.clone()),
        reason_code: "worker.completed".to_owned(),
        timestamp_unix_ms: observed_at_unix_ms,
    }
}

/// Lexical-only fallback scoring over journal workspace documents, used when
/// the retrieval backend returned no hits (for example, content not yet
/// indexed).
fn fallback_workspace_document_search_hits(
    documents: Vec<WorkspaceDocumentRecord>,
    request: &WorkspaceSearchRequest,
    retrieval_config: &RetrievalRuntimeConfig,
    now_unix_ms: i64,
) -> Vec<WorkspaceSearchHit> {
    let profile =
        retrieval_config.scoring.profile_for(RetrievalSourceProfileKind::WorkspaceDocument);
    let query_variants = vec![request.query.clone()];
    let mut hits = documents
        .into_iter()
        .filter(|document| request.include_quarantined || document.risk_state != "quarantined")
        .filter_map(|document| {
            let searchable_text =
                format!("{}\n{}\n{}", document.title, document.path, document.content_text);
            let lexical_score = lexical_overlap_score(
                searchable_text.as_str(),
                query_variants.as_slice(),
                retrieval_config.scoring.phrase_match_bonus_bps,
            );
            if lexical_score <= 0.0 {
                return None;
            }
            let recency = retrieval_recency_score(
                document.updated_at_unix_ms,
                now_unix_ms,
                profile.min_recency_bps,
            );
            let source_quality = workspace_source_quality(
                document.pinned,
                document.manual_override,
                document.prompt_binding.as_str(),
                document.risk_state.as_str(),
                profile,
            );
            let breakdown = score_with_profile(
                lexical_score,
                0.0,
                recency,
                source_quality,
                document.pinned,
                profile,
            );
            if breakdown.final_score < request.min_score {
                return None;
            }
            let snippet = fallback_workspace_document_snippet(
                document.content_text.as_str(),
                request.query.as_str(),
            );
            Some(WorkspaceSearchHit {
                version: document.latest_version,
                chunk_index: 0,
                chunk_count: 1,
                score: breakdown.final_score,
                reason: format!(
                    "journal_document_fallback(lexical={:.2},recency={:.2},quality={:.2})",
                    breakdown.lexical_score,
                    breakdown.recency_score,
                    breakdown.source_quality_score,
                ),
                breakdown: WorkspaceScoreBreakdown {
                    lexical_score: breakdown.lexical_score,
                    vector_score: 0.0,
                    recency_score: breakdown.recency_score,
                    source_quality_score: breakdown.source_quality_score,
                    final_score: breakdown.final_score,
                },
                snippet,
                document,
            })
        })
        .collect::<Vec<_>>();
    hits.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| right.document.updated_at_unix_ms.cmp(&left.document.updated_at_unix_ms))
            .then_with(|| left.document.document_id.cmp(&right.document.document_id))
    });
    hits.truncate(request.top_k.clamp(1, MAX_MEMORY_SEARCH_TOP_K));
    hits
}

/// Extracts a query-anchored snippet: finds the earliest token match, then
/// backs the start up by a quarter of the snippet length (on a char boundary)
/// so the match appears with leading context rather than at position zero.
fn fallback_workspace_document_snippet(content: &str, query: &str) -> String {
    const SNIPPET_CHARS: usize = 512;
    let query_tokens = query
        .split(|ch: char| !ch.is_alphanumeric() && ch != '_' && ch != '-')
        .map(str::trim)
        .filter(|token| token.chars().count() >= 3)
        .map(str::to_ascii_lowercase)
        .collect::<Vec<_>>();
    let content_lower = content.to_ascii_lowercase();
    let match_index = query_tokens
        .iter()
        .filter_map(|token| content_lower.find(token.as_str()))
        .min()
        .unwrap_or(0);
    let start = content[..match_index.min(content.len())]
        .char_indices()
        .rev()
        .nth(SNIPPET_CHARS / 4)
        .map(|(index, _)| index)
        .unwrap_or(0);
    content[start..].chars().take(SNIPPET_CHARS).collect::<String>()
}

/// Enforces the configured memory content byte and whitespace-token limits.
///
/// # Errors
/// `invalid_argument` naming the limit that was exceeded.
fn validate_memory_item_content_limits(
    content_text: &str,
    config: &MemoryRuntimeConfig,
) -> Result<(), Status> {
    let payload_bytes = content_text.len();
    if payload_bytes > config.max_item_bytes {
        return Err(Status::invalid_argument(format!(
            "memory content exceeds byte limit ({payload_bytes} > {})",
            config.max_item_bytes
        )));
    }
    let token_count = content_text.split_whitespace().count();
    if token_count > config.max_item_tokens {
        return Err(Status::invalid_argument(format!(
            "memory content exceeds token limit ({token_count} > {})",
            config.max_item_tokens
        )));
    }
    Ok(())
}

/// Picks the first non-blank model profile in priority order: registry
/// default chat model, active model, OpenAI model, Anthropic model.
fn select_default_agent_model_profile(
    registry_default_chat_model_id: Option<&str>,
    active_model_id: Option<&str>,
    openai_model: Option<&str>,
    anthropic_model: Option<&str>,
) -> Option<String> {
    [registry_default_chat_model_id, active_model_id, openai_model, anthropic_model]
        .into_iter()
        .find_map(|value| value.and_then(normalize_optional_agent_model_profile))
}

fn normalize_optional_agent_model_profile(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::{
        apply_shared_runtime_tape_identities, current_unix_ms,
        fallback_workspace_document_search_hits, provider_credential_attribution_for_provider,
        provider_lease_timeout_status, select_default_agent_model_profile,
        shared_runtime_event_for_tape, sign_canvas_hmac_sha256,
        validate_memory_item_content_limits, AuthRuntimeState, BrowserServiceRuntimeConfig,
        CanvasHostRuntimeConfig, CredentialAvailabilityService, GatewayJournalConfigSnapshot,
        GatewayProviderAttemptAdmission, GatewayProviderAttemptRuntimeAuthorityGuard,
        GatewayRuntimeConfigSnapshot, GatewayRuntimeState, HttpFetchRuntimeConfig,
        ManagedRuntimeHealthFamily, MemoryRuntimeConfig, ProviderAttemptFeedback,
        RunInterruptLatencyCounters,
    };
    use crate::agents::AgentRegistry;
    use crate::application::code_intel_runtime::{
        CodeIntelLanguage, CodeIntelProviderObservation, CodeIntelProviderSnapshotAuthority,
        LspClientLifecycleStatus,
    };
    use crate::application::run_stream::flow_control::{
        RunInterruptLatencyObservation, RunInterruptPhase, RUN_INTERRUPT_LATENCY_MAX_MS,
    };
    use crate::gateway::RUN_PARAMETER_DELTA_CACHE_CAPACITY;
    use crate::journal::{
        CanvasStatePatchRecord, JournalConfig, JournalStore, OrchestratorRunStartRequest,
        OrchestratorSessionUpsertRequest, OrchestratorTapeAppendRequest,
        RuntimeHealthComponentActivation, RuntimeHealthObservationRequest,
        ToolEffectObservationCommitRequest, WorkspaceDocumentRecord, WorkspaceSearchRequest,
    };
    use crate::media::MediaRuntimeConfig;
    use crate::model_provider::{
        provider_attempt_admission_provider_error, AudioTranscriptionRequest,
        AudioTranscriptionResponse, ModelProvider, ProviderAttemptAdmission,
        ProviderAttemptAdmissionError, ProviderAttemptCompletionDisposition,
        ProviderAttemptHealthAuthority, ProviderAttemptSummary, ProviderCapabilitiesSnapshot,
        ProviderCircuitBreakerSnapshot, ProviderDiscoverySnapshot, ProviderError,
        ProviderFinishReason, ProviderHealthProbeSnapshot, ProviderHealthProbeTarget,
        ProviderProbeAdmission, ProviderRawProviderRefs, ProviderRegistryModelSnapshot,
        ProviderRegistryProviderSnapshot, ProviderRegistrySnapshot, ProviderRequest,
        ProviderResponse, ProviderResponseCacheSnapshot, ProviderRetryPolicySnapshot,
        ProviderRouteSelectionTrace, ProviderRuntimeMetricsSnapshot, ProviderStatusSnapshot,
        ProviderTurnOutput, ProviderUsage,
    };
    use crate::provider_leases::{
        LeasePreviewState, LeasePriority, ProviderLeaseExecutionContext,
        ProviderLeasePreviewSnapshot,
    };
    use crate::retrieval::RetrievalRuntimeConfig;
    use palyra_auth::{
        AuthCredential, AuthCredentialType, AuthProfileFailureKind, AuthProfileRegistry,
        AuthProfileScope, AuthProfileSetRequest, AuthProvider, AuthProviderKind,
        OAuthRefreshAdapter, OAuthRefreshError, OAuthRefreshRequest, OAuthRefreshResponse,
        OAuthRefreshState,
    };
    use palyra_common::runtime_contracts::{
        CircuitBreakerPolicy, HealthProbeDisposition, LegacyRuntimeIdentityAdapter,
        RuntimeAuthorityClass, RuntimeCausalLinkKind, RuntimeEventActorKind,
        RuntimeEventEnvelopeV2, RuntimeEventName, RuntimeGeneration, RuntimeGenerationLane,
        RuntimeHealthState, RuntimeIdentityKind, RuntimeIdentitySetV1, RuntimeOperationId,
        RuntimeRunId, RuntimeSessionId, RuntimeSubsystem, RuntimeToolExecutionId, RuntimeTraceId,
        RUNTIME_EVENT_LEGACY_IDENTITY_ADAPTER_EXTENSION,
    };
    use palyra_model_providers::{classify_http_provider_failure, retry_provider_classification};
    use palyra_vault::{Vault, VaultScope};
    use std::{
        future::Future,
        pin::Pin,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc, Mutex,
        },
        time::Duration,
    };
    use tokio::sync::{mpsc, Notify};
    use tonic::Code;

    #[derive(Default)]
    struct SingleFlightRefreshAdapter {
        calls: AtomicUsize,
        active: AtomicUsize,
        max_active: AtomicUsize,
    }

    impl OAuthRefreshAdapter for SingleFlightRefreshAdapter {
        fn refresh_access_token(
            &self,
            _request: &OAuthRefreshRequest,
        ) -> Result<OAuthRefreshResponse, OAuthRefreshError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let active = self.active.fetch_add(1, Ordering::SeqCst).saturating_add(1);
            self.max_active.fetch_max(active, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(25));
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(OAuthRefreshResponse {
                access_token: "refreshed-access-token".to_owned(),
                refresh_token: None,
                expires_in_seconds: Some(3_600),
            })
        }
    }

    fn expired_oauth_credential_service(
        adapter: Arc<SingleFlightRefreshAdapter>,
    ) -> (CredentialAvailabilityService, Arc<Vault>) {
        let state_root = unique_runtime_test_root("palyra-credential-availability");
        let identity_root = state_root.join("identity");
        std::fs::create_dir_all(identity_root.as_path())
            .expect("test identity root should initialize");
        let registry = Arc::new(
            AuthProfileRegistry::open(identity_root.as_path())
                .expect("test auth profile registry should initialize"),
        );
        let state = test_runtime_state();
        let vault = Arc::clone(&state.vault);
        let scope = "global".parse::<VaultScope>().expect("test vault scope should parse");
        vault
            .put_secret(&scope, "oauth_access", b"expired-access-token")
            .expect("expired access token should persist");
        vault
            .put_secret(&scope, "oauth_refresh", b"refresh-token")
            .expect("refresh token should persist");
        registry
            .set_profile(AuthProfileSetRequest {
                profile_id: "oauth-primary".to_owned(),
                provider: AuthProvider::known(AuthProviderKind::Openai),
                profile_name: "OAuth primary".to_owned(),
                scope: AuthProfileScope::Global,
                credential: AuthCredential::Oauth {
                    access_token_vault_ref: "global/oauth_access".to_owned(),
                    refresh_token_vault_ref: "global/oauth_refresh".to_owned(),
                    token_endpoint: "https://example.test/oauth/token".to_owned(),
                    client_id: Some("test-client".to_owned()),
                    client_secret_vault_ref: None,
                    scopes: vec!["chat".to_owned()],
                    expires_at_unix_ms: Some(current_unix_ms().saturating_sub(60_000)),
                    refresh_state: OAuthRefreshState::default(),
                },
            })
            .expect("expired OAuth profile should persist");
        let refresh_adapter: Arc<dyn OAuthRefreshAdapter> = adapter;
        let auth_runtime = Arc::new(AuthRuntimeState::new(registry, refresh_adapter));
        (CredentialAvailabilityService::new(auth_runtime, Arc::clone(&vault)), vault)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn credential_availability_refreshes_expired_oauth_before_materialization() {
        let adapter = Arc::new(SingleFlightRefreshAdapter::default());
        let (service, vault) = expired_oauth_credential_service(Arc::clone(&adapter));

        let (_, binding, report) = service
            .select_attempt(
                "openai-primary",
                "auth-profile:openai-primary:oauth-primary",
                Vec::new(),
            )
            .await
            .expect("credential selection should succeed")
            .expect("configured auth profile should use dynamic selection");
        let lease = service
            .materialize(binding.clone())
            .await
            .expect("refreshed credential should materialize");

        assert_eq!(adapter.calls.load(Ordering::SeqCst), 1);
        assert_eq!(binding.auth_class, AuthCredentialType::Oauth);
        assert_eq!(binding.profile_id_sha256.len(), 64);
        assert_eq!(binding.selection_reason, "eligible");
        assert_eq!(report.reason_code, "selected");
        assert_eq!(lease.auth_class(), AuthCredentialType::Oauth);
        assert!(!format!("{lease:?}").contains("refreshed-access-token"));
        drop(lease);
        let scope = "global".parse::<VaultScope>().expect("test vault scope should parse");
        assert_eq!(
            vault.get_secret(&scope, "oauth_access").expect("access token should load"),
            b"refreshed-access-token"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_credential_selection_refreshes_one_oauth_profile_single_flight() {
        let adapter = Arc::new(SingleFlightRefreshAdapter::default());
        let (service, _vault) = expired_oauth_credential_service(Arc::clone(&adapter));
        let left = service.clone();
        let right = service;

        let (left_result, right_result) = tokio::join!(
            left.select_attempt(
                "openai-primary",
                "auth-profile:openai-primary:oauth-primary",
                Vec::new(),
            ),
            right.select_attempt(
                "openai-primary",
                "auth-profile:openai-primary:oauth-primary",
                Vec::new(),
            )
        );

        assert!(left_result.expect("left selection should complete").is_some());
        assert!(right_result.expect("right selection should complete").is_some());
        assert_eq!(adapter.calls.load(Ordering::SeqCst), 1);
        assert_eq!(adapter.max_active.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn credential_availability_reports_exhaustion_when_all_profiles_are_unavailable() {
        let state_root = unique_runtime_test_root("palyra-credential-exhaustion");
        let identity_root = state_root.join("identity");
        std::fs::create_dir_all(identity_root.as_path())
            .expect("test identity root should initialize");
        let registry = Arc::new(
            AuthProfileRegistry::open(identity_root.as_path())
                .expect("test auth profile registry should initialize"),
        );
        registry
            .set_profile(AuthProfileSetRequest {
                profile_id: "missing-primary".to_owned(),
                provider: AuthProvider::known(AuthProviderKind::Openai),
                profile_name: "Missing primary".to_owned(),
                scope: AuthProfileScope::Global,
                credential: AuthCredential::ApiKey {
                    api_key_vault_ref: "global/missing_primary".to_owned(),
                },
            })
            .expect("unavailable profile descriptor should persist");
        let state = test_runtime_state();
        let adapter: Arc<dyn OAuthRefreshAdapter> = Arc::new(SingleFlightRefreshAdapter::default());
        let auth_runtime = Arc::new(AuthRuntimeState::new(registry, adapter));
        let service = CredentialAvailabilityService::new(auth_runtime, Arc::clone(&state.vault));

        let error = service
            .select_attempt(
                "openai-primary",
                "auth-profile:openai-primary:missing-primary",
                Vec::new(),
            )
            .await
            .expect_err("selection must fail when every credential is unavailable");

        let ProviderAttemptAdmissionError::HealthBlocked { reason_code, .. } = error else {
            panic!("credential exhaustion should use a health-blocked admission error");
        };
        assert_eq!(reason_code, "credential_selection_exhausted");
    }

    #[test]
    fn stale_lsp_generation_cannot_overwrite_reactivated_read_model() {
        let state = test_runtime_state_with_code_intel_idle_reap(0);
        let stale_authority = state
            .admit_managed_runtime_health(ManagedRuntimeHealthFamily::Lsp, "rust")
            .expect("initial Rust LSP health authority should admit");

        state
            .replace_managed_runtime_health_family(
                ManagedRuntimeHealthFamily::Lsp,
                ["rust".to_owned()],
            )
            .expect("Rust LSP health authority should reactivate");
        let current_authority = state
            .admit_managed_runtime_health(ManagedRuntimeHealthFamily::Lsp, "rust")
            .expect("replacement Rust LSP health authority should admit");
        assert!(current_authority.generation > stale_authority.generation);

        let current_ready = CodeIntelProviderObservation::from_status_fields(
            "rust-analyzer",
            CodeIntelLanguage::Rust,
            "ready",
            "rust-analyzer",
            "code_intel.provider_ready.rust",
            "repair",
        )
        .with_runtime_authority(
            current_authority.component_id.clone(),
            current_authority.generation,
        );
        let current = state.observe_code_intel_runtime(
            Some("workspace"),
            std::slice::from_ref(&current_ready),
            &[],
        );
        assert_eq!(current.snapshot.clients.len(), 1);
        assert_eq!(
            current.snapshot.clients[0].runtime_generation,
            Some(current_authority.generation)
        );

        let suppressions_before = state
            .managed_runtime_health_snapshot_sync()
            .expect("managed runtime health snapshot should load")
            .stale_suppressions_total;
        let late_stale = CodeIntelProviderObservation::from_status_fields(
            "rust-analyzer",
            CodeIntelLanguage::Rust,
            "failed",
            "rust-analyzer",
            "code_intel.provider_failed.rust",
            "repair",
        )
        .with_runtime_authority(stale_authority.component_id.clone(), stale_authority.generation);
        let suppressed = state.observe_code_intel_runtime(
            Some("workspace"),
            std::slice::from_ref(&late_stale),
            &[],
        );

        assert_eq!(suppressed.snapshot, current.snapshot);
        assert!(suppressed.audit_events.is_empty());
        assert_eq!(
            suppressed.provider_snapshot_authority.get(&CodeIntelLanguage::Rust),
            Some(&CodeIntelProviderSnapshotAuthority::Stale),
        );
        assert_eq!(
            state
                .managed_runtime_health_snapshot_sync()
                .expect("managed runtime health snapshot should reload")
                .stale_suppressions_total,
            suppressions_before + 1
        );
        let diagnostics = state
            .journal_store
            .shared_runtime_diagnostics()
            .expect("shared runtime diagnostics should load");
        assert_eq!(diagnostics.stale_events_by_subsystem.get("tool"), Some(&1));

        let current_degraded = CodeIntelProviderObservation::from_status_fields(
            "rust-analyzer",
            CodeIntelLanguage::Rust,
            "degraded",
            "rust-analyzer",
            "code_intel.provider_degraded.rust",
            "repair",
        )
        .with_runtime_authority(
            current_authority.component_id.clone(),
            current_authority.generation,
        );
        let applied = state.observe_code_intel_runtime(
            Some("workspace"),
            std::slice::from_ref(&current_degraded),
            &[],
        );
        assert_eq!(applied.snapshot.clients.len(), 1);
        assert_eq!(applied.snapshot.clients[0].status, LspClientLifecycleStatus::Degraded);
        assert_eq!(
            applied.snapshot.clients[0].runtime_generation,
            Some(current_authority.generation)
        );
        assert_eq!(applied.snapshot.clients[0].reason_code, "code_intel.provider_degraded.rust");
    }

    #[test]
    fn stale_worker_run_health_observation_is_diagnostic_only() {
        let state = test_runtime_state();
        let worker_id = "worker-health-generation";
        state
            .activate_networked_worker_runtime_health(worker_id)
            .expect("worker health should activate");
        let authority = state
            .admit_managed_runtime_health(ManagedRuntimeHealthFamily::Worker, worker_id)
            .expect("worker health authority should admit");
        let session_id = "session_worker_health_gateway";
        let run_id = "run_worker_health_gateway";
        state
            .journal_store
            .upsert_orchestrator_session(&OrchestratorSessionUpsertRequest {
                session_id: session_id.to_owned(),
                session_key: session_id.to_owned(),
                session_label: None,
                principal: "user:worker-health-test".to_owned(),
                device_id: "device:worker-health-test".to_owned(),
                channel: Some("test".to_owned()),
            })
            .expect("worker health test session should persist");
        state
            .journal_store
            .start_orchestrator_run(&OrchestratorRunStartRequest {
                run_id: run_id.to_owned(),
                session_id: session_id.to_owned(),
                origin_kind: "manual".to_owned(),
                origin_run_id: None,
                triggered_by_principal: Some("user:worker-health-test".to_owned()),
                parameter_delta_json: None,
                delegated_admission: None,
            })
            .expect("worker health test run should persist");
        let run_generation = state
            .journal_store
            .active_runtime_generation_for_run(run_id, RuntimeGenerationLane::Run)
            .expect("run generation should load")
            .expect("run generation should be active")
            .generation;
        let session_identity =
            RuntimeSessionId::parse(session_id).expect("session id should validate");
        let run_identity = RuntimeRunId::parse(run_id).expect("run id should validate");
        let health_before_stale = state
            .journal_store
            .runtime_component_health(authority.component_id.as_str())
            .expect("worker health should load")
            .expect("worker health should exist");
        let suppressions_before = state
            .managed_runtime_health_snapshot_sync()
            .expect("managed runtime health snapshot should load")
            .stale_suppressions_total;

        state
            .journal_store
            .supersede_run_runtime_generation(
                session_id,
                run_id,
                "runtime.generation.test_superseded",
            )
            .expect("run generation should supersede");
        assert!(!state.record_managed_runtime_health_observation_for_run(
            &authority,
            &session_identity,
            &run_identity,
            run_generation,
            false,
            "runtime.health.worker_dispatch_released",
        ));

        assert_eq!(
            state
                .journal_store
                .runtime_component_health(authority.component_id.as_str())
                .expect("worker health should reload")
                .expect("worker health should remain active"),
            health_before_stale
        );
        assert_eq!(
            state
                .journal_store
                .runtime_stale_event_diagnostic_count_for_scope(
                    session_id,
                    run_id,
                    "runtime.worker.stale_health_observation_suppressed",
                )
                .expect("stale worker health diagnostic count should load"),
            1
        );
        assert_eq!(
            state
                .managed_runtime_health_snapshot_sync()
                .expect("managed runtime health snapshot should reload")
                .stale_suppressions_total,
            suppressions_before + 1
        );
        let diagnostics = state
            .journal_store
            .shared_runtime_diagnostics()
            .expect("shared runtime diagnostics should load");
        assert_eq!(diagnostics.stale_events_by_subsystem.get("worker"), Some(&1));
    }

    #[test]
    fn run_interrupt_latency_snapshot_has_fixed_phases_and_bounded_aggregates() {
        let counters = RunInterruptLatencyCounters::default();
        counters.record(RunInterruptLatencyObservation {
            phase: RunInterruptPhase::Provider,
            latency_ms: 37,
            clamped: false,
        });
        counters.record(RunInterruptLatencyObservation {
            phase: RunInterruptPhase::Approval,
            latency_ms: RUN_INTERRUPT_LATENCY_MAX_MS,
            clamped: true,
        });

        let snapshot = counters.snapshot();

        assert_eq!(snapshot.reason_code, "runtime.interrupt_latency.observed");
        assert_eq!(snapshot.clamped_reason_code, "runtime.interrupt_latency.clamped");
        assert_eq!(snapshot.max_observation_ms, RUN_INTERRUPT_LATENCY_MAX_MS);
        assert_eq!(
            snapshot.phases.iter().map(|phase| phase.phase.as_str()).collect::<Vec<_>>(),
            ["pre_provider", "provider", "approval", "tool", "delivery_terminal"]
        );
        let provider = &snapshot.phases[RunInterruptPhase::Provider.index()];
        assert_eq!(provider.observations, 1);
        assert_eq!(provider.total_latency_ms, 37);
        assert_eq!(provider.max_latency_ms, 37);
        assert_eq!(provider.clamped_observations, 0);
        let approval = &snapshot.phases[RunInterruptPhase::Approval.index()];
        assert_eq!(approval.observations, 1);
        assert_eq!(approval.clamped_observations, 1);
    }

    #[test]
    fn shared_runtime_tape_adapter_requires_explicit_run_start_marker() {
        let progress = OrchestratorTapeAppendRequest {
            run_id: "run_01".to_owned(),
            seq: 1,
            event_type: "status".to_owned(),
            payload_json: serde_json::json!({
                "kind": "in_progress",
                "message": "progress:agent_loop.turn_started",
                "lifecycle_state": null,
            })
            .to_string(),
        };
        let started = OrchestratorTapeAppendRequest {
            run_id: "run_01".to_owned(),
            seq: 2,
            event_type: "status".to_owned(),
            payload_json: serde_json::json!({
                "kind": "in_progress",
                "message": "streaming",
                "lifecycle_state": "in_progress",
            })
            .to_string(),
        };

        assert!(shared_runtime_event_for_tape(&progress).is_none());
        assert_eq!(
            shared_runtime_event_for_tape(&started).map(|event| event.name),
            Some(RuntimeEventName::RunStarted)
        );
    }

    #[test]
    fn retry_run_start_projects_recovered_from_and_hash_only_diagnostics() {
        let state = test_runtime_state();
        let session_id = "runtime_recovery_link_session";
        let origin_run_id = "runtime_recovery_link_origin";
        let retry_run_id = "runtime_recovery_link_retry";
        state
            .journal_store
            .upsert_orchestrator_session(&OrchestratorSessionUpsertRequest {
                session_id: session_id.to_owned(),
                session_key: session_id.to_owned(),
                session_label: None,
                principal: "user:runtime-recovery-test".to_owned(),
                device_id: "device:runtime-recovery-test".to_owned(),
                channel: Some("test".to_owned()),
            })
            .expect("session should persist");
        for (run_id, origin_kind, recovered_from) in
            [(origin_run_id, "manual", None), (retry_run_id, "retry", Some(origin_run_id))]
        {
            if let Some(origin) = recovered_from {
                state
                    .journal_store
                    .update_orchestrator_run_state(
                        origin,
                        crate::orchestrator::RunLifecycleState::Failed,
                        None,
                    )
                    .expect("recovered run should be terminal before retry");
            }
            state
                .journal_store
                .start_orchestrator_run(&OrchestratorRunStartRequest {
                    run_id: run_id.to_owned(),
                    session_id: session_id.to_owned(),
                    origin_kind: origin_kind.to_owned(),
                    origin_run_id: recovered_from.map(ToOwned::to_owned),
                    triggered_by_principal: Some("user:runtime-recovery-test".to_owned()),
                    parameter_delta_json: None,
                    delegated_admission: None,
                })
                .expect("run should persist");
        }
        let generation = state
            .journal_store
            .active_runtime_generation_for_run(retry_run_id, RuntimeGenerationLane::Run)
            .expect("generation should load")
            .expect("retry generation should be active");
        let request = OrchestratorTapeAppendRequest {
            run_id: retry_run_id.to_owned(),
            seq: 0,
            event_type: "status".to_owned(),
            payload_json: serde_json::json!({
                "kind": "in_progress",
                "lifecycle_state": "in_progress",
            })
            .to_string(),
        };

        let projection = state
            .shared_runtime_tape_projection(&request, &generation)
            .expect("projection should build")
            .expect("run start should project");
        let recovered = projection
            .envelope
            .identities
            .causal_links
            .iter()
            .find(|link| link.relation == RuntimeCausalLinkKind::RecoveredFrom)
            .expect("retry should link to recovered origin");
        assert_eq!(recovered.source.kind, RuntimeIdentityKind::Run);
        assert_eq!(recovered.source.value, retry_run_id);
        assert_eq!(recovered.target.kind, RuntimeIdentityKind::Run);
        assert_eq!(recovered.target.value, origin_run_id);
        let diagnostics = projection
            .envelope
            .extensions
            .get("runtime_identity_diagnostics_v1")
            .expect("redacted causal diagnostics should project");
        let serialized = serde_json::to_string(diagnostics).expect("diagnostics should serialize");
        assert!(!serialized.contains(retry_run_id));
        assert!(!serialized.contains(origin_run_id));
    }

    #[test]
    fn shared_runtime_tape_adapter_maps_tool_identity_and_actor() {
        let request = OrchestratorTapeAppendRequest {
            run_id: "run_01".to_owned(),
            seq: 7,
            event_type: "tool_approval_request".to_owned(),
            payload_json: serde_json::json!({
                "proposal_id": "proposal_01",
                "approval_id": "approval_01",
            })
            .to_string(),
        };
        let event = shared_runtime_event_for_tape(&request).expect("event should map");
        assert_eq!(event.name, RuntimeEventName::ApprovalRequired);
        let descriptor = event.name.descriptor();
        assert_eq!(descriptor.subsystem, RuntimeSubsystem::Approval);
        assert_eq!(descriptor.actor_kind, RuntimeEventActorKind::Host);

        let mut identities = RuntimeIdentitySetV1::for_run(
            RuntimeTraceId::parse("trace_01").expect("trace id"),
            RuntimeSessionId::parse("session_01").expect("session id"),
            RuntimeRunId::parse("run_01").expect("run id"),
            RuntimeGeneration::new(1).expect("generation"),
        );
        apply_shared_runtime_tape_identities(&mut identities, &request)
            .expect("identities should adapt");
        assert_eq!(
            identities.tool_proposal_id.as_ref().map(|value| value.as_str()),
            Some("proposal_01")
        );
        assert_eq!(
            identities.approval_subject_id.as_ref().map(|value| value.as_str()),
            Some("approval_01")
        );

        let tool_result = OrchestratorTapeAppendRequest {
            run_id: "run_01".to_owned(),
            seq: 8,
            event_type: "tool_result".to_owned(),
            payload_json: serde_json::json!({
                "proposal_id": "proposal_01",
                "success": true,
            })
            .to_string(),
        };
        apply_shared_runtime_tape_identities(&mut identities, &tool_result)
            .expect("tool result identities should adapt");
        assert_eq!(
            identities.tool_execution_id,
            Some(RuntimeToolExecutionId::parse("tool:proposal_01").expect("execution id"))
        );
        assert_eq!(
            identities.operation_id,
            Some(RuntimeOperationId::parse("tool:proposal_01").expect("operation id"))
        );
        let (_, adapter) = RuntimeIdentitySetV1::from_legacy_run(
            "session_01",
            "run_01",
            RuntimeGeneration::new(1).expect("generation"),
        )
        .expect("legacy identities should adapt");
        let adapter = adapter
            .reconcile_with_identities(&identities)
            .expect("attempt identity should remain unavailable");
        assert_eq!(adapter.missing_fields, vec!["attempt_id"]);
    }

    #[tokio::test]
    async fn tool_result_tape_projection_persists_reconciled_legacy_identity_evidence() {
        let state = test_runtime_state();
        let session_id = "session_tool_result_projection";
        let run_id = "run_tool_result_projection";
        start_test_orchestrator_run(&state, session_id, run_id);
        let payload_json = serde_json::json!({
            "proposal_id": "proposal_tool_result_projection",
            "success": true,
            "result": "must not be copied into the runtime envelope",
        })
        .to_string();

        state
            .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
                run_id: run_id.to_owned(),
                seq: 0,
                event_type: "tool_result".to_owned(),
                payload_json: payload_json.clone(),
            })
            .await
            .expect("tool result projection should persist");

        let connection = rusqlite::Connection::open(state.journal_config.db_path.as_path())
            .expect("journal database should reopen");
        let raw = connection
            .query_row(
                "SELECT envelope_json FROM runtime_events_v2 WHERE run_ulid = ?1 AND event_name = ?2",
                rusqlite::params![run_id, RuntimeEventName::ToolResultObserved.as_str()],
                |row| row.get::<_, String>(0),
            )
            .expect("tool result runtime event should exist");
        assert!(!raw.contains("must not be copied"));
        let envelope: RuntimeEventEnvelopeV2 =
            serde_json::from_str(raw.as_str()).expect("runtime event should decode");
        envelope.validate().expect("runtime event should validate");
        assert_eq!(
            envelope.identities.tool_execution_id.as_ref().map(|value| value.as_str()),
            Some("tool:proposal_tool_result_projection")
        );
        assert_eq!(
            envelope.identities.operation_id.as_ref().map(|value| value.as_str()),
            Some("tool:proposal_tool_result_projection")
        );
        let adapter: LegacyRuntimeIdentityAdapter = serde_json::from_value(
            envelope.extensions[RUNTIME_EVENT_LEGACY_IDENTITY_ADAPTER_EXTENSION].clone(),
        )
        .expect("legacy identity evidence should decode");
        assert_eq!(adapter.missing_fields, vec!["attempt_id"]);
        assert!(matches!(
            envelope.payload,
            palyra_common::runtime_contracts::RuntimeEventPayloadRef::Omitted {
                reason_code,
                digest_sha256: None,
                size_bytes,
            } if reason_code == "runtime.event.legacy_tape_payload_omitted"
                && size_bytes == u64::try_from(payload_json.len()).expect("payload length should fit")
        ));
    }

    #[tokio::test]
    async fn mapped_tape_projection_and_tape_row_commit_atomically() {
        let state = test_runtime_state();
        let session_id = "session_atomic_tape_projection";
        let run_id = "run_atomic_tape_projection";
        start_test_orchestrator_run(&state, session_id, run_id);
        let first_payload = serde_json::json!({
            "proposal_id": "proposal_atomic_tape_projection",
            "success": true,
        })
        .to_string();
        let first_request = OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: 0,
            event_type: "tool_result".to_owned(),
            payload_json: first_payload,
        };

        state
            .append_orchestrator_tape_event(first_request.clone())
            .await
            .expect("mapped tape boundary should persist");
        state
            .append_orchestrator_tape_event(first_request)
            .await
            .expect("matching mapped tape replay should be idempotent");

        let conflicting = state
            .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
                run_id: run_id.to_owned(),
                seq: 0,
                event_type: "tool_result".to_owned(),
                payload_json: serde_json::json!({
                    "proposal_id": "proposal_conflicting_tape_projection",
                    "success": true,
                })
                .to_string(),
            })
            .await
            .expect_err("conflicting mapped tape replay must fail closed");
        assert_eq!(conflicting.code(), Code::InvalidArgument);

        let connection = rusqlite::Connection::open(state.journal_config.db_path.as_path())
            .expect("journal database should reopen");
        let tape_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM orchestrator_tape WHERE run_ulid = ?1",
                rusqlite::params![run_id],
                |row| row.get(0),
            )
            .expect("tape count should load");
        let runtime_event_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM runtime_events_v2 WHERE run_ulid = ?1",
                rusqlite::params![run_id],
                |row| row.get(0),
            )
            .expect("runtime event count should load");
        assert_eq!(tape_count, 1);
        assert_eq!(runtime_event_count, 1);
    }

    #[tokio::test]
    async fn atomic_tool_effect_replay_keeps_metadata_projection_idempotent() {
        let state = test_runtime_state();
        let session_id = "session_atomic_tool_effect_metadata";
        let run_id = "run_atomic_tool_effect_metadata";
        let proposal_id = "proposal_atomic_tool_effect_metadata";
        start_test_orchestrator_run(&state, session_id, run_id);
        let (_, generation) = state
            .runtime_generation_for_tool_blocking(run_id)
            .expect("tool generation query should succeed")
            .expect("tool generation should be active");
        let (operation_id, tool_execution_id) =
            GatewayRuntimeState::tool_side_effect_identities(proposal_id)
                .expect("tool side-effect identities should validate");
        let fence = palyra_common::runtime_contracts::SideEffectFenceV1 {
            schema_version: 1,
            operation_id: operation_id.clone(),
            tool_execution_id,
            intent_generation: generation,
            observed_generation: generation,
            intent_sha256: "a".repeat(64),
            state: palyra_common::runtime_contracts::SideEffectFenceState::IntentRecorded,
            semantics: palyra_common::runtime_contracts::ToolExecutionSemantics {
                schema_version: 1,
                tool_name: "palyra.fs.apply_patch".to_owned(),
                idempotency_class:
                    palyra_common::runtime_contracts::RuntimeIdempotencyClass::ReconciliableMutation,
                restart_policy:
                    palyra_common::runtime_contracts::SideEffectRestartPolicy::ReconcileBeforeRetry,
                reconciliation_strategy:
                    palyra_common::runtime_contracts::ReconciliationStrategy::WorkspaceDigest,
                external_idempotency_key_required: false,
            },
            external_idempotency_key_sha256: None,
            evidence_sha256: None,
            reason_code: "tool.effect.intent_recorded".to_owned(),
            updated_at_unix_ms: current_unix_ms(),
        };
        assert_eq!(
            state
                .prepare_tool_side_effect_fence(session_id.to_owned(), run_id.to_owned(), fence,)
                .await
                .expect("tool effect intent should persist"),
            palyra_common::runtime_contracts::SideEffectRetryDecision::Safe
        );
        state
            .transition_tool_side_effect_fence(
                operation_id.clone(),
                palyra_common::runtime_contracts::SideEffectFenceState::EffectStarted,
                generation,
                "tool.effect.started".to_owned(),
                None,
            )
            .await
            .expect("tool effect should enter started state");

        let request = ToolEffectObservationCommitRequest {
            operation_id,
            generation,
            evidence_sha256: "b".repeat(64),
            tape_events: vec![
                OrchestratorTapeAppendRequest {
                    run_id: run_id.to_owned(),
                    seq: 0,
                    event_type: "tool_result".to_owned(),
                    payload_json: serde_json::json!({
                        "proposal_id": proposal_id,
                        "success": true,
                        "output_json": {"ok": true},
                        "error": "",
                    })
                    .to_string(),
                },
                OrchestratorTapeAppendRequest {
                    run_id: run_id.to_owned(),
                    seq: 1,
                    event_type: "tool_attestation".to_owned(),
                    payload_json: serde_json::json!({
                        "proposal_id": proposal_id,
                        "attestation_id": "attestation_atomic_tool_effect_metadata",
                        "execution_sha256": "b".repeat(64),
                        "executed_at_unix_ms": current_unix_ms(),
                        "timed_out": false,
                        "executor": "test",
                        "sandbox_enforcement": "test",
                    })
                    .to_string(),
                },
                OrchestratorTapeAppendRequest {
                    run_id: run_id.to_owned(),
                    seq: 2,
                    event_type: "tool.executed".to_owned(),
                    payload_json: serde_json::json!({
                        "proposal_id": proposal_id,
                        "tool_name": "palyra.fs.apply_patch",
                        "success": true,
                        "error": "",
                    })
                    .to_string(),
                },
            ],
        };
        let observed = state
            .commit_tool_effect_observation(request.clone())
            .await
            .expect("atomic tool effect observation should persist");
        assert_eq!(
            observed.state,
            palyra_common::runtime_contracts::SideEffectFenceState::EffectObserved
        );
        assert_eq!(
            state
                .commit_tool_effect_observation(request)
                .await
                .expect("matching atomic tool effect replay should succeed"),
            observed
        );

        let connection = rusqlite::Connection::open(state.journal_config.db_path.as_path())
            .expect("journal database should reopen");
        let tape_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM orchestrator_tape WHERE run_ulid = ?1",
                rusqlite::params![run_id],
                |row| row.get(0),
            )
            .expect("tool effect tape rows should count");
        let metadata_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM metadata_trace_events WHERE run_ulid = ?1 AND event_kind = 'tool_outcome'",
                rusqlite::params![run_id],
                |row| row.get(0),
            )
            .expect("metadata trace rows should count");
        assert_eq!(tape_count, 3);
        assert_eq!(metadata_count, 1);
        assert_eq!(state.counters.snapshot().metadata_trace_events, 1);
    }

    #[test]
    fn provider_lease_timeout_status_surfaces_backpressure_without_internal_reason_codes() {
        let status = provider_lease_timeout_status(
            763,
            &ProviderLeaseExecutionContext {
                provider_id: "openai".to_owned(),
                credential_id: "cred-a".to_owned(),
                priority: LeasePriority::Foreground,
                task_label: "primary_interactive".to_owned(),
                max_wait_ms: 30_000,
                session_id: Some("session-1".to_owned()),
                run_id: Some("run-1".to_owned()),
                runtime_authority: None,
                diagnostic_scope_id: None,
            },
            ProviderLeasePreviewSnapshot {
                state: LeasePreviewState::Waiting,
                priority: LeasePriority::Foreground,
                estimated_wait_ms: Some(25),
                retry_after_ms: None,
                active_provider_leases: 2,
                active_credential_leases: 2,
                foreground_waiters: 1,
                background_waiters: 0,
                credential_state: None,
                reason: Some("shared_capacity_exhausted".to_owned()),
                queue_position: Some(2),
                wait_reason: Some("shared_capacity_exhausted".to_owned()),
                priority_class: "foreground".to_owned(),
                selected_provider_candidate: Some("openai:cred-a".to_owned()),
                timeout_ms: Some(30_000),
            },
        );

        assert_eq!(status.code(), Code::ResourceExhausted);
        assert!(
            status.message().contains("model provider capacity is busy"),
            "lease timeout should be reported as explicit backpressure"
        );
        assert!(
            status.message().contains("queued for 763 ms"),
            "lease timeout should include the observed queue wait"
        );
        assert!(
            !status.message().contains("shared_capacity_exhausted"),
            "internal lease reason codes should not leak into user-facing errors"
        );
    }

    #[test]
    fn run_parameter_delta_cache_updates_and_evicts_oldest() {
        let mut cache = super::RunParameterDeltaCache::default();

        cache.insert(" run-1 ", r#"{"cli_context":{"launch_cwd":"/tmp/one"}}"#);
        cache.insert("run-1", r#"{"cli_context":{"launch_cwd":"/tmp/two"}}"#);

        assert_eq!(
            cache.get("run-1").as_deref(),
            Some(r#"{"cli_context":{"launch_cwd":"/tmp/two"}}"#)
        );

        for index in 2..=(RUN_PARAMETER_DELTA_CACHE_CAPACITY + 1) {
            cache.insert(format!("run-{index}").as_str(), "{}");
        }

        assert_eq!(cache.get("run-1"), None);
        assert_eq!(cache.entries.len(), RUN_PARAMETER_DELTA_CACHE_CAPACITY);
        let newest_run_id = format!("run-{}", RUN_PARAMETER_DELTA_CACHE_CAPACITY + 1);
        assert_eq!(cache.get(newest_run_id.as_str()).as_deref(), Some("{}"));
    }

    #[test]
    fn canvas_patch_history_response_budget_keeps_recent_records() {
        let records = (1..=5)
            .map(|state_version| test_canvas_patch_record(state_version, 128))
            .collect::<Vec<_>>();

        let limited = GatewayRuntimeState::limit_canvas_patch_history_response(records, 1_100);

        assert_eq!(
            limited.iter().map(|record| record.state_version).collect::<Vec<_>>(),
            vec![4, 5],
            "response budgeting should retain the newest contiguous revisions"
        );
    }

    #[test]
    fn canvas_hmac_signature_frames_parts_unambiguously() {
        let secret = [7_u8; 32];

        let left = sign_canvas_hmac_sha256(&secret, "canvas_bundle.v1", &[b"ab", b"c"]);
        let right = sign_canvas_hmac_sha256(&secret, "canvas_bundle.v1", &[b"a", b"bc"]);

        assert_ne!(left, right);
    }

    #[test]
    fn default_agent_model_profile_prefers_registry_default_model() {
        let selected = select_default_agent_model_profile(
            Some("MiniMax-M2.7"),
            Some("gpt-4o-mini"),
            Some("gpt-4o-mini"),
            Some("claude-3-5-sonnet-latest"),
        );

        assert_eq!(selected.as_deref(), Some("MiniMax-M2.7"));
    }

    fn test_canvas_patch_record(
        state_version: u64,
        payload_bytes: usize,
    ) -> CanvasStatePatchRecord {
        let payload = "x".repeat(payload_bytes);
        let sqlite_version = i64::try_from(state_version).expect("test version fits i64");
        CanvasStatePatchRecord {
            seq: sqlite_version,
            canvas_id: "canvas-test".to_owned(),
            state_version,
            base_state_version: state_version.saturating_sub(1),
            state_schema_version: 1,
            patch_json: payload.clone(),
            resulting_state_json: payload,
            closed: false,
            close_reason: None,
            actor_principal: "admin:local".to_owned(),
            actor_device_id: "device:test".to_owned(),
            applied_at_unix_ms: sqlite_version,
        }
    }

    #[test]
    fn provider_credential_attribution_uses_actual_failover_provider() {
        let lease_context =
            provider_lease_context("openai-primary", "auth-profile:openai-primary:primary-profile");
        let snapshot = provider_status_snapshot(true);

        let attribution = provider_credential_attribution_for_provider(
            &snapshot,
            &lease_context,
            "anthropic-primary",
        )
        .expect("fallback provider should resolve to a credential attribution");

        assert_eq!(attribution.provider_id, "anthropic-primary");
        assert_eq!(attribution.credential_id, "auth-profile:anthropic-primary:fallback-profile");
        assert_eq!(attribution.auth_profile_id.as_deref(), Some("fallback-profile"));
    }

    #[tokio::test]
    async fn failover_provider_errors_still_record_credential_feedback() {
        let state = test_runtime_state();
        start_test_orchestrator_run(&state, "session-1", "run-1");
        let _ = state.configure_model_provider(Arc::new(RateLimitedFailoverModelProvider));
        let lease_context =
            provider_lease_context("openai-primary", "auth-profile:openai-primary:primary-profile");

        let result = state
            .execute_model_provider_with_lease(
                ProviderRequest::from_input_text(
                    "exercise rate-limit feedback".to_owned(),
                    false,
                    Vec::new(),
                    None,
                ),
                lease_context,
            )
            .await;

        assert!(result.is_err(), "fake provider should return the configured rate-limit error");
        let snapshot = state.provider_lease_snapshot();
        let feedback = snapshot
            .credential_feedback
            .iter()
            .find(|entry| entry.credential_id == "auth-profile:openai-primary:primary-profile")
            .expect("provider error should record credential feedback even when failover exists");
        assert_eq!(feedback.provider_id, "openai-primary");
        assert_eq!(feedback.state, "rate_limited");
        assert!(
            feedback.reason.contains("category=rate_limit"),
            "feedback should preserve the provider recovery category"
        );
        let authority = state
            .current_model_provider_runtime()
            .health_authority_by_provider
            .get("openai-primary")
            .cloned()
            .expect("primary provider health authority should exist");
        let health = state
            .journal_store
            .runtime_component_health(authority.component_id.as_str())
            .expect("provider health should load")
            .expect("provider health should exist");
        assert_eq!(health.generation, authority.generation);
        assert_eq!(health.state, RuntimeHealthState::Cooldown);
        assert_eq!(health.strike_count, 1);
        assert!(health.expires_at_unix_ms.is_some());
        assert_eq!(health.reason_code, "runtime.health.provider_call_failed");
    }

    #[test]
    fn context_window_failure_does_not_poison_provider_health_before_retry() {
        let state = test_runtime_state();
        let _ = state.configure_model_provider(Arc::new(RateLimitedFailoverModelProvider));
        let admission = test_provider_attempt_admission(&state);
        let binding = admission
            .bind_attempt(
                "openai-primary",
                "auth-profile:openai-primary:primary-profile",
                "gpt-4o-mini",
            )
            .expect("provider attempt should bind");
        let authority = binding.health_authority.clone();
        let context_window_error = ProviderError::RequestFailed {
            message: "request context window exceeded".to_owned(),
            retryable: false,
            retry_count: 0,
            classification: crate::model_provider::ProviderFailureClassification::new(
                crate::model_provider::ProviderFailureClass::ContextWindowExceeded,
                crate::model_provider::ProviderFailureAction::UserActionRequired,
                None,
                Some("test_context_window_exceeded".to_owned()),
            ),
        };

        admission.apply_feedback(ProviderAttemptFeedback::Failure(binding, context_window_error));

        let health = state
            .journal_store
            .runtime_component_health(authority.component_id.as_str())
            .expect("provider health should load")
            .expect("provider health should exist");
        assert_eq!(health.generation, authority.generation);
        assert_eq!(health.state, RuntimeHealthState::Healthy);
        assert_eq!(health.strike_count, 0);
        assert!(
            state.provider_lease_snapshot().credential_feedback.is_empty(),
            "request-local context pressure must not penalize a credential"
        );
    }

    #[tokio::test]
    async fn duplicate_provider_success_applies_mutable_feedback_once() {
        let (state, auth_registry) = test_runtime_state_with_auth_profile();
        let _ = state.configure_model_provider(Arc::new(RateLimitedFailoverModelProvider));
        let mut admission = test_provider_attempt_admission(&state);
        admission.lease_context.run_id = None;
        let binding = admission
            .bind_attempt(
                "openai-primary",
                "auth-profile:openai-primary:primary-profile",
                "gpt-4o-mini",
            )
            .expect("provider attempt should bind");
        let _permit = admission.acquire(&binding).await.expect("provider attempt should acquire");
        let authority = gateway_provider_attempt_authority(
            admission.record_started(&binding).await.expect("provider attempt should record start"),
        );

        assert_eq!(
            admission
                .record_success(&binding, Box::new(authority.clone()))
                .await
                .expect("first provider success should append"),
            ProviderAttemptCompletionDisposition::Appended
        );
        assert_eq!(
            admission
                .record_success(&binding, Box::new(authority))
                .await
                .expect("duplicate provider success should replay"),
            ProviderAttemptCompletionDisposition::AlreadyAppended
        );
        assert_eq!(
            admission.feedback.lock().expect("provider feedback lock should not be poisoned").len(),
            1,
            "duplicate completion must not enqueue a second mutable feedback bundle"
        );

        admission.apply_buffered_feedback();

        assert_eq!(provider_configuration_completion_count(&state), 1);
        assert_eq!(
            provider_health_event_count(
                &state,
                binding.health_authority.component_id.as_str(),
                "runtime.health.provider_call_succeeded",
            ),
            1
        );
        let health = state
            .journal_store
            .runtime_component_health(binding.health_authority.component_id.as_str())
            .expect("provider health should load")
            .expect("provider health should exist");
        assert_eq!(health.state, RuntimeHealthState::Healthy);
        assert_eq!(health.strike_count, 0);
        assert_eq!(health.reason_code, "runtime.health.provider_call_succeeded");
        let lease_snapshot = state.provider_lease_snapshot();
        assert_eq!(
            lease_snapshot
                .recent_events
                .iter()
                .filter(|entry| {
                    entry.event == "credential_feedback_cleared"
                        && entry.credential_id == "auth-profile:openai-primary:primary-profile"
                })
                .count(),
            1
        );
        let auth_record = auth_registry
            .runtime_records_for_agent_readonly(state.vault.as_ref(), None)
            .expect("auth profile runtime state should load")
            .into_iter()
            .find(|record| record.profile_id == "primary-profile")
            .expect("auth profile runtime state should exist");
        assert!(auth_record.last_success_unix_ms.is_some());
        assert_eq!(auth_record.failure_count, 0);
    }

    #[tokio::test]
    async fn dropped_started_provider_authority_settles_outcome_unknown() {
        let state = test_runtime_state();
        let _ = state.configure_model_provider(Arc::new(RateLimitedFailoverModelProvider));
        let mut admission = test_provider_attempt_admission(&state);
        admission.lease_context.run_id = None;
        let binding = admission
            .bind_attempt(
                "openai-primary",
                "auth-profile:openai-primary:primary-profile",
                "gpt-4o-mini",
            )
            .expect("provider attempt should bind");
        let _permit = admission.acquire(&binding).await.expect("provider attempt should acquire");
        let authority =
            admission.record_started(&binding).await.expect("provider attempt should record start");

        drop(authority);

        let connection = rusqlite::Connection::open(state.journal_config.db_path.as_path())
            .expect("test journal query connection should open");
        let completion = connection
            .query_row(
                "SELECT outcome, error_class FROM runtime_provider_attempt_completions WHERE attempt_ulid = ?1",
                rusqlite::params![binding.attempt_id.as_str()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
            )
            .expect("dropped started attempt should have durable completion evidence");
        assert_eq!(completion.0, "outcome_unknown");
        assert_eq!(completion.1.as_deref(), Some("provider_future_cancelled_before_settlement"));
    }

    #[tokio::test]
    async fn duplicate_provider_failure_applies_mutable_feedback_once() {
        let (state, auth_registry) = test_runtime_state_with_auth_profile();
        start_test_orchestrator_run(&state, "session-1", "run-1");
        let _ = state.configure_model_provider(Arc::new(RateLimitedFailoverModelProvider));
        let admission = test_provider_attempt_admission(&state);
        let binding = admission
            .bind_attempt(
                "openai-primary",
                "auth-profile:openai-primary:primary-profile",
                "gpt-4o-mini",
            )
            .expect("provider attempt should bind");
        let _permit = admission.acquire(&binding).await.expect("provider attempt should acquire");
        let authority = gateway_provider_attempt_authority(
            admission.record_started(&binding).await.expect("provider attempt should record start"),
        );
        let error = rate_limit_provider_error();

        assert_eq!(
            admission
                .record_failure(&binding, Box::new(authority.clone()), &error)
                .await
                .expect("first provider failure should append"),
            ProviderAttemptCompletionDisposition::Appended
        );
        assert_eq!(
            admission
                .record_failure(&binding, Box::new(authority), &error)
                .await
                .expect("duplicate provider failure should replay"),
            ProviderAttemptCompletionDisposition::AlreadyAppended
        );
        assert_eq!(
            admission.feedback.lock().expect("provider feedback lock should not be poisoned").len(),
            1,
            "duplicate completion must not enqueue a second mutable feedback bundle"
        );

        admission.apply_buffered_feedback();

        assert_eq!(provider_completion_event_count(&state, "run-1"), 1);
        assert_eq!(
            provider_health_event_count(
                &state,
                binding.health_authority.component_id.as_str(),
                "runtime.health.provider_call_failed",
            ),
            1
        );
        let health = state
            .journal_store
            .runtime_component_health(binding.health_authority.component_id.as_str())
            .expect("provider health should load")
            .expect("provider health should exist");
        assert_eq!(health.state, RuntimeHealthState::Cooldown);
        assert_eq!(health.strike_count, 1);
        assert_eq!(health.reason_code, "runtime.health.provider_call_failed");
        let lease_snapshot = state.provider_lease_snapshot();
        assert_eq!(lease_snapshot.credential_feedback.len(), 1);
        assert_eq!(lease_snapshot.credential_feedback[0].state, "rate_limited");
        assert_eq!(
            lease_snapshot
                .recent_events
                .iter()
                .filter(|entry| {
                    entry.event == "credential_feedback_recorded"
                        && entry.credential_id == "auth-profile:openai-primary:primary-profile"
                })
                .count(),
            1
        );
        let auth_record = auth_registry
            .runtime_records_for_agent_readonly(state.vault.as_ref(), None)
            .expect("auth profile runtime state should load")
            .into_iter()
            .find(|record| record.profile_id == "primary-profile")
            .expect("auth profile runtime state should exist");
        assert_eq!(auth_record.failure_count, 1);
        assert_eq!(auth_record.last_failure_kind, Some(AuthProfileFailureKind::RateLimit));
    }

    #[tokio::test]
    async fn provider_health_observation_failure_latches_exact_generation() {
        let state = test_runtime_state();
        let _ = state.configure_model_provider(Arc::new(RateLimitedFailoverModelProvider));
        let provider_runtime = state.current_model_provider_runtime();
        let authority = provider_runtime
            .health_authority_by_provider
            .get("openai-primary")
            .cloned()
            .expect("provider health authority should exist");
        let admission = GatewayProviderAttemptAdmission {
            runtime_state: Arc::clone(&state),
            lease_context: provider_lease_context(
                "openai-primary",
                "auth-profile:openai-primary:primary-profile",
            ),
            expected_configuration_epoch: provider_runtime.configuration_epoch,
            health_authority_by_provider: Arc::new(
                provider_runtime.health_authority_by_provider.clone(),
            ),
            feedback: Arc::new(Mutex::new(Vec::new())),
            attempted_profile_ids: Arc::new(Mutex::new(std::collections::BTreeMap::new())),
            fail_health_observation_once: Some(Arc::new(AtomicBool::new(true))),
        };
        let binding = admission
            .bind_attempt(
                "openai-primary",
                "auth-profile:openai-primary:primary-profile",
                "gpt-4o-mini",
            )
            .expect("provider attempt should bind");
        let failures_before = state.counters.journal_persist_failures.load(Ordering::Relaxed);

        admission
            .feedback
            .lock()
            .expect("provider feedback lock should not be poisoned")
            .push(ProviderAttemptFeedback::Success(binding.clone()));
        admission.apply_buffered_feedback();

        assert!(state.provider_health_authority_is_latched(&authority));
        assert_eq!(
            state.counters.journal_persist_failures.load(Ordering::Relaxed),
            failures_before + 1
        );
        let error = admission
            .check_eligibility(&binding)
            .expect_err("latched provider authority must fail closed before another effect");
        assert!(matches!(
            error,
            ProviderAttemptAdmissionError::HealthBlocked { ref reason_code, .. }
                if reason_code == "provider_attempt_admission_health_observation_unavailable"
        ));
        assert_eq!(
            state
                .journal_store
                .runtime_component_health(authority.component_id.as_str())
                .expect("provider health should load")
                .expect("provider health should exist")
                .state,
            RuntimeHealthState::Healthy,
            "failed observation persistence must not invent durable health state"
        );
    }

    #[tokio::test]
    async fn provider_reload_prunes_observation_failure_latch() {
        let state = test_runtime_state();
        let old_runtime = state.current_model_provider_runtime();
        let old_authority = old_runtime
            .health_authority_by_provider
            .get("deterministic-primary")
            .cloned()
            .expect("initial provider health authority should exist");
        state.latch_provider_health_authority(&old_authority);
        assert!(state.provider_health_authority_is_latched(&old_authority));

        let _ = state.configure_model_provider(Arc::new(RateLimitedFailoverModelProvider));

        assert!(
            !state.provider_health_authority_is_latched(&old_authority),
            "successful reload should prune obsolete exact-generation latches"
        );
        for authority in
            state.current_model_provider_runtime().health_authority_by_provider.values()
        {
            assert!(!state.provider_health_authority_is_latched(authority));
        }
    }

    #[tokio::test]
    async fn stale_provider_health_generation_blocks_before_candidate_effect() {
        let state = test_runtime_state();
        let (started_tx, mut started_rx) = mpsc::channel(1);
        let release = Arc::new(Notify::new());
        let _ = state.configure_model_provider(Arc::new(BlockingCandidateAdmissionProvider {
            started: started_tx,
            release,
        }));
        let authority = state
            .current_model_provider_runtime()
            .health_authority_by_provider
            .get("openai-primary")
            .cloned()
            .expect("candidate health authority should exist");
        state
            .journal_store
            .activate_runtime_health_components(
                &[RuntimeHealthComponentActivation {
                    component_id: authority.component_id,
                    authority_class: RuntimeAuthorityClass::PrivilegedMutation,
                    fallback_component_id: None,
                    fallback_authority_class: None,
                    policy: CircuitBreakerPolicy {
                        strike_threshold: 3,
                        cooldown_ms: 30_000,
                        max_probe_concurrency: 1,
                        security_quarantine_auto_clear: false,
                    },
                    reason_code: "runtime.health.provider_activated".to_owned(),
                }],
                current_unix_ms(),
            )
            .expect("external generation advance should succeed");

        let error = state
            .execute_model_provider_with_lease(
                ProviderRequest::from_input_text(
                    "stale health generation must block".to_owned(),
                    false,
                    Vec::new(),
                    None,
                ),
                provider_lease_context(
                    "openai-primary",
                    "auth-profile:openai-primary:primary-profile",
                ),
            )
            .await
            .expect_err("stale provider health authority should deny the request");
        assert_eq!(error.code(), Code::ResourceExhausted);
        assert!(
            error.message().contains("provider candidate health authority is stale"),
            "denial should identify the stale health authority"
        );
        assert!(
            started_rx.try_recv().is_err(),
            "candidate effect must not start under stale health authority"
        );
    }

    #[tokio::test]
    async fn stale_candidate_feedback_is_discarded_after_provider_reconfiguration() {
        let state = test_runtime_state();
        let session_id = "session-provider-candidate-feedback";
        let run_id = "run-provider-candidate-feedback";
        start_test_orchestrator_run(&state, session_id, run_id);
        let (started_tx, mut started_rx) = mpsc::channel(1);
        let release = Arc::new(Notify::new());
        let initial_generation =
            state.configure_model_provider(Arc::new(BlockingCandidateAdmissionProvider {
                started: started_tx,
                release: Arc::clone(&release),
            }));
        let mut lease_context =
            provider_lease_context("openai-primary", "auth-profile:openai-primary:primary-profile");
        lease_context.session_id = Some(session_id.to_owned());
        lease_context.run_id = Some(run_id.to_owned());
        let request_state = Arc::clone(&state);
        let mut request = tokio::spawn(async move {
            request_state
                .execute_model_provider_with_lease(
                    ProviderRequest::from_input_text(
                        "stale candidate feedback must not escape".to_owned(),
                        false,
                        Vec::new(),
                        None,
                    ),
                    lease_context,
                )
                .await
        });

        tokio::select! {
            started = started_rx.recv() => {
                started.expect("candidate provider call should start");
            }
            result = &mut request => {
                panic!("provider request ended before candidate synchronization: {result:?}");
            }
        }
        let replacement_generation =
            state.configure_model_provider(Arc::new(RateLimitedFailoverModelProvider));
        assert!(replacement_generation > initial_generation);
        release.notify_one();

        let error = request
            .await
            .expect("provider request task should join")
            .expect_err("stale candidate result should be suppressed");
        assert_eq!(error.code(), Code::Aborted);
        assert!(
            state.provider_lease_snapshot().credential_feedback.is_empty(),
            "stale candidate feedback must not update current credential health"
        );
    }

    #[tokio::test]
    async fn targetless_provider_supersession_records_task_correlation() {
        let (state, auth_registry) = test_runtime_state_with_auth_profile();
        let session_id = "session-provider-targetless";
        let task_id = "task-provider-targetless";
        state
            .journal_store
            .upsert_orchestrator_session(&OrchestratorSessionUpsertRequest {
                session_id: session_id.to_owned(),
                session_key: session_id.to_owned(),
                session_label: None,
                principal: "user:test".to_owned(),
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                channel: Some("test".to_owned()),
            })
            .expect("targetless test session should be created");

        let (started_tx, mut started_rx) = mpsc::channel(1);
        let release = Arc::new(Notify::new());
        let initial_generation =
            state.configure_model_provider(Arc::new(BlockingModelProvider::new(
                started_tx,
                Arc::clone(&release),
                provider_status_snapshot(false),
            )));
        let old_authority = state
            .current_model_provider_runtime()
            .health_authority_by_provider
            .get("openai-primary")
            .cloned()
            .expect("targetless provider health authority should exist");
        let request_state = Arc::clone(&state);
        let request = tokio::spawn(async move {
            request_state
                .execute_model_provider_with_lease(
                    ProviderRequest::from_input_text(
                        "targetless stale provider response".to_owned(),
                        false,
                        Vec::new(),
                        None,
                    ),
                    ProviderLeaseExecutionContext {
                        provider_id: "openai-primary".to_owned(),
                        credential_id: "auth-profile:openai-primary:primary-profile".to_owned(),
                        priority: LeasePriority::Background,
                        task_label: "targetless_auxiliary".to_owned(),
                        max_wait_ms: 30_000,
                        session_id: Some(session_id.to_owned()),
                        run_id: None,
                        runtime_authority: None,
                        diagnostic_scope_id: Some(task_id.to_owned()),
                    },
                )
                .await
        });

        started_rx.recv().await.expect("targetless provider call should start");
        let replacement_generation =
            state.configure_model_provider(Arc::new(RateLimitedFailoverModelProvider));
        assert!(replacement_generation > initial_generation);
        release.notify_one();

        let error = request
            .await
            .expect("targetless provider task should join")
            .expect_err("targetless stale provider response should be suppressed");
        assert_eq!(error.code(), Code::Aborted);
        assert_eq!(
            state
                .journal_store
                .runtime_stale_event_diagnostic_count_for_scope(
                    session_id,
                    task_id,
                    "runtime.generation.provider_reconfigured",
                )
                .expect("targetless stale diagnostic count should load"),
            1
        );
        assert_eq!(
            provider_health_event_count(
                &state,
                old_authority.component_id.as_str(),
                "runtime.health.provider_call_succeeded",
            ),
            0,
            "late targetless completion must not mutate provider health"
        );
        assert_eq!(
            provider_configuration_completion_count(&state),
            0,
            "late targetless completion must not append canonical completion evidence"
        );
        let lease_snapshot = state.provider_lease_snapshot();
        assert!(
            lease_snapshot.credential_feedback.is_empty(),
            "late targetless completion must not mutate credential feedback"
        );
        assert_eq!(
            lease_snapshot
                .recent_events
                .iter()
                .filter(|entry| {
                    matches!(
                        entry.event.as_str(),
                        "credential_feedback_cleared" | "credential_feedback_recorded"
                    )
                })
                .count(),
            0
        );
        let auth_record = auth_registry
            .runtime_records_for_agent_readonly(state.vault.as_ref(), None)
            .expect("auth profile runtime state should load")
            .into_iter()
            .find(|record| record.profile_id == "primary-profile")
            .expect("auth profile runtime state should exist");
        assert!(auth_record.last_success_unix_ms.is_none());
        assert_eq!(auth_record.failure_count, 0);
    }

    #[test]
    fn provider_reload_activates_primary_and_failover_health_authority() {
        let state = test_runtime_state();
        let _ = state.configure_model_provider(Arc::new(RateLimitedFailoverModelProvider));

        let runtime = state.current_model_provider_runtime();
        let openai = runtime
            .health_authority_by_provider
            .get("openai-primary")
            .expect("primary provider health authority should exist");
        let anthropic = runtime
            .health_authority_by_provider
            .get("anthropic-primary")
            .expect("failover provider health authority should exist");
        assert_eq!(openai.component_id.as_str(), "provider:openai-primary");
        assert_eq!(anthropic.component_id.as_str(), "provider:anthropic-primary");
        assert_eq!(
            state
                .journal_store
                .runtime_component_health(openai.component_id.as_str())
                .expect("primary health should load")
                .expect("primary health should exist")
                .generation,
            openai.generation
        );
        assert_eq!(
            state
                .journal_store
                .runtime_component_health(anthropic.component_id.as_str())
                .expect("failover health should load")
                .expect("failover health should exist")
                .generation,
            anthropic.generation
        );
    }

    #[test]
    fn provider_and_generation_are_captured_under_one_read_lock() {
        let state = test_runtime_state();
        let provider_guard = state.model_provider.read().unwrap_or_else(|error| error.into_inner());
        let replacement_state = Arc::clone(&state);
        let replacement = std::thread::spawn(move || {
            replacement_state.configure_model_provider(Arc::new(RateLimitedFailoverModelProvider))
        });

        std::thread::sleep(Duration::from_millis(25));
        assert!(
            !replacement.is_finished(),
            "provider replacement must wait for an active provider snapshot read"
        );
        let captured = provider_guard.clone();
        drop(provider_guard);

        let replacement_generation =
            replacement.join().expect("provider replacement thread should join");
        assert!(replacement_generation > captured.configuration_epoch.get());
        assert_eq!(captured.provider.status_snapshot().provider_id, "deterministic-primary");
        assert_eq!(captured.health_authority_by_provider.len(), 1);
        let current = state.current_model_provider_runtime();
        assert_eq!(current.provider.status_snapshot().provider_id, "openai-primary");
        assert_eq!(current.health_authority_by_provider.len(), 2);
    }

    #[tokio::test]
    async fn provider_reconfiguration_suppresses_in_flight_result() {
        let state = test_runtime_state();
        let session_id = "session-provider-reconfiguration";
        let run_id = "run-provider-reconfiguration";
        start_test_orchestrator_run(&state, session_id, run_id);

        let (started_tx, mut started_rx) = mpsc::channel(1);
        let release = Arc::new(Notify::new());
        let provider = Arc::new(BlockingModelProvider::new(
            started_tx,
            Arc::clone(&release),
            state.model_provider_status_snapshot(),
        ));
        let initial_generation = state.configure_model_provider(provider);
        let request_state = Arc::clone(&state);
        let request = tokio::spawn(async move {
            request_state
                .execute_model_provider_with_lease(
                    ProviderRequest::from_input_text(
                        "stale provider response must not escape".to_owned(),
                        false,
                        Vec::new(),
                        None,
                    ),
                    ProviderLeaseExecutionContext {
                        provider_id: "blocking-provider".to_owned(),
                        credential_id: "blocking-credential".to_owned(),
                        priority: LeasePriority::Foreground,
                        task_label: "stale_provider_regression".to_owned(),
                        max_wait_ms: 30_000,
                        session_id: Some(session_id.to_owned()),
                        run_id: Some(run_id.to_owned()),
                        runtime_authority: None,
                        diagnostic_scope_id: None,
                    },
                )
                .await
        });

        started_rx.recv().await.expect("blocked provider should start");
        let replacement_generation =
            state.configure_model_provider(Arc::new(RateLimitedFailoverModelProvider));
        assert!(replacement_generation > initial_generation);
        release.notify_one();

        let error = request
            .await
            .expect("provider request task should join")
            .expect_err("stale provider response should be suppressed");
        assert_eq!(error.code(), Code::Aborted);
        assert_eq!(
            error.message(),
            "model provider response was suppressed after provider reconfiguration"
        );

        let diagnostics = state
            .shared_runtime_diagnostics()
            .await
            .expect("shared runtime diagnostics should load");
        assert_eq!(diagnostics.stale_events_by_subsystem.get("provider"), Some(&1));
        assert_eq!(
            state.counters.snapshot().model_provider_failures,
            0,
            "stale completion must not be projected as a provider failure"
        );

        let run = state
            .journal_store
            .orchestrator_run_status_snapshot(run_id)
            .expect("run snapshot should load")
            .expect("run should exist");
        assert_eq!(run.state, "accepted");
        assert_eq!(run.tape_events, 0);
        assert!(
            state
                .journal_store
                .list_idempotency_records_for_run(run_id)
                .expect("run idempotency records should load")
                .is_empty(),
            "stale completion must not persist tool or delivery idempotency records"
        );
        assert!(
            state
                .journal_store
                .list_tool_result_artifacts_for_run(run_id)
                .expect("run tool artifacts should load")
                .is_empty(),
            "stale completion must not persist tool results"
        );
    }

    #[tokio::test]
    async fn provider_health_probe_uses_exact_non_mutating_lease() {
        let state = test_runtime_state();
        let provider = Arc::new(CandidateAudioTranscriptionProvider {
            started: None,
            release: None,
            result: Err(ProviderError::StatePoisoned),
            status: provider_status_snapshot(false),
        });
        let _ = state.configure_model_provider(provider);
        let authority = state
            .current_model_provider_runtime()
            .health_authority_by_provider
            .get("openai-primary")
            .cloned()
            .expect("provider health authority should exist");
        let mut cooldown = state
            .journal_store
            .record_runtime_health_observation(&RuntimeHealthObservationRequest {
                component_id: authority.component_id.clone(),
                expected_generation: authority.generation,
                succeeded: false,
                reason_code: "runtime.health.provider_call_failed".to_owned(),
                observed_at_unix_ms: current_unix_ms(),
            })
            .expect("threshold failure should enter cooldown")
            .health;
        cooldown.expires_at_unix_ms = Some(cooldown.updated_at_unix_ms);
        state
            .journal_store
            .upsert_runtime_component_health(&cooldown)
            .expect("test cooldown should be expired");

        let outcome = state
            .execute_provider_health_probe(
                "provider:openai-primary",
                "runtime.health.operator_probe".to_owned(),
                None,
                None,
            )
            .await
            .expect("fixed provider probe should settle");

        assert_eq!(outcome.disposition, HealthProbeDisposition::Passed);
        assert_eq!(outcome.health.state, RuntimeHealthState::Healthy);
        assert!(!outcome.replayed);
        let health = state
            .journal_store
            .runtime_component_health(authority.component_id.as_str())
            .expect("provider health should load")
            .expect("provider health should exist");
        assert_eq!(health.generation, authority.generation);
        assert_eq!(health.state, RuntimeHealthState::Healthy);
        let lease_snapshot = state.provider_lease_snapshot();
        assert!(
            lease_snapshot.credential_feedback.is_empty(),
            "read-only probes must not mutate ordinary credential feedback"
        );
        assert_eq!(
            lease_snapshot
                .recent_events
                .iter()
                .filter(|entry| {
                    matches!(
                        entry.event.as_str(),
                        "credential_feedback_cleared" | "credential_feedback_recorded"
                    )
                })
                .count(),
            0,
            "read-only probes must not emit ordinary credential feedback events"
        );
    }

    #[tokio::test]
    async fn provider_health_probe_resolves_hashed_component_identity() {
        let state = test_runtime_state();
        let provider_id = format!("provider-{}", "x".repeat(120));
        let mut status = provider_status_snapshot(false);
        status.provider_id = provider_id.clone();
        status.credential_id = format!("auth-profile:{provider_id}:primary-profile");
        status.registry.default_chat_model_id = Some("gpt-4o-mini".to_owned());
        status.registry.providers = vec![provider_snapshot(
            provider_id.as_str(),
            status.credential_id.as_str(),
            "primary-profile",
            "openai",
        )];
        status.registry.models = vec![model_snapshot("gpt-4o-mini", provider_id.as_str())];
        let provider = Arc::new(ConfigurableProbeProvider {
            status,
            result: Ok(()),
            started: None,
            release: None,
        });
        let _ = state.configure_model_provider(provider);
        let authority = expire_provider_health_cooldown(&state, provider_id.as_str());
        assert!(authority.component_id.as_str().starts_with("provider:sha256:"));

        let outcome = state
            .execute_provider_health_probe(
                authority.component_id.as_str(),
                "runtime.health.operator_probe".to_owned(),
                None,
                None,
            )
            .await
            .expect("hashed provider component should resolve to its exact target");

        assert_eq!(outcome.disposition, HealthProbeDisposition::Passed);
        assert_eq!(outcome.health.component_id, authority.component_id);
        assert_eq!(outcome.health.state, RuntimeHealthState::Healthy);
    }

    #[tokio::test]
    async fn provider_health_probe_caller_drop_does_not_strand_durable_lease() {
        let state = test_runtime_state();
        let (started_tx, mut started_rx) = mpsc::channel(1);
        let release = Arc::new(Notify::new());
        let provider = Arc::new(ConfigurableProbeProvider {
            status: provider_status_snapshot(false),
            result: Ok(()),
            started: Some(started_tx),
            release: Some(Arc::clone(&release)),
        });
        let _ = state.configure_model_provider(provider);
        let authority = expire_provider_health_cooldown(&state, "openai-primary");
        let probe_state = Arc::clone(&state);
        let caller = tokio::spawn(async move {
            probe_state
                .execute_provider_health_probe(
                    "provider:openai-primary",
                    "runtime.health.operator_disconnected".to_owned(),
                    None,
                    None,
                )
                .await
        });

        started_rx.recv().await.expect("provider probe should start");
        let lease = state
            .journal_store
            .runtime_health_probe_lease(authority.component_id.as_str())
            .expect("provider probe lease should load")
            .expect("provider probe lease should be active");
        assert_eq!(
            state
                .journal_store
                .runtime_health_probe_begin_reason(lease.lease_id.as_str())
                .expect("provider probe begin evidence should load")
                .as_deref(),
            Some("runtime.health.operator_disconnected")
        );
        caller.abort();
        let join_error = caller.await.expect_err("aborted caller should not complete");
        assert!(join_error.is_cancelled());
        release.notify_one();

        let health = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let health = state
                    .journal_store
                    .runtime_component_health(authority.component_id.as_str())
                    .expect("provider health should load")
                    .expect("provider health should exist");
                if health.state != RuntimeHealthState::Probing {
                    break health;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("host-owned provider probe should settle after caller cancellation");
        assert_eq!(health.state, RuntimeHealthState::Healthy);
        assert_eq!(health.generation, authority.generation);
        assert_eq!(
            state
                .journal_store
                .runtime_health_probe_lease(authority.component_id.as_str())
                .expect("provider probe lease should load"),
            None
        );
    }

    #[tokio::test]
    async fn provider_health_probe_transport_ambiguity_settles_inconclusive() {
        let state = test_runtime_state();
        let provider = Arc::new(ConfigurableProbeProvider {
            status: provider_status_snapshot(false),
            result: Err(ProviderError::RequestFailed {
                message: "transport outcome unavailable".to_owned(),
                retryable: true,
                retry_count: 0,
                classification: retry_provider_classification("provider_probe_network_unavailable"),
            }),
            started: None,
            release: None,
        });
        let _ = state.configure_model_provider(provider);
        let authority = expire_provider_health_cooldown(&state, "openai-primary");

        let outcome = state
            .execute_provider_health_probe(
                "provider:openai-primary",
                "runtime.health.operator_probe".to_owned(),
                None,
                None,
            )
            .await
            .expect("ambiguous provider probe should settle");

        assert_eq!(outcome.disposition, HealthProbeDisposition::Inconclusive);
        assert_eq!(outcome.health.state, RuntimeHealthState::Quarantined);
        assert_eq!(outcome.health.generation, authority.generation);
        assert_eq!(outcome.health.reason_code, "runtime.health.provider_probe_ambiguous");
    }

    #[tokio::test]
    async fn provider_reconfiguration_after_probe_begin_settles_inconclusive() {
        let state = test_runtime_state();
        let (started_tx, mut started_rx) = mpsc::channel(1);
        let release = Arc::new(Notify::new());
        let provider = Arc::new(ConfigurableProbeProvider {
            status: provider_status_snapshot(false),
            result: Ok(()),
            started: Some(started_tx),
            release: Some(Arc::clone(&release)),
        });
        let _ = state.configure_model_provider(provider);
        let authority = expire_provider_health_cooldown(&state, "openai-primary");
        let probe_state = Arc::clone(&state);
        let probe = tokio::spawn(async move {
            probe_state
                .execute_provider_health_probe(
                    "provider:openai-primary",
                    "runtime.health.operator_probe".to_owned(),
                    None,
                    None,
                )
                .await
        });

        started_rx.recv().await.expect("provider probe should start");
        {
            let mut provider_runtime =
                state.model_provider.write().unwrap_or_else(|error| error.into_inner());
            provider_runtime.configuration_epoch = provider_runtime
                .configuration_epoch
                .next()
                .expect("provider test generation should advance");
        }
        release.notify_one();
        let outcome = probe
            .await
            .expect("provider probe task should join")
            .expect("reconfigured provider probe should settle");

        assert_eq!(outcome.disposition, HealthProbeDisposition::Inconclusive);
        assert_eq!(outcome.health.state, RuntimeHealthState::Quarantined);
        assert_eq!(outcome.health.generation, authority.generation);
        assert_eq!(
            outcome.health.reason_code,
            "runtime.health.provider_probe_runtime_reconfigured"
        );
    }

    #[tokio::test]
    async fn audio_transcription_health_denial_blocks_before_candidate_effect() {
        let state = test_runtime_state();
        let (started_tx, mut started_rx) = mpsc::channel(1);
        let provider = Arc::new(CandidateAudioTranscriptionProvider {
            started: Some(started_tx),
            release: None,
            result: Ok(AudioTranscriptionResponse {
                text: "must not escape".to_owned(),
                language: Some("en".to_owned()),
                duration_ms: Some(100),
                model_name: "whisper-1".to_owned(),
                retry_count: 0,
                segments: Vec::new(),
            }),
            status: audio_provider_status_snapshot(),
        });
        let _ = state.configure_model_provider(provider);
        let authority = state
            .current_model_provider_runtime()
            .health_authority_by_provider
            .get("openai-primary")
            .cloned()
            .expect("audio provider health authority should exist");
        state
            .journal_store
            .activate_runtime_health_components(
                &[RuntimeHealthComponentActivation {
                    component_id: authority.component_id,
                    authority_class: RuntimeAuthorityClass::PrivilegedMutation,
                    fallback_component_id: None,
                    fallback_authority_class: None,
                    policy: CircuitBreakerPolicy {
                        strike_threshold: 1,
                        cooldown_ms: 60_000,
                        max_probe_concurrency: 1,
                        security_quarantine_auto_clear: false,
                    },
                    reason_code: "runtime.health.provider_activated".to_owned(),
                }],
                current_unix_ms(),
            )
            .expect("external health generation advance should succeed");

        let error = state
            .execute_audio_transcription(AudioTranscriptionRequest {
                file_name: "voice.wav".to_owned(),
                content_type: "audio/wav".to_owned(),
                bytes: vec![1, 2, 3],
                prompt: None,
                language: None,
            })
            .await
            .expect_err("stale audio health authority should deny the request");

        assert_eq!(error.code(), Code::ResourceExhausted);
        assert!(
            started_rx.try_recv().is_err(),
            "audio provider effect must not start after health denial"
        );
    }

    #[tokio::test]
    async fn audio_transcription_failure_records_exact_shared_health() {
        let state = test_runtime_state();
        let provider = Arc::new(CandidateAudioTranscriptionProvider {
            started: None,
            release: None,
            result: Err(rate_limit_provider_error()),
            status: audio_provider_status_snapshot(),
        });
        let _ = state.configure_model_provider(provider);
        let authority = state
            .current_model_provider_runtime()
            .health_authority_by_provider
            .get("openai-primary")
            .cloned()
            .expect("audio provider health authority should exist");

        let error = state
            .execute_audio_transcription(AudioTranscriptionRequest {
                file_name: "voice.wav".to_owned(),
                content_type: "audio/wav".to_owned(),
                bytes: vec![1, 2, 3],
                prompt: None,
                language: None,
            })
            .await
            .expect_err("rate-limited audio transcription should fail");

        assert_eq!(error.code(), Code::Unavailable);
        let health = state
            .journal_store
            .runtime_component_health(authority.component_id.as_str())
            .expect("audio provider health should load")
            .expect("audio provider health should exist");
        assert_eq!(health.generation, authority.generation);
        assert_eq!(health.state, RuntimeHealthState::Cooldown);
        assert_eq!(health.strike_count, 1);
        assert_eq!(health.reason_code, "runtime.health.provider_call_failed");
    }

    #[tokio::test]
    async fn audio_transcription_stale_feedback_is_discarded_after_reconfiguration() {
        let state = test_runtime_state();
        let (started_tx, mut started_rx) = mpsc::channel(1);
        let release = Arc::new(Notify::new());
        let initial_generation =
            state.configure_model_provider(Arc::new(CandidateAudioTranscriptionProvider {
                started: Some(started_tx),
                release: Some(Arc::clone(&release)),
                result: Err(rate_limit_provider_error()),
                status: audio_provider_status_snapshot(),
            }));
        let old_authority = state
            .current_model_provider_runtime()
            .health_authority_by_provider
            .get("openai-primary")
            .cloned()
            .expect("audio provider health authority should exist");
        let request_state = Arc::clone(&state);
        let request = tokio::spawn(async move {
            request_state
                .execute_audio_transcription(AudioTranscriptionRequest {
                    file_name: "voice.wav".to_owned(),
                    content_type: "audio/wav".to_owned(),
                    bytes: vec![1, 2, 3],
                    prompt: None,
                    language: None,
                })
                .await
        });

        started_rx.recv().await.expect("audio candidate should start");
        let replacement_generation =
            state.configure_model_provider(Arc::new(RateLimitedFailoverModelProvider));
        assert!(replacement_generation > initial_generation);
        release.notify_one();

        let error = request
            .await
            .expect("audio transcription task should join")
            .expect_err("stale audio result should be suppressed");
        assert_eq!(error.code(), Code::Aborted);
        let health = state
            .journal_store
            .runtime_component_health(old_authority.component_id.as_str())
            .expect("provider health should load")
            .expect("provider health should exist");
        assert_ne!(health.generation, old_authority.generation);
        assert_eq!(health.state, RuntimeHealthState::Healthy);
        assert_eq!(health.strike_count, 0);
    }

    #[tokio::test]
    async fn audio_transcription_provider_reconfiguration_suppresses_stale_success_and_counters() {
        let state = test_runtime_state();
        let (started_tx, mut started_rx) = mpsc::channel(1);
        let release = Arc::new(Notify::new());
        let initial_generation =
            state.configure_model_provider(Arc::new(BlockingAudioTranscriptionProvider {
                started: started_tx,
                release: Arc::clone(&release),
                result: Ok(AudioTranscriptionResponse {
                    text: "stale transcript".to_owned(),
                    language: Some("en".to_owned()),
                    duration_ms: Some(100),
                    model_name: "stale-audio-model".to_owned(),
                    retry_count: 2,
                    segments: Vec::new(),
                }),
                status: state.model_provider_status_snapshot(),
            }));
        let request_state = Arc::clone(&state);
        let request = tokio::spawn(async move {
            request_state
                .execute_audio_transcription(AudioTranscriptionRequest {
                    file_name: "voice.wav".to_owned(),
                    content_type: "audio/wav".to_owned(),
                    bytes: vec![1, 2, 3],
                    prompt: None,
                    language: None,
                })
                .await
        });

        started_rx.recv().await.expect("audio transcription should start");
        let replacement_generation =
            state.configure_model_provider(Arc::new(RateLimitedFailoverModelProvider));
        assert!(replacement_generation > initial_generation);
        release.notify_one();

        let error = request
            .await
            .expect("audio transcription task should join")
            .expect_err("stale transcript should be suppressed");
        assert_eq!(error.code(), Code::Aborted);
        assert_eq!(
            error.message(),
            "model provider response was suppressed after provider reconfiguration"
        );
        let counters = state.counters.snapshot();
        assert_eq!(counters.model_provider_requests, 1);
        assert_eq!(counters.model_provider_failures, 0);
        assert_eq!(counters.model_provider_retry_attempts, 0);
        assert_eq!(counters.model_provider_circuit_open_rejections, 0);
    }

    #[tokio::test]
    async fn audio_transcription_provider_reconfiguration_suppresses_stale_error_and_counters() {
        let state = test_runtime_state();
        let (started_tx, mut started_rx) = mpsc::channel(1);
        let release = Arc::new(Notify::new());
        let initial_generation =
            state.configure_model_provider(Arc::new(BlockingAudioTranscriptionProvider {
                started: started_tx,
                release: Arc::clone(&release),
                result: Err(ProviderError::CircuitOpen { retry_after_ms: 500 }),
                status: state.model_provider_status_snapshot(),
            }));
        let request_state = Arc::clone(&state);
        let request = tokio::spawn(async move {
            request_state
                .execute_audio_transcription(AudioTranscriptionRequest {
                    file_name: "voice.wav".to_owned(),
                    content_type: "audio/wav".to_owned(),
                    bytes: vec![1, 2, 3],
                    prompt: None,
                    language: None,
                })
                .await
        });

        started_rx.recv().await.expect("audio transcription should start");
        let replacement_generation =
            state.configure_model_provider(Arc::new(RateLimitedFailoverModelProvider));
        assert!(replacement_generation > initial_generation);
        release.notify_one();

        let error = request
            .await
            .expect("audio transcription task should join")
            .expect_err("stale audio failure should be suppressed");
        assert_eq!(error.code(), Code::Aborted);
        let counters = state.counters.snapshot();
        assert_eq!(counters.model_provider_requests, 1);
        assert_eq!(counters.model_provider_failures, 0);
        assert_eq!(counters.model_provider_retry_attempts, 0);
        assert_eq!(counters.model_provider_circuit_open_rejections, 0);
    }

    #[test]
    fn memory_content_limit_validation_rejects_oversized_updates() {
        let byte_config =
            MemoryRuntimeConfig { max_item_bytes: 12, max_item_tokens: 32, ..Default::default() };

        let byte_error = validate_memory_item_content_limits("0123456789abc", &byte_config)
            .expect_err("content above byte limit should be rejected");
        assert_eq!(byte_error.code(), Code::InvalidArgument);
        assert!(
            byte_error.message().contains("exceeds byte limit"),
            "unexpected byte-limit error: {byte_error}"
        );

        let token_config =
            MemoryRuntimeConfig { max_item_bytes: 128, max_item_tokens: 3, ..Default::default() };
        let token_error = validate_memory_item_content_limits("one two three four", &token_config)
            .expect_err("content above token limit should be rejected");
        assert_eq!(token_error.code(), Code::InvalidArgument);
        assert!(
            token_error.message().contains("exceeds token limit"),
            "unexpected token-limit error: {token_error}"
        );
    }

    #[test]
    fn fallback_workspace_document_search_finds_unindexed_active_documents() {
        let document = WorkspaceDocumentRecord {
            document_id: "workspace-doc-1".to_owned(),
            principal: "principal-1".to_owned(),
            channel: None,
            agent_id: None,
            latest_session_id: Some("session-1".to_owned()),
            path: "projects/S033/MEMORY.md".to_owned(),
            parent_path: Some("projects/S033".to_owned()),
            title: "Project Memory".to_owned(),
            kind: "memory".to_owned(),
            document_class: "workspace_memory".to_owned(),
            state: "active".to_owned(),
            prompt_binding: "system_candidate".to_owned(),
            risk_state: "clean".to_owned(),
            risk_reasons: Vec::new(),
            pinned: false,
            manual_override: false,
            template_id: None,
            template_version: None,
            source_memory_id: None,
            latest_version: 1,
            content_text: "- remembered_at_unix_ms=1780430000000 source=manual\n  S033-PREF-20260602 prefers TypeScript, Vitest, and short concise reports.".to_owned(),
            content_hash: "hash-1".to_owned(),
            created_at_unix_ms: 1_780_430_000_000,
            updated_at_unix_ms: 1_780_430_000_000,
            deleted_at_unix_ms: None,
            last_recalled_at_unix_ms: None,
        };
        let request = WorkspaceSearchRequest {
            principal: "principal-1".to_owned(),
            channel: None,
            agent_id: None,
            query: "S033-PREF-20260602 Vitest".to_owned(),
            prefix: Some("projects/S033".to_owned()),
            top_k: 4,
            min_score: 0.0,
            include_historical: false,
            include_quarantined: false,
        };

        let hits = fallback_workspace_document_search_hits(
            vec![document],
            &request,
            &RetrievalRuntimeConfig::default(),
            1_780_430_000_000,
        );

        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].document.path, "projects/S033/MEMORY.md");
        assert!(hits[0].snippet.contains("S033-PREF-20260602"));
        assert!(hits[0].reason.contains("journal_document_fallback"));
    }

    #[test]
    fn default_agent_model_profile_uses_active_model_when_registry_default_is_blank() {
        let selected = select_default_agent_model_profile(
            Some("   "),
            Some("MiniMax-M2.7"),
            Some("gpt-4o-mini"),
            Some("claude-3-5-sonnet-latest"),
        );

        assert_eq!(selected.as_deref(), Some("MiniMax-M2.7"));
    }

    fn provider_lease_context(
        provider_id: &str,
        credential_id: &str,
    ) -> ProviderLeaseExecutionContext {
        ProviderLeaseExecutionContext {
            provider_id: provider_id.to_owned(),
            credential_id: credential_id.to_owned(),
            priority: LeasePriority::Foreground,
            task_label: "primary_interactive".to_owned(),
            max_wait_ms: 30_000,
            session_id: Some("session-1".to_owned()),
            run_id: Some("run-1".to_owned()),
            runtime_authority: None,
            diagnostic_scope_id: None,
        }
    }

    fn test_provider_attempt_admission(
        state: &Arc<GatewayRuntimeState>,
    ) -> GatewayProviderAttemptAdmission {
        let provider_runtime = state.current_model_provider_runtime();
        GatewayProviderAttemptAdmission {
            runtime_state: Arc::clone(state),
            lease_context: provider_lease_context(
                "openai-primary",
                "auth-profile:openai-primary:primary-profile",
            ),
            expected_configuration_epoch: provider_runtime.configuration_epoch,
            health_authority_by_provider: Arc::new(
                provider_runtime.health_authority_by_provider.clone(),
            ),
            feedback: Arc::new(Mutex::new(Vec::new())),
            attempted_profile_ids: Arc::new(Mutex::new(std::collections::BTreeMap::new())),
            fail_health_observation_once: None,
        }
    }

    fn gateway_provider_attempt_authority(
        authority: Box<dyn crate::model_provider::ProviderAttemptRuntimeAuthority>,
    ) -> GatewayProviderAttemptRuntimeAuthorityGuard {
        let guard = authority
            .as_ref()
            .as_any()
            .downcast_ref::<GatewayProviderAttemptRuntimeAuthorityGuard>()
            .expect("gateway provider attempt should return guarded journal authority");
        guard.settled.store(true, Ordering::Release);
        guard.clone()
    }

    pub(crate) fn start_test_orchestrator_run(
        state: &GatewayRuntimeState,
        session_id: &str,
        run_id: &str,
    ) {
        state
            .journal_store
            .upsert_orchestrator_session(&OrchestratorSessionUpsertRequest {
                session_id: session_id.to_owned(),
                session_key: session_id.to_owned(),
                session_label: None,
                principal: "user:test".to_owned(),
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                channel: Some("test".to_owned()),
            })
            .expect("test orchestrator session should be created");
        state
            .journal_store
            .start_orchestrator_run(&OrchestratorRunStartRequest {
                run_id: run_id.to_owned(),
                session_id: session_id.to_owned(),
                origin_kind: "test".to_owned(),
                origin_run_id: None,
                triggered_by_principal: Some("user:test".to_owned()),
                parameter_delta_json: None,
                delegated_admission: None,
            })
            .expect("test orchestrator run should be created");
    }

    pub(crate) struct BlockingModelProvider {
        pub(crate) started: mpsc::Sender<()>,
        pub(crate) release: Arc<Notify>,
        pub(crate) response_text: &'static str,
        pub(crate) status: ProviderStatusSnapshot,
    }

    impl BlockingModelProvider {
        fn new(
            started: mpsc::Sender<()>,
            release: Arc<Notify>,
            status: ProviderStatusSnapshot,
        ) -> Self {
            Self { started, release, response_text: "stale response", status }
        }
    }

    impl ModelProvider for BlockingModelProvider {
        fn complete<'a>(
            &'a self,
            _request: ProviderRequest,
        ) -> Pin<Box<dyn Future<Output = Result<ProviderResponse, ProviderError>> + Send + 'a>>
        {
            Box::pin(async move {
                self.started
                    .send(())
                    .await
                    .expect("stale provider test receiver should remain open");
                self.release.notified().await;
                let output = ProviderTurnOutput::text(
                    self.response_text.to_owned(),
                    ProviderFinishReason::Stop,
                    ProviderUsage::new(1, 2, "test"),
                    ProviderRawProviderRefs::default(),
                );
                Ok(ProviderResponse {
                    events: palyra_model_providers::provider_events_from_output(&output),
                    prompt_tokens: output.usage.prompt_tokens,
                    completion_tokens: output.usage.completion_tokens,
                    output,
                    retry_count: 0,
                    provider_id: "blocking-provider".to_owned(),
                    model_id: "blocking-model".to_owned(),
                    served_from_cache: false,
                    failover_count: 0,
                    attempts: vec![ProviderAttemptSummary {
                        provider_id: "blocking-provider".to_owned(),
                        model_id: "blocking-model".to_owned(),
                        outcome: "success".to_owned(),
                        retryable: false,
                        served_from_cache: false,
                        reason_code: None,
                        state: None,
                    }],
                    qa_lane_attestation: None,
                })
            })
        }

        fn transcribe_audio<'a>(
            &'a self,
            _request: AudioTranscriptionRequest,
        ) -> Pin<
            Box<dyn Future<Output = Result<AudioTranscriptionResponse, ProviderError>> + Send + 'a>,
        > {
            Box::pin(async { Err(ProviderError::MissingApiKey) })
        }

        fn status_snapshot(&self) -> ProviderStatusSnapshot {
            self.status.clone()
        }
    }

    struct BlockingAudioTranscriptionProvider {
        started: mpsc::Sender<()>,
        release: Arc<Notify>,
        result: Result<AudioTranscriptionResponse, ProviderError>,
        status: ProviderStatusSnapshot,
    }

    struct CandidateAudioTranscriptionProvider {
        started: Option<mpsc::Sender<()>>,
        release: Option<Arc<Notify>>,
        result: Result<AudioTranscriptionResponse, ProviderError>,
        status: ProviderStatusSnapshot,
    }

    struct ConfigurableProbeProvider {
        status: ProviderStatusSnapshot,
        result: Result<(), ProviderError>,
        started: Option<mpsc::Sender<()>>,
        release: Option<Arc<Notify>>,
    }

    impl ModelProvider for BlockingAudioTranscriptionProvider {
        fn complete<'a>(
            &'a self,
            _request: ProviderRequest,
        ) -> Pin<Box<dyn Future<Output = Result<ProviderResponse, ProviderError>> + Send + 'a>>
        {
            Box::pin(async { Err(ProviderError::MissingApiKey) })
        }

        fn transcribe_audio<'a>(
            &'a self,
            _request: AudioTranscriptionRequest,
        ) -> Pin<
            Box<dyn Future<Output = Result<AudioTranscriptionResponse, ProviderError>> + Send + 'a>,
        > {
            Box::pin(async move {
                self.started
                    .send(())
                    .await
                    .expect("audio supersession test receiver should remain open");
                self.release.notified().await;
                self.result.clone()
            })
        }

        fn status_snapshot(&self) -> ProviderStatusSnapshot {
            self.status.clone()
        }
    }

    impl ModelProvider for CandidateAudioTranscriptionProvider {
        fn complete<'a>(
            &'a self,
            _request: ProviderRequest,
        ) -> Pin<Box<dyn Future<Output = Result<ProviderResponse, ProviderError>> + Send + 'a>>
        {
            Box::pin(async { Err(ProviderError::StatePoisoned) })
        }

        fn uses_candidate_attempt_admission(&self) -> bool {
            true
        }

        fn probe_with_attempt_admission<'a>(
            &'a self,
            target: ProviderHealthProbeTarget,
            admission: Arc<dyn ProviderProbeAdmission>,
        ) -> Pin<Box<dyn Future<Output = Result<(), ProviderError>> + Send + 'a>> {
            Box::pin(async move {
                let binding = admission
                    .bind_probe(
                        target.provider_id.as_str(),
                        target.credential_id.as_str(),
                        target.model_id.as_str(),
                    )
                    .map_err(provider_attempt_admission_provider_error)?;
                admission
                    .check_probe_eligibility(&binding)
                    .map_err(provider_attempt_admission_provider_error)?;
                Ok(())
            })
        }

        fn transcribe_audio<'a>(
            &'a self,
            _request: AudioTranscriptionRequest,
        ) -> Pin<
            Box<dyn Future<Output = Result<AudioTranscriptionResponse, ProviderError>> + Send + 'a>,
        > {
            Box::pin(async { Err(ProviderError::StatePoisoned) })
        }

        fn transcribe_audio_with_attempt_admission<'a>(
            &'a self,
            _request: AudioTranscriptionRequest,
            admission: Arc<dyn ProviderAttemptAdmission>,
        ) -> Pin<
            Box<dyn Future<Output = Result<AudioTranscriptionResponse, ProviderError>> + Send + 'a>,
        > {
            Box::pin(async move {
                let binding = admission
                    .bind_attempt(
                        "openai-primary",
                        "auth-profile:openai-primary:primary-profile",
                        "whisper-1",
                    )
                    .map_err(provider_attempt_admission_provider_error)?;
                admission
                    .check_eligibility(&binding)
                    .map_err(provider_attempt_admission_provider_error)?;
                let _permit = admission
                    .acquire(&binding)
                    .await
                    .map_err(provider_attempt_admission_provider_error)?;
                let runtime_authority = admission
                    .record_started(&binding)
                    .await
                    .map_err(provider_attempt_admission_provider_error)?;
                if let Some(started) = self.started.as_ref() {
                    started
                        .send(())
                        .await
                        .expect("audio candidate test receiver should remain open");
                }
                if let Some(release) = self.release.as_ref() {
                    release.notified().await;
                }
                match self.result.clone() {
                    Ok(response) => {
                        if admission
                            .record_success(&binding, runtime_authority)
                            .await
                            .map_err(provider_attempt_admission_provider_error)?
                            == ProviderAttemptCompletionDisposition::StaleSuppressed
                        {
                            return Err(crate::model_provider::provider_attempt_superseded_error());
                        }
                        Ok(response)
                    }
                    Err(error) => {
                        if admission
                            .record_failure(&binding, runtime_authority, &error)
                            .await
                            .map_err(provider_attempt_admission_provider_error)?
                            == ProviderAttemptCompletionDisposition::StaleSuppressed
                        {
                            return Err(crate::model_provider::provider_attempt_superseded_error());
                        }
                        Err(error)
                    }
                }
            })
        }

        fn status_snapshot(&self) -> ProviderStatusSnapshot {
            self.status.clone()
        }
    }

    impl ModelProvider for ConfigurableProbeProvider {
        fn complete<'a>(
            &'a self,
            _request: ProviderRequest,
        ) -> Pin<Box<dyn Future<Output = Result<ProviderResponse, ProviderError>> + Send + 'a>>
        {
            Box::pin(async { Err(ProviderError::StatePoisoned) })
        }

        fn uses_candidate_attempt_admission(&self) -> bool {
            true
        }

        fn probe_with_attempt_admission<'a>(
            &'a self,
            target: ProviderHealthProbeTarget,
            admission: Arc<dyn ProviderProbeAdmission>,
        ) -> Pin<Box<dyn Future<Output = Result<(), ProviderError>> + Send + 'a>> {
            Box::pin(async move {
                let binding = admission
                    .bind_probe(
                        target.provider_id.as_str(),
                        target.credential_id.as_str(),
                        target.model_id.as_str(),
                    )
                    .map_err(provider_attempt_admission_provider_error)?;
                admission
                    .check_probe_eligibility(&binding)
                    .map_err(provider_attempt_admission_provider_error)?;
                if let Some(started) = self.started.as_ref() {
                    started
                        .send(())
                        .await
                        .expect("provider probe test receiver should remain open");
                }
                if let Some(release) = self.release.as_ref() {
                    release.notified().await;
                }
                self.result.clone()
            })
        }

        fn transcribe_audio<'a>(
            &'a self,
            _request: AudioTranscriptionRequest,
        ) -> Pin<
            Box<dyn Future<Output = Result<AudioTranscriptionResponse, ProviderError>> + Send + 'a>,
        > {
            Box::pin(async { Err(ProviderError::StatePoisoned) })
        }

        fn status_snapshot(&self) -> ProviderStatusSnapshot {
            self.status.clone()
        }
    }

    pub(crate) struct SuccessfulModelProvider {
        pub(crate) requests: mpsc::Sender<ProviderRequest>,
        pub(crate) response_text: &'static str,
        pub(crate) status: ProviderStatusSnapshot,
    }

    impl ModelProvider for SuccessfulModelProvider {
        fn complete<'a>(
            &'a self,
            request: ProviderRequest,
        ) -> Pin<Box<dyn Future<Output = Result<ProviderResponse, ProviderError>> + Send + 'a>>
        {
            Box::pin(async move {
                self.requests
                    .send(request)
                    .await
                    .expect("replacement provider receiver should remain open");
                let output = ProviderTurnOutput::text(
                    self.response_text.to_owned(),
                    ProviderFinishReason::Stop,
                    ProviderUsage::new(1, 2, "test"),
                    ProviderRawProviderRefs::default(),
                );
                let provider_id = self.status.provider_id.clone();
                let model_id =
                    self.status.model_id.clone().unwrap_or_else(|| "replacement".to_owned());
                Ok(ProviderResponse {
                    events: palyra_model_providers::provider_events_from_output(&output),
                    prompt_tokens: output.usage.prompt_tokens,
                    completion_tokens: output.usage.completion_tokens,
                    output,
                    retry_count: 0,
                    provider_id: provider_id.clone(),
                    model_id: model_id.clone(),
                    served_from_cache: false,
                    failover_count: 0,
                    attempts: vec![ProviderAttemptSummary {
                        provider_id,
                        model_id,
                        outcome: "success".to_owned(),
                        retryable: false,
                        served_from_cache: false,
                        reason_code: None,
                        state: None,
                    }],
                    qa_lane_attestation: None,
                })
            })
        }

        fn transcribe_audio<'a>(
            &'a self,
            _request: AudioTranscriptionRequest,
        ) -> Pin<
            Box<dyn Future<Output = Result<AudioTranscriptionResponse, ProviderError>> + Send + 'a>,
        > {
            Box::pin(async { Err(ProviderError::MissingApiKey) })
        }

        fn status_snapshot(&self) -> ProviderStatusSnapshot {
            self.status.clone()
        }
    }

    struct BlockingCandidateAdmissionProvider {
        started: mpsc::Sender<()>,
        release: Arc<Notify>,
    }

    impl ModelProvider for BlockingCandidateAdmissionProvider {
        fn complete<'a>(
            &'a self,
            _request: ProviderRequest,
        ) -> Pin<Box<dyn Future<Output = Result<ProviderResponse, ProviderError>> + Send + 'a>>
        {
            Box::pin(async { Err(ProviderError::StatePoisoned) })
        }

        fn uses_candidate_attempt_admission(&self) -> bool {
            true
        }

        fn complete_with_attempt_admission<'a>(
            &'a self,
            _request: ProviderRequest,
            admission: Arc<dyn ProviderAttemptAdmission>,
        ) -> Pin<Box<dyn Future<Output = Result<ProviderResponse, ProviderError>> + Send + 'a>>
        {
            Box::pin(async move {
                let binding = admission
                    .bind_attempt(
                        "openai-primary",
                        "auth-profile:openai-primary:primary-profile",
                        "gpt-4o-mini",
                    )
                    .expect("candidate health authority should bind");
                let _permit = admission
                    .acquire(&binding)
                    .await
                    .map_err(provider_attempt_admission_provider_error)?;
                let runtime_authority = admission
                    .record_started(&binding)
                    .await
                    .map_err(provider_attempt_admission_provider_error)?;
                let error = rate_limit_provider_error();
                admission
                    .record_failure(&binding, runtime_authority, &error)
                    .await
                    .map_err(provider_attempt_admission_provider_error)?;
                self.started
                    .send(())
                    .await
                    .expect("candidate feedback test receiver should remain open");
                self.release.notified().await;
                Err(rate_limit_provider_error())
            })
        }

        fn transcribe_audio<'a>(
            &'a self,
            _request: AudioTranscriptionRequest,
        ) -> Pin<
            Box<dyn Future<Output = Result<AudioTranscriptionResponse, ProviderError>> + Send + 'a>,
        > {
            Box::pin(async { Err(ProviderError::MissingApiKey) })
        }

        fn status_snapshot(&self) -> ProviderStatusSnapshot {
            provider_status_snapshot(false)
        }
    }

    struct RateLimitedFailoverModelProvider;

    impl ModelProvider for RateLimitedFailoverModelProvider {
        fn complete<'a>(
            &'a self,
            _request: ProviderRequest,
        ) -> Pin<Box<dyn Future<Output = Result<ProviderResponse, ProviderError>> + Send + 'a>>
        {
            Box::pin(async { Err(rate_limit_provider_error()) })
        }

        fn uses_candidate_attempt_admission(&self) -> bool {
            true
        }

        fn complete_with_attempt_admission<'a>(
            &'a self,
            _request: ProviderRequest,
            admission: Arc<dyn ProviderAttemptAdmission>,
        ) -> Pin<Box<dyn Future<Output = Result<ProviderResponse, ProviderError>> + Send + 'a>>
        {
            Box::pin(async move {
                let binding = admission
                    .bind_attempt(
                        "openai-primary",
                        "auth-profile:openai-primary:primary-profile",
                        "gpt-4o-mini",
                    )
                    .map_err(provider_attempt_admission_provider_error)?;
                let _permit = admission
                    .acquire(&binding)
                    .await
                    .map_err(provider_attempt_admission_provider_error)?;
                let runtime_authority = admission
                    .record_started(&binding)
                    .await
                    .map_err(provider_attempt_admission_provider_error)?;
                let error = rate_limit_provider_error();
                admission
                    .record_failure(&binding, runtime_authority, &error)
                    .await
                    .map_err(provider_attempt_admission_provider_error)?;
                Err(error)
            })
        }

        fn transcribe_audio<'a>(
            &'a self,
            _request: AudioTranscriptionRequest,
        ) -> Pin<
            Box<dyn Future<Output = Result<AudioTranscriptionResponse, ProviderError>> + Send + 'a>,
        > {
            Box::pin(async { Err(ProviderError::MissingApiKey) })
        }

        fn status_snapshot(&self) -> ProviderStatusSnapshot {
            provider_status_snapshot(true)
        }
    }

    fn rate_limit_provider_error() -> ProviderError {
        ProviderError::RequestFailed {
            message: "rate limit exceeded".to_owned(),
            retryable: true,
            retry_count: 0,
            classification: classify_http_provider_failure(
                429,
                true,
                "openai_chat_http",
                "rate limit exceeded",
            ),
        }
    }

    pub(crate) fn test_runtime_state() -> Arc<GatewayRuntimeState> {
        let db_path =
            unique_runtime_test_root("palyra-runtime-feedback-journal").join("events.sqlite3");
        test_runtime_state_for_journal(db_path)
    }

    fn test_runtime_state_with_code_intel() -> Arc<GatewayRuntimeState> {
        test_runtime_state_with_code_intel_idle_reap(
            crate::config::CodeIntelConfig::default().idle_reap_ms,
        )
    }

    fn test_runtime_state_with_code_intel_idle_reap(idle_reap_ms: u64) -> Arc<GatewayRuntimeState> {
        let state_root = unique_runtime_test_root("palyra-runtime-code-intel-state");
        let db_path = state_root.join("events.sqlite3");
        let agent_registry = AgentRegistry::open_for_test_state_root(state_root.as_path())
            .expect("test agent registry should initialize");
        let journal_store = JournalStore::open(JournalConfig {
            db_path: db_path.clone(),
            hash_chain_enabled: false,
            max_payload_bytes: 256 * 1024,
            max_events: 10_000,
        })
        .expect("test journal store should initialize");
        let mut config = test_runtime_config();
        config.code_intel.enabled = true;
        config.code_intel.idle_reap_ms = idle_reap_ms;

        GatewayRuntimeState::new(
            config,
            GatewayJournalConfigSnapshot { db_path, hash_chain_enabled: false },
            journal_store,
            0,
            agent_registry,
        )
        .expect("test runtime state should initialize")
    }

    pub(crate) fn test_runtime_state_for_journal(
        db_path: std::path::PathBuf,
    ) -> Arc<GatewayRuntimeState> {
        let state_root = unique_runtime_test_root("palyra-runtime-feedback-state");
        let agent_registry = AgentRegistry::open_for_test_state_root(state_root.as_path())
            .expect("test agent registry should initialize");
        let journal_store = JournalStore::open(JournalConfig {
            db_path: db_path.clone(),
            hash_chain_enabled: false,
            max_payload_bytes: 256 * 1024,
            max_events: 10_000,
        })
        .expect("test journal store should initialize");

        GatewayRuntimeState::new(
            test_runtime_config(),
            GatewayJournalConfigSnapshot { db_path, hash_chain_enabled: false },
            journal_store,
            0,
            agent_registry,
        )
        .expect("test runtime state should initialize")
    }

    fn test_runtime_state_with_auth_profile() -> (Arc<GatewayRuntimeState>, Arc<AuthProfileRegistry>)
    {
        let state_root = unique_runtime_test_root("palyra-runtime-feedback-auth");
        // Auth state lives beside the identity store by default. Nest the
        // identity root so parallel tests do not share the system temp parent.
        let identity_root = state_root.join("identity");
        std::fs::create_dir_all(identity_root.as_path())
            .expect("test identity root should initialize");
        let auth_registry = Arc::new(
            AuthProfileRegistry::open(identity_root.as_path())
                .expect("test auth profile registry should initialize"),
        );
        auth_registry
            .set_profile(AuthProfileSetRequest {
                profile_id: "primary-profile".to_owned(),
                provider: AuthProvider::known(AuthProviderKind::Openai),
                profile_name: "Primary profile".to_owned(),
                scope: AuthProfileScope::Global,
                credential: AuthCredential::ApiKey {
                    api_key_vault_ref: "global/provider_test_key".to_owned(),
                },
            })
            .expect("test auth profile should persist");
        let mut state = test_runtime_state();
        let scope = "global".parse::<VaultScope>().expect("test vault scope should parse");
        state
            .vault
            .put_secret(&scope, "provider_test_key", b"test-provider-key")
            .expect("test provider key should persist");
        Arc::get_mut(&mut state)
            .expect("new test runtime state should have one strong owner")
            .auth_profile_registry = Some(Arc::clone(&auth_registry));
        (state, auth_registry)
    }

    fn provider_completion_event_count(state: &GatewayRuntimeState, run_id: &str) -> i64 {
        let connection = rusqlite::Connection::open(state.journal_config.db_path.as_path())
            .expect("test journal query connection should open");
        connection
            .query_row(
                "SELECT COUNT(*) FROM runtime_events_v2 WHERE run_ulid = ?1 AND event_name = ?2",
                rusqlite::params![run_id, RuntimeEventName::ProviderAttemptCompleted.as_str()],
                |row| row.get(0),
            )
            .expect("provider completion event count should load")
    }

    fn provider_configuration_completion_count(state: &GatewayRuntimeState) -> i64 {
        let connection = rusqlite::Connection::open(state.journal_config.db_path.as_path())
            .expect("test journal query connection should open");
        connection
            .query_row("SELECT COUNT(*) FROM runtime_provider_attempt_completions", [], |row| {
                row.get(0)
            })
            .expect("configuration-scoped provider completion count should load")
    }

    fn provider_health_event_count(
        state: &GatewayRuntimeState,
        component_id: &str,
        reason_code: &str,
    ) -> i64 {
        let connection = rusqlite::Connection::open(state.journal_config.db_path.as_path())
            .expect("test journal query connection should open");
        connection
            .query_row(
                "SELECT COUNT(*) FROM runtime_component_health_events WHERE component_ulid = ?1 AND reason_code = ?2",
                rusqlite::params![component_id, reason_code],
                |row| row.get(0),
            )
            .expect("provider health event count should load")
    }

    fn unique_runtime_test_root(prefix: &str) -> std::path::PathBuf {
        let root = std::env::temp_dir().join(format!(
            "{prefix}-{}-{}",
            std::process::id(),
            ulid::Ulid::new()
        ));
        std::fs::create_dir_all(root.as_path()).expect("test runtime temp root should exist");
        root
    }

    fn test_runtime_config() -> GatewayRuntimeConfigSnapshot {
        let model_provider_request_timeout_ms =
            crate::model_provider::ModelProviderConfig::default().request_timeout_ms;

        GatewayRuntimeConfigSnapshot {
            grpc_bind_addr: "127.0.0.1".to_owned(),
            grpc_port: 7443,
            quic_bind_addr: "127.0.0.1".to_owned(),
            quic_port: 7444,
            quic_enabled: true,
            orchestrator_runloop_v1_enabled: true,
            model_provider_request_timeout_ms,
            qa_execution_key_digest: None,
            qa_provider_binding_sha256: None,
            node_rpc_mtls_required: true,
            admin_auth_required: true,
            vault_get_approval_required_refs: vec!["global/openai_api_key".to_owned()],
            max_tape_entries_per_response: 1_000,
            max_tape_bytes_per_response: 2 * 1024 * 1024,
            feature_rollouts: crate::config::FeatureRolloutsConfig::default(),
            session_queue_policy: crate::config::SessionQueuePolicyConfig::default(),
            pruning_policy_matrix: crate::config::PruningPolicyMatrixConfig::default(),
            retrieval_dual_path: crate::config::RetrievalDualPathConfig::default(),
            auxiliary_executor: crate::config::AuxiliaryExecutorConfig::default(),
            flow_orchestration: crate::config::FlowOrchestrationConfig::default(),
            delivery_arbitration: crate::config::DeliveryArbitrationConfig::default(),
            replay_capture: crate::config::ReplayCaptureConfig::default(),
            networked_workers: crate::config::NetworkedWorkersConfig::default(),
            mcp_servers: crate::config::McpServersConfig::default(),
            plugin_binding_ids: Vec::new(),
            execution_backend_profiles: crate::config::ExecutionBackendProfilesConfig::default(),
            agent_harness_registry: crate::config::AgentHarnessRegistryConfig::default(),
            channel_router: crate::channel_router::ChannelRouterConfig::default(),
            media: MediaRuntimeConfig::default(),
            code_intel: crate::config::CodeIntelConfig::default(),
            tool_catalog_policy:
                crate::application::tool_registry::ToolCatalogPolicySnapshot::direct_from_allowed_tools(
                    &["palyra.echo".to_owned()],
                ),
            tool_call: test_tool_call_config(),
            http_fetch: HttpFetchRuntimeConfig {
                allow_private_targets: false,
                connect_timeout_ms: 1_500,
                request_timeout_ms: 10_000,
                max_response_bytes: 512 * 1024,
                allow_redirects: true,
                max_redirects: 3,
                allowed_content_types: vec![
                    "text/html".to_owned(),
                    "text/plain".to_owned(),
                    "application/json".to_owned(),
                ],
                allowed_request_headers: vec![
                    "accept".to_owned(),
                    "accept-language".to_owned(),
                    "content-type".to_owned(),
                    "if-none-match".to_owned(),
                    "if-modified-since".to_owned(),
                    "user-agent".to_owned(),
                    "x-client-version".to_owned(),
                ],
                allowed_credential_vault_refs: Vec::new(),
                cache_enabled: true,
                cache_ttl_ms: 30_000,
                max_cache_entries: 256,
            },
            browser_service: BrowserServiceRuntimeConfig {
                enabled: false,
                endpoint: "http://127.0.0.1:7543".to_owned(),
                auth_token: None,
                connect_timeout_ms: 1_500,
                request_timeout_ms: 15_000,
                max_screenshot_bytes: 256 * 1024,
                max_title_bytes: 4 * 1024,
            },
            canvas_host: CanvasHostRuntimeConfig {
                enabled: true,
                public_base_url: "http://127.0.0.1:7142".to_owned(),
                token_ttl_ms: 15 * 60 * 1_000,
                max_state_bytes: 64 * 1024,
                max_bundle_bytes: 512 * 1024,
                max_assets_per_bundle: 32,
                max_updates_per_minute: 120,
            },
            smart_routing: crate::usage_governance::SmartRoutingRuntimeConfig {
                enabled: true,
                default_mode: "suggest".to_owned(),
                auxiliary_routing_enabled: true,
            },
        }
    }

    fn test_tool_call_config() -> crate::tool_protocol::ToolCallConfig {
        crate::tool_protocol::ToolCallConfig {
            allowed_tools: vec!["palyra.echo".to_owned()],
            max_calls_per_run: 4,
            execution_timeout_ms: 250,
            process_runner: crate::sandbox_runner::SandboxProcessRunnerPolicy {
                enabled: false,
                tier: crate::sandbox_runner::SandboxProcessRunnerTier::B,
                workspace_root: std::path::PathBuf::from("."),
                path_access_mode: crate::sandbox_runner::PathAccessMode::WorkspaceOnly,
                allowed_executables: Vec::new(),
                allow_interpreters: false,
                egress_enforcement_mode: crate::sandbox_runner::EgressEnforcementMode::Strict,
                allowed_egress_hosts: Vec::new(),
                allowed_dns_suffixes: Vec::new(),
                cpu_time_limit_ms: 2_000,
                memory_limit_bytes: 256 * 1024 * 1024,
                max_output_bytes: 64 * 1024,
            },
            wasm_runtime: crate::wasm_plugin_runner::WasmPluginRunnerPolicy {
                enabled: false,
                allow_inline_modules: false,
                max_module_size_bytes: 256 * 1024,
                fuel_budget: 10_000_000,
                max_memory_bytes: 64 * 1024 * 1024,
                max_table_elements: 100_000,
                max_instances: 256,
                allowed_http_hosts: Vec::new(),
                allowed_secrets: Vec::new(),
                allowed_storage_prefixes: Vec::new(),
                allowed_channels: Vec::new(),
            },
        }
    }

    fn expire_provider_health_cooldown(
        state: &Arc<GatewayRuntimeState>,
        provider_id: &str,
    ) -> ProviderAttemptHealthAuthority {
        let authority = state
            .current_model_provider_runtime()
            .health_authority_by_provider
            .get(provider_id)
            .cloned()
            .expect("provider health authority should exist");
        let mut cooldown = state
            .journal_store
            .record_runtime_health_observation(&RuntimeHealthObservationRequest {
                component_id: authority.component_id.clone(),
                expected_generation: authority.generation,
                succeeded: false,
                reason_code: "runtime.health.provider_call_failed".to_owned(),
                observed_at_unix_ms: current_unix_ms(),
            })
            .expect("threshold failure should enter cooldown")
            .health;
        cooldown.expires_at_unix_ms = Some(cooldown.updated_at_unix_ms);
        state
            .journal_store
            .upsert_runtime_component_health(&cooldown)
            .expect("test cooldown should be expired");
        authority
    }

    pub(crate) fn provider_status_snapshot(failover_enabled: bool) -> ProviderStatusSnapshot {
        ProviderStatusSnapshot {
            kind: "registry".to_owned(),
            provider_id: "openai-primary".to_owned(),
            credential_id: "auth-profile:openai-primary:primary-profile".to_owned(),
            model_id: Some("gpt-4o-mini".to_owned()),
            capabilities: provider_capabilities(),
            openai_base_url: None,
            anthropic_base_url: None,
            openai_model: Some("gpt-4o-mini".to_owned()),
            anthropic_model: None,
            openai_embeddings_model: None,
            openai_embeddings_dims: None,
            auth_profile_id: Some("primary-profile".to_owned()),
            auth_profile_provider_kind: Some("openai".to_owned()),
            credential_source: Some("auth_profile_api_key".to_owned()),
            api_key_configured: true,
            retry_policy: retry_policy(),
            circuit_breaker: circuit_breaker(),
            runtime_metrics: runtime_metrics(),
            response_cache: ProviderResponseCacheSnapshot {
                enabled: false,
                entry_count: 0,
                hit_count: 0,
                miss_count: 0,
            },
            health: health("ok"),
            discovery: discovery(),
            registry: ProviderRegistrySnapshot {
                default_chat_model_id: Some("gpt-4o-mini".to_owned()),
                default_embeddings_model_id: None,
                default_audio_transcription_model_id: None,
                failover_enabled,
                response_cache_enabled: false,
                providers: vec![
                    provider_snapshot(
                        "openai-primary",
                        "auth-profile:openai-primary:primary-profile",
                        "primary-profile",
                        "openai",
                    ),
                    provider_snapshot(
                        "anthropic-primary",
                        "auth-profile:anthropic-primary:fallback-profile",
                        "fallback-profile",
                        "anthropic",
                    ),
                ],
                credentials: Vec::new(),
                models: vec![
                    model_snapshot("gpt-4o-mini", "openai-primary"),
                    model_snapshot("claude-3-5-sonnet-latest", "anthropic-primary"),
                ],
            },
            route_selection: ProviderRouteSelectionTrace::empty(),
        }
    }

    fn provider_snapshot(
        provider_id: &str,
        credential_id: &str,
        auth_profile_id: &str,
        kind: &str,
    ) -> ProviderRegistryProviderSnapshot {
        ProviderRegistryProviderSnapshot {
            provider_id: provider_id.to_owned(),
            credential_id: credential_id.to_owned(),
            display_name: provider_id.to_owned(),
            kind: kind.to_owned(),
            enabled: true,
            endpoint_base_url: None,
            auth_profile_id: Some(auth_profile_id.to_owned()),
            auth_profile_provider_kind: Some(kind.to_owned()),
            credential_source: Some("auth_profile_api_key".to_owned()),
            api_key_configured: true,
            retry_policy: retry_policy(),
            circuit_breaker: circuit_breaker(),
            runtime_metrics: runtime_metrics(),
            health: health("ok"),
            discovery: discovery(),
        }
    }

    fn model_snapshot(model_id: &str, provider_id: &str) -> ProviderRegistryModelSnapshot {
        ProviderRegistryModelSnapshot {
            model_id: model_id.to_owned(),
            provider_id: provider_id.to_owned(),
            role: "chat".to_owned(),
            enabled: true,
            capabilities: provider_capabilities(),
        }
    }

    fn audio_provider_status_snapshot() -> ProviderStatusSnapshot {
        let mut snapshot = provider_status_snapshot(false);
        snapshot.model_id = Some("whisper-1".to_owned());
        snapshot.registry.default_chat_model_id = None;
        snapshot.registry.default_audio_transcription_model_id = Some("whisper-1".to_owned());
        snapshot.registry.models = vec![ProviderRegistryModelSnapshot {
            model_id: "whisper-1".to_owned(),
            provider_id: "openai-primary".to_owned(),
            role: "audio_transcription".to_owned(),
            enabled: true,
            capabilities: ProviderCapabilitiesSnapshot {
                streaming_tokens: false,
                tool_calls: false,
                json_mode: false,
                vision: false,
                audio_transcribe: true,
                embeddings: false,
                reasoning: false,
                reasoning_efforts: Vec::new(),
                service_tier: false,
                service_tiers: Vec::new(),
                max_context_tokens: None,
                cost_tier: "standard".to_owned(),
                latency_tier: "standard".to_owned(),
                recommended_use_cases: Vec::new(),
                known_limitations: Vec::new(),
                operator_override: false,
                metadata_source: "static".to_owned(),
            },
        }];
        snapshot
    }

    fn provider_capabilities() -> ProviderCapabilitiesSnapshot {
        ProviderCapabilitiesSnapshot {
            streaming_tokens: true,
            tool_calls: true,
            json_mode: true,
            vision: false,
            audio_transcribe: false,
            embeddings: false,
            reasoning: false,
            reasoning_efforts: Vec::new(),
            service_tier: false,
            service_tiers: Vec::new(),
            max_context_tokens: Some(128_000),
            cost_tier: "standard".to_owned(),
            latency_tier: "standard".to_owned(),
            recommended_use_cases: Vec::new(),
            known_limitations: Vec::new(),
            operator_override: false,
            metadata_source: "static".to_owned(),
        }
    }

    fn retry_policy() -> ProviderRetryPolicySnapshot {
        ProviderRetryPolicySnapshot { max_retries: 0, retry_backoff_ms: 0 }
    }

    fn circuit_breaker() -> ProviderCircuitBreakerSnapshot {
        ProviderCircuitBreakerSnapshot {
            failure_threshold: 1,
            cooldown_ms: 60_000,
            consecutive_failures: 0,
            open: false,
        }
    }

    fn runtime_metrics() -> ProviderRuntimeMetricsSnapshot {
        ProviderRuntimeMetricsSnapshot {
            request_count: 0,
            error_count: 0,
            error_rate_bps: 0,
            total_retry_attempts: 0,
            total_prompt_tokens: 0,
            total_completion_tokens: 0,
            avg_prompt_tokens_per_run: 0,
            avg_completion_tokens_per_run: 0,
            last_latency_ms: 0,
            avg_latency_ms: 0,
            max_latency_ms: 0,
            last_used_at_unix_ms: None,
            last_success_at_unix_ms: None,
            last_error_at_unix_ms: None,
            last_error: None,
        }
    }

    fn health(state: &str) -> ProviderHealthProbeSnapshot {
        ProviderHealthProbeSnapshot {
            state: state.to_owned(),
            message: "test".to_owned(),
            checked_at_unix_ms: None,
            latency_ms: None,
            source: "test".to_owned(),
        }
    }

    fn discovery() -> ProviderDiscoverySnapshot {
        ProviderDiscoverySnapshot {
            status: "unknown".to_owned(),
            checked_at_unix_ms: None,
            expires_at_unix_ms: None,
            discovered_model_ids: Vec::new(),
            source: "test".to_owned(),
            message: None,
        }
    }
}
