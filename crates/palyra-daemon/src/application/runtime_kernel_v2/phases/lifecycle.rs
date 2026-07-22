// Admission, provider, compaction, finalization, and delivery phase payloads.
// These host-safe contracts carry opaque references and never expose retained
// provider, credential, or delivery payloads to the runtime kernel.

/// Origin class admitted into the canonical kernel.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AdmissionOrigin {
    /// Interactive gRPC run stream.
    RunStream,
    /// Authenticated channel ingress.
    Channel,
    /// Durable background queue.
    Background,
    /// Startup or crash recovery.
    Recovery,
}

/// Redacted input to admission and identity binding.
#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AdmissionRequest {
    /// Request origin class.
    #[cfg(test)]
    pub(crate) origin: AdmissionOrigin,
    /// Domain-separated hash of the admitted principal binding.
    pub(crate) principal_binding_sha256: [u8; SHA256_BYTES],
    /// Domain-separated hash of the admitted session/origin binding.
    pub(crate) session_binding_sha256: [u8; SHA256_BYTES],
}

/// Admission result without transport or credential material.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AdmissionDecision {
    /// The bound generation may proceed to runtime selection.
    #[cfg(test)]
    Admitted,
    /// Admission stopped without granting run authority.
    Rejected,
}

/// Inputs needed by the atomic runtime-selection service.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RuntimeSelectionRequest {
    /// Current V2 readiness evidence.
    pub(crate) availability: V2RuntimeAvailability,
    /// Progress evidence that constrains fallback.
    pub(crate) progress: RuntimeAuthorityProgressEvidence,
}

/// Request to assemble a provider-facing context from host-retained inputs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ContextAssemblyRequest {
    /// Host-retained input manifest.
    pub(crate) input_manifest: ContextInputRef,
    /// Maximum provider input tokens after completion/tool reserves.
    pub(crate) max_input_tokens: u64,
}

/// Context assembly result safe for kernel orchestration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ContextAssemblyResult {
    /// Typed context projection identity.
    pub(crate) projection_id: RuntimeContextProjectionId,
    /// Host-retained provider request.
    pub(crate) provider_request: ProviderRequestRef,
    /// Hash of the retained trust-labeled segment manifest.
    pub(crate) segment_manifest_sha256: [u8; SHA256_BYTES],
    /// Bounded token estimate used for observability and recovery.
    pub(crate) retained_token_estimate: u64,
}

/// One host-owned provider attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProviderCallRequest {
    /// Context projection consumed by this attempt.
    pub(crate) context_projection_id: RuntimeContextProjectionId,
    /// Host-retained provider request.
    pub(crate) provider_request: ProviderRequestRef,
}

/// Normalized provider outcome without raw provider data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ProviderCallResult {
    /// The host retained a normalized provider response.
    Completed {
        /// Host-retained normalized response.
        response: ProviderResponseRef,
        /// Whether model-visible output crossed the stream boundary.
        output_emitted: bool,
    },
    /// The host retained recovery-classification evidence.
    Failed {
        /// Redacted recovery/error evidence.
        evidence: RedactedEvidenceRef,
        /// Whether model-visible output crossed the stream boundary.
        output_emitted: bool,
    },
}

/// Durable compaction request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompactionRequest {
    /// Context projection being compacted.
    pub(crate) context_projection_id: RuntimeContextProjectionId,
    /// Host-retained pressure and transcript manifest.
    pub(crate) pressure_manifest: ContextInputRef,
}

/// Durable compaction outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CompactionResult {
    /// A successor context projection was committed.
    Applied {
        /// Successor context identity.
        context_projection_id: RuntimeContextProjectionId,
        /// Host-retained compaction/checkpoint evidence.
        evidence: RedactedEvidenceRef,
    },
}

/// Request to commit exactly one terminal run outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FinalizationRequest {
    /// Typed terminal classification.
    pub(crate) outcome: RuntimeTerminalOutcome,
    /// Host-retained final output or terminal error projection.
    pub(crate) final_projection: FinalProjectionRef,
}

/// Durable terminal commitment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FinalizationReceipt {
    /// Committed terminal classification.
    pub(crate) outcome: RuntimeTerminalOutcome,
    /// Journal-owned terminal evidence.
    pub(crate) terminal_evidence: RedactedEvidenceRef,
}

/// Request to record or advance a durable delivery intent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeliveryRequest {
    /// Typed delivery intent identity.
    pub(crate) delivery_intent_id: RuntimeDeliveryIntentId,
    /// Host-retained finalized output.
    pub(crate) final_projection: FinalProjectionRef,
}

/// Delivery state visible to kernel orchestration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeliveryDisposition {
    /// Durable intent exists but has not reached the queue.
    IntentRecorded,
    /// The intent is queued for its connector.
    Queued,
    /// The downstream adapter acknowledged delivery.
    Delivered,
    /// The adapter outcome is unresolved and cannot be replayed blindly.
    Unknown,
}

/// Durable delivery result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeliveryResult {
    /// Delivery intent identity.
    pub(crate) delivery_intent_id: RuntimeDeliveryIntentId,
    /// Current durable disposition.
    pub(crate) disposition: DeliveryDisposition,
    /// Host-retained delivery evidence.
    pub(crate) evidence: RedactedEvidenceRef,
}

/// Canonical phase aliases used by the kernel and host adapters.
#[cfg(test)]
pub(crate) type AdmissionPhaseInput = KernelPhaseInput<AdmissionPhase, AdmissionRequest>;
#[cfg(test)]
pub(crate) type AdmissionPhaseOutput = KernelPhaseOutput<AdmissionPhase, AdmissionDecision>;
#[cfg(test)]
pub(crate) type RuntimeSelectionPhaseInput =
    KernelPhaseInput<RuntimeSelectionPhase, RuntimeSelectionRequest>;
#[cfg(test)]
pub(crate) type RuntimeSelectionPhaseOutput =
    KernelPhaseOutput<RuntimeSelectionPhase, ResolvedRuntimeSelection>;
pub(crate) type ContextAssemblyPhaseInput =
    KernelPhaseInput<ContextAssemblyPhase, ContextAssemblyRequest>;
pub(crate) type ContextAssemblyPhaseOutput =
    KernelPhaseOutput<ContextAssemblyPhase, ContextAssemblyResult>;
pub(crate) type ProviderCallPhaseInput = KernelPhaseInput<ProviderCallPhase, ProviderCallRequest>;
pub(crate) type ProviderCallPhaseOutput = KernelPhaseOutput<ProviderCallPhase, ProviderCallResult>;
pub(crate) type ToolGatePhaseInput = KernelPhaseInput<ToolGatePhase, ToolProposalRequest>;
pub(crate) type ToolGatePhaseOutput = KernelPhaseOutput<ToolGatePhase, ToolGateDecision>;
pub(crate) type ApprovalWaitPhaseInput =
    KernelPhaseInput<ApprovalWaitPhase, ApprovalWaitResumeRequest>;
pub(crate) type ApprovalWaitPhaseOutput =
    KernelPhaseOutput<ApprovalWaitPhase, ApprovalWaitResumeResult>;
pub(crate) type ToolExecutionPhaseInput =
    KernelPhaseInput<ToolExecutionPhase, ToolExecutionRequest>;
pub(crate) type ToolExecutionPhaseOutput =
    KernelPhaseOutput<ToolExecutionPhase, ToolExecutionReceipt>;
pub(crate) type ResultProjectionPhaseInput =
    KernelPhaseInput<ResultProjectionPhase, ToolResultProjectionRequest>;
pub(crate) type ResultProjectionPhaseOutput =
    KernelPhaseOutput<ResultProjectionPhase, ToolResultProjection>;
pub(crate) type CompactionPhaseInput = KernelPhaseInput<CompactionPhase, CompactionRequest>;
pub(crate) type CompactionPhaseOutput = KernelPhaseOutput<CompactionPhase, CompactionResult>;
pub(crate) type DeliveryPhaseInput = KernelPhaseInput<DeliveryPhase, DeliveryRequest>;
pub(crate) type DeliveryPhaseOutput = KernelPhaseOutput<DeliveryPhase, DeliveryResult>;

pub(in crate::application::runtime_kernel_v2) mod tool_gateway_sealed {
    /// Marker restricted to adapters implemented inside RuntimeKernelV2.
    ///
    /// The eventual implementation is expected to delegate to the live
    /// `run_stream::tool_flow` boundary.
    pub trait LiveToolFlowAdapter {}
}
