// Shared typed phase boundaries, cancellation, lane authority, and service contracts.
// These definitions remain in the parent `phases` namespace so existing callers
// retain their paths while the implementation stays below the module budget.

/// Boxed, sendable future returned by object-safe kernel capabilities.
pub(crate) type KernelPhaseFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Internal representation shared by distinct host-retained reference domains.
#[derive(Debug, Clone, PartialEq, Eq)]
struct OpaqueHostRef {
    id: RuntimeOperationId,
    sha256: [u8; SHA256_BYTES],
}

impl OpaqueHostRef {
    const fn new(id: RuntimeOperationId, sha256: [u8; SHA256_BYTES]) -> Self {
        Self { id, sha256 }
    }
}

macro_rules! opaque_host_ref {
    ($(#[$metadata:meta])* $name:ident) => {
        $(#[$metadata])*
        #[derive(Debug, Clone, PartialEq, Eq)]
        pub(crate) struct $name(OpaqueHostRef);

        impl $name {
            pub(in crate::application::runtime_kernel_v2) fn from_host(
                id: RuntimeOperationId,
                sha256: [u8; SHA256_BYTES],
            ) -> Self {
                Self(OpaqueHostRef::new(id, sha256))
            }

            /// Returns the opaque host identity without resolving its payload.
            #[must_use]
            pub(crate) const fn id(&self) -> &RuntimeOperationId {
                &self.0.id
            }

            /// Returns the domain-bound payload digest.
            #[must_use]
            pub(crate) const fn sha256(&self) -> &[u8; SHA256_BYTES] {
                &self.0.sha256
            }
        }
    };
}

opaque_host_ref!(
    /// Host-retained context input manifest.
    ContextInputRef
);
opaque_host_ref!(
    /// Host-retained provider request.
    ProviderRequestRef
);
opaque_host_ref!(
    /// Host-retained normalized provider response.
    ProviderResponseRef
);
opaque_host_ref!(
    /// Normalized tool proposal retained behind the live tool-flow boundary.
    RetainedToolProposalRef
);
opaque_host_ref!(
    /// Raw tool execution outcome visible only to the live result projector.
    RawToolExecutionOutcomeRef
);
opaque_host_ref!(
    /// Redacted evidence safe for kernel decisions and durable metadata.
    RedactedEvidenceRef
);
opaque_host_ref!(
    /// Model-visible tool result produced only by the live projection boundary.
    ModelVisibleToolResultRef
);
opaque_host_ref!(
    /// Finalized output projection safe for durable delivery.
    FinalProjectionRef
);

/// Whether a phase can observe data or cross a state-changing boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PhaseAuthorityClass {
    /// The phase may read host-owned state but cannot mutate it.
    HostRead,
    /// The phase may mutate durable host state without invoking an external effect.
    HostMutation,
    /// The phase may cross a tool or provider side-effect boundary.
    ExternalEffect,
    /// The phase may record and enqueue a terminal delivery intent.
    TerminalDelivery,
}

/// Durable evidence required around a phase boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DurableTracePolicy {
    /// Append metadata-only evidence after the phase resolves.
    MetadataAfter,
    /// Record a durable intent before any state mutation.
    IntentBeforeMutation,
    /// Record intent before dispatch and outcome after dispatch.
    IntentAndOutcome,
    /// Use the terminal-reserved journal path.
    TerminalReserved,
}

/// Live cancellation signal paired with a durable cancellation snapshot.
pub(crate) trait KernelCancellationSignal: Send + Sync {
    /// Returns the first committed cancellation reason without waiting.
    fn current_reason(&self) -> Option<CancellationReason>;

    /// Waits for the host cancellation authority to commit a reason.
    fn cancelled(&self) -> KernelPhaseFuture<'_, CancellationReason>;
}

/// Generation-bound cancellation scope supplied to one phase invocation.
#[derive(Clone)]
pub(crate) struct KernelCancellationScope {
    context: CancellationContextV1,
    signal: Arc<dyn KernelCancellationSignal>,
}

impl fmt::Debug for KernelCancellationScope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KernelCancellationScope")
            .field("scope", &self.context.scope)
            .field("generation", &self.context.generation)
            .field("deadline_unix_ms", &self.context.deadline_unix_ms)
            .field("cancelled", &self.signal.current_reason().is_some())
            .finish_non_exhaustive()
    }
}

impl KernelCancellationScope {
    /// Creates a live phase scope from host-derived cancellation authority.
    ///
    /// # Errors
    /// Returns [`KernelPhaseContractError`] when the durable context is invalid.
    pub(in crate::application::runtime_kernel_v2) fn new(
        context: CancellationContextV1,
        signal: Arc<dyn KernelCancellationSignal>,
    ) -> Result<Self, KernelPhaseContractError> {
        context.validate().map_err(|_| KernelPhaseContractError::InvalidFlowControl)?;
        Ok(Self { context, signal })
    }

    /// Returns the durable cancellation snapshot.
    #[must_use]
    pub(crate) const fn context(&self) -> &CancellationContextV1 {
        &self.context
    }

    /// Returns the live cancellation signal.
    #[must_use]
    pub(crate) fn signal(&self) -> &dyn KernelCancellationSignal {
        self.signal.as_ref()
    }
}

/// Host-issued lane authority for one phase invocation.
///
/// Every child lane remains subordinate to the exact Run lease that admitted
/// the kernel context. A matching generation number without the matching lease
/// identity never grants authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PhaseLaneAuthority {
    session_id: RuntimeSessionId,
    run_id: RuntimeRunId,
    run_generation: RuntimeGeneration,
    run_lease_id: RuntimeLeaseId,
    lane: RuntimeGenerationLane,
    lane_generation: RuntimeGeneration,
    lane_lease_id: RuntimeLeaseId,
}

impl PhaseLaneAuthority {
    /// Builds a lane grant from host-verified active leases.
    pub(in crate::application::runtime_kernel_v2) fn from_host_leases(
        session_id: RuntimeSessionId,
        run_id: RuntimeRunId,
        run_generation: RuntimeGeneration,
        run_lease_id: RuntimeLeaseId,
        lane: RuntimeGenerationLane,
        lane_generation: RuntimeGeneration,
        lane_lease_id: RuntimeLeaseId,
    ) -> Self {
        Self {
            session_id,
            run_id,
            run_generation,
            run_lease_id,
            lane,
            lane_generation,
            lane_lease_id,
        }
    }

    /// Builds a lane grant for cross-module mailbox tests without widening the
    /// production host-authority minting boundary.
    #[cfg(test)]
    pub(crate) fn test_only_from_host_leases(
        session_id: RuntimeSessionId,
        run_id: RuntimeRunId,
        run_generation: RuntimeGeneration,
        run_lease_id: RuntimeLeaseId,
        lane: RuntimeGenerationLane,
        lane_generation: RuntimeGeneration,
        lane_lease_id: RuntimeLeaseId,
    ) -> Self {
        Self::from_host_leases(
            session_id,
            run_id,
            run_generation,
            run_lease_id,
            lane,
            lane_generation,
            lane_lease_id,
        )
    }

    /// Returns the session bound to both leases.
    #[must_use]
    pub(crate) const fn session_id(&self) -> &RuntimeSessionId {
        &self.session_id
    }

    /// Returns the run bound to both leases.
    #[must_use]
    pub(crate) const fn run_id(&self) -> &RuntimeRunId {
        &self.run_id
    }

    /// Returns the immutable authoritative Run generation.
    #[must_use]
    pub(crate) const fn run_generation(&self) -> RuntimeGeneration {
        self.run_generation
    }

    /// Returns the exact authoritative Run lease.
    #[must_use]
    pub(crate) const fn run_lease_id(&self) -> &RuntimeLeaseId {
        &self.run_lease_id
    }

    /// Returns the lane granted to this phase.
    #[must_use]
    pub(crate) const fn lane(&self) -> RuntimeGenerationLane {
        self.lane
    }

    /// Returns the active generation in the phase lane.
    #[must_use]
    pub(crate) const fn lane_generation(&self) -> RuntimeGeneration {
        self.lane_generation
    }

    /// Returns the exact active lease in the phase lane.
    #[must_use]
    pub(crate) const fn lane_lease_id(&self) -> &RuntimeLeaseId {
        &self.lane_lease_id
    }
}

/// Bounded control contract applied to one phase invocation.
#[derive(Debug, Clone)]
pub(crate) struct PhaseExecutionContext {
    cancellation: KernelCancellationScope,
    timeout_ms: u64,
    backpressure: BackpressurePolicy,
    lane_authority: PhaseLaneAuthority,
    authority_class: PhaseAuthorityClass,
    durable_trace_policy: DurableTracePolicy,
}

impl PhaseExecutionContext {
    /// Creates and validates a host-derived phase control contract.
    ///
    /// # Errors
    /// Returns [`KernelPhaseContractError`] for an invalid timeout, cancellation
    /// scope, or backpressure policy.
    pub(in crate::application::runtime_kernel_v2) fn new(
        cancellation: KernelCancellationScope,
        timeout_ms: u64,
        backpressure: BackpressurePolicy,
        lane_authority: PhaseLaneAuthority,
        authority_class: PhaseAuthorityClass,
        durable_trace_policy: DurableTracePolicy,
    ) -> Result<Self, KernelPhaseContractError> {
        cancellation
            .context()
            .validate()
            .map_err(|_| KernelPhaseContractError::InvalidFlowControl)?;
        backpressure.validate().map_err(|_| KernelPhaseContractError::InvalidFlowControl)?;
        if timeout_ms == 0 {
            return Err(KernelPhaseContractError::InvalidFlowControl);
        }
        Ok(Self {
            cancellation,
            timeout_ms,
            backpressure,
            lane_authority,
            authority_class,
            durable_trace_policy,
        })
    }

    /// Returns the live cancellation scope.
    #[must_use]
    pub(crate) const fn cancellation(&self) -> &KernelCancellationScope {
        &self.cancellation
    }

    /// Returns the finite host-owned timeout budget.
    #[must_use]
    pub(crate) const fn timeout_ms(&self) -> u64 {
        self.timeout_ms
    }

    /// Returns the bounded overflow policy.
    #[must_use]
    pub(crate) const fn backpressure(&self) -> &BackpressurePolicy {
        &self.backpressure
    }

    /// Returns the exact host-issued run and phase-lane leases.
    #[must_use]
    pub(crate) const fn lane_authority(&self) -> &PhaseLaneAuthority {
        &self.lane_authority
    }

    /// Returns the maximum authority this phase may exercise.
    #[must_use]
    pub(crate) const fn authority_class(&self) -> PhaseAuthorityClass {
        self.authority_class
    }

    /// Returns the required durable evidence posture.
    #[must_use]
    pub(crate) const fn durable_trace_policy(&self) -> DurableTracePolicy {
        self.durable_trace_policy
    }
}

/// Stable reason attached to every successful or denied phase transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KernelPhaseReason {
    /// Admission granted run authority.
    #[cfg(test)]
    AdmissionGranted,
    /// Admission rejected the request.
    #[cfg(test)]
    AdmissionRejected,
    /// Runtime selection chose an authoritative implementation.
    #[cfg(test)]
    RuntimeSelected,
    /// Runtime selection blocked without a fallback.
    #[cfg(test)]
    RuntimeSelectionBlocked,
    /// Provider-facing context assembly completed.
    ContextAssembled,
    /// Provider-facing context assembly failed closed.
    ContextAssemblyBlocked,
    /// A provider attempt completed.
    ProviderCallCompleted,
    /// A provider attempt requires recovery or terminalization.
    ProviderCallFailed,
    /// Tool governance issued executable authority.
    ToolGateGranted,
    /// Tool governance denied the proposal.
    ToolGateDenied,
    /// Tool governance requires an approval decision.
    ToolGateApprovalRequired,
    /// Approval remains durably pending.
    ApprovalPending,
    /// Approval issued executable authority.
    ApprovalGranted,
    /// Approval denied executable authority.
    ApprovalDenied,
    /// Approval wait expired without authority.
    ApprovalTimedOut,
    /// Tool execution produced a receipt.
    ToolExecutionCompleted,
    /// Tool execution crossed a boundary with an unresolved outcome.
    ToolExecutionUnknown,
    /// A tool result was safely projected for the model.
    ResultProjected,
    /// A tool result projection was withheld.
    ResultProjectionWithheld,
    /// Durable context compaction was applied.
    CompactionApplied,
    /// Context compaction was not required.
    CompactionSkipped,
    /// A single terminal outcome was committed.
    FinalizationCommitted,
    /// Finalization was rejected to preserve a terminal invariant.
    FinalizationBlocked,
    /// A durable delivery intent was recorded or advanced.
    DeliveryAdvanced,
    /// Delivery has an explicit unresolved outcome.
    DeliveryUnknown,
}

impl KernelPhaseReason {
    /// Returns the canonical phase that owns this reason.
    #[must_use]
    pub(crate) const fn phase(self) -> RuntimeErrorPhase {
        match self {
            #[cfg(test)]
            Self::AdmissionGranted | Self::AdmissionRejected => RuntimeErrorPhase::Admission,
            #[cfg(test)]
            Self::RuntimeSelected | Self::RuntimeSelectionBlocked => {
                RuntimeErrorPhase::RuntimeSelection
            }
            Self::ContextAssembled | Self::ContextAssemblyBlocked => {
                RuntimeErrorPhase::ContextAssembly
            }
            Self::ProviderCallCompleted | Self::ProviderCallFailed => {
                RuntimeErrorPhase::ProviderCall
            }
            Self::ToolGateGranted | Self::ToolGateDenied | Self::ToolGateApprovalRequired => {
                RuntimeErrorPhase::ToolGate
            }
            Self::ApprovalPending
            | Self::ApprovalGranted
            | Self::ApprovalDenied
            | Self::ApprovalTimedOut => RuntimeErrorPhase::Approval,
            Self::ToolExecutionCompleted | Self::ToolExecutionUnknown => {
                RuntimeErrorPhase::ToolExecution
            }
            Self::ResultProjected | Self::ResultProjectionWithheld => {
                RuntimeErrorPhase::ResultProjection
            }
            Self::CompactionApplied | Self::CompactionSkipped => RuntimeErrorPhase::Compaction,
            Self::FinalizationCommitted | Self::FinalizationBlocked => {
                RuntimeErrorPhase::Finalization
            }
            Self::DeliveryAdvanced | Self::DeliveryUnknown => RuntimeErrorPhase::DeliveryQueue,
        }
    }

    /// Returns the stable, low-cardinality reason code.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            #[cfg(test)]
            Self::AdmissionGranted => "runtime.phase.admission.granted",
            #[cfg(test)]
            Self::AdmissionRejected => "runtime.phase.admission.rejected",
            #[cfg(test)]
            Self::RuntimeSelected => "runtime.phase.runtime_selection.selected",
            #[cfg(test)]
            Self::RuntimeSelectionBlocked => "runtime.phase.runtime_selection.blocked",
            Self::ContextAssembled => "runtime.phase.context_assembly.completed",
            Self::ContextAssemblyBlocked => "runtime.phase.context_assembly.blocked",
            Self::ProviderCallCompleted => "runtime.phase.provider_call.completed",
            Self::ProviderCallFailed => "runtime.phase.provider_call.failed",
            Self::ToolGateGranted => "runtime.phase.tool_gate.granted",
            Self::ToolGateDenied => "runtime.phase.tool_gate.denied",
            Self::ToolGateApprovalRequired => "runtime.phase.tool_gate.approval_required",
            Self::ApprovalPending => "runtime.phase.approval.pending",
            Self::ApprovalGranted => "runtime.phase.approval.granted",
            Self::ApprovalDenied => "runtime.phase.approval.denied",
            Self::ApprovalTimedOut => "runtime.phase.approval.timed_out",
            Self::ToolExecutionCompleted => "runtime.phase.tool_execution.completed",
            Self::ToolExecutionUnknown => "runtime.phase.tool_execution.unknown",
            Self::ResultProjected => "runtime.phase.result_projection.completed",
            Self::ResultProjectionWithheld => "runtime.phase.result_projection.withheld",
            Self::CompactionApplied => "runtime.phase.compaction.applied",
            Self::CompactionSkipped => "runtime.phase.compaction.skipped",
            Self::FinalizationCommitted => "runtime.phase.finalization.committed",
            Self::FinalizationBlocked => "runtime.phase.finalization.blocked",
            Self::DeliveryAdvanced => "runtime.phase.delivery.advanced",
            Self::DeliveryUnknown => "runtime.phase.delivery.unknown",
        }
    }
}

/// Compile-time descriptor implemented by each canonical phase.
pub(crate) trait CanonicalPhase: fmt::Debug + Send + Sync + 'static {
    /// Shared runtime error phase.
    const PHASE: RuntimeErrorPhase;
    /// Child cancellation scope required for phase work.
    const CANCELLATION_SCOPE: CancellationScopeKind;
    /// Runtime generation lane that must grant this phase.
    const LANE: RuntimeGenerationLane;
    /// Maximum authority the phase may exercise.
    const AUTHORITY_CLASS: PhaseAuthorityClass;
    /// Durable evidence posture required by the phase.
    const TRACE_POLICY: DurableTracePolicy;
}

macro_rules! canonical_phase {
    (
        $(#[$metadata:meta])*
        $name:ident,
        $phase:expr,
        $scope:expr,
        $lane:expr,
        $authority:expr,
        $trace:expr
    ) => {
        $(#[$metadata])*
        #[derive(Debug)]
        pub(crate) enum $name {}

        impl CanonicalPhase for $name {
            const PHASE: RuntimeErrorPhase = $phase;
            const CANCELLATION_SCOPE: CancellationScopeKind = $scope;
            const LANE: RuntimeGenerationLane = $lane;
            const AUTHORITY_CLASS: PhaseAuthorityClass = $authority;
            const TRACE_POLICY: DurableTracePolicy = $trace;
        }
    };
}

#[cfg(test)]
canonical_phase!(
    /// Admission and run-generation binding.
    AdmissionPhase,
    RuntimeErrorPhase::Admission,
    CancellationScopeKind::Run,
    RuntimeGenerationLane::Run,
    PhaseAuthorityClass::HostMutation,
    DurableTracePolicy::IntentBeforeMutation
);
#[cfg(test)]
canonical_phase!(
    /// Atomic runtime-profile selection.
    RuntimeSelectionPhase,
    RuntimeErrorPhase::RuntimeSelection,
    CancellationScopeKind::ChildTask,
    RuntimeGenerationLane::Run,
    PhaseAuthorityClass::HostRead,
    DurableTracePolicy::MetadataAfter
);
canonical_phase!(
    /// Provider-facing context assembly.
    ContextAssemblyPhase,
    RuntimeErrorPhase::ContextAssembly,
    CancellationScopeKind::ChildTask,
    RuntimeGenerationLane::Harness,
    PhaseAuthorityClass::HostRead,
    DurableTracePolicy::MetadataAfter
);
canonical_phase!(
    /// Provider attempt dispatch and response normalization.
    ProviderCallPhase,
    RuntimeErrorPhase::ProviderCall,
    CancellationScopeKind::ProviderAttempt,
    RuntimeGenerationLane::Provider,
    PhaseAuthorityClass::ExternalEffect,
    DurableTracePolicy::IntentAndOutcome
);
canonical_phase!(
    /// Catalog, schema, policy, approval-posture, and side-effect gate.
    ToolGatePhase,
    RuntimeErrorPhase::ToolGate,
    CancellationScopeKind::ChildTask,
    RuntimeGenerationLane::Tool,
    PhaseAuthorityClass::HostRead,
    DurableTracePolicy::IntentAndOutcome
);
canonical_phase!(
    /// Durable approval wait or restart resume.
    ApprovalWaitPhase,
    RuntimeErrorPhase::Approval,
    CancellationScopeKind::ApprovalWait,
    RuntimeGenerationLane::Tool,
    PhaseAuthorityClass::HostMutation,
    DurableTracePolicy::IntentAndOutcome
);
canonical_phase!(
    /// Granted tool execution through the existing runtime dispatch.
    ToolExecutionPhase,
    RuntimeErrorPhase::ToolExecution,
    CancellationScopeKind::ToolExecution,
    RuntimeGenerationLane::Tool,
    PhaseAuthorityClass::ExternalEffect,
    DurableTracePolicy::IntentAndOutcome
);
canonical_phase!(
    /// Host middleware and redaction of a tool result.
    ResultProjectionPhase,
    RuntimeErrorPhase::ResultProjection,
    CancellationScopeKind::ChildTask,
    RuntimeGenerationLane::Tool,
    PhaseAuthorityClass::HostRead,
    DurableTracePolicy::MetadataAfter
);
canonical_phase!(
    /// Durable session-context compaction.
    CompactionPhase,
    RuntimeErrorPhase::Compaction,
    CancellationScopeKind::ChildTask,
    RuntimeGenerationLane::Run,
    PhaseAuthorityClass::HostMutation,
    DurableTracePolicy::IntentAndOutcome
);
canonical_phase!(
    /// Single terminal outcome commitment.
    FinalizationPhase,
    RuntimeErrorPhase::Finalization,
    CancellationScopeKind::Run,
    RuntimeGenerationLane::Run,
    PhaseAuthorityClass::HostMutation,
    DurableTracePolicy::TerminalReserved
);
canonical_phase!(
    /// Durable delivery intent and downstream queue handoff.
    DeliveryPhase,
    RuntimeErrorPhase::DeliveryQueue,
    CancellationScopeKind::Delivery,
    RuntimeGenerationLane::Delivery,
    PhaseAuthorityClass::TerminalDelivery,
    DurableTracePolicy::IntentBeforeMutation
);

/// Generation and flow-control binding shared by every phase input and output.
#[derive(Debug, Clone)]
pub(crate) struct KernelPhaseBoundary {
    identities: RuntimeIdentitySetV1,
    generation: RuntimeGeneration,
    execution: PhaseExecutionContext,
}

impl KernelPhaseBoundary {
    fn new<P: CanonicalPhase>(
        identities: RuntimeIdentitySetV1,
        generation: RuntimeGeneration,
        execution: PhaseExecutionContext,
    ) -> Result<Self, KernelPhaseContractError> {
        let boundary = Self { identities, generation, execution };
        boundary.validate::<P>()?;
        Ok(boundary)
    }

    fn validate<P: CanonicalPhase>(&self) -> Result<(), KernelPhaseContractError> {
        self.identities.validate().map_err(KernelPhaseContractError::InvalidIdentities)?;
        let lane_authority = self.execution().lane_authority();
        if self.identities().generation != self.generation()
            || self.execution().cancellation().context().generation != self.generation()
            || lane_authority.run_generation() != self.generation()
        {
            return Err(KernelPhaseContractError::GenerationMismatch);
        }
        if lane_authority.session_id() != &self.identities().session_id
            || lane_authority.run_id() != &self.identities().run_id
        {
            return Err(KernelPhaseContractError::LaneIdentityMismatch);
        }
        if lane_authority.lane() != P::LANE {
            return Err(KernelPhaseContractError::LaneMismatch {
                expected: P::LANE,
                observed: lane_authority.lane(),
            });
        }
        if P::LANE == RuntimeGenerationLane::Run
            && (lane_authority.lane_generation() != lane_authority.run_generation()
                || lane_authority.lane_lease_id() != lane_authority.run_lease_id())
        {
            return Err(KernelPhaseContractError::InvalidRunLaneBinding);
        }
        if self.execution().cancellation().context().scope != P::CANCELLATION_SCOPE
            || self.execution().authority_class() != P::AUTHORITY_CLASS
            || self.execution().durable_trace_policy() != P::TRACE_POLICY
        {
            return Err(KernelPhaseContractError::InvalidPhaseControl { phase: P::PHASE });
        }
        Ok(())
    }

    /// Returns the complete typed identity set.
    #[must_use]
    pub(crate) const fn identities(&self) -> &RuntimeIdentitySetV1 {
        &self.identities
    }

    /// Returns the generation that owns this phase.
    #[must_use]
    pub(crate) const fn generation(&self) -> RuntimeGeneration {
        self.generation
    }

    /// Returns the bounded execution contract.
    #[must_use]
    pub(crate) const fn execution(&self) -> &PhaseExecutionContext {
        &self.execution
    }
}

/// Canonical input to one typed runtime phase.
#[derive(Debug)]
pub(crate) struct KernelPhaseInput<P, T> {
    boundary: KernelPhaseBoundary,
    payload: T,
    phase: PhantomData<fn() -> P>,
}

impl<P: CanonicalPhase, T> KernelPhaseInput<P, T> {
    /// Creates a phase input after checking identity, generation, and authority bindings.
    ///
    /// # Errors
    /// Returns [`KernelPhaseContractError`] when any binding is inconsistent.
    pub(in crate::application::runtime_kernel_v2) fn new(
        identities: RuntimeIdentitySetV1,
        generation: RuntimeGeneration,
        execution: PhaseExecutionContext,
        payload: T,
    ) -> Result<Self, KernelPhaseContractError> {
        Ok(Self {
            boundary: KernelPhaseBoundary::new::<P>(identities, generation, execution)?,
            payload,
            phase: PhantomData,
        })
    }

    /// Returns the identity and execution binding.
    #[must_use]
    pub(crate) const fn boundary(&self) -> &KernelPhaseBoundary {
        &self.boundary
    }

    /// Returns the phase payload.
    #[must_use]
    pub(crate) const fn payload(&self) -> &T {
        &self.payload
    }

    /// Consumes the input and returns its phase payload.
    #[must_use]
    pub(crate) fn into_payload(self) -> T {
        self.payload
    }
}

/// Canonical output from one typed runtime phase.
#[derive(Debug)]
pub(crate) struct KernelPhaseOutput<P, T> {
    boundary: KernelPhaseBoundary,
    reason: KernelPhaseReason,
    payload: T,
    phase: PhantomData<fn() -> P>,
}

impl<P: CanonicalPhase, T> KernelPhaseOutput<P, T> {
    /// Creates an output tied to the exact input generation and phase controls.
    ///
    /// # Errors
    /// Returns [`KernelPhaseContractError::ReasonPhaseMismatch`] when the reason
    /// is owned by a different canonical phase.
    pub(in crate::application::runtime_kernel_v2) fn from_input<I>(
        input: &KernelPhaseInput<P, I>,
        reason: KernelPhaseReason,
        payload: T,
    ) -> Result<Self, KernelPhaseContractError> {
        if reason.phase() != P::PHASE {
            return Err(KernelPhaseContractError::ReasonPhaseMismatch {
                expected: P::PHASE,
                observed: reason.phase(),
            });
        }
        Ok(Self { boundary: input.boundary.clone(), reason, payload, phase: PhantomData })
    }

    /// Returns the identity and execution binding.
    #[must_use]
    pub(crate) const fn boundary(&self) -> &KernelPhaseBoundary {
        &self.boundary
    }

    /// Returns the stable transition reason.
    #[must_use]
    pub(crate) const fn reason(&self) -> KernelPhaseReason {
        self.reason
    }

    /// Returns the stable transition reason code.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn reason_code(&self) -> &'static str {
        self.reason.as_str()
    }

    /// Returns the phase payload.
    #[must_use]
    #[cfg(test)]
    pub(crate) const fn payload(&self) -> &T {
        &self.payload
    }

    /// Consumes the output and returns its phase payload.
    ///
    /// # Errors
    /// Returns [`KernelPhaseContractError`] if the returned phase envelope no
    /// longer carries the exact canonical boundary or reason for `P`.
    pub(crate) fn into_payload(self) -> Result<T, KernelPhaseContractError> {
        self.boundary().validate::<P>()?;
        if self.reason().phase() != P::PHASE {
            return Err(KernelPhaseContractError::ReasonPhaseMismatch {
                expected: P::PHASE,
                observed: self.reason().phase(),
            });
        }
        Ok(self.payload)
    }
}

/// Object-safe service contract for one non-tool phase.
///
/// Implementations retain raw subsystem state. The kernel receives only the
/// typed input and output contracts declared in this module.
pub(crate) trait RuntimePhaseService<P, I, O>: Send + Sync
where
    P: CanonicalPhase,
    I: Send + 'static,
    O: Send + 'static,
{
    /// Executes one bounded phase invocation.
    fn execute(
        &self,
        input: KernelPhaseInput<P, I>,
    ) -> KernelPhaseFuture<'_, Result<KernelPhaseOutput<P, O>, KernelPhaseError>>;
}
