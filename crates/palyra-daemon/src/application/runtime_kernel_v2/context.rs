//! Host capabilities and redacted run context for RuntimeKernelV2.
//!
//! The context binds one typed generation to runtime selection and narrow service
//! objects. Credential resolution and raw provider/tool payloads stay inside the
//! host adapters represented by those capabilities.

use std::{fmt, sync::Arc};

use palyra_common::runtime_contracts::{
    BackpressurePolicy, CancellationContextV1, CancellationScopeKind, RuntimeErrorPhase,
    RuntimeGeneration, RuntimeGenerationLane, RuntimeIdentityError, RuntimeIdentitySetV1,
};
use thiserror::Error;

#[cfg(test)]
use super::phases::{
    AdmissionDecision, AdmissionPhase, AdmissionRequest, RuntimeSelectionPhase,
    RuntimeSelectionRequest,
};
use super::{
    phases::{
        CanonicalPhase, CompactionPhase, CompactionRequest, CompactionResult, ContextAssemblyPhase,
        ContextAssemblyRequest, ContextAssemblyResult, DeliveryPhase, DeliveryRequest,
        DeliveryResult, FinalizationPhase, FinalizationReceipt, FinalizationRequest,
        KernelCancellationScope, KernelPhaseContractError, KernelPhaseInput, PhaseExecutionContext,
        PhaseLaneAuthority, ProviderCallPhase, ProviderCallRequest, ProviderCallResult,
        RuntimePhaseService, ToolAuthorityGateway,
    },
    runtime_selection::{
        AuthoritativeRuntimeGrant, ResolvedRuntimeSelection, RuntimeSelectionError,
        RuntimeSelectionV1,
    },
    selection::RuntimeAuthority,
    RuntimeKernelVersion,
};

/// Fail-closed failure returned by a host flow-control capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub(crate) enum KernelAuthorityError {
    /// The host could not derive a cancellation child for this phase.
    #[error("kernel cancellation authority is unavailable")]
    Cancellation,
    /// The host has no finite deadline for this phase.
    #[error("kernel deadline authority is unavailable")]
    Deadline,
    /// The host could not provide a bounded backpressure policy.
    #[error("kernel backpressure authority is unavailable")]
    Backpressure,
    /// The host could not provide an active lease for the requested phase lane.
    #[error("kernel phase lane authority is unavailable")]
    LaneAuthority,
}

impl KernelAuthorityError {
    /// Returns the stable reason code for redacted diagnostics.
    #[must_use]
    pub(crate) const fn reason_code(self) -> &'static str {
        match self {
            Self::Cancellation => "runtime.kernel.cancellation_authority_unavailable",
            Self::Deadline => "runtime.kernel.deadline_authority_unavailable",
            Self::Backpressure => "runtime.kernel.backpressure_authority_unavailable",
            Self::LaneAuthority => "runtime.kernel.lane_authority_unavailable",
        }
    }
}

/// Host owner of the live cancellation hierarchy for one run generation.
pub(crate) trait KernelCancellationAuthority: Send + Sync {
    /// Returns the durable root context without its live notification channel.
    fn root_context(&self) -> CancellationContextV1;

    /// Derives a live child bounded by the root deadline and settlement budget.
    fn derive_scope(
        &self,
        scope: CancellationScopeKind,
        timeout_ms: u64,
    ) -> Result<KernelCancellationScope, KernelAuthorityError>;
}

/// Host owner of finite per-phase timeout budgets.
pub(crate) trait KernelDeadlineAuthority: Send + Sync {
    /// Returns the finite timeout for one canonical runtime phase.
    fn timeout_ms(&self, phase: RuntimeErrorPhase) -> Result<u64, KernelAuthorityError>;
}

/// Host owner of bounded phase event queues and protected-event behavior.
pub(crate) trait KernelBackpressureAuthority: Send + Sync {
    /// Returns the bounded policy for one canonical runtime phase.
    fn policy(&self, phase: RuntimeErrorPhase) -> Result<BackpressurePolicy, KernelAuthorityError>;
}

/// Host owner of active Run and child-lane lease grants.
pub(crate) trait KernelLaneAuthority: Send + Sync {
    /// Returns the currently active grant for one canonical phase lane.
    fn authority_for(
        &self,
        lane: RuntimeGenerationLane,
    ) -> Result<PhaseLaneAuthority, KernelAuthorityError>;
}

/// Object-safe admission service.
#[cfg(test)]
pub(crate) type AdmissionService =
    dyn RuntimePhaseService<AdmissionPhase, AdmissionRequest, AdmissionDecision>;
/// Object-safe runtime-selection service.
#[cfg(test)]
pub(crate) type RuntimeSelectionService = dyn RuntimePhaseService<
    RuntimeSelectionPhase,
    RuntimeSelectionRequest,
    ResolvedRuntimeSelection,
>;
/// Object-safe context-assembly service.
pub(crate) type ContextAssemblyService =
    dyn RuntimePhaseService<ContextAssemblyPhase, ContextAssemblyRequest, ContextAssemblyResult>;
/// Object-safe provider-call service.
pub(crate) type ProviderCallService =
    dyn RuntimePhaseService<ProviderCallPhase, ProviderCallRequest, ProviderCallResult>;
/// Object-safe compaction service.
pub(crate) type CompactionService =
    dyn RuntimePhaseService<CompactionPhase, CompactionRequest, CompactionResult>;
/// Object-safe finalization service.
pub(crate) type FinalizationService =
    dyn RuntimePhaseService<FinalizationPhase, FinalizationRequest, FinalizationReceipt>;
/// Object-safe delivery service.
pub(crate) type DeliveryService =
    dyn RuntimePhaseService<DeliveryPhase, DeliveryRequest, DeliveryResult>;

/// Host capabilities used before and during a provider turn.
pub(crate) struct RuntimeKernelTurnServices {
    #[cfg(test)]
    admission: Option<Arc<AdmissionService>>,
    #[cfg(test)]
    runtime_selection: Option<Arc<RuntimeSelectionService>>,
    context_assembly: Arc<ContextAssemblyService>,
    provider_call: Arc<ProviderCallService>,
}

impl fmt::Debug for RuntimeKernelTurnServices {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeKernelTurnServices")
            .field(
                "capabilities",
                &["admission", "runtime_selection", "context_assembly", "provider_call"],
            )
            .finish()
    }
}

impl RuntimeKernelTurnServices {
    /// Creates the provider-turn capability group.
    #[must_use]
    pub(crate) fn new(
        context_assembly: Arc<ContextAssemblyService>,
        provider_call: Arc<ProviderCallService>,
    ) -> Self {
        Self {
            #[cfg(test)]
            admission: None,
            #[cfg(test)]
            runtime_selection: None,
            context_assembly,
            provider_call,
        }
    }

    /// Creates a test capability group with the sealed pre-context services.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn with_preflight_services(
        admission: Arc<AdmissionService>,
        runtime_selection: Arc<RuntimeSelectionService>,
        context_assembly: Arc<ContextAssemblyService>,
        provider_call: Arc<ProviderCallService>,
    ) -> Self {
        Self {
            admission: Some(admission),
            runtime_selection: Some(runtime_selection),
            context_assembly,
            provider_call,
        }
    }

    /// Returns the admission service.
    #[must_use]
    #[cfg(test)]
    pub(crate) fn admission(&self) -> Option<&AdmissionService> {
        self.admission.as_deref()
    }

    /// Returns the runtime-selection service.
    #[must_use]
    #[cfg(test)]
    pub(crate) fn runtime_selection(&self) -> Option<&RuntimeSelectionService> {
        self.runtime_selection.as_deref()
    }

    /// Returns the context-assembly service.
    #[must_use]
    pub(crate) fn context_assembly(&self) -> &ContextAssemblyService {
        self.context_assembly.as_ref()
    }

    /// Returns the provider-call service.
    #[must_use]
    pub(crate) fn provider_call(&self) -> &ProviderCallService {
        self.provider_call.as_ref()
    }
}

/// Host capabilities used after provider or tool work resolves.
pub(crate) struct RuntimeKernelLifecycleServices {
    compaction: Arc<CompactionService>,
    finalization: Arc<FinalizationService>,
    delivery: Arc<DeliveryService>,
}

impl fmt::Debug for RuntimeKernelLifecycleServices {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeKernelLifecycleServices")
            .field("capabilities", &["compaction", "finalization", "delivery"])
            .finish()
    }
}

impl RuntimeKernelLifecycleServices {
    /// Creates the post-turn capability group.
    #[must_use]
    pub(crate) fn new(
        compaction: Arc<CompactionService>,
        finalization: Arc<FinalizationService>,
        delivery: Arc<DeliveryService>,
    ) -> Self {
        Self { compaction, finalization, delivery }
    }

    /// Returns the compaction service.
    #[must_use]
    pub(crate) fn compaction(&self) -> &CompactionService {
        self.compaction.as_ref()
    }

    /// Returns the single-terminalization service.
    #[must_use]
    pub(crate) fn finalization(&self) -> &FinalizationService {
        self.finalization.as_ref()
    }

    /// Returns the durable-delivery service.
    #[must_use]
    pub(crate) fn delivery(&self) -> &DeliveryService {
        self.delivery.as_ref()
    }
}

/// Complete authoritative service set available to RuntimeKernelV2.
pub(crate) struct RuntimeKernelServices {
    turn: RuntimeKernelTurnServices,
    lifecycle: RuntimeKernelLifecycleServices,
    tool_authority: Arc<dyn ToolAuthorityGateway>,
}

impl fmt::Debug for RuntimeKernelServices {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeKernelServices")
            .field("turn", &self.turn)
            .field("lifecycle", &self.lifecycle)
            .field("tool_authority", &"live_tool_flow_facade")
            .finish()
    }
}

impl RuntimeKernelServices {
    /// Creates an authoritative capability set.
    #[must_use]
    pub(crate) fn new(
        turn: RuntimeKernelTurnServices,
        lifecycle: RuntimeKernelLifecycleServices,
        tool_authority: Arc<dyn ToolAuthorityGateway>,
    ) -> Self {
        Self { turn, lifecycle, tool_authority }
    }

    /// Returns provider-turn services.
    #[must_use]
    pub(crate) const fn turn(&self) -> &RuntimeKernelTurnServices {
        &self.turn
    }

    /// Returns post-turn services.
    #[must_use]
    pub(crate) const fn lifecycle(&self) -> &RuntimeKernelLifecycleServices {
        &self.lifecycle
    }

    /// Returns the sole tool authority gateway.
    #[must_use]
    pub(crate) fn tool_authority(&self) -> &dyn ToolAuthorityGateway {
        self.tool_authority.as_ref()
    }
}

/// Host-owned, generation-pinned context for an authoritative V2 run.
///
/// Its closed field set is intentional: typed identities, the selection
/// projection, consumed executable grant, flow-control authorities, and narrow
/// service capabilities are the only data allowed to enter the kernel.
pub(crate) struct RuntimeKernelContext {
    identities: RuntimeIdentitySetV1,
    generation: RuntimeGeneration,
    runtime_selection: RuntimeSelectionV1,
    runtime_grant: AuthoritativeRuntimeGrant,
    cancellation: Arc<dyn KernelCancellationAuthority>,
    deadlines: Arc<dyn KernelDeadlineAuthority>,
    backpressure: Arc<dyn KernelBackpressureAuthority>,
    lanes: Arc<dyn KernelLaneAuthority>,
    services: RuntimeKernelServices,
}

impl fmt::Debug for RuntimeKernelContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeKernelContext")
            .field("generation", &self.generation)
            .field("selected_profile", &self.runtime_selection.selected_profile())
            .field(
                "runtime_selection_reason",
                &self.runtime_selection.authority_decision().reason(),
            )
            .field("services", &self.services)
            .finish_non_exhaustive()
    }
}

impl RuntimeKernelContext {
    /// Creates a context only for an explicit authoritative V2 selection.
    ///
    /// Construction preflights the root cancellation generation and every
    /// canonical phase's deadline/backpressure posture. It never accepts a
    /// credential resolver or raw provider/tool payload.
    ///
    /// # Errors
    /// Returns [`RuntimeKernelContextError`] when identities, selection,
    /// generation, or host flow-control authority is inconsistent.
    pub(crate) fn new(
        identities: RuntimeIdentitySetV1,
        resolved_selection: ResolvedRuntimeSelection,
        cancellation: Arc<dyn KernelCancellationAuthority>,
        deadlines: Arc<dyn KernelDeadlineAuthority>,
        backpressure: Arc<dyn KernelBackpressureAuthority>,
        lanes: Arc<dyn KernelLaneAuthority>,
        services: RuntimeKernelServices,
    ) -> Result<Self, RuntimeKernelContextError> {
        identities.validate().map_err(RuntimeKernelContextError::InvalidIdentities)?;
        if has_child_identities(&identities) {
            return Err(RuntimeKernelContextError::ChildIdentitiesInBaseContext);
        }
        let (runtime_selection, runtime_grant) = resolved_selection.into_parts();
        runtime_selection.validate().map_err(RuntimeKernelContextError::InvalidSelection)?;
        let generation = identities.generation;
        if runtime_selection.authority_decision().generation() != generation
            || runtime_grant.run_generation() != generation
        {
            return Err(RuntimeKernelContextError::GenerationMismatch);
        }
        let selected_profile = runtime_selection.selected_profile();
        if runtime_selection.authority_decision().selected_runtime() != Some(RuntimeAuthority::V2)
            || runtime_selection.authority_decision().shadow_evaluation_enabled()
            || !matches!(
                selected_profile,
                RuntimeKernelVersion::V2 | RuntimeKernelVersion::V2Canary
            )
            || runtime_grant.selected_profile() != selected_profile
            || runtime_grant.selected_authority() != Some(RuntimeAuthority::V2)
        {
            return Err(RuntimeKernelContextError::SelectionIsNotAuthoritativeV2);
        }
        validate_runtime_grant(&identities, &runtime_selection, &runtime_grant)?;

        let root = cancellation.root_context();
        root.validate().map_err(|_| RuntimeKernelContextError::InvalidRootCancellation)?;
        if root.scope != CancellationScopeKind::Run || root.generation != generation {
            return Err(RuntimeKernelContextError::InvalidRootCancellation);
        }
        preflight_phase_authorities(deadlines.as_ref(), backpressure.as_ref())?;
        preflight_run_lane_authority(lanes.as_ref(), &identities, &runtime_grant)?;

        Ok(Self {
            identities,
            generation,
            runtime_selection,
            runtime_grant,
            cancellation,
            deadlines,
            backpressure,
            lanes,
            services,
        })
    }

    /// Returns the typed correlation identities.
    #[must_use]
    pub(crate) const fn identities(&self) -> &RuntimeIdentitySetV1 {
        &self.identities
    }

    /// Returns the validated authoritative runtime-selection result.
    #[must_use]
    pub(crate) const fn runtime_selection(&self) -> &RuntimeSelectionV1 {
        &self.runtime_selection
    }

    /// Returns the host service capabilities.
    #[must_use]
    pub(crate) const fn services(&self) -> &RuntimeKernelServices {
        &self.services
    }

    /// Returns proof that this context is authoritative rather than shadow-only.
    #[must_use]
    pub(crate) const fn authority(&self) -> &AuthoritativeRuntimeGrant {
        &self.runtime_grant
    }

    /// Creates a canonical input with host-derived phase controls.
    ///
    /// # Errors
    /// Returns [`RuntimeKernelContextError`] if the host cannot derive the
    /// phase deadline, cancellation scope, or bounded backpressure policy.
    pub(crate) fn phase_input<P, T>(
        &self,
        payload: T,
    ) -> Result<KernelPhaseInput<P, T>, RuntimeKernelContextError>
    where
        P: CanonicalPhase,
    {
        let timeout_ms = self.deadlines.timeout_ms(P::PHASE)?;
        if timeout_ms == 0 {
            return Err(RuntimeKernelContextError::InvalidPhaseAuthority { phase: P::PHASE });
        }
        let cancellation = self.cancellation.derive_scope(P::CANCELLATION_SCOPE, timeout_ms)?;
        let backpressure = self.backpressure.policy(P::PHASE)?;
        let lane_authority = self.lanes.authority_for(P::LANE)?;
        validate_lane_authority(&lane_authority, P::LANE, &self.identities, &self.runtime_grant)?;
        let execution = PhaseExecutionContext::new(
            cancellation,
            timeout_ms,
            backpressure,
            lane_authority,
            P::AUTHORITY_CLASS,
            P::TRACE_POLICY,
        )
        .map_err(RuntimeKernelContextError::InvalidPhaseContract)?;
        KernelPhaseInput::new(self.identities.clone(), self.generation, execution, payload)
            .map_err(RuntimeKernelContextError::InvalidPhaseContract)
    }
}

fn has_child_identities(identities: &RuntimeIdentitySetV1) -> bool {
    identities.attempt_id.is_some()
        || identities.tool_proposal_id.is_some()
        || identities.tool_execution_id.is_some()
        || identities.approval_subject_id.is_some()
        || identities.delivery_intent_id.is_some()
        || identities.plugin_call_id.is_some()
        || identities.context_projection_id.is_some()
        || identities.recovery_action_id.is_some()
        || identities.operation_id.is_some()
        || identities.runtime_instance_id.is_some()
        || !identities.causal_links.is_empty()
}

fn validate_runtime_grant(
    identities: &RuntimeIdentitySetV1,
    selection: &RuntimeSelectionV1,
    grant: &AuthoritativeRuntimeGrant,
) -> Result<(), RuntimeKernelContextError> {
    let evidence = selection.evidence();
    if grant.trace_id() != &identities.trace_id
        || grant.session_id() != &identities.session_id
        || grant.run_id() != &identities.run_id
        || grant.run_generation() != identities.generation
        || grant.selected_profile() != selection.selected_profile()
        || grant.selected_authority() != selection.authority_decision().selected_runtime()
        || grant.admission_snapshot_digest() != evidence.admission_snapshot_digest()
        || grant.persisted_admission_token_digest() != evidence.persisted_admission_token_digest()
        || grant.selection_epochs_digest() != evidence.selection_epochs_digest()
        || grant.selection_digest() != selection.selection_digest()
    {
        return Err(RuntimeKernelContextError::RuntimeGrantBindingMismatch);
    }
    Ok(())
}

fn validate_lane_authority(
    authority: &PhaseLaneAuthority,
    expected_lane: RuntimeGenerationLane,
    identities: &RuntimeIdentitySetV1,
    grant: &AuthoritativeRuntimeGrant,
) -> Result<(), RuntimeKernelContextError> {
    if authority.session_id() != &identities.session_id
        || authority.run_id() != &identities.run_id
        || authority.run_generation() != grant.run_generation()
        || authority.run_lease_id() != grant.run_lease_id()
        || authority.lane() != expected_lane
        || (expected_lane == RuntimeGenerationLane::Run
            && (authority.lane_generation() != grant.run_generation()
                || authority.lane_lease_id() != grant.run_lease_id()))
    {
        return Err(RuntimeKernelContextError::LaneAuthorityBindingMismatch {
            lane: expected_lane,
        });
    }
    Ok(())
}

fn preflight_phase_authorities(
    deadlines: &dyn KernelDeadlineAuthority,
    backpressure: &dyn KernelBackpressureAuthority,
) -> Result<(), RuntimeKernelContextError> {
    for phase in CANONICAL_PHASES {
        if deadlines.timeout_ms(*phase)? == 0 {
            return Err(RuntimeKernelContextError::InvalidPhaseAuthority { phase: *phase });
        }
        backpressure
            .policy(*phase)?
            .validate()
            .map_err(|_| RuntimeKernelContextError::InvalidPhaseAuthority { phase: *phase })?;
    }
    Ok(())
}

fn preflight_run_lane_authority(
    lanes: &dyn KernelLaneAuthority,
    identities: &RuntimeIdentitySetV1,
    grant: &AuthoritativeRuntimeGrant,
) -> Result<(), RuntimeKernelContextError> {
    let authority = lanes.authority_for(RuntimeGenerationLane::Run)?;
    validate_lane_authority(&authority, RuntimeGenerationLane::Run, identities, grant)
}

const CANONICAL_PHASES: &[RuntimeErrorPhase] = &[
    RuntimeErrorPhase::Admission,
    RuntimeErrorPhase::RuntimeSelection,
    RuntimeErrorPhase::ContextAssembly,
    RuntimeErrorPhase::ProviderCall,
    RuntimeErrorPhase::ToolGate,
    RuntimeErrorPhase::Approval,
    RuntimeErrorPhase::ToolExecution,
    RuntimeErrorPhase::ResultProjection,
    RuntimeErrorPhase::Compaction,
    RuntimeErrorPhase::Finalization,
    RuntimeErrorPhase::DeliveryQueue,
];

/// Validation error for an authoritative kernel context.
#[derive(Debug, Error)]
pub(crate) enum RuntimeKernelContextError {
    /// Typed identities failed validation.
    #[error("runtime kernel context identities are invalid")]
    InvalidIdentities(#[source] RuntimeIdentityError),
    /// Runtime selection failed its own closed contract.
    #[error("runtime kernel selection is invalid")]
    InvalidSelection(#[source] RuntimeSelectionError),
    /// A base run context incorrectly carried phase-child identities.
    #[error("runtime kernel base context must not contain child identities")]
    ChildIdentitiesInBaseContext,
    /// Selection and identity generations differ.
    #[error("runtime kernel context generation does not match runtime selection")]
    GenerationMismatch,
    /// Selection did not grant authoritative V2 ownership.
    #[error("runtime kernel context requires an authoritative v2 selection")]
    SelectionIsNotAuthoritativeV2,
    /// Executable grant did not bind the exact selection, trace, session, and run.
    #[error("runtime kernel authoritative grant binding is inconsistent")]
    RuntimeGrantBindingMismatch,
    /// Root cancellation authority was invalid or belonged to another generation.
    #[error("runtime kernel root cancellation authority is invalid")]
    InvalidRootCancellation,
    /// A host flow-control capability was unavailable.
    #[error(transparent)]
    Authority(#[from] KernelAuthorityError),
    /// A phase control contract was malformed.
    #[error("runtime kernel phase contract is invalid")]
    InvalidPhaseContract(#[source] KernelPhaseContractError),
    /// A canonical phase lacked a finite deadline or valid bounded queue.
    #[error("runtime kernel phase authority is invalid for {phase:?}")]
    InvalidPhaseAuthority {
        /// Phase whose host authority was invalid.
        phase: RuntimeErrorPhase,
    },
    /// A phase lane grant did not match the admitted Run lease or requested lane.
    #[error("runtime kernel lane authority is invalid for {lane:?}")]
    LaneAuthorityBindingMismatch {
        /// Canonical lane requested by the phase.
        lane: RuntimeGenerationLane,
    },
}

impl RuntimeKernelContextError {
    /// Returns the stable reason code for diagnostics and failure projection.
    #[must_use]
    pub(crate) const fn reason_code(&self) -> &'static str {
        match self {
            Self::InvalidIdentities(_) => "runtime.kernel.context.invalid_identities",
            Self::InvalidSelection(_) => "runtime.kernel.context.invalid_selection",
            Self::ChildIdentitiesInBaseContext => {
                "runtime.kernel.context.child_identities_in_base_context"
            }
            Self::GenerationMismatch => "runtime.kernel.context.generation_mismatch",
            Self::SelectionIsNotAuthoritativeV2 => {
                "runtime.kernel.context.selection_not_authoritative_v2"
            }
            Self::RuntimeGrantBindingMismatch => {
                "runtime.kernel.context.runtime_grant_binding_mismatch"
            }
            Self::InvalidRootCancellation => "runtime.kernel.context.invalid_root_cancellation",
            Self::Authority(error) => error.reason_code(),
            Self::InvalidPhaseContract(error) => error.reason_code(),
            Self::InvalidPhaseAuthority { .. } => "runtime.kernel.context.invalid_phase_authority",
            Self::LaneAuthorityBindingMismatch { .. } => {
                "runtime.kernel.context.lane_authority_binding_mismatch"
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future;

    use palyra_common::runtime_contracts::{
        BackpressureOverflowAction, CancellationReason, CircuitBreakerPolicy, GenerationLeaseV1,
        RuntimeAuthorityClass, RuntimeComponentHealthV1, RuntimeHealthState, RuntimeInstanceId,
        RuntimeLeaseId, RuntimeRunId, RuntimeSessionId, RuntimeTraceId,
        RUNTIME_COMPONENT_HEALTH_SCHEMA_VERSION, RUNTIME_FLOW_CONTROL_SCHEMA_VERSION,
        RUNTIME_GENERATION_SCHEMA_VERSION,
    };

    use super::super::{
        phases::{
            tool_gateway_sealed, ApprovalWaitPhaseInput, ApprovalWaitPhaseOutput,
            DeliveryDisposition, FinalProjectionRef, KernelCancellationSignal, KernelPhaseError,
            KernelPhaseFuture, ResultProjectionPhaseInput, ResultProjectionPhaseOutput,
            ToolExecutionPhaseInput, ToolExecutionPhaseOutput, ToolGatePhaseInput,
            ToolGatePhaseOutput,
        },
        profile::{RuntimeKernelCompatibilityOverridesV1, RuntimeKernelProfileConfigV1},
        runtime_selection::{
            AdmissionSnapshotReferenceV1, AuthCandidatePolicyReferenceV1, AuthSelectionModeV1,
            ContextEngineBindingV1, ContextEngineRegistryCandidateV1, ExecutionProfileBindingV1,
            FallbackPermissionV1, HarnessBindingV1, HarnessRegistryCandidateV1,
            HostCandidateRegistryProof, HostHealthSnapshotProof,
            HostRuntimeSelectionAuthorityProof, ImmutableHealthSnapshotV1,
            MiddlewareChainBindingV1, ProviderRegistryCandidateV1, ProviderRouteBindingV1,
            ProviderRouteClassV1, RuntimeCapabilityRequirementsV1, RuntimeFallbackPolicyV1,
            RuntimeSelectionRequest as AtomicRuntimeSelectionRequest,
            RuntimeSelectionService as AtomicRuntimeSelectionService, SafeLabel,
            SealedRuntimeCandidateRegistryV1, SealedToolCatalogSelectionV1, SelectionDigest,
            SelectionEpochsV1, SessionOverridePolicyV1,
        },
        selection::{
            resolve_runtime_authority, RuntimeAuthorityProgressEvidence, V2RuntimeAvailability,
        },
    };
    use super::*;
    use crate::application::{
        agent_harness::AgentHarnessDescriptor, context_engine::ContextEngineDescriptor,
    };

    struct NeverCancelled;

    impl KernelCancellationSignal for NeverCancelled {
        fn current_reason(&self) -> Option<CancellationReason> {
            None
        }

        fn cancelled(&self) -> KernelPhaseFuture<'_, CancellationReason> {
            Box::pin(future::pending())
        }
    }

    struct TestCancellationAuthority {
        root: CancellationContextV1,
    }

    impl KernelCancellationAuthority for TestCancellationAuthority {
        fn root_context(&self) -> CancellationContextV1 {
            self.root.clone()
        }

        fn derive_scope(
            &self,
            scope: CancellationScopeKind,
            _timeout_ms: u64,
        ) -> Result<KernelCancellationScope, KernelAuthorityError> {
            let mut context = self.root.clone();
            context.scope = scope;
            context.scope_id = palyra_common::runtime_contracts::RuntimeOperationId::parse(
                format!("phase_scope_{}", scope.as_str()).as_str(),
            )
            .map_err(|_| KernelAuthorityError::Cancellation)?;
            context.parent_scope_id = if scope == CancellationScopeKind::Run {
                None
            } else {
                Some(self.root.scope_id.clone())
            };
            KernelCancellationScope::new(context, Arc::new(NeverCancelled))
                .map_err(|_| KernelAuthorityError::Cancellation)
        }
    }

    struct TestDeadlineAuthority;

    impl KernelDeadlineAuthority for TestDeadlineAuthority {
        fn timeout_ms(&self, _phase: RuntimeErrorPhase) -> Result<u64, KernelAuthorityError> {
            Ok(1_000)
        }
    }

    struct TestBackpressureAuthority;

    impl KernelBackpressureAuthority for TestBackpressureAuthority {
        fn policy(
            &self,
            _phase: RuntimeErrorPhase,
        ) -> Result<BackpressurePolicy, KernelAuthorityError> {
            Ok(BackpressurePolicy {
                schema_version: RUNTIME_FLOW_CONTROL_SCHEMA_VERSION,
                capacity: 16,
                overflow_action: BackpressureOverflowAction::BlockProducer,
                preserve_terminal: true,
                preserve_approval: true,
                max_summary_bytes: 512,
            })
        }
    }

    struct TestLaneAuthority {
        session_id: RuntimeSessionId,
        run_id: RuntimeRunId,
        run_lease_id: RuntimeLeaseId,
    }

    impl KernelLaneAuthority for TestLaneAuthority {
        fn authority_for(
            &self,
            lane: RuntimeGenerationLane,
        ) -> Result<PhaseLaneAuthority, KernelAuthorityError> {
            let lane_generation = if lane == RuntimeGenerationLane::Run {
                generation()
            } else {
                RuntimeGeneration::new(13).expect("child lane generation")
            };
            let lane_lease_id = if lane == RuntimeGenerationLane::Run {
                self.run_lease_id.clone()
            } else {
                RuntimeLeaseId::parse(format!("lease-{}", lane.as_str()).as_str())
                    .map_err(|_| KernelAuthorityError::LaneAuthority)?
            };
            Ok(PhaseLaneAuthority::from_host_leases(
                self.session_id.clone(),
                self.run_id.clone(),
                generation(),
                self.run_lease_id.clone(),
                lane,
                lane_generation,
                lane_lease_id,
            ))
        }
    }

    struct UnavailablePhaseService;

    impl<P, I, O> RuntimePhaseService<P, I, O> for UnavailablePhaseService
    where
        P: CanonicalPhase,
        I: Send + 'static,
        O: Send + 'static,
    {
        fn execute(
            &self,
            _input: KernelPhaseInput<P, I>,
        ) -> KernelPhaseFuture<
            '_,
            Result<super::super::phases::KernelPhaseOutput<P, O>, KernelPhaseError>,
        > {
            Box::pin(async { Err(KernelPhaseContractError::InvalidFlowControl.into()) })
        }
    }

    struct UnavailableToolGateway;

    impl tool_gateway_sealed::LiveToolFlowAdapter for UnavailableToolGateway {}

    impl ToolAuthorityGateway for UnavailableToolGateway {
        fn gate(
            &self,
            _input: ToolGatePhaseInput,
        ) -> KernelPhaseFuture<'_, Result<ToolGatePhaseOutput, KernelPhaseError>> {
            Box::pin(async { Err(KernelPhaseContractError::InvalidFlowControl.into()) })
        }

        fn wait_or_resume_approval(
            &self,
            _input: ApprovalWaitPhaseInput,
        ) -> KernelPhaseFuture<'_, Result<ApprovalWaitPhaseOutput, KernelPhaseError>> {
            Box::pin(async { Err(KernelPhaseContractError::InvalidFlowControl.into()) })
        }

        fn execute(
            &self,
            _input: ToolExecutionPhaseInput,
        ) -> KernelPhaseFuture<'_, Result<ToolExecutionPhaseOutput, KernelPhaseError>> {
            Box::pin(async { Err(KernelPhaseContractError::InvalidFlowControl.into()) })
        }

        fn project_result(
            &self,
            _input: ResultProjectionPhaseInput,
        ) -> KernelPhaseFuture<'_, Result<ResultProjectionPhaseOutput, KernelPhaseError>> {
            Box::pin(async { Err(KernelPhaseContractError::InvalidFlowControl.into()) })
        }
    }

    fn generation() -> RuntimeGeneration {
        RuntimeGeneration::new(7).expect("test generation is non-zero")
    }

    fn identities() -> RuntimeIdentitySetV1 {
        RuntimeIdentitySetV1::for_run(
            RuntimeTraceId::parse("trace_context").expect("trace id"),
            RuntimeSessionId::parse("session_context").expect("session id"),
            RuntimeRunId::parse("run_context").expect("run id"),
            generation(),
        )
    }

    fn authority_decision() -> super::super::selection::RuntimeAuthorityDecisionV1 {
        let profile = RuntimeKernelProfileConfigV1::new(
            RuntimeKernelVersion::V2,
            0,
            RuntimeKernelCompatibilityOverridesV1::none(),
        )
        .expect("profile");
        resolve_runtime_authority(
            &profile,
            &identities(),
            V2RuntimeAvailability::Ready,
            RuntimeAuthorityProgressEvidence::pristine(),
            None,
        )
        .expect("selection")
    }

    fn digest(seed: &str) -> SelectionDigest {
        SelectionDigest::from_domain_bytes(b"context-test\0", seed.as_bytes())
    }

    fn label(value: &str) -> SafeLabel {
        SafeLabel::parse(value.to_owned()).expect("safe selection label")
    }

    fn instance(id: &str) -> RuntimeInstanceId {
        RuntimeInstanceId::parse(id).expect("component id")
    }

    fn health(id: &str) -> RuntimeComponentHealthV1 {
        RuntimeComponentHealthV1 {
            schema_version: RUNTIME_COMPONENT_HEALTH_SCHEMA_VERSION,
            component_id: instance(id),
            generation: generation(),
            state: RuntimeHealthState::Healthy,
            authority_class: RuntimeAuthorityClass::ScopedMutation,
            strike_count: 0,
            reason_code: "runtime.health.healthy".to_owned(),
            first_failure_at_unix_ms: None,
            last_failure_at_unix_ms: None,
            expires_at_unix_ms: None,
            fallback_component_id: None,
            fallback_authority_class: None,
            security_quarantine: false,
            policy: CircuitBreakerPolicy {
                strike_threshold: 3,
                cooldown_ms: 100,
                max_probe_concurrency: 1,
                security_quarantine_auto_clear: false,
            },
            updated_at_unix_ms: 30,
        }
    }

    fn resolved_selection() -> ResolvedRuntimeSelection {
        let authority = RuntimeAuthorityClass::ScopedMutation;
        let harness_descriptor = AgentHarnessDescriptor::new("embedded", "embedded harness", true);
        let context_descriptor = ContextEngineDescriptor {
            engine_id: "context".to_owned(),
            label: "context engine".to_owned(),
            version: "1.0.0".to_owned(),
            lifecycle_hooks: vec!["prepare_context".to_owned()],
        };
        let identities = identities();
        let run_lease = GenerationLeaseV1 {
            schema_version: RUNTIME_GENERATION_SCHEMA_VERSION,
            lease_id: RuntimeLeaseId::parse("lease-context").expect("run lease"),
            session_id: identities.session_id.clone(),
            run_id: Some(identities.run_id.clone()),
            lane: RuntimeGenerationLane::Run,
            generation: generation(),
            owner: "runtime-selection".to_owned(),
            acquired_at_unix_ms: 1,
            expires_at_unix_ms: 1_000,
        };
        let epochs = SelectionEpochsV1::new(23, 29).expect("selection epochs");
        let candidate_registry = SealedRuntimeCandidateRegistryV1::seal(
            HostCandidateRegistryProof::test_only(41),
            vec![HarnessRegistryCandidateV1::new(
                HarnessBindingV1::from_registry_descriptor(
                    &harness_descriptor,
                    label("1.0.0"),
                    authority,
                )
                .expect("harness binding"),
                instance("harness"),
                vec![label("text")],
                0,
            )
            .expect("harness candidate")],
            vec![ContextEngineRegistryCandidateV1::new(
                ContextEngineBindingV1::from_registry_descriptor(
                    &context_descriptor,
                    11,
                    authority,
                )
                .expect("context binding"),
                instance("context"),
                vec![label("budgeting")],
                0,
            )
            .expect("context candidate")],
            vec![ProviderRegistryCandidateV1::new(
                ProviderRouteBindingV1::new(
                    label("route-primary"),
                    label("provider-primary"),
                    label("provider-model"),
                    ProviderRouteClassV1::Primary,
                    AuthCandidatePolicyReferenceV1::new(
                        AuthSelectionModeV1::HostPolicy,
                        digest("auth-candidates"),
                        digest("auth-policy"),
                    ),
                    authority,
                ),
                instance("provider"),
                vec![label("chat")],
                0,
            )
            .expect("provider candidate")],
        )
        .expect("candidate registry");
        let request = AtomicRuntimeSelectionRequest {
            admission_snapshot: AdmissionSnapshotReferenceV1::new(
                label("session-context-binding"),
                digest("admission"),
                generation(),
                authority,
            )
            .expect("admission snapshot"),
            override_policy: SessionOverridePolicyV1::deny_all(authority).expect("override policy"),
            capability_requirements: RuntimeCapabilityRequirementsV1::new(
                vec![label("text")],
                vec![label("budgeting")],
                vec![label("chat")],
                Vec::new(),
            )
            .expect("capability requirements"),
            health: ImmutableHealthSnapshotV1::capture(
                HostHealthSnapshotProof::test_only(43),
                50,
                vec![health("harness"), health("context"), health("provider")],
            )
            .expect("health snapshot"),
            fallback_policy: RuntimeFallbackPolicyV1::new(
                FallbackPermissionV1::BeforeProgress,
                FallbackPermissionV1::BeforeProgress,
            )
            .expect("fallback policy"),
            candidates: candidate_registry,
            tool_catalog: SealedToolCatalogSelectionV1::test_only(
                label("toolcat-context"),
                17,
                Vec::new(),
            ),
            middleware_chain: MiddlewareChainBindingV1::new(vec![label("production")])
                .expect("middleware"),
            execution_profile: ExecutionProfileBindingV1::new(label("production"), authority)
                .expect("execution profile"),
            epochs: epochs.clone(),
        };
        let proof = HostRuntimeSelectionAuthorityProof::test_only(
            identities,
            run_lease,
            authority_decision(),
            digest("admission"),
            digest("persisted-admission"),
            epochs,
        )
        .expect("authority proof");
        AtomicRuntimeSelectionService::select(proof, &request).expect("resolved selection")
    }

    fn services() -> RuntimeKernelServices {
        let admission: Arc<AdmissionService> = Arc::new(UnavailablePhaseService);
        let runtime_selection: Arc<RuntimeSelectionService> = Arc::new(UnavailablePhaseService);
        let context_assembly: Arc<ContextAssemblyService> = Arc::new(UnavailablePhaseService);
        let provider_call: Arc<ProviderCallService> = Arc::new(UnavailablePhaseService);
        let compaction: Arc<CompactionService> = Arc::new(UnavailablePhaseService);
        let finalization: Arc<FinalizationService> = Arc::new(UnavailablePhaseService);
        let delivery: Arc<DeliveryService> = Arc::new(UnavailablePhaseService);
        let turn = RuntimeKernelTurnServices::with_preflight_services(
            admission,
            runtime_selection,
            context_assembly,
            provider_call,
        );
        assert!(turn.admission().is_some());
        assert!(turn.runtime_selection().is_some());
        RuntimeKernelServices::new(
            turn,
            RuntimeKernelLifecycleServices::new(compaction, finalization, delivery),
            Arc::new(UnavailableToolGateway),
        )
    }

    fn build_context(
        identities: RuntimeIdentitySetV1,
        resolved_selection: ResolvedRuntimeSelection,
    ) -> Result<RuntimeKernelContext, RuntimeKernelContextError> {
        let root = CancellationContextV1 {
            schema_version: RUNTIME_FLOW_CONTROL_SCHEMA_VERSION,
            scope_id: palyra_common::runtime_contracts::RuntimeOperationId::parse(
                "run_context_scope",
            )
            .expect("scope id"),
            scope: CancellationScopeKind::Run,
            generation: generation(),
            parent_scope_id: None,
            reason: None,
            deadline_unix_ms: Some(10_000),
            graceful_settle_ms: 100,
            hard_abort_after_ms: 1_000,
        };
        let lanes: Arc<dyn KernelLaneAuthority> = Arc::new(TestLaneAuthority {
            session_id: identities.session_id.clone(),
            run_id: identities.run_id.clone(),
            run_lease_id: RuntimeLeaseId::parse("lease-context").expect("run lease"),
        });
        RuntimeKernelContext::new(
            identities,
            resolved_selection,
            Arc::new(TestCancellationAuthority { root }),
            Arc::new(TestDeadlineAuthority),
            Arc::new(TestBackpressureAuthority),
            lanes,
            services(),
        )
    }

    fn context() -> RuntimeKernelContext {
        build_context(identities(), resolved_selection()).expect("context")
    }

    #[test]
    fn context_field_allowlist_excludes_secret_and_raw_credential_storage() {
        let source = include_str!("context.rs");
        let declaration = source
            .split("pub(crate) struct RuntimeKernelContext {")
            .nth(1)
            .and_then(|tail| tail.split("\n}").next())
            .expect("context declaration");
        let field_names = declaration
            .lines()
            .filter_map(|line| line.trim().split_once(':').map(|(name, _)| name))
            .collect::<Vec<_>>();

        assert_eq!(
            field_names,
            [
                "identities",
                "generation",
                "runtime_selection",
                "runtime_grant",
                "cancellation",
                "deadlines",
                "backpressure",
                "lanes",
                "services",
            ]
        );
        assert!(!declaration.contains("credential"));
        assert!(!declaration.contains("secret"));
        assert!(!declaration.contains("token"));
        assert!(!declaration.contains("provider_request"));
        assert!(!declaration.contains("tool_argument"));
    }

    #[test]
    fn context_debug_is_identity_free_and_capability_only() {
        let debug = format!("{:?}", context());

        assert!(!debug.contains("trace_context"));
        assert!(!debug.contains("session_context"));
        assert!(!debug.contains("run_context"));
        assert!(debug.contains("live_tool_flow_facade"));
        assert!(!debug.to_ascii_lowercase().contains("credential"));
    }

    #[test]
    fn context_rejects_child_identities_in_the_base_identity_set() {
        let mut child_bound = identities();
        child_bound.attempt_id = Some(
            palyra_common::runtime_contracts::RuntimeAttemptId::parse("attempt_child")
                .expect("attempt id"),
        );

        assert!(matches!(
            build_context(child_bound, resolved_selection()),
            Err(RuntimeKernelContextError::ChildIdentitiesInBaseContext)
        ));
    }

    #[test]
    fn context_rejects_authoritative_grant_from_another_run() {
        let mut another_run = identities();
        another_run.run_id = RuntimeRunId::parse("run_other").expect("other run id");

        assert!(matches!(
            build_context(another_run, resolved_selection()),
            Err(RuntimeKernelContextError::RuntimeGrantBindingMismatch)
        ));
    }

    #[test]
    fn context_consumes_non_cloneable_authoritative_runtime_grant() {
        let source = include_str!("runtime_selection/authority.rs");
        let declaration = source
            .split("pub(crate) struct AuthoritativeRuntimeGrant {")
            .nth(1)
            .and_then(|tail| tail.split("\n}").next())
            .expect("grant declaration");
        let clone_impl = ["impl Clone", " for AuthoritativeRuntimeGrant"].concat();
        let deserialize_impl =
            ["impl<'de> Deserialize<'de>", " for AuthoritativeRuntimeGrant"].concat();

        assert!(declaration.contains("run_lease: GenerationLeaseV1"));
        assert!(declaration.contains("selection_digest: SelectionDigest"));
        assert!(!source.contains(clone_impl.as_str()));
        assert!(!source.contains(deserialize_impl.as_str()));
    }

    #[test]
    fn authoritative_context_derives_generation_bound_phase_controls() {
        let context = context();
        let input = context
            .phase_input::<DeliveryPhase, _>(DeliveryRequest {
                delivery_intent_id:
                    palyra_common::runtime_contracts::RuntimeDeliveryIntentId::parse(
                        "delivery_context",
                    )
                    .expect("delivery id"),
                final_projection: FinalProjectionRef::from_host(
                    palyra_common::runtime_contracts::RuntimeOperationId::parse("final_projection")
                        .expect("projection ref"),
                    [9; 32],
                ),
            })
            .expect("delivery input");

        assert_eq!(input.boundary().generation(), generation());
        assert_eq!(
            input.boundary().execution().cancellation().context().scope,
            CancellationScopeKind::Delivery
        );
        assert_eq!(
            input.boundary().execution().authority_class(),
            super::super::phases::PhaseAuthorityClass::TerminalDelivery
        );
        let _ = DeliveryDisposition::Queued;
    }
}
