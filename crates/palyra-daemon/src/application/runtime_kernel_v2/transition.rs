// Typed transitions, outcomes, validation errors, and legal state edges.

/// Typed orchestration decision prepared alongside one canonical runtime event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum KernelTransition {
    /// Begin runtime selection.
    BeginRuntimeSelection,
    /// Begin assembling provider context.
    BeginContextAssembly,
    /// Begin or continue a provider call.
    BeginProviderCall,
    /// Begin policy evaluation for a proposed tool call.
    BeginToolGate,
    /// Pause for operator approval.
    BeginApprovalWait,
    /// Return an approval decision to the tool gate.
    ResumeToolGate,
    /// Begin an authorized tool operation.
    BeginToolExecution,
    /// Project a denied or host-synthetic proposal without creating execution authority.
    ResolveToolWithoutExecution,
    /// Begin projecting an observed or reconciled tool result.
    BeginResultProjection,
    /// Begin context compaction after the provider attempt ended.
    BeginCompaction,
    /// Begin output and terminal finalization.
    BeginFinalization,
    /// Wait behind a durable delivery intent.
    BeginDeliveryWait,
    /// Complete the run successfully.
    Complete,
    /// End the run in failure.
    Fail,
    /// Cancel the run.
    Cancel,
    /// Pause progress without terminalizing.
    Suspend,
    /// Enter fail-closed recovery handling.
    BeginRecovery,
    /// Resume selection within the same Run generation after recovery cleanup.
    ResumeRuntimeSelection,
}

impl KernelTransition {
    const fn target_state(self) -> KernelState {
        match self {
            Self::BeginRuntimeSelection | Self::ResumeRuntimeSelection => {
                KernelState::SelectingRuntime
            }
            Self::BeginContextAssembly => KernelState::AssemblingContext,
            Self::BeginProviderCall => KernelState::CallingProvider,
            Self::BeginToolGate | Self::ResumeToolGate => KernelState::AwaitingToolGate,
            Self::BeginApprovalWait => KernelState::AwaitingApproval,
            Self::BeginToolExecution => KernelState::ExecutingTool,
            Self::ResolveToolWithoutExecution | Self::BeginResultProjection => {
                KernelState::ProjectingResult
            }
            Self::BeginCompaction => KernelState::Compacting,
            Self::BeginFinalization => KernelState::Finalizing,
            Self::BeginDeliveryWait => KernelState::AwaitingDelivery,
            Self::Complete => KernelState::Done,
            Self::Fail => KernelState::Failed,
            Self::Cancel => KernelState::Cancelled,
            Self::Suspend => KernelState::Suspended,
            Self::BeginRecovery => KernelState::RecoveryPending,
        }
    }

    const fn reason_code(self) -> &'static str {
        match self {
            Self::BeginRuntimeSelection => "runtime.kernel.transition.runtime_selection_started",
            Self::BeginContextAssembly => "runtime.kernel.transition.context_assembly_started",
            Self::BeginProviderCall => "runtime.kernel.transition.provider_call_started",
            Self::BeginToolGate => "runtime.kernel.transition.tool_gate_started",
            Self::BeginApprovalWait => "runtime.kernel.transition.approval_wait_started",
            Self::ResumeToolGate => "runtime.kernel.transition.approval_resolved",
            Self::BeginToolExecution => "runtime.kernel.transition.tool_execution_started",
            Self::ResolveToolWithoutExecution => {
                "runtime.kernel.transition.tool_resolved_without_execution"
            }
            Self::BeginResultProjection => "runtime.kernel.transition.result_projection_started",
            Self::BeginCompaction => "runtime.kernel.transition.compaction_required",
            Self::BeginFinalization => "runtime.kernel.transition.finalization_started",
            Self::BeginDeliveryWait => "runtime.kernel.transition.delivery_intent_recorded",
            Self::Complete => "runtime.kernel.transition.completed",
            Self::Fail => "runtime.kernel.transition.failed",
            Self::Cancel => "runtime.kernel.transition.cancelled",
            Self::Suspend => "runtime.kernel.transition.suspended_for_backpressure",
            Self::BeginRecovery => "runtime.kernel.transition.recovery_pending",
            Self::ResumeRuntimeSelection => "runtime.kernel.transition.recovery_cleanup_completed",
        }
    }
}

/// Durable result of a transition evaluation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum TransitionOutcome {
    /// The prepared record advances the state after journal commit.
    Applied {
        /// State persisted before the transition.
        previous_state: KernelState,
        /// State to persist atomically with the event.
        next_state: KernelState,
        /// Stable transition reason persisted for diagnostics.
        reason_code: String,
    },
    /// The exact last committed request is being replayed.
    Duplicate {
        /// State retained without mutation.
        state: KernelState,
        /// Stable duplicate disposition.
        reason_code: String,
    },
}

impl TransitionOutcome {
    /// Validates the outcome against prepared previous and next snapshots.
    ///
    /// # Errors
    /// Returns [`KernelTransitionError::InvalidOutcome`] when the durable
    /// disposition contradicts its snapshots or transition.
    pub(crate) fn validate(
        &self,
        previous: &KernelStateSnapshot,
        next: &KernelStateSnapshot,
        transition: KernelTransition,
    ) -> Result<(), KernelTransitionError> {
        match self {
            Self::Applied { previous_state, next_state, reason_code }
                if *previous_state == previous.state
                    && *next_state == next.state
                    && next.state == transition.target_state()
                    && reason_code == transition.reason_code()
                    && previous != next =>
            {
                Ok(())
            }
            Self::Duplicate { state, reason_code }
                if *state == previous.state
                    && previous == next
                    && reason_code == "runtime.kernel.transition.duplicate" =>
            {
                Ok(())
            }
            Self::Applied { .. } | Self::Duplicate { .. } => {
                Err(KernelTransitionError::InvalidOutcome)
            }
        }
    }
}

/// Journal-ready transition whose snapshots and request evidence validate as one record.
///
/// Full-history idempotency remains journal-owned. The pure kernel recognizes
/// only exact replay of the last committed request represented by its snapshot.
#[must_use = "prepared transitions must be committed atomically or discarded"]
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) struct PreparedKernelTransition {
    schema_version: u32,
    previous_snapshot: KernelStateSnapshot,
    next_snapshot: KernelStateSnapshot,
    idempotency_key: String,
    request_sha256: String,
    lane_authority: KernelLaneAuthoritySet,
    event: RuntimeEventEnvelopeV2,
    transition: KernelTransition,
    outcome: TransitionOutcome,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct PreparedKernelTransitionWire {
    schema_version: u32,
    previous_snapshot: KernelStateSnapshot,
    next_snapshot: KernelStateSnapshot,
    idempotency_key: String,
    request_sha256: String,
    lane_authority: KernelLaneAuthoritySet,
    event: RuntimeEventEnvelopeV2,
    transition: KernelTransition,
    outcome: TransitionOutcome,
}

impl<'de> Deserialize<'de> for PreparedKernelTransition {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = PreparedKernelTransitionWire::deserialize(deserializer)?;
        let prepared = Self {
            schema_version: wire.schema_version,
            previous_snapshot: wire.previous_snapshot,
            next_snapshot: wire.next_snapshot,
            idempotency_key: wire.idempotency_key,
            request_sha256: wire.request_sha256,
            lane_authority: wire.lane_authority,
            event: wire.event,
            transition: wire.transition,
            outcome: wire.outcome,
        };
        prepared.validate().map_err(D::Error::custom)?;
        Ok(prepared)
    }
}

impl PreparedKernelTransition {
    /// Returns the immutable snapshot evaluated by the kernel.
    #[must_use]
    pub(crate) fn previous_snapshot(&self) -> &KernelStateSnapshot {
        &self.previous_snapshot
    }

    /// Returns the exact journal revision that must still be current at commit.
    #[must_use]
    pub(crate) const fn expected_revision(&self) -> u64 {
        self.previous_snapshot.revision
    }

    /// Validates the journal head revision before any durable write.
    ///
    /// # Errors
    /// Returns [`KernelTransitionError::RevisionConflict`] when another commit
    /// advanced or replaced the journal head after this transition prepared.
    #[cfg(test)]
    pub(crate) fn validate_expected_revision(
        &self,
        actual_revision: u64,
    ) -> Result<(), KernelTransitionError> {
        let expected = self.expected_revision();
        if actual_revision == expected {
            Ok(())
        } else {
            Err(KernelTransitionError::RevisionConflict { expected, actual: actual_revision })
        }
    }

    /// Returns the snapshot that may become authoritative only after journal commit.
    #[must_use]
    pub(crate) fn next_snapshot(&self) -> &KernelStateSnapshot {
        &self.next_snapshot
    }

    /// Returns the durable transition disposition.
    #[must_use]
    pub(crate) fn outcome(&self) -> &TransitionOutcome {
        &self.outcome
    }

    /// Returns the caller-supplied bounded idempotency key.
    #[must_use]
    pub(crate) fn idempotency_key(&self) -> &str {
        self.idempotency_key.as_str()
    }

    /// Returns the canonical SHA-256 of event, transition, reason, and idempotency key.
    #[must_use]
    pub(crate) fn request_sha256(&self) -> &str {
        self.request_sha256.as_str()
    }

    /// Returns the lane leases that must be revalidated in the commit transaction.
    #[must_use]
    pub(crate) const fn lane_authority(&self) -> &KernelLaneAuthoritySet {
        &self.lane_authority
    }

    /// Returns the canonical runtime event committed with this transition.
    #[must_use]
    pub(crate) const fn event(&self) -> &RuntimeEventEnvelopeV2 {
        &self.event
    }

    /// Returns the typed state transition committed with the event.
    #[must_use]
    pub(crate) const fn transition(&self) -> KernelTransition {
        self.transition
    }

    /// Validates the complete record before persistence or restore.
    ///
    /// # Errors
    /// Returns [`KernelTransitionError::InvalidPreparedTransition`] when any
    /// snapshot, digest, event, transition, or outcome field was tampered.
    pub(crate) fn validate(&self) -> Result<(), KernelTransitionError> {
        if self.schema_version != PREPARED_KERNEL_TRANSITION_SCHEMA_VERSION
            || !is_idempotency_key(self.idempotency_key.as_str())
        {
            return Err(KernelTransitionError::InvalidPreparedTransition);
        }
        self.previous_snapshot
            .validate()
            .map_err(|_| KernelTransitionError::InvalidPreparedTransition)?;
        self.next_snapshot
            .validate()
            .map_err(|_| KernelTransitionError::InvalidPreparedTransition)?;
        self.event.validate().map_err(|_| KernelTransitionError::InvalidPreparedTransition)?;
        self.lane_authority
            .validate(&self.previous_snapshot.base_identities)
            .map_err(|_| KernelTransitionError::InvalidPreparedTransition)?;
        self.lane_authority
            .validate_admitted_run_lease(
                &self.previous_snapshot.base_identities,
                &self.previous_snapshot.run_lease,
            )
            .map_err(|_| KernelTransitionError::InvalidPreparedTransition)?;
        validate_event_lane_authority(&self.lane_authority, &self.event)
            .map_err(|_| KernelTransitionError::InvalidPreparedTransition)?;
        let request_sha256 =
            request_sha256(self.idempotency_key.as_str(), &self.event, self.transition)
                .map_err(|_| KernelTransitionError::InvalidPreparedTransition)?;
        if self.request_sha256 != request_sha256 {
            return Err(KernelTransitionError::InvalidPreparedTransition);
        }
        self.outcome
            .validate(&self.previous_snapshot, &self.next_snapshot, self.transition)
            .map_err(|_| KernelTransitionError::InvalidPreparedTransition)?;

        match self.outcome {
            TransitionOutcome::Applied { .. } => {
                let expected = derive_next_snapshot(
                    &self.previous_snapshot,
                    &self.event,
                    self.transition,
                    self.idempotency_key.as_str(),
                    self.request_sha256.as_str(),
                    &self.lane_authority,
                )
                .map_err(|_| KernelTransitionError::InvalidPreparedTransition)?;
                if expected != self.next_snapshot {
                    return Err(KernelTransitionError::InvalidPreparedTransition);
                }
            }
            TransitionOutcome::Duplicate { .. } => {
                if !last_request_matches(
                    &self.previous_snapshot,
                    self.idempotency_key.as_str(),
                    self.request_sha256.as_str(),
                    &self.event,
                    self.transition,
                ) {
                    return Err(KernelTransitionError::InvalidPreparedTransition);
                }
            }
        }
        Ok(())
    }
}

/// Fail-closed errors produced by kernel construction and transition preparation.
#[derive(Debug, Clone, PartialEq, Error)]
pub(crate) enum KernelTransitionError {
    /// Admission did not present an authority decision that permits this V2 posture.
    #[error("runtime kernel authority decision does not admit V2 evaluation")]
    InvalidRuntimeAuthorityDecision,
    /// The persisted authority decision selected another Run generation.
    #[error(
        "runtime kernel authority generation mismatch: expected {active}, observed {observed}"
    )]
    RuntimeAuthorityGenerationMismatch {
        /// Run generation bound to the exact base identities.
        active: RuntimeGeneration,
        /// Generation selected by the persisted authority decision.
        observed: RuntimeGeneration,
    },
    /// Admission did not present a structurally valid exact Run lease.
    #[error("runtime kernel admitted Run lease is invalid")]
    InvalidRunLease,
    /// The Run lease was not active at the explicit admission timestamp.
    #[error("runtime kernel admitted Run lease is not active")]
    InactiveRunLease,
    /// A transition authority set replaced the immutable admitted Run lease.
    #[error("runtime kernel transition Run lease does not match admission")]
    RunLeaseMismatch,
    /// Base identities were malformed or carried child-operation identities.
    #[error("runtime kernel base identities are invalid")]
    InvalidBaseIdentities,
    /// The caller did not hold the immutable Run generation.
    #[error("runtime kernel run generation mismatch: expected {active}, observed {observed}")]
    RunGenerationMismatch {
        /// Run generation bound to the snapshot.
        active: RuntimeGeneration,
        /// Run generation presented by the caller.
        observed: RuntimeGeneration,
    },
    /// The supplied set of active lane leases was malformed or cross-run.
    #[error("runtime kernel lane authority is invalid")]
    InvalidLaneAuthority,
    /// The event lane was absent from the authority set.
    #[error("runtime kernel has no active authority for lane {lane}")]
    MissingLaneAuthority {
        /// Canonical event generation lane.
        lane: RuntimeGenerationLane,
    },
    /// The event carried a stale or unrelated lane generation.
    #[error(
        "runtime kernel lane {lane} generation mismatch: expected {active}, observed {observed}"
    )]
    LaneGenerationMismatch {
        /// Canonical event generation lane.
        lane: RuntimeGenerationLane,
        /// Active generation for that lane.
        active: RuntimeGeneration,
        /// Event generation.
        observed: RuntimeGeneration,
    },
    /// The event failed the shared runtime-envelope contract.
    #[error("runtime kernel event envelope is invalid")]
    InvalidEnvelope {
        /// Shared envelope validation failure.
        #[source]
        source: RuntimeEventValidationError,
    },
    /// The event did not belong to the snapshot's trace/session/run.
    #[error("runtime kernel event identity does not match the active run")]
    IdentityMismatch,
    /// A child identity did not match the active attempt or tool chain.
    #[error("runtime kernel child identity {field} does not match active correlation")]
    ChildIdentityMismatch {
        /// Stable identity field name.
        field: &'static str,
    },
    /// The exact last event identity was reused for different request content.
    #[error("runtime kernel event id conflicts with the last committed request")]
    EventIdConflict,
    /// The event sequence was lower than its lane-generation cursor.
    #[error(
        "runtime kernel event sequence {observed} is stale for {lane} generation {generation}; last accepted sequence is {last}"
    )]
    StaleSequence {
        /// Independent event lane.
        lane: RuntimeGenerationLane,
        /// Lane generation.
        generation: RuntimeGeneration,
        /// Last accepted sequence.
        last: u64,
        /// Rejected sequence.
        observed: u64,
    },
    /// A lane-generation sequence was reused for different request content.
    #[error(
        "runtime kernel event sequence {sequence} conflicts for {lane} generation {generation}"
    )]
    SequenceConflict {
        /// Independent event lane.
        lane: RuntimeGenerationLane,
        /// Lane generation.
        generation: RuntimeGeneration,
        /// Reused sequence.
        sequence: u64,
    },
    /// The event phase cannot authorize the requested transition.
    #[error(
        "runtime kernel event phase {observed} cannot authorize transition {transition:?} from {state:?}"
    )]
    EventPhaseMismatch {
        /// Current state.
        state: KernelState,
        /// Requested transition.
        transition: KernelTransition,
        /// Validated event phase.
        observed: RuntimeErrorPhase,
    },
    /// The event and state do not form an allowed edge.
    #[error(
        "runtime kernel event {event_name} cannot apply transition {transition:?} from {state:?}"
    )]
    InvalidTransition {
        /// Current state.
        state: KernelState,
        /// Requested transition.
        transition: KernelTransition,
        /// Validated canonical event name.
        event_name: RuntimeEventName,
    },
    /// A non-duplicate transition was attempted after terminalization.
    #[error("runtime kernel state {state:?} is terminal")]
    TerminalState {
        /// Closed state.
        state: KernelState,
    },
    /// The fixed cursor bound would be exceeded.
    #[error("runtime kernel event cursor bound is exhausted")]
    EventCursorLimit,
    /// The durable compare-and-swap revision cannot advance.
    #[error("runtime kernel revision space is exhausted")]
    RevisionExhausted,
    /// The durable journal head no longer matches the prepared CAS revision.
    #[error("runtime kernel revision conflict: expected {expected}, observed {actual}")]
    #[cfg(test)]
    RevisionConflict {
        /// Revision observed when the transition prepared.
        expected: u64,
        /// Current durable journal-head revision.
        actual: u64,
    },
    /// The idempotency key was empty, oversized, or contained unsafe bytes.
    #[error("runtime kernel idempotency key is invalid")]
    InvalidIdempotencyKey,
    /// Canonical request serialization failed.
    #[error("runtime kernel request digest could not be computed")]
    RequestDigest,
    /// A deserialized snapshot violated its durable invariants.
    #[error("runtime kernel snapshot is invalid")]
    InvalidSnapshot,
    /// A durable transition outcome contradicted its snapshots.
    #[error("runtime kernel transition outcome is invalid")]
    InvalidOutcome,
    /// A prepared transition record was incomplete or tampered.
    #[error("prepared runtime kernel transition is invalid")]
    InvalidPreparedTransition,
}

fn phase_authorizes(transition: KernelTransition, phase: RuntimeErrorPhase) -> bool {
    match transition {
        KernelTransition::BeginRuntimeSelection => phase == RuntimeErrorPhase::Admission,
        KernelTransition::BeginContextAssembly => phase == RuntimeErrorPhase::RuntimeSelection,
        KernelTransition::BeginProviderCall => phase == RuntimeErrorPhase::ProviderCall,
        KernelTransition::BeginToolGate => phase == RuntimeErrorPhase::ToolValidation,
        KernelTransition::BeginApprovalWait | KernelTransition::ResumeToolGate => {
            phase == RuntimeErrorPhase::Approval
        }
        KernelTransition::BeginToolExecution => phase == RuntimeErrorPhase::ToolExecution,
        KernelTransition::ResolveToolWithoutExecution => phase == RuntimeErrorPhase::ToolGate,
        KernelTransition::BeginResultProjection => {
            matches!(phase, RuntimeErrorPhase::ResultProjection | RuntimeErrorPhase::Recovery)
        }
        KernelTransition::BeginCompaction => phase == RuntimeErrorPhase::ProviderFinalization,
        KernelTransition::BeginFinalization => {
            matches!(
                phase,
                RuntimeErrorPhase::ProviderFinalization | RuntimeErrorPhase::Finalization
            )
        }
        KernelTransition::BeginDeliveryWait => phase == RuntimeErrorPhase::DeliveryIntent,
        KernelTransition::Complete | KernelTransition::Fail => {
            phase == RuntimeErrorPhase::Finalization
        }
        KernelTransition::Cancel => phase == RuntimeErrorPhase::Cancellation,
        KernelTransition::Suspend => phase == RuntimeErrorPhase::Queueing,
        KernelTransition::BeginRecovery => {
            matches!(
                phase,
                RuntimeErrorPhase::Recovery
                    | RuntimeErrorPhase::ToolExecution
                    | RuntimeErrorPhase::ProviderFinalization
            )
        }
        KernelTransition::ResumeRuntimeSelection => phase == RuntimeErrorPhase::Finalization,
    }
}

fn edge_is_allowed(
    state: KernelState,
    transition: KernelTransition,
    event_name: RuntimeEventName,
) -> bool {
    matches!((state, transition, event_name),
        (
            KernelState::Admitted,
            KernelTransition::BeginRuntimeSelection,
            RuntimeEventName::RunStarted,
        )
        | (
            KernelState::SelectingRuntime,
            KernelTransition::BeginContextAssembly,
            RuntimeEventName::HarnessAttemptStarted,
        )
        | (
            KernelState::AssemblingContext
            | KernelState::ProjectingResult
            | KernelState::Compacting,
            KernelTransition::BeginProviderCall,
            RuntimeEventName::ProviderAttemptStarted,
        )
        | (
            KernelState::CallingProvider,
            KernelTransition::BeginToolGate,
            RuntimeEventName::ToolProposed,
        )
        | (
            KernelState::AwaitingToolGate,
            KernelTransition::BeginApprovalWait,
            RuntimeEventName::ApprovalRequired,
        )
        | (
            KernelState::AwaitingApproval,
            KernelTransition::ResumeToolGate,
            RuntimeEventName::ApprovalResolved,
        )
        | (
            KernelState::AwaitingToolGate,
            KernelTransition::BeginToolExecution,
            RuntimeEventName::ToolIntentRecorded,
        )
        | (
            KernelState::AwaitingToolGate,
            KernelTransition::ResolveToolWithoutExecution,
            RuntimeEventName::ToolDecisionRecorded,
        )
        | (
            KernelState::ExecutingTool,
            KernelTransition::BeginResultProjection,
            RuntimeEventName::ToolResultObserved,
        )
        | (
            KernelState::RecoveryPending,
            KernelTransition::BeginResultProjection,
            RuntimeEventName::ToolEffectReconciled,
        )
        | (
            KernelState::CallingProvider,
            KernelTransition::BeginRecovery,
            RuntimeEventName::ProviderAttemptCompleted,
        )
        | (
            KernelState::RecoveryPending,
            KernelTransition::BeginProviderCall,
            RuntimeEventName::ProviderAttemptStarted,
        )
        | (
            KernelState::CallingProvider,
            KernelTransition::BeginCompaction,
            RuntimeEventName::ProviderAttemptCompleted,
        )
        | (
            KernelState::CallingProvider | KernelState::ProjectingResult,
            KernelTransition::BeginFinalization,
            RuntimeEventName::ProviderAttemptCompleted,
        )
        | (
            KernelState::AssemblingContext
            | KernelState::CallingProvider
            | KernelState::AwaitingToolGate
            | KernelState::AwaitingApproval
            | KernelState::ExecutingTool
            | KernelState::ProjectingResult
            | KernelState::Compacting,
            KernelTransition::BeginFinalization,
            RuntimeEventName::FinalizationStarted,
        )
        | (
            KernelState::Finalizing,
            KernelTransition::BeginDeliveryWait,
            RuntimeEventName::DeliveryIntentRecorded,
        )
        | (
            KernelState::AwaitingDelivery,
            KernelTransition::Complete,
            RuntimeEventName::RunCompleted,
        )
        | (KernelState::Finalizing, KernelTransition::Complete, RuntimeEventName::RunCompleted)
        | (
            KernelState::SelectingRuntime
            | KernelState::AssemblingContext
            | KernelState::CallingProvider
            | KernelState::AwaitingToolGate
            | KernelState::AwaitingApproval
            | KernelState::ExecutingTool
            | KernelState::ProjectingResult
            | KernelState::Compacting
            | KernelState::Finalizing
            | KernelState::AwaitingDelivery,
            KernelTransition::Suspend,
            RuntimeEventName::BackpressureApplied,
        )
        | (
            KernelState::Suspended,
            KernelTransition::BeginRecovery,
            RuntimeEventName::CleanupPartial | RuntimeEventName::CleanupUnknown,
        )
        | (
            KernelState::ExecutingTool,
            KernelTransition::BeginRecovery,
            RuntimeEventName::ToolEffectUnknown,
        )
        | (
            KernelState::RecoveryPending,
            KernelTransition::ResumeRuntimeSelection,
            RuntimeEventName::CleanupCompleted,
        )
        |
        (
            KernelState::Admitted
            | KernelState::SelectingRuntime
            | KernelState::AssemblingContext
            | KernelState::CallingProvider
            | KernelState::AwaitingToolGate
            | KernelState::AwaitingApproval
            | KernelState::ExecutingTool
            | KernelState::ProjectingResult
            | KernelState::Compacting
            | KernelState::Finalizing
            | KernelState::AwaitingDelivery
            | KernelState::Suspended
            | KernelState::RecoveryPending,
            KernelTransition::Fail,
            RuntimeEventName::RunFailed,
        )
        | (
            KernelState::Admitted
            | KernelState::SelectingRuntime
            | KernelState::AssemblingContext
            | KernelState::CallingProvider
            | KernelState::AwaitingToolGate
            | KernelState::AwaitingApproval
            | KernelState::ExecutingTool
            | KernelState::ProjectingResult
            | KernelState::Compacting
            | KernelState::Finalizing
            | KernelState::AwaitingDelivery
            | KernelState::Suspended
            | KernelState::RecoveryPending,
            KernelTransition::Cancel,
            RuntimeEventName::RunCancelled,
        )
    )
}
