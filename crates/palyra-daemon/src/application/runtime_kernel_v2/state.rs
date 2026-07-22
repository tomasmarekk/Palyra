// Durable runtime-kernel state and authority contracts.

/// Runtime contract selected atomically for one run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RuntimeKernelVersion {
    /// Existing orchestration remains authoritative.
    Legacy,
    /// V2 evaluates transitions without owning their effects.
    V2Shadow,
    /// V2 owns explicitly selected canary runs.
    V2Canary,
    /// V2 is authoritative for the run.
    V2,
}

/// Declarative timeout ownership for a kernel state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum KernelTimeoutPosture {
    /// The state advances only after an explicit host decision.
    None,
    /// The host must enforce the declared finite residence budget.
    Bounded {
        /// Maximum residence budget in milliseconds.
        budget_ms: u64,
    },
    /// The enclosing run deadline controls expiration.
    HostDeadline,
}

/// Stable metadata attached to one kernel state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct KernelStateDescriptor {
    reason_code: &'static str,
    timeout_posture: KernelTimeoutPosture,
}

impl KernelStateDescriptor {
    /// Returns the stable machine-readable state reason.
    #[must_use]
    pub(crate) const fn reason_code(self) -> &'static str {
        self.reason_code
    }

    /// Returns the host-owned timeout policy for the state.
    #[must_use]
    pub(crate) const fn timeout_posture(self) -> KernelTimeoutPosture {
        self.timeout_posture
    }
}

/// Authoritative orchestration state for one immutable Run generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum KernelState {
    /// The run passed the admission boundary.
    Admitted,
    /// The host is selecting an execution runtime.
    SelectingRuntime,
    /// Provider-facing context is being assembled.
    AssemblingContext,
    /// A provider attempt is active.
    CallingProvider,
    /// A tool proposal is waiting for policy and authority checks.
    AwaitingToolGate,
    /// An exact tool proposal is waiting for operator approval.
    AwaitingApproval,
    /// An authorized tool operation is active.
    ExecutingTool,
    /// A tool result is being projected into the turn.
    ProjectingResult,
    /// Context is being compacted before another provider attempt.
    Compacting,
    /// Output and terminal metadata are being finalized.
    Finalizing,
    /// A durable delivery intent is awaiting downstream handling.
    AwaitingDelivery,
    /// The run generation completed successfully.
    Done,
    /// The run generation ended in failure.
    Failed,
    /// The run generation was cancelled.
    Cancelled,
    /// Progress is intentionally paused without terminalization.
    Suspended,
    /// Recovery evidence or an explicit recovery decision is pending.
    RecoveryPending,
}

impl KernelState {
    /// Lists every state in stable registry order.
    #[cfg(test)]
    pub(crate) const ALL: &'static [Self] = &[
        Self::Admitted,
        Self::SelectingRuntime,
        Self::AssemblingContext,
        Self::CallingProvider,
        Self::AwaitingToolGate,
        Self::AwaitingApproval,
        Self::ExecutingTool,
        Self::ProjectingResult,
        Self::Compacting,
        Self::Finalizing,
        Self::AwaitingDelivery,
        Self::Done,
        Self::Failed,
        Self::Cancelled,
        Self::Suspended,
        Self::RecoveryPending,
    ];

    /// Returns the stable reason and timeout contract for this state.
    #[must_use]
    pub(crate) const fn descriptor(self) -> KernelStateDescriptor {
        match self {
            Self::Admitted => {
                state_descriptor("runtime.kernel.admitted", KernelTimeoutPosture::HostDeadline)
            }
            Self::SelectingRuntime => state_descriptor(
                "runtime.kernel.selecting_runtime",
                KernelTimeoutPosture::Bounded { budget_ms: 30_000 },
            ),
            Self::AssemblingContext => state_descriptor(
                "runtime.kernel.assembling_context",
                KernelTimeoutPosture::Bounded { budget_ms: 120_000 },
            ),
            Self::CallingProvider => state_descriptor(
                "runtime.kernel.calling_provider",
                KernelTimeoutPosture::HostDeadline,
            ),
            Self::AwaitingToolGate => state_descriptor(
                "runtime.kernel.awaiting_tool_gate",
                KernelTimeoutPosture::Bounded { budget_ms: 30_000 },
            ),
            Self::AwaitingApproval => {
                state_descriptor("runtime.kernel.awaiting_approval", KernelTimeoutPosture::None)
            }
            Self::ExecutingTool => state_descriptor(
                "runtime.kernel.executing_tool",
                KernelTimeoutPosture::HostDeadline,
            ),
            Self::ProjectingResult => state_descriptor(
                "runtime.kernel.projecting_result",
                KernelTimeoutPosture::Bounded { budget_ms: 30_000 },
            ),
            Self::Compacting => state_descriptor(
                "runtime.kernel.compacting",
                KernelTimeoutPosture::Bounded { budget_ms: 120_000 },
            ),
            Self::Finalizing => state_descriptor(
                "runtime.kernel.finalizing",
                KernelTimeoutPosture::Bounded { budget_ms: 30_000 },
            ),
            Self::AwaitingDelivery => state_descriptor(
                "runtime.kernel.awaiting_delivery",
                KernelTimeoutPosture::HostDeadline,
            ),
            Self::Done => state_descriptor("runtime.kernel.done", KernelTimeoutPosture::None),
            Self::Failed => state_descriptor("runtime.kernel.failed", KernelTimeoutPosture::None),
            Self::Cancelled => {
                state_descriptor("runtime.kernel.cancelled", KernelTimeoutPosture::None)
            }
            Self::Suspended => {
                state_descriptor("runtime.kernel.suspended", KernelTimeoutPosture::None)
            }
            Self::RecoveryPending => state_descriptor(
                "runtime.kernel.recovery_pending",
                KernelTimeoutPosture::HostDeadline,
            ),
        }
    }

    /// Returns whether the Run generation has reserved its sole terminal outcome.
    #[must_use]
    pub(crate) const fn is_terminal(self) -> bool {
        matches!(self, Self::Done | Self::Failed | Self::Cancelled)
    }
}

const fn state_descriptor(
    reason_code: &'static str,
    timeout_posture: KernelTimeoutPosture,
) -> KernelStateDescriptor {
    KernelStateDescriptor { reason_code, timeout_posture }
}

/// Terminal classification reserved exactly once by a kernel snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum KernelTerminalOutcome {
    /// Successful completion.
    Done,
    /// Terminal failure.
    Failed,
    /// Operator or host cancellation.
    Cancelled,
}

impl KernelTerminalOutcome {
    const fn from_state(state: KernelState) -> Option<Self> {
        match state {
            KernelState::Done => Some(Self::Done),
            KernelState::Failed => Some(Self::Failed),
            KernelState::Cancelled => Some(Self::Cancelled),
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
            | KernelState::RecoveryPending => None,
        }
    }
}

/// Validated active generation leases presented to one transition evaluation.
///
/// The complete set is intentionally not retained by the kernel snapshot. The
/// host and journal remain authoritative for current lane ownership at
/// evaluation time; the admitted Run lease is retained separately.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) struct KernelLaneAuthoritySet {
    leases: Vec<GenerationLeaseV1>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct KernelLaneAuthoritySetWire {
    #[serde(deserialize_with = "deserialize_lane_leases")]
    leases: Vec<GenerationLeaseV1>,
}

impl<'de> Deserialize<'de> for KernelLaneAuthoritySet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = KernelLaneAuthoritySetWire::deserialize(deserializer)?;
        let authorities = Self { leases: wire.leases };
        authorities.validate_structure().map_err(D::Error::custom)?;
        Ok(authorities)
    }
}

impl KernelLaneAuthoritySet {
    /// Creates a bounded lane-authority set bound to the kernel's base run identity.
    ///
    /// # Errors
    /// Returns [`KernelTransitionError::InvalidLaneAuthority`] for malformed,
    /// duplicate, cross-session, or cross-run leases.
    pub(crate) fn new(
        base_identities: &RuntimeIdentitySetV1,
        leases: Vec<GenerationLeaseV1>,
    ) -> Result<Self, KernelTransitionError> {
        let authorities = Self { leases };
        authorities.validate(base_identities)?;
        Ok(authorities)
    }

    /// Validates lease structure and exact session/run ownership.
    ///
    /// This method does not inspect wall-clock expiry; the host performs that
    /// check before supplying its current authority snapshot.
    ///
    /// # Errors
    /// Returns [`KernelTransitionError::InvalidLaneAuthority`] when the set is
    /// empty, oversized, duplicated, malformed, or bound to another run.
    pub(crate) fn validate(
        &self,
        base_identities: &RuntimeIdentitySetV1,
    ) -> Result<(), KernelTransitionError> {
        self.validate_structure()?;
        let mut lanes = BTreeSet::new();
        for lease in &self.leases {
            if !lanes.insert(lease.lane)
                || lease.session_id != base_identities.session_id
                || lease.run_id.as_ref() != Some(&base_identities.run_id)
            {
                return Err(KernelTransitionError::InvalidLaneAuthority);
            }
        }
        let run_generation = self
            .generation(RuntimeGenerationLane::Run)
            .ok_or(KernelTransitionError::InvalidLaneAuthority)?;
        if run_generation != base_identities.generation {
            return Err(KernelTransitionError::InvalidLaneAuthority);
        }
        Ok(())
    }

    fn validate_structure(&self) -> Result<(), KernelTransitionError> {
        if self.leases.is_empty()
            || self.leases.len() > RuntimeGenerationLane::wire_contract_values().len()
        {
            return Err(KernelTransitionError::InvalidLaneAuthority);
        }
        let mut lanes = BTreeSet::new();
        for lease in &self.leases {
            lease.validate().map_err(|_| KernelTransitionError::InvalidLaneAuthority)?;
            if !lanes.insert(lease.lane) {
                return Err(KernelTransitionError::InvalidLaneAuthority);
            }
        }
        Ok(())
    }

    fn generation(&self, lane: RuntimeGenerationLane) -> Option<RuntimeGeneration> {
        self.leases.iter().find(|lease| lease.lane == lane).map(|lease| lease.generation)
    }

    /// Returns the exact host-issued leases that must still be active at journal commit.
    #[must_use]
    pub(crate) fn leases(&self) -> &[GenerationLeaseV1] {
        self.leases.as_slice()
    }

    #[cfg(test)]
    pub(super) fn lease_mut_for_test(
        &mut self,
        lane: RuntimeGenerationLane,
    ) -> Option<&mut GenerationLeaseV1> {
        self.leases.iter_mut().find(|lease| lease.lane == lane)
    }

    /// Returns the validated Run-lane lease for the exact base identity.
    ///
    /// # Errors
    /// Returns [`KernelTransitionError::InvalidLaneAuthority`] when the set or
    /// its Run lease is malformed or bound to another run generation.
    pub(crate) fn run_lease(
        &self,
        base_identities: &RuntimeIdentitySetV1,
    ) -> Result<&GenerationLeaseV1, KernelTransitionError> {
        self.validate(base_identities)?;
        self.leases
            .iter()
            .find(|lease| lease.lane == RuntimeGenerationLane::Run)
            .ok_or(KernelTransitionError::InvalidLaneAuthority)
    }

    fn validate_admitted_run_lease(
        &self,
        base_identities: &RuntimeIdentitySetV1,
        admitted_run_lease: &GenerationLeaseV1,
    ) -> Result<(), KernelTransitionError> {
        if self.run_lease(base_identities)? == admitted_run_lease {
            Ok(())
        } else {
            Err(KernelTransitionError::RunLeaseMismatch)
        }
    }
}

/// Last accepted event cursor for the active generation of one lane.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct KernelEventCursor {
    lane: RuntimeGenerationLane,
    generation: RuntimeGeneration,
    last_sequence: u64,
    last_event_id: RuntimeEventId,
    last_event_sha256: String,
}

/// Bounded child identities that prove tool and delivery correlation.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct KernelChildCorrelationState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    active_attempt_id: Option<palyra_common::runtime_contracts::RuntimeAttemptId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tool_proposal_id: Option<RuntimeToolProposalId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    approval_subject_id: Option<RuntimeApprovalSubjectId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tool_execution_id: Option<RuntimeToolExecutionId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tool_operation_id: Option<RuntimeOperationId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    delivery_intent_id: Option<RuntimeDeliveryIntentId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    delivery_operation_id: Option<RuntimeOperationId>,
}

impl KernelChildCorrelationState {
    fn validate(&self) -> Result<(), KernelTransitionError> {
        if self.approval_subject_id.is_some() && self.tool_proposal_id.is_none() {
            return Err(KernelTransitionError::InvalidSnapshot);
        }
        if self.tool_execution_id.is_some() != self.tool_operation_id.is_some()
            || self.tool_execution_id.is_some() && self.tool_proposal_id.is_none()
            || self.delivery_intent_id.is_some() != self.delivery_operation_id.is_some()
        {
            return Err(KernelTransitionError::InvalidSnapshot);
        }
        Ok(())
    }
}

/// Durable evidence for the transition that produced a snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct KernelTransitionEvidence {
    previous_revision: u64,
    previous_state: KernelState,
    previous_correlations: KernelChildCorrelationState,
    transition: KernelTransition,
    transition_reason_code: String,
    idempotency_key: String,
    request_sha256: String,
    event_sha256: String,
    lane_authority: KernelLaneAuthoritySet,
    event: RuntimeEventEnvelopeV2,
}

/// Validated serializable state for one immutable Run generation.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) struct KernelStateSnapshot {
    schema_version: u32,
    revision: u64,
    version: RuntimeKernelVersion,
    runtime_authority_decision: RuntimeAuthorityDecisionV1,
    run_lease: GenerationLeaseV1,
    admitted_at_unix_ms: i64,
    admission_sha256: String,
    state: KernelState,
    base_identities: RuntimeIdentitySetV1,
    reason_code: String,
    timeout_posture: KernelTimeoutPosture,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    event_cursors: Vec<KernelEventCursor>,
    correlations: KernelChildCorrelationState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_transition: Option<KernelTransitionEvidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    terminal_outcome: Option<KernelTerminalOutcome>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct KernelStateSnapshotWire {
    schema_version: u32,
    revision: u64,
    version: RuntimeKernelVersion,
    runtime_authority_decision: RuntimeAuthorityDecisionV1,
    run_lease: GenerationLeaseV1,
    admitted_at_unix_ms: i64,
    admission_sha256: String,
    state: KernelState,
    base_identities: RuntimeIdentitySetV1,
    reason_code: String,
    timeout_posture: KernelTimeoutPosture,
    #[serde(default, deserialize_with = "deserialize_event_cursors")]
    event_cursors: Vec<KernelEventCursor>,
    correlations: KernelChildCorrelationState,
    #[serde(default)]
    last_transition: Option<KernelTransitionEvidence>,
    #[serde(default)]
    terminal_outcome: Option<KernelTerminalOutcome>,
}

impl<'de> Deserialize<'de> for KernelStateSnapshot {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = KernelStateSnapshotWire::deserialize(deserializer)?;
        let snapshot = Self {
            schema_version: wire.schema_version,
            revision: wire.revision,
            version: wire.version,
            runtime_authority_decision: wire.runtime_authority_decision,
            run_lease: wire.run_lease,
            admitted_at_unix_ms: wire.admitted_at_unix_ms,
            admission_sha256: wire.admission_sha256,
            state: wire.state,
            base_identities: wire.base_identities,
            reason_code: wire.reason_code,
            timeout_posture: wire.timeout_posture,
            event_cursors: wire.event_cursors,
            correlations: wire.correlations,
            last_transition: wire.last_transition,
            terminal_outcome: wire.terminal_outcome,
        };
        snapshot.validate().map_err(D::Error::custom)?;
        Ok(snapshot)
    }
}

impl KernelStateSnapshot {
    fn initial(
        runtime_authority_decision: RuntimeAuthorityDecisionV1,
        base_identities: RuntimeIdentitySetV1,
        run_lease: GenerationLeaseV1,
        admitted_at_unix_ms: i64,
    ) -> Result<Self, KernelTransitionError> {
        validate_base_identities(&base_identities)?;
        let version = admitted_kernel_version(&runtime_authority_decision)?;
        validate_admission_binding(
            &runtime_authority_decision,
            &base_identities,
            &run_lease,
            admitted_at_unix_ms,
        )?;
        let admission_sha256 = admission_sha256(
            &runtime_authority_decision,
            &base_identities,
            &run_lease,
            admitted_at_unix_ms,
        )?;
        let state = KernelState::Admitted;
        let descriptor = state.descriptor();
        Ok(Self {
            schema_version: KERNEL_STATE_SNAPSHOT_SCHEMA_VERSION,
            revision: 0,
            version,
            runtime_authority_decision,
            run_lease,
            admitted_at_unix_ms,
            admission_sha256,
            state,
            base_identities,
            reason_code: descriptor.reason_code().to_owned(),
            timeout_posture: descriptor.timeout_posture(),
            event_cursors: Vec::new(),
            correlations: KernelChildCorrelationState::default(),
            last_transition: None,
            terminal_outcome: None,
        })
    }

    /// Returns the selected runtime contract version.
    #[must_use]
    pub(crate) const fn version(&self) -> RuntimeKernelVersion {
        self.version
    }

    /// Returns the persisted authority decision consumed at admission.
    #[must_use]
    pub(crate) const fn runtime_authority_decision(&self) -> &RuntimeAuthorityDecisionV1 {
        &self.runtime_authority_decision
    }

    /// Returns the exact Run lease that admitted this immutable generation.
    #[must_use]
    pub(crate) const fn run_lease(&self) -> &GenerationLeaseV1 {
        &self.run_lease
    }

    /// Returns the monotonic compare-and-swap revision owned by the journal.
    #[must_use]
    pub(crate) const fn revision(&self) -> u64 {
        self.revision
    }

    /// Returns the current orchestration state.
    #[must_use]
    pub(crate) const fn state(&self) -> KernelState {
        self.state
    }

    /// Returns the immutable Run generation.
    #[must_use]
    pub(crate) const fn run_generation(&self) -> RuntimeGeneration {
        self.base_identities.generation
    }

    /// Returns the validated trace/session/run identity binding.
    #[must_use]
    pub(crate) fn base_identities(&self) -> &RuntimeIdentitySetV1 {
        &self.base_identities
    }

    /// Returns the stable reason code for the current state.
    #[must_use]
    #[cfg(test)]
    pub(crate) fn reason_code(&self) -> &str {
        self.reason_code.as_str()
    }

    /// Returns the timeout posture for the current state.
    #[must_use]
    #[cfg(test)]
    pub(crate) const fn timeout_posture(&self) -> KernelTimeoutPosture {
        self.timeout_posture
    }

    /// Returns the terminal classification when this generation is closed.
    #[must_use]
    pub(crate) const fn terminal_outcome(&self) -> Option<KernelTerminalOutcome> {
        self.terminal_outcome
    }

    /// Validates all durable cross-field snapshot invariants.
    ///
    /// # Errors
    /// Returns [`KernelTransitionError::InvalidSnapshot`] when deserialized
    /// state, identity, cursor, correlation, transition, or terminal evidence
    /// does not form one coherent Run generation.
    pub(crate) fn validate(&self) -> Result<(), KernelTransitionError> {
        if self.schema_version != KERNEL_STATE_SNAPSHOT_SCHEMA_VERSION {
            return Err(KernelTransitionError::InvalidSnapshot);
        }
        validate_base_identities(&self.base_identities)
            .map_err(|_| KernelTransitionError::InvalidSnapshot)?;
        let derived_version = admitted_kernel_version(&self.runtime_authority_decision)
            .map_err(|_| KernelTransitionError::InvalidSnapshot)?;
        validate_admission_binding(
            &self.runtime_authority_decision,
            &self.base_identities,
            &self.run_lease,
            self.admitted_at_unix_ms,
        )
        .map_err(|_| KernelTransitionError::InvalidSnapshot)?;
        let expected_admission_sha256 = admission_sha256(
            &self.runtime_authority_decision,
            &self.base_identities,
            &self.run_lease,
            self.admitted_at_unix_ms,
        )
        .map_err(|_| KernelTransitionError::InvalidSnapshot)?;
        let descriptor = self.state.descriptor();
        if self.version != derived_version
            || self.admission_sha256 != expected_admission_sha256
            || self.reason_code != descriptor.reason_code()
            || self.timeout_posture != descriptor.timeout_posture()
            || self.event_cursors.len() > MAX_KERNEL_EVENT_CURSORS
        {
            return Err(KernelTransitionError::InvalidSnapshot);
        }
        self.correlations.validate()?;
        validate_cursors(&self.event_cursors)?;
        if self.terminal_outcome != KernelTerminalOutcome::from_state(self.state) {
            return Err(KernelTransitionError::InvalidSnapshot);
        }

        let Some(evidence) = &self.last_transition else {
            if self.revision == 0
                && self.state == KernelState::Admitted
                && self.event_cursors.is_empty()
                && self.correlations == KernelChildCorrelationState::default()
                && self.terminal_outcome.is_none()
            {
                return Ok(());
            }
            return Err(KernelTransitionError::InvalidSnapshot);
        };
        validate_transition_evidence(self, evidence)
    }

    fn cursor(
        &self,
        lane: RuntimeGenerationLane,
        generation: RuntimeGeneration,
    ) -> Option<&KernelEventCursor> {
        self.event_cursors
            .iter()
            .find(|cursor| cursor.lane == lane && cursor.generation == generation)
    }

    #[cfg(test)]
    pub(super) fn event_cursor_count_for_test(&self) -> usize {
        self.event_cursors.len()
    }

    #[cfg(test)]
    pub(super) fn cursor_sequence_for_test(
        &self,
        lane: RuntimeGenerationLane,
        generation: RuntimeGeneration,
    ) -> Option<u64> {
        self.cursor(lane, generation).map(|cursor| cursor.last_sequence)
    }
}

fn admitted_kernel_version(
    decision: &RuntimeAuthorityDecisionV1,
) -> Result<RuntimeKernelVersion, KernelTransitionError> {
    decision.validate().map_err(|_| KernelTransitionError::InvalidRuntimeAuthorityDecision)?;
    match (decision.reason(), decision.selected_runtime(), decision.shadow_evaluation_enabled()) {
        (RuntimeAuthorityReason::V2ShadowLegacyAuthority, Some(RuntimeAuthority::Legacy), true) => {
            Ok(RuntimeKernelVersion::V2Shadow)
        }
        (RuntimeAuthorityReason::V2CanarySessionSelected, Some(RuntimeAuthority::V2), false) => {
            Ok(RuntimeKernelVersion::V2Canary)
        }
        (RuntimeAuthorityReason::V2ProfileSelected, Some(RuntimeAuthority::V2), false) => {
            Ok(RuntimeKernelVersion::V2)
        }
        _ => Err(KernelTransitionError::InvalidRuntimeAuthorityDecision),
    }
}

fn validate_admission_binding(
    decision: &RuntimeAuthorityDecisionV1,
    base_identities: &RuntimeIdentitySetV1,
    run_lease: &GenerationLeaseV1,
    admitted_at_unix_ms: i64,
) -> Result<(), KernelTransitionError> {
    if decision.generation() != base_identities.generation {
        return Err(KernelTransitionError::RuntimeAuthorityGenerationMismatch {
            active: base_identities.generation,
            observed: decision.generation(),
        });
    }
    run_lease.validate().map_err(|_| KernelTransitionError::InvalidRunLease)?;
    if run_lease.lane != RuntimeGenerationLane::Run
        || run_lease.session_id != base_identities.session_id
        || run_lease.run_id.as_ref() != Some(&base_identities.run_id)
        || run_lease.generation != base_identities.generation
    {
        return Err(KernelTransitionError::InvalidRunLease);
    }
    if admitted_at_unix_ms < run_lease.acquired_at_unix_ms
        || admitted_at_unix_ms >= run_lease.expires_at_unix_ms
    {
        return Err(KernelTransitionError::InactiveRunLease);
    }
    Ok(())
}

#[derive(Serialize)]
struct KernelAdmissionDigestMaterial<'a> {
    schema_version: u32,
    runtime_authority_decision: &'a RuntimeAuthorityDecisionV1,
    base_identities: &'a RuntimeIdentitySetV1,
    run_lease: &'a GenerationLeaseV1,
    admitted_at_unix_ms: i64,
}

fn admission_sha256(
    runtime_authority_decision: &RuntimeAuthorityDecisionV1,
    base_identities: &RuntimeIdentitySetV1,
    run_lease: &GenerationLeaseV1,
    admitted_at_unix_ms: i64,
) -> Result<String, KernelTransitionError> {
    let material = KernelAdmissionDigestMaterial {
        schema_version: KERNEL_STATE_SNAPSHOT_SCHEMA_VERSION,
        runtime_authority_decision,
        base_identities,
        run_lease,
        admitted_at_unix_ms,
    };
    digest_serializable(b"palyra.runtime.kernel.admission.v1\0", &material)
}

fn validate_base_identities(
    identities: &RuntimeIdentitySetV1,
) -> Result<(), KernelTransitionError> {
    identities.validate().map_err(|_| KernelTransitionError::InvalidBaseIdentities)?;
    if identities.attempt_id.is_some()
        || identities.tool_proposal_id.is_some()
        || identities.tool_execution_id.is_some()
        || identities.approval_subject_id.is_some()
        || identities.delivery_intent_id.is_some()
        || identities.plugin_call_id.is_some()
        || identities.context_projection_id.is_some()
        || identities.recovery_action_id.is_some()
        || identities.operation_id.is_some()
        || identities.runtime_instance_id.is_some()
    {
        return Err(KernelTransitionError::InvalidBaseIdentities);
    }
    Ok(())
}

fn validate_event_base_identity(
    base: &RuntimeIdentitySetV1,
    event: &RuntimeEventEnvelopeV2,
) -> Result<(), KernelTransitionError> {
    if event.identities.trace_id != base.trace_id
        || event.identities.session_id != base.session_id
        || event.identities.run_id != base.run_id
    {
        return Err(KernelTransitionError::IdentityMismatch);
    }
    Ok(())
}

fn validate_event_lane_authority(
    authorities: &KernelLaneAuthoritySet,
    event: &RuntimeEventEnvelopeV2,
) -> Result<(), KernelTransitionError> {
    let lane = event.event_name.descriptor().generation_lane;
    let active =
        authorities.generation(lane).ok_or(KernelTransitionError::MissingLaneAuthority { lane })?;
    if event.identities.generation != active {
        return Err(KernelTransitionError::LaneGenerationMismatch {
            lane,
            active,
            observed: event.identities.generation,
        });
    }
    Ok(())
}

fn validate_cursors(cursors: &[KernelEventCursor]) -> Result<(), KernelTransitionError> {
    let mut keys = BTreeSet::new();
    for cursor in cursors {
        if !keys.insert(cursor.lane) || !is_sha256(cursor.last_event_sha256.as_str()) {
            return Err(KernelTransitionError::InvalidSnapshot);
        }
    }
    Ok(())
}

fn validate_transition_evidence(
    snapshot: &KernelStateSnapshot,
    evidence: &KernelTransitionEvidence,
) -> Result<(), KernelTransitionError> {
    evidence.event.validate().map_err(|_| KernelTransitionError::InvalidSnapshot)?;
    evidence
        .lane_authority
        .validate(&snapshot.base_identities)
        .map_err(|_| KernelTransitionError::InvalidSnapshot)?;
    evidence
        .lane_authority
        .validate_admitted_run_lease(&snapshot.base_identities, &snapshot.run_lease)
        .map_err(|_| KernelTransitionError::InvalidSnapshot)?;
    validate_event_lane_authority(&evidence.lane_authority, &evidence.event)
        .map_err(|_| KernelTransitionError::InvalidSnapshot)?;
    validate_event_base_identity(&snapshot.base_identities, &evidence.event)
        .map_err(|_| KernelTransitionError::InvalidSnapshot)?;
    if evidence.transition.target_state() != snapshot.state
        || evidence.previous_revision.checked_add(1) != Some(snapshot.revision)
        || evidence.transition_reason_code != evidence.transition.reason_code()
        || !is_idempotency_key(evidence.idempotency_key.as_str())
        || !is_sha256(evidence.request_sha256.as_str())
        || !is_sha256(evidence.event_sha256.as_str())
        || event_sha256(&evidence.event).ok().as_deref() != Some(evidence.event_sha256.as_str())
        || request_sha256(evidence.idempotency_key.as_str(), &evidence.event, evidence.transition)
            .ok()
            .as_deref()
            != Some(evidence.request_sha256.as_str())
        || !phase_authorizes(evidence.transition, evidence.event.phase)
        || !edge_is_allowed(evidence.previous_state, evidence.transition, evidence.event.event_name)
    {
        return Err(KernelTransitionError::InvalidSnapshot);
    }
    let expected_correlations = derive_correlations(
        evidence.previous_state,
        &evidence.previous_correlations,
        &evidence.event,
        evidence.transition,
    )
    .map_err(|_| KernelTransitionError::InvalidSnapshot)?;
    if expected_correlations != snapshot.correlations {
        return Err(KernelTransitionError::InvalidSnapshot);
    }
    let lane = evidence.event.event_name.descriptor().generation_lane;
    let cursor = snapshot
        .cursor(lane, evidence.event.identities.generation)
        .ok_or(KernelTransitionError::InvalidSnapshot)?;
    if cursor.last_sequence != evidence.event.sequence
        || cursor.last_event_id != evidence.event.event_id
        || cursor.last_event_sha256 != evidence.event_sha256
    {
        return Err(KernelTransitionError::InvalidSnapshot);
    }
    let expected_terminal = match evidence.event.event_name {
        RuntimeEventName::RunCompleted => Some(KernelTerminalOutcome::Done),
        RuntimeEventName::RunFailed => Some(KernelTerminalOutcome::Failed),
        RuntimeEventName::RunCancelled => Some(KernelTerminalOutcome::Cancelled),
        _ => None,
    };
    if evidence.event.terminal != snapshot.state.is_terminal()
        || expected_terminal != snapshot.terminal_outcome
    {
        return Err(KernelTransitionError::InvalidSnapshot);
    }
    Ok(())
}
