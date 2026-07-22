// Capability-safe tool gating, approval, execution, and result projection.
// Private token constructors preserve the single host authority boundary while
// the included items retain their established parent-module visibility.

/// Authority requested by a model-proposed tool call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ToolAuthorityClass {
    /// Pure or otherwise host-classified read-only call.
    ReadOnly,
    /// Durable host mutation.
    Mutation,
    /// External side effect.
    ExternalEffect,
    /// Classification failed; the gateway must deny.
    Unknown,
}

/// Canonical tool proposal passed to the sole host authority gateway.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ToolProposalRequest {
    proposal_id: RuntimeToolProposalId,
    tool_name: String,
    retained_proposal: RetainedToolProposalRef,
    requested_authority: ToolAuthorityClass,
}

impl ToolProposalRequest {
    /// Creates a proposal whose raw normalized arguments remain host-retained.
    ///
    /// # Errors
    /// Returns [`KernelPhaseContractError::InvalidToolName`] for an empty or
    /// oversized catalog name.
    pub(crate) fn new(
        proposal_id: RuntimeToolProposalId,
        tool_name: String,
        retained_proposal: RetainedToolProposalRef,
        requested_authority: ToolAuthorityClass,
    ) -> Result<Self, KernelPhaseContractError> {
        if tool_name.trim().is_empty() || tool_name.len() > MAX_TOOL_NAME_BYTES {
            return Err(KernelPhaseContractError::InvalidToolName);
        }
        Ok(Self { proposal_id, tool_name, retained_proposal, requested_authority })
    }

    /// Returns the typed proposal identity.
    #[must_use]
    pub(crate) const fn proposal_id(&self) -> &RuntimeToolProposalId {
        &self.proposal_id
    }

    /// Returns the validated catalog name.
    #[must_use]
    pub(crate) fn tool_name(&self) -> &str {
        self.tool_name.as_str()
    }

    /// Returns the host-retained normalized proposal.
    #[must_use]
    pub(crate) const fn retained_proposal(&self) -> &RetainedToolProposalRef {
        &self.retained_proposal
    }

    /// Returns the requested authority class.
    #[must_use]
    pub(crate) const fn requested_authority(&self) -> ToolAuthorityClass {
        self.requested_authority
    }
}

/// Stable approval subject bound to one normalized proposal and authority class.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ApprovalSubjectBinding {
    approval_subject_id: RuntimeApprovalSubjectId,
    normalized_proposal_sha256: [u8; SHA256_BYTES],
    authority_class: ToolAuthorityClass,
}

impl ApprovalSubjectBinding {
    /// Creates a host-issued approval subject binding.
    fn from_host(
        approval_subject_id: RuntimeApprovalSubjectId,
        normalized_proposal_sha256: [u8; SHA256_BYTES],
        authority_class: ToolAuthorityClass,
    ) -> Self {
        Self { approval_subject_id, normalized_proposal_sha256, authority_class }
    }

    /// Returns the stable approval subject identity.
    #[must_use]
    pub(crate) const fn approval_subject_id(&self) -> &RuntimeApprovalSubjectId {
        &self.approval_subject_id
    }

    /// Returns the digest of the normalized proposal covered by approval.
    #[must_use]
    pub(crate) const fn normalized_proposal_sha256(&self) -> &[u8; SHA256_BYTES] {
        &self.normalized_proposal_sha256
    }

    /// Returns the authority class covered by approval.
    #[must_use]
    pub(crate) const fn authority_class(&self) -> ToolAuthorityClass {
        self.authority_class
    }
}

/// Non-cloneable pending authority tied to one approval subject.
#[derive(Debug)]
pub(crate) struct PendingToolAuthority {
    proposal_id: RuntimeToolProposalId,
    lane_authority: PhaseLaneAuthority,
    retained_proposal: RetainedToolProposalRef,
    authority_class: ToolAuthorityClass,
    approval_subject: ApprovalSubjectBinding,
}

impl PendingToolAuthority {
    /// Issues a pending token from inside the kernel-owned gateway adapter.
    ///
    /// # Errors
    /// Returns [`KernelPhaseContractError`] when the grant is not for the Tool
    /// lane, the authority class is unknown, or approval is not bound to the
    /// exact normalized proposal and authority class.
    fn issue(
        proposal_id: RuntimeToolProposalId,
        lane_authority: PhaseLaneAuthority,
        retained_proposal: RetainedToolProposalRef,
        authority_class: ToolAuthorityClass,
        approval_subject: ApprovalSubjectBinding,
    ) -> Result<Self, KernelPhaseContractError> {
        validate_tool_lane_authority(&lane_authority)?;
        validate_tool_authority_class(authority_class)?;
        if approval_subject.normalized_proposal_sha256() != retained_proposal.sha256()
            || approval_subject.authority_class() != authority_class
        {
            return Err(KernelPhaseContractError::ApprovalSubjectBindingMismatch);
        }
        Ok(Self {
            proposal_id,
            lane_authority,
            retained_proposal,
            authority_class,
            approval_subject,
        })
    }

    /// Returns the proposal identity covered by the pending approval.
    #[must_use]
    pub(crate) const fn proposal_id(&self) -> &RuntimeToolProposalId {
        &self.proposal_id
    }

    /// Returns the exact Run and Tool lane leases awaiting approval.
    #[must_use]
    pub(crate) const fn lane_authority(&self) -> &PhaseLaneAuthority {
        &self.lane_authority
    }

    /// Returns the immutable Run generation awaiting the decision.
    #[must_use]
    #[cfg(test)]
    pub(crate) const fn run_generation(&self) -> RuntimeGeneration {
        self.lane_authority.run_generation()
    }

    /// Returns the host-retained normalized proposal.
    #[must_use]
    pub(crate) const fn retained_proposal(&self) -> &RetainedToolProposalRef {
        &self.retained_proposal
    }

    /// Returns the authority class awaiting approval.
    #[must_use]
    pub(crate) const fn authority_class(&self) -> ToolAuthorityClass {
        self.authority_class
    }

    /// Returns the stable subject and normalized-proposal approval binding.
    #[must_use]
    pub(crate) const fn approval_subject(&self) -> &ApprovalSubjectBinding {
        &self.approval_subject
    }
}

/// Non-cloneable authority required to request tool execution.
///
/// Only an adapter inside `runtime_kernel_v2` can issue this token. Moving it
/// into [`ToolExecutionRequest`] consumes the grant, preventing the kernel from
/// accidentally dispatching the same authority twice.
#[derive(Debug)]
pub(crate) struct GrantedToolAuthority {
    proposal_id: RuntimeToolProposalId,
    execution_id: RuntimeToolExecutionId,
    lane_authority: PhaseLaneAuthority,
    retained_proposal: RetainedToolProposalRef,
    authority_class: ToolAuthorityClass,
    approval_subject: Option<ApprovalSubjectBinding>,
    grant_evidence: RedactedEvidenceRef,
}

impl GrantedToolAuthority {
    /// Issues non-interactive executable authority inside the live gateway.
    ///
    /// # Errors
    /// Returns [`KernelPhaseContractError`] unless authority is for the exact
    /// Tool lane and has a known class.
    fn issue_noninteractive(
        proposal_id: RuntimeToolProposalId,
        execution_id: RuntimeToolExecutionId,
        lane_authority: PhaseLaneAuthority,
        retained_proposal: RetainedToolProposalRef,
        authority_class: ToolAuthorityClass,
        grant_evidence: RedactedEvidenceRef,
    ) -> Result<Self, KernelPhaseContractError> {
        validate_tool_lane_authority(&lane_authority)?;
        validate_tool_authority_class(authority_class)?;
        Ok(Self {
            proposal_id,
            execution_id,
            lane_authority,
            retained_proposal,
            authority_class,
            approval_subject: None,
            grant_evidence,
        })
    }

    /// Converts one consumed pending authority into one executable grant.
    ///
    /// The conversion preserves every proposal, lane, authority-class, and
    /// approval-subject binding. The execution identity exists only after this
    /// conversion consumes the pending token.
    ///
    /// # Errors
    /// Returns [`KernelPhaseContractError`] if the retained pending authority
    /// no longer satisfies the Tool lane or authority-class invariant.
    fn from_approved_pending(
        pending: PendingToolAuthority,
        execution_id: RuntimeToolExecutionId,
        grant_evidence: RedactedEvidenceRef,
    ) -> Result<Self, KernelPhaseContractError> {
        validate_tool_lane_authority(&pending.lane_authority)?;
        validate_tool_authority_class(pending.authority_class)?;
        Ok(Self {
            proposal_id: pending.proposal_id,
            execution_id,
            lane_authority: pending.lane_authority,
            retained_proposal: pending.retained_proposal,
            authority_class: pending.authority_class,
            approval_subject: Some(pending.approval_subject),
            grant_evidence,
        })
    }

    /// Returns the exact proposal covered by the grant.
    #[must_use]
    pub(crate) const fn proposal_id(&self) -> &RuntimeToolProposalId {
        &self.proposal_id
    }

    /// Returns the exact execution identity covered by the grant.
    #[must_use]
    pub(crate) const fn execution_id(&self) -> &RuntimeToolExecutionId {
        &self.execution_id
    }

    /// Returns the exact Run and Tool lane leases that own the grant.
    #[must_use]
    pub(crate) const fn lane_authority(&self) -> &PhaseLaneAuthority {
        &self.lane_authority
    }

    /// Returns the immutable Run generation that owns the grant.
    #[must_use]
    #[cfg(test)]
    pub(crate) const fn run_generation(&self) -> RuntimeGeneration {
        self.lane_authority.run_generation()
    }

    /// Returns the granted authority class.
    #[must_use]
    pub(crate) const fn authority_class(&self) -> ToolAuthorityClass {
        self.authority_class
    }

    /// Returns the host-retained normalized proposal.
    #[must_use]
    pub(crate) const fn retained_proposal(&self) -> &RetainedToolProposalRef {
        &self.retained_proposal
    }

    /// Returns approval binding for an interactively approved grant.
    #[must_use]
    pub(crate) const fn approval_subject(&self) -> Option<&ApprovalSubjectBinding> {
        self.approval_subject.as_ref()
    }

    /// Returns the policy and approval evidence that issued this grant.
    #[must_use]
    pub(crate) const fn grant_evidence(&self) -> &RedactedEvidenceRef {
        &self.grant_evidence
    }
}

fn validate_tool_lane_authority(
    authority: &PhaseLaneAuthority,
) -> Result<(), KernelPhaseContractError> {
    if authority.lane() != RuntimeGenerationLane::Tool {
        return Err(KernelPhaseContractError::LaneMismatch {
            expected: RuntimeGenerationLane::Tool,
            observed: authority.lane(),
        });
    }
    Ok(())
}

fn validate_tool_authority_class(
    authority_class: ToolAuthorityClass,
) -> Result<(), KernelPhaseContractError> {
    if authority_class == ToolAuthorityClass::Unknown {
        return Err(KernelPhaseContractError::UnknownToolAuthority);
    }
    Ok(())
}

/// Result of catalog, validation, policy, approval-posture, and fence gating.
#[derive(Debug)]
pub(crate) enum ToolGateDecision {
    /// The proposal may execute without an interactive approval wait.
    Granted(GrantedToolAuthority),
    /// The exact proposal must wait for its stable approval subject.
    ApprovalRequired(PendingToolAuthority),
    /// The proposal was denied; no execution token exists.
    Denied {
        /// Redacted policy/gate evidence.
        evidence: RedactedEvidenceRef,
    },
}

/// Approval wait or crash-resume request.
#[derive(Debug)]
pub(crate) struct ApprovalWaitResumeRequest {
    /// Pending authority issued by the tool gate.
    pub(crate) pending: PendingToolAuthority,
    /// Optional durable resolution evidence loaded after restart.
    pub(crate) resume_evidence: Option<RedactedEvidenceRef>,
}

/// Approval resolution without a reusable approval capability.
#[derive(Debug)]
pub(crate) enum ApprovalWaitResumeResult {
    /// Approval granted a one-shot execution token.
    Granted(GrantedToolAuthority),
    /// Approval denied the proposal.
    Denied {
        /// Redacted approval resolution evidence.
        evidence: RedactedEvidenceRef,
    },
    /// The bounded wait expired and denied authority.
    TimedOut {
        /// Redacted timeout/resolution evidence.
        evidence: RedactedEvidenceRef,
    },
    /// The approval remains durable and the run may suspend.
    Pending(PendingToolAuthority),
}

/// Execution request that cannot exist without a consumed authority token.
#[derive(Debug)]
pub(crate) struct ToolExecutionRequest {
    authority: GrantedToolAuthority,
}

impl ToolExecutionRequest {
    /// Consumes a gateway-issued grant for the exact invocation lane.
    ///
    /// # Errors
    /// Returns [`KernelPhaseContractError::ToolAuthorityBindingMismatch`] when
    /// the grant belongs to another run, lease, or Tool lane generation.
    pub(crate) fn new(
        authority: GrantedToolAuthority,
        invocation_authority: &PhaseLaneAuthority,
    ) -> Result<Self, KernelPhaseContractError> {
        if authority.lane_authority() != invocation_authority {
            return Err(KernelPhaseContractError::ToolAuthorityBindingMismatch);
        }
        Ok(Self { authority })
    }

    /// Returns the exact proposal covered by this request.
    #[must_use]
    pub(crate) const fn proposal_id(&self) -> &RuntimeToolProposalId {
        self.authority.proposal_id()
    }

    /// Returns the exact execution identity covered by this request.
    #[must_use]
    pub(crate) const fn execution_id(&self) -> &RuntimeToolExecutionId {
        self.authority.execution_id()
    }

    /// Returns the generation that owns this request.
    #[must_use]
    #[cfg(test)]
    pub(crate) const fn run_generation(&self) -> RuntimeGeneration {
        self.authority.run_generation()
    }

    /// Returns the exact Run and Tool lane leases consumed by this request.
    #[must_use]
    pub(crate) const fn lane_authority(&self) -> &PhaseLaneAuthority {
        self.authority.lane_authority()
    }
}

/// Host-only execution receipt; it is never a model projection.
#[derive(Debug)]
pub(crate) struct ToolExecutionReceipt {
    proposal_id: RuntimeToolProposalId,
    execution_id: RuntimeToolExecutionId,
    lane_authority: PhaseLaneAuthority,
    outcome: RawToolExecutionOutcomeRef,
    side_effect_state: Option<SideEffectFenceState>,
    evidence: Vec<RedactedEvidenceRef>,
}

impl ToolExecutionReceipt {
    /// Creates a receipt bound to the consumed request identities.
    ///
    /// # Errors
    /// Returns [`KernelPhaseContractError::TooManyEvidenceRefs`] when evidence
    /// exceeds the bounded kernel projection.
    fn from_request(
        request: &ToolExecutionRequest,
        outcome: RawToolExecutionOutcomeRef,
        side_effect_state: Option<SideEffectFenceState>,
        evidence: Vec<RedactedEvidenceRef>,
    ) -> Result<Self, KernelPhaseContractError> {
        if evidence.len() > MAX_EVIDENCE_REFS {
            return Err(KernelPhaseContractError::TooManyEvidenceRefs);
        }
        Ok(Self {
            proposal_id: request.proposal_id().clone(),
            execution_id: request.execution_id().clone(),
            lane_authority: request.lane_authority().clone(),
            outcome,
            side_effect_state,
            evidence,
        })
    }

    /// Returns the proposal identity.
    #[must_use]
    pub(crate) const fn proposal_id(&self) -> &RuntimeToolProposalId {
        &self.proposal_id
    }

    /// Returns the execution identity.
    #[must_use]
    pub(crate) const fn execution_id(&self) -> &RuntimeToolExecutionId {
        &self.execution_id
    }

    /// Returns the exact Run and Tool lane leases used for execution.
    #[must_use]
    pub(crate) const fn lane_authority(&self) -> &PhaseLaneAuthority {
        &self.lane_authority
    }

    /// Returns the host-retained raw execution outcome.
    #[must_use]
    pub(crate) const fn outcome(&self) -> &RawToolExecutionOutcomeRef {
        &self.outcome
    }

    /// Returns the side-effect fence state, when execution required one.
    #[must_use]
    pub(crate) const fn side_effect_state(&self) -> Option<SideEffectFenceState> {
        self.side_effect_state
    }

    /// Returns separate host-only execution evidence.
    #[must_use]
    pub(crate) fn evidence(&self) -> &[RedactedEvidenceRef] {
        self.evidence.as_slice()
    }
}

/// Request to apply the live result middleware and redaction boundary.
#[derive(Debug)]
pub(crate) struct ToolResultProjectionRequest {
    /// Host-only execution receipt.
    pub(crate) receipt: ToolExecutionReceipt,
}

/// Model-visible tool result plus separate host evidence references.
#[derive(Debug)]
pub(crate) struct ToolResultProjection {
    /// Exact proposal identity.
    pub(crate) proposal_id: RuntimeToolProposalId,
    /// Exact execution identity.
    pub(crate) execution_id: RuntimeToolExecutionId,
    /// Host-retained, already-redacted model projection.
    pub(crate) model_visible_result: ModelVisibleToolResultRef,
    /// Separate evidence not exposed to the model.
    pub(crate) evidence: Vec<RedactedEvidenceRef>,
}

/// Sole host authority for tool validation, approval, execution, and projection.
///
/// The live implementation must be an adapter over
/// `application::run_stream::tool_flow`; implementations must not evaluate a
/// second policy, call an execution backend directly, or project a raw receipt.
/// The four methods expose the existing boundary as typed kernel phases. The
/// adapter is implemented as a child of this module so only that boundary can
/// issue the private pending/granted tokens and execution receipts.
pub(crate) trait ToolAuthorityGateway:
    tool_gateway_sealed::LiveToolFlowAdapter + Send + Sync
{
    /// Runs catalog/schema/policy/approval-posture/fence preparation.
    fn gate(
        &self,
        input: ToolGatePhaseInput,
    ) -> KernelPhaseFuture<'_, Result<ToolGatePhaseOutput, KernelPhaseError>>;

    /// Waits for or resumes the exact durable approval subject.
    fn wait_or_resume_approval(
        &self,
        input: ApprovalWaitPhaseInput,
    ) -> KernelPhaseFuture<'_, Result<ApprovalWaitPhaseOutput, KernelPhaseError>>;

    /// Dispatches one call using a consumed gateway-issued authority token.
    fn execute(
        &self,
        input: ToolExecutionPhaseInput,
    ) -> KernelPhaseFuture<'_, Result<ToolExecutionPhaseOutput, KernelPhaseError>>;

    /// Applies live result middleware, redaction, spill, and model projection.
    fn project_result(
        &self,
        input: ResultProjectionPhaseInput,
    ) -> KernelPhaseFuture<'_, Result<ResultProjectionPhaseOutput, KernelPhaseError>>;
}

include!("tool_authority/live.rs");
