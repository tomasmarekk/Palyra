// Typed failures for host phase execution and boundary-contract validation.
// Runtime-facing errors stay low-cardinality while preserving retryability and
// stable failure phases for journal and recovery decisions.

/// Fail-closed error returned by a host phase service.
#[derive(Debug, Error)]
pub(crate) enum KernelPhaseError {
    /// Input or output violated the canonical phase contract.
    #[error(transparent)]
    Contract(#[from] KernelPhaseContractError),
    /// The owning host service failed and retained redacted evidence.
    #[error("runtime phase host service failed in {phase:?}")]
    HostService {
        /// Phase that owns classification.
        phase: RuntimeErrorPhase,
        /// Stable, low-cardinality reason.
        reason: KernelPhaseReason,
        /// Host-retained redacted evidence.
        evidence: Option<RedactedEvidenceRef>,
    },
}

impl KernelPhaseError {
    /// Returns the stable reason code for tracing and terminal classification.
    #[must_use]
    pub(crate) const fn reason_code(&self) -> &'static str {
        match self {
            Self::Contract(error) => error.reason_code(),
            Self::HostService { reason, .. } => reason.as_str(),
        }
    }
}

/// Validation error for typed phase boundaries.
#[derive(Debug, Error)]
pub(crate) enum KernelPhaseContractError {
    /// Typed runtime identities were invalid.
    #[error("runtime phase identities are invalid")]
    InvalidIdentities(#[source] RuntimeIdentityError),
    /// Identity, cancellation, or authority generation did not match.
    #[error("runtime phase generation binding is inconsistent")]
    GenerationMismatch,
    /// Lane authority was issued for a different session or run.
    #[error("runtime phase lane authority identity binding is inconsistent")]
    LaneIdentityMismatch,
    /// Lane authority did not match the canonical lane for the phase.
    #[error("runtime phase requires {expected:?} lane authority, observed {observed:?}")]
    LaneMismatch {
        /// Canonical lane required by the phase.
        expected: RuntimeGenerationLane,
        /// Lane carried by the host grant.
        observed: RuntimeGenerationLane,
    },
    /// Run-lane authority did not use the authoritative Run lease itself.
    #[error("runtime Run lane authority is not bound to the authoritative Run lease")]
    InvalidRunLaneBinding,
    /// Cancellation or bounded backpressure policy was invalid.
    #[error("runtime phase flow control is invalid")]
    InvalidFlowControl,
    /// A phase received a scope, authority class, or trace policy owned elsewhere.
    #[error("runtime phase controls do not match {phase:?}")]
    InvalidPhaseControl {
        /// Expected phase.
        phase: RuntimeErrorPhase,
    },
    /// A reason code was attached to a different phase.
    #[error("runtime phase reason belongs to {observed:?}, expected {expected:?}")]
    ReasonPhaseMismatch {
        /// Expected phase.
        expected: RuntimeErrorPhase,
        /// Reason owner.
        observed: RuntimeErrorPhase,
    },
    /// Catalog tool name was empty or exceeded the bounded contract.
    #[error("runtime tool proposal name is invalid")]
    InvalidToolName,
    /// Tool authority could not be classified and therefore cannot execute.
    #[error("runtime tool authority class is unknown")]
    UnknownToolAuthority,
    /// Approval did not cover the exact normalized proposal and authority class.
    #[error("runtime approval subject binding does not match the tool proposal")]
    ApprovalSubjectBindingMismatch,
    /// An execution token belongs to another run or Tool lane lease.
    #[error("runtime tool authority does not match the execution phase lane")]
    ToolAuthorityBindingMismatch,
    /// Evidence exceeded the bounded kernel projection.
    #[error("runtime phase evidence reference limit exceeded")]
    TooManyEvidenceRefs,
}

impl KernelPhaseContractError {
    /// Returns the stable reason code for diagnostics and terminal mapping.
    #[must_use]
    pub(crate) const fn reason_code(&self) -> &'static str {
        match self {
            Self::InvalidIdentities(_) => "runtime.phase.invalid_identities",
            Self::GenerationMismatch => "runtime.phase.generation_mismatch",
            Self::LaneIdentityMismatch => "runtime.phase.lane_identity_mismatch",
            Self::LaneMismatch { .. } => "runtime.phase.lane_mismatch",
            Self::InvalidRunLaneBinding => "runtime.phase.invalid_run_lane_binding",
            Self::InvalidFlowControl => "runtime.phase.invalid_flow_control",
            Self::InvalidPhaseControl { .. } => "runtime.phase.invalid_phase_control",
            Self::ReasonPhaseMismatch { .. } => "runtime.phase.reason_phase_mismatch",
            Self::InvalidToolName => "runtime.phase.invalid_tool_name",
            Self::UnknownToolAuthority => "runtime.phase.unknown_tool_authority",
            Self::ApprovalSubjectBindingMismatch => {
                "runtime.phase.approval_subject_binding_mismatch"
            }
            Self::ToolAuthorityBindingMismatch => "runtime.phase.tool_authority_binding_mismatch",
            Self::TooManyEvidenceRefs => "runtime.phase.too_many_evidence_refs",
        }
    }
}
