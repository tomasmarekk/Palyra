// Owned messages for the RuntimeKernelV2 adapter over the live tool flow.
//
// These messages carry only opaque host references and typed identities. Raw
// proposals and raw execution results remain retained by the run-stream owner.

/// Boxed live-tool-flow operation used by the kernel authority adapter.
pub(crate) type LiveToolFlowFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, LiveToolFlowError>> + Send + 'a>>;

/// Neutral descriptor for a payload retained by the run-stream host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LiveToolHostRef {
    /// Host-store identity.
    pub(crate) id: palyra_common::runtime_contracts::RuntimeOperationId,
    /// Domain-bound digest of the retained payload.
    pub(crate) sha256: [u8; 32],
}

/// Canonical stage that failed inside the live tool-flow owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiveToolFlowStage {
    /// Catalog, schema, classification, policy, and fence preparation.
    Gate,
    /// Durable approval wait or restart resume.
    Approval,
    /// Canonical runtime dispatcher execution.
    Execution,
    /// Middleware, redaction, evidence, and model projection.
    Projection,
}

/// Redacted failure returned by the host-owned live tool flow.
#[derive(Debug)]
pub(crate) struct LiveToolFlowError {
    stage: LiveToolFlowStage,
    evidence: Option<LiveToolHostRef>,
}

impl LiveToolFlowError {
    /// Creates a stage-classified failure without exposing raw error payloads.
    #[must_use]
    pub(crate) const fn new(stage: LiveToolFlowStage, evidence: Option<LiveToolHostRef>) -> Self {
        Self { stage, evidence }
    }

    /// Returns the canonical stage that failed.
    #[must_use]
    pub(crate) const fn stage(&self) -> LiveToolFlowStage {
        self.stage
    }

    /// Consumes the failure and returns its redacted evidence.
    #[must_use]
    pub(crate) fn into_evidence(self) -> Option<LiveToolHostRef> {
        self.evidence
    }
}

/// Owned request for catalog/schema/policy/fence preparation.
#[derive(Debug)]
pub(crate) struct LiveToolGateRequest {
    /// Exact generation and Tool lane lease authorized by the kernel.
    pub(crate) lane_authority: crate::application::runtime_kernel_v2::phases::PhaseLaneAuthority,
    /// Model-issued proposal identity.
    pub(crate) proposal_id: palyra_common::runtime_contracts::RuntimeToolProposalId,
    /// Validated catalog name.
    pub(crate) tool_name: String,
    /// Opaque normalized proposal retained by the host.
    pub(crate) retained_proposal:
        crate::application::runtime_kernel_v2::phases::RetainedToolProposalRef,
    /// Maximum authority requested by the classified tool.
    pub(crate) requested_authority:
        crate::application::runtime_kernel_v2::phases::ToolAuthorityClass,
}

/// Host gate decision before any interactive approval wait.
#[derive(Debug)]
pub(crate) enum LiveToolGateResult {
    /// Policy granted non-interactive one-shot execution.
    Granted {
        /// Identity allocated only after the grant decision.
        execution_id: palyra_common::runtime_contracts::RuntimeToolExecutionId,
        /// Redacted policy and fence evidence.
        evidence: LiveToolHostRef,
    },
    /// The exact normalized proposal requires durable approval.
    ApprovalRequired {
        /// Stable subject produced from the normalized proposal.
        approval_subject_id: palyra_common::runtime_contracts::RuntimeApprovalSubjectId,
    },
    /// Catalog, schema, classification, policy, or fence checks denied authority.
    Denied {
        /// Redacted denial evidence.
        evidence: LiveToolHostRef,
    },
}

/// Owned request to wait for or resume one durable approval.
#[derive(Debug)]
pub(crate) struct LiveToolApprovalRequest {
    /// Exact generation and Tool lane lease awaiting resolution.
    pub(crate) lane_authority: crate::application::runtime_kernel_v2::phases::PhaseLaneAuthority,
    /// Exact proposal awaiting resolution.
    pub(crate) proposal_id: palyra_common::runtime_contracts::RuntimeToolProposalId,
    /// Opaque normalized proposal retained by the host.
    pub(crate) retained_proposal:
        crate::application::runtime_kernel_v2::phases::RetainedToolProposalRef,
    /// Classified authority covered by the stable approval subject.
    pub(crate) authority_class: crate::application::runtime_kernel_v2::phases::ToolAuthorityClass,
    /// Stable approval subject.
    pub(crate) approval_subject_id: palyra_common::runtime_contracts::RuntimeApprovalSubjectId,
    /// Optional durable resolution evidence loaded after restart.
    pub(crate) resume_evidence:
        Option<crate::application::runtime_kernel_v2::phases::RedactedEvidenceRef>,
}

/// Durable approval resolution from the live run owner.
#[derive(Debug)]
pub(crate) enum LiveToolApprovalResult {
    /// Approval granted one-shot execution authority.
    Granted {
        /// Identity allocated only after approval was granted.
        execution_id: palyra_common::runtime_contracts::RuntimeToolExecutionId,
        /// Redacted approval evidence.
        evidence: LiveToolHostRef,
    },
    /// Approval denied authority.
    Denied {
        /// Redacted denial evidence.
        evidence: LiveToolHostRef,
    },
    /// The bounded approval wait expired.
    TimedOut {
        /// Redacted timeout evidence.
        evidence: LiveToolHostRef,
    },
    /// The durable approval remains unresolved and can be resumed.
    Pending,
}

/// Owned execution request consumed by the existing canonical dispatcher path.
#[derive(Debug)]
pub(crate) struct LiveToolExecutionRequest {
    /// Exact generation and Tool lane lease authorizing execution.
    pub(crate) lane_authority: crate::application::runtime_kernel_v2::phases::PhaseLaneAuthority,
    /// Exact proposal covered by the grant.
    pub(crate) proposal_id: palyra_common::runtime_contracts::RuntimeToolProposalId,
    /// Identity allocated after policy or approval grant.
    pub(crate) execution_id: palyra_common::runtime_contracts::RuntimeToolExecutionId,
    /// Opaque normalized proposal retained by the host.
    pub(crate) retained_proposal:
        crate::application::runtime_kernel_v2::phases::RetainedToolProposalRef,
    /// Granted authority class.
    pub(crate) authority_class: crate::application::runtime_kernel_v2::phases::ToolAuthorityClass,
    /// Exact policy or approval evidence that issued the one-shot grant.
    pub(crate) grant_evidence: crate::application::runtime_kernel_v2::phases::RedactedEvidenceRef,
    /// Stable subject when an interactive approval issued the grant.
    pub(crate) approval_subject_id:
        Option<palyra_common::runtime_contracts::RuntimeApprovalSubjectId>,
}

/// Host-only receipt material returned by canonical tool execution.
#[derive(Debug)]
pub(crate) struct LiveToolExecutionResult {
    /// Opaque raw outcome retained by the live tool flow.
    pub(crate) outcome: LiveToolHostRef,
    /// Durable side-effect fence state, when one exists.
    pub(crate) side_effect_state: Option<palyra_common::runtime_contracts::SideEffectFenceState>,
    /// Separate redacted execution evidence.
    pub(crate) evidence: Vec<LiveToolHostRef>,
}

/// Owned projection request; raw payload bytes remain host-side.
#[derive(Debug)]
pub(crate) struct LiveToolProjectionRequest {
    /// Exact generation and Tool lane lease that executed the tool.
    pub(crate) lane_authority: crate::application::runtime_kernel_v2::phases::PhaseLaneAuthority,
    /// Exact proposal identity.
    pub(crate) proposal_id: palyra_common::runtime_contracts::RuntimeToolProposalId,
    /// Exact execution identity.
    pub(crate) execution_id: palyra_common::runtime_contracts::RuntimeToolExecutionId,
    /// Opaque raw result retained by the host.
    pub(crate) outcome: crate::application::runtime_kernel_v2::phases::RawToolExecutionOutcomeRef,
    /// Execution evidence kept separate from model-visible output.
    pub(crate) execution_evidence:
        Vec<crate::application::runtime_kernel_v2::phases::RedactedEvidenceRef>,
    /// Durable side-effect posture bound to the retained raw result.
    pub(crate) side_effect_state: Option<palyra_common::runtime_contracts::SideEffectFenceState>,
}

/// Redacted model projection returned by live result middleware.
#[derive(Debug)]
pub(crate) struct LiveToolProjectionResult {
    /// Opaque, already-redacted model-visible result.
    pub(crate) model_visible_result: LiveToolHostRef,
    /// Separate host evidence not exposed to the model.
    pub(crate) evidence: Vec<LiveToolHostRef>,
}

/// Generation-bound owner of live tool-flow stage execution.
///
/// RuntimeKernelV2 injects one owned implementation per active run. The
/// implementation resolves opaque references in its host-side stores and must
/// delegate execution to `execute_prepared_tool_runtime`, which contains the
/// repository's sole canonical dispatcher callsite.
pub(crate) trait LiveToolFlowPort: Send + Sync {
    /// Runs catalog/schema/classification/policy/fence preparation without waiting.
    fn gate(&self, request: LiveToolGateRequest) -> LiveToolFlowFuture<'_, LiveToolGateResult>;

    /// Waits for or resumes the exact durable approval subject.
    fn wait_or_resume_approval(
        &self,
        request: LiveToolApprovalRequest,
    ) -> LiveToolFlowFuture<'_, LiveToolApprovalResult>;

    /// Executes a granted proposal through the canonical dispatcher exactly once.
    fn execute(
        &self,
        request: LiveToolExecutionRequest,
    ) -> LiveToolFlowFuture<'_, LiveToolExecutionResult>;

    /// Applies middleware/redaction and returns only a model-visible reference.
    fn project_result(
        &self,
        request: LiveToolProjectionRequest,
    ) -> LiveToolFlowFuture<'_, LiveToolProjectionResult>;
}

#[cfg(test)]
mod stage_contract_tests {
    const LIVE_OWNER_SOURCE: &str = include_str!("owner.rs");
    const TOOL_FLOW_SOURCE: &str = include_str!("../tool_flow.rs");
    const LIVE_GATEWAY_SOURCE: &str =
        include_str!("../../runtime_kernel_v2/phases/tool_authority/live.rs");

    #[test]
    fn live_run_stream_keeps_one_canonical_dispatcher_callsite() {
        let dispatcher_call =
            ["execute_tool_with_runtime_dispatch_with_cancellation_and_progress", "("].concat();
        assert_eq!(
            TOOL_FLOW_SOURCE.matches(dispatcher_call.as_str()).count(),
            1,
            "the staged adapter must not duplicate backend execution"
        );
    }

    #[test]
    fn pre_approval_gate_cannot_wait_for_approval() {
        let gate_start = TOOL_FLOW_SOURCE
            .find("async fn prepare_run_stream_tool_gate_without_approval")
            .expect("pre-approval gate stage exists");
        let approval_start = TOOL_FLOW_SOURCE
            .find("async fn resolve_run_stream_tool_gate_approval")
            .expect("approval stage exists");
        let gate_source = &TOOL_FLOW_SOURCE[gate_start..approval_start];

        assert!(!gate_source.contains("resolve_run_stream_tool_approval_outcome("));
        assert!(TOOL_FLOW_SOURCE[approval_start..]
            .contains("resolve_run_stream_tool_approval_outcome("));
    }

    #[test]
    fn projection_precedes_legacy_commit_and_publish() {
        let finalize_start = TOOL_FLOW_SOURCE
            .find("async fn finalize_prepared_tool_execution_outcome")
            .expect("legacy finalization wrapper exists");
        let finalize_source = &TOOL_FLOW_SOURCE[finalize_start..];
        let projection = finalize_source
            .find("project_prepared_tool_execution_outcome(")
            .expect("projection stage is called");
        let commit = finalize_source
            .find("commit_and_publish_projected_tool_execution_outcome(")
            .expect("legacy commit wrapper is called");

        assert!(projection < commit);
    }

    #[test]
    fn drained_post_execution_error_settles_before_propagation() {
        let branch_start = LIVE_OWNER_SOURCE
            .find("if let Some(post_execution_error) = post_execution_error")
            .expect("post-execution error branch exists");
        let branch_end = LIVE_OWNER_SOURCE[branch_start..]
            .find("let execution_evidence")
            .map(|offset| branch_start + offset)
            .expect("normal raw-outcome retention follows the error branch");
        let branch = &LIVE_OWNER_SOURCE[branch_start..branch_end];
        let settlement = branch
            .find("finalize_drained_tool_execution_before_error(")
            .expect("drained outcome settlement is called");
        let propagation = branch
            .find("return Err(self.status_failure")
            .expect("post-execution error is propagated");

        assert!(settlement < propagation);
        assert!(!branch.contains("retain_raw_outcome("));
    }

    #[test]
    fn live_gateway_has_no_unsafe_lifetime_escape_or_direct_dispatch() {
        assert_eq!(LIVE_GATEWAY_SOURCE.matches("impl ToolAuthorityGateway for").count(), 1);
        assert!(!LIVE_GATEWAY_SOURCE.contains("unsafe"));
        assert!(!LIVE_GATEWAY_SOURCE.contains("transmute"));
        assert!(!LIVE_GATEWAY_SOURCE
            .contains("execute_tool_with_runtime_dispatch_with_cancellation_and_progress"));
    }
}
