// Live RuntimeKernelV2 adapter over the host-owned run-stream tool flow.
//
// This adapter alone mints non-cloneable authority tokens. Its injected port
// resolves opaque payload references and owns all raw proposal/result state.

use crate::application::run_stream::tool_flow::{
    LiveToolApprovalRequest, LiveToolApprovalResult, LiveToolExecutionRequest, LiveToolFlowError,
    LiveToolFlowPort, LiveToolFlowStage, LiveToolGateRequest, LiveToolGateResult, LiveToolHostRef,
    LiveToolProjectionRequest,
};

/// Sole non-test implementation of the kernel tool-authority boundary.
///
/// One instance is bound to an exact Run lease and Tool lane lease. Reusing it
/// after a generation change, for another run, or for another lane fails closed
/// before the live flow is called.
pub(crate) struct LiveToolAuthorityGateway {
    lane_authority: PhaseLaneAuthority,
    port: Arc<dyn LiveToolFlowPort>,
}

impl fmt::Debug for LiveToolAuthorityGateway {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LiveToolAuthorityGateway")
            .field("lane_authority", &self.lane_authority)
            .finish_non_exhaustive()
    }
}

impl LiveToolAuthorityGateway {
    /// Mints the opaque kernel reference for a proposal retained by the live
    /// run-stream owner.
    #[must_use]
    pub(crate) fn retained_proposal_ref(reference: LiveToolHostRef) -> RetainedToolProposalRef {
        RetainedToolProposalRef::from_host(reference.id, reference.sha256)
    }

    /// Binds an owned live-flow port to one active Run and Tool lane lease.
    ///
    /// # Errors
    /// Returns [`KernelPhaseContractError`] unless the supplied authority is
    /// for the Tool lane.
    pub(crate) fn new(
        lane_authority: PhaseLaneAuthority,
        port: Arc<dyn LiveToolFlowPort>,
    ) -> Result<Self, KernelPhaseContractError> {
        validate_tool_lane_authority(&lane_authority)?;
        Ok(Self { lane_authority, port })
    }

    fn validate_boundary(
        &self,
        boundary: &KernelPhaseBoundary,
    ) -> Result<(), KernelPhaseContractError> {
        if boundary.execution().lane_authority() != &self.lane_authority {
            return Err(KernelPhaseContractError::ToolAuthorityBindingMismatch);
        }
        Ok(())
    }
}

impl tool_gateway_sealed::LiveToolFlowAdapter for LiveToolAuthorityGateway {}

impl ToolAuthorityGateway for LiveToolAuthorityGateway {
    fn gate(
        &self,
        input: ToolGatePhaseInput,
    ) -> KernelPhaseFuture<'_, Result<ToolGatePhaseOutput, KernelPhaseError>> {
        Box::pin(async move {
            self.validate_boundary(input.boundary())?;
            let proposal = input.payload();
            let port_result = self
                .port
                .gate(LiveToolGateRequest {
                    lane_authority: self.lane_authority.clone(),
                    proposal_id: proposal.proposal_id().clone(),
                    tool_name: proposal.tool_name().to_owned(),
                    retained_proposal: proposal.retained_proposal().clone(),
                    requested_authority: proposal.requested_authority(),
                })
                .await;

            let (reason, decision) = match port_result {
                Ok(LiveToolGateResult::Granted { execution_id, evidence }) => {
                    let authority = GrantedToolAuthority::issue_noninteractive(
                        proposal.proposal_id().clone(),
                        execution_id,
                        self.lane_authority.clone(),
                        proposal.retained_proposal().clone(),
                        proposal.requested_authority(),
                        redacted_evidence_ref(evidence),
                    )?;
                    (KernelPhaseReason::ToolGateGranted, ToolGateDecision::Granted(authority))
                }
                Ok(LiveToolGateResult::ApprovalRequired { approval_subject_id }) => {
                    let approval_subject = ApprovalSubjectBinding::from_host(
                        approval_subject_id,
                        *proposal.retained_proposal().sha256(),
                        proposal.requested_authority(),
                    );
                    let pending = PendingToolAuthority::issue(
                        proposal.proposal_id().clone(),
                        self.lane_authority.clone(),
                        proposal.retained_proposal().clone(),
                        proposal.requested_authority(),
                        approval_subject,
                    )?;
                    (
                        KernelPhaseReason::ToolGateApprovalRequired,
                        ToolGateDecision::ApprovalRequired(pending),
                    )
                }
                Ok(LiveToolGateResult::Denied { evidence }) => (
                    KernelPhaseReason::ToolGateDenied,
                    ToolGateDecision::Denied { evidence: redacted_evidence_ref(evidence) },
                ),
                // Any catalog/schema/classification/policy/fence error denies
                // authority. It never escapes as a retryable path that could
                // accidentally bypass the gate.
                Err(error) => (
                    KernelPhaseReason::ToolGateDenied,
                    ToolGateDecision::Denied {
                        evidence: require_fail_closed_evidence(
                            error,
                            LiveToolFlowStage::Gate,
                            RuntimeErrorPhase::ToolGate,
                            KernelPhaseReason::ToolGateDenied,
                        )?,
                    },
                ),
            };
            Ok(KernelPhaseOutput::from_input(&input, reason, decision)?)
        })
    }

    fn wait_or_resume_approval(
        &self,
        input: ApprovalWaitPhaseInput,
    ) -> KernelPhaseFuture<'_, Result<ApprovalWaitPhaseOutput, KernelPhaseError>> {
        Box::pin(async move {
            self.validate_boundary(input.boundary())?;
            if input.payload().pending.lane_authority() != &self.lane_authority {
                return Err(KernelPhaseContractError::ToolAuthorityBindingMismatch.into());
            }

            let request = LiveToolApprovalRequest {
                lane_authority: self.lane_authority.clone(),
                proposal_id: input.payload().pending.proposal_id().clone(),
                retained_proposal: input.payload().pending.retained_proposal().clone(),
                authority_class: input.payload().pending.authority_class(),
                approval_subject_id: input
                    .payload()
                    .pending
                    .approval_subject()
                    .approval_subject_id()
                    .clone(),
                resume_evidence: input.payload().resume_evidence.clone(),
            };
            let boundary = input.boundary().clone();
            let ApprovalWaitResumeRequest { pending, resume_evidence: _ } = input.into_payload();
            let result = self.port.wait_or_resume_approval(request).await;
            let (reason, result) = match result {
                Ok(LiveToolApprovalResult::Granted { execution_id, evidence }) => {
                    let granted = GrantedToolAuthority::from_approved_pending(
                        pending,
                        execution_id,
                        redacted_evidence_ref(evidence),
                    )?;
                    (KernelPhaseReason::ApprovalGranted, ApprovalWaitResumeResult::Granted(granted))
                }
                Ok(LiveToolApprovalResult::Denied { evidence }) => (
                    KernelPhaseReason::ApprovalDenied,
                    ApprovalWaitResumeResult::Denied { evidence: redacted_evidence_ref(evidence) },
                ),
                Ok(LiveToolApprovalResult::TimedOut { evidence }) => (
                    KernelPhaseReason::ApprovalTimedOut,
                    ApprovalWaitResumeResult::TimedOut {
                        evidence: redacted_evidence_ref(evidence),
                    },
                ),
                Ok(LiveToolApprovalResult::Pending) => {
                    (KernelPhaseReason::ApprovalPending, ApprovalWaitResumeResult::Pending(pending))
                }
                Err(error) => (
                    KernelPhaseReason::ApprovalDenied,
                    ApprovalWaitResumeResult::Denied {
                        evidence: require_fail_closed_evidence(
                            error,
                            LiveToolFlowStage::Approval,
                            RuntimeErrorPhase::Approval,
                            KernelPhaseReason::ApprovalDenied,
                        )?,
                    },
                ),
            };
            phase_output_from_boundary::<ApprovalWaitPhase, _>(boundary, reason, result)
                .map_err(Into::into)
        })
    }

    fn execute(
        &self,
        input: ToolExecutionPhaseInput,
    ) -> KernelPhaseFuture<'_, Result<ToolExecutionPhaseOutput, KernelPhaseError>> {
        Box::pin(async move {
            self.validate_boundary(input.boundary())?;
            if input.payload().lane_authority() != &self.lane_authority {
                return Err(KernelPhaseContractError::ToolAuthorityBindingMismatch.into());
            }

            let request = LiveToolExecutionRequest {
                lane_authority: self.lane_authority.clone(),
                proposal_id: input.payload().proposal_id().clone(),
                execution_id: input.payload().execution_id().clone(),
                retained_proposal: input.payload().authority.retained_proposal().clone(),
                authority_class: input.payload().authority.authority_class(),
                grant_evidence: input.payload().authority.grant_evidence().clone(),
                approval_subject_id: input
                    .payload()
                    .authority
                    .approval_subject()
                    .map(|subject| subject.approval_subject_id().clone()),
            };
            let boundary = input.boundary().clone();
            let execution_request = input.into_payload();
            let result = self.port.execute(request).await.map_err(tool_execution_error)?;
            let reason = if result.side_effect_state == Some(SideEffectFenceState::EffectUnknown) {
                KernelPhaseReason::ToolExecutionUnknown
            } else {
                KernelPhaseReason::ToolExecutionCompleted
            };
            let receipt = ToolExecutionReceipt::from_request(
                &execution_request,
                raw_outcome_ref(result.outcome),
                result.side_effect_state,
                result.evidence.into_iter().map(redacted_evidence_ref).collect(),
            )?;
            phase_output_from_boundary::<ToolExecutionPhase, _>(boundary, reason, receipt)
                .map_err(Into::into)
        })
    }

    fn project_result(
        &self,
        input: ResultProjectionPhaseInput,
    ) -> KernelPhaseFuture<'_, Result<ResultProjectionPhaseOutput, KernelPhaseError>> {
        Box::pin(async move {
            self.validate_boundary(input.boundary())?;
            if input.payload().receipt.lane_authority() != &self.lane_authority {
                return Err(KernelPhaseContractError::ToolAuthorityBindingMismatch.into());
            }

            let receipt = &input.payload().receipt;
            let retained_evidence = receipt.evidence().to_vec();
            let request = LiveToolProjectionRequest {
                lane_authority: self.lane_authority.clone(),
                proposal_id: receipt.proposal_id().clone(),
                execution_id: receipt.execution_id().clone(),
                outcome: receipt.outcome().clone(),
                execution_evidence: retained_evidence.clone(),
                side_effect_state: receipt.side_effect_state(),
            };
            let boundary = input.boundary().clone();
            let ToolResultProjectionRequest { receipt } = input.into_payload();
            let result = self.port.project_result(request).await.map_err(tool_projection_error)?;
            let mut evidence = retained_evidence;
            for reference in result.evidence.into_iter().map(redacted_evidence_ref) {
                if !evidence.contains(&reference) {
                    evidence.push(reference);
                }
            }
            if evidence.len() > MAX_EVIDENCE_REFS {
                return Err(KernelPhaseContractError::TooManyEvidenceRefs.into());
            }
            let projection = ToolResultProjection {
                proposal_id: receipt.proposal_id,
                execution_id: receipt.execution_id,
                model_visible_result: model_visible_result_ref(result.model_visible_result),
                evidence,
            };
            phase_output_from_boundary::<ResultProjectionPhase, _>(
                boundary,
                KernelPhaseReason::ResultProjected,
                projection,
            )
            .map_err(Into::into)
        })
    }
}

fn phase_output_from_boundary<P: CanonicalPhase, T>(
    boundary: KernelPhaseBoundary,
    reason: KernelPhaseReason,
    payload: T,
) -> Result<KernelPhaseOutput<P, T>, KernelPhaseContractError> {
    if reason.phase() != P::PHASE {
        return Err(KernelPhaseContractError::ReasonPhaseMismatch {
            expected: P::PHASE,
            observed: reason.phase(),
        });
    }
    Ok(KernelPhaseOutput { boundary, reason, payload, phase: PhantomData })
}

fn require_fail_closed_evidence(
    error: LiveToolFlowError,
    expected_stage: LiveToolFlowStage,
    phase: RuntimeErrorPhase,
    reason: KernelPhaseReason,
) -> Result<RedactedEvidenceRef, KernelPhaseError> {
    debug_assert_eq!(error.stage(), expected_stage);
    error.into_evidence().map(redacted_evidence_ref).ok_or(KernelPhaseError::HostService {
        phase,
        reason,
        evidence: None,
    })
}

fn tool_execution_error(error: LiveToolFlowError) -> KernelPhaseError {
    debug_assert_eq!(error.stage(), LiveToolFlowStage::Execution);
    KernelPhaseError::HostService {
        phase: RuntimeErrorPhase::ToolExecution,
        reason: KernelPhaseReason::ToolExecutionUnknown,
        evidence: error.into_evidence().map(redacted_evidence_ref),
    }
}

fn tool_projection_error(error: LiveToolFlowError) -> KernelPhaseError {
    debug_assert_eq!(error.stage(), LiveToolFlowStage::Projection);
    KernelPhaseError::HostService {
        phase: RuntimeErrorPhase::ResultProjection,
        reason: KernelPhaseReason::ResultProjectionWithheld,
        evidence: error.into_evidence().map(redacted_evidence_ref),
    }
}

fn redacted_evidence_ref(reference: LiveToolHostRef) -> RedactedEvidenceRef {
    RedactedEvidenceRef::from_host(reference.id, reference.sha256)
}

fn raw_outcome_ref(reference: LiveToolHostRef) -> RawToolExecutionOutcomeRef {
    RawToolExecutionOutcomeRef::from_host(reference.id, reference.sha256)
}

fn model_visible_result_ref(reference: LiveToolHostRef) -> ModelVisibleToolResultRef {
    ModelVisibleToolResultRef::from_host(reference.id, reference.sha256)
}

#[cfg(test)]
mod live_gateway_factory_tests {
    use super::*;
    use crate::application::run_stream::tool_flow::run_stream_live_tool_authority;

    #[test]
    fn run_owner_factory_injects_the_concrete_live_gateway() {
        let session_id = RuntimeSessionId::parse("session_tool_factory").expect("session id");
        let run_id = RuntimeRunId::parse("run_tool_factory").expect("run id");
        let generation = RuntimeGeneration::new(7).expect("generation");
        let run_lease = RuntimeLeaseId::parse("lease_run_tool_factory").expect("run lease");
        let tool_lease = RuntimeLeaseId::parse("lease_tool_factory").expect("tool lease");
        let authority = PhaseLaneAuthority::from_host_leases(
            session_id,
            run_id,
            generation,
            run_lease,
            RuntimeGenerationLane::Tool,
            generation,
            tool_lease,
        );

        let (gateway, _owner) =
            run_stream_live_tool_authority(authority).expect("live gateway factory");

        assert_eq!(Arc::strong_count(&gateway), 1);
    }
}
