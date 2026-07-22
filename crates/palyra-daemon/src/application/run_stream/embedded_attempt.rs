//! Production in-process driver for the RuntimeKernelV2 harness.
//!
//! The driver owns orchestration only. Host phase services retain provider
//! payloads and final projections, while the existing run-stream tool owner
//! retains raw proposals and pumps the sole live tool-authority gateway.

use std::sync::Arc;

use super::tool_flow::{
    run_stream_live_tool_authority, RunStreamLiveToolFlowOwner, RunStreamLiveToolHost,
};
use crate::application::runtime_kernel_v2::{
    context::RuntimeKernelContext,
    embedded_harness::{
        EmbeddedAttemptDriver, EmbeddedAttemptError, EmbeddedHarnessAdapter,
        EmbeddedHarnessEventPort,
    },
    harness::{
        HarnessApprovalResolution, HarnessAttemptRequest, HarnessAttemptTerminal,
        HarnessContractError, HarnessEvent, HarnessEventKind, HarnessEventSink, HarnessFuture,
        HarnessRuntimeV2, HarnessTerminalOutcome,
    },
    host_event_contract::{HarnessDeliveryBinding, HarnessTerminalReceipt},
    phases::{
        ApprovalWaitResumeRequest, ApprovalWaitResumeResult, CompactionRequest, CompactionResult,
        ContextAssemblyRequest, ContextAssemblyResult, DeliveryRequest, DeliveryResult,
        FinalizationReceipt, FinalizationRequest, KernelPhaseContractError, PhaseLaneAuthority,
        ProviderCallRequest, ProviderCallResult, RedactedEvidenceRef, ToolAuthorityGateway,
        ToolExecutionRequest, ToolGateDecision, ToolProposalRequest, ToolResultProjection,
        ToolResultProjectionRequest,
    },
};
use palyra_common::runtime_contracts::{
    RuntimeErrorEnvelopeV1, RuntimeOperationId, RuntimeToolProposalId,
};

const MAX_EMBEDDED_PROVIDER_TURNS: usize = 64;

/// One normalized provider turn returned by host-owned phase services.
#[derive(Debug)]
pub(crate) enum EmbeddedProviderTurn {
    /// A final provider response is ready for verification.
    Completed { text_utf8_bytes: u64, prompt_tokens: u64, completion_tokens: u64 },
    /// The provider proposed a host-retained tool operation.
    Tool { proposal: ToolProposalRequest, operation_id: RuntimeOperationId },
    /// The current context must be compacted before another provider call.
    CompactionRequired,
    /// Cancellation won the provider race.
    Cancelled { reason_code: String },
}

/// Explicit post-finalization delivery decision retained with host evidence.
pub(crate) enum EmbeddedDeliveryPlan {
    Commit(DeliveryRequest),
    Skip { evidence: RedactedEvidenceRef },
}

pub(crate) enum EmbeddedDeliveryOutcome {
    Committed(HarnessDeliveryBinding),
    Skipped(RedactedEvidenceRef),
    NotApplicable,
}

/// Failure boundary around the immutable final-artifact commit.
pub(crate) enum EmbeddedFinalizationError {
    /// No immutable final artifact was committed, so normal failure classification remains legal.
    BeforeCommit(RuntimeErrorEnvelopeV1),
    /// The original terminal outcome is already immutable and must remain authoritative.
    AfterCommit { error: RuntimeErrorEnvelopeV1, stage: &'static str },
}

/// Run-owned state adapter that retains all payload-bearing phase material.
///
/// M024 constructs this adapter beside its provider turn state. The canonical
/// wrapper below invokes M017 services while this boundary supplies requests,
/// interprets retained provider responses, and stores opaque results.
pub(crate) trait EmbeddedAttemptHostState: Send + Sync {
    fn context_request<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
    ) -> HarnessFuture<'a, Result<ContextAssemblyRequest, RuntimeErrorEnvelopeV1>>;

    fn accept_context<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
        result: ContextAssemblyResult,
    ) -> HarnessFuture<'a, Result<(), RuntimeErrorEnvelopeV1>>;

    fn provider_request<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
    ) -> HarnessFuture<'a, Result<ProviderCallRequest, RuntimeErrorEnvelopeV1>>;

    fn project_provider_result<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
        result: ProviderCallResult,
    ) -> HarnessFuture<'a, Result<EmbeddedProviderTurn, RuntimeErrorEnvelopeV1>>;

    fn compaction_request<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
    ) -> HarnessFuture<'a, Result<CompactionRequest, RuntimeErrorEnvelopeV1>>;

    fn accept_compaction<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
        result: CompactionResult,
    ) -> HarnessFuture<'a, Result<(), RuntimeErrorEnvelopeV1>>;

    fn accept_tool_projection<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
        projection: ToolResultProjection,
    ) -> HarnessFuture<'a, Result<(), RuntimeErrorEnvelopeV1>>;

    fn accept_tool_denial<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
        proposal_id: RuntimeToolProposalId,
        reason_code: &'static str,
    ) -> HarnessFuture<'a, Result<(), RuntimeErrorEnvelopeV1>>;

    fn verify<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
    ) -> HarnessFuture<'a, Result<(), RuntimeErrorEnvelopeV1>>;

    fn finalization_request<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
        outcome: &'a HarnessTerminalOutcome,
    ) -> HarnessFuture<'a, Result<FinalizationRequest, RuntimeErrorEnvelopeV1>>;

    fn accept_finalization<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
        receipt: FinalizationReceipt,
    ) -> HarnessFuture<'a, Result<(), RuntimeErrorEnvelopeV1>>;

    fn delivery_plan<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
        outcome: &'a HarnessTerminalOutcome,
    ) -> HarnessFuture<'a, Result<EmbeddedDeliveryPlan, RuntimeErrorEnvelopeV1>>;

    fn accept_delivery<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
        result: DeliveryResult,
    ) -> HarnessFuture<'a, Result<HarnessDeliveryBinding, RuntimeErrorEnvelopeV1>>;

    fn kernel_failure(&self, reason_code: &'static str) -> RuntimeErrorEnvelopeV1;
}

/// Host-owned non-tool phase boundary used by the concrete embedded driver.
///
/// Implementations may delegate to the canonical context, provider,
/// compaction, verification, and finalization services. They return only
/// normalized, redacted contracts and never expose credentials or raw payloads.
pub(crate) trait EmbeddedAttemptPhaseServices: Send + Sync {
    fn assemble_context<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
    ) -> HarnessFuture<'a, Result<(), RuntimeErrorEnvelopeV1>>;

    fn provider_turn<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
    ) -> HarnessFuture<'a, Result<EmbeddedProviderTurn, RuntimeErrorEnvelopeV1>>;

    fn compact<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
    ) -> HarnessFuture<'a, Result<(), RuntimeErrorEnvelopeV1>>;

    fn accept_tool_projection<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
        projection: ToolResultProjection,
    ) -> HarnessFuture<'a, Result<(), RuntimeErrorEnvelopeV1>>;

    fn accept_tool_denial<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
        proposal_id: RuntimeToolProposalId,
        reason_code: &'static str,
    ) -> HarnessFuture<'a, Result<(), RuntimeErrorEnvelopeV1>>;

    fn verify<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
    ) -> HarnessFuture<'a, Result<(), RuntimeErrorEnvelopeV1>>;

    fn finalize<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
        outcome: &'a HarnessTerminalOutcome,
    ) -> HarnessFuture<'a, Result<EmbeddedDeliveryOutcome, EmbeddedFinalizationError>>;

    /// Classifies a fail-closed kernel/tool contract error without raw evidence.
    fn kernel_failure(&self, reason_code: &'static str) -> RuntimeErrorEnvelopeV1;
}

/// Production adapter over the canonical M017 phase services.
struct CanonicalEmbeddedAttemptPhaseServices {
    context: Arc<RuntimeKernelContext>,
    state: Arc<dyn EmbeddedAttemptHostState>,
}

impl CanonicalEmbeddedAttemptPhaseServices {
    fn new(context: Arc<RuntimeKernelContext>, state: Arc<dyn EmbeddedAttemptHostState>) -> Self {
        Self { context, state }
    }

    fn failure(&self, reason_code: &'static str) -> RuntimeErrorEnvelopeV1 {
        self.state.kernel_failure(reason_code)
    }
}

impl EmbeddedAttemptPhaseServices for CanonicalEmbeddedAttemptPhaseServices {
    fn assemble_context<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
    ) -> HarnessFuture<'a, Result<(), RuntimeErrorEnvelopeV1>> {
        Box::pin(async move {
            let payload = self.state.context_request(request).await?;
            let input = self
                .context
                .phase_input(payload)
                .map_err(|error| self.failure(error.reason_code()))?;
            let result = self
                .context
                .services()
                .turn()
                .context_assembly()
                .execute(input)
                .await
                .map_err(|error| self.failure(error.reason_code()))?
                .into_payload()
                .map_err(|error| self.failure(error.reason_code()))?;
            self.state.accept_context(request, result).await
        })
    }

    fn provider_turn<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
    ) -> HarnessFuture<'a, Result<EmbeddedProviderTurn, RuntimeErrorEnvelopeV1>> {
        Box::pin(async move {
            let payload = self.state.provider_request(request).await?;
            let input = self
                .context
                .phase_input(payload)
                .map_err(|error| self.failure(error.reason_code()))?;
            let result = self
                .context
                .services()
                .turn()
                .provider_call()
                .execute(input)
                .await
                .map_err(|error| self.failure(error.reason_code()))?
                .into_payload()
                .map_err(|error| self.failure(error.reason_code()))?;
            self.state.project_provider_result(request, result).await
        })
    }

    fn compact<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
    ) -> HarnessFuture<'a, Result<(), RuntimeErrorEnvelopeV1>> {
        Box::pin(async move {
            let payload = self.state.compaction_request(request).await?;
            let input = self
                .context
                .phase_input(payload)
                .map_err(|error| self.failure(error.reason_code()))?;
            let result = self
                .context
                .services()
                .lifecycle()
                .compaction()
                .execute(input)
                .await
                .map_err(|error| self.failure(error.reason_code()))?
                .into_payload()
                .map_err(|error| self.failure(error.reason_code()))?;
            self.state.accept_compaction(request, result).await
        })
    }

    fn accept_tool_projection<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
        projection: ToolResultProjection,
    ) -> HarnessFuture<'a, Result<(), RuntimeErrorEnvelopeV1>> {
        self.state.accept_tool_projection(request, projection)
    }

    fn accept_tool_denial<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
        proposal_id: RuntimeToolProposalId,
        reason_code: &'static str,
    ) -> HarnessFuture<'a, Result<(), RuntimeErrorEnvelopeV1>> {
        self.state.accept_tool_denial(request, proposal_id, reason_code)
    }

    fn verify<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
    ) -> HarnessFuture<'a, Result<(), RuntimeErrorEnvelopeV1>> {
        self.state.verify(request)
    }

    fn finalize<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
        outcome: &'a HarnessTerminalOutcome,
    ) -> HarnessFuture<'a, Result<EmbeddedDeliveryOutcome, EmbeddedFinalizationError>> {
        Box::pin(async move {
            let payload = self
                .state
                .finalization_request(request, outcome)
                .await
                .map_err(EmbeddedFinalizationError::BeforeCommit)?;
            let input = self.context.phase_input(payload).map_err(|error| {
                EmbeddedFinalizationError::BeforeCommit(self.failure(error.reason_code()))
            })?;
            let receipt = self
                .context
                .services()
                .lifecycle()
                .finalization()
                .execute(input)
                .await
                .map_err(|error| {
                    EmbeddedFinalizationError::BeforeCommit(self.failure(error.reason_code()))
                })?
                .into_payload()
                .map_err(|error| {
                    EmbeddedFinalizationError::BeforeCommit(self.failure(error.reason_code()))
                })?;
            self.state.accept_finalization(request, receipt).await.map_err(|error| {
                EmbeddedFinalizationError::AfterCommit { error, stage: "accept_finalization" }
            })?;
            if !matches!(outcome, HarnessTerminalOutcome::Completed) {
                return Ok(EmbeddedDeliveryOutcome::NotApplicable);
            }
            match self.state.delivery_plan(request, outcome).await.map_err(|error| {
                EmbeddedFinalizationError::AfterCommit { error, stage: "delivery_plan" }
            })? {
                EmbeddedDeliveryPlan::Commit(payload) => {
                    let input = self.context.phase_input(payload).map_err(|error| {
                        EmbeddedFinalizationError::AfterCommit {
                            error: self.failure(error.reason_code()),
                            stage: "delivery_input",
                        }
                    })?;
                    let result = self
                        .context
                        .services()
                        .lifecycle()
                        .delivery()
                        .execute(input)
                        .await
                        .map_err(|error| EmbeddedFinalizationError::AfterCommit {
                            error: self.failure(error.reason_code()),
                            stage: "delivery_execute",
                        })?
                        .into_payload()
                        .map_err(|error| EmbeddedFinalizationError::AfterCommit {
                            error: self.failure(error.reason_code()),
                            stage: "delivery_output",
                        })?;
                    let committed_intent_id = result.delivery_intent_id.clone();
                    let binding =
                        self.state.accept_delivery(request, result).await.map_err(|error| {
                            EmbeddedFinalizationError::AfterCommit {
                                error,
                                stage: "accept_delivery",
                            }
                        })?;
                    if binding.delivery_intent_id != committed_intent_id {
                        return Err(EmbeddedFinalizationError::AfterCommit {
                            error: self.failure("runtime.delivery.intent_binding_mismatch"),
                            stage: "intent_binding",
                        });
                    }
                    Ok(EmbeddedDeliveryOutcome::Committed(binding))
                }
                EmbeddedDeliveryPlan::Skip { evidence } => {
                    Ok(EmbeddedDeliveryOutcome::Skipped(evidence))
                }
            }
        })
    }

    fn kernel_failure(&self, reason_code: &'static str) -> RuntimeErrorEnvelopeV1 {
        self.failure(reason_code)
    }
}

/// Concrete neutral driver over canonical phase services and tool authority.
pub(crate) struct RunStreamEmbeddedAttemptDriver {
    context: Arc<RuntimeKernelContext>,
    phases: Arc<dyn EmbeddedAttemptPhaseServices>,
}

impl std::fmt::Debug for RunStreamEmbeddedAttemptDriver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("RunStreamEmbeddedAttemptDriver").finish_non_exhaustive()
    }
}

impl RunStreamEmbeddedAttemptDriver {
    fn new(
        context: Arc<RuntimeKernelContext>,
        phases: Arc<dyn EmbeddedAttemptPhaseServices>,
    ) -> Self {
        Self { context, phases }
    }

    async fn emit(
        request: &HarnessAttemptRequest,
        events: &mut dyn EmbeddedHarnessEventPort,
        sequence: &mut u64,
        kind: HarnessEventKind,
    ) -> Result<(), EmbeddedAttemptError> {
        *sequence = sequence
            .checked_add(1)
            .ok_or(EmbeddedAttemptError::Contract(HarnessContractError::InvalidEvent))?;
        events
            .emit(HarnessEvent { generation: request.generation(), sequence: *sequence, kind })
            .await
            .map_err(EmbeddedAttemptError::Contract)
    }

    fn terminal(
        request: &HarnessAttemptRequest,
        sequence: u64,
        outcome: HarnessTerminalOutcome,
    ) -> Option<HarnessAttemptTerminal> {
        Some(HarnessAttemptTerminal {
            generation: request.generation(),
            sequence: sequence.checked_add(1)?,
            outcome,
        })
    }

    async fn drive_tool(
        &self,
        request: &HarnessAttemptRequest,
        events: &mut dyn EmbeddedHarnessEventPort,
        sequence: &mut u64,
        proposal: ToolProposalRequest,
        operation_id: RuntimeOperationId,
    ) -> Result<(), EmbeddedAttemptError> {
        let proposal_id = proposal.proposal_id().clone();
        Self::emit(
            request,
            events,
            sequence,
            HarnessEventKind::ToolProposed { proposal_id: proposal_id.clone() },
        )
        .await?;
        let input = self
            .context
            .phase_input(proposal)
            .map_err(|error| self.kernel_error(error.reason_code()))?;
        let decision = self
            .context
            .services()
            .tool_authority()
            .gate(input)
            .await
            .map_err(|error| self.kernel_error(error.reason_code()))?
            .into_payload()
            .map_err(|error| self.kernel_error(error.reason_code()))?;
        let grant = match decision {
            ToolGateDecision::Granted(grant) => grant,
            ToolGateDecision::ApprovalRequired(pending) => {
                let approval_id = pending.approval_subject().approval_subject_id().clone();
                Self::emit(
                    request,
                    events,
                    sequence,
                    HarnessEventKind::ApprovalRequired {
                        proposal_id: proposal_id.clone(),
                        approval_id: approval_id.clone(),
                    },
                )
                .await?;
                let input = self
                    .context
                    .phase_input(ApprovalWaitResumeRequest { pending, resume_evidence: None })
                    .map_err(|error| self.kernel_error(error.reason_code()))?;
                match self
                    .context
                    .services()
                    .tool_authority()
                    .wait_or_resume_approval(input)
                    .await
                    .map_err(|error| self.kernel_error(error.reason_code()))?
                    .into_payload()
                    .map_err(|error| self.kernel_error(error.reason_code()))?
                {
                    ApprovalWaitResumeResult::Granted(grant) => {
                        Self::emit(
                            request,
                            events,
                            sequence,
                            HarnessEventKind::ApprovalResolved {
                                proposal_id: proposal_id.clone(),
                                approval_id,
                                resolution: HarnessApprovalResolution::Approved,
                                evidence_id: None,
                                evidence_sha256: None,
                            },
                        )
                        .await?;
                        grant
                    }
                    ApprovalWaitResumeResult::Denied { evidence } => {
                        Self::emit(
                            request,
                            events,
                            sequence,
                            HarnessEventKind::ApprovalResolved {
                                proposal_id: proposal_id.clone(),
                                approval_id,
                                resolution: HarnessApprovalResolution::Denied,
                                evidence_id: Some(evidence.id().clone()),
                                evidence_sha256: Some(*evidence.sha256()),
                            },
                        )
                        .await?;
                        self.phases
                            .accept_tool_denial(
                                request,
                                proposal_id,
                                "runtime.tool.approval_denied",
                            )
                            .await
                            .map_err(EmbeddedAttemptError::Terminal)?;
                        return Ok(());
                    }
                    ApprovalWaitResumeResult::TimedOut { evidence } => {
                        Self::emit(
                            request,
                            events,
                            sequence,
                            HarnessEventKind::ApprovalResolved {
                                proposal_id: proposal_id.clone(),
                                approval_id,
                                resolution: HarnessApprovalResolution::Expired,
                                evidence_id: Some(evidence.id().clone()),
                                evidence_sha256: Some(*evidence.sha256()),
                            },
                        )
                        .await?;
                        self.phases
                            .accept_tool_denial(
                                request,
                                proposal_id,
                                "runtime.tool.approval_expired",
                            )
                            .await
                            .map_err(EmbeddedAttemptError::Terminal)?;
                        return Ok(());
                    }
                    ApprovalWaitResumeResult::Pending(pending) => {
                        if pending.proposal_id() != &proposal_id
                            || pending.approval_subject().approval_subject_id() != &approval_id
                        {
                            return Err(EmbeddedAttemptError::Contract(
                                HarnessContractError::InvalidEvent,
                            ));
                        }
                        return Err(EmbeddedAttemptError::Contract(
                            HarnessContractError::InvalidEvent,
                        ));
                    }
                }
            }
            ToolGateDecision::Denied { evidence } => {
                Self::emit(
                    request,
                    events,
                    sequence,
                    HarnessEventKind::ToolDenied {
                        proposal_id: proposal_id.clone(),
                        evidence_id: evidence.id().clone(),
                        evidence_sha256: *evidence.sha256(),
                    },
                )
                .await?;
                self.phases
                    .accept_tool_denial(request, proposal_id, "runtime.tool.policy_denied")
                    .await
                    .map_err(EmbeddedAttemptError::Terminal)?;
                return Ok(());
            }
        };

        let execution_id = grant.execution_id().clone();
        let lane_authority = grant.lane_authority().clone();
        Self::emit(
            request,
            events,
            sequence,
            HarnessEventKind::ToolExecutionStarted {
                proposal_id: proposal_id.clone(),
                execution_id: execution_id.clone(),
                operation_id: operation_id.clone(),
            },
        )
        .await?;
        let execution = ToolExecutionRequest::new(grant, &lane_authority)
            .map_err(|error| self.kernel_error(error.reason_code()))?;
        let input = self
            .context
            .phase_input(execution)
            .map_err(|error| self.kernel_error(error.reason_code()))?;
        let receipt = self
            .context
            .services()
            .tool_authority()
            .execute(input)
            .await
            .map_err(|error| self.kernel_error(error.reason_code()))?
            .into_payload()
            .map_err(|error| self.kernel_error(error.reason_code()))?;
        let input = self
            .context
            .phase_input(ToolResultProjectionRequest { receipt })
            .map_err(|error| self.kernel_error(error.reason_code()))?;
        let projection = self
            .context
            .services()
            .tool_authority()
            .project_result(input)
            .await
            .map_err(|error| self.kernel_error(error.reason_code()))?
            .into_payload()
            .map_err(|error| self.kernel_error(error.reason_code()))?;
        Self::emit(
            request,
            events,
            sequence,
            HarnessEventKind::ToolResultObserved { proposal_id, execution_id, operation_id },
        )
        .await?;
        self.phases
            .accept_tool_projection(request, projection)
            .await
            .map_err(EmbeddedAttemptError::Terminal)
    }

    fn kernel_error(&self, reason_code: &'static str) -> EmbeddedAttemptError {
        EmbeddedAttemptError::Terminal(self.phases.kernel_failure(reason_code))
    }
}

impl EmbeddedAttemptDriver for RunStreamEmbeddedAttemptDriver {
    fn run_attempt<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
        events: &'a mut dyn EmbeddedHarnessEventPort,
    ) -> HarnessFuture<'a, Result<HarnessAttemptTerminal, EmbeddedAttemptError>> {
        Box::pin(async move {
            self.phases.assemble_context(request).await.map_err(EmbeddedAttemptError::Terminal)?;
            let mut sequence = 1;
            let mut provider_start_already_committed = false;
            for _ in 0..MAX_EMBEDDED_PROVIDER_TURNS {
                if provider_start_already_committed {
                    provider_start_already_committed = false;
                } else {
                    Self::emit(
                        request,
                        events,
                        &mut sequence,
                        HarnessEventKind::ProviderCallStarted,
                    )
                    .await?;
                }
                match self
                    .phases
                    .provider_turn(request)
                    .await
                    .map_err(EmbeddedAttemptError::Terminal)?
                {
                    EmbeddedProviderTurn::Completed {
                        text_utf8_bytes,
                        prompt_tokens,
                        completion_tokens,
                    } => {
                        if text_utf8_bytes > 0 {
                            let utf8_bytes = u32::try_from(text_utf8_bytes).map_err(|_| {
                                EmbeddedAttemptError::Contract(HarnessContractError::InvalidEvent)
                            })?;
                            Self::emit(
                                request,
                                events,
                                &mut sequence,
                                HarnessEventKind::TextDelta { utf8_bytes },
                            )
                            .await?;
                        }
                        if prompt_tokens > 0 || completion_tokens > 0 {
                            Self::emit(
                                request,
                                events,
                                &mut sequence,
                                HarnessEventKind::Usage { prompt_tokens, completion_tokens },
                            )
                            .await?;
                        }
                        Self::emit(
                            request,
                            events,
                            &mut sequence,
                            HarnessEventKind::ProviderCallCompleted,
                        )
                        .await?;
                        Self::emit(
                            request,
                            events,
                            &mut sequence,
                            HarnessEventKind::VerificationStarted,
                        )
                        .await?;
                        let outcome = match self.phases.verify(request).await {
                            Ok(()) => {
                                Self::emit(
                                    request,
                                    events,
                                    &mut sequence,
                                    HarnessEventKind::VerificationPassed,
                                )
                                .await?;
                                HarnessTerminalOutcome::Completed
                            }
                            Err(error) => {
                                Self::emit(
                                    request,
                                    events,
                                    &mut sequence,
                                    HarnessEventKind::VerificationFailed { error: error.clone() },
                                )
                                .await?;
                                HarnessTerminalOutcome::Failed { error }
                            }
                        };
                        return Self::terminal(request, sequence, outcome).ok_or(
                            EmbeddedAttemptError::Contract(HarnessContractError::InvalidTerminal),
                        );
                    }
                    EmbeddedProviderTurn::Tool { proposal, operation_id } => {
                        self.drive_tool(request, events, &mut sequence, proposal, operation_id)
                            .await?;
                    }
                    EmbeddedProviderTurn::CompactionRequired => {
                        Self::emit(
                            request,
                            events,
                            &mut sequence,
                            HarnessEventKind::CompactionRequired,
                        )
                        .await?;
                        self.phases
                            .compact(request)
                            .await
                            .map_err(EmbeddedAttemptError::Terminal)?;
                        Self::emit(
                            request,
                            events,
                            &mut sequence,
                            HarnessEventKind::CompactionCompleted,
                        )
                        .await?;
                        // CompactionCompleted is itself the durable transition
                        // back into CallingProvider for the retry iteration.
                        provider_start_already_committed = true;
                    }
                    EmbeddedProviderTurn::Cancelled { reason_code } => {
                        Self::emit(
                            request,
                            events,
                            &mut sequence,
                            HarnessEventKind::CancellationObserved,
                        )
                        .await?;
                        let outcome = HarnessTerminalOutcome::Cancelled { reason_code };
                        return Self::terminal(request, sequence, outcome).ok_or(
                            EmbeddedAttemptError::Contract(HarnessContractError::InvalidTerminal),
                        );
                    }
                }
            }
            Err(EmbeddedAttemptError::Contract(HarnessContractError::EventLimitExceeded))
        })
    }

    fn finalize_attempt<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
        mut terminal: HarnessAttemptTerminal,
        events: &'a mut dyn EmbeddedHarnessEventPort,
    ) -> HarnessFuture<'a, Result<HarnessAttemptTerminal, EmbeddedAttemptError>> {
        Box::pin(async move {
            let mut sequence = terminal
                .sequence
                .checked_sub(1)
                .ok_or(EmbeddedAttemptError::Contract(HarnessContractError::InvalidTerminal))?;
            Self::emit(request, events, &mut sequence, HarnessEventKind::FinalizationReady).await?;
            terminal.sequence = sequence
                .checked_add(1)
                .ok_or(EmbeddedAttemptError::Contract(HarnessContractError::InvalidTerminal))?;
            let delivery = self.phases.finalize(request, &terminal.outcome).await;
            let delivery = match delivery {
                Ok(delivery) => delivery,
                Err(EmbeddedFinalizationError::BeforeCommit(error)) => {
                    return Err(EmbeddedAttemptError::Terminal(error));
                }
                Err(EmbeddedFinalizationError::AfterCommit { error, stage }) => {
                    return Err(EmbeddedAttemptError::PostFinalization { terminal, error, stage });
                }
            };
            if matches!(terminal.outcome, HarnessTerminalOutcome::Completed) {
                let event = match delivery {
                    EmbeddedDeliveryOutcome::Committed(binding) => {
                        HarnessEventKind::DeliveryIntentCommitted {
                            delivery_intent_id: binding.delivery_intent_id,
                            operation_id: binding.operation_id,
                            output_event_id: binding.output_event_id,
                        }
                    }
                    EmbeddedDeliveryOutcome::Skipped(evidence) => {
                        HarnessEventKind::DeliverySkipped {
                            evidence_id: evidence.id().clone(),
                            evidence_sha256: *evidence.sha256(),
                        }
                    }
                    EmbeddedDeliveryOutcome::NotApplicable => {
                        return Err(EmbeddedAttemptError::Contract(
                            HarnessContractError::InvalidTerminal,
                        ));
                    }
                };
                Self::emit(request, events, &mut sequence, event).await?;
                terminal.sequence = sequence
                    .checked_add(1)
                    .ok_or(EmbeddedAttemptError::Contract(HarnessContractError::InvalidTerminal))?;
            }
            Ok(terminal)
        })
    }
}

/// Builder that creates the sole live gateway and its paired run-owned pump.
pub(crate) struct ProductionEmbeddedAttemptFactory {
    tool_authority: Arc<dyn ToolAuthorityGateway>,
    owner: RunStreamLiveToolFlowOwner,
}

impl ProductionEmbeddedAttemptFactory {
    /// Creates one generation-pinned live tool authority pair.
    pub(crate) fn new(
        lane_authority: PhaseLaneAuthority,
    ) -> Result<Self, KernelPhaseContractError> {
        let (tool_authority, owner) = run_stream_live_tool_authority(lane_authority)?;
        Ok(Self { tool_authority, owner })
    }

    /// Returns the gateway that must be installed in the matching kernel context.
    pub(crate) fn tool_authority(&self) -> Arc<dyn ToolAuthorityGateway> {
        Arc::clone(&self.tool_authority)
    }

    /// Returns the capability used by provider callbacks to retain raw proposals.
    pub(crate) fn proposal_retention(&self) -> super::tool_flow::RunStreamToolProposalRetention {
        self.owner.proposal_retention()
    }

    /// Seals the factory into an executable adapter plus its mailbox owner.
    pub(crate) fn build(
        self,
        context: Arc<RuntimeKernelContext>,
        state: Arc<dyn EmbeddedAttemptHostState>,
    ) -> ProductionEmbeddedAttempt {
        let phases =
            Arc::new(CanonicalEmbeddedAttemptPhaseServices::new(Arc::clone(&context), state));
        ProductionEmbeddedAttempt {
            runtime: EmbeddedHarnessAdapter::new(RunStreamEmbeddedAttemptDriver::new(
                context, phases,
            )),
            owner: self.owner,
        }
    }
}

/// Production adapter that pumps live tool commands while the harness awaits.
pub(crate) struct ProductionEmbeddedAttempt {
    runtime: EmbeddedHarnessAdapter<RunStreamEmbeddedAttemptDriver>,
    owner: RunStreamLiveToolFlowOwner,
}

impl ProductionEmbeddedAttempt {
    /// Runs one attempt while the run-owned live tool host services gateway work.
    pub(crate) async fn drive(
        &mut self,
        host: &mut RunStreamLiveToolHost<'_>,
        request: &HarnessAttemptRequest,
        sink: &mut dyn HarnessEventSink,
    ) -> Result<HarnessTerminalReceipt, HarnessContractError> {
        let future = self.runtime.run_attempt(request, sink);
        self.owner
            .drive_until(host, future)
            .await
            .map_err(|_| HarnessContractError::InvalidEvent)?
    }
}
