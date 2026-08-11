// Concrete run-owned mailbox for RuntimeKernelV2 tool authority.
//
// The sendable port contains no raw payloads. Its paired owner is serviced by
// the run-stream task that already owns the gRPC stream, tape cursor, budgets,
// and live cancellation scope.

use sha2::{Digest as _, Sha256};
use tokio::sync::oneshot;

/// Raw proposal retained only by the run-stream owner.
#[derive(Debug, Clone)]
pub(crate) struct RunStreamRetainedToolProposal {
    /// Model proposal identity.
    pub(crate) proposal_id: String,
    /// Model-visible catalog name.
    pub(crate) tool_name: String,
    /// Untrusted raw input to validate and normalize at the gate.
    pub(crate) input_json: Vec<u8>,
    /// Immutable catalog snapshot shown to the model.
    pub(crate) catalog: ModelVisibleToolCatalogSnapshot,
}

/// Borrowed live state used to service one mailbox command.
pub(crate) struct RunStreamLiveToolHost<'a> {
    pub(crate) sender: &'a RunStreamProgressSender,
    pub(crate) stream:
        &'a mut Streaming<crate::transport::grpc::proto::palyra::common::v1::RunStreamRequest>,
    pub(crate) runtime_state: &'a Arc<GatewayRuntimeState>,
    pub(crate) request_context: &'a RequestContext,
    pub(crate) active_session_id: Option<&'a str>,
    pub(crate) session_id: &'a str,
    pub(crate) run_id: &'a str,
    pub(crate) remaining_tool_budget: &'a mut u32,
    pub(crate) approval_cache_generation: Option<u64>,
    pub(crate) flow_control: &'a RunStreamFlowControl,
    pub(crate) tape_seq: &'a mut i64,
}

/// Sendable port injected into [`LiveToolAuthorityGateway`].
#[derive(Debug, Clone)]
pub(crate) struct RunStreamLiveToolFlowPort {
    sender: mpsc::Sender<RunStreamLiveToolCommand>,
}

/// Run-owned receiver and host-side retained payload stores.
pub(crate) struct RunStreamLiveToolFlowOwner {
    lane_authority: crate::application::runtime_kernel_v2::phases::PhaseLaneAuthority,
    receiver: mpsc::Receiver<RunStreamLiveToolCommand>,
    proposals: Arc<std::sync::Mutex<BTreeMap<String, RetainedProposalEntry>>>,
    pending: BTreeMap<String, PendingProposalEntry>,
    prepared: BTreeMap<String, PreparedProposalEntry>,
    raw_outcomes: BTreeMap<String, RetainedRawOutcome>,
    projected: Arc<std::sync::Mutex<BTreeMap<String, RetainedProjectedEntry>>>,
    evidence: BTreeMap<String, String>,
}

/// Cloneable host capability for retaining provider-produced tool proposals.
///
/// Raw proposal bytes remain behind the run-owned mailbox boundary. The
/// callback receives only this capability and returns opaque kernel refs.
#[derive(Clone)]
pub(crate) struct RunStreamToolProposalRetention {
    proposals: Arc<std::sync::Mutex<BTreeMap<String, RetainedProposalEntry>>>,
    projected: Arc<std::sync::Mutex<BTreeMap<String, RetainedProjectedEntry>>>,
}

struct RetainedProposalEntry {
    reference: LiveToolHostRef,
    proposal: RunStreamRetainedToolProposal,
}

struct PendingProposalEntry {
    prepared_gate: RunStreamToolGatePreparation,
    execution_tool_name: String,
    execution_input_json: Vec<u8>,
    replay_safety_class: ToolReplaySafetyClass,
    expected_dynamic_provenance: Option<String>,
    retained_proposal: crate::application::runtime_kernel_v2::phases::RetainedToolProposalRef,
    authority_class: crate::application::runtime_kernel_v2::phases::ToolAuthorityClass,
}

struct PreparedProposalEntry {
    prepared: RunStreamPreparedToolExecution,
    retained_proposal: crate::application::runtime_kernel_v2::phases::RetainedToolProposalRef,
    authority_class: crate::application::runtime_kernel_v2::phases::ToolAuthorityClass,
    grant_evidence: LiveToolHostRef,
    approval_subject_id: Option<palyra_common::runtime_contracts::RuntimeApprovalSubjectId>,
}

struct FinishGateRequest {
    proposal_id: palyra_common::runtime_contracts::RuntimeToolProposalId,
    tool_name: String,
    input_json: Vec<u8>,
    replay_safety_class: ToolReplaySafetyClass,
    retained_proposal: crate::application::runtime_kernel_v2::phases::RetainedToolProposalRef,
    authority_class: crate::application::runtime_kernel_v2::phases::ToolAuthorityClass,
    approval_subject_id: Option<palyra_common::runtime_contracts::RuntimeApprovalSubjectId>,
    preparation: RunStreamToolProposalPreparation,
    stage: LiveToolFlowStage,
}

struct RetainedRawOutcome {
    execution_id: palyra_common::runtime_contracts::RuntimeToolExecutionId,
    prepared: RunStreamPreparedToolExecution,
    outcome: ToolExecutionOutcome,
    side_effect_fence: Option<ActiveToolSideEffectFence>,
    side_effect_state: Option<SideEffectFenceState>,
    execution_evidence: Vec<LiveToolHostRef>,
}

struct RetainedProjectedEntry {
    reference: LiveToolHostRef,
    proposal_id: palyra_common::runtime_contracts::RuntimeToolProposalId,
    execution_id: palyra_common::runtime_contracts::RuntimeToolExecutionId,
    outcome: ToolExecutionOutcome,
}

enum RunStreamLiveToolCommand {
    Gate {
        request: LiveToolGateRequest,
        response: oneshot::Sender<Result<LiveToolGateResult, LiveToolFlowError>>,
    },
    Approval {
        request: LiveToolApprovalRequest,
        response: oneshot::Sender<Result<LiveToolApprovalResult, LiveToolFlowError>>,
    },
    Execute {
        request: LiveToolExecutionRequest,
        response: oneshot::Sender<Result<LiveToolExecutionResult, LiveToolFlowError>>,
    },
    Project {
        request: LiveToolProjectionRequest,
        response: oneshot::Sender<Result<LiveToolProjectionResult, LiveToolFlowError>>,
    },
    #[cfg(test)]
    Probe { response: oneshot::Sender<()> },
}

/// Creates one generation-bound port/owner pair for an active run.
#[must_use]
pub(crate) fn run_stream_live_tool_flow(
    lane_authority: crate::application::runtime_kernel_v2::phases::PhaseLaneAuthority,
) -> (Arc<RunStreamLiveToolFlowPort>, RunStreamLiveToolFlowOwner) {
    let (sender, receiver) = mpsc::channel(8);
    let proposals = Arc::new(std::sync::Mutex::new(BTreeMap::new()));
    let projected = Arc::new(std::sync::Mutex::new(BTreeMap::new()));
    (
        Arc::new(RunStreamLiveToolFlowPort { sender }),
        RunStreamLiveToolFlowOwner {
            lane_authority,
            receiver,
            proposals,
            pending: BTreeMap::new(),
            prepared: BTreeMap::new(),
            raw_outcomes: BTreeMap::new(),
            projected,
            evidence: BTreeMap::new(),
        },
    )
}

/// Creates the live gateway and its paired run-owned mailbox.
///
/// This is the production injection seam for the RuntimeKernelV2 context. The
/// caller keeps `owner` beside the active run and services it while the kernel
/// awaits gateway phases.
pub(crate) fn run_stream_live_tool_authority(
    lane_authority: crate::application::runtime_kernel_v2::phases::PhaseLaneAuthority,
) -> Result<
    (
        Arc<dyn crate::application::runtime_kernel_v2::phases::ToolAuthorityGateway>,
        RunStreamLiveToolFlowOwner,
    ),
    crate::application::runtime_kernel_v2::phases::KernelPhaseContractError,
> {
    let (port, owner) = run_stream_live_tool_flow(lane_authority.clone());
    let gateway = crate::application::runtime_kernel_v2::phases::LiveToolAuthorityGateway::new(
        lane_authority,
        port,
    )?;
    Ok((Arc::new(gateway), owner))
}

impl RunStreamLiveToolFlowOwner {
    /// Returns a cloneable proposal-retention capability for provider callbacks.
    #[must_use]
    pub(crate) fn proposal_retention(&self) -> RunStreamToolProposalRetention {
        RunStreamToolProposalRetention {
            proposals: Arc::clone(&self.proposals),
            projected: Arc::clone(&self.projected),
        }
    }

    /// Services one pending command with the live state owned by the run task.
    ///
    /// Returns `false` after all senders are dropped.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn serve_next(
        &mut self,
        host: &mut RunStreamLiveToolHost<'_>,
    ) -> Result<bool, Status> {
        let Some(command) = self.receiver.recv().await else {
            return Ok(false);
        };
        match command {
            RunStreamLiveToolCommand::Gate { request, response } => {
                let _ = response.send(self.serve_gate(host, request).await);
            }
            RunStreamLiveToolCommand::Approval { request, response } => {
                let _ = response.send(self.serve_approval(host, request).await);
            }
            RunStreamLiveToolCommand::Execute { request, response } => {
                let _ = response.send(self.serve_execute(host, request).await);
            }
            RunStreamLiveToolCommand::Project { request, response } => {
                let _ = response.send(self.serve_projection(host, request).await);
            }
            #[cfg(test)]
            RunStreamLiveToolCommand::Probe { response } => {
                let _ = response.send(());
            }
        }
        Ok(true)
    }

    /// Pumps mailbox commands while a kernel operation awaits its gateway.
    ///
    /// Awaiting the gateway future without this driver would deadlock because
    /// the run task itself owns every live stage dependency.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn drive_until<F>(
        &mut self,
        host: &mut RunStreamLiveToolHost<'_>,
        future: F,
    ) -> Result<F::Output, Status>
    where
        F: Future,
    {
        tokio::pin!(future);
        loop {
            tokio::select! {
                output = &mut future => return Ok(output),
                served = self.serve_next(host) => {
                    if !served? {
                        return Err(Status::unavailable(
                            "live tool-flow mailbox closed before gateway completion",
                        ));
                    }
                }
            }
        }
    }

    #[allow(clippy::result_large_err)]
    async fn serve_gate(
        &mut self,
        host: &mut RunStreamLiveToolHost<'_>,
        request: LiveToolGateRequest,
    ) -> Result<LiveToolGateResult, LiveToolFlowError> {
        self.validate_lane(&request.lane_authority, LiveToolFlowStage::Gate)?;
        let entry = {
            let mut proposals =
                self.proposals.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            proposals.remove(request.retained_proposal.id().as_str())
        };
        let Some(entry) = entry else {
            return Err(self.failure(LiveToolFlowStage::Gate, "tool_gate.proposal_not_retained"));
        };
        if entry.reference.sha256 != *request.retained_proposal.sha256()
            || entry.proposal.proposal_id != request.proposal_id.as_str()
            || entry.proposal.tool_name != request.tool_name
        {
            return Err(
                self.failure(LiveToolFlowStage::Gate, "tool_gate.proposal_binding_mismatch")
            );
        }
        let resolved = match resolve_live_port_tool_call(&entry.proposal) {
            Ok(resolved) => resolved,
            Err(reason) => {
                return Ok(LiveToolGateResult::Denied {
                    evidence: self.required_evidence(LiveToolFlowStage::Gate, reason.as_str())?,
                });
            }
        };
        if resolved.authority_class != request.requested_authority {
            return Ok(LiveToolGateResult::Denied {
                evidence: self.required_evidence(
                    LiveToolFlowStage::Gate,
                    "tool_gate.authority_class_mismatch",
                )?,
            });
        }
        let expected_dynamic_provenance =
            match dynamic_tool_snapshot_provenance(
                &entry.proposal.catalog,
                resolved.tool_name.as_str(),
            ) {
                Ok(provenance) => provenance,
                Err(reason) => {
                    return Ok(LiveToolGateResult::Denied {
                        evidence: self.required_evidence(LiveToolFlowStage::Gate, reason)?,
                    });
                }
            };

        let prepared_gate = prepare_run_stream_tool_gate_without_approval(
            host.sender,
            host.runtime_state,
            host.request_context,
            host.active_session_id,
            host.run_id,
            request.proposal_id.as_str(),
            resolved.tool_name.as_str(),
            resolved.input_json.as_slice(),
            host.tape_seq,
        )
        .await
        .map_err(|error| self.status_failure(LiveToolFlowStage::Gate, &error))?;

        if prepared_gate.proposal_approval_required {
            let subject = palyra_common::runtime_contracts::RuntimeApprovalSubjectId::parse(
                prepared_gate.approval_subject_id.as_str(),
            )
            .map_err(|_| {
                self.failure(LiveToolFlowStage::Gate, "tool_gate.invalid_approval_subject")
            })?;
            self.pending.insert(
                request.proposal_id.as_str().to_owned(),
                PendingProposalEntry {
                    prepared_gate,
                    execution_tool_name: resolved.tool_name,
                    execution_input_json: resolved.input_json,
                    replay_safety_class: resolved.replay_safety_class,
                    expected_dynamic_provenance,
                    retained_proposal: request.retained_proposal,
                    authority_class: request.requested_authority,
                },
            );
            return Ok(LiveToolGateResult::ApprovalRequired { approval_subject_id: subject });
        }

        let mut preparation = resolve_run_stream_tool_gate_approval(
            host.sender,
            host.stream,
            host.runtime_state,
            host.request_context,
            host.session_id,
            host.run_id,
            request.proposal_id.as_str(),
            resolved.tool_name.as_str(),
            resolved.input_json.as_slice(),
            host.remaining_tool_budget,
            host.approval_cache_generation,
            host.flow_control,
            host.tape_seq,
            prepared_gate,
        )
        .await
        .map_err(|error| self.status_failure(LiveToolFlowStage::Gate, &error))?;
        preparation.expected_dynamic_provenance = expected_dynamic_provenance;
        self.finish_gate(FinishGateRequest {
            proposal_id: request.proposal_id,
            tool_name: resolved.tool_name,
            input_json: resolved.input_json,
            replay_safety_class: resolved.replay_safety_class,
            retained_proposal: request.retained_proposal,
            authority_class: request.requested_authority,
            approval_subject_id: None,
            preparation,
            stage: LiveToolFlowStage::Gate,
        })
    }
}

impl RunStreamToolProposalRetention {
    fn retain_proposal(
        &self,
        proposal: RunStreamRetainedToolProposal,
    ) -> Result<LiveToolHostRef, Status> {
        let id = runtime_operation_id("tool-proposal")?;
        let sha256 = sha256_bytes(
            &[
                proposal.proposal_id.as_bytes(),
                proposal.tool_name.as_bytes(),
                proposal.input_json.as_slice(),
                proposal.catalog.catalog_hash.as_bytes(),
            ]
            .concat(),
        );
        let reference = LiveToolHostRef { id, sha256 };
        self.proposals.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).insert(
            reference.id.as_str().to_owned(),
            RetainedProposalEntry { reference: reference.clone(), proposal },
        );
        Ok(reference)
    }

    /// Retains a proposal and returns the opaque reference accepted by the
    /// kernel tool-gate request.
    pub(crate) fn retain_kernel_proposal(
        &self,
        proposal: RunStreamRetainedToolProposal,
    ) -> Result<crate::application::runtime_kernel_v2::phases::RetainedToolProposalRef, Status>
    {
        self.retain_proposal(proposal).map(
            crate::application::runtime_kernel_v2::phases::LiveToolAuthorityGateway::retained_proposal_ref,
        )
    }

    /// Retains one provider proposal and constructs its exact kernel request.
    pub(crate) fn retain_provider_proposal(
        &self,
        proposal: RunStreamRetainedToolProposal,
    ) -> Result<crate::application::runtime_kernel_v2::phases::ToolProposalRequest, Status> {
        let resolved =
            resolve_live_port_tool_call(&proposal).map_err(Status::failed_precondition)?;
        let proposal_id =
            palyra_common::runtime_contracts::RuntimeToolProposalId::parse(&proposal.proposal_id)
                .map_err(|error| {
                Status::failed_precondition(format!(
                    "provider returned an invalid tool proposal identity: {error}"
                ))
            })?;
        let tool_name = proposal.tool_name.clone();
        let retained = self.retain_kernel_proposal(proposal)?;
        crate::application::runtime_kernel_v2::phases::ToolProposalRequest::new(
            proposal_id,
            tool_name,
            retained,
            resolved.authority_class,
        )
        .map_err(|error| Status::failed_precondition(error.to_string()))
    }

    /// Resolves exactly one model-visible projection emitted by the live gateway.
    pub(crate) fn take_model_visible_result(
        &self,
        reference: &crate::application::runtime_kernel_v2::phases::ModelVisibleToolResultRef,
        proposal_id: &palyra_common::runtime_contracts::RuntimeToolProposalId,
        execution_id: &palyra_common::runtime_contracts::RuntimeToolExecutionId,
    ) -> Option<ToolExecutionOutcome> {
        let retained = self.projected.lock().ok()?.remove(reference.id().as_str())?;
        (retained.reference.sha256 == *reference.sha256()
            && &retained.proposal_id == proposal_id
            && &retained.execution_id == execution_id)
            .then_some(retained.outcome)
    }
}

impl RunStreamLiveToolFlowOwner {
    #[allow(clippy::result_large_err)]
    async fn serve_approval(
        &mut self,
        host: &mut RunStreamLiveToolHost<'_>,
        request: LiveToolApprovalRequest,
    ) -> Result<LiveToolApprovalResult, LiveToolFlowError> {
        self.validate_lane(&request.lane_authority, LiveToolFlowStage::Approval)?;
        let Some(pending) = self.pending.remove(request.proposal_id.as_str()) else {
            // After restart the durable approval evidence may arrive before
            // the run owner has rehydrated its proposal entry. Preserve the
            // non-executable pending token instead of guessing or granting.
            if request.resume_evidence.is_some() {
                return Ok(LiveToolApprovalResult::Pending);
            }
            return Err(self.failure(LiveToolFlowStage::Approval, "approval.proposal_not_pending"));
        };
        if pending.prepared_gate.approval_subject_id != request.approval_subject_id.as_str()
            || pending.prepared_gate.resolved_session_id != host.session_id
            || pending.retained_proposal != request.retained_proposal
            || pending.authority_class != request.authority_class
        {
            return Err(
                self.failure(LiveToolFlowStage::Approval, "approval.subject_binding_mismatch")
            );
        }
        let mut preparation = resolve_run_stream_tool_gate_approval(
            host.sender,
            host.stream,
            host.runtime_state,
            host.request_context,
            host.session_id,
            host.run_id,
            request.proposal_id.as_str(),
            pending.execution_tool_name.as_str(),
            pending.execution_input_json.as_slice(),
            host.remaining_tool_budget,
            host.approval_cache_generation,
            host.flow_control,
            host.tape_seq,
            pending.prepared_gate,
        )
        .await
        .map_err(|error| self.status_failure(LiveToolFlowStage::Approval, &error))?;
        preparation.expected_dynamic_provenance = pending.expected_dynamic_provenance;
        let approval_timed_out = preparation.approval_timed_out;
        let result = self.finish_gate(FinishGateRequest {
            proposal_id: request.proposal_id,
            tool_name: pending.execution_tool_name,
            input_json: pending.execution_input_json,
            replay_safety_class: pending.replay_safety_class,
            retained_proposal: pending.retained_proposal,
            authority_class: pending.authority_class,
            approval_subject_id: Some(request.approval_subject_id),
            preparation,
            stage: LiveToolFlowStage::Approval,
        })?;
        match result {
            LiveToolGateResult::Granted { execution_id, evidence } => {
                Ok(LiveToolApprovalResult::Granted { execution_id, evidence })
            }
            LiveToolGateResult::Denied { evidence } => {
                if approval_timed_out {
                    Ok(LiveToolApprovalResult::TimedOut { evidence })
                } else {
                    Ok(LiveToolApprovalResult::Denied { evidence })
                }
            }
            LiveToolGateResult::ApprovalRequired { .. } => Ok(LiveToolApprovalResult::Pending),
        }
    }

    #[allow(clippy::result_large_err)]
    async fn serve_execute(
        &mut self,
        host: &mut RunStreamLiveToolHost<'_>,
        request: LiveToolExecutionRequest,
    ) -> Result<LiveToolExecutionResult, LiveToolFlowError> {
        self.validate_lane(&request.lane_authority, LiveToolFlowStage::Execution)?;
        let Some(granted) = self.prepared.remove(request.proposal_id.as_str()) else {
            return Err(self.failure(LiveToolFlowStage::Execution, "tool_execution.not_prepared"));
        };
        if granted.retained_proposal != request.retained_proposal
            || granted.authority_class != request.authority_class
            || &granted.grant_evidence.id != request.grant_evidence.id()
            || granted.grant_evidence.sha256 != *request.grant_evidence.sha256()
            || granted.approval_subject_id != request.approval_subject_id
        {
            return Err(
                self.failure(LiveToolFlowStage::Execution, "tool_execution.grant_binding_mismatch")
            );
        }
        let prepared = granted.prepared;
        if !prepared.decision.allowed {
            return Err(self.failure(LiveToolFlowStage::Execution, "tool_execution.not_granted"));
        }
        let nested_tool_budget = shared_tool_budget(*host.remaining_tool_budget);
        let cancellation = host
            .flow_control
            .live_child(
                CancellationScopeKind::ToolExecution,
                tool_execution_timeout(host.runtime_state, prepared.tool_name.as_str()),
            )
            .map_err(|error| self.status_failure(LiveToolFlowStage::Execution, &error))?;
        let runtime_outcome = execute_prepared_tool_runtime(PreparedToolRuntimeExecution {
            progress_sender: Some(host.sender),
            runtime_state: host.runtime_state,
            request_context: host.request_context,
            run_id: host.run_id,
            progress_tape_seq: Some(host.tape_seq),
            effect_started_tape_seq: None,
            prepared: &prepared,
            remaining_tool_budget: Some(nested_tool_budget.clone()),
            flow_control: host.flow_control.clone(),
            cancellation,
        })
        .await
        .map_err(|error| self.status_failure(LiveToolFlowStage::Execution, &error))?
        .ok_or_else(|| self.failure(LiveToolFlowStage::Execution, "tool_execution.cancelled"))?;
        *host.remaining_tool_budget = shared_tool_budget_remaining(&nested_tool_budget);

        let PreparedToolRuntimeOutcome { outcome, side_effect_fence, post_execution_error } =
            runtime_outcome;
        let side_effect_state = side_effect_fence.as_ref().map(|_| {
            if outcome.attestation.timed_out {
                SideEffectFenceState::EffectUnknown
            } else {
                SideEffectFenceState::EffectObserved
            }
        });
        if let Some(post_execution_error) = post_execution_error {
            let proposal_id = request.proposal_id.clone();
            let execution_id = request.execution_id.clone();
            let post_execution_error = finalize_drained_tool_execution_before_error(
                host.sender,
                host.runtime_state,
                host.request_context,
                host.run_id,
                &prepared,
                outcome,
                side_effect_fence.as_ref(),
                host.tape_seq,
                |projected| self.retain_projection(proposal_id, execution_id, projected.clone()),
                post_execution_error,
            )
            .await
            .map_err(|error| self.status_failure(LiveToolFlowStage::Execution, &error))?;
            return Err(self.status_failure(LiveToolFlowStage::Execution, &post_execution_error));
        }
        let execution_evidence =
            vec![self.required_evidence(LiveToolFlowStage::Execution, "tool_execution.completed")?];
        let outcome_ref = self.retain_raw_outcome(
            request.execution_id,
            prepared,
            outcome,
            side_effect_fence,
            side_effect_state,
            execution_evidence.clone(),
        )?;
        Ok(LiveToolExecutionResult {
            outcome: outcome_ref,
            side_effect_state,
            evidence: execution_evidence,
        })
    }

    #[allow(clippy::result_large_err)]
    async fn serve_projection(
        &mut self,
        host: &mut RunStreamLiveToolHost<'_>,
        request: LiveToolProjectionRequest,
    ) -> Result<LiveToolProjectionResult, LiveToolFlowError> {
        self.validate_lane(&request.lane_authority, LiveToolFlowStage::Projection)?;
        let Some(raw) = self.raw_outcomes.remove(request.outcome.id().as_str()) else {
            return Err(self.failure(LiveToolFlowStage::Projection, "tool_projection.raw_missing"));
        };
        let raw_digest = raw_outcome_digest(&raw.outcome);
        if raw_digest != *request.outcome.sha256()
            || raw.prepared.proposal_id != request.proposal_id.as_str()
            || raw.execution_id != request.execution_id
            || raw.side_effect_state != request.side_effect_state
            || raw.execution_evidence.len() != request.execution_evidence.len()
            || !raw.execution_evidence.iter().zip(&request.execution_evidence).all(
                |(retained, observed)| {
                    &retained.id == observed.id() && retained.sha256 == *observed.sha256()
                },
            )
        {
            let error = settle_failed_tool_finalization(
                host.runtime_state,
                host.run_id,
                &raw.prepared,
                raw.side_effect_fence.as_ref(),
                Status::failed_precondition("tool projection binding mismatch"),
            )
            .await
            .into_status();
            return Err(self.status_failure(LiveToolFlowStage::Projection, &error));
        }
        let proposal_id = request.proposal_id;
        let execution_id = request.execution_id;
        let finalization = project_retain_commit_tool_execution_outcome(
            host.sender,
            host.runtime_state,
            host.request_context,
            host.run_id,
            &raw.prepared,
            raw.outcome,
            raw.side_effect_fence.as_ref(),
            host.tape_seq,
            |projected| self.retain_projection(proposal_id, execution_id, projected.clone()),
        )
        .await;
        let finalized = finalization.map_err(|error| {
            let error = error.into_status();
            self.status_failure(LiveToolFlowStage::Projection, &error)
        })?;
        Ok(LiveToolProjectionResult {
            model_visible_result: finalized.retained_projection,
            evidence: vec![
                self.required_evidence(LiveToolFlowStage::Projection, "tool_projection.redacted")?,
            ],
        })
    }

    fn finish_gate(
        &mut self,
        request: FinishGateRequest,
    ) -> Result<LiveToolGateResult, LiveToolFlowError> {
        let FinishGateRequest {
            proposal_id,
            tool_name,
            input_json,
            replay_safety_class,
            retained_proposal,
            authority_class,
            approval_subject_id,
            preparation,
            stage,
        } = request;
        if !preparation.decision.allowed {
            return Ok(LiveToolGateResult::Denied {
                evidence: self.required_evidence(stage, preparation.decision.reason.as_str())?,
            });
        }
        if preparation.synthetic_outcome.is_some() {
            return Ok(LiveToolGateResult::Denied {
                evidence: self.required_evidence(
                    stage,
                    "tool_gate.synthetic_result_requires_legacy_commit",
                )?,
            });
        }
        let grant_evidence = self.required_evidence(stage, "tool_gate.granted")?;
        self.prepared.insert(
            proposal_id.as_str().to_owned(),
            PreparedProposalEntry {
                prepared: RunStreamPreparedToolExecution {
                    proposal_id: proposal_id.as_str().to_owned(),
                    tool_name,
                    input_json,
                    replay_safety_class,
                    tool_signature: preparation.tool_signature,
                    decision: preparation.decision,
                    resolved_session_id: preparation.resolved_session_id,
                    backend_selection: preparation.backend_selection,
                    expected_dynamic_provenance: preparation.expected_dynamic_provenance,
                },
                retained_proposal,
                authority_class,
                grant_evidence: grant_evidence.clone(),
                approval_subject_id,
            },
        );
        Ok(LiveToolGateResult::Granted {
            execution_id: runtime_execution_id()
                .map_err(|error| self.status_failure(stage, &error))?,
            evidence: grant_evidence,
        })
    }

    fn validate_lane(
        &mut self,
        observed: &crate::application::runtime_kernel_v2::phases::PhaseLaneAuthority,
        stage: LiveToolFlowStage,
    ) -> Result<(), LiveToolFlowError> {
        if observed != &self.lane_authority {
            return Err(self.failure(stage, "tool_flow.stale_or_cross_run_authority"));
        }
        Ok(())
    }

    fn retain_raw_outcome(
        &mut self,
        execution_id: palyra_common::runtime_contracts::RuntimeToolExecutionId,
        prepared: RunStreamPreparedToolExecution,
        outcome: ToolExecutionOutcome,
        side_effect_fence: Option<ActiveToolSideEffectFence>,
        side_effect_state: Option<SideEffectFenceState>,
        execution_evidence: Vec<LiveToolHostRef>,
    ) -> Result<LiveToolHostRef, LiveToolFlowError> {
        let id = runtime_operation_id("tool-outcome")
            .map_err(|error| self.status_failure(LiveToolFlowStage::Execution, &error))?;
        let reference = LiveToolHostRef { id, sha256: raw_outcome_digest(&outcome) };
        self.raw_outcomes.insert(
            reference.id.as_str().to_owned(),
            RetainedRawOutcome {
                execution_id,
                prepared,
                outcome,
                side_effect_fence,
                side_effect_state,
                execution_evidence,
            },
        );
        Ok(reference)
    }

    fn retain_projection(
        &self,
        proposal_id: palyra_common::runtime_contracts::RuntimeToolProposalId,
        execution_id: palyra_common::runtime_contracts::RuntimeToolExecutionId,
        outcome: ToolExecutionOutcome,
    ) -> Result<LiveToolHostRef, Status> {
        let id = runtime_operation_id("tool-projection")?;
        let reference = LiveToolHostRef { id, sha256: raw_outcome_digest(&outcome) };
        self.projected.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).insert(
            reference.id.as_str().to_owned(),
            RetainedProjectedEntry {
                reference: reference.clone(),
                proposal_id,
                execution_id,
                outcome,
            },
        );
        Ok(reference)
    }

    fn retain_evidence(&mut self, reason: &str) -> Option<LiveToolHostRef> {
        let redacted = redact_run_stream_text(reason);
        let id = runtime_operation_id("tool-evidence").ok()?;
        let reference = LiveToolHostRef { id, sha256: sha256_bytes(redacted.as_bytes()) };
        self.evidence.insert(reference.id.as_str().to_owned(), redacted);
        Some(reference)
    }

    fn required_evidence(
        &mut self,
        stage: LiveToolFlowStage,
        reason: &str,
    ) -> Result<LiveToolHostRef, LiveToolFlowError> {
        self.retain_evidence(reason).ok_or_else(|| LiveToolFlowError::new(stage, None))
    }

    fn failure(&mut self, stage: LiveToolFlowStage, reason: &str) -> LiveToolFlowError {
        LiveToolFlowError::new(stage, self.retain_evidence(reason))
    }

    fn status_failure(&mut self, stage: LiveToolFlowStage, error: &Status) -> LiveToolFlowError {
        self.failure(stage, redact_run_stream_text(error.message()).as_str())
    }
}

impl LiveToolFlowPort for RunStreamLiveToolFlowPort {
    fn gate(&self, request: LiveToolGateRequest) -> LiveToolFlowFuture<'_, LiveToolGateResult> {
        Box::pin(send_live_tool_command(&self.sender, LiveToolFlowStage::Gate, |response| {
            RunStreamLiveToolCommand::Gate { request, response }
        }))
    }

    fn wait_or_resume_approval(
        &self,
        request: LiveToolApprovalRequest,
    ) -> LiveToolFlowFuture<'_, LiveToolApprovalResult> {
        Box::pin(send_live_tool_command(&self.sender, LiveToolFlowStage::Approval, |response| {
            RunStreamLiveToolCommand::Approval { request, response }
        }))
    }

    fn execute(
        &self,
        request: LiveToolExecutionRequest,
    ) -> LiveToolFlowFuture<'_, LiveToolExecutionResult> {
        Box::pin(send_live_tool_command(&self.sender, LiveToolFlowStage::Execution, |response| {
            RunStreamLiveToolCommand::Execute { request, response }
        }))
    }

    fn project_result(
        &self,
        request: LiveToolProjectionRequest,
    ) -> LiveToolFlowFuture<'_, LiveToolProjectionResult> {
        Box::pin(send_live_tool_command(&self.sender, LiveToolFlowStage::Projection, |response| {
            RunStreamLiveToolCommand::Project { request, response }
        }))
    }
}

#[cfg(test)]
impl RunStreamLiveToolFlowPort {
    async fn probe(&self) -> Result<(), &'static str> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(RunStreamLiveToolCommand::Probe { response })
            .await
            .map_err(|_| "probe command send failed")?;
        receiver.await.map_err(|_| "probe response dropped")
    }
}

async fn send_live_tool_command<T>(
    sender: &mpsc::Sender<RunStreamLiveToolCommand>,
    stage: LiveToolFlowStage,
    command: impl FnOnce(oneshot::Sender<Result<T, LiveToolFlowError>>) -> RunStreamLiveToolCommand,
) -> Result<T, LiveToolFlowError> {
    let (response_sender, response_receiver) = oneshot::channel();
    sender.send(command(response_sender)).await.map_err(|_| LiveToolFlowError::new(stage, None))?;
    response_receiver.await.map_err(|_| LiveToolFlowError::new(stage, None))?
}

struct ResolvedLivePortToolCall {
    tool_name: String,
    input_json: Vec<u8>,
    replay_safety_class: ToolReplaySafetyClass,
    authority_class: crate::application::runtime_kernel_v2::phases::ToolAuthorityClass,
}

fn resolve_live_port_tool_call(
    retained: &RunStreamRetainedToolProposal,
) -> Result<ResolvedLivePortToolCall, String> {
    let normalized = validate_tool_call_against_catalog_snapshot(
        &retained.catalog,
        retained.tool_name.as_str(),
        retained.input_json.as_slice(),
    )
    .map_err(|rejection| format!("{}:{}", rejection.reason_code, rejection.message))?;
    if retained.tool_name == TOOL_CATALOG_SEARCH_TOOL_NAME
        || retained.tool_name == TOOL_CATALOG_DESCRIBE_TOOL_NAME
    {
        return Err("tool_catalog.synthetic_bridge_requires_legacy_commit".to_owned());
    }
    let (tool_name, normalized) = if retained.tool_name == TOOL_CATALOG_INVOKE_TOOL_NAME {
        let target =
            resolve_catalog_invoke_target(&retained.catalog, normalized.input_json.as_slice())
                .map_err(|error| format!("{}:{}", error.reason_code, error.message))?;
        let visible = retained
            .catalog
            .indexed_tools
            .iter()
            .find(|tool| tool.name == target.tool_name)
            .ok_or_else(|| "tool_catalog.tool_not_indexed".to_owned())?;
        let normalized = validate_tool_call_against_model_visible_tool(
            &retained.catalog,
            visible,
            target.tool_name.as_str(),
            target.input_json.as_slice(),
        )
        .map_err(|rejection| format!("{}:{}", rejection.reason_code, rejection.message))?;
        (target.tool_name, normalized)
    } else {
        (retained.tool_name.clone(), normalized)
    };
    let replay_safety_class = retained
        .catalog
        .tools
        .iter()
        .chain(retained.catalog.indexed_tools.iter())
        .find(|tool| tool.name == tool_name)
        .map_or(ToolReplaySafetyClass::RequiresHumanConfirmation, |tool| tool.replay_safety_class);
    let authority_class = authority_class_for_parallelism(classify_tool_parallelism(
        tool_name.as_str(),
        normalized.input_json.as_slice(),
    ));
    Ok(ResolvedLivePortToolCall {
        tool_name,
        input_json: normalized.input_json,
        replay_safety_class,
        authority_class,
    })
}

fn runtime_operation_id(
    prefix: &str,
) -> Result<palyra_common::runtime_contracts::RuntimeOperationId, Status> {
    palyra_common::runtime_contracts::RuntimeOperationId::parse(
        format!("{prefix}:{}", Ulid::new()).as_str(),
    )
    .map_err(|error| Status::internal(format!("failed to allocate runtime operation id: {error}")))
}

fn runtime_execution_id() -> Result<palyra_common::runtime_contracts::RuntimeToolExecutionId, Status>
{
    palyra_common::runtime_contracts::RuntimeToolExecutionId::parse(
        format!("tool-execution:{}", Ulid::new()).as_str(),
    )
    .map_err(|error| Status::internal(format!("failed to allocate tool execution id: {error}")))
}

fn raw_outcome_digest(outcome: &ToolExecutionOutcome) -> [u8; 32] {
    sha256_bytes(
        &[
            outcome.output_json.as_slice(),
            outcome.error.as_bytes(),
            outcome.attestation.execution_sha256.as_bytes(),
        ]
        .concat(),
    )
}

fn sha256_bytes(value: &[u8]) -> [u8; 32] {
    Sha256::digest(value).into()
}

const fn authority_class_for_parallelism(
    parallelism: ToolParallelism,
) -> crate::application::runtime_kernel_v2::phases::ToolAuthorityClass {
    match parallelism {
        ToolParallelism::ReadOnlySafe => {
            crate::application::runtime_kernel_v2::phases::ToolAuthorityClass::ReadOnly
        }
        ToolParallelism::PathScoped => {
            crate::application::runtime_kernel_v2::phases::ToolAuthorityClass::Mutation
        }
        ToolParallelism::Never | ToolParallelism::IdempotentNetwork => {
            crate::application::runtime_kernel_v2::phases::ToolAuthorityClass::ExternalEffect
        }
    }
}

#[cfg(test)]
mod live_owner_tests {
    use super::{
        FinishGateRequest, RunStreamLiveToolCommand, RunStreamToolProposalPreparation,
        TOOL_APPROVAL_RESPONSE_TIMEOUT, ToolParallelism, authority_class_for_parallelism,
        run_stream_live_tool_flow,
    };
    use crate::{
        application::{
            tool_governance::build_tool_call_signature,
            tool_runtime::dynamic_tools::dynamic_tool_provenance_is_current,
            tool_security::ToolProposalBackendSelection,
        },
        execution_backends::{ExecutionBackendPreference, ExecutionBackendResolution},
        tool_protocol::ToolDecision,
    };
    use crate::application::runtime_kernel_v2::phases::ToolAuthorityClass;
    use palyra_common::runtime_contracts::{
        RuntimeGeneration, RuntimeGenerationLane, RuntimeLeaseId, RuntimeRunId, RuntimeSessionId,
        RuntimeToolProposalId,
    };

    const OWNER_SOURCE: &str = include_str!("owner.rs");
    const TOOL_FLOW_SOURCE: &str = include_str!("../tool_flow.rs");

    fn production_owner_source() -> &'static str {
        production_owner_source_from(OWNER_SOURCE)
    }

    fn production_owner_source_from(source: &str) -> &str {
        // The LF before `mod` is present in both LF and CRLF source text.
        source
            .split_once("\nmod live_owner_tests {")
            .map_or(source, |(production, _)| production)
    }

    #[test]
    fn read_only_and_mutating_tools_cannot_raise_authority() {
        assert_eq!(
            authority_class_for_parallelism(ToolParallelism::ReadOnlySafe),
            ToolAuthorityClass::ReadOnly
        );
        assert_eq!(
            authority_class_for_parallelism(ToolParallelism::PathScoped),
            ToolAuthorityClass::Mutation
        );
        assert_eq!(
            authority_class_for_parallelism(ToolParallelism::Never),
            ToolAuthorityClass::ExternalEffect
        );
    }

    #[test]
    fn gate_uses_canonical_schema_and_policy_stages() {
        assert!(OWNER_SOURCE.contains("validate_tool_call_against_catalog_snapshot("));
        assert!(OWNER_SOURCE.contains("prepare_run_stream_tool_gate_without_approval("));
        assert!(OWNER_SOURCE.contains("LiveToolGateResult::Denied"));
    }

    #[test]
    fn approval_timeout_and_resume_inputs_stay_on_durable_path() {
        assert!(!TOOL_APPROVAL_RESPONSE_TIMEOUT.is_zero());
        assert!(OWNER_SOURCE.contains("resolve_run_stream_tool_gate_approval("));
        assert!(OWNER_SOURCE.contains("request.resume_evidence"));
        assert!(OWNER_SOURCE.contains("LiveToolApprovalResult::TimedOut"));
    }

    #[test]
    fn raw_results_and_secret_redaction_remain_host_owned() {
        let production = production_owner_source();
        assert!(production.contains("raw_outcomes: BTreeMap"));
        assert!(production.contains("project_retain_commit_tool_execution_outcome("));
        assert!(production.contains("redact_run_stream_text(reason)"));
        for unsafe_construct in ["unsafe {", "unsafe fn ", "unsafe impl ", "unsafe extern "] {
            assert!(
                !production.contains(unsafe_construct),
                "live tool owner must remain safe Rust: found {unsafe_construct:?}"
            );
        }
    }

    #[test]
    fn production_source_boundary_accepts_lf_and_crlf() {
        for source in [
            "fn production() {}\n#[cfg(test)]\nmod live_owner_tests { unsafe {} }",
            "fn production() {}\r\n#[cfg(test)]\r\nmod live_owner_tests { unsafe {} }",
        ] {
            let production = production_owner_source_from(source);
            assert!(production.contains("fn production() {}"));
            assert!(!production.contains("unsafe {"));
        }
    }

    #[test]
    fn projection_consumes_raw_outcome_without_redispatch() {
        let projection = production_owner_source()
            .split_once("async fn serve_projection")
            .and_then(|(_, suffix)| suffix.split_once("fn finish_gate"))
            .map(|(body, _)| body)
            .expect("serve_projection body should remain discoverable");
        assert!(projection.contains("self.raw_outcomes.remove("));
        assert!(!projection.contains("self.raw_outcomes.insert("));
        assert!(!projection.contains("execute_prepared_tool_runtime("));
    }

    #[test]
    fn parallel_and_path_conflict_regressions_remain_in_canonical_flow() {
        assert!(TOOL_FLOW_SOURCE.contains("execute_prepared_run_stream_tool_proposals_ordered("));
        assert!(TOOL_FLOW_SOURCE.contains("split_parallel_tool_groups("));
        assert!(TOOL_FLOW_SOURCE.contains("execute_parallel_prepared_tool_group("));
        assert!(TOOL_FLOW_SOURCE.contains("path_scope_key"));
        assert!(TOOL_FLOW_SOURCE.contains("PathScoped"));
    }

    #[test]
    fn mcp_tools_remain_behind_canonical_backend_dispatch() {
        let canonical_dispatch =
            ["execute_tool_with_runtime_dispatch_with_", "cancellation_and_progress("].concat();
        assert!(TOOL_FLOW_SOURCE.contains(canonical_dispatch.as_str()));
        assert!(!TOOL_FLOW_SOURCE.contains("execute_persistent_mcp_tool("));
    }

    #[test]
    fn stale_generation_is_rejected_before_stage_dispatch() {
        assert!(OWNER_SOURCE.contains("tool_flow.stale_or_cross_run_authority"));
        assert!(OWNER_SOURCE.contains("observed != &self.lane_authority"));
    }

    #[test]
    fn owner_retains_snapshot_provenance_across_update_and_rollback() {
        let generation = RuntimeGeneration::new(1).expect("generation");
        let authority =
            crate::application::runtime_kernel_v2::phases::PhaseLaneAuthority::test_only_from_host_leases(
                RuntimeSessionId::parse("session_dynamic_snapshot").expect("session"),
                RuntimeRunId::parse("run_dynamic_snapshot").expect("run"),
                generation,
                RuntimeLeaseId::parse("run_lease_dynamic_snapshot").expect("run lease"),
                RuntimeGenerationLane::Tool,
                generation,
                RuntimeLeaseId::parse("tool_lease_dynamic_snapshot").expect("tool lease"),
            );
        let (_, mut owner) = run_stream_live_tool_flow(authority);
        let proposal_id =
            RuntimeToolProposalId::parse("tool-proposal:01ARZ3NDEKTSV4RRFFQ69G5FAV")
                .expect("proposal");
        let tool_name = "dynamic.snapshot".to_owned();
        let input_json = br#"{"value":"snapshot"}"#.to_vec();
        let snapshot_provenance = "dynamic:artifact-v2:eval-v2:2:7:11".to_owned();
        let preparation = RunStreamToolProposalPreparation {
            decision: ToolDecision {
                allowed: true,
                reason: "test_policy_grant".to_owned(),
                approval_required: false,
                policy_enforced: true,
            },
            resolved_session_id: "session_dynamic_snapshot".to_owned(),
            backend_selection: ToolProposalBackendSelection {
                agent_id: None,
                requested_preference: ExecutionBackendPreference::LocalSandbox,
                resolution: ExecutionBackendResolution {
                    requested: ExecutionBackendPreference::LocalSandbox,
                    resolved: ExecutionBackendPreference::LocalSandbox,
                    fallback_used: false,
                    reason_code: "test_local_sandbox".to_owned(),
                    approval_required: false,
                    reason: "test local sandbox".to_owned(),
                },
            },
            expected_dynamic_provenance: Some(snapshot_provenance.clone()),
            tool_signature: build_tool_call_signature(&tool_name, &input_json),
            synthetic_outcome: None,
            approval_timed_out: false,
        };
        let retained_proposal =
            crate::application::runtime_kernel_v2::phases::LiveToolAuthorityGateway::retained_proposal_ref(
                super::LiveToolHostRef {
                    id: palyra_common::runtime_contracts::RuntimeOperationId::parse(
                        "retained-proposal:01ARZ3NDEKTSV4RRFFQ69G5FAW",
                    )
                    .expect("retained proposal"),
                    sha256: [7; 32],
                },
            );
        owner
            .finish_gate(FinishGateRequest {
                proposal_id: proposal_id.clone(),
                tool_name,
                input_json,
                replay_safety_class:
                    crate::application::tool_registry::ToolReplaySafetyClass::RequiresHumanConfirmation,
                retained_proposal,
                authority_class: ToolAuthorityClass::ExternalEffect,
                approval_subject_id: None,
                preparation,
                stage: super::LiveToolFlowStage::Gate,
            })
            .expect("snapshot gate");

        let retained = owner.prepared.get(proposal_id.as_str()).expect("prepared");
        let observed = retained
            .prepared
            .expected_dynamic_provenance
            .as_deref()
            .expect("snapshot provenance");
        assert_eq!(observed, snapshot_provenance);
        assert!(!dynamic_tool_provenance_is_current(
            observed,
            "dynamic:artifact-v3:eval-v3:3:8:12"
        ));
        assert!(!dynamic_tool_provenance_is_current(
            observed,
            "dynamic:artifact-v1:eval-v1:4:9:13"
        ));
    }

    #[tokio::test]
    async fn concrete_mailbox_roundtrip_completes_without_deadlock() {
        let generation = RuntimeGeneration::new(1).expect("generation");
        let authority =
            crate::application::runtime_kernel_v2::phases::PhaseLaneAuthority::test_only_from_host_leases(
                RuntimeSessionId::parse("session_mailbox").expect("session"),
                RuntimeRunId::parse("run_mailbox").expect("run"),
                generation,
                RuntimeLeaseId::parse("run_lease_mailbox").expect("run lease"),
                RuntimeGenerationLane::Tool,
                generation,
                RuntimeLeaseId::parse("tool_lease_mailbox").expect("tool lease"),
            );
        let (port, mut owner) = run_stream_live_tool_flow(authority);
        let probe = tokio::spawn(async move { port.probe().await });
        let command = owner.receiver.recv().await.expect("probe command");
        let RunStreamLiveToolCommand::Probe { response } = command else {
            panic!("unexpected mailbox command");
        };
        response.send(()).expect("probe receiver");

        probe.await.expect("probe task").expect("probe roundtrip");
    }
}
