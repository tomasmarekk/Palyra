//! Host-owned projection from typed harness observations into M017 transitions.
//!
//! This module constructs canonical events, commits them through the private
//! journal capability, and restores the kernel before acknowledging progress.

use std::sync::Arc;

use palyra_common::runtime_contracts::{RuntimeEventName, RuntimeGenerationLane};
use serde_json::json;

use super::{
    harness::{
        HarnessAccepted, HarnessApprovalResolution, HarnessAttemptRequest, HarnessAttemptTerminal,
        HarnessContractError, HarnessEvent, HarnessEventKind, HarnessEventSink, HarnessFuture,
        MAX_HARNESS_EVENTS_PER_ATTEMPT,
    },
    host_event_contract::{
        approval_resolution_str, runtime_profile_str, DeliveryObservationState,
        DeliverySkipEvidence, FinalizationRecoveryEvidence, HarnessHostEventAuthority,
        HarnessTerminalReceipt, SystemHarnessEventAuthority, VerificationState,
    },
    journal_adapter::KernelJournalAdapter,
    KernelLaneAuthoritySet, KernelState, KernelTransition, RuntimeKernelV2,
};
use crate::journal::runtime_kernel::RuntimeKernelObservationCommitRequest;

#[path = "host_event_commit.rs"]
mod commit;
mod interface;
#[path = "host_event_terminal.rs"]
mod terminal;

/// Bounded host sink for one generation-pinned harness attempt.
pub(crate) struct HostHarnessEventSink {
    request: HarnessAttemptRequest,
    kernel: RuntimeKernelV2,
    lane_authority: KernelLaneAuthoritySet,
    journal: KernelJournalAdapter,
    event_authority: Box<dyn HarnessHostEventAuthority>,
    accepted: bool,
    terminal_seen: bool,
    last_harness_sequence: u64,
    observations_accepted: usize,
    prompt_tokens: u64,
    completion_tokens: u64,
    verification: VerificationState,
    delivery: DeliveryObservationState,
    delivery_skip_evidence: Option<DeliverySkipEvidence>,
    finalization_recovery_evidence: Option<FinalizationRecoveryEvidence>,
}

impl HostHarnessEventSink {
    /// Creates the production sink over the gateway-owned journal.
    pub(crate) fn from_runtime_state(
        request: HarnessAttemptRequest,
        kernel: RuntimeKernelV2,
        lane_authority: KernelLaneAuthoritySet,
        runtime_state: Arc<crate::gateway::GatewayRuntimeState>,
    ) -> Result<Self, HarnessContractError> {
        Self::with_journal_adapter(
            request,
            kernel,
            lane_authority,
            KernelJournalAdapter::from_runtime_state(runtime_state),
            Box::new(SystemHarnessEventAuthority),
        )
    }

    fn with_journal_adapter(
        request: HarnessAttemptRequest,
        kernel: RuntimeKernelV2,
        lane_authority: KernelLaneAuthoritySet,
        journal: KernelJournalAdapter,
        event_authority: Box<dyn HarnessHostEventAuthority>,
    ) -> Result<Self, HarnessContractError> {
        let snapshot = kernel.snapshot();
        if snapshot.state() != KernelState::Admitted
            || snapshot.version() != request.selected_profile()
            || snapshot.base_identities() != request.identities()
            || snapshot.run_generation() != request.generation()
        {
            return Err(HarnessContractError::InvalidAttemptRequest);
        }
        lane_authority.validate(request.identities())?;
        if lane_authority.run_lease(request.identities())? != snapshot.run_lease() {
            return Err(HarnessContractError::InvalidAttemptRequest);
        }
        Ok(Self {
            request,
            kernel,
            lane_authority,
            journal,
            event_authority,
            accepted: false,
            terminal_seen: false,
            last_harness_sequence: 0,
            observations_accepted: 0,
            prompt_tokens: 0,
            completion_tokens: 0,
            verification: VerificationState::NotStarted,
            delivery: DeliveryObservationState::Pending,
            delivery_skip_evidence: None,
            finalization_recovery_evidence: None,
        })
    }

    #[cfg(test)]
    fn for_test(
        request: HarnessAttemptRequest,
        kernel: RuntimeKernelV2,
        lane_authority: KernelLaneAuthoritySet,
        journal: KernelJournalAdapter,
        event_authority: Box<dyn HarnessHostEventAuthority>,
    ) -> Result<Self, HarnessContractError> {
        Self::with_journal_adapter(request, kernel, lane_authority, journal, event_authority)
    }

    fn accept(&mut self, accepted: HarnessAccepted) -> Result<(), HarnessContractError> {
        if self.terminal_seen {
            return Err(HarnessContractError::EventAfterTerminal);
        }
        if self.accepted {
            return Err(HarnessContractError::DuplicateAccepted);
        }
        self.validate_generation(accepted.generation)?;
        if accepted.sequence != 1 {
            return Err(HarnessContractError::NonMonotonicSequence {
                last: 0,
                observed: accepted.sequence,
            });
        }

        self.apply_transition(
            RuntimeEventName::RunStarted,
            KernelTransition::BeginRuntimeSelection,
            self.request.identities().clone(),
            "runtime.harness.run_accepted",
            json!({"harness_id": self.request.harness_id()}),
            accepted.sequence,
        )?;
        let identities =
            self.identities_for_lane(RuntimeGenerationLane::Harness, |identities| {
                identities.attempt_id = Some(self.request.attempt_id().clone());
            })?;
        self.apply_transition(
            RuntimeEventName::HarnessAttemptStarted,
            KernelTransition::BeginContextAssembly,
            identities,
            "runtime.harness.embedded_attempt_accepted",
            json!({"profile": runtime_profile_str(self.request.selected_profile())}),
            accepted.sequence,
        )?;
        self.accepted = true;
        self.last_harness_sequence = accepted.sequence;
        Ok(())
    }

    fn observe(&mut self, event: HarnessEvent) -> Result<(), HarnessContractError> {
        self.require_open()?;
        event.validate()?;
        self.validate_generation(event.generation)?;
        self.validate_next_sequence(event.sequence)?;
        if self.observations_accepted >= MAX_HARNESS_EVENTS_PER_ATTEMPT {
            return Err(HarnessContractError::EventLimitExceeded);
        }

        let sequence = event.sequence;
        match event.kind {
            HarnessEventKind::ProviderCallStarted => {
                let identities =
                    self.identities_for_lane(RuntimeGenerationLane::Provider, |identities| {
                        identities.attempt_id = Some(self.request.attempt_id().clone());
                    })?;
                self.apply_transition(
                    RuntimeEventName::ProviderAttemptStarted,
                    KernelTransition::BeginProviderCall,
                    identities,
                    "runtime.harness.provider_call_started",
                    json!({}),
                    sequence,
                )?;
            }
            HarnessEventKind::ProviderCallCompleted => {
                self.begin_finalization(
                    sequence,
                    RuntimeEventName::ProviderAttemptCompleted,
                    "runtime.harness.provider_call_completed",
                )?;
            }
            HarnessEventKind::ToolProposed { proposal_id } => {
                let identities =
                    self.identities_for_lane(RuntimeGenerationLane::Run, |identities| {
                        identities.attempt_id = Some(self.request.attempt_id().clone());
                        identities.tool_proposal_id = Some(proposal_id);
                    })?;
                self.apply_transition(
                    RuntimeEventName::ToolProposed,
                    KernelTransition::BeginToolGate,
                    identities,
                    "runtime.harness.tool_proposed",
                    json!({}),
                    sequence,
                )?;
            }
            HarnessEventKind::ToolDenied { proposal_id, evidence_id, evidence_sha256 } => {
                let identities =
                    self.identities_for_lane(RuntimeGenerationLane::Run, |identities| {
                        identities.attempt_id = Some(self.request.attempt_id().clone());
                        identities.tool_proposal_id = Some(proposal_id);
                    })?;
                self.apply_transition(
                    RuntimeEventName::ToolDecisionRecorded,
                    KernelTransition::ResolveToolWithoutExecution,
                    identities,
                    "runtime.harness.tool_denied",
                    json!({
                        "evidence_id": evidence_id.as_str(),
                        "evidence_sha256": hex::encode(evidence_sha256),
                    }),
                    sequence,
                )?;
            }
            HarnessEventKind::ApprovalRequired { proposal_id, approval_id } => {
                let identities =
                    self.identities_for_lane(RuntimeGenerationLane::Run, |identities| {
                        identities.attempt_id = Some(self.request.attempt_id().clone());
                        identities.tool_proposal_id = Some(proposal_id);
                        identities.approval_subject_id = Some(approval_id);
                    })?;
                self.apply_transition(
                    RuntimeEventName::ApprovalRequired,
                    KernelTransition::BeginApprovalWait,
                    identities,
                    "runtime.harness.approval_required",
                    json!({}),
                    sequence,
                )?;
            }
            HarnessEventKind::ApprovalResolved {
                proposal_id,
                approval_id,
                resolution,
                evidence_id,
                evidence_sha256,
            } => {
                let identities =
                    self.identities_for_lane(RuntimeGenerationLane::Run, |identities| {
                        identities.attempt_id = Some(self.request.attempt_id().clone());
                        identities.tool_proposal_id = Some(proposal_id);
                        identities.approval_subject_id = Some(approval_id);
                    })?;
                self.apply_transition(
                    RuntimeEventName::ApprovalResolved,
                    KernelTransition::ResumeToolGate,
                    identities.clone(),
                    "runtime.harness.approval_resolved",
                    json!({
                        "resolution": approval_resolution_str(resolution),
                        "evidence_id": evidence_id.as_ref().map(|value| value.as_str()),
                        "evidence_sha256": evidence_sha256.map(hex::encode),
                    }),
                    sequence,
                )?;
                if !matches!(resolution, HarnessApprovalResolution::Approved) {
                    self.apply_transition(
                        RuntimeEventName::ToolDecisionRecorded,
                        KernelTransition::ResolveToolWithoutExecution,
                        identities,
                        "runtime.harness.tool_denied_after_approval",
                        json!({"resolution": approval_resolution_str(resolution)}),
                        sequence,
                    )?;
                }
            }
            HarnessEventKind::ToolExecutionStarted { proposal_id, execution_id, operation_id } => {
                let identities =
                    self.identities_for_lane(RuntimeGenerationLane::Run, |identities| {
                        identities.attempt_id = Some(self.request.attempt_id().clone());
                        identities.tool_proposal_id = Some(proposal_id);
                        identities.tool_execution_id = Some(execution_id);
                        identities.operation_id = Some(operation_id);
                    })?;
                self.apply_transition(
                    RuntimeEventName::ToolIntentRecorded,
                    KernelTransition::BeginToolExecution,
                    identities,
                    "runtime.harness.tool_execution_authorized",
                    json!({}),
                    sequence,
                )?;
            }
            HarnessEventKind::ToolResultObserved { proposal_id, execution_id, operation_id } => {
                let identities =
                    self.identities_for_lane(RuntimeGenerationLane::Run, |identities| {
                        identities.attempt_id = Some(self.request.attempt_id().clone());
                        identities.tool_proposal_id = Some(proposal_id);
                        identities.tool_execution_id = Some(execution_id);
                        identities.operation_id = Some(operation_id);
                    })?;
                self.apply_transition(
                    RuntimeEventName::ToolResultObserved,
                    KernelTransition::BeginResultProjection,
                    identities,
                    "runtime.harness.tool_result_observed",
                    json!({}),
                    sequence,
                )?;
            }
            HarnessEventKind::CompactionRequired => {
                let identities =
                    self.identities_for_lane(RuntimeGenerationLane::Provider, |identities| {
                        identities.attempt_id = Some(self.request.attempt_id().clone());
                    })?;
                self.apply_transition(
                    RuntimeEventName::ProviderAttemptCompleted,
                    KernelTransition::BeginCompaction,
                    identities,
                    "runtime.harness.compaction_required",
                    json!({}),
                    sequence,
                )?;
            }
            HarnessEventKind::CompactionCompleted => {
                let identities =
                    self.identities_for_lane(RuntimeGenerationLane::Provider, |identities| {
                        identities.attempt_id = Some(self.request.attempt_id().clone());
                    })?;
                self.apply_transition(
                    RuntimeEventName::ProviderAttemptStarted,
                    KernelTransition::BeginProviderCall,
                    identities,
                    "runtime.harness.compaction_completed",
                    json!({}),
                    sequence,
                )?;
            }
            HarnessEventKind::FinalizationReady => {
                if self.kernel.snapshot().state() != KernelState::Finalizing {
                    self.begin_finalization(
                        sequence,
                        RuntimeEventName::FinalizationStarted,
                        "runtime.harness.finalization_ready",
                    )?;
                }
            }
            HarnessEventKind::DeliveryIntentCommitted {
                delivery_intent_id,
                operation_id,
                output_event_id,
            } => {
                if self.verification != VerificationState::Passed
                    || self.delivery != DeliveryObservationState::Pending
                {
                    return Err(HarnessContractError::InvalidEvent);
                }
                let mut identities =
                    self.identities_for_lane(RuntimeGenerationLane::Delivery, |identities| {
                        identities.attempt_id = Some(self.request.attempt_id().clone());
                        identities.operation_id = Some(operation_id);
                    })?;
                identities
                    .bind_delivery_intent(delivery_intent_id, &output_event_id)
                    .map_err(|_| HarnessContractError::InvalidEvent)?;
                self.apply_transition(
                    RuntimeEventName::DeliveryIntentRecorded,
                    KernelTransition::BeginDeliveryWait,
                    identities,
                    "runtime.harness.delivery_intent_committed",
                    json!({}),
                    sequence,
                )?;
                self.delivery = DeliveryObservationState::Committed;
            }
            HarnessEventKind::DeliverySkipped { evidence_id, evidence_sha256 } => {
                if self.verification != VerificationState::Passed
                    || self.delivery != DeliveryObservationState::Pending
                    || self.kernel.snapshot().state() != KernelState::Finalizing
                {
                    return Err(HarnessContractError::InvalidEvent);
                }
                self.delivery_skip_evidence =
                    Some(DeliverySkipEvidence { evidence_id, evidence_sha256 });
                self.delivery = DeliveryObservationState::Skipped;
            }
            HarnessEventKind::FinalizationRecoveryPending { reason_code, stage } => {
                if self.verification != VerificationState::Passed
                    || self.delivery != DeliveryObservationState::Pending
                    || self.kernel.snapshot().state() != KernelState::Finalizing
                {
                    return Err(HarnessContractError::InvalidEvent);
                }
                self.finalization_recovery_evidence =
                    Some(FinalizationRecoveryEvidence { reason_code, stage });
                self.delivery = DeliveryObservationState::RecoveryPending;
            }
            HarnessEventKind::Usage { prompt_tokens, completion_tokens } => {
                self.prompt_tokens = self
                    .prompt_tokens
                    .checked_add(prompt_tokens)
                    .ok_or(HarnessContractError::InvalidEvent)?;
                self.completion_tokens = self
                    .completion_tokens
                    .checked_add(completion_tokens)
                    .ok_or(HarnessContractError::InvalidEvent)?;
            }
            HarnessEventKind::VerificationStarted => match self.verification {
                VerificationState::NotStarted => {
                    self.verification = VerificationState::InProgress;
                }
                VerificationState::InProgress
                | VerificationState::Passed
                | VerificationState::Failed => {
                    return Err(HarnessContractError::InvalidVerificationTransition);
                }
            },
            HarnessEventKind::VerificationPassed => match self.verification {
                VerificationState::InProgress => {
                    self.verification = VerificationState::Passed;
                }
                VerificationState::NotStarted
                | VerificationState::Passed
                | VerificationState::Failed => {
                    return Err(HarnessContractError::InvalidVerificationTransition);
                }
            },
            HarnessEventKind::VerificationFailed { .. } => match self.verification {
                VerificationState::InProgress => {
                    self.verification = VerificationState::Failed;
                }
                VerificationState::NotStarted
                | VerificationState::Passed
                | VerificationState::Failed => {
                    return Err(HarnessContractError::InvalidVerificationTransition);
                }
            },
            HarnessEventKind::TextDelta { .. } | HarnessEventKind::CancellationObserved => {}
            #[cfg(test)]
            HarnessEventKind::Progress { .. } | HarnessEventKind::Heartbeat { .. } => {}
        }

        self.last_harness_sequence = sequence;
        self.observations_accepted += 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex,
        },
    };

    use palyra_common::runtime_contracts::{
        GenerationLeaseV1, RuntimeApprovalSubjectId, RuntimeAttemptId, RuntimeDeliveryIntentId,
        RuntimeEventEnvelopeV2, RuntimeEventId, RuntimeEventPayloadRef, RuntimeGeneration,
        RuntimeIdentitySetV1, RuntimeLeaseId, RuntimeOperationId, RuntimeRunId, RuntimeSessionId,
        RuntimeToolExecutionId, RuntimeToolProposalId, RuntimeTraceId,
        RUNTIME_GENERATION_SCHEMA_VERSION,
    };

    use super::*;
    use crate::application::runtime_kernel_v2::{
        embedded_harness::{
            EmbeddedAttemptDriver, EmbeddedAttemptError, EmbeddedHarnessAdapter,
            EmbeddedHarnessEventPort,
        },
        harness::{HarnessProviderFailure, HarnessRuntimeV2, HarnessTerminalOutcome},
        host_event_contract::HarnessHostEventStamp,
        journal_adapter::{
            KernelJournalAdapter, KernelJournalAdapterError, KernelJournalCommit, KernelJournalPort,
        },
        profile::{RuntimeKernelCompatibilityOverridesV1, RuntimeKernelProfileConfigV1},
        rollback::VerifiedRuntimeRollbackSafeBoundary,
        selection::{
            resolve_runtime_authority, RuntimeAuthorityProgressEvidence, V2RuntimeAvailability,
        },
        KernelTerminalOutcome, RuntimeKernelVersion,
    };
    use crate::journal::runtime_kernel::RuntimeRollbackBoundaryOutcome;

    fn generation(value: u64) -> RuntimeGeneration {
        RuntimeGeneration::new(value).expect("test generation is non-zero")
    }

    fn base_identities() -> RuntimeIdentitySetV1 {
        RuntimeIdentitySetV1::for_run(
            RuntimeTraceId::parse("trace_harness").expect("test trace id is valid"),
            RuntimeSessionId::parse("session_harness").expect("test session id is valid"),
            RuntimeRunId::parse("run_harness").expect("test run id is valid"),
            generation(7),
        )
    }

    fn lane_generation(lane: RuntimeGenerationLane) -> RuntimeGeneration {
        match lane {
            RuntimeGenerationLane::Run => generation(7),
            RuntimeGenerationLane::Harness => generation(11),
            RuntimeGenerationLane::Provider => generation(12),
            RuntimeGenerationLane::Tool => generation(13),
            RuntimeGenerationLane::Plugin => generation(14),
            RuntimeGenerationLane::Worker => generation(15),
            RuntimeGenerationLane::Process => generation(16),
            RuntimeGenerationLane::Mcp => generation(17),
            RuntimeGenerationLane::Delivery => generation(18),
        }
    }

    fn lane_authority() -> KernelLaneAuthoritySet {
        let identities = base_identities();
        let leases = RuntimeGenerationLane::wire_contract_values()
            .iter()
            .filter_map(|value| RuntimeGenerationLane::parse(value.canonical))
            .enumerate()
            .map(|(index, lane)| GenerationLeaseV1 {
                schema_version: RUNTIME_GENERATION_SCHEMA_VERSION,
                lease_id: RuntimeLeaseId::parse(format!("harness_lease_{index}").as_str())
                    .expect("test lease id is valid"),
                session_id: identities.session_id.clone(),
                run_id: Some(identities.run_id.clone()),
                lane,
                generation: lane_generation(lane),
                owner: "harness_test_host".to_owned(),
                acquired_at_unix_ms: 1,
                expires_at_unix_ms: 10_000,
            })
            .collect();
        KernelLaneAuthoritySet::new(&identities, leases).expect("test lane authority is valid")
    }

    fn request_and_kernel() -> (HarnessAttemptRequest, RuntimeKernelV2, KernelLaneAuthoritySet) {
        let identities = base_identities();
        let lanes = lane_authority();
        let profile = RuntimeKernelProfileConfigV1::new(
            RuntimeKernelVersion::V2,
            0,
            RuntimeKernelCompatibilityOverridesV1::none(),
        )
        .expect("test profile is valid");
        let decision = resolve_runtime_authority(
            &profile,
            &identities,
            V2RuntimeAvailability::Ready,
            RuntimeAuthorityProgressEvidence::pristine(),
            None,
        )
        .expect("test V2 authority resolves");
        let kernel = RuntimeKernelV2::admit_for_test(
            decision,
            identities.clone(),
            lanes.run_lease(&identities).expect("test Run lease exists").clone(),
            1,
        )
        .expect("test kernel admission succeeds");
        let request = HarnessAttemptRequest::from_host_parts(
            identities,
            attempt_id("attempt_harness"),
            "embedded".to_owned(),
            RuntimeKernelVersion::V2,
        )
        .expect("test request is valid");
        (request, kernel, lanes)
    }

    #[derive(Default)]
    struct FakeJournalPort {
        events: Mutex<Vec<RuntimeEventEnvelopeV2>>,
        next_by_lane: Mutex<BTreeMap<RuntimeGenerationLane, u64>>,
        head: Mutex<Option<crate::application::runtime_kernel_v2::KernelStateSnapshot>>,
    }

    impl FakeJournalPort {
        fn events(&self) -> Vec<RuntimeEventEnvelopeV2> {
            self.events.lock().expect("fake journal mutex is healthy").clone()
        }

        fn restored_head_revision(&self) -> u64 {
            let snapshot = self
                .head
                .lock()
                .expect("fake journal head mutex is healthy")
                .clone()
                .expect("successful attempt persists a kernel head");
            RuntimeKernelV2::restore_from_journal(snapshot)
                .expect("persisted kernel head restores")
                .snapshot()
                .revision()
        }
    }

    impl KernelJournalPort for FakeJournalPort {
        fn commit_observation(
            &self,
            request: &RuntimeKernelObservationCommitRequest,
        ) -> Result<KernelJournalCommit, KernelJournalAdapterError> {
            let lane = request.event_template.event_name.descriptor().generation_lane;
            let mut sequences = self.next_by_lane.lock().expect("fake sequence mutex is healthy");
            let sequence = sequences.entry(lane).or_default();
            *sequence = sequence.checked_add(1).expect("test sequence remains bounded");
            let mut event = request.event_template.clone();
            event.sequence = *sequence;
            drop(sequences);
            let kernel = RuntimeKernelV2::restore_from_journal(request.expected_snapshot.clone())
                .map_err(KernelJournalAdapterError::Restore)?;
            let prepared = kernel
                .prepare_transition(
                    request.expected_run_generation,
                    &request.lane_authority,
                    request.idempotency_key.as_str(),
                    event,
                    request.transition,
                )
                .map_err(KernelJournalAdapterError::Restore)?;
            self.events
                .lock()
                .expect("fake journal mutex is healthy")
                .push(prepared.event().clone());
            let next_snapshot = prepared.next_snapshot().clone();
            *self.head.lock().expect("fake journal head mutex is healthy") =
                Some(next_snapshot.clone());
            Ok(KernelJournalCommit::Applied(next_snapshot))
        }

        fn apply_pending_rollback(
            &self,
            _boundary: &VerifiedRuntimeRollbackSafeBoundary,
        ) -> Result<RuntimeRollbackBoundaryOutcome, KernelJournalAdapterError> {
            Ok(RuntimeRollbackBoundaryOutcome::NoRequest)
        }
    }

    #[derive(Default)]
    struct FakeEventAuthority {
        event_ordinal: u64,
    }

    impl HarnessHostEventAuthority for FakeEventAuthority {
        fn issue(
            &mut self,
            _lane: RuntimeGenerationLane,
            _generation: RuntimeGeneration,
        ) -> Result<HarnessHostEventStamp, HarnessContractError> {
            self.event_ordinal =
                self.event_ordinal.checked_add(1).ok_or(HarnessContractError::HostEventMetadata)?;
            Ok(HarnessHostEventStamp {
                event_id: RuntimeEventId::parse(
                    format!("harness_host_event_{}", self.event_ordinal).as_str(),
                )
                .map_err(|_| HarnessContractError::HostEventMetadata)?,
                occurred_at_unix_ms: 1_700_000_000_000,
            })
        }
    }

    #[derive(Clone)]
    struct FakeDriver {
        events: Vec<HarnessEvent>,
        terminal: HarnessAttemptTerminal,
        provider_failure: Option<HarnessProviderFailure>,
        finalize_calls: Arc<AtomicUsize>,
    }

    impl EmbeddedAttemptDriver for FakeDriver {
        fn run_attempt<'a>(
            &'a self,
            _request: &'a HarnessAttemptRequest,
            events: &'a mut dyn EmbeddedHarnessEventPort,
        ) -> HarnessFuture<'a, Result<HarnessAttemptTerminal, EmbeddedAttemptError>> {
            Box::pin(async move {
                for event in self.events.clone() {
                    events.emit(event).await.map_err(EmbeddedAttemptError::Contract)?;
                }
                if let Some(failure) = self.provider_failure.clone() {
                    Err(EmbeddedAttemptError::Provider(failure))
                } else {
                    Ok(self.terminal.clone())
                }
            })
        }

        fn finalize_attempt<'a>(
            &'a self,
            request: &'a HarnessAttemptRequest,
            mut terminal: HarnessAttemptTerminal,
            events: &'a mut dyn EmbeddedHarnessEventPort,
        ) -> HarnessFuture<'a, Result<HarnessAttemptTerminal, EmbeddedAttemptError>> {
            Box::pin(async move {
                self.finalize_calls.fetch_add(1, Ordering::SeqCst);
                events
                    .emit(event(terminal.sequence, HarnessEventKind::FinalizationReady))
                    .await
                    .map_err(EmbeddedAttemptError::Contract)?;
                terminal.sequence = terminal
                    .sequence
                    .checked_add(1)
                    .ok_or(EmbeddedAttemptError::Contract(HarnessContractError::InvalidTerminal))?;
                if matches!(terminal.outcome, HarnessTerminalOutcome::Completed) {
                    let sequence = terminal.sequence;
                    events
                        .emit(delivery_skipped(sequence))
                        .await
                        .map_err(EmbeddedAttemptError::Contract)?;
                    terminal.sequence = sequence.checked_add(1).ok_or(
                        EmbeddedAttemptError::Contract(HarnessContractError::InvalidTerminal),
                    )?;
                    debug_assert_eq!(terminal.generation, request.generation());
                }
                Ok(terminal)
            })
        }
    }

    fn sink() -> (HostHarnessEventSink, Arc<FakeJournalPort>) {
        let (request, kernel, lanes) = request_and_kernel();
        let journal = Arc::new(FakeJournalPort::default());
        let sink = HostHarnessEventSink::for_test(
            request,
            kernel,
            lanes,
            KernelJournalAdapter::from_test_port(journal.clone()),
            Box::<FakeEventAuthority>::default(),
        )
        .expect("test sink is valid");
        (sink, journal)
    }

    fn event(sequence: u64, kind: HarnessEventKind) -> HarnessEvent {
        HarnessEvent { generation: generation(7), sequence, kind }
    }

    fn delivery_skipped(sequence: u64) -> HarnessEvent {
        event(
            sequence,
            HarnessEventKind::DeliverySkipped {
                evidence_id: operation_id("delivery_skip_evidence"),
                evidence_sha256: [0xab; 32],
            },
        )
    }

    fn completed(sequence: u64) -> HarnessAttemptTerminal {
        HarnessAttemptTerminal {
            generation: generation(7),
            sequence,
            outcome: HarnessTerminalOutcome::Completed,
        }
    }

    fn driver(events: Vec<HarnessEvent>, terminal: HarnessAttemptTerminal) -> FakeDriver {
        FakeDriver {
            events,
            terminal,
            provider_failure: None,
            finalize_calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    #[tokio::test]
    async fn text_attempt_preserves_progress_usage_heartbeat_and_verification() {
        let (mut sink, journal) = sink();
        let adapter = EmbeddedHarnessAdapter::new(driver(
            vec![
                event(2, HarnessEventKind::ProviderCallStarted),
                event(3, HarnessEventKind::TextDelta { utf8_bytes: 42 }),
                event(4, HarnessEventKind::Progress { completed_units: 1, total_units: 2 }),
                event(5, HarnessEventKind::Usage { prompt_tokens: 30, completion_tokens: 12 }),
                event(6, HarnessEventKind::Heartbeat { ordinal: 1 }),
                event(7, HarnessEventKind::ProviderCallCompleted),
                event(8, HarnessEventKind::VerificationStarted),
                event(9, HarnessEventKind::VerificationPassed),
            ],
            completed(10),
        ));

        let request = sink.request.clone();
        let receipt =
            adapter.run_attempt(&request, &mut sink).await.expect("text attempt succeeds");

        assert_eq!(receipt.outcome(), KernelTerminalOutcome::Done);
        assert_eq!(receipt.observations_accepted(), 10);
        assert_eq!(receipt.prompt_tokens(), 30);
        assert_eq!(receipt.completion_tokens(), 12);
        assert_eq!(receipt.harness_terminal_sequence(), 12);
        assert_eq!(journal.events().len(), 5);
        assert_eq!(receipt.kernel_revision(), journal.restored_head_revision());
        assert!(receipt.verification_passed());
        let completed = journal
            .events()
            .into_iter()
            .find(|event| event.event_name == RuntimeEventName::RunCompleted)
            .expect("run completion is durable");
        let RuntimeEventPayloadRef::Inline { metadata } = completed.payload else {
            panic!("run completion uses inline redacted metadata");
        };
        assert_eq!(metadata["delivery"], "skipped");
        assert_eq!(metadata["delivery_skip_evidence_id"], "delivery_skip_evidence");
        assert_eq!(
            metadata["delivery_skip_evidence_sha256"],
            "abababababababababababababababababababababababababababababababab"
        );
    }

    #[tokio::test]
    async fn tool_and_approval_attempt_runs_only_through_kernel_edges() {
        let proposal = proposal_id("proposal_harness");
        let approval = approval_id("approval_harness");
        let execution = execution_id("execution_harness");
        let operation = operation_id("operation_harness");
        let (mut sink, journal) = sink();
        let adapter = EmbeddedHarnessAdapter::new(driver(
            vec![
                event(2, HarnessEventKind::ProviderCallStarted),
                event(3, HarnessEventKind::ToolProposed { proposal_id: proposal.clone() }),
                event(
                    4,
                    HarnessEventKind::ApprovalRequired {
                        proposal_id: proposal.clone(),
                        approval_id: approval.clone(),
                    },
                ),
                event(
                    5,
                    HarnessEventKind::ApprovalResolved {
                        proposal_id: proposal.clone(),
                        approval_id: approval,
                        resolution: HarnessApprovalResolution::Approved,
                        evidence_id: None,
                        evidence_sha256: None,
                    },
                ),
                event(
                    6,
                    HarnessEventKind::ToolExecutionStarted {
                        proposal_id: proposal.clone(),
                        execution_id: execution.clone(),
                        operation_id: operation.clone(),
                    },
                ),
                event(
                    7,
                    HarnessEventKind::ToolResultObserved {
                        proposal_id: proposal,
                        execution_id: execution,
                        operation_id: operation,
                    },
                ),
                event(8, HarnessEventKind::ProviderCallStarted),
                event(9, HarnessEventKind::ProviderCallCompleted),
                event(10, HarnessEventKind::VerificationStarted),
                event(11, HarnessEventKind::VerificationPassed),
            ],
            completed(12),
        ));

        let request = sink.request.clone();
        let receipt =
            adapter.run_attempt(&request, &mut sink).await.expect("tool attempt succeeds");

        assert_eq!(receipt.outcome(), KernelTerminalOutcome::Done);
        let names = journal.events().into_iter().map(|event| event.event_name).collect::<Vec<_>>();
        assert!(names.contains(&RuntimeEventName::ApprovalRequired));
        assert!(names.contains(&RuntimeEventName::ApprovalResolved));
        assert!(names.contains(&RuntimeEventName::ToolIntentRecorded));
        assert!(names.contains(&RuntimeEventName::ToolResultObserved));
    }

    #[tokio::test]
    async fn cancellation_terminalizes_once() {
        let (mut sink, _journal) = sink();
        let adapter = EmbeddedHarnessAdapter::new(driver(
            vec![event(2, HarnessEventKind::CancellationObserved)],
            HarnessAttemptTerminal {
                generation: generation(7),
                sequence: 3,
                outcome: HarnessTerminalOutcome::Cancelled {
                    reason_code: "runtime.harness.cancelled".to_owned(),
                },
            },
        ));
        let request = sink.request.clone();
        let receipt =
            adapter.run_attempt(&request, &mut sink).await.expect("cancellation succeeds");
        assert_eq!(receipt.outcome(), KernelTerminalOutcome::Cancelled);
    }

    #[tokio::test]
    async fn compaction_returns_to_provider_before_completion() {
        let (mut sink, journal) = sink();
        let adapter = EmbeddedHarnessAdapter::new(driver(
            vec![
                event(2, HarnessEventKind::ProviderCallStarted),
                event(3, HarnessEventKind::CompactionRequired),
                event(4, HarnessEventKind::CompactionCompleted),
                event(5, HarnessEventKind::ProviderCallCompleted),
                event(6, HarnessEventKind::VerificationStarted),
                event(7, HarnessEventKind::VerificationPassed),
            ],
            completed(8),
        ));
        let request = sink.request.clone();
        adapter.run_attempt(&request, &mut sink).await.expect("compaction attempt succeeds");
        let provider_starts = journal
            .events()
            .iter()
            .filter(|event| event.event_name == RuntimeEventName::ProviderAttemptStarted)
            .count();
        assert_eq!(provider_starts, 2);
    }

    #[tokio::test]
    async fn provider_failure_maps_to_strict_error_envelope_and_run_failure() {
        let (mut sink, journal) = sink();
        let finalize_calls = Arc::new(AtomicUsize::new(0));
        let adapter = EmbeddedHarnessAdapter::new(FakeDriver {
            events: vec![event(2, HarnessEventKind::ProviderCallStarted)],
            terminal: completed(3),
            provider_failure: Some(HarnessProviderFailure {
                reason_code: "provider.timeout".to_owned(),
                retryability:
                    palyra_common::runtime_contracts::RuntimeRetryability::SafeAfterBackoff,
                output_emitted: false,
                safe_message: "provider request timed out".to_owned(),
                recovery_hint: "retry after backoff".to_owned(),
            }),
            finalize_calls: Arc::clone(&finalize_calls),
        });
        let request = sink.request.clone();
        let receipt =
            adapter.run_attempt(&request, &mut sink).await.expect("provider failure terminalizes");
        assert_eq!(receipt.outcome(), KernelTerminalOutcome::Failed);
        let events = journal.events();
        let finalization_index = events
            .iter()
            .position(|event| event.event_name == RuntimeEventName::FinalizationStarted)
            .expect("finalization boundary is durable before the artifact");
        let failure_index = events
            .iter()
            .position(|event| event.event_name == RuntimeEventName::RunFailed)
            .expect("run failure event is committed");
        assert!(finalization_index < failure_index);
        let failed = events
            .into_iter()
            .find(|event| event.event_name == RuntimeEventName::RunFailed)
            .expect("run failure event is committed");
        let RuntimeEventPayloadRef::Inline { metadata } = failed.payload else {
            panic!("run failure uses inline strict metadata");
        };
        assert_eq!(metadata["error"]["reason_code"], "provider.timeout");
        assert_eq!(metadata["error"]["class"], "provider_retryable");
        assert_eq!(finalize_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn delivery_intent_is_projected_only_from_committed_delivery_observation() {
        let (mut sink, journal) = sink();
        sink.accepted(HarnessAccepted { generation: generation(7), sequence: 1 })
            .await
            .expect("acceptance succeeds");
        for (sequence, kind) in [
            (2, HarnessEventKind::ProviderCallStarted),
            (3, HarnessEventKind::ProviderCallCompleted),
            (4, HarnessEventKind::VerificationStarted),
            (5, HarnessEventKind::VerificationPassed),
        ] {
            sink.event(event(sequence, kind)).await.expect("pre-finalization event succeeds");
        }
        assert!(!journal
            .events()
            .iter()
            .any(|event| event.event_name == RuntimeEventName::DeliveryIntentRecorded));
        sink.event(event(
            6,
            HarnessEventKind::DeliveryIntentCommitted {
                delivery_intent_id: RuntimeDeliveryIntentId::parse("delivery_intent_harness")
                    .expect("test delivery intent id is valid"),
                operation_id: operation_id("delivery_operation_harness"),
                output_event_id: RuntimeEventId::parse("output_event_harness")
                    .expect("test output event id is valid"),
            },
        ))
        .await
        .expect("M022 committed delivery observation is accepted");
        sink.terminal(completed(7)).await.expect("committed delivery permits completion");

        let names = journal.events().into_iter().map(|event| event.event_name).collect::<Vec<_>>();
        let delivery_index = names
            .iter()
            .position(|name| *name == RuntimeEventName::DeliveryIntentRecorded)
            .expect("delivery intent is durable");
        let completion_index = names
            .iter()
            .position(|name| *name == RuntimeEventName::RunCompleted)
            .expect("completion is durable");
        assert!(delivery_index < completion_index);
    }

    #[tokio::test]
    async fn post_artifact_recovery_pending_preserves_one_completed_terminal() {
        let (mut sink, journal) = sink();
        sink.accepted(HarnessAccepted { generation: generation(7), sequence: 1 })
            .await
            .expect("acceptance succeeds");
        for (sequence, kind) in [
            (2, HarnessEventKind::ProviderCallStarted),
            (3, HarnessEventKind::ProviderCallCompleted),
            (4, HarnessEventKind::VerificationStarted),
            (5, HarnessEventKind::VerificationPassed),
        ] {
            sink.event(event(sequence, kind)).await.expect("pre-finalization event succeeds");
        }
        sink.event(event(
            6,
            HarnessEventKind::FinalizationRecoveryPending {
                reason_code: "runtime.delivery.outcome_unknown".to_owned(),
                stage: "delivery_execute",
            },
        ))
        .await
        .expect("post-artifact recovery evidence is accepted");
        let receipt = sink.terminal(completed(7)).await.expect("committed outcome must win");
        assert_eq!(receipt.outcome(), KernelTerminalOutcome::Done);
        assert!(matches!(
            sink.terminal(completed(8)).await,
            Err(HarnessContractError::DuplicateTerminal)
        ));
        let completed = journal
            .events()
            .into_iter()
            .find(|event| event.event_name == RuntimeEventName::RunCompleted)
            .expect("run completion is durable");
        let RuntimeEventPayloadRef::Inline { metadata } = completed.payload else {
            panic!("run completion uses inline recovery metadata");
        };
        assert_eq!(metadata["delivery"], "recovery_pending");
        assert_eq!(metadata["reason_code"], "runtime.delivery.outcome_unknown");
        assert_eq!(metadata["stage"], "delivery_execute");
    }

    #[tokio::test]
    async fn duplicate_terminal_is_rejected() {
        let (mut sink, _journal) = sink();
        sink.accepted(HarnessAccepted { generation: generation(7), sequence: 1 })
            .await
            .expect("acceptance succeeds");
        let terminal = HarnessAttemptTerminal {
            generation: generation(7),
            sequence: 2,
            outcome: HarnessTerminalOutcome::Cancelled {
                reason_code: "runtime.harness.cancelled".to_owned(),
            },
        };
        sink.terminal(terminal.clone()).await.expect("first terminal succeeds");
        assert!(matches!(
            sink.terminal(terminal).await,
            Err(HarnessContractError::DuplicateTerminal)
        ));
    }

    #[tokio::test]
    async fn completion_requires_successful_ordered_verification() {
        let (mut sink, _journal) = sink();
        sink.accepted(HarnessAccepted { generation: generation(7), sequence: 1 })
            .await
            .expect("acceptance succeeds");
        sink.event(event(2, HarnessEventKind::ProviderCallStarted)).await.expect("provider starts");
        sink.event(event(3, HarnessEventKind::ProviderCallCompleted))
            .await
            .expect("provider completes");
        assert!(matches!(
            sink.terminal(completed(4)).await,
            Err(HarnessContractError::InvalidTerminal)
        ));

        sink.event(event(4, HarnessEventKind::VerificationStarted))
            .await
            .expect("verification starts after rejected terminal");
        sink.event(event(5, HarnessEventKind::VerificationPassed))
            .await
            .expect("verification passes");
        sink.event(delivery_skipped(6))
            .await
            .expect("hidden output records explicit delivery skip");
        assert_eq!(
            sink.terminal(completed(7)).await.expect("verified completion succeeds").outcome(),
            KernelTerminalOutcome::Done
        );
    }

    #[tokio::test]
    async fn failed_verification_allows_only_failure_or_cancellation_terminal() {
        let (mut sink, journal) = sink();
        sink.accepted(HarnessAccepted { generation: generation(7), sequence: 1 })
            .await
            .expect("acceptance succeeds");
        sink.event(event(2, HarnessEventKind::ProviderCallStarted)).await.expect("provider starts");
        sink.event(event(3, HarnessEventKind::ProviderCallCompleted))
            .await
            .expect("provider completes");
        sink.event(event(4, HarnessEventKind::VerificationStarted))
            .await
            .expect("verification starts");
        let error = HarnessProviderFailure {
            reason_code: "verification.output_invalid".to_owned(),
            retryability: palyra_common::runtime_contracts::RuntimeRetryability::NotRetryable,
            output_emitted: false,
            safe_message: "output verification failed".to_owned(),
            recovery_hint: "retry with corrected output".to_owned(),
        }
        .into_error_envelope()
        .expect("test error envelope is valid");
        sink.event(event(5, HarnessEventKind::VerificationFailed { error: error.clone() }))
            .await
            .expect("verification failure is recorded");
        assert!(matches!(
            sink.terminal(completed(6)).await,
            Err(HarnessContractError::InvalidTerminal)
        ));
        let receipt = sink
            .terminal(HarnessAttemptTerminal {
                generation: generation(7),
                sequence: 6,
                outcome: HarnessTerminalOutcome::Failed { error },
            })
            .await
            .expect("failed verification terminalizes as failure");
        assert_eq!(receipt.outcome(), KernelTerminalOutcome::Failed);
        assert!(journal
            .events()
            .iter()
            .any(|event| event.event_name == RuntimeEventName::RunFailed));
    }

    #[tokio::test]
    async fn duplicate_and_out_of_order_verification_are_rejected() {
        let (mut sink, _journal) = sink();
        sink.accepted(HarnessAccepted { generation: generation(7), sequence: 1 })
            .await
            .expect("acceptance succeeds");
        assert!(matches!(
            sink.event(event(2, HarnessEventKind::VerificationPassed)).await,
            Err(HarnessContractError::InvalidVerificationTransition)
        ));
        sink.event(event(2, HarnessEventKind::VerificationStarted))
            .await
            .expect("first verification start succeeds");
        assert!(matches!(
            sink.event(event(3, HarnessEventKind::VerificationStarted)).await,
            Err(HarnessContractError::InvalidVerificationTransition)
        ));
    }

    #[tokio::test]
    async fn stale_generation_and_event_before_accept_are_rejected() {
        let (mut sink, _journal) = sink();
        assert!(matches!(
            sink.event(event(2, HarnessEventKind::Heartbeat { ordinal: 1 })).await,
            Err(HarnessContractError::EventBeforeAccepted)
        ));
        sink.accepted(HarnessAccepted { generation: generation(7), sequence: 1 })
            .await
            .expect("acceptance succeeds");
        let stale = HarnessEvent {
            generation: generation(8),
            sequence: 2,
            kind: HarnessEventKind::Heartbeat { ordinal: 1 },
        };
        assert!(matches!(
            sink.event(stale).await,
            Err(HarnessContractError::StaleGeneration { .. })
        ));
    }

    #[test]
    fn canary_profile_uses_the_canonical_wire_label() {
        assert_eq!(runtime_profile_str(RuntimeKernelVersion::V2Canary), "v2_canary");
    }

    fn attempt_id(value: &str) -> RuntimeAttemptId {
        RuntimeAttemptId::parse(value).expect("test attempt id is valid")
    }

    fn proposal_id(value: &str) -> RuntimeToolProposalId {
        RuntimeToolProposalId::parse(value).expect("test proposal id is valid")
    }

    fn approval_id(value: &str) -> RuntimeApprovalSubjectId {
        RuntimeApprovalSubjectId::parse(value).expect("test approval id is valid")
    }

    fn execution_id(value: &str) -> RuntimeToolExecutionId {
        RuntimeToolExecutionId::parse(value).expect("test execution id is valid")
    }

    fn operation_id(value: &str) -> RuntimeOperationId {
        RuntimeOperationId::parse(value).expect("test operation id is valid")
    }
}
