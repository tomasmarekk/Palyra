//! Terminal projection and shared sink validation.

use palyra_common::runtime_contracts::{
    RuntimeGeneration, RuntimeGenerationLane, RuntimeIdentitySetV1,
};
use serde_json::json;

use super::{
    DeliveryObservationState, HarnessAttemptTerminal, HarnessContractError, HarnessTerminalReceipt,
    HostHarnessEventSink, KernelState, KernelTransition, RuntimeEventName, VerificationState,
};
use crate::application::runtime_kernel_v2::harness::HarnessTerminalOutcome;

impl HostHarnessEventSink {
    pub(super) fn finish(
        &mut self,
        terminal: HarnessAttemptTerminal,
    ) -> Result<HarnessTerminalReceipt, HarnessContractError> {
        if self.terminal_seen {
            return Err(HarnessContractError::DuplicateTerminal);
        }
        self.require_accepted()?;
        terminal.validate()?;
        self.validate_generation(terminal.generation)?;
        self.validate_next_sequence(terminal.sequence)?;
        match terminal.outcome {
            HarnessTerminalOutcome::Completed => {
                if self.verification != VerificationState::Passed
                    || self.delivery == DeliveryObservationState::Pending
                    || !matches!(
                        (self.delivery, self.kernel.snapshot().state()),
                        (DeliveryObservationState::Committed, KernelState::AwaitingDelivery)
                            | (DeliveryObservationState::Skipped, KernelState::Finalizing)
                            | (DeliveryObservationState::RecoveryPending, KernelState::Finalizing)
                    )
                {
                    return Err(HarnessContractError::InvalidTerminal);
                }
                let identities =
                    self.identities_for_lane(RuntimeGenerationLane::Run, |identities| {
                        identities.attempt_id = Some(self.request.attempt_id().clone());
                    })?;
                let (reason_code, metadata) = match self.delivery {
                    DeliveryObservationState::Committed => (
                        "runtime.harness.completed_after_delivery",
                        json!({"verification": "passed", "delivery": "committed"}),
                    ),
                    DeliveryObservationState::Skipped => {
                        let evidence = self
                            .delivery_skip_evidence
                            .as_ref()
                            .ok_or(HarnessContractError::InvalidTerminal)?;
                        (
                            "runtime.harness.completed_without_delivery",
                            json!({
                                "verification": "passed",
                                "delivery": "skipped",
                                "delivery_skip_evidence_id": evidence.evidence_id.as_str(),
                                "delivery_skip_evidence_sha256": evidence.sha256_hex(),
                            }),
                        )
                    }
                    DeliveryObservationState::RecoveryPending => {
                        let evidence = self
                            .finalization_recovery_evidence
                            .as_ref()
                            .ok_or(HarnessContractError::InvalidTerminal)?;
                        (
                            "runtime.harness.completed_recovery_pending",
                            json!({
                                "verification": "passed",
                                "delivery": "recovery_pending",
                                "reason_code": evidence.reason_code.as_str(),
                                "stage": evidence.stage,
                            }),
                        )
                    }
                    DeliveryObservationState::Pending => {
                        return Err(HarnessContractError::InvalidTerminal);
                    }
                };
                self.apply_transition(
                    RuntimeEventName::RunCompleted,
                    KernelTransition::Complete,
                    identities,
                    reason_code,
                    metadata,
                    terminal.sequence,
                )?;
            }
            HarnessTerminalOutcome::Failed { error } => {
                let identities =
                    self.identities_for_lane(RuntimeGenerationLane::Run, |identities| {
                        identities.attempt_id = Some(self.request.attempt_id().clone());
                    })?;
                self.apply_transition(
                    RuntimeEventName::RunFailed,
                    KernelTransition::Fail,
                    identities,
                    "runtime.harness.failed",
                    json!({"error": error}),
                    terminal.sequence,
                )?;
            }
            HarnessTerminalOutcome::Cancelled { reason_code } => {
                let identities =
                    self.identities_for_lane(RuntimeGenerationLane::Run, |identities| {
                        identities.attempt_id = Some(self.request.attempt_id().clone());
                    })?;
                self.apply_transition(
                    RuntimeEventName::RunCancelled,
                    KernelTransition::Cancel,
                    identities,
                    reason_code.as_str(),
                    json!({}),
                    terminal.sequence,
                )?;
            }
        }

        let outcome = self
            .kernel
            .snapshot()
            .terminal_outcome()
            .ok_or(HarnessContractError::InvalidTerminal)?;
        self.terminal_seen = true;
        self.last_harness_sequence = terminal.sequence;
        Ok(HarnessTerminalReceipt {
            outcome,
            kernel_revision: self.kernel.snapshot().revision(),
            harness_terminal_sequence: terminal.sequence,
            observations_accepted: self.observations_accepted,
            prompt_tokens: self.prompt_tokens,
            completion_tokens: self.completion_tokens,
            verification_passed: self.verification == VerificationState::Passed,
        })
    }

    pub(super) fn identities_for_lane(
        &self,
        lane: RuntimeGenerationLane,
        bind: impl FnOnce(&mut RuntimeIdentitySetV1),
    ) -> Result<RuntimeIdentitySetV1, HarnessContractError> {
        let generation = self
            .lane_authority
            .leases()
            .iter()
            .find(|lease| lease.lane == lane)
            .map(|lease| lease.generation)
            .ok_or(HarnessContractError::InvalidAttemptRequest)?;
        let mut identities = self.request.identities().clone();
        identities.generation = generation;
        bind(&mut identities);
        Ok(identities)
    }

    pub(super) fn require_open(&self) -> Result<(), HarnessContractError> {
        self.require_accepted()?;
        if self.terminal_seen {
            return Err(HarnessContractError::EventAfterTerminal);
        }
        Ok(())
    }

    pub(super) fn require_accepted(&self) -> Result<(), HarnessContractError> {
        if self.accepted {
            Ok(())
        } else {
            Err(HarnessContractError::EventBeforeAccepted)
        }
    }

    pub(super) fn validate_generation(
        &self,
        observed: RuntimeGeneration,
    ) -> Result<(), HarnessContractError> {
        let active = self.request.generation();
        if observed == active {
            Ok(())
        } else {
            Err(HarnessContractError::StaleGeneration { active, observed })
        }
    }

    pub(super) fn validate_next_sequence(&self, observed: u64) -> Result<(), HarnessContractError> {
        if observed > self.last_harness_sequence {
            Ok(())
        } else {
            Err(HarnessContractError::NonMonotonicSequence {
                last: self.last_harness_sequence,
                observed,
            })
        }
    }
}
