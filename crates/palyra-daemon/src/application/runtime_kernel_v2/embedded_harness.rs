//! In-process adapter for the provisional async RuntimeKernelV2 harness contract.
//!
//! The adapter delegates one attempt to a neutral host driver and only relays
//! typed events. It cannot execute tools, write the journal, or mint receipts.

use super::{
    harness::{
        HarnessAccepted, HarnessAttemptRequest, HarnessAttemptTerminal, HarnessContractError,
        HarnessEvent, HarnessEventSink, HarnessFuture, HarnessRuntimeV2, HarnessTerminalOutcome,
    },
    host_event_contract::HarnessTerminalReceipt,
};
use palyra_common::runtime_contracts::RuntimeErrorEnvelopeV1;
use palyra_common::runtime_contracts::{
    RuntimeErrorClass, RuntimeErrorEnvelopeV1Input, RuntimeErrorPhase, RuntimeErrorSecurityClass,
    RuntimeErrorUserVisibility, RuntimeRetryability, RuntimeSubsystem,
};

#[cfg(test)]
use super::harness::HarnessProviderFailure;

/// Event-only port exposed to the embedded attempt driver.
///
/// Terminalization remains adapter-controlled so a driver cannot bypass the
/// host sink's exactly-once terminal reservation.
pub(crate) trait EmbeddedHarnessEventPort: Send {
    fn emit<'a>(
        &'a mut self,
        event: HarnessEvent,
    ) -> HarnessFuture<'a, Result<(), HarnessContractError>>;
}

/// Neutral host driver for the current in-process provider/tool orchestration.
///
/// A live implementation may call host provider, tool, approval, compaction,
/// and verification services. It must not call the legacy run loop wholesale.
pub(crate) trait EmbeddedAttemptDriver: Send + Sync {
    fn run_attempt<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
        events: &'a mut dyn EmbeddedHarnessEventPort,
    ) -> HarnessFuture<'a, Result<HarnessAttemptTerminal, EmbeddedAttemptError>>;

    /// Emits `FinalizationReady` before committing exactly one finalization artifact.
    fn finalize_attempt<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
        terminal: HarnessAttemptTerminal,
        events: &'a mut dyn EmbeddedHarnessEventPort,
    ) -> HarnessFuture<'a, Result<HarnessAttemptTerminal, EmbeddedAttemptError>>;
}

/// Failure returned by the neutral embedded driver boundary.
#[derive(Debug)]
pub(crate) enum EmbeddedAttemptError {
    /// Provider evidence that the adapter must map into the strict error taxonomy.
    #[cfg(test)]
    Provider(HarnessProviderFailure),
    /// Strict host-phase failure already classified at its owning boundary.
    Terminal(RuntimeErrorEnvelopeV1),
    /// A host contract failure classified into one failed terminal unless authority is stale.
    Contract(HarnessContractError),
    /// The immutable final artifact already committed the original terminal outcome.
    PostFinalization {
        terminal: HarnessAttemptTerminal,
        error: RuntimeErrorEnvelopeV1,
        stage: &'static str,
    },
}

/// Async adapter that gives the embedded driver the same event contract as
/// future process- or plugin-backed harnesses.
#[derive(Debug)]
pub(crate) struct EmbeddedHarnessAdapter<D> {
    driver: D,
}

impl<D> EmbeddedHarnessAdapter<D> {
    /// Creates an adapter around a host-owned attempt driver.
    #[must_use]
    pub(crate) const fn new(driver: D) -> Self {
        Self { driver }
    }
}

impl<D> HarnessRuntimeV2 for EmbeddedHarnessAdapter<D>
where
    D: EmbeddedAttemptDriver,
{
    fn run_attempt<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
        sink: &'a mut dyn HarnessEventSink,
    ) -> HarnessFuture<'a, Result<HarnessTerminalReceipt, HarnessContractError>> {
        Box::pin(async move {
            sink.accepted(HarnessAccepted { generation: request.generation(), sequence: 1 })
                .await?;

            let mut relay = EmbeddedEventRelay {
                sink,
                last_sequence: 1,
                output_emitted: false,
                side_effect_may_have_occurred: false,
            };
            let (terminal, already_finalized) =
                match self.driver.run_attempt(request, &mut relay).await {
                    Ok(terminal) => (terminal, false),
                    #[cfg(test)]
                    Err(EmbeddedAttemptError::Provider(failure)) => (
                        HarnessAttemptTerminal {
                            generation: request.generation(),
                            sequence: relay
                                .last_sequence
                                .checked_add(1)
                                .ok_or(HarnessContractError::InvalidTerminal)?,
                            outcome: HarnessTerminalOutcome::Failed {
                                error: failure.into_error_envelope()?,
                            },
                        },
                        false,
                    ),
                    Err(EmbeddedAttemptError::Terminal(error)) => (
                        HarnessAttemptTerminal {
                            generation: request.generation(),
                            sequence: relay
                                .last_sequence
                                .checked_add(1)
                                .ok_or(HarnessContractError::InvalidTerminal)?,
                            outcome: HarnessTerminalOutcome::Failed { error },
                        },
                        false,
                    ),
                    Err(EmbeddedAttemptError::Contract(error)) => {
                        if should_stop_without_terminal(&error) {
                            return Err(error);
                        }
                        (
                            failed_terminal(
                                request,
                                relay.last_sequence,
                                contract_failure_envelope(
                                    &error,
                                    RuntimeErrorPhase::Internal,
                                    relay.output_emitted,
                                    relay.side_effect_may_have_occurred,
                                )?,
                            )?,
                            false,
                        )
                    }
                    Err(EmbeddedAttemptError::PostFinalization { terminal, error, stage }) => (
                        preserve_post_finalization_terminal(
                            request, &mut relay, terminal, error, stage,
                        )
                        .await?,
                        true,
                    ),
                };
            let terminal_sequence = terminal.sequence;
            let terminal = if already_finalized {
                terminal
            } else {
                match self.driver.finalize_attempt(request, terminal, &mut relay).await {
                    Ok(terminal) => terminal,
                    Err(EmbeddedAttemptError::Contract(error))
                        if should_stop_without_terminal(&error) =>
                    {
                        return Err(error);
                    }
                    Err(EmbeddedAttemptError::PostFinalization { terminal, error, stage }) => {
                        preserve_post_finalization_terminal(
                            request, &mut relay, terminal, error, stage,
                        )
                        .await?
                    }
                    Err(error) => failed_terminal(
                        request,
                        relay.last_sequence.max(terminal_sequence),
                        embedded_failure_envelope(
                            &error,
                            RuntimeErrorPhase::Finalization,
                            relay.output_emitted,
                            relay.side_effect_may_have_occurred,
                        )?,
                    )?,
                }
            };
            relay.sink.terminal(terminal).await
        })
    }
}

struct EmbeddedEventRelay<'a> {
    sink: &'a mut dyn HarnessEventSink,
    last_sequence: u64,
    output_emitted: bool,
    side_effect_may_have_occurred: bool,
}

async fn preserve_post_finalization_terminal(
    request: &HarnessAttemptRequest,
    relay: &mut EmbeddedEventRelay<'_>,
    mut terminal: HarnessAttemptTerminal,
    error: RuntimeErrorEnvelopeV1,
    stage: &'static str,
) -> Result<HarnessAttemptTerminal, HarnessContractError> {
    if matches!(terminal.outcome, HarnessTerminalOutcome::Completed) {
        let sequence =
            relay.last_sequence.checked_add(1).ok_or(HarnessContractError::InvalidTerminal)?;
        relay
            .emit(HarnessEvent {
                generation: request.generation(),
                sequence,
                kind: super::harness::HarnessEventKind::FinalizationRecoveryPending {
                    reason_code: error.reason_code().to_owned(),
                    stage,
                },
            })
            .await?;
        terminal.sequence =
            relay.last_sequence.checked_add(1).ok_or(HarnessContractError::InvalidTerminal)?;
    }
    Ok(terminal)
}

impl EmbeddedHarnessEventPort for EmbeddedEventRelay<'_> {
    fn emit<'a>(
        &'a mut self,
        event: HarnessEvent,
    ) -> HarnessFuture<'a, Result<(), HarnessContractError>> {
        Box::pin(async move {
            let sequence = event.sequence;
            let output_emitted =
                matches!(&event.kind, super::harness::HarnessEventKind::TextDelta { .. });
            let side_effect_may_have_occurred = matches!(
                &event.kind,
                super::harness::HarnessEventKind::ToolExecutionStarted { .. }
                    | super::harness::HarnessEventKind::ToolResultObserved { .. }
                    | super::harness::HarnessEventKind::DeliveryIntentCommitted { .. }
                    | super::harness::HarnessEventKind::FinalizationRecoveryPending { .. }
            );
            self.sink.event(event).await?;
            self.last_sequence = sequence;
            self.output_emitted |= output_emitted;
            self.side_effect_may_have_occurred |= side_effect_may_have_occurred;
            Ok(())
        })
    }
}

fn failed_terminal(
    request: &HarnessAttemptRequest,
    last_sequence: u64,
    error: RuntimeErrorEnvelopeV1,
) -> Result<HarnessAttemptTerminal, HarnessContractError> {
    Ok(HarnessAttemptTerminal {
        generation: request.generation(),
        sequence: last_sequence.checked_add(1).ok_or(HarnessContractError::InvalidTerminal)?,
        outcome: HarnessTerminalOutcome::Failed { error },
    })
}

fn embedded_failure_envelope(
    error: &EmbeddedAttemptError,
    phase: RuntimeErrorPhase,
    output_emitted: bool,
    side_effect_may_have_occurred: bool,
) -> Result<RuntimeErrorEnvelopeV1, HarnessContractError> {
    match error {
        #[cfg(test)]
        EmbeddedAttemptError::Provider(provider) => provider.clone().into_error_envelope(),
        EmbeddedAttemptError::Terminal(error) => Ok(error.clone()),
        EmbeddedAttemptError::PostFinalization { error, .. } => Ok(error.clone()),
        EmbeddedAttemptError::Contract(error) => {
            contract_failure_envelope(error, phase, output_emitted, side_effect_may_have_occurred)
        }
    }
}

fn contract_failure_envelope(
    error: &HarnessContractError,
    phase: RuntimeErrorPhase,
    output_emitted: bool,
    side_effect_may_have_occurred: bool,
) -> Result<RuntimeErrorEnvelopeV1, HarnessContractError> {
    let reason_code = match error {
        HarnessContractError::EventLimitExceeded => "runtime.harness.event_limit_exceeded",
        HarnessContractError::NonMonotonicSequence { .. } => {
            "runtime.harness.non_monotonic_sequence"
        }
        HarnessContractError::InvalidVerificationTransition => {
            "runtime.harness.invalid_verification_transition"
        }
        HarnessContractError::KernelTransition(_) => "runtime.harness.kernel_transition_failed",
        HarnessContractError::Journal(_) => "runtime.harness.journal_boundary_failed",
        HarnessContractError::RollbackSuspended => "runtime.harness.rollback_suspended",
        HarnessContractError::HostEventMetadata => "runtime.harness.metadata_authority_unavailable",
        HarnessContractError::InvalidAttemptRequest
        | HarnessContractError::UnsupportedRuntimeSelection
        | HarnessContractError::EventBeforeAccepted
        | HarnessContractError::DuplicateAccepted
        | HarnessContractError::DuplicateTerminal
        | HarnessContractError::EventAfterTerminal
        | HarnessContractError::StaleGeneration { .. }
        | HarnessContractError::InvalidEvent
        | HarnessContractError::InvalidTerminal => "runtime.harness.contract_violation",
        #[cfg(test)]
        HarnessContractError::InvalidProviderFailure => "runtime.harness.contract_violation",
    };
    RuntimeErrorEnvelopeV1::try_new(RuntimeErrorEnvelopeV1Input {
        class: RuntimeErrorClass::InternalInvariantViolation,
        reason_code: reason_code.to_owned(),
        subsystem: RuntimeSubsystem::RuntimeKernel,
        phase,
        retryability: RuntimeRetryability::NotRetryable,
        security_class: RuntimeErrorSecurityClass::Internal,
        user_visibility: RuntimeErrorUserVisibility::StatusOnly,
        output_emitted,
        side_effect_may_have_occurred,
        safe_message: "The runtime attempt stopped at a protected boundary.".to_owned(),
        recovery_hint: "Start a new run after reviewing runtime diagnostics.".to_owned(),
    })
    .map_err(|_| HarnessContractError::InvalidTerminal)
}

const fn should_stop_without_terminal(error: &HarnessContractError) -> bool {
    matches!(
        error,
        HarnessContractError::StaleGeneration { .. }
            | HarnessContractError::RollbackSuspended
            | HarnessContractError::Journal(
                super::journal_adapter::KernelJournalAdapterError::RollbackStaleDenied { .. }
            )
    )
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use palyra_common::runtime_contracts::{
        RuntimeAttemptId, RuntimeGeneration, RuntimeIdentitySetV1,
    };

    use super::*;
    use crate::application::runtime_kernel_v2::{
        harness::{HarnessEvent, HarnessEventKind, HarnessEventSink},
        KernelTerminalOutcome, RuntimeKernelVersion,
    };

    struct RecordingSink {
        terminal: Option<HarnessAttemptTerminal>,
        terminal_calls: usize,
    }

    impl HarnessEventSink for RecordingSink {
        fn accepted<'a>(
            &'a mut self,
            _accepted: HarnessAccepted,
        ) -> HarnessFuture<'a, Result<(), HarnessContractError>> {
            Box::pin(async { Ok(()) })
        }

        fn event<'a>(
            &'a mut self,
            _event: HarnessEvent,
        ) -> HarnessFuture<'a, Result<(), HarnessContractError>> {
            Box::pin(async { Ok(()) })
        }

        fn terminal<'a>(
            &'a mut self,
            terminal: HarnessAttemptTerminal,
        ) -> HarnessFuture<'a, Result<HarnessTerminalReceipt, HarnessContractError>> {
            self.terminal_calls += 1;
            self.terminal = Some(terminal.clone());
            Box::pin(async move {
                Ok(HarnessTerminalReceipt {
                    outcome: match terminal.outcome {
                        HarnessTerminalOutcome::Completed => KernelTerminalOutcome::Done,
                        HarnessTerminalOutcome::Failed { .. } => KernelTerminalOutcome::Failed,
                        HarnessTerminalOutcome::Cancelled { .. } => {
                            KernelTerminalOutcome::Cancelled
                        }
                    },
                    kernel_revision: 1,
                    harness_terminal_sequence: terminal.sequence,
                    observations_accepted: 0,
                    prompt_tokens: 0,
                    completion_tokens: 0,
                    verification_passed: false,
                })
            })
        }
    }

    struct ContractFailureDriver {
        finalize_calls: Arc<AtomicUsize>,
    }

    impl EmbeddedAttemptDriver for ContractFailureDriver {
        fn run_attempt<'a>(
            &'a self,
            _request: &'a HarnessAttemptRequest,
            _events: &'a mut dyn EmbeddedHarnessEventPort,
        ) -> HarnessFuture<'a, Result<HarnessAttemptTerminal, EmbeddedAttemptError>> {
            Box::pin(async {
                Err(EmbeddedAttemptError::Contract(HarnessContractError::EventLimitExceeded))
            })
        }

        fn finalize_attempt<'a>(
            &'a self,
            _request: &'a HarnessAttemptRequest,
            terminal: HarnessAttemptTerminal,
            _events: &'a mut dyn EmbeddedHarnessEventPort,
        ) -> HarnessFuture<'a, Result<HarnessAttemptTerminal, EmbeddedAttemptError>> {
            self.finalize_calls.fetch_add(1, Ordering::Relaxed);
            Box::pin(async move { Ok(terminal) })
        }
    }

    struct FinalizationFailureDriver {
        finalize_calls: Arc<AtomicUsize>,
    }

    struct PostFinalizationFailureDriver {
        stage: &'static str,
    }

    impl EmbeddedAttemptDriver for PostFinalizationFailureDriver {
        fn run_attempt<'a>(
            &'a self,
            request: &'a HarnessAttemptRequest,
            events: &'a mut dyn EmbeddedHarnessEventPort,
        ) -> HarnessFuture<'a, Result<HarnessAttemptTerminal, EmbeddedAttemptError>> {
            Box::pin(async move {
                for (sequence, kind) in [
                    (2, HarnessEventKind::ProviderCallCompleted),
                    (3, HarnessEventKind::VerificationStarted),
                    (4, HarnessEventKind::VerificationPassed),
                ] {
                    events
                        .emit(HarnessEvent { generation: request.generation(), sequence, kind })
                        .await
                        .map_err(EmbeddedAttemptError::Contract)?;
                }
                Ok(HarnessAttemptTerminal {
                    generation: request.generation(),
                    sequence: 5,
                    outcome: HarnessTerminalOutcome::Completed,
                })
            })
        }

        fn finalize_attempt<'a>(
            &'a self,
            _request: &'a HarnessAttemptRequest,
            terminal: HarnessAttemptTerminal,
            _events: &'a mut dyn EmbeddedHarnessEventPort,
        ) -> HarnessFuture<'a, Result<HarnessAttemptTerminal, EmbeddedAttemptError>> {
            Box::pin(async move {
                Err(EmbeddedAttemptError::PostFinalization {
                    terminal,
                    error: contract_failure_envelope(
                        &HarnessContractError::InvalidTerminal,
                        RuntimeErrorPhase::Finalization,
                        true,
                        true,
                    )
                    .map_err(EmbeddedAttemptError::Contract)?,
                    stage: self.stage,
                })
            })
        }
    }

    impl EmbeddedAttemptDriver for FinalizationFailureDriver {
        fn run_attempt<'a>(
            &'a self,
            request: &'a HarnessAttemptRequest,
            _events: &'a mut dyn EmbeddedHarnessEventPort,
        ) -> HarnessFuture<'a, Result<HarnessAttemptTerminal, EmbeddedAttemptError>> {
            Box::pin(async move {
                Ok(HarnessAttemptTerminal {
                    generation: request.generation(),
                    sequence: 2,
                    outcome: HarnessTerminalOutcome::Completed,
                })
            })
        }

        fn finalize_attempt<'a>(
            &'a self,
            _request: &'a HarnessAttemptRequest,
            _terminal: HarnessAttemptTerminal,
            _events: &'a mut dyn EmbeddedHarnessEventPort,
        ) -> HarnessFuture<'a, Result<HarnessAttemptTerminal, EmbeddedAttemptError>> {
            self.finalize_calls.fetch_add(1, Ordering::Relaxed);
            Box::pin(async {
                Err(EmbeddedAttemptError::Contract(HarnessContractError::InvalidTerminal))
            })
        }
    }

    fn request() -> HarnessAttemptRequest {
        let generation = RuntimeGeneration::new(7).expect("generation should validate");
        let (identities, _) = RuntimeIdentitySetV1::from_legacy_run(
            "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "01ARZ3NDEKTSV4RRFFQ69G5FAW",
            generation,
        )
        .expect("identities should validate");
        HarnessAttemptRequest::from_host_parts(
            identities,
            RuntimeAttemptId::parse("attempt_embedded_failure")
                .expect("attempt id should validate"),
            "embedded_run_stream".to_owned(),
            RuntimeKernelVersion::V2,
        )
        .expect("request should validate")
    }

    #[tokio::test]
    async fn turn_cap_contract_failure_runs_finalization_and_emits_one_failed_terminal() {
        let finalize_calls = Arc::new(AtomicUsize::new(0));
        let adapter = EmbeddedHarnessAdapter::new(ContractFailureDriver {
            finalize_calls: Arc::clone(&finalize_calls),
        });
        let mut sink = RecordingSink { terminal: None, terminal_calls: 0 };

        adapter
            .run_attempt(&request(), &mut sink)
            .await
            .expect("contract failure should terminalize durably");

        assert_eq!(finalize_calls.load(Ordering::Relaxed), 1);
        let terminal = sink.terminal.expect("failed terminal should be emitted");
        let HarnessTerminalOutcome::Failed { error } = terminal.outcome else {
            panic!("turn-cap failure must emit Failed");
        };
        assert_eq!(error.reason_code(), "runtime.harness.event_limit_exceeded");
    }

    #[tokio::test]
    async fn finalization_failure_is_replaced_by_one_classified_failed_terminal() {
        let finalize_calls = Arc::new(AtomicUsize::new(0));
        let adapter = EmbeddedHarnessAdapter::new(FinalizationFailureDriver {
            finalize_calls: Arc::clone(&finalize_calls),
        });
        let mut sink = RecordingSink { terminal: None, terminal_calls: 0 };

        adapter
            .run_attempt(&request(), &mut sink)
            .await
            .expect("finalization failure should terminalize durably");

        assert_eq!(finalize_calls.load(Ordering::Relaxed), 1);
        let terminal = sink.terminal.expect("failed terminal should be emitted");
        let HarnessTerminalOutcome::Failed { error } = terminal.outcome else {
            panic!("finalization failure must emit Failed");
        };
        assert_eq!(error.phase(), RuntimeErrorPhase::Finalization);
        assert_eq!(error.reason_code(), "runtime.harness.contract_violation");
    }

    #[tokio::test]
    async fn every_post_commit_failure_preserves_the_single_committed_terminal() {
        for stage in [
            "accept_finalization",
            "delivery_plan",
            "delivery_input",
            "delivery_execute",
            "accept_delivery",
            "intent_binding",
        ] {
            let adapter = EmbeddedHarnessAdapter::new(PostFinalizationFailureDriver { stage });
            let mut sink = RecordingSink { terminal: None, terminal_calls: 0 };

            adapter
                .run_attempt(&request(), &mut sink)
                .await
                .expect("post-commit failure should preserve the committed outcome");

            assert_eq!(sink.terminal_calls, 1, "{stage} must emit exactly one terminal");
            assert!(
                matches!(
                    sink.terminal.expect("terminal should be emitted").outcome,
                    HarnessTerminalOutcome::Completed
                ),
                "{stage} must not contradict the immutable completed artifact"
            );
        }
    }
}
