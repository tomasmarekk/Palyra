//! Trait and diagnostic interfaces for the host-owned harness event sink.
//!
//! Keeping these boundary implementations separate leaves event validation
//! and state projection in the parent module without widening its internals.

use super::{
    HarnessAccepted, HarnessAttemptTerminal, HarnessContractError, HarnessEvent, HarnessEventSink,
    HarnessFuture, HarnessTerminalReceipt, HostHarnessEventSink,
};

impl std::fmt::Debug for HostHarnessEventSink {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HostHarnessEventSink")
            .field("attempt_id", &self.request.attempt_id())
            .field("generation", &self.request.generation())
            .field("kernel_state", &self.kernel.snapshot().state())
            .field("accepted", &self.accepted)
            .field("terminal_seen", &self.terminal_seen)
            .field("observations_accepted", &self.observations_accepted)
            .finish_non_exhaustive()
    }
}

impl HarnessEventSink for HostHarnessEventSink {
    fn accepted<'a>(
        &'a mut self,
        accepted: HarnessAccepted,
    ) -> HarnessFuture<'a, Result<(), HarnessContractError>> {
        Box::pin(async move { self.accept(accepted) })
    }

    fn event<'a>(
        &'a mut self,
        event: HarnessEvent,
    ) -> HarnessFuture<'a, Result<(), HarnessContractError>> {
        Box::pin(async move { self.observe(event) })
    }

    fn terminal<'a>(
        &'a mut self,
        terminal: HarnessAttemptTerminal,
    ) -> HarnessFuture<'a, Result<HarnessTerminalReceipt, HarnessContractError>> {
        Box::pin(async move { self.finish(terminal) })
    }
}
