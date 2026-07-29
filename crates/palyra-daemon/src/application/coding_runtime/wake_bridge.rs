use super::super::process_supervisor::ProcessSessionRecordV2;
use super::super::pty_backend::{PtyExitOutcomeV1, PtySessionDescriptorV1};
use super::contracts::{
    CodingObjectiveWaitContextV2, CodingWaitBarrierReceiptV2, CodingWakeReceiptV2,
};

/// Durable adapter used by managed command completion to resume an objective.
pub trait CodingWakeBridge: Send + Sync {
    /// Registers the process source before waiting for completion.
    fn register_process_wait(
        &self,
        context: &CodingObjectiveWaitContextV2,
        process: &ProcessSessionRecordV2,
    ) -> Result<CodingWaitBarrierReceiptV2, String>;

    /// Emits completion only after output drain and process-tree cleanup settle.
    fn emit_process_completion(
        &self,
        barrier: &CodingWaitBarrierReceiptV2,
        process: &ProcessSessionRecordV2,
    ) -> Result<CodingWakeReceiptV2, String>;

    /// Registers a native terminal source before returning control to the turn.
    fn register_terminal_wait(
        &self,
        _context: &CodingObjectiveWaitContextV2,
        _terminal: &PtySessionDescriptorV1,
    ) -> Result<CodingWaitBarrierReceiptV2, String> {
        Err("terminal completion waits are unavailable".to_owned())
    }

    /// Emits completion after the terminal actor verifies tree cleanup.
    fn emit_terminal_completion(
        &self,
        _barrier: &CodingWaitBarrierReceiptV2,
        _terminal: &PtySessionDescriptorV1,
        _outcome: &PtyExitOutcomeV1,
    ) -> Result<CodingWakeReceiptV2, String> {
        Err("terminal completion wakes are unavailable".to_owned())
    }
}
