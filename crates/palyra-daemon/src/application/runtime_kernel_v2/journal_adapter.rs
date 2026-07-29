//! Private journal capability used by the host harness event sink.
//!
//! This adapter is the only component allowed to commit prepared kernel
//! transitions and restore the immutable kernel from the committed snapshot.

use std::sync::Arc;

use palyra_common::runtime_contracts::RuntimeGeneration;
use thiserror::Error;

use super::{
    rollback::VerifiedRuntimeRollbackSafeBoundary, KernelStateSnapshot, KernelTransitionError,
    RuntimeKernelV2,
};
use crate::journal::{
    runtime_kernel::{
        RuntimeKernelObservationCommitRequest, RuntimeKernelTransitionCommitOutcome,
        RuntimeRollbackBoundaryOutcome,
    },
    JournalError, JournalStore,
};

pub(super) trait KernelJournalPort: Send + Sync {
    fn commit_observation(
        &self,
        request: &RuntimeKernelObservationCommitRequest,
    ) -> Result<KernelJournalCommit, KernelJournalAdapterError>;

    fn apply_pending_rollback(
        &self,
        boundary: &VerifiedRuntimeRollbackSafeBoundary,
    ) -> Result<RuntimeRollbackBoundaryOutcome, KernelJournalAdapterError>;
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum KernelJournalCommit {
    Applied(KernelStateSnapshot),
    AlreadyApplied(KernelStateSnapshot),
    StaleSuppressed { active_generation: Option<RuntimeGeneration> },
}

impl KernelJournalPort for JournalStore {
    fn commit_observation(
        &self,
        request: &RuntimeKernelObservationCommitRequest,
    ) -> Result<KernelJournalCommit, KernelJournalAdapterError> {
        match self.commit_runtime_kernel_observation(request)? {
            RuntimeKernelTransitionCommitOutcome::Applied { snapshot, .. } => {
                Ok(KernelJournalCommit::Applied(snapshot))
            }
            RuntimeKernelTransitionCommitOutcome::AlreadyApplied { snapshot, .. } => {
                Ok(KernelJournalCommit::AlreadyApplied(snapshot))
            }
            RuntimeKernelTransitionCommitOutcome::StaleSuppressed { active_generation } => {
                Ok(KernelJournalCommit::StaleSuppressed { active_generation })
            }
        }
    }

    fn apply_pending_rollback(
        &self,
        boundary: &VerifiedRuntimeRollbackSafeBoundary,
    ) -> Result<RuntimeRollbackBoundaryOutcome, KernelJournalAdapterError> {
        Ok(self.apply_pending_runtime_rollback_at_safe_boundary(boundary)?)
    }
}

impl KernelJournalPort for crate::gateway::GatewayRuntimeState {
    fn commit_observation(
        &self,
        request: &RuntimeKernelObservationCommitRequest,
    ) -> Result<KernelJournalCommit, KernelJournalAdapterError> {
        self.journal_store.commit_observation(request)
    }

    fn apply_pending_rollback(
        &self,
        boundary: &VerifiedRuntimeRollbackSafeBoundary,
    ) -> Result<RuntimeRollbackBoundaryOutcome, KernelJournalAdapterError> {
        self.journal_store.apply_pending_rollback(boundary)
    }
}

pub(super) struct KernelJournalAdapter {
    port: Arc<dyn KernelJournalPort>,
}

impl std::fmt::Debug for KernelJournalAdapter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("KernelJournalAdapter").finish_non_exhaustive()
    }
}

impl KernelJournalAdapter {
    pub(super) fn from_runtime_state(
        runtime_state: Arc<crate::gateway::GatewayRuntimeState>,
    ) -> Self {
        Self { port: runtime_state }
    }

    #[cfg(test)]
    pub(super) fn from_test_port(port: Arc<dyn KernelJournalPort>) -> Self {
        Self { port }
    }

    pub(super) fn commit_observation_and_restore(
        &self,
        request: &RuntimeKernelObservationCommitRequest,
    ) -> Result<RuntimeKernelV2, KernelJournalAdapterError> {
        let snapshot = match self.port.commit_observation(request)? {
            KernelJournalCommit::Applied(snapshot)
            | KernelJournalCommit::AlreadyApplied(snapshot) => snapshot,
            KernelJournalCommit::StaleSuppressed { active_generation } => {
                return Err(KernelJournalAdapterError::StaleSuppressed { active_generation });
            }
        };
        RuntimeKernelV2::restore_from_journal(snapshot).map_err(KernelJournalAdapterError::Restore)
    }

    pub(super) fn apply_pending_rollback_and_restore(
        &self,
        boundary: &VerifiedRuntimeRollbackSafeBoundary,
    ) -> Result<Option<RuntimeKernelV2>, KernelJournalAdapterError> {
        match self.port.apply_pending_rollback(boundary)? {
            RuntimeRollbackBoundaryOutcome::NoRequest
            | RuntimeRollbackBoundaryOutcome::FinishAllowed
            | RuntimeRollbackBoundaryOutcome::TerminalNoAction => Ok(None),
            RuntimeRollbackBoundaryOutcome::Suspended { snapshot, .. } => {
                RuntimeKernelV2::restore_from_journal(*snapshot)
                    .map(Some)
                    .map_err(KernelJournalAdapterError::Restore)
            }
            RuntimeRollbackBoundaryOutcome::StaleDenied { expected_revision, actual_revision } => {
                Err(KernelJournalAdapterError::RollbackStaleDenied {
                    expected_revision,
                    actual_revision,
                })
            }
        }
    }
}

/// Failure at the private prepared-transition commit/restore boundary.
#[derive(Debug, Error)]
pub(crate) enum KernelJournalAdapterError {
    #[error("runtime kernel journal commit failed")]
    Commit(#[from] JournalError),
    #[error("runtime kernel transition was stale at commit")]
    StaleSuppressed { active_generation: Option<RuntimeGeneration> },
    #[error(
        "runtime kernel rollback boundary was stale: expected revision {expected_revision}, actual revision {actual_revision:?}"
    )]
    RollbackStaleDenied { expected_revision: u64, actual_revision: Option<u64> },
    #[error("runtime kernel journal restored an invalid snapshot")]
    Restore(#[source] KernelTransitionError),
}
