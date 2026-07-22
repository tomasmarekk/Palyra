//! Pure rollback planning for active RuntimeKernelV2 generations.
//!
//! Plans retain persisted authority and contain no journal-delete, runtime-switch,
//! or side-effect-replay operation. The dispatcher may act only at the declared
//! safe boundary.

use palyra_common::runtime_contracts::RuntimeEventName;
use serde::Serialize;
use thiserror::Error;

use crate::config::RuntimeKernelRollbackPolicy;

use super::selection::{RuntimeAuthority, RuntimeAuthorityDecisionV1, RuntimeAuthorityError};
use super::{KernelState, KernelStateSnapshot};

/// Host-sealed proof that one canonical event reached a rollback-safe handoff.
///
/// The token can only be minted inside the RuntimeKernelV2 host adapter. The
/// journal still compares the embedded snapshot with its exact durable head
/// before it commits a suspension.
#[derive(Debug, Clone)]
pub(crate) struct VerifiedRuntimeRollbackSafeBoundary {
    snapshot: KernelStateSnapshot,
    event_name: RuntimeEventName,
}

impl VerifiedRuntimeRollbackSafeBoundary {
    /// Seals a just-committed host event when work has not yet entered its next phase.
    #[must_use]
    pub(super) fn after_host_event(
        snapshot: &KernelStateSnapshot,
        event_name: RuntimeEventName,
    ) -> Option<Self> {
        if event_opens_safe_handoff(snapshot.state(), event_name) {
            Some(Self { snapshot: snapshot.clone(), event_name })
        } else {
            None
        }
    }

    /// Returns the exact durable snapshot observed at the host handoff.
    #[must_use]
    pub(crate) const fn snapshot(&self) -> &KernelStateSnapshot {
        &self.snapshot
    }

    /// Returns the event that established the handoff.
    #[must_use]
    pub(crate) const fn event_name(&self) -> RuntimeEventName {
        self.event_name
    }

    #[cfg(test)]
    pub(crate) fn for_test(snapshot: &KernelStateSnapshot, event_name: RuntimeEventName) -> Self {
        Self::after_host_event(snapshot, event_name)
            .expect("test event and snapshot must form a safe handoff")
    }
}

const fn event_opens_safe_handoff(state: KernelState, event_name: RuntimeEventName) -> bool {
    matches!(
        (state, event_name),
        (KernelState::SelectingRuntime, RuntimeEventName::RunStarted)
            | (KernelState::AssemblingContext, RuntimeEventName::HarnessAttemptStarted)
            | (
                KernelState::AwaitingToolGate,
                RuntimeEventName::ToolProposed | RuntimeEventName::ApprovalResolved
            )
            | (KernelState::AwaitingApproval, RuntimeEventName::ApprovalRequired)
            | (
                KernelState::ProjectingResult,
                RuntimeEventName::ToolDecisionRecorded
                    | RuntimeEventName::ToolResultObserved
                    | RuntimeEventName::ToolEffectReconciled
            )
            | (
                KernelState::Compacting | KernelState::Finalizing,
                RuntimeEventName::ProviderAttemptCompleted | RuntimeEventName::FinalizationStarted
            )
            | (KernelState::AwaitingDelivery, RuntimeEventName::DeliveryIntentRecorded)
            | (KernelState::SelectingRuntime, RuntimeEventName::CleanupCompleted)
            | (KernelState::Suspended, RuntimeEventName::BackpressureApplied)
    )
}

/// Observable effect posture of an active run at rollback evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ActiveRunEffectPosture {
    /// No mutating or externally visible effect has started.
    ReadOnly,
    /// A mutating effect is active or durably observed.
    Mutating,
    /// An external effect may have happened and requires reconciliation.
    OutcomeUnknown,
    /// The run already reached a terminal state.
    Terminal,
}

/// Whether suspension can be committed without cutting through an operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RollbackBoundary {
    Safe,
    Unsafe,
}

/// Authority-preserving action emitted by the rollback planner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RuntimeRollbackAction {
    /// Allow a read-only V2 generation to finish under its existing authority.
    FinishWithPersistedAuthority,
    /// Suspend immediately at the already-proven safe boundary.
    SuspendAtSafeBoundary,
    /// Continue only until the next safe boundary, then suspend.
    AwaitSafeBoundaryThenSuspend,
    /// The generation is terminal and needs no rollback action.
    NoActionTerminal,
    /// Legacy already owns the generation; a V2 rollback cannot alter it.
    NoActionLegacyAuthority,
}

/// Stable reason attached to an active-run rollback plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RuntimeRollbackReason {
    ReadOnlyFinishAllowed,
    PolicyRequiresSuspension,
    MutatingRunRequiresSuspension,
    UnknownEffectRequiresSuspension,
    RunAlreadyTerminal,
    LegacyAuthorityUnaffected,
}

impl RuntimeRollbackReason {
    /// Returns the stable metadata-trace reason code.
    #[must_use]
    pub(crate) const fn as_reason_code(self) -> &'static str {
        match self {
            Self::ReadOnlyFinishAllowed => "runtime.rollback.read_only_finish_allowed",
            Self::PolicyRequiresSuspension => "runtime.rollback.policy_requires_suspension",
            Self::MutatingRunRequiresSuspension => {
                "runtime.rollback.mutating_run_requires_suspension"
            }
            Self::UnknownEffectRequiresSuspension => {
                "runtime.rollback.unknown_effect_requires_suspension"
            }
            Self::RunAlreadyTerminal => "runtime.rollback.run_already_terminal",
            Self::LegacyAuthorityUnaffected => "runtime.rollback.legacy_authority_unaffected",
        }
    }
}

/// Pure rollback plan retaining the exact persisted authority decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct RuntimeRollbackPlanV1 {
    authority: RuntimeAuthorityDecisionV1,
    action: RuntimeRollbackAction,
    reason: RuntimeRollbackReason,
    reason_code: String,
}

impl RuntimeRollbackPlanV1 {
    /// Returns the unchanged persisted authority decision.
    #[must_use]
    #[cfg(test)]
    pub(crate) const fn authority(&self) -> &RuntimeAuthorityDecisionV1 {
        &self.authority
    }

    /// Returns the only permitted dispatcher action.
    #[must_use]
    pub(crate) const fn action(&self) -> RuntimeRollbackAction {
        self.action
    }

    /// Returns the stable metadata-trace reason code.
    #[must_use]
    pub(crate) fn reason_code(&self) -> &str {
        self.reason_code.as_str()
    }
}

/// Plans rollback without mutating authority, journal state, or effects.
///
/// # Errors
/// Returns [`RuntimeRollbackError`] when persisted authority is invalid or
/// selection had blocked without an owner.
pub(crate) fn plan_runtime_rollback(
    policy: RuntimeKernelRollbackPolicy,
    authority: &RuntimeAuthorityDecisionV1,
    effect_posture: ActiveRunEffectPosture,
    boundary: RollbackBoundary,
) -> Result<RuntimeRollbackPlanV1, RuntimeRollbackError> {
    authority.validate()?;
    let selected = authority.selected_runtime().ok_or(RuntimeRollbackError::AuthorityBlocked)?;
    let (action, reason) = match (selected, effect_posture, policy) {
        (RuntimeAuthority::Legacy, _, _) => (
            RuntimeRollbackAction::NoActionLegacyAuthority,
            RuntimeRollbackReason::LegacyAuthorityUnaffected,
        ),
        (RuntimeAuthority::V2, ActiveRunEffectPosture::Terminal, _) => {
            (RuntimeRollbackAction::NoActionTerminal, RuntimeRollbackReason::RunAlreadyTerminal)
        }
        (
            RuntimeAuthority::V2,
            ActiveRunEffectPosture::ReadOnly,
            RuntimeKernelRollbackPolicy::FinishReadOnlySuspendMutating,
        ) => (
            RuntimeRollbackAction::FinishWithPersistedAuthority,
            RuntimeRollbackReason::ReadOnlyFinishAllowed,
        ),
        (
            RuntimeAuthority::V2,
            ActiveRunEffectPosture::ReadOnly,
            RuntimeKernelRollbackPolicy::SuspendAllAtSafeBoundary,
        ) => suspension(boundary, RuntimeRollbackReason::PolicyRequiresSuspension),
        (RuntimeAuthority::V2, ActiveRunEffectPosture::Mutating, _) => {
            suspension(boundary, RuntimeRollbackReason::MutatingRunRequiresSuspension)
        }
        (RuntimeAuthority::V2, ActiveRunEffectPosture::OutcomeUnknown, _) => {
            suspension(boundary, RuntimeRollbackReason::UnknownEffectRequiresSuspension)
        }
    };
    Ok(RuntimeRollbackPlanV1 {
        authority: authority.clone(),
        action,
        reason,
        reason_code: reason.as_reason_code().to_owned(),
    })
}

const fn suspension(
    boundary: RollbackBoundary,
    reason: RuntimeRollbackReason,
) -> (RuntimeRollbackAction, RuntimeRollbackReason) {
    let action = match boundary {
        RollbackBoundary::Safe => RuntimeRollbackAction::SuspendAtSafeBoundary,
        RollbackBoundary::Unsafe => RuntimeRollbackAction::AwaitSafeBoundaryThenSuspend,
    };
    (action, reason)
}

/// Fail-closed rollback planning error.
#[derive(Debug, Error)]
pub(crate) enum RuntimeRollbackError {
    /// Persisted authority evidence is malformed.
    #[error(transparent)]
    InvalidAuthority(#[from] RuntimeAuthorityError),
    /// Selection blocked without granting an implementation authority.
    #[error("runtime rollback cannot plan a generation without authority")]
    AuthorityBlocked,
}

#[cfg(test)]
mod tests {
    use palyra_common::runtime_contracts::{
        RuntimeGeneration, RuntimeIdentitySetV1, RuntimeRunId, RuntimeSessionId, RuntimeTraceId,
    };

    use super::*;
    use crate::application::runtime_kernel_v2::{
        profile::{RuntimeKernelCompatibilityOverridesV1, RuntimeKernelProfileConfigV1},
        selection::{
            resolve_runtime_authority, RuntimeAuthorityProgressEvidence, V2RuntimeAvailability,
        },
        RuntimeKernelVersion,
    };

    fn authority(version: RuntimeKernelVersion) -> RuntimeAuthorityDecisionV1 {
        let config = RuntimeKernelProfileConfigV1::new(
            version,
            0,
            RuntimeKernelCompatibilityOverridesV1::none(),
        )
        .expect("test profile should validate");
        let identities = RuntimeIdentitySetV1::for_run(
            RuntimeTraceId::parse("trace_rollback").expect("test trace id is valid"),
            RuntimeSessionId::parse("session_rollback").expect("test session id is valid"),
            RuntimeRunId::parse("run_rollback").expect("test run id is valid"),
            RuntimeGeneration::new(3).expect("test generation is non-zero"),
        );
        resolve_runtime_authority(
            &config,
            &identities,
            V2RuntimeAvailability::Ready,
            RuntimeAuthorityProgressEvidence::pristine(),
            None,
        )
        .expect("test authority should resolve")
    }

    #[test]
    fn read_only_v2_run_finishes_without_authority_change() {
        let authority = authority(RuntimeKernelVersion::V2);
        let plan = plan_runtime_rollback(
            RuntimeKernelRollbackPolicy::FinishReadOnlySuspendMutating,
            &authority,
            ActiveRunEffectPosture::ReadOnly,
            RollbackBoundary::Unsafe,
        )
        .expect("read-only rollback should plan");

        assert_eq!(plan.action(), RuntimeRollbackAction::FinishWithPersistedAuthority);
        assert_eq!(plan.authority(), &authority);
        assert_eq!(plan.reason_code(), "runtime.rollback.read_only_finish_allowed");
    }

    #[test]
    fn mutating_and_unknown_runs_suspend_only_at_safe_boundary() {
        let authority = authority(RuntimeKernelVersion::V2);
        for effect in [ActiveRunEffectPosture::Mutating, ActiveRunEffectPosture::OutcomeUnknown] {
            let waiting = plan_runtime_rollback(
                RuntimeKernelRollbackPolicy::FinishReadOnlySuspendMutating,
                &authority,
                effect,
                RollbackBoundary::Unsafe,
            )
            .expect("unsafe rollback should wait");
            assert_eq!(waiting.action(), RuntimeRollbackAction::AwaitSafeBoundaryThenSuspend);
            assert_eq!(waiting.authority(), &authority);

            let safe = plan_runtime_rollback(
                RuntimeKernelRollbackPolicy::FinishReadOnlySuspendMutating,
                &authority,
                effect,
                RollbackBoundary::Safe,
            )
            .expect("safe rollback should suspend");
            assert_eq!(safe.action(), RuntimeRollbackAction::SuspendAtSafeBoundary);
            assert_eq!(safe.authority(), &authority);
        }
    }
}
