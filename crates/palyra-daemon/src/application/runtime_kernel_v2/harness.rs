//! Async, generation-aware harness contracts for RuntimeKernelV2.
//!
//! Harnesses receive only redacted attempt metadata and emit typed observations.
//! The host event sink retains ordering, journal, transition, and receipt authority.

use std::{future::Future, pin::Pin};

use palyra_common::runtime_contracts::{
    RuntimeApprovalSubjectId, RuntimeAttemptId, RuntimeDeliveryIntentId, RuntimeErrorEnvelopeV1,
    RuntimeEventId, RuntimeGeneration, RuntimeIdentitySetV1, RuntimeOperationId,
    RuntimeToolExecutionId, RuntimeToolProposalId,
};
#[cfg(test)]
use palyra_common::runtime_contracts::{
    RuntimeErrorClass, RuntimeErrorEnvelopeV1Input, RuntimeErrorPhase, RuntimeErrorSecurityClass,
    RuntimeErrorUserVisibility, RuntimeRetryability, RuntimeSubsystem,
};
use thiserror::Error;

use super::{
    context::RuntimeKernelContext, host_event_contract::HarnessTerminalReceipt,
    runtime_selection::HarnessKindV1, selection::RuntimeAuthority, KernelTransitionError,
    RuntimeKernelVersion,
};

/// Maximum non-terminal observations accepted for one harness attempt.
pub(crate) const MAX_HARNESS_EVENTS_PER_ATTEMPT: usize = 4_096;
/// Maximum model-text bytes represented by one metadata-only delta observation.
const MAX_TEXT_DELTA_BYTES: u32 = 1_048_576;

/// Boxed async result used by object-safe harness contracts.
pub(crate) type HarnessFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Redacted, generation-pinned input for one harness attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HarnessAttemptRequest {
    identities: RuntimeIdentitySetV1,
    attempt_id: RuntimeAttemptId,
    harness_id: String,
    selected_profile: RuntimeKernelVersion,
}

impl HarnessAttemptRequest {
    /// Creates an attempt request from an authoritative embedded V2 context.
    ///
    /// # Errors
    /// Returns [`HarnessContractError::UnsupportedRuntimeSelection`] for legacy,
    /// shadow, externally selected, or otherwise non-authoritative contexts.
    pub(crate) fn from_context(
        context: &RuntimeKernelContext,
        attempt_id: RuntimeAttemptId,
    ) -> Result<Self, HarnessContractError> {
        let selection = context.runtime_selection();
        let profile = selection.selected_profile();
        if !authorizes_embedded_v2(
            profile,
            context.authority().selected_profile(),
            context.authority().selected_authority(),
            selection.authority_decision().shadow_evaluation_enabled(),
        ) || selection.selected_harness_kind() != HarnessKindV1::Embedded
        {
            return Err(HarnessContractError::UnsupportedRuntimeSelection);
        }
        Ok(Self {
            identities: context.identities().clone(),
            attempt_id,
            harness_id: selection.selected_harness_id().to_owned(),
            selected_profile: profile,
        })
    }

    /// Constructs a request after the caller has supplied equivalent host proof.
    ///
    /// This narrow seam exists for the journal-backed host sink and deterministic
    /// conformance fixtures; it does not grant runtime authority by itself.
    #[cfg(test)]
    pub(in crate::application::runtime_kernel_v2) fn from_host_parts(
        identities: RuntimeIdentitySetV1,
        attempt_id: RuntimeAttemptId,
        harness_id: String,
        selected_profile: RuntimeKernelVersion,
    ) -> Result<Self, HarnessContractError> {
        identities.validate().map_err(|_| HarnessContractError::InvalidAttemptRequest)?;
        if identities.attempt_id.is_some()
            || identities.tool_proposal_id.is_some()
            || identities.tool_execution_id.is_some()
            || identities.approval_subject_id.is_some()
            || identities.delivery_intent_id.is_some()
            || identities.operation_id.is_some()
            || !matches!(
                selected_profile,
                RuntimeKernelVersion::V2 | RuntimeKernelVersion::V2Canary
            )
            || harness_id.trim().is_empty()
            || harness_id.len() > 128
        {
            return Err(HarnessContractError::InvalidAttemptRequest);
        }
        Ok(Self { identities, attempt_id, harness_id, selected_profile })
    }

    /// Returns the immutable run identities.
    #[must_use]
    pub(crate) const fn identities(&self) -> &RuntimeIdentitySetV1 {
        &self.identities
    }

    /// Returns the active Run generation.
    #[must_use]
    pub(crate) const fn generation(&self) -> RuntimeGeneration {
        self.identities.generation
    }

    /// Returns the host-reserved attempt identity.
    #[must_use]
    pub(crate) const fn attempt_id(&self) -> &RuntimeAttemptId {
        &self.attempt_id
    }

    /// Returns the registry-pinned embedded harness identity.
    #[must_use]
    pub(crate) fn harness_id(&self) -> &str {
        self.harness_id.as_str()
    }

    /// Returns the authoritative V2 rollout profile.
    #[must_use]
    pub(crate) const fn selected_profile(&self) -> RuntimeKernelVersion {
        self.selected_profile
    }
}

/// First callback accepted for an attempt before ordinary observations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HarnessAccepted {
    /// Generation presented by the harness.
    pub(crate) generation: RuntimeGeneration,
    /// Harness-local sequence; acceptance must be sequence one.
    pub(crate) sequence: u64,
}

/// Operator approval resolution reported through a host-owned approval callback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HarnessApprovalResolution {
    Approved,
    Denied,
    Expired,
}

/// Typed non-terminal observation produced by a harness attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HarnessEventKind {
    ProviderCallStarted,
    ProviderCallCompleted,
    TextDelta {
        utf8_bytes: u32,
    },
    #[cfg(test)]
    Progress {
        completed_units: u64,
        total_units: u64,
    },
    Usage {
        prompt_tokens: u64,
        completion_tokens: u64,
    },
    #[cfg(test)]
    Heartbeat {
        ordinal: u64,
    },
    ToolProposed {
        proposal_id: RuntimeToolProposalId,
    },
    ToolDenied {
        proposal_id: RuntimeToolProposalId,
        evidence_id: RuntimeOperationId,
        evidence_sha256: [u8; 32],
    },
    ApprovalRequired {
        proposal_id: RuntimeToolProposalId,
        approval_id: RuntimeApprovalSubjectId,
    },
    ApprovalResolved {
        proposal_id: RuntimeToolProposalId,
        approval_id: RuntimeApprovalSubjectId,
        resolution: HarnessApprovalResolution,
        evidence_id: Option<RuntimeOperationId>,
        evidence_sha256: Option<[u8; 32]>,
    },
    ToolExecutionStarted {
        proposal_id: RuntimeToolProposalId,
        execution_id: RuntimeToolExecutionId,
        operation_id: RuntimeOperationId,
    },
    ToolResultObserved {
        proposal_id: RuntimeToolProposalId,
        execution_id: RuntimeToolExecutionId,
        operation_id: RuntimeOperationId,
    },
    CompactionRequired,
    CompactionCompleted,
    ProviderRecoveryStarted {
        reason_code: String,
    },
    ProviderRecoveryCompleted {
        reason_code: String,
    },
    FinalizationReady,
    DeliveryIntentCommitted {
        delivery_intent_id: RuntimeDeliveryIntentId,
        operation_id: RuntimeOperationId,
        output_event_id: RuntimeEventId,
    },
    DeliverySkipped {
        evidence_id: RuntimeOperationId,
        evidence_sha256: [u8; 32],
    },
    FinalizationRecoveryPending {
        reason_code: String,
        stage: &'static str,
    },
    CancellationObserved,
    VerificationStarted,
    VerificationPassed,
    VerificationFailed {
        error: RuntimeErrorEnvelopeV1,
    },
}

/// One generation-aware, monotonically ordered harness observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HarnessEvent {
    pub(crate) generation: RuntimeGeneration,
    pub(crate) sequence: u64,
    pub(crate) kind: HarnessEventKind,
}

impl HarnessEvent {
    /// Validates bounded scalar metadata before host projection.
    ///
    /// # Errors
    /// Returns [`HarnessContractError::InvalidEvent`] when a count is empty,
    /// oversized, inconsistent, or arithmetically invalid.
    pub(crate) fn validate(&self) -> Result<(), HarnessContractError> {
        let valid = match &self.kind {
            HarnessEventKind::TextDelta { utf8_bytes } => {
                *utf8_bytes > 0 && *utf8_bytes <= MAX_TEXT_DELTA_BYTES
            }
            #[cfg(test)]
            HarnessEventKind::Progress { completed_units, total_units } => {
                *total_units > 0 && completed_units <= total_units
            }
            HarnessEventKind::Usage { prompt_tokens, completion_tokens } => {
                prompt_tokens.checked_add(*completion_tokens).is_some()
            }
            #[cfg(test)]
            HarnessEventKind::Heartbeat { ordinal } => *ordinal > 0,
            HarnessEventKind::FinalizationRecoveryPending { reason_code, stage } => {
                is_reason_code(reason_code) && is_reason_code(stage)
            }
            HarnessEventKind::ProviderRecoveryStarted { reason_code }
            | HarnessEventKind::ProviderRecoveryCompleted { reason_code } => {
                is_reason_code(reason_code)
            }
            HarnessEventKind::ApprovalResolved {
                resolution, evidence_id, evidence_sha256, ..
            } => {
                let evidence_present = evidence_id.is_some() && evidence_sha256.is_some();
                let evidence_absent = evidence_id.is_none() && evidence_sha256.is_none();
                match resolution {
                    HarnessApprovalResolution::Approved => evidence_absent,
                    HarnessApprovalResolution::Denied | HarnessApprovalResolution::Expired => {
                        evidence_present
                    }
                }
            }
            HarnessEventKind::ProviderCallStarted
            | HarnessEventKind::ProviderCallCompleted
            | HarnessEventKind::ToolProposed { .. }
            | HarnessEventKind::ToolDenied { .. }
            | HarnessEventKind::ApprovalRequired { .. }
            | HarnessEventKind::ToolExecutionStarted { .. }
            | HarnessEventKind::ToolResultObserved { .. }
            | HarnessEventKind::CompactionRequired
            | HarnessEventKind::CompactionCompleted
            | HarnessEventKind::FinalizationReady
            | HarnessEventKind::DeliveryIntentCommitted { .. }
            | HarnessEventKind::DeliverySkipped { .. }
            | HarnessEventKind::CancellationObserved
            | HarnessEventKind::VerificationStarted
            | HarnessEventKind::VerificationPassed
            | HarnessEventKind::VerificationFailed { .. } => true,
        };
        if self.sequence == 0 || !valid {
            return Err(HarnessContractError::InvalidEvent);
        }
        Ok(())
    }
}

/// Exactly one outcome returned by an embedded attempt driver.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HarnessTerminalOutcome {
    Completed,
    Failed { error: RuntimeErrorEnvelopeV1 },
    Cancelled { reason_code: String },
}

/// Generation-aware terminal callback for one harness attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HarnessAttemptTerminal {
    pub(crate) generation: RuntimeGeneration,
    pub(crate) sequence: u64,
    pub(crate) outcome: HarnessTerminalOutcome,
}

impl HarnessAttemptTerminal {
    /// Validates terminal ordering metadata and stable cancellation evidence.
    ///
    /// # Errors
    /// Returns [`HarnessContractError::InvalidTerminal`] for an empty sequence
    /// or unsafe cancellation reason code.
    pub(crate) fn validate(&self) -> Result<(), HarnessContractError> {
        if self.sequence == 0 {
            return Err(HarnessContractError::InvalidTerminal);
        }
        if let HarnessTerminalOutcome::Cancelled { reason_code } = &self.outcome {
            if !is_reason_code(reason_code) {
                return Err(HarnessContractError::InvalidTerminal);
            }
        }
        Ok(())
    }
}

/// Provider-owned typed failure before it becomes a kernel failure event.
#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HarnessProviderFailure {
    pub(crate) reason_code: String,
    pub(crate) retryability: RuntimeRetryability,
    pub(crate) output_emitted: bool,
    pub(crate) safe_message: String,
    pub(crate) recovery_hint: String,
}

#[cfg(test)]
impl HarnessProviderFailure {
    /// Maps structured provider evidence into the strict kernel error taxonomy.
    ///
    /// # Errors
    /// Returns [`HarnessContractError::InvalidProviderFailure`] when the
    /// provider evidence contradicts the shared retry or redaction contract.
    pub(crate) fn into_error_envelope(
        self,
    ) -> Result<RuntimeErrorEnvelopeV1, HarnessContractError> {
        let class = if matches!(
            self.retryability,
            RuntimeRetryability::SafeSameRequest
                | RuntimeRetryability::SafeAfterBackoff
                | RuntimeRetryability::RequiresCredentialRefresh
                | RuntimeRetryability::RequiresRequestTransform
                | RuntimeRetryability::RequiresContextCompaction
                | RuntimeRetryability::RequiresProviderFailover
        ) {
            RuntimeErrorClass::ProviderRetryable
        } else {
            RuntimeErrorClass::ProviderTerminal
        };
        RuntimeErrorEnvelopeV1::try_new(RuntimeErrorEnvelopeV1Input {
            class,
            reason_code: self.reason_code,
            subsystem: RuntimeSubsystem::Provider,
            phase: RuntimeErrorPhase::ProviderCall,
            retryability: self.retryability,
            security_class: RuntimeErrorSecurityClass::Internal,
            user_visibility: RuntimeErrorUserVisibility::SafeMessage,
            output_emitted: self.output_emitted,
            side_effect_may_have_occurred: false,
            safe_message: self.safe_message,
            recovery_hint: self.recovery_hint,
        })
        .map_err(|_| HarnessContractError::InvalidProviderFailure)
    }
}

/// Host callback surface with accepted, event, and exactly-one terminal semantics.
pub(crate) trait HarnessEventSink: Send {
    fn accepted<'a>(
        &'a mut self,
        accepted: HarnessAccepted,
    ) -> HarnessFuture<'a, Result<(), HarnessContractError>>;

    fn event<'a>(
        &'a mut self,
        event: HarnessEvent,
    ) -> HarnessFuture<'a, Result<(), HarnessContractError>>;

    fn terminal<'a>(
        &'a mut self,
        terminal: HarnessAttemptTerminal,
    ) -> HarnessFuture<'a, Result<HarnessTerminalReceipt, HarnessContractError>>;
}

/// Provisional async runtime contract shared by embedded and future external harnesses.
pub(crate) trait HarnessRuntimeV2: Send + Sync {
    fn run_attempt<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
        sink: &'a mut dyn HarnessEventSink,
    ) -> HarnessFuture<'a, Result<HarnessTerminalReceipt, HarnessContractError>>;
}

/// Fail-closed harness contract error.
#[derive(Debug, Error)]
pub(crate) enum HarnessContractError {
    #[error("harness attempt request is invalid")]
    InvalidAttemptRequest,
    #[error("runtime selection does not authorize the embedded V2 harness")]
    UnsupportedRuntimeSelection,
    #[error("harness event arrived before acceptance")]
    EventBeforeAccepted,
    #[error("harness acceptance was emitted more than once")]
    DuplicateAccepted,
    #[error("harness terminal outcome was emitted more than once")]
    DuplicateTerminal,
    #[error("harness event arrived after terminalization")]
    EventAfterTerminal,
    #[error("harness event generation is stale")]
    StaleGeneration { active: RuntimeGeneration, observed: RuntimeGeneration },
    #[error("harness event sequence is not strictly monotonic")]
    NonMonotonicSequence { last: u64, observed: u64 },
    #[error("harness event limit is exhausted")]
    EventLimitExceeded,
    #[error("harness event metadata is invalid")]
    InvalidEvent,
    #[error("harness terminal metadata is invalid")]
    InvalidTerminal,
    #[error("harness verification observation is out of order or duplicated")]
    InvalidVerificationTransition,
    #[error("provider failure metadata is invalid")]
    #[cfg(test)]
    InvalidProviderFailure,
    #[error("runtime kernel transition failed")]
    KernelTransition(#[source] KernelTransitionError),
    #[error("runtime kernel journal boundary failed")]
    Journal(#[source] super::journal_adapter::KernelJournalAdapterError),
    #[error("runtime kernel generation was suspended by rollback policy")]
    RollbackSuspended,
    #[error("host event metadata authority is unavailable")]
    HostEventMetadata,
}

impl HarnessContractError {
    /// Returns whether rollback CAS evidence requires the caller to stop and reload.
    #[must_use]
    pub(crate) const fn is_rollback_boundary_stale(&self) -> bool {
        matches!(
            self,
            Self::Journal(
                super::journal_adapter::KernelJournalAdapterError::RollbackStaleDenied { .. }
            )
        )
    }
}

impl From<KernelTransitionError> for HarnessContractError {
    fn from(source: KernelTransitionError) -> Self {
        Self::KernelTransition(source)
    }
}

fn is_reason_code(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
}

fn authorizes_embedded_v2(
    selected_profile: RuntimeKernelVersion,
    grant_profile: RuntimeKernelVersion,
    authority: Option<RuntimeAuthority>,
    shadow_evaluation_enabled: bool,
) -> bool {
    matches!(selected_profile, RuntimeKernelVersion::V2 | RuntimeKernelVersion::V2Canary)
        && selected_profile == grant_profile
        && matches!(authority, Some(RuntimeAuthority::V2))
        && !shadow_evaluation_enabled
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authoritative_profiles_accept_v2_and_selected_canary_only() {
        assert!(authorizes_embedded_v2(
            RuntimeKernelVersion::V2,
            RuntimeKernelVersion::V2,
            Some(RuntimeAuthority::V2),
            false,
        ));
        assert!(authorizes_embedded_v2(
            RuntimeKernelVersion::V2Canary,
            RuntimeKernelVersion::V2Canary,
            Some(RuntimeAuthority::V2),
            false,
        ));
        assert!(!authorizes_embedded_v2(
            RuntimeKernelVersion::V2Shadow,
            RuntimeKernelVersion::V2Shadow,
            Some(RuntimeAuthority::Legacy),
            true,
        ));
        assert!(!authorizes_embedded_v2(
            RuntimeKernelVersion::Legacy,
            RuntimeKernelVersion::Legacy,
            Some(RuntimeAuthority::Legacy),
            false,
        ));
    }
}
