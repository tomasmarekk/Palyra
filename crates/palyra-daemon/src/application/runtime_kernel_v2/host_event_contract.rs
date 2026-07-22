//! Host metadata and receipt contracts shared by the harness event sink.

use std::time::{SystemTime, UNIX_EPOCH};

use palyra_common::runtime_contracts::{
    RuntimeDeliveryIntentId, RuntimeEventId, RuntimeGeneration, RuntimeGenerationLane,
    RuntimeOperationId,
};

use super::{
    harness::{HarnessApprovalResolution, HarnessContractError},
    KernelTerminalOutcome, RuntimeKernelVersion,
};
use ulid::Ulid;

/// Host-issued event identity and observation time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HarnessHostEventStamp {
    pub(crate) event_id: RuntimeEventId,
    pub(crate) occurred_at_unix_ms: i64,
}

/// Authority that issues non-ordering event metadata before journal commit.
///
/// Canonical lane ordering deliberately does not cross this boundary. The
/// journal allocates it inside the same immediate transaction as the append.
pub(crate) trait HarnessHostEventAuthority: Send {
    fn issue(
        &mut self,
        lane: RuntimeGenerationLane,
        generation: RuntimeGeneration,
    ) -> Result<HarnessHostEventStamp, HarnessContractError>;
}

pub(super) struct SystemHarnessEventAuthority;

impl HarnessHostEventAuthority for SystemHarnessEventAuthority {
    fn issue(
        &mut self,
        _lane: RuntimeGenerationLane,
        _generation: RuntimeGeneration,
    ) -> Result<HarnessHostEventStamp, HarnessContractError> {
        let occurred_at_unix_ms = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| HarnessContractError::HostEventMetadata)?
                .as_millis(),
        )
        .map_err(|_| HarnessContractError::HostEventMetadata)?;
        Ok(HarnessHostEventStamp {
            event_id: RuntimeEventId::parse(format!("event_{}", Ulid::new()).as_str())
                .map_err(|_| HarnessContractError::HostEventMetadata)?,
            occurred_at_unix_ms,
        })
    }
}

/// Host-reserved delivery identities consumed only after successful finalization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HarnessDeliveryBinding {
    pub(crate) delivery_intent_id: RuntimeDeliveryIntentId,
    pub(crate) operation_id: RuntimeOperationId,
    pub(crate) output_event_id: RuntimeEventId,
}

/// Host-issued acknowledgement proving that the sole terminal outcome is durable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HarnessTerminalReceipt {
    pub(super) outcome: KernelTerminalOutcome,
    pub(super) kernel_revision: u64,
    pub(super) harness_terminal_sequence: u64,
    pub(super) observations_accepted: usize,
    pub(super) prompt_tokens: u64,
    pub(super) completion_tokens: u64,
    pub(super) verification_passed: bool,
}

impl HarnessTerminalReceipt {
    #[must_use]
    pub(crate) const fn outcome(&self) -> KernelTerminalOutcome {
        self.outcome
    }

    #[must_use]
    #[cfg(test)]
    pub(crate) const fn kernel_revision(&self) -> u64 {
        self.kernel_revision
    }

    #[must_use]
    #[cfg(test)]
    pub(crate) const fn harness_terminal_sequence(&self) -> u64 {
        self.harness_terminal_sequence
    }

    #[must_use]
    #[cfg(test)]
    pub(crate) const fn observations_accepted(&self) -> usize {
        self.observations_accepted
    }

    #[must_use]
    pub(crate) const fn prompt_tokens(&self) -> u64 {
        self.prompt_tokens
    }

    #[must_use]
    pub(crate) const fn completion_tokens(&self) -> u64 {
        self.completion_tokens
    }

    #[must_use]
    #[cfg(test)]
    pub(crate) const fn verification_passed(&self) -> bool {
        self.verification_passed
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum VerificationState {
    NotStarted,
    InProgress,
    Passed,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DeliveryObservationState {
    Pending,
    Committed,
    Skipped,
    RecoveryPending,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct DeliverySkipEvidence {
    pub(super) evidence_id: RuntimeOperationId,
    pub(super) evidence_sha256: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct FinalizationRecoveryEvidence {
    pub(super) reason_code: String,
    pub(super) stage: &'static str,
}

impl DeliverySkipEvidence {
    pub(super) fn sha256_hex(&self) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut encoded = String::with_capacity(64);
        for byte in self.evidence_sha256 {
            encoded.push(char::from(HEX[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
        encoded
    }
}

pub(super) const fn approval_resolution_str(resolution: HarnessApprovalResolution) -> &'static str {
    match resolution {
        HarnessApprovalResolution::Approved => "approved",
        HarnessApprovalResolution::Denied => "denied",
        HarnessApprovalResolution::Expired => "expired",
    }
}

pub(super) const fn runtime_profile_str(profile: RuntimeKernelVersion) -> &'static str {
    match profile {
        RuntimeKernelVersion::Legacy => "legacy",
        RuntimeKernelVersion::V2Shadow => "v2_shadow",
        RuntimeKernelVersion::V2Canary => "v2_canary",
        RuntimeKernelVersion::V2 => "v2",
    }
}
