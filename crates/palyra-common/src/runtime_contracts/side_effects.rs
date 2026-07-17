//! Durable side-effect fence and reconciliation contracts.
//!
//! The contract distinguishes intent, started effects, observed receipts, and
//! uncertain effects so retry policy never treats a missing acknowledgement as no effect.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::{RuntimeGeneration, RuntimeOperationId, RuntimeToolExecutionId};

/// Schema version for [`SideEffectFenceV1`].
pub const SIDE_EFFECT_FENCE_SCHEMA_VERSION: u32 = 1;

runtime_contract_enum! {
    /// Durable lifecycle of a mutating operation.
    pub enum SideEffectFenceState {
        IntentRecorded => "intent_recorded",
        EffectStarted => "effect_started",
        EffectObserved => "effect_observed",
        EffectUnknown => "effect_unknown",
        Reconciled => "reconciled",
        Abandoned => "abandoned"
    }
}

impl SideEffectFenceState {
    /// Returns whether this state admits no automatic execution transition.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::EffectObserved | Self::Reconciled | Self::Abandoned)
    }

    /// Returns whether retry requires new reconciliation or operator evidence.
    #[must_use]
    pub const fn blocks_automatic_retry(self) -> bool {
        matches!(self, Self::EffectStarted | Self::EffectUnknown)
    }

    /// Validates a state-machine transition.
    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::IntentRecorded, Self::EffectStarted)
                | (Self::IntentRecorded, Self::Abandoned)
                | (Self::EffectStarted, Self::EffectObserved)
                | (Self::EffectStarted, Self::EffectUnknown)
                | (Self::EffectUnknown, Self::Reconciled)
                | (Self::EffectUnknown, Self::Abandoned)
        )
    }
}

runtime_contract_enum! {
    /// Idempotency guarantee declared by the host tool registry.
    pub enum RuntimeIdempotencyClass {
        ReadOnly => "read_only",
        DeterministicIdempotent => "deterministic_idempotent",
        ExternalIdempotencyKey => "external_idempotency_key",
        ReconciliableMutation => "reconciliable_mutation",
        NonIdempotent => "non_idempotent"
    }
}

runtime_contract_enum! {
    /// Restart behavior selected before a tool execution begins.
    pub enum SideEffectRestartPolicy {
        SafeRetry => "safe_retry",
        ReconcileBeforeRetry => "reconcile_before_retry",
        RequireConfirmation => "require_confirmation",
        NeverRetry => "never_retry"
    }
}

runtime_contract_enum! {
    /// Evidence strategy used to explain an uncertain effect.
    pub enum ReconciliationStrategy {
        None => "none",
        WorkspaceDigest => "workspace_digest",
        ProcessProvenance => "process_provenance",
        ExternalIdempotencyReceipt => "external_idempotency_receipt",
        DeliveryAcknowledgement => "delivery_acknowledgement",
        WorkerLeaseReceipt => "worker_lease_receipt"
    }
}

runtime_contract_enum! {
    /// Decision produced before re-executing a fenced operation.
    pub enum SideEffectRetryDecision {
        Safe => "safe",
        ReconciliationRequired => "reconciliation_required",
        ConfirmationRequired => "confirmation_required",
        Blocked => "blocked",
        Completed => "completed"
    }
}

/// Static semantics declared for one executable tool contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolExecutionSemantics {
    /// Contract schema version.
    pub schema_version: u32,
    /// Tool contract name.
    pub tool_name: String,
    /// Idempotency guarantee.
    pub idempotency_class: RuntimeIdempotencyClass,
    /// Restart posture.
    pub restart_policy: SideEffectRestartPolicy,
    /// Reconciliation strategy.
    pub reconciliation_strategy: ReconciliationStrategy,
    /// Whether an external idempotency key must be bound before dispatch.
    pub external_idempotency_key_required: bool,
}

impl ToolExecutionSemantics {
    /// Validates cross-field restart semantics.
    ///
    /// # Errors
    /// Returns [`SideEffectFenceError::InvalidSemantics`] when the declaration
    /// permits an unsafe retry or omits a required reconciliation strategy.
    pub fn validate(&self) -> Result<(), SideEffectFenceError> {
        if self.schema_version != SIDE_EFFECT_FENCE_SCHEMA_VERSION
            || self.tool_name.trim().is_empty()
        {
            return Err(SideEffectFenceError::InvalidSemantics);
        }
        match self.idempotency_class {
            RuntimeIdempotencyClass::ReadOnly
            | RuntimeIdempotencyClass::DeterministicIdempotent => {
                if self.restart_policy != SideEffectRestartPolicy::SafeRetry
                    || self.reconciliation_strategy != ReconciliationStrategy::None
                    || self.external_idempotency_key_required
                {
                    return Err(SideEffectFenceError::InvalidSemantics);
                }
            }
            RuntimeIdempotencyClass::ExternalIdempotencyKey => {
                if self.restart_policy != SideEffectRestartPolicy::ReconcileBeforeRetry
                    || !self.external_idempotency_key_required
                    || self.reconciliation_strategy
                        != ReconciliationStrategy::ExternalIdempotencyReceipt
                {
                    return Err(SideEffectFenceError::InvalidSemantics);
                }
            }
            RuntimeIdempotencyClass::ReconciliableMutation => {
                if self.restart_policy != SideEffectRestartPolicy::ReconcileBeforeRetry
                    || self.reconciliation_strategy == ReconciliationStrategy::None
                    || self.external_idempotency_key_required
                {
                    return Err(SideEffectFenceError::InvalidSemantics);
                }
            }
            RuntimeIdempotencyClass::NonIdempotent => {
                if !matches!(
                    self.restart_policy,
                    SideEffectRestartPolicy::RequireConfirmation
                        | SideEffectRestartPolicy::NeverRetry
                ) || self.reconciliation_strategy != ReconciliationStrategy::None
                    || self.external_idempotency_key_required
                {
                    return Err(SideEffectFenceError::InvalidSemantics);
                }
            }
        }
        Ok(())
    }
}

/// Durable current state of one mutating operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SideEffectFenceV1 {
    /// Contract schema version.
    pub schema_version: u32,
    /// Stable operation identity across provider attempts and generations.
    pub operation_id: RuntimeOperationId,
    /// Stable tool execution identity.
    pub tool_execution_id: RuntimeToolExecutionId,
    /// Generation that recorded the intent.
    pub intent_generation: RuntimeGeneration,
    /// Most recent generation allowed to reconcile the operation.
    pub observed_generation: RuntimeGeneration,
    /// Canonical normalized intent digest.
    pub intent_sha256: String,
    /// Current fence state.
    pub state: SideEffectFenceState,
    /// Tool contract semantics.
    pub semantics: ToolExecutionSemantics,
    /// External idempotency key hash, never the raw key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_idempotency_key_sha256: Option<String>,
    /// Receipt or reconciliation evidence digest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_sha256: Option<String>,
    /// Stable reason code for the current state.
    pub reason_code: String,
    /// Last transition timestamp.
    pub updated_at_unix_ms: i64,
}

impl SideEffectFenceV1 {
    /// Validates the current fence state and its evidence requirements.
    ///
    /// # Errors
    /// Returns [`SideEffectFenceError`] for malformed digests, semantics, or state evidence.
    pub fn validate(&self) -> Result<(), SideEffectFenceError> {
        if self.schema_version != SIDE_EFFECT_FENCE_SCHEMA_VERSION
            || !is_sha256(self.intent_sha256.as_str())
            || self
                .external_idempotency_key_sha256
                .as_deref()
                .is_some_and(|value| !is_sha256(value))
            || self.evidence_sha256.as_deref().is_some_and(|value| !is_sha256(value))
            || self.reason_code.trim().is_empty()
            || self.updated_at_unix_ms < 0
        {
            return Err(SideEffectFenceError::InvalidRecord);
        }
        self.semantics.validate()?;
        if self.semantics.external_idempotency_key_required
            != self.external_idempotency_key_sha256.is_some()
        {
            return Err(SideEffectFenceError::InvalidRecord);
        }
        if matches!(
            self.state,
            SideEffectFenceState::EffectObserved | SideEffectFenceState::Reconciled
        ) && self.evidence_sha256.is_none()
        {
            return Err(SideEffectFenceError::MissingEvidence);
        }
        Ok(())
    }

    /// Transitions the fence while preserving stable operation identity.
    ///
    /// # Errors
    /// Returns [`SideEffectFenceError::InvalidTransition`] for an illegal transition.
    pub fn transition(
        &mut self,
        next: SideEffectFenceState,
        observed_generation: RuntimeGeneration,
        reason_code: impl Into<String>,
        evidence_sha256: Option<String>,
        updated_at_unix_ms: i64,
    ) -> Result<(), SideEffectFenceError> {
        if !self.state.can_transition_to(next) {
            return Err(SideEffectFenceError::InvalidTransition { from: self.state, to: next });
        }
        let mut candidate = self.clone();
        candidate.state = next;
        candidate.observed_generation = observed_generation;
        candidate.reason_code = reason_code.into();
        candidate.evidence_sha256 = evidence_sha256;
        candidate.updated_at_unix_ms = updated_at_unix_ms;
        candidate.validate()?;
        *self = candidate;
        Ok(())
    }

    /// Returns the retry decision for the current durable evidence.
    #[must_use]
    pub const fn retry_decision(&self) -> SideEffectRetryDecision {
        match self.state {
            SideEffectFenceState::EffectObserved | SideEffectFenceState::Reconciled => {
                SideEffectRetryDecision::Completed
            }
            SideEffectFenceState::EffectUnknown => match self.semantics.restart_policy {
                SideEffectRestartPolicy::ReconcileBeforeRetry => {
                    SideEffectRetryDecision::ReconciliationRequired
                }
                SideEffectRestartPolicy::RequireConfirmation => {
                    SideEffectRetryDecision::ConfirmationRequired
                }
                SideEffectRestartPolicy::SafeRetry | SideEffectRestartPolicy::NeverRetry => {
                    SideEffectRetryDecision::Blocked
                }
            },
            SideEffectFenceState::EffectStarted => SideEffectRetryDecision::Blocked,
            // A durable intent without `effect_started` is proof that dispatch
            // never crossed the side-effect boundary, regardless of the tool's
            // post-start retry policy.
            SideEffectFenceState::IntentRecorded => SideEffectRetryDecision::Safe,
            SideEffectFenceState::Abandoned => SideEffectRetryDecision::Blocked,
        }
    }
}

/// Side-effect fence validation error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SideEffectFenceError {
    /// Tool execution semantics are inconsistent.
    #[error("tool execution semantics are inconsistent")]
    InvalidSemantics,
    /// Fence fields are malformed.
    #[error("side-effect fence record is invalid")]
    InvalidRecord,
    /// Observed/reconciled state lacks a receipt digest.
    #[error("side-effect fence state requires evidence")]
    MissingEvidence,
    /// State-machine transition is not allowed.
    #[error("side-effect fence cannot transition from {from} to {to}")]
    InvalidTransition {
        /// Current state.
        from: SideEffectFenceState,
        /// Requested state.
        to: SideEffectFenceState,
    },
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn semantics() -> ToolExecutionSemantics {
        ToolExecutionSemantics {
            schema_version: 1,
            tool_name: "palyra.fs.apply_patch".to_owned(),
            idempotency_class: RuntimeIdempotencyClass::ReconciliableMutation,
            restart_policy: SideEffectRestartPolicy::ReconcileBeforeRetry,
            reconciliation_strategy: ReconciliationStrategy::WorkspaceDigest,
            external_idempotency_key_required: false,
        }
    }

    fn fence() -> SideEffectFenceV1 {
        SideEffectFenceV1 {
            schema_version: 1,
            operation_id: RuntimeOperationId::parse("operation_01").expect("operation id"),
            tool_execution_id: RuntimeToolExecutionId::parse("execution_01").expect("execution id"),
            intent_generation: RuntimeGeneration::new(1).expect("generation"),
            observed_generation: RuntimeGeneration::new(1).expect("generation"),
            intent_sha256: "a".repeat(64),
            state: SideEffectFenceState::IntentRecorded,
            semantics: semantics(),
            external_idempotency_key_sha256: None,
            evidence_sha256: None,
            reason_code: "tool.effect.intent_recorded".to_owned(),
            updated_at_unix_ms: 1,
        }
    }

    #[test]
    fn recorded_intent_can_retry_before_effect_start() {
        assert_eq!(fence().retry_decision(), SideEffectRetryDecision::Safe);
    }

    #[test]
    fn unknown_effect_blocks_blind_retry() {
        let mut fence = fence();
        fence
            .transition(
                SideEffectFenceState::EffectStarted,
                RuntimeGeneration::new(1).expect("generation"),
                "tool.effect.started",
                None,
                2,
            )
            .expect("effect should start");
        fence
            .transition(
                SideEffectFenceState::EffectUnknown,
                RuntimeGeneration::new(2).expect("generation"),
                "tool.effect.ack_unknown",
                None,
                3,
            )
            .expect("effect should become unknown");
        assert_eq!(fence.retry_decision(), SideEffectRetryDecision::ReconciliationRequired);
    }

    #[test]
    fn observed_effect_requires_receipt_without_mutating_on_failure() {
        let mut fence = fence();
        fence
            .transition(
                SideEffectFenceState::EffectStarted,
                RuntimeGeneration::new(1).expect("generation"),
                "tool.effect.started",
                None,
                2,
            )
            .expect("effect should start");
        let before = fence.clone();
        assert_eq!(
            fence.transition(
                SideEffectFenceState::EffectObserved,
                RuntimeGeneration::new(1).expect("generation"),
                "tool.effect.observed",
                None,
                3,
            ),
            Err(SideEffectFenceError::MissingEvidence)
        );
        assert_eq!(fence, before);
    }

    #[test]
    fn external_key_semantics_require_matching_key_hash() {
        let mut fence = fence();
        fence.semantics.idempotency_class = RuntimeIdempotencyClass::ExternalIdempotencyKey;
        fence.semantics.restart_policy = SideEffectRestartPolicy::ReconcileBeforeRetry;
        fence.semantics.reconciliation_strategy =
            ReconciliationStrategy::ExternalIdempotencyReceipt;
        fence.semantics.external_idempotency_key_required = true;
        assert_eq!(fence.validate(), Err(SideEffectFenceError::InvalidRecord));
        fence.external_idempotency_key_sha256 = Some("b".repeat(64));
        fence.validate().expect("keyed fence should validate");
    }

    #[test]
    fn safe_retry_semantics_reject_reconciliation_claims() {
        let mut semantics = semantics();
        semantics.idempotency_class = RuntimeIdempotencyClass::ReadOnly;
        semantics.restart_policy = SideEffectRestartPolicy::SafeRetry;
        assert_eq!(semantics.validate(), Err(SideEffectFenceError::InvalidSemantics));
    }
}
