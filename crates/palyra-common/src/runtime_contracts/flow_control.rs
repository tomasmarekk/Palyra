//! Shared cancellation, deadline, settlement, and backpressure contracts.
//!
//! Runtime owners derive child scopes from a run root. Overflow policies preserve
//! terminal and approval events while allowing bounded progress coalescing.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::{RuntimeGeneration, RuntimeOperationId};

/// Schema version for flow-control contracts.
pub const RUNTIME_FLOW_CONTROL_SCHEMA_VERSION: u32 = 1;
/// Maximum supported bounded channel capacity.
pub const MAX_RUNTIME_CHANNEL_CAPACITY: usize = 65_536;

runtime_contract_enum! {
    /// Cancellable operation families.
    pub enum CancellationScopeKind {
        Run => "run",
        ProviderAttempt => "provider_attempt",
        ToolExecution => "tool_execution",
        ApprovalWait => "approval_wait",
        ChildTask => "child_task",
        Process => "process",
        Delivery => "delivery"
    }
}

runtime_contract_enum! {
    /// Host-classified reason cancellation was requested.
    pub enum CancellationReason {
        UserCancel => "user_cancel",
        SteerSupersede => "steer_supersede",
        InterruptSupersede => "interrupt_supersede",
        DaemonDrain => "daemon_drain",
        DeadlineExceeded => "deadline_exceeded",
        ResourceEviction => "resource_eviction",
        GenerationSuperseded => "generation_superseded",
        ParentCancelled => "parent_cancelled"
    }
}

runtime_contract_enum! {
    /// Action taken when a bounded channel reaches capacity.
    pub enum BackpressureOverflowAction {
        BlockProducer => "block_producer",
        CoalesceProgress => "coalesce_progress",
        SpillMetadataArtifact => "spill_metadata_artifact",
        RejectProducer => "reject_producer",
        CancelChild => "cancel_child"
    }
}

runtime_contract_enum! {
    /// Result of graceful cancellation settlement.
    pub enum CancellationSettlementOutcome {
        Graceful => "graceful",
        HardAborted => "hard_aborted",
        CleanupUnknown => "cleanup_unknown"
    }
}

/// Cancellation context inherited by one runtime operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CancellationContextV1 {
    /// Contract schema version.
    pub schema_version: u32,
    /// Stable operation identity.
    pub scope_id: RuntimeOperationId,
    /// Scope family.
    pub scope: CancellationScopeKind,
    /// Current generation.
    pub generation: RuntimeGeneration,
    /// Parent operation identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_scope_id: Option<RuntimeOperationId>,
    /// Cancellation reason, absent until cancellation is requested.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<CancellationReason>,
    /// Absolute deadline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_unix_ms: Option<i64>,
    /// Maximum graceful settlement interval.
    pub graceful_settle_ms: u64,
    /// Maximum hard-abort interval after graceful settlement.
    pub hard_abort_after_ms: u64,
}

impl CancellationContextV1 {
    /// Validates timing and hierarchy fields.
    ///
    /// # Errors
    /// Returns [`RuntimeFlowControlError::InvalidCancellationContext`] for malformed budgets.
    pub fn validate(&self) -> Result<(), RuntimeFlowControlError> {
        if self.schema_version != RUNTIME_FLOW_CONTROL_SCHEMA_VERSION
            || self.hard_abort_after_ms < self.graceful_settle_ms
            || self.deadline_unix_ms.is_some_and(|deadline| deadline < 0)
            || self.parent_scope_id.as_ref() == Some(&self.scope_id)
        {
            return Err(RuntimeFlowControlError::InvalidCancellationContext);
        }
        Ok(())
    }

    /// Derives a child context without widening the parent deadline or abort budget.
    ///
    /// # Errors
    /// Returns [`RuntimeFlowControlError::DeadlineWidened`] if the child deadline exceeds
    /// its parent, or an invalid-context error for malformed budgets.
    pub fn derive_child(
        &self,
        scope_id: RuntimeOperationId,
        scope: CancellationScopeKind,
        deadline_unix_ms: Option<i64>,
        graceful_settle_ms: u64,
        hard_abort_after_ms: u64,
    ) -> Result<Self, RuntimeFlowControlError> {
        if let (Some(parent), Some(child)) = (self.deadline_unix_ms, deadline_unix_ms) {
            if child > parent {
                return Err(RuntimeFlowControlError::DeadlineWidened);
            }
        }
        let child = Self {
            schema_version: RUNTIME_FLOW_CONTROL_SCHEMA_VERSION,
            scope_id,
            scope,
            generation: self.generation,
            parent_scope_id: Some(self.scope_id.clone()),
            reason: self.reason.map(|_| CancellationReason::ParentCancelled),
            deadline_unix_ms: deadline_unix_ms.or(self.deadline_unix_ms),
            graceful_settle_ms: graceful_settle_ms.min(self.graceful_settle_ms),
            hard_abort_after_ms: hard_abort_after_ms.min(self.hard_abort_after_ms),
        };
        child.validate()?;
        Ok(child)
    }

    /// Returns whether work may start at `now_unix_ms`.
    #[must_use]
    pub fn permits_new_work(&self, now_unix_ms: i64) -> bool {
        self.reason.is_none() && self.deadline_unix_ms.is_none_or(|deadline| now_unix_ms < deadline)
    }
}

/// Bounded channel behavior for one runtime event class.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackpressurePolicy {
    /// Contract schema version.
    pub schema_version: u32,
    /// Bounded queue capacity.
    pub capacity: usize,
    /// Overflow behavior for non-protected events.
    pub overflow_action: BackpressureOverflowAction,
    /// Whether terminal events have a reserved durable path.
    pub preserve_terminal: bool,
    /// Whether approval events have a reserved durable path.
    pub preserve_approval: bool,
    /// Maximum byte length retained in overflow summaries.
    pub max_summary_bytes: usize,
}

impl BackpressurePolicy {
    /// Validates bounded capacity and protected event posture.
    ///
    /// # Errors
    /// Returns [`RuntimeFlowControlError::InvalidBackpressurePolicy`] when the policy
    /// is unbounded or could silently drop terminal/approval events.
    pub fn validate(&self) -> Result<(), RuntimeFlowControlError> {
        if self.schema_version != RUNTIME_FLOW_CONTROL_SCHEMA_VERSION
            || self.capacity == 0
            || self.capacity > MAX_RUNTIME_CHANNEL_CAPACITY
            || self.max_summary_bytes == 0
            || self.max_summary_bytes > 8 * 1024
            || !self.preserve_terminal
            || !self.preserve_approval
        {
            return Err(RuntimeFlowControlError::InvalidBackpressurePolicy);
        }
        Ok(())
    }
}

/// Flow-control contract validation error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RuntimeFlowControlError {
    /// Cancellation hierarchy or timing is invalid.
    #[error("cancellation context is invalid")]
    InvalidCancellationContext,
    /// A child attempted to exceed its parent deadline.
    #[error("child cancellation deadline cannot exceed parent deadline")]
    DeadlineWidened,
    /// Channel policy is unbounded or can drop protected events.
    #[error("backpressure policy is invalid")]
    InvalidBackpressurePolicy,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root() -> CancellationContextV1 {
        CancellationContextV1 {
            schema_version: 1,
            scope_id: RuntimeOperationId::parse("run_scope").expect("scope id"),
            scope: CancellationScopeKind::Run,
            generation: RuntimeGeneration::new(1).expect("generation"),
            parent_scope_id: None,
            reason: None,
            deadline_unix_ms: Some(1_000),
            graceful_settle_ms: 500,
            hard_abort_after_ms: 2_000,
        }
    }

    #[test]
    fn child_cannot_widen_deadline() {
        assert_eq!(
            root().derive_child(
                RuntimeOperationId::parse("tool_scope").expect("scope id"),
                CancellationScopeKind::ToolExecution,
                Some(1_001),
                100,
                1_000,
            ),
            Err(RuntimeFlowControlError::DeadlineWidened)
        );
    }

    #[test]
    fn protected_events_are_required() {
        let policy = BackpressurePolicy {
            schema_version: 1,
            capacity: 32,
            overflow_action: BackpressureOverflowAction::CoalesceProgress,
            preserve_terminal: false,
            preserve_approval: true,
            max_summary_bytes: 512,
        };
        assert_eq!(policy.validate(), Err(RuntimeFlowControlError::InvalidBackpressurePolicy));
    }
}
