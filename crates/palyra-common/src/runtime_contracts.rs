//! Shared runtime vocabulary for queueing, flow orchestration, delivery policy,
//! auxiliary tasks, and worker lifecycle reporting.
//!
//! Design note:
//! - These enums define the canonical wire names that runtime preview stabilizes before
//!   queue, retrieval, flow, and worker business logic is expanded.
//! - Backward-compatible aliases keep persisted records and existing UI payloads
//!   readable while new surfaces emit only the canonical forms.
//! - Intentionally deferred variants stay out of this module until the
//!   corresponding behavior is implemented and covered by rollout/config
//!   guardrails, diagnostics, and regression harnesses.
//!
use serde::{Deserialize, Serialize};
use std::fmt;

macro_rules! runtime_contract_enum {
    (
        $(#[$meta:meta])*
        pub enum $name:ident {
            $(
                $variant:ident => $canonical:literal $(| $alias:literal )*
            ),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        pub enum $name {
            $(
                #[serde(rename = $canonical $(, alias = $alias)*)]
                $variant,
            )+
        }

        impl $name {
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(
                        Self::$variant => $canonical,
                    )+
                }
            }

            #[must_use]
            pub fn parse(value: &str) -> Option<Self> {
                let normalized = value.trim().to_ascii_lowercase();
                match normalized.as_str() {
                    $(
                        $canonical $(| $alias )* => Some(Self::$variant),
                    )+
                    _ => None,
                }
            }

            #[allow(clippy::should_implement_trait)]
            #[must_use]
            pub fn from_str(value: &str) -> Option<Self> {
                Self::parse(value)
            }
        }

        impl std::str::FromStr for $name {
            type Err = ();

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::parse(value).ok_or(())
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }
    };
}

runtime_contract_enum! {
    /// Canonical queue runtime modes used by queue orchestration surfaces.
    pub enum QueueMode {
        Followup => "followup" | "follow_up",
        Collect => "collect",
        Steer => "steer",
        SteerBacklog => "steer_backlog" | "steer-backlog",
        Interrupt => "interrupt"
    }
}

runtime_contract_enum! {
    /// Canonical queue decisions used by queue explainability and event payloads.
    pub enum QueueDecision {
        Enqueue => "enqueue",
        Merge => "merge" | "coalesce",
        Steer => "steer",
        SteerBacklog => "steer_backlog" | "steer-backlog",
        Interrupt => "interrupt",
        Overflow => "overflow",
        Defer => "defer" | "deferred"
    }
}

runtime_contract_enum! {
    /// High-level pruning policy classes that keep future rollout knobs stable.
    pub enum PruningPolicyClass {
        Disabled => "disabled" | "off",
        Conservative => "conservative" | "safe",
        Balanced => "balanced" | "default",
        Aggressive => "aggressive" | "high_reduction"
    }
}

runtime_contract_enum! {
    /// Background and auxiliary task kinds shared across daemon, CLI, and web console.
    pub enum AuxiliaryTaskKind {
        BackgroundPrompt => "background_prompt",
        DelegationPrompt => "delegation_prompt",
        AttachmentDerivation => "attachment_derivation",
        AttachmentRecompute => "attachment_recompute",
        PostRunReflection => "post_run_reflection" | "reflection"
    }
}

runtime_contract_enum! {
    /// Canonical auxiliary task lifecycle states.
    pub enum AuxiliaryTaskState {
        Queued => "queued" | "pending",
        Running => "running" | "in_progress",
        Paused => "paused",
        Succeeded => "succeeded" | "complete" | "completed",
        Failed => "failed",
        CancelRequested => "cancel_requested",
        Cancelled => "cancelled" | "canceled",
        Expired => "expired"
    }
}

impl AuxiliaryTaskState {
    #[must_use]
    pub const fn is_active(self) -> bool {
        matches!(self, Self::Queued | Self::Running | Self::Paused | Self::CancelRequested)
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled | Self::Expired)
    }
}

runtime_contract_enum! {
    /// Queue lifecycle states currently persisted for queued inputs.
    pub enum QueuedInputState {
        Pending => "pending" | "queued",
        Forwarded => "forwarded" | "delivered",
        DeliveryFailed => "delivery_failed" | "failed_delivery",
        Merged => "merged",
        Steered => "steered",
        Interrupted => "interrupted",
        Overflowed => "overflowed" | "overflow"
    }
}

impl QueuedInputState {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        !matches!(self, Self::Pending)
    }
}

runtime_contract_enum! {
    /// Canonical flow states for future durable orchestration surfaces.
    pub enum FlowState {
        Pending => "pending",
        Ready => "ready",
        Running => "running" | "in_progress",
        WaitingForApproval => "waiting_for_approval" | "approval_wait" | "waiting",
        Paused => "paused",
        Blocked => "blocked",
        Retrying => "retrying",
        Compensating => "compensating",
        TimedOut => "timed_out" | "timeout",
        Succeeded => "succeeded" | "completed",
        Failed => "failed",
        CancelRequested => "cancel_requested",
        Cancelled => "cancelled" | "canceled"
    }
}

impl FlowState {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::TimedOut | Self::Cancelled)
    }
}

runtime_contract_enum! {
    /// Canonical flow step states for step adapter and retry surfaces.
    pub enum FlowStepState {
        Pending => "pending",
        Ready => "ready",
        Running => "running" | "in_progress",
        WaitingForApproval => "waiting_for_approval" | "approval_wait" | "waiting",
        Paused => "paused",
        Blocked => "blocked",
        Retrying => "retrying",
        Skipped => "skipped",
        Compensating => "compensating",
        Compensated => "compensated",
        TimedOut => "timed_out" | "timeout",
        Succeeded => "succeeded" | "completed",
        Failed => "failed",
        CancelRequested => "cancel_requested",
        Cancelled => "cancelled" | "canceled"
    }
}

impl FlowStepState {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Skipped
                | Self::Compensated
                | Self::TimedOut
                | Self::Succeeded
                | Self::Failed
                | Self::Cancelled
        )
    }
}

runtime_contract_enum! {
    /// Delivery arbitration policies reserved for descendant-aware completion.
    pub enum DeliveryPolicy {
        PreferTerminalDescendant => "prefer_terminal_descendant" | "prefer_child_terminal",
        SuppressStaleParent => "suppress_stale_parent",
        MergeProgressUpdates => "merge_progress_updates" | "coalesce_progress",
        DeliverInterimParent => "deliver_interim_parent",
        RequireFinalReview => "require_final_review"
    }
}

runtime_contract_enum! {
    /// Shared worker lifecycle states surfaced by preview diagnostics and audit events.
    pub enum WorkerLifecycleState {
        Registered => "registered",
        Assigned => "assigned" | "leased",
        Completed => "completed" | "succeeded",
        Failed => "failed",
        Orphaned => "orphaned"
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AuxiliaryTaskKind, AuxiliaryTaskState, DeliveryPolicy, FlowState, FlowStepState,
        PruningPolicyClass, QueueDecision, QueueMode, QueuedInputState, WorkerLifecycleState,
    };

    #[test]
    fn queue_modes_round_trip_with_canonical_serialization() {
        let serialized =
            serde_json::to_string(&QueueMode::SteerBacklog).expect("queue mode should serialize");
        assert_eq!(serialized, "\"steer_backlog\"");
        let parsed: QueueMode =
            serde_json::from_str("\"steer_backlog\"").expect("queue mode should deserialize");
        assert_eq!(parsed, QueueMode::SteerBacklog);
        assert_eq!(parsed.as_str(), "steer_backlog");
    }

    #[test]
    fn runtime_contract_aliases_stay_backward_compatible() {
        assert_eq!(QueueMode::parse("follow_up"), Some(QueueMode::Followup));
        assert_eq!(QueueDecision::parse("coalesce"), Some(QueueDecision::Merge));
        assert_eq!(QueuedInputState::parse("delivered"), Some(QueuedInputState::Forwarded));
        assert_eq!(AuxiliaryTaskState::parse("canceled"), Some(AuxiliaryTaskState::Cancelled));
        assert_eq!(WorkerLifecycleState::parse("leased"), Some(WorkerLifecycleState::Assigned));
    }

    #[test]
    fn task_and_flow_state_helpers_identify_terminal_states() {
        assert!(AuxiliaryTaskState::Succeeded.is_terminal());
        assert!(AuxiliaryTaskState::Queued.is_active());
        assert!(QueuedInputState::DeliveryFailed.is_terminal());
        assert!(FlowState::TimedOut.is_terminal());
        assert!(FlowStepState::Compensated.is_terminal());
    }

    #[test]
    fn extended_runtime_contracts_expose_expected_canonical_names() {
        assert_eq!(PruningPolicyClass::Balanced.as_str(), "balanced");
        assert_eq!(AuxiliaryTaskKind::PostRunReflection.as_str(), "post_run_reflection");
        assert_eq!(DeliveryPolicy::PreferTerminalDescendant.as_str(), "prefer_terminal_descendant");
    }
}
