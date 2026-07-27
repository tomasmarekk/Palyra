//! ACP session actor queue and turn timeout contracts.

#![allow(dead_code)]

use std::collections::{BTreeMap, VecDeque};

use serde::{Deserialize, Serialize};

pub(crate) const ACP_SESSION_ACTOR_QUEUE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AcpPreparedTurn {
    pub turn_id: String,
    pub acp_session_id: String,
    pub palyra_session_id: String,
    pub runtime_id: String,
    pub handle_id: String,
    pub mutating: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AcpSessionActorQueuePolicy {
    pub supports_concurrent_turns: bool,
    pub max_active_turns: usize,
    pub max_pending_turns: usize,
}

impl Default for AcpSessionActorQueuePolicy {
    fn default() -> Self {
        Self { supports_concurrent_turns: false, max_active_turns: 1, max_pending_turns: 16 }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AcpTurnQueueDecisionKind {
    Started,
    Queued,
    BackpressureRejected,
    Cancelled,
    Completed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AcpTurnQueueReasonCode {
    StartedImmediately,
    QueuedBehindActive,
    ConcurrencyUnsupported,
    Backpressure,
    CancellationClosedStream,
    HandleReleased,
    Completed,
}

impl AcpTurnQueueReasonCode {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::StartedImmediately => "acp_turn_queue.started_immediately",
            Self::QueuedBehindActive => "acp_turn_queue.queued_behind_active",
            Self::ConcurrencyUnsupported => "acp_turn_queue.concurrency_unsupported",
            Self::Backpressure => "acp_turn_queue.backpressure",
            Self::CancellationClosedStream => "acp_turn_queue.cancellation_closed_stream",
            Self::HandleReleased => "acp_turn_queue.handle_released",
            Self::Completed => "acp_turn_queue.completed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AcpTurnQueueDecision {
    pub schema_version: u32,
    pub decision: AcpTurnQueueDecisionKind,
    pub reason_codes: Vec<AcpTurnQueueReasonCode>,
    pub active_turns: usize,
    pub pending_turns: usize,
    pub closed_stream: bool,
    pub released_handle: bool,
}

#[derive(Debug, Default)]
pub(crate) struct AcpSessionActorQueue {
    active: BTreeMap<String, AcpPreparedTurn>,
    pending: VecDeque<AcpPreparedTurn>,
}

impl AcpSessionActorQueue {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn enqueue(
        &mut self,
        turn: AcpPreparedTurn,
        policy: AcpSessionActorQueuePolicy,
    ) -> AcpTurnQueueDecision {
        let active_limit =
            if policy.supports_concurrent_turns { policy.max_active_turns.max(1) } else { 1 };
        if self.active.len() < active_limit {
            self.active.insert(turn.turn_id.clone(), turn);
            return self.decision(
                AcpTurnQueueDecisionKind::Started,
                vec![AcpTurnQueueReasonCode::StartedImmediately],
                false,
                false,
            );
        }

        if self.pending.len() >= policy.max_pending_turns {
            let mut reasons = vec![AcpTurnQueueReasonCode::Backpressure];
            if !policy.supports_concurrent_turns {
                reasons.push(AcpTurnQueueReasonCode::ConcurrencyUnsupported);
            }
            return self.decision(
                AcpTurnQueueDecisionKind::BackpressureRejected,
                reasons,
                false,
                false,
            );
        }

        self.pending.push_back(turn);
        let mut reasons = vec![AcpTurnQueueReasonCode::QueuedBehindActive];
        if !policy.supports_concurrent_turns {
            reasons.push(AcpTurnQueueReasonCode::ConcurrencyUnsupported);
        }
        self.decision(AcpTurnQueueDecisionKind::Queued, reasons, false, false)
    }

    pub(crate) fn complete_turn(
        &mut self,
        turn_id: &str,
        policy: AcpSessionActorQueuePolicy,
    ) -> AcpTurnQueueDecision {
        self.active.remove(turn_id);
        self.promote_pending(policy);
        self.decision(
            AcpTurnQueueDecisionKind::Completed,
            vec![AcpTurnQueueReasonCode::Completed],
            false,
            false,
        )
    }

    pub(crate) fn cancel_active(&mut self, turn_id: &str) -> AcpTurnQueueDecision {
        self.active.remove(turn_id);
        self.pending.retain(|turn| turn.turn_id != turn_id);
        self.decision(
            AcpTurnQueueDecisionKind::Cancelled,
            vec![
                AcpTurnQueueReasonCode::CancellationClosedStream,
                AcpTurnQueueReasonCode::HandleReleased,
            ],
            true,
            true,
        )
    }

    pub(crate) fn cancel_and_promote(
        &mut self,
        turn_id: &str,
        policy: AcpSessionActorQueuePolicy,
    ) -> AcpTurnQueueDecision {
        self.active.remove(turn_id);
        self.pending.retain(|turn| turn.turn_id != turn_id);
        self.promote_pending(policy);
        self.decision(
            AcpTurnQueueDecisionKind::Cancelled,
            vec![
                AcpTurnQueueReasonCode::CancellationClosedStream,
                AcpTurnQueueReasonCode::HandleReleased,
            ],
            true,
            true,
        )
    }

    #[must_use]
    pub(crate) fn is_active(&self, turn_id: &str) -> bool {
        self.active.contains_key(turn_id)
    }

    #[must_use]
    pub(crate) fn contains(&self, turn_id: &str) -> bool {
        self.active.contains_key(turn_id) || self.pending.iter().any(|turn| turn.turn_id == turn_id)
    }

    #[must_use]
    pub(crate) fn active_turn_count(&self) -> usize {
        self.active.len()
    }

    #[must_use]
    pub(crate) fn pending_turn_count(&self) -> usize {
        self.pending.len()
    }

    fn promote_pending(&mut self, policy: AcpSessionActorQueuePolicy) {
        let active_limit =
            if policy.supports_concurrent_turns { policy.max_active_turns.max(1) } else { 1 };
        while self.active.len() < active_limit {
            let Some(turn) = self.pending.pop_front() else {
                break;
            };
            self.active.insert(turn.turn_id.clone(), turn);
        }
    }

    fn decision(
        &self,
        decision: AcpTurnQueueDecisionKind,
        reason_codes: Vec<AcpTurnQueueReasonCode>,
        closed_stream: bool,
        released_handle: bool,
    ) -> AcpTurnQueueDecision {
        AcpTurnQueueDecision {
            schema_version: ACP_SESSION_ACTOR_QUEUE_SCHEMA_VERSION,
            decision,
            reason_codes,
            active_turns: self.active.len(),
            pending_turns: self.pending.len(),
            closed_stream,
            released_handle,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AcpTurnTimeoutPhase {
    Startup,
    Model,
    Tool,
    PermissionWait,
    Idle,
    Overall,
}

impl AcpTurnTimeoutPhase {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Startup => "startup",
            Self::Model => "model",
            Self::Tool => "tool",
            Self::PermissionWait => "permission_wait",
            Self::Idle => "idle",
            Self::Overall => "overall",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AcpTurnTimeoutInput {
    pub phase: AcpTurnTimeoutPhase,
    pub elapsed_ms: u64,
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AcpTurnTimeoutClassification {
    pub timed_out: bool,
    pub phase: AcpTurnTimeoutPhase,
    pub reason_code: String,
}

#[must_use]
pub(crate) fn classify_turn_timeout(input: AcpTurnTimeoutInput) -> AcpTurnTimeoutClassification {
    let timed_out = input.elapsed_ms >= input.timeout_ms;
    let reason_code = if timed_out {
        format!("acp_turn_timeout.{}", input.phase.as_str())
    } else {
        "acp_turn_timeout.within_budget".to_owned()
    };
    AcpTurnTimeoutClassification { timed_out, phase: input.phase, reason_code }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn turn(id: &str) -> AcpPreparedTurn {
        AcpPreparedTurn {
            turn_id: id.to_owned(),
            acp_session_id: "acp-session-a".to_owned(),
            palyra_session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            runtime_id: "native-acp".to_owned(),
            handle_id: "handle-1".to_owned(),
            mutating: true,
        }
    }

    #[test]
    fn actor_queue_serializes_turns_without_concurrency() {
        let mut queue = AcpSessionActorQueue::new();
        let policy = AcpSessionActorQueuePolicy {
            supports_concurrent_turns: false,
            max_active_turns: 4,
            max_pending_turns: 4,
        };

        let first = queue.enqueue(turn("turn-1"), policy);
        let second = queue.enqueue(turn("turn-2"), policy);

        assert_eq!(first.decision, AcpTurnQueueDecisionKind::Started);
        assert_eq!(second.decision, AcpTurnQueueDecisionKind::Queued);
        assert_eq!(queue.active_turn_count(), 1);
        assert_eq!(queue.pending_turn_count(), 1);
        assert!(second.reason_codes.contains(&AcpTurnQueueReasonCode::ConcurrencyUnsupported));
    }

    #[test]
    fn backpressure_rejects_after_max_pending_turns() {
        let mut queue = AcpSessionActorQueue::new();
        let policy = AcpSessionActorQueuePolicy {
            supports_concurrent_turns: false,
            max_active_turns: 1,
            max_pending_turns: 1,
        };

        queue.enqueue(turn("turn-1"), policy);
        queue.enqueue(turn("turn-2"), policy);
        let rejected = queue.enqueue(turn("turn-3"), policy);

        assert_eq!(rejected.decision, AcpTurnQueueDecisionKind::BackpressureRejected);
        assert!(rejected.reason_codes.contains(&AcpTurnQueueReasonCode::Backpressure));
    }

    #[test]
    fn cancellation_closes_stream_and_releases_handle() {
        let mut queue = AcpSessionActorQueue::new();
        queue.enqueue(turn("turn-1"), AcpSessionActorQueuePolicy::default());

        let cancelled = queue.cancel_active("turn-1");

        assert_eq!(cancelled.decision, AcpTurnQueueDecisionKind::Cancelled);
        assert!(cancelled.closed_stream);
        assert!(cancelled.released_handle);
        assert_eq!(queue.active_turn_count(), 0);
    }

    #[test]
    fn timeout_classification_uses_stable_phase_codes() {
        let classified = classify_turn_timeout(AcpTurnTimeoutInput {
            phase: AcpTurnTimeoutPhase::PermissionWait,
            elapsed_ms: 10_000,
            timeout_ms: 5_000,
        });

        assert!(classified.timed_out);
        assert_eq!(classified.reason_code, "acp_turn_timeout.permission_wait");
    }
}
