//! Session input queue policy: admission, coalescing, and busy-state analysis.
//!
//! When new input arrives for a session that may already have an active run,
//! [`decide_session_queue_mode`] maps the requested [`QueueMode`] onto a
//! [`QueueDecision`] gated by [`SessionQueueSafeBoundary`]: interrupts and
//! steering are honored only at safe points (no pending approval, no
//! sensitive tool execution), otherwise the input defers into collect mode,
//! and hitting the per-group cap forces a deterministic overflow summary
//! instead of unbounded queue growth. [`build_queue_collect_summary`] renders
//! that summary with full provenance; [`analyze_session_queue`] derives the
//! operator-facing busy state and depth/age/fairness metrics; and
//! [`decide_queue_steering`] projects explicit queued-input lane changes into
//! journal-ready decisions. This module stays pure over journal
//! `OrchestratorQueuedInputRecord`s; runtime code owns persistence, audit, and
//! forwarding side effects.

use std::collections::BTreeSet;

use palyra_common::runtime_contracts::{
    QueueDecision, QueueMode, QueueOutcome, QueuedInputDeliveryBoundary, QueuedInputState,
    QUEUE_OUTCOME_SCHEMA_VERSION,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::config::SessionQueuePolicyConfig;
use crate::journal::OrchestratorQueuedInputRecord;

/// Policy identifier recorded in queue decisions and explain payloads.
pub(crate) const SESSION_QUEUE_POLICY_ID: &str = "session_queue.v1";
pub(crate) const QUEUE_STEERING_SCHEMA_VERSION: i64 = 1;
pub(crate) const QUEUE_STEERING_EVENT_STARTED: &str = "queue_steering_pro_queued_inputs.started";
pub(crate) const QUEUE_STEERING_EVENT_COMPLETED: &str =
    "queue_steering_pro_queued_inputs.completed";
pub(crate) const QUEUE_STEERING_EVENT_FAILED: &str = "queue_steering_pro_queued_inputs.failed";
pub(crate) const QUEUE_STEERING_REDACTION_NONE: &str = "none";
const DEFAULT_PRIORITY_LANE: &str = "normal";
const DEFAULT_DROP_POLICY: &str = "summarize_oldest";
const DEFAULT_OVERFLOW_BEHAVIOR: &str = "deterministic_backlog_summary";
const COLLECT_SUMMARY_MAX_ITEMS: usize = 12;
const COLLECT_SUMMARY_TEXT_LIMIT: usize = 240;

/// Effective queue policy for one session: cap, debounce, and coalescing scope.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct SessionQueuePolicy {
    pub(crate) policy_id: String,
    /// Default mode applied when the caller does not request one explicitly.
    pub(crate) mode: QueueMode,
    pub(crate) priority_lane: String,
    pub(crate) debounce_ms: u64,
    /// Pending-depth limit per coalescing group; reaching it forces overflow.
    pub(crate) cap: usize,
    pub(crate) drop_policy: String,
    pub(crate) overflow_behavior: String,
    /// Scope key that groups inputs for merging and depth accounting.
    pub(crate) coalescing_group: String,
    pub(crate) source: String,
}

/// Snapshot of where the active run currently stands; gates interrupt/steer.
///
/// The flags feed [`Self::can_steer`] and [`Self::can_interrupt`]: both are
/// denied while an approval, sensitive tool, or final delivery is active.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct SessionQueueSafeBoundary {
    pub(crate) active_run_stream: bool,
    pub(crate) pending_approval: bool,
    pub(crate) sensitive_tool_execution: bool,
    pub(crate) delivery_in_progress: bool,
    pub(crate) before_model_round: bool,
    pub(crate) after_model_round: bool,
    pub(crate) after_tool_result: bool,
    pub(crate) after_approval_wait: bool,
    pub(crate) after_child_merge: bool,
}

/// Outcome of queue admission: decision, effective mode, and policy snapshot.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct SessionQueueDecision {
    pub(crate) decision: QueueDecision,
    pub(crate) requested_mode: QueueMode,
    /// Mode actually applied, which may differ from the requested one (for
    /// example a deferred interrupt lands in collect mode).
    pub(crate) mode: QueueMode,
    /// False only for overflow: the input must not enter the pending queue.
    pub(crate) accepted: bool,
    pub(crate) reason: String,
    pub(crate) delivery_boundary: QueuedInputDeliveryBoundary,
    pub(crate) safe_boundary: SessionQueueSafeBoundary,
    pub(crate) policy: SessionQueuePolicy,
}

/// Deterministic backlog summary produced when queued inputs are collected.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct QueueCollectSummary {
    pub(crate) summary_ref: String,
    /// Human-readable digest; bounded to the first few queued inputs.
    pub(crate) text: String,
    pub(crate) source_count: usize,
    /// Deduplicated attachment references retained for the summary input.
    pub(crate) attachment_refs_json: Value,
    /// Full audit trail: every source id is retained even when the rendered
    /// text omits items beyond the display bound.
    pub(crate) provenance_json: Value,
}

/// Origin lane of a queued input; drives fairness counts and prioritization.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SessionQueueProfile {
    Interactive,
    Background,
    Routine,
    OperatorPriority,
}

impl SessionQueueProfile {
    /// Returns the stable snake_case identifier used in metrics payloads.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Interactive => "interactive",
            Self::Background => "background",
            Self::Routine => "routine",
            Self::OperatorPriority => "operator_priority",
        }
    }
}

/// Operator-facing session state derived from queue metrics and run boundaries.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SessionBusyState {
    Idle,
    BusyAcceptsFollowups,
    BusyCollecting,
    WaitingOnApproval,
    Backpressured,
    Paused,
}

impl SessionBusyState {
    /// Returns the stable snake_case identifier used in analysis payloads.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::BusyAcceptsFollowups => "busy_accepts_followups",
            Self::BusyCollecting => "busy_collecting",
            Self::WaitingOnApproval => "waiting_on_approval",
            Self::Backpressured => "backpressured",
            Self::Paused => "paused",
        }
    }
}

/// Per-profile tallies of queued inputs within one coalescing group.
#[derive(Debug, Default, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct SessionQueueProfileCounts {
    pub(crate) interactive: usize,
    pub(crate) background: usize,
    pub(crate) routine: usize,
    pub(crate) operator_priority: usize,
}

impl SessionQueueProfileCounts {
    fn observe(&mut self, profile: SessionQueueProfile) {
        match profile {
            SessionQueueProfile::Interactive => self.interactive += 1,
            SessionQueueProfile::Background => self.background += 1,
            SessionQueueProfile::Routine => self.routine += 1,
            SessionQueueProfile::OperatorPriority => self.operator_priority += 1,
        }
    }
}

/// Depth, age, and fairness metrics for one session's queue.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct SessionQueueMetrics {
    pub(crate) pending_depth: usize,
    pub(crate) terminal_count: usize,
    pub(crate) total_count: usize,
    pub(crate) oldest_pending_age_ms: Option<u64>,
    pub(crate) newest_pending_age_ms: Option<u64>,
    /// Pending inputs beyond the first one, i.e. how many could merge.
    pub(crate) merge_candidate_count: usize,
    pub(crate) merged_count: usize,
    pub(crate) overflowed_count: usize,
    pub(crate) operator_priority_pending: usize,
    pub(crate) profile_counts: SessionQueueProfileCounts,
}

impl SessionQueueMetrics {
    /// Renders the metrics as a JSON object for explain/console payloads.
    #[must_use]
    pub(crate) fn snapshot_json(&self) -> Value {
        json!(self)
    }
}

/// Busy-state classification plus the recommended next operator action.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct SessionQueueAnalysis {
    pub(crate) busy_state: SessionBusyState,
    pub(crate) recommendation: String,
    pub(crate) metrics: SessionQueueMetrics,
}

impl SessionQueueAnalysis {
    /// Renders the analysis as a JSON object for console payloads.
    #[must_use]
    pub(crate) fn snapshot_json(&self) -> Value {
        json!({
            "busy_state": self.busy_state.as_str(),
            "recommendation": self.recommendation,
            "metrics": self.metrics.snapshot_json(),
        })
    }
}

/// Runtime action selected for a queued-input steering request.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum QueueSteeringAction {
    Noop,
    SetPriorityLane,
    Reject,
}

impl QueueSteeringAction {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Noop => "noop",
            Self::SetPriorityLane => "set_priority_lane",
            Self::Reject => "reject",
        }
    }
}

/// Stable reason codes for queued-input steering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QueueSteeringReasonCode {
    PriorityLaneSelected,
    AlreadyInRequestedLane,
    MissingActor,
    MissingPriorityLane,
    InvalidPriorityLane,
    NonPendingInput,
}

impl QueueSteeringReasonCode {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::PriorityLaneSelected => "queue_steering.priority_lane_selected",
            Self::AlreadyInRequestedLane => "queue_steering.already_in_requested_lane",
            Self::MissingActor => "queue_steering.missing_actor",
            Self::MissingPriorityLane => "queue_steering.missing_priority_lane",
            Self::InvalidPriorityLane => "queue_steering.invalid_priority_lane",
            Self::NonPendingInput => "queue_steering.non_pending_input",
        }
    }
}

/// Operator request to steer one queued input.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct QueueSteeringRequest {
    pub(crate) actor_principal: String,
    pub(crate) requested_priority_lane: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) reason: Option<String>,
}

/// Pure decision and journal projection for queued-input steering.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct QueueSteeringDecision {
    pub(crate) schema_version: i64,
    pub(crate) action: QueueSteeringAction,
    pub(crate) accepted: bool,
    pub(crate) reason_code: String,
    pub(crate) queued_input_id: String,
    pub(crate) session_id: String,
    pub(crate) run_id: String,
    pub(crate) from_priority_lane: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) to_priority_lane: Option<String>,
    pub(crate) terminal_event_type: String,
    pub(crate) payload_json: String,
    pub(crate) evidence_refs_json: String,
    pub(crate) redaction_level: String,
}

impl SessionQueuePolicy {
    /// Builds the effective policy for a session from daemon configuration.
    ///
    /// The coalescing group is scoped by the most specific identity
    /// available: agent id over channel over a bare session scope.
    #[must_use]
    pub(crate) fn from_config(
        config: &SessionQueuePolicyConfig,
        session_id: &str,
        channel: Option<&str>,
        agent_id: Option<&str>,
    ) -> Self {
        let scope = agent_id
            .map(|agent_id| format!("agent:{agent_id}"))
            .or_else(|| channel.map(|channel| format!("channel:{channel}")))
            .unwrap_or_else(|| "session".to_owned());
        Self {
            policy_id: SESSION_QUEUE_POLICY_ID.to_owned(),
            mode: QueueMode::Followup,
            priority_lane: DEFAULT_PRIORITY_LANE.to_owned(),
            debounce_ms: config.merge_window_ms,
            cap: config.max_depth,
            drop_policy: DEFAULT_DROP_POLICY.to_owned(),
            overflow_behavior: DEFAULT_OVERFLOW_BEHAVIOR.to_owned(),
            coalescing_group: format!("{scope}:{session_id}"),
            source: "config.session_queue_policy".to_owned(),
        }
    }

    /// Renders the policy as the `policy` object embedded in explain payloads.
    #[must_use]
    pub(crate) fn snapshot_json(&self) -> Value {
        json!({
            "policy_id": self.policy_id,
            "mode": self.mode.as_str(),
            "priority_lane": self.priority_lane,
            "debounce_ms": self.debounce_ms,
            "cap": self.cap,
            "drop_policy": self.drop_policy,
            "overflow_behavior": self.overflow_behavior,
            "coalescing_group": self.coalescing_group,
            "source": self.source,
            "supported_profiles": [
                SessionQueueProfile::Interactive.as_str(),
                SessionQueueProfile::Background.as_str(),
                SessionQueueProfile::Routine.as_str(),
                SessionQueueProfile::OperatorPriority.as_str(),
            ],
        })
    }
}

impl SessionQueueSafeBoundary {
    /// Builds the boundary snapshot for a possibly-active run.
    ///
    /// Boundary flags that cannot be observed from here default to `false`,
    /// which keeps the gate conservative: steering is only allowed at the
    /// pre-model-round point this constructor can actually prove.
    #[must_use]
    pub(crate) fn active(active_run_stream: bool, pending_approval: bool) -> Self {
        Self {
            active_run_stream,
            pending_approval,
            sensitive_tool_execution: false,
            delivery_in_progress: false,
            before_model_round: active_run_stream && !pending_approval,
            after_model_round: false,
            after_tool_result: false,
            after_approval_wait: pending_approval,
            after_child_merge: false,
        }
    }

    /// True when injected guidance may join the run at the current boundary.
    #[must_use]
    pub(crate) const fn can_steer(&self) -> bool {
        self.active_run_stream
            && !self.pending_approval
            && !self.sensitive_tool_execution
            && !self.delivery_in_progress
            && (self.before_model_round
                || self.after_model_round
                || self.after_tool_result
                || self.after_child_merge)
    }

    /// True when the run may be interrupted without abandoning an approval
    /// or a sensitive tool mid-flight.
    #[must_use]
    pub(crate) const fn can_interrupt(&self) -> bool {
        self.active_run_stream
            && !self.pending_approval
            && !self.sensitive_tool_execution
            && !self.delivery_in_progress
    }
}

impl SessionQueueDecision {
    /// Renders the decision and its inputs as a JSON explain payload.
    #[must_use]
    pub(crate) fn explain_json(&self) -> Value {
        json!({
            "decision": self.decision.as_str(),
            "requested_mode": self.requested_mode.as_str(),
            "mode": self.mode.as_str(),
            "accepted": self.accepted,
            "reason": self.reason,
            "delivery_boundary": self.delivery_boundary.as_str(),
            "safe_boundary": self.safe_boundary,
            "policy": self.policy.snapshot_json(),
        })
    }
}

/// Decides how a new session input is admitted into the queue.
///
/// Interrupt and steer requests are honored only when the safe boundary
/// allows them; otherwise they defer into collect mode rather than being
/// rejected, so no operator input is lost.
#[must_use]
pub(crate) fn decide_session_queue_mode(
    policy: SessionQueuePolicy,
    requested_mode: Option<QueueMode>,
    safe_boundary: SessionQueueSafeBoundary,
    current_depth: usize,
) -> SessionQueueDecision {
    let requested_mode = requested_mode.unwrap_or(policy.mode);
    // The cap wins over any requested mode: at capacity the only acceptable
    // outcome is a collect-mode overflow summary, and the input itself is
    // not admitted into the pending queue.
    if current_depth >= policy.cap {
        return SessionQueueDecision {
            decision: QueueDecision::Overflow,
            requested_mode,
            mode: QueueMode::Collect,
            accepted: false,
            reason: "queue_cap_reached_overflow_summary_required".to_owned(),
            delivery_boundary: QueuedInputDeliveryBoundary::BacklogSummary,
            safe_boundary,
            policy,
        };
    }
    let (decision, mode, reason) = match requested_mode {
        QueueMode::Interrupt if safe_boundary.can_interrupt() => {
            (QueueDecision::Interrupt, QueueMode::Interrupt, "safe_boundary_allows_interrupt")
        }
        QueueMode::Interrupt => {
            (QueueDecision::Defer, QueueMode::Collect, "interrupt_deferred_until_safe_boundary")
        }
        QueueMode::Steer if safe_boundary.can_steer() => {
            (QueueDecision::Steer, QueueMode::Steer, "safe_boundary_allows_steer")
        }
        QueueMode::Steer => {
            (QueueDecision::Defer, QueueMode::Collect, "steer_deferred_until_safe_boundary")
        }
        QueueMode::SteerBacklog => {
            (QueueDecision::SteerBacklog, QueueMode::SteerBacklog, "backlog_steering_requested")
        }
        QueueMode::Collect => (QueueDecision::Enqueue, QueueMode::Collect, "collect_requested"),
        QueueMode::Followup => (QueueDecision::Enqueue, QueueMode::Followup, "followup_requested"),
    };
    SessionQueueDecision {
        decision,
        requested_mode,
        mode,
        accepted: true,
        reason: reason.to_owned(),
        delivery_boundary: delivery_boundary_for_mode(requested_mode, decision, mode),
        safe_boundary,
        policy,
    }
}

#[must_use]
pub(crate) fn queue_outcome(
    queued_input_id: impl Into<String>,
    lifecycle_state: QueuedInputState,
    delivery_boundary: QueuedInputDeliveryBoundary,
    expected_active_generation: Option<u64>,
    observed_active_generation: Option<u64>,
    accepted: bool,
    reason_code: impl Into<String>,
) -> QueueOutcome {
    QueueOutcome {
        schema_version: QUEUE_OUTCOME_SCHEMA_VERSION,
        queued_input_id: queued_input_id.into(),
        lifecycle_state,
        delivery_boundary,
        expected_active_generation,
        observed_active_generation,
        accepted,
        reason_code: reason_code.into(),
    }
}

#[must_use]
const fn delivery_boundary_for_mode(
    requested_mode: QueueMode,
    decision: QueueDecision,
    effective_mode: QueueMode,
) -> QueuedInputDeliveryBoundary {
    if matches!(decision, QueueDecision::Overflow | QueueDecision::Defer)
        || matches!(effective_mode, QueueMode::Collect | QueueMode::SteerBacklog)
    {
        return QueuedInputDeliveryBoundary::BacklogSummary;
    }
    match requested_mode {
        QueueMode::Followup => QueuedInputDeliveryBoundary::NextTurn,
        QueueMode::Steer => QueuedInputDeliveryBoundary::CurrentRunBeforeProvider,
        QueueMode::Interrupt => QueuedInputDeliveryBoundary::CancelThenNextTurn,
        QueueMode::Collect | QueueMode::SteerBacklog => QueuedInputDeliveryBoundary::BacklogSummary,
    }
}

/// Builds the deterministic backlog summary for collected queued inputs.
///
/// The rendered text and per-source provenance entries are bounded to the
/// first [`COLLECT_SUMMARY_MAX_ITEMS`] inputs with truncated previews, but
/// every source id is recorded so omitted inputs remain auditable.
#[must_use]
pub(crate) fn build_queue_collect_summary(
    summary_ref: String,
    queued_inputs: &[OrchestratorQueuedInputRecord],
    reason: &str,
) -> QueueCollectSummary {
    let source_count = queued_inputs.len();
    let rendered_items = queued_inputs.iter().take(COLLECT_SUMMARY_MAX_ITEMS).map(|queued| {
        json!({
            "queued_input_id": queued.queued_input_id,
            "run_id": queued.run_id,
            "queue_mode": queued.queue_mode,
            "priority_lane": queued.priority_lane,
            "created_at_unix_ms": queued.created_at_unix_ms,
            "decision_reason": queued.decision_reason,
            "text_preview": truncate_for_summary(queued.text.as_str(), COLLECT_SUMMARY_TEXT_LIMIT),
        })
    });
    let source_ids =
        queued_inputs.iter().map(|queued| queued.queued_input_id.clone()).collect::<Vec<_>>();
    let attachment_refs = queued_inputs
        .iter()
        .flat_map(queued_attachment_refs)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|artifact_id| json!({ "artifact_id": artifact_id }))
        .collect::<Vec<_>>();
    let omitted_count = source_count.saturating_sub(COLLECT_SUMMARY_MAX_ITEMS);
    let mut lines = Vec::with_capacity(source_count.min(COLLECT_SUMMARY_MAX_ITEMS) + 2);
    lines.push(format!("Collected {source_count} queued input(s) for later handling."));
    for (index, queued) in queued_inputs.iter().take(COLLECT_SUMMARY_MAX_ITEMS).enumerate() {
        lines.push(format!(
            "{}. {}",
            index + 1,
            truncate_for_summary(queued.text.as_str(), COLLECT_SUMMARY_TEXT_LIMIT)
        ));
    }
    if omitted_count > 0 {
        lines.push(format!("... {omitted_count} additional queued input(s) omitted."));
    }
    QueueCollectSummary {
        summary_ref: summary_ref.clone(),
        text: lines.join("\n"),
        source_count,
        attachment_refs_json: Value::Array(attachment_refs.clone()),
        provenance_json: json!({
            "summary_ref": summary_ref,
            "reason": reason,
            "source_count": source_count,
            "omitted_count": omitted_count,
            "source_queued_input_ids": source_ids,
            "attachment_refs": attachment_refs,
            "sources": rendered_items.collect::<Vec<_>>(),
        }),
    }
}

fn queued_attachment_refs(queued: &OrchestratorQueuedInputRecord) -> Vec<String> {
    serde_json::from_str::<Value>(queued.attachments_json.as_str())
        .ok()
        .and_then(|value| value.as_array().cloned())
        .unwrap_or_default()
        .into_iter()
        .filter_map(|value| {
            value
                .get("artifact_id")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|artifact_id| !artifact_id.is_empty())
                .map(str::to_owned)
        })
        .collect()
}

/// Classifies the session's busy state and recommends the next operator step.
///
/// Metrics are scoped to the policy's coalescing group so unrelated lanes in
/// the same session do not skew the depth or age numbers.
#[must_use]
pub(crate) fn analyze_session_queue(
    queued_inputs: &[OrchestratorQueuedInputRecord],
    policy: &SessionQueuePolicy,
    safe_boundary: &SessionQueueSafeBoundary,
    paused: bool,
    observed_at_unix_ms: i64,
) -> SessionQueueAnalysis {
    let metrics = session_queue_metrics(
        queued_inputs,
        Some(policy.coalescing_group.as_str()),
        observed_at_unix_ms,
    );
    let busy_state = derive_session_busy_state(&metrics, policy, safe_boundary, paused);
    let recommendation = match busy_state {
        SessionBusyState::Idle => "start_new_run".to_owned(),
        SessionBusyState::BusyAcceptsFollowups => "send_followup_or_choose_interrupt".to_owned(),
        SessionBusyState::BusyCollecting => "wait_for_merge_or_collect_summary".to_owned(),
        SessionBusyState::WaitingOnApproval => "wait_for_approval_before_forwarding".to_owned(),
        SessionBusyState::Backpressured => {
            "drain_or_collect_summary_before_accepting_more".to_owned()
        }
        SessionBusyState::Paused => "resume_reject_or_drain_before_forwarding".to_owned(),
    };
    SessionQueueAnalysis { busy_state, recommendation, metrics }
}

/// Decides whether and how one pending queued input should move lanes.
#[must_use]
pub(crate) fn decide_queue_steering(
    queued: &OrchestratorQueuedInputRecord,
    request: &QueueSteeringRequest,
) -> QueueSteeringDecision {
    if request.actor_principal.trim().is_empty() {
        return queue_steering_decision(
            queued,
            QueueSteeringAction::Reject,
            false,
            QueueSteeringReasonCode::MissingActor,
            None,
        );
    }
    let requested_lane = request.requested_priority_lane.trim();
    if requested_lane.is_empty() {
        return queue_steering_decision(
            queued,
            QueueSteeringAction::Reject,
            false,
            QueueSteeringReasonCode::MissingPriorityLane,
            None,
        );
    }
    let Some(normalized_lane) = normalize_priority_lane(requested_lane) else {
        return queue_steering_decision(
            queued,
            QueueSteeringAction::Reject,
            false,
            QueueSteeringReasonCode::InvalidPriorityLane,
            None,
        );
    };
    if queued.state != "pending" {
        return queue_steering_decision(
            queued,
            QueueSteeringAction::Reject,
            false,
            QueueSteeringReasonCode::NonPendingInput,
            Some(normalized_lane),
        );
    }
    if queued.priority_lane == normalized_lane {
        return queue_steering_decision(
            queued,
            QueueSteeringAction::Noop,
            true,
            QueueSteeringReasonCode::AlreadyInRequestedLane,
            Some(normalized_lane),
        );
    }
    queue_steering_decision(
        queued,
        QueueSteeringAction::SetPriorityLane,
        true,
        QueueSteeringReasonCode::PriorityLaneSelected,
        Some(normalized_lane),
    )
}

/// Computes depth, age, and fairness metrics over the queued inputs.
///
/// When `coalescing_group` is given, only records in that group are counted;
/// `None` counts everything (used for whole-session views).
#[must_use]
pub(crate) fn session_queue_metrics(
    queued_inputs: &[OrchestratorQueuedInputRecord],
    coalescing_group: Option<&str>,
    observed_at_unix_ms: i64,
) -> SessionQueueMetrics {
    let mut pending_created_at = Vec::new();
    let mut terminal_count = 0usize;
    let mut merged_count = 0usize;
    let mut overflowed_count = 0usize;
    let mut operator_priority_pending = 0usize;
    let mut profile_counts = SessionQueueProfileCounts::default();
    let mut total_count = 0usize;

    for queued in queued_inputs.iter().filter(|queued| {
        coalescing_group.is_none_or(|group| queued.coalescing_group.as_deref() == Some(group))
    }) {
        total_count += 1;
        let profile = queue_profile_for_input(queued);
        profile_counts.observe(profile);
        if QueuedInputState::parse(queued.state.as_str()).is_some_and(QueuedInputState::is_active) {
            pending_created_at.push(queued.created_at_unix_ms);
            if profile == SessionQueueProfile::OperatorPriority {
                operator_priority_pending += 1;
            }
        } else {
            terminal_count += 1;
        }
        match queued.state.as_str() {
            "merged" => merged_count += 1,
            "overflowed" => overflowed_count += 1,
            _ => {}
        }
    }

    let pending_depth = pending_created_at.len();
    let oldest_pending_age_ms = pending_created_at
        .iter()
        .min()
        .map(|created_at| queue_age_ms(observed_at_unix_ms, *created_at));
    let newest_pending_age_ms = pending_created_at
        .iter()
        .max()
        .map(|created_at| queue_age_ms(observed_at_unix_ms, *created_at));

    SessionQueueMetrics {
        pending_depth,
        terminal_count,
        total_count,
        oldest_pending_age_ms,
        newest_pending_age_ms,
        merge_candidate_count: pending_depth.saturating_sub(1),
        merged_count,
        overflowed_count,
        operator_priority_pending,
        profile_counts,
    }
}

/// Classifies a queued input into its fairness profile.
///
/// Precedence: an operator priority lane beats a `routine:` coalescing
/// group, which beats the queue-mode heuristic, so an operator escalation
/// is never misfiled into a background lane.
#[must_use]
pub(crate) fn queue_profile_for_input(
    queued: &OrchestratorQueuedInputRecord,
) -> SessionQueueProfile {
    let priority_lane = queued.priority_lane.trim().to_ascii_lowercase();
    if matches!(priority_lane.as_str(), "operator" | "operator_priority" | "operator-priority") {
        return SessionQueueProfile::OperatorPriority;
    }
    if queued.coalescing_group.as_deref().is_some_and(|group| group.starts_with("routine:")) {
        return SessionQueueProfile::Routine;
    }
    match queued.queue_mode.as_str() {
        "collect" | "steer_backlog" => SessionQueueProfile::Background,
        _ => SessionQueueProfile::Interactive,
    }
}

/// Counts pending inputs, optionally scoped to one coalescing group.
#[must_use]
pub(crate) fn pending_queue_depth(
    queued_inputs: &[OrchestratorQueuedInputRecord],
    coalescing_group: Option<&str>,
) -> usize {
    queued_inputs
        .iter()
        .filter(|queued| {
            QueuedInputState::parse(queued.state.as_str()).is_some_and(QueuedInputState::is_active)
                && coalescing_group
                    .is_none_or(|group| queued.coalescing_group.as_deref() == Some(group))
        })
        .count()
}

// Truncates by char (not byte) count so multi-byte input cannot be split
// mid-character.
#[must_use]
fn truncate_for_summary(value: &str, limit: usize) -> String {
    let trimmed = value.trim();
    let mut output = trimmed.chars().take(limit).collect::<String>();
    if trimmed.chars().count() > limit {
        output.push_str("...");
    }
    output
}

// Precedence is deliberate: paused > backpressured > waiting-on-approval >
// idle > collecting. A paused or saturated queue must surface before the
// softer "busy" states so the operator sees the action that unblocks it.
fn derive_session_busy_state(
    metrics: &SessionQueueMetrics,
    policy: &SessionQueuePolicy,
    safe_boundary: &SessionQueueSafeBoundary,
    paused: bool,
) -> SessionBusyState {
    if paused {
        return SessionBusyState::Paused;
    }
    if metrics.pending_depth >= policy.cap {
        return SessionBusyState::Backpressured;
    }
    if safe_boundary.pending_approval {
        return SessionBusyState::WaitingOnApproval;
    }
    if !safe_boundary.active_run_stream {
        return SessionBusyState::Idle;
    }
    if metrics.pending_depth > 0 {
        return SessionBusyState::BusyCollecting;
    }
    SessionBusyState::BusyAcceptsFollowups
}

fn queue_age_ms(observed_at_unix_ms: i64, created_at_unix_ms: i64) -> u64 {
    // Clock skew can place created_at after observed_at; clamp to zero so
    // the age never wraps into a huge unsigned value.
    observed_at_unix_ms.saturating_sub(created_at_unix_ms).max(0) as u64
}

fn queue_steering_decision(
    queued: &OrchestratorQueuedInputRecord,
    action: QueueSteeringAction,
    accepted: bool,
    reason_code: QueueSteeringReasonCode,
    to_priority_lane: Option<String>,
) -> QueueSteeringDecision {
    let terminal_event_type =
        if accepted { QUEUE_STEERING_EVENT_COMPLETED } else { QUEUE_STEERING_EVENT_FAILED };
    let reason_code = reason_code.as_str().to_owned();
    let payload_json = json!({
        "schema_version": QUEUE_STEERING_SCHEMA_VERSION,
        "queued_input_id": queued.queued_input_id.as_str(),
        "session_id": queued.session_id.as_str(),
        "run_id": queued.run_id.as_str(),
        "state": queued.state.as_str(),
        "action": action.as_str(),
        "accepted": accepted,
        "reason_code": reason_code,
        "from_priority_lane": queued.priority_lane.as_str(),
        "to_priority_lane": to_priority_lane.as_deref(),
        "queue_mode": queued.queue_mode.as_str(),
        "coalescing_group": queued.coalescing_group.as_deref(),
    })
    .to_string();
    let evidence_refs_json = json!([{
        "kind": "queued_input",
        "queued_input_id": queued.queued_input_id.as_str(),
        "session_id": queued.session_id.as_str(),
        "run_id": queued.run_id.as_str(),
        "created_at_unix_ms": queued.created_at_unix_ms,
    }])
    .to_string();
    QueueSteeringDecision {
        schema_version: QUEUE_STEERING_SCHEMA_VERSION,
        action,
        accepted,
        reason_code,
        queued_input_id: queued.queued_input_id.clone(),
        session_id: queued.session_id.clone(),
        run_id: queued.run_id.clone(),
        from_priority_lane: queued.priority_lane.clone(),
        to_priority_lane,
        terminal_event_type: terminal_event_type.to_owned(),
        payload_json,
        evidence_refs_json,
        redaction_level: QUEUE_STEERING_REDACTION_NONE.to_owned(),
    }
}

fn normalize_priority_lane(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.len() > 64 {
        return None;
    }
    trimmed
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | ':' | '.'))
        .then(|| trimmed.to_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use palyra_common::runtime_contracts::{QueueDecision, QueueMode, QueuedInputDeliveryBoundary};

    use crate::config::SessionQueuePolicyConfig;

    use crate::journal::OrchestratorQueuedInputRecord;

    use super::{
        analyze_session_queue, build_queue_collect_summary, decide_queue_steering,
        decide_session_queue_mode, pending_queue_depth, queue_profile_for_input,
        QueueSteeringAction, QueueSteeringReasonCode, QueueSteeringRequest, SessionBusyState,
        SessionQueuePolicy, SessionQueueProfile, SessionQueueSafeBoundary,
        QUEUE_STEERING_EVENT_COMPLETED, QUEUE_STEERING_EVENT_FAILED, QUEUE_STEERING_REDACTION_NONE,
        QUEUE_STEERING_SCHEMA_VERSION,
    };

    #[test]
    fn policy_maps_legacy_depth_and_merge_window_to_cap_and_debounce() {
        let config = SessionQueuePolicyConfig {
            max_depth: 12,
            merge_window_ms: 2_500,
            ..SessionQueuePolicyConfig::default()
        };

        let policy =
            SessionQueuePolicy::from_config(&config, "session-1", Some("discord"), Some("agent-1"));

        assert_eq!(policy.cap, 12);
        assert_eq!(policy.debounce_ms, 2_500);
        assert_eq!(policy.priority_lane, "normal");
        assert_eq!(policy.drop_policy, "summarize_oldest");
        assert_eq!(policy.overflow_behavior, "deterministic_backlog_summary");
        assert_eq!(policy.coalescing_group, "agent:agent-1:session-1");
    }

    #[test]
    fn pending_approval_defers_steer_into_collect() {
        let policy = SessionQueuePolicy::from_config(
            &SessionQueuePolicyConfig::default(),
            "session-1",
            None,
            None,
        );
        let decision = decide_session_queue_mode(
            policy,
            Some(QueueMode::Steer),
            SessionQueueSafeBoundary::active(true, true),
            0,
        );

        assert_eq!(decision.decision, QueueDecision::Defer);
        assert_eq!(decision.mode, QueueMode::Collect);
        assert_eq!(decision.reason, "steer_deferred_until_safe_boundary");
        assert!(decision.safe_boundary.pending_approval);
    }

    #[test]
    fn queue_modes_map_to_exact_boundaries_across_active_run_states() {
        let safe = SessionQueueSafeBoundary::active(true, false);
        let approval = SessionQueueSafeBoundary::active(true, true);
        let mut tool = safe.clone();
        tool.sensitive_tool_execution = true;
        let mut delivery = safe.clone();
        delivery.delivery_in_progress = true;
        let active_states =
            [("safe", safe), ("approval", approval), ("tool", tool), ("delivery", delivery)];
        let modes = [
            QueueMode::Followup,
            QueueMode::Collect,
            QueueMode::SteerBacklog,
            QueueMode::Steer,
            QueueMode::Interrupt,
        ];

        for (active_state, boundary) in active_states {
            let permits_control = active_state == "safe";
            for mode in modes {
                let decision = decide_session_queue_mode(
                    SessionQueuePolicy::from_config(
                        &SessionQueuePolicyConfig::default(),
                        "session-1",
                        None,
                        None,
                    ),
                    Some(mode),
                    boundary.clone(),
                    0,
                );
                let (expected_decision, expected_mode, expected_boundary) = match mode {
                    QueueMode::Followup => (
                        QueueDecision::Enqueue,
                        QueueMode::Followup,
                        QueuedInputDeliveryBoundary::NextTurn,
                    ),
                    QueueMode::Collect => (
                        QueueDecision::Enqueue,
                        QueueMode::Collect,
                        QueuedInputDeliveryBoundary::BacklogSummary,
                    ),
                    QueueMode::SteerBacklog => (
                        QueueDecision::SteerBacklog,
                        QueueMode::SteerBacklog,
                        QueuedInputDeliveryBoundary::BacklogSummary,
                    ),
                    QueueMode::Steer if permits_control => (
                        QueueDecision::Steer,
                        QueueMode::Steer,
                        QueuedInputDeliveryBoundary::CurrentRunBeforeProvider,
                    ),
                    QueueMode::Interrupt if permits_control => (
                        QueueDecision::Interrupt,
                        QueueMode::Interrupt,
                        QueuedInputDeliveryBoundary::CancelThenNextTurn,
                    ),
                    QueueMode::Steer | QueueMode::Interrupt => (
                        QueueDecision::Defer,
                        QueueMode::Collect,
                        QueuedInputDeliveryBoundary::BacklogSummary,
                    ),
                };

                assert_eq!(
                    (decision.decision, decision.mode, decision.delivery_boundary),
                    (expected_decision, expected_mode, expected_boundary),
                    "unexpected delivery contract for {mode:?} at {active_state}"
                );
                assert_eq!(decision.requested_mode, mode);
            }
        }
    }

    #[test]
    fn queue_cap_switches_to_overflow_summary() {
        let config =
            SessionQueuePolicyConfig { max_depth: 2, ..SessionQueuePolicyConfig::default() };
        let policy = SessionQueuePolicy::from_config(&config, "session-1", None, None);
        let decision = decide_session_queue_mode(
            policy,
            Some(QueueMode::Followup),
            SessionQueueSafeBoundary::active(true, false),
            2,
        );

        assert_eq!(decision.decision, QueueDecision::Overflow);
        assert_eq!(decision.mode, QueueMode::Collect);
        assert!(!decision.accepted, "overflow must not be accepted into the pending queue");
        assert_eq!(decision.reason, "queue_cap_reached_overflow_summary_required");
    }

    #[test]
    fn collect_summary_preserves_provenance_and_bounds_items() {
        let records = (0..14)
            .map(|index| OrchestratorQueuedInputRecord {
                queued_input_id: format!("queued-{index}"),
                run_id: "run-1".to_owned(),
                session_id: "session-1".to_owned(),
                state: "pending".to_owned(),
                queue_mode: "collect".to_owned(),
                delivery_boundary: "backlog_summary".to_owned(),
                expected_active_generation: Some(1),
                claimed_active_generation: None,
                lifecycle_revision: 0,
                priority_lane: "normal".to_owned(),
                coalescing_group: Some("group-1".to_owned()),
                overflow_summary_ref: None,
                safe_boundary_flags_json: "{}".to_owned(),
                decision_reason: "collect_requested".to_owned(),
                text: format!("queued input text {index}"),
                attachments_json: format!(r#"[{{"artifact_id":"artifact-{index}"}}]"#),
                queue_outcome_json: "{}".to_owned(),
                accepted_at_unix_ms: Some(index),
                coalesced_at_unix_ms: None,
                forwarded_at_unix_ms: None,
                terminal_at_unix_ms: None,
                policy_snapshot_json: "{}".to_owned(),
                explain_json: "{}".to_owned(),
                created_at_unix_ms: index,
                updated_at_unix_ms: index,
                origin_run_id: None,
            })
            .collect::<Vec<_>>();

        let summary =
            build_queue_collect_summary("summary-1".to_owned(), records.as_slice(), "forced");

        assert_eq!(summary.source_count, 14);
        assert!(summary.text.contains("Collected 14 queued input"));
        assert_eq!(summary.provenance_json["omitted_count"], 2);
        assert_eq!(
            summary.provenance_json["source_queued_input_ids"].as_array().unwrap().len(),
            14
        );
        assert_eq!(summary.provenance_json["sources"].as_array().unwrap().len(), 12);
        assert_eq!(summary.attachment_refs_json.as_array().unwrap().len(), 14);
        assert_eq!(pending_queue_depth(records.as_slice(), Some("group-1")), 14);
    }

    #[test]
    fn queue_analysis_reports_busy_state_and_pending_ages() {
        let policy = SessionQueuePolicy::from_config(
            &SessionQueuePolicyConfig::default(),
            "session-1",
            None,
            None,
        );
        let records = vec![
            OrchestratorQueuedInputRecord {
                queued_input_id: "queued-1".to_owned(),
                run_id: "run-1".to_owned(),
                session_id: "session-1".to_owned(),
                state: "pending".to_owned(),
                queue_mode: "collect".to_owned(),
                delivery_boundary: "backlog_summary".to_owned(),
                expected_active_generation: Some(1),
                claimed_active_generation: None,
                lifecycle_revision: 0,
                priority_lane: "normal".to_owned(),
                coalescing_group: Some(policy.coalescing_group.clone()),
                overflow_summary_ref: None,
                safe_boundary_flags_json: "{}".to_owned(),
                decision_reason: "collect_requested".to_owned(),
                text: "old".to_owned(),
                attachments_json: "[]".to_owned(),
                queue_outcome_json: "{}".to_owned(),
                accepted_at_unix_ms: Some(100),
                coalesced_at_unix_ms: None,
                forwarded_at_unix_ms: None,
                terminal_at_unix_ms: None,
                policy_snapshot_json: "{}".to_owned(),
                explain_json: "{}".to_owned(),
                created_at_unix_ms: 100,
                updated_at_unix_ms: 100,
                origin_run_id: None,
            },
            OrchestratorQueuedInputRecord {
                queued_input_id: "queued-2".to_owned(),
                run_id: "run-1".to_owned(),
                session_id: "session-1".to_owned(),
                state: "pending".to_owned(),
                queue_mode: "followup".to_owned(),
                delivery_boundary: "next_turn".to_owned(),
                expected_active_generation: Some(1),
                claimed_active_generation: None,
                lifecycle_revision: 0,
                priority_lane: "operator_priority".to_owned(),
                coalescing_group: Some(policy.coalescing_group.clone()),
                overflow_summary_ref: None,
                safe_boundary_flags_json: "{}".to_owned(),
                decision_reason: "operator_prioritized".to_owned(),
                text: "new".to_owned(),
                attachments_json: "[]".to_owned(),
                queue_outcome_json: "{}".to_owned(),
                accepted_at_unix_ms: Some(250),
                coalesced_at_unix_ms: None,
                forwarded_at_unix_ms: None,
                terminal_at_unix_ms: None,
                policy_snapshot_json: "{}".to_owned(),
                explain_json: "{}".to_owned(),
                created_at_unix_ms: 250,
                updated_at_unix_ms: 250,
                origin_run_id: None,
            },
        ];

        let analysis = analyze_session_queue(
            records.as_slice(),
            &policy,
            &SessionQueueSafeBoundary::active(true, false),
            false,
            400,
        );

        assert_eq!(analysis.busy_state, SessionBusyState::BusyCollecting);
        assert_eq!(analysis.metrics.pending_depth, 2);
        assert_eq!(analysis.metrics.merge_candidate_count, 1);
        assert_eq!(analysis.metrics.oldest_pending_age_ms, Some(300));
        assert_eq!(analysis.metrics.newest_pending_age_ms, Some(150));
        assert_eq!(analysis.metrics.operator_priority_pending, 1);
        assert_eq!(queue_profile_for_input(&records[1]), SessionQueueProfile::OperatorPriority);
    }

    #[test]
    fn queue_steering_moves_pending_input_to_normalized_lane() {
        let queued = queued_input_fixture("pending", "normal");
        let decision = decide_queue_steering(
            &queued,
            &QueueSteeringRequest {
                actor_principal: "user:ops".to_owned(),
                requested_priority_lane: "Operator_Priority".to_owned(),
                reason: Some("operator escalation".to_owned()),
            },
        );

        assert_eq!(decision.schema_version, QUEUE_STEERING_SCHEMA_VERSION);
        assert_eq!(decision.action, QueueSteeringAction::SetPriorityLane);
        assert!(decision.accepted);
        assert_eq!(decision.reason_code, QueueSteeringReasonCode::PriorityLaneSelected.as_str());
        assert_eq!(decision.from_priority_lane, "normal");
        assert_eq!(decision.to_priority_lane.as_deref(), Some("operator_priority"));
        assert_eq!(decision.terminal_event_type, QUEUE_STEERING_EVENT_COMPLETED);
        assert_eq!(decision.redaction_level, QUEUE_STEERING_REDACTION_NONE);

        let payload: serde_json::Value =
            serde_json::from_str(decision.payload_json.as_str()).expect("payload should be JSON");
        assert_eq!(payload["action"], QueueSteeringAction::SetPriorityLane.as_str());
        assert_eq!(payload["to_priority_lane"], "operator_priority");

        let roundtrip: super::QueueSteeringDecision =
            serde_json::from_str(&serde_json::to_string(&decision).expect("serializes"))
                .expect("deserializes");
        assert_eq!(roundtrip, decision);
    }

    #[test]
    fn queue_steering_rejects_non_pending_input_without_lane_change() {
        let queued = queued_input_fixture("forwarded", "normal");
        let decision = decide_queue_steering(
            &queued,
            &QueueSteeringRequest {
                actor_principal: "user:ops".to_owned(),
                requested_priority_lane: "operator_priority".to_owned(),
                reason: None,
            },
        );

        assert_eq!(decision.action, QueueSteeringAction::Reject);
        assert!(!decision.accepted);
        assert_eq!(decision.reason_code, QueueSteeringReasonCode::NonPendingInput.as_str());
        assert_eq!(decision.to_priority_lane.as_deref(), Some("operator_priority"));
        assert_eq!(decision.terminal_event_type, QUEUE_STEERING_EVENT_FAILED);
    }

    #[test]
    fn queue_steering_noops_when_input_is_already_in_requested_lane() {
        let queued = queued_input_fixture("pending", "operator_priority");
        let decision = decide_queue_steering(
            &queued,
            &QueueSteeringRequest {
                actor_principal: "user:ops".to_owned(),
                requested_priority_lane: "operator_priority".to_owned(),
                reason: None,
            },
        );

        assert_eq!(decision.action, QueueSteeringAction::Noop);
        assert!(decision.accepted);
        assert_eq!(decision.reason_code, QueueSteeringReasonCode::AlreadyInRequestedLane.as_str());
        assert_eq!(decision.to_priority_lane.as_deref(), Some("operator_priority"));
        assert_eq!(decision.terminal_event_type, QUEUE_STEERING_EVENT_COMPLETED);
    }

    #[test]
    fn queue_steering_rejects_missing_actor_and_invalid_lane() {
        let queued = queued_input_fixture("pending", "normal");
        let missing_actor = decide_queue_steering(
            &queued,
            &QueueSteeringRequest {
                actor_principal: " ".to_owned(),
                requested_priority_lane: "operator_priority".to_owned(),
                reason: None,
            },
        );
        assert_eq!(missing_actor.action, QueueSteeringAction::Reject);
        assert_eq!(missing_actor.reason_code, QueueSteeringReasonCode::MissingActor.as_str());

        let invalid_lane = decide_queue_steering(
            &queued,
            &QueueSteeringRequest {
                actor_principal: "user:ops".to_owned(),
                requested_priority_lane: "bad lane".to_owned(),
                reason: None,
            },
        );
        assert_eq!(invalid_lane.action, QueueSteeringAction::Reject);
        assert_eq!(invalid_lane.reason_code, QueueSteeringReasonCode::InvalidPriorityLane.as_str());
        assert_eq!(invalid_lane.terminal_event_type, QUEUE_STEERING_EVENT_FAILED);
    }

    fn queued_input_fixture(state: &str, priority_lane: &str) -> OrchestratorQueuedInputRecord {
        OrchestratorQueuedInputRecord {
            queued_input_id: "queued-1".to_owned(),
            run_id: "run-1".to_owned(),
            session_id: "session-1".to_owned(),
            state: state.to_owned(),
            queue_mode: "followup".to_owned(),
            delivery_boundary: "next_turn".to_owned(),
            expected_active_generation: Some(1),
            claimed_active_generation: None,
            lifecycle_revision: 0,
            priority_lane: priority_lane.to_owned(),
            coalescing_group: Some("session:session-1".to_owned()),
            overflow_summary_ref: None,
            safe_boundary_flags_json: "{}".to_owned(),
            decision_reason: "followup_requested".to_owned(),
            text: "queued input".to_owned(),
            attachments_json: "[]".to_owned(),
            queue_outcome_json: "{}".to_owned(),
            accepted_at_unix_ms: Some(100),
            coalesced_at_unix_ms: None,
            forwarded_at_unix_ms: None,
            terminal_at_unix_ms: None,
            policy_snapshot_json: "{}".to_owned(),
            explain_json: "{}".to_owned(),
            created_at_unix_ms: 100,
            updated_at_unix_ms: 100,
            origin_run_id: None,
        }
    }
}
