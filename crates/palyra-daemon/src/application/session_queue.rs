use palyra_common::runtime_contracts::{QueueDecision, QueueMode};
use serde::Serialize;
use serde_json::{json, Value};

use crate::config::SessionQueuePolicyConfig;
use crate::journal::OrchestratorQueuedInputRecord;

pub(crate) const SESSION_QUEUE_POLICY_ID: &str = "session_queue.v1";
const DEFAULT_PRIORITY_LANE: &str = "normal";
const DEFAULT_DROP_POLICY: &str = "summarize_oldest";
const DEFAULT_OVERFLOW_BEHAVIOR: &str = "deterministic_backlog_summary";
const COLLECT_SUMMARY_MAX_ITEMS: usize = 12;
const COLLECT_SUMMARY_TEXT_LIMIT: usize = 240;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct SessionQueuePolicy {
    pub(crate) policy_id: String,
    pub(crate) mode: QueueMode,
    pub(crate) priority_lane: String,
    pub(crate) debounce_ms: u64,
    pub(crate) cap: usize,
    pub(crate) drop_policy: String,
    pub(crate) overflow_behavior: String,
    pub(crate) coalescing_group: String,
    pub(crate) source: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct SessionQueueSafeBoundary {
    pub(crate) active_run_stream: bool,
    pub(crate) pending_approval: bool,
    pub(crate) sensitive_tool_execution: bool,
    pub(crate) before_model_round: bool,
    pub(crate) after_model_round: bool,
    pub(crate) after_tool_result: bool,
    pub(crate) after_approval_wait: bool,
    pub(crate) after_child_merge: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct SessionQueueDecision {
    pub(crate) decision: QueueDecision,
    pub(crate) mode: QueueMode,
    pub(crate) accepted: bool,
    pub(crate) reason: String,
    pub(crate) safe_boundary: SessionQueueSafeBoundary,
    pub(crate) policy: SessionQueuePolicy,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct QueueCollectSummary {
    pub(crate) summary_ref: String,
    pub(crate) text: String,
    pub(crate) source_count: usize,
    pub(crate) provenance_json: Value,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SessionQueueProfile {
    Interactive,
    Background,
    Routine,
    OperatorPriority,
}

impl SessionQueueProfile {
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

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct SessionQueueMetrics {
    pub(crate) pending_depth: usize,
    pub(crate) terminal_count: usize,
    pub(crate) total_count: usize,
    pub(crate) oldest_pending_age_ms: Option<u64>,
    pub(crate) newest_pending_age_ms: Option<u64>,
    pub(crate) merge_candidate_count: usize,
    pub(crate) merged_count: usize,
    pub(crate) overflowed_count: usize,
    pub(crate) operator_priority_pending: usize,
    pub(crate) profile_counts: SessionQueueProfileCounts,
}

impl SessionQueueMetrics {
    #[must_use]
    pub(crate) fn snapshot_json(&self) -> Value {
        json!(self)
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct SessionQueueAnalysis {
    pub(crate) busy_state: SessionBusyState,
    pub(crate) recommendation: String,
    pub(crate) metrics: SessionQueueMetrics,
}

impl SessionQueueAnalysis {
    #[must_use]
    pub(crate) fn snapshot_json(&self) -> Value {
        json!({
            "busy_state": self.busy_state.as_str(),
            "recommendation": self.recommendation,
            "metrics": self.metrics.snapshot_json(),
        })
    }
}

impl SessionQueuePolicy {
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
    #[must_use]
    pub(crate) fn active(active_run_stream: bool, pending_approval: bool) -> Self {
        Self {
            active_run_stream,
            pending_approval,
            sensitive_tool_execution: false,
            before_model_round: active_run_stream && !pending_approval,
            after_model_round: false,
            after_tool_result: false,
            after_approval_wait: pending_approval,
            after_child_merge: false,
        }
    }

    #[must_use]
    pub(crate) const fn can_steer(&self) -> bool {
        self.active_run_stream
            && !self.pending_approval
            && !self.sensitive_tool_execution
            && (self.before_model_round
                || self.after_model_round
                || self.after_tool_result
                || self.after_child_merge)
    }

    #[must_use]
    pub(crate) const fn can_interrupt(&self) -> bool {
        self.active_run_stream && !self.pending_approval && !self.sensitive_tool_execution
    }
}

impl SessionQueueDecision {
    #[must_use]
    pub(crate) fn explain_json(&self) -> Value {
        json!({
            "decision": self.decision.as_str(),
            "mode": self.mode.as_str(),
            "accepted": self.accepted,
            "reason": self.reason,
            "safe_boundary": self.safe_boundary,
            "policy": self.policy.snapshot_json(),
        })
    }
}

#[must_use]
pub(crate) fn decide_session_queue_mode(
    policy: SessionQueuePolicy,
    requested_mode: Option<QueueMode>,
    safe_boundary: SessionQueueSafeBoundary,
    current_depth: usize,
) -> SessionQueueDecision {
    let requested_mode = requested_mode.unwrap_or(policy.mode);
    if current_depth >= policy.cap {
        return SessionQueueDecision {
            decision: QueueDecision::Overflow,
            mode: QueueMode::Collect,
            accepted: true,
            reason: "queue_cap_reached_overflow_summary_required".to_owned(),
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
        mode,
        accepted: true,
        reason: reason.to_owned(),
        safe_boundary,
        policy,
    }
}

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
        provenance_json: json!({
            "summary_ref": summary_ref,
            "reason": reason,
            "source_count": source_count,
            "omitted_count": omitted_count,
            "source_queued_input_ids": source_ids,
            "sources": rendered_items.collect::<Vec<_>>(),
        }),
    }
}

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
        if queued.state == "pending" {
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

#[must_use]
pub(crate) fn pending_queue_depth(
    queued_inputs: &[OrchestratorQueuedInputRecord],
    coalescing_group: Option<&str>,
) -> usize {
    queued_inputs
        .iter()
        .filter(|queued| {
            queued.state == "pending"
                && match coalescing_group {
                    Some(group) => queued.coalescing_group.as_deref() == Some(group),
                    None => true,
                }
        })
        .count()
}

#[must_use]
fn truncate_for_summary(value: &str, limit: usize) -> String {
    let trimmed = value.trim();
    let mut output = String::with_capacity(limit.min(trimmed.len()));
    for character in trimmed.chars().take(limit) {
        output.push(character);
    }
    if trimmed.chars().count() > limit {
        output.push_str("...");
    }
    output
}

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
    observed_at_unix_ms.saturating_sub(created_at_unix_ms).max(0) as u64
}

#[cfg(test)]
mod tests {
    use palyra_common::runtime_contracts::{QueueDecision, QueueMode};

    use crate::config::SessionQueuePolicyConfig;

    use crate::journal::OrchestratorQueuedInputRecord;

    use super::{
        analyze_session_queue, build_queue_collect_summary, decide_session_queue_mode,
        pending_queue_depth, queue_profile_for_input, SessionBusyState, SessionQueuePolicy,
        SessionQueueProfile, SessionQueueSafeBoundary,
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
                priority_lane: "normal".to_owned(),
                coalescing_group: Some("group-1".to_owned()),
                overflow_summary_ref: None,
                safe_boundary_flags_json: "{}".to_owned(),
                decision_reason: "collect_requested".to_owned(),
                text: format!("queued input text {index}"),
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
                priority_lane: "normal".to_owned(),
                coalescing_group: Some(policy.coalescing_group.clone()),
                overflow_summary_ref: None,
                safe_boundary_flags_json: "{}".to_owned(),
                decision_reason: "collect_requested".to_owned(),
                text: "old".to_owned(),
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
                priority_lane: "operator_priority".to_owned(),
                coalescing_group: Some(policy.coalescing_group.clone()),
                overflow_summary_ref: None,
                safe_boundary_flags_json: "{}".to_owned(),
                decision_reason: "operator_prioritized".to_owned(),
                text: "new".to_owned(),
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
}
