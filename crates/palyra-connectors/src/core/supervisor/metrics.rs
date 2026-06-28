//! Runtime metrics derived from the persisted connector event log, plus the
//! queue saturation classifier.
//!
//! Metrics are reconstructed on demand by replaying the most recent events
//! rather than kept as in-memory counters, so they survive restarts and need
//! no extra synchronization; the window size bounds the cost per snapshot.

use std::collections::HashMap;

use serde::Serialize;
use serde_json::Value;

use super::super::storage::{ConnectorEventRecord, ConnectorQueueSnapshot};
use super::{ConnectorSupervisor, ConnectorSupervisorError};

/// Number of most-recent events replayed per metrics snapshot.
pub(super) const CONNECTOR_METRICS_EVENT_WINDOW: usize = 2_048;
const CONNECTOR_POLICY_DENIAL_REASON_LIMIT: usize = 16;
/// Due-entry backlog at which the queue is reported saturated.
const SATURATED_DUE_OUTBOX_THRESHOLD: u64 = 8;

/// Aggregate routing latency over the sampled event window.
#[derive(Debug, Clone, Default, Serialize)]
pub(super) struct RouteMessageLatencySnapshot {
    sample_count: u64,
    avg_ms: u64,
    max_ms: u64,
}

/// One policy denial reason with its occurrence count.
#[derive(Debug, Clone, Serialize)]
pub(super) struct PolicyDenialReasonCount {
    reason: String,
    count: u64,
}

/// Event-derived counters exposed in runtime snapshots.
#[derive(Debug, Clone, Default, Serialize)]
pub(super) struct ConnectorRuntimeMetricsSnapshot {
    event_window_size: u64,
    inbound_events_processed: u64,
    inbound_dedupe_hits: u64,
    outbound_sends_ok: u64,
    outbound_sends_retry: u64,
    outbound_sends_dead_letter: u64,
    route_message_latency_ms: RouteMessageLatencySnapshot,
    policy_denials: Vec<PolicyDenialReasonCount>,
}

/// Coarse queue-health classification with the contributing reasons.
#[derive(Debug, Clone, Serialize)]
pub(super) struct ConnectorSaturationSnapshot {
    state: &'static str,
    reasons: Vec<String>,
}

impl ConnectorSupervisor {
    /// Builds the metrics snapshot by replaying the connector's recent events.
    pub(super) fn build_runtime_metrics(
        &self,
        connector_id: &str,
    ) -> Result<ConnectorRuntimeMetricsSnapshot, ConnectorSupervisorError> {
        let events = self.store.list_events(connector_id, CONNECTOR_METRICS_EVENT_WINDOW)?;
        let mut metrics = ConnectorRuntimeMetricsSnapshot {
            event_window_size: u64::try_from(events.len()).unwrap_or(u64::MAX),
            ..ConnectorRuntimeMetricsSnapshot::default()
        };
        let mut route_latency_total_ms = 0_u128;
        let mut denial_counts: HashMap<String, u64> = HashMap::new();
        for event in events {
            self.accumulate_event_metrics(
                &event,
                &mut metrics,
                &mut route_latency_total_ms,
                &mut denial_counts,
            );
        }
        if metrics.route_message_latency_ms.sample_count > 0 {
            metrics.route_message_latency_ms.avg_ms = u64::try_from(
                route_latency_total_ms
                    / u128::from(metrics.route_message_latency_ms.sample_count.max(1)),
            )
            .unwrap_or(u64::MAX);
        }
        let mut denials = denial_counts
            .into_iter()
            .map(|(reason, count)| PolicyDenialReasonCount { reason, count })
            .collect::<Vec<_>>();
        denials.sort_by(|left, right| {
            right.count.cmp(&left.count).then_with(|| left.reason.cmp(&right.reason))
        });
        denials.truncate(CONNECTOR_POLICY_DENIAL_REASON_LIMIT);
        metrics.policy_denials = denials;
        Ok(metrics)
    }

    fn accumulate_event_metrics(
        &self,
        event: &ConnectorEventRecord,
        metrics: &mut ConnectorRuntimeMetricsSnapshot,
        route_latency_total_ms: &mut u128,
        denial_counts: &mut HashMap<String, u64>,
    ) {
        match event.event_type.as_str() {
            "inbound.received" | "inbound.duplicate" | "inbound.rejected" => {
                metrics.inbound_events_processed =
                    metrics.inbound_events_processed.saturating_add(1);
            }
            "outbox.delivered" => {
                metrics.outbound_sends_ok = metrics.outbound_sends_ok.saturating_add(1);
            }
            "outbox.retry" => {
                metrics.outbound_sends_retry = metrics.outbound_sends_retry.saturating_add(1);
            }
            "outbox.dead_letter" => {
                metrics.outbound_sends_dead_letter =
                    metrics.outbound_sends_dead_letter.saturating_add(1);
            }
            _ => {}
        }
        if event.event_type == "inbound.duplicate" {
            metrics.inbound_dedupe_hits = metrics.inbound_dedupe_hits.saturating_add(1);
        }
        if matches!(event.event_type.as_str(), "inbound.routed" | "inbound.not_routed") {
            if let Some(latency_ms) =
                event.details.as_ref().and_then(parse_route_message_latency_ms)
            {
                metrics.route_message_latency_ms.sample_count =
                    metrics.route_message_latency_ms.sample_count.saturating_add(1);
                metrics.route_message_latency_ms.max_ms =
                    metrics.route_message_latency_ms.max_ms.max(latency_ms);
                *route_latency_total_ms =
                    route_latency_total_ms.saturating_add(u128::from(latency_ms));
            }
        }
        if event.event_type == "inbound.not_routed"
            && !event
                .details
                .as_ref()
                .and_then(|details| details.get("queued_for_retry"))
                .and_then(Value::as_bool)
                .unwrap_or(false)
            && is_policy_denial_reason(event.message.as_str())
        {
            let reason = event.message.trim().to_owned();
            *denial_counts.entry(reason).or_insert(0) += 1;
        }
    }
}

fn parse_route_message_latency_ms(details: &Value) -> Option<u64> {
    details.get("route_message_latency_ms").and_then(Value::as_u64)
}

/// Backpressure rejections are capacity signals, not policy denials, so they
/// are excluded from the denial-reason breakdown.
fn is_policy_denial_reason(reason: &str) -> bool {
    !matches!(
        reason.trim(),
        "backpressure_queue_full"
            | "backpressure_retry_enqueue_failed"
            | "backpressure_poison_quarantine"
    )
}

/// Classifies queue health: paused beats everything; dead letters, claimed
/// (in-flight) entries, or a due backlog count as saturated; any other
/// pending work is merely backlogged.
pub(super) fn build_saturation_snapshot(
    queue: &ConnectorQueueSnapshot,
) -> ConnectorSaturationSnapshot {
    let mut reasons = Vec::new();
    if queue.paused {
        reasons.push("queue_paused".to_owned());
    }
    if queue.due_ingress > 0 {
        reasons.push(format!("due_ingress={}", queue.due_ingress));
    }
    if queue.claimed_ingress > 0 {
        reasons.push(format!("claimed_ingress={}", queue.claimed_ingress));
    }
    if queue.retrying_ingress > 0 {
        reasons.push(format!("retrying_ingress={}", queue.retrying_ingress));
    }
    if queue.failed_ingress > 0 {
        reasons.push(format!("failed_ingress={}", queue.failed_ingress));
    }
    if queue.quarantined_ingress > 0 {
        reasons.push(format!("quarantined_ingress={}", queue.quarantined_ingress));
    }
    if queue.due_outbox > 0 {
        reasons.push(format!("due_outbox={}", queue.due_outbox));
    }
    if queue.claimed_outbox > 0 {
        reasons.push(format!("claimed_outbox={}", queue.claimed_outbox));
    }
    if queue.dead_letters > 0 {
        reasons.push(format!("dead_letters={}", queue.dead_letters));
    }
    if queue.pending_outbox > queue.due_outbox && queue.pending_outbox > 0 {
        reasons.push(format!("pending_outbox={}", queue.pending_outbox));
    }
    let state = if queue.paused {
        "paused"
    } else if queue.quarantined_ingress > 0 || queue.failed_ingress > 0 {
        "action_required"
    } else if queue.dead_letters > 0
        || queue.claimed_outbox > 0
        || queue.claimed_ingress > 0
        || queue.due_outbox >= SATURATED_DUE_OUTBOX_THRESHOLD
        || queue.due_ingress >= SATURATED_DUE_OUTBOX_THRESHOLD
    {
        "saturated"
    } else if queue.pending_outbox > 0 || queue.due_outbox > 0 || queue.pending_ingress > 0 {
        "backlogged"
    } else {
        "nominal"
    };
    ConnectorSaturationSnapshot { state, reasons }
}
