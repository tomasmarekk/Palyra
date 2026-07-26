//! Pure restart decisions for validated configuration changes.

use serde::{Deserialize, Serialize};

/// Filesystem observation emitted by the native watcher or polling fallback.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ConfigWatchEventKind {
    /// Native filesystem notification matched the active config path.
    NativeEvent,
    /// Polling observed a changed metadata/content fingerprint.
    PollingChange,
    /// The watched file is temporarily absent.
    Missing,
    /// Candidate parsing or validation failed.
    Invalid,
    /// The candidate loaded successfully.
    Validated,
    /// Native watcher creation failed and polling remains authoritative.
    PollingFallback,
    /// A native watcher was recreated after failure.
    WatcherRestarted,
}

impl ConfigWatchEventKind {
    /// Returns the stable journal representation.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::NativeEvent => "native_event",
            Self::PollingChange => "polling_change",
            Self::Missing => "missing",
            Self::Invalid => "invalid",
            Self::Validated => "validated",
            Self::PollingFallback => "polling_fallback",
            Self::WatcherRestarted => "watcher_restarted",
        }
    }
}

/// Redacted versioned config-watcher evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ConfigWatchEventV1 {
    /// Stable event identity.
    pub(crate) event_id: String,
    /// Observation class.
    pub(crate) kind: ConfigWatchEventKind,
    /// Hash of the normalized local source identity.
    pub(crate) source_identity_sha256: String,
    /// Hash of config bytes when available.
    pub(crate) config_sha256: Option<String>,
    /// Stable redacted operational reason.
    pub(crate) reason_code: String,
    /// Native watcher generation, starting at one.
    pub(crate) watcher_generation: u64,
    /// Observation timestamp.
    pub(crate) observed_at_unix_ms: i64,
    /// Contract schema version.
    pub(crate) schema_version: u32,
}

/// Stable result of one idempotent restart request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RestartDecisionKind {
    /// The daemon can enter its coordinated drain immediately.
    ReadyNow,
    /// Existing non-ambiguous runs may finish inside the drain deadline.
    ScheduledAfterDrain,
    /// An active or outcome-unknown mutation blocks automatic restart.
    DeferredByActiveMutation,
    /// A sensitive policy change requires explicit operator review.
    BlockedByManualReview,
    /// An equivalent request already owns the decision.
    Coalesced,
    /// The change requires no process restart.
    Cancelled,
}

impl RestartDecisionKind {
    /// Returns the stable journal and diagnostics representation.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::ReadyNow => "ready_now",
            Self::ScheduledAfterDrain => "scheduled_after_drain",
            Self::DeferredByActiveMutation => "deferred_by_active_mutation",
            Self::BlockedByManualReview => "blocked_by_manual_review",
            Self::Coalesced => "coalesced",
            Self::Cancelled => "cancelled",
        }
    }

    /// Returns whether this decision authorizes the lifecycle controller.
    #[must_use]
    pub(crate) const fn starts_drain(self) -> bool {
        matches!(self, Self::ReadyNow | Self::ScheduledAfterDrain)
    }
}

/// Hash-only request created after a candidate configuration validates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RestartRequest {
    /// Unique request identity.
    pub(crate) request_id: String,
    /// Digest used to collapse equivalent filesystem events.
    pub(crate) coalescing_key: String,
    /// SHA-256 of the candidate config bytes.
    pub(crate) config_sha256: String,
    /// SHA-256 of the normalized source identity, never the local path.
    pub(crate) source_identity_sha256: String,
    /// Previously accepted configuration digest.
    pub(crate) last_known_good_sha256: String,
    /// Number of planner steps that require process restart.
    pub(crate) restart_required_steps: u32,
    /// Number of hot-safe planner steps.
    pub(crate) hot_safe_steps: u32,
    /// Host request timestamp.
    pub(crate) requested_at_unix_ms: i64,
}

/// Runtime facts captured atomically for one decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RestartBlockerSnapshot {
    /// Runs active at decision time.
    pub(crate) active_runs: u64,
    /// Durable side-effect fences whose outcome remains unknown.
    pub(crate) outcome_unknown_mutations: u64,
    /// Reload steps blocked while runs are active.
    pub(crate) blocked_active_steps: u32,
    /// Sensitive reload steps requiring explicit review.
    pub(crate) manual_review_steps: u32,
    /// Lifecycle phase observed at decision time.
    pub(crate) lifecycle_phase: String,
}

/// Durable restart decision and its redacted reason.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RestartDecision {
    /// Original request.
    pub(crate) request: RestartRequest,
    /// Snapshot that determined the outcome.
    pub(crate) blockers: RestartBlockerSnapshot,
    /// Stable outcome.
    pub(crate) kind: RestartDecisionKind,
    /// Stable reason suitable for logs and APIs.
    pub(crate) reason_code: String,
    /// Existing request identity when this request coalesced.
    pub(crate) coalesced_into_request_id: Option<String>,
    /// Decision timestamp.
    pub(crate) decided_at_unix_ms: i64,
}

/// Produces the sole restart decision from planner and runtime evidence.
#[must_use]
pub(crate) fn decide_restart(
    request: RestartRequest,
    blockers: RestartBlockerSnapshot,
    existing_request_id: Option<String>,
    decided_at_unix_ms: i64,
) -> RestartDecision {
    let (kind, reason_code, coalesced_into_request_id) = if let Some(existing_request_id) =
        existing_request_id
    {
        (RestartDecisionKind::Coalesced, "daemon.restart.coalesced", Some(existing_request_id))
    } else if blockers.manual_review_steps > 0 {
        (RestartDecisionKind::BlockedByManualReview, "daemon.restart.manual_review_required", None)
    } else if blockers.outcome_unknown_mutations > 0 || blockers.blocked_active_steps > 0 {
        (
            RestartDecisionKind::DeferredByActiveMutation,
            "daemon.restart.active_mutation_blocked",
            None,
        )
    } else if request.restart_required_steps == 0 {
        (RestartDecisionKind::Cancelled, "daemon.restart.not_required", None)
    } else if blockers.active_runs > 0 {
        (RestartDecisionKind::ScheduledAfterDrain, "daemon.restart.scheduled_after_drain", None)
    } else {
        (RestartDecisionKind::ReadyNow, "daemon.restart.ready", None)
    };
    RestartDecision {
        request,
        blockers,
        kind,
        reason_code: reason_code.to_owned(),
        coalesced_into_request_id,
        decided_at_unix_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(restart_required_steps: u32) -> RestartRequest {
        RestartRequest {
            request_id: "01TESTREQUEST".to_owned(),
            coalescing_key: "a".repeat(64),
            config_sha256: "b".repeat(64),
            source_identity_sha256: "c".repeat(64),
            last_known_good_sha256: "d".repeat(64),
            restart_required_steps,
            hot_safe_steps: 0,
            requested_at_unix_ms: 10,
        }
    }

    fn blockers() -> RestartBlockerSnapshot {
        RestartBlockerSnapshot {
            active_runs: 0,
            outcome_unknown_mutations: 0,
            blocked_active_steps: 0,
            manual_review_steps: 0,
            lifecycle_phase: "running".to_owned(),
        }
    }

    #[test]
    fn unknown_mutation_blocks_automatic_restart() {
        let mut blockers = blockers();
        blockers.outcome_unknown_mutations = 1;
        let decision = decide_restart(request(1), blockers, None, 11);
        assert_eq!(decision.kind, RestartDecisionKind::DeferredByActiveMutation);
        assert!(!decision.kind.starts_drain());
    }

    #[test]
    fn active_non_mutating_run_schedules_one_drain() {
        let mut blockers = blockers();
        blockers.active_runs = 1;
        let decision = decide_restart(request(1), blockers, None, 11);
        assert_eq!(decision.kind, RestartDecisionKind::ScheduledAfterDrain);
        assert!(decision.kind.starts_drain());
    }

    #[test]
    fn equivalent_config_change_coalesces_before_other_classification() {
        let mut blockers = blockers();
        blockers.manual_review_steps = 1;
        let decision = decide_restart(request(1), blockers, Some("01EXISTING".to_owned()), 11);
        assert_eq!(decision.kind, RestartDecisionKind::Coalesced);
        assert_eq!(decision.coalesced_into_request_id.as_deref(), Some("01EXISTING"));
    }
}
