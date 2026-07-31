//! Durable concurrency, resource admission, failure-circuit, and cancellation contracts.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Maximum accepted concurrency value for any one policy dimension.
pub(crate) const MAX_WORK_GRAPH_CONCURRENCY: u32 = 1_024;

/// Host-enforced concurrency and retry policy for one graph.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkGraphConcurrencyPolicy {
    pub(crate) max_active_items: u32,
    pub(crate) max_active_per_profile: BTreeMap<String, u32>,
    pub(crate) max_active_per_provider: BTreeMap<String, u32>,
    pub(crate) max_workspace_readers_per_scope: u32,
    pub(crate) failure_limit: u32,
    pub(crate) retry_backoff_base_ms: u64,
    pub(crate) retry_backoff_max_ms: u64,
    pub(crate) cancel_settle_timeout_ms: u64,
}

impl Default for WorkGraphConcurrencyPolicy {
    fn default() -> Self {
        Self {
            max_active_items: 16,
            max_active_per_profile: BTreeMap::new(),
            max_active_per_provider: BTreeMap::new(),
            max_workspace_readers_per_scope: 8,
            // Preserve terminal-on-first-failure behavior unless an owner explicitly enables retry.
            failure_limit: 1,
            retry_backoff_base_ms: 1_000,
            retry_backoff_max_ms: 60_000,
            cancel_settle_timeout_ms: 5_000,
        }
    }
}

/// Durable failure-storm guard projected on each work item.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkItemFailureCircuitState {
    pub(crate) consecutive_failures: u32,
    pub(crate) failure_limit: u32,
    pub(crate) retry_not_before_unix_ms: Option<i64>,
    pub(crate) opened_at_unix_ms: Option<i64>,
    pub(crate) reason_code: Option<String>,
}

/// One active worker authority returned for bounded cancellation fanout.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkGraphCancellationTargetV1 {
    pub(crate) work_item_id: String,
    pub(crate) worker_id: String,
    pub(crate) generation: u64,
    pub(crate) resource_lease_id: Option<String>,
}

/// Atomic host cancellation result and worker fanout plan.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkGraphCancellationPlanV1 {
    pub(crate) graph_id: String,
    pub(crate) graph_revision: u64,
    pub(crate) settle_timeout_ms: u64,
    pub(crate) targets: Vec<WorkGraphCancellationTargetV1>,
    pub(crate) reason_code: String,
}

/// Stable machine-readable reasons for WorkGraph admission and failure control.
pub(crate) mod concurrency_reason {
    pub(crate) const GLOBAL_LIMIT: &str = "work_graph.admission.global_limit";
    pub(crate) const PROFILE_LIMIT: &str = "work_graph.admission.profile_limit";
    pub(crate) const PROVIDER_LIMIT: &str = "work_graph.admission.provider_limit";
    pub(crate) const PROVIDER_RATE_LIMITED: &str = "work_graph.admission.provider_rate_limited";
    pub(crate) const SERIALIZATION_CONFLICT: &str = "work_graph.admission.serialization_conflict";
    pub(crate) const WORKSPACE_CONFLICT: &str = "work_graph.admission.workspace_conflict";
    pub(crate) const WORKSPACE_READER_LIMIT: &str = "work_graph.admission.workspace_reader_limit";
    pub(crate) const MEMORY_PRESSURE: &str = "work_graph.admission.memory_pressure";
    pub(crate) const RETRY_BACKOFF: &str = "work_graph.failure.retry_backoff";
    pub(crate) const CIRCUIT_OPEN: &str = "work_graph.failure.circuit_open";
    pub(crate) const GRAPH_CANCELLED: &str = "work_graph.cancel.fanout_requested";
}
